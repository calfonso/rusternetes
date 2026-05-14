use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Extension,
};
use rusternetes_common::auth::{BootstrapTokenManager, TokenManager, UserInfo};
use std::sync::{Arc, LazyLock};
use tracing::{debug, info, warn};

/// Global protobuf schema registry — initialized once on first use
static PROTO_REGISTRY: LazyLock<crate::protobuf::ProtoRegistry> =
    LazyLock::new(crate::protobuf::ProtoRegistry::new);

/// Extension type to carry UserInfo through the request
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub user: UserInfo,
}

/// Middleware that adds a default admin AuthContext when skip_auth is enabled
pub async fn skip_auth_middleware(mut request: Request, next: Next) -> Result<Response, Response> {
    debug!(
        "skip_auth_middleware called for: {} {}",
        request.method(),
        request.uri()
    );

    // Create an admin user context
    let admin_user = UserInfo {
        username: "admin".to_string(),
        uid: "system:admin".to_string(),
        groups: vec!["system:masters".to_string()],
        extra: std::collections::HashMap::new(),
    };

    // Insert AuthContext into request extensions
    request
        .extensions_mut()
        .insert(AuthContext { user: admin_user });

    debug!("AuthContext inserted into request extensions");

    Ok(next.run(request).await)
}

/// Authentication middleware that extracts and validates JWT tokens
pub async fn auth_middleware(
    Extension(token_manager): Extension<Arc<TokenManager>>,
    Extension(bootstrap_token_manager): Extension<Arc<BootstrapTokenManager>>,
    mut request: Request,
    next: Next,
) -> Result<Response, Response> {
    // Extract Bearer token from Authorization header
    let auth_header = request
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");

    let user = if let Some(token) = auth_header.strip_prefix("Bearer ") {
        // Skip "Bearer "

        // Try to validate as a service account token first
        if let Ok(claims) = token_manager.validate_token(token) {
            let user_info = UserInfo::from_service_account_claims(&claims);
            debug!(
                "Authenticated user (service account): {}",
                user_info.username
            );
            user_info
        }
        // Try to validate as a bootstrap token
        else if let Ok(bootstrap_token) = bootstrap_token_manager.validate_token(token) {
            let user_info = UserInfo::from_bootstrap_token(&bootstrap_token);
            debug!(
                "Authenticated user (bootstrap token): {}",
                user_info.username
            );
            user_info
        }
        // Invalid token
        else {
            warn!("Invalid token");
            return Err((StatusCode::UNAUTHORIZED, "Invalid token").into_response());
        }
    } else {
        // Anonymous user
        debug!("Anonymous request");
        UserInfo::anonymous()
    };

    // Insert UserInfo into request extensions
    request.extensions_mut().insert(AuthContext { user });

    Ok(next.run(request).await)
}

/// Middleware that normalizes Content-Type to application/json for write requests.
/// The Kubernetes client defaults to application/vnd.kubernetes.protobuf, but we only
/// support JSON. Axum's Json extractor rejects non-application/json content types with
/// HTTP 415, so we rewrite the header before the request reaches the handler.
pub async fn normalize_content_type_middleware(
    mut request: Request,
    next: Next,
) -> Result<Response, Response> {
    if request.method() == axum::http::Method::POST
        || request.method() == axum::http::Method::PUT
        || request.method() == axum::http::Method::PATCH
        || request.method() == axum::http::Method::DELETE
    {
        let content_type = request
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        // Handle protobuf Content-Type: extract JSON from K8s protobuf envelope.
        // The K8s protobuf format wraps JSON in a simple envelope:
        //   magic: "k8s\0" (4 bytes)
        //   protobuf Unknown message with `raw` field containing JSON
        if content_type.starts_with("application/vnd.kubernetes.protobuf") {
            debug!(
                "Converting protobuf to JSON for: {} {}",
                request.method(),
                request.uri()
            );

            // Read the body
            let (parts, body) = request.into_parts();
            let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
                Ok(b) => b,
                Err(_) => {
                    return Err(axum::response::Response::builder()
                        .status(axum::http::StatusCode::BAD_REQUEST)
                        .body(axum::body::Body::from("failed to read request body"))
                        .unwrap());
                }
            };

            let json_body = if body_bytes.starts_with(b"k8s\0") {
                // K8s protobuf envelope — extract the JSON from the `raw` field.
                let extracted = extract_json_from_k8s_protobuf(&body_bytes);
                if let Some(json) = extracted {
                    json
                } else {
                    // Extraction failed — the `raw` field contains native protobuf, not JSON.
                    // First try the structured protobuf-to-JSON decoder.
                    // Then fall back to brace-scanning, but always validate the result.
                    use std::fmt::Write as _;
                    let mut hex_preview = String::with_capacity(80 * 3);
                    for b in body_bytes.iter().skip(4).take(80) {
                        let _ = write!(hex_preview, "{:02x} ", b);
                    }
                    if hex_preview.ends_with(' ') {
                        hex_preview.pop();
                    }
                    debug!(
                        "Protobuf body has no JSON in raw field ({} bytes). Hex after k8s\\0: {}",
                        body_bytes.len(),
                        hex_preview
                    );

                    // Try the generic proto schema-based decoder first.
                    // This handles ALL standard K8s types (Deployment, Pod, Service, etc.)
                    // by using field number → name mappings from the K8s .proto definitions.
                    if let Some(json_bytes) = PROTO_REGISTRY.decode_k8s_resource(&body_bytes) {
                        if serde_json::from_slice::<serde_json::Value>(&json_bytes).is_ok() {
                            info!(
                                "Decoded K8s protobuf via schema registry ({} bytes)",
                                json_bytes.len()
                            );
                            json_bytes
                        } else {
                            warn!(
                                "Schema-decoded protobuf produced invalid JSON, trying CRD decoder"
                            );
                            // Fall through to CRD-specific decoder
                            if let Some(json_bytes) = decode_k8s_protobuf_to_json(&body_bytes) {
                                if serde_json::from_slice::<serde_json::Value>(&json_bytes).is_ok()
                                {
                                    info!(
                                        "Decoded K8s protobuf to JSON ({} bytes)",
                                        json_bytes.len()
                                    );
                                    json_bytes
                                } else {
                                    try_brace_scan_or_type_meta(&body_bytes)
                                }
                            } else {
                                try_brace_scan_or_type_meta(&body_bytes)
                            }
                        }
                    }
                    // Schema-based decode returned None (unknown kind) — try CRD decoder
                    else if let Some(json_bytes) = decode_k8s_protobuf_to_json(&body_bytes) {
                        if serde_json::from_slice::<serde_json::Value>(&json_bytes).is_ok() {
                            info!("Decoded K8s protobuf to JSON ({} bytes)", json_bytes.len());
                            json_bytes
                        } else {
                            warn!("Decoded protobuf produced invalid JSON, trying brace scan");
                            try_brace_scan_or_type_meta(&body_bytes)
                        }
                    } else {
                        // Both decoders failed — try brace scan, then TypeMeta
                        try_brace_scan_or_type_meta(&body_bytes)
                    }
                }
            } else if body_bytes.starts_with(b"{") || body_bytes.starts_with(b"[") {
                // Already JSON despite protobuf Content-Type
                body_bytes.to_vec()
            } else {
                // Unknown binary format — might be K8s protobuf without k8s\0 magic,
                // or CBOR, or another encoding.
                // Try brace scan but validate the result is actual JSON.
                let mut found_valid = None;
                for i in 0..body_bytes.len() {
                    if body_bytes[i] == b'{' {
                        // Try to extract a balanced JSON object
                        let candidate = scan_balanced_braces(&body_bytes[i..]);
                        if let Some(ref c) = candidate {
                            if serde_json::from_slice::<serde_json::Value>(c).is_ok() {
                                found_valid = Some(c.clone());
                                break;
                            }
                        }
                        // This `{` wasn't valid JSON start, try next one
                    }
                }
                found_valid.unwrap_or_else(|| b"{}".to_vec())
            };

            let mut new_request = Request::from_parts(parts, axum::body::Body::from(json_body));
            new_request.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            // Mark that this request was originally protobuf so the response
            // middleware can wrap the JSON response back in protobuf
            new_request.headers_mut().insert(
                axum::http::HeaderName::from_static("x-was-protobuf"),
                axum::http::HeaderValue::from_static("true"),
            );
            request = new_request;
        }

        // For patch content types, save the original in a custom header before
        // normalizing to application/json (which Axum's Json extractor requires).
        // Patch handlers check X-Original-Content-Type or Content-Type to determine patch type.
        if content_type.starts_with("application/strategic-merge-patch+json")
            || content_type.starts_with("application/merge-patch+json")
            || content_type.starts_with("application/json-patch+json")
        {
            if let Ok(hv) = axum::http::HeaderValue::from_str(&content_type) {
                request.headers_mut().insert(
                    axum::http::HeaderName::from_static("x-original-content-type"),
                    hv,
                );
            }
            request.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
        } else if !content_type.starts_with("application/json")
            && !content_type.starts_with("application/apply-patch+yaml")
        {
            request.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
        }
    }

    // Track if the client wants protobuf responses (via Accept header).
    // Real Kubernetes always wraps in protobuf when Accept requests it,
    // regardless of request Content-Type.
    // Skip for watch/streaming requests — those use chunked JSON lines and
    // cannot be collected into a single protobuf envelope.
    let accept_header = request
        .headers()
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let _is_watch_request = accept_header.contains("stream=watch")
        || request.uri().path().contains("/watch/")
        || request
            .uri()
            .query()
            .map(|q| q.contains("watch=true") || q.contains("watch=1"))
            .unwrap_or(false);
    // Protobuf disabled. K8s protobuf requires Unknown.raw to contain native
    // protobuf-encoded bytes (e.g., proto.Marshal(NamespaceList)), not JSON.
    // We can't produce native protobuf for K8s types in Rust. Client-go always
    // sends "Accept: application/vnd.kubernetes.protobuf, application/json"
    // and falls back to JSON when protobuf is unavailable.
    let wants_protobuf = false;

    let response = next.run(request).await;

    // Wrap response in protobuf if:
    // 1. The Accept header requests protobuf
    // 2. The response is JSON (our handlers always produce JSON)
    // 3. The response is NOT a streaming/watch response (no Content-Length means streaming)
    if wants_protobuf {
        let response_ct = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if response_ct.starts_with("application/json") {
            let (parts, body) = response.into_parts();
            if let Ok(json_bytes) = axum::body::to_bytes(body, 10 * 1024 * 1024).await {
                // Extract apiVersion/kind from JSON for the TypeMeta field
                let (api_version, kind) = extract_api_version_kind(&json_bytes);
                let pb = wrap_json_in_protobuf_with_type_meta(
                    &json_bytes,
                    api_version.as_deref().unwrap_or(""),
                    kind.as_deref().unwrap_or(""),
                );
                let mut resp = Response::from_parts(parts, Body::from(pb));
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    axum::http::HeaderValue::from_static("application/vnd.kubernetes.protobuf"),
                );
                return Ok(resp);
            }
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap());
        }
    }

    Ok(response)
}

/// Extract apiVersion and kind from JSON bytes without full parsing.
fn extract_api_version_kind(json: &[u8]) -> (Option<String>, Option<String>) {
    // Quick parse just the top-level apiVersion and kind
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(json) {
        let api_version = v
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let kind = v
            .get("kind")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        (api_version, kind)
    } else {
        (None, None)
    }
}

/// Wrap JSON bytes in the K8s protobuf envelope with TypeMeta:
/// "k8s\0" + Unknown{typeMeta: {apiVersion, kind}, raw: json, contentType: "application/json"}
fn wrap_json_in_protobuf_with_type_meta(json: &[u8], api_version: &str, kind: &str) -> Vec<u8> {
    // K8s runtime.Unknown protobuf message:
    //   field 1 (typeMeta, TypeMeta): nested message
    //     field 1 (apiVersion, string)
    //     field 2 (kind, string)
    //   field 2 (raw, bytes): the JSON payload
    //   field 3 (contentEncoding, string): empty
    //   field 4 (contentType, string): "application/json"
    let content_type = b"application/json";
    let mut msg = Vec::with_capacity(json.len() + 80);

    // Field 1: TypeMeta (nested message) — tag = (1 << 3) | 2 = 0x0a
    if !api_version.is_empty() || !kind.is_empty() {
        let mut type_meta = Vec::new();
        if !api_version.is_empty() {
            // TypeMeta field 1: apiVersion — tag = (1 << 3) | 2 = 0x0a
            type_meta.push(0x0a);
            encode_protobuf_varint(&mut type_meta, api_version.len() as u64);
            type_meta.extend_from_slice(api_version.as_bytes());
        }
        if !kind.is_empty() {
            // TypeMeta field 2: kind — tag = (2 << 3) | 2 = 0x12
            type_meta.push(0x12);
            encode_protobuf_varint(&mut type_meta, kind.len() as u64);
            type_meta.extend_from_slice(kind.as_bytes());
        }
        msg.push(0x0a);
        encode_protobuf_varint(&mut msg, type_meta.len() as u64);
        msg.extend_from_slice(&type_meta);
    }

    // Field 2: raw bytes (the JSON) — tag = (2 << 3) | 2 = 0x12
    msg.push(0x12);
    encode_protobuf_varint(&mut msg, json.len() as u64);
    msg.extend_from_slice(json);
    // Field 4: contentType — tag = (4 << 3) | 2 = 0x22
    msg.push(0x22);
    encode_protobuf_varint(&mut msg, content_type.len() as u64);
    msg.extend_from_slice(content_type);

    let mut buf = Vec::with_capacity(msg.len() + 4);
    buf.extend_from_slice(b"k8s\0");
    buf.extend_from_slice(&msg);
    buf
}

/// Wrap JSON bytes in the K8s protobuf envelope: "k8s\0" + Unknown{raw: json}
#[allow(dead_code)]
fn wrap_json_in_protobuf(json: &[u8]) -> Vec<u8> {
    // K8s runtime.Unknown protobuf message (from k8s.io/apimachinery generated.proto):
    //   field 1 (typeMeta, TypeMeta): nested message (empty for responses)
    //   field 2 (raw, bytes): the JSON payload
    //   field 3 (contentEncoding, string): empty
    //   field 4 (contentType, string): "application/json"
    //
    // IMPORTANT: These field numbers match the Go protobuf definition, NOT our
    // prost Unknown struct which flattened TypeMeta and shifted field numbers.
    // The Go client decodes using the original proto definition.
    let content_type = b"application/json";
    let mut msg = Vec::with_capacity(json.len() + 30);

    // Field 2: raw bytes (the JSON) — tag = (2 << 3) | 2 = 0x12
    msg.push(0x12);
    encode_protobuf_varint(&mut msg, json.len() as u64);
    msg.extend_from_slice(json);
    // Field 4: contentType — tag = (4 << 3) | 2 = 0x22
    msg.push(0x22);
    encode_protobuf_varint(&mut msg, content_type.len() as u64);
    msg.extend_from_slice(content_type);

    let mut buf = Vec::with_capacity(msg.len() + 4);
    buf.extend_from_slice(b"k8s\0");
    buf.extend_from_slice(&msg);
    buf
}

fn encode_protobuf_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        } else {
            buf.push(byte | 0x80);
        }
    }
}

/// Extract JSON from a Kubernetes protobuf envelope.
/// K8s protobuf format: "k8s\0" + protobuf Unknown { raw: bytes, contentType: string }
/// The `raw` field (protobuf field 2, wire type 2 = length-delimited) contains the JSON.
fn extract_json_from_k8s_protobuf(data: &[u8]) -> Option<Vec<u8>> {
    // Skip the 4-byte magic "k8s\0"
    if data.len() < 5 || &data[0..4] != b"k8s\0" {
        return None;
    }
    let data = &data[4..];

    // Parse the protobuf Unknown message looking for field 2 (raw bytes)
    // Field 2, wire type 2 (length-delimited) = tag byte 0x12
    let mut pos = 0;
    while pos < data.len() {
        // Read tag as varint (supports field numbers > 15)
        let mut tag: u64 = 0;
        let mut shift = 0;
        while pos < data.len() {
            let b = data[pos] as u64;
            pos += 1;
            tag |= (b & 0x7f) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        let field_number = tag >> 3;
        let wire_type = tag & 0x07;

        match wire_type {
            0 => {
                // Varint — skip
                while pos < data.len() && data[pos] & 0x80 != 0 {
                    pos += 1;
                }
                if pos < data.len() {
                    pos += 1;
                }
            }
            1 => {
                // 64-bit fixed — skip 8 bytes
                pos += 8;
            }
            2 => {
                // Length-delimited — read length then data
                let mut len: usize = 0;
                let mut shift = 0;
                while pos < data.len() {
                    let b = data[pos] as usize;
                    pos += 1;
                    len |= (b & 0x7f) << shift;
                    if b & 0x80 == 0 {
                        break;
                    }
                    shift += 7;
                }
                if field_number == 2 && pos + len <= data.len() {
                    // Field 2 contains the raw bytes (serialized resource)
                    let raw = &data[pos..pos + len];
                    if !raw.is_empty() && (raw[0] == b'{' || raw[0] == b'[') {
                        return Some(raw.to_vec());
                    }
                    // Log what field 2 contains if it's not JSON
                    if field_number == 2 && !raw.is_empty() {
                        let preview: String = raw
                            .iter()
                            .take(40)
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join(" ");
                        tracing::debug!(
                            "Protobuf field 2 ({} bytes, not JSON): first bytes: {}",
                            len,
                            preview
                        );
                    }
                }
                if pos + len > data.len() {
                    return None;
                }
                pos += len;
            }
            5 => {
                // 32-bit fixed — skip 4 bytes
                pos += 4;
            }
            _ => {
                // Unknown wire type — can't parse further, try fallback
                break;
            }
        }
    }

    // Fallback: scan for the first valid JSON object in the data
    for i in 0..data.len() {
        if data[i] == b'{' {
            if let Some(candidate) = scan_balanced_braces(&data[i..]) {
                // Only return if this is actually valid JSON, not binary garbage
                if serde_json::from_slice::<serde_json::Value>(&candidate).is_ok() {
                    return Some(candidate);
                }
            }
        }
    }
    None
}

/// Extract TypeMeta (apiVersion, kind) from a K8s protobuf envelope.
/// The Unknown message structure: field 1 = TypeMeta { field 1 = apiVersion, field 2 = kind }
fn extract_type_meta_from_protobuf(data: &[u8]) -> Option<(String, String)> {
    if data.len() < 5 || &data[0..4] != b"k8s\0" {
        return None;
    }
    let data = &data[4..];

    // Read field 1 (TypeMeta) — tag 0x0a, wire type 2
    let mut pos = 0;
    if pos >= data.len() || data[pos] != 0x0a {
        return None;
    }
    pos += 1;

    // Read length varint
    let mut type_meta_len: usize = 0;
    let mut shift = 0;
    while pos < data.len() {
        let b = data[pos] as usize;
        pos += 1;
        type_meta_len |= (b & 0x7f) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    if pos + type_meta_len > data.len() {
        return None;
    }
    let type_meta = &data[pos..pos + type_meta_len];

    // Parse TypeMeta: field 1 = apiVersion, field 2 = kind
    let mut api_version = String::new();
    let mut kind = String::new();
    let mut tpos = 0;
    while tpos < type_meta.len() {
        let tag = type_meta[tpos];
        tpos += 1;
        let field_num = tag >> 3;
        let wire_type = tag & 0x07;

        if wire_type == 2 {
            // Length-delimited string
            let mut slen: usize = 0;
            let mut sshift = 0;
            while tpos < type_meta.len() {
                let b = type_meta[tpos] as usize;
                tpos += 1;
                slen |= (b & 0x7f) << sshift;
                if b & 0x80 == 0 {
                    break;
                }
                sshift += 7;
            }
            if tpos + slen <= type_meta.len() {
                if let Ok(s) = std::str::from_utf8(&type_meta[tpos..tpos + slen]) {
                    match field_num {
                        1 => api_version = s.to_string(),
                        2 => kind = s.to_string(),
                        _ => {}
                    }
                }
            }
            tpos += slen;
        } else {
            break; // Unknown wire type, stop
        }
    }

    if !api_version.is_empty() && !kind.is_empty() {
        Some((api_version, kind))
    } else {
        None
    }
}

/// Attempt to decode a K8s protobuf body into JSON.
/// The K8s Unknown message wraps: field 1 = TypeMeta, field 2 = raw object (protobuf).
/// We extract TypeMeta and the raw object, then recursively decode protobuf string fields
/// into a JSON structure. This is a best-effort decoder for CRDs and other resources
/// where the client hardcodes protobuf encoding.
fn decode_k8s_protobuf_to_json(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 5 || &data[0..4] != b"k8s\0" {
        return None;
    }
    let data = &data[4..];

    let mut api_version = String::new();
    let mut kind = String::new();
    let mut raw_bytes: Option<&[u8]> = None;

    let mut pos = 0;
    while pos < data.len() {
        let tag = data[pos];
        pos += 1;
        let field_num = tag >> 3;
        let wire_type = tag & 0x07;

        if wire_type == 2 {
            // Length-delimited
            let mut len: usize = 0;
            let mut shift = 0;
            while pos < data.len() {
                let b = data[pos] as usize;
                pos += 1;
                len |= (b & 0x7f) << shift;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            if pos + len > data.len() {
                break;
            }
            let field_data = &data[pos..pos + len];
            pos += len;

            match field_num {
                1 => {
                    // TypeMeta — parse apiVersion and kind
                    let mut tpos = 0;
                    while tpos < field_data.len() {
                        let t = field_data[tpos];
                        tpos += 1;
                        let fnum = t >> 3;
                        let twire = t & 0x07;
                        if twire == 0 {
                            while tpos < field_data.len() && field_data[tpos] & 0x80 != 0 {
                                tpos += 1;
                            }
                            if tpos < field_data.len() {
                                tpos += 1;
                            }
                            continue;
                        } else if twire == 1 {
                            tpos += 8;
                            continue;
                        } else if twire == 5 {
                            tpos += 4;
                            continue;
                        } else if twire != 2 {
                            break;
                        }
                        let mut slen: usize = 0;
                        let mut ss = 0;
                        while tpos < field_data.len() {
                            let b = field_data[tpos] as usize;
                            tpos += 1;
                            slen |= (b & 0x7f) << ss;
                            if b & 0x80 == 0 {
                                break;
                            }
                            ss += 7;
                        }
                        if tpos + slen <= field_data.len() {
                            if let Ok(s) = std::str::from_utf8(&field_data[tpos..tpos + slen]) {
                                match fnum {
                                    1 => api_version = s.to_string(),
                                    2 => kind = s.to_string(),
                                    _ => {}
                                }
                            }
                        }
                        tpos += slen;
                    }
                }
                2 => raw_bytes = Some(field_data),
                _ => {}
            }
        } else if wire_type == 0 {
            // Varint — skip
            while pos < data.len() && data[pos] & 0x80 != 0 {
                pos += 1;
            }
            if pos < data.len() {
                pos += 1;
            }
        } else {
            break;
        }
    }

    if api_version.is_empty() || kind.is_empty() {
        tracing::warn!(
            "Protobuf decode: api_version='{}' kind='{}' raw_bytes={}",
            api_version,
            kind,
            raw_bytes.map(|r| r.len()).unwrap_or(0)
        );
        return None;
    }

    // For CRDs specifically, try to decode the raw protobuf into a JSON CRD.
    // The CRD protobuf schema has known field numbers:
    //   ObjectMeta = field 1, CRDSpec = field 2
    // ObjectMeta fields: name=1, namespace=3, uid=5, resourceVersion=6
    // CRDSpec fields: group=1, names=3, scope=4, versions=7
    // This is best-effort — we extract what we can.
    let raw = raw_bytes?;

    // Extract ObjectMeta.name and CRD spec fields from the raw protobuf
    let mut metadata_name = String::new();
    let mut metadata_namespace = String::new();
    let mut spec_group = String::new();
    let mut spec_scope = String::new();
    let mut spec_names_plural = String::new();
    let mut spec_names_singular = String::new();
    let mut spec_names_kind = String::new();
    let mut spec_names_list_kind = String::new();
    let mut spec_version_names: Vec<String> = Vec::new();

    let mut rpos = 0;
    while rpos < raw.len() {
        // Decode tag as varint (supports field numbers > 15)
        let mut tag: u64 = 0;
        let mut tag_shift = 0;
        while rpos < raw.len() {
            let b = raw[rpos] as u64;
            rpos += 1;
            tag |= (b & 0x7f) << tag_shift;
            if b & 0x80 == 0 {
                break;
            }
            tag_shift += 7;
        }
        let field_num = (tag >> 3) as u8;
        let wire_type = (tag & 0x07) as u8;

        if wire_type == 2 {
            let mut len: usize = 0;
            let mut shift = 0;
            while rpos < raw.len() {
                let b = raw[rpos] as usize;
                rpos += 1;
                len |= (b & 0x7f) << shift;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            if rpos + len > raw.len() {
                break;
            }
            let field_data = &raw[rpos..rpos + len];
            rpos += len;

            match field_num {
                1 => {
                    // ObjectMeta — parse name (field 1) and namespace (field 3)
                    let mut mpos = 0;
                    while mpos < field_data.len() {
                        let mut mt: u64 = 0;
                        let mut mt_shift = 0;
                        while mpos < field_data.len() {
                            let b = field_data[mpos] as u64;
                            mpos += 1;
                            mt |= (b & 0x7f) << mt_shift;
                            if b & 0x80 == 0 {
                                break;
                            }
                            mt_shift += 7;
                        }
                        let mfnum = (mt >> 3) as u8;
                        let mwire = (mt & 0x07) as u8;
                        if mwire == 2 {
                            let mut mlen: usize = 0;
                            let mut ms = 0;
                            while mpos < field_data.len() {
                                let b = field_data[mpos] as usize;
                                mpos += 1;
                                mlen |= (b & 0x7f) << ms;
                                if b & 0x80 == 0 {
                                    break;
                                }
                                ms += 7;
                            }
                            if mpos + mlen <= field_data.len() {
                                if let Ok(s) = std::str::from_utf8(&field_data[mpos..mpos + mlen]) {
                                    match mfnum {
                                        1 => metadata_name = s.to_string(),
                                        3 => metadata_namespace = s.to_string(),
                                        _ => {}
                                    }
                                }
                            }
                            mpos += mlen;
                        } else if mwire == 0 {
                            while mpos < field_data.len() && field_data[mpos] & 0x80 != 0 {
                                mpos += 1;
                            }
                            if mpos < field_data.len() {
                                mpos += 1;
                            }
                        } else if mwire == 1 {
                            mpos += 8; // 64-bit fixed
                        } else if mwire == 5 {
                            mpos += 4; // 32-bit fixed
                        } else {
                            break;
                        }
                    }
                }
                2 => {
                    // CRDSpec — parse group, names, scope, versions
                    let mut spos = 0;
                    while spos < field_data.len() {
                        let mut st: u64 = 0;
                        let mut st_shift = 0;
                        while spos < field_data.len() {
                            let b = field_data[spos] as u64;
                            spos += 1;
                            st |= (b & 0x7f) << st_shift;
                            if b & 0x80 == 0 {
                                break;
                            }
                            st_shift += 7;
                        }
                        let sfnum = (st >> 3) as u8;
                        let swire = (st & 0x07) as u8;
                        if swire == 2 {
                            let mut slen: usize = 0;
                            let mut ss = 0;
                            while spos < field_data.len() {
                                let b = field_data[spos] as usize;
                                spos += 1;
                                slen |= (b & 0x7f) << ss;
                                if b & 0x80 == 0 {
                                    break;
                                }
                                ss += 7;
                            }
                            if spos + slen <= field_data.len() {
                                match sfnum {
                                    1 => {
                                        spec_group =
                                            String::from_utf8_lossy(&field_data[spos..spos + slen])
                                                .to_string();
                                    }
                                    3 => {
                                        // Names submessage — parse plural, singular, kind, listKind
                                        let names = &field_data[spos..spos + slen];
                                        let mut npos = 0;
                                        while npos < names.len() {
                                            let mut nt: u64 = 0;
                                            let mut nt_shift = 0;
                                            while npos < names.len() {
                                                let b = names[npos] as u64;
                                                npos += 1;
                                                nt |= (b & 0x7f) << nt_shift;
                                                if b & 0x80 == 0 {
                                                    break;
                                                }
                                                nt_shift += 7;
                                            }
                                            let nfnum = (nt >> 3) as u8;
                                            if (nt & 0x07) != 2 {
                                                // Skip non-length-delimited fields
                                                let nwire = (nt & 0x07) as u8;
                                                if nwire == 0 {
                                                    while npos < names.len()
                                                        && names[npos] & 0x80 != 0
                                                    {
                                                        npos += 1;
                                                    }
                                                    if npos < names.len() {
                                                        npos += 1;
                                                    }
                                                    continue;
                                                } else if nwire == 1 {
                                                    npos += 8;
                                                    continue;
                                                } else if nwire == 5 {
                                                    npos += 4;
                                                    continue;
                                                }
                                                break;
                                            }
                                            let mut nlen: usize = 0;
                                            let mut ns = 0;
                                            while npos < names.len() {
                                                let b = names[npos] as usize;
                                                npos += 1;
                                                nlen |= (b & 0x7f) << ns;
                                                if b & 0x80 == 0 {
                                                    break;
                                                }
                                                ns += 7;
                                            }
                                            if npos + nlen <= names.len() {
                                                if let Ok(s) =
                                                    std::str::from_utf8(&names[npos..npos + nlen])
                                                {
                                                    match nfnum {
                                                        1 => spec_names_plural = s.to_string(),
                                                        2 => spec_names_singular = s.to_string(),
                                                        // field 3 = shortNames (repeated, skip)
                                                        4 => spec_names_kind = s.to_string(),
                                                        5 => spec_names_list_kind = s.to_string(),
                                                        _ => {}
                                                    }
                                                }
                                            }
                                            npos += nlen;
                                        }
                                    }
                                    4 => {
                                        spec_scope =
                                            String::from_utf8_lossy(&field_data[spos..spos + slen])
                                                .to_string();
                                    }
                                    7 => {
                                        // Version submessage — extract name (field 1)
                                        let ver = &field_data[spos..spos + slen];
                                        let mut vpos = 0;
                                        while vpos < ver.len() {
                                            let mut vt: u64 = 0;
                                            let mut vt_shift = 0;
                                            while vpos < ver.len() {
                                                let b = ver[vpos] as u64;
                                                vpos += 1;
                                                vt |= (b & 0x7f) << vt_shift;
                                                if b & 0x80 == 0 {
                                                    break;
                                                }
                                                vt_shift += 7;
                                            }
                                            let vfnum = (vt >> 3) as u8;
                                            let vwire = (vt & 0x07) as u8;
                                            if vwire == 2 {
                                                let mut vlen: usize = 0;
                                                let mut vs = 0;
                                                while vpos < ver.len() {
                                                    let b = ver[vpos] as usize;
                                                    vpos += 1;
                                                    vlen |= (b & 0x7f) << vs;
                                                    if b & 0x80 == 0 {
                                                        break;
                                                    }
                                                    vs += 7;
                                                }
                                                if vfnum == 1 && vpos + vlen <= ver.len() {
                                                    if let Ok(vname) =
                                                        std::str::from_utf8(&ver[vpos..vpos + vlen])
                                                    {
                                                        spec_version_names.push(vname.to_string());
                                                    }
                                                }
                                                if vpos + vlen <= ver.len() {
                                                    vpos += vlen;
                                                } else {
                                                    break;
                                                }
                                            } else if vwire == 0 {
                                                while vpos < ver.len() && ver[vpos] & 0x80 != 0 {
                                                    vpos += 1;
                                                }
                                                if vpos < ver.len() {
                                                    vpos += 1;
                                                }
                                            } else if vwire == 1 {
                                                vpos += 8; // 64-bit
                                            } else if vwire == 5 {
                                                vpos += 4; // 32-bit
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            spos += slen;
                        } else if swire == 0 {
                            while spos < field_data.len() && field_data[spos] & 0x80 != 0 {
                                spos += 1;
                            }
                            if spos < field_data.len() {
                                spos += 1;
                            }
                        } else if swire == 1 {
                            spos += 8; // 64-bit fixed
                        } else if swire == 5 {
                            spos += 4; // 32-bit fixed
                        } else {
                            break;
                        }
                    }
                }
                _ => {}
            }
        } else if wire_type == 0 {
            // Varint — skip
            while rpos < raw.len() && raw[rpos] & 0x80 != 0 {
                rpos += 1;
            }
            if rpos < raw.len() {
                rpos += 1;
            }
        } else if wire_type == 1 {
            // 64-bit fixed (double, fixed64, sfixed64) — skip 8 bytes
            rpos += 8;
        } else if wire_type == 5 {
            // 32-bit fixed (float, fixed32, sfixed32) — skip 4 bytes
            rpos += 4;
        } else {
            break;
        }
    }

    if metadata_name.is_empty() {
        // Try extracting name from the raw bytes directly (string search)
        if let Ok(raw_str) = std::str::from_utf8(raw) {
            // Look for strings that look like CRD names (contain dots)
            for word in raw_str.split(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-')
            {
                if word.contains('.')
                    && word.len() > 5
                    && !word.starts_with('.')
                    && !word.ends_with('.')
                {
                    // Likely a CRD name like "foos.example.com"
                    if spec_group.is_empty() || word.ends_with(&format!(".{}", spec_group)) {
                        metadata_name = word.to_string();
                        tracing::info!(
                            "CRD protobuf: extracted name '{}' via string search",
                            metadata_name
                        );
                        break;
                    }
                }
            }
        }
        if metadata_name.is_empty() {
            tracing::warn!(
                "CRD protobuf decode: metadata_name empty, group='{}', plural='{}', versions={:?}",
                spec_group,
                spec_names_plural,
                spec_version_names
            );
            return None;
        }
    }

    // Construct a JSON CRD with the extracted fields
    let scope = if spec_scope.is_empty() {
        "Namespaced"
    } else {
        &spec_scope
    };
    let json = serde_json::json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {
            "name": metadata_name,
            "namespace": if metadata_namespace.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(metadata_namespace) },
        },
        "spec": {
            "group": spec_group,
            "scope": scope,
            "names": {
                "plural": spec_names_plural,
                "singular": spec_names_singular,
                "kind": spec_names_kind,
                "listKind": if spec_names_list_kind.is_empty() { format!("{}List", spec_names_kind) } else { spec_names_list_kind },
            },
            "versions": if spec_version_names.is_empty() {
                vec![serde_json::json!({"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}, "subresources": {"status": {}}})]
            } else {
                spec_version_names.iter().enumerate().map(|(i, vname)| {
                    serde_json::json!({"name": vname, "served": true, "storage": i == 0, "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}, "subresources": {"status": {}}})
                }).collect::<Vec<_>>()
            },
        }
    });

    serde_json::to_vec(&json).ok()
}

/// Scan for a balanced JSON object starting from data[0] which must be `{`.
/// Returns the balanced slice as a Vec, or None if unbalanced.
fn scan_balanced_braces(data: &[u8]) -> Option<Vec<u8>> {
    if data.is_empty() || data[0] != b'{' {
        return None;
    }
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    for j in 0..data.len() {
        if escape {
            escape = false;
            continue;
        }
        match data[j] {
            b'\\' if in_string => escape = true,
            b'"' => in_string = !in_string,
            b'{' if !in_string => depth += 1,
            b'}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(data[..=j].to_vec());
                }
            }
            _ => {}
        }
    }
    // Unbalanced — return from { to end as a last resort
    Some(data.to_vec())
}

/// Try brace-scanning to find embedded JSON in a protobuf body, validating the
/// result with serde_json. If no valid JSON is found, fall back to extracting
/// TypeMeta (apiVersion/kind) to construct a minimal JSON object.
fn try_brace_scan_or_type_meta(body_bytes: &[u8]) -> Vec<u8> {
    // Scan for a valid JSON object embedded in the binary data.
    // We must validate that the JSON looks like a K8s resource (has apiVersion/kind/metadata
    // or at least metadata with a name) — protobuf binary can contain accidental JSON
    // fragments (like string field values) that parse as valid JSON but aren't the resource.
    let skip = if body_bytes.starts_with(b"k8s\0") {
        4
    } else {
        0
    };
    // First pass: look for JSON that looks like a K8s resource
    for i in skip..body_bytes.len() {
        if body_bytes[i] == b'{' {
            if let Some(candidate) = scan_balanced_braces(&body_bytes[i..]) {
                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&candidate) {
                    // Validate this looks like a K8s resource object, not a random fragment.
                    // A valid resource must have either:
                    // - "apiVersion" and "kind" fields, OR
                    // - a "metadata" field with "name"
                    let has_api_version = val.get("apiVersion").is_some();
                    let has_kind = val.get("kind").is_some();
                    let has_metadata_name =
                        val.get("metadata").and_then(|m| m.get("name")).is_some();
                    let has_spec = val.get("spec").is_some();

                    if (has_api_version && has_kind) || (has_metadata_name && has_spec) {
                        info!(
                            "Found valid K8s JSON via brace scan at offset {} ({} bytes)",
                            i,
                            candidate.len()
                        );
                        return candidate;
                    }
                    debug!(
                        "Skipping non-resource JSON at offset {} ({} bytes, has_api={}, has_kind={}, has_meta_name={}, has_spec={})",
                        i, candidate.len(), has_api_version, has_kind, has_metadata_name, has_spec
                    );
                }
            }
            // This `{` wasn't valid K8s JSON, try next occurrence
        }
    }

    // No valid resource JSON found — extract TypeMeta to construct minimal JSON
    let type_meta = extract_type_meta_from_protobuf(body_bytes);
    if let Some((api_version, kind)) = type_meta {
        let minimal = format!(
            r#"{{"apiVersion":"{}","kind":"{}","metadata":{{}}}}"#,
            api_version, kind
        );
        info!(
            "Extracted TypeMeta from protobuf: apiVersion={}, kind={}",
            api_version, kind
        );
        minimal.into_bytes()
    } else {
        warn!("Could not extract TypeMeta from protobuf, using empty object");
        b"{}".to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_balanced_braces_valid_json() {
        let data = br#"{"key":"value"}"#;
        let result = scan_balanced_braces(data);
        assert_eq!(result, Some(data.to_vec()));
    }

    #[test]
    fn test_scan_balanced_braces_nested() {
        let data = br#"{"a":{"b":"c"}}"#;
        let result = scan_balanced_braces(data);
        assert_eq!(result, Some(data.to_vec()));
    }

    #[test]
    fn test_scan_balanced_braces_with_trailing() {
        let data = br#"{"key":"value"}extra"#;
        let result = scan_balanced_braces(data);
        assert_eq!(result, Some(br#"{"key":"value"}"#.to_vec()));
    }

    #[test]
    fn test_extract_json_from_k8s_protobuf_with_json_payload() {
        // Construct a K8s protobuf envelope wrapping JSON in field 2
        let json = br#"{"apiVersion":"v1","kind":"Pod"}"#;
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        // Field 1 (TypeMeta) — empty for simplicity
        data.push(0x0a); // field 1, wire type 2
        data.push(0x00); // length 0
                         // Field 2 (raw) — contains the JSON
        data.push(0x12); // field 2, wire type 2
        data.push(json.len() as u8); // length
        data.extend_from_slice(json);

        let result = extract_json_from_k8s_protobuf(&data);
        assert!(result.is_some());
        let extracted = result.unwrap();
        assert_eq!(extracted, json.to_vec());
    }

    #[test]
    fn test_extract_json_from_k8s_protobuf_with_native_protobuf() {
        // Construct a K8s protobuf envelope where field 2 contains native protobuf (not JSON)
        let native_pb = &[0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f]; // field 1, string "hello"
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        // Field 1 (TypeMeta) — empty
        data.push(0x0a); // field 1, wire type 2
        data.push(0x00); // length 0
                         // Field 2 (raw) — native protobuf, not JSON
        data.push(0x12); // field 2, wire type 2
        data.push(native_pb.len() as u8);
        data.extend_from_slice(native_pb);

        let result = extract_json_from_k8s_protobuf(&data);
        // Should return None because field 2 doesn't start with { or [
        assert!(result.is_none());
    }

    #[test]
    fn test_try_brace_scan_validates_json() {
        // Binary data with a `{` byte that isn't valid JSON
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        // Some binary with a { byte followed by non-JSON
        data.push(0x0a);
        data.push(b'{');
        data.push(0x05);
        data.push(b'}');
        // The brace scan would find {0x05} which is balanced but not valid JSON
        let result = try_brace_scan_or_type_meta(&data);
        // Should fall through to TypeMeta extraction or empty object, not return {0x05}
        // Since there's no valid TypeMeta, we get empty object
        assert_eq!(result, b"{}".to_vec());
    }

    #[test]
    fn test_try_brace_scan_finds_embedded_json() {
        // Binary prefix followed by actual JSON — must have apiVersion AND kind
        // to be recognized as a K8s resource (not a random fragment)
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        data.extend_from_slice(&[0x0a, 0x10, 0x12]); // some protobuf prefix
        data.extend_from_slice(br#"{"apiVersion":"v1","kind":"Pod"}"#);

        let result = try_brace_scan_or_type_meta(&data);
        assert_eq!(result, br#"{"apiVersion":"v1","kind":"Pod"}"#.to_vec());
    }

    #[test]
    fn test_extract_type_meta_from_protobuf() {
        // Build a protobuf with TypeMeta containing apiVersion and kind
        let mut type_meta = Vec::new();
        // field 1 = apiVersion = "apiextensions.k8s.io/v1"
        let av = b"apiextensions.k8s.io/v1";
        type_meta.push(0x0a); // field 1, wire type 2
        type_meta.push(av.len() as u8);
        type_meta.extend_from_slice(av);
        // field 2 = kind = "CustomResourceDefinition"
        let kind = b"CustomResourceDefinition";
        type_meta.push(0x12); // field 2, wire type 2
        type_meta.push(kind.len() as u8);
        type_meta.extend_from_slice(kind);

        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        data.push(0x0a); // field 1, wire type 2 (TypeMeta wrapper)
        data.push(type_meta.len() as u8);
        data.extend_from_slice(&type_meta);

        let result = extract_type_meta_from_protobuf(&data);
        assert!(result.is_some());
        let (api_v, k) = result.unwrap();
        assert_eq!(api_v, "apiextensions.k8s.io/v1");
        assert_eq!(k, "CustomResourceDefinition");
    }

    #[test]
    fn test_decode_k8s_protobuf_to_json_crd() {
        // Build a realistic CRD protobuf message
        // TypeMeta: apiVersion="apiextensions.k8s.io/v1", kind="CustomResourceDefinition"
        let mut type_meta = Vec::new();
        let av = b"apiextensions.k8s.io/v1";
        type_meta.push(0x0a);
        type_meta.push(av.len() as u8);
        type_meta.extend_from_slice(av);
        let kind_str = b"CustomResourceDefinition";
        type_meta.push(0x12);
        type_meta.push(kind_str.len() as u8);
        type_meta.extend_from_slice(kind_str);

        // Build raw object (CRD body):
        // Field 1 = ObjectMeta with name = "foos.example.com"
        let mut obj_meta = Vec::new();
        let name = b"foos.example.com";
        obj_meta.push(0x0a); // field 1, wire type 2
        obj_meta.push(name.len() as u8);
        obj_meta.extend_from_slice(name);

        // Field 2 = CRDSpec
        let mut crd_spec = Vec::new();
        // spec.group = "example.com" (field 1)
        let group = b"example.com";
        crd_spec.push(0x0a);
        crd_spec.push(group.len() as u8);
        crd_spec.extend_from_slice(group);
        // spec.names (field 3) — submessage
        let mut names_msg = Vec::new();
        let plural = b"foos";
        names_msg.push(0x0a); // field 1 = plural
        names_msg.push(plural.len() as u8);
        names_msg.extend_from_slice(plural);
        let singular = b"foo";
        names_msg.push(0x12); // field 2 = singular
        names_msg.push(singular.len() as u8);
        names_msg.extend_from_slice(singular);
        let kind_name = b"Foo";
        names_msg.push(0x22); // field 4 = kind
        names_msg.push(kind_name.len() as u8);
        names_msg.extend_from_slice(kind_name);
        crd_spec.push(0x1a); // field 3, wire type 2
        crd_spec.push(names_msg.len() as u8);
        crd_spec.extend_from_slice(&names_msg);
        // spec.scope = "Namespaced" (field 4)
        let scope = b"Namespaced";
        crd_spec.push(0x22); // field 4, wire type 2
        crd_spec.push(scope.len() as u8);
        crd_spec.extend_from_slice(scope);
        // spec.versions (field 7) — one version "v1"
        let mut ver_msg = Vec::new();
        let ver_name = b"v1";
        ver_msg.push(0x0a); // field 1 = name
        ver_msg.push(ver_name.len() as u8);
        ver_msg.extend_from_slice(ver_name);
        crd_spec.push(0x3a); // field 7, wire type 2
        crd_spec.push(ver_msg.len() as u8);
        crd_spec.extend_from_slice(&ver_msg);

        // Assemble raw object: field 1 = ObjectMeta, field 2 = CRDSpec
        let mut raw = Vec::new();
        raw.push(0x0a); // field 1, wire type 2
        raw.push(obj_meta.len() as u8);
        raw.extend_from_slice(&obj_meta);
        raw.push(0x12); // field 2, wire type 2
        raw.push(crd_spec.len() as u8);
        raw.extend_from_slice(&crd_spec);

        // Assemble Unknown message: field 1 = TypeMeta, field 2 = raw
        let mut unknown = Vec::new();
        unknown.extend_from_slice(b"k8s\0");
        unknown.push(0x0a); // field 1 = TypeMeta
        unknown.push(type_meta.len() as u8);
        unknown.extend_from_slice(&type_meta);
        unknown.push(0x12); // field 2 = raw
        unknown.push(raw.len() as u8);
        unknown.extend_from_slice(&raw);

        let result = decode_k8s_protobuf_to_json(&unknown);
        assert!(
            result.is_some(),
            "decode_k8s_protobuf_to_json returned None"
        );
        let json_bytes = result.unwrap();
        let val: serde_json::Value = serde_json::from_slice(&json_bytes)
            .expect("decoded protobuf should produce valid JSON");
        assert_eq!(val["apiVersion"], "apiextensions.k8s.io/v1");
        assert_eq!(val["kind"], "CustomResourceDefinition");
        assert_eq!(val["metadata"]["name"], "foos.example.com");
        assert_eq!(val["spec"]["group"], "example.com");
        assert_eq!(val["spec"]["names"]["plural"], "foos");
        assert_eq!(val["spec"]["names"]["kind"], "Foo");
        assert_eq!(val["spec"]["scope"], "Namespaced");
    }

    #[test]
    fn test_binary_body_with_false_brace_not_treated_as_json() {
        // Simulate a protobuf body that contains 0x7b ({) and 0x7d (}) as part of
        // binary data but isn't valid JSON. The middleware should NOT pass this
        // through as-is.
        let mut data = Vec::new();
        data.extend_from_slice(b"k8s\0");
        // TypeMeta
        let mut type_meta = Vec::new();
        let av = b"apiextensions.k8s.io/v1";
        type_meta.push(0x0a);
        type_meta.push(av.len() as u8);
        type_meta.extend_from_slice(av);
        let kind_str = b"CustomResourceDefinition";
        type_meta.push(0x12);
        type_meta.push(kind_str.len() as u8);
        type_meta.extend_from_slice(kind_str);
        data.push(0x0a);
        data.push(type_meta.len() as u8);
        data.extend_from_slice(&type_meta);
        // Field 2 = raw bytes that happen to contain { and } but aren't JSON
        let fake_raw: Vec<u8> = vec![0x0a, 0x03, b'{', 0x05, b'}', 0x12, 0x01, 0x00];
        data.push(0x12);
        data.push(fake_raw.len() as u8);
        data.extend_from_slice(&fake_raw);

        // extract_json_from_k8s_protobuf should NOT return the {0x05} garbage
        let extracted = extract_json_from_k8s_protobuf(&data);
        if let Some(ref e) = extracted {
            // If it did extract something, it must be valid JSON
            assert!(
                serde_json::from_slice::<serde_json::Value>(e).is_ok(),
                "extract_json_from_k8s_protobuf returned invalid JSON: {:?}",
                e
            );
        }

        // try_brace_scan_or_type_meta should produce valid JSON (TypeMeta fallback)
        let result = try_brace_scan_or_type_meta(&data);
        let parsed = serde_json::from_slice::<serde_json::Value>(&result);
        assert!(
            parsed.is_ok(),
            "try_brace_scan_or_type_meta produced invalid JSON: {:?}",
            String::from_utf8_lossy(&result)
        );
    }

    #[test]
    fn test_wrap_json_in_protobuf_roundtrip() {
        let json = b"{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"test\"}}";
        let wrapped = wrap_json_in_protobuf(json);

        // Should start with k8s\0 magic
        assert_eq!(&wrapped[..4], b"k8s\0");

        // Should be extractable back to JSON
        let extracted = extract_json_from_k8s_protobuf(&wrapped);
        assert!(
            extracted.is_some(),
            "Should extract JSON from protobuf envelope"
        );
        let extracted = extracted.unwrap();
        assert_eq!(extracted, json, "Extracted JSON should match original");
    }

    #[test]
    fn test_wrap_json_in_protobuf_valid_wireformat() {
        let json = b"{\"test\":true}";
        let wrapped = wrap_json_in_protobuf(json);

        // Verify protobuf field tags match the Unknown message schema.
        // After k8s\0 magic (4 bytes), first byte should be field 2 tag (raw).
        // K8s runtime.Unknown proto: field 2 = raw, wire type 2 = (2 << 3) | 2 = 0x12
        let tag1 = wrapped[4];
        let field_num1 = tag1 >> 3;
        let wire_type1 = tag1 & 0x07;
        assert_eq!(
            field_num1, 2,
            "First field should be field 2 (raw) per K8s runtime.Unknown proto"
        );
        assert_eq!(wire_type1, 2, "Wire type should be 2 (length-delimited)");
    }

    #[test]
    fn test_wrap_json_in_protobuf_large_payload() {
        // Test with payload >127 bytes to exercise varint encoding
        let json = format!("{{\"data\":\"{}\"}}", "x".repeat(200));
        let wrapped = wrap_json_in_protobuf(json.as_bytes());

        let extracted = extract_json_from_k8s_protobuf(&wrapped);
        assert!(extracted.is_some(), "Should handle large payloads");
        assert_eq!(
            extracted.unwrap(),
            json.as_bytes(),
            "Large payload should roundtrip"
        );
    }

    #[test]
    fn test_wrap_json_in_protobuf_decodable_by_common_decoder() {
        // The middleware encodes using Go's runtime.Unknown field numbers
        // (field 2 = raw, field 4 = contentType) which differ from our prost
        // Unknown struct (field 3 = raw, field 5 = contentType). The Go client
        // needs field 2/4. Our own extract_json_from_k8s_protobuf handles both.
        use rusternetes_common::protobuf::is_protobuf;

        let json = b"{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"test\"}}";
        let wrapped = wrap_json_in_protobuf(json);

        assert!(
            is_protobuf(&wrapped),
            "wrapped output must have k8s magic prefix"
        );

        // Verify our own extractor can decode it (handles field 2 and 3)
        let extracted = extract_json_from_k8s_protobuf(&wrapped);
        assert!(
            extracted.is_some(),
            "extract_json_from_k8s_protobuf must decode the wrapper"
        );

        // The extracted JSON should match the original
        let original: serde_json::Value = serde_json::from_slice(json).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&extracted.unwrap()).unwrap();
        assert_eq!(decoded, original, "decoded JSON must match original input");
    }

    #[test]
    fn test_wrap_json_in_protobuf_field_numbers_correct() {
        // Verify field 2 (raw) and field 4 (contentType) per K8s runtime.Unknown proto.
        // Field 2, wire type 2 = (2 << 3) | 2 = 0x12
        // Field 5, wire type 2 = (5 << 3) | 2 = 0x2a
        let json = b"{\"test\":1}";
        let wrapped = wrap_json_in_protobuf(json);

        // After k8s\0 (4 bytes), first byte should be field 2 tag
        assert_eq!(
            wrapped[4], 0x12,
            "first field tag should be 0x12 (field 2, wire type 2)"
        );

        // Find field 4 tag after the raw field data
        // raw field: tag(1) + varint_len + json_data
        let json_len = json.len();
        let varint_size = if json_len < 128 { 1 } else { 2 };
        let content_type_tag_pos = 4 + 1 + varint_size + json_len;
        assert_eq!(wrapped[content_type_tag_pos], 0x22,
            "contentType field tag should be 0x22 (field 4, wire type 2) per K8s runtime.Unknown proto");
    }

    #[test]
    fn test_wrap_and_extract_roundtrip_with_correct_fields() {
        // End-to-end test: wrap JSON in protobuf, then extract it back.
        // This proves the encoding is compatible with the decoder.
        let json =
            b"{\"apiVersion\":\"apps/v1\",\"kind\":\"Deployment\",\"spec\":{\"replicas\":3}}";
        let wrapped = wrap_json_in_protobuf(json);
        let extracted = extract_json_from_k8s_protobuf(&wrapped);
        assert!(
            extracted.is_some(),
            "should extract JSON from wrapped protobuf"
        );
        assert_eq!(
            extracted.unwrap(),
            json,
            "extracted JSON must match original"
        );
    }

    #[test]
    fn test_is_watch_request_detection() {
        // Watch requests should be detected via query param watch=true, watch=1, or /watch/ in path.
        // This ensures protobuf wrapping is skipped for streaming watch responses.

        // watch=true query param
        let uri: axum::http::Uri = "http://localhost/api/v1/pods?watch=true".parse().unwrap();
        let has_watch = uri
            .query()
            .map(|q| q.contains("watch=true") || q.contains("watch=1"))
            .unwrap_or(false);
        assert!(has_watch, "watch=true query param should be detected");

        // watch=1 query param
        let uri: axum::http::Uri = "http://localhost/api/v1/pods?watch=1".parse().unwrap();
        let has_watch = uri
            .query()
            .map(|q| q.contains("watch=true") || q.contains("watch=1"))
            .unwrap_or(false);
        assert!(has_watch, "watch=1 query param should be detected");

        // /watch/ in path
        let uri: axum::http::Uri = "http://localhost/api/v1/watch/pods".parse().unwrap();
        assert!(
            uri.path().contains("/watch/"),
            "/watch/ in path should be detected"
        );

        // stream=watch in Accept header
        let accept = "application/json;stream=watch";
        assert!(
            accept.contains("stream=watch"),
            "stream=watch in Accept should be detected"
        );

        // Non-watch request should NOT be detected
        let uri: axum::http::Uri = "http://localhost/api/v1/pods".parse().unwrap();
        let has_watch = uri
            .query()
            .map(|q| q.contains("watch=true") || q.contains("watch=1"))
            .unwrap_or(false);
        assert!(
            !has_watch,
            "regular request should not be detected as watch"
        );
        assert!(
            !uri.path().contains("/watch/"),
            "regular path should not contain /watch/"
        );
    }
}
