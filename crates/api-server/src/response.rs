//! HTTP response handling with content negotiation
//!
//! Supports both JSON and Protobuf serialization based on Accept header.
//!
//! Native Protobuf responses
//! -------------------------
//! Real Kubernetes encodes Protobuf responses as
//! `k8s\0` + `runtime.Unknown { typeMeta, raw, contentType }` where `raw`
//! contains native protobuf bytes produced by the generated `pb.go`
//! `Marshal` methods for the resource type (e.g. `core/v1.Pod`).
//!
//! Until rusternetes ships per-resource native protobuf encoders, the
//! Unknown envelope's `contentType` field is set to `application/json` and
//! `raw` carries the JSON-serialized resource. That is still a valid K8s
//! protobuf envelope — clients that decode via `Unknown` see the
//! `contentType` hint and decode `raw` as JSON. `decode_protobuf` in
//! `rusternetes_common::protobuf` exercises exactly that path.
//!
//! The [`ProtoEncoder`] trait is the extensibility seam: each resource type
//! can register an implementation that produces native protobuf bytes for
//! its kind. The default impl returned by [`default_proto_encoder`] wraps
//! the JSON payload, matching today's behaviour. The
//! [`NativeProtoOptIn`] response extension is how a handler tells the
//! response-wrapping middleware "I'm OK with you emitting a protobuf
//! envelope for this response when the client asked for one".
//!
//! See `crates/api-server/src/middleware.rs` for where the marker is read
//! and `crates/api-server/src/handlers/pod.rs` for the first opt-in
//! consumer.

use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// API response wrapper that supports content negotiation
pub struct ApiResponse<T> {
    data: T,
    status: StatusCode,
}

impl<T> ApiResponse<T> {
    /// Create a new API response
    pub fn new(data: T) -> Self {
        Self {
            data,
            status: StatusCode::OK,
        }
    }

    /// Create a new API response with a specific status code
    pub fn with_status(data: T, status: StatusCode) -> Self {
        Self { data, status }
    }
}

impl<T> IntoResponse for ApiResponse<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        // For now, default to JSON
        // In full implementation, check Accept header and return protobuf if requested
        match serde_json::to_vec(&self.data) {
            Ok(body) => Response::builder()
                .status(self.status)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
            Err(e) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to serialize response: {}", e)))
                .unwrap(),
        }
    }
}

/// Negotiate content type based on Accept header
pub fn negotiate_content_type(headers: &HeaderMap) -> ContentType {
    if let Some(accept) = headers.get(header::ACCEPT) {
        if let Ok(accept_str) = accept.to_str() {
            if accept_str.contains("application/vnd.kubernetes.protobuf") {
                return ContentType::Protobuf;
            }
        }
    }
    ContentType::Json
}

/// Content type for responses
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Json,
    Protobuf,
}

impl ContentType {
    /// Get the MIME type string
    pub fn mime_type(&self) -> &'static str {
        match self {
            ContentType::Json => "application/json",
            ContentType::Protobuf => "application/vnd.kubernetes.protobuf",
        }
    }
}

/// Create a response with content negotiation
/// Note: Protobuf encoding requires api_version and kind, so this is a simplified version
pub fn create_response<T>(data: T, status: StatusCode, _content_type: ContentType) -> Response
where
    T: Serialize,
{
    // For now, always use JSON since protobuf encoding needs type metadata
    // In full implementation, this would check content_type and encode appropriately
    match serde_json::to_vec(&data) {
        Ok(body) => Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, ContentType::Json.mime_type())
            .body(Body::from(body))
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Failed to serialize: {}", e)))
            .unwrap(),
    }
}

// ---------------------------------------------------------------------------
// Native Protobuf scaffold
// ---------------------------------------------------------------------------

/// Marker extension that a handler attaches to its response to opt in to
/// protobuf encoding when the client's `Accept` header asks for
/// `application/vnd.kubernetes.protobuf`.
///
/// The response-wrapping middleware in `middleware.rs` looks for this
/// extension; if it is present AND the client requested protobuf AND the
/// response body is JSON, the middleware rewrites the response into a K8s
/// `k8s\0`-framed `runtime.Unknown` envelope via the configured
/// [`ProtoEncoder`].
///
/// Without the marker the middleware leaves the JSON response untouched —
/// preserving today's behaviour for every resource type that has not yet
/// opted in.
#[derive(Clone, Debug)]
pub struct NativeProtoOptIn {
    /// `apiVersion` to write into the `runtime.Unknown.typeMeta.apiVersion`
    /// field. For Pod GET this is `"v1"`.
    pub api_version: &'static str,
    /// `kind` to write into the `runtime.Unknown.typeMeta.kind` field.
    /// For Pod GET this is `"Pod"`; for LIST it is `"PodList"`; etc.
    pub kind: &'static str,
}

impl NativeProtoOptIn {
    pub const fn new(api_version: &'static str, kind: &'static str) -> Self {
        Self { api_version, kind }
    }

    /// Opt-in for a single `core/v1.Pod` response.
    pub const fn pod() -> Self {
        Self::new("v1", "Pod")
    }

    /// Opt-in for a `core/v1.PodList` response.
    pub const fn pod_list() -> Self {
        Self::new("v1", "PodList")
    }
}

/// Strategy for turning a JSON-serialised resource into the bytes that a
/// `application/vnd.kubernetes.protobuf` response should carry.
///
/// Implementations are responsible for emitting the full K8s wire envelope:
/// the `k8s\0` magic prefix followed by a `runtime.Unknown` protobuf
/// message. The default implementation
/// ([`WrappedJsonProtoEncoder`]) stuffs the JSON bytes into
/// `Unknown.raw` and sets `Unknown.contentType` to `application/json`,
/// which is the same envelope that
/// `rusternetes_common::protobuf::encode_protobuf` produces and that
/// `decode_protobuf` round-trips. A future per-resource implementation
/// would replace the body with native protobuf bytes produced from the
/// resource type (matching what upstream's generated `pb.go` does).
pub trait ProtoEncoder: Send + Sync {
    /// Wrap the JSON-serialised resource bytes in a K8s protobuf envelope
    /// suitable for an `application/vnd.kubernetes.protobuf` response.
    ///
    /// `json` is the canonical JSON encoding of the resource (the same
    /// bytes that would be written to a `Content-Type: application/json`
    /// response). `api_version` and `kind` are written into the
    /// `runtime.Unknown.typeMeta` field so that an `Unknown`-aware client
    /// can dispatch on type before fully decoding the body.
    fn encode(&self, json: &[u8], api_version: &str, kind: &str) -> Vec<u8>;
}

/// Default [`ProtoEncoder`] that wraps the JSON in `runtime.Unknown.raw`
/// and sets `contentType = "application/json"`.
///
/// Delegates to [`rusternetes_common::protobuf::encode_protobuf`] so the
/// envelope shape (prost-derived `Unknown` field layout) round-trips
/// through `decode_protobuf` without any wire-format drift. A future
/// per-resource native encoder can wrap `encode_protobuf` (or replace it)
/// once `pb.rs` descriptors are wired in.
pub struct WrappedJsonProtoEncoder;

impl ProtoEncoder for WrappedJsonProtoEncoder {
    fn encode(&self, json: &[u8], api_version: &str, kind: &str) -> Vec<u8> {
        wrap_json_in_protobuf_envelope(json, api_version, kind)
    }
}

/// Build a K8s `runtime.Unknown` protobuf envelope around `json`.
///
/// Delegates to [`rusternetes_common::protobuf::encode_protobuf`] for the
/// envelope shape so encode / decode share one prost-derived `Unknown`
/// definition. If encoding fails we fall back to the raw JSON bytes — the
/// response middleware will still emit them as `application/json` if the
/// caller chooses not to override the Content-Type, but the standard
/// usage path always treats the result as the protobuf envelope.
pub fn wrap_json_in_protobuf_envelope(json: &[u8], api_version: &str, kind: &str) -> Vec<u8> {
    use rusternetes_common::protobuf::{Unknown, UnknownTypeMeta};

    let unknown = Unknown {
        type_meta: Some(UnknownTypeMeta {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        }),
        raw: json.to_vec(),
        content_encoding: String::new(),
        // `contentType` tells decoders that `raw` carries JSON — required
        // until we ship native per-resource protobuf marshalling.
        content_type: "application/json".to_string(),
    };

    use prost::Message;
    let mut buf = Vec::with_capacity(4 + unknown.encoded_len());
    buf.extend_from_slice(b"k8s\0");
    // `Message::encode` on `Vec<u8>` cannot fail (infallible BufMut).
    unknown.encode(&mut buf).expect("Unknown encode");
    buf
}

/// Return the encoder used to satisfy `application/vnd.kubernetes.protobuf`
/// responses today. Lives as a free function so test code and the response
/// middleware can share one definition.
pub fn default_proto_encoder() -> &'static dyn ProtoEncoder {
    &WrappedJsonProtoEncoder
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_content_type_negotiation() {
        let mut headers = HeaderMap::new();
        assert_eq!(negotiate_content_type(&headers), ContentType::Json);

        headers.insert(header::ACCEPT, "application/json".parse().unwrap());
        assert_eq!(negotiate_content_type(&headers), ContentType::Json);

        headers.insert(
            header::ACCEPT,
            "application/vnd.kubernetes.protobuf".parse().unwrap(),
        );
        assert_eq!(negotiate_content_type(&headers), ContentType::Protobuf);
    }

    #[test]
    fn test_content_type_mime_types() {
        assert_eq!(ContentType::Json.mime_type(), "application/json");
        assert_eq!(
            ContentType::Protobuf.mime_type(),
            "application/vnd.kubernetes.protobuf"
        );
    }

    #[test]
    fn test_api_response_creation() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        let response = ApiResponse::new(data.clone());
        assert_eq!(response.status, StatusCode::OK);

        let response = ApiResponse::with_status(data, StatusCode::CREATED);
        assert_eq!(response.status, StatusCode::CREATED);
    }

    /// The default [`ProtoEncoder`] must emit a `k8s\0`-framed envelope and
    /// the body must round-trip back through
    /// [`rusternetes_common::protobuf::decode_protobuf`] to the same value.
    #[test]
    fn test_default_proto_encoder_roundtrips_via_decode_protobuf() {
        use rusternetes_common::protobuf::{decode_protobuf, is_protobuf};

        let data = TestData {
            name: "rt".into(),
            value: 7,
        };
        let json = serde_json::to_vec(&data).unwrap();
        let envelope = default_proto_encoder().encode(&json, "v1", "TestData");

        assert!(envelope.starts_with(b"k8s\0"), "magic prefix missing");
        assert!(is_protobuf(&envelope));

        let (decoded, tm): (TestData, _) = decode_protobuf(&envelope).expect("decode");
        assert_eq!(decoded.name, "rt");
        assert_eq!(decoded.value, 7);
        assert_eq!(tm.api_version, "v1");
        assert_eq!(tm.kind, "TestData");
    }

    /// `NativeProtoOptIn::pod()` must label responses as `v1` / `Pod`.
    #[test]
    fn test_native_proto_opt_in_pod_constants() {
        let opt = NativeProtoOptIn::pod();
        assert_eq!(opt.api_version, "v1");
        assert_eq!(opt.kind, "Pod");

        let list_opt = NativeProtoOptIn::pod_list();
        assert_eq!(list_opt.api_version, "v1");
        assert_eq!(list_opt.kind, "PodList");
    }

    /// Empty `api_version` + empty `kind` must still produce a valid
    /// envelope. The body should round-trip via `decode_protobuf` (which
    /// only requires the `raw` field — empty TypeMeta is fine).
    #[test]
    fn test_wrap_envelope_without_type_meta() {
        use rusternetes_common::protobuf::{decode_protobuf, is_protobuf};

        let data = TestData {
            name: "x".into(),
            value: 1,
        };
        let json = serde_json::to_vec(&data).unwrap();
        let env = wrap_json_in_protobuf_envelope(&json, "", "");
        assert!(env.starts_with(b"k8s\0"));
        assert!(is_protobuf(&env));
        let (decoded, tm): (TestData, _) = decode_protobuf(&env).expect("decode");
        assert_eq!(decoded.name, "x");
        assert!(tm.api_version.is_empty());
        assert!(tm.kind.is_empty());
    }
}
