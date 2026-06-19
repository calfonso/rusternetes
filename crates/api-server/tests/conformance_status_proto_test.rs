//! Wire-format conformance: `metav1.Status` returned as native protobuf when
//! the client negotiates `application/vnd.kubernetes.protobuf`.
//!
//! Upstream contract (see
//! `staging/src/k8s.io/client-go/rest/request.go::transformResponse` and
//! `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go`): every error
//! response from kube-apiserver carries a `metav1.Status` body. When the
//! client's `Accept` header lists `application/vnd.kubernetes.protobuf`, the
//! server emits the same `Status` wrapped in the `k8s\0` + `Unknown` envelope
//! whose `raw` field is the NATIVE protobuf encoding of `Status` (NOT
//! JSON-in-protobuf). The typed `client-go` decoder will reject a JSON
//! payload behind a protobuf Content-Type as a wire-format error — that's
//! what the missing-proto-Status gap looked like in the canary runs that
//! motivated this fix.
//!
//! The test seeds nothing, asks for a Pod that doesn't exist, and asserts:
//!   1. HTTP status is 404
//!   2. Content-Type is `application/vnd.kubernetes.protobuf`
//!   3. Body starts with the `k8s\0` magic
//!   4. The `Unknown.raw` payload, decoded through
//!      `ProtoRegistry::decode_message("Status", …)`, surfaces the expected
//!      `kind="Status"`, `status="Failure"`, `reason="NotFound"`, `code=404`,
//!      `details.name=<pod-name>` quartet
//!
//! Harness mirrors `decoder_accept_header_test.rs` — in-process axum router
//! over `MemoryStorage`, driven via `tower::ServiceExt::oneshot`.

use axum::http::StatusCode;
use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::Value;

const K8S_MAGIC: &[u8] = b"k8s\0";

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

/// Send a request with an explicit `Accept` (and optional content-type + body);
/// return `(status, response Content-Type, raw body bytes)`.
async fn send_accept(
    router: &TestApiServer,
    method: &str,
    uri: &str,
    accept: &str,
    content_type: Option<&str>,
    body: Option<Vec<u8>>,
) -> (StatusCode, String, Vec<u8>) {
    let mut headers = vec![("accept", accept)];
    if let Some(ct) = content_type {
        headers.push(("content-type", ct));
    }
    let (status, hmap, bytes, _) = router.send_with_headers(method, uri, &headers, body).await;
    let content_type = hmap
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    (status, content_type, bytes)
}

/// Read the `Unknown.raw` (proto field 2, wire type 2) payload out of a K8s
/// protobuf envelope. The envelope is `k8s\0` + a protobuf-encoded `Unknown`
/// message whose `raw` field (tag = 2 << 3 | 2 = 0x12) carries the actual
/// resource bytes.
fn extract_unknown_raw(envelope: &[u8]) -> Vec<u8> {
    assert!(
        envelope.starts_with(K8S_MAGIC),
        "envelope must start with k8s\\0 magic; got first bytes={:?}",
        &envelope[..envelope.len().min(16)],
    );
    let body = &envelope[K8S_MAGIC.len()..];
    let mut pos = 0;
    while pos < body.len() {
        // Decode the field tag (single-byte for fields ≤ 15).
        let tag = body[pos];
        pos += 1;
        let field_num = (tag >> 3) as u32;
        let wire_type = tag & 0x07;
        // We only care about length-delimited fields here.
        assert_eq!(
            wire_type, 2,
            "Unknown is all length-delimited fields, got wire_type={}",
            wire_type,
        );
        // Length is a varint; in practice our payloads are small.
        let (len, consumed) = read_varint(&body[pos..]);
        pos += consumed;
        let end = pos + len as usize;
        assert!(end <= body.len(), "length-delimited field overruns body");
        if field_num == 2 {
            return body[pos..end].to_vec();
        }
        pos = end;
    }
    panic!("Unknown.raw (field 2) not present in envelope");
}

/// Read the `Unknown.typeMeta` (field 1, length-delimited TypeMeta submessage)
/// out of the envelope. Used to assert the envelope carries the
/// `apiVersion=v1, kind=Status` TypeMeta the typed client uses to route the
/// payload through `StatusUnmarshaler`.
fn extract_unknown_type_meta(envelope: &[u8]) -> (String, String) {
    let body = &envelope[K8S_MAGIC.len()..];
    let mut pos = 0;
    while pos < body.len() {
        let tag = body[pos];
        pos += 1;
        let field_num = (tag >> 3) as u32;
        let wire_type = tag & 0x07;
        let (len, consumed) = read_varint(&body[pos..]);
        pos += consumed;
        let end = pos + len as usize;
        if field_num == 1 && wire_type == 2 {
            // Decode the inner TypeMeta { apiVersion=1, kind=2 } message.
            let tm = &body[pos..end];
            let mut tpos = 0;
            let mut api_version = String::new();
            let mut kind = String::new();
            while tpos < tm.len() {
                let ttag = tm[tpos];
                tpos += 1;
                let tfield = (ttag >> 3) as u32;
                let (tlen, tconsumed) = read_varint(&tm[tpos..]);
                tpos += tconsumed;
                let tend = tpos + tlen as usize;
                let s = String::from_utf8_lossy(&tm[tpos..tend]).to_string();
                if tfield == 1 {
                    api_version = s;
                } else if tfield == 2 {
                    kind = s;
                }
                tpos = tend;
            }
            return (api_version, kind);
        }
        pos = end;
    }
    panic!("Unknown.typeMeta (field 1) not present in envelope");
}

fn read_varint(buf: &[u8]) -> (u64, usize) {
    let mut value: u64 = 0;
    let mut shift = 0;
    let mut i = 0;
    loop {
        assert!(i < buf.len(), "truncated varint");
        let byte = buf[i];
        i += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        assert!(shift < 64, "varint exceeds 64 bits");
    }
    (value, i)
}

/// 404 on a missing Pod with `Accept: application/vnd.kubernetes.protobuf`
/// must return the Status as native protobuf (NOT JSON inside an envelope,
/// NOT a plain JSON body). The typed `client-go` decoder is wire-format
/// strict — a JSON Status behind a protobuf Content-Type is treated as a
/// decode error, which is what motivated this fix.
#[tokio::test]
async fn status_404_returns_native_protobuf_when_accept_is_protobuf() {
    let router = spawn_router();

    let (status, content_type, bytes) = send_accept(
        &router,
        "GET",
        "/api/v1/namespaces/default/pods/does-not-exist",
        "application/vnd.kubernetes.protobuf",
        None,
        None,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "missing pod must produce 404; got {status} body={:?}",
        String::from_utf8_lossy(&bytes),
    );
    assert!(
        content_type.starts_with("application/vnd.kubernetes.protobuf"),
        "Content-Type must be protobuf when Accept is protobuf; got {content_type}",
    );
    assert!(
        bytes.starts_with(K8S_MAGIC),
        "body must start with k8s\\0 magic; got first bytes={:?}",
        &bytes[..bytes.len().min(16)],
    );

    // TypeMeta envelope must say apiVersion=v1, kind=Status so client-go's
    // negotiator routes the body through StatusUnmarshaler.
    let (api_version, kind) = extract_unknown_type_meta(&bytes);
    assert_eq!(api_version, "v1", "Unknown.typeMeta.apiVersion must be v1");
    assert_eq!(kind, "Status", "Unknown.typeMeta.kind must be Status");

    // Decode the native Status proto via the registry — this is exactly what
    // the typed client does internally after stripping the Unknown envelope.
    let raw = extract_unknown_raw(&bytes);
    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("Status", &raw)
        .expect("Status schema must be registered and the raw bytes must decode");

    assert_eq!(
        decoded.get("status").and_then(Value::as_str),
        Some("Failure"),
        "Status.status must be 'Failure'; got {decoded}",
    );
    assert_eq!(
        decoded.get("reason").and_then(Value::as_str),
        Some("NotFound"),
        "Status.reason must be 'NotFound'; got {decoded}",
    );
    assert_eq!(
        decoded.get("code").and_then(Value::as_i64),
        Some(404),
        "Status.code must be 404; got {decoded}",
    );
    let message = decoded.get("message").and_then(Value::as_str).unwrap_or("");
    assert!(
        message.contains("does-not-exist"),
        "Status.message must mention the missing pod name; got {message:?}",
    );

    // StatusDetails must surface so typed clients can branch on the failure.
    // The exact `details.name` shape is set by the pre-existing
    // `extract_resource_details` helper in `crates/common/src/error.rs` —
    // upstream parity for that field is tracked separately. Here we only
    // assert that the StatusDetails round-trips through the proto encoder
    // and surfaces a non-empty `name` referencing the missing pod.
    let details = decoded
        .get("details")
        .unwrap_or_else(|| panic!("Status.details must be populated for NotFound; got {decoded}"));
    let details_name = details
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("Status.details.name must be populated; got {details}"));
    assert!(
        details_name.contains("does-not-exist"),
        "Status.details.name must reference the missing pod; got {details_name:?}",
    );
}

/// `Accept: application/json` is the canonical client-go default for tests
/// that don't speak protobuf. The server must keep producing JSON (no
/// envelope) so the existing JSON path isn't regressed by the protobuf
/// branch. Pairs with the test above to lock the negotiation matrix.
#[tokio::test]
async fn status_404_stays_json_when_accept_is_json() {
    let router = spawn_router();

    let (status, content_type, bytes) = send_accept(
        &router,
        "GET",
        "/api/v1/namespaces/default/pods/does-not-exist",
        "application/json",
        None,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        content_type.starts_with("application/json"),
        "Content-Type must remain JSON when Accept is JSON; got {content_type}",
    );
    assert!(
        !bytes.starts_with(K8S_MAGIC),
        "JSON path must not emit the k8s\\0 envelope; got first bytes={:?}",
        &bytes[..bytes.len().min(16)],
    );
    let v: Value = serde_json::from_slice(&bytes).expect("JSON body must parse");
    assert_eq!(v["kind"], "Status");
    assert_eq!(v["reason"], "NotFound");
    assert_eq!(v["code"], 404);
}

/// Regression guard for the kind-defaulting bug. `metav1.Status` has
/// `#[serde(default = "default_status_kind")]` on its `kind` field, so a
/// naive `serde_json::from_slice::<Status>(...)` succeeds on ANY JSON body
/// — the `kind` is silently defaulted to the literal string `"Status"`.
/// Without an explicit `"kind":"Status"` check on the source bytes, a
/// proto-Accept request to a handler that returns plain JSON (no top-level
/// `kind`, e.g. `/version` returning a `VersionInfo`) would have its body
/// silently replaced with an empty Status proto envelope. The middleware
/// must parse the JSON to `Value` and gate the re-encode on the explicit
/// `kind` field.
#[tokio::test]
async fn non_status_json_passes_through_unchanged_when_accept_is_protobuf() {
    let router = spawn_router();

    // `/version` is the canonical kind-less JSON endpoint: `VersionInfo`
    // serialises to a flat object without `apiVersion` or `kind`.
    let (status, content_type, bytes) = send_accept(
        &router,
        "GET",
        "/version",
        "application/vnd.kubernetes.protobuf",
        None,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "/version must return 200");
    assert!(
        !bytes.starts_with(K8S_MAGIC),
        "kind-less JSON body must NOT be wrapped in the k8s\\0 envelope; got first bytes={:?}",
        &bytes[..bytes.len().min(16)],
    );
    assert!(
        content_type.starts_with("application/json"),
        "kind-less JSON must keep `application/json` Content-Type; got {content_type}",
    );

    // Body must remain the canonical VersionInfo JSON with the literal
    // field set the handler produced — gitVersion etc. — and crucially must
    // NOT have been replaced with an empty Status proto envelope.
    let v: Value = serde_json::from_slice(&bytes).expect("body must be JSON");
    assert!(
        v.get("gitVersion").and_then(Value::as_str).is_some(),
        "/version response must contain `gitVersion`; got {v}",
    );
}

/// A 422 Invalid response from the field-validation layer must round-trip
/// the populated `Status.details.causes[]` through the native proto encoder.
/// Each cause carries `(type, message, field)`; typed clients walk those to
/// surface per-field error toasts. Lose any of them on the wire and the
/// `pod-validation` and `apply` conformance suites flag an opaque
/// "invalid request" instead of the field-specific error.
///
/// Trigger path: POST a Pod with empty `spec.containers` — the create
/// handler in `crates/api-server/src/handlers/pod.rs` builds a
/// `field::ErrorList` via `validation.go::ValidatePodSpec` parity and returns
/// `Error::Invalid(errs)`, which `crates/common/src/error.rs::IntoResponse`
/// renders as a 422 Status whose `details.causes[]` mirrors every entry.
#[tokio::test]
async fn status_422_invalid_pod_round_trips_causes_as_native_protobuf() {
    let router = spawn_router();

    // Pod body with an empty `spec.containers` list — the create validator
    // accumulates `spec.containers: Required value` as the first cause.
    let body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "no-containers", "namespace": "default" },
        "spec": { "containers": [] },
    });

    let (status, content_type, bytes) = send_accept(
        &router,
        "POST",
        "/api/v1/namespaces/default/pods",
        "application/vnd.kubernetes.protobuf",
        Some("application/json"),
        Some(serde_json::to_vec(&body).unwrap()),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "empty containers must produce 422; got {status} body={:?}",
        String::from_utf8_lossy(&bytes),
    );
    assert!(
        content_type.starts_with("application/vnd.kubernetes.protobuf"),
        "Content-Type must be protobuf; got {content_type}",
    );
    assert!(
        bytes.starts_with(K8S_MAGIC),
        "body must start with k8s\\0 magic; got first bytes={:?}",
        &bytes[..bytes.len().min(16)],
    );

    let raw = extract_unknown_raw(&bytes);
    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("Status", &raw)
        .expect("Status schema must be registered and the raw bytes must decode");

    assert_eq!(
        decoded.get("status").and_then(Value::as_str),
        Some("Failure"),
        "Status.status must be 'Failure'; got {decoded}",
    );
    assert_eq!(
        decoded.get("reason").and_then(Value::as_str),
        Some("Invalid"),
        "Status.reason must be 'Invalid'; got {decoded}",
    );
    assert_eq!(
        decoded.get("code").and_then(Value::as_i64),
        Some(422),
        "Status.code must be 422; got {decoded}",
    );

    let details = decoded
        .get("details")
        .unwrap_or_else(|| panic!("Status.details must be populated for Invalid; got {decoded}"));
    let causes = details
        .get("causes")
        .and_then(Value::as_array)
        .unwrap_or_else(|| {
            panic!("Status.details.causes must be a populated array; got {details}")
        });
    assert!(
        !causes.is_empty(),
        "Status.details.causes must carry at least one entry; got {details}",
    );

    // At least one cause must surface the (reason, message, field) triple
    // on the wire — clients walk this triple to surface per-field errors.
    let has_field_cause = causes.iter().any(|c| {
        let reason = c.get("reason").and_then(Value::as_str).unwrap_or("");
        let message = c.get("message").and_then(Value::as_str).unwrap_or("");
        let field = c.get("field").and_then(Value::as_str).unwrap_or("");
        !reason.is_empty() && !message.is_empty() && !field.is_empty()
    });
    assert!(
        has_field_cause,
        "at least one cause must carry reason/message/field intact; got {causes:?}",
    );

    // And the `spec.containers` field path must be reachable through the
    // proto round-trip — that's the load-bearing signal for typed clients.
    let mentions_containers = causes.iter().any(|c| {
        c.get("field")
            .and_then(Value::as_str)
            .map(|f| f.contains("spec.containers"))
            .unwrap_or(false)
    });
    assert!(
        mentions_containers,
        "at least one cause.field must reference spec.containers; got {causes:?}",
    );
}

/// 200 Status responses (e.g. `deleteCollection` success) must also be
/// re-encoded as native protobuf when the client negotiates it. Upstream
/// `responsewriters/writers.go::SerializeObject` encodes any `runtime.Object`
/// — Status included — as proto when negotiated, regardless of HTTP status.
/// Trigger: `DELETE /apis/apps/v1/namespaces/default/statefulsets` which
/// returns the K8s-canonical `{kind:Status, status:Success, code:200}`.
#[tokio::test]
async fn status_200_success_returns_native_protobuf_when_accept_is_protobuf() {
    let router = spawn_router();

    let (status, content_type, bytes) = send_accept(
        &router,
        "DELETE",
        "/apis/apps/v1/namespaces/default/statefulsets",
        "application/vnd.kubernetes.protobuf",
        None,
        None,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "deleteCollection (empty) must return 200; got {status} body={:?}",
        String::from_utf8_lossy(&bytes),
    );
    assert!(
        content_type.starts_with("application/vnd.kubernetes.protobuf"),
        "Content-Type must be protobuf; got {content_type}",
    );
    assert!(
        bytes.starts_with(K8S_MAGIC),
        "body must start with k8s\\0 magic; got first bytes={:?}",
        &bytes[..bytes.len().min(16)],
    );

    let (api_version, kind) = extract_unknown_type_meta(&bytes);
    assert_eq!(api_version, "v1", "Unknown.typeMeta.apiVersion must be v1");
    assert_eq!(kind, "Status", "Unknown.typeMeta.kind must be Status");

    let raw = extract_unknown_raw(&bytes);
    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("Status", &raw)
        .expect("Status proto must decode");
    assert_eq!(
        decoded.get("status").and_then(Value::as_str),
        Some("Success"),
        "Status.status must be 'Success' for a successful deleteCollection; got {decoded}",
    );
    assert_eq!(
        decoded.get("code").and_then(Value::as_i64),
        Some(200),
        "Status.code must be 200; got {decoded}",
    );
}

/// The client-go default Accept is the dual `protobuf, json` header. Per
/// upstream contract the server picks the highest-precedence supported type;
/// for Status responses Rusternetes now supports protobuf, so this case must
/// emit the protobuf envelope (same as the explicit-protobuf case above).
#[tokio::test]
async fn status_404_with_protobuf_then_json_accept_picks_protobuf() {
    let router = spawn_router();

    let (status, content_type, bytes) = send_accept(
        &router,
        "GET",
        "/api/v1/namespaces/default/pods/does-not-exist",
        "application/vnd.kubernetes.protobuf, application/json",
        None,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        content_type.starts_with("application/vnd.kubernetes.protobuf"),
        "dual Accept must pick protobuf for Status; got {content_type}",
    );
    assert!(
        bytes.starts_with(K8S_MAGIC),
        "body must start with k8s\\0 magic; got first bytes={:?}",
        &bytes[..bytes.len().min(16)],
    );
    let raw = extract_unknown_raw(&bytes);
    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("Status", &raw)
        .expect("Status proto must decode");
    assert_eq!(
        decoded.get("reason").and_then(Value::as_str),
        Some("NotFound"),
    );
}
