//! Layer-4 decoder tests: malformed JSON request bodies must produce a valid
//! Kubernetes `Status` failure response, NEVER plain text and NEVER an empty
//! body.
//!
//! Upstream parity: `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/json/json.go`
//! and the apiserver's `request.NegotiateInputSerializer` path reject malformed
//! payloads at decode time and emit a Status object via `responsewriters.ErrorNegotiated`.
//! The status code is typically `400 BadRequest` (decode error) or `422 Invalid`
//! (semantic decode error such as wrong-type field on a known schema).
//!
//! rusternetes currently routes every body-decode failure through
//! `Error::InvalidResource` (see `crates/api-server/src/handlers/pod.rs:31`),
//! which maps to HTTP 422 with `reason=Invalid` in `crates/common/src/error.rs:105-108`.
//! Either 400 or 422 is acceptable per upstream — the contract these tests pin
//! is:
//!   - response status is 4xx (one of {400, 422})
//!   - response body parses as a Kubernetes `Status` object
//!     (`kind=Status`, `apiVersion=v1`, `status=Failure`,
//!     `reason ∈ {BadRequest, Invalid}`)
//!   - response body is NOT empty and NOT plain text
//!
//! Each `test_malformed_<scenario>` POSTs a malformed body to
//! `/api/v1/namespaces/default/pods` and asserts the contract above.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::Value;

// ---------------------------------------------------------------------------
// HTTP harness — thin wrappers over the shared `TestApiServer`, preserving this
// file's `spawn_router()` / `post_pod_raw(router, body)` call sites. Malformed
// bodies (truncated JSON, invalid UTF-8) go through `send_bytes`, which accepts
// arbitrary raw bytes instead of a `serde_json::Value`.
// ---------------------------------------------------------------------------

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

/// Send a raw byte body to the pod create endpoint and return
/// `(status, body_bytes)`.
async fn post_pod_raw(router: TestApiServer, body: Vec<u8>) -> (u16, Vec<u8>) {
    let (status, bytes, _) = router
        .send_bytes(
            "POST",
            "/api/v1/namespaces/default/pods",
            Some("application/json"),
            Some(body),
        )
        .await;
    (status.as_u16(), bytes)
}

/// Assert the contract described in the file-level docs:
///   - status is 400 or 422
///   - body parses as a Kubernetes `Status` failure object
///     (`kind=Status`, `apiVersion=v1`, `status=Failure`,
///     `reason ∈ {BadRequest, Invalid}`)
///   - body is non-empty
fn assert_status_contract(scenario: &str, status: u16, body: &[u8]) {
    assert!(
        status == 400 || status == 422,
        "[{}] expected HTTP 400 or 422 for malformed body, got {} body={:?}",
        scenario,
        status,
        String::from_utf8_lossy(body),
    );
    assert!(
        !body.is_empty(),
        "[{}] response body must NOT be empty",
        scenario,
    );
    let v: Value = serde_json::from_slice(body).unwrap_or_else(|e| {
        panic!(
            "[{}] response body must be valid JSON (a Status object), got {:?}: parse error {}",
            scenario,
            String::from_utf8_lossy(body),
            e,
        )
    });
    assert_eq!(
        v["kind"], "Status",
        "[{}] response must be a Status object, got body={}",
        scenario, v
    );
    assert_eq!(
        v["apiVersion"], "v1",
        "[{}] Status.apiVersion must be v1, got body={}",
        scenario, v
    );
    assert_eq!(
        v["status"], "Failure",
        "[{}] Status.status must be Failure, got body={}",
        scenario, v
    );
    let reason = v["reason"].as_str().unwrap_or("");
    assert!(
        reason == "BadRequest" || reason == "Invalid",
        "[{}] Status.reason must be BadRequest or Invalid, got {:?} body={}",
        scenario,
        reason,
        v,
    );
}

#[tokio::test]
async fn test_malformed_empty_body() {
    let router = spawn_router();
    let (status, body) = post_pod_raw(router, Vec::new()).await;
    assert_status_contract("empty_body", status, &body);
}

#[tokio::test]
async fn test_malformed_truncated_open_brace() {
    let router = spawn_router();
    let (status, body) = post_pod_raw(router, b"{".to_vec()).await;
    assert_status_contract("truncated_open_brace", status, &body);
}

#[tokio::test]
async fn test_malformed_truncated_open_bracket() {
    let router = spawn_router();
    let (status, body) = post_pod_raw(router, b"[".to_vec()).await;
    assert_status_contract("truncated_open_bracket", status, &body);
}

#[tokio::test]
async fn test_malformed_truncated_key_only() {
    let router = spawn_router();
    let (status, body) = post_pod_raw(router, br#"{"kind":"#.to_vec()).await;
    assert_status_contract("truncated_key_only", status, &body);
}

// `serde_json::from_slice` requires UTF-8 input; raw 0xff 0xfe inside a
// string value is rejected at parse time.
#[tokio::test]
async fn test_malformed_invalid_utf8() {
    let router = spawn_router();
    let mut body = Vec::from(&b"{\"name\":\""[..]);
    body.extend_from_slice(&[0xffu8, 0xfeu8]);
    body.extend_from_slice(b"\"}");
    let (status, body) = post_pod_raw(router, body).await;
    assert_status_contract("invalid_utf8", status, &body);
}

// Closest Pod analogue to upstream's `Deployment{"spec":{"replicas":"three"}}`
// case: top-level `metadata` is a non-Option field expecting an object.
#[tokio::test]
async fn test_malformed_wrong_type_metadata() {
    let router = spawn_router();
    let (status, body) = post_pod_raw(router, br#"{"metadata":"not-an-object"}"#.to_vec()).await;
    assert_status_contract("wrong_type_metadata", status, &body);
}

// `spec.activeDeadlineSeconds` is `Option<i64>` in the schema; sending a
// string forces a deserialization error.
#[tokio::test]
async fn test_malformed_wrong_type_active_deadline() {
    let router = spawn_router();
    let body = br#"{"metadata":{"name":"p"},"spec":{"activeDeadlineSeconds":"three","containers":[{"name":"c","image":"i"}]}}"#.to_vec();
    let (status, body) = post_pod_raw(router, body).await;
    assert_status_contract("wrong_type_active_deadline", status, &body);
}

// Upstream Kubernetes treats `spec` as required-non-nullable on PodTemplateSpec /
// Pod: PodSpec validation ("spec.containers: Required value") fires when `spec`
// is null/absent. In rusternetes, `Pod.spec` is `Option<PodSpec>`
// (crates/common/src/resources/pod.rs:50), so `spec: null` deserializes to
// `None` and the create handler at crates/api-server/src/handlers/pod.rs:63
// only validates containers when `spec` is `Some`. Result: 201 with an empty
// Pod instead of 422 Invalid + Status.
#[tokio::test]
async fn test_malformed_spec_null() {
    let router = spawn_router();
    let body = br#"{"metadata":{"name":"p"},"spec":null}"#.to_vec();
    let (status, body) = post_pod_raw(router, body).await;
    assert_status_contract("spec_null", status, &body);
}

#[tokio::test]
async fn test_malformed_integer_overflow() {
    let router = spawn_router();
    // 9223372036854775808 = i64::MAX + 1 — cannot fit Option<i64>.
    let body = br#"{"metadata":{"name":"p"},"spec":{"activeDeadlineSeconds":9223372036854775808,"containers":[{"name":"c","image":"i"}]}}"#.to_vec();
    let (status, body) = post_pod_raw(router, body).await;
    assert_status_contract("integer_overflow", status, &body);
}

// `serde_json` enforces a recursion limit (~128 by default) to prevent stack
// overflow on adversarial input — 10_000 nested arrays produce a syntax error.
// Pins the contract: rusternetes (via serde_json) REJECTS deeply nested JSON.
#[tokio::test]
async fn test_malformed_deeply_nested() {
    let router = spawn_router();
    let depth = 10_000usize;
    let mut body = vec![b'['; depth];
    body.resize(depth * 2, b']');
    let (status, body) = post_pod_raw(router, body).await;
    assert_status_contract("deeply_nested", status, &body);
}

// `serde_json::from_slice` reads to end-of-input and reports `trailing
// characters` when garbage follows a complete value.
#[tokio::test]
async fn test_malformed_trailing_garbage() {
    let router = spawn_router();
    let (status, body) = post_pod_raw(router, br#"{"kind":"Pod"}garbage"#.to_vec()).await;
    assert_status_contract("trailing_garbage", status, &body);
}

// serde_json default: last-wins on duplicate keys. The decoded Pod has no
// spec → no containers, so handler validation returns 422 Invalid + Status.
// Contract pinned here: Status object, not 2xx.
#[tokio::test]
async fn test_malformed_duplicate_keys() {
    let router = spawn_router();
    let body = br#"{"kind":"Pod","kind":"Service","metadata":{"name":"p"}}"#.to_vec();
    let (status, body) = post_pod_raw(router, body).await;
    assert_status_contract("duplicate_keys", status, &body);
}

#[tokio::test]
async fn test_malformed_bare_string() {
    let router = spawn_router();
    let (status, body) = post_pod_raw(router, br#""hello""#.to_vec()).await;
    assert_status_contract("bare_string", status, &body);
}

#[tokio::test]
async fn test_malformed_bare_array() {
    let router = spawn_router();
    let (status, body) = post_pod_raw(router, br#"[]"#.to_vec()).await;
    assert_status_contract("bare_array", status, &body);
}
