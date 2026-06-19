//! Conformance tests for the `application/cbor` codec.
//!
//! Upstream Kubernetes ships a CBOR serializer in
//! `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/cbor/` and registers
//! it alongside JSON / YAML / protobuf so clients can negotiate a binary wire
//! format that does NOT require generated `.proto` definitions. The two media
//! types involved are:
//!
//!   - `application/cbor`               — full object encoding (POST / PUT / GET / LIST)
//!   - `application/apply-patch+cbor`   — server-side apply patches
//!
//! Rusternetes routes these media types through
//! `crates/api-server/src/middleware.rs`. The request branch decodes the CBOR
//! body into a JSON value before any handler sees it; the response branch
//! re-encodes the JSON value as CBOR when the client negotiated CBOR via the
//! `Accept` header (or simply sent a CBOR request body without overriding the
//! `Accept`). The handlers themselves remain JSON-only — this matches
//! upstream's architecture where the negotiation layer sits in front of the
//! per-resource Storage handler.
//!
//! Each test below pins one observable contract of that pipeline:
//!
//!   - `test_post_cbor_configmap_decoded_and_stored`
//!     POST `application/cbor` → server decodes → ConfigMap appears in
//!     storage with the same `.data` map.
//!
//!   - `test_get_cbor_round_trip`
//!     GET with `Accept: application/cbor` → response is CBOR bytes whose
//!     transcoded JSON matches the JSON shape of the resource.
//!
//!   - `test_crd_handler_accepts_cbor_body`
//!     Pre-fix, `crates/api-server/src/handlers/crd.rs` rejected any
//!     non-JSON first byte with HTTP 415. The fix moves CBOR decode into
//!     the middleware, so CRD creation with `application/cbor` now
//!     succeeds.
//!
//!   - `test_malformed_cbor_returns_400`
//!     Random bytes sent with `application/cbor` must produce a 4xx, not
//!     a panic and not silent data corruption.

use axum::http::{Method, StatusCode};
use rusternetes_api_server::cbor;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

const TEST_NS: &str = "default";

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send(
    router: TestApiServer,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> (StatusCode, String, Vec<u8>) {
    let (status, hmap, bytes, _) = router
        .send_with_headers(method.as_str(), uri, headers, Some(body))
        .await;
    let content_type = hmap
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    (status, content_type, bytes)
}

/// POST a ConfigMap encoded as CBOR. The middleware must decode the body
/// before the handler runs so the resource lands in storage.
#[tokio::test]
async fn test_post_cbor_configmap_decoded_and_stored() {
    let (mem, router) = spawn_router();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cbor-cm", "namespace": TEST_NS},
        "data": {"hello": "world", "k": "v"}
    });
    let cbor_body = cbor::encode_json_to_cbor(&cm).expect("encode CBOR");

    let (status, _ct, body) = send(
        router,
        Method::POST,
        "/api/v1/namespaces/default/configmaps",
        &[("content-type", "application/cbor")],
        cbor_body,
    )
    .await;

    assert!(
        status.is_success(),
        "POST application/cbor should succeed; got {} body={}",
        status,
        String::from_utf8_lossy(&body)
    );

    let stored = mem
        .get::<Value>(&build_key("configmaps", Some(TEST_NS), "cbor-cm"))
        .await
        .expect("ConfigMap must be persisted");
    assert_eq!(stored["metadata"]["name"], "cbor-cm");
    assert_eq!(stored["data"]["hello"], "world");
    assert_eq!(stored["data"]["k"], "v");
}

/// GET with `Accept: application/cbor` returns the resource encoded as CBOR.
/// We round-trip the bytes back into a JSON value and compare against the
/// seeded shape to prove the negotiation + encoding pipeline is lossless.
#[tokio::test]
async fn test_get_cbor_round_trip() {
    let (mem, router) = spawn_router();

    // Seed the resource directly via storage so this test does NOT depend on
    // the POST path also being CBOR-correct (POST is covered by
    // test_post_cbor_configmap_decoded_and_stored).
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cbor-get-cm", "namespace": TEST_NS},
        "data": {"a": "1", "b": "2"}
    });
    mem.create(&build_key("configmaps", Some(TEST_NS), "cbor-get-cm"), &cm)
        .await
        .expect("seed configmap");

    let (status, content_type, body) = send(
        router,
        Method::GET,
        "/api/v1/namespaces/default/configmaps/cbor-get-cm",
        &[("accept", "application/cbor")],
        Vec::new(),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "GET with Accept: application/cbor must return 200; got {} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );
    assert!(
        content_type.starts_with(cbor::CBOR_CONTENT_TYPE),
        "response Content-Type must be application/cbor; got {:?}",
        content_type
    );

    // Decode the CBOR body and confirm the round-trip preserves the shape
    // we care about: apiVersion, kind, metadata.name, and the full data map.
    let decoded = cbor::decode_cbor_to_json(&body).expect("decode CBOR response");
    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "ConfigMap");
    assert_eq!(decoded["metadata"]["name"], "cbor-get-cm");
    assert_eq!(decoded["data"]["a"], "1");
    assert_eq!(decoded["data"]["b"], "2");
}

/// CRD POST with `application/cbor` previously failed with HTTP 415 because
/// `crates/api-server/src/handlers/crd.rs` rejected any non-JSON first byte
/// before middleware could decode CBOR. The fix runs the CBOR decoder in
/// the middleware, so the body reaches the CRD handler as JSON.
#[tokio::test]
async fn test_crd_handler_accepts_cbor_body() {
    let (_mem, router) = spawn_router();

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {
                "plural": "widgets",
                "singular": "widget",
                "kind": "Widget",
                "listKind": "WidgetList"
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}
                }
            }]
        }
    });
    let cbor_body = cbor::encode_json_to_cbor(&crd).expect("encode CRD CBOR");

    let (status, _ct, body) = send(
        router,
        Method::POST,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &[("content-type", "application/cbor")],
        cbor_body,
    )
    .await;

    assert!(
        status.is_success(),
        "CRD POST with application/cbor must succeed (previously 415); got {} body={}",
        status,
        String::from_utf8_lossy(&body)
    );
}

/// Bytes that do not form a valid CBOR item must be rejected with a 4xx.
/// Upstream `runtime/serializer/cbor.Decode` returns the same error class.
#[tokio::test]
async fn test_malformed_cbor_returns_400() {
    let (mem, router) = spawn_router();

    // 0xff alone is a "break" marker outside any indefinite-length container
    // — illegal as a top-level item. Other random-byte choices work too;
    // this one keeps the test deterministic.
    let bad_body = vec![0xff, 0xfe, 0xfd];

    let (status, _ct, _body) = send(
        router,
        Method::POST,
        "/api/v1/namespaces/default/configmaps",
        &[("content-type", "application/cbor")],
        bad_body,
    )
    .await;

    assert!(
        status.is_client_error(),
        "malformed CBOR body must produce a 4xx; got {}",
        status
    );

    // Storage must be untouched (no half-decoded resource sneaks in).
    assert!(
        mem.list::<Value>("/registry/configmaps/")
            .await
            .unwrap_or_default()
            .is_empty(),
        "malformed CBOR must not result in a stored object"
    );
}

/// Sending CBOR with `Accept: application/json` overrides the CBOR-in =>
/// CBOR-out convention: the response is plain JSON. This pins that the
/// `Accept` header always wins over the request body's content type for
/// response negotiation, matching upstream's negotiated-codec factory.
#[tokio::test]
async fn test_cbor_request_with_json_accept_returns_json() {
    let (_mem, router) = spawn_router();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cbor-json-resp", "namespace": TEST_NS},
        "data": {"k": "v"}
    });
    let cbor_body = cbor::encode_json_to_cbor(&cm).expect("encode");

    let (status, content_type, body) = send(
        router,
        Method::POST,
        "/api/v1/namespaces/default/configmaps",
        &[
            ("content-type", "application/cbor"),
            ("accept", "application/json"),
        ],
        cbor_body,
    )
    .await;

    assert!(
        status.is_success(),
        "POST cbor + accept json should succeed; got {} body={}",
        status,
        String::from_utf8_lossy(&body)
    );
    assert!(
        content_type.starts_with("application/json"),
        "Accept: application/json must win over the request CBOR Content-Type; got {}",
        content_type
    );

    // The body itself must parse as JSON.
    serde_json::from_slice::<Value>(&body).expect("response body must be JSON");
}
