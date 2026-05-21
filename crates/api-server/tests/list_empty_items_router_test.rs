//! End-to-end pin of the upstream-Kubernetes contract: an empty `*List` API
//! response MUST serialize `items: []` on the wire — never `items: null`,
//! never with `items` absent.
//!
//! The serde-level invariant is exercised in
//! `crates/common/tests/list_empty_items_invariant_test.rs`. This file adds
//! the router-driven side: a real HTTP `GET` against the live api-server
//! routes for several resource kinds with no objects in storage, and asserts
//! the parsed JSON body has `items` set to an empty array.
//!
//! Why both layers: the serde unit test guarantees the *type* serialises
//! correctly; this test guarantees the *handler* assembles its response with
//! the same wrapper (and not, e.g. `{ "items": value_or_null }` from a manual
//! `serde_json::json!` macro).
//!
//! Harness mirrors `integration_configmap_lifecycle.rs`:
//!   * `Arc<MemoryStorage>` backend.
//!   * `AlwaysAllowAuthorizer` + `skip_auth=true` so no bearer token is needed.
//!   * `tower::ServiceExt::oneshot` per request.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// HTTP harness (inline, matches the in-process pattern used elsewhere).
// ---------------------------------------------------------------------------

fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true, // skip_auth
    ))
}

fn router_for(state: &Arc<ApiServerState>) -> axum::Router {
    build_router(state.clone(), None)
}

async fn read_body(response: axum::response::Response) -> (StatusCode, Vec<u8>, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_value: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, bytes.to_vec(), body_value)
}

async fn post_json(state: &Arc<ApiServerState>, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router_for(state).oneshot(req).await.unwrap();
    let (status, _raw, value) = read_body(response).await;
    (status, value)
}

async fn get_list(state: &Arc<ApiServerState>, uri: &str) -> (StatusCode, Vec<u8>, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = router_for(state).oneshot(req).await.unwrap();
    read_body(response).await
}

/// Assert the parsed list body has `items` present, as an array, and empty.
/// Also re-parses the raw bytes and looks at the literal substring `"items":`
/// to defend against `null` slipping past a permissive deserializer.
fn assert_empty_items_payload(label: &str, raw: &[u8], body: &Value) {
    let raw_str = std::str::from_utf8(raw).expect("response body is not UTF-8");
    assert!(
        body.is_object(),
        "{label} response body must be a JSON object, got {body:?}",
    );
    let items = body.get("items").unwrap_or_else(|| {
        panic!("{label} response missing `items` key (must be `[]`): {raw_str}")
    });
    assert!(
        !items.is_null(),
        "{label} response has `items: null` (must be `[]`): {raw_str}",
    );
    assert!(
        items.is_array(),
        "{label} response `items` must be a JSON array, got {items:?} (raw: {raw_str})",
    );
    assert!(
        items.as_array().unwrap().is_empty(),
        "{label} response `items` expected []; got {items:?}",
    );
    // Wire-level double-check: the textual response must contain `"items":[`
    // (with optional whitespace). `null` would render as `"items":null`.
    assert!(
        raw_str.contains("\"items\":[") || raw_str.contains("\"items\": ["),
        "{label} response must literally contain \"items\":[ on the wire, got: {raw_str}",
    );
    assert!(
        !raw_str.contains("\"items\":null") && !raw_str.contains("\"items\": null"),
        "{label} response must NOT contain \"items\":null: {raw_str}",
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// GET /api/v1/namespaces/<empty-ns>/pods — the canonical case called out in
/// the spec for this unit. An empty namespace must return `items: []`.
#[tokio::test]
async fn list_pods_in_empty_namespace_returns_items_empty_array() {
    let state = make_state(Arc::new(MemoryStorage::new()));

    // Create the namespace so the handler returns 200, not a missing-ns error.
    let ns_name = "empty-pod-ns";
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": ns_name },
    });
    let (status, _body) = post_json(&state, "/api/v1/namespaces", &ns_body).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create returned {status}",
    );

    let uri = format!("/api/v1/namespaces/{ns_name}/pods");
    let (status, raw, body) = get_list(&state, &uri).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} expected 200; got {status}",
    );
    assert_eq!(body["kind"], "PodList");
    assert_eq!(body["apiVersion"], "v1");
    assert_empty_items_payload("PodList", &raw, &body);
}

/// GET /api/v1/namespaces/<empty-ns>/configmaps — another core/v1 list path.
#[tokio::test]
async fn list_configmaps_in_empty_namespace_returns_items_empty_array() {
    let state = make_state(Arc::new(MemoryStorage::new()));

    let ns_name = "empty-cm-ns";
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": ns_name },
    });
    let (status, _body) = post_json(&state, "/api/v1/namespaces", &ns_body).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create returned {status}",
    );

    let uri = format!("/api/v1/namespaces/{ns_name}/configmaps");
    let (status, raw, body) = get_list(&state, &uri).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} expected 200; got {status}",
    );
    assert_eq!(body["kind"], "ConfigMapList");
    assert_eq!(body["apiVersion"], "v1");
    assert_empty_items_payload("ConfigMapList", &raw, &body);
}
