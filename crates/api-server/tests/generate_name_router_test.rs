//! Router-level pin for GitHub #1052: server-side `metadata.generateName` must
//! work for every built-in create handler, not just Secret/CR.
//!
//! A client POSTing an object with an empty `metadata.name` and a non-empty
//! `metadata.generateName` prefix must get a created object whose name was
//! synthesised as `<prefix><suffix>`. This is handled centrally by
//! `generate_name_middleware` (after content-type normalisation), so the same
//! pass covers namespaced and cluster-scoped kinds regardless of how each
//! handler parses its body.
//!
//! Harness mirrors `list_resource_version_router_test.rs`.

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

fn make_state() -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
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

async fn post_json(state: &Arc<ApiServerState>, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = build_router(state.clone(), None)
        .oneshot(req)
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, value)
}

#[tokio::test]
async fn configmap_create_honors_generate_name() {
    let state = make_state();
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"generateName": "my-cm-"},
        "data": {"key": "value"},
    });
    let (status, created) = post_json(&state, "/api/v1/namespaces/default/configmaps", &body).await;

    assert!(
        status.is_success(),
        "create with generateName must succeed, got {status}: {created}"
    );
    let name = created["metadata"]["name"].as_str().unwrap_or_default();
    assert!(
        name.starts_with("my-cm-") && name.len() > "my-cm-".len(),
        "expected a synthesised name with prefix 'my-cm-', got {name:?}"
    );
}

#[tokio::test]
async fn pod_create_honors_generate_name() {
    let state = make_state();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"generateName": "my-pod-"},
        "spec": {"containers": [{"name": "c", "image": "nginx:latest"}]},
    });
    let (status, created) = post_json(&state, "/api/v1/namespaces/default/pods", &body).await;

    assert!(
        status.is_success(),
        "create with generateName must succeed, got {status}: {created}"
    );
    let name = created["metadata"]["name"].as_str().unwrap_or_default();
    assert!(
        name.starts_with("my-pod-") && name.len() > "my-pod-".len(),
        "expected a synthesised name with prefix 'my-pod-', got {name:?}"
    );
}

#[tokio::test]
async fn secret_create_honors_generate_name() {
    // Secret used to synthesise via a per-handler call; #1063 removed it in
    // favour of the central middleware. Pin that Secret still works.
    let state = make_state();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"generateName": "my-secret-"},
        "type": "Opaque",
    });
    let (status, created) = post_json(&state, "/api/v1/namespaces/default/secrets", &body).await;

    assert!(
        status.is_success(),
        "create with generateName must succeed, got {status}: {created}"
    );
    let name = created["metadata"]["name"].as_str().unwrap_or_default();
    assert!(
        name.starts_with("my-secret-") && name.len() > "my-secret-".len(),
        "expected a synthesised name with prefix 'my-secret-', got {name:?}"
    );
}

#[tokio::test]
async fn explicit_name_still_wins_over_generate_name() {
    let state = make_state();
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "explicit", "generateName": "my-cm-"},
    });
    let (status, created) = post_json(&state, "/api/v1/namespaces/default/configmaps", &body).await;

    assert!(status.is_success(), "got {status}: {created}");
    assert_eq!(
        created["metadata"]["name"], "explicit",
        "an explicit name must take precedence over generateName"
    );
}
