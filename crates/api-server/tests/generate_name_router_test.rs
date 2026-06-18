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

use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

// Harness: `TestApiServer` (rusternetes-test-support) — `build_router` on
// `MemoryStorage` with `--skip-auth`, driven via `tower::oneshot`.

#[tokio::test]
async fn configmap_create_honors_generate_name() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"generateName": "my-cm-"},
        "data": {"key": "value"},
    });
    let (status, created) = state
        .post("/api/v1/namespaces/default/configmaps", &body)
        .await;

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
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"generateName": "my-pod-"},
        "spec": {"containers": [{"name": "c", "image": "nginx:latest"}]},
    });
    let (status, created) = state.post("/api/v1/namespaces/default/pods", &body).await;

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
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"generateName": "my-secret-"},
        "type": "Opaque",
    });
    let (status, created) = state
        .post("/api/v1/namespaces/default/secrets", &body)
        .await;

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
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "explicit", "generateName": "my-cm-"},
    });
    let (status, created) = state
        .post("/api/v1/namespaces/default/configmaps", &body)
        .await;

    assert!(status.is_success(), "got {status}: {created}");
    assert_eq!(
        created["metadata"]["name"], "explicit",
        "an explicit name must take precedence over generateName"
    );
}
