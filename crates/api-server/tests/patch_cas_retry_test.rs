//! Regression tests for PATCH retry on resourceVersion conflict.
//!
//! Covers:
//!   8f271ca  fix: PATCH retry on rv conflict + inline ObjectMeta
//!             → generic_patch.rs: retry once on Error::Conflict from storage.update()
//!   4f9111b  fix: scale PATCH retry on resourceVersion conflict
//!             → scale.rs: retry up to 5 times on Error::Conflict from storage.update()
//!
//! Each test spins up a full `ApiServerState` backed by `StorageBackend::Memory`
//! (pointing to an `Arc<MemoryStorage>` with conflict injection), builds the
//! Axum router, and sends a real HTTP PATCH request through tower's `oneshot`.
//! The `MemoryStorage::inject_conflicts(1)` call primes the storage to return
//! `Error::Conflict` on the *first* `update()` call, simulating the etcd CAS
//! mismatch that triggered these fixes.
//!
//! Revert scenario: reverting the retry loop causes the Conflict to be surfaced
//! to the client as HTTP 409 instead of the expected 200.

use axum::{body::Body, http::Request};
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_storage::{build_key, memory::MemoryStorage, StorageBackend, Storage};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Construct a minimal `ApiServerState` backed by the given `MemoryStorage`.
/// `skip_auth = true` so the router uses `skip_auth_middleware` and no token
/// is needed.
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

/// Send a PATCH request to `uri` with the given JSON body and
/// `Content-Type: application/merge-patch+json`.
/// Returns the HTTP status code and the response body as a `serde_json::Value`.
async fn patch_json(
    state: Arc<ApiServerState>,
    uri: &str,
    body: &Value,
) -> (u16, Value) {
    let router = build_router(state, None);
    let body_bytes = serde_json::to_vec(body).unwrap();
    let req = Request::builder()
        .method("PATCH")
        .uri(uri)
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(body_bytes))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status().as_u16();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap_or(json!(null));
    (status, body_json)
}

// ---------------------------------------------------------------------------
// 8f271ca  — generic PATCH retry
// ---------------------------------------------------------------------------

/// Regression test for commit 8f271ca.
///
/// The fix adds a retry branch in `patch_namespaced_resource`: when
/// `storage.update()` returns `Error::Conflict`, re-read the resource, re-apply
/// the patch, and try again.
///
/// Without the fix, the first `Conflict` would be propagated to the client as
/// HTTP 409. With the fix, the handler retries and returns HTTP 200 with the
/// patched resource.
#[tokio::test]
async fn test_patch_generic_retries_on_conflict() {
    let mem = Arc::new(MemoryStorage::new());

    // Pre-create a ConfigMap so the handler finds it.
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-retry-test",
            "namespace": "default"
        },
        "data": {
            "key": "original"
        }
    });
    let key = build_key("configmaps", Some("default"), "cm-retry-test");
    mem.create(&key, &cm).await.unwrap();

    // Arm the conflict injector: the NEXT update() call returns Error::Conflict.
    mem.inject_conflicts(1);

    let state = make_state(mem.clone());
    let patch = json!({"data": {"key": "patched"}});
    let (status, body) =
        patch_json(state, "/api/v1/namespaces/default/configmaps/cm-retry-test", &patch).await;

    println!("status={} body={}", status, body);

    assert_eq!(
        status, 200,
        "PATCH must succeed (200) even when the first update() returns Conflict; \
         got {} — did the retry loop get reverted? body={}",
        status, body
    );
    assert_eq!(
        body["data"]["key"], "patched",
        "Patch must be applied in the retried write"
    );
}

// ---------------------------------------------------------------------------
// 4f9111b  — scale PATCH retry
// ---------------------------------------------------------------------------

/// Regression test for the scale-PATCH sub-fix in commit 4f9111b.
///
/// The fix wraps the `get + update` loop in `patch_scale` with a retry: on
/// `Error::Conflict`, re-read the resource and retry the write.
///
/// Without the fix, the first `Conflict` is returned to the client as HTTP 409.
/// With the fix, the handler retries and returns HTTP 200 with the patched scale.
#[tokio::test]
async fn test_patch_scale_retries_on_conflict() {
    let mem = Arc::new(MemoryStorage::new());

    // Pre-create a Deployment so patch_scale can find it.
    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "scale-retry-deploy",
            "namespace": "default"
        },
        "spec": {
            "replicas": 2,
            "selector": {
                "matchLabels": {"app": "scale-retry"}
            },
            "template": {
                "metadata": {"labels": {"app": "scale-retry"}},
                "spec": {
                    "containers": [{"name": "app", "image": "nginx:latest"}]
                }
            }
        },
        "status": {
            "replicas": 2
        }
    });
    let key = build_key("deployments", Some("default"), "scale-retry-deploy");
    mem.create(&key, &deployment).await.unwrap();

    // Arm the conflict injector.
    mem.inject_conflicts(1);

    let state = make_state(mem.clone());
    // K8s scale PATCH body: {"spec":{"replicas": N}}
    let patch = json!({"spec": {"replicas": 5}});
    let (status, body) = patch_json(
        state,
        "/apis/apps/v1/namespaces/default/deployments/scale-retry-deploy/scale",
        &patch,
    )
    .await;

    println!("status={} body={}", status, body);

    assert_eq!(
        status, 200,
        "Scale PATCH must succeed (200) even when the first update() returns Conflict; \
         got {} — did the retry loop in patch_scale get reverted? body={}",
        status, body
    );
    assert_eq!(
        body["spec"]["replicas"], 5,
        "Replicas must reflect the patched value"
    );
}
