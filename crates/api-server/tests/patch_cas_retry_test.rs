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

use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Thin shim over the shared harness, preserving this file's `(u16, Value)`
/// return. `TestApiServer::new()` boots `build_router` on `MemoryStorage` with
/// `--skip-auth`; `api.storage` is the backing store whose `inject_conflicts`
/// primes the CAS mismatch these tests exercise. `patch` uses
/// `application/merge-patch+json`.
async fn patch_json(api: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = api.patch(uri, body).await;
    (status.as_u16(), value)
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
    let api = TestApiServer::new();
    let mem = api.storage.clone();

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
    let patch = json!({"data": {"key": "patched"}});
    let (status, body) = patch_json(
        &api,
        "/api/v1/namespaces/default/configmaps/cm-retry-test",
        &patch,
    )
    .await;

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
    let api = TestApiServer::new();
    let mem = api.storage.clone();

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
    // K8s scale PATCH body: {"spec":{"replicas": N}}
    let patch = json!({"spec": {"replicas": 5}});
    let (status, body) = patch_json(
        &api,
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
