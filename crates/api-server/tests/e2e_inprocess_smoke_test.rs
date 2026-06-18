//! Layer 7: cargo-driven, in-process full-stack e2e smoke test.
//!
//! Mirrors the spirit of upstream Kubernetes e2e/conformance — a single flow
//! that creates a Deployment via the REST API and watches Pods materialise
//! through the controller chain (Deployment -> ReplicaSet -> Pods), then a
//! foreground cascade delete via the same REST API.
//!
//! Wiring:
//!   * api-server router via `build_router()` over `tower::ServiceExt::oneshot`
//!   * `DeploymentController::reconcile_all()` (in-process tick)
//!   * `ReplicaSetController::reconcile_all()` (in-process tick)
//!   * scheduler step simulated by direct `MemoryStorage` write — wiring
//!     `rusternetes-scheduler` in-process would require an extra dev-dep and
//!     all we need to assert is "spec.nodeName is set after one scheduling
//!     pass", which a single storage update reproduces exactly. The
//!     production scheduler's `try_schedule_pod` does the same write.
//!   * `GarbageCollector::scan_and_collect()` (in-process tick) for the
//!     foreground cascade.
//!
//! The router and the controllers share one `MemoryStorage` instance via
//! `Arc<MemoryStorage>` for direct controller use and
//! `Arc<StorageBackend::Memory(mem.clone()))` for the router state — both
//! wrap the same underlying map, just at different type layers.
//!
//! Test function name: `test_e2e_deployment_to_pods_and_foreground_cascade`.

use rusternetes_common::resources::{Pod, ReplicaSet};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_controller_manager::controllers::garbage_collector::GarbageCollector;
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_storage::{build_prefix, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Harness — thin `(u16, Value)` shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

/// Build a router + the underlying MemoryStorage. The two halves share the
/// same memory map: the router writes through `StorageBackend::Memory(mem)`
/// while the controllers (DeploymentController, ReplicaSetController, GC)
/// drive `mem` directly via the `Storage` trait.
fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn post_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router.post(uri, body).await;
    (status.as_u16(), value)
}

async fn get(router: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.get(uri).await;
    (status.as_u16(), value)
}

async fn delete_with_query(router: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.delete(uri).await;
    (status.as_u16(), value)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn ns_body(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    })
}

/// Deployment with `replicas: 3`, single nginx container, label-selector
/// match `app=e2e`. Mirrors `apps/v1` shape exactly.
fn deployment_body(name: &str, replicas: i32) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": name },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": { "app": "e2e" } },
            "template": {
                "metadata": { "labels": { "app": "e2e" } },
                "spec": {
                    "containers": [{
                        "name": "nginx",
                        "image": "nginx:1.25-alpine",
                    }],
                },
            },
        },
    })
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Full-stack flow:
///   1. POST Namespace `e2e-test`.
///   2. POST Deployment (replicas=3) into `e2e-test`.
///   3. Tick DeploymentController -> a ReplicaSet appears with
///      `metadata.ownerReferences` pointing at the Deployment.
///   4. Tick ReplicaSetController -> 3 Pods appear with ownerReferences
///      pointing at the ReplicaSet.
///   5. Simulate scheduler -> each pod gets `spec.nodeName` set.
///      (Wiring the real scheduler in-process would require another dev-dep;
///      the production scheduler's `try_schedule_pod` ultimately writes the
///      same `spec.nodeName` field, so a direct storage update reproduces
///      the same observable post-state.)
///   6. DELETE the Deployment with `?propagationPolicy=Foreground`. The
///      api-server attaches the `foregroundDeletion` finalizer and stamps a
///      deletionTimestamp.
///   7. Tick GarbageCollector -> the ReplicaSet (and through it, its Pods)
///      are deleted. The foregroundDeletion finalizer is then removed from
///      the Deployment so it can disappear.
///   8. Assert everything under the deployment selector is gone.
#[tokio::test]
async fn test_e2e_deployment_to_pods_and_foreground_cascade() {
    let (mem, router) = spawn_router();
    let ns = "e2e-test";

    // ---- 1. Namespace -----------------------------------------------------
    let (st, body) = post_json(&router, "/api/v1/namespaces", &ns_body(ns)).await;
    assert!(
        st == 201 || st == 200,
        "namespace create: status={} body={}",
        st,
        body
    );

    // ---- 2. Deployment ----------------------------------------------------
    let dep = deployment_body("web", 3);
    let (st, dep_body) = post_json(
        &router,
        &format!("/apis/apps/v1/namespaces/{}/deployments", ns),
        &dep,
    )
    .await;
    assert!(
        st == 201 || st == 200,
        "deployment create: status={} body={}",
        st,
        dep_body
    );
    let deployment_uid = dep_body["metadata"]["uid"]
        .as_str()
        .expect("deployment uid populated by api-server")
        .to_string();
    assert!(
        !deployment_uid.is_empty(),
        "uid must be non-empty: {}",
        dep_body
    );

    // ---- 3. Tick Deployment controller -> ReplicaSet created --------------
    let dep_ctrl = DeploymentController::new(mem.clone(), 10);
    dep_ctrl
        .reconcile_all()
        .await
        .expect("deployment reconcile");

    let rs_prefix = build_prefix("replicasets", Some(ns));
    let replicasets: Vec<ReplicaSet> = mem.list(&rs_prefix).await.expect("list rs");
    assert_eq!(
        replicasets.len(),
        1,
        "exactly one ReplicaSet should be created by the deployment controller, got {}",
        replicasets.len()
    );
    let rs = &replicasets[0];
    let rs_uid = rs.metadata.uid.clone();
    let owners = rs
        .metadata
        .owner_references
        .as_ref()
        .expect("RS must carry ownerReferences pointing at the Deployment");
    assert!(
        owners
            .iter()
            .any(|o| o.kind == "Deployment" && o.uid == deployment_uid),
        "RS ownerReferences must include the Deployment uid {}, got {:?}",
        deployment_uid,
        owners,
    );

    // ---- 4. Tick ReplicaSet controller -> Pods created --------------------
    let rs_ctrl = ReplicaSetController::new(mem.clone(), 10);
    rs_ctrl.reconcile_all().await.expect("rs reconcile");

    let pods: Vec<Pod> = mem
        .list(&build_prefix("pods", Some(ns)))
        .await
        .expect("list pods");
    assert_eq!(
        pods.len(),
        3,
        "ReplicaSet controller must materialise exactly 3 pods, got {}",
        pods.len()
    );
    for pod in &pods {
        let owners = pod
            .metadata
            .owner_references
            .as_ref()
            .unwrap_or_else(|| panic!("pod {} missing ownerReferences", pod.metadata.name));
        assert!(
            owners
                .iter()
                .any(|o| o.kind == "ReplicaSet" && o.uid == rs_uid),
            "pod {} ownerReferences must include the ReplicaSet uid {}, got {:?}",
            pod.metadata.name,
            rs_uid,
            owners,
        );
    }

    // ---- 5. Simulate scheduler -------------------------------------------
    // Production `Scheduler::try_schedule_pod` reads the pod, picks a Node,
    // and writes `spec.nodeName`. We reproduce the same observable end-state
    // (`spec.nodeName` set on every pod) with one storage update per pod;
    // wiring the real scheduler in-process would require pulling
    // `rusternetes-scheduler` in as a dev-dep, plus seeding Nodes, and adds
    // no signal to the cascade-delete assertions that follow.
    for pod in &pods {
        let pod_key = rusternetes_storage::build_key("pods", Some(ns), &pod.metadata.name);
        let mut updated: Pod = mem.get(&pod_key).await.expect("re-read pod");
        if let Some(spec) = updated.spec.as_mut() {
            spec.node_name = Some("e2e-node-1".to_string());
        }
        mem.update(&pod_key, &updated).await.expect("schedule pod");
    }
    // Confirm all pods have nodeName via a fresh list (defends against any
    // controller that resets it on a subsequent tick).
    let scheduled: Vec<Pod> = mem
        .list(&build_prefix("pods", Some(ns)))
        .await
        .expect("list scheduled pods");
    for pod in &scheduled {
        assert_eq!(
            pod.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("e2e-node-1"),
            "pod {} must have spec.nodeName after the scheduler pass",
            pod.metadata.name,
        );
    }

    // ---- 6. DELETE deployment with Foreground propagation -----------------
    let (st, _) = delete_with_query(
        &router,
        &format!(
            "/apis/apps/v1/namespaces/{}/deployments/web?propagationPolicy=Foreground",
            ns
        ),
    )
    .await;
    assert!(
        st == 200 || st == 202,
        "delete deployment (Foreground): status={}",
        st
    );

    // The api-server must NOT have hard-deleted the deployment yet — Foreground
    // adds the `foregroundDeletion` finalizer and a deletionTimestamp so the
    // GC can cascade before the owner disappears.
    let (st, dep_after) = get(
        &router,
        &format!("/apis/apps/v1/namespaces/{}/deployments/web", ns),
    )
    .await;
    assert_eq!(
        st, 200,
        "deployment must still be retrievable while finalizers run: body={}",
        dep_after
    );
    let finalizers = dep_after["metadata"]["finalizers"]
        .as_array()
        .expect("deployment must carry finalizers after Foreground delete");
    assert!(
        finalizers
            .iter()
            .any(|f| f.as_str() == Some("foregroundDeletion")),
        "deployment must carry foregroundDeletion finalizer, got {:?}",
        finalizers,
    );
    assert!(
        dep_after["metadata"]["deletionTimestamp"].is_string(),
        "deployment must carry a deletionTimestamp, body={}",
        dep_after,
    );

    // ---- 7. Tick GC -> cascade ------------------------------------------
    // The GC scan deletes RSes (and through them, Pods) owned by the dying
    // Deployment, then removes the foregroundDeletion finalizer so the
    // Deployment itself can be reaped on the next pass.
    let gc = GarbageCollector::new(mem.clone());
    // One scan is enough: `process_deletion` deletes dependents, strips the
    // foregroundDeletion finalizer, then re-reads and deletes the now
    // finalizer-free Deployment all in the same pass.
    gc.scan_and_collect().await.expect("gc scan");

    // ---- 8. Assert everything is gone ------------------------------------
    let remaining_pods: Vec<Pod> = mem
        .list(&build_prefix("pods", Some(ns)))
        .await
        .expect("list pods after gc");
    assert!(
        remaining_pods.is_empty(),
        "all pods must be GC'd after Foreground cascade, got {} remaining: {:?}",
        remaining_pods.len(),
        remaining_pods
            .iter()
            .map(|p| p.metadata.name.clone())
            .collect::<Vec<_>>(),
    );

    let remaining_rs: Vec<ReplicaSet> = mem
        .list(&rs_prefix)
        .await
        .expect("list replicasets after gc");
    assert!(
        remaining_rs.is_empty(),
        "all replicasets must be GC'd after Foreground cascade, got {} remaining",
        remaining_rs.len(),
    );

    let (st, _) = get(
        &router,
        &format!("/apis/apps/v1/namespaces/{}/deployments/web", ns),
    )
    .await;
    assert_eq!(
        st, 404,
        "deployment must be gone after foregroundDeletion finalizer is removed by GC"
    );
}
