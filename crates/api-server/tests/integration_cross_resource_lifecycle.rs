//! Integration tests that exercise multi-resource lifecycle flows through the
//! in-process Axum router — the rusternetes mirror of upstream Kubernetes
//! `test/integration/` tests that drive the API server end-to-end with multiple
//! resources, asserting cross-resource semantics (namespace isolation,
//! ownerReference cascade, label-selector filtering, deletion finalizer
//! protocol).
//!
//! Source inspirations (Kubernetes v1.35):
//! - `test/integration/garbagecollector/garbage_collector_test.go`
//! - `test/integration/namespace/ns_conditions_test.go`
//! - `test/integration/objectmeta/owner_test.go`
//!
//! ## Scope
//!
//! Each `#[tokio::test]` spins up a fresh `MemoryStorage` + router and
//! sequences a handful of requests via `tower::ServiceExt::oneshot`, asserting
//! both the HTTP responses AND the stored objects in `MemoryStorage`.
//!
//! The api-server is responsible for the **synchronous** half of these
//! flows (validation, finalizer fence, deletionTimestamp stamping,
//! propagation-policy finalizer addition, namespace isolation). The
//! **eventual** cleanup half (cascade deletion, garbage-collected dependents)
//! is owned by the namespace and garbage-collector controllers. Scenarios
//! that depend on those controllers wire them in-process by sharing the
//! underlying `Arc<MemoryStorage>` with the router (the router talks to it
//! through `StorageBackend::Memory(mem)`, the controllers talk to it
//! directly via the `Storage` trait), then tick `reconcile_all()` /
//! `scan_and_collect()` between request phases. This mirrors the pattern
//! in `crates/api-server/tests/e2e_inprocess_smoke_test.rs`.

use axum::http::{Method, StatusCode};
use rusternetes_controller_manager::controllers::{
    garbage_collector::GarbageCollector, namespace::NamespaceController,
};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `mem` is the
// backing store so the GC/namespace controllers drive it directly.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send_json(
    router: &TestApiServer,
    method: Method,
    uri: &str,
    body: &Value,
) -> (StatusCode, Value) {
    router
        .send(method.as_str(), uri, Some("application/json"), Some(body))
        .await
}

async fn send_get(router: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    router.get(uri).await
}

async fn send_delete(router: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    router.delete(uri).await
}

async fn snapshot(mem: &Arc<MemoryStorage>, key: &str) -> Option<Value> {
    mem.get::<Value>(key).await.ok()
}

fn assert_success(label: &str, status: StatusCode, body: &Value) {
    assert!(
        status.is_success(),
        "[{label}] expected 2xx, got {status} body={body}",
    );
}

fn names(list_body: &Value) -> Vec<&str> {
    list_body["items"]
        .as_array()
        .expect("list body must contain items array")
        .iter()
        .filter_map(|p| p["metadata"]["name"].as_str())
        .collect()
}

// ---------------------------------------------------------------------------
// Fixture builders. JSON wire format uses camelCase — these are sent as-is to
// the handlers.
// ---------------------------------------------------------------------------

fn namespace_stub(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": name}
    })
}

fn pod_stub(ns: &str, name: &str, labels: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": ns,
            "labels": labels,
        },
        "spec": {"containers": [{"name": "c1", "image": "busybox"}]}
    })
}

fn configmap_stub(ns: &str, name: &str, labels: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": ns,
            "labels": labels,
        },
        "data": {"foo": "bar"}
    })
}

fn secret_stub(ns: &str, name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": ns},
        "data": {"key": "ZGF0YS1maWxl"}
    })
}

fn service_stub(ns: &str, name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": ns},
        "spec": {
            "type": "ClusterIP",
            "ports": [{"port": 80, "targetPort": 8080}],
            "selector": {"app": "demo"}
        }
    })
}

fn replicaset_stub(ns: &str, name: &str) -> Value {
    // ReplicaSet ships with no finalizers; the api-server adds
    // `foregroundDeletion` on DELETE when Foreground propagation is requested
    // (see `handle_delete_with_finalizers_and_propagation`).
    json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": name, "namespace": ns},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "demo"}},
            "template": {
                "metadata": {"labels": {"app": "demo"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        }
    })
}

/// Build a Pod owned by `owner_kind`/`owner_name` (with `owner_uid`).
/// `controller=true` mirrors what a ReplicaSet controller stamps on its
/// dependents — the garbage collector keys off this when foreground propagation
/// is requested on the ReplicaSet.
fn owned_pod_stub(
    ns: &str,
    name: &str,
    owner_kind: &str,
    owner_name: &str,
    owner_uid: &str,
) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": ns,
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": owner_kind,
                "name": owner_name,
                "uid": owner_uid,
                "controller": true,
                "blockOwnerDeletion": true,
            }],
        },
        "spec": {"containers": [{"name": "c1", "image": "busybox"}]}
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Create a Namespace and POST four child resources (Pod, ConfigMap, Secret,
/// Service) into it. Verify each is retrievable via the namespace-scoped
/// item URI, that list returns all of them, and that a label selector
/// narrows the list as expected.
#[tokio::test]
async fn test_lifecycle_namespace_child_resources_visible() {
    let (mem, router) = spawn_router();

    let (status, body) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("test-ns"),
    )
    .await;
    assert_success("namespace POST", status, &body);

    let labels = json!({"app": "demo", "tier": "backend"});
    let posts: [(&str, &str, Value); 4] = [
        (
            "pod",
            "/api/v1/namespaces/test-ns/pods",
            pod_stub("test-ns", "pod-a", labels.clone()),
        ),
        (
            "configmap",
            "/api/v1/namespaces/test-ns/configmaps",
            configmap_stub("test-ns", "cm-a", labels.clone()),
        ),
        (
            "secret",
            "/api/v1/namespaces/test-ns/secrets",
            secret_stub("test-ns", "secret-a"),
        ),
        (
            "service",
            "/api/v1/namespaces/test-ns/services",
            service_stub("test-ns", "svc-a"),
        ),
    ];
    for (label, uri, body) in posts {
        let (status, resp) = send_json(&router, Method::POST, uri, &body).await;
        assert_success(label, status, &resp);
    }

    for (kind, uri) in [
        ("pod", "/api/v1/namespaces/test-ns/pods/pod-a"),
        ("configmap", "/api/v1/namespaces/test-ns/configmaps/cm-a"),
        ("secret", "/api/v1/namespaces/test-ns/secrets/secret-a"),
        ("service", "/api/v1/namespaces/test-ns/services/svc-a"),
    ] {
        let (status, body) = send_get(&router, uri).await;
        assert_success(&format!("GET {kind} at {uri}"), status, &body);
        assert_eq!(
            body["metadata"]["namespace"].as_str(),
            Some("test-ns"),
            "[{kind}] expected namespace=test-ns in response body",
        );
    }

    let (status, body) = send_get(
        &router,
        "/api/v1/namespaces/test-ns/pods?labelSelector=app%3Ddemo",
    )
    .await;
    assert_success("list with selector", status, &body);
    let matched = names(&body);
    assert_eq!(
        matched,
        vec!["pod-a"],
        "labelSelector=app=demo should yield exactly [pod-a], got {matched:?}",
    );

    for (resource, name) in [
        ("pods", "pod-a"),
        ("configmaps", "cm-a"),
        ("secrets", "secret-a"),
        ("services", "svc-a"),
    ] {
        let key = build_key(resource, Some("test-ns"), name);
        assert!(
            snapshot(&mem, &key).await.is_some(),
            "expected storage key {key} to exist after POST",
        );
    }
}

/// Pod `foo` in `ns-a` must NOT be visible at `/api/v1/namespaces/ns-b/pods/foo`,
/// and the list endpoint scoped to `ns-a` must return only `ns-a` pods.
#[tokio::test]
async fn test_lifecycle_cross_namespace_isolation() {
    let (_mem, router) = spawn_router();

    for ns in ["ns-a", "ns-b"] {
        let (status, body) = send_json(
            &router,
            Method::POST,
            "/api/v1/namespaces",
            &namespace_stub(ns),
        )
        .await;
        assert_success(&format!("namespace {ns} POST"), status, &body);
    }

    let posts: [(&str, &str, Value); 2] = [
        (
            "ns-a/pods/foo",
            "/api/v1/namespaces/ns-a/pods",
            pod_stub("ns-a", "foo", json!({"loc": "a"})),
        ),
        (
            "ns-b/pods/bar",
            "/api/v1/namespaces/ns-b/pods",
            pod_stub("ns-b", "bar", json!({"loc": "b"})),
        ),
    ];
    for (label, uri, body) in posts {
        let (status, resp) = send_json(&router, Method::POST, uri, &body).await;
        assert_success(label, status, &resp);
    }

    let (status, _) = send_get(&router, "/api/v1/namespaces/ns-b/pods/foo").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "pod foo must not be visible in ns-b, got {status}",
    );

    let (status, body) = send_get(&router, "/api/v1/namespaces/ns-a/pods/foo").await;
    assert_success("pod foo must be visible in ns-a", status, &body);
    assert_eq!(body["metadata"]["namespace"].as_str(), Some("ns-a"));

    for (ns, expected) in [("ns-a", vec!["foo"]), ("ns-b", vec!["bar"])] {
        let (status, body) = send_get(&router, &format!("/api/v1/namespaces/{ns}/pods")).await;
        assert_success(&format!("list {ns}/pods"), status, &body);
        let got = names(&body);
        assert_eq!(
            got, expected,
            "{ns} pod list should be {expected:?}, got {got:?}"
        );
    }
}

/// DELETE a Namespace that holds child resources. The api-server contract
/// (synchronous half) is: namespace transitions to `Terminating`, gains a
/// `deletionTimestamp`, and the `kubernetes` finalizer keeps it in storage so
/// the namespace controller can perform cascade cleanup later.
///
/// Child resources are NOT removed by the api-server handler — that's the
/// namespace controller's job. This assertion covers exactly what the api-server
/// does synchronously, matching `crates/api-server/src/handlers/namespace.rs:386-410`.
#[tokio::test]
async fn test_lifecycle_namespace_delete_marks_terminating_and_keeps_children() {
    let (mem, router) = spawn_router();

    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("doomed-ns"),
    )
    .await;
    assert_success("namespace POST", s, &b);

    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces/doomed-ns/pods",
        &pod_stub("doomed-ns", "p", json!({})),
    )
    .await;
    assert_success("pod POST", s, &b);

    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces/doomed-ns/configmaps",
        &configmap_stub("doomed-ns", "c", json!({})),
    )
    .await;
    assert_success("configmap POST", s, &b);

    let (status, body) = send_delete(&router, "/api/v1/namespaces/doomed-ns").await;
    assert_success("namespace DELETE", status, &body);

    let stored_ns = snapshot(&mem, &build_key("namespaces", None, "doomed-ns"))
        .await
        .expect("namespace must remain in storage during termination");
    assert_eq!(
        stored_ns["status"]["phase"].as_str(),
        Some("Terminating"),
        "phase should be Terminating; got {}",
        stored_ns["status"]
    );
    assert!(
        stored_ns["metadata"]["deletionTimestamp"].is_string(),
        "deletionTimestamp should be set; metadata={}",
        stored_ns["metadata"]
    );
    let finalizers: Vec<&str> = stored_ns["metadata"]["finalizers"]
        .as_array()
        .expect("finalizers array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        finalizers.contains(&"kubernetes"),
        "kubernetes finalizer must remain on terminating namespace, got {finalizers:?}",
    );

    // Cascade is the controller's job, not the handler's — covered by the
    // ignored test below.
    for (resource, name) in [("pods", "p"), ("configmaps", "c")] {
        assert!(
            snapshot(&mem, &build_key(resource, Some("doomed-ns"), name))
                .await
                .is_some(),
            "{resource}/{name} in terminating ns should still exist (controller cleans up async)",
        );
    }
}

/// Mirror of upstream
/// `test/integration/namespace/ns_conditions_test.go` cascade assertion:
/// after the namespace controller observes a terminating namespace, every
/// namespaced child resource is GC'd. The rusternetes namespace controller
/// does this in `crates/controller-manager/src/controllers/namespace.rs`.
///
/// In-process wiring (mirrors the pattern in `e2e_inprocess_smoke_test.rs`):
/// the router writes through `StorageBackend::Memory(mem)` while
/// `NamespaceController` drives the same `Arc<MemoryStorage>` directly via
/// the `Storage` trait. Calling `reconcile_all()` ticks the controller once
/// against the in-memory store. The controller defers finalization across
/// reconciles (sets conditions first, removes the `kubernetes` finalizer
/// the next cycle — see `finalize_namespace` in
/// `crates/controller-manager/src/controllers/namespace.rs`), so we tick
/// it multiple times until either the namespace is fully reaped or a small
/// upper bound is reached.
#[tokio::test]
async fn test_lifecycle_namespace_cascade_deletes_children() {
    let (mem, router) = spawn_router();

    // 1. POST namespace + 2. POST children (pod + configmap).
    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("ns-c"),
    )
    .await;
    assert_success("namespace POST", s, &b);

    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces/ns-c/pods",
        &pod_stub("ns-c", "p", json!({})),
    )
    .await;
    assert_success("pod POST", s, &b);

    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces/ns-c/configmaps",
        &configmap_stub("ns-c", "c", json!({})),
    )
    .await;
    assert_success("configmap POST", s, &b);

    // 3. DELETE namespace — api-server stamps deletionTimestamp +
    //    Terminating phase and keeps the `kubernetes` finalizer in place.
    let (status, body) = send_delete(&router, "/api/v1/namespaces/ns-c").await;
    assert_success("namespace DELETE", status, &body);

    // 4. Tick the namespace controller. The controller's `reconcile_namespace`
    //    spreads work across multiple cycles (sets conditions first, then
    //    finalizes), so we drive `reconcile_all` repeatedly until the
    //    namespace is gone or we hit a sane upper bound.
    let ns_ctrl = NamespaceController::new(mem.clone());
    let mut ticks = 0;
    loop {
        ns_ctrl
            .reconcile_all()
            .await
            .expect("namespace reconcile_all");
        ticks += 1;
        let ns_gone = snapshot(&mem, &build_key("namespaces", None, "ns-c"))
            .await
            .is_none();
        if ns_gone {
            break;
        }
        assert!(
            ticks < 10,
            "namespace controller failed to finalize ns-c within 10 ticks",
        );
    }

    // 5. Children GET → 404.
    for (kind, uri) in [
        ("pod", "/api/v1/namespaces/ns-c/pods/p"),
        ("configmap", "/api/v1/namespaces/ns-c/configmaps/c"),
    ] {
        let (status, _) = send_get(&router, uri).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{kind} at {uri} should be 404 after namespace cascade",
        );
    }
    // And in the storage layer too.
    for (resource, name) in [("pods", "p"), ("configmaps", "c")] {
        assert!(
            snapshot(&mem, &build_key(resource, Some("ns-c"), name))
                .await
                .is_none(),
            "{resource}/{name} should be gone from storage after cascade",
        );
    }

    // 6. Namespace itself is gone.
    let (status, _) = send_get(&router, "/api/v1/namespaces/ns-c").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "namespace ns-c should be 404 once finalizer is cleared",
    );
}

/// DELETE a ReplicaSet with `?propagationPolicy=Foreground`. The api-server
/// contract: the ReplicaSet gains the `foregroundDeletion` finalizer and a
/// `deletionTimestamp`, but stays in storage. The garbage collector then
/// removes dependents (Pods marked `blockOwnerDeletion=true`) before clearing
/// the finalizer.
///
/// This test exercises the **synchronous** half — the api-server's
/// finalizer-and-timestamp fence on the owner. Dependent reaping is the GC
/// controller's job and is pinned to the ignored test below.
#[tokio::test]
async fn test_lifecycle_owner_foreground_deletion_marks_owner() {
    let (mem, router) = spawn_router();

    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("gc-fg"),
    )
    .await;
    assert_success("namespace POST", s, &b);

    let (s, rs_body) = send_json(
        &router,
        Method::POST,
        "/apis/apps/v1/namespaces/gc-fg/replicasets",
        &replicaset_stub("gc-fg", "rs1"),
    )
    .await;
    assert_success("replicaset POST", s, &rs_body);
    let owner_uid = rs_body["metadata"]["uid"]
        .as_str()
        .expect("ReplicaSet must have a UID")
        .to_string();

    for pod_name in ["pod-x", "pod-y"] {
        let (s, b) = send_json(
            &router,
            Method::POST,
            "/api/v1/namespaces/gc-fg/pods",
            &owned_pod_stub("gc-fg", pod_name, "ReplicaSet", "rs1", &owner_uid),
        )
        .await;
        assert_success(&format!("owned pod {pod_name} POST"), s, &b);
    }

    let (status, body) = send_delete(
        &router,
        "/apis/apps/v1/namespaces/gc-fg/replicasets/rs1?propagationPolicy=Foreground",
    )
    .await;
    assert_success("rs1 DELETE Foreground", status, &body);

    let stored_rs = snapshot(&mem, &build_key("replicasets", Some("gc-fg"), "rs1"))
        .await
        .expect("ReplicaSet must remain in storage while finalizers are pending");
    let finalizers: Vec<&str> = stored_rs["metadata"]["finalizers"]
        .as_array()
        .expect("finalizers must be present after Foreground delete")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        finalizers.contains(&"foregroundDeletion"),
        "Foreground propagation must add the foregroundDeletion finalizer, got {finalizers:?}",
    );
    assert!(
        stored_rs["metadata"]["deletionTimestamp"].is_string(),
        "ReplicaSet must carry a deletionTimestamp post-DELETE",
    );

    // Dependents are still present — the GC controller has not run yet.
    for pod_name in ["pod-x", "pod-y"] {
        assert!(
            snapshot(&mem, &build_key("pods", Some("gc-fg"), pod_name))
                .await
                .is_some(),
            "owned pod {pod_name} should still be present until GC controller runs",
        );
    }
}

/// DELETE a ReplicaSet with `?propagationPolicy=Background`. The api-server
/// contract for Background propagation is: **no `foregroundDeletion`
/// finalizer added**. If the owner has no other finalizers, the owner is
/// removed from storage immediately (Background = "fire and forget";
/// dependents get cleaned up later by GC).
#[tokio::test]
async fn test_lifecycle_owner_background_deletion_removes_owner() {
    let (mem, router) = spawn_router();

    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("gc-bg"),
    )
    .await;
    assert_success("namespace POST", s, &b);

    let (s, b) = send_json(
        &router,
        Method::POST,
        "/apis/apps/v1/namespaces/gc-bg/replicasets",
        &replicaset_stub("gc-bg", "rs2"),
    )
    .await;
    assert_success("replicaset POST", s, &b);

    let (status, body) = send_delete(
        &router,
        "/apis/apps/v1/namespaces/gc-bg/replicasets/rs2?propagationPolicy=Background",
    )
    .await;
    assert_success("rs2 DELETE Background", status, &body);

    let stored = snapshot(&mem, &build_key("replicasets", Some("gc-bg"), "rs2")).await;
    assert!(
        stored.is_none(),
        "Background DELETE should remove the ReplicaSet from storage, found {stored:?}",
    );
}

/// Mirror of upstream
/// `test/integration/garbagecollector/garbage_collector_test.go::TestCascadingDeletion`:
/// once the garbage collector observes the owner has only the
/// `foregroundDeletion` finalizer, it deletes dependents and then clears the
/// finalizer, after which the owner is removed.
///
/// Rusternetes implements this in
/// `crates/controller-manager/src/controllers/garbage_collector.rs`. This
/// test wires it in-process the same way `e2e_inprocess_smoke_test.rs` does:
/// the GC drives `Arc<MemoryStorage>` directly via `Storage`, while the
/// router shares the same map through `StorageBackend::Memory`.
#[tokio::test]
async fn test_lifecycle_owner_foreground_eventual_dependent_deletion() {
    let (mem, router) = spawn_router();

    // 1. POST namespace + owner ReplicaSet + dependent pods with
    //    controller=true.
    let (s, b) = send_json(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("gc-cascade"),
    )
    .await;
    assert_success("namespace POST", s, &b);

    let (s, rs_body) = send_json(
        &router,
        Method::POST,
        "/apis/apps/v1/namespaces/gc-cascade/replicasets",
        &replicaset_stub("gc-cascade", "rs1"),
    )
    .await;
    assert_success("replicaset POST", s, &rs_body);
    let owner_uid = rs_body["metadata"]["uid"]
        .as_str()
        .expect("ReplicaSet must have a UID")
        .to_string();

    for pod_name in ["pod-x", "pod-y"] {
        let (s, b) = send_json(
            &router,
            Method::POST,
            "/api/v1/namespaces/gc-cascade/pods",
            &owned_pod_stub("gc-cascade", pod_name, "ReplicaSet", "rs1", &owner_uid),
        )
        .await;
        assert_success(&format!("owned pod {pod_name} POST"), s, &b);
    }

    // 2. DELETE owner with Foreground propagation. The api-server attaches
    //    the foregroundDeletion finalizer + deletionTimestamp; nothing is
    //    actually removed yet.
    let (status, body) = send_delete(
        &router,
        "/apis/apps/v1/namespaces/gc-cascade/replicasets/rs1?propagationPolicy=Foreground",
    )
    .await;
    assert_success("rs1 DELETE Foreground", status, &body);

    let stored_rs = snapshot(&mem, &build_key("replicasets", Some("gc-cascade"), "rs1"))
        .await
        .expect("ReplicaSet must remain in storage while finalizers are pending");
    let finalizers: Vec<&str> = stored_rs["metadata"]["finalizers"]
        .as_array()
        .expect("finalizers array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        finalizers.contains(&"foregroundDeletion"),
        "Foreground propagation must add the foregroundDeletion finalizer, got {finalizers:?}",
    );

    // 3. Tick the GC. `scan_and_collect` deletes dependents (Pods with
    //    `controller=true` ownerReferences pointing at the dying owner),
    //    strips the foregroundDeletion finalizer, then re-reads and deletes
    //    the now-unblocked ReplicaSet in the same pass (see
    //    `process_deletion` in
    //    `crates/controller-manager/src/controllers/garbage_collector.rs`).
    let gc = GarbageCollector::new(mem.clone());
    gc.scan_and_collect().await.expect("gc scan");

    // 4. Dependents are gone.
    for pod_name in ["pod-x", "pod-y"] {
        assert!(
            snapshot(&mem, &build_key("pods", Some("gc-cascade"), pod_name))
                .await
                .is_none(),
            "owned pod {pod_name} must be GC'd after foreground cascade",
        );
    }

    // 5. Owner is gone (foregroundDeletion was the only finalizer and the
    //    GC clears it once dependents are reaped).
    assert!(
        snapshot(&mem, &build_key("replicasets", Some("gc-cascade"), "rs1"))
            .await
            .is_none(),
        "ReplicaSet rs1 must be removed once GC clears foregroundDeletion",
    );
    let (status, _) = send_get(
        &router,
        "/apis/apps/v1/namespaces/gc-cascade/replicasets/rs1",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "ReplicaSet GET must be 404 after foreground cascade",
    );
}

/// Sanity test: list with a label selector must never bleed objects across
/// namespaces. Two namespaces, three pods each; list one namespace with a
/// selector and assert only matching pods from that namespace come back.
#[tokio::test]
async fn test_lifecycle_label_selector_does_not_cross_namespaces() {
    let (mem, router) = spawn_router();

    for ns in ["proj-a", "proj-b"] {
        let (s, b) = send_json(
            &router,
            Method::POST,
            "/api/v1/namespaces",
            &namespace_stub(ns),
        )
        .await;
        assert_success(&format!("namespace {ns} POST"), s, &b);

        for i in 0..3 {
            let labels = json!({"tier": if i == 0 { "frontend" } else { "backend" }});
            let (s, b) = send_json(
                &router,
                Method::POST,
                &format!("/api/v1/namespaces/{ns}/pods"),
                &pod_stub(ns, &format!("p{i}"), labels),
            )
            .await;
            assert_success(&format!("POST {ns}/pods/p{i}"), s, &b);
        }
    }

    let (status, body) = send_get(
        &router,
        "/api/v1/namespaces/proj-a/pods?labelSelector=tier%3Dbackend",
    )
    .await;
    assert_success("list proj-a tier=backend", status, &body);
    let matched = names(&body);
    assert_eq!(
        matched.len(),
        2,
        "proj-a tier=backend list should yield 2 pods, got {matched:?}",
    );
    for n in &matched {
        let stored = snapshot(&mem, &build_key("pods", Some("proj-a"), n))
            .await
            .expect("matched pod must exist");
        assert_eq!(stored["metadata"]["namespace"].as_str(), Some("proj-a"));
    }

    for (ns, expected) in [("proj-a", 3), ("proj-b", 3)] {
        let stored = mem
            .list::<Value>(&build_prefix("pods", Some(ns)))
            .await
            .unwrap();
        assert_eq!(stored.len(), expected, "expected {expected} pods in {ns}");
    }
}
