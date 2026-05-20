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
//! is owned by the namespace and garbage-collector controllers, which do not
//! run in-process for these tests. Scenarios that require those controllers
//! are pinned with `#[ignore = "blocked on issue #TBD: ..."]` and noted in
//! the PR body.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{
    build_key, build_prefix, memory::MemoryStorage, Storage, StorageBackend,
};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// HTTP harness — mirrors `integration_dryrun_all_resources.rs:82-100`.
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

fn spawn_router() -> (Arc<MemoryStorage>, axum::Router) {
    let mem = Arc::new(MemoryStorage::new());
    let router = build_router(make_state(mem.clone()), None);
    (mem, router)
}

async fn send_with_body(
    router: axum::Router,
    method: Method,
    uri: &str,
    body: Body,
    content_type: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    let response = router.oneshot(req.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

async fn send_json(
    router: axum::Router,
    method: Method,
    uri: &str,
    body: &Value,
) -> (StatusCode, Value) {
    send_with_body(
        router,
        method,
        uri,
        Body::from(serde_json::to_vec(body).unwrap()),
        Some("application/json"),
    )
    .await
}

async fn send_get(router: axum::Router, uri: &str) -> (StatusCode, Value) {
    send_with_body(router, Method::GET, uri, Body::empty(), None).await
}

async fn send_delete(router: axum::Router, uri: &str) -> (StatusCode, Value) {
    send_with_body(router, Method::DELETE, uri, Body::empty(), None).await
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
        router.clone(),
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
        let (status, resp) = send_json(router.clone(), Method::POST, uri, &body).await;
        assert_success(label, status, &resp);
    }

    for (kind, uri) in [
        ("pod", "/api/v1/namespaces/test-ns/pods/pod-a"),
        ("configmap", "/api/v1/namespaces/test-ns/configmaps/cm-a"),
        ("secret", "/api/v1/namespaces/test-ns/secrets/secret-a"),
        ("service", "/api/v1/namespaces/test-ns/services/svc-a"),
    ] {
        let (status, body) = send_get(router.clone(), uri).await;
        assert_success(&format!("GET {kind} at {uri}"), status, &body);
        assert_eq!(
            body["metadata"]["namespace"].as_str(),
            Some("test-ns"),
            "[{kind}] expected namespace=test-ns in response body",
        );
    }

    let (status, body) = send_get(
        router.clone(),
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
            router.clone(),
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
        let (status, resp) = send_json(router.clone(), Method::POST, uri, &body).await;
        assert_success(label, status, &resp);
    }

    let (status, _) = send_get(router.clone(), "/api/v1/namespaces/ns-b/pods/foo").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "pod foo must not be visible in ns-b, got {status}",
    );

    let (status, body) = send_get(router.clone(), "/api/v1/namespaces/ns-a/pods/foo").await;
    assert_success("pod foo must be visible in ns-a", status, &body);
    assert_eq!(body["metadata"]["namespace"].as_str(), Some("ns-a"));

    for (ns, expected) in [("ns-a", vec!["foo"]), ("ns-b", vec!["bar"])] {
        let (status, body) =
            send_get(router.clone(), &format!("/api/v1/namespaces/{ns}/pods")).await;
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
        router.clone(),
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("doomed-ns"),
    )
    .await;
    assert_success("namespace POST", s, &b);

    let (s, b) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/doomed-ns/pods",
        &pod_stub("doomed-ns", "p", json!({})),
    )
    .await;
    assert_success("pod POST", s, &b);

    let (s, b) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/doomed-ns/configmaps",
        &configmap_stub("doomed-ns", "c", json!({})),
    )
    .await;
    assert_success("configmap POST", s, &b);

    let (status, body) = send_delete(router.clone(), "/api/v1/namespaces/doomed-ns").await;
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
/// does this in `crates/controller-manager/src/controllers/namespace.rs`, but
/// it does NOT run in-process for these tests. Spinning it up would require
/// wiring the controller-manager into the test harness and giving it shared
/// `Arc<StorageBackend>` — out of scope for this RED-state pin.
#[tokio::test]
#[ignore = "blocked on issue #TBD: namespace cascade is handled by the namespace controller in controller-manager, not by the api-server handler — needs controller-manager spun up in-process"]
async fn test_lifecycle_namespace_cascade_deletes_children() {
    // Expected GREEN behavior (once the controller runs in-process):
    //   1. POST /api/v1/namespaces { name: "ns-c" }
    //   2. POST /api/v1/namespaces/ns-c/pods { name: "p" }
    //   3. DELETE /api/v1/namespaces/ns-c
    //   4. <wait for namespace controller to observe deletionTimestamp>
    //   5. GET /api/v1/namespaces/ns-c/pods/p → 404
    //   6. GET /api/v1/namespaces/ns-c → 404 (finalizer removed after cleanup)
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
        router.clone(),
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("gc-fg"),
    )
    .await;
    assert_success("namespace POST", s, &b);

    let (s, rs_body) = send_json(
        router.clone(),
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
            router.clone(),
            Method::POST,
            "/api/v1/namespaces/gc-fg/pods",
            &owned_pod_stub("gc-fg", pod_name, "ReplicaSet", "rs1", &owner_uid),
        )
        .await;
        assert_success(&format!("owned pod {pod_name} POST"), s, &b);
    }

    let (status, body) = send_delete(
        router.clone(),
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
        router.clone(),
        Method::POST,
        "/api/v1/namespaces",
        &namespace_stub("gc-bg"),
    )
    .await;
    assert_success("namespace POST", s, &b);

    let (s, b) = send_json(
        router.clone(),
        Method::POST,
        "/apis/apps/v1/namespaces/gc-bg/replicasets",
        &replicaset_stub("gc-bg", "rs2"),
    )
    .await;
    assert_success("replicaset POST", s, &b);

    let (status, body) = send_delete(
        router.clone(),
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
/// `crates/controller-manager/src/controllers/garbagecollector.rs` but that
/// controller does not run in-process for these tests. Pinned for tracking.
#[tokio::test]
#[ignore = "blocked on issue #TBD: ownerReference cascade reaping requires the garbage-collector controller running in-process — out of scope for this api-server-only integration test"]
async fn test_lifecycle_owner_foreground_eventual_dependent_deletion() {
    // Expected GREEN behavior:
    //   1. POST owner (ReplicaSet) + dependent pods with controller=true.
    //   2. DELETE owner?propagationPolicy=Foreground.
    //   3. <wait for GC controller to observe foregroundDeletion finalizer>
    //   4. Each dependent pod has metadata.deletionTimestamp set (and is
    //      eventually removed once its own finalizers clear).
    //   5. Once all dependents are gone, the GC clears foregroundDeletion
    //      from the owner; the owner is then removed from storage.
}

/// Sanity test: list with a label selector must never bleed objects across
/// namespaces. Two namespaces, three pods each; list one namespace with a
/// selector and assert only matching pods from that namespace come back.
#[tokio::test]
async fn test_lifecycle_label_selector_does_not_cross_namespaces() {
    let (mem, router) = spawn_router();

    for ns in ["proj-a", "proj-b"] {
        let (s, b) = send_json(
            router.clone(),
            Method::POST,
            "/api/v1/namespaces",
            &namespace_stub(ns),
        )
        .await;
        assert_success(&format!("namespace {ns} POST"), s, &b);

        for i in 0..3 {
            let labels = json!({"tier": if i == 0 { "frontend" } else { "backend" }});
            let (s, b) = send_json(
                router.clone(),
                Method::POST,
                &format!("/api/v1/namespaces/{ns}/pods"),
                &pod_stub(ns, &format!("p{i}"), labels),
            )
            .await;
            assert_success(&format!("POST {ns}/pods/p{i}"), s, &b);
        }
    }

    let (status, body) = send_get(
        router.clone(),
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
