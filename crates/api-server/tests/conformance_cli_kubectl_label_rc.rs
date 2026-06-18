//! Mirror of upstream Kubernetes v1.35 conformance tests for [sig-cli]:
//!
//!   * `Kubectl client Kubectl label should update the label on a resource [Conformance]`
//!   * `Kubectl client Update Demo should create and stop a replication controller [Conformance]`
//!   * `Kubectl client Update Demo should scale a replication controller [Conformance]`
//!
//! Upstream source (release-1.35):
//!   https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/kubectl/kubectl.go
//!
//! # Strategy
//!
//! These tests use the router-harness (option a) introduced in
//! `conformance_pod_generation.rs`: an in-process Axum router over
//! `MemoryStorage` driven with `tower::ServiceExt::oneshot`.  No live cluster
//! is required.
//!
//! ## `kubectl label` — HTTP surface
//!
//! `kubectl label <resource> <name> key=value` issues a strategic-merge-patch
//! (or merge-patch) PATCH request against
//! `/api/v1/namespaces/{ns}/{resource}/{name}` with body
//! `{"metadata":{"labels":{"key":"value"}}}`.
//! The conformance test asserts the label appears on the returned object and
//! can subsequently be removed by patching `{"metadata":{"labels":{"key":null}}}`.
//!
//! ## `Update Demo create/stop RC` — HTTP surface
//!
//! `kubectl run` creates a ReplicationController (legacy path) then `kubectl
//! delete` sends DELETE. The conformance test asserts the RC transitions from
//! present to absent, and that intermediate list responses reflect the current
//! state.
//!
//! ## `Update Demo scale RC` — HTTP surface
//!
//! `kubectl scale rc` issues a PATCH (or PUT) against
//! `/api/v1/namespaces/{ns}/replicationcontrollers/{name}/scale` with
//! `{"spec":{"replicas":N}}`.  The conformance test drives the replica count
//! from the initial value through several scale steps (down to 0, back up) and
//! asserts each step is reflected in the stored object.

use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

const TEST_NS: &str = "sig-cli-test";

// ---------------------------------------------------------------------------
// Router harness — thin `(u16, Value)` shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn post_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router.post(uri, body).await;
    (status.as_u16(), value)
}

async fn patch_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    // Default kubectl `label`/`annotate` uses a strategic/merge patch; the
    // harness `patch` helper sends `application/merge-patch+json`.
    let (status, value) = router.patch(uri, body).await;
    (status.as_u16(), value)
}

async fn get_json(router: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.get(uri).await;
    (status.as_u16(), value)
}

async fn delete_resource(router: &TestApiServer, uri: &str) -> u16 {
    let (status, _) = router.delete(uri).await;
    status.as_u16()
}

/// Ensure the test namespace exists.
async fn create_namespace(router: &TestApiServer) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": TEST_NS },
    });
    let (status, _) = post_json(router, "/api/v1/namespaces", &body).await;
    assert!(
        status == 201 || status == 200,
        "namespace create failed: {status}"
    );
}

/// Minimal ReplicationController body.
fn rc_body(name: &str, replicas: i64) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
        },
        "spec": {
            "replicas": replicas,
            "selector": { "app": name },
            "template": {
                "metadata": { "labels": { "app": name } },
                "spec": {
                    "containers": [{"name": "c", "image": "nginx:1.27"}]
                }
            }
        }
    })
}

// ===========================================================================
// 1. kubectl label — update the label on a resource
//    Upstream: kubectl.go — "should update the label on a resource"
// ===========================================================================

/// Mirror of upstream conformance: `kubectl label pod <name> key=value`
/// issues a merge-patch PATCH and the label appears on the returned Pod.
///
/// kubectl.go (release-1.35) "should update the label on a resource":
///   - creates a pod
///   - runs `kubectl label pod <name> app=testlabel`
///   - asserts the pod now carries `app=testlabel`
///   - runs `kubectl label pod <name> app-` (remove)
///   - asserts the label is gone
#[tokio::test]
async fn test_kubectl_label_add_and_remove_on_pod() {
    let (_mem, router) = spawn_router();
    create_namespace(&router).await;

    // Create a pod.
    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "label-test-pod", "namespace": TEST_NS },
        "spec": {
            "containers": [{"name": "c", "image": "nginx:1.27"}]
        }
    });
    let (status, _) = post_json(
        &router,
        &format!("/api/v1/namespaces/{TEST_NS}/pods"),
        &pod_body,
    )
    .await;
    assert!(
        status == 201 || status == 200,
        "pod create failed: {status}"
    );

    // kubectl label pod label-test-pod app=testlabel
    let patch_add = json!({ "metadata": { "labels": { "app": "testlabel" } } });
    let (status, patched) = patch_json(
        &router,
        &format!("/api/v1/namespaces/{TEST_NS}/pods/label-test-pod"),
        &patch_add,
    )
    .await;
    assert_eq!(
        status, 200,
        "PATCH to add label must return 200; got {status} body={patched}"
    );
    assert_eq!(
        patched["metadata"]["labels"]["app"].as_str(),
        Some("testlabel"),
        "label 'app=testlabel' must be present after PATCH; got {patched}"
    );

    // kubectl label pod label-test-pod app-  (remove)
    let patch_remove = json!({ "metadata": { "labels": { "app": null } } });
    let (status, after_remove) = patch_json(
        &router,
        &format!("/api/v1/namespaces/{TEST_NS}/pods/label-test-pod"),
        &patch_remove,
    )
    .await;
    assert_eq!(
        status, 200,
        "PATCH to remove label must return 200; got {status} body={after_remove}"
    );
    // kubectl's post-removal GET shows the key fully ABSENT from the labels
    // map — not present-with-null. The map may itself be absent (no labels
    // left); in that case "app" is trivially absent. If the map is present it
    // must NOT contain the "app" key.
    let labels_after = &after_remove["metadata"]["labels"];
    let app_absent = match labels_after.as_object() {
        Some(map) => !map.contains_key("app"),
        None => true, // labels map omitted entirely → key is absent
    };
    assert!(
        app_absent,
        "label 'app' must be absent (not null) after removal PATCH; got {after_remove}"
    );
}

/// Mirror of the node-label variant: `kubectl label node <name> key=value`.
/// Nodes are cluster-scoped, so the path is `/api/v1/nodes/{name}` — not
/// namespace-scoped.
///
/// Upstream kubectl.go (release-1.35) exercises both namespace-scoped and
/// cluster-scoped resources in the same conformance test.
#[tokio::test]
async fn test_kubectl_label_add_on_cluster_scoped_node() {
    let (_mem, router) = spawn_router();

    // Create a node.
    let node_body = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": { "name": "label-test-node" },
        "spec": {}
    });
    let (status, _) = post_json(&router, "/api/v1/nodes", &node_body).await;
    assert!(
        status == 201 || status == 200,
        "node create failed: {status}"
    );

    // kubectl label node label-test-node env=staging
    let patch = json!({ "metadata": { "labels": { "env": "staging" } } });
    let (status, patched) = patch_json(&router, "/api/v1/nodes/label-test-node", &patch).await;
    assert_eq!(
        status, 200,
        "PATCH label on node must return 200; got {status} body={patched}"
    );
    assert_eq!(
        patched["metadata"]["labels"]["env"].as_str(),
        Some("staging"),
        "label 'env=staging' must be present on node; got {patched}"
    );
}

/// `kubectl label` must be able to set a label with `--overwrite` semantics:
/// patching an already-present key to a new value replaces the old value.
///
/// Upstream test: kubectl.go "should update the label on a resource" includes
/// a second `kubectl label --overwrite` step.
#[tokio::test]
async fn test_kubectl_label_overwrite_existing_value() {
    let (_mem, router) = spawn_router();
    create_namespace(&router).await;

    // Create pod with an initial label.
    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "overwrite-pod",
            "namespace": TEST_NS,
            "labels": { "version": "v1" }
        },
        "spec": {
            "containers": [{"name": "c", "image": "nginx:1.27"}]
        }
    });
    let (status, _) = post_json(
        &router,
        &format!("/api/v1/namespaces/{TEST_NS}/pods"),
        &pod_body,
    )
    .await;
    assert!(
        status == 201 || status == 200,
        "pod create failed: {status}"
    );

    // kubectl label pod overwrite-pod version=v2 --overwrite
    let patch = json!({ "metadata": { "labels": { "version": "v2" } } });
    let (status, patched) = patch_json(
        &router,
        &format!("/api/v1/namespaces/{TEST_NS}/pods/overwrite-pod"),
        &patch,
    )
    .await;
    assert_eq!(
        status, 200,
        "overwrite PATCH must return 200; body={patched}"
    );
    assert_eq!(
        patched["metadata"]["labels"]["version"].as_str(),
        Some("v2"),
        "label 'version' must now be 'v2'; got {patched}"
    );
}

// ===========================================================================
// 2. Update Demo — create and stop a ReplicationController
//    Upstream: kubectl.go — "should create and stop a replication controller"
// ===========================================================================

/// Mirror of upstream conformance: creates an RC, verifies it is returned by
/// GET, then deletes it via DELETE and asserts a subsequent GET returns 404.
///
/// kubectl.go (release-1.35) "should create and stop a replication controller":
///   - `kubectl run <name> --image=... --replicas=1 --generator=run/v1`
///     → POST /api/v1/namespaces/.../replicationcontrollers
///   - asserts GET returns the RC
///   - `kubectl delete rc <name>`
///     → DELETE /api/v1/namespaces/.../replicationcontrollers/<name>
///   - asserts subsequent GET returns 404 (or the RC no longer appears in List)
#[tokio::test]
async fn test_update_demo_create_and_stop_rc() {
    let (_mem, router) = spawn_router();
    create_namespace(&router).await;

    let rc_name = "update-demo-rc";
    let create_uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers");
    let rc_uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers/{rc_name}");

    // Create the RC.
    let body = rc_body(rc_name, 1);
    let (status, created) = post_json(&router, &create_uri, &body).await;
    assert!(
        status == 201 || status == 200,
        "RC create must succeed; got {status} body={created}"
    );
    assert_eq!(
        created["metadata"]["name"].as_str(),
        Some(rc_name),
        "created RC name must match"
    );

    // GET the RC — must be present.
    let (status, fetched) = get_json(&router, &rc_uri).await;
    assert_eq!(
        status, 200,
        "GET after create must return 200; body={fetched}"
    );
    assert_eq!(
        fetched["metadata"]["name"].as_str(),
        Some(rc_name),
        "fetched RC name must match"
    );

    // DELETE the RC.
    let status = delete_resource(&router, &rc_uri).await;
    assert!(
        status == 200 || status == 202 || status == 204,
        "DELETE must succeed; got {status}"
    );

    // GET the RC — must now be 404.
    let (status, _) = get_json(&router, &rc_uri).await;
    assert_eq!(status, 404, "GET after delete must return 404 (RC stopped)");
}

// ===========================================================================
// 3. Update Demo — scale a ReplicationController
//    Upstream: kubectl.go — "should scale a replication controller"
// ===========================================================================

/// Mirror of upstream conformance: creates an RC with 1 replica, scales it
/// to 0 (stop), then back to 2 (up), and asserts the replica count in the
/// stored scale sub-resource is reflected at each step.
///
/// kubectl.go (release-1.35) "should scale a replication controller":
///   - creates RC with replicas=1
///   - `kubectl scale rc <name> --replicas=0`
///     → PATCH /api/v1/namespaces/.../replicationcontrollers/<name>/scale
///       body: {"spec":{"replicas":0}}
///   - asserts scale.spec.replicas == 0
///   - `kubectl scale rc <name> --replicas=2`
///   - asserts scale.spec.replicas == 2
#[tokio::test]
async fn test_update_demo_scale_rc() {
    let (_mem, router) = spawn_router();
    create_namespace(&router).await;

    let rc_name = "scale-demo-rc";
    let create_uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers");
    let scale_uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers/{rc_name}/scale");

    // Create the RC with replicas=1.
    let body = rc_body(rc_name, 1);
    let (status, _) = post_json(&router, &create_uri, &body).await;
    assert!(status == 201 || status == 200, "RC create failed: {status}");

    // Verify initial scale via GET /scale.
    let (status, scale) = get_json(&router, &scale_uri).await;
    assert_eq!(status, 200, "GET /scale must return 200; body={scale}");
    assert_eq!(
        scale["spec"]["replicas"].as_i64(),
        Some(1),
        "initial scale must be 1; got {scale}"
    );

    // kubectl scale rc scale-demo-rc --replicas=0
    let patch_zero = json!({ "spec": { "replicas": 0 } });
    let (status, scaled) = patch_json(&router, &scale_uri, &patch_zero).await;
    assert_eq!(
        status, 200,
        "PATCH scale to 0 must return 200; body={scaled}"
    );
    assert_eq!(
        scaled["spec"]["replicas"].as_i64(),
        Some(0),
        "scale.spec.replicas must be 0 after scale-down; got {scaled}"
    );

    // kubectl scale rc scale-demo-rc --replicas=2
    let patch_two = json!({ "spec": { "replicas": 2 } });
    let (status, scaled) = patch_json(&router, &scale_uri, &patch_two).await;
    assert_eq!(
        status, 200,
        "PATCH scale to 2 must return 200; body={scaled}"
    );
    assert_eq!(
        scaled["spec"]["replicas"].as_i64(),
        Some(2),
        "scale.spec.replicas must be 2 after scale-up; got {scaled}"
    );

    // Confirm the RC itself reflects the updated replica count via the main
    // object (not only via /scale). The RC controller would normally reconcile
    // this, but the scale handler must at minimum persist the spec.replicas
    // change so a subsequent GET shows the updated value.
    let rc_uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers/{rc_name}");
    let (status, rc) = get_json(&router, &rc_uri).await;
    assert_eq!(status, 200, "GET RC after scale must return 200; body={rc}");
    assert_eq!(
        rc["spec"]["replicas"].as_i64(),
        Some(2),
        "RC spec.replicas must reflect the last scale operation; got {rc}"
    );
}

/// Edge-case: scale an RC to the same value it already has (idempotent
/// scale). The scale handler must return 200 and not error.
#[tokio::test]
async fn test_update_demo_scale_rc_idempotent() {
    let (_mem, router) = spawn_router();
    create_namespace(&router).await;

    let rc_name = "scale-idem-rc";
    let create_uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers");
    let scale_uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers/{rc_name}/scale");

    let body = rc_body(rc_name, 3);
    let (status, _) = post_json(&router, &create_uri, &body).await;
    assert!(status == 201 || status == 200, "RC create failed: {status}");

    // Patch to the same replicas=3.
    let patch = json!({ "spec": { "replicas": 3 } });
    let (status, scaled) = patch_json(&router, &scale_uri, &patch).await;
    assert_eq!(
        status, 200,
        "idempotent scale must return 200; body={scaled}"
    );
    assert_eq!(
        scaled["spec"]["replicas"].as_i64(),
        Some(3),
        "replicas must remain 3; got {scaled}"
    );
}
