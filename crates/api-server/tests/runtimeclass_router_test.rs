//! Router-driven (in-process) coverage for the RuntimeClass API surface and
//! the Pod-admission behaviour that depends on it.
//!
//! These tests exercise already-implemented features end-to-end through the
//! real api-server routes — no live cluster, scheduler or kubelet — using the
//! `build_router` + `MemoryStorage` + `tower::oneshot` harness (mirrors
//! `crates/api-server/tests/list_empty_items_router_test.rs`).
//!
//! Behaviours covered (verified against k8s release-1.35
//! `test/e2e/common/node/runtimeclass.go`):
//!   1. RuntimeClass CRUD lifecycle: create → get → list → patch → delete.
//!   2. Pod admission rejects a reference to a *deleted* RuntimeClass, while a
//!      Pod created before deletion is unaffected.
//!   3. PodOverhead injection: a RuntimeClass with `overhead.podFixed` causes
//!      the api-server to inject `pod.spec.overhead`; a RuntimeClass without
//!      overhead leaves `pod.spec.overhead` absent.
//!
//! Implementation references:
//!   * RuntimeClass handlers: `crates/api-server/src/handlers/runtimeclass.rs`
//!   * Pod admission: `crates/api-server/src/handlers/pod.rs` (RuntimeClass
//!     existence check + overhead injection).

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
// HTTP harness
// ---------------------------------------------------------------------------

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

fn router_for(state: &Arc<ApiServerState>) -> axum::Router {
    build_router(state.clone(), None)
}

async fn read_body(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, body)
}

async fn send(
    state: &Arc<ApiServerState>,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(ct) = content_type {
        builder = builder.header("content-type", ct);
    }
    let req = match body {
        Some(b) => builder
            .body(Body::from(serde_json::to_vec(b).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = router_for(state).oneshot(req).await.unwrap();
    read_body(response).await
}

async fn post(state: &Arc<ApiServerState>, uri: &str, body: &Value) -> (StatusCode, Value) {
    send(state, "POST", uri, Some("application/json"), Some(body)).await
}

async fn get(state: &Arc<ApiServerState>, uri: &str) -> (StatusCode, Value) {
    send(state, "GET", uri, None, None).await
}

async fn delete(state: &Arc<ApiServerState>, uri: &str) -> (StatusCode, Value) {
    send(state, "DELETE", uri, None, None).await
}

async fn patch(state: &Arc<ApiServerState>, uri: &str, body: &Value) -> (StatusCode, Value) {
    send(
        state,
        "PATCH",
        uri,
        Some("application/merge-patch+json"),
        Some(body),
    )
    .await
}

const RC_COLLECTION: &str = "/apis/node.k8s.io/v1/runtimeclasses";

fn rc_item(name: &str) -> String {
    format!("/apis/node.k8s.io/v1/runtimeclasses/{name}")
}

async fn create_namespace(state: &Arc<ApiServerState>, name: &str) {
    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    });
    let (status, _) = post(state, "/api/v1/namespaces", &ns).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create returned {status}",
    );
}

fn pod_with_rc(name: &str, rc_name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name },
        "spec": {
            "runtimeClassName": rc_name,
            "containers": [
                { "name": "main", "image": "busybox" }
            ]
        }
    })
}

// ---------------------------------------------------------------------------
// 1. RuntimeClass CRUD lifecycle
// ---------------------------------------------------------------------------

/// create → get → list → patch → delete against the RuntimeClass routes.
/// Mirrors upstream "should support RuntimeClasses API operations".
#[tokio::test]
async fn runtimeclass_crud_lifecycle() {
    let state = make_state();

    // -- create --
    let rc = json!({
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": { "name": "crud-rc" },
        "handler": "runc",
    });
    let (status, body) = post(&state, RC_COLLECTION, &rc).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create returned {status}: {body}"
    );
    assert_eq!(body["kind"], "RuntimeClass");
    assert_eq!(body["handler"], "runc");
    assert_eq!(body["metadata"]["name"], "crud-rc");

    // -- get --
    let (status, body) = get(&state, &rc_item("crud-rc")).await;
    assert_eq!(status, StatusCode::OK, "get returned {status}: {body}");
    assert_eq!(body["handler"], "runc");

    // -- list --
    let (status, body) = get(&state, RC_COLLECTION).await;
    assert_eq!(status, StatusCode::OK, "list returned {status}: {body}");
    assert_eq!(body["kind"], "RuntimeClassList");
    let items = body["items"].as_array().expect("items must be an array");
    assert_eq!(items.len(), 1, "expected exactly one RuntimeClass: {body}");
    assert_eq!(items[0]["metadata"]["name"], "crud-rc");

    // -- patch (merge-patch a label) --
    let patch_body = json!({ "metadata": { "labels": { "tier": "secure" } } });
    let (status, body) = patch(&state, &rc_item("crud-rc"), &patch_body).await;
    assert_eq!(status, StatusCode::OK, "patch returned {status}: {body}");
    assert_eq!(
        body["metadata"]["labels"]["tier"], "secure",
        "patched label must be present: {body}"
    );
    // Handler must be untouched by the metadata-only patch.
    assert_eq!(
        body["handler"], "runc",
        "patch must not drop handler: {body}"
    );

    // -- delete --
    let (status, _body) = delete(&state, &rc_item("crud-rc")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "delete returned {status}",
    );

    // -- get after delete: gone --
    let (status, _body) = get(&state, &rc_item("crud-rc")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "get after delete must be 404, got {status}",
    );
}

// ---------------------------------------------------------------------------
// 2. Deny Pod for deleted RuntimeClass
// ---------------------------------------------------------------------------

/// Pod referencing an existing RuntimeClass is admitted; after the
/// RuntimeClass is deleted, a *new* Pod referencing it is rejected, while the
/// Pod created earlier is unaffected. Mirrors upstream "should reject a Pod
/// requesting a deleted RuntimeClass".
#[tokio::test]
async fn pod_referencing_deleted_runtimeclass_is_rejected() {
    let state = make_state();
    create_namespace(&state, "default").await;

    // Create RuntimeClass "myrc".
    let rc = json!({
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": { "name": "myrc" },
        "handler": "runc",
    });
    let (status, body) = post(&state, RC_COLLECTION, &rc).await;
    assert_eq!(status, StatusCode::CREATED, "rc create: {status} {body}");

    // First Pod referencing "myrc" is accepted while the RC exists.
    let pods_uri = "/api/v1/namespaces/default/pods";
    let (status, body) = post(&state, pods_uri, &pod_with_rc("pod-before", "myrc")).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod referencing existing RC must be accepted, got {status}: {body}",
    );

    // Delete "myrc".
    let (status, _body) = delete(&state, &rc_item("myrc")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "rc delete returned {status}",
    );
    // Confirm it is gone.
    let (status, _body) = get(&state, &rc_item("myrc")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "RC must be deleted");

    // A new Pod referencing the now-deleted "myrc" must be rejected (Forbidden).
    let (status, body) = post(&state, pods_uri, &pod_with_rc("pod-after", "myrc")).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "pod referencing deleted RC must be 403 Forbidden, got {status}: {body}",
    );

    // The Pod created before deletion is unaffected — deletion only gates new pods.
    let (status, body) = get(&state, "/api/v1/namespaces/default/pods/pod-before").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "pod created before RC deletion must still exist, got {status}: {body}",
    );
}

// ---------------------------------------------------------------------------
// 3. PodOverhead injection
// ---------------------------------------------------------------------------

/// A RuntimeClass with `overhead.podFixed` causes the api-server to inject
/// `pod.spec.overhead` at admission. Mirrors upstream "should schedule a Pod
/// requesting a RuntimeClass and initialize its Overhead" (the overhead
/// initialisation half, which rusternetes performs at admission rather than in
/// the scheduler).
#[tokio::test]
async fn pod_overhead_injected_from_runtimeclass() {
    let state = make_state();
    create_namespace(&state, "default").await;

    // RuntimeClass with overhead.podFixed = {cpu: 250m, memory: 120Mi}.
    let rc = json!({
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": { "name": "overhead-rc" },
        "handler": "kata",
        "overhead": { "podFixed": { "cpu": "250m", "memory": "120Mi" } },
    });
    let (status, body) = post(&state, RC_COLLECTION, &rc).await;
    assert_eq!(status, StatusCode::CREATED, "rc create: {status} {body}");

    // Create a Pod referencing it.
    let pods_uri = "/api/v1/namespaces/default/pods";
    let (status, body) = post(&state, pods_uri, &pod_with_rc("over-pod", "overhead-rc")).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod create must succeed, got {status}: {body}",
    );

    // GET the pod and assert overhead was injected to match podFixed.
    let (status, body) = get(&state, "/api/v1/namespaces/default/pods/over-pod").await;
    assert_eq!(status, StatusCode::OK, "get pod: {status} {body}");
    let overhead = &body["spec"]["overhead"];
    assert!(
        overhead.is_object(),
        "spec.overhead must be injected as an object, got {overhead:?} (full: {body})",
    );
    assert_eq!(
        overhead["cpu"], "250m",
        "spec.overhead.cpu must match podFixed: {body}",
    );
    assert_eq!(
        overhead["memory"], "120Mi",
        "spec.overhead.memory must match podFixed: {body}",
    );
}

/// A RuntimeClass WITHOUT overhead leaves `pod.spec.overhead` absent — no
/// resource inflation. Mirrors upstream "should schedule a Pod requesting a
/// RuntimeClass without PodOverhead" (the admission half).
#[tokio::test]
async fn pod_without_overhead_runtimeclass_has_no_injected_overhead() {
    let state = make_state();
    create_namespace(&state, "default").await;

    // RuntimeClass with no overhead.
    let rc = json!({
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": { "name": "plain-rc" },
        "handler": "runc",
    });
    let (status, body) = post(&state, RC_COLLECTION, &rc).await;
    assert_eq!(status, StatusCode::CREATED, "rc create: {status} {body}");

    let pods_uri = "/api/v1/namespaces/default/pods";
    let (status, body) = post(&state, pods_uri, &pod_with_rc("plain-pod", "plain-rc")).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod create must succeed, got {status}: {body}",
    );

    let (status, body) = get(&state, "/api/v1/namespaces/default/pods/plain-pod").await;
    assert_eq!(status, StatusCode::OK, "get pod: {status} {body}");
    let overhead = &body["spec"]["overhead"];
    assert!(
        overhead.is_null(),
        "spec.overhead must be absent for a RuntimeClass without overhead, got {overhead:?}: {body}",
    );
}
