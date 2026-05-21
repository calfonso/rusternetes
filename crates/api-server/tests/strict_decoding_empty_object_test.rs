//! Strict-decoding regression for empty-object originals that our typed
//! deserialiser collapses to `None`.
//!
//! Discovered during canary 2026-05-21 run — `[sig-node] Pods should
//! allow activeDeadlineSeconds to be updated [Conformance]` and 3 sibling
//! tests failed with
//!
//!     strict decoding error: unknown field "status.containerStatuses[0].lastState"
//!
//! The K8s typed client (and `kubectl`) serialises an empty
//! `ContainerState` as `{}` rather than dropping the key, because the
//! Go marshaller preserves zero-valued struct fields. Our
//! `deserialize_container_state_option` helper (pod.rs:9-24) treats the
//! empty object as `None` so the tagged-enum parse doesn't fail; then
//! `#[serde(skip_serializing_if = "Option::is_none")]` strips the field
//! from the canonical round-trip; then the strict differ flags it as
//! "unknown field".
//!
//! Same class of false-positive as the `creationTimestamp: null` case
//! fixed in PR #687. This file pins the `{}` case so the heuristic in
//! `find_unknown_fields_recursive` accepts both forms.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    Router,
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const TEST_NS: &str = "default";

fn make_router() -> (Arc<MemoryStorage>, Router) {
    let mem = Arc::new(MemoryStorage::new());
    let backend = Arc::new(StorageBackend::Memory(mem.clone()));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    let state = Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true,
    ));
    let router = build_router(state, None);
    (mem, router)
}

async fn send(router: Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let bytes = serde_json::to_vec(&body).unwrap();
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(bytes))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let bs = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bs).unwrap_or(json!(null)))
}

/// Pod create with `status.containerStatuses[0].lastState: {}` must be
/// accepted under default-Strict. Mirrors what client-go emits when a
/// container has no prior terminated/waiting/running state.
#[tokio::test]
async fn test_pod_with_empty_last_state_accepted_under_strict() {
    let (_, router) = make_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p1", "namespace": TEST_NS},
        "spec": {
            "containers": [{"name": "c", "image": "pause:latest"}]
        },
        "status": {
            "containerStatuses": [{
                "name": "c",
                "ready": false,
                "restartCount": 0,
                "image": "pause:latest",
                "imageID": "",
                "state": {},
                "lastState": {}
            }]
        }
    });
    let (status, resp) = send(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{}/pods", TEST_NS),
        body,
    )
    .await;
    assert!(
        status.is_success(),
        "containerStatus.lastState: {{}} must be accepted under default-Strict; got {} body={}",
        status,
        resp
    );
}

/// Same shape on `initContainerStatuses` — separate path through the
/// deserialiser, deserves its own pin so a future refactor can't
/// silently drop it.
#[tokio::test]
async fn test_pod_with_empty_init_container_last_state_accepted_under_strict() {
    let (_, router) = make_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p2", "namespace": TEST_NS},
        "spec": {
            "containers": [{"name": "c", "image": "pause:latest"}],
            "initContainers": [{"name": "init", "image": "pause:latest"}]
        },
        "status": {
            "initContainerStatuses": [{
                "name": "init",
                "ready": false,
                "restartCount": 0,
                "image": "pause:latest",
                "imageID": "",
                "state": {},
                "lastState": {}
            }]
        }
    });
    let (status, resp) = send(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{}/pods", TEST_NS),
        body,
    )
    .await;
    assert!(
        status.is_success(),
        "initContainerStatus.lastState: {{}} must be accepted under default-Strict; got {} body={}",
        status,
        resp
    );
}

/// Negative guard kept from PR #687's siblings: non-null + non-empty
/// truly-unknown fields are still rejected.
#[tokio::test]
async fn test_genuinely_unknown_non_empty_field_still_rejected_under_strict() {
    let (_, router) = make_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p3", "namespace": TEST_NS},
        "spec": {
            "containers": [{"name": "c", "image": "pause:latest"}],
            "thisFieldDoesNotExist": "has-a-value"
        }
    });
    let (status, resp) = send(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{}/pods", TEST_NS),
        body,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "truly unknown non-empty field must still be rejected; got {} body={}",
        status,
        resp
    );
}
