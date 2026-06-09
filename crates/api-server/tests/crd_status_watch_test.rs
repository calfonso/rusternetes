//! Regression for #63: a custom-resource `/status` subresource update MUST be
//! delivered to watchers as a MODIFIED event carrying the new `status`.
//!
//! cert-manager's controllers write an Issuer/Certificate condition via the
//! status subresource, then rely on their informer (fed by the watch stream)
//! observing that status. If the watch event doesn't carry the updated status,
//! the controller never sees its own write and re-reconciles forever
//! (hot-loop), so a Certificate is never issued.

use axum::{
    body::{to_bytes, Body},
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
use std::time::Duration;
use tower::ServiceExt;

const GROUP: &str = "stable.example.com";

fn spawn_router() -> Router {
    let mem = Arc::new(MemoryStorage::new());
    let backend = Arc::new(StorageBackend::Memory(mem));
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
    build_router(state, None)
}

async fn send(
    router: &Router,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(match body {
            Some(b) => Body::from(serde_json::to_vec(b).unwrap()),
            None => Body::empty(),
        })
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let val = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
    (status, val)
}

fn crd_with_status() -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("widgets.{GROUP}") },
        "spec": {
            "group": GROUP,
            "scope": "Namespaced",
            "names": { "plural": "widgets", "singular": "widget", "kind": "Widget", "listKind": "WidgetList" },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "subresources": { "status": {} }
            }]
        }
    })
}

#[tokio::test]
async fn cr_status_update_is_delivered_to_watchers() {
    let router = spawn_router();

    assert_eq!(
        send(
            &router,
            Method::POST,
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            Some(&crd_with_status())
        )
        .await
        .0,
        StatusCode::CREATED
    );

    let base = format!("/apis/{GROUP}/v1/namespaces/default/widgets");
    let (sc, _) = send(
        &router,
        Method::POST,
        &base,
        Some(&json!({
            "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
            "metadata": { "name": "w1", "namespace": "default" },
            "spec": { "size": 1 }
        })),
    )
    .await;
    assert_eq!(sc, StatusCode::CREATED);

    // Open the watch in a background task; it self-closes after timeoutSeconds.
    let watch_router = router.clone();
    let watch_uri = format!("{base}?watch=true&timeoutSeconds=3");
    let watch = tokio::spawn(async move {
        let req = Request::builder()
            .method(Method::GET)
            .uri(&watch_uri)
            .body(Body::empty())
            .unwrap();
        let resp = watch_router.oneshot(req).await.unwrap();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    });

    // Let the watch establish + send initial ADDED, then update status.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (sc, body) = send(
        &router,
        Method::PUT,
        &format!("{base}/w1/status"),
        Some(&json!({
            "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
            "metadata": { "name": "w1", "namespace": "default" },
            "status": { "conditions": [{ "type": "Ready", "status": "True" }] }
        })),
    )
    .await;
    assert_eq!(sc, StatusCode::OK, "status update should succeed: {body:?}");
    // The status update response itself must carry the status.
    assert_eq!(
        body["status"]["conditions"][0]["status"],
        json!("True"),
        "status PUT response missing status"
    );

    let stream = watch.await.unwrap();
    let events: Vec<Value> = stream
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .collect();

    // Find a MODIFIED (or ADDED, if the status landed before the initial event)
    // whose object carries the Ready=True status.
    let carries_status = events.iter().any(|e| {
        matches!(e["type"].as_str(), Some("MODIFIED") | Some("ADDED"))
            && e["object"]["status"]["conditions"][0]["status"] == json!("True")
    });
    assert!(
        carries_status,
        "watch never delivered an event carrying the updated status. events: {events:?}"
    );
}
