//! Regression for #63: a custom-resource `/status` subresource update MUST be
//! delivered to watchers as a MODIFIED event carrying the new `status`.
//!
//! cert-manager's controllers write an Issuer/Certificate condition via the
//! status subresource, then rely on their informer (fed by the watch stream)
//! observing that status. If the watch event doesn't carry the updated status,
//! the controller never sees its own write and re-reconciles forever
//! (hot-loop), so a Certificate is never issued.

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;

const GROUP: &str = "stable.example.com";

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

async fn send(
    router: &TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    router.send(method.as_str(), uri, content_type, body).await
}

/// Collect a self-closing watch stream (`?watch=true&timeoutSeconds=…`) into a
/// single string. `send_full` awaits the full body, which arrives once the
/// server closes the stream at the timeout.
async fn collect_watch(router: &TestApiServer, uri: &str) -> String {
    let (_status, _headers, bytes, _) = router.send_full("GET", uri, None, None, None).await;
    String::from_utf8(bytes).unwrap()
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
    let watch = tokio::spawn(async move { collect_watch(&watch_router, &watch_uri).await });

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
