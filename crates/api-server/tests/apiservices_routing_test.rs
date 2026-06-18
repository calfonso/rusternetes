//! Regression: APIService CRUD routes used to be wired into `public_routes`
//! even though every handler consumed `Extension<AuthContext>`. The auth
//! middleware only runs on `protected_routes`, so the extension extractor
//! failed before the handler ran and every request returned **500 Internal
//! Server Error** with `Missing request extension`. Routes were moved into
//! `protected_routes` — these tests pin the fix by driving the real Axum
//! router with `tower::ServiceExt::oneshot` and asserting non-500 responses
//! on every CRUD verb.
//!
//! Surfaced by Unit 7 of the /batch conformance mirror
//! (`crates/api-server/tests/conformance_apimachinery_aggregation_discovery.rs`,
//! PR #78) which had to work around the bug by seeding APIServices through
//! the storage layer instead.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

async fn make_test_state() -> TestApiServer {
    TestApiServer::new()
}

// Thin `(u16, Value)` shim over the shared harness, preserving this file's
// `send(&state, method, path, Option<Value>)` call sites.
async fn send(api: &TestApiServer, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    let (status, value) = api.send(method, path, content_type, body.as_ref()).await;
    (status.as_u16(), value)
}

fn local_apiservice(name: &str) -> Value {
    json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": { "name": name },
        "spec": {
            "group": "example.com",
            "version": "v1",
            "versionPriority": 100,
            "groupPriorityMinimum": 1000,
        }
    })
}

#[tokio::test]
async fn create_apiservice_returns_201_not_500() {
    let state = make_test_state().await;
    let (status, body) = send(
        &state,
        "POST",
        "/apis/apiregistration.k8s.io/v1/apiservices",
        Some(local_apiservice("v1.example.com")),
    )
    .await;
    assert_eq!(
        status, 201,
        "POST /apiservices must reach handler (was 500 — missing AuthContext extension). Body: {}",
        body
    );
    assert_eq!(
        body.pointer("/metadata/name").and_then(|v| v.as_str()),
        Some("v1.example.com")
    );
}

#[tokio::test]
async fn list_apiservices_returns_200_not_500() {
    let state = make_test_state().await;
    // Seed one via POST first so the list is non-empty.
    let (post_status, _) = send(
        &state,
        "POST",
        "/apis/apiregistration.k8s.io/v1/apiservices",
        Some(local_apiservice("v1.list.example.com")),
    )
    .await;
    assert_eq!(post_status, 201, "seed POST must succeed");

    let (status, body) = send(
        &state,
        "GET",
        "/apis/apiregistration.k8s.io/v1/apiservices",
        None,
    )
    .await;
    assert_eq!(status, 200, "GET /apiservices must reach handler (was 500)");
    assert!(
        body.get("items").is_some(),
        "list response must have an `items` field. Body: {}",
        body
    );
}

#[tokio::test]
async fn get_apiservice_by_name_returns_200_not_500() {
    let state = make_test_state().await;
    let (_, _) = send(
        &state,
        "POST",
        "/apis/apiregistration.k8s.io/v1/apiservices",
        Some(local_apiservice("v1.get.example.com")),
    )
    .await;
    let (status, body) = send(
        &state,
        "GET",
        "/apis/apiregistration.k8s.io/v1/apiservices/v1.get.example.com",
        None,
    )
    .await;
    assert_eq!(status, 200, "GET by name must reach handler (was 500)");
    assert_eq!(
        body.get("kind").and_then(|v| v.as_str()),
        Some("APIService")
    );
}

#[tokio::test]
async fn update_apiservice_returns_200_not_500() {
    let state = make_test_state().await;
    send(
        &state,
        "POST",
        "/apis/apiregistration.k8s.io/v1/apiservices",
        Some(local_apiservice("v1.put.example.com")),
    )
    .await;
    let mut updated = local_apiservice("v1.put.example.com");
    updated["spec"]["versionPriority"] = json!(200);
    let (status, body) = send(
        &state,
        "PUT",
        "/apis/apiregistration.k8s.io/v1/apiservices/v1.put.example.com",
        Some(updated),
    )
    .await;
    assert_eq!(
        status, 200,
        "PUT must reach handler (was 500). Body: {}",
        body
    );
    assert_eq!(
        body.pointer("/spec/versionPriority")
            .and_then(|v| v.as_i64()),
        Some(200)
    );
}

#[tokio::test]
async fn delete_apiservice_returns_200_not_500() {
    let state = make_test_state().await;
    send(
        &state,
        "POST",
        "/apis/apiregistration.k8s.io/v1/apiservices",
        Some(local_apiservice("v1.del.example.com")),
    )
    .await;
    let (status, _) = send(
        &state,
        "DELETE",
        "/apis/apiregistration.k8s.io/v1/apiservices/v1.del.example.com",
        None,
    )
    .await;
    assert_eq!(status, 200, "DELETE must reach handler (was 500)");
}

/// The public discovery route at `/apis/apiregistration.k8s.io/v1` does NOT
/// consume AuthContext (handler is parameterless), so it stays in
/// public_routes. Pin that it still serves the resource list without auth.
#[tokio::test]
async fn discovery_route_stays_public_and_unauthenticated() {
    let state = make_test_state().await;
    let (status, body) = send(&state, "GET", "/apis/apiregistration.k8s.io/v1", None).await;
    assert_eq!(status, 200);
    assert!(
        body.get("resources").is_some(),
        "discovery response must list `resources`. Body: {}",
        body
    );
}
