//! Reproducer test for the kubectl-style flow:
//!   1. Create a namespace via REST.
//!   2. Create a pod in that namespace via REST.
//!   3. List pods in that namespace via the same URL kubectl hits
//!      (`/api/v1/namespaces/{ns}/pods`) and assert the pod appears.
//!   4. List pods in an unrelated namespace and assert it does NOT appear
//!      (cross-namespace isolation).
//!
//! The kubectl `get pods -n <ns>` code path is a thin wrapper around
//! `GET /api/v1/namespaces/{ns}/pods`, then deserializing the
//! `KubernetesList<Pod>` envelope (see crates/kubectl/src/commands/get.rs
//! line 683 and crates/kubectl/src/client.rs `get_list`). Exercising the
//! REST surface here reproduces any server-side bug kubectl would surface.

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

// Thin shim over the shared harness, preserving this file's (Method, Option<&Value>)
// call sites. `TestApiServer` boots build_router on MemoryStorage with --skip-auth.
async fn send(
    api: &TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let content_type = body.map(|_| "application/json");
    api.send(method.as_str(), uri, content_type, body).await
}

fn pod_body(name: &str, namespace: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "registry.k8s.io/pause:3.10",
            }],
        },
    })
}

#[tokio::test]
async fn kubectl_get_pods_in_created_namespace_returns_pod() {
    let router = spawn_router();
    let ns = "team-a";
    let pod_name = "nginx-1";

    let (status, body) = send(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": ns },
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "namespace create should return 201, got {status}: {body}"
    );

    let (status, body) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&pod_body(pod_name, ns)),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod create should return 201, got {status}: {body}"
    );

    let (status, body) = send(
        &router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/pods"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list should return 200: {body}");

    assert_eq!(
        body["kind"].as_str(),
        Some("PodList"),
        "list response should have kind=PodList: {body}"
    );

    let items = body["items"].as_array().expect("items must be an array");
    let names: Vec<&str> = items
        .iter()
        .filter_map(|p| p["metadata"]["name"].as_str())
        .collect();
    assert!(
        names.contains(&pod_name),
        "expected pod {pod_name} in namespace {ns} list, got {names:?}",
    );

    let item_ns = items[0]["metadata"]["namespace"].as_str();
    assert_eq!(
        item_ns,
        Some(ns),
        "listed pod's metadata.namespace should match the request namespace, got {item_ns:?}",
    );
}

#[tokio::test]
async fn kubectl_get_pods_does_not_leak_across_namespaces() {
    let router = spawn_router();

    for ns in ["team-a", "team-b"] {
        let (status, _) = send(
            &router,
            Method::POST,
            "/api/v1/namespaces",
            Some(&json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": { "name": ns },
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, _) = send(
        &router,
        Method::POST,
        "/api/v1/namespaces/team-a/pods",
        Some(&pod_body("only-in-a", "team-a")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = send(&router, Method::GET, "/api/v1/namespaces/team-b/pods", None).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().expect("items must be an array");
    assert!(
        items.is_empty(),
        "listing pods in team-b must not leak pods from team-a, got {items:?}",
    );
}
