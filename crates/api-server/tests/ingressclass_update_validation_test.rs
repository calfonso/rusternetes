//! IngressClass update: spec.controller is immutable (upstream
//! ValidateIngressClassUpdate). The update handler previously persisted PUTs
//! unchecked.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn uri() -> &'static str {
    "/apis/networking.k8s.io/v1/ingressclasses"
}

fn ingressclass(name: &str, controller: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "IngressClass",
        "metadata": {"name": name},
        "spec": {"controller": controller}
    })
}

#[tokio::test]
async fn ingressclass_controller_immutable() {
    let state = TestApiServer::new();
    let name = "ic1";
    let (code, _) = state
        .post(uri(), &ingressclass(name, "example.com/ingress"))
        .await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed");
    let item = format!("{}/{name}", uri());

    // Changing controller is rejected.
    let (code, body) = state
        .put(&item, &ingressclass(name, "other.com/ingress"))
        .await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "controller change must be rejected: {body}"
    );

    // Unchanged controller update succeeds.
    let (code, _) = state
        .put(&item, &ingressclass(name, "example.com/ingress"))
        .await;
    assert_eq!(
        code,
        StatusCode::OK,
        "unchanged-controller update must succeed"
    );
}
