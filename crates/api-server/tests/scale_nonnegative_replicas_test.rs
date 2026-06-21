//! Scale subresource: spec.replicas must be >= 0 (upstream ValidateScale).
//! The /scale PUT and PATCH handlers previously persisted negative replicas.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn deployments_uri() -> String {
    format!("/apis/apps/v1/namespaces/{NS}/deployments")
}
fn scale_uri(name: &str) -> String {
    format!("{}/{name}/scale", deployments_uri())
}

fn deployment(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

fn scale(replicas: i64) -> Value {
    json!({
        "apiVersion": "autoscaling/v1", "kind": "Scale",
        "metadata": {"name": "x", "namespace": NS},
        "spec": {"replicas": replicas},
        "status": {"replicas": 0}
    })
}

#[tokio::test]
async fn scale_rejects_negative_replicas() {
    let state = TestApiServer::new();
    let name = "web";
    let (code, _) = state.post(&deployments_uri(), &deployment(name)).await;
    assert_eq!(code, StatusCode::CREATED, "deployment create must succeed");

    // PUT /scale with negative replicas must be rejected.
    let (code, body) = state.put(&scale_uri(name), &scale(-1)).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "negative scale replicas must be rejected: got {code} {body}"
    );

    // A non-negative scale succeeds.
    let (code, body) = state.put(&scale_uri(name), &scale(3)).await;
    assert_eq!(code, StatusCode::OK, "valid scale must succeed: {body}");
}
