//! Update-path validation for HorizontalPodAutoscaler (autoscaling/v1).
//! Upstream ValidateHorizontalPodAutoscalerUpdate re-runs the spec validator
//! on the new object; the api-server update handler previously persisted PUTs
//! unchecked.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn uri() -> String {
    format!("/apis/autoscaling/v1/namespaces/{NS}/horizontalpodautoscalers")
}

fn hpa(name: &str, max_replicas: i64) -> Value {
    json!({
        "apiVersion": "autoscaling/v1",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {"name": name},
        "spec": {
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
            "minReplicas": 1,
            "maxReplicas": max_replicas
        }
    })
}

#[tokio::test]
async fn hpa_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let name = "web-hpa";

    let (code, _) = state.post(&uri(), &hpa(name, 5)).await;
    assert_eq!(code, StatusCode::CREATED, "valid create must succeed");

    let item = format!("{}/{name}", uri());

    // maxReplicas must be > 0.
    let (code, _) = state.put(&item, &hpa(name, 0)).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid maxReplicas on update must be rejected"
    );

    let (code, _) = state.put(&item, &hpa(name, 10)).await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}
