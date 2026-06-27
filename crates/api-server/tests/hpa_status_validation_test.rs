//! HPA /status validates currentReplicas/desiredReplicas non-negative (#1485,
//! upstream ValidateHorizontalPodAutoscalerStatusUpdate).

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

async fn create_hpa(api: &TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": { "name": name, "namespace": "default" },
        "spec": {
            "scaleTargetRef": { "apiVersion": "apps/v1", "kind": "Deployment", "name": "web" },
            "maxReplicas": 10,
            "minReplicas": 1
        }
    });
    let (status, b): (StatusCode, Value) = api
        .send(
            Method::POST.as_str(),
            "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers",
            Some("application/json"),
            Some(&body),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "create hpa: {b}");
}

#[tokio::test]
async fn hpa_status_negative_replicas_rejected() {
    let api = TestApiServer::new();
    create_hpa(&api, "hpa-bad").await;

    let bad = json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": { "name": "hpa-bad", "namespace": "default" },
        "spec": {
            "scaleTargetRef": { "apiVersion": "apps/v1", "kind": "Deployment", "name": "web" },
            "maxReplicas": 10,
            "minReplicas": 1
        },
        "status": { "currentReplicas": -1, "desiredReplicas": 2 }
    });
    let (status, b): (StatusCode, Value) = api
        .send(
            Method::PUT.as_str(),
            "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers/hpa-bad/status",
            Some("application/json"),
            Some(&bad),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "negative currentReplicas must be rejected, got {status}: {b}"
    );
}

#[tokio::test]
async fn hpa_status_valid_replicas_accepted() {
    let api = TestApiServer::new();
    create_hpa(&api, "hpa-ok").await;

    let good = json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": { "name": "hpa-ok", "namespace": "default" },
        "spec": {
            "scaleTargetRef": { "apiVersion": "apps/v1", "kind": "Deployment", "name": "web" },
            "maxReplicas": 10,
            "minReplicas": 1
        },
        "status": { "currentReplicas": 3, "desiredReplicas": 5 }
    });
    let (status, b): (StatusCode, Value) = api
        .send(
            Method::PUT.as_str(),
            "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers/hpa-ok/status",
            Some("application/json"),
            Some(&good),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "valid status update must succeed: {b}"
    );
    assert_eq!(b["status"]["currentReplicas"], json!(3), "{b}");
}
