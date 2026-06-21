//! SetDefaults_HorizontalPodAutoscaler: spec.minReplicas defaults to 1 when
//! unset; an explicit value is preserved.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn uri() -> String {
    format!("/apis/autoscaling/v1/namespaces/{NS}/horizontalpodautoscalers")
}

fn hpa(name: &str, min: Option<i64>) -> Value {
    let mut spec = json!({
        "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
        "maxReplicas": 5
    });
    if let Some(m) = min {
        spec["minReplicas"] = json!(m);
    }
    json!({
        "apiVersion": "autoscaling/v1", "kind": "HorizontalPodAutoscaler",
        "metadata": {"name": name}, "spec": spec
    })
}

#[tokio::test]
async fn min_replicas_defaults_to_one() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&uri(), &hpa("hpa-def", None)).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["minReplicas"], json!(1), "{body}");
}

#[tokio::test]
async fn explicit_min_replicas_preserved() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&uri(), &hpa("hpa-3", Some(3))).await;
    assert_eq!(code, StatusCode::CREATED);
    assert_eq!(body["spec"]["minReplicas"], json!(3), "{body}");
}
