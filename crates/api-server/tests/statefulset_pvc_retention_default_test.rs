//! SetDefaults_StatefulSet: persistentVolumeClaimRetentionPolicy defaults to
//! {whenDeleted: Retain, whenScaled: Retain}; explicit values preserved.
//! K8s ref: pkg/apis/apps/v1/defaults.go SetDefaults_StatefulSet.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn uri() -> String {
    format!("/apis/apps/v1/namespaces/{NS}/statefulsets")
}

fn sts(name: &str, extra_spec: Value) -> Value {
    let mut spec = json!({
        "serviceName": "svc",
        "selector": {"matchLabels": {"app": "s"}},
        "template": {
            "metadata": {"labels": {"app": "s"}},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]}
        }
    });
    if let Value::Object(m) = extra_spec {
        for (k, v) in m {
            spec[k] = v;
        }
    }
    json!({
        "apiVersion": "apps/v1", "kind": "StatefulSet",
        "metadata": {"name": name}, "spec": spec
    })
}

#[tokio::test]
async fn pvc_retention_policy_defaults_to_retain() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&uri(), &sts("sts-def", json!({}))).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let p = &body["spec"]["persistentVolumeClaimRetentionPolicy"];
    assert_eq!(p["whenDeleted"], json!("Retain"), "{body}");
    assert_eq!(p["whenScaled"], json!("Retain"), "{body}");
}

#[tokio::test]
async fn explicit_retention_preserved() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(&uri(), &sts("sts-exp", json!({
            "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete", "whenScaled": "Retain"}
        })))
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let p = &body["spec"]["persistentVolumeClaimRetentionPolicy"];
    assert_eq!(p["whenDeleted"], json!("Delete"), "{body}");
    assert_eq!(p["whenScaled"], json!("Retain"), "{body}");
}
