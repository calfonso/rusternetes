//! SetDefaults_Job: manualSelector defaults to false; backoffLimit defaults to
//! 6, or MaxInt32 when backoffLimitPerIndex is set. K8s ref:
//! pkg/apis/batch/v1/defaults.go SetDefaults_Job.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn jobs_uri() -> String {
    format!("/apis/batch/v1/namespaces/{NS}/jobs")
}

fn job(name: &str, extra_spec: Value) -> Value {
    let mut spec = json!({
        "template": {
            "metadata": {"labels": {"app": "j"}},
            "spec": {
                "restartPolicy": "Never",
                "containers": [{"name": "c", "image": "busybox"}]
            }
        }
    });
    if let Value::Object(m) = extra_spec {
        for (k, v) in m {
            spec[k] = v;
        }
    }
    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name},
        "spec": spec
    })
}

#[tokio::test]
async fn manual_selector_defaults_to_false() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&jobs_uri(), &job("j-ms", json!({}))).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["manualSelector"], json!(false), "{body}");
}

#[tokio::test]
async fn backoff_limit_defaults_to_six() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&jobs_uri(), &job("j-bl", json!({}))).await;
    assert_eq!(code, StatusCode::CREATED);
    assert_eq!(body["spec"]["backoffLimit"], json!(6), "{body}");
}

#[tokio::test]
async fn backoff_limit_maxint_when_per_index_set() {
    let state = TestApiServer::new();
    // Indexed job with per-index backoff limit; overall backoffLimit omitted.
    let (code, body) = state
        .post(
            &jobs_uri(),
            &job(
                "j-idx",
                json!({"completionMode": "Indexed", "completions": 3, "backoffLimitPerIndex": 2}),
            ),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["backoffLimit"], json!(i32::MAX), "{body}");
}
