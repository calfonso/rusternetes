//! K8s defaults ReplicationController spec.replicas to 1 when unset (the
//! declarative default on the core/v1 type). Explicit values preserved.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn uri() -> String {
    format!("/api/v1/namespaces/{NS}/replicationcontrollers")
}

fn rc(name: &str, replicas: Option<i64>) -> Value {
    let mut spec = json!({
        "selector": {"app": "rc"},
        "template": {
            "metadata": {"labels": {"app": "rc"}},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]}
        }
    });
    if let Some(r) = replicas {
        spec["replicas"] = json!(r);
    }
    json!({
        "apiVersion": "v1", "kind": "ReplicationController",
        "metadata": {"name": name}, "spec": spec
    })
}

#[tokio::test]
async fn replicas_defaults_to_one() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&uri(), &rc("rc-def", None)).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["replicas"], json!(1), "{body}");
}

#[tokio::test]
async fn explicit_replicas_preserved() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&uri(), &rc("rc-3", Some(3))).await;
    assert_eq!(code, StatusCode::CREATED);
    assert_eq!(body["spec"]["replicas"], json!(3), "{body}");
}
