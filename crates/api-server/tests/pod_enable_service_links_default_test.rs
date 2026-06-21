//! SetDefaults_Pod: spec.enableServiceLinks defaults to true on standalone Pod
//! create (v1.DefaultEnableServiceLinks), and an explicit value is preserved.
//! K8s ref: pkg/apis/core/v1/defaults.go SetDefaults_Pod.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn pods_uri() -> String {
    format!("/api/v1/namespaces/{NS}/pods")
}

fn pod(name: &str, enable_service_links: Option<bool>) -> Value {
    let mut spec = json!({"containers": [{"name": "c", "image": "nginx"}]});
    if let Some(v) = enable_service_links {
        spec["enableServiceLinks"] = json!(v);
    }
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name},
        "spec": spec
    })
}

#[tokio::test]
async fn enable_service_links_defaults_to_true() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&pods_uri(), &pod("p-default", None)).await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");
    assert_eq!(
        body["spec"]["enableServiceLinks"],
        json!(true),
        "enableServiceLinks must default to true: {body}"
    );
}

#[tokio::test]
async fn explicit_enable_service_links_false_preserved() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&pods_uri(), &pod("p-false", Some(false))).await;
    assert_eq!(code, StatusCode::CREATED);
    assert_eq!(
        body["spec"]["enableServiceLinks"],
        json!(false),
        "explicit false must be preserved: {body}"
    );
}
