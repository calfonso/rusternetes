//! Full-spec validation on ReplicaSet / DaemonSet update. Upstream
//! ValidateReplicaSetUpdate / ValidateDaemonSetUpdate re-run the spec validator
//! on the new object; the api-server update handlers previously checked only
//! selector immutability, letting an otherwise-invalid spec through on PUT.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn rs(name: &str, template_labels: Value) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": name},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": template_labels},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

fn ds(name: &str, template_labels: Value) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": name},
        "spec": {
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": template_labels},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

#[tokio::test]
async fn replicaset_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/replicasets");
    let name = "web-rs";

    let (code, _) = state.post(&uri, &rs(name, json!({"app": "web"}))).await;
    assert_eq!(code, StatusCode::CREATED, "valid create must succeed");

    let item = format!("{uri}/{name}");
    // Template labels no longer match the (immutable) selector -> invalid spec.
    let (code, _) = state.put(&item, &rs(name, json!({"app": "other"}))).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "template-label/selector mismatch on update must be rejected"
    );

    // A valid update (still matching) succeeds.
    let (code, _) = state
        .put(&item, &rs(name, json!({"app": "web", "tier": "fe"})))
        .await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}

#[tokio::test]
async fn daemonset_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/daemonsets");
    let name = "web-ds";

    let (code, _) = state.post(&uri, &ds(name, json!({"app": "web"}))).await;
    assert_eq!(code, StatusCode::CREATED, "valid create must succeed");

    let item = format!("{uri}/{name}");
    let (code, _) = state.put(&item, &ds(name, json!({"app": "other"}))).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "template-label/selector mismatch on update must be rejected"
    );

    let (code, _) = state
        .put(&item, &ds(name, json!({"app": "web", "tier": "fe"})))
        .await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}
