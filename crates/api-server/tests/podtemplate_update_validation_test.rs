//! Update-path validation for PodTemplate (core/v1). Upstream
//! ValidatePodTemplateUpdate re-runs ValidatePodTemplateSpec on the new
//! object; this guards the api-server update handler against persisting an
//! invalid template via PUT.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn uri() -> String {
    format!("/api/v1/namespaces/{NS}/podtemplates")
}

fn podtemplate(name: &str, containers: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "PodTemplate",
        "metadata": {"name": name},
        "template": {
            "metadata": {"labels": {"app": "web"}},
            "spec": {"containers": containers}
        }
    })
}

#[tokio::test]
async fn podtemplate_update_rejects_invalid_template() {
    let state = TestApiServer::new();
    let name = "pt";
    let good = json!([{"name": "c", "image": "nginx"}]);

    let (code, _) = state.post(&uri(), &podtemplate(name, good.clone())).await;
    assert_eq!(code, StatusCode::CREATED, "valid create must succeed");

    let item = format!("{}/{name}", uri());

    // A container with an empty name is an invalid template spec.
    let bad = json!([{"name": "", "image": "nginx"}]);
    let (code, _) = state.put(&item, &podtemplate(name, bad)).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid template on update must be rejected"
    );

    // A valid update still succeeds.
    let (code, _) = state.put(&item, &podtemplate(name, good)).await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}
