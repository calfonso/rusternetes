//! Update-path validation for Ingress (`networking.k8s.io/v1`). Upstream
//! `ValidateIngressUpdate` re-runs full spec validation on the new object;
//! this guards the api-server update handler (handlers/ingress.rs) against
//! persisting an invalid spec via PUT.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn ingresses_uri() -> String {
    format!("/apis/networking.k8s.io/v1/namespaces/{NS}/ingresses")
}

fn valid_ingress(name: &str, path_type: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {"name": name},
        "spec": {
            "rules": [{
                "host": "example.com",
                "http": {
                    "paths": [{
                        "path": "/",
                        "pathType": path_type,
                        "backend": {"service": {"name": "web", "port": {"number": 80}}}
                    }]
                }
            }]
        }
    })
}

#[tokio::test]
async fn ingress_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let name = "site";

    // Create a valid Ingress.
    let (code, _) = state
        .post(&ingresses_uri(), &valid_ingress(name, "Prefix"))
        .await;
    assert_eq!(code, StatusCode::CREATED, "valid create must succeed");

    let item_uri = format!("{}/{name}", ingresses_uri());

    // PUT with an unsupported pathType must be rejected (was silently persisted
    // before the update-path validation wiring).
    let bad = valid_ingress(name, "Nonsense");
    let (code, _) = state.put(&item_uri, &bad).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid pathType on update must be rejected"
    );

    // A valid update still succeeds.
    let good = valid_ingress(name, "Exact");
    let (code, _) = state.put(&item_uri, &good).await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}
