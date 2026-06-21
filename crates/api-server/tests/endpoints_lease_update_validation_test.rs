//! Update-path validation for Endpoints (core/v1) and Lease
//! (coordination.k8s.io/v1). Both upstream update validators re-run the spec
//! validator on the new object; the api-server update handlers previously
//! persisted PUTs unchecked.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

// ---- Endpoints ----------------------------------------------------------

fn ep_uri() -> String {
    format!("/api/v1/namespaces/{NS}/endpoints")
}
fn endpoints(name: &str, ip: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Endpoints",
        "metadata": {"name": name},
        "subsets": [{
            "addresses": [{"ip": ip}],
            "ports": [{"port": 80}]
        }]
    })
}

#[tokio::test]
async fn endpoints_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let name = "ep1";
    let (code, _) = state.post(&ep_uri(), &endpoints(name, "10.0.0.1")).await;
    assert_eq!(code, StatusCode::CREATED);
    let item = format!("{}/{name}", ep_uri());

    let (code, _) = state.put(&item, &endpoints(name, "not-an-ip")).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid IP on update must be rejected"
    );

    let (code, _) = state.put(&item, &endpoints(name, "10.0.0.2")).await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}

// ---- Lease --------------------------------------------------------------

fn lease_uri() -> String {
    format!("/apis/coordination.k8s.io/v1/namespaces/{NS}/leases")
}
fn lease(name: &str, duration: Option<i64>) -> Value {
    let mut spec = json!({"holderIdentity": "holder-a"});
    if let Some(d) = duration {
        spec["leaseDurationSeconds"] = json!(d);
    }
    json!({
        "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
        "metadata": {"name": name}, "spec": spec
    })
}

#[tokio::test]
async fn lease_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let name = "lease1";
    let (code, _) = state.post(&lease_uri(), &lease(name, Some(30))).await;
    assert_eq!(code, StatusCode::CREATED);
    let item = format!("{}/{name}", lease_uri());

    // leaseDurationSeconds must be > 0.
    let (code, _) = state.put(&item, &lease(name, Some(0))).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid leaseDurationSeconds must be rejected"
    );

    let (code, _) = state.put(&item, &lease(name, Some(60))).await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}
