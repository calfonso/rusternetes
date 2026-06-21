//! Update-path validation for NetworkPolicy (`networking.k8s.io/v1`). Upstream
//! `ValidateNetworkPolicyUpdate` re-runs `ValidateNetworkPolicySpec` on the new
//! object; this guards the api-server update handler against persisting an
//! invalid spec via PUT.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn netpols_uri() -> String {
    format!("/apis/networking.k8s.io/v1/namespaces/{NS}/networkpolicies")
}

fn netpol(name: &str, policy_types: Value) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name},
        "spec": {
            "podSelector": {},
            "policyTypes": policy_types
        }
    })
}

#[tokio::test]
async fn networkpolicy_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let name = "allow-ingress";

    let (code, _) = state
        .post(&netpols_uri(), &netpol(name, json!(["Ingress"])))
        .await;
    assert_eq!(code, StatusCode::CREATED, "valid create must succeed");

    let item_uri = format!("{}/{name}", netpols_uri());

    // PUT with an unsupported policyType must be rejected (was persisted
    // unchecked before the update-path validation wiring).
    let (code, _) = state.put(&item_uri, &netpol(name, json!(["Bogus"]))).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid policyType on update must be rejected"
    );

    // A valid update still succeeds.
    let (code, _) = state
        .put(&item_uri, &netpol(name, json!(["Ingress", "Egress"])))
        .await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}
