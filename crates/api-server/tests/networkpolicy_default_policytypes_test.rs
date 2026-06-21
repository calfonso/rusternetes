//! SetDefaults_NetworkPolicy: policyTypes is derived from rules when omitted
//! ([Ingress], plus Egress when egress rules exist), and port protocols
//! default to TCP. K8s ref: pkg/apis/networking/v1/defaults.go.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn uri() -> String {
    format!("/apis/networking.k8s.io/v1/namespaces/{NS}/networkpolicies")
}

fn netpol(name: &str, extra: Value) -> Value {
    let mut spec = json!({"podSelector": {}});
    if let Value::Object(m) = extra {
        for (k, v) in m {
            spec[k] = v;
        }
    }
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": {"name": name}, "spec": spec
    })
}

#[tokio::test]
async fn policy_types_defaults_to_ingress() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&uri(), &netpol("np-i", json!({}))).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["policyTypes"], json!(["Ingress"]), "{body}");
}

#[tokio::test]
async fn policy_types_includes_egress_when_egress_rules_present() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(&uri(), &netpol("np-e", json!({"egress": [{}]})))
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["spec"]["policyTypes"],
        json!(["Ingress", "Egress"]),
        "{body}"
    );
}

#[tokio::test]
async fn explicit_policy_types_preserved() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(
            &uri(),
            &netpol("np-x", json!({"policyTypes": ["Egress"], "egress": [{}]})),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["policyTypes"], json!(["Egress"]), "{body}");
}
