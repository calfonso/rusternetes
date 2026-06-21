//! Update-path validation for LimitRange (core/v1) and PodDisruptionBudget
//! (policy/v1). Both upstream update strategies re-run the create-time
//! validator on the new object; these guard the api-server update handlers
//! against persisting an invalid spec via PUT.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

// ---- LimitRange ----------------------------------------------------------

fn limitranges_uri() -> String {
    format!("/api/v1/namespaces/{NS}/limitranges")
}

fn limitrange(name: &str, types: &[&str]) -> Value {
    let limits: Vec<Value> = types
        .iter()
        .map(|t| json!({"type": t, "max": {"cpu": "2"}, "min": {"cpu": "100m"}}))
        .collect();
    json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": name},
        "spec": {"limits": limits}
    })
}

#[tokio::test]
async fn limitrange_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let name = "lr";

    let (code, _) = state
        .post(&limitranges_uri(), &limitrange(name, &["Container"]))
        .await;
    assert_eq!(code, StatusCode::CREATED, "valid create must succeed");

    let item_uri = format!("{}/{name}", limitranges_uri());

    // Duplicate limit type is invalid (upstream ValidateLimitRange).
    let (code, _) = state
        .put(&item_uri, &limitrange(name, &["Container", "Container"]))
        .await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "duplicate limit type on update must be rejected"
    );

    let (code, _) = state.put(&item_uri, &limitrange(name, &["Pod"])).await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}

// ---- PodDisruptionBudget -------------------------------------------------

fn pdbs_uri() -> String {
    format!("/apis/policy/v1/namespaces/{NS}/poddisruptionbudgets")
}

fn pdb(name: &str, extra: Value) -> Value {
    let mut spec = json!({
        "minAvailable": 1,
        "selector": {"matchLabels": {"app": "web"}}
    });
    if let Value::Object(extra) = extra {
        for (k, v) in extra {
            spec[k] = v;
        }
    }
    json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {"name": name},
        "spec": spec
    })
}

#[tokio::test]
async fn pdb_update_rejects_invalid_spec() {
    let state = TestApiServer::new();
    let name = "web-pdb";

    let (code, _) = state.post(&pdbs_uri(), &pdb(name, json!({}))).await;
    assert_eq!(code, StatusCode::CREATED, "valid create must succeed");

    let item_uri = format!("{}/{name}", pdbs_uri());

    // minAvailable + maxUnavailable are mutually exclusive (upstream).
    let (code, _) = state
        .put(&item_uri, &pdb(name, json!({"maxUnavailable": 1})))
        .await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "minAvailable + maxUnavailable on update must be rejected"
    );

    // A valid update (just minAvailable) still succeeds.
    let (code, _) = state
        .put(&item_uri, &pdb(name, json!({"minAvailable": 2})))
        .await;
    assert_eq!(code, StatusCode::OK, "valid update must succeed");
}
