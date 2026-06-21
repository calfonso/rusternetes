//! Eviction deleteOptions.gracePeriodSeconds must be non-negative (upstream
//! ValidateDeleteOptions). Previously a negative value was accepted.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn pods_uri() -> String {
    format!("/api/v1/namespaces/{NS}/pods")
}
fn eviction_uri(pod: &str) -> String {
    format!("{}/{pod}/eviction", pods_uri())
}

fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    })
}

fn eviction(pod: &str, grace: i64) -> Value {
    json!({
        "apiVersion": "policy/v1", "kind": "Eviction",
        "metadata": {"name": pod, "namespace": NS},
        "deleteOptions": {"gracePeriodSeconds": grace}
    })
}

#[tokio::test]
async fn eviction_rejects_negative_grace_period() {
    let state = TestApiServer::new();
    let name = "p-evict";
    let (code, _) = state.post(&pods_uri(), &pod(name)).await;
    assert_eq!(code, StatusCode::CREATED, "pod create must succeed");

    let (code, body) = state.post(&eviction_uri(name), &eviction(name, -5)).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "negative gracePeriodSeconds must be rejected: {body}"
    );
}

#[tokio::test]
async fn eviction_accepts_nonnegative_grace_period() {
    let state = TestApiServer::new();
    let name = "p-evict-ok";
    let (code, _) = state.post(&pods_uri(), &pod(name)).await;
    assert_eq!(code, StatusCode::CREATED);

    // No PDB → eviction is allowed; a valid gracePeriod must not be rejected.
    let (code, body) = state.post(&eviction_uri(name), &eviction(name, 30)).await;
    assert!(
        code == StatusCode::OK || code == StatusCode::CREATED,
        "valid eviction must succeed: {code} {body}"
    );
}
