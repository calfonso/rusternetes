//! TokenRequest.spec.expirationSeconds bounds (upstream ValidateTokenRequest):
//! must be >= 10 minutes (600s) and <= 2^32 seconds.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn sa_uri() -> String {
    format!("/api/v1/namespaces/{NS}/serviceaccounts")
}
fn token_uri(sa: &str) -> String {
    format!("{}/{sa}/token", sa_uri())
}

fn token_req(expiration: i64) -> Value {
    json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest",
        "spec": {"expirationSeconds": expiration}
    })
}

async fn make_sa(state: &TestApiServer, name: &str) {
    let (code, _) = state
        .post(
            &sa_uri(),
            &json!({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": name}}),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "SA create must succeed");
}

#[tokio::test]
async fn token_expiration_below_minimum_rejected() {
    let state = TestApiServer::new();
    make_sa(&state, "sa-a").await;
    let (code, body) = state.post(&token_uri("sa-a"), &token_req(60)).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "expirationSeconds < 600 must be rejected: {body}"
    );
}

#[tokio::test]
async fn token_expiration_valid_accepted() {
    let state = TestApiServer::new();
    make_sa(&state, "sa-b").await;
    let (code, body) = state.post(&token_uri("sa-b"), &token_req(3600)).await;
    assert!(
        code == StatusCode::OK || code == StatusCode::CREATED,
        "valid expirationSeconds must succeed: {code} {body}"
    );
    assert!(
        body["status"]["token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "token must be issued: {body}"
    );
}
