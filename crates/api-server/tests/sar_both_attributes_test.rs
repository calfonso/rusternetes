//! SubjectAccessReview specs may set exactly one of resourceAttributes /
//! nonResourceAttributes (upstream ValidateSubjectAccessReviewSpec rejects
//! both being present). Previously the handler silently dropped
//! nonResourceAttributes when both were set.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn ssar_uri() -> &'static str {
    "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
}

fn ssar(spec: Value) -> Value {
    json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "spec": spec
    })
}

#[tokio::test]
async fn ssar_rejects_both_attribute_kinds() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(
            ssar_uri(),
            &ssar(json!({
                "resourceAttributes": {"verb": "get", "resource": "pods"},
                "nonResourceAttributes": {"verb": "get", "path": "/healthz"}
            })),
        )
        .await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "both resource+nonResource attributes must be rejected: {body}"
    );
}

#[tokio::test]
async fn ssar_accepts_single_attribute_kind() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(
            ssar_uri(),
            &ssar(json!({"resourceAttributes": {"verb": "get", "resource": "pods"}})),
        )
        .await;
    assert!(
        code == StatusCode::OK || code == StatusCode::CREATED,
        "single-attribute SSAR must succeed: {code} {body}"
    );
}
