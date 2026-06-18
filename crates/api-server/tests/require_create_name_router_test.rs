//! Router-level pin for GitHub #1065: a persisted create that supplies neither
//! `metadata.name` nor `metadata.generateName` must be rejected with HTTP 422
//! and the upstream message `name or generateName is required`, instead of being
//! stored at a malformed `/registry/<type>/<ns>/` key.
//!
//! After #1063 `ObjectMeta::ensure_name` no longer fabricates an `auto-<id>`
//! name, and `generate_name_middleware` only synthesises a name when a non-empty
//! `generateName` is present — so an empty name at handler time means the client
//! sent neither field. Each persisted create handler now calls
//! `require_object_name`.
//!
//! The non-persisted review POSTs (SubjectAccessReview/TokenReview/…) legitimately
//! carry no name and must be unaffected.
//!
//! Harness mirrors `generate_name_router_test.rs`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// Harness: `TestApiServer` (rusternetes-test-support) — `build_router` on
// `MemoryStorage` with `--skip-auth`, driven via `tower::oneshot`.

/// Assert a create response is the upstream "name or generateName is required"
/// 422.
fn assert_name_required(status: StatusCode, body: &Value, ctx: &str) {
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{ctx}: expected 422, got {status}: {body}"
    );
    assert_eq!(
        body["reason"].as_str(),
        Some("Invalid"),
        "{ctx}: expected reason=Invalid, got {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("name or generateName is required"),
        "{ctx}: expected upstream message, got {msg:?}"
    );
}

// --- Namespaced kinds ------------------------------------------------------

#[tokio::test]
async fn configmap_create_without_name_is_rejected() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {},
        "data": {"key": "value"},
    });
    let (status, body) = state
        .post("/api/v1/namespaces/default/configmaps", &body)
        .await;
    assert_name_required(status, &body, "ConfigMap");
}

#[tokio::test]
async fn pod_create_without_name_is_rejected() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {},
        "spec": {"containers": [{"name": "c", "image": "nginx:latest"}]},
    });
    let (status, body) = state.post("/api/v1/namespaces/default/pods", &body).await;
    assert_name_required(status, &body, "Pod");
}

#[tokio::test]
async fn deployment_create_without_name_is_rejected() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {},
        "spec": {
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{"name": "c", "image": "nginx:latest"}]},
            },
        },
    });
    let (status, body) = state
        .post("/apis/apps/v1/namespaces/default/deployments", &body)
        .await;
    assert_name_required(status, &body, "Deployment");
}

// --- Cluster-scoped kinds --------------------------------------------------

#[tokio::test]
async fn namespace_create_without_name_is_rejected() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {},
    });
    let (status, body) = state.post("/api/v1/namespaces", &body).await;
    assert_name_required(status, &body, "Namespace");
}

#[tokio::test]
async fn priorityclass_create_without_name_is_rejected() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "scheduling.k8s.io/v1",
        "kind": "PriorityClass",
        "metadata": {},
        "value": 100,
    });
    let (status, body) = state
        .post("/apis/scheduling.k8s.io/v1/priorityclasses", &body)
        .await;
    assert_name_required(status, &body, "PriorityClass");
}

// --- Empty generateName behaves like no name -------------------------------

#[tokio::test]
async fn empty_generate_name_is_rejected() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"generateName": ""},
        "data": {"key": "value"},
    });
    let (status, body) = state
        .post("/api/v1/namespaces/default/configmaps", &body)
        .await;
    assert_name_required(status, &body, "ConfigMap empty generateName");
}

// --- A named create still succeeds (no over-rejection) ---------------------

#[tokio::test]
async fn named_create_still_succeeds() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "named-cm"},
        "data": {"key": "value"},
    });
    let (status, created) = state
        .post("/api/v1/namespaces/default/configmaps", &body)
        .await;
    assert!(
        status.is_success(),
        "named create must succeed, got {status}: {created}"
    );
}

// --- Non-persisted reviews are unaffected ----------------------------------

#[tokio::test]
async fn subject_access_review_without_name_is_not_rejected() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "spec": {
            "resourceAttributes": {"namespace": "default", "verb": "get", "resource": "pods"},
            "user": "alice",
        },
    });
    let (status, body) = state
        .post("/apis/authorization.k8s.io/v1/subjectaccessreviews", &body)
        .await;
    assert_ne!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "SubjectAccessReview has no name and must NOT be name-rejected, got {status}: {body}"
    );
    assert!(
        status.is_success(),
        "SubjectAccessReview should succeed, got {status}: {body}"
    );
}

#[tokio::test]
async fn token_review_without_name_is_not_rejected() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": {"token": "abc"},
    });
    let (status, body) = state
        .post("/apis/authentication.k8s.io/v1/tokenreviews", &body)
        .await;
    assert_ne!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "TokenReview has no name and must NOT be name-rejected, got {status}: {body}"
    );
}
