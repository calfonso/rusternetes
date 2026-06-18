//! Strict JSON decoder tests for `?fieldValidation=<mode>`.
//!
//! Mirrors upstream Kubernetes apimachinery serializer/json strict-decoding
//! tests. The wire contract is:
//!
//! - `fieldValidation=Strict`  → reject unknown / duplicate JSON fields with
//!   an HTTP error whose body is a `Status` object identifying the offending
//!   field paths. Upstream serializes as `strict decoding error: ...`.
//! - `fieldValidation=Warn`    → accept the request, drop unknown fields on
//!   read-back, and emit one `Warning:` header per unknown field.
//! - `fieldValidation=Ignore`  → accept silently; unknown fields are dropped
//!   on read-back with no header.
//! - No param                  → in Kubernetes >= 1.25 the server-side
//!   default is `Strict`. Rusternetes currently defaults to `Ignore`
//!   (handler short-circuits when the query param is absent), so the
//!   strict-default test is `#[ignore]` pending parity work.
//!
//! Pattern follows `tests/integration_dryrun_all_resources.rs` — spawn an
//! in-process Axum router backed by `MemoryStorage`, drive it with
//! `tower::ServiceExt::oneshot`, and inspect the response.
//!
//! Tests are named `test_field_validation_<mode>_<scenario>` per the unit
//! brief.

use axum::http::{Method, StatusCode};
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `send_raw` returns
// the flattened response headers so the tests can assert on strict-decoding
// `Warning:` headers; `send_full` exposes the response `HeaderMap`.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "fieldvalidation";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// Send a raw JSON byte body so we can exercise duplicate keys that would
/// otherwise be normalized away by `serde_json::to_vec`.
async fn send_raw(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: Vec<u8>,
) -> (StatusCode, Vec<(String, String)>, Value) {
    let (status, header_map, _bytes, value) = router
        .send_full(
            method.as_str(),
            uri,
            Some("application/json"),
            None,
            Some(body),
        )
        .await;
    let headers: Vec<(String, String)> = header_map
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_string(),
                v.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    (status, headers, value)
}

async fn send_json(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: &Value,
) -> (StatusCode, Vec<(String, String)>, Value) {
    let bytes = serde_json::to_vec(body).unwrap();
    send_raw(router, method, uri, bytes).await
}

fn warning_headers(headers: &[(String, String)]) -> Vec<&str> {
    headers
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("warning"))
        .map(|(_, v)| v.as_str())
        .collect()
}

// ---------------------------------------------------------------------------
// Resource stubs — minimal valid bodies for each representative GVR.
// ---------------------------------------------------------------------------

fn pod_uri() -> String {
    format!("/api/v1/namespaces/{}/pods", TEST_NS)
}

fn pod_stub(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {"containers": [{"name": "c1", "image": "busybox"}]}
    })
}

fn deployment_uri() -> String {
    format!("/apis/apps/v1/namespaces/{}/deployments", TEST_NS)
}

fn deployment_stub(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{"image": "busybox", "name": "c"}]}
            }
        }
    })
}

fn service_uri() -> String {
    format!("/api/v1/namespaces/{}/services", TEST_NS)
}

fn service_stub(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [{"port": 80, "targetPort": 8080}],
            "selector": {"app": "x"}
        }
    })
}

// ---------------------------------------------------------------------------
// Strict mode — unknown fields
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_field_validation_strict_pod_unknown_field_rejected() {
    let (_mem, router) = spawn_router();
    let mut stub = pod_stub("pod-strict-unknown");
    stub["spec"]["bogusField"] = json!("nope");
    let uri = format!("{}?fieldValidation=Strict", pod_uri());

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    // Rusternetes maps strict-decoding errors through `Error::InvalidResource`,
    // which renders as HTTP 422 with a Kubernetes `Status` body whose reason
    // is `Invalid` (see crates/common/src/error.rs:105). Upstream uses 400 /
    // `BadRequest`; the parity gap is tracked by the ignored test below.
    assert!(
        status.is_client_error(),
        "expected 4xx for strict unknown field, got {}: body={}",
        status,
        body
    );
    assert_eq!(body["kind"], "Status", "body should be a Status object");
    assert_eq!(body["status"], "Failure");
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("strict decoding error"),
        "message should mention strict decoding error: {}",
        message
    );
    assert!(
        message.contains("bogusField"),
        "message should identify the offending field: {}",
        message
    );
}

#[tokio::test]
async fn test_field_validation_strict_deployment_unknown_field_rejected() {
    let (_mem, router) = spawn_router();
    let mut stub = deployment_stub("dep-strict-unknown");
    stub["spec"]["weirdoFlag"] = json!(true);
    let uri = format!("{}?fieldValidation=Strict", deployment_uri());

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(
        status.is_client_error(),
        "expected 4xx for strict unknown field on Deployment, got {}: body={}",
        status,
        body
    );
    assert_eq!(body["kind"], "Status");
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("strict decoding error") && message.contains("weirdoFlag"),
        "message should call out unknown field 'weirdoFlag': {}",
        message
    );
}

#[tokio::test]
async fn test_field_validation_strict_service_unknown_field_rejected() {
    let (_mem, router) = spawn_router();
    let mut stub = service_stub("svc-strict-unknown");
    stub["spec"]["unsupportedThing"] = json!(42);
    let uri = format!("{}?fieldValidation=Strict", service_uri());

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(
        status.is_client_error(),
        "expected 4xx for strict unknown field on Service, got {}: body={}",
        status,
        body
    );
    assert_eq!(body["kind"], "Status");
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("strict decoding error") && message.contains("unsupportedThing"),
        "message should call out the unknown field: {}",
        message
    );
}

#[tokio::test]
async fn test_field_validation_strict_pod_duplicate_field_rejected() {
    let (_mem, router) = spawn_router();
    // Pod with duplicate `metadata.namespace` keys. We assemble the JSON by
    // hand because `serde_json::Value` deduplicates keys at parse time.
    let body = format!(
        r#"{{
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {{"name": "pod-strict-dup", "namespace": "{ns}", "namespace": "{ns}"}},
            "spec": {{"containers": [{{"name": "c1", "image": "busybox"}}]}}
        }}"#,
        ns = TEST_NS
    );
    let uri = format!("{}?fieldValidation=Strict", pod_uri());

    let (status, _hdrs, resp) = send_raw(router, Method::POST, &uri, body.into_bytes()).await;

    assert!(
        status.is_client_error(),
        "expected 4xx for strict duplicate field, got {}: body={}",
        status,
        resp
    );
    assert_eq!(resp["kind"], "Status");
    let message = resp["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("duplicate field") || message.contains("strict decoding error"),
        "message should mention duplicate field: {}",
        message
    );
    assert!(
        message.contains("namespace"),
        "message should identify the duplicated field 'namespace': {}",
        message
    );
}

#[tokio::test]
async fn test_field_validation_strict_deployment_duplicate_field_rejected() {
    let (_mem, router) = spawn_router();
    // Duplicate `spec.replicas` keys — known parity case from
    // crates/api-server/src/handlers/deployment.rs:36.
    let body = format!(
        r#"{{
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {{"name": "dep-strict-dup", "namespace": "{ns}"}},
            "spec": {{
                "replicas": 1,
                "replicas": 2,
                "selector": {{"matchLabels": {{"app": "x"}}}},
                "template": {{
                    "metadata": {{"labels": {{"app": "x"}}}},
                    "spec": {{"containers": [{{"image": "busybox", "name": "c"}}]}}
                }}
            }}
        }}"#,
        ns = TEST_NS
    );
    let uri = format!("{}?fieldValidation=Strict", deployment_uri());

    let (status, _hdrs, resp) = send_raw(router, Method::POST, &uri, body.into_bytes()).await;

    assert!(
        status.is_client_error(),
        "expected 4xx for strict duplicate spec.replicas, got {}: body={}",
        status,
        resp
    );
    assert_eq!(resp["kind"], "Status");
    let message = resp["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("duplicate field"),
        "message should mention duplicate field: {}",
        message
    );
    assert!(
        message.contains("spec.replicas") || message.contains("replicas"),
        "message should identify duplicated 'spec.replicas' path: {}",
        message
    );
}

#[tokio::test]
async fn test_field_validation_strict_status_body_reason_invalid() {
    // After unit #7 fixed the strict-decode status mapping, rusternetes now
    // matches upstream: HTTP 400 with reason=BadRequest. Test name preserved
    // for blame history; behaviour asserts the post-fix contract.
    let (_mem, router) = spawn_router();
    let mut stub = pod_stub("pod-status-shape");
    stub["spec"]["bogus"] = json!("x");
    let uri = format!("{}?fieldValidation=Strict", pod_uri());

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "strict decode failures must map to HTTP 400 per upstream parity"
    );
    assert_eq!(body["kind"], "Status");
    assert_eq!(body["apiVersion"], "v1");
    assert_eq!(body["status"], "Failure");
    assert_eq!(body["reason"], "BadRequest");
    assert_eq!(body["code"], 400);
}

#[tokio::test]
async fn test_field_validation_strict_returns_400_badrequest_upstream_parity() {
    let (_mem, router) = spawn_router();
    let mut stub = pod_stub("pod-upstream-parity");
    stub["spec"]["bogus"] = json!("x");
    let uri = format!("{}?fieldValidation=Strict", pod_uri());

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["reason"], "BadRequest");
    assert_eq!(body["code"], 400);
}

// ---------------------------------------------------------------------------
// Warn mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_field_validation_warn_pod_unknown_field_accepted() {
    // Warn mode must accept the request and persist the resource.
    let (_mem, router) = spawn_router();
    let mut stub = pod_stub("pod-warn-unknown");
    stub["spec"]["bogusField"] = json!("ignored");
    let uri = format!("{}?fieldValidation=Warn", pod_uri());

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(
        status.is_success(),
        "Warn mode must accept unknown fields; got {}: body={}",
        status,
        body
    );
    assert_eq!(body["kind"], "Pod");
    assert_eq!(body["metadata"]["name"], "pod-warn-unknown");
    // Unknown field is dropped on read-back — strongly typed Pod has no slot
    // for `bogusField`, so it must not round-trip.
    assert!(
        body["spec"].get("bogusField").is_none(),
        "unknown field must be dropped from the response body: {}",
        body["spec"]
    );
}

#[tokio::test]
async fn test_field_validation_warn_pod_emits_warning_header() {
    let (_mem, router) = spawn_router();
    let mut stub = pod_stub("pod-warn-header");
    stub["spec"]["bogusField"] = json!("ignored");
    let uri = format!("{}?fieldValidation=Warn", pod_uri());

    let (status, headers, _body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(status.is_success(), "Warn mode should succeed");
    let warnings = warning_headers(&headers);
    assert!(
        !warnings.is_empty(),
        "Warn mode must emit at least one Warning header"
    );
    assert!(
        warnings.iter().any(|w| w.contains("bogusField")),
        "Warning header must mention the unknown field: {:?}",
        warnings
    );
    // Upstream prefixes warnings with `299 - "..."`; record that pattern here.
    assert!(
        warnings.iter().any(|w| w.starts_with("299 ")),
        "Warning headers should use code 299 per RFC 7234: {:?}",
        warnings
    );
}

#[tokio::test]
async fn test_field_validation_warn_deployment_unknown_field_accepted() {
    let (_mem, router) = spawn_router();
    let mut stub = deployment_stub("dep-warn");
    stub["spec"]["weirdFlag"] = json!(true);
    let uri = format!("{}?fieldValidation=Warn", deployment_uri());

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(
        status.is_success(),
        "Warn mode should accept unknown field on Deployment; got {}: body={}",
        status,
        body
    );
    assert!(
        body["spec"].get("weirdFlag").is_none(),
        "unknown field must be stripped from the response: {}",
        body["spec"]
    );
}

// ---------------------------------------------------------------------------
// Ignore mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_field_validation_ignore_pod_unknown_field_silently_accepted() {
    let (_mem, router) = spawn_router();
    let mut stub = pod_stub("pod-ignore");
    stub["spec"]["bogusField"] = json!("dropped");
    let uri = format!("{}?fieldValidation=Ignore", pod_uri());

    let (status, headers, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(
        status.is_success(),
        "Ignore mode must accept the request; got {}: body={}",
        status,
        body
    );
    let warnings = warning_headers(&headers);
    assert!(
        warnings.is_empty(),
        "Ignore mode must not emit Warning headers, got {:?}",
        warnings
    );
    assert!(
        body["spec"].get("bogusField").is_none(),
        "unknown field must be dropped on read-back: {}",
        body["spec"]
    );
}

#[tokio::test]
async fn test_field_validation_ignore_deployment_unknown_field_silently_accepted() {
    let (_mem, router) = spawn_router();
    let mut stub = deployment_stub("dep-ignore");
    stub["spec"]["weirdFlag"] = json!(true);
    let uri = format!("{}?fieldValidation=Ignore", deployment_uri());

    let (status, headers, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(
        status.is_success(),
        "Ignore mode should succeed on Deployment; got {}: body={}",
        status,
        body
    );
    let warnings = warning_headers(&headers);
    assert!(
        warnings.is_empty(),
        "Ignore mode must not emit Warning headers, got {:?}",
        warnings
    );
}

// ---------------------------------------------------------------------------
// Default mode (no `fieldValidation=` query param)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_field_validation_default_pod_clean_body_accepted() {
    // After unit #7 flipped the default to Strict, the no-param + clean-body
    // path must still succeed. This is the regression guard for the default
    // change: only *unknown* fields should be rejected, valid bodies pass
    // through.
    let (_mem, router) = spawn_router();
    let stub = pod_stub("pod-default-clean");
    let uri = pod_uri();

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(
        status.is_success(),
        "clean body must succeed under default-Strict; got {}: body={}",
        status,
        body
    );
    assert_eq!(body["metadata"]["name"], "pod-default-clean");
}

#[tokio::test]
async fn test_field_validation_default_pod_unknown_field_rejected_k8s_1_35() {
    let (_mem, router) = spawn_router();
    let mut stub = pod_stub("pod-default-strict");
    stub["spec"]["bogusField"] = json!("nope");
    let uri = pod_uri();

    let (status, _hdrs, body) = send_json(router, Method::POST, &uri, &stub).await;

    assert!(
        status.is_client_error(),
        "K8s 1.35 default must reject unknown fields without explicit param; \
         got {}: body={}",
        status,
        body
    );
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("strict decoding error"),
        "default rejection should match strict decoder format: {}",
        message
    );
}

// ---------------------------------------------------------------------------
// PUT (update) path — strict mode should fire on updates too.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_field_validation_strict_pod_update_unknown_field_rejected() {
    let (_mem, router) = spawn_router();

    // Seed a pod first so PUT has something to update.
    let stub = pod_stub("pod-update-strict");
    let (create_status, _, create_body) =
        send_json(router.clone(), Method::POST, &pod_uri(), &stub).await;
    assert!(
        create_status.is_success(),
        "seed pod create should succeed, got {}: {}",
        create_status,
        create_body
    );

    // Now PUT with an unknown field + fieldValidation=Strict.
    let mut updated = stub.clone();
    updated["spec"]["bogusField"] = json!("nope");
    let put_uri = format!(
        "/api/v1/namespaces/{}/pods/pod-update-strict?fieldValidation=Strict",
        TEST_NS
    );
    let (status, _hdrs, body) = send_json(router, Method::PUT, &put_uri, &updated).await;

    assert!(
        status.is_client_error(),
        "PUT with fieldValidation=Strict must reject unknown fields, got {}: {}",
        status,
        body
    );
    assert_eq!(body["kind"], "Status");
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("strict decoding error") && message.contains("bogusField"),
        "update strict error should identify field: {}",
        message
    );
}
