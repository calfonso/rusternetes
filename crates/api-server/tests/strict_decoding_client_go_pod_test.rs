//! Strict-decoding regression tests for client-go-shaped Pod bodies.
//!
//! After PR #675 made `?fieldValidation=Strict` the server-side default
//! (matching K8s 1.25+), any Pod create issued by stock client-go
//! against rusternetes started failing with
//!
//!     strict decoding error: unknown field "metadata.creationTimestamp",
//!     unknown field "spec.hostIPC", unknown field "spec.hostPID"
//!
//! Hydrophone's conformance-test pod is one such body. Three bugs are
//! exercised below:
//!
//! 1. `metadata.creationTimestamp: null` — Go marshals the zero-value
//!    `time.Time` as JSON `null`, so client-go always emits this field.
//!    `ObjectMeta::creation_timestamp` is `Option<DateTime<Utc>>` with
//!    `skip_serializing_if = "Option::is_none"`, so the canonical
//!    round-trip drops the key. The diff-based strict differ then
//!    flags the original `null` as "unknown field".
//!
//! 2. `spec.hostPID` — `PodSpec::host_pid` exists but serde renames
//!    `host_pid` → `hostPid` via `rename_all = "camelCase"`. Upstream
//!    K8s emits the acronym all-caps (`hostPID`), per the same
//!    convention as `podIP`/`containerID`/`hostIPC`. The lowercase
//!    `hostPid` never matches client-go's bytes.
//!
//! 3. `spec.hostIPC` — same root cause as #2.
//!
//! Each test below sends a request whose body is a minimised version
//! of what hydrophone (and stock client-go) actually emit; they assert
//! the server returns 2xx, NOT 400 BadRequest. Three of the five tests
//! fail on `fork/main` HEAD as of 2026-05-21.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const TEST_NS: &str = "default";

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

// Consumes the harness per request, matching the original by-value Router.
async fn post_pod(api: TestApiServer, body: Value) -> (StatusCode, Value) {
    api.post(&format!("/api/v1/namespaces/{TEST_NS}/pods"), &body)
        .await
}

/// `metadata.creationTimestamp: null` is what `time.Time{}.MarshalJSON()`
/// emits when client-go serialises a Pod whose creation timestamp has
/// not yet been set. The default `Strict` field-validation mode must
/// accept it — `creationTimestamp` IS a declared field on `ObjectMeta`,
/// it just happens to round-trip to an absent key on the canonical
/// re-serialise path because `Option::is_none` is skipped.
#[tokio::test]
async fn test_client_go_pod_with_creation_timestamp_null_accepted_under_strict() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ts-null",
            "namespace": TEST_NS,
            "creationTimestamp": null
        },
        "spec": {
            "containers": [{"name": "c", "image": "pause:latest"}]
        }
    });
    let (status, resp) = post_pod(router, body).await;
    assert!(
        status.is_success(),
        "creationTimestamp: null must be accepted under default-Strict; got {} body={}",
        status,
        resp
    );
}

/// `spec.hostPID: false` from client-go must round-trip — the K8s
/// camelCase abbreviation convention spells this `hostPID`
/// (acronym all-caps), matching `podIP` / `containerID` / `hostIPC`.
#[tokio::test]
async fn test_client_go_pod_with_host_pid_accepted_under_strict() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "hostpid", "namespace": TEST_NS},
        "spec": {
            "hostPID": false,
            "containers": [{"name": "c", "image": "pause:latest"}]
        }
    });
    let (status, resp) = post_pod(router, body).await;
    assert!(
        status.is_success(),
        "spec.hostPID must be accepted under default-Strict; got {} body={}",
        status,
        resp
    );
}

/// `spec.hostIPC: false` from client-go must round-trip with the
/// acronym-all-caps spelling `hostIPC`.
#[tokio::test]
async fn test_client_go_pod_with_host_ipc_accepted_under_strict() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "hostipc", "namespace": TEST_NS},
        "spec": {
            "hostIPC": false,
            "containers": [{"name": "c", "image": "pause:latest"}]
        }
    });
    let (status, resp) = post_pod(router, body).await;
    assert!(
        status.is_success(),
        "spec.hostIPC must be accepted under default-Strict; got {} body={}",
        status,
        resp
    );
}

/// The exact hydrophone failure surface: a Pod body carrying all three
/// fields the canary reported as `unknown` (creationTimestamp:null,
/// hostIPC, hostPID). This test pins the regression so future strict-
/// decoder refactors don't reintroduce the gap.
#[tokio::test]
async fn test_hydrophone_conformance_pod_shape_accepted_under_strict() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "e2e-conformance-test",
            "namespace": TEST_NS,
            "creationTimestamp": null
        },
        "spec": {
            "hostIPC": false,
            "hostPID": false,
            "containers": [
                {"name": "conformance", "image": "registry.k8s.io/conformance:v1.35.0"}
            ]
        }
    });
    let (status, resp) = post_pod(router, body).await;
    assert!(
        status.is_success(),
        "hydrophone-shaped pod must be accepted under default-Strict; got {} body={}",
        status,
        resp
    );
}

/// Negative-path coverage: truly unknown fields carrying a NON-null
/// value are still rejected. The null-valued case is a known
/// false-negative of diff-based strict decoding — see the
/// `#[ignore]`'d sibling below.
#[tokio::test]
async fn test_genuinely_unknown_field_with_non_null_value_still_rejected_under_strict() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "bogus-nonnull", "namespace": TEST_NS},
        "spec": {
            "containers": [{"name": "c", "image": "pause:latest"}],
            "thisFieldDoesNotExistOnPodSpec": "carrying-a-value"
        }
    });
    let (status, resp) = post_pod(router, body).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "truly unknown non-null-valued field must still be rejected; got {} body={}",
        status,
        resp
    );
}

/// Schema-aware strict decoding (via `serde_ignored`, see
/// `find_unknown_fields_via_schema` in `validation.rs`) flags every key
/// that isn't declared on `PodSpec`, regardless of value. Previously
/// this case slipped through because the diff-based decoder couldn't
/// distinguish "truly unknown null-valued field" from "legit
/// `Option<...>` field round-trip-dropped because the value was null"
/// — the new decoder asks `PodSpec`'s own `Visitor` what it consumed,
/// so the ambiguity disappears.
#[tokio::test]
async fn test_genuinely_unknown_null_valued_field_currently_slips_through() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "bogus-null", "namespace": TEST_NS},
        "spec": {
            "containers": [{"name": "c", "image": "pause:latest"}],
            "thisFieldDoesNotExistOnPodSpec": null
        }
    });
    let (status, resp) = post_pod(router, body).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "schema-aware strict decode must reject null-valued unknowns; got {} body={}",
        status,
        resp
    );
}
