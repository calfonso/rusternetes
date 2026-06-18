//! Regression test for the conformance bug:
//!   "the server rejected our request due to an error in our request
//!    (post replicationcontrollers)"
//!
//! client-go emits that message when the api-server returns an error WITHOUT a
//! proper `metav1.Status` body. The root cause is that POSTing a valid
//! core/v1 ReplicationController (as built by the upstream e2e framework) must
//! deserialize, validate, and persist cleanly — and on any error path the body
//! must be a `Status` object, not axum's default plain-text 400.
//!
//! These tests drive the *full* Axum router over real HTTP via tower's
//! `oneshot`, posting the exact JSON shape the e2e framework sends:
//! `spec.replicas`, `spec.selector` (plain label map), and `spec.template`
//! (PodTemplateSpec with `metadata.labels` + `spec.containers`).
//!
//! Failing e2e tests this guards:
//!   [sig-apps] ReplicationController should get and update a ReplicationController
//!   [sig-apps] ReplicationController should release no longer matching pods
//!   [sig-api-machinery] ResourceQuota ... life of a pod/replication controller
//!   [sig-api-machinery] Garbage collector should orphan pods created by rc

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const TEST_NS: &str = "rc-e2e";

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

// Posts the exact e2e-framework byte stream (built via `serde_json::to_vec`) so
// the test pins what client-go sends on the wire. `send_bytes` accepts the raw
// body verbatim rather than re-serialising a `Value`.
async fn post_raw(router: TestApiServer, uri: &str, body: Vec<u8>) -> (StatusCode, Value) {
    let (status, _, value) = router
        .send_bytes("POST", uri, Some("application/json"), Some(body))
        .await;
    (status, value)
}

/// The canonical RC JSON produced by the upstream e2e framework's `NewRC` /
/// `rc()` helpers: a label-map selector and an inline pod template carrying
/// `metadata.labels` plus a single container with a named port.
fn e2e_rc_json(name: &str) -> Value {
    json!({
        "kind": "ReplicationController",
        "apiVersion": "v1",
        "metadata": {
            "name": name,
            "labels": { "name": name }
        },
        "spec": {
            "replicas": 2,
            "selector": { "name": name },
            "template": {
                "metadata": {
                    "labels": { "name": name }
                },
                "spec": {
                    "containers": [
                        {
                            "name": name,
                            "image": "registry.k8s.io/pause:3.9",
                            "ports": [
                                { "containerPort": 80, "protocol": "TCP" }
                            ]
                        }
                    ]
                }
            }
        }
    })
}

#[tokio::test]
async fn test_post_e2e_replicationcontroller_succeeds() {
    let router = spawn_router();
    let uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers");
    let body = serde_json::to_vec(&e2e_rc_json("my-hostname-basic")).unwrap();

    let (status, body) = post_raw(router, &uri, body).await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "POST of a valid e2e ReplicationController must return 201 Created, got {status}: {body}"
    );
    assert_eq!(body["kind"], "ReplicationController");
    assert_eq!(body["metadata"]["name"], "my-hostname-basic");
    assert_eq!(body["spec"]["replicas"], 2);
    // selector must round-trip as the label map the e2e framework sent
    assert_eq!(body["spec"]["selector"]["name"], "my-hostname-basic");
    assert!(
        !body["metadata"]["uid"].as_str().unwrap_or("").is_empty(),
        "created RC must be assigned a uid"
    );
}

/// Even when the payload is genuinely invalid, the error body must be a proper
/// `metav1.Status` (so client-go surfaces the real reason instead of the
/// generic "the server rejected our request due to an error in our request").
#[tokio::test]
async fn test_post_malformed_replicationcontroller_returns_status() {
    let router = spawn_router();
    let uri = format!("/api/v1/namespaces/{TEST_NS}/replicationcontrollers");
    // `replicas` as a string is the wrong type — deserialization will fail.
    let bad = json!({
        "kind": "ReplicationController",
        "apiVersion": "v1",
        "metadata": { "name": "bad-rc" },
        "spec": {
            "replicas": "not-a-number",
            "selector": { "name": "bad-rc" },
            "template": {
                "metadata": { "labels": { "name": "bad-rc" } },
                "spec": { "containers": [ { "name": "c", "image": "x" } ] }
            }
        }
    });
    let body = serde_json::to_vec(&bad).unwrap();

    let (status, body) = post_raw(router, &uri, body).await;

    assert!(
        status.is_client_error(),
        "malformed RC should be a 4xx, got {status}: {body}"
    );
    assert_eq!(
        body["kind"], "Status",
        "error body must be a metav1.Status, got: {body}"
    );
    assert_eq!(body["status"], "Failure");
}
