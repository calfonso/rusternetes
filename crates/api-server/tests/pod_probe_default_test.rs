//! Regression test for the pod create defaulting/validation ordering bug
//! (Node Conformance: ~28 probe specs failed at create with
//! `livenessProbe.successThreshold: Invalid value: 0: must be 1`).
//!
//! Upstream defaults the object (SetDefaults_Probe → successThreshold=1) BEFORE
//! validation; rusternetes previously validated first, so a probe that omits
//! successThreshold (the common case — Go's omitempty drops the zero value) was
//! rejected by the probe validator instead of being defaulted to 1.
//!
//! A pod whose liveness probe omits successThreshold must therefore be created
//! (201) and come back with successThreshold defaulted to 1.

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

#[tokio::test]
async fn pod_liveness_probe_without_success_threshold_is_defaulted_not_rejected() {
    let api = TestApiServer::new();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "probe-pod", "namespace": "default" },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "registry.k8s.io/pause:3.10",
                // successThreshold / failureThreshold / periodSeconds omitted on
                // purpose — they must be defaulted, not validated as 0.
                "livenessProbe": {
                    "httpGet": { "path": "/healthz", "port": 8080 }
                }
            }]
        }
    });

    let (status, body): (StatusCode, Value) = api
        .send(
            Method::POST.as_str(),
            "/api/v1/namespaces/default/pods",
            Some("application/json"),
            Some(&pod),
        )
        .await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod with an omitted probe successThreshold must be created, got {status}: {body}"
    );

    // The stored probe must carry the upstream defaults (successThreshold=1,
    // failureThreshold=3, periodSeconds=10, timeoutSeconds=1).
    let probe = &body["spec"]["containers"][0]["livenessProbe"];
    assert_eq!(probe["successThreshold"], json!(1), "body: {body}");
    assert_eq!(probe["failureThreshold"], json!(3), "body: {body}");
    assert_eq!(probe["periodSeconds"], json!(10), "body: {body}");
    assert_eq!(probe["timeoutSeconds"], json!(1), "body: {body}");
}
