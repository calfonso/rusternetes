//! PriorityLevelConfiguration update re-validates spec (upstream re-runs
//! ValidatePriorityLevelConfiguration on the new object). The update handler
//! previously persisted PUTs unchecked.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn base() -> &'static str {
    "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations"
}

fn limited_plc(name: &str) -> Value {
    json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {"name": name},
        "spec": {
            "type": "Limited",
            "limited": {
                "nominalConcurrencyShares": 10,
                "limitResponse": {
                    "type": "Queue",
                    "queuing": {"queues": 8, "handSize": 2, "queueLengthLimit": 50}
                }
            }
        }
    })
}

#[tokio::test]
async fn plc_update_revalidates_spec() {
    let state = TestApiServer::new();
    let name = "test-plc";
    let (code, body) = state.post(base(), &limited_plc(name)).await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");
    let item = format!("{}/{name}", base());

    // Invalid update: type Exempt while name is not "exempt" (upstream:
    // "must be 'Exempt' if and only if name is 'exempt'").
    let bad = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {"name": name},
        "spec": {"type": "Exempt"}
    });
    let (code, body) = state.put(&item, &bad).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid PLC spec on update must be rejected: {body}"
    );

    // A valid update succeeds.
    let (code, body) = state.put(&item, &limited_plc(name)).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "valid PLC update must succeed: {body}"
    );
}
