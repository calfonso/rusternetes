//! CSIDriver update: attachRequired and volumeLifecycleModes are immutable
//! (upstream ValidateCSIDriverUpdate).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn uri() -> &'static str {
    "/apis/storage.k8s.io/v1/csidrivers"
}

fn csidriver(name: &str, attach_required: bool) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
        "metadata": {"name": name},
        "spec": {"attachRequired": attach_required, "podInfoOnMount": false}
    })
}

#[tokio::test]
async fn csidriver_attach_required_immutable() {
    let state = TestApiServer::new();
    let name = "csi.example.com";
    let (code, _) = state.post(uri(), &csidriver(name, true)).await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed");
    let item = format!("{}/{name}", uri());

    // Changing attachRequired is rejected.
    let (code, body) = state.put(&item, &csidriver(name, false)).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "attachRequired change must be rejected: {body}"
    );

    // Unchanged update succeeds.
    let (code, _) = state.put(&item, &csidriver(name, true)).await;
    assert_eq!(code, StatusCode::OK, "unchanged update must succeed");
}
