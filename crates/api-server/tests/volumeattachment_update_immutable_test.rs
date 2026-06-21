//! VolumeAttachment spec is read-only on update (upstream
//! ValidateVolumeAttachmentUpdate). The api-server update handler previously
//! persisted spec changes via PUT.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn uri() -> &'static str {
    "/apis/storage.k8s.io/v1/volumeattachments"
}

fn va(name: &str, attacher: &str) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "VolumeAttachment",
        "metadata": {"name": name},
        "spec": {
            "attacher": attacher,
            "nodeName": "node1",
            "source": {"persistentVolumeName": "pv-1"}
        }
    })
}

#[tokio::test]
async fn volumeattachment_spec_immutable_on_update() {
    let state = TestApiServer::new();
    let name = "va1";
    let (code, _) = state.post(uri(), &va(name, "csi-driver")).await;
    assert_eq!(code, StatusCode::CREATED);
    let item = format!("{}/{name}", uri());

    // Changing the spec (attacher) is rejected.
    let (code, _) = state.put(&item, &va(name, "other-driver")).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "spec change must be rejected"
    );

    // An unchanged-spec update succeeds.
    let (code, _) = state.put(&item, &va(name, "csi-driver")).await;
    assert_eq!(code, StatusCode::OK, "unchanged-spec update must succeed");
}
