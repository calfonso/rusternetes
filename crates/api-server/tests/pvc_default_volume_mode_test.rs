//! SetDefaults_PersistentVolumeClaimSpec: spec.volumeMode defaults to
//! Filesystem when unset; an explicit Block is preserved; and an update that
//! omits volumeMode is not falsely rejected by the immutability check (both
//! sides default to Filesystem). K8s ref: pkg/apis/core/v1/defaults.go.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn pvcs_uri() -> String {
    format!("/api/v1/namespaces/{NS}/persistentvolumeclaims")
}

fn pvc(name: &str, volume_mode: Option<&str>) -> Value {
    let mut spec = json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": "1Gi"}}
    });
    if let Some(vm) = volume_mode {
        spec["volumeMode"] = json!(vm);
    }
    json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name},
        "spec": spec
    })
}

#[tokio::test]
async fn volume_mode_defaults_to_filesystem() {
    let state = TestApiServer::new();
    let (code, body) = state.post(&pvcs_uri(), &pvc("pvc-def", None)).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["volumeMode"], json!("Filesystem"), "{body}");
}

#[tokio::test]
async fn explicit_block_preserved() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(&pvcs_uri(), &pvc("pvc-blk", Some("Block")))
        .await;
    assert_eq!(code, StatusCode::CREATED);
    assert_eq!(body["spec"]["volumeMode"], json!("Block"), "{body}");
}

#[tokio::test]
async fn update_omitting_volume_mode_not_rejected() {
    let state = TestApiServer::new();
    let name = "pvc-upd";
    let (code, _) = state.post(&pvcs_uri(), &pvc(name, None)).await;
    assert_eq!(code, StatusCode::CREATED);
    // PUT without volumeMode must default to Filesystem and pass the
    // immutability check (old was defaulted to Filesystem too).
    let (code, body) = state
        .put(&format!("{}/{name}", pvcs_uri()), &pvc(name, None))
        .await;
    assert_eq!(
        code,
        StatusCode::OK,
        "update omitting volumeMode must succeed: {body}"
    );
}
