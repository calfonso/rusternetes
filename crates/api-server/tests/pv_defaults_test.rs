//! SetDefaults_PersistentVolume: reclaimPolicy defaults to Retain and
//! volumeMode defaults to Filesystem when unset; explicit values preserved.
//! K8s ref: pkg/apis/core/v1/defaults.go SetDefaults_PersistentVolume.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn pvs_uri() -> &'static str {
    "/api/v1/persistentvolumes"
}

fn pv(name: &str, extra_spec: Value) -> Value {
    let mut spec = json!({
        "capacity": {"storage": "1Gi"},
        "accessModes": ["ReadWriteOnce"],
        "hostPath": {"path": "/tmp/data"}
    });
    if let Value::Object(m) = extra_spec {
        for (k, v) in m {
            spec[k] = v;
        }
    }
    json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": name},
        "spec": spec
    })
}

#[tokio::test]
async fn reclaim_policy_and_volume_mode_default() {
    let state = TestApiServer::new();
    let (code, body) = state.post(pvs_uri(), &pv("pv-def", json!({}))).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["spec"]["persistentVolumeReclaimPolicy"],
        json!("Retain"),
        "{body}"
    );
    assert_eq!(body["spec"]["volumeMode"], json!("Filesystem"), "{body}");
}

#[tokio::test]
async fn explicit_values_preserved() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(
            pvs_uri(),
            &pv(
                "pv-exp",
                json!({"persistentVolumeReclaimPolicy": "Delete", "volumeMode": "Block"}),
            ),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["spec"]["persistentVolumeReclaimPolicy"],
        json!("Delete"),
        "{body}"
    );
    assert_eq!(body["spec"]["volumeMode"], json!("Block"), "{body}");
}
