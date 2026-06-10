//! Regression tests for `Storage::update_status` — the status-only write that
//! prevents a background controller from clobbering a concurrently-updated spec.
//!
//! Models the ResourceQuota "should be able to update and delete" conformance
//! flake (GitHub #268): the quota controller computes status from a stale
//! snapshot and writes its whole object back, reverting the client's spec
//! update. A status-only write must leave the stored spec untouched.

use rusternetes_storage::{MemoryStorage, Storage};
use serde_json::{json, Value};

fn quota(cpu: &str, memory: &str) -> Value {
    json!({
        "metadata": {"name": "test-quota", "namespace": "ns"},
        "spec": {"hard": {"cpu": cpu, "memory": memory}},
    })
}

#[tokio::test]
async fn update_status_preserves_concurrently_updated_spec() {
    let storage = MemoryStorage::new();
    let key = "/registry/resourcequotas/ns/test-quota";

    // Quota created with CPU=1 / 500Mi, no status yet.
    storage
        .create::<Value>(key, &quota("1", "500Mi"))
        .await
        .unwrap();

    // Client updates the spec to CPU=2 / 1Gi — this is now the stored truth.
    storage
        .update::<Value>(key, &quota("2", "1Gi"))
        .await
        .unwrap();

    // The controller acts on a STALE view (still CPU=1) and writes its computed
    // status back. A status-only write must NOT carry the stale spec.
    let stale_with_status = json!({
        "metadata": {"name": "test-quota", "namespace": "ns"},
        "spec": {"hard": {"cpu": "1", "memory": "500Mi"}},
        "status": {
            "hard": {"cpu": "1", "memory": "500Mi"},
            "used": {"cpu": "0", "memory": "0"},
        },
    });
    storage
        .update_status::<Value>(key, &stale_with_status)
        .await
        .unwrap();

    let got: Value = storage.get(key).await.unwrap();

    // Spec must remain the client's CPU=2 / 1Gi — not reverted to 1 / 500Mi.
    assert_eq!(
        got["spec"]["hard"]["cpu"],
        json!("2"),
        "status write clobbered spec.hard.cpu back to the stale value"
    );
    assert_eq!(
        got["spec"]["hard"]["memory"],
        json!("1Gi"),
        "status write clobbered spec.hard.memory back to the stale value"
    );

    // The status the controller computed must have been applied.
    assert_eq!(got["status"]["hard"]["cpu"], json!("1"));
    assert_eq!(got["status"]["used"]["cpu"], json!("0"));
}
