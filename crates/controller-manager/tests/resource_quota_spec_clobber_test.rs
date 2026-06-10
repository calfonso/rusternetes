//! Regression test for GitHub #268 — "[sig-api-machinery] ResourceQuota should
//! be able to update and delete ResourceQuota. [Conformance]".
//!
//! The quota controller reconciles status from a (possibly stale) snapshot. It
//! must update only the status subresource: writing the whole object back would
//! revert a spec the client just changed. The conformance test updates a quota
//! from CPU=1/500Mi to CPU=2/1Gi and then GETs it; a racing status write that
//! carried a stale spec reverted the GET to the old values (resource_quota.go:984).

use rusternetes_common::resources::{ResourceQuota, ResourceQuotaSpec};
use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn hard(cpu: &str, memory: &str) -> HashMap<String, String> {
    HashMap::from([
        ("cpu".to_string(), cpu.to_string()),
        ("memory".to_string(), memory.to_string()),
    ])
}

fn quota_spec(cpu: &str, memory: &str) -> ResourceQuotaSpec {
    ResourceQuotaSpec {
        hard: Some(hard(cpu, memory)),
        scopes: None,
        scope_selector: None,
    }
}

#[tokio::test]
async fn reconcile_after_spec_update_preserves_new_spec() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ResourceQuotaController::new(storage.clone());
    let key = build_key("resourcequotas", Some("ns"), "test-quota");

    // Quota created with CPU=1 / 500Mi, no status.
    storage
        .create(
            &key,
            &ResourceQuota::new("test-quota", "ns", quota_spec("1", "500Mi")),
        )
        .await
        .unwrap();

    // First reconcile populates status; spec is untouched.
    controller.reconcile_one("ns", "test-quota").await.unwrap();
    let after_first: ResourceQuota = storage.get(&key).await.unwrap();
    assert!(after_first.status.is_some(), "status should be populated");
    assert_eq!(after_first.spec.hard.as_ref().unwrap()["cpu"], "1");

    // Client updates the spec to CPU=2 / 1Gi (carrying the current RV).
    let mut updated = after_first.clone();
    updated.spec = quota_spec("2", "1Gi");
    storage.update(&key, &updated).await.unwrap();

    // A status reconcile now must NOT revert the spec to the old values.
    controller.reconcile_one("ns", "test-quota").await.unwrap();

    let after_reconcile: ResourceQuota = storage.get(&key).await.unwrap();
    let spec_hard = after_reconcile.spec.hard.as_ref().unwrap();
    assert_eq!(
        spec_hard["cpu"], "2",
        "status reconcile clobbered spec.hard.cpu back to the pre-update value (#268)"
    );
    assert_eq!(
        spec_hard["memory"], "1Gi",
        "status reconcile clobbered spec.hard.memory back to the pre-update value (#268)"
    );
    // Status should track the current spec's hard limits.
    let status_hard = after_reconcile.status.unwrap().hard.unwrap();
    assert_eq!(status_hard["cpu"], "2");
}
