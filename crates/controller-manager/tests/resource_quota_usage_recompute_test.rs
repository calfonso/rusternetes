//! Conformance regression: ResourceQuota usage must recompute when a tracked
//! object is deleted.
//!
//! Mirrors upstream e2e test `apimachinery/resource_quota.go:312`
//! ("ResourceQuota tracks pods consuming compute" / scope-selector tests):
//!
//! 1. Create a ResourceQuota with `hard.pods = 10` plus compute hard limits.
//! 2. Create a Pod with compute requests.
//! 3. Reconcile — `status.used.pods` must be "1".
//! 4. Delete the Pod (object removal from storage, as the upstream test does).
//! 5. Reconcile — `status.used.pods` must drop back to "0".
//!
//! Also covers the scope-selector variant: BestEffort and NotBestEffort
//! quotas must both recompute to 0 after the (only) tracked pod is deleted.
//!
//! The bug class this test guards against: the controller skips a recompute
//! (or operates against a stale cache) when an object is deleted between
//! reconciles, so `.status.used` never decrements and the upstream e2e
//! times out.

use rusternetes_common::resources::{
    Container, Pod, PodSpec, PodStatus, ResourceQuota, ResourceQuotaSpec,
};
use rusternetes_common::types::{ObjectMeta, Phase, ResourceRequirements, TypeMeta};
use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn make_pod_with_compute(name: &str, namespace: &str, cpu: &str, memory: &str) -> Pod {
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), cpu.to_string());
    requests.insert("memory".to_string(), memory.to_string());

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources: Some(ResourceRequirements {
                    requests: Some(requests),
                    limits: None,
                    claims: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        }),
    }
}

fn make_best_effort_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources: None,
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        }),
    }
}

fn make_compute_quota(name: &str, namespace: &str) -> ResourceQuota {
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "10".to_string());
    hard.insert("requests.cpu".to_string(), "2".to_string());
    hard.insert("requests.memory".to_string(), "2Gi".to_string());
    ResourceQuota::new(
        name,
        namespace,
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    )
}

fn make_scoped_quota(name: &str, namespace: &str, scope: &str) -> ResourceQuota {
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "10".to_string());
    ResourceQuota::new(
        name,
        namespace,
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: Some(vec![scope.to_string()]),
            scope_selector: None,
        },
    )
}

/// Direct reconcile path: after a tracked pod is deleted from storage,
/// `reconcile_one` MUST observe the deletion (fresh list) and write back
/// `used.pods = "0"`.
#[tokio::test]
async fn test_reconcile_one_decrements_used_on_pod_delete() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ResourceQuotaController::new(storage.clone());

    let ns = "test-ns";
    let quota_name = "compute-quota";
    let quota_key = build_key("resourcequotas", Some(ns), quota_name);
    let pod_key = build_key("pods", Some(ns), "pod-a");

    storage
        .create(&quota_key, &make_compute_quota(quota_name, ns))
        .await
        .unwrap();
    storage
        .create(
            &pod_key,
            &make_pod_with_compute("pod-a", ns, "500m", "256Mi"),
        )
        .await
        .unwrap();

    controller
        .reconcile_one(ns, quota_name)
        .await
        .expect("first reconcile");

    let after_create: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let used_after_create = after_create
        .status
        .as_ref()
        .and_then(|s| s.used.as_ref())
        .expect("status.used set after first reconcile");
    assert_eq!(
        used_after_create.get("pods").map(|s| s.as_str()),
        Some("1"),
        "after pod create, used.pods must be 1, got {:?}",
        used_after_create.get("pods")
    );

    storage.delete(&pod_key).await.unwrap();

    controller
        .reconcile_one(ns, quota_name)
        .await
        .expect("second reconcile");

    let after_delete: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let used_after_delete = after_delete
        .status
        .as_ref()
        .and_then(|s| s.used.as_ref())
        .expect("status.used set after second reconcile");
    assert_eq!(
        used_after_delete.get("pods").map(|s| s.as_str()),
        Some("0"),
        "after pod delete, used.pods must be 0, got {:?}",
        used_after_delete.get("pods")
    );
    assert_eq!(
        used_after_delete
            .get("requests.cpu")
            .map(|s| s.as_str())
            .unwrap_or(""),
        "0m",
        "after pod delete, used.requests.cpu must be 0 (got {:?})",
        used_after_delete.get("requests.cpu")
    );
    assert_eq!(
        used_after_delete
            .get("requests.memory")
            .map(|s| s.as_str())
            .unwrap_or(""),
        "0",
        "after pod delete, used.requests.memory must be 0 (got {:?})",
        used_after_delete.get("requests.memory")
    );
}

/// Scope-selector variant of the upstream e2e: both BestEffort and
/// NotBestEffort quotas in the same namespace must recompute correctly
/// after the (only) tracked pod is deleted. This is the scenario behind
/// the line-312 sub-test in upstream `apimachinery/resource_quota.go`.
#[tokio::test]
async fn test_reconcile_one_decrements_scoped_quotas_on_pod_delete() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ResourceQuotaController::new(storage.clone());

    let ns = "scope-ns";
    let be_quota_key = build_key("resourcequotas", Some(ns), "be-quota");
    let nbe_quota_key = build_key("resourcequotas", Some(ns), "nbe-quota");
    let pod_key = build_key("pods", Some(ns), "be-pod");

    storage
        .create(
            &be_quota_key,
            &make_scoped_quota("be-quota", ns, "BestEffort"),
        )
        .await
        .unwrap();
    storage
        .create(
            &nbe_quota_key,
            &make_scoped_quota("nbe-quota", ns, "NotBestEffort"),
        )
        .await
        .unwrap();
    storage
        .create(&pod_key, &make_best_effort_pod("be-pod", ns))
        .await
        .unwrap();

    // After create: BE=1, NBE=0.
    controller.reconcile_one(ns, "be-quota").await.unwrap();
    controller.reconcile_one(ns, "nbe-quota").await.unwrap();
    let be: ResourceQuota = storage.get(&be_quota_key).await.unwrap();
    let nbe: ResourceQuota = storage.get(&nbe_quota_key).await.unwrap();
    assert_eq!(
        be.status
            .as_ref()
            .and_then(|s| s.used.as_ref())
            .and_then(|u| u.get("pods"))
            .map(|s| s.as_str()),
        Some("1"),
        "BestEffort quota should count the BE pod"
    );
    assert_eq!(
        nbe.status
            .as_ref()
            .and_then(|s| s.used.as_ref())
            .and_then(|u| u.get("pods"))
            .map(|s| s.as_str()),
        Some("0"),
        "NotBestEffort quota should not count the BE pod"
    );

    // Delete the pod.
    storage.delete(&pod_key).await.unwrap();

    // After delete: BE=0, NBE=0.
    controller.reconcile_one(ns, "be-quota").await.unwrap();
    controller.reconcile_one(ns, "nbe-quota").await.unwrap();
    let be: ResourceQuota = storage.get(&be_quota_key).await.unwrap();
    let nbe: ResourceQuota = storage.get(&nbe_quota_key).await.unwrap();
    assert_eq!(
        be.status
            .as_ref()
            .and_then(|s| s.used.as_ref())
            .and_then(|u| u.get("pods"))
            .map(|s| s.as_str()),
        Some("0"),
        "after pod delete, BestEffort quota must recompute to 0"
    );
    assert_eq!(
        nbe.status
            .as_ref()
            .and_then(|s| s.used.as_ref())
            .and_then(|u| u.get("pods"))
            .map(|s| s.as_str()),
        Some("0"),
        "after pod delete, NotBestEffort quota must remain at 0"
    );
}

/// End-to-end watch path: spawn the controller's full `run()` loop and
/// verify that a pod deletion triggers a status.used recompute via the
/// pod-watch fan-out into the quota work queue.
///
/// This guards the upstream e2e scenario: the controller in production
/// only sees pod-delete via its watch on `/registry/pods/`. If the
/// pod-delete watch event isn't fanned out to the quota work queue, the
/// quota's `status.used.pods` will remain at 1 forever and the e2e times
/// out.
#[tokio::test]
async fn test_watch_loop_decrements_used_on_pod_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let ns = "watch-ns";
    let quota_name = "compute-quota";
    let pod_name = "watch-pod";
    let quota_key = build_key("resourcequotas", Some(ns), quota_name);
    let pod_key = build_key("pods", Some(ns), pod_name);

    storage
        .create(&quota_key, &make_compute_quota(quota_name, ns))
        .await
        .unwrap();

    let controller = Arc::new(ResourceQuotaController::new(storage.clone()));
    let run_handle = {
        let c = Arc::clone(&controller);
        tokio::spawn(async move {
            let _ = c.run().await;
        })
    };

    // Give run() a moment to install its watches and finish initial enqueue.
    tokio::time::sleep(Duration::from_millis(300)).await;

    storage
        .create(
            &pod_key,
            &make_pod_with_compute(pod_name, ns, "500m", "256Mi"),
        )
        .await
        .unwrap();

    // Poll until used.pods becomes "1" (controller saw the create).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_one = false;
    while tokio::time::Instant::now() < deadline {
        let q: ResourceQuota = storage.get(&quota_key).await.unwrap();
        if q.status
            .as_ref()
            .and_then(|s| s.used.as_ref())
            .and_then(|u| u.get("pods"))
            .map(|s| s.as_str())
            == Some("1")
        {
            saw_one = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if !saw_one {
        run_handle.abort();
        let q: ResourceQuota = storage.get(&quota_key).await.unwrap();
        panic!(
            "controller never reflected pod-create in status.used.pods (final: {:?})",
            q.status
                .and_then(|s| s.used)
                .and_then(|u| u.get("pods").cloned())
        );
    }

    // Delete the pod.
    storage.delete(&pod_key).await.unwrap();

    // Poll until used.pods becomes "0" (controller saw the delete).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_zero = false;
    let mut last_seen: Option<String> = None;
    while tokio::time::Instant::now() < deadline {
        let q: ResourceQuota = storage.get(&quota_key).await.unwrap();
        let v = q
            .status
            .as_ref()
            .and_then(|s| s.used.as_ref())
            .and_then(|u| u.get("pods"))
            .cloned();
        last_seen = v.clone();
        if v.as_deref() == Some("0") {
            saw_zero = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    run_handle.abort();

    assert!(
        saw_zero,
        "controller failed to recompute status.used.pods after pod delete \
         (last seen: {:?}). Upstream e2e apimachinery/resource_quota.go:312 \
         will time out on this regression.",
        last_seen
    );
}
