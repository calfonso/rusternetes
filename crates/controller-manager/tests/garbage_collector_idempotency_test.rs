// Idempotency tests for the Garbage Collector.
//
// Context: a previous hot-loop investigation flagged the GC as a candidate
// busy-loop source. The primary cause was traced to kubelet `sync_pod`
// (commit f0eea82). These tests verify that, post-Unit-14 (which removed
// the 2-scan grace gate so orphans are reaped on the first scan), repeated
// reconciles over the same state do not:
//
//   1. Mutate pods that have no owner references and are not being deleted.
//   2. Re-emit DELETE calls for resources that have already been reaped.
//
// Each test runs `gc.scan_and_collect()` more than once over a fixed input
// and asserts the steady-state contract. Hot-loop symptoms (re-deletes,
// status churn on terminal pods, repeated updates) would manifest as either
// extra DELETE calls observed by the counting storage wrapper, or as a
// byte-level diff in the stored Pod JSON between scans.

use async_trait::async_trait;
use rusternetes_common::resources::pod::*;
use rusternetes_common::types::{ObjectMeta, OwnerReference, Phase, TypeMeta};
use rusternetes_common::Result as RnResult;
use rusternetes_controller_manager::controllers::garbage_collector::GarbageCollector;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, WatchStream};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Storage wrapper that delegates every operation to an inner `MemoryStorage`
/// while incrementing per-operation counters. Used to assert the GC issues
/// exactly one DELETE for an orphan across repeated reconciles.
struct CountingStorage {
    inner: Arc<MemoryStorage>,
    delete_count: AtomicUsize,
    update_count: AtomicUsize,
}

impl CountingStorage {
    fn new(inner: Arc<MemoryStorage>) -> Self {
        Self {
            inner,
            delete_count: AtomicUsize::new(0),
            update_count: AtomicUsize::new(0),
        }
    }

    fn delete_count(&self) -> usize {
        self.delete_count.load(Ordering::SeqCst)
    }

    fn update_count(&self) -> usize {
        self.update_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Storage for CountingStorage {
    async fn create<T>(&self, key: &str, value: &T) -> RnResult<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> RnResult<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> RnResult<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.update_count.fetch_add(1, Ordering::SeqCst);
        self.inner.update(key, value).await
    }

    async fn update_raw(&self, key: &str, value: &Value) -> RnResult<()> {
        self.update_count.fetch_add(1, Ordering::SeqCst);
        self.inner.update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> RnResult<()> {
        self.delete_count.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key).await
    }

    async fn list<T>(&self, prefix: &str) -> RnResult<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.list(prefix).await
    }

    async fn watch(&self, prefix: &str) -> RnResult<WatchStream> {
        self.inner.watch(prefix).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> RnResult<WatchStream> {
        self.inner.watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> RnResult<i64> {
        self.inner.current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> RnResult<bool> {
        self.inner.is_revision_compacted(revision).await
    }
}

/// Build a minimal pod with explicit phase and (optionally) ownerReferences.
/// Uses a stable UID so the stored JSON is deterministic across calls.
fn make_pod(
    name: &str,
    namespace: &str,
    uid: &str,
    phase: Phase,
    owner_refs: Option<Vec<OwnerReference>>,
) -> Pod {
    let mut metadata = ObjectMeta::new(name);
    metadata.namespace = Some(namespace.to_string());
    metadata.uid = uid.to_string();
    metadata.owner_references = owner_refs;

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata,
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
                image: "pause:3.9".to_string(),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ports: Some(vec![]),
                env: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                resources: None,
                working_dir: None,
                command: None,
                args: None,
                restart_policy: None,
                resize_policy: None,
                security_context: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            init_containers: None,
            restart_policy: Some("Never".to_string()),
            node_selector: None,
            node_name: None,
            volumes: None,
            affinity: None,
            tolerations: None,
            service_account_name: None,
            service_account: None,
            priority: None,
            priority_class_name: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            automount_service_account_token: None,
            ephemeral_containers: None,
            overhead: None,
            scheduler_name: None,
            topology_spread_constraints: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(phase),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        }),
    }
}

/// Read the raw stored bytes for a key. Asserts the value is present.
async fn read_raw(storage: &MemoryStorage, key: &str) -> Value {
    storage
        .get::<Value>(key)
        .await
        .expect("expected pod to be present in storage")
}

/// A pod with no ownerReferences that has terminated (Succeeded) must be
/// invisible to the GC: it is neither an orphan (no owners to be missing)
/// nor pending deletion. Repeated scans must leave the stored JSON
/// byte-equal — any mutation here would mean GC is touching terminal pods
/// on every tick (hot-loop symptom on a busy cluster).
#[tokio::test]
async fn test_gc_does_not_touch_succeeded_pod_without_owners() {
    let storage = Arc::new(MemoryStorage::new());
    let gc = GarbageCollector::new(storage.clone());

    let pod = make_pod(
        "kubectl-run-12345",
        "kubectl-1863",
        "succeeded-pod-uid-stable",
        Phase::Succeeded,
        None,
    );
    let key = build_key("pods", Some("kubectl-1863"), "kubectl-run-12345");
    storage.create(&key, &pod).await.unwrap();

    let before = read_raw(&storage, &key).await;

    gc.scan_and_collect().await.expect("first scan failed");
    let between = read_raw(&storage, &key).await;
    assert_eq!(
        before, between,
        "GC mutated a Succeeded pod with no owner references on the first scan: \
         this would cause needless writes on every reconcile loop"
    );

    gc.scan_and_collect().await.expect("second scan failed");
    let after = read_raw(&storage, &key).await;
    assert_eq!(
        before, after,
        "GC mutated a Succeeded pod with no owner references across two scans: \
         hot-loop symptom"
    );
}

/// Same invariant as the Succeeded case, but for a Running pod with no
/// owners. The GC must leave it strictly alone across repeated scans.
#[tokio::test]
async fn test_gc_does_not_touch_running_pod_without_owners() {
    let storage = Arc::new(MemoryStorage::new());
    let gc = GarbageCollector::new(storage.clone());

    let pod = make_pod(
        "standalone-runner",
        "kubectl-1863",
        "running-pod-uid-stable",
        Phase::Running,
        None,
    );
    let key = build_key("pods", Some("kubectl-1863"), "standalone-runner");
    storage.create(&key, &pod).await.unwrap();

    let before = read_raw(&storage, &key).await;

    gc.scan_and_collect().await.expect("first scan failed");
    let between = read_raw(&storage, &key).await;
    assert_eq!(
        before, between,
        "GC mutated a Running pod with no owner references on the first scan"
    );

    gc.scan_and_collect().await.expect("second scan failed");
    let after = read_raw(&storage, &key).await;
    assert_eq!(
        before, after,
        "GC mutated a Running pod with no owner references across two scans"
    );
}

/// Classification check (orphan-detection equivalent path).
///
/// `find_orphans` is private; instead we drive `scan_and_collect` and
/// observe the externally visible behaviour: a pod with no owner refs is
/// never classified as an orphan, therefore is never reaped, therefore
/// is still present (and still byte-equal) after a scan. Tracked update
/// and delete counters confirm the GC did not mutate or remove the pod.
#[tokio::test]
async fn test_gc_orphan_detection_skips_pods_with_no_owner_refs() {
    let mem = Arc::new(MemoryStorage::new());
    let storage = Arc::new(CountingStorage::new(mem.clone()));
    let gc = GarbageCollector::new(storage.clone());

    let pod = make_pod(
        "no-owners-pod",
        "kubectl-1863",
        "no-owners-uid-stable",
        Phase::Running,
        None,
    );
    let key = build_key("pods", Some("kubectl-1863"), "no-owners-pod");
    mem.create(&key, &pod).await.unwrap();

    let before = read_raw(&mem, &key).await;
    gc.scan_and_collect().await.expect("scan failed");
    let after = read_raw(&mem, &key).await;

    assert_eq!(
        before, after,
        "GC mutated a pod with no owner references — orphan classification \
         should have skipped it entirely"
    );
    assert_eq!(
        storage.delete_count(),
        0,
        "GC issued a DELETE for a pod with no owner references"
    );
    assert_eq!(
        storage.update_count(),
        0,
        "GC issued an UPDATE for a pod with no owner references"
    );
}

/// Orphan reap is idempotent across repeated scans.
///
/// Setup: one pod whose only ownerReference points to a ReplicaSet that
/// does not exist in storage. Expectation under post-Unit-14 semantics:
///
///   - First `scan_and_collect` reaps the pod (one DELETE).
///   - Second `scan_and_collect` is a no-op: the pod is already gone, so
///     `find_orphans` returns empty and `delete_orphan` is never invoked.
///
/// Anti-symptoms we are guarding against:
///
///   - Re-delete of an already-deleted key (would surface as a second
///     DELETE and an `Error::NotFound` failure_count++ in the GC loop).
///   - Infinite retry on the missing owner (hot-loop).
///   - Resurrection (some mutation re-inserting the pod).
#[tokio::test]
async fn test_gc_orphan_reap_is_idempotent() {
    let mem = Arc::new(MemoryStorage::new());
    let storage = Arc::new(CountingStorage::new(mem.clone()));
    let gc = GarbageCollector::new(storage.clone());

    let owner_ref = OwnerReference {
        api_version: "apps/v1".to_string(),
        kind: "ReplicaSet".to_string(),
        name: "missing-rs".to_string(),
        uid: "rs-uid-never-existed".to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    };
    let pod = make_pod(
        "orphan-pod",
        "kubectl-1863",
        "orphan-pod-uid-stable",
        Phase::Running,
        Some(vec![owner_ref]),
    );
    let key = build_key("pods", Some("kubectl-1863"), "orphan-pod");
    mem.create(&key, &pod).await.unwrap();

    assert_eq!(mem.len(), 1, "precondition: pod is in storage");

    gc.scan_and_collect().await.expect("first scan failed");
    assert!(
        mem.get::<Value>(&key).await.is_err(),
        "first scan must reap the orphan (post-Unit-14)"
    );
    let after_first = storage.delete_count();
    assert_eq!(
        after_first, 1,
        "first scan must issue exactly one DELETE, got {}",
        after_first
    );

    gc.scan_and_collect()
        .await
        .expect("second scan failed — possible infinite-retry path on already-deleted orphan");

    assert_eq!(
        storage.delete_count(),
        1,
        "second scan must not re-DELETE an already-reaped orphan; \
         total DELETE calls across both scans = {}",
        storage.delete_count()
    );
    assert!(
        mem.get::<Value>(&key).await.is_err(),
        "orphan must remain deleted; resurrection bug"
    );
}
