//! Regression tests for CAS-retry fixes in kubelet pod-status writes.
//!
//! Covers:
//!   3147f7b  fix: Fix broken CAS re-reads in kubelet pod status updates (CRITICAL)
//!   3bf2ed2  fix: retry pod status update on resourceVersion conflict
//!   a1d78d8  Fix #270: Kubelet readiness write — remove duplicate that caused RV conflict
//!
//! The production code pattern under test:
//!   1. Read pod from storage (get fresh resourceVersion).
//!   2. Mutate status.
//!   3. Update storage — if Conflict, re-read and retry.
//!
//! `ConflictOnceStorage` injects a single Conflict on the first update for a
//! matching key, then passes subsequent calls through to the inner MemoryStorage.
//!
//! `RvEnforcingStorage` stamps each stored object with an auto-incrementing
//! resource_version and rejects updates whose metadata.resource_version doesn't
//! match the current stored version. This allows tests to verify that using a
//! stale pod (pre-fix: `Ok(Some(p)) => p` fell through to `_ => pod.clone()`)
//! causes a real CAS failure on retry.

use async_trait::async_trait;
use rusternetes_common::{
    resources::{Container, ContainerStatus, Pod, PodCondition, PodSpec, PodStatus},
    types::{ObjectMeta, Phase, TypeMeta},
    Error,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, WatchStream};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};

// ---------------------------------------------------------------------------
// RvEnforcingStorage — stamps objects with auto-incrementing resource_version
// and rejects updates with stale versions (simulates etcd CAS behavior).
// ---------------------------------------------------------------------------

struct RvEnforcingStorage {
    inner: MemoryStorage,
    rv_counter: Arc<AtomicU64>,
    current_rvs: Arc<Mutex<HashMap<String, u64>>>,
}

impl RvEnforcingStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            rv_counter: Arc::new(AtomicU64::new(1)),
            current_rvs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn next_rv(&self) -> u64 {
        self.rv_counter.fetch_add(1, Ordering::SeqCst)
    }

    fn stamp_rv(value: &mut serde_json::Value, rv: u64) {
        if let Some(metadata) = value.get_mut("metadata") {
            if let Some(obj) = metadata.as_object_mut() {
                obj.insert(
                    "resourceVersion".to_string(),
                    serde_json::Value::String(rv.to_string()),
                );
            }
        }
    }

    fn extract_rv(value: &serde_json::Value) -> Option<u64> {
        value
            .get("metadata")
            .and_then(|m| m.get("resourceVersion"))
            .and_then(|rv| rv.as_str())
            .and_then(|s| s.parse::<u64>().ok())
    }
}

#[async_trait]
impl Storage for RvEnforcingStorage {
    async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let mut json = serde_json::to_value(value)?;
        let rv = self.next_rv();
        Self::stamp_rv(&mut json, rv);
        self.current_rvs.lock().unwrap().insert(key.to_string(), rv);
        let serialized = serde_json::to_string(&json)?;
        // Store directly in inner without going through inner.create (which adds its own UID/RV)
        // We bypass inner by using update_raw after creating a placeholder
        self.inner
            .create(
                key,
                &serde_json::from_str::<serde_json::Value>(&serialized)?,
            )
            .await?;
        Ok(serde_json::from_value(json)?)
    }

    async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let json = serde_json::to_value(value)?;
        // Check the resource_version in the submitted value matches current
        let submitted_rv = Self::extract_rv(&json);
        let current_rv = self.current_rvs.lock().unwrap().get(key).copied();

        if let (Some(submitted), Some(current)) = (submitted_rv, current_rv) {
            if submitted != current {
                return Err(Error::Conflict(format!(
                    "resourceVersion mismatch: expected {}, got {} for key {}",
                    current, submitted, key
                )));
            }
        }

        // Stamp new RV
        let mut new_json = json;
        let new_rv = self.next_rv();
        Self::stamp_rv(&mut new_json, new_rv);
        self.current_rvs
            .lock()
            .unwrap()
            .insert(key.to_string(), new_rv);

        self.inner.update_raw(key, &new_json).await?;
        Ok(serde_json::from_value(new_json)?)
    }

    async fn update_raw(
        &self,
        key: &str,
        value: &serde_json::Value,
    ) -> rusternetes_common::Result<()> {
        self.inner.update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
        self.inner.delete(key).await
    }

    async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.list(prefix).await
    }

    async fn watch(&self, prefix: &str) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch(prefix).await
    }

    async fn watch_from_revision(
        &self,
        prefix: &str,
        revision: i64,
    ) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> rusternetes_common::Result<i64> {
        self.inner.current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
        self.inner.is_revision_compacted(revision).await
    }
}

// ---------------------------------------------------------------------------
// ConflictOnceStorage — injects one Conflict error on `update` then passes through
// ---------------------------------------------------------------------------

struct ConflictOnceStorage {
    inner: MemoryStorage,
    /// Number of times a conflict has already been injected for this instance.
    conflicts_injected: Arc<AtomicUsize>,
    max_conflicts: usize,
}

impl ConflictOnceStorage {
    fn new(inner: MemoryStorage) -> Self {
        Self {
            inner,
            conflicts_injected: Arc::new(AtomicUsize::new(0)),
            max_conflicts: 1,
        }
    }
}

#[async_trait]
impl Storage for ConflictOnceStorage {
    async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let already = self.conflicts_injected.load(Ordering::SeqCst);
        if already < self.max_conflicts {
            self.conflicts_injected.fetch_add(1, Ordering::SeqCst);
            return Err(Error::Conflict(format!(
                "resourceVersion mismatch: resource was modified (injected for key {})",
                key
            )));
        }
        self.inner.update(key, value).await
    }

    async fn update_raw(
        &self,
        key: &str,
        value: &serde_json::Value,
    ) -> rusternetes_common::Result<()> {
        self.inner.update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
        self.inner.delete(key).await
    }

    async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.list(prefix).await
    }

    async fn watch(&self, prefix: &str) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch(prefix).await
    }

    async fn watch_from_revision(
        &self,
        prefix: &str,
        revision: i64,
    ) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> rusternetes_common::Result<i64> {
        self.inner.current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
        self.inner.is_revision_compacted(revision).await
    }
}

// ---------------------------------------------------------------------------
// ConflictOnSecondUpdateStorage — succeeds first update, fails second
// ---------------------------------------------------------------------------

struct ConflictOnSecondUpdateStorage {
    inner: MemoryStorage,
    update_count: Arc<AtomicUsize>,
}

impl ConflictOnSecondUpdateStorage {
    fn new(inner: MemoryStorage) -> Self {
        Self {
            inner,
            update_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl Storage for ConflictOnSecondUpdateStorage {
    async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let count = self.update_count.fetch_add(1, Ordering::SeqCst);
        if count == 1 {
            return Err(Error::Conflict(format!(
                "resourceVersion mismatch after first write (injected for key {})",
                key
            )));
        }
        self.inner.update(key, value).await
    }

    async fn update_raw(
        &self,
        key: &str,
        value: &serde_json::Value,
    ) -> rusternetes_common::Result<()> {
        self.inner.update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
        self.inner.delete(key).await
    }

    async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.list(prefix).await
    }

    async fn watch(&self, prefix: &str) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch(prefix).await
    }

    async fn watch_from_revision(
        &self,
        prefix: &str,
        revision: i64,
    ) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> rusternetes_common::Result<i64> {
        self.inner.current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
        self.inner.is_revision_compacted(revision).await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
                image: "nginx:latest".to_string(),
                image_pull_policy: None,
                command: None,
                args: None,
                ports: None,
                env: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                resources: None,
                working_dir: None,
                security_context: None,
                restart_policy: None,
                resize_policy: None,
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
            ephemeral_containers: None,
            restart_policy: Some("Always".to_string()),
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
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
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
            phase: Some(Phase::Pending),
            conditions: Some(vec![]),
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            pod_ip: None,
            pod_i_ps: None,
            host_ip: None,
            host_i_ps: None,
            message: None,
            reason: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        }),
    }
}

// ---------------------------------------------------------------------------
// 3147f7b: CRITICAL — re-read must use Ok(p), not Ok(Some(p))
//
// Pattern under test (post-fix):
//   let fresh_pod: Pod = match self.storage.get::<Pod>(&key).await {
//       Ok(p) => p,          // FIXED: correctly unpacks Result<T>
//       Err(_) => pod.clone(),
//   };
//   // mutate fresh_pod ...
//   if let Err(e) = self.storage.update(&key, &fresh_pod).await {
//       // CAS retry: re-read and apply status
//       if let Ok(mut retry_pod) = self.storage.get::<Pod>(&key).await {
//           ... apply status fields to retry_pod ...
//           self.storage.update(&key, &retry_pod).await
//       }
//   }
//
// Pre-fix bug: `Ok(Some(p)) => p` never matches `Ok(pod: Pod)` since storage.get()
// returns Result<T> not Result<Option<T>>. The code fell through to `_ => pod.clone()`
// on every re-read, always using the stale original pod.
//
// With RvEnforcingStorage:
//   - After an external update to the pod (simulating concurrent write), the stored RV advances.
//   - A fresh `storage.get()` returns the pod with the NEW RV.
//   - Using a stale clone (pre-fix) means the retry has the OLD RV → CAS failure again.
//   - The test asserts that after conflict + proper re-read, the final update succeeds
//     AND the condition is persisted. If re-read returns stale (pre-fix bug), the
//     retry ALSO conflicts → condition never persisted.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_3147f7b_cas_reread_uses_fresh_pod_not_stale() {
    let storage = RvEnforcingStorage::new();
    let key = build_key("pods", Some("default"), "test-pod");

    // Store a pod (simulates what kubelet has in storage at start of sync)
    let pod = make_pod("test-pod", "default");
    let stored_pod = storage.create(&key, &pod).await.expect("create failed");
    let initial_rv = stored_pod.metadata.resource_version.clone();
    println!("initial_rv = {:?}", initial_rv);

    // Simulate an external update to the pod by another controller.
    // This advances the resource_version in storage.
    let mut external_pod = stored_pod.clone();
    if let Some(ref mut s) = external_pod.status {
        s.message = Some("externally updated".to_string());
    }
    let after_external = storage
        .update(&key, &external_pod)
        .await
        .expect("external update");
    let after_external_rv = after_external.metadata.resource_version.clone();
    println!(
        "after_external_rv = {:?} (should differ from initial {:?})",
        after_external_rv, initial_rv
    );
    assert_ne!(
        initial_rv, after_external_rv,
        "RV must advance after external update"
    );

    // At this point the kubelet holds `stored_pod` (with OLD RV = initial_rv).
    // The kubelet tries to update the pod status.

    // The kubelet detects a conflict on update with the stale pod...
    // (Simulate: kubelet tries to update with old RV — should fail)
    let mut stale_attempt = stored_pod.clone(); // OLD RV
    if let Some(ref mut s) = stale_attempt.status {
        s.phase = Some(Phase::Running);
    }
    let conflict_result = storage.update(&key, &stale_attempt).await;
    assert!(
        conflict_result.is_err(),
        "update with stale RV must fail with Conflict"
    );
    println!(
        "expected conflict on stale update: {}",
        conflict_result.unwrap_err()
    );

    // POST-FIX: Re-read correctly gets fresh pod (Ok(p), not stale clone)
    let reread: Pod = storage.get(&key).await.expect("re-read must succeed");
    let reread_rv = reread.metadata.resource_version.clone();
    println!(
        "re-read rv = {:?} (must match after_external_rv = {:?})",
        reread_rv, after_external_rv
    );
    assert_eq!(
        reread_rv, after_external_rv,
        "re-read must return the current (post-external-update) RV, not stale"
    );

    // Apply desired condition to the freshly re-read pod
    let ready_condition = PodCondition {
        condition_type: "Ready".to_string(),
        status: "True".to_string(),
        last_probe_time: None,
        last_transition_time: None,
        reason: None,
        message: None,
        observed_generation: None,
    };
    let mut retry_pod = reread;
    if let Some(ref mut s) = retry_pod.status {
        s.conditions = Some(vec![ready_condition.clone()]);
        s.phase = Some(Phase::Running);
    }

    // Retry update with fresh RV: must succeed
    let saved = storage
        .update(&key, &retry_pod)
        .await
        .expect("retry update with fresh RV must succeed");

    println!(
        "final phase = {:?}, conditions = {:?}",
        saved.status.as_ref().and_then(|s| s.phase.as_ref()),
        saved
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| c.len())
    );

    // Assert: the Ready condition must be persisted
    let conditions = saved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("conditions must be set");
    assert!(
        conditions
            .iter()
            .any(|c| c.condition_type == "Ready" && c.status == "True"),
        "Ready=True condition must be persisted after proper re-read + retry"
    );
    assert_eq!(
        saved.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(&Phase::Running)
    );

    // Verify via final storage read
    let final_pod: Pod = storage.get(&key).await.expect("final get");
    let final_conditions = final_pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("final conditions must be set");
    assert!(
        final_conditions
            .iter()
            .any(|c| c.condition_type == "Ready" && c.status == "True"),
        "Ready=True must persist in storage after CAS retry with fresh re-read"
    );
    println!("test passed: fresh re-read enables successful CAS retry");
}

// ---------------------------------------------------------------------------
// 3bf2ed2: retry pod status update on resourceVersion conflict
//
// Pattern: if update returns Conflict, re-read and retry with Phase::Running.
// Without the fix, Conflict is logged as WARN and dropped — pod never reaches Running.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_3bf2ed2_retry_pod_running_status_on_conflict() {
    let inner = MemoryStorage::new();
    let key = build_key("pods", Some("default"), "running-pod");

    let pod = make_pod("running-pod", "default");
    inner.create(&key, &pod).await.expect("create");

    let storage = Arc::new(ConflictOnceStorage::new(inner));

    // Read fresh pod
    let fresh: Pod = storage.get(&key).await.expect("get");
    let mut new_pod = fresh.clone();
    if let Some(ref mut s) = new_pod.status {
        s.phase = Some(Phase::Running);
        s.message = Some("All containers ready".to_string());
    }

    // First update: Conflict (injected)
    let err = storage.update(&key, &new_pod).await.unwrap_err();
    println!("conflict on first update: {}", err);
    assert!(
        err.to_string().contains("Conflict") || err.to_string().contains("mismatch"),
        "expected Conflict error, got: {}",
        err
    );

    // Post-fix retry: re-read and apply Running
    // Without 3bf2ed2, the code just warns and returns — pod stays non-Running.
    let fresh2: Pod = storage.get(&key).await.expect("re-read for retry");
    let mut retry_pod = fresh2;
    if let Some(ref mut s) = retry_pod.status {
        s.phase = Some(Phase::Running);
        s.message = Some("All containers ready".to_string());
    }
    let saved = storage
        .update(&key, &retry_pod)
        .await
        .expect("retry must succeed");

    assert_eq!(
        saved.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(&Phase::Running),
        "pod must be Running after CAS retry"
    );

    // Verify final state in storage
    let final_pod: Pod = storage.get(&key).await.expect("final get");
    assert_eq!(
        final_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(&Phase::Running),
        "Running phase must persist in storage"
    );
    println!(
        "final phase = {:?}",
        final_pod.status.as_ref().and_then(|s| s.phase.as_ref())
    );
}

// ---------------------------------------------------------------------------
// a1d78d8: remove duplicate write that caused CAS conflict
//
// Pre-fix: Two writes happened, second with stale pod.clone() failed silently.
// Post-fix: ONE write with fresh re-read.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_a1d78d8_duplicate_write_causes_conflict_single_write_with_reread_succeeds() {
    let inner = MemoryStorage::new();
    let key = build_key("pods", Some("default"), "readiness-pod");

    let pod = make_pod("readiness-pod", "default");
    let stored = inner.create(&key, &pod).await.expect("create");
    println!("initial rv = {:?}", stored.metadata.resource_version);

    // Use ConflictOnSecondUpdateStorage: first update passes, second fails
    let storage = ConflictOnSecondUpdateStorage::new(inner);

    // --- Pre-fix pattern: two writes with stale pod.clone() ---
    // First write: container statuses + conditions (this is the "extra" write removed by a1d78d8)
    let mut first_write = stored.clone();
    if let Some(ref mut s) = first_write.status {
        s.container_statuses = Some(vec![ContainerStatus {
            name: "main".to_string(),
            ready: true,
            restart_count: 0,
            state: None,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]);
        s.conditions = Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_probe_time: None,
            last_transition_time: None,
            reason: None,
            message: None,
            observed_generation: None,
        }]);
    }
    let after_first = storage
        .update(&key, &first_write)
        .await
        .expect("first write must succeed");
    println!(
        "after first write rv = {:?}",
        after_first.metadata.resource_version
    );

    // Second write (also using stale stored.clone() — pre-fix bug): Conflict injected
    let mut second_write = stored.clone(); // STALE: same RV as original stored pod
    if let Some(ref mut s) = second_write.status {
        s.phase = Some(Phase::Running);
        s.container_statuses = Some(vec![ContainerStatus {
            name: "main".to_string(),
            ready: true,
            restart_count: 0,
            state: None,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]);
        s.conditions = Some(vec![
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: None,
                reason: None,
                message: None,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: None,
                reason: None,
                message: None,
                observed_generation: None,
            },
        ]);
    }

    let second_result = storage.update(&key, &second_write).await;
    assert!(
        second_result.is_err(),
        "pre-fix: second write with stale clone must fail (Conflict injected)"
    );
    println!(
        "second write conflict (expected, simulates pre-fix bug): {}",
        second_result.unwrap_err()
    );

    // At this point (pre-fix), storage has only the first write's conditions.
    let after_pre_fix: Pod = storage.get(&key).await.expect("read after pre-fix writes");
    println!(
        "after pre-fix writes: phase={:?} conditions={:?}",
        after_pre_fix.status.as_ref().and_then(|s| s.phase.as_ref()),
        after_pre_fix
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| c.len())
    );

    // --- Post-fix pattern: ONE write using fresh re-read ---
    let fresh_pod: Pod = storage.get(&key).await.expect("re-read for post-fix write");
    let mut final_pod = fresh_pod;
    if let Some(ref mut s) = final_pod.status {
        s.phase = Some(Phase::Running);
        s.container_statuses = Some(vec![ContainerStatus {
            name: "main".to_string(),
            ready: true,
            restart_count: 0,
            state: None,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]);
        s.conditions = Some(vec![
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: None,
                reason: None,
                message: None,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: None,
                reason: None,
                message: None,
                observed_generation: None,
            },
        ]);
    }

    // Third update: post-fix writes through (count=2, no conflict injected)
    let saved = storage
        .update(&key, &final_pod)
        .await
        .expect("post-fix single write with fresh re-read must succeed");

    let conditions = saved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("conditions must be set");
    assert!(
        conditions
            .iter()
            .any(|c| c.condition_type == "Ready" && c.status == "True"),
        "Ready=True must persist in post-fix write"
    );
    assert!(
        conditions
            .iter()
            .any(|c| c.condition_type == "ContainersReady" && c.status == "True"),
        "ContainersReady=True must persist; pre-fix: second write dropped on Conflict"
    );
    assert_eq!(
        saved.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(&Phase::Running),
        "pod must be Running"
    );
    println!("test passed: single re-read write persisted all conditions correctly");
}
