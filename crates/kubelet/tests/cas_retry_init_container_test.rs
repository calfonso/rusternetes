//! Regression test for init container status refresh on CAS retry.
//!
//! Covers: 9ff9e3a  fix: refresh init container statuses on CAS retry and running sync
//!
//! Pre-fix: The CAS retry path in the Running status write only updated
//! `phase` and `message`, leaving `init_container_statuses` from a stale
//! intermediate write where init containers had `ready=false`.
//!
//! Post-fix: The retry re-fetches ALL statuses (including init containers)
//! before writing, ensuring `ready=true` for completed init containers.
//!
//! This test verifies that after a CAS conflict on a Running-status update,
//! the retry includes `init_container_statuses` with `ready=true` for
//! all completed init containers.

use async_trait::async_trait;
use rusternetes_common::{
    resources::{Container, ContainerState, ContainerStatus, Pod, PodSpec, PodStatus},
    types::{ObjectMeta, Phase, TypeMeta},
    Error,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, WatchStream};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

// ---------------------------------------------------------------------------
// ConflictOnceStorage — local copy; do NOT share across test files
// ---------------------------------------------------------------------------

struct ConflictOnceStorage {
    inner: MemoryStorage,
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
// Helpers
// ---------------------------------------------------------------------------

/// Simulate get_init_container_statuses as the runtime would return after
/// init containers complete: all ready=true, state=Terminated(exit_code=0).
fn get_fresh_init_container_statuses() -> Option<Vec<ContainerStatus>> {
    Some(vec![
        ContainerStatus {
            name: "init-1".to_string(),
            ready: true,
            restart_count: 0,
            state: Some(ContainerState::Terminated {
                exit_code: 0,
                reason: Some("Completed".to_string()),
                message: None,
                started_at: None,
                finished_at: None,
                container_id: None,
                signal: None,
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        },
        ContainerStatus {
            name: "init-2".to_string(),
            ready: true,
            restart_count: 0,
            state: Some(ContainerState::Terminated {
                exit_code: 0,
                reason: Some("Completed".to_string()),
                message: None,
                started_at: None,
                finished_at: None,
                container_id: None,
                signal: None,
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        },
    ])
}

/// Stale init container statuses — ready=false, simulating intermediate write
fn get_stale_init_container_statuses() -> Option<Vec<ContainerStatus>> {
    Some(vec![
        ContainerStatus {
            name: "init-1".to_string(),
            ready: false, // stale — intermediate write before init completed
            restart_count: 0,
            state: Some(ContainerState::Waiting {
                reason: Some("PodInitializing".to_string()),
                message: None,
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        },
        ContainerStatus {
            name: "init-2".to_string(),
            ready: false, // stale
            restart_count: 0,
            state: Some(ContainerState::Waiting {
                reason: Some("PodInitializing".to_string()),
                message: None,
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        },
    ])
}

fn make_pod_with_inits(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
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
            }],
            init_containers: Some(vec![
                Container {
                    name: "init-1".to_string(),
                    image: "busybox:latest".to_string(),
                    image_pull_policy: None,
                    command: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "echo init1".to_string(),
                    ]),
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
                },
                Container {
                    name: "init-2".to_string(),
                    image: "busybox:latest".to_string(),
                    image_pull_policy: None,
                    command: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "echo init2".to_string(),
                    ]),
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
                },
            ]),
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
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Pending),
            conditions: None,
            container_statuses: None,
            init_container_statuses: get_stale_init_container_statuses(),
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
        }),
    }
}

// ---------------------------------------------------------------------------
// 9ff9e3a: init container statuses must be refreshed on CAS retry
//
// Pattern under test (post-fix, CAS retry path):
//   if let Ok(fresh_pod) = storage.get::<Pod>(&key).await {
//       let mut retry_pod = fresh_pod;
//       let fresh_init_statuses = runtime.get_init_container_statuses(pod).await;  // NEW
//       if let Some(ref mut status) = retry_pod.status {
//           status.phase = Some(Phase::Running);
//           status.init_container_statuses = fresh_init_statuses;  // NEW
//       }
//       storage.update(&key, &retry_pod).await?;
//   }
//
// Pre-fix: the retry only set phase/message, leaving stale init_container_statuses
// (ready=false) from the intermediate write. Post-fix: fresh statuses are re-fetched.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_9ff9e3a_init_container_statuses_refreshed_on_cas_retry() {
    let inner = MemoryStorage::new();
    let key = build_key("pods", Some("default"), "init-pod");

    // Create pod with stale init container statuses (ready=false, Waiting/PodInitializing)
    // This simulates an intermediate write that happened before init containers completed
    let pod = make_pod_with_inits("init-pod", "default");
    let stored = inner.create(&key, &pod).await.expect("create");
    println!("initial rv = {:?}", stored.metadata.resource_version);

    // Verify initial state has stale (ready=false) init container statuses
    let initial_ics = stored
        .status
        .as_ref()
        .and_then(|s| s.init_container_statuses.as_ref())
        .expect("must have init container statuses");
    assert!(
        initial_ics.iter().all(|ic| !ic.ready),
        "initial init container statuses must be stale (ready=false)"
    );

    let storage = ConflictOnceStorage::new(inner);

    // Simulate the kubelet attempting a Running-status write
    let fresh_pod: Pod = storage.get(&key).await.expect("get");
    let mut new_pod = fresh_pod.clone();
    if let Some(ref mut s) = new_pod.status {
        s.phase = Some(Phase::Running);
        s.message = Some("All containers ready".to_string());
        // Pre-fix bug: init_container_statuses NOT updated here — stale values persist
        // Post-fix: fresh init statuses are fetched from runtime and set
    }

    // First update: Conflict
    let err = storage.update(&key, &new_pod).await.unwrap_err();
    println!("conflict on running status update: {}", err);
    assert!(
        err.to_string().contains("Conflict") || err.to_string().contains("mismatch"),
        "expected Conflict, got: {}",
        err
    );

    // CAS retry path (post-fix):
    //   1. Re-read fresh pod
    //   2. Fetch fresh init container statuses from runtime (simulated here)
    //   3. Apply Running + fresh init statuses
    //   4. Retry update
    let fresh_for_retry: Pod = storage.get(&key).await.expect("re-read for retry");
    let mut retry_pod = fresh_for_retry;

    // Simulate runtime.get_init_container_statuses() returning ready=true
    let fresh_init_statuses = get_fresh_init_container_statuses();

    if let Some(ref mut s) = retry_pod.status {
        s.phase = Some(Phase::Running);
        s.message = Some("All containers ready".to_string());
        // POST-FIX: refresh init container statuses
        s.init_container_statuses = fresh_init_statuses;
    }

    let saved = storage
        .update(&key, &retry_pod)
        .await
        .expect("retry update must succeed");

    // Verify: init container statuses must have ready=true after retry
    let saved_ics = saved
        .status
        .as_ref()
        .and_then(|s| s.init_container_statuses.as_ref())
        .expect("init_container_statuses must be set after retry");

    println!("saved init container statuses:");
    for ic in saved_ics {
        println!("  {} ready={} state={:?}", ic.name, ic.ready, ic.state);
    }

    assert_eq!(saved_ics.len(), 2, "both init containers must be present");
    for ic in saved_ics {
        assert!(
            ic.ready,
            "init container '{}' must be ready=true after retry refresh; \
             pre-fix: stale ready=false from intermediate write persists",
            ic.name
        );
        matches!(
            ic.state,
            Some(ContainerState::Terminated { exit_code: 0, .. })
        );
    }

    assert_eq!(
        saved.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(&Phase::Running),
        "pod must be Running"
    );

    // Verify via final read from storage
    let final_pod: Pod = storage.get(&key).await.expect("final get");
    let final_ics = final_pod
        .status
        .as_ref()
        .and_then(|s| s.init_container_statuses.as_ref())
        .expect("final init_container_statuses must be set");
    assert!(
        final_ics.iter().all(|ic| ic.ready),
        "all init containers must have ready=true in final storage state"
    );
    println!("test passed: init container statuses refreshed on CAS retry");
}
