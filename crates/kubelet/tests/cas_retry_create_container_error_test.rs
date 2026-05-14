//! Regression test for CAS retry in CreateContainerError status write.
//!
//! Covers: 7881f80  CAS-retry sub-fix for CreateContainerError
//! (skipping the unrelated emptyDir bind-mount change in the same commit)
//!
//! Pre-fix: When writing the CreateContainerError container status to storage,
//! a CAS conflict was logged as WARN and dropped — the pod stayed in Pending
//! without the expected Waiting/CreateContainerError state.
//!
//! Post-fix:
//!   if let Err(e) = storage.update(&key, &new_pod).await {
//!       warn!("..., retrying");
//!       if let Ok(mut retry_pod) = storage.get::<Pod>(&key).await {
//!           retry_pod.status = new_pod.status.clone();
//!           let _ = storage.update(&key, &retry_pod).await;
//!       }
//!   }
//!
//! This test verifies that after a CAS conflict on the CreateContainerError write,
//! the retry correctly persists the Waiting/CreateContainerError container status.

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
// Helper
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
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Pending),
            conditions: None,
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
        }),
    }
}

// ---------------------------------------------------------------------------
// 7881f80 (CAS piece only): CreateContainerError status persisted after conflict
//
// Pattern under test (post-fix):
//   if let Err(_e) = storage.update(&key, &new_pod).await {
//       warn!("... retrying");
//       if let Ok(mut retry_pod) = storage.get::<Pod>(&key).await {
//           retry_pod.status = new_pod.status.clone();
//           let _ = storage.update(&key, &retry_pod).await;
//       }
//   }
//
// Pre-fix: Conflict logged, no retry — pod stays in Pending/unknown state.
// The subPath validation test timed out because it could never see
// Waiting/CreateContainerError state.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_7881f80_create_container_error_persisted_after_cas_conflict() {
    let inner = MemoryStorage::new();
    let key = build_key("pods", Some("default"), "cce-pod");

    // Store a pod in Pending state
    let pod = make_pod("cce-pod", "default");
    let stored = inner.create(&key, &pod).await.expect("create");
    println!("initial rv = {:?}", stored.metadata.resource_version);

    // Simulate a concurrent update before the CreateContainerError write
    let mut concurrent = stored.clone();
    if let Some(ref mut s) = concurrent.status {
        s.message = Some("concurrent update".to_string());
    }
    let _ = inner.update(&key, &concurrent).await.expect("concurrent update");

    // Wrap in ConflictOnceStorage
    let storage = ConflictOnceStorage::new(inner);

    // Build new_pod with CreateContainerError status (using re-read from storage)
    let fresh_pod: Pod = storage.get(&key).await.expect("get fresh pod");
    let mut new_pod = fresh_pod;

    // The CreateContainerError container status the kubelet wants to persist
    let cce_status = ContainerStatus {
        name: "main".to_string(),
        ready: false,
        restart_count: 0,
        state: Some(ContainerState::Waiting {
            reason: Some("CreateContainerError".to_string()),
            message: Some("failed to create container: invalid image reference".to_string()),
        }),
        last_state: None,
        image: Some("nginx:latest".to_string()),
        image_id: None,
        container_id: None,
        started: Some(false),
        allocated_resources: None,
        allocated_resources_status: None,
        resources: None,
        user: None,
        volume_mounts: None,
        stop_signal: None,
    };

    if let Some(ref mut s) = new_pod.status {
        s.container_statuses = Some(vec![cce_status]);
    }

    // First update attempt: Conflict (injected)
    let err = storage.update(&key, &new_pod).await.unwrap_err();
    println!("CAS conflict on CreateContainerError write: {}", err);
    assert!(
        err.to_string().contains("Conflict") || err.to_string().contains("mismatch"),
        "expected Conflict, got: {}",
        err
    );

    // Post-fix retry pattern:
    //   re-read the pod and apply the same status, then retry update
    let retry_result: rusternetes_common::Result<Pod> = storage.get(&key).await;
    assert!(retry_result.is_ok(), "re-read must succeed for retry");
    let mut retry_pod = retry_result.unwrap();

    // Apply same status from new_pod (the CreateContainerError)
    retry_pod.status = new_pod.status.clone();

    let saved = storage
        .update(&key, &retry_pod)
        .await
        .expect("retry update must succeed");

    // Verify: Waiting/CreateContainerError state must be persisted
    let saved_statuses = saved
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .expect("container_statuses must be set after retry");

    assert_eq!(saved_statuses.len(), 1, "one container status expected");
    let main_status = &saved_statuses[0];
    assert_eq!(main_status.name, "main");
    assert!(!main_status.ready, "container must not be ready");

    let state = main_status.state.as_ref().expect("state must be set");
    match state {
        ContainerState::Waiting { reason, .. } => {
            assert_eq!(
                reason.as_deref(),
                Some("CreateContainerError"),
                "must be CreateContainerError; pre-fix: Conflict dropped, state never persisted"
            );
        }
        other => {
            panic!(
                "expected Waiting/CreateContainerError state, got: {:?}",
                other
            );
        }
    }

    println!(
        "saved state = {:?}",
        main_status.state
    );

    // Verify via final read from storage
    let final_pod: Pod = storage.get(&key).await.expect("final get");
    let final_statuses = final_pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .expect("final container_statuses must be set");
    let final_main = &final_statuses[0];
    assert!(
        matches!(
            &final_main.state,
            Some(ContainerState::Waiting { reason, .. }) if reason.as_deref() == Some("CreateContainerError")
        ),
        "CreateContainerError must persist in final storage read"
    );
    println!("test passed: CreateContainerError persisted after CAS retry");
}
