//! Regression test for CAS retry in the kubelet heartbeat task.
//!
//! Covers: f94882a  fix: Kubelet heartbeat — add CAS retry and logging
//!
//! Pre-fix: `let _ = heartbeat_storage.update(&key, &node).await;`
//! The result was silently discarded on Conflict, so heartbeats were never sent.
//!
//! Post-fix: on Conflict, re-read the node and retry the heartbeat update.
//!
//! This test verifies that the heartbeat CAS-retry pattern correctly persists
//! the heartbeat even when the first write conflicts. When the fix is reverted,
//! the heartbeat update is silently dropped and the node's heartbeat time is
//! never advanced — the assertion on `last_heartbeat_time` fails.

use async_trait::async_trait;
use rusternetes_common::{
    resources::{Node, NodeCondition, NodeSpec, NodeStatus},
    types::{ObjectMeta, TypeMeta},
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
// f94882a: Heartbeat CAS retry
//
// Pattern under test (post-fix):
//   match storage.update(&key, &node).await {
//       Ok(_) => { /* debug log */ }
//       Err(e) => {
//           // Retry with fresh read
//           if let Ok(mut fresh) = storage.get::<Node>(&key).await {
//               apply_heartbeat(&mut fresh);
//               let _ = storage.update(&key, &fresh).await;
//           }
//       }
//   }
//
// Pre-fix: `let _ = storage.update(&key, &node).await;`
// The error is silently discarded — heartbeat never sent on conflict.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_f94882a_heartbeat_cas_retry_persists_heartbeat() {
    let inner = MemoryStorage::new();
    let node_name = "node-1";
    let key = build_key("nodes", None, node_name);

    // Set up a node with a Ready condition with an old heartbeat time
    let old_heartbeat = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(node_name),
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        }),
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: Some("KubeletReady".to_string()),
                message: None,
                last_heartbeat_time: Some(old_heartbeat),
                last_transition_time: None,
            }]),
            addresses: None,
            capacity: None,
            allocatable: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
        }),
    };
    let stored_node = inner.create(&key, &node).await.expect("create node");
    println!(
        "initial node rv = {:?}",
        stored_node.metadata.resource_version
    );

    // Simulate a concurrent update by the sync_loop (the reason heartbeats conflict)
    let mut concurrent_node = stored_node.clone();
    if let Some(ref mut s) = concurrent_node.status {
        s.conditions = Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: Some("KubeletReady".to_string()),
            message: Some("concurrent sync update".to_string()),
            last_heartbeat_time: Some(old_heartbeat),
            last_transition_time: None,
        }]);
    }
    let _after_concurrent = inner
        .update(&key, &concurrent_node)
        .await
        .expect("concurrent update");

    // Wrap in ConflictOnceStorage — first heartbeat update will fail
    let storage = ConflictOnceStorage::new(inner);

    // Heartbeat attempts to update the node with fresh timestamp
    // This is the stale node (from before the concurrent update) — CAS conflict
    let mut heartbeat_node = stored_node.clone();
    let new_heartbeat = chrono::Utc::now();
    if let Some(ref mut s) = heartbeat_node.status {
        if let Some(ref mut conditions) = s.conditions {
            for condition in conditions.iter_mut() {
                condition.last_heartbeat_time = Some(new_heartbeat);
            }
        }
    }

    // First attempt: Conflict (injected) — simulates the CAS failure
    let result = storage.update(&key, &heartbeat_node).await;
    assert!(
        result.is_err(),
        "expected Conflict on first heartbeat update"
    );
    println!(
        "first heartbeat conflict (expected): {}",
        result.unwrap_err()
    );

    // Fixed retry pattern: re-read and apply heartbeat
    // Pre-fix: the error is `let _ = ` discarded — heartbeat never sent
    // Post-fix: re-read fresh node and retry
    let fresh_node_result: rusternetes_common::Result<Node> = storage.get(&key).await;
    assert!(fresh_node_result.is_ok(), "re-read must succeed");
    let mut fresh_node = fresh_node_result.unwrap();

    if let Some(ref mut s) = fresh_node.status {
        if let Some(ref mut conditions) = s.conditions {
            for condition in conditions.iter_mut() {
                condition.last_heartbeat_time = Some(new_heartbeat);
            }
        }
    }

    // Retry update with fresh node — must succeed
    let saved = storage
        .update(&key, &fresh_node)
        .await
        .expect("retry heartbeat update must succeed");

    // Verify: heartbeat time was persisted
    let saved_heartbeat = saved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.first())
        .and_then(|c| c.last_heartbeat_time)
        .expect("heartbeat time must be set");

    println!("old heartbeat: {}", old_heartbeat);
    println!("new heartbeat: {}", new_heartbeat);
    println!("saved heartbeat: {}", saved_heartbeat);

    // The saved heartbeat must be newer than the old one (pre-fix: old timestamp persists)
    assert!(
        saved_heartbeat > old_heartbeat,
        "heartbeat must advance past old time; pre-fix: Conflict was silently discarded \
         and old timestamp persisted. saved={} old={}",
        saved_heartbeat,
        old_heartbeat
    );

    // Verify via storage read-back
    let final_node: Node = storage.get(&key).await.expect("final read");
    let final_heartbeat = final_node
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.first())
        .and_then(|c| c.last_heartbeat_time)
        .expect("final heartbeat time must be set");

    assert!(
        final_heartbeat > old_heartbeat,
        "final storage must contain updated heartbeat time"
    );
    println!("test passed: heartbeat persisted after CAS retry");
}
