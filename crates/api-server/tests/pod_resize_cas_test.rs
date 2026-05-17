//! CAS-retry behaviour for the `/pods/{name}/resize` subresource (KEP-1287).
//!
//! Upstream e2e site `common/node/pod_resize.go:753` exercises the in-place
//! resize subresource. The kubelet and admission webhooks may bump a pod's
//! `resourceVersion` between the client's GET and the server's PUT, so the API
//! server must absorb a small number of conflicts by re-reading the latest
//! object and re-applying the resource changes. Before this fix our handler
//! surfaced the storage-level `Error::Conflict` directly to the client (HTTP
//! 409), failing the e2e with `failed to resize pod: resourceVersion mismatch:
//! resource was modified (...)`.
//!
//! This test wraps `MemoryStorage` with a small adapter that fails the first
//! `update()` call with `Error::Conflict` — exactly the conflict the e2e was
//! seeing — and asserts that `apply_pod_resize_with_retry` recovers on the
//! retry and persists the new container resources.

use async_trait::async_trait;
use rusternetes_api_server::handlers::pod_subresources::apply_pod_resize_with_retry;
use rusternetes_common::resources::{Container, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, WatchStream};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Wraps a `MemoryStorage` and fails the first `n` `update()` calls with
/// `Error::Conflict`, mirroring an etcd/rhino `resourceVersion` race. All
/// other methods delegate straight through.
struct ConflictInjectingStorage {
    inner: Arc<MemoryStorage>,
    remaining_conflicts: AtomicUsize,
}

impl ConflictInjectingStorage {
    fn new(inner: Arc<MemoryStorage>, conflicts: usize) -> Self {
        Self {
            inner,
            remaining_conflicts: AtomicUsize::new(conflicts),
        }
    }
}

#[async_trait]
impl Storage for ConflictInjectingStorage {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // Inject a Conflict on the first N updates, then pass through.
        if self
            .remaining_conflicts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n > 0 {
                    Some(n - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            return Err(Error::Conflict(format!(
                "resourceVersion mismatch: resource was modified (expected: 631808, current: 631812) [injected for {}]",
                key
            )));
        }
        self.inner.update(key, value).await
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        self.inner.update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.inner.list(prefix).await
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        self.inner.watch(prefix).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        self.inner.watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> Result<i64> {
        self.inner.current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        self.inner.is_revision_compacted(revision).await
    }
}

fn make_pod_with_cpu(name: &str, namespace: &str, cpu: &str) -> Pod {
    let mut limits = HashMap::new();
    limits.insert("cpu".to_string(), cpu.to_string());
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), cpu.to_string());

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                image: "nginx:latest".to_string(),
                resources: Some(ResourceRequirements {
                    limits: Some(limits),
                    requests: Some(requests),
                    claims: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    }
}

#[tokio::test]
async fn pod_resize_recovers_from_storage_conflict() {
    // Arrange: stored pod has cpu=100m, request resizes to cpu=200m, the first
    // storage write fails with a Conflict (as the kubelet would after racing
    // the resize PUT).
    let inner = Arc::new(MemoryStorage::new());
    let storage = ConflictInjectingStorage::new(inner.clone(), 1);
    let namespace = "default";
    let name = "resize-cas";
    let key = build_key("pods", Some(namespace), name);

    let initial = make_pod_with_cpu(name, namespace, "100m");
    inner.create(&key, &initial).await.unwrap();

    let desired = make_pod_with_cpu(name, namespace, "200m");

    // Act
    let updated = apply_pod_resize_with_retry(&storage, namespace, name, &desired)
        .await
        .expect("resize should succeed after one CAS retry");

    // Assert: resources took effect, status.resize=Proposed was written.
    let updated_cpu = updated
        .spec
        .as_ref()
        .and_then(|s| s.containers.first())
        .and_then(|c| c.resources.as_ref())
        .and_then(|r| r.limits.as_ref())
        .and_then(|l| l.get("cpu"))
        .map(|s| s.as_str());
    assert_eq!(
        updated_cpu,
        Some("200m"),
        "container cpu limit should reflect the resized value"
    );
    assert_eq!(
        updated.status.as_ref().and_then(|s| s.resize.clone()),
        Some("Proposed".to_string()),
        "status.resize should be Proposed so the kubelet picks up the change (KEP-1287)"
    );

    // And the persisted pod matches what was returned.
    let stored: Pod = inner.get(&key).await.unwrap();
    let stored_cpu = stored
        .spec
        .as_ref()
        .and_then(|s| s.containers.first())
        .and_then(|c| c.resources.as_ref())
        .and_then(|r| r.limits.as_ref())
        .and_then(|l| l.get("cpu"))
        .map(|s| s.as_str());
    assert_eq!(stored_cpu, Some("200m"));
}

#[tokio::test]
async fn pod_resize_surfaces_conflict_after_exhausting_retries() {
    // Arrange: every update fails. The helper should give up after the bounded
    // retry budget and return Conflict — we don't want to retry forever and
    // wedge the request handler.
    let inner = Arc::new(MemoryStorage::new());
    // Pick a number larger than POD_RESIZE_MAX_RETRIES so we exhaust the budget.
    let storage = ConflictInjectingStorage::new(inner.clone(), 100);
    let namespace = "default";
    let name = "resize-cas-exhausted";
    let key = build_key("pods", Some(namespace), name);

    let initial = make_pod_with_cpu(name, namespace, "100m");
    inner.create(&key, &initial).await.unwrap();

    let desired = make_pod_with_cpu(name, namespace, "200m");

    // Act
    let err = apply_pod_resize_with_retry(&storage, namespace, name, &desired)
        .await
        .expect_err("should fail after exhausting retries");

    // Assert
    assert!(
        matches!(err, Error::Conflict(_)),
        "expected Error::Conflict, got {:?}",
        err
    );
}
