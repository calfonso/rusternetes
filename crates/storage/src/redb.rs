//! Embedded redb storage (feature `redb`).
//!
//! Runs [`tunnr_etcd`] in-process: one redb file, the current
//! state mirrored in RAM (so `get` and `list` never touch disk), etcd-style
//! revisions, watch from a revision, and compaction. No etcd, no rhino, no
//! external process. Its only dependency is redb.
//!
//! Revisions map to `resourceVersion` exactly as with etcd (`mod_revision`).
//! The log is compacted every five minutes to the revision seen five minutes
//! earlier, the same retention kube-apiserver asks of etcd.

use crate::concurrency;
use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use rusternetes_common::{authz::AuthzStorage, Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use tunnr_etcd::{Error as StoreError, Event, Options, Store};

const COMPACT_INTERVAL: Duration = Duration::from_secs(300);

/// Storage backed by an embedded redb file.
pub struct RedbStorage {
    store: Store,
}

impl RedbStorage {
    /// Open (or create) the redb file at `path` and start background compaction.
    pub async fn new(path: &str) -> Result<Self> {
        let p = path.to_string();
        let store = tokio::task::spawn_blocking(move || Store::open(&p, Options::main()))
            .await
            .map_err(|e| Error::Storage(format!("redb open task failed: {e}")))?
            .map_err(|e| Error::Storage(format!("Failed to open redb at {path}: {e}")))?;
        info!(
            "RedbStorage initialized at {} (revision {})",
            path,
            store.revision()
        );
        let compactor = store.clone();
        tokio::spawn(async move {
            let mut keep_from = compactor.revision();
            let mut tick = tokio::time::interval(COMPACT_INTERVAL);
            tick.tick().await;
            loop {
                tick.tick().await;
                let target = keep_from;
                keep_from = compactor.revision();
                if target > compactor.compacted_revision() {
                    let c = compactor.clone();
                    match tokio::task::spawn_blocking(move || c.compact(target)).await {
                        Ok(Ok(())) => debug!("redb compacted to revision {}", target),
                        Ok(Err(e)) => warn!("redb compaction to {} failed: {}", target, e),
                        Err(e) => warn!("redb compaction task failed: {}", e),
                    }
                }
            }
        });
        Ok(Self { store })
    }

    /// The underlying store, for example to attach a twin.
    pub fn store(&self) -> &Store {
        &self.store
    }

    fn inject_resource_version(json: &[u8], mod_revision: i64) -> Result<String> {
        let rv = concurrency::mod_revision_to_resource_version(mod_revision);
        let mut v: serde_json::Value =
            serde_json::from_slice(json).map_err(Error::Serialization)?;
        if let Some(metadata) = v.get_mut("metadata") {
            metadata["resourceVersion"] = serde_json::Value::String(rv);
        }
        serde_json::to_string(&v).map_err(Error::Serialization)
    }

    fn decode<T: DeserializeOwned>(json: &[u8], mod_revision: i64) -> Result<T> {
        let s = Self::inject_resource_version(json, mod_revision)?;
        serde_json::from_str(&s).map_err(Error::Serialization)
    }

    /// Run a write on the blocking pool: a write is one fsynced redb commit.
    async fn blocking<R: Send + 'static>(
        &self,
        f: impl FnOnce(Store) -> std::result::Result<R, StoreError> + Send + 'static,
    ) -> std::result::Result<R, StoreError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || f(store))
            .await
            .map_err(|e| StoreError::Db(format!("write task failed: {e}")))?
    }
}

fn map_err(key: &str, e: StoreError) -> Error {
    match e {
        StoreError::NotFound(_) => Error::NotFound(key.to_string()),
        StoreError::AlreadyExists(_) => Error::AlreadyExists(key.to_string()),
        StoreError::Conflict {
            expected, current, ..
        } => Error::Conflict(format!(
            "resourceVersion mismatch: resource was modified (expected: {}, current: {})",
            concurrency::mod_revision_to_resource_version(expected),
            concurrency::mod_revision_to_resource_version(current)
        )),
        StoreError::Compacted {
            requested,
            compacted,
        } => Error::Gone(format!(
            "too old resource version: {} ({})",
            requested, compacted
        )),
        other => Error::Storage(other.to_string()),
    }
}

/// Kubernetes keys are UTF-8 paths; the store keeps etcd's byte keys.
fn key_str(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

fn to_watch_event(ev: &Event) -> Result<WatchEvent> {
    Ok(match ev {
        Event::Put {
            rev,
            key,
            kv,
            prev: None,
        } => WatchEvent::Added(
            key_str(key),
            RedbStorage::inject_resource_version(&kv.value, *rev)?,
        ),
        Event::Put { rev, key, kv, .. } => WatchEvent::Modified(
            key_str(key),
            RedbStorage::inject_resource_version(&kv.value, *rev)?,
        ),
        Event::Delete { rev, key, prev } => WatchEvent::Deleted(
            key_str(key),
            RedbStorage::inject_resource_version(&prev.value, *rev)?,
        ),
        // Watches only carry key events; lease events never reach them.
        other => return Err(Error::Storage(format!("unexpected store event {other:?}"))),
    })
}

#[async_trait]
impl Storage for RedbStorage {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // Ensure metadata exists and generation is set to 1 on creation, as
        // the etcd and rhino backends do.
        let mut v = serde_json::to_value(value).map_err(Error::Serialization)?;
        if v.get("metadata").is_none_or(|m| m.is_null()) {
            v["metadata"] = serde_json::json!({});
        }
        if let Some(metadata) = v.get_mut("metadata") {
            if metadata.get("generation").is_none_or(|g| g.is_null()) {
                metadata["generation"] = serde_json::json!(1);
            }
        }
        let json = serde_json::to_vec(&v).map_err(Error::Serialization)?;
        let k = key.to_string();
        let kv = self
            .blocking(move |s| s.create(&k, &json))
            .await
            .map_err(|e| map_err(key, e))?;
        debug!("Created resource at key: {}", key);
        Self::decode(&kv.value, kv.mod_rev)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let kv = self
            .store
            .get(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?;
        Self::decode(&kv.value, kv.mod_rev)
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let json = serde_json::to_vec(value).map_err(Error::Serialization)?;
        let incoming: serde_json::Value =
            serde_json::from_slice(&json).map_err(Error::Serialization)?;
        let expected = match concurrency::extract_resource_version(
            incoming.get("metadata").unwrap_or(&serde_json::json!({})),
        ) {
            Some(rv) => Some(concurrency::resource_version_to_mod_revision(&rv)?),
            None => None,
        };
        let k = key.to_string();
        // Without a resourceVersion the store's writer makes the
        // read-check-write atomic, so no retry loop is needed.
        let kv = self
            .blocking(move |s| s.update(&k, &json, expected))
            .await
            .map_err(|e| map_err(key, e))?;
        debug!("Updated resource at key: {}", key);
        Self::decode(&kv.value, kv.mod_rev)
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let json = serde_json::to_vec(value).map_err(Error::Serialization)?;
        let k = key.to_string();
        self.blocking(move |s| s.update(&k, &json, None))
            .await
            .map_err(|e| map_err(key, e))?;
        debug!("Updated resource (raw) at key: {}", key);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let k = key.to_string();
        self.blocking(move |s| s.delete(&k, None))
            .await
            .map_err(|e| map_err(key, e))?;
        debug!("Deleted resource at key: {}", key);
        Ok(())
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let rows = self.store.list(prefix);
        let mut results = Vec::with_capacity(rows.len());
        for (key, kv) in rows {
            match Self::decode::<T>(&kv.value, kv.mod_rev) {
                Ok(v) => results.push(v),
                Err(e) => error!("Failed to deserialize value at {}: {}", key_str(&key), e),
            }
        }
        debug!("Listed {} resources with prefix: {}", results.len(), prefix);
        Ok(results)
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        self.watch_from_revision(prefix, self.store.revision() + 1)
            .await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Event>>();
        // The sink only hands the event to the channel; it never blocks the
        // writer. The event is shared with every other watcher, not copied.
        self.store
            .watch_with(prefix, revision.max(1), move |ev| {
                tx.send(Arc::clone(ev)).is_ok()
            })
            .map_err(|e| map_err(prefix, e))?;
        info!(
            "Started watching prefix: {} from revision {}",
            prefix, revision
        );
        let stream = async_stream::stream! {
            while let Some(ev) = rx.recv().await {
                yield to_watch_event(&ev);
            }
        };
        Ok(Box::pin(stream))
    }

    async fn current_revision(&self) -> Result<i64> {
        Ok(self.store.revision())
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        Ok(revision < self.store.compacted_revision())
    }
}

// AuthzStorage: the same key layout as the etcd and rhino backends.
#[async_trait]
impl AuthzStorage for RedbStorage {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let name = std::any::type_name::<T>();
        let full_key = match namespace {
            Some(ns)
                if name.contains("Role")
                    && !name.contains("Cluster")
                    && !name.contains("Binding") =>
            {
                format!("/registry/roles/{}/{}", ns, key)
            }
            Some(ns) if name.contains("RoleBinding") && !name.contains("Cluster") => {
                format!("/registry/rolebindings/{}/{}", ns, key)
            }
            Some(ns) => format!("/registry/unknown/{}/{}", ns, key),
            None if name.contains("ClusterRole") && !name.contains("Binding") => {
                format!("/registry/clusterroles/{}", key)
            }
            None if name.contains("ClusterRoleBinding") => {
                format!("/registry/clusterrolebindings/{}", key)
            }
            None => format!("/registry/unknown/{}", key),
        };
        Storage::get(self, &full_key).await
    }

    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let name = std::any::type_name::<T>();
        let prefix = match namespace {
            Some(ns)
                if name.contains("Role")
                    && !name.contains("Cluster")
                    && !name.contains("Binding") =>
            {
                format!("/registry/roles/{}/", ns)
            }
            Some(ns) if name.contains("RoleBinding") && !name.contains("Cluster") => {
                format!("/registry/rolebindings/{}/", ns)
            }
            Some(ns) => format!("/registry/unknown/{}/", ns),
            None if name.contains("ClusterRole") && !name.contains("Binding") => {
                "/registry/clusterroles/".to_string()
            }
            None if name.contains("ClusterRoleBinding") => {
                "/registry/clusterrolebindings/".to_string()
            }
            None => "/registry/unknown/".to_string(),
        };
        Storage::list(self, &prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use serde_json::json;

    async fn storage(name: &str) -> RedbStorage {
        let p = std::env::temp_dir().join(format!("rn-redb-{}-{name}.redb", std::process::id()));
        let _ = std::fs::remove_file(&p);
        RedbStorage::new(p.to_str().unwrap()).await.unwrap()
    }

    fn pod(name: &str) -> serde_json::Value {
        json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": "default"}})
    }

    #[tokio::test]
    async fn create_sets_generation_and_resource_version() {
        let s = storage("create").await;
        let out: serde_json::Value = s
            .create("/registry/pods/default/a", &pod("a"))
            .await
            .unwrap();
        assert_eq!(out["metadata"]["generation"], 1);
        assert_eq!(out["metadata"]["resourceVersion"], "1");
        let again = s.create("/registry/pods/default/a", &pod("a")).await;
        assert!(matches!(again, Err(Error::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn update_enforces_resource_version() {
        let s = storage("cas").await;
        let mut p: serde_json::Value = s
            .create("/registry/pods/default/a", &pod("a"))
            .await
            .unwrap();
        p["spec"] = json!({"x": 1});
        let p2: serde_json::Value = s.update("/registry/pods/default/a", &p).await.unwrap();
        assert_eq!(p2["metadata"]["resourceVersion"], "2");
        // p still carries resourceVersion 1: a stale write must conflict.
        assert!(matches!(
            s.update::<serde_json::Value>("/registry/pods/default/a", &p)
                .await,
            Err(Error::Conflict(_))
        ));
        let mut no_rv = p2.clone();
        no_rv["metadata"]
            .as_object_mut()
            .unwrap()
            .remove("resourceVersion");
        let p3: serde_json::Value = s.update("/registry/pods/default/a", &no_rv).await.unwrap();
        assert_eq!(p3["metadata"]["resourceVersion"], "3");
    }

    #[tokio::test]
    async fn list_and_delete() {
        let s = storage("list").await;
        for n in ["a", "b"] {
            let _: serde_json::Value = s
                .create(&format!("/registry/pods/default/{n}"), &pod(n))
                .await
                .unwrap();
        }
        let _: serde_json::Value = s.create("/registry/pods/other/c", &pod("c")).await.unwrap();
        let l: Vec<serde_json::Value> = Storage::list(&s, "/registry/pods/default/").await.unwrap();
        assert_eq!(l.len(), 2);
        s.delete("/registry/pods/default/a").await.unwrap();
        assert!(matches!(
            Storage::get::<serde_json::Value>(&s, "/registry/pods/default/a").await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            s.delete("/registry/pods/default/a").await,
            Err(Error::NotFound(_))
        ));
    }

    async fn next(w: &mut WatchStream) -> WatchEvent {
        tokio::time::timeout(Duration::from_secs(2), w.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn watch_from_revision_replays_then_streams() {
        let s = storage("watch").await;
        let _: serde_json::Value = s
            .create("/registry/pods/default/a", &pod("a"))
            .await
            .unwrap();
        let mut w = s.watch_from_revision("/registry/pods/", 1).await.unwrap();
        let mut p: serde_json::Value = Storage::get(&s, "/registry/pods/default/a").await.unwrap();
        p["spec"] = json!({"x": 1});
        let _: serde_json::Value = s.update("/registry/pods/default/a", &p).await.unwrap();
        s.delete("/registry/pods/default/a").await.unwrap();
        assert!(matches!(next(&mut w).await, WatchEvent::Added(..)));
        assert!(matches!(next(&mut w).await, WatchEvent::Modified(..)));
        match next(&mut w).await {
            WatchEvent::Deleted(k, v) => {
                assert_eq!(k, "/registry/pods/default/a");
                let v: serde_json::Value = serde_json::from_str(&v).unwrap();
                assert_eq!(v["metadata"]["resourceVersion"], "3");
            }
            other => panic!("expected a delete, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compacted_revisions_are_gone() {
        let s = storage("gone").await;
        for i in 0..5 {
            let _: serde_json::Value = s
                .create(&format!("/registry/pods/default/{i}"), &pod("x"))
                .await
                .unwrap();
        }
        s.store().compact(4).unwrap();
        assert!(s.is_revision_compacted(3).await.unwrap());
        assert!(!s.is_revision_compacted(4).await.unwrap());
        assert!(matches!(
            s.watch_from_revision("/registry/", 2).await,
            Err(Error::Gone(_))
        ));
    }
}
