use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use rusternetes_common::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

/// In-memory storage implementation for testing
#[derive(Clone)]
pub struct MemoryStorage {
    data: Arc<RwLock<HashMap<String, String>>>,
    // Broadcast channel for watch events
    watch_tx: broadcast::Sender<WatchEvent>,
    /// Number of remaining update() calls that will return Error::Conflict
    /// before delegating to the real update. Used by tests to inject CAS conflicts.
    conflict_update_count: Arc<AtomicUsize>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        // Create a broadcast channel with capacity of 1000 events
        let (watch_tx, _) = broadcast::channel(1000);
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            watch_tx,
            conflict_update_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Clear all data (useful for test cleanup)
    pub fn clear(&self) {
        self.data.write().unwrap().clear();
    }

    /// Get the number of stored items
    pub fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }

    /// Check if storage is empty
    pub fn is_empty(&self) -> bool {
        self.data.read().unwrap().is_empty()
    }

    /// Arrange for the next `n` calls to `update()` to return `Error::Conflict`.
    /// Subsequent calls behave normally. Used by tests to verify retry logic.
    pub fn inject_conflicts(&self, n: usize) {
        self.conflict_update_count.store(n, Ordering::SeqCst);
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let mut value_json: serde_json::Value = serde_json::to_value(value)?;

        // Generate UID if not already set or empty
        if let Some(metadata) = value_json.get_mut("metadata") {
            if let Some(metadata_obj) = metadata.as_object_mut() {
                // Check if uid field exists and is empty/null, or doesn't exist
                let should_generate_uid = match metadata_obj.get("uid") {
                    Some(serde_json::Value::String(s)) if !s.is_empty() => false,
                    _ => true, // None, null, empty string, or non-string
                };

                if should_generate_uid {
                    metadata_obj.insert(
                        "uid".to_string(),
                        serde_json::Value::String(uuid::Uuid::new_v4().to_string()),
                    );
                }

                // Set creation timestamp if not set
                if metadata_obj.get("creationTimestamp").is_none()
                    || metadata_obj.get("creationTimestamp") == Some(&serde_json::Value::Null)
                {
                    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    metadata_obj.insert(
                        "creationTimestamp".to_string(),
                        serde_json::Value::String(now),
                    );
                }
            }
        }

        let serialized = serde_json::to_string(&value_json)?;

        let mut data = self.data.write().unwrap();
        if data.contains_key(key) {
            return Err(Error::AlreadyExists(key.to_string()));
        }

        data.insert(key.to_string(), serialized.clone());
        drop(data); // Release lock before sending event

        // Emit watch event
        let _ = self
            .watch_tx
            .send(WatchEvent::Added(key.to_string(), serialized.clone()));

        Ok(serde_json::from_str(&serialized)?)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let data = self.data.read().unwrap();
        let value = data
            .get(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?;

        Ok(serde_json::from_str(value)?)
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // Inject a Conflict error if requested by a test (see inject_conflicts).
        if self.conflict_update_count.load(Ordering::SeqCst) > 0 {
            let prev = self
                .conflict_update_count
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                    if v > 0 { Some(v - 1) } else { None }
                });
            if prev.is_ok() {
                return Err(Error::Conflict(
                    "injected conflict for test".to_string(),
                ));
            }
        }

        let serialized = serde_json::to_string(value)?;

        let mut data = self.data.write().unwrap();
        if !data.contains_key(key) {
            return Err(Error::NotFound(key.to_string()));
        }

        data.insert(key.to_string(), serialized.clone());
        drop(data); // Release lock before sending event

        // Emit watch event
        let _ = self
            .watch_tx
            .send(WatchEvent::Modified(key.to_string(), serialized.clone()));

        Ok(serde_json::from_str(&serialized)?)
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let serialized = serde_json::to_string(value)?;

        let mut data = self.data.write().unwrap();
        if !data.contains_key(key) {
            return Err(Error::NotFound(key.to_string()));
        }

        data.insert(key.to_string(), serialized.clone());
        drop(data); // Release lock before sending event

        // Emit watch event
        let _ = self
            .watch_tx
            .send(WatchEvent::Modified(key.to_string(), serialized));

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut data = self.data.write().unwrap();
        let previous_value = data
            .remove(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?;
        drop(data); // Release lock before sending event

        // Emit watch event with previous value
        let _ = self
            .watch_tx
            .send(WatchEvent::Deleted(key.to_string(), previous_value));

        Ok(())
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let data = self.data.read().unwrap();
        let mut results = Vec::new();

        for (key, value) in data.iter() {
            if key.starts_with(prefix) {
                results.push(serde_json::from_str(value)?);
            }
        }

        Ok(results)
    }

    async fn watch_from_revision(&self, prefix: &str, _revision: i64) -> Result<WatchStream> {
        // Memory storage doesn't support revisions, just delegate to watch
        self.watch(prefix).await
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        let mut rx = self.watch_tx.subscribe();
        let prefix = prefix.to_string();

        let stream = async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                // Filter events by prefix
                let matches = match &event {
                    WatchEvent::Added(key, _) => key.starts_with(&prefix),
                    WatchEvent::Modified(key, _) => key.starts_with(&prefix),
                    WatchEvent::Deleted(key, _) => key.starts_with(&prefix),
                };

                if matches {
                    yield Ok(event);
                }
            }
        };

        Ok(Box::pin(stream))
    }

    async fn current_revision(&self) -> Result<i64> {
        Ok(chrono::Utc::now().timestamp())
    }

    async fn is_revision_compacted(&self, _revision: i64) -> Result<bool> {
        Ok(false) // Memory storage never compacts
    }
}

// AuthzStorage for MemoryStorage — used when StorageBackend::Memory is selected.
// The RBAC queries in tests use AlwaysAllowAuthorizer so these methods are
// typically not called; they're implemented for completeness.
#[async_trait::async_trait]
impl rusternetes_common::authz::AuthzStorage for MemoryStorage {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> rusternetes_common::Result<T>
    where
        T: serde::de::DeserializeOwned + Send + Sync,
    {
        use crate::build_key;
        // Mirror the EtcdStorage pattern: derive a full /registry path from type name.
        let full_key = match namespace {
            Some(ns) => {
                let tn = std::any::type_name::<T>();
                if tn.contains("Role") && !tn.contains("Cluster") {
                    format!("/registry/roles/{}/{}", ns, key)
                } else if tn.contains("RoleBinding") && !tn.contains("Cluster") {
                    format!("/registry/rolebindings/{}/{}", ns, key)
                } else {
                    build_key("unknown", Some(ns), key)
                }
            }
            None => {
                let tn = std::any::type_name::<T>();
                if tn.contains("ClusterRole") && !tn.contains("Binding") {
                    format!("/registry/clusterroles/{}", key)
                } else if tn.contains("ClusterRoleBinding") {
                    format!("/registry/clusterrolebindings/{}", key)
                } else {
                    format!("/registry/unknown/{}", key)
                }
            }
        };
        Storage::get(self, &full_key).await
    }

    async fn list<T>(&self, namespace: Option<&str>) -> rusternetes_common::Result<Vec<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        let prefix = match namespace {
            Some(ns) => {
                let tn = std::any::type_name::<T>();
                if tn.contains("Role") && !tn.contains("Cluster") {
                    format!("/registry/roles/{}/", ns)
                } else if tn.contains("RoleBinding") && !tn.contains("Cluster") {
                    format!("/registry/rolebindings/{}/", ns)
                } else {
                    format!("/registry/unknown/{}/", ns)
                }
            }
            None => {
                let tn = std::any::type_name::<T>();
                if tn.contains("ClusterRole") && !tn.contains("Binding") {
                    "/registry/clusterroles/".to_string()
                } else if tn.contains("ClusterRoleBinding") {
                    "/registry/clusterrolebindings/".to_string()
                } else {
                    "/registry/unknown/".to_string()
                }
            }
        };
        Storage::list(self, &prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestResource {
        name: String,
        value: i32,
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let storage = MemoryStorage::new();
        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        storage.create("/test/key", &resource).await.unwrap();
        let retrieved: TestResource = storage.get("/test/key").await.unwrap();

        assert_eq!(resource, retrieved);
    }

    #[tokio::test]
    async fn test_create_duplicate_fails() {
        let storage = MemoryStorage::new();
        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        storage.create("/test/key", &resource).await.unwrap();
        let result = storage.create("/test/key", &resource).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update() {
        let storage = MemoryStorage::new();
        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        storage.create("/test/key", &resource).await.unwrap();

        let updated = TestResource {
            name: "updated".to_string(),
            value: 100,
        };
        storage.update("/test/key", &updated).await.unwrap();

        let retrieved: TestResource = storage.get("/test/key").await.unwrap();
        assert_eq!(updated, retrieved);
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = MemoryStorage::new();
        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        storage.create("/test/key", &resource).await.unwrap();
        storage.delete("/test/key").await.unwrap();

        let result: Result<TestResource> = storage.get("/test/key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list() {
        let storage = MemoryStorage::new();

        storage
            .create(
                "/test/ns1/pod1",
                &TestResource {
                    name: "pod1".to_string(),
                    value: 1,
                },
            )
            .await
            .unwrap();
        storage
            .create(
                "/test/ns1/pod2",
                &TestResource {
                    name: "pod2".to_string(),
                    value: 2,
                },
            )
            .await
            .unwrap();
        storage
            .create(
                "/test/ns2/pod3",
                &TestResource {
                    name: "pod3".to_string(),
                    value: 3,
                },
            )
            .await
            .unwrap();

        let ns1_pods: Vec<TestResource> = storage.list("/test/ns1/").await.unwrap();
        assert_eq!(ns1_pods.len(), 2);

        let all_pods: Vec<TestResource> = storage.list("/test/").await.unwrap();
        assert_eq!(all_pods.len(), 3);
    }

    #[tokio::test]
    async fn test_clear() {
        let storage = MemoryStorage::new();

        storage
            .create(
                "/test/key1",
                &TestResource {
                    name: "test1".to_string(),
                    value: 1,
                },
            )
            .await
            .unwrap();
        storage
            .create(
                "/test/key2",
                &TestResource {
                    name: "test2".to_string(),
                    value: 2,
                },
            )
            .await
            .unwrap();

        assert_eq!(storage.len(), 2);

        storage.clear();

        assert_eq!(storage.len(), 0);
        assert!(storage.is_empty());
    }
}
