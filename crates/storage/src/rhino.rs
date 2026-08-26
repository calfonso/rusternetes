//! Rhino-backed storage using rhino's `Backend` trait directly.
//!
//! This module implements the rusternetes [`Storage`] trait on top of rhino
//! backends (SQLite, Redis, etc.), bypassing the gRPC layer entirely.
//! The result is an embedded, zero-dependency storage backend suitable for
//! single-node / all-in-one deployments — no external etcd or rhino server
//! process needed.

use crate::concurrency;
use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use rhino::backend::Backend;
#[cfg(feature = "redis")]
use rhino::{RedisBackend, RedisConfig};
#[cfg(feature = "sqlite")]
use rhino::{SqliteBackend, SqliteConfig};
use rusternetes_common::{authz::AuthzStorage, Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Storage implementation backed by a rhino backend.
///
/// Embeds a rhino backend in-process and translates rusternetes'
/// [`Storage`] trait calls directly into rhino backend operations.
/// No gRPC, no network — pure in-process Rust calls.
pub struct RhinoStorage<B: Backend> {
    backend: Arc<B>,
}

#[cfg(feature = "sqlite")]
impl RhinoStorage<SqliteBackend> {
    /// Create a new RhinoStorage with the given SQLite database path.
    ///
    /// This initialises the SQLite database (creating it if necessary),
    /// sets up the schema, and starts background compaction/poll loops.
    pub async fn new(db_path: &str) -> Result<Self> {
        let config = SqliteConfig {
            dsn: db_path.to_string(),
            compact_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let backend = SqliteBackend::new(config)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create SQLite backend: {}", e)))?;

        backend
            .start()
            .await
            .map_err(|e| Error::Storage(format!("Failed to start SQLite backend: {}", e)))?;

        info!("RhinoStorage (SQLite) initialized at {}", db_path);

        Ok(Self {
            backend: Arc::new(backend),
        })
    }
}

#[cfg(feature = "redis")]
impl RhinoStorage<RedisBackend> {
    /// Create a new RhinoStorage backed by Redis.
    pub async fn new_redis(redis_url: &str) -> Result<Self> {
        let config = RedisConfig {
            dsn: redis_url.to_string(),
            compact_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let backend = RedisBackend::new(config)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create Redis backend: {}", e)))?;

        backend
            .start()
            .await
            .map_err(|e| Error::Storage(format!("Failed to start Redis backend: {}", e)))?;

        info!("RhinoStorage (Redis) initialized at {}", redis_url);

        Ok(Self {
            backend: Arc::new(backend),
        })
    }
}

impl<B: Backend> RhinoStorage<B> {
    /// Serialize a value to JSON.
    fn serialize<T: Serialize>(value: &T) -> Result<String> {
        serde_json::to_string(value).map_err(Error::Serialization)
    }

    /// Inject resourceVersion into JSON metadata from a rhino mod_revision.
    fn inject_resource_version(json: &str, mod_revision: i64) -> String {
        let rv_str = concurrency::mod_revision_to_resource_version(mod_revision);
        if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(json) {
            if let Some(metadata) = v.get_mut("metadata") {
                metadata["resourceVersion"] = serde_json::Value::String(rv_str);
            }
            serde_json::to_string(&v).unwrap_or_else(|_| json.to_string())
        } else {
            json.to_string()
        }
    }
}

#[async_trait]
impl<B: Backend + Send + Sync + 'static> Storage for RhinoStorage<B> {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // Ensure metadata exists and generation is set to 1 on creation
        let json = {
            let mut raw = Self::serialize(value)?;
            if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&raw) {
                if v.get("metadata").is_none() || v.get("metadata").is_some_and(|m| m.is_null()) {
                    v["metadata"] = serde_json::json!({});
                }
                if let Some(metadata) = v.get_mut("metadata") {
                    if metadata.get("generation").is_none_or(|g| g.is_null()) {
                        metadata["generation"] = serde_json::json!(1);
                    }
                }
                raw = serde_json::to_string(&v).unwrap_or(raw);
            }
            raw
        };

        let mod_revision =
            self.backend
                .create(key, json.as_bytes(), 0)
                .await
                .map_err(|e| match e {
                    rhino::backend::BackendError::KeyExists => {
                        Error::AlreadyExists(key.to_string())
                    }
                    other => Error::Storage(format!("Failed to create resource: {}", other)),
                })?;

        debug!("Created resource at key: {}", key);

        let json_with_rv = Self::inject_resource_version(&json, mod_revision);
        serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let (_rev, kv) = self
            .backend
            .get(key, "", 1, 0, false)
            .await
            .map_err(|e| Error::Storage(format!("Failed to get resource: {}", e)))?;

        match kv {
            Some(kv) => {
                let json = String::from_utf8(kv.value)
                    .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;
                let json_with_rv = Self::inject_resource_version(&json, kv.mod_revision);
                serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
            }
            None => Err(Error::NotFound(key.to_string())),
        }
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let json = Self::serialize(value)?;

        // Extract resourceVersion from the incoming resource for optimistic concurrency
        let incoming_resource: serde_json::Value =
            serde_json::from_str(&json).map_err(Error::Serialization)?;
        let incoming_rv = concurrency::extract_resource_version(
            incoming_resource
                .get("metadata")
                .unwrap_or(&serde_json::json!({})),
        );

        if let Some(incoming_rv) = incoming_rv.as_deref() {
            let expected_mod_revision = concurrency::resource_version_to_mod_revision(incoming_rv)?;

            let (rev, prev_kv, succeeded) = self
                .backend
                .update(key, json.as_bytes(), expected_mod_revision, 0)
                .await
                .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;

            if !succeeded {
                let current_rv = prev_kv
                    .as_ref()
                    .map(|kv| concurrency::mod_revision_to_resource_version(kv.mod_revision))
                    .unwrap_or_else(|| "unknown".to_string());

                if prev_kv.is_none() {
                    return Err(Error::NotFound(key.to_string()));
                }

                return Err(Error::Conflict(format!(
                    "resourceVersion mismatch: resource was modified (expected: {}, current: {})",
                    incoming_rv, current_rv
                )));
            }

            debug!("Updated resource at key: {}", key);
            let json_with_rv = Self::inject_resource_version(&json, rev);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        } else {
            // No resourceVersion provided — check key exists, then update
            // Use the current revision from a get to perform the update
            let (_rev, existing_kv) = self
                .backend
                .get(key, "", 1, 0, false)
                .await
                .map_err(|e| Error::Storage(format!("Failed to check resource: {}", e)))?;

            let existing_kv = existing_kv.ok_or_else(|| Error::NotFound(key.to_string()))?;

            let (new_rev, _prev_kv, succeeded) = self
                .backend
                .update(key, json.as_bytes(), existing_kv.mod_revision, 0)
                .await
                .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;

            if !succeeded {
                // Concurrent modification — retry once by re-reading
                let (_rev, latest_kv) =
                    self.backend.get(key, "", 1, 0, false).await.map_err(|e| {
                        Error::Storage(format!("Failed to re-read resource: {}", e))
                    })?;

                let latest_kv = latest_kv.ok_or_else(|| Error::NotFound(key.to_string()))?;

                let (new_rev, _prev_kv, succeeded) = self
                    .backend
                    .update(key, json.as_bytes(), latest_kv.mod_revision, 0)
                    .await
                    .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;

                if !succeeded {
                    return Err(Error::Conflict(
                        "resourceVersion mismatch after retry".to_string(),
                    ));
                }

                let json_with_rv = Self::inject_resource_version(&json, new_rev);
                return serde_json::from_str(&json_with_rv).map_err(Error::Serialization);
            }

            debug!("Updated resource at key: {}", key);
            let json_with_rv = Self::inject_resource_version(&json, new_rev);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        }
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let json = serde_json::to_string(value).map_err(Error::Serialization)?;

        // Get current revision to perform update
        let (_rev, existing_kv) = self
            .backend
            .get(key, "", 1, 0, false)
            .await
            .map_err(|e| Error::Storage(format!("Failed to check resource: {}", e)))?;

        let existing_kv = existing_kv.ok_or_else(|| Error::NotFound(key.to_string()))?;

        let (_new_rev, _prev_kv, succeeded) = self
            .backend
            .update(key, json.as_bytes(), existing_kv.mod_revision, 0)
            .await
            .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;

        if !succeeded {
            return Err(Error::Conflict(
                "resource was modified concurrently".to_string(),
            ));
        }

        debug!("Updated resource (raw) at key: {}", key);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let (_rev, _prev_kv, succeeded) = self
            .backend
            .delete(key, 0)
            .await
            .map_err(|e| Error::Storage(format!("Failed to delete resource: {}", e)))?;

        if !succeeded {
            return Err(Error::NotFound(key.to_string()));
        }

        debug!("Deleted resource at key: {}", key);
        Ok(())
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let (_rev, kvs) = self
            .backend
            .list(prefix, "", 0, 0, false)
            .await
            .map_err(|e| Error::Storage(format!("Failed to list resources: {}", e)))?;

        let mut results = Vec::with_capacity(kvs.len());
        for kv in kvs {
            let json = String::from_utf8(kv.value)
                .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;
            let json_with_rv = Self::inject_resource_version(&json, kv.mod_revision);
            match serde_json::from_str::<T>(&json_with_rv) {
                Ok(value) => results.push(value),
                Err(e) => {
                    error!("Failed to deserialize value at {}: {}", kv.key, e);
                    continue;
                }
            }
        }

        debug!("Listed {} resources with prefix: {}", results.len(), prefix);
        Ok(results)
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        // Watch from current revision (0 means "now")
        let current_rev = self
            .backend
            .current_revision()
            .await
            .map_err(|e| Error::Storage(format!("Failed to get current revision: {}", e)))?;

        self.watch_from_revision(prefix, current_rev + 1).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        let watch_result = self
            .backend
            .watch(prefix, revision)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create watch: {}", e)))?;

        info!(
            "Started watching prefix: {} from revision {}",
            prefix, revision
        );

        let mut events_rx = watch_result.events;

        let watch_stream = async_stream::stream! {
            while let Some(events) = events_rx.recv().await {
                for event in events {
                    let key = event.kv.key.clone();
                    let mod_revision = event.kv.mod_revision;

                    if event.delete {
                        // For deletes, use prev_kv value if available, otherwise use kv value
                        let raw_prev = event
                            .prev_kv
                            .as_ref()
                            .map(|pk| String::from_utf8_lossy(&pk.value).to_string())
                            .unwrap_or_default();
                        let prev_value = Self::inject_resource_version(&raw_prev, mod_revision);
                        yield Ok(WatchEvent::Deleted(key, prev_value));
                    } else {
                        let raw_value = String::from_utf8_lossy(&event.kv.value).to_string();
                        let value = Self::inject_resource_version(&raw_value, mod_revision);
                        if event.create {
                            yield Ok(WatchEvent::Added(key, value));
                        } else {
                            yield Ok(WatchEvent::Modified(key, value));
                        }
                    }
                }
            }
        };

        Ok(Box::pin(watch_stream))
    }

    async fn current_revision(&self) -> Result<i64> {
        self.backend
            .current_revision()
            .await
            .map_err(|e| Error::Storage(format!("Failed to get current revision: {}", e)))
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        // Try to list at the given revision — if compacted, rhino returns Compacted error
        match self.backend.list("/registry/", "", 1, revision, true).await {
            Ok(_) => Ok(false),
            Err(rhino::backend::BackendError::Compacted) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

// Implement AuthzStorage for RhinoStorage — same pattern as EtcdStorage
#[async_trait]
impl<B: Backend + Send + Sync + 'static> AuthzStorage for RhinoStorage<B> {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let full_key = match namespace {
            Some(ns) => {
                if std::any::type_name::<T>().contains("Role")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/roles/{}/{}", ns, key)
                } else if std::any::type_name::<T>().contains("RoleBinding")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/rolebindings/{}/{}", ns, key)
                } else {
                    format!("/registry/unknown/{}/{}", ns, key)
                }
            }
            None => {
                if std::any::type_name::<T>().contains("ClusterRole")
                    && !std::any::type_name::<T>().contains("Binding")
                {
                    format!("/registry/clusterroles/{}", key)
                } else if std::any::type_name::<T>().contains("ClusterRoleBinding") {
                    format!("/registry/clusterrolebindings/{}", key)
                } else {
                    format!("/registry/unknown/{}", key)
                }
            }
        };

        Storage::get(self, &full_key).await
    }

    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let prefix = match namespace {
            Some(ns) => {
                if std::any::type_name::<T>().contains("Role")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/roles/{}/", ns)
                } else if std::any::type_name::<T>().contains("RoleBinding")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/rolebindings/{}/", ns)
                } else {
                    format!("/registry/unknown/{}/", ns)
                }
            }
            None => {
                if std::any::type_name::<T>().contains("ClusterRole")
                    && !std::any::type_name::<T>().contains("Binding")
                {
                    "/registry/clusterroles/".to_string()
                } else if std::any::type_name::<T>().contains("ClusterRoleBinding") {
                    "/registry/clusterrolebindings/".to_string()
                } else {
                    "/registry/unknown/".to_string()
                }
            }
        };

        Storage::list(self, &prefix).await
    }
}
