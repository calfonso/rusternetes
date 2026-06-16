use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, GetOptions, TxnOp, WatchOptions};
use futures::StreamExt;
use rusternetes_common::{authz::AuthzStorage, Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, error, info};

/// EtcdStorage implements the Storage trait using etcd as the backend.
///
/// The etcd `Client` is `Clone` and internally uses gRPC/tonic which
/// multiplexes requests over a single HTTP/2 connection. No mutex is needed —
/// cloning the client is cheap and allows fully concurrent access.
pub struct EtcdStorage {
    client: Client,
}

impl EtcdStorage {
    /// Create a new EtcdStorage instance
    pub async fn new(endpoints: Vec<String>) -> Result<Self> {
        let options = Some(
            etcd_client::ConnectOptions::new()
                .with_keep_alive(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(3),
                )
                .with_keep_alive_while_idle(true),
        );
        let client = Client::connect(endpoints, options)
            .await
            .map_err(|e| Error::Storage(format!("Failed to connect to etcd: {}", e)))?;

        info!("Connected to etcd successfully");

        Ok(Self { client })
    }

    /// Helper to serialize a value to JSON
    fn serialize<T: Serialize>(value: &T) -> Result<String> {
        serde_json::to_string(value).map_err(Error::Serialization)
    }

    /// Inject resourceVersion into a JSON string by parsing, modifying, and re-serializing.
    ///
    /// This is a single parse→modify→reserialize pass (vs the old code which did
    /// parse→Value→modify→reserialize→from_value = two full cycles). Still faster
    /// than the original approach while being completely safe against edge cases.
    fn inject_resource_version(json: &str, mod_revision: i64) -> String {
        let rv_str = crate::concurrency::mod_revision_to_resource_version(mod_revision);
        if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(json) {
            if let Some(metadata) = v.get_mut("metadata") {
                metadata["resourceVersion"] = serde_json::Value::String(rv_str);
            }
            // serde_json::to_string on a Value is infallible in practice
            serde_json::to_string(&v).unwrap_or_else(|_| json.to_string())
        } else {
            // Unparseable JSON — return as-is (should not happen with etcd data)
            json.to_string()
        }
    }
}

#[async_trait]
impl Storage for EtcdStorage {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let mut client = self.client.clone();
        // Stamp system-managed creation metadata (uid, creationTimestamp,
        // generation) centrally, mirroring k8s registry.Store.Create.
        let json = {
            let mut raw = Self::serialize(value)?;
            if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&raw) {
                crate::metadata::ensure_create_metadata(&mut v);
                raw = serde_json::to_string(&v).unwrap_or(raw);
            }
            raw
        };

        // Use a transaction to ensure the key doesn't already exist.
        // Include a GET in the then branch to read back the exact mod_revision.
        let txn = etcd_client::Txn::new()
            .when(vec![Compare::version(key, CompareOp::Equal, 0)])
            .and_then(vec![
                TxnOp::put(key, json.clone(), None),
                TxnOp::get(key, None),
            ])
            .or_else(vec![]);

        let txn_resp = client
            .txn(txn)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create resource: {}", e)))?;

        if !txn_resp.succeeded() {
            return Err(Error::AlreadyExists(key.to_string()));
        }

        debug!("Created resource at key: {}", key);

        // Get the exact mod_revision from the GET in the then branch (2nd op)
        let mod_revision = txn_resp
            .op_responses()
            .get(1)
            .and_then(|resp| {
                if let etcd_client::TxnOpResponse::Get(get_resp) = resp {
                    get_resp.kvs().first().map(|kv| kv.mod_revision())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| txn_resp.header().map(|h| h.revision()).unwrap_or(0));

        // Inject resourceVersion and deserialize
        let json_with_rv = Self::inject_resource_version(&json, mod_revision);
        serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let mut client = self.client.clone();

        let resp = client
            .get(key, None)
            .await
            .map_err(|e| Error::Storage(format!("Failed to get resource: {}", e)))?;

        if let Some(kv) = resp.kvs().first() {
            let json = kv
                .value_str()
                .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;

            let mod_revision = kv.mod_revision();
            let json_with_rv = Self::inject_resource_version(json, mod_revision);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        } else {
            Err(Error::NotFound(key.to_string()))
        }
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let mut client = self.client.clone();
        let json = Self::serialize(value)?;

        // Extract resourceVersion from the incoming resource
        let incoming_resource: serde_json::Value =
            serde_json::from_str(&json).map_err(Error::Serialization)?;
        let incoming_rv = crate::concurrency::extract_resource_version(
            incoming_resource
                .get("metadata")
                .unwrap_or(&serde_json::json!({})),
        );

        // Validate optimistic concurrency if resourceVersion is provided
        if let Some(incoming_rv) = incoming_rv.as_deref() {
            // Use a transaction to ensure atomic update with version check
            let expected_mod_revision =
                crate::concurrency::resource_version_to_mod_revision(incoming_rv)?;
            // Transaction: if mod_revision matches, PUT then GET (to read back mod_revision).
            // On failure, GET to report the current version in the error.
            let txn = etcd_client::Txn::new()
                .when(vec![Compare::mod_revision(
                    key,
                    CompareOp::Equal,
                    expected_mod_revision,
                )])
                .and_then(vec![
                    TxnOp::put(key, json.clone(), None),
                    TxnOp::get(key, None),
                ])
                .or_else(vec![TxnOp::get(key, None)]);

            let txn_resp = client
                .txn(txn)
                .await
                .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;

            if !txn_resp.succeeded() {
                // Get the current resourceVersion from the failed txn's else branch
                let current_rv = txn_resp
                    .op_responses()
                    .first()
                    .and_then(|resp| {
                        if let etcd_client::TxnOpResponse::Get(get_resp) = resp {
                            get_resp.kvs().first().map(|kv| {
                                crate::concurrency::mod_revision_to_resource_version(
                                    kv.mod_revision(),
                                )
                            })
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| "unknown".to_string());
                return Err(Error::Conflict(format!(
                    "resourceVersion mismatch: resource was modified (expected: {}, current: {})",
                    incoming_rv, current_rv
                )));
            }

            debug!("Updated resource at key: {}", key);

            // Get mod_revision from the GET in the then branch (2nd op response)
            let mod_revision = txn_resp
                .op_responses()
                .get(1)
                .and_then(|resp| {
                    if let etcd_client::TxnOpResponse::Get(get_resp) = resp {
                        get_resp.kvs().first().map(|kv| kv.mod_revision())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| txn_resp.header().map(|h| h.revision()).unwrap_or(0));

            let json_with_rv = Self::inject_resource_version(&json, mod_revision);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        } else {
            // No resourceVersion provided — check key exists, then put
            let get_resp = client
                .get(key, Some(GetOptions::new().with_keys_only()))
                .await
                .map_err(|e| Error::Storage(format!("Failed to check resource: {}", e)))?;

            if get_resp.kvs().is_empty() {
                return Err(Error::NotFound(key.to_string()));
            }

            client
                .put(key, json.clone(), None)
                .await
                .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;

            debug!("Updated resource at key: {}", key);

            // Read back to get exact mod_revision
            let get_resp = client
                .get(key, None)
                .await
                .map_err(|e| Error::Storage(format!("Failed to get updated resource: {}", e)))?;

            if let Some(kv) = get_resp.kvs().first() {
                let mod_revision = kv.mod_revision();
                let json_with_rv = Self::inject_resource_version(&json, mod_revision);
                serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
            } else {
                // Key was deleted between put and get — shouldn't happen
                serde_json::from_str(&json).map_err(Error::Serialization)
            }
        }
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let mut client = self.client.clone();
        let json = serde_json::to_string(value).map_err(Error::Serialization)?;

        // Check if the key exists first (keys_only to save bandwidth)
        let get_resp = client
            .get(key, Some(GetOptions::new().with_keys_only()))
            .await
            .map_err(|e| Error::Storage(format!("Failed to check resource: {}", e)))?;

        if get_resp.kvs().is_empty() {
            return Err(Error::NotFound(key.to_string()));
        }

        client
            .put(key, json, None)
            .await
            .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;

        debug!("Updated resource (raw) at key: {}", key);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.client.clone();

        let resp = client
            .delete(key, None)
            .await
            .map_err(|e| Error::Storage(format!("Failed to delete resource: {}", e)))?;

        if resp.deleted() == 0 {
            return Err(Error::NotFound(key.to_string()));
        }

        debug!("Deleted resource at key: {}", key);
        Ok(())
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let mut client = self.client.clone();

        // Paginate etcd list calls to avoid hitting the default 4MB gRPC
        // message size limit. Fetch up to 500 keys per request.
        const PAGE_SIZE: i64 = 500;
        let mut results = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;

        loop {
            let get_options = match &last_key {
                None => {
                    // First page: use prefix scan
                    GetOptions::new().with_prefix().with_limit(PAGE_SIZE)
                }
                Some(_key) => {
                    // Subsequent pages: start from last_key (exclusive) with prefix
                    GetOptions::new()
                        .with_prefix()
                        .with_from_key()
                        .with_limit(PAGE_SIZE + 1) // +1 because from_key is inclusive
                }
            };

            let query_key: Vec<u8> = match &last_key {
                None => prefix.as_bytes().to_vec(),
                Some(key) => key.clone(),
            };

            let resp = client
                .get(query_key, Some(get_options))
                .await
                .map_err(|e| Error::Storage(format!("Failed to list resources: {}", e)))?;

            let kvs = resp.kvs();
            for kv in kvs {
                // Skip the last_key itself (from_key is inclusive)
                if let Some(ref lk) = last_key {
                    if kv.key() == lk.as_slice() {
                        continue;
                    }
                }

                // Ensure key still has the prefix (from_key may go beyond prefix)
                let key_str = kv
                    .key_str()
                    .map_err(|e| Error::Storage(format!("Invalid UTF-8 in key: {}", e)))?;
                if !key_str.starts_with(prefix) {
                    // We've gone past the prefix range, stop
                    debug!("Listed {} resources with prefix: {}", results.len(), prefix);
                    return Ok(results);
                }

                let json = kv
                    .value_str()
                    .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;
                let mod_revision = kv.mod_revision();

                // Inject resourceVersion and deserialize in one step
                let json_with_rv = Self::inject_resource_version(json, mod_revision);
                match serde_json::from_str::<T>(&json_with_rv) {
                    Ok(value) => {
                        results.push(value);
                    }
                    Err(e) => {
                        error!("Failed to deserialize value at {}: {}", key_str, e);
                        continue;
                    }
                }
            }

            // If we got fewer results than PAGE_SIZE, we've reached the end
            let total_kvs = kvs.len() as i64;
            let expected = if last_key.is_some() {
                PAGE_SIZE + 1
            } else {
                PAGE_SIZE
            };
            if total_kvs < expected {
                break;
            }

            // Set last_key to the last key we received for the next page
            if let Some(last_kv) = kvs.last() {
                last_key = Some(last_kv.key().to_vec());
            } else {
                break;
            }
        }

        debug!("Listed {} resources with prefix: {}", results.len(), prefix);
        Ok(results)
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        let mut client = self.client.clone();
        let watch_options = WatchOptions::new()
            .with_prefix()
            .with_prev_key()
            .with_start_revision(revision);
        let (watcher, stream) = client
            .watch(prefix, Some(watch_options))
            .await
            .map_err(|e| {
                Error::Storage(format!(
                    "Failed to create watch from revision {}: {}",
                    revision, e
                ))
            })?;
        info!(
            "Started watching prefix: {} from revision {}",
            prefix, revision
        );
        // Use flat_map to handle multiple events per etcd watch response.
        // etcd can batch multiple events into a single response, and we must
        // emit all of them — not just the first one.
        // IMPORTANT: Move `watcher` into the closure to keep the watch alive.
        // Dropping it closes the gRPC stream, which terminates the watch.
        let watch_stream = stream.flat_map(move |watch_resp| {
            let _ = &watcher;
            let events: Vec<Result<WatchEvent>> = match watch_resp {
                Ok(resp) => {
                    resp.events().iter().map(|event| {
                        let key = event
                            .kv()
                            .map(|kv| kv.key_str().unwrap_or("").to_string())
                            .unwrap_or_default();
                        match event.event_type() {
                            etcd_client::EventType::Put => {
                                let raw_value = event
                                    .kv()
                                    .map(|kv| String::from_utf8_lossy(kv.value()).to_string())
                                    .unwrap_or_default();
                                let mod_revision = event.kv().map(|kv| kv.mod_revision()).unwrap_or(0);
                                let value = Self::inject_resource_version(&raw_value, mod_revision);
                                // Use etcd key version to distinguish create vs update:
                                // version=1 means first write (create), >1 means update.
                                // This is more reliable than prev_kv() which may be absent
                                // after etcd compaction.
                                let kv_version = event.kv().map(|kv| kv.version()).unwrap_or(0);
                                debug!("etcd watch_from_rev event: key={} mod_rev={} version={} type={}",
                                    key, mod_revision, kv_version,
                                    if kv_version == 1 { "ADDED" } else { "MODIFIED" });
                                if kv_version == 1 {
                                    Ok(WatchEvent::Added(key, value))
                                } else {
                                    Ok(WatchEvent::Modified(key, value))
                                }
                            }
                            etcd_client::EventType::Delete => {
                                let mod_revision = event.kv().map(|kv| kv.mod_revision()).unwrap_or(0);
                                // Use prev_kv for the deleted object's value.
                                // If prev_kv is missing (etcd compaction), construct a
                                // minimal JSON object with just metadata so watchers
                                // can still deliver the DELETE event.
                                let prev_value = if let Some(prev_kv) = event.prev_kv() {
                                    let raw = String::from_utf8_lossy(prev_kv.value()).to_string();
                                    Self::inject_resource_version(&raw, mod_revision)
                                } else {
                                    // Extract name from key: /registry/{type}/{ns}/{name}
                                    let parts: Vec<&str> = key.split('/').collect();
                                    let name = parts.last().unwrap_or(&"");
                                    let ns = if parts.len() >= 4 { parts[parts.len()-2] } else { "" };
                                    format!(
                                        r#"{{"metadata":{{"name":"{}","namespace":"{}","resourceVersion":"{}"}}}}"#,
                                        name, ns, mod_revision
                                    )
                                };
                                Ok(WatchEvent::Deleted(key, prev_value))
                            }
                        }
                    }).collect()
                }
                Err(e) => vec![Err(Error::Storage(format!("Watch error: {}", e)))],
            };
            futures::stream::iter(events)
        });
        Ok(Box::pin(watch_stream))
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        let mut client = self.client.clone();

        // Enable prev_kv to get the previous value on DELETE events (required for Kubernetes)
        let watch_options = WatchOptions::new().with_prefix().with_prev_key();
        let (watcher, stream) = client
            .watch(prefix, Some(watch_options))
            .await
            .map_err(|e| Error::Storage(format!("Failed to create watch: {}", e)))?;

        info!("Started watching prefix: {}", prefix);

        // Convert etcd watch stream to our WatchStream.
        // Use flat_map to handle multiple events per etcd watch response.
        // IMPORTANT: Move `watcher` into the closure to keep the watch alive.
        // Dropping it closes the gRPC stream, which terminates the watch.
        let watch_stream = stream.flat_map(move |watch_resp| {
            let _ = &watcher;
            let events: Vec<Result<WatchEvent>> = match watch_resp {
                Ok(resp) => resp
                    .events()
                    .iter()
                    .map(|event| {
                        let key = event
                            .kv()
                            .map(|kv| kv.key_str().unwrap_or("").to_string())
                            .unwrap_or_default();

                        match event.event_type() {
                            etcd_client::EventType::Put => {
                                let raw_value = event
                                    .kv()
                                    .and_then(|kv| kv.value_str().ok())
                                    .unwrap_or("")
                                    .to_string();

                                let mod_revision =
                                    event.kv().map(|kv| kv.mod_revision()).unwrap_or(0);
                                let value = Self::inject_resource_version(&raw_value, mod_revision);

                                if event.kv().map(|kv| kv.version()).unwrap_or(0) == 1 {
                                    Ok(WatchEvent::Added(key, value))
                                } else {
                                    Ok(WatchEvent::Modified(key, value))
                                }
                            }
                            etcd_client::EventType::Delete => {
                                let raw_prev = event
                                    .prev_kv()
                                    .and_then(|kv| kv.value_str().ok())
                                    .unwrap_or("")
                                    .to_string();
                                let mod_revision =
                                    event.kv().map(|kv| kv.mod_revision()).unwrap_or(0);
                                let prev_value =
                                    Self::inject_resource_version(&raw_prev, mod_revision);
                                Ok(WatchEvent::Deleted(key, prev_value))
                            }
                        }
                    })
                    .collect(),
                Err(e) => vec![Err(Error::Storage(format!("Watch error: {}", e)))],
            };
            futures::stream::iter(events)
        });

        Ok(Box::pin(watch_stream))
    }

    async fn current_revision(&self) -> Result<i64> {
        let mut client = self.client.clone();
        // Use keys_only to minimize data transfer
        let resp = client
            .get("/", Some(GetOptions::new().with_keys_only()))
            .await
            .map_err(|e| Error::Storage(format!("Failed to get current revision: {}", e)))?;
        Ok(resp.header().unwrap().revision())
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        let mut client = self.client.clone();
        // Try to get a key at the given revision; if compacted, etcd returns an error
        let opts = GetOptions::new().with_revision(revision).with_keys_only();
        match client.get("/registry/", Some(opts)).await {
            Ok(_) => Ok(false), // revision still available
            Err(e) => {
                let err_msg = format!("{}", e);
                if err_msg.contains("compacted")
                    || err_msg.contains("required revision has been compacted")
                {
                    Ok(true) // revision has been compacted
                } else {
                    // Other error — not a compaction issue
                    Ok(false)
                }
            }
        }
    }
}

// Implement AuthzStorage for EtcdStorage
#[async_trait]
impl AuthzStorage for EtcdStorage {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        // Build the full key based on the resource type and namespace.
        // IMPORTANT: order matters — `type_name::<RoleBinding>()` also matches
        // `contains("Role")`, so the more-specific `RoleBinding`/`ClusterRoleBinding`
        // checks must come first or they get shadowed.
        let full_key = match namespace {
            Some(ns) => {
                if std::any::type_name::<T>().contains("RoleBinding")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/rolebindings/{}/{}", ns, key)
                } else if std::any::type_name::<T>().contains("Role")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/roles/{}/{}", ns, key)
                } else {
                    format!("/registry/unknown/{}/{}", ns, key)
                }
            }
            None => {
                if std::any::type_name::<T>().contains("ClusterRoleBinding") {
                    format!("/registry/clusterrolebindings/{}", key)
                } else if std::any::type_name::<T>().contains("ClusterRole")
                    && !std::any::type_name::<T>().contains("Binding")
                {
                    format!("/registry/clusterroles/{}", key)
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
        // IMPORTANT: order matters — `type_name::<RoleBinding>()` also matches
        // `contains("Role")`, so the more-specific `RoleBinding`/`ClusterRoleBinding`
        // checks must come first or they get shadowed.
        let prefix = match namespace {
            Some(ns) => {
                if std::any::type_name::<T>().contains("RoleBinding")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/rolebindings/{}/", ns)
                } else if std::any::type_name::<T>().contains("Role")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/roles/{}/", ns)
                } else {
                    format!("/registry/unknown/{}/", ns)
                }
            }
            None => {
                if std::any::type_name::<T>().contains("ClusterRoleBinding") {
                    "/registry/clusterrolebindings/".to_string()
                } else if std::any::type_name::<T>().contains("ClusterRole")
                    && !std::any::type_name::<T>().contains("Binding")
                {
                    "/registry/clusterroles/".to_string()
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

    #[test]
    fn test_inject_resource_version_with_existing() {
        let json = r#"{"metadata":{"name":"test","resourceVersion":"100"},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 200);
        assert!(result.contains("\"200\""));
        assert!(!result.contains("\"100\""));
    }

    #[test]
    fn test_inject_resource_version_without_existing() {
        let json = r#"{"metadata":{"name":"test"},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 42);
        assert!(result.contains("\"resourceVersion\":\"42\""));
    }

    #[test]
    fn test_inject_resource_version_empty_metadata() {
        let json = r#"{"metadata":{},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 99);
        assert!(result.contains("\"resourceVersion\":\"99\""));
    }

    // These tests exercise the real etcd backend by spinning up a disposable
    // etcd container via `testcontainers`. They require Docker (or a
    // Docker-compatible socket) on the host. The container is torn down
    // automatically when the `ContainerAsync` handle is dropped at the end of
    // the test, so no manual cleanup is required.
    //
    // On runners without a Docker socket (e.g. the ARC self-hosted runners
    // that back this repo's `cargo nextest` workflow), `start_etcd` returns
    // `None` and the test prints a skip message and exits 0 instead of
    // panicking. Don't promote this back to `expect("...")` — that's exactly
    // the regression that turned PR #746's nextest job red.
    //
    // The image and CLI flags match what `compose.yml` runs in production
    // (`quay.io/coreos/etcd:v3.5.17`, single-node, insecure client listener).

    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        GenericImage, ImageExt, TestcontainersError,
    };

    /// Detects whether the host has a usable Docker (or Docker-compatible)
    /// socket. We treat any `Client(Init(...))` failure from testcontainers
    /// as "no Docker" — that variant wraps the bollard connect error, which
    /// fires both for a missing `/var/run/docker.sock` and for a refused
    /// connection to a custom `DOCKER_HOST`. Either way the test cannot run.
    fn is_docker_unavailable(err: &TestcontainersError) -> bool {
        matches!(
            err,
            TestcontainersError::Client(testcontainers::core::client::ClientError::Init(_))
        )
    }

    /// Boot a single-node etcd container and return an `EtcdStorage` pointing
    /// at it, alongside the container handle which must be kept alive for the
    /// duration of the test (drop = teardown).
    ///
    /// Returns `None` when Docker isn't reachable so callers can soft-skip
    /// rather than fail the whole nextest job on Docker-less runners.
    async fn start_etcd() -> Option<(testcontainers::ContainerAsync<GenericImage>, EtcdStorage)> {
        // `quay.io/coreos/etcd` listens on 2379 for client gRPC and prints
        // "ready to serve client requests" once the listener is up. We bind
        // 0.0.0.0 so the port mapping works from the host network.
        let result = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
            .with_exposed_port(2379.tcp())
            .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
            .with_cmd([
                "/usr/local/bin/etcd",
                "--name=etcd-test",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://0.0.0.0:2379",
            ])
            .start()
            .await;

        let container = match result {
            Ok(c) => c,
            Err(e) if is_docker_unavailable(&e) => {
                eprintln!("skipping etcd integration test: Docker unavailable ({e})");
                return None;
            }
            Err(e) => panic!("failed to start etcd test container: {e}"),
        };

        let host = container
            .get_host()
            .await
            .expect("failed to resolve test container host");
        let port = container
            .get_host_port_ipv4(2379)
            .await
            .expect("failed to read mapped etcd client port");

        let endpoint = format!("http://{host}:{port}");
        let storage = EtcdStorage::new(vec![endpoint])
            .await
            .expect("failed to connect to test etcd");

        Some((container, storage))
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let Some((_etcd, storage)) = start_etcd().await else {
            return;
        };

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            name: String,
            value: i32,
        }

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let created = Storage::create(&storage, "/test/key", &data).await.unwrap();
        assert_eq!(created, data);

        let retrieved: TestData = Storage::get(&storage, "/test/key").await.unwrap();
        assert_eq!(retrieved, data);

        storage.delete("/test/key").await.unwrap();
    }
}
