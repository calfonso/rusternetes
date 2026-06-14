//! [`ApiStorage`]: a [`Storage`] implementation backed by the api-server's
//! REST API instead of a storage backend directly.
//!
//! Every controller in this crate is generic over `S: Storage` and touches
//! cluster state only through the trait. `ApiStorage` lets the SAME controllers
//! run as an api-server *client* — the in-cluster path, and the all-in-one
//! binary over loopback — with no per-controller change: each trait call is
//! translated to the matching REST verb.
//!
//! - storage keys `/registry/{plural}/{ns}/{name}` ↔ REST paths
//!   (`/api/v1/...` or `/apis/{group}/{version}/...`), resolved through a
//!   built-in GVR table ([`resource_info`]);
//! - `get`/`list` → GET, `create` → POST, `update` → PUT, `delete` → DELETE;
//! - `update_status` → PUT to the `/status` subresource (grafted onto a fresh
//!   GET so a stale caller cannot clobber spec, mirroring the storage impl);
//! - `watch` → the api-server's `?watch=true` JSON-lines stream, re-keyed to
//!   the `/registry/...` form [`rusternetes_storage::extract_key`] expects.
//!
//! ## Known gaps (tracked follow-ups, not adapter bugs)
//!
//! - **Status via `update`.** Resources whose api-server handler enforces the
//!   status subresource (e.g. pods) strip `.status` on a full-object PUT — so a
//!   controller that persists status via [`Storage::update`] rather than
//!   [`Storage::update_status`] will not see it stick in API mode. That is
//!   faithful Kubernetes behavior; the fix is per-controller (use the status
//!   subresource), not here.
//! - **Unmapped types.** CRDs the built-in table doesn't know (VPA, volume
//!   snapshots) return [`Error::Internal`]; wiring those (likely via discovery)
//!   is a follow-up.
//! - **`current_revision`/`is_revision_compacted`** are best-effort (0 / false):
//!   no controller in this crate calls them, and `list_paginated` is a handler
//!   concern, not a controller one.

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::StreamExt;
use rusternetes_client::http::{ApiClient, GetError, KubernetesList};
use rusternetes_client::watch::{watch_stream, WatchEvent as ClientWatchEvent};
use rusternetes_common::{Error, Result};
use rusternetes_storage::{Storage, WatchEvent, WatchStream};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// A [`Storage`] implementation that proxies to the api-server over REST.
pub struct ApiStorage {
    client: Arc<ApiClient>,
}

impl ApiStorage {
    pub fn new(client: Arc<ApiClient>) -> Self {
        Self { client }
    }
}

/// Resolve a resource-type plural to its REST API root and namespacing.
///
/// Mirrors the group/versions the api-server router actually serves (built-in
/// types only). Returns `(api_root, namespaced)` where `api_root` is e.g.
/// `/api/v1` (core) or `/apis/apps/v1`.
fn resource_info(rt: &str) -> Result<(&'static str, bool)> {
    let info = match rt {
        // core /api/v1 — namespaced
        "pods"
        | "services"
        | "endpoints"
        | "configmaps"
        | "secrets"
        | "serviceaccounts"
        | "persistentvolumeclaims"
        | "events"
        | "replicationcontrollers"
        | "limitranges"
        | "resourcequotas"
        | "podtemplates" => ("/api/v1", true),
        // core /api/v1 — cluster-scoped
        "nodes" | "namespaces" | "persistentvolumes" | "componentstatuses" => ("/api/v1", false),
        // apps/v1 — namespaced
        "deployments" | "replicasets" | "statefulsets" | "daemonsets" | "controllerrevisions" => {
            ("/apis/apps/v1", true)
        }
        // batch/v1 — namespaced
        "jobs" | "cronjobs" => ("/apis/batch/v1", true),
        // autoscaling/v2 — namespaced
        "horizontalpodautoscalers" => ("/apis/autoscaling/v2", true),
        // networking.k8s.io/v1
        "ingresses" | "networkpolicies" => ("/apis/networking.k8s.io/v1", true),
        "ingressclasses" | "ipaddresses" | "servicecidrs" => ("/apis/networking.k8s.io/v1", false),
        // discovery.k8s.io/v1 — namespaced
        "endpointslices" => ("/apis/discovery.k8s.io/v1", true),
        // policy/v1 — namespaced
        "poddisruptionbudgets" => ("/apis/policy/v1", true),
        // coordination.k8s.io/v1 — namespaced
        "leases" => ("/apis/coordination.k8s.io/v1", true),
        // storage.k8s.io/v1 — cluster-scoped
        "storageclasses"
        | "volumeattachments"
        | "csinodes"
        | "csidrivers"
        | "volumeattributesclasses" => ("/apis/storage.k8s.io/v1", false),
        // scheduling.k8s.io/v1 — cluster-scoped
        "priorityclasses" => ("/apis/scheduling.k8s.io/v1", false),
        // rbac.authorization.k8s.io/v1
        "roles" | "rolebindings" => ("/apis/rbac.authorization.k8s.io/v1", true),
        "clusterroles" | "clusterrolebindings" => ("/apis/rbac.authorization.k8s.io/v1", false),
        // certificates.k8s.io/v1 — cluster-scoped
        "certificatesigningrequests" => ("/apis/certificates.k8s.io/v1", false),
        // apiextensions.k8s.io/v1 — cluster-scoped
        "customresourcedefinitions" => ("/apis/apiextensions.k8s.io/v1", false),
        // apiregistration.k8s.io/v1 — cluster-scoped
        "apiservices" => ("/apis/apiregistration.k8s.io/v1", false),
        // resource.k8s.io/v1 (DRA)
        "resourceclaims" | "resourceclaimtemplates" => ("/apis/resource.k8s.io/v1", true),
        "deviceclasses" | "resourceslices" => ("/apis/resource.k8s.io/v1", false),
        _ => {
            return Err(Error::Internal(format!(
                "ApiStorage: no REST mapping for resource type '{rt}'"
            )))
        }
    };
    Ok(info)
}

/// Split a `/registry/{plural}/...` key into `(plural, remaining_segments)`.
fn parse_key(key: &str) -> Result<(String, Vec<String>)> {
    let stripped = key.strip_prefix("/registry/").ok_or_else(|| {
        Error::Internal(format!("ApiStorage: key missing /registry/ prefix: {key}"))
    })?;
    let parts: Vec<String> = stripped
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let rt = parts
        .first()
        .cloned()
        .ok_or_else(|| Error::Internal(format!("ApiStorage: empty key {key}")))?;
    Ok((rt, parts[1..].to_vec()))
}

/// REST path for a single object key (`get`/`update`/`delete`).
fn object_path(key: &str) -> Result<String> {
    let (rt, rest) = parse_key(key)?;
    let (root, namespaced) = resource_info(&rt)?;
    if namespaced {
        match rest.as_slice() {
            [ns, name] => Ok(format!("{root}/namespaces/{ns}/{rt}/{name}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected namespaced object key /registry/{rt}/<ns>/<name>, got {key}"
            ))),
        }
    } else {
        match rest.as_slice() {
            [name] => Ok(format!("{root}/{rt}/{name}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected cluster object key /registry/{rt}/<name>, got {key}"
            ))),
        }
    }
}

/// REST collection path for an object key (drops the name) — used by `create`.
fn collection_for_object(key: &str) -> Result<String> {
    let (rt, rest) = parse_key(key)?;
    let (root, namespaced) = resource_info(&rt)?;
    if namespaced {
        match rest.as_slice() {
            [ns, _name] => Ok(format!("{root}/namespaces/{ns}/{rt}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected namespaced object key for create, got {key}"
            ))),
        }
    } else {
        Ok(format!("{root}/{rt}"))
    }
}

/// REST collection path for a list/watch prefix.
fn collection_for_prefix(prefix: &str) -> Result<String> {
    let (rt, rest) = parse_key(prefix)?;
    let (root, namespaced) = resource_info(&rt)?;
    if namespaced {
        match rest.as_slice() {
            [] => Ok(format!("{root}/{rt}")),
            [ns] => Ok(format!("{root}/namespaces/{ns}/{rt}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: unexpected list prefix {prefix}"
            ))),
        }
    } else {
        Ok(format!("{root}/{rt}"))
    }
}

/// Reconstruct the `/registry/...` storage key for a watch-event object.
fn storage_key_for(rt: &str, namespaced: bool, obj: &Value) -> Result<String> {
    let md = obj.get("metadata");
    let name = md
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Internal("ApiStorage: watch object missing metadata.name".into()))?;
    if namespaced {
        let ns = md
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        Ok(format!("/registry/{rt}/{ns}/{name}"))
    } else {
        Ok(format!("/registry/{rt}/{name}"))
    }
}

/// Map a write error (`post`/`put`) to the storage [`Error`] vocabulary by the
/// api-server's `Error from server (<Reason>): ...` token. Needed so
/// optimistic-concurrency conflicts surface as [`Error::Conflict`].
fn map_write_err(e: anyhow::Error) -> Error {
    let s = e.to_string();
    if s.contains("(AlreadyExists)") {
        Error::AlreadyExists(s)
    } else if s.contains("(Conflict)") {
        Error::Conflict(s)
    } else if s.contains("(NotFound)") {
        Error::NotFound(s)
    } else if s.contains("(Invalid)") || s.contains("(BadRequest)") {
        Error::BadRequest(s)
    } else if s.contains("(Forbidden)") {
        Error::Forbidden(s)
    } else {
        Error::Storage(s)
    }
}

fn map_get_err(e: GetError) -> Error {
    match e {
        GetError::NotFound => Error::NotFound("resource not found".into()),
        GetError::Other(e) => Error::Storage(e.to_string()),
    }
}

#[async_trait]
impl Storage for ApiStorage {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = collection_for_object(key)?;
        let body = serde_json::to_value(value).map_err(Error::Serialization)?;
        let created: Value = self
            .client
            .post(&path, &body)
            .await
            .map_err(map_write_err)?;
        serde_json::from_value(created).map_err(Error::Serialization)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let path = object_path(key)?;
        let v: Value = self.client.get(&path).await.map_err(map_get_err)?;
        serde_json::from_value(v).map_err(Error::Serialization)
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = object_path(key)?;
        let body = serde_json::to_value(value).map_err(Error::Serialization)?;
        let updated: Value = self.client.put(&path, &body).await.map_err(map_write_err)?;
        serde_json::from_value(updated).map_err(Error::Serialization)
    }

    async fn update_status<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // Graft only the caller's `.status` onto a freshly-read object (so a
        // stale spec can't be written back), then PUT the /status subresource.
        let incoming = serde_json::to_value(value).map_err(Error::Serialization)?;
        let new_status = incoming.get("status").cloned();

        let path = object_path(key)?;
        let mut current: Value = self.client.get(&path).await.map_err(map_get_err)?;
        if let Some(obj) = current.as_object_mut() {
            match new_status {
                Some(status) => {
                    obj.insert("status".to_string(), status);
                }
                None => {
                    obj.remove("status");
                }
            }
        }
        let status_path = format!("{path}/status");
        let updated: Value = self
            .client
            .put(&status_path, &current)
            .await
            .map_err(map_write_err)?;
        serde_json::from_value(updated).map_err(Error::Serialization)
    }

    async fn update_raw(&self, key: &str, value: &Value) -> Result<()> {
        let path = object_path(key)?;
        let _: Value = self.client.put(&path, value).await.map_err(map_write_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = object_path(key)?;
        let status = self
            .client
            .delete_with_options(&path, &[], None)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        if status.is_success() {
            Ok(())
        } else if status.as_u16() == 404 {
            Err(Error::NotFound(format!("{key} not found")))
        } else {
            Err(Error::Storage(format!(
                "delete {key} failed: HTTP {status}"
            )))
        }
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = collection_for_prefix(prefix)?;
        let list: KubernetesList<Value> = self.client.get(&path).await.map_err(map_get_err)?;
        let mut out = Vec::with_capacity(list.items.len());
        for item in list.items {
            out.push(serde_json::from_value(item).map_err(Error::Serialization)?);
        }
        Ok(out)
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        self.watch_inner(prefix, None).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        self.watch_inner(prefix, Some(revision.to_string())).await
    }

    async fn current_revision(&self) -> Result<i64> {
        // Unused by controllers; the api-server owns revisions.
        Ok(0)
    }

    async fn is_revision_compacted(&self, _revision: i64) -> Result<bool> {
        Ok(false)
    }
}

impl ApiStorage {
    /// Establish a `?watch=true` stream and re-key its events into the
    /// `/registry/...` form. A background task owns the `Arc<ApiClient>` and the
    /// underlying byte stream so the returned `WatchStream` is `'static`.
    async fn watch_inner(&self, prefix: &str, rv: Option<String>) -> Result<WatchStream> {
        let (rt, _rest) = parse_key(prefix)?;
        let (_root, namespaced) = resource_info(&rt)?;
        let path = collection_for_prefix(prefix)?;
        let client = self.client.clone();

        let (tx, rx) = mpsc::unbounded::<Result<WatchEvent>>();
        tokio::spawn(async move {
            let raw = match watch_stream::<Value>(&client, &path, rv.as_deref()).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.unbounded_send(Err(Error::Network(e.to_string())));
                    return;
                }
            };
            futures::pin_mut!(raw);
            while let Some(ev) = raw.next().await {
                let mapped: Option<Result<WatchEvent>> = match ev {
                    Ok(ClientWatchEvent::Added(o)) => Some(
                        storage_key_for(&rt, namespaced, &o)
                            .map(|k| WatchEvent::Added(k, o.to_string())),
                    ),
                    Ok(ClientWatchEvent::Modified(o)) => Some(
                        storage_key_for(&rt, namespaced, &o)
                            .map(|k| WatchEvent::Modified(k, o.to_string())),
                    ),
                    Ok(ClientWatchEvent::Deleted(o)) => Some(
                        storage_key_for(&rt, namespaced, &o)
                            .map(|k| WatchEvent::Deleted(k, o.to_string())),
                    ),
                    // Bookmarks are watch-progress markers, not resource deltas.
                    Ok(ClientWatchEvent::Bookmark(_)) => None,
                    Err(e) => Some(Err(Error::Network(e.to_string()))),
                };
                if let Some(item) = mapped {
                    if tx.unbounded_send(item).is_err() {
                        break; // consumer dropped the stream
                    }
                }
            }
        });

        Ok(rx.boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_path_namespaced_and_cluster() {
        assert_eq!(
            object_path("/registry/pods/default/nginx").unwrap(),
            "/api/v1/namespaces/default/pods/nginx"
        );
        assert_eq!(
            object_path("/registry/deployments/kube-system/coredns").unwrap(),
            "/apis/apps/v1/namespaces/kube-system/deployments/coredns"
        );
        assert_eq!(
            object_path("/registry/nodes/node-1").unwrap(),
            "/api/v1/nodes/node-1"
        );
        assert_eq!(
            object_path("/registry/priorityclasses/high").unwrap(),
            "/apis/scheduling.k8s.io/v1/priorityclasses/high"
        );
    }

    #[test]
    fn collection_paths() {
        assert_eq!(
            collection_for_prefix("/registry/replicasets/").unwrap(),
            "/apis/apps/v1/replicasets"
        );
        assert_eq!(
            collection_for_prefix("/registry/pods/default/").unwrap(),
            "/api/v1/namespaces/default/pods"
        );
        assert_eq!(
            collection_for_prefix("/registry/nodes/").unwrap(),
            "/api/v1/nodes"
        );
        assert_eq!(
            collection_for_object("/registry/pods/default/nginx").unwrap(),
            "/api/v1/namespaces/default/pods"
        );
    }

    #[test]
    fn unmapped_type_errors() {
        assert!(object_path("/registry/verticalpodautoscalers/ns/v").is_err());
    }

    #[test]
    fn storage_key_roundtrip() {
        let obj = serde_json::json!({"metadata": {"name": "x", "namespace": "ns"}});
        assert_eq!(
            storage_key_for("pods", true, &obj).unwrap(),
            "/registry/pods/ns/x"
        );
        let cobj = serde_json::json!({"metadata": {"name": "node-1"}});
        assert_eq!(
            storage_key_for("nodes", false, &cobj).unwrap(),
            "/registry/nodes/node-1"
        );
    }
}
