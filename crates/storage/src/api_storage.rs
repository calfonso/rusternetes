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
//!   built-in GVR table ([`static_resource_info`]) with a discovery fallback;
//! - `get`/`list` → GET, `create` → POST, `update` → PUT, `delete` → DELETE;
//! - `update_status` → PUT to the `/status` subresource (grafted onto a fresh
//!   GET so a stale caller cannot clobber spec, mirroring the storage impl);
//! - `watch` → the api-server's `?watch=true` JSON-lines stream, re-keyed to
//!   the `/registry/...` form [`rusternetes_storage::extract_key`] expects, and
//!   **shared**: all controllers watching the same resource type fan out from a
//!   single upstream connection via a broadcast (#1138).
//!
//! ## Known gaps (tracked follow-ups, not adapter bugs)
//!
//! - **Status via `update`.** Resources whose api-server handler enforces the
//!   status subresource (e.g. pods) strip `.status` on a full-object PUT — so a
//!   controller that persists status via [`Storage::update`] rather than
//!   [`Storage::update_status`] will not see it stick in API mode. That is
//!   faithful Kubernetes behavior; the fix is per-controller (use the status
//!   subresource), not here.
//! - **GVR resolution.** Built-in types resolve through a static table
//!   ([`static_resource_info`]) with no network. Types not in the table (CRDs,
//!   aggregated APIs, and the arbitrary types the garbage collector traverses)
//!   are resolved by loading the api-server's discovery once and caching it. A
//!   plural that is neither built-in nor present in discovery (e.g. a CRD whose
//!   controller runs but whose CRD is not registered) errors via [`unmapped`].
//! - **`current_revision`/`is_revision_compacted`** are best-effort (0 / false):
//!   no controller in this crate calls them, and `list_paginated` is a handler
//!   concern, not a controller one.

use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use futures::StreamExt;
use rusternetes_client::http::{ApiClient, GetError, KubernetesList};
use rusternetes_client::watch::{watch_stream, WatchEvent as ClientWatchEvent};
use rusternetes_common::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};

/// Per-resource-type buffer for the shared watch broadcast. A subscriber that
/// falls more than this many events behind is told to relist (see `watch`).
///
/// tokio's broadcast eagerly allocates all `N` slots up front, so this is a
/// fixed per-watch cost (× ~one channel per resource type). 256 is ample lag
/// tolerance for promptly-draining controllers while keeping the idle-RAM
/// footprint small (#1138); a lagged subscriber just relists.
const SHARED_WATCH_BUFFER: usize = 256;

/// Registry of live shared upstream watches, keyed by resolved collection path.
type SharedWatches = Arc<Mutex<HashMap<String, broadcast::Sender<Arc<WatchEvent>>>>>;

/// A [`Storage`] implementation that proxies to the api-server over REST.
pub struct ApiStorage {
    client: Arc<ApiClient>,
    /// Discovery-resolved `plural -> (api_root, namespaced)` for types not in
    /// the built-in [`static_resource_info`] table (CRDs, aggregated APIs, and
    /// the arbitrary types the garbage collector traverses). `None` until the
    /// first miss triggers a one-shot discovery load; `Some` thereafter.
    dynamic: RwLock<Option<HashMap<String, (String, bool)>>>,
    /// One shared upstream `?watch=true` stream per resolved collection path,
    /// fanned out to every controller watching that type via a broadcast — so
    /// the ~50 controller watch() calls collapse to ~one HTTP connection per
    /// resource type (idle-RAM, #1138).
    shared_watches: SharedWatches,
}

impl ApiStorage {
    pub fn new(client: Arc<ApiClient>) -> Self {
        Self {
            client,
            dynamic: RwLock::new(None),
            shared_watches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Resolve a plural to `(api_root, namespaced)`. Built-in types hit the
    /// static table with no network; on a miss, the api-server's discovery is
    /// loaded once and cached, then retried, so CRD/aggregated types map too.
    async fn resolve(&self, rt: &str) -> Result<(String, bool)> {
        if let Some((root, namespaced)) = static_resource_info(rt) {
            return Ok((root.to_string(), namespaced));
        }
        // Fast path: already-loaded discovery cache.
        if let Some(map) = self.dynamic.read().await.as_ref() {
            return map.get(rt).cloned().ok_or_else(|| unmapped(rt));
        }
        // Slow path: load discovery once, cache it, then look up. A concurrent
        // miss may load too — idempotent, last write wins.
        let map = self.load_discovery().await;
        let found = map.get(rt).cloned();
        *self.dynamic.write().await = Some(map);
        found.ok_or_else(|| unmapped(rt))
    }

    /// Like [`Self::resolve`] but yields `None` for a type the api-server does
    /// not serve, so `list`/`watch` can mirror storage mode's "empty
    /// collection" semantics instead of erroring (and log-spamming) on, e.g., a
    /// CRD whose controller runs but whose CRD is not registered.
    async fn try_resolve(&self, rt: &str) -> Option<(String, bool)> {
        self.resolve(rt).await.ok()
    }

    /// Build `plural -> (api_root, namespaced)` from the api-server's discovery
    /// documents: core `/api/v1`, then every group's preferred version. Best
    /// effort — discovery failures yield an empty map (callers then error with
    /// `unmapped`), never a panic.
    async fn load_discovery(&self) -> HashMap<String, (String, bool)> {
        let mut map = HashMap::new();
        // Core group.
        if let Ok(list) = self.client.get::<Value>("/api/v1").await {
            ingest_resource_list(&mut map, "/api/v1", &list);
        }
        // Named groups: /apis -> APIGroupList, then each preferred groupVersion.
        if let Ok(groups) = self.client.get::<Value>("/apis").await {
            if let Some(arr) = groups.get("groups").and_then(|g| g.as_array()) {
                for g in arr {
                    let gv = g
                        .get("preferredVersion")
                        .and_then(|pv| pv.get("groupVersion"))
                        .and_then(|v| v.as_str());
                    if let Some(gv) = gv {
                        let root = format!("/apis/{gv}");
                        if let Ok(list) = self.client.get::<Value>(&root).await {
                            ingest_resource_list(&mut map, &root, &list);
                        }
                    }
                }
            }
        }
        map
    }
}

/// `Error` for a plural that is neither built-in nor present in discovery.
fn unmapped(rt: &str) -> Error {
    Error::Internal(format!(
        "ApiStorage: resource type '{rt}' not served by the api-server (not built-in, absent from discovery)"
    ))
}

/// Query params that force an immediate (hard) delete on the api-server.
///
/// `Storage::delete` is unconditional removal in every backend. The api-server
/// only removes a pod immediately when `gracePeriodSeconds=0`; otherwise it
/// does a graceful (soft) delete that just stamps `deletionTimestamp`. Sending
/// grace=0 keeps the Api backend's `delete` consistent with the others — see
/// the call site in `ApiStorage::delete` for the full regression context.
fn force_delete_query() -> Vec<(String, String)> {
    vec![("gracePeriodSeconds".to_string(), "0".to_string())]
}

/// Fold an `APIResourceList` (`{resources: [{name, namespaced}, ...]}`) into the
/// dynamic map under `root`, skipping subresources (names containing `/`).
fn ingest_resource_list(map: &mut HashMap<String, (String, bool)>, root: &str, list: &Value) {
    let Some(resources) = list.get("resources").and_then(|r| r.as_array()) else {
        return;
    };
    for r in resources {
        let Some(name) = r.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        if name.contains('/') {
            continue; // subresource (e.g. "deployments/status")
        }
        let namespaced = r
            .get("namespaced")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        map.entry(name.to_string())
            .or_insert((root.to_string(), namespaced));
    }
}

/// Resolve a built-in resource-type plural to its REST API root and namespacing.
///
/// Covers the group/versions the api-server router serves natively. Returns
/// `(api_root, namespaced)` where `api_root` is e.g. `/api/v1` (core) or
/// `/apis/apps/v1`; `None` for non-built-in types (resolved via discovery).
fn static_resource_info(rt: &str) -> Option<(&'static str, bool)> {
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
        _ => return None,
    };
    Some(info)
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

/// Build the REST path for a single object key (`get`/`update`/`delete`) from
/// already-resolved `(root, namespaced)`.
fn build_object_path(root: &str, namespaced: bool, rt: &str, rest: &[String]) -> Result<String> {
    if namespaced {
        match rest {
            [ns, name] => Ok(format!("{root}/namespaces/{ns}/{rt}/{name}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected namespaced object key /registry/{rt}/<ns>/<name>"
            ))),
        }
    } else {
        match rest {
            [name] => Ok(format!("{root}/{rt}/{name}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected cluster object key /registry/{rt}/<name>"
            ))),
        }
    }
}

/// Build the REST collection path for an object key (drops the name) — `create`.
fn build_collection_for_object(
    root: &str,
    namespaced: bool,
    rt: &str,
    rest: &[String],
) -> Result<String> {
    if namespaced {
        match rest {
            [ns, _name] => Ok(format!("{root}/namespaces/{ns}/{rt}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected namespaced object key for create of {rt}"
            ))),
        }
    } else {
        Ok(format!("{root}/{rt}"))
    }
}

/// Build the REST collection path for a list/watch prefix.
fn build_collection_for_prefix(
    root: &str,
    namespaced: bool,
    rt: &str,
    rest: &[String],
) -> Result<String> {
    if namespaced {
        match rest {
            [] => Ok(format!("{root}/{rt}")),
            [ns] => Ok(format!("{root}/namespaces/{ns}/{rt}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: unexpected list prefix for {rt}"
            ))),
        }
    } else {
        Ok(format!("{root}/{rt}"))
    }
}

impl ApiStorage {
    /// Async REST path for a single object key (resolves the GVR first).
    async fn object_path(&self, key: &str) -> Result<String> {
        let (rt, rest) = parse_key(key)?;
        let (root, namespaced) = self.resolve(&rt).await?;
        build_object_path(&root, namespaced, &rt, &rest)
    }

    /// Async REST collection path for an object key (`create`).
    async fn collection_for_object(&self, key: &str) -> Result<String> {
        let (rt, rest) = parse_key(key)?;
        let (root, namespaced) = self.resolve(&rt).await?;
        build_collection_for_object(&root, namespaced, &rt, &rest)
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
        let path = self.collection_for_object(key).await?;
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
        let path = self.object_path(key).await?;
        let v: Value = self.client.get(&path).await.map_err(map_get_err)?;
        serde_json::from_value(v).map_err(Error::Serialization)
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = self.object_path(key).await?;
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

        let path = self.object_path(key).await?;
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
        let path = self.object_path(key).await?;
        let _: Value = self.client.put(&path, value).await.map_err(map_write_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.object_path(key).await?;
        // `Storage::delete` is a HARD delete in every backend (memory / rhino /
        // etcd remove the key outright). The api-server's pod DELETE, however,
        // defaults to GRACEFUL deletion: it merely stamps deletionTimestamp and
        // keeps the object until a force delete arrives — it only removes the
        // object when gracePeriodSeconds=0 (see api-server handlers/pod.rs).
        // The kubelet runs as an api-server client (StorageBackend::Api) and
        // calls this in `finalize_terminated_pod_storage` to complete deletion
        // of an already-terminated, already-deletionTimestamp'd pod. Without
        // gracePeriodSeconds=0 that final delete is a no-op: the pod lingers,
        // the kubelet worker re-terminates it every reconcile forever, and the
        // object is never removed (conformance "wait for pod to disappear"
        // 300s timeouts; pod-deletion regression from #1150). Force grace=0 so
        // the Api backend honours the same hard-delete contract as the others.
        let status = self
            .client
            .delete_with_options(&path, &force_delete_query(), None)
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
        let (rt, rest) = parse_key(prefix)?;
        let Some((root, namespaced)) = self.try_resolve(&rt).await else {
            // Type not served by the api-server — mirror storage mode, where an
            // unknown prefix simply lists empty (no error, no log spam).
            return Ok(Vec::new());
        };
        let path = build_collection_for_prefix(&root, namespaced, &rt, &rest)?;
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
    /// Subscribe to a shared upstream `?watch=true` stream for `prefix`'s
    /// collection. The first subscriber for a path spawns one upstream task
    /// (re-keying events into `/registry/...` form and broadcasting them);
    /// later subscribers join its broadcast. So the ~50 controller watch()
    /// calls collapse to one HTTP connection per resource type (#1138).
    ///
    /// `rv` is intentionally ignored: a shared stream cannot honor a
    /// per-subscriber resourceVersion, and controllers relist on (re)connect.
    async fn watch_inner(&self, prefix: &str, _rv: Option<String>) -> Result<WatchStream> {
        let (rt, rest) = parse_key(prefix)?;
        let Some((root, namespaced)) = self.try_resolve(&rt).await else {
            // Type not served — a stream that never yields, mirroring a watch on
            // an empty storage prefix (the controller idles; any periodic
            // relist also returns empty).
            return Ok(futures::stream::pending().boxed());
        };
        let path = build_collection_for_prefix(&root, namespaced, &rt, &rest)?;

        // Get-or-create the shared upstream for this collection path.
        let rx = {
            let mut guard = self.shared_watches.lock().await;
            match guard.get(&path) {
                Some(tx) => tx.subscribe(),
                None => {
                    let (tx, rx) = broadcast::channel::<Arc<WatchEvent>>(SHARED_WATCH_BUFFER);
                    guard.insert(path.clone(), tx.clone());
                    tokio::spawn(shared_upstream(
                        self.client.clone(),
                        path,
                        rt,
                        namespaced,
                        tx,
                        self.shared_watches.clone(),
                    ));
                    rx
                }
            }
        };

        // Adapt the broadcast receiver to a storage `WatchStream`. A lagged
        // subscriber (fell behind the buffer) yields an error so its controller
        // relists+reconnects; a closed channel (upstream ended) ends the stream
        // so the controller reconnects, recreating the upstream.
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(ev) => Some((Ok((*ev).clone()), rx)),
                Err(broadcast::error::RecvError::Lagged(n)) => Some((
                    Err(Error::Network(format!(
                        "shared watch lagged {n} events; relist"
                    ))),
                    rx,
                )),
                Err(broadcast::error::RecvError::Closed) => None,
            }
        });

        Ok(stream.boxed())
    }
}

/// The single upstream task behind a shared watch: read the api-server's
/// `?watch=true` stream for `path`, re-key each event into `/registry/...`
/// form, and broadcast it to all subscribers. Exits (and deregisters itself)
/// when the upstream closes/errors or when no subscribers remain — at which
/// point any surviving/late subscriber sees the channel close and reconnects,
/// recreating a fresh upstream.
async fn shared_upstream(
    client: Arc<ApiClient>,
    path: String,
    rt: String,
    namespaced: bool,
    tx: broadcast::Sender<Arc<WatchEvent>>,
    registry: SharedWatches,
) {
    let raw = match watch_stream::<Value>(&client, &path, None).await {
        Ok(s) => s,
        Err(_) => {
            // Connect failed; drop the registration so the next watch() retries.
            remove_shared(&registry, &path, &tx).await;
            return;
        }
    };
    futures::pin_mut!(raw);
    while let Some(ev) = raw.next().await {
        let mapped: Option<WatchEvent> = match ev {
            Ok(ClientWatchEvent::Added(o)) => storage_key_for(&rt, namespaced, &o)
                .ok()
                .map(|k| WatchEvent::Added(k, o.to_string())),
            Ok(ClientWatchEvent::Modified(o)) => storage_key_for(&rt, namespaced, &o)
                .ok()
                .map(|k| WatchEvent::Modified(k, o.to_string())),
            Ok(ClientWatchEvent::Deleted(o)) => storage_key_for(&rt, namespaced, &o)
                .ok()
                .map(|k| WatchEvent::Deleted(k, o.to_string())),
            // Bookmarks are watch-progress markers, not resource deltas.
            Ok(ClientWatchEvent::Bookmark(_)) => None,
            // Upstream error → end; subscribers see close and reconnect.
            Err(_) => break,
        };
        if let Some(ev) = mapped {
            // `send` errors only when there are zero receivers — every
            // controller has dropped this watch, so stop holding the connection.
            if tx.send(Arc::new(ev)).is_err() {
                break;
            }
        }
    }
    remove_shared(&registry, &path, &tx).await;
}

/// Deregister a shared upstream, but only if the registry still points at *this*
/// sender — a concurrent `watch()` may have already replaced it with a fresh one.
async fn remove_shared(
    registry: &SharedWatches,
    path: &str,
    tx: &broadcast::Sender<Arc<WatchEvent>>,
) {
    let mut guard = registry.lock().await;
    if let Some(current) = guard.get(path) {
        if current.same_channel(tx) {
            guard.remove(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Resolve a built-in object key through the static table + pure builder,
    /// mirroring the old free-fn `object_path` for test purposes.
    fn static_object_path(key: &str) -> Result<String> {
        let (rt, rest) = parse_key(key)?;
        let (root, namespaced) = static_resource_info(&rt).ok_or_else(|| unmapped(&rt))?;
        build_object_path(root, namespaced, &rt, &rest)
    }

    fn static_collection_prefix(prefix: &str) -> Result<String> {
        let (rt, rest) = parse_key(prefix)?;
        let (root, namespaced) = static_resource_info(&rt).ok_or_else(|| unmapped(&rt))?;
        build_collection_for_prefix(root, namespaced, &rt, &rest)
    }

    /// Regression: the Api backend's `delete` must force an immediate (hard)
    /// delete. Without `gracePeriodSeconds=0` the api-server only soft-deletes
    /// a pod (stamps deletionTimestamp), so the kubelet's finalize never
    /// removes an already-terminated pod and loops forever (#1150 pod-deletion
    /// regression — conformance "wait for pod to disappear" 300s timeouts).
    #[test]
    fn delete_forces_zero_grace_period() {
        let q = force_delete_query();
        assert!(
            q.iter().any(|(k, v)| k == "gracePeriodSeconds" && v == "0"),
            "ApiStorage::delete must send gracePeriodSeconds=0 for a hard delete, got {q:?}"
        );
    }

    #[test]
    fn object_path_namespaced_and_cluster() {
        assert_eq!(
            static_object_path("/registry/pods/default/nginx").unwrap(),
            "/api/v1/namespaces/default/pods/nginx"
        );
        assert_eq!(
            static_object_path("/registry/deployments/kube-system/coredns").unwrap(),
            "/apis/apps/v1/namespaces/kube-system/deployments/coredns"
        );
        assert_eq!(
            static_object_path("/registry/nodes/node-1").unwrap(),
            "/api/v1/nodes/node-1"
        );
        assert_eq!(
            static_object_path("/registry/priorityclasses/high").unwrap(),
            "/apis/scheduling.k8s.io/v1/priorityclasses/high"
        );
    }

    #[test]
    fn collection_paths() {
        assert_eq!(
            static_collection_prefix("/registry/replicasets/").unwrap(),
            "/apis/apps/v1/replicasets"
        );
        assert_eq!(
            static_collection_prefix("/registry/pods/default/").unwrap(),
            "/api/v1/namespaces/default/pods"
        );
        assert_eq!(
            static_collection_prefix("/registry/nodes/").unwrap(),
            "/api/v1/nodes"
        );
        let (rt, rest) = parse_key("/registry/pods/default/nginx").unwrap();
        let (root, ns) = static_resource_info(&rt).unwrap();
        assert_eq!(
            build_collection_for_object(root, ns, &rt, &rest).unwrap(),
            "/api/v1/namespaces/default/pods"
        );
    }

    #[test]
    fn non_builtin_type_is_not_static() {
        // CRD types fall through the static table (resolved via discovery at
        // runtime); they are not hard-coded.
        assert!(static_resource_info("verticalpodautoscalers").is_none());
        assert!(static_object_path("/registry/verticalpodautoscalers/ns/v").is_err());
    }

    #[test]
    fn discovery_ingest_maps_plurals_and_skips_subresources() {
        let mut map = HashMap::new();
        let list = serde_json::json!({
            "resources": [
                {"name": "volumesnapshots", "namespaced": true},
                {"name": "volumesnapshots/status", "namespaced": true},
                {"name": "volumesnapshotcontents", "namespaced": false},
            ]
        });
        ingest_resource_list(&mut map, "/apis/snapshot.storage.k8s.io/v1", &list);
        assert_eq!(
            map.get("volumesnapshots"),
            Some(&("/apis/snapshot.storage.k8s.io/v1".to_string(), true))
        );
        assert_eq!(
            map.get("volumesnapshotcontents"),
            Some(&("/apis/snapshot.storage.k8s.io/v1".to_string(), false))
        );
        // Subresource entry must not become its own mapping.
        assert!(!map.contains_key("volumesnapshots/status"));
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
