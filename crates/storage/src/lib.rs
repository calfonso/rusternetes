use async_trait::async_trait;
use rusternetes_common::authz::AuthzStorage;
use rusternetes_common::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

#[cfg(feature = "api-client")]
pub mod api_storage;
pub mod concurrency;
pub mod etcd;
mod event_bus;
pub mod event_recorder;
pub mod memory;
pub mod metadata;
#[cfg(any(feature = "sqlite", feature = "redis"))]
pub mod rhino;
pub mod workqueue;

// Re-export MemoryStorage for convenient testing
pub use memory::MemoryStorage;

// Re-export the in-process watch event bus (#1039)
pub use event_bus::EventBus;

// Re-export the unified event recorder
pub use event_recorder::EventRecorder;

// Re-export work queue types
pub use workqueue::{extract_key, WorkQueue, WorkQueueConfig, RECONCILE_ALL_SENTINEL};

// Re-export RhinoStorage when sqlite or redis features are enabled
#[cfg(feature = "sqlite")]
pub type RhinoStorage = rhino::RhinoStorage<::rhino::SqliteBackend>;
#[cfg(feature = "redis")]
pub type RhinoRedisStorage = rhino::RhinoStorage<::rhino::RedisBackend>;

/// Storage trait for persisting Kubernetes resources
#[async_trait]
pub trait Storage: Send + Sync {
    /// Create a new resource
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync;

    /// Get a resource by key
    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync;

    /// Update an existing resource
    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync;

    /// Atomically update ONLY the `.status` subobject of the stored resource,
    /// preserving the currently-stored spec and metadata.
    ///
    /// Mirrors the Kubernetes `/status` subresource (registry status strategy
    /// forces `new.Spec = old.Spec`): a status write must never clobber a
    /// concurrently-updated spec. A background controller computing status from
    /// a stale snapshot would otherwise write its whole stale object back via
    /// [`Storage::update`], reverting a spec the client just changed (this is
    /// the ResourceQuota update+delete conformance flake, GitHub #268).
    ///
    /// Implemented as a compare-and-swap read-modify-write (upstream's
    /// `guaranteedUpdate`): re-read the current object, graft only the incoming
    /// `status` onto it, and write with the fresh resourceVersion so a racing
    /// spec update either wins the CAS (and we retry onto its value) or is
    /// preserved. Only `value`'s `status` field is ever applied.
    async fn update_status<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // Extract only the `status` subobject from the caller's value. The spec
        // (and everything else) comes from the freshly-read stored object, so a
        // stale caller can never overwrite spec.
        let incoming = serde_json::to_value(value).map_err(Error::Serialization)?;
        let new_status = incoming.get("status").cloned();

        // CAS-retry read-modify-write: graft status onto the current object and
        // write with its fresh resourceVersion. If a concurrent writer bumped
        // the version between our read and write, `update`'s optimistic
        // concurrency check fails with Conflict and we re-read onto its value.
        const MAX_ATTEMPTS: usize = 8;
        for attempt in 0..MAX_ATTEMPTS {
            let mut current: serde_json::Value = self.get(key).await?;
            if let Some(obj) = current.as_object_mut() {
                match &new_status {
                    Some(status) => {
                        obj.insert("status".to_string(), status.clone());
                    }
                    None => {
                        obj.remove("status");
                    }
                }
            }
            match self.update::<serde_json::Value>(key, &current).await {
                Ok(updated) => {
                    return serde_json::from_value(updated).map_err(Error::Serialization)
                }
                Err(Error::Conflict(_)) if attempt + 1 < MAX_ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
        Err(Error::Conflict(format!(
            "update_status: exhausted {} CAS retries for {}",
            MAX_ATTEMPTS, key
        )))
    }

    /// Update a resource with raw JSON value (for GC operations)
    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()>;

    /// Delete a resource
    async fn delete(&self, key: &str) -> Result<()>;

    /// List resources with a given prefix
    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync;

    /// Paginated list — return up to `limit` items in deterministic (sorted by
    /// storage key) order and a continue token for the next page, if any.
    ///
    /// `continue_token` is the opaque token returned from a previous call;
    /// `None` requests the first page. When the storage backend has compacted
    /// past the token's referenced revision, returns [`Error::Gone`] so the
    /// handler can surface `410 Gone` with reason `Expired`.
    ///
    /// The default implementation performs in-memory chunking on top of
    /// [`Storage::list`]: list the full prefix, sort by key, slice, and
    /// embed the next sort key in the token. Backends that can stream a
    /// partial range from native pagination (e.g. etcd `RangeRequest.limit`)
    /// may override for efficiency.
    ///
    /// This is a *storage-level* primitive that resumes by sort key. The
    /// handler-level offset-based helper in `rusternetes_common::pagination`
    /// (see `paginate`) operates on already-fetched `Vec<T>` and is used by
    /// resource handlers that need to filter/decorate items before paging.
    async fn list_paginated<T>(
        &self,
        prefix: &str,
        limit: usize,
        continue_token: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>)>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        if limit == 0 {
            // limit=0 means "no chunking, return everything".
            return Ok((self.list(prefix).await?, None));
        }

        // Default path: list everything, sort by a stable per-item key, slice.
        // Backends with native pagination (e.g. etcd `RangeRequest.limit`) may
        // override this for efficiency.
        let all: Vec<serde_json::Value> = self.list(prefix).await?;

        let mut indexed: Vec<(String, serde_json::Value)> =
            all.into_iter().map(|v| (default_sort_key(&v), v)).collect();
        indexed.sort_by(|a, b| a.0.cmp(&b.0));

        let start = if let Some(token) = continue_token {
            let decoded = decode_default_token(token)?;
            if let Some(rv) = decoded.compacted_at {
                if self.is_revision_compacted(rv).await.unwrap_or(false) {
                    return Err(Error::Gone(format!(
                        "continue token expired (resource version {} has been compacted)",
                        rv
                    )));
                }
            }
            indexed
                .iter()
                .position(|(k, _)| k.as_str() >= decoded.start_key.as_str())
                .unwrap_or(indexed.len())
        } else {
            0
        };

        let end = (start + limit).min(indexed.len());
        let next_token = if end < indexed.len() {
            let next_key = &indexed[end].0;
            let rv = self.current_revision().await.unwrap_or(0);
            Some(encode_default_token(next_key, rv))
        } else {
            None
        };

        let mut out = Vec::with_capacity(end - start);
        for (_, v) in indexed.drain(start..end) {
            out.push(serde_json::from_value(v).map_err(Error::Serialization)?);
        }
        Ok((out, next_token))
    }

    /// Watch for changes to resources with a given prefix
    async fn watch(&self, prefix: &str) -> Result<WatchStream>;

    /// Watch for changes starting from a specific revision
    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream>;

    /// Get the current storage revision (etcd mod_revision)
    async fn current_revision(&self) -> Result<i64>;

    /// Check if a revision has been compacted (no longer available)
    async fn is_revision_compacted(&self, revision: i64) -> Result<bool>;
}

/// Default sort key for an opaque JSON resource — uses
/// `metadata.namespace/metadata.name` so iteration order matches
/// `/registry/<type>/<ns>/<name>` storage layout.
fn default_sort_key(v: &serde_json::Value) -> String {
    let ns = v
        .pointer("/metadata/namespace")
        .and_then(|n| n.as_str())
        .unwrap_or("");
    let name = v
        .pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .unwrap_or("");
    if ns.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", ns, name)
    }
}

/// Decoded continue token (opaque to callers).
#[derive(Debug, Clone)]
pub struct ContinueToken {
    /// Sort key of the next item to return.
    pub start_key: String,
    /// Resource version at which the token was issued; used to detect
    /// compaction.
    pub compacted_at: Option<i64>,
}

/// Encode a continue token. Format: `c1:<rv>:<key>`. The version prefix lets
/// us evolve the format without breaking existing clients.
pub fn encode_default_token(start_key: &str, rv: i64) -> String {
    format!("c1:{}:{}", rv, start_key)
}

/// Decode a continue token produced by [`encode_default_token`].
pub fn decode_default_token(token: &str) -> Result<ContinueToken> {
    let rest = token.strip_prefix("c1:").ok_or_else(|| {
        Error::InvalidResource(format!(
            "malformed continue token (unknown version): {}",
            token
        ))
    })?;
    let (rv_str, start_key) = rest.split_once(':').ok_or_else(|| {
        Error::InvalidResource(format!("malformed continue token (missing key): {}", token))
    })?;
    let rv: i64 = rv_str.parse().map_err(|_| {
        Error::InvalidResource(format!(
            "malformed continue token (bad resource version): {}",
            token
        ))
    })?;
    Ok(ContinueToken {
        start_key: start_key.to_string(),
        compacted_at: Some(rv),
    })
}

/// Event types for watch operations
#[derive(Debug, Clone)]
pub enum WatchEvent {
    Added(String, String),    // key, value
    Modified(String, String), // key, value
    Deleted(String, String),  // key, previous value (for Kubernetes compliance)
}

/// Stream of watch events
pub type WatchStream = futures::stream::BoxStream<'static, Result<WatchEvent>>;

/// Blanket implementation so `Arc<S>` can be used wherever `S: Storage` is required.
#[async_trait]
impl<S: Storage> Storage for std::sync::Arc<S> {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        (**self).get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).update(key, value).await
    }

    // Must forward (not inherit the trait default), or the inner type's
    // `update_status` override is bypassed. Notably `ApiStorage` routes status
    // to the api-server's `/status` subresource — without this, an
    // `Arc<ApiStorage>` would fall back to get+update (a full PUT), which the
    // api-server strips of `.status`, silently dropping every status write.
    async fn update_status<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).update_status(key, value).await
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        (**self).update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        (**self).delete(key).await
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).list(prefix).await
    }

    async fn list_paginated<T>(
        &self,
        prefix: &str,
        limit: usize,
        continue_token: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>)>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).list_paginated(prefix, limit, continue_token).await
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        (**self).watch(prefix).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        (**self).watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> Result<i64> {
        (**self).current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        (**self).is_revision_compacted(revision).await
    }
}

/// Configuration for selecting a storage backend.
pub enum StorageConfig {
    /// Use an external etcd cluster.
    Etcd {
        /// Etcd endpoint URLs (e.g. `["http://localhost:2379"]`).
        endpoints: Vec<String>,
    },
    /// Use an embedded SQLite database via rhino (requires `sqlite` feature).
    #[cfg(feature = "sqlite")]
    Sqlite {
        /// Path to the SQLite database file.
        path: String,
    },
    /// Use Redis via rhino (requires `redis` feature).
    #[cfg(feature = "redis")]
    Redis {
        /// Redis connection URL (e.g. `"redis://localhost:6379"`).
        url: String,
    },
}

/// Unified storage backend that dispatches to etcd, SQLite, Redis, or in-memory at runtime.
///
/// This allows all components to remain generic over `S: Storage` while the
/// concrete backend is chosen once at startup via `StorageConfig`.
#[allow(clippy::large_enum_variant)]
pub enum StorageBackend {
    Etcd(etcd::EtcdStorage),
    #[cfg(feature = "sqlite")]
    Sqlite(RhinoStorage),
    #[cfg(feature = "redis")]
    Redis(RhinoRedisStorage),
    /// In-memory backend backed by `MemoryStorage`. Intended for unit/integration
    /// tests that need a full `ApiServerState` without an external store.
    Memory(Arc<MemoryStorage>),
    /// API-client backend: every `Storage` call is proxied to the api-server
    /// over REST via [`api_storage::ApiStorage`]. Lets a component run as an
    /// api-server client (in-cluster kubelet/controller-manager/kube-proxy)
    /// while keeping its `Arc<StorageBackend>` handle unchanged.
    #[cfg(feature = "api-client")]
    Api(api_storage::ApiStorage),
}

impl StorageBackend {
    /// Create a new storage backend from the given configuration.
    pub async fn new(config: StorageConfig) -> Result<Self> {
        match config {
            StorageConfig::Etcd { endpoints } => {
                let storage = etcd::EtcdStorage::new(endpoints).await?;
                Ok(StorageBackend::Etcd(storage))
            }
            #[cfg(feature = "sqlite")]
            StorageConfig::Sqlite { path } => {
                let storage = RhinoStorage::new(&path).await?;
                Ok(StorageBackend::Sqlite(storage))
            }
            #[cfg(feature = "redis")]
            StorageConfig::Redis { url } => {
                let storage = RhinoRedisStorage::new_redis(&url).await?;
                Ok(StorageBackend::Redis(storage))
            }
        }
    }

    /// Construct an in-memory backend suitable for unit/integration tests.
    /// Wraps `MemoryStorage` in an `Arc` so the same handle can be cloned by
    /// the caller (e.g. for `inject_conflicts(...)`) while the enum owns one
    /// copy.
    pub fn new_memory() -> Self {
        StorageBackend::Memory(Arc::new(MemoryStorage::new()))
    }

    /// Construct an API-client backend that proxies all `Storage` calls to the
    /// api-server over REST. The component keeps an ordinary
    /// `Arc<StorageBackend>` and never touches a real store directly.
    #[cfg(feature = "api-client")]
    pub fn new_api(client: Arc<rusternetes_client::http::ApiClient>) -> Self {
        StorageBackend::Api(api_storage::ApiStorage::new(client))
    }

    /// Gracefully evict a pod, in a mode-aware way (#1284). `mutated_pod` is the
    /// victim with `deletionTimestamp` + eviction status already applied.
    ///
    /// Storage-direct backends persist it whole (a single PUT) — the kubelet's
    /// own loop then observes `deletionTimestamp` and terminates it, exactly as
    /// before. The `Api` backend can't (the api-server owns `deletionTimestamp`
    /// on a PUT), so it stamps the status via `/status` then issues a real
    /// graceful `DELETE` with `grace_seconds`. Mirrors the scheduler's
    /// `DataPlane::evict_pod_for_preemption`.
    ///
    /// `grace_seconds` is only consumed by the `Api` backend's graceful
    /// `DELETE`; the storage-direct backends carry the grace period inside
    /// `mutated_pod`'s `deletionGracePeriodSeconds` and just persist it. When the
    /// `api-client` feature is off the `Api` arm is compiled out, so the
    /// parameter is legitimately unused — silence the lint only in that config.
    #[cfg_attr(not(feature = "api-client"), allow(unused_variables))]
    pub async fn evict_pod<T>(&self, key: &str, mutated_pod: &T, grace_seconds: i64) -> Result<()>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => s.evict_pod_graceful(key, mutated_pod, grace_seconds).await,
            _ => {
                Storage::update(self, key, mutated_pod).await?;
                Ok(())
            }
        }
    }

    /// Attach an in-process event bus to the backend so internal `watch()`
    /// consumers get the in-process fast path. Only valid in a single-writer
    /// (all-in-one) process. No-op for etcd/memory: memory already has an
    /// in-process bus, and etcd keeps its native watch.
    pub fn enable_event_bus(&mut self) {
        match self {
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => {
                s.set_event_bus(EventBus::new(event_bus::DEFAULT_CAPACITY))
            }
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.set_event_bus(EventBus::new(event_bus::DEFAULT_CAPACITY)),
            _ => {}
        }
    }

    /// Native backend watch, bypassing any attached event bus. The api-server
    /// watch cache uses this so external HTTP watch clients keep the
    /// globally-RV-ordered feed even when the bus is enabled for internal
    /// consumers.
    pub async fn watch_backend(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(s) => Storage::watch(s, prefix).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => s.watch_native(prefix).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.watch_native(prefix).await,
            StorageBackend::Memory(s) => Storage::watch(s.as_ref(), prefix).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::watch(s, prefix).await,
        }
    }
}

#[async_trait]
impl Storage for StorageBackend {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => Storage::create(s, key, value).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::create(s, key, value).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::create(s, key, value).await,
            StorageBackend::Memory(s) => Storage::create(s.as_ref(), key, value).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::create(s, key, value).await,
        }
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => Storage::get(s, key).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::get(s, key).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::get(s, key).await,
            StorageBackend::Memory(s) => Storage::get(s.as_ref(), key).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::get(s, key).await,
        }
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => Storage::update(s, key, value).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::update(s, key, value).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::update(s, key, value).await,
            StorageBackend::Memory(s) => Storage::update(s.as_ref(), key, value).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::update(s, key, value).await,
        }
    }

    /// Status writes must reach the inner backend's `update_status` — for the
    /// `Api` variant that routes to the `/status` subresource, which a plain
    /// `update` (full PUT) would otherwise strip. Other backends keep the
    /// default get+graft+update behavior.
    async fn update_status<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::update_status(s, key, value).await,
            // Non-API backends: replicate the trait default (graft only the
            // incoming status onto the freshly-read object, CAS-retry on
            // conflict) — they don't separate status, so update() persists it.
            _ => {
                let incoming = serde_json::to_value(value).map_err(Error::Serialization)?;
                let new_status = incoming.get("status").cloned();
                const MAX_ATTEMPTS: usize = 8;
                for attempt in 0..MAX_ATTEMPTS {
                    let mut current: serde_json::Value = Storage::get(self, key).await?;
                    if let Some(obj) = current.as_object_mut() {
                        match &new_status {
                            Some(status) => {
                                obj.insert("status".to_string(), status.clone());
                            }
                            None => {
                                obj.remove("status");
                            }
                        }
                    }
                    match Storage::update::<serde_json::Value>(self, key, &current).await {
                        Ok(updated) => {
                            return serde_json::from_value(updated).map_err(Error::Serialization)
                        }
                        Err(Error::Conflict(_)) if attempt + 1 < MAX_ATTEMPTS => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(Error::Conflict(format!(
                    "update_status: exhausted {} CAS retries for {}",
                    MAX_ATTEMPTS, key
                )))
            }
        }
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        match self {
            StorageBackend::Etcd(s) => Storage::update_raw(s, key, value).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::update_raw(s, key, value).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::update_raw(s, key, value).await,
            StorageBackend::Memory(s) => Storage::update_raw(s.as_ref(), key, value).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::update_raw(s, key, value).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self {
            StorageBackend::Etcd(s) => Storage::delete(s, key).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::delete(s, key).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::delete(s, key).await,
            StorageBackend::Memory(s) => Storage::delete(s.as_ref(), key).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::delete(s, key).await,
        }
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => Storage::list(s, prefix).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::list(s, prefix).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::list(s, prefix).await,
            StorageBackend::Memory(s) => Storage::list(s.as_ref(), prefix).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::list(s, prefix).await,
        }
    }

    async fn list_paginated<T>(
        &self,
        prefix: &str,
        limit: usize,
        continue_token: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>)>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => {
                Storage::list_paginated(s, prefix, limit, continue_token).await
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => {
                Storage::list_paginated(s, prefix, limit, continue_token).await
            }
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => {
                Storage::list_paginated(s, prefix, limit, continue_token).await
            }
            StorageBackend::Memory(s) => {
                Storage::list_paginated(s.as_ref(), prefix, limit, continue_token).await
            }
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => {
                Storage::list_paginated(s, prefix, limit, continue_token).await
            }
        }
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(s) => Storage::watch(s, prefix).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::watch(s, prefix).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::watch(s, prefix).await,
            StorageBackend::Memory(s) => Storage::watch(s.as_ref(), prefix).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::watch(s, prefix).await,
        }
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(s) => Storage::watch_from_revision(s, prefix, revision).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::watch_from_revision(s, prefix, revision).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::watch_from_revision(s, prefix, revision).await,
            StorageBackend::Memory(s) => {
                Storage::watch_from_revision(s.as_ref(), prefix, revision).await
            }
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::watch_from_revision(s, prefix, revision).await,
        }
    }

    async fn current_revision(&self) -> Result<i64> {
        match self {
            StorageBackend::Etcd(s) => Storage::current_revision(s).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::current_revision(s).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::current_revision(s).await,
            StorageBackend::Memory(s) => Storage::current_revision(s.as_ref()).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::current_revision(s).await,
        }
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        match self {
            StorageBackend::Etcd(s) => Storage::is_revision_compacted(s, revision).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::is_revision_compacted(s, revision).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::is_revision_compacted(s, revision).await,
            StorageBackend::Memory(s) => Storage::is_revision_compacted(s.as_ref(), revision).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::is_revision_compacted(s, revision).await,
        }
    }
}

// AuthzStorage for StorageBackend — delegates to the inner implementation.
#[async_trait]
impl rusternetes_common::authz::AuthzStorage for StorageBackend {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => AuthzStorage::get(s, key, namespace).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => AuthzStorage::get(s, key, namespace).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => AuthzStorage::get(s, key, namespace).await,
            StorageBackend::Memory(s) => AuthzStorage::get(s.as_ref(), key, namespace).await,
            // The API-client backend is used by node/controller components that
            // never run the authorizer (only the api-server does, with a real
            // backend), so AuthzStorage is unreachable here.
            #[cfg(feature = "api-client")]
            StorageBackend::Api(_) => Err(Error::Internal(
                "AuthzStorage is not supported on the API-client backend".into(),
            )),
        }
    }

    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => AuthzStorage::list(s, namespace).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => AuthzStorage::list(s, namespace).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => AuthzStorage::list(s, namespace).await,
            StorageBackend::Memory(s) => AuthzStorage::list(s.as_ref(), namespace).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(_) => Err(Error::Internal(
                "AuthzStorage is not supported on the API-client backend".into(),
            )),
        }
    }
}

/// Helper function to build resource keys
pub fn build_key(resource_type: &str, namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(ns) => format!("/registry/{}/{}/{}", resource_type, ns, name),
        None => format!("/registry/{}/{}", resource_type, name),
    }
}

/// Helper function to build prefix for listing
pub fn build_prefix(resource_type: &str, namespace: Option<&str>) -> String {
    match namespace {
        Some(ns) => format!("/registry/{}/{}/", resource_type, ns),
        None => format!("/registry/{}/", resource_type),
    }
}

#[cfg(test)]
mod evict_tests {
    use super::*;
    use serde_json::json;

    /// Storage-mode eviction must behaviour-preservingly persist the mutated pod
    /// (deletionTimestamp + Evicted status) via a whole-object write — i.e.
    /// identical to the prior `update` path. #1284.
    #[tokio::test]
    async fn evict_pod_storage_mode_persists_mutated_pod() {
        let backend = StorageBackend::new_memory();
        let key = "/registry/pods/default/victim";
        let pod = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": "victim", "namespace": "default" },
            "status": { "phase": "Running" }
        });
        let _: serde_json::Value = Storage::create(&backend, key, &pod).await.unwrap();

        let mut evicted = pod.clone();
        evicted["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        evicted["status"]["phase"] = json!("Failed");
        evicted["status"]["reason"] = json!("Evicted");

        backend.evict_pod(key, &evicted, 30).await.unwrap();

        let stored: serde_json::Value = Storage::get(&backend, key).await.unwrap();
        assert_eq!(stored["status"]["phase"], "Failed");
        assert_eq!(stored["status"]["reason"], "Evicted");
        assert!(
            stored["metadata"]["deletionTimestamp"].is_string(),
            "deletionTimestamp must persist in storage mode: {stored}"
        );
    }
}
