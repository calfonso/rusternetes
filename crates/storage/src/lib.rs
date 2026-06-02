use async_trait::async_trait;
use rusternetes_common::authz::AuthzStorage;
use rusternetes_common::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

pub mod concurrency;
pub mod etcd;
pub mod event_recorder;
pub mod memory;
pub mod metadata;
#[cfg(any(feature = "sqlite", feature = "redis"))]
pub mod rhino;
pub mod workqueue;

// Re-export MemoryStorage for convenient testing
pub use memory::MemoryStorage;

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
