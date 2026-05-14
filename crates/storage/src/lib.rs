use async_trait::async_trait;
use rusternetes_common::authz::AuthzStorage;
use rusternetes_common::Result;
use serde::{de::DeserializeOwned, Serialize};

pub mod concurrency;
pub mod etcd;
pub mod memory;
#[cfg(any(feature = "sqlite", feature = "redis"))]
pub mod rhino;
pub mod workqueue;

// Re-export MemoryStorage for convenient testing
pub use memory::MemoryStorage;

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

    /// Watch for changes to resources with a given prefix
    async fn watch(&self, prefix: &str) -> Result<WatchStream>;

    /// Watch for changes starting from a specific revision
    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream>;

    /// Get the current storage revision (etcd mod_revision)
    async fn current_revision(&self) -> Result<i64>;

    /// Check if a revision has been compacted (no longer available)
    async fn is_revision_compacted(&self, revision: i64) -> Result<bool>;
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

/// Unified storage backend that dispatches to etcd, SQLite, or Redis at runtime.
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
    /// In-memory backend, intended for unit tests only.
    Memory(memory::MemoryStorage),
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

    /// Create an in-memory backend for testing.
    pub fn new_memory() -> Self {
        StorageBackend::Memory(memory::MemoryStorage::new())
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
            StorageBackend::Memory(s) => Storage::create(s, key, value).await,
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
            StorageBackend::Memory(s) => Storage::get(s, key).await,
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
            StorageBackend::Memory(s) => Storage::update(s, key, value).await,
        }
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        match self {
            StorageBackend::Etcd(s) => Storage::update_raw(s, key, value).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::update_raw(s, key, value).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::update_raw(s, key, value).await,
            StorageBackend::Memory(s) => Storage::update_raw(s, key, value).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self {
            StorageBackend::Etcd(s) => Storage::delete(s, key).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::delete(s, key).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::delete(s, key).await,
            StorageBackend::Memory(s) => Storage::delete(s, key).await,
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
            StorageBackend::Memory(s) => Storage::list(s, prefix).await,
        }
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(s) => Storage::watch(s, prefix).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::watch(s, prefix).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::watch(s, prefix).await,
            StorageBackend::Memory(s) => Storage::watch(s, prefix).await,
        }
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(s) => Storage::watch_from_revision(s, prefix, revision).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::watch_from_revision(s, prefix, revision).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::watch_from_revision(s, prefix, revision).await,
            StorageBackend::Memory(s) => Storage::watch_from_revision(s, prefix, revision).await,
        }
    }

    async fn current_revision(&self) -> Result<i64> {
        match self {
            StorageBackend::Etcd(s) => Storage::current_revision(s).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::current_revision(s).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::current_revision(s).await,
            StorageBackend::Memory(s) => Storage::current_revision(s).await,
        }
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        match self {
            StorageBackend::Etcd(s) => Storage::is_revision_compacted(s, revision).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::is_revision_compacted(s, revision).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::is_revision_compacted(s, revision).await,
            StorageBackend::Memory(s) => Storage::is_revision_compacted(s, revision).await,
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
            StorageBackend::Memory(s) => {
                // Build the key in the same style as EtcdStorage: /registry/<kind>/<namespace>/<name>
                let full_key = match namespace {
                    Some(ns) => format!("/registry/{}/{}", ns, key),
                    None => format!("/registry/{}", key),
                };
                Storage::get(s, &full_key).await
            }
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
            StorageBackend::Memory(s) => {
                let prefix = match namespace {
                    Some(ns) => format!("/registry/{}/", ns),
                    None => "/registry/".to_string(),
                };
                Storage::list(s, &prefix).await
            }
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
