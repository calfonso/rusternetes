//! List+watch reflector with resourceVersion resume and an in-memory store.
//!
//! Upstream: `k8s.io/client-go/tools/cache` (Reflector + ThreadSafeStore),
//! simplified: no DeltaFIFO — a keyed store plus a broadcast event channel.
//!
//! The reflector is generic over a [`ListWatch`] trait so unit tests inject a
//! scripted mock; the production impl ([`ApiListWatch`]) wraps
//! [`ApiClient`] + [`watch_stream`].

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;

use crate::http::{ApiClient, KubernetesList};
use crate::watch::{watch_stream, WatchEvent};

/// List+watch source. `list` returns `(items, listResourceVersion)`; `watch`
/// runs one watch session from `rv` and returns the events it produced before
/// the stream ended, plus the final resourceVersion observed (None if the
/// session saw no versioned event). The production impl streams; the trait
/// batches so the reflector loop and tests share one shape.
#[async_trait::async_trait]
pub trait ListWatch<T>: Send + Sync {
    async fn list(&self) -> Result<(Vec<T>, String)>;
    async fn watch(&self, rv: Option<String>) -> Result<(Vec<WatchEvent<T>>, Option<String>)>;
}

/// Store mutation emitted to subscribers (bookmarks are not emitted).
#[derive(Clone, Debug)]
pub enum StoreEvent<T> {
    Added(T),
    Modified(T),
    Deleted(T),
}

/// Read view over the reflector's keyed store. `get` clones.
pub struct Store<T> {
    inner: Arc<RwLock<HashMap<String, T>>>,
}

impl<T: Clone> Store<T> {
    pub fn get(&self, key: &str) -> Option<T> {
        self.inner.read().unwrap().get(key).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().unwrap().is_empty()
    }

    /// Snapshot of all items.
    pub fn items(&self) -> Vec<T> {
        self.inner.read().unwrap().values().cloned().collect()
    }
}

pub struct Reflector<T, K: Fn(&T) -> String> {
    lw: Arc<dyn ListWatch<T>>,
    key_fn: K,
    store: Arc<RwLock<HashMap<String, T>>>,
    tx: broadcast::Sender<StoreEvent<T>>,
    last_rv: RwLock<Option<String>>,
}

impl<T, K> Reflector<T, K>
where
    T: Clone + Send + Sync + 'static,
    K: Fn(&T) -> String,
{
    pub fn new(lw: Arc<dyn ListWatch<T>>, key_fn: K) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            lw,
            key_fn,
            store: Arc::new(RwLock::new(HashMap::new())),
            tx,
            last_rv: RwLock::new(None),
        }
    }

    /// Read view over the current store contents.
    pub fn store(&self) -> Store<T> {
        Store {
            inner: Arc::clone(&self.store),
        }
    }

    /// Subscribe to store mutations. Initial-list population does not emit;
    /// consumers needing the initial state read `store()` after the first
    /// sync.
    pub fn subscribe(&self) -> broadcast::Receiver<StoreEvent<T>> {
        self.tx.subscribe()
    }

    /// One list (only when no resourceVersion is held yet) + one watch
    /// session, applying every event to the store.
    pub async fn sync_once(&self) -> Result<()> {
        if self.last_rv.read().unwrap().is_none() {
            let (items, rv) = self.lw.list().await.context("reflector list")?;
            let mut store = self.store.write().unwrap();
            store.clear();
            for item in items {
                store.insert((self.key_fn)(&item), item);
            }
            drop(store);
            *self.last_rv.write().unwrap() = Some(rv);
        }

        let rv = self.last_rv.read().unwrap().clone();
        let (events, final_rv) = self.lw.watch(rv).await.context("reflector watch")?;
        for event in events {
            match event {
                WatchEvent::Added(obj) => {
                    self.store
                        .write()
                        .unwrap()
                        .insert((self.key_fn)(&obj), obj.clone());
                    let _ = self.tx.send(StoreEvent::Added(obj));
                }
                WatchEvent::Modified(obj) => {
                    self.store
                        .write()
                        .unwrap()
                        .insert((self.key_fn)(&obj), obj.clone());
                    let _ = self.tx.send(StoreEvent::Modified(obj));
                }
                WatchEvent::Deleted(obj) => {
                    self.store.write().unwrap().remove(&(self.key_fn)(&obj));
                    let _ = self.tx.send(StoreEvent::Deleted(obj));
                }
                // Bookmark: rv progress only — no store change, no emit.
                WatchEvent::Bookmark(_) => {}
            }
        }
        if final_rv.is_some() {
            *self.last_rv.write().unwrap() = final_rv;
        }
        Ok(())
    }

    /// Run forever: list+watch with exponential backoff (1s → 30s, reset on
    /// success). A 410-Gone-style failure (compacted resourceVersion — the
    /// api-server's in-stream ERROR envelope says `reason:"Expired"`,
    /// `message:"too old resource version: X (Y)"`) clears the held rv so the
    /// next cycle re-lists; other errors keep the rv and just retry.
    pub async fn run(&self) {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.sync_once().await {
                Ok(()) => {
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    let text = format!("{e:#}");
                    if text.contains("Expired") || text.contains("too old resource version") {
                        tracing::warn!("reflector: resourceVersion expired, re-listing: {text}");
                        *self.last_rv.write().unwrap() = None;
                    } else {
                        tracing::warn!("reflector: sync failed (will retry): {text}");
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}

/// Production [`ListWatch`] over a live api-server: `list` GETs `path` as a
/// [`KubernetesList`]; `watch` runs [`watch_stream`] until the connection
/// ends, tracking the last `metadata.resourceVersion` observed.
pub struct ApiListWatch {
    client: Arc<ApiClient>,
    /// Resource collection path, e.g. `/api/v1/pods`.
    path: String,
}

impl ApiListWatch {
    pub fn new(client: Arc<ApiClient>, path: impl Into<String>) -> Self {
        Self {
            client,
            path: path.into(),
        }
    }
}

#[async_trait::async_trait]
impl<T: DeserializeOwned + Send + Sync + 'static> ListWatch<T> for ApiListWatch {
    async fn list(&self) -> Result<(Vec<T>, String)> {
        let list: KubernetesList<T> = self
            .client
            .get(&self.path)
            .await
            .map_err(|e| anyhow::anyhow!("list {} failed: {e}", self.path))?;
        let rv = list
            .metadata
            .and_then(|m| m.resource_version)
            .unwrap_or_else(|| "0".to_string());
        Ok((list.items, rv))
    }

    async fn watch(&self, rv: Option<String>) -> Result<(Vec<WatchEvent<T>>, Option<String>)> {
        // Stream as raw JSON values so each event's metadata.resourceVersion
        // can be tracked, then decode into T.
        let mut stream = Box::pin(
            watch_stream::<serde_json::Value>(&self.client, &self.path, rv.as_deref()).await?,
        );
        let mut events = Vec::new();
        let mut final_rv: Option<String> = None;
        while let Some(item) = stream.next().await {
            let event = item?;
            let (value, is_bookmark) = match &event {
                WatchEvent::Added(v) | WatchEvent::Modified(v) | WatchEvent::Deleted(v) => {
                    (v.clone(), false)
                }
                WatchEvent::Bookmark(v) => (v.clone(), true),
            };
            if let Some(obj_rv) = value
                .get("metadata")
                .and_then(|m| m.get("resourceVersion"))
                .and_then(|v| v.as_str())
            {
                final_rv = Some(obj_rv.to_string());
            }
            if is_bookmark {
                continue;
            }
            let typed: T = serde_json::from_value(value).context("decoding watch object")?;
            events.push(match event {
                WatchEvent::Added(_) => WatchEvent::Added(typed),
                WatchEvent::Modified(_) => WatchEvent::Modified(typed),
                WatchEvent::Deleted(_) => WatchEvent::Deleted(typed),
                WatchEvent::Bookmark(_) => unreachable!("bookmarks skipped above"),
            });
        }
        Ok((events, final_rv))
    }
}
