//! Watches Services, EndpointSlices, and Pods and rebuilds the in-memory
//! DNS zone on every change.
//!
//! Two data sources share one debounce/rebuild loop ([`DnsSource`]):
//!
//! - **Storage** — direct reads from the rusternetes storage backend
//!   (etcd/sqlite/redis). Used by the all-in-one binary and the legacy
//!   compose container.
//! - **Api** — list + watch against the api-server REST endpoints via
//!   `rusternetes-client`. Used by the in-cluster Deployment.
//!
//! Pattern lifted from `crates/kube-proxy/src/lib.rs`: open one watch
//! stream per resource type, multiplex them with `tokio::select!`, and
//! enqueue a full resync whenever any event arrives. We do not try to
//! apply diffs — a zone for a 10k-pod cluster is well under a megabyte,
//! and rebuilding from scratch keeps the lookup table simple and
//! consistent.
//!
//! Safety net: a fixed-period resync ticker (default 30s) re-lists from
//! the source even when no watch events have arrived, so a missed event
//! never leaves stale records in service forever.

use crate::server::SharedZone;
use crate::zone::Zone;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_client::http::ApiClient;
use rusternetes_client::watch::watch_stream;
use rusternetes_common::resources::{EndpointSlice, Pod, Service};
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// REST collection endpoints the API data source lists and watches.
pub mod api_paths {
    pub const SERVICES: &str = "/api/v1/services";
    pub const ENDPOINTSLICES: &str = "/apis/discovery.k8s.io/v1/endpointslices";
    pub const PODS: &str = "/api/v1/pods";
}

/// Everything the zone is built from, decoupled from where it was read.
///
/// `rebuild_zone(data)` is pure, so the storage path and the API path
/// provably produce identical zones for identical inputs.
pub struct DnsData {
    pub services: Vec<Service>,
    pub endpoint_slices: Vec<EndpointSlice>,
    pub pods: Vec<Pod>,
}

/// Pure zone construction — shared by the storage and API data paths.
pub fn rebuild_zone(data: &DnsData, cluster_zone: &str) -> Result<Zone> {
    Ok(Zone::build(
        cluster_zone,
        &data.services,
        &data.endpoint_slices,
        &data.pods,
    ))
}

/// Where the watcher reads cluster state from.
#[derive(Clone)]
pub enum DnsSource {
    /// Direct storage backend reads (etcd/sqlite/redis).
    Storage(Arc<StorageBackend>),
    /// API-server client (list + watch over REST).
    Api(Arc<ApiClient>),
}

impl DnsSource {
    async fn rebuild(&self, cluster_zone: &str) -> Result<Zone> {
        match self {
            DnsSource::Storage(storage) => rebuild(storage, cluster_zone).await,
            DnsSource::Api(client) => rebuild_from_api(client, cluster_zone).await,
        }
    }

    async fn run_watches(&self, tx: mpsc::Sender<()>) -> Result<()> {
        match self {
            DnsSource::Storage(storage) => run_watches(storage, tx).await,
            DnsSource::Api(client) => run_watches_api(client, tx).await,
        }
    }
}

pub struct WatcherConfig {
    pub cluster_zone: String,
    pub resync_interval_secs: u64,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            cluster_zone: crate::zone::CLUSTER_ZONE.to_string(),
            resync_interval_secs: 30,
        }
    }
}

/// Run the watcher loop against the storage backend until cancelled
/// (no graceful shutdown — the server's signal handler tears down the
/// whole process).
pub async fn run(
    storage: Arc<StorageBackend>,
    zone: SharedZone,
    config: WatcherConfig,
) -> Result<()> {
    run_with_source(DnsSource::Storage(storage), zone, config).await
}

/// Run the watcher loop against the api-server until cancelled.
pub async fn run_api(
    client: Arc<ApiClient>,
    zone: SharedZone,
    config: WatcherConfig,
) -> Result<()> {
    run_with_source(DnsSource::Api(client), zone, config).await
}

/// Shared debounce/rebuild loop for both data sources.
pub async fn run_with_source(
    source: DnsSource,
    zone: SharedZone,
    config: WatcherConfig,
) -> Result<()> {
    info!(
        "DNS watcher started for zone {}, resync every {}s",
        config.cluster_zone, config.resync_interval_secs
    );

    // Single-slot mpsc — coalescing channel. Multiple rapid events
    // collapse into one rebuild instead of stampeding the source.
    let (tx, mut rx) = mpsc::channel::<()>(1);

    // Trigger the initial sync immediately.
    let _ = tx.try_send(());

    // Watch spawner — restarts watches when the underlying stream errors.
    let watch_source = source.clone();
    let watch_tx = tx.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = watch_source.run_watches(watch_tx.clone()).await {
                warn!("DNS watch loop errored, retrying in 5s: {e:?}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    });

    // Periodic resync ticker. `interval` fires immediately; consume it
    // so the first tick after startup is one full period out.
    let mut ticker = tokio::time::interval(Duration::from_secs(config.resync_interval_secs));
    ticker.tick().await;
    let tick_tx = tx.clone();
    tokio::spawn(async move {
        loop {
            ticker.tick().await;
            let _ = tick_tx.try_send(());
        }
    });

    // Rebuild loop. We don't care WHY a rebuild was requested — we just
    // do a full re-list and swap.
    loop {
        if rx.recv().await.is_none() {
            warn!("DNS rebuild channel closed");
            return Ok(());
        }
        // Drain any extras that piled up during the rebuild.
        while rx.try_recv().is_ok() {}

        match source.rebuild(&config.cluster_zone).await {
            Ok(new_zone) => {
                zone.store(new_zone).await;
                debug!("DNS zone rebuilt");
            }
            Err(e) => {
                error!("Failed to rebuild DNS zone: {e:?}");
            }
        }
    }
}

async fn rebuild(storage: &Arc<StorageBackend>, cluster_zone: &str) -> Result<Zone> {
    // Use empty Vec on list failure rather than aborting the rebuild —
    // a transient storage hiccup shouldn't blow the whole zone away.
    let services: Vec<Service> = storage
        .list("/registry/services/")
        .await
        .unwrap_or_else(|e| {
            warn!("services list failed: {e:?}");
            Vec::new()
        });
    let endpoint_slices: Vec<EndpointSlice> = storage
        .list("/registry/endpointslices/")
        .await
        .unwrap_or_else(|e| {
            warn!("endpointslices list failed: {e:?}");
            Vec::new()
        });
    let pods: Vec<Pod> = storage.list("/registry/pods/").await.unwrap_or_else(|e| {
        warn!("pods list failed: {e:?}");
        Vec::new()
    });

    rebuild_zone(
        &DnsData {
            services,
            endpoint_slices,
            pods,
        },
        cluster_zone,
    )
}

/// Full re-list over the api-server REST endpoints. Unlike the storage
/// path, a list error aborts the rebuild (the loop keeps serving the
/// previous zone snapshot and retries on the next trigger).
pub async fn rebuild_from_api(client: &ApiClient, cluster_zone: &str) -> Result<Zone> {
    let services: Vec<Service> = client
        .get_list(api_paths::SERVICES)
        .await
        .map_err(|e| anyhow::anyhow!("services list failed: {e}"))?;
    let endpoint_slices: Vec<EndpointSlice> = client
        .get_list(api_paths::ENDPOINTSLICES)
        .await
        .map_err(|e| anyhow::anyhow!("endpointslices list failed: {e}"))?;
    let pods: Vec<Pod> = client
        .get_list(api_paths::PODS)
        .await
        .map_err(|e| anyhow::anyhow!("pods list failed: {e}"))?;
    rebuild_zone(
        &DnsData {
            services,
            endpoint_slices,
            pods,
        },
        cluster_zone,
    )
}

/// Open three watch streams (services / endpointslices / pods) and
/// notify the rebuild channel on every event. Returns when any stream
/// errors so the caller can restart all three together (cheap — watches
/// are HTTP keep-alive on storage; re-establishing them costs ~ms).
async fn run_watches(storage: &Arc<StorageBackend>, tx: mpsc::Sender<()>) -> Result<()> {
    let mut svc_watch = storage.watch("/registry/services/").await?;
    let mut es_watch = storage.watch("/registry/endpointslices/").await?;
    let mut pod_watch = storage.watch("/registry/pods/").await?;

    info!("DNS watches established (services, endpointslices, pods)");

    loop {
        tokio::select! {
            event = svc_watch.next() => {
                match event {
                    Some(Ok(_)) => { let _ = tx.try_send(()); }
                    Some(Err(e)) => {
                        warn!("Service watch error: {e:?}");
                        return Ok(());
                    }
                    None => {
                        warn!("Service watch stream ended");
                        return Ok(());
                    }
                }
            }
            event = es_watch.next() => {
                match event {
                    Some(Ok(_)) => { let _ = tx.try_send(()); }
                    Some(Err(e)) => {
                        warn!("EndpointSlice watch error: {e:?}");
                        return Ok(());
                    }
                    None => {
                        warn!("EndpointSlice watch stream ended");
                        return Ok(());
                    }
                }
            }
            event = pod_watch.next() => {
                match event {
                    Some(Ok(_)) => { let _ = tx.try_send(()); }
                    Some(Err(e)) => {
                        warn!("Pod watch error: {e:?}");
                        return Ok(());
                    }
                    None => {
                        warn!("Pod watch stream ended");
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// API-mode trigger loop: three `watch_stream`s, any event → debounce
/// signal. Mirrors `run_watches` exactly: error/stream-end returns `Ok`
/// so the caller's reconnect loop restarts all three together. Events
/// are decoded as `serde_json::Value` and discarded — they are only
/// rebuild triggers, same as the storage path.
async fn run_watches_api(client: &ApiClient, tx: mpsc::Sender<()>) -> Result<()> {
    let mut svc_watch =
        Box::pin(watch_stream::<serde_json::Value>(client, api_paths::SERVICES, None).await?);
    let mut es_watch =
        Box::pin(watch_stream::<serde_json::Value>(client, api_paths::ENDPOINTSLICES, None).await?);
    let mut pod_watch =
        Box::pin(watch_stream::<serde_json::Value>(client, api_paths::PODS, None).await?);

    info!("DNS API watches established (services, endpointslices, pods)");

    loop {
        tokio::select! {
            event = svc_watch.next() => {
                match event {
                    Some(Ok(_)) => { let _ = tx.try_send(()); }
                    Some(Err(e)) => {
                        warn!("Service API watch error: {e:?}");
                        return Ok(());
                    }
                    None => {
                        warn!("Service API watch stream ended");
                        return Ok(());
                    }
                }
            }
            event = es_watch.next() => {
                match event {
                    Some(Ok(_)) => { let _ = tx.try_send(()); }
                    Some(Err(e)) => {
                        warn!("EndpointSlice API watch error: {e:?}");
                        return Ok(());
                    }
                    None => {
                        warn!("EndpointSlice API watch stream ended");
                        return Ok(());
                    }
                }
            }
            event = pod_watch.next() => {
                match event {
                    Some(Ok(_)) => { let _ = tx.try_send(()); }
                    Some(Err(e)) => {
                        warn!("Pod API watch error: {e:?}");
                        return Ok(());
                    }
                    None => {
                        warn!("Pod API watch stream ended");
                        return Ok(());
                    }
                }
            }
        }
    }
}
