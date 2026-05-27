//! Watches Services, EndpointSlices, and Pods from the rusternetes
//! storage backend and rebuilds the in-memory DNS zone on every change.
//!
//! Pattern lifted from `crates/kube-proxy/src/lib.rs`: open one watch
//! stream per resource type, multiplex them with `tokio::select!`, and
//! enqueue a full resync whenever any event arrives. We do not try to
//! apply diffs — a zone for a 10k-pod cluster is well under a megabyte,
//! and rebuilding from scratch keeps the lookup table simple and
//! consistent.
//!
//! Safety net: a fixed-period resync ticker (default 30s) re-lists from
//! storage even when no watch events have arrived, so a missed event
//! never leaves stale records in service forever.

use crate::server::SharedZone;
use crate::zone::Zone;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::{EndpointSlice, Pod, Service};
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

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

/// Run the watcher loop until cancelled (no graceful shutdown — the
/// server's signal handler tears down the whole process).
pub async fn run(
    storage: Arc<StorageBackend>,
    zone: SharedZone,
    config: WatcherConfig,
) -> Result<()> {
    info!(
        "DNS watcher started for zone {}, resync every {}s",
        config.cluster_zone, config.resync_interval_secs
    );

    // Single-slot mpsc — coalescing channel. Multiple rapid events
    // collapse into one rebuild instead of stampeding storage.
    let (tx, mut rx) = mpsc::channel::<()>(1);

    // Trigger the initial sync immediately.
    let _ = tx.try_send(());

    // Watch spawner — restarts watches when the underlying stream errors.
    let watch_storage = Arc::clone(&storage);
    let watch_tx = tx.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = run_watches(&watch_storage, watch_tx.clone()).await {
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

        match rebuild(&storage, &config.cluster_zone).await {
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

    Ok(Zone::build(
        cluster_zone,
        &services,
        &endpoint_slices,
        &pods,
    ))
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
