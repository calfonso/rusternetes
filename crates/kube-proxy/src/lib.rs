pub mod iptables;
pub mod proxy;

use futures::StreamExt;
use proxy::KubeProxy;
use rusternetes_storage::{Storage, StorageBackend, WorkQueue, RECONCILE_ALL_SENTINEL};
use std::sync::Arc;
use tracing::{info, warn};

/// Configuration for the kube-proxy component.
pub struct KubeProxyConfig {
    pub node_name: String,
    pub sync_interval: u64,
}

/// Run the kube-proxy component.
///
/// This is the main entry point for embedding kube-proxy in the all-in-one binary.
/// It runs the iptables sync loop until cancelled.
pub async fn run(storage: Arc<StorageBackend>, config: KubeProxyConfig) -> anyhow::Result<()> {
    info!(
        "Starting Rusternetes Kube-proxy for node: {}",
        config.node_name
    );

    // iptables invocation is blocking; offload to a worker thread so the runtime
    // is not stalled even briefly during startup.
    match tokio::task::spawn_blocking(check_iptables).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!("iptables check failed: {}. Some features may not work.", e);
            warn!("Kube-proxy requires iptables to be installed and accessible.");
        }
        Err(e) => {
            warn!(
                "iptables check task panicked: {}. Some features may not work.",
                e
            );
            warn!("Kube-proxy requires iptables to be installed and accessible.");
        }
    }

    let kube_proxy = Arc::new(tokio::sync::Mutex::new(KubeProxy::new(Arc::clone(
        &storage,
    ))?));

    info!("Kube-proxy initialized successfully");
    info!("Syncing services every {} seconds", config.sync_interval);

    let sync_interval = tokio::time::Duration::from_secs(config.sync_interval);

    let queue = WorkQueue::new();

    let worker_queue = queue.clone();
    let worker_proxy = Arc::clone(&kube_proxy);
    tokio::spawn(async move {
        info!("Kube-proxy worker started");
        while let Some(key) = worker_queue.get().await {
            info!("Kube-proxy worker processing key: {}", key);
            match worker_proxy.lock().await.sync().await {
                Ok(()) => worker_queue.forget(&key).await,
                Err(e) => {
                    tracing::error!("sync error: {}", e);
                    worker_queue.requeue_rate_limited(key.clone()).await;
                }
            }
            worker_queue.done(&key).await;
        }
    });

    // SIGTERM is what `docker stop` (and Kubernetes pod termination) sends.
    // Without an explicit handler, tokio just lets the process get killed
    // and the IptablesManager::Drop never runs — leaving the host's
    // iptables polluted with RUSTERNETES-* chains and jump rules.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");

    loop {
        queue.add(RECONCILE_ALL_SENTINEL.into()).await;

        // Watch services, endpoints, AND endpointslices for fast iptables updates.
        // K8s kube-proxy watches all three resource types to react immediately
        // when backends change.
        let svc_watch = storage.watch("/registry/services/").await;
        let ep_watch = storage.watch("/registry/endpoints/").await;
        let es_watch = storage.watch("/registry/endpointslices/").await;

        let mut svc_watch = match svc_watch {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("Failed to establish service watch: {}, retrying", e);
                tokio::time::sleep(sync_interval).await;
                continue;
            }
        };
        let mut ep_watch = match ep_watch {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("Failed to establish endpoints watch: {}, retrying", e);
                tokio::time::sleep(sync_interval).await;
                continue;
            }
        };
        let mut es_watch = match es_watch {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("Failed to establish endpointslice watch: {}, retrying", e);
                tokio::time::sleep(sync_interval).await;
                continue;
            }
        };

        // Periodic resync as a safety net — 10s keeps iptables fresh even if
        // watch events are missed.
        let mut resync = tokio::time::interval(std::time::Duration::from_secs(10));
        resync.tick().await; // consume the immediate first tick

        let mut watch_broken = false;
        while !watch_broken {
            tokio::select! {
                event = svc_watch.next() => {
                    match event {
                        Some(Ok(_)) => {
                            queue.add(RECONCILE_ALL_SENTINEL.into()).await;
                        }
                        Some(Err(e)) => {
                            tracing::warn!("Service watch error: {}, reconnecting", e);
                            watch_broken = true;
                        }
                        None => {
                            tracing::warn!("Service watch stream ended, reconnecting");
                            watch_broken = true;
                        }
                    }
                }
                event = ep_watch.next() => {
                    match event {
                        Some(Ok(_)) => {
                            queue.add(RECONCILE_ALL_SENTINEL.into()).await;
                        }
                        Some(Err(e)) => {
                            tracing::warn!("Endpoints watch error: {}, reconnecting", e);
                            watch_broken = true;
                        }
                        None => {
                            tracing::warn!("Endpoints watch stream ended, reconnecting");
                            watch_broken = true;
                        }
                    }
                }
                event = es_watch.next() => {
                    match event {
                        Some(Ok(_)) => {
                            queue.add(RECONCILE_ALL_SENTINEL.into()).await;
                        }
                        Some(Err(e)) => {
                            tracing::warn!("EndpointSlice watch error: {}, reconnecting", e);
                            watch_broken = true;
                        }
                        None => {
                            tracing::warn!("EndpointSlice watch stream ended, reconnecting");
                            watch_broken = true;
                        }
                    }
                }
                _ = resync.tick() => {
                    queue.add(RECONCILE_ALL_SENTINEL.into()).await;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Received SIGINT — shutting down");
                    return Ok(());
                }
                _ = sigterm.recv() => {
                    info!("Received SIGTERM — shutting down");
                    return Ok(());
                }
            }
        }
    }
}

fn check_iptables() -> anyhow::Result<()> {
    let output = std::process::Command::new("/usr/sbin/iptables-legacy")
        .arg("--version")
        .output()?;

    if output.status.success() {
        let version = String::from_utf8_lossy(&output.stdout);
        info!("iptables version: {}", version.trim());
        Ok(())
    } else {
        Err(anyhow::anyhow!("iptables-legacy not available"))
    }
}
