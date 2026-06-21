#[allow(dead_code)]
pub mod cni;
pub mod config;
pub mod cri_runtime;
pub mod downward_api;
pub mod events;
#[allow(dead_code)]
pub mod eviction;
pub mod kubelet;
pub mod labels;
pub mod lifecycle;
pub mod runtime;
pub mod server;
pub mod static_pods;
pub mod sync_locks;
pub mod sysctl;
pub mod volumes;

pub use kubelet::PodWorkerState;

use config::KubeletConfiguration;
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;
use tracing::info;

// `PodNetworkMode` is defined in `runtime.rs` (where every consumer
// lives) and re-exported here for the public API. Lives in
// `runtime` so it's accessible from BOTH the lib crate (via
// `pub mod runtime`) AND the standalone bin crate (which declares
// its own `mod runtime`) — `crate::runtime::PodNetworkMode`
// resolves correctly in both compile contexts.
pub use runtime::PodNetworkMode;

/// Configuration for the kubelet component.
pub struct KubeletConfig {
    pub node_name: String,
    pub volume_dir: String,
    pub cluster_dns: String,
    pub cluster_domain: String,
    pub network: String,
    pub sync_interval: u64,
    pub metrics_port: u16,
    pub kubernetes_service_host: String,
    /// Optional embedded-netstack handle. Set by the all-in-one
    /// binary when launched with any `--pod-network-mode=netstack*`
    /// variant. Leave `None` to keep the existing CNI/Docker-bridge
    /// behaviour.
    pub netstack: Option<Arc<dyn rusternetes_netstack::manager::NetstackHandle>>,
    /// Which networking implementation kubelet uses per pod. Only
    /// honored when `netstack` is also `Some(_)`; otherwise the
    /// kubelet treats it as [`PodNetworkMode::Cni`].
    pub pod_network_mode: PodNetworkMode,
}

impl Default for KubeletConfig {
    fn default() -> Self {
        Self {
            node_name: "node-1".to_string(),
            volume_dir: "./volumes".to_string(),
            cluster_dns: "10.96.0.10".to_string(),
            cluster_domain: "cluster.local".to_string(),
            network: "rusternetes-network".to_string(),
            sync_interval: 3,
            metrics_port: 10250,
            kubernetes_service_host: "10.96.0.1".to_string(),
            netstack: None,
            pod_network_mode: PodNetworkMode::Cni,
        }
    }
}

/// Run the kubelet component.
///
/// This is the main entry point for embedding the kubelet in the all-in-one binary.
/// Starts the kubelet sync loop and metrics server, blocks until shutdown.
pub async fn run(storage: Arc<StorageBackend>, config: KubeletConfig) -> anyhow::Result<()> {
    info!(
        "Starting Rusternetes Kubelet for node: {}",
        config.node_name
    );

    // Discover cluster DNS if not hardcoded
    let cluster_dns = {
        use rusternetes_common::resources::Service;
        match storage
            .get::<Service>("/registry/services/kube-system/kube-dns")
            .await
        {
            Ok(service) => {
                if let Some(ref cluster_ip) = service.spec.cluster_ip {
                    info!("Discovered cluster DNS IP: {}", cluster_ip);
                    cluster_ip.clone()
                } else {
                    config.cluster_dns.clone()
                }
            }
            Err(_) => config.cluster_dns.clone(),
        }
    };

    // Metrics server
    let metrics =
        Arc::new(rusternetes_common::observability::MetricsRegistry::new().with_kubelet_metrics()?);
    let metrics_clone = metrics.clone();

    let kubelet_config = KubeletConfiguration {
        api_version: "kubelet.config.k8s.io/v1beta1".to_string(),
        kind: "KubeletConfiguration".to_string(),
        root_dir: None,
        volume_dir: Some(config.volume_dir.clone()),
        volume_plugin_dir: None,
        sync_frequency: Some(config.sync_interval),
        metrics_bind_port: Some(config.metrics_port),
        log_level: Some("info".to_string()),
        cluster_service_cidr: None,
    };
    let kubelet_config = Arc::new(kubelet_config);
    let kubelet_config_clone = kubelet_config.clone();

    let metrics_addr = format!("0.0.0.0:{}", config.metrics_port);
    info!(
        "Starting kubelet API server on {} (metrics + configz)",
        metrics_addr
    );

    // For the all-in-one binary, derive the statvfs root from the volume dir's
    // parent (or default to /var/lib/kubelet). Eviction config uses upstream
    // defaults; CLI overrides are only available via the standalone `kubelet`
    // binary today.
    let eviction_root = std::path::PathBuf::from(&config.volume_dir)
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("/var/lib/kubelet"));

    let k = Arc::new(
        kubelet::Kubelet::new_with_eviction(
            config.node_name.clone(),
            storage.clone(),
            config.sync_interval,
            config.volume_dir,
            cluster_dns,
            config.cluster_domain,
            config.network,
            config.kubernetes_service_host,
            eviction_root,
            eviction::EvictionManager::new(),
            config.netstack,
            config.pod_network_mode,
            config.metrics_port,
            // The all-in-one binary doesn't expose --allowed-unsafe-sysctls;
            // default to none (unsafe sysctls rejected with SysctlForbidden).
            Vec::new(),
        )
        .await?,
    );

    let server_state = server::ServerState {
        node_name: config.node_name.clone(),
        storage: storage.clone(),
        kubelet: Some(k.clone()),
    };
    tokio::spawn(async move {
        use axum::{routing::get, Json, Router};
        let app = Router::new()
            .route("/metrics", get(|| async move { metrics_clone.gather() }))
            .route(
                "/configz",
                get(|| async move { Json(kubelet_config_clone.as_ref().clone()) }),
            )
            .merge(server::read_only_router(server_state));
        let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    k.run().await?;

    Ok(())
}
