//! Rusternetes — all-in-one Kubernetes in a single binary.
//!
//! Runs the API server, scheduler, controller manager, kubelet, and kube-proxy
//! as concurrent tokio tasks sharing a single storage backend.
//!
//! Usage:
//!   rusternetes                                   # SQLite at ./data/rusternetes.db
//!   rusternetes --data-dir /var/lib/rusternetes.db # custom path
//!   rusternetes --etcd-servers http://etcd:2379    # use etcd instead

use anyhow::Result;
use clap::Parser;
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::sync::Arc;
use tracing::{error, info, Level};

#[derive(Parser, Debug)]
#[command(name = "rusternetes")]
#[command(about = "Rusternetes — all-in-one Kubernetes in a single binary")]
#[command(version)]
struct Args {
    /// Storage backend: "sqlite", "etcd", or "redis"
    #[arg(long, default_value = "sqlite")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Etcd endpoints, comma-separated (only used when --storage-backend=etcd)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Redis URL (only used when --storage-backend=redis)
    #[arg(long, default_value = "redis://localhost:6379")]
    redis_url: String,

    /// API server bind address
    #[arg(long, default_value = "0.0.0.0:6443")]
    bind_address: String,

    /// Node name for the embedded kubelet
    #[arg(long, default_value = "node-1")]
    node_name: String,

    /// Volume directory for pod volumes
    #[arg(long, default_value = "./data/volumes")]
    volume_dir: String,

    /// Cluster DNS IP
    #[arg(long, default_value = "10.96.0.10")]
    cluster_dns: String,

    /// Container network name
    #[arg(long, default_value = "rusternetes-network")]
    network: String,

    /// Enable TLS with self-signed certificates
    #[arg(long)]
    tls: bool,

    /// TLS certificate file
    #[arg(long)]
    tls_cert_file: Option<String>,

    /// TLS private key file
    #[arg(long)]
    tls_key_file: Option<String>,

    /// TLS Subject Alternative Names (comma-separated)
    #[arg(long, default_value = "localhost,127.0.0.1")]
    tls_san: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Controller sync interval in seconds
    #[arg(long, default_value = "5")]
    sync_interval: u64,

    /// Scheduler interval in seconds
    #[arg(long, default_value = "2")]
    scheduler_interval: u64,

    /// Kubelet sync interval in seconds
    #[arg(long, default_value = "3")]
    kubelet_sync_interval: u64,

    /// Kube-proxy sync interval in seconds
    #[arg(long, default_value = "1")]
    proxy_sync_interval: u64,

    /// ClusterIP CIDR — must match the apiserver's
    /// `--service-cluster-ip-range`. Used by kube-proxy to scope its
    /// POSTROUTING MASQUERADE rule.
    #[arg(long, default_value = rusternetes_kube_proxy::iptables::DEFAULT_CLUSTER_CIDR)]
    cluster_cidr: String,

    /// NodePort range in iptables `start:end` form — must match the
    /// apiserver's `--service-node-port-range`. Hyphen also accepted.
    #[arg(long, default_value = rusternetes_kube_proxy::iptables::DEFAULT_NODEPORT_RANGE)]
    node_port_range: String,

    /// Skip authentication (insecure, for development)
    #[arg(long, default_value = "true")]
    skip_auth: bool,

    /// Disable kube-proxy (useful when iptables is not available)
    #[arg(long)]
    disable_proxy: bool,

    /// Path to the console SPA build directory (enables web console at /console/)
    #[arg(long)]
    console_dir: Option<String>,

    /// Kubernetes service host override for pods (e.g. "api-server" in containerized deployments).
    /// Falls back to KUBERNETES_SERVICE_HOST_OVERRIDE env var if not set.
    #[arg(long)]
    kubernetes_service_host: Option<String>,

    /// Client CA certificate file for mTLS client certificate authentication
    #[arg(long)]
    client_ca_file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    tracing_subscriber::fmt().with_max_level(level).init();

    info!("Starting Rusternetes (all-in-one)");

    // Initialize storage — all components share one instance
    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Storage: SQLite at {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir,
            }
        }
        "etcd" => {
            let endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Storage: etcd at {:?}", endpoints);
            StorageConfig::Etcd { endpoints }
        }
        #[cfg(feature = "redis")]
        "redis" => {
            info!("Storage: Redis at {}", args.redis_url);
            StorageConfig::Redis {
                url: args.redis_url,
            }
        }
        other => {
            anyhow::bail!(
                "Unknown storage backend: {}. Use 'sqlite', 'etcd', or 'redis'.",
                other
            );
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    info!("Storage initialized, starting components...");

    // --- API Server ---
    let api_storage = storage.clone();
    let api_config = rusternetes_api_server::ApiServerConfig {
        bind_address: args.bind_address.clone(),
        tls: args.tls,
        tls_cert_file: args.tls_cert_file.clone(),
        tls_key_file: args.tls_key_file.clone(),
        tls_self_signed: args.tls,
        tls_san: args.tls_san.clone(),
        skip_auth: args.skip_auth,
        console_dir: args.console_dir.map(std::path::PathBuf::from),
        client_ca_file: args.client_ca_file,
        ..Default::default()
    };
    let api_handle = tokio::spawn(async move {
        if let Err(e) = rusternetes_api_server::run(api_storage, api_config).await {
            error!("API server error: {}", e);
        }
    });

    // Give API server a moment to bind before starting clients
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // --- Scheduler ---
    let sched_storage = storage.clone();
    let sched_config = rusternetes_scheduler::SchedulerConfig {
        interval: args.scheduler_interval,
    };
    tokio::spawn(async move {
        if let Err(e) = rusternetes_scheduler::run(sched_storage, sched_config).await {
            error!("Scheduler error: {}", e);
        }
    });

    // --- Controller Manager ---
    let cm_storage = storage.clone();
    let cm_config = rusternetes_controller_manager::ControllerManagerConfig {
        sync_interval: args.sync_interval,
    };
    tokio::spawn(async move {
        if let Err(e) = rusternetes_controller_manager::run(cm_storage, cm_config).await {
            error!("Controller manager error: {}", e);
        }
    });

    // --- Kubelet ---
    let kubelet_storage = storage.clone();
    let kubelet_config = rusternetes_kubelet::KubeletConfig {
        node_name: args.node_name.clone(),
        volume_dir: args.volume_dir,
        cluster_dns: args.cluster_dns,
        cluster_domain: "cluster.local".to_string(),
        network: args.network,
        sync_interval: args.kubelet_sync_interval,
        metrics_port: 10250,
        kubernetes_service_host: args
            .kubernetes_service_host
            .clone()
            .or_else(|| std::env::var("KUBERNETES_SERVICE_HOST_OVERRIDE").ok())
            .unwrap_or_else(|| "127.0.0.1".to_string()),
    };
    tokio::spawn(async move {
        if let Err(e) = rusternetes_kubelet::run(kubelet_storage, kubelet_config).await {
            error!("Kubelet error: {}", e);
        }
    });

    // --- Kube-proxy ---
    if !args.disable_proxy {
        let proxy_storage = storage.clone();
        let proxy_config = rusternetes_kube_proxy::KubeProxyConfig {
            node_name: args.node_name,
            sync_interval: args.proxy_sync_interval,
            cluster_cidr: args.cluster_cidr,
            // Accept hyphen form as a convenience; iptables wants colon.
            nodeport_range: args.node_port_range.replace('-', ":"),
        };
        tokio::spawn(async move {
            if let Err(e) = rusternetes_kube_proxy::run(proxy_storage, proxy_config).await {
                error!("Kube-proxy error: {}", e);
            }
        });
    } else {
        info!("Kube-proxy disabled");
    }

    info!("All components started");

    // The API server task blocks on its listener — wait for it or ctrl-c
    tokio::select! {
        result = api_handle => {
            if let Err(e) = result {
                error!("API server task panicked: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, stopping rusternetes");
        }
    }

    Ok(())
}
