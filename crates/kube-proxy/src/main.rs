use anyhow::Result;
use clap::Parser;
use rusternetes_kube_proxy::{
    iptables::{DEFAULT_CLUSTER_CIDR, DEFAULT_NODEPORT_RANGE},
    KubeProxyConfig,
};
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::sync::Arc;
use tracing::{info, Level};

#[derive(Parser, Debug)]
#[command(name = "rusternetes-kube-proxy")]
#[command(about = "Rusternetes Kube-proxy - Network proxy for service load balancing")]
struct Args {
    /// Node name
    #[arg(long)]
    node_name: String,

    /// Etcd endpoints (comma-separated)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Storage backend: "etcd" or "sqlite"
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Sync interval in seconds
    #[arg(long, default_value = "1")]
    sync_interval: u64,

    /// ClusterIP CIDR — must match the apiserver's
    /// `--service-cluster-ip-range`. Used to scope the POSTROUTING
    /// MASQUERADE rule so it doesn't fire on non-cluster traffic.
    #[arg(long, default_value = DEFAULT_CLUSTER_CIDR)]
    cluster_cidr: String,

    /// NodePort range in iptables `start:end` form — must match the
    /// apiserver's `--service-node-port-range`. Hyphen also accepted
    /// (`30000-32767`) and normalized.
    #[arg(long, default_value = DEFAULT_NODEPORT_RANGE)]
    node_port_range: String,
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

    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Using SQLite storage backend at: {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir,
            }
        }
        _ => {
            let endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Connecting to etcd at: {:?}", endpoints);
            StorageConfig::Etcd { endpoints }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    let config = KubeProxyConfig {
        node_name: args.node_name,
        sync_interval: args.sync_interval,
        cluster_cidr: args.cluster_cidr,
        // Accept hyphen form (`30000-32767`) as a convenience — k8s and Go
        // flags use the hyphen, iptables wants the colon. Normalize here.
        nodeport_range: args.node_port_range.replace('-', ":"),
    };

    rusternetes_kube_proxy::run(storage, config).await
}
