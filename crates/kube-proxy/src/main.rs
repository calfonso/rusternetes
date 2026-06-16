use anyhow::Result;
use clap::Parser;
use rusternetes_kube_proxy::{
    iptables::{DEFAULT_CLUSTER_CIDR, DEFAULT_NODEPORT_RANGE},
    KubeProxyConfig,
};
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::sync::Arc;
use tracing::info;

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

    /// API server URL for API mode (e.g. https://localhost:6443). kube-proxy
    /// runs host-network, so this is the host-published api-server port, not the
    /// `api-server` compose DNS name.
    #[arg(long, default_value = "https://localhost:6443")]
    api_server_url: String,

    /// Path to a kubeconfig for API mode (CA + server). When set, kube-proxy
    /// reads Services/Endpoints/EndpointSlices from the api-server via
    /// ApiStorage instead of storage (in-cluster compose service). When unset,
    /// the storage backend is used (all-in-one / legacy path).
    #[arg(long)]
    kubeconfig: Option<String>,

    /// Skip TLS verification for the api-server connection in API mode (the
    /// kubeconfig CA normally validates the self-signed cert).
    #[arg(long)]
    insecure_skip_tls_verify: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    rusternetes_common::tracing::init_basic_tracing("kube-proxy", &args.log_level)?;
    info!(
        "Starting Rusternetes kube-proxy {}",
        rusternetes_common::build_info::version_line()
    );

    let config = KubeProxyConfig {
        node_name: args.node_name.clone(),
        sync_interval: args.sync_interval,
        cluster_cidr: args.cluster_cidr.clone(),
        // Accept hyphen form (`30000-32767`) as a convenience — k8s and Go
        // flags use the hyphen, iptables wants the colon. Normalize here.
        nodeport_range: args.node_port_range.replace('-', ":"),
    };

    // In-cluster path: read cluster state from the api-server (no storage handle).
    if let Some(kubeconfig) = args.kubeconfig.as_deref() {
        use rusternetes_client::http::ApiClient;
        use rusternetes_client::kubeconfig::KubeConfig;

        let cfg = KubeConfig::load_from_file(&std::path::PathBuf::from(kubeconfig))?;
        let ca_pem = cfg.get_ca_cert_pem().ok().flatten();
        let insecure =
            args.insecure_skip_tls_verify || cfg.should_skip_tls_verify().unwrap_or(false);
        info!(
            "Kube-proxy API mode: api-server={}, ca={}, insecure={}",
            args.api_server_url,
            ca_pem.is_some(),
            insecure
        );
        // --skip-auth on the api-server means the CA only validates TLS; no
        // client credentials needed (authn tracked in #1129).
        let client = Arc::new(ApiClient::with_tls(
            &args.api_server_url,
            insecure,
            ca_pem,
            None,
        )?);
        return rusternetes_kube_proxy::run_with_api(client, config).await;
    }

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

    rusternetes_kube_proxy::run(storage, config).await
}
