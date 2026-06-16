//! Rusternetes DNS server binary entrypoint.
//!
//! Same CLI surface as the other rusternetes service binaries: storage
//! backend selection, log level, bind addresses, cluster zone. Also
//! supports an api-server data source (`--api-server-url`, or implicit
//! in-cluster config when running as a pod) so the binary can run as a
//! kube-system Deployment without direct storage access.

use anyhow::Result;
use clap::Parser;
use rusternetes_client::config::{ClientConfig, SA_DIR};
use rusternetes_client::http::ApiClient;
use rusternetes_dns::{run, run_with_api, DnsConfig};
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "rusternetes-dns")]
#[command(about = "Rusternetes DNS server - authoritative cluster DNS")]
struct Args {
    /// Etcd endpoints (comma-separated). Defaults to
    /// `http://localhost:2379` when storage mode is selected.
    #[arg(long)]
    etcd_servers: Option<String>,

    /// Storage backend: "etcd" or "sqlite". Defaults to "etcd" when
    /// storage mode is selected.
    #[arg(long)]
    storage_backend: Option<String>,

    /// API server URL (e.g. https://api-server:6443). When set, dns reads
    /// cluster state via the API instead of storage. When neither this
    /// nor any storage flag is set, the standard in-cluster config
    /// (serviceaccount token + KUBERNETES_SERVICE_HOST) is tried first.
    #[arg(long)]
    api_server_url: Option<String>,

    /// Bearer token file for --api-server-url (defaults to the in-cluster
    /// serviceaccount token when running as a pod).
    #[arg(long)]
    token_file: Option<String>,

    /// SQLite database path (only used when --storage-backend=sqlite).
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Log level (trace|debug|info|warn|error).
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Cluster zone suffix. Defaults to `cluster.local`.
    #[arg(long, default_value = "cluster.local")]
    cluster_zone: String,

    /// UDP bind address (host:port).
    #[arg(long, default_value = "0.0.0.0:53")]
    udp_bind: String,

    /// TCP bind address (host:port).
    #[arg(long, default_value = "0.0.0.0:53")]
    tcp_bind: String,

    /// Full-resync interval in seconds (safety net for missed watches).
    #[arg(long, default_value = "30")]
    resync_interval: u64,
}

/// Resolve the API-mode client config, if API mode applies.
///
/// Explicit `--api-server-url` always wins (token from `--token-file`,
/// falling back to the serviceaccount projection; CA from the
/// serviceaccount projection). Otherwise, when no storage flag was
/// passed, try the standard in-cluster config so the binary works
/// arg-free as a pod.
fn resolve_api_config(args: &Args) -> Result<Option<ClientConfig>> {
    if let Some(url) = &args.api_server_url {
        let sa_dir = Path::new(SA_DIR);
        let token = match &args.token_file {
            Some(file) => Some(
                std::fs::read_to_string(file)
                    .map_err(|e| anyhow::anyhow!("reading --token-file {file}: {e}"))?
                    .trim()
                    .to_string(),
            ),
            None => std::fs::read_to_string(sa_dir.join("token"))
                .ok()
                .map(|t| t.trim().to_string()),
        };
        let ca_pem = std::fs::read_to_string(sa_dir.join("ca.crt")).ok();
        return Ok(Some(ClientConfig {
            base_url: url.clone(),
            token,
            ca_pem,
        }));
    }
    if args.etcd_servers.is_none() && args.storage_backend.is_none() {
        // No explicit source selected — prefer in-cluster config when the
        // pod environment provides one.
        return Ok(ClientConfig::in_cluster().ok());
    }
    Ok(None)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    rusternetes_common::tracing::init_basic_tracing("dns", &args.log_level)?;
    tracing::info!(
        "Starting Rusternetes DNS {}",
        rusternetes_common::build_info::version_line()
    );

    let udp_bind: SocketAddr = args.udp_bind.parse()?;
    let tcp_bind: SocketAddr = args.tcp_bind.parse()?;

    let config = DnsConfig {
        cluster_zone: args.cluster_zone.clone(),
        udp_bind,
        tcp_bind,
        resync_interval: args.resync_interval,
    };

    if let Some(client_config) = resolve_api_config(&args)? {
        info!(
            "Using api-server data source at: {}",
            client_config.base_url
        );
        let client = Arc::new(ApiClient::from_config(&client_config)?);
        return run_with_api(client, config).await;
    }

    let storage_backend = args.storage_backend.as_deref().unwrap_or("etcd");
    let storage_config = match storage_backend {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Using SQLite storage backend at: {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir.clone(),
            }
        }
        _ => {
            let endpoints: Vec<String> = args
                .etcd_servers
                .as_deref()
                .unwrap_or("http://localhost:2379")
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Connecting to etcd at: {:?}", endpoints);
            StorageConfig::Etcd { endpoints }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    run(storage, config).await
}
