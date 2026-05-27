//! Rusternetes DNS server binary entrypoint.
//!
//! Same CLI surface as the other rusternetes service binaries: storage
//! backend selection, log level, bind addresses, cluster zone.

use anyhow::Result;
use clap::Parser;
use rusternetes_dns::{run, DnsConfig};
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "rusternetes-dns")]
#[command(about = "Rusternetes DNS server - authoritative cluster DNS")]
struct Args {
    /// Etcd endpoints (comma-separated).
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Storage backend: "etcd" or "sqlite".
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    rusternetes_common::tracing::init_basic_tracing("dns", &args.log_level)?;

    let storage_config = match args.storage_backend.as_str() {
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
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Connecting to etcd at: {:?}", endpoints);
            StorageConfig::Etcd { endpoints }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    let udp_bind: SocketAddr = args.udp_bind.parse()?;
    let tcp_bind: SocketAddr = args.tcp_bind.parse()?;

    let config = DnsConfig {
        cluster_zone: args.cluster_zone,
        udp_bind,
        tcp_bind,
        resync_interval: args.resync_interval,
    };

    run(storage, config).await
}
