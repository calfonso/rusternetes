mod advanced;
#[allow(dead_code)]
mod framework;
#[allow(dead_code)]
mod plugins;
mod scheduler;

use anyhow::Result;
use axum::{routing::get, Router};
use clap::Parser;
use rusternetes_common::leader_election::{LeaderElectionConfig, LeaderElector};
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{StorageBackend, StorageConfig};
use scheduler::Scheduler;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "rusternetes-scheduler")]
#[command(about = "Rusternetes Scheduler - Assigns pods to nodes")]
struct Args {
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

    /// Scheduling interval in seconds
    #[arg(long, default_value = "2")]
    interval: u64,

    /// Metrics server port
    #[arg(long, default_value = "8081")]
    metrics_port: u16,

    /// Enable leader election (for HA)
    #[arg(long)]
    enable_leader_election: bool,

    /// Leader election identity (unique for each instance)
    #[arg(long)]
    leader_election_identity: Option<String>,

    /// Leader election lock key
    #[arg(long, default_value = "/rusternetes/scheduler/leader")]
    leader_election_lock_key: String,

    /// Leader election lease duration in seconds
    #[arg(long, default_value = "15")]
    leader_election_lease_duration: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    rusternetes_common::tracing::init_basic_tracing("scheduler", &args.log_level)?;

    info!("Starting Rusternetes Scheduler");

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
            info!("Connecting to etcd: {:?}", endpoints);
            StorageConfig::Etcd { endpoints }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    // Initialize metrics
    let metrics = Arc::new(MetricsRegistry::new().with_scheduler_metrics()?);
    let metrics_clone = metrics.clone();

    let metrics_addr = format!("0.0.0.0:{}", args.metrics_port);
    info!("Starting metrics server on {}", metrics_addr);

    tokio::spawn(async move {
        let app = Router::new().route("/metrics", get(|| async move { metrics_clone.gather() }));
        let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // Leader election
    if args.enable_leader_election {
        let etcd_endpoints: Vec<String> = args
            .etcd_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let identity = args
            .leader_election_identity
            .unwrap_or_else(|| format!("scheduler-{}", Uuid::new_v4()));

        let config = LeaderElectionConfig {
            identity: identity.clone(),
            lock_key: args.leader_election_lock_key,
            lease_duration: args.leader_election_lease_duration,
            renew_interval: args.leader_election_lease_duration / 3,
            retry_interval: 2,
        };

        info!(identity = %identity, "Leader election enabled - starting in follower mode");

        let elector = Arc::new(LeaderElector::new(etcd_endpoints, config).await?);
        let elector_clone = elector.clone();
        tokio::spawn(async move {
            if let Err(e) = elector_clone.run().await {
                tracing::error!("Leader election error: {}", e);
            }
        });

        let scheduler = Arc::new(Scheduler::new(storage, args.interval));
        loop {
            while !elector.is_leader().await {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
            info!("Scheduler starting (leader acquired)");
            if let Err(e) = Arc::clone(&scheduler).run().await {
                tracing::error!("Scheduler error: {}", e);
            }
            if !elector.is_leader().await {
                warn!("Scheduler stopped (lost leadership)");
                continue;
            }
            break;
        }
    } else {
        warn!("Leader election disabled - running in single-instance mode");
        let scheduler = Arc::new(Scheduler::new(storage, args.interval));
        scheduler.run().await?;
    }

    Ok(())
}
