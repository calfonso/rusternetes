mod advanced;
mod data_plane;
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

    /// API-server URL. When set, the scheduler runs in API mode: reads come
    /// from pods/nodes informers and writes go through the binding subresource,
    /// status PUT, and event POSTs, with no direct storage handle. Used by the
    /// in-cluster static pod. When unset, the scheduler uses the storage backend
    /// (all-in-one binary path).
    #[arg(long)]
    api_server_url: Option<String>,

    /// Path to a kubeconfig for API mode (client cert/CA + server URL). When
    /// given, its CA validates the api-server's TLS cert; an absent CA means the
    /// connection trusts the system roots (or, with --insecure-skip-tls-verify,
    /// skips verification).
    #[arg(long)]
    kubeconfig: Option<String>,

    /// Skip TLS verification for the api-server connection in API mode.
    #[arg(long)]
    insecure_skip_tls_verify: bool,

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

    // API mode (in-cluster static pod): no storage backend, no leader election
    // (single instance by design). Reads come from informers; writes go through
    // the binding subresource + status + events. Resolve the api-server URL and
    // CA from --kubeconfig (preferred) or the explicit flags.
    if args.api_server_url.is_some() || args.kubeconfig.is_some() {
        return run_api_mode(args).await;
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

/// Run the scheduler as an api-server client (in-cluster static pod). Resolves
/// the api-server URL + CA from `--kubeconfig` (preferred) or the explicit
/// `--api-server-url` flag, builds the [`ApiBackend`] (client + informers +
/// event recorder) and runs the single-instance scheduling loop.
async fn run_api_mode(args: Args) -> Result<()> {
    use rusternetes_client::http::ApiClient;
    use rusternetes_client::kubeconfig::KubeConfig;

    // Metrics server (same as storage mode).
    let metrics = Arc::new(MetricsRegistry::new().with_scheduler_metrics()?);
    let metrics_clone = metrics.clone();
    let metrics_addr = format!("0.0.0.0:{}", args.metrics_port);
    info!("Starting metrics server on {}", metrics_addr);
    tokio::spawn(async move {
        let app = Router::new().route("/metrics", get(|| async move { metrics_clone.gather() }));
        let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // Resolve connection params. A kubeconfig wins for both server URL and CA;
    // --api-server-url overrides the server URL when provided.
    let (base_url, ca_pem, kube_insecure) = if let Some(path) = args.kubeconfig.as_deref() {
        let cfg = KubeConfig::load_from_file(&std::path::PathBuf::from(path))?;
        let server = args
            .api_server_url
            .clone()
            .or_else(|| cfg.get_server().ok())
            .ok_or_else(|| anyhow::anyhow!("kubeconfig has no server URL"))?;
        let ca = cfg.get_ca_cert_pem().ok().flatten();
        let insecure = cfg.should_skip_tls_verify().unwrap_or(false);
        (server, ca, insecure)
    } else {
        let server = args
            .api_server_url
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--api-server-url is required without --kubeconfig"))?;
        (server, None, false)
    };

    let insecure = args.insecure_skip_tls_verify || kube_insecure;
    info!(
        "Scheduler API mode: api-server={}, ca={}, insecure={}",
        base_url,
        ca_pem.is_some(),
        insecure
    );

    // The api-server runs with --skip-auth in compose, so no client credentials
    // are needed; the CA only validates the server's TLS cert. (When auth is
    // later enabled, a bearer token / client-cert path plugs in here.)
    let client = Arc::new(ApiClient::with_tls(&base_url, insecure, ca_pem, None)?);

    let scheduler_name = "default-scheduler".to_string();
    let backend = data_plane::ApiBackend::new(client, &scheduler_name);
    let scheduler = Arc::new(Scheduler::new_api(backend, args.interval, scheduler_name));
    info!("Scheduler starting (API mode, single instance, no leader election)");
    scheduler.run().await?;
    Ok(())
}
