#[allow(dead_code)]
mod cni;
mod config;
#[allow(dead_code)]
mod eviction;
mod kubelet;
mod lifecycle;
mod runtime;

use anyhow::Result;
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::Docker;
use clap::Parser;
use config::{KubeletConfiguration, RuntimeConfig};
use futures::StreamExt;
use kubelet::Kubelet;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn, Level};

#[derive(Parser, Debug)]
#[command(name = "rusternetes-kubelet")]
#[command(about = "Rusternetes Kubelet - Node agent that manages containers", long_about = None)]
#[command(version)]
struct Args {
    /// Node name
    #[arg(long)]
    node_name: String,

    /// Etcd endpoints (comma-separated)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Path to kubelet configuration file
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Root directory for managing kubelet files (volume data, plugin state, etc.)
    #[arg(long, value_name = "DIR")]
    root_dir: Option<String>,

    /// Directory path for managing volume data
    #[arg(long, value_name = "DIR")]
    volume_dir: Option<String>,

    /// Directory where volume plugins are installed
    #[arg(long, value_name = "DIR")]
    volume_plugin_dir: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long)]
    log_level: Option<String>,

    /// Sync interval in seconds
    #[arg(long)]
    sync_interval: Option<u64>,

    /// Metrics server port
    #[arg(long)]
    metrics_port: Option<u16>,

    /// Cluster DNS service IP address (dynamically discovered if not provided)
    #[arg(long)]
    cluster_dns: Option<String>,

    /// Cluster domain suffix
    #[arg(long, default_value = "cluster.local")]
    cluster_domain: String,

    /// Container network to connect pods to
    #[arg(long, default_value = "rusternetes-network")]
    network: String,

    /// Storage backend: "etcd" or "sqlite"
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Load configuration file if specified
    let config_file = if let Some(config_path) = &args.config {
        info!("Loading kubelet configuration from: {}", config_path);
        Some(KubeletConfiguration::from_file(config_path)?)
    } else {
        None
    };

    // Parse etcd endpoints
    let etcd_endpoints: Vec<String> = args
        .etcd_servers
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    // Build runtime configuration with proper precedence
    let runtime_config = RuntimeConfig::build(
        args.root_dir,
        args.volume_dir,
        args.volume_plugin_dir,
        args.sync_interval,
        args.metrics_port,
        args.log_level,
        config_file,
        args.node_name,
        etcd_endpoints,
    )?;

    // Initialize tracing
    let level = match runtime_config.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    tracing_subscriber::fmt().with_max_level(level).init();

    info!("Starting Rusternetes Kubelet");
    info!("{}", runtime_config.display());

    // Initialize storage
    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Using SQLite storage backend at: {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir.clone(),
            }
        }
        _ => {
            info!("Connecting to etcd: {:?}", runtime_config.etcd_endpoints);
            StorageConfig::Etcd {
                endpoints: runtime_config.etcd_endpoints.clone(),
            }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    // Discover cluster DNS IP if not provided
    let cluster_dns = match args.cluster_dns {
        Some(dns) => {
            info!("Using provided cluster DNS: {}", dns);
            dns
        }
        None => {
            info!("Discovering cluster DNS IP from kube-dns service...");
            use rusternetes_common::resources::Service;
            use rusternetes_storage::Storage;

            match storage
                .get::<Service>("/registry/services/kube-system/kube-dns")
                .await
            {
                Ok(service) => {
                    if let Some(ref cluster_ip) = service.spec.cluster_ip {
                        info!("Discovered cluster DNS IP: {}", cluster_ip);
                        cluster_ip.clone()
                    } else {
                        warn!("kube-dns service has no ClusterIP, falling back to 10.96.0.10");
                        "10.96.0.10".to_string()
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to discover cluster DNS IP: {}. Falling back to 10.96.0.10",
                        e
                    );
                    "10.96.0.10".to_string()
                }
            }
        }
    };

    // Initialize metrics
    let metrics = Arc::new(MetricsRegistry::new().with_kubelet_metrics()?);
    let metrics_clone = metrics.clone();

    // Convert RuntimeConfig to KubeletConfiguration for /configz endpoint
    let kubelet_config = KubeletConfiguration {
        api_version: "kubelet.config.k8s.io/v1beta1".to_string(),
        kind: "KubeletConfiguration".to_string(),
        root_dir: Some(runtime_config.root_dir.to_string_lossy().to_string()),
        volume_dir: Some(runtime_config.volume_dir.to_string_lossy().to_string()),
        volume_plugin_dir: Some(
            runtime_config
                .volume_plugin_dir
                .to_string_lossy()
                .to_string(),
        ),
        sync_frequency: Some(runtime_config.sync_frequency),
        metrics_bind_port: Some(runtime_config.metrics_bind_port),
        log_level: Some(runtime_config.log_level.clone()),
        cluster_service_cidr: None, // Not exposed in config endpoint
    };
    let kubelet_config = Arc::new(kubelet_config);
    let kubelet_config_clone = kubelet_config.clone();

    // Start metrics and config server
    let metrics_addr = format!("0.0.0.0:{}", runtime_config.metrics_bind_port);
    info!(
        "Starting kubelet API server on {} (metrics + configz)",
        metrics_addr
    );

    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(|| async move { metrics_clone.gather() }))
            .route(
                "/configz",
                get(|| async move { Json(kubelet_config_clone.as_ref().clone()) }),
            )
            .route("/exec/:container_id", post(handle_exec));

        let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // Create and run kubelet
    let kubelet = Arc::new(
        Kubelet::new(
            runtime_config.node_name.clone(),
            storage,
            runtime_config.sync_frequency,
            runtime_config.volume_dir.to_string_lossy().to_string(),
            cluster_dns,
            args.cluster_domain,
            args.network,
            runtime_config.kubernetes_service_host.clone(),
        )
        .await?,
    );
    kubelet.run().await?;

    Ok(())
}

/// Handle exec requests from the API server.
///
/// The API server proxies exec requests to the kubelet, which uses bollard
/// to create and start a Docker exec on the target container.
async fn handle_exec(
    Path(container_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, String)> {
    let command: Vec<String> = params
        .get("command")
        .map(|c| c.split(',').map(|s| s.to_string()).collect())
        .unwrap_or_default();
    let stdin_data = if body.is_empty() { None } else { Some(body) };
    let tty = params.get("tty").map(|v| v == "true").unwrap_or(false);

    let docker = Docker::connect_with_local_defaults()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let exec_config = CreateExecOptions {
        cmd: Some(command.iter().map(|s| s.as_str()).collect()),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        attach_stdin: Some(stdin_data.is_some()),
        tty: Some(tty),
        ..Default::default()
    };

    let exec = docker
        .create_exec(&container_id, exec_config)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Always use attached mode to collect output
    let start_config = Some(bollard::exec::StartExecOptions {
        detach: false,
        ..Default::default()
    });

    let output = docker
        .start_exec(&exec.id, start_config)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Collect output with short timeout per read to prevent hanging
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exec_id = exec.id.clone();
    if let StartExecResults::Attached {
        output: mut stream, ..
    } = output
    {
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(1), stream.next()).await {
                Ok(Some(Ok(msg))) => match msg {
                    LogOutput::StdOut { message } => stdout.extend_from_slice(&message),
                    LogOutput::StdErr { message } => stderr.extend_from_slice(&message),
                    _ => {}
                },
                Ok(Some(Err(_))) | Ok(None) => break, // stream ended or error
                Err(_) => {
                    // Timeout — check if exec is still running
                    match docker.inspect_exec(&exec_id).await {
                        Ok(info) => {
                            if !info.running.unwrap_or(false) {
                                break; // exec finished, stream just didn't close
                            }
                            // still running, continue waiting
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    }

    info!(
        "Exec completed: container={}, stdout_len={}, stderr_len={}",
        container_id,
        stdout.len(),
        stderr.len()
    );

    Ok(Json(serde_json::json!({
        "stdout": String::from_utf8_lossy(&stdout),
        "stderr": String::from_utf8_lossy(&stderr),
    })))
}
