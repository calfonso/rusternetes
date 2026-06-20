mod admission;
pub use rusternetes_admission_webhook as admission_webhook;
mod bootstrap;
pub use rusternetes_admission_webhook::cel_evaluators as cel;
mod conversion;
#[allow(dead_code)]
mod cri_exec;
mod dynamic_routes;
#[allow(dead_code)]
mod flow_control;
mod gnostic;
mod handlers;
mod ip_allocator;
use rusternetes_middleware as middleware;
mod openapi;
mod patch;
mod peer_cert_acceptor;
mod prometheus_client;
pub use rusternetes_protobuf as protobuf;
#[allow(dead_code)]
mod response;
mod router;
#[allow(dead_code)]
mod spdy;
// The bin only drives handle_spdy3_exec; the rest of the codec API is exercised
// by the lib + tests, so allow dead_code in the binary build.
#[allow(dead_code)]
mod spdy3;
#[allow(dead_code)]
mod spdy3_handlers;
#[allow(dead_code)]
mod spdy_handlers;
mod ssa;
mod state;
#[allow(dead_code)]
mod streaming;
mod watch_cache;

use anyhow::Result;
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use prometheus_client::PrometheusClient;
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::RBACAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::tls::TlsConfig;
use rusternetes_storage::{Storage, StorageBackend, StorageConfig};
use state::ApiServerState;
use std::sync::Arc;
use tracing::debug;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "rusternetes-api-server")]
#[command(about = "Rusternetes API Server - Kubernetes API reimplemented in Rust")]
struct Args {
    /// Address to bind to
    #[arg(long, default_value = "0.0.0.0:6443")]
    bind_address: String,

    /// Etcd endpoints (comma-separated)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// JWT secret for service account tokens
    #[arg(long, default_value = "rusternetes-secret-change-in-production")]
    jwt_secret: String,

    /// Enable TLS/HTTPS
    #[arg(long)]
    tls: bool,

    /// TLS certificate file (PEM format)
    #[arg(long)]
    tls_cert_file: Option<String>,

    /// TLS private key file (PEM format)
    #[arg(long)]
    tls_key_file: Option<String>,

    /// Generate self-signed certificate if TLS files not provided
    #[arg(long)]
    tls_self_signed: bool,

    /// Subject Alternative Names for self-signed cert (comma-separated)
    #[arg(long, default_value = "localhost,127.0.0.1")]
    tls_san: String,

    /// Skip authentication and authorization (INSECURE - development only)
    #[arg(long)]
    skip_auth: bool,

    /// Storage backend: "etcd" or "sqlite"
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Prometheus server URL for custom metrics (optional)
    #[arg(long)]
    prometheus_url: Option<String>,

    /// Path to the console SPA build directory (enables web console at /console/)
    #[arg(long)]
    console_dir: Option<String>,

    /// Client CA certificate file for mTLS client certificate authentication
    #[arg(long)]
    client_ca_file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    rusternetes_common::tracing::init_basic_tracing("api-server", &args.log_level)?;
    rusternetes_common::dump::install_panic_hook("api-server");

    info!(
        "Starting Rusternetes API Server {}",
        rusternetes_common::build_info::version_line()
    );

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
            let etcd_endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Connecting to etcd: {:?}", etcd_endpoints);
            StorageConfig::Etcd {
                endpoints: etcd_endpoints,
            }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    // Initialize TokenManager — prefer RSA keys for RS256 (K8s OIDC compatible),
    // fall back to HMAC HS256 if no RSA keys found.
    info!("Initializing TokenManager");
    let token_manager = Arc::new(TokenManager::new_auto(args.jwt_secret.as_bytes()));

    // Initialize Authorizer (RBAC or AlwaysAllow based on skip_auth)
    let authorizer: Arc<dyn rusternetes_common::authz::Authorizer> = if args.skip_auth {
        warn!("⚠️  AUTHENTICATION AND AUTHORIZATION DISABLED - INSECURE MODE");
        warn!("⚠️  Using AlwaysAllowAuthorizer - all requests will be permitted");
        warn!("⚠️  This should ONLY be used in development/testing environments");
        Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer)
    } else {
        info!("Initializing RBAC Authorizer");
        Arc::new(RBACAuthorizer::new(storage.clone()))
    };

    // Initialize Metrics Registry
    info!("Initializing Metrics Registry");
    let metrics = Arc::new(MetricsRegistry::new().with_api_server_metrics()?);

    // Load or generate TLS config (if TLS is enabled)
    let ca_cert_pem = if args.tls {
        info!("TLS enabled - loading/generating certificates");

        let tls_config = if let (Some(cert_file), Some(key_file)) =
            (args.tls_cert_file.clone(), args.tls_key_file.clone())
        {
            info!(
                "Loading TLS certificate from {} and key from {}",
                cert_file, key_file
            );
            TlsConfig::from_pem_files(&cert_file, &key_file)?
        } else if args.tls_self_signed {
            warn!("Generating self-signed certificate - NOT suitable for production!");
            let sans: Vec<String> = args
                .tls_san
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Self-signed cert SANs: {:?}", sans);
            TlsConfig::generate_self_signed("rusternetes-api", sans)?
        } else {
            anyhow::bail!("TLS enabled but no certificate provided. Use --tls-cert-file and --tls-key-file, or --tls-self-signed");
        };

        // ca.crt distributed to SA tokens / kube-root-ca must be the issuing CA,
        // not the (now leaf) serving cert — see resolve_ca_cert_pem.
        tls_config.cert_pem.as_deref().map(|serving| {
            rusternetes_api_server::resolve_ca_cert_pem(args.tls_cert_file.as_deref(), serving)
        })
    } else {
        None
    };

    // Bootstrap kubernetes Service Endpoints with dynamic IP discovery
    let api_port = args
        .bind_address
        .split(':')
        .next_back()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6443);

    if let Err(e) = bootstrap::bootstrap_kubernetes_service(storage.clone(), api_port).await {
        warn!(
            "Failed to bootstrap kubernetes Service Endpoints: {}. Continuing anyway.",
            e
        );
    }
    // Keep the kubernetes endpoint tracking the live api-server IP across
    // container recreates / IP changes (upstream EndpointReconciler, #1188).
    bootstrap::spawn_endpoint_reconciler(storage.clone(), api_port);

    // Create default ServiceCIDR "kubernetes" (required by conformance tests)
    {
        let cidr_key = rusternetes_storage::build_key("servicecidrs", None, "kubernetes");
        if storage.get::<serde_json::Value>(&cidr_key).await.is_err() {
            let service_cidr = serde_json::json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "ServiceCIDR",
                "metadata": {
                    "name": "kubernetes",
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": chrono::Utc::now().to_rfc3339()
                },
                "spec": {
                    "cidrs": ["10.96.0.0/12"]
                },
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "lastTransitionTime": chrono::Utc::now().to_rfc3339(),
                        "reason": "NetworkReady",
                        "message": "ServiceCIDR is ready"
                    }]
                }
            });
            if let Err(e) = storage.create(&cidr_key, &service_cidr).await {
                warn!("Failed to create default ServiceCIDR: {}", e);
            } else {
                info!("Created default ServiceCIDR 'kubernetes' with CIDR 10.96.0.0/12");
            }
        }
    }

    // Create default StorageClass (like k3s/kind ship with a default)
    {
        let sc_key = rusternetes_storage::build_key("storageclasses", None, "standard");
        if storage.get::<serde_json::Value>(&sc_key).await.is_err() {
            let storage_class = serde_json::json!({
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {
                    "name": "standard",
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": chrono::Utc::now().to_rfc3339(),
                    "annotations": {
                        "storageclass.kubernetes.io/is-default-class": "true"
                    }
                },
                "provisioner": "rusternetes.io/hostpath",
                "reclaimPolicy": "Delete",
                "volumeBindingMode": "WaitForFirstConsumer"
            });
            if let Err(e) = storage.create(&sc_key, &storage_class).await {
                warn!("Failed to create default StorageClass: {}", e);
            } else {
                info!("Created default StorageClass 'standard' with rusternetes.io/hostpath provisioner");
            }
        }
    }

    // Initialize Prometheus client for custom metrics (if URL provided)
    let prometheus_client = if let Some(url) = args.prometheus_url {
        info!("Initializing Prometheus client: {}", url);
        match PrometheusClient::new(url.clone()) {
            Ok(client) => {
                info!("Prometheus client initialized successfully");
                Some(Arc::new(client))
            }
            Err(e) => {
                warn!("Failed to initialize Prometheus client: {}. Custom metrics will return mock data.", e);
                None
            }
        }
    } else {
        info!("Prometheus URL not provided, custom metrics will return mock data");
        None
    };

    // Create shared state with CA certificate and Prometheus client
    let state = Arc::new(
        ApiServerState::new(storage, token_manager, authorizer, metrics, args.skip_auth)
            .with_ca_cert(ca_cert_pem)
            .with_prometheus_client(prometheus_client),
    );

    // Pre-allocate ClusterIPs from existing services to prevent collisions after restart
    {
        let existing_services: Vec<rusternetes_common::resources::Service> =
            Storage::list(state.storage.as_ref(), "/registry/services/")
                .await
                .unwrap_or_default();
        for svc in &existing_services {
            if let Some(ref ip) = svc.spec.cluster_ip {
                if ip != "None" && !ip.is_empty() {
                    state.ip_allocator.mark_allocated(ip.clone());
                    debug!(
                        "Pre-allocated ClusterIP {} for existing service {}",
                        ip, svc.metadata.name
                    );
                }
            }
        }
        info!(
            "Pre-allocated {} ClusterIPs from existing services",
            existing_services.len()
        );
    }

    // Build router
    let console_path = args.console_dir.as_ref().map(std::path::PathBuf::from);
    let app = router::build_router(state, console_path.as_deref());

    // Start server (with or without TLS)
    if args.tls {
        info!("TLS enabled - starting HTTPS server");

        // Reload TLS config for server
        let tls_config =
            if let (Some(cert_file), Some(key_file)) = (args.tls_cert_file, args.tls_key_file) {
                TlsConfig::from_pem_files(&cert_file, &key_file)?
            } else {
                // Must be self-signed
                let sans: Vec<String> = args
                    .tls_san
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                TlsConfig::generate_self_signed("rusternetes-api", sans)?
            };

        // `--client-ca-file` enables x509 client-cert authentication: build an
        // mTLS server config (client cert OPTIONAL — bearer-token clients still
        // connect) and serve via PeerCertAcceptor so the verified cert reaches
        // handlers for CN→user / O→groups mapping (#1129). Without it, plain
        // serving-only TLS as before.
        let client_cert_authn = args.client_ca_file.is_some();
        let server_config = if let Some(ref client_ca) = args.client_ca_file {
            info!(
                "Client certificate authentication enabled (CA: {})",
                client_ca
            );
            tls_config.into_mtls_server_config(client_ca)?
        } else {
            tls_config.into_server_config()?
        };
        let rustls_config = RustlsConfig::from_config(server_config);

        info!("HTTPS server listening on {}", args.bind_address);
        let addr = args.bind_address.parse()?;

        // Configure HTTP/2 settings to match K8s API server.
        // K8s sets these in secure_serving.go:175-199:
        //   MaxConcurrentStreams = 100
        //   MaxUploadBufferPerStream = 256KB
        //   IdleTimeout = 90s
        //
        // Hyper defaults (64KB window, 200 streams) cause watch stream
        // stalls with many concurrent watches — the flow control windows
        // fill up and events can't be delivered, causing client-go's
        // "Watch failed: context canceled" errors.
        //
        // `apply_http2_tuning!` keeps the two acceptor branches (mTLS vs
        // serving-only) in sync — the Server type differs by acceptor, so the
        // tuning can't be hoisted into a plain fn without naming both types.
        macro_rules! apply_http2_tuning {
            ($server:expr) => {{
                let builder = $server.http_builder();
                // Set timer first — required for HTTP/2 keepalive to function
                builder
                    .http2()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .initial_stream_window_size(256 * 1024) // 256KB per stream (K8s: 256KB)
                    .initial_connection_window_size(256 * 1024 * 100) // 25MB total (K8s: 256KB * 100)
                    .max_concurrent_streams(1000) // High limit — watch timeout (2min) handles stream recycling
                    // HTTP/2 PING keepalive: send PING frames to keep connections alive.
                    // Without this, network intermediaries (Podman Machine virtio-net,
                    // Docker Desktop proxy) may close idle TCP connections, killing
                    // watch streams with "context canceled".
                    // K8s Go server uses net.KeepAlive = 3 minutes on the TCP listener.
                    .keep_alive_interval(std::time::Duration::from_secs(30))
                    .keep_alive_timeout(std::time::Duration::from_secs(20));
            }};
        }

        if client_cert_authn {
            let mut server = axum_server::bind(addr)
                .acceptor(peer_cert_acceptor::PeerCertAcceptor::new(rustls_config));
            apply_http2_tuning!(server);
            server.serve(app.into_make_service()).await?;
        } else {
            let mut server = axum_server::bind_rustls(addr, rustls_config);
            apply_http2_tuning!(server);
            server.serve(app.into_make_service()).await?;
        }
    } else {
        info!("TLS disabled - starting HTTP server (not recommended for production)");
        info!("API Server listening on {}", args.bind_address);
        let listener = tokio::net::TcpListener::bind(&args.bind_address).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}
