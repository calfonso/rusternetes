pub mod admission;
pub use rusternetes_admission_webhook as admission_webhook;
pub mod bootstrap;
pub use rusternetes_admission_webhook::cel_evaluators as cel;
pub use rusternetes_middleware::cbor;
pub mod conversion;
#[allow(dead_code)]
pub mod cri_exec;
pub mod dynamic_routes;
#[allow(dead_code)]
pub mod flow_control;
pub mod gnostic;
pub mod handlers;
pub mod ip_allocator;
pub use rusternetes_middleware as middleware;
pub mod openapi;
pub mod patch;
pub mod peer_cert_acceptor;
pub mod prometheus_client;
pub use rusternetes_protobuf as protobuf;
#[allow(dead_code)]
pub mod response;
pub mod router;
#[allow(dead_code)]
pub mod spdy;
pub mod spdy3;
#[allow(dead_code)]
pub mod spdy_handlers;
pub mod ssa;
pub mod state;
#[allow(dead_code)]
pub mod streaming;
pub mod watch_cache;

use axum_server::tls_rustls::RustlsConfig;
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::RBACAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::tls::TlsConfig;
use rusternetes_storage::{Storage, StorageBackend};
use state::ApiServerState;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Resolve the cluster **CA** certificate PEM that gets embedded as `ca.crt` in
/// ServiceAccount token secrets and `kube-root-ca.crt` ConfigMaps.
///
/// This MUST be the issuing CA, not the api-server's serving cert. The two used
/// to be the same self-signed CA:TRUE cert, so handing back the serving cert
/// happened to work; now the serving cert is a leaf (CA:FALSE) signed by a
/// separate CA, and embedding the leaf makes in-cluster clients reject the
/// api-server with `UnknownIssuer`. Prefer an explicit CA file, then the CA
/// sitting next to the serving cert, then the well-known deploy paths; fall back
/// to the serving cert only when no CA file exists (self-signed / legacy setups).
pub fn resolve_ca_cert_pem(cert_file: Option<&str>, serving_cert_pem: &str) -> String {
    let mut candidates: Vec<String> = Vec::new();
    if let Ok(p) = std::env::var("CA_CERT_PATH") {
        candidates.push(p);
    }
    if let Some(cf) = cert_file {
        if let Some(dir) = std::path::Path::new(cf).parent() {
            candidates.push(dir.join("ca.crt").to_string_lossy().into_owned());
        }
    }
    candidates.push("/etc/kubernetes/pki/ca.crt".to_string());
    if let Ok(home) = std::env::var("HOME") {
        candidates.push(format!("{home}/.rusternetes/certs/ca.crt"));
    }

    for path in &candidates {
        if let Ok(pem) = std::fs::read_to_string(path) {
            if pem.contains("BEGIN CERTIFICATE") {
                info!("Loaded cluster CA for SA tokens / kube-root-ca from {path}");
                return pem;
            }
        }
    }
    warn!(
        "No separate CA cert found (tried {:?}); falling back to the serving cert. \
         In-cluster clients using a strict TLS stack may fail to verify the api-server.",
        candidates
    );
    serving_cert_pem.to_string()
}

/// Configuration for the API server component.
pub struct ApiServerConfig {
    pub bind_address: String,
    pub jwt_secret: String,
    pub tls: bool,
    pub tls_cert_file: Option<String>,
    pub tls_key_file: Option<String>,
    pub tls_self_signed: bool,
    pub tls_san: String,
    pub skip_auth: bool,
    pub prometheus_url: Option<String>,
    /// Path to the console SPA build directory. When set, the API server
    /// serves the console UI at `/console/` and falls back to `index.html`
    /// for client-side routing.
    pub console_dir: Option<PathBuf>,
    /// Path to client CA certificate for x509 client-certificate authentication.
    /// When set, the API server verifies any client cert presented against this
    /// CA and maps its Subject CN→username and O→groups (#1129). The cert is
    /// OPTIONAL — bearer-token clients still connect; presenting one is just an
    /// additional way to authenticate.
    pub client_ca_file: Option<String>,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:6443".to_string(),
            jwt_secret: "rusternetes-secret-change-in-production".to_string(),
            tls: false,
            tls_cert_file: None,
            tls_key_file: None,
            tls_self_signed: false,
            tls_san: "localhost,127.0.0.1".to_string(),
            skip_auth: true,
            prometheus_url: None,
            console_dir: None,
            client_ca_file: None,
        }
    }
}

/// Run the API server component.
///
/// This is the main entry point for embedding the API server in the all-in-one binary.
/// Starts the HTTPS/HTTP server and blocks until shutdown.
pub async fn run(storage: Arc<StorageBackend>, config: ApiServerConfig) -> anyhow::Result<()> {
    info!("Starting Rusternetes API Server");

    let token_manager = Arc::new(TokenManager::new_auto(config.jwt_secret.as_bytes()));

    let authorizer: Arc<dyn rusternetes_common::authz::Authorizer> = if config.skip_auth {
        warn!("Authentication and authorization disabled - insecure mode");
        Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer)
    } else {
        info!("Initializing RBAC Authorizer");
        Arc::new(RBACAuthorizer::new(storage.clone()))
    };

    let metrics = Arc::new(MetricsRegistry::new().with_api_server_metrics()?);

    let ca_cert_pem = if config.tls {
        info!("TLS enabled - loading/generating certificates");
        let tls_config = if let (Some(ref cert_file), Some(ref key_file)) =
            (&config.tls_cert_file, &config.tls_key_file)
        {
            TlsConfig::from_pem_files(cert_file, key_file)?
        } else if config.tls_self_signed {
            let sans: Vec<String> = config
                .tls_san
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            TlsConfig::generate_self_signed("rusternetes-api", sans)?
        } else {
            anyhow::bail!("TLS enabled but no certificate provided");
        };
        tls_config
            .cert_pem
            .as_deref()
            .map(|serving| resolve_ca_cert_pem(config.tls_cert_file.as_deref(), serving))
    } else {
        None
    };

    // Bootstrap kubernetes Service
    let api_port = config
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

    // Create default ServiceCIDR
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
                "spec": { "cidrs": ["10.96.0.0/12"] },
                "status": { "conditions": [{ "type": "Ready", "status": "True",
                    "lastTransitionTime": chrono::Utc::now().to_rfc3339(),
                    "reason": "NetworkReady", "message": "ServiceCIDR is ready" }] }
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

    // Prometheus client
    let prom_client = if let Some(ref url) = config.prometheus_url {
        match prometheus_client::PrometheusClient::new(url.clone()) {
            Ok(c) => Some(Arc::new(c)),
            Err(e) => {
                warn!("Failed to init Prometheus client: {}", e);
                None
            }
        }
    } else {
        None
    };

    let state = Arc::new(
        ApiServerState::new(
            storage,
            token_manager,
            authorizer,
            metrics,
            config.skip_auth,
        )
        .with_ca_cert(ca_cert_pem)
        .with_prometheus_client(prom_client),
    );

    // Pre-allocate ClusterIPs
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

    let app = router::build_router(state, config.console_dir.as_deref());

    if config.tls {
        let tls_config = if let (Some(cert_file), Some(key_file)) =
            (config.tls_cert_file, config.tls_key_file)
        {
            TlsConfig::from_pem_files(&cert_file, &key_file)?
        } else {
            let sans: Vec<String> = config
                .tls_san
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            TlsConfig::generate_self_signed("rusternetes-api", sans)?
        };

        let client_cert_authn = config.client_ca_file.is_some();
        let server_config = if let Some(ref client_ca) = config.client_ca_file {
            info!(
                "Client certificate authentication enabled (CA: {})",
                client_ca
            );
            tls_config.into_mtls_server_config(client_ca)?
        } else {
            tls_config.into_server_config()?
        };
        let rustls_config = RustlsConfig::from_config(server_config);
        info!("HTTPS server listening on {}", config.bind_address);
        let addr = config.bind_address.parse()?;
        if client_cert_authn {
            // mTLS: serve via PeerCertAcceptor so the verified client cert reaches
            // handlers for x509 authn (CN→user / O→groups, #1129).
            let mut server = axum_server::bind(addr).acceptor(
                crate::peer_cert_acceptor::PeerCertAcceptor::new(rustls_config),
            );
            server
                .http_builder()
                .http2()
                .initial_stream_window_size(256 * 1024)
                .initial_connection_window_size(256 * 1024 * 100)
                .max_concurrent_streams(250);
            server.serve(app.into_make_service()).await?;
        } else {
            let mut server = axum_server::bind_rustls(addr, rustls_config);
            server
                .http_builder()
                .http2()
                .initial_stream_window_size(256 * 1024)
                .initial_connection_window_size(256 * 1024 * 100)
                .max_concurrent_streams(250);
            server.serve(app.into_make_service()).await?;
        }
    } else {
        info!(
            "API Server listening on {} (HTTP, no TLS)",
            config.bind_address
        );
        let listener = tokio::net::TcpListener::bind(&config.bind_address).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}
