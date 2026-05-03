pub mod admission;
pub mod admission_webhook;
pub mod bootstrap;
pub mod conversion;
pub mod dynamic_routes;
#[allow(dead_code)]
pub mod flow_control;
pub mod gnostic;
pub mod handlers;
pub mod ip_allocator;
pub mod middleware;
pub mod openapi;
pub mod patch;
pub mod prometheus_client;
pub mod protobuf;
#[allow(dead_code)]
pub mod response;
pub mod router;
#[allow(dead_code)]
pub mod spdy;
#[allow(dead_code)]
pub mod spdy_handlers;
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
    /// Path to client CA certificate for mTLS client certificate authentication.
    /// When set, the API server requires clients to present a certificate signed
    /// by this CA. The CN field becomes the username and O fields become groups.
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
        tls_config.cert_pem.clone()
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
        let mut server = axum_server::bind_rustls(config.bind_address.parse()?, rustls_config);
        server
            .http_builder()
            .http2()
            .initial_stream_window_size(256 * 1024)
            .initial_connection_window_size(256 * 1024 * 100)
            .max_concurrent_streams(250);
        server.serve(app.into_make_service()).await?;
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
