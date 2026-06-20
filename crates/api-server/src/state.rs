use crate::admission_webhook::AdmissionWebhookManager;
use crate::ip_allocator::ClusterIPAllocator;
use crate::prometheus_client::PrometheusClient;
use crate::watch_cache::WatchCache;
use rusternetes_common::auth::{BootstrapTokenManager, TokenManager};
use rusternetes_common::authz::Authorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;

/// Shared state for the API server
pub struct ApiServerState {
    pub storage: Arc<StorageBackend>,
    pub token_manager: Arc<TokenManager>,
    pub bootstrap_token_manager: Arc<BootstrapTokenManager>,
    pub authorizer: Arc<dyn Authorizer>,
    pub metrics: Arc<MetricsRegistry>,
    pub skip_auth: bool,
    pub ip_allocator: Arc<ClusterIPAllocator>,
    pub webhook_manager: Arc<AdmissionWebhookManager<StorageBackend>>,
    pub watch_cache: Arc<WatchCache>,
    pub ca_cert_pem: Option<String>,
    pub prometheus_client: Option<Arc<PrometheusClient>>,
}

impl ApiServerState {
    pub fn new(
        storage: Arc<StorageBackend>,
        token_manager: Arc<TokenManager>,
        authorizer: Arc<dyn Authorizer>,
        metrics: Arc<MetricsRegistry>,
        skip_auth: bool,
    ) -> Self {
        let webhook_manager = Arc::new(AdmissionWebhookManager::new(storage.clone()));
        let watch_cache = Arc::new(WatchCache::new(storage.clone()));
        // Reclaim per-prefix replay-ring memory once a prefix's watchers drop to
        // zero (#1089).
        watch_cache.spawn_idle_gc();

        Self {
            storage,
            token_manager,
            bootstrap_token_manager: Arc::new(BootstrapTokenManager::new()),
            authorizer,
            metrics,
            skip_auth,
            ip_allocator: Arc::new(ClusterIPAllocator::new()),
            webhook_manager,
            watch_cache,
            ca_cert_pem: None,
            prometheus_client: None,
        }
    }

    /// Set the CA certificate PEM for distribution to service accounts
    pub fn with_ca_cert(mut self, ca_cert_pem: Option<String>) -> Self {
        self.ca_cert_pem = ca_cert_pem;
        self
    }

    /// Set the Prometheus client for custom metrics
    pub fn with_prometheus_client(
        mut self,
        prometheus_client: Option<Arc<PrometheusClient>>,
    ) -> Self {
        self.prometheus_client = prometheus_client;
        self
    }
}
