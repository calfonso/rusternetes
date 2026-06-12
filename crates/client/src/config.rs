//! Client connection config: kubeconfig file OR in-cluster SA projection.

use anyhow::{Context, Result};
use std::path::Path;

/// Standard mount point of the projected ServiceAccount token volume.
pub const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Resolved connection parameters for an [`crate::http::ApiClient`].
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub token: Option<String>,
    pub ca_pem: Option<String>,
}

impl ClientConfig {
    /// Standard in-cluster config: SA token dir + KUBERNETES_SERVICE_HOST /
    /// KUBERNETES_SERVICE_PORT (honoring KUBERNETES_SERVICE_HOST_OVERRIDE,
    /// the repo's pod-side override).
    pub fn in_cluster() -> Result<Self> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST_OVERRIDE")
            .or_else(|_| std::env::var("KUBERNETES_SERVICE_HOST"))
            .ok();
        let port = std::env::var("KUBERNETES_SERVICE_PORT").ok();
        Self::in_cluster_from(Path::new(SA_DIR), host, port)
    }

    /// Injectable variant for tests.
    pub fn in_cluster_from(
        sa_dir: &Path,
        host: Option<String>,
        port: Option<String>,
    ) -> Result<Self> {
        let host = host.context("KUBERNETES_SERVICE_HOST not set")?;
        let port = port.unwrap_or_else(|| "6443".to_string());
        let token = std::fs::read_to_string(sa_dir.join("token"))
            .context("reading serviceaccount token")?;
        let ca_pem = std::fs::read_to_string(sa_dir.join("ca.crt")).ok();
        Ok(Self {
            base_url: format!("https://{}:{}", host, port),
            token: Some(token.trim().to_string()),
            ca_pem,
        })
    }
}
