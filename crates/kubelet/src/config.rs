use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// KubeletConfiguration contains the configuration for the Kubelet
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletConfiguration {
    /// API version of the configuration
    #[serde(default = "default_api_version")]
    pub api_version: String,

    /// Kind of the configuration
    #[serde(default = "default_kind")]
    pub kind: String,

    /// Root directory for managing kubelet files
    /// (volume data, plugin state, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_dir: Option<String>,

    /// Directory path for managing volume data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_dir: Option<String>,

    /// Directory where volume plugins are installed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_plugin_dir: Option<String>,

    /// How frequently to sync pod state (in seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_frequency: Option<u64>,

    /// Port for the metrics server
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_bind_port: Option<u16>,

    /// Log verbosity level
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_level: Option<String>,

    /// Cluster service CIDR (e.g., "10.96.0.0/12")
    /// The first IP in this range is used for the kubernetes service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_service_cidr: Option<String>,
}

fn default_api_version() -> String {
    "kubelet.config.k8s.io/v1beta1".to_string()
}

fn default_kind() -> String {
    "KubeletConfiguration".to_string()
}

impl Default for KubeletConfiguration {
    fn default() -> Self {
        Self {
            api_version: default_api_version(),
            kind: default_kind(),
            root_dir: None,
            volume_dir: None,
            volume_plugin_dir: None,
            sync_frequency: None,
            metrics_bind_port: None,
            log_level: None,
            cluster_service_cidr: None,
        }
    }
}

impl KubeletConfiguration {
    /// Load configuration from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        let config: KubeletConfiguration = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {:?}", path.as_ref()))?;

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Validate API version
        if self.api_version != "kubelet.config.k8s.io/v1beta1" {
            anyhow::bail!(
                "Unsupported apiVersion: {}. Expected: kubelet.config.k8s.io/v1beta1",
                self.api_version
            );
        }

        // Validate kind
        if self.kind != "KubeletConfiguration" {
            anyhow::bail!(
                "Invalid kind: {}. Expected: KubeletConfiguration",
                self.kind
            );
        }

        // Validate paths exist if specified
        if let Some(root_dir) = &self.root_dir {
            let path = PathBuf::from(root_dir);
            if !path.exists() {
                tracing::warn!(
                    "Root directory does not exist and will be created: {}",
                    root_dir
                );
            }
        }

        if let Some(volume_dir) = &self.volume_dir {
            let path = PathBuf::from(volume_dir);
            if !path.exists() {
                tracing::warn!(
                    "Volume directory does not exist and will be created: {}",
                    volume_dir
                );
            }
        }

        // Validate sync frequency
        if let Some(sync_freq) = self.sync_frequency {
            if sync_freq == 0 {
                anyhow::bail!("syncFrequency must be greater than 0");
            }
            if sync_freq > 3600 {
                tracing::warn!(
                    "syncFrequency of {} seconds is unusually high (> 1 hour)",
                    sync_freq
                );
            }
        }

        // Validate metrics port
        if let Some(port) = self.metrics_bind_port {
            if port < 1024 {
                tracing::warn!("metricsBindPort {} is a privileged port (< 1024)", port);
            }
        }

        // Validate log level
        if let Some(level) = &self.log_level {
            match level.to_lowercase().as_str() {
                "trace" | "debug" | "info" | "warn" | "error" => {}
                _ => anyhow::bail!(
                    "Invalid logLevel: {}. Must be one of: trace, debug, info, warn, error",
                    level
                ),
            }
        }

        Ok(())
    }

    /// Save configuration to a YAML file
    #[allow(dead_code)]
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let contents = serde_yaml::to_string(self).context("Failed to serialize configuration")?;

        std::fs::write(path.as_ref(), contents)
            .with_context(|| format!("Failed to write config file: {:?}", path.as_ref()))?;

        Ok(())
    }
}

/// RuntimeConfig holds the resolved runtime configuration for the kubelet
/// after merging CLI flags, config file, environment variables, and defaults
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Root directory for kubelet files
    pub root_dir: PathBuf,

    /// Directory for volume data
    pub volume_dir: PathBuf,

    /// Directory for volume plugins
    pub volume_plugin_dir: PathBuf,

    /// Sync frequency in seconds
    pub sync_frequency: u64,

    /// Metrics server port
    pub metrics_bind_port: u16,

    /// Log level
    pub log_level: String,

    /// Node name
    pub node_name: String,

    /// Etcd endpoints
    pub etcd_endpoints: Vec<String>,

    /// Kubernetes service ClusterIP (first IP in service CIDR)
    pub kubernetes_service_host: String,
}

/// Extract the first usable IP address from a CIDR range
/// For example, "10.96.0.0/12" -> "10.96.0.1"
fn first_ip_from_cidr(cidr: &str) -> Result<String> {
    use std::net::IpAddr;

    let parts: Vec<&str> = cidr.split('/').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid CIDR format: {}", cidr);
    }

    let base_ip: IpAddr = parts[0]
        .parse()
        .with_context(|| format!("Invalid IP address in CIDR: {}", parts[0]))?;

    match base_ip {
        IpAddr::V4(ipv4) => {
            // Get the IP as u32, add 1, convert back
            let ip_u32 = u32::from(ipv4);
            let first_ip_u32 = ip_u32 + 1;
            let first_ip = std::net::Ipv4Addr::from(first_ip_u32);
            Ok(first_ip.to_string())
        }
        IpAddr::V6(ipv6) => {
            // For IPv6, convert to u128, add 1, convert back
            let ip_u128 = u128::from(ipv6);
            let first_ip_u128 = ip_u128 + 1;
            let first_ip = std::net::Ipv6Addr::from(first_ip_u128);
            Ok(first_ip.to_string())
        }
    }
}

impl RuntimeConfig {
    /// Build RuntimeConfig from multiple sources with proper precedence:
    /// CLI flags > Config file > Environment variables > Defaults
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        cli_root_dir: Option<String>,
        cli_volume_dir: Option<String>,
        cli_volume_plugin_dir: Option<String>,
        cli_sync_frequency: Option<u64>,
        cli_metrics_port: Option<u16>,
        cli_log_level: Option<String>,
        config_file: Option<KubeletConfiguration>,
        node_name: String,
        etcd_endpoints: Vec<String>,
    ) -> Result<Self> {
        Self::build_with_env(
            cli_root_dir,
            cli_volume_dir,
            cli_volume_plugin_dir,
            cli_sync_frequency,
            cli_metrics_port,
            cli_log_level,
            config_file,
            node_name,
            etcd_endpoints,
            |key| std::env::var(key).ok(),
        )
    }

    /// Testable core of [`RuntimeConfig::build`].
    ///
    /// Takes the environment lookup as an argument instead of reading the
    /// process environment, so a test can exercise the config-file and default
    /// tiers without depending on the developer's shell. The environment tier
    /// sits above the defaults, so reading the real environment makes any
    /// assertion about a default fail once the corresponding variable is
    /// exported — and this repo's own workflow exports `KUBELET_VOLUMES_PATH`.
    /// Same rationale as `rusternetes_common::async_runtime`.
    #[allow(clippy::too_many_arguments)]
    fn build_with_env(
        cli_root_dir: Option<String>,
        cli_volume_dir: Option<String>,
        cli_volume_plugin_dir: Option<String>,
        cli_sync_frequency: Option<u64>,
        cli_metrics_port: Option<u16>,
        cli_log_level: Option<String>,
        config_file: Option<KubeletConfiguration>,
        node_name: String,
        etcd_endpoints: Vec<String>,
        mut get_env: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self> {
        // Determine root directory
        // Precedence: CLI > Config > Env > Default
        let root_dir = cli_root_dir
            .or_else(|| config_file.as_ref().and_then(|c| c.root_dir.clone()))
            .or_else(|| get_env("KUBELET_ROOT_DIR"))
            .unwrap_or_else(|| {
                // For development: use current dir, for production: /var/lib/kubelet
                std::env::current_dir()
                    .ok()
                    .and_then(|p| p.to_str().map(String::from))
                    .unwrap_or_else(|| "/var/lib/kubelet".to_string())
            });

        // Determine volume directory
        // Precedence: CLI > Config > Env > Root-dir-based default
        let volume_dir = cli_volume_dir
            .or_else(|| config_file.as_ref().and_then(|c| c.volume_dir.clone()))
            .or_else(|| get_env("KUBELET_VOLUMES_PATH"))
            .unwrap_or_else(|| format!("{}/volumes", root_dir));

        // Determine volume plugin directory
        let volume_plugin_dir = cli_volume_plugin_dir
            .or_else(|| {
                config_file
                    .as_ref()
                    .and_then(|c| c.volume_plugin_dir.clone())
            })
            .or_else(|| get_env("KUBELET_VOLUME_PLUGIN_DIR"))
            .unwrap_or_else(|| "/usr/libexec/kubernetes/kubelet-plugins/volume/exec".to_string());

        // Determine sync frequency
        let sync_frequency = cli_sync_frequency
            .or_else(|| config_file.as_ref().and_then(|c| c.sync_frequency))
            .unwrap_or(10);

        // Determine metrics port
        let metrics_bind_port = cli_metrics_port
            .or_else(|| config_file.as_ref().and_then(|c| c.metrics_bind_port))
            .unwrap_or(8082);

        // Determine log level
        let log_level = cli_log_level
            .or_else(|| config_file.as_ref().and_then(|c| c.log_level.clone()))
            .or_else(|| get_env("RUST_LOG"))
            .unwrap_or_else(|| "info".to_string());

        // Determine cluster service CIDR and extract kubernetes service host IP
        // Precedence: Config > Env > Default (10.96.0.0/12)
        let cluster_service_cidr = config_file
            .as_ref()
            .and_then(|c| c.cluster_service_cidr.clone())
            .or_else(|| get_env("CLUSTER_SERVICE_CIDR"))
            .unwrap_or_else(|| "10.96.0.0/12".to_string());

        // Use KUBERNETES_SERVICE_HOST_OVERRIDE if set — this allows the API server
        // address to be configured for environments where ClusterIP routing doesn't
        // work from pod containers (e.g., Podman Machine without br_netfilter).
        // Falls back to the kubernetes service ClusterIP (10.96.0.1).
        let kubernetes_service_host =
            get_env("KUBERNETES_SERVICE_HOST_OVERRIDE").unwrap_or_else(|| {
                first_ip_from_cidr(&cluster_service_cidr)
                    .unwrap_or_else(|_| "10.96.0.1".to_string())
            });

        let config = Self {
            root_dir: PathBuf::from(root_dir),
            volume_dir: PathBuf::from(volume_dir),
            volume_plugin_dir: PathBuf::from(volume_plugin_dir),
            sync_frequency,
            metrics_bind_port,
            log_level,
            node_name,
            etcd_endpoints,
            kubernetes_service_host,
        };

        config.validate()?;
        config.create_directories()?;

        Ok(config)
    }

    /// Validate the runtime configuration
    fn validate(&self) -> Result<()> {
        if self.node_name.is_empty() {
            anyhow::bail!("Node name cannot be empty");
        }

        if self.etcd_endpoints.is_empty() {
            anyhow::bail!("At least one etcd endpoint must be specified");
        }

        if self.sync_frequency == 0 {
            anyhow::bail!("Sync frequency must be greater than 0");
        }

        Ok(())
    }

    /// Create necessary directories
    fn create_directories(&self) -> Result<()> {
        // Create root directory
        std::fs::create_dir_all(&self.root_dir)
            .with_context(|| format!("Failed to create root directory: {:?}", self.root_dir))?;

        // Create volume directory
        std::fs::create_dir_all(&self.volume_dir)
            .with_context(|| format!("Failed to create volume directory: {:?}", self.volume_dir))?;

        // Create volume plugin directory
        if let Err(e) = std::fs::create_dir_all(&self.volume_plugin_dir) {
            tracing::warn!(
                "Failed to create volume plugin directory {:?}: {}. This is non-fatal for basic operation.",
                self.volume_plugin_dir,
                e
            );
        }

        Ok(())
    }

    /// Display the configuration (for logging/debugging)
    pub fn display(&self) -> String {
        format!(
            r#"Kubelet Runtime Configuration:
  Node Name: {}
  Root Directory: {}
  Volume Directory: {}
  Volume Plugin Directory: {}
  Sync Frequency: {}s
  Metrics Port: {}
  Log Level: {}
  Etcd Endpoints: {}"#,
            self.node_name,
            self.root_dir.display(),
            self.volume_dir.display(),
            self.volume_plugin_dir.display(),
            self.sync_frequency,
            self.metrics_bind_port,
            self.log_level,
            self.etcd_endpoints.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = KubeletConfiguration::default();
        assert_eq!(config.api_version, "kubelet.config.k8s.io/v1beta1");
        assert_eq!(config.kind, "KubeletConfiguration");
    }

    #[test]
    fn test_config_validation() {
        let mut config = KubeletConfiguration::default();
        assert!(config.validate().is_ok());

        // Invalid API version
        config.api_version = "v1".to_string();
        assert!(config.validate().is_err());

        // Reset
        config.api_version = "kubelet.config.k8s.io/v1beta1".to_string();

        // Invalid kind
        config.kind = "Pod".to_string();
        assert!(config.validate().is_err());

        // Reset
        config.kind = "KubeletConfiguration".to_string();

        // Invalid sync frequency
        config.sync_frequency = Some(0);
        assert!(config.validate().is_err());

        // Reset
        config.sync_frequency = Some(10);

        // Invalid log level
        config.log_level = Some("invalid".to_string());
        assert!(config.validate().is_err());

        // Valid log level
        config.log_level = Some("debug".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_file_roundtrip() {
        let config = KubeletConfiguration {
            api_version: "kubelet.config.k8s.io/v1beta1".to_string(),
            kind: "KubeletConfiguration".to_string(),
            root_dir: Some("/var/lib/kubelet".to_string()),
            volume_dir: Some("/var/lib/kubelet/volumes".to_string()),
            volume_plugin_dir: Some(
                "/usr/libexec/kubernetes/kubelet-plugins/volume/exec".to_string(),
            ),
            sync_frequency: Some(15),
            metrics_bind_port: Some(10250),
            log_level: Some("info".to_string()),
            cluster_service_cidr: Some("10.96.0.0/12".to_string()),
        };

        // Write to temp file
        let mut file = NamedTempFile::new().unwrap();
        let yaml = serde_yaml::to_string(&config).unwrap();
        file.write_all(yaml.as_bytes()).unwrap();

        // Read back
        let loaded = KubeletConfiguration::from_file(file.path()).unwrap();

        assert_eq!(loaded.api_version, config.api_version);
        assert_eq!(loaded.kind, config.kind);
        assert_eq!(loaded.root_dir, config.root_dir);
        assert_eq!(loaded.volume_dir, config.volume_dir);
        assert_eq!(loaded.sync_frequency, config.sync_frequency);
        assert_eq!(loaded.metrics_bind_port, config.metrics_bind_port);
        assert_eq!(loaded.log_level, config.log_level);
    }

    #[test]
    fn test_runtime_config_precedence() {
        use tempfile::tempdir;

        // Create temp directories for testing
        let tmp_dir = tempdir().unwrap();
        let cli_root = tmp_dir.path().join("cli/root");
        let cli_volumes = tmp_dir.path().join("cli/volumes");
        let config_root = tmp_dir.path().join("config/root");
        let config_volumes = tmp_dir.path().join("config/volumes");

        // CLI values should take precedence
        let runtime = RuntimeConfig::build(
            Some(cli_root.to_str().unwrap().to_string()),
            Some(cli_volumes.to_str().unwrap().to_string()),
            None,
            Some(20),
            Some(9090),
            Some("debug".to_string()),
            Some(KubeletConfiguration {
                root_dir: Some(config_root.to_str().unwrap().to_string()),
                volume_dir: Some(config_volumes.to_str().unwrap().to_string()),
                sync_frequency: Some(30),
                ..Default::default()
            }),
            "test-node".to_string(),
            vec!["http://localhost:2379".to_string()],
        )
        .unwrap();

        assert_eq!(runtime.root_dir, cli_root);
        assert_eq!(runtime.volume_dir, cli_volumes);
        assert_eq!(runtime.sync_frequency, 20);
        assert_eq!(runtime.metrics_bind_port, 9090);
        assert_eq!(runtime.log_level, "debug");
    }

    #[test]
    fn test_runtime_config_defaults() {
        // Empty environment: the env tier outranks the defaults asserted here,
        // so reading the real one would fail this test for anyone who exports
        // RUST_LOG or KUBELET_VOLUMES_PATH.
        let runtime = RuntimeConfig::build_with_env(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "test-node".to_string(),
            vec!["http://localhost:2379".to_string()],
            |_| None,
        )
        .unwrap();

        assert_eq!(runtime.sync_frequency, 10);
        assert_eq!(runtime.metrics_bind_port, 8082);
        assert_eq!(runtime.log_level, "info");
        assert!(runtime.volume_dir.ends_with("volumes"));
    }

    #[test]
    fn test_runtime_config_env_tier_outranks_defaults() {
        // The tier the test above has to be insulated from: with no CLI flag and
        // no config file, an exported value wins over the default.
        let runtime = RuntimeConfig::build_with_env(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "test-node".to_string(),
            vec!["http://localhost:2379".to_string()],
            |key| match key {
                "RUST_LOG" => Some("debug".to_string()),
                "KUBELET_VOLUMES_PATH" => Some("/tmp/rusternetes-test-volume-dir".to_string()),
                _ => None,
            },
        )
        .unwrap();

        assert_eq!(runtime.log_level, "debug");
        assert_eq!(
            runtime.volume_dir,
            PathBuf::from("/tmp/rusternetes-test-volume-dir")
        );
    }

    #[test]
    fn test_runtime_config_validation() {
        // Empty node name should fail
        let result = RuntimeConfig::build(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "".to_string(),
            vec!["http://localhost:2379".to_string()],
        );
        assert!(result.is_err());

        // Empty etcd endpoints should fail
        let result = RuntimeConfig::build(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "test-node".to_string(),
            vec![],
        );
        assert!(result.is_err());
    }
}
