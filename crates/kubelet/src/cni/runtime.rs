use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

use super::config::{CniConfigManager, NetworkConfigList};
use super::plugin::CniPluginManager;
use super::result::{CniError, CniResult, ErrorCode};
use super::CniCommand;

/// Network attachment information for a container
#[derive(Debug, Clone)]
pub struct NetworkAttachment {
    /// Container ID
    pub container_id: String,

    /// Network namespace path
    pub netns: String,

    /// Interface name
    pub ifname: String,

    /// Network configuration used
    pub network: String,

    /// CNI result from ADD operation
    pub result: CniResult,
}

/// CNI Runtime - high-level interface for container network lifecycle
pub struct CniRuntime {
    /// Plugin manager
    plugin_manager: Arc<Mutex<CniPluginManager>>,

    /// Configuration manager
    config_manager: CniConfigManager,

    /// Active network attachments (container_id -> attachments)
    attachments: Arc<Mutex<HashMap<String, Vec<NetworkAttachment>>>>,

    /// Default network to use
    default_network: Option<String>,
}

impl CniRuntime {
    /// Create a new CNI runtime
    pub fn new(plugin_paths: Vec<PathBuf>, config_dir: PathBuf) -> Result<Self, CniError> {
        let mut plugin_manager = CniPluginManager::new(plugin_paths);
        plugin_manager.discover_plugins()?;

        let config_manager = CniConfigManager::new(config_dir);

        Ok(Self {
            plugin_manager: Arc::new(Mutex::new(plugin_manager)),
            config_manager,
            attachments: Arc::new(Mutex::new(HashMap::new())),
            default_network: None,
        })
    }

    /// Set the default network to use
    pub fn with_default_network(mut self, network: String) -> Self {
        self.default_network = Some(network);
        self
    }

    /// Setup network for a container (ADD operation)
    pub fn setup_network(
        &self,
        container_id: &str,
        netns: &str,
        ifname: &str,
        network_name: Option<&str>,
    ) -> Result<CniResult, CniError> {
        info!(
            "Setting up network for container {} in netns {}",
            container_id, netns
        );

        // Validate inputs
        if container_id.is_empty() {
            return Err(CniError::new(
                ErrorCode::InvalidEnvironmentVariables,
                "Container ID cannot be empty".to_string(),
            ));
        }

        if netns.is_empty() {
            return Err(CniError::new(
                ErrorCode::InvalidEnvironmentVariables,
                "Network namespace cannot be empty".to_string(),
            ));
        }

        // Check if netns exists
        if !Path::new(netns).exists() {
            return Err(CniError::new(
                ErrorCode::InvalidEnvironmentVariables,
                format!("Network namespace {} does not exist", netns),
            ));
        }

        // Get network configuration
        let network = self.get_network_config(network_name)?;

        // Execute ADD command
        let plugin_manager = self.plugin_manager.lock().unwrap();
        let result = plugin_manager.execute_network(
            CniCommand::Add,
            &network,
            container_id,
            netns,
            ifname,
            None,
        )?;

        // Store attachment
        let attachment = NetworkAttachment {
            container_id: container_id.to_string(),
            netns: netns.to_string(),
            ifname: ifname.to_string(),
            network: network.name.clone(),
            result: result.clone(),
        };

        let mut attachments = self.attachments.lock().unwrap();
        attachments
            .entry(container_id.to_string())
            .or_default()
            .push(attachment);

        info!(
            "Network setup complete for container {}, IP: {:?}",
            container_id,
            result.primary_ip()
        );

        Ok(result)
    }

    /// Teardown network for a container (DEL operation)
    pub fn teardown_network(
        &self,
        container_id: &str,
        netns: &str,
        ifname: &str,
        network_name: Option<&str>,
    ) -> Result<(), CniError> {
        info!(
            "Tearing down network for container {} in netns {}",
            container_id, netns
        );

        // Get network configuration
        let network = self.get_network_config(network_name)?;

        // Execute DEL command
        let plugin_manager = self.plugin_manager.lock().unwrap();
        plugin_manager.execute_network(
            CniCommand::Del,
            &network,
            container_id,
            netns,
            ifname,
            None,
        )?;

        // Remove attachment
        let mut attachments = self.attachments.lock().unwrap();
        if let Some(container_attachments) = attachments.get_mut(container_id) {
            container_attachments.retain(|a| a.network != network.name);
            if container_attachments.is_empty() {
                attachments.remove(container_id);
            }
        }

        info!("Network teardown complete for container {}", container_id);

        Ok(())
    }

    /// Check network configuration for a container (CHECK operation)
    pub fn check_network(
        &self,
        container_id: &str,
        netns: &str,
        ifname: &str,
        network_name: Option<&str>,
    ) -> Result<(), CniError> {
        debug!(
            "Checking network for container {} in netns {}",
            container_id, netns
        );

        // Get network configuration
        let network = self.get_network_config(network_name)?;

        // Skip check if disabled in config
        if network.disable_check == Some(true) {
            debug!("Check disabled for network {}", network.name);
            return Ok(());
        }

        // Execute CHECK command
        let plugin_manager = self.plugin_manager.lock().unwrap();
        plugin_manager.execute_network(
            CniCommand::Check,
            &network,
            container_id,
            netns,
            ifname,
            None,
        )?;

        debug!("Network check passed for container {}", container_id);

        Ok(())
    }

    /// Teardown all networks for a container
    pub fn teardown_all_networks(&self, container_id: &str) -> Result<(), CniError> {
        info!("Tearing down all networks for container {}", container_id);

        let attachments = {
            let attachments_lock = self.attachments.lock().unwrap();
            attachments_lock
                .get(container_id)
                .cloned()
                .unwrap_or_default()
        };

        for attachment in attachments {
            if let Err(e) = self.teardown_network(
                container_id,
                &attachment.netns,
                &attachment.ifname,
                Some(&attachment.network),
            ) {
                warn!(
                    "Failed to teardown network {} for container {}: {}",
                    attachment.network, container_id, e
                );
            }
        }

        Ok(())
    }

    /// Get network attachments for a container
    pub fn get_attachments(&self, container_id: &str) -> Vec<NetworkAttachment> {
        let attachments = self.attachments.lock().unwrap();
        attachments.get(container_id).cloned().unwrap_or_default()
    }

    /// Get the primary IP address for a container
    pub fn get_container_ip(&self, container_id: &str) -> Option<String> {
        let attachments = self.attachments.lock().unwrap();
        attachments
            .get(container_id)
            .and_then(|atts| atts.first())
            .and_then(|att| att.result.primary_ip())
            .map(|ip| ip.to_string())
    }

    /// Reload plugin discovery
    pub fn reload_plugins(&self) -> Result<(), CniError> {
        info!("Reloading CNI plugins");
        let mut plugin_manager = self.plugin_manager.lock().unwrap();
        plugin_manager.discover_plugins()
    }

    /// Get network configuration by name or use default
    fn get_network_config(
        &self,
        network_name: Option<&str>,
    ) -> Result<NetworkConfigList, CniError> {
        if let Some(name) = network_name {
            self.config_manager.get_config(name)
        } else if let Some(default) = &self.default_network {
            self.config_manager.get_config(default)
        } else {
            self.config_manager.get_default_config()
        }
    }

    /// List available networks
    pub fn list_networks(&self) -> Result<Vec<String>, CniError> {
        let configs = self.config_manager.load_configs()?;
        Ok(configs.into_iter().map(|c| c.name).collect())
    }

    /// Get statistics about active attachments
    pub fn get_stats(&self) -> NetworkStats {
        let attachments = self.attachments.lock().unwrap();
        NetworkStats {
            total_containers: attachments.len(),
            total_attachments: attachments.values().map(|v| v.len()).sum(),
        }
    }
}

/// Network statistics
#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub total_containers: usize,
    pub total_attachments: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_cni_runtime_creation() {
        let temp_plugin_dir = TempDir::new().unwrap();
        let temp_config_dir = TempDir::new().unwrap();

        let runtime = CniRuntime::new(
            vec![temp_plugin_dir.path().to_path_buf()],
            temp_config_dir.path().to_path_buf(),
        );

        assert!(runtime.is_ok());
    }

    #[test]
    fn test_default_network() {
        let temp_plugin_dir = TempDir::new().unwrap();
        let temp_config_dir = TempDir::new().unwrap();

        let runtime = CniRuntime::new(
            vec![temp_plugin_dir.path().to_path_buf()],
            temp_config_dir.path().to_path_buf(),
        )
        .unwrap()
        .with_default_network("test-network".to_string());

        assert_eq!(runtime.default_network, Some("test-network".to_string()));
    }

    #[test]
    fn test_get_stats() {
        let temp_plugin_dir = TempDir::new().unwrap();
        let temp_config_dir = TempDir::new().unwrap();

        let runtime = CniRuntime::new(
            vec![temp_plugin_dir.path().to_path_buf()],
            temp_config_dir.path().to_path_buf(),
        )
        .unwrap();

        let stats = runtime.get_stats();
        assert_eq!(stats.total_containers, 0);
        assert_eq!(stats.total_attachments, 0);
    }

    #[test]
    fn test_setup_network_validation() {
        let temp_plugin_dir = TempDir::new().unwrap();
        let temp_config_dir = TempDir::new().unwrap();

        let runtime = CniRuntime::new(
            vec![temp_plugin_dir.path().to_path_buf()],
            temp_config_dir.path().to_path_buf(),
        )
        .unwrap();

        // Test with empty container ID
        let result = runtime.setup_network("", "/var/run/netns/test", "eth0", None);
        assert!(result.is_err());

        // Test with empty netns
        let result = runtime.setup_network("container1", "", "eth0", None);
        assert!(result.is_err());
    }
}
