use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tracing::{debug, error, info, warn};

use super::config::{NetworkConfigList, PluginConfig};
use super::result::{CniError, CniResult, ErrorCode};
use super::{CniCommand, CNI_VERSION};
use super::{CNI_ARGS, CNI_COMMAND, CNI_CONTAINERID, CNI_IFNAME, CNI_NETNS, CNI_PATH};

/// CNI Plugin executor
pub struct CniPlugin {
    /// Plugin type/name (executable name)
    plugin_type: String,

    /// Plugin binary path
    plugin_path: PathBuf,
}

impl CniPlugin {
    /// Create a new CNI plugin reference
    pub fn new(plugin_type: String, plugin_path: PathBuf) -> Self {
        Self {
            plugin_type,
            plugin_path,
        }
    }

    /// Execute the plugin with given parameters
    #[allow(clippy::too_many_arguments)]
    pub fn execute(
        &self,
        command: CniCommand,
        container_id: &str,
        netns: &str,
        ifname: &str,
        config: &str,
        args: Option<&str>,
        cni_path: &str,
    ) -> Result<CniResult, CniError> {
        debug!(
            "Executing CNI plugin {} with command {}",
            self.plugin_type, command
        );

        // Prepare environment variables
        let mut cmd = Command::new(&self.plugin_path);
        cmd.env(CNI_COMMAND, command.as_str())
            .env(CNI_CONTAINERID, container_id)
            .env(CNI_NETNS, netns)
            .env(CNI_IFNAME, ifname)
            .env(CNI_PATH, cni_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(args) = args {
            cmd.env(CNI_ARGS, args);
        }

        debug!("Plugin config: {}", config);

        // Spawn the process
        let mut child = cmd.spawn().map_err(|e| {
            CniError::new(
                ErrorCode::IoFailure,
                format!("Failed to spawn plugin {}: {}", self.plugin_type, e),
            )
        })?;

        // Write config to stdin
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(config.as_bytes()).map_err(|e| {
                CniError::new(
                    ErrorCode::IoFailure,
                    format!("Failed to write to plugin stdin: {}", e),
                )
            })?;
        }

        // Wait for the plugin to complete
        let output = child.wait_with_output().map_err(|e| {
            CniError::new(
                ErrorCode::IoFailure,
                format!("Failed to wait for plugin: {}", e),
            )
        })?;

        // Check exit status
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);

            error!("Plugin {} failed: {}", self.plugin_type, stderr);

            // Try to parse error from stdout (CNI plugins return errors as JSON on stdout)
            if let Ok(cni_error) = serde_json::from_slice::<CniError>(&output.stdout) {
                return Err(cni_error);
            }

            return Err(CniError::new(
                ErrorCode::Generic,
                format!(
                    "Plugin {} failed with exit code {}: {}",
                    self.plugin_type,
                    output.status.code().unwrap_or(-1),
                    if !stderr.is_empty() {
                        stderr.as_ref()
                    } else {
                        stdout.as_ref()
                    }
                ),
            ));
        }

        // For DEL and CHECK commands, success with no output is valid
        if matches!(command, CniCommand::Del | CniCommand::Check) && output.stdout.is_empty() {
            return Ok(CniResult::new(CNI_VERSION.to_string()));
        }

        // Parse the result from stdout
        let result: CniResult = serde_json::from_slice(&output.stdout).map_err(|e| {
            let stdout = String::from_utf8_lossy(&output.stdout);
            error!("Failed to parse plugin output: {}", stdout);
            CniError::new(
                ErrorCode::DecodingFailure,
                format!("Failed to parse plugin output: {}", e),
            )
            .with_details(stdout.to_string())
        })?;

        debug!("Plugin {} completed successfully", self.plugin_type);
        Ok(result)
    }
}

/// CNI Plugin Manager - manages plugin discovery and execution
pub struct CniPluginManager {
    /// Directories to search for CNI plugins
    plugin_paths: Vec<PathBuf>,

    /// Cache of discovered plugins
    plugins: HashMap<String, PathBuf>,
}

impl CniPluginManager {
    /// Create a new plugin manager
    pub fn new(plugin_paths: Vec<PathBuf>) -> Self {
        Self {
            plugin_paths,
            plugins: HashMap::new(),
        }
    }

    /// Discover all available CNI plugins
    pub fn discover_plugins(&mut self) -> Result<(), CniError> {
        info!("Discovering CNI plugins in {:?}", self.plugin_paths);

        self.plugins.clear();

        for path in &self.plugin_paths {
            if !path.exists() {
                warn!("Plugin path {:?} does not exist", path);
                continue;
            }

            let entries = std::fs::read_dir(path).map_err(|e| {
                CniError::new(
                    ErrorCode::IoFailure,
                    format!("Failed to read plugin directory {:?}: {}", path, e),
                )
            })?;

            for entry in entries {
                let entry = entry.map_err(|e| {
                    CniError::new(
                        ErrorCode::IoFailure,
                        format!("Failed to read directory entry: {}", e),
                    )
                })?;

                let file_path = entry.path();

                // Check if it's a file and executable
                if file_path.is_file() {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let metadata = std::fs::metadata(&file_path).map_err(|e| {
                            CniError::new(
                                ErrorCode::IoFailure,
                                format!("Failed to get file metadata: {}", e),
                            )
                        })?;

                        let permissions = metadata.permissions();
                        if permissions.mode() & 0o111 != 0 {
                            // File is executable
                            if let Some(name) = file_path.file_name() {
                                let plugin_name = name.to_string_lossy().to_string();
                                debug!("Discovered plugin: {} at {:?}", plugin_name, file_path);
                                self.plugins.insert(plugin_name, file_path);
                            }
                        }
                    }

                    #[cfg(not(unix))]
                    {
                        // On non-Unix systems, assume all files in the plugin directory are plugins
                        if let Some(name) = file_path.file_name() {
                            let plugin_name = name.to_string_lossy().to_string();
                            debug!("Discovered plugin: {} at {:?}", plugin_name, file_path);
                            self.plugins.insert(plugin_name, file_path);
                        }
                    }
                }
            }
        }

        info!("Discovered {} CNI plugins", self.plugins.len());
        Ok(())
    }

    /// Get a plugin by type/name
    pub fn get_plugin(&self, plugin_type: &str) -> Result<CniPlugin, CniError> {
        let plugin_path = self.plugins.get(plugin_type).ok_or_else(|| {
            CniError::new(
                ErrorCode::Generic,
                format!("CNI plugin '{}' not found", plugin_type),
            )
        })?;

        Ok(CniPlugin::new(plugin_type.to_string(), plugin_path.clone()))
    }

    /// Execute a network configuration (chain of plugins)
    pub fn execute_network(
        &self,
        command: CniCommand,
        network: &NetworkConfigList,
        container_id: &str,
        netns: &str,
        ifname: &str,
        args: Option<&str>,
    ) -> Result<CniResult, CniError> {
        debug!(
            "Executing network {} with command {} for container {}",
            network.name, command, container_id
        );

        let mut prev_result: Option<CniResult> = None;
        let cni_path = self.get_cni_path_str();

        for (idx, plugin_config) in network.plugins.iter().enumerate() {
            debug!(
                "Executing plugin {}/{}: {}",
                idx + 1,
                network.plugins.len(),
                plugin_config.plugin_type
            );

            // Get the plugin
            let plugin = self.get_plugin(&plugin_config.plugin_type)?;

            // Build the config for this plugin
            let config = self.build_plugin_config(network, plugin_config, prev_result.as_ref())?;

            // Execute the plugin
            let result = plugin.execute(
                command.clone(),
                container_id,
                netns,
                ifname,
                &config,
                args,
                &cni_path,
            )?;

            prev_result = Some(result);
        }

        prev_result.ok_or_else(|| {
            CniError::new(
                ErrorCode::Generic,
                "No plugins executed in network configuration".to_string(),
            )
        })
    }

    /// Build plugin configuration including previous result if available
    fn build_plugin_config(
        &self,
        network: &NetworkConfigList,
        plugin_config: &PluginConfig,
        prev_result: Option<&CniResult>,
    ) -> Result<String, CniError> {
        let mut config = serde_json::json!({
            "cniVersion": network.cni_version,
            "name": network.name,
            "type": plugin_config.plugin_type,
        });

        // Merge plugin-specific config
        if let serde_json::Value::Object(ref mut map) = config {
            for (k, v) in &plugin_config.config {
                map.insert(k.clone(), v.clone());
            }

            // Add previous result if available (for plugin chaining)
            if let Some(prev) = prev_result {
                map.insert("prevResult".to_string(), serde_json::to_value(prev)?);
            }
        }

        serde_json::to_string(&config).map_err(|e| {
            CniError::new(
                ErrorCode::InvalidNetworkConfig,
                format!("Failed to serialize plugin config: {}", e),
            )
        })
    }

    /// Get CNI_PATH as a string (colon-separated on Unix)
    fn get_cni_path_str(&self) -> String {
        self.plugin_paths
            .iter()
            .map(|p| p.to_string_lossy())
            .collect::<Vec<_>>()
            .join(":")
    }

    /// Check if a plugin is available
    pub fn has_plugin(&self, plugin_type: &str) -> bool {
        self.plugins.contains_key(plugin_type)
    }

    /// List all available plugins
    pub fn list_plugins(&self) -> Vec<String> {
        self.plugins.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_plugin_discovery() {
        let temp_dir = TempDir::new().unwrap();

        // Create a mock plugin executable
        let plugin_path = temp_dir.path().join("test-plugin");
        fs::write(&plugin_path, "#!/bin/sh\necho test").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&plugin_path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&plugin_path, perms).unwrap();
        }

        let mut manager = CniPluginManager::new(vec![temp_dir.path().to_path_buf()]);
        manager.discover_plugins().unwrap();

        assert!(manager.has_plugin("test-plugin"));
        assert_eq!(manager.list_plugins().len(), 1);
    }

    #[test]
    fn test_get_cni_path_str() {
        let manager = CniPluginManager::new(vec![
            PathBuf::from("/opt/cni/bin"),
            PathBuf::from("/usr/lib/cni"),
        ]);

        let path_str = manager.get_cni_path_str();
        assert!(path_str.contains("/opt/cni/bin"));
        assert!(path_str.contains("/usr/lib/cni"));
    }
}
