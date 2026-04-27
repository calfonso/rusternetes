// Container Runtime Abstraction Layer
//
// This module provides an abstraction over different container runtimes,
// allowing the kubelet to work with Docker/Podman (via bollard) or
// Apple's container tool (via CLI).

mod types;
mod docker;
mod apple;

pub use types::*;
pub use docker::DockerRuntime;
pub use apple::AppleContainerRuntime;

use async_trait::async_trait;
use std::collections::HashMap;

/// ContainerRuntime trait defines the interface for container operations
/// that the kubelet needs to manage pods and containers.
#[async_trait]
pub trait ContainerRuntime: Send + Sync {
    // === Image Operations ===

    /// Check if an image exists locally
    async fn inspect_image(&self, image: &str) -> Result<Option<ImageInfo>, RuntimeError>;

    /// Pull an image from a registry
    async fn pull_image(&self, image: &str) -> Result<(), RuntimeError>;

    // === Container Lifecycle ===

    /// Create a container (but don't start it)
    async fn create_container(
        &self,
        name: &str,
        config: ContainerConfig,
    ) -> Result<String, RuntimeError>;

    /// Start a container by ID
    async fn start_container(&self, id: &str) -> Result<(), RuntimeError>;

    /// Stop a container by ID
    async fn stop_container(&self, id: &str, timeout: u64) -> Result<(), RuntimeError>;

    /// Remove a container by ID
    async fn remove_container(&self, id: &str, force: bool) -> Result<(), RuntimeError>;

    /// Inspect a container by name
    async fn inspect_container(&self, name: &str) -> Result<Option<ContainerInfo>, RuntimeError>;

    /// List containers matching filters
    async fn list_containers(&self, filters: ContainerFilters) -> Result<Vec<ContainerSummary>, RuntimeError>;

    // === Exec Operations ===

    /// Create an exec instance
    async fn create_exec(
        &self,
        container: &str,
        config: ExecConfig,
    ) -> Result<String, RuntimeError>;

    /// Start an exec instance
    async fn start_exec(&self, exec_id: &str) -> Result<ExecOutput, RuntimeError>;

    /// Inspect an exec instance
    async fn inspect_exec(&self, exec_id: &str) -> Result<ExecInfo, RuntimeError>;

    // === Logs ===

    /// Get container logs
    async fn logs(
        &self,
        container: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> Result<LogStream, RuntimeError>;

    // === File Operations ===

    /// Download files from a container
    async fn download_from_container(
        &self,
        container: &str,
        path: &str,
    ) -> Result<Vec<u8>, RuntimeError>;

    // === Volume Operations ===

    /// List volumes
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>, RuntimeError>;

    /// Remove a volume
    async fn remove_volume(&self, name: &str) -> Result<(), RuntimeError>;
}

/// Helper to select the appropriate runtime based on configuration or platform
pub fn create_runtime(runtime_type: RuntimeType) -> Result<Box<dyn ContainerRuntime>, RuntimeError> {
    match runtime_type {
        RuntimeType::Docker => Ok(Box::new(DockerRuntime::new()?)),
        RuntimeType::AppleContainer => Ok(Box::new(AppleContainerRuntime::new()?)),
    }
}

/// Runtime type selection
#[derive(Debug, Clone, Copy)]
pub enum RuntimeType {
    Docker,
    AppleContainer,
}

impl RuntimeType {
    /// Auto-detect the best available runtime
    pub fn auto_detect() -> Self {
        // Check for Apple container first (macOS only)
        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            if Command::new("container")
                .arg("--version")
                .output()
                .is_ok()
            {
                return RuntimeType::AppleContainer;
            }
        }

        // Default to Docker (includes Podman via Docker socket)
        RuntimeType::Docker
    }
}
