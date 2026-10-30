//! The container backend the kubelet drives.
//!
//! [`ContainerRuntime`](crate::runtime::ContainerRuntime) holds pod logic
//! (volumes, probes, init containers, networking decisions) and talks to the
//! machine only through [`ContainerBackend`]. Docker and Podman via bollard
//! are one implementation, [`DockerBackend`].

mod docker;
#[cfg(test)]
pub(crate) mod fake;
mod types;
#[cfg(feature = "youki")]
pub mod youki;

pub use docker::DockerBackend;
pub use types::*;

use anyhow::Result;
use async_trait::async_trait;

/// Container operations the kubelet needs from a runtime.
///
/// Containers are addressed by name or ID. Errors keep the backend's own
/// message, since the kubelet matches on a few of them (e.g. name conflicts).
#[async_trait]
pub trait ContainerBackend: Send + Sync {
    /// Whether containers can join network namespaces the kubelet creates
    /// (`network_mode: "ns:<path>"`). CNI networking is only used when true.
    fn joins_kubelet_netns(&self) -> bool {
        false
    }

    /// A human-readable identity of the runtime, e.g. `"Podman Engine"`.
    async fn version_identity(&self) -> Result<String>;

    async fn inspect_image(&self, image: &str) -> Result<ImageInspect>;

    /// Pull an image, yielding progress messages.
    fn pull_image<'a>(&'a self, image: &'a str) -> BackendStream<'a, PullProgress>;

    /// Create a container named `name`; returns its ID.
    async fn create_container(&self, name: &str, config: ContainerConfig) -> Result<String>;

    async fn start_container(&self, name: &str) -> Result<()>;

    async fn stop_container(&self, name: &str, options: StopContainerOptions) -> Result<()>;

    async fn remove_container(&self, name: &str, options: RemoveContainerOptions) -> Result<()>;

    async fn list_containers(
        &self,
        options: ListContainersOptions,
    ) -> Result<Vec<ContainerSummary>>;

    async fn inspect_container(&self, name: &str) -> Result<ContainerInspect>;

    async fn update_container(&self, name: &str, options: UpdateContainerOptions) -> Result<()>;

    fn stats<'a>(
        &'a self,
        name: &'a str,
        options: StatsOptions,
    ) -> BackendStream<'a, ContainerStats>;

    /// Create an exec instance in a running container; returns its ID.
    async fn create_exec(&self, name: &str, options: CreateExecOptions) -> Result<String>;

    async fn start_exec(&self, exec_id: &str) -> Result<StartExecResults>;

    async fn inspect_exec(&self, exec_id: &str) -> Result<ExecInspect>;

    fn logs<'a>(&'a self, name: &'a str, options: LogsOptions) -> BackendStream<'a, LogOutput>;

    /// Copy `path` out of a container as a tar archive.
    fn download_from_container<'a>(
        &'a self,
        name: &'a str,
        path: &'a str,
    ) -> BackendStream<'a, Vec<u8>>;

    /// Names of the backend's named volumes.
    async fn list_volumes(&self) -> Result<Vec<String>>;

    async fn remove_volume(&self, name: &str) -> Result<()>;
}
