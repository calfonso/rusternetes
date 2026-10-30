//! Kubelet-side types for the [`ContainerBackend`](super::ContainerBackend)
//! trait.
//!
//! They carry only the fields the kubelet reads or sets, so no backend's
//! client types leak into the rest of the crate. Field names follow the
//! Docker Engine API, which is what the kubelet logic was written against.

use std::collections::HashMap;
use std::fmt;

use futures::stream::BoxStream;

/// A boxed stream of backend results.
pub type BackendStream<'a, T> = BoxStream<'a, anyhow::Result<T>>;

/// Options for [`ContainerBackend::list_containers`](super::ContainerBackend::list_containers).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListContainersOptions {
    /// Include stopped containers, not only running ones.
    pub all: bool,
    /// Docker-style filters, e.g. `name` (regex) and `status`.
    pub filters: HashMap<String, Vec<String>>,
}

/// Options for [`ContainerBackend::remove_container`](super::ContainerBackend::remove_container).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RemoveContainerOptions {
    /// Kill the container first if it is running.
    pub force: bool,
}

/// Options for [`ContainerBackend::stop_container`](super::ContainerBackend::stop_container).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct StopContainerOptions {
    /// Seconds to wait after SIGTERM before SIGKILL.
    pub t: i64,
}

/// Options for [`ContainerBackend::logs`](super::ContainerBackend::logs).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogsOptions {
    pub stdout: bool,
    pub stderr: bool,
    /// Number of lines from the end, or `"all"`.
    pub tail: String,
}

/// Options for [`ContainerBackend::stats`](super::ContainerBackend::stats).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct StatsOptions {
    pub stream: bool,
    pub one_shot: bool,
}

/// Options for [`ContainerBackend::create_exec`](super::ContainerBackend::create_exec).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CreateExecOptions {
    pub cmd: Option<Vec<String>>,
    pub attach_stdout: Option<bool>,
    pub attach_stderr: Option<bool>,
    pub attach_stdin: Option<bool>,
    pub tty: Option<bool>,
}

/// In-place resource update for a running container (pod resize).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct UpdateContainerOptions {
    pub cpu_period: Option<i64>,
    pub cpu_quota: Option<i64>,
    pub cpu_shares: Option<i64>,
    pub memory: Option<i64>,
    pub memory_swap: Option<i64>,
}

/// A host port binding for a container port.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PortBinding {
    pub host_ip: Option<String>,
    pub host_port: Option<String>,
}

/// A mount handled by the backend itself (as opposed to a `binds` entry).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Mount {
    pub target: Option<String>,
    pub source: Option<String>,
    pub read_only: Option<bool>,
}

/// Host-side settings of a container to create.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HostConfig {
    /// `"<host path>:<container path>[:ro]"` bind mounts.
    pub binds: Option<Vec<String>>,
    pub mounts: Option<Vec<Mount>>,
    /// Container path to tmpfs options.
    pub tmpfs: Option<HashMap<String, String>>,
    /// `"<port>/<proto>"` to host bindings.
    pub port_bindings: Option<HashMap<String, Option<Vec<PortBinding>>>>,
    pub dns: Option<Vec<String>>,
    pub dns_search: Option<Vec<String>>,
    pub dns_options: Option<Vec<String>>,
    /// A network name, `"host"`, `"none"`, `"container:<name>"` or `"ns:<path>"`.
    pub network_mode: Option<String>,
    /// `"shareable"`, `"container:<name>"` or unset.
    pub ipc_mode: Option<String>,
    /// `"container:<name>"`, `"host"` or unset.
    pub pid_mode: Option<String>,
    /// `"container:<name>"` or unset.
    pub uts_mode: Option<String>,
    pub sysctls: Option<HashMap<String, String>>,
    pub memory: Option<i64>,
    pub cpu_period: Option<i64>,
    pub cpu_quota: Option<i64>,
    pub cpu_shares: Option<i64>,
    pub readonly_rootfs: Option<bool>,
    pub security_opt: Option<Vec<String>>,
    pub cap_add: Option<Vec<String>>,
    pub cap_drop: Option<Vec<String>>,
    pub privileged: Option<bool>,
}

/// Everything needed to create a container.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerConfig {
    pub image: Option<String>,
    pub entrypoint: Option<Vec<String>>,
    pub cmd: Option<Vec<String>>,
    /// `"KEY=value"` entries.
    pub env: Option<Vec<String>>,
    pub working_dir: Option<String>,
    /// `"uid"` or `"uid:gid"`.
    pub user: Option<String>,
    pub hostname: Option<String>,
    /// `"<port>/<proto>"` keys.
    pub exposed_ports: Option<HashMap<String, HashMap<(), ()>>>,
    pub host_config: Option<HostConfig>,
}

/// Lifecycle state of a container as the backend reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerStateStatus {
    Empty,
    Created,
    Running,
    Paused,
    Restarting,
    Removing,
    Exited,
    Dead,
}

/// Runtime state of a container.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerStateInfo {
    pub status: Option<ContainerStateStatus>,
    pub running: Option<bool>,
    pub oom_killed: Option<bool>,
    pub pid: Option<i64>,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
    /// RFC 3339.
    pub started_at: Option<String>,
    /// RFC 3339.
    pub finished_at: Option<String>,
}

/// Address of a container on one network.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EndpointSettings {
    pub ip_address: Option<String>,
}

/// Network settings of a container.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NetworkSettings {
    pub ip_address: Option<String>,
    pub networks: Option<HashMap<String, EndpointSettings>>,
}

/// The part of a container's host config the kubelet reads back.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct InspectHostConfig {
    pub network_mode: Option<String>,
}

/// Image config as stored in an image or container.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ImageConfig {
    pub entrypoint: Option<Vec<String>>,
    pub cmd: Option<Vec<String>>,
    pub env: Option<Vec<String>>,
    pub user: Option<String>,
    pub working_dir: Option<String>,
}

/// Result of [`ContainerBackend::inspect_container`](super::ContainerBackend::inspect_container).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerInspect {
    pub id: Option<String>,
    pub name: Option<String>,
    /// Image ID (e.g. `sha256:...`).
    pub image: Option<String>,
    pub restart_count: Option<i64>,
    pub state: Option<ContainerStateInfo>,
    pub network_settings: Option<NetworkSettings>,
    pub host_config: Option<InspectHostConfig>,
    pub config: Option<ImageConfig>,
}

/// One entry of [`ContainerBackend::list_containers`](super::ContainerBackend::list_containers).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerSummary {
    pub id: Option<String>,
    /// Names with a leading `/`, Docker style.
    pub names: Option<Vec<String>>,
    pub image: Option<String>,
    /// Creation time, seconds since the epoch.
    pub created: Option<i64>,
    /// `"created"`, `"running"`, `"exited"`, ...
    pub state: Option<String>,
    pub host_config: Option<InspectHostConfig>,
}

/// Result of [`ContainerBackend::inspect_image`](super::ContainerBackend::inspect_image).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ImageInspect {
    pub id: Option<String>,
    pub config: Option<ImageConfig>,
}

/// One progress message of an image pull.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PullProgress {
    pub status: Option<String>,
    pub progress: Option<String>,
    pub error: Option<String>,
}

/// Result of [`ContainerBackend::inspect_exec`](super::ContainerBackend::inspect_exec).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecInspect {
    pub running: Option<bool>,
    pub exit_code: Option<i64>,
}

/// One chunk of container or exec output.
#[derive(Debug, Clone, PartialEq)]
pub enum LogOutput {
    StdErr { message: Vec<u8> },
    StdOut { message: Vec<u8> },
    StdIn { message: Vec<u8> },
    Console { message: Vec<u8> },
}

impl fmt::Display for LogOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            LogOutput::StdErr { message }
            | LogOutput::StdOut { message }
            | LogOutput::StdIn { message }
            | LogOutput::Console { message } => message,
        };
        write!(f, "{}", String::from_utf8_lossy(message))
    }
}

/// Result of [`ContainerBackend::start_exec`](super::ContainerBackend::start_exec).
pub enum StartExecResults {
    Attached {
        output: BackendStream<'static, LogOutput>,
    },
    Detached,
}

/// One resource usage sample of a container.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerStats {
    pub memory_usage: Option<u64>,
    /// Page cache to subtract from `memory_usage` (cgroup v1 `cache`,
    /// cgroup v2 `inactive_file`).
    pub memory_cache: Option<u64>,
    pub cpu_total_usage: u64,
    pub system_cpu_usage: Option<u64>,
    pub precpu_total_usage: u64,
    pub precpu_system_cpu_usage: Option<u64>,
    pub online_cpus: Option<u64>,
    /// Per-device byte counters from blkio `io_service_bytes_recursive`.
    pub blkio_service_bytes: Option<Vec<u64>>,
}
