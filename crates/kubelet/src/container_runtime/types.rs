// Common types for container runtime abstraction

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use futures::Stream;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("Container runtime error: {0}")]
    Generic(String),

    #[error("Image not found: {0}")]
    ImageNotFound(String),

    #[error("Container not found: {0}")]
    ContainerNotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Command execution failed: {0}")]
    CommandFailed(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Bollard error: {0}")]
    BollardError(String),
}

// === Image Types ===

#[derive(Debug, Clone)]
pub struct ImageInfo {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub size: u64,
    pub config: Option<ImageConfig>,
}

#[derive(Debug, Clone)]
pub struct ImageConfig {
    pub entrypoint: Option<Vec<String>>,
    pub cmd: Option<Vec<String>>,
    pub env: Option<Vec<String>>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
}

// === Container Types ===

#[derive(Debug, Clone, Default)]
pub struct ContainerConfig {
    pub image: String,
    pub hostname: Option<String>,
    pub env: Vec<String>,
    pub cmd: Option<Vec<String>>,
    pub entrypoint: Option<Vec<String>>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
    pub labels: HashMap<String, String>,
    pub host_config: HostConfig,
    pub network_mode: Option<String>,
    pub tty: bool,
    pub attach_stdin: bool,
    pub attach_stdout: bool,
    pub attach_stderr: bool,
    pub open_stdin: bool,
    pub stdin_once: bool,
}

#[derive(Debug, Clone, Default)]
pub struct HostConfig {
    pub binds: Vec<String>,
    pub network_mode: Option<String>,
    pub privileged: bool,
    pub cap_add: Vec<String>,
    pub cap_drop: Vec<String>,
    pub devices: Vec<DeviceMapping>,
    pub ulimits: Vec<Ulimit>,
    pub dns: Vec<String>,
    pub dns_search: Vec<String>,
    pub extra_hosts: Vec<String>,
    pub readonly_rootfs: bool,
}

#[derive(Debug, Clone)]
pub struct DeviceMapping {
    pub path_on_host: String,
    pub path_in_container: String,
    pub cgroup_permissions: String,
}

#[derive(Debug, Clone)]
pub struct Ulimit {
    pub name: String,
    pub soft: i64,
    pub hard: i64,
}

#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub id: String,
    pub name: String,
    pub state: ContainerState,
    pub image: String,
    pub network_settings: Option<NetworkSettings>,
    pub config: Option<ContainerConfig>,
}

#[derive(Debug, Clone)]
pub struct ContainerState {
    pub running: bool,
    pub paused: bool,
    pub restarting: bool,
    pub status: String,
    pub exit_code: Option<i32>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NetworkSettings {
    pub ip_address: Option<String>,
    pub networks: HashMap<String, EndpointSettings>,
}

#[derive(Debug, Clone)]
pub struct EndpointSettings {
    pub ip_address: Option<String>,
    pub network_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ContainerSummary {
    pub id: String,
    pub names: Vec<String>,
    pub image: String,
    pub state: String,
    pub status: String,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct ContainerFilters {
    pub name: Option<Vec<String>>,
    pub label: Option<Vec<String>>,
    pub status: Option<Vec<String>>,
    pub all: bool,
}

// === Exec Types ===

#[derive(Debug, Clone)]
pub struct ExecConfig {
    pub attach_stdout: bool,
    pub attach_stderr: bool,
    pub attach_stdin: bool,
    pub tty: bool,
    pub cmd: Vec<String>,
    pub env: Vec<String>,
    pub user: Option<String>,
    pub working_dir: Option<String>,
    pub privileged: bool,
}

#[derive(Debug)]
pub struct ExecOutput {
    pub output: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ExecInfo {
    pub id: String,
    pub running: bool,
    pub exit_code: Option<i64>,
}

// === Log Types ===

pub type LogStream = Pin<Box<dyn Stream<Item = Result<LogEntry, RuntimeError>> + Send>>;

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub stream: LogStreamType,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum LogStreamType {
    Stdout,
    Stderr,
}

// === Volume Types ===

#[derive(Debug, Clone)]
pub struct VolumeInfo {
    pub name: String,
    pub driver: String,
    pub mountpoint: String,
    pub labels: HashMap<String, String>,
}
