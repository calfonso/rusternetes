//! An in-memory [`ContainerBackend`] for unit tests.

use std::collections::BTreeMap;
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;

use super::types::*;
use super::ContainerBackend;

#[derive(Debug, Clone)]
pub(crate) struct FakeContainer {
    pub id: String,
    pub state: &'static str,
    pub created: i64,
    pub exit_code: Option<i64>,
    pub config: ContainerConfig,
}

#[derive(Default)]
pub(crate) struct FakeState {
    /// By container name.
    pub containers: BTreeMap<String, FakeContainer>,
    /// Every mutating call, e.g. `"stop web_app"`.
    pub calls: Vec<String>,
    next_id: u64,
}

/// Containers live in a map; nothing runs.
pub(crate) struct FakeBackend {
    identity: String,
    pub state: Mutex<FakeState>,
}

impl FakeBackend {
    pub fn new(identity: &str) -> Self {
        Self {
            identity: identity.to_string(),
            state: Mutex::new(FakeState::default()),
        }
    }

    /// Add a container in the given state (`"running"`, `"exited"`, ...).
    pub fn add(&self, name: &str, state: &'static str, created: i64) {
        let mut st = self.state.lock().unwrap();
        st.next_id += 1;
        let id = format!("id{}", st.next_id);
        st.containers.insert(
            name.to_string(),
            FakeContainer {
                id,
                state,
                created,
                exit_code: (state == "exited").then_some(0),
                config: ContainerConfig::default(),
            },
        );
    }

    pub fn names(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .containers
            .keys()
            .cloned()
            .collect()
    }

    pub fn calls(&self) -> Vec<String> {
        self.state.lock().unwrap().calls.clone()
    }

    fn key_of(st: &FakeState, name_or_id: &str) -> Option<String> {
        if st.containers.contains_key(name_or_id) {
            return Some(name_or_id.to_string());
        }
        st.containers
            .iter()
            .find(|(_, c)| c.id == name_or_id)
            .map(|(k, _)| k.clone())
    }

    fn not_found(name: &str) -> anyhow::Error {
        anyhow!("404 No such container: {}", name)
    }
}

/// Docker's `name` filter is a regex on the `/`-prefixed name; tests only
/// use a leading `^` anchor or plain substrings.
fn name_matches(name: &str, pattern: &str) -> bool {
    let full = format!("/{}", name);
    match pattern.strip_prefix('^') {
        Some(prefix) => full.starts_with(prefix),
        None => full.contains(pattern),
    }
}

#[async_trait]
impl ContainerBackend for FakeBackend {
    async fn version_identity(&self) -> Result<String> {
        Ok(self.identity.clone())
    }

    async fn inspect_image(&self, _image: &str) -> Result<ImageInspect> {
        Ok(ImageInspect::default())
    }

    fn pull_image<'a>(&'a self, _image: &'a str) -> BackendStream<'a, PullProgress> {
        futures::stream::empty().boxed()
    }

    async fn create_container(&self, name: &str, config: ContainerConfig) -> Result<String> {
        let mut st = self.state.lock().unwrap();
        if st.containers.contains_key(name) {
            return Err(anyhow!("409 Conflict: name {} already in use", name));
        }
        st.next_id += 1;
        let id = format!("id{}", st.next_id);
        st.calls.push(format!("create {}", name));
        st.containers.insert(
            name.to_string(),
            FakeContainer {
                id: id.clone(),
                state: "created",
                created: chrono::Utc::now().timestamp(),
                exit_code: None,
                config,
            },
        );
        Ok(id)
    }

    async fn start_container(&self, name: &str) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        let key = Self::key_of(&st, name).ok_or_else(|| Self::not_found(name))?;
        st.calls.push(format!("start {}", key));
        st.containers.get_mut(&key).unwrap().state = "running";
        Ok(())
    }

    async fn stop_container(&self, name: &str, _options: StopContainerOptions) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        let key = Self::key_of(&st, name).ok_or_else(|| Self::not_found(name))?;
        st.calls.push(format!("stop {}", key));
        let c = st.containers.get_mut(&key).unwrap();
        c.state = "exited";
        c.exit_code = Some(137);
        Ok(())
    }

    async fn remove_container(&self, name: &str, _options: RemoveContainerOptions) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        let key = Self::key_of(&st, name).ok_or_else(|| Self::not_found(name))?;
        st.calls.push(format!("remove {}", key));
        st.containers.remove(&key);
        Ok(())
    }

    async fn list_containers(
        &self,
        options: ListContainersOptions,
    ) -> Result<Vec<ContainerSummary>> {
        let st = self.state.lock().unwrap();
        let names = options.filters.get("name");
        let statuses = options.filters.get("status");
        Ok(st
            .containers
            .iter()
            .filter(|(_, c)| options.all || c.state == "running")
            .filter(|(n, _)| names.is_none_or(|ps| ps.iter().any(|p| name_matches(n, p))))
            .filter(|(_, c)| statuses.is_none_or(|ss| ss.iter().any(|s| s == c.state)))
            .map(|(n, c)| ContainerSummary {
                id: Some(c.id.clone()),
                names: Some(vec![format!("/{}", n)]),
                image: c.config.image.clone(),
                created: Some(c.created),
                state: Some(c.state.to_string()),
                host_config: Some(InspectHostConfig {
                    network_mode: c
                        .config
                        .host_config
                        .as_ref()
                        .and_then(|h| h.network_mode.clone()),
                }),
            })
            .collect())
    }

    async fn inspect_container(&self, name: &str) -> Result<ContainerInspect> {
        let st = self.state.lock().unwrap();
        let key = Self::key_of(&st, name).ok_or_else(|| Self::not_found(name))?;
        let c = &st.containers[&key];
        let status = match c.state {
            "created" => ContainerStateStatus::Created,
            "running" => ContainerStateStatus::Running,
            "dead" => ContainerStateStatus::Dead,
            _ => ContainerStateStatus::Exited,
        };
        Ok(ContainerInspect {
            id: Some(c.id.clone()),
            name: Some(format!("/{}", key)),
            image: c.config.image.clone(),
            restart_count: Some(0),
            state: Some(ContainerStateInfo {
                status: Some(status),
                running: Some(c.state == "running"),
                exit_code: c.exit_code,
                ..Default::default()
            }),
            network_settings: None,
            host_config: Some(InspectHostConfig {
                network_mode: c
                    .config
                    .host_config
                    .as_ref()
                    .and_then(|h| h.network_mode.clone()),
            }),
            config: None,
        })
    }

    async fn update_container(&self, name: &str, _options: UpdateContainerOptions) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .calls
            .push(format!("update {}", name));
        Ok(())
    }

    fn stats<'a>(
        &'a self,
        _name: &'a str,
        _options: StatsOptions,
    ) -> BackendStream<'a, ContainerStats> {
        futures::stream::empty().boxed()
    }

    async fn create_exec(&self, name: &str, options: CreateExecOptions) -> Result<String> {
        let mut st = self.state.lock().unwrap();
        st.calls.push(format!(
            "exec {} {}",
            name,
            options.cmd.unwrap_or_default().join(" ")
        ));
        Ok("exec1".to_string())
    }

    async fn start_exec(&self, _exec_id: &str) -> Result<StartExecResults> {
        Ok(StartExecResults::Detached)
    }

    async fn inspect_exec(&self, _exec_id: &str) -> Result<ExecInspect> {
        Ok(ExecInspect {
            running: Some(false),
            exit_code: Some(0),
        })
    }

    fn logs<'a>(&'a self, _name: &'a str, _options: LogsOptions) -> BackendStream<'a, LogOutput> {
        futures::stream::empty().boxed()
    }

    fn download_from_container<'a>(
        &'a self,
        _name: &'a str,
        _path: &'a str,
    ) -> BackendStream<'a, Vec<u8>> {
        futures::stream::empty().boxed()
    }

    async fn list_volumes(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn remove_volume(&self, _name: &str) -> Result<()> {
        Ok(())
    }
}
