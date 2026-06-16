//! CRI-backed container runtime — the lifecycle type that replaces the bollard
//! [`crate::runtime::ContainerRuntime`].
//!
//! It is stateless about pod→id mappings: like the upstream kubelet, it
//! discovers sandboxes and containers by querying the runtime with the
//! `io.kubernetes.*` labels [`translate`] stamps on them, so it reconciles
//! correctly across restarts without an in-memory index.
//!
//! Covered: pod bring-up with init containers and host-side volume provisioning
//! (`start_pod` + an attached [`VolumeManager`](crate::volumes::VolumeManager)),
//! container/init status, status/introspection, metrics, gc, in-place resize,
//! single-attempt probes (`probe_container`), and teardown. Remaining: wiring
//! this type into `kubelet.rs` in place of the bollard runtime, the probe
//! threshold/period state machine, and networking — tracked in the migration
//! issue.

use std::path::Path;

use rusternetes_common::resources::pod::{ContainerState, ContainerStatus, Pod};
use rusternetes_cri::{v1, CriClient, CriError};

use super::{probe, status, translate};

/// Errors from the CRI-backed runtime.
#[derive(Debug, thiserror::Error)]
pub enum CriRuntimeError {
    #[error(transparent)]
    Cri(#[from] CriError),

    #[error("preparing pod log directory {dir}: {source}")]
    LogDir {
        dir: String,
        #[source]
        source: std::io::Error,
    },

    #[error("init container {name} failed with exit code {exit_code}")]
    InitContainerFailed { name: String, exit_code: i32 },

    #[error("timed out waiting for init container {name} to complete")]
    InitContainerTimeout { name: String },

    #[error("provisioning volumes for pod {pod}: {source}")]
    Volumes {
        pod: String,
        #[source]
        source: anyhow::Error,
    },
}

type Result<T> = std::result::Result<T, CriRuntimeError>;

/// Status for a spec container the runtime has not created yet.
fn waiting_status(name: &str) -> ContainerStatus {
    ContainerStatus {
        name: name.to_string(),
        ready: false,
        restart_count: 0,
        state: Some(ContainerState::Waiting {
            reason: Some("ContainerCreating".to_string()),
            message: None,
        }),
        last_state: None,
        image: None,
        image_id: None,
        container_id: None,
        started: Some(false),
        allocated_resources: None,
        allocated_resources_status: None,
        resources: None,
        user: None,
        volume_mounts: None,
        stop_signal: None,
    }
}

/// Drives a CRI v1 runtime (containerd → Youki) for the kubelet.
#[derive(Clone)]
pub struct CriContainerRuntime {
    cri: CriClient,
    /// Runtime class passed to `RunPodSandbox` (e.g. `youki`); empty = default.
    runtime_handler: String,
    /// Root under which per-pod log directories are created.
    log_root: String,
    /// Provisions host-side pod volumes. `None` keeps `start_pod` volume-less
    /// (used by socket-only smoke tests); the kubelet always wires one in.
    volumes: Option<crate::volumes::VolumeManager>,
}

impl CriContainerRuntime {
    /// Connect to the CRI runtime at `socket` and use `runtime_handler` for new
    /// sandboxes. `log_root` is the base dir for per-pod container logs.
    pub async fn connect(
        socket: impl AsRef<Path>,
        runtime_handler: impl Into<String>,
        log_root: impl Into<String>,
    ) -> Result<Self> {
        let cri = CriClient::connect(socket).await?;
        Ok(Self {
            cri,
            runtime_handler: runtime_handler.into(),
            log_root: log_root.into(),
            volumes: None,
        })
    }

    /// Attach a [`VolumeManager`](crate::volumes::VolumeManager) so `start_pod`
    /// provisions host-side pod volumes and mounts them into containers.
    #[must_use]
    pub fn with_volumes(mut self, volumes: crate::volumes::VolumeManager) -> Self {
        self.volumes = Some(volumes);
        self
    }

    /// Per-pod log directory: `<log_root>/<namespace>_<name>_<uid>`.
    fn log_dir_for(&self, pod: &Pod) -> String {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        format!(
            "{}/{}_{}_{}",
            self.log_root, ns, pod.metadata.name, pod.metadata.uid
        )
    }

    /// Create and start one container from its translated config, returning its
    /// id. Pulls the image on behalf of the sandbox first.
    async fn create_and_start_container(
        &self,
        cri: &mut CriClient,
        pod: &Pod,
        container: &rusternetes_common::resources::pod::Container,
        sandbox_id: &str,
        sandbox_cfg: &v1::PodSandboxConfig,
        host_paths: &std::collections::HashMap<String, String>,
    ) -> Result<String> {
        let image_ref = cri
            .pull_image(
                &container.image,
                Some(&self.runtime_handler),
                Some(sandbox_cfg.clone()),
            )
            .await?;
        let cfg = translate::container_config(pod, container, &image_ref, host_paths);
        let container_id = cri
            .create_container(sandbox_id, cfg, sandbox_cfg.clone())
            .await?;
        cri.start_container(&container_id).await?;
        Ok(container_id)
    }

    /// Poll a container until it exits, returning its exit code. Errors with
    /// `InitContainerTimeout` if it does not finish within ~30s.
    async fn wait_for_exit(
        &self,
        cri: &mut CriClient,
        container_id: &str,
        name: &str,
    ) -> Result<i32> {
        let exited = v1::ContainerState::ContainerExited as i32;
        for _ in 0..300 {
            let status = cri.container_status(container_id, false).await?;
            if let Some(s) = status.status {
                if s.state == exited {
                    return Ok(s.exit_code);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Err(CriRuntimeError::InitContainerTimeout {
            name: name.to_string(),
        })
    }

    /// Bring a pod up: run the sandbox, run init containers to completion in
    /// order (failing the start on a non-zero exit), then create and start the
    /// app containers.
    ///
    /// Probes and volume provisioning are handled by the kubelet around this
    /// call.
    pub async fn start_pod(&self, pod: &Pod) -> Result<()> {
        let Some(spec) = pod.spec.as_ref() else {
            return Ok(());
        };

        let log_dir = self.log_dir_for(pod);
        std::fs::create_dir_all(&log_dir).map_err(|source| CriRuntimeError::LogDir {
            dir: log_dir.clone(),
            source,
        })?;

        // Provision host-side volumes once for the pod (emptyDir/hostPath/
        // configMap/secret/projected/downwardAPI), yielding the volume-name →
        // host-path map the container translation mounts.
        let host_paths = match self.volumes.as_ref() {
            Some(vm) => {
                vm.create_pod_volumes(pod)
                    .await
                    .map_err(|source| CriRuntimeError::Volumes {
                        pod: pod.metadata.name.clone(),
                        source,
                    })?
            }
            None => std::collections::HashMap::new(),
        };

        let sandbox_cfg = translate::sandbox_config(pod, &log_dir);
        let handler = self.runtime_handler.clone();

        let mut cri = self.cri.clone();
        let sandbox_id = cri.run_pod_sandbox(sandbox_cfg.clone(), &handler).await?;

        // Init containers run sequentially to completion before app containers.
        if let Some(init_containers) = spec.init_containers.as_ref() {
            for container in init_containers {
                let id = self
                    .create_and_start_container(
                        &mut cri,
                        pod,
                        container,
                        &sandbox_id,
                        &sandbox_cfg,
                        &host_paths,
                    )
                    .await?;
                let exit_code = self.wait_for_exit(&mut cri, &id, &container.name).await?;
                if exit_code != 0 {
                    return Err(CriRuntimeError::InitContainerFailed {
                        name: container.name.clone(),
                        exit_code,
                    });
                }
            }
        }

        for container in &spec.containers {
            self.create_and_start_container(
                &mut cri,
                pod,
                container,
                &sandbox_id,
                &sandbox_cfg,
                &host_paths,
            )
            .await?;
        }
        Ok(())
    }

    /// Find the sandbox id for a pod by its name label, if one exists.
    pub async fn sandbox_id_for(&self, pod_name: &str) -> Result<Option<String>> {
        let filter = v1::PodSandboxFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_NAME.to_string(),
                pod_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let sandboxes = cri.list_pod_sandbox(Some(filter)).await?;
        Ok(sandboxes.into_iter().next().map(|s| s.id))
    }

    /// True when at least one of the pod's containers is in the RUNNING state.
    pub async fn is_pod_running(&self, pod: &Pod) -> Result<bool> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_UID.to_string(),
                pod.metadata.uid.clone(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let containers = cri.list_containers(Some(filter)).await?;
        let running = v1::ContainerState::ContainerRunning as i32;
        Ok(containers.iter().any(|c| c.state == running))
    }

    /// Names of all pods with a READY sandbox on this runtime.
    pub async fn list_running_pods(&self) -> Result<Vec<String>> {
        let filter = v1::PodSandboxFilter {
            state: Some(v1::PodSandboxStateValue {
                state: v1::PodSandboxState::SandboxReady as i32,
            }),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let sandboxes = cri.list_pod_sandbox(Some(filter)).await?;
        Ok(sandboxes
            .into_iter()
            .filter_map(|s| s.metadata.map(|m| m.name))
            .collect())
    }

    /// Statuses for a given set of container names, in order. Names the runtime
    /// has not created yet are reported as `Waiting / ContainerCreating` so the
    /// result always has one entry per name.
    async fn statuses_for(&self, pod: &Pod, names: &[String]) -> Result<Vec<ContainerStatus>> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_UID.to_string(),
                pod.metadata.uid.clone(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let containers = cri.list_containers(Some(filter)).await?;

        // Index runtime containers by their kubernetes container name.
        let mut by_name: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for c in &containers {
            if let Some(name) = c
                .labels
                .get(translate::labels::CONTAINER_NAME)
                .or_else(|| c.metadata.as_ref().map(|m| &m.name))
            {
                by_name.insert(name.clone(), c.id.clone());
            }
        }

        let mut out = Vec::with_capacity(names.len());
        for name in names {
            match by_name.get(name) {
                Some(id) => {
                    let full = cri.container_status(id, false).await?;
                    let mapped = full
                        .status
                        .as_ref()
                        .map(status::map_container_status)
                        .unwrap_or_else(|| waiting_status(name));
                    out.push(mapped);
                }
                None => out.push(waiting_status(name)),
            }
        }
        Ok(out)
    }

    /// Statuses for every container in `pod.spec.containers`, in spec order.
    pub async fn get_container_statuses(&self, pod: &Pod) -> Result<Vec<ContainerStatus>> {
        let Some(spec) = pod.spec.as_ref() else {
            return Ok(Vec::new());
        };
        let names: Vec<String> = spec.containers.iter().map(|c| c.name.clone()).collect();
        self.statuses_for(pod, &names).await
    }

    /// Statuses for the pod's init containers, in spec order. `None` if the pod
    /// has no init containers. Errors are swallowed to `None` to match the
    /// bollard runtime's contract.
    pub async fn get_init_container_statuses(&self, pod: &Pod) -> Option<Vec<ContainerStatus>> {
        let init = pod.spec.as_ref()?.init_containers.as_ref()?;
        if init.is_empty() {
            return None;
        }
        let names: Vec<String> = init.iter().map(|c| c.name.clone()).collect();
        self.statuses_for(pod, &names).await.ok()
    }

    /// Statuses for the pod's ephemeral containers, in spec order. `None` if the
    /// pod has none.
    pub async fn get_ephemeral_container_statuses(
        &self,
        pod: &Pod,
    ) -> Option<Vec<ContainerStatus>> {
        let ecs = pod.spec.as_ref()?.ephemeral_containers.as_ref()?;
        if ecs.is_empty() {
            return None;
        }
        let names: Vec<String> = ecs.iter().map(|c| c.name.clone()).collect();
        self.statuses_for(pod, &names).await.ok()
    }

    /// Decide init-container progress: `(all_init_done, next_index_to_start,
    /// should_retry)`. Mirrors the bollard runtime — observes each init
    /// container's runtime state and defers to the shared
    /// [`decide_next_init_action`](crate::runtime::decide_next_init_action).
    pub async fn compute_init_container_actions(&self, pod: &Pod) -> (bool, Option<usize>, bool) {
        let init_containers = match pod.spec.as_ref().and_then(|s| s.init_containers.as_ref()) {
            Some(ics) if !ics.is_empty() => ics,
            _ => return (true, None, false), // no init containers = all done
        };

        // Index runtime containers for this pod by kubernetes container name.
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_UID.to_string(),
                pod.metadata.uid.clone(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let containers = cri.list_containers(Some(filter)).await.unwrap_or_default();
        let running = v1::ContainerState::ContainerRunning as i32;
        let exited = v1::ContainerState::ContainerExited as i32;

        let mut observed = Vec::with_capacity(init_containers.len());
        for ic in init_containers {
            let id = containers.iter().find(|c| {
                c.labels
                    .get(translate::labels::CONTAINER_NAME)
                    .map(|n| n == &ic.name)
                    .unwrap_or(false)
            });
            let obs = match id {
                None => crate::runtime::InitContainerObserved::NotStarted,
                Some(c) if c.state == running => crate::runtime::InitContainerObserved::Running,
                Some(c) if c.state == exited => {
                    // Fetch the exit code from full status.
                    let code = cri
                        .container_status(&c.id, false)
                        .await
                        .ok()
                        .and_then(|s| s.status)
                        .map(|s| s.exit_code)
                        .unwrap_or(-1);
                    crate::runtime::InitContainerObserved::Exited(code)
                }
                Some(_) => crate::runtime::InitContainerObserved::NotStarted,
            };
            observed.push(obs);
        }

        let action = crate::runtime::decide_next_init_action(pod, &observed);
        (action.all_init_done, action.next_index, action.should_retry)
    }

    /// Names of all pods that have a sandbox on this runtime, regardless of
    /// state (ready or not).
    pub async fn list_all_pods(&self) -> Result<Vec<String>> {
        let mut cri = self.cri.clone();
        let sandboxes = cri.list_pod_sandbox(None).await?;
        Ok(sandboxes
            .into_iter()
            .filter_map(|s| s.metadata.map(|m| m.name))
            .collect())
    }

    /// The pod's primary IP, read from its sandbox network status. `None` if the
    /// pod has no sandbox or no IP yet (e.g. CNI not done). Host-network pods
    /// report the node IP.
    pub async fn get_pod_ip(&self, pod_name: &str) -> Result<Option<String>> {
        let Some(sandbox_id) = self.sandbox_id_for(pod_name).await? else {
            return Ok(None);
        };
        let mut cri = self.cri.clone();
        let status = cri.pod_sandbox_status(&sandbox_id, false).await?;
        Ok(status
            .status
            .and_then(|s| s.network)
            .map(|n| n.ip)
            .filter(|ip| !ip.is_empty()))
    }

    /// Whether any container named `container_name` is currently RUNNING. CRI
    /// container names are per-pod, so this matches across all pods by label.
    pub async fn is_container_running(&self, container_name: &str) -> Result<bool> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::CONTAINER_NAME.to_string(),
                container_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let containers = cri.list_containers(Some(filter)).await?;
        let running = v1::ContainerState::ContainerRunning as i32;
        Ok(containers.iter().any(|c| c.state == running))
    }

    /// Execute a single probe attempt against a container, returning whether it
    /// succeeded. The kubelet drives the surrounding state machine (delay,
    /// period, thresholds); this performs one attempt:
    ///
    /// - `exec`: run the command via CRI `ExecSync`; success = exit 0.
    /// - `tcpSocket`: TCP connect to the (host or pod IP):port from the node.
    /// - `httpGet`: HTTP(S) GET to the (host or pod IP):port/path; success =
    ///   status < 400 (k8s treats 2xx/3xx as healthy).
    /// - `grpc`: not yet supported — returns `false`.
    ///
    /// A probe with no action configured is treated as success (nothing to check).
    pub async fn probe_container(
        &self,
        pod: &Pod,
        container: &rusternetes_common::resources::pod::Container,
        probe: &rusternetes_common::resources::pod::Probe,
    ) -> Result<bool> {
        let timeout =
            std::time::Duration::from_secs(probe.timeout_seconds.unwrap_or(1).max(1) as u64);

        if let Some(exec) = probe.exec.as_ref() {
            if exec.command.is_empty() {
                return Ok(true);
            }
            // Scope by pod uid + container name so a sibling pod with the same
            // container name is never probed by mistake.
            let filter = v1::ContainerFilter {
                label_selector: std::collections::HashMap::from([
                    (
                        translate::labels::POD_UID.to_string(),
                        pod.metadata.uid.clone(),
                    ),
                    (
                        translate::labels::CONTAINER_NAME.to_string(),
                        container.name.clone(),
                    ),
                ]),
                ..Default::default()
            };
            let mut cri = self.cri.clone();
            let Some(c) = cri.list_containers(Some(filter)).await?.into_iter().next() else {
                return Ok(false);
            };
            let cmd: Vec<&str> = exec.command.iter().map(String::as_str).collect();
            let resp = cri
                .exec_sync(
                    &c.id,
                    &cmd,
                    probe.timeout_seconds.unwrap_or(1).max(1) as i64,
                )
                .await?;
            return Ok(resp.exit_code == 0);
        }

        if let Some(tcp) = probe.tcp_socket.as_ref() {
            let Some(port) = probe::resolve_port(container, &tcp.port) else {
                return Ok(false);
            };
            let host = match tcp.host.clone() {
                Some(h) => h,
                None => match self.get_pod_ip(&pod.metadata.name).await? {
                    Some(ip) => ip,
                    None => return Ok(false),
                },
            };
            let addr = format!("{host}:{port}");
            let ok = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr))
                .await
                .map(|r| r.is_ok())
                .unwrap_or(false);
            return Ok(ok);
        }

        if let Some(http) = probe.http_get.as_ref() {
            let Some(port) = probe::resolve_port(container, &http.port) else {
                return Ok(false);
            };
            let host = match http.host.clone() {
                Some(h) => h,
                None => match self.get_pod_ip(&pod.metadata.name).await? {
                    Some(ip) => ip,
                    None => return Ok(false),
                },
            };
            let scheme = if http
                .scheme
                .as_deref()
                .unwrap_or("HTTP")
                .eq_ignore_ascii_case("HTTPS")
            {
                "https"
            } else {
                "http"
            };
            let path = http.path.as_deref().unwrap_or("/");
            let url = format!("{scheme}://{host}:{port}{path}");

            let client = match reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .timeout(timeout)
                .build()
            {
                Ok(c) => c,
                Err(_) => return Ok(false),
            };
            let mut req = client.get(&url);
            if let Some(headers) = http.http_headers.as_ref() {
                for h in headers {
                    req = req.header(&h.name, &h.value);
                }
            }
            let ok = match req.send().await {
                Ok(resp) => resp.status().as_u16() < 400,
                Err(_) => false,
            };
            return Ok(ok);
        }

        // No probe action configured.
        Ok(true)
    }

    /// Exit code of the (most recent) container named `container_name`, or 0 if
    /// no such container is known to the runtime.
    pub async fn get_container_exit_code(&self, container_name: &str) -> Result<i64> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::CONTAINER_NAME.to_string(),
                container_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let Some(container) = cri.list_containers(Some(filter)).await?.into_iter().next() else {
            return Ok(0);
        };
        let status = cri.container_status(&container.id, false).await?;
        Ok(status.status.map(|s| i64::from(s.exit_code)).unwrap_or(0))
    }

    /// Remove every exited container named `container_name` so a restart can
    /// recreate it. Running containers are left alone.
    pub async fn remove_terminated_container(&self, container_name: &str) -> Result<()> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::CONTAINER_NAME.to_string(),
                container_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let exited = v1::ContainerState::ContainerExited as i32;
        for c in cri.list_containers(Some(filter)).await? {
            if c.state == exited {
                cri.remove_container(&c.id).await?;
            }
        }
        Ok(())
    }

    /// Update a container's cgroup limits in place (in-place pod resize). Matches
    /// the container by name; no-op if it is not found.
    pub async fn update_container_resources(
        &self,
        container_name: &str,
        cpu_period: Option<i64>,
        cpu_quota: Option<i64>,
        cpu_shares: Option<i64>,
        memory: Option<i64>,
    ) -> Result<()> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::CONTAINER_NAME.to_string(),
                container_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let Some(container) = cri.list_containers(Some(filter)).await?.into_iter().next() else {
            return Ok(());
        };
        let resources = v1::LinuxContainerResources {
            cpu_period: cpu_period.unwrap_or(0),
            cpu_quota: cpu_quota.unwrap_or(0),
            cpu_shares: cpu_shares.unwrap_or(0),
            memory_limit_in_bytes: memory.unwrap_or(0),
            ..Default::default()
        };
        cri.update_container_resources(&container.id, resources)
            .await?;
        Ok(())
    }

    /// Per-container CPU/memory usage for the given pods, keyed by pod name.
    /// Each entry is `(container_name, cpu_nano_cores, working_set_bytes)`.
    pub async fn collect_pod_metrics(
        &self,
        pod_names: &[String],
    ) -> std::collections::HashMap<String, Vec<(String, u64, u64)>> {
        let mut out: std::collections::HashMap<String, Vec<(String, u64, u64)>> =
            std::collections::HashMap::new();
        let mut cri = self.cri.clone();

        for pod_name in pod_names {
            let Ok(Some(sandbox_id)) = self.sandbox_id_for(pod_name).await else {
                continue;
            };
            let filter = v1::ContainerStatsFilter {
                pod_sandbox_id: sandbox_id,
                ..Default::default()
            };
            let Ok(stats) = cri.list_container_stats(Some(filter)).await else {
                continue;
            };
            let mut per_pod = Vec::new();
            for s in stats {
                let name = s
                    .attributes
                    .as_ref()
                    .and_then(|a| a.metadata.as_ref().map(|m| m.name.clone()))
                    .unwrap_or_default();
                let cpu = s
                    .cpu
                    .and_then(|c| c.usage_nano_cores.map(|v| v.value))
                    .unwrap_or(0);
                let mem = s
                    .memory
                    .and_then(|m| m.working_set_bytes.map(|v| v.value))
                    .unwrap_or(0);
                per_pod.push((name, cpu, mem));
            }
            out.insert(pod_name.clone(), per_pod);
        }
        out
    }

    /// Whether any container named `container_name` exists on the runtime,
    /// regardless of state.
    pub async fn container_exists(&self, container_name: &str) -> bool {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::CONTAINER_NAME.to_string(),
                container_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        cri.list_containers(Some(filter))
            .await
            .map(|c| !c.is_empty())
            .unwrap_or(false)
    }

    /// Whether any of the pod's containers have exited.
    pub async fn has_terminated_containers(&self, pod: &Pod) -> bool {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_UID.to_string(),
                pod.metadata.uid.clone(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let exited = v1::ContainerState::ContainerExited as i32;
        cri.list_containers(Some(filter))
            .await
            .map(|cs| cs.iter().any(|c| c.state == exited))
            .unwrap_or(false)
    }

    /// Total CPU (nano-cores) and memory (working-set bytes) across the given
    /// pods — the node-level rollup the kubelet reports.
    pub async fn collect_node_metrics(&self, pod_names: &[String]) -> (u64, u64) {
        let per_pod = self.collect_pod_metrics(pod_names).await;
        let mut cpu = 0u64;
        let mut mem = 0u64;
        for containers in per_pod.values() {
            for (_, c, m) in containers {
                cpu += *c;
                mem += *m;
            }
        }
        (cpu, mem)
    }

    /// Remove sandboxes (and their containers) for pods that are no longer
    /// desired — any sandbox whose pod name is not in `existing_pods`. Returns
    /// the number of sandboxes removed.
    pub async fn garbage_collect_containers(
        &self,
        existing_pods: &std::collections::HashSet<String>,
    ) -> Result<usize> {
        let mut cri = self.cri.clone();
        let sandboxes = cri.list_pod_sandbox(None).await?;
        let mut removed = 0;
        for sb in sandboxes {
            let name = sb.metadata.as_ref().map(|m| m.name.as_str()).unwrap_or("");
            if name.is_empty() || existing_pods.contains(name) {
                continue;
            }
            let _ = cri.stop_pod_sandbox(&sb.id).await;
            if cri.remove_pod_sandbox(&sb.id).await.is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Age of a pod's sandbox (time since it was created). Zero if the pod has
    /// no sandbox or the runtime reports no creation time.
    pub async fn get_container_age(&self, pod_name: &str) -> Result<std::time::Duration> {
        let Some(sandbox_id) = self.sandbox_id_for(pod_name).await? else {
            return Ok(std::time::Duration::ZERO);
        };
        let mut cri = self.cri.clone();
        let status = cri.pod_sandbox_status(&sandbox_id, false).await?;
        let created_at = status.status.map(|s| s.created_at).unwrap_or(0);
        if created_at <= 0 {
            return Ok(std::time::Duration::ZERO);
        }
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let age_nanos = (now - created_at).max(0) as u64;
        Ok(std::time::Duration::from_nanos(age_nanos))
    }

    /// Gracefully stop a pod: stop each of its containers with `grace_period_seconds`,
    /// then stop and remove the sandbox. No-op if the pod has no sandbox.
    pub async fn stop_pod_for(&self, pod: &Pod, grace_period_seconds: i64) -> Result<()> {
        let Some(sandbox_id) = self.sandbox_id_for(&pod.metadata.name).await? else {
            return Ok(());
        };
        let mut cri = self.cri.clone();

        let filter = v1::ContainerFilter {
            pod_sandbox_id: sandbox_id.clone(),
            ..Default::default()
        };
        for c in cri.list_containers(Some(filter)).await? {
            // Best-effort: keep tearing down even if one container stop fails.
            let _ = cri.stop_container(&c.id, grace_period_seconds).await;
        }

        cri.stop_pod_sandbox(&sandbox_id).await?;
        cri.remove_pod_sandbox(&sandbox_id).await?;
        Ok(())
    }

    /// Gracefully stop a pod by name: stop its containers with the grace period,
    /// then stop and remove the sandbox. No-op if the pod has no sandbox.
    pub async fn stop_pod_with_grace_period(
        &self,
        pod_name: &str,
        grace_period_seconds: i64,
    ) -> Result<()> {
        let Some(sandbox_id) = self.sandbox_id_for(pod_name).await? else {
            return Ok(());
        };
        let mut cri = self.cri.clone();
        let filter = v1::ContainerFilter {
            pod_sandbox_id: sandbox_id.clone(),
            ..Default::default()
        };
        for c in cri.list_containers(Some(filter)).await? {
            let _ = cri.stop_container(&c.id, grace_period_seconds).await;
        }
        cri.stop_pod_sandbox(&sandbox_id).await?;
        cri.remove_pod_sandbox(&sandbox_id).await?;
        Ok(())
    }

    // ---- Volume management (delegates to the attached VolumeManager) -------

    /// Base directory under which pod volume trees are created. Empty when no
    /// [`VolumeManager`](crate::volumes::VolumeManager) is attached.
    pub fn volumes_base_path(&self) -> &str {
        self.volumes
            .as_ref()
            .map(|v| v.volumes_base_path.as_str())
            .unwrap_or("")
    }

    /// Refresh a pod's volumes (re-render configMap/secret/projected content).
    /// No-op when no VolumeManager is attached.
    pub async fn refresh_volumes(&self, pod: &Pod) -> Result<()> {
        if let Some(vm) = self.volumes.as_ref() {
            vm.refresh_volumes(pod)
                .await
                .map_err(|source| CriRuntimeError::Volumes {
                    pod: pod.metadata.name.clone(),
                    source,
                })?;
        }
        Ok(())
    }

    /// Re-provision a pod's volumes from storage (e.g. after a kubelet restart).
    /// No-op when no VolumeManager is attached.
    pub async fn resync_volumes<S: rusternetes_storage::Storage>(
        &self,
        pod: &Pod,
        storage: &S,
    ) -> Result<()> {
        if let Some(vm) = self.volumes.as_ref() {
            vm.resync_volumes(pod, storage)
                .await
                .map_err(|source| CriRuntimeError::Volumes {
                    pod: pod.metadata.name.clone(),
                    source,
                })?;
        }
        Ok(())
    }

    /// Stop and remove every sandbox for a pod name; removing a sandbox tears
    /// down its containers. Removes all matches (a pod can have a stale sandbox
    /// alongside a new one), so it is also a no-op when none exist.
    pub async fn stop_and_remove_pod(&self, pod_name: &str) -> Result<()> {
        let filter = v1::PodSandboxFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_NAME.to_string(),
                pod_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        for sb in cri.list_pod_sandbox(Some(filter)).await? {
            let _ = cri.stop_pod_sandbox(&sb.id).await;
            cri.remove_pod_sandbox(&sb.id).await?;
        }
        Ok(())
    }
}
