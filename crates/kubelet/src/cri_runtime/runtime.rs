//! CRI-backed container runtime — the lifecycle type that replaces the bollard
//! runtime.
//!
//! It is stateless about pod→id mappings: like the upstream kubelet, it
//! discovers sandboxes and containers by querying the runtime with the
//! `io.kubernetes.*` labels [`translate`] stamps on them, so it reconciles
//! correctly across restarts without an in-memory index.
//!
//! Covered: pod bring-up with init containers and host-side volume provisioning
//! (`start_pod` + an attached [`VolumeManager`](crate::volumes::VolumeManager)),
//! container/init/ephemeral status, status/introspection, metrics, gc, in-place
//! resize, single-attempt probes plus the full liveness/startup threshold state
//! machine (`check_liveness`), init-action decisions, image pulls with policy,
//! per-container start, lifecycle events, and teardown. This is now the
//! kubelet's runtime backend (the bollard `ContainerRuntime` handle was
//! replaced). Remaining: pod DNS config, unsafe-sysctl plumbing, and moving pod
//! networking fully to containerd-CNI — tracked in the migration issue.

use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusternetes_common::resources::pod::{Container, ContainerState, ContainerStatus, Pod, Probe};
use rusternetes_common::resources::{ConfigMap, Secret, Service};
use rusternetes_cri::{v1, CriClient, CriError};
use rusternetes_storage::{build_prefix, Storage};
use tracing::{debug, warn};

use super::{probe, status, translate};

/// Per-probe threshold-tracking state for the liveness/startup probe state
/// machine. Mirrors the bollard runtime's `ProbeState`.
#[derive(Default)]
struct ProbeState {
    consecutive_failures: i32,
    consecutive_successes: i32,
    /// Last time this probe was evaluated; honors `periodSeconds` so counters
    /// advance at most once per period regardless of reconcile-loop frequency.
    last_eval: Option<chrono::DateTime<Utc>>,
    /// Level-triggered readiness latch (readiness probes only): held between
    /// evaluations, flipped only when a success/failure threshold is crossed.
    /// Starts `false` — a container is not ready until its readiness probe
    /// first passes.
    ready: bool,
}

/// Advance a readiness probe's threshold state machine by one observation and
/// return the resulting readiness. Becomes ready after `success_threshold`
/// consecutive successes; not-ready after `failure_threshold` consecutive
/// failures; otherwise holds the prior readiness (level-triggered, matching the
/// upstream prober worker).
fn readiness_after_observation(
    st: &mut ProbeState,
    healthy: bool,
    success_threshold: i32,
    failure_threshold: i32,
) -> bool {
    if healthy {
        st.consecutive_successes += 1;
        st.consecutive_failures = 0;
        if st.consecutive_successes >= success_threshold.max(1) {
            st.ready = true;
        }
    } else {
        st.consecutive_failures += 1;
        st.consecutive_successes = 0;
        if st.consecutive_failures >= failure_threshold.max(1) {
            st.ready = false;
        }
    }
    st.ready
}

/// Errors from the CRI-backed runtime.
#[derive(Debug, thiserror::Error)]
pub enum CriRuntimeError {
    #[error(transparent)]
    Cri(#[from] CriError),

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

/// Find a container's spec by name across the pod's regular and init containers
/// (used to resolve its `terminationMessagePolicy`). Ephemeral containers carry
/// a distinct type and always use the default `File` policy here.
fn find_container<'a>(pod: &'a Pod, name: &str) -> Option<&'a Container> {
    let spec = pod.spec.as_ref()?;
    spec.containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
        .find(|c| c.name == name)
}

/// Read the last [`status::MAX_TERMINATION_MESSAGE_LENGTH`] bytes of a container
/// log file for the `FallbackToLogsOnError` path. `None` if the log is absent or
/// empty. Returns the raw log tail; the CRI log line framing is left intact
/// (this is only the error-fallback source, not the primary message).
fn read_log_tail(log_path: &str) -> Option<String> {
    let data = std::fs::read(log_path).ok()?;
    if data.is_empty() {
        return None;
    }
    let start = data
        .len()
        .saturating_sub(status::MAX_TERMINATION_MESSAGE_LENGTH);
    let tail = String::from_utf8_lossy(&data[start..]).into_owned();
    if tail.is_empty() {
        None
    } else {
        Some(tail)
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
    /// Threshold-tracking state for liveness/startup probes, keyed
    /// `<pod>/<container>/<liveness|startup>`. `Arc` so clones share it.
    probe_states: Arc<Mutex<std::collections::HashMap<String, ProbeState>>>,
    /// Records Kubernetes lifecycle events. `None` disables event emission.
    event_recorder: Option<rusternetes_storage::EventRecorder<rusternetes_storage::StorageBackend>>,
    /// ClusterIP + port of the default `kubernetes` Service, injected into every
    /// container as KUBERNETES_SERVICE_HOST/PORT so in-cluster clients (kube-rs,
    /// client-go) can reach the api-server.
    service_host: String,
    service_port: String,
    /// Cluster DNS server IPs (`--cluster-dns`) and domain (`--cluster-domain`)
    /// used to build the pod-sandbox `DnsConfig` for `ClusterFirst*` pods. Empty
    /// `cluster_dns` => pods inherit the node's resolv.conf.
    cluster_dns: Vec<String>,
    cluster_domain: String,
    /// Node allocatable (`cpu`/`memory`/`ephemeral-storage`/`hugepages-*`),
    /// used to default unset resourceFieldRef LIMITS into downward-API env
    /// (upstream `MergeContainerResourceLimits`). Empty leaves such vars unset.
    node_allocatable: std::collections::HashMap<String, String>,
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
            probe_states: Arc::new(Mutex::new(std::collections::HashMap::new())),
            event_recorder: None,
            service_host: "10.96.0.1".to_string(),
            service_port: "443".to_string(),
            cluster_dns: Vec::new(),
            cluster_domain: "cluster.local".to_string(),
            node_allocatable: std::collections::HashMap::new(),
        })
    }

    /// Query the CRI runtime's identity (Version RPC) and format it as the
    /// k8s `containerRuntimeVersion` string `<runtime_name>://<runtime_version>`
    /// (e.g. `containerd-rs://0.1.2`). Reflects whichever runtime the configured
    /// `CONTAINER_RUNTIME_ENDPOINT` actually points at, rather than a hardcoded
    /// literal.
    pub async fn runtime_version_string(&self) -> Result<String> {
        let mut cri = self.cri.clone();
        let v = cri.version().await?;
        Ok(format_runtime_version(&v))
    }

    /// Set the `kubernetes` Service host:port injected as KUBERNETES_SERVICE_*
    /// env into pods (defaults to 10.96.0.1:443).
    #[must_use]
    pub fn with_service_host(mut self, host: impl Into<String>, port: impl Into<String>) -> Self {
        self.service_host = host.into();
        self.service_port = port.into();
        self
    }

    /// Set the node allocatable used to default unset resourceFieldRef LIMITS
    /// into downward-API env (e.g. `limits.cpu` → node allocatable cpu).
    #[must_use]
    pub fn with_node_allocatable(
        mut self,
        allocatable: std::collections::HashMap<String, String>,
    ) -> Self {
        self.node_allocatable = allocatable;
        self
    }

    /// Set the cluster DNS servers + domain used to populate `ClusterFirst*`
    /// pods' sandbox `DnsConfig`. `cluster_dns` is the raw `--cluster-dns` value
    /// (comma- or whitespace-separated IPs); empty leaves pods on node DNS. An
    /// empty `cluster_domain` falls back to `cluster.local`.
    #[must_use]
    pub fn with_cluster_dns(mut self, cluster_dns: &str, cluster_domain: &str) -> Self {
        self.cluster_dns = cluster_dns
            .split([',', ' '])
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        if !cluster_domain.is_empty() {
            self.cluster_domain = cluster_domain.to_string();
        }
        self
    }

    /// Attach a [`VolumeManager`](crate::volumes::VolumeManager) so `start_pod`
    /// provisions host-side pod volumes and mounts them into containers.
    #[must_use]
    pub fn with_volumes(mut self, volumes: crate::volumes::VolumeManager) -> Self {
        self.volumes = Some(volumes);
        self
    }

    /// Emit Kubernetes lifecycle events through `storage`.
    #[must_use]
    pub fn with_event_recorder(
        mut self,
        storage: Arc<rusternetes_storage::StorageBackend>,
    ) -> Self {
        self.event_recorder = Some(rusternetes_storage::EventRecorder::new(storage));
        self
    }

    /// Emit a pod/container lifecycle event. No-op without an event recorder.
    pub async fn emit_event(
        &self,
        pod: &Pod,
        container_name: Option<&str>,
        reason: &str,
        event_type: rusternetes_common::resources::EventType,
        message: &str,
    ) {
        let Some(recorder) = &self.event_recorder else {
            return;
        };
        let _ = crate::events::emit_lifecycle_event(
            recorder,
            pod,
            container_name,
            reason,
            event_type,
            message,
        )
        .await;
    }

    /// Provision a pod's host-side volumes, returning the volume-name →
    /// host-path map. Empty when no VolumeManager is attached.
    pub async fn create_pod_volumes(
        &self,
        pod: &Pod,
    ) -> Result<std::collections::HashMap<String, String>> {
        match self.volumes.as_ref() {
            Some(vm) => {
                vm.create_pod_volumes(pod)
                    .await
                    .map_err(|source| CriRuntimeError::Volumes {
                        pod: pod.metadata.name.clone(),
                        source,
                    })
            }
            None => Ok(std::collections::HashMap::new()),
        }
    }

    /// Ensure an image is present, honoring the pull policy. `Never` requires it
    /// to already exist; `Always` always pulls; `IfNotPresent` (default) pulls
    /// only when absent.
    pub async fn ensure_image(
        &self,
        image: &str,
        pull_policy: Option<&str>,
        _event_ctx: Option<(&Pod, &str)>,
    ) -> anyhow::Result<()> {
        let policy = pull_policy.unwrap_or("IfNotPresent");
        let mut cri = self.cri.clone();

        if policy != "Always" {
            let present = cri.image_status(image, false).await?.is_some();
            if present {
                return Ok(());
            }
            if policy == "Never" {
                // Absent and pulling disallowed. The typed error lets the
                // kubelet report `ErrImageNeverPull` via reason_from_anyhow.
                return Err(anyhow::Error::new(crate::lifecycle::ImageNeverPullError {
                    image: image.to_string(),
                }));
            }
        }
        cri.pull_image(image, Some(&self.runtime_handler), None)
            .await?;
        Ok(())
    }

    /// Create and start a single container in the pod's existing sandbox. Used
    /// by the kubelet for init-container sequencing, restarts, and ephemeral
    /// containers. `netns_path`/`hosts_file_path`/`pod_ip` are bollard-era
    /// networking hints that containerd-CNI handles itself — ignored here.
    pub async fn start_container(
        &self,
        pod: &Pod,
        container: &Container,
        volume_paths: &std::collections::HashMap<String, String>,
        _netns_path: Option<&str>,
        _hosts_file_path: Option<&str>,
        _pod_ip: Option<&str>,
    ) -> anyhow::Result<()> {
        let sandbox_id = self
            .sandbox_id_for(&pod.metadata.name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no sandbox for pod {}", pod.metadata.name))?;
        let log_dir = self.log_dir_for(pod);
        let mut sandbox_cfg = translate::sandbox_config(pod, &log_dir);
        sandbox_cfg.dns_config =
            translate::dns_config(pod, &self.cluster_dns, &self.cluster_domain);
        let mut cri = self.cri.clone();
        self.create_and_start_container(
            &mut cri,
            pod,
            container,
            &sandbox_id,
            &sandbox_cfg,
            volume_paths,
        )
        .await?;
        Ok(())
    }

    /// Per-pod log directory: `<log_root>/<namespace>_<name>_<uid>`.
    fn log_dir_for(&self, pod: &Pod) -> String {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        format!(
            "{}/{}_{}_{}",
            self.log_root, ns, pod.metadata.name, pod.metadata.uid
        )
    }

    /// Host path of a container's termination-message file. Lives next to the
    /// container logs in the per-pod dir; the same path is bind-mounted into the
    /// container at its `terminationMessagePath` on create and read back on exit
    /// (#442). Derivable identically on the create and status paths, so no extra
    /// plumbing is needed.
    fn termination_log_host_path(&self, pod: &Pod, container_name: &str) -> String {
        format!(
            "{}/{}-termination-log",
            self.log_dir_for(pod),
            container_name
        )
    }

    /// Apply upstream `getTerminationMessage` semantics to a just-mapped
    /// container status: for a terminated container, read its termination-message
    /// file (and, for `FallbackToLogsOnError`, the log tail) and override the
    /// runtime-supplied message. No-op for non-terminated states. #442.
    fn apply_termination_message(&self, pod: &Pod, name: &str, status: &mut ContainerStatus) {
        let Some(ContainerState::Terminated {
            exit_code,
            reason,
            message,
            ..
        }) = status.state.as_mut()
        else {
            return;
        };

        let policy = find_container(pod, name)
            .and_then(|c| c.termination_message_policy.clone())
            .unwrap_or_else(|| "File".to_string());

        let host_path = self.termination_log_host_path(pod, name);
        let file_read = std::fs::read_to_string(&host_path).ok();

        let log_path = format!("{}/{}.log", self.log_dir_for(pod), name);
        let exit = *exit_code;
        let reason = reason.clone();
        let resolved = status::resolve_termination_message(
            file_read,
            &policy,
            exit,
            reason.as_deref(),
            || read_log_tail(&log_path),
        );
        if let Some(m) = resolved {
            *message = Some(m);
        }
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
    ) -> anyhow::Result<String> {
        // Honor the image pull policy (also surfaces ErrImageNeverPull).
        self.ensure_image(
            &container.image,
            container.image_pull_policy.as_deref(),
            Some((pod, &container.name)),
        )
        .await?;
        let (config_maps, secrets) = self.resolve_env_sources(pod, container).await;
        // Fail container creation if a non-optional configMap/secret keyRef is
        // unresolvable, mirroring upstream's CreateContainerConfigError rather
        // than silently launching the container with the var missing.
        translate::validate_env_key_refs(pod, container, &config_maps, &secrets)
            .map_err(|msg| anyhow::anyhow!("env for container {}: {msg}", container.name))?;

        // Downward-API `status.podIP`/`hostIP`(`s`) env is resolved at
        // container-create time. Upstream resolves it from the pod status the
        // kubelet already built off the sandbox; our sync loop only writes that
        // status after containers start, so populate the IPs here (pod IP from
        // the running sandbox, host IP = node InternalIP) on a local copy.
        let mut eff_pod;
        let pod = if pod
            .status
            .as_ref()
            .map(|s| s.pod_ip.is_none() || s.host_ip.is_none())
            .unwrap_or(true)
        {
            eff_pod = pod.clone();
            let pod_ip = self.get_pod_ip(&pod.metadata.name).await.ok().flatten();
            let st = eff_pod.status.get_or_insert_with(Default::default);
            if st.pod_ip.is_none() {
                if let Some(ip) = pod_ip {
                    st.pod_i_ps = Some(vec![rusternetes_common::resources::pod::PodIP {
                        ip: ip.clone(),
                    }]);
                    st.pod_ip = Some(ip);
                }
            }
            if st.host_ip.is_none() {
                let nip = node_internal_ip().to_string();
                st.host_i_ps = Some(vec![rusternetes_common::resources::pod::HostIP {
                    ip: nip.clone(),
                }]);
                st.host_ip = Some(nip);
            }
            &eff_pod
        } else {
            pod
        };

        let node_allocatable = if self.node_allocatable.is_empty() {
            None
        } else {
            Some(&self.node_allocatable)
        };
        let mut cfg = translate::container_config_with_allocatable(
            pod,
            container,
            &container.image,
            host_paths,
            &config_maps,
            &secrets,
            node_allocatable,
        );
        self.inject_service_env(&mut cfg);
        self.inject_service_links(pod, &mut cfg).await;

        // Bind-mount a per-container host file at the container's
        // terminationMessagePath so it can write a termination message that the
        // kubelet reads back on exit (#442). Best-effort: if the host file can't
        // be created we omit the mount, leaving behaviour unchanged. The file
        // must exist before create so the runtime bind-mounts a file (not a new
        // dir) — matching upstream makeMounts, which Creates+Chmods it first.
        let term_host = self.termination_log_host_path(pod, &container.name);
        if let Some(parent) = Path::new(&term_host).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match crate::runtime::setup_termination_message_file(&term_host) {
            Ok(()) => {
                let container_path = container
                    .termination_message_path
                    .clone()
                    .unwrap_or_else(|| "/dev/termination-log".to_string());
                cfg.mounts.push(v1::Mount {
                    container_path,
                    host_path: term_host,
                    readonly: false,
                    ..Default::default()
                });
            }
            Err(e) => warn!(
                "container {}: termination-log setup failed, message unavailable: {e}",
                container.name
            ),
        }
        // Managed /etc/hosts (#1024): the CRI/containerd path otherwise gets
        // containerd's default hosts file, which omits `spec.hostAliases`.
        // Generate the kubelet-managed file and bind-mount it at /etc/hosts so
        // hostAliases (and the pod's own FQDN entry) are present — matching
        // upstream `makeMounts`. hostNetwork pods start from the node's
        // /etc/hosts and then get the aliases appended.
        // If the container mounts its own /etc/hosts (a volumeMount at that
        // path), the user's mount wins — upstream `makeMounts` skips the managed
        // hosts file in that case (the KubeletManagedEtcHosts spec asserts such
        // a container's /etc/hosts is NOT kubelet-managed).
        let container_mounts_etc_hosts = container
            .volume_mounts
            .iter()
            .flatten()
            .any(|m| Path::new(&m.mount_path) == Path::new("/etc/hosts"));
        let host_network = pod
            .spec
            .as_ref()
            .and_then(|s| s.host_network)
            .unwrap_or(false);
        let hosts_content = if container_mounts_etc_hosts {
            None
        } else if host_network {
            let mut c = std::fs::read_to_string("/etc/hosts").unwrap_or_default();
            for alias in pod
                .spec
                .iter()
                .flat_map(|s| s.host_aliases.iter().flatten())
            {
                match alias.hostnames.as_deref() {
                    Some(h) if !h.is_empty() => {
                        c.push_str(&format!("{}\t{}\n", alias.ip, h.join("\t")));
                    }
                    _ => {}
                }
            }
            Some(c)
        } else {
            let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.as_deref());
            crate::kubelet::build_managed_hosts_content(pod, pod_ip, &self.cluster_domain)
        };
        if let Some(content) = hosts_content {
            let hosts_path = format!("{}/etc-hosts", self.log_dir_for(pod));
            if let Some(parent) = Path::new(&hosts_path).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(&hosts_path, content) {
                Ok(()) => cfg.mounts.push(v1::Mount {
                    container_path: "/etc/hosts".to_string(),
                    host_path: hosts_path,
                    readonly: false,
                    ..Default::default()
                }),
                Err(e) => warn!(
                    "container {}: managed /etc/hosts setup failed: {e}",
                    container.name
                ),
            }
        }

        let container_id = cri
            .create_container(sandbox_id, cfg, sandbox_cfg.clone())
            .await?;
        cri.start_container(&container_id).await?;

        // postStart lifecycle hook: runs immediately after the container starts
        // (upstream kuberuntime `startContainer`). Best-effort — a failed
        // postStart upstream restarts the container, but the conformance handler
        // asserts the hook request was *received*, so executing it is the goal;
        // log on failure rather than tearing the container down.
        if let Some(ps) = container
            .lifecycle
            .as_ref()
            .and_then(|lc| lc.post_start.as_ref())
        {
            if let Err(e) = self
                .run_lifecycle_handler(pod, container, &container_id, ps)
                .await
            {
                warn!("container {}: postStart hook failed: {e}", container.name);
            }
        }
        Ok(container_id)
    }

    /// Fetch the ConfigMap/Secret objects a container's env references —
    /// `configMapKeyRef`/`secretKeyRef` (`valueFrom`) and `configMapRef`/
    /// `secretRef` (`envFrom`) — keyed by name, so [`translate`] can resolve
    /// them without storage access. Mirrors the upstream kubelet's configMap/
    /// secret managers feeding `makeEnvironmentVariables`. Missing or
    /// un-fetchable sources are simply absent from the maps (translate then
    /// omits the var); storage-less smoke runs return empty maps.
    async fn resolve_env_sources(
        &self,
        pod: &Pod,
        container: &Container,
    ) -> (
        std::collections::HashMap<String, ConfigMap>,
        std::collections::HashMap<String, Secret>,
    ) {
        let mut config_maps = std::collections::HashMap::new();
        let mut secrets = std::collections::HashMap::new();
        let Some(storage) = self.volumes.as_ref().and_then(|v| v.storage.as_ref()) else {
            return (config_maps, secrets);
        };
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");

        // Collect the ConfigMap/Secret names referenced by both `valueFrom`
        // keyrefs and `envFrom` bulk sources, then fetch each once.
        let mut cm_names: Vec<&str> = Vec::new();
        let mut secret_names: Vec<&str> = Vec::new();
        for e in container.env.iter().flatten() {
            if let Some(src) = e.value_from.as_ref() {
                if let Some(cmr) = src.config_map_key_ref.as_ref() {
                    cm_names.push(&cmr.name);
                }
                if let Some(skr) = src.secret_key_ref.as_ref() {
                    secret_names.push(&skr.name);
                }
            }
        }
        for ef in container.env_from.iter().flatten() {
            if let Some(cmr) = ef.config_map_ref.as_ref() {
                cm_names.push(&cmr.name);
            }
            if let Some(skr) = ef.secret_ref.as_ref() {
                secret_names.push(&skr.name);
            }
        }

        for name in cm_names {
            if !config_maps.contains_key(name) {
                let key = rusternetes_storage::build_key("configmaps", Some(ns), name);
                if let Ok(cm) = storage.get::<ConfigMap>(&key).await {
                    config_maps.insert(name.to_string(), cm);
                }
            }
        }
        for name in secret_names {
            if !secrets.contains_key(name) {
                let key = rusternetes_storage::build_key("secrets", Some(ns), name);
                if let Ok(s) = storage.get::<Secret>(&key).await {
                    secrets.insert(name.to_string(), s);
                }
            }
        }

        (config_maps, secrets)
    }

    /// Inject the standard `kubernetes` Service env vars (KUBERNETES_SERVICE_HOST
    /// /PORT and the docker-link KUBERNETES_PORT_* forms) so in-cluster clients
    /// can reach the api-server. Existing same-named vars in the container spec
    /// win.
    fn inject_service_env(&self, cfg: &mut v1::ContainerConfig) {
        for (key, value) in service_env_vars(&self.service_host, &self.service_port) {
            if cfg.envs.iter().any(|e| e.key == key) {
                continue; // explicit pod env overrides the injected default
            }
            cfg.envs.push(v1::KeyValue { key, value });
        }
    }

    /// Inject Docker-link Service env for every service in the pod's namespace
    /// (upstream `enableServiceLinks`, default on). Mirrors the kubelet's
    /// `getServiceEnvVarMap`; explicit container env wins over an injected var.
    async fn inject_service_links(&self, pod: &Pod, cfg: &mut v1::ContainerConfig) {
        let enabled = pod
            .spec
            .as_ref()
            .and_then(|s| s.enable_service_links)
            .unwrap_or(true);
        if !enabled {
            return;
        }
        let Some(storage) = self.volumes.as_ref().and_then(|v| v.storage.as_ref()) else {
            return;
        };
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        let Ok(services) = storage
            .list::<Service>(&build_prefix("services", Some(ns)))
            .await
        else {
            return;
        };
        for (key, value) in translate::service_link_env_vars(&services) {
            if cfg.envs.iter().any(|e| e.key == key) {
                continue;
            }
            cfg.envs.push(v1::KeyValue { key, value });
        }
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
    pub async fn start_pod(&self, pod: &Pod) -> anyhow::Result<()> {
        let Some(spec) = pod.spec.as_ref() else {
            return Ok(());
        };

        let log_dir = self.log_dir_for(pod);
        std::fs::create_dir_all(&log_dir)?;

        // Provision host-side volumes once for the pod (emptyDir/hostPath/
        // configMap/secret/projected/downwardAPI), yielding the volume-name →
        // host-path map the container translation mounts.
        let host_paths = self.create_pod_volumes(pod).await?;

        let mut sandbox_cfg = translate::sandbox_config(pod, &log_dir);
        sandbox_cfg.dns_config =
            translate::dns_config(pod, &self.cluster_dns, &self.cluster_domain);
        let handler = self.runtime_handler.clone();

        let mut cri = self.cri.clone();
        // Idempotent: reuse a ready sandbox if one already exists for this pod
        // (start_pod is retried by the reconcile loop, and the runtime reserves
        // the sandbox name, so re-running RunPodSandbox would fail with "name
        // reserved"). A non-ready leftover sandbox is removed and recreated.
        let sandbox_id = match self.ready_sandbox_for(&pod.metadata.name).await {
            Some(existing) => existing,
            None => {
                // Drop any stale (not-ready) sandbox holding the name first.
                let _ = self.stop_and_remove_pod(&pod.metadata.name).await;
                cri.run_pod_sandbox(sandbox_cfg.clone(), &handler).await?
            }
        };

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
                    }
                    .into());
                }
            }
        }

        for container in &spec.containers {
            // Idempotent: start_pod is retried by the reconcile loop. Skip a
            // container the runtime already has (any state) — re-creating it
            // would race a duplicate CreateContainer for the same name and, for
            // long-running multi-container pods, crash-loop on port collisions.
            // Restarting an exited container is the reconcile loop's job.
            if self
                .container_exists(&pod.metadata.uid, &container.name)
                .await
            {
                continue;
            }
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

    /// The id of a READY sandbox for the pod, if one exists — used by `start_pod`
    /// to reuse a running sandbox across reconcile retries instead of trying to
    /// create a new one (which would collide on the reserved sandbox name).
    async fn ready_sandbox_for(&self, pod_name: &str) -> Option<String> {
        let filter = v1::PodSandboxFilter {
            state: Some(v1::PodSandboxStateValue {
                state: v1::PodSandboxState::SandboxReady as i32,
            }),
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_NAME.to_string(),
                pod_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        cri.list_pod_sandbox(Some(filter))
            .await
            .ok()
            .and_then(|s| s.into_iter().next())
            .map(|s| s.id)
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

        // Index runtime containers by their kubernetes container name. A
        // restarting container leaves several CRI containers with the same name
        // (the exited prior attempt plus the current one); keep the one with the
        // highest `attempt` so the reported `restartCount` is the latest and
        // never regresses — list order is unspecified, so picking the last-seen
        // entry made the count flip backwards (the monotonic-restart-count
        // NodeConformance spec). Matches upstream, which reports the newest
        // container and the max restart count.
        let mut by_name: std::collections::HashMap<String, (String, u32)> =
            std::collections::HashMap::new();
        for c in &containers {
            let attempt = c.metadata.as_ref().map(|m| m.attempt).unwrap_or(0);
            if let Some(name) = c
                .labels
                .get(translate::labels::CONTAINER_NAME)
                .or_else(|| c.metadata.as_ref().map(|m| &m.name))
            {
                match by_name.get(name) {
                    Some((_, prev)) if *prev >= attempt => {}
                    _ => {
                        by_name.insert(name.clone(), (c.id.clone(), attempt));
                    }
                }
            }
        }

        let mut out = Vec::with_capacity(names.len());
        for name in names {
            match by_name.get(name) {
                Some((id, _)) => {
                    let full = cri.container_status(id, false).await?;
                    let mut mapped = full
                        .status
                        .as_ref()
                        .map(status::map_container_status)
                        .unwrap_or_else(|| waiting_status(name));
                    self.apply_termination_message(pod, name, &mut mapped);
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
        let mut statuses = self.statuses_for(pod, &names).await.ok()?;
        // A successfully-terminated plain init container is Ready=true (upstream
        // prober_manager); statuses_for derives ready from RUNNING only, so a
        // completed init container would otherwise report ready=false and fail
        // the "init container should be in Ready status" NodeConformance spec.
        for st in &mut statuses {
            let restartable = init
                .iter()
                .find(|c| c.name == st.name)
                .and_then(|c| c.restart_policy.as_deref())
                == Some("Always");
            st.ready = init_container_ready(&st.state, restartable, st.ready);
        }
        Some(statuses)
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
    pub async fn is_container_running(&self, pod_uid: &str, container_name: &str) -> Result<bool> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([
                (translate::labels::POD_UID.to_string(), pod_uid.to_string()),
                (
                    translate::labels::CONTAINER_NAME.to_string(),
                    container_name.to_string(),
                ),
            ]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let containers = cri.list_containers(Some(filter)).await?;
        // "Alive" = CREATED or RUNNING. Counting CREATED (not just RUNNING) is
        // essential: during the create→running window a container is CREATED,
        // and a concurrent reconcile that treated it as dead would start a
        // duplicate (port collision → crash-loop). Only EXITED is restartable.
        let created = v1::ContainerState::ContainerCreated as i32;
        let running = v1::ContainerState::ContainerRunning as i32;
        Ok(containers
            .iter()
            .any(|c| c.state == running || c.state == created))
    }

    /// True if the pod's sandbox exists on this runtime (the `pause`-equivalent
    /// PodSandbox has been created), regardless of container state.
    pub async fn has_sandbox(&self, pod_name: &str) -> bool {
        self.sandbox_id_for(pod_name).await.ok().flatten().is_some()
    }

    /// Execute a single probe attempt against a container, returning whether it
    /// succeeded. The kubelet drives the surrounding state machine (delay,
    /// period, thresholds); this performs one attempt:
    ///
    /// - `exec`: run the command via CRI `ExecSync`; success = exit 0.
    /// - `tcpSocket`: TCP connect to the (host or pod IP):port from the node.
    /// - `httpGet`: HTTP(S) GET to the (host or pod IP):port/path; success =
    ///   status < 400 (k8s treats 2xx/3xx as healthy).
    /// - `grpc`: dial `grpc.health.v1` Health/Check at (pod IP):port; success =
    ///   `SERVING`. Unreachable / NOT_SERVING / errored = failure.
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
            // Enforce timeoutSeconds node-side: a CRI runtime may not honor
            // ExecSyncRequest.timeout, so bound the call ourselves. A command
            // that outruns the timeout fails the probe (→ restart), matching the
            // kubelet exec prober — the in-container process is left to the
            // runtime to reap.
            let timeout_secs = probe.timeout_seconds.unwrap_or(1).max(1) as i64;
            let exit = match tokio::time::timeout(timeout, cri.exec_sync(&c.id, &cmd, timeout_secs))
                .await
            {
                Ok(Ok(resp)) => Some(resp.exit_code),
                Ok(Err(e)) => return Err(e.into()),
                Err(_elapsed) => None,
            };
            return Ok(exec_probe_passed(exit));
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
                .redirect(reqwest::redirect::Policy::custom(|attempt| {
                    if probe::redirect_is_non_local(attempt.url(), attempt.previous()) {
                        return attempt.stop();
                    }
                    if attempt.previous().len() >= 10 {
                        return attempt.error("stopped after 10 redirects");
                    }
                    attempt.follow()
                }))
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
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_redirection() {
                        let body = resp.text().await.unwrap_or_default();
                        self.emit_event(
                            pod,
                            Some(&container.name),
                            crate::events::CONTAINER_PROBE_WARNING,
                            rusternetes_common::resources::EventType::Warning,
                            &format!("Probe terminated redirects, Response body: {body}"),
                        )
                        .await;
                    }
                    status.as_u16() < 400
                }
                Err(_) => false,
            };
            return Ok(ok);
        }

        if let Some(grpc) = probe.grpc.as_ref() {
            let Some(port) = probe::resolve_port(container, &grpc.port) else {
                return Ok(false);
            };
            let Some(host) = self.get_pod_ip(&pod.metadata.name).await? else {
                return Ok(false);
            };
            let endpoint = format!("http://{host}:{port}");
            let service = grpc.service.clone().unwrap_or_default();
            // Dial grpc.health.v1 Health/Check from the node; SERVING = healthy.
            // Connect + RPC are bounded by the probe timeout; any error (down,
            // refused, NOT_SERVING) fails the probe (matches the kubelet grpc
            // prober).
            let check = async {
                let channel = tonic::transport::Channel::from_shared(endpoint)
                    .map_err(|e| anyhow::anyhow!("grpc probe endpoint: {e}"))?
                    .connect()
                    .await?;
                let resp = tonic_health::pb::health_client::HealthClient::new(channel)
                    .check(tonic_health::pb::HealthCheckRequest { service })
                    .await?;
                Ok::<i32, anyhow::Error>(resp.into_inner().status)
            };
            let healthy = matches!(
                tokio::time::timeout(timeout, check).await,
                Ok(Ok(status)) if grpc_status_healthy(status)
            );
            return Ok(healthy);
        }

        // No probe action configured.
        Ok(true)
    }

    /// Execute a container lifecycle handler (postStart / preStop): exec via CRI
    /// `ExecSync` inside `container_id`, an httpGet/tcpSocket dialed from the
    /// node against the (handler host or pod) IP, or a sleep. Mirrors upstream
    /// `lifecycle.handlerRunner.Run`. Returns an error when the handler fails
    /// (non-zero exec, HTTP >= 400, unreachable socket) so the caller can react.
    async fn run_lifecycle_handler(
        &self,
        pod: &Pod,
        container: &Container,
        container_id: &str,
        handler: &rusternetes_common::resources::pod::LifecycleHandler,
    ) -> anyhow::Result<()> {
        // Lifecycle handlers have no per-handler timeout in the API; upstream
        // bounds preStop by the grace period and lets postStart block readiness.
        // A generous fixed cap keeps a hung handler from wedging the worker.
        const HANDLER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

        if let Some(exec) = handler.exec.as_ref() {
            if exec.command.is_empty() {
                return Ok(());
            }
            let cmd: Vec<&str> = exec.command.iter().map(String::as_str).collect();
            let mut cri = self.cri.clone();
            let resp = cri
                .exec_sync(container_id, &cmd, HANDLER_TIMEOUT.as_secs() as i64)
                .await?;
            if resp.exit_code != 0 {
                anyhow::bail!(
                    "exec lifecycle handler {:?} exited {}",
                    exec.command,
                    resp.exit_code
                );
            }
            return Ok(());
        }

        if let Some(http) = handler.http_get.as_ref() {
            let Some(port) = probe::resolve_port(container, &http.port) else {
                anyhow::bail!("httpGet lifecycle handler: unresolved port");
            };
            let host = match http.host.clone() {
                Some(h) if !h.is_empty() => h,
                _ => self
                    .get_pod_ip(&pod.metadata.name)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("httpGet lifecycle handler: no pod IP"))?,
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
            let client = reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .timeout(HANDLER_TIMEOUT)
                .build()?;
            let mut req = client.get(&url);
            if let Some(headers) = http.http_headers.as_ref() {
                for h in headers {
                    req = req.header(&h.name, &h.value);
                }
            }
            let resp = req.send().await?;
            if resp.status().as_u16() >= 400 {
                anyhow::bail!("httpGet lifecycle handler {url} returned {}", resp.status());
            }
            return Ok(());
        }

        if let Some(tcp) = handler.tcp_socket.as_ref() {
            let Some(port) = probe::resolve_port(container, &tcp.port) else {
                anyhow::bail!("tcpSocket lifecycle handler: unresolved port");
            };
            let host = match tcp.host.clone() {
                Some(h) if !h.is_empty() => h,
                _ => self
                    .get_pod_ip(&pod.metadata.name)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("tcpSocket lifecycle handler: no pod IP"))?,
            };
            tokio::time::timeout(
                HANDLER_TIMEOUT,
                tokio::net::TcpStream::connect(format!("{host}:{port}")),
            )
            .await??;
            return Ok(());
        }

        if let Some(sleep) = handler.sleep.as_ref() {
            if sleep.seconds > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(sleep.seconds as u64)).await;
            }
            return Ok(());
        }

        Ok(())
    }

    /// Seconds since the named container started, from CRI `started_at`. `None`
    /// if it is not found or has no start time yet.
    async fn container_age_secs(&self, pod: &Pod, container_name: &str) -> Option<i64> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([
                (
                    translate::labels::POD_UID.to_string(),
                    pod.metadata.uid.clone(),
                ),
                (
                    translate::labels::CONTAINER_NAME.to_string(),
                    container_name.to_string(),
                ),
            ]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let c = cri
            .list_containers(Some(filter))
            .await
            .ok()?
            .into_iter()
            .next()?;
        let started = cri
            .container_status(&c.id, false)
            .await
            .ok()?
            .status?
            .started_at;
        if started <= 0 {
            return None;
        }
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Some((now - started).max(0) / 1_000_000_000)
    }

    /// Clear all probe threshold state for a pod (on pod deletion/restart) so a
    /// recreated pod's probes start fresh.
    pub fn clear_probe_states_for_pod(&self, pod_name: &str) {
        let prefix = format!("{pod_name}/");
        let mut states = self.probe_states.lock().unwrap();
        states.retain(|key, _| !key.starts_with(&prefix));
    }

    /// Evaluate liveness across a pod, returning `Some(grace_seconds)` if a
    /// regular container's startup/liveness probe has failed enough consecutive
    /// times to warrant restarting the whole pod. Restartable init containers
    /// (sidecars) are restarted individually instead. Liveness is disabled while
    /// the pod is terminating.
    pub async fn check_liveness(&self, pod: &Pod) -> Result<Option<i64>> {
        if pod.metadata.deletion_timestamp.is_some() {
            return Ok(None);
        }
        let Some(spec) = pod.spec.as_ref() else {
            return Ok(None);
        };

        for container in &spec.containers {
            if let Some(grace) = self.evaluate_container_liveness(pod, container).await {
                return Ok(Some(grace));
            }
        }

        // Restartable init containers (restartPolicy=Always sidecars) are
        // restarted individually, not by restarting the whole pod.
        let restartable = spec
            .init_containers
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|ic| ic.restart_policy.as_deref() == Some("Always"));
        for container in restartable {
            if self
                .evaluate_container_liveness(pod, container)
                .await
                .is_some()
            {
                warn!(
                    "restarting sidecar {} after failed liveness probe",
                    container.name
                );
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
                if let Ok(cs) = cri.list_containers(Some(filter)).await {
                    for c in cs {
                        let _ = cri.stop_container(&c.id, 0).await;
                    }
                }
            }
        }
        Ok(None)
    }

    /// Evaluate one container's startup+liveness probes with threshold tracking.
    /// `Some(grace)` means restart is warranted (grace = the probe's
    /// terminationGracePeriodSeconds, else the pod's, else 30).
    async fn evaluate_container_liveness(&self, pod: &Pod, container: &Container) -> Option<i64> {
        let pod_name = &pod.metadata.name;
        let pod_grace = pod
            .spec
            .as_ref()
            .and_then(|s| s.termination_grace_period_seconds);
        let grace_of = |probe: &Probe| {
            probe
                .termination_grace_period_seconds
                .or(pod_grace)
                .unwrap_or(30)
        };

        // Startup probe gates the liveness probe until it passes.
        if let Some(startup) = &container.startup_probe {
            let key = format!("{pod_name}/{}/startup", container.name);
            let period = startup.period_seconds.unwrap_or(10).max(1) as i64;
            let due = {
                let mut states = self.probe_states.lock().unwrap();
                let st = states.entry(key.clone()).or_default();
                let now = Utc::now();
                let due = st
                    .last_eval
                    .map(|t| (now - t).num_seconds() >= period)
                    .unwrap_or(true);
                if due {
                    st.last_eval = Some(now);
                }
                due
            };
            if !due {
                return None;
            }
            let ok = self
                .probe_container(pod, container, startup)
                .await
                .unwrap_or(false);
            let failure_threshold = startup.failure_threshold.unwrap_or(3);
            let success_threshold = startup.success_threshold.unwrap_or(1);
            enum Outcome {
                Passed,
                Pending,
                Failed,
            }
            let outcome = {
                let mut states = self.probe_states.lock().unwrap();
                let st = states.entry(key).or_default();
                if ok {
                    st.consecutive_successes += 1;
                    st.consecutive_failures = 0;
                    if st.consecutive_successes >= success_threshold {
                        Outcome::Passed
                    } else {
                        Outcome::Pending
                    }
                } else {
                    st.consecutive_failures += 1;
                    st.consecutive_successes = 0;
                    if st.consecutive_failures >= failure_threshold {
                        st.consecutive_failures = 0;
                        Outcome::Failed
                    } else {
                        Outcome::Pending
                    }
                }
            };
            match outcome {
                Outcome::Passed => {}
                Outcome::Pending => return None,
                Outcome::Failed => {
                    warn!(
                        "startup probe exceeded failure threshold for {} — restarting",
                        container.name
                    );
                    return Some(grace_of(startup));
                }
            }
        }

        let probe = container.liveness_probe.as_ref()?;

        // Honor initialDelaySeconds from the container's start time.
        let initial_delay = probe.initial_delay_seconds.unwrap_or(0);
        if initial_delay > 0 {
            if let Some(age) = self.container_age_secs(pod, &container.name).await {
                if age < initial_delay as i64 {
                    return None;
                }
            }
        }

        // A probe error (vs a clean failure) is transient — skip without counting.
        let healthy = match self.probe_container(pod, container, probe).await {
            Ok(h) => h,
            Err(_) => return None,
        };
        let failure_threshold = probe.failure_threshold.unwrap_or(3);
        let key = format!("{pod_name}/{}/liveness", container.name);
        let mut states = self.probe_states.lock().unwrap();
        let st = states.entry(key).or_default();
        if healthy {
            st.consecutive_successes += 1;
            st.consecutive_failures = 0;
            None
        } else {
            st.consecutive_failures += 1;
            st.consecutive_successes = 0;
            if st.consecutive_failures >= failure_threshold {
                warn!(
                    "liveness probe failed {} times (threshold {}) for {}",
                    st.consecutive_failures, failure_threshold, container.name
                );
                st.consecutive_failures = 0;
                Some(grace_of(probe))
            } else {
                debug!(
                    "liveness probe failed for {} ({}/{})",
                    container.name, st.consecutive_failures, failure_threshold
                );
                None
            }
        }
    }

    /// Set `ready` on each container status from its readiness probe, with
    /// initialDelay + success/failure threshold tracking. Containers without a
    /// readiness probe keep their running-based readiness; only a running
    /// container can be ready. Readiness never restarts — it only gates the
    /// `ready` flag the api-server rolls up into ContainersReady/Ready.
    pub async fn apply_readiness(&self, pod: &Pod, statuses: &mut [ContainerStatus]) {
        let Some(spec) = pod.spec.as_ref() else {
            return;
        };
        for st in statuses.iter_mut() {
            let container = spec
                .containers
                .iter()
                .find(|c| c.name == st.name)
                .or_else(|| {
                    spec.init_containers.iter().flatten().find(|c| {
                        c.name == st.name && c.restart_policy.as_deref() == Some("Always")
                    })
                });
            let Some(container) = container else { continue };
            let Some(probe) = container.readiness_probe.as_ref() else {
                continue;
            };
            let running = matches!(st.state, Some(ContainerState::Running { .. }));
            st.ready = running
                && self
                    .evaluate_container_readiness(pod, container, probe)
                    .await;
        }
    }

    /// Evaluate one container's readiness probe, returning the current readiness
    /// latch. Honors `initialDelaySeconds` (not ready, and unprobed, until it
    /// elapses) and `periodSeconds` (the latch holds between periods). A probe
    /// error counts as a failure (upstream `doProbe`).
    async fn evaluate_container_readiness(
        &self,
        pod: &Pod,
        container: &Container,
        probe: &Probe,
    ) -> bool {
        let key = format!("{}/{}/readiness", pod.metadata.name, container.name);

        let initial_delay = probe.initial_delay_seconds.unwrap_or(0);
        if initial_delay > 0 {
            if let Some(age) = self.container_age_secs(pod, &container.name).await {
                if age < initial_delay as i64 {
                    let mut states = self.probe_states.lock().unwrap();
                    states.entry(key).or_default().ready = false;
                    return false;
                }
            }
        }

        let period = probe.period_seconds.unwrap_or(10).max(1) as i64;
        let due = {
            let mut states = self.probe_states.lock().unwrap();
            let st = states.entry(key.clone()).or_default();
            let now = Utc::now();
            let due = st
                .last_eval
                .map(|t| (now - t).num_seconds() >= period)
                .unwrap_or(true);
            if due {
                st.last_eval = Some(now);
            }
            due
        };
        if !due {
            return self
                .probe_states
                .lock()
                .unwrap()
                .entry(key)
                .or_default()
                .ready;
        }

        let healthy = self
            .probe_container(pod, container, probe)
            .await
            .unwrap_or(false);
        let success_threshold = probe.success_threshold.unwrap_or(1);
        let failure_threshold = probe.failure_threshold.unwrap_or(3);
        let mut states = self.probe_states.lock().unwrap();
        let st = states.entry(key).or_default();
        readiness_after_observation(st, healthy, success_threshold, failure_threshold)
    }

    /// Exit code of the (most recent) container named `container_name`, or 0 if
    /// no such container is known to the runtime.
    pub async fn get_container_exit_code(
        &self,
        pod_uid: &str,
        container_name: &str,
    ) -> Result<i64> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([
                (translate::labels::POD_UID.to_string(), pod_uid.to_string()),
                (
                    translate::labels::CONTAINER_NAME.to_string(),
                    container_name.to_string(),
                ),
            ]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        // Pick the latest attempt so the exit code reflects the most recent run.
        let Some(container) = cri
            .list_containers(Some(filter))
            .await?
            .into_iter()
            .max_by_key(|c| c.metadata.as_ref().map(|m| m.attempt).unwrap_or(0))
        else {
            return Ok(0);
        };
        let status = cri.container_status(&container.id, false).await?;
        Ok(status.status.map(|s| i64::from(s.exit_code)).unwrap_or(0))
    }

    /// Remove every exited container named `container_name` so a restart can
    /// recreate it. Running containers are left alone.
    pub async fn remove_terminated_container(
        &self,
        pod_uid: &str,
        container_name: &str,
    ) -> Result<()> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([
                (translate::labels::POD_UID.to_string(), pod_uid.to_string()),
                (
                    translate::labels::CONTAINER_NAME.to_string(),
                    container_name.to_string(),
                ),
            ]),
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
    /// Whether a container with the given name exists for the given pod, in any
    /// state. Scoped by pod UID **and** container name to match the CRI labels we
    /// actually write (`io.kubernetes.pod.uid` + `io.kubernetes.container.name`,
    /// the upstream convention). The container name label is the bare
    /// `container.name` (e.g. `kube-flannel`), shared across every pod of a
    /// DaemonSet — so without the UID scope this would either never match (when
    /// the caller passes a `pod_pod_name_container`-style key) or match the wrong
    /// pod's container. Both break `computePodActions`, which then recreates a
    /// container that already exists and collides with containerd's reserved name.
    pub async fn container_exists(&self, pod_uid: &str, container_name: &str) -> bool {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([
                (translate::labels::POD_UID.to_string(), pod_uid.to_string()),
                (
                    translate::labels::CONTAINER_NAME.to_string(),
                    container_name.to_string(),
                ),
            ]),
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
            // preStop lifecycle hook runs (within the grace period) before the
            // container is stopped — upstream `kuberuntime_container.go`
            // killContainer. Best-effort; never block teardown on a hook error.
            if c.state == v1::ContainerState::ContainerRunning as i32 {
                if let Some(name) = c
                    .labels
                    .get(translate::labels::CONTAINER_NAME)
                    .or_else(|| c.metadata.as_ref().map(|m| &m.name))
                {
                    if let Some(pre) = find_container(pod, name)
                        .and_then(|spec_c| spec_c.lifecycle.as_ref())
                        .and_then(|lc| lc.pre_stop.as_ref())
                    {
                        if let Some(spec_c) = find_container(pod, name) {
                            if let Err(e) =
                                self.run_lifecycle_handler(pod, spec_c, &c.id, pre).await
                            {
                                warn!("container {name}: preStop hook failed: {e}");
                            }
                        }
                    }
                }
            }
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

/// The standard `kubernetes` master-Service env vars, in upstream order
/// (`pkg/kubelet/envvars/envvars.go` `FromServices` + `makeLinkVariables`).
///
/// The default Service is a single TCP port named `https`, so upstream emits
/// the `KUBERNETES_SERVICE_PORT_HTTPS` alias for it. The docker-link var names
/// embed the *actual* Service port and protocol (`KUBERNETES_PORT_<port>_TCP`),
/// never a hardcoded value, so a non-443 api-server gets correct names.
fn service_env_vars(host: &str, port: &str) -> Vec<(String, String)> {
    let tcp = format!("tcp://{host}:{port}");
    // Docker-link prefix mirrors makeLinkVariables' "%s_PORT_%d_%s": the actual
    // port and (TCP) protocol, so a non-443 api-server gets the right names.
    let pp = format!("KUBERNETES_PORT_{port}_TCP");
    vec![
        ("KUBERNETES_SERVICE_HOST".to_string(), host.to_string()),
        ("KUBERNETES_SERVICE_PORT".to_string(), port.to_string()),
        (
            "KUBERNETES_SERVICE_PORT_HTTPS".to_string(),
            port.to_string(),
        ),
        ("KUBERNETES_PORT".to_string(), tcp.clone()),
        (pp.clone(), tcp),
        (format!("{pp}_PROTO"), "tcp".to_string()),
        (format!("{pp}_PORT"), port.to_string()),
        (format!("{pp}_ADDR"), host.to_string()),
    ]
}

/// Memoized node InternalIP, used to fill `status.hostIP`(`s`) for downward-API
/// env resolution at container-create time. Mirrors the kubelet's
/// `detect_internal_ip` (resolve our own hostname to its non-loopback IPv4);
/// stable for the process lifetime.
fn node_internal_ip() -> &'static str {
    static NODE_IP: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    NODE_IP.get_or_init(|| {
        std::env::var("HOSTNAME")
            .ok()
            .and_then(|h| std::net::ToSocketAddrs::to_socket_addrs(&(h.as_str(), 0u16)).ok())
            .and_then(|addrs| {
                addrs
                    .filter_map(|a| match a.ip() {
                        std::net::IpAddr::V4(ip) if !ip.is_loopback() => Some(ip.to_string()),
                        _ => None,
                    })
                    .next()
            })
            .unwrap_or_else(|| "127.0.0.1".to_string())
    })
}

/// Format a CRI `VersionResponse` as the k8s `containerRuntimeVersion`
/// (`<runtime_name>://<runtime_version>`).
fn format_runtime_version(v: &v1::VersionResponse) -> String {
    format!("{}://{}", v.runtime_name, v.runtime_version)
}

/// Readiness of an init container's status, per upstream prober_manager. A
/// plain (non-restartable) init container is Ready only once it has terminated
/// successfully (exit 0). A restartable init (sidecar) keeps its existing
/// running/probe-based readiness (`prior`).
fn init_container_ready(state: &Option<ContainerState>, restartable: bool, prior: bool) -> bool {
    if restartable {
        return prior;
    }
    matches!(state, Some(ContainerState::Terminated { exit_code, .. }) if *exit_code == 0)
}

/// Map a `grpc.health.v1` ServingStatus code to probe success. Per the kubelet
/// grpc prober, only SERVING is healthy; NOT_SERVING / UNKNOWN / SERVICE_UNKNOWN
/// and an unreachable/errored server all fail.
fn grpc_status_healthy(status: i32) -> bool {
    status == tonic_health::pb::health_check_response::ServingStatus::Serving as i32
}

/// An exec probe passes iff the command completed with exit code 0 within
/// timeoutSeconds. `None` (the call timed out) fails — the command outran its
/// timeout, which the kubelet exec prober treats as a probe failure.
fn exec_probe_passed(exit_code: Option<i32>) -> bool {
    exit_code == Some(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::pod::HTTPGetAction;
    use rusternetes_common::resources::pod::PodSpec;
    use rusternetes_common::resources::policy::IntOrString;
    use rusternetes_common::resources::Event;
    use rusternetes_storage::{Storage, StorageBackend};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn plain_init_container_ready_only_after_successful_exit() {
        let term = |code| {
            Some(ContainerState::Terminated {
                exit_code: code,
                signal: None,
                reason: None,
                message: None,
                started_at: None,
                finished_at: None,
                container_id: None,
            })
        };
        let running = Some(ContainerState::Running { started_at: None });
        // Plain init container: Ready only once it has terminated with exit 0
        // (upstream prober_manager). A non-zero exit or still-running = not ready.
        assert!(init_container_ready(&term(0), false, false));
        assert!(!init_container_ready(&term(1), false, false));
        assert!(!init_container_ready(&running, false, false));
        // Restartable init (sidecar) keeps its prior running/probe-based readiness.
        assert!(init_container_ready(&running, true, true));
        assert!(!init_container_ready(&running, true, false));
    }

    #[test]
    fn grpc_probe_only_serving_status_is_healthy() {
        use tonic_health::pb::health_check_response::ServingStatus;
        // The kubelet grpc prober treats only SERVING as healthy; every other
        // status (and an unreachable server) is a probe failure.
        assert!(grpc_status_healthy(ServingStatus::Serving as i32));
        assert!(!grpc_status_healthy(ServingStatus::NotServing as i32));
        assert!(!grpc_status_healthy(ServingStatus::Unknown as i32));
        assert!(!grpc_status_healthy(ServingStatus::ServiceUnknown as i32));
    }

    #[test]
    fn exec_probe_passes_only_on_zero_exit_within_timeout() {
        // Healthy iff the command completed with exit 0 within timeoutSeconds.
        // A timed-out exec (None) fails — the command outran its timeout, which
        // the kubelet exec prober treats as a probe failure (→ restart).
        assert!(exec_probe_passed(Some(0)));
        assert!(!exec_probe_passed(Some(1)));
        assert!(!exec_probe_passed(None));
    }

    #[test]
    fn readiness_state_machine_latches_on_thresholds() {
        let mut st = ProbeState::default();
        // Not ready until the first success (successThreshold 1).
        assert!(!st.ready);
        assert!(readiness_after_observation(&mut st, true, 1, 3));
        // Single/second failure with failureThreshold 3 holds ready=true.
        assert!(readiness_after_observation(&mut st, false, 1, 3));
        assert!(readiness_after_observation(&mut st, false, 1, 3));
        // Third consecutive failure crosses the threshold → not ready.
        assert!(!readiness_after_observation(&mut st, false, 1, 3));
        // A later success (threshold 1) recovers readiness.
        assert!(readiness_after_observation(&mut st, true, 1, 3));
    }

    #[tokio::test]
    async fn apply_readiness_updates_restartable_init_sidecars_only() {
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .expect("runtime handle is lazy and should not dial during construction");
        let plain_init = Container {
            name: "plain-init".to_string(),
            image: "busybox".to_string(),
            readiness_probe: Some(serde_json::from_str::<Probe>("{}").unwrap()),
            ..Default::default()
        };
        let sidecar = Container {
            name: "sidecar".to_string(),
            image: "busybox".to_string(),
            restart_policy: Some("Always".to_string()),
            readiness_probe: Some(serde_json::from_str::<Probe>("{}").unwrap()),
            ..Default::default()
        };
        let app = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        let pod = Pod::new(
            "sidecar-ready",
            PodSpec {
                containers: vec![app],
                init_containers: Some(vec![plain_init, sidecar]),
                ..Default::default()
            },
        );
        let running = Some(ContainerState::Running { started_at: None });
        let mut statuses = vec![
            ContainerStatus {
                name: "plain-init".to_string(),
                ready: false,
                state: running.clone(),
                ..waiting_status("plain-init")
            },
            ContainerStatus {
                name: "sidecar".to_string(),
                ready: false,
                state: running,
                ..waiting_status("sidecar")
            },
        ];

        runtime.apply_readiness(&pod, &mut statuses).await;

        assert!(
            !statuses[0].ready,
            "plain init containers are not readiness-probe targets"
        );
        assert!(
            statuses[1].ready,
            "restartable init sidecars must get readiness-probe results"
        );
    }

    async fn spawn_http_probe_redirect_server() -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let response = match path {
                        "/redirect-local" => {
                            "HTTP/1.1 302 Found\r\nLocation: /fail\r\nContent-Length: 0\r\n\r\n"
                        }
                        "/redirect-nonlocal" => {
                            concat!(
                                "HTTP/1.1 302 Found\r\n",
                                "Location: http://0.0.0.0/fail\r\n",
                                "Content-Length: 40\r\n",
                                "\r\n",
                                "<a href=\"http://0.0.0.0/fail\">Found</a>."
                            )
                        }
                        "/fail" => {
                            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"
                        }
                        _ => "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    fn http_probe(path: &str, port: u16) -> Probe {
        Probe {
            http_get: Some(HTTPGetAction {
                path: Some(path.to_string()),
                port: IntOrString::Int(i32::from(port)),
                host: Some("127.0.0.1".to_string()),
                scheme: Some("HTTP".to_string()),
                http_headers: None,
            }),
            ..serde_json::from_str::<Probe>("{}").unwrap()
        }
    }

    fn probe_test_container() -> Container {
        Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn http_probe_follows_local_redirect_to_final_failure() {
        let port = spawn_http_probe_redirect_server().await;
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap();
        let container = probe_test_container();
        let pod = Pod::new(
            "http-local-redirect",
            PodSpec {
                containers: vec![container.clone()],
                ..Default::default()
            },
        );

        let healthy = runtime
            .probe_container(&pod, &container, &http_probe("/redirect-local", port))
            .await
            .unwrap();

        assert!(
            !healthy,
            "same-host redirects must be followed and use the final response"
        );
    }

    #[tokio::test]
    async fn http_probe_non_local_redirect_is_warning_success() {
        let port = spawn_http_probe_redirect_server().await;
        let storage = Arc::new(StorageBackend::new_memory());
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap()
        .with_event_recorder(Arc::clone(&storage));
        let container = probe_test_container();
        let mut pod = Pod::new(
            "http-nonlocal-redirect",
            PodSpec {
                containers: vec![container.clone()],
                ..Default::default()
            },
        );
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = "pod-uid".to_string();

        let healthy = runtime
            .probe_container(&pod, &container, &http_probe("/redirect-nonlocal", port))
            .await
            .unwrap();

        assert!(
            healthy,
            "different-host redirects terminate with warning but count as success"
        );
        let obj = crate::events::container_object_reference(&pod, &container.name);
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, crate::events::CONTAINER_PROBE_WARNING)
        );
        let event: Event = storage.get(&key).await.expect("warning event recorded");
        assert_eq!(event.reason, crate::events::CONTAINER_PROBE_WARNING);
        assert!(event
            .message
            .contains("Probe terminated redirects, Response body:"));
    }
    #[test]
    fn runtime_version_string_uses_cri_name_and_version() {
        // The k8s `containerRuntimeVersion` is `<runtime_name>://<runtime_version>`,
        // sourced from the CRI Version RPC — never a hardcoded literal.
        let v = v1::VersionResponse {
            version: "0.1.0".to_string(),
            runtime_name: "containerd-rs".to_string(),
            runtime_version: "0.1.2".to_string(),
            runtime_api_version: "v1".to_string(),
        };
        assert_eq!(format_runtime_version(&v), "containerd-rs://0.1.2");
    }

    #[test]
    fn find_container_searches_regular_and_init() {
        let spec = PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                ..Default::default()
            }],
            init_containers: Some(vec![Container {
                name: "setup".to_string(),
                termination_message_policy: Some("FallbackToLogsOnError".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let pod = Pod::new("web", spec);
        assert_eq!(
            find_container(&pod, "app").map(|c| c.name.as_str()),
            Some("app")
        );
        assert_eq!(
            find_container(&pod, "setup").and_then(|c| c.termination_message_policy.as_deref()),
            Some("FallbackToLogsOnError")
        );
        assert!(find_container(&pod, "missing").is_none());
    }

    #[test]
    fn read_log_tail_keeps_trailing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.log");
        let big = "a".repeat(status::MAX_TERMINATION_MESSAGE_LENGTH + 50) + "END";
        std::fs::write(&path, &big).unwrap();
        let tail = read_log_tail(path.to_str().unwrap()).unwrap();
        assert_eq!(tail.len(), status::MAX_TERMINATION_MESSAGE_LENGTH);
        assert!(tail.ends_with("END"));

        // Missing / empty logs yield None.
        assert!(read_log_tail(dir.path().join("nope.log").to_str().unwrap()).is_none());
        let empty = dir.path().join("empty.log");
        std::fs::write(&empty, "").unwrap();
        assert!(read_log_tail(empty.to_str().unwrap()).is_none());
    }

    #[test]
    fn service_env_derives_port_from_actual_service_port() {
        let map: HashMap<String, String> =
            service_env_vars("10.96.0.1", "8080").into_iter().collect();
        let get = |k: &str| map.get(k).map(String::as_str);

        // Link-var names embed the actual port, never a hardcoded 443.
        assert!(!map.keys().any(|k| k.contains("443")));
        assert_eq!(
            get("KUBERNETES_PORT_8080_TCP"),
            Some("tcp://10.96.0.1:8080")
        );
        assert_eq!(get("KUBERNETES_PORT_8080_TCP_PROTO"), Some("tcp"));
        assert_eq!(get("KUBERNETES_PORT_8080_TCP_PORT"), Some("8080"));
        assert_eq!(get("KUBERNETES_PORT_8080_TCP_ADDR"), Some("10.96.0.1"));
        assert_eq!(get("KUBERNETES_PORT"), Some("tcp://10.96.0.1:8080"));

        // Core + named-port alias (the Service port is named "https").
        assert_eq!(get("KUBERNETES_SERVICE_HOST"), Some("10.96.0.1"));
        assert_eq!(get("KUBERNETES_SERVICE_PORT"), Some("8080"));
        assert_eq!(get("KUBERNETES_SERVICE_PORT_HTTPS"), Some("8080"));
    }
}
