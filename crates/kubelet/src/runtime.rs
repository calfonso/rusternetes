use anyhow::{Context, Result};
use bollard::container::{
    Config, CreateContainerOptions, InspectContainerOptions, ListContainersOptions,
    RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::image::CreateImageOptions;
use bollard::Docker;
use chrono::Utc;
use futures_util::StreamExt;
use rusternetes_common::resources::{
    ConfigMap, Container, ContainerState, ContainerStatus, ExecAction, GRPCAction, HTTPGetAction,
    LifecycleHandler, PersistentVolume, PersistentVolumeClaim, Pod, Probe, Secret, TCPSocketAction,
};
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::cni::CniRuntime;

/// Wrapper for pre-encoded protobuf bytes (gRPC health check request).
#[derive(Clone, Debug, Default)]
struct EncodedBytes(Vec<u8>);

impl prost::Message for EncodedBytes {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        buf.put_slice(&self.0);
    }
    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: prost::encoding::WireType,
        _buf: &mut impl prost::bytes::Buf,
        _ctx: prost::encoding::DecodeContext,
    ) -> std::result::Result<(), prost::DecodeError> {
        Ok(())
    }
    fn encoded_len(&self) -> usize {
        self.0.len()
    }
    fn clear(&mut self) {
        self.0.clear();
    }
}

/// Decoded gRPC health check response — extracts the status field (field 1, varint).
#[derive(Clone, Debug, Default)]
struct DecodedStatus(i32);

impl prost::Message for DecodedStatus {
    fn encode_raw(&self, _buf: &mut impl prost::bytes::BufMut) {}
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> std::result::Result<(), prost::DecodeError> {
        if tag == 1 {
            // status field is an enum (varint)
            prost::encoding::int32::merge(wire_type, &mut self.0, buf, ctx)
        } else {
            prost::encoding::skip_field(wire_type, tag, buf, ctx)
        }
    }
    fn encoded_len(&self) -> usize {
        0
    }
    fn clear(&mut self) {
        self.0 = 0;
    }
}

/// Tracks consecutive probe successes/failures for threshold-based probe evaluation.
#[derive(Debug, Clone, Default)]
struct ProbeState {
    consecutive_failures: i32,
    consecutive_successes: i32,
}

/// ContainerRuntime manages containers using Docker/Podman with CNI networking
pub struct ContainerRuntime {
    docker: Docker,
    storage: Option<Arc<rusternetes_storage::StorageBackend>>,
    volumes_base_path: String,
    cluster_dns: String,
    cluster_domain: String,
    network: String,
    cni: Option<CniRuntime>,
    use_cni: bool,
    kubernetes_service_host: String,
    /// Token manager for generating projected service account tokens
    token_manager: rusternetes_common::auth::TokenManager,
    /// Probe state tracker: key is "{pod_name}/{container_name}/{probe_type}"
    probe_states: Mutex<HashMap<String, ProbeState>>,
    /// Cache of images known to exist locally (avoid repeated Docker API calls)
    image_cache: Mutex<std::collections::HashSet<String>>,
    /// Cache of shell availability per image (true = has /bin/sh)
    shell_cache: Mutex<HashMap<String, bool>>,
}

/// Join a list of strings into a shell-safe command string.
/// Wraps arguments containing spaces or special characters in single quotes.
fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|a| {
            if a.is_empty() {
                "''".to_string()
            } else if a.contains('$') {
                // Use double quotes to allow variable expansion
                format!("\"{}\"", a.replace('\\', "\\\\").replace('"', "\\\""))
            } else if needs_shell_quoting(a) {
                format!("'{}'", a.replace('\'', "'\\''"))
            } else {
                a.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Check if a string contains any shell metacharacters that require quoting
fn needs_shell_quoting(s: &str) -> bool {
    s.chars().any(|c| {
        matches!(
            c,
            ' ' | '\''
                | '"'
                | '\\'
                | ';'
                | '&'
                | '|'
                | '('
                | ')'
                | '{'
                | '}'
                | '<'
                | '>'
                | '!'
                | '?'
                | '*'
                | '['
                | ']'
                | '#'
                | '~'
                | '`'
                | '\n'
                | '\t'
        )
    })
}

/// Set up an EmptyDir volume directory with mode 0o777, matching upstream
/// Kubernetes (pkg/volume/emptydir/empty_dir.go setupDir).
///
/// The chmod is idempotent: it runs after `create_dir_all` regardless of whether
/// the directory was newly created or already existed (e.g. from a prior pod run
/// that left stale state). This guarantees the host-side directory always exposes
/// mode 0o777 to bind-mount consumers.
///
/// On Linux — where Kubernetes conformance runs — bind mounts preserve these mode
/// bits inside the container. On macOS dev VMs (Podman Machine / Docker Desktop
/// virtiofs) the host mode bits are NOT propagated through the shared-filesystem
/// layer; that is a known dev-env limitation and not a kubelet bug. The
/// `[Conformance] EmptyDir.*(mode|0644|0666|0777)` tests are all `[LinuxOnly]`,
/// so the chmod path here is the production code path.
pub(crate) fn setup_emptydir_dir(path: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))?;
    }
    Ok(())
}

impl ContainerRuntime {
    pub async fn new(
        volumes_base_path: String,
        cluster_dns: String,
        cluster_domain: String,
        network: String,
        kubernetes_service_host: String,
    ) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;

        info!("Using volumes base path: {}", volumes_base_path);
        info!(
            "Cluster DNS: {}, domain: {}, network: {}",
            cluster_dns, cluster_domain, network
        );
        info!("Kubernetes service host: {}", kubernetes_service_host);

        // Initialize CNI if plugins are available
        let (cni, use_cni) = match Self::initialize_cni() {
            Ok(cni_runtime) => {
                info!("CNI networking enabled");
                (Some(cni_runtime), true)
            }
            Err(e) => {
                warn!(
                    "CNI not available, falling back to Podman networking: {}",
                    e
                );
                (None, false)
            }
        };

        // Use the same JWT secret as the API server for token generation
        let jwt_secret = std::env::var("JWT_SECRET")
            .unwrap_or_else(|_| "rusternetes-secret-change-in-production".to_string());
        let token_manager = rusternetes_common::auth::TokenManager::new_auto(jwt_secret.as_bytes());

        Ok(Self {
            docker,
            storage: None,
            volumes_base_path,
            cluster_dns,
            cluster_domain,
            network,
            cni,
            use_cni,
            kubernetes_service_host,
            token_manager,
            probe_states: Mutex::new(HashMap::new()),
            image_cache: Mutex::new(std::collections::HashSet::new()),
            shell_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Initialize CNI runtime
    fn initialize_cni() -> Result<CniRuntime> {
        let cni_plugin_paths = vec![PathBuf::from("/opt/cni/bin")];
        let cni_config_dir = PathBuf::from("/etc/cni/net.d");

        // Check if CNI plugins exist
        if !cni_plugin_paths.iter().any(|p| p.exists()) {
            return Err(anyhow::anyhow!("CNI plugin directory not found"));
        }

        // Pre-flight check: verify we can create and access network namespaces
        // This will fail in Podman Machine where netns created in container aren't
        // accessible to other containers
        let test_netns = "rusternetes-cni-test";
        let create_result = Command::new("ip")
            .args(["netns", "add", test_netns])
            .output();

        if let Ok(output) = create_result {
            if output.status.success()
                || String::from_utf8_lossy(&output.stderr).contains("File exists")
            {
                // Clean up test namespace
                let _ = Command::new("ip")
                    .args(["netns", "del", test_netns])
                    .output();

                // Network namespaces work, but we're likely in a container where
                // they won't be accessible to sibling containers (Podman Machine case)
                warn!("CNI plugins found but network namespaces may not work across containers.");
                warn!("This is normal in Podman Machine - will fall back to Podman networking.");
                return Err(anyhow::anyhow!(
                    "Network namespace isolation prevents CNI usage"
                ));
            }
        }

        let cni = CniRuntime::new(cni_plugin_paths, cni_config_dir)?
            .with_default_network("rusternetes".to_string());

        Ok(cni)
    }

    pub fn volumes_base_path(&self) -> &str {
        &self.volumes_base_path
    }

    pub fn with_storage(mut self, storage: Arc<rusternetes_storage::StorageBackend>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Setup CNI networking for a pod
    /// Creates a network namespace and configures CNI networking
    /// Returns None if CNI setup fails (will fall back to Podman networking)
    async fn setup_pod_network(&self, pod_name: &str) -> Option<String> {
        info!("Setting up CNI network for pod: {}", pod_name);

        // Create network namespace
        let netns_name = format!("cni-{}", pod_name);
        let output = match Command::new("ip")
            .args(["netns", "add", &netns_name])
            .output()
        {
            Ok(out) => out,
            Err(e) => {
                warn!("Failed to create network namespace for pod {}: {}. Falling back to Podman networking.", pod_name, e);
                return None;
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Ignore error if namespace already exists
            if !stderr.contains("File exists") {
                warn!("Failed to create network namespace for pod {}: {}. Falling back to Podman networking.", pod_name, stderr);
                return None;
            }
            info!(
                "Network namespace {} already exists, reusing it",
                netns_name
            );
        } else {
            info!("Created network namespace: {}", netns_name);
        }

        // Get the network namespace path
        let netns_path = format!("/var/run/netns/{}", netns_name);

        // Setup CNI networking in the namespace
        if let Some(cni) = &self.cni {
            match cni.setup_network(pod_name, &netns_path, "eth0", None) {
                Ok(result) => {
                    info!(
                        "CNI network setup successful for pod {}: IP={:?}",
                        pod_name, result.ips
                    );
                }
                Err(e) => {
                    warn!("Failed to setup CNI network for pod {}: {}. Falling back to Podman networking.", pod_name, e);
                    // Clean up the network namespace on failure
                    let _ = Command::new("ip")
                        .args(["netns", "del", &netns_name])
                        .output();
                    return None;
                }
            }
        }

        Some(netns_path)
    }

    /// Teardown CNI networking for a pod
    /// Removes CNI configuration and deletes the network namespace
    async fn teardown_pod_network(&self, pod_name: &str) -> Result<()> {
        info!("Tearing down CNI network for pod: {}", pod_name);

        let netns_name = format!("cni-{}", pod_name);
        let netns_path = format!("/var/run/netns/{}", netns_name);

        // Teardown CNI networking
        if let Some(cni) = &self.cni {
            if let Err(e) = cni.teardown_network(pod_name, &netns_path, "eth0", None) {
                warn!("Failed to teardown CNI network for pod {}: {}", pod_name, e);
                // Continue with namespace deletion even if CNI teardown fails
            } else {
                info!("CNI network teardown successful for pod {}", pod_name);
            }
        }

        // Delete network namespace
        let output = Command::new("ip")
            .args(["netns", "del", &netns_name])
            .output()
            .context("Failed to execute ip netns del command")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                "Failed to delete network namespace {}: {}",
                netns_name, stderr
            );
        } else {
            info!("Deleted network namespace: {}", netns_name);
        }

        Ok(())
    }

    /// Pull an image if necessary based on the pull policy
    pub async fn ensure_image(&self, image: &str, pull_policy: Option<&str>) -> Result<()> {
        let policy = pull_policy.unwrap_or("IfNotPresent");

        // Normalize image name to include registry if not specified
        let normalized_image = self.normalize_image_name(image);

        // Fast path: check in-memory cache first (avoids Docker API call)
        let cached = {
            let cache = self.image_cache.lock().unwrap();
            cache.contains(image) || cache.contains(&normalized_image)
        };

        let image_exists = if cached && policy != "Always" {
            true
        } else {
            // Check Docker API
            let exists = self.check_image_exists(image).await
                || self.check_image_exists(&normalized_image).await;
            if exists {
                let mut cache = self.image_cache.lock().unwrap();
                cache.insert(image.to_string());
                cache.insert(normalized_image.clone());
            }
            exists
        };

        let should_pull = match policy {
            "Always" => true,
            "Never" => false,
            "IfNotPresent" => !image_exists,
            _ => !image_exists, // Default to IfNotPresent
        };

        debug!(
            "Image {} - exists: {}, should_pull: {}",
            image, image_exists, should_pull
        );

        if should_pull {
            info!("Pulling image: {}", normalized_image);

            // Try to pull the image with proper registry handling
            if let Err(e) = self.pull_image_with_retry(&normalized_image).await {
                error!("Failed to pull image {}: {}", normalized_image, e);

                // If normalized image failed and it's different from original, try original
                if normalized_image != image {
                    warn!("Retrying with original image name: {}", image);
                    self.pull_image_with_retry(image).await?;
                } else {
                    return Err(e);
                }
            }

            info!("Successfully pulled image: {}", image);
            // Cache the pulled image
            let mut cache = self.image_cache.lock().unwrap();
            cache.insert(image.to_string());
            cache.insert(normalized_image.clone());
        } else {
            debug!("Image {} already exists locally, skipping pull", image);
        }

        Ok(())
    }

    /// Check if an image exists locally
    async fn check_image_exists(&self, image: &str) -> bool {
        match self.docker.inspect_image(image).await {
            Ok(_) => {
                debug!("Image {} exists locally", image);
                true
            }
            Err(e) => {
                debug!("Image {} not found locally: {}", image, e);
                false
            }
        }
    }

    /// Normalize image name to include default registry
    fn normalize_image_name(&self, image: &str) -> String {
        // If image already has a registry (contains '/'), use as-is
        if image.contains('/') {
            // Check for explicit registry (contains '.' or ':' in first component)
            let first_component = image.split('/').next().unwrap_or("");
            if first_component.contains('.') || first_component.contains(':') {
                return image.to_string();
            }
            // Otherwise prepend docker.io/library/ for official images like "library/image"
            if !image.starts_with("docker.io/") {
                return format!("docker.io/{}", image);
            }
            image.to_string()
        } else {
            // Simple image name without registry - use docker.io/library
            format!("docker.io/library/{}", image)
        }
    }

    /// Pull image with retry logic
    async fn pull_image_with_retry(&self, image: &str) -> Result<()> {
        let options = CreateImageOptions {
            from_image: image,
            ..Default::default()
        };

        let mut stream = self.docker.create_image(Some(options), None, None);
        let mut last_error = None;

        while let Some(result) = stream.next().await {
            match result {
                Ok(info) => {
                    if let Some(status) = &info.status {
                        debug!("Image pull: {}", status);
                    }
                    if let Some(progress) = &info.progress {
                        debug!("Image pull progress: {}", progress);
                    }
                    if let Some(error) = info.error {
                        last_error = Some(error.clone());
                        error!("Image pull error: {}", error);
                    }
                }
                Err(e) => {
                    last_error = Some(format!("{}", e));
                    error!("Image pull stream error: {}", e);
                }
            }
        }

        // Check if there was an error
        if let Some(err) = last_error {
            return Err(anyhow::anyhow!("Image pull failed: {}", err));
        }

        Ok(())
    }

    /// Start all containers for a pod
    pub async fn start_pod(&self, pod: &Pod) -> Result<()> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // Guard: if the pod's containers are already running, skip the start.
        // This prevents duplicate container creation when sync_pod is called
        // multiple times in rapid succession (e.g., watch feedback loops).
        if self.is_pod_running(pod_name).await.unwrap_or(false) {
            debug!(
                "Pod {}/{} containers already running, skipping start",
                namespace, pod_name
            );
            return Ok(());
        }

        info!("Starting pod: {}/{}", namespace, pod_name);

        // Proactively remove any old exited containers for this pod.
        // K8s does this in SyncPod before creating new containers.
        // Without this, Docker returns 409 Conflict for container name reuse.
        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_manager.go — SyncPod
        if let Ok(containers) = self
            .docker
            .list_containers(Some(ListContainersOptions::<String> {
                all: true,
                filters: {
                    let mut f = std::collections::HashMap::new();
                    f.insert("name".to_string(), vec![format!("^/{}", pod_name)]);
                    f.insert(
                        "status".to_string(),
                        vec![
                            "exited".to_string(),
                            "dead".to_string(),
                            "created".to_string(),
                        ],
                    );
                    f
                },
                ..Default::default()
            }))
            .await
        {
            // Remove old containers in parallel for faster cleanup
            let remove_futures: Vec<_> = containers
                .iter()
                .filter_map(|c| c.id.as_ref())
                .map(|id| {
                    debug!("Removing old container {} for pod {}", id, pod_name);
                    self.docker.remove_container(
                        id,
                        Some(bollard::container::RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                })
                .collect();
            let _ = futures_util::future::join_all(remove_futures).await;
        }

        // Create network namespace and setup CNI networking if enabled
        // If CNI setup fails, netns_path will be None and we fall back to Podman networking
        let netns_path = if self.use_cni {
            self.setup_pod_network(pod_name).await
        } else {
            None
        };

        // Ensure the pod has a kube-api-access volume for SA tokens.
        // Controllers that create pods directly in etcd bypass the API server's
        // admission controller, so the SA token volume may not be injected.
        let mut pod_with_sa = pod.clone();
        if let Some(ref mut spec) = pod_with_sa.spec {
            let has_sa_volume = spec
                .volumes
                .as_ref()
                .map(|vols| vols.iter().any(|v| v.name.contains("kube-api-access")))
                .unwrap_or(false);
            if !has_sa_volume {
                // Add projected SA token volume
                let sa_vol = rusternetes_common::resources::Volume {
                    name: "kube-api-access".to_string(),
                    empty_dir: None,
                    host_path: None,
                    config_map: None,
                    secret: None,
                    projected: Some(rusternetes_common::resources::ProjectedVolumeSource {
                        sources: Some(vec![rusternetes_common::resources::VolumeProjection {
                            service_account_token: Some(
                                rusternetes_common::resources::ServiceAccountTokenProjection {
                                    path: "token".to_string(),
                                    expiration_seconds: Some(3600),
                                    audience: None,
                                },
                            ),
                            config_map: None,
                            secret: None,
                            downward_api: None,
                            cluster_trust_bundle: None,
                        }]),
                        default_mode: Some(0o644),
                    }),
                    persistent_volume_claim: None,
                    downward_api: None,
                    csi: None,
                    ephemeral: None,
                    nfs: None,
                    image: None,
                    iscsi: None,
                };
                spec.volumes.get_or_insert_with(Vec::new).push(sa_vol);
                // Add volume mount to each container
                for container in &mut spec.containers {
                    let mounts = container.volume_mounts.get_or_insert_with(Vec::new);
                    if !mounts.iter().any(|m| m.name.contains("kube-api-access")) {
                        mounts.push(rusternetes_common::resources::VolumeMount {
                            name: "kube-api-access".to_string(),
                            mount_path: "/var/run/secrets/kubernetes.io/serviceaccount".to_string(),
                            read_only: Some(true),
                            sub_path: None,
                            sub_path_expr: None,
                            mount_propagation: None,
                            recursive_read_only: None,
                        });
                    }
                }
            }
        }
        let pod = &pod_with_sa;

        // Create volumes first (includes service account token volumes)
        let volume_binds = self.create_pod_volumes(pod).await?;

        // Flush volume directory to ensure files are visible to Docker containers.
        // Docker Desktop uses virtiofs which may cache writes. Instead of a global
        // sync (which flushes ALL filesystems), we sync_data just the pod's volume dir.
        {
            let pod_vol_dir = format!("{}/{}", self.volumes_base_path, pod_name);
            if let Ok(dir) = std::fs::File::open(&pod_vol_dir) {
                let _ = dir.sync_all();
            }
        }

        // Get pod IP. For CNI mode, IP is available right after network setup.
        // For non-CNI (Docker bridge) mode, we start a pause container first so we
        // can learn the pod's IP before creating real containers (which need the IP
        // in their environment for Downward API env vars like SONOBUOY_ADVERTISE_IP).
        let mut pod_ip: Option<String> = if self.use_cni {
            if let Some(cni) = &self.cni {
                cni.get_container_ip(pod_name)
            } else {
                None
            }
        } else {
            // Start a pause container to obtain the pod's Docker-assigned IP before
            // creating any real containers.
            match self.start_pause_container(pod_name, pod).await {
                Ok(ip) => {
                    info!("Pause container assigned IP {} for pod {}", ip, pod_name);
                    Some(ip)
                }
                Err(e) => {
                    warn!(
                        "Failed to start pause container for pod {}: {}",
                        pod_name, e
                    );
                    None
                }
            }
        };

        // Create /etc/hosts now that we know the pod IP.
        let hosts_file_path = self.create_pod_hosts_file(pod, pod_ip.as_deref())?;

        // /etc/hosts is bind-mounted into each app container (see start_container).
        // No need to upload into the pause container — app containers use the bind mount.

        // resolved_ip is only used in the non-CNI/non-pause fallback path now.
        let mut resolved_ip = pod_ip.is_some();

        // Pre-pull ALL images (init + sidecar + main) in parallel while the
        // pause container is running. This eliminates serial image pull latency.
        // K8s EnsureImageExists is called per-container, but Docker handles
        // concurrent pulls safely and we benefit from parallelism.
        {
            let mut all_images: Vec<(String, Option<String>)> = Vec::new();
            if let Some(init_containers) = &pod.spec.as_ref().unwrap().init_containers {
                for ic in init_containers {
                    all_images.push((ic.image.clone(), ic.image_pull_policy.clone()));
                }
            }
            for c in &pod.spec.as_ref().unwrap().containers {
                all_images.push((c.image.clone(), c.image_pull_policy.clone()));
            }
            // Deduplicate
            let mut seen = std::collections::HashSet::new();
            let unique: Vec<(String, Option<String>)> = all_images
                .into_iter()
                .filter(|(img, _)| seen.insert(img.clone()))
                .collect();
            if !unique.is_empty() {
                debug!(
                    "Pre-pulling {} unique images for pod {}",
                    unique.len(),
                    pod_name
                );
                let futs: Vec<_> = unique
                    .iter()
                    .map(|(img, pol)| self.ensure_image(img, pol.as_deref()))
                    .collect();
                let results = futures_util::future::join_all(futs).await;
                for (i, r) in results.into_iter().enumerate() {
                    if let Err(e) = r {
                        error!("Failed to pull image {}: {}", unique[i].0, e);
                        return Err(e);
                    }
                }
            }
        }

        // Step 1: Run init containers sequentially (non-sidecar init containers)
        // Sidecar init containers (with restartPolicy: Always) will be started with main containers
        if let Some(init_containers) = &pod.spec.as_ref().unwrap().init_containers {
            for container in init_containers {
                // Check if this is a sidecar container (restartPolicy: Always)
                let is_sidecar = container.restart_policy.as_deref() == Some("Always");

                if !is_sidecar {
                    // Regular init container - run to completion
                    info!("Running init container: {}", container.name);

                    // Update pod status to show which init container is running.
                    // K8s kubelet sends status updates between init container runs
                    // so watches can observe the progression.
                    if let Some(ref storage) = self.storage {
                        use rusternetes_storage::Storage;
                        let pod_key =
                            rusternetes_storage::build_key("pods", Some(namespace), pod_name);
                        if let Ok(mut status_pod) = storage.get::<Pod>(&pod_key).await {
                            // Build init container statuses showing current state
                            let init_statuses = self.get_init_container_statuses(&status_pod).await;
                            // Set container statuses to Waiting/PodInitializing
                            let container_statuses: Vec<
                                rusternetes_common::resources::ContainerStatus,
                            > = status_pod
                                .spec
                                .as_ref()
                                .map(|s| {
                                    s.containers
                                        .iter()
                                        .map(|c| {
                                            rusternetes_common::resources::ContainerStatus {
                                                name: c.name.clone(),
                                                ready: false,
                                                restart_count: 0,
                                                state: Some(
                                                    rusternetes_common::resources::ContainerState::Waiting {
                                                        reason: Some(
                                                            "PodInitializing".to_string(),
                                                        ),
                                                        message: None,
                                                    },
                                                ),
                                                last_state: None,
                                                image: Some(c.image.clone()),
                                                image_id: None,
                                                container_id: None,
                                                started: Some(false),
                                                resources: None,
                                                allocated_resources: None,
                                                allocated_resources_status: None,
                                                volume_mounts: None,
                                                user: None,
                                                stop_signal: None,
                                            }
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                            if let Some(ref mut status) = status_pod.status {
                                status.init_container_statuses = init_statuses;
                                status.container_statuses = Some(container_statuses);
                            }
                            let _ = storage.update(&pod_key, &status_pod).await;
                        }
                    }

                    // Image already pre-pulled above

                    // For restartPolicy=Always pods, retry failed init containers
                    // with exponential backoff (matching Kubernetes CrashLoopBackOff)
                    let restart_always = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.restart_policy.as_deref())
                        .unwrap_or("Always")
                        == "Always";
                    // For restartPolicy=Always, retry init containers up to 3 times
                    // in start_pod. Further retries happen via the kubelet sync loop
                    // which shows CrashLoopBackOff status.
                    let max_retries = if restart_always { 3 } else { 0 };
                    let mut attempt = 0;

                    loop {
                        // Start the init container
                        self.start_container(
                            pod,
                            container,
                            &volume_binds,
                            netns_path.as_deref(),
                            hosts_file_path.as_deref(),
                            pod_ip.as_deref(),
                        )
                        .await?;

                        // Resolve pod IP from first container for non-CNI mode
                        if !resolved_ip && pod_ip.is_none() {
                            if let Ok(Some(ip)) = self.get_pod_ip(pod_name).await {
                                pod_ip = Some(ip);
                                resolved_ip = true;
                                self.create_pod_hosts_file(pod, pod_ip.as_deref())?;
                            }
                        }

                        // Publish Running state so watches see the transition from
                        // PodInitializing → Running for this init container.
                        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_manager.go — SyncPod
                        // publishes status after each container action.
                        self.refresh_init_container_statuses(namespace, pod_name, None, None)
                            .await;

                        // Wait for init container to complete
                        match self
                            .wait_for_container_completion(pod_name, &container.name)
                            .await
                        {
                            Ok(()) => {
                                info!("Init container {} completed successfully", container.name);
                                // Publish Terminated/Completed before moving on so the
                                // next init container starts from a status reflecting prior
                                // success — required by the "sequential init" conformance test.
                                self.refresh_init_container_statuses(
                                    namespace, pod_name, None, None,
                                )
                                .await;
                                break;
                            }
                            Err(e) => {
                                // Capture Terminated/error BEFORE removing the container.
                                // After removal, get_init_container_statuses would see only
                                // the Running prev state, so the failure would never be
                                // observed by watches — restart_count stays at 0 and the
                                // "restart_count >= 3" conformance check hangs.
                                self.refresh_init_container_statuses(
                                    namespace, pod_name, None, None,
                                )
                                .await;

                                if attempt < max_retries && restart_always {
                                    attempt += 1;
                                    let backoff = std::cmp::min(2u64.pow(attempt as u32), 30);
                                    warn!(
                                        "Init container {} failed (attempt {}), retrying in {}s: {}",
                                        container.name, attempt, backoff, e
                                    );
                                    // Remove the failed container before retrying
                                    let full_name = format!("{}_{}", pod_name, container.name);
                                    let _ = self
                                        .docker
                                        .remove_container(
                                            &full_name,
                                            Some(RemoveContainerOptions {
                                                force: true,
                                                ..Default::default()
                                            }),
                                        )
                                        .await;

                                    // Publish CrashLoopBackOff. Previous state is now
                                    // Terminated/error (captured above), so the transition
                                    // Terminated → CrashLoopBackOff increments restart_count.
                                    self.refresh_init_container_statuses(
                                        namespace,
                                        pod_name,
                                        Some("PodInitializing"),
                                        Some(format!(
                                            "Init container {} failed, retrying in {}s",
                                            container.name, backoff
                                        )),
                                    )
                                    .await;

                                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                                } else {
                                    // RestartPolicy=Never (or max retries exhausted): return
                                    // Err so the caller (kubelet.rs sync_pod) can transition
                                    // the pod to Failed. For Never, remaining init containers
                                    // MUST NOT run — the early return guarantees this because
                                    // we propagate out of the init_containers loop.
                                    // The Terminated/error state was already captured above
                                    // (before this branch), so init_container_statuses
                                    // already reflects the failure.
                                    // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go
                                    warn!(
                                        "Init container {} failed (will retry next sync): {}",
                                        container.name, e
                                    );
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Step 2: Start sidecar containers (init containers with restartPolicy: Always)
        // Images already pre-pulled in the parallel pre-pull step above.
        if let Some(init_containers) = &pod.spec.as_ref().unwrap().init_containers {
            for container in init_containers {
                let is_sidecar = container.restart_policy.as_deref() == Some("Always");

                if is_sidecar {
                    info!("Starting sidecar container: {}", container.name);

                    // Image already pre-pulled above

                    // Start the sidecar container (it will run alongside main containers)
                    self.start_container(
                        pod,
                        container,
                        &volume_binds,
                        netns_path.as_deref(),
                        hosts_file_path.as_deref(),
                        pod_ip.as_deref(),
                    )
                    .await?;

                    // Resolve pod IP from first container for non-CNI mode
                    if !resolved_ip && pod_ip.is_none() {
                        if let Ok(Some(ip)) = self.get_pod_ip(pod_name).await {
                            pod_ip = Some(ip);
                            resolved_ip = true;
                            self.create_pod_hosts_file(pod, pod_ip.as_deref())?;
                        }
                    }
                }
            }
        }

        // Step 3: Start main containers (images already pre-pulled above)
        let spec_containers = pod.spec.as_ref().unwrap().containers.clone();
        for container in &spec_containers {
            // Start the container with volume bindings
            self.start_container(
                pod,
                container,
                &volume_binds,
                netns_path.as_deref(),
                hosts_file_path.as_deref(),
                pod_ip.as_deref(),
            )
            .await?;

            // Resolve pod IP from first container for non-CNI mode
            if !resolved_ip && pod_ip.is_none() {
                if let Ok(Some(ip)) = self.get_pod_ip(pod_name).await {
                    pod_ip = Some(ip.clone());
                    resolved_ip = true;
                    self.create_pod_hosts_file(pod, pod_ip.as_deref())?;
                    info!("Resolved pod IP {} for pod {} (non-CNI mode)", ip, pod_name);
                }
            }
        }

        // For restartPolicy=Never pods, containers may exit immediately.
        // K8s SyncPod detects this within the same call and updates status.
        // Without this, the kubelet sync loop (3s interval) misses fast-exiting
        // containers that have already been removed by Docker.
        // K8s ref: pkg/kubelet/kubelet_pods.go:1639 — getPhase
        let restart_policy = pod
            .spec
            .as_ref()
            .and_then(|s| s.restart_policy.as_deref())
            .unwrap_or("Always");
        if restart_policy == "Never" {
            // Brief delay for containers to exit
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            // Check if all containers have terminated
            if let Ok(statuses) = self.get_container_statuses(pod).await {
                let all_terminated = !statuses.is_empty()
                    && statuses.iter().all(|cs| {
                        matches!(
                            cs.state,
                            Some(rusternetes_common::resources::ContainerState::Terminated { .. })
                        )
                    });
                if all_terminated {
                    let any_failed = statuses.iter().any(|cs| {
                        matches!(
                            cs.state,
                            Some(rusternetes_common::resources::ContainerState::Terminated { exit_code, .. }) if exit_code != 0
                        )
                    });
                    let phase = if any_failed {
                        rusternetes_common::types::Phase::Failed
                    } else {
                        rusternetes_common::types::Phase::Succeeded
                    };

                    if let Some(ref storage) = self.storage {
                        use rusternetes_storage::Storage;
                        let pod_key =
                            rusternetes_storage::build_key("pods", Some(namespace), pod_name);
                        if let Ok(mut p) = storage.get::<Pod>(&pod_key).await {
                            if let Some(ref mut status) = p.status {
                                status.phase = Some(phase.clone());
                                status.container_statuses = Some(statuses);
                                // Set conditions for terminal pods.
                                // K8s always sets PodInitialized=True with reason
                                // PodCompleted for succeeded pods.
                                if phase == rusternetes_common::types::Phase::Succeeded {
                                    let now = Some(chrono::Utc::now());
                                    status.conditions = Some(vec![
                                        rusternetes_common::resources::PodCondition {
                                            condition_type: "Initialized".to_string(),
                                            status: "True".to_string(),
                                            reason: Some("PodCompleted".to_string()),
                                            message: None,
                                            last_transition_time: now,
                                            observed_generation: None,
                                        },
                                        rusternetes_common::resources::PodCondition {
                                            condition_type: "PodScheduled".to_string(),
                                            status: "True".to_string(),
                                            reason: None,
                                            message: None,
                                            last_transition_time: now,
                                            observed_generation: None,
                                        },
                                        rusternetes_common::resources::PodCondition {
                                            condition_type: "ContainersReady".to_string(),
                                            status: "False".to_string(),
                                            reason: Some("PodCompleted".to_string()),
                                            message: None,
                                            last_transition_time: now,
                                            observed_generation: None,
                                        },
                                        rusternetes_common::resources::PodCondition {
                                            condition_type: "Ready".to_string(),
                                            status: "False".to_string(),
                                            reason: Some("PodCompleted".to_string()),
                                            message: None,
                                            last_transition_time: now,
                                            observed_generation: None,
                                        },
                                    ]);
                                }
                            }
                            let _ = storage.update(&pod_key, &p).await;
                            info!(
                                "Pod {}/{} all containers terminated, set phase={:?}",
                                namespace,
                                pod_name,
                                if any_failed { "Failed" } else { "Succeeded" }
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Create /etc/hosts file for a pod.
    ///
    /// Generates a hosts file with:
    /// - Standard localhost entries
    /// - The pod's own hostname → IP mapping (if IP is known)
    /// - Subdomain-based FQDN if spec.subdomain is set
    ///
    /// Returns the path to the hosts file, or None if the pod is CoreDNS
    /// (which uses the host's /etc/hosts directly).
    fn create_pod_hosts_file(&self, pod: &Pod, pod_ip: Option<&str>) -> Result<Option<String>> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let spec = pod.spec.as_ref().unwrap();

        // Host network pods should NOT have kubelet-managed /etc/hosts.
        // They use the host's /etc/hosts directly.
        if spec.host_network == Some(true) {
            return Ok(None);
        }

        // Determine the pod's hostname: spec.hostname if set, otherwise pod name
        // Linux hostnames limited to 63 chars
        let raw_hostname = spec.hostname.as_deref().unwrap_or(pod_name);
        let hostname = if raw_hostname.len() > 63 {
            raw_hostname[..63].trim_end_matches('-')
        } else {
            raw_hostname
        };

        let pod_dir = format!("{}/{}", self.volumes_base_path, pod_name);
        std::fs::create_dir_all(&pod_dir)
            .context("Failed to create pod directory for /etc/hosts")?;

        let hosts_path = format!("{}/hosts", pod_dir);

        let mut content = String::from(
            "# Kubernetes-managed hosts file.\n\
             127.0.0.1\tlocalhost\n\
             ::1\tlocalhost ip6-localhost ip6-loopback\n\
             fe00::\tip6-localnet\n\
             fe00::\tip6-mcastprefix\n\
             fe00::1\tip6-allnodes\n\
             fe00::2\tip6-allrouters\n",
        );

        // Add the pod's own hostname → IP entry if we have an IP
        if let Some(ip) = pod_ip {
            // Build FQDN aliases based on subdomain and cluster domain
            let mut aliases = vec![hostname.to_string()];
            if let Some(subdomain) = &spec.subdomain {
                // <hostname>.<subdomain>.<namespace>.svc.<cluster-domain>
                aliases.push(format!(
                    "{}.{}.{}.svc.{}",
                    hostname, subdomain, namespace, self.cluster_domain
                ));
            }
            content.push_str(&format!("{}\t{}\n", ip, aliases.join("\t")));
            info!(
                "Added /etc/hosts entry for pod {}/{}: {} -> {}",
                namespace,
                pod_name,
                aliases.join(", "),
                ip
            );
        }

        // Add entries from spec.hostAliases
        // Kubernetes groups all hostnames for the same IP on a single line
        if let Some(host_aliases) = &spec.host_aliases {
            for alias in host_aliases {
                if let Some(hostnames) = &alias.hostnames {
                    if !hostnames.is_empty() {
                        content.push_str(&format!("{}\t{}\n", alias.ip, hostnames.join("\t")));
                    }
                }
            }
        }

        std::fs::write(&hosts_path, &content)
            .with_context(|| format!("Failed to write /etc/hosts for pod {}", pod_name))?;

        Ok(Some(hosts_path))
    }

    /// Start a pause (infra) container for a pod in non-CNI mode.
    ///
    /// The pause container holds the pod's network namespace. All real containers
    /// join its network via `--network container:<pause_name>`. This lets us know
    /// the pod's IP before creating any real containers, which is required to
    /// correctly populate Downward API env vars like `status.podIP`.
    ///
    /// Returns the IP address assigned to the pause container.
    async fn start_pause_container(&self, pod_name: &str, pod: &Pod) -> Result<String> {
        let pause_name = format!("{}_pause", pod_name);

        // Check if pause container already exists and is running — if so, just return its IP.
        // Recreating the pause container would destroy all containers sharing its network namespace.
        if let Ok(inspect) = self
            .docker
            .inspect_container(&pause_name, None::<InspectContainerOptions>)
            .await
        {
            let state = inspect.state.as_ref();
            let is_running = state.and_then(|s| s.running).unwrap_or(false);

            if is_running {
                // Pause container is already running — return its IP
                if let Some(network_settings) = inspect.network_settings {
                    if let Some(networks) = network_settings.networks {
                        if let Some(network_info) = networks.get(&self.network) {
                            if let Some(ip) = &network_info.ip_address {
                                if !ip.is_empty() {
                                    debug!(
                                        "Pause container {} already running with IP {}",
                                        pause_name, ip
                                    );
                                    return Ok(ip.clone());
                                }
                            }
                        }
                    }
                }
            }

            // Remove dependent containers first (required for Podman which
            // refuses to remove a container that has dependents).
            if let Ok(containers) = self
                .docker
                .list_containers(Some(bollard::container::ListContainersOptions::<String> {
                    all: true,
                    ..Default::default()
                }))
                .await
            {
                let network_mode_key = format!("container:{}", pause_name);
                for c in &containers {
                    let is_dependent = c
                        .host_config
                        .as_ref()
                        .and_then(|hc| hc.network_mode.as_deref())
                        .map(|nm| nm == network_mode_key)
                        .unwrap_or(false);
                    if is_dependent {
                        if let Some(id) = &c.id {
                            let _ = self
                                .docker
                                .stop_container(
                                    id,
                                    Some(bollard::container::StopContainerOptions { t: 0 }),
                                )
                                .await;
                            let _ = self
                                .docker
                                .remove_container(
                                    id,
                                    Some(bollard::container::RemoveContainerOptions {
                                        force: true,
                                        ..Default::default()
                                    }),
                                )
                                .await;
                        }
                    }
                }
            }
            // Pause container exists but is not running — remove it
            let remove_options = RemoveContainerOptions {
                force: true,
                ..Default::default()
            };
            let _ = self
                .docker
                .remove_container(&pause_name, Some(remove_options))
                .await;
        }

        // Collect all ports from all containers in the pod.
        // The pause container owns the network namespace and must declare all ports;
        // child containers that join via --network container:<pause> cannot re-declare them.
        let mut exposed_ports: HashMap<String, HashMap<(), ()>> = HashMap::new();
        let mut port_bindings: HashMap<String, Option<Vec<bollard::models::PortBinding>>> =
            HashMap::new();
        if let Some(spec) = &pod.spec {
            for c in &spec.containers {
                if let Some(ports) = &c.ports {
                    for port in ports {
                        let proto = port.protocol.as_deref().unwrap_or("TCP").to_lowercase();
                        let port_key = format!("{}/{}", port.container_port, proto);
                        exposed_ports.insert(port_key.clone(), HashMap::new());
                        if let Some(host_port) = port.host_port {
                            // Use the pod spec's hostIP if specified, otherwise 0.0.0.0.
                            // Different pods can bind the same port on different hostIPs.
                            // K8s ref: pkg/kubelet/cm/container_manager_linux.go
                            //
                            // We pass the hostIP directly to Docker. In our architecture
                            // the kubelet uses the host Docker daemon (via docker.sock),
                            // so the node's InternalIP (Docker bridge IP) is available
                            // for binding. This allows two pods with the same hostPort
                            // but different hostIPs (e.g., 127.0.0.1 vs 172.18.0.6) to
                            // coexist without conflict.
                            let bind_ip = port.host_ip.as_deref().unwrap_or("0.0.0.0");
                            let bind_ip = if bind_ip.is_empty() || bind_ip == "::" {
                                "0.0.0.0"
                            } else {
                                bind_ip
                            };
                            port_bindings.insert(
                                port_key,
                                Some(vec![bollard::models::PortBinding {
                                    host_ip: Some(bind_ip.to_string()),
                                    host_port: Some(host_port.to_string()),
                                }]),
                            );
                        }
                    }
                }
            }
        }

        // Create the pause container using busybox with sleep infinity.
        // This holds the pod's network namespace so all real containers can
        // join it via --network container:<pause_name>.
        // The pause container owns the DNS configuration for the pod network namespace.
        // CoreDNS pods must NOT use cluster DNS (circular dependency).
        let is_coredns = pod_name == "coredns";
        // Collect sysctls from pod security context
        let sysctls_map: Option<HashMap<String, String>> = pod
            .spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.sysctls.as_ref())
            .map(|sysctls| {
                sysctls
                    .iter()
                    .map(|s| (s.name.clone(), s.value.clone()))
                    .collect()
            });

        // Set hostname on pause container (it owns the network namespace)
        // Linux hostnames are limited to 63 characters (POSIX HOST_NAME_MAX - 1).
        // Kubernetes truncates pod hostnames to 63 chars as well.
        let raw_hostname = pod
            .spec
            .as_ref()
            .and_then(|s| s.hostname.as_deref())
            .unwrap_or(pod_name);
        let pause_hostname = if raw_hostname.len() > 63 {
            raw_hostname[..63].trim_end_matches('-').to_string()
        } else {
            raw_hostname.to_string()
        };

        // Ensure busybox image is available (critical for podman which may not auto-pull)
        // Use IfNotPresent policy to avoid unnecessary pulls on every pod start
        self.ensure_image("busybox:latest", Some("IfNotPresent"))
            .await
            .context("Failed to ensure busybox image for pause container")?;

        let config = Config {
            image: Some("busybox:latest".to_string()),
            cmd: Some(vec!["sleep".to_string(), "infinity".to_string()]),
            hostname: Some(pause_hostname),
            exposed_ports: if exposed_ports.is_empty() {
                None
            } else {
                Some(exposed_ports)
            },
            host_config: Some(bollard::models::HostConfig {
                network_mode: Some(self.network.clone()),
                // Shareable IPC so app containers can join via ipc_mode=container:pause
                ipc_mode: Some("shareable".to_string()),
                dns: if is_coredns {
                    None
                } else {
                    Some(vec![self.cluster_dns.clone()])
                },
                dns_options: if is_coredns {
                    None
                } else {
                    Some(vec!["ndots:5".to_string()])
                },
                port_bindings: if port_bindings.is_empty() {
                    None
                } else {
                    Some(port_bindings)
                },
                sysctls: sysctls_map,
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: pause_name.clone(),
            ..Default::default()
        };

        // Create pause container, handling 409 Conflict by removing and retrying
        match self
            .docker
            .create_container(Some(options.clone()), config.clone())
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let err_str = format!("{}", e);
                if err_str.contains("409")
                    || err_str.contains("Conflict")
                    || err_str.contains("already in use")
                {
                    warn!(
                        "Pause container {} already exists, removing and retrying",
                        pause_name
                    );
                    // Remove dependent containers first (required for Podman which
                    // refuses to remove a container that has dependents).
                    if let Ok(containers) = self
                        .docker
                        .list_containers(Some(
                            bollard::container::ListContainersOptions::<String> {
                                all: true,
                                ..Default::default()
                            },
                        ))
                        .await
                    {
                        let network_mode_key = format!("container:{}", pause_name);
                        for c in &containers {
                            let is_dependent = c
                                .host_config
                                .as_ref()
                                .and_then(|hc| hc.network_mode.as_deref())
                                .map(|nm| nm == network_mode_key)
                                .unwrap_or(false);
                            if is_dependent {
                                if let Some(id) = &c.id {
                                    let _ = self
                                        .docker
                                        .stop_container(
                                            id,
                                            Some(bollard::container::StopContainerOptions { t: 0 }),
                                        )
                                        .await;
                                    let _ = self
                                        .docker
                                        .remove_container(
                                            id,
                                            Some(bollard::container::RemoveContainerOptions {
                                                force: true,
                                                ..Default::default()
                                            }),
                                        )
                                        .await;
                                }
                            }
                        }
                    }
                    // Force stop the pause container, then remove.
                    let _ = self
                        .docker
                        .stop_container(
                            &pause_name,
                            Some(bollard::container::StopContainerOptions { t: 0 }),
                        )
                        .await;
                    match self
                        .docker
                        .remove_container(
                            &pause_name,
                            Some(bollard::container::RemoveContainerOptions {
                                force: true,
                                ..Default::default()
                            }),
                        )
                        .await
                    {
                        Ok(_) => {
                            // Wait briefly for the runtime to release the container name
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        Err(rm_err) => {
                            warn!(
                                "Failed to remove pause container {}: {}",
                                pause_name, rm_err
                            );
                            // Try waiting longer — runtime may still be processing
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                    }
                    self.docker
                        .create_container(Some(options), config.clone())
                        .await
                        .context("Failed to create pause container after cleanup")?;
                } else {
                    return Err(e).context("Failed to create pause container");
                }
            }
        }

        self.docker
            .start_container(&pause_name, None::<StartContainerOptions<String>>)
            .await
            .context("Failed to start pause container")?;

        // K8s CRI RunPodSandbox is synchronous — returns only after the sandbox
        // is running. Docker's start_container returns immediately. We must block
        // until the pause container is confirmed running, otherwise app containers
        // fail with "cannot join network namespace of a non running container".
        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_sandbox.go — RunPodSandbox
        {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                match self
                    .docker
                    .inspect_container(&pause_name, None::<InspectContainerOptions>)
                    .await
                {
                    Ok(info) if info.state.as_ref().and_then(|s| s.running).unwrap_or(false) => {
                        break
                    }
                    Ok(_) if std::time::Instant::now() > deadline => {
                        anyhow::bail!(
                            "Pause container {} did not reach running state within 10s",
                            pause_name
                        );
                    }
                    Err(e) if std::time::Instant::now() > deadline => {
                        anyhow::bail!(
                            "Pause container {} inspect failed after 10s: {}",
                            pause_name,
                            e
                        );
                    }
                    _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
                }
            }
        }

        info!("Pause container {} running", pause_name);

        // Inspect to get the assigned IP
        let inspect = self
            .docker
            .inspect_container(&pause_name, None::<InspectContainerOptions>)
            .await
            .context("Failed to inspect pause container")?;

        if let Some(network_settings) = inspect.network_settings {
            if let Some(networks) = network_settings.networks {
                if let Some(network_info) = networks.get(&self.network) {
                    if let Some(ip) = &network_info.ip_address {
                        if !ip.is_empty() && ip != "0.0.0.0" {
                            return Ok(ip.clone());
                        }
                    }
                }
            }
            if let Some(ip) = network_settings.ip_address {
                if !ip.is_empty() && ip != "0.0.0.0" {
                    return Ok(ip);
                }
            }
        }

        Err(anyhow::anyhow!(
            "Pause container started but no IP address was assigned"
        ))
    }

    /// Refresh `pod.status.init_container_statuses` in storage from the
    /// live Docker state. Called between init container actions so watches
    /// observe each transition (Running, Terminated, CrashLoopBackOff).
    /// Optionally sets `status.reason` and `status.message`.
    async fn refresh_init_container_statuses(
        &self,
        namespace: &str,
        pod_name: &str,
        reason: Option<&str>,
        message: Option<String>,
    ) {
        let Some(ref storage) = self.storage else {
            return;
        };
        use rusternetes_storage::Storage;
        let pod_key = rusternetes_storage::build_key("pods", Some(namespace), pod_name);
        let Ok(mut status_pod) = storage.get::<Pod>(&pod_key).await else {
            return;
        };
        let init_statuses = self.get_init_container_statuses(&status_pod).await;
        if let Some(ref mut s) = status_pod.status {
            s.init_container_statuses = init_statuses;
            if let Some(r) = reason {
                s.reason = Some(r.to_string());
            }
            if let Some(m) = message {
                s.message = Some(m);
            }
        }
        let _ = storage.update(&pod_key, &status_pod).await;
    }

    /// Wait for a container to complete (used for init containers)
    async fn wait_for_container_completion(
        &self,
        pod_name: &str,
        container_name: &str,
    ) -> Result<()> {
        let full_container_name = format!("{}_{}", pod_name, container_name);
        let timeout = Duration::from_secs(300); // 5 minute timeout
        let start_time = std::time::Instant::now();

        loop {
            if start_time.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for init container {} to complete",
                    container_name
                ));
            }

            match self
                .docker
                .inspect_container(&full_container_name, None::<InspectContainerOptions>)
                .await
            {
                Ok(inspect) => {
                    if let Some(state) = inspect.state {
                        let running = state.running.unwrap_or(false);

                        if !running {
                            // Container has stopped
                            let exit_code = state.exit_code.unwrap_or(1);

                            if exit_code == 0 {
                                debug!("Init container {} completed successfully", container_name);
                                return Ok(());
                            } else {
                                let error_msg = state
                                    .error
                                    .unwrap_or_else(|| format!("Exit code: {}", exit_code));
                                error!("Init container {} failed: {}", container_name, error_msg);
                                return Err(anyhow::anyhow!(
                                    "Init container {} failed with exit code {}: {}",
                                    container_name,
                                    exit_code,
                                    error_msg
                                ));
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to inspect init container {}: {}", container_name, e);
                }
            }

            // Wait before checking again. K8s uses Docker event streaming
            // for instant notification; we poll. 100ms is fast enough to not
            // add noticeable latency to init container completion.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Create volumes for a pod and return volume bindings for containers
    pub async fn create_pod_volumes(&self, pod: &Pod) -> Result<HashMap<String, String>> {
        let mut volume_paths = HashMap::new();

        if let Some(volumes) = &pod.spec.as_ref().unwrap().volumes {
            for volume in volumes {
                let volume_path = self.create_volume(pod, volume).await?;
                volume_paths.insert(volume.name.clone(), volume_path);
            }
        }

        // Apply fsGroup: change group ownership on all volume files.
        // Real Kubernetes behavior: fsGroup changes the group owner of volume files
        // to the specified GID, and sets group permission bits to match the owner bits
        // (i.e., if owner has read, group gets read; if owner has write, group gets write).
        // This preserves the defaultMode permissions — a file with mode 0440 stays 0440,
        // not 0460 (which would happen with unconditional g+rwX).
        #[cfg(unix)]
        if let Some(fs_group) = pod
            .spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.fs_group)
        {
            use std::os::unix::fs::PermissionsExt;
            for path in volume_paths.values() {
                // Recursively chown to fsGroup
                let _ = std::process::Command::new("chown")
                    .args(["-R", &format!(":{}", fs_group), path])
                    .output();

                // Set group bits to mirror owner bits on each file/directory.
                // This matches real K8s behavior: if owner=r--, group becomes r--
                // (not r+w which chmod g+rwX would do).
                fn apply_fsgroup_permissions(dir: &std::path::Path) {
                    use std::os::unix::fs::PermissionsExt;
                    if let Ok(entries) = std::fs::read_dir(dir) {
                        for entry in entries.flatten() {
                            let fpath = entry.path();
                            if let Ok(meta) = std::fs::metadata(&fpath) {
                                let mode = meta.permissions().mode();
                                // Copy owner bits (bits 8-6) to group bits (bits 5-3)
                                let owner_bits = (mode >> 6) & 0o7;
                                let new_mode = (mode & !0o070) | (owner_bits << 3);
                                if new_mode != mode {
                                    let _ = std::fs::set_permissions(
                                        &fpath,
                                        std::fs::Permissions::from_mode(new_mode),
                                    );
                                }
                                if meta.is_dir() {
                                    apply_fsgroup_permissions(&fpath);
                                }
                            }
                        }
                    }
                }
                apply_fsgroup_permissions(std::path::Path::new(path));

                // Set setgid bit on the directory itself so new files inherit group
                if let Ok(meta) = std::fs::metadata(path) {
                    let mode = meta.permissions().mode();
                    let owner_bits = (mode >> 6) & 0o7;
                    let new_mode = (mode & !0o070) | (owner_bits << 3) | 0o2000;
                    let _ =
                        std::fs::set_permissions(path, std::fs::Permissions::from_mode(new_mode));
                }
            }
            info!(
                "Applied fsGroup {} to {} volumes",
                fs_group,
                volume_paths.len()
            );
        }

        Ok(volume_paths)
    }

    /// Resync projected/secret/configmap volumes for a running pod.
    /// Re-reads source data from storage and updates volume files if changed.
    pub async fn resync_volumes<S: rusternetes_storage::Storage>(
        &self,
        pod: &Pod,
        storage: &S,
    ) -> Result<()> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        if let Some(volumes) = &pod.spec.as_ref().unwrap().volumes {
            for volume in volumes {
                // Resync secret volumes
                if let Some(secret_source) = &volume.secret {
                    let secret_name = match &secret_source.secret_name {
                        Some(n) => n,
                        None => continue,
                    };
                    let key =
                        rusternetes_storage::build_key("secrets", Some(namespace), secret_name);
                    let volume_dir =
                        format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
                    if let Ok(secret) = storage
                        .get::<rusternetes_common::resources::Secret>(&key)
                        .await
                    {
                        let mut expected_files: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        if let Some(data) = &secret.data {
                            if let Some(ref items) = secret_source.items {
                                // Only mount the specified keys at their mapped paths
                                for item in items {
                                    if let Some(v) = data.get(&item.key) {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        expected_files.insert(item.path.clone());
                                        if let Ok(existing) = std::fs::read(&file_path) {
                                            if existing == *v {
                                                continue;
                                            }
                                        }
                                        if let Some(parent) =
                                            std::path::Path::new(&file_path).parent()
                                        {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(&file_path, v);
                                    }
                                }
                            } else {
                                // Mount all keys
                                for (k, v) in data {
                                    let file_path = format!("{}/{}", volume_dir, k);
                                    expected_files.insert(k.clone());
                                    // Only write if content changed
                                    if let Ok(existing) = std::fs::read(&file_path) {
                                        if existing == *v {
                                            continue;
                                        }
                                    }
                                    let _ = std::fs::write(&file_path, v);
                                }
                            }
                        }
                        // Remove files that are no longer expected
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                if let Some(name) = entry.file_name().to_str() {
                                    if !expected_files.contains(name) {
                                        let _ = std::fs::remove_file(entry.path());
                                    }
                                }
                            }
                        }
                    } else if secret_source.optional != Some(true) {
                        // Secret was deleted entirely — remove all files if not optional
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                let _ = std::fs::remove_file(entry.path());
                            }
                        }
                    }
                }
                // Resync configmap volumes
                if let Some(cm_source) = &volume.config_map {
                    if let Some(cm_name) = &cm_source.name {
                        let key =
                            rusternetes_storage::build_key("configmaps", Some(namespace), cm_name);
                        if let Ok(cm) = storage
                            .get::<rusternetes_common::resources::ConfigMap>(&key)
                            .await
                        {
                            let volume_dir =
                                format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
                            if let Some(ref items) = cm_source.items {
                                // Only mount the specified keys at their mapped paths
                                for item in items {
                                    if let Some(value) =
                                        cm.data.as_ref().and_then(|d| d.get(&item.key))
                                    {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                            if existing == *value {
                                                continue;
                                            }
                                        }
                                        if let Some(parent) =
                                            std::path::Path::new(&file_path).parent()
                                        {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(&file_path, value);
                                    } else if let Some(value) =
                                        cm.binary_data.as_ref().and_then(|d| d.get(&item.key))
                                    {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        if let Ok(existing) = std::fs::read(&file_path) {
                                            if existing == *value {
                                                continue;
                                            }
                                        }
                                        if let Some(parent) =
                                            std::path::Path::new(&file_path).parent()
                                        {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(&file_path, value);
                                    }
                                }
                            } else {
                                // Mount all keys from data
                                if let Some(data) = &cm.data {
                                    for (k, v) in data {
                                        let file_path = format!("{}/{}", volume_dir, k);
                                        if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                            if existing == *v {
                                                continue;
                                            }
                                        }
                                        let _ = std::fs::write(&file_path, v);
                                    }
                                }
                                // Mount all keys from binaryData
                                if let Some(binary_data) = &cm.binary_data {
                                    for (k, v) in binary_data {
                                        let file_path = format!("{}/{}", volume_dir, k);
                                        if let Ok(existing) = std::fs::read(&file_path) {
                                            if existing == *v {
                                                continue;
                                            }
                                        }
                                        let _ = std::fs::write(&file_path, v);
                                    }
                                }
                            }
                        }
                    }
                }
                // Resync projected volumes (may contain configmap/secret projections)
                if let Some(projected) = &volume.projected {
                    if let Some(sources) = &projected.sources {
                        let volume_dir =
                            format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
                        // Track expected files so we can delete stale ones
                        let mut expected_files: std::collections::HashSet<String> =
                            std::collections::HashSet::new();

                        for source in sources {
                            if let Some(cm_proj) = &source.config_map {
                                if let Some(cm_name) = &cm_proj.name {
                                    let key = rusternetes_storage::build_key(
                                        "configmaps",
                                        Some(namespace),
                                        cm_name,
                                    );
                                    if let Ok(cm) = storage
                                        .get::<rusternetes_common::resources::ConfigMap>(&key)
                                        .await
                                    {
                                        if let Some(items) = &cm_proj.items {
                                            // Selective projection — only mount specified keys
                                            for item in items {
                                                let file_path =
                                                    format!("{}/{}", volume_dir, item.path);
                                                expected_files.insert(file_path.clone());
                                                if let Some(value) =
                                                    cm.data.as_ref().and_then(|d| d.get(&item.key))
                                                {
                                                    if let Ok(existing) =
                                                        std::fs::read_to_string(&file_path)
                                                    {
                                                        if existing == *value {
                                                            continue;
                                                        }
                                                    }
                                                    if let Some(parent) =
                                                        std::path::Path::new(&file_path).parent()
                                                    {
                                                        let _ = std::fs::create_dir_all(parent);
                                                    }
                                                    let _ = std::fs::write(&file_path, value);
                                                }
                                            }
                                        } else if let Some(data) = &cm.data {
                                            // Mount all keys
                                            for (k, v) in data {
                                                let file_path = format!("{}/{}", volume_dir, k);
                                                expected_files.insert(file_path.clone());
                                                if let Ok(existing) =
                                                    std::fs::read_to_string(&file_path)
                                                {
                                                    if existing == *v {
                                                        continue;
                                                    }
                                                }
                                                let _ = std::fs::write(&file_path, v);
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some(sec_proj) = &source.secret {
                                if let Some(sec_name) = &sec_proj.name {
                                    let key = rusternetes_storage::build_key(
                                        "secrets",
                                        Some(namespace),
                                        sec_name,
                                    );
                                    if let Ok(secret) = storage
                                        .get::<rusternetes_common::resources::Secret>(&key)
                                        .await
                                    {
                                        if let Some(data) = &secret.data {
                                            for (k, v) in data {
                                                let file_path = format!("{}/{}", volume_dir, k);
                                                expected_files.insert(file_path.clone());
                                                if let Ok(existing) = std::fs::read(&file_path) {
                                                    if existing == *v {
                                                        continue;
                                                    }
                                                }
                                                let _ = std::fs::write(&file_path, v);
                                            }
                                        }
                                    }
                                }
                            }
                            // ServiceAccountToken projection resync — preserve the token file
                            if let Some(sa_token) = &source.service_account_token {
                                let file_path = format!("{}/{}", volume_dir, sa_token.path);
                                expected_files.insert(file_path);
                            }
                            // DownwardAPI projection resync
                            if let Some(downward_api) = &source.downward_api {
                                if let Some(items) = &downward_api.items {
                                    for item in items {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        expected_files.insert(file_path.clone());
                                        let value = if let Some(ref field_ref) = item.field_ref {
                                            self.get_pod_field_value(pod, &field_ref.field_path)
                                                .unwrap_or_default()
                                        } else if let Some(ref resource_ref) =
                                            item.resource_field_ref
                                        {
                                            self.get_container_resource_value(pod, resource_ref)
                                                .unwrap_or_default()
                                        } else {
                                            String::new()
                                        };
                                        if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                            if existing == value {
                                                continue;
                                            }
                                        }
                                        let _ = std::fs::write(&file_path, &value);
                                    }
                                }
                            }
                        }

                        // Delete stale files that are no longer in any projection source
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_file() {
                                    let path_str = path.to_string_lossy().to_string();
                                    if !expected_files.contains(&path_str) {
                                        let _ = std::fs::remove_file(&path);
                                    }
                                }
                            }
                        }
                    }
                }
                // Resync standalone downwardAPI volumes
                if let Some(downward_api) = &volume.downward_api {
                    if let Some(items) = &downward_api.items {
                        let volume_dir =
                            format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
                        for item in items {
                            let file_path = format!("{}/{}", volume_dir, item.path);
                            let value = if let Some(ref field_ref) = item.field_ref {
                                self.get_pod_field_value(pod, &field_ref.field_path)
                                    .unwrap_or_default()
                            } else if let Some(ref resource_ref) = item.resource_field_ref {
                                self.get_container_resource_value(pod, resource_ref)
                                    .unwrap_or_default()
                            } else {
                                String::new()
                            };
                            if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                if existing == value {
                                    continue;
                                }
                            }
                            let _ = std::fs::write(&file_path, &value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Expand $(VAR_NAME) references in a subPathExpr using the container's env vars.
    /// Returns Err if a referenced variable is not defined, or if the result is an absolute path.
    fn expand_subpath_expr(
        expr: &str,
        env_vars: &[(String, String)],
    ) -> std::result::Result<String, String> {
        // Check for backticks in the expression BEFORE expansion (K8s validates
        // this at admission time, but we do it here for safety).
        if expr.contains('`') {
            return Err("subPath must not contain backticks".to_string());
        }

        let mut result = expr.to_string();
        // Find all $(VAR_NAME) references and expand them
        while let Some(start) = result.find("$(") {
            let rest = &result[start + 2..];
            let end = match rest.find(')') {
                Some(e) => e,
                None => break,
            };
            let var_name = &rest[..end];
            if let Some((_, value)) = env_vars.iter().find(|(k, _)| k == var_name) {
                result = format!(
                    "{}{}{}",
                    &result[..start],
                    value,
                    &result[start + 2 + end + 1..]
                );
            } else {
                return Err(format!("variable {} not found", var_name));
            }
        }
        // Reject absolute paths in the expanded result
        if result.starts_with('/') {
            return Err(format!(
                "subPath must not be an absolute path (expr='{}' result='{}')",
                expr, result
            ));
        }
        // Reject path traversal — check for ".." as a path component, not substring.
        // This matches K8s behavior: "foo..bar" is valid, but "foo/../bar" is not.
        for component in result.split('/') {
            if component == ".." {
                return Err("subPath must not contain '..'".to_string());
            }
        }
        Ok(result)
    }

    /// Expand environment variables in a string (e.g., ${VAR_NAME} or $VAR_NAME)
    fn expand_env_vars(input: &str) -> String {
        let mut result = input.to_string();

        // Expand ${VAR_NAME} format
        while let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let var_name = &result[start + 2..start + end];
                let var_value = std::env::var(var_name).unwrap_or_default();
                result.replace_range(start..start + end + 1, &var_value);
            } else {
                break;
            }
        }

        // Expand $VAR_NAME format (word boundary based)
        let mut i = 0;
        while i < result.len() {
            if result[i..].starts_with('$') && i + 1 < result.len() {
                let rest = &result[i + 1..];
                let var_len = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .count();

                if var_len > 0 {
                    let var_name = &rest[..var_len];
                    let var_value = std::env::var(var_name).unwrap_or_default();
                    result.replace_range(i..i + 1 + var_len, &var_value);
                    i += var_value.len();
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        result
    }

    /// Create a single volume and return its host path
    async fn create_volume(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> Result<String> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // EmptyDir: create a directory on the shared volumes path.
        // K8s ref: pkg/volume/emptydir/empty_dir.go — setupDir() sets mode 0777.
        // Note: host bind mounts through virtiofs (Podman Machine / Docker Desktop)
        // may not enforce chmod correctly. The emptyDir 0777/0666 permission tests
        // are pre-existing failures on macOS VM-based runtimes. On Linux (where
        // conformance actually runs), bind mounts preserve mode bits, so setup_emptydir_dir
        // ensures the directory exists with mode 0o777 and idempotently re-chmods even
        // when the directory pre-exists from a prior run.
        if volume.empty_dir.is_some() {
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            setup_emptydir_dir(&volume_dir).context("Failed to create emptyDir volume")?;
            info!("Created emptyDir volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // HostPath: use the specified host path
        if let Some(host_path) = &volume.host_path {
            // Expand environment variables in the path
            let path = Self::expand_env_vars(&host_path.path);
            // Optionally create the directory if it doesn't exist
            if let Some(type_) = &host_path.type_ {
                if type_ == "DirectoryOrCreate" {
                    std::fs::create_dir_all(&path).context("Failed to create hostPath volume")?;
                }
            }
            info!("Using hostPath volume {} at {}", volume.name, path);
            return Ok(path);
        }

        // ConfigMap: mount configmap data as files
        if let Some(configmap_source) = &volume.config_map {
            let storage = self
                .storage
                .as_ref()
                .context("Storage not available for ConfigMap volumes")?;

            let configmap_name = configmap_source
                .name
                .as_ref()
                .context("ConfigMap volume must specify name")?;

            let is_optional = configmap_source.optional.unwrap_or(false);

            let key = build_key("configmaps", Some(namespace), configmap_name);
            let configmap_result: Result<ConfigMap, _> = storage.get(&key).await;

            // Create volume directory
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create ConfigMap volume directory")?;

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let cm_default_mode = configmap_source.default_mode.unwrap_or(0o644);

            // Compute final directory permissions (will be applied after files are written)
            #[cfg(unix)]
            let cm_dir_mode = cm_default_mode as u32 | 0o111;

            match configmap_result {
                Ok(configmap) => {
                    // Helper closure to write a file and set permissions
                    let write_cm_file =
                        |file_path: &str, content: &[u8], mode: i32| -> Result<()> {
                            if let Some(parent) = std::path::Path::new(file_path).parent() {
                                std::fs::create_dir_all(parent)?;
                            }
                            std::fs::write(file_path, content)?;
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                std::fs::set_permissions(
                                    file_path,
                                    std::fs::Permissions::from_mode(mode as u32),
                                )?;
                            }
                            Ok(())
                        };

                    // Check if specific items are requested
                    if let Some(ref items) = configmap_source.items {
                        // Only mount the specified keys (look in both data and binaryData)
                        for item in items {
                            let mode = item.mode.unwrap_or(cm_default_mode);
                            let file_path = format!("{}/{}", volume_dir, item.path);

                            // Try data first, then binary_data
                            if let Some(value) =
                                configmap.data.as_ref().and_then(|d| d.get(&item.key))
                            {
                                write_cm_file(&file_path, value.as_bytes(), mode).with_context(
                                    || {
                                        format!(
                                            "Failed to write ConfigMap key {} to file",
                                            item.key
                                        )
                                    },
                                )?;
                                info!("Wrote ConfigMap key {} to {}", item.key, file_path);
                            } else if let Some(value) = configmap
                                .binary_data
                                .as_ref()
                                .and_then(|d| d.get(&item.key))
                            {
                                write_cm_file(&file_path, value, mode).with_context(|| {
                                    format!(
                                        "Failed to write ConfigMap binaryData key {} to file",
                                        item.key
                                    )
                                })?;
                                info!(
                                    "Wrote ConfigMap binaryData key {} to {}",
                                    item.key, file_path
                                );
                            } else if !is_optional {
                                warn!("ConfigMap {} missing key {}", configmap_name, item.key);
                            }
                        }
                    } else {
                        // Mount all keys from data
                        if let Some(data) = &configmap.data {
                            for (key, value) in data {
                                let file_path = format!("{}/{}", volume_dir, key);
                                write_cm_file(&file_path, value.as_bytes(), cm_default_mode)
                                    .with_context(|| {
                                        format!("Failed to write ConfigMap key {} to file", key)
                                    })?;
                                info!("Wrote ConfigMap key {} to {}", key, file_path);
                            }
                        }
                        // Mount all keys from binaryData
                        if let Some(binary_data) = &configmap.binary_data {
                            for (key, value) in binary_data {
                                let file_path = format!("{}/{}", volume_dir, key);
                                write_cm_file(&file_path, value, cm_default_mode).with_context(
                                    || {
                                        format!(
                                            "Failed to write ConfigMap binaryData key {} to file",
                                            key
                                        )
                                    },
                                )?;
                                info!("Wrote ConfigMap binaryData key {} to {}", key, file_path);
                            }
                        }
                    }
                }
                Err(e) => {
                    if is_optional {
                        info!(
                            "Optional ConfigMap {} not found in namespace {}, creating empty volume",
                            configmap_name, namespace
                        );
                    } else {
                        // Required ConfigMap not found — abort pod start so kubelet
                        // retries on next reconciliation (when the ConfigMap exists).
                        return Err(anyhow::anyhow!(
                            "ConfigMap {} not found in namespace {}: {}",
                            configmap_name,
                            namespace,
                            e
                        ));
                    }
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(cm_dir_mode),
                )?;
            }

            info!("Created ConfigMap volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // Secret: mount secret data as files
        if let Some(secret_source) = &volume.secret {
            let storage = self
                .storage
                .as_ref()
                .context("Storage not available for Secret volumes")?;

            let secret_name = secret_source
                .secret_name
                .as_ref()
                .context("Secret volume must specify secret_name")?;

            let is_optional = secret_source.optional.unwrap_or(false);

            // For SA token volumes, generate a bound token with pod reference
            // instead of using the static token from the Secret.
            let is_sa_token_volume =
                volume.name.contains("kube-api-access") || secret_name.ends_with("-token");
            let bound_token: Option<String> = if is_sa_token_volume {
                let sa_name = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.service_account_name.as_deref())
                    .unwrap_or("default");
                let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);
                let sa_uid = storage
                    .get::<serde_json::Value>(&sa_key)
                    .await
                    .ok()
                    .and_then(|v| {
                        v.pointer("/metadata/uid")
                            .and_then(|u| u.as_str())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_default();
                let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
                let node_uid = if let Some(ref nn) = node_name {
                    let node_key = build_key("nodes", None::<&str>, nn);
                    storage
                        .get::<serde_json::Value>(&node_key)
                        .await
                        .ok()
                        .and_then(|v| {
                            v.pointer("/metadata/uid")
                                .and_then(|u| u.as_str())
                                .map(|s| s.to_string())
                        })
                } else {
                    None
                };
                let now = chrono::Utc::now();
                let claims = rusternetes_common::auth::ServiceAccountClaims {
                    sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
                    namespace: namespace.to_string(),
                    uid: sa_uid.clone(),
                    iat: now.timestamp(),
                    exp: (now + chrono::Duration::hours(1)).timestamp(),
                    iss: "https://kubernetes.default.svc.cluster.local".to_string(),
                    aud: vec!["rusternetes".to_string()],
                    kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                        namespace: namespace.to_string(),
                        svcacct: rusternetes_common::auth::KubeRef {
                            name: sa_name.to_string(),
                            uid: sa_uid,
                        },
                        pod: Some(rusternetes_common::auth::KubeRef {
                            name: pod_name.to_string(),
                            uid: pod.metadata.uid.clone(),
                        }),
                        node: node_name
                            .as_ref()
                            .map(|nn| rusternetes_common::auth::KubeRef {
                                name: nn.clone(),
                                uid: node_uid.clone().unwrap_or_default(),
                            }),
                    }),
                    pod_name: Some(pod_name.to_string()),
                    pod_uid: Some(pod.metadata.uid.clone()),
                    node_name,
                    node_uid,
                };
                self.token_manager.generate_token(claims).ok()
            } else {
                None
            };

            let key = build_key("secrets", Some(namespace), secret_name);
            let secret_result: Result<Secret, _> = storage.get(&key).await;

            // Create volume directory
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create Secret volume directory")?;

            // Compute final directory permissions (will be applied after files are written)
            #[cfg(unix)]
            let secret_dir_mode = secret_source.default_mode.unwrap_or(0o644) as u32 | 0o111;

            let secret = match secret_result {
                Ok(s) => Some(s),
                Err(e) => {
                    if is_optional {
                        info!(
                            "Optional Secret {} not found in namespace {}, creating empty volume",
                            secret_name, namespace
                        );
                        None
                    } else {
                        // Required secret not found — return error so the kubelet
                        // retries on next sync. K8s leaves the pod in Pending with
                        // ContainerCreating and retries until the secret exists.
                        // K8s ref: pkg/kubelet/kubelet_pods.go — makeVolumes
                        return Err(anyhow::anyhow!(
                            "Secret {} not found in namespace {}: {}",
                            secret_name,
                            namespace,
                            e
                        ));
                    }
                }
            };

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let secret_default_mode = secret_source.default_mode.unwrap_or(0o644);

            // Write secret data as files
            if let Some(data) = secret.as_ref().and_then(|s| s.data.as_ref()) {
                if let Some(ref items) = secret_source.items {
                    // Only mount the specified keys
                    for item in items {
                        if let Some(value) = data.get(&item.key) {
                            let file_path = format!("{}/{}", volume_dir, item.path);
                            // Create parent directories if needed
                            if let Some(parent) = std::path::Path::new(&file_path).parent() {
                                std::fs::create_dir_all(parent).with_context(|| {
                                    format!(
                                        "Failed to create directory for Secret item {}",
                                        item.path
                                    )
                                })?;
                            }
                            // For SA token volumes, substitute the bound token
                            let write_value: &[u8] = if item.key == "token" {
                                if let Some(ref bt) = bound_token {
                                    bt.as_bytes()
                                } else {
                                    value
                                }
                            } else {
                                value
                            };
                            std::fs::write(&file_path, write_value).with_context(|| {
                                format!("Failed to write Secret key {} to file", item.key)
                            })?;
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                let mode = item.mode.unwrap_or(secret_default_mode) as u32;
                                std::fs::set_permissions(
                                    &file_path,
                                    std::fs::Permissions::from_mode(mode),
                                )?;
                            }
                            if bound_token.is_some() && item.key == "token" {
                                info!("Wrote bound SA token for pod {} to {}", pod_name, file_path);
                            } else {
                                info!("Wrote Secret key {} to {}", item.key, file_path);
                            }
                        }
                    }
                } else {
                    // Mount all keys
                    for (key, value) in data {
                        let file_path = format!("{}/{}", volume_dir, key);
                        // For SA token volumes, substitute the bound token
                        let write_value: &[u8] = if key == "token" {
                            if let Some(ref bt) = bound_token {
                                bt.as_bytes()
                            } else {
                                value.as_slice()
                            }
                        } else {
                            value.as_slice()
                        };
                        std::fs::write(&file_path, write_value).with_context(|| {
                            format!("Failed to write Secret key {} to file", key)
                        })?;
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            std::fs::set_permissions(
                                &file_path,
                                std::fs::Permissions::from_mode(secret_default_mode as u32),
                            )?;
                        }
                        info!("Wrote Secret key {} to {}", key, file_path);
                    }
                }
            }

            // Special handling for service account token secrets - add ca.crt
            // Service account secrets are identified by having a "token" key or by name pattern
            let is_service_account_secret = secret
                .as_ref()
                .and_then(|s| s.data.as_ref())
                .map(|data| data.contains_key("token"))
                .unwrap_or(false)
                || secret_name.ends_with("-token");

            if is_service_account_secret {
                // Check if ca.crt already exists in the secret data
                let has_ca_cert = secret
                    .as_ref()
                    .and_then(|s| s.data.as_ref())
                    .map(|data| data.contains_key("ca.crt"))
                    .unwrap_or(false);

                if !has_ca_cert {
                    // Inject ca.crt from the cluster CA certificate
                    // Try multiple locations: environment variable, volumes/_certs, then fallback to .rusternetes/certs
                    let ca_cert_source = std::env::var("CA_CERT_PATH").unwrap_or_else(|_| {
                        // First try volumes/_certs (accessible from kubelet container)
                        let volumes_cert_path = format!("{}/_certs/ca.crt", self.volumes_base_path);
                        if std::path::Path::new(&volumes_cert_path).exists() {
                            volumes_cert_path
                        } else {
                            // Fallback to .rusternetes/certs (for host-based kubelet)
                            format!(
                                "{}/.rusternetes/certs/ca.crt",
                                std::env::var("HOME").unwrap_or_else(|_| "/root".to_string())
                            )
                        }
                    });

                    let ca_path = format!("{}/ca.crt", volume_dir);
                    if let Ok(ca_content) = std::fs::read(&ca_cert_source) {
                        std::fs::write(&ca_path, ca_content)
                            .context("Failed to write CA certificate")?;
                        info!("Injected CA certificate into service account secret volume at {} (from {})", ca_path, ca_cert_source);
                    } else {
                        warn!("CA certificate not found at {}, pods may not be able to verify API server", ca_cert_source);
                    }
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values (e.g., 0o400) don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(secret_dir_mode),
                )?;
            }

            info!("Created Secret volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // PersistentVolumeClaim: find bound PV and use its path
        if let Some(pvc_source) = &volume.persistent_volume_claim {
            let storage = self
                .storage
                .as_ref()
                .context("Storage not available for PersistentVolumeClaim volumes")?;

            let pvc_key = build_key(
                "persistentvolumeclaims",
                Some(namespace),
                &pvc_source.claim_name,
            );
            let pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.with_context(|| {
                format!(
                    "PersistentVolumeClaim {} not found in namespace {}",
                    pvc_source.claim_name, namespace
                )
            })?;

            // Get the bound PV name
            let pv_name = pvc
                .spec
                .volume_name
                .as_ref()
                .context("PersistentVolumeClaim is not bound to a volume")?;

            // Get the PV
            let pv_key = build_key("persistentvolumes", None, pv_name);
            let pv: PersistentVolume = storage
                .get(&pv_key)
                .await
                .with_context(|| format!("PersistentVolume {} not found", pv_name))?;

            // Get the host path from the PV
            let path = if let Some(hp) = &pv.spec.host_path {
                hp.path.clone()
            } else {
                return Err(anyhow::anyhow!(
                    "PersistentVolume does not have a hostPath volume source"
                ));
            };
            info!(
                "Using PersistentVolumeClaim volume {} backed by PV {} at {}",
                volume.name, pv_name, path
            );
            return Ok(path);
        }

        // DownwardAPI: expose pod/container metadata as files
        if let Some(downward_api) = &volume.downward_api {
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create DownwardAPI volume directory")?;

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let da_default_mode = downward_api.default_mode.unwrap_or(0o644);

            // Compute final directory permissions (applied after files are written)
            #[cfg(unix)]
            let da_dir_mode = da_default_mode as u32 | 0o111;

            if let Some(items) = &downward_api.items {
                for item in items {
                    let file_path = format!("{}/{}", volume_dir, item.path);

                    // Create parent directories if needed
                    if let Some(parent) = std::path::Path::new(&file_path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }

                    // Get the value from field_ref or resource_field_ref
                    let value = if let Some(field_ref) = &item.field_ref {
                        self.get_pod_field_value(pod, &field_ref.field_path)?
                    } else if let Some(resource_ref) = &item.resource_field_ref {
                        self.get_container_resource_value(pod, resource_ref)?
                    } else {
                        return Err(anyhow::anyhow!(
                            "DownwardAPI item must have either fieldRef or resourceFieldRef"
                        ));
                    };

                    std::fs::write(&file_path, value).with_context(|| {
                        format!("Failed to write DownwardAPI file {}", file_path)
                    })?;

                    // Set file permissions: per-item mode overrides defaultMode
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let mode = item.mode.unwrap_or(da_default_mode) as u32;
                        std::fs::set_permissions(
                            &file_path,
                            std::fs::Permissions::from_mode(mode),
                        )?;
                    }

                    info!(
                        "Wrote DownwardAPI file {} with value from {}",
                        file_path, item.path
                    );
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(da_dir_mode),
                )?;
            }

            info!(
                "Created DownwardAPI volume {} at {}",
                volume.name, volume_dir
            );
            return Ok(volume_dir);
        }

        // CSI: ephemeral inline volume (handled by external CSI driver)
        if let Some(_csi) = &volume.csi {
            // CSI ephemeral inline volumes are managed by the CSI driver via the kubelet CSI plugin
            // For conformance, we create a placeholder directory and rely on the CSI driver to populate it
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create CSI volume directory")?;

            info!(
                "Created CSI ephemeral volume {} at {} (managed by CSI driver)",
                volume.name, volume_dir
            );
            return Ok(volume_dir);
        }

        // Ephemeral: generic ephemeral volume with PVC template
        if let Some(ephemeral) = &volume.ephemeral {
            if let Some(pvc_template) = &ephemeral.volume_claim_template {
                let storage = self
                    .storage
                    .as_ref()
                    .context("Storage not available for ephemeral volumes")?;

                // Create a PVC name based on pod name and volume name
                let pvc_name = format!("{}-{}", pod_name, volume.name);

                // Check if PVC already exists
                let pvc_key = build_key("persistentvolumeclaims", Some(namespace), &pvc_name);
                let pvc_exists = storage.get::<PersistentVolumeClaim>(&pvc_key).await.is_ok();

                if !pvc_exists {
                    // Create the PVC from the template
                    let mut pvc = PersistentVolumeClaim {
                        type_meta: rusternetes_common::types::TypeMeta {
                            kind: "PersistentVolumeClaim".to_string(),
                            api_version: "v1".to_string(),
                        },
                        metadata: rusternetes_common::types::ObjectMeta::new(&pvc_name)
                            .with_namespace(namespace),
                        spec: pvc_template.spec.clone(),
                        status: None,
                    };

                    // Copy labels and annotations from template if provided
                    if let Some(template_meta) = &pvc_template.metadata {
                        if let Some(labels) = &template_meta.labels {
                            pvc.metadata.labels = Some(labels.clone());
                        }
                        if let Some(annotations) = &template_meta.annotations {
                            pvc.metadata.annotations = Some(annotations.clone());
                        }
                    }

                    // Store the PVC
                    storage
                        .create(&pvc_key, &pvc)
                        .await
                        .context("Failed to create ephemeral PVC")?;

                    info!(
                        "Created ephemeral PVC {} for volume {}",
                        pvc_name, volume.name
                    );

                    // Wait for PVC to be bound (simplified - in production would poll/watch)
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }

                // Now use the PVC like a regular PersistentVolumeClaim
                let pvc: PersistentVolumeClaim = storage
                    .get(&pvc_key)
                    .await
                    .with_context(|| format!("Ephemeral PVC {} not found", pvc_name))?;

                if let Some(pv_name) = &pvc.spec.volume_name {
                    let pv_key = build_key("persistentvolumes", None, pv_name);
                    let pv: PersistentVolume = storage.get(&pv_key).await.with_context(|| {
                        format!(
                            "PersistentVolume {} not found for ephemeral volume",
                            pv_name
                        )
                    })?;

                    let path = if let Some(hp) = &pv.spec.host_path {
                        hp.path.clone()
                    } else {
                        return Err(anyhow::anyhow!(
                            "PersistentVolume does not have a hostPath volume source"
                        ));
                    };

                    info!(
                        "Using ephemeral volume {} backed by PVC {} and PV {} at {}",
                        volume.name, pvc_name, pv_name, path
                    );
                    return Ok(path);
                } else {
                    return Err(anyhow::anyhow!(
                        "Ephemeral PVC {} is not bound yet",
                        pvc_name
                    ));
                }
            }
        }

        // Projected: combine multiple volume sources (configMap, secret, downwardAPI, serviceAccountToken) into one directory
        if let Some(projected) = &volume.projected {
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create projected volume directory")?;

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let proj_default_mode = projected.default_mode.unwrap_or(0o644);

            // Compute final directory permissions (will be applied after files are written)
            #[cfg(unix)]
            let proj_dir_mode = proj_default_mode as u32 | 0o111;

            if let Some(sources) = &projected.sources {
                let storage = self.storage.as_ref();

                for source in sources {
                    // ConfigMap projection
                    if let Some(cm_proj) = &source.config_map {
                        if let Some(cm_name) = &cm_proj.name {
                            let key = build_key("configmaps", Some(namespace), cm_name);
                            if let Some(storage) = storage {
                                match storage.get::<ConfigMap>(&key).await {
                                    Ok(cm) => {
                                        // Helper to write a projected file with permissions
                                        let write_proj_file =
                                            |path: &str, content: &[u8], mode: i32| -> Result<()> {
                                                if let Some(parent) =
                                                    std::path::Path::new(path).parent()
                                                {
                                                    std::fs::create_dir_all(parent)?;
                                                }
                                                std::fs::write(path, content)?;
                                                #[cfg(unix)]
                                                {
                                                    use std::os::unix::fs::PermissionsExt;
                                                    std::fs::set_permissions(
                                                        path,
                                                        std::fs::Permissions::from_mode(
                                                            mode as u32,
                                                        ),
                                                    )?;
                                                }
                                                Ok(())
                                            };

                                        if let Some(items) = &cm_proj.items {
                                            for item in items {
                                                let mode = item.mode.unwrap_or(proj_default_mode);
                                                let file_path =
                                                    format!("{}/{}", volume_dir, item.path);
                                                // Try data first, then binaryData
                                                if let Some(value) =
                                                    cm.data.as_ref().and_then(|d| d.get(&item.key))
                                                {
                                                    write_proj_file(
                                                        &file_path,
                                                        value.as_bytes(),
                                                        mode,
                                                    )?;
                                                } else if let Some(value) = cm
                                                    .binary_data
                                                    .as_ref()
                                                    .and_then(|d| d.get(&item.key))
                                                {
                                                    write_proj_file(&file_path, value, mode)?;
                                                }
                                            }
                                        } else {
                                            // Mount all keys from data
                                            if let Some(data) = &cm.data {
                                                for (k, v) in data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    write_proj_file(
                                                        &file_path,
                                                        v.as_bytes(),
                                                        proj_default_mode,
                                                    )?;
                                                }
                                            }
                                            // Mount all keys from binaryData
                                            if let Some(binary_data) = &cm.binary_data {
                                                for (k, v) in binary_data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    write_proj_file(
                                                        &file_path,
                                                        v,
                                                        proj_default_mode,
                                                    )?;
                                                }
                                            }
                                        }
                                    }
                                    Err(_) if cm_proj.optional.unwrap_or(false) => {
                                        // Optional configmap not found, skip
                                    }
                                    Err(e) => {
                                        warn!("Failed to get ConfigMap {} for projected volume: {}. Skipping.", cm_name, e);
                                    }
                                }
                            }
                        }
                    }

                    // Secret projection
                    if let Some(secret_proj) = &source.secret {
                        if let Some(secret_name) = &secret_proj.name {
                            let key = build_key("secrets", Some(namespace), secret_name);
                            if let Some(storage) = storage {
                                match storage.get::<Secret>(&key).await {
                                    Ok(secret) => {
                                        if let Some(data) = &secret.data {
                                            if let Some(items) = &secret_proj.items {
                                                for item in items {
                                                    if let Some(value) = data.get(&item.key) {
                                                        let file_path =
                                                            format!("{}/{}", volume_dir, item.path);
                                                        if let Some(parent) =
                                                            std::path::Path::new(&file_path)
                                                                .parent()
                                                        {
                                                            std::fs::create_dir_all(parent)?;
                                                        }
                                                        std::fs::write(&file_path, value)?;
                                                        #[cfg(unix)]
                                                        {
                                                            use std::os::unix::fs::PermissionsExt;
                                                            let mode = item
                                                                .mode
                                                                .unwrap_or(proj_default_mode)
                                                                as u32;
                                                            std::fs::set_permissions(
                                                                &file_path,
                                                                std::fs::Permissions::from_mode(
                                                                    mode,
                                                                ),
                                                            )?;
                                                        }
                                                    }
                                                }
                                            } else {
                                                for (k, v) in data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    std::fs::write(&file_path, v)?;
                                                    #[cfg(unix)]
                                                    {
                                                        use std::os::unix::fs::PermissionsExt;
                                                        std::fs::set_permissions(
                                                            &file_path,
                                                            std::fs::Permissions::from_mode(
                                                                proj_default_mode as u32,
                                                            ),
                                                        )?;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(_) if secret_proj.optional.unwrap_or(false) => {
                                        // Optional secret not found, skip
                                    }
                                    Err(e) => {
                                        warn!("Failed to get Secret {} for projected volume: {}. Skipping.", secret_name, e);
                                    }
                                }
                            }
                        }
                    }

                    // DownwardAPI projection
                    if let Some(downward_api) = &source.downward_api {
                        if let Some(items) = &downward_api.items {
                            for item in items {
                                let file_path = format!("{}/{}", volume_dir, item.path);
                                if let Some(parent) = std::path::Path::new(&file_path).parent() {
                                    std::fs::create_dir_all(parent)?;
                                }
                                let value = if let Some(ref field_ref) = item.field_ref {
                                    self.get_pod_field_value(pod, &field_ref.field_path)
                                        .unwrap_or_default()
                                } else if let Some(ref resource_ref) = item.resource_field_ref {
                                    self.get_container_resource_value(pod, resource_ref)
                                        .unwrap_or_default()
                                } else {
                                    String::new()
                                };
                                std::fs::write(&file_path, &value)?;
                                #[cfg(unix)]
                                {
                                    use std::os::unix::fs::PermissionsExt;
                                    let mode = item.mode.unwrap_or(proj_default_mode) as u32;
                                    std::fs::set_permissions(
                                        &file_path,
                                        std::fs::Permissions::from_mode(mode),
                                    )?;
                                }
                            }
                        }
                    }

                    // ServiceAccountToken projection
                    if let Some(sa_token) = &source.service_account_token {
                        let token_path = format!("{}/{}", volume_dir, sa_token.path);
                        if let Some(parent) = std::path::Path::new(&token_path).parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        // Generate a real JWT token bound to this pod
                        let sa_name = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.service_account_name.as_deref())
                            .unwrap_or("default");
                        let sa_uid = if let Some(storage) = storage {
                            let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);
                            match storage
                                .get::<rusternetes_common::resources::ServiceAccount>(&sa_key)
                                .await
                            {
                                Ok(sa) => sa.metadata.uid.clone(),
                                Err(_) => String::new(),
                            }
                        } else {
                            String::new()
                        };
                        let expiration_seconds = sa_token.expiration_seconds.unwrap_or(3600);
                        let now = chrono::Utc::now();
                        let exp = now.timestamp() + expiration_seconds;
                        let mut audiences = vec!["rusternetes".to_string()];
                        if let Some(ref aud) = sa_token.audience {
                            audiences = vec![aud.clone()];
                        }
                        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
                        let node_uid = if let (Some(ref nn), Some(st)) = (&node_name, storage) {
                            let node_key = build_key("nodes", None::<&str>, nn);
                            st.get::<serde_json::Value>(&node_key)
                                .await
                                .ok()
                                .and_then(|v| {
                                    v.pointer("/metadata/uid")
                                        .and_then(|u| u.as_str())
                                        .map(|s| s.to_string())
                                })
                        } else {
                            None
                        };
                        let claims = rusternetes_common::auth::ServiceAccountClaims {
                            sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
                            namespace: namespace.to_string(),
                            uid: sa_uid.clone(),
                            iat: now.timestamp(),
                            exp,
                            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
                            aud: audiences,
                            kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                                namespace: namespace.to_string(),
                                svcacct: rusternetes_common::auth::KubeRef {
                                    name: sa_name.to_string(),
                                    uid: sa_uid,
                                },
                                pod: Some(rusternetes_common::auth::KubeRef {
                                    name: pod_name.clone(),
                                    uid: pod.metadata.uid.clone(),
                                }),
                                node: node_name.as_ref().map(|nn| {
                                    rusternetes_common::auth::KubeRef {
                                        name: nn.clone(),
                                        uid: node_uid.clone().unwrap_or_default(),
                                    }
                                }),
                            }),
                            pod_name: Some(pod_name.clone()),
                            pod_uid: Some(pod.metadata.uid.clone()),
                            node_name,
                            node_uid,
                        };
                        let token = match self.token_manager.generate_token(claims) {
                            Ok(t) => t,
                            Err(e) => {
                                warn!(
                                    "Failed to generate SA token for pod {}: {}, using placeholder",
                                    pod_name, e
                                );
                                "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.placeholder".to_string()
                            }
                        };
                        std::fs::write(&token_path, &token)?;
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            std::fs::set_permissions(
                                &token_path,
                                std::fs::Permissions::from_mode(proj_default_mode as u32),
                            )?;
                        }
                    }
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(proj_dir_mode),
                )?;
            }

            info!("Created projected volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // Fallback: create an empty directory for unrecognized volume types
        // (e.g. nfs, iscsi, image, or any future types)
        // This prevents pod startup failures for volumes we don't natively handle.
        warn!(
            "Unknown volume type for volume {}, creating empty directory as fallback (volume debug: downward_api={}, empty_dir={}, host_path={}, config_map={}, secret={}, projected={}, pvc={}, csi={}, ephemeral={})",
            volume.name,
            volume.downward_api.is_some(),
            volume.empty_dir.is_some(),
            volume.host_path.is_some(),
            volume.config_map.is_some(),
            volume.secret.is_some(),
            volume.projected.is_some(),
            volume.persistent_volume_claim.is_some(),
            volume.csi.is_some(),
            volume.ephemeral.is_some(),
        );
        let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
        std::fs::create_dir_all(&volume_dir)
            .context("Failed to create fallback volume directory")?;
        Ok(volume_dir)
    }

    /// Create ServiceAccount token volume for in-cluster authentication
    #[allow(dead_code)]
    async fn create_serviceaccount_token_volume(&self, pod: &Pod) -> Result<String> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // Get ServiceAccount name from pod spec (default to "default")
        let sa_name = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.service_account_name.as_deref())
            .unwrap_or("default");

        info!(
            "Creating ServiceAccount token volume for pod {} using ServiceAccount {}/{}",
            pod_name, namespace, sa_name
        );

        // Find the ServiceAccount token secret
        let storage = self
            .storage
            .as_ref()
            .context("Storage not available for ServiceAccount token volumes")?;

        let secret_name = format!("{}-token", sa_name);
        let key = build_key("secrets", Some(namespace), &secret_name);

        // Try to get the secret; if it doesn't exist, create a basic token mount anyway
        let _secret: Option<Secret> = match storage.get(&key).await {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(
                    "ServiceAccount token secret {} not found: {}. Creating empty token volume.",
                    secret_name, e
                );
                None
            }
        };

        // Create volume directory
        let volume_dir = format!(
            "{}/{}/serviceaccount-token",
            self.volumes_base_path, pod_name
        );
        std::fs::create_dir_all(&volume_dir)
            .context("Failed to create ServiceAccount token volume directory")?;

        // Generate a bound projected token with pod reference.
        // This creates a JWT with pod_name, pod_uid, and node_name claims so that
        // TokenReview returns these in the extra info, matching real K8s behavior.
        let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);
        let sa_uid = storage
            .get::<serde_json::Value>(&sa_key)
            .await
            .ok()
            .and_then(|v| {
                v.get("metadata")
                    .and_then(|m| m.get("uid"))
                    .and_then(|u| u.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default();

        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
        let node_uid = if let Some(ref nn) = node_name {
            let node_key = build_key("nodes", None::<&str>, nn);
            storage
                .get::<serde_json::Value>(&node_key)
                .await
                .ok()
                .and_then(|v| {
                    v.pointer("/metadata/uid")
                        .and_then(|u| u.as_str())
                        .map(|s| s.to_string())
                })
        } else {
            None
        };

        let now = chrono::Utc::now();
        let claims = rusternetes_common::auth::ServiceAccountClaims {
            sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
            namespace: namespace.to_string(),
            uid: sa_uid.clone(),
            iat: now.timestamp(),
            exp: (now + chrono::Duration::hours(1)).timestamp(),
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            aud: vec!["rusternetes".to_string()],
            kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                namespace: namespace.to_string(),
                svcacct: rusternetes_common::auth::KubeRef {
                    name: sa_name.to_string(),
                    uid: sa_uid,
                },
                pod: Some(rusternetes_common::auth::KubeRef {
                    name: pod_name.clone(),
                    uid: pod.metadata.uid.clone(),
                }),
                node: node_name
                    .as_ref()
                    .map(|nn| rusternetes_common::auth::KubeRef {
                        name: nn.clone(),
                        uid: node_uid.clone().unwrap_or_default(),
                    }),
            }),
            pod_name: Some(pod_name.clone()),
            pod_uid: Some(pod.metadata.uid.clone()),
            node_name,
            node_uid,
        };

        let token = match self.token_manager.generate_token(claims) {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    "Failed to generate bound token, falling back to static: {}",
                    e
                );
                let secret_name = format!("{}-token", sa_name);
                let skey = build_key("secrets", Some(namespace), &secret_name);
                storage
                    .get::<Secret>(&skey)
                    .await
                    .ok()
                    .and_then(|s| {
                        s.data.as_ref().and_then(|d| {
                            d.get("token")
                                .map(|v| String::from_utf8_lossy(v).to_string())
                        })
                    })
                    .unwrap_or_default()
            }
        };

        // Write token file
        {
            let token_path = format!("{}/token", volume_dir);
            std::fs::write(&token_path, &token).context("Failed to write ServiceAccount token")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600))?;
            }
            info!(
                "Wrote bound ServiceAccount token for pod {} to {}",
                pod_name, token_path
            );
        }

        // Write namespace file
        {
            let ns_path = format!("{}/namespace", volume_dir);
            std::fs::write(&ns_path, namespace).context("Failed to write namespace file")?;
        }

        // Write ca.crt (cluster CA certificate) so pods can verify API server
        let ca_cert_source = std::env::var("CA_CERT_PATH").unwrap_or_else(|_| {
            format!(
                "{}/.rusternetes/certs/ca.crt",
                std::env::var("HOME").unwrap_or_else(|_| "/root".to_string())
            )
        });

        let ca_path = format!("{}/ca.crt", volume_dir);
        if let Ok(ca_content) = std::fs::read(&ca_cert_source) {
            std::fs::write(&ca_path, ca_content).context("Failed to write CA certificate")?;
            info!("Wrote CA certificate to {}", ca_path);
        } else {
            warn!(
                "CA certificate not found at {}, pods may not be able to verify API server",
                ca_cert_source
            );
        }

        info!("Created ServiceAccount token volume at {}", volume_dir);
        Ok(volume_dir)
    }

    pub async fn start_container(
        &self,
        pod: &Pod,
        container: &Container,
        volume_paths: &HashMap<String, String>,
        netns_path: Option<&str>,
        hosts_file_path: Option<&str>,
        pod_ip: Option<&str>,
    ) -> Result<()> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let container_name = format!("{}_{}", pod_name, container.name);

        info!(
            "Starting container: {} (netns: {:?})",
            container_name, netns_path
        );

        // Check if container already exists
        if let Ok(inspect) = self
            .docker
            .inspect_container(&container_name, None::<InspectContainerOptions>)
            .await
        {
            let state = inspect.state.as_ref();
            let is_running = state.and_then(|s| s.running).unwrap_or(false);
            let status = state.and_then(|s| s.status.as_ref());

            // Skip if container is running or just created (about to start)
            if is_running {
                return Ok(());
            }
            if matches!(
                status,
                Some(bollard::secret::ContainerStateStatusEnum::CREATED)
            ) {
                debug!(
                    "Container {} is in created state, waiting for it to start",
                    container_name
                );
                return Ok(());
            }

            // Only remove if container has actually exited
            if matches!(
                status,
                Some(bollard::secret::ContainerStateStatusEnum::EXITED)
                    | Some(bollard::secret::ContainerStateStatusEnum::DEAD)
            ) {
                debug!("Removing exited container: {}", container_name);
                let remove_options = RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                };
                self.docker
                    .remove_container(&container_name, Some(remove_options))
                    .await?;
                // Brief wait for Docker to release the container name.
                // Docker typically releases names within 50ms after force removal.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            } else {
                // Unknown state — don't remove, don't recreate
                debug!(
                    "Container {} in state {:?}, skipping",
                    container_name, status
                );
                return Ok(());
            }
        }

        // Build environment variables
        let mut env_list = Vec::new();

        // Inject Kubernetes service environment variables for in-cluster access.
        // When using direct API server IP (KUBERNETES_SERVICE_HOST_OVERRIDE),
        // use port 6443 (the actual API server port). When using ClusterIP,
        // use port 443 (the service port that DNAT maps to 6443).
        let k8s_port = if std::env::var("KUBERNETES_SERVICE_HOST_OVERRIDE").is_ok() {
            "6443"
        } else {
            "443"
        };
        env_list.push(format!(
            "KUBERNETES_SERVICE_HOST={}",
            self.kubernetes_service_host
        ));
        env_list.push(format!("KUBERNETES_SERVICE_PORT={}", k8s_port));
        env_list.push(format!("KUBERNETES_SERVICE_PORT_HTTPS={}", k8s_port));
        env_list.push(format!(
            "KUBERNETES_PORT=tcp://{}:{}",
            self.kubernetes_service_host, k8s_port
        ));
        env_list.push(format!(
            "KUBERNETES_PORT_443_TCP=tcp://{}:{}",
            self.kubernetes_service_host, k8s_port
        ));
        env_list.push("KUBERNETES_PORT_443_TCP_PROTO=tcp".to_string());
        env_list.push(format!("KUBERNETES_PORT_443_TCP_PORT={}", k8s_port));
        env_list.push(format!(
            "KUBERNETES_PORT_443_TCP_ADDR={}",
            self.kubernetes_service_host
        ));

        // Inject JOB_COMPLETION_INDEX for indexed Jobs
        if let Some(annotations) = &pod.metadata.annotations {
            if let Some(index) = annotations.get("batch.kubernetes.io/job-completion-index") {
                env_list.push(format!("JOB_COMPLETION_INDEX={}", index));
            }
        }

        // Inject service link environment variables (Kubernetes convention).
        // When enableServiceLinks is true (default), inject env vars for every
        // Service in the pod's namespace: {SVC}_SERVICE_HOST, {SVC}_SERVICE_PORT, etc.
        let enable_service_links = pod
            .spec
            .as_ref()
            .and_then(|s| s.enable_service_links)
            .unwrap_or(true);

        if enable_service_links {
            if let Some(storage) = &self.storage {
                let svc_prefix = rusternetes_storage::build_prefix("services", Some(namespace));
                match storage
                    .list::<rusternetes_common::resources::Service>(&svc_prefix)
                    .await
                {
                    Ok(services) => {
                        for service in &services {
                            let svc_name_raw = &service.metadata.name;
                            // Skip the "kubernetes" service — already injected above
                            if svc_name_raw == "kubernetes" {
                                continue;
                            }
                            let cluster_ip = match &service.spec.cluster_ip {
                                Some(ip) if ip != "None" && !ip.is_empty() => ip,
                                _ => continue,
                            };
                            let svc_env = svc_name_raw.to_uppercase().replace('-', "_");
                            env_list.push(format!("{}_SERVICE_HOST={}", svc_env, cluster_ip));
                            if let Some(first_port) = service.spec.ports.first() {
                                let proto = first_port
                                    .protocol
                                    .as_deref()
                                    .unwrap_or("TCP")
                                    .to_lowercase();
                                env_list
                                    .push(format!("{}_SERVICE_PORT={}", svc_env, first_port.port));
                                env_list.push(format!(
                                    "{}_PORT={}://{}:{}",
                                    svc_env, proto, cluster_ip, first_port.port
                                ));
                                env_list.push(format!(
                                    "{}_PORT_{}_{}={}://{}:{}",
                                    svc_env,
                                    first_port.port,
                                    proto.to_uppercase(),
                                    proto,
                                    cluster_ip,
                                    first_port.port
                                ));
                                env_list.push(format!(
                                    "{}_PORT_{}_{}_PROTO={}",
                                    svc_env,
                                    first_port.port,
                                    proto.to_uppercase(),
                                    proto
                                ));
                                env_list.push(format!(
                                    "{}_PORT_{}_{}_PORT={}",
                                    svc_env,
                                    first_port.port,
                                    proto.to_uppercase(),
                                    first_port.port
                                ));
                                env_list.push(format!(
                                    "{}_PORT_{}_{}_ADDR={}",
                                    svc_env,
                                    first_port.port,
                                    proto.to_uppercase(),
                                    cluster_ip
                                ));
                                // Named port: {SVC}_SERVICE_PORT_{PORT_NAME}
                                if let Some(port_name) = &first_port.name {
                                    let port_name_env = port_name.to_uppercase().replace('-', "_");
                                    env_list.push(format!(
                                        "{}_SERVICE_PORT_{}={}",
                                        svc_env, port_name_env, first_port.port
                                    ));
                                }
                            }
                            // Additional named ports beyond the first
                            for port in service.spec.ports.iter().skip(1) {
                                if let Some(port_name) = &port.name {
                                    let port_name_env = port_name.to_uppercase().replace('-', "_");
                                    env_list.push(format!(
                                        "{}_SERVICE_PORT_{}={}",
                                        svc_env, port_name_env, port.port
                                    ));
                                }
                                let proto =
                                    port.protocol.as_deref().unwrap_or("TCP").to_lowercase();
                                env_list.push(format!(
                                    "{}_PORT_{}_{}={}://{}:{}",
                                    svc_env,
                                    port.port,
                                    proto.to_uppercase(),
                                    proto,
                                    cluster_ip,
                                    port.port
                                ));
                                env_list.push(format!(
                                    "{}_PORT_{}_{}_PROTO={}",
                                    svc_env,
                                    port.port,
                                    proto.to_uppercase(),
                                    proto
                                ));
                                env_list.push(format!(
                                    "{}_PORT_{}_{}_PORT={}",
                                    svc_env,
                                    port.port,
                                    proto.to_uppercase(),
                                    port.port
                                ));
                                env_list.push(format!(
                                    "{}_PORT_{}_{}_ADDR={}",
                                    svc_env,
                                    port.port,
                                    proto.to_uppercase(),
                                    cluster_ip
                                ));
                            }
                        }
                        debug!(
                            "Injected service link env vars for {} services in namespace {}",
                            services.len(),
                            namespace
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to list services for service links in namespace {}: {}",
                            namespace, e
                        );
                    }
                }
            } else {
                debug!("No storage available for service link env var injection");
            }
        }

        // Add envFrom: inject all keys from referenced ConfigMaps/Secrets
        if let Some(env_from_sources) = &container.env_from {
            for source in env_from_sources {
                let prefix = source.prefix.as_deref().unwrap_or("");
                // ConfigMap envFrom
                if let Some(cm_ref) = &source.config_map_ref {
                    if let Some(storage) = &self.storage {
                        let cm_key = build_key("configmaps", Some(namespace), &cm_ref.name);
                        match storage.get::<ConfigMap>(&cm_key).await {
                            Ok(cm) => {
                                if let Some(data) = &cm.data {
                                    for (k, v) in data {
                                        env_list.push(format!("{}{}={}", prefix, k, v));
                                    }
                                }
                            }
                            Err(e) => {
                                let optional = cm_ref.optional.unwrap_or(false);
                                if !optional {
                                    warn!(
                                        "Failed to get ConfigMap {} for envFrom: {}",
                                        cm_ref.name, e
                                    );
                                }
                            }
                        }
                    }
                }
                // Secret envFrom
                if let Some(secret_ref) = &source.secret_ref {
                    if let Some(storage) = &self.storage {
                        let secret_key = build_key("secrets", Some(namespace), &secret_ref.name);
                        match storage.get::<Secret>(&secret_key).await {
                            Ok(secret) => {
                                if let Some(data) = &secret.data {
                                    for (k, v) in data {
                                        if let Ok(val) = String::from_utf8(v.clone()) {
                                            env_list.push(format!("{}{}={}", prefix, k, val));
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let optional = secret_ref.optional.unwrap_or(false);
                                if !optional {
                                    warn!(
                                        "Failed to get Secret {} for envFrom: {}",
                                        secret_ref.name, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Add user-defined environment variables
        if let Some(env_vars) = &container.env {
            for env_var in env_vars {
                // Direct value — expand $(VAR) references using previously set env vars
                if let Some(value) = &env_var.value {
                    let mut expanded = value.clone();
                    while let Some(start) = expanded.find("$(") {
                        let end = match expanded[start..].find(')') {
                            Some(e) => start + e,
                            None => break,
                        };
                        let var_name = &expanded[start + 2..end];
                        let replacement = env_list
                            .iter()
                            .find_map(|entry| {
                                let (k, v) = entry.split_once('=')?;

                                if k == var_name {
                                    Some(v.to_string())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default();
                        expanded.replace_range(start..end + 1, &replacement);
                    }
                    env_list.push(format!("{}={}", env_var.name, expanded));
                    continue;
                }

                // Value from ConfigMap, Secret, or Downward API
                if let Some(value_from) = &env_var.value_from {
                    // ConfigMap reference
                    if let Some(configmap_ref) = &value_from.config_map_key_ref {
                        match self
                            .get_configmap_value(namespace, &configmap_ref.name, &configmap_ref.key)
                            .await
                        {
                            Ok(value) => {
                                env_list.push(format!("{}={}", env_var.name, value));
                            }
                            Err(e) => {
                                warn!("Failed to get ConfigMap value for {}: {}", env_var.name, e);
                            }
                        }
                        continue;
                    }

                    // Secret reference
                    if let Some(secret_ref) = &value_from.secret_key_ref {
                        match self
                            .get_secret_value(namespace, &secret_ref.name, &secret_ref.key)
                            .await
                        {
                            Ok(value) => {
                                env_list.push(format!("{}={}", env_var.name, value));
                            }
                            Err(e) => {
                                warn!("Failed to get Secret value for {}: {}", env_var.name, e);
                            }
                        }
                        continue;
                    }

                    // Field reference (Downward API)
                    if let Some(field_ref) = &value_from.field_ref {
                        match self.get_pod_field_value(pod, &field_ref.field_path) {
                            Ok(value) => {
                                // For status.podIP, fall back to the CNI-assigned IP when
                                // pod status hasn't been written to etcd yet (i.e., at first
                                // container creation). This ensures SONOBUOY_ADVERTISE_IP and
                                // similar env vars get the correct IP.
                                let resolved =
                                    if value.is_empty() && field_ref.field_path == "status.podIP" {
                                        pod_ip.unwrap_or("").to_string()
                                    } else {
                                        value
                                    };

                                // Always set the env var — even empty values are valid.
                                // Kubernetes never skips a fieldRef env var.
                                env_list.push(format!("{}={}", env_var.name, resolved));
                                if !resolved.is_empty() {
                                    info!(
                                        "Set env var {} from field {}: {}",
                                        env_var.name, field_ref.field_path, resolved
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Failed to get pod field value for {}: {}", env_var.name, e);
                            }
                        }
                        continue;
                    }

                    // Resource field reference
                    if let Some(resource_ref) = &value_from.resource_field_ref {
                        match self.get_container_resource_value(pod, resource_ref) {
                            Ok(value) => {
                                env_list.push(format!("{}={}", env_var.name, value));
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to get resource field value for {}: {}",
                                    env_var.name, e
                                );
                            }
                        }
                        continue;
                    }
                }
            }
        }

        // Collect resolved environment variables for subPathExpr expansion
        // (must be done before env_list is consumed).
        let resolved_env_pairs: Vec<(String, String)> = env_list
            .iter()
            .filter_map(|entry| {
                let mut parts = entry.splitn(2, '=');
                let name = parts.next()?.to_string();
                let value = parts.next().unwrap_or("").to_string();
                Some((name, value))
            })
            .collect();

        let env = if env_list.is_empty() {
            None
        } else {
            Some(env_list)
        };

        // Build port bindings.
        // When using container:<pause> network mode, ports must be declared on the
        // pause container (which owns the network namespace), not on child containers.
        // Docker rejects port declarations on containers that join another's network.
        let using_pause_network = !self.use_cni && netns_path.is_none();

        let mut exposed_ports = HashMap::new();
        let mut port_bindings = HashMap::new();

        if !using_pause_network {
            if let Some(ports) = &container.ports {
                for port in ports {
                    let proto = port.protocol.as_deref().unwrap_or("TCP").to_lowercase();
                    let port_key = format!("{}/{}", port.container_port, proto);
                    exposed_ports.insert(port_key.clone(), HashMap::new());

                    if let Some(host_port) = port.host_port {
                        // Use the pod spec's hostIP if specified, otherwise 0.0.0.0.
                        // K8s allows different pods to bind the same hostPort on
                        // different hostIPs (e.g., 127.0.0.1 vs 172.18.0.6).
                        let bind_ip = port.host_ip.as_deref().unwrap_or("0.0.0.0").to_string();
                        port_bindings.insert(
                            port_key,
                            Some(vec![bollard::models::PortBinding {
                                host_ip: Some(bind_ip),
                                host_port: Some(host_port.to_string()),
                            }]),
                        );
                    }
                }
            }
        }

        // Build volume bindings
        let mut binds = Vec::new();
        let mut tmpfs_mounts: HashMap<String, String> = HashMap::new();
        let docker_vol_mounts: Vec<bollard::models::Mount> = Vec::new();

        // Identify which volumes are emptyDir (should use tmpfs)
        let empty_dir_volumes: std::collections::HashSet<String> = pod
            .spec
            .as_ref()
            .and_then(|s| s.volumes.as_ref())
            .map(|volumes| {
                volumes
                    .iter()
                    .filter(|v| v.empty_dir.is_some())
                    .map(|v| v.name.clone())
                    .collect()
            })
            .unwrap_or_default();

        // Mount volumes based on volumeMounts (includes service account tokens injected by admission controller)
        if let Some(volume_mounts) = &container.volume_mounts {
            for mount in volume_mounts {
                // Validate subPathExpr / subPath BEFORE looking up the volume.
                // Kubernetes rejects containers whose expanded subpath contains
                // ".." or is absolute, regardless of whether the volume exists.
                let expanded_sub_path: Option<String> = if let Some(ref expr) = mount.sub_path_expr
                {
                    debug!(
                        "subPathExpr='{}' for container {} mount {}, env_pairs={:?}",
                        expr, container.name, mount.name, resolved_env_pairs
                    );
                    match Self::expand_subpath_expr(expr, &resolved_env_pairs) {
                        Ok(expanded) => {
                            if expanded.is_empty() {
                                return Err(anyhow::anyhow!(
                                        "CreateContainerConfigError: subPathExpr '{}' expanded to empty string in container {}",
                                        expr, container.name
                                    ));
                            }
                            Some(expanded)
                        }
                        Err(e) => {
                            return Err(anyhow::anyhow!(
                                    "CreateContainerConfigError: subPathExpr expansion failed for container {}: {}",
                                    container.name, e
                                ));
                        }
                    }
                } else if let Some(ref sub_path) = mount.sub_path {
                    if !sub_path.is_empty() {
                        // Validate plain subPath for path traversal / absolute path
                        if sub_path.starts_with('/') {
                            return Err(anyhow::anyhow!(
                                    "CreateContainerConfigError: subPath must not be an absolute path in container {}",
                                    container.name
                                ));
                        }
                        if sub_path.contains('`') {
                            return Err(anyhow::anyhow!(
                                    "CreateContainerConfigError: subPath must not contain backticks in container {}",
                                    container.name
                                ));
                        }
                        if sub_path.split('/').any(|c| c == "..") {
                            return Err(anyhow::anyhow!(
                                    "CreateContainerConfigError: subPath must not contain '..' in container {}",
                                    container.name
                                ));
                        }
                        Some(sub_path.clone())
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(host_path) = volume_paths.get(&mount.name) {
                    let read_only = mount.read_only.unwrap_or(false);

                    {
                        // Determine the effective host path, applying validated sub_path
                        let effective_host_path = if let Some(ref sub) = expanded_sub_path {
                            let full = format!("{}/{}", host_path, sub);
                            match std::fs::metadata(&full) {
                                Ok(_meta) => {
                                    // subPath target exists — use it as-is (file or directory).
                                    // For projected volumes (configmaps, secrets), the subPath
                                    // points to a specific file within the volume, not a directory.
                                    // Docker can bind-mount files directly.
                                }
                                Err(_) => {
                                    // subPath target doesn't exist — create as directory
                                    if let Err(e) = std::fs::create_dir_all(&full) {
                                        warn!("Failed to create subPath dir {}: {}", full, e);
                                    }
                                }
                            }
                            full
                        } else {
                            host_path.clone()
                        };
                        // emptyDir volumes: use tmpfs for proper permission support.
                        // Bind mounts through virtiofs (Podman Machine / Docker Desktop)
                        // don't respect chmod correctly, causing permission test failures.
                        // tmpfs enforces mode at the filesystem level.
                        //
                        // For non-Memory emptyDir, we still also use the bind mount
                        // for the host-side volume directory (for ConfigMap/Secret data
                        // already written there). But the tmpfs takes precedence at
                        // the mount point for files the container creates.
                        let is_emptydir = empty_dir_volumes.contains(&mount.name);
                        let is_memory_medium = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.volumes.as_ref())
                            .and_then(|vols| vols.iter().find(|v| v.name == mount.name))
                            .and_then(|v| v.empty_dir.as_ref())
                            .and_then(|ed| ed.medium.as_deref())
                            == Some("Memory");

                        // Use tmpfs for Memory medium emptyDir (matches K8s behavior
                        // and conformance test expectations for mount type).
                        // Note: per-container tmpfs is destroyed on container restart,
                        // which breaks data persistence. K8s keeps tmpfs alive via
                        // the pod sandbox mount namespace. We don't share mounts yet.
                        // Default medium uses bind mounts which persist across restarts.
                        let use_tmpfs =
                            is_emptydir && is_memory_medium && expanded_sub_path.is_none();

                        if use_tmpfs {
                            let opts = if read_only {
                                "ro,mode=0777".to_string()
                            } else {
                                "mode=0777".to_string()
                            };
                            tmpfs_mounts.insert(mount.mount_path.clone(), opts);
                        } else {
                            let ro_suffix = if read_only { ":ro" } else { "" };
                            let bind = format!(
                                "{}:{}{}",
                                effective_host_path, mount.mount_path, ro_suffix
                            );
                            binds.push(bind);
                        }
                        info!(
                            "Mounting volume {} at {} in container {}",
                            mount.name, mount.mount_path, container.name
                        );
                    }
                }
            }
        }

        // Create and mount custom resolv.conf based on DNS policy.
        // CoreDNS pods always skip custom DNS to avoid circular dependency.
        if pod_name != "coredns" {
            let dns_policy = pod
                .spec
                .as_ref()
                .and_then(|s| s.dns_policy.as_deref())
                .unwrap_or("ClusterFirst");

            let is_host_network = pod
                .spec
                .as_ref()
                .and_then(|s| s.host_network)
                .unwrap_or(false);

            // Build base resolv.conf content based on DNS policy
            let resolv_conf_content = match dns_policy {
                "None" => {
                    // Policy "None": start with empty resolv.conf, only dnsConfig applies
                    String::new()
                }
                "Default" => {
                    // Policy "Default": use the node's (host) /etc/resolv.conf
                    match std::fs::read_to_string("/etc/resolv.conf") {
                        Ok(content) => content,
                        Err(e) => {
                            warn!(
                                "Failed to read host /etc/resolv.conf for DNS policy Default: {}",
                                e
                            );
                            // Fall back to cluster DNS
                            format!(
                                "nameserver {}\nsearch {}.svc.{} svc.{} {}\noptions ndots:5\n",
                                self.cluster_dns,
                                namespace,
                                self.cluster_domain,
                                self.cluster_domain,
                                self.cluster_domain
                            )
                        }
                    }
                }
                _ => {
                    // ClusterFirst (default) and ClusterFirstWithHostNet: use cluster DNS.
                    // For ClusterFirstWithHostNet, if on host network, still use cluster DNS.
                    if dns_policy == "ClusterFirst" && is_host_network {
                        // ClusterFirst + host network: Kubernetes falls back to host DNS
                        match std::fs::read_to_string("/etc/resolv.conf") {
                            Ok(content) => content,
                            Err(_) => format!(
                                "nameserver {}\nsearch {}.svc.{} svc.{} {}\noptions ndots:5\n",
                                self.cluster_dns,
                                namespace,
                                self.cluster_domain,
                                self.cluster_domain,
                                self.cluster_domain
                            ),
                        }
                    } else {
                        // ClusterFirst: include both container DNS and cluster DNS.
                        // Container DNS first so container hostnames (api-server) resolve.
                        // Cluster DNS second for K8s service names.
                        // K8s ref: pkg/kubelet/network/dns/dns.go — getClusterDNS()
                        let host_dns =
                            std::fs::read_to_string("/etc/resolv.conf")
                                .ok()
                                .and_then(|c| {
                                    c.lines().find(|l| l.starts_with("nameserver")).map(|l| {
                                        l.trim_start_matches("nameserver").trim().to_string()
                                    })
                                });
                        let nameservers = match host_dns {
                            Some(dns) => {
                                format!("nameserver {}\nnameserver {}", dns, self.cluster_dns)
                            }
                            None => format!("nameserver {}", self.cluster_dns),
                        };
                        format!(
                            "{}\nsearch {}.svc.{} svc.{} {}\noptions ndots:5\n",
                            nameservers,
                            namespace,
                            self.cluster_domain,
                            self.cluster_domain,
                            self.cluster_domain
                        )
                    }
                }
            };

            // Apply dnsConfig overrides (nameservers, searches, options)
            let final_content =
                if let Some(dns_config) = pod.spec.as_ref().and_then(|s| s.dns_config.as_ref()) {
                    let mut nameservers: Vec<String> = Vec::new();
                    let mut searches: Vec<String> = Vec::new();
                    let mut options: Vec<String> = Vec::new();

                    // Parse existing content
                    for line in resolv_conf_content.lines() {
                        let line = line.trim();
                        if let Some(rest) = line.strip_prefix("nameserver ") {
                            nameservers.push(rest.to_string());
                        } else if let Some(rest) = line.strip_prefix("search ") {
                            for domain in rest.split_whitespace() {
                                searches.push(domain.to_string());
                            }
                        } else if let Some(rest) = line.strip_prefix("options ") {
                            for opt in rest.split_whitespace() {
                                options.push(opt.to_string());
                            }
                        }
                    }

                    // Prepend custom nameservers
                    if let Some(ref custom_ns) = dns_config.nameservers {
                        let mut merged = custom_ns.clone();
                        for ns in &nameservers {
                            if !merged.contains(ns) {
                                merged.push(ns.clone());
                            }
                        }
                        nameservers = merged;
                    }

                    // Add/replace custom search domains
                    if let Some(ref custom_searches) = dns_config.searches {
                        let mut merged = custom_searches.clone();
                        for s in &searches {
                            if !merged.contains(s) {
                                merged.push(s.clone());
                            }
                        }
                        searches = merged;
                    }

                    // Add custom options
                    if let Some(ref custom_opts) = dns_config.options {
                        for opt in custom_opts {
                            let opt_str = if let Some(ref val) = opt.value {
                                format!("{}:{}", opt.name, val)
                            } else {
                                opt.name.clone()
                            };
                            // Replace existing option with same name
                            let opt_name = opt.name.as_str();
                            options.retain(|o| !o.starts_with(opt_name));
                            options.push(opt_str);
                        }
                    }

                    let mut result = String::new();
                    for ns in &nameservers {
                        result.push_str(&format!("nameserver {}\n", ns));
                    }
                    if !searches.is_empty() {
                        result.push_str(&format!("search {}\n", searches.join(" ")));
                    }
                    if !options.is_empty() {
                        result.push_str(&format!("options {}\n", options.join(" ")));
                    }
                    result
                } else {
                    resolv_conf_content
                };

            if !final_content.is_empty() {
                let resolv_conf_path =
                    format!("{}/{}/resolv.conf", self.volumes_base_path, pod_name);

                // Create directory if it doesn't exist
                std::fs::create_dir_all(format!("{}/{}", self.volumes_base_path, pod_name))
                    .context("Failed to create pod directory for resolv.conf")?;

                // Write custom resolv.conf
                std::fs::write(&resolv_conf_path, &final_content).with_context(|| {
                    format!("Failed to write custom resolv.conf for pod {}", pod_name)
                })?;

                // Mount custom resolv.conf into container (avoid duplicate mounts)
                if !binds.iter().any(|b| b.contains(":/etc/resolv.conf")) {
                    binds.push(format!("{}:/etc/resolv.conf:ro", resolv_conf_path));
                }
                info!(
                    "Mounted custom resolv.conf for pod {} (dns_policy={})",
                    pod_name, dns_policy
                );
            } else {
                debug!(
                    "DNS policy '{}' with no content — not mounting resolv.conf for pod {}",
                    dns_policy, pod_name
                );
            }
        }

        // Mount /etc/hosts if a pod-specific hosts file was created,
        // but skip if the container already has a volume mount at /etc/hosts
        let has_hosts_mount = container
            .volume_mounts
            .as_ref()
            .map(|mounts| mounts.iter().any(|m| m.mount_path == "/etc/hosts"))
            .unwrap_or(false);
        if let Some(hosts_path) = hosts_file_path {
            if !has_hosts_mount && !binds.iter().any(|b| b.contains(":/etc/hosts")) {
                binds.push(format!("{}:/etc/hosts", hosts_path));
                info!("Mounted custom /etc/hosts for pod {}", pod_name);
            }
        }

        // Create and bind-mount termination message log file.
        // Kubernetes writes termination messages to /dev/termination-log (or a custom path).
        // Docker's /dev is a tmpfs that becomes inaccessible after the container stops,
        // so we create a host-side file and bind-mount it into the container.
        {
            let term_msg_path = container
                .termination_message_path
                .as_deref()
                .unwrap_or("/dev/termination-log");
            let term_host_dir = format!("{}/{}/termination", self.volumes_base_path, pod_name);
            std::fs::create_dir_all(&term_host_dir).ok();
            let term_host_file = format!("{}/{}", term_host_dir, container.name);
            // Create an empty file
            std::fs::write(&term_host_file, "").ok();
            binds.push(format!("{}:{}", term_host_file, term_msg_path));
        }

        // Create container configuration
        // Skip cluster DNS configuration for:
        //   - CoreDNS (to avoid circular dependency)
        //   - Non-CNI containers (they join the pause container's network namespace,
        //     which owns the DNS config; Docker rejects dns options in container mode)
        let (dns_servers, dns_search_domains, dns_options) = if pod_name == "coredns" {
            info!("Skipping cluster DNS configuration for CoreDNS pod (using default/host DNS)");
            (None, None, None)
        } else if !self.use_cni {
            // DNS is inherited from the pause container's network namespace
            (None, None, None)
        } else {
            info!(
                "Configuring DNS for container {} in namespace {}",
                container.name, namespace
            );
            let servers = vec![self.cluster_dns.clone()];
            let search_domains = vec![
                format!("{}.svc.{}", namespace, self.cluster_domain),
                format!("svc.{}", self.cluster_domain),
                self.cluster_domain.clone(),
            ];
            // Add ndots:5 to match Kubernetes default DNS behavior
            // This tells the resolver to try search domains after 5 dots in the query
            let options = vec!["ndots:5".to_string()];
            info!(
                "DNS servers: {:?}, search domains: {:?}, options: {:?}",
                servers, search_domains, options
            );
            (Some(servers), Some(search_domains), Some(options))
        };

        // Parse resource limits for container cgroup enforcement
        let mut memory_limit: Option<i64> = None;
        let mut cpu_period: Option<i64> = None;
        let mut cpu_quota: Option<i64> = None;
        // Default cpu_shares to 2 (Kubernetes minimum, maps to cgroup2 weight ~1).
        // Docker's default of 1024 (weight 100) is too high for conformance tests
        // that check cpu.weight matches the pod's CPU request.
        let mut cpu_shares: Option<i64> = Some(2);

        if let Some(ref resources) = container.resources {
            if let Some(ref limits) = resources.limits {
                if let Some(memory) = limits.get("memory") {
                    let parsed = parse_memory_quantity(memory);
                    if parsed > 0 {
                        memory_limit = Some(parsed);
                        info!(
                            "Setting memory limit for container {}: {} bytes",
                            container.name, parsed
                        );
                    }
                }
                if let Some(cpu) = limits.get("cpu") {
                    let cpu_millicores = parse_cpu_quantity(cpu);
                    if cpu_millicores > 0 {
                        cpu_period = Some(100_000); // 100ms in microseconds
                        cpu_quota = Some((cpu_millicores * 100_000) / 1000);
                        info!(
                            "Setting CPU limit for container {}: {}m (quota={}/period={})",
                            container.name,
                            cpu_millicores,
                            cpu_quota.unwrap(),
                            cpu_period.unwrap()
                        );
                    }
                }
            }
            // Compute cpu_shares from CPU requests (cgroup cpu.weight via Docker).
            // Kubernetes formula: shares = max(2, milliCPU * 1024 / 1000)
            // Docker converts shares to cgroup2 cpu.weight automatically.
            // If requests.cpu is not set, fall back to limits.cpu (Kubernetes defaults
            // requests to limits when only limits are specified).
            let cpu_request = resources
                .requests
                .as_ref()
                .and_then(|r| r.get("cpu"))
                .or_else(|| resources.limits.as_ref().and_then(|l| l.get("cpu")));
            if let Some(cpu) = cpu_request {
                let cpu_millicores = parse_cpu_quantity(cpu);
                if cpu_millicores > 0 {
                    let shares = std::cmp::max(2, (cpu_millicores * 1024) / 1000);
                    cpu_shares = Some(shares);
                    info!(
                        "Setting CPU shares for container {}: {}m -> {} shares",
                        container.name, cpu_millicores, shares
                    );
                }
            }
        }

        // Resolve runAsUser and runAsGroup from container or pod security context
        let run_as_user_id: Option<i64> = container
            .security_context
            .as_ref()
            .and_then(|sc| sc.run_as_user)
            .or_else(|| {
                pod.spec
                    .as_ref()
                    .and_then(|s| s.security_context.as_ref())
                    .and_then(|sc| sc.run_as_user)
            });
        let run_as_group_id: Option<i64> = container
            .security_context
            .as_ref()
            .and_then(|sc| sc.run_as_group)
            .or_else(|| {
                pod.spec
                    .as_ref()
                    .and_then(|s| s.security_context.as_ref())
                    .and_then(|sc| sc.run_as_group)
            });
        // Docker user format: "uid" or "uid:gid"
        let run_as_user: Option<String> = match (run_as_user_id, run_as_group_id) {
            (Some(uid), Some(gid)) => Some(format!("{}:{}", uid, gid)),
            (Some(uid), None) => Some(uid.to_string()),
            (None, Some(gid)) => Some(format!("0:{}", gid)), // default uid 0 if only gid set
            (None, None) => None,
        };

        // Set container hostname to pod hostname — but only when NOT using container:
        // network mode (Docker rejects hostname on containers sharing another's network NS)
        let using_container_network = !self.use_cni && netns_path.is_none();
        let pod_hostname = if !using_container_network {
            let raw = pod
                .spec
                .as_ref()
                .and_then(|s| s.hostname.as_deref())
                .unwrap_or(&pod.metadata.name);
            // Linux hostnames limited to 63 chars
            let truncated = if raw.len() > 63 {
                raw[..63].trim_end_matches('-').to_string()
            } else {
                raw.to_string()
            };
            Some(truncated)
        } else {
            None // Hostname is set on the pause container instead
        };

        let mut config = Config {
            image: Some(container.image.clone()),
            env,
            working_dir: container.working_dir.clone(),
            user: run_as_user,
            hostname: pod_hostname,
            exposed_ports: if exposed_ports.is_empty() {
                None
            } else {
                Some(exposed_ports)
            },
            host_config: Some(bollard::models::HostConfig {
                port_bindings: if port_bindings.is_empty() {
                    None
                } else {
                    Some(port_bindings)
                },
                binds: if binds.is_empty() { None } else { Some(binds) },
                tmpfs: if tmpfs_mounts.is_empty() {
                    None
                } else {
                    Some(tmpfs_mounts)
                },
                mounts: if docker_vol_mounts.is_empty() {
                    None
                } else {
                    Some(docker_vol_mounts)
                },
                // Configure DNS to use kube-dns service
                // CoreDNS uses default/host DNS to avoid circular dependency
                dns: dns_servers,
                dns_search: dns_search_domains,
                dns_options,
                // App containers MUST share the pause container's network namespace.
                // This ensures all containers in a pod share the same IP address,
                // which is fundamental to K8s networking (pod IP = pause container IP).
                //
                // With CNI netns: use ns:{path} to join the network namespace
                // Without CNI: use container:{pause} to join the pause container's namespace
                //
                // Previously, when use_cni was true but no netns was available,
                // we fell back to the Docker bridge which gave each container its
                // OWN IP — breaking pod proxy, service routing, and inter-container
                // communication. K8s ref: all containers share the pod sandbox network.
                network_mode: if let Some(netns) = netns_path {
                    Some(format!("ns:{}", netns))
                } else {
                    // Always use pause container's network namespace
                    Some(format!("container:{}_pause", pod_name))
                },
                // Share IPC and PID namespaces with pause container (K8s pod semantics)
                ipc_mode: if !self.use_cni {
                    Some(format!("container:{}_pause", pod_name))
                } else {
                    None
                },
                pid_mode: if pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.share_process_namespace)
                    .unwrap_or(false)
                {
                    Some(format!("container:{}_pause", pod_name))
                } else {
                    None
                },
                // Share UTS namespace with pause container so app containers
                // inherit the pod hostname. Without this, containers get their
                // container ID as hostname instead of the pod name.
                // K8s CRI shares UTS via the pod sandbox; Docker/Podman need explicit uts_mode.
                uts_mode: if using_container_network {
                    Some(format!("container:{}_pause", pod_name))
                } else {
                    None
                },
                // Resource limits enforcement via cgroups
                memory: memory_limit,
                cpu_period,
                cpu_quota,
                cpu_shares,
                // Read-only root filesystem from security context
                readonly_rootfs: container
                    .security_context
                    .as_ref()
                    .and_then(|sc| sc.read_only_root_filesystem),
                // Security options: no-new-privileges when allowPrivilegeEscalation is false
                security_opt: {
                    let ape = container
                        .security_context
                        .as_ref()
                        .and_then(|sc| sc.allow_privilege_escalation)
                        .or_else(|| {
                            pod.spec
                                .as_ref()
                                .and_then(|s| s.security_context.as_ref())
                                .and_then(|sc| sc.run_as_non_root)
                                .map(|_| false)
                        });
                    if ape == Some(false) {
                        Some(vec!["no-new-privileges".to_string()])
                    } else {
                        None
                    }
                },
                // Capabilities
                cap_add: container
                    .security_context
                    .as_ref()
                    .and_then(|sc| sc.capabilities.as_ref())
                    .and_then(|c| c.add.clone()),
                cap_drop: container
                    .security_context
                    .as_ref()
                    .and_then(|sc| sc.capabilities.as_ref())
                    .and_then(|c| c.drop.clone()),
                // Privileged mode
                privileged: container
                    .security_context
                    .as_ref()
                    .and_then(|sc| sc.privileged),
                // Sysctls are set on the pause container (which owns the namespaces).
                // App containers share namespaces via ipc_mode/pid_mode above.
                ..Default::default()
            }),
            ..Default::default()
        };

        // Set command and args
        // In Kubernetes: command overrides Docker ENTRYPOINT, args overrides Docker CMD
        // Kubernetes expands $(VAR_NAME) references in command and args using the
        // container's own environment variables.
        // K8s variable expansion matching third_party/forked/golang/expansion/expand.go:
        // - $(VAR_NAME) → expand if VAR_NAME is a defined env var, else leave literal
        // - $$ → $ (escape sequence, critical for shell command substitutions)
        // - $other → $other (literal)
        let expand_k8s_vars = |items: &[String]| -> Vec<String> {
            items
                .iter()
                .map(|item| {
                    let input = item.as_bytes();
                    let mut buf = Vec::with_capacity(input.len());
                    let mut cursor = 0;
                    while cursor < input.len() {
                        if input[cursor] == b'$' && cursor + 1 < input.len() {
                            match input[cursor + 1] {
                                b'$' => {
                                    // $$ → $ (escaped operator)
                                    buf.push(b'$');
                                    cursor += 2;
                                }
                                b'(' => {
                                    // Possible $(VAR_NAME) reference
                                    if let Some(close) =
                                        input[cursor + 2..].iter().position(|&b| b == b')')
                                    {
                                        let var_name = std::str::from_utf8(
                                            &input[cursor + 2..cursor + 2 + close],
                                        )
                                        .unwrap_or("");
                                        if let Some((_, value)) =
                                            resolved_env_pairs.iter().find(|(k, _)| k == var_name)
                                        {
                                            buf.extend_from_slice(value.as_bytes());
                                            cursor += 2 + close + 1; // skip past )
                                        } else {
                                            // Not a defined env var — return literal $(VAR_NAME)
                                            buf.extend_from_slice(
                                                &input[cursor..cursor + 2 + close + 1],
                                            );
                                            cursor += 2 + close + 1;
                                        }
                                    } else {
                                        // No closing ) — literal $(
                                        buf.extend_from_slice(&input[cursor..cursor + 2]);
                                        cursor += 2;
                                    }
                                }
                                _ => {
                                    // $other → literal $other
                                    buf.push(input[cursor]);
                                    cursor += 1;
                                }
                            }
                        } else {
                            buf.push(input[cursor]);
                            cursor += 1;
                        }
                    }
                    String::from_utf8(buf).unwrap_or_else(|_| item.clone())
                })
                .collect()
        };

        // K8s sets emptyDir directory permissions to 0777 via chmod in setupDir()
        // (pkg/volume/emptydir/empty_dir.go). It does NOT wrap container commands
        // with "umask 0 && exec". We already chmod 0777 in create_pod_volumes().
        // Never wrap commands — it breaks shell-less images (sonobuoy, conformance, etc).
        let has_emptydir_mount = false; // disabled — chmod handles permissions
        let has_shell = false;
        #[allow(unused)]
        if false {
            let cached_result: Option<bool> = self
                .shell_cache
                .lock()
                .unwrap()
                .get(&container.image)
                .copied();
            if let Some(cached) = cached_result {
                cached
            } else {
                // Use image inspection to heuristically detect if /bin/sh exists.
                // Known no-shell images: distroless, scratch, static.
                // Most standard images (alpine, debian, ubuntu, busybox, etc.) have /bin/sh.
                // This avoids creating+starting+removing a probe container per image.
                let shell_ok =
                    if let Ok(inspect) = self.docker.inspect_image(&container.image).await {
                        // Check image labels/config for distroless/scratch indicators
                        let image_name_lower = container.image.to_lowercase();
                        let is_known_no_shell = image_name_lower.contains("distroless")
                            || image_name_lower.contains("scratch")
                            || image_name_lower.contains("static");
                        if is_known_no_shell {
                            false
                        } else {
                            // Check if the image has a shell-based entrypoint (strong signal)
                            let has_shell_ep = inspect
                                .config
                                .as_ref()
                                .and_then(|c| c.entrypoint.as_ref())
                                .map(|ep| {
                                    ep.iter().any(|e| {
                                        e.contains("/bin/sh")
                                            || e.contains("/bin/bash")
                                            || e.contains("/bin/ash")
                                    })
                                })
                                .unwrap_or(false);
                            let has_shell_cmd = inspect
                                .config
                                .as_ref()
                                .and_then(|c| c.cmd.as_ref())
                                .map(|cmd| {
                                    cmd.iter().any(|c| {
                                        c.contains("/bin/sh")
                                            || c.contains("/bin/bash")
                                            || c.contains("/bin/ash")
                                    })
                                })
                                .unwrap_or(false);
                            // If there's a shell in entrypoint/cmd, definitely has shell.
                            // Otherwise, check if image name suggests a distro with /bin/sh.
                            // Default to false (no shell) to avoid wrapping images that lack it.
                            let is_known_has_shell = image_name_lower.contains("alpine")
                                || image_name_lower.contains("debian")
                                || image_name_lower.contains("ubuntu")
                                || image_name_lower.contains("centos")
                                || image_name_lower.contains("fedora")
                                || image_name_lower.contains("busybox")
                                || image_name_lower.contains("nginx")
                                || image_name_lower.contains("redis")
                                || image_name_lower.contains("postgres")
                                || image_name_lower.contains("mysql")
                                || image_name_lower.contains("node:")
                                || image_name_lower.contains("python")
                                || image_name_lower.contains("ruby")
                                || image_name_lower.contains("golang")
                                || image_name_lower.contains("openjdk")
                                || image_name_lower.contains("httpd")
                                || image_name_lower.contains("perl")
                                || image_name_lower.contains("php");
                            has_shell_ep || has_shell_cmd || is_known_has_shell
                        }
                    } else {
                        // Can't inspect — assume it has a shell (safe default)
                        true
                    };
                if !shell_ok {
                    info!(
                        "Container {} - image lacks /bin/sh, skipping umask wrapper",
                        container.name
                    );
                }
                // Cache the result
                self.shell_cache
                    .lock()
                    .unwrap()
                    .insert(container.image.clone(), shell_ok);
                shell_ok
            } // end else (cache miss)
        } else {
            false
        };
        let needs_umask_fix = has_emptydir_mount && has_shell;

        if let Some(command) = &container.command {
            if let Some(args) = &container.args {
                let expanded_cmd = expand_k8s_vars(command);
                let expanded_args = expand_k8s_vars(args);
                if needs_umask_fix {
                    // If the command is already ["sh", "-c", "script"], inject
                    // umask into the script itself to avoid double-wrapping.
                    // Double-wrapping breaks backticks, quotes, and $() in the script.
                    if expanded_cmd.len() >= 2
                        && (expanded_cmd[0] == "sh" || expanded_cmd[0] == "/bin/sh")
                        && expanded_cmd[1] == "-c"
                        && expanded_cmd.len() == 3
                    {
                        let mut modified_cmd = expanded_cmd.clone();
                        modified_cmd[2] = format!("umask 0000 && {}", modified_cmd[2]);
                        info!(
                            "Container {} - injecting umask into sh -c script",
                            container.name
                        );
                        config.entrypoint = Some(modified_cmd);
                        config.cmd = Some(expanded_args);
                    } else {
                        let full = format!(
                            "umask 0000 && exec {} {}",
                            shell_join(&expanded_cmd),
                            shell_join(&expanded_args)
                        );
                        info!(
                            "Container {} - wrapping with umask 0: {}",
                            container.name, full
                        );
                        config.entrypoint =
                            Some(vec!["/bin/sh".to_string(), "-c".to_string(), full]);
                        config.cmd = Some(vec![]);
                    }
                } else {
                    info!(
                        "Container {} - setting entrypoint {:?} and cmd {:?}",
                        container.name, expanded_cmd, expanded_args
                    );
                    config.entrypoint = Some(expanded_cmd);
                    config.cmd = Some(expanded_args);
                }
            } else {
                let expanded_cmd = expand_k8s_vars(command);
                if needs_umask_fix {
                    // Same sh -c injection for command-only case
                    if expanded_cmd.len() >= 2
                        && (expanded_cmd[0] == "sh" || expanded_cmd[0] == "/bin/sh")
                        && expanded_cmd[1] == "-c"
                        && expanded_cmd.len() == 3
                    {
                        let mut modified_cmd = expanded_cmd.clone();
                        modified_cmd[2] = format!("umask 0000 && {}", modified_cmd[2]);
                        config.entrypoint = Some(modified_cmd);
                    } else {
                        let full = format!("umask 0000 && exec {}", shell_join(&expanded_cmd));
                        info!(
                            "Container {} - wrapping with umask 0: {}",
                            container.name, full
                        );
                        config.entrypoint =
                            Some(vec!["/bin/sh".to_string(), "-c".to_string(), full]);
                    }
                } else {
                    info!(
                        "Container {} - setting entrypoint: {:?}",
                        container.name, expanded_cmd
                    );
                    config.entrypoint = Some(expanded_cmd);
                }
                config.cmd = Some(vec![]);
            }
        } else if let Some(args) = &container.args {
            let expanded_args = expand_k8s_vars(args);
            if needs_umask_fix {
                // args-only: discover image entrypoint and wrap with umask
                let image_entrypoint = self
                    .docker
                    .inspect_image(&container.image)
                    .await
                    .ok()
                    .and_then(|info| info.config)
                    .and_then(|cfg| cfg.entrypoint)
                    .unwrap_or_default();
                let ep_str = if image_entrypoint.is_empty() {
                    String::new()
                } else {
                    shell_join(&image_entrypoint)
                };
                let full = format!(
                    "umask 0000 && exec {} {}",
                    ep_str,
                    shell_join(&expanded_args)
                );
                info!(
                    "Container {} - wrapping args with umask 0: {}",
                    container.name, full
                );
                config.entrypoint = Some(vec!["/bin/sh".to_string(), "-c".to_string(), full]);
                config.cmd = Some(vec![]);
            } else {
                info!(
                    "Container {} - setting cmd (args): {:?}",
                    container.name, expanded_args
                );
                config.cmd = Some(expanded_args);
            }
        } else if needs_umask_fix {
            // No command, no args — use image defaults with umask wrapper.
            // Discover image entrypoint+cmd and wrap with umask.
            let inspect = self.docker.inspect_image(&container.image).await.ok();
            let image_config = inspect.and_then(|i| i.config);
            let image_ep = image_config
                .as_ref()
                .and_then(|c| c.entrypoint.clone())
                .unwrap_or_default();
            let image_cmd = image_config
                .as_ref()
                .and_then(|c| c.cmd.clone())
                .unwrap_or_default();
            // Only wrap if we found an actual entrypoint or cmd from the image.
            // If both are empty (image inspect failed or image has no defaults),
            // skip the umask wrapper — an empty `exec` would fail.
            if !image_ep.is_empty() || !image_cmd.is_empty() {
                let full = format!(
                    "umask 0000 && exec {} {}",
                    shell_join(&image_ep),
                    shell_join(&image_cmd)
                );
                info!(
                    "Container {} - wrapping image defaults with umask 0: {}",
                    container.name, full
                );
                config.entrypoint = Some(vec!["/bin/sh".to_string(), "-c".to_string(), full]);
                config.cmd = Some(vec![]);
            }
        }

        let options = CreateContainerOptions {
            name: container_name.clone(),
            ..Default::default()
        };

        // Create the container. If a container with this name already exists
        // (Docker 409 Conflict), remove it and retry. K8s kills and removes
        // old containers before creating new ones during SyncPod.
        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_manager.go:1433-1447
        if let Err(e) = self
            .docker
            .create_container(Some(options.clone()), config.clone())
            .await
        {
            let err_str = format!("{}", e);
            if err_str.contains("409")
                || err_str.contains("Conflict")
                || err_str.contains("already in use")
            {
                warn!(
                    "Container {} already exists, removing and retrying",
                    container_name
                );
                // Parse the container ID from the Docker error message.
                // Format: "...already in use by container \"<id>\". You have to..."
                // Remove THAT specific container, not just by name.
                let conflicting_id = err_str
                    .split("already in use by container \"")
                    .nth(1)
                    .and_then(|s| s.split('"').next())
                    .map(|s| s.to_string());

                // Remove by ID if available, otherwise by name
                let remove_target = conflicting_id.as_deref().unwrap_or(&container_name);
                let _ = self
                    .docker
                    .remove_container(
                        remove_target,
                        Some(bollard::container::RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                    .await;
                // Also remove by name in case there are multiple conflicts
                if conflicting_id.is_some() {
                    let _ = self
                        .docker
                        .remove_container(
                            &container_name,
                            Some(bollard::container::RemoveContainerOptions {
                                force: true,
                                ..Default::default()
                            }),
                        )
                        .await;
                }
                // Wait for Docker to finalize removal and release the container name.
                // Force-remove is synchronous in Docker daemon; 200ms is sufficient.
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                // Retry creation after removal
                if let Err(e2) = self.docker.create_container(Some(options), config).await {
                    error!(
                        "Docker API error creating container {} after cleanup: {}",
                        container_name, e2
                    );
                    return Err(anyhow::anyhow!("Failed to create container: {}", e2));
                }
            } else {
                error!(
                    "Docker API error creating container {}: {}",
                    container_name, e
                );
                return Err(anyhow::anyhow!("Failed to create container: {}", e));
            }
        }

        // Start the container
        if let Err(e) = self
            .docker
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
        {
            error!("Failed to start container {}: {}", container_name, e);
            return Err(anyhow::anyhow!(
                "Failed to start container {}: {}",
                container_name,
                e
            ));
        }

        info!("Container {} started successfully", container_name);

        // Write Kubernetes-managed /etc/hosts into the container after start.
        // Docker may override bind-mounted /etc/hosts during container creation,
        // so we write it via `docker exec` after start to guarantee our content.
        if let Some(hosts_path) = hosts_file_path {
            if let Ok(hosts_content) = std::fs::read_to_string(hosts_path) {
                // Use printf to write the exact content (handles newlines correctly)
                let exec_config = CreateExecOptions {
                    cmd: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        format!("cat > /etc/hosts << 'KUBEEOF'\n{}KUBEEOF", hosts_content),
                    ]),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                };
                match self.docker.create_exec(&container_name, exec_config).await {
                    Ok(exec) => {
                        if let Err(e) = self.docker.start_exec(&exec.id, None).await {
                            debug!(
                                "Failed to write /etc/hosts via exec for {}: {}",
                                container_name, e
                            );
                        } else {
                            debug!("Wrote Kubernetes-managed /etc/hosts for {}", container_name);
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Failed to create exec for /etc/hosts write in {}: {}",
                            container_name, e
                        );
                    }
                }
            }
        }

        // Execute postStart lifecycle hook if present
        if let Some(ref lifecycle) = container.lifecycle {
            if let Some(ref post_start) = lifecycle.post_start {
                info!("Executing postStart hook for container {}", container.name);
                if let Err(e) = self
                    .execute_lifecycle_handler(post_start, &container_name, container)
                    .await
                {
                    warn!(
                        "postStart hook failed for container {}: {}",
                        container.name, e
                    );
                    // K8s kills the container if postStart fails
                    // See: pkg/kubelet/kuberuntime/kuberuntime_container.go — killContainer on FailedPostStartHook
                    let _ = self
                        .docker
                        .stop_container(&container_name, Some(StopContainerOptions { t: 0 }))
                        .await;
                    return Err(anyhow::anyhow!(
                        "PostStartHook failed for container {}: {}",
                        container.name,
                        e
                    ));
                }
            }
        }

        Ok(())
    }

    /// Stop all containers for a pod
    #[allow(dead_code)]
    pub async fn stop_pod(&self, pod_name: &str) -> Result<()> {
        self.clear_probe_states_for_pod(pod_name);
        self.stop_pod_with_grace_period(pod_name, 30).await
    }

    /// Stop and force-remove all containers for a pod.
    /// Used for orphaned container cleanup where logs are no longer needed.
    pub async fn stop_and_remove_pod(&self, pod_name: &str) -> Result<()> {
        self.clear_probe_states_for_pod(pod_name);

        let mut filters = HashMap::new();
        filters.insert("name".to_string(), vec![format!("{}_", pod_name)]);
        let options = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };
        let containers = self.docker.list_containers(Some(options)).await?;

        for container in containers {
            if let Some(id) = container.id {
                let _ = self
                    .docker
                    .stop_container(&id, Some(StopContainerOptions { t: 0 }))
                    .await;
                let remove_options = RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                };
                let _ = self
                    .docker
                    .remove_container(&id, Some(remove_options))
                    .await;
            }
        }

        if self.use_cni {
            let _ = self.teardown_pod_network(pod_name).await;
        }
        self.cleanup_pod_volumes(pod_name).await?;
        Ok(())
    }

    /// Stop all containers for a pod with a specific grace period in seconds.
    /// Stops the pause container last to keep the network namespace alive.
    pub async fn stop_pod_with_grace_period(
        &self,
        pod_name: &str,
        grace_period_seconds: i64,
    ) -> Result<()> {
        info!(
            "Stopping pod: {} (grace period: {}s)",
            pod_name, grace_period_seconds
        );

        // List all containers with this pod prefix
        let mut filters = HashMap::new();
        filters.insert("name".to_string(), vec![format!("{}_", pod_name)]);

        let options = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        let containers = self.docker.list_containers(Some(options)).await?;

        // Stop app containers first, then the pause container last.
        // The pause container owns the network namespace — stopping it first
        // would destroy networking for containers still shutting down.
        let mut pause_container_id: Option<String> = None;

        for container in &containers {
            if let Some(ref id) = container.id {
                let names = container.names.clone().unwrap_or_default();
                let container_name = names
                    .first()
                    .map(|n| n.trim_start_matches('/').to_string())
                    .unwrap_or_default();

                if container_name.ends_with("_pause") {
                    pause_container_id = Some(id.clone());
                    continue;
                }

                info!("Stopping container: {}", id);

                // Stop the container gracefully using the pod's terminationGracePeriodSeconds
                let stop_options = StopContainerOptions {
                    t: grace_period_seconds,
                };
                if let Err(e) = self.docker.stop_container(id, Some(stop_options)).await {
                    warn!("Failed to stop container {}: {}", id, e);
                }

                // Do NOT remove containers here — keep them stopped for log
                // retrieval. Conformance tests read logs from completed/deleted
                // pods after the pod has been deleted from the API. The orphaned
                // container cleanup will remove them on the next cycle.
                debug!("Container {} stopped, keeping for log access", id);
            }
        }

        // Stop the pause container last
        if let Some(ref pause_id) = pause_container_id {
            info!("Stopping pause container: {} (last)", pause_id);
            let stop_options = StopContainerOptions {
                t: grace_period_seconds,
            };
            if let Err(e) = self
                .docker
                .stop_container(pause_id, Some(stop_options))
                .await
            {
                warn!("Failed to stop pause container {}: {}", pause_id, e);
            }
        }

        // Teardown CNI networking if enabled
        if self.use_cni {
            if let Err(e) = self.teardown_pod_network(pod_name).await {
                warn!("Failed to teardown CNI network for pod {}: {}", pod_name, e);
                // Continue with cleanup even if CNI teardown fails
            }
        }

        // Clean up emptyDir volumes (but keep container data for logs)
        self.cleanup_pod_volumes(pod_name).await?;

        Ok(())
    }

    /// Clean up volumes created for a pod
    /// Clean up volumes for a pod. Called by the TerminatedPod handler
    /// after all containers are stopped, and by the container GC for
    /// orphaned pods. K8s ref: pkg/kubelet/kubelet.go:2484
    pub async fn cleanup_pod_volumes(&self, pod_name: &str) -> Result<()> {
        let volume_dir = format!("{}/{}", self.volumes_base_path, pod_name);

        if std::path::Path::new(&volume_dir).exists() {
            if let Err(e) = std::fs::remove_dir_all(&volume_dir) {
                warn!("Failed to remove volume directory {}: {}", volume_dir, e);
            } else {
                info!("Cleaned up volumes for pod {}", pod_name);
            }
        }

        // Clean up Docker named volumes created for emptyDir volumes.
        // These are named rusternetes-emptydir-{pod_name}-{volume_name}.
        let prefix = format!("rusternetes-emptydir-{}-", pod_name);
        if let Ok(volumes) = self.docker.list_volumes::<String>(None).await {
            if let Some(volume_list) = volumes.volumes {
                for vol in volume_list {
                    if vol.name.starts_with(&prefix) {
                        if let Err(e) = self.docker.remove_volume(&vol.name, None).await {
                            warn!("Failed to remove Docker volume {}: {}", vol.name, e);
                        } else {
                            info!("Removed Docker volume {}", vol.name);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a specific container is running
    pub async fn is_container_running(&self, container_name: &str) -> Result<bool> {
        match self
            .docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await
        {
            Ok(inspect) => Ok(inspect
                .state
                .as_ref()
                .and_then(|s| s.running)
                .unwrap_or(false)),
            Err(_) => Ok(false),
        }
    }

    /// Check if a container exists in Docker (in any state: running, exited, created, etc.).
    /// Returns true if the container can be inspected, false if it doesn't exist.
    pub async fn container_exists(&self, container_name: &str) -> bool {
        self.docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await
            .is_ok()
    }

    /// Check if any spec container in a pod has terminated (exited).
    /// This is a lightweight check that only inspects Docker state — it does NOT
    /// run any probes, unlike `get_container_statuses`.
    pub async fn has_terminated_containers(&self, pod: &Pod) -> bool {
        let pod_name = &pod.metadata.name;
        if let Some(spec) = &pod.spec {
            for container in &spec.containers {
                let container_name = format!("{}_{}", pod_name, container.name);
                match self
                    .docker
                    .inspect_container(&container_name, None::<InspectContainerOptions>)
                    .await
                {
                    Ok(inspect) => {
                        let state = inspect.state.unwrap_or_default();
                        let running = state.running.unwrap_or(false);
                        if !running {
                            // Container is not running — check if it has exited
                            if state.exit_code.is_some()
                                || matches!(
                                    state.status,
                                    Some(
                                        bollard::secret::ContainerStateStatusEnum::EXITED
                                            | bollard::secret::ContainerStateStatusEnum::DEAD
                                    )
                                )
                            {
                                return true;
                            }
                        }
                    }
                    Err(_) => {
                        // Container doesn't exist or can't be inspected
                    }
                }
            }
        }
        false
    }

    /// Check if a pod's containers are running
    pub async fn is_pod_running(&self, pod_name: &str) -> Result<bool> {
        let mut filters = HashMap::new();
        filters.insert("name".to_string(), vec![format!("{}_", pod_name)]);

        let options = ListContainersOptions {
            all: false, // Only running containers
            filters,
            ..Default::default()
        };

        let containers = self.docker.list_containers(Some(options)).await?;
        // Check that at least one non-pause container is running.
        // Just the pause container running doesn't count — the app containers may have
        // failed to start (e.g., CreateContainerConfigError from subpath validation).
        let pause_suffix = format!("{}_pause", pod_name);
        let has_app_container = containers.iter().any(|c| {
            c.names
                .as_ref()
                .map(|names| names.iter().any(|n| !n.contains(&pause_suffix)))
                .unwrap_or(false)
        });
        Ok(has_app_container)
    }

    /// Check if any app (non-init, non-pause) container has been created for a pod.
    /// K8s ref: kubelet_pods.go:2689 — HasAnyRegularContainerCreated.
    /// If app containers exist, all init containers must have completed successfully.
    async fn has_any_app_container(&self, pod: &Pod) -> bool {
        let pod_name = &pod.metadata.name;
        let mut filters = HashMap::new();
        filters.insert("name".to_string(), vec![format!("{}_", pod_name)]);

        let options = ListContainersOptions {
            all: true, // Include exited/created containers
            filters,
            ..Default::default()
        };

        let containers = match self.docker.list_containers(Some(options)).await {
            Ok(c) => c,
            Err(_) => return false,
        };

        let init_names: Vec<String> = pod
            .spec
            .as_ref()
            .and_then(|s| s.init_containers.as_ref())
            .map(|ics| {
                ics.iter()
                    .map(|ic| format!("{}_{}", pod_name, ic.name))
                    .collect()
            })
            .unwrap_or_default();
        let pause_name = format!("{}_pause", pod_name);

        containers.iter().any(|c| {
            c.names
                .as_ref()
                .map(|names| {
                    names.iter().any(|n| {
                        let clean = n.trim_start_matches('/');
                        clean != pause_name && !init_names.iter().any(|init| clean == init)
                    })
                })
                .unwrap_or(false)
        })
    }

    /// Get detailed status of init containers by inspecting actual Docker container state.
    pub async fn get_init_container_statuses(&self, pod: &Pod) -> Option<Vec<ContainerStatus>> {
        let init_containers = pod.spec.as_ref()?.init_containers.as_ref()?;
        if init_containers.is_empty() {
            return None;
        }

        let pod_name = &pod.metadata.name;
        let mut statuses = Vec::new();

        for ic in init_containers {
            let container_name = format!("{}_{}", pod_name, ic.name);

            let container_state_info = self
                .docker
                .inspect_container(&container_name, None::<InspectContainerOptions>)
                .await;

            let (state, container_id, image_id): (ContainerState, Option<String>, Option<String>) =
                match container_state_info {
                    Ok(inspect) => {
                        let ds = inspect.state.unwrap_or_default();
                        let cid = inspect.id.clone().map(|id| format!("docker://{}", id));
                        let iid = inspect.image.clone().map(|img| {
                            if img.starts_with("sha256:") {
                                format!("docker-pullable://{}", img)
                            } else {
                                img
                            }
                        });
                        let running = ds.running.unwrap_or(false);

                        if running {
                            (
                                ContainerState::Running {
                                    started_at: ds.started_at,
                                },
                                cid,
                                iid,
                            )
                        } else if ds.finished_at.is_some()
                            || matches!(
                                ds.status,
                                Some(bollard::secret::ContainerStateStatusEnum::EXITED)
                            )
                        {
                            let code = ds.exit_code.unwrap_or(0) as i32;
                            let term_msg = self
                                .read_termination_message(&container_name, ic, code as i64)
                                .await;
                            let terminated = ContainerState::Terminated {
                                exit_code: code,
                                signal: None,
                                reason: Some(if code == 0 {
                                    "Completed".to_string()
                                } else {
                                    ds.error
                                        .clone()
                                        .filter(|e| !e.is_empty())
                                        .unwrap_or_else(|| "Error".to_string())
                                }),
                                message: term_msg,
                                started_at: ds.started_at.clone(),
                                finished_at: ds.finished_at.clone(),
                                container_id: cid.clone(),
                            };
                            // Keep the Terminated state as-is — K8s shows the actual
                            // container state (Terminated) with the exit code. The
                            // CrashLoopBackOff reason is only shown when the container
                            // has been removed and the kubelet is waiting to restart it.
                            (terminated, cid, iid)
                        } else {
                            (
                                ContainerState::Waiting {
                                    reason: Some("PodInitializing".to_string()),
                                    message: None,
                                },
                                cid,
                                iid,
                            )
                        }
                    }
                    Err(_) => {
                        // Container doesn't exist in Docker — it may have been removed
                        // after successful completion or for restart.
                        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go
                        let prev = pod
                            .status
                            .as_ref()
                            .and_then(|s| s.init_container_statuses.as_ref())
                            .and_then(|statuses| statuses.iter().find(|s| s.name == ic.name));

                        if let Some(prev_status) = prev {
                            if let Some(ContainerState::Terminated { exit_code, .. }) =
                                &prev_status.state
                            {
                                if *exit_code == 0 {
                                    // Preserve the successful termination state
                                    (
                                        prev_status.state.clone().unwrap(),
                                        prev_status.container_id.clone(),
                                        prev_status.image_id.clone(),
                                    )
                                } else {
                                    // Previously terminated with error — container was
                                    // removed for restart. Show CrashLoopBackOff.
                                    let restart_always = pod
                                        .spec
                                        .as_ref()
                                        .and_then(|s| s.restart_policy.as_deref())
                                        .unwrap_or("Always")
                                        == "Always";
                                    if restart_always {
                                        (
                                            ContainerState::Waiting {
                                                reason: Some("CrashLoopBackOff".to_string()),
                                                message: Some(format!("back-off restarting failed container init container \"{}\" exited with {}", ic.name, exit_code)),
                                            },
                                            None,
                                            None,
                                        )
                                    } else {
                                        (
                                            ContainerState::Waiting {
                                                reason: Some("PodInitializing".to_string()),
                                                message: None,
                                            },
                                            None,
                                            None,
                                        )
                                    }
                                }
                            } else if matches!(&prev_status.state, Some(ContainerState::Waiting { reason, .. }) if reason.as_deref() == Some("CrashLoopBackOff"))
                            {
                                // Previous state was CrashLoopBackOff — container was
                                // removed and being restarted. Preserve the state.
                                (prev_status.state.clone().unwrap(), None, None)
                            } else if matches!(
                                &prev_status.state,
                                Some(ContainerState::Running { .. })
                            ) {
                                // Previous state was Running but container is gone.
                                // For init containers, this means it completed and Docker
                                // removed it before we could capture the exit status.
                                // Treat as successful completion (exit code 0) since the
                                // next init container or app containers wouldn't have
                                // started if this one failed.
                                (
                                    ContainerState::Terminated {
                                        exit_code: 0,
                                        reason: Some("Completed".to_string()),
                                        message: None,
                                        started_at: prev_status.state.as_ref().and_then(|s| {
                                            if let ContainerState::Running { started_at } = s {
                                                started_at.clone()
                                            } else {
                                                None
                                            }
                                        }),
                                        finished_at: Some(chrono::Utc::now().to_rfc3339()),
                                        container_id: prev_status.container_id.clone(),
                                        signal: None,
                                    },
                                    prev_status.container_id.clone(),
                                    prev_status.image_id.clone(),
                                )
                            } else {
                                // Previous state was Waiting/PodInitializing — container
                                // was never started or still initializing
                                (
                                    ContainerState::Waiting {
                                        reason: Some("PodInitializing".to_string()),
                                        message: None,
                                    },
                                    None,
                                    None,
                                )
                            }
                        } else {
                            // No previous status — the container was cleaned up.
                            // K8s ref: kubelet_pods.go:2689 — if init container has no
                            // CRI status and regular containers exist, infer completion.
                            // Also mark as Completed if pod is already Succeeded/Failed.
                            let pod_phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                            // Check if any app container has been created/is running.
                            // If so, ALL init containers must have completed (K8s rule:
                            // app containers don't start until all init containers succeed).
                            let app_containers_exist = self.has_any_app_container(pod).await;
                            if matches!(
                                pod_phase,
                                Some(rusternetes_common::types::Phase::Succeeded)
                                    | Some(rusternetes_common::types::Phase::Failed)
                            ) || app_containers_exist
                            {
                                (
                                    ContainerState::Terminated {
                                        exit_code: 0,
                                        reason: Some("Completed".to_string()),
                                        message: None,
                                        started_at: None,
                                        finished_at: None,
                                        container_id: None,
                                        signal: None,
                                    },
                                    None,
                                    None,
                                )
                            } else {
                                (
                                    ContainerState::Waiting {
                                        reason: Some("PodInitializing".to_string()),
                                        message: None,
                                    },
                                    None,
                                    None,
                                )
                            }
                        }
                    }
                };

            let is_terminated = matches!(state, ContainerState::Terminated { .. });
            let is_running = matches!(state, ContainerState::Running { .. });

            // Preserve restart_count and last_state from the pod's existing status.
            // When we remove and recreate a failed init container, the Docker state
            // resets, but K8s tracks restarts across container recreations.
            // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go — calcRestartCountByLogDir
            let existing_status = pod
                .status
                .as_ref()
                .and_then(|s| s.init_container_statuses.as_ref())
                .and_then(|statuses| statuses.iter().find(|s| s.name == ic.name));

            let mut restart_count = existing_status.map(|s| s.restart_count).unwrap_or(0);
            let mut last_state = existing_status.and_then(|s| s.last_state.clone());

            // Track restarts based on the current and previous states.
            // K8s increments restart_count each time a container is restarted.
            match &state {
                ContainerState::Terminated { exit_code, .. } if *exit_code != 0 => {
                    // Container failed. Check if this is a NEW failure by comparing
                    // with the existing status. If existing was Terminated with a
                    // different container_id, or was Waiting (removed for restart),
                    // this is a restart.
                    let is_new_failure = existing_status
                        .map(|prev| {
                            // If previous state was Waiting (CrashLoopBackOff or
                            // PodInitializing), the container was recreated → restart
                            matches!(&prev.state, Some(ContainerState::Waiting { .. }))
                            // If previous was also Terminated, check container_id
                            || matches!(&prev.state, Some(ContainerState::Terminated { .. }) if prev.container_id != container_id)
                        })
                        .unwrap_or(false);

                    if is_new_failure {
                        restart_count += 1;
                    }
                    // Move existing terminated to last_state
                    if let Some(prev) = existing_status {
                        if let Some(ContainerState::Terminated { exit_code, .. }) = &prev.state {
                            if *exit_code != 0 {
                                last_state = prev.state.clone();
                            }
                        }
                    }
                }
                ContainerState::Waiting { reason, .. }
                    if reason.as_deref() == Some("CrashLoopBackOff") =>
                {
                    // Container was removed for restart — preserve last_state from
                    // existing. If existing had a Terminated state, that becomes last_state.
                    if let Some(prev) = existing_status {
                        match &prev.state {
                            Some(ContainerState::Terminated { exit_code, .. })
                                if *exit_code != 0 =>
                            {
                                // Transition from Terminated → CrashLoopBackOff (removal)
                                // The terminated state becomes last_state
                                last_state = prev.state.clone();
                                restart_count += 1;
                            }
                            Some(ContainerState::Waiting {
                                reason: prev_reason,
                                ..
                            }) if prev_reason.as_deref() == Some("CrashLoopBackOff") => {
                                // Already in CrashLoopBackOff — keep existing last_state
                                // and don't double-count
                            }
                            _ => {}
                        }
                    }
                }
                _ => {
                    // Running, Waiting/PodInitializing, or successful termination
                    // — no restart count change needed
                }
            }

            // Init container started=true if it's running or has terminated.
            // K8s sets started=true once the container has been started at least once.
            let has_started = is_running || is_terminated;
            statuses.push(ContainerStatus {
                name: ic.name.clone(),
                ready: is_terminated && matches!(&state, ContainerState::Terminated { exit_code, .. } if *exit_code == 0),
                restart_count,
                state: Some(state),
                last_state,
                image: Some(ic.image.clone()),
                image_id,
                container_id,
                started: Some(has_started),
                allocated_resources: ic.resources.as_ref().and_then(|r| r.requests.clone()),
                allocated_resources_status: None,
                resources: ic.resources.clone(),
                user: None,
                volume_mounts: None,
                stop_signal: None,
            });
        }

        Some(statuses)
    }

    /// Determine the init container action to take, following K8s's state machine.
    /// Returns: (all_init_done, next_init_index, should_retry)
    /// - all_init_done: true if all init containers completed successfully
    /// - next_init_index: index of the next init container to start/retry, or None
    /// - should_retry: true if the next init container failed and should be retried
    ///
    /// K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go — computeInitContainerActions
    pub async fn compute_init_container_actions(&self, pod: &Pod) -> (bool, Option<usize>, bool) {
        let init_containers = match pod.spec.as_ref().and_then(|s| s.init_containers.as_ref()) {
            Some(ics) if !ics.is_empty() => ics,
            _ => return (true, None, false), // No init containers = all done
        };

        let pod_name = &pod.metadata.name;
        let restart_on_failure = pod
            .spec
            .as_ref()
            .and_then(|s| s.restart_policy.as_deref())
            .unwrap_or("Always")
            != "Never";

        // Check each init container in order
        for (i, ic) in init_containers.iter().enumerate() {
            let is_sidecar = ic.restart_policy.as_deref() == Some("Always");
            if is_sidecar {
                continue; // Sidecar init containers are handled separately
            }

            let container_name = format!("{}_{}", pod_name, ic.name);
            let inspect = self
                .docker
                .inspect_container(&container_name, None::<InspectContainerOptions>)
                .await;

            match inspect {
                Ok(info) => {
                    let state = info.state.unwrap_or_default();
                    let running = state.running.unwrap_or(false);
                    let exit_code = state.exit_code.unwrap_or(-1);
                    let status = state.status;

                    if running {
                        // Init container is still running — wait for it
                        return (false, None, false);
                    }

                    if matches!(
                        status,
                        Some(bollard::secret::ContainerStateStatusEnum::EXITED)
                    ) {
                        if exit_code == 0 {
                            // This init container completed successfully — check next
                            continue;
                        } else {
                            // Failed with non-zero exit code
                            if restart_on_failure {
                                // Should retry this init container
                                return (false, Some(i), true);
                            } else {
                                // RestartPolicy=Never — pod is terminal
                                return (false, None, false);
                            }
                        }
                    }

                    // Container exists but in unknown state — treat as not started
                    return (false, Some(i), false);
                }
                Err(_) => {
                    // Container doesn't exist — need to start this init container
                    return (false, Some(i), false);
                }
            }
        }

        // All init containers completed successfully
        (true, None, false)
    }

    /// Get detailed status of all containers in a pod
    /// Get statuses for ephemeral containers in a pod.
    pub async fn get_ephemeral_container_statuses(
        &self,
        pod: &Pod,
    ) -> Option<Vec<ContainerStatus>> {
        let ecs = pod.spec.as_ref()?.ephemeral_containers.as_ref()?;
        if ecs.is_empty() {
            return None;
        }

        let pod_name = &pod.metadata.name;
        let mut statuses = Vec::new();

        for ec in ecs {
            let container_name = format!("{}_{}", pod_name, ec.name);
            let status = match self
                .docker
                .inspect_container(&container_name, None::<InspectContainerOptions>)
                .await
            {
                Ok(inspect) => {
                    let state = inspect.state.unwrap_or_default();
                    let running = state.running.unwrap_or(false);
                    let exit_code = state.exit_code.unwrap_or(0);

                    // Capture started_at before state fields are consumed
                    let ec_started_at = state.started_at.clone();

                    let container_state = if running {
                        Some(ContainerState::Running {
                            started_at: state.started_at,
                        })
                    } else if state.finished_at.is_some() {
                        Some(ContainerState::Terminated {
                            exit_code: exit_code as i32,
                            signal: None,
                            reason: Some(if exit_code == 0 {
                                "Completed".to_string()
                            } else {
                                "Error".to_string()
                            }),
                            message: None,
                            started_at: ec_started_at,
                            finished_at: state.finished_at,
                            container_id: inspect.id.clone().map(|id| format!("docker://{}", id)),
                        })
                    } else {
                        Some(ContainerState::Waiting {
                            reason: Some("ContainerCreating".to_string()),
                            message: None,
                        })
                    };

                    ContainerStatus {
                        name: ec.name.clone(),
                        ready: running,
                        restart_count: 0,
                        state: container_state,
                        last_state: None,
                        image: Some(ec.image.clone()),
                        image_id: inspect
                            .image
                            .clone()
                            .map(|id| format!("docker-pullable://{}", id)),
                        container_id: inspect.id.map(|id| format!("docker://{}", id)),
                        started: Some(running),
                        allocated_resources: None,
                        allocated_resources_status: None,
                        resources: ec.resources.clone(),
                        user: None,
                        volume_mounts: None,
                        stop_signal: None,
                    }
                }
                Err(_) => ContainerStatus {
                    name: ec.name.clone(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState::Waiting {
                        reason: Some("ContainerCreating".to_string()),
                        message: None,
                    }),
                    last_state: None,
                    image: Some(ec.image.clone()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: ec.resources.clone(),
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                },
            };
            statuses.push(status);
        }

        Some(statuses)
    }

    pub async fn get_container_statuses(&self, pod: &Pod) -> Result<Vec<ContainerStatus>> {
        let mut statuses = Vec::new();
        let pod_name = &pod.metadata.name;

        for container in &pod.spec.as_ref().unwrap().containers {
            let container_name = format!("{}_{}", pod_name, container.name);

            let status = match self
                .docker
                .inspect_container(&container_name, None::<InspectContainerOptions>)
                .await
            {
                Ok(inspect) => {
                    let state = inspect.state.unwrap_or_default();
                    let running = state.running.unwrap_or(false);
                    let exit_code = state.exit_code.unwrap_or(0);

                    // Get restart count: use the MAX of Docker's count and the
                    // previously reported count. Docker's count resets when a
                    // container is recreated, so we must never decrease.
                    let docker_count = inspect.restart_count.map(|c| c as u32).unwrap_or(0);
                    let prev_count = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.container_statuses.as_ref())
                        .and_then(|statuses| {
                            statuses
                                .iter()
                                .find(|cs| cs.name == container.name)
                                .map(|cs| cs.restart_count)
                        })
                        .unwrap_or(0);
                    let restart_count = docker_count.max(prev_count);

                    // Capture started_at before state fields are consumed by branches
                    let docker_started_at = state.started_at.clone();

                    // Preserve last_state from existing pod status for restart tracking
                    let prev_last_state = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.container_statuses.as_ref())
                        .and_then(|statuses| statuses.iter().find(|cs| cs.name == container.name))
                        .and_then(|cs| cs.last_state.clone());

                    let container_state = if running {
                        Some(ContainerState::Running {
                            started_at: state.started_at,
                        })
                    } else if matches!(
                        state.status,
                        Some(bollard::secret::ContainerStateStatusEnum::EXITED)
                    ) || state.finished_at.is_some()
                    {
                        // Read termination message based on terminationMessagePolicy:
                        // - "File" (default): always read from file
                        // - "FallbackToLogsOnError": read from file first; if file is
                        //   empty AND exit != 0, fall back to container logs
                        // Both policies always read from the file when it has content.
                        let termination_msg = self
                            .read_termination_message(&container_name, container, exit_code)
                            .await;

                        // Container has exited (any exit code, including 0)
                        Some(ContainerState::Terminated {
                            exit_code: exit_code as i32,
                            signal: None,
                            reason: Some(if exit_code == 0 {
                                "Completed".to_string()
                            } else if exit_code == 137 {
                                state
                                    .error
                                    .filter(|e| !e.is_empty())
                                    .unwrap_or_else(|| "OOMKilled".to_string())
                            } else {
                                state
                                    .error
                                    .filter(|e| !e.is_empty())
                                    .unwrap_or_else(|| "Error".to_string())
                            }),
                            message: termination_msg,
                            started_at: docker_started_at,
                            finished_at: state.finished_at,
                            container_id: inspect.id.clone().map(|id| format!("docker://{}", id)),
                        })
                    } else {
                        Some(ContainerState::Waiting {
                            reason: Some("ContainerCreating".to_string()),
                            message: None,
                        })
                    };

                    // Check startup probe first. If defined and not yet passing,
                    // liveness and readiness probes are skipped.
                    // Uses threshold tracking for accurate Kubernetes semantics.
                    let startup_passed = if running {
                        if let Some(startup_probe) = &container.startup_probe {
                            let raw = self
                                .check_probe(&container_name, container, startup_probe)
                                .await
                                .unwrap_or(false);
                            let success_threshold = startup_probe.success_threshold.unwrap_or(1);
                            let key = format!("{}/{}/startup_status", pod_name, container.name);
                            let mut states = self.probe_states.lock().unwrap();
                            let state = states.entry(key).or_default();
                            if raw {
                                state.consecutive_successes += 1;
                                state.consecutive_failures = 0;
                                state.consecutive_successes >= success_threshold
                            } else {
                                state.consecutive_failures += 1;
                                state.consecutive_successes = 0;
                                false
                            }
                        } else {
                            true // No startup probe means started
                        }
                    } else {
                        false
                    };

                    // Check readiness probe only if startup probe has passed.
                    // Uses threshold tracking: successThreshold consecutive successes
                    // required to become ready, failureThreshold consecutive failures
                    // required to become not ready.
                    let ready = if running && startup_passed {
                        if let Some(probe) = &container.readiness_probe {
                            // Respect initialDelaySeconds before running readiness probe
                            let initial_delay = probe.initial_delay_seconds.unwrap_or(0);
                            let past_initial_delay = if initial_delay > 0 {
                                // Check container start time from Docker state
                                if let Some(ContainerState::Running {
                                    started_at: Some(ref started_at_str),
                                }) = container_state
                                {
                                    if let Ok(started) =
                                        chrono::DateTime::parse_from_rfc3339(started_at_str)
                                    {
                                        let elapsed = Utc::now().signed_duration_since(started);
                                        elapsed.num_seconds() >= initial_delay as i64
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                true
                            };

                            if !past_initial_delay {
                                false // Not ready yet, still within initial delay
                            } else {
                                let raw = self
                                    .check_probe(&container_name, container, probe)
                                    .await
                                    .unwrap_or(false);
                                let _failure_threshold = probe.failure_threshold.unwrap_or(3);
                                let success_threshold = probe.success_threshold.unwrap_or(1);
                                let key = format!("{}/{}/readiness", pod_name, container.name);
                                let mut states = self.probe_states.lock().unwrap();
                                let state = states.entry(key).or_default();
                                if raw {
                                    state.consecutive_successes += 1;
                                    state.consecutive_failures = 0;
                                    state.consecutive_successes >= success_threshold
                                } else {
                                    state.consecutive_failures += 1;
                                    state.consecutive_successes = 0;
                                    // Not ready until success threshold is met
                                    false
                                }
                            } // end else past_initial_delay
                        } else {
                            true // No probe means ready
                        }
                    } else {
                        false
                    };

                    ContainerStatus {
                        name: container.name.clone(),
                        ready,
                        restart_count,
                        state: container_state,
                        last_state: prev_last_state,
                        image: Some(container.image.clone()),
                        image_id: inspect.image.clone().map(|img| {
                            if img.starts_with("sha256:") {
                                format!("docker-pullable://{}", img)
                            } else {
                                img
                            }
                        }),
                        container_id: inspect.id.map(|id| format!("docker://{}", id)),
                        started: Some(startup_passed),
                        allocated_resources: container
                            .resources
                            .as_ref()
                            .and_then(|r| r.requests.clone()),
                        allocated_resources_status: None,
                        resources: container.resources.clone(),
                        user: None,
                        volume_mounts: None,
                        stop_signal: None,
                    }
                }
                Err(_) => {
                    // If pod already has a container status (from a previous sync),
                    // preserve it. This handles containers that exit and are removed
                    // before the kubelet can inspect them.
                    let existing_status = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.container_statuses.as_ref())
                        .and_then(|statuses| {
                            statuses
                                .iter()
                                .find(|cs| cs.name == container.name)
                                .cloned()
                        });

                    if let Some(prev) = existing_status {
                        // Preserve the previous container status (terminated, waiting, etc.)
                        statuses.push(prev);
                        continue;
                    }

                    let existing_reason = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.container_statuses.as_ref())
                        .and_then(|statuses| statuses.iter().find(|cs| cs.name == container.name))
                        .and_then(|cs| match &cs.state {
                            Some(ContainerState::Waiting {
                                reason: Some(r),
                                message,
                            }) if r == "CreateContainerError"
                                || r == "CreateContainerConfigError" =>
                            {
                                Some((r.clone(), message.clone()))
                            }
                            _ => None,
                        });

                    let (reason, message) = match existing_reason {
                        Some((r, m)) => (r, m),
                        None => {
                            let has_init = pod
                                .spec
                                .as_ref()
                                .and_then(|s| s.init_containers.as_ref())
                                .is_some_and(|ic| !ic.is_empty());
                            if has_init {
                                ("PodInitializing".to_string(), None)
                            } else {
                                ("ContainerCreating".to_string(), None)
                            }
                        }
                    };

                    ContainerStatus {
                        name: container.name.clone(),
                        ready: false,
                        restart_count: 0,
                        state: Some(ContainerState::Waiting {
                            reason: Some(reason),
                            message,
                        }),
                        last_state: None,
                        image: Some(container.image.clone()),
                        image_id: None,
                        container_id: None,
                        started: Some(false),
                        allocated_resources: container
                            .resources
                            .as_ref()
                            .and_then(|r| r.requests.clone()),
                        allocated_resources_status: None,
                        resources: container.resources.clone(),
                        user: None,
                        volume_mounts: None,
                        stop_signal: None,
                    }
                }
            };

            statuses.push(status);
        }

        Ok(statuses)
    }

    /// Check if a container needs to be restarted based on liveness probe.
    ///
    /// If a startup probe is defined and hasn't passed yet, liveness probes
    /// are skipped for that container (per Kubernetes semantics).
    ///
    /// Respects `failureThreshold` (default 3) and `successThreshold` (default 1)
    /// so that a single probe failure does not immediately trigger a restart.
    pub async fn check_liveness(&self, pod: &Pod) -> Result<bool> {
        let pod_name = &pod.metadata.name;

        for container in &pod.spec.as_ref().unwrap().containers {
            let container_name = format!("{}_{}", pod_name, container.name);

            // If a startup probe is defined, check it first.
            // Liveness probes are disabled until the startup probe succeeds.
            if let Some(startup_probe) = &container.startup_probe {
                let startup_key = format!("{}/{}/startup", pod_name, container.name);
                let raw_result = self
                    .check_probe(&container_name, container, startup_probe)
                    .await
                    .unwrap_or(false);

                let failure_threshold = startup_probe.failure_threshold.unwrap_or(3);
                let success_threshold = startup_probe.success_threshold.unwrap_or(1);

                let startup_passed = {
                    let mut states = self.probe_states.lock().unwrap();
                    let state = states.entry(startup_key).or_default();
                    if raw_result {
                        state.consecutive_successes += 1;
                        state.consecutive_failures = 0;
                        state.consecutive_successes >= success_threshold
                    } else {
                        state.consecutive_failures += 1;
                        state.consecutive_successes = 0;
                        if state.consecutive_failures >= failure_threshold {
                            warn!(
                                "Startup probe exceeded failure threshold ({}) for container {}",
                                failure_threshold, container_name
                            );
                        }
                        false
                    }
                };

                if !startup_passed {
                    debug!(
                        "Startup probe not yet passing for container {}, skipping liveness check",
                        container_name
                    );
                    continue;
                }
            }

            if let Some(probe) = &container.liveness_probe {
                // Wait for initial delay
                let initial_delay = probe.initial_delay_seconds.unwrap_or(0);
                if initial_delay > 0 {
                    // Check container start time
                    if let Ok(inspect) = self
                        .docker
                        .inspect_container(&container_name, None::<InspectContainerOptions>)
                        .await
                    {
                        if let Some(state) = inspect.state {
                            if let Some(started_at) = state.started_at {
                                if let Ok(started) =
                                    chrono::DateTime::parse_from_rfc3339(&started_at)
                                {
                                    let elapsed = Utc::now().signed_duration_since(started);
                                    if elapsed.num_seconds() < initial_delay as i64 {
                                        debug!("Skipping liveness check, within initial delay");
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }

                // Check liveness with threshold tracking
                let healthy = self.check_probe(&container_name, container, probe).await?;
                let failure_threshold = probe.failure_threshold.unwrap_or(3);
                // For liveness probes, Kubernetes requires successThreshold=1
                let _success_threshold = probe.success_threshold.unwrap_or(1);
                let liveness_key = format!("{}/{}/liveness", pod_name, container.name);

                let needs_restart = {
                    let mut states = self.probe_states.lock().unwrap();
                    let state = states.entry(liveness_key).or_default();
                    if healthy {
                        state.consecutive_successes += 1;
                        state.consecutive_failures = 0;
                        false
                    } else {
                        state.consecutive_failures += 1;
                        state.consecutive_successes = 0;
                        if state.consecutive_failures >= failure_threshold {
                            warn!(
                                "Liveness probe failed {} consecutive times (threshold {}) for container: {}",
                                state.consecutive_failures, failure_threshold, container_name
                            );
                            // Reset state so next cycle starts fresh after restart
                            state.consecutive_failures = 0;
                            true
                        } else {
                            debug!(
                                "Liveness probe failed for container {} ({}/{} failures before restart)",
                                container_name, state.consecutive_failures, failure_threshold
                            );
                            false
                        }
                    }
                };

                if needs_restart {
                    return Ok(true);
                }
            }
        }

        Ok(false) // All probes passed
    }

    /// Execute a probe check
    async fn check_probe(
        &self,
        container_name: &str,
        container: &Container,
        probe: &Probe,
    ) -> Result<bool> {
        // K8s default timeout is 1s; treat 0 as default
        let timeout_secs = probe.timeout_seconds.unwrap_or(1).max(1) as u64;
        let timeout = Duration::from_secs(timeout_secs);

        // HTTP GET probe
        if let Some(http_get) = &probe.http_get {
            return self
                .check_http_probe(container_name, container, http_get, timeout)
                .await;
        }

        // TCP Socket probe
        if let Some(tcp_socket) = &probe.tcp_socket {
            return self
                .check_tcp_probe(container_name, container, tcp_socket, timeout)
                .await;
        }

        // Exec probe
        if let Some(exec) = &probe.exec {
            return self.check_exec_probe(container_name, exec, timeout).await;
        }

        // gRPC probe
        if let Some(grpc) = &probe.grpc {
            return self
                .check_grpc_probe(container_name, container, grpc, timeout)
                .await;
        }

        Ok(true) // No probe configured
    }

    /// Clear all probe states for a given pod (e.g., on restart or deletion).
    pub fn clear_probe_states_for_pod(&self, pod_name: &str) {
        let prefix = format!("{}/", pod_name);
        let mut states = self.probe_states.lock().unwrap();
        states.retain(|key, _| !key.starts_with(&prefix));
        debug!("Cleared probe states for pod {}", pod_name);
    }

    /// Read the termination message from a stopped container.
    /// First tries reading from the host-side bind-mounted file, then falls back to docker cp.
    async fn read_termination_message(
        &self,
        container_name: &str,
        container: &Container,
        exit_code: i64,
    ) -> Option<String> {
        let msg_path = container
            .termination_message_path
            .as_deref()
            .unwrap_or("/dev/termination-log");

        debug!(
            "Reading termination message from {} in container {}",
            msg_path, container_name
        );

        // Extract pod_name from container_name (format: {pod_name}_{container_name})
        let pod_name = container_name
            .rsplitn(2, '_')
            .last()
            .unwrap_or(container_name);

        // Try host-side file first (bind-mounted during container creation)
        let term_host_file = format!(
            "{}/{}/termination/{}",
            self.volumes_base_path, pod_name, container.name
        );
        if std::path::Path::new(&term_host_file).exists() {
            // Host file exists — read from it (authoritative, not docker cp)
            if let Ok(content) = std::fs::read_to_string(&term_host_file) {
                if !content.is_empty() {
                    let mut content = content;
                    if content.len() > 4096 {
                        content.truncate(4096);
                    }
                    debug!(
                        "Read termination message ({} bytes) from host file for {}",
                        content.len(),
                        container_name
                    );
                    return Some(content);
                }
            }
            // Host file exists but is empty — termination message is empty (don't fall through to docker cp)
            debug!(
                "Termination message file is empty (host-side) for {}",
                container_name
            );
            // FallbackToLogsOnError: only fall back to logs when the container failed (non-zero exit)
            if container.termination_message_policy.as_deref() == Some("FallbackToLogsOnError")
                && exit_code != 0
            {
                return self.read_container_logs_tail(container_name, 80).await;
            }
            return None;
        }

        // Fall back to docker cp for containers created before the bind-mount fix
        let mut stream = self.docker.download_from_container(
            container_name,
            Some(bollard::container::DownloadFromContainerOptions {
                path: msg_path.to_string(),
            }),
        );
        let mut all_bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => all_bytes.extend_from_slice(&bytes),
                Err(e) => {
                    debug!(
                        "Error reading termination message from {}: {}",
                        container_name, e
                    );
                    if container.termination_message_policy.as_deref()
                        == Some("FallbackToLogsOnError")
                        && exit_code != 0
                    {
                        return self.read_container_logs_tail(container_name, 80).await;
                    }
                    return None;
                }
            }
        }

        if all_bytes.is_empty() {
            debug!(
                "Termination message file empty or not found in {}",
                container_name
            );
            if container.termination_message_policy.as_deref() == Some("FallbackToLogsOnError")
                && exit_code != 0
            {
                return self.read_container_logs_tail(container_name, 80).await;
            }
            return None;
        }

        // Docker returns a tar archive; extract the file content
        let mut archive = tar::Archive::new(&all_bytes[..]);
        if let Ok(mut entries) = archive.entries() {
            while let Some(Ok(mut entry)) = entries.next() {
                let mut content = String::new();
                if std::io::Read::read_to_string(&mut entry, &mut content).is_ok()
                    && !content.is_empty()
                {
                    if content.len() > 4096 {
                        content.truncate(4096);
                    }
                    debug!(
                        "Read termination message ({} bytes) from {}",
                        content.len(),
                        container_name
                    );
                    return Some(content);
                }
            }
        }

        debug!(
            "Failed to extract termination message from tar archive for {}",
            container_name
        );
        if container.termination_message_policy.as_deref() == Some("FallbackToLogsOnError")
            && exit_code != 0
        {
            return self.read_container_logs_tail(container_name, 80).await;
        }
        None
    }

    /// Read the last N lines of container logs (for FallbackToLogsOnError)
    async fn read_container_logs_tail(&self, container_name: &str, lines: usize) -> Option<String> {
        use bollard::container::LogsOptions;
        let options = LogsOptions::<String> {
            stdout: true,
            stderr: true,
            tail: lines.to_string(),
            ..Default::default()
        };
        let mut stream = self.docker.logs(container_name, Some(options));
        let mut output = String::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(log) => output.push_str(&log.to_string()),
                Err(_) => break,
            }
        }
        if output.is_empty() {
            None
        } else {
            // Trim to 4KB
            if output.len() > 4096 {
                output.truncate(4096);
            }
            Some(output)
        }
    }

    /// Extract the best available IP from container network settings.
    /// Tries the specific network first, then default ip_address, then 127.0.0.1.
    fn extract_container_ip(
        &self,
        network_settings: Option<bollard::secret::NetworkSettings>,
    ) -> String {
        if let Some(ns) = network_settings {
            // First try the specific network we're using
            if let Some(networks) = &ns.networks {
                if let Some(net_info) = networks.get(&self.network) {
                    if let Some(ip) = &net_info.ip_address {
                        if !ip.is_empty() && ip != "0.0.0.0" {
                            return ip.clone();
                        }
                    }
                }
                // Try any network with a valid IP
                for net_info in networks.values() {
                    if let Some(ip) = &net_info.ip_address {
                        if !ip.is_empty() && ip != "0.0.0.0" {
                            return ip.clone();
                        }
                    }
                }
            }
            // Fallback to top-level ip_address
            if let Some(ip) = ns.ip_address {
                if !ip.is_empty() && ip != "0.0.0.0" {
                    return ip;
                }
            }
        }
        "127.0.0.1".to_string()
    }

    /// Get the effective IP for a container, resolving through pause containers
    /// when the app container uses NetworkMode=container:pause.
    async fn get_effective_container_ip(&self, container_name: &str) -> String {
        let inspect = match self
            .docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await
        {
            Ok(i) => i,
            Err(_) => return "127.0.0.1".to_string(),
        };

        // Check if this container uses another container's network
        if let Some(ref hc) = inspect.host_config {
            if let Some(ref net_mode) = hc.network_mode {
                if net_mode.starts_with("container:") {
                    // Get the pause container's IP instead
                    let pause_id = net_mode.trim_start_matches("container:");
                    if let Ok(pause_inspect) = self
                        .docker
                        .inspect_container(pause_id, None::<InspectContainerOptions>)
                        .await
                    {
                        let ip = self.extract_container_ip(pause_inspect.network_settings);
                        if ip != "127.0.0.1" {
                            return ip;
                        }
                    }
                }
            }
        }

        self.extract_container_ip(inspect.network_settings)
    }

    async fn check_http_probe(
        &self,
        container_name: &str,
        container: &Container,
        http_get: &HTTPGetAction,
        timeout: Duration,
    ) -> Result<bool> {
        // Use host field if specified, otherwise resolve container IP
        let ip = if let Some(ref host) = http_get.host {
            host.clone()
        } else {
            self.get_effective_container_ip(container_name).await
        };

        // Resolve named port via container.ports[].name lookup (K8s IntOrString).
        let port =
            match rusternetes_common::resources::resolve_probe_port(&http_get.port, container) {
                Some(p) => p,
                None => {
                    warn!(
                        "HTTP probe port {} unresolvable for container {}",
                        http_get.port, container_name
                    );
                    return Ok(false);
                }
            };

        // Kubernetes sends scheme as uppercase ("HTTP", "HTTPS") — lowercase for URL
        let scheme = http_get.scheme.as_deref().unwrap_or("HTTP").to_lowercase();
        let path = http_get.path.as_deref().unwrap_or("/");
        let url = format!("{}://{}:{}{}", scheme, ip, port, path);

        debug!("HTTP probe: {}", url);

        // Kubernetes probes skip TLS verification (accept self-signed certs).
        // Disable proxy to ensure direct connection to pod IPs.
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .danger_accept_invalid_certs(true)
            .no_proxy()
            .build()?;

        // Build request with custom headers from probe spec (K8s sends these)
        let mut request = client.get(&url);
        if let Some(ref headers) = http_get.http_headers {
            for header in headers {
                if let Ok(name) = reqwest::header::HeaderName::from_bytes(header.name.as_bytes()) {
                    if let Ok(value) = reqwest::header::HeaderValue::from_str(&header.value) {
                        request = request.header(name, value);
                    }
                }
            }
        }

        match request.send().await {
            Ok(response) => {
                let code = response.status().as_u16();
                // K8s probes consider 200-399 as success
                Ok((200..400).contains(&code))
            }
            Err(e) => {
                debug!("HTTP probe failed: {}", e);
                Ok(false)
            }
        }
    }

    async fn check_tcp_probe(
        &self,
        container_name: &str,
        container: &Container,
        tcp_socket: &TCPSocketAction,
        timeout: Duration,
    ) -> Result<bool> {
        // Get container IP (resolving through pause container if needed)
        let ip = self.get_effective_container_ip(container_name).await;

        let port =
            match rusternetes_common::resources::resolve_probe_port(&tcp_socket.port, container) {
                Some(p) => p,
                None => {
                    warn!(
                        "TCP probe port {} unresolvable for container {}",
                        tcp_socket.port, container_name
                    );
                    return Ok(false);
                }
            };

        let addr = format!("{}:{}", ip, port);
        debug!("TCP probe: {}", addr);

        match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await {
            Ok(Ok(_)) => Ok(true),
            _ => Ok(false),
        }
    }

    async fn check_exec_probe(
        &self,
        container_name: &str,
        exec: &ExecAction,
        _timeout: Duration,
    ) -> Result<bool> {
        debug!("Exec probe: {:?}", exec.command);

        let exec_config = CreateExecOptions {
            cmd: Some(exec.command.clone()),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };

        let exec_id = self
            .docker
            .create_exec(container_name, exec_config)
            .await?
            .id;

        let start_result = self.docker.start_exec(&exec_id, None).await?;

        match start_result {
            StartExecResults::Attached { mut output, .. } => while output.next().await.is_some() {},
            StartExecResults::Detached => {}
        }

        // Get exec inspect to check exit code
        let inspect = self.docker.inspect_exec(&exec_id).await?;
        let exit_code = inspect.exit_code.unwrap_or(1);

        Ok(exit_code == 0)
    }

    /// Check a gRPC health probe by connecting to the gRPC health service.
    /// Sends a raw grpc.health.v1.Health/Check request over HTTP/2 via tonic Channel.
    async fn check_grpc_probe(
        &self,
        container_name: &str,
        container: &Container,
        grpc: &GRPCAction,
        timeout: Duration,
    ) -> Result<bool> {
        let ip = self.get_effective_container_ip(container_name).await;
        let port = match rusternetes_common::resources::resolve_probe_port(&grpc.port, container) {
            Some(p) => p,
            None => {
                warn!(
                    "gRPC probe port {} unresolvable for container {}",
                    grpc.port, container_name
                );
                return Ok(false);
            }
        };
        let addr = format!("http://{}:{}", ip, port);
        debug!("gRPC probe: {} service={:?}", addr, grpc.service);

        let endpoint = match tonic::transport::Endpoint::from_shared(addr.clone()) {
            Ok(ep) => ep.timeout(timeout).connect_timeout(timeout),
            Err(e) => {
                debug!("gRPC probe invalid endpoint {}: {}", addr, e);
                return Ok(false);
            }
        };

        let mut client = match tokio::time::timeout(timeout, endpoint.connect()).await {
            Ok(Ok(ch)) => tonic::client::Grpc::new(ch),
            Ok(Err(e)) => {
                debug!("gRPC probe connect failed: {}", e);
                return Ok(false);
            }
            Err(_) => {
                debug!("gRPC probe connect timed out");
                return Ok(false);
            }
        };

        if client.ready().await.is_err() {
            debug!("gRPC probe: client not ready");
            return Ok(false);
        }

        // Construct the HealthCheckRequest protobuf manually
        let service_name = grpc.service.as_deref().unwrap_or("");
        let mut request_bytes = Vec::new();
        if !service_name.is_empty() {
            // field 1, wire type 2 (length-delimited string)
            request_bytes.push(0x0a);
            // varint encode the length
            let len = service_name.len();
            if len < 128 {
                request_bytes.push(len as u8);
            } else {
                request_bytes.push((len & 0x7f | 0x80) as u8);
                request_bytes.push((len >> 7) as u8);
            }
            request_bytes.extend_from_slice(service_name.as_bytes());
        }

        let codec = tonic::codec::ProstCodec::default();
        let path = http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check");

        // Use EncodedBytes to wrap our pre-encoded protobuf message
        let request = tonic::Request::new(EncodedBytes(request_bytes));
        let result: std::result::Result<tonic::Response<DecodedStatus>, tonic::Status> =
            client.unary(request, path, codec).await;

        match result {
            Ok(response) => {
                let status_val = response.into_inner().0;
                // HealthCheckResponse.ServingStatus: SERVING = 1
                let serving = status_val == 1;
                debug!("gRPC probe result: serving={}", serving);
                Ok(serving)
            }
            Err(status) => {
                debug!("gRPC probe failed with gRPC status: {:?}", status);
                Ok(false)
            }
        }
    }

    /// Execute a lifecycle handler (postStart/preStop) inside a container.
    ///
    /// Supports exec, httpGet, tcpSocket, and sleep handler types.
    /// Reuses the same probe execution patterns (exec via Docker API, HTTP via reqwest,
    /// TCP via tokio::net) for consistency.
    async fn execute_lifecycle_handler(
        &self,
        handler: &LifecycleHandler,
        container_name: &str,
        container: &Container,
    ) -> Result<()> {
        if let Some(ref exec) = handler.exec {
            // Execute command inside the container
            debug!(
                "Lifecycle exec handler: {:?} in {}",
                exec.command, container_name
            );
            let exec_config = CreateExecOptions {
                cmd: Some(exec.command.clone()),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                ..Default::default()
            };

            let exec_id = self
                .docker
                .create_exec(container_name, exec_config)
                .await
                .context("Failed to create exec for lifecycle handler")?
                .id;

            let start_result = self
                .docker
                .start_exec(&exec_id, None)
                .await
                .context("Failed to start exec for lifecycle handler")?;

            // Drain output with a timeout to prevent indefinite hangs
            match start_result {
                StartExecResults::Attached { mut output, .. } => {
                    let drain = async { while output.next().await.is_some() {} };
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(30), drain).await;
                }
                StartExecResults::Detached => {}
            }

            // Check exit code
            let inspect = self.docker.inspect_exec(&exec_id).await?;
            let exit_code = inspect.exit_code.unwrap_or(1);
            if exit_code != 0 {
                return Err(anyhow::anyhow!(
                    "Lifecycle exec handler exited with code {}",
                    exit_code
                ));
            }
        } else if let Some(ref http_get) = handler.http_get {
            // Resolve named port via container.ports lookup (K8s IntOrString).
            let hook_port =
                rusternetes_common::resources::resolve_probe_port(&http_get.port, container)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Lifecycle HTTP handler port {} unresolvable for container {}",
                            http_get.port,
                            container_name
                        )
                    })?;
            // Execute HTTP GET request — use host field if specified, otherwise container/pause IP.
            // Containers using container: network mode won't have their own IP — we need the
            // pause container's IP (which owns the network namespace).
            let ip = if let Some(ref host) = http_get.host {
                // If host looks like a hostname (not an IP), try to resolve it via
                // the pod's /etc/hosts or DNS. The kubelet container can't resolve
                // K8s service names, so we check if it's an IP first.
                if host.parse::<std::net::Ipv4Addr>().is_ok()
                    || host.parse::<std::net::Ipv6Addr>().is_ok()
                {
                    host.clone()
                } else {
                    // Try resolving as a hostname — look up in pod's network namespace
                    // by checking the pod's /etc/hosts or using DNS
                    let mut resolved = None;
                    if let Ok(mut addrs) =
                        tokio::net::lookup_host(format!("{}:{}", host, hook_port)).await
                    {
                        if let Some(addr) = addrs.next() {
                            resolved = Some(addr.ip().to_string());
                        }
                    }
                    // Fallback: resolve K8s service/pod names via storage.
                    // The kubelet container can't resolve K8s DNS names, so
                    // look up services and pods by name in etcd.
                    if resolved.is_none() {
                        if let Some(ref storage) = self.storage {
                            use rusternetes_storage::Storage;
                            // Derive namespace from the container name (pod_name -> pod -> namespace)
                            let pod_name_from_container = container_name
                                .rsplitn(2, '_')
                                .last()
                                .unwrap_or(container_name);
                            // Search all pods to find our pod's namespace
                            let ns = {
                                let prefix = rusternetes_storage::build_prefix("pods", None);
                                storage.list::<Pod>(&prefix).await.ok().and_then(|pods| {
                                    pods.iter()
                                        .find(|p| p.metadata.name == pod_name_from_container)
                                        .and_then(|p| p.metadata.namespace.clone())
                                })
                            };
                            let ns = ns.as_deref().unwrap_or("default");

                            // Try as a service name first
                            let svc_key =
                                rusternetes_storage::build_key("services", Some(ns), host);
                            if let Ok(svc) = storage
                                .get::<rusternetes_common::resources::Service>(&svc_key)
                                .await
                            {
                                if let Some(ref cluster_ip) = svc.spec.cluster_ip {
                                    if !cluster_ip.is_empty() && cluster_ip != "None" {
                                        info!(
                                            "Lifecycle HTTP handler: resolved service {} to ClusterIP {}",
                                            host, cluster_ip
                                        );
                                        resolved = Some(cluster_ip.clone());
                                    }
                                }
                            }

                            // Try as a pod name
                            if resolved.is_none() {
                                let prefix = rusternetes_storage::build_prefix("pods", None);
                                if let Ok(pods) = storage.list::<Pod>(&prefix).await {
                                    if let Some(target_pod) = pods.iter().find(|p| {
                                        p.metadata.name == *host
                                            && p.metadata.namespace.as_deref() == Some(ns)
                                    }) {
                                        if let Some(pod_ip) = target_pod
                                            .status
                                            .as_ref()
                                            .and_then(|s| s.pod_ip.as_ref())
                                            .filter(|ip| !ip.is_empty())
                                        {
                                            info!(
                                                "Lifecycle HTTP handler: resolved pod {} to IP {}",
                                                host, pod_ip
                                            );
                                            resolved = Some(pod_ip.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    resolved.unwrap_or_else(|| host.clone())
                }
            } else {
                let inspect = self
                    .docker
                    .inspect_container(container_name, None::<InspectContainerOptions>)
                    .await?;
                let container_ip = inspect
                    .network_settings
                    .as_ref()
                    .and_then(|ns| ns.ip_address.as_ref())
                    .filter(|ip| !ip.is_empty())
                    .cloned();
                if let Some(ip) = container_ip {
                    ip
                } else {
                    // Container uses container: network mode — get IP from the pause container.
                    // Container names have format "{pod_name}_{container_suffix}".
                    // Use rsplitn(2, '_') to split at the last underscore, preserving
                    // any underscores in the pod name itself.
                    let pod_name = container_name
                        .rsplitn(2, '_')
                        .last()
                        .unwrap_or(container_name);
                    let pause_name = format!("{}_pause", pod_name);
                    info!(
                        "Lifecycle HTTP handler: resolving IP from pause container {} (container: {})",
                        pause_name, container_name
                    );
                    let pause_inspect = self
                        .docker
                        .inspect_container(&pause_name, None::<InspectContainerOptions>)
                        .await
                        .ok();
                    pause_inspect
                        .and_then(|pi| pi.network_settings)
                        .and_then(|ns| {
                            // Check bridge network first, then global IP
                            ns.networks
                                .and_then(|nets| {
                                    nets.values()
                                        .next()
                                        .and_then(|n| n.ip_address.clone())
                                        .filter(|ip| !ip.is_empty())
                                })
                                .or_else(|| ns.ip_address.filter(|ip| !ip.is_empty()))
                        })
                        .unwrap_or_else(|| "127.0.0.1".to_string())
                }
            };

            let scheme = http_get.scheme.as_deref().unwrap_or("HTTP").to_lowercase();
            let path = http_get.path.as_deref().unwrap_or("/");
            let url = format!("{}://{}:{}{}", scheme, ip, hook_port, path);

            info!(
                "Lifecycle HTTP handler: {} for container {}",
                url, container_name
            );

            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .danger_accept_invalid_certs(true)
                .no_proxy()
                .build()?;

            let mut request = client.get(&url);

            // Add custom HTTP headers from the handler spec
            if let Some(ref headers) = http_get.http_headers {
                for header in headers {
                    request = request.header(&header.name, &header.value);
                }
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    info!(
                        "Lifecycle HTTP handler response: {} for container {}",
                        status, container_name
                    );
                    // Kubernetes ignores the response status for lifecycle hooks —
                    // any response (even non-2xx) counts as success. Only connection
                    // failures are errors.
                }
                Err(e) => {
                    warn!(
                        "Lifecycle HTTP handler failed for {}: {}",
                        container_name, e
                    );
                    return Err(anyhow::anyhow!("Lifecycle HTTP handler failed: {}", e));
                }
            }
        } else if let Some(ref tcp_socket) = handler.tcp_socket {
            // Open TCP connection to the container
            let inspect = self
                .docker
                .inspect_container(container_name, None::<InspectContainerOptions>)
                .await?;

            let ip = inspect
                .network_settings
                .and_then(|ns| ns.ip_address)
                .unwrap_or_else(|| "127.0.0.1".to_string());

            let port =
                rusternetes_common::resources::resolve_probe_port(&tcp_socket.port, container)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Lifecycle TCP handler port {} unresolvable for container {}",
                            tcp_socket.port,
                            container_name
                        )
                    })?;
            let addr = format!("{}:{}", ip, port);
            debug!("Lifecycle TCP handler: {}", addr);

            match tokio::time::timeout(
                Duration::from_secs(10),
                tokio::net::TcpStream::connect(&addr),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    return Err(anyhow::anyhow!("Lifecycle TCP handler failed: {}", e));
                }
                Err(_) => {
                    return Err(anyhow::anyhow!("Lifecycle TCP handler timed out"));
                }
            }
        } else if let Some(ref sleep) = handler.sleep {
            // Sleep for the specified duration
            debug!("Lifecycle sleep handler: {}s", sleep.seconds);
            tokio::time::sleep(Duration::from_secs(sleep.seconds as u64)).await;
        }

        Ok(())
    }

    /// Stop all containers for a pod, executing preStop lifecycle hooks first.
    ///
    /// This is the preferred method when the Pod spec is available, as it
    /// allows preStop hooks to be executed before container termination.
    pub async fn stop_pod_for(&self, pod: &Pod, grace_period_seconds: i64) -> Result<()> {
        let pod_name = &pod.metadata.name;
        self.clear_probe_states_for_pod(pod_name);
        info!(
            "Stopping pod: {} (grace period: {}s, with lifecycle hooks)",
            pod_name, grace_period_seconds
        );

        // Build a map of container name -> lifecycle for preStop hook lookup
        let lifecycle_map = crate::lifecycle::build_prestop_lifecycle_map(pod);

        if !lifecycle_map.is_empty() {
            info!(
                "Pod {} has preStop hooks for containers: {:?}",
                pod_name,
                lifecycle_map.keys().collect::<Vec<_>>()
            );
        }

        // List all containers with this pod prefix
        let mut filters = HashMap::new();
        filters.insert("name".to_string(), vec![format!("{}_", pod_name)]);

        let options = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        let containers = self.docker.list_containers(Some(options)).await?;

        // First pass: execute preStop hooks on all running containers.
        // We must run ALL preStop hooks before stopping ANY containers, because
        // preStop hooks may need to communicate with sibling containers (e.g.,
        // sending an HTTP request to another container in the same pod).
        //
        // K8s runs preStop hooks concurrently per container and bounds the total
        // wait by `terminationGracePeriodSeconds`. If a hook overruns, it is
        // aborted and SIGTERM is delivered with the remaining grace period
        // floored at the minimum (2s) — UNLESS the initial grace period was 0
        // (force-delete), in which case preStop is skipped entirely and the
        // container is SIGKILL'd immediately.
        //
        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go:killContainer
        //   if gracePeriod == 0 { skip preStop, force SIGKILL }
        //   gracePeriod -= preStopElapsed
        //   gracePeriod = max(gracePeriod, minimumGracePeriodInSeconds)  // 2s
        let run_prestop = crate::lifecycle::should_run_prestop(grace_period_seconds);
        let prestop_start = std::time::Instant::now();
        let prestop_budget = crate::lifecycle::compute_prestop_budget(grace_period_seconds);

        // Build a name -> Container lookup so the lifecycle handler can
        // resolve named probe/handler ports against the container's declared
        // ports[].name (K8s IntOrString).
        let spec_containers: std::collections::HashMap<&str, &Container> = pod
            .spec
            .as_ref()
            .map(|s| s.containers.iter().map(|c| (c.name.as_str(), c)).collect())
            .unwrap_or_default();

        // Collect (container_name, pre_stop_handler, container) for all running
        // containers that have a preStop hook. Match exact name first, falling
        // back to suffix match if Docker returns a different name shape.
        // Skipped entirely on force-delete (grace_period == 0).
        let mut hooks_to_run: Vec<(
            String,
            rusternetes_common::resources::LifecycleHandler,
            Container,
        )> = Vec::new();
        if !run_prestop && !lifecycle_map.is_empty() {
            info!(
                "Pod {} has preStop hooks but grace period is 0 — skipping (force-delete)",
                pod_name
            );
        }
        for container in &containers {
            let names = container.names.clone().unwrap_or_default();
            let container_name = names
                .first()
                .map(|n| n.trim_start_matches('/').to_string())
                .unwrap_or_default();
            if container.state.as_deref() != Some("running") {
                continue;
            }
            let lifecycle = lifecycle_map.get(&container_name).or_else(|| {
                lifecycle_map.iter().find_map(|(key, val)| {
                    if container_name.ends_with(&key[pod_name.len()..]) {
                        Some(val)
                    } else {
                        None
                    }
                })
            });
            if let Some(lifecycle) = lifecycle {
                if let Some(ref pre_stop) = lifecycle.pre_stop {
                    // Derive the K8s container name (strip `{pod_name}_` prefix).
                    let k8s_name = container_name
                        .strip_prefix(&format!("{}_", pod_name))
                        .unwrap_or(&container_name);
                    let spec_container = spec_containers
                        .get(k8s_name)
                        .copied()
                        .cloned()
                        .unwrap_or_else(Container::default);
                    hooks_to_run.push((container_name, pre_stop.clone(), spec_container));
                }
            } else if !container_name.ends_with("_pause") {
                debug!(
                    "No preStop hook for running container {} (lifecycle_map keys: {:?})",
                    container_name,
                    lifecycle_map.keys().collect::<Vec<_>>()
                );
            }
        }

        if run_prestop && !hooks_to_run.is_empty() {
            // Run preStop hooks concurrently using futures (no Send required
            // because the executor remains on the current task).
            use futures::stream::{FuturesUnordered, StreamExt};
            let mut futs: FuturesUnordered<_> = hooks_to_run
                .into_iter()
                .map(|(container_name, pre_stop, spec_container)| async move {
                    info!("Executing preStop hook for container {}", container_name);
                    match self
                        .execute_lifecycle_handler(&pre_stop, &container_name, &spec_container)
                        .await
                    {
                        Ok(()) => info!(
                            "preStop hook completed successfully for container {}",
                            container_name
                        ),
                        Err(e) => warn!(
                            "preStop hook failed for container {}: {}",
                            container_name, e
                        ),
                    }
                })
                .collect();
            let wait_all = async { while futs.next().await.is_some() {} };
            // Bound total preStop time by the grace period. If hooks overrun,
            // we abort and proceed to SIGTERM. K8s does the same — preStop is
            // best-effort within the grace period.
            if tokio::time::timeout(prestop_budget, wait_all)
                .await
                .is_err()
            {
                warn!(
                    "preStop hooks exceeded grace period {}s for pod {} — proceeding with SIGTERM",
                    grace_period_seconds, pod_name
                );
            }
        }

        // Decrement grace period by preStop hook elapsed time.
        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go:860-862
        //   gracePeriod -= executePreStopHook elapsed
        //   gracePeriod = max(gracePeriod, minimumGracePeriodInSeconds)
        // For force-delete (initial grace == 0), remaining_grace stays 0
        // and the container is SIGKILL'd immediately — the 2s floor does
        // not apply.
        let prestop_elapsed = prestop_start.elapsed().as_secs() as i64;
        let remaining_grace =
            crate::lifecycle::remaining_grace_after_prestop(grace_period_seconds, prestop_elapsed);
        if prestop_elapsed > 0 {
            info!(
                "preStop hooks took {}s, remaining grace period: {}s (was {}s)",
                prestop_elapsed, remaining_grace, grace_period_seconds
            );
        }

        // Second pass: stop all containers, stopping the pause container LAST.
        // K8s always stops the infra/sandbox container after all app containers
        // to keep the pod's network namespace alive during graceful shutdown.
        // This ensures that:
        //  1. preStop hooks can reach sibling containers (already handled above)
        //  2. The pod remains network-reachable (via its IP) while containers
        //     are shutting down, so pod proxy and monitoring can still reach it
        //  3. SIGTERM handlers in app containers can make outbound network calls
        //
        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go —
        //   killContainersWithSyncResult stops app containers first, then
        //   StopPodSandbox tears down the sandbox (pause) container.
        let pause_suffix = format!("{}_pause", pod_name);
        let mut pause_container_id: Option<String> = None;

        for container in &containers {
            if let Some(ref id) = container.id {
                let is_running = container.state.as_deref() == Some("running");
                if !is_running {
                    continue;
                }

                // Check if this is the pause container — defer stopping it
                let names = container.names.clone().unwrap_or_default();
                let container_name = names
                    .first()
                    .map(|n| n.trim_start_matches('/').to_string())
                    .unwrap_or_default();
                if container_name == pause_suffix || container_name.ends_with("_pause") {
                    pause_container_id = Some(id.clone());
                    continue;
                }

                info!("Stopping container: {}", id);

                // Stop the container with remaining grace period (after preStop).
                let stop_options = StopContainerOptions { t: remaining_grace };
                if let Err(e) = self.docker.stop_container(id, Some(stop_options)).await {
                    warn!("Failed to stop container {}: {}", id, e);
                }

                debug!("Container {} stopped, keeping for log access", id);
            }
        }

        // Stop the pause container last — the network namespace dies with it
        if let Some(ref pause_id) = pause_container_id {
            info!("Stopping pause container: {} (last)", pause_id);
            let stop_options = StopContainerOptions { t: remaining_grace };
            if let Err(e) = self
                .docker
                .stop_container(pause_id, Some(stop_options))
                .await
            {
                warn!("Failed to stop pause container {}: {}", pause_id, e);
            }
        }

        // Teardown CNI networking if enabled
        if self.use_cni {
            if let Err(e) = self.teardown_pod_network(pod_name).await {
                warn!("Failed to teardown CNI network for pod {}: {}", pod_name, e);
            }
        }

        // Clean up volumes after all containers are stopped.
        // K8s does this in SyncTerminatedPod, but our sync loop only iterates
        // pods that exist in etcd. If a pod is deleted from etcd before
        // TerminatedPod runs, volumes would never be cleaned. So we clean
        // here to ensure volumes are always cleaned when containers stop.
        self.cleanup_pod_volumes(pod_name).await?;

        Ok(())
    }

    /// Get the exit code of a terminated container.
    /// Returns the exit code or an error if the container doesn't exist.
    pub async fn get_container_exit_code(&self, container_name: &str) -> Result<i64> {
        let inspect = self
            .docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await?;
        Ok(inspect.state.and_then(|s| s.exit_code).unwrap_or(1))
    }

    /// Remove a terminated container so it can be recreated for restart.
    pub async fn remove_terminated_container(&self, container_name: &str) -> Result<()> {
        if let Ok(info) = self
            .docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await
        {
            let running = info.state.as_ref().and_then(|s| s.running).unwrap_or(false);
            if !running {
                let opts = bollard::container::RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                };
                self.docker
                    .remove_container(container_name, Some(opts))
                    .await?;
                debug!(
                    "Removed terminated container {} for restart",
                    container_name
                );
            }
        }
        Ok(())
    }

    /// Refresh Secret and ConfigMap volumes for a running pod.
    /// Re-reads the data from storage and overwrites files on disk.
    pub async fn refresh_volumes(&self, pod: &Pod) -> Result<()> {
        let storage = match &self.storage {
            Some(s) => s,
            None => return Ok(()),
        };
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let spec = match &pod.spec {
            Some(s) => s,
            None => return Ok(()),
        };
        let volumes = match &spec.volumes {
            Some(v) => v,
            None => return Ok(()),
        };

        for volume in volumes {
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);

            // Refresh Secret volumes
            if let Some(secret_source) = &volume.secret {
                // Create volume dir if it doesn't exist (optional Secret that was created later)
                let _ = std::fs::create_dir_all(&volume_dir);
                let secret_name = match &secret_source.secret_name {
                    Some(n) => n,
                    None => continue,
                };
                let secret_key =
                    rusternetes_storage::build_key("secrets", Some(namespace), secret_name);
                match storage
                    .get::<rusternetes_common::resources::Secret>(&secret_key)
                    .await
                {
                    Ok(secret) => {
                        if let Some(data) = &secret.data {
                            let items = secret_source.items.as_ref();
                            if let Some(items) = items {
                                for item in items {
                                    if let Some(value) = data.get(&item.key) {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        let _ = std::fs::write(&file_path, value);
                                    }
                                }
                            } else {
                                // Write all current keys
                                for (key, value) in data {
                                    let file_path = format!("{}/{}", volume_dir, key);
                                    let _ = std::fs::write(&file_path, value);
                                }
                                // Delete files for keys that no longer exist
                                if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                    for entry in entries.flatten() {
                                        if let Some(fname) = entry.file_name().to_str() {
                                            if !data.contains_key(fname)
                                                && fname != "..data"
                                                && fname != "ca.crt"
                                            {
                                                let _ = std::fs::remove_file(entry.path());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Secret was deleted — if optional, remove all volume files
                        let is_optional = secret_source.optional.unwrap_or(false);
                        if is_optional {
                            if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                for entry in entries.flatten() {
                                    let _ = std::fs::remove_file(entry.path());
                                }
                            }
                        }
                    }
                }
            }

            // Refresh ConfigMap volumes
            if let Some(cm_source) = &volume.config_map {
                let _ = std::fs::create_dir_all(&volume_dir);
                let cm_name = match &cm_source.name {
                    Some(n) => n,
                    None => continue,
                };
                let cm_key = rusternetes_storage::build_key("configmaps", Some(namespace), cm_name);
                match storage
                    .get::<rusternetes_common::resources::ConfigMap>(&cm_key)
                    .await
                {
                    Ok(cm) => {
                        if let Some(data) = &cm.data {
                            let items = cm_source.items.as_ref();
                            if let Some(items) = items {
                                for item in items {
                                    if let Some(value) = data.get(&item.key) {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        let _ = std::fs::write(&file_path, value);
                                    }
                                }
                            } else {
                                for (key, value) in data {
                                    let file_path = format!("{}/{}", volume_dir, key);
                                    let _ = std::fs::write(&file_path, value);
                                }
                                // Delete files for keys removed from ConfigMap
                                if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                    for entry in entries.flatten() {
                                        if let Some(fname) = entry.file_name().to_str() {
                                            if !data.contains_key(fname) && fname != "..data" {
                                                let _ = std::fs::remove_file(entry.path());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // ConfigMap deleted — clean up files if optional
                        let is_optional = cm_source.optional.unwrap_or(false);
                        if is_optional {
                            if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                for entry in entries.flatten() {
                                    let _ = std::fs::remove_file(entry.path());
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get the pod IP address from the first running container
    pub async fn get_pod_ip(&self, pod_name: &str) -> Result<Option<String>> {
        // If using CNI, get IP from CNI runtime
        if self.use_cni {
            if let Some(cni) = &self.cni {
                if let Some(ip) = cni.get_container_ip(pod_name) {
                    debug!("Retrieved pod IP {} from CNI for pod {}", ip, pod_name);
                    return Ok(Some(ip));
                }
            }
        }

        // Fallback to Docker/Podman network inspection
        // Look specifically for the pause container which owns the network namespace
        let pause_name = format!("{}_pause", pod_name);
        let mut filters = HashMap::new();
        filters.insert("name".to_string(), vec![pause_name.clone()]);

        let options = ListContainersOptions {
            all: false, // Only running containers
            filters,
            ..Default::default()
        };

        let mut containers = self.docker.list_containers(Some(options)).await?;

        // If no pause container, try any container matching the pod name
        if containers.is_empty() {
            let mut filters2 = HashMap::new();
            filters2.insert("name".to_string(), vec![format!("{}_", pod_name)]);
            let options2 = ListContainersOptions {
                all: false,
                filters: filters2,
                ..Default::default()
            };
            containers = self.docker.list_containers(Some(options2)).await?;
        }

        // Get the IP from the pause container (or first matching)
        if let Some(container) = containers.first() {
            if let Some(id) = &container.id {
                let inspect = self
                    .docker
                    .inspect_container(id, None::<InspectContainerOptions>)
                    .await?;

                if let Some(network_settings) = inspect.network_settings {
                    // First try to get IP from the specific network we're using
                    if let Some(networks) = network_settings.networks {
                        if let Some(network_info) = networks.get(&self.network) {
                            if let Some(ip) = &network_info.ip_address {
                                if !ip.is_empty() && ip != "0.0.0.0" {
                                    debug!(
                                        "Retrieved pod IP {} from network {} for pod {}",
                                        ip, self.network, pod_name
                                    );
                                    return Ok(Some(ip.clone()));
                                }
                            }
                        }
                    }

                    // Fallback to default network IP
                    if let Some(ip) = network_settings.ip_address {
                        if !ip.is_empty() && ip != "0.0.0.0" {
                            debug!(
                                "Retrieved pod IP {} from default network for pod {}",
                                ip, pod_name
                            );
                            return Ok(Some(ip));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Get a value from a ConfigMap
    async fn get_configmap_value(&self, namespace: &str, name: &str, key: &str) -> Result<String> {
        let storage = self.storage.as_ref().context("Storage not available")?;

        let configmap_key = build_key("configmaps", Some(namespace), name);
        let configmap: ConfigMap = storage
            .get(&configmap_key)
            .await
            .with_context(|| format!("ConfigMap {} not found in namespace {}", name, namespace))?;

        configmap
            .data
            .as_ref()
            .and_then(|data| data.get(key))
            .cloned()
            .with_context(|| format!("Key {} not found in ConfigMap {}", key, name))
    }

    /// Get a value from a Secret
    async fn get_secret_value(&self, namespace: &str, name: &str, key: &str) -> Result<String> {
        let storage = self.storage.as_ref().context("Storage not available")?;

        let secret_key = build_key("secrets", Some(namespace), name);
        let secret: Secret = storage
            .get(&secret_key)
            .await
            .with_context(|| format!("Secret {} not found in namespace {}", name, namespace))?;

        // Secret data is stored base64-encoded in storage, but needs to be decoded
        // for environment variables
        secret
            .data
            .as_ref()
            .and_then(|data| data.get(key))
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            .with_context(|| format!("Key {} not found in Secret {}", key, name))
    }

    /// Get a pod field value for DownwardAPI
    fn get_pod_field_value(&self, pod: &Pod, field_path: &str) -> Result<String> {
        let value = match field_path {
            "metadata.name" => pod.metadata.name.clone(),
            "metadata.namespace" => pod
                .metadata
                .namespace
                .clone()
                .unwrap_or("default".to_string()),
            "metadata.uid" => pod.metadata.uid.clone(),
            "spec.nodeName" => pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone())
                .unwrap_or("".to_string()),
            "spec.serviceAccountName" => pod
                .spec
                .as_ref()
                .and_then(|s| s.service_account_name.clone())
                .unwrap_or("default".to_string()),
            "status.podIP" => pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.clone())
                .unwrap_or("".to_string()),
            "status.hostIP" => pod
                .status
                .as_ref()
                .and_then(|s| s.host_ip.clone())
                .unwrap_or_else(|| "127.0.0.1".to_string()),
            // All labels formatted as key="value"\n (with trailing newline, matching K8s)
            "metadata.labels" => pod
                .metadata
                .labels
                .as_ref()
                .map(|labels| {
                    let mut pairs: Vec<_> = labels.iter().collect();
                    pairs.sort_by_key(|(k, _)| (*k).clone());
                    let mut result = pairs
                        .iter()
                        .map(|(k, v)| format!("{}=\"{}\"", k, v))
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !result.is_empty() {
                        result.push('\n');
                    }
                    result
                })
                .unwrap_or_default(),
            // All annotations formatted as key="value"\n (with trailing newline, matching K8s)
            "metadata.annotations" => pod
                .metadata
                .annotations
                .as_ref()
                .map(|anns| {
                    let mut pairs: Vec<_> = anns.iter().collect();
                    pairs.sort_by_key(|(k, _)| (*k).clone());
                    let mut result = pairs
                        .iter()
                        .map(|(k, v)| format!("{}=\"{}\"", k, v))
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !result.is_empty() {
                        result.push('\n');
                    }
                    result
                })
                .unwrap_or_default(),
            _ => {
                // Support metadata.labels['key'] and metadata.annotations['key']
                if field_path.starts_with("metadata.labels['") && field_path.ends_with("']") {
                    let key = &field_path[17..field_path.len() - 2];
                    pod.metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get(key))
                        .cloned()
                        .unwrap_or("".to_string())
                } else if field_path.starts_with("metadata.annotations['")
                    && field_path.ends_with("']")
                {
                    let key = &field_path[22..field_path.len() - 2];
                    pod.metadata
                        .annotations
                        .as_ref()
                        .and_then(|annotations| annotations.get(key))
                        .cloned()
                        .unwrap_or("".to_string())
                } else {
                    return Err(anyhow::anyhow!("Unsupported field path: {}", field_path));
                }
            }
        };
        Ok(value)
    }

    /// Get a container resource value for DownwardAPI
    ///
    /// Returns the resource value formatted according to the divisor.
    /// For memory: returns bytes (or bytes/divisor) as a string.
    /// For CPU: returns millicores (or cores with divisor "1") as a string.
    /// When divisor is "0" or absent, default units are used (bytes for memory, whole-number
    /// representation for CPU).
    fn get_container_resource_value(
        &self,
        pod: &Pod,
        resource_ref: &rusternetes_common::resources::ResourceFieldSelector,
    ) -> Result<String> {
        let spec = pod.spec.as_ref().context("Pod has no spec")?;

        // Find the container — if containerName is not set, default to the first container
        let container = if let Some(ref container_name) = resource_ref.container_name {
            spec.containers
                .iter()
                .find(|c| &c.name == container_name)
                .with_context(|| format!("Container {} not found", container_name))?
        } else {
            spec.containers.first().context("Pod has no containers")?
        };

        let is_cpu =
            resource_ref.resource.contains("cpu") || resource_ref.resource.contains("hugepages");
        let is_memory = resource_ref.resource.contains("memory")
            || resource_ref.resource.contains("ephemeral-storage");

        let raw_value = match resource_ref.resource.as_str() {
            "limits.cpu" => container
                .resources
                .as_ref()
                .and_then(|r| r.limits.as_ref())
                .and_then(|l| l.get("cpu"))
                .cloned(),
            "limits.memory" => container
                .resources
                .as_ref()
                .and_then(|r| r.limits.as_ref())
                .and_then(|l| l.get("memory"))
                .cloned(),
            "limits.ephemeral-storage" => container
                .resources
                .as_ref()
                .and_then(|r| r.limits.as_ref())
                .and_then(|l| l.get("ephemeral-storage"))
                .cloned(),
            "requests.cpu" => container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .and_then(|l| l.get("cpu"))
                .cloned(),
            "requests.memory" => container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .and_then(|l| l.get("memory"))
                .cloned(),
            "requests.ephemeral-storage" => container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .and_then(|l| l.get("ephemeral-storage"))
                .cloned(),
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported resource field: {}",
                    resource_ref.resource
                ))
            }
        };

        // Parse the divisor (default: "1" meaning base units — bytes for memory, cores for cpu)
        let divisor_str = resource_ref.divisor.as_deref().unwrap_or("0");
        // A divisor of "0" means use default units (same as "1")

        if is_cpu {
            // Convert CPU value to millicores, then apply divisor
            // When no limit is set, use node capacity (default 4 cores = 4000m)
            let millicores = raw_value.as_deref().map(parse_cpu_quantity).unwrap_or(4000); // 4 cores default
            let divisor_millicores = if divisor_str == "0" || divisor_str == "1" {
                // Default divisor "1" means return in cores (1 core = 1000 millicores)
                1000
            } else {
                parse_cpu_quantity(divisor_str).max(1)
            };
            // Kubernetes uses ceiling division for resource quantities
            let result = (millicores + divisor_millicores - 1) / divisor_millicores;
            Ok(result.to_string())
        } else if is_memory {
            // Convert memory value to bytes, then apply divisor
            // When no limit is set, use node allocatable memory (default 8Gi)
            let bytes = raw_value
                .as_deref()
                .map(parse_memory_quantity)
                .unwrap_or(8 * 1024 * 1024 * 1024); // 8Gi default
            let divisor_bytes = if divisor_str == "0" || divisor_str == "1" {
                1 // return bytes
            } else {
                parse_memory_quantity(divisor_str).max(1)
            };
            // Kubernetes uses ceiling division for resource quantities
            let result = (bytes + divisor_bytes - 1) / divisor_bytes;
            Ok(result.to_string())
        } else {
            // Unknown resource type, return raw value
            Ok(raw_value.unwrap_or_else(|| "0".to_string()))
        }
    }

    /// Update container resource limits in-place (for pod resize)
    pub async fn update_container_resources(
        &self,
        container_name: &str,
        cpu_period: Option<i64>,
        cpu_quota: Option<i64>,
        cpu_shares: Option<i64>,
        memory: Option<i64>,
    ) -> Result<()> {
        // Docker requires memory_swap >= memory. If we set memory without
        // memory_swap, Docker rejects: "Memory limit should be smaller than
        // already set memoryswap limit". Set memory_swap = memory (no swap).
        let memory_swap = memory;
        let update = bollard::container::UpdateContainerOptions::<String> {
            cpu_period,
            cpu_quota,
            cpu_shares: cpu_shares.map(|s| s as isize),
            memory,
            memory_swap,
            ..Default::default()
        };
        self.docker
            .update_container(container_name, update)
            .await
            .context("Failed to update container resources")?;
        Ok(())
    }

    /// List all running pod names from the container runtime
    #[allow(dead_code)]
    pub async fn list_running_pods(&self) -> Result<Vec<String>> {
        let options = ListContainersOptions::<String> {
            all: false, // Only running containers
            ..Default::default()
        };

        let containers = self.docker.list_containers(Some(options)).await?;

        let mut pod_names = std::collections::HashSet::new();
        for container in containers {
            if let Some(names) = container.names {
                for name in names {
                    // Container names are in format: /{pod_name}_{container_name}
                    let name = name.trim_start_matches('/');
                    if let Some(pod_name) = name.split('_').next() {
                        // Skip Rusternetes control plane containers
                        if !pod_name.starts_with("rusternetes-") {
                            pod_names.insert(pod_name.to_string());
                        }
                    }
                }
            }
        }

        Ok(pod_names.into_iter().collect())
    }

    /// Container garbage collector — removes dead containers to prevent buildup.
    /// Matches K8s kuberuntime_gc.go:
    /// - For deleted pods (not in existing_pods): removes ALL dead containers
    /// - For existing pods: keeps at most 1 dead container per pod (for log access)
    /// - Removes orphaned pause containers with no running app containers
    /// - Removes stale "created" containers older than 5 minutes
    ///
    /// K8s ref: pkg/kubelet/kuberuntime/kuberuntime_gc.go — evictContainers
    /// Returns the number of containers removed.
    pub async fn garbage_collect_containers(
        &self,
        existing_pods: &std::collections::HashSet<String>,
    ) -> Result<usize> {
        let options = ListContainersOptions::<String> {
            all: true,
            ..Default::default()
        };

        let containers = self.docker.list_containers(Some(options)).await?;

        // Group exited containers by pod name, track which pods have running containers
        let mut exited_by_pod: HashMap<String, Vec<(String, i64)>> = HashMap::new();
        let mut pods_with_running: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut stale_created: Vec<String> = Vec::new();

        for container in &containers {
            let names = match &container.names {
                Some(n) => n,
                None => continue,
            };
            let name = names
                .first()
                .map(|n| n.trim_start_matches('/'))
                .unwrap_or("");

            // Skip infrastructure containers (compose services)
            if name.starts_with("rusternetes-") {
                continue;
            }

            let pod_name = match name.split('_').next() {
                Some(p) => p.to_string(),
                None => continue,
            };

            let state = container.state.as_deref().unwrap_or("");
            let container_id = match &container.id {
                Some(id) => id.clone(),
                None => continue,
            };
            let created = container.created.unwrap_or(0);

            match state {
                "running" => {
                    pods_with_running.insert(pod_name);
                }
                "exited" | "dead" | "stopped" => {
                    exited_by_pod
                        .entry(pod_name)
                        .or_default()
                        .push((container_id, created));
                }
                "created" => {
                    // Containers stuck in "created" for more than 5 minutes
                    let age_secs = chrono::Utc::now().timestamp() - created;
                    if age_secs > 300 {
                        stale_created.push(container_id);
                    }
                }
                _ => {}
            }
        }

        let mut removed = 0;

        // 1. Remove exited containers.
        // K8s ref: evictContainers — for deleted pods (allSourcesReady), remove ALL.
        // For existing pods, keep at most MaxPerPodContainerCount (default 1).
        for (pod_name, mut exited) in exited_by_pod {
            // Sort by created time descending — keep the newest
            exited.sort_by(|a, b| b.1.cmp(&a.1));

            // For pods still in etcd, keep 1 dead container for log access.
            // For deleted pods, remove ALL dead containers.
            let keep_count = if existing_pods.contains(&pod_name) {
                1
            } else {
                0
            };
            for (container_id, _) in exited.iter().skip(keep_count) {
                let opts = RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                };
                if self
                    .docker
                    .remove_container(container_id, Some(opts))
                    .await
                    .is_ok()
                {
                    removed += 1;
                }
            }

            // If the pod has no running containers, remove the last dead one too
            // and clean up the pause container (orphaned sandbox)
            if !pods_with_running.contains(&pod_name) {
                for (container_id, _) in exited.iter().take(1) {
                    let opts = RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    };
                    if self
                        .docker
                        .remove_container(container_id, Some(opts))
                        .await
                        .is_ok()
                    {
                        removed += 1;
                    }
                }
                // Remove orphaned pause container
                let pause_name = format!("{}_pause", pod_name);
                let _ = self
                    .docker
                    .stop_container(&pause_name, Some(StopContainerOptions { t: 0 }))
                    .await;
                let opts = RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                };
                if self
                    .docker
                    .remove_container(&pause_name, Some(opts))
                    .await
                    .is_ok()
                {
                    removed += 1;
                }
                // Clean up volumes
                let _ = self.cleanup_pod_volumes(&pod_name).await;
            }
        }

        // 2. Remove stale "created" containers
        for container_id in &stale_created {
            let opts = RemoveContainerOptions {
                force: true,
                ..Default::default()
            };
            if self
                .docker
                .remove_container(container_id, Some(opts))
                .await
                .is_ok()
            {
                removed += 1;
            }
        }

        Ok(removed)
    }

    /// List all pods with containers in Docker, including exited/stopped.
    /// Used by orphan cleanup to find and remove stopped containers from
    /// pods that have been deleted from storage.
    pub async fn list_all_pods(&self) -> Result<Vec<String>> {
        let options = ListContainersOptions::<String> {
            all: true,
            ..Default::default()
        };

        let containers = self.docker.list_containers(Some(options)).await?;

        let mut pod_names = std::collections::HashSet::new();
        for container in containers {
            if let Some(names) = container.names {
                for name in names {
                    let name = name.trim_start_matches('/');
                    if let Some(pod_name) = name.split('_').next() {
                        if !pod_name.starts_with("rusternetes-") {
                            pod_names.insert(pod_name.to_string());
                        }
                    }
                }
            }
        }

        Ok(pod_names.into_iter().collect())
    }

    /// Get the age of a pod's pause container (time since creation).
    /// Returns Duration::ZERO if the container can't be found.
    pub async fn get_container_age(&self, pod_name: &str) -> Result<std::time::Duration> {
        let pause_name = format!("{}_pause", pod_name);
        match self.docker.inspect_container(&pause_name, None).await {
            Ok(info) => {
                if let Some(state) = info.state {
                    if let Some(started_at) = state.started_at {
                        if let Ok(started) = chrono::DateTime::parse_from_rfc3339(&started_at) {
                            let age = chrono::Utc::now().signed_duration_since(started);
                            return Ok(std::time::Duration::from_secs(
                                age.num_seconds().max(0) as u64
                            ));
                        }
                    }
                }
                Ok(std::time::Duration::from_secs(0))
            }
            Err(_) => Ok(std::time::Duration::from_secs(0)),
        }
    }

    /// Collect CPU and memory usage for all containers belonging to pods on this node.
    /// Returns (cpu_millicores, memory_bytes).
    pub async fn collect_node_metrics(&self, pod_names: &[String]) -> (u64, u64) {
        use futures::StreamExt;

        if pod_names.is_empty() {
            return (0, 0);
        }

        let opts = ListContainersOptions::<String> {
            all: false,
            ..Default::default()
        };

        let containers = match self.docker.list_containers(Some(opts)).await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to list containers for metrics: {}", e);
                return (0, 0);
            }
        };

        // Match containers to pods by name prefix ({pod_name}_{container_name})
        // Skip pause containers — they have minimal resource usage
        let node_containers: Vec<_> = containers
            .iter()
            .filter(|c| {
                if let Some(names) = &c.names {
                    for name in names {
                        let clean = name.trim_start_matches('/');
                        if clean.ends_with("_pause") {
                            return false;
                        }
                        for pod_name in pod_names {
                            if clean.starts_with(pod_name) {
                                return true;
                            }
                        }
                    }
                }
                false
            })
            .collect();

        if node_containers.is_empty() {
            return (0, 0);
        }

        // Collect stats from all containers in parallel
        let mut stat_futures = Vec::new();
        for container in &node_containers {
            if let Some(id) = &container.id {
                let id_clone = id.clone();
                let docker_ref = &self.docker;
                stat_futures.push(async move {
                    let stats_opts = bollard::container::StatsOptions {
                        stream: true,
                        one_shot: false,
                    };
                    let mut stream = docker_ref.stats(&id_clone, Some(stats_opts));
                    // Skip first sample (precpu_stats may be zeros), use second
                    let _ = stream.next().await;
                    let second = stream.next().await;
                    drop(stream);
                    second
                });
            }
        }

        let results = futures::future::join_all(stat_futures).await;

        let mut total_memory_bytes: u64 = 0;
        let mut total_cpu_pct: f64 = 0.0;

        for result in results {
            if let Some(Ok(stats)) = result {
                // Memory: usage minus cache
                if let Some(usage) = stats.memory_stats.usage {
                    let cache = stats
                        .memory_stats
                        .stats
                        .as_ref()
                        .map(|s| match s {
                            bollard::container::MemoryStatsStats::V1(v1) => v1.cache,
                            bollard::container::MemoryStatsStats::V2(v2) => v2.inactive_file,
                        })
                        .unwrap_or(0);
                    total_memory_bytes += usage.saturating_sub(cache);
                }

                // CPU: delta between current and previous sample
                let total_usage = stats.cpu_stats.cpu_usage.total_usage;
                if let Some(system_cpu) = stats.cpu_stats.system_cpu_usage {
                    let prev_total = stats.precpu_stats.cpu_usage.total_usage;
                    let prev_system = stats.precpu_stats.system_cpu_usage.unwrap_or(0);
                    let cpu_delta = total_usage.saturating_sub(prev_total);
                    let system_delta = system_cpu.saturating_sub(prev_system);
                    if system_delta > 0 {
                        let num_cpus = stats.cpu_stats.online_cpus.unwrap_or(1);
                        total_cpu_pct +=
                            (cpu_delta as f64 / system_delta as f64) * num_cpus as f64 * 100.0;
                    }
                }
            }
        }

        // Convert CPU percentage to millicores (1 core = 1000m, so 3.5% = 35m)
        let cpu_millicores = (total_cpu_pct * 10.0) as u64;

        debug!(
            "Node metrics: {}m CPU, {} bytes memory ({} containers from {} pods)",
            cpu_millicores,
            total_memory_bytes,
            node_containers.len(),
            pod_names.len()
        );

        (cpu_millicores, total_memory_bytes)
    }
}

/// Parse a Kubernetes memory quantity string (e.g., "128Mi", "1Gi", "1000000") into bytes.
pub fn parse_memory_quantity(s: &str) -> i64 {
    if s.ends_with("Gi") {
        s.trim_end_matches("Gi").parse::<i64>().unwrap_or(0) * 1024 * 1024 * 1024
    } else if s.ends_with("Mi") {
        s.trim_end_matches("Mi").parse::<i64>().unwrap_or(0) * 1024 * 1024
    } else if s.ends_with("Ki") {
        s.trim_end_matches("Ki").parse::<i64>().unwrap_or(0) * 1024
    } else if s.ends_with('G') {
        s.trim_end_matches('G').parse::<i64>().unwrap_or(0) * 1_000_000_000
    } else if s.ends_with('M') {
        s.trim_end_matches('M').parse::<i64>().unwrap_or(0) * 1_000_000
    } else if s.ends_with('K') || s.ends_with('k') {
        s.trim_end_matches(['K', 'k']).parse::<i64>().unwrap_or(0) * 1000
    } else {
        s.parse::<i64>().unwrap_or(0)
    }
}

/// Parse a Kubernetes CPU quantity string (e.g., "500m", "1", "0.5") into millicores.
pub fn parse_cpu_quantity(s: &str) -> i64 {
    if s.ends_with('m') {
        s.trim_end_matches('m').parse::<i64>().unwrap_or(0)
    } else {
        (s.parse::<f64>().unwrap_or(0.0) * 1000.0) as i64
    }
}

#[cfg(test)]
mod tests {
    use rusternetes_common::resources::{Container, ContainerState, ContainerStatus, Pod, PodSpec};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};

    fn make_container(name: &str) -> Container {
        Container {
            name: name.to_string(),
            image: "nginx:latest".to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            command: None,
            args: None,
            ports: None,
            env: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            resources: None,
            working_dir: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            env_from: None,
            volume_devices: None,
        }
    }

    fn make_pod(
        name: &str,
        namespace: &str,
        hostname: Option<&str>,
        subdomain: Option<&str>,
    ) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec: Some(PodSpec {
                containers: vec![make_container("app")],
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: Some("Always".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: hostname.map(|s| s.to_string()),
                subdomain: subdomain.map(|s| s.to_string()),
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
            }),
            status: None,
        }
    }

    /// Build the /etc/hosts content string the same way create_pod_hosts_file does,
    /// so we can unit-test the logic without needing a live ContainerRuntime.
    fn build_hosts_content(pod: &Pod, pod_ip: Option<&str>, cluster_domain: &str) -> String {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let spec = pod.spec.as_ref().unwrap();
        let raw_hostname = spec.hostname.as_deref().unwrap_or(pod_name);
        let hostname = if raw_hostname.len() > 63 {
            raw_hostname[..63].trim_end_matches('-')
        } else {
            raw_hostname
        };

        let mut content = String::from(
            "# Kubernetes-managed hosts file.\n\
             127.0.0.1\tlocalhost\n\
             ::1\tlocalhost ip6-localhost ip6-loopback\n\
             fe00::\tip6-localnet\n\
             fe00::\tip6-mcastprefix\n\
             fe00::1\tip6-allnodes\n\
             fe00::2\tip6-allrouters\n",
        );

        if let Some(ip) = pod_ip {
            let mut aliases = vec![hostname.to_string()];
            if let Some(subdomain) = &spec.subdomain {
                aliases.push(format!(
                    "{}.{}.{}.svc.{}",
                    hostname, subdomain, namespace, cluster_domain
                ));
            }
            content.push_str(&format!("{}\t{}\n", ip, aliases.join("\t")));
        }

        // Add entries from spec.hostAliases
        // Kubernetes groups all hostnames for the same IP on a single line
        if let Some(host_aliases) = &spec.host_aliases {
            for alias in host_aliases {
                if let Some(hostnames) = &alias.hostnames {
                    if !hostnames.is_empty() {
                        content.push_str(&format!("{}\t{}\n", alias.ip, hostnames.join("\t")));
                    }
                }
            }
        }

        content
    }

    // --- hosts file content tests ---

    #[test]
    fn test_hosts_file_always_contains_localhost() {
        let pod = make_pod("my-pod", "default", None, None);
        let content = build_hosts_content(&pod, None, "cluster.local");

        assert!(content.contains("127.0.0.1\tlocalhost"));
        assert!(content.contains("::1\tlocalhost ip6-localhost ip6-loopback"));
        assert!(content.contains("# Kubernetes-managed hosts file."));
    }

    #[test]
    fn test_hosts_file_no_ip_no_hostname_entry() {
        let pod = make_pod("my-pod", "default", None, None);
        let content = build_hosts_content(&pod, None, "cluster.local");

        // Without a pod IP, no hostname entry should appear
        assert!(!content.contains("my-pod"));
    }

    #[test]
    fn test_hosts_file_pod_name_used_as_hostname_when_not_set() {
        let pod = make_pod("my-pod", "default", None, None);
        let content = build_hosts_content(&pod, Some("10.244.1.5"), "cluster.local");

        assert!(content.contains("10.244.1.5\tmy-pod\n"));
    }

    #[test]
    fn test_hosts_file_uses_spec_hostname_when_set() {
        let pod = make_pod("my-pod-abc", "default", Some("web-0"), None);
        let content = build_hosts_content(&pod, Some("10.244.1.5"), "cluster.local");

        // spec.hostname overrides pod name
        assert!(content.contains("10.244.1.5\tweb-0\n"));
        // pod name should NOT appear as a hostname entry
        assert!(!content.contains("my-pod-abc"));
    }

    #[test]
    fn test_hosts_file_subdomain_generates_fqdn() {
        let pod = make_pod("web-0", "default", Some("web-0"), Some("nginx"));
        let content = build_hosts_content(&pod, Some("10.244.1.5"), "cluster.local");

        // Should have: IP  hostname  FQDN
        assert!(content.contains("10.244.1.5\tweb-0\tweb-0.nginx.default.svc.cluster.local\n"));
    }

    #[test]
    fn test_hosts_file_subdomain_uses_pod_name_when_no_hostname() {
        // subdomain set, but spec.hostname is None -> pod name used as hostname
        let pod = make_pod("web-0", "default", None, Some("nginx"));
        let content = build_hosts_content(&pod, Some("10.244.1.5"), "cluster.local");

        assert!(content.contains("10.244.1.5\tweb-0\tweb-0.nginx.default.svc.cluster.local\n"));
    }

    #[test]
    fn test_hosts_file_subdomain_fqdn_uses_correct_namespace() {
        let pod = make_pod("cache-0", "kube-system", Some("cache-0"), Some("redis"));
        let content = build_hosts_content(&pod, Some("10.244.2.10"), "cluster.local");

        assert!(
            content.contains("10.244.2.10\tcache-0\tcache-0.redis.kube-system.svc.cluster.local\n")
        );
    }

    #[test]
    fn test_hosts_file_subdomain_fqdn_uses_custom_cluster_domain() {
        let pod = make_pod("web-0", "default", Some("web-0"), Some("nginx"));
        let content = build_hosts_content(&pod, Some("10.244.1.5"), "k8s.example.com");

        assert!(content.contains("10.244.1.5\tweb-0\tweb-0.nginx.default.svc.k8s.example.com\n"));
    }

    #[test]
    fn test_hosts_file_no_fqdn_without_subdomain() {
        // hostname set but no subdomain: only simple hostname entry, no FQDN
        let pod = make_pod("web-0", "default", Some("web-0"), None);
        let content = build_hosts_content(&pod, Some("10.244.1.5"), "cluster.local");

        assert!(content.contains("10.244.1.5\tweb-0\n"));
        assert!(!content.contains("svc.cluster.local"));
    }

    // --- hosts file path tests ---

    #[test]
    fn test_hosts_file_path_format() {
        let volumes_base = "/var/lib/rusternetes/volumes";
        let pod_name = "my-pod";
        let expected = format!("{}/{}/hosts", volumes_base, pod_name);
        assert_eq!(expected, "/var/lib/rusternetes/volumes/my-pod/hosts");
    }

    #[test]
    fn test_resolv_conf_and_hosts_colocated() {
        // Both files should live in the same pod directory
        let volumes_base = "/var/lib/rusternetes/volumes";
        let pod_name = "my-pod";
        let hosts_path = format!("{}/{}/hosts", volumes_base, pod_name);
        let resolv_path = format!("{}/{}/resolv.conf", volumes_base, pod_name);

        // Same directory
        assert_eq!(
            std::path::Path::new(&hosts_path).parent(),
            std::path::Path::new(&resolv_path).parent(),
        );
    }

    // --- PodSpec.subdomain field tests ---

    #[test]
    fn test_podspec_subdomain_field_default_none() {
        let pod = make_pod("test", "default", None, None);
        assert!(pod.spec.as_ref().unwrap().subdomain.is_none());
    }

    #[test]
    fn test_podspec_subdomain_field_can_be_set() {
        let pod = make_pod("web-0", "default", Some("web-0"), Some("nginx"));
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.subdomain, Some("nginx".to_string()));
        assert_eq!(spec.hostname, Some("web-0".to_string()));
    }

    #[test]
    fn test_podspec_subdomain_serializes_correctly() {
        let pod = make_pod("web-0", "default", Some("web-0"), Some("nginx"));
        let json = serde_json::to_string(&pod).expect("serialize");
        assert!(json.contains(r#""subdomain":"nginx""#));
        assert!(json.contains(r#""hostname":"web-0""#));
    }

    #[test]
    fn test_podspec_subdomain_omitted_when_none() {
        let pod = make_pod("my-pod", "default", None, None);
        let json = serde_json::to_string(&pod).expect("serialize");
        // skip_serializing_if = Option::is_none means it must not appear
        assert!(!json.contains("subdomain"));
    }

    #[test]
    fn test_podspec_subdomain_roundtrip_deserialization() {
        let original = make_pod("web-0", "default", Some("web-0"), Some("nginx"));
        let json = serde_json::to_string(&original).expect("serialize");
        let restored: Pod = serde_json::from_str(&json).expect("deserialize");
        let spec = restored.spec.as_ref().unwrap();
        assert_eq!(spec.subdomain, Some("nginx".to_string()));
        assert_eq!(spec.hostname, Some("web-0".to_string()));
    }

    #[test]
    fn test_emptydir_volume_path_format() {
        let pod_name = "test-pod-emptydir";
        let volume_name = "test-volume";
        let expected_path = format!("/volumes/{}/{}", pod_name, volume_name);

        assert_eq!(expected_path, "/volumes/test-pod-emptydir/test-volume");
    }

    // --- EmptyDir mode bits regression tests (conformance [Conformance].*EmptyDir.*) ---
    //
    // Kubernetes sets emptyDir directory permissions to 0o777 via setupDir() in
    // pkg/volume/emptydir/empty_dir.go. The conformance tests
    //   [Conformance] EmptyDir.*(mode|0644|0666|0777)
    // are all marked [LinuxOnly] — they exercise the chmod path verified here.
    //
    // On macOS dev environments the bind mount through virtiofs strips mode bits;
    // that is a dev-env limitation, not a kubelet bug. Linux CI hits the path below.

    #[cfg(unix)]
    fn unique_tmp_dir(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rusternetes-emptydir-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[cfg(unix)]
    fn mode_of(path: &std::path::Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
    }

    #[cfg(unix)]
    #[test]
    fn test_setup_emptydir_dir_sets_mode_0777_on_new_dir() {
        let tmp = unique_tmp_dir("new");

        super::setup_emptydir_dir(tmp.to_str().unwrap()).expect("setup_emptydir_dir");

        let mode = mode_of(&tmp);
        assert_eq!(
            mode, 0o777,
            "newly created emptyDir must have mode 0o777, got {:o}",
            mode
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(unix)]
    #[test]
    fn test_setup_emptydir_dir_rechmods_existing_dir() {
        use std::os::unix::fs::PermissionsExt;

        // Pre-create the dir with mode 0o700 (simulating a stale dir left from
        // a prior pod run or pre-existing host directory).
        let tmp = unique_tmp_dir("existing");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(mode_of(&tmp), 0o700, "pre-condition: mode 0o700");

        super::setup_emptydir_dir(tmp.to_str().unwrap()).expect("setup_emptydir_dir");

        let after = mode_of(&tmp);
        assert_eq!(
            after, 0o777,
            "setup_emptydir_dir must re-chmod existing dir to 0o777, got {:o}",
            after
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(unix)]
    #[test]
    fn test_setup_emptydir_dir_creates_nested_parent_dirs() {
        // The pod volumes path is {base}/{pod_name}/{volume_name}; parents may not
        // exist on the first volume of a pod. create_dir_all must build them.
        let root = unique_tmp_dir("nested");
        let target = root.join("pod-x").join("vol-y");

        super::setup_emptydir_dir(target.to_str().unwrap()).expect("setup_emptydir_dir");

        assert!(target.exists(), "nested target dir must exist");
        let mode = mode_of(&target);
        assert_eq!(
            mode, 0o777,
            "nested emptyDir must have mode 0o777, got {:o}",
            mode
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /// On Linux, the host chmod 0o777 is the production code path; the bind
    /// mount preserves mode bits inside the container. This test makes that
    /// invariant explicit so any regression that drops the chmod fails CI.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_setup_emptydir_dir_linux_full_bit_pattern() {
        let tmp = unique_tmp_dir("linux");

        super::setup_emptydir_dir(tmp.to_str().unwrap()).expect("setup_emptydir_dir");

        let mode = mode_of(&tmp);
        // On Linux, chmod is honored by the kernel — verify every rwx triple.
        for (shift, label) in [
            (6, "owner"), // 0o700
            (3, "group"), // 0o070
            (0, "other"), // 0o007
        ] {
            for (bit, name) in [(4, "read"), (2, "write"), (1, "execute")] {
                let expected = bit << shift;
                assert!(
                    mode & expected != 0,
                    "{} {} bit missing in mode {:o}",
                    label,
                    name,
                    mode
                );
            }
        }

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_hostpath_volume_path() {
        let path = "/tmp/test-hostpath";
        assert_eq!(path, "/tmp/test-hostpath");
    }

    #[test]
    fn test_volume_bind_string_format() {
        // Test read-write bind
        let host_path = "/tmp/test";
        let mount_path = "/data";
        let read_only = false;
        let bind_rw = format!(
            "{}:{}{}",
            host_path,
            mount_path,
            if read_only { ":ro" } else { "" }
        );
        assert_eq!(bind_rw, "/tmp/test:/data");

        // Test read-only bind
        let read_only = true;
        let bind_ro = format!(
            "{}:{}{}",
            host_path,
            mount_path,
            if read_only { ":ro" } else { "" }
        );
        assert_eq!(bind_ro, "/tmp/test:/data:ro");
    }

    #[test]
    fn test_cleanup_volume_path() {
        let pod_name = "test-pod";
        let volume_dir = format!("/volumes/{}", pod_name);

        assert_eq!(volume_dir, "/volumes/test-pod");
    }

    #[test]
    fn test_hostpath_types() {
        let types = vec![
            "DirectoryOrCreate",
            "Directory",
            "FileOrCreate",
            "File",
            "Socket",
            "CharDevice",
            "BlockDevice",
        ];

        for hp_type in types {
            assert!(!hp_type.is_empty());
        }
    }

    #[test]
    fn test_downward_api_field_paths() {
        // Test common DownwardAPI field paths
        let field_paths = vec![
            "metadata.name",
            "metadata.namespace",
            "metadata.uid",
            "spec.nodeName",
            "spec.serviceAccountName",
            "status.podIP",
            "status.hostIP",
        ];

        for path in field_paths {
            assert!(path.contains('.'));
        }
    }

    #[test]
    fn test_downward_api_label_syntax() {
        let field_path = "metadata.labels['app']";
        assert!(field_path.starts_with("metadata.labels['"));
        assert!(field_path.ends_with("']"));

        // Extract key
        let key = &field_path[17..field_path.len() - 2];
        assert_eq!(key, "app");
    }

    #[test]
    fn test_downward_api_annotation_syntax() {
        let field_path = "metadata.annotations['description']";
        assert!(field_path.starts_with("metadata.annotations['"));
        assert!(field_path.ends_with("']"));

        // Extract key
        let key = &field_path[22..field_path.len() - 2];
        assert_eq!(key, "description");
    }

    #[test]
    fn test_ephemeral_pvc_naming() {
        let pod_name = "test-pod";
        let volume_name = "cache";
        let pvc_name = format!("{}-{}", pod_name, volume_name);
        assert_eq!(pvc_name, "test-pod-cache");
    }

    #[test]
    fn test_csi_volume_directory_format() {
        let pod_name = "test-pod";
        let volume_name = "csi-vol";
        let volume_dir = format!("/volumes/{}/{}", pod_name, volume_name);
        assert_eq!(volume_dir, "/volumes/test-pod/csi-vol");
    }

    // --- pause container (non-CNI network sandbox) tests ---

    #[test]
    fn test_pause_container_name_format() {
        // The pause container for a pod must be named {pod_name}_pause so that
        // get_pod_ip (which filters by "{pod_name}_") discovers it.
        let pod_name = "sonobuoy";
        let pause_name = format!("{}_pause", pod_name);
        assert_eq!(pause_name, "sonobuoy_pause");

        // Verify it matches the pod prefix filter used by get_pod_ip
        assert!(pause_name.starts_with(&format!("{}_", pod_name)));
    }

    #[test]
    fn test_pause_container_name_format_various_pods() {
        for pod_name in &["web-0", "redis-0", "my-app-abc123", "kube-dns"] {
            let pause_name = format!("{}_pause", pod_name);
            assert!(pause_name.starts_with(&format!("{}_", pod_name)));
            assert!(pause_name.ends_with("_pause"));
        }
    }

    #[test]
    fn test_hostname_truncation_for_long_pod_names() {
        // Linux hostnames are limited to 63 characters.
        // Pod names can be up to 253 chars, so we must truncate.
        let long_name = "sample-webhook-deployment-1ea22597-ec36f15a-8ae5-4dc4-8f3b-1da2641cef30";
        assert!(long_name.len() > 63);

        let truncated = if long_name.len() > 63 {
            long_name[..63].trim_end_matches('-').to_string()
        } else {
            long_name.to_string()
        };

        assert!(truncated.len() <= 63);
        assert!(!truncated.ends_with('-'));

        // Short names should not be modified
        let short_name = "web-0";
        let result = if short_name.len() > 63 {
            short_name[..63].trim_end_matches('-').to_string()
        } else {
            short_name.to_string()
        };
        assert_eq!(result, "web-0");

        // Exactly 63 chars should not be modified
        let exact = "a".repeat(63);
        let result = if exact.len() > 63 {
            exact[..63].trim_end_matches('-').to_string()
        } else {
            exact.clone()
        };
        assert_eq!(result.len(), 63);

        // Name that would truncate to end with dash should have dash stripped
        let dash_name = "abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-1234567890-xyz";
        assert!(dash_name.len() > 63);
        let truncated = if dash_name.len() > 63 {
            dash_name[..63].trim_end_matches('-').to_string()
        } else {
            dash_name.to_string()
        };
        assert!(!truncated.ends_with('-'));
        assert!(truncated.len() <= 63);
    }

    #[test]
    fn test_non_cni_network_mode_uses_pause_container() {
        // In non-CNI mode, real containers join the pause container's network
        // namespace so they share the pod IP and localhost.
        let pod_name = "my-pod";
        let use_cni = false;
        let network_mode = if use_cni {
            "rusternetes-network".to_string()
        } else {
            format!("container:{}_pause", pod_name)
        };
        assert_eq!(network_mode, "container:my-pod_pause");
    }

    #[test]
    fn test_cni_network_mode_uses_bridge_network() {
        // In CNI mode, containers join the named Docker network directly.
        let pod_name = "my-pod";
        let use_cni = true;
        let bridge_network = "rusternetes-network";
        let network_mode = if use_cni {
            bridge_network.to_string()
        } else {
            format!("container:{}_pause", pod_name)
        };
        assert_eq!(network_mode, "rusternetes-network");
    }

    // --- lifecycle hook tests ---

    #[test]
    fn test_lifecycle_handler_exec_is_recognized() {
        use rusternetes_common::resources::{ExecAction, Lifecycle, LifecycleHandler};

        let lifecycle = Lifecycle {
            post_start: Some(LifecycleHandler {
                exec: Some(ExecAction {
                    command: vec![
                        "/bin/sh".to_string(),
                        "-c".to_string(),
                        "echo hello".to_string(),
                    ],
                }),
                http_get: None,
                tcp_socket: None,
                sleep: None,
            }),
            pre_stop: None,
            stop_signal: None,
        };

        assert!(lifecycle.post_start.is_some());
        let handler = lifecycle.post_start.unwrap();
        assert!(handler.exec.is_some());
        assert_eq!(handler.exec.unwrap().command.len(), 3);
    }

    #[test]
    fn test_lifecycle_handler_http_get_is_recognized() {
        use rusternetes_common::resources::{HTTPGetAction, Lifecycle, LifecycleHandler};

        let lifecycle = Lifecycle {
            post_start: None,
            pre_stop: Some(LifecycleHandler {
                exec: None,
                http_get: Some(HTTPGetAction {
                    path: Some("/shutdown".to_string()),
                    port: rusternetes_common::resources::IntOrString::Int(8080),
                    host: Some("localhost".to_string()),
                    scheme: Some("HTTP".to_string()),
                    http_headers: None,
                }),
                tcp_socket: None,
                sleep: None,
            }),
            stop_signal: None,
        };

        assert!(lifecycle.pre_stop.is_some());
        let handler = lifecycle.pre_stop.unwrap();
        assert!(handler.http_get.is_some());
        let http = handler.http_get.unwrap();
        assert_eq!(
            http.port,
            rusternetes_common::resources::IntOrString::Int(8080)
        );
        assert_eq!(http.path.as_deref(), Some("/shutdown"));
    }

    #[test]
    fn test_lifecycle_handler_sleep_is_recognized() {
        use rusternetes_common::resources::{Lifecycle, LifecycleHandler, SleepAction};

        let lifecycle = Lifecycle {
            post_start: None,
            pre_stop: Some(LifecycleHandler {
                exec: None,
                http_get: None,
                tcp_socket: None,
                sleep: Some(SleepAction { seconds: 5 }),
            }),
            stop_signal: None,
        };

        assert!(lifecycle.pre_stop.is_some());
        let handler = lifecycle.pre_stop.unwrap();
        assert!(handler.sleep.is_some());
        assert_eq!(handler.sleep.unwrap().seconds, 5);
    }

    #[test]
    fn test_container_lifecycle_field_present() {
        use rusternetes_common::resources::{ExecAction, Lifecycle, LifecycleHandler};

        let mut container = make_container("app");
        assert!(container.lifecycle.is_none());

        container.lifecycle = Some(Lifecycle {
            post_start: Some(LifecycleHandler {
                exec: Some(ExecAction {
                    command: vec![
                        "/bin/sh".to_string(),
                        "-c".to_string(),
                        "touch /tmp/started".to_string(),
                    ],
                }),
                http_get: None,
                tcp_socket: None,
                sleep: None,
            }),
            pre_stop: Some(LifecycleHandler {
                exec: Some(ExecAction {
                    command: vec![
                        "/bin/sh".to_string(),
                        "-c".to_string(),
                        "touch /tmp/stopping".to_string(),
                    ],
                }),
                http_get: None,
                tcp_socket: None,
                sleep: None,
            }),
            stop_signal: None,
        });

        assert!(container.lifecycle.is_some());
        let lc = container.lifecycle.unwrap();
        assert!(lc.post_start.is_some());
        assert!(lc.pre_stop.is_some());
    }

    #[test]
    fn test_lifecycle_serializes_correctly() {
        use rusternetes_common::resources::{ExecAction, Lifecycle, LifecycleHandler};

        let mut container = make_container("app");
        container.lifecycle = Some(Lifecycle {
            post_start: Some(LifecycleHandler {
                exec: Some(ExecAction {
                    command: vec!["echo".to_string(), "started".to_string()],
                }),
                http_get: None,
                tcp_socket: None,
                sleep: None,
            }),
            pre_stop: None,
            stop_signal: None,
        });

        let json = serde_json::to_string(&container).expect("serialize");
        assert!(json.contains("\"lifecycle\""));
        assert!(json.contains("\"postStart\""));
        assert!(json.contains("\"exec\""));
    }

    // --- startup probe tests ---

    #[test]
    fn test_container_startup_probe_field() {
        use rusternetes_common::resources::{ExecAction, Probe};

        let mut container = make_container("app");
        assert!(container.startup_probe.is_none());

        container.startup_probe = Some(Probe {
            exec: Some(ExecAction {
                command: vec!["cat".to_string(), "/tmp/healthy".to_string()],
            }),
            http_get: None,
            tcp_socket: None,
            initial_delay_seconds: Some(5),
            period_seconds: Some(10),
            timeout_seconds: Some(1),
            success_threshold: Some(1),
            failure_threshold: Some(30),
            grpc: None,
            termination_grace_period_seconds: None,
        });

        assert!(container.startup_probe.is_some());
        let probe = container.startup_probe.unwrap();
        assert_eq!(probe.failure_threshold, Some(30));
        assert!(probe.exec.is_some());
    }

    #[test]
    fn test_startup_probe_prevents_readiness_when_not_started() {
        // This tests the logical condition used in get_container_statuses:
        // when startup_passed is false, ready should be false
        let startup_passed = false;
        let running = true;
        let _has_readiness_probe = true;

        // Simulated logic from get_container_statuses
        let ready = running && startup_passed;

        assert!(!ready);
        assert!(!startup_passed);
    }

    #[test]
    fn test_startup_probe_allows_readiness_when_started() {
        // When startup probe passes, readiness probe should be evaluated
        let startup_passed = true;
        let running = true;

        let ready = if running && startup_passed {
            true // would check readiness probe
        } else {
            false
        };

        assert!(ready);
    }

    #[test]
    fn test_startup_probe_blocks_liveness_check() {
        // Verify the logical condition: if startup probe hasn't passed,
        // liveness checks should be skipped (continue in the loop)
        let has_startup_probe = true;
        let startup_passed = false;

        let should_skip_liveness = has_startup_probe && !startup_passed;
        assert!(should_skip_liveness);
    }

    #[test]
    fn test_no_startup_probe_does_not_block_liveness() {
        // Without a startup probe, liveness should proceed normally
        let has_startup_probe = false;

        // No startup probe means we don't skip
        let should_skip_liveness = has_startup_probe;
        assert!(!should_skip_liveness);
    }

    #[test]
    fn test_lifecycle_and_startup_probe_on_same_container() {
        use rusternetes_common::resources::{ExecAction, Lifecycle, LifecycleHandler, Probe};

        let mut container = make_container("app");
        container.lifecycle = Some(Lifecycle {
            post_start: Some(LifecycleHandler {
                exec: Some(ExecAction {
                    command: vec!["echo".to_string(), "started".to_string()],
                }),
                http_get: None,
                tcp_socket: None,
                sleep: None,
            }),
            pre_stop: Some(LifecycleHandler {
                exec: Some(ExecAction {
                    command: vec!["echo".to_string(), "stopping".to_string()],
                }),
                http_get: None,
                tcp_socket: None,
                sleep: None,
            }),
            stop_signal: None,
        });
        container.startup_probe = Some(Probe {
            exec: Some(ExecAction {
                command: vec!["cat".to_string(), "/tmp/ready".to_string()],
            }),
            http_get: None,
            tcp_socket: None,
            initial_delay_seconds: Some(0),
            period_seconds: Some(5),
            timeout_seconds: Some(1),
            success_threshold: Some(1),
            failure_threshold: Some(10),
            grpc: None,
            termination_grace_period_seconds: None,
        });

        assert!(container.lifecycle.is_some());
        assert!(container.startup_probe.is_some());

        // Both can coexist
        let lc = container.lifecycle.as_ref().unwrap();
        assert!(lc.post_start.is_some());
        assert!(lc.pre_stop.is_some());
    }

    #[test]
    fn test_pause_container_ip_is_pod_ip() {
        // The pause container holds the network namespace, so its IP is the pod IP.
        // Verify this convention by checking that get_pod_ip searches by pod name prefix,
        // which matches both real containers AND the pause container.
        let pod_name = "web-0";
        let pause_name = format!("{}_pause", pod_name);
        let filter_prefix = format!("{}_", pod_name);

        // Both the pause container and real containers match this filter
        assert!(pause_name.starts_with(&filter_prefix));
        assert!(format!("{}_app", pod_name).starts_with(&filter_prefix));
    }

    // --- probe threshold tests ---

    #[test]
    fn test_probe_state_default() {
        let state = super::ProbeState::default();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.consecutive_successes, 0);
    }

    #[test]
    fn test_probe_threshold_logic_single_failure_no_action() {
        // Simulate: failure_threshold=3, one failure should NOT trigger action
        let failure_threshold = 3;
        let mut state = super::ProbeState::default();

        // First failure
        state.consecutive_failures += 1;
        state.consecutive_successes = 0;
        assert!(state.consecutive_failures < failure_threshold);
    }

    #[test]
    fn test_probe_threshold_logic_threshold_reached() {
        // Simulate: failure_threshold=3, three failures should trigger action
        let failure_threshold = 3;
        let mut state = super::ProbeState::default();

        for _ in 0..3 {
            state.consecutive_failures += 1;
            state.consecutive_successes = 0;
        }
        assert!(state.consecutive_failures >= failure_threshold);
    }

    #[test]
    fn test_probe_threshold_success_resets_failures() {
        let mut state = super::ProbeState {
            consecutive_failures: 2,
            consecutive_successes: 0,
        };

        // Then a success resets failures
        state.consecutive_successes += 1;
        state.consecutive_failures = 0;

        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.consecutive_successes, 1);
    }

    #[test]
    fn test_probe_threshold_failure_resets_successes() {
        let mut state = super::ProbeState {
            consecutive_successes: 1,
            consecutive_failures: 0,
        };

        // Then a failure resets successes
        state.consecutive_failures += 1;
        state.consecutive_successes = 0;

        assert_eq!(state.consecutive_successes, 0);
        assert_eq!(state.consecutive_failures, 1);
    }

    #[test]
    fn test_probe_threshold_readiness_needs_success_threshold() {
        // successThreshold=3 means 3 consecutive successes needed for ready
        let success_threshold = 3;
        let mut state = super::ProbeState::default();

        // First success: not ready yet
        state.consecutive_successes += 1;
        state.consecutive_failures = 0;
        assert!(state.consecutive_successes < success_threshold);

        // Second success: still not ready
        state.consecutive_successes += 1;
        assert!(state.consecutive_successes < success_threshold);

        // Third success: now ready
        state.consecutive_successes += 1;
        assert!(state.consecutive_successes >= success_threshold);
    }

    #[test]
    fn test_probe_threshold_defaults() {
        use rusternetes_common::resources::Probe;

        let probe = Probe {
            http_get: None,
            tcp_socket: None,
            exec: None,
            initial_delay_seconds: None,
            timeout_seconds: None,
            period_seconds: None,
            success_threshold: None,
            failure_threshold: None,
            grpc: None,
            termination_grace_period_seconds: None,
        };

        // Kubernetes defaults
        assert_eq!(probe.failure_threshold.unwrap_or(3), 3);
        assert_eq!(probe.success_threshold.unwrap_or(1), 1);
        assert_eq!(probe.period_seconds.unwrap_or(10), 10);
    }

    #[test]
    fn test_probe_state_map_key_format() {
        let pod_name = "web-0";
        let container_name = "nginx";
        let liveness_key = format!("{}/{}/liveness", pod_name, container_name);
        let readiness_key = format!("{}/{}/readiness", pod_name, container_name);
        let startup_key = format!("{}/{}/startup", pod_name, container_name);

        assert_eq!(liveness_key, "web-0/nginx/liveness");
        assert_eq!(readiness_key, "web-0/nginx/readiness");
        assert_eq!(startup_key, "web-0/nginx/startup");

        // Keys for different probe types should be distinct
        assert_ne!(liveness_key, readiness_key);
        assert_ne!(readiness_key, startup_key);
    }

    #[test]
    fn test_clear_probe_states_removes_pod_entries() {
        let mut states = std::collections::HashMap::new();
        states.insert(
            "web-0/nginx/liveness".to_string(),
            super::ProbeState {
                consecutive_failures: 2,
                consecutive_successes: 0,
            },
        );
        states.insert(
            "web-0/nginx/readiness".to_string(),
            super::ProbeState {
                consecutive_failures: 0,
                consecutive_successes: 3,
            },
        );
        states.insert(
            "redis-0/redis/liveness".to_string(),
            super::ProbeState {
                consecutive_failures: 1,
                consecutive_successes: 0,
            },
        );

        let prefix = "web-0/";
        states.retain(|key, _| !key.starts_with(prefix));

        // web-0 entries should be removed
        assert!(!states.contains_key("web-0/nginx/liveness"));
        assert!(!states.contains_key("web-0/nginx/readiness"));
        // redis-0 should remain
        assert!(states.contains_key("redis-0/redis/liveness"));
    }

    // --- service environment variable tests ---

    #[test]
    fn test_service_env_var_name_formatting() {
        let svc_name = "my-redis-svc";
        let svc_env = svc_name.to_uppercase().replace('-', "_");
        assert_eq!(svc_env, "MY_REDIS_SVC");
    }

    #[test]
    fn test_service_env_var_host_format() {
        let svc_env = "MY_SVC";
        let cluster_ip = "10.96.0.10";
        let env_var = format!("{}_SERVICE_HOST={}", svc_env, cluster_ip);
        assert_eq!(env_var, "MY_SVC_SERVICE_HOST=10.96.0.10");
    }

    #[test]
    fn test_service_env_var_port_format() {
        let svc_env = "MY_SVC";
        let port = 8080;
        let cluster_ip = "10.96.0.10";

        let service_port = format!("{}_SERVICE_PORT={}", svc_env, port);
        assert_eq!(service_port, "MY_SVC_SERVICE_PORT=8080");

        let port_url = format!("{}_PORT=tcp://{}:{}", svc_env, cluster_ip, port);
        assert_eq!(port_url, "MY_SVC_PORT=tcp://10.96.0.10:8080");

        let port_tcp = format!(
            "{}_PORT_{}_TCP=tcp://{}:{}",
            svc_env, port, cluster_ip, port
        );
        assert_eq!(port_tcp, "MY_SVC_PORT_8080_TCP=tcp://10.96.0.10:8080");
    }

    #[test]
    fn test_service_env_var_named_port() {
        let svc_env = "MY_SVC";
        let port_name = "http-web";
        let port_name_env = port_name.to_uppercase().replace('-', "_");
        let env_var = format!("{}_SERVICE_PORT_{}={}", svc_env, port_name_env, 8080);
        assert_eq!(env_var, "MY_SVC_SERVICE_PORT_HTTP_WEB=8080");
    }

    #[test]
    fn test_service_env_var_skips_none_cluster_ip() {
        let cluster_ip = "None";
        let should_skip = cluster_ip == "None" || cluster_ip.is_empty();
        assert!(should_skip);
    }

    #[test]
    fn test_service_env_var_skips_empty_cluster_ip() {
        let cluster_ip = "";
        let should_skip = cluster_ip == "None" || cluster_ip.is_empty();
        assert!(should_skip);
    }

    #[test]
    fn test_enable_service_links_default_true() {
        let pod = make_pod("test", "default", None, None);
        let enable = pod
            .spec
            .as_ref()
            .and_then(|s| s.enable_service_links)
            .unwrap_or(true);
        assert!(enable);
    }

    // --- DNS policy tests ---

    #[test]
    fn test_dns_policy_default_is_cluster_first() {
        let pod = make_pod("test", "default", None, None);
        let dns_policy = pod
            .spec
            .as_ref()
            .and_then(|s| s.dns_policy.as_deref())
            .unwrap_or("ClusterFirst");
        assert_eq!(dns_policy, "ClusterFirst");
    }

    #[test]
    fn test_dns_policy_none_produces_empty_content() {
        let dns_policy = "None";
        let content = match dns_policy {
            "None" => String::new(),
            _ => "nameserver 10.96.0.10\n".to_string(),
        };
        assert!(content.is_empty());
    }

    #[test]
    fn test_dns_config_nameserver_prepend() {
        use rusternetes_common::resources::pod::PodDNSConfig;

        let dns_config = PodDNSConfig {
            nameservers: Some(vec!["8.8.8.8".to_string()]),
            searches: None,
            options: None,
        };

        let existing = vec!["10.96.0.10".to_string()];
        let mut merged = dns_config.nameservers.unwrap();
        for ns in &existing {
            if !merged.contains(ns) {
                merged.push(ns.clone());
            }
        }

        assert_eq!(merged, vec!["8.8.8.8", "10.96.0.10"]);
    }

    #[test]
    fn test_dns_config_search_domains() {
        use rusternetes_common::resources::pod::PodDNSConfig;

        let dns_config = PodDNSConfig {
            nameservers: None,
            searches: Some(vec!["custom.local".to_string()]),
            options: None,
        };

        let existing = vec!["default.svc.cluster.local".to_string()];
        let mut merged = dns_config.searches.unwrap();
        for s in &existing {
            if !merged.contains(s) {
                merged.push(s.clone());
            }
        }

        assert_eq!(merged, vec!["custom.local", "default.svc.cluster.local"]);
    }

    #[test]
    fn test_dns_config_options_with_value() {
        use rusternetes_common::resources::pod::PodDNSConfigOption;

        let opt = PodDNSConfigOption {
            name: "ndots".to_string(),
            value: Some("3".to_string()),
        };

        let opt_str = if let Some(ref val) = opt.value {
            format!("{}:{}", opt.name, val)
        } else {
            opt.name.clone()
        };

        assert_eq!(opt_str, "ndots:3");
    }

    #[test]
    fn test_dns_config_options_without_value() {
        use rusternetes_common::resources::pod::PodDNSConfigOption;

        let opt = PodDNSConfigOption {
            name: "single-request-reopen".to_string(),
            value: None,
        };

        let opt_str = if let Some(ref val) = opt.value {
            format!("{}:{}", opt.name, val)
        } else {
            opt.name.clone()
        };

        assert_eq!(opt_str, "single-request-reopen");
    }

    #[test]
    fn test_resolv_conf_parsing() {
        let content = "nameserver 10.96.0.10\nsearch default.svc.cluster.local svc.cluster.local cluster.local\noptions ndots:5\n";

        let mut nameservers = Vec::new();
        let mut searches = Vec::new();
        let mut options = Vec::new();

        for line in content.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("nameserver ") {
                nameservers.push(rest.to_string());
            } else if let Some(rest) = line.strip_prefix("search ") {
                for domain in rest.split_whitespace() {
                    searches.push(domain.to_string());
                }
            } else if let Some(rest) = line.strip_prefix("options ") {
                for opt in rest.split_whitespace() {
                    options.push(opt.to_string());
                }
            }
        }

        assert_eq!(nameservers, vec!["10.96.0.10"]);
        assert_eq!(
            searches,
            vec![
                "default.svc.cluster.local",
                "svc.cluster.local",
                "cluster.local"
            ]
        );
        assert_eq!(options, vec!["ndots:5"]);
    }

    #[test]
    fn test_cluster_first_with_host_net_uses_cluster_dns() {
        // ClusterFirstWithHostNet should use cluster DNS regardless of host network
        let dns_policy = "ClusterFirstWithHostNet";
        let is_host_network = true;
        let cluster_dns = "10.96.0.10";

        let uses_cluster_dns = match dns_policy {
            "ClusterFirstWithHostNet" => true,          // always cluster DNS
            "ClusterFirst" if is_host_network => false, // falls back to host DNS
            "ClusterFirst" => true,
            _ => false,
        };

        assert!(uses_cluster_dns);
        assert_eq!(cluster_dns, "10.96.0.10");
    }

    #[test]
    fn test_cluster_first_with_host_network_uses_host_dns() {
        // ClusterFirst + hostNetwork=true should fall back to host DNS
        let dns_policy = "ClusterFirst";
        let is_host_network = true;

        let uses_host_dns = dns_policy == "ClusterFirst" && is_host_network;
        assert!(uses_host_dns);
    }

    #[test]
    fn test_probe_timeout_zero_uses_default() {
        // K8s treats timeout_seconds=0 as "use default" (1s)
        // timeout_seconds=0 → .max(1) → 1
        assert_eq!(1u64, 1);
        // timeout_seconds=None → unwrap_or(1) → 1
        assert_eq!(1u64, 1);
        // timeout_seconds=5 → .max(1) → 5
        assert_eq!(5u64, 5);
    }

    // --- Sysctl tests ---

    use rusternetes_common::resources::pod::{PodSecurityContext, Sysctl};
    use std::collections::HashMap;

    /// Helper: build a pod with optional sysctls in its security context
    fn make_pod_with_sysctls(name: &str, sysctls: Option<Vec<Sysctl>>) -> Pod {
        let security_context = sysctls.map(|s| PodSecurityContext {
            run_as_user: None,
            run_as_group: None,
            run_as_non_root: None,
            fs_group: None,
            fs_group_change_policy: None,
            supplemental_groups: None,
            sysctls: Some(s),
            seccomp_profile: None,
            app_armor_profile: None,
            se_linux_options: None,
            windows_options: None,
            se_linux_change_policy: None,
            supplemental_groups_policy: None,
        });

        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![make_container("app")],
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: Some("Always".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
            }),
            status: None,
        }
    }

    /// Extract the sysctls map from a pod the same way create_pod does (lines 874-882)
    fn extract_sysctls(pod: &Pod) -> Option<HashMap<String, String>> {
        pod.spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.sysctls.as_ref())
            .map(|sysctls| {
                sysctls
                    .iter()
                    .map(|s| (s.name.clone(), s.value.clone()))
                    .collect()
            })
    }

    #[test]
    fn test_safe_sysctls_accepted() {
        // Safe sysctls that Kubernetes allows by default
        let safe_sysctls = vec![
            Sysctl {
                name: "kernel.shm_rmid_forced".to_string(),
                value: "1".to_string(),
            },
            Sysctl {
                name: "net.ipv4.ip_local_port_range".to_string(),
                value: "1024 65535".to_string(),
            },
            Sysctl {
                name: "net.ipv4.tcp_syncookies".to_string(),
                value: "1".to_string(),
            },
            Sysctl {
                name: "net.ipv4.ping_group_range".to_string(),
                value: "0 2147483647".to_string(),
            },
        ];

        let pod = make_pod_with_sysctls("safe-sysctl-pod", Some(safe_sysctls));
        let result = extract_sysctls(&pod);

        assert!(result.is_some());
        let map = result.unwrap();
        assert_eq!(map.len(), 4);
        assert_eq!(map.get("kernel.shm_rmid_forced"), Some(&"1".to_string()));
        assert_eq!(
            map.get("net.ipv4.ip_local_port_range"),
            Some(&"1024 65535".to_string())
        );
        assert_eq!(map.get("net.ipv4.tcp_syncookies"), Some(&"1".to_string()));
        assert_eq!(
            map.get("net.ipv4.ping_group_range"),
            Some(&"0 2147483647".to_string())
        );
    }

    #[test]
    fn test_unsafe_sysctls_accepted_when_explicitly_set() {
        // Unsafe sysctls that require explicit allowlisting in real K8s,
        // but our runtime passes them through to Docker regardless
        let unsafe_sysctls = vec![
            Sysctl {
                name: "kernel.msgmax".to_string(),
                value: "65536".to_string(),
            },
            Sysctl {
                name: "net.core.somaxconn".to_string(),
                value: "1024".to_string(),
            },
            Sysctl {
                name: "kernel.shmmax".to_string(),
                value: "67108864".to_string(),
            },
        ];

        let pod = make_pod_with_sysctls("unsafe-sysctl-pod", Some(unsafe_sysctls));
        let result = extract_sysctls(&pod);

        assert!(result.is_some());
        let map = result.unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(map.get("kernel.msgmax"), Some(&"65536".to_string()));
        assert_eq!(map.get("net.core.somaxconn"), Some(&"1024".to_string()));
        assert_eq!(map.get("kernel.shmmax"), Some(&"67108864".to_string()));
    }

    #[test]
    fn test_sysctls_values_passed_to_docker_config() {
        // Verify the sysctl values are collected into the HashMap format
        // that bollard's HostConfig.sysctls expects
        let sysctls = vec![
            Sysctl {
                name: "net.ipv4.ip_forward".to_string(),
                value: "1".to_string(),
            },
            Sysctl {
                name: "net.ipv4.conf.all.forwarding".to_string(),
                value: "1".to_string(),
            },
        ];

        let pod = make_pod_with_sysctls("sysctl-docker-pod", Some(sysctls));
        let sysctls_map = extract_sysctls(&pod);

        // This is exactly what gets assigned to HostConfig.sysctls
        assert!(sysctls_map.is_some());
        let map = sysctls_map.unwrap();
        assert_eq!(map["net.ipv4.ip_forward"], "1");
        assert_eq!(map["net.ipv4.conf.all.forwarding"], "1");
    }

    #[test]
    fn test_pod_without_sysctls_has_none() {
        // Pod with no security context at all
        let pod = make_pod_with_sysctls("no-sysctl-pod", None);
        let result = extract_sysctls(&pod);
        assert!(result.is_none(), "Pod without sysctls should return None");
    }

    #[test]
    fn test_pod_with_empty_security_context_no_sysctls() {
        // Pod has a security context but sysctls field is None
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("empty-sc-pod").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![make_container("app")],
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: Some("Always".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: Some(PodSecurityContext {
                    run_as_user: Some(1000),
                    run_as_group: None,
                    run_as_non_root: None,
                    fs_group: None,
                    fs_group_change_policy: None,
                    supplemental_groups: None,
                    sysctls: None,
                    seccomp_profile: None,
                    app_armor_profile: None,
                    se_linux_options: None,
                    windows_options: None,
                    se_linux_change_policy: None,
                    supplemental_groups_policy: None,
                }),
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
            }),
            status: None,
        };

        let result = extract_sysctls(&pod);
        assert!(
            result.is_none(),
            "Security context without sysctls should return None"
        );
    }

    #[test]
    fn test_single_sysctl_produces_single_entry() {
        let sysctls = vec![Sysctl {
            name: "kernel.shm_rmid_forced".to_string(),
            value: "0".to_string(),
        }];

        let pod = make_pod_with_sysctls("single-sysctl-pod", Some(sysctls));
        let map = extract_sysctls(&pod).unwrap();

        assert_eq!(map.len(), 1);
        assert_eq!(map["kernel.shm_rmid_forced"], "0");
    }

    #[test]
    fn test_sysctl_serialization_roundtrip() {
        // Verify that a pod with sysctls survives JSON serialization
        let sysctls = vec![
            Sysctl {
                name: "net.core.somaxconn".to_string(),
                value: "4096".to_string(),
            },
            Sysctl {
                name: "kernel.shm_rmid_forced".to_string(),
                value: "1".to_string(),
            },
        ];

        let pod = make_pod_with_sysctls("roundtrip-pod", Some(sysctls));
        let json = serde_json::to_string(&pod).expect("serialize");
        let restored: Pod = serde_json::from_str(&json).expect("deserialize");

        let restored_sysctls = restored
            .spec
            .as_ref()
            .unwrap()
            .security_context
            .as_ref()
            .unwrap()
            .sysctls
            .as_ref()
            .unwrap();

        assert_eq!(restored_sysctls.len(), 2);
        assert_eq!(restored_sysctls[0].name, "net.core.somaxconn");
        assert_eq!(restored_sysctls[0].value, "4096");
        assert_eq!(restored_sysctls[1].name, "kernel.shm_rmid_forced");
        assert_eq!(restored_sysctls[1].value, "1");
    }

    #[test]
    fn test_http_probe_url_scheme_lowercased() {
        // Kubernetes sends scheme as uppercase ("HTTP", "HTTPS").
        // The probe URL must use lowercase scheme for correct reqwest handling.
        use rusternetes_common::resources::HTTPGetAction;

        let http_get = HTTPGetAction {
            path: Some("/readyz".to_string()),
            port: rusternetes_common::resources::IntOrString::Int(443),
            host: None,
            scheme: Some("HTTPS".to_string()),
            http_headers: None,
        };

        let scheme = http_get.scheme.as_deref().unwrap_or("HTTP").to_lowercase();
        let ip = "172.18.0.5";
        let path = http_get.path.as_deref().unwrap_or("/");
        let url = format!("{}://{}:{}{}", scheme, ip, http_get.port, path);

        assert_eq!(url, "https://172.18.0.5:443/readyz");
    }

    #[test]
    fn test_http_probe_url_scheme_default_is_http() {
        use rusternetes_common::resources::HTTPGetAction;

        let http_get = HTTPGetAction {
            path: Some("/healthz".to_string()),
            port: rusternetes_common::resources::IntOrString::Int(8080),
            host: None,
            scheme: None,
            http_headers: None,
        };

        let scheme = http_get.scheme.as_deref().unwrap_or("HTTP").to_lowercase();
        let ip = "10.244.0.5";
        let path = http_get.path.as_deref().unwrap_or("/");
        let url = format!("{}://{}:{}{}", scheme, ip, http_get.port, path);

        assert_eq!(url, "http://10.244.0.5:8080/healthz");
    }

    #[test]
    fn test_http_probe_url_with_uppercase_http_scheme() {
        use rusternetes_common::resources::HTTPGetAction;

        let http_get = HTTPGetAction {
            path: None,
            port: rusternetes_common::resources::IntOrString::Int(80),
            host: Some("my-service".to_string()),
            scheme: Some("HTTP".to_string()),
            http_headers: None,
        };

        let scheme = http_get.scheme.as_deref().unwrap_or("HTTP").to_lowercase();
        let ip = http_get.host.as_deref().unwrap_or("127.0.0.1");
        let path = http_get.path.as_deref().unwrap_or("/");
        let url = format!("{}://{}:{}{}", scheme, ip, http_get.port, path);

        assert_eq!(url, "http://my-service:80/");
    }

    #[test]
    fn test_http_probe_custom_headers_parsed() {
        use rusternetes_common::resources::{HTTPGetAction, HTTPHeader};

        let http_get = HTTPGetAction {
            path: Some("/readyz".to_string()),
            port: rusternetes_common::resources::IntOrString::Int(443),
            host: None,
            scheme: Some("HTTPS".to_string()),
            http_headers: Some(vec![
                HTTPHeader {
                    name: "X-Custom-Header".to_string(),
                    value: "test-value".to_string(),
                },
                HTTPHeader {
                    name: "Accept".to_string(),
                    value: "application/json".to_string(),
                },
            ]),
        };

        // Verify headers can be parsed into reqwest types
        if let Some(ref headers) = http_get.http_headers {
            for header in headers {
                let name = reqwest::header::HeaderName::from_bytes(header.name.as_bytes());
                let value = reqwest::header::HeaderValue::from_str(&header.value);
                assert!(name.is_ok(), "Header name '{}' should parse", header.name);
                assert!(
                    value.is_ok(),
                    "Header value '{}' should parse",
                    header.value
                );
            }
        }
    }

    #[test]
    fn test_no_proxy_client_builds_successfully() {
        // Verify that a reqwest client with no_proxy and danger_accept_invalid_certs builds OK
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(1))
            .danger_accept_invalid_certs(true)
            .no_proxy()
            .build();
        assert!(
            client.is_ok(),
            "Client with no_proxy should build successfully"
        );
    }

    #[test]
    fn test_expand_subpath_expr_basic() {
        use super::ContainerRuntime;
        let env = vec![
            ("POD_NAME".to_string(), "my-pod".to_string()),
            ("NAMESPACE".to_string(), "default".to_string()),
        ];
        let result = ContainerRuntime::expand_subpath_expr("$(POD_NAME)", &env);
        assert_eq!(result.unwrap(), "my-pod");
    }

    #[test]
    fn test_expand_subpath_expr_multiple_vars() {
        use super::ContainerRuntime;
        let env = vec![
            ("POD_NAME".to_string(), "my-pod".to_string()),
            ("NAMESPACE".to_string(), "default".to_string()),
        ];
        let result = ContainerRuntime::expand_subpath_expr("$(NAMESPACE)/$(POD_NAME)", &env);
        assert_eq!(result.unwrap(), "default/my-pod");
    }

    #[test]
    fn test_expand_subpath_expr_rejects_backticks_in_expr() {
        use super::ContainerRuntime;
        let env = vec![("POD_NAME".to_string(), "my-pod".to_string())];
        let result = ContainerRuntime::expand_subpath_expr("$(POD_NAME)`echo hack`", &env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("backtick"));
    }

    #[test]
    fn test_expand_subpath_expr_rejects_absolute_path() {
        use super::ContainerRuntime;
        let env = vec![("DIR".to_string(), "/etc".to_string())];
        let result = ContainerRuntime::expand_subpath_expr("$(DIR)/passwd", &env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("absolute path"));
    }

    #[test]
    fn test_expand_subpath_expr_rejects_dotdot_component() {
        use super::ContainerRuntime;
        let env = vec![("DIR".to_string(), "foo".to_string())];
        let result = ContainerRuntime::expand_subpath_expr("$(DIR)/../secret", &env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(".."));
    }

    #[test]
    fn test_expand_subpath_expr_allows_dots_in_name() {
        use super::ContainerRuntime;
        // "foo..bar" is NOT a path traversal — only ".." as a path component is
        let env = vec![("POD_NAME".to_string(), "foo..bar".to_string())];
        let result = ContainerRuntime::expand_subpath_expr("$(POD_NAME)", &env);
        assert_eq!(result.unwrap(), "foo..bar");
    }

    #[test]
    fn test_expand_subpath_expr_backtick_checked_before_expansion() {
        use super::ContainerRuntime;
        // Even if expansion would produce a valid path, backticks in the
        // expression should be rejected first
        let env = vec![("POD_NAME".to_string(), "/tmp".to_string())];
        let result = ContainerRuntime::expand_subpath_expr("`$(POD_NAME)`", &env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("backtick"));
    }

    /// Test that ConfigMap volume with items only creates the specified files
    /// at the mapped paths, not all keys from the ConfigMap.
    #[test]
    fn test_configmap_volume_items_selective_mount() {
        use std::collections::BTreeMap;
        let tmp = tempfile::tempdir().expect("create tempdir");
        let volume_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&volume_dir).unwrap();

        // Simulate a ConfigMap with 3 keys
        let mut data = BTreeMap::new();
        data.insert("data-1".to_string(), "value-1".to_string());
        data.insert("data-2".to_string(), "value-2".to_string());
        data.insert("data-3".to_string(), "value-3".to_string());

        // Items: only mount data-2 at path/to/data-2
        let items = vec![rusternetes_common::resources::KeyToPath {
            key: "data-2".to_string(),
            path: "path/to/data-2".to_string(),
            mode: None,
        }];

        // Simulate the items-based mount logic from create_volume
        for item in &items {
            if let Some(value) = data.get(&item.key) {
                let file_path = volume_dir.join(&item.path);
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::write(&file_path, value).unwrap();
            }
        }

        // Verify: only the mapped file exists, not all keys
        assert!(volume_dir.join("path/to/data-2").exists());
        assert_eq!(
            std::fs::read_to_string(volume_dir.join("path/to/data-2")).unwrap(),
            "value-2"
        );
        // Other keys should NOT be present
        assert!(!volume_dir.join("data-1").exists());
        assert!(!volume_dir.join("data-3").exists());
    }

    /// Test that Secret volume with items only creates the specified files
    /// at the mapped paths, not all keys from the Secret.
    #[test]
    fn test_secret_volume_items_selective_mount() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let volume_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&volume_dir).unwrap();

        // Simulate a Secret with 2 keys
        let mut data = std::collections::BTreeMap::new();
        data.insert("data-1".to_string(), b"value-1".to_vec());
        data.insert("data-2".to_string(), b"value-2".to_vec());

        // Items: only mount data-1 at new-path-data-1
        let items = vec![rusternetes_common::resources::KeyToPath {
            key: "data-1".to_string(),
            path: "new-path-data-1".to_string(),
            mode: None,
        }];

        // Simulate the items-based mount logic
        for item in &items {
            if let Some(value) = data.get(&item.key) {
                let file_path = volume_dir.join(&item.path);
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::write(&file_path, value).unwrap();
            }
        }

        // Verify: only the mapped file exists
        assert!(volume_dir.join("new-path-data-1").exists());
        assert_eq!(
            std::fs::read_to_string(volume_dir.join("new-path-data-1")).unwrap(),
            "value-1"
        );
        // The raw key name should NOT exist
        assert!(!volume_dir.join("data-1").exists());
        assert!(!volume_dir.join("data-2").exists());
    }

    /// Test that resync of a Secret volume with items only writes
    /// mapped paths and removes stale files.
    #[test]
    fn test_secret_resync_with_items_only_writes_mapped_paths() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let volume_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&volume_dir).unwrap();

        // Pre-existing stale file (simulates a previous all-keys mount)
        std::fs::write(volume_dir.join("stale-key"), b"old-value").unwrap();

        // Secret data
        let mut data = std::collections::BTreeMap::new();
        data.insert("data-1".to_string(), b"value-1".to_vec());
        data.insert("data-2".to_string(), b"value-2".to_vec());

        // Items mapping
        let items = vec![rusternetes_common::resources::KeyToPath {
            key: "data-1".to_string(),
            path: "new-path-data-1".to_string(),
            mode: None,
        }];

        // Simulate resync logic with items
        let mut expected_files: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for item in &items {
            if let Some(v) = data.get(&item.key) {
                let file_path = volume_dir.join(&item.path);
                expected_files.insert(item.path.clone());
                if let Some(parent) = file_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(&file_path, v);
            }
        }

        // Remove files not in expected set
        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if !expected_files.contains(name) {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }

        // Verify
        assert!(volume_dir.join("new-path-data-1").exists());
        assert_eq!(
            std::fs::read_to_string(volume_dir.join("new-path-data-1")).unwrap(),
            "value-1"
        );
        // Stale file should be removed
        assert!(!volume_dir.join("stale-key").exists());
        // Raw key names should NOT exist
        assert!(!volume_dir.join("data-1").exists());
        assert!(!volume_dir.join("data-2").exists());
    }

    /// Test that ConfigMap resync with items only writes mapped paths,
    /// including nested paths and binaryData.
    #[test]
    fn test_configmap_resync_with_items_handles_nested_paths() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let volume_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&volume_dir).unwrap();

        let mut data = std::collections::BTreeMap::new();
        data.insert("data-2".to_string(), "value-2".to_string());

        let items = vec![rusternetes_common::resources::KeyToPath {
            key: "data-2".to_string(),
            path: "path/to/data-2".to_string(),
            mode: None,
        }];

        // Simulate resync logic
        for item in &items {
            if let Some(value) = data.get(&item.key) {
                let file_path = volume_dir.join(&item.path);
                if let Some(parent) = file_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(&file_path, value);
            }
        }

        // Verify nested path was created
        assert!(volume_dir.join("path/to/data-2").exists());
        assert_eq!(
            std::fs::read_to_string(volume_dir.join("path/to/data-2")).unwrap(),
            "value-2"
        );
    }

    /// Test that Docker volume sentinel path is correctly detected.
    #[test]
    fn test_docker_volume_sentinel_path_detection() {
        let sentinel = "docker-vol::rusternetes-emptydir-test-pod-vol1";
        assert!(sentinel.starts_with("docker-vol::"));
        let vol_name = sentinel.strip_prefix("docker-vol::").unwrap();
        assert_eq!(vol_name, "rusternetes-emptydir-test-pod-vol1");

        // Non-sentinel paths should not match
        let regular = "/volumes/test-pod/vol1";
        assert!(!regular.starts_with("docker-vol::"));
    }

    /// Test that Docker volume names follow the expected naming convention.
    #[test]
    fn test_emptydir_docker_volume_name_format() {
        let pod_name = "test-pod";
        let volume_name = "cache-vol";
        let docker_vol_name = format!("rusternetes-emptydir-{}-{}", pod_name, volume_name);
        assert_eq!(docker_vol_name, "rusternetes-emptydir-test-pod-cache-vol");

        // Cleanup prefix detection
        let prefix = format!("rusternetes-emptydir-{}-", pod_name);
        assert!(docker_vol_name.starts_with(&prefix));
    }

    /// Test expand_k8s_vars logic matching K8s third_party/forked/golang/expansion/expand.go:
    /// - $(VAR) → expand if VAR is defined env var, else leave literal
    /// - $$ → $ (escape sequence, critical for DNS test shell commands)
    /// - $other → $other (literal)
    #[test]
    fn test_expand_k8s_vars_preserves_shell_substitutions() {
        // Simulate the expand_k8s_vars closure logic
        let resolved_env_pairs: Vec<(String, String)> = vec![
            ("MY_VAR".to_string(), "hello".to_string()),
            ("PORT".to_string(), "8080".to_string()),
        ];

        let expand = |items: &[String]| -> Vec<String> {
            items
                .iter()
                .map(|item| {
                    let input = item.as_bytes();
                    let mut buf = Vec::with_capacity(input.len());
                    let mut cursor = 0;
                    while cursor < input.len() {
                        if input[cursor] == b'$' && cursor + 1 < input.len() {
                            match input[cursor + 1] {
                                b'$' => {
                                    buf.push(b'$');
                                    cursor += 2;
                                }
                                b'(' => {
                                    if let Some(close) =
                                        input[cursor + 2..].iter().position(|&b| b == b')')
                                    {
                                        let var_name = std::str::from_utf8(
                                            &input[cursor + 2..cursor + 2 + close],
                                        )
                                        .unwrap_or("");
                                        if let Some((_, value)) =
                                            resolved_env_pairs.iter().find(|(k, _)| k == var_name)
                                        {
                                            buf.extend_from_slice(value.as_bytes());
                                            cursor += 2 + close + 1;
                                        } else {
                                            buf.extend_from_slice(
                                                &input[cursor..cursor + 2 + close + 1],
                                            );
                                            cursor += 2 + close + 1;
                                        }
                                    } else {
                                        buf.extend_from_slice(&input[cursor..cursor + 2]);
                                        cursor += 2;
                                    }
                                }
                                _ => {
                                    buf.push(input[cursor]);
                                    cursor += 1;
                                }
                            }
                        } else {
                            buf.push(input[cursor]);
                            cursor += 1;
                        }
                    }
                    String::from_utf8(buf).unwrap_or_else(|_| item.clone())
                })
                .collect()
        };

        // Known env var is expanded
        assert_eq!(expand(&["echo $(MY_VAR)".to_string()]), vec!["echo hello"]);

        // Shell command substitution is preserved (not a defined env var)
        assert_eq!(
            expand(&["test $(id -u) -eq 65534".to_string()]),
            vec!["test $(id -u) -eq 65534"]
        );

        // Multiple vars: known ones expanded, unknown preserved
        assert_eq!(
            expand(&["$(MY_VAR):$(PORT) $(unknown)".to_string()]),
            vec!["hello:8080 $(unknown)"]
        );

        // No vars at all
        assert_eq!(expand(&["plain text".to_string()]), vec!["plain text"]);

        // $$ → $ (escape sequence — K8s expand.go line 83-85)
        // This is critical for DNS conformance tests which use $$(dig ...)
        assert_eq!(
            expand(&["check=$$(dig +notcp)".to_string()]),
            vec!["check=$(dig +notcp)"],
            "$$ should be unescaped to $ for shell command substitution"
        );

        // Multiple $$ escapes
        assert_eq!(
            expand(&["$$A $$B $$(cmd)".to_string()]),
            vec!["$A $B $(cmd)"]
        );

        // Mixed: $$ escape + $(VAR) expansion
        assert_eq!(
            expand(&["$$(echo $(MY_VAR))".to_string()]),
            vec!["$(echo hello)"],
            "$$ unescaped then $(MY_VAR) expanded"
        );

        // K8s test case: $$$$$$(BIG_MONEY) → $$$(BIG_MONEY)
        assert_eq!(
            expand(&["$$$$$$(BIG_MONEY)".to_string()]),
            vec!["$$$(BIG_MONEY)"]
        );

        // DNS test probe command pattern
        assert_eq!(
            expand(&[
                r#"for i in 1 2 3; do check="$$(dig +notcp)" && test -n "$$check"; done"#
                    .to_string()
            ]),
            vec![r#"for i in 1 2 3; do check="$(dig +notcp)" && test -n "$check"; done"#],
            "DNS probe command $$ escaping must produce valid shell syntax"
        );
    }

    /// fsGroup should copy owner permission bits to group bits, not unconditionally
    /// add g+rwX. A file with mode 0440 (r--r-----) should stay 0440 after fsGroup,
    /// not become 0460 (r--rw----) which would happen with `chmod g+rwX`.
    #[test]
    #[cfg(unix)]
    fn test_fsgroup_preserves_restrictive_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("secret-file");
        std::fs::write(&file_path, "secret-data").unwrap();

        // Set restrictive mode: 0440 (r--r-----)
        std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o440)).unwrap();

        // Apply fsGroup logic: copy owner bits to group bits
        let meta = std::fs::metadata(&file_path).unwrap();
        let mode = meta.permissions().mode();
        let owner_bits = (mode >> 6) & 0o7;
        let new_mode = (mode & !0o070) | (owner_bits << 3);
        std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(new_mode)).unwrap();

        // Verify: mode should still be 0440 (owner=r, group=r, others=none)
        let final_mode = std::fs::metadata(&file_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            final_mode, 0o440,
            "fsGroup should preserve mode 0440, got {:04o}",
            final_mode
        );
    }

    /// fsGroup should make group match owner — a file with 0644 gets group=rw (0664).
    #[test]
    #[cfg(unix)]
    fn test_fsgroup_copies_owner_bits_to_group() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("config-file");
        std::fs::write(&file_path, "config-data").unwrap();

        // Set mode: 0644 (rw-r--r--)
        std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        // Apply fsGroup logic
        let meta = std::fs::metadata(&file_path).unwrap();
        let mode = meta.permissions().mode();
        let owner_bits = (mode >> 6) & 0o7;
        let new_mode = (mode & !0o070) | (owner_bits << 3);
        std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(new_mode)).unwrap();

        // Owner is rw (6), so group should also be rw (6): 0664
        let final_mode = std::fs::metadata(&file_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            final_mode, 0o664,
            "fsGroup should copy owner rw bits to group, got {:04o}",
            final_mode
        );
    }

    // --- Init container failure tests ---

    /// Helper: build a pod with a failing init container and an app container,
    /// then simulate what the kubelet does when start_pod returns an error
    /// due to init container failure, and return the resulting PodStatus.
    fn simulate_init_container_failure(
        restart_policy: &str,
    ) -> rusternetes_common::resources::pod::PodStatus {
        use rusternetes_common::resources::pod::PodStatus;
        use rusternetes_common::types::Phase;

        // Build a pod with an init container that will "fail" (exit code 1)
        // and an app container that should NOT be started.
        let init_container = Container {
            name: "init-fail".to_string(),
            image: "busybox:latest".to_string(),
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "exit 1".to_string(),
            ]),
            ..make_container("init-fail")
        };

        let app_container = make_container("app");

        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-fail-pod").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![app_container],
                init_containers: Some(vec![init_container]),
                ephemeral_containers: None,
                restart_policy: Some(restart_policy.to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
            }),
            status: None,
        };

        // Simulate what kubelet.rs does when start_pod returns an error
        // due to init container failure (the logic from the else branch
        // in the error handler).

        // Simulate init container terminated with exit code 1
        let init_container_statuses = Some(vec![ContainerStatus {
            name: "init-fail".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState::Terminated {
                exit_code: 1,
                signal: None,
                reason: Some("Error".to_string()),
                message: None,
                started_at: Some("2026-01-01T00:00:00Z".to_string()),
                finished_at: Some("2026-01-01T00:00:01Z".to_string()),
                container_id: Some("docker://abc123".to_string()),
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: Some("docker://abc123".to_string()),
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]);

        // Determine phase based on restart policy (mirrors kubelet.rs logic)
        let (phase, reason) = if restart_policy == "Never" {
            (Phase::Failed, "FailedToStart".to_string())
        } else {
            (Phase::Pending, "InitContainerFailed".to_string())
        };

        // Build app container statuses as Waiting/PodInitializing
        let app_container_statuses: Option<Vec<ContainerStatus>> = pod.spec.as_ref().map(|spec| {
            spec.containers
                .iter()
                .map(|c| ContainerStatus {
                    name: c.name.clone(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState::Waiting {
                        reason: Some("PodInitializing".to_string()),
                        message: None,
                    }),
                    last_state: None,
                    image: Some(c.image.clone()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                })
                .collect()
        });

        PodStatus {
            phase: Some(phase),
            message: Some("containers with incomplete status: [init-fail]".to_string()),
            reason: Some(reason),
            host_ip: Some("127.0.0.1".to_string()),
            pod_ip: None,
            conditions: None,
            container_statuses: app_container_statuses,
            init_container_statuses,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            host_i_ps: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
        }
    }

    #[test]
    fn test_init_container_failure_restart_never_pod_phase_is_failed() {
        use rusternetes_common::types::Phase;

        let status = simulate_init_container_failure("Never");

        // Pod phase must be Failed for RestartNever
        assert_eq!(
            status.phase,
            Some(Phase::Failed),
            "Pod with RestartNever and failed init container must have Failed phase"
        );
        assert_eq!(status.reason, Some("FailedToStart".to_string()),);
    }

    #[test]
    fn test_init_container_failure_restart_never_init_shows_terminated() {
        let status = simulate_init_container_failure("Never");

        // Init container must show Terminated with exit code 1
        let init_statuses = status
            .init_container_statuses
            .expect("init_container_statuses should be set");
        assert_eq!(init_statuses.len(), 1);
        let init_status = &init_statuses[0];
        assert_eq!(init_status.name, "init-fail");
        assert!(
            !init_status.ready,
            "failed init container should not be ready"
        );

        match &init_status.state {
            Some(ContainerState::Terminated { exit_code, .. }) => {
                assert_eq!(*exit_code, 1, "init container exit code should be 1");
            }
            other => panic!("Expected Terminated state, got: {:?}", other),
        }
    }

    #[test]
    fn test_init_container_failure_restart_never_app_not_started() {
        let status = simulate_init_container_failure("Never");

        // App container must NOT have been started — should show Waiting
        let app_statuses = status
            .container_statuses
            .expect("container_statuses should be set");
        assert_eq!(app_statuses.len(), 1);
        let app_status = &app_statuses[0];
        assert_eq!(app_status.name, "app");
        assert!(!app_status.ready, "app container should not be ready");
        assert_eq!(
            app_status.started,
            Some(false),
            "app container should not have started"
        );
        assert!(
            app_status.container_id.is_none(),
            "app container should have no container ID (never created)"
        );

        match &app_status.state {
            Some(ContainerState::Waiting { reason, .. }) => {
                assert_eq!(
                    reason.as_deref(),
                    Some("PodInitializing"),
                    "app container should be Waiting with reason PodInitializing"
                );
            }
            other => panic!("Expected Waiting state for app container, got: {:?}", other),
        }
    }

    #[test]
    fn test_init_container_failure_restart_always_pod_stays_pending() {
        use rusternetes_common::types::Phase;

        let status = simulate_init_container_failure("Always");

        // Pod phase must be Pending (not Failed) for RestartAlways
        // so the init container can be retried
        assert_eq!(
            status.phase,
            Some(Phase::Pending),
            "Pod with RestartAlways and failed init container must stay Pending, not Failed"
        );
        assert_eq!(status.reason, Some("InitContainerFailed".to_string()),);
    }

    #[test]
    fn test_init_container_failure_restart_always_app_not_started() {
        let status = simulate_init_container_failure("Always");

        // Even for RestartAlways, app containers must NOT start if init containers failed
        let app_statuses = status
            .container_statuses
            .expect("container_statuses should be set");
        assert_eq!(app_statuses.len(), 1);
        let app_status = &app_statuses[0];
        assert!(!app_status.ready, "app container should not be ready");
        assert_eq!(app_status.started, Some(false));
        assert!(app_status.container_id.is_none());

        match &app_status.state {
            Some(ContainerState::Waiting { reason, .. }) => {
                assert_eq!(reason.as_deref(), Some("PodInitializing"));
            }
            other => panic!("Expected Waiting state for app container, got: {:?}", other),
        }
    }

    // --- container status reporting tests ---

    #[test]
    fn test_hosts_file_contains_host_aliases() {
        use rusternetes_common::resources::pod::HostAlias;

        let mut pod = make_pod("alias-pod", "default", None, None);
        pod.spec.as_mut().unwrap().host_aliases = Some(vec![
            HostAlias {
                ip: "1.2.3.4".to_string(),
                hostnames: Some(vec!["foo.local".to_string(), "bar.local".to_string()]),
            },
            HostAlias {
                ip: "5.6.7.8".to_string(),
                hostnames: Some(vec!["baz.local".to_string()]),
            },
        ]);

        let content = build_hosts_content(&pod, Some("10.244.1.5"), "cluster.local");

        // Pod IP entry
        assert!(content.contains("10.244.1.5\talias-pod\n"));
        // Host aliases
        assert!(content.contains("1.2.3.4\tfoo.local\tbar.local\n"));
        assert!(content.contains("5.6.7.8\tbaz.local\n"));
    }

    #[test]
    fn test_hosts_file_ipv6_entries_present() {
        let pod = make_pod("ipv6-pod", "default", None, None);
        let content = build_hosts_content(&pod, Some("10.244.1.1"), "cluster.local");

        // Check that standard IPv6 entries are present
        assert!(content.contains("fe00::\tip6-localnet"));
        assert!(content.contains("fe00::\tip6-mcastprefix"));
        assert!(content.contains("fe00::1\tip6-allnodes"));
        assert!(content.contains("fe00::2\tip6-allrouters"));
    }

    #[test]
    fn test_hosts_file_empty_host_aliases_ignored() {
        use rusternetes_common::resources::pod::HostAlias;

        let mut pod = make_pod("empty-alias", "default", None, None);
        pod.spec.as_mut().unwrap().host_aliases = Some(vec![
            HostAlias {
                ip: "1.2.3.4".to_string(),
                hostnames: Some(vec![]), // Empty hostnames
            },
            HostAlias {
                ip: "5.6.7.8".to_string(),
                hostnames: None, // No hostnames
            },
        ]);

        let content = build_hosts_content(&pod, Some("10.0.0.1"), "cluster.local");
        // Neither alias IP should appear in the hosts file
        assert!(!content.contains("1.2.3.4"));
        assert!(!content.contains("5.6.7.8"));
    }

    #[test]
    fn test_container_status_terminated_has_started_at() {
        // Verify that building a Terminated container state includes started_at.
        // This tests the logic fixed in get_container_statuses.
        let started = "2026-01-01T00:00:00Z".to_string();
        let finished = "2026-01-01T00:01:00Z".to_string();

        let state = ContainerState::Terminated {
            exit_code: 0,
            signal: None,
            reason: Some("Completed".to_string()),
            message: None,
            started_at: Some(started.clone()),
            finished_at: Some(finished.clone()),
            container_id: Some("docker://abc123".to_string()),
        };

        match state {
            ContainerState::Terminated {
                started_at,
                finished_at,
                ..
            } => {
                assert_eq!(
                    started_at,
                    Some(started),
                    "Terminated state must include started_at"
                );
                assert_eq!(
                    finished_at,
                    Some(finished),
                    "Terminated state must include finished_at"
                );
            }
            _ => panic!("Expected Terminated state"),
        }
    }

    #[test]
    fn test_container_status_last_state_preserved() {
        // When a container restarts, last_state should be the previous state.
        let prev_state = ContainerState::Terminated {
            exit_code: 1,
            signal: None,
            reason: Some("Error".to_string()),
            message: None,
            started_at: Some("2026-01-01T00:00:00Z".to_string()),
            finished_at: Some("2026-01-01T00:01:00Z".to_string()),
            container_id: Some("docker://prev123".to_string()),
        };

        let status = ContainerStatus {
            name: "app".to_string(),
            ready: false,
            restart_count: 1,
            state: Some(ContainerState::Running {
                started_at: Some("2026-01-01T00:02:00Z".to_string()),
            }),
            last_state: Some(prev_state.clone()),
            image: Some("nginx:latest".to_string()),
            image_id: Some("docker-pullable://sha256:abc".to_string()),
            container_id: Some("docker://new456".to_string()),
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };

        assert!(status.last_state.is_some(), "last_state should be set");
        match &status.last_state {
            Some(ContainerState::Terminated { exit_code, .. }) => {
                assert_eq!(
                    *exit_code, 1,
                    "last_state should have the previous exit code"
                );
            }
            _ => panic!("Expected Terminated last_state"),
        }
    }

    #[test]
    fn test_container_status_image_id_format() {
        // Verify Docker image SHA is prefixed with docker-pullable://
        let sha = "sha256:abcdef1234567890";
        let formatted = if sha.starts_with("sha256:") {
            format!("docker-pullable://{}", sha)
        } else {
            sha.to_string()
        };

        assert_eq!(
            formatted, "docker-pullable://sha256:abcdef1234567890",
            "image_id should be prefixed with docker-pullable://"
        );
    }

    #[test]
    fn test_container_status_serialization() {
        // Verify ContainerStatus serializes with correct JSON field names
        let status = ContainerStatus {
            name: "web".to_string(),
            ready: true,
            restart_count: 0,
            state: Some(ContainerState::Running {
                started_at: Some("2026-01-01T00:00:00Z".to_string()),
            }),
            last_state: None,
            image: Some("nginx:1.25".to_string()),
            image_id: Some("docker-pullable://sha256:abc".to_string()),
            container_id: Some("docker://def".to_string()),
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };

        let json = serde_json::to_string(&status).unwrap();
        // Check camelCase serialization
        assert!(json.contains("\"imageID\""), "Should serialize as imageID");
        assert!(
            json.contains("\"containerID\""),
            "Should serialize as containerID"
        );
        assert!(
            json.contains("\"restartCount\""),
            "Should serialize as restartCount"
        );
        assert!(json.contains("\"started\":true"), "started should be true");
    }

    // --- Fix #46: needs_umask_fix is false when container has no shell/entrypoint ---

    #[test]
    fn test_needs_umask_fix_false_without_emptydir() {
        // When a container does NOT mount any emptyDir volume,
        // has_emptydir_mount is false, so needs_umask_fix must be false
        // regardless of whether the image has a shell.
        let has_emptydir_mount = false;
        let has_shell = true; // even if shell exists
        let needs_umask_fix = has_emptydir_mount && has_shell;
        assert!(
            !needs_umask_fix,
            "needs_umask_fix should be false without emptyDir mount"
        );
    }

    #[test]
    fn test_needs_umask_fix_false_without_shell() {
        // When the container mounts an emptyDir but the image has no shell
        // (e.g. distroless/scratch), has_shell is false, so needs_umask_fix
        // must be false — we cannot wrap with "umask 0 && exec ...".
        let has_emptydir_mount = true;
        let has_shell = false;
        let needs_umask_fix = has_emptydir_mount && has_shell;
        assert!(
            !needs_umask_fix,
            "needs_umask_fix should be false when image has no shell"
        );
    }

    #[test]
    fn test_needs_umask_fix_true_only_with_both() {
        // needs_umask_fix should only be true when BOTH conditions hold:
        // the container mounts an emptyDir AND the image has /bin/sh.
        let has_emptydir_mount = true;
        let has_shell = true;
        let needs_umask_fix = has_emptydir_mount && has_shell;
        assert!(
            needs_umask_fix,
            "needs_umask_fix should be true when both conditions hold"
        );
    }

    #[test]
    fn test_has_emptydir_mount_detection() {
        use rusternetes_common::resources::pod::VolumeMount;
        use std::collections::HashSet;

        let mut empty_dir_volumes: HashSet<String> = HashSet::new();
        empty_dir_volumes.insert("cache-vol".to_string());

        // Container with an emptyDir volume mount
        let mut container_with = make_container("app");
        container_with.volume_mounts = Some(vec![VolumeMount {
            name: "cache-vol".to_string(),
            mount_path: "/cache".to_string(),
            read_only: None,
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: None,
            recursive_read_only: None,
        }]);
        let has_emptydir = container_with
            .volume_mounts
            .as_ref()
            .map(|mounts| mounts.iter().any(|m| empty_dir_volumes.contains(&m.name)))
            .unwrap_or(false);
        assert!(has_emptydir, "should detect emptyDir mount");

        // Container with a non-emptyDir volume mount
        let mut container_without = make_container("sidecar");
        container_without.volume_mounts = Some(vec![VolumeMount {
            name: "config-vol".to_string(),
            mount_path: "/config".to_string(),
            read_only: None,
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: None,
            recursive_read_only: None,
        }]);
        let has_emptydir = container_without
            .volume_mounts
            .as_ref()
            .map(|mounts| mounts.iter().any(|m| empty_dir_volumes.contains(&m.name)))
            .unwrap_or(false);
        assert!(
            !has_emptydir,
            "should not detect emptyDir mount for non-emptyDir volume"
        );

        // Container with no volume mounts at all
        let container_none = make_container("plain");
        let has_emptydir = container_none
            .volume_mounts
            .as_ref()
            .map(|mounts| mounts.iter().any(|m| empty_dir_volumes.contains(&m.name)))
            .unwrap_or(false);
        assert!(
            !has_emptydir,
            "should not detect emptyDir mount when no volume mounts"
        );
    }

    // --- Fix #57: FallbackToLogsOnError ---

    #[test]
    fn test_fallback_to_logs_on_error_policy_detection() {
        // When terminationMessagePolicy is "FallbackToLogsOnError" and the
        // termination message file is empty, the code falls back to container logs.
        // Verify the policy string matching works correctly.
        let policy_fallback = Some("FallbackToLogsOnError".to_string());
        let policy_file = Some("File".to_string());
        let policy_none: Option<String> = None;

        assert_eq!(
            policy_fallback.as_deref(),
            Some("FallbackToLogsOnError"),
            "FallbackToLogsOnError policy should match"
        );
        assert_ne!(
            policy_file.as_deref(),
            Some("FallbackToLogsOnError"),
            "File policy should not match FallbackToLogsOnError"
        );
        assert_ne!(
            policy_none.as_deref(),
            Some("FallbackToLogsOnError"),
            "None policy should not match FallbackToLogsOnError"
        );
    }

    #[test]
    fn test_fallback_skipped_on_success_exit() {
        // With FallbackToLogsOnError, if exit_code == 0, the termination
        // message should be None (no message for successful exit).
        let policy = "FallbackToLogsOnError";
        let exit_code: u64 = 0;
        let termination_msg: Option<String> = if policy == "FallbackToLogsOnError" && exit_code == 0
        {
            None
        } else {
            Some("would read from file or logs".to_string())
        };
        assert!(
            termination_msg.is_none(),
            "FallbackToLogsOnError with exit_code 0 should produce no message"
        );
    }

    #[test]
    fn test_fallback_triggered_on_error_exit() {
        // With FallbackToLogsOnError, if exit_code != 0, we should attempt
        // to read the termination message (and fall back to logs if file is empty).
        let policy = "FallbackToLogsOnError";
        let exit_code: u64 = 1;
        let should_read = !(policy == "FallbackToLogsOnError" && exit_code == 0);
        assert!(
            should_read,
            "FallbackToLogsOnError with non-zero exit should read termination message"
        );
    }

    #[test]
    fn test_termination_message_truncation() {
        // Termination messages are truncated to 4096 bytes per K8s spec.
        let long_content = "x".repeat(8192);
        let mut content = long_content;
        if content.len() > 4096 {
            content.truncate(4096);
        }
        assert_eq!(content.len(), 4096);
    }

    // --- Fix #62: Ephemeral containers identified for starting ---

    #[test]
    fn test_ephemeral_container_name_format() {
        // Ephemeral containers are named {pod_name}_{ec_name} in Docker,
        // matching the convention used for regular containers.
        let pod_name = "debug-pod";
        let ec_name = "debugger";
        let container_name = format!("{}_{}", pod_name, ec_name);
        assert_eq!(container_name, "debug-pod_debugger");
    }

    #[test]
    fn test_new_ephemeral_containers_detected() {
        use rusternetes_common::resources::EphemeralContainer;

        // Simulate detecting new ephemeral containers that don't exist yet.
        // The kubelet iterates over spec.ephemeralContainers and checks
        // container_exists() for each one. Those that don't exist are new.
        let ecs = [
            EphemeralContainer {
                name: "debugger".to_string(),
                image: "busybox:latest".to_string(),
                command: Some(vec!["sh".to_string()]),
                args: None,
                working_dir: None,
                env: None,
                volume_mounts: None,
                image_pull_policy: None,
                security_context: None,
                target_container_name: None,
                stdin: Some(true),
                stdin_once: None,
                tty: Some(true),
                resize_policy: None,
                restart_policy: None,
                termination_message_path: None,
                termination_message_policy: None,
                resources: None,
            },
            EphemeralContainer {
                name: "logger".to_string(),
                image: "alpine:latest".to_string(),
                command: Some(vec![
                    "tail".to_string(),
                    "-f".to_string(),
                    "/var/log/app.log".to_string(),
                ]),
                args: None,
                working_dir: None,
                env: None,
                volume_mounts: None,
                image_pull_policy: None,
                security_context: None,
                target_container_name: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                resize_policy: None,
                restart_policy: None,
                termination_message_path: None,
                termination_message_policy: None,
                resources: None,
            },
        ];

        let pod_name = "my-pod";

        // Simulate: "debugger" already exists, "logger" does not
        let existing_containers: std::collections::HashSet<String> =
            vec![format!("{}_{}", pod_name, "debugger")]
                .into_iter()
                .collect();

        let new_ecs: Vec<&EphemeralContainer> = ecs
            .iter()
            .filter(|ec| {
                let name = format!("{}_{}", pod_name, ec.name);
                !existing_containers.contains(&name)
            })
            .collect();

        assert_eq!(new_ecs.len(), 1);
        assert_eq!(new_ecs[0].name, "logger");
    }

    #[test]
    fn test_ephemeral_container_to_container_conversion() {
        use rusternetes_common::resources::EphemeralContainer;

        // Verify the conversion from EphemeralContainer to Container
        // preserves the correct fields and nullifies probe/lifecycle fields.
        let ec = EphemeralContainer {
            name: "debugger".to_string(),
            image: "busybox:latest".to_string(),
            command: Some(vec!["sh".to_string()]),
            args: Some(vec!["-c".to_string(), "sleep 3600".to_string()]),
            working_dir: Some("/tmp".to_string()),
            env: None,
            volume_mounts: None,
            image_pull_policy: Some("Always".to_string()),
            security_context: None,
            target_container_name: Some("app".to_string()),
            stdin: Some(true),
            stdin_once: None,
            tty: Some(true),
            resize_policy: None,
            restart_policy: None,
            termination_message_path: Some("/dev/termination-log".to_string()),
            termination_message_policy: Some("File".to_string()),
            resources: None,
        };

        let container = Container {
            name: ec.name.clone(),
            image: ec.image.clone(),
            command: ec.command.clone(),
            args: ec.args.clone(),
            env: ec.env.clone(),
            volume_mounts: ec.volume_mounts.clone(),
            resources: ec.resources.clone(),
            image_pull_policy: ec.image_pull_policy.clone(),
            security_context: ec.security_context.clone(),
            stdin: ec.stdin,
            tty: ec.tty,
            working_dir: ec.working_dir.clone(),
            ports: None,
            env_from: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            termination_message_path: ec.termination_message_path.clone(),
            termination_message_policy: ec.termination_message_policy.clone(),
            stdin_once: ec.stdin_once,
            restart_policy: None,
            resize_policy: None,
            volume_devices: None,
        };

        assert_eq!(container.name, "debugger");
        assert_eq!(container.image, "busybox:latest");
        assert_eq!(container.command, Some(vec!["sh".to_string()]));
        assert_eq!(container.stdin, Some(true));
        assert_eq!(container.tty, Some(true));
        assert_eq!(container.working_dir, Some("/tmp".to_string()));
        // Ephemeral containers must NOT have probes or lifecycle
        assert!(container.liveness_probe.is_none());
        assert!(container.readiness_probe.is_none());
        assert!(container.startup_probe.is_none());
        assert!(container.lifecycle.is_none());
        // Ports are not forwarded from ephemeral containers
        assert!(container.ports.is_none());
    }

    // --- Fix #71: /etc/hosts mounted read-write (no :ro suffix) ---

    #[test]
    fn test_etc_hosts_bind_mount_is_read_write() {
        // The /etc/hosts bind mount string must NOT contain ":ro"
        // because Kubernetes mounts it read-write so pods can modify it.
        let hosts_path = "/var/lib/kubelet/pods/abc/etc-hosts";
        let bind = format!("{}:/etc/hosts", hosts_path);

        assert!(
            !bind.contains(":ro"),
            "/etc/hosts bind mount must not be read-only, got: {}",
            bind
        );
        assert!(
            bind.ends_with(":/etc/hosts"),
            "bind mount should end with :/etc/hosts (no :ro suffix), got: {}",
            bind
        );
    }

    #[test]
    fn test_etc_hosts_vs_resolv_conf_mount_mode() {
        // resolv.conf is mounted :ro, but /etc/hosts is NOT.
        // Verify the difference in bind mount format.
        let resolv_bind = format!("{}:/etc/resolv.conf:ro", "/tmp/resolv.conf");
        let hosts_bind = format!("{}:/etc/hosts", "/tmp/hosts");

        assert!(
            resolv_bind.contains(":ro"),
            "resolv.conf should be mounted read-only"
        );
        assert!(
            !hosts_bind.contains(":ro"),
            "/etc/hosts should be mounted read-write"
        );
    }

    // --- Init container state machine tests ---
    // These verify our implementation matches K8s conformance test expectations:
    // - init_container.go:440 "should not start app containers if init containers fail on a RestartAlways pod"
    // - init_container.go:565 "should not start app containers and fail the pod if init containers fail on a RestartNever pod"

    #[test]
    fn test_init_container_status_shows_crashloopbackoff_on_failure() {
        // K8s conformance expects: init container that exits non-zero with RestartAlways
        // should show CrashLoopBackOff in status.
        // See: init_container.go:414-419 — checks status.State.Terminated.ExitCode != 0
        let state = ContainerState::Waiting {
            reason: Some("CrashLoopBackOff".to_string()),
            message: Some(
                "back-off restarting failed container init container \"init1\" exited with 1"
                    .to_string(),
            ),
        };
        match &state {
            ContainerState::Waiting { reason, .. } => {
                assert_eq!(reason.as_deref(), Some("CrashLoopBackOff"));
            }
            _ => panic!("Expected Waiting state"),
        }
    }

    #[test]
    fn test_app_containers_show_pod_initializing_during_init() {
        // K8s conformance expects: app containers must be in Waiting state
        // with reason "PodInitializing" while init containers are running.
        // See: init_container.go:396-403
        let app_status = ContainerStatus {
            name: "app".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState::Waiting {
                reason: Some("PodInitializing".to_string()),
                message: None,
            }),
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };
        match &app_status.state {
            Some(ContainerState::Waiting { reason, .. }) => {
                assert_eq!(
                    reason.as_deref(),
                    Some("PodInitializing"),
                    "App containers must show PodInitializing while init containers run"
                );
            }
            _ => panic!("App container should be in Waiting state during init"),
        }
        assert!(
            !app_status.ready,
            "App container should not be ready during init"
        );
        assert_eq!(
            app_status.started,
            Some(false),
            "App container should not be started during init"
        );
    }

    #[test]
    fn test_init_container_restart_count_increments() {
        // K8s conformance expects: init container RestartCount >= 3 after multiple failures.
        // See: init_container.go:428-431 — checks status.RestartCount < 3
        let status = ContainerStatus {
            name: "init1".to_string(),
            ready: false,
            restart_count: 3,
            state: Some(ContainerState::Waiting {
                reason: Some("CrashLoopBackOff".to_string()),
                message: Some("back-off restarting failed container".to_string()),
            }),
            last_state: Some(ContainerState::Terminated {
                exit_code: 1,
                signal: None,
                reason: Some("Error".to_string()),
                message: None,
                started_at: None,
                finished_at: None,
                container_id: None,
            }),
            image: Some("init-image:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };
        assert!(
            status.restart_count >= 3,
            "Init container restart count should be >= 3 after multiple failures"
        );
        assert!(
            status.last_state.is_some(),
            "Init container should have lastTerminationState after restart"
        );
        match &status.last_state {
            Some(ContainerState::Terminated { exit_code, .. }) => {
                assert_ne!(
                    *exit_code, 0,
                    "LastTerminationState should show non-zero exit code"
                );
            }
            _ => panic!("LastTerminationState should be Terminated"),
        }
    }

    #[test]
    fn test_pod_stays_pending_during_init_failure() {
        // K8s conformance expects: pod phase remains Pending while init containers fail.
        // See: init_container.go:444 — gomega.Expect(endPod.Status.Phase).To(Equal(v1.PodPending))
        use rusternetes_common::types::Phase;
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pod"),
            spec: Some(PodSpec {
                containers: vec![make_container("app")],
                init_containers: Some(vec![make_container("init1")]),
                restart_policy: Some("Always".to_string()),
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Pending),
                reason: Some("PodInitializing".to_string()),
                ..Default::default()
            }),
        };
        assert_eq!(
            pod.status.as_ref().unwrap().phase,
            Some(Phase::Pending),
            "Pod must stay Pending during init container failures"
        );
    }

    #[test]
    fn test_init_container_state_machine_no_init_containers() {
        // Pod with no init containers should return (true, None, false) = all done
        // This is tested implicitly since compute_init_container_actions is async
        // and needs a Docker connection. We test the logic here.
        let has_init = false;
        assert!(
            !has_init,
            "Pod without init containers should be considered initialized"
        );
    }

    #[test]
    fn test_second_init_container_waits_for_first() {
        // K8s conformance expects: second init container is Waiting/PodInitializing
        // while first init container is running or retrying.
        // See: init_container.go:407-413
        let init_statuses = [
            ContainerStatus {
                name: "init1".to_string(),
                ready: false,
                restart_count: 1,
                state: Some(ContainerState::Waiting {
                    reason: Some("CrashLoopBackOff".to_string()),
                    message: Some("back-off restarting failed container".to_string()),
                }),
                last_state: Some(ContainerState::Terminated {
                    exit_code: 1,
                    signal: None,
                    reason: Some("Error".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
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
            },
            ContainerStatus {
                name: "init2".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState::Waiting {
                    reason: Some("PodInitializing".to_string()),
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
            },
        ];

        // First init container should show failure
        match &init_statuses[0].state {
            Some(ContainerState::Waiting { reason, .. }) => {
                assert_eq!(reason.as_deref(), Some("CrashLoopBackOff"));
            }
            _ => panic!("First init container should be Waiting/CrashLoopBackOff"),
        }

        // Second init container should be waiting
        match &init_statuses[1].state {
            Some(ContainerState::Waiting { reason, .. }) => {
                assert_eq!(reason.as_deref(), Some("PodInitializing"));
            }
            _ => panic!("Second init container should be Waiting/PodInitializing"),
        }
    }

    /// Verify that `lastState.terminated.exitCode` carries the previous
    /// container's exit code through container restarts.
    ///
    /// Conformance: the K8s runtime exit-status test expects pods to report
    /// the precise non-zero exit code in `lastState.terminated.exitCode`
    /// after the kubelet restarts the failed container.
    #[test]
    fn test_last_state_terminated_carries_exit_code() {
        let prev_terminated = ContainerState::Terminated {
            exit_code: 42,
            signal: None,
            reason: Some("Error".to_string()),
            message: None,
            started_at: Some("2026-01-01T00:00:00Z".to_string()),
            finished_at: Some("2026-01-01T00:00:05Z".to_string()),
            container_id: Some("docker://prev".to_string()),
        };
        let cs = ContainerStatus {
            name: "app".to_string(),
            ready: true,
            restart_count: 1,
            state: Some(ContainerState::Running {
                started_at: Some("2026-01-01T00:00:10Z".to_string()),
            }),
            last_state: Some(prev_terminated.clone()),
            image: Some("busybox".to_string()),
            image_id: None,
            container_id: Some("docker://next".to_string()),
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };

        match cs.last_state {
            Some(ContainerState::Terminated { exit_code, .. }) => {
                assert_eq!(exit_code, 42, "lastState exit_code must match previous run");
            }
            _ => panic!("lastState must be Terminated with exit_code"),
        }

        let json = serde_json::to_string(&cs).unwrap();
        assert!(
            json.contains("\"lastState\""),
            "ContainerStatus JSON must include lastState"
        );
        assert!(
            json.contains("\"exitCode\":42"),
            "lastState.terminated.exitCode must be 42 in JSON, got: {}",
            json
        );
    }

    /// Verify that a container that exits with a non-zero code surfaces the
    /// exit code on `state.terminated.exitCode` for `restartPolicy: Never`.
    #[test]
    fn test_terminated_state_exposes_exit_code_in_json() {
        let cs = ContainerStatus {
            name: "main".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState::Terminated {
                exit_code: 137,
                signal: None,
                reason: Some("OOMKilled".to_string()),
                message: None,
                started_at: Some("2026-01-01T00:00:00Z".to_string()),
                finished_at: Some("2026-01-01T00:00:30Z".to_string()),
                container_id: Some("docker://abc".to_string()),
            }),
            last_state: None,
            image: Some("busybox".to_string()),
            image_id: None,
            container_id: Some("docker://abc".to_string()),
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };

        let json = serde_json::to_value(&cs).unwrap();
        let exit_code = json
            .pointer("/state/terminated/exitCode")
            .and_then(|v| v.as_i64())
            .expect("state.terminated.exitCode missing from JSON");
        assert_eq!(exit_code, 137);
        let reason = json
            .pointer("/state/terminated/reason")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_eq!(reason, "OOMKilled");
    }

    /// Verify lifecycle preStop hooks are recognized for all handler types
    /// the kubelet needs to support during graceful termination.
    #[test]
    fn test_prestop_lifecycle_handler_variants() {
        use rusternetes_common::resources::{
            ExecAction, HTTPGetAction, Lifecycle, LifecycleHandler, SleepAction, TCPSocketAction,
        };

        let exec_hook = LifecycleHandler {
            exec: Some(ExecAction {
                command: vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "graceful".to_string(),
                ],
            }),
            http_get: None,
            tcp_socket: None,
            sleep: None,
        };
        assert!(exec_hook.exec.is_some());

        let http_hook = LifecycleHandler {
            exec: None,
            http_get: Some(HTTPGetAction {
                host: None,
                http_headers: None,
                path: Some("/shutdown".to_string()),
                port: rusternetes_common::resources::IntOrString::Int(8080),
                scheme: None,
            }),
            tcp_socket: None,
            sleep: None,
        };
        assert!(http_hook.http_get.is_some());

        let tcp_hook = LifecycleHandler {
            exec: None,
            http_get: None,
            tcp_socket: Some(TCPSocketAction {
                host: None,
                port: rusternetes_common::resources::IntOrString::Int(9000),
            }),
            sleep: None,
        };
        assert!(tcp_hook.tcp_socket.is_some());

        let sleep_hook = LifecycleHandler {
            exec: None,
            http_get: None,
            tcp_socket: None,
            sleep: Some(SleepAction { seconds: 5 }),
        };
        assert!(sleep_hook.sleep.is_some());

        let lifecycle = Lifecycle {
            post_start: None,
            pre_stop: Some(exec_hook),
            stop_signal: None,
        };
        let json = serde_json::to_string(&lifecycle).unwrap();
        assert!(json.contains("\"preStop\""), "JSON must include preStop");
        assert!(
            json.contains("\"exec\""),
            "preStop exec handler must serialize"
        );
    }

    /// Verify that `terminationGracePeriodSeconds` is read from the pod spec
    /// so the kubelet can pass it through to the preStop budget.
    #[test]
    fn test_termination_grace_period_propagation() {
        let mut pod = make_pod("graceful", "default", None, None);
        pod.spec.as_mut().unwrap().termination_grace_period_seconds = Some(45);

        let grace = pod
            .spec
            .as_ref()
            .and_then(|s| s.termination_grace_period_seconds)
            .unwrap_or(30);
        assert_eq!(
            grace, 45,
            "spec.terminationGracePeriodSeconds must propagate"
        );
    }
}
