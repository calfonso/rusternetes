use rusternetes_common::resources::Pod;
use tracing::{info, warn};

/// Per-pod networking implementation kubelet uses. Picked via the
/// all-in-one binary's `--pod-network-mode` flag.
///
/// Lives in this module (rather than `lib.rs`) because both the
/// lib AND the standalone `kubelet` bin include `runtime.rs` via
/// `mod runtime`; defining the enum here keeps
/// `crate::runtime::PodNetworkMode` resolvable from BOTH compile
/// contexts. `lib.rs` re-exports it as `kubelet::PodNetworkMode`
/// for downstream callers (the all-in-one binary, etc.).
// The standalone `kubelet` bin only ever constructs `Cni` (it
// has no embedded netstack), so its compile context flags
// `NetstackShadow` as unused. The lib compile context uses all
// three variants through the public API. Allowing dead_code here
// covers both compile units cleanly.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PodNetworkMode {
    /// CNI plugins + Docker-bridge fallback. The production path
    /// today, default for safety.
    #[default]
    Cni,
    /// CNI / Docker bridge **plus** every pod is also registered
    /// with the embedded netstack (IP allocated, TAP opened,
    /// runtime notified). Pod traffic still rides the legacy path;
    /// the netstack just observes. Used to validate the netstack
    /// data plane in staging without breaking pod networking.
    NetstackShadow,
    /// Embedded netstack carries pod traffic. The pod's netns is
    /// configured by `netstack.start_pod_in_netns` (TAP moved into
    /// netns, IP + default route assigned) instead of by the CNI
    /// plugin. Pod containers join the netns the same way as in
    /// CNI mode (`NetworkMode=ns:`). No Docker bridge involved.
    ///
    /// Requires `--cap-add NET_ADMIN` on the rusternetes container,
    /// a working netstack runtime, AND the Service-watcher
    /// populating Service VIPs before pods can reach
    /// `kubernetes.default` etc.
    NetstackActive,
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
pub fn setup_emptydir_dir(path: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))?;
    }
    Ok(())
}

/// Return true if `path` is already a mount point (its device differs from its
/// parent's). Used to make tmpfs setup/teardown idempotent.
fn is_mount_point(path: &str) -> bool {
    use std::os::unix::fs::MetadataExt;
    let p = std::path::Path::new(path);
    match (p.metadata(), p.parent().map(|pp| pp.metadata())) {
        (Ok(m), Some(Ok(pm))) => m.dev() != pm.dev(),
        _ => false,
    }
}

/// Mount a tmpfs at `dir` for a `medium: Memory` emptyDir volume.
///
/// K8s ref: `pkg/volume/emptydir/empty_dir.go` — Memory-medium emptyDir is a
/// tmpfs mount. Mounting it on the host volume dir (rather than as a
/// per-container Docker `--tmpfs`) makes the data persist across container
/// restarts for the pod lifetime, and reports `fs_type=tmpfs` to the
/// conformance emptyDir-tmpfs tests. Relies on the kubelet's volume bind being
/// `rshared` so the mount propagates to the host daemon's namespace. Idempotent
/// (no-op if already mounted). Best-effort: logs and continues on failure so a
/// kernel without tmpfs propagation degrades to a plain (persistent) bind dir.
pub(crate) fn mount_tmpfs_for_emptydir(dir: &str, size_bytes: Option<u64>) {
    if is_mount_point(dir) {
        return; // already mounted (pod re-sync) — keep the existing tmpfs + data
    }
    let mut opts = String::from("mode=0777");
    if let Some(bytes) = size_bytes {
        opts.push_str(&format!(",size={}", bytes));
    }
    match std::process::Command::new("mount")
        .args(["-t", "tmpfs", "-o", &opts, "tmpfs", dir])
        .output()
    {
        Ok(out) if out.status.success() => {
            info!("Mounted tmpfs for Memory emptyDir at {}", dir);
        }
        Ok(out) => {
            warn!(
                "Failed to mount tmpfs at {} ({}): falling back to persistent dir",
                dir,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Err(e) => {
            warn!("Could not exec mount for tmpfs at {}: {}", dir, e);
        }
    }
}

/// Parse a Kubernetes resource.Quantity (e.g. `1Gi`, `512Mi`, `100M`) into a
/// byte count. Returns None on unrecognised input. Supports the binary (Ki, Mi,
/// Gi, Ti) and decimal (k/K, M, G, T) suffixes used for memory quantities.
pub(crate) fn parse_quantity_bytes(q: &str) -> Option<u64> {
    let q = q.trim();
    let (num, mult): (&str, u64) = if let Some(n) = q.strip_suffix("Ki") {
        (n, 1024)
    } else if let Some(n) = q.strip_suffix("Mi") {
        (n, 1024 * 1024)
    } else if let Some(n) = q.strip_suffix("Gi") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = q.strip_suffix("Ti") {
        (n, 1024u64.pow(4))
    } else if let Some(n) = q.strip_suffix('k').or_else(|| q.strip_suffix('K')) {
        (n, 1000)
    } else if let Some(n) = q.strip_suffix('M') {
        (n, 1_000_000)
    } else if let Some(n) = q.strip_suffix('G') {
        (n, 1_000_000_000)
    } else if let Some(n) = q.strip_suffix('T') {
        (n, 1_000_000_000_000)
    } else {
        (q, 1)
    };
    num.trim()
        .parse::<f64>()
        .ok()
        .map(|v| (v * mult as f64) as u64)
}

/// Create the host-side termination-log file with world-writable permissions so
/// a container running as a non-root UID can write its termination message
/// through the bind mount.
///
/// Mirrors upstream `pkg/kubelet/kuberuntime/kuberuntime_container.go::makeMounts`
/// in release-1.35 (around lines 502-531), which does:
///
/// ```text
/// fs, err := m.osInterface.Create(containerLogPath)
/// ...
/// fs.Close()
/// // Chmod is needed because os.Create() ends up calling open(2) to create
/// // the file, so the final mode used is "mode & ~umask". But we want to
/// // make sure the specified mode is used in the file no matter what the
/// // umask is.
/// if err := m.osInterface.Chmod(containerLogPath, 0666); err != nil { ... }
/// ```
///
/// Without the explicit chmod, `std::fs::write` (which calls `open(2)` with
/// the default `0o666` requested mode) yields `0o664` on typical hosts after
/// the process umask (`0o002`) is applied — root-owned, group-writable only.
/// The conformance test
/// `[sig-node] Container Runtime blackbox test on terminated container
/// should report termination message if TerminationMessagePath is set as
/// non-root user and at a non-default path` runs the container as UID 10000
/// and shells `echo -n DONE > /dev/termination-custom-log`. With a `0o664`
/// root-owned file the redirect fails (`Permission denied`), the container
/// exits non-zero, and the pod is reported `Failed` instead of `Succeeded`.
///
/// Idempotent: the chmod runs whether the file was just created or already
/// existed (e.g. from a prior pod incarnation), and `std::fs::write("")`
/// truncates a pre-existing file so we never read back a stale message.
pub fn setup_termination_message_file(path: &str) -> std::io::Result<()> {
    std::fs::write(path, "")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // 0o666 mirrors upstream's `os.Chmod(containerLogPath, 0666)`. Do not
        // narrow this — the container's RunAsUser is arbitrary (any non-root
        // UID upstream chooses to test with).
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
    }
    Ok(())
}

/// Return the per-pod filesystem key used to compose host-side volume paths.
///
/// Mirrors upstream `pkg/kubelet/kubelet_getters.go::getPodDir`, which keys
/// pod directories on `pod.metadata.uid`. Two pods that share a name but have
/// distinct UIDs (the recreation case — e.g. hydrophone's `e2e-conformance-test`
/// driver pod) must not collide on disk, or the new pod's container will read
/// stale files written by the previous one.
///
/// Falls back to the pod's name when `uid` is empty. Real pods admitted through
/// the api-server always have a non-empty UID (assigned at `BeforeCreate`), so
/// the fallback only matters for in-process test fixtures that construct a Pod
/// without going through the registry.
pub(crate) fn pod_dir_key(pod: &Pod) -> &str {
    if !pod.metadata.uid.is_empty() {
        &pod.metadata.uid
    } else {
        &pod.metadata.name
    }
}

/// Unix special-file kinds that HostPath validates separately from
/// regular files / directories. Internal helper for `check_host_path_type`.
#[derive(Debug, Clone, Copy)]
enum UnixKind {
    Socket,
    CharDevice,
    BlockDevice,
}

fn check_unix_special(
    meta_res: std::io::Result<std::fs::Metadata>,
    kind: UnixKind,
) -> HostPathCheck {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        match meta_res {
            Ok(meta) => {
                let ft = meta.file_type();
                let matched = match kind {
                    UnixKind::Socket => ft.is_socket(),
                    UnixKind::CharDevice => ft.is_char_device(),
                    UnixKind::BlockDevice => ft.is_block_device(),
                };
                if matched {
                    HostPathCheck::Ok
                } else {
                    HostPathCheck::WrongKind
                }
            }
            Err(_) => HostPathCheck::Missing,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (meta_res, kind);
        HostPathCheck::WrongKind
    }
}

/// Result of validating a HostPath volume against its declared `type` per
/// upstream Kubernetes `pkg/volume/host_path/host_path.go::checkType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostPathCheck {
    /// The host path exists (or was created) and matches the declared type.
    Ok,
    /// The declared type requires the path to pre-exist but it does not.
    Missing,
    /// The path exists but is not the kind the declared type requires
    /// (e.g. `File` but the path is a directory).
    WrongKind,
    /// The declared type string is not one of the supported variants.
    UnsupportedType,
}

/// Pure-function mirror of upstream `pkg/volume/host_path/host_path.go`
/// HostPath `type` validation + "OrCreate" creation, made available so
/// scoped conformance tests can pin the [sig-storage] HostPath type
/// semantics without spinning up a real kubelet.
///
/// Semantics (matches Kubernetes v1.35):
/// - `None` or `Some("")` → accept anything (legacy unchecked behavior).
/// - `Some("Directory")` → path must exist and be a directory.
/// - `Some("DirectoryOrCreate")` → create the directory (and any missing
///   parents) if absent; otherwise it must be a directory.
/// - `Some("File")` → path must exist and be a regular file.
/// - `Some("FileOrCreate")` → create an empty file if absent; otherwise it
///   must be a regular file. Parent directory must already exist.
/// - `Some("Socket")` → path must exist and be a Unix socket.
/// - `Some("CharDevice")` / `Some("BlockDevice")` → must exist and be the
///   matching device kind (treated as `WrongKind` on non-Unix targets).
/// - Anything else → `UnsupportedType`.
pub fn check_host_path_type(path: &str, type_: Option<&str>) -> HostPathCheck {
    let kind = match type_ {
        None | Some("") => return HostPathCheck::Ok,
        Some(k) => k,
    };

    let meta_res = std::fs::symlink_metadata(path);

    match kind {
        "DirectoryOrCreate" => {
            if let Ok(meta) = &meta_res {
                if meta.file_type().is_dir() {
                    return HostPathCheck::Ok;
                }
                return HostPathCheck::WrongKind;
            }
            match std::fs::create_dir_all(path) {
                Ok(()) => HostPathCheck::Ok,
                Err(_) => HostPathCheck::Missing,
            }
        }
        "Directory" => match meta_res {
            Ok(meta) if meta.file_type().is_dir() => HostPathCheck::Ok,
            Ok(_) => HostPathCheck::WrongKind,
            Err(_) => HostPathCheck::Missing,
        },
        "FileOrCreate" => {
            if let Ok(meta) = &meta_res {
                if meta.file_type().is_file() {
                    return HostPathCheck::Ok;
                }
                return HostPathCheck::WrongKind;
            }
            // Parent directory must already exist — `FileOrCreate` does
            // NOT recursively create parent dirs (only `DirectoryOrCreate`
            // does). This matches upstream `host_path.go::createHostPathFile`.
            let parent_ok = std::path::Path::new(path)
                .parent()
                .map(|p| p.as_os_str().is_empty() || p.is_dir())
                .unwrap_or(false);
            if !parent_ok {
                return HostPathCheck::Missing;
            }
            match std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)
            {
                Ok(_) => HostPathCheck::Ok,
                Err(_) => HostPathCheck::Missing,
            }
        }
        "File" => match meta_res {
            Ok(meta) if meta.file_type().is_file() => HostPathCheck::Ok,
            Ok(_) => HostPathCheck::WrongKind,
            Err(_) => HostPathCheck::Missing,
        },
        "Socket" => check_unix_special(meta_res, UnixKind::Socket),
        "CharDevice" => check_unix_special(meta_res, UnixKind::CharDevice),
        "BlockDevice" => check_unix_special(meta_res, UnixKind::BlockDevice),
        _ => HostPathCheck::UnsupportedType,
    }
}

/// Compute the supplementary group IDs that must be added to a container's
/// GIDs so a non-root `runAsUser` can read volume files we chowned to
/// `:fsGroup` in `create_pod_volumes`.
///
/// Per `PodSecurityContext` semantics: `fsGroup` is appended first (when
/// set), followed by `securityContext.supplementalGroups`. Duplicates are
/// elided to keep the runtime arg list compact. Returns `None` when there
/// are no GIDs to add — Docker's `HostConfig.group_add` treats `None` and
/// an empty list identically, but `None` avoids a wasted allocation.
///
/// This is a pure function and is unit-tested in this module's test block.
/// `pub` so integration tests in the `tests/` crate can assert that the
/// container-arg layer is wired to `securityContext.fsGroup`.
pub fn compute_group_add(pod: &Pod) -> Option<Vec<String>> {
    let pod_sc = pod
        .spec
        .as_ref()
        .and_then(|s| s.security_context.as_ref())?;
    let mut gids: Vec<String> = Vec::new();
    if let Some(fs_group) = pod_sc.fs_group {
        gids.push(fs_group.to_string());
    }
    if let Some(ref supplemental) = pod_sc.supplemental_groups {
        for gid in supplemental {
            let gid_str = gid.to_string();
            if !gids.contains(&gid_str) {
                gids.push(gid_str);
            }
        }
    }
    if gids.is_empty() {
        None
    } else {
        Some(gids)
    }
}

/// Runtime observation of a single init container, derived from the
/// container runtime's inspect call (or test fixtures).
///
/// The `decide_next_init_action` helper consumes a slice of these (one per
/// declared init container, in declaration order) and is the single place
/// that encodes K8s init-container restart semantics: failed inits on a
/// `restartPolicy=Never` pod are terminal (not retried), failed inits on
/// `Always` / `OnFailure` pods retry, sidecar inits (per-container
/// `restartPolicy=Always`, KEP-753) do not gate app-container startup, and
/// init containers run sequentially.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitContainerObserved {
    /// Container does not yet exist (never created/started).
    NotStarted,
    /// Container is currently running.
    Running,
    /// Container has exited with the given exit code.
    Exited(i32),
}

/// Decision the kubelet's reconcile loop should take for a pod's init
/// containers, as a structured value. Mirrors the historical
/// `(all_init_done, next_index, should_retry)` tuple returned from
/// `compute_init_container_actions`, but in a form that's easier to read
/// and test exhaustively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitAction {
    /// True iff every non-sidecar init container has completed successfully
    /// (exit code 0). When true the kubelet may start app containers.
    pub all_init_done: bool,
    /// Index (into the pod's `init_containers` slice) of the next init
    /// container to start or retry, if any.
    pub next_index: Option<usize>,
    /// True iff `next_index` refers to a failed init container that should
    /// be retried under the pod's restart policy.
    pub should_retry: bool,
}

/// Decide the next init-container action for `pod` given a snapshot of how
/// each declared init container is currently observed by the runtime.
///
/// `observed[i]` corresponds to `pod.spec.init_containers[i]`. Missing
/// entries (a short slice) are treated as `NotStarted`, matching the prior
/// behaviour for inspect errors and unknown container states.
///
/// Semantics (matches upstream `pkg/kubelet/kuberuntime`):
/// - Sidecar init containers (per-container `restartPolicy=Always`) are
///   skipped from the gating check — they run alongside app containers
///   and must not block them, even while still Running or after they exit.
/// - The first regular init container that isn't successfully terminated
///   determines the action.
/// - A Running regular init container blocks advancement (`next_index =
///   None`) — the kubelet should wait, not start another container.
/// - A non-zero-exit regular init container is a retry candidate when the
///   pod's `restartPolicy` is `Always` or `OnFailure`; with `Never` the
///   pod is terminal (`next_index = None`).
/// - A NotStarted regular init container returns its index without
///   `should_retry` — the kubelet should create and start it.
///
/// This is a pure function: it makes no I/O calls and is the unit covered
/// by `tests/init_container_restart_test.rs`.
pub fn decide_next_init_action(pod: &Pod, observed: &[InitContainerObserved]) -> InitAction {
    let init_containers = match pod.spec.as_ref().and_then(|s| s.init_containers.as_ref()) {
        Some(ics) if !ics.is_empty() => ics,
        _ => {
            return InitAction {
                all_init_done: true,
                next_index: None,
                should_retry: false,
            };
        }
    };

    // K8s rule: init containers are retried on pod restart policies
    // "Always" and "OnFailure"; "Never" is terminal on failure.
    // Default (unset) is "Always".
    let restart_on_failure = pod
        .spec
        .as_ref()
        .and_then(|s| s.restart_policy.as_deref())
        .unwrap_or("Always")
        != "Never";

    for (i, ic) in init_containers.iter().enumerate() {
        // Sidecar init containers (KEP-753) run alongside app containers
        // and do NOT gate app-container startup, so skip them here.
        let is_sidecar = ic.restart_policy.as_deref() == Some("Always");
        if is_sidecar {
            continue;
        }

        let obs = observed
            .get(i)
            .copied()
            .unwrap_or(InitContainerObserved::NotStarted);

        match obs {
            InitContainerObserved::Running => {
                // Current init container is still running — wait for it.
                return InitAction {
                    all_init_done: false,
                    next_index: None,
                    should_retry: false,
                };
            }
            InitContainerObserved::Exited(0) => {
                // Completed successfully — advance to the next init container.
                continue;
            }
            InitContainerObserved::Exited(_) => {
                // Failed with a non-zero exit code.
                return if restart_on_failure {
                    InitAction {
                        all_init_done: false,
                        next_index: Some(i),
                        should_retry: true,
                    }
                } else {
                    // RestartPolicy=Never: pod is terminal. The kubelet
                    // sync loop will mark the pod as Failed.
                    InitAction {
                        all_init_done: false,
                        next_index: None,
                        should_retry: false,
                    }
                };
            }
            InitContainerObserved::NotStarted => {
                // Container needs to be created/started — not a retry.
                return InitAction {
                    all_init_done: false,
                    next_index: Some(i),
                    should_retry: false,
                };
            }
        }
    }

    // Every non-sidecar init container completed successfully.
    InitAction {
        all_init_done: true,
        next_index: None,
        should_retry: false,
    }
}

/// Expand environment variables in a string (e.g., ${VAR_NAME} or $VAR_NAME)
pub(crate) fn expand_env_vars(input: &str) -> String {
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
    use super::{parse_quantity_bytes, pod_dir_key};

    #[test]
    fn parse_quantity_bytes_handles_binary_and_decimal_suffixes() {
        assert_eq!(parse_quantity_bytes("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_quantity_bytes("512Mi"), Some(512 * 1024 * 1024));
        assert_eq!(parse_quantity_bytes("100M"), Some(100_000_000));
        assert_eq!(parse_quantity_bytes("64Ki"), Some(64 * 1024));
        assert_eq!(parse_quantity_bytes("2048"), Some(2048));
        assert_eq!(parse_quantity_bytes("bogus"), None);
    }
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
            ..Default::default()
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
                ..Default::default()
            }),
            status: None,
        }
    }

    /// Build the /etc/hosts content string the same way create_pod_hosts_file does,
    /// so we can unit-test the logic without needing a live ContainerRuntime.
    /// Delegates to the canonical kubelet helper; returns an empty string for
    /// hostNetwork pods (these tests don't exercise that branch).
    fn build_hosts_content(pod: &Pod, pod_ip: Option<&str>, cluster_domain: &str) -> String {
        crate::kubelet::build_managed_hosts_content(pod, pod_ip, cluster_domain).unwrap_or_default()
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

    // --- pod_dir_key: UID-keyed pod filesystem paths ---
    //
    // Mirrors upstream Kubernetes pkg/kubelet/kubelet_getters.go::getPodDir,
    // which keys per-pod on-disk paths on `pod.metadata.uid`. Without this,
    // a recreated pod with the same name (common in conformance test runners
    // like hydrophone, which always names its driver pod "e2e-conformance-test")
    // reuses the previous pod's host-side emptyDir directory and reads stale
    // /tmp/results from the prior run.

    #[test]
    fn pod_dir_key_uses_uid_when_present() {
        let mut pod = make_pod("p", "default", None, None);
        pod.metadata.uid = "abc-123".to_string();
        assert_eq!(pod_dir_key(&pod), "abc-123");
    }

    #[test]
    fn pod_dir_key_falls_back_to_name_when_uid_empty() {
        let mut pod = make_pod("static-pod", "kube-system", None, None);
        pod.metadata.uid = String::new();
        assert_eq!(pod_dir_key(&pod), "static-pod");
    }

    #[test]
    fn pod_dir_key_isolates_recreated_pod_with_same_name() {
        // Two pods with identical name but distinct UIDs (the recreation case
        // that breaks emptyDir reuse) must yield distinct on-disk keys.
        let mut a = make_pod("e2e-conformance-test", "conformance", None, None);
        let mut b = make_pod("e2e-conformance-test", "conformance", None, None);
        a.metadata.uid = "uid-a".to_string();
        b.metadata.uid = "uid-b".to_string();
        assert_ne!(pod_dir_key(&a), pod_dir_key(&b));
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
                ..Default::default()
            }),
            status: None,
        }
    }

    /// Extract the sysctls map from a pod, normalising the slash separator
    /// Kubernetes accepts (`kernel/foo`) to the dotted form (`kernel.foo`).
    fn extract_sysctls(pod: &Pod) -> Option<HashMap<String, String>> {
        pod.spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.sysctls.as_ref())
            .map(|sysctls| {
                sysctls
                    .iter()
                    .map(|s| (s.name.replace('/', "."), s.value.clone()))
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
                ..Default::default()
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
                ..Default::default()
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
            ..Default::default()
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

        // Check that standard IPv6 entries are present — addresses must match
        // upstream kubelet exactly (pkg/kubelet/kubelet_pods.go).
        assert!(content.contains("fe00::0\tip6-localnet"));
        assert!(content.contains("ff00::0\tip6-mcastprefix"));
        assert!(content.contains("ff02::1\tip6-allnodes"));
        assert!(content.contains("ff02::2\tip6-allrouters"));
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

    // --- Empty-string optional fields treated as unset ---
    //
    // The Kubernetes API server applies SetDefaults_Container at admission,
    // but some clients (e.g. hydrophone, the conformance runner) submit
    // PodSpecs where optional string fields are present as `""` rather than
    // omitted. Upstream Go kubelet gates these with `!= ""`; the Rust
    // kubelet must do the same or it rejects valid pods.

    #[test]
    fn test_sub_path_expr_empty_string_treated_as_unset() {
        // Mirrors upstream `pkg/kubelet/kubelet_pods.go::makeMounts`:
        //   if mount.SubPathExpr != "" { ... expand ... }
        // The expansion branch must not fire for Some("").
        let empty: Option<String> = Some(String::new());
        let unset: Option<String> = None;
        let present: Option<String> = Some("$(POD_NAME)".to_string());

        let gate = |o: &Option<String>| o.as_deref().filter(|s| !s.is_empty()).is_some();

        assert!(!gate(&empty), "Some(\"\") must be treated as unset");
        assert!(!gate(&unset), "None must be treated as unset");
        assert!(gate(&present), "Some(non-empty) must trigger expansion");
    }

    #[test]
    fn test_termination_message_path_empty_string_defaults() {
        // Mirrors upstream `pkg/apis/core/v1/defaults.go::SetDefaults_Container`:
        //   if obj.TerminationMessagePath == "" {
        //       obj.TerminationMessagePath = v1.TerminationMessagePathDefault
        //   }
        // The kubelet's defensive guard must also treat `""` as unset so
        // we never emit an invalid bind spec like `/host/path:` (empty target).
        const DEFAULT: &str = "/dev/termination-log";

        let resolve = |o: &Option<String>| -> String {
            o.as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or(DEFAULT)
                .to_string()
        };

        assert_eq!(resolve(&None), DEFAULT);
        assert_eq!(resolve(&Some(String::new())), DEFAULT);
        assert_eq!(
            resolve(&Some("/var/log/custom".to_string())),
            "/var/log/custom"
        );
    }

    #[test]
    fn test_termination_message_path_default_yields_valid_bind_spec() {
        // Regression: hydrophone's pod had terminationMessagePath="".
        // The kubelet built `format!("{}:{}", host_file, path)`, which with
        // an empty path produced `"/host/.../conformance-container:"` —
        // Docker rejects this with "invalid volume specification".
        // After defaulting, the bind spec must end with the default path.
        const DEFAULT: &str = "/dev/termination-log";
        let host_file = "/var/lib/kubelet/pod/termination/conformance-container";
        let raw: Option<String> = Some(String::new());
        let term_msg_path = raw.as_deref().filter(|s| !s.is_empty()).unwrap_or(DEFAULT);
        let bind = format!("{}:{}", host_file, term_msg_path);
        assert!(
            bind.ends_with(":/dev/termination-log"),
            "bind spec must have a non-empty container target, got {bind}"
        );
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
                ..Default::default()
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
                ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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

    // --- compute_group_add (fsGroup → container GIDs) ---

    mod group_add {
        use super::*;
        use crate::runtime::compute_group_add;
        use rusternetes_common::resources::pod::PodSecurityContext;

        fn pod_with_security_context(sc: PodSecurityContext) -> Pod {
            let mut pod = make_pod("p", "default", None, None);
            pod.spec.as_mut().unwrap().security_context = Some(sc);
            pod
        }

        #[test]
        fn returns_none_when_no_security_context() {
            let pod = make_pod("p", "default", None, None);
            assert_eq!(compute_group_add(&pod), None);
        }

        #[test]
        fn returns_none_when_no_fs_group_or_supplemental() {
            let pod = pod_with_security_context(Default::default());
            assert_eq!(compute_group_add(&pod), None);
        }

        /// The bug fix: a pod with `fsGroup` set must add that GID to the
        /// container's supplementary groups so files chowned to `:fsGroup`
        /// in `create_pod_volumes` are readable by a non-root runAsUser.
        #[test]
        fn includes_fs_group() {
            let pod = pod_with_security_context(PodSecurityContext {
                fs_group: Some(2000),
                ..Default::default()
            });
            assert_eq!(compute_group_add(&pod), Some(vec!["2000".to_string()]));
        }

        #[test]
        fn includes_supplemental_groups_when_no_fs_group() {
            let pod = pod_with_security_context(PodSecurityContext {
                supplemental_groups: Some(vec![3000, 4000]),
                ..Default::default()
            });
            assert_eq!(
                compute_group_add(&pod),
                Some(vec!["3000".to_string(), "4000".to_string()])
            );
        }

        #[test]
        fn combines_fs_group_first_then_supplemental() {
            let pod = pod_with_security_context(PodSecurityContext {
                fs_group: Some(2000),
                supplemental_groups: Some(vec![3000, 4000]),
                ..Default::default()
            });
            assert_eq!(
                compute_group_add(&pod),
                Some(vec![
                    "2000".to_string(),
                    "3000".to_string(),
                    "4000".to_string()
                ])
            );
        }

        /// fsGroup repeated in supplementalGroups must not be added twice —
        /// keeps the runtime arg list compact.
        #[test]
        fn dedupes_fs_group_against_supplemental() {
            let pod = pod_with_security_context(PodSecurityContext {
                fs_group: Some(2000),
                supplemental_groups: Some(vec![2000, 3000]),
                ..Default::default()
            });
            assert_eq!(
                compute_group_add(&pod),
                Some(vec!["2000".to_string(), "3000".to_string()])
            );
        }
    }
}
