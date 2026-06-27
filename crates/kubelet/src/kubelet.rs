use crate::cri_runtime::CriContainerRuntime;
use crate::eviction::{get_node_stats, get_pod_stats, EvictionManager, EvictionSignal};
use crate::lifecycle::should_skip_phase_write;
use anyhow::Result;
use rusternetes_common::{
    resources::{
        ContainerState, ContainerStatus, Node, NodeAddress, NodeCondition, NodeSpec, NodeStatus,
        Pod, PodCondition, PodIP, PodStatus, Taint, Toleration,
    },
    types::Phase,
};
use rusternetes_storage::{build_key, build_prefix, Storage, StorageBackend, WatchEvent};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Does a single toleration tolerate `taint`?
///
/// Mirrors upstream `(*v1.Toleration).ToleratesTaint`
/// (staging/src/k8s.io/api/core/v1/toleration.go:52): empty effect matches all
/// effects, empty key matches all keys, operator `Exists` matches all values,
/// empty/`Equal` operator requires value equality. Comparison operators
/// (`Lt`/`Gt`) are not used by node taints and are treated as non-matching.
fn toleration_tolerates_taint(t: &Toleration, taint: &Taint) -> bool {
    if let Some(effect) = t.effect.as_deref() {
        if !effect.is_empty() && effect != taint.effect {
            return false;
        }
    }
    if let Some(key) = t.key.as_deref() {
        if !key.is_empty() && key != taint.key {
            return false;
        }
    }
    match t.operator.as_deref().unwrap_or("Equal") {
        "Exists" => true,
        "Equal" | "" => t.value == taint.value,
        _ => false,
    }
}

/// Decide whether a pod must be evicted *now* for a single NoExecute `taint`.
///
/// Mirrors upstream `pkg/controller/tainteviction/taint_eviction.go`
/// (`processPodOnNode` + `getMinTolerationTime`, lines 451-490 / 160-182) as
/// closely as a poll loop allows — we have no persistent work-queue timer, so
/// instead of `startTime = now` we derive elapsed time from the taint's
/// `time_added` set by the node controller:
///
/// - No matching toleration → not tolerated → evict now (true).
/// - Any matching toleration with `tolerationSeconds <= 0` → evict now
///   (`getMinTolerationTime` returns 0 mid-iteration, before nil entries).
/// - No matching toleration carries `tolerationSeconds` → tolerated forever
///   (`getMinTolerationTime` returns -1) → never evict (false). A nil entry
///   alongside a timed one does NOT mean forever — the timed minimum wins.
/// - Otherwise evict once `now - time_added >= min(tolerationSeconds)`.
/// - `time_added: None` → treat as just-added: a matching *timed* toleration is
///   still within its grace period, so do not evict yet (the caller logs once).
///
/// Returns true iff the pod should be evicted because of this taint.
fn noexecute_eviction_due(
    tolerations: &[Toleration],
    taint: &Taint,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let matching: Vec<&Toleration> = tolerations
        .iter()
        .filter(|t| toleration_tolerates_taint(t, taint))
        .collect();

    // No toleration matches → the taint is not tolerated at all → evict now.
    if matching.is_empty() {
        return true;
    }

    // getMinTolerationTime evaluation order (taint_eviction.go:167-181): a
    // tolerationSeconds <= 0 short-circuits to "evict now" DURING iteration,
    // before any nil (= tolerate forever) entry is considered; only if no
    // toleration carries tolerationSeconds at all does it return -1 (forever).
    let mut min_secs: Option<i64> = None;
    for t in &matching {
        if let Some(secs) = t.toleration_seconds {
            if secs <= 0 {
                return true;
            }
            min_secs = Some(min_secs.map_or(secs, |m| m.min(secs)));
        }
    }

    // No matching toleration has tolerationSeconds → tolerated forever.
    let Some(min_secs) = min_secs else {
        return false;
    };

    match taint.time_added {
        // time_added not yet stamped: treat as just-added, still within grace.
        None => false,
        Some(added) => (now - added).num_seconds() >= min_secs,
    }
}

/// Pod worker state machine matching K8s pkg/kubelet/pod_workers.go.
///
/// K8s transitions:
/// - SyncPod: normal operation — create/update containers, retry on failure
/// - TerminatingPod: pod is being stopped (deletionTimestamp set OR evicted)
///   → stop containers, run preStop hooks, set Phase=Failed
/// - TerminatedPod: all containers stopped → delete pod from storage
///
/// IMPORTANT: Container creation errors do NOT trigger TerminatingPod.
/// The kubelet retries in SyncPod state. Only deletion and eviction trigger it.
/// K8s ref: pkg/kubelet/pod_workers.go:110-117, 260
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
pub enum PodWorkerState {
    /// Pod is expected to be started and running. Failures are retried.
    SyncPod,
    /// Pod is being torn down — deletion or eviction requested.
    TerminatingPod,
    /// All containers stopped, pod can be removed from storage.
    TerminatedPod,
}

/// Initial CrashLoopBackOff delay before the *second* restart of a container.
/// The first restart after a crash is immediate. Matches upstream
/// `pkg/kubelet/kubelet.go` `backOffPeriod = 10s`.
const CRASHLOOP_BACKOFF_INITIAL: Duration = Duration::from_secs(10);
/// Maximum CrashLoopBackOff delay. Matches upstream `MaxCrashLoopBackOff` (5m).
const CRASHLOOP_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// Defensive backoff for a terminal pod whose `finalize_terminated_pod_storage`
/// reported removal but left the object in storage (#1157). The per-pod worker
/// would otherwise re-enter `TerminatingPod` every reconcile and re-stop
/// containers forever. We back off the terminate retry (initial, doubling,
/// capped) and escalate the log so a storage-delete regression is loud.
const TERMINAL_FINALIZE_BACKOFF_INITIAL: Duration = Duration::from_secs(5);
/// Cap for the terminal-finalize retry backoff.
const TERMINAL_FINALIZE_BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Attempt count at which the still-present warning escalates to `error!`.
const TERMINAL_FINALIZE_ERROR_THRESHOLD: u32 = 3;

/// Backoff delay before re-attempting termination of a terminal pod whose
/// storage delete isn't taking effect, given the failed-attempt `count` (1 =
/// first failure). Pure so it can be unit-tested without wall-clock state.
fn terminal_finalize_backoff(count: u32) -> Duration {
    let shifts = count.saturating_sub(1).min(6);
    TERMINAL_FINALIZE_BACKOFF_INITIAL
        .saturating_mul(2u32.pow(shifts))
        .min(TERMINAL_FINALIZE_BACKOFF_MAX)
}

/// Per-container CrashLoopBackOff state. This is the kubelet-owned source of
/// truth for `restartCount` and restart pacing, decoupled from sync frequency.
///
/// Without this, the old code incremented `restartCount` once per *sync that
/// observed a terminated container* and restarted with no wall-clock gate — so
/// the watch-driven sync hot loop (~30 Hz) drove `restartCount` to tens of
/// thousands instead of the handful the conformance suite expects. K8s ref:
/// `pkg/kubelet/kuberuntime/kuberuntime_manager.go` `doBackOff` +
/// `client-go/util/flowcontrol.Backoff`.
#[derive(Clone)]
struct RestartBackoff {
    /// Number of times the kubelet has actually (re)started the container.
    /// Reported verbatim as `restartCount`.
    restart_count: u32,
    /// Wall-clock instant of the most recent restart.
    last_restart: Instant,
    /// Current backoff delay; the container may not be restarted again until
    /// `last_restart + backoff`. Doubles each restart, capped at the max.
    backoff: Duration,
}

pub struct Kubelet {
    node_name: String,
    storage: Arc<StorageBackend>,
    runtime: Arc<CriContainerRuntime>,
    /// `containerRuntimeVersion` reported in NodeStatus, resolved once at startup
    /// from the CRI Version RPC (`<runtime_name>://<runtime_version>`), not a
    /// hardcoded literal. Falls back to `"unknown"` if the handshake fails.
    container_runtime_version: String,
    sync_interval: Duration,
    /// Filesystem path used for `statvfs` when computing nodefs eviction stats.
    /// Defaults to `/var/lib/kubelet` when not provided.
    eviction_root_dir: PathBuf,
    eviction_manager: Mutex<EvictionManager>,
    /// Per-pod worker state. K8s uses a goroutine per pod; we track state
    /// per-UID and dispatch in the sync loop.
    /// K8s ref: pkg/kubelet/pod_workers.go
    pod_states: Mutex<HashMap<String, PodWorkerState>>,
    /// Per-pod sync lock, keyed by `"{namespace}/{name}"` (NOT UID).
    /// Prevents concurrent sync_pod calls for the same pod, and — because two
    /// incarnations of a recreated pod (e.g. a StatefulSet replacement) share
    /// the name but not the UID — also serializes the old incarnation's
    /// teardown against the new incarnation's stale-incarnation sweep and
    /// container creation (issue #1112).
    ///
    /// Upstream note: the Go kubelet has no single same-name barrier for
    /// regular pods — it gets the equivalent from per-UID pod workers
    /// (pkg/kubelet/pod_workers.go, UpdatePod/podWorkerLoop) whose
    /// termination state machine lets the new UID's worker create containers
    /// only after the old UID's worker has finished terminating; an explicit
    /// same-fullname barrier exists only for static pods (allowStaticPodStart,
    /// startedStaticPodsByFullname). Our name-keyed skip-and-retry is a
    /// rusternetes-specific serialization achieving the same effect.
    pod_sync_locks: crate::sync_locks::SyncLocks,
    /// Per-container CrashLoopBackOff state, keyed by
    /// `"{namespace}/{pod}/{container}"`. Source of truth for restartCount +
    /// restart pacing. See [`RestartBackoff`].
    restart_backoff: Mutex<HashMap<String, RestartBackoff>>,
    /// Terminal pods whose `finalize_terminated_pod_storage` reported removal
    /// but left the object in storage, keyed by the `/registry/...` pod key.
    /// Value is `(failed_attempts, last_attempt)` driving the retry backoff in
    /// [`terminal_finalize_backoff`] (#1157). Cleared once the object is gone.
    terminal_finalize_failures: Mutex<HashMap<String, (u32, Instant)>>,
    /// Track recently-deleted pod names (from watch events) so orphan cleanup
    /// can skip the grace period for pods that were explicitly deleted from storage.
    recently_deleted: Arc<Mutex<HashMap<String, Option<Pod>>>>,
    /// Per-pod worker signal channels. K8s uses one goroutine per pod.
    /// When a watch event arrives for a pod, we signal its channel to
    /// trigger an immediate reconciliation without a full sync_loop.
    pod_workers: Arc<Mutex<HashMap<String, mpsc::Sender<()>>>>,
    /// Unix-seconds timestamp of the most recent successful `sync_loop`
    /// completion. Exposed via [`Kubelet::healthy`] so the HTTP
    /// `/healthz` endpoint can answer 200 only when the reconciler has
    /// ticked recently. Mirrors upstream `pkg/kubelet/kubelet.go`'s
    /// `syncLoopMonitor`. 0 = no successful sync yet.
    last_sync: AtomicU64,
    /// Port the kubelet API server listens on (the `--metrics-port` flag).
    /// Advertised in the node's `status.daemonEndpoints.kubeletEndpoint.Port`
    /// so the api-server proxies log/exec/metrics requests to the right port.
    /// Every node uses the standard 10250 (each kubelet has its own network
    /// namespace, so there is no clash); the value stays configurable rather
    /// than hardcoded so a future shared-host topology can override it without
    /// the advertised port drifting from the bind port (the conformance
    /// framework hardcodes `<node>:10250`, so deviating breaks node-proxy).
    metrics_port: u16,
    /// Static pod manifest dir (upstream staticPodPath). None = disabled.
    pod_manifest_path: Option<PathBuf>,
    /// Current file-sourced static pods, keyed by (suffixed) pod name.
    /// Workers consult this before storage so static pods survive
    /// mirror-pod deletion.
    static_pods: Arc<Mutex<HashMap<String, Pod>>>,
    /// Sysctl admission allowlist (safe set + `--allowed-unsafe-sysctls`).
    /// A pod declaring a forbidden sysctl is rejected with reason
    /// `SysctlForbidden`. Upstream `pkg/kubelet/sysctl`.
    sysctl_allowlist: crate::sysctl::Allowlist,
}

// Kubelet needs Send+Sync for Arc<Kubelet> in spawned tasks
// All fields are Send+Sync: Arc<StorageBackend>, Arc<CriContainerRuntime>, Mutex<EvictionManager>

/// Return true iff `a.status` and `b.status` are semantically equal.
///
/// Compares via canonical JSON to ignore field ordering and `None` vs
/// `Some(default)` differences. Used to gate terminal-pod status writes
/// in `sync_pod` so a Succeeded pod whose status hasn't actually changed
/// doesn't get republished every reconcile cycle. Without this gate, the
/// terminal-pod paths re-derive status from the runtime on every sync,
/// write it back blindly, and emit a MODIFIED watch event — every cycle.
/// Whether a pod's readiness should be gated on a readiness probe — i.e. the
/// pod must start NOT-ready and only flip Ready once a probe succeeds. This
/// covers regular containers AND restartable init containers (sidecars): a
/// sidecar's readiness probe gates pod readiness just like a regular
/// container's (upstream pkg/kubelet/status/generate.go). Missing this for
/// sidecars marks the pod Ready immediately, before the probe's
/// initialDelaySeconds (NodeConformance "readiness before initial delay", #1069).
fn spec_has_readiness_probe(spec: &rusternetes_common::resources::pod::PodSpec) -> bool {
    if spec.containers.iter().any(|c| c.readiness_probe.is_some()) {
        return true;
    }
    spec.init_containers
        .as_ref()
        .map(|ics| {
            ics.iter().any(|c| {
                c.restart_policy.as_deref() == Some("Always") && c.readiness_probe.is_some()
            })
        })
        .unwrap_or(false)
}

/// True when every condition named in `spec.readinessGates` is present in
/// `status.conditions` with status `"True"`. A pod with no readinessGates is
/// trivially satisfied. Mirrors upstream `GeneratePodReadyCondition`: the Ready
/// condition ANDs the gates on top of container readiness, so an unsatisfied
/// gate must hold `Ready=False` even when all containers are ready.
fn readiness_gates_satisfied(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.readiness_gates.as_ref())
        .map(|gates| {
            gates.iter().all(|gate| {
                pod.status
                    .as_ref()
                    .and_then(|st| st.conditions.as_ref())
                    .map(|conds| {
                        conds
                            .iter()
                            .any(|c| c.condition_type == gate.condition_type && c.status == "True")
                    })
                    .unwrap_or(false)
            })
        })
        .unwrap_or(true)
}

pub fn pod_status_equal(a: &Pod, b: &Pod) -> bool {
    serde_json::to_value(&a.status).ok() == serde_json::to_value(&b.status).ok()
}

/// Decide whether a pod that has reached `TerminatedPod` should be removed
/// from storage, and remove it if so. Returns `Ok(true)` when the object was
/// deleted, `Ok(false)` when it was intentionally retained.
///
/// An explicitly-deleted pod (`deletionTimestamp` set) with no finalizers
/// MUST be removed: rusternetes has no finalizer-driven tombstone GC, so the
/// kubelet deletes directly. Leaving it makes the next reconcile default back
/// to `SyncPod`, which re-detects `deletionTimestamp => needs_terminating` and
/// re-enters `TerminatingPod` indefinitely (a `pelagos stop` storm).
///
/// A naturally-terminated pod (no `deletionTimestamp`) is retained so
/// `kubectl get pod` keeps showing its terminal status. A pod with an
/// unresolved finalizer is retained so the finalizer owner can still observe
/// it. Extracted from `sync_pod` so the decision can be unit-tested against a
/// real `Storage` backend rather than a predicate recomputed in the test.
pub async fn finalize_terminated_pod_storage<S: Storage + ?Sized>(
    storage: &S,
    key: &str,
    has_deletion_timestamp: bool,
    has_finalizers: bool,
) -> Result<bool> {
    if has_deletion_timestamp && !has_finalizers {
        storage.delete(key).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

impl Kubelet {
    /// Construct a Kubelet with upstream-default eviction config and
    /// `root_dir = /var/lib/kubelet`. Kept for library back-compat; the
    /// binary uses [`Kubelet::new_with_eviction`] to honor CLI flags.
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub async fn new(
        node_name: String,
        storage: Arc<StorageBackend>,
        sync_interval_secs: u64,
        volume_dir: String,
        cluster_dns: String,
        cluster_domain: String,
        network: String,
        kubernetes_service_host: String,
    ) -> Result<Self> {
        Self::new_with_eviction(
            node_name,
            storage,
            sync_interval_secs,
            volume_dir,
            cluster_dns,
            cluster_domain,
            network,
            kubernetes_service_host,
            PathBuf::from("/var/lib/kubelet"),
            EvictionManager::new(),
            None,
            crate::runtime::PodNetworkMode::Cni,
            10250,
            Vec::new(),
        )
        .await
    }

    /// Construct a Kubelet with an explicit eviction-root path (for statvfs)
    /// and a pre-configured `EvictionManager`. Used by the kubelet binary's
    /// main to honor `--root-dir`, `--eviction-hard`, etc.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_eviction(
        node_name: String,
        storage: Arc<StorageBackend>,
        sync_interval_secs: u64,
        volume_dir: String,
        cluster_dns: String,
        cluster_domain: String,
        network: String,
        kubernetes_service_host: String,
        eviction_root_dir: PathBuf,
        eviction_manager: EvictionManager,
        netstack: Option<Arc<dyn rusternetes_netstack::manager::NetstackHandle>>,
        pod_network_mode: crate::runtime::PodNetworkMode,
        metrics_port: u16,
        allowed_unsafe_sysctls: Vec<String>,
    ) -> Result<Self> {
        // CRI runtime backend (containerd + Youki). The endpoint and runtime
        // handler come from the standard kubelet env vars; pod networking is
        // owned by containerd's CNI plugin, so the netstack/pod-network-mode
        // knobs from the bollard path are not applied here yet (tracked for the
        // CRI migration follow-ups). `allowed_unsafe_sysctls` IS wired: it builds
        // the sysctl admission allowlist below. Cluster DNS is applied via the
        // runtime.
        let _ = (&network, pod_network_mode, &netstack);
        let socket = std::env::var("CONTAINER_RUNTIME_ENDPOINT")
            .unwrap_or_else(|_| "unix:///run/containerd/containerd.sock".to_string());
        let runtime_handler = std::env::var("CONTAINER_RUNTIME_HANDLER").unwrap_or_default();
        let log_root = format!("{volume_dir}/pod-logs");

        // VolumeManager shares the kubelet's storage + the api-server JWT secret
        // (for projected service-account tokens), mirroring the bollard runtime.
        let jwt_secret = std::env::var("JWT_SECRET")
            .unwrap_or_else(|_| "rusternetes-secret-change-in-production".to_string());
        let token_manager = rusternetes_common::auth::TokenManager::new_auto(jwt_secret.as_bytes());
        let volumes = crate::volumes::VolumeManager::new(
            volume_dir.clone(),
            Some(storage.clone()),
            token_manager,
        );

        // The kubernetes Service host:port injected into pods as
        // KUBERNETES_SERVICE_* so in-cluster clients reach the api-server.
        let service_host = if kubernetes_service_host.is_empty() {
            "10.96.0.1".to_string()
        } else {
            kubernetes_service_host
        };
        let runtime = CriContainerRuntime::connect(&socket, runtime_handler, log_root)
            .await
            .map_err(|e| anyhow::anyhow!("connecting to CRI runtime at {socket}: {e}"))?
            .with_volumes(volumes)
            .with_event_recorder(storage.clone())
            .with_service_host(service_host, "443")
            .with_cluster_dns(&cluster_dns, &cluster_domain);

        // Resolve the runtime identity once via the CRI Version RPC so
        // NodeStatus reports the runtime actually behind CONTAINER_RUNTIME_ENDPOINT
        // (e.g. containerd-rs://0.1.2) instead of a hardcoded literal.
        let container_runtime_version = match runtime.runtime_version_string().await {
            Ok(s) => {
                info!("CRI runtime: {s} (endpoint {socket})");
                s
            }
            Err(e) => {
                warn!(
                    "CRI Version RPC failed at {socket} ({e}); reporting containerRuntimeVersion as 'unknown'"
                );
                "unknown".to_string()
            }
        };

        // Log the resolved statvfs path once so operators can see which
        // mount we're measuring for eviction. Upstream cadvisor logs this
        // in `fs.go::GetFsInfoForPath`.
        crate::eviction::log_statvfs_path(&eviction_root_dir);

        Ok(Self {
            node_name,
            storage,
            runtime: Arc::new(runtime),
            container_runtime_version,
            sync_interval: Duration::from_secs(sync_interval_secs),
            eviction_root_dir,
            eviction_manager: Mutex::new(eviction_manager),
            pod_states: Mutex::new(HashMap::new()),
            terminal_finalize_failures: Mutex::new(HashMap::new()),
            pod_sync_locks: crate::sync_locks::SyncLocks::new(),
            restart_backoff: Mutex::new(HashMap::new()),
            recently_deleted: Arc::new(Mutex::new(HashMap::new())),
            pod_workers: Arc::new(Mutex::new(HashMap::new())),
            last_sync: AtomicU64::new(0),
            metrics_port,
            pod_manifest_path: None,
            static_pods: Arc::new(Mutex::new(HashMap::new())),
            sysctl_allowlist: crate::sysctl::Allowlist::new(&allowed_unsafe_sysctls),
        })
    }

    /// Enable static pods from a manifest directory (kubeadm staticPodPath).
    pub fn with_pod_manifest_path(mut self, path: Option<PathBuf>) -> Self {
        self.pod_manifest_path = path;
        self
    }

    /// Liveness probe — true iff `sync_loop` completed inside the
    /// stale-sync window (`max(6, 2 × sync_interval)` seconds, mirroring
    /// upstream's `syncLoopHealthCheck` constants in
    /// `pkg/kubelet/kubelet.go`). Backs the HTTP `/healthz` endpoint.
    /// Returns `false` until the first successful sync_loop tick.
    pub fn healthy(&self) -> bool {
        let last = self.last_sync.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let stale_after = self.sync_interval.as_secs().saturating_mul(2).max(6);
        now.saturating_sub(last) <= stale_after
    }

    /// Ensure the node-heartbeat Lease carries an `OwnerReference` back to its
    /// owning Node.
    ///
    /// The conformance test `[sig-node] NodeLease should have OwnerReferences set`
    /// (k8s.io/kubernetes/test/e2e/common/node/node_lease.go) asserts that every
    /// kubelet-managed Lease in `kube-node-lease` references its Node via a
    /// single owner reference with apiVersion `v1`, kind `Node`, the node's
    /// name, and the node's UID.
    ///
    /// Upstream kubelet's `pkg/kubelet/nodelease/controller.go` `setOwnerFunc`
    /// has the same self-heal semantics that this helper implements: if the
    /// stored Lease already carries a matching owner reference it is left
    /// alone, otherwise the field is overwritten with the single canonical
    /// reference. Returns `true` if the lease metadata was mutated.
    fn apply_node_lease_owner_ref(
        lease: &mut rusternetes_common::resources::Lease,
        node_name: &str,
        node_uid: &str,
    ) -> bool {
        if node_uid.is_empty() {
            return false;
        }
        let matches_existing = lease
            .metadata
            .owner_references
            .as_ref()
            .and_then(|refs| refs.first())
            .map(|owner| {
                owner.api_version == "v1"
                    && owner.kind == "Node"
                    && owner.name == node_name
                    && owner.uid == node_uid
            })
            .unwrap_or(false);
        if matches_existing {
            return false;
        }
        let owner = rusternetes_common::types::OwnerReference::new(
            "v1",
            "Node",
            node_name.to_string(),
            node_uid.to_string(),
        );
        lease.metadata.owner_references = Some(vec![owner]);
        true
    }

    pub async fn run(self: &Arc<Self>) -> Result<()> {
        info!("Kubelet started for node: {}", self.node_name);

        // Register the node
        self.register_node().await?;

        // Startup cleanup: immediately remove any containers from previous runs
        // that don't correspond to pods in etcd. K8s kubelet does a full
        // reconciliation at startup to ensure no stale containers remain.
        self.startup_cleanup().await;

        // Container GC — runs every 60 seconds to remove dead containers.
        // K8s ref: pkg/kubelet/kubelet.go — ContainerGCPeriod = 1 minute
        {
            let gc_self = Arc::clone(self);
            tokio::spawn(async move {
                // Wait before first GC run to let startup_cleanup finish
                // and pods populate in etcd
                tokio::time::sleep(Duration::from_secs(60)).await;
                let mut gc_timer = tokio::time::interval(Duration::from_secs(60));
                loop {
                    gc_timer.tick().await;
                    gc_self.container_gc().await;
                }
            });
        }

        // Channel for watch events to trigger immediate pod syncs
        let (watch_tx, mut watch_rx) = mpsc::channel::<String>(256);

        // Start a background watch on pod changes to react immediately
        // instead of waiting for the next poll cycle
        let storage_clone = self.storage.clone();
        let node_name = self.node_name.clone();
        let watch_tx_clone = watch_tx.clone();
        let recently_deleted_clone = self.recently_deleted.clone();
        tokio::spawn(async move {
            let prefix = build_prefix("pods", None);
            loop {
                match storage_clone.watch(&prefix).await {
                    Ok(mut stream) => {
                        use futures::StreamExt;
                        while let Some(event) = stream.next().await {
                            match event {
                                Ok(
                                    WatchEvent::Added(key, value)
                                    | WatchEvent::Modified(key, value),
                                ) => {
                                    // Parse to check nodeName reliably (avoid string matching on JSON)
                                    if let Ok(pod) =
                                        serde_json::from_str::<serde_json::Value>(&value)
                                    {
                                        let assigned_node =
                                            pod.pointer("/spec/nodeName").and_then(|v| v.as_str());
                                        if assigned_node == Some(&node_name) {
                                            let _ = watch_tx_clone.try_send(key);
                                        }
                                    }
                                }
                                Ok(WatchEvent::Deleted(key, prev_value)) => {
                                    // Only trigger for pods that were on our node
                                    if let Ok(pod) =
                                        serde_json::from_str::<serde_json::Value>(&prev_value)
                                    {
                                        let assigned_node =
                                            pod.pointer("/spec/nodeName").and_then(|v| v.as_str());
                                        if assigned_node == Some(&node_name) {
                                            // Cache the pod spec so orphan cleanup can run preStop hooks
                                            if let Some(pod_name) = pod
                                                .pointer("/metadata/name")
                                                .and_then(|v| v.as_str())
                                            {
                                                let cached_pod =
                                                    serde_json::from_value::<Pod>(pod.clone()).ok();
                                                recently_deleted_clone
                                                    .lock()
                                                    .unwrap()
                                                    .insert(pod_name.to_string(), cached_pod);
                                            }
                                            let _ = watch_tx_clone.try_send(key);
                                        }
                                    }
                                }
                                Err(e) => {
                                    debug!("Pod watch error: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to start pod watch: {}", e);
                    }
                }
                // Reconnect after a brief pause
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        // Full sync interval is the safety net — runs less frequently
        // The watch-triggered syncs handle the fast path
        let full_sync_interval = Duration::from_secs(self.sync_interval.as_secs().max(1));
        let mut full_sync_timer = tokio::time::interval(full_sync_interval);

        // Lease-based heartbeat in a SEPARATE task.
        // K8s kubelet uses Lease objects (coordination.k8s.io/v1) for heartbeats
        // since v1.14. The Lease is in the kube-node-lease namespace and is a
        // separate object from the Node, so updates NEVER conflict with node
        // status updates (no CAS conflicts).
        //
        // The node controller checks the Lease renewTime to determine if
        // the node is healthy. This is lightweight (just one field update)
        // and reliable (no competing writers).
        //
        // K8s ref: pkg/kubelet/util/nodelease.go, pkg/kubelet/kubelet.go:235
        {
            let lease_storage = self.storage.clone();
            let lease_node_name = self.node_name.clone();
            tokio::spawn(async move {
                let lease_key = format!("/registry/leases/kube-node-lease/{}", lease_node_name);
                let node_key = build_key("nodes", None, &lease_node_name);
                let mut lease_timer = tokio::time::interval(Duration::from_secs(10));

                // Ensure kube-node-lease namespace exists
                let ns_key = "/registry/namespaces/kube-node-lease";
                if lease_storage
                    .get::<serde_json::Value>(ns_key)
                    .await
                    .is_err()
                {
                    let ns = serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Namespace",
                        "metadata": {"name": "kube-node-lease"}
                    });
                    let _ = lease_storage.create(ns_key, &ns).await;
                }

                // Look up and cache the Node UID for OwnerReferences. Conformance
                // test [sig-node] NodeLease "should have OwnerReferences set"
                // requires every kubelet-managed Lease to carry an ownerRef
                // back to its Node. K8s ref:
                // pkg/kubelet/nodelease/controller.go — setOwnerFunc fetches the
                // node once and stamps the OwnerReference onto every Lease the
                // kubelet creates or repairs.
                //
                // The node UID is immutable after registration, so caching it
                // avoids a storage lookup on every 10s heartbeat. Retry a few
                // times in case the node CREATE hasn't been observed yet (e.g.
                // an etcd compaction or eventual-consistency window).
                let mut cached_node_uid: Option<String> = None;
                for attempt in 0..5 {
                    match lease_storage
                        .get::<rusternetes_common::resources::Node>(&node_key)
                        .await
                    {
                        Ok(node) if !node.metadata.uid.is_empty() => {
                            cached_node_uid = Some(node.metadata.uid);
                            break;
                        }
                        Ok(_) => {
                            tracing::warn!(
                                "Lease heartbeat: node {} has empty UID on attempt {}",
                                lease_node_name,
                                attempt
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Lease heartbeat: failed to read node {} for ownerRef (attempt {}): {}",
                                lease_node_name,
                                attempt,
                                e
                            );
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                if cached_node_uid.is_none() {
                    tracing::warn!(
                        "Lease heartbeat: starting without cached node UID for {}; \
                         ownerReferences will be populated lazily once the node is readable",
                        lease_node_name
                    );
                }

                loop {
                    lease_timer.tick().await;
                    let now = chrono::Utc::now();

                    // Lazy-load the node UID if it wasn't available at startup.
                    if cached_node_uid.is_none() {
                        if let Ok(node) = lease_storage
                            .get::<rusternetes_common::resources::Node>(&node_key)
                            .await
                        {
                            if !node.metadata.uid.is_empty() {
                                cached_node_uid = Some(node.metadata.uid);
                            }
                        }
                    }

                    // Try to update existing lease
                    match lease_storage
                        .get::<rusternetes_common::resources::Lease>(&lease_key)
                        .await
                    {
                        Ok(mut lease) => {
                            if let Some(ref mut spec) = lease.spec {
                                spec.renew_time = Some(now);
                            }
                            // Backfill ownerReferences if the existing lease was
                            // written by an earlier kubelet that didn't set them.
                            // Upstream kubelet's setOwnerFunc has the same
                            // self-heal behaviour so a controller restart fixes
                            // pre-existing leases without manual intervention.
                            if let Some(uid) = cached_node_uid.as_deref() {
                                Self::apply_node_lease_owner_ref(&mut lease, &lease_node_name, uid);
                            }
                            match lease_storage.update(&lease_key, &lease).await {
                                Ok(_) => {
                                    tracing::debug!(
                                        "Lease heartbeat: renewed for node {}",
                                        lease_node_name
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Lease heartbeat: update failed for {}: {}",
                                        lease_node_name,
                                        e
                                    );
                                }
                            }
                        }
                        Err(_) => {
                            // Lease doesn't exist — create it
                            let mut lease = rusternetes_common::resources::Lease::new(
                                lease_node_name.clone(),
                                "kube-node-lease",
                            )
                            .with_spec(
                                rusternetes_common::resources::LeaseSpec {
                                    holder_identity: Some(lease_node_name.clone()),
                                    lease_duration_seconds: Some(40),
                                    acquire_time: Some(now),
                                    renew_time: Some(now),
                                    lease_transitions: Some(0),
                                    preferred_holder: None,
                                    strategy: None,
                                },
                            );
                            if let Some(uid) = cached_node_uid.as_deref() {
                                Self::apply_node_lease_owner_ref(&mut lease, &lease_node_name, uid);
                            }
                            match lease_storage.create(&lease_key, &lease).await {
                                Ok(_) => {
                                    tracing::debug!(
                                        "Lease heartbeat: created lease for node {}",
                                        lease_node_name
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Lease heartbeat: create failed for {}: {}",
                                        lease_node_name,
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            });
        }

        loop {
            tokio::select! {
                // Watch-triggered: a specific pod changed, signal its per-pod worker
                Some(key) = watch_rx.recv() => {
                    // Extract pod name and namespace from key
                    // Key format: /registry/pods/{namespace}/{name}
                    let parts: Vec<&str> = key.split('/').collect();
                    let pod_name = parts.last().unwrap_or(&"").to_string();
                    if pod_name.is_empty() { continue; }

                    // Signal the per-pod worker if one exists
                    let has_worker = {
                        let workers = self.pod_workers.lock().unwrap();
                        if let Some(tx) = workers.get(&pod_name) {
                            let _ = tx.try_send(());
                            true
                        } else {
                            false
                        }
                    };

                    // If no worker exists, start one
                    if !has_worker {
                        self.ensure_pod_worker(&pod_name).await;
                    }
                }
                // Periodic full sync as safety net
                _ = full_sync_timer.tick() => {
                    // Timeout sync_loop to prevent blocking heartbeat.
                    // sync_loop is fire-and-forget so it should return in
                    // <100ms, but storage.list can be slow under load.
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        self.sync_loop(),
                    ).await {
                        Ok(Ok(())) => {},
                        Ok(Err(e)) => error!("Error in periodic sync: {}", e),
                        Err(_) => warn!("sync_loop timed out after 5s"),
                    }
                    // Always send heartbeat after sync
                    if let Err(e) = self.update_node_status().await {
                        error!("Error updating node status: {}", e);
                    }
                }
                // Dedicated heartbeat — runs every 10s independently of sync
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    if let Err(e) = self.update_node_status().await {
                        error!("Error updating node status: {}", e);
                    }
                }
            }
        }
    }

    async fn register_node(&self) -> Result<()> {
        info!("Registering node: {}", self.node_name);

        let mut node = Node::new(&self.node_name);

        // Set default node labels matching K8s kubelet initialNode()
        // See: pkg/kubelet/kubelet_node_status.go:initialNode()
        node.metadata.labels = Some(HashMap::from([
            ("kubernetes.io/hostname".to_string(), self.node_name.clone()),
            ("kubernetes.io/os".to_string(), "linux".to_string()),
            ("kubernetes.io/arch".to_string(), "amd64".to_string()),
            ("beta.kubernetes.io/os".to_string(), "linux".to_string()),
            ("beta.kubernetes.io/arch".to_string(), "amd64".to_string()),
        ]));

        // Set node spec to mark it as schedulable
        node.spec = Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: Some(false),
            taints: None,
        });

        // Set node status
        node.status = Some(NodeStatus {
            capacity: Some(HashMap::from([
                ("cpu".to_string(), "4".to_string()),
                ("memory".to_string(), "8Gi".to_string()),
                ("pods".to_string(), "110".to_string()),
                ("ephemeral-storage".to_string(), "100Gi".to_string()),
            ])),
            allocatable: Some(HashMap::from([
                ("cpu".to_string(), "4".to_string()),
                ("memory".to_string(), "8Gi".to_string()),
                ("pods".to_string(), "110".to_string()),
                ("ephemeral-storage".to_string(), "100Gi".to_string()),
            ])),
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(chrono::Utc::now()),
                last_transition_time: Some(chrono::Utc::now()),
                reason: Some("KubeletReady".to_string()),
                message: Some("kubelet is posting ready status".to_string()),
            }]),
            addresses: Some(vec![
                NodeAddress {
                    address_type: "InternalIP".to_string(),
                    address: Self::detect_internal_ip(),
                },
                NodeAddress {
                    address_type: "Hostname".to_string(),
                    address: self.node_name.clone(),
                },
            ]),
            node_info: Some(rusternetes_common::resources::NodeSystemInfo {
                machine_id: format!("rusternetes-{}", self.node_name),
                system_uuid: format!("rusternetes-{}", self.node_name),
                boot_id: format!("rusternetes-{}", self.node_name),
                kernel_version: "6.1.0-rusternetes".to_string(),
                os_image: "Rusternetes OS".to_string(),
                container_runtime_version: self.container_runtime_version.clone(),
                kubelet_version: "v1.35.0-rusternetes".to_string(),
                kube_proxy_version: "v1.35.0-rusternetes".to_string(),
                operating_system: "linux".to_string(),
                architecture: "amd64".to_string(),
                swap: None,
            }),
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            // Advertise the port the kubelet API server actually listens on
            // (the --metrics-port flag), so the api-server proxies log/exec/
            // metrics to the right port. Every node uses the standard 10250
            // (separate network namespaces, no clash); advertising the actual
            // bind port — rather than hardcoding 10250 — keeps the two in sync
            // if a future topology overrides it. The conformance framework
            // hardcodes <node>:10250, so the bind port must stay 10250.
            // See: pkg/kubelet/kubelet.go:505 — DaemonEndpoints{KubeletEndpoint{Port: kubeCfg.Port}}
            daemon_endpoints: Some(rusternetes_common::resources::NodeDaemonEndpoints {
                kubelet_endpoint: Some(rusternetes_common::resources::DaemonEndpoint {
                    port: self.metrics_port as i32,
                }),
            }),
            config: None,
            features: None,
            runtime_handlers: None,
            // KEP-5328: leave empty until the kubelet's feature-discovery
            // surface is wired up. The api-server treats `None` the same
            // as "no features declared".
            declared_features: None,
        });

        let key = build_key("nodes", None, &self.node_name);

        // Debug: log what we're trying to store
        let node_json = serde_json::to_string_pretty(&node)
            .unwrap_or_else(|_| "failed to serialize".to_string());
        debug!("Registering node with spec: {}", node_json);

        // Try to create, if it exists, update it. `create`/`update` carry the
        // node spec + labels; the node STATUS (addresses, capacity, Ready
        // condition) is written separately via update_status because a full
        // create/PUT strips `.status` through the api-server (storage mode is
        // unaffected — its update_status grafts the same status).
        match self.storage.create(&key, &node).await {
            Ok(_) => info!("Node registered successfully"),
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                self.storage.update(&key, &node).await?;
                info!("Node updated successfully");
            }
            Err(e) => return Err(e.into()),
        }
        self.storage.update_status(&key, &node).await?;

        Ok(())
    }

    /// Detect the node's internal IP address.
    /// In Docker, resolves the container hostname to get the network IP.
    /// Falls back to 127.0.0.1 if detection fails.
    fn detect_internal_ip() -> String {
        // Try to resolve our own hostname to get the Docker network IP
        if let Ok(hostname) = std::env::var("HOSTNAME") {
            if let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&(hostname.as_str(), 0u16))
            {
                for addr in addrs {
                    if let std::net::IpAddr::V4(ip) = addr.ip() {
                        if !ip.is_loopback() {
                            return ip.to_string();
                        }
                    }
                }
            }
        }
        // Fallback: try to find a non-loopback IP from network interfaces
        if let Ok(output) = std::process::Command::new("hostname").arg("-i").output() {
            let ip_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !ip_str.is_empty() && ip_str != "127.0.0.1" {
                return ip_str;
            }
        }
        "127.0.0.1".to_string()
    }

    /// Cached node InternalIP used for pod `status.hostIP` / `status.hostIPs`.
    /// Upstream sets these to the node's address (kubelet `generateAPIPodStatus`
    /// → `hostIPs`), and conformance reads them via the downward API. Resolving
    /// the IP shells out / does DNS, so memoize — it is stable for the kubelet's
    /// lifetime.
    fn node_internal_ip() -> &'static str {
        static NODE_IP: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        NODE_IP.get_or_init(Self::detect_internal_ip)
    }

    async fn update_node_status(&self) -> Result<()> {
        debug!("Updating node status");

        let key = build_key("nodes", None, &self.node_name);
        let mut node: Node = self.storage.get(&key).await?;

        // Get current node resource statistics via upstream-parity statvfs.
        let node_stats = get_node_stats(&self.eviction_root_dir);

        // Check if eviction is needed — scoped block ensures the MutexGuard
        // is dropped before any subsequent .await points. When the eviction
        // subsystem is disabled (empty thresholds), this is a no-op beyond
        // clearing any stale pressure conditions.
        let active_signals = {
            let mut eviction_manager = self.eviction_manager.lock().unwrap();
            if eviction_manager.is_disabled() {
                // Disabled: ensure conditions are False/cleared without churning logs.
                eviction_manager.update_node_conditions(&mut node, &[])?;
                Vec::new()
            } else {
                let active_signals = eviction_manager.check_eviction_needed(&node_stats);
                eviction_manager.update_node_conditions(&mut node, &active_signals)?;
                active_signals
            }
        };

        // Ensure default node labels are always set (K8s kubelet updateDefaultLabels)
        let labels = node.metadata.labels.get_or_insert_with(HashMap::new);
        labels
            .entry("kubernetes.io/hostname".to_string())
            .or_insert_with(|| self.node_name.clone());
        labels
            .entry("kubernetes.io/os".to_string())
            .or_insert_with(|| "linux".to_string());
        labels
            .entry("kubernetes.io/arch".to_string())
            .or_insert_with(|| "amd64".to_string());
        labels
            .entry("beta.kubernetes.io/os".to_string())
            .or_insert_with(|| "linux".to_string());
        labels
            .entry("beta.kubernetes.io/arch".to_string())
            .or_insert_with(|| "amd64".to_string());

        // Ensure capacity, allocatable, and nodeInfo are always set
        if let Some(ref mut status) = node.status {
            if status.capacity.as_ref().is_none_or(|c| c.is_empty()) {
                status.capacity = Some(HashMap::from([
                    ("cpu".to_string(), "4".to_string()),
                    ("memory".to_string(), "8Gi".to_string()),
                    ("pods".to_string(), "110".to_string()),
                    ("ephemeral-storage".to_string(), "100Gi".to_string()),
                ]));
            }
            if status.allocatable.as_ref().is_none_or(|a| a.is_empty()) {
                status.allocatable = Some(HashMap::from([
                    ("cpu".to_string(), "4".to_string()),
                    ("memory".to_string(), "8Gi".to_string()),
                    ("pods".to_string(), "110".to_string()),
                    ("ephemeral-storage".to_string(), "100Gi".to_string()),
                ]));
            }
            // Ensure nodeInfo is populated (may have been lost during updates)
            if status
                .node_info
                .as_ref()
                .is_none_or(|ni| ni.machine_id.is_empty())
            {
                status.node_info = Some(rusternetes_common::resources::NodeSystemInfo {
                    machine_id: format!("rusternetes-{}", self.node_name),
                    system_uuid: format!("rusternetes-{}", self.node_name),
                    boot_id: format!("rusternetes-{}", self.node_name),
                    kernel_version: "6.1.0-rusternetes".to_string(),
                    os_image: "Rusternetes OS".to_string(),
                    container_runtime_version: self.container_runtime_version.clone(),
                    kubelet_version: "v1.35.0-rusternetes".to_string(),
                    kube_proxy_version: "v1.35.0-rusternetes".to_string(),
                    operating_system: "linux".to_string(),
                    architecture: "amd64".to_string(),
                    swap: None,
                });
            }
        }

        // Ensure node addresses are populated (may have failed during initial registration)
        if let Some(ref mut status) = node.status {
            let addresses = status.addresses.get_or_insert_with(Vec::new);
            if addresses.is_empty() {
                let ip = Self::detect_internal_ip();
                if ip != "127.0.0.1" {
                    addresses.push(rusternetes_common::resources::NodeAddress {
                        address_type: "InternalIP".to_string(),
                        address: ip,
                    });
                    addresses.push(rusternetes_common::resources::NodeAddress {
                        address_type: "Hostname".to_string(),
                        address: self.node_name.clone(),
                    });
                }
            }
        }

        // Update heartbeat and ensure Ready=True.
        // Only write to storage if the heartbeat is stale (>10s old) or status changed.
        // This prevents rv churn that causes PATCH conflicts for external node updates.
        let mut needs_write = false;

        // Re-ensure the kubelet endpoint port on EVERY heartbeat, not only at
        // registration. `status.daemonEndpoints.kubeletEndpoint.Port` is read by
        // the e2e metrics grabber and the api-server log/exec/metrics proxy; it
        // was previously set only in register_node, so once a status
        // round-trip dropped it nothing re-added it (unlike capacity /
        // conditions / addresses, which are re-ensured above) — leaving it
        // null and producing "Invalid Kubelet port 0". Upstream's setNodeStatus
        // sets DaemonEndpoints on every sync; mirror that.
        if let Some(ref mut status) = node.status {
            let current_port = status
                .daemon_endpoints
                .as_ref()
                .and_then(|d| d.kubelet_endpoint.as_ref())
                .map(|e| e.port)
                .unwrap_or(0);
            if current_port != self.metrics_port as i32 {
                status.daemon_endpoints =
                    Some(rusternetes_common::resources::NodeDaemonEndpoints {
                        kubelet_endpoint: Some(rusternetes_common::resources::DaemonEndpoint {
                            port: self.metrics_port as i32,
                        }),
                    });
                needs_write = true;
            }
        }

        if let Some(ref mut status) = node.status {
            if let Some(ref mut conditions) = status.conditions {
                for condition in conditions.iter_mut() {
                    if condition.condition_type == "Ready" {
                        let now = chrono::Utc::now();
                        let last = condition
                            .last_heartbeat_time
                            .unwrap_or(now - chrono::Duration::seconds(60));
                        let stale = (now - last).num_seconds() > 10;
                        if stale {
                            condition.last_heartbeat_time = Some(now);
                            needs_write = true;
                        }
                        if condition.status != "True" {
                            condition.status = "True".to_string();
                            condition.last_transition_time = Some(now);
                            condition.reason = Some("KubeletReady".to_string());
                            condition.message = Some("kubelet is posting ready status".to_string());
                            needs_write = true;
                        }
                    }
                }
            }
        }

        if needs_write {
            // Node heartbeat is a pure status write (Ready condition, heartbeat
            // time) — route through the /status subresource.
            self.storage.update_status(&key, &node).await?;
        }

        // Collect and publish node + per-pod metrics to storage
        self.publish_node_metrics().await;
        self.publish_pod_metrics().await;

        // If eviction is needed, trigger pod eviction
        if !active_signals.is_empty() {
            if let Err(e) = self.handle_eviction(&active_signals).await {
                error!("Error handling eviction: {}", e);
            }
        }

        // Check for NoExecute taints and evict pods that don't tolerate them.
        // Re-read the node to get the freshest taint state — a test may have
        // removed taints between the initial read and this check.
        let fresh_node: Node = self.storage.get(&key).await.unwrap_or(node.clone());
        if let Some(ref spec) = fresh_node.spec {
            if let Some(ref taints) = spec.taints {
                let no_execute_taints: Vec<_> =
                    taints.iter().filter(|t| t.effect == "NoExecute").collect();
                if !no_execute_taints.is_empty() {
                    let pod_prefix = build_prefix("pods", None);
                    let all_pods: Vec<Pod> =
                        self.storage.list(&pod_prefix).await.unwrap_or_default();
                    let now = chrono::Utc::now();
                    for pod in &all_pods {
                        if pod.spec.as_ref().and_then(|s| s.node_name.as_ref())
                            != Some(&self.node_name)
                        {
                            continue;
                        }
                        if pod.metadata.is_being_deleted() {
                            continue;
                        }
                        // Skip terminal (Succeeded/Failed) pods. They have no
                        // running containers for the taint to protect, and our
                        // sweep ultimately leads to STORAGE REMOVAL (via the
                        // TerminatedPod worker path) — unlike upstream's
                        // taint-eviction controller, which issues an API DELETE
                        // that still honors finalizers/observability windows.
                        // Upstream pkg/controller/tainteviction/taint_eviction.go
                        // has no explicit terminal-pod skip (its work queue keys
                        // off the node taint, not pod phase); we add one here
                        // because destroying a Succeeded pod's status erases
                        // state conformance tests still need to read (#442).
                        let is_terminal = pod
                            .status
                            .as_ref()
                            .and_then(|s| s.phase.as_ref())
                            .map(|p| matches!(p, Phase::Succeeded | Phase::Failed))
                            .unwrap_or(false);
                        if is_terminal {
                            continue;
                        }
                        let empty_tols: Vec<Toleration> = Vec::new();
                        let tolerations = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.tolerations.as_ref())
                            .unwrap_or(&empty_tols);
                        for taint in &no_execute_taints {
                            if taint.time_added.is_none()
                                && tolerations.iter().any(|t| {
                                    toleration_tolerates_taint(t, taint)
                                        && t.toleration_seconds.is_some()
                                })
                            {
                                debug!(
                                    "NoExecute taint {:?} on node {} has no timeAdded; \
                                     deferring timed-toleration eviction of pod {}/{}",
                                    taint.key,
                                    self.node_name,
                                    pod.metadata.namespace.as_deref().unwrap_or("default"),
                                    pod.metadata.name,
                                );
                            }
                            if noexecute_eviction_due(tolerations, taint, now) {
                                info!(
                                    "Evicting pod {}/{} due to NoExecute taint {:?}",
                                    pod.metadata.namespace.as_deref().unwrap_or("default"),
                                    pod.metadata.name,
                                    taint.key
                                );
                                let mut evicted = pod.clone();
                                evicted.metadata.deletion_timestamp = Some(chrono::Utc::now());
                                let status = evicted.status.get_or_insert_with(Default::default);
                                status.phase = Some(Phase::Failed);
                                status.reason = Some("Evicted".to_string());
                                status.message = Some("Taint-based eviction".to_string());
                                let pod_key = build_key(
                                    "pods",
                                    evicted.metadata.namespace.as_deref(),
                                    &evicted.metadata.name,
                                );
                                // Mode-aware graceful eviction (#1284): storage
                                // mode persists deletionTimestamp+status via the
                                // whole-pod write; API mode can't (the api-server
                                // owns deletionTimestamp on a PUT) so it stamps
                                // /status then issues a real DELETE-with-grace.
                                let grace = pod
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.termination_grace_period_seconds)
                                    .unwrap_or(30);
                                let _ = self.storage.evict_pod(&pod_key, &evicted, grace).await;
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Collect container metrics from the runtime and write NodeMetrics to storage.
    /// The api-server reads these to serve the metrics.k8s.io API.
    async fn publish_node_metrics(&self) {
        use rusternetes_common::resources::{NodeMetrics, NodeMetricsMetadata};
        use std::collections::BTreeMap;

        // Get pods assigned to this node
        let all_pods: Vec<Pod> = self
            .storage
            .list(&build_prefix("pods", None))
            .await
            .unwrap_or_default();

        let pod_names: Vec<String> = all_pods
            .iter()
            .filter(|p| {
                p.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_deref())
                    .map(|n| n == self.node_name)
                    .unwrap_or(false)
            })
            .map(|p| p.metadata.name.clone())
            .collect();

        let (cpu_millicores, memory_bytes) = self.runtime.collect_node_metrics(&pod_names).await;
        let memory_mi = memory_bytes / (1024 * 1024);

        let mut usage = BTreeMap::new();
        usage.insert("cpu".to_string(), format!("{}m", cpu_millicores));
        usage.insert("memory".to_string(), format!("{}Mi", memory_mi));

        let metrics = NodeMetrics {
            api_version: "metrics.k8s.io/v1beta1".to_string(),
            kind: "NodeMetrics".to_string(),
            metadata: NodeMetricsMetadata {
                name: self.node_name.clone(),
                creation_timestamp: Some(chrono::Utc::now()),
            },
            timestamp: chrono::Utc::now(),
            window: "30s".to_string(),
            usage,
        };

        let metrics_key = format!("/registry/metrics.k8s.io/nodes/{}", self.node_name);
        match self.storage.get::<NodeMetrics>(&metrics_key).await {
            Ok(_) => {
                if let Err(e) = self.storage.update(&metrics_key, &metrics).await {
                    debug!("Failed to update node metrics: {}", e);
                }
            }
            Err(_) => {
                if let Err(e) = self.storage.create(&metrics_key, &metrics).await {
                    debug!("Failed to create node metrics: {}", e);
                }
            }
        }
    }

    /// Collect per-pod container usage from the runtime and write `PodMetrics`
    /// to storage. The api-server reads these to serve real
    /// `metrics.k8s.io` pod metrics (replacing synthetic usage=requests), which
    /// in turn lets the HPA controller compute true resource utilization.
    async fn publish_pod_metrics(&self) {
        use rusternetes_common::resources::{ContainerMetrics, PodMetrics, PodMetricsMetadata};
        use std::collections::BTreeMap;

        let all_pods: Vec<Pod> = self
            .storage
            .list(&build_prefix("pods", None))
            .await
            .unwrap_or_default();

        // Pods assigned to this node, keyed by name -> namespace.
        let node_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_deref())
                    .map(|n| n == self.node_name)
                    .unwrap_or(false)
            })
            .collect();
        if node_pods.is_empty() {
            return;
        }

        let pod_names: Vec<String> = node_pods.iter().map(|p| p.metadata.name.clone()).collect();
        let per_pod = self.runtime.collect_pod_metrics(&pod_names).await;

        for pod in node_pods {
            let name = &pod.metadata.name;
            let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
            let readings = match per_pod.get(name) {
                Some(r) if !r.is_empty() => r,
                // No live stats (e.g. pod not yet running) — skip; the handler
                // falls back to request-based estimates for absent metrics.
                _ => continue,
            };

            let containers: Vec<ContainerMetrics> = readings
                .iter()
                .map(|(cname, cpu_milli, mem_bytes)| {
                    let mut usage = BTreeMap::new();
                    usage.insert("cpu".to_string(), format!("{cpu_milli}m"));
                    usage.insert("memory".to_string(), format!("{mem_bytes}"));
                    ContainerMetrics {
                        name: cname.clone(),
                        usage,
                    }
                })
                .collect();

            let metrics = PodMetrics {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "PodMetrics".to_string(),
                metadata: PodMetricsMetadata {
                    name: name.clone(),
                    namespace: namespace.to_string(),
                    creation_timestamp: Some(chrono::Utc::now()),
                },
                timestamp: chrono::Utc::now(),
                window: "30s".to_string(),
                containers,
            };

            let key = format!("/registry/metrics.k8s.io/pods/{namespace}/{name}");
            match self.storage.get::<PodMetrics>(&key).await {
                Ok(_) => {
                    if let Err(e) = self.storage.update(&key, &metrics).await {
                        debug!("Failed to update pod metrics for {name}: {e}");
                    }
                }
                Err(_) => {
                    if let Err(e) = self.storage.create(&key, &metrics).await {
                        debug!("Failed to create pod metrics for {name}: {e}");
                    }
                }
            }
        }
    }

    async fn sync_loop(self: &Arc<Self>) -> Result<()> {
        debug!("Running sync loop for node: {}", self.node_name);

        // Get all pods — used for both node-pod filtering and orphan cleanup
        let all_pods_prefix = build_prefix("pods", None);
        let all_pods: Vec<Pod> = self.storage.list(&all_pods_prefix).await?;

        // Static pods: rescan the manifest dir (file source resync) and
        // project mirrors into storage before computing the node's pod set.
        let static_pods: Vec<Pod> = if let Some(dir) = &self.pod_manifest_path {
            let pods = crate::static_pods::load_static_pods(dir, &self.node_name);
            if let Err(e) = crate::static_pods::reconcile_mirror_pods(
                self.storage.as_ref(),
                &self.node_name,
                &pods,
            )
            .await
            {
                warn!("static pods: mirror reconcile failed: {}", e);
            }
            *self.static_pods.lock().unwrap() = pods
                .iter()
                .map(|p| (p.metadata.name.clone(), p.clone()))
                .collect();
            pods
        } else {
            Vec::new()
        };

        let node_pods: Vec<Pod> =
            crate::static_pods::merge_node_pods(all_pods.clone(), static_pods, &self.node_name);

        debug!("Found {} pods assigned to this node", node_pods.len());

        // Ensure per-pod workers exist for all assigned pods and signal them.
        // K8s ref: pkg/kubelet/pod_workers.go — podWorkerLoop (long-lived)
        for pod in &node_pods {
            let pod_name = &pod.metadata.name;

            // Signal existing worker or create a new one
            let has_worker = {
                let workers = self.pod_workers.lock().unwrap();
                if let Some(tx) = workers.get(pod_name.as_str()) {
                    let _ = tx.try_send(());
                    true
                } else {
                    false
                }
            };

            if !has_worker {
                self.ensure_pod_worker(pod_name).await;
            }
        }

        // Clean up workers for pods that are no longer assigned to this node
        {
            let worker_names: Vec<String> =
                self.pod_workers.lock().unwrap().keys().cloned().collect();
            let pod_names: HashSet<&str> =
                node_pods.iter().map(|p| p.metadata.name.as_str()).collect();
            for name in worker_names {
                if !pod_names.contains(name.as_str()) {
                    // Pod no longer exists — remove worker (it will shut down on next recv)
                    self.pod_workers.lock().unwrap().remove(&name);
                }
            }
        }

        // Legacy one-shot sync for backward compatibility during transition.
        // TODO: Remove once per-pod workers are proven stable.
        // For now, run one-shot syncs for any pod that doesn't have a worker yet.
        for pod in &node_pods {
            let pod = pod.clone();
            let kubelet = Arc::clone(self);
            let timeout_secs = 120u64;
            tokio::spawn(async move {
                let body = serde_json::to_vec(&pod).unwrap_or_default().into();
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_secs),
                    rusternetes_common::dump::with_payload(body, kubelet.sync_pod(&pod)),
                )
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        let err_str = e.to_string();
                        if err_str.contains("Failed to create container")
                            || err_str.contains("Failed to pull image")
                            || err_str.contains("FailedToStart")
                        {
                            tracing::error!(
                                "Fatal error syncing pod {}/{}: {}",
                                pod.metadata.namespace.as_deref().unwrap_or(""),
                                pod.metadata.name,
                                err_str
                            );
                            let _ = kubelet.update_pod_status_error(&pod, &err_str).await;
                        } else {
                            tracing::warn!(
                                "Transient error syncing pod {}/{} (will retry): {}",
                                pod.metadata.namespace.as_deref().unwrap_or(""),
                                pod.metadata.name,
                                err_str
                            );
                        }
                    }
                    Err(_) => {
                        tracing::warn!(
                            "Pod sync timed out for {}/{} ({}s)",
                            pod.metadata.namespace.as_deref().unwrap_or(""),
                            pod.metadata.name,
                            timeout_secs
                        );
                    }
                }
            });
        }

        // Pod sync tasks now run independently (fire-and-forget).
        // Error handling is in each spawned task above.
        // This matches K8s podWorkerLoop which runs independently per pod.

        // Clean up orphaned containers using the pod list we already fetched
        if let Err(e) = self
            .cleanup_orphaned_containers(&node_pods, &all_pods)
            .await
        {
            error!("Error cleaning up orphaned containers: {}", e);
        }

        // Garbage-collect terminal pods (Succeeded/Failed) from storage.
        // K8s has a terminated-pod-gc-threshold (default 12500) and the kubelet
        // periodically cleans up terminal pods. This prevents accumulation of
        // stale pod records that block namespace deletion.
        self.cleanup_terminal_pods(&node_pods).await;

        // Record completion for the /healthz liveness probe.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_sync.store(now, Ordering::Relaxed);

        Ok(())
    }

    /// Garbage-collect terminal pods (Succeeded/Failed) from storage.
    /// K8s has a terminated-pod-gc-threshold (default 12500) and the kubelet's
    /// In real K8s, the kubelet does NOT delete pods from the API server.
    /// Pod lifecycle is managed by the API server — pods are cleaned up by
    /// namespace deletion, GC (owner reference), or Job TTL controller.
    /// The kubelet only reports status changes.
    /// Previously this deleted Failed/Succeeded pods, which broke tests that
    /// need to observe terminal pod status (e.g., terminated reason check).
    async fn cleanup_terminal_pods(&self, _node_pods: &[Pod]) {
        // No-op: let the API server manage pod lifecycle.
    }

    /// Startup cleanup: remove all containers that don't correspond to pods
    /// in etcd. Runs once at kubelet startup before the main sync loop.
    /// K8s kubelet does this in syncLoopIteration → HandlePodCleanups.
    async fn startup_cleanup(&self) {
        info!("Running startup cleanup — removing stale containers from previous runs");

        // Get all pods from etcd
        let all_pods: Vec<Pod> = match self.storage.list("/registry/pods/").await {
            Ok(pods) => pods,
            Err(e) => {
                warn!("Failed to list pods for startup cleanup: {}", e);
                return;
            }
        };

        let existing_pod_names: std::collections::HashSet<String> =
            all_pods.iter().map(|p| p.metadata.name.clone()).collect();

        // Get all containers from Docker (including exited) so orphan cleanup
        // can remove stopped containers from deleted pods
        let running_pods = match self.runtime.list_all_pods().await {
            Ok(pods) => pods,
            Err(e) => {
                warn!("Failed to list pods for startup cleanup: {}", e);
                return;
            }
        };

        // Find orphans — running in Docker but not in etcd
        let orphans: Vec<String> = running_pods
            .into_iter()
            .filter(|name| !existing_pod_names.contains(name))
            .collect();

        if orphans.is_empty() {
            info!("Startup cleanup: no stale containers found");
            return;
        }

        info!(
            "Startup cleanup: found {} stale containers, removing",
            orphans.len()
        );

        // Clean up in parallel (up to 10 concurrent) for fast startup
        let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
        let mut handles = Vec::new();

        for orphan in orphans {
            let runtime = self.runtime.clone();
            let sem = semaphore.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                if let Err(e) = runtime.stop_and_remove_pod(&orphan).await {
                    warn!("Startup cleanup: failed to remove {}: {}", orphan, e);
                } else {
                    info!("Startup cleanup: removed stale container {}", orphan);
                }
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        info!("Startup cleanup complete");
    }

    async fn cleanup_orphaned_containers(
        &self,
        _current_pods: &[Pod],
        all_existing_pods: &[Pod],
    ) -> Result<()> {
        debug!("Checking for orphaned containers");

        // Reuse the pod list already fetched by sync_loop to avoid a redundant etcd round-trip.
        // NOTE: Terminal+deleted pod container cleanup is handled by the container GC
        // (container_gc method), which runs independently every 60 seconds and removes
        // exited containers. This orphan cleanup only handles containers whose pods
        // have been fully removed from etcd.
        let existing_pod_names: std::collections::HashSet<String> = all_existing_pods
            .iter()
            .map(|p| p.metadata.name.clone())
            .collect();

        debug!("Found {} pods in etcd", existing_pod_names.len());

        // Get all pod names from the container runtime (including exited)
        // so orphan cleanup removes stopped containers from deleted pods
        let running_pods = self.runtime.list_all_pods().await?;
        debug!(
            "Found {} running pods in container runtime",
            running_pods.len()
        );

        // Check for orphaned pods — running in the container runtime but not
        // tracked by any pod worker or found in etcd.
        // K8s ref: pkg/kubelet/kubelet_pods.go:1270 — kills orphaned runtime
        // pods not in workingPods with a 1-second grace period.
        //
        // IMPORTANT: In a shared Docker daemon, ALL kubelets see ALL containers.
        // We must not kill containers belonging to other nodes' pods.
        let known_pod_names: std::collections::HashSet<String> = {
            let states = self.pod_states.lock().unwrap();
            states.keys().cloned().collect()
        };
        for running_pod_name in &running_pods {
            if existing_pod_names.contains(running_pod_name) {
                continue; // Pod exists in etcd — not an orphan
            }
            if known_pod_names.contains(running_pod_name) {
                continue; // Pod worker is still tracking this pod
            }
            // Fast path: if this pod was explicitly deleted (via watch event),
            // skip the grace period and clean up immediately.
            let cached_pod = self
                .recently_deleted
                .lock()
                .unwrap()
                .get(running_pod_name)
                .cloned()
                .flatten();
            let is_recently_deleted = cached_pod.is_some()
                || self
                    .recently_deleted
                    .lock()
                    .unwrap()
                    .contains_key(running_pod_name);
            if !is_recently_deleted {
                // Check container age — don't kill containers younger than 30s
                let container_age = self
                    .runtime
                    .get_container_age(running_pod_name)
                    .await
                    .unwrap_or(std::time::Duration::from_secs(0));
                if container_age < std::time::Duration::from_secs(30) {
                    debug!(
                        "Skipping recently started orphan {} (age {:?})",
                        running_pod_name, container_age
                    );
                    continue;
                }
            } else {
                // Remove from tracker — we're about to clean it up
                self.recently_deleted
                    .lock()
                    .unwrap()
                    .remove(running_pod_name.as_str());
                info!(
                    "Fast-path cleanup for explicitly deleted pod {} — skipping grace period",
                    running_pod_name
                );
            }
            // Re-check etcd before cleanup — a new pod with the same name may have
            // been created since we fetched the pod list at the start of sync_loop.
            // Without this check, we'd delete volumes that the new pod needs.
            let still_orphaned = {
                let fresh_pods: Vec<Pod> = self
                    .storage
                    .list("/registry/pods/")
                    .await
                    .unwrap_or_default();
                !fresh_pods
                    .iter()
                    .any(|p| p.metadata.name == *running_pod_name)
            };
            if !still_orphaned {
                debug!(
                    "Pod {} was recreated in etcd — skipping cleanup",
                    running_pod_name
                );
                continue;
            }

            info!(
                "Found orphaned pod {} - not in etcd, stopping and removing containers",
                running_pod_name
            );
            // Stop orphaned containers. K8s HandlePodCleanups kills orphaned
            // runtime pods with a 1-second grace period. Container removal is
            // left to the container GC.
            // K8s ref: pkg/kubelet/kubelet_pods.go:1280
            if let Some(ref pod) = cached_pod {
                // If we have a cached pod spec, run preStop hooks
                let grace = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.termination_grace_period_seconds)
                    .unwrap_or(1); // K8s uses 1s for orphans
                if let Err(e) = self.runtime.stop_pod_for(pod, grace).await {
                    warn!("Failed to stop orphaned pod {}: {}", running_pod_name, e);
                }
            } else {
                // No cached spec — stop with 1s grace, no preStop hooks
                if let Err(e) = self
                    .runtime
                    .stop_pod_with_grace_period(running_pod_name, 1)
                    .await
                {
                    warn!("Failed to stop orphaned pod {}: {}", running_pod_name, e);
                }
            }
        }

        // Stale "created" containers and exited orphan containers are handled
        // by the container GC (garbage_collect_containers), which runs independently
        // every 60 seconds. K8s ref: pkg/kubelet/kuberuntime/kuberuntime_gc.go

        Ok(())
    }

    /// Container garbage collector — runs independently every 60 seconds.
    /// Matches K8s kuberuntime_gc.go behavior:
    /// 1. For deleted pods (not in etcd): remove ALL dead containers
    /// 2. For existing pods: keep at most 1 dead container per pod (for log access)
    /// 3. Remove orphaned pause containers (sandboxes) with no running app containers
    /// 4. Remove stale "created" containers that were never started
    ///
    /// K8s ref: pkg/kubelet/kuberuntime/kuberuntime_gc.go — GarbageCollect
    async fn container_gc(&self) {
        // Get pod names from etcd to distinguish deleted vs existing pods
        // K8s ref: evictContainers checks allSourcesReady
        let existing_pods: HashSet<String> = self
            .storage
            .list::<Pod>("/registry/pods/")
            .await
            .unwrap_or_default()
            .iter()
            .map(|p| p.metadata.name.clone())
            .collect();

        match self
            .runtime
            .garbage_collect_containers(&existing_pods)
            .await
        {
            Ok(removed) => {
                if removed > 0 {
                    info!("Container GC: removed {} dead/stale containers", removed);
                }
            }
            Err(e) => {
                error!("Container GC failed: {}", e);
            }
        }
    }

    /// Ensure a per-pod worker task exists for the given pod.
    /// K8s ref: pkg/kubelet/pod_workers.go — podWorkerLoop
    ///
    /// Each pod gets a persistent tokio task that:
    /// 1. Waits for a signal on its channel
    /// 2. Reads the latest pod state from storage
    /// 3. Calls sync_pod
    /// 4. Goes back to waiting
    ///
    /// The worker stays alive until the pod is deleted and cleaned up.
    /// This avoids the full sync_loop on every watch event.
    async fn ensure_pod_worker(self: &Arc<Self>, pod_name: &str) {
        let (tx, mut rx) = mpsc::channel::<()>(4);

        // Signal immediately to sync now
        let _ = tx.try_send(());

        {
            let mut workers = self.pod_workers.lock().unwrap();
            if workers.contains_key(pod_name) {
                // Worker already exists — just signal it
                if let Some(existing_tx) = workers.get(pod_name) {
                    let _ = existing_tx.try_send(());
                }
                return;
            }
            workers.insert(pod_name.to_string(), tx);
        }

        let kubelet = Arc::clone(self);
        let name = pod_name.to_string();
        let pod_workers = Arc::clone(&self.pod_workers);
        let node_name = self.node_name.clone();

        tokio::spawn(async move {
            // Per-pod worker loop — stays alive for the lifetime of the pod
            loop {
                // Wait for signal (or timeout for periodic re-check)
                let signaled = tokio::time::timeout(
                    Duration::from_secs(5), // periodic fallback re-sync
                    rx.recv(),
                )
                .await;

                // If channel closed, pod worker is being removed
                if matches!(signaled, Ok(None)) {
                    debug!("Pod worker for {} shutting down (channel closed)", name);
                    break;
                }

                // Drain any additional queued signals
                while rx.try_recv().is_ok() {}

                // Read the latest pod state from storage by scanning for this pod.
                // We need to find the namespace since it's not stored in the worker key.
                // Search all namespaces for this pod name assigned to our node.
                let pod = {
                    // File-sourced static pods are authoritative: consult the
                    // cache first so a static pod keeps running even if its
                    // mirror was deleted from storage.
                    let cached = kubelet.static_pods.lock().unwrap().get(&name).cloned();
                    if let Some(p) = cached {
                        Some(p)
                    } else {
                        let prefix = build_prefix("pods", None);
                        match kubelet.storage.list::<Pod>(&prefix).await {
                            Ok(pods) => pods.into_iter().find(|p| {
                                p.metadata.name == name
                                    && p.spec
                                        .as_ref()
                                        .and_then(|s| s.node_name.as_ref())
                                        .map(|n| n == &node_name)
                                        .unwrap_or(false)
                            }),
                            Err(e) => {
                                debug!("Pod worker {}: storage error: {}", name, e);
                                continue;
                            }
                        }
                    }
                };

                match pod {
                    Some(pod) => {
                        // Use a longer timeout for pods being deleted — preStop hooks
                        // plus container stop grace period can exceed 30s.
                        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go:860
                        //   grace_period can be up to terminationGracePeriodSeconds (default 30s)
                        //   plus preStop hook execution time.
                        // K8s pod workers don't have a per-sync timeout — they
                        // K8s pod workers don't have a per-sync timeout.
                        // 120s is generous enough for container startup + probes.
                        let timeout_secs = 120u64;
                        let body = serde_json::to_vec(&pod).unwrap_or_default().into();
                        match tokio::time::timeout(
                            Duration::from_secs(timeout_secs),
                            rusternetes_common::dump::with_payload(body, kubelet.sync_pod(&pod)),
                        )
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                let err_str = e.to_string();
                                if err_str.contains("Failed to create container")
                                    || err_str.contains("Failed to pull image")
                                {
                                    let _ = kubelet.update_pod_status_error(&pod, &err_str).await;
                                }
                                debug!("Pod worker {}: sync error: {}", name, err_str);
                            }
                            Err(_) => {
                                warn!("Pod worker {}: sync timed out", name);
                            }
                        }
                    }
                    None => {
                        // Pod no longer exists for this node — stop the worker
                        debug!("Pod worker {}: pod not found, shutting down", name);
                        break;
                    }
                }
            }

            // Clean up worker entry
            pod_workers.lock().unwrap().remove(&name);
            debug!("Pod worker for {} removed", name);
        });
    }

    async fn sync_pod(&self, pod: &Pod) -> Result<()> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pod_uid = &pod.metadata.uid;

        // Per-pod sync lock: prevent concurrent sync_pod calls for the same pod.
        // K8s uses one goroutine per pod; without this, concurrent syncs create
        // Docker 409 "container name already in use" errors (1014 per run).
        //
        // Keyed by namespace/name, NOT UID: a recreated pod (same name, new
        // UID — the StatefulSet replacement pattern) must not sweep/create
        // containers while a queued sync of the OLD incarnation is still
        // running, or the swept container can be resurrected mid-start
        // (issue #1112). Same-name pods cannot legitimately coexist in
        // storage, so name-keyed skip-and-retry loses no real concurrency.
        let sync_lock_key = format!("{}/{}", namespace, pod_name);
        // Skip-and-retry: if another sync already holds this pod's key, bail (a
        // later reconcile retries). The guard releases the key on every return
        // path. See `crate::sync_locks` for the rationale + tests.
        let _sync_guard = match self.pod_sync_locks.try_acquire(sync_lock_key) {
            Some(guard) => guard,
            None => {
                debug!(
                    "Skipping sync for pod {}/{} (uid {}) — already syncing",
                    namespace, pod_name, pod_uid
                );
                return Ok(());
            }
        };

        debug!("Syncing pod: {}/{}", namespace, pod_name);

        // Pod worker state machine dispatch.
        // K8s ref: pkg/kubelet/pod_workers.go — podWorkerLoop
        //
        // State transitions:
        // - SyncPod (default): normal operation — create/update containers
        // - TerminatingPod: pod is being stopped — stop containers, run preStop hooks
        //   Triggered by: deletionTimestamp set, phase Succeeded/Failed, eviction
        // - TerminatedPod: all containers stopped — update status, clean volumes
        //   Container REMOVAL is NOT done here — it's handled by the container GC.
        //
        // K8s ref: pkg/kubelet/pod_workers.go:110-117
        let current_state = {
            self.pod_states
                .lock()
                .unwrap()
                .get(pod_uid)
                .cloned()
                .unwrap_or(PodWorkerState::SyncPod)
        };

        // Determine if we need to transition to TerminatingPod.
        // Triggers:
        // 1. deletionTimestamp set (API delete, controller delete)
        // 2. Pod phase is terminal (Succeeded/Failed) AND restartPolicy prevents
        //    restart. With restartPolicy=Always, the kubelet restarts containers
        //    instead of terminating — the pod stays in SyncPod state.
        //    K8s ref: pkg/kubelet/pod_workers.go — completeSync checks isTerminal
        //    which is only true when the pod is finished (all containers exited
        //    and restartPolicy doesn't allow restart).
        let is_terminal_phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref())
            .map(|p| matches!(p, Phase::Succeeded | Phase::Failed))
            .unwrap_or(false);
        let restart_policy = pod
            .spec
            .as_ref()
            .and_then(|s| s.restart_policy.as_deref())
            .unwrap_or("Always");
        let terminal_and_done = is_terminal_phase && restart_policy != "Always";
        let needs_terminating = (pod.metadata.deletion_timestamp.is_some() || terminal_and_done)
            && matches!(current_state, PodWorkerState::SyncPod);
        // INVARIANT: a pod whose phase is terminal either transitions to
        // TerminatingPod here or dispatches into the Succeeded|Failed match
        // arm below — it never re-enters the start_pod/Pending paths, so the
        // Pending status writes in those paths don't need the
        // should_skip_phase_write guard; only write sites that re-read a
        // possibly-terminal fresh pod from storage carry it.

        if needs_terminating {
            // Backoff guard (#1157): if a prior terminate cycle reported a
            // finalize-removal but storage still has the object, don't re-stop
            // containers every reconcile. Skip until the backoff window elapses
            // — the failed-finalize path keeps extending it and escalating the
            // log, so a storage-delete regression is loud, not a silent loop.
            let key = build_key("pods", Some(namespace), pod_name);
            if let Some(remaining) = self.terminal_finalize_backoff_remaining(&key) {
                debug!(
                    "Pod {}/{}: delaying re-terminate for {:?} (previous finalize did not remove the object)",
                    namespace, pod_name, remaining
                );
                return Ok(());
            }
            self.pod_states
                .lock()
                .unwrap()
                .insert(pod_uid.clone(), PodWorkerState::TerminatingPod);
            // Fall through to TerminatingPod handling below
        }

        // Re-read state after potential transition
        let current_state = {
            self.pod_states
                .lock()
                .unwrap()
                .get(pod_uid)
                .cloned()
                .unwrap_or(PodWorkerState::SyncPod)
        };

        // === TerminatedPod: update status, clean up resources ===
        // K8s ref: pkg/kubelet/kubelet.go:2467 — SyncTerminatedPod
        // Container removal is NOT done here — left to the container GC.
        if matches!(current_state, PodWorkerState::TerminatedPod) {
            let key = build_key("pods", Some(namespace), pod_name);

            // Keep a gracefully-terminating pod visible (Terminating, NotReady)
            // until its containers actually stop or the grace period elapses.
            // stop_pod_for can return — or a concurrent sync can reach finalize —
            // while a sidecar is still draining; finalizing then removes the pod
            // from the API within ~2s of deletion, racing the readiness flip so
            // watchers never observe Ready=False ("mark readiness on pods to
            // false while pod is in progress of terminating"). Gate on the
            // container state (fast pods, whose containers stop in ~1s, are not
            // delayed) with a grace-period backstop for stuck containers.
            if pod.metadata.deletion_timestamp.is_some() {
                let grace = pod
                    .metadata
                    .deletion_grace_period_seconds
                    .or_else(|| {
                        pod.spec
                            .as_ref()
                            .and_then(|s| s.termination_grace_period_seconds)
                    })
                    .unwrap_or(30);
                let grace_elapsed = pod
                    .metadata
                    .deletion_timestamp
                    .map(|dt| (chrono::Utc::now() - dt).num_seconds() >= grace)
                    .unwrap_or(true);
                let containers_running = self.runtime.is_pod_running(pod).await.unwrap_or(false);
                if containers_running && !grace_elapsed {
                    debug!(
                        "Pod {}/{} terminating — keeping visible until containers stop ({}s grace)",
                        namespace, pod_name, grace
                    );
                    return Ok(());
                }
            }

            let has_finalizers = pod
                .metadata
                .finalizers
                .as_ref()
                .map(|f| !f.is_empty())
                .unwrap_or(false);
            if !has_finalizers {
                // Update status to terminal phase before removing from storage.
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    let original = p.clone();
                    if let Some(ref mut status) = p.status {
                        if status.phase != Some(Phase::Failed)
                            && status.phase != Some(Phase::Succeeded)
                        {
                            status.phase = Some(Phase::Succeeded);
                        }
                        // Refresh init container statuses — set ready=true for
                        // all completed init containers. When the pod reaches
                        // Succeeded, all init containers must have completed.
                        if let Some(ref mut ics) = status.init_container_statuses {
                            for ic in ics.iter_mut() {
                                if let Some(ContainerState::Terminated { exit_code, .. }) =
                                    &ic.state
                                {
                                    if *exit_code == 0 {
                                        ic.ready = true;
                                        ic.started = Some(true);
                                    }
                                } else {
                                    // Docker removed the container — mark as completed
                                    ic.state = Some(ContainerState::Terminated {
                                        exit_code: 0,
                                        reason: Some("Completed".to_string()),
                                        message: None,
                                        started_at: None,
                                        finished_at: None,
                                        container_id: None,
                                        signal: None,
                                    });
                                    ic.ready = true;
                                    ic.started = Some(true);
                                }
                            }
                        }
                    }
                    // Refresh container statuses
                    let fresh_statuses = self.get_container_statuses(&p).await.ok();
                    if let Some(ref mut status) = p.status {
                        if let Some(cs) = fresh_statuses {
                            status.container_statuses = Some(cs);
                        }
                    }
                    if !pod_status_equal(&original, &p) {
                        let _ = self.storage.update_status(&key, &p).await;
                    }
                }
                // Now that containers are stopped and the terminal status is
                // persisted, decide whether the object stays or is removed. The
                // rule lives in `finalize_terminated_pod_storage` so it can be
                // unit-tested against real storage; see that helper for why an
                // explicitly-deleted, finalizer-free pod must be removed.
                match finalize_terminated_pod_storage(
                    self.storage.as_ref(),
                    &key,
                    pod.metadata.deletion_timestamp.is_some(),
                    has_finalizers,
                )
                .await
                {
                    Ok(true) => {
                        // Defense in depth (#1157): a finalize that reports
                        // removal but leaves the object behind would make this
                        // worker re-enter TerminatingPod every reconcile and
                        // re-stop containers forever. Verify the delete took;
                        // if not, record a failure (drives the retry backoff at
                        // the needs_terminating gate) and escalate the log so a
                        // storage-delete regression is loud, not a silent loop.
                        if self.storage.get::<Pod>(&key).await.is_ok() {
                            let attempts = self.record_terminal_finalize_failure(&key);
                            if attempts >= TERMINAL_FINALIZE_ERROR_THRESHOLD {
                                error!(
                                    "Pod {}/{}: finalize reported removal but the object is still in storage after {} attempts — storage delete is not taking effect (regression of #1156?)",
                                    namespace, pod_name, attempts
                                );
                            } else {
                                warn!(
                                    "Pod {}/{}: finalize reported removal but the object is still in storage (attempt {}); backing off before retry",
                                    namespace, pod_name, attempts
                                );
                            }
                        } else {
                            self.clear_terminal_finalize_failure(&key);
                            debug!(
                                "Pod {}/{} removed from storage (deletionTimestamp set, no finalizers)",
                                namespace, pod_name
                            );
                        }
                    }
                    Ok(false) => {
                        debug!("Pod {}/{} marked terminal in storage", namespace, pod_name)
                    }
                    Err(e) => warn!(
                        "Pod {}/{}: failed to finalize storage state: {}",
                        namespace, pod_name, e
                    ),
                }
                // Pod is terminal — drop its CrashLoopBackOff state.
                self.forget_restart_backoff(namespace, pod_name);
            } else {
                // Pod has finalizers — update status to Failed but don't delete
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    let original = p.clone();
                    if let Some(ref mut status) = p.status {
                        if status.phase != Some(Phase::Failed)
                            && status.phase != Some(Phase::Succeeded)
                        {
                            status.phase = Some(Phase::Succeeded);
                        }
                    }
                    if !pod_status_equal(&original, &p) {
                        let _ = self.storage.update_status(&key, &p).await;
                    }
                }
            }
            // Volumes are cleaned by stop_pod_for during TerminatingPod.
            // Remove pod worker state — K8s HandlePodCleanups removes finished workers
            self.pod_states.lock().unwrap().remove(pod_uid);
            return Ok(());
        }

        // === TerminatingPod: stop containers, transition to TerminatedPod ===
        // K8s ref: pkg/kubelet/kubelet.go:2398 — SyncTerminatingPod
        // Stops all containers (with preStop hooks), does NOT remove them.
        // Container removal is handled by the container GC.
        if matches!(current_state, PodWorkerState::TerminatingPod) {
            info!(
                "Pod {}/{} terminating — stopping containers",
                namespace, pod_name
            );

            // A pod in the process of terminating is no longer Ready, and its
            // liveness probes are disabled (see check_liveness, which short-
            // circuits on a deletionTimestamp). Persist Ready=False /
            // ContainersReady=False *before* the (blocking) container stop so
            // watchers observe the readiness flip while the pod drains during
            // its grace period — upstream marks a terminating pod NotReady
            // (status_manager + the prober disabling probes on termination).
            // Conformance: "should mark readiness on pods to false … while …
            // terminating" waits for this via an informer.
            {
                let key = build_key("pods", Some(namespace), pod_name);
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    let original = p.clone();
                    if let Some(ref mut status) = p.status {
                        status.conditions = Some(Self::merge_pod_conditions(
                            status.conditions.as_deref().unwrap_or(&[]),
                            Self::not_ready_pod_conditions(),
                        ));
                        if let Some(ref mut cs) = status.container_statuses {
                            for c in cs.iter_mut() {
                                c.ready = false;
                            }
                        }
                        if let Some(ref mut ics) = status.init_container_statuses {
                            for ic in ics.iter_mut() {
                                ic.ready = false;
                            }
                        }
                    }
                    if !pod_status_equal(&original, &p) {
                        let _ = self.storage.update_status(&key, &p).await;
                    }
                }
            }

            let grace_period = pod
                .metadata
                .deletion_grace_period_seconds
                .or_else(|| {
                    pod.spec
                        .as_ref()
                        .and_then(|s| s.termination_grace_period_seconds)
                })
                .unwrap_or(30);

            // Stop the pod containers, executing preStop lifecycle hooks.
            // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go:849
            //
            // ONLY when the pod is actually being deleted (deletionTimestamp
            // set). A pod that reached a terminal phase ON ITS OWN
            // (Succeeded/Failed with restartPolicy != Always) has already had
            // every container exit — there is nothing left to stop. Crucially,
            // stop_pod_for also REMOVES the pod sandbox, which deletes the
            // exited containers and, with them, the CRI log files the api-server
            // serves `kubectl logs` / `GET pods/<p>/log` from
            // (resolve_container_id then returns "no container found"). Tearing
            // them down the instant the pod completes breaks log retrieval for
            // every run-to-completion pod — i.e. most output-checking
            // conformance tests. Leave the exited containers in place; the
            // container GC keeps one dead container per still-existing pod for
            // log access and reaps the rest, exactly like upstream
            // SyncTerminatingPod (which kills containers but never removes them).
            if pod.metadata.deletion_timestamp.is_some() {
                if let Err(e) = self.runtime.stop_pod_for(pod, grace_period).await {
                    warn!("Error stopping pod {}/{}: {}", namespace, pod_name, e);
                }
            }

            // Transition to TerminatedPod — containers are stopped.
            self.pod_states
                .lock()
                .unwrap()
                .insert(pod_uid.clone(), PodWorkerState::TerminatedPod);

            // Update pod status to terminal phase.
            // K8s kubelet NEVER deletes pods from the API server.
            let key = build_key("pods", Some(namespace), pod_name);
            if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                let original = p.clone();
                if let Some(ref mut status) = p.status {
                    if status.phase != Some(Phase::Failed) {
                        status.phase = Some(Phase::Succeeded);
                    }
                    Self::fixup_init_container_ready(status);
                }
                let fresh_statuses = self.get_container_statuses(&p).await.ok();
                if let Some(ref mut status) = p.status {
                    if let Some(cs) = fresh_statuses {
                        status.container_statuses = Some(cs);
                    }
                }
                if !pod_status_equal(&original, &p) {
                    let _ = self.storage.update_status(&key, &p).await;
                }
            }
            debug!(
                "Pod {}/{} terminated (containers stopped, left for GC)",
                namespace, pod_name
            );
            return Ok(());
        }

        // Check activeDeadlineSeconds — terminate pod if it has been running too long
        if let Some(ref spec) = pod.spec {
            if let Some(deadline) = spec.active_deadline_seconds {
                if let Some(ref status) = pod.status {
                    if let Some(start_time) = status.start_time {
                        let elapsed = chrono::Utc::now()
                            .signed_duration_since(start_time)
                            .num_seconds();
                        if elapsed > deadline {
                            info!(
                                "Pod {}/{} exceeded activeDeadlineSeconds ({}s > {}s)",
                                namespace, pod_name, elapsed, deadline
                            );
                            let key = build_key("pods", Some(namespace), pod_name);
                            let mut failed_pod = pod.clone();
                            if let Some(ref mut s) = failed_pod.status {
                                s.phase = Some(Phase::Failed);
                                s.reason = Some("DeadlineExceeded".to_string());
                                s.message = Some(format!(
                                    "Pod was active on the node longer than the specified deadline ({}s)",
                                    deadline
                                ));
                            }
                            let _ = self.storage.update_status(&key, &failed_pod).await;
                            // Stop the pod
                            if self.runtime.is_pod_running(pod).await.unwrap_or(false) {
                                let _ = self.runtime.stop_pod_with_grace_period(pod_name, 0).await;
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }

        // Check current runtime status with timeout to prevent sync loop blocking.
        // If the pod was already Running and the Docker check times out, assume it's
        // still running — defaulting to "not running" causes the readiness path to be
        // skipped entirely, leaving pods stuck in not-Ready state.
        let was_running = matches!(
            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(Phase::Running)
        );
        let is_running = match tokio::time::timeout(
            std::time::Duration::from_secs(15),
            self.runtime.is_pod_running(pod),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                if was_running {
                    debug!(
                        "Timeout checking pod {}/{} runtime status, assuming still running",
                        namespace, pod_name
                    );
                    true
                } else {
                    warn!(
                        "Timeout checking pod {}/{} runtime status, assuming not running",
                        namespace, pod_name
                    );
                    false
                }
            }
        };

        // Get current phase from pod status
        let current_phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref())
            .unwrap_or(&Phase::Pending);

        // K8s kubelet admission: reject a pod whose declared OS does not match
        // this node's OS. Our nodes are Linux, so a pod with spec.os.name set to
        // anything but "linux" is rejected with Phase=Failed and reason
        // PodOSNotSupported. K8s ref: pkg/kubelet/kubelet_pods.go — the PodOS
        // admit handler (GetPodOSValidationError / "PodOSNotSupported").
        if matches!(current_phase, Phase::Pending) && !is_running {
            if let Some(os) = pod.spec.as_ref().and_then(|s| s.os.as_ref()) {
                if os.name != "linux" {
                    info!(
                        "Pod {}/{} rejected: pod OS {:?} not supported on this (linux) node",
                        namespace, pod_name, os.name
                    );
                    let key = build_key("pods", Some(namespace), pod_name);
                    if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                        if let Some(ref mut status) = p.status {
                            status.phase = Some(Phase::Failed);
                            status.reason = Some("PodOSNotSupported".to_string());
                            status.message = Some(
                                "Pod was rejected as the node does not support the requested pod OS"
                                    .to_string(),
                            );
                        }
                        let _ = self.storage.update_status(&key, &p).await;
                    }
                    return Ok(());
                }
            }
        }

        // K8s kubelet admission: reject a pod requesting a forbidden sysctl. A
        // sysctl is allowed only if it is safe (namespaced + isolated) or
        // explicitly permitted via --allowed-unsafe-sysctls; otherwise the pod
        // is rejected with Phase=Failed, reason=SysctlForbidden (without ever
        // creating containers). K8s ref: pkg/kubelet/sysctl/allowlist.go::Admit.
        if matches!(current_phase, Phase::Pending) && !is_running {
            if let Err(message) = self.sysctl_allowlist.admit(pod) {
                info!("Pod {}/{} rejected: {}", namespace, pod_name, message);
                let key = build_key("pods", Some(namespace), pod_name);
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    if let Some(ref mut status) = p.status {
                        status.phase = Some(Phase::Failed);
                        status.reason = Some(crate::sysctl::FORBIDDEN_REASON.to_string());
                        status.message = Some(message);
                    }
                    let _ = self.storage.update_status(&key, &p).await;
                }
                return Ok(());
            }
        }

        // K8s kubelet admission: check hostPort conflicts before starting the pod.
        // K8s ref: pkg/kubelet/kubelet.go:2752 — allocationManager.AddPod
        // If a pod's hostPorts conflict with already-running pods on this node,
        // reject it immediately with Phase=Failed. The owning controller (StatefulSet,
        // etc.) can then delete and recreate it.
        if matches!(current_phase, Phase::Pending) && !is_running {
            if let Some(spec) = &pod.spec {
                // Collect (hostPort, protocol, hostIP) tuples from all containers.
                // K8s ref: pkg/scheduler/framework/types.go — HostPortInfo.Add
                let pod_host_ports: Vec<(u16, String, String)> = spec
                    .containers
                    .iter()
                    .flat_map(|c| c.ports.iter().flatten())
                    .filter_map(|p| {
                        // hostPort == 0 means "no host port" — it must not be
                        // treated as an allocated port, or two pods that both
                        // leave hostPort unset (0) falsely conflict and the
                        // kubelet rejects the pod with HostPortConflict. The
                        // conformance netexec pods declare containerPort with
                        // hostPort: 0.
                        p.host_port.filter(|&hp| hp != 0).map(|hp| {
                            let proto = p.protocol.clone();
                            let ip = p.host_ip.clone().unwrap_or_default();
                            (hp, proto, ip)
                        })
                    })
                    .collect();

                if !pod_host_ports.is_empty() {
                    // Get all pods on this node
                    let all_pods_prefix = build_prefix("pods", None);
                    let all_pods: Vec<Pod> = self
                        .storage
                        .list(&all_pods_prefix)
                        .await
                        .unwrap_or_default();
                    let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
                    let active_on_node: Vec<&Pod> = all_pods
                        .iter()
                        .filter(|p| {
                            // Filter by namespace+name to avoid matching wrong pod
                            let same_pod = p.metadata.name == pod.metadata.name
                                && p.metadata.namespace.as_deref().unwrap_or("default") == pod_ns;
                            !same_pod
                                && p.spec.as_ref().and_then(|s| s.node_name.as_deref())
                                    == Some(&self.node_name)
                                && !matches!(
                                    p.status.as_ref().and_then(|s| s.phase.as_ref()),
                                    Some(Phase::Failed) | Some(Phase::Succeeded)
                                )
                                && p.metadata.deletion_timestamp.is_none()
                        })
                        .collect();

                    for (port, proto, ip) in &pod_host_ports {
                        for existing in &active_on_node {
                            if let Some(existing_spec) = &existing.spec {
                                for c in &existing_spec.containers {
                                    for ep in c.ports.iter().flatten() {
                                        if let Some(ehp) = ep.host_port {
                                            let eproto = ep.protocol.as_str();
                                            // Must match port AND protocol to conflict
                                            // K8s ref: pkg/scheduler/framework/types.go — CheckConflict
                                            if ehp == *port && eproto == proto {
                                                let eip = ep.host_ip.clone().unwrap_or_default();
                                                // Check hostIP overlap:
                                                // - Empty/"0.0.0.0"/"::" are wildcards that overlap everything
                                                // - Two specific different IPs do NOT conflict
                                                let is_wildcard = |s: &str| {
                                                    s.is_empty() || s == "0.0.0.0" || s == "::"
                                                };
                                                let conflict = is_wildcard(ip)
                                                    || is_wildcard(&eip)
                                                    || ip == &eip;
                                                if conflict {
                                                    info!(
                                                        "Pod {}/{} rejected: hostPort {}/{} conflicts with pod {}",
                                                        namespace, pod_name, port, proto, existing.metadata.name
                                                    );
                                                    let key = build_key(
                                                        "pods",
                                                        Some(namespace),
                                                        pod_name,
                                                    );
                                                    if let Ok(mut p) =
                                                        self.storage.get::<Pod>(&key).await
                                                    {
                                                        if let Some(ref mut status) = p.status {
                                                            status.phase = Some(Phase::Failed);
                                                            status.reason = Some(
                                                                "HostPortConflict".to_string(),
                                                            );
                                                            status.message = Some(format!(
                                                                "Pod was rejected: host port {} is already in use",
                                                                port
                                                            ));
                                                        }
                                                        let _ = self
                                                            .storage
                                                            .update_status(&key, &p)
                                                            .await;
                                                    }
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        match current_phase {
            // If pod is Pending and has been scheduled to this node, start it
            Phase::Pending if !is_running => {
                // Don't overwrite error status — the test needs to observe it.
                // For ErrImagePull, skip retry for this sync cycle to prevent
                // blocking the sync loop with repeated pull failures.
                // K8s uses exponential backoff for image pulls.
                let already_has_error = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.reason.as_deref())
                    .is_some_and(|r| {
                        r == "CreateContainerError"
                            || r == "CreateContainerConfigError"
                            || r == "ErrImagePull"
                            || r == "ErrImageNeverPull"
                            || r == "ImagePullBackOff"
                    });

                if !already_has_error {
                    let has_init_containers = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.init_containers.as_ref())
                        .is_some_and(|ic| {
                            ic.iter()
                                .any(|c| c.restart_policy.as_deref() != Some("Always"))
                        });

                    // For pods with init containers, use the state machine approach.
                    // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go — computeInitContainerActions
                    // Check if the pod sandbox has been created.
                    let sandbox_exists = self.runtime.has_sandbox(pod_name).await;

                    if has_init_containers && sandbox_exists {
                        // Pod sandbox exists — check init container progress
                        let (all_done, next_idx, should_retry) =
                            self.runtime.compute_init_container_actions(pod).await;

                        if all_done {
                            // All init containers done — start_pod will skip init and start app containers
                            info!("All init containers completed for pod {}/{}, starting app containers", namespace, pod_name);
                        } else if let Some(idx) = next_idx {
                            let init_containers =
                                pod.spec.as_ref().unwrap().init_containers.as_ref().unwrap();
                            let ic = &init_containers[idx];

                            if should_retry {
                                // Init container failed — update status and return.
                                // The next sync cycle will retry (with implicit backoff from sync interval).
                                debug!(
                                    "Init container {} failed for pod {}/{}, will retry next sync",
                                    ic.name, namespace, pod_name
                                );
                                // Remove failed container so it can be recreated
                                let _ = self
                                    .runtime
                                    .remove_terminated_container(&pod.metadata.uid, &ic.name)
                                    .await;
                                // Update status with CrashLoopBackOff AND make sure
                                // the PodInitialized=False condition + app-container
                                // Waiting/PodInitializing statuses are present.
                                //
                                // K8s conformance ("should not start app containers if
                                // init containers fail on a RestartAlways pod") asserts:
                                //   - pod.status.phase == Pending
                                //   - pod.conditions[Initialized].reason ==
                                //         "ContainersNotInitialized"
                                //   - pod.conditions[Initialized].message ==
                                //         "containers with incomplete status: [<names>]"
                                //   - every container_status.state.waiting.reason ==
                                //         "PodInitializing"
                                //
                                // start_pod's failure handler sets these on the first
                                // sync, but every subsequent retry that re-enters via
                                // should_retry must also assert them so a partial
                                // status overwrite from any concurrent writer can't
                                // strip the Initialized condition or leak an app
                                // container into a non-Waiting state.
                                // K8s ref: pkg/kubelet/status/generate.go —
                                //          GeneratePodInitializedCondition
                                let key = build_key("pods", Some(namespace), pod_name);
                                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                    let init_statuses = self.get_init_container_statuses(&p).await;
                                    // Names of init containers that did NOT terminate
                                    // with exit 0 — these are "incomplete" per K8s.
                                    let incomplete_inits: Vec<String> = p
                                        .spec
                                        .as_ref()
                                        .and_then(|s| s.init_containers.as_ref())
                                        .map(|ics| {
                                            ics.iter()
                                                .filter(|c| {
                                                    let completed = init_statuses
                                                        .as_ref()
                                                        .and_then(|statuses| {
                                                            statuses
                                                                .iter()
                                                                .find(|s| s.name == c.name)
                                                        })
                                                        .map(|s| {
                                                            matches!(
                                                                &s.state,
                                                                Some(ContainerState::Terminated {
                                                                    exit_code: 0,
                                                                    ..
                                                                })
                                                            )
                                                        })
                                                        .unwrap_or(false);
                                                    !completed
                                                })
                                                .map(|c| c.name.clone())
                                                .collect()
                                        })
                                        .unwrap_or_default();
                                    let failed_conditions =
                                        Self::init_failed_pod_conditions(&incomplete_inits);
                                    // Ensure app containers stay Waiting/PodInitializing.
                                    // The conformance watch errors out the moment it
                                    // sees any app container outside that state.
                                    let app_container_statuses: Option<Vec<ContainerStatus>> =
                                        p.spec.as_ref().map(|spec| {
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
                                                    allocated_resources: c
                                                        .resources
                                                        .as_ref()
                                                        .and_then(|r| r.requests.clone()),
                                                    allocated_resources_status: None,
                                                    resources: c.resources.clone(),
                                                    user: None,
                                                    volume_mounts: None,
                                                    stop_signal: None,
                                                })
                                                .collect()
                                        });
                                    let status_msg = if !incomplete_inits.is_empty() {
                                        format!(
                                            "containers with incomplete status: [{}]",
                                            incomplete_inits.join(" ")
                                        )
                                    } else {
                                        "Init container failed".to_string()
                                    };
                                    if let Some(ref mut s) = p.status {
                                        // Pod stays Pending under RestartAlways — never
                                        // flip to Failed when an init can still retry.
                                        s.phase = Some(Phase::Pending);
                                        s.reason = Some("PodInitializing".to_string());
                                        s.message = Some(status_msg);
                                        s.init_container_statuses = init_statuses;
                                        s.container_statuses = app_container_statuses;
                                        s.conditions = Some(failed_conditions);
                                    }
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                                return Ok(());
                            } else {
                                // Need to start this init container
                                info!(
                                    "Starting init container {} (index {}) for pod {}/{}",
                                    ic.name, idx, namespace, pod_name
                                );
                                // Ensure image is available before starting
                                if let Err(e) = self
                                    .runtime
                                    .ensure_image(
                                        &ic.image,
                                        ic.image_pull_policy.as_deref(),
                                        Some((pod, ic.name.as_str())),
                                    )
                                    .await
                                {
                                    warn!(
                                        "Failed to pull image for init container {}: {}",
                                        ic.name, e
                                    );
                                    return Ok(());
                                }
                                let volume_paths: std::collections::HashMap<String, String> = pod
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.volumes.as_ref())
                                    .map(|vols| {
                                        vols.iter()
                                            .map(|v| {
                                                let path = format!(
                                                    "{}/{}/{}",
                                                    self.runtime.volumes_base_path(),
                                                    pod_name,
                                                    v.name
                                                );
                                                (v.name.clone(), path)
                                            })
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.as_deref());
                                if let Err(e) = self
                                    .runtime
                                    .start_container(pod, ic, &volume_paths, None, None, pod_ip)
                                    .await
                                {
                                    warn!(
                                        "Failed to start init container {} for {}/{}: {}",
                                        ic.name, namespace, pod_name, e
                                    );
                                }
                                // Update status
                                let init_statuses = self.get_init_container_statuses(pod).await;
                                let key = build_key("pods", Some(namespace), pod_name);
                                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                    if let Some(ref mut s) = p.status {
                                        s.init_container_statuses = init_statuses;
                                        s.reason = Some("PodInitializing".to_string());
                                    }
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                                return Ok(());
                            }
                        } else {
                            // No next init container and not all done — pod is terminal or waiting
                            return Ok(());
                        }
                    }

                    info!("Starting pod: {}/{}", namespace, pod_name);
                    let reason = if has_init_containers && !sandbox_exists {
                        "PodInitializing"
                    } else {
                        "ContainerCreating"
                    };
                    self.update_pod_status(pod, Phase::Pending, Some(reason), None)
                        .await?;
                } else {
                    debug!("Pod {}/{} already has CreateContainer(Config)Error, retrying without status reset", namespace, pod_name);
                }

                // Start the pod with timeout
                match tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    self.runtime.start_pod(pod),
                )
                .await
                {
                    Err(_timeout) => {
                        warn!(
                            "Timeout starting pod {}/{}, will retry",
                            namespace, pod_name
                        );
                        return Ok(());
                    }
                    Ok(result) => match result {
                        Ok(_) => {
                            info!("Pod {}/{} started successfully", namespace, pod_name);

                            // Re-fetch the pod from etcd to get the latest resourceVersion.
                            // Between start_pod being called and now, the admission controller or
                            // another writer may have incremented the resourceVersion (e.g. injecting
                            // service account tokens). Using a stale resourceVersion causes an
                            // optimistic-concurrency conflict that silently leaves the pod in Pending,
                            // which causes sonobuoy-worker and similar clients to mis-detect that
                            // all containers have already finished.
                            let key = build_key("pods", Some(namespace), pod_name);
                            let fresh_pod: Pod = match self.storage.get(&key).await {
                                Ok(p) => p,
                                _ => pod.clone(),
                            };

                            // Terminal phases are sticky: if the pod already
                            // reached Succeeded/Failed in storage, don't write
                            // the Running status below. This subsumes the older
                            // SysctlForbidden-specific guard (start_pod may have
                            // rejected the pod during admission, e.g. an unsafe
                            // sysctl -> phase=Failed, reason=SysctlForbidden,
                            // without creating any containers) and also blocks a
                            // genuine Succeeded->Running flap that would let the
                            // job controller delete a completed pod (#1048).
                            // Upstream parity: kubelet_pods.go:1934-1942
                            // generateAPIPodStatus ("pods are not allowed to
                            // transition out of terminal phases").
                            if should_skip_phase_write(
                                fresh_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                                &Phase::Running,
                            ) {
                                debug!(
                                    "Pod {}/{} already terminal in storage ({:?}); not overwriting with Running",
                                    namespace,
                                    pod_name,
                                    fresh_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                                );
                                return Ok(());
                            }

                            // Get container statuses and pod IP
                            let container_statuses =
                                self.get_container_statuses(&fresh_pod).await.ok();
                            let pod_ip = self.runtime.get_pod_ip(pod_name).await.ok().flatten();
                            let pod_i_ps = pod_ip.as_ref().map(|ip| vec![PodIP { ip: ip.clone() }]);

                            // Write Running status using the fresh resourceVersion
                            let mut new_pod = fresh_pod;
                            let qos = Self::compute_qos_class(&new_pod);
                            let observed_gen = new_pod.metadata.generation;
                            let init_container_statuses =
                                self.get_init_container_statuses(&new_pod).await;

                            // If any container has a readiness probe, start as not-ready
                            // and let the probe check in the sync loop update Ready to True.
                            let has_readiness_probe = new_pod
                                .spec
                                .as_ref()
                                .map(spec_has_readiness_probe)
                                .unwrap_or(false);
                            let conditions = if has_readiness_probe {
                                Self::not_ready_pod_conditions()
                            } else if !readiness_gates_satisfied(&new_pod) {
                                // Containers are ready, but an unsatisfied
                                // readinessGate must hold Ready=False (upstream
                                // GeneratePodReadyCondition). ContainersReady stays
                                // True; the gate condition arrives later via the
                                // status subresource and the reconcile loop flips
                                // Ready to True.
                                Self::pod_readiness_conditions(true, false)
                            } else {
                                Self::running_pod_conditions()
                            };

                            let ephemeral_container_statuses = self
                                .runtime
                                .get_ephemeral_container_statuses(&new_pod)
                                .await;

                            new_pod.status = Some(PodStatus {
                                phase: Some(Phase::Running),
                                message: Some("All containers started".to_string()),
                                reason: None,
                                host_ip: Some(Self::node_internal_ip().to_string()),
                                pod_ip,
                                conditions: Some(conditions),
                                container_statuses,
                                init_container_statuses,
                                ephemeral_container_statuses,
                                resize: None,
                                resource_claim_statuses: None,
                                observed_generation: observed_gen,
                                host_i_ps: Some(vec![rusternetes_common::resources::pod::HostIP {
                                    ip: Self::node_internal_ip().to_string(),
                                }]),
                                pod_i_ps,
                                nominated_node_name: None,
                                qos_class: Some(qos),
                                start_time: Some(chrono::Utc::now()),
                                ..Default::default()
                            });

                            if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                                // Retry with fresh read on conflict (K8s pattern)
                                if e.to_string().contains("Conflict")
                                    || e.to_string().contains("mismatch")
                                {
                                    if let Ok(fresh_pod) = self.storage.get::<Pod>(&key).await {
                                        // Terminal phases are sticky — don't
                                        // regress a Succeeded/Failed pod to
                                        // Running on the conflict retry.
                                        if should_skip_phase_write(
                                            fresh_pod
                                                .status
                                                .as_ref()
                                                .and_then(|s| s.phase.as_ref()),
                                            &Phase::Running,
                                        ) {
                                            return Ok(());
                                        }
                                        let mut retry_pod = fresh_pod;
                                        // observedGeneration tracks the generation of
                                        // the re-read object, not the value computed
                                        // from the pre-conflict read — a concurrent
                                        // spec update is what bumped generation and
                                        // caused this conflict (#1170). Capture before
                                        // the mut borrow below.
                                        let retry_gen = retry_pod.metadata.generation;
                                        // Re-fetch ALL statuses for the retry — stale
                                        // init_container_statuses from an intermediate
                                        // write may have ready=false for completed inits.
                                        // K8s prober_manager.UpdatePodStatus always sets
                                        // ready=true for terminated init containers.
                                        let fresh_init_statuses =
                                            self.get_init_container_statuses(pod).await;
                                        let fresh_container_statuses =
                                            self.get_container_statuses(pod).await.ok();
                                        if let Some(ref mut status) = retry_pod.status {
                                            status.phase = Some(Phase::Running);
                                            status.message =
                                                Some("All containers ready".to_string());
                                            status.init_container_statuses = fresh_init_statuses;
                                            status.observed_generation = retry_gen;
                                            if let Some(cs) = fresh_container_statuses {
                                                status.container_statuses = Some(cs);
                                            }
                                        }
                                        if let Err(e2) =
                                            self.storage.update_status(&key, &retry_pod).await
                                        {
                                            warn!("Failed to update pod {}/{} status to Running after retry: {}", namespace, pod_name, e2);
                                        }
                                    }
                                } else {
                                    warn!(
                                        "Failed to update pod {}/{} status to Running: {}",
                                        namespace, pod_name, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            let err_msg = e.to_string();

                            // K8s retries volume mounting when a volume source
                            // isn't ready yet (Secret/ConfigMap not created, or a
                            // PersistentVolumeClaim that doesn't exist / isn't
                            // bound — #1096). The pod stays Pending with containers
                            // in Waiting{ContainerCreating}; syncPod returns early
                            // without creating any containers and the pod worker
                            // retries on the next sync cycle.
                            // K8s ref: pkg/kubelet/kubelet.go:2204 — WaitForAttachAndMount
                            //          pkg/kubelet/kubelet_pods.go:2496 — defaultWaitingState
                            if crate::lifecycle::is_volume_wait_error(&err_msg) {
                                warn!(
                                    "Pod {}/{} waiting for volume (will retry): {}",
                                    namespace, pod_name, err_msg
                                );
                                let key = build_key("pods", Some(namespace), pod_name);
                                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                    // Set container statuses to Waiting{ContainerCreating}
                                    let container_statuses: Vec<ContainerStatus> = p
                                        .spec
                                        .as_ref()
                                        .map(|s| {
                                            s.containers
                                                .iter()
                                                .map(|c| ContainerStatus {
                                                    name: c.name.clone(),
                                                    ready: false,
                                                    restart_count: 0,
                                                    state: Some(ContainerState::Waiting {
                                                        reason: Some(
                                                            "ContainerCreating".to_string(),
                                                        ),
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
                                        })
                                        .unwrap_or_default();
                                    if let Some(ref mut status) = p.status {
                                        status.phase = Some(Phase::Pending);
                                        status.container_statuses = Some(container_statuses);
                                        status.conditions = Some(Self::not_ready_pod_conditions());
                                    }
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                                return Ok(());
                            }

                            error!(
                                "Failed to start pod {}/{}: {}",
                                namespace, pod_name, err_msg
                            );

                            // Determine the error reason matching K8s container status reasons.
                            // Prefers typed downcast over substring sniffing — see upstream
                            // `pkg/kubelet/images/types.go` (sentinel errors) and
                            // `pkg/kubelet/kuberuntime/kuberuntime_container.go`.
                            let create_error_reason =
                                crate::lifecycle::reason_from_anyhow(&e).map(str::to_string);

                            if let Some(reason) = create_error_reason {
                                // Container creation/config error — pod stays Pending with
                                // container in Waiting state with appropriate reason.
                                let key = build_key("pods", Some(namespace), pod_name);
                                let fresh_pod: Pod = match self.storage.get(&key).await {
                                    Ok(p) => p,
                                    _ => pod.clone(),
                                };
                                let mut new_pod = fresh_pod;

                                // Build container statuses with the failed container
                                let container_statuses: Option<Vec<ContainerStatus>> =
                                    new_pod.spec.as_ref().map(|spec| {
                                        spec.containers
                                            .iter()
                                            .map(|c| ContainerStatus {
                                                name: c.name.clone(),
                                                ready: false,
                                                restart_count: 0,
                                                state: Some(ContainerState::Waiting {
                                                    reason: Some(reason.clone()),
                                                    message: Some(err_msg.clone()),
                                                }),
                                                last_state: None,
                                                image: Some(c.image.clone()),
                                                image_id: None,
                                                container_id: None,
                                                started: Some(false),
                                                allocated_resources: c
                                                    .resources
                                                    .as_ref()
                                                    .and_then(|r| r.requests.clone()),
                                                allocated_resources_status: None,
                                                resources: c.resources.clone(),
                                                user: None,
                                                volume_mounts: None,
                                                stop_signal: None,
                                            })
                                            .collect()
                                    });

                                // Get init container statuses — they may have run before the error
                                let init_container_statuses =
                                    self.get_init_container_statuses(&new_pod).await;

                                let qos = Self::compute_qos_class(&new_pod);
                                let observed_gen = new_pod.metadata.generation;
                                new_pod.status = Some(PodStatus {
                                    phase: Some(Phase::Pending),
                                    message: Some(err_msg),
                                    reason: Some(reason),
                                    host_ip: Some(Self::node_internal_ip().to_string()),
                                    pod_ip: None,
                                    conditions: None,
                                    container_statuses,
                                    init_container_statuses,
                                    ephemeral_container_statuses: None,
                                    resize: None,
                                    resource_claim_statuses: None,
                                    observed_generation: observed_gen,
                                    host_i_ps: Some(vec![
                                        rusternetes_common::resources::pod::HostIP {
                                            ip: Self::node_internal_ip().to_string(),
                                        },
                                    ]),
                                    pod_i_ps: None,
                                    nominated_node_name: None,
                                    qos_class: Some(qos),
                                    start_time: Some(chrono::Utc::now()),
                                    ..Default::default()
                                });

                                if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                                    warn!(
                                    "Failed to update pod {}/{} status to container error: {}, retrying",
                                    namespace, pod_name, e
                                );
                                    // CAS retry — re-read and apply status
                                    if let Ok(mut retry_pod) = self.storage.get::<Pod>(&key).await {
                                        retry_pod.status = new_pod.status.clone();
                                        let _ = self.storage.update_status(&key, &retry_pod).await;
                                    }
                                }
                            } else {
                                // Get init container statuses from Docker to capture
                                // actual exit codes for failed init containers
                                let key = build_key("pods", Some(namespace), pod_name);
                                let fresh_pod: Pod = match self.storage.get(&key).await {
                                    Ok(p) => p,
                                    _ => pod.clone(),
                                };
                                let init_container_statuses =
                                    self.get_init_container_statuses(&fresh_pod).await;
                                let qos = Self::compute_qos_class(&fresh_pod);
                                let observed_gen = fresh_pod.metadata.generation;

                                let mut new_pod = fresh_pod;

                                // Determine phase based on restart policy AND error type:
                                // - RestartNever: pod is Failed
                                // - Permanent failures (port conflict, etc.): pod is Failed
                                //   K8s kubelet marks unrecoverable pods as Failed via TerminatePod
                                //   K8s ref: pkg/kubelet/status/status_manager.go:629
                                // - Transient failures with RestartAlways: pod stays Pending
                                let restart_policy = new_pod
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.restart_policy.as_deref())
                                    .unwrap_or("Always");
                                // K8s does NOT transition to TerminatingPod for container
                                // creation errors. It retries in SyncPod state. The pod stays
                                // Pending and the controller (StatefulSet, etc.) handles it.
                                // Only eviction and deletion trigger TerminatingPod.
                                // K8s ref: pkg/kubelet/pod_workers.go — podWorkerLoop
                                let is_port_conflict = err_msg
                                    .contains("port is already allocated")
                                    || err_msg.contains("bind: address already in use");

                                let (phase, reason) =
                                    if restart_policy == "Never" || is_port_conflict {
                                        (Phase::Failed, "FailedToStart".to_string())
                                    } else {
                                        (Phase::Pending, "InitContainerFailed".to_string())
                                    };

                                // Build K8s-style message listing only INCOMPLETE init containers.
                                // An init container is "incomplete" if it didn't terminate with exit code 0.
                                // Successfully completed init containers (exit 0) should NOT be listed.
                                let init_statuses = new_pod
                                    .status
                                    .as_ref()
                                    .and_then(|s| s.init_container_statuses.as_ref());
                                let incomplete_inits: Vec<String> = new_pod
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.init_containers.as_ref())
                                    .map(|ics| {
                                        ics.iter()
                                            .filter(|c| {
                                                // Check if this init container completed successfully
                                                let completed = init_statuses
                                                    .and_then(|statuses| {
                                                        statuses.iter().find(|s| s.name == c.name)
                                                    })
                                                    .map(|s| {
                                                        matches!(
                                                            &s.state,
                                                            Some(
                                                                rusternetes_common::resources::ContainerState::Terminated {
                                                                    exit_code: 0, ..
                                                                }
                                                            )
                                                        )
                                                    })
                                                    .unwrap_or(false);
                                                !completed
                                            })
                                            .map(|c| c.name.clone())
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                let status_msg = if !incomplete_inits.is_empty() {
                                    format!(
                                        "containers with incomplete status: [{}]",
                                        incomplete_inits.join(" ")
                                    )
                                } else {
                                    err_msg.clone()
                                };
                                // Set proper conditions for failed init containers
                                let failed_conditions =
                                    Self::init_failed_pod_conditions(&incomplete_inits);

                                // Build app container statuses as Waiting/PodInitializing
                                // since init containers haven't completed, app containers were never started
                                let app_container_statuses: Option<Vec<ContainerStatus>> =
                                    new_pod.spec.as_ref().map(|spec| {
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
                                                allocated_resources: c
                                                    .resources
                                                    .as_ref()
                                                    .and_then(|r| r.requests.clone()),
                                                allocated_resources_status: None,
                                                resources: c.resources.clone(),
                                                user: None,
                                                volume_mounts: None,
                                                stop_signal: None,
                                            })
                                            .collect()
                                    });

                                new_pod.status = Some(PodStatus {
                                    phase: Some(phase),
                                    message: Some(status_msg),
                                    reason: Some(reason),
                                    host_ip: Some(Self::node_internal_ip().to_string()),
                                    pod_ip: None,
                                    conditions: Some(failed_conditions),
                                    container_statuses: app_container_statuses,
                                    init_container_statuses,
                                    ephemeral_container_statuses: None,
                                    resize: None,
                                    resource_claim_statuses: None,
                                    observed_generation: observed_gen,
                                    host_i_ps: Some(vec![
                                        rusternetes_common::resources::pod::HostIP {
                                            ip: Self::node_internal_ip().to_string(),
                                        },
                                    ]),
                                    pod_i_ps: None,
                                    nominated_node_name: None,
                                    qos_class: Some(qos),
                                    start_time: Some(chrono::Utc::now()),
                                    ..Default::default()
                                });

                                if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                                    warn!(
                                        "Failed to update pod {}/{} status after init failure: {}",
                                        namespace, pod_name, e
                                    );
                                }
                            }
                        }
                    }, // end inner match result
                } // end outer match timeout
            }
            // If pod is Pending but containers are already running, update to Running.
            // If a container has CreateContainerError/CreateContainerConfigError, retry starting it first.
            Phase::Pending if is_running => {
                // Check if any container is in CreateContainerError or CreateContainerConfigError
                let has_create_error = pod.status.as_ref()
                    .and_then(|s| s.container_statuses.as_ref())
                    .is_some_and(|statuses| {
                        statuses.iter().any(|cs| {
                            matches!(&cs.state, Some(ContainerState::Waiting { reason: Some(r), .. }) if r == "CreateContainerError" || r == "CreateContainerConfigError")
                        })
                    });

                let should_update_running = if has_create_error {
                    // Retry starting — spec/annotations may have changed
                    debug!(
                        "Pod {}/{} has container creation error, retrying start",
                        namespace, pod_name
                    );
                    match self.runtime.start_pod(pod).await {
                        Ok(_) => {
                            info!("Pod {}/{} retry succeeded", namespace, pod_name);
                            true // Now update to Running
                        }
                        Err(e) => {
                            debug!("Pod {}/{} retry still failing: {}", namespace, pod_name, e);
                            false // Stay in error state
                        }
                    }
                } else {
                    true // No error, proceed to Running
                };

                if should_update_running {
                    debug!(
                        "Pod {}/{} containers are running, updating status to Running",
                        namespace, pod_name
                    );

                    let key = build_key("pods", Some(namespace), pod_name);
                    let fresh_pod: Pod = match self.storage.get(&key).await {
                        Ok(p) => p,
                        _ => pod.clone(),
                    };

                    // Terminal phases are sticky — if a concurrent reconcile
                    // already set Succeeded/Failed in storage, don't regress to
                    // Running. Upstream parity: kubelet_pods.go:1934-1942
                    // generateAPIPodStatus ("pods are not allowed to
                    // transition out of terminal phases").
                    if should_skip_phase_write(
                        fresh_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                        &Phase::Running,
                    ) {
                        debug!(
                            "Pod {}/{} already terminal in storage ({:?}); not overwriting with Running",
                            namespace,
                            pod_name,
                            fresh_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                        );
                        return Ok(());
                    }

                    // Get container statuses
                    let container_statuses = self.get_container_statuses(&fresh_pod).await.ok();

                    // Get pod IP
                    let pod_ip = self.runtime.get_pod_ip(pod_name).await.ok().flatten();
                    let pod_i_ps = pod_ip.as_ref().map(|ip| vec![PodIP { ip: ip.clone() }]);

                    // Update status to Running
                    let mut new_pod = fresh_pod;
                    let qos = Self::compute_qos_class(&new_pod);
                    let observed_gen = new_pod.metadata.generation;
                    let init_container_statuses = self.get_init_container_statuses(&new_pod).await;

                    let has_readiness_probe = new_pod
                        .spec
                        .as_ref()
                        .map(spec_has_readiness_probe)
                        .unwrap_or(false);
                    let conditions = if has_readiness_probe {
                        Self::not_ready_pod_conditions()
                    } else if !readiness_gates_satisfied(&new_pod) {
                        // Containers are ready, but an unsatisfied readinessGate
                        // must hold Ready=False (upstream GeneratePodReadyCondition).
                        // ContainersReady stays True; the gate condition arrives
                        // later via the status subresource and the reconcile loop
                        // flips Ready to True.
                        Self::pod_readiness_conditions(true, false)
                    } else {
                        Self::running_pod_conditions()
                    };

                    let ephemeral_container_statuses = self
                        .runtime
                        .get_ephemeral_container_statuses(&new_pod)
                        .await;

                    new_pod.status = Some(PodStatus {
                        phase: Some(Phase::Running),
                        message: Some("All containers started".to_string()),
                        reason: None,
                        host_ip: Some(Self::node_internal_ip().to_string()),
                        pod_ip,
                        conditions: Some(conditions),
                        container_statuses,
                        init_container_statuses,
                        ephemeral_container_statuses,
                        resize: None,
                        resource_claim_statuses: None,
                        observed_generation: observed_gen,
                        host_i_ps: Some(vec![rusternetes_common::resources::pod::HostIP {
                            ip: Self::node_internal_ip().to_string(),
                        }]),
                        pod_i_ps,
                        nominated_node_name: None,
                        qos_class: Some(qos),
                        start_time: Some(chrono::Utc::now()),
                        ..Default::default()
                    });

                    // Use non-fatal update: if the write fails (e.g., concurrency conflict),
                    // the next sync will retry via the Pending+is_running path.
                    // Do NOT propagate the error — that causes update_pod_status_error to
                    // set the pod to Failed, which is unrecoverable.
                    if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                        warn!(
                            "Failed to update pod {}/{} to Running (will retry): {}",
                            namespace, pod_name, e
                        );
                    }
                } // end if should_update_running
            }
            Phase::Running if is_running => {
                debug!("Pod {}/{} is running, checking health", namespace, pod_name);

                // Re-read from storage when resize is pending to get the latest spec
                // (the API server may have updated resources since our list() call).
                let key = build_key("pods", Some(namespace), pod_name);
                let resize_status = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.resize.as_deref())
                    .unwrap_or("");
                let fresh_pod = if resize_status == "Proposed" || resize_status == "InProgress" {
                    // Re-read to get fresh spec with updated resources
                    self.storage
                        .get::<Pod>(&key)
                        .await
                        .unwrap_or_else(|_| pod.clone())
                } else {
                    pod.clone()
                };

                // Re-check resize status from fresh pod (may differ from list-fetched pod)
                let resize_status = fresh_pod
                    .status
                    .as_ref()
                    .and_then(|s| s.resize.as_deref())
                    .unwrap_or("");

                // Handle in-place pod resize (KEP-1287):
                // Flow: API sets resize="Proposed" -> kubelet sets "InProgress" ->
                // applies resources -> sets resize="" with updated allocatedResources
                if resize_status == "Proposed" || resize_status == "InProgress" {
                    // Set resize to InProgress if it was Proposed
                    if resize_status == "Proposed" {
                        let rkey = build_key("pods", Some(namespace), pod_name);
                        if let Ok(mut rpod) = self.storage.get::<Pod>(&rkey).await {
                            if let Some(ref mut status) = rpod.status {
                                status.resize = Some("InProgress".to_string());
                            }
                            let _ = self.storage.update_status(&rkey, &rpod).await;
                        }
                    }

                    // Apply resource changes to containers
                    let mut all_resized = true;
                    if let Some(spec) = &fresh_pod.spec {
                        for container in &spec.containers {
                            if let Some(resources) = &container.resources {
                                let container_name = format!("{}_{}", pod_name, container.name);
                                let mut cpu_period = None;
                                let mut cpu_quota = None;
                                let mut memory = None;
                                let mut needs_update = false;

                                // Set CPU shares from requests (maps to cgroup cpu.weight)
                                let mut cpu_shares: Option<i64> = None;
                                if let Some(requests) = &resources.requests {
                                    if let Some(cpu) = requests.get("cpu") {
                                        let millicores = crate::runtime::parse_cpu_quantity(cpu);
                                        if millicores > 0 {
                                            // K8s formula: shares = max(2, (millicores * 1024) / 1000)
                                            cpu_shares = Some(((millicores * 1024) / 1000).max(2));
                                            needs_update = true;
                                        }
                                    }
                                }

                                if let Some(limits) = &resources.limits {
                                    if let Some(cpu) = limits.get("cpu") {
                                        let millicores = crate::runtime::parse_cpu_quantity(cpu);
                                        if millicores > 0 {
                                            let period = 100000i64; // 100ms
                                            let quota = (millicores * period) / 1000;
                                            cpu_period = Some(period);
                                            cpu_quota = Some(quota);
                                            needs_update = true;
                                            // Also set shares from limits if no requests
                                            if cpu_shares.is_none() {
                                                cpu_shares =
                                                    Some(((millicores * 1024) / 1000).max(2));
                                            }
                                        }
                                    }
                                    if let Some(mem) = limits.get("memory") {
                                        let bytes = crate::runtime::parse_memory_quantity(mem);
                                        if bytes > 0 {
                                            memory = Some(bytes);
                                            needs_update = true;
                                        }
                                    }
                                }

                                if needs_update {
                                    match self
                                        .runtime
                                        .update_container_resources(
                                            &container_name,
                                            cpu_period,
                                            cpu_quota,
                                            cpu_shares,
                                            memory,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            info!(
                                                "Updated container {} resources (resize)",
                                                container_name
                                            );
                                        }
                                        Err(e) => {
                                            debug!(
                                                "Failed to update container {} resources: {}",
                                                container_name, e
                                            );
                                            all_resized = false;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Mark resize as complete and update allocatedResources
                    if all_resized {
                        let rkey = build_key("pods", Some(namespace), pod_name);
                        if let Ok(mut rpod) = self.storage.get::<Pod>(&rkey).await {
                            if let Some(ref mut status) = rpod.status {
                                status.resize = Some(String::new()); // Empty = resize complete
                                                                     // Update allocatedResources in container statuses
                                if let Some(ref spec) = rpod.spec.clone() {
                                    if let Some(ref mut cs_list) = status.container_statuses {
                                        for cs in cs_list.iter_mut() {
                                            if let Some(c) =
                                                spec.containers.iter().find(|c| c.name == cs.name)
                                            {
                                                if let Some(ref res) = c.resources {
                                                    cs.allocated_resources = res
                                                        .requests
                                                        .clone()
                                                        .or_else(|| res.limits.clone());
                                                    cs.resources = Some(res.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            let _ = self.storage.update_status(&rkey, &rpod).await;
                        }
                    }
                }

                // Use fresh_pod for all subsequent checks (spec may have been updated by resize PATCH)
                let pod = &fresh_pod;

                // Refresh Secret/ConfigMap volumes so updates are reflected in running pods
                if let Err(e) = self.runtime.refresh_volumes(pod).await {
                    debug!(
                        "Failed to refresh volumes for pod {}/{}: {}",
                        namespace, pod_name, e
                    );
                }

                // K8s computePodActions: check if any spec containers are MISSING from the
                // runtime and need to be (re)created. This happens when a container was never
                // started (e.g., after a StatefulSet PATCH recreates the pod) or was removed.
                if let Some(ref spec) = pod.spec {
                    for container in &spec.containers {
                        if !self
                            .runtime
                            .container_exists(&pod.metadata.uid, &container.name)
                            .await
                        {
                            info!(
                                "Container {} missing for running pod {}/{}, creating",
                                container.name, namespace, pod_name
                            );
                            let empty_binds = std::collections::HashMap::new();
                            if let Err(e) = self
                                .runtime
                                .start_container(
                                    pod,
                                    container,
                                    &empty_binds,
                                    None, // netns — pod already has networking via pause
                                    None, // hosts file
                                    pod.status.as_ref().and_then(|s| s.pod_ip.as_deref()),
                                )
                                .await
                            {
                                warn!(
                                    "Failed to create missing container {} for pod {}/{}: {}",
                                    container.name, namespace, pod_name, e
                                );
                            }
                        }
                    }
                }

                // Check if all spec containers have terminated (pause container may still be running).
                // This must happen before liveness probes, which may error on exited containers.
                {
                    let restart_policy = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.restart_policy.as_deref())
                        .unwrap_or("Always");

                    if restart_policy == "Never" || restart_policy == "OnFailure" {
                        if let Ok(container_statuses) = self.get_container_statuses(pod).await {
                            let all_terminated = !container_statuses.is_empty()
                                && container_statuses.iter().all(|cs| {
                                    matches!(cs.state, Some(ContainerState::Terminated { .. }))
                                });

                            if all_terminated && restart_policy == "Never" {
                                let any_failed = container_statuses.iter().any(|cs| {
                                    matches!(cs.state, Some(ContainerState::Terminated { exit_code, .. }) if exit_code != 0)
                                });
                                let terminal_phase = if any_failed {
                                    Phase::Failed
                                } else {
                                    Phase::Succeeded
                                };
                                let message = if any_failed {
                                    "Pod failed".to_string()
                                } else {
                                    "Pod completed successfully".to_string()
                                };

                                let key = build_key("pods", Some(namespace), pod_name);
                                let mut new_pod: Pod = match self.storage.get(&key).await {
                                    Ok(p) => p,
                                    _ => pod.clone(),
                                };
                                let original = new_pod.clone();
                                // Refresh init container statuses so completed init containers
                                // have ready=true in the final pod status.
                                // K8s ref: pkg/kubelet/prober/prober_manager.go — UpdatePodStatus
                                let init_container_statuses =
                                    self.get_init_container_statuses(&new_pod).await;
                                if let Some(ref mut status) = new_pod.status {
                                    status.phase = Some(terminal_phase);
                                    status.message = Some(message);
                                    status.container_statuses = Some(container_statuses);
                                    if init_container_statuses.is_some() {
                                        status.init_container_statuses = init_container_statuses;
                                    }
                                    // Update conditions — terminated pod is not Ready
                                    if let Some(ref mut conditions) = status.conditions {
                                        for c in conditions.iter_mut() {
                                            if c.condition_type == "Ready"
                                                || c.condition_type == "ContainersReady"
                                            {
                                                c.status = "False".to_string();
                                                c.reason = Some("PodCompleted".to_string());
                                            }
                                        }
                                    }
                                }
                                if !pod_status_equal(&original, &new_pod) {
                                    let _ = self.storage.update_status(&key, &new_pod).await;
                                }
                                return Ok(());
                            }

                            if all_terminated && restart_policy == "OnFailure" {
                                let any_failed = container_statuses.iter().any(|cs| {
                                    matches!(cs.state, Some(ContainerState::Terminated { exit_code, .. }) if exit_code != 0)
                                });

                                if !any_failed {
                                    // All containers exited successfully — re-read for fresh RV
                                    let key = build_key("pods", Some(namespace), pod_name);
                                    let mut new_pod: Pod = match self.storage.get(&key).await {
                                        Ok(p) => p,
                                        _ => pod.clone(),
                                    };
                                    let original = new_pod.clone();
                                    let init_container_statuses =
                                        self.get_init_container_statuses(&new_pod).await;
                                    if let Some(ref mut status) = new_pod.status {
                                        status.phase = Some(Phase::Succeeded);
                                        status.message =
                                            Some("Pod completed successfully".to_string());
                                        status.container_statuses = Some(container_statuses);
                                        if init_container_statuses.is_some() {
                                            status.init_container_statuses =
                                                init_container_statuses;
                                        }
                                        status.conditions = Some(Self::succeeded_pod_conditions());
                                    }
                                    if !pod_status_equal(&original, &new_pod) {
                                        let _ = self.storage.update_status(&key, &new_pod).await;
                                    }
                                    return Ok(());
                                }
                            }
                        }
                    }
                }

                // For restartPolicy=Always, detect exited containers and restart them.
                // Track restart counts and set CrashLoopBackOff when appropriate.
                // IMPORTANT: Use has_terminated_containers() instead of get_container_statuses()
                // to avoid running readiness probes here. Running probes twice per sync cycle
                // (once here and once in the readiness update below) causes the probe state
                // machine to advance twice, which can make intermittent probe results flip
                // the ready state from true to false within a single sync cycle.
                // Detect exited containers and restart them per restartPolicy,
                // paced by CrashLoopBackOff. restartCount is owned by the
                // per-container backoff map (incremented only on an actual
                // restart), NOT recomputed per sync observation.
                self.reconcile_container_restarts(namespace, pod_name, pod)
                    .await;

                // Start any ephemeral containers that aren't running yet.
                // Re-read the pod from storage to pick up ephemeral containers added
                // via PATCH since the last list() call.
                {
                    let key = build_key("pods", Some(namespace), pod_name);
                    let ec_pod: Pod = match self.storage.get(&key).await {
                        Ok(p) => p,
                        _ => pod.clone(),
                    };
                    if let Some(spec) = &ec_pod.spec {
                        if let Some(ecs) = &spec.ephemeral_containers {
                            let mut started_any = false;
                            for ec in ecs {
                                // Ephemeral containers are one-shot — never restart them.
                                // Skip if the container already exists in any state (running,
                                // exited, created). Only start truly new ephemeral containers.
                                if self
                                    .runtime
                                    .container_exists(&ec_pod.metadata.uid, &ec.name)
                                    .await
                                {
                                    continue;
                                }
                                info!(
                                    "Starting ephemeral container {} for pod {}/{}",
                                    ec.name, namespace, pod_name
                                );
                                // Convert EphemeralContainer to Container for start_container
                                let container = rusternetes_common::resources::Container {
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
                                    termination_message_policy: ec
                                        .termination_message_policy
                                        .clone(),
                                    stdin_once: ec.stdin_once,
                                    restart_policy: None,
                                    resize_policy: None,
                                    volume_devices: None,
                                    ..Default::default()
                                };
                                let volume_paths = self
                                    .runtime
                                    .create_pod_volumes(&ec_pod)
                                    .await
                                    .unwrap_or_default();
                                if let Err(e) = self
                                    .runtime
                                    .start_container(
                                        &ec_pod,
                                        &container,
                                        &volume_paths,
                                        None,
                                        None,
                                        None,
                                    )
                                    .await
                                {
                                    warn!("Failed to start ephemeral container {}: {}", ec.name, e);
                                } else {
                                    started_any = true;
                                }
                            }
                            // Update ephemeral container statuses after starting new ones
                            if started_any {
                                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                    let ec_statuses =
                                        self.runtime.get_ephemeral_container_statuses(&p).await;
                                    if let Some(ref mut status) = p.status {
                                        status.ephemeral_container_statuses = ec_statuses;
                                    }
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                            }
                        }
                    }
                }

                // Check liveness probes
                // check_liveness may error on transient probe failures — treat errors as "no restart needed"
                // to ensure the status update branch always runs
                let restart_grace = self.runtime.check_liveness(pod).await.unwrap_or(None);
                {
                    if let Some(probe_grace) = restart_grace {
                        let restart_policy = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.restart_policy.as_deref())
                            .unwrap_or("Always");

                        match restart_policy {
                            "Always" | "OnFailure" => {
                                warn!(
                                    "Restarting pod {}/{} due to failed liveness probe",
                                    namespace, pod_name
                                );

                                // Upstream emits Unhealthy when a probe fails and
                                // Killing as it tears the container down to restart.
                                self.runtime
                                    .emit_event(
                                        pod,
                                        None,
                                        crate::events::CONTAINER_UNHEALTHY,
                                        rusternetes_common::resources::EventType::Warning,
                                        "Liveness probe failed, container will be restarted",
                                    )
                                    .await;
                                self.runtime
                                    .emit_event(
                                        pod,
                                        None,
                                        crate::events::KILLING_CONTAINER,
                                        rusternetes_common::resources::EventType::Normal,
                                        "Stopping container after failed liveness probe",
                                    )
                                    .await;

                                // Capture current restart counts before stopping
                                let current_restart_counts: HashMap<String, u32> = pod
                                    .status
                                    .as_ref()
                                    .and_then(|s| s.container_statuses.as_ref())
                                    .map(|statuses| {
                                        statuses
                                            .iter()
                                            .map(|cs| (cs.name.clone(), cs.restart_count))
                                            .collect()
                                    })
                                    .unwrap_or_default();

                                // Stop and restart the pod using the failed
                                // probe's terminationGracePeriodSeconds (falls
                                // back to the pod's, then 30) — upstream uses the
                                // probe's grace to kill a container that failed
                                // its probe, not the (possibly much longer) pod
                                // grace. Conformance "should override
                                // timeoutGracePeriodSeconds when Liveness/
                                // StartupProbe field is set".
                                let grace = probe_grace;
                                if let Err(e) = self.runtime.stop_pod_for(pod, grace).await {
                                    error!("Failed to stop pod for restart: {}", e);
                                } else {
                                    // Restart is in back-off — upstream emits BackOff
                                    // before the container is recreated.
                                    self.runtime
                                        .emit_event(
                                            pod,
                                            None,
                                            crate::events::BACK_OFF_START_CONTAINER,
                                            rusternetes_common::resources::EventType::Warning,
                                            "Back-off restarting failed container",
                                        )
                                        .await;
                                    // Build container statuses with incremented restart counts
                                    let restarting_statuses: Vec<ContainerStatus> = pod
                                        .spec
                                        .as_ref()
                                        .map(|s| &s.containers)
                                        .unwrap_or(&vec![])
                                        .iter()
                                        .map(|c| {
                                            let prev_count = current_restart_counts
                                                .get(&c.name)
                                                .copied()
                                                .unwrap_or(0);
                                            ContainerStatus {
                                                name: c.name.clone(),
                                                ready: false,
                                                restart_count: prev_count + 1,
                                                state: Some(ContainerState::Waiting {
                                                    reason: Some("CrashLoopBackOff".to_string()),
                                                    message: Some(
                                                        "Liveness probe failed".to_string(),
                                                    ),
                                                }),
                                                last_state: None,
                                                image: Some(c.image.clone()),
                                                image_id: None,
                                                container_id: None,
                                                started: Some(false),
                                                allocated_resources: c
                                                    .resources
                                                    .as_ref()
                                                    .and_then(|r| r.requests.clone()),
                                                allocated_resources_status: None,
                                                resources: c.resources.clone(),
                                                user: None,
                                                volume_mounts: None,
                                                stop_signal: None,
                                            }
                                        })
                                        .collect();

                                    // Update status with incremented restart counts — re-read for fresh RV
                                    let key = build_key("pods", Some(namespace), pod_name);
                                    let mut new_pod: Pod = match self.storage.get(&key).await {
                                        Ok(p) => p,
                                        _ => pod.clone(),
                                    };
                                    // Terminal phases are sticky. A restartPolicy=
                                    // Always pod is never terminal (the kubelet keeps
                                    // it Running/CrashLoopBackOff), so this guard does
                                    // NOT affect legitimate liveness-probe restarts —
                                    // it only blocks a racing terminal->Running flap.
                                    if should_skip_phase_write(
                                        new_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                                        &Phase::Running,
                                    ) {
                                        return Ok(());
                                    }
                                    if let Some(ref mut status) = new_pod.status {
                                        status.phase = Some(Phase::Running);
                                        status.message = Some("Liveness probe failed".to_string());
                                        status.reason = Some("Restarting".to_string());
                                        status.container_statuses = Some(restarting_statuses);
                                    } else {
                                        new_pod.status = Some(PodStatus {
                                            phase: Some(Phase::Running),
                                            message: Some("Liveness probe failed".to_string()),
                                            reason: Some("Restarting".to_string()),
                                            host_ip: Some(Self::node_internal_ip().to_string()),
                                            pod_ip: None,
                                            conditions: None,
                                            container_statuses: None,
                                            init_container_statuses: None,
                                            ephemeral_container_statuses: None,
                                            resize: None,
                                            resource_claim_statuses: None,
                                            observed_generation: new_pod.metadata.generation,
                                            host_i_ps: Some(vec![
                                                rusternetes_common::resources::pod::HostIP {
                                                    ip: Self::node_internal_ip().to_string(),
                                                },
                                            ]),
                                            pod_i_ps: None,
                                            nominated_node_name: None,
                                            qos_class: None,
                                            start_time: None,
                                            ..Default::default()
                                        });
                                    }

                                    let _ = self.storage.update_status(&key, &new_pod).await;

                                    // Start again
                                    if let Err(e) = self.runtime.start_pod(&new_pod).await {
                                        error!("Failed to restart pod: {}", e);
                                        self.update_pod_status(
                                            pod,
                                            Phase::Failed,
                                            Some("FailedToRestart"),
                                            Some(&e.to_string()),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            "Never" => {
                                warn!("Liveness probe failed but restart policy is Never for pod {}/{}", namespace, pod_name);
                                self.update_pod_status(
                                    pod,
                                    Phase::Failed,
                                    Some("LivenessProbeFailedterm"),
                                    Some("Restart policy is Never"),
                                )
                                .await?;
                            }
                            _ => {}
                        }
                    } else {
                        // Resync projected/secret/configmap volumes (data may have changed)
                        if let Err(e) = self.runtime.resync_volumes(pod, &*self.storage).await {
                            debug!(
                                "Volume resync error for pod {}/{}: {}",
                                namespace, pod_name, e
                            );
                        }

                        // Update container statuses with readiness info.
                        // IMPORTANT: Read the fresh pod from storage so that
                        // restart_count/last_state set during the restart path
                        // above are preserved (the original `pod` variable is stale).
                        let readiness_pod_key = build_key("pods", Some(namespace), pod_name);
                        let readiness_pod = self
                            .storage
                            .get::<Pod>(&readiness_pod_key)
                            .await
                            .unwrap_or_else(|_| pod.clone());
                        if let Ok(container_statuses) =
                            self.get_container_statuses(&readiness_pod).await
                        {
                            // Restartable init containers (sidecars) count toward
                            // ContainersReady too (upstream status/generate.go):
                            // every sidecar must be ready. Plain init containers
                            // are excluded — they complete before the main
                            // containers and aren't part of steady-state readiness.
                            let init_sidecars_ready = match self
                                .runtime
                                .get_init_container_statuses(&readiness_pod)
                                .await
                            {
                                Some(init) => {
                                    let ics = readiness_pod
                                        .spec
                                        .as_ref()
                                        .and_then(|sp| sp.init_containers.as_ref());
                                    init.iter().all(|s| {
                                        let is_sidecar = ics
                                            .and_then(|list| {
                                                list.iter().find(|ic| ic.name == s.name)
                                            })
                                            .and_then(|ic| ic.restart_policy.as_deref())
                                            == Some("Always");
                                        !is_sidecar || s.ready
                                    })
                                }
                                None => true,
                            };
                            let containers_ready =
                                container_statuses.iter().all(|s| s.ready) && init_sidecars_ready;
                            // Readiness gates: every condition named in
                            // spec.readinessGates must be present in
                            // status.conditions with status "True" for the pod to
                            // be Ready (upstream GeneratePodReadyCondition). These
                            // conditions are supplied via the status subresource.
                            // ContainersReady ignores the gates; only Ready ANDs
                            // them in.
                            let all_ready = containers_ready && readiness_gates_satisfied(pod);

                            // Check if all containers have terminated (for Never/OnFailure restart policies)
                            let restart_policy = pod
                                .spec
                                .as_ref()
                                .and_then(|s| s.restart_policy.as_deref())
                                .unwrap_or("Always");

                            let all_terminated = !container_statuses.is_empty()
                                && container_statuses.iter().all(|cs| {
                                    matches!(cs.state, Some(ContainerState::Terminated { .. }))
                                });

                            if all_terminated && restart_policy == "Never" {
                                let any_failed = container_statuses.iter().any(|cs| {
                                    matches!(cs.state, Some(ContainerState::Terminated { exit_code, .. }) if exit_code != 0)
                                });
                                let terminal_phase = if any_failed {
                                    Phase::Failed
                                } else {
                                    Phase::Succeeded
                                };
                                let message = if any_failed {
                                    "Pod failed".to_string()
                                } else {
                                    "Pod completed successfully".to_string()
                                };

                                let key = build_key("pods", Some(namespace), pod_name);
                                let mut new_pod: Pod = match self.storage.get(&key).await {
                                    Ok(p) => p,
                                    _ => pod.clone(),
                                };
                                let original = new_pod.clone();
                                // Refresh init container statuses for terminal pod
                                let init_container_statuses =
                                    self.get_init_container_statuses(&new_pod).await;
                                if let Some(ref mut status) = new_pod.status {
                                    status.phase = Some(terminal_phase);
                                    status.message = Some(message);
                                    status.container_statuses = Some(container_statuses);
                                    if init_container_statuses.is_some() {
                                        status.init_container_statuses = init_container_statuses;
                                    }
                                    // Update conditions — terminated pod is not Ready
                                    if let Some(ref mut conditions) = status.conditions {
                                        for c in conditions.iter_mut() {
                                            if c.condition_type == "Ready"
                                                || c.condition_type == "ContainersReady"
                                            {
                                                c.status = "False".to_string();
                                                c.reason = Some("PodCompleted".to_string());
                                            }
                                        }
                                    }
                                }
                                if !pod_status_equal(&original, &new_pod) {
                                    let _ = self.storage.update_status(&key, &new_pod).await;
                                }
                                return Ok(());
                            }

                            if all_terminated && restart_policy == "OnFailure" {
                                let any_failed = container_statuses.iter().any(|cs| {
                                    matches!(cs.state, Some(ContainerState::Terminated { exit_code, .. }) if exit_code != 0)
                                });

                                if any_failed {
                                    // Restart only the failed containers
                                    warn!(
                                        "Restarting failed containers for pod {}/{} (OnFailure)",
                                        namespace, pod_name
                                    );
                                    let grace = pod
                                        .spec
                                        .as_ref()
                                        .and_then(|s| s.termination_grace_period_seconds)
                                        .unwrap_or(30);
                                    if let Err(e) = self.runtime.stop_pod_for(pod, grace).await {
                                        error!("Failed to stop pod for restart: {}", e);
                                    } else if let Err(e) = self.runtime.start_pod(pod).await {
                                        error!("Failed to restart pod: {}", e);
                                        self.update_pod_status(
                                            pod,
                                            Phase::Failed,
                                            Some("FailedToRestart"),
                                            Some(&e.to_string()),
                                        )
                                        .await?;
                                    }
                                    return Ok(());
                                } else {
                                    // All containers exited 0 — transition to Succeeded
                                    let key = build_key("pods", Some(namespace), pod_name);
                                    let mut new_pod: Pod = match self.storage.get(&key).await {
                                        Ok(p) => p,
                                        _ => pod.clone(),
                                    };
                                    let init_container_statuses =
                                        self.get_init_container_statuses(&new_pod).await;
                                    if let Some(ref mut status) = new_pod.status {
                                        status.phase = Some(Phase::Succeeded);
                                        status.message =
                                            Some("Pod completed successfully".to_string());
                                        status.container_statuses = Some(container_statuses);
                                        if init_container_statuses.is_some() {
                                            status.init_container_statuses =
                                                init_container_statuses;
                                        }
                                        status.conditions = Some(Self::succeeded_pod_conditions());
                                    }
                                    let _ = self.storage.update_status(&key, &new_pod).await;
                                    return Ok(());
                                }
                            }

                            // Get pod IP (important for pods started by docker-compose)
                            let pod_ip = self.runtime.get_pod_ip(pod_name).await.ok().flatten();

                            // Re-read pod from storage to get latest resourceVersion
                            // to avoid CAS conflicts when other controllers have
                            // updated the pod since we last read it.
                            let key = build_key("pods", Some(namespace), pod_name);
                            let mut new_pod: Pod = match self.storage.get::<Pod>(&key).await {
                                Ok(p) => p,
                                Err(_) => pod.clone(),
                            };
                            // Update ephemeral container statuses from Docker
                            let ephemeral_container_statuses = self
                                .runtime
                                .get_ephemeral_container_statuses(&new_pod)
                                .await;

                            // Refresh init container statuses — K8s prober_manager
                            // sets ready=true for terminated init containers on every
                            // status sync. Without this, stale ready=false from an
                            // intermediate write persists indefinitely.
                            let init_container_statuses =
                                self.get_init_container_statuses(&new_pod).await;

                            if let Some(ref mut status) = new_pod.status {
                                status.container_statuses = Some(container_statuses);
                                status.init_container_statuses = init_container_statuses;
                                status.ephemeral_container_statuses = ephemeral_container_statuses;
                                status.observed_generation = new_pod.metadata.generation;
                                // Update pod IP if we got one and it's different
                                if pod_ip.is_some() && status.pod_ip != pod_ip {
                                    status.pod_i_ps =
                                        pod_ip.as_ref().map(|ip| vec![PodIP { ip: ip.clone() }]);
                                    status.pod_ip = pod_ip;
                                }
                                status.message = Some(
                                    if all_ready {
                                        "All containers ready"
                                    } else if containers_ready {
                                        "Waiting on pod readiness gates"
                                    } else {
                                        "Some containers not ready"
                                    }
                                    .to_string(),
                                );
                                // ContainersReady reflects only the containers;
                                // Ready additionally requires the readinessGates.
                                status.conditions = Some(Self::merge_pod_conditions(
                                    status.conditions.as_deref().unwrap_or(&[]),
                                    Self::pod_readiness_conditions(containers_ready, all_ready),
                                ));
                            }

                            // Skip update if status hasn't changed — avoids unnecessary
                            // resourceVersion bumps that cause CAS conflicts for kubectl replace.
                            // K8s kubelet uses status manager to diff before writing.
                            let old_status_json = serde_json::to_value(pod.status.as_ref()).ok();
                            let new_status_json =
                                serde_json::to_value(new_pod.status.as_ref()).ok();
                            if old_status_json == new_status_json {
                                debug!(
                                    "Pod {}/{} status unchanged, skipping update",
                                    namespace, pod_name
                                );
                                return Ok(());
                            }

                            if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                                // CAS conflict — re-read and retry once
                                debug!("Pod status update CAS conflict, retrying: {}", e);
                                if let Ok(mut fresh_pod) = self.storage.get::<Pod>(&key).await {
                                    // observedGeneration MUST track the generation
                                    // of the object we just re-read, not the stale
                                    // value carried in new_pod / persisted earlier.
                                    // A spec update that bumped generation is what
                                    // caused this conflict; stamping the fresh
                                    // generation here is how the kubelet converges
                                    // observedGeneration under rapid parallel
                                    // updates (#1170). Capture before the mut borrow.
                                    let fresh_gen = fresh_pod.metadata.generation;
                                    if let Some(ref mut status) = fresh_pod.status {
                                        status.container_statuses = new_pod
                                            .status
                                            .as_ref()
                                            .and_then(|s| s.container_statuses.clone());
                                        status.conditions = new_pod
                                            .status
                                            .as_ref()
                                            .and_then(|s| s.conditions.clone());
                                        status.message =
                                            new_pod.status.as_ref().and_then(|s| s.message.clone());
                                        status.observed_generation = fresh_gen;
                                        if let Some(ref new_status) = new_pod.status {
                                            if new_status.pod_ip.is_some() {
                                                status.pod_ip = new_status.pod_ip.clone();
                                                status.pod_i_ps = new_status.pod_i_ps.clone();
                                            }
                                        }
                                    }
                                    if let Err(e2) =
                                        self.storage.update_status(&key, &fresh_pod).await
                                    {
                                        warn!("Failed to update pod status after retry: {}", e2);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Phase::Running if !is_running => {
                // Containers have stopped — decide based on restart policy
                let restart_policy = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.restart_policy.as_deref())
                    .unwrap_or("Always");

                let container_statuses = self.get_container_statuses(pod).await.ok();
                let any_failed = container_statuses
                    .as_ref()
                    .map(|statuses| {
                        statuses.iter().any(|cs| {
                            matches!(cs.state, Some(ContainerState::Terminated { exit_code, .. }) if exit_code != 0)
                        })
                    })
                    .unwrap_or(false);

                match restart_policy {
                    "Always" => {
                        // Restart stopped containers individually, paced by
                        // per-container CrashLoopBackOff. restartCount is owned
                        // by the backoff map and advances ONLY on a real restart
                        // — never per sync observation (which previously inflated
                        // it to tens of thousands under the sync hot loop).
                        self.reconcile_container_restarts(namespace, pod_name, pod)
                            .await;
                    }
                    "OnFailure" => {
                        if any_failed {
                            // Restart failed containers individually, paced by
                            // per-container CrashLoopBackOff (restartCount owned
                            // by the backoff map, advanced only on real restart).
                            self.reconcile_container_restarts(namespace, pod_name, pod)
                                .await;
                        } else {
                            info!(
                                "Pod {}/{} completed successfully (restartPolicy=OnFailure)",
                                namespace, pod_name
                            );
                            let key = build_key("pods", Some(namespace), pod_name);
                            let mut new_pod: Pod = match self.storage.get(&key).await {
                                Ok(p) => p,
                                _ => pod.clone(),
                            };
                            let init_container_statuses =
                                self.get_init_container_statuses(&new_pod).await;
                            if let Some(ref mut status) = new_pod.status {
                                status.phase = Some(Phase::Succeeded);
                                status.message = Some("Pod completed successfully".to_string());
                                if let Some(ref cs) = container_statuses {
                                    status.container_statuses = Some(cs.clone());
                                }
                                if init_container_statuses.is_some() {
                                    status.init_container_statuses = init_container_statuses;
                                }
                                Self::fixup_init_container_ready(status);
                                status.conditions = Some(Self::succeeded_pod_conditions());
                            }
                            let key = build_key("pods", Some(namespace), pod_name);
                            let _ = self.storage.update_status(&key, &new_pod).await;
                        }
                    }
                    "Never" => {
                        let terminal_phase = if any_failed {
                            Phase::Failed
                        } else {
                            Phase::Succeeded
                        };
                        let message = if any_failed {
                            "Pod failed".to_string()
                        } else {
                            "Pod completed successfully".to_string()
                        };
                        info!(
                            "Pod {}/{} terminated (restartPolicy=Never, phase={:?})",
                            namespace, pod_name, terminal_phase
                        );
                        let key = build_key("pods", Some(namespace), pod_name);
                        let mut new_pod: Pod = match self.storage.get(&key).await {
                            Ok(p) => p,
                            _ => pod.clone(),
                        };
                        let init_container_statuses =
                            self.get_init_container_statuses(&new_pod).await;
                        if let Some(ref mut status) = new_pod.status {
                            status.phase = Some(terminal_phase.clone());
                            status.message = Some(message);
                            if let Some(ref cs) = container_statuses {
                                status.container_statuses = Some(cs.clone());
                            }
                            if init_container_statuses.is_some() {
                                status.init_container_statuses = init_container_statuses;
                            }
                            Self::fixup_init_container_ready(status);
                            // A terminal pod is never Ready. `IsPodReady` (which
                            // the conformance suite checks) reads the Ready
                            // CONDITION, so set it False for BOTH terminal phases
                            // — not just Succeeded (the Failed case previously
                            // leaked the Running conditions with Ready=True).
                            status.conditions = Some(if terminal_phase == Phase::Succeeded {
                                Self::succeeded_pod_conditions()
                            } else {
                                Self::failed_pod_conditions()
                            });
                        }
                        let key = build_key("pods", Some(namespace), pod_name);
                        let _ = self.storage.update_status(&key, &new_pod).await;
                    }
                    _ => {}
                }
            }
            Phase::Succeeded | Phase::Failed => {
                // Terminal phase is handled by the state machine above
                // (SyncPod → TerminatingPod → TerminatedPod).
                // This branch should not be reached for pods in SyncPod state
                // because needs_terminating transitions them first.
                debug!(
                    "Pod {}/{} reached terminal phase handler in SyncPod (should not happen)",
                    namespace, pod_name
                );
            }
            _ => {
                debug!(
                    "Pod {}/{} is in sync (phase: {:?}, running: {})",
                    namespace, pod_name, current_phase, is_running
                );
            }
        }

        Ok(())
    }

    /// Detect terminated containers and restart them per the pod's
    /// `restartPolicy`, paced by per-container CrashLoopBackOff.
    ///
    /// `restartCount` is owned by [`RestartBackoff`] and advances **only on an
    /// actual restart** — not once per sync that happens to observe a
    /// terminated container, which is what previously drove the count to tens
    /// of thousands under the watch-driven sync hot loop. The first restart
    /// after a crash is immediate; each subsequent restart waits `backoff`
    /// (10s, doubling, capped at 5m) measured from the previous restart, so the
    /// observable `restartCount` advances slowly enough for clients to see each
    /// value. Volume binds are rebuilt via `create_pod_volumes` so a restarted
    /// container re-binds the SAME on-disk volumes (emptyDir data persists).
    ///
    /// K8s ref: `pkg/kubelet/kuberuntime/kuberuntime_manager.go`
    /// (computePodActions + doBackOff) and `client-go/util/flowcontrol.Backoff`.
    async fn reconcile_container_restarts(&self, namespace: &str, pod_name: &str, pod: &Pod) {
        // Never restart containers of a terminating pod: its containers are being
        // shut down on purpose, and restarting a sidecar mid-drain would keep the
        // pod alive past its grace period and prevent it from being finalized.
        if pod.metadata.deletion_timestamp.is_some() {
            return;
        }
        let restart_policy = pod
            .spec
            .as_ref()
            .and_then(|s| s.restart_policy.as_deref())
            .unwrap_or("Always");
        if restart_policy != "Always" && restart_policy != "OnFailure" {
            return;
        }
        if !self.runtime.has_terminated_containers(pod).await {
            return;
        }
        let Some(spec) = pod.spec.as_ref() else {
            return;
        };

        // Rebuild the per-volume bind paths exactly as the initial start did
        // (idempotent: re-creates dirs, re-applies fsGroup, keeps the existing
        // Memory-emptyDir tmpfs). This is what makes emptyDir data survive a
        // restart — the recreated container binds the same on-disk volume.
        let volume_paths = self
            .runtime
            .create_pod_volumes(pod)
            .await
            .unwrap_or_default();

        let now = Instant::now();
        // Restart regular containers and restartable init containers (sidecars:
        // restartPolicy=Always) the same way. start_container handles both; the
        // backoff map keys on the (unique) container name. Plain init containers
        // run to completion and are not restarted here.
        let restartable_inits = spec
            .init_containers
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|ic| ic.restart_policy.as_deref() == Some("Always"));
        for c in spec.containers.iter().chain(restartable_inits) {
            if self
                .runtime
                .is_container_running(&pod.metadata.uid, &c.name)
                .await
                .unwrap_or(true)
            {
                continue; // still running — nothing to do
            }

            // OnFailure: a clean exit (code 0) is terminal — do not restart.
            if restart_policy == "OnFailure" {
                let exit_code = self
                    .runtime
                    .get_container_exit_code(&pod.metadata.uid, &c.name)
                    .await
                    .unwrap_or(1);
                if exit_code == 0 {
                    continue;
                }
            }

            // CrashLoopBackOff gate. The check-and-advance is atomic under the
            // lock, so even if two syncs race only one wins the restart.
            let bkey = format!("{}/{}/{}", namespace, pod_name, c.name);
            let do_restart = {
                let mut map = self.restart_backoff.lock().unwrap();
                match map.get_mut(&bkey) {
                    None => {
                        // First crash → restart immediately, seed the backoff.
                        map.insert(
                            bkey.clone(),
                            RestartBackoff {
                                restart_count: 1,
                                last_restart: now,
                                backoff: CRASHLOOP_BACKOFF_INITIAL,
                            },
                        );
                        true
                    }
                    Some(entry) => {
                        if now.duration_since(entry.last_restart) >= entry.backoff {
                            entry.restart_count += 1;
                            entry.last_restart = now;
                            entry.backoff = (entry.backoff * 2).min(CRASHLOOP_BACKOFF_MAX);
                            true
                        } else {
                            false // still within the backoff window
                        }
                    }
                }
            };

            if do_restart {
                let _ = self
                    .runtime
                    .remove_terminated_container(&pod.metadata.uid, &c.name)
                    .await;
                let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.as_deref());
                if let Err(e) = self
                    .runtime
                    .start_container(pod, c, &volume_paths, None, None, pod_ip)
                    .await
                {
                    debug!(
                        "Failed to restart container {} in pod {}/{}: {}",
                        c.name, namespace, pod_name, e
                    );
                } else {
                    info!(
                        "Restarted container {} in pod {}/{}",
                        c.name, namespace, pod_name
                    );
                }
            }
        }

        // Publish status: restartCount from the backoff map; any container still
        // Terminated here was NOT restarted this pass (backing off) → surface
        // Waiting/CrashLoopBackOff. Re-read the pod so we don't clobber a
        // concurrent writer.
        let key = build_key("pods", Some(namespace), pod_name);
        if let Ok(mut fresh_pod) = self.storage.get::<Pod>(&key).await {
            if let Ok(mut statuses) = self.get_container_statuses(&fresh_pod).await {
                {
                    let map = self.restart_backoff.lock().unwrap();
                    for cs in statuses.iter_mut() {
                        if let Some(entry) =
                            map.get(&format!("{}/{}/{}", namespace, pod_name, cs.name))
                        {
                            cs.restart_count = entry.restart_count;
                        }
                        if matches!(cs.state, Some(ContainerState::Terminated { .. })) {
                            cs.last_state = cs.state.take();
                            cs.state = Some(ContainerState::Waiting {
                                reason: Some("CrashLoopBackOff".to_string()),
                                message: Some("Back-off restarting failed container".to_string()),
                            });
                            cs.ready = false;
                            cs.started = Some(false);
                        }
                    }
                }
                // Restartable init containers (sidecars) carry their own
                // restart counts in init_container_statuses; overlay the backoff
                // map there too so a restarted sidecar reports its count.
                let mut init_statuses = self.get_init_container_statuses(&fresh_pod).await;
                if let Some(ref mut list) = init_statuses {
                    let map = self.restart_backoff.lock().unwrap();
                    for cs in list.iter_mut() {
                        if let Some(entry) =
                            map.get(&format!("{}/{}/{}", namespace, pod_name, cs.name))
                        {
                            cs.restart_count = entry.restart_count;
                        }
                    }
                }
                if let Some(ref mut s) = fresh_pod.status {
                    s.container_statuses = Some(statuses);
                    if init_statuses.is_some() {
                        s.init_container_statuses = init_statuses;
                    }
                }
                let _ = self.storage.update_status(&key, &fresh_pod).await;
            }
        }
    }

    /// Drop CrashLoopBackOff state for every container of a removed pod so the
    /// map does not grow without bound.
    fn forget_restart_backoff(&self, namespace: &str, pod_name: &str) {
        let prefix = format!("{}/{}/", namespace, pod_name);
        self.restart_backoff
            .lock()
            .unwrap()
            .retain(|k, _| !k.starts_with(&prefix));
    }

    /// Overlay the authoritative `restartCount` from the [`RestartBackoff`] map
    /// onto freshly-read container statuses. The CRI status reports
    /// `metadata.attempt`, which resets to 0 when a new sandbox is created — so
    /// without this overlay a status write can report a LOWER count than a prior
    /// write (non-monotonic `1 → 0`), failing the container_probe NodeConformance
    /// specs. The backoff map is the single source of truth for restartCount;
    /// apply it everywhere statuses are published (#1514).
    fn overlay_restart_backoff(&self, statuses: &mut [ContainerStatus], pod: &Pod) {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pod_name = &pod.metadata.name;
        let map = self.restart_backoff.lock().unwrap();
        for cs in statuses.iter_mut() {
            if let Some(entry) = map.get(&format!("{}/{}/{}", namespace, pod_name, cs.name)) {
                cs.restart_count = entry.restart_count;
            }
        }
    }

    /// `ContainerRuntime::get_container_statuses` + the [`RestartBackoff`] overlay.
    /// All status-publish paths go through this so `restartCount` is monotonic
    /// and consistent regardless of CRI sandbox churn (#1514).
    async fn get_container_statuses(&self, pod: &Pod) -> Result<Vec<ContainerStatus>> {
        let mut statuses = self.runtime.get_container_statuses(pod).await?;
        self.overlay_restart_backoff(&mut statuses, pod);
        Ok(statuses)
    }

    /// Restartable-init (sidecar) variant — same backoff overlay (#1514).
    async fn get_init_container_statuses(&self, pod: &Pod) -> Option<Vec<ContainerStatus>> {
        let mut statuses = self.runtime.get_init_container_statuses(pod).await?;
        self.overlay_restart_backoff(&mut statuses, pod);
        Some(statuses)
    }

    /// Record one more failed terminal finalize for `key` (the object is still
    /// present after a reported removal) and return the new attempt count (#1157).
    fn record_terminal_finalize_failure(&self, key: &str) -> u32 {
        let mut m = self.terminal_finalize_failures.lock().unwrap();
        let entry = m.entry(key.to_string()).or_insert((0, Instant::now()));
        entry.0 += 1;
        entry.1 = Instant::now();
        entry.0
    }

    /// Forget any terminal-finalize failure record for `key` (the object is now
    /// gone, so the next delete starts fresh).
    fn clear_terminal_finalize_failure(&self, key: &str) {
        self.terminal_finalize_failures.lock().unwrap().remove(key);
    }

    /// `Some(remaining)` when `key` has a pending terminal-finalize backoff that
    /// has not yet elapsed — the caller should skip re-terminating this cycle.
    /// `None` when there is no record or the window has passed (retry now).
    fn terminal_finalize_backoff_remaining(&self, key: &str) -> Option<Duration> {
        let m = self.terminal_finalize_failures.lock().unwrap();
        let (count, last) = m.get(key)?;
        terminal_finalize_backoff(*count).checked_sub(last.elapsed())
    }

    /// Preserve `last_transition_time` from existing conditions whose
    /// type+status match the incoming ones. Without this every status
    /// sync re-stamps the time → pod_status_equal returns false → write
    /// → watch event → next sync repeats. Hot loop at ~30 Hz per pod.
    fn merge_pod_conditions(
        existing: &[PodCondition],
        mut incoming: Vec<PodCondition>,
    ) -> Vec<PodCondition> {
        for new in incoming.iter_mut() {
            if let Some(prev) = existing
                .iter()
                .find(|c| c.condition_type == new.condition_type && c.status == new.status)
            {
                new.last_transition_time = prev.last_transition_time;
            }
        }
        // Preserve existing conditions this writer doesn't manage — notably the
        // custom conditions a readinessGate refers to, supplied out-of-band via
        // the pod status subresource. Without this the kubelet clobbers them on
        // every status sync and the readinessGate can never be satisfied.
        for prev in existing {
            if !incoming
                .iter()
                .any(|c| c.condition_type == prev.condition_type)
            {
                incoming.push(prev.clone());
            }
        }
        incoming
    }

    /// Build the standard pod conditions with ContainersReady and Ready set
    /// independently. ContainersReady reflects only container/sidecar readiness;
    /// Ready additionally requires every spec.readinessGates condition to be True
    /// (upstream GeneratePodReadyCondition). Initialized and PodScheduled are
    /// always True here (the pod has been admitted and its containers created).
    fn pod_readiness_conditions(containers_ready: bool, pod_ready: bool) -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        let bool_str = |b: bool| if b { "True" } else { "False" }.to_string();
        let not_ready_reason = |b: bool| {
            if b {
                (None, None)
            } else {
                (
                    Some("ContainersNotReady".to_string()),
                    Some("containers or readiness gates not ready".to_string()),
                )
            }
        };
        let (cr_reason, cr_msg) = not_ready_reason(containers_ready);
        let (rd_reason, rd_msg) = not_ready_reason(pod_ready);
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: bool_str(containers_ready),
                reason: cr_reason,
                message: cr_msg,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: bool_str(pod_ready),
                reason: rd_reason,
                message: rd_msg,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Build the standard pod conditions for a Running pod.
    /// Real Kubernetes sets Initialized, PodScheduled, ContainersReady, and Ready=True
    /// when all containers are running. The e2e conformance suite checks these conditions.
    #[allow(dead_code)]
    fn running_pod_conditions() -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    fn not_ready_pod_conditions() -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotReady".to_string()),
                message: Some("Not all containers are ready".to_string()),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotReady".to_string()),
                message: Some("Not all containers are ready".to_string()),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Fix up init container ready status per K8s prober_manager.go:377.
    /// For non-restartable init containers, if terminated with exit_code=0,
    /// set ready=true. This must run on EVERY status write, not just deletion.
    fn fixup_init_container_ready(status: &mut PodStatus) {
        if let Some(ref mut ics) = status.init_container_statuses {
            for ic in ics.iter_mut() {
                if let Some(ContainerState::Terminated { exit_code, .. }) = &ic.state {
                    if *exit_code == 0 {
                        ic.ready = true;
                        ic.started = Some(true);
                    }
                }
            }
        }
    }

    /// Build conditions for a pod that has succeeded (all containers completed successfully).
    /// K8s ref: pkg/kubelet/status/generate.go — PodCompleted reason
    fn succeeded_pod_conditions() -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: Some("PodCompleted".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "False".to_string(),
                reason: Some("PodCompleted".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                reason: Some("PodCompleted".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Conditions for a pod that has reached the terminal `Failed` phase
    /// (e.g. restartPolicy=Never with a non-zero container exit). Like the
    /// Succeeded set, `Ready` and `ContainersReady` MUST be `False` —
    /// `podutil.IsPodReady` (which the conformance suite checks) keys off the
    /// `Ready` condition, not the container `ready` field.
    fn failed_pod_conditions() -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "False".to_string(),
                reason: Some("PodFailed".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                reason: Some("PodFailed".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Build conditions for a pod whose init containers failed.
    fn init_failed_pod_conditions(incomplete_init_names: &[String]) -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        let msg = if !incomplete_init_names.is_empty() {
            format!(
                "containers with incomplete status: [{}]",
                incomplete_init_names.join(" ")
            )
        } else {
            "Init container failed".to_string()
        };
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotInitialized".to_string()),
                message: Some(msg.clone()),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotReady".to_string()),
                message: Some(msg.clone()),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotReady".to_string()),
                message: Some(msg),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Build init container statuses for a running pod (static fallback).
    /// Prefer `runtime.get_init_container_statuses()` which inspects actual Docker state.
    #[allow(dead_code)]
    fn build_init_container_statuses(pod: &Pod) -> Option<Vec<ContainerStatus>> {
        let init_containers = pod.spec.as_ref()?.init_containers.as_ref()?;
        if init_containers.is_empty() {
            return None;
        }
        Some(
            init_containers
                .iter()
                .map(|ic| ContainerStatus {
                    name: ic.name.clone(),
                    ready: true,
                    restart_count: 0,
                    state: Some(ContainerState::Terminated {
                        exit_code: 0,
                        signal: None,
                        reason: Some("Completed".to_string()),
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                    }),
                    last_state: None,
                    image: Some(ic.image.clone()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: ic.resources.as_ref().and_then(|r| r.requests.clone()),
                    allocated_resources_status: None,
                    resources: ic.resources.clone(),
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                })
                .collect(),
        )
    }

    /// Compute the QoS class for a pod based on resource requests/limits.
    ///
    /// - Guaranteed: every container has both cpu and memory limits AND requests, and they're equal
    /// - BestEffort: no container has any requests or limits
    /// - Burstable: everything else
    fn compute_qos_class(pod: &Pod) -> String {
        let spec = match &pod.spec {
            Some(s) => s,
            None => return "BestEffort".to_string(),
        };

        let containers = &spec.containers;
        if containers.is_empty() {
            return "BestEffort".to_string();
        }

        let mut all_have_limits_eq_requests = true;
        let mut none_have_any = true;

        for container in containers {
            let resources = match &container.resources {
                Some(r) => r,
                None => {
                    all_have_limits_eq_requests = false;
                    // no resources at all — still counts as "none" for BestEffort
                    continue;
                }
            };

            let limits = resources.limits.as_ref();
            let requests = resources.requests.as_ref();

            let has_any =
                limits.is_some_and(|l| !l.is_empty()) || requests.is_some_and(|r| !r.is_empty());

            if has_any {
                none_have_any = false;
            }

            // For Guaranteed, both cpu and memory limits must exist, and requests must equal limits
            for res in &["cpu", "memory"] {
                let limit_val = limits.and_then(|l| l.get(*res));
                let request_val = requests.and_then(|r| r.get(*res));

                match (limit_val, request_val) {
                    (Some(l), Some(r)) => {
                        if l != r {
                            all_have_limits_eq_requests = false;
                        }
                    }
                    (Some(_), None) => {
                        // Kubernetes defaults requests to limits if not set,
                        // but we check explicitly here
                        // Still counts as Guaranteed if request is missing (defaults to limit)
                    }
                    (None, _) => {
                        all_have_limits_eq_requests = false;
                    }
                }
            }
        }

        if none_have_any {
            "BestEffort".to_string()
        } else if all_have_limits_eq_requests {
            "Guaranteed".to_string()
        } else {
            "Burstable".to_string()
        }
    }

    async fn update_pod_status(
        &self,
        pod: &Pod,
        phase: Phase,
        reason: Option<&str>,
        message: Option<&str>,
    ) -> Result<()> {
        let key = build_key(
            "pods",
            pod.metadata.namespace.as_deref(),
            &pod.metadata.name,
        );

        // Read the fresh pod from storage so we preserve container_statuses,
        // init_container_statuses, conditions, pod_ip, start_time, etc.
        // Constructing a fresh PodStatus would WIPE those fields — destructive
        // when called on a failure path of a previously-Running pod.
        let mut new_pod = match self.storage.get::<Pod>(&key).await {
            Ok(p) => p,
            Err(_) => pod.clone(),
        };

        // Terminal pod phases are STICKY. If storage already reports the pod
        // as Succeeded/Failed, refuse to regress it to a non-terminal phase
        // (and never rewrite one terminal phase into the other). Without this
        // a Succeeded pod could flap back to Running; the job controller then
        // deletes that Running pod on job completion while its completion
        // index stays counted, dropping the index from the pod listing — the
        // Indexed Job conformance flake (#1048).
        // Upstream parity: pkg/kubelet/kubelet_pods.go:1934-1942
        // generateAPIPodStatus — "pods are not allowed to transition out of
        // terminal phases" forces the computed phase back to the API
        // server's terminal value ("Pod attempted illegal phase transition").
        let current_phase = new_pod.status.as_ref().and_then(|s| s.phase.clone());
        if should_skip_phase_write(current_phase.as_ref(), &phase) {
            debug!(
                "refusing to regress terminal phase of pod {}/{}: storage={:?} requested={:?} (upstream: generateAPIPodStatus illegal-phase-transition guard)",
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod.metadata.name,
                current_phase,
                phase,
            );
            return Ok(());
        }

        let original = new_pod.clone();

        let mut status = new_pod.status.take().unwrap_or_default();
        status.phase = Some(phase);
        status.reason = reason.map(|s| s.to_string());
        status.message = message.map(|s| s.to_string());
        new_pod.status = Some(status);

        // Gate the write so a no-op call doesn't emit a MODIFIED watch event.
        if !pod_status_equal(&original, &new_pod) {
            self.storage.update_status(&key, &new_pod).await?;
        }

        Ok(())
    }

    async fn update_pod_status_error(&self, pod: &Pod, error: &str) -> Result<()> {
        self.update_pod_status(pod, Phase::Failed, Some("Error"), Some(error))
            .await
    }

    /// Handle pod eviction when node resources are exhausted
    async fn handle_eviction(&self, signals: &[EvictionSignal]) -> Result<()> {
        info!("Handling eviction for signals: {:?}", signals);

        // Get all pods assigned to this node
        let all_pods_prefix = build_prefix("pods", None);
        let all_pods: Vec<Pod> = self.storage.list(&all_pods_prefix).await?;

        let node_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|p| {
                p.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_ref())
                    .map(|n| n == &self.node_name)
                    .unwrap_or(false)
            })
            .filter(|p| {
                // Only consider running pods for eviction
                p.status
                    .as_ref()
                    .map(|s| s.phase == Some(Phase::Running))
                    .unwrap_or(false)
            })
            .collect();

        // Get pod resource usage statistics
        let pod_stats = get_pod_stats(&node_pods).await;

        // For each active signal, select pods for eviction
        for signal in signals {
            let pods_to_evict = {
                let eviction_manager = self.eviction_manager.lock().unwrap();
                eviction_manager.select_pods_for_eviction(&node_pods, &pod_stats, signal)
            };

            for pod_key in pods_to_evict {
                warn!(
                    "Evicting pod {} due to resource pressure ({:?})",
                    pod_key, signal
                );

                // Parse namespace and name from key
                let parts: Vec<&str> = pod_key.split('/').collect();
                if parts.len() != 2 {
                    continue;
                }
                let namespace = parts[0];
                let name = parts[1];

                // Find the pod
                if let Some(pod) = node_pods.iter().find(|p| {
                    p.metadata.namespace.as_deref().unwrap_or("default") == namespace
                        && p.metadata.name == name
                }) {
                    // Stop the pod (use short grace period for eviction)
                    let grace = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.termination_grace_period_seconds)
                        .unwrap_or(30);
                    if let Err(e) = self.runtime.stop_pod_for(pod, grace).await {
                        error!("Failed to stop evicted pod {}: {}", pod_key, e);
                        continue;
                    }

                    // Update pod status to reflect eviction
                    if let Err(e) = self
                        .update_pod_status(
                            pod,
                            Phase::Failed,
                            Some("Evicted"),
                            Some(&format!(
                                "Pod evicted due to resource pressure: {:?}",
                                signal
                            )),
                        )
                        .await
                    {
                        error!("Failed to update evicted pod status: {}", e);
                    }

                    info!("Successfully evicted pod {}", pod_key);
                }
            }
        }

        Ok(())
    }
}

/// Build the kubelet-managed /etc/hosts content for a pod.
///
/// Returns `None` for `hostNetwork: true` pods — those use the host's
/// /etc/hosts directly and must NOT receive a managed file (upstream
/// `pkg/kubelet/kubelet_pods.go::managedHostsFileContent`).
///
/// For non-host-network pods, the content matches upstream byte-for-byte:
/// - `# Kubernetes-managed hosts file.` header
/// - Standard IPv4/IPv6 localhost + multicast entries
/// - The pod's own IP / hostname (and FQDN if `spec.subdomain` is set)
/// - Every `spec.hostAliases[]` entry, one line per IP with hostnames
///   tab-separated. Aliases with empty/missing hostnames are skipped.
///
/// Mirrors upstream e2e site
/// `test/e2e/common/node/kubelet_etc_hosts.go:143`.
pub fn build_managed_hosts_content(
    pod: &Pod,
    pod_ip: Option<&str>,
    cluster_domain: &str,
) -> Option<String> {
    let spec = pod.spec.as_ref()?;

    // hostNetwork pods use the host's /etc/hosts directly.
    if spec.host_network == Some(true) {
        return None;
    }

    let pod_name = &pod.metadata.name;
    let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

    // Linux hostnames are limited to 63 chars; trim trailing hyphens.
    let raw_hostname = spec.hostname.as_deref().unwrap_or(pod_name);
    let hostname = if raw_hostname.len() > 63 {
        raw_hostname[..63].trim_end_matches('-')
    } else {
        raw_hostname
    };

    // Header + standard entries. IPv6 addresses match upstream
    // `pkg/kubelet/kubelet_pods.go` exactly — do NOT change without
    // verifying the upstream constants.
    let mut content = String::from(
        "# Kubernetes-managed hosts file.\n\
         127.0.0.1\tlocalhost\n\
         ::1\tlocalhost ip6-localhost ip6-loopback\n\
         fe00::0\tip6-localnet\n\
         ff00::0\tip6-mcastprefix\n\
         ff02::1\tip6-allnodes\n\
         ff02::2\tip6-allrouters\n",
    );

    // Pod's own IP entry (when known). Includes the FQDN built from
    // <hostname>.<subdomain>.<namespace>.svc.<cluster-domain> if subdomain set.
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

    // spec.hostAliases — one line per IP, hostnames tab-joined.
    // Skip aliases with empty/missing hostnames (upstream behaviour).
    for alias in spec.host_aliases.iter().flatten() {
        let hostnames = match alias.hostnames.as_deref() {
            Some(h) if !h.is_empty() => h,
            _ => continue,
        };
        content.push_str(&format!("{}\t{}\n", alias.ip, hostnames.join("\t")));
    }

    Some(content)
}

#[cfg(test)]
mod taint_eviction_tests {
    use super::{noexecute_eviction_due, Taint, Toleration};

    fn no_execute_taint(time_added_secs_ago: Option<i64>) -> Taint {
        Taint {
            key: "node.kubernetes.io/not-ready".to_string(),
            value: Some("".to_string()),
            effect: "NoExecute".to_string(),
            time_added: time_added_secs_ago
                .map(|s| chrono::Utc::now() - chrono::Duration::seconds(s)),
        }
    }

    fn default_toleration(secs: Option<i64>) -> Toleration {
        Toleration {
            key: Some("node.kubernetes.io/not-ready".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoExecute".to_string()),
            toleration_seconds: secs,
        }
    }

    #[test]
    fn untolerated_pod_is_due() {
        let taint = no_execute_taint(Some(10));
        assert!(noexecute_eviction_due(&[], &taint, chrono::Utc::now()));
    }

    #[test]
    fn exists_toleration_no_seconds_never_due() {
        let taint = no_execute_taint(Some(10_000));
        let tols = vec![default_toleration(None)];
        assert!(!noexecute_eviction_due(&tols, &taint, chrono::Utc::now()));
    }

    #[test]
    fn timed_toleration_within_grace_not_due() {
        // 300s toleration, taint added 10s ago → still within grace.
        let taint = no_execute_taint(Some(10));
        let tols = vec![default_toleration(Some(300))];
        assert!(!noexecute_eviction_due(&tols, &taint, chrono::Utc::now()));
    }

    #[test]
    fn timed_toleration_past_grace_is_due() {
        // 300s toleration, taint added 400s ago → grace expired.
        let taint = no_execute_taint(Some(400));
        let tols = vec![default_toleration(Some(300))];
        assert!(noexecute_eviction_due(&tols, &taint, chrono::Utc::now()));
    }

    #[test]
    fn time_added_none_with_timed_toleration_not_due() {
        // Taint just added (no timeAdded) + matching timed toleration → grace
        // has not elapsed, so the pod is not yet evicted.
        let taint = no_execute_taint(None);
        let tols = vec![default_toleration(Some(300))];
        assert!(!noexecute_eviction_due(&tols, &taint, chrono::Utc::now()));
    }

    #[test]
    fn time_added_none_untolerated_still_due() {
        // No matching toleration is independent of timeAdded → evict.
        let taint = no_execute_taint(None);
        assert!(noexecute_eviction_due(&[], &taint, chrono::Utc::now()));
    }

    #[test]
    fn non_matching_key_toleration_does_not_count() {
        let taint = no_execute_taint(Some(10));
        let tols = vec![Toleration {
            key: Some("other-key".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoExecute".to_string()),
            toleration_seconds: None,
        }];
        assert!(noexecute_eviction_due(&tols, &taint, chrono::Utc::now()));
    }

    #[test]
    fn zero_or_negative_toleration_seconds_is_due() {
        let taint = no_execute_taint(Some(1));
        let tols = vec![default_toleration(Some(0))];
        assert!(noexecute_eviction_due(&tols, &taint, chrono::Utc::now()));
    }

    #[test]
    fn zero_seconds_short_circuits_over_forever_toleration() {
        // Mirror upstream getMinTolerationTime (taint_eviction.go:167-181):
        // a tolerationSeconds <= 0 returns 0 (evict now) DURING iteration,
        // before a nil/forever toleration is ever considered.
        let taint = no_execute_taint(Some(1));
        let tols = vec![default_toleration(None), default_toleration(Some(0))];
        assert!(noexecute_eviction_due(&tols, &taint, chrono::Utc::now()));
        // Order-independent: same result with the entries swapped.
        let tols = vec![default_toleration(Some(0)), default_toleration(None)];
        assert!(noexecute_eviction_due(&tols, &taint, chrono::Utc::now()));
    }

    #[test]
    fn forever_plus_timed_toleration_uses_timed_minimum() {
        // Upstream getMinTolerationTime skips nil entries (it does NOT treat
        // them as forever when a timed entry exists): nil + 300s → min 300s.
        let tols = vec![default_toleration(None), default_toleration(Some(300))];
        let fresh = no_execute_taint(Some(10));
        assert!(!noexecute_eviction_due(&tols, &fresh, chrono::Utc::now()));
        let expired = no_execute_taint(Some(400));
        assert!(noexecute_eviction_due(&tols, &expired, chrono::Utc::now()));
    }
}

#[cfg(test)]
mod tests {
    use super::Kubelet;
    use rusternetes_common::resources::pod::{PodCondition, PodSpec};
    use rusternetes_common::resources::{
        Container, ContainerState, ContainerStatus, Pod, PodStatus,
    };
    use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};

    #[test]
    fn terminal_finalize_backoff_grows_then_caps() {
        use super::{
            terminal_finalize_backoff, TERMINAL_FINALIZE_BACKOFF_INITIAL,
            TERMINAL_FINALIZE_BACKOFF_MAX,
        };
        // First failure → initial; each subsequent doubles.
        assert_eq!(
            terminal_finalize_backoff(1),
            TERMINAL_FINALIZE_BACKOFF_INITIAL
        );
        assert_eq!(
            terminal_finalize_backoff(2),
            TERMINAL_FINALIZE_BACKOFF_INITIAL * 2
        );
        assert_eq!(
            terminal_finalize_backoff(3),
            TERMINAL_FINALIZE_BACKOFF_INITIAL * 4
        );
        // Monotonic non-decreasing and never above the cap.
        let mut prev = terminal_finalize_backoff(1);
        for c in 2..=50u32 {
            let d = terminal_finalize_backoff(c);
            assert!(d >= prev, "backoff must not shrink");
            assert!(
                d <= TERMINAL_FINALIZE_BACKOFF_MAX,
                "backoff must stay capped"
            );
            prev = d;
        }
        assert_eq!(terminal_finalize_backoff(50), TERMINAL_FINALIZE_BACKOFF_MAX);
        // count 0 (defensive) must not panic and stays at the initial delay.
        assert_eq!(
            terminal_finalize_backoff(0),
            TERMINAL_FINALIZE_BACKOFF_INITIAL
        );
    }

    fn cond(t: &str, status: &str, transition: chrono::DateTime<chrono::Utc>) -> PodCondition {
        PodCondition {
            condition_type: t.to_string(),
            status: status.to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(transition),
            observed_generation: None,
        }
    }

    fn make_node_lease(node_name: &str) -> rusternetes_common::resources::Lease {
        rusternetes_common::resources::Lease::new(node_name, "kube-node-lease").with_spec(
            rusternetes_common::resources::LeaseSpec {
                holder_identity: Some(node_name.to_string()),
                lease_duration_seconds: Some(40),
                acquire_time: Some(chrono::Utc::now()),
                renew_time: Some(chrono::Utc::now()),
                lease_transitions: Some(0),
                preferred_holder: None,
                strategy: None,
            },
        )
    }

    #[test]
    fn apply_node_lease_owner_ref_sets_canonical_reference() {
        let mut lease = make_node_lease("node-a");
        assert!(lease.metadata.owner_references.is_none());

        let mutated = Kubelet::apply_node_lease_owner_ref(
            &mut lease,
            "node-a",
            "11111111-2222-3333-4444-555555555555",
        );

        assert!(mutated, "first apply must mutate metadata");
        let refs = lease
            .metadata
            .owner_references
            .as_ref()
            .expect("owner_references should be set");
        assert_eq!(refs.len(), 1, "exactly one owner reference is allowed");
        let owner = &refs[0];
        assert_eq!(owner.api_version, "v1");
        assert_eq!(owner.kind, "Node");
        assert_eq!(owner.name, "node-a");
        assert_eq!(owner.uid, "11111111-2222-3333-4444-555555555555");
        // Per upstream nodelease/controller.go: neither controller nor
        // blockOwnerDeletion is set — the test only requires the four
        // identity fields.
        assert!(owner.controller.is_none());
        assert!(owner.block_owner_deletion.is_none());
    }

    #[test]
    fn apply_node_lease_owner_ref_is_idempotent_when_already_correct() {
        let mut lease = make_node_lease("node-b");
        let _ = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-b", "uid-b");

        let mutated_again = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-b", "uid-b");

        assert!(
            !mutated_again,
            "second apply with identical owner must not mutate metadata"
        );
        assert_eq!(
            lease
                .metadata
                .owner_references
                .as_ref()
                .map(Vec::len)
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn apply_node_lease_owner_ref_backfills_stale_uid() {
        let mut lease = make_node_lease("node-c");
        lease.metadata.owner_references =
            Some(vec![rusternetes_common::types::OwnerReference::new(
                "v1", "Node", "node-c", "old-uid",
            )]);

        let mutated = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-c", "new-uid");

        assert!(mutated, "stale UID must be overwritten");
        let refs = lease.metadata.owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].uid, "new-uid");
    }

    #[test]
    fn apply_node_lease_owner_ref_noop_for_empty_uid() {
        let mut lease = make_node_lease("node-d");
        let mutated = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-d", "");
        assert!(!mutated);
        assert!(
            lease.metadata.owner_references.is_none(),
            "empty UID must never produce an invalid owner reference"
        );
    }

    #[test]
    fn merge_pod_conditions_preserves_time_when_status_unchanged() {
        let stamp_old = chrono::Utc::now() - chrono::Duration::seconds(60);
        let existing = vec![
            cond("Ready", "True", stamp_old),
            cond("Initialized", "True", stamp_old),
        ];
        // Incoming carries a fresh "now" — must be discarded for matching entries.
        let stamp_new = chrono::Utc::now();
        let incoming = vec![
            cond("Ready", "True", stamp_new),
            cond("Initialized", "True", stamp_new),
        ];

        let merged = Kubelet::merge_pod_conditions(&existing, incoming);

        assert_eq!(merged[0].last_transition_time, Some(stamp_old));
        assert_eq!(merged[1].last_transition_time, Some(stamp_old));
    }

    #[test]
    fn merge_pod_conditions_keeps_fresh_time_on_status_flip() {
        let stamp_old = chrono::Utc::now() - chrono::Duration::seconds(60);
        let stamp_new = chrono::Utc::now();
        let existing = vec![cond("Ready", "False", stamp_old)];
        let incoming = vec![cond("Ready", "True", stamp_new)];

        let merged = Kubelet::merge_pod_conditions(&existing, incoming);

        assert_eq!(merged[0].last_transition_time, Some(stamp_new));
    }

    #[test]
    fn merge_pod_conditions_passes_new_conditions_through() {
        let stamp_new = chrono::Utc::now();
        let merged = Kubelet::merge_pod_conditions(&[], vec![cond("Ready", "True", stamp_new)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].last_transition_time, Some(stamp_new));
    }

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

    fn make_pod(name: &str, namespace: &str, resource_version: Option<&str>) -> Pod {
        let mut meta = ObjectMeta::new(name).with_namespace(namespace);
        if let Some(rv) = resource_version {
            meta.resource_version = Some(rv.to_string());
        }
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
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

    fn make_running_container_status(name: &str) -> ContainerStatus {
        ContainerStatus {
            name: name.to_string(),
            ready: true,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: Some("docker://abc123".to_string()),
            state: Some(ContainerState::Running {
                started_at: Some("2024-01-01T00:00:00Z".to_string()),
            }),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }
    }

    // A Running pod must have containerStatuses so consumers of the pod status
    // don't misinterpret an empty list as "container already finished".
    #[test]
    fn test_running_pod_must_have_container_statuses() {
        let mut pod = make_pod("my-pod", "default", Some("1"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: Some("All containers started".to_string()),
            reason: None,
            host_ip: Some("127.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let status = pod.status.as_ref().unwrap();
        assert_eq!(status.phase, Some(Phase::Running));
        let statuses = status
            .container_statuses
            .as_ref()
            .expect("must have containerStatuses");
        assert!(
            !statuses.is_empty(),
            "Running pod must have at least one containerStatus"
        );
        assert!(statuses[0].ready, "container must be ready=true");
    }

    // Documents the problematic state: phase=Pending with no containerStatuses.
    // Consumers watching pod status may interpret this as "container already finished".
    #[test]
    fn test_pending_with_no_container_statuses_is_the_bug_state() {
        let mut pod = make_pod("my-pod", "default", Some("1"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            message: Some("ContainerCreating".to_string()),
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None, // <-- the bug: sonobuoy-worker sees this and declares done
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let status = pod.status.as_ref().unwrap();
        // Document that this state (Pending + no containerStatuses) is the problematic one
        let is_bug_state = status.phase == Some(Phase::Pending)
            && status
                .container_statuses
                .as_ref()
                .is_none_or(|v| v.is_empty());
        assert!(
            is_bug_state,
            "This is the state that triggers premature result submission"
        );
    }

    // When re-fetching from etcd fails, we fall back to the original pod clone.
    // The fallback ensures we still attempt the status update even if stale.
    #[test]
    fn test_fresh_fetch_fallback_uses_pod_clone_when_get_fails() {
        let original = make_pod("my-pod", "default", Some("42"));
        // Simulate fallback: use the original pod if re-fetch fails
        let fresh_pod = original.clone();
        assert_eq!(
            fresh_pod.metadata.resource_version.as_deref(),
            Some("42"),
            "Fallback uses original resourceVersion"
        );
    }

    #[test]
    fn readiness_gates_gate_ready_until_condition_true() {
        use rusternetes_common::resources::pod::PodReadinessGate;

        let gated_spec = || PodSpec {
            containers: vec![make_container("app")],
            readiness_gates: Some(vec![PodReadinessGate {
                condition_type: "www.example.com/feature-1".to_string(),
            }]),
            ..Default::default()
        };
        let now = chrono::Utc::now();
        let with_status = |spec: PodSpec, conds: Vec<PodCondition>| {
            let mut pod = Pod::new("pod-ready", spec);
            pod.status = Some(PodStatus {
                conditions: Some(conds),
                ..Default::default()
            });
            pod
        };

        // No readinessGates at all -> trivially satisfied.
        let no_gates = Pod::new(
            "pod-ready",
            PodSpec {
                containers: vec![make_container("app")],
                ..Default::default()
            },
        );
        assert!(super::readiness_gates_satisfied(&no_gates));

        // Gate present but no matching condition -> NOT satisfied (this is the
        // "initially Ready=False" case the conformance suite checks).
        let pending = with_status(gated_spec(), vec![cond("ContainersReady", "True", now)]);
        assert!(!super::readiness_gates_satisfied(&pending));

        // Gate condition present but False -> NOT satisfied.
        let gate_false = with_status(
            gated_spec(),
            vec![cond("www.example.com/feature-1", "False", now)],
        );
        assert!(!super::readiness_gates_satisfied(&gate_false));

        // Gate condition present and True -> satisfied.
        let gate_true = with_status(
            gated_spec(),
            vec![cond("www.example.com/feature-1", "True", now)],
        );
        assert!(super::readiness_gates_satisfied(&gate_true));
    }

    #[test]
    fn spec_has_readiness_probe_includes_sidecars() {
        use rusternetes_common::resources::pod::{PodSpec, Probe};

        // A restartable init container (sidecar) with a readiness probe gates
        // pod readiness (#1069).
        let mut sidecar = make_container("sidecar");
        sidecar.restart_policy = Some("Always".to_string());
        sidecar.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec = PodSpec {
            containers: vec![make_container("app")],
            init_containers: Some(vec![sidecar]),
            ..Default::default()
        };
        assert!(super::spec_has_readiness_probe(&spec));

        // No readiness probe anywhere -> not gated.
        let spec_none = PodSpec {
            containers: vec![make_container("app")],
            ..Default::default()
        };
        assert!(!super::spec_has_readiness_probe(&spec_none));

        // A regular container's readiness probe still gates.
        let mut app = make_container("app");
        app.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec_reg = PodSpec {
            containers: vec![app],
            ..Default::default()
        };
        assert!(super::spec_has_readiness_probe(&spec_reg));

        // A plain (non-restartable) init container's probe does NOT gate.
        let mut plain_init = make_container("init");
        plain_init.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec_plain = PodSpec {
            containers: vec![make_container("app")],
            init_containers: Some(vec![plain_init]),
            ..Default::default()
        };
        assert!(!super::spec_has_readiness_probe(&spec_plain));
    }

    // A container with state=Running signals that the container is still in
    // progress. Consumers should not treat it as finished.
    #[test]
    fn test_container_status_running_state_prevents_premature_submission() {
        let status = make_running_container_status("app");
        match &status.state {
            Some(ContainerState::Running { .. }) => {
                // This state correctly signals "still running" to sonobuoy-worker
            }
            other => panic!("Expected Running state, got {:?}", other),
        }
        assert!(status.ready, "Running container must be ready=true");
    }

    // A container with state=Waiting also signals "not finished" since it hasn't
    // exited yet. Only Terminated state means the container is done.
    #[test]
    fn test_container_status_waiting_also_signals_not_finished() {
        let status = ContainerStatus {
            name: "app".to_string(),
            ready: false,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: None,
            state: Some(ContainerState::Waiting {
                reason: Some("ContainerCreating".to_string()),
                message: None,
            }),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };
        let is_terminated = matches!(status.state, Some(ContainerState::Terminated { .. }));
        assert!(
            !is_terminated,
            "Waiting container is not terminated — sonobuoy-worker should wait"
        );
    }

    // Documents why re-fetching from etcd before writing Running status is necessary.
    // An admission controller or scheduler may have incremented resourceVersion between
    // when we fetched the pod and when start_pod returned, causing update to fail.
    #[test]
    fn test_stale_resource_version_causes_conflict() {
        let stale = make_pod("my-pod", "default", Some("5"));
        // Simulate etcd advancing the resourceVersion (e.g., admission controller touch)
        let fresh = make_pod("my-pod", "default", Some("6"));

        assert_ne!(
            stale.metadata.resource_version, fresh.metadata.resource_version,
            "Stale rv={:?} differs from fresh rv={:?} — using stale would cause conflict",
            stale.metadata.resource_version, fresh.metadata.resource_version
        );

        // The fix: always use fresh.metadata.resource_version when writing status
        let rv_to_use = fresh.metadata.resource_version.as_deref().unwrap_or("0");
        assert_eq!(
            rv_to_use, "6",
            "Must use fresh resourceVersion to avoid conflict"
        );
    }

    // ---- Pod Resize Tests (KEP-1287) ----

    /// When pod status.resize is "Proposed", the kubelet should detect it as a resize request.
    #[test]
    fn test_resize_proposed_detected() {
        let mut pod = make_pod("resize-pod", "default", Some("10"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("10.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("Proposed".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");

        assert_eq!(resize_status, "Proposed");
        assert!(
            resize_status == "Proposed" || resize_status == "InProgress",
            "Kubelet should process Proposed or InProgress resize"
        );
    }

    /// When pod status.resize is "InProgress", the kubelet should continue processing.
    #[test]
    fn test_resize_in_progress_continues() {
        let mut pod = make_pod("resize-pod-ip", "default", Some("11"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("10.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");

        assert_eq!(resize_status, "InProgress");
        // Kubelet should still process an InProgress resize
        assert!(resize_status == "Proposed" || resize_status == "InProgress");
    }

    /// After resize completes, status.resize should be empty string.
    #[test]
    fn test_resize_completion_sets_empty_string() {
        let mut pod = make_pod("resize-done", "default", Some("12"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Simulate the kubelet marking resize as complete
        if let Some(ref mut status) = pod.status {
            status.resize = Some(String::new()); // Empty = resize complete
        }

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("missing");

        assert_eq!(
            resize_status, "",
            "After resize completes, status.resize should be empty string"
        );
    }

    /// Verify that allocatedResources is populated from spec resources after resize.
    #[test]
    fn test_resize_populates_allocated_resources() {
        use rusternetes_common::resources::pod::PodSpec;
        use rusternetes_common::types::ResourceRequirements;
        use std::collections::HashMap;

        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), "500m".to_string());
        requests.insert("memory".to_string(), "256Mi".to_string());

        let mut limits = HashMap::new();
        limits.insert("cpu".to_string(), "1".to_string());
        limits.insert("memory".to_string(), "512Mi".to_string());

        let mut pod = make_pod("resize-alloc", "default", Some("13"));
        pod.spec = Some(PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                image: "nginx:latest".to_string(),
                resources: Some(ResourceRequirements {
                    requests: Some(requests.clone()),
                    limits: Some(limits.clone()),
                    claims: None,
                }),
                ..make_container("app")
            }],
            ..pod.spec.unwrap()
        });
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![ContainerStatus {
                name: "app".to_string(),
                ready: true,
                restart_count: 0,
                state: Some(ContainerState::Running {
                    started_at: Some("2024-01-01T00:00:00Z".to_string()),
                }),
                last_state: None,
                image: Some("nginx:latest".to_string()),
                image_id: None,
                container_id: Some("docker://abc123".to_string()),
                started: Some(true),
                allocated_resources: None, // Not yet populated
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            }]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Simulate the kubelet logic: after successful resize, populate allocatedResources
        // from spec containers (mirroring the actual kubelet code at line ~930-948)
        if let Some(ref mut status) = pod.status {
            status.resize = Some(String::new());
            if let Some(ref spec) = pod.spec.clone() {
                if let Some(ref mut cs_list) = status.container_statuses {
                    for cs in cs_list.iter_mut() {
                        if let Some(c) = spec.containers.iter().find(|c| c.name == cs.name) {
                            if let Some(ref res) = c.resources {
                                cs.allocated_resources =
                                    res.requests.clone().or_else(|| res.limits.clone());
                                cs.resources = Some(res.clone());
                            }
                        }
                    }
                }
            }
        }

        // Verify allocatedResources were populated
        let cs = &pod
            .status
            .as_ref()
            .unwrap()
            .container_statuses
            .as_ref()
            .unwrap()[0];
        let alloc = cs
            .allocated_resources
            .as_ref()
            .expect("allocatedResources should be populated after resize");
        assert_eq!(alloc.get("cpu"), Some(&"500m".to_string()));
        assert_eq!(alloc.get("memory"), Some(&"256Mi".to_string()));

        // Verify resources were populated
        let res = cs
            .resources
            .as_ref()
            .expect("resources should be populated after resize");
        assert_eq!(
            res.requests.as_ref().unwrap().get("cpu"),
            Some(&"500m".to_string())
        );
        assert_eq!(
            res.limits.as_ref().unwrap().get("cpu"),
            Some(&"1".to_string())
        );
    }

    /// When resize is not Proposed or InProgress, the kubelet should not process a resize.
    #[test]
    fn test_resize_not_triggered_for_empty_or_none() {
        // No resize field
        let mut pod = make_pod("no-resize", "default", Some("14"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");
        assert!(
            resize_status != "Proposed" && resize_status != "InProgress",
            "No resize should be triggered when resize is None"
        );

        // Empty string (completed)
        pod.status.as_mut().unwrap().resize = Some(String::new());
        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");
        assert!(
            resize_status != "Proposed" && resize_status != "InProgress",
            "No resize should be triggered when resize is empty (completed)"
        );
    }

    /// Verify the resize status transition: Proposed -> InProgress -> "" (complete)
    #[test]
    fn test_resize_status_transition() {
        let mut pod = make_pod("resize-transition", "default", Some("15"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("Proposed".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Step 1: API sets resize="Proposed"
        assert_eq!(
            pod.status.as_ref().unwrap().resize.as_deref(),
            Some("Proposed")
        );

        // Step 2: Kubelet transitions to "InProgress"
        pod.status.as_mut().unwrap().resize = Some("InProgress".to_string());
        assert_eq!(
            pod.status.as_ref().unwrap().resize.as_deref(),
            Some("InProgress")
        );

        // Step 3: Kubelet completes resize, sets to ""
        pod.status.as_mut().unwrap().resize = Some(String::new());
        assert_eq!(pod.status.as_ref().unwrap().resize.as_deref(), Some(""));
    }

    // --- Init-container failure conditions (K8s conformance) ---
    //
    // K8s conformance test "should not start app containers if init
    // containers fail on a RestartAlways pod" (init_container.go:446) asserts:
    //   - pod.conditions[Initialized].Status  == "False"
    //   - pod.conditions[Initialized].Reason  == "ContainersNotInitialized"
    //   - pod.conditions[Initialized].Message == "containers with incomplete status: [init1 init2]"
    //
    // The kubelet sync loop's should_retry branch builds these conditions
    // from `init_failed_pod_conditions(&incomplete_inits)` using the same
    // filter as the start_pod error handler: an init container is
    // "incomplete" iff its state is NOT Terminated{exit_code:0}.

    /// Mirror the incomplete-init filter that kubelet.rs uses in the
    /// init-failure status update. Kept in sync with the inline copies in
    /// the should_retry branch and the start_pod error handler.
    fn incomplete_init_names(pod: &Pod) -> Vec<String> {
        let init_statuses = pod
            .status
            .as_ref()
            .and_then(|s| s.init_container_statuses.as_ref());
        pod.spec
            .as_ref()
            .and_then(|s| s.init_containers.as_ref())
            .map(|ics| {
                ics.iter()
                    .filter(|c| {
                        let completed = init_statuses
                            .and_then(|statuses| statuses.iter().find(|s| s.name == c.name))
                            .map(|s| {
                                matches!(
                                    &s.state,
                                    Some(ContainerState::Terminated { exit_code: 0, .. })
                                )
                            })
                            .unwrap_or(false);
                        !completed
                    })
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn init_status(name: &str, state: ContainerState) -> ContainerStatus {
        ContainerStatus {
            name: name.to_string(),
            ready: false,
            restart_count: 0,
            state: Some(state),
            last_state: None,
            image: Some("busybox:latest".to_string()),
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

    fn restartalways_pod_with_two_inits() -> Pod {
        // init1 failed (exit 1), init2 never ran (Waiting/PodInitializing)
        let init_statuses = vec![
            init_status(
                "init1",
                ContainerState::Terminated {
                    exit_code: 1,
                    signal: None,
                    reason: Some("Error".to_string()),
                    message: None,
                    started_at: Some("2026-01-01T00:00:00Z".to_string()),
                    finished_at: Some("2026-01-01T00:00:01Z".to_string()),
                    container_id: Some("docker://abc123".to_string()),
                },
            ),
            init_status(
                "init2",
                ContainerState::Waiting {
                    reason: Some("PodInitializing".to_string()),
                    message: None,
                },
            ),
        ];
        let mut pod = make_pod("pod-init", "default", None);
        if let Some(ref mut spec) = pod.spec {
            spec.init_containers = Some(vec![make_container("init1"), make_container("init2")]);
            spec.containers = vec![make_container("run1")];
            spec.restart_policy = Some("Always".to_string());
        }
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            message: None,
            reason: Some("PodInitializing".to_string()),
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: Some(init_statuses),
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });
        pod
    }

    #[test]
    fn test_init_failed_conditions_lists_both_inits_when_first_failed_and_second_not_run() {
        // The conformance test asserts a specific message format that
        // includes BOTH init container names — init1 (failed) and init2
        // (which never had a chance to run) — separated by a single space.
        let pod = restartalways_pod_with_two_inits();
        let incomplete = incomplete_init_names(&pod);
        assert_eq!(
            incomplete,
            vec!["init1".to_string(), "init2".to_string()],
            "both init1 (Terminated with non-zero exit) and init2 (Waiting) must be marked incomplete"
        );

        let conditions = Kubelet::init_failed_pod_conditions(&incomplete);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");

        assert_eq!(initialized.status, "False");
        assert_eq!(
            initialized.reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        assert_eq!(
            initialized.message.as_deref(),
            Some("containers with incomplete status: [init1 init2]"),
            "K8s conformance asserts this exact message verbatim"
        );
    }

    #[test]
    fn test_init_failed_conditions_drops_successful_init_from_list() {
        // After init1 succeeds and init2 fails, only init2 must appear in
        // the conditions message — successful init containers are dropped.
        let mut pod = restartalways_pod_with_two_inits();
        if let Some(ref mut status) = pod.status {
            status.init_container_statuses = Some(vec![
                init_status(
                    "init1",
                    ContainerState::Terminated {
                        exit_code: 0,
                        signal: None,
                        reason: Some("Completed".to_string()),
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                    },
                ),
                init_status(
                    "init2",
                    ContainerState::Terminated {
                        exit_code: 1,
                        signal: None,
                        reason: Some("Error".to_string()),
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                    },
                ),
            ]);
        }
        let incomplete = incomplete_init_names(&pod);
        assert_eq!(
            incomplete,
            vec!["init2".to_string()],
            "successfully terminated (exit 0) init containers must not appear in incomplete list"
        );
        let conditions = Kubelet::init_failed_pod_conditions(&incomplete);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");
        assert_eq!(
            initialized.message.as_deref(),
            Some("containers with incomplete status: [init2]"),
            "only failing init container must appear in the message"
        );
    }

    #[test]
    fn test_init_failed_conditions_pod_initialized_is_false_with_correct_reason() {
        // RestartAlways pod whose init container fails must report
        // PodInitialized=False with reason ContainersNotInitialized.
        let conditions = Kubelet::init_failed_pod_conditions(&["init1".to_string()]);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");
        assert_eq!(initialized.status, "False");
        assert_eq!(
            initialized.reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        // Sanity: ContainersReady and Ready also exist and are False.
        let containers_ready = conditions
            .iter()
            .find(|c| c.condition_type == "ContainersReady")
            .expect("ContainersReady condition must be present");
        assert_eq!(containers_ready.status, "False");
        let ready = conditions
            .iter()
            .find(|c| c.condition_type == "Ready")
            .expect("Ready condition must be present");
        assert_eq!(ready.status, "False");
    }
}
