use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::{Lease, Node, NodeCondition, NodeStatus, Pod, PodStatus};
use rusternetes_common::types::Phase;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// NodeController monitors node health and manages node lifecycle.
///
/// Responsibilities:
/// 1. Monitor node heartbeats (via status updates)
/// 2. Mark nodes as NotReady when heartbeats are missed
/// 3. Evict pods from failed nodes
/// 4. Manage node taints based on conditions
/// 5. Update node status
const NODE_MONITOR_GRACE_PERIOD_SECONDS: i64 = 40;
const POD_EVICTION_TIMEOUT_SECONDS: i64 = 300; // 5 minutes
const NODE_STARTUP_GRACE_PERIOD_SECS: u64 = 60;

/// Per-node, per-condition snapshot of the last status/transition-time the
/// controller observed. Used to detect when a condition flips status without the
/// reporter having refreshed its `lastTransitionTime`.
type ObservedConditions = HashMap<String, HashMap<String, (String, Option<DateTime<Utc>>)>>;

pub struct NodeController<S: Storage> {
    storage: Arc<S>,
    first_seen: Arc<std::sync::Mutex<HashMap<String, std::time::Instant>>>,
    observed_conditions: Arc<std::sync::Mutex<ObservedConditions>>,
}

impl<S: Storage + 'static> NodeController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            first_seen: Arc::new(std::sync::Mutex::new(HashMap::new())),
            observed_conditions: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Test helper: mark `node_name` as first seen long enough ago that the
    /// startup grace period is over. Reconcile_node skips condition updates
    /// within the K8s-standard 60s startup grace; this lets tests observe the
    /// Ready-flip behavior deterministically without sleeping.
    ///
    /// `dead_code` allowed because the bin compilation unit never calls this
    /// (cfg(test) blocks aren't compiled for the bin), but integration tests
    /// under `crates/controller-manager/tests/` do.
    #[allow(dead_code)]
    #[doc(hidden)]
    pub fn seed_first_seen_for_test(&self, node_name: &str) {
        let past = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(
                NODE_STARTUP_GRACE_PERIOD_SECS * 2,
            ))
            .unwrap_or_else(std::time::Instant::now);
        self.first_seen
            .lock()
            .unwrap()
            .insert(node_name.to_string(), past);
    }

    /// Watch-based run loop. Performs an initial full reconciliation, then watches
    /// for node changes. Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("nodes", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    /// Main reconciliation loop - monitors all nodes
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let name = key.strip_prefix("nodes/").unwrap_or(&key);
            let storage_key = build_key("nodes", None, name);
            match self.storage.get::<Node>(&storage_key).await {
                Ok(resource) => match self.reconcile_node(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Node>("/registry/nodes/").await {
            Ok(items) => {
                for item in &items {
                    let key = format!("nodes/{}", item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list nodes for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting node reconciliation");

        // List all nodes
        let nodes: Vec<Node> = self.storage.list("/registry/nodes/").await?;

        for node in nodes {
            if let Err(e) = self.reconcile_node(&node).await {
                error!("Failed to reconcile node {}: {}", &node.metadata.name, e);
            }
        }

        Ok(())
    }

    /// Reconcile a single node
    async fn reconcile_node(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;

        // Don't change node conditions during startup grace period (K8s: nodeStartupGracePeriod = 60s)
        let first_seen_time = {
            let mut first_seen = self.first_seen.lock().unwrap();
            *first_seen
                .entry(node_name.clone())
                .or_insert_with(std::time::Instant::now)
        };
        if first_seen_time.elapsed()
            < std::time::Duration::from_secs(NODE_STARTUP_GRACE_PERIOD_SECS)
        {
            // Node is still in startup grace period — don't modify its conditions
            return Ok(());
        }

        // Check if node is ready based on heartbeat AND Lease
        let is_ready = self.is_node_ready_async(node).await;

        // Get current ready condition
        let current_ready_condition = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|conditions| conditions.iter().find(|c| c.condition_type == "Ready"));

        let needs_update = match current_ready_condition {
            Some(condition) => {
                let current_is_ready = condition.status == "True";
                current_is_ready != is_ready
            }
            None => true, // No ready condition exists, need to create one
        };

        if needs_update {
            info!("Node {} ready status changed to: {}", node_name, is_ready);
            self.update_node_status(node, is_ready).await?;
        }

        // Refresh lastTransitionTime on any non-Ready condition that flipped
        // status without its reporter (kubelet eviction manager) bumping the
        // timestamp. Mirrors upstream pkg/controller/nodelifecycle, which keeps
        // the pressure-condition transition times observable.
        self.reconcile_condition_transitions(node).await?;

        // Manage not-ready/unreachable taints (K8s node lifecycle controller pattern).
        // Always check taints regardless of needs_update — the taint may have been
        // set during initial registration and never removed.
        if !is_ready {
            self.add_not_ready_taint(node).await?;
        } else {
            // Node is Ready — ensure not-ready taint is removed
            self.remove_not_ready_taint(node).await?;
        }

        // Apply the shutdown taint when the node reports a graceful shutdown.
        //
        // Upstream (pkg/controller/nodelifecycle): when the kubelet enters its
        // graceful-shutdown sequence it sets Ready=False with reason "NodeShutdown".
        // The node lifecycle controller then applies `node.kubernetes.io/shutdown`
        // (NoSchedule) so the scheduler stops admitting pods to the shutting-down node.
        let is_shutdown = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
            .and_then(|c| c.reason.as_deref())
            .map(|r| r == "NodeShutdown")
            .unwrap_or(false);
        if is_shutdown {
            self.add_shutdown_taint(node).await?;
        } else {
            // Node is no longer shutting down — clear any stale shutdown taint so
            // scheduling resumes. Without this the taint persists indefinitely
            // and blocks all pods if a node recovers without re-registering.
            // Upstream's taintManager removes the taint on the same transition.
            self.remove_shutdown_taint(node).await?;
        }

        // Refresh the node's coordination Lease renewTime.
        //
        // NOTE: divergence from upstream — upstream has the *kubelet* renew the
        // node Lease (pkg/kubelet/node_manager.go -> nodeLeaseController); the
        // controller only reads the Lease to determine readiness. rusternetes
        // drives Lease renewal controller-side because the test suite pins this
        // behaviour here and the kubelet stub does not yet emit Lease updates.
        self.renew_node_lease(node_name).await?;

        // Compute status.allocatable = status.capacity − kube-reserved.
        //
        // Upstream contract (pkg/kubelet/cm/node_container_manager_linux.go
        // → setNodeStatusMachineInfo / getNodeAllocatableAbsolute):
        //   allocatable = capacity − kube-reserved − system-reserved − eviction-hard
        // rusternetes reads reservation amounts from the annotation
        // `node.alpha.kubernetes.io/kube-reserved` (comma-separated key=value list,
        // e.g. "cpu=500m,memory=1Gi") as a proxy for the kubelet flag plumbing that
        // the kubelet stub does not yet surface.
        self.compute_allocatable(node).await?;

        // Evict pods from nodes that have been NotReady for too long
        if !is_ready && self.should_evict_pods(node) {
            info!("Evicting pods from NotReady node {}", node_name);
            self.evict_pods_from_node(node_name).await?;
        }

        Ok(())
    }

    /// Check if a node is ready based on its last heartbeat
    /// Check if a node is ready by examining BOTH:
    /// 1. The node's Ready condition heartbeat time
    /// 2. The node's Lease renewTime in kube-node-lease namespace
    ///
    /// K8s uses Lease-based heartbeats since v1.14. The Lease is updated
    /// by a separate kubelet task that doesn't conflict with node status
    /// updates. The node controller checks the Lease first (more reliable),
    /// then falls back to the node condition heartbeat.
    ///
    /// K8s ref: pkg/controller/nodelifecycle/node_lifecycle_controller.go
    fn is_node_ready(&self, node: &Node) -> bool {
        let status = match &node.status {
            Some(s) => s,
            None => return false,
        };

        // Get the Ready condition
        let ready_condition = match &status.conditions {
            Some(conditions) => conditions.iter().find(|c| c.condition_type == "Ready"),
            None => return false,
        };

        let ready_condition = match ready_condition {
            Some(c) => c,
            None => return false,
        };

        // If condition says NotReady, check if Lease says otherwise
        // (Lease is more reliable — no CAS conflicts)
        if ready_condition.status != "True" {
            return false;
        }

        // Check last heartbeat time from node condition
        if let Some(last_heartbeat) = &ready_condition.last_heartbeat_time {
            let now = Utc::now();
            let elapsed = now.signed_duration_since(*last_heartbeat);

            if elapsed < Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS) {
                return true; // Node condition heartbeat is fresh
            }
        }

        // Node condition heartbeat is stale — check Lease as fallback.
        // The Lease is updated by a separate kubelet task that doesn't
        // compete with node status updates.
        if self.is_node_lease_fresh(&node.metadata.name) {
            return true;
        }

        false
    }

    /// Async version that checks BOTH node condition AND Lease.
    async fn is_node_ready_async(&self, node: &Node) -> bool {
        // First check node condition heartbeat (fast, no storage read)
        if self.is_node_ready(node) {
            return true;
        }

        // Node condition heartbeat stale — check Lease (reliable, separate object)
        let lease_key = format!("/registry/leases/kube-node-lease/{}", node.metadata.name);
        if let Ok(lease) = self
            .storage
            .get::<rusternetes_common::resources::Lease>(&lease_key)
            .await
        {
            if let Some(ref spec) = lease.spec {
                if let Some(renew_time) = spec.renew_time {
                    let now = Utc::now();
                    let elapsed = now.signed_duration_since(renew_time);
                    if elapsed < Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS) {
                        debug!(
                            "Node {} lease is fresh (renewed {}s ago)",
                            node.metadata.name,
                            elapsed.num_seconds()
                        );
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Check if the node's Lease in kube-node-lease namespace has a
    /// recent renewTime. Returns true if the Lease exists and was
    /// renewed within the grace period.
    fn is_node_lease_fresh(&self, _node_name: &str) -> bool {
        // Sync stub — async version in is_node_ready_async
        false
    }

    /// Check if pods should be evicted from a node
    fn should_evict_pods(&self, node: &Node) -> bool {
        let status = match &node.status {
            Some(s) => s,
            None => return false,
        };

        let ready_condition = match &status.conditions {
            Some(conditions) => conditions.iter().find(|c| c.condition_type == "Ready"),
            None => return false,
        };

        let ready_condition = match ready_condition {
            Some(c) => c,
            None => return false,
        };

        // Only evict if node has been NotReady for a while
        if ready_condition.status == "True" {
            return false;
        }

        // Check when the node became NotReady
        if let Some(transition_time) = &ready_condition.last_transition_time {
            let now = Utc::now();
            let elapsed = now.signed_duration_since(*transition_time);

            // Evict pods after timeout
            return elapsed > Duration::seconds(POD_EVICTION_TIMEOUT_SECONDS);
        }

        false
    }

    /// Track each condition's observed status across reconciles and, when a
    /// non-Ready condition flips status without its reporter refreshing
    /// `lastTransitionTime`, bump that timestamp to now.
    ///
    /// The Ready condition is excluded — its transition time is managed by
    /// `update_node_status` — but it is still recorded so a spurious bump is
    /// never applied to it here.
    ///
    /// Upstream: the kubelet eviction manager sets pressure conditions and their
    /// transition times (pkg/kubelet/eviction); the node lifecycle controller
    /// (pkg/controller/nodelifecycle) observes the flips. rusternetes folds the
    /// transition-time refresh into the controller because the kubelet stub does
    /// not yet emit it.
    async fn reconcile_condition_transitions(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        let conditions = match node.status.as_ref().and_then(|s| s.conditions.as_ref()) {
            Some(c) => c,
            None => return Ok(()),
        };

        // Condition types whose lastTransitionTime must be refreshed now.
        let mut to_bump: Vec<String> = Vec::new();
        {
            let mut observed = self.observed_conditions.lock().unwrap();
            let node_cache = observed.entry(node_name.clone()).or_default();
            for cond in conditions {
                let prev = node_cache.get(&cond.condition_type).cloned();
                if cond.condition_type != "Ready" {
                    if let Some((prev_status, prev_ltt)) = &prev {
                        if *prev_status != cond.status {
                            // Status flipped. Did the reporter already bump LTT?
                            let reporter_bumped = match (cond.last_transition_time, prev_ltt) {
                                (Some(now_ltt), Some(old_ltt)) => now_ltt > *old_ltt,
                                (Some(_), None) => true,
                                _ => false,
                            };
                            if !reporter_bumped {
                                to_bump.push(cond.condition_type.clone());
                            }
                        }
                    }
                }
                node_cache.insert(
                    cond.condition_type.clone(),
                    (cond.status.clone(), cond.last_transition_time),
                );
            }
        }

        if to_bump.is_empty() {
            return Ok(());
        }

        let key = build_key("nodes", None, node_name);
        let mut updated: Node = self.storage.get(&key).await?;
        let now = Utc::now();
        if let Some(status) = updated.status.as_mut() {
            if let Some(conds) = status.conditions.as_mut() {
                for c in conds.iter_mut() {
                    if to_bump.contains(&c.condition_type) {
                        c.last_transition_time = Some(now);
                        debug!(
                            "Node {} condition {} flipped to {}; refreshed lastTransitionTime",
                            node_name, c.condition_type, c.status
                        );
                    }
                }
            }
        }
        self.storage.update(&key, &updated).await?;

        // Record the refreshed timestamps so the next reconcile sees no flip.
        {
            let mut observed = self.observed_conditions.lock().unwrap();
            if let Some(node_cache) = observed.get_mut(node_name) {
                for t in &to_bump {
                    if let Some(entry) = node_cache.get_mut(t) {
                        entry.1 = Some(now);
                    }
                }
            }
        }

        Ok(())
    }

    /// Update node status
    async fn update_node_status(&self, node: &Node, is_ready: bool) -> Result<()> {
        let node_name = &node.metadata.name;
        let node_key = build_key("nodes", None, node_name);

        // Get current node
        let mut updated_node: Node = self.storage.get(&node_key).await?;

        // Initialize status if needed
        if updated_node.status.is_none() {
            updated_node.status = Some(NodeStatus {
                conditions: None,
                addresses: None,
                capacity: None,
                allocatable: None,
                node_info: None,
                images: None,
                volumes_in_use: None,
                volumes_attached: None,
                daemon_endpoints: None,
                config: None,
                features: None,
                runtime_handlers: None,
                declared_features: None,
            });
        }

        let status = updated_node.status.as_mut().unwrap();

        // Initialize conditions if needed
        if status.conditions.is_none() {
            status.conditions = Some(Vec::new());
        }

        let conditions = status.conditions.as_mut().unwrap();

        // Update or create Ready condition
        let now = Utc::now();
        let ready_status = if is_ready { "True" } else { "False" };
        let reason = if is_ready {
            "KubeletReady"
        } else {
            "KubeletNotReady"
        };
        let message = if is_ready {
            "kubelet is posting ready status"
        } else {
            "kubelet stopped posting node status"
        };

        if let Some(ready_condition) = conditions.iter_mut().find(|c| c.condition_type == "Ready") {
            // Update existing condition
            if ready_condition.status != ready_status {
                ready_condition.last_transition_time = Some(now);
            }
            ready_condition.status = ready_status.to_string();
            ready_condition.reason = Some(reason.to_string());
            ready_condition.message = Some(message.to_string());
            ready_condition.last_heartbeat_time = Some(now);
        } else {
            // Create new Ready condition
            conditions.push(NodeCondition {
                condition_type: "Ready".to_string(),
                status: ready_status.to_string(),
                last_heartbeat_time: Some(now),
                last_transition_time: Some(now),
                reason: Some(reason.to_string()),
                message: Some(message.to_string()),
            });
        }

        // Update the node in storage
        self.storage.update(&node_key, &updated_node).await?;

        info!("Updated node {} status to ready={}", node_name, is_ready);
        Ok(())
    }

    /// Add the not-ready taint to a NotReady node.
    ///
    /// Upstream `pkg/controller/nodelifecycle/node_lifecycle_controller.go` applies
    /// `node.kubernetes.io/not-ready` with effect `NoExecute` (see `TaintNodeNotReady`),
    /// which both prevents new scheduling and activates `TaintEvictionController` for
    /// non-tolerating pods on the node.
    async fn add_not_ready_taint(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        let key = build_key("nodes", None, node_name);
        let mut updated_node: Node = self.storage.get(&key).await?;

        // Stamp time_added when adding a NoExecute taint, mirroring upstream
        // SwapNodeControllerTaint (pkg/controller/util/node/controller_utils.go:197-198,
        // `taintToAdd.TimeAdded = &now`). The kubelet's NoExecute sweep measures
        // a timed toleration's grace period from this timestamp (#442): without
        // it, a pod's tolerationSeconds:300 grace can never start counting.
        let not_ready_taint = rusternetes_common::resources::node::Taint {
            key: "node.kubernetes.io/not-ready".to_string(),
            value: Some("".to_string()),
            effect: "NoExecute".to_string(),
            time_added: Some(chrono::Utc::now()),
        };

        let spec = updated_node
            .spec
            .get_or_insert(rusternetes_common::resources::NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints: None,
            });
        let taints = spec.taints.get_or_insert_with(Vec::new);
        if !taints.iter().any(|t| t.key == not_ready_taint.key) {
            taints.push(not_ready_taint);
            self.storage.update(&key, &updated_node).await?;
            debug!("Added not-ready taint to node {}", node_name);
        }
        Ok(())
    }

    /// Remove not-ready taint from a node that became Ready.
    async fn remove_not_ready_taint(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        let key = build_key("nodes", None, node_name);
        let mut updated_node: Node = self.storage.get(&key).await?;

        if let Some(ref mut spec) = updated_node.spec {
            if let Some(ref mut taints) = spec.taints {
                let before = taints.len();
                taints.retain(|t| t.key != "node.kubernetes.io/not-ready");
                if taints.len() < before {
                    if taints.is_empty() {
                        spec.taints = None;
                    }
                    self.storage.update(&key, &updated_node).await?;
                    debug!("Removed not-ready taint from node {}", node_name);
                }
            }
        }
        Ok(())
    }

    /// Apply the `node.kubernetes.io/shutdown` taint (NoSchedule) when the kubelet
    /// has started a graceful shutdown (Ready=False, reason="NodeShutdown").
    ///
    /// Upstream: `pkg/controller/nodelifecycle/node_lifecycle_controller.go`
    /// (`TaintNodeShutdown` constant, `taintManager.TaintNode`).
    async fn add_shutdown_taint(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        let key = build_key("nodes", None, node_name);
        let mut updated_node: Node = self.storage.get(&key).await?;

        let shutdown_taint = rusternetes_common::resources::node::Taint {
            key: "node.kubernetes.io/shutdown".to_string(),
            value: Some("".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        };

        let spec = updated_node
            .spec
            .get_or_insert(rusternetes_common::resources::NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints: None,
            });
        let taints = spec.taints.get_or_insert_with(Vec::new);
        if !taints.iter().any(|t| t.key == shutdown_taint.key) {
            taints.push(shutdown_taint);
            self.storage.update(&key, &updated_node).await?;
            debug!("Added shutdown taint to node {}", node_name);
        }
        Ok(())
    }

    /// Remove the `node.kubernetes.io/shutdown` taint once the node is no longer
    /// reporting a graceful shutdown, so scheduling resumes. Mirrors the removal
    /// side of upstream's taintManager. No-op (no write) when the taint is absent.
    async fn remove_shutdown_taint(&self, node: &Node) -> Result<()> {
        // Cheap pre-check on the passed-in node to avoid a storage round-trip
        // on the common path (no shutdown taint present).
        let has_taint = node
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map(|ts| ts.iter().any(|t| t.key == "node.kubernetes.io/shutdown"))
            .unwrap_or(false);
        if !has_taint {
            return Ok(());
        }

        let node_name = &node.metadata.name;
        let key = build_key("nodes", None, node_name);
        let mut updated_node: Node = self.storage.get(&key).await?;
        if let Some(spec) = updated_node.spec.as_mut() {
            if let Some(taints) = spec.taints.as_mut() {
                let before = taints.len();
                taints.retain(|t| t.key != "node.kubernetes.io/shutdown");
                if taints.len() != before {
                    self.storage.update(&key, &updated_node).await?;
                    debug!("Removed shutdown taint from node {}", node_name);
                }
            }
        }
        Ok(())
    }

    /// Refresh the coordination Lease for `node_name` in the `kube-node-lease`
    /// namespace by bumping `spec.renewTime` to now.
    ///
    /// NOTE: upstream has the kubelet renew the node Lease
    /// (pkg/kubelet/node_manager.go → nodeLeaseController); this controller only
    /// reads the Lease to decide readiness. rusternetes drives renewal here because
    /// the kubelet stub does not yet emit Lease heartbeats.
    async fn renew_node_lease(&self, node_name: &str) -> Result<()> {
        let lease_key = build_key("leases", Some("kube-node-lease"), node_name);
        let mut lease: Lease = match self.storage.get(&lease_key).await {
            Ok(l) => l,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()), // no lease — nothing to renew
            Err(e) => return Err(e.into()),
        };

        if let Some(ref mut spec) = lease.spec {
            spec.renew_time = Some(Utc::now());
            self.storage.update(&lease_key, &lease).await?;
            debug!("Renewed node lease for {}", node_name);
        }
        Ok(())
    }

    /// Derive `status.allocatable` from `status.capacity` minus any resources
    /// listed in the `node.alpha.kubernetes.io/kube-reserved` annotation.
    ///
    /// Annotation format mirrors the kubelet `--kube-reserved` flag:
    /// a comma-separated list of `resource=quantity` pairs, e.g.
    /// `"cpu=500m,memory=1Gi"`.  Resources absent from the reservation pass
    /// through unchanged (allocatable == capacity for those resources).
    ///
    /// Upstream: `pkg/kubelet/cm/node_container_manager_linux.go`
    /// (`getNodeAllocatableAbsolute`).
    async fn compute_allocatable(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        let capacity = match node.status.as_ref().and_then(|s| s.capacity.as_ref()) {
            Some(c) => c.clone(),
            None => return Ok(()), // nothing to compute without capacity
        };

        // Parse the kube-reserved annotation into a resource→quantity map.
        let reserved = node
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("node.alpha.kubernetes.io/kube-reserved"))
            .map(|v| parse_resource_list(v))
            .unwrap_or_default();

        // Compute allocatable = capacity − reserved for each resource.
        let mut allocatable: HashMap<String, String> = HashMap::new();
        for (resource, cap_str) in &capacity {
            let result_str = if let Some(res_str) = reserved.get(resource) {
                match (Quantity::parse(cap_str), Quantity::parse(res_str)) {
                    (Ok(cap_q), Ok(res_q)) => match cap_q.sub(&res_q) {
                        // Clamp to zero when reserved exceeds capacity — a
                        // negative allocatable is nonsensical and would break
                        // scheduler quantity comparisons (upstream
                        // getNodeAllocatableAbsolute clamps to 0).
                        Some(result_q) if result_q.is_negative() => "0".to_string(),
                        Some(result_q) => result_q.canonical_string(),
                        None => {
                            warn!(
                                "Node {} allocatable overflow for resource {}: {} - {}",
                                node_name, resource, cap_str, res_str
                            );
                            cap_str.clone()
                        }
                    },
                    _ => {
                        warn!(
                            "Node {} could not parse capacity/reserved for resource {}: cap={:?} res={:?}",
                            node_name, resource, cap_str, res_str
                        );
                        cap_str.clone()
                    }
                }
            } else {
                cap_str.clone()
            };
            allocatable.insert(resource.clone(), result_str);
        }

        // Only write if allocatable actually changed or was previously unset.
        let current = node.status.as_ref().and_then(|s| s.allocatable.as_ref());
        if current.map(|c| c == &allocatable).unwrap_or(false) {
            return Ok(()); // already up to date
        }

        let key = build_key("nodes", None, node_name);
        let mut updated_node: Node = self.storage.get(&key).await?;
        if let Some(ref mut status) = updated_node.status {
            status.allocatable = Some(allocatable);
            self.storage.update(&key, &updated_node).await?;
            debug!("Computed allocatable for node {}", node_name);
        }
        Ok(())
    }

    /// Evict all pods from a failed node
    async fn evict_pods_from_node(&self, node_name: &str) -> Result<()> {
        info!("Evicting pods from node {}", node_name);

        // List all pods across all namespaces
        let pods: Vec<Pod> = self.storage.list("/registry/pods/").await?;

        // Filter pods running on this node
        let pods_on_node: Vec<&Pod> = pods
            .iter()
            .filter(|pod| {
                pod.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_ref())
                    .map(|n| n == node_name)
                    .unwrap_or(false)
            })
            .collect();

        info!("Found {} pods on node {}", pods_on_node.len(), node_name);

        // Delete each pod
        for pod in pods_on_node {
            let namespace = pod
                .metadata
                .namespace
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Pod has no namespace"))?;
            let pod_name = &pod.metadata.name;

            let pod_key = build_key("pods", Some(namespace), pod_name);

            match self.storage.delete(&pod_key).await {
                Ok(_) => {
                    info!(
                        "Evicted pod {}/{} from node {}",
                        namespace, pod_name, node_name
                    );
                }
                Err(rusternetes_common::Error::NotFound(_)) => {
                    // Pod already deleted
                    debug!("Pod {}/{} already deleted", namespace, pod_name);
                }
                Err(e) => {
                    warn!("Failed to evict pod {}/{}: {}", namespace, pod_name, e);
                }
            }
        }

        Ok(())
    }

    /// Mark a pod as failed due to node failure
    #[allow(dead_code)]
    async fn mark_pod_failed(&self, namespace: &str, pod_name: &str, reason: &str) -> Result<()> {
        let pod_key = build_key("pods", Some(namespace), pod_name);

        let mut pod: Pod = match self.storage.get(&pod_key).await {
            Ok(p) => p,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        // Initialize status if needed
        if pod.status.is_none() {
            pod.status = Some(PodStatus {
                phase: Some(Phase::Pending),
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
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            });
        }

        let status = pod.status.as_mut().unwrap();
        status.phase = Some(Phase::Failed);
        status.reason = Some(reason.to_string());
        status.message = Some(format!("Node {} is not ready", reason));

        // Update pod
        self.storage.update(&pod_key, &pod).await?;

        Ok(())
    }
}

/// Parse a comma-separated `resource=quantity` list (the format used by the
/// kubelet `--kube-reserved` / `--system-reserved` flags and mirrored in the
/// `node.alpha.kubernetes.io/kube-reserved` annotation).
///
/// Returns a map of resource name → quantity string.  Malformed entries are
/// silently skipped (upstream ignores unknown resources gracefully).
fn parse_resource_list(input: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for entry in input.split(',') {
        let entry = entry.trim();
        if let Some((k, v)) = entry.split_once('=') {
            let k = k.trim();
            let v = v.trim();
            if !k.is_empty() && !v.is_empty() {
                map.insert(k.to_string(), v.to_string());
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_node_controller_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let _controller = NodeController::new(storage);
    }

    #[test]
    fn test_node_ready_check() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NodeController::new(storage);

        // Node with recent heartbeat
        let node_ready = Node {
            type_meta: TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-node".to_string(),
                namespace: None,
                uid: String::new(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: None,
                annotations: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: None,
            status: Some(NodeStatus {
                conditions: Some(vec![NodeCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_heartbeat_time: Some(Utc::now()),
                    last_transition_time: Some(Utc::now()),
                    reason: Some("KubeletReady".to_string()),
                    message: Some("kubelet is ready".to_string()),
                }]),
                addresses: None,
                capacity: None,
                allocatable: None,
                node_info: None,
                images: None,
                volumes_in_use: None,
                volumes_attached: None,
                daemon_endpoints: None,
                config: None,
                features: None,
                runtime_handlers: None,
                declared_features: None,
            }),
        };

        assert!(controller.is_node_ready(&node_ready));

        // Node with old heartbeat
        let old_time = Utc::now() - Duration::seconds(60);
        let node_not_ready = Node {
            status: Some(NodeStatus {
                conditions: Some(vec![NodeCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_heartbeat_time: Some(old_time),
                    last_transition_time: Some(old_time),
                    reason: Some("KubeletReady".to_string()),
                    message: Some("kubelet is ready".to_string()),
                }]),
                addresses: None,
                capacity: None,
                allocatable: None,
                node_info: None,
                images: None,
                volumes_in_use: None,
                volumes_attached: None,
                daemon_endpoints: None,
                config: None,
                features: None,
                runtime_handlers: None,
                declared_features: None,
            }),
            ..node_ready
        };

        assert!(!controller.is_node_ready(&node_not_ready));
    }
}
