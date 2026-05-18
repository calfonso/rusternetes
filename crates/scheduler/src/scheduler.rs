use chrono::Utc;
use rusternetes_common::{
    resources::{Node, Pod, PriorityClass},
    types::Phase,
};
use rusternetes_storage::{
    build_key, build_prefix, extract_key, Storage, StorageBackend, WorkQueue,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

use crate::advanced::{
    check_host_port_conflicts, check_node_affinity, check_pod_affinity, check_pod_anti_affinity,
    check_preemption, check_taints_tolerations, check_topology_spread_constraints, NodeScore,
};

pub struct Scheduler<S: Storage + Send + Sync + 'static = StorageBackend> {
    storage: Arc<S>,
    interval: Duration,
    /// Name of this scheduler (default "default-scheduler")
    scheduler_name: String,
}

impl Scheduler<StorageBackend> {
    pub fn new(storage: Arc<StorageBackend>, interval_secs: u64) -> Self {
        Self::new_with_name(storage, interval_secs, "default-scheduler".to_string())
    }
}

impl<S: Storage + Send + Sync + 'static> Scheduler<S> {
    pub fn new_with_name(storage: Arc<S>, interval_secs: u64, scheduler_name: String) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(interval_secs),
            scheduler_name,
        }
    }

    /// Write a pod update with one-attempt retry on resource-version Conflict
    /// and tracing for all other errors. Avoids silently dropping storage
    /// failures from `let _ = storage.update(...)`. The mutator is re-applied
    /// to the freshly-read pod on Conflict, so it must be idempotent.
    async fn update_pod_with_retry<F>(&self, pod_key: &str, mutate: F)
    where
        F: Fn(&mut Pod) + Send,
    {
        for attempt in 0..2 {
            let mut pod: Pod = match self.storage.get(pod_key).await {
                Ok(p) => p,
                Err(rusternetes_common::Error::NotFound(_)) => return,
                Err(e) => {
                    error!(error = %e, pod = pod_key, "scheduler: failed to read pod");
                    return;
                }
            };
            mutate(&mut pod);
            match self.storage.update(pod_key, &pod).await {
                Ok(_) => return,
                Err(rusternetes_common::Error::Conflict(_)) if attempt == 0 => continue,
                Err(e) => {
                    error!(error = %e, pod = pod_key, "scheduler: failed to update pod");
                    return;
                }
            }
        }
    }

    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        use futures::StreamExt;

        info!(
            "Scheduler '{}' started (watch-based, resync every {:?})",
            self.scheduler_name, self.interval
        );

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            // Watch for pod changes (new pods, status changes)
            let prefix = build_prefix("pods", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(self.interval).await;
                    continue;
                }
            };

            // Resync interval as a safety net — shorter than other controllers
            // because scheduling latency directly impacts pod startup time
            let mut resync = tokio::time::interval(self.interval);
            resync.tick().await; // consume the immediate first tick

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
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
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
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            // K8s schedules ONE pod per cycle (scheduleOne), not all pending
            // pods. This prevents the worker from blocking for minutes when
            // many pods are pending. Each pod key gets its own fast cycle.
            match self.try_schedule_pod(&key).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    debug!("Failed to schedule pod {}: {}", key, e);
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Enqueue all pending pods for scheduling.
    async fn enqueue_all(&self, queue: &WorkQueue) {
        let prefix = build_prefix("pods", None);
        match self.storage.list::<Pod>(&prefix).await {
            Ok(pods) => {
                for pod in &pods {
                    // Only enqueue pods that need scheduling
                    // Treat nodeName="" same as None (Go JSON produces empty string)
                    let has_node = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.node_name.as_deref())
                        .is_some_and(|n| !n.is_empty());
                    let needs_scheduling = !has_node
                        && matches!(
                            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                            None | Some(Phase::Pending)
                        );
                    if needs_scheduling {
                        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
                        let key = format!("pods/{}/{}", ns, pod.metadata.name);
                        queue.add(key).await;
                    }
                }
            }
            Err(e) => {
                error!("Failed to list pods for enqueue: {}", e);
            }
        }
    }

    /// Try to schedule a single pod by its queue key (pods/{ns}/{name}).
    /// K8s schedules one pod per cycle in scheduleOne().
    async fn try_schedule_pod(&self, key: &str) -> rusternetes_common::Result<()> {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        let (ns, name) = match parts.len() {
            3 => (parts[1], parts[2]),
            _ => return Ok(()), // invalid key
        };

        let pod_key = build_key("pods", Some(ns), name);
        let pod: Pod = match self.storage.get(&pod_key).await {
            Ok(p) => p,
            Err(_) => return Ok(()), // pod deleted
        };

        // Check if pod needs scheduling
        let has_node = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .is_some_and(|n| !n.is_empty());
        if has_node {
            return Ok(()); // already scheduled
        }
        let is_pending = pod
            .status
            .as_ref()
            .map(|s| s.phase.is_none() || s.phase == Some(Phase::Pending))
            .unwrap_or(true);
        if !is_pending {
            return Ok(()); // not pending
        }
        let pod_scheduler = pod
            .spec
            .as_ref()
            .and_then(|s| s.scheduler_name.as_deref())
            .unwrap_or("default-scheduler");
        if pod_scheduler != self.scheduler_name {
            return Ok(()); // wrong scheduler
        }

        // Get nodes and all pods for scheduling context
        let nodes: Vec<Node> = self.storage.list(&build_prefix("nodes", None)).await?;
        if nodes.is_empty() {
            return Ok(());
        }
        let all_pods: Vec<Pod> = self.storage.list(&build_prefix("pods", None)).await?;
        let priority_classes = self.load_priority_classes().await?;

        // Resolve priority
        let mut pod = pod;
        if pod.spec.as_ref().and_then(|s| s.priority).is_none() {
            let resolved = self.get_pod_priority_sync(&pod, &priority_classes);
            if resolved != 0 {
                if let Some(ref mut spec) = pod.spec {
                    spec.priority = Some(resolved);
                }
                self.update_pod_with_retry(&pod_key, |p| {
                    if let Some(ref mut spec) = p.spec {
                        if spec.priority.is_none() {
                            spec.priority = Some(resolved);
                        }
                    }
                })
                .await;
            }
        }

        // Try to schedule
        if let Some(node) = self
            .select_node(&pod, &nodes, &all_pods, &priority_classes)
            .await
        {
            if let Err(e) = self.bind_pod_to_node(pod, &node.metadata.name).await {
                error!("Failed to bind pod {}/{} to node: {}", ns, name, e);
            }
        }

        Ok(())
    }

    /// Run one scheduling cycle — schedules all pending pods.
    /// Public for testing.
    pub async fn schedule_pending_pods(&self) -> rusternetes_common::Result<()> {
        debug!("Looking for pending pods to schedule");

        // Get all pods
        let prefix = build_prefix("pods", None);
        let all_pods: Vec<Pod> = self.storage.list(&prefix).await?;

        // Filter pending pods without a node assignment that are assigned to this scheduler
        let pending_pods: Vec<Pod> = all_pods
            .iter()
            .filter(|p| {
                // Check if pod is pending and unscheduled
                // A pod is considered pending if:
                // 1. It has no node assignment, AND
                // 2. Either it has no status, OR phase is None, OR phase is Pending
                // A pod is unscheduled if node_name is None OR empty string.
                // Some K8s clients send "nodeName": "" in pod templates,
                // which deserializes to Some("") instead of None.
                let has_node = p
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_deref())
                    .is_some_and(|n| !n.is_empty());
                let is_pending = !has_node
                    && p.status
                        .as_ref()
                        .map(|s| s.phase.is_none() || s.phase == Some(Phase::Pending))
                        .unwrap_or(true);

                if !is_pending {
                    return false;
                }

                // Check if pod is assigned to this scheduler
                // If schedulerName is not specified, defaults to "default-scheduler"
                let pod_scheduler_name = p
                    .spec
                    .as_ref()
                    .and_then(|s| s.scheduler_name.as_deref())
                    .unwrap_or("default-scheduler");

                pod_scheduler_name == self.scheduler_name
            })
            .cloned()
            .collect();

        if pending_pods.is_empty() {
            debug!("No pending pods to schedule");
            return Ok(());
        }

        debug!("Found {} pending pods to schedule", pending_pods.len());

        // Get all nodes
        let nodes_prefix = build_prefix("nodes", None);
        let nodes: Vec<Node> = self.storage.list(&nodes_prefix).await?;

        if nodes.is_empty() {
            warn!("No nodes available for scheduling");
            return Ok(());
        }

        // Load all PriorityClasses for pod priority resolution
        let priority_classes = self.load_priority_classes().await?;

        // Sort pending pods by priority (descending) — K8s scheduling queue
        // processes higher-priority pods first. Without this, lower-priority
        // replacement pods (from RS controller) can be scheduled before the
        // preemptor, consuming the resources that preemption freed and causing
        // a live-lock: preempt → replacement scheduled → preempt again → ...
        let mut pending_pods = pending_pods;
        pending_pods.sort_by(|a, b| {
            let a_pri = self.get_pod_priority_sync(a, &priority_classes);
            let b_pri = self.get_pod_priority_sync(b, &priority_classes);
            b_pri.cmp(&a_pri) // Descending: highest priority first
        });

        // Re-read all_pods before each scheduling decision. K8s re-evaluates
        // cluster state per-pod. Using stale pod data causes preemption to
        // fail for the second pod because evicted victims are still counted.
        let mut all_pods = all_pods;

        // Schedule each pod with a timeout to prevent one slow pod from
        // blocking all others. K8s processes pods concurrently in the
        // scheduling queue; we process sequentially but with a per-pod timeout.
        for mut pod in pending_pods {
            // Resolve priority from PriorityClass if not explicitly set.
            // K8s admission controller sets spec.priority from priorityClassName,
            // but our admission doesn't always do this. Ensure it's set so
            // preemption can check the priority correctly.
            if pod.spec.as_ref().and_then(|s| s.priority).is_none() {
                let resolved = self.get_pod_priority_sync(&pod, &priority_classes);
                if resolved != 0 {
                    if let Some(ref mut spec) = pod.spec {
                        spec.priority = Some(resolved);
                    }
                    // Also update in storage so preemption picks up the priority
                    let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
                    let pod_key =
                        rusternetes_storage::build_key("pods", Some(pod_ns), &pod.metadata.name);
                    self.update_pod_with_retry(&pod_key, |p| {
                        if let Some(ref mut spec) = p.spec {
                            if spec.priority.is_none() {
                                spec.priority = Some(resolved);
                            }
                        }
                    })
                    .await;
                }
            }
            // 5-second timeout per pod — if scheduling takes longer (e.g.,
            // complex preemption calculation), skip and retry next cycle.
            let schedule_result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                if let Some(node) = self
                    .select_node(&pod, &nodes, &all_pods, &priority_classes)
                    .await
                {
                    if let Err(e) = self
                        .bind_pod_to_node(pod.clone(), &node.metadata.name)
                        .await
                    {
                        error!("Failed to bind pod to node: {}", e);
                    }
                    return true;
                }
                false
            })
            .await;

            match schedule_result {
                Ok(true) => {
                    // Pod bound — re-read all_pods so next pod sees updated state
                    all_pods = self.storage.list(&prefix).await.unwrap_or_default();
                    continue;
                }
                Ok(false) => {} // No node found, try preemption below
                Err(_) => {
                    warn!(
                        "Scheduling timed out for pod {}/{}, will retry",
                        pod.metadata.namespace.as_deref().unwrap_or(""),
                        pod.metadata.name
                    );
                    continue;
                }
            }

            {
                // No suitable node found, try preemption if pod has priority
                if let Some(preemption_result) = self.try_preempt(&pod, &nodes, &all_pods).await {
                    let (node_name, pods_to_evict) = preemption_result;
                    info!(
                        "Preempting {} pods on node {} for high-priority pod {}",
                        pods_to_evict.len(),
                        node_name,
                        pod.metadata.name
                    );

                    // Evict lower-priority pods
                    for pod_name in &pods_to_evict {
                        if let Err(e) = self.evict_pod(pod_name).await {
                            error!("Failed to evict pod {}: {}", pod_name, e);
                        }
                    }

                    // After evicting victims, try to bind the preemptor immediately.
                    // K8s sets nominatedNodeName and waits for the next cycle, but in
                    // our architecture the RS controller may create replacement pods
                    // that steal the freed resources before the next scheduling cycle.
                    // To avoid this race, re-read cluster state and attempt binding now.
                    all_pods = self.storage.list(&prefix).await.unwrap_or_default();
                    let fresh_nodes: Vec<Node> =
                        self.storage.list(&nodes_prefix).await.unwrap_or_default();

                    // Re-read the pod for fresh state
                    let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
                    let pod_key =
                        rusternetes_storage::build_key("pods", Some(pod_ns), &pod.metadata.name);
                    let fresh_pod = self.storage.get::<Pod>(&pod_key).await.ok();

                    if let Some(fresh_pod) = fresh_pod {
                        // Try to schedule directly to the nominated node first
                        if let Some(target_node) =
                            fresh_nodes.iter().find(|n| n.metadata.name == node_name)
                        {
                            let resource_score = self.calculate_resource_score_with_overhead(
                                target_node,
                                &fresh_pod,
                                &all_pods,
                            );
                            if resource_score > 0 {
                                // Resources are free — bind immediately
                                if let Err(e) = self.bind_pod_to_node(fresh_pod, &node_name).await {
                                    error!("Failed to bind preemptor to nominated node: {}", e);
                                    // Fall back to setting nominatedNodeName
                                    let nn = node_name.clone();
                                    self.update_pod_with_retry(&pod_key, |p| {
                                        if let Some(ref mut status) = p.status {
                                            status.nominated_node_name = Some(nn.clone());
                                        }
                                    })
                                    .await;
                                } else {
                                    info!(
                                        "Immediately bound preemptor pod {} to node {}",
                                        pod.metadata.name, node_name
                                    );
                                }
                            } else {
                                // Resources not yet free (victims still running) — set nominatedNodeName
                                let nn = node_name.clone();
                                self.update_pod_with_retry(&pod_key, |p| {
                                    if let Some(ref mut status) = p.status {
                                        status.nominated_node_name = Some(nn.clone());
                                    }
                                })
                                .await;
                                info!(
                                    "Set nominatedNodeName={} on preempting pod {} (resources not yet free)",
                                    node_name, pod.metadata.name
                                );
                            }
                        } else {
                            // Node not found — set nominatedNodeName anyway
                            let nn = node_name.clone();
                            self.update_pod_with_retry(&pod_key, |p| {
                                if let Some(ref mut status) = p.status {
                                    status.nominated_node_name = Some(nn.clone());
                                }
                            })
                            .await;
                        }
                    }

                    // Re-read all_pods after potential binding so next pod sees updated state
                    all_pods = self.storage.list(&prefix).await.unwrap_or_default();
                } else {
                    warn!(
                        "No suitable node found for pod {} (even with preemption)",
                        pod.metadata.name
                    );
                    // Set pod condition to Unschedulable so tests can observe it
                    let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
                    let sched_message = format!(
                        "0/{} nodes are available: no node matched the scheduling constraints",
                        nodes.len()
                    );
                    let pod_key =
                        rusternetes_storage::build_key("pods", Some(pod_ns), &pod.metadata.name);
                    let msg = sched_message.clone();
                    self.update_pod_with_retry(&pod_key, |p| {
                        let condition = rusternetes_common::resources::PodCondition {
                            condition_type: "PodScheduled".to_string(),
                            status: "False".to_string(),
                            reason: Some("Unschedulable".to_string()),
                            message: Some(msg.clone()),
                            last_transition_time: Some(chrono::Utc::now()),
                            observed_generation: None,
                        };
                        if let Some(ref mut status) = p.status {
                            let conditions = status.conditions.get_or_insert_with(Vec::new);
                            conditions.retain(|c| c.condition_type != "PodScheduled");
                            conditions.push(condition);
                        }
                    })
                    .await;

                    // Emit FailedScheduling event — K8s conformance tests wait for this
                    let event_name = format!(
                        "{}.{:x}",
                        pod.metadata.name,
                        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64
                    );
                    let event = serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Event",
                        "metadata": {
                            "name": event_name,
                            "namespace": pod_ns,
                        },
                        "involvedObject": {
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "name": pod.metadata.name,
                            "namespace": pod_ns,
                            "uid": pod.metadata.uid,
                        },
                        "reason": "FailedScheduling",
                        "message": sched_message,
                        "type": "Warning",
                        "source": {
                            "component": "default-scheduler",
                        },
                        "firstTimestamp": chrono::Utc::now().to_rfc3339(),
                        "lastTimestamp": chrono::Utc::now().to_rfc3339(),
                        "count": 1,
                    });
                    let event_key =
                        rusternetes_storage::build_key("events", Some(pod_ns), &event_name);
                    let _ = self.storage.create(&event_key, &event).await;
                }
            }
        }

        Ok(())
    }

    async fn select_node(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        priority_classes: &HashMap<String, PriorityClass>,
    ) -> Option<Node> {
        // Advanced scheduling algorithm:
        // 1. Filter out unschedulable nodes
        // 2. Check taints and tolerations
        // 3. Check node selectors
        // 4. Check DRA device availability
        // 5. Check node affinity
        // 6. Calculate resource scores
        // 7. Select node with highest score

        // Phase 1: Filter schedulable nodes
        let schedulable_nodes: Vec<&Node> = nodes
            .iter()
            .filter(|n| {
                !n.spec
                    .as_ref()
                    .and_then(|s| s.unschedulable)
                    .unwrap_or(false)
            })
            .collect();

        if schedulable_nodes.is_empty() {
            return None;
        }

        // Phase 2: Filter by taints and tolerations
        let tolerated_nodes: Vec<&Node> = schedulable_nodes
            .iter()
            .filter(|node| check_taints_tolerations(node, pod))
            .copied()
            .collect();

        if tolerated_nodes.is_empty() {
            debug!("No nodes tolerate pod taints");
            return None;
        }

        // Phase 3: Check node selectors (basic label matching)
        let selector_matched_nodes: Vec<&Node> =
            if let Some(node_selector) = pod.spec.as_ref().and_then(|s| s.node_selector.as_ref()) {
                tolerated_nodes
                    .iter()
                    .filter(|node| self.matches_node_selector(node, node_selector))
                    .copied()
                    .collect()
            } else {
                tolerated_nodes
            };

        if selector_matched_nodes.is_empty() {
            debug!("No nodes match node selector");
            return None;
        }

        // Phase 4: Check DRA device availability
        // Filter nodes that have required devices for ResourceClaims
        let mut dra_matched_nodes: Vec<&Node> = Vec::new();
        for node in selector_matched_nodes {
            if self.check_dra_device_availability(node, pod).await {
                dra_matched_nodes.push(node);
            } else {
                debug!(
                    "Node {} does not have required DRA devices for pod {}",
                    node.metadata.name, pod.metadata.name
                );
            }
        }

        if dra_matched_nodes.is_empty() {
            debug!("No nodes have required DRA devices");
            return None;
        }

        // Phase 4b: Check hostPort conflicts
        let port_ok_nodes: Vec<&Node> = dra_matched_nodes
            .into_iter()
            .filter(|node| {
                if !check_host_port_conflicts(node, pod, all_pods) {
                    debug!(
                        "Node {} rejected for pod {}: hostPort conflict",
                        node.metadata.name, pod.metadata.name
                    );
                    false
                } else {
                    true
                }
            })
            .collect();

        if port_ok_nodes.is_empty() {
            debug!("No nodes without hostPort conflicts");
            return None;
        }

        // Phase 5, 6 & 7: Score nodes based on affinity, pod affinity/anti-affinity, topology spread, and resources
        let mut node_scores: Vec<NodeScore> = Vec::new();

        for node in port_ok_nodes {
            // Check node affinity (hard requirements and scoring)
            let (affinity_ok, node_affinity_score) = check_node_affinity(node, pod);
            if !affinity_ok {
                continue; // Skip nodes that don't meet hard affinity requirements
            }

            // Check pod affinity (hard requirements and scoring)
            let (pod_affinity_ok, pod_affinity_score) = check_pod_affinity(node, pod, all_pods);
            if !pod_affinity_ok {
                continue; // Skip nodes that don't meet hard pod affinity requirements
            }

            // Check pod anti-affinity (hard requirements and penalty scoring)
            let (pod_anti_affinity_ok, pod_anti_affinity_penalty) =
                check_pod_anti_affinity(node, pod, all_pods);
            if !pod_anti_affinity_ok {
                continue; // Skip nodes that violate hard pod anti-affinity requirements
            }

            // Check topology spread constraints (hard requirements and penalty scoring)
            let (topology_ok, topology_penalty) =
                check_topology_spread_constraints(node, pod, all_pods, nodes);
            if !topology_ok {
                continue; // Skip nodes that violate hard topology spread constraints
            }

            // Calculate resource-based score (accounting for pod overhead and existing pod usage)
            let resource_score = self.calculate_resource_score_with_overhead(node, pod, all_pods);

            // If pod doesn't fit resource-wise, skip
            if resource_score == 0 {
                debug!(
                    "Node {} rejected for pod {}: resource_score=0 (insufficient resources)",
                    node.metadata.name, pod.metadata.name,
                );
                continue;
            }

            // Priority score (resolve from PriorityClass if needed)
            let priority_score = self.get_pod_priority_sync(pod, priority_classes);

            // Combined score:
            // - resource (weight 25%)
            // - node affinity (weight 20%)
            // - pod affinity (weight 18%)
            // - priority (weight 15%)
            // - pod anti-affinity penalty (weight 12%)
            // - topology spread penalty (weight 10%)
            let total_score = (resource_score as i64 * 25 / 100)
                + (node_affinity_score as i64 * 20 / 100)
                + (pod_affinity_score as i64 * 18 / 100)
                + (priority_score as i64 * 15 / 100)
                - (pod_anti_affinity_penalty as i64 * 12 / 100)
                - (topology_penalty as i64 * 10 / 100);

            node_scores.push(NodeScore {
                node_name: node.metadata.name.clone(),
                score: total_score.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
            });
        }

        if node_scores.is_empty() {
            return None;
        }

        // Sort by score (descending). On tie, prefer the node with fewer
        // pods already scheduled (LeastAllocated). This spreads pods across
        // nodes when scores are equal (e.g. identical node configurations).
        let pod_counts: std::collections::HashMap<String, usize> = {
            let mut counts = std::collections::HashMap::new();
            for p in all_pods {
                if let Some(node) = p.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
                    *counts.entry(node.clone()).or_insert(0) += 1;
                }
            }
            counts
        };
        node_scores.sort_by(|a, b| {
            let score_cmp = b.score.cmp(&a.score);
            if score_cmp == std::cmp::Ordering::Equal {
                // Fewer pods = better (ascending)
                let a_pods = pod_counts.get(&a.node_name).unwrap_or(&0);
                let b_pods = pod_counts.get(&b.node_name).unwrap_or(&0);
                a_pods.cmp(b_pods)
            } else {
                score_cmp
            }
        });

        let best_node_name = &node_scores[0].node_name;
        debug!(
            "Selected node {} with score {} for pod {}",
            best_node_name, node_scores[0].score, pod.metadata.name
        );

        nodes
            .iter()
            .find(|n| &n.metadata.name == best_node_name)
            .cloned()
    }

    fn matches_node_selector(&self, node: &Node, selector: &HashMap<String, String>) -> bool {
        let node_labels = node.metadata.labels.as_ref();

        if node_labels.is_none() {
            return selector.is_empty();
        }

        let labels = node_labels.unwrap();

        for (key, value) in selector {
            if labels.get(key) != Some(value) {
                return false;
            }
        }

        true
    }

    async fn bind_pod_to_node(
        &self,
        mut pod: Pod,
        node_name: &str,
    ) -> rusternetes_common::Result<()> {
        // K8s scheduler validates node_name is non-empty before binding.
        // An empty node_name means select_node returned a node with no name,
        // which should never happen but would leave the pod stuck Pending.
        if node_name.is_empty() {
            return Err(rusternetes_common::Error::Internal(
                "cannot bind pod to empty node name".to_string(),
            ));
        }

        debug!(
            "Binding pod {}/{} to node {}",
            pod.metadata
                .namespace
                .as_ref()
                .unwrap_or(&"default".to_string()),
            pod.metadata.name,
            node_name
        );

        // Update pod spec with node name
        if let Some(ref mut spec) = pod.spec {
            spec.node_name = Some(node_name.to_string());
        }

        // Update pod status with PodScheduled condition
        let scheduled_condition = rusternetes_common::resources::PodCondition {
            condition_type: "PodScheduled".to_string(),
            status: "True".to_string(),
            last_transition_time: Some(chrono::Utc::now()),
            reason: Some("Scheduled".to_string()),
            message: Some(format!("Successfully assigned to {}", node_name)),
            observed_generation: None,
        };

        if let Some(ref mut status) = pod.status {
            status.phase = Some(Phase::Pending);
            status.message = Some("Pod scheduled".to_string());
            // Add or update PodScheduled condition
            let conditions = status.conditions.get_or_insert_with(Vec::new);
            if let Some(existing) = conditions
                .iter_mut()
                .find(|c| c.condition_type == "PodScheduled")
            {
                *existing = scheduled_condition;
            } else {
                conditions.push(scheduled_condition);
            }
        } else {
            pod.status = Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Pending),
                message: Some("Pod scheduled".to_string()),
                reason: None,
                host_ip: None,
                pod_ip: None,
                conditions: Some(vec![scheduled_condition]),
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                host_i_ps: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
            });
        }

        // Update pod in storage with retry on conflict
        let key = rusternetes_storage::build_key(
            "pods",
            pod.metadata.namespace.as_deref(),
            &pod.metadata.name,
        );

        match self.storage.update(&key, &pod).await {
            Ok(_) => {
                info!("Successfully bound pod to node {}", node_name);

                // Create a Scheduled event (K8s scheduler always creates this)
                let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
                let event_name = format!(
                    "{}.sched.{}",
                    pod.metadata.name,
                    chrono::Utc::now().format("%Y%m%d%H%M%S")
                );
                let event = serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Event",
                    "metadata": {
                        "name": event_name,
                        "namespace": pod_ns,
                    },
                    "involvedObject": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": pod.metadata.name,
                        "namespace": pod_ns,
                        "uid": pod.metadata.uid,
                    },
                    "reason": "Scheduled",
                    "message": format!("Successfully assigned {}/{} to {}", pod_ns, pod.metadata.name, node_name),
                    "type": "Normal",
                    "source": {
                        "component": "default-scheduler",
                    },
                    "firstTimestamp": chrono::Utc::now().to_rfc3339(),
                    "lastTimestamp": chrono::Utc::now().to_rfc3339(),
                    "count": 1,
                });
                let event_key = rusternetes_storage::build_key("events", Some(pod_ns), &event_name);
                let _ = self.storage.create(&event_key, &event).await;

                Ok(())
            }
            Err(rusternetes_common::Error::Conflict(_)) => {
                // ResourceVersion conflict — re-read and retry once
                debug!("Bind conflict for pod {}, retrying", pod.metadata.name);
                let mut fresh_pod: Pod = self.storage.get(&key).await?;
                if let Some(ref mut spec) = fresh_pod.spec {
                    spec.node_name = Some(node_name.to_string());
                }
                let condition = rusternetes_common::resources::PodCondition {
                    condition_type: "PodScheduled".to_string(),
                    status: "True".to_string(),
                    last_transition_time: Some(chrono::Utc::now()),
                    reason: Some("Scheduled".to_string()),
                    message: Some(format!("Successfully assigned to {}", node_name)),
                    observed_generation: None,
                };
                if let Some(ref mut status) = fresh_pod.status {
                    let conditions = status.conditions.get_or_insert_with(Vec::new);
                    if let Some(existing) = conditions
                        .iter_mut()
                        .find(|c| c.condition_type == "PodScheduled")
                    {
                        *existing = condition;
                    } else {
                        conditions.push(condition);
                    }
                }
                self.storage.update(&key, &fresh_pod).await?;
                info!("Successfully bound pod to node {} (retry)", node_name);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Try to preempt lower-priority pods to make room for a high-priority pod
    /// Returns Some((node_name, pods_to_evict)) if preemption is possible, None otherwise
    async fn try_preempt(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
    ) -> Option<(String, Vec<String>)> {
        // If the pod's preemptionPolicy is "Never", skip preemption entirely
        let preemption_policy = pod
            .spec
            .as_ref()
            .and_then(|s| s.preemption_policy.as_deref())
            .unwrap_or("PreemptLowerPriority");
        if preemption_policy == "Never" {
            debug!(
                "Pod {} has preemptionPolicy=Never, skipping preemption",
                pod.metadata.name
            );
            return None;
        }

        // Check each node to see if preemption is possible
        // Only consider nodes that pass basic scheduling constraints (except resources)
        for node in nodes {
            // Skip unschedulable nodes
            if node
                .spec
                .as_ref()
                .and_then(|s| s.unschedulable)
                .unwrap_or(false)
            {
                continue;
            }

            // Check taints/tolerations
            if !check_taints_tolerations(node, pod) {
                continue;
            }

            // Check node selector
            if let Some(node_selector) = pod.spec.as_ref().and_then(|s| s.node_selector.as_ref()) {
                if !self.matches_node_selector(node, node_selector) {
                    continue;
                }
            }

            // Check node affinity (hard requirements only)
            let (affinity_ok, _) = check_node_affinity(node, pod);
            if !affinity_ok {
                continue;
            }

            let (can_preempt, pods_to_evict) = check_preemption(node, pod, all_pods);
            if can_preempt && !pods_to_evict.is_empty() {
                return Some((node.metadata.name.clone(), pods_to_evict));
            }
        }
        None
    }

    /// Evict a pod by setting its deletionTimestamp (graceful delete).
    /// The kubelet will detect the deletionTimestamp and handle graceful shutdown.
    async fn evict_pod(&self, pod_name: &str) -> rusternetes_common::Result<()> {
        // Find the pod in all namespaces
        let prefix = build_prefix("pods", None);
        let all_pods: Vec<Pod> = self.storage.list(&prefix).await?;

        for mut pod in all_pods {
            if pod.metadata.name == pod_name {
                let key = rusternetes_storage::build_key(
                    "pods",
                    pod.metadata.namespace.as_deref(),
                    pod_name,
                );

                // Set deletionTimestamp and add DisruptionTarget condition
                if pod.metadata.deletion_timestamp.is_none() {
                    pod.metadata.deletion_timestamp = Some(Utc::now());
                    // K8s uses the pod's termination grace period, not 0.
                    // See: pkg/scheduler/framework/preemption/preemption.go — DeletePod
                    pod.metadata.deletion_grace_period_seconds = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.termination_grace_period_seconds)
                        .or(Some(30));
                    // Update the status phase to indicate termination
                    if let Some(ref mut status) = pod.status {
                        status.phase = Some(rusternetes_common::types::Phase::Failed);
                        status.reason = Some("Preempted".to_string());
                        status.message =
                            Some("Pod was preempted by a higher-priority pod".to_string());
                        // Add DisruptionTarget condition (K8s conformance requirement)
                        let disruption_condition = rusternetes_common::resources::PodCondition {
                            condition_type: "DisruptionTarget".to_string(),
                            status: "True".to_string(),
                            last_transition_time: Some(Utc::now()),
                            reason: Some("PreemptionByScheduler".to_string()),
                            message: Some("Preempted by a higher-priority pod".to_string()),
                            observed_generation: None,
                        };
                        let conditions = status.conditions.get_or_insert_with(Vec::new);
                        conditions.push(disruption_condition);
                    }
                    self.storage.update(&key, &pod).await?;
                    info!("Evicted pod {} for preemption (set deletionTimestamp + DisruptionTarget condition)", pod_name);
                }
                return Ok(());
            }
        }

        warn!("Pod {} not found for eviction", pod_name);
        Ok(())
    }

    /// Load all PriorityClasses from storage into a HashMap for fast lookup
    async fn load_priority_classes(
        &self,
    ) -> rusternetes_common::Result<HashMap<String, PriorityClass>> {
        let prefix = build_prefix("priorityclasses", None);
        let priority_classes: Vec<PriorityClass> = self.storage.list(&prefix).await?;

        let mut map = HashMap::new();
        for pc in priority_classes {
            map.insert(pc.metadata.name.clone(), pc);
        }

        Ok(map)
    }

    /// Calculate resource score with pod overhead
    /// Pod overhead represents additional resources required beyond container requests
    fn calculate_resource_score_with_overhead(
        &self,
        node: &Node,
        pod: &Pod,
        all_pods: &[Pod],
    ) -> i32 {
        use crate::advanced::calculate_resource_score_with_pods;

        // Get base resource score accounting for existing pod usage
        let base_score = calculate_resource_score_with_pods(node, pod, all_pods);

        // If no overhead specified, return base score
        let overhead = match &pod.spec {
            Some(spec) => match &spec.overhead {
                Some(o) => o,
                None => return base_score,
            },
            None => return base_score,
        };

        // Parse overhead resources
        let mut cpu_overhead = 0i64;
        let mut memory_overhead = 0i64;

        if let Some(cpu) = overhead.get("cpu") {
            cpu_overhead = self.parse_resource_quantity(cpu, "cpu");
        }
        if let Some(memory) = overhead.get("memory") {
            memory_overhead = self.parse_resource_quantity(memory, "memory");
        }

        // Get node allocatable resources
        let allocatable = match &node.status {
            Some(status) => match &status.allocatable {
                Some(a) => a,
                None => return base_score,
            },
            None => return base_score,
        };

        let available_cpu = allocatable
            .get("cpu")
            .map(|s| self.parse_resource_quantity(s, "cpu"))
            .unwrap_or(0);
        let available_memory = allocatable
            .get("memory")
            .map(|s| self.parse_resource_quantity(s, "memory"))
            .unwrap_or(0);

        // Check if overhead alone would prevent scheduling
        if cpu_overhead > available_cpu || memory_overhead > available_memory {
            return 0; // Can't schedule
        }

        // Reduce the base score proportionally to overhead impact
        let cpu_overhead_ratio = if available_cpu > 0 {
            (cpu_overhead * 100 / available_cpu) as i32
        } else {
            0
        };
        let memory_overhead_ratio = if available_memory > 0 {
            (memory_overhead * 100 / available_memory) as i32
        } else {
            0
        };

        let overhead_penalty = (cpu_overhead_ratio + memory_overhead_ratio) / 2;

        // Return score minus overhead penalty (but not less than 0)
        (base_score - overhead_penalty).max(0)
    }

    /// Parse resource quantity (helper method)
    /// Handles K8s resource formats:
    ///   CPU: "100m" (millicores), "0.5" or "1.5" (decimal cores), "2" (whole cores)
    ///   Memory: "128974848" (bytes), "129e6" (scientific), "129M" (SI), "123Mi" (binary)
    fn parse_resource_quantity(&self, quantity: &str, resource_type: &str) -> i64 {
        let quantity = quantity.trim();

        if resource_type == "cpu" {
            if let Some(stripped) = quantity.strip_suffix('m') {
                stripped.parse::<i64>().unwrap_or(0)
            } else if let Ok(val) = quantity.parse::<f64>() {
                (val * 1000.0) as i64
            } else {
                0
            }
        } else {
            if let Some(stripped) = quantity.strip_suffix("Ki") {
                stripped.parse::<i64>().unwrap_or(0) * 1024
            } else if let Some(stripped) = quantity.strip_suffix("Mi") {
                stripped.parse::<i64>().unwrap_or(0) * 1024 * 1024
            } else if let Some(stripped) = quantity.strip_suffix("Gi") {
                stripped.parse::<i64>().unwrap_or(0) * 1024 * 1024 * 1024
            } else if let Some(stripped) = quantity.strip_suffix("Ti") {
                stripped.parse::<i64>().unwrap_or(0) * 1024 * 1024 * 1024 * 1024
            } else if let Some(stripped) = quantity.strip_suffix('T') {
                stripped.parse::<i64>().unwrap_or(0) * 1_000_000_000_000
            } else if let Some(stripped) = quantity.strip_suffix('G') {
                stripped.parse::<i64>().unwrap_or(0) * 1_000_000_000
            } else if let Some(stripped) = quantity.strip_suffix('M') {
                stripped.parse::<i64>().unwrap_or(0) * 1_000_000
            } else if let Some(stripped) = quantity.strip_suffix('k') {
                stripped.parse::<i64>().unwrap_or(0) * 1000
            } else if let Some(stripped) = quantity.strip_suffix('E') {
                stripped.parse::<i64>().unwrap_or(0) * 1_000_000_000_000_000_000
            } else if let Some(stripped) = quantity.strip_suffix('P') {
                stripped.parse::<i64>().unwrap_or(0) * 1_000_000_000_000_000
            } else if let Ok(val) = quantity.parse::<f64>() {
                val as i64
            } else {
                0
            }
        }
    }

    /// Get the priority value for a pod (synchronous version using pre-loaded PriorityClasses)
    /// If pod.spec.priority is set, use it directly
    /// Otherwise, look up the PriorityClass specified by pod.spec.priorityClassName
    /// If neither is set, return 0 (default priority)
    fn get_pod_priority_sync(
        &self,
        pod: &Pod,
        priority_classes: &HashMap<String, PriorityClass>,
    ) -> i32 {
        let spec = match pod.spec.as_ref() {
            Some(s) => s,
            None => return 0,
        };

        // If priority is explicitly set, use it
        if let Some(priority) = spec.priority {
            return priority;
        }

        // If priorityClassName is set, look it up
        if let Some(class_name) = &spec.priority_class_name {
            if let Some(priority_class) = priority_classes.get(class_name) {
                debug!(
                    "Resolved priority {} from PriorityClass {} for pod {}",
                    priority_class.value, class_name, pod.metadata.name
                );
                return priority_class.value;
            } else {
                warn!(
                    "PriorityClass {} not found for pod {}, using default priority 0",
                    class_name, pod.metadata.name
                );
                return 0;
            }
        }

        // No priority specified
        0
    }

    // DRA (Dynamic Resource Allocation) Integration Methods

    /// Check if node has available devices for DRA ResourceClaims
    /// Returns true if all required devices are available on the node, or if no resource claims are specified
    async fn check_dra_device_availability(&self, node: &Node, pod: &Pod) -> bool {
        use rusternetes_common::resources::{ResourceClaim, ResourceSlice};

        // Extract resourceClaims from pod.spec
        let spec = match &pod.spec {
            Some(s) => s,
            None => return true, // No spec, no claims to check
        };

        let resource_claims_refs = match &spec.resource_claims {
            Some(claims) => claims,
            None => return true, // No resource claims, all nodes are suitable
        };

        if resource_claims_refs.is_empty() {
            return true;
        }

        let pod_namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // For each claim reference, resolve the ResourceClaim object
        for claim_ref in resource_claims_refs {
            let claim_name = if let Some(name) = &claim_ref.resource_claim_name {
                name.as_str()
            } else if let Some(template_name) = &claim_ref.resource_claim_template_name {
                // TODO: In a full implementation, we'd need to resolve the template
                // and create a ResourceClaim from it. For now, we'll treat the template
                // name as the claim name (simplified)
                debug!(
                    "ResourceClaimTemplate '{}' referenced, treating as claim name",
                    template_name
                );
                template_name.as_str()
            } else {
                warn!("ResourceClaim reference has no name or template");
                return false;
            };

            // Get the ResourceClaim from storage
            let claim_key =
                rusternetes_storage::build_key("resourceclaims", Some(pod_namespace), claim_name);

            let claim: ResourceClaim = match self.storage.get(&claim_key).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Failed to get ResourceClaim {}/{}: {}",
                        pod_namespace, claim_name, e
                    );
                    return false;
                }
            };

            // Check if the claim is allocated
            let allocation = match &claim.status {
                Some(status) => match &status.allocation {
                    Some(alloc) => alloc,
                    None => {
                        debug!(
                            "ResourceClaim {}/{} is not yet allocated",
                            pod_namespace, claim_name
                        );
                        return false; // Claim not allocated yet
                    }
                },
                None => {
                    debug!(
                        "ResourceClaim {}/{} has no status",
                        pod_namespace, claim_name
                    );
                    return false;
                }
            };

            // Check if the allocation has a node selector and if this node matches
            if let Some(node_selector) = &allocation.node_selector {
                // Check if node matches the node selector
                let node_labels = node.metadata.labels.as_ref();
                if let Some(required_labels) = &node_selector.node_selector_terms.first() {
                    if let Some(match_expressions) = &required_labels.match_expressions {
                        for expr in match_expressions {
                            let node_label_value = node_labels.and_then(|l| l.get(&expr.key));

                            match expr.operator.as_str() {
                                "In" => {
                                    if let Some(values) = &expr.values {
                                        let matches = node_label_value
                                            .map(|v| values.contains(v))
                                            .unwrap_or(false);
                                        if !matches {
                                            debug!(
                                                "Node {} does not match ResourceClaim node selector (key={}, operator=In)",
                                                node.metadata.name, expr.key
                                            );
                                            return false;
                                        }
                                    }
                                }
                                "NotIn" => {
                                    if let Some(values) = &expr.values {
                                        let matches = node_label_value
                                            .map(|v| !values.contains(v))
                                            .unwrap_or(true);
                                        if !matches {
                                            debug!(
                                                "Node {} does not match ResourceClaim node selector (key={}, operator=NotIn)",
                                                node.metadata.name, expr.key
                                            );
                                            return false;
                                        }
                                    }
                                }
                                "Exists" => {
                                    if node_label_value.is_none() {
                                        debug!(
                                            "Node {} does not match ResourceClaim node selector (key={}, operator=Exists)",
                                            node.metadata.name, expr.key
                                        );
                                        return false;
                                    }
                                }
                                "DoesNotExist" => {
                                    if node_label_value.is_some() {
                                        debug!(
                                            "Node {} does not match ResourceClaim node selector (key={}, operator=DoesNotExist)",
                                            node.metadata.name, expr.key
                                        );
                                        return false;
                                    }
                                }
                                _ => {
                                    warn!("Unknown node selector operator: {}", expr.operator);
                                }
                            }
                        }
                    }
                }
            }

            // Verify devices are available on this node
            // Check each allocated device to ensure it's on this node
            for device_result in &allocation.devices.results {
                // Get ResourceSlices to find which node has this device
                let slices_prefix = build_prefix("resourceslices", None);
                let slices: Vec<ResourceSlice> = match self.storage.list(&slices_prefix).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to list ResourceSlices: {}", e);
                        return false;
                    }
                };

                let mut device_found_on_node = false;

                for slice in slices {
                    // Check if this slice is for the right driver and pool
                    if slice.spec.driver != device_result.driver {
                        continue;
                    }
                    if slice.spec.pool.name != device_result.pool {
                        continue;
                    }

                    // Check if slice has node name specified
                    let slice_node_name = match &slice.spec.node_name {
                        Some(name) => name,
                        None => continue, // Slice not associated with a specific node
                    };

                    // Check if this is the target node
                    if slice_node_name != &node.metadata.name {
                        continue;
                    }

                    // Check if the device exists in this slice
                    for device in &slice.spec.devices {
                        if device.name == device_result.device {
                            device_found_on_node = true;
                            break;
                        }
                    }

                    if device_found_on_node {
                        break;
                    }
                }

                if !device_found_on_node {
                    debug!(
                        "Device {} from pool {} (driver {}) not found on node {}",
                        device_result.device,
                        device_result.pool,
                        device_result.driver,
                        node.metadata.name
                    );
                    return false;
                }
            }
        }

        // All resource claims are satisfied on this node
        true
    }
}

// Unit tests for the DisruptionTarget condition are verified inline:
#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Container, PodSpec, PodStatus};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;

    fn make_node(name: &str) -> Node {
        Node {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.labels = Some({
                    let mut labels = HashMap::new();
                    labels.insert("kubernetes.io/os".to_string(), "linux".to_string());
                    labels.insert("kubernetes.io/arch".to_string(), "amd64".to_string());
                    labels.insert("kubernetes.io/hostname".to_string(), name.to_string());
                    labels
                });
                m
            },
            spec: None,
            status: Some(rusternetes_common::resources::NodeStatus {
                conditions: Some(vec![rusternetes_common::resources::NodeCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: Some("KubeletReady".to_string()),
                    message: Some("kubelet is posting ready status".to_string()),
                    last_heartbeat_time: Some(Utc::now()),
                    last_transition_time: Some(Utc::now()),
                }]),
                capacity: Some({
                    let mut m = HashMap::new();
                    m.insert("cpu".to_string(), "4".to_string());
                    m.insert("memory".to_string(), "8Gi".to_string());
                    m.insert("pods".to_string(), "110".to_string());
                    m
                }),
                allocatable: Some({
                    let mut m = HashMap::new();
                    m.insert("cpu".to_string(), "4".to_string());
                    m.insert("memory".to_string(), "8Gi".to_string());
                    m.insert("pods".to_string(), "110".to_string());
                    m
                }),
                addresses: None,
                daemon_endpoints: None,
                node_info: None,
                images: None,
                volumes_in_use: None,
                volumes_attached: None,
                config: None,
                runtime_handlers: None,
                features: None,
            }),
        }
    }

    fn make_pending_pod(name: &str, ns: &str) -> Pod {
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some(ns.to_string());
                m
            },
            spec: Some(PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "main".to_string(),
                    image: "busybox".to_string(),
                    command: None,
                    args: None,
                    working_dir: None,
                    ports: None,
                    env: None,
                    env_from: None,
                    resources: None,
                    volume_mounts: None,
                    volume_devices: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    image_pull_policy: None,
                    security_context: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    resize_policy: None,
                    restart_policy: None,
                }],
                scheduler_name: Some("default-scheduler".to_string()),
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: None,
                termination_grace_period_seconds: None,
                dns_policy: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                automount_service_account_token: None,
                node_name: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                security_context: None,
                image_pull_secrets: None,
                hostname: None,
                subdomain: None,
                affinity: None,
                tolerations: None,
                host_aliases: None,
                priority_class_name: None,
                priority: None,
                preemption_policy: None,
                overhead: None,
                topology_spread_constraints: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_config: None,
                enable_service_links: None,
                readiness_gates: None,
                runtime_class_name: None,
                os: None,
                set_hostname_as_fqdn: None,
                share_process_namespace: None,
                scheduling_gates: None,
                resource_claims: None,
                host_users: None,
                resources: None,
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                host_ip: None,
                pod_ip: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                start_time: None,
                qos_class: None,
                nominated_node_name: None,
                host_i_ps: None,
                pod_i_ps: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
            }),
        }
    }

    #[tokio::test]
    async fn test_scheduler_assigns_pod_to_node() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        // Create two nodes
        let node1 = make_node("node-1");
        let node2 = make_node("node-2");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();
        storage
            .create("/registry/nodes/node-2", &node2)
            .await
            .unwrap();

        // Create a pending pod
        let pod = make_pending_pod("test-pod", "default");
        storage
            .create("/registry/pods/default/test-pod", &pod)
            .await
            .unwrap();

        // Run one scheduling cycle
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should now have a node name assigned
        let scheduled_pod: Pod = storage
            .get("/registry/pods/default/test-pod")
            .await
            .unwrap();
        let node_name = scheduled_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref());
        assert!(
            node_name.is_some(),
            "Pod should be assigned to a node after scheduling"
        );
        let node_name = node_name.unwrap();
        assert!(
            node_name == "node-1" || node_name == "node-2",
            "Pod should be on node-1 or node-2, got: {}",
            node_name
        );

        // Pod should have PodScheduled condition
        let conditions = scheduled_pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        assert!(conditions.is_some(), "Pod should have conditions");
        let has_scheduled = conditions
            .unwrap()
            .iter()
            .any(|c| c.condition_type == "PodScheduled" && c.status == "True");
        assert!(has_scheduled, "Pod should have PodScheduled=True condition");
    }

    #[tokio::test]
    async fn test_scheduler_emits_event_for_unschedulable_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        // Create a node with a label
        let node1 = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();

        // Create a pod with a nodeSelector that doesn't match any node
        let mut pod = make_pending_pod("unsched-pod", "default");
        if let Some(ref mut spec) = pod.spec {
            spec.node_selector = Some({
                let mut m = HashMap::new();
                m.insert("disktype".to_string(), "ssd".to_string());
                m
            });
        }
        storage
            .create("/registry/pods/default/unsched-pod", &pod)
            .await
            .unwrap();

        // Run scheduling
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should NOT have a node name
        let unsched_pod: Pod = storage
            .get("/registry/pods/default/unsched-pod")
            .await
            .unwrap();
        assert!(
            unsched_pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .is_none(),
            "Unschedulable pod should not be assigned to a node"
        );

        // Pod should have PodScheduled=False condition
        let conditions = unsched_pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        assert!(conditions.is_some(), "Pod should have conditions");
        let has_unschedulable = conditions.unwrap().iter().any(|c| {
            c.condition_type == "PodScheduled"
                && c.status == "False"
                && c.reason.as_deref() == Some("Unschedulable")
        });
        assert!(
            has_unschedulable,
            "Pod should have PodScheduled=False with Unschedulable reason"
        );

        // Should have a FailedScheduling event
        let events: Vec<serde_json::Value> = storage
            .list("/registry/events/default/")
            .await
            .unwrap_or_default();
        let has_failed_event = events.iter().any(|e| {
            e.get("reason")
                .and_then(|r| r.as_str())
                .map(|r| r == "FailedScheduling")
                .unwrap_or(false)
        });
        assert!(
            has_failed_event,
            "Should have a FailedScheduling event for unschedulable pod"
        );
    }

    #[tokio::test]
    async fn test_scheduler_does_not_reschedule_already_scheduled_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        let node1 = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();

        // Create a pod that is already scheduled
        let mut pod = make_pending_pod("scheduled-pod", "default");
        if let Some(ref mut spec) = pod.spec {
            spec.node_name = Some("node-1".to_string());
        }
        storage
            .create("/registry/pods/default/scheduled-pod", &pod)
            .await
            .unwrap();

        // Run scheduling — should not touch already-scheduled pod
        scheduler.schedule_pending_pods().await.unwrap();

        let result_pod: Pod = storage
            .get("/registry/pods/default/scheduled-pod")
            .await
            .unwrap();
        assert_eq!(
            result_pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref()),
            Some("node-1"),
            "Already-scheduled pod should remain on its node"
        );
    }

    /// Preemption should set deletionTimestamp and DisruptionTarget condition
    /// on evicted pods, but NOT delete them from storage. The kubelet handles
    /// actual cleanup. The conformance test needs to observe the condition.
    #[tokio::test]
    async fn test_preemption_sets_disruption_target_condition() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 2, "default-scheduler".to_string());

        // Create a node
        let node = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node)
            .await
            .unwrap();

        // Create a low-priority pod already scheduled on node-1
        let mut low_pod = make_pending_pod("low-pod", "default");
        low_pod.spec.as_mut().unwrap().node_name = Some("node-1".to_string());
        low_pod.spec.as_mut().unwrap().priority = Some(0);
        low_pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            pod_ip: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            start_time: None,
            qos_class: None,
            nominated_node_name: None,
            host_i_ps: None,
            pod_i_ps: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
        });
        storage
            .create("/registry/pods/default/low-pod", &low_pod)
            .await
            .unwrap();

        // Evict the pod (private method, accessible from within the module)
        scheduler.evict_pod("low-pod").await.unwrap();

        // The pod should still exist in storage with:
        // 1. deletionTimestamp set
        // 2. DisruptionTarget condition
        // 3. Phase = Failed
        let evicted: Pod = storage
            .get("/registry/pods/default/low-pod")
            .await
            .expect("Evicted pod should still exist in storage (not hard-deleted)");

        assert!(
            evicted.metadata.deletion_timestamp.is_some(),
            "Evicted pod should have deletionTimestamp set"
        );

        let status = evicted.status.as_ref().expect("Should have status");
        assert_eq!(
            status.phase,
            Some(Phase::Failed),
            "Evicted pod phase should be Failed"
        );
        assert_eq!(
            status.reason.as_deref(),
            Some("Preempted"),
            "Evicted pod reason should be Preempted"
        );

        let conditions = status.conditions.as_ref().expect("Should have conditions");
        let disruption = conditions
            .iter()
            .find(|c| c.condition_type == "DisruptionTarget");
        assert!(
            disruption.is_some(),
            "Evicted pod must have DisruptionTarget condition"
        );
        let dt = disruption.unwrap();
        assert_eq!(dt.status, "True");
        assert_eq!(dt.reason.as_deref(), Some("PreemptionByScheduler"));
    }

    /// K8s treats nodeName="" the same as nodeName=nil (unscheduled).
    /// Some Go JSON serialization produces "nodeName":"" in pod templates.
    /// The scheduler MUST consider these pods as pending and schedule them.
    /// K8s ref: pkg/scheduler/schedule_one.go — pod is unscheduled if
    /// len(pod.Spec.NodeName) == 0
    #[tokio::test]
    async fn test_empty_node_name_treated_as_unscheduled() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 5, "default-scheduler".to_string());

        // Create a node
        let node = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node)
            .await
            .unwrap();

        // Create a pod with nodeName="" (empty string, not None)
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pod").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                // Key: nodeName is Some("") — empty string, not None
                node_name: Some(String::new()),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/test-pod", &pod)
            .await
            .unwrap();

        // Schedule
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should be scheduled to node-1
        let scheduled: Pod = storage
            .get("/registry/pods/default/test-pod")
            .await
            .unwrap();
        assert!(
            scheduled
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                .is_some_and(|n| !n.is_empty()),
            "Pod with nodeName='' must be treated as unscheduled and get assigned a node"
        );
    }
}
