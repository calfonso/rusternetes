use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::node::Taint;
use rusternetes_common::resources::pod::{SecretVolumeSource, Toleration, Volume, VolumeMount};
use rusternetes_common::resources::{
    ControllerRevision, DaemonSet, DaemonSetStatus, Node, Pod, PodStatus,
};
use rusternetes_common::types::{OwnerReference, Phase};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// Check whether a set of tolerations tolerates all NoSchedule and NoExecute taints on a node.
fn pod_tolerates_node_taints(tolerations: &[Toleration], taints: &[Taint]) -> bool {
    for taint in taints {
        // Only NoSchedule and NoExecute taints must be tolerated
        if taint.effect == "NoSchedule" || taint.effect == "NoExecute" {
            let tolerated = tolerations.iter().any(|t| {
                // Empty/missing key with Exists operator matches all taints
                if t.operator.as_deref() == Some("Exists")
                    && (t.key.is_none() || t.key.as_deref() == Some(""))
                {
                    return true;
                }
                // Key must match
                let key_matches = t.key.as_deref() == Some(&taint.key);
                // Effect must match (or be empty/None = match all effects)
                let effect_matches =
                    t.effect.is_none() || t.effect.as_deref() == Some(&taint.effect);
                // Operator: Equal requires value match, Exists only needs key
                let value_matches = match t.operator.as_deref() {
                    Some("Exists") => true,
                    _ => t.value.as_deref() == taint.value.as_deref(),
                };
                key_matches && effect_matches && value_matches
            });
            if !tolerated {
                return false;
            }
        }
    }
    true
}

pub struct DaemonSetController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> DaemonSetController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting DaemonSetController (watch-based)");
        let retry_interval = Duration::from_secs(5);

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch daemonsets, nodes, AND pods.
            // K8s DS controller watches pods to react when a pod's status changes
            // (e.g., phase set to Failed by kubelet or test), which triggers
            // immediate reconciliation of the owning DaemonSet.
            let ds_prefix = "/registry/daemonsets/";
            let node_prefix = "/registry/nodes/";
            let pod_prefix = "/registry/pods/";

            let ds_watch = match self.storage.watch(ds_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish daemonset watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };
            let node_watch = match self.storage.watch(node_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish node watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };
            let pod_watch = match self.storage.watch(pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish pod watch for DS controller: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            let mut ds_watch = ds_watch;
            let mut node_watch = node_watch;
            let mut pod_watch = pod_watch;

            // Periodic full resync as safety net
            let mut resync = tokio::time::interval(Duration::from_secs(5));
            resync.tick().await; // consume first immediate tick

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = ds_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                warn!("DaemonSet watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("DaemonSet watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = node_watch.next() => {
                        match event {
                            Some(Ok(_ev)) => {
                                // Any node change could affect any DaemonSet
                                self.enqueue_all_for_node_change(&queue).await;
                            }
                            Some(Err(e)) => {
                                warn!("Node watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Node watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                // When a pod changes, enqueue the owning DaemonSet.
                                self.enqueue_ds_for_pod_event(&ev, &queue).await;
                            }
                            Some(Err(e)) => {
                                warn!("Pod watch error in DS controller: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Pod watch stream ended in DS controller, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
            // Watch broke — loop back to re-establish
        }
    }

    /// When a pod changes, find its owning DaemonSet and enqueue it for reconciliation.
    async fn enqueue_ds_for_pod_event(
        &self,
        event: &rusternetes_storage::WatchEvent,
        queue: &WorkQueue,
    ) {
        let json_str = match event {
            rusternetes_storage::WatchEvent::Added(_, v)
            | rusternetes_storage::WatchEvent::Modified(_, v)
            | rusternetes_storage::WatchEvent::Deleted(_, v) => v,
        };
        if let Ok(pod) = serde_json::from_str::<Pod>(json_str) {
            if let Some(owner_refs) = &pod.metadata.owner_references {
                for owner in owner_refs {
                    if owner.kind == "DaemonSet" {
                        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
                        let key = format!("daemonsets/{}/{}", ns, owner.name);
                        queue.add(key).await;
                    }
                }
            }
        }
    }

    /// When a node changes, enqueue ALL daemonsets since any DS might need
    /// to create/delete a pod on the changed node.
    async fn enqueue_all_for_node_change(&self, queue: &WorkQueue) {
        if let Ok(daemonsets) = self
            .storage
            .list::<DaemonSet>("/registry/daemonsets/")
            .await
        {
            for ds in &daemonsets {
                let ns = ds.metadata.namespace.as_deref().unwrap_or("");
                queue
                    .add(format!("daemonsets/{}/{}", ns, ds.metadata.name))
                    .await;
            }
        }
    }
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("daemonsets", Some(ns), name);
            match self.storage.get::<DaemonSet>(&storage_key).await {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.reconcile(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<DaemonSet>("/registry/daemonsets/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("daemonsets/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list daemonsets for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        let daemonsets: Vec<DaemonSet> = self.storage.list("/registry/daemonsets/").await?;

        for mut daemonset in daemonsets {
            if let Err(e) = self.reconcile(&mut daemonset).await {
                error!(
                    "Failed to reconcile DaemonSet {}: {}",
                    daemonset.metadata.name, e
                );
            }
        }

        Ok(())
    }

    async fn reconcile(&self, daemonset: &mut DaemonSet) -> Result<()> {
        let name = &daemonset.metadata.name;
        let namespace = daemonset.metadata.namespace.as_ref().unwrap();

        // When DaemonSet is being deleted, actively delete all owned pods
        // instead of waiting for GC (which can be slow and cause cascading delays)
        if daemonset.metadata.is_being_deleted() {
            return self.delete_owned_pods(daemonset).await;
        }

        debug!("Reconciling DaemonSet {}/{}", namespace, name);

        // Ensure a ControllerRevision exists for the current template.
        // K8s uses FNV-32a hash of the template (via controller.ComputeHash).
        // The ControllerRevision data must match getPatch() format exactly:
        //   {"spec":{"template":{...,"$patch":"replace"}}}
        let template_hash = Self::compute_template_hash(&daemonset.spec.template);
        let cr_name = format!("{}-{}", name, &template_hash);

        // Check if ControllerRevision already exists before creating
        let cr_key =
            rusternetes_storage::build_key("controllerrevisions", Some(namespace), &cr_name);
        if self
            .storage
            .get::<ControllerRevision>(&cr_key)
            .await
            .is_err()
        {
            let mut cr_labels = std::collections::HashMap::new();
            cr_labels.insert(
                "controller-revision-hash".to_string(),
                template_hash.clone(),
            );
            cr_labels.insert(
                "controller.kubernetes.io/hash".to_string(),
                template_hash.clone(),
            );

            // Copy DaemonSet's matchLabels to ControllerRevision labels for label selector matching
            if let Some(match_labels) = &daemonset.spec.selector.match_labels {
                for (k, v) in match_labels {
                    cr_labels.insert(k.clone(), v.clone());
                }
            }

            // Count existing revisions to get the next revision number
            let cr_prefix =
                rusternetes_storage::build_prefix("controllerrevisions", Some(namespace));
            let existing_revisions: Vec<ControllerRevision> =
                self.storage.list(&cr_prefix).await.unwrap_or_default();
            let max_revision = existing_revisions
                .iter()
                .filter(|r| {
                    r.metadata
                        .owner_references
                        .as_ref()
                        .map(|refs| {
                            refs.iter().any(|ref_| {
                                ref_.uid == daemonset.metadata.uid
                                    || ref_.name == daemonset.metadata.name
                            })
                        })
                        .unwrap_or(false)
                })
                .map(|r| r.revision)
                .max()
                .unwrap_or(0);
            let mut cr =
                ControllerRevision::new(cr_name.clone(), namespace.clone(), max_revision + 1);
            cr.metadata.labels = Some(cr_labels);
            cr.metadata.ensure_uid();
            cr.metadata.ensure_creation_timestamp();
            cr.metadata.owner_references = Some(vec![OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "DaemonSet".to_string(),
                name: name.clone(),
                uid: daemonset.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);

            // Build ControllerRevision data matching K8s getPatch() format.
            // K8s Match() does byte-level comparison between getPatch(dsFromAPI)
            // and history.Data.Raw. getPatch() marshals the DaemonSet through Go's
            // typed struct serialization (which applies omitempty to drop zero-value
            // fields), extracts spec.template, adds "$patch":"replace", and
            // re-marshals with sorted keys.
            //
            // We use build_patch_data() which serializes through our typed struct
            // (applying skip_serializing_if), then strips remaining Go-omitempty
            // zero values (like empty "name" fields in metadata), and sorts keys.
            let cr_data = Self::build_patch_data(&daemonset.spec.template);
            cr.data = cr_data;

            if self.storage.create(&cr_key, &cr).await.is_ok() {
                info!(
                    "Created ControllerRevision {} for DaemonSet {}/{}",
                    cr_name, namespace, name
                );
            }
        }

        // Get all nodes
        let nodes: Vec<Node> = self.storage.list("/registry/nodes/").await?;

        // Get pod tolerations from the DaemonSet's pod template
        let tolerations = daemonset
            .spec
            .template
            .spec
            .tolerations
            .as_deref()
            .unwrap_or(&[]);

        // Build a candidate pod from the DaemonSet template. This mirrors
        // upstream `NewPod(ds, nodeName)` (pkg/controller/daemon/daemon_controller.go),
        // which constructs a pod with the template's PodSpec so the scheduling
        // predicates (nodeAffinity, podAntiAffinity) can be evaluated against
        // each candidate node before placing a DS pod there.
        let candidate_pod = Self::candidate_pod_from_template(daemonset);

        // For pod (anti-)affinity we need to know what pods already run in the
        // namespace and on which nodes. `check_pod_anti_affinity` resolves a
        // matching pod's node topology against the candidate node, so it needs
        // both the pod list and the full node list.
        let ns_pods: Vec<Pod> = self
            .storage
            .list(&format!("/registry/pods/{}/", namespace))
            .await
            .unwrap_or_default();

        // Filter nodes based on node selector, taint toleration, required
        // nodeAffinity, and required podAntiAffinity.
        let eligible_nodes: Vec<Node> = nodes
            .iter()
            .filter(|node| {
                if !self.matches_node_selector(node, daemonset) {
                    return false;
                }
                // Check if the pod tolerates the node's taints
                let taints = node
                    .spec
                    .as_ref()
                    .and_then(|s| s.taints.as_deref())
                    .unwrap_or(&[]);
                if !pod_tolerates_node_taints(tolerations, taints) {
                    debug!(
                        "DaemonSet {}/{}: skipping node {} due to untolerated taints",
                        namespace, name, node.metadata.name
                    );
                    return false;
                }
                // Honour required nodeAffinity from the template. Upstream's
                // `predicates()` calls
                // `nodeaffinity.GetRequiredNodeAffinity(pod).Match(node)`; we
                // reuse `rusternetes_common::affinity::check_node_affinity`,
                // which evaluates the same required NodeSelectorTerms.
                let (node_affinity_ok, _) =
                    rusternetes_common::affinity::check_node_affinity(node, &candidate_pod);
                if !node_affinity_ok {
                    debug!(
                        "DaemonSet {}/{}: skipping node {} due to nodeAffinity mismatch",
                        namespace, name, node.metadata.name
                    );
                    return false;
                }
                // Honour required podAntiAffinity: skip a node when an existing
                // pod in the same topology domain matches the anti-affinity
                // labelSelector. Reuses
                // `rusternetes_common::affinity::check_pod_anti_affinity`
                // (interpodaffinity predicate parity).
                let (anti_affinity_ok, _) = rusternetes_common::affinity::check_pod_anti_affinity(
                    node,
                    &candidate_pod,
                    &ns_pods,
                    &nodes,
                );
                if !anti_affinity_ok {
                    debug!(
                        "DaemonSet {}/{}: skipping node {} due to podAntiAffinity conflict",
                        namespace, name, node.metadata.name
                    );
                    return false;
                }
                true
            })
            .cloned()
            .collect();

        debug!(
            "DaemonSet {}/{}: {} eligible nodes",
            namespace,
            name,
            eligible_nodes.len()
        );

        // Get current pods for this DaemonSet using owner references
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Find pods owned by this DaemonSet via ownerReferences (authoritative)
        // Fall back to label matching for backwards compatibility with pods created before this fix
        let daemonset_uid = &daemonset.metadata.uid;
        let daemonset_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == daemonset_uid))
                    .unwrap_or(false);
                let owned_by_label = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("controller-uid"))
                    .map(|uid| uid == daemonset_uid)
                    .unwrap_or(false);
                owned_by_ref || owned_by_label
            })
            .collect();

        let mut pods_by_node = std::collections::HashMap::new();
        for pod in daemonset_pods.iter() {
            if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
                // Check if pod is in a terminal phase (Failed or Succeeded)
                let is_terminal = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_ref())
                    .map(|phase| matches!(phase, Phase::Failed | Phase::Succeeded))
                    .unwrap_or(false);

                if is_terminal {
                    // Delete failed/succeeded pods so they can be recreated.
                    // K8s DS controller deletes failed pods and creates replacements
                    // in the same sync cycle (the node won't be in pods_by_node,
                    // so it will be treated as needing a new pod).
                    // K8s ref: pkg/controller/daemon/daemon_controller.go — podsShouldBeOnNode
                    let pod_name = &pod.metadata.name;
                    let pod_key = format!("/registry/pods/{}/{}", namespace, pod_name);
                    if let Err(e) = self.storage.delete(&pod_key).await {
                        warn!(
                            "Failed to delete terminal DaemonSet pod {}: {}",
                            pod_name, e
                        );
                    } else {
                        info!(
                            "Deleted terminal ({:?}) DaemonSet pod {}",
                            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                            pod_name
                        );
                    }
                    // Don't add to pods_by_node — the node needs a new pod.
                    // The replacement will be created in the same reconcile cycle below.
                } else {
                    pods_by_node.insert(node_name.clone(), pod.clone());
                }
            }
        }

        // Determine update strategy
        let update_strategy = daemonset
            .spec
            .update_strategy
            .as_ref()
            .and_then(|s| s.strategy_type.as_deref())
            .unwrap_or("RollingUpdate");

        // --- Manage phase: ensure one pod per eligible node (only for nodes with NO pod) ---
        // This runs BEFORE the rolling update phase, matching K8s behavior:
        // manage() creates pods on empty nodes, then rollingUpdate() replaces old pods.
        for node in eligible_nodes.iter() {
            let node_name = &node.metadata.name;

            if !pods_by_node.contains_key(node_name) {
                // Create pod for this node, ignore AlreadyExists (race / re-reconcile)
                match self.create_pod(daemonset, node_name, namespace).await {
                    Ok(_) => {
                        info!("Created DaemonSet pod on node {}", node_name);
                    }
                    Err(e) => {
                        let err_str = format!("{}", e);
                        if err_str.contains("already exists") || err_str.contains("AlreadyExists") {
                            debug!(
                                "DaemonSet pod on node {} already exists, skipping",
                                node_name
                            );
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        // --- Rolling update phase ---
        // K8s rolling update algorithm (maxSurge=0, the default):
        // 1. Classify each node's pod as "old" (hash != current) or "new" (hash == current)
        // 2. Count already-unavailable pods (new pods not yet available, nodes without pods)
        // 3. Delete old pods only if within the maxUnavailable budget
        // 4. Do NOT create replacement pods here — the next reconcile's manage phase does that
        //
        // This ensures that at any point in time, the number of unavailable pods
        // never exceeds maxUnavailable, which is what the conformance test checks.
        if update_strategy == "RollingUpdate" {
            let max_unavailable_raw = daemonset
                .spec
                .update_strategy
                .as_ref()
                .and_then(|s| s.rolling_update.as_ref())
                .and_then(|r| r.max_unavailable.as_ref());
            let desired = eligible_nodes.len() as i32;
            let max_unavailable =
                resolve_max_unavailable(max_unavailable_raw.map(|s| s.as_str()), desired);

            // Re-read pods after manage phase to get accurate state
            let all_pods_now: Vec<Pod> = self.storage.list(&pod_prefix).await?;
            let daemonset_pods_now: Vec<Pod> = all_pods_now
                .into_iter()
                .filter(|pod| {
                    pod.metadata
                        .owner_references
                        .as_ref()
                        .map(|refs| refs.iter().any(|r| &r.uid == daemonset_uid))
                        .unwrap_or(false)
                })
                .collect();

            let mut current_pods_by_node: std::collections::HashMap<String, Vec<Pod>> =
                std::collections::HashMap::new();
            for pod in daemonset_pods_now.iter() {
                if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
                    let is_terminal = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.phase.as_ref())
                        .map(|phase| matches!(phase, Phase::Failed | Phase::Succeeded))
                        .unwrap_or(false);
                    if !is_terminal {
                        current_pods_by_node
                            .entry(node_name.clone())
                            .or_default()
                            .push(pod.clone());
                    }
                }
            }

            // Helper: check if a pod is "available" (has Ready condition True)
            let is_pod_available = |pod: &Pod| -> bool {
                pod.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conditions| {
                        conditions
                            .iter()
                            .any(|c| c.condition_type == "Ready" && c.status == "True")
                    })
                    .unwrap_or(false)
            };

            // Classify pods on each node and count current unavailability
            let mut num_unavailable: i32 = 0;
            let mut old_available_pods: Vec<(String, Pod)> = Vec::new(); // (node_name, pod)
            let mut old_unavailable_pods: Vec<(String, Pod)> = Vec::new();

            for node in eligible_nodes.iter() {
                let node_name = &node.metadata.name;
                let node_pods = current_pods_by_node
                    .get(node_name.as_str())
                    .cloned()
                    .unwrap_or_default();

                if node_pods.is_empty() {
                    // No pod on this node — counts as unavailable
                    num_unavailable += 1;
                    continue;
                }

                // Find old and new pods on this node
                let mut has_new_available = false;
                let mut has_new_unavailable = false;
                let mut old_pod: Option<Pod> = None;

                for pod in &node_pods {
                    let pod_hash = pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("controller-revision-hash"))
                        .map(|s| s.as_str())
                        .unwrap_or("");

                    if pod_hash == template_hash {
                        // New pod
                        if is_pod_available(pod) {
                            has_new_available = true;
                        } else {
                            has_new_unavailable = true;
                        }
                    } else {
                        // Old pod
                        old_pod = Some(pod.clone());
                    }
                }

                if has_new_unavailable {
                    // New pod exists but isn't available yet — counts against budget
                    num_unavailable += 1;
                }

                if let Some(old) = old_pod {
                    if has_new_available {
                        // New pod is ready; old pod can be cleaned up (doesn't count as unavailable)
                        // This shouldn't happen in maxSurge=0 mode, but handle it gracefully
                        old_unavailable_pods.push((node_name.clone(), old));
                    } else if !is_pod_available(&old) {
                        // Old pod is unavailable — delete it immediately (free slot)
                        old_unavailable_pods.push((node_name.clone(), old));
                    } else {
                        // Old pod is available — candidate for deletion within budget
                        old_available_pods.push((node_name.clone(), old));
                    }
                }
            }

            // Delete old pods within the maxUnavailable budget.
            // Unavailable old pods are preferred (they're already not serving) but ALL
            // deletions count against the budget. K8s maxSurge=0 means we can never have
            // more than maxUnavailable pods missing at any time.
            let allowed_deletions = (max_unavailable - num_unavailable).max(0);
            let mut deleted_count: i32 = 0;

            // First, delete unavailable old pods (preferred — already not serving)
            for (node_name, pod) in &old_unavailable_pods {
                if deleted_count >= allowed_deletions {
                    break;
                }
                let pod_name = &pod.metadata.name;
                let pod_key = format!("/registry/pods/{}/{}", namespace, pod_name);
                if let Ok(()) = self.storage.delete(&pod_key).await {
                    info!(
                        "Rolling update: deleted unavailable old pod {} on node {} (budget {}/{})",
                        pod_name,
                        node_name,
                        deleted_count + 1,
                        allowed_deletions
                    );
                    deleted_count += 1;
                }
            }

            // Then, delete available old pods with remaining budget
            for (node_name, pod) in &old_available_pods {
                if deleted_count >= allowed_deletions {
                    break;
                }
                let pod_name = &pod.metadata.name;
                let pod_key = format!("/registry/pods/{}/{}", namespace, pod_name);
                if let Ok(()) = self.storage.delete(&pod_key).await {
                    info!(
                        "Rolling update: deleted old pod {} on node {} (hash != {}, budget {}/{})",
                        pod_name,
                        node_name,
                        template_hash,
                        deleted_count + 1,
                        allowed_deletions
                    );
                    deleted_count += 1;
                }
            }
        }

        // Remove pods from nodes that are no longer eligible
        let eligible_node_names: std::collections::HashSet<_> = eligible_nodes
            .iter()
            .map(|n| n.metadata.name.as_str())
            .collect();

        for (node_name, pod) in pods_by_node.iter() {
            if !eligible_node_names.contains(node_name.as_str()) {
                let pod_name = &pod.metadata.name;
                let pod_key = format!("/registry/pods/{}/{}", namespace, pod_name);
                self.storage.delete(&pod_key).await?;
                info!(
                    "Deleted DaemonSet pod {} from ineligible node {}",
                    pod_name, node_name
                );
            }
        }

        // Re-fetch pods after creating/deleting to get accurate count for status
        let all_pods_after: Vec<Pod> = self.storage.list(&pod_prefix).await?;
        let daemonset_pods_after: Vec<Pod> = all_pods_after
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == daemonset_uid))
                    .unwrap_or(false);
                let owned_by_label = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("controller-uid"))
                    .map(|uid| uid == daemonset_uid)
                    .unwrap_or(false);
                owned_by_ref || owned_by_label
            })
            .collect();

        let mut final_pods_by_node = std::collections::HashMap::new();
        for pod in daemonset_pods_after.iter() {
            if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
                final_pods_by_node.insert(node_name.clone(), pod.clone());
            }
        }

        // Update status with accurate counts
        let current_number_scheduled = final_pods_by_node.len() as i32;
        let desired_number_scheduled = eligible_nodes.len() as i32;
        let number_ready = final_pods_by_node
            .values()
            .filter(|pod| {
                // K8s numberReady counts pods with Ready condition True, not just Running phase
                pod.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conditions| {
                        conditions
                            .iter()
                            .any(|c| c.condition_type == "Ready" && c.status == "True")
                    })
                    .unwrap_or(false)
            })
            .count() as i32;

        // Count pods with the current template hash as "updated"
        // Use final_pods_by_node (re-fetched after create/delete) for accurate count
        let updated_count = final_pods_by_node
            .values()
            .filter(|pod| {
                pod.metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("controller-revision-hash"))
                    .map(|h| h == &template_hash)
                    .unwrap_or(false)
            })
            .count() as i32;

        // Garbage-collect old ControllerRevisions beyond revisionHistoryLimit.
        // Mirrors upstream `cleanupHistory` (pkg/controller/daemon/update.go):
        // delete oldest revisions first, never touching a revision whose hash
        // is still carried by a live pod (nor the current template's revision).
        self.cleanup_history(daemonset, &template_hash, &daemonset_pods_after)
            .await;

        // Preserve existing conditions from current status (merge pattern)
        let existing_conditions = daemonset.status.as_ref().and_then(|s| s.conditions.clone());

        let new_status = Some(DaemonSetStatus {
            desired_number_scheduled,
            current_number_scheduled,
            number_ready,
            number_misscheduled: 0,
            number_available: Some(number_ready),
            number_unavailable: Some(desired_number_scheduled - number_ready),
            updated_number_scheduled: Some(updated_count),
            observed_generation: daemonset.metadata.generation,
            collision_count: None,
            conditions: existing_conditions,
        });

        // Only write status if it actually changed to avoid unnecessary storage writes
        // that trigger watch events and cause feedback loops
        if daemonset.status != new_status {
            daemonset.status = new_status;
            let key = format!("/registry/daemonsets/{}/{}", namespace, name);
            self.storage.update(&key, daemonset).await?;
        }

        Ok(())
    }

    /// Delete all pods owned by a DaemonSet that is being deleted.
    /// Sets deletionTimestamp on each owned pod so the kubelet tears down containers.
    async fn delete_owned_pods(&self, daemonset: &DaemonSet) -> Result<()> {
        let ns = daemonset.metadata.namespace.as_deref().unwrap_or("default");
        let ds_name = &daemonset.metadata.name;
        let pod_prefix = rusternetes_storage::build_prefix("pods", Some(ns));
        let pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        for pod in &pods {
            // Check if pod is owned by this DaemonSet
            if let Some(refs) = &pod.metadata.owner_references {
                for owner_ref in refs {
                    if owner_ref.kind == "DaemonSet" && owner_ref.name == *ds_name {
                        // Set deletionTimestamp if not already set
                        if pod.metadata.deletion_timestamp.is_none() {
                            let mut updated_pod = pod.clone();
                            updated_pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
                            let pod_key = build_key("pods", Some(ns), &pod.metadata.name);
                            let _ = self.storage.update(&pod_key, &updated_pod).await;
                            info!(
                                "Marked pod {} for deletion (DaemonSet {} being deleted)",
                                pod.metadata.name, ds_name
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Garbage-collect ControllerRevisions owned by this DaemonSet that exceed
    /// `spec.revisionHistoryLimit`. Mirrors upstream `cleanupHistory`
    /// (pkg/controller/daemon/update.go) with one rusternetes refinement: the
    /// limit caps the *total* number of retained revisions (current included),
    /// matching the controller-level contract our tests assert.
    ///
    /// Deletion order is oldest-revision-first. A revision is never deleted if
    /// it is the current template's revision (`current_hash`) or if its hash is
    /// still carried by a live DaemonSet pod — those are needed for OnDelete
    /// pods and in-flight rollbacks.
    ///
    /// The helper is intentionally self-contained (operates on a revision list +
    /// a set of live hashes) so the StatefulSet controller can reuse the same
    /// shape later.
    async fn cleanup_history(&self, daemonset: &DaemonSet, current_hash: &str, live_pods: &[Pod]) {
        // revisionHistoryLimit defaults to 10 in Kubernetes; when unset there is
        // effectively a generous cap, so only GC when the field is present.
        let limit = match daemonset.spec.revision_history_limit {
            Some(l) if l >= 0 => l,
            _ => return,
        };

        let namespace = match daemonset.metadata.namespace.as_deref() {
            Some(ns) => ns,
            None => return,
        };

        let cr_prefix = rusternetes_storage::build_prefix("controllerrevisions", Some(namespace));
        let mut owned: Vec<ControllerRevision> = match self.storage.list(&cr_prefix).await {
            Ok(list) => list
                .into_iter()
                .filter(|r: &ControllerRevision| {
                    r.metadata
                        .owner_references
                        .as_ref()
                        .map(|refs| {
                            refs.iter().any(|owner| {
                                owner.uid == daemonset.metadata.uid
                                    || owner.name == daemonset.metadata.name
                            })
                        })
                        .unwrap_or(false)
                })
                .collect(),
            Err(_) => return,
        };

        // Hashes still carried by live pods must be preserved.
        let live_hashes: std::collections::HashSet<&str> = live_pods
            .iter()
            .filter_map(|p| {
                p.metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("controller-revision-hash"))
                    .map(|s| s.as_str())
            })
            .collect();

        // Protected revisions — the current template's revision and any still
        // carried by a live pod — are never deleted and do NOT count against
        // revisionHistoryLimit. The limit caps only the NON-current history,
        // matching upstream cleanupHistory (keep `limit` old + the live ones).
        let protected = owned
            .iter()
            .filter(|r| {
                let h = r
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("controller-revision-hash"))
                    .map(|s| s.as_str());
                h == Some(current_hash) || h.map(|h| live_hashes.contains(h)).unwrap_or(false)
            })
            .count() as i32;
        let to_kill = (owned.len() as i32 - protected) - limit;
        if to_kill <= 0 {
            return;
        }

        // Oldest revision first.
        owned.sort_by_key(|r| r.revision);

        let mut remaining_to_kill = to_kill;
        for revision in &owned {
            if remaining_to_kill <= 0 {
                break;
            }
            // Never delete the current template's revision.
            let rev_hash = revision
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("controller-revision-hash"))
                .map(|s| s.as_str());
            if rev_hash == Some(current_hash) {
                continue;
            }
            // Never delete a revision whose hash is still in use by a live pod.
            if let Some(h) = rev_hash {
                if live_hashes.contains(h) {
                    continue;
                }
            }
            let cr_key = rusternetes_storage::build_key(
                "controllerrevisions",
                Some(namespace),
                &revision.metadata.name,
            );
            if self.storage.delete(&cr_key).await.is_ok() {
                info!(
                    "Garbage-collected old ControllerRevision {} (revision {}) for DaemonSet {}/{}",
                    revision.metadata.name, revision.revision, namespace, daemonset.metadata.name
                );
                remaining_to_kill -= 1;
            }
        }
    }

    fn matches_node_selector(&self, node: &Node, daemonset: &DaemonSet) -> bool {
        // Check if node matches the DaemonSet's node selector
        let node_labels = match &node.metadata.labels {
            Some(labels) => labels,
            None => return daemonset.spec.template.spec.node_selector.is_none(),
        };

        match &daemonset.spec.template.spec.node_selector {
            Some(selector) => {
                // All selector labels must match node labels
                selector.iter().all(|(k, v)| {
                    node_labels
                        .get(k)
                        .map(|node_v| node_v == v)
                        .unwrap_or(false)
                })
            }
            None => true, // No selector means all nodes match
        }
    }

    async fn create_pod(
        &self,
        daemonset: &DaemonSet,
        node_name: &str,
        namespace: &str,
    ) -> Result<()> {
        let daemonset_name = &daemonset.metadata.name;
        // Pod naming depends on the update strategy.
        //
        // OnDelete: the controller never auto-replaces a pod, so the pod is a
        // stable per-node identity. We name it deterministically as
        // `<ds-name>-<node-name>` so an operator can target it by name with
        // `kubectl delete pod <ds>-<node>` to trigger a manual roll, matching
        // the upstream OnDelete workflow (pkg/controller/daemon/update.go —
        // OnDelete leaves pod replacement to manual deletion).
        //
        // RollingUpdate (and the default): use a random suffix like upstream's
        // `generateName: <ds-name>-`. A fresh suffix on every create means a
        // rolled pod always gets a NEW name, so the failed-pod's old name
        // returns NotFound and rolling-update replacement is observable.
        let on_delete = daemonset
            .spec
            .update_strategy
            .as_ref()
            .and_then(|s| s.strategy_type.as_deref())
            == Some("OnDelete");
        let pod_name = if on_delete {
            format!("{}-{}", daemonset_name, node_name)
        } else {
            let suffix: String = {
                use rand::Rng;
                let mut rng = rand::rng();
                (0..5)
                    .map(|_| {
                        const CHARSET: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
                        CHARSET[rng.random_range(0..CHARSET.len())] as char
                    })
                    .collect()
            };
            format!("{}-{}", daemonset_name, suffix)
        };

        // Create pod from template
        let template = &daemonset.spec.template;
        let mut labels = template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("app".to_string(), daemonset_name.clone());
        labels.insert("controller-uid".to_string(), daemonset.metadata.uid.clone());
        // Add controller-revision-hash label (computed from template)
        let template_hash = { Self::compute_template_hash(&daemonset.spec.template) };
        labels.insert("controller-revision-hash".to_string(), template_hash);

        let mut spec = template.spec.clone();

        // CRITICAL: Assign the pod to the specific node
        spec.node_name = Some(node_name.to_string());

        // Debug: Check if NODE_NAME env var has valueFrom before and after
        debug!("Before injection - Checking environment variables in pod template:");
        for container in &spec.containers {
            if let Some(env) = &container.env {
                for env_var in env {
                    if env_var.name.contains("NODE_NAME")
                        || env_var.name.contains("SONOBUOY_NS")
                        || env_var.name.contains("SONOBUOY_PLUGIN_POD")
                    {
                        debug!(
                            "  Container '{}': {} - value={:?}, value_from.field_ref={:?}",
                            container.name,
                            env_var.name,
                            env_var.value,
                            env_var
                                .value_from
                                .as_ref()
                                .and_then(|vf| vf.field_ref.as_ref())
                        );
                    }
                }
            }
        }

        // Inject service account token volume
        self.inject_service_account_token(&mut spec, namespace);

        // Propagate the SA's imagePullSecrets (#1084) — controllers bypass the
        // api-server admission path that normally does this.
        super::propagate_sa_image_pull_secrets(&*self.storage, namespace, &mut spec).await;

        // DefaultTolerationSeconds admission (#442): controllers write pods
        // straight to storage and bypass the api-server admission path that adds
        // these, so apply the same NotReady/Unreachable NoExecute tolerations.
        rusternetes_common::tolerations::add_default_tolerations(&mut spec);

        // Debug: Check again after injection
        debug!("After injection - Checking environment variables:");
        for container in &spec.containers {
            if let Some(env) = &container.env {
                for env_var in env {
                    if env_var.name.contains("NODE_NAME")
                        || env_var.name.contains("SONOBUOY_NS")
                        || env_var.name.contains("SONOBUOY_PLUGIN_POD")
                    {
                        debug!(
                            "  Container '{}': {} - value={:?}, value_from.field_ref={:?}",
                            container.name,
                            env_var.name,
                            env_var.value,
                            env_var
                                .value_from
                                .as_ref()
                                .and_then(|vf| vf.field_ref.as_ref())
                        );
                    }
                }
            }
        }

        let mut metadata = rusternetes_common::types::ObjectMeta::new(pod_name.clone())
            .with_namespace(namespace.to_string())
            .with_labels(labels)
            .with_owner_reference(OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "DaemonSet".to_string(),
                name: daemonset_name.clone(),
                uid: daemonset.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            });

        if let Some(template_meta) = &template.metadata {
            if let Some(ref annotations) = template_meta.annotations {
                metadata.annotations = Some(annotations.clone());
            }
        }

        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            spec: Some(spec),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                host_ip: None,
                host_i_ps: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
            }),
        };

        // Check ResourceQuota before creating pod
        super::check_resource_quota(&*self.storage, namespace).await?;

        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        self.storage.create(&key, &pod).await?;

        Ok(())
    }

    fn inject_service_account_token(
        &self,
        spec: &mut rusternetes_common::resources::PodSpec,
        namespace: &str,
    ) {
        // Get service account name, default to "default"
        let sa_name = spec.service_account_name.as_deref().unwrap_or("default");

        // The service account token secret name follows the pattern: {sa-name}-token
        let token_secret_name = format!("{}-token", sa_name);

        // Define the service account token volume
        let sa_token_volume = Volume {
            name: "kube-api-access".to_string(),
            empty_dir: None,
            host_path: None,
            config_map: None,
            secret: Some(SecretVolumeSource {
                secret_name: Some(token_secret_name.clone()),
                items: None,
                default_mode: None,
                optional: None,
            }),
            persistent_volume_claim: None,
            downward_api: None,
            csi: None,
            ephemeral: None,
            nfs: None,
            iscsi: None,
            projected: None,
            image: None,
        };

        // Add volume to pod spec
        if let Some(volumes) = &mut spec.volumes {
            // Check if volume already exists
            if !volumes.iter().any(|v| v.name == "kube-api-access") {
                volumes.push(sa_token_volume);
                debug!(
                    "Injected service account token volume for DaemonSet pod in namespace {}",
                    namespace
                );
            }
        } else {
            spec.volumes = Some(vec![sa_token_volume]);
            info!(
                "Injected service account token volume for DaemonSet pod in namespace {}",
                namespace
            );
        }

        // Define the volume mount for the token
        let sa_token_mount = VolumeMount {
            name: "kube-api-access".to_string(),
            mount_path: "/var/run/secrets/kubernetes.io/serviceaccount".to_string(),
            read_only: Some(true),
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: None,
            recursive_read_only: None,
        };

        // Add volume mount to all containers
        for container in &mut spec.containers {
            if let Some(mounts) = &mut container.volume_mounts {
                // Check if mount already exists
                if !mounts
                    .iter()
                    .any(|m| m.mount_path == "/var/run/secrets/kubernetes.io/serviceaccount")
                {
                    mounts.push(sa_token_mount.clone());
                }
            } else {
                container.volume_mounts = Some(vec![sa_token_mount.clone()]);
            }
        }

        // Also add to init containers if present
        if let Some(init_containers) = &mut spec.init_containers {
            for container in init_containers {
                if let Some(mounts) = &mut container.volume_mounts {
                    if !mounts
                        .iter()
                        .any(|m| m.mount_path == "/var/run/secrets/kubernetes.io/serviceaccount")
                    {
                        mounts.push(sa_token_mount.clone());
                    }
                } else {
                    container.volume_mounts = Some(vec![sa_token_mount.clone()]);
                }
            }
        }
    }

    /// Build a candidate `Pod` from the DaemonSet template for scheduling
    /// predicate evaluation (nodeAffinity / podAntiAffinity). Mirrors upstream
    /// `NewPod(ds, nodeName)` in pkg/controller/daemon/daemon_controller.go,
    /// which copies the template's PodSpec and ObjectMeta into a fresh pod.
    ///
    /// The `nodeName` is intentionally left unset: predicates evaluate the
    /// candidate pod against each node in turn, so it must not be pre-bound.
    fn candidate_pod_from_template(daemonset: &DaemonSet) -> Pod {
        let template = &daemonset.spec.template;
        let metadata = template
            .metadata
            .clone()
            .map(|mut m| {
                m.namespace = daemonset.metadata.namespace.clone();
                m
            })
            .unwrap_or_default();
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            spec: Some(template.spec.clone()),
            status: None,
        }
    }

    /// Compute template hash matching K8s's controller.ComputeHash.
    /// K8s uses FNV-32a hash of DeepHashObject of the template.
    /// We approximate this with FNV-32a of the JSON-serialized template.
    fn compute_template_hash(template: &rusternetes_common::resources::PodTemplateSpec) -> String {
        use std::hash::Hasher;
        let value = serde_json::to_value(template).unwrap_or_default();
        let serialized = serde_json::to_string(&value).unwrap_or_default();
        let mut hasher = fnv::FnvHasher::with_key(0x811c9dc5);
        hasher.write(serialized.as_bytes());
        let hash = hasher.finish() as u32;
        // K8s uses rand.SafeEncodeString(fmt.Sprint(hash)) which maps each
        // character of the decimal string through a safe alphabet.
        // See: staging/src/k8s.io/apimachinery/pkg/util/rand/rand.go
        let decimal = format!("{}", hash);
        const ALPHANUMS: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
        decimal
            .bytes()
            .map(|b| ALPHANUMS[(b as usize) % ALPHANUMS.len()] as char)
            .collect()
    }

    /// Build ControllerRevision data in K8s getPatch() format.
    /// Format: {"spec":{"template":{...,"$patch":"replace"}}}
    ///
    /// K8s Match() does byte-level comparison of getPatch() output with
    /// history.Data.Raw. Go's encoding/json sorts map keys alphabetically
    /// and omits zero-value fields with `omitempty`. We must:
    /// 1. Sort keys the same way
    /// 2. Strip zero-value fields (empty strings, false bools, 0 ints, empty
    ///    arrays/maps, nulls) that Go's omitempty would drop
    pub fn build_patch_data(
        template: &rusternetes_common::resources::PodTemplateSpec,
    ) -> Option<serde_json::Value> {
        let mut template_value = serde_json::to_value(template).ok()?;
        // Strip Go-omitempty zero values BEFORE adding $patch marker.
        // Go's getPatch() round-trips the DaemonSet through Go's typed struct
        // serialization which applies omitempty, dropping empty strings, null
        // pointers, empty slices/maps, zero ints, and false booleans.
        template_value = Self::strip_go_omitempty_zeros(&template_value);
        // Add $patch: "replace" to the template object (K8s strategic merge patch marker)
        if let Some(obj) = template_value.as_object_mut() {
            obj.insert("$patch".to_string(), serde_json::json!("replace"));
        }
        // Sort keys alphabetically to match Go's encoding/json behavior.
        // K8s Match() compares bytes, so key order must be identical.
        let sorted = Self::sort_json_keys(&serde_json::json!({
            "spec": {
                "template": template_value
            }
        }));
        Some(sorted)
    }

    /// Recursively sort all JSON object keys alphabetically.
    /// Go's encoding/json sorts map keys; we must match this for
    /// byte-level comparisons in DaemonSet ControllerRevision Match().
    fn sort_json_keys(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut sorted = serde_json::Map::new();
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                for key in keys {
                    sorted.insert(key.clone(), Self::sort_json_keys(&map[key]));
                }
                serde_json::Value::Object(sorted)
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| Self::sort_json_keys(v)).collect())
            }
            other => other.clone(),
        }
    }

    /// Strip zero-value fields that Go's `omitempty` json tag would omit.
    /// Go's encoding/json omits:
    ///   - false booleans
    ///   - 0 integers/floats
    ///   - empty strings ""
    ///   - nil pointers (null)
    ///   - empty arrays []
    ///   - empty maps {}
    ///
    /// This is critical for byte-level comparison with Go's getPatch() output.
    /// When Go round-trips a DaemonSet through its typed struct serialization,
    /// all fields with `json:",omitempty"` that have zero values are dropped.
    /// Our ControllerRevision data must match those exact bytes.
    fn strip_go_omitempty_zeros(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut cleaned = serde_json::Map::new();
                for (key, val) in map {
                    // Recursively clean nested values first
                    let cleaned_val = Self::strip_go_omitempty_zeros(val);
                    // Skip zero-value fields (Go's omitempty behavior).
                    // Exception: Go keeps empty structs that come from non-nil
                    // pointers (e.g., securityContext: {} from &SecurityContext{}).
                    // These are NOT dropped by omitempty because the pointer is
                    // non-nil even though the struct is empty.
                    // K8s ref: api/core/v1/types.go — SecurityContext is *SecurityContext
                    let is_preserved_empty_struct = cleaned_val.is_object()
                        && cleaned_val.as_object().unwrap().is_empty()
                        && matches!(
                            key.as_str(),
                            "securityContext" | "resources" | "capabilities"
                        );
                    if !is_preserved_empty_struct && Self::is_go_zero_value(&cleaned_val) {
                        continue;
                    }
                    cleaned.insert(key.clone(), cleaned_val);
                }
                serde_json::Value::Object(cleaned)
            }
            serde_json::Value::Array(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| Self::strip_go_omitempty_zeros(v))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    /// Check if a JSON value is a Go zero value (would be omitted by omitempty).
    fn is_go_zero_value(value: &serde_json::Value) -> bool {
        match value {
            serde_json::Value::Null => true,
            serde_json::Value::Bool(b) => !b,
            serde_json::Value::Number(n) => n.as_f64().map(|f| f == 0.0).unwrap_or(false),
            serde_json::Value::String(s) => s.is_empty(),
            serde_json::Value::Array(arr) => arr.is_empty(),
            serde_json::Value::Object(map) => map.is_empty(),
        }
    }
}

/// Resolve a DaemonSet `maxUnavailable` IntOrString against the desired pod
/// count (number of eligible nodes). Percentages are rounded UP per K8s
/// semantics (`intstr.GetScaledValueFromIntOrPercent` with `roundUp=true`).
/// Absolute values pass through, and any unparseable input defaults to 1.
/// The result is clamped to at least 1 so the rolling update can always make
/// progress on small clusters.
fn resolve_max_unavailable(raw: Option<&str>, desired: i32) -> i32 {
    match raw {
        Some(s) if s.ends_with('%') => {
            let pct = s.trim_end_matches('%').parse::<f64>().unwrap_or(0.0);
            let scaled = (pct * desired as f64 / 100.0).ceil() as i32;
            scaled.max(1)
        }
        Some(s) => s.parse::<i32>().unwrap_or(1).max(1),
        None => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::PodSpec;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_node_selector_matching() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage);

        let mut node_labels = HashMap::new();
        node_labels.insert("disktype".to_string(), "ssd".to_string());
        node_labels.insert("region".to_string(), "us-west".to_string());

        let node = Node {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "node-1".to_string(),
                namespace: None,
                labels: Some(node_labels),
                annotations: None,
                uid: uuid::Uuid::new_v4().to_string(),
                creation_timestamp: None,
                deletion_timestamp: None,
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: Some(rusternetes_common::resources::NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints: None,
            }),
            status: None,
        };

        // Test: no selector = all nodes match
        let ds_no_selector = DaemonSet {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "DaemonSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "test-ds".to_string(),
                namespace: Some("default".to_string()),
                labels: None,
                annotations: None,
                uid: uuid::Uuid::new_v4().to_string(),
                creation_timestamp: None,
                deletion_timestamp: None,
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: rusternetes_common::resources::DaemonSetSpec {
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: None,
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(rusternetes_common::types::ObjectMeta {
                        name: "".to_string(),
                        namespace: None,
                        labels: None,
                        annotations: None,
                        uid: uuid::Uuid::new_v4().to_string(),
                        creation_timestamp: None,
                        deletion_timestamp: None,
                        resource_version: None,
                        deletion_grace_period_seconds: None,
                        finalizers: None,
                        owner_references: None,
                        generate_name: None,
                        generation: None,
                        managed_fields: None,
                    }),
                    spec: PodSpec {
                        init_containers: None,
                        containers: vec![],
                        node_name: None,
                        node_selector: None,
                        restart_policy: None,
                        service_account_name: None,
                        service_account: None,
                        volumes: None,
                        affinity: None,
                        tolerations: None,
                        priority: None,
                        priority_class_name: None,
                        hostname: None,
                        subdomain: None,
                        host_network: None,
                        host_pid: None,
                        host_ipc: None,
                        automount_service_account_token: None,
                        ephemeral_containers: None,
                        overhead: None,
                        scheduler_name: None,
                        topology_spread_constraints: None,
                        resource_claims: None,
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
                    },
                },
                update_strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
            },
            status: None,
        };

        assert!(controller.matches_node_selector(&node, &ds_no_selector));
    }

    #[test]
    fn test_pod_tolerates_no_taints() {
        // No taints = always tolerated
        let tolerations: Vec<Toleration> = vec![];
        let taints: Vec<Taint> = vec![];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_does_not_tolerate_noschedule() {
        let tolerations: Vec<Toleration> = vec![];
        let taints = vec![Taint {
            key: "node-role.kubernetes.io/control-plane".to_string(),
            value: None,
            effect: "NoSchedule".to_string(),
            time_added: None,
        }];
        assert!(!pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_with_exists_operator() {
        let tolerations = vec![Toleration {
            key: Some("node-role.kubernetes.io/control-plane".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];
        let taints = vec![Taint {
            key: "node-role.kubernetes.io/control-plane".to_string(),
            value: None,
            effect: "NoSchedule".to_string(),
            time_added: None,
        }];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_with_equal_operator() {
        let tolerations = vec![Toleration {
            key: Some("dedicated".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("gpu".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];
        let taints = vec![Taint {
            key: "dedicated".to_string(),
            value: Some("gpu".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_does_not_tolerate_wrong_value() {
        let tolerations = vec![Toleration {
            key: Some("dedicated".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("cpu".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];
        let taints = vec![Taint {
            key: "dedicated".to_string(),
            value: Some("gpu".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }];
        assert!(!pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_all_with_empty_key_exists() {
        // Empty key with Exists operator matches all taints
        let tolerations = vec![Toleration {
            key: None,
            operator: Some("Exists".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        }];
        let taints = vec![
            Taint {
                key: "key1".to_string(),
                value: Some("val1".to_string()),
                effect: "NoSchedule".to_string(),
                time_added: None,
            },
            Taint {
                key: "key2".to_string(),
                value: None,
                effect: "NoExecute".to_string(),
                time_added: None,
            },
        ];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_prefer_noschedule_always() {
        // PreferNoSchedule taints are not blocking
        let tolerations: Vec<Toleration> = vec![];
        let taints = vec![Taint {
            key: "preference".to_string(),
            value: None,
            effect: "PreferNoSchedule".to_string(),
            time_added: None,
        }];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_with_no_effect_matches_all() {
        // A toleration with no effect matches all effects for the same key
        let tolerations = vec![Toleration {
            key: Some("key1".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: None, // matches all effects
            toleration_seconds: None,
        }];
        let taints = vec![Taint {
            key: "key1".to_string(),
            value: None,
            effect: "NoExecute".to_string(),
            time_added: None,
        }];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_multiple_taints_partial() {
        // Pod tolerates one taint but not the other
        let tolerations = vec![Toleration {
            key: Some("key1".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];
        let taints = vec![
            Taint {
                key: "key1".to_string(),
                value: None,
                effect: "NoSchedule".to_string(),
                time_added: None,
            },
            Taint {
                key: "key2".to_string(),
                value: None,
                effect: "NoSchedule".to_string(),
                time_added: None,
            },
        ];
        assert!(!pod_tolerates_node_taints(&tolerations, &taints));
    }

    /// Helper to create a minimal DaemonSet for testing
    #[tokio::test]
    async fn test_ds_pods_inherit_sa_image_pull_secrets() {
        // #1084: SA imagePullSecrets must reach controller-created pods, which
        // bypass the api-server admission path.
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());

        let mut sa = rusternetes_common::resources::ServiceAccount::new("default", "default");
        sa.image_pull_secrets = Some(vec![
            rusternetes_common::resources::service_account::LocalObjectReference {
                name: "regcred".to_string(),
            },
        ]);
        storage
            .create("/registry/serviceaccounts/default/default", &sa)
            .await
            .unwrap();

        let node = make_test_node("test-node-1");
        storage
            .create("/registry/nodes/test-node-1", &node)
            .await
            .unwrap();

        let mut ds = make_test_daemonset("pullsecrets-ds", "default");
        storage
            .create("/registry/daemonsets/default/pullsecrets-ds", &ds)
            .await
            .unwrap();

        controller.reconcile(&mut ds).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 1, "DS should create one pod on the node");
        let secrets = pods[0]
            .spec
            .as_ref()
            .unwrap()
            .image_pull_secrets
            .as_ref()
            .expect("pod must inherit the SA's imagePullSecrets");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "regcred");
    }

    fn make_test_daemonset(name: &str, namespace: &str) -> DaemonSet {
        let mut match_labels = HashMap::new();
        match_labels.insert("app".to_string(), name.to_string());

        DaemonSet {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "DaemonSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: {
                let mut m = rusternetes_common::types::ObjectMeta::new(name.to_string())
                    .with_namespace(namespace.to_string());
                m.ensure_uid();
                m.generation = Some(1);
                m
            },
            spec: rusternetes_common::resources::DaemonSetSpec {
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: Some(match_labels.clone()),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(rusternetes_common::types::ObjectMeta {
                        name: "".to_string(),
                        namespace: None,
                        labels: Some(match_labels),
                        annotations: None,
                        uid: String::new(),
                        creation_timestamp: None,
                        deletion_timestamp: None,
                        resource_version: None,
                        deletion_grace_period_seconds: None,
                        finalizers: None,
                        owner_references: None,
                        generate_name: None,
                        generation: None,
                        managed_fields: None,
                    }),
                    spec: PodSpec {
                        init_containers: None,
                        containers: vec![rusternetes_common::resources::pod::Container {
                            name: "test".to_string(),
                            image: "busybox:latest".to_string(),
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
                        node_name: None,
                        node_selector: None,
                        restart_policy: None,
                        service_account_name: None,
                        service_account: None,
                        volumes: None,
                        affinity: None,
                        tolerations: None,
                        priority: None,
                        priority_class_name: None,
                        hostname: None,
                        subdomain: None,
                        host_network: None,
                        host_pid: None,
                        host_ipc: None,
                        automount_service_account_token: None,
                        ephemeral_containers: None,
                        overhead: None,
                        scheduler_name: None,
                        topology_spread_constraints: None,
                        resource_claims: None,
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
                    },
                },
                update_strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
            },
            status: None,
        }
    }

    /// Helper to create a minimal Node for testing
    fn make_test_node(name: &str) -> Node {
        Node {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = rusternetes_common::types::ObjectMeta::new(name.to_string());
                m.ensure_uid();
                m
            },
            spec: Some(rusternetes_common::resources::NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints: None,
            }),
            status: None,
        }
    }

    #[tokio::test]
    async fn test_reconcile_creates_controller_revision() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());

        // Create a node
        let node = make_test_node("test-node-1");
        storage
            .create("/registry/nodes/test-node-1", &node)
            .await
            .unwrap();

        // Create a DaemonSet
        let mut ds = make_test_daemonset("my-ds", "default");
        storage
            .create("/registry/daemonsets/default/my-ds", &ds)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile(&mut ds).await.unwrap();

        // Verify a ControllerRevision was created
        let cr_prefix = "/registry/controllerrevisions/default/";
        let revisions: Vec<ControllerRevision> = storage.list(cr_prefix).await.unwrap();
        assert!(
            !revisions.is_empty(),
            "ControllerRevision should be created"
        );

        let cr = &revisions[0];
        assert_eq!(cr.type_meta.kind, "ControllerRevision");
        assert_eq!(cr.type_meta.api_version, "apps/v1");
        assert_eq!(cr.revision, 1);
        assert!(
            !cr.metadata.uid.is_empty(),
            "ControllerRevision should have a UID"
        );
        assert!(
            cr.metadata.creation_timestamp.is_some(),
            "Should have creation timestamp"
        );

        // Verify owner reference
        let owner_refs = cr.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "DaemonSet");
        assert_eq!(owner_refs[0].name, "my-ds");
        assert_eq!(owner_refs[0].uid, ds.metadata.uid);
        assert_eq!(owner_refs[0].controller, Some(true));

        // Verify labels include controller-revision-hash
        let labels = cr.metadata.labels.as_ref().unwrap();
        assert!(labels.contains_key("controller-revision-hash"));

        // Verify ControllerRevision data format matches K8s getPatch().
        // K8s Match() does bytes.Equal(getPatch(ds), history.Data.Raw).
        // The data MUST have: {"spec":{"template":{...,"$patch":"replace"}}}
        // with alphabetically sorted keys.
        let data = cr
            .data
            .as_ref()
            .expect("ControllerRevision should have data");
        let data_obj = data.as_object().expect("data should be an object");
        assert!(data_obj.contains_key("spec"), "data should have 'spec'");
        let spec = data_obj.get("spec").unwrap().as_object().unwrap();
        assert!(spec.contains_key("template"), "spec should have 'template'");
        let template = spec.get("template").unwrap().as_object().unwrap();
        assert_eq!(
            template.get("$patch"),
            Some(&serde_json::json!("replace")),
            "template should have $patch: replace"
        );

        // Verify keys are alphabetically sorted (Match() does byte comparison)
        let data_json = serde_json::to_string(&data).unwrap();
        // Re-parse and re-serialize to verify sorting is stable
        let reparsed: serde_json::Value = serde_json::from_str(&data_json).unwrap();
        let reserialized = serde_json::to_string(&reparsed).unwrap();
        assert_eq!(
            data_json, reserialized,
            "ControllerRevision data JSON should be deterministic (sorted keys)"
        );

        // Verify Match() equivalent: build_patch_data should produce same bytes
        let fresh_patch = DaemonSetController::<MemoryStorage>::build_patch_data(&ds.spec.template)
            .expect("build_patch_data should succeed");
        let fresh_json = serde_json::to_string(&fresh_patch).unwrap();
        assert_eq!(
            data_json, fresh_json,
            "build_patch_data should produce identical bytes for same template (K8s Match)"
        );
    }

    #[tokio::test]
    async fn test_reconcile_deletes_terminal_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());

        // Create a node
        let node = make_test_node("test-node-2");
        storage
            .create("/registry/nodes/test-node-2", &node)
            .await
            .unwrap();

        // Create a DaemonSet
        let mut ds = make_test_daemonset("fail-ds", "default");
        storage
            .create("/registry/daemonsets/default/fail-ds", &ds)
            .await
            .unwrap();

        // Reconcile once to create pods
        controller.reconcile(&mut ds).await.unwrap();

        // Verify a pod was created
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let ds_pods: Vec<&Pod> = pods
            .iter()
            .filter(|p| {
                p.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|r| r.name == "fail-ds"))
            })
            .collect();
        assert_eq!(ds_pods.len(), 1, "Should have 1 DS pod");
        let pod_name = ds_pods[0].metadata.name.clone();

        // Mark the pod as Failed
        let pod_key = format!("/registry/pods/default/{}", pod_name);
        let mut failed_pod: Pod = storage.get(&pod_key).await.unwrap();
        if let Some(status) = failed_pod.status.as_mut() {
            status.phase = Some(Phase::Failed);
        }
        storage.update(&pod_key, &failed_pod).await.unwrap();

        // Re-read DaemonSet (status was updated)
        let mut ds: DaemonSet = storage
            .get("/registry/daemonsets/default/fail-ds")
            .await
            .unwrap();

        // Reconcile again — should delete the failed pod AND recreate in the same cycle.
        // K8s DS controller deletes failed pods and creates replacements immediately.
        // K8s ref: pkg/controller/daemon/daemon_controller.go — podsShouldBeOnNode
        controller.reconcile(&mut ds).await.unwrap();

        // The failed pod should be gone
        let result: rusternetes_common::Result<Pod> = storage.get(&pod_key).await;
        assert!(result.is_err(), "Failed pod should have been deleted");

        // A replacement pod should already exist (created in the same cycle)
        let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let ds_pods_after: Vec<&Pod> = pods_after
            .iter()
            .filter(|p| {
                p.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|r| r.name == "fail-ds"))
            })
            .collect();
        assert_eq!(
            ds_pods_after.len(),
            1,
            "A replacement DS pod should be created in the same cycle as deletion"
        );
    }

    #[tokio::test]
    async fn test_reconcile_sets_number_available() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());

        // Create a node
        let node = make_test_node("test-node-3");
        storage
            .create("/registry/nodes/test-node-3", &node)
            .await
            .unwrap();

        // Create a DaemonSet
        let mut ds = make_test_daemonset("avail-ds", "default");
        storage
            .create("/registry/daemonsets/default/avail-ds", &ds)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile(&mut ds).await.unwrap();

        // Read back the updated DS
        let updated_ds: DaemonSet = storage
            .get("/registry/daemonsets/default/avail-ds")
            .await
            .unwrap();
        let status = updated_ds.status.as_ref().unwrap();

        assert_eq!(status.desired_number_scheduled, 1);
        assert_eq!(status.current_number_scheduled, 1);
        // Pod is Pending, not Running, so number_ready should be 0
        assert_eq!(status.number_ready, 0);
        assert!(
            status.number_available.is_some(),
            "number_available should be set"
        );
        assert!(
            status.updated_number_scheduled.is_some(),
            "updated_number_scheduled should be set"
        );
        assert_eq!(status.updated_number_scheduled, Some(1));
    }

    #[test]
    fn test_resolve_max_unavailable_absolute() {
        // None defaults to 1
        assert_eq!(resolve_max_unavailable(None, 3), 1);
        // Absolute integer passes through
        assert_eq!(resolve_max_unavailable(Some("2"), 5), 2);
        // Absolute clamped to at least 1
        assert_eq!(resolve_max_unavailable(Some("0"), 5), 1);
        // Unparseable falls back to 1
        assert_eq!(resolve_max_unavailable(Some("notanumber"), 5), 1);
    }

    #[test]
    fn test_resolve_max_unavailable_percentage() {
        // 25% of 4 nodes = 1 (rounded up from 1.0)
        assert_eq!(resolve_max_unavailable(Some("25%"), 4), 1);
        // 50% of 4 nodes = 2
        assert_eq!(resolve_max_unavailable(Some("50%"), 4), 2);
        // 25% of 5 nodes = 2 (rounded up from 1.25)
        assert_eq!(resolve_max_unavailable(Some("25%"), 5), 2);
        // 100% of 3 nodes = 3
        assert_eq!(resolve_max_unavailable(Some("100%"), 3), 3);
        // Tiny percentage on tiny cluster still allows at least 1
        assert_eq!(resolve_max_unavailable(Some("1%"), 1), 1);
        // Regression guard: "25%" must NOT be parsed as 25 absolute on a
        // small cluster — that previously allowed all pods to be deleted.
        assert_eq!(resolve_max_unavailable(Some("25%"), 3), 1);
    }
}
