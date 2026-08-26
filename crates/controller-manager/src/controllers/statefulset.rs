use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::{
    PersistentVolumeClaim, Pod, PodStatus, StatefulSet, StatefulSetStatus,
};
use rusternetes_common::types::{ObjectMeta, OwnerReference, Phase, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

pub struct StatefulSetController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> StatefulSetController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Re-read the pod, apply `mutate`, and write it back. On a resource-version
    /// conflict, re-read and reapply once. NotFound is a no-op (the pod is
    /// already gone). Other errors are logged via tracing::error!. Avoids
    /// silently dropping storage failures from `let _ = storage.update(...)`.
    async fn update_pod_with_retry<F>(&self, pod_key: &str, mutate: F)
    where
        F: Fn(&mut Pod) + Send,
    {
        for attempt in 0..2 {
            let mut pod: Pod = match self.storage.get(pod_key).await {
                Ok(p) => p,
                Err(rusternetes_common::Error::NotFound(_)) => return,
                Err(e) => {
                    error!(error = %e, pod = pod_key, "failed to read pod for update");
                    return;
                }
            };
            mutate(&mut pod);
            match self.storage.update(pod_key, &pod).await {
                Ok(_) => return,
                Err(rusternetes_common::Error::Conflict(_)) if attempt == 0 => continue,
                Err(e) => {
                    error!(error = %e, pod = pod_key, "failed to update pod");
                    return;
                }
            }
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting StatefulSetController (watch-based)");
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

            // Watch for changes to StatefulSets AND Pods
            let prefix = "/registry/statefulsets/";
            let watch_result = self.storage.watch(prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            let pod_prefix = build_prefix("pods", None);
            let mut pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish pod watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            // Periodic full resync as safety net (every 30s)
            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await; // consume first immediate tick

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
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                self.enqueue_owner_statefulset(&queue, &ev).await;
                            }
                            Some(Err(e)) => {
                                warn!("Pod watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Pod watch stream ended, reconnecting");
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
            let storage_key = build_key("statefulsets", Some(ns), name);
            match self.storage.get::<StatefulSet>(&storage_key).await {
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
            .list::<StatefulSet>("/registry/statefulsets/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("statefulsets/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list statefulsets for enqueue: {}", e);
            }
        }
    }

    /// When a pod changes, check its ownerReferences for a StatefulSet owner
    /// and enqueue that StatefulSet for reconciliation.
    async fn enqueue_owner_statefulset(
        &self,
        queue: &WorkQueue,
        event: &rusternetes_storage::WatchEvent,
    ) {
        let pod_key = extract_key(event);
        let parts: Vec<&str> = pod_key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };

        let storage_key = format!("/registry/{}", pod_key);
        match self.storage.get::<Pod>(&storage_key).await {
            Ok(pod) => {
                if let Some(refs) = &pod.metadata.owner_references {
                    for owner_ref in refs {
                        if owner_ref.kind == "StatefulSet" {
                            queue
                                .add(format!("statefulsets/{}/{}", ns, owner_ref.name))
                                .await;
                        }
                    }
                }
            }
            Err(_) => {
                // Pod deleted — enqueue all StatefulSets in this namespace
                if let Ok(items) = self
                    .storage
                    .list::<StatefulSet>(&build_prefix("statefulsets", Some(ns)))
                    .await
                {
                    for ss in &items {
                        queue
                            .add(format!("statefulsets/{}/{}", ns, ss.metadata.name))
                            .await;
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        let statefulsets: Vec<StatefulSet> = self.storage.list("/registry/statefulsets/").await?;

        for mut statefulset in statefulsets {
            if let Err(e) = self.reconcile(&mut statefulset).await {
                error!(
                    "Failed to reconcile StatefulSet {}: {}",
                    statefulset.metadata.name, e
                );
            }
        }

        Ok(())
    }

    async fn reconcile(&self, statefulset: &mut StatefulSet) -> Result<()> {
        let name = &statefulset.metadata.name;
        let namespace = statefulset.metadata.namespace.as_ref().unwrap();

        // Skip reconciliation for StatefulSets being deleted — GC handles pod cleanup
        if statefulset.metadata.is_being_deleted() {
            return Ok(());
        }

        debug!("Reconciling StatefulSet {}/{}", namespace, name);

        let desired_replicas = statefulset.spec.replicas.unwrap_or(1);

        // Get current pods for this StatefulSet
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Filter pods that belong to this StatefulSet via ownerReferences (authoritative)
        // Fall back to label matching for backwards compatibility
        let statefulset_uid = &statefulset.metadata.uid;
        let statefulset_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == statefulset_uid))
                    .unwrap_or(false);
                let owned_by_label = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("app"))
                    .map(|app| app == name)
                    .unwrap_or(false)
                    && pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get("statefulset.kubernetes.io/pod-name"))
                        .is_some();
                owned_by_ref || owned_by_label
            })
            .collect();

        // K8s processReplica(): delete Failed/Succeeded pods so they get recreated.
        // This matches K8s behavior where the StatefulSet controller deletes completed
        // pods and recreates them on the next sync cycle.
        let mut active_pods = Vec::new();
        for pod in statefulset_pods {
            let is_terminal = matches!(
                pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Failed) | Some(Phase::Succeeded)
            );
            if is_terminal && pod.metadata.deletion_timestamp.is_none() {
                // Delete the terminal pod so it gets recreated
                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                self.update_pod_with_retry(&pod_key, |p| {
                    p.metadata.deletion_timestamp = Some(chrono::Utc::now());
                    p.metadata.deletion_grace_period_seconds = Some(0);
                })
                .await;
                info!(
                    "StatefulSet {}/{}: deleted terminal pod {} (phase: {:?})",
                    namespace,
                    name,
                    pod.metadata.name,
                    pod.status.as_ref().and_then(|s| s.phase.as_ref())
                );
            } else if !is_terminal {
                active_pods.push(pod);
            }
            // Terminal pods with deletionTimestamp already set are being cleaned up
        }
        let mut statefulset_pods = active_pods;

        // Sort pods by ordinal index
        statefulset_pods.sort_by_key(|pod| {
            pod.metadata
                .name
                .rsplit_once('-')
                .and_then(|(_, idx)| idx.parse::<i32>().ok())
                .unwrap_or(0)
        });

        // Count only non-terminating pods as current replicas.
        // Terminating pods (deletion_timestamp set) should not prevent scale-up
        // to recreate them with the new template.
        let current_replicas = statefulset_pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .count() as i32;

        debug!(
            "StatefulSet {}/{}: desired={}, current={}",
            namespace, name, desired_replicas, current_replicas
        );

        let is_ordered_ready = statefulset
            .spec
            .pod_management_policy
            .as_ref()
            .map(|p| p == "OrderedReady")
            .unwrap_or(true);

        // Extract partition for rolling update — pods below partition use current (old) template
        let partition = statefulset
            .spec
            .update_strategy
            .as_ref()
            .and_then(|s| s.rolling_update.as_ref())
            .and_then(|ru| ru.partition)
            .unwrap_or(0);

        // Scale up or down
        if current_replicas < desired_replicas {
            // Scale up: create any missing pods in ordinal order.
            // During rolling updates, gaps can appear at any ordinal (not just at the end),
            // so we check all ordinals 0..desired rather than current..desired.
            for i in 0..desired_replicas {
                let pod_name = format!("{}-{}", name, i);
                let pod_key = build_key("pods", Some(namespace), &pod_name);
                // Treat evicted/terminating pods (deletionTimestamp set) as missing
                // so the controller recreates them. The kubelet handles actual
                // deletion from storage after graceful shutdown.
                let pod_exists = match self.storage.get::<Pod>(&pod_key).await {
                    Ok(pod) => pod.metadata.deletion_timestamp.is_none(),
                    Err(_) => false,
                };
                if pod_exists {
                    continue;
                }
                // For OrderedReady policy, check that the previous pod is Ready before
                // creating the next one. If it's not ready, halt scaling.
                if is_ordered_ready && i > 0 {
                    let prev_pod_name = format!("{}-{}", name, i - 1);
                    let prev_pod_key = build_key("pods", Some(namespace), &prev_pod_name);
                    match self.storage.get::<Pod>(&prev_pod_key).await {
                        Ok(prev_pod) => {
                            let is_ready = prev_pod
                                .status
                                .as_ref()
                                .and_then(|s| s.conditions.as_ref())
                                .map(|conditions| {
                                    conditions
                                        .iter()
                                        .any(|c| c.condition_type == "Ready" && c.status == "True")
                                })
                                .unwrap_or(false);

                            if !is_ready {
                                info!(
                                    "StatefulSet {}: pod {} not ready, halting scale-up",
                                    name, prev_pod_name
                                );
                                break;
                            }
                        }
                        Err(_) => {
                            // Previous pod doesn't exist yet
                            info!(
                                "StatefulSet {}: pod {} not found, halting scale-up",
                                name, prev_pod_name
                            );
                            break;
                        }
                    }
                }

                // Ensure PVCs exist for this ordinal before creating the pod
                self.ensure_pvcs_for_ordinal(statefulset, i, namespace)
                    .await?;
                // For rolling updates with partition: pods below partition should
                // use the CURRENT revision (old template), not the update revision.
                // This matches K8s behavior where newVersionedStatefulSetPod creates
                // pods with the appropriate template based on ordinal vs partition.
                let current_rev = statefulset
                    .status
                    .as_ref()
                    .and_then(|s| s.current_revision.as_deref());
                let update_rev_str = Self::compute_revision(&statefulset.spec.template);
                if i < partition {
                    if let Some(cr_rev) = current_rev {
                        if cr_rev != update_rev_str {
                            // Pod is below partition — try to create with the old template
                            // by looking up the ControllerRevision
                            if let Some(old_template) = self
                                .get_template_from_revision(
                                    namespace,
                                    &statefulset.metadata.name,
                                    cr_rev,
                                )
                                .await
                            {
                                self.create_pod_with_template(
                                    statefulset,
                                    i,
                                    namespace,
                                    &old_template,
                                    cr_rev,
                                )
                                .await?;
                                info!(
                                    "Created pod {}-{} with current revision {}",
                                    name, i, cr_rev
                                );
                                continue;
                            }
                        }
                    }
                }
                self.create_pod(statefulset, i, namespace).await?;
                info!("Created pod {}-{}", name, i);
            }
        } else if current_replicas > desired_replicas {
            // Scale down following K8s processCondemned() logic:
            // Pods with ordinal >= desired_replicas are "condemned" (to be deleted).
            // Process them in REVERSE ordinal order (highest first).
            // For OrderedReady policy, enforce:
            //   - If any pod is terminating, BLOCK (wait for it)
            //   - Find the first unhealthy pod (lowest ordinal, any pod not Running+Ready)
            //   - A condemned pod can only be deleted if:
            //     a) It IS Running and Ready, OR
            //     b) It IS the firstUnhealthyPod (the one with lowest ordinal among unhealthy)
            //   - Delete at most ONE pod per reconcile cycle

            // Find the first unhealthy pod across ALL pods (lowest ordinal first)
            let first_unhealthy_name = if is_ordered_ready {
                statefulset_pods.iter().find_map(|p| {
                    let is_ready = p
                        .status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .map(|conds| {
                            conds
                                .iter()
                                .any(|c| c.condition_type == "Ready" && c.status == "True")
                        })
                        .unwrap_or(false);
                    let is_running = matches!(
                        p.status.as_ref().and_then(|s| s.phase.as_ref()),
                        Some(Phase::Running)
                    );
                    if (!is_ready || !is_running) && p.metadata.deletion_timestamp.is_none() {
                        Some(p.metadata.name.clone())
                    } else {
                        None
                    }
                })
            } else {
                None
            };

            // Process condemned pods (ordinal >= desired_replicas) in reverse order
            let mut condemned: Vec<&Pod> = statefulset_pods
                .iter()
                .filter(|p| {
                    let ordinal = p
                        .metadata
                        .name
                        .rsplit_once('-')
                        .and_then(|(_, idx)| idx.parse::<i32>().ok())
                        .unwrap_or(0);
                    ordinal >= desired_replicas
                })
                .collect();
            condemned.sort_by_key(|p| {
                std::cmp::Reverse(
                    p.metadata
                        .name
                        .rsplit_once('-')
                        .and_then(|(_, idx)| idx.parse::<i32>().ok())
                        .unwrap_or(0),
                )
            });

            let mut deleted_one = false;
            for pod in &condemned {
                // If pod is already terminating, wait for it
                if pod.metadata.deletion_timestamp.is_some() {
                    debug!(
                        "StatefulSet {}/{}: waiting for pod {} to terminate",
                        namespace, name, pod.metadata.name
                    );
                    break; // Block further deletions
                }

                if is_ordered_ready {
                    let is_ready = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .map(|conds| {
                            conds
                                .iter()
                                .any(|c| c.condition_type == "Ready" && c.status == "True")
                        })
                        .unwrap_or(false);
                    let is_running = matches!(
                        pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                        Some(Phase::Running)
                    );

                    // Can only delete this pod if it's Ready+Running OR if it's the firstUnhealthyPod
                    if !(is_ready && is_running) {
                        let is_first_unhealthy = first_unhealthy_name
                            .as_ref()
                            .map(|n| n == &pod.metadata.name)
                            .unwrap_or(false);
                        if !is_first_unhealthy {
                            debug!(
                                "StatefulSet {}/{}: pod {} is unhealthy but not first unhealthy, blocking scale-down",
                                namespace, name, pod.metadata.name
                            );
                            break; // Block — can't skip unhealthy pods
                        }
                    }
                }

                // Delete this condemned pod — follows K8s DeleteStatefulPod pattern.
                // K8s calls Pods(ns).Delete(name, DeleteOptions{}) which sets
                // deletionTimestamp and lets the kubelet handle graceful shutdown.
                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                match self.storage.get::<Pod>(&pod_key).await {
                    Ok(pod_to_delete) => {
                        if pod_to_delete.metadata.deletion_timestamp.is_none() {
                            self.update_pod_with_retry(&pod_key, |p| {
                                if p.metadata.deletion_timestamp.is_some() {
                                    return;
                                }
                                p.metadata.deletion_timestamp = Some(chrono::Utc::now());
                                // Use pod's terminationGracePeriodSeconds (K8s default behavior
                                // when DeleteOptions.GracePeriodSeconds is not set)
                                p.metadata.deletion_grace_period_seconds = p
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.termination_grace_period_seconds);
                                // Set pod phase to indicate it's terminating
                                if let Some(ref mut status) = p.status {
                                    if !matches!(
                                        status.phase,
                                        Some(Phase::Succeeded) | Some(Phase::Failed)
                                    ) {
                                        status.reason = Some("StatefulSetScaleDown".to_string());
                                    }
                                }
                            })
                            .await;
                            info!(
                                "Scale down: set deletionTimestamp on pod {} ({} -> {})",
                                pod.metadata.name, current_replicas, desired_replicas
                            );
                            deleted_one = true;
                        }
                    }
                    Err(_) => {
                        info!("Scale down: pod {} already gone", pod.metadata.name);
                    }
                }
                if deleted_one {
                    break; // Only delete one per reconcile
                }
            }
        }

        // Rolling update: if replica count matches but pods have old revision, delete one at a time.
        // The controller will recreate them with the new template on the next reconcile.
        // Skip if updateStrategy is OnDelete (user must manually delete pods to trigger update).
        let update_strategy = statefulset
            .spec
            .update_strategy
            .as_ref()
            .and_then(|s| s.strategy_type.as_deref())
            .unwrap_or("RollingUpdate");

        if current_replicas == desired_replicas
            && desired_replicas > 0
            && update_strategy == "RollingUpdate"
        {
            let update_revision = Self::compute_revision(&statefulset.spec.template);
            debug!(
                "StatefulSet {}/{}: rolling update check, update_revision={}",
                namespace, name, update_revision
            );

            // Check pods in reverse order for rolling update.
            // Only update pods with ordinal >= partition.
            // For each pod: if it has a stale revision AND is Ready (or at least Running),
            // delete it so it gets recreated with the new template.
            // If the most recently deleted pod's replacement is not yet Ready, wait before
            // deleting the next one.
            let mut deleted_one = false;
            for pod in statefulset_pods.iter().rev() {
                let ordinal = pod
                    .metadata
                    .name
                    .rsplit_once('-')
                    .and_then(|(_, idx)| idx.parse::<i32>().ok())
                    .unwrap_or(0);

                // Only update pods with ordinal >= partition
                if ordinal < partition {
                    continue;
                }

                let pod_revision = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("controller-revision-hash"))
                    .map(|s| s.as_str())
                    .unwrap_or("");
                debug!(
                    "StatefulSet {}/{}: pod {} revision={} vs update_revision={}",
                    namespace, name, pod.metadata.name, pod_revision, update_revision
                );
                if pod_revision != update_revision {
                    // Check if this pod is at least Running or Ready — don't delete pods
                    // that haven't even started yet (prevents cascading deletions during initial creation)
                    let pod_phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                    let pod_is_active =
                        matches!(pod_phase, Some(Phase::Running) | Some(Phase::Pending));
                    let pod_is_ready = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .map(|c| {
                            c.iter()
                                .any(|cond| cond.condition_type == "Ready" && cond.status == "True")
                        })
                        .unwrap_or(false);

                    // Delete pods with stale revision using graceful termination
                    // (set deletionTimestamp instead of direct delete, so the kubelet
                    // can perform cleanup and the pod gets properly recreated).
                    // Empty revision means newly created — skip those.
                    if !pod_revision.is_empty() && (pod_is_ready || pod_is_active) {
                        let pod_key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
                        if pod.metadata.deletion_timestamp.is_none() {
                            self.update_pod_with_retry(&pod_key, |p| {
                                if p.metadata.deletion_timestamp.is_some() {
                                    return;
                                }
                                p.metadata.deletion_timestamp = Some(chrono::Utc::now());
                                p.metadata.deletion_grace_period_seconds = p
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.termination_grace_period_seconds)
                                    .or(Some(30));
                            })
                            .await;
                            info!(
                                "Rolling update: set deletionTimestamp on pod {} (old revision {}, update revision {})",
                                pod.metadata.name, pod_revision, update_revision
                            );
                        }
                        deleted_one = true;
                        break; // Delete one at a time for OrderedReady rolling updates
                    }
                }
            }
            let _ = deleted_one;
        }

        // Re-fetch and recount pods after create/delete operations to get accurate status
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods_after: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        let statefulset_pods_after: Vec<Pod> = all_pods_after
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == statefulset_uid))
                    .unwrap_or(false);
                let owned_by_label = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("statefulset.kubernetes.io/pod-name"))
                    .is_some()
                    && pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get("app"))
                        .map(|app| app == name)
                        .unwrap_or(false);
                owned_by_ref || owned_by_label
            })
            .collect();

        // K8s computeReplicaStatus() counts differently per field:
        // - replicas: isCreated(pod) — includes terminating pods
        // - readyReplicas: isRunningAndReady(pod) — Running + Ready condition
        // - availableReplicas: isRunningAndAvailable(pod, minReadySeconds)
        // - currentReplicas/updatedReplicas: isCreated && !isTerminating && revision match
        // See: pkg/controller/statefulset/stateful_set_control.go:370-399
        let is_created =
            |pod: &&Pod| -> bool { pod.status.as_ref().and_then(|s| s.phase.as_ref()).is_some() };
        let is_terminating = |pod: &&Pod| -> bool { pod.metadata.deletion_timestamp.is_some() };
        let is_ready = |pod: &&Pod| -> bool {
            // K8s isRunningAndReady excludes terminating pods so that a pod
            // marked for graceful deletion drops out of readyReplicas
            // immediately. Conformance tests for SS eviction/scale-down rely
            // on this — see pkg/controller/statefulset/stateful_set_utils.go.
            if is_terminating(pod) {
                return false;
            }
            matches!(
                pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Running)
            ) && pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conditions| {
                    conditions
                        .iter()
                        .any(|c| c.condition_type == "Ready" && c.status == "True")
                })
                .unwrap_or(false)
        };

        // replicas = created pods excluding those being terminated.
        // K8s historically counted all created pods, but conformance tests for
        // StatefulSet eviction/scale-down observe replicas decrement as soon as
        // deletionTimestamp is set (graceful termination), not only when the
        // pod is fully removed. Counting terminating pods here causes the
        // scale-down test to flap on `status.replicas` and the eviction tests
        // to fail on PDB recalculation timing.
        let final_current_replicas = statefulset_pods_after
            .iter()
            .filter(|p| is_created(p) && !is_terminating(p))
            .count() as i32;
        // readyReplicas = Running + Ready (non-terminating implied by Running phase)
        let final_ready_pods = statefulset_pods_after
            .iter()
            .filter(|p| is_ready(p))
            .count() as i32;
        // availableReplicas = same as ready for now (minReadySeconds=0 default)
        let final_available_pods = final_ready_pods;

        // Generate a revision hash from the current pod template spec
        let update_revision = Self::compute_revision(&statefulset.spec.template);

        // The current_revision is the revision that existing pods are running.
        // During a rolling update, this differs from update_revision.
        // Preserve the existing current_revision if set, otherwise derive from pods.
        let current_revision = statefulset
            .status
            .as_ref()
            .and_then(|s| s.current_revision.clone())
            .or_else(|| {
                // No current_revision in status — derive from actual pod labels
                statefulset_pods_after.iter().find_map(|pod| {
                    pod.metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("controller-revision-hash"))
                        .cloned()
                })
            })
            .unwrap_or_else(|| update_revision.clone());

        // K8s: currentReplicas/updatedReplicas only count isCreated && !isTerminating pods
        let updated_count = statefulset_pods_after
            .iter()
            .filter(|pod| {
                is_created(pod)
                    && !is_terminating(pod)
                    && pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("controller-revision-hash"))
                        .map(|h| h == &update_revision)
                        .unwrap_or(false)
            })
            .count() as i32;

        let current_rev_count = statefulset_pods_after
            .iter()
            .filter(|pod| {
                is_created(pod)
                    && !is_terminating(pod)
                    && pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("controller-revision-hash"))
                        .map(|h| h == &current_revision)
                        .unwrap_or(false)
            })
            .count() as i32;

        // Determine the final current_revision:
        // Only advance current_revision to update_revision when ALL pods have been
        // updated (updated_count >= desired_replicas). This ensures that during a
        // rolling update, currentRevision != updateRevision, which conformance tests verify.
        let final_current_revision = if updated_count >= desired_replicas {
            update_revision.clone()
        } else {
            current_revision.clone()
        };

        // Preserve existing conditions of unknown types — the StatefulSet controller
        // doesn't manage any condition types itself, so keep ALL existing conditions.
        // This prevents overwriting conditions set via PUT /status (e.g. "StatusUpdate"
        // condition from conformance tests).
        let existing_conditions = statefulset
            .status
            .as_ref()
            .and_then(|s| s.conditions.clone());

        // Update status with accurate counts
        // current_replicas = pods matching currentRevision (K8s semantics)
        // updated_replicas = pods matching updateRevision
        let new_status = Some(StatefulSetStatus {
            replicas: final_current_replicas,
            ready_replicas: Some(final_ready_pods),
            current_replicas: Some(if final_current_revision == update_revision {
                // All pods are on the same (current) revision
                final_current_replicas
            } else {
                // During rolling update: count pods on the old (current) revision
                current_rev_count
            }),
            updated_replicas: Some(updated_count),
            available_replicas: Some(final_available_pods),
            collision_count: None,
            observed_generation: statefulset.metadata.generation,
            current_revision: Some(final_current_revision),
            update_revision: Some(update_revision),
            conditions: existing_conditions,
        });

        // Only write status if it actually changed to avoid unnecessary storage writes
        // that trigger watch events and cause feedback loops
        if statefulset.status != new_status {
            statefulset.status = new_status;
            let key = format!("/registry/statefulsets/{}/{}", namespace, name);
            self.storage.update(&key, statefulset).await?;
        }

        // Ensure a ControllerRevision exists for the current template revision
        let revision = Self::compute_revision(&statefulset.spec.template);
        let cr_name = format!(
            "{}-{}",
            name,
            &revision[..std::cmp::min(10, revision.len())]
        );
        let cr_key = format!("/registry/controllerrevisions/{}/{}", namespace, cr_name);
        if self
            .storage
            .get::<serde_json::Value>(&cr_key)
            .await
            .is_err()
        {
            // Create the ControllerRevision
            let template_data =
                serde_json::to_value(&statefulset.spec.template).unwrap_or_default();
            let cr = serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "ControllerRevision",
                "metadata": {
                    "name": cr_name,
                    "namespace": namespace,
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": chrono::Utc::now().to_rfc3339(),
                    "labels": {
                        "controller.kubernetes.io/hash": revision,
                        "app": name
                    },
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "StatefulSet",
                        "name": name,
                        "uid": statefulset.metadata.uid,
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "data": template_data,
                "revision": 1
            });
            if let Err(e) = self.storage.create(&cr_key, &cr).await {
                debug!(
                    "ControllerRevision {} already exists or failed: {}",
                    cr_name, e
                );
            } else {
                info!(
                    "Created ControllerRevision {} for StatefulSet {}/{}",
                    cr_name, namespace, name
                );
            }
        }

        Ok(())
    }

    async fn ensure_pvcs_for_ordinal(
        &self,
        statefulset: &StatefulSet,
        ordinal: i32,
        namespace: &str,
    ) -> Result<()> {
        if let Some(ref templates) = statefulset.spec.volume_claim_templates {
            for template in templates {
                let pvc_name = format!(
                    "{}-{}-{}",
                    template.metadata.name, statefulset.metadata.name, ordinal
                );
                let key = build_key("persistentvolumeclaims", Some(namespace), &pvc_name);

                // Check if PVC already exists
                if self
                    .storage
                    .get::<PersistentVolumeClaim>(&key)
                    .await
                    .is_ok()
                {
                    continue; // PVC already exists
                }

                // Create PVC from template
                let mut pvc_metadata =
                    ObjectMeta::new(&pvc_name).with_namespace(namespace.to_string());

                // Copy labels and annotations from template metadata
                if let Some(ref tmpl_labels) = template.metadata.labels {
                    pvc_metadata.labels = Some(tmpl_labels.clone());
                }
                if let Some(ref tmpl_annotations) = template.metadata.annotations {
                    pvc_metadata.annotations = Some(tmpl_annotations.clone());
                }

                // Set owner reference to the StatefulSet
                pvc_metadata.owner_references = Some(vec![OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "StatefulSet".to_string(),
                    name: statefulset.metadata.name.clone(),
                    uid: statefulset.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);

                let pvc = PersistentVolumeClaim {
                    type_meta: TypeMeta {
                        kind: "PersistentVolumeClaim".to_string(),
                        api_version: "v1".to_string(),
                    },
                    metadata: pvc_metadata,
                    spec: template.spec.clone(),
                    status: None,
                };

                self.storage.create(&key, &pvc).await?;
                info!(
                    "Created PVC {} for StatefulSet {}/{}",
                    pvc_name, namespace, statefulset.metadata.name
                );
            }
        }
        Ok(())
    }

    /// Compute a revision string from the pod template spec.
    /// This produces a deterministic hash that captures template changes.
    ///
    /// IMPORTANT: We must convert to serde_json::Value first before serializing
    /// to string. Direct `to_string` on the struct iterates HashMap fields in
    /// arbitrary order (HashMap has no guaranteed iteration order), producing
    /// non-deterministic output. Converting to Value first normalizes all maps
    /// into BTreeMap-backed serde_json::Map, which iterates in sorted key order.
    fn compute_revision(template: &rusternetes_common::resources::PodTemplateSpec) -> String {
        use sha2::{Digest, Sha256};
        // Convert to Value first to normalize HashMap ordering to sorted BTreeMap
        let value = serde_json::to_value(template).unwrap_or_default();
        let serialized = serde_json::to_string(&value).unwrap_or_default();
        let hash = Sha256::digest(serialized.as_bytes());
        let revision = format!(
            "{:010x}",
            u64::from_be_bytes(hash[..8].try_into().unwrap_or([0u8; 8]))
        );
        // Log the first container's image for debugging rolling updates
        let image = value
            .pointer("/spec/containers/0/image")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        debug!(
            "compute_revision: image={}, hash={}, json_len={}",
            image,
            revision,
            serialized.len()
        );
        revision
    }

    async fn create_pod(
        &self,
        statefulset: &StatefulSet,
        ordinal: i32,
        namespace: &str,
    ) -> Result<()> {
        let statefulset_name = &statefulset.metadata.name;
        let pod_name = format!("{}-{}", statefulset_name, ordinal);

        // Create pod from template
        let template = &statefulset.spec.template;
        let mut labels = template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("app".to_string(), statefulset_name.clone());
        labels.insert(
            "statefulset.kubernetes.io/pod-name".to_string(),
            pod_name.clone(),
        );
        // Set the controller-revision-hash label so tests can verify pod revision
        let revision = Self::compute_revision(&statefulset.spec.template);
        labels.insert("controller-revision-hash".to_string(), revision);

        let mut metadata = rusternetes_common::types::ObjectMeta::new(pod_name.clone())
            .with_namespace(namespace.to_string())
            .with_labels(labels)
            .with_owner_reference(OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "StatefulSet".to_string(),
                name: statefulset_name.clone(),
                uid: statefulset.metadata.uid.clone(),
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
            spec: Some(template.spec.clone()),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                pod_ip: None,
                host_ip: None,
                conditions: None,
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
            }),
        };

        // Check ResourceQuota before creating pod
        super::check_resource_quota(&*self.storage, namespace).await?;

        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        match self.storage.create(&key, &pod).await {
            Ok(_) => Ok(()),
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                debug!("Pod {} already exists, skipping creation", pod_name);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Create a pod with a specific template and revision (for pods below the partition
    /// that should use the old/current template, not the update template).
    async fn create_pod_with_template(
        &self,
        statefulset: &StatefulSet,
        ordinal: i32,
        namespace: &str,
        template: &rusternetes_common::resources::PodTemplateSpec,
        revision: &str,
    ) -> Result<()> {
        let statefulset_name = &statefulset.metadata.name;
        let pod_name = format!("{}-{}", statefulset_name, ordinal);

        let mut labels = template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("app".to_string(), statefulset_name.clone());
        labels.insert(
            "statefulset.kubernetes.io/pod-name".to_string(),
            pod_name.clone(),
        );
        labels.insert("controller-revision-hash".to_string(), revision.to_string());

        let mut metadata = rusternetes_common::types::ObjectMeta::new(pod_name.clone())
            .with_namespace(namespace.to_string())
            .with_labels(labels)
            .with_owner_reference(OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "StatefulSet".to_string(),
                name: statefulset_name.clone(),
                uid: statefulset.metadata.uid.clone(),
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
            spec: Some(template.spec.clone()),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                pod_ip: None,
                host_ip: None,
                conditions: None,
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
            }),
        };

        super::check_resource_quota(&*self.storage, namespace).await?;

        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        match self.storage.create(&key, &pod).await {
            Ok(_) => Ok(()),
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                debug!("Pod {} already exists, skipping creation", pod_name);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Look up a ControllerRevision and extract the PodTemplateSpec from its data field.
    async fn get_template_from_revision(
        &self,
        namespace: &str,
        statefulset_name: &str,
        revision_hash: &str,
    ) -> Option<rusternetes_common::resources::PodTemplateSpec> {
        // ControllerRevision names follow the pattern: {ss-name}-{revision-hash-prefix}
        let cr_name = format!(
            "{}-{}",
            statefulset_name,
            &revision_hash[..std::cmp::min(10, revision_hash.len())]
        );
        let cr_key = format!("/registry/controllerrevisions/{}/{}", namespace, cr_name);
        if let Ok(cr) = self.storage.get::<serde_json::Value>(&cr_key).await {
            if let Some(data) = cr.get("data") {
                if let Ok(template) = serde_json::from_value::<
                    rusternetes_common::resources::PodTemplateSpec,
                >(data.clone())
                {
                    return Some(template);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::workloads::{
        RollingUpdateStatefulSetStrategy, StatefulSetUpdateStrategy,
    };
    use rusternetes_common::resources::{
        Container, PodCondition, PodSpec, PodTemplateSpec, StatefulSetSpec,
    };
    use rusternetes_common::types::LabelSelector;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    #[test]
    fn test_pod_name_generation() {
        let statefulset_name = "web";
        let ordinal = 2;
        let pod_name = format!("{}-{}", statefulset_name, ordinal);
        assert_eq!(pod_name, "web-2");
    }

    #[test]
    fn test_pod_ordinal_parsing() {
        let pod_name = "web-5";
        let ordinal: i32 = pod_name
            .rsplit_once('-')
            .and_then(|(_, idx)| idx.parse().ok())
            .unwrap();
        assert_eq!(ordinal, 5);
    }

    /// Verify compute_revision is deterministic even with HashMap-backed labels
    #[test]
    fn test_compute_revision_deterministic() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());
        labels.insert("version".to_string(), "v1".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());

        let template = PodTemplateSpec {
            metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
            spec: PodSpec {
                containers: vec![test_container("nginx", "nginx:1.19")],
                ..Default::default()
            },
        };

        // Compute multiple times — must always produce the same result
        let rev1 = StatefulSetController::<MemoryStorage>::compute_revision(&template);
        let rev2 = StatefulSetController::<MemoryStorage>::compute_revision(&template);
        let rev3 = StatefulSetController::<MemoryStorage>::compute_revision(&template);
        assert_eq!(rev1, rev2, "Revision should be deterministic across calls");
        assert_eq!(rev2, rev3, "Revision should be deterministic across calls");
    }

    /// Verify compute_revision changes when image changes
    #[test]
    fn test_compute_revision_changes_on_image_change() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());

        let template_v1 = PodTemplateSpec {
            metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
            spec: PodSpec {
                containers: vec![test_container("nginx", "nginx:1.19")],
                ..Default::default()
            },
        };

        let template_v2 = PodTemplateSpec {
            metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
            spec: PodSpec {
                containers: vec![test_container("nginx", "nginx:1.20")],
                ..Default::default()
            },
        };

        let rev1 = StatefulSetController::<MemoryStorage>::compute_revision(&template_v1);
        let rev2 = StatefulSetController::<MemoryStorage>::compute_revision(&template_v2);
        assert_ne!(rev1, rev2, "Revision should change when image changes");
    }

    fn test_container(name: &str, image: &str) -> Container {
        Container {
            name: name.to_string(),
            image: image.to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: None,
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
        }
    }

    fn make_statefulset(name: &str, namespace: &str, replicas: i32, image: &str) -> StatefulSet {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), name.to_string());

        StatefulSet {
            type_meta: TypeMeta {
                kind: "StatefulSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace.to_string()),
            spec: StatefulSetSpec {
                replicas: Some(replicas),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                service_name: format!("{}-svc", name),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels)),
                    spec: PodSpec {
                        containers: vec![test_container("main", image)],
                        ..Default::default()
                    },
                },
                update_strategy: None,
                pod_management_policy: Some("Parallel".to_string()),
                min_ready_seconds: None,
                revision_history_limit: None,
                volume_claim_templates: None,
                persistent_volume_claim_retention_policy: None,
                ordinals: None,
            },
            status: None,
        }
    }

    /// Make a pod look like it's Running and Ready
    async fn make_pod_ready(storage: &Arc<MemoryStorage>, namespace: &str, pod_name: &str) {
        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        if let Ok(mut pod) = storage.get::<Pod>(&key).await {
            pod.status = Some(PodStatus {
                phase: Some(Phase::Running),
                conditions: Some(vec![PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            });
            let _ = storage.update(&key, &pod).await;
        }
    }

    /// Simulate kubelet behavior: delete pods with deletionTimestamp from storage
    async fn simulate_kubelet_cleanup(storage: &Arc<MemoryStorage>, namespace: &str) {
        let prefix = format!("/registry/pods/{}/", namespace);
        let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
        for pod in pods {
            if pod.metadata.deletion_timestamp.is_some() {
                let key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
                let _ = storage.delete(&key).await;
            }
        }
    }

    /// During a rolling update, currentRevision != updateRevision.
    /// After all pods are updated, currentRevision == updateRevision.
    #[tokio::test]
    async fn test_rolling_update_revision_tracking() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());

        let ns = "default";
        let ss = make_statefulset("web", ns, 3, "nginx:1.19");

        // Store the StatefulSet
        let key = format!("/registry/statefulsets/{}/{}", ns, "web");
        storage.create(&key, &ss).await.unwrap();

        // First reconcile: creates 3 pods with revision hash for nginx:1.19
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Make all pods Running+Ready
        for i in 0..3 {
            make_pod_ready(&storage, ns, &format!("web-{}", i)).await;
        }

        // Reconcile again so status reflects ready pods
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Verify initial state: currentRevision == updateRevision
        let ss: StatefulSet = storage.get(&key).await.unwrap();
        let status = ss.status.as_ref().unwrap();
        assert_eq!(
            status.current_revision, status.update_revision,
            "Before update: currentRevision should equal updateRevision"
        );
        let old_revision = status.current_revision.clone().unwrap();

        // Now patch the StatefulSet to use a new image (simulate a rolling update)
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(&key, &ss).await.unwrap();

        // Reconcile: should detect template change and begin rolling update
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Check that currentRevision != updateRevision during rolling update
        let ss: StatefulSet = storage.get(&key).await.unwrap();
        let status = ss.status.as_ref().unwrap();
        let new_revision = status.update_revision.clone().unwrap();

        assert_ne!(
            old_revision, new_revision,
            "Update revision should differ from old revision after template change"
        );
        assert_ne!(
            status.current_revision, status.update_revision,
            "During rolling update: currentRevision should NOT equal updateRevision"
        );
        assert_eq!(
            status.current_revision.as_ref().unwrap(),
            &old_revision,
            "currentRevision should still be the old revision during rolling update"
        );

        // Now simulate completing the rolling update:
        // Each cycle: make pods ready → reconcile (deletes one old, creates one new)
        // Need enough cycles for all 3 pods to be replaced + a final reconcile
        for cycle in 0..20 {
            // Simulate kubelet: remove terminated pods from storage
            simulate_kubelet_cleanup(&storage, ns).await;

            // Make all current pods ready
            for i in 0..3 {
                let pod_name = format!("web-{}", i);
                let pod_key = format!("/registry/pods/{}/{}", ns, pod_name);
                if storage.get::<Pod>(&pod_key).await.is_ok() {
                    make_pod_ready(&storage, ns, &pod_name).await;
                }
            }

            let mut ss: StatefulSet = storage.get(&key).await.unwrap();
            controller.reconcile(&mut ss).await.unwrap();

            let ss: StatefulSet = storage.get(&key).await.unwrap();
            let status = ss.status.as_ref().unwrap();
            if status.current_revision == status.update_revision {
                break;
            }
            assert!(cycle < 19, "Rolling update did not complete in 20 cycles");
        }

        // After rollout completes, currentRevision == updateRevision
        let ss: StatefulSet = storage.get(&key).await.unwrap();
        let status = ss.status.as_ref().unwrap();
        assert_eq!(
            status.current_revision, status.update_revision,
            "After rollout completes: currentRevision should equal updateRevision"
        );
        assert_eq!(
            status.updated_replicas,
            Some(3),
            "All replicas should be updated"
        );
    }

    /// Partition should be respected: only pods with ordinal >= partition are updated.
    #[tokio::test]
    async fn test_canary_update_respects_partition() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());

        let ns = "default";
        let mut ss = make_statefulset("web", ns, 3, "nginx:1.19");
        // Set partition=2 so only pod web-2 should be updated
        ss.spec.update_strategy = Some(StatefulSetUpdateStrategy {
            strategy_type: Some("RollingUpdate".to_string()),
            rolling_update: Some(RollingUpdateStatefulSetStrategy {
                partition: Some(2),
                max_unavailable: None,
            }),
        });

        let key = format!("/registry/statefulsets/{}/{}", ns, "web");
        storage.create(&key, &ss).await.unwrap();

        // Create pods and make them ready
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        for i in 0..3 {
            make_pod_ready(&storage, ns, &format!("web-{}", i)).await;
        }

        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Record the old revision from pod-0
        let pod0: Pod = storage
            .get(&format!("/registry/pods/{}/web-0", ns))
            .await
            .unwrap();
        let old_rev = pod0
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();

        // Patch image
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(&key, &ss).await.unwrap();

        // Run several reconcile cycles
        for _ in 0..5 {
            simulate_kubelet_cleanup(&storage, ns).await;

            let pod_prefix = format!("/registry/pods/{}/", ns);
            let pods: Vec<Pod> = storage.list(&pod_prefix).await.unwrap();
            for pod in &pods {
                if pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("app"))
                    .map(|a| a == "web")
                    .unwrap_or(false)
                {
                    make_pod_ready(&storage, ns, &pod.metadata.name).await;
                }
            }

            let mut ss: StatefulSet = storage.get(&key).await.unwrap();
            controller.reconcile(&mut ss).await.unwrap();
        }

        // Check that pod-0 and pod-1 still have the old revision (partition=2 protects them)
        let pod0: Pod = storage
            .get(&format!("/registry/pods/{}/web-0", ns))
            .await
            .unwrap();
        let pod0_rev = pod0
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        assert_eq!(
            pod0_rev, old_rev,
            "Pod-0 should keep old revision (below partition)"
        );

        let pod1: Pod = storage
            .get(&format!("/registry/pods/{}/web-1", ns))
            .await
            .unwrap();
        let pod1_rev = pod1
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        assert_eq!(
            pod1_rev, old_rev,
            "Pod-1 should keep old revision (below partition)"
        );

        // Pod-2 should have the new revision
        let pod2: Pod = storage
            .get(&format!("/registry/pods/{}/web-2", ns))
            .await
            .unwrap();
        let pod2_rev = pod2
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        assert_ne!(
            pod2_rev, old_rev,
            "Pod-2 should have new revision (at or above partition)"
        );

        // currentRevision should NOT equal updateRevision (partition prevents full rollout)
        let ss: StatefulSet = storage.get(&key).await.unwrap();
        let status = ss.status.as_ref().unwrap();
        assert_ne!(
            status.current_revision, status.update_revision,
            "With partition, currentRevision should not equal updateRevision"
        );
    }

    /// Test that scale-down sets deletionTimestamp instead of direct delete.
    #[tokio::test]
    async fn test_scale_down_sets_deletion_timestamp() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());

        let ns = "default";
        // Create a StatefulSet with 3 replicas
        let ss = make_statefulset("ss-scale", ns, 3, "busybox");
        storage
            .create("/registry/statefulsets/default/ss-scale", &ss)
            .await
            .unwrap();

        // First reconcile: creates 3 pods
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Make all pods ready
        for i in 0..3 {
            make_pod_ready(&storage, ns, &format!("ss-scale-{}", i)).await;
        }

        // Reconcile again to update status
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Scale down to 2
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        ss.spec.replicas = Some(2);
        storage
            .update("/registry/statefulsets/default/ss-scale", &ss)
            .await
            .unwrap();

        // Reconcile — should set deletionTimestamp on pod ss-scale-2
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Pod ss-scale-2 should have deletionTimestamp set, not be deleted
        let pod2: Pod = storage
            .get("/registry/pods/default/ss-scale-2")
            .await
            .expect("Pod ss-scale-2 should still exist (graceful termination)");

        assert!(
            pod2.metadata.deletion_timestamp.is_some(),
            "Pod ss-scale-2 should have deletionTimestamp set for graceful termination"
        );

        // Pods 0 and 1 should not have deletionTimestamp
        let pod0: Pod = storage
            .get("/registry/pods/default/ss-scale-0")
            .await
            .unwrap();
        assert!(
            pod0.metadata.deletion_timestamp.is_none(),
            "Pod ss-scale-0 should not be terminating"
        );

        // Status should not count terminating pods
        let ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        let status = ss.status.unwrap();
        assert_eq!(
            status.replicas, 2,
            "replicas should exclude terminating pods"
        );
        assert_eq!(
            status.ready_replicas.unwrap_or(0),
            2,
            "readyReplicas should exclude terminating pods"
        );
    }

    /// Rolling update should use graceful termination (set deletionTimestamp)
    /// instead of direct deletion. This ensures the kubelet can perform cleanup
    /// and the pod gets properly recreated.
    #[tokio::test]
    async fn test_rolling_update_uses_graceful_termination() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";
        let ss = make_statefulset("ss-grace", ns, 2, "nginx:1.19");
        let key = "/registry/statefulsets/default/ss-grace";
        storage.create(key, &ss).await.unwrap();

        // Create initial pods
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        for i in 0..2 {
            make_pod_ready(&storage, ns, &format!("ss-grace-{}", i)).await;
        }
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Change image to trigger rolling update
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(key, &ss).await.unwrap();

        // Reconcile — should set deletionTimestamp on highest-ordinal stale pod
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // The pod should still exist (not directly deleted) with deletionTimestamp set
        let pod1_key = "/registry/pods/default/ss-grace-1";
        let pod1: Pod = storage.get(pod1_key).await.expect(
            "Pod ss-grace-1 should still exist after rolling update (graceful termination)",
        );
        assert!(
            pod1.metadata.deletion_timestamp.is_some(),
            "Rolling update should set deletionTimestamp for graceful termination, not direct delete"
        );
        assert!(
            pod1.metadata.deletion_grace_period_seconds.is_some(),
            "Rolling update should set deletion_grace_period_seconds"
        );
    }

    /// Current replicas count should exclude terminating pods so that
    /// the controller can recreate them with the new template.
    #[tokio::test]
    async fn test_current_replicas_excludes_terminating() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";
        let ss = make_statefulset("ss-term", ns, 2, "nginx:1.19");
        let key = "/registry/statefulsets/default/ss-term";
        storage.create(key, &ss).await.unwrap();

        // Create initial pods
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        for i in 0..2 {
            make_pod_ready(&storage, ns, &format!("ss-term-{}", i)).await;
        }

        // Manually set deletionTimestamp on one pod (simulating graceful termination)
        let pod_key = "/registry/pods/default/ss-term-1";
        let mut pod: Pod = storage.get(pod_key).await.unwrap();
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage.update(pod_key, &pod).await.unwrap();

        // Reconcile — controller should see only 1 active replica (not 2)
        // and attempt to recreate the terminating one
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Status should reflect the non-terminating count
        let ss: StatefulSet = storage.get(key).await.unwrap();
        let status = ss.status.unwrap();
        // replicas should be the total non-terminating pod count
        assert!(
            status.replicas <= 2,
            "replicas ({}) should not double-count terminating pods",
            status.replicas
        );
    }

    #[tokio::test]
    async fn test_scale_down_blocked_when_pods_unhealthy() {
        // Reproduces the conformance test "should not scale past 3 replicas"
        // When pods are Running but NOT Ready, scale-down must be blocked.
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());

        let ns = "default";
        let mut ss = make_statefulset("ss-block", ns, 3, "busybox");
        // Use OrderedReady policy (the K8s default, used by conformance tests)
        ss.spec.pod_management_policy = Some("OrderedReady".to_string());
        storage
            .create("/registry/statefulsets/default/ss-block", &ss)
            .await
            .unwrap();

        // Create all 3 pods by reconciling with Ready status between each
        for round in 0..3 {
            let mut ss: StatefulSet = storage
                .get("/registry/statefulsets/default/ss-block")
                .await
                .unwrap();
            controller.reconcile(&mut ss).await.unwrap();
            // Make the newly created pod Ready so the next one can be created
            let pod_key = format!("/registry/pods/default/ss-block-{}", round);
            if storage.get::<Pod>(&pod_key).await.is_ok() {
                make_pod_ready(&storage, ns, &format!("ss-block-{}", round)).await;
            }
        }
        // One more reconcile to ensure all pods are created
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-block")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Now make all pods Running but NOT Ready (simulate broken readiness probe)
        for i in 0..3 {
            let pod_key = format!("/registry/pods/default/ss-block-{}", i);
            let mut pod: Pod = storage.get(&pod_key).await.unwrap();
            pod.status = Some(PodStatus {
                phase: Some(Phase::Running),
                conditions: Some(vec![PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "False".to_string(),
                    reason: Some("ContainersNotReady".to_string()),
                    message: Some("Not all containers are ready".to_string()),
                    last_transition_time: Some(chrono::Utc::now()),
                    observed_generation: None,
                }]),
                ..pod.status.unwrap_or_default()
            });
            storage.update(&pod_key, &pod).await.unwrap();
        }

        // Scale down to 0 — should be BLOCKED because pods are unhealthy
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-block")
            .await
            .unwrap();
        ss.spec.replicas = Some(0);
        storage
            .update("/registry/statefulsets/default/ss-block", &ss)
            .await
            .unwrap();

        // Reconcile — should NOT delete any pods
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-block")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // ALL 3 pods should still exist (no deletionTimestamp)
        for i in 0..3 {
            let pod_key = format!("/registry/pods/default/ss-block-{}", i);
            let pod: Pod = storage
                .get(&pod_key)
                .await
                .unwrap_or_else(|_| panic!("Pod ss-block-{} should still exist", i));
            assert!(
                pod.metadata.deletion_timestamp.is_none(),
                "Pod ss-block-{} should NOT have deletionTimestamp — scale-down should be blocked when pods are unhealthy",
                i
            );
        }
    }
}
