use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::{
    resources::{
        Deployment, DeploymentCondition, DeploymentStatus, Pod, ReplicaSet, ReplicaSetSpec,
    },
    types::{ObjectMeta, TypeMeta},
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

/// Parse a value that can be either an absolute integer or a percentage string (e.g. "25%" or "1").
/// For percentages, the result is ceil(pct/100 * total). Defaults to 1 if unparseable.
fn parse_int_or_percent(s: &str, total: i32) -> i32 {
    if s.ends_with('%') {
        let pct: f64 = s.trim_end_matches('%').parse().unwrap_or(25.0);
        ((pct / 100.0) * total as f64).ceil() as i32
    } else {
        s.parse().unwrap_or(1)
    }
}

/// Compute the max surge and max unavailable counts for a rolling update.
/// Returns (max_surge, max_unavailable).
fn compute_rolling_update_counts(
    desired: i32,
    max_surge: &str,
    max_unavailable: &str,
) -> (i32, i32) {
    let surge = parse_int_or_percent(max_surge, desired);
    let unavailable = parse_int_or_percent(max_unavailable, desired);
    (surge, unavailable)
}

/// DeploymentController reconciles Deployment resources by creating and managing ReplicaSets
/// This follows the Kubernetes pattern: Deployment -> ReplicaSet -> Pods
pub struct DeploymentController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
}

impl<S: Storage + 'static> DeploymentController<S> {
    pub fn new(storage: Arc<S>, interval_secs: u64) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(interval_secs),
        }
    }

    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        info!("Deployment controller started (watch-based)");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch for changes to Deployments AND ReplicaSets
            let prefix = build_prefix("deployments", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish watch: {}, retrying in {:?}",
                        e, self.interval
                    );
                    tokio::time::sleep(self.interval).await;
                    continue;
                }
            };

            let rs_prefix = build_prefix("replicasets", None);
            let mut rs_watch = match self.storage.watch(&rs_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish replicaset watch: {}, retrying in {:?}",
                        e, self.interval
                    );
                    tokio::time::sleep(self.interval).await;
                    continue;
                }
            };

            // Periodic full resync as safety net
            let mut resync = tokio::time::interval(std::time::Duration::from_secs(5));
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
                    event = rs_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                self.enqueue_owner_deployment(&queue, &ev).await;
                            }
                            Some(Err(e)) => {
                                warn!("ReplicaSet watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("ReplicaSet watch stream ended, reconnecting");
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
            let storage_key = build_key("deployments", Some(ns), name);
            match self.storage.get::<Deployment>(&storage_key).await {
                Ok(resource) => match self.reconcile_deployment(&resource).await {
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
        match self
            .storage
            .list::<Deployment>("/registry/deployments/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("deployments/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list deployments for enqueue: {}", e);
            }
        }
    }

    /// When a ReplicaSet changes, check its ownerReferences for a Deployment owner
    /// and enqueue that Deployment for reconciliation.
    async fn enqueue_owner_deployment(
        &self,
        queue: &WorkQueue,
        event: &rusternetes_storage::WatchEvent,
    ) {
        let rs_key = extract_key(event);
        let parts: Vec<&str> = rs_key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };

        let storage_key = format!("/registry/{}", rs_key);
        match self.storage.get::<ReplicaSet>(&storage_key).await {
            Ok(rs) => {
                if let Some(refs) = &rs.metadata.owner_references {
                    for owner_ref in refs {
                        if owner_ref.kind == "Deployment" {
                            queue
                                .add(format!("deployments/{}/{}", ns, owner_ref.name))
                                .await;
                        }
                    }
                }
            }
            Err(_) => {
                // ReplicaSet deleted — enqueue all Deployments in this namespace
                if let Ok(items) = self
                    .storage
                    .list::<Deployment>(&build_prefix("deployments", Some(ns)))
                    .await
                {
                    for d in &items {
                        queue
                            .add(format!("deployments/{}/{}", ns, d.metadata.name))
                            .await;
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> rusternetes_common::Result<()> {
        debug!("Reconciling all deployments");

        // Get all deployments
        let prefix = build_prefix("deployments", None);
        let deployments: Vec<Deployment> = self.storage.list(&prefix).await?;

        for deployment in deployments {
            if let Err(e) = self.reconcile_deployment(&deployment).await {
                error!(
                    "Error reconciling deployment {}: {}",
                    deployment.metadata.name, e
                );
            }
        }

        Ok(())
    }

    pub async fn reconcile_deployment(
        &self,
        deployment: &Deployment,
    ) -> rusternetes_common::Result<()> {
        let namespace = deployment
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        // K8s deployment controller skips reconciliation for deleted deployments.
        // The GC handles cascade/orphan deletion via finalizers.
        // K8s ref: pkg/controller/deployment/deployment_controller.go — syncDeployment
        if deployment.metadata.deletion_timestamp.is_some() {
            debug!(
                "Deployment {}/{} is being deleted, skipping reconciliation",
                namespace, deployment.metadata.name
            );
            return Ok(());
        }

        debug!(
            "Reconciling deployment: {}/{}",
            namespace, deployment.metadata.name
        );

        // Get all ReplicaSets and claim/adopt matching ones (K8s ClaimReplicaSets pattern)
        let rs_prefix = build_prefix("replicasets", Some(namespace));
        let all_replicasets: Vec<ReplicaSet> = self.storage.list(&rs_prefix).await?;

        // Adopt orphan ReplicaSets whose labels match the deployment's selector
        let selector = &deployment.spec.selector;
        for rs in &all_replicasets {
            // Skip if already owned by this deployment
            if self.is_owned_by_deployment(rs, deployment) {
                continue;
            }
            // Skip if owned by another controller
            let has_controller_owner = rs
                .metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| r.controller == Some(true)))
                .unwrap_or(false);
            if has_controller_owner {
                continue;
            }
            // Check if RS labels match deployment selector
            let labels_match = if let Some(match_labels) = &selector.match_labels {
                if let Some(rs_labels) = &rs.metadata.labels {
                    match_labels
                        .iter()
                        .all(|(k, v)| rs_labels.get(k) == Some(v))
                } else {
                    false
                }
            } else {
                false
            };
            if labels_match {
                // Adopt: add ownerReference
                let mut adopted = rs.clone();
                let owner_ref = rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    name: deployment.metadata.name.clone(),
                    uid: deployment.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                };
                adopted
                    .metadata
                    .owner_references
                    .get_or_insert_with(Vec::new)
                    .push(owner_ref);
                let rs_key = build_key("replicasets", Some(namespace), &rs.metadata.name);
                let _ = self.storage.update(&rs_key, &adopted).await;
                info!(
                    "Adopted orphan ReplicaSet {} into Deployment {}/{}",
                    rs.metadata.name, namespace, deployment.metadata.name
                );
            }
        }

        // Re-fetch after adoption
        let all_replicasets: Vec<ReplicaSet> = self.storage.list(&rs_prefix).await?;
        let owned_replicasets: Vec<ReplicaSet> = all_replicasets
            .into_iter()
            .filter(|rs| self.is_owned_by_deployment(rs, deployment))
            .collect();

        // K8s deployment revision handling (sync.go:146-190):
        // 1. Find "new" RS (matches current template) and "old" RSes (don't match)
        // 2. newRevision = MaxRevision(oldRSes) + 1
        // 3. Set new RS annotation to newRevision
        // 4. Set deployment annotation to newRevision
        {
            let template_hash = Self::compute_pod_template_hash(deployment);
            let max_old_revision = owned_replicasets
                .iter()
                .filter(|rs| {
                    // "Old" RSes: those that DON'T match the current template
                    let rs_hash = rs
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("pod-template-hash"))
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    rs_hash != template_hash
                })
                .filter_map(|rs| {
                    rs.metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                        .and_then(|v| v.parse::<i64>().ok())
                })
                .max()
                .unwrap_or(0);

            // Also consider the new RS's current revision (it might already be higher)
            let new_rs_revision = owned_replicasets
                .iter()
                .filter(|rs| {
                    let rs_hash = rs
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("pod-template-hash"))
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    rs_hash == template_hash
                })
                .filter_map(|rs| {
                    rs.metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                        .and_then(|v| v.parse::<i64>().ok())
                })
                .max()
                .unwrap_or(0);

            let new_revision = std::cmp::max(max_old_revision + 1, new_rs_revision);
            let revision_str = std::cmp::max(new_revision, 1).to_string();

            // Update new RS annotation if needed
            for rs in &owned_replicasets {
                let rs_hash = rs
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("pod-template-hash"))
                    .map(|s| s.as_str())
                    .unwrap_or("");
                if rs_hash == template_hash {
                    let current_rs_rev = rs
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                        .cloned()
                        .unwrap_or_default();
                    if current_rs_rev != revision_str {
                        let mut updated_rs = rs.clone();
                        updated_rs
                            .metadata
                            .annotations
                            .get_or_insert_with(std::collections::HashMap::new)
                            .insert(
                                "deployment.kubernetes.io/revision".to_string(),
                                revision_str.clone(),
                            );
                        let rs_key = build_key("replicasets", Some(namespace), &rs.metadata.name);
                        let _ = self.storage.update(&rs_key, &updated_rs).await;
                    }
                }
            }

            // Update deployment annotation
            let current_dep_rev = deployment
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                .cloned()
                .unwrap_or_default();
            if current_dep_rev != revision_str {
                let mut updated = deployment.clone();
                updated
                    .metadata
                    .annotations
                    .get_or_insert_with(std::collections::HashMap::new)
                    .insert(
                        "deployment.kubernetes.io/revision".to_string(),
                        revision_str,
                    );
                let key = build_key("deployments", Some(namespace), &deployment.metadata.name);
                let _ = self.storage.update(&key, &updated).await;
            }
        }

        debug!(
            "Found {} ReplicaSets owned by deployment {}/{}",
            owned_replicasets.len(),
            namespace,
            deployment.metadata.name
        );

        // Paused deployments freeze all rollout activity: no new ReplicaSet
        // creation, no scaling of existing RSes (up or down). Status is still
        // refreshed so callers see current replica counts.
        // K8s ref: pkg/controller/deployment/deployment_controller.go — syncDeployment
        // delegates to sync() (status-only path) when d.Spec.Paused is true.
        if deployment.spec.paused.unwrap_or(false) {
            debug!(
                "Deployment {}/{} is paused; skipping rollout (status only)",
                namespace, deployment.metadata.name
            );
            return self.update_deployment_status(deployment).await;
        }

        // Find the active ReplicaSet (matches current pod template)
        let active_rs = owned_replicasets
            .iter()
            .find(|rs| self.replicaset_matches_template(rs, deployment));

        let desired_replicas = deployment.spec.replicas.unwrap_or(1);

        // Determine if this is a RollingUpdate strategy
        let is_rolling_update = deployment
            .spec
            .strategy
            .as_ref()
            .map(|s| s.strategy_type == "RollingUpdate")
            .unwrap_or(true); // Default strategy is RollingUpdate

        // Get rolling update parameters
        let (max_surge_str, max_unavailable_str) = deployment
            .spec
            .strategy
            .as_ref()
            .and_then(|s| s.rolling_update.as_ref())
            .map(|ru| {
                let surge = ru
                    .max_surge
                    .as_ref()
                    .and_then(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .or_else(|| v.as_i64().map(|n| n.to_string()))
                    })
                    .unwrap_or_else(|| "25%".to_string());
                let unavail = ru
                    .max_unavailable
                    .as_ref()
                    .and_then(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .or_else(|| v.as_i64().map(|n| n.to_string()))
                    })
                    .unwrap_or_else(|| "25%".to_string());
                (surge, unavail)
            })
            .unwrap_or(("25%".to_string(), "25%".to_string()));

        let (max_surge, max_unavailable) =
            compute_rolling_update_counts(desired_replicas, &max_surge_str, &max_unavailable_str);

        // Calculate total old RS replicas
        let old_rs_total: i32 = owned_replicasets
            .iter()
            .filter(|rs| {
                if let Some(active) = owned_replicasets
                    .iter()
                    .find(|rs| self.replicaset_matches_template(rs, deployment))
                {
                    rs.metadata.name != active.metadata.name
                } else {
                    true
                }
            })
            .map(|rs| rs.spec.replicas)
            .sum();

        // Proportional scaling: only runs during scaling events (when
        // deployment.spec.replicas changes while a rolling update is in progress).
        // K8s ref: pkg/controller/deployment/sync.go — scale() + isScalingEvent()
        //
        // A scaling event is detected by checking the "deployment.kubernetes.io/desired-replicas"
        // annotation on active ReplicaSets. If the annotation differs from the deployment's
        // desired replicas, this is a scaling event that requires proportional distribution.
        // During normal rolling updates (template change only), this block is skipped.
        let is_scaling_event = if is_rolling_update && old_rs_total > 0 {
            let active_rs_list: Vec<&ReplicaSet> = owned_replicasets
                .iter()
                .filter(|rs| rs.spec.replicas > 0)
                .collect();
            active_rs_list.iter().any(|rs| {
                rs.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("deployment.kubernetes.io/desired-replicas"))
                    .and_then(|v| v.parse::<i32>().ok())
                    .map(|annotated_desired| annotated_desired != desired_replicas)
                    .unwrap_or(false) // No annotation = not a scaling event
            })
        } else {
            false
        };

        if is_scaling_event {
            let all_rs_replicas: i32 = owned_replicasets.iter().map(|rs| rs.spec.replicas).sum();
            let allowed_size = desired_replicas + max_surge;
            let replicas_to_add = allowed_size - all_rs_replicas;

            if replicas_to_add != 0 {
                // K8s proportional scaling formula (deployment_util.go:getReplicaSetFraction):
                //   newSize = rs.Replicas * (newMaxTotal / oldMaxTotal)
                //   fraction = round(newSize) - rs.Replicas
                // where oldMaxTotal comes from the RS "max-replicas" annotation.
                // K8s ref: pkg/controller/deployment/util/deployment_util.go
                //
                // K8s sorts the RS list before distributing the delta so that
                // the bigger RS rounds first and absorbs its proportional share
                // before "allowed" narrows for the smaller one. Sort by
                // spec.replicas DESC; on ties break by creation timestamp:
                // older-first when scaling down (ReplicaSetsBySizeOlder),
                // newer-first when scaling up (ReplicaSetsBySizeNewer).
                // K8s ref: pkg/controller/controller_utils.go.
                let mut sorted_rss: Vec<&ReplicaSet> = owned_replicasets.iter().collect();
                let scaling_up = replicas_to_add > 0;
                sorted_rss.sort_by(|a, b| {
                    b.spec.replicas.cmp(&a.spec.replicas).then_with(|| {
                        let primary = a
                            .metadata
                            .creation_timestamp
                            .cmp(&b.metadata.creation_timestamp); // older first
                        if scaling_up {
                            primary.reverse() // newer first when scaling up
                        } else {
                            primary
                        }
                        .then_with(|| a.metadata.name.cmp(&b.metadata.name))
                    })
                });

                let mut added = 0i32;
                let mut updates: Vec<(String, i32)> = Vec::new();

                for rs in sorted_rss.iter().copied() {
                    if rs.spec.replicas == 0 {
                        continue;
                    }
                    // Read oldMaxTotal from RS annotation (set during previous scale)
                    let old_max = rs
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get("deployment.kubernetes.io/max-replicas"))
                        .and_then(|v| v.parse::<i32>().ok())
                        .unwrap_or(all_rs_replicas); // fallback if no annotation

                    let fraction = if old_max > 0 {
                        let new_size =
                            (rs.spec.replicas as f64) * (allowed_size as f64) / (old_max as f64);
                        new_size.round() as i32 - rs.spec.replicas
                    } else {
                        0
                    };

                    let allowed = replicas_to_add - added;
                    let proportion = if replicas_to_add > 0 {
                        fraction.min(allowed).max(0)
                    } else {
                        fraction.max(allowed).min(0)
                    };

                    let new_replicas = (rs.spec.replicas + proportion).max(0);
                    if new_replicas != rs.spec.replicas {
                        updates.push((rs.metadata.name.clone(), new_replicas));
                    }
                    added += proportion;
                }

                // Apply leftover to first RS (K8s assigns leftover to largest/newest)
                if !updates.is_empty() && replicas_to_add != added {
                    let leftover = replicas_to_add - added;
                    updates[0].1 = (updates[0].1 + leftover).max(0);
                }

                for (rs_name, new_replicas) in &updates {
                    if let Some(rs) = owned_replicasets
                        .iter()
                        .find(|r| &r.metadata.name == rs_name)
                    {
                        info!(
                            "Proportional scaling: {}/{} {} -> {}",
                            namespace, rs_name, rs.spec.replicas, new_replicas
                        );
                        self.update_replicaset_replicas(rs, *new_replicas).await?;
                    }
                }

                // Update desired-replicas annotation on all active RSes after scaling.
                // K8s SetReplicasAnnotations sets max-replicas = desired + maxSurge.
                for rs in owned_replicasets.iter() {
                    if rs.spec.replicas > 0 {
                        self.set_desired_replicas_annotation(
                            rs,
                            desired_replicas,
                            max_surge,
                            namespace,
                        )
                        .await;
                    }
                }

                if !updates.is_empty() {
                    // Status will be updated at end of reconcile
                    return self.update_deployment_status(deployment).await;
                }
            }
        }

        if let Some(active) = active_rs {
            let active_name = active.metadata.name.clone();
            let active_replicas = active.spec.replicas;

            // K8s scale() — simple scaling: if only one active RS (no old RSes
            // with pods), scale it directly to the desired replica count.
            // K8s ref: pkg/controller/deployment/sync.go:310-316
            if old_rs_total == 0 && active_replicas != desired_replicas {
                info!(
                    "Scaling active ReplicaSet {}/{} from {} to {} (no old RSes)",
                    namespace, active_name, active_replicas, desired_replicas
                );
                self.update_replicaset_replicas(active, desired_replicas)
                    .await?;
                return self.update_deployment_status(deployment).await;
            }

            // Also handle the case where the new RS has the right count but old
            // RSes still have pods — need to scale them down even without a
            // rolling update in progress.
            // K8s ref: sync.go:320-327 — IsSaturated check.
            //
            // IsSaturated requires the new RS to be fully scaled up AND every
            // replica available, not merely spec.replicas == desired:
            //   Spec.Replicas == Status.Replicas == Status.AvailableReplicas == desired
            // (deployment_util.go::IsSaturated). Scaling old RSes to 0 on the
            // spec count alone tears down the running old pods while the new
            // ones are still unavailable — violating maxUnavailable. The
            // conformance "deployment should support rollover" creates a new RS
            // on a nonexistent image (never available) and asserts the old RS
            // stays at 1; the spec-only check wrongly scaled it to 0.
            let active_available = if let Some(status) = &active.status {
                status.available_replicas
            } else {
                self.count_available_pods_for_rs(&active_name, namespace)
                    .await
            };
            let new_rs_saturated =
                active_replicas == desired_replicas && active_available == desired_replicas;
            if new_rs_saturated && old_rs_total > 0 && !is_scaling_event {
                // New RS is saturated (fully available) — scale all old RSes to 0.
                for rs in owned_replicasets.iter() {
                    if rs.metadata.name != active_name && rs.spec.replicas > 0 {
                        info!(
                            "Saturated new RS: scaling down old ReplicaSet {}/{} to 0",
                            namespace, rs.metadata.name
                        );
                        self.update_replicaset_replicas(rs, 0).await?;
                    }
                }
            }

            if is_rolling_update && active_replicas < desired_replicas && old_rs_total > 0 {
                // Rolling update in progress: gradually scale new RS up and old RS down.
                // K8s ref: pkg/controller/deployment/rolling.go — rolloutRolling()
                //
                // reconcileNewReplicaSet: scale up based on maxTotalPods - currentPodCount
                // reconcileOldReplicaSets: scale down based on allPodsCount - minAvailable - newRSUnavailable
                let max_total = desired_replicas + max_surge;

                // K8s NewRSNewReplicas (deployment_util.go:820):
                // currentPodCount = sum of all RS replicas (spec, not status)
                // scaleUpCount = maxTotalPods - currentPodCount
                // scaleUpCount = min(scaleUpCount, desired - newRS.Replicas)
                let current_pod_count: i32 =
                    owned_replicasets.iter().map(|rs| rs.spec.replicas).sum();
                let scale_up_count = (max_total - current_pod_count).max(0);
                let scale_up_count = scale_up_count.min(desired_replicas - active_replicas);
                let new_active_replicas = active_replicas + scale_up_count;

                if new_active_replicas != active_replicas {
                    info!(
                        "Rolling update: scaling new ReplicaSet {}/{} from {} to {} (max_total={}, current_total={})",
                        namespace, active_name, active_replicas, new_active_replicas, max_total, current_pod_count
                    );
                    self.update_replicaset_replicas(
                        &owned_replicasets
                            .iter()
                            .find(|rs| rs.metadata.name == active_name)
                            .unwrap()
                            .clone(),
                        new_active_replicas,
                    )
                    .await?;
                }

                // reconcileOldReplicaSets — two-phase scale-down (cleanup
                // unhealthy, then scale down healthy to minAvailable). Shared
                // with the active>=desired path so both use lag-free,
                // minReadySeconds-aware availability.
                self.reconcile_old_replicasets(
                    &owned_replicasets,
                    &active_name,
                    new_active_replicas,
                    desired_replicas,
                    max_unavailable,
                    namespace,
                )
                .await?;
            } else {
                // No rolling update needed (or already at desired), just ensure correct count
                if active_replicas != desired_replicas {
                    info!(
                        "Updating ReplicaSet {}/{} replicas from {} to {}",
                        namespace, active_name, active_replicas, desired_replicas
                    );
                    self.update_replicaset_replicas(
                        &owned_replicasets
                            .iter()
                            .find(|rs| rs.metadata.name == active_name)
                            .unwrap()
                            .clone(),
                        desired_replicas,
                    )
                    .await?;
                }

                // Scale down old ReplicaSets.
                if is_rolling_update {
                    // active >= desired: the new RS is at full count. Use the
                    // same availability-gated two-phase reconcileOldReplicaSets
                    // as the active<desired path, so old pods are never drained
                    // below minAvailable while the new RS is still inside its
                    // minReadySeconds window (conformance "deployment should
                    // support rollover").
                    self.reconcile_old_replicasets(
                        &owned_replicasets,
                        &active_name,
                        desired_replicas,
                        desired_replicas,
                        max_unavailable,
                        namespace,
                    )
                    .await?;
                } else {
                    // Recreate / non-rolling: once the new RS is fully available,
                    // scale every old ReplicaSet to 0.
                    let new_rs_available = self
                        .count_available_pods_for_rs(&active_name, namespace)
                        .await;
                    if new_rs_available >= desired_replicas && old_rs_total > 0 {
                        for rs in owned_replicasets.iter() {
                            if rs.metadata.name != active_name && rs.spec.replicas > 0 {
                                info!(
                                    "Scaling down old ReplicaSet {}/{} to 0 (new RS available={})",
                                    namespace, rs.metadata.name, new_rs_available
                                );
                                self.update_replicaset_replicas(rs, 0).await?;
                            }
                        }
                    }
                }
            }
        } else {
            // No active ReplicaSet, create one
            info!(
                "Creating new ReplicaSet for deployment {}/{}",
                namespace, deployment.metadata.name
            );

            if !is_rolling_update && old_rs_total > 0 {
                // Recreate strategy: scale down ALL old RSs to 0 FIRST
                for rs in owned_replicasets.iter() {
                    if rs.spec.replicas > 0 {
                        info!(
                            "Recreate: scaling down old ReplicaSet {}/{} to 0",
                            namespace, rs.metadata.name
                        );
                        self.update_replicaset_replicas(rs, 0).await?;
                    }
                }
                // Wait for old pods to actually be gone before creating new RS.
                // Count non-terminated pods owned by old ReplicaSets.
                let pods_prefix = build_prefix("pods", Some(namespace));
                let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await?;
                let old_pods_remaining = all_pods
                    .iter()
                    .filter(|p| {
                        p.metadata.deletion_timestamp.is_none()
                            && p.metadata
                                .owner_references
                                .as_ref()
                                .map(|refs| {
                                    refs.iter().any(|r| {
                                        r.kind == "ReplicaSet"
                                            && owned_replicasets
                                                .iter()
                                                .any(|rs| r.uid == rs.metadata.uid)
                                    })
                                })
                                .unwrap_or(false)
                    })
                    .count();
                if old_pods_remaining > 0 {
                    debug!(
                        "Recreate: waiting for {} old pods to terminate before creating new RS",
                        old_pods_remaining
                    );
                    // Don't create new RS yet — wait for next reconcile cycle
                } else {
                    // All old pods gone — create new RS at full desired count
                    self.create_replicaset(deployment).await?;
                }
            } else if is_rolling_update && old_rs_total > 0 {
                // Start the rolling update: create new RS.
                // K8s ref: pkg/controller/deployment/rolling.go — rolloutRolling()
                //          pkg/controller/deployment/util/deployment_util.go — NewRSNewReplicas()
                //
                // K8s NewRSNewReplicas calculates: maxTotalPods - currentPodCount
                // For a rollover (mid-rollout update), currentPodCount = old_rs_total
                // and maxTotalPods = desired + maxSurge.
                let max_total = desired_replicas + max_surge;
                let scale_up_count = (max_total - old_rs_total).max(0);
                let initial_replicas = scale_up_count.min(desired_replicas).max(0);

                // If we can't scale up at all (currentPodCount >= maxTotalPods),
                // create with 0 replicas. K8s does this — it returns newRS.Spec.Replicas
                // which starts at 0 for a brand-new RS.
                self.create_replicaset_with_replicas(deployment, initial_replicas)
                    .await?;

                // Do NOT scale old ReplicaSets down in this same pass. Old-RS
                // scale-down is handled by the rolling-update branch above
                // (reconcileOldReplicaSets), which uses lag-free, minReadySeconds-
                // aware availability and a faithful two-phase (cleanup-unhealthy
                // then scale-down-healthy) algorithm. The previous inline loop
                // here scaled down old RSes in arbitrary order with no
                // availability check, draining a still-available old RS the
                // instant the new RS was created — dropping total availability
                // below minAvailable and failing the "deployment should support
                // rollover" rolling-update invariant. The next reconcile (with
                // the new RS now the active RS) performs the correct scale-down.
            } else {
                // No old RSs: create at full desired count
                self.create_replicaset(deployment).await?;
            }
        }

        // Clean up old ReplicaSets beyond revisionHistoryLimit.
        // K8s ref: pkg/controller/deployment/deployment_controller.go — cleanupDeployment()
        // After a rollout completes, delete old RSes (replicas=0) that exceed the history limit.
        // Re-fetch owned RSes since they may have changed during reconcile.
        let cleanup_rs_prefix = build_prefix("replicasets", Some(namespace));
        let cleanup_all_rs: Vec<ReplicaSet> = self
            .storage
            .list(&cleanup_rs_prefix)
            .await
            .unwrap_or_default();
        let cleanup_owned: Vec<ReplicaSet> = cleanup_all_rs
            .into_iter()
            .filter(|rs| self.is_owned_by_deployment(rs, deployment))
            .collect();

        let revision_history_limit = deployment.spec.revision_history_limit.unwrap_or(10);
        if revision_history_limit >= 0 {
            // Collect old RSes (those that don't match current template AND have 0 replicas)
            let mut old_rses: Vec<ReplicaSet> = cleanup_owned
                .iter()
                .filter(|rs| {
                    !self.replicaset_matches_template(rs, deployment) && rs.spec.replicas == 0
                })
                .cloned()
                .collect();

            // Sort by revision (ascending) so we delete the oldest first
            old_rses.sort_by(|a, b| {
                let rev_a = a
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|ann| ann.get("deployment.kubernetes.io/revision"))
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
                let rev_b = b
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|ann| ann.get("deployment.kubernetes.io/revision"))
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
                rev_a.cmp(&rev_b)
            });

            // Delete old RSes that exceed the history limit
            let to_delete = old_rses.len() as i32 - revision_history_limit;
            if to_delete > 0 {
                for rs in old_rses.iter().take(to_delete as usize) {
                    let rs_key = build_key("replicasets", Some(namespace), &rs.metadata.name);
                    info!(
                        "Cleaning up old ReplicaSet {}/{} (revisionHistoryLimit={})",
                        namespace, rs.metadata.name, revision_history_limit
                    );
                    let _ = self.storage.delete(&rs_key).await;
                }
            }
        }

        // Update deployment status
        self.update_deployment_status(deployment).await?;

        Ok(())
    }

    fn is_owned_by_deployment(&self, rs: &ReplicaSet, deployment: &Deployment) -> bool {
        if let Some(owner_refs) = &rs.metadata.owner_references {
            owner_refs.iter().any(|owner| {
                owner.kind == "Deployment"
                    && owner.name == deployment.metadata.name
                    && owner.uid == deployment.metadata.uid
            })
        } else {
            false
        }
    }

    fn replicaset_matches_template(&self, rs: &ReplicaSet, deployment: &Deployment) -> bool {
        // K8s EqualIgnoreHash: deep-compare templates ignoring pod-template-hash label.
        // See: pkg/controller/deployment/util/deployment_util.go:EqualIgnoreHash()
        //
        // Compare by serializing both templates to JSON, removing pod-template-hash,
        // and comparing the resulting JSON values.
        let mut rs_template = serde_json::to_value(&rs.spec.template).unwrap_or_default();
        let mut deploy_template =
            serde_json::to_value(&deployment.spec.template).unwrap_or_default();

        // Remove pod-template-hash from labels (K8s ignores this for comparison)
        for template in [&mut rs_template, &mut deploy_template] {
            if let Some(labels) = template
                .pointer_mut("/metadata/labels")
                .and_then(|l| l.as_object_mut())
            {
                labels.remove("pod-template-hash");
            }
            // Also remove fields that are defaulted differently between the
            // deployment template and the RS template. K8s normalizes templates
            // before comparison; we strip fields that vary due to defaulting.
            // Strip API server defaulted fields from PodSpec so templates
            // compare equal regardless of whether defaults were applied.
            // K8s ref: pkg/apis/core/v1/defaults.go SetDefaults_PodSpec/Container
            if let Some(spec) = template.pointer_mut("/spec") {
                if let Some(obj) = spec.as_object_mut() {
                    // PodSpec defaults (SetDefaults_PodSpec)
                    if obj.get("dnsPolicy") == Some(&serde_json::json!("ClusterFirst")) {
                        obj.remove("dnsPolicy");
                    }
                    if obj.get("restartPolicy") == Some(&serde_json::json!("Always")) {
                        obj.remove("restartPolicy");
                    }
                    if obj.get("schedulerName") == Some(&serde_json::json!("default-scheduler")) {
                        obj.remove("schedulerName");
                    }
                    if obj.get("terminationGracePeriodSeconds") == Some(&serde_json::json!(30)) {
                        obj.remove("terminationGracePeriodSeconds");
                    }
                    // Remove empty/null securityContext
                    if let Some(sc) = obj.get("securityContext") {
                        if sc.is_null() || sc == &serde_json::json!({}) {
                            obj.remove("securityContext");
                        }
                    }
                    // Remove null/empty optional fields
                    for field in &[
                        "nodeName",
                        "nodeSelector",
                        "hostname",
                        "subdomain",
                        "priority",
                        "runtimeClassName",
                        "overhead",
                        "serviceAccountName",
                        "serviceAccount",
                        "automountServiceAccountToken",
                        "preemptionPolicy",
                    ] {
                        if let Some(v) = obj.get(*field) {
                            if v.is_null() || v == &serde_json::json!("") {
                                obj.remove(*field);
                            }
                        }
                    }
                    // Container defaults (SetDefaults_Container)
                    for key in &["containers", "initContainers"] {
                        if let Some(containers) = obj.get_mut(*key).and_then(|c| c.as_array_mut()) {
                            for container in containers.iter_mut() {
                                if let Some(cobj) = container.as_object_mut() {
                                    if cobj.get("terminationMessagePath")
                                        == Some(&serde_json::json!("/dev/termination-log"))
                                    {
                                        cobj.remove("terminationMessagePath");
                                    }
                                    if cobj.get("terminationMessagePolicy")
                                        == Some(&serde_json::json!("File"))
                                    {
                                        cobj.remove("terminationMessagePolicy");
                                    }
                                    // ImagePullPolicy depends on tag — just remove it
                                    cobj.remove("imagePullPolicy");
                                    // Remove empty resources
                                    if let Some(r) = cobj.get("resources") {
                                        if r == &serde_json::json!({}) {
                                            cobj.remove("resources");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Remove empty volumes array
                    if let Some(v) = obj.get("volumes") {
                        if v == &serde_json::json!([]) || v.is_null() {
                            obj.remove("volumes");
                        }
                    }
                }
            }
        }

        let matches = rs_template == deploy_template;
        if !matches {
            debug!(
                "Template mismatch for RS {} vs deployment {}: RS={}, Deploy={}",
                rs.metadata.name,
                deployment.metadata.name,
                serde_json::to_string(&rs_template).unwrap_or_default(),
                serde_json::to_string(&deploy_template).unwrap_or_default()
            );
        }
        matches
    }

    async fn create_replicaset(&self, deployment: &Deployment) -> rusternetes_common::Result<()> {
        self.create_replicaset_with_replicas(deployment, deployment.spec.replicas.unwrap_or(1))
            .await
    }

    /// Generate a deterministic pod-template-hash from the pod template spec.
    /// Uses SHA-256 via serde_json::Value normalization (sorts HashMap keys).
    fn compute_pod_template_hash(deployment: &Deployment) -> String {
        use sha2::{Digest, Sha256};
        // Convert to Value first to normalize HashMap key ordering
        let value = serde_json::to_value(&deployment.spec.template).unwrap_or_default();
        let template_json = serde_json::to_string(&value).unwrap_or_default();
        let hash = Sha256::digest(template_json.as_bytes());
        format!(
            "{:08x}",
            u32::from_be_bytes(hash[..4].try_into().unwrap_or([0u8; 4]))
        )
    }

    async fn create_replicaset_with_replicas(
        &self,
        deployment: &Deployment,
        replicas: i32,
    ) -> rusternetes_common::Result<()> {
        let namespace = deployment
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        // Generate pod-template-hash from pod template
        let pod_template_hash = Self::compute_pod_template_hash(deployment);

        // Generate ReplicaSet name using the pod-template-hash
        let rs_name = format!("{}-{}", deployment.metadata.name, &pod_template_hash);

        let mut metadata = ObjectMeta::new(&rs_name);
        metadata.namespace = Some(namespace.to_string());

        // Start with template labels, then add pod-template-hash
        let mut labels = deployment
            .spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("pod-template-hash".to_string(), pod_template_hash.clone());
        metadata.labels = Some(labels);

        // Set revision annotation on the ReplicaSet.
        // Compute the next revision by finding the max among existing ReplicaSets + 1.
        let rs_prefix = build_prefix("replicasets", Some(namespace));
        let all_rs: Vec<ReplicaSet> = self.storage.list(&rs_prefix).await.unwrap_or_default();
        let max_existing_revision = all_rs
            .iter()
            .filter(|rs| {
                rs.metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| r.name == deployment.metadata.name))
                    .unwrap_or(false)
            })
            .filter_map(|rs| {
                rs.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                    .and_then(|v| v.parse::<i64>().ok())
            })
            .max()
            .unwrap_or(0);
        let new_revision = (max_existing_revision + 1).to_string();

        {
            let annotations = metadata
                .annotations
                .get_or_insert_with(std::collections::HashMap::new);
            annotations.insert(
                "deployment.kubernetes.io/revision".to_string(),
                new_revision.clone(),
            );
            // K8s sets desired-replicas and max-replicas annotations on each RS
            // for proportional scaling detection. See SetReplicasAnnotations() in
            // pkg/controller/deployment/util/deployment_util.go
            let desired = deployment.spec.replicas.unwrap_or(1);
            let local_max_surge = deployment
                .spec
                .strategy
                .as_ref()
                .and_then(|s| s.rolling_update.as_ref())
                .and_then(|ru| {
                    ru.max_surge.as_ref().and_then(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .or_else(|| v.as_i64().map(|n| n.to_string()))
                    })
                })
                .unwrap_or_else(|| "25%".to_string());
            let surge = parse_int_or_percent(&local_max_surge, desired);
            annotations.insert(
                "deployment.kubernetes.io/desired-replicas".to_string(),
                desired.to_string(),
            );
            annotations.insert(
                "deployment.kubernetes.io/max-replicas".to_string(),
                (desired + surge).to_string(),
            );
        }

        // Also update the deployment's revision annotation (with CAS retry)
        {
            let dep_key = build_key("deployments", Some(namespace), &deployment.metadata.name);
            let new_rev = new_revision.clone();
            for _ in 0..3 {
                match self.storage.get::<Deployment>(&dep_key).await {
                    Ok(mut dep) => {
                        dep.metadata
                            .annotations
                            .get_or_insert_with(std::collections::HashMap::new)
                            .insert(
                                "deployment.kubernetes.io/revision".to_string(),
                                new_rev.clone(),
                            );
                        match self.storage.update(&dep_key, &dep).await {
                            Ok(_) => break,
                            Err(e) => {
                                debug!("CAS retry updating deployment revision: {}", e);
                                continue;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        // Set owner reference to the deployment
        metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "Deployment".to_string(),
            name: deployment.metadata.name.clone(),
            uid: deployment.metadata.uid.clone(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);

        // Add pod-template-hash to the pod template labels
        let mut template = deployment.spec.template.clone();
        let template_labels = template
            .metadata
            .get_or_insert_with(|| ObjectMeta::new(""))
            .labels
            .get_or_insert_with(Default::default);
        template_labels.insert("pod-template-hash".to_string(), pod_template_hash.clone());

        // Add pod-template-hash to the selector matchLabels
        let mut selector = deployment.spec.selector.clone();
        let match_labels = selector.match_labels.get_or_insert_with(Default::default);
        match_labels.insert("pod-template-hash".to_string(), pod_template_hash.clone());

        let replicaset = ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata,
            spec: ReplicaSetSpec {
                replicas,
                selector,
                template,
                min_ready_seconds: deployment.spec.min_ready_seconds,
            },
            status: None,
        };

        let key = build_key("replicasets", Some(namespace), &rs_name);
        match self.storage.create(&key, &replicaset).await {
            Ok(_) => {
                info!(
                    "Created ReplicaSet {}/{} with {} replicas for deployment {}",
                    namespace, rs_name, replicas, deployment.metadata.name
                );
            }
            Err(e) => {
                let err_str = format!("{}", e);
                if err_str.contains("already exists") || err_str.contains("AlreadyExists") {
                    debug!(
                        "ReplicaSet {}/{} already exists, skipping creation",
                        namespace, rs_name
                    );
                } else {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Set the desired-replicas and max-replicas annotations on a ReplicaSet.
    /// K8s ref: pkg/controller/deployment/util/deployment_util.go — SetReplicasAnnotations
    /// desired-replicas: the deployment's spec.replicas
    /// max-replicas: desired + maxSurge (the maximum total pods allowed during rollout)
    async fn set_desired_replicas_annotation(
        &self,
        rs: &ReplicaSet,
        desired: i32,
        max_surge: i32,
        namespace: &str,
    ) {
        let key = build_key("replicasets", Some(namespace), &rs.metadata.name);
        if let Ok(mut fresh_rs) = self.storage.get::<ReplicaSet>(&key).await {
            let annotations = fresh_rs
                .metadata
                .annotations
                .get_or_insert_with(std::collections::HashMap::new);
            let desired_str = desired.to_string();
            // K8s: max-replicas = desired + maxSurge (from deployment strategy)
            // K8s ref: pkg/controller/deployment/util/deployment_util.go — SetReplicasAnnotations
            let max_replicas_str = (desired + max_surge).to_string();
            let mut changed = false;
            if annotations.get("deployment.kubernetes.io/desired-replicas") != Some(&desired_str) {
                annotations.insert(
                    "deployment.kubernetes.io/desired-replicas".to_string(),
                    desired_str,
                );
                changed = true;
            }
            if annotations.get("deployment.kubernetes.io/max-replicas") != Some(&max_replicas_str) {
                annotations.insert(
                    "deployment.kubernetes.io/max-replicas".to_string(),
                    max_replicas_str,
                );
                changed = true;
            }
            if changed {
                let _ = self.storage.update(&key, &fresh_rs).await;
            }
        }
    }

    async fn update_replicaset_replicas(
        &self,
        rs: &ReplicaSet,
        replicas: i32,
    ) -> rusternetes_common::Result<()> {
        let namespace = rs.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key("replicasets", Some(namespace), &rs.metadata.name);

        // Re-read from storage for fresh resourceVersion to avoid CAS conflicts
        let mut updated_rs: ReplicaSet = match self.storage.get(&key).await {
            Ok(fresh) => fresh,
            Err(_) => rs.clone(),
        };

        if updated_rs.spec.replicas == replicas {
            return Ok(()); // Already at desired count
        }

        updated_rs.spec.replicas = replicas;
        self.storage.update(&key, &updated_rs).await?;

        info!(
            "Updated ReplicaSet {}/{} replicas to {}",
            namespace, rs.metadata.name, replicas
        );

        Ok(())
    }

    /// Count pods that are Ready for a given ReplicaSet
    /// reconcileOldReplicaSets (pkg/controller/deployment/rolling.go) — the
    /// single, correct old-ReplicaSet scale-down used by every rolling-update
    /// path. Two phases:
    ///
    /// 1. cleanupUnhealthyReplicas — remove only UNAVAILABLE replicas
    ///    (spec - available) from each old RS, bounded by maxScaledDown.
    /// 2. scaleDownOldReplicaSetsForRollingUpdate — remove HEALTHY old pods,
    ///    but only down to minAvailable AVAILABLE pods across the deployment.
    ///
    /// Availability is computed from pods (lag-free, minReadySeconds-aware) so a
    /// still-available old RS is never drained while the new RS is inside its
    /// minReadySeconds window. `new_active_replicas` is the new RS's spec count
    /// after this round's scale-up.
    async fn reconcile_old_replicasets(
        &self,
        owned_replicasets: &[ReplicaSet],
        active_name: &str,
        new_active_replicas: i32,
        desired_replicas: i32,
        max_unavailable: i32,
        namespace: &str,
    ) -> rusternetes_common::Result<()> {
        let min_available = (desired_replicas - max_unavailable).max(0);
        let old_rs_list: Vec<ReplicaSet> = owned_replicasets
            .iter()
            .filter(|rs| rs.metadata.name != active_name)
            .cloned()
            .collect();
        if old_rs_list.iter().all(|rs| rs.spec.replicas == 0) {
            return Ok(());
        }

        let new_rs_available = if let Some(new_rs) = owned_replicasets
            .iter()
            .find(|rs| rs.metadata.name == active_name)
        {
            self.count_available_pods_min_ready(new_rs, namespace).await
        } else {
            0
        };
        let new_rs_unavailable = (new_active_replicas - new_rs_available).max(0);
        let all_pods_count =
            new_active_replicas + old_rs_list.iter().map(|rs| rs.spec.replicas).sum::<i32>();
        let max_scaled_down = (all_pods_count - min_available - new_rs_unavailable).max(0);
        if max_scaled_down <= 0 {
            return Ok(());
        }

        let mut cur_replicas: std::collections::HashMap<String, i32> =
            std::collections::HashMap::new();
        let mut old_available: std::collections::HashMap<String, i32> =
            std::collections::HashMap::new();
        for rs in &old_rs_list {
            cur_replicas.insert(rs.metadata.name.clone(), rs.spec.replicas);
            let avail = self.count_available_pods_min_ready(rs, namespace).await;
            old_available.insert(rs.metadata.name.clone(), avail);
        }
        let mut ordered: Vec<&ReplicaSet> = old_rs_list.iter().collect();
        ordered.sort_by(|a, b| {
            a.metadata
                .creation_timestamp
                .cmp(&b.metadata.creation_timestamp)
        });

        // Phase 1 — cleanup unhealthy (unavailable) replicas only.
        let mut cleanup_done = 0;
        for rs in &ordered {
            if cleanup_done >= max_scaled_down {
                break;
            }
            let name = &rs.metadata.name;
            let cur = cur_replicas[name];
            if cur == 0 {
                continue;
            }
            let unhealthy = (cur - old_available[name]).max(0);
            let scale = unhealthy.min(max_scaled_down - cleanup_done);
            if scale <= 0 {
                continue;
            }
            cur_replicas.insert(name.clone(), cur - scale);
            cleanup_done += scale;
        }

        // Phase 2 — scale down healthy old pods, never below minAvailable
        // AVAILABLE pods. When the new RS is not yet available this budget is 0.
        let total_available = new_rs_available + old_available.values().sum::<i32>();
        let healthy_budget = (total_available - min_available).max(0);
        let mut healthy_done = 0;
        for rs in &ordered {
            if healthy_done >= healthy_budget {
                break;
            }
            let name = &rs.metadata.name;
            let cur = cur_replicas[name];
            if cur == 0 {
                continue;
            }
            let scale = cur.min(healthy_budget - healthy_done);
            if scale <= 0 {
                continue;
            }
            cur_replicas.insert(name.clone(), cur - scale);
            healthy_done += scale;
        }

        for rs in &old_rs_list {
            let target = cur_replicas[&rs.metadata.name];
            if target != rs.spec.replicas {
                info!(
                    "Rolling update: scaling down old ReplicaSet {}/{} from {} to {} (available={})",
                    namespace, rs.metadata.name, rs.spec.replicas, target,
                    old_available.get(&rs.metadata.name).copied().unwrap_or(0)
                );
                self.update_replicaset_replicas(rs, target).await?;
            }
        }
        Ok(())
    }

    /// Count pods owned by `rs` that are *available*: Ready=True for at least
    /// the RS's own `minReadySeconds`. Computed directly from pods (not from
    /// `rs.status.availableReplicas`) so it never lags behind the ReplicaSet
    /// controller's status writes — a lag that previously caused the deployment
    /// controller to drain a still-available old ReplicaSet during a rollover.
    async fn count_available_pods_min_ready(&self, rs: &ReplicaSet, namespace: &str) -> i32 {
        let min_ready = rs.spec.min_ready_seconds.unwrap_or(0).max(0) as i64;
        let now = chrono::Utc::now();
        let pod_prefix = build_prefix("pods", Some(namespace));
        let pods: Vec<Pod> = self.storage.list(&pod_prefix).await.unwrap_or_default();
        let owned: Vec<&Pod> = pods
            .iter()
            .filter(|pod| {
                pod.metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| r.name == rs.metadata.name))
                    .unwrap_or(false)
            })
            .collect();
        // No Pod objects exist for this RS (the RS controller hasn't created
        // them yet, or — in unit/integration tests — only RS status is
        // maintained). Fall back to the RS's reported availableReplicas so
        // availability isn't under-counted to 0, which would stall the rolling
        // update. When pods DO exist we count them directly (lag-free,
        // minReadySeconds-aware) — that is what fixes the rollover scale-down
        // (#310) where rs.status lagged behind reality.
        if owned.is_empty() {
            return rs
                .status
                .as_ref()
                .map(|s| s.available_replicas)
                .unwrap_or(0);
        }
        owned
            .iter()
            .filter(|pod| {
                let ready_cond = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .and_then(|c| {
                        c.iter()
                            .find(|cond| cond.condition_type == "Ready" && cond.status == "True")
                    });
                match ready_cond {
                    Some(_) if min_ready == 0 => true,
                    Some(cond) => match cond.last_transition_time {
                        Some(t) => (now - t).num_seconds() >= min_ready,
                        None => true,
                    },
                    None => false,
                }
            })
            .count() as i32
    }

    async fn count_available_pods_for_rs(&self, rs_name: &str, namespace: &str) -> i32 {
        let pod_prefix = build_prefix("pods", Some(namespace));
        let pods: Vec<Pod> = self.storage.list(&pod_prefix).await.unwrap_or_default();
        pods.iter()
            .filter(|pod| {
                // Pod must be owned by this RS
                let owned = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| r.name == rs_name))
                    .unwrap_or(false);
                if !owned {
                    return false;
                }
                // Pod must be Ready
                pod.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|c| {
                        c.iter()
                            .any(|cond| cond.condition_type == "Ready" && cond.status == "True")
                    })
                    .unwrap_or(false)
            })
            .count() as i32
    }

    async fn update_deployment_status(
        &self,
        deployment: &Deployment,
    ) -> rusternetes_common::Result<()> {
        let namespace = deployment
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        // Get all ReplicaSets owned by this deployment
        let rs_prefix = build_prefix("replicasets", Some(namespace));
        let all_replicasets: Vec<ReplicaSet> = self.storage.list(&rs_prefix).await?;

        let owned_replicasets: Vec<ReplicaSet> = all_replicasets
            .into_iter()
            .filter(|rs| self.is_owned_by_deployment(rs, deployment))
            .collect();

        // Aggregate status from all ReplicaSets.
        // If a ReplicaSet has no status yet (controller hasn't run), fall back to
        // counting its pods directly so the deployment status is never stuck at 0.
        let mut total_replicas = 0;
        let mut ready_replicas = 0;
        let mut available_replicas = 0;
        let mut updated_replicas = 0;

        for rs in &owned_replicasets {
            // Always count pods directly for the most accurate status.
            // RS status may be stale if the RS controller hasn't run recently.
            let (pod_total, pod_ready, pod_available) = self.count_pods_for_replicaset(rs).await;

            // Use the higher of RS status or direct pod count
            if let Some(status) = &rs.status {
                total_replicas += std::cmp::max(status.replicas, pod_total);
                ready_replicas += std::cmp::max(status.ready_replicas, pod_ready);
                available_replicas += std::cmp::max(status.available_replicas, pod_available);
            } else {
                total_replicas += pod_total;
                ready_replicas += pod_ready;
                available_replicas += pod_available;
            }

            if self.replicaset_matches_template(rs, deployment) {
                if let Some(status) = &rs.status {
                    updated_replicas += std::cmp::max(status.replicas, pod_total);
                } else {
                    updated_replicas += pod_total;
                }
            }
        }

        let desired_replicas = deployment.spec.replicas.unwrap_or(1);

        let unavailable = if total_replicas > available_replicas {
            total_replicas - available_replicas
        } else {
            0
        };

        // Build status conditions — merge with existing, preserving unknown types
        let mut conditions = deployment
            .status
            .as_ref()
            .and_then(|s| s.conditions.clone())
            .unwrap_or_default();

        // Remove only the types we manage, then add our computed ones
        conditions.retain(|c| {
            c.condition_type != "Available"
                && c.condition_type != "Progressing"
                && c.condition_type != "ReplicaFailure"
        });

        // Available condition
        if available_replicas >= desired_replicas {
            conditions.push(DeploymentCondition {
                condition_type: "Available".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(Utc::now()),
                last_update_time: Some(Utc::now()),
                reason: Some("MinimumReplicasAvailable".to_string()),
                message: Some("Deployment has minimum availability.".to_string()),
            });
        } else {
            conditions.push(DeploymentCondition {
                condition_type: "Available".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(Utc::now()),
                last_update_time: Some(Utc::now()),
                reason: Some("MinimumReplicasUnavailable".to_string()),
                message: Some(format!(
                    "Deployment does not have minimum availability. {} of {} available.",
                    available_replicas, desired_replicas
                )),
            });
        }

        // Progressing condition — check progressDeadlineSeconds
        let progress_deadline = deployment.spec.progress_deadline_seconds.unwrap_or(600);
        let deadline_exceeded = if available_replicas < desired_replicas {
            // Check if the deployment has been progressing long enough to exceed the deadline
            deployment
                .metadata
                .creation_timestamp
                .map(|ct| {
                    let elapsed = Utc::now().signed_duration_since(ct).num_seconds();
                    elapsed > progress_deadline as i64
                })
                .unwrap_or(false)
        } else {
            false
        };

        if deadline_exceeded {
            conditions.push(DeploymentCondition {
                condition_type: "Progressing".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(Utc::now()),
                last_update_time: Some(Utc::now()),
                reason: Some("ProgressDeadlineExceeded".to_string()),
                message: Some(format!(
                    "ReplicaSet \"{}\" has timed out progressing.",
                    owned_replicasets
                        .first()
                        .map(|rs| rs.metadata.name.as_str())
                        .unwrap_or("unknown")
                )),
            });
        } else if updated_replicas == desired_replicas && available_replicas >= desired_replicas {
            conditions.push(DeploymentCondition {
                condition_type: "Progressing".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(Utc::now()),
                last_update_time: Some(Utc::now()),
                reason: Some("NewReplicaSetAvailable".to_string()),
                message: Some(format!(
                    "ReplicaSet \"{}\" has successfully progressed.",
                    owned_replicasets
                        .first()
                        .map(|rs| rs.metadata.name.as_str())
                        .unwrap_or("unknown")
                )),
            });
        } else {
            conditions.push(DeploymentCondition {
                condition_type: "Progressing".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(Utc::now()),
                last_update_time: Some(Utc::now()),
                reason: Some("ReplicaSetUpdated".to_string()),
                message: Some(format!(
                    "ReplicaSet \"{}\" is progressing.",
                    owned_replicasets
                        .first()
                        .map(|rs| rs.metadata.name.as_str())
                        .unwrap_or("unknown")
                )),
            });
        }

        let new_status = DeploymentStatus {
            replicas: Some(total_replicas),
            ready_replicas: Some(ready_replicas),
            available_replicas: Some(available_replicas),
            unavailable_replicas: Some(unavailable),
            updated_replicas: Some(updated_replicas),
            conditions: Some(conditions),
            collision_count: None,
            observed_generation: deployment.metadata.generation,
            terminating_replicas: None,
        };

        // Check if the revision annotation needs updating
        let max_revision = owned_replicasets
            .iter()
            .filter_map(|rs| {
                rs.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                    .and_then(|v| v.parse::<i64>().ok())
            })
            .max();

        // Compare old status (ignoring condition timestamps) with new status counts.
        // For deployments, we compare the numeric fields to decide if an update is needed,
        // since condition timestamps always change.
        let old_status = &deployment.status;
        let status_changed = match old_status {
            Some(old) => {
                old.replicas != new_status.replicas
                    || old.ready_replicas != new_status.ready_replicas
                    || old.available_replicas != new_status.available_replicas
                    || old.unavailable_replicas != new_status.unavailable_replicas
                    || old.updated_replicas != new_status.updated_replicas
                    || old.observed_generation != new_status.observed_generation
                    || old.conditions.as_ref().map(|c| {
                        c.iter()
                            .map(|cond| (&cond.condition_type, &cond.status, &cond.reason))
                            .collect::<Vec<_>>()
                    }) != new_status.conditions.as_ref().map(|c| {
                        c.iter()
                            .map(|cond| (&cond.condition_type, &cond.status, &cond.reason))
                            .collect::<Vec<_>>()
                    })
            }
            None => true,
        };

        let revision_changed = max_revision.is_some_and(|rev| {
            let current = deployment
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                .and_then(|v| v.parse::<i64>().ok());
            current != Some(rev)
        });

        // Only write if status or revision annotation actually changed
        if status_changed || revision_changed {
            let key = build_key("deployments", Some(namespace), &deployment.metadata.name);
            // Re-read from storage for fresh resourceVersion to avoid CAS conflicts
            // with concurrent test PATCH operations
            let mut updated_deployment: Deployment = match self.storage.get(&key).await {
                Ok(d) => d,
                Err(_) => deployment.clone(),
            };
            updated_deployment.status = Some(new_status);

            if let Some(rev) = max_revision {
                updated_deployment
                    .metadata
                    .annotations
                    .get_or_insert_with(std::collections::HashMap::new)
                    .insert(
                        "deployment.kubernetes.io/revision".to_string(),
                        rev.to_string(),
                    );
            }

            self.storage.update(&key, &updated_deployment).await?;
        }

        debug!(
            "Updated status for deployment {}/{}: total={}, ready={}, available={}, updated={}",
            namespace,
            deployment.metadata.name,
            total_replicas,
            ready_replicas,
            available_replicas,
            updated_replicas
        );

        Ok(())
    }

    /// Count pods owned by a ReplicaSet directly from storage.
    /// Returns (total, ready, available).
    /// Used as a fallback when the RS has no status yet.
    async fn count_pods_for_replicaset(&self, rs: &ReplicaSet) -> (i32, i32, i32) {
        let namespace = rs.metadata.namespace.as_deref().unwrap_or("default");
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await.unwrap_or_default();

        let mut total = 0i32;
        let mut ready = 0i32;
        let mut available = 0i32;

        for pod in &all_pods {
            // Skip terminated or deleting pods
            if pod.metadata.deletion_timestamp.is_some() {
                continue;
            }
            if let Some(ref status) = pod.status {
                if let Some(ref phase) = status.phase {
                    if matches!(
                        phase,
                        rusternetes_common::types::Phase::Failed
                            | rusternetes_common::types::Phase::Succeeded
                    ) {
                        continue;
                    }
                }
            }

            // Check if pod is owned by this RS
            let owned = pod
                .metadata
                .owner_references
                .as_ref()
                .map(|refs| {
                    refs.iter()
                        .any(|r| r.kind == "ReplicaSet" && r.name == rs.metadata.name)
                })
                .unwrap_or(false);

            if !owned {
                // Fallback: check label selector match
                let labels_match =
                    if let Some(match_labels) = rs.spec.selector.match_labels.as_ref() {
                        if let Some(pod_labels) = &pod.metadata.labels {
                            match_labels
                                .iter()
                                .all(|(k, v)| pod_labels.get(k) == Some(v))
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                if !labels_match {
                    continue;
                }
            }

            total += 1;

            // Check readiness
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

            if is_ready {
                ready += 1;
                // Check availability (minReadySeconds)
                let min_ready = rs.spec.min_ready_seconds.unwrap_or(0);
                if min_ready > 0 {
                    if let Some(creation) = pod.metadata.creation_timestamp {
                        let elapsed = chrono::Utc::now().signed_duration_since(creation);
                        if elapsed.num_seconds() >= min_ready as i64 {
                            available += 1;
                        }
                    }
                } else {
                    available += 1;
                }
            }
        }

        (total, ready, available)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_int_or_percent_percentage() {
        assert_eq!(parse_int_or_percent("25%", 10), 3); // ceil(2.5) = 3
        assert_eq!(parse_int_or_percent("50%", 10), 5);
        assert_eq!(parse_int_or_percent("100%", 10), 10);
        assert_eq!(parse_int_or_percent("25%", 4), 1); // ceil(1.0) = 1
        assert_eq!(parse_int_or_percent("25%", 1), 1); // ceil(0.25) = 1
    }

    #[test]
    fn test_parse_int_or_percent_absolute() {
        assert_eq!(parse_int_or_percent("1", 10), 1);
        assert_eq!(parse_int_or_percent("3", 10), 3);
        assert_eq!(parse_int_or_percent("0", 10), 0);
    }

    #[test]
    fn test_parse_int_or_percent_invalid() {
        // Invalid strings default to 1
        assert_eq!(parse_int_or_percent("abc", 10), 1);
        assert_eq!(parse_int_or_percent("", 10), 1);
    }

    #[test]
    fn test_parse_int_or_percent_invalid_percentage() {
        // Invalid percentage defaults to 25%
        assert_eq!(parse_int_or_percent("abc%", 10), 3); // ceil(25% of 10) = 3
    }

    #[test]
    fn test_compute_rolling_update_counts_defaults() {
        let (surge, unavailable) = compute_rolling_update_counts(10, "25%", "25%");
        assert_eq!(surge, 3); // ceil(2.5) = 3
        assert_eq!(unavailable, 3);
    }

    #[test]
    fn test_compute_rolling_update_counts_absolute() {
        let (surge, unavailable) = compute_rolling_update_counts(10, "2", "1");
        assert_eq!(surge, 2);
        assert_eq!(unavailable, 1);
    }

    #[test]
    fn test_compute_rolling_update_counts_mixed() {
        let (surge, unavailable) = compute_rolling_update_counts(10, "30%", "1");
        assert_eq!(surge, 3); // ceil(3.0) = 3
        assert_eq!(unavailable, 1);
    }

    #[test]
    fn test_compute_rolling_update_counts_small_deployment() {
        // For a deployment with 1 replica, 25% rounds up to 1
        let (surge, unavailable) = compute_rolling_update_counts(1, "25%", "25%");
        assert_eq!(surge, 1);
        assert_eq!(unavailable, 1);
    }

    /// Helper to create a minimal Container for tests
    fn test_container(name: &str, image: &str) -> rusternetes_common::resources::Container {
        rusternetes_common::resources::Container {
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

    #[tokio::test]
    async fn test_deployment_status_aggregates_from_replicaset_status() {
        use rusternetes_common::resources::{PodTemplateSpec, ReplicaSetStatus};
        use rusternetes_common::types::LabelSelector;
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());

        // Create a deployment
        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-deploy").with_namespace("default"),
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(3),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("test", "nginx:latest")],
                        ..Default::default()
                    },
                },
                strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
                paused: None,
                progress_deadline_seconds: None,
            },
            status: None,
        };

        let dep_key = build_key("deployments", Some("default"), "test-deploy");
        storage.create(&dep_key, &deployment).await.unwrap();

        // Create an owned ReplicaSet with populated status
        let mut rs_labels = labels.clone();
        rs_labels.insert("pod-template-hash".to_string(), "abc123".to_string());

        let rs = ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-deploy-abc123".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(rs_labels.clone()),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    name: "test-deploy".to_string(),
                    uid: deployment.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas: 3,
                selector: LabelSelector {
                    match_labels: Some(rs_labels.clone()),
                    match_expressions: None,
                },
                template: deployment.spec.template.clone(),
                min_ready_seconds: None,
            },
            status: Some(ReplicaSetStatus {
                replicas: 3,
                ready_replicas: 3,
                available_replicas: 3,
                fully_labeled_replicas: Some(3),
                observed_generation: None,
                conditions: None,
                terminating_replicas: None,
            }),
        };

        let rs_key = build_key("replicasets", Some("default"), "test-deploy-abc123");
        storage.create(&rs_key, &rs).await.unwrap();

        // Run status update
        controller
            .update_deployment_status(&deployment)
            .await
            .unwrap();

        // Read back the deployment and verify status
        let updated: Deployment = storage.get(&dep_key).await.unwrap();
        let status = updated.status.unwrap();
        assert_eq!(status.replicas, Some(3));
        assert_eq!(status.ready_replicas, Some(3));
        assert_eq!(status.available_replicas, Some(3));
        assert_eq!(status.updated_replicas, Some(3));

        // Verify Available condition is True
        let conditions = status.conditions.unwrap();
        let available_cond = conditions
            .iter()
            .find(|c| c.condition_type == "Available")
            .unwrap();
        assert_eq!(available_cond.status, "True");
    }

    #[tokio::test]
    async fn test_deployment_status_fallback_to_pod_count_when_rs_has_no_status() {
        use rusternetes_common::resources::{PodCondition, PodStatus, PodTemplateSpec};
        use rusternetes_common::types::{LabelSelector, Phase};
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());

        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-deploy").with_namespace("default"),
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(2),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("test", "nginx:latest")],
                        ..Default::default()
                    },
                },
                strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
                paused: None,
                progress_deadline_seconds: None,
            },
            status: None,
        };

        let dep_key = build_key("deployments", Some("default"), "test-deploy");
        storage.create(&dep_key, &deployment).await.unwrap();

        // Create an RS without status (simulates RS controller not having run yet)
        let mut rs_labels = labels.clone();
        rs_labels.insert("pod-template-hash".to_string(), "abc123".to_string());

        let rs = ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-deploy-abc123".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(rs_labels.clone()),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    name: "test-deploy".to_string(),
                    uid: deployment.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas: 2,
                selector: LabelSelector {
                    match_labels: Some(rs_labels.clone()),
                    match_expressions: None,
                },
                template: deployment.spec.template.clone(),
                min_ready_seconds: None,
            },
            status: None, // No status yet!
        };

        let rs_key = build_key("replicasets", Some("default"), "test-deploy-abc123");
        storage.create(&rs_key, &rs).await.unwrap();

        // Create 2 ready pods owned by the RS
        for i in 0..2 {
            let pod_name = format!("test-deploy-abc123-pod-{}", i);
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: ObjectMeta {
                    name: pod_name.clone(),
                    namespace: Some("default".to_string()),
                    labels: Some(rs_labels.clone()),
                    owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                        api_version: "apps/v1".to_string(),
                        kind: "ReplicaSet".to_string(),
                        name: "test-deploy-abc123".to_string(),
                        uid: rs.metadata.uid.clone(),
                        controller: Some(true),
                        block_owner_deletion: Some(true),
                    }]),
                    creation_timestamp: Some(chrono::Utc::now() - chrono::Duration::minutes(5)),
                    ..Default::default()
                },
                spec: None,
                status: Some(PodStatus {
                    phase: Some(Phase::Running),
                    conditions: Some(vec![PodCondition {
                        condition_type: "Ready".to_string(),
                        status: "True".to_string(),
                        last_probe_time: None,
                        last_transition_time: None,
                        reason: None,
                        message: None,
                        observed_generation: None,
                    }]),
                    ..Default::default()
                }),
            };
            let pod_key = build_key("pods", Some("default"), &pod_name);
            storage.create(&pod_key, &pod).await.unwrap();
        }

        // Run status update — should pick up pod counts via fallback
        controller
            .update_deployment_status(&deployment)
            .await
            .unwrap();

        let updated: Deployment = storage.get(&dep_key).await.unwrap();
        let status = updated.status.unwrap();

        // Should have counted the pods directly
        assert_eq!(status.replicas, Some(2));
        assert_eq!(status.ready_replicas, Some(2));
        assert_eq!(status.available_replicas, Some(2));
    }

    #[tokio::test]
    async fn test_recreate_deployment_waits_for_old_pods() {
        use rusternetes_common::resources::PodTemplateSpec;
        use rusternetes_common::types::LabelSelector;
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "recreate-test".to_string());

        // Create a Recreate deployment with 1 replica
        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("recreate-deploy").with_namespace("default"),
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(1),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("main", "nginx:1.0")],
                        ..Default::default()
                    },
                },
                strategy: Some(
                    rusternetes_common::resources::deployment::DeploymentStrategy {
                        strategy_type: "Recreate".to_string(),
                        rolling_update: None,
                    },
                ),
                min_ready_seconds: None,
                revision_history_limit: None,
                paused: None,
                progress_deadline_seconds: None,
            },
            status: None,
        };

        let dep_key = build_key("deployments", Some("default"), "recreate-deploy");
        storage.create(&dep_key, &deployment).await.unwrap();

        // First reconcile — creates initial RS and pods
        controller.reconcile_all().await.unwrap();

        // Should have created a ReplicaSet
        let rs_prefix = build_prefix("replicasets", Some("default"));
        let replicasets: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
        assert!(
            !replicasets.is_empty(),
            "Should have created at least one ReplicaSet"
        );

        // Now simulate an image update (template change) by updating the deployment
        let mut dep: Deployment = storage.get(&dep_key).await.unwrap();
        dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
        dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
        storage.update(&dep_key, &dep).await.unwrap();

        // Create a pod owned by the old RS that is still "running" (not terminated)
        let old_rs = &replicasets[0];
        let old_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "old-pod-0".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(labels.clone()),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "ReplicaSet".to_string(),
                    name: old_rs.metadata.name.clone(),
                    uid: old_rs.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![test_container("main", "nginx:1.0")],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "old-pod-0"), &old_pod)
            .await
            .unwrap();

        // Reconcile — should scale down old RS but NOT create new RS yet
        // because old pod is still running
        controller.reconcile_all().await.unwrap();

        // Count ReplicaSets — should still be 1 (old one), no new RS created
        let replicasets_after: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
        let new_rs_count = replicasets_after
            .iter()
            .filter(|rs| {
                rs.spec
                    .template
                    .spec
                    .containers
                    .first()
                    .map(|c| c.image == "nginx:2.0")
                    .unwrap_or(false)
            })
            .count();

        // Old pod is still running, so new RS should NOT be created
        assert_eq!(
            new_rs_count, 0,
            "Recreate should not create new RS while old pods are still running"
        );
    }

    /// Deployment revision annotation should be computed from existing ReplicaSets,
    /// not hardcoded to "1". When a deployment owns pre-existing ReplicaSets,
    /// its revision should be the max revision from those ReplicaSets.
    #[tokio::test]
    async fn test_deployment_revision_from_existing_replicasets() {
        use rusternetes_storage::MemoryStorage;
        use std::collections::HashMap;
        use std::sync::Arc;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 2);
        let ns = "default";

        let mut rs_labels = HashMap::new();
        rs_labels.insert("app".to_string(), "web".to_string());
        let mut rs_annotations = HashMap::new();
        rs_annotations.insert(
            "deployment.kubernetes.io/revision".to_string(),
            "5".to_string(),
        );

        // Create a pre-existing ReplicaSet with revision 5 as raw JSON
        let rs_json = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "web-rs-1",
                "namespace": ns,
                "uid": "rs-uid-1",
                "annotations": { "deployment.kubernetes.io/revision": "5" },
                "labels": { "app": "web" },
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "web",
                    "uid": "deploy-uid-1",
                    "controller": true
                }]
            },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": { "app": "web" } },
                "template": {
                    "metadata": { "labels": { "app": "web" } },
                    "spec": { "containers": [{ "name": "nginx", "image": "nginx:1.19" }] }
                }
            }
        });
        let rs: ReplicaSet = serde_json::from_value(rs_json).unwrap();
        let rs_key = format!("/registry/replicasets/{}/web-rs-1", ns);
        storage.create(&rs_key, &rs).await.unwrap();

        // Create a Deployment (no revision annotation yet) as raw JSON
        let deploy_json = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "web",
                "namespace": ns,
                "uid": "deploy-uid-1"
            },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": { "app": "web" } },
                "template": {
                    "metadata": { "labels": { "app": "web" } },
                    "spec": { "containers": [{ "name": "nginx", "image": "nginx:1.19" }] }
                }
            }
        });
        let deployment: Deployment = serde_json::from_value(deploy_json).unwrap();
        let deploy_key = format!("/registry/deployments/{}/web", ns);
        storage.create(&deploy_key, &deployment).await.unwrap();

        // Reconcile
        let d: Deployment = storage.get(&deploy_key).await.unwrap();
        controller.reconcile_deployment(&d).await.unwrap();

        // The deployment's revision should be based on the pre-existing ReplicaSet (5),
        // NOT hardcoded to "1"
        let d: Deployment = storage.get(&deploy_key).await.unwrap();
        let revision = d
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("deployment.kubernetes.io/revision"))
            .expect("deployment should have revision annotation");
        assert!(
            revision.parse::<i64>().unwrap() >= 5,
            "deployment revision ({}) should be >= 5 (max from owned ReplicaSets)",
            revision
        );
    }
}
