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
                if let Err(e) = self.storage.update(&rs_key, &adopted).await {
                    if !matches!(e, rusternetes_common::Error::Conflict(_)) {
                        error!(
                            error = %e,
                            replicaset = rs.metadata.name,
                            "failed to adopt orphan ReplicaSet"
                        );
                    }
                }
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
                        if let Err(e) = self.storage.update(&rs_key, &updated_rs).await {
                            if !matches!(e, rusternetes_common::Error::Conflict(_)) {
                                error!(
                                    error = %e,
                                    replicaset = rs.metadata.name,
                                    "failed to refresh ReplicaSet revision annotation"
                                );
                            }
                        }
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
                if let Err(e) = self.storage.update(&key, &updated).await {
                    if !matches!(e, rusternetes_common::Error::Conflict(_)) {
                        error!(
                            error = %e,
                            deployment = deployment.metadata.name,
                            "failed to refresh deployment revision annotation"
                        );
                    }
                }
            }
        }

        debug!(
            "Found {} ReplicaSets owned by deployment {}/{}",
            owned_replicasets.len(),
            namespace,
            deployment.metadata.name
        );

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

        // K8s ref: pkg/controller/deployment/deployment_controller.go —
        //   syncDeployment: when d.Spec.Paused, dc.sync(d, rsList) runs but
        //   rolloutRolling does NOT. scale() can still adjust replica counts
        //   (e.g. apply scale-event surges), but the rolling-update
        //   progression that scales the old RS down is skipped.
        let is_paused = deployment.spec.paused.unwrap_or(false);

        // Detect a scaling event by comparing deployment.spec.replicas with the
        // "deployment.kubernetes.io/desired-replicas" annotation on any active RS.
        // K8s ref: pkg/controller/deployment/sync.go — isScalingEvent()
        //
        // Unlike a "rollout in progress" check, this examines every active RS
        // regardless of how many old RSes have replicas — the same way K8s
        // upstream does, so that proportional scaling kicks in even when only
        // the new RS is currently active.
        let is_scaling_event = owned_replicasets
            .iter()
            .filter(|rs| rs.spec.replicas > 0)
            .any(|rs| {
                rs.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("deployment.kubernetes.io/desired-replicas"))
                    .and_then(|v| v.parse::<i32>().ok())
                    .is_some_and(|annotated_desired| annotated_desired != desired_replicas)
            });

        // K8s sync() / scale() — runs when the deployment is paused or when a
        // scaling event is detected. Distributes replicas proportionally across
        // active RSes (or scales a single active RS directly).
        // K8s ref: pkg/controller/deployment/sync.go — sync(), scale()
        if is_paused || is_scaling_event {
            self.scale_replica_sets(
                &owned_replicasets,
                active_rs,
                desired_replicas,
                max_surge,
                is_rolling_update,
                namespace,
            )
            .await?;

            // When paused, do NOT progress the rollout — return after status
            // and cleanup. When it's only a scaling event (not paused), we
            // also return to let the next reconcile pick up the now-updated
            // RSes; this matches upstream which returns from sync() before
            // attempting rollout progression.
            return self.update_deployment_status(deployment).await;
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

            // Saturated new RS but old RSes still have pods — scale them down.
            // K8s ref: sync.go:320-327 — IsSaturated check. Reachable only
            // when not paused and not a scaling event (those return early
            // above), so no extra guard needed.
            if active_replicas >= desired_replicas && old_rs_total > 0 {
                // New RS is saturated — scale all old RSes to 0
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
                let min_available = (desired_replicas - max_unavailable).max(0);

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

                // K8s reconcileOldReplicaSets (rolling.go:86-132):
                // maxScaledDown = allPodsCount - minAvailable - newRSUnavailablePodCount
                // Uses RS status.AvailableReplicas for availability counts.
                // This prevents over-aggressive scale-down when new pods aren't ready.
                //
                // Count available replicas from all RSes (status-based, matching K8s).
                // Fall back to counting pods directly if RS status is not yet populated.
                let mut all_available: i32 = 0;
                for rs in &owned_replicasets {
                    all_available += if let Some(status) = &rs.status {
                        status.available_replicas
                    } else {
                        // Fall back to pod count
                        self.count_available_pods_for_rs(&rs.metadata.name, namespace)
                            .await
                    };
                }
                let _all_available = all_available;

                // New RS unavailable count = newRS.Spec.Replicas - newRS.Status.AvailableReplicas
                let new_rs_available = if let Some(new_rs) = owned_replicasets
                    .iter()
                    .find(|rs| rs.metadata.name == active_name)
                {
                    if let Some(status) = &new_rs.status {
                        status.available_replicas
                    } else {
                        self.count_available_pods_for_rs(&active_name, namespace)
                            .await
                    }
                } else {
                    0
                };
                let new_rs_unavailable = (new_active_replicas - new_rs_available).max(0);

                // allPodsCount uses the updated count after scaling up
                let all_pods_count: i32 = owned_replicasets
                    .iter()
                    .map(|rs| {
                        if rs.metadata.name == active_name {
                            new_active_replicas // Use the just-scaled-up count
                        } else {
                            rs.spec.replicas
                        }
                    })
                    .sum();

                let max_scaled_down = (all_pods_count - min_available - new_rs_unavailable).max(0);
                let scale_down_by = max_scaled_down.min(old_rs_total);

                if scale_down_by > 0 {
                    let mut remaining_to_remove = scale_down_by;
                    for rs in owned_replicasets.iter() {
                        if rs.metadata.name != active_name
                            && rs.spec.replicas > 0
                            && remaining_to_remove > 0
                        {
                            let remove_from_this = rs.spec.replicas.min(remaining_to_remove);
                            let new_replicas = rs.spec.replicas - remove_from_this;
                            info!(
                                "Rolling update: scaling down old ReplicaSet {}/{} from {} to {}",
                                namespace, rs.metadata.name, rs.spec.replicas, new_replicas
                            );
                            self.update_replicaset_replicas(rs, new_replicas).await?;
                            remaining_to_remove -= remove_from_this;
                        }
                    }
                }
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

                // Scale down old ReplicaSets gradually — only remove old pods
                // when the new RS has enough available replicas to maintain
                // the minimum availability guarantee.
                // K8s ref: pkg/controller/deployment/rolling.go — reconcileOldReplicaSets
                let new_rs_available = self
                    .count_available_pods_for_rs(&active_name, namespace)
                    .await;
                let min_available = (desired_replicas - max_unavailable).max(0);
                let mut can_scale_down = (new_rs_available - min_available).max(0);
                // When new RS is fully available, ensure old RSes are scaled to 0
                // even if maxUnavailable rounds to 0 (e.g., 25% of 1 = 0)
                if new_rs_available >= desired_replicas && old_rs_total > 0 && can_scale_down == 0 {
                    can_scale_down = old_rs_total; // Force scale down all old RSes
                }

                if can_scale_down > 0 {
                    let mut remaining = can_scale_down;
                    for rs in owned_replicasets.iter() {
                        if rs.metadata.name != active_name && rs.spec.replicas > 0 && remaining > 0
                        {
                            let remove = rs.spec.replicas.min(remaining);
                            let new_replicas = rs.spec.replicas - remove;
                            info!(
                                "Scaling down old ReplicaSet {}/{} from {} to {} (available={})",
                                namespace,
                                rs.metadata.name,
                                rs.spec.replicas,
                                new_replicas,
                                new_rs_available
                            );
                            self.update_replicaset_replicas(rs, new_replicas).await?;
                            remaining -= remove;
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

                // K8s reconcileOldReplicaSets (rolling.go:86-132):
                // maxScaledDown = allPodsCount - minAvailable - newRSUnavailablePodCount
                // Since new RS was just created, all its pods are unavailable.
                // newRSUnavailablePodCount = initial_replicas (none are available yet)
                let min_available = (desired_replicas - max_unavailable).max(0);
                let all_pods_count = old_rs_total + initial_replicas;
                let new_rs_unavailable = initial_replicas; // Just created, none available yet
                let max_scaled_down = (all_pods_count - min_available - new_rs_unavailable).max(0);
                let scale_down_by = max_scaled_down.min(old_rs_total);

                let mut remaining_to_remove = scale_down_by;
                for rs in owned_replicasets.iter() {
                    if rs.spec.replicas > 0 && remaining_to_remove > 0 {
                        let remove_from_this = rs.spec.replicas.min(remaining_to_remove);
                        let new_replicas = rs.spec.replicas - remove_from_this;
                        info!(
                            "Rolling update: scaling down old ReplicaSet {}/{} from {} to {}",
                            namespace, rs.metadata.name, rs.spec.replicas, new_replicas
                        );
                        self.update_replicaset_replicas(rs, new_replicas).await?;
                        remaining_to_remove -= remove_from_this;
                    }
                }
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

        // Also update the deployment's revision annotation (with CAS retry +
        // exponential backoff + jitter so contended deployments don't thrash).
        {
            use rand::Rng;
            let dep_key = build_key("deployments", Some(namespace), &deployment.metadata.name);
            let new_rev = new_revision.clone();
            let mut delay_ms: u64 = 10;
            for attempt in 0..3 {
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
                            Err(rusternetes_common::Error::Conflict(_)) => {
                                if attempt + 1 < 3 {
                                    let jitter = rand::thread_rng().gen_range(0..delay_ms);
                                    tokio::time::sleep(Duration::from_millis(delay_ms + jitter))
                                        .await;
                                    delay_ms = (delay_ms * 2).min(500);
                                }
                                continue;
                            }
                            Err(e) => {
                                error!(
                                    error = %e,
                                    deployment = deployment.metadata.name,
                                    "failed to update deployment revision annotation"
                                );
                                break;
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

    /// K8s `scale()` (pkg/controller/deployment/sync.go) — invoked when the
    /// deployment is paused or when a scaling event is detected. Mirrors the
    /// upstream three-branch logic:
    ///   1. If a single active-or-latest RS exists, scale it directly to the
    ///      deployment's desired replica count.
    ///   2. If the new RS is "saturated" (IsSaturated), scale every old RS
    ///      with pods down to 0.
    ///   3. Otherwise (multiple active RSes + RollingUpdate), distribute the
    ///      replica delta proportionally using each RS's max-replicas
    ///      annotation as the old denominator (GetProportion).
    #[allow(clippy::too_many_arguments)]
    async fn scale_replica_sets(
        &self,
        owned_replicasets: &[ReplicaSet],
        active_rs: Option<&ReplicaSet>,
        desired_replicas: i32,
        max_surge: i32,
        is_rolling_update: bool,
        namespace: &str,
    ) -> rusternetes_common::Result<()> {
        // FindActiveOrLatest: if exactly one RS has spec.replicas > 0, scale
        // that one directly. If none are active, fall back to the new RS (or
        // newest old RS). If more than one is active, return None and fall
        // through to the saturated / proportional branches.
        let active_list: Vec<&ReplicaSet> = owned_replicasets
            .iter()
            .filter(|rs| rs.spec.replicas > 0)
            .collect();

        let single = match active_list.len() {
            0 => active_rs.or_else(|| owned_replicasets.first()),
            1 => Some(active_list[0]),
            _ => None,
        };

        if let Some(single_rs) = single {
            if single_rs.spec.replicas != desired_replicas {
                info!(
                    "Scale: ReplicaSet {}/{} {} -> {} (single active RS)",
                    namespace, single_rs.metadata.name, single_rs.spec.replicas, desired_replicas
                );
                self.update_replicaset_replicas(single_rs, desired_replicas)
                    .await?;
            }
            self.set_desired_replicas_annotation(single_rs, desired_replicas, max_surge, namespace)
                .await;
            return Ok(());
        }

        // IsSaturated: when the new RS owns all the desired replicas and is
        // fully available, scale every old RS down to 0. K8s checks the
        // desired-replicas annotation + status.available_replicas; we mirror
        // that, falling back to spec.replicas when status isn't populated yet.
        if let Some(new_rs) = active_rs {
            let new_rs_available = new_rs
                .status
                .as_ref()
                .map(|s| s.available_replicas)
                .unwrap_or(new_rs.spec.replicas);
            let new_rs_annotated_desired = new_rs
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("deployment.kubernetes.io/desired-replicas"))
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(0);
            let saturated = new_rs.spec.replicas == desired_replicas
                && new_rs_annotated_desired == desired_replicas
                && new_rs_available == desired_replicas;
            if saturated {
                for rs in owned_replicasets.iter() {
                    if rs.metadata.name != new_rs.metadata.name && rs.spec.replicas > 0 {
                        info!(
                            "Scale (saturated): old RS {}/{} -> 0",
                            namespace, rs.metadata.name
                        );
                        self.update_replicaset_replicas(rs, 0).await?;
                    }
                }
                self.set_desired_replicas_annotation(
                    new_rs,
                    desired_replicas,
                    max_surge,
                    namespace,
                )
                .await;
                return Ok(());
            }
        }

        // Proportional scaling (RollingUpdate only). K8s ref:
        // pkg/controller/deployment/util/deployment_util.go — GetProportion,
        // getReplicaSetFraction.
        if !is_rolling_update {
            return Ok(());
        }

        let all_rs_replicas: i32 = active_list.iter().map(|rs| rs.spec.replicas).sum();
        let allowed_size = if desired_replicas == 0 {
            0
        } else {
            desired_replicas + max_surge
        };
        let replicas_to_add = allowed_size - all_rs_replicas;

        // Sort active RSes by creation timestamp DESC so the newest RS is
        // first — matches K8s, where the new RS receives the leftover when
        // rounding doesn't sum exactly. K8s ref: sync.go scale() loops over
        // (newRS, oldRSs) sorted newest-first.
        let mut active_sorted: Vec<&ReplicaSet> = active_list.clone();
        active_sorted.sort_by(|a, b| {
            b.metadata
                .creation_timestamp
                .cmp(&a.metadata.creation_timestamp)
        });

        let mut added = 0i32;
        let mut updates: Vec<(String, i32)> = Vec::new();

        for rs in active_sorted.iter() {
            // getReplicaSetFraction: when desired == 0, scale RS down to 0.
            // Otherwise newSize = rs.replicas * (deploymentMaxReplicas /
            // annotatedMaxReplicas). Missing/zero annotation → fraction 0
            // (matches upstream, which returns 0 in that case).
            let fraction = if desired_replicas == 0 {
                -rs.spec.replicas
            } else {
                let annotated_max = rs
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("deployment.kubernetes.io/max-replicas"))
                    .and_then(|v| v.parse::<i32>().ok())
                    .unwrap_or(0);
                if annotated_max == 0 {
                    0
                } else {
                    let new_size =
                        (rs.spec.replicas as f64) * (allowed_size as f64) / (annotated_max as f64);
                    new_size.round() as i32 - rs.spec.replicas
                }
            };

            let allowed = replicas_to_add - added;
            let proportion = if replicas_to_add > 0 {
                fraction.min(allowed).max(0)
            } else if replicas_to_add < 0 {
                fraction.max(allowed).min(0)
            } else {
                0
            };

            added += proportion;
            let new_replicas = (rs.spec.replicas + proportion).max(0);
            if new_replicas != rs.spec.replicas {
                updates.push((rs.metadata.name.clone(), new_replicas));
            }
        }

        // Leftover (rounding remainder) goes to the newest active RS.
        // K8s ref: sync.go scale() — leftover is added to the first RS
        // (newest after the newRS+oldRSs reverse-sort).
        if !active_sorted.is_empty() && replicas_to_add != added {
            let leftover = replicas_to_add - added;
            let newest_name = active_sorted[0].metadata.name.clone();
            if let Some(existing) = updates.iter_mut().find(|(n, _)| n == &newest_name) {
                existing.1 = (existing.1 + leftover).max(0);
            } else {
                // Newest RS had no fraction (e.g. fraction was 0); still apply
                // the leftover so totals balance.
                let current = active_sorted[0].spec.replicas;
                let target = (current + leftover).max(0);
                if target != current {
                    updates.push((newest_name, target));
                }
            }
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

        // K8s SetReplicasAnnotations sets desired-replicas + max-replicas on
        // every active RS after scaling so the next reconcile knows the new
        // baseline. We refresh all owned RSes (including those with 0
        // replicas), matching upstream behavior.
        for rs in owned_replicasets.iter() {
            self.set_desired_replicas_annotation(rs, desired_replicas, max_surge, namespace)
                .await;
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
                if let Err(e) = self.storage.update(&key, &fresh_rs).await {
                    if !matches!(e, rusternetes_common::Error::Conflict(_)) {
                        error!(
                            error = %e,
                            replicaset = rs.metadata.name,
                            "failed to refresh ReplicaSet replica annotations"
                        );
                    }
                }
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

    /// Build an owned ReplicaSet that mirrors the deployment's pod template
    /// (so `replicaset_matches_template` returns true when image matches),
    /// plus the pod-template-hash label and proportional-scaling annotations.
    fn build_owned_rs(
        deployment: &Deployment,
        suffix: &str,
        replicas: i32,
        max_replicas_annotation: i32,
        desired_replicas_annotation: i32,
        image: &str,
    ) -> ReplicaSet {
        use rusternetes_common::types::LabelSelector;
        use std::collections::HashMap;

        let mut labels = deployment
            .spec
            .selector
            .match_labels
            .clone()
            .unwrap_or_default();
        labels.insert("pod-template-hash".to_string(), suffix.to_string());

        let mut annotations = HashMap::new();
        annotations.insert(
            "deployment.kubernetes.io/desired-replicas".to_string(),
            desired_replicas_annotation.to_string(),
        );
        annotations.insert(
            "deployment.kubernetes.io/max-replicas".to_string(),
            max_replicas_annotation.to_string(),
        );

        let namespace = deployment
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        // Clone the deployment template wholesale; only adjust the container
        // image and add the pod-template-hash label. Template metadata
        // (UIDs/timestamps from ObjectMeta::new) thus matches exactly so
        // EqualIgnoreHash returns true for the same image.
        let mut template = deployment.spec.template.clone();
        if let Some(metadata) = template.metadata.as_mut() {
            metadata
                .labels
                .get_or_insert_with(HashMap::new)
                .insert("pod-template-hash".to_string(), suffix.to_string());
        }
        if let Some(container) = template.spec.containers.first_mut() {
            container.image = image.to_string();
        }

        ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: format!("{}-{}", deployment.metadata.name, suffix),
                namespace: Some(namespace),
                labels: Some(labels.clone()),
                annotations: Some(annotations),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    name: deployment.metadata.name.clone(),
                    uid: deployment.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas,
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template,
                min_ready_seconds: None,
            },
            status: None,
        }
    }

    /// Build a Deployment with the given image + replicas + (maxSurge,
    /// maxUnavailable) and optional `paused` flag. Reduces boilerplate in the
    /// proportional / rollover tests below.
    fn build_deployment_for_tests(
        name: &str,
        replicas: i32,
        image: &str,
        labels: &std::collections::HashMap<String, String>,
        max_surge: serde_json::Value,
        max_unavailable: serde_json::Value,
        paused: bool,
        uid: &str,
    ) -> Deployment {
        use rusternetes_common::resources::{
            deployment::{DeploymentStrategy, RollingUpdateDeployment},
            PodTemplateSpec,
        };
        use rusternetes_common::types::LabelSelector;

        Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some("default".to_string()),
                uid: uid.to_string(),
                ..Default::default()
            },
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(replicas),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("main", image)],
                        ..Default::default()
                    },
                },
                strategy: Some(DeploymentStrategy {
                    strategy_type: "RollingUpdate".to_string(),
                    rolling_update: Some(RollingUpdateDeployment {
                        max_surge: Some(max_surge),
                        max_unavailable: Some(max_unavailable),
                    }),
                }),
                min_ready_seconds: None,
                revision_history_limit: None,
                paused: if paused { Some(true) } else { None },
                progress_deadline_seconds: None,
            },
            status: None,
        }
    }

    /// Proportional scaling distributes a scale delta across two active RSes
    /// (old + new) proportionally to each RS's current size using its
    /// max-replicas annotation as the denominator. K8s ref:
    /// pkg/controller/deployment/util/deployment_util.go — GetProportion.
    #[tokio::test]
    async fn test_proportional_scaling_distributes_replicas_to_old_and_new() {
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "prop-scale".to_string());

        // Mirror upstream e2e: replicas was 10 with maxSurge=3 / maxUnavail=2,
        // mid-rollout state oldRS=8 (image v1), newRS=5 (image v2). The
        // deployment is then scaled from 10 to 30. Expected distribution:
        // oldRS=20, newRS=13 (totaling 33).
        let deployment = build_deployment_for_tests(
            "prop",
            30,
            "v2",
            &labels,
            serde_json::json!(3),
            serde_json::json!(2),
            false,
            "dep-uid",
        );
        let dep_key = build_key("deployments", Some("default"), "prop");
        storage.create(&dep_key, &deployment).await.unwrap();

        let mut old_rs = build_owned_rs(&deployment, "old", 8, 13, 10, "v1");
        old_rs.metadata.creation_timestamp =
            Some(chrono::Utc::now() - chrono::Duration::minutes(10));
        let old_key = build_key("replicasets", Some("default"), &old_rs.metadata.name);
        storage.create(&old_key, &old_rs).await.unwrap();

        let mut new_rs = build_owned_rs(&deployment, "new", 5, 13, 10, "v2");
        new_rs.metadata.creation_timestamp =
            Some(chrono::Utc::now() - chrono::Duration::minutes(1));
        let new_key = build_key("replicasets", Some("default"), &new_rs.metadata.name);
        storage.create(&new_key, &new_rs).await.unwrap();

        controller.reconcile_deployment(&deployment).await.unwrap();

        let old_after: ReplicaSet = storage.get(&old_key).await.unwrap();
        let new_after: ReplicaSet = storage.get(&new_key).await.unwrap();

        // allowedSize = 30 + 3 = 33, current = 13, delta = 20
        //   oldRS: 8 * 33/13 = 20.30 -> 20  (fraction +12)
        //   newRS: 5 * 33/13 = 12.69 -> 13  (fraction +8)
        assert_eq!(new_after.spec.replicas, 13, "new RS should be 13");
        assert_eq!(old_after.spec.replicas, 20, "old RS should be 20");

        // Annotations refresh to new baseline (desired=30, max=33).
        let new_anno = new_after.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            new_anno.get("deployment.kubernetes.io/desired-replicas"),
            Some(&"30".to_string())
        );
        assert_eq!(
            new_anno.get("deployment.kubernetes.io/max-replicas"),
            Some(&"33".to_string())
        );
    }

    /// Proportional scaling for a paused deployment: scaling a paused rollout
    /// must still distribute replicas proportionally across the two active
    /// RSes, and the rollout must not progress beyond that.
    #[tokio::test]
    async fn test_proportional_scaling_while_paused() {
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "paused-scale".to_string());

        let deployment = build_deployment_for_tests(
            "paused",
            30,
            "v2",
            &labels,
            serde_json::json!(3),
            serde_json::json!(2),
            true,
            "dep-uid",
        );
        let dep_key = build_key("deployments", Some("default"), "paused");
        storage.create(&dep_key, &deployment).await.unwrap();

        let mut old_rs = build_owned_rs(&deployment, "old", 8, 13, 10, "v1");
        old_rs.metadata.creation_timestamp =
            Some(chrono::Utc::now() - chrono::Duration::minutes(10));
        let old_key = build_key("replicasets", Some("default"), &old_rs.metadata.name);
        storage.create(&old_key, &old_rs).await.unwrap();

        let mut new_rs = build_owned_rs(&deployment, "new", 5, 13, 10, "v2");
        new_rs.metadata.creation_timestamp =
            Some(chrono::Utc::now() - chrono::Duration::minutes(1));
        let new_key = build_key("replicasets", Some("default"), &new_rs.metadata.name);
        storage.create(&new_key, &new_rs).await.unwrap();

        controller.reconcile_deployment(&deployment).await.unwrap();

        let old_after: ReplicaSet = storage.get(&old_key).await.unwrap();
        let new_after: ReplicaSet = storage.get(&new_key).await.unwrap();
        assert_eq!(new_after.spec.replicas, 13, "paused new RS should be 13");
        assert_eq!(old_after.spec.replicas, 20, "paused old RS should be 20");
    }

    /// Paused deployments must NOT run the rolling-update progression
    /// (reconcileNewReplicaSet / reconcileOldReplicaSets, which would scale
    /// the old RS down aggressively). scale() still applies surge leftover
    /// to the new RS, but never reduces the old RS below its current count.
    #[tokio::test]
    async fn test_paused_deployment_does_not_progress_rollout() {
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "paused-rollout".to_string());

        let deployment = build_deployment_for_tests(
            "pp",
            10,
            "v2",
            &labels,
            serde_json::json!(3),
            serde_json::json!(2),
            true,
            "dep-uid",
        );
        let dep_key = build_key("deployments", Some("default"), "pp");
        storage.create(&dep_key, &deployment).await.unwrap();

        let mut old_rs = build_owned_rs(&deployment, "old", 8, 13, 10, "v1");
        old_rs.metadata.creation_timestamp =
            Some(chrono::Utc::now() - chrono::Duration::minutes(10));
        let old_key = build_key("replicasets", Some("default"), &old_rs.metadata.name);
        storage.create(&old_key, &old_rs).await.unwrap();

        let mut new_rs = build_owned_rs(&deployment, "new", 2, 13, 10, "v2");
        new_rs.metadata.creation_timestamp =
            Some(chrono::Utc::now() - chrono::Duration::minutes(1));
        let new_key = build_key("replicasets", Some("default"), &new_rs.metadata.name);
        storage.create(&new_key, &new_rs).await.unwrap();

        controller.reconcile_deployment(&deployment).await.unwrap();

        let old_after: ReplicaSet = storage.get(&old_key).await.unwrap();
        let new_after: ReplicaSet = storage.get(&new_key).await.unwrap();
        // scale() assigns surge leftover (allowed_size 13 - current 10 = 3)
        // to the newest RS — new RS grows from 2 to 5. The old RS does NOT
        // shrink: no rolling-update progression while paused.
        assert_eq!(old_after.spec.replicas, 8, "paused: old RS unchanged");
        assert_eq!(
            new_after.spec.replicas, 5,
            "paused: new RS gets surge leftover (2 + 3)"
        );
    }

    /// Rollover: triggering a second rollout before the first finishes must
    /// not leave two simultaneous in-progress new ReplicaSets. The
    /// previously in-progress RS becomes an "old" RS, and only ONE new RS —
    /// matching the current template — is created with a higher revision.
    #[tokio::test]
    async fn test_rollover_creates_single_new_replicaset() {
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "rollover".to_string());

        let deployment = build_deployment_for_tests(
            "ro",
            4,
            "v3",
            &labels,
            serde_json::json!(1),
            serde_json::json!(1),
            false,
            "dep-uid",
        );
        let dep_key = build_key("deployments", Some("default"), "ro");
        storage.create(&dep_key, &deployment).await.unwrap();

        let mut rs_v1 = build_owned_rs(&deployment, "v1hash", 3, 5, 4, "v1");
        rs_v1.metadata.creation_timestamp =
            Some(chrono::Utc::now() - chrono::Duration::minutes(20));
        rs_v1
            .metadata
            .annotations
            .get_or_insert_with(HashMap::new)
            .insert(
                "deployment.kubernetes.io/revision".to_string(),
                "1".to_string(),
            );
        let v1_key = build_key("replicasets", Some("default"), &rs_v1.metadata.name);
        storage.create(&v1_key, &rs_v1).await.unwrap();

        let mut rs_v2 = build_owned_rs(&deployment, "v2hash", 1, 5, 4, "v2");
        rs_v2.metadata.creation_timestamp = Some(chrono::Utc::now() - chrono::Duration::minutes(5));
        rs_v2
            .metadata
            .annotations
            .get_or_insert_with(HashMap::new)
            .insert(
                "deployment.kubernetes.io/revision".to_string(),
                "2".to_string(),
            );
        let v2_key = build_key("replicasets", Some("default"), &rs_v2.metadata.name);
        storage.create(&v2_key, &rs_v2).await.unwrap();

        controller.reconcile_deployment(&deployment).await.unwrap();

        let rs_prefix = build_prefix("replicasets", Some("default"));
        let all_rs: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
        let new_rses: Vec<&ReplicaSet> = all_rs
            .iter()
            .filter(|rs| {
                rs.spec
                    .template
                    .spec
                    .containers
                    .first()
                    .map(|c| c.image == "v3")
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(new_rses.len(), 1, "rollover should produce one new RS");

        let new_rs = new_rses[0];
        let rev = new_rs
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("deployment.kubernetes.io/revision"))
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        assert!(rev >= 3, "new RS revision should be >= 3 (was {})", rev);
    }

    /// Rollover scale-down: once the new RS is saturated, every old RS
    /// (including the previously in-progress one) must be scaled to 0.
    #[tokio::test]
    async fn test_rollover_scales_down_old_replicasets_to_zero() {
        use rusternetes_common::resources::ReplicaSetStatus;
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "rollover2".to_string());

        let deployment = build_deployment_for_tests(
            "ro2",
            4,
            "v3",
            &labels,
            serde_json::json!(1),
            serde_json::json!(1),
            false,
            "dep-uid-2",
        );
        let dep_key = build_key("deployments", Some("default"), "ro2");
        storage.create(&dep_key, &deployment).await.unwrap();

        let mut rs_v3 = build_owned_rs(&deployment, "v3hash", 4, 5, 4, "v3");
        rs_v3.metadata.creation_timestamp = Some(chrono::Utc::now());
        rs_v3.status = Some(ReplicaSetStatus {
            replicas: 4,
            ready_replicas: 4,
            available_replicas: 4,
            fully_labeled_replicas: Some(4),
            observed_generation: None,
            conditions: None,
            terminating_replicas: None,
        });
        let v3_key = build_key("replicasets", Some("default"), &rs_v3.metadata.name);
        storage.create(&v3_key, &rs_v3).await.unwrap();

        let mut rs_v1 = build_owned_rs(&deployment, "v1hash", 2, 5, 4, "v1");
        rs_v1.metadata.creation_timestamp =
            Some(chrono::Utc::now() - chrono::Duration::minutes(20));
        let v1_key = build_key("replicasets", Some("default"), &rs_v1.metadata.name);
        storage.create(&v1_key, &rs_v1).await.unwrap();

        let mut rs_v2 = build_owned_rs(&deployment, "v2hash", 1, 5, 4, "v2");
        rs_v2.metadata.creation_timestamp = Some(chrono::Utc::now() - chrono::Duration::minutes(5));
        let v2_key = build_key("replicasets", Some("default"), &rs_v2.metadata.name);
        storage.create(&v2_key, &rs_v2).await.unwrap();

        controller.reconcile_deployment(&deployment).await.unwrap();

        let v1_after: ReplicaSet = storage.get(&v1_key).await.unwrap();
        let v2_after: ReplicaSet = storage.get(&v2_key).await.unwrap();
        let v3_after: ReplicaSet = storage.get(&v3_key).await.unwrap();
        assert_eq!(v3_after.spec.replicas, 4, "new v3 stays at 4");
        assert_eq!(v1_after.spec.replicas, 0, "old v1 scales to 0");
        assert_eq!(v2_after.spec.replicas, 0, "old v2 scales to 0");
    }
}
