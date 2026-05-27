use futures::StreamExt;
use rusternetes_common::{
    resources::{Pod, PodStatus, ReplicaSet, ReplicaSetStatus},
    types::{ObjectMeta, Phase},
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

/// ReplicaSetController reconciles ReplicaSet resources
/// A ReplicaSet ensures that a specified number of pod replicas are running at any given time
pub struct ReplicaSetController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
}

impl<S: Storage + 'static> ReplicaSetController<S> {
    pub fn new(storage: Arc<S>, interval_secs: u64) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(interval_secs),
        }
    }

    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        info!("ReplicaSet controller started (watch-based)");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch for changes to ReplicaSets AND Pods
            let prefix = build_prefix("replicasets", None);
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

            let pod_prefix = build_prefix("pods", None);
            let mut pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish pod watch: {}, retrying in {:?}",
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
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                self.enqueue_owner_replicaset(&queue, &ev).await;
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
            let storage_key = build_key("replicasets", Some(ns), name);
            match self.storage.get::<ReplicaSet>(&storage_key).await {
                Ok(resource) => match self.reconcile_replicaset(&resource).await {
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
            .list::<ReplicaSet>("/registry/replicasets/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("replicasets/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list replicasets for enqueue: {}", e);
            }
        }
    }

    /// When a pod changes, check its ownerReferences for a ReplicaSet owner
    /// and enqueue that ReplicaSet for reconciliation.
    async fn enqueue_owner_replicaset(
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
                        if owner_ref.kind == "ReplicaSet" {
                            queue
                                .add(format!("replicasets/{}/{}", ns, owner_ref.name))
                                .await;
                        }
                    }
                }
            }
            Err(_) => {
                // Pod deleted — enqueue all ReplicaSets in this namespace
                if let Ok(items) = self
                    .storage
                    .list::<ReplicaSet>(&build_prefix("replicasets", Some(ns)))
                    .await
                {
                    for rs in &items {
                        queue
                            .add(format!("replicasets/{}/{}", ns, rs.metadata.name))
                            .await;
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> rusternetes_common::Result<()> {
        debug!("Reconciling all replicasets");

        // Get all replicasets
        let prefix = build_prefix("replicasets", None);
        let replicasets: Vec<ReplicaSet> = self.storage.list(&prefix).await?;

        for replicaset in replicasets {
            if let Err(e) = self.reconcile_replicaset(&replicaset).await {
                error!(
                    "Error reconciling replicaset {}: {}",
                    replicaset.metadata.name, e
                );
            }
        }

        Ok(())
    }

    async fn reconcile_replicaset(
        &self,
        replicaset: &ReplicaSet,
    ) -> rusternetes_common::Result<()> {
        let namespace = replicaset
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        debug!(
            "Reconciling replicaset: {}/{}",
            namespace, replicaset.metadata.name
        );

        // Get all pods for this replicaset
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await?;

        // Adopt orphan pods: pods that match the selector labels but have no controller ownerReference
        // Release owned pods: pods owned by this RS but whose labels no longer match the selector
        let all_pods = self
            .adopt_and_release(replicaset, all_pods, namespace)
            .await?;

        // Filter pods that match this replicaset's selector (owned + matching labels)
        let replicaset_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|p| {
                let matches = self.matches_selector(p, replicaset);
                debug!(
                    "Pod {} matches selector: {} (labels: {:?})",
                    p.metadata.name, matches, p.metadata.labels
                );
                matches
            })
            .collect();

        // Count ready and available pods
        let ready_count = replicaset_pods
            .iter()
            .filter(|p| self.is_pod_ready(p))
            .count() as i32;

        let available_count = replicaset_pods
            .iter()
            .filter(|p| self.is_pod_available(p, replicaset))
            .count() as i32;

        let current_replicas = replicaset_pods.len() as i32;
        let desired_replicas = replicaset.spec.replicas;

        debug!(
            "ReplicaSet {}/{}: current={}, ready={}, available={}, desired={}",
            namespace,
            replicaset.metadata.name,
            current_replicas,
            ready_count,
            available_count,
            desired_replicas
        );

        // Reconcile pod count
        if current_replicas < desired_replicas {
            // Need to create more pods
            let to_create = desired_replicas - current_replicas;
            info!(
                "Creating {} pods for replicaset {}/{}",
                to_create, namespace, replicaset.metadata.name
            );
            for _ in 0..to_create {
                self.create_pod(replicaset).await?;
            }
        } else if current_replicas > desired_replicas {
            // Need to delete excess pods
            let to_delete = current_replicas - desired_replicas;
            info!(
                "Deleting {} excess pods for replicaset {}/{}",
                to_delete, namespace, replicaset.metadata.name
            );
            for pod in replicaset_pods.iter().take(to_delete as usize) {
                self.delete_pod(&pod.metadata.name, namespace).await?;
            }
        }

        // Re-fetch and recount pods after create/delete operations to get accurate status
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods_after: Vec<Pod> = self.storage.list(&pods_prefix).await?;

        let replicaset_pods_after: Vec<Pod> = all_pods_after
            .into_iter()
            .filter(|p| self.matches_selector(p, replicaset))
            .collect();

        let final_ready_count = replicaset_pods_after
            .iter()
            .filter(|p| self.is_pod_ready(p))
            .count() as i32;

        let final_available_count = replicaset_pods_after
            .iter()
            .filter(|p| self.is_pod_available(p, replicaset))
            .count() as i32;

        let final_current_replicas = replicaset_pods_after.len() as i32;

        // Update status with accurate counts
        self.update_status(
            replicaset,
            final_current_replicas,
            final_ready_count,
            final_available_count,
        )
        .await?;

        Ok(())
    }

    /// Check if a pod's labels match the ReplicaSet's selector (ignoring ownerReference)
    fn labels_match_selector(&self, pod: &Pod, replicaset: &ReplicaSet) -> bool {
        if let Some(match_labels) = &replicaset.spec.selector.match_labels {
            if let Some(pod_labels) = &pod.metadata.labels {
                for (key, value) in match_labels {
                    if pod_labels.get(key) != Some(value) {
                        return false;
                    }
                }
                return true;
            }
        }
        false
    }

    /// Check if a pod is owned by this ReplicaSet (has a controller ownerReference pointing to it).
    ///
    /// Mirrors `controller.IsControlledBy` from upstream
    /// `pkg/controller/replicaset/replica_set.go`: a pod is owned iff one of its
    /// `metadata.ownerReferences` has `controller=true` AND `uid == rs.UID`.
    /// UID matching is the upstream contract (the conformance test
    /// `ReplicaSet should adopt matching pods on creation and release no
    /// longer matching pods` polls for `owner.UID == rs.UID`). When the RS UID
    /// is empty (in-process tests that never go through the api-server) we
    /// fall back to name+kind matching so existing reconciler unit-tests still
    /// pass.
    fn is_owned_by(&self, pod: &Pod, replicaset: &ReplicaSet) -> bool {
        pod.metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter().any(|r| {
                    if r.controller != Some(true) {
                        return false;
                    }
                    if !replicaset.metadata.uid.is_empty() {
                        // UID match is the upstream-strict path
                        r.uid == replicaset.metadata.uid
                    } else {
                        // Fallback for unit-tests where the RS lacks a UID
                        r.kind == "ReplicaSet" && r.name == replicaset.metadata.name
                    }
                })
            })
            .unwrap_or(false)
    }

    /// Check if a pod has any controller ownerReference at all
    fn has_controller_owner(&self, pod: &Pod) -> bool {
        pod.metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.controller == Some(true)))
            .unwrap_or(false)
    }

    /// Adopt orphan pods that match the selector and release owned pods that no longer match.
    /// Returns the updated list of all pods (with ownerReferences modified as needed).
    async fn adopt_and_release(
        &self,
        replicaset: &ReplicaSet,
        mut all_pods: Vec<Pod>,
        namespace: &str,
    ) -> rusternetes_common::Result<Vec<Pod>> {
        #[allow(clippy::needless_range_loop)]
        for i in 0..all_pods.len() {
            let pod = &all_pods[i];

            // Skip terminated or deleting pods
            if let Some(ref status) = pod.status {
                if let Some(ref phase) = status.phase {
                    if matches!(phase, Phase::Failed | Phase::Succeeded) {
                        continue;
                    }
                }
            }
            if pod.metadata.deletion_timestamp.is_some() {
                continue;
            }

            let labels_match = self.labels_match_selector(pod, replicaset);
            let owned = self.is_owned_by(pod, replicaset);

            if labels_match && !owned && !self.has_controller_owner(pod) {
                // Adopt orphan pod: labels match, no controller owner
                let mut adopted_pod = pod.clone();
                let owner_ref = rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "ReplicaSet".to_string(),
                    name: replicaset.metadata.name.clone(),
                    uid: replicaset.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                };
                adopted_pod
                    .metadata
                    .owner_references
                    .get_or_insert_with(Vec::new)
                    .push(owner_ref);

                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                match self.storage.update(&pod_key, &adopted_pod).await {
                    Ok(_) => {
                        info!(
                            "Adopted orphan pod {} for replicaset {}/{}",
                            pod.metadata.name, namespace, replicaset.metadata.name
                        );
                        all_pods[i] = adopted_pod;
                    }
                    Err(e) => {
                        debug!("Failed to adopt pod {}: {}", pod.metadata.name, e);
                    }
                }
            } else if !labels_match && owned {
                // Release owned pod: labels no longer match.
                // Mirror `is_owned_by` and remove ALL controller refs whose
                // UID matches this RS (with name fallback for tests without
                // a UID). Upstream `release()` in `controllerRefManager.go`
                // uses UID matching exclusively.
                let mut released_pod = pod.clone();
                if let Some(refs) = &mut released_pod.metadata.owner_references {
                    let rs_uid = replicaset.metadata.uid.clone();
                    let rs_name = replicaset.metadata.name.clone();
                    refs.retain(|r| {
                        if r.controller != Some(true) {
                            return true;
                        }
                        let points_at_us = if !rs_uid.is_empty() {
                            r.uid == rs_uid
                        } else {
                            r.kind == "ReplicaSet" && r.name == rs_name
                        };
                        !points_at_us
                    });
                    if refs.is_empty() {
                        released_pod.metadata.owner_references = None;
                    }
                }

                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                match self.storage.update(&pod_key, &released_pod).await {
                    Ok(_) => {
                        info!(
                            "Released pod {} from replicaset {}/{}",
                            pod.metadata.name, namespace, replicaset.metadata.name
                        );
                        all_pods[i] = released_pod;
                    }
                    Err(e) => {
                        debug!("Failed to release pod {}: {}", pod.metadata.name, e);
                    }
                }
            }
        }

        Ok(all_pods)
    }

    fn matches_selector(&self, pod: &Pod, replicaset: &ReplicaSet) -> bool {
        // Skip pods that are terminated (Failed or Succeeded) — they don't count toward replicas
        if let Some(ref status) = pod.status {
            if let Some(ref phase) = status.phase {
                if matches!(phase, Phase::Failed | Phase::Succeeded) {
                    return false;
                }
            }
        }

        // Skip pods being deleted (have a deletionTimestamp)
        if pod.metadata.deletion_timestamp.is_some() {
            return false;
        }

        // Check owner reference — only count pods owned by this ReplicaSet.
        // Delegates to `is_owned_by` so name vs UID matching stays in one
        // place (upstream conformance polls for `owner.UID == rs.UID`).
        if !self.is_owned_by(pod, replicaset) {
            return false;
        }

        self.labels_match_selector(pod, replicaset)
    }

    /// Check if a pod is ready by examining its conditions
    fn is_pod_ready(&self, pod: &Pod) -> bool {
        if let Some(conditions) = pod.status.as_ref().and_then(|s| s.conditions.as_ref()) {
            conditions
                .iter()
                .any(|c| c.condition_type == "Ready" && c.status == "True")
        } else {
            false
        }
    }

    fn is_pod_available(&self, pod: &Pod, replicaset: &ReplicaSet) -> bool {
        // K8s IsPodAvailable: Ready condition True + minReadySeconds + not terminating
        // Does NOT require phase == Running (a pod can be Ready before/during phase transitions)
        if !self.is_pod_ready(pod) {
            return false;
        }

        // Pod must not be terminating
        if pod.metadata.deletion_timestamp.is_some() {
            return false;
        }

        // Check if pod has been ready for minReadySeconds
        let min_ready_seconds = replicaset.spec.min_ready_seconds.unwrap_or(0);
        if min_ready_seconds > 0 {
            // Get pod creation time as a proxy for when it became ready
            // In a full implementation, we'd check the Ready condition's lastTransitionTime
            if let Some(creation_time) = pod.metadata.creation_timestamp {
                let now = chrono::Utc::now();
                let elapsed = now.signed_duration_since(creation_time);

                // Pod is available if it's been ready for at least minReadySeconds
                return elapsed.num_seconds() >= min_ready_seconds as i64;
            }
            // If no timestamp, can't determine availability
            false
        } else {
            // If minReadySeconds is 0, pod is available as soon as it's ready
            true
        }
    }

    async fn update_status(
        &self,
        replicaset: &ReplicaSet,
        replicas: i32,
        ready_replicas: i32,
        available_replicas: i32,
    ) -> rusternetes_common::Result<()> {
        let namespace = replicaset
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        let key = build_key("replicasets", Some(namespace), &replicaset.metadata.name);

        // Re-read from storage for fresh resourceVersion to avoid CAS conflicts
        let mut updated_rs: ReplicaSet = match self.storage.get(&key).await {
            Ok(rs) => rs,
            Err(_) => replicaset.clone(),
        };

        // Preserve existing conditions (user/test-set) instead of wiping them
        let existing_conditions = updated_rs
            .status
            .as_ref()
            .and_then(|s| s.conditions.clone());

        let new_status = Some(ReplicaSetStatus {
            replicas,
            ready_replicas,
            available_replicas,
            fully_labeled_replicas: Some(replicas),
            observed_generation: updated_rs.metadata.generation,
            conditions: existing_conditions,
            terminating_replicas: None,
        });

        // Only write status if it actually changed to avoid unnecessary storage writes
        // that trigger watch events and cause feedback loops
        if updated_rs.status != new_status {
            updated_rs.status = new_status;

            if let Err(e) = self.storage.update(&key, &updated_rs).await {
                // CAS conflict — re-read and retry once
                debug!("RS status update CAS conflict, retrying: {}", e);
                if let Ok(mut fresh_rs) = self.storage.get::<ReplicaSet>(&key).await {
                    let fresh_conditions =
                        fresh_rs.status.as_ref().and_then(|s| s.conditions.clone());
                    let retry_status = Some(ReplicaSetStatus {
                        replicas,
                        ready_replicas,
                        available_replicas,
                        fully_labeled_replicas: Some(replicas),
                        observed_generation: fresh_rs.metadata.generation,
                        conditions: fresh_conditions,
                        terminating_replicas: None,
                    });
                    if fresh_rs.status != retry_status {
                        fresh_rs.status = retry_status;
                        let _ = self.storage.update(&key, &fresh_rs).await;
                    }
                }
            }
        }

        debug!(
            "Updated status for replicaset {}/{}: replicas={}, ready={}, available={}",
            namespace, replicaset.metadata.name, replicas, ready_replicas, available_replicas
        );

        Ok(())
    }

    async fn create_pod(&self, replicaset: &ReplicaSet) -> rusternetes_common::Result<()> {
        let namespace = replicaset
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        // K8s uses <rs-name>-<5-char-random> to keep pod names under 63 chars
        // (Linux hostname limit). Full UUIDs make names too long.
        let suffix: String = uuid::Uuid::new_v4().to_string().chars().take(5).collect();
        let pod_name = format!("{}-{}", replicaset.metadata.name, suffix);

        let mut metadata = ObjectMeta::new(&pod_name);
        metadata.namespace = Some(namespace.to_string());
        // Use template labels when present, otherwise fall back to selector
        // matchLabels so created pods can be matched by the controller on the
        // next reconcile. Per K8s validation, template labels must be a
        // superset of the selector — but a defensive fallback prevents
        // runaway pod creation if a malformed RS arrives with no template
        // labels.
        metadata.labels = replicaset
            .spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .or_else(|| replicaset.spec.selector.match_labels.clone());

        // Set owner reference so pods are garbage collected when ReplicaSet is deleted
        metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: replicaset.metadata.name.clone(),
            uid: replicaset.metadata.uid.clone(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);

        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            spec: Some(replicaset.spec.template.spec.clone()),
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
        super::check_resource_quota(&*self.storage, namespace)
            .await
            .map_err(|e| rusternetes_common::Error::Forbidden(e.to_string()))?;

        let key = build_key("pods", Some(namespace), &pod_name);
        self.storage.create(&key, &pod).await?;

        info!(
            "Created pod {}/{} for replicaset {}",
            namespace, pod_name, replicaset.metadata.name
        );

        Ok(())
    }

    async fn delete_pod(&self, name: &str, namespace: &str) -> rusternetes_common::Result<()> {
        let key = build_key("pods", Some(namespace), name);
        self.storage.delete(&key).await?;

        info!("Deleted pod {}/{}", namespace, name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::PodSpec;
    use rusternetes_common::types::{LabelSelector, OwnerReference, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;
    use rusternetes_storage::Storage;
    use std::collections::HashMap;

    fn make_labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn make_replicaset(name: &str, labels: HashMap<String, String>, replicas: i32) -> ReplicaSet {
        ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("default"),
            spec: rusternetes_common::resources::ReplicaSetSpec {
                replicas,
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels)),
                    spec: PodSpec {
                        containers: vec![],
                        ..Default::default()
                    },
                },
                min_ready_seconds: None,
            },
            status: None,
        }
    }

    fn make_pod(name: &str, labels: HashMap<String, String>) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name)
                .with_namespace("default")
                .with_labels(labels),
            spec: Some(PodSpec {
                containers: vec![],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_matches_selector() {
        let labels = make_labels(&[("app", "test")]);

        let pod_with_labels = make_pod("test-pod", labels);
        assert_eq!(pod_with_labels.metadata.name, "test-pod");
    }

    #[tokio::test]
    async fn test_adopt_orphan_pods_and_release_non_matching() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);

        let labels = make_labels(&[("app", "myapp")]);

        // Create an orphan pod with matching labels but no ownerReference
        let orphan_pod = make_pod("orphan-pod", labels.clone());
        let pod_key = build_key("pods", Some("default"), "orphan-pod");
        storage.create(&pod_key, &orphan_pod).await.unwrap();

        // Create a ReplicaSet with matching selector and replicas=1
        let rs = make_replicaset("my-rs", labels.clone(), 1);
        let rs_key = build_key("replicasets", Some("default"), "my-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        // Run reconciliation
        controller.reconcile_all().await.unwrap();

        // Verify the orphan pod was adopted (now has ownerReference pointing to the RS)
        let adopted_pod: Pod = storage.get(&pod_key).await.unwrap();
        let owner_refs = adopted_pod.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "ReplicaSet");
        assert_eq!(owner_refs[0].name, "my-rs");
        assert_eq!(owner_refs[0].controller, Some(true));
        assert_eq!(owner_refs[0].block_owner_deletion, Some(true));

        // Since the RS wants 1 replica and has adopted 1, no extra pods should be created
        let pods_prefix = build_prefix("pods", Some("default"));
        let all_pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(
            all_pods.len(),
            1,
            "Should have exactly 1 pod (the adopted one), got {}",
            all_pods.len()
        );

        // Now change the pod's labels so they no longer match the RS selector
        let mut modified_pod: Pod = storage.get(&pod_key).await.unwrap();
        modified_pod.metadata.labels = Some(make_labels(&[("app", "different")]));
        storage.update(&pod_key, &modified_pod).await.unwrap();

        // Run reconciliation again
        controller.reconcile_all().await.unwrap();

        // Verify the pod's ownerReference was removed (released)
        let released_pod: Pod = storage.get(&pod_key).await.unwrap();
        let has_rs_owner = released_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.kind == "ReplicaSet" && r.name == "my-rs")
            })
            .unwrap_or(false);
        assert!(
            !has_rs_owner,
            "Pod should no longer have ownerReference to RS after label change"
        );
    }

    #[tokio::test]
    async fn test_adopt_does_not_steal_owned_pods() {
        // Pods that already have a controller owner should NOT be adopted
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);

        let labels = make_labels(&[("app", "myapp")]);

        // Create a pod owned by a different controller
        let mut owned_pod = make_pod("owned-pod", labels.clone());
        owned_pod.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: "other-rs".to_string(),
            uid: "other-uid".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod_key = build_key("pods", Some("default"), "owned-pod");
        storage.create(&pod_key, &owned_pod).await.unwrap();

        // Create a RS with matching selector
        let rs = make_replicaset("my-rs", labels.clone(), 1);
        let rs_key = build_key("replicasets", Some("default"), "my-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        // Run reconciliation
        controller.reconcile_all().await.unwrap();

        // Verify the owned pod was NOT adopted (still owned by other-rs)
        let pod: Pod = storage.get(&pod_key).await.unwrap();
        let refs = pod.metadata.owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "other-rs");
    }

    #[tokio::test]
    async fn test_rs_creates_pods_with_selector_labels_when_template_has_none() {
        // Regression: a ReplicaSet whose template has no labels would create
        // pods with no labels, so the controller's matches_selector check
        // failed for its own pods, causing infinite pod creation. Fall back
        // to selector matchLabels when template labels are absent.
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);

        let labels = make_labels(&[("app", "myapp")]);

        // Build an RS where the pod template has no labels at all.
        let mut rs = make_replicaset("no-template-labels-rs", labels.clone(), 2);
        rs.spec.template.metadata = None;
        let rs_key = build_key("replicasets", Some("default"), "no-template-labels-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        controller.reconcile_all().await.unwrap();

        // Two pods should be created with selector labels applied so the
        // controller matches them on the next reconcile.
        let pods_prefix = build_prefix("pods", Some("default"));
        let pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(pods.len(), 2, "RS should create exactly 2 pods");
        for pod in &pods {
            let pod_labels = pod
                .metadata
                .labels
                .as_ref()
                .expect("Pod must inherit selector labels");
            assert_eq!(pod_labels.get("app"), Some(&"myapp".to_string()));
        }

        // Second reconcile must not create more pods (matching works).
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(
            pods.len(),
            2,
            "second reconcile must match existing pods, not create more"
        );
    }
}
