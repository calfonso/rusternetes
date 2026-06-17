use chrono::{DateTime, Utc};
use rusternetes_common::resources::{
    CustomResource, CustomResourceDefinition, IntOrString, Pod, PodDisruptionBudget,
    PodDisruptionBudgetStatus,
};
use rusternetes_common::types::{LabelSelector, OwnerReference, Phase};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{debug, error, info, warn};

pub struct PodDisruptionBudgetController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> PodDisruptionBudgetController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        use futures::StreamExt;

        info!("Starting PodDisruptionBudget controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("poddisruptionbudgets", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
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
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("poddisruptionbudgets", Some(ns), name);
            match self.storage.get::<PodDisruptionBudget>(&storage_key).await {
                Ok(resource) => match self.reconcile_pdb(&resource).await {
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
            .list::<PodDisruptionBudget>("/registry/poddisruptionbudgets/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("poddisruptionbudgets/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list poddisruptionbudgets for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> rusternetes_common::Result<()> {
        debug!("Reconciling all PodDisruptionBudgets");

        // Get all PDBs
        let prefix = build_prefix("poddisruptionbudgets", None);
        let pdbs: Vec<PodDisruptionBudget> = self.storage.list(&prefix).await?;

        for pdb in pdbs {
            if let Err(e) = self.reconcile_pdb(&pdb).await {
                warn!("Failed to reconcile PDB {}: {}", pdb.metadata.name, e);
            }
        }

        Ok(())
    }

    async fn reconcile_pdb(&self, pdb: &PodDisruptionBudget) -> rusternetes_common::Result<()> {
        let namespace = pdb.metadata.namespace.as_deref().unwrap_or("default");

        debug!(
            "Reconciling PodDisruptionBudget: {}/{}",
            namespace, pdb.metadata.name
        );

        // 1. Find all pods matching the selector in the PDB's namespace
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await?;

        // 2. Filter pods that match the PDB selector. The `policy/v1beta1` API
        // gave empty selectors the opposite meaning to `policy/v1`: an empty
        // selector matches NO pods (whereas v1 treats it as match-all). We
        // detect the apiVersion off the stored TypeMeta and pass it through.
        let is_v1beta1 = pdb.type_meta.api_version == "policy/v1beta1";
        let matching_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|p| self.pod_matches_selector(p, &pdb.spec.selector, is_v1beta1))
            .collect();

        // 3. Count healthy pods (Running + Ready).
        let pod_count = matching_pods.len() as i32;
        let healthy_pods = matching_pods
            .iter()
            .filter(|p| self.is_pod_healthy(p))
            .count() as i32;

        // 3a. Compute expectedPods. Upstream mirrors
        // `pkg/controller/disruption/disruption.go::getExpectedScale`: walk
        // every matched pod's controller ownerReference up to a workload
        // root (Deployment, ReplicaSet, StatefulSet, ReplicationController,
        // or any CRD with a scale subresource) and SUM their `spec.replicas`
        // values. Pods without a resolvable controller fall back to the
        // pod count for that owner, which is the same shape upstream uses
        // when an owner kind is unknown.
        let total_pods = self
            .compute_expected_pods(namespace, &matching_pods)
            .await
            .unwrap_or(pod_count);

        debug!(
            "PDB {}/{}: total={} (pods={}), healthy={}",
            namespace, pdb.metadata.name, total_pods, pod_count, healthy_pods
        );

        // 4. Calculate desired_healthy based on min_available or max_unavailable
        let desired_healthy = self.calculate_desired_healthy(pdb, total_pods)?;

        // 5. Calculate disruptions_allowed
        // disruptions_allowed = current_healthy - desired_healthy
        let disruptions_allowed = healthy_pods - desired_healthy;

        debug!(
            "PDB {}/{}: desired_healthy={}, disruptions_allowed={}",
            namespace, pdb.metadata.name, desired_healthy, disruptions_allowed
        );

        // 6. Build desired status
        let new_status = PodDisruptionBudgetStatus {
            current_healthy: healthy_pods,
            desired_healthy,
            disruptions_allowed,
            expected_pods: total_pods,
            observed_generation: pdb.metadata.generation,
            conditions: pdb.status.as_ref().and_then(|s| s.conditions.clone()),
            disrupted_pods: pdb.status.as_ref().and_then(|s| s.disrupted_pods.clone()),
        };

        // Only write if status actually changed to avoid unnecessary storage writes
        // that cause resourceVersion conflicts with concurrent test PATCH operations
        if pdb.status.as_ref() != Some(&new_status) {
            let key = build_key("poddisruptionbudgets", Some(namespace), &pdb.metadata.name);
            // Re-read from storage for fresh resourceVersion to avoid CAS conflicts
            let mut fresh_pdb: PodDisruptionBudget = match self.storage.get(&key).await {
                Ok(p) => p,
                Err(_) => pdb.clone(),
            };
            fresh_pdb.status = Some(new_status);
            self.storage.update(&key, &fresh_pdb).await?;
        }

        Ok(())
    }

    /// Calculate desired_healthy based on min_available or max_unavailable
    fn calculate_desired_healthy(
        &self,
        pdb: &PodDisruptionBudget,
        total_pods: i32,
    ) -> rusternetes_common::Result<i32> {
        if let Some(ref min_available) = pdb.spec.min_available {
            // Use min_available (either int or percentage)
            match min_available {
                IntOrString::Int(value) => Ok(*value),
                IntOrString::String(s) => {
                    // Parse percentage (e.g., "50%")
                    if let Some(stripped) = s.strip_suffix('%') {
                        let percentage: f64 = stripped.parse().map_err(|_| {
                            rusternetes_common::Error::InvalidResource(format!(
                                "Invalid percentage in minAvailable: {}",
                                s
                            ))
                        })?;
                        let desired = ((total_pods as f64) * (percentage / 100.0)).ceil() as i32;
                        Ok(desired)
                    } else {
                        Err(rusternetes_common::Error::InvalidResource(format!(
                            "Invalid minAvailable string format: {}",
                            s
                        )))
                    }
                }
            }
        } else if let Some(ref max_unavailable) = pdb.spec.max_unavailable {
            // Use max_unavailable (either int or percentage)
            let max_unavailable_count = match max_unavailable {
                IntOrString::Int(value) => *value,
                IntOrString::String(s) => {
                    // Parse percentage (e.g., "20%")
                    if let Some(stripped) = s.strip_suffix('%') {
                        let percentage: f64 = stripped.parse().map_err(|_| {
                            rusternetes_common::Error::InvalidResource(format!(
                                "Invalid percentage in maxUnavailable: {}",
                                s
                            ))
                        })?;
                        ((total_pods as f64) * (percentage / 100.0)).floor() as i32
                    } else {
                        return Err(rusternetes_common::Error::InvalidResource(format!(
                            "Invalid maxUnavailable string format: {}",
                            s
                        )));
                    }
                }
            };
            // desired_healthy = total - max_unavailable
            Ok(total_pods - max_unavailable_count)
        } else {
            // No min_available or max_unavailable specified - invalid PDB
            Err(rusternetes_common::Error::InvalidResource(
                "PodDisruptionBudget must specify either minAvailable or maxUnavailable"
                    .to_string(),
            ))
        }
    }

    /// Compute `expectedPods` by walking each pod's controller ownerReference
    /// up to a workload root and summing the workload sizes.
    ///
    /// Mirrors upstream `pkg/controller/disruption/disruption.go::
    /// getExpectedScale`. The upstream algorithm:
    ///
    ///   1. Bucket pods by their controller ownerReference UID. Pods with
    ///      no controller owner are bucketed under a sentinel "orphan" key.
    ///   2. For each unique controller, resolve a scale value:
    ///        - Well-known workload kinds (Deployment, StatefulSet,
    ///          ReplicaSet, ReplicationController) → read `spec.replicas`
    ///          from the workload object.
    ///        - ReplicaSet whose controller owner is a Deployment → bubble
    ///          up to the Deployment's `spec.replicas`.
    ///        - CRD kinds with a `scale` subresource → fetch the CR and
    ///          resolve `subresources.scale.specReplicasPath` against its
    ///          JSON body.
    ///        - Unknown / unresolvable → fall back to the pod count for
    ///          that owner (so we never UNDER-report).
    ///   3. Sum the scales. Orphan pods contribute their own count.
    ///
    /// Returns `None` only when the storage layer is unreachable — that
    /// case is treated as transient and the caller falls back to the raw
    /// pod count rather than failing the whole reconcile.
    async fn compute_expected_pods(&self, namespace: &str, matching_pods: &[Pod]) -> Option<i32> {
        // Group pods by their controller owner UID. Pods without a
        // controller owner are accumulated into `orphan_pods` and
        // contribute their raw count to expectedPods (one-per-pod), which
        // mirrors upstream's "no controller found" path in
        // `getExpectedScale`.
        let mut owners_by_uid: std::collections::HashMap<String, OwnerReference> =
            std::collections::HashMap::new();
        let mut pod_count_by_owner: std::collections::HashMap<String, i32> =
            std::collections::HashMap::new();
        let mut orphan_pods: i32 = 0;
        for pod in matching_pods {
            match controller_ref(pod) {
                Some(owner) => {
                    *pod_count_by_owner.entry(owner.uid.clone()).or_insert(0) += 1;
                    owners_by_uid
                        .entry(owner.uid.clone())
                        .or_insert_with(|| owner.clone());
                }
                None => orphan_pods += 1,
            }
        }

        // Dedupe scales by the **root** owner UID, not the pod's direct
        // owner UID. Upstream `getExpectedScale` does the same: when a
        // ReplicaSet bubbles up to its Deployment, the returned UID is
        // the Deployment's, so two RSes of the same Deployment collapse
        // to a single Deployment-scale entry rather than double-counting.
        let mut scale_by_root_uid: std::collections::HashMap<String, i32> =
            std::collections::HashMap::new();
        // `unresolved_pod_fallback` accumulates pod counts for owners
        // whose scale we could not determine (deleted workload, unknown
        // CRD, missing replicas field). These contribute their raw pod
        // count so we never under-report expectedPods.
        let mut unresolved_pod_fallback: i32 = 0;
        for (uid, owner) in &owners_by_uid {
            let pod_count = pod_count_by_owner.get(uid).copied().unwrap_or(0);
            let mut visited: HashSet<String> = HashSet::new();
            visited.insert(uid.clone());
            match self
                .resolve_owner_scale(namespace, owner, &mut visited)
                .await
            {
                Some((root_uid, scale)) => {
                    // Insert dedupes; if the same Deployment is reached
                    // via two different RSes we keep one entry.
                    scale_by_root_uid.insert(root_uid, scale);
                }
                None => unresolved_pod_fallback += pod_count,
            }
        }

        let total = orphan_pods + unresolved_pod_fallback + scale_by_root_uid.values().sum::<i32>();
        Some(total)
    }

    /// Resolve `(root_uid, replicas)` for a single controller ownerReference.
    ///
    /// `root_uid` is the UID we want the caller to dedupe by — for the
    /// built-in workloads it is the workload's own UID, except for a
    /// ReplicaSet whose controller owner is a Deployment, in which case
    /// it is the Deployment's UID (mirrors upstream
    /// `pkg/controller/disruption/disruption.go::getPodReplicaSet`).
    ///
    /// Returns `None` when:
    ///   - the owner could not be fetched (deleted / not yet stored)
    ///   - the owner kind is unknown AND no CRD with a scale subresource
    ///     matches its apiVersion+kind
    ///   - the scale path on a CRD-backed owner did not resolve to a
    ///     non-negative integer
    ///
    /// In any "unresolvable" case the caller falls back to the pod count
    /// for that owner, mirroring upstream's behaviour of never
    /// under-reporting `expectedPods`.
    async fn resolve_owner_scale(
        &self,
        namespace: &str,
        owner: &OwnerReference,
        visited: &mut HashSet<String>,
    ) -> Option<(String, i32)> {
        // Built-in workload kinds — read .spec.replicas directly.
        // Upstream uses the same hard-coded list because the dynamic
        // scale client is only consulted for kinds that DO NOT appear
        // here (`disruption.go::finders`).
        let key = match owner.kind.as_str() {
            "Deployment" => Some(build_key("deployments", Some(namespace), &owner.name)),
            "StatefulSet" => Some(build_key("statefulsets", Some(namespace), &owner.name)),
            "ReplicationController" => Some(build_key(
                "replicationcontrollers",
                Some(namespace),
                &owner.name,
            )),
            "ReplicaSet" => Some(build_key("replicasets", Some(namespace), &owner.name)),
            _ => None,
        };

        if let Some(key) = key {
            // Built-in workload — fetch as opaque JSON and read .spec.replicas.
            // Going through JSON keeps this function generic across the four
            // workload types without pulling a half-dozen typed structs.
            let workload: serde_json::Value = self.storage.get(&key).await.ok()?;
            // ReplicaSets owned by a Deployment must report the
            // Deployment's UID + scale (NOT the RS's), so two RSes of the
            // same Deployment dedupe to a single entry at the caller.
            if owner.kind == "ReplicaSet" {
                if let Some(parent) = controller_ref_from_json(&workload) {
                    if parent.kind == "Deployment" && !visited.contains(&parent.uid) {
                        visited.insert(parent.uid.clone());
                        if let Some((dep_uid, dep_scale)) =
                            Box::pin(self.resolve_owner_scale(namespace, &parent, visited)).await
                        {
                            return Some((dep_uid, dep_scale));
                        }
                    }
                }
            }
            let scale = workload
                .get("spec")
                .and_then(|s| s.get("replicas"))
                .and_then(|r| r.as_i64())
                .map(|r| r as i32)?;
            return Some((owner.uid.clone(), scale));
        }

        // CRD path: look up a CRD whose names.kind matches the owner kind
        // AND whose group matches the owner apiVersion's group. Then use
        // the CRD's subresources.scale.specReplicasPath to resolve scale
        // from the CR body.
        let (group, version) = split_group_version(&owner.api_version);
        let crd = self.find_crd(&group, &owner.kind).await?;
        let crd_version = crd.spec.versions.iter().find(|v| v.name == version)?;
        let scale = crd_version.subresources.as_ref()?.scale.as_ref()?;

        // Storage key for the CR mirrors the api-server convention:
        // `<group_with_underscores>_<plural>`. We fetch it as a generic
        // CustomResource (whose `spec` is a serde_json::Value) so we can
        // resolve the JSONPath without growing a schema dependency.
        let resource_type = format!("{}_{}", group.replace('.', "_"), crd.spec.names.plural);
        let cr_key = build_key(&resource_type, Some(namespace), &owner.name);
        let cr: CustomResource = self.storage.get(&cr_key).await.ok()?;

        // Build the JSON document from the CR's spec/status so the path
        // can address either side (`.spec.replicas`, `.status.replicas`,
        // ...). Upstream's scale client walks the same document shape.
        let mut doc = serde_json::Map::new();
        if let Some(s) = cr.spec {
            doc.insert("spec".to_string(), s);
        }
        if let Some(s) = cr.status {
            doc.insert("status".to_string(), s);
        }
        let doc = serde_json::Value::Object(doc);

        let scale_val = resolve_json_path(&doc, &scale.spec_replicas_path)
            .and_then(|v| v.as_i64().map(|i| i as i32))?;
        Some((owner.uid.clone(), scale_val))
    }

    /// Look up a CRD by group + kind (mirrors apiextensions discovery).
    /// Returns `None` if no matching CRD is registered.
    async fn find_crd(&self, group: &str, kind: &str) -> Option<CustomResourceDefinition> {
        let crds: Vec<CustomResourceDefinition> = self
            .storage
            .list("/registry/customresourcedefinitions/")
            .await
            .ok()?;
        crds.into_iter()
            .find(|c| c.spec.group == group && c.spec.names.kind == kind)
    }

    /// Check if a pod is healthy (Running and Ready)
    fn is_pod_healthy(&self, pod: &Pod) -> bool {
        // Check if pod is in Running phase
        let is_running = pod
            .status
            .as_ref()
            .map(|s| matches!(s.phase, Some(rusternetes_common::types::Phase::Running)))
            .unwrap_or(false);

        if !is_running {
            return false;
        }

        // Check if pod has Ready condition set to True
        // For simplicity, we'll consider a pod ready if it's Running
        // In a full implementation, we'd check pod.status.conditions for Ready=True
        true
    }

    /// Check if a pod matches the PDB selector.
    ///
    /// Mirrors upstream `apimachinery/pkg/apis/meta/v1.LabelSelectorAsSelector`
    /// + `labels.Selector.Matches`:
    ///
    ///   * An empty selector (`matchLabels` and `matchExpressions` both
    ///     empty/absent) matches everything — including pods with no labels at
    ///     all. `TestSelectorsForPodsWithoutLabels` pins this contract for the
    ///     current `policy/v1` API. The deprecated `policy/v1beta1` API had
    ///     the inverse meaning (empty selector matched NO pods) and upstream
    ///     `TestEmptySelector` keeps that compat shim alive — set
    ///     `empty_selector_matches_nothing = true` for v1beta1 PDBs.
    ///   * `matchLabels` entries are AND-combined and treated as exact-match.
    ///   * `matchExpressions` entries are AND-combined; operator semantics:
    ///       - `In`           — key present AND pod's value in `values`.
    ///       - `NotIn`        — key absent OR pod's value not in `values`.
    ///       - `Exists`       — key present.
    ///       - `DoesNotExist` — key absent (matches label-less pods).
    fn pod_matches_selector(
        &self,
        pod: &Pod,
        selector: &LabelSelector,
        empty_selector_matches_nothing: bool,
    ) -> bool {
        let pod_labels = pod.metadata.labels.as_ref();

        let match_labels_empty = selector
            .match_labels
            .as_ref()
            .map(|m| m.is_empty())
            .unwrap_or(true);
        let match_expressions_empty = selector
            .match_expressions
            .as_ref()
            .map(|m| m.is_empty())
            .unwrap_or(true);

        // Empty selector: v1 matches every pod, v1beta1 matches none.
        if match_labels_empty && match_expressions_empty {
            return !empty_selector_matches_nothing;
        }

        if let Some(match_labels) = &selector.match_labels {
            for (key, value) in match_labels {
                let got = pod_labels.and_then(|l| l.get(key));
                if got != Some(value) {
                    return false;
                }
            }
        }

        if let Some(match_expressions) = &selector.match_expressions {
            for req in match_expressions {
                let pod_value = pod_labels.and_then(|l| l.get(&req.key));
                let matched = match req.operator.as_str() {
                    "In" => match pod_value {
                        Some(v) => req
                            .values
                            .as_ref()
                            .map(|vals| vals.iter().any(|x| x == v))
                            .unwrap_or(false),
                        None => false,
                    },
                    "NotIn" => match pod_value {
                        Some(v) => req
                            .values
                            .as_ref()
                            .map(|vals| !vals.iter().any(|x| x == v))
                            .unwrap_or(true),
                        None => true,
                    },
                    "Exists" => pod_value.is_some(),
                    "DoesNotExist" => pod_value.is_none(),
                    other => {
                        debug!(
                            "unknown LabelSelector operator `{other}` on PDB selector \
                             (key={}); treating as non-match",
                            req.key
                        );
                        false
                    }
                };
                if !matched {
                    return false;
                }
            }
        }

        true
    }
}

/// Return the *controller* ownerReference of an object (the one with
/// `controller: true`). Mirrors upstream `metav1.GetControllerOf`.
fn controller_ref(pod: &Pod) -> Option<OwnerReference> {
    pod.metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.controller == Some(true)).cloned())
}

/// Same as [`controller_ref`] but reads from a raw JSON object so we can
/// chase ownership through workload types fetched as `serde_json::Value`.
fn controller_ref_from_json(obj: &serde_json::Value) -> Option<OwnerReference> {
    let refs = obj
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|r| r.as_array())?;
    for r in refs {
        if r.get("controller").and_then(|c| c.as_bool()) == Some(true) {
            return serde_json::from_value(r.clone()).ok();
        }
    }
    None
}

/// Split a Kubernetes apiVersion ("group/version" or just "version" for
/// core /api/v1) into its component parts. Core resources return an
/// empty group, matching upstream's `schema.ParseGroupVersion`.
fn split_group_version(api_version: &str) -> (String, String) {
    match api_version.split_once('/') {
        Some((g, v)) => (g.to_string(), v.to_string()),
        None => (String::new(), api_version.to_string()),
    }
}

/// Resolve a dot-prefixed JSONPath (e.g. `.spec.replicas`) against a
/// JSON object. Only supports the dotted-field subset that CRD
/// `specReplicasPath` / `statusReplicasPath` are allowed to use per the
/// apiextensions docs — no array indexing, no filters. The leading `.`
/// is required by the spec; we tolerate its absence for ergonomics.
fn resolve_json_path<'a>(doc: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let trimmed = path.strip_prefix('.').unwrap_or(path);
    let mut cur = doc;
    for segment in trimmed.split('.') {
        if segment.is_empty() {
            return None;
        }
        cur = cur.get(segment)?;
    }
    Some(cur)
}

/// Default `stalePodDisruptionTimeout` mirrored from upstream
/// `pkg/controller/disruption/disruption.go` — the disruption controller
/// flips a stale `DisruptionTarget=True` condition on a Running pod after
/// this many minutes.
pub const STALE_POD_DISRUPTION_TIMEOUT: StdDuration = StdDuration::from_secs(120);

/// Sub-controller that mirrors upstream
/// `pkg/controller/disruption/stalepoddisruption.go`. Periodically scans
/// pods carrying a `DisruptionTarget=True` condition and decides whether
/// to flip the condition to `False` (the original disruption never
/// completed) or leave it alone (the pod truly was disrupted).
///
/// Decision matrix matches upstream (`syncStalePodDisruption`):
///
/// | Pod state                              | Action                       |
/// |----------------------------------------|------------------------------|
/// | `deletionTimestamp` set (terminating)  | Preserve `True`              |
/// | `status.phase == Failed`               | Preserve `True` + reason     |
/// | `status.phase == Running` AND stale    | Set `False`                  |
/// | `status.phase == Running` AND fresh    | No-op (re-check on next tick)|
///
/// "Stale" means the condition's `lastTransitionTime` is older than
/// [`STALE_POD_DISRUPTION_TIMEOUT`].
pub struct StalePodDisruptionController<S: Storage> {
    storage: Arc<S>,
    timeout: StdDuration,
}

impl<S: Storage + 'static> StalePodDisruptionController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            timeout: STALE_POD_DISRUPTION_TIMEOUT,
        }
    }

    /// Test helper: install a custom timeout so the sub-controller can be
    /// driven deterministically without 120s of wall-clock waiting.
    #[allow(dead_code)]
    #[doc(hidden)]
    pub fn with_timeout(storage: Arc<S>, timeout: StdDuration) -> Self {
        Self { storage, timeout }
    }

    /// Periodic resync loop. Upstream rate-limits this work queue —
    /// rusternetes uses a fixed 30s tick which is good enough until the
    /// condition gets exercised by real workloads.
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(StdDuration::from_secs(30));
        interval.tick().await; // skip the immediate tick on startup
        loop {
            interval.tick().await;
            if let Err(e) = self.reconcile_all().await {
                error!("stale-pod-disruption reconcile_all failed: {}", e);
            }
        }
    }

    /// Walk every pod, fix the stale ones. Designed for tests to call
    /// directly against `Arc<MemoryStorage>` (no informers, no rate limit).
    pub async fn reconcile_all(&self) -> anyhow::Result<()> {
        let pods: Vec<Pod> = self.storage.list("/registry/pods/").await?;
        for pod in pods {
            if let Err(e) = self.reconcile_pod(&pod).await {
                warn!(
                    "stale-pod-disruption: failed to reconcile {}/{}: {}",
                    pod.metadata.namespace.as_deref().unwrap_or("?"),
                    pod.metadata.name,
                    e
                );
            }
        }
        Ok(())
    }

    async fn reconcile_pod(&self, pod: &Pod) -> anyhow::Result<()> {
        // Only act on pods that actually carry `DisruptionTarget=True`.
        let conditions = match pod.status.as_ref().and_then(|s| s.conditions.as_ref()) {
            Some(c) => c,
            None => return Ok(()),
        };
        let dt_idx = conditions
            .iter()
            .position(|c| c.condition_type == "DisruptionTarget" && c.status == "True");
        let dt_idx = match dt_idx {
            Some(i) => i,
            None => return Ok(()),
        };

        // Preserve `True` for terminating pods.
        if pod.metadata.deletion_timestamp.is_some() {
            debug!(
                "stale-pod-disruption: preserving DisruptionTarget=True on terminating pod {}/{}",
                pod.metadata.namespace.as_deref().unwrap_or("?"),
                pod.metadata.name
            );
            return Ok(());
        }

        // Preserve `True` for Failed pods (regardless of reason).
        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
        if matches!(phase, Some(Phase::Failed)) {
            debug!(
                "stale-pod-disruption: preserving DisruptionTarget=True on Failed pod {}/{}",
                pod.metadata.namespace.as_deref().unwrap_or("?"),
                pod.metadata.name
            );
            return Ok(());
        }

        // For Running pods, flip to False only after the timeout elapses.
        if !matches!(phase, Some(Phase::Running)) {
            return Ok(());
        }
        let last_transition: Option<DateTime<Utc>> = conditions[dt_idx].last_transition_time;
        let stale = match last_transition {
            Some(t) => {
                Utc::now().signed_duration_since(t)
                    >= chrono::Duration::from_std(self.timeout)
                        .unwrap_or(chrono::Duration::seconds(0))
            }
            None => true, // missing timestamp is treated as already stale
        };
        if !stale {
            return Ok(());
        }

        // Flip True → False. Re-read for fresh resourceVersion to avoid CAS
        // races with concurrent writers (the canonical in-repo pattern).
        let key = build_key(
            "pods",
            pod.metadata.namespace.as_deref(),
            &pod.metadata.name,
        );
        let mut fresh: Pod = self.storage.get(&key).await?;
        if let Some(status) = fresh.status.as_mut() {
            if let Some(conds) = status.conditions.as_mut() {
                if let Some(c) = conds
                    .iter_mut()
                    .find(|c| c.condition_type == "DisruptionTarget")
                {
                    if c.status == "True" {
                        c.status = "False".to_string();
                        c.last_transition_time = Some(Utc::now());
                    }
                }
            }
        }
        self.storage.update(&key, &fresh).await?;
        info!(
            "stale-pod-disruption: flipped DisruptionTarget True->False on Running pod {}/{}",
            pod.metadata.namespace.as_deref().unwrap_or("?"),
            pod.metadata.name
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Container, IntOrString, PodDisruptionBudgetSpec, PodSpec};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    #[test]
    fn test_resolve_json_path_simple() {
        let doc = serde_json::json!({"spec": {"replicas": 7, "nested": {"x": 1}}});
        assert_eq!(
            resolve_json_path(&doc, ".spec.replicas").and_then(|v| v.as_i64()),
            Some(7)
        );
        // Leading-dot stripping is just sugar — both should work identically.
        assert_eq!(
            resolve_json_path(&doc, "spec.replicas").and_then(|v| v.as_i64()),
            Some(7)
        );
        // Multi-level descent.
        assert_eq!(
            resolve_json_path(&doc, ".spec.nested.x").and_then(|v| v.as_i64()),
            Some(1)
        );
        // Missing path returns None (caller falls back to pod count).
        assert!(resolve_json_path(&doc, ".spec.missing").is_none());
        // Empty segment is malformed input; must not panic / silently match.
        assert!(resolve_json_path(&doc, ".spec..replicas").is_none());
    }

    #[test]
    fn test_split_group_version() {
        assert_eq!(
            split_group_version("apps/v1"),
            ("apps".to_string(), "v1".to_string())
        );
        // Core resources live under just "v1" (no group prefix).
        assert_eq!(split_group_version("v1"), (String::new(), "v1".to_string()));
        assert_eq!(
            split_group_version("example.com/v1beta1"),
            ("example.com".to_string(), "v1beta1".to_string())
        );
    }

    #[test]
    fn test_controller_ref_from_json_returns_only_controller() {
        // Pure ownerRef without `controller: true` MUST be ignored — upstream's
        // GetControllerOf is strict about this. Only the explicit controller
        // ref is the one we walk up from.
        let obj = serde_json::json!({
            "metadata": {
                "ownerReferences": [
                    {"apiVersion": "v1", "kind": "Pod", "name": "side", "uid": "1"},
                    {
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "main",
                        "uid": "2",
                        "controller": true
                    }
                ]
            }
        });
        let r = controller_ref_from_json(&obj).expect("controller present");
        assert_eq!(r.kind, "Deployment");
        assert_eq!(r.uid, "2");

        // No controller flag set anywhere → None.
        let obj2 = serde_json::json!({
            "metadata": {
                "ownerReferences": [
                    {"apiVersion": "v1", "kind": "Pod", "name": "side", "uid": "1"}
                ]
            }
        });
        assert!(controller_ref_from_json(&obj2).is_none());

        // No metadata at all → None.
        let obj3 = serde_json::json!({});
        assert!(controller_ref_from_json(&obj3).is_none());
    }

    #[tokio::test]
    async fn test_calculate_desired_healthy_min_available_int() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PodDisruptionBudgetController::new(storage);

        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(3)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("test-pdb", "default", spec);
        let desired = controller.calculate_desired_healthy(&pdb, 5).unwrap();
        assert_eq!(desired, 3);
    }

    #[tokio::test]
    async fn test_calculate_desired_healthy_min_available_percentage() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PodDisruptionBudgetController::new(storage);

        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::String("50%".to_string())),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("test-pdb", "default", spec);
        let desired = controller.calculate_desired_healthy(&pdb, 10).unwrap();
        assert_eq!(desired, 5); // 50% of 10 = 5
    }

    #[tokio::test]
    async fn test_calculate_desired_healthy_max_unavailable_int() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PodDisruptionBudgetController::new(storage);

        let spec = PodDisruptionBudgetSpec {
            min_available: None,
            max_unavailable: Some(IntOrString::Int(2)),
            selector: LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("test-pdb", "default", spec);
        let desired = controller.calculate_desired_healthy(&pdb, 5).unwrap();
        assert_eq!(desired, 3); // 5 - 2 = 3
    }

    #[tokio::test]
    async fn test_pod_matches_selector() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PodDisruptionBudgetController::new(storage);

        let mut pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pod"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test".to_string(),
                    image: "nginx".to_string(),
                    image_pull_policy: None,
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
                }],
                init_containers: None,
                restart_policy: None,
                node_selector: None,
                node_name: None,
                volumes: None,
                affinity: None,
                tolerations: None,
                service_account_name: None,
                service_account: None,
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
                ..Default::default()
            }),
            status: None,
        };

        pod.metadata.labels = Some(HashMap::from([
            ("app".to_string(), "web".to_string()),
            ("tier".to_string(), "frontend".to_string()),
        ]));

        let selector = LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
            match_expressions: None,
        };

        assert!(controller.pod_matches_selector(&pod, &selector, false));

        let selector_no_match = LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "api".to_string())])),
            match_expressions: None,
        };

        assert!(!controller.pod_matches_selector(&pod, &selector_no_match, false));
    }

    #[tokio::test]
    async fn test_is_pod_healthy() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PodDisruptionBudgetController::new(storage);

        let mut pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pod"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test".to_string(),
                    image: "nginx".to_string(),
                    image_pull_policy: None,
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
                }],
                init_containers: None,
                restart_policy: None,
                node_selector: None,
                node_name: None,
                volumes: None,
                affinity: None,
                tolerations: None,
                service_account_name: None,
                service_account: None,
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
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
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
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        };

        assert!(controller.is_pod_healthy(&pod));

        // Test with Pending pod
        if let Some(ref mut status) = pod.status {
            status.phase = Some(Phase::Pending);
        }
        assert!(!controller.is_pod_healthy(&pod));
    }
}
