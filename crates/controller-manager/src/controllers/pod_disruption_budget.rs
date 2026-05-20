use chrono::{DateTime, Utc};
use rusternetes_common::resources::{
    IntOrString, Pod, PodDisruptionBudget, PodDisruptionBudgetStatus,
};
use rusternetes_common::types::{LabelSelector, Phase};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
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

        // 3. Count healthy vs unhealthy pods
        let total_pods = matching_pods.len() as i32;
        let healthy_pods = matching_pods
            .iter()
            .filter(|p| self.is_pod_healthy(p))
            .count() as i32;

        debug!(
            "PDB {}/{}: total={}, healthy={}",
            namespace, pdb.metadata.name, total_pods, healthy_pods
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
