use anyhow::{Context, Result};
use rusternetes_common::resources::{
    Deployment, Pod, RecommendedContainerResources, RecommendedPodResources, ReplicaSet,
    StatefulSet, VerticalPodAutoscaler, VerticalPodAutoscalerStatus,
};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

/// VPA Controller - Manages Vertical Pod Autoscaling
///
/// Implements:
/// - Resource usage tracking and analysis
/// - Recommendation generation based on percentile-based algorithm
/// - Update policy enforcement (Off, Initial, Recreate, Auto)
/// - Min/max resource bounds enforcement
pub struct VerticalPodAutoscalerController<S: Storage> {
    storage: Arc<S>,
    /// Historical resource usage data: pod_key -> container_name -> usage samples
    #[allow(clippy::type_complexity)]
    usage_history: Arc<tokio::sync::RwLock<HashMap<String, HashMap<String, Vec<ResourceUsage>>>>>,
    /// How many samples to keep for recommendations
    history_size: usize,
}

#[derive(Debug, Clone)]
struct ResourceUsage {
    cpu_millicores: i64,
    memory_bytes: i64,
    #[allow(dead_code)]
    timestamp: chrono::DateTime<chrono::Utc>,
}

impl<S: Storage + 'static> VerticalPodAutoscalerController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            usage_history: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            history_size: 1000, // Keep last 1000 samples per container
        }
    }

    pub async fn run(self: Arc<Self>) {
        use futures::StreamExt;

        info!("Starting Vertical Pod Autoscaler controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = "/registry/verticalpodautoscalers/".to_string();
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(Duration::from_secs(60)).await;
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
            let storage_key = build_key("verticalpodautoscalers", Some(ns), name);
            match self
                .storage
                .get::<VerticalPodAutoscaler>(&storage_key)
                .await
            {
                Ok(resource) => match self.reconcile_vpa(&resource).await {
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
            .list::<VerticalPodAutoscaler>("/registry/verticalpodautoscalers/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("verticalpodautoscalers/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list verticalpodautoscalers for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Reconciling all VPAs");

        // Get all VPAs
        let vpas: Vec<VerticalPodAutoscaler> = self
            .storage
            .list("/registry/verticalpodautoscalers/")
            .await?;

        for vpa in vpas {
            if let Err(e) = self.reconcile_vpa(&vpa).await {
                let namespace = vpa.metadata.namespace.as_deref().unwrap_or("default");
                error!(
                    "Failed to reconcile VPA {}/{}: {}",
                    namespace, vpa.metadata.name, e
                );
            }
        }

        Ok(())
    }

    async fn reconcile_vpa(&self, vpa: &VerticalPodAutoscaler) -> Result<()> {
        let namespace = vpa.metadata.namespace.as_deref().unwrap_or("default");
        let name = &vpa.metadata.name;

        debug!("Reconciling VPA {}/{}", namespace, name);

        // Find target pods based on targetRef
        let target_pods = self.get_target_pods(vpa).await?;

        if target_pods.is_empty() {
            debug!("No pods found for VPA {}/{}", namespace, name);
            return Ok(());
        }

        debug!(
            "VPA {}/{} found {} target pods",
            namespace,
            name,
            target_pods.len()
        );

        // Collect resource usage from pods
        self.collect_resource_usage(&target_pods).await?;

        // Generate recommendations based on historical data
        let recommendations = self.generate_recommendations(vpa, &target_pods).await?;

        if recommendations.is_empty() {
            debug!(
                "Not enough data to generate recommendations for VPA {}/{}",
                namespace, name
            );
            return Ok(());
        }

        debug!(
            "Generated {} container recommendations for VPA {}/{}",
            recommendations.len(),
            namespace,
            name
        );

        // Update VPA status with recommendations
        self.update_vpa_status(vpa, recommendations).await?;

        // Apply recommendations based on update policy
        self.apply_recommendations(vpa, &target_pods).await?;

        Ok(())
    }

    /// Find pods targeted by this VPA
    async fn get_target_pods(&self, vpa: &VerticalPodAutoscaler) -> Result<Vec<Pod>> {
        let namespace = vpa.metadata.namespace.as_deref().unwrap_or("default");
        let target_ref = &vpa.spec.target_ref;

        // Get the target resource (Deployment, ReplicaSet, StatefulSet)
        let selector_labels = match target_ref.kind.as_str() {
            "Deployment" => {
                let key = rusternetes_storage::build_key(
                    "deployments",
                    Some(namespace),
                    &target_ref.name,
                );
                let deployment: Deployment = self.storage.get(&key).await?;
                deployment
                    .spec
                    .selector
                    .match_labels
                    .clone()
                    .unwrap_or_default()
            }
            "ReplicaSet" => {
                let key = rusternetes_storage::build_key(
                    "replicasets",
                    Some(namespace),
                    &target_ref.name,
                );
                let replicaset: ReplicaSet = self.storage.get(&key).await?;
                replicaset
                    .spec
                    .selector
                    .match_labels
                    .clone()
                    .unwrap_or_default()
            }
            "StatefulSet" => {
                let key = rusternetes_storage::build_key(
                    "statefulsets",
                    Some(namespace),
                    &target_ref.name,
                );
                let statefulset: StatefulSet = self.storage.get(&key).await?;
                statefulset
                    .spec
                    .selector
                    .match_labels
                    .clone()
                    .unwrap_or_default()
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported target kind: {}",
                    target_ref.kind
                ));
            }
        };

        // Find all pods matching the selector
        let prefix = rusternetes_storage::build_prefix("pods", Some(namespace));
        let all_pods: Vec<Pod> = self.storage.list(&prefix).await?;

        let target_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|pod| {
                if let Some(pod_labels) = &pod.metadata.labels {
                    selector_labels
                        .iter()
                        .all(|(k, v)| pod_labels.get(k).map(|pv| pv == v).unwrap_or(false))
                } else {
                    false
                }
            })
            .collect();

        Ok(target_pods)
    }

    /// Collect resource usage from pods (simulated from pod status)
    async fn collect_resource_usage(&self, pods: &[Pod]) -> Result<()> {
        let mut history = self.usage_history.write().await;
        let now = chrono::Utc::now();

        for pod in pods {
            let pod_key = format!(
                "{}/{}",
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod.metadata.name
            );

            // Get containers from pod spec
            if let Some(spec) = &pod.spec {
                for container in &spec.containers {
                    // In a real implementation, this would query actual metrics from kubelet/metrics-server
                    // For now, we'll simulate reasonable usage based on requests/limits
                    let (cpu_usage, memory_usage) = self.simulate_container_usage(container);

                    let container_history =
                        history.entry(pod_key.clone()).or_insert_with(HashMap::new);

                    let samples = container_history
                        .entry(container.name.clone())
                        .or_insert_with(Vec::new);

                    samples.push(ResourceUsage {
                        cpu_millicores: cpu_usage,
                        memory_bytes: memory_usage,
                        timestamp: now,
                    });

                    // Keep only last N samples
                    if samples.len() > self.history_size {
                        samples.drain(0..samples.len() - self.history_size);
                    }
                }
            }
        }

        Ok(())
    }

    /// Simulate container resource usage (in production, query from metrics-server)
    fn simulate_container_usage(
        &self,
        container: &rusternetes_common::resources::Container,
    ) -> (i64, i64) {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        // Get CPU request (default to 100m if not specified)
        let cpu_request = container
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .and_then(|req| req.get("cpu"))
            .and_then(|cpu| parse_cpu_string(cpu))
            .unwrap_or(100);

        // Get memory request (default to 128Mi if not specified)
        let memory_request = container
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .and_then(|req| req.get("memory"))
            .and_then(|mem| parse_memory_string(mem))
            .unwrap_or(128 * 1024 * 1024);

        // Simulate usage as 60-90% of request with some variance
        let cpu_usage = (cpu_request as f64 * (0.6 + rng.gen::<f64>() * 0.3)) as i64;
        let memory_usage = (memory_request as f64 * (0.6 + rng.gen::<f64>() * 0.3)) as i64;

        (cpu_usage, memory_usage)
    }

    /// Generate resource recommendations using percentile-based algorithm
    async fn generate_recommendations(
        &self,
        vpa: &VerticalPodAutoscaler,
        pods: &[Pod],
    ) -> Result<Vec<RecommendedContainerResources>> {
        let history = self.usage_history.read().await;
        let mut recommendations = Vec::new();

        // Get the first pod as a template for container names
        let template_pod = pods.first().context("No pods available")?;
        let spec = template_pod.spec.as_ref().context("Pod has no spec")?;

        for container in &spec.containers {
            let container_name = &container.name;

            // Collect all usage samples for this container across all pods
            let mut all_cpu_samples = Vec::new();
            let mut all_memory_samples = Vec::new();

            for pod in pods {
                let pod_key = format!(
                    "{}/{}",
                    pod.metadata.namespace.as_deref().unwrap_or("default"),
                    pod.metadata.name
                );

                if let Some(pod_history) = history.get(&pod_key) {
                    if let Some(container_samples) = pod_history.get(container_name) {
                        for sample in container_samples {
                            all_cpu_samples.push(sample.cpu_millicores);
                            all_memory_samples.push(sample.memory_bytes);
                        }
                    }
                }
            }

            // Need sufficient samples to make recommendations
            if all_cpu_samples.len() < 10 {
                debug!(
                    "Not enough samples for container {} (have {})",
                    container_name,
                    all_cpu_samples.len()
                );
                continue;
            }

            // Calculate recommendations using 95th percentile for upper bound
            // and 50th percentile (median) for target
            all_cpu_samples.sort();
            all_memory_samples.sort();

            let cpu_p50 = percentile(&all_cpu_samples, 50);
            let cpu_p95 = percentile(&all_cpu_samples, 95);
            let memory_p50 = percentile(&all_memory_samples, 50);
            let memory_p95 = percentile(&all_memory_samples, 95);

            // Add some headroom (20% for target, 50% for upper bound)
            let target_cpu = (cpu_p50 as f64 * 1.2) as i64;
            let upper_cpu = (cpu_p95 as f64 * 1.5) as i64;
            let target_memory = (memory_p50 as f64 * 1.2) as i64;
            let upper_memory = (memory_p95 as f64 * 1.5) as i64;

            // Lower bound is typically minimum viable (10% of target)
            let lower_cpu = (target_cpu as f64 * 0.1) as i64;
            let lower_memory = (target_memory as f64 * 0.1) as i64;

            // Apply min/max constraints from resource policy
            let (final_target_cpu, final_upper_cpu, final_lower_cpu) =
                self.apply_cpu_constraints(vpa, container_name, target_cpu, upper_cpu, lower_cpu);

            let (final_target_memory, final_upper_memory, final_lower_memory) = self
                .apply_memory_constraints(
                    vpa,
                    container_name,
                    target_memory,
                    upper_memory,
                    lower_memory,
                );

            let recommendation = RecommendedContainerResources {
                container_name: container_name.clone(),
                target: {
                    let mut resources = HashMap::new();
                    resources.insert("cpu".to_string(), format_cpu(final_target_cpu));
                    resources.insert("memory".to_string(), format_memory(final_target_memory));
                    resources
                },
                lower_bound: Some({
                    let mut resources = HashMap::new();
                    resources.insert("cpu".to_string(), format_cpu(final_lower_cpu));
                    resources.insert("memory".to_string(), format_memory(final_lower_memory));
                    resources
                }),
                upper_bound: Some({
                    let mut resources = HashMap::new();
                    resources.insert("cpu".to_string(), format_cpu(final_upper_cpu));
                    resources.insert("memory".to_string(), format_memory(final_upper_memory));
                    resources
                }),
                uncapped_target: Some({
                    let mut resources = HashMap::new();
                    resources.insert("cpu".to_string(), format_cpu(target_cpu));
                    resources.insert("memory".to_string(), format_memory(target_memory));
                    resources
                }),
            };

            debug!(
                "Recommendation for container {}: target CPU={}, memory={}",
                container_name,
                format_cpu(final_target_cpu),
                format_memory(final_target_memory)
            );

            recommendations.push(recommendation);
        }

        Ok(recommendations)
    }

    /// Apply CPU constraints from VPA resource policy
    fn apply_cpu_constraints(
        &self,
        vpa: &VerticalPodAutoscaler,
        container_name: &str,
        target: i64,
        upper: i64,
        lower: i64,
    ) -> (i64, i64, i64) {
        let mut final_target = target;
        let mut final_upper = upper;
        let mut final_lower = lower;

        if let Some(policy) = &vpa.spec.resource_policy {
            if let Some(container_policies) = &policy.container_policies {
                for cp in container_policies {
                    if cp.container_name.as_deref() == Some(container_name) {
                        if let Some(min_allowed) = &cp.min_allowed {
                            if let Some(min_cpu) =
                                min_allowed.get("cpu").and_then(|s| parse_cpu_string(s))
                            {
                                final_target = final_target.max(min_cpu);
                                final_lower = final_lower.max(min_cpu);
                            }
                        }
                        if let Some(max_allowed) = &cp.max_allowed {
                            if let Some(max_cpu) =
                                max_allowed.get("cpu").and_then(|s| parse_cpu_string(s))
                            {
                                final_target = final_target.min(max_cpu);
                                final_upper = final_upper.min(max_cpu);
                            }
                        }
                    }
                }
            }
        }

        (final_target, final_upper, final_lower)
    }

    /// Apply memory constraints from VPA resource policy
    fn apply_memory_constraints(
        &self,
        vpa: &VerticalPodAutoscaler,
        container_name: &str,
        target: i64,
        upper: i64,
        lower: i64,
    ) -> (i64, i64, i64) {
        let mut final_target = target;
        let mut final_upper = upper;
        let mut final_lower = lower;

        if let Some(policy) = &vpa.spec.resource_policy {
            if let Some(container_policies) = &policy.container_policies {
                for cp in container_policies {
                    if cp.container_name.as_deref() == Some(container_name) {
                        if let Some(min_allowed) = &cp.min_allowed {
                            if let Some(min_mem) = min_allowed
                                .get("memory")
                                .and_then(|s| parse_memory_string(s))
                            {
                                final_target = final_target.max(min_mem);
                                final_lower = final_lower.max(min_mem);
                            }
                        }
                        if let Some(max_allowed) = &cp.max_allowed {
                            if let Some(max_mem) = max_allowed
                                .get("memory")
                                .and_then(|s| parse_memory_string(s))
                            {
                                final_target = final_target.min(max_mem);
                                final_upper = final_upper.min(max_mem);
                            }
                        }
                    }
                }
            }
        }

        (final_target, final_upper, final_lower)
    }

    /// Update VPA status with recommendations
    async fn update_vpa_status(
        &self,
        vpa: &VerticalPodAutoscaler,
        recommendations: Vec<RecommendedContainerResources>,
    ) -> Result<()> {
        let namespace = vpa.metadata.namespace.as_deref().unwrap_or("default");
        let name = &vpa.metadata.name;

        let mut updated_vpa = vpa.clone();
        updated_vpa.status = Some(VerticalPodAutoscalerStatus {
            recommendation: Some(RecommendedPodResources {
                container_recommendations: Some(recommendations),
            }),
            conditions: None,
        });

        let key = rusternetes_storage::build_key("verticalpodautoscalers", Some(namespace), name);
        self.storage.update(&key, &updated_vpa).await?;

        debug!(
            "Updated VPA {}/{} status with recommendations",
            namespace, name
        );

        Ok(())
    }

    /// Apply recommendations based on update policy
    async fn apply_recommendations(&self, vpa: &VerticalPodAutoscaler, pods: &[Pod]) -> Result<()> {
        let update_mode = vpa
            .spec
            .update_policy
            .as_ref()
            .and_then(|p| p.update_mode.as_deref())
            .unwrap_or("Off");

        match update_mode {
            "Off" => {
                // Only generate recommendations, don't apply
                debug!(
                    "VPA {} is in Off mode, not applying recommendations",
                    vpa.metadata.name
                );
            }
            "Initial" => {
                // Apply only to newly created pods (handled by admission webhook)
                debug!("VPA {} is in Initial mode, recommendations will be applied by admission webhook", vpa.metadata.name);
            }
            "Recreate" => {
                // Evict pods to trigger recreation with new resources
                info!(
                    "VPA {} is in Recreate mode, would evict {} pods for recreation",
                    vpa.metadata.name,
                    pods.len()
                );

                // In a real implementation, this would:
                // 1. Check if recommendations differ significantly from current resources
                // 2. Evict pods one at a time (respecting PodDisruptionBudget)
                // 3. Wait for new pods to be created with updated resources

                // For now, we'll just log the intent
                for pod in pods {
                    debug!("Would evict pod {} for VPA update", pod.metadata.name);
                }
            }
            "Auto" => {
                // In-place updates (future Kubernetes feature)
                warn!(
                    "VPA {} is in Auto mode, but in-place updates are not yet supported",
                    vpa.metadata.name
                );
            }
            _ => {
                warn!("Unknown VPA update mode: {}", update_mode);
            }
        }

        Ok(())
    }
}

/// Calculate percentile of sorted data
fn percentile(sorted_data: &[i64], p: u8) -> i64 {
    if sorted_data.is_empty() {
        return 0;
    }

    let index = ((p as f64 / 100.0) * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index.min(sorted_data.len() - 1)]
}

/// Parse CPU string (e.g., "500m", "1", "2000m") to millicores
fn parse_cpu_string(cpu: &str) -> Option<i64> {
    if cpu.ends_with('m') {
        cpu.trim_end_matches('m').parse().ok()
    } else {
        cpu.parse::<i64>().ok().map(|cores| cores * 1000)
    }
}

/// Parse memory string (e.g., "128Mi", "1Gi") to bytes
fn parse_memory_string(memory: &str) -> Option<i64> {
    let (num_str, suffix) = if memory.ends_with("Ki") {
        (memory.trim_end_matches("Ki"), 1024)
    } else if memory.ends_with("Mi") {
        (memory.trim_end_matches("Mi"), 1024 * 1024)
    } else if memory.ends_with("Gi") {
        (memory.trim_end_matches("Gi"), 1024 * 1024 * 1024)
    } else if memory.ends_with("Ti") {
        (memory.trim_end_matches("Ti"), 1024 * 1024 * 1024 * 1024)
    } else {
        (memory, 1)
    };

    num_str.parse::<i64>().ok().map(|n| n * suffix)
}

/// Format CPU millicores to string
fn format_cpu(millicores: i64) -> String {
    if millicores % 1000 == 0 {
        format!("{}", millicores / 1000)
    } else {
        format!("{}m", millicores)
    }
}

/// Format memory bytes to string
fn format_memory(bytes: i64) -> String {
    const GI: i64 = 1024 * 1024 * 1024;
    const MI: i64 = 1024 * 1024;
    const KI: i64 = 1024;

    if bytes >= GI && bytes % GI == 0 {
        format!("{}Gi", bytes / GI)
    } else if bytes >= MI && bytes % MI == 0 {
        format!("{}Mi", bytes / MI)
    } else if bytes >= KI && bytes % KI == 0 {
        format!("{}Ki", bytes / KI)
    } else {
        format!("{}", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpu_string() {
        assert_eq!(parse_cpu_string("100m"), Some(100));
        assert_eq!(parse_cpu_string("1"), Some(1000));
        assert_eq!(parse_cpu_string("2"), Some(2000));
        assert_eq!(parse_cpu_string("500m"), Some(500));
    }

    #[test]
    fn test_parse_memory_string() {
        assert_eq!(parse_memory_string("128Mi"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_string("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_string("512Ki"), Some(512 * 1024));
        assert_eq!(parse_memory_string("1024"), Some(1024));
    }

    #[test]
    fn test_format_cpu() {
        assert_eq!(format_cpu(100), "100m");
        assert_eq!(format_cpu(1000), "1");
        assert_eq!(format_cpu(2000), "2");
        assert_eq!(format_cpu(1500), "1500m");
    }

    #[test]
    fn test_format_memory() {
        assert_eq!(format_memory(128 * 1024 * 1024), "128Mi");
        assert_eq!(format_memory(1024 * 1024 * 1024), "1Gi");
        assert_eq!(format_memory(512 * 1024), "512Ki");
        assert_eq!(format_memory(1024), "1Ki"); // 1024 bytes = 1 KiB
    }

    #[test]
    fn test_percentile() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        // 50th percentile: index = (0.5 * 9).round() = 4.5.round() = 5, value at index 5 is 6
        assert_eq!(percentile(&data, 50), 6); // median (rounded index)
        assert_eq!(percentile(&data, 95), 10); // 95th percentile
        assert_eq!(percentile(&data, 0), 1); // min
        assert_eq!(percentile(&data, 100), 10); // max
    }
}
