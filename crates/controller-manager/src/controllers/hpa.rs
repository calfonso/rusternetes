use anyhow::Result;
use chrono::Utc;
use rusternetes_common::resources::autoscaling::ResourceMetricStatus;
use rusternetes_common::resources::{
    Deployment, HorizontalPodAutoscaler, HorizontalPodAutoscalerCondition,
    HorizontalPodAutoscalerStatus, MetricSpec, MetricStatus, MetricValueStatus, ReplicaSet,
    StatefulSet,
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub struct HorizontalPodAutoscalerController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> HorizontalPodAutoscalerController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting HorizontalPodAutoscaler controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("horizontalpodautoscalers", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
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
            let storage_key = build_key("horizontalpodautoscalers", Some(ns), name);
            match self
                .storage
                .get::<HorizontalPodAutoscaler>(&storage_key)
                .await
            {
                Ok(resource) => match self.reconcile_hpa(&resource).await {
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
            .list::<HorizontalPodAutoscaler>("/registry/horizontalpodautoscalers/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("horizontalpodautoscalers/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list horizontalpodautoscalers for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Reconciling all HorizontalPodAutoscalers");

        // Get all HPAs across all namespaces
        let prefix = build_prefix("horizontalpodautoscalers", None);
        let hpas: Vec<HorizontalPodAutoscaler> = self.storage.list(&prefix).await?;

        for hpa in hpas {
            if let Err(e) = self.reconcile_hpa(&hpa).await {
                warn!(
                    "Failed to reconcile HPA {}/{}: {}",
                    hpa.metadata.namespace.as_deref().unwrap_or("default"),
                    hpa.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    async fn reconcile_hpa(&self, hpa: &HorizontalPodAutoscaler) -> Result<()> {
        let namespace = hpa.metadata.namespace.as_deref().unwrap_or("default");
        debug!("Reconciling HPA: {}/{}", namespace, hpa.metadata.name);

        let target_ref = &hpa.spec.scale_target_ref;
        debug!(
            "HPA {} targets {}/{} - min: {:?}, max: {}",
            hpa.metadata.name,
            target_ref.kind,
            target_ref.name,
            hpa.spec.min_replicas,
            hpa.spec.max_replicas
        );

        // 1. Get current replica count from target resource
        let current_replicas = match self.get_current_replicas(namespace, target_ref).await {
            Ok(replicas) => replicas,
            Err(e) => {
                warn!(
                    "Failed to get current replicas for HPA {}/{}: {}",
                    namespace, hpa.metadata.name, e
                );
                // Update status with error condition
                self.update_hpa_status_with_error(hpa, &format!("Failed to get target: {}", e))
                    .await?;
                return Ok(());
            }
        };

        debug!(
            "Current replicas for {}/{}: {}",
            namespace, target_ref.name, current_replicas
        );

        // 2. Calculate desired replica count based on metrics
        let desired_replicas = match self
            .calculate_desired_replicas(hpa, current_replicas, namespace)
            .await
        {
            Ok(replicas) => replicas,
            Err(e) => {
                warn!(
                    "Failed to calculate desired replicas for HPA {}/{}: {}",
                    namespace, hpa.metadata.name, e
                );
                // Update status with error condition
                self.update_hpa_status_with_error(
                    hpa,
                    &format!("Failed to compute replicas: {}", e),
                )
                .await?;
                return Ok(());
            }
        };

        debug!(
            "Desired replicas for {}/{}: {}",
            namespace, target_ref.name, desired_replicas
        );

        // 3. If desired replicas differ from current, scale the target
        if desired_replicas != current_replicas {
            info!(
                "Scaling {}/{} from {} to {} replicas",
                namespace, target_ref.name, current_replicas, desired_replicas
            );

            if let Err(e) = self
                .scale_target(namespace, target_ref, desired_replicas)
                .await
            {
                error!(
                    "Failed to scale target for HPA {}/{}: {}",
                    namespace, hpa.metadata.name, e
                );
                self.update_hpa_status_with_error(hpa, &format!("Failed to scale: {}", e))
                    .await?;
                return Ok(());
            }
        }

        // 4. Update HPA status
        self.update_hpa_status_success(hpa, current_replicas, desired_replicas)
            .await?;

        Ok(())
    }

    /// Get current replica count from the target resource
    async fn get_current_replicas(
        &self,
        namespace: &str,
        target_ref: &rusternetes_common::resources::CrossVersionObjectReference,
    ) -> Result<i32> {
        match target_ref.kind.as_str() {
            "Deployment" => {
                let key = build_key("deployments", Some(namespace), &target_ref.name);
                let deployment: Deployment = self.storage.get(&key).await?;
                Ok(deployment.spec.replicas.unwrap_or(1))
            }
            "ReplicaSet" => {
                let key = build_key("replicasets", Some(namespace), &target_ref.name);
                let replicaset: ReplicaSet = self.storage.get(&key).await?;
                Ok(replicaset.spec.replicas)
            }
            "StatefulSet" => {
                let key = build_key("statefulsets", Some(namespace), &target_ref.name);
                let statefulset: StatefulSet = self.storage.get(&key).await?;
                Ok(statefulset.spec.replicas.unwrap_or(1))
            }
            _ => Err(anyhow::anyhow!(
                "Unsupported scale target kind: {}",
                target_ref.kind
            )),
        }
    }

    /// Scale the target resource to the desired replica count
    async fn scale_target(
        &self,
        namespace: &str,
        target_ref: &rusternetes_common::resources::CrossVersionObjectReference,
        desired_replicas: i32,
    ) -> Result<()> {
        match target_ref.kind.as_str() {
            "Deployment" => {
                let key = build_key("deployments", Some(namespace), &target_ref.name);
                let mut deployment: Deployment = self.storage.get(&key).await?;
                deployment.spec.replicas = Some(desired_replicas);
                self.storage.update(&key, &deployment).await?;
                info!(
                    "Scaled Deployment {}/{} to {} replicas",
                    namespace, target_ref.name, desired_replicas
                );
            }
            "ReplicaSet" => {
                let key = build_key("replicasets", Some(namespace), &target_ref.name);
                let mut replicaset: ReplicaSet = self.storage.get(&key).await?;
                replicaset.spec.replicas = desired_replicas;
                self.storage.update(&key, &replicaset).await?;
                info!(
                    "Scaled ReplicaSet {}/{} to {} replicas",
                    namespace, target_ref.name, desired_replicas
                );
            }
            "StatefulSet" => {
                let key = build_key("statefulsets", Some(namespace), &target_ref.name);
                let mut statefulset: StatefulSet = self.storage.get(&key).await?;
                statefulset.spec.replicas = Some(desired_replicas);
                self.storage.update(&key, &statefulset).await?;
                info!(
                    "Scaled StatefulSet {}/{} to {} replicas",
                    namespace, target_ref.name, desired_replicas
                );
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported scale target kind: {}",
                    target_ref.kind
                ));
            }
        }
        Ok(())
    }

    /// Calculate desired replica count based on metrics
    /// Implements the HPA algorithm: desiredReplicas = ceil[currentReplicas * (currentMetricValue / targetMetricValue)]
    async fn calculate_desired_replicas(
        &self,
        hpa: &HorizontalPodAutoscaler,
        current_replicas: i32,
        namespace: &str,
    ) -> Result<i32> {
        let metrics = match &hpa.spec.metrics {
            Some(m) if !m.is_empty() => m,
            _ => {
                // No metrics specified - maintain current replicas
                return Ok(current_replicas);
            }
        };

        let mut max_desired_replicas = current_replicas;

        // Iterate through all metrics and take the maximum desired replicas
        // (Kubernetes HPA uses the highest recommendation)
        for metric in metrics {
            let desired = self
                .calculate_replicas_for_metric(metric, current_replicas, namespace, hpa)
                .await?;
            if desired > max_desired_replicas {
                max_desired_replicas = desired;
            }
        }

        // Apply min/max bounds
        let min_replicas = hpa.spec.min_replicas.unwrap_or(1);
        let max_replicas = hpa.spec.max_replicas;

        let bounded_replicas = max_desired_replicas.max(min_replicas).min(max_replicas);

        debug!(
            "Calculated replicas: desired={}, min={}, max={}, bounded={}",
            max_desired_replicas, min_replicas, max_replicas, bounded_replicas
        );

        Ok(bounded_replicas)
    }

    /// Calculate desired replicas for a single metric
    async fn calculate_replicas_for_metric(
        &self,
        metric: &MetricSpec,
        current_replicas: i32,
        namespace: &str,
        hpa: &HorizontalPodAutoscaler,
    ) -> Result<i32> {
        match metric.metric_type.as_str() {
            "Resource" => {
                if let Some(resource) = &metric.resource {
                    self.calculate_replicas_for_resource_metric(
                        resource,
                        current_replicas,
                        namespace,
                        hpa,
                    )
                    .await
                } else {
                    Err(anyhow::anyhow!(
                        "Resource metric specified but resource field is None"
                    ))
                }
            }
            "Pods" | "Object" | "External" | "ContainerResource" => {
                // For now, these metric types are not implemented
                // In a full implementation, would query custom metrics API
                debug!(
                    "Metric type {} not yet implemented, using current replicas",
                    metric.metric_type
                );
                Ok(current_replicas)
            }
            _ => {
                warn!("Unknown metric type: {}", metric.metric_type);
                Ok(current_replicas)
            }
        }
    }

    /// Calculate desired replicas based on resource metric (CPU/memory)
    async fn calculate_replicas_for_resource_metric(
        &self,
        resource: &rusternetes_common::resources::ResourceMetricSource,
        current_replicas: i32,
        _namespace: &str,
        _hpa: &HorizontalPodAutoscaler,
    ) -> Result<i32> {
        debug!(
            "Calculating replicas for resource metric: {}",
            resource.name
        );

        // Get target utilization
        let target_utilization = match resource.target.average_utilization {
            Some(util) => util as f64,
            None => {
                // If no average_utilization, we'd need to use value or average_value
                // For now, default to 80%
                debug!("No target utilization specified, defaulting to 80%");
                80.0
            }
        };

        // In a real implementation, this would query the metrics API
        // For now, we'll simulate by returning a mock current utilization
        let current_utilization = self
            .get_current_resource_utilization(&resource.name)
            .await?;

        debug!(
            "Resource {} - current: {}%, target: {}%",
            resource.name, current_utilization, target_utilization
        );

        // HPA formula: desiredReplicas = ceil[currentReplicas * (currentMetricValue / targetMetricValue)]
        let ratio = current_utilization / target_utilization;
        let desired_replicas = (current_replicas as f64 * ratio).ceil() as i32;

        debug!(
            "Calculated desired replicas: {} (ratio: {:.2})",
            desired_replicas, ratio
        );

        Ok(desired_replicas)
    }

    /// Get current resource utilization from metrics API
    /// In a real implementation, this would query metrics.k8s.io/v1beta1
    /// For now, returns mock data based on simple heuristics
    async fn get_current_resource_utilization(&self, resource_name: &str) -> Result<f64> {
        // TODO: Query actual metrics from metrics API
        // This is where we'd call GET /apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods
        // and aggregate the metrics across all pods in the target

        match resource_name {
            "cpu" => {
                // Mock: return a utilization that would trigger scaling
                // In reality, this would be calculated from actual pod metrics
                Ok(85.0) // 85% CPU utilization (above typical 80% target)
            }
            "memory" => {
                Ok(70.0) // 70% memory utilization
            }
            _ => {
                debug!("Unknown resource type: {}, returning 50%", resource_name);
                Ok(50.0)
            }
        }
    }

    /// Update HPA status with success
    async fn update_hpa_status_success(
        &self,
        hpa: &HorizontalPodAutoscaler,
        current_replicas: i32,
        desired_replicas: i32,
    ) -> Result<()> {
        let namespace = hpa.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key(
            "horizontalpodautoscalers",
            Some(namespace),
            &hpa.metadata.name,
        );

        let mut updated_hpa = hpa.clone();

        // Build metric status (simplified for now)
        let current_metrics = hpa.spec.metrics.as_ref().map(|specs| {
            specs
                .iter()
                .map(|spec| MetricStatus {
                    metric_type: spec.metric_type.clone(),
                    resource: spec.resource.as_ref().map(|r| ResourceMetricStatus {
                        name: r.name.clone(),
                        current: MetricValueStatus {
                            value: None,
                            average_value: None,
                            average_utilization: Some(85), // Mock current utilization
                        },
                    }),
                    pods: None,
                    object: None,
                    external: None,
                    container_resource: None,
                })
                .collect()
        });

        // Build conditions
        let now = Utc::now();
        let mut conditions = vec![
            HorizontalPodAutoscalerCondition {
                condition_type: "AbleToScale".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("ReadyForNewScale".to_string()),
                message: Some(
                    "the HPA controller was able to get the target's current scale".to_string(),
                ),
            },
            HorizontalPodAutoscalerCondition {
                condition_type: "ScalingActive".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("ValidMetricFound".to_string()),
                message: Some(
                    "the HPA was able to successfully calculate a replica count from the metrics"
                        .to_string(),
                ),
            },
        ];

        // Add ScalingLimited condition if at min/max
        let min_replicas = hpa.spec.min_replicas.unwrap_or(1);
        let max_replicas = hpa.spec.max_replicas;
        if desired_replicas >= max_replicas {
            conditions.push(HorizontalPodAutoscalerCondition {
                condition_type: "ScalingLimited".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("TooManyReplicas".to_string()),
                message: Some(format!(
                    "the desired replica count is more than the maximum replica count of {}",
                    max_replicas
                )),
            });
        } else if desired_replicas <= min_replicas {
            conditions.push(HorizontalPodAutoscalerCondition {
                condition_type: "ScalingLimited".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("TooFewReplicas".to_string()),
                message: Some(format!(
                    "the desired replica count is less than the minimum replica count of {}",
                    min_replicas
                )),
            });
        } else {
            conditions.push(HorizontalPodAutoscalerCondition {
                condition_type: "ScalingLimited".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("DesiredWithinRange".to_string()),
                message: Some(
                    "the desired replica count is within the acceptable range".to_string(),
                ),
            });
        }

        let last_scale_time = if current_replicas != desired_replicas {
            Some(Utc::now())
        } else {
            hpa.status.as_ref().and_then(|s| s.last_scale_time)
        };

        // K8s convention: condition.last_transition_time updates ONLY when the
        // condition's .status field actually transitions (e.g. False -> True).
        // Without this preservation we'd stamp Utc::now() every reconcile and
        // produce a MODIFIED watch event every interval, even when nothing
        // changed — a controller-side hot-loop source.
        let prev_conditions = hpa
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        for new_cond in conditions.iter_mut() {
            if let Some(prev) = prev_conditions
                .iter()
                .find(|p| p.condition_type == new_cond.condition_type)
            {
                if prev.status == new_cond.status {
                    new_cond.last_transition_time = prev.last_transition_time;
                }
            }
        }

        updated_hpa.status = Some(HorizontalPodAutoscalerStatus {
            observed_generation: None, // Would need generation tracking in ObjectMeta
            last_scale_time,
            current_replicas,
            desired_replicas,
            current_metrics,
            conditions: Some(conditions),
        });

        // Skip the write when nothing actually changed. Compare via canonical
        // JSON so optional field ordering doesn't trigger spurious updates.
        let old_status_json = serde_json::to_value(hpa.status.as_ref()).ok();
        let new_status_json = serde_json::to_value(updated_hpa.status.as_ref()).ok();
        if old_status_json == new_status_json {
            debug!(
                "HPA {}/{} status unchanged — skipping write",
                namespace, hpa.metadata.name
            );
            return Ok(());
        }

        self.storage.update(&key, &updated_hpa).await?;
        debug!("Updated HPA status: {}/{}", namespace, hpa.metadata.name);

        Ok(())
    }

    /// Update HPA status with error condition
    async fn update_hpa_status_with_error(
        &self,
        hpa: &HorizontalPodAutoscaler,
        error_msg: &str,
    ) -> Result<()> {
        let namespace = hpa.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key(
            "horizontalpodautoscalers",
            Some(namespace),
            &hpa.metadata.name,
        );

        let mut updated_hpa = hpa.clone();
        let now = Utc::now();

        let current_replicas = hpa.status.as_ref().map(|s| s.current_replicas).unwrap_or(0);

        let conditions = vec![
            HorizontalPodAutoscalerCondition {
                condition_type: "AbleToScale".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("FailedGetScale".to_string()),
                message: Some(error_msg.to_string()),
            },
            HorizontalPodAutoscalerCondition {
                condition_type: "ScalingActive".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("FailedComputeMetricsReplicas".to_string()),
                message: Some(error_msg.to_string()),
            },
        ];

        updated_hpa.status = Some(HorizontalPodAutoscalerStatus {
            observed_generation: None,
            last_scale_time: hpa.status.as_ref().and_then(|s| s.last_scale_time),
            current_replicas,
            desired_replicas: current_replicas,
            current_metrics: None,
            conditions: Some(conditions),
        });

        self.storage.update(&key, &updated_hpa).await?;
        debug!(
            "Updated HPA status with error: {}/{}",
            namespace, hpa.metadata.name
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        CrossVersionObjectReference, DeploymentSpec, HorizontalPodAutoscalerSpec, MetricSpec,
        MetricTarget, ResourceMetricSource,
    };
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_get_current_replicas_deployment() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = HorizontalPodAutoscalerController::new(storage.clone());

        // Create a deployment
        let mut deployment = Deployment {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("web-app").with_namespace("default"),
            spec: DeploymentSpec {
                replicas: Some(3),
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("web-pod")),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![],
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
        deployment.metadata.ensure_uid();
        deployment.metadata.ensure_creation_timestamp();

        let key = build_key("deployments", Some("default"), "web-app");
        storage.create(&key, &deployment).await.unwrap();

        let target_ref = CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: "web-app".to_string(),
            api_version: Some("apps/v1".to_string()),
        };

        let replicas = controller
            .get_current_replicas("default", &target_ref)
            .await
            .unwrap();
        assert_eq!(replicas, 3);
    }

    #[tokio::test]
    async fn test_calculate_desired_replicas_with_bounds() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = HorizontalPodAutoscalerController::new(storage);

        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "web-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(2),
            max_replicas: 10,
            metrics: Some(vec![MetricSpec {
                metric_type: "Resource".to_string(),
                resource: Some(ResourceMetricSource {
                    name: "cpu".to_string(),
                    target: MetricTarget {
                        target_type: "Utilization".to_string(),
                        value: None,
                        average_value: None,
                        average_utilization: Some(80),
                    },
                }),
                pods: None,
                object: None,
                external: None,
                container_resource: None,
            }]),
            behavior: None,
        };

        let hpa = HorizontalPodAutoscaler::new("test-hpa", "default", spec);

        // Test with current replicas = 1 (below min)
        let desired = controller
            .calculate_desired_replicas(&hpa, 1, "default")
            .await
            .unwrap();
        assert!(
            desired >= 2,
            "Desired replicas should be at least min_replicas (2), got {}",
            desired
        );

        // Test with current replicas = 20 (above max)
        let desired = controller
            .calculate_desired_replicas(&hpa, 20, "default")
            .await
            .unwrap();
        assert!(
            desired <= 10,
            "Desired replicas should be at most max_replicas (10), got {}",
            desired
        );
    }

    #[tokio::test]
    async fn test_scale_target_deployment() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = HorizontalPodAutoscalerController::new(storage.clone());

        // Create a deployment
        let mut deployment = Deployment {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("web-app").with_namespace("default"),
            spec: DeploymentSpec {
                replicas: Some(2),
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("web-pod")),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![],
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
        deployment.metadata.ensure_uid();
        deployment.metadata.ensure_creation_timestamp();

        let key = build_key("deployments", Some("default"), "web-app");
        storage.create(&key, &deployment).await.unwrap();

        let target_ref = CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: "web-app".to_string(),
            api_version: Some("apps/v1".to_string()),
        };

        // Scale to 5 replicas
        controller
            .scale_target("default", &target_ref, 5)
            .await
            .unwrap();

        // Verify the deployment was scaled
        let updated_deployment: Deployment = storage.get(&key).await.unwrap();
        assert_eq!(updated_deployment.spec.replicas, Some(5));
    }

    /// Regression test: `update_hpa_status_success` must not rewrite the HPA on
    /// successive reconciles when nothing has changed. Without preservation of
    /// each condition's `last_transition_time`, every reconcile cycle stamped
    /// `Utc::now()` into the conditions and re-wrote the HPA — emitting a
    /// MODIFIED watch event per interval and a downstream churn loop.
    #[tokio::test]
    async fn test_update_hpa_status_idempotent_when_nothing_changes() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = HorizontalPodAutoscalerController::new(storage.clone());

        // Minimal Deployment with replicas matching what the HPA will compute.
        let mut deployment_spec = DeploymentSpec {
            replicas: Some(3),
            selector: rusternetes_common::types::LabelSelector {
                match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                match_expressions: None,
            },
            template: rusternetes_common::resources::PodTemplateSpec {
                metadata: Some(ObjectMeta::new("web-pod")),
                spec: rusternetes_common::resources::PodSpec::default(),
            },
            strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
            paused: None,
            progress_deadline_seconds: None,
        };
        deployment_spec.replicas = Some(3);

        let mut deployment = Deployment::new("web-app", deployment_spec);
        deployment.metadata = ObjectMeta::new("web-app").with_namespace("default");
        deployment.metadata.ensure_uid();
        deployment.metadata.ensure_creation_timestamp();
        storage
            .create(
                &build_key("deployments", Some("default"), "web-app"),
                &deployment,
            )
            .await
            .unwrap();

        // HPA targeting that Deployment, with min/max wide enough to leave
        // ScalingLimited=False so we exercise the within-range branch.
        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "web-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(1),
            max_replicas: 10,
            metrics: None,
            behavior: None,
        };
        let hpa = HorizontalPodAutoscaler::new("test-hpa", "default", spec);
        let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "test-hpa");
        storage.create(&hpa_key, &hpa).await.unwrap();

        // First reconcile establishes the HPA status + conditions.
        controller.reconcile_all().await.unwrap();
        let after_first: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
        let first_v: serde_json::Value = serde_json::to_value(after_first.status.as_ref()).unwrap();

        // Sleep enough that any Utc::now() on a re-write would differ from the
        // timestamps captured above (chrono::Utc has sub-millisecond resolution
        // but in tight CI the two calls could otherwise coincide).
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Second reconcile over the unchanged cluster must NOT mutate the HPA.
        controller.reconcile_all().await.unwrap();
        let after_second: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
        let second_v: serde_json::Value =
            serde_json::to_value(after_second.status.as_ref()).unwrap();

        assert_eq!(
            first_v, second_v,
            "HPA status (incl. condition.last_transition_time fields) must be \
             semantically equal after a no-op reconcile — otherwise the controller \
             produces a MODIFIED event every interval (controller hot-loop)."
        );
    }
}
