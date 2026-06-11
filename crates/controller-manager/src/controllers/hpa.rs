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

use crate::controllers::hpa_metrics_client::{
    FakeMetricsClient, HttpMetricsClient, HttpMetricsConfig, MetricsClient,
};
use crate::controllers::hpa_replica_calculator as calc;

pub struct HorizontalPodAutoscalerController<S: Storage> {
    storage: Arc<S>,
    metrics_client: Arc<dyn MetricsClient>,
}

impl<S: Storage + 'static> HorizontalPodAutoscalerController<S> {
    /// Default-config constructor — builds an `HttpMetricsClient` from
    /// `HttpMetricsConfig::default()`. Binaries thread explicit config via
    /// `with_config`, so this is only used by tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_config(storage, HttpMetricsConfig::default())
    }

    pub fn with_config(storage: Arc<S>, cfg: HttpMetricsConfig) -> Self {
        let metrics_client: Arc<dyn MetricsClient> = match HttpMetricsClient::new(cfg) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                warn!("HPA metrics client unavailable ({e:#}); metric fetches will fail");
                Arc::new(FakeMetricsClient::new())
            }
        };
        Self {
            storage,
            metrics_client,
        }
    }

    /// Test/explicit constructor — inject any MetricsClient.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_metrics_client(storage: Arc<S>, metrics_client: Arc<dyn MetricsClient>) -> Self {
        Self {
            storage,
            metrics_client,
        }
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
        let (desired_replicas, metric_statuses) = match self
            .calculate_desired_replicas(hpa, current_replicas, namespace)
            .await
        {
            Ok(v) => v,
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
        self.update_hpa_status_success(hpa, current_replicas, desired_replicas, metric_statuses)
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

    /// Resolve the label selector of the HPA's scale target (for pod metrics).
    async fn target_selector(
        &self,
        namespace: &str,
        target_ref: &rusternetes_common::resources::CrossVersionObjectReference,
    ) -> rusternetes_common::types::LabelSelector {
        use rusternetes_common::types::LabelSelector;
        match target_ref.kind.as_str() {
            "Deployment" => {
                let key = build_key("deployments", Some(namespace), &target_ref.name);
                self.storage
                    .get::<Deployment>(&key)
                    .await
                    .ok()
                    .map(|d| d.spec.selector)
                    .unwrap_or_default()
            }
            "ReplicaSet" => {
                let key = build_key("replicasets", Some(namespace), &target_ref.name);
                self.storage
                    .get::<ReplicaSet>(&key)
                    .await
                    .ok()
                    .map(|r| r.spec.selector)
                    .unwrap_or_default()
            }
            "StatefulSet" => {
                let key = build_key("statefulsets", Some(namespace), &target_ref.name);
                self.storage
                    .get::<StatefulSet>(&key)
                    .await
                    .ok()
                    .map(|s| s.spec.selector)
                    .unwrap_or_default()
            }
            _ => LabelSelector::default(),
        }
    }

    /// Calculate desired replica count based on metrics
    /// Implements the HPA algorithm: desiredReplicas = ceil[currentReplicas * (currentMetricValue / targetMetricValue)]
    async fn calculate_desired_replicas(
        &self,
        hpa: &HorizontalPodAutoscaler,
        current_replicas: i32,
        namespace: &str,
    ) -> Result<(i32, Vec<MetricStatus>)> {
        let metrics = match &hpa.spec.metrics {
            Some(m) if !m.is_empty() => m,
            _ => return Ok((current_replicas, Vec::new())),
        };

        let mut max_desired = current_replicas;
        let mut statuses = Vec::new();
        for metric in metrics {
            let (desired, status) = self
                .calculate_replicas_for_metric(metric, current_replicas, namespace, hpa)
                .await?;
            if desired > max_desired {
                max_desired = desired;
            }
            statuses.push(status);
        }

        let min_replicas = hpa.spec.min_replicas.unwrap_or(1);
        let max_replicas = hpa.spec.max_replicas;
        let bounded = max_desired.max(min_replicas).min(max_replicas);
        Ok((bounded, statuses))
    }

    /// Calculate desired replicas + status for a single metric.
    async fn calculate_replicas_for_metric(
        &self,
        metric: &MetricSpec,
        current_replicas: i32,
        namespace: &str,
        hpa: &HorizontalPodAutoscaler,
    ) -> Result<(i32, MetricStatus)> {
        let selector = self
            .target_selector(namespace, &hpa.spec.scale_target_ref)
            .await;
        match metric.metric_type.as_str() {
            "Resource" => {
                let r = metric
                    .resource
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Resource metric without resource field"))?;
                let target = r.target.average_utilization.unwrap_or(80);
                let info = self
                    .metrics_client
                    .get_resource_metric(&r.name, namespace, &selector)
                    .await?;
                let (replicas, avg) = calc::get_resource_replicas(&info, current_replicas, target)?;
                let status = MetricStatus {
                    metric_type: "Resource".into(),
                    resource: Some(ResourceMetricStatus {
                        name: r.name.clone(),
                        current: MetricValueStatus {
                            value: None,
                            average_value: None,
                            average_utilization: Some(avg),
                        },
                    }),
                    pods: None,
                    object: None,
                    external: None,
                    container_resource: None,
                };
                Ok((replicas, status))
            }
            "ContainerResource" => {
                let cr = metric.container_resource.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("ContainerResource metric without containerResource field")
                })?;
                let target = cr.target.average_utilization.unwrap_or(80);
                let info = self
                    .metrics_client
                    .get_container_resource_metric(&cr.name, &cr.container, namespace, &selector)
                    .await?;
                let (replicas, avg) = calc::get_resource_replicas(&info, current_replicas, target)?;
                let status = MetricStatus {
                    metric_type: "ContainerResource".into(),
                    resource: None,
                    pods: None,
                    object: None,
                    external: None,
                    container_resource: Some(
                        rusternetes_common::resources::ContainerResourceMetricStatus {
                            name: cr.name.clone(),
                            container: cr.container.clone(),
                            current: MetricValueStatus {
                                value: None,
                                average_value: None,
                                average_utilization: Some(avg),
                            },
                        },
                    ),
                };
                Ok((replicas, status))
            }
            "Pods" => {
                let p = metric
                    .pods
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Pods metric without pods field"))?;
                let target = parse_target_avg(&p.target)?;
                let info = self
                    .metrics_client
                    .get_raw_metric(&p.metric.name, namespace, &selector)
                    .await?;
                let (replicas, avg) = calc::get_metric_replicas(&info, current_replicas, target)?;
                let status = MetricStatus {
                    metric_type: "Pods".into(),
                    resource: None,
                    pods: Some(rusternetes_common::resources::PodsMetricStatus {
                        metric: p.metric.clone(),
                        current: MetricValueStatus {
                            value: None,
                            average_value: Some(avg.to_string()),
                            average_utilization: None,
                        },
                    }),
                    object: None,
                    external: None,
                    container_resource: None,
                };
                Ok((replicas, status))
            }
            "Object" => {
                let o = metric
                    .object
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Object metric without object field"))?;
                let target = parse_target_value(&o.target)?;
                let (value, _) = self
                    .metrics_client
                    .get_object_metric(&o.metric.name, namespace, &o.described_object)
                    .await?;
                let (replicas, v) =
                    calc::get_object_metric_replicas(value, current_replicas, target)?;
                // Report the field matching the target type: AverageValue
                // targets report currentAverageValue = value / replicas; Value
                // targets report the raw value. (k8s ObjectMetricStatus.)
                let current = if o.target.target_type == "AverageValue" {
                    let replicas = current_replicas.max(1) as i64;
                    MetricValueStatus {
                        value: None,
                        average_value: Some((v / replicas).to_string()),
                        average_utilization: None,
                    }
                } else {
                    MetricValueStatus {
                        value: Some(v.to_string()),
                        average_value: None,
                        average_utilization: None,
                    }
                };
                let status = MetricStatus {
                    metric_type: "Object".into(),
                    resource: None,
                    pods: None,
                    object: Some(rusternetes_common::resources::ObjectMetricStatus {
                        metric: o.metric.clone(),
                        described_object: o.described_object.clone(),
                        current,
                    }),
                    external: None,
                    container_resource: None,
                };
                Ok((replicas, status))
            }
            "External" => {
                let e = metric
                    .external
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("External metric without external field"))?;
                let target = parse_target_avg(&e.target)?;
                let default_sel = rusternetes_common::types::LabelSelector::default();
                let sel = e.metric.selector.as_ref().unwrap_or(&default_sel);
                let (values, _) = self
                    .metrics_client
                    .get_external_metric(&e.metric.name, namespace, sel)
                    .await?;
                let (replicas, sum) = calc::get_external_metric_replicas(&values, target)?;
                // k8s reports currentAverageValue = sum / currentReplicas for an
                // AverageValue target (the scaling math already used the per-pod
                // target); the raw sum would mislabel the status field.
                let avg = sum / (current_replicas.max(1) as i64);
                let status = MetricStatus {
                    metric_type: "External".into(),
                    resource: None,
                    pods: None,
                    object: None,
                    external: Some(rusternetes_common::resources::ExternalMetricStatus {
                        metric: e.metric.clone(),
                        current: MetricValueStatus {
                            value: None,
                            average_value: Some(avg.to_string()),
                            average_utilization: None,
                        },
                    }),
                    container_resource: None,
                };
                Ok((replicas, status))
            }
            other => anyhow::bail!("unsupported metric type: {other}"),
        }
    }

    /// Update HPA status with success
    async fn update_hpa_status_success(
        &self,
        hpa: &HorizontalPodAutoscaler,
        current_replicas: i32,
        desired_replicas: i32,
        metric_statuses: Vec<MetricStatus>,
    ) -> Result<()> {
        let namespace = hpa.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key(
            "horizontalpodautoscalers",
            Some(namespace),
            &hpa.metadata.name,
        );

        let mut updated_hpa = hpa.clone();

        let current_metrics = if metric_statuses.is_empty() {
            None
        } else {
            Some(metric_statuses)
        };

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

fn parse_target_avg(t: &rusternetes_common::resources::MetricTarget) -> Result<i64> {
    t.average_value
        .as_ref()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| anyhow::anyhow!("metric target missing/invalid averageValue"))
}

fn parse_target_value(t: &rusternetes_common::resources::MetricTarget) -> Result<i64> {
    t.value
        .as_ref()
        .or(t.average_value.as_ref())
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| anyhow::anyhow!("metric target missing/invalid value"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::hpa_metrics_client::FakeMetricsClient;
    use rusternetes_common::resources::{
        CrossVersionObjectReference, DeploymentSpec, HorizontalPodAutoscalerSpec, MetricSpec,
        MetricTarget, ResourceMetricSource,
    };
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    fn controller_with_cpu_util(
        storage: std::sync::Arc<MemoryStorage>,
        utilization: i32,
    ) -> HorizontalPodAutoscalerController<MemoryStorage> {
        let mut fake = FakeMetricsClient::new();
        fake.resource.insert(
            "cpu".to_string(),
            FakeMetricsClient::pods_info(&[("pod-a", 0, Some(utilization))]),
        );
        HorizontalPodAutoscalerController::with_metrics_client(storage, std::sync::Arc::new(fake))
    }

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
        let controller = controller_with_cpu_util(storage, 85);

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
        let (desired, _) = controller
            .calculate_desired_replicas(&hpa, 1, "default")
            .await
            .unwrap();
        assert!(
            desired >= 2,
            "Desired replicas should be at least min_replicas (2), got {}",
            desired
        );

        // Test with current replicas = 20 (above max)
        let (desired, _) = controller
            .calculate_desired_replicas(&hpa, 20, "default")
            .await
            .unwrap();
        assert!(
            desired <= 10,
            "Desired replicas should be at most max_replicas (10), got {}",
            desired
        );
    }

    fn hpa_with_metric(metric: MetricSpec) -> HorizontalPodAutoscaler {
        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "web-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(1),
            max_replicas: 100,
            metrics: Some(vec![metric]),
            behavior: None,
        };
        HorizontalPodAutoscaler::new("test-hpa", "default", spec)
    }

    #[tokio::test]
    async fn container_resource_metric_scales_on_container_utilization() {
        use rusternetes_common::resources::ContainerResourceMetricSource;
        let storage = Arc::new(MemoryStorage::new());
        let mut fake = FakeMetricsClient::new();
        // Keyed "{resource}/{container}" — 100% util vs 50% target -> ratio 2.
        fake.resource.insert(
            "cpu/app".to_string(),
            FakeMetricsClient::pods_info(&[("pod-a", 0, Some(100))]),
        );
        let controller =
            HorizontalPodAutoscalerController::with_metrics_client(storage, Arc::new(fake));

        let metric = MetricSpec {
            metric_type: "ContainerResource".to_string(),
            resource: None,
            pods: None,
            object: None,
            external: None,
            container_resource: Some(ContainerResourceMetricSource {
                name: "cpu".to_string(),
                container: "app".to_string(),
                target: MetricTarget {
                    target_type: "Utilization".to_string(),
                    value: None,
                    average_value: None,
                    average_utilization: Some(50),
                },
            }),
        };
        let hpa = hpa_with_metric(metric.clone());
        let (replicas, status) = controller
            .calculate_replicas_for_metric(&metric, 2, "default", &hpa)
            .await
            .unwrap();
        assert_eq!(replicas, 4, "100% vs 50% target on 2 replicas -> 4");
        let cr = status
            .container_resource
            .expect("container_resource status");
        assert_eq!(cr.container, "app");
        assert_eq!(cr.current.average_utilization, Some(100));
    }

    #[tokio::test]
    async fn external_status_reports_sum_over_replicas() {
        use rusternetes_common::resources::{ExternalMetricSource, MetricIdentifier};
        let storage = Arc::new(MemoryStorage::new());
        let mut fake = FakeMetricsClient::new();
        fake.external.insert("queue".to_string(), vec![90]);
        let controller =
            HorizontalPodAutoscalerController::with_metrics_client(storage, Arc::new(fake));

        let metric = MetricSpec {
            metric_type: "External".to_string(),
            resource: None,
            pods: None,
            object: None,
            external: Some(ExternalMetricSource {
                metric: MetricIdentifier {
                    name: "queue".to_string(),
                    selector: None,
                },
                target: MetricTarget {
                    target_type: "AverageValue".to_string(),
                    value: None,
                    average_value: Some("30".to_string()),
                    average_utilization: None,
                },
            }),
            container_resource: None,
        };
        let hpa = hpa_with_metric(metric.clone());
        let (_replicas, status) = controller
            .calculate_replicas_for_metric(&metric, 3, "default", &hpa)
            .await
            .unwrap();
        let ext = status.external.expect("external status");
        // sum 90 / 3 replicas = 30, not the raw sum 90.
        assert_eq!(ext.current.average_value.as_deref(), Some("30"));
    }

    #[tokio::test]
    async fn object_average_value_target_reports_average_value() {
        use rusternetes_common::resources::{MetricIdentifier, ObjectMetricSource};
        let storage = Arc::new(MemoryStorage::new());
        let mut fake = FakeMetricsClient::new();
        fake.object.insert("hits".to_string(), 200);
        let controller =
            HorizontalPodAutoscalerController::with_metrics_client(storage, Arc::new(fake));

        let metric = MetricSpec {
            metric_type: "Object".to_string(),
            resource: None,
            pods: None,
            object: Some(ObjectMetricSource {
                described_object: CrossVersionObjectReference {
                    kind: "Service".to_string(),
                    name: "web".to_string(),
                    api_version: Some("v1".to_string()),
                },
                metric: MetricIdentifier {
                    name: "hits".to_string(),
                    selector: None,
                },
                target: MetricTarget {
                    target_type: "AverageValue".to_string(),
                    value: None,
                    average_value: Some("100".to_string()),
                    average_utilization: None,
                },
            }),
            external: None,
            container_resource: None,
        };
        let hpa = hpa_with_metric(metric.clone());
        let (_replicas, status) = controller
            .calculate_replicas_for_metric(&metric, 2, "default", &hpa)
            .await
            .unwrap();
        let obj = status.object.expect("object status");
        // AverageValue target -> averageValue = value/replicas = 200/2 = 100;
        // the `value` field stays unset.
        assert_eq!(obj.current.average_value.as_deref(), Some("100"));
        assert!(obj.current.value.is_none());
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
        let controller = controller_with_cpu_util(storage.clone(), 85);

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
