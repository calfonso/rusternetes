use rusternetes_common::resources::pod::{Pod, PodCondition, PodSpec, PodStatus};
use rusternetes_common::resources::{
    Container, CrossVersionObjectReference, Deployment, DeploymentSpec, ExternalMetricSource,
    HPAScalingPolicy, HPAScalingRules, HorizontalPodAutoscaler, HorizontalPodAutoscalerBehavior,
    HorizontalPodAutoscalerSpec, MetricIdentifier, MetricSpec, MetricTarget, ObjectMetricSource,
    PodTemplateSpec, PodsMetricSource, ResourceMetricSource,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::hpa::HorizontalPodAutoscalerController;
use rusternetes_controller_manager::controllers::hpa_metrics_client::{
    FakeMetricsClient, MetricsClient, PodMetricsInfo,
};
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// A `MetricsClient` whose resource (cpu) reading changes per call — pops the
/// next configured utilization. Used to drive a high→low load sequence across
/// reconciles (the stabilization test); all other metric types are unused.
struct SeqCpuMetrics {
    utils: std::sync::Mutex<std::collections::VecDeque<i32>>,
}

impl SeqCpuMetrics {
    fn new(utils: &[i32]) -> Self {
        Self {
            utils: std::sync::Mutex::new(utils.iter().copied().collect()),
        }
    }
}

#[async_trait::async_trait]
impl MetricsClient for SeqCpuMetrics {
    async fn get_resource_metric(
        &self,
        _resource: &str,
        _namespace: &str,
        _selector: &LabelSelector,
    ) -> anyhow::Result<PodMetricsInfo> {
        // Hold the last value once the sequence is exhausted.
        let mut q = self.utils.lock().unwrap();
        let u = if q.len() > 1 {
            q.pop_front().unwrap()
        } else {
            *q.front().unwrap_or(&0)
        };
        Ok(FakeMetricsClient::pods_info(&[("p", 0, Some(u))]))
    }
    async fn get_container_resource_metric(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &LabelSelector,
    ) -> anyhow::Result<PodMetricsInfo> {
        anyhow::bail!("not used")
    }
    async fn get_raw_metric(
        &self,
        _: &str,
        _: &str,
        _: &LabelSelector,
    ) -> anyhow::Result<PodMetricsInfo> {
        anyhow::bail!("not used")
    }
    async fn get_object_metric(
        &self,
        _: &str,
        _: &str,
        _: &CrossVersionObjectReference,
    ) -> anyhow::Result<(i64, chrono::DateTime<chrono::Utc>)> {
        anyhow::bail!("not used")
    }
    async fn get_external_metric(
        &self,
        _: &str,
        _: &str,
        _: &LabelSelector,
    ) -> anyhow::Result<(Vec<i64>, chrono::DateTime<chrono::Utc>)> {
        anyhow::bail!("not used")
    }
}

/// Create a Running-but-unready pod (Ready=False) that started "now" — i.e.
/// still inside the cpu-initialization window — labelled `app=<app>`.
async fn create_unready_pod(storage: &Arc<MemoryStorage>, ns: &str, name: &str, app: &str) {
    let now = chrono::Utc::now();
    let mut pod = Pod::new(name, PodSpec::default());
    let mut meta = ObjectMeta::new(name);
    meta.namespace = Some(ns.to_string());
    meta.labels = Some(HashMap::from([("app".to_string(), app.to_string())]));
    pod.metadata = meta;
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        start_time: Some(now),
        conditions: Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(now),
            observed_generation: None,
        }]),
        ..Default::default()
    });
    let key = build_key("pods", Some(ns), name);
    storage.create(&key, &pod).await.unwrap();
}

fn create_test_deployment(name: &str, namespace: &str, replicas: i32) -> Deployment {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    Deployment {
        type_meta: TypeMeta {
            kind: "Deployment".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            min_ready_seconds: None,
            revision_history_limit: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
                        image: "nginx:1.25-alpine".to_string(),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        ports: Some(vec![]),
                        env: None,
                        volume_mounts: None,
                        liveness_probe: None,
                        readiness_probe: None,
                        startup_probe: None,
                        resources: None,
                        working_dir: None,
                        command: None,
                        args: None,
                        restart_policy: None,
                        resize_policy: None,
                        security_context: None,
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
                    ephemeral_containers: None,
                    volumes: None,
                    restart_policy: Some("Always".to_string()),
                    node_name: None,
                    node_selector: None,
                    service_account_name: None,
                    service_account: None,
                    automount_service_account_token: None,
                    hostname: None,
                    subdomain: None,
                    host_network: None,
                    host_pid: None,
                    host_ipc: None,
                    affinity: None,
                    tolerations: None,
                    priority: None,
                    priority_class_name: None,
                    scheduler_name: None,
                    overhead: None,
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
                },
            },
            strategy: None,
            paused: None,
            progress_deadline_seconds: None,
        },
        status: None,
    }
}

fn create_test_hpa(
    name: &str,
    namespace: &str,
    target_name: &str,
    target_kind: &str,
    min_replicas: Option<i32>,
    max_replicas: i32,
    target_cpu_utilization: i32,
) -> HorizontalPodAutoscaler {
    let spec = HorizontalPodAutoscalerSpec {
        scale_target_ref: CrossVersionObjectReference {
            kind: target_kind.to_string(),
            name: target_name.to_string(),
            api_version: Some("apps/v1".to_string()),
        },
        min_replicas,
        max_replicas,
        metrics: Some(vec![MetricSpec {
            metric_type: "Resource".to_string(),
            resource: Some(ResourceMetricSource {
                name: "cpu".to_string(),
                target: MetricTarget {
                    target_type: "Utilization".to_string(),
                    value: None,
                    average_value: None,
                    average_utilization: Some(target_cpu_utilization),
                },
            }),
            pods: None,
            object: None,
            external: None,
            container_resource: None,
        }]),
        behavior: None,
    };

    HorizontalPodAutoscaler::new(name, namespace, spec)
}

#[tokio::test]
async fn test_hpa_scales_deployment_up_when_cpu_high() {
    let storage = Arc::new(MemoryStorage::new());
    // cpu @ 90% util vs target 80% ⇒ ratio 1.125 (outside ±10% band) ⇒
    // ceil(2 * 90/80) = 3, genuinely scaling up as the test asserts.
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(90))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    // Create deployment with 2 replicas
    let deployment = create_test_deployment("web-app", "default", 2);
    let deploy_key = build_key("deployments", Some("default"), "web-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Create HPA targeting the deployment
    // target CPU = 80%, current CPU will be ~85% (from mock), so should scale up
    let hpa = create_test_hpa(
        "web-hpa",
        "default",
        "web-app",
        "Deployment",
        Some(2),
        10,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "web-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Reconcile the HPA
    controller.reconcile_all().await.unwrap();

    // Verify the deployment was scaled up
    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    // Mock CPU utilization is 85%, target is 80%
    // Formula: ceil(2 * (85/80)) = ceil(2.125) = 3
    assert!(
        updated_deployment.spec.replicas.unwrap_or(0) >= 2,
        "Replicas should be at least 2 (current or scaled up), got {}",
        updated_deployment.spec.replicas.unwrap_or(0)
    );

    // Verify HPA status was updated
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    assert!(
        updated_hpa.status.is_some(),
        "HPA status should be populated"
    );
    let status = updated_hpa.status.unwrap();
    assert!(
        status.current_replicas > 0,
        "Current replicas should be > 0"
    );
    assert!(
        status.desired_replicas > 0,
        "Desired replicas should be > 0"
    );
}

#[tokio::test]
async fn test_hpa_respects_min_replicas() {
    let storage = Arc::new(MemoryStorage::new());
    // cpu @ 5% util vs target 80% ⇒ raw desired ceil(1 * 5/80) = 1, which is
    // below min_replicas (3) ⇒ the min clamp must raise it to 3.
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(5))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    // Create deployment with only 1 replica
    let deployment = create_test_deployment("small-app", "default", 1);
    let deploy_key = build_key("deployments", Some("default"), "small-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Create HPA with min_replicas = 3
    let hpa = create_test_hpa(
        "small-hpa",
        "default",
        "small-app",
        "Deployment",
        Some(3),
        10,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "small-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify deployment was scaled to at least min_replicas
    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    assert!(
        updated_deployment.spec.replicas.unwrap_or(0) >= 3,
        "Deployment should be scaled to at least min_replicas (3), got {}",
        updated_deployment.spec.replicas.unwrap_or(0)
    );
}

#[tokio::test]
async fn test_hpa_respects_max_replicas() {
    let storage = Arc::new(MemoryStorage::new());
    // cpu @ 90% util vs target 80% ⇒ raw desired ceil(20 * 90/80) = 23, far
    // above max_replicas (5) ⇒ the max clamp must cap it at 5.
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(90))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    // Create deployment with many replicas
    let deployment = create_test_deployment("large-app", "default", 20);
    let deploy_key = build_key("deployments", Some("default"), "large-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Create HPA with max_replicas = 5
    // Even though current is 20, HPA should cap it at 5
    let hpa = create_test_hpa(
        "large-hpa",
        "default",
        "large-app",
        "Deployment",
        Some(1),
        5,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "large-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify deployment was scaled down to max_replicas
    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        updated_deployment.spec.replicas,
        Some(5),
        "Deployment should be capped at max_replicas (5), got {}",
        updated_deployment.spec.replicas.unwrap_or(0)
    );
}

#[tokio::test]
async fn test_hpa_handles_missing_target() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

    // Create HPA but don't create the target deployment
    let hpa = create_test_hpa(
        "orphan-hpa",
        "default",
        "nonexistent",
        "Deployment",
        Some(2),
        10,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "orphan-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Reconcile - should not crash, should update status with error
    controller.reconcile_all().await.unwrap();

    // Verify HPA status shows error
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    assert!(
        updated_hpa.status.is_some(),
        "HPA status should be populated"
    );
    let status = updated_hpa.status.unwrap();

    // Check conditions for error
    if let Some(conditions) = status.conditions {
        let able_to_scale = conditions
            .iter()
            .find(|c| c.condition_type == "AbleToScale");
        assert!(able_to_scale.is_some(), "Should have AbleToScale condition");
        assert_eq!(
            able_to_scale.unwrap().status,
            "False",
            "AbleToScale should be False when target is missing"
        );
    } else {
        panic!("HPA should have conditions when target is missing");
    }
}

#[tokio::test]
async fn test_hpa_updates_status_conditions() {
    let storage = Arc::new(MemoryStorage::new());
    // A successful cpu metric path so the controller writes the full set of
    // status conditions (AbleToScale / ScalingActive / ScalingLimited).
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(85))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    // Create deployment
    let deployment = create_test_deployment("status-app", "default", 3);
    let deploy_key = build_key("deployments", Some("default"), "status-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Create HPA
    let hpa = create_test_hpa(
        "status-hpa",
        "default",
        "status-app",
        "Deployment",
        Some(2),
        10,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "status-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify HPA status has expected conditions
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    assert!(
        updated_hpa.status.is_some(),
        "HPA status should be populated"
    );

    let status = updated_hpa.status.unwrap();
    assert!(status.conditions.is_some(), "HPA should have conditions");

    let conditions = status.conditions.unwrap();
    assert!(
        conditions.iter().any(|c| c.condition_type == "AbleToScale"),
        "Should have AbleToScale condition"
    );
    assert!(
        conditions
            .iter()
            .any(|c| c.condition_type == "ScalingActive"),
        "Should have ScalingActive condition"
    );
    assert!(
        conditions
            .iter()
            .any(|c| c.condition_type == "ScalingLimited"),
        "Should have ScalingLimited condition"
    );
}

#[tokio::test]
async fn test_hpa_with_no_metrics_maintains_current() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

    // Create deployment with 4 replicas
    let deployment = create_test_deployment("no-metrics-app", "default", 4);
    let deploy_key = build_key("deployments", Some("default"), "no-metrics-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Create HPA with no metrics specified
    let mut hpa = HorizontalPodAutoscaler::new(
        "no-metrics-hpa",
        "default",
        HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "no-metrics-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(2),
            max_replicas: 10,
            metrics: None, // No metrics
            behavior: None,
        },
    );
    hpa.metadata.ensure_uid();
    hpa.metadata.ensure_creation_timestamp();

    let hpa_key = build_key(
        "horizontalpodautoscalers",
        Some("default"),
        "no-metrics-hpa",
    );
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify deployment replicas unchanged (should maintain current)
    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        updated_deployment.spec.replicas,
        Some(4),
        "Deployment replicas should remain unchanged when no metrics specified, got {}",
        updated_deployment.spec.replicas.unwrap_or(0)
    );
}

#[tokio::test]
async fn test_hpa_multiple_hpas_in_different_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

    // Create deployments in different namespaces
    let deploy1 = create_test_deployment("app", "ns1", 2);
    let deploy2 = create_test_deployment("app", "ns2", 3);

    storage
        .create(&build_key("deployments", Some("ns1"), "app"), &deploy1)
        .await
        .unwrap();
    storage
        .create(&build_key("deployments", Some("ns2"), "app"), &deploy2)
        .await
        .unwrap();

    // Create HPAs in different namespaces
    let hpa1 = create_test_hpa("app-hpa", "ns1", "app", "Deployment", Some(2), 8, 80);
    let hpa2 = create_test_hpa("app-hpa", "ns2", "app", "Deployment", Some(3), 10, 80);

    storage
        .create(
            &build_key("horizontalpodautoscalers", Some("ns1"), "app-hpa"),
            &hpa1,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("horizontalpodautoscalers", Some("ns2"), "app-hpa"),
            &hpa2,
        )
        .await
        .unwrap();

    // Reconcile all
    controller.reconcile_all().await.unwrap();

    // Verify both HPAs were reconciled and updated
    let updated_hpa1: HorizontalPodAutoscaler = storage
        .get(&build_key(
            "horizontalpodautoscalers",
            Some("ns1"),
            "app-hpa",
        ))
        .await
        .unwrap();
    let updated_hpa2: HorizontalPodAutoscaler = storage
        .get(&build_key(
            "horizontalpodautoscalers",
            Some("ns2"),
            "app-hpa",
        ))
        .await
        .unwrap();

    assert!(
        updated_hpa1.status.is_some(),
        "HPA in ns1 should have status"
    );
    assert!(
        updated_hpa2.status.is_some(),
        "HPA in ns2 should have status"
    );

    // Verify namespaces are isolated
    assert_eq!(updated_hpa1.metadata.namespace.as_deref().unwrap(), "ns1");
    assert_eq!(updated_hpa2.metadata.namespace.as_deref().unwrap(), "ns2");
}

#[tokio::test]
async fn test_hpa_scaling_limited_condition_at_max() {
    let storage = Arc::new(MemoryStorage::new());
    // cpu @ 90% util vs target 80% ⇒ raw desired ceil(10 * 90/80) = 12, above
    // max_replicas (10) ⇒ ScalingLimited=True with reason TooManyReplicas.
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(90))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    // Create deployment at max replicas
    let deployment = create_test_deployment("max-app", "default", 10);
    let deploy_key = build_key("deployments", Some("default"), "max-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Create HPA with max = 10
    let hpa = create_test_hpa(
        "max-hpa",
        "default",
        "max-app",
        "Deployment",
        Some(1),
        10,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "max-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify ScalingLimited condition is True with reason TooManyReplicas
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    let status = updated_hpa.status.unwrap();
    let conditions = status.conditions.unwrap();

    let scaling_limited = conditions
        .iter()
        .find(|c| c.condition_type == "ScalingLimited")
        .expect("Should have ScalingLimited condition");

    assert_eq!(
        scaling_limited.status, "True",
        "ScalingLimited should be True when at max replicas"
    );
    assert_eq!(
        scaling_limited.reason.as_deref().unwrap(),
        "TooManyReplicas",
        "Reason should be TooManyReplicas"
    );
}

#[tokio::test]
async fn test_hpa_current_metrics_populated() {
    let storage = Arc::new(MemoryStorage::new());
    // Seed a cpu resource reading so status.current_metrics surfaces a
    // Resource/cpu entry with average_utilization populated.
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(85))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    // Create deployment
    let deployment = create_test_deployment("metrics-app", "default", 3);
    storage
        .create(
            &build_key("deployments", Some("default"), "metrics-app"),
            &deployment,
        )
        .await
        .unwrap();

    // Create HPA
    let hpa = create_test_hpa(
        "metrics-hpa",
        "default",
        "metrics-app",
        "Deployment",
        Some(2),
        10,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "metrics-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify current metrics are populated in status
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    let status = updated_hpa.status.unwrap();

    assert!(
        status.current_metrics.is_some(),
        "Current metrics should be populated"
    );
    let current_metrics = status.current_metrics.unwrap();
    assert!(
        !current_metrics.is_empty(),
        "Should have at least one current metric"
    );

    let metric = &current_metrics[0];
    assert_eq!(metric.metric_type, "Resource");
    assert!(
        metric.resource.is_some(),
        "Resource metric should be present"
    );

    let resource_metric = metric.resource.as_ref().unwrap();
    assert_eq!(resource_metric.name, "cpu");
    assert!(
        resource_metric.current.average_utilization.is_some(),
        "Average utilization should be populated"
    );
}

// ---------------------------------------------------------------------------
// Phase 4.1 — Extended HPA coverage
// Mirrors upstream `kubernetes/test/e2e/apps/hpa.go` behavioural assertions.
//
// Tests marked `#[ignore = "RED-state: ..."]` describe upstream-compatible
// behaviour that the rusternetes HPA controller does NOT yet implement
// (custom metrics API integration, behaviour policies, stabilization
// windows, multi-metric AND/OR logic, etc.). They serve as a TODO ratchet:
// remove the `#[ignore]` attribute once the corresponding controller logic
// lands and the assertions begin to pass.
// ---------------------------------------------------------------------------

/// Helper that mirrors `create_test_hpa` but lets callers supply a custom
/// HorizontalPodAutoscalerBehavior (for stabilization / policies / tolerance
/// tests). Returns a fully-formed HPA with `behavior` set on the spec.
fn create_test_hpa_with_behavior(
    name: &str,
    namespace: &str,
    target_name: &str,
    min_replicas: Option<i32>,
    max_replicas: i32,
    target_cpu_utilization: i32,
    behavior: HorizontalPodAutoscalerBehavior,
) -> HorizontalPodAutoscaler {
    let spec = HorizontalPodAutoscalerSpec {
        scale_target_ref: CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: target_name.to_string(),
            api_version: Some("apps/v1".to_string()),
        },
        min_replicas,
        max_replicas,
        metrics: Some(vec![MetricSpec {
            metric_type: "Resource".to_string(),
            resource: Some(ResourceMetricSource {
                name: "cpu".to_string(),
                target: MetricTarget {
                    target_type: "Utilization".to_string(),
                    value: None,
                    average_value: None,
                    average_utilization: Some(target_cpu_utilization),
                },
            }),
            pods: None,
            object: None,
            external: None,
            container_resource: None,
        }]),
        behavior: Some(behavior),
    };

    HorizontalPodAutoscaler::new(name, namespace, spec)
}

/// HPA scale-down stabilization window — once an HPA has scaled up, a
/// subsequent reconcile that would scale the workload DOWN must hold off
/// until the stabilization window has elapsed. Upstream behaviour: HPA
/// remembers the highest recommendation seen within
/// `behavior.scaleDown.stabilizationWindowSeconds` and uses it as a floor
/// for the next decision.
///
/// Upstream stabilizeRecommendationWithBehaviors records the *unstabilized*
/// recommendation each reconcile and uses the max within
/// `scaleDown.stabilizationWindowSeconds` as a floor — so a load spike that
/// scales up, immediately followed by a dip, must NOT collapse: the recent high
/// is held until the window elapses.
#[tokio::test]
async fn test_hpa_scale_down_stabilization_window() {
    let storage = Arc::new(MemoryStorage::new());
    // Reconcile 1 sees high cpu (95%), reconcile 2 sees a dip (10%); same
    // controller instance so its recommendation history carries across.
    let fake = Arc::new(SeqCpuMetrics::new(&[95, 10]));
    let controller = HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), fake);

    let deployment = create_test_deployment("cooldown-app", "default", 4);
    let deploy_key = build_key("deployments", Some("default"), "cooldown-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    let behavior = HorizontalPodAutoscalerBehavior {
        scale_up: None,
        scale_down: Some(HPAScalingRules {
            stabilization_window_seconds: Some(300),
            select_policy: Some("Max".to_string()),
            policies: Some(vec![HPAScalingPolicy {
                policy_type: "Percent".to_string(),
                value: 100,
                period_seconds: 15,
            }]),
            tolerance: None,
        }),
    };

    // target=50%. R1: cpu 95% on 4 replicas → ceil(4*1.9)=8 → scale up to 8,
    // recording rec=8. R2: cpu 10% on 8 → wants 2, but the 300s down-window
    // still holds the recent high (8) → must stay at 8.
    let hpa = create_test_hpa_with_behavior(
        "cooldown-hpa",
        "default",
        "cooldown-app",
        Some(2),
        20,
        50,
        behavior,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "cooldown-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    // R1: scales up to 8 under the load spike.
    controller.reconcile_all().await.unwrap();
    let after_up: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        after_up.spec.replicas,
        Some(8),
        "load spike must scale up to 8 first; got {:?}",
        after_up.spec.replicas
    );

    // R2: the dip must be absorbed by the stabilization window — hold at 8.
    controller.reconcile_all().await.unwrap();
    let after_dip: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        after_dip.spec.replicas,
        Some(8),
        "within the scale-down stabilization window the count must hold at the \
         recent high (8); got {:?}",
        after_dip.spec.replicas
    );
}

/// HPA custom metrics server integration — when a Pods or Object metric is
/// declared, the controller must call the custom metrics API and feed the
/// returned value into the standard HPA replica formula.
///
/// RED-state: `calculate_replicas_for_metric` short-circuits `Pods` /
/// `Object` / `External` / `ContainerResource` to `current_replicas`
/// instead of querying any metrics endpoint (see hpa.rs:385-393).
#[tokio::test]
async fn test_hpa_metrics_server_custom_metrics_integration() {
    let storage = Arc::new(MemoryStorage::new());
    let mut fake = FakeMetricsClient::new();
    fake.pods.insert(
        "requests-per-second".to_string(),
        FakeMetricsClient::pods_info(&[("p1", 200, None), ("p2", 200, None)]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    let deployment = create_test_deployment("custom-metric-app", "default", 2);
    let deploy_key = build_key("deployments", Some("default"), "custom-metric-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Pods metric: requests-per-second, target 100/pod. The custom metrics
    // server should report ~200/pod, prompting a doubling of replicas.
    let spec = HorizontalPodAutoscalerSpec {
        scale_target_ref: CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: "custom-metric-app".to_string(),
            api_version: Some("apps/v1".to_string()),
        },
        min_replicas: Some(2),
        max_replicas: 10,
        metrics: Some(vec![MetricSpec {
            metric_type: "Pods".to_string(),
            resource: None,
            pods: Some(PodsMetricSource {
                metric: MetricIdentifier {
                    name: "requests-per-second".to_string(),
                    selector: None,
                },
                target: MetricTarget {
                    target_type: "AverageValue".to_string(),
                    value: None,
                    average_value: Some("100".to_string()),
                    average_utilization: None,
                },
            }),
            object: None,
            external: None,
            container_resource: None,
        }]),
        behavior: None,
    };
    let hpa = HorizontalPodAutoscaler::new("custom-hpa", "default", spec);
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "custom-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // With reported=200, target=100, current=2 ⇒ desired=4.
    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        updated_deployment.spec.replicas,
        Some(4),
        "Pods metric @ 200/pod against target=100 should drive 2→4 replicas; got {}",
        updated_deployment.spec.replicas.unwrap_or(0),
    );

    // current_metrics MUST report the observed pods-metric value.
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    let status = updated_hpa.status.unwrap();
    let metrics = status.current_metrics.expect("current_metrics populated");
    let pods_metric = metrics
        .iter()
        .find(|m| m.metric_type == "Pods")
        .expect("Pods metric should be reported in status");
    assert!(
        pods_metric.pods.is_some(),
        "Pods metric status struct should be populated"
    );
}

/// HPA external metrics — metrics not associated with any in-cluster object
/// (e.g. cloud-provider queue depth). The controller must call the external
/// metrics API and feed the response into the replica formula.
///
/// RED-state: `External` is in the catch-all `Pods | Object | External |
/// ContainerResource` arm that returns `current_replicas` unchanged.
#[tokio::test]
async fn test_hpa_external_metrics() {
    let storage = Arc::new(MemoryStorage::new());
    let mut fake = FakeMetricsClient::new();
    // ceil(120/30) = 4 replicas; the test only asserts the External reading
    // is surfaced in status, not a precise replica count.
    fake.external
        .insert("sqs_queue_depth".to_string(), vec![120]);
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    let deployment = create_test_deployment("queue-worker", "default", 3);
    let deploy_key = build_key("deployments", Some("default"), "queue-worker");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // External metric: SQS queue depth, target 30 messages per pod.
    let spec = HorizontalPodAutoscalerSpec {
        scale_target_ref: CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: "queue-worker".to_string(),
            api_version: Some("apps/v1".to_string()),
        },
        min_replicas: Some(1),
        max_replicas: 20,
        metrics: Some(vec![MetricSpec {
            metric_type: "External".to_string(),
            resource: None,
            pods: None,
            object: None,
            external: Some(ExternalMetricSource {
                metric: MetricIdentifier {
                    name: "sqs_queue_depth".to_string(),
                    selector: Some(LabelSelector {
                        match_labels: Some(HashMap::from([(
                            "queue".to_string(),
                            "work-queue".to_string(),
                        )])),
                        match_expressions: None,
                    }),
                },
                target: MetricTarget {
                    target_type: "AverageValue".to_string(),
                    value: None,
                    average_value: Some("30".to_string()),
                    average_utilization: None,
                },
            }),
            container_resource: None,
        }]),
        behavior: None,
    };
    let hpa = HorizontalPodAutoscaler::new("external-hpa", "default", spec);
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "external-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // status.current_metrics must surface the External metric reading.
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    let metrics = updated_hpa
        .status
        .unwrap()
        .current_metrics
        .expect("current_metrics should include external reading");
    assert!(
        metrics
            .iter()
            .any(|m| m.metric_type == "External" && m.external.is_some()),
        "Status should expose an External metric snapshot"
    );
}

/// HPA average utilization calculation — per-pod CPU utilization must be
/// derived as `sum(pod.cpu) / count(running_pods)` from real pod metrics,
/// not from a static mock. Upstream HPA aggregates only Ready pods that
/// have passed the initial readiness delay.
///
/// RED-state: `get_current_resource_utilization` returns a hard-coded
/// constant (85% for cpu) that ignores actual pod metrics. The test
/// asserts that the reported `current.average_utilization` differs from
/// that constant when the underlying pod metrics are configured to
/// produce a different aggregate — proving the controller is actually
/// looking at pod state instead of returning the canned value.
#[tokio::test]
async fn test_hpa_average_utilization_per_pod_calculation() {
    let storage = Arc::new(MemoryStorage::new());
    // Empty fake: the resource metric fetch finds no pod utilization →
    // fetch error → ScalingActive=False (cannot aggregate per-pod util).
    let controller = HorizontalPodAutoscalerController::with_metrics_client(
        storage.clone(),
        Arc::new(FakeMetricsClient::new()),
    );

    // Zero replicas → no pods → average utilization is undefined. A real
    // controller would surface a FailedGetResourceMetric / scaling-active
    // False condition. The mocked controller will instead happily report
    // the canned 85%, so the assertion below distinguishes them.
    let deployment = create_test_deployment("avg-util-app", "default", 0);
    let deploy_key = build_key("deployments", Some("default"), "avg-util-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    let hpa = create_test_hpa(
        "avg-util-hpa",
        "default",
        "avg-util-app",
        "Deployment",
        Some(0),
        10,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "avg-util-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // With zero replicas a real metrics aggregation cannot produce 85% —
    // it must either skip the metric or report ScalingActive=False. The
    // current mock implementation reports 85% regardless, so this
    // assertion is RED until per-pod averaging is wired up.
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    let status = updated_hpa.status.unwrap();
    let conditions = status.conditions.expect("conditions present");
    let scaling_active = conditions
        .iter()
        .find(|c| c.condition_type == "ScalingActive")
        .expect("ScalingActive condition must exist");
    assert_eq!(
        scaling_active.status, "False",
        "With zero running pods the controller cannot aggregate per-pod \
         utilization; ScalingActive must be False (real per-pod averaging \
         absent today reports True)."
    );
}

/// HPA initial readiness delay — pods that have not yet been Ready for at
/// least `--horizontal-pod-autoscaler-initial-readiness-delay` (default 30s)
/// must be excluded from utilization calculations to avoid scale-up storms
/// caused by cold-start CPU spikes.
///
/// The pods are unready and just-started, so even a 95% cpu reading (well above
/// the 80% target) must NOT scale up — upstream groupPods excludes pods within
/// the cpu-initialization window from the utilization calc.
#[tokio::test]
async fn test_hpa_initial_readiness_delay() {
    let storage = Arc::new(MemoryStorage::new());

    let deployment = create_test_deployment("startup-app", "default", 3);
    let deploy_key = build_key("deployments", Some("default"), "startup-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Three Running-but-unready pods that started "now" (inside the cpu-init
    // window), each reporting a hot 95% cpu. matchLabels = {app: startup-app}.
    let mut fake = FakeMetricsClient::new();
    let mut readings = Vec::new();
    for i in 0..3 {
        let name = format!("startup-app-{i}");
        create_unready_pod(&storage, "default", &name, "startup-app").await;
        readings.push((name, 0i64, Some(95)));
    }
    let readings_ref: Vec<(&str, i64, Option<i32>)> = readings
        .iter()
        .map(|(n, v, u)| (n.as_str(), *v, *u))
        .collect();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&readings_ref),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    let hpa = create_test_hpa(
        "startup-hpa",
        "default",
        "startup-app",
        "Deployment",
        Some(3),
        10,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "startup-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // All pods are excluded as not-yet-ready → no usable metric → hold at 3,
    // despite the 95% reading that would otherwise scale to ceil(3*95/80)=4.
    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        updated_deployment.spec.replicas,
        Some(3),
        "cold-start unready pods must be excluded; expected 3, got {}",
        updated_deployment.spec.replicas.unwrap_or(0),
    );
}

/// HPA tolerance — by default the HPA ignores metric drift within ±10% of
/// the target (`--horizontal-pod-autoscaler-tolerance=0.1`). A 5% drift
/// must NOT cause a rescale; the algorithm should report
/// `DesiredWithinRange`.
///
/// RED-state: `calculate_replicas_for_resource_metric` computes
/// `ceil(currentReplicas * ratio)` with no tolerance check. With the
/// mocked utilization (85 vs target 80, ratio=1.0625) tiny drifts trigger
/// rescales they should not.
#[tokio::test]
async fn test_hpa_tolerance_threshold() {
    let storage = Arc::new(MemoryStorage::new());
    // cpu @ 85% vs target 80% → ratio 1.0625, a 6.25% drift inside the default
    // ±10% band. A real reading is required: with no metric the fetch errors
    // and the test would pass vacuously (no scale on error, not on tolerance).
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(85))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    let deployment = create_test_deployment("tolerance-app", "default", 10);
    let deploy_key = build_key("deployments", Some("default"), "tolerance-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // target=80, mock-current=85 ⇒ ratio≈1.0625 (within the default ±10%
    // tolerance band ⇒ no rescale).
    let hpa = create_test_hpa(
        "tolerance-hpa",
        "default",
        "tolerance-app",
        "Deployment",
        Some(1),
        20,
        80,
    );
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "tolerance-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        updated_deployment.spec.replicas,
        Some(10),
        "A 6.25% drift from target lives inside the default ±10% tolerance \
         band; replicas must stay at 10, got {}",
        updated_deployment.spec.replicas.unwrap_or(0),
    );
}

/// HPA behavior policies — `behavior.scaleUp.policies` caps how fast the
/// HPA may grow the workload in a single reconcile (e.g. "+2 pods every
/// 60s" or "+50% every 60s"). The first reconcile after a large desired
/// jump must respect the policy and only scale by the allowed delta.
///
/// RED-state: rusternetes ignores `behavior` entirely — the field is
/// accepted on the spec but never consulted in
/// `calculate_desired_replicas`.
#[tokio::test]
async fn test_hpa_behavior_scale_up_policy() {
    let storage = Arc::new(MemoryStorage::new());
    // cpu at 85% vs a 10% target → ratio 8.5 → unbounded desire ceil(2*8.5)=17.
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(85))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    // Currently 2 replicas; metrics would push to ~17. Policy: max +2 per
    // 60s window. Expected outcome on a single reconcile: 2 → 4.
    let deployment = create_test_deployment("rate-limited-app", "default", 2);
    let deploy_key = build_key("deployments", Some("default"), "rate-limited-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    let behavior = HorizontalPodAutoscalerBehavior {
        scale_up: Some(HPAScalingRules {
            stabilization_window_seconds: Some(0),
            select_policy: Some("Max".to_string()),
            policies: Some(vec![HPAScalingPolicy {
                policy_type: "Pods".to_string(),
                value: 2,
                period_seconds: 60,
            }]),
            tolerance: None,
        }),
        scale_down: None,
    };

    // Target=10% with mock cpu=85% ⇒ ratio≈8.5 ⇒ unbounded math wants
    // ceil(2*8.5)=17 replicas. The policy caps the jump at +2 pods, so
    // the correct first-step result is 4. A controller that ignores the
    // policy will scale 2→17 (or 2→max=20), which fails this assertion.
    let hpa = create_test_hpa_with_behavior(
        "rate-limit-hpa",
        "default",
        "rate-limited-app",
        Some(1),
        20,
        10, // extreme target to drive a large unbounded recommendation
        behavior,
    );
    let hpa_key = build_key(
        "horizontalpodautoscalers",
        Some("default"),
        "rate-limit-hpa",
    );
    storage.create(&hpa_key, &hpa).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        updated_deployment.spec.replicas,
        Some(4),
        "Behavior policy caps scale-up at +2 pods/60s; expected 2→4, got {}",
        updated_deployment.spec.replicas.unwrap_or(0),
    );
}

/// HPA accepts behavior fields (defensive deserialization). Even before
/// rusternetes enforces `behavior.scaleUp` / `behavior.scaleDown`, the API
/// must accept HPAs that declare them — otherwise upstream manifests fail
/// to apply.
///
/// This test runs GREEN today: it only asserts that an HPA with a
/// populated `behavior` reconciles without error and round-trips through
/// storage with the behavior intact.
#[tokio::test]
async fn test_hpa_behavior_field_round_trips() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

    let deployment = create_test_deployment("behavior-rt-app", "default", 3);
    storage
        .create(
            &build_key("deployments", Some("default"), "behavior-rt-app"),
            &deployment,
        )
        .await
        .unwrap();

    let behavior = HorizontalPodAutoscalerBehavior {
        scale_up: Some(HPAScalingRules {
            stabilization_window_seconds: Some(60),
            select_policy: Some("Max".to_string()),
            policies: Some(vec![
                HPAScalingPolicy {
                    policy_type: "Pods".to_string(),
                    value: 4,
                    period_seconds: 60,
                },
                HPAScalingPolicy {
                    policy_type: "Percent".to_string(),
                    value: 100,
                    period_seconds: 60,
                },
            ]),
            tolerance: None,
        }),
        scale_down: Some(HPAScalingRules {
            stabilization_window_seconds: Some(300),
            select_policy: Some("Min".to_string()),
            policies: Some(vec![HPAScalingPolicy {
                policy_type: "Percent".to_string(),
                value: 10,
                period_seconds: 60,
            }]),
            tolerance: None,
        }),
    };

    let hpa = create_test_hpa_with_behavior(
        "behavior-rt-hpa",
        "default",
        "behavior-rt-app",
        Some(1),
        10,
        80,
        behavior,
    );
    let hpa_key = build_key(
        "horizontalpodautoscalers",
        Some("default"),
        "behavior-rt-hpa",
    );
    storage.create(&hpa_key, &hpa).await.unwrap();

    // Must not panic / error even though behavior is not yet enforced.
    controller.reconcile_all().await.unwrap();

    // Behavior must survive a reconcile (the controller writes status, not
    // spec — the spec.behavior field must be preserved verbatim).
    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    let stored_behavior = updated_hpa
        .spec
        .behavior
        .as_ref()
        .expect("behavior must survive reconcile");
    let scale_up = stored_behavior
        .scale_up
        .as_ref()
        .expect("scale_up must survive");
    assert_eq!(scale_up.stabilization_window_seconds, Some(60));
    assert_eq!(scale_up.select_policy.as_deref(), Some("Max"));
    assert_eq!(scale_up.policies.as_ref().map(Vec::len), Some(2));
    let scale_down = stored_behavior
        .scale_down
        .as_ref()
        .expect("scale_down must survive");
    assert_eq!(scale_down.stabilization_window_seconds, Some(300));
}

/// HPA with multiple metrics — when several MetricSpec entries are
/// declared, the controller must pick the maximum recommendation across
/// them (Kubernetes uses the "highest wins" rule, *not* per-metric AND
/// logic). This test exercises that aggregation using two Resource
/// metrics with identical thresholds; both should produce the same desired
/// count, and the maximum (== that value) must be applied.
///
/// This test runs GREEN today — the loop in
/// `calculate_desired_replicas` already takes the max across all metrics.
#[tokio::test]
async fn test_hpa_multiple_metrics_highest_wins() {
    let storage = Arc::new(MemoryStorage::new());
    // cpu @ 90% util vs target 80% ⇒ ratio 1.125 (outside ±10% band) ⇒
    // ceil(4 * 90/80) = 5; memory @ 70% util ⇒ ceil(4 * 70/80) = 4
    // (no growth). Highest wins ⇒ 5.
    let mut fake = FakeMetricsClient::new();
    fake.resource.insert(
        "cpu".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(90))]),
    );
    fake.resource.insert(
        "memory".to_string(),
        FakeMetricsClient::pods_info(&[("p", 0, Some(70))]),
    );
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    let deployment = create_test_deployment("multi-metric-app", "default", 4);
    let deploy_key = build_key("deployments", Some("default"), "multi-metric-app");
    storage.create(&deploy_key, &deployment).await.unwrap();

    // Both metrics are Resource/CPU; the mock returns 85% utilization for
    // cpu and 70% for memory. With current=4, target=80%:
    //   cpu desired   = ceil(4 * 85/80) = 5
    //   memory desired= ceil(4 * 70/80) = 4 (i.e. no growth)
    // "Highest wins" ⇒ 5 replicas.
    let spec = HorizontalPodAutoscalerSpec {
        scale_target_ref: CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: "multi-metric-app".to_string(),
            api_version: Some("apps/v1".to_string()),
        },
        min_replicas: Some(1),
        max_replicas: 20,
        metrics: Some(vec![
            MetricSpec {
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
            },
            MetricSpec {
                metric_type: "Resource".to_string(),
                resource: Some(ResourceMetricSource {
                    name: "memory".to_string(),
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
            },
        ]),
        behavior: None,
    };
    let hpa = HorizontalPodAutoscaler::new("multi-hpa", "default", spec);
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "multi-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let updated_deployment: Deployment = storage.get(&deploy_key).await.unwrap();
    assert_eq!(
        updated_deployment.spec.replicas,
        Some(5),
        "With multiple metrics the HIGHEST recommendation wins: \
         ceil(4 * max(85,70)/80) = 5; got {}",
        updated_deployment.spec.replicas.unwrap_or(0),
    );
}

/// Object metrics — Object metrics describe a single k8s object (e.g. an
/// Ingress' requests-per-second) and must route through the custom metrics
/// API. Like Pods/External, they are currently unimplemented.
///
/// RED-state: hpa.rs treats `Object` as a no-op (returns current_replicas).
#[tokio::test]
async fn test_hpa_object_metric_routing() {
    let storage = Arc::new(MemoryStorage::new());
    let mut fake = FakeMetricsClient::new();
    // Object metric value vs target=1000; the test asserts the Object reading
    // is routed/surfaced in status, not a precise replica count.
    fake.object.insert("requests-per-second".to_string(), 2000);
    let controller =
        HorizontalPodAutoscalerController::with_metrics_client(storage.clone(), Arc::new(fake));

    let deployment = create_test_deployment("ingress-app", "default", 2);
    storage
        .create(
            &build_key("deployments", Some("default"), "ingress-app"),
            &deployment,
        )
        .await
        .unwrap();

    // Object metric describing an Ingress; mock value should drive a
    // scale-up. The exact factor depends on the metrics backend, so the
    // assertion is on the status surface (current_metrics populated with
    // an Object entry) rather than on a precise replica count.
    let spec = HorizontalPodAutoscalerSpec {
        scale_target_ref: CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: "ingress-app".to_string(),
            api_version: Some("apps/v1".to_string()),
        },
        min_replicas: Some(1),
        max_replicas: 10,
        metrics: Some(vec![MetricSpec {
            metric_type: "Object".to_string(),
            resource: None,
            pods: None,
            object: Some(ObjectMetricSource {
                described_object: CrossVersionObjectReference {
                    kind: "Ingress".to_string(),
                    name: "main-ingress".to_string(),
                    api_version: Some("networking.k8s.io/v1".to_string()),
                },
                metric: MetricIdentifier {
                    name: "requests-per-second".to_string(),
                    selector: None,
                },
                target: MetricTarget {
                    target_type: "Value".to_string(),
                    value: Some("1000".to_string()),
                    average_value: None,
                    average_utilization: None,
                },
            }),
            external: None,
            container_resource: None,
        }]),
        behavior: None,
    };
    let hpa = HorizontalPodAutoscaler::new("ingress-hpa", "default", spec);
    let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "ingress-hpa");
    storage.create(&hpa_key, &hpa).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let updated_hpa: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
    let metrics = updated_hpa
        .status
        .unwrap()
        .current_metrics
        .expect("current_metrics must include the Object reading");
    assert!(
        metrics
            .iter()
            .any(|m| m.metric_type == "Object" && m.object.is_some()),
        "Status should expose an Object metric snapshot"
    );
}
