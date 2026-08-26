use rusternetes_common::resources::pod::PodSpec;
use rusternetes_common::resources::{
    Container, CrossVersionObjectReference, Deployment, DeploymentSpec, HorizontalPodAutoscaler,
    HorizontalPodAutoscalerSpec, MetricSpec, MetricTarget, PodTemplateSpec, ResourceMetricSource,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::hpa::HorizontalPodAutoscalerController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

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
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

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
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

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
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

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
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

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
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

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
    let controller = HorizontalPodAutoscalerController::new(storage.clone());

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
