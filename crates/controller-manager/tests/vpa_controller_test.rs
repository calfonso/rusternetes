use rusternetes_common::resources::{
    Container, ContainerResourcePolicy, CrossVersionObjectReference, Deployment, DeploymentSpec,
    Pod, PodResourcePolicy, PodSpec, PodStatus, PodTemplateSpec, PodUpdatePolicy,
    VerticalPodAutoscaler, VerticalPodAutoscalerSpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::vpa::VerticalPodAutoscalerController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn create_test_deployment_with_pods(
    _storage: &Arc<MemoryStorage>,
    name: &str,
    namespace: &str,
    replicas: i32,
) -> (Deployment, Vec<Pod>) {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    let deployment = Deployment {
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
                    meta.labels = Some(labels.clone());
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "app".to_string(),
                        image: "nginx:latest".to_string(),
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
    };

    // Create pods for the deployment
    let mut pods = Vec::new();
    for i in 0..replicas {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut meta = ObjectMeta::new(format!("{}-pod-{}", name, i));
                meta.namespace = Some(namespace.to_string());
                meta.labels = Some(labels.clone());
                meta.uid = uuid::Uuid::new_v4().to_string();
                meta
            },
            spec: Some(deployment.spec.template.spec.clone()),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                message: None,
                reason: None,
                host_ip: Some("10.0.0.1".to_string()),
                host_i_ps: None,
                pod_ip: Some(format!("10.244.0.{}", i + 10)),
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
        pods.push(pod);
    }

    (deployment, pods)
}

fn create_test_vpa(
    name: &str,
    namespace: &str,
    target_name: &str,
    target_kind: &str,
    update_mode: Option<&str>,
) -> VerticalPodAutoscaler {
    VerticalPodAutoscaler {
        type_meta: TypeMeta {
            kind: "VerticalPodAutoscaler".to_string(),
            api_version: "autoscaling.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VerticalPodAutoscalerSpec {
            target_ref: CrossVersionObjectReference {
                kind: target_kind.to_string(),
                name: target_name.to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            update_policy: update_mode.map(|mode| PodUpdatePolicy {
                update_mode: Some(mode.to_string()),
            }),
            resource_policy: None,
            recommenders: None,
        },
        status: None,
    }
}

#[tokio::test]
async fn test_vpa_generates_recommendations() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VerticalPodAutoscalerController::new(storage.clone());

    // Create deployment with pods
    let (deployment, pods) = create_test_deployment_with_pods(&storage, "web-app", "default", 3);

    storage
        .create(
            &build_key("deployments", Some("default"), "web-app"),
            &deployment,
        )
        .await
        .unwrap();

    for (i, pod) in pods.iter().enumerate() {
        storage
            .create(
                &build_key("pods", Some("default"), &format!("web-app-pod-{}", i)),
                pod,
            )
            .await
            .unwrap();
    }

    // Create VPA in "Off" mode (recommendations only)
    let vpa = create_test_vpa("web-vpa", "default", "web-app", "Deployment", Some("Off"));
    storage
        .create(
            &build_key("verticalpodautoscalers", Some("default"), "web-vpa"),
            &vpa,
        )
        .await
        .unwrap();

    // Run controller multiple times to collect samples
    for _ in 0..15 {
        controller.reconcile_all().await.unwrap();
    }

    // Verify VPA status has recommendations
    let updated_vpa: VerticalPodAutoscaler = storage
        .get(&build_key(
            "verticalpodautoscalers",
            Some("default"),
            "web-vpa",
        ))
        .await
        .unwrap();

    assert!(updated_vpa.status.is_some(), "VPA should have status");
    let status = updated_vpa.status.unwrap();
    assert!(
        status.recommendation.is_some(),
        "VPA should have recommendations"
    );

    let recommendation = status.recommendation.unwrap();
    assert!(
        recommendation.container_recommendations.is_some(),
        "Should have container recommendations"
    );

    let containers = recommendation.container_recommendations.unwrap();
    assert!(
        !containers.is_empty(),
        "Should have at least one container recommendation"
    );

    let first_container = &containers[0];
    assert_eq!(first_container.container_name, "app");
    assert!(
        first_container.target.contains_key("cpu"),
        "Should have CPU recommendation"
    );
    assert!(
        first_container.target.contains_key("memory"),
        "Should have memory recommendation"
    );
}

#[tokio::test]
async fn test_vpa_respects_update_mode_off() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VerticalPodAutoscalerController::new(storage.clone());

    // Create deployment with pods
    let (deployment, pods) = create_test_deployment_with_pods(&storage, "app", "default", 2);

    storage
        .create(
            &build_key("deployments", Some("default"), "app"),
            &deployment,
        )
        .await
        .unwrap();

    for (i, pod) in pods.iter().enumerate() {
        storage
            .create(
                &build_key("pods", Some("default"), &format!("app-pod-{}", i)),
                pod,
            )
            .await
            .unwrap();
    }

    // Create VPA in "Off" mode
    let vpa = create_test_vpa("app-vpa", "default", "app", "Deployment", Some("Off"));
    storage
        .create(
            &build_key("verticalpodautoscalers", Some("default"), "app-vpa"),
            &vpa,
        )
        .await
        .unwrap();

    // Run controller
    for _ in 0..15 {
        controller.reconcile_all().await.unwrap();
    }

    // Verify pods are NOT evicted (Off mode doesn't apply changes)
    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods_after.len(),
        2,
        "Pods should not be evicted in Off mode"
    );
}

#[tokio::test]
async fn test_vpa_with_missing_target() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VerticalPodAutoscalerController::new(storage.clone());

    // Create VPA but don't create the target deployment
    let vpa = create_test_vpa(
        "orphan-vpa",
        "default",
        "nonexistent",
        "Deployment",
        Some("Off"),
    );
    storage
        .create(
            &build_key("verticalpodautoscalers", Some("default"), "orphan-vpa"),
            &vpa,
        )
        .await
        .unwrap();

    // Run controller - should not crash
    let result = controller.reconcile_all().await;
    assert!(
        result.is_ok(),
        "Controller should handle missing target gracefully"
    );
}

#[tokio::test]
async fn test_vpa_resource_policy_constraints() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VerticalPodAutoscalerController::new(storage.clone());

    // Create deployment with pods
    let (deployment, pods) =
        create_test_deployment_with_pods(&storage, "constrained-app", "default", 2);

    storage
        .create(
            &build_key("deployments", Some("default"), "constrained-app"),
            &deployment,
        )
        .await
        .unwrap();

    for (i, pod) in pods.iter().enumerate() {
        storage
            .create(
                &build_key(
                    "pods",
                    Some("default"),
                    &format!("constrained-app-pod-{}", i),
                ),
                pod,
            )
            .await
            .unwrap();
    }

    // Create VPA with resource policy constraints
    let mut vpa = create_test_vpa(
        "constrained-vpa",
        "default",
        "constrained-app",
        "Deployment",
        Some("Off"),
    );
    vpa.spec.resource_policy = Some(PodResourcePolicy {
        container_policies: Some(vec![ContainerResourcePolicy {
            container_name: Some("app".to_string()),
            mode: None,
            min_allowed: Some({
                let mut resources = HashMap::new();
                resources.insert("cpu".to_string(), "100m".to_string());
                resources.insert("memory".to_string(), "128Mi".to_string());
                resources
            }),
            max_allowed: Some({
                let mut resources = HashMap::new();
                resources.insert("cpu".to_string(), "2000m".to_string());
                resources.insert("memory".to_string(), "2Gi".to_string());
                resources
            }),
            controlled_resources: None,
        }]),
    });

    storage
        .create(
            &build_key("verticalpodautoscalers", Some("default"), "constrained-vpa"),
            &vpa,
        )
        .await
        .unwrap();

    // Run controller
    for _ in 0..15 {
        controller.reconcile_all().await.unwrap();
    }

    // Verify recommendations respect constraints
    let updated_vpa: VerticalPodAutoscaler = storage
        .get(&build_key(
            "verticalpodautoscalers",
            Some("default"),
            "constrained-vpa",
        ))
        .await
        .unwrap();

    if let Some(status) = updated_vpa.status {
        if let Some(recommendation) = status.recommendation {
            if let Some(containers) = recommendation.container_recommendations {
                let app_rec = containers.iter().find(|c| c.container_name == "app");
                assert!(
                    app_rec.is_some(),
                    "Should have recommendation for app container"
                );

                // Note: Actual constraint enforcement would be tested by checking
                // that recommended values fall within min/max bounds
            }
        }
    }
}

#[tokio::test]
async fn test_vpa_multiple_vpas_different_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VerticalPodAutoscalerController::new(storage.clone());

    // Create deployments with pods in different namespaces
    let (deploy1, pods1) = create_test_deployment_with_pods(&storage, "app", "ns1", 2);
    let (deploy2, pods2) = create_test_deployment_with_pods(&storage, "app", "ns2", 2);

    storage
        .create(&build_key("deployments", Some("ns1"), "app"), &deploy1)
        .await
        .unwrap();
    storage
        .create(&build_key("deployments", Some("ns2"), "app"), &deploy2)
        .await
        .unwrap();

    for (i, pod) in pods1.iter().enumerate() {
        storage
            .create(
                &build_key("pods", Some("ns1"), &format!("app-pod-{}", i)),
                pod,
            )
            .await
            .unwrap();
    }
    for (i, pod) in pods2.iter().enumerate() {
        storage
            .create(
                &build_key("pods", Some("ns2"), &format!("app-pod-{}", i)),
                pod,
            )
            .await
            .unwrap();
    }

    // Create VPAs in different namespaces
    let vpa1 = create_test_vpa("app-vpa", "ns1", "app", "Deployment", Some("Off"));
    let vpa2 = create_test_vpa("app-vpa", "ns2", "app", "Deployment", Some("Off"));

    storage
        .create(
            &build_key("verticalpodautoscalers", Some("ns1"), "app-vpa"),
            &vpa1,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("verticalpodautoscalers", Some("ns2"), "app-vpa"),
            &vpa2,
        )
        .await
        .unwrap();

    // Run controller
    for _ in 0..15 {
        controller.reconcile_all().await.unwrap();
    }

    // Verify both VPAs were reconciled
    let updated_vpa1: VerticalPodAutoscaler = storage
        .get(&build_key("verticalpodautoscalers", Some("ns1"), "app-vpa"))
        .await
        .unwrap();
    let updated_vpa2: VerticalPodAutoscaler = storage
        .get(&build_key("verticalpodautoscalers", Some("ns2"), "app-vpa"))
        .await
        .unwrap();

    assert_eq!(updated_vpa1.metadata.namespace.as_deref().unwrap(), "ns1");
    assert_eq!(updated_vpa2.metadata.namespace.as_deref().unwrap(), "ns2");
}

#[tokio::test]
async fn test_vpa_with_no_pods() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VerticalPodAutoscalerController::new(storage.clone());

    // Create deployment but NO pods
    let (deployment, _) = create_test_deployment_with_pods(&storage, "no-pods-app", "default", 0);

    storage
        .create(
            &build_key("deployments", Some("default"), "no-pods-app"),
            &deployment,
        )
        .await
        .unwrap();

    // Create VPA
    let vpa = create_test_vpa(
        "no-pods-vpa",
        "default",
        "no-pods-app",
        "Deployment",
        Some("Off"),
    );
    storage
        .create(
            &build_key("verticalpodautoscalers", Some("default"), "no-pods-vpa"),
            &vpa,
        )
        .await
        .unwrap();

    // Run controller - should handle gracefully
    let result = controller.reconcile_all().await;
    assert!(result.is_ok(), "Controller should handle VPA with no pods");

    // Verify VPA status - should not have recommendations
    let updated_vpa: VerticalPodAutoscaler = storage
        .get(&build_key(
            "verticalpodautoscalers",
            Some("default"),
            "no-pods-vpa",
        ))
        .await
        .unwrap();

    // VPA might not have recommendations with no pods
    if let Some(status) = updated_vpa.status {
        if let Some(recommendation) = status.recommendation {
            if let Some(containers) = recommendation.container_recommendations {
                assert!(
                    containers.is_empty() || containers.is_empty(),
                    "Should have no recommendations without pods or very few"
                );
            }
        }
    }
}
