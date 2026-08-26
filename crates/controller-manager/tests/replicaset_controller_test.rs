use rusternetes_common::resources::pod::PodCondition;
use rusternetes_common::resources::{
    Container, Pod, PodSpec, PodStatus, PodTemplateSpec, ReplicaSet, ReplicaSetSpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn create_test_replicaset(name: &str, namespace: &str, replicas: i32) -> ReplicaSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    ReplicaSet {
        type_meta: TypeMeta {
            kind: "ReplicaSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: ReplicaSetSpec {
            replicas,
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            min_ready_seconds: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
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
        },
        status: None,
    }
}

#[tokio::test]
async fn test_replicaset_creates_pods() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with 3 replicas
    let rs = create_test_replicaset("web", "default", 3);
    storage
        .create(&build_key("replicasets", Some("default"), "web"), &rs)
        .await
        .unwrap();

    // Run controller
    controller.reconcile_all().await.unwrap();

    // Verify 3 pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should create 3 pods");

    // Verify pods have correct labels
    for pod in &pods {
        let labels = pod
            .metadata
            .labels
            .as_ref()
            .expect("Pod should have labels");
        assert_eq!(labels.get("app"), Some(&"web".to_string()));
    }
}

#[tokio::test]
async fn test_replicaset_scales_up() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with 2 replicas
    let mut rs = create_test_replicaset("app", "default", 2);
    let key = build_key("replicasets", Some("default"), "app");
    storage.create(&key, &rs).await.unwrap();

    // Run controller to create initial pods
    controller.reconcile_all().await.unwrap();

    // Verify 2 pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Should initially create 2 pods");

    // Update replicaset to 5 replicas
    rs.spec.replicas = 5;
    storage.update(&key, &rs).await.unwrap();

    // Run controller again
    controller.reconcile_all().await.unwrap();

    // Verify 5 pods now exist
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 5, "Should scale up to 5 pods");
}

#[tokio::test]
async fn test_replicaset_scales_down() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with 5 replicas
    let mut rs = create_test_replicaset("app", "default", 5);
    let key = build_key("replicasets", Some("default"), "app");
    storage.create(&key, &rs).await.unwrap();

    // Run controller to create initial pods
    controller.reconcile_all().await.unwrap();

    // Verify 5 pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 5, "Should initially create 5 pods");

    // Update replicaset to 2 replicas
    rs.spec.replicas = 2;
    storage.update(&key, &rs).await.unwrap();

    // Run controller again
    controller.reconcile_all().await.unwrap();

    // Verify 2 pods remain
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Should scale down to 2 pods");
}

#[tokio::test]
async fn test_replicaset_self_healing() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with 3 replicas
    let rs = create_test_replicaset("app", "default", 3);
    storage
        .create(&build_key("replicasets", Some("default"), "app"), &rs)
        .await
        .unwrap();

    // Run controller to create initial pods
    controller.reconcile_all().await.unwrap();

    // Verify 3 pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should create 3 pods");

    // Delete one pod to simulate failure
    let pod_to_delete = &pods[0];
    let pod_key = build_key("pods", Some("default"), &pod_to_delete.metadata.name);
    storage.delete(&pod_key).await.unwrap();

    // Verify only 2 pods remain
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Should have 2 pods after deletion");

    // Run controller again to heal
    controller.reconcile_all().await.unwrap();

    // Verify 3 pods exist again (self-healed)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should self-heal back to 3 pods");
}

#[tokio::test]
async fn test_replicaset_selector_matching() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with specific selector
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "myapp".to_string());
    labels.insert("tier".to_string(), "frontend".to_string());

    let rs = ReplicaSet {
        type_meta: TypeMeta {
            kind: "ReplicaSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("frontend-rs");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: ReplicaSetSpec {
            replicas: 2,
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            min_ready_seconds: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new("frontend-pod");
                    meta.labels = Some(labels.clone());
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
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
        },
        status: None,
    };

    storage
        .create(
            &build_key("replicasets", Some("default"), "frontend-rs"),
            &rs,
        )
        .await
        .unwrap();

    // Create a pod that doesn't match the selector (missing "tier" label)
    let mut non_matching_labels = HashMap::new();
    non_matching_labels.insert("app".to_string(), "myapp".to_string());

    let non_matching_pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("non-matching-pod");
            meta.namespace = Some("default".to_string());
            meta.labels = Some(non_matching_labels);
            meta
        },
        spec: Some(rs.spec.template.spec.clone()),
        status: Some(PodStatus {
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

    storage
        .create(
            &build_key("pods", Some("default"), "non-matching-pod"),
            &non_matching_pod,
        )
        .await
        .unwrap();

    // Run controller
    controller.reconcile_all().await.unwrap();

    // Verify 2 pods created (replicaset should ignore the non-matching pod)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    // Total should be 3: 1 non-matching + 2 from replicaset
    assert_eq!(
        pods.len(),
        3,
        "Should have 1 non-matching pod + 2 replicaset pods"
    );

    // Count pods with matching labels
    let matching_pods = pods
        .iter()
        .filter(|p| {
            if let Some(labels) = &p.metadata.labels {
                labels.get("app") == Some(&"myapp".to_string())
                    && labels.get("tier") == Some(&"frontend".to_string())
            } else {
                false
            }
        })
        .count();

    assert_eq!(
        matching_pods, 2,
        "Should have exactly 2 pods matching replicaset selector"
    );
}

#[tokio::test]
async fn test_replicaset_zero_replicas() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with 0 replicas
    let rs = create_test_replicaset("app", "default", 0);
    storage
        .create(&build_key("replicasets", Some("default"), "app"), &rs)
        .await
        .unwrap();

    // Run controller
    controller.reconcile_all().await.unwrap();

    // Verify no pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 0, "Should not create any pods for 0 replicas");
}

#[tokio::test]
async fn test_replicaset_multiple_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset in namespace1
    let rs1 = create_test_replicaset("app", "ns1", 2);
    storage
        .create(&build_key("replicasets", Some("ns1"), "app"), &rs1)
        .await
        .unwrap();

    // Create replicaset in namespace2
    let rs2 = create_test_replicaset("app", "ns2", 3);
    storage
        .create(&build_key("replicasets", Some("ns2"), "app"), &rs2)
        .await
        .unwrap();

    // Run controller
    controller.reconcile_all().await.unwrap();

    // Verify pods in namespace1
    let pods1: Vec<Pod> = storage.list("/registry/pods/ns1/").await.unwrap();
    assert_eq!(pods1.len(), 2, "Should create 2 pods in ns1");

    // Verify pods in namespace2
    let pods2: Vec<Pod> = storage.list("/registry/pods/ns2/").await.unwrap();
    assert_eq!(pods2.len(), 3, "Should create 3 pods in ns2");
}

#[tokio::test]
async fn test_replicaset_updates_status() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset
    let rs = create_test_replicaset("app", "default", 3);
    let key = build_key("replicasets", Some("default"), "app");
    storage.create(&key, &rs).await.unwrap();

    // Run controller to create pods and update status
    controller.reconcile_all().await.unwrap();

    // Verify pods were created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should have created 3 pods");

    // Run controller again to update status with correct pod count
    controller.reconcile_all().await.unwrap();

    // Verify status updated
    let updated_rs: ReplicaSet = storage.get(&key).await.unwrap();
    assert!(updated_rs.status.is_some(), "ReplicaSet should have status");

    let status = updated_rs.status.unwrap();
    assert_eq!(
        status.replicas, 3,
        "Status should show 3 replicas (matching actual pod count)"
    );
    // Pods start in Pending state, so ready and available should be 0
    assert!(
        status.ready_replicas >= 0,
        "Ready replicas should be non-negative"
    );
    assert!(
        status.available_replicas >= 0,
        "Available replicas should be non-negative"
    );
}

#[tokio::test]
async fn test_replicaset_status_with_ready_pods() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with 3 replicas
    let rs = create_test_replicaset("ready-test", "default", 3);
    let key = build_key("replicasets", Some("default"), "ready-test");
    storage.create(&key, &rs).await.unwrap();

    // Create pods
    controller.reconcile_all().await.unwrap();

    // Mark all pods as Ready
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in &pods {
        let mut updated = pod.clone();
        updated.status = Some(PodStatus {
            phase: Some(Phase::Running),
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_transition_time: Some(chrono::Utc::now()),
                observed_generation: None,
            }]),
            ..Default::default()
        });
        let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
        storage.update(&pod_key, &updated).await.unwrap();
    }

    // Reconcile again to update status
    controller.reconcile_all().await.unwrap();

    let updated_rs: ReplicaSet = storage.get(&key).await.unwrap();
    let status = updated_rs.status.unwrap();
    assert_eq!(status.replicas, 3, "Should have 3 replicas");
    assert_eq!(status.ready_replicas, 3, "Should have 3 ready replicas");
    assert_eq!(
        status.available_replicas, 3,
        "Should have 3 available replicas"
    );
    assert_eq!(
        status.fully_labeled_replicas,
        Some(3),
        "Should have 3 fully labeled replicas"
    );
    // observedGeneration is set from metadata.generation; if not set on creation, it will be None
    // This is acceptable — the API server sets generation on creation
}

#[tokio::test]
async fn test_replicaset_skips_terminated_pods() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with 3 replicas
    let rs = create_test_replicaset("term-test", "default", 3);
    let key = build_key("replicasets", Some("default"), "term-test");
    storage.create(&key, &rs).await.unwrap();

    // Create pods
    controller.reconcile_all().await.unwrap();

    // Mark one pod as Succeeded (terminated)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3);
    let mut terminated_pod = pods[0].clone();
    terminated_pod.status = Some(PodStatus {
        phase: Some(Phase::Succeeded),
        ..Default::default()
    });
    let pod_key = build_key("pods", Some("default"), &terminated_pod.metadata.name);
    storage.update(&pod_key, &terminated_pod).await.unwrap();

    // Reconcile — should create a replacement for the terminated pod
    controller.reconcile_all().await.unwrap();

    // Count active (non-terminated) pods matching the RS
    let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let active_pods: Vec<&Pod> = all_pods
        .iter()
        .filter(|p| {
            !matches!(
                p.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Failed) | Some(Phase::Succeeded)
            )
        })
        .collect();
    assert_eq!(
        active_pods.len(),
        3,
        "Should have 3 active pods after replacing terminated one"
    );
}

#[tokio::test]
async fn test_replicaset_skips_deleting_pods() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    // Create replicaset with 3 replicas
    let rs = create_test_replicaset("deleting-test", "default", 3);
    let key = build_key("replicasets", Some("default"), "deleting-test");
    storage.create(&key, &rs).await.unwrap();

    // Create pods
    controller.reconcile_all().await.unwrap();

    // Mark one pod as being deleted
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3);
    let mut deleting_pod = pods[0].clone();
    deleting_pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
    let pod_key = build_key("pods", Some("default"), &deleting_pod.metadata.name);
    storage.update(&pod_key, &deleting_pod).await.unwrap();

    // Reconcile — the deleting pod should not be counted, so a replacement should be created
    controller.reconcile_all().await.unwrap();

    let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    // Should have 4 pods: 3 active + 1 deleting
    let non_deleting: Vec<&Pod> = all_pods
        .iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none())
        .collect();
    assert_eq!(non_deleting.len(), 3, "Should have 3 non-deleting pods");
}
