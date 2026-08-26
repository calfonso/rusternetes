// Integration tests for Deployment Controller
// Tests verify that Deployment creates and manages ReplicaSets (Deployment -> ReplicaSet -> Pods)

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

/// Mark all pods in a namespace as Running and Ready
async fn make_all_pods_ready(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for mut pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            ..Default::default()
        });
        let key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
        let _ = storage.update(&key, &pod).await;
    }
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
                        security_context: None,
                        working_dir: None,
                        command: None,
                        args: None,
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
                    restart_policy: Some("Always".to_string()),
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
            paused: None,
            progress_deadline_seconds: None,
        },
        status: Some(DeploymentStatus {
            replicas: Some(0),
            ready_replicas: Some(0),
            available_replicas: Some(0),
            unavailable_replicas: Some(0),
            updated_replicas: Some(0),
            conditions: None,
            collision_count: None,
            observed_generation: None,
            terminating_replicas: None,
        }),
    }
}

#[tokio::test]
async fn test_deployment_creates_replicaset() {
    let storage = setup_test().await;

    // Create deployment with 3 replicas
    let deployment = create_test_deployment("nginx", "default", 3);
    let key = build_key("deployments", Some("default"), "nginx");
    storage.create(&key, &deployment).await.unwrap();

    // Run controller
    let controller = DeploymentController::new(storage.clone(), 10);
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify 1 ReplicaSet created (Deployment -> ReplicaSet -> Pods is the correct architecture)
    let replicasets: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(replicasets.len(), 1, "Should create 1 ReplicaSet");

    // Verify ReplicaSet has correct spec
    let rs = &replicasets[0];
    assert_eq!(rs.spec.replicas, 3, "ReplicaSet should have 3 replicas");

    // Verify ReplicaSet has owner reference to Deployment
    let owner_refs = rs
        .metadata
        .owner_references
        .as_ref()
        .expect("ReplicaSet should have owner references");
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0].kind, "Deployment");
    assert_eq!(owner_refs[0].name, "nginx");
    assert_eq!(owner_refs[0].controller, Some(true));
}

#[tokio::test]
async fn test_deployment_scales_up_replicaset() {
    let storage = setup_test().await;

    // Create deployment with 2 replicas
    let mut deployment = create_test_deployment("app", "default", 2);
    let key = build_key("deployments", Some("default"), "app");
    storage.create(&key, &deployment).await.unwrap();

    // Run controller to create initial ReplicaSet
    let controller = DeploymentController::new(storage.clone(), 10);
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify 1 ReplicaSet created with 2 replicas
    let replicasets: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(replicasets.len(), 1, "Should create 1 ReplicaSet");
    assert_eq!(
        replicasets[0].spec.replicas, 2,
        "ReplicaSet should have 2 replicas"
    );

    // Update deployment to 5 replicas
    deployment.spec.replicas = Some(5);
    storage.update(&key, &deployment).await.unwrap();

    // Run controller again
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify ReplicaSet scaled to 5 replicas
    let replicasets: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(replicasets.len(), 1, "Should still have 1 ReplicaSet");
    assert_eq!(
        replicasets[0].spec.replicas, 5,
        "ReplicaSet should scale to 5 replicas"
    );
}

#[tokio::test]
async fn test_deployment_scales_down_replicaset() {
    let storage = setup_test().await;

    // Create deployment with 5 replicas
    let mut deployment = create_test_deployment("app", "default", 5);
    let key = build_key("deployments", Some("default"), "app");
    storage.create(&key, &deployment).await.unwrap();

    // Run controller to create initial ReplicaSet
    let controller = DeploymentController::new(storage.clone(), 10);
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify ReplicaSet created with 5 replicas
    let replicasets: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(replicasets.len(), 1, "Should create 1 ReplicaSet");
    assert_eq!(
        replicasets[0].spec.replicas, 5,
        "ReplicaSet should have 5 replicas"
    );

    // Update deployment to 2 replicas
    deployment.spec.replicas = Some(2);
    storage.update(&key, &deployment).await.unwrap();

    // Run controller again
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify ReplicaSet scaled down to 2 replicas
    let replicasets: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(replicasets.len(), 1, "Should still have 1 ReplicaSet");
    assert_eq!(
        replicasets[0].spec.replicas, 2,
        "ReplicaSet should scale down to 2 replicas"
    );
}

#[tokio::test]
async fn test_deployment_template_change_creates_new_replicaset() {
    let storage = setup_test().await;

    // Create deployment with nginx:1.25
    let mut deployment = create_test_deployment("app", "default", 3);
    let key = build_key("deployments", Some("default"), "app");
    storage.create(&key, &deployment).await.unwrap();

    // Run controller to create initial ReplicaSet
    let controller = DeploymentController::new(storage.clone(), 10);
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify 1 ReplicaSet created
    let replicasets: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(replicasets.len(), 1, "Should create 1 ReplicaSet");
    let old_rs_name = replicasets[0].metadata.name.clone();

    // Change pod template (update image) to trigger rolling update
    deployment.spec.template.spec.containers[0].image = "nginx:1.26-alpine".to_string();
    storage.update(&key, &deployment).await.unwrap();

    // Run controllers multiple times to complete the rolling update.
    // The RS controller creates pods from ReplicaSets, then we mark them Ready,
    // then the deployment controller can progress the rollout.
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);
    for _ in 0..10 {
        rs_controller.reconcile_all().await.unwrap();
        make_all_pods_ready(&storage, "default").await;
        controller.reconcile_all().await.unwrap();
        sleep(Duration::from_millis(100)).await;
    }

    // Verify 2 ReplicaSets exist (old scaled to 0, new with 3 replicas)
    let replicasets: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(
        replicasets.len(),
        2,
        "Should have 2 ReplicaSets after template change"
    );

    // Find new and old ReplicaSets
    let new_rs = replicasets
        .iter()
        .find(|rs| rs.metadata.name != old_rs_name)
        .expect("Should find new ReplicaSet");
    let old_rs = replicasets
        .iter()
        .find(|rs| rs.metadata.name == old_rs_name)
        .expect("Should find old ReplicaSet");

    assert_eq!(
        new_rs.spec.replicas, 3,
        "New ReplicaSet should have 3 replicas"
    );
    assert_eq!(
        old_rs.spec.replicas, 0,
        "Old ReplicaSet should be scaled to 0"
    );
}

#[tokio::test]
async fn test_deployment_zero_replicas() {
    let storage = setup_test().await;

    // Create deployment with 0 replicas
    let deployment = create_test_deployment("app", "default", 0);
    let key = build_key("deployments", Some("default"), "app");
    storage.create(&key, &deployment).await.unwrap();

    // Run controller
    let controller = DeploymentController::new(storage.clone(), 10);
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify ReplicaSet created with 0 replicas
    let replicasets: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(replicasets.len(), 1, "Should create 1 ReplicaSet");
    assert_eq!(
        replicasets[0].spec.replicas, 0,
        "ReplicaSet should have 0 replicas"
    );
}

#[tokio::test]
async fn test_deployment_multiple_namespaces() {
    let storage = setup_test().await;

    // Create deployment in namespace1
    let deployment1 = create_test_deployment("app", "namespace1", 2);
    let key1 = build_key("deployments", Some("namespace1"), "app");
    storage.create(&key1, &deployment1).await.unwrap();

    // Create deployment in namespace2
    let deployment2 = create_test_deployment("app", "namespace2", 3);
    let key2 = build_key("deployments", Some("namespace2"), "app");
    storage.create(&key2, &deployment2).await.unwrap();

    // Run controller
    let controller = DeploymentController::new(storage.clone(), 10);
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify ReplicaSets in namespace1
    let rs1: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/namespace1/")
        .await
        .unwrap();
    assert_eq!(rs1.len(), 1, "Should create 1 ReplicaSet in namespace1");
    assert_eq!(
        rs1[0].spec.replicas, 2,
        "Namespace1 ReplicaSet should have 2 replicas"
    );

    // Verify ReplicaSets in namespace2
    let rs2: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/namespace2/")
        .await
        .unwrap();
    assert_eq!(rs2.len(), 1, "Should create 1 ReplicaSet in namespace2");
    assert_eq!(
        rs2[0].spec.replicas, 3,
        "Namespace2 ReplicaSet should have 3 replicas"
    );
}
