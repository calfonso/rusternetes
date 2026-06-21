// StatefulSet Controller Integration Tests
// Tests the StatefulSet controller's ability to manage stateful workloads with stable identities

use rusternetes_common::resources::pod::PodCondition;
use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::statefulset::StatefulSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

/// Simulate kubelet behavior: remove from storage any pods the controller has
/// marked for deletion. StatefulSet reconcile only sets a deletionTimestamp
/// (graceful delete handed off to kubelet). Without a real kubelet,
/// scale-down and rolling-update assertions on storage state need this helper.
async fn simulate_kubelet_cleanup(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            let key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
            let _ = storage.delete(&key).await;
        }
    }
}

fn create_test_statefulset(name: &str, namespace: &str, replicas: i32) -> StatefulSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    StatefulSet {
        type_meta: TypeMeta {
            kind: "StatefulSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: StatefulSetSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
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
                        ports: None,
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
                    ..Default::default()
                },
            },
            service_name: format!("{}-headless", name),
            pod_management_policy: Some("Parallel".to_string()),
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
            volume_claim_templates: None,
            persistent_volume_claim_retention_policy: None,
            ordinals: None,
        },
        status: Some(StatefulSetStatus {
            replicas: 0,
            ready_replicas: Some(0),
            current_replicas: Some(0),
            updated_replicas: Some(0),
            available_replicas: None,
            collision_count: None,
            observed_generation: None,
            current_revision: None,
            update_revision: None,
            conditions: None,
        }),
    }
}

#[tokio::test]
async fn test_statefulset_creates_ordered_pods() {
    let storage = setup_test().await;

    // Create statefulset with 3 replicas
    let statefulset = create_test_statefulset("web", "default", 3);
    let key = build_key("statefulsets", Some("default"), "web");
    storage.create(&key, &statefulset).await.unwrap();

    // Run controller
    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify 3 pods created with ordered names
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should create 3 pods");

    // Verify pod names are ordered: web-0, web-1, web-2
    let mut pod_names: Vec<String> = pods.iter().map(|p| p.metadata.name.clone()).collect();
    pod_names.sort();
    assert_eq!(pod_names, vec!["web-0", "web-1", "web-2"]);

    // Verify each pod has the stateful identity label
    for pod in &pods {
        let labels = pod
            .metadata
            .labels
            .as_ref()
            .expect("Pod should have labels");
        assert!(labels.contains_key("statefulset.kubernetes.io/pod-name"));
    }
}

#[tokio::test]
async fn test_statefulset_scales_up_ordered() {
    let storage = setup_test().await;

    // Create statefulset with 2 replicas
    let mut statefulset = create_test_statefulset("web", "default", 2);
    let key = build_key("statefulsets", Some("default"), "web");
    storage.create(&key, &statefulset).await.unwrap();

    // Run controller
    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify 2 pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Should create 2 pods initially");

    // Scale up to 4 replicas
    statefulset.spec.replicas = Some(4);
    storage.update(&key, &statefulset).await.unwrap();

    // Run controller again
    controller.reconcile_all().await.unwrap();

    // Verify 4 pods exist
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 4, "Should scale up to 4 pods");

    // Verify new pods are web-2 and web-3
    let mut pod_names: Vec<String> = pods.iter().map(|p| p.metadata.name.clone()).collect();
    pod_names.sort();
    assert_eq!(pod_names, vec!["web-0", "web-1", "web-2", "web-3"]);
}

#[tokio::test]
async fn test_statefulset_scales_down_reverse_order() {
    let storage = setup_test().await;

    // Create statefulset with 4 replicas
    let mut statefulset = create_test_statefulset("web", "default", 4);
    let key = build_key("statefulsets", Some("default"), "web");
    storage.create(&key, &statefulset).await.unwrap();

    // Run controller
    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify 4 pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 4);

    // Scale down to 2 replicas
    statefulset.spec.replicas = Some(2);
    storage.update(&key, &statefulset).await.unwrap();

    // Run controller multiple times (one pod deleted per cycle, matching K8s
    // behavior). Simulate kubelet between cycles so pods marked for deletion
    // are actually removed from storage and the next reconcile sees the new
    // pod count.
    for _ in 0..4 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, "default").await;
    }

    // Verify only 2 pods remain
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Should scale down to 2 pods");

    // Verify remaining pods are web-0 and web-1 (highest ordinals deleted first)
    let mut pod_names: Vec<String> = pods.iter().map(|p| p.metadata.name.clone()).collect();
    pod_names.sort();
    assert_eq!(pod_names, vec!["web-0", "web-1"]);
}

#[tokio::test]
async fn test_statefulset_updates_status() {
    let storage = setup_test().await;

    // Create statefulset
    let statefulset = create_test_statefulset("status-test", "default", 3);
    let key = build_key("statefulsets", Some("default"), "status-test");
    storage.create(&key, &statefulset).await.unwrap();

    // Run controller
    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify status was updated
    let updated_ss: StatefulSet = storage.get(&key).await.unwrap();
    let status = updated_ss.status.expect("Status should be set");

    assert_eq!(status.replicas, 3, "Status replicas should match actual");
    assert_eq!(
        status.current_replicas,
        Some(3),
        "Current replicas should be 3"
    );
}

/// Helper to mark a pod as Ready in storage
async fn mark_pod_ready(storage: &Arc<MemoryStorage>, namespace: &str, pod_name: &str) {
    let pod_key = build_key("pods", Some(namespace), pod_name);
    let mut pod: Pod = storage.get(&pod_key).await.unwrap();
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        conditions: Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(chrono::Utc::now()),
            observed_generation: None,
        }]),
        ..pod.status.unwrap_or_default()
    });
    storage.update(&pod_key, &pod).await.unwrap();
}

#[tokio::test]
async fn test_statefulset_ordered_ready_requires_previous_ready() {
    let storage = setup_test().await;

    // Create statefulset with OrderedReady policy and 3 replicas
    let mut statefulset = create_test_statefulset("ordered", "default", 3);
    statefulset.spec.pod_management_policy = Some("OrderedReady".to_string());
    let key = build_key("statefulsets", Some("default"), "ordered");
    storage.create(&key, &statefulset).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // First reconcile: should create only pod-0 (no previous pod to check)
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1, "OrderedReady should create only pod-0 first");
    assert_eq!(pods[0].metadata.name, "ordered-0");

    // Second reconcile: pod-0 not Ready yet, should NOT create pod-1
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods.len(),
        1,
        "Should still have only 1 pod (pod-0 not Ready)"
    );

    // Mark pod-0 as Ready
    mark_pod_ready(&storage, "default", "ordered-0").await;

    // Third reconcile: pod-0 is Ready, should create pod-1
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Should create pod-1 now that pod-0 is Ready");

    // Mark pod-1 as Ready
    mark_pod_ready(&storage, "default", "ordered-1").await;

    // Fourth reconcile: pod-1 is Ready, should create pod-2
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should create pod-2 now that pod-1 is Ready");
}

#[tokio::test]
async fn test_statefulset_parallel_creates_all_at_once() {
    let storage = setup_test().await;

    // Create statefulset with Parallel policy
    let mut statefulset = create_test_statefulset("parallel", "default", 3);
    statefulset.spec.pod_management_policy = Some("Parallel".to_string());
    let key = build_key("statefulsets", Some("default"), "parallel");
    storage.create(&key, &statefulset).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Parallel should create all 3 pods at once without waiting for readiness
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods.len(),
        3,
        "Parallel should create all 3 pods immediately"
    );

    let mut names: Vec<String> = pods.iter().map(|p| p.metadata.name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["parallel-0", "parallel-1", "parallel-2"]);
}

#[tokio::test]
async fn test_statefulset_rolling_update_changes_image() {
    let storage = setup_test().await;

    // Create statefulset with 2 replicas
    let mut statefulset = create_test_statefulset("rolling", "default", 2);
    statefulset.spec.pod_management_policy = Some("Parallel".to_string());
    let key = build_key("statefulsets", Some("default"), "rolling");
    storage.create(&key, &statefulset).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify 2 pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);

    // Get the initial revision
    let old_revision = pods[0]
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("controller-revision-hash"))
        .cloned()
        .unwrap();

    // Mark all pods as Running/Ready so rolling update can proceed
    for pod in &pods {
        mark_pod_ready(&storage, "default", &pod.metadata.name).await;
    }

    // Update the image in the SS template — this should change the revision hash
    statefulset.spec.template.spec.containers[0].image = "nginx:1.26-alpine".to_string();
    // Re-read from storage to get fresh resourceVersion
    let mut fresh_ss: StatefulSet = storage.get(&key).await.unwrap();
    fresh_ss.spec.template.spec.containers[0].image = "nginx:1.26-alpine".to_string();
    storage.update(&key, &fresh_ss).await.unwrap();

    // Reconcile — should detect revision mismatch and mark one pod for
    // deletion. Simulate kubelet to actually remove the marked pod from
    // storage so the assertion below observes the new pod count.
    controller.reconcile_all().await.unwrap();
    simulate_kubelet_cleanup(&storage, "default").await;

    // Should have deleted one pod (rolling update deletes one at a time)
    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods_after.len(),
        1,
        "Rolling update should delete one stale pod"
    );

    // Reconcile again — should recreate the deleted pod with new revision
    controller.reconcile_all().await.unwrap();
    simulate_kubelet_cleanup(&storage, "default").await;

    let pods_recreated: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods_recreated.len(),
        2,
        "Should recreate deleted pod with new template"
    );

    // The recreated pod should have the new revision
    let new_revision_pod = pods_recreated.iter().find(|p| {
        p.metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .map(|r| r != &old_revision)
            .unwrap_or(false)
    });
    assert!(
        new_revision_pod.is_some(),
        "Recreated pod should have new revision hash"
    );
}

/// Force a pod into the `Failed` phase, simulating a Node eviction/rejection.
async fn mark_pod_failed(storage: &Arc<MemoryStorage>, namespace: &str, pod_name: &str) {
    let pod_key = build_key("pods", Some(namespace), pod_name);
    let mut pod: Pod = storage.get(&pod_key).await.unwrap();
    pod.status = Some(PodStatus {
        phase: Some(Phase::Failed),
        reason: Some("Evicted".to_string()),
        ..pod.status.unwrap_or_default()
    });
    storage.update(&pod_key, &pod).await.unwrap();
}

/// #346: "StatefulSet MUST delete and recreate Pods it owns that go into a
/// Failed state, such as when they are rejected or evicted by a Node."
///
/// Mirrors upstream `stateful_set_control.go`: a Failed replica is deleted
/// (`isFailed` -> DeleteStatefulPod) and recreated at the same ordinal on a
/// later sync, once the old pod is actually gone from storage.
#[tokio::test]
async fn test_statefulset_recreates_evicted_pod() {
    let storage = setup_test().await;
    let ns = "default";
    let ss = create_test_statefulset("web", ns, 1);
    let ss_key = build_key("statefulsets", Some(ns), "web");
    storage.create(&ss_key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // Initial sync brings up web-0.
    controller.reconcile_all().await.unwrap();
    let pod_key = build_key("pods", Some(ns), "web-0");
    let original: Pod = storage.get(&pod_key).await.unwrap();
    mark_pod_ready(&storage, ns, "web-0").await;

    // The Node evicts web-0 -> it goes Failed.
    mark_pod_failed(&storage, ns, "web-0").await;

    // Reconcile: the controller must mark the Failed pod for deletion.
    controller.reconcile_all().await.unwrap();
    let after_evict: Pod = storage.get(&pod_key).await.unwrap();
    assert!(
        after_evict.metadata.deletion_timestamp.is_some(),
        "Failed pod must be marked for deletion so it gets recreated"
    );

    // Kubelet finalizes the delete (removes it from storage).
    simulate_kubelet_cleanup(&storage, ns).await;
    assert!(
        storage.get::<Pod>(&pod_key).await.is_err(),
        "evicted pod should be gone from storage before recreate"
    );

    // Next reconcile recreates web-0 at the same ordinal, fresh (not Failed).
    controller.reconcile_all().await.unwrap();
    let recreated: Pod = storage
        .get(&pod_key)
        .await
        .expect("StatefulSet must recreate the evicted pod");
    assert!(
        recreated.metadata.deletion_timestamp.is_none(),
        "recreated pod must not carry a deletionTimestamp"
    );
    assert_ne!(
        recreated.metadata.uid, original.metadata.uid,
        "recreated pod must be a new object, not the evicted one"
    );
    assert!(
        !matches!(
            recreated.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(Phase::Failed)
        ),
        "recreated pod must not be in the Failed phase"
    );
}
