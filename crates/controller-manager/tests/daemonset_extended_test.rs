//! Extended DaemonSet controller tests borrowed from upstream Kubernetes Go implementation.
//!
//! These tests cover advanced DaemonSet features not covered in basic conformance:
//! - Taint and toleration filtering
//! - Node affinity constraints
//! - Update strategies (RollingUpdate, OnDelete) with maxUnavailable
//! - Revision history management
//! - Pod scheduling on node join/leave events
//! - DaemonSet status correctness
//!
//! Source: kubernetes/test/e2e/apps/daemon_set.go
//!         kubernetes/pkg/controller/daemon/daemon_controller_test.go

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::{DaemonSetUpdateStrategy, RollingUpdateDaemonSet};
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::daemonset::DaemonSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn empty_pod_spec(image: &str, container_name: &str) -> PodSpec {
    PodSpec {
        containers: vec![Container {
            name: container_name.to_string(),
            image: image.to_string(),
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
    }
}

fn make_node(name: &str, labels: Option<HashMap<String, String>>) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.labels = labels;
            m.ensure_uid();
            m
        },
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        }),
        status: None,
    }
}

fn make_node_with_taint(name: &str, taints: Vec<Taint>) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.ensure_uid();
            m
        },
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: Some(taints),
        }),
        status: None,
    }
}

fn make_daemonset(
    name: &str,
    namespace: &str,
    image: &str,
    node_selector: Option<HashMap<String, String>>,
) -> DaemonSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());
    labels.insert("daemonset-name".to_string(), name.to_string());

    let mut spec = empty_pod_spec(image, "app");
    spec.node_selector = node_selector;

    DaemonSet {
        type_meta: TypeMeta {
            kind: "DaemonSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name).with_namespace(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: DaemonSetSpec {
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
                spec,
            },
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
        },
        status: None,
    }
}

/// Replay kubelet: physically delete pods whose `deletionTimestamp` was
/// stamped by the controller.
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

async fn mark_all_pods_ready(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for pod in pods {
        mark_pod_ready(storage, namespace, &pod.metadata.name).await;
    }
}

// ===========================================================================
// Extended DaemonSet Tests
// ===========================================================================

/// DaemonSet should not schedule pods on nodes with taints it doesn't tolerate
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
/// Tests that DaemonSets respect node taints without corresponding tolerations.
#[tokio::test]
async fn daemonset_should_respect_node_taints_without_tolerations() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 3 nodes: 2 clean, 1 with NoSchedule taint
    let node1 = make_node("node-1", None);
    let node2 = make_node("node-2", None);
    let node3_tainted = make_node_with_taint(
        "node-3",
        vec![Taint {
            key: "dedicated".to_string(),
            value: Some("special".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }],
    );

    storage
        .create(&build_key("nodes", None, "node-1"), &node1)
        .await
        .unwrap();
    storage
        .create(&build_key("nodes", None, "node-2"), &node2)
        .await
        .unwrap();
    storage
        .create(&build_key("nodes", None, "node-3"), &node3_tainted)
        .await
        .unwrap();

    // Create DaemonSet without tolerations
    let ds = make_daemonset("no-toleration", ns, "nginx:1.25-alpine", None);
    let key = build_key("daemonsets", Some(ns), "no-toleration");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();

    // Should only schedule on node-1 and node-2, not on tainted node-3
    assert_eq!(
        pods.len(),
        2,
        "DaemonSet should skip tainted nodes without tolerations"
    );

    let pod_nodes: Vec<&str> = pods
        .iter()
        .filter_map(|p| p.spec.as_ref().and_then(|s| s.node_name.as_deref()))
        .collect();

    assert!(pod_nodes.contains(&"node-1"));
    assert!(pod_nodes.contains(&"node-2"));
    assert!(!pod_nodes.contains(&"node-3"));
}

/// DaemonSet should schedule pods on tainted nodes when it has matching tolerations
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
/// Tests that DaemonSets with proper tolerations can schedule on tainted nodes.
#[tokio::test]
async fn daemonset_with_tolerations_should_schedule_on_tainted_nodes() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 2 nodes: 1 clean, 1 with NoSchedule taint
    let node1 = make_node("node-1", None);
    let node2_tainted = make_node_with_taint(
        "node-2",
        vec![Taint {
            key: "dedicated".to_string(),
            value: Some("special".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }],
    );

    storage
        .create(&build_key("nodes", None, "node-1"), &node1)
        .await
        .unwrap();
    storage
        .create(&build_key("nodes", None, "node-2"), &node2_tainted)
        .await
        .unwrap();

    // Create DaemonSet WITH matching toleration
    let mut ds = make_daemonset("with-toleration", ns, "nginx:1.25-alpine", None);
    ds.spec.template.spec.tolerations = Some(vec![Toleration {
        key: Some("dedicated".to_string()),
        operator: Some("Equal".to_string()),
        value: Some("special".to_string()),
        effect: Some("NoSchedule".to_string()),
        toleration_seconds: None,
    }]);

    let key = build_key("daemonsets", Some(ns), "with-toleration");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();

    // Should schedule on BOTH nodes (toleration allows scheduling on tainted node)
    assert_eq!(
        pods.len(),
        2,
        "DaemonSet with tolerations should schedule on all nodes"
    );

    let pod_nodes: Vec<&str> = pods
        .iter()
        .filter_map(|p| p.spec.as_ref().and_then(|s| s.node_name.as_deref()))
        .collect();

    assert!(pod_nodes.contains(&"node-1"));
    assert!(pod_nodes.contains(&"node-2"));
}

/// DaemonSet with node affinity should only schedule on matching nodes
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
/// Tests that nodeAffinity constraints filter eligible nodes.
#[tokio::test]
async fn daemonset_with_node_affinity_should_filter_nodes() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 3 nodes with different labels
    let node1 = make_node(
        "node-1",
        Some(HashMap::from([(
            "zone".to_string(),
            "us-east-1a".to_string(),
        )])),
    );
    let node2 = make_node(
        "node-2",
        Some(HashMap::from([(
            "zone".to_string(),
            "us-east-1b".to_string(),
        )])),
    );
    let node3 = make_node(
        "node-3",
        Some(HashMap::from([(
            "zone".to_string(),
            "us-west-2a".to_string(),
        )])),
    );

    storage
        .create(&build_key("nodes", None, "node-1"), &node1)
        .await
        .unwrap();
    storage
        .create(&build_key("nodes", None, "node-2"), &node2)
        .await
        .unwrap();
    storage
        .create(&build_key("nodes", None, "node-3"), &node3)
        .await
        .unwrap();

    // Create DaemonSet with node affinity for us-east-1a zone only
    let mut ds = make_daemonset("affinity-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.template.spec.affinity = Some(Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(vec![NodeSelectorRequirement {
                        key: "zone".to_string(),
                        operator: "In".to_string(),
                        values: Some(vec!["us-east-1a".to_string()]),
                    }]),
                    match_fields: None,
                }],
            }),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
        pod_affinity: None,
        pod_anti_affinity: None,
    });

    let key = build_key("daemonsets", Some(ns), "affinity-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();

    // Should only schedule on node-1 (us-east-1a)
    assert_eq!(
        pods.len(),
        1,
        "DaemonSet with node affinity should only match labeled nodes"
    );
    assert_eq!(
        pods[0].spec.as_ref().unwrap().node_name,
        Some("node-1".to_string())
    );
}

/// DaemonSet RollingUpdate with maxUnavailable should limit disruption
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
/// Tests that maxUnavailable controls how many pods can be down during update.
#[tokio::test]
async fn daemonset_rolling_update_respects_max_unavailable() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 5 nodes
    for i in 1..=5 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    // Create DaemonSet with RollingUpdate and maxUnavailable=2
    let mut ds = make_daemonset("rolling-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateDaemonSet {
            max_unavailable: Some("2".to_string()),
            max_surge: None,
        }),
    });

    let key = build_key("daemonsets", Some(ns), "rolling-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    let initial_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(initial_pods.len(), 5);

    let _original_image = initial_pods[0].spec.as_ref().unwrap().containers[0]
        .image
        .clone();

    // Change template image
    let mut updated: DaemonSet = storage.get(&key).await.unwrap();
    updated.spec.template.spec.containers[0].image = "nginx:1.26-alpine".to_string();
    storage.update(&key, &updated).await.unwrap();

    // Start rolling update - first reconcile should mark some pods for deletion
    controller.reconcile_all().await.unwrap();

    // Count pods marked for deletion (should be at most maxUnavailable=2)
    let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let deleting_count = all_pods
        .iter()
        .filter(|p| p.metadata.deletion_timestamp.is_some())
        .count();

    assert!(
        deleting_count <= 2,
        "maxUnavailable=2 should limit concurrent deletions to 2, found {}",
        deleting_count
    );

    // Complete the rolling update
    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(final_pods.len(), 5);

    // All pods should have new image
    for pod in &final_pods {
        assert_eq!(
            pod.spec.as_ref().unwrap().containers[0].image,
            "nginx:1.26-alpine",
            "All pods should be updated"
        );
    }
}

/// DaemonSet OnDelete strategy should not automatically update pods
///
/// Upstream: k8s.io/kubernetes/pkg/controller/daemon/daemon_controller_test.go
/// Tests that OnDelete requires manual pod deletion for updates.
#[tokio::test]
async fn daemonset_ondelete_strategy_requires_manual_deletion() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 3 nodes
    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    // Create DaemonSet with OnDelete strategy
    let mut ds = make_daemonset("ondelete-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("OnDelete".to_string()),
        rolling_update: None,
    });

    let key = build_key("daemonsets", Some(ns), "ondelete-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    let initial_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(initial_pods.len(), 3);

    let original_image = initial_pods[0].spec.as_ref().unwrap().containers[0]
        .image
        .clone();

    // Change template image
    let mut updated: DaemonSet = storage.get(&key).await.unwrap();
    updated.spec.template.spec.containers[0].image = "nginx:1.27-alpine".to_string();
    storage.update(&key, &updated).await.unwrap();

    // Reconcile - should NOT update any pods with OnDelete
    controller.reconcile_all().await.unwrap();

    let after_reconcile: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in &after_reconcile {
        assert_eq!(
            pod.spec.as_ref().unwrap().containers[0].image,
            original_image,
            "OnDelete strategy should not auto-update pods"
        );
    }

    // Manually delete one pod
    let pod_key = build_key("pods", Some(ns), "ondelete-ds-node-1");
    storage.delete(&pod_key).await.unwrap();

    // Now reconcile should recreate with new image
    controller.reconcile_all().await.unwrap();

    let recreated_pod: Pod = storage.get(&pod_key).await.unwrap();
    assert_eq!(
        recreated_pod.spec.as_ref().unwrap().containers[0].image,
        "nginx:1.27-alpine",
        "Manually deleted pod should be recreated with new image"
    );
}

/// DaemonSet should add pod when new node joins cluster
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
/// Tests that DaemonSet reacts to node addition events.
#[tokio::test]
async fn daemonset_should_add_pod_when_node_joins() {
    let storage = setup_test().await;
    let ns = "default";

    // Start with 2 nodes
    let node1 = make_node("node-1", None);
    let node2 = make_node("node-2", None);
    storage
        .create(&build_key("nodes", None, "node-1"), &node1)
        .await
        .unwrap();
    storage
        .create(&build_key("nodes", None, "node-2"), &node2)
        .await
        .unwrap();

    let ds = make_daemonset("node-join-ds", ns, "nginx:1.25-alpine", None);
    let key = build_key("daemonsets", Some(ns), "node-join-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let initial_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(initial_pods.len(), 2);

    // Add a new node
    let node3 = make_node("node-3", None);
    storage
        .create(&build_key("nodes", None, "node-3"), &node3)
        .await
        .unwrap();

    // Reconcile should create pod on new node
    controller.reconcile_all().await.unwrap();

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        final_pods.len(),
        3,
        "DaemonSet should create pod on new node"
    );

    let pod_nodes: Vec<&str> = final_pods
        .iter()
        .filter_map(|p| p.spec.as_ref().and_then(|s| s.node_name.as_deref()))
        .collect();

    assert!(pod_nodes.contains(&"node-3"));
}

/// DaemonSet should remove pod when node leaves cluster
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
/// Tests that DaemonSet cleans up pods when nodes are removed.
#[tokio::test]
async fn daemonset_should_remove_pod_when_node_leaves() {
    let storage = setup_test().await;
    let ns = "default";

    // Start with 3 nodes
    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("node-leave-ds", ns, "nginx:1.25-alpine", None);
    let key = build_key("daemonsets", Some(ns), "node-leave-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let initial_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(initial_pods.len(), 3);

    // Remove node-2
    storage
        .delete(&build_key("nodes", None, "node-2"))
        .await
        .unwrap();

    // Reconcile should remove pod from departed node
    controller.reconcile_all().await.unwrap();

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        final_pods.len(),
        2,
        "DaemonSet should remove pod when node leaves"
    );

    let pod_nodes: Vec<&str> = final_pods
        .iter()
        .filter_map(|p| p.spec.as_ref().and_then(|s| s.node_name.as_deref()))
        .collect();

    assert!(pod_nodes.contains(&"node-1"));
    assert!(pod_nodes.contains(&"node-3"));
    assert!(!pod_nodes.contains(&"node-2"));
}

/// DaemonSet should maintain accurate status during updates
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
/// Tests status.numberReady, status.desiredNumberScheduled accuracy.
#[tokio::test]
async fn daemonset_status_should_be_accurate_during_update() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 4 nodes
    for i in 1..=4 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("status-ds", ns, "nginx:1.25-alpine", None);
    let key = build_key("daemonsets", Some(ns), "status-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let ds_after: DaemonSet = storage.get(&key).await.unwrap();
    let status = ds_after.status.as_ref().unwrap();
    assert_eq!(status.desired_number_scheduled, 4);

    // Mark all pods ready
    mark_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    let ds_ready: DaemonSet = storage.get(&key).await.unwrap();
    let ready_status = ds_ready.status.as_ref().unwrap();
    assert_eq!(ready_status.number_ready, 4);
    assert_eq!(ready_status.updated_number_scheduled, Some(4));
    assert_eq!(ready_status.number_available, Some(4));
}

/// DaemonSet should respect revision history limit
///
/// Upstream: k8s.io/kubernetes/pkg/controller/daemon/daemon_controller_test.go
/// Tests that old ControllerRevisions are garbage collected.
#[tokio::test]
async fn daemonset_should_respect_revision_history_limit() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 2 nodes
    for i in 1..=2 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let mut ds = make_daemonset("revision-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.revision_history_limit = Some(2);
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: None,
    });

    let key = build_key("daemonsets", Some(ns), "revision-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    // Perform multiple updates
    for i in 0..5 {
        let mut updated: DaemonSet = storage.get(&key).await.unwrap();
        updated.spec.template.spec.containers[0].image = format!("nginx:1.{}-alpine", 25 + i);
        storage.update(&key, &updated).await.unwrap();

        for _ in 0..4 {
            controller.reconcile_all().await.unwrap();
            simulate_kubelet_cleanup(&storage, ns).await;
            mark_all_pods_ready(&storage, ns).await;
        }
    }

    // Count ControllerRevisions
    let revisions: Vec<rusternetes_common::resources::ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap_or_default();

    // revisionHistoryLimit=2 caps the NON-current history (upstream
    // cleanupHistory), so at most 2 old revisions plus the current one survive.
    assert!(
        revisions.len() <= 3,
        "Should have at most 3 revisions (2 non-current + current), found {}",
        revisions.len()
    );
}

/// DaemonSet with minReadySeconds should wait before marking pod as available
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
/// Tests that minReadySeconds delays availability reporting.
#[tokio::test]
async fn daemonset_should_respect_min_ready_seconds() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 2 nodes
    for i in 1..=2 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let mut ds = make_daemonset("min-ready-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.min_ready_seconds = Some(300); // 5 minutes

    let key = build_key("daemonsets", Some(ns), "min-ready-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Mark pods as Running but NOT Ready (simulating minReadySeconds wait)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in &pods {
        let pod_key = build_key("pods", Some(ns), &pod.metadata.name);
        let mut p: Pod = storage.get(&pod_key).await.unwrap();
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(), // Not yet ready due to minReadySeconds
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: Some(chrono::Utc::now()),
                observed_generation: None,
            }]),
            ..p.status.unwrap_or_default()
        });
        storage.update(&pod_key, &p).await.unwrap();
    }

    controller.reconcile_all().await.unwrap();

    let ds_status: DaemonSet = storage.get(&key).await.unwrap();
    let status = ds_status.status.as_ref().unwrap();

    // Pods should be created but not counted as available yet
    assert_eq!(
        status.number_ready, 0,
        "Pods not ready due to minReadySeconds"
    );
}

// ===========================================================================
// Additional extended DaemonSet coverage (batch 1.3 expansion)
// ===========================================================================
//
// The tests below extend the suite with five upstream-inspired scenarios that
// are not yet covered above. Each test documents its upstream reference and,
// where Rusternetes' controller does not yet honour the contract, is gated
// with `#[ignore = "RED-state: ..."]` so they document missing behaviour
// without making the suite red.

/// DaemonSet `maxUnavailable=1` should serialise rolling updates one pod at a
/// time even when many nodes are eligible.
///
/// Upstream: kubernetes/test/e2e/apps/daemon_set.go
///   "rolling update should respect maxUnavailable"
///
/// The earlier `daemonset_rolling_update_respects_max_unavailable` test
/// exercises the absolute value `"2"`. This case pins the strictest budget
/// (`"1"`) across six nodes to ensure the controller never deletes more than
/// a single pod per reconcile, even when most pods are ready and updating.
#[tokio::test]
async fn daemonset_rolling_update_max_unavailable_one_serialises_deletions() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 6 nodes so the budget (1) is meaningfully smaller than the fleet.
    for i in 1..=6 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let mut ds = make_daemonset("serial-rolling-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateDaemonSet {
            max_unavailable: Some("1".to_string()),
            max_surge: None,
        }),
    });

    let key = build_key("daemonsets", Some(ns), "serial-rolling-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    let initial_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        initial_pods.len(),
        6,
        "expected one pod per node before update"
    );

    // Bump the template image — triggers rolling update.
    let mut updated: DaemonSet = storage.get(&key).await.unwrap();
    updated.spec.template.spec.containers[0].image = "nginx:1.26-alpine".to_string();
    storage.update(&key, &updated).await.unwrap();

    // First reconcile after the template change should delete AT MOST one pod
    // (the maxUnavailable budget). The controller marks deletion via
    // deletionTimestamp; the in-memory store also issues a hard delete in
    // certain code paths, so count both.
    controller.reconcile_all().await.unwrap();

    let after_first_reconcile: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let marked_for_deletion = after_first_reconcile
        .iter()
        .filter(|p| p.metadata.deletion_timestamp.is_some())
        .count();
    let hard_deleted = 6 - after_first_reconcile.len();

    let disruption_window = marked_for_deletion + hard_deleted;
    assert!(
        disruption_window <= 1,
        "maxUnavailable=1 must cap concurrent disruption at one pod, observed {} \
         (deletionTimestamp: {}, hard-deleted: {})",
        disruption_window,
        marked_for_deletion,
        hard_deleted
    );

    // Drive the rolling update to completion: simulate kubelet cleanup +
    // readiness for several rounds. Each round can only progress one pod.
    for _ in 0..12 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(final_pods.len(), 6, "fleet restored after rolling update");
    for pod in &final_pods {
        assert_eq!(
            pod.spec.as_ref().unwrap().containers[0].image,
            "nginx:1.26-alpine",
            "all pods should have been rolled forward"
        );
    }
}

/// DaemonSet with a `priorityClassName` should propagate that field (and any
/// pre-computed `priority` integer) to every pod it creates.
///
/// Upstream: kubernetes/pkg/controller/daemon/daemon_controller.go (the
/// template's PodSpec is cloned wholesale into each pod).
///
/// A scheduler/admission plugin is responsible for resolving the named
/// PriorityClass into the integer `priority` value, but the *controller*'s
/// contract is that whatever the user puts on the template arrives intact on
/// the pod. This test pins that property.
#[tokio::test]
async fn daemonset_propagates_priority_class_to_pods() {
    let storage = setup_test().await;
    let ns = "default";

    // Three eligible nodes — assert propagation on every one.
    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    // Persist the PriorityClass at the cluster-scoped registry path. The
    // controller is not required to consult it for this test, but storing it
    // keeps the test honest in a future where the controller might.
    let pc = rusternetes_common::resources::policy::PriorityClass::new("high-priority", 1_000_000)
        .with_description("DaemonSet critical workloads");
    storage
        .create(&build_key("priorityclasses", None, "high-priority"), &pc)
        .await
        .unwrap();

    let mut ds = make_daemonset("priority-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.template.spec.priority_class_name = Some("high-priority".to_string());
    ds.spec.template.spec.priority = Some(1_000_000);

    let key = build_key("daemonsets", Some(ns), "priority-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "one pod per eligible node");

    for pod in &pods {
        let spec = pod.spec.as_ref().expect("pod spec must be populated");
        assert_eq!(
            spec.priority_class_name.as_deref(),
            Some("high-priority"),
            "priorityClassName must be propagated from the DaemonSet template"
        );
        assert_eq!(
            spec.priority,
            Some(1_000_000),
            "priority integer must be propagated from the DaemonSet template"
        );
    }
}

/// DaemonSet status must fill the full set of counter fields correctly at
/// steady state: desired, current, ready, numberMisscheduled,
/// updatedNumberScheduled, numberAvailable, numberUnavailable.
///
/// Upstream: kubernetes/pkg/controller/daemon/update.go
///   `manage()` + `updateDaemonSetStatus()`
///
/// The earlier `daemonset_status_should_be_accurate_during_update` test
/// only asserts three of these. This case pins every counter, including the
/// ones whose value is `0` at steady state, so a regression that flips e.g.
/// `numberMisscheduled` to non-zero would be caught.
#[tokio::test]
async fn daemonset_status_counters_steady_state_all_fields() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 3 eligible nodes.
    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("full-status-ds", ns, "nginx:1.25-alpine", None);
    let key = build_key("daemonsets", Some(ns), "full-status-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());

    // Round 1: pods are created but not yet ready.
    controller.reconcile_all().await.unwrap();
    let after_create: DaemonSet = storage.get(&key).await.unwrap();
    let status = after_create.status.as_ref().expect("status must be set");
    assert_eq!(
        status.desired_number_scheduled, 3,
        "desired = eligible nodes"
    );
    assert_eq!(
        status.current_number_scheduled, 3,
        "current = nodes with at least one pod"
    );
    assert_eq!(
        status.number_ready, 0,
        "no pods are Ready before mark_all_pods_ready"
    );
    assert_eq!(status.number_misscheduled, 0, "no pods on ineligible nodes");
    assert_eq!(
        status.number_unavailable,
        Some(3),
        "desired - ready when nothing is ready yet"
    );
    assert_eq!(
        status.number_available,
        Some(0),
        "no pods are available before they're Ready"
    );
    assert_eq!(
        status.updated_number_scheduled,
        Some(3),
        "all pods share the current template hash on first reconcile"
    );

    // Round 2: kubelet marks everything Ready and the controller resyncs.
    mark_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    let after_ready: DaemonSet = storage.get(&key).await.unwrap();
    let status = after_ready
        .status
        .as_ref()
        .expect("status must be set after ready reconcile");

    assert_eq!(status.desired_number_scheduled, 3);
    assert_eq!(status.current_number_scheduled, 3);
    assert_eq!(status.number_ready, 3, "all three pods now Ready");
    assert_eq!(status.number_misscheduled, 0);
    assert_eq!(
        status.number_unavailable,
        Some(0),
        "no pods unavailable when all are Ready"
    );
    assert_eq!(
        status.number_available,
        Some(3),
        "all Ready pods count as available"
    );
    assert_eq!(
        status.updated_number_scheduled,
        Some(3),
        "all pods carry the current template hash"
    );
}

/// DaemonSet with pod *anti*-affinity should refuse to schedule a second pod
/// onto a node already running a matching pod.
///
/// Upstream: kubernetes/test/e2e/apps/daemon_set.go
///   "should not schedule extra pods when constrained"
///
/// Pod anti-affinity is normally enforced by the scheduler, not the
/// DaemonSet controller. Our controller is happy to place one DS pod per
/// node regardless. The interesting failure mode is when a *user* pod with
/// matching labels already exists on a node and pod-anti-affinity should
/// block the DS from co-locating. Rusternetes' controller does not yet
/// consult `podAntiAffinity`, so this is RED-state.
#[tokio::test]
async fn daemonset_with_pod_anti_affinity_skips_conflicting_node() {
    use rusternetes_common::resources::pod::{PodAffinityTerm, PodAntiAffinity};

    let storage = setup_test().await;
    let ns = "default";

    // Two nodes, both labelled with the same topology key.
    for i in 1..=2 {
        let labels = HashMap::from([("kubernetes.io/hostname".to_string(), format!("node-{}", i))]);
        let node = make_node(&format!("node-{}", i), Some(labels));
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    // Place a conflicting user pod on node-1 carrying the DaemonSet's label.
    let mut conflict_labels = HashMap::new();
    conflict_labels.insert("conflict".to_string(), "true".to_string());
    let mut conflict_meta = ObjectMeta::new("conflict-pod").with_namespace(ns.to_string());
    conflict_meta.labels = Some(conflict_labels.clone());
    conflict_meta.ensure_uid();
    let mut conflict_spec = empty_pod_spec("nginx:1.25-alpine", "app");
    conflict_spec.node_name = Some("node-1".to_string());
    let conflict_pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: conflict_meta,
        spec: Some(conflict_spec),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        }),
    };
    storage
        .create(&build_key("pods", Some(ns), "conflict-pod"), &conflict_pod)
        .await
        .unwrap();

    // DaemonSet has pod anti-affinity on `conflict=true` across hostname.
    let mut ds = make_daemonset("antiaffinity-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.template.spec.affinity = Some(Affinity {
        node_affinity: None,
        pod_affinity: None,
        pod_anti_affinity: Some(PodAntiAffinity {
            required_during_scheduling_ignored_during_execution: Some(vec![PodAffinityTerm {
                label_selector: LabelSelector {
                    match_labels: Some(conflict_labels),
                    match_expressions: None,
                },
                namespaces: Some(vec![ns.to_string()]),
                topology_key: "kubernetes.io/hostname".to_string(),
                ..Default::default()
            }]),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
    });

    let key = build_key("daemonsets", Some(ns), "antiaffinity-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Only DS-owned pods (skip the conflict pod we seeded).
    let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let ds_pods: Vec<&Pod> = all_pods
        .iter()
        .filter(|p| {
            p.metadata
                .owner_references
                .as_ref()
                .map(|refs| {
                    refs.iter()
                        .any(|r| r.kind == "DaemonSet" && r.name == "antiaffinity-ds")
                })
                .unwrap_or(false)
        })
        .collect();

    // Anti-affinity should skip node-1; expect exactly one pod on node-2.
    assert_eq!(
        ds_pods.len(),
        1,
        "podAntiAffinity should keep the DS off the conflicting node"
    );
    assert_eq!(
        ds_pods[0].spec.as_ref().unwrap().node_name.as_deref(),
        Some("node-2"),
        "the surviving pod must land on the conflict-free node"
    );
}

/// DaemonSet should converge to the final template even when the user
/// "bursts" several template updates back to back without waiting for the
/// rolling update to settle.
///
/// Upstream: kubernetes/test/e2e/apps/daemon_set.go
///   "should rollback without unnecessary restarts" — covers the related
///   case where rapid spec changes must not strand pods on intermediate
///   templates.
///
/// We don't assert the *number* of intermediate revisions (that's a
/// controller implementation detail) — we assert (a) the controller never
/// crashes on a burst, and (b) after enough reconciles, every pod carries
/// the *final* image and the status reports a fully-updated fleet.
#[tokio::test]
async fn daemonset_handles_burst_template_updates() {
    let storage = setup_test().await;
    let ns = "default";

    // Four nodes.
    for i in 1..=4 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let mut ds = make_daemonset("burst-ds", ns, "nginx:1.25-alpine", None);
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateDaemonSet {
            max_unavailable: Some("2".to_string()),
            max_surge: None,
        }),
    });

    let key = build_key("daemonsets", Some(ns), "burst-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    // Burst: five back-to-back template changes with no reconcile in between.
    let final_image = "nginx:1.30-alpine";
    let bursts = [
        "nginx:1.26-alpine",
        "nginx:1.27-alpine",
        "nginx:1.28-alpine",
        "nginx:1.29-alpine",
        final_image,
    ];
    for image in &bursts {
        let mut updated: DaemonSet = storage.get(&key).await.unwrap();
        updated.spec.template.spec.containers[0].image = (*image).to_string();
        storage.update(&key, &updated).await.unwrap();
    }

    // Drive the controller until the fleet converges to the final image.
    for _ in 0..40 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(final_pods.len(), 4, "fleet returns to the desired size");
    for pod in &final_pods {
        assert_eq!(
            pod.spec.as_ref().unwrap().containers[0].image,
            final_image,
            "every pod must converge to the final template image after the burst"
        );
    }

    // Status must also reflect a fully-updated fleet.
    let ds_after: DaemonSet = storage.get(&key).await.unwrap();
    let status = ds_after.status.as_ref().expect("status must be set");
    assert_eq!(status.desired_number_scheduled, 4);
    assert_eq!(status.current_number_scheduled, 4);
    assert_eq!(
        status.number_ready, 4,
        "all pods Ready after burst converges"
    );
    assert_eq!(
        status.updated_number_scheduled,
        Some(4),
        "all pods carry the final template hash"
    );
    assert_eq!(status.number_available, Some(4));
    assert_eq!(status.number_unavailable, Some(0));
}
