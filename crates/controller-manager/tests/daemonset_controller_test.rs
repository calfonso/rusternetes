// DaemonSet Controller Integration Tests
// Tests the DaemonSet controller's ability to ensure one pod per node

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::daemonset::DaemonSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn create_test_node(name: &str, labels: Option<HashMap<String, String>>) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None,
            labels,
            annotations: None,
            uid: uuid::Uuid::new_v4().to_string(),
            creation_timestamp: None,
            deletion_timestamp: None,
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
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

fn create_test_daemonset(
    name: &str,
    namespace: &str,
    node_selector: Option<HashMap<String, String>>,
) -> DaemonSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    DaemonSet {
        type_meta: TypeMeta {
            kind: "DaemonSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
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
                spec: PodSpec {
                    containers: vec![Container {
                        name: "logger".to_string(),
                        image: "busybox:latest".to_string(),
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
                    node_selector,
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
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
        },
        status: None,
    }
}

#[tokio::test]
async fn test_daemonset_creates_pod_per_node() {
    let storage = setup_test().await;

    // Create 3 nodes
    for i in 1..=3 {
        let node = create_test_node(&format!("node-{}", i), None);
        let key = build_key("nodes", None, &node.metadata.name);
        storage.create(&key, &node).await.unwrap();
    }

    // Create DaemonSet
    let ds = create_test_daemonset("test-ds", "default", None);
    let ds_key = build_key("daemonsets", Some("default"), &ds.metadata.name);
    storage.create(&ds_key, &ds).await.unwrap();

    // Run controller
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Should create 3 pods (one per node)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should create one pod per node");

    // Each pod should be assigned to a different node
    let node_names: std::collections::HashSet<_> = pods
        .iter()
        .filter_map(|p| p.spec.as_ref()?.node_name.as_ref())
        .collect();
    assert_eq!(
        node_names.len(),
        3,
        "Each pod should be on a different node"
    );
}

#[tokio::test]
async fn test_daemonset_respects_node_selector() {
    let storage = setup_test().await;

    // Create nodes with different labels
    let mut ssd_labels = HashMap::new();
    ssd_labels.insert("disktype".to_string(), "ssd".to_string());

    let mut hdd_labels = HashMap::new();
    hdd_labels.insert("disktype".to_string(), "hdd".to_string());

    let node1 = create_test_node("node-1", Some(ssd_labels.clone()));
    let node2 = create_test_node("node-2", Some(hdd_labels));
    let node3 = create_test_node("node-3", Some(ssd_labels));

    storage
        .create(&build_key("nodes", None, &node1.metadata.name), &node1)
        .await
        .unwrap();
    storage
        .create(&build_key("nodes", None, &node2.metadata.name), &node2)
        .await
        .unwrap();
    storage
        .create(&build_key("nodes", None, &node3.metadata.name), &node3)
        .await
        .unwrap();

    // Create DaemonSet with node selector for SSD nodes only
    let mut node_selector = HashMap::new();
    node_selector.insert("disktype".to_string(), "ssd".to_string());

    let ds = create_test_daemonset("ssd-ds", "default", Some(node_selector));
    storage
        .create(
            &build_key("daemonsets", Some("default"), &ds.metadata.name),
            &ds,
        )
        .await
        .unwrap();

    // Run controller
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Should only create pods on SSD nodes (node-1 and node-3)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Should only create pods on SSD nodes");

    let node_names: std::collections::HashSet<_> = pods
        .iter()
        .filter_map(|p| p.spec.as_ref()?.node_name.as_ref())
        .map(|s| s.as_str())
        .collect();

    assert!(node_names.contains("node-1"));
    assert!(node_names.contains("node-3"));
    assert!(!node_names.contains("node-2"));
}

#[tokio::test]
async fn test_daemonset_adds_pods_when_nodes_added() {
    let storage = setup_test().await;

    // Create 2 nodes initially
    for i in 1..=2 {
        let node = create_test_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    // Create DaemonSet
    let ds = create_test_daemonset("test-ds", "default", None);
    storage
        .create(
            &build_key("daemonsets", Some("default"), &ds.metadata.name),
            &ds,
        )
        .await
        .unwrap();

    // Run controller - should create 2 pods
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);

    // Add a third node
    let node3 = create_test_node("node-3", None);
    storage
        .create(&build_key("nodes", None, &node3.metadata.name), &node3)
        .await
        .unwrap();

    // Run controller again - should create pod on new node
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should create pod on newly added node");
}

#[tokio::test]
async fn test_daemonset_removes_pods_when_nodes_removed() {
    let storage = setup_test().await;

    // Create 3 nodes
    for i in 1..=3 {
        let node = create_test_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    // Create DaemonSet
    let ds = create_test_daemonset("test-ds", "default", None);
    storage
        .create(
            &build_key("daemonsets", Some("default"), &ds.metadata.name),
            &ds,
        )
        .await
        .unwrap();

    // Run controller - should create 3 pods
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3);

    // Remove node-2
    storage
        .delete(&build_key("nodes", None, "node-2"))
        .await
        .unwrap();

    // Run controller again - should remove pod from deleted node
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Should remove pod from deleted node");

    let node_names: std::collections::HashSet<_> = pods
        .iter()
        .filter_map(|p| p.spec.as_ref()?.node_name.as_ref())
        .map(|s| s.as_str())
        .collect();

    assert!(node_names.contains("node-1"));
    assert!(!node_names.contains("node-2"));
    assert!(node_names.contains("node-3"));
}

#[tokio::test]
async fn test_daemonset_updates_status() {
    let storage = setup_test().await;

    // Create 3 nodes
    for i in 1..=3 {
        let node = create_test_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    // Create DaemonSet
    let ds = create_test_daemonset("test-ds", "default", None);
    let ds_key = build_key("daemonsets", Some("default"), &ds.metadata.name);
    storage.create(&ds_key, &ds).await.unwrap();

    // Run controller
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Check status
    let updated_ds: DaemonSet = storage.get(&ds_key).await.unwrap();
    let status = updated_ds.status.unwrap();

    assert_eq!(status.desired_number_scheduled, 3);
    assert_eq!(status.current_number_scheduled, 3);
}

#[tokio::test]
async fn test_daemonset_multiple_namespaces() {
    let storage = setup_test().await;

    // Create nodes
    for i in 1..=2 {
        let node = create_test_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    // Create DaemonSets in different namespaces
    let ds1 = create_test_daemonset("test-ds", "namespace-1", None);
    let ds2 = create_test_daemonset("test-ds", "namespace-2", None);

    storage
        .create(
            &build_key("daemonsets", Some("namespace-1"), &ds1.metadata.name),
            &ds1,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("daemonsets", Some("namespace-2"), &ds2.metadata.name),
            &ds2,
        )
        .await
        .unwrap();

    // Run controller
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Each namespace should have 2 pods
    let ns1_pods: Vec<Pod> = storage.list("/registry/pods/namespace-1/").await.unwrap();
    let ns2_pods: Vec<Pod> = storage.list("/registry/pods/namespace-2/").await.unwrap();

    assert_eq!(ns1_pods.len(), 2, "Namespace 1 should have 2 pods");
    assert_eq!(ns2_pods.len(), 2, "Namespace 2 should have 2 pods");
}

#[tokio::test]
async fn test_daemonset_no_nodes_no_pods() {
    let storage = setup_test().await;

    // Create DaemonSet but NO nodes
    let ds = create_test_daemonset("test-ds", "default", None);
    storage
        .create(
            &build_key("daemonsets", Some("default"), &ds.metadata.name),
            &ds,
        )
        .await
        .unwrap();

    // Run controller
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Should create 0 pods (no nodes available)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 0, "Should not create pods when no nodes exist");
}

#[tokio::test]
async fn test_daemonset_pod_naming_convention() {
    let storage = setup_test().await;

    // Create a node with dots in the name
    let node = create_test_node("node.example.com", None);
    storage
        .create(&build_key("nodes", None, &node.metadata.name), &node)
        .await
        .unwrap();

    // Create DaemonSet
    let ds = create_test_daemonset("test-ds", "default", None);
    storage
        .create(
            &build_key("daemonsets", Some("default"), &ds.metadata.name),
            &ds,
        )
        .await
        .unwrap();

    // Run controller
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Check pod name - dots should be replaced with dashes
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1);

    let pod = &pods[0];
    // K8s convention: DaemonSet pods are named "<ds-name>-<random>" via
    // generateName, NOT "<ds-name>-<node-name>-...". A fresh suffix every
    // time a pod is recreated is required by the "should retry creating
    // failed daemon pods" conformance test, which asserts the original
    // failed pod's name returns NotFound via GET.
    assert!(
        pod.metadata.name.starts_with("test-ds-"),
        "Pod name should start with 'test-ds-', got: {}",
        pod.metadata.name
    );
    assert!(
        !pod.metadata.name.contains('.'),
        "Pod name should not contain dots"
    );
}
