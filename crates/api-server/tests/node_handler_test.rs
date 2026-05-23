//! Integration tests for Node handler
//!
//! Tests all CRUD operations, edge cases, and error handling for nodes

use rusternetes_common::resources::{Node, NodeAddress, NodeCondition, NodeSpec, NodeStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test node
fn create_test_node(name: &str) -> Node {
    let mut labels = HashMap::new();
    labels.insert("kubernetes.io/hostname".to_string(), name.to_string());

    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None, // Nodes are cluster-scoped
            labels: Some(labels),
            uid: String::new(),
            creation_timestamp: None,
            resource_version: None,
            finalizers: None,
            deletion_timestamp: None,
            owner_references: None,
            annotations: None,
            deletion_grace_period_seconds: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NodeSpec {
            pod_cidr: Some("10.244.0.0/24".to_string()),
            pod_cidrs: None,
            provider_id: None,
            unschedulable: Some(false),
            taints: None,
        }),
        status: Some(NodeStatus {
            capacity: None,
            allocatable: None,
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(chrono::Utc::now()),
                last_transition_time: Some(chrono::Utc::now()),
                reason: Some("KubeletReady".to_string()),
                message: Some("kubelet is posting ready status".to_string()),
            }]),
            addresses: Some(vec![
                NodeAddress {
                    address_type: "InternalIP".to_string(),
                    address: "192.168.1.100".to_string(),
                },
                NodeAddress {
                    address_type: "Hostname".to_string(),
                    address: name.to_string(),
                },
            ]),
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        }),
    }
}

#[tokio::test]
async fn test_node_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_node("test-node");
    let key = build_key("nodes", None, "test-node");

    // Create
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert_eq!(created.metadata.name, "test-node");
    assert_eq!(
        created.spec.as_ref().unwrap().pod_cidr,
        Some("10.244.0.0/24".to_string())
    );

    // Get
    let retrieved: Node = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-node");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_node("test-update-node");
    let key = build_key("nodes", None, "test-update-node");

    // Create
    storage.create(&key, &node).await.unwrap();

    // Update to unschedulable
    node.spec.as_mut().unwrap().unschedulable = Some(true);
    let updated: Node = storage.update(&key, &node).await.unwrap();
    assert_eq!(updated.spec.as_ref().unwrap().unschedulable, Some(true));

    // Verify update
    let retrieved: Node = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.as_ref().unwrap().unschedulable, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_node("test-delete-node");
    let key = build_key("nodes", None, "test-delete-node");

    // Create
    storage.create(&key, &node).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<Node>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_node_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple nodes
    let node1 = create_test_node("node-1");
    let node2 = create_test_node("node-2");
    let node3 = create_test_node("node-3");

    let key1 = build_key("nodes", None, "node-1");
    let key2 = build_key("nodes", None, "node-2");
    let key3 = build_key("nodes", None, "node-3");

    storage.create(&key1, &node1).await.unwrap();
    storage.create(&key2, &node2).await.unwrap();
    storage.create(&key3, &node3).await.unwrap();

    // List
    let prefix = build_prefix("nodes", None);
    let nodes: Vec<Node> = storage.list(&prefix).await.unwrap();

    assert!(nodes.len() >= 3);
    let names: Vec<String> = nodes.iter().map(|n| n.metadata.name.clone()).collect();
    assert!(names.contains(&"node-1".to_string()));
    assert!(names.contains(&"node-2".to_string()));
    assert!(names.contains(&"node-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_node_with_status() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_node("test-status");
    let key = build_key("nodes", None, "test-status");

    // Create with status
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert!(created.status.is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_ready_condition() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_node("test-ready");
    let key = build_key("nodes", None, "test-ready");

    // Create with Ready condition
    let created: Node = storage.create(&key, &node).await.unwrap();
    let conditions = created
        .status
        .as_ref()
        .unwrap()
        .conditions
        .as_ref()
        .unwrap();
    assert_eq!(conditions.len(), 1);
    assert_eq!(conditions[0].condition_type, "Ready");
    assert_eq!(conditions[0].status, "True");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_unschedulable() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_node("test-unschedulable");
    node.spec.as_mut().unwrap().unschedulable = Some(true);

    let key = build_key("nodes", None, "test-unschedulable");

    // Create unschedulable node
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert_eq!(created.spec.as_ref().unwrap().unschedulable, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_with_addresses() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_node("test-addresses");
    let key = build_key("nodes", None, "test-addresses");

    // Create with addresses
    let created: Node = storage.create(&key, &node).await.unwrap();
    let addresses = created.status.as_ref().unwrap().addresses.as_ref().unwrap();
    assert_eq!(addresses.len(), 2);
    assert_eq!(addresses[0].address_type, "InternalIP");
    assert_eq!(addresses[0].address, "192.168.1.100");
    assert_eq!(addresses[1].address_type, "Hostname");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_with_pod_cidr() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_node("test-cidr");
    node.spec.as_mut().unwrap().pod_cidr = Some("10.100.0.0/24".to_string());

    let key = build_key("nodes", None, "test-cidr");

    // Create with CIDR
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert_eq!(
        created.spec.as_ref().unwrap().pod_cidr,
        Some("10.100.0.0/24".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_node("test-immutable");
    let key = build_key("nodes", None, "test-immutable");

    // Create
    let created: Node = storage.create(&key, &node).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_node = created.clone();
    updated_node.spec.as_mut().unwrap().unschedulable = Some(true);

    let updated: Node = storage.update(&key, &updated_node).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(updated.spec.as_ref().unwrap().unschedulable, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("node-role.kubernetes.io/master".to_string(), "".to_string());
    labels.insert("kubernetes.io/arch".to_string(), "amd64".to_string());

    let mut node = create_test_node("test-labels");
    node.metadata.labels = Some(labels);

    let key = build_key("nodes", None, "test-labels");

    // Create with labels
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert!(created_labels.contains_key("node-role.kubernetes.io/master"));
    assert_eq!(
        created_labels.get("kubernetes.io/arch"),
        Some(&"amd64".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("nodes", None, "nonexistent");
    let result = storage.get::<Node>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_node_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_node("nonexistent");
    let key = build_key("nodes", None, "nonexistent");

    let result = storage.update(&key, &node).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_node_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_node("test-finalizers");
    node.metadata.finalizers = Some(vec!["finalizer.test.io".to_string()]);

    let key = build_key("nodes", None, "test-finalizers");

    // Create with finalizer
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: Node = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Clean up - remove finalizer first
    node.metadata.finalizers = None;
    storage.update(&key, &node).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert("node.alpha.kubernetes.io/ttl".to_string(), "0".to_string());

    let mut node = create_test_node("test-annotations");
    node.metadata.annotations = Some(annotations);

    let key = build_key("nodes", None, "test-annotations");

    // Create with annotations
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    assert_eq!(
        created
            .metadata
            .annotations
            .unwrap()
            .get("node.alpha.kubernetes.io/ttl"),
        Some(&"0".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_with_provider_id() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_node("test-provider");
    node.spec.as_mut().unwrap().provider_id =
        Some("aws:///us-east-1a/i-1234567890abcdef0".to_string());

    let key = build_key("nodes", None, "test-provider");

    // Create with provider ID
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert_eq!(
        created.spec.as_ref().unwrap().provider_id,
        Some("aws:///us-east-1a/i-1234567890abcdef0".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_cluster_scoped() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_node("test-cluster-scoped");
    let key = build_key("nodes", None, "test-cluster-scoped");

    // Nodes are cluster-scoped, namespace should be None
    let created: Node = storage.create(&key, &node).await.unwrap();
    assert!(created.metadata.namespace.is_none());

    // Clean up
    storage.delete(&key).await.unwrap();
}
