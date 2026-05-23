//! Integration tests for NodeController

use chrono::{Duration, Utc};
use rusternetes_common::resources::{Node, NodeCondition, NodeStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::node::NodeController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

#[tokio::test]
async fn test_node_controller_creation() {
    let storage = Arc::new(MemoryStorage::new());
    let _controller = NodeController::new(storage);
}

#[tokio::test]
async fn test_node_ready_with_recent_heartbeat() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // Create a node with recent heartbeat
    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-node-ready".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(Utc::now()), // Recent heartbeat
                last_transition_time: Some(Utc::now()),
                reason: Some("KubeletReady".to_string()),
                message: Some("kubelet is ready".to_string()),
            }]),
            addresses: None,
            capacity: None,
            allocatable: None,
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
    };

    let key = build_key("nodes", None, "test-node-ready");
    storage.create(&key, &node).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Node should still be marked as ready
    let retrieved: Node = storage.get(&key).await.unwrap();
    let ready_condition = retrieved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| conditions.iter().find(|c| c.condition_type == "Ready"));

    assert!(ready_condition.is_some());
    assert_eq!(ready_condition.unwrap().status, "True");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_not_ready_with_old_heartbeat() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // Create a node with old heartbeat (60 seconds ago)
    let old_time = Utc::now() - Duration::seconds(60);

    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-node-not-ready".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(old_time), // Old heartbeat
                last_transition_time: Some(old_time),
                reason: Some("KubeletReady".to_string()),
                message: Some("kubelet is ready".to_string()),
            }]),
            addresses: None,
            capacity: None,
            allocatable: None,
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
    };

    let key = build_key("nodes", None, "test-node-not-ready");
    storage.create(&key, &node).await.unwrap();

    // Skip the 60s startup grace so reconcile flips the condition this tick.
    controller.seed_first_seen_for_test("test-node-not-ready");

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Node should be marked as not ready
    let retrieved: Node = storage.get(&key).await.unwrap();
    let ready_condition = retrieved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| conditions.iter().find(|c| c.condition_type == "Ready"));

    assert!(ready_condition.is_some());
    let condition = ready_condition.unwrap();

    // Status should be False due to old heartbeat
    assert_eq!(condition.status, "False");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_without_ready_condition() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // Create a node without Ready condition
    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-node-no-condition".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions: None,
            addresses: None,
            capacity: None,
            allocatable: None,
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
    };

    let key = build_key("nodes", None, "test-node-no-condition");
    storage.create(&key, &node).await.unwrap();

    // Skip the 60s startup grace so reconcile creates the condition this tick.
    controller.seed_first_seen_for_test("test-node-no-condition");

    // Reconcile should create a Ready condition
    controller.reconcile_all().await.unwrap();

    // Node should have a Ready condition now
    let retrieved: Node = storage.get(&key).await.unwrap();
    let ready_condition = retrieved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| conditions.iter().find(|c| c.condition_type == "Ready"));

    assert!(ready_condition.is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}
