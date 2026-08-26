//! Integration tests for CSINode handler
//!
//! Tests all CRUD operations, edge cases, and error handling for csinodes.
//! CSINode holds information about all CSI drivers installed on a node.

use rusternetes_common::resources::csi::{
    CSINode, CSINodeDriver, CSINodeSpec, VolumeNodeResources,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test csinode with basic configuration
fn create_test_csinode(name: &str) -> CSINode {
    CSINode {
        type_meta: TypeMeta {
            api_version: "storage.k8s.io/v1".to_string(),
            kind: "CSINode".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            ..Default::default()
        },
        spec: CSINodeSpec { drivers: vec![] },
    }
}

#[tokio::test]
async fn test_csinode_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("node-1");
    node.spec.drivers = vec![CSINodeDriver {
        name: "ebs.csi.aws.com".to_string(),
        node_id: "i-0123456789abcdef0".to_string(),
        topology_keys: None,
        allocatable: None,
    }];

    let key = build_key("csinodes", None, "node-1");

    // Create
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    assert_eq!(created.metadata.name, "node-1");
    assert!(!created.metadata.uid.is_empty());
    assert_eq!(created.spec.drivers.len(), 1);
    assert_eq!(created.spec.drivers[0].name, "ebs.csi.aws.com");

    // Get
    let retrieved: CSINode = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "node-1");
    assert_eq!(retrieved.spec.drivers[0].name, "ebs.csi.aws.com");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-update-node");
    node.spec.drivers = vec![CSINodeDriver {
        name: "ebs.csi.aws.com".to_string(),
        node_id: "node-123".to_string(),
        topology_keys: None,
        allocatable: None,
    }];

    let key = build_key("csinodes", None, "test-update-node");

    // Create
    storage.create(&key, &node).await.unwrap();

    // Update - add another driver
    node.spec.drivers.push(CSINodeDriver {
        name: "efs.csi.aws.com".to_string(),
        node_id: "node-123".to_string(),
        topology_keys: None,
        allocatable: None,
    });

    let updated: CSINode = storage.update(&key, &node).await.unwrap();
    assert_eq!(updated.spec.drivers.len(), 2);

    // Verify update
    let retrieved: CSINode = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.drivers.len(), 2);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_csinode("test-delete-node");
    let key = build_key("csinodes", None, "test-delete-node");

    // Create
    storage.create(&key, &node).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<CSINode>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_csinode_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple csinodes
    let node1 = create_test_csinode("node-1");
    let node2 = create_test_csinode("node-2");
    let node3 = create_test_csinode("node-3");

    let key1 = build_key("csinodes", None, "node-1");
    let key2 = build_key("csinodes", None, "node-2");
    let key3 = build_key("csinodes", None, "node-3");

    storage.create(&key1, &node1).await.unwrap();
    storage.create(&key2, &node2).await.unwrap();
    storage.create(&key3, &node3).await.unwrap();

    // List
    let prefix = build_prefix("csinodes", None);
    let nodes: Vec<CSINode> = storage.list(&prefix).await.unwrap();

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
async fn test_csinode_with_multiple_drivers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-multi-drivers");
    node.spec.drivers = vec![
        CSINodeDriver {
            name: "ebs.csi.aws.com".to_string(),
            node_id: "i-abc123".to_string(),
            topology_keys: Some(vec!["topology.ebs.csi.aws.com/zone".to_string()]),
            allocatable: None,
        },
        CSINodeDriver {
            name: "efs.csi.aws.com".to_string(),
            node_id: "i-abc123".to_string(),
            topology_keys: None,
            allocatable: None,
        },
        CSINodeDriver {
            name: "fsx.csi.aws.com".to_string(),
            node_id: "i-abc123".to_string(),
            topology_keys: None,
            allocatable: None,
        },
    ];

    let key = build_key("csinodes", None, "test-multi-drivers");

    // Create with multiple drivers
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    assert_eq!(created.spec.drivers.len(), 3);
    assert_eq!(created.spec.drivers[0].name, "ebs.csi.aws.com");
    assert_eq!(created.spec.drivers[1].name, "efs.csi.aws.com");
    assert_eq!(created.spec.drivers[2].name, "fsx.csi.aws.com");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_with_topology_keys() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-topology");
    node.spec.drivers = vec![CSINodeDriver {
        name: "pd.csi.storage.gke.io".to_string(),
        node_id: "gke-node-123".to_string(),
        topology_keys: Some(vec![
            "topology.kubernetes.io/zone".to_string(),
            "topology.kubernetes.io/region".to_string(),
        ]),
        allocatable: None,
    }];

    let key = build_key("csinodes", None, "test-topology");

    // Create with topology keys
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    let driver = &created.spec.drivers[0];
    assert!(driver.topology_keys.is_some());
    let keys = driver.topology_keys.as_ref().unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], "topology.kubernetes.io/zone");
    assert_eq!(keys[1], "topology.kubernetes.io/region");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_with_allocatable() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-allocatable");
    node.spec.drivers = vec![CSINodeDriver {
        name: "ebs.csi.aws.com".to_string(),
        node_id: "node-123".to_string(),
        topology_keys: None,
        allocatable: Some(VolumeNodeResources { count: Some(39) }),
    }];

    let key = build_key("csinodes", None, "test-allocatable");

    // Create with allocatable resources
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    let driver = &created.spec.drivers[0];
    assert!(driver.allocatable.is_some());
    assert_eq!(driver.allocatable.as_ref().unwrap().count, Some(39));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-finalizers");
    node.metadata.finalizers = Some(vec!["finalizer.csi.storage.k8s.io".to_string()]);

    let key = build_key("csinodes", None, "test-finalizers");

    // Create with finalizer
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.csi.storage.k8s.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: CSINode = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.csi.storage.k8s.io".to_string()])
    );

    // Clean up - remove finalizer first
    node.metadata.finalizers = None;
    storage.update(&key, &node).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_csinode("test-immutable");
    let key = build_key("csinodes", None, "test-immutable");

    // Create
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_node = created.clone();
    updated_node.spec.drivers.push(CSINodeDriver {
        name: "new-driver".to_string(),
        node_id: "node-123".to_string(),
        topology_keys: None,
        allocatable: None,
    });

    let updated: CSINode = storage.update(&key, &updated_node).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(updated.spec.drivers.len(), 1);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("csinodes", None, "nonexistent");
    let result = storage.get::<CSINode>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_csinode_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let node = create_test_csinode("nonexistent");
    let key = build_key("csinodes", None, "nonexistent");

    let result = storage.update(&key, &node).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_csinode_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("node-role.kubernetes.io/worker".to_string(), "".to_string());
    labels.insert(
        "topology.kubernetes.io/zone".to_string(),
        "us-west-1a".to_string(),
    );

    let mut node = create_test_csinode("test-labels");
    node.metadata.labels = Some(labels.clone());

    let key = build_key("csinodes", None, "test-labels");

    // Create with labels
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert!(created_labels.contains_key("node-role.kubernetes.io/worker"));
    assert_eq!(
        created_labels.get("topology.kubernetes.io/zone"),
        Some(&"us-west-1a".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert(
        "csi.volume.kubernetes.io/nodeid".to_string(),
        "{\"ebs.csi.aws.com\":\"i-abc123\"}".to_string(),
    );
    annotations.insert("description".to_string(), "Test node".to_string());

    let mut node = create_test_csinode("test-annotations");
    node.metadata.annotations = Some(annotations.clone());

    let key = build_key("csinodes", None, "test-annotations");

    // Create with annotations
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    let created_annotations = created.metadata.annotations.unwrap();
    assert!(created_annotations.contains_key("csi.volume.kubernetes.io/nodeid"));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_empty_drivers() {
    let storage = Arc::new(MemoryStorage::new());

    // CSINode with empty drivers list
    let node = create_test_csinode("test-empty-drivers");
    let key = build_key("csinodes", None, "test-empty-drivers");

    let created: CSINode = storage.create(&key, &node).await.unwrap();
    assert_eq!(created.spec.drivers.len(), 0);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_common_drivers() {
    let storage = Arc::new(MemoryStorage::new());

    // Test common CSI drivers
    let drivers = [
        ("ebs.csi.aws.com", "i-abc123"),
        ("efs.csi.aws.com", "i-abc123"),
        ("pd.csi.storage.gke.io", "gke-node-123"),
        ("disk.csi.azure.com", "azure-node-456"),
    ];

    for (idx, (driver_name, node_id)) in drivers.iter().enumerate() {
        let name = format!("test-driver-{}", idx);
        let mut node = create_test_csinode(&name);
        node.spec.drivers = vec![CSINodeDriver {
            name: driver_name.to_string(),
            node_id: node_id.to_string(),
            topology_keys: None,
            allocatable: None,
        }];

        let key = build_key("csinodes", None, &name);

        let created: CSINode = storage.create(&key, &node).await.unwrap();
        assert_eq!(created.spec.drivers[0].name, *driver_name);
        assert_eq!(created.spec.drivers[0].node_id, *node_id);

        // Clean up
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_csinode_all_fields() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("test".to_string(), "comprehensive".to_string());

    let mut annotations = HashMap::new();
    annotations.insert("description".to_string(), "Full test".to_string());

    let mut node = create_test_csinode("test-all-fields");
    node.metadata.labels = Some(labels);
    node.metadata.annotations = Some(annotations);
    node.spec.drivers = vec![
        CSINodeDriver {
            name: "ebs.csi.aws.com".to_string(),
            node_id: "i-0123456789abcdef0".to_string(),
            topology_keys: Some(vec!["topology.ebs.csi.aws.com/zone".to_string()]),
            allocatable: Some(VolumeNodeResources { count: Some(39) }),
        },
        CSINodeDriver {
            name: "efs.csi.aws.com".to_string(),
            node_id: "i-0123456789abcdef0".to_string(),
            topology_keys: None,
            allocatable: None,
        },
    ];

    let key = build_key("csinodes", None, "test-all-fields");

    // Create with all fields populated
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    assert!(created.metadata.labels.is_some());
    assert!(created.metadata.annotations.is_some());
    assert_eq!(created.spec.drivers.len(), 2);

    let driver1 = &created.spec.drivers[0];
    assert_eq!(driver1.name, "ebs.csi.aws.com");
    assert!(driver1.topology_keys.is_some());
    assert!(driver1.allocatable.is_some());

    let driver2 = &created.spec.drivers[1];
    assert_eq!(driver2.name, "efs.csi.aws.com");
    assert!(driver2.topology_keys.is_none());
    assert!(driver2.allocatable.is_none());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_driver_update_scenario() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-driver-update");
    node.spec.drivers = vec![CSINodeDriver {
        name: "ebs.csi.aws.com".to_string(),
        node_id: "old-node-id".to_string(),
        topology_keys: None,
        allocatable: None,
    }];

    let key = build_key("csinodes", None, "test-driver-update");

    // Create
    storage.create(&key, &node).await.unwrap();

    // Update driver info (node_id changed)
    node.spec.drivers[0].node_id = "new-node-id".to_string();
    node.spec.drivers[0].topology_keys = Some(vec!["topology.ebs.csi.aws.com/zone".to_string()]);
    node.spec.drivers[0].allocatable = Some(VolumeNodeResources { count: Some(39) });

    let updated: CSINode = storage.update(&key, &node).await.unwrap();
    assert_eq!(updated.spec.drivers[0].node_id, "new-node-id");
    assert!(updated.spec.drivers[0].topology_keys.is_some());
    assert!(updated.spec.drivers[0].allocatable.is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_max_allocatable() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-max-allocatable");
    node.spec.drivers = vec![CSINodeDriver {
        name: "test-driver".to_string(),
        node_id: "node-123".to_string(),
        topology_keys: None,
        allocatable: Some(VolumeNodeResources { count: Some(256) }),
    }];

    let key = build_key("csinodes", None, "test-max-allocatable");

    // Create with large allocatable count
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    let allocatable = created.spec.drivers[0].allocatable.as_ref().unwrap();
    assert_eq!(allocatable.count, Some(256));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_zero_allocatable() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-zero-allocatable");
    node.spec.drivers = vec![CSINodeDriver {
        name: "test-driver".to_string(),
        node_id: "node-123".to_string(),
        topology_keys: None,
        allocatable: Some(VolumeNodeResources { count: Some(0) }),
    }];

    let key = build_key("csinodes", None, "test-zero-allocatable");

    // Create with zero allocatable (valid - node is full)
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    let allocatable = created.spec.drivers[0].allocatable.as_ref().unwrap();
    assert_eq!(allocatable.count, Some(0));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csinode_single_topology_key() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node = create_test_csinode("test-single-topo");
    node.spec.drivers = vec![CSINodeDriver {
        name: "ebs.csi.aws.com".to_string(),
        node_id: "i-abc123".to_string(),
        topology_keys: Some(vec!["topology.ebs.csi.aws.com/zone".to_string()]),
        allocatable: None,
    }];

    let key = build_key("csinodes", None, "test-single-topo");

    // Create with single topology key
    let created: CSINode = storage.create(&key, &node).await.unwrap();
    let keys = created.spec.drivers[0].topology_keys.as_ref().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "topology.ebs.csi.aws.com/zone");

    // Clean up
    storage.delete(&key).await.unwrap();
}
