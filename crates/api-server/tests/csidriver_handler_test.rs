//! Integration tests for CSIDriver handler
//!
//! Tests all CRUD operations, edge cases, and error handling for csidrivers.
//! CSIDriver represents information about a CSI volume driver installed on a cluster.

use rusternetes_common::resources::csi::{
    CSIDriver, CSIDriverSpec, FSGroupPolicy, TokenRequest, VolumeLifecycleMode,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test csidriver with basic configuration
fn create_test_csidriver(name: &str) -> CSIDriver {
    CSIDriver {
        type_meta: TypeMeta {
            api_version: "storage.k8s.io/v1".to_string(),
            kind: "CSIDriver".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            ..Default::default()
        },
        spec: CSIDriverSpec {
            attach_required: Some(true),
            pod_info_on_mount: Some(false),
            fs_group_policy: None,
            storage_capacity: Some(false),
            volume_lifecycle_modes: None,
            token_requests: None,
            requires_republish: Some(false),
            se_linux_mount: Some(false),
            node_allocatable_update_period_seconds: None,
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn test_csidriver_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let driver = create_test_csidriver("ebs.csi.aws.com");
    let key = build_key("csidrivers", None, "ebs.csi.aws.com");

    // Create
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert_eq!(created.metadata.name, "ebs.csi.aws.com");
    assert!(!created.metadata.uid.is_empty());
    assert_eq!(created.spec.attach_required, Some(true));
    assert_eq!(created.spec.pod_info_on_mount, Some(false));

    // Get
    let retrieved: CSIDriver = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "ebs.csi.aws.com");
    assert_eq!(retrieved.spec.attach_required, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-driver");
    let key = build_key("csidrivers", None, "test-driver");

    // Create
    storage.create(&key, &driver).await.unwrap();

    // Update storage_capacity
    driver.spec.storage_capacity = Some(true);
    let updated: CSIDriver = storage.update(&key, &driver).await.unwrap();
    assert_eq!(updated.spec.storage_capacity, Some(true));

    // Verify update
    let retrieved: CSIDriver = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.storage_capacity, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let driver = create_test_csidriver("test-delete-driver");
    let key = build_key("csidrivers", None, "test-delete-driver");

    // Create
    storage.create(&key, &driver).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<CSIDriver>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_csidriver_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple csidrivers
    let driver1 = create_test_csidriver("ebs.csi.aws.com");
    let driver2 = create_test_csidriver("efs.csi.aws.com");
    let driver3 = create_test_csidriver("pd.csi.storage.gke.io");

    let key1 = build_key("csidrivers", None, "ebs.csi.aws.com");
    let key2 = build_key("csidrivers", None, "efs.csi.aws.com");
    let key3 = build_key("csidrivers", None, "pd.csi.storage.gke.io");

    storage.create(&key1, &driver1).await.unwrap();
    storage.create(&key2, &driver2).await.unwrap();
    storage.create(&key3, &driver3).await.unwrap();

    // List
    let prefix = build_prefix("csidrivers", None);
    let drivers: Vec<CSIDriver> = storage.list(&prefix).await.unwrap();

    assert!(drivers.len() >= 3);
    let names: Vec<String> = drivers.iter().map(|d| d.metadata.name.clone()).collect();
    assert!(names.contains(&"ebs.csi.aws.com".to_string()));
    assert!(names.contains(&"efs.csi.aws.com".to_string()));
    assert!(names.contains(&"pd.csi.storage.gke.io".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_fs_group_policy() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-fs-group");
    driver.spec.fs_group_policy = Some(FSGroupPolicy::ReadWriteOnceWithFSType);

    let key = build_key("csidrivers", None, "test-fs-group");

    // Create with FSGroupPolicy
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert_eq!(
        created.spec.fs_group_policy,
        Some(FSGroupPolicy::ReadWriteOnceWithFSType)
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_fs_group_policy_file() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-fs-file");
    driver.spec.fs_group_policy = Some(FSGroupPolicy::File);

    let key = build_key("csidrivers", None, "test-fs-file");

    // Create with File policy
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert_eq!(created.spec.fs_group_policy, Some(FSGroupPolicy::File));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_fs_group_policy_none() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-fs-none");
    driver.spec.fs_group_policy = Some(FSGroupPolicy::None);

    let key = build_key("csidrivers", None, "test-fs-none");

    // Create with None policy
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert_eq!(created.spec.fs_group_policy, Some(FSGroupPolicy::None));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_volume_lifecycle_modes() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-lifecycle");
    driver.spec.volume_lifecycle_modes = Some(vec![
        VolumeLifecycleMode::Persistent,
        VolumeLifecycleMode::Ephemeral,
    ]);

    let key = build_key("csidrivers", None, "test-lifecycle");

    // Create with lifecycle modes
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    let modes = created.spec.volume_lifecycle_modes.as_ref().unwrap();
    assert_eq!(modes.len(), 2);
    assert_eq!(modes[0], VolumeLifecycleMode::Persistent);
    assert_eq!(modes[1], VolumeLifecycleMode::Ephemeral);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_token_requests() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-tokens");
    driver.spec.token_requests = Some(vec![
        TokenRequest {
            audience: "https://kubernetes.default.svc".to_string(),
            expiration_seconds: Some(3600),
        },
        TokenRequest {
            audience: "https://api.example.com".to_string(),
            expiration_seconds: Some(7200),
        },
    ]);

    let key = build_key("csidrivers", None, "test-tokens");

    // Create with token requests
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    let tokens = created.spec.token_requests.as_ref().unwrap();
    assert_eq!(tokens.len(), 2);
    assert_eq!(
        tokens[0].audience,
        "https://kubernetes.default.svc".to_string()
    );
    assert_eq!(tokens[0].expiration_seconds, Some(3600));
    assert_eq!(tokens[1].audience, "https://api.example.com".to_string());
    assert_eq!(tokens[1].expiration_seconds, Some(7200));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_requires_republish() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-republish");
    driver.spec.requires_republish = Some(true);

    let key = build_key("csidrivers", None, "test-republish");

    // Create with requires_republish
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert_eq!(created.spec.requires_republish, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_se_linux_mount() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-selinux");
    driver.spec.se_linux_mount = Some(true);

    let key = build_key("csidrivers", None, "test-selinux");

    // Create with SELinux mount support
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert_eq!(created.spec.se_linux_mount, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-finalizers");
    driver.metadata.finalizers = Some(vec!["finalizer.csi.storage.k8s.io".to_string()]);

    let key = build_key("csidrivers", None, "test-finalizers");

    // Create with finalizer
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.csi.storage.k8s.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: CSIDriver = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.csi.storage.k8s.io".to_string()])
    );

    // Clean up - remove finalizer first
    driver.metadata.finalizers = None;
    storage.update(&key, &driver).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let driver = create_test_csidriver("test-immutable");
    let key = build_key("csidrivers", None, "test-immutable");

    // Create
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_driver = created.clone();
    updated_driver.spec.storage_capacity = Some(true);

    let updated: CSIDriver = storage.update(&key, &updated_driver).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(updated.spec.storage_capacity, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("csidrivers", None, "nonexistent");
    let result = storage.get::<CSIDriver>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_csidriver_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let driver = create_test_csidriver("nonexistent");
    let key = build_key("csidrivers", None, "nonexistent");

    let result = storage.update(&key, &driver).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_csidriver_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("provider".to_string(), "aws".to_string());
    labels.insert("tier".to_string(), "storage".to_string());

    let mut driver = create_test_csidriver("test-labels");
    driver.metadata.labels = Some(labels.clone());

    let key = build_key("csidrivers", None, "test-labels");

    // Create with labels
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(created_labels.get("provider"), Some(&"aws".to_string()));
    assert_eq!(created_labels.get("tier"), Some(&"storage".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert(
        "description".to_string(),
        "AWS EBS CSI driver for persistent storage".to_string(),
    );
    annotations.insert("owner".to_string(), "storage-team".to_string());

    let mut driver = create_test_csidriver("test-annotations");
    driver.metadata.annotations = Some(annotations.clone());

    let key = build_key("csidrivers", None, "test-annotations");

    // Create with annotations
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    let created_annotations = created.metadata.annotations.unwrap();
    assert_eq!(
        created_annotations.get("description"),
        Some(&"AWS EBS CSI driver for persistent storage".to_string())
    );
    assert_eq!(
        created_annotations.get("owner"),
        Some(&"storage-team".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_all_fields() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("test".to_string(), "comprehensive".to_string());

    let mut driver = create_test_csidriver("test-all-fields");
    driver.metadata.labels = Some(labels);
    driver.spec.attach_required = Some(true);
    driver.spec.pod_info_on_mount = Some(true);
    driver.spec.fs_group_policy = Some(FSGroupPolicy::ReadWriteOnceWithFSType);
    driver.spec.storage_capacity = Some(true);
    driver.spec.volume_lifecycle_modes = Some(vec![
        VolumeLifecycleMode::Persistent,
        VolumeLifecycleMode::Ephemeral,
    ]);
    driver.spec.token_requests = Some(vec![TokenRequest {
        audience: "https://kubernetes.default.svc".to_string(),
        expiration_seconds: Some(3600),
    }]);
    driver.spec.requires_republish = Some(true);
    driver.spec.se_linux_mount = Some(true);

    let key = build_key("csidrivers", None, "test-all-fields");

    // Create with all fields populated
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert!(created.metadata.labels.is_some());
    assert_eq!(created.spec.attach_required, Some(true));
    assert_eq!(created.spec.pod_info_on_mount, Some(true));
    assert_eq!(
        created.spec.fs_group_policy,
        Some(FSGroupPolicy::ReadWriteOnceWithFSType)
    );
    assert_eq!(created.spec.storage_capacity, Some(true));
    assert!(created.spec.volume_lifecycle_modes.is_some());
    assert!(created.spec.token_requests.is_some());
    assert_eq!(created.spec.requires_republish, Some(true));
    assert_eq!(created.spec.se_linux_mount, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_common_drivers() {
    let storage = Arc::new(MemoryStorage::new());

    // Test common CSI driver configurations
    let drivers = vec![
        ("ebs.csi.aws.com", true, true), // AWS EBS - requires attach, supports storage capacity
        ("efs.csi.aws.com", false, false), // AWS EFS - no attach required, no storage capacity
        ("pd.csi.storage.gke.io", true, true), // GCE PD - requires attach, supports capacity
        ("disk.csi.azure.com", true, true), // Azure Disk - requires attach
    ];

    for (name, attach_required, storage_capacity) in drivers {
        let mut driver = create_test_csidriver(name);
        driver.spec.attach_required = Some(attach_required);
        driver.spec.storage_capacity = Some(storage_capacity);

        let key = build_key("csidrivers", None, name);

        let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
        assert_eq!(created.spec.attach_required, Some(attach_required));
        assert_eq!(created.spec.storage_capacity, Some(storage_capacity));

        // Clean up
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_csidriver_ephemeral_only() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-ephemeral-only");
    driver.spec.attach_required = Some(false);
    driver.spec.volume_lifecycle_modes = Some(vec![VolumeLifecycleMode::Ephemeral]);

    let key = build_key("csidrivers", None, "test-ephemeral-only");

    // Create driver supporting only ephemeral volumes
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    assert_eq!(created.spec.attach_required, Some(false));
    let modes = created.spec.volume_lifecycle_modes.as_ref().unwrap();
    assert_eq!(modes.len(), 1);
    assert_eq!(modes[0], VolumeLifecycleMode::Ephemeral);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_csidriver_token_request_without_expiration() {
    let storage = Arc::new(MemoryStorage::new());

    let mut driver = create_test_csidriver("test-token-no-exp");
    driver.spec.token_requests = Some(vec![TokenRequest {
        audience: "https://vault.example.com".to_string(),
        expiration_seconds: None, // No expiration specified
    }]);

    let key = build_key("csidrivers", None, "test-token-no-exp");

    // Create with token request without expiration
    let created: CSIDriver = storage.create(&key, &driver).await.unwrap();
    let tokens = created.spec.token_requests.as_ref().unwrap();
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].audience, "https://vault.example.com".to_string());
    assert!(tokens[0].expiration_seconds.is_none());

    // Clean up
    storage.delete(&key).await.unwrap();
}
