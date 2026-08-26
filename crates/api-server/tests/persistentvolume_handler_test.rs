//! Integration tests for PersistentVolume handler
//!
//! Tests all CRUD operations, edge cases, and error handling for persistent volumes

use rusternetes_common::resources::volume::{
    HostPathType, HostPathVolumeSource, NFSVolumeSource, PersistentVolumeMode,
    PersistentVolumePhase, PersistentVolumeReclaimPolicy,
};
use rusternetes_common::resources::{
    PersistentVolume, PersistentVolumeAccessMode, PersistentVolumeSpec, PersistentVolumeStatus,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test persistent volume
fn create_test_pv(name: &str, capacity: &str) -> PersistentVolume {
    let mut capacity_map = HashMap::new();
    capacity_map.insert("storage".to_string(), capacity.to_string());

    PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None, // PVs are cluster-scoped
            labels: None,
            uid: String::new(),
            creation_timestamp: None,
            resource_version: None,
            finalizers: None,
            deletion_timestamp: None,
            deletion_grace_period_seconds: None,
            owner_references: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: PersistentVolumeSpec {
            capacity: capacity_map,
            host_path: Some(HostPathVolumeSource {
                path: "/mnt/data".to_string(),
                r#type: Some(HostPathType::Directory),
            }),
            nfs: None,
            iscsi: None,
            local: None,
            csi: None,
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Retain),
            storage_class_name: Some("standard".to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            mount_options: None,
            node_affinity: None,
            claim_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Available,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        }),
    }
}

#[tokio::test]
async fn test_pv_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let pv = create_test_pv("test-pv", "10Gi");
    let key = build_key("persistentvolumes", None, "test-pv");

    // Create
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(created.metadata.name, "test-pv");
    assert!(!created.metadata.uid.is_empty());
    assert_eq!(
        created.spec.capacity.get("storage"),
        Some(&"10Gi".to_string())
    );
    assert_eq!(
        created.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Available
    );

    // Get
    let retrieved: PersistentVolume = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-pv");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-update-pv", "10Gi");
    let key = build_key("persistentvolumes", None, "test-update-pv");

    // Create
    storage.create(&key, &pv).await.unwrap();

    // Update capacity
    let mut new_capacity = HashMap::new();
    new_capacity.insert("storage".to_string(), "20Gi".to_string());
    pv.spec.capacity = new_capacity;

    let updated: PersistentVolume = storage.update(&key, &pv).await.unwrap();
    assert_eq!(
        updated.spec.capacity.get("storage"),
        Some(&"20Gi".to_string())
    );

    // Verify update
    let retrieved: PersistentVolume = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.spec.capacity.get("storage"),
        Some(&"20Gi".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let pv = create_test_pv("test-delete-pv", "10Gi");
    let key = build_key("persistentvolumes", None, "test-delete-pv");

    // Create
    storage.create(&key, &pv).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<PersistentVolume>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_pv_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple PVs
    let pv1 = create_test_pv("pv-1", "10Gi");
    let pv2 = create_test_pv("pv-2", "20Gi");
    let pv3 = create_test_pv("pv-3", "30Gi");

    let key1 = build_key("persistentvolumes", None, "pv-1");
    let key2 = build_key("persistentvolumes", None, "pv-2");
    let key3 = build_key("persistentvolumes", None, "pv-3");

    storage.create(&key1, &pv1).await.unwrap();
    storage.create(&key2, &pv2).await.unwrap();
    storage.create(&key3, &pv3).await.unwrap();

    // List
    let prefix = build_prefix("persistentvolumes", None);
    let pvs: Vec<PersistentVolume> = storage.list(&prefix).await.unwrap();

    assert!(pvs.len() >= 3);
    let names: Vec<String> = pvs.iter().map(|pv| pv.metadata.name.clone()).collect();
    assert!(names.contains(&"pv-1".to_string()));
    assert!(names.contains(&"pv-2".to_string()));
    assert!(names.contains(&"pv-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_pv_reclaim_policy_retain() {
    let storage = Arc::new(MemoryStorage::new());

    let pv = create_test_pv("test-retain", "10Gi");
    let key = build_key("persistentvolumes", None, "test-retain");

    // Create with Retain policy
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(
        created.spec.persistent_volume_reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Retain)
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_reclaim_policy_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-delete-policy", "10Gi");
    pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);

    let key = build_key("persistentvolumes", None, "test-delete-policy");

    // Create with Delete policy
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(
        created.spec.persistent_volume_reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Delete)
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_reclaim_policy_recycle() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-recycle", "10Gi");
    pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Recycle);

    let key = build_key("persistentvolumes", None, "test-recycle");

    // Create with Recycle policy
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(
        created.spec.persistent_volume_reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Recycle)
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_access_modes() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-access", "10Gi");
    pv.spec.access_modes = vec![
        PersistentVolumeAccessMode::ReadWriteOnce,
        PersistentVolumeAccessMode::ReadOnlyMany,
    ];

    let key = build_key("persistentvolumes", None, "test-access");

    // Create with multiple access modes
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    let modes = &created.spec.access_modes;
    assert_eq!(modes.len(), 2);
    assert!(modes.contains(&PersistentVolumeAccessMode::ReadWriteOnce));
    assert!(modes.contains(&PersistentVolumeAccessMode::ReadOnlyMany));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_with_nfs() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-nfs", "10Gi");
    pv.spec.nfs = Some(NFSVolumeSource {
        server: "nfs.example.com".to_string(),
        path: "/exports/data".to_string(),
        read_only: Some(false),
    });
    pv.spec.host_path = None;

    let key = build_key("persistentvolumes", None, "test-nfs");

    // Create with NFS
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    let nfs = created
        .spec
        .nfs
        .as_ref()
        .expect("Expected NFS volume source");
    assert_eq!(nfs.server, "nfs.example.com");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_status_available() {
    let storage = Arc::new(MemoryStorage::new());

    let pv = create_test_pv("test-available", "10Gi");
    let key = build_key("persistentvolumes", None, "test-available");

    // Create with Available status
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(
        created.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Available
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_status_bound() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-bound", "10Gi");
    pv.status = Some(PersistentVolumeStatus {
        phase: PersistentVolumePhase::Bound,
        message: None,
        reason: None,
        last_phase_transition_time: None,
    });

    let key = build_key("persistentvolumes", None, "test-bound");

    // Create with Bound status
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(
        created.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Bound
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_with_storage_class() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-sc", "10Gi");
    pv.spec.storage_class_name = Some("fast-ssd".to_string());

    let key = build_key("persistentvolumes", None, "test-sc");

    // Create with storage class
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(
        created.spec.storage_class_name,
        Some("fast-ssd".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_volume_mode_filesystem() {
    let storage = Arc::new(MemoryStorage::new());

    let pv = create_test_pv("test-fs", "10Gi");
    let key = build_key("persistentvolumes", None, "test-fs");

    // Create with Filesystem mode
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(
        created.spec.volume_mode,
        Some(PersistentVolumeMode::Filesystem)
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_volume_mode_block() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-block", "10Gi");
    pv.spec.volume_mode = Some(PersistentVolumeMode::Block);

    let key = build_key("persistentvolumes", None, "test-block");

    // Create with Block mode
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(created.spec.volume_mode, Some(PersistentVolumeMode::Block));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let pv = create_test_pv("test-immutable", "10Gi");
    let key = build_key("persistentvolumes", None, "test-immutable");

    // Create
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_pv = created.clone();
    updated_pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);

    let updated: PersistentVolume = storage.update(&key, &updated_pv).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(
        updated.spec.persistent_volume_reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Delete)
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("persistentvolumes", None, "nonexistent");
    let result = storage.get::<PersistentVolume>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_pv_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let pv = create_test_pv("nonexistent", "10Gi");
    let key = build_key("persistentvolumes", None, "nonexistent");

    let result = storage.update(&key, &pv).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_pv_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pv = create_test_pv("test-finalizers", "10Gi");
    pv.metadata.finalizers = Some(vec!["kubernetes.io/pv-protection".to_string()]);

    let key = build_key("persistentvolumes", None, "test-finalizers");

    // Create with finalizer
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["kubernetes.io/pv-protection".to_string()])
    );

    // Verify finalizer is present
    let retrieved: PersistentVolume = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["kubernetes.io/pv-protection".to_string()])
    );

    // Clean up - remove finalizer first
    pv.metadata.finalizers = None;
    storage.update(&key, &pv).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("type".to_string(), "local".to_string());
    labels.insert("performance".to_string(), "high".to_string());

    let mut pv = create_test_pv("test-labels", "10Gi");
    pv.metadata.labels = Some(labels);

    let key = build_key("persistentvolumes", None, "test-labels");

    // Create with labels
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(created_labels.get("type"), Some(&"local".to_string()));
    assert_eq!(created_labels.get("performance"), Some(&"high".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert(
        "pv.kubernetes.io/provisioned-by".to_string(),
        "manual".to_string(),
    );

    let mut pv = create_test_pv("test-annotations", "10Gi");
    pv.metadata.annotations = Some(annotations);

    let key = build_key("persistentvolumes", None, "test-annotations");

    // Create with annotations
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    assert_eq!(
        created
            .metadata
            .annotations
            .unwrap()
            .get("pv.kubernetes.io/provisioned-by"),
        Some(&"manual".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pv_cluster_scoped() {
    let storage = Arc::new(MemoryStorage::new());

    let pv = create_test_pv("test-cluster-scoped", "10Gi");
    let key = build_key("persistentvolumes", None, "test-cluster-scoped");

    // PVs are cluster-scoped, namespace should be None
    let created: PersistentVolume = storage.create(&key, &pv).await.unwrap();
    assert!(created.metadata.namespace.is_none());

    // Clean up
    storage.delete(&key).await.unwrap();
}
