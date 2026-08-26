// Integration tests for Volume Snapshot Controller
// Note: These tests use in-memory storage for fast, isolated testing

use rusternetes_common::resources::volume::*;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::volume_snapshot::VolumeSnapshotController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn setup_test() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

async fn create_test_pvc(
    storage: &MemoryStorage,
    name: &str,
    namespace: &str,
    pv_name: &str,
) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                limits: None,
                requests: Some(requests),
            },
            volume_name: Some(pv_name.to_string()),
            storage_class_name: Some("fast".to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeClaimStatus {
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            phase: PersistentVolumeClaimPhase::Bound,
            access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
            capacity: Some({
                let mut cap = HashMap::new();
                cap.insert("storage".to_string(), "5Gi".to_string());
                cap
            }),
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let key = build_key("persistentvolumeclaims", Some(namespace), name);
    storage.create(&key, &pvc).await.unwrap()
}

async fn create_test_pv(storage: &MemoryStorage, name: &str) -> PersistentVolume {
    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), "10Gi".to_string());

    let pv = PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeSpec {
            capacity,
            host_path: Some(HostPathVolumeSource {
                path: format!("/tmp/test-pv/{}", name),
                r#type: Some(HostPathType::DirectoryOrCreate),
            }),
            nfs: None,
            iscsi: None,
            local: None,
            csi: None,
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
            storage_class_name: Some("fast".to_string()),
            mount_options: None,
            volume_mode: Some(PersistentVolumeMode::Filesystem),
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
    };

    let key = build_key("persistentvolumes", None, name);
    storage.create(&key, &pv).await.unwrap()
}

#[tokio::test]
async fn test_snapshot_content_auto_creation() {
    let storage = setup_test().await;

    // Create VolumeSnapshotClass
    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("test-snapclass"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Delete,
    };

    let vsc_key = build_key("volumesnapshotclasses", None, "test-snapclass");
    storage.create(&vsc_key, &vsc).await.unwrap();

    // Create PV and bound PVC
    let _pv = create_test_pv(&storage, "test-pv").await;
    let _pvc = create_test_pvc(&storage, "test-pvc", "default", "test-pv").await;

    // Create VolumeSnapshot
    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-snapshot");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("test-pvc".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: "test-snapclass".to_string(),
        },
        status: None,
    };

    let vs_key = build_key("volumesnapshots", Some("default"), "test-snapshot");
    storage.create(&vs_key, &vs).await.unwrap();

    // Run controller reconciliation
    let controller = VolumeSnapshotController::new(storage.clone());
    tokio::spawn(async move {
        let _ = controller.reconcile_all().await;
    });

    // Wait for reconciliation
    sleep(Duration::from_secs(2)).await;

    // Verify VolumeSnapshotContent was created
    let content_name = "snapcontent-default-test-snapshot";
    let content_key = build_key("volumesnapshotcontents", None, content_name);
    let content: Result<VolumeSnapshotContent, _> = storage.get(&content_key).await;

    assert!(content.is_ok(), "VolumeSnapshotContent should be created");
    let content = content.unwrap();
    assert_eq!(content.spec.driver, "rusternetes.io/hostpath-snapshotter");
    assert_eq!(content.spec.deletion_policy, DeletionPolicy::Delete);

    // Verify VolumeSnapshot status was updated
    let updated_vs: VolumeSnapshot = storage.get(&vs_key).await.unwrap();
    assert!(updated_vs.status.is_some());
    assert_eq!(updated_vs.status.as_ref().unwrap().ready_to_use, Some(true));
}

#[tokio::test]
async fn test_snapshot_deletion_with_delete_policy() {
    let storage = setup_test().await;

    // Create VolumeSnapshotClass with Delete policy
    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("delete-policy-class"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Delete,
    };

    let vsc_key = build_key("volumesnapshotclasses", None, "delete-policy-class");
    storage.create(&vsc_key, &vsc).await.unwrap();

    // Create PV and bound PVC
    let _pv = create_test_pv(&storage, "test-pv-delete").await;
    let _pvc = create_test_pvc(&storage, "test-pvc-delete", "default", "test-pv-delete").await;

    // Create VolumeSnapshot
    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-snapshot-delete");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("test-pvc-delete".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: "delete-policy-class".to_string(),
        },
        status: None,
    };

    let vs_key = build_key("volumesnapshots", Some("default"), "test-snapshot-delete");
    storage.create(&vs_key, &vs).await.unwrap();

    // Run controller to create snapshot content
    let controller = VolumeSnapshotController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify VolumeSnapshotContent was created
    let content_name = "snapcontent-default-test-snapshot-delete";
    let content_key = build_key("volumesnapshotcontents", None, content_name);
    let content: Result<VolumeSnapshotContent, _> = storage.get(&content_key).await;
    assert!(
        content.is_ok(),
        "VolumeSnapshotContent should exist before deletion"
    );

    // Delete the VolumeSnapshot
    storage.delete(&vs_key).await.unwrap();

    // Run controller to reconcile deletions
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify VolumeSnapshotContent was also deleted (Delete policy)
    let content_after: Result<VolumeSnapshotContent, _> = storage.get(&content_key).await;
    assert!(
        content_after.is_err(),
        "VolumeSnapshotContent should be deleted with Delete policy"
    );
}

#[tokio::test]
async fn test_snapshot_deletion_with_retain_policy() {
    let storage = setup_test().await;

    // Create VolumeSnapshotClass with Retain policy
    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("retain-policy-class"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Retain,
    };

    let vsc_key = build_key("volumesnapshotclasses", None, "retain-policy-class");
    storage.create(&vsc_key, &vsc).await.unwrap();

    // Create PV and bound PVC
    let _pv = create_test_pv(&storage, "test-pv-retain").await;
    let _pvc = create_test_pvc(&storage, "test-pvc-retain", "default", "test-pv-retain").await;

    // Create VolumeSnapshot
    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-snapshot-retain");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("test-pvc-retain".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: "retain-policy-class".to_string(),
        },
        status: None,
    };

    let vs_key = build_key("volumesnapshots", Some("default"), "test-snapshot-retain");
    storage.create(&vs_key, &vs).await.unwrap();

    // Run controller to create snapshot content
    let controller = VolumeSnapshotController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify VolumeSnapshotContent was created
    let content_name = "snapcontent-default-test-snapshot-retain";
    let content_key = build_key("volumesnapshotcontents", None, content_name);
    let content: Result<VolumeSnapshotContent, _> = storage.get(&content_key).await;
    assert!(
        content.is_ok(),
        "VolumeSnapshotContent should exist before deletion"
    );

    // Delete the VolumeSnapshot
    storage.delete(&vs_key).await.unwrap();

    // Run controller to reconcile deletions
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify VolumeSnapshotContent still exists (Retain policy)
    let content_after: Result<VolumeSnapshotContent, _> = storage.get(&content_key).await;
    assert!(
        content_after.is_ok(),
        "VolumeSnapshotContent should be retained with Retain policy"
    );
}

#[tokio::test]
async fn test_snapshot_without_bound_pvc_fails() {
    let storage = setup_test().await;

    // Create VolumeSnapshotClass
    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("test-class-unbound"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Delete,
    };

    let vsc_key = build_key("volumesnapshotclasses", None, "test-class-unbound");
    storage.create(&vsc_key, &vsc).await.unwrap();

    // Create unbound PVC (no volume_name)
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("unbound-pvc");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                limits: None,
                requests: Some(requests),
            },
            volume_name: None, // Not bound
            storage_class_name: Some("fast".to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeClaimStatus {
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            phase: PersistentVolumeClaimPhase::Pending, // Pending, not Bound
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "unbound-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Create VolumeSnapshot
    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("snapshot-unbound");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("unbound-pvc".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: "test-class-unbound".to_string(),
        },
        status: None,
    };

    let vs_key = build_key("volumesnapshots", Some("default"), "snapshot-unbound");
    storage.create(&vs_key, &vs).await.unwrap();

    // Run controller
    let controller = VolumeSnapshotController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify no VolumeSnapshotContent was created
    let content_name = "snapcontent-default-snapshot-unbound";
    let content_key = build_key("volumesnapshotcontents", None, content_name);
    let content: Result<VolumeSnapshotContent, _> = storage.get(&content_key).await;
    assert!(
        content.is_err(),
        "VolumeSnapshotContent should not be created for unbound PVC"
    );
}

#[tokio::test]
async fn test_snapshot_with_invalid_class_fails() {
    let storage = setup_test().await;

    // Create PV and bound PVC
    let _pv = create_test_pv(&storage, "test-pv-invalid").await;
    let _pvc = create_test_pvc(&storage, "test-pvc-invalid", "default", "test-pv-invalid").await;

    // Create VolumeSnapshot with non-existent class
    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("snapshot-invalid-class");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("test-pvc-invalid".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: "non-existent-class".to_string(),
        },
        status: None,
    };

    let vs_key = build_key("volumesnapshots", Some("default"), "snapshot-invalid-class");
    storage.create(&vs_key, &vs).await.unwrap();

    // Run controller (should fail gracefully)
    let controller = VolumeSnapshotController::new(storage.clone());
    controller.reconcile_all().await.unwrap(); // Should not panic
    sleep(Duration::from_millis(500)).await;

    // Verify no VolumeSnapshotContent was created
    let content_name = "snapcontent-default-snapshot-invalid-class";
    let content_key = build_key("volumesnapshotcontents", None, content_name);
    let content: Result<VolumeSnapshotContent, _> = storage.get(&content_key).await;
    assert!(
        content.is_err(),
        "VolumeSnapshotContent should not be created with invalid class"
    );
}
