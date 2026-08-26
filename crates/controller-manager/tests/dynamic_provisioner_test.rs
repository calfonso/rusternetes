// Integration tests for Dynamic Provisioner Controller
// Note: These tests use in-memory storage for fast, isolated testing

use rusternetes_common::resources::volume::*;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::dynamic_provisioner::DynamicProvisionerController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn setup_test() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

#[tokio::test]
async fn test_provisions_pv_for_pvc_with_storageclass() {
    let storage = setup_test().await;

    // Create StorageClass
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: Some({
            let mut params = HashMap::new();
            params.insert(
                "path".to_string(),
                "/tmp/rusternetes/test-dynamic-pvs".to_string(),
            );
            params
        }),
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };

    let sc_key = build_key("storageclasses", None, "fast");
    storage.create(&sc_key, &sc).await.unwrap();

    // Create PVC with storageClassName
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-pvc");
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
            volume_name: None,
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
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "test-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Run provisioner
    let controller = DynamicProvisionerController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify PV was created
    let pv_name = "pvc-default-test-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: Result<PersistentVolume, _> = storage.get(&pv_key).await;

    assert!(pv.is_ok(), "PV should be created by dynamic provisioner");
    let pv = pv.unwrap();

    assert_eq!(pv.spec.storage_class_name, Some("fast".to_string()));
    assert_eq!(pv.spec.capacity.get("storage"), Some(&"5Gi".to_string()));
    assert_eq!(
        pv.spec.access_modes,
        vec![PersistentVolumeAccessMode::ReadWriteOnce]
    );
    assert_eq!(
        pv.spec.persistent_volume_reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Delete)
    );
}

#[tokio::test]
async fn test_skips_pvc_without_storageclass() {
    let storage = setup_test().await;

    // Create PVC without storageClassName
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("no-sc-pvc");
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
            volume_name: None,
            storage_class_name: None, // No storage class
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
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "no-sc-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Run provisioner
    let controller = DynamicProvisionerController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify no PV was created
    let pv_name = "pvc-default-no-sc-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: Result<PersistentVolume, _> = storage.get(&pv_key).await;

    assert!(
        pv.is_err(),
        "PV should not be created for PVC without storage class"
    );
}

#[tokio::test]
async fn test_skips_already_bound_pvcs() {
    let storage = setup_test().await;

    // Create StorageClass
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };

    let sc_key = build_key("storageclasses", None, "fast");
    storage.create(&sc_key, &sc).await.unwrap();

    // Create already bound PVC
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("bound-pvc");
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
            volume_name: Some("existing-pv".to_string()), // Already bound
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

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "bound-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Count existing PVs before provisioning
    let pvs_before: Vec<PersistentVolume> = storage
        .list("/registry/persistentvolumes/")
        .await
        .unwrap_or_default();
    let count_before = pvs_before.len();

    // Run provisioner
    let controller = DynamicProvisionerController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify no new PV was created
    let pvs_after: Vec<PersistentVolume> = storage
        .list("/registry/persistentvolumes/")
        .await
        .unwrap_or_default();
    let count_after = pvs_after.len();

    assert_eq!(
        count_after, count_before,
        "No new PV should be created for already bound PVC"
    );
}

#[tokio::test]
async fn test_honors_storage_capacity() {
    let storage = setup_test().await;

    // Create StorageClass
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };

    let sc_key = build_key("storageclasses", None, "fast");
    storage.create(&sc_key, &sc).await.unwrap();

    // Create PVC requesting 10Gi
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "10Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("capacity-test-pvc");
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
            volume_name: None,
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
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key(
        "persistentvolumeclaims",
        Some("default"),
        "capacity-test-pvc",
    );
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Run provisioner
    let controller = DynamicProvisionerController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify PV has capacity 10Gi
    let pv_name = "pvc-default-capacity-test-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: PersistentVolume = storage.get(&pv_key).await.unwrap();

    assert_eq!(pv.spec.capacity.get("storage"), Some(&"10Gi".to_string()));
}

#[tokio::test]
async fn test_honors_access_modes() {
    let storage = setup_test().await;

    // Create StorageClass
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };

    let sc_key = build_key("storageclasses", None, "fast");
    storage.create(&sc_key, &sc).await.unwrap();

    // Create PVC with ReadWriteMany
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("rwm-pvc");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![
                PersistentVolumeAccessMode::ReadWriteMany,
                PersistentVolumeAccessMode::ReadOnlyMany,
            ],
            resources: ResourceRequirements {
                limits: None,
                requests: Some(requests),
            },
            volume_name: None,
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
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "rwm-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Run provisioner
    let controller = DynamicProvisionerController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify PV has same access modes
    let pv_name = "pvc-default-rwm-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: PersistentVolume = storage.get(&pv_key).await.unwrap();

    assert_eq!(
        pv.spec.access_modes,
        vec![
            PersistentVolumeAccessMode::ReadWriteMany,
            PersistentVolumeAccessMode::ReadOnlyMany,
        ]
    );
}

#[tokio::test]
async fn test_honors_reclaim_policy_delete() {
    let storage = setup_test().await;

    // Create StorageClass with Delete reclaim policy
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("delete-policy"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };

    let sc_key = build_key("storageclasses", None, "delete-policy");
    storage.create(&sc_key, &sc).await.unwrap();

    // Create PVC
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("delete-pvc");
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
            volume_name: None,
            storage_class_name: Some("delete-policy".to_string()),
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
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "delete-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Run provisioner
    let controller = DynamicProvisionerController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify PV has Delete reclaim policy
    let pv_name = "pvc-default-delete-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: PersistentVolume = storage.get(&pv_key).await.unwrap();

    assert_eq!(
        pv.spec.persistent_volume_reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Delete)
    );
}

#[tokio::test]
async fn test_honors_reclaim_policy_retain() {
    let storage = setup_test().await;

    // Create StorageClass with Retain reclaim policy
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("retain-policy"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Retain),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };

    let sc_key = build_key("storageclasses", None, "retain-policy");
    storage.create(&sc_key, &sc).await.unwrap();

    // Create PVC
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("retain-pvc");
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
            volume_name: None,
            storage_class_name: Some("retain-policy".to_string()),
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
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "retain-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Run provisioner
    let controller = DynamicProvisionerController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify PV has Retain reclaim policy
    let pv_name = "pvc-default-retain-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: PersistentVolume = storage.get(&pv_key).await.unwrap();

    assert_eq!(
        pv.spec.persistent_volume_reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Retain)
    );
}

#[tokio::test]
async fn test_restores_pvc_from_snapshot() {
    let storage = setup_test().await;

    // Step 1: Create StorageClass
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };
    let sc_key = build_key("storageclasses", None, "fast");
    storage.create(&sc_key, &sc).await.unwrap();

    // Step 2: Create VolumeSnapshotClass
    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("hostpath-snapclass"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Delete,
    };
    let vsc_key = build_key("volumesnapshotclasses", None, "hostpath-snapclass");
    storage.create(&vsc_key, &vsc).await.unwrap();

    // Step 3: Create VolumeSnapshotContent (simulating a completed snapshot)
    let content = VolumeSnapshotContent {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotContent".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("snapcontent-default-test-snapshot");
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta.resource_version = Some("1".to_string());
            meta
        },
        spec: VolumeSnapshotContentSpec {
            source: rusternetes_common::resources::volume::VolumeSnapshotContentSource {
                volume_handle: Some("original-pv".to_string()),
                snapshot_handle: None,
            },
            volume_snapshot_ref: rusternetes_common::resources::service_account::ObjectReference {
                kind: Some("VolumeSnapshot".to_string()),
                namespace: Some("default".to_string()),
                name: Some("test-snapshot".to_string()),
                uid: Some(uuid::Uuid::new_v4().to_string()),
                api_version: Some("snapshot.storage.k8s.io/v1".to_string()),
                resource_version: Some("1".to_string()),
                field_path: None,
            },
            volume_snapshot_class_name: "hostpath-snapclass".to_string(),
            deletion_policy: DeletionPolicy::Delete,
            driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        },
        status: Some(VolumeSnapshotContentStatus {
            snapshot_handle: Some("snapshot-handle-12345".to_string()),
            creation_time: Some(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)),
            ready_to_use: Some(true),
            restore_size: Some(5_000_000_000), // 5GB
            error: None,
        }),
    };
    let content_key = build_key(
        "volumesnapshotcontents",
        None,
        "snapcontent-default-test-snapshot",
    );
    storage.create(&content_key, &content).await.unwrap();

    // Step 4: Create VolumeSnapshot (ready to use)
    let snapshot = VolumeSnapshot {
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
            source: rusternetes_common::resources::volume::VolumeSnapshotSource {
                persistent_volume_claim_name: Some("original-pvc".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: "hostpath-snapclass".to_string(),
        },
        status: Some(VolumeSnapshotStatus {
            bound_volume_snapshot_content_name: Some(
                "snapcontent-default-test-snapshot".to_string(),
            ),
            creation_time: Some(chrono::Utc::now().to_rfc3339()),
            ready_to_use: Some(true),
            restore_size: Some("5Gi".to_string()),
            error: None,
        }),
    };
    let snapshot_key = build_key("volumesnapshots", Some("default"), "test-snapshot");
    storage.create(&snapshot_key, &snapshot).await.unwrap();

    // Step 5: Create PVC with dataSource pointing to the snapshot
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("restored-pvc");
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
            volume_name: None,
            storage_class_name: Some("fast".to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            selector: None,
            data_source: Some(TypedLocalObjectReference {
                api_group: Some("snapshot.storage.k8s.io".to_string()),
                kind: "VolumeSnapshot".to_string(),
                name: "test-snapshot".to_string(),
            }),
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeClaimStatus {
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "restored-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Step 6: Run provisioner
    let controller = DynamicProvisionerController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Step 7: Verify PV was created with snapshot restoration
    let pv_name = "pvc-default-restored-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: Result<PersistentVolume, _> = storage.get(&pv_key).await;

    assert!(
        pv.is_ok(),
        "PV should be created for PVC with snapshot dataSource"
    );
    let pv = pv.unwrap();

    // Verify PV was provisioned correctly
    assert_eq!(pv.spec.storage_class_name, Some("fast".to_string()));
    assert_eq!(pv.spec.capacity.get("storage"), Some(&"5Gi".to_string()));

    // Verify status message indicates snapshot restore
    assert!(pv
        .status
        .as_ref()
        .unwrap()
        .message
        .as_ref()
        .unwrap()
        .contains("snapshot"));
}

#[tokio::test]
async fn test_rejects_pvc_restore_from_non_ready_snapshot() {
    let storage = setup_test().await;

    // Step 1: Create StorageClass
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };
    let sc_key = build_key("storageclasses", None, "fast");
    storage.create(&sc_key, &sc).await.unwrap();

    // Step 2: Create VolumeSnapshot (NOT ready to use)
    let snapshot = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("not-ready-snapshot");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: rusternetes_common::resources::volume::VolumeSnapshotSource {
                persistent_volume_claim_name: Some("original-pvc".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: "hostpath-snapclass".to_string(),
        },
        status: Some(VolumeSnapshotStatus {
            bound_volume_snapshot_content_name: None,
            creation_time: None,
            ready_to_use: Some(false), // NOT READY
            restore_size: None,
            error: None,
        }),
    };
    let snapshot_key = build_key("volumesnapshots", Some("default"), "not-ready-snapshot");
    storage.create(&snapshot_key, &snapshot).await.unwrap();

    // Step 3: Create PVC with dataSource pointing to the not-ready snapshot
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("fail-restored-pvc");
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
            volume_name: None,
            storage_class_name: Some("fast".to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            selector: None,
            data_source: Some(TypedLocalObjectReference {
                api_group: Some("snapshot.storage.k8s.io".to_string()),
                kind: "VolumeSnapshot".to_string(),
                name: "not-ready-snapshot".to_string(),
            }),
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeClaimStatus {
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key(
        "persistentvolumeclaims",
        Some("default"),
        "fail-restored-pvc",
    );
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Step 4: Run provisioner - should fail or skip
    let controller = DynamicProvisionerController::new(storage.clone());
    let _result = controller.reconcile_all().await;

    // The reconcile might succeed but not create the PV
    // Let's verify the PV was NOT created
    sleep(Duration::from_millis(500)).await;

    let pv_name = "pvc-default-fail-restored-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: Result<PersistentVolume, _> = storage.get(&pv_key).await;

    assert!(
        pv.is_err(),
        "PV should not be created for PVC with non-ready snapshot"
    );
}
