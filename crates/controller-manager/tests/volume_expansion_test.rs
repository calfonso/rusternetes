use rusternetes_common::resources::volume::{
    HostPathType, HostPathVolumeSource, PersistentVolumeAccessMode, PersistentVolumeClaimPhase,
    PersistentVolumeMode, PersistentVolumePhase, PersistentVolumeReclaimPolicy,
    ResourceRequirements,
};
use rusternetes_common::resources::{
    PersistentVolume, PersistentVolumeClaim, PersistentVolumeClaimStatus, StorageClass,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::volume_expansion::VolumeExpansionController;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn setup_test() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

async fn create_test_storage_class(
    storage: &Arc<MemoryStorage>,
    name: &str,
    allow_expansion: bool,
) -> StorageClass {
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new(name),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: None,
        allowed_topologies: None,
        allow_volume_expansion: Some(allow_expansion),
        mount_options: None,
    };

    let key = build_key("storageclasses", None, name);
    storage.create(&key, &sc).await.unwrap();
    sc
}

async fn create_test_pv(
    storage: &Arc<MemoryStorage>,
    name: &str,
    storage_class: &str,
    capacity_gi: u32,
) -> PersistentVolume {
    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), format!("{}Gi", capacity_gi));

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
        spec: rusternetes_common::resources::PersistentVolumeSpec {
            capacity,
            host_path: Some(HostPathVolumeSource {
                path: format!("/tmp/test/{}", name),
                r#type: Some(HostPathType::DirectoryOrCreate),
            }),
            nfs: None,
            iscsi: None,
            local: None,
            csi: None,
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
            storage_class_name: Some(storage_class.to_string()),
            mount_options: None,
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            node_affinity: None,
            claim_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(rusternetes_common::resources::PersistentVolumeStatus {
            phase: PersistentVolumePhase::Available,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        }),
    };

    let key = build_key("persistentvolumes", None, name);
    storage.create(&key, &pv).await.unwrap();
    pv
}

async fn create_bound_pvc(
    storage: &Arc<MemoryStorage>,
    name: &str,
    namespace: &str,
    storage_class: &str,
    pv_name: &str,
    capacity_gi: u32,
) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), format!("{}Gi", capacity_gi));

    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), format!("{}Gi", capacity_gi));

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
        spec: rusternetes_common::resources::PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                requests: Some(requests),
                limits: None,
            },
            volume_name: Some(pv_name.to_string()),
            storage_class_name: Some(storage_class.to_string()),
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
            capacity: Some(capacity),
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let key = build_key("persistentvolumeclaims", Some(namespace), name);
    storage.create(&key, &pvc).await.unwrap();
    pvc
}

#[tokio::test]
async fn test_volume_expansion_allowed() {
    let storage = setup_test();

    // Create StorageClass with expansion enabled
    create_test_storage_class(&storage, "expandable", true).await;

    // Create PV with 5Gi
    let _pv = create_test_pv(&storage, "test-pv", "expandable", 5).await;

    // Create bound PVC with 5Gi
    let mut pvc =
        create_bound_pvc(&storage, "test-pvc", "default", "expandable", "test-pv", 5).await;

    // Update PVC to request 10Gi (expansion)
    pvc.spec
        .resources
        .requests
        .as_mut()
        .unwrap()
        .insert("storage".to_string(), "10Gi".to_string());

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "test-pvc");
    storage.update(&pvc_key, &pvc).await.unwrap();

    // Run expansion controller
    let controller = VolumeExpansionController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify PVC was expanded
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(
        updated_pvc
            .status
            .as_ref()
            .unwrap()
            .capacity
            .as_ref()
            .unwrap()
            .get("storage"),
        Some(&"10Gi".to_string())
    );
    assert_eq!(
        updated_pvc
            .status
            .as_ref()
            .unwrap()
            .allocated_resources
            .as_ref()
            .unwrap()
            .get("storage"),
        Some(&"10Gi".to_string())
    );
    assert_eq!(updated_pvc.status.as_ref().unwrap().resize_status, None); // Completed

    // Verify PV was expanded
    let pv_key = build_key("persistentvolumes", None, "test-pv");
    let updated_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    assert_eq!(
        updated_pv.spec.capacity.get("storage"),
        Some(&"10Gi".to_string())
    );
}

#[tokio::test]
async fn test_volume_expansion_not_allowed() {
    let storage = setup_test();

    // Create StorageClass with expansion disabled
    create_test_storage_class(&storage, "non-expandable", false).await;

    // Create PV with 5Gi
    create_test_pv(&storage, "test-pv-2", "non-expandable", 5).await;

    // Create bound PVC with 5Gi
    let mut pvc = create_bound_pvc(
        &storage,
        "test-pvc-2",
        "default",
        "non-expandable",
        "test-pv-2",
        5,
    )
    .await;

    // Update PVC to request 10Gi
    pvc.spec
        .resources
        .requests
        .as_mut()
        .unwrap()
        .insert("storage".to_string(), "10Gi".to_string());

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "test-pvc-2");
    storage.update(&pvc_key, &pvc).await.unwrap();

    // Run expansion controller
    let controller = VolumeExpansionController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify PVC was NOT expanded (capacity remains 5Gi)
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(
        updated_pvc
            .status
            .as_ref()
            .unwrap()
            .capacity
            .as_ref()
            .unwrap()
            .get("storage"),
        Some(&"5Gi".to_string())
    );

    // Verify PV was NOT expanded
    let pv_key = build_key("persistentvolumes", None, "test-pv-2");
    let updated_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    assert_eq!(
        updated_pv.spec.capacity.get("storage"),
        Some(&"5Gi".to_string())
    );
}

#[tokio::test]
async fn test_expansion_only_for_bound_pvcs() {
    let storage = setup_test();

    // Create StorageClass with expansion enabled
    create_test_storage_class(&storage, "expandable-2", true).await;

    // Create unbound PVC requesting 10Gi
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "10Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-pvc-unbound");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: rusternetes_common::resources::PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                requests: Some(requests),
                limits: None,
            },
            volume_name: None, // Not bound
            storage_class_name: Some("expandable-2".to_string()),
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
            phase: PersistentVolumeClaimPhase::Pending, // Not bound
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
        "test-pvc-unbound",
    );
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Run expansion controller
    let controller = VolumeExpansionController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify PVC was NOT expanded (still pending, no capacity)
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(
        updated_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending
    );
    assert!(updated_pvc.status.as_ref().unwrap().capacity.is_none());
}

#[tokio::test]
async fn test_no_expansion_when_sizes_equal() {
    let storage = setup_test();

    // Create StorageClass with expansion enabled
    create_test_storage_class(&storage, "expandable-3", true).await;

    // Create PV with 5Gi
    create_test_pv(&storage, "test-pv-3", "expandable-3", 5).await;

    // Create bound PVC with 5Gi (request and capacity match)
    let _pvc = create_bound_pvc(
        &storage,
        "test-pvc-3",
        "default",
        "expandable-3",
        "test-pv-3",
        5,
    )
    .await;

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "test-pvc-3");

    // Run expansion controller
    let controller = VolumeExpansionController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify PVC status hasn't changed (no expansion needed)
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(
        updated_pvc
            .status
            .as_ref()
            .unwrap()
            .capacity
            .as_ref()
            .unwrap()
            .get("storage"),
        Some(&"5Gi".to_string())
    );
    assert!(updated_pvc
        .status
        .as_ref()
        .unwrap()
        .allocated_resources
        .is_none());
    assert_eq!(updated_pvc.status.as_ref().unwrap().resize_status, None);
}
