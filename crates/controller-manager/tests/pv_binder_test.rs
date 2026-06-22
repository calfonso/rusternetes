// Integration tests for PV Binder Controller
// Note: These tests use in-memory storage for fast, isolated testing

use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::*;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::pv_binder::PVBinderController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());

    // Clean up test data (memory storage starts fresh)
    storage.clear();

    storage
}

async fn create_test_pv(
    storage: &MemoryStorage,
    name: &str,
    storage_class: Option<String>,
    capacity_gb: u32,
) -> PersistentVolume {
    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), format!("{}Gi", capacity_gb));

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
            storage_class_name: storage_class,
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

async fn create_test_pvc(
    storage: &MemoryStorage,
    name: &str,
    namespace: &str,
    storage_class: Option<String>,
    capacity_gb: u32,
) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), format!("{}Gi", capacity_gb));

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
            volume_name: None,
            storage_class_name: storage_class,
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

    let key = build_key("persistentvolumeclaims", Some(namespace), name);
    storage.create(&key, &pvc).await.unwrap()
}

#[tokio::test]
async fn test_binds_matching_pv_to_pvc() {
    let storage = setup_test().await;

    // Create available PV
    let _pv = create_test_pv(&storage, "test-pv-1", Some("fast".to_string()), 10).await;

    // Create pending PVC
    let _pvc = create_test_pvc(
        &storage,
        "test-pvc-1",
        "default",
        Some("fast".to_string()),
        5,
    )
    .await;

    // Run binder
    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify binding
    let pv_key = build_key("persistentvolumes", None, "test-pv-1");
    let updated_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "test-pvc-1");
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();

    // Check PV is bound to PVC
    assert!(
        updated_pv.spec.claim_ref.is_some(),
        "PV should have claim_ref set"
    );
    let claim_ref = updated_pv.spec.claim_ref.as_ref().unwrap();
    assert_eq!(claim_ref.name.as_deref().unwrap(), "test-pvc-1");
    assert_eq!(claim_ref.namespace.as_deref().unwrap(), "default");
    assert_eq!(
        updated_pv.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Bound
    );

    // Check PVC is bound to PV
    assert_eq!(updated_pvc.spec.volume_name, Some("test-pv-1".to_string()));
    assert_eq!(
        updated_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound
    );
}

#[tokio::test]
async fn test_matches_storage_class() {
    let storage = setup_test().await;

    // Create PV with storage class "fast"
    let _pv_fast = create_test_pv(&storage, "pv-fast", Some("fast".to_string()), 10).await;

    // Create PV with storage class "slow"
    let _pv_slow = create_test_pv(&storage, "pv-slow", Some("slow".to_string()), 10).await;

    // Create PVC requesting "fast" storage class
    let _pvc = create_test_pvc(&storage, "pvc-fast", "default", Some("fast".to_string()), 5).await;

    // Run binder
    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify PVC bound to "fast" PV, not "slow"
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "pvc-fast");
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();

    assert_eq!(updated_pvc.spec.volume_name, Some("pv-fast".to_string()));

    // Verify slow PV is still available
    let pv_slow_key = build_key("persistentvolumes", None, "pv-slow");
    let pv_slow: PersistentVolume = storage.get(&pv_slow_key).await.unwrap();
    assert_eq!(
        pv_slow.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Available
    );
}

#[tokio::test]
async fn test_matches_capacity_sufficient() {
    let storage = setup_test().await;

    // Create PV with 10Gi capacity
    let _pv = create_test_pv(&storage, "big-pv", Some("fast".to_string()), 10).await;

    // Create PVC requesting 5Gi (PV has sufficient capacity)
    let _pvc = create_test_pvc(
        &storage,
        "small-pvc",
        "default",
        Some("fast".to_string()),
        5,
    )
    .await;

    // Run binder
    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify binding succeeded
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "small-pvc");
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();

    assert_eq!(updated_pvc.spec.volume_name, Some("big-pv".to_string()));
    assert_eq!(
        updated_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound
    );
}

#[tokio::test]
async fn test_matches_capacity_insufficient() {
    let storage = setup_test().await;

    // Create PV with 5Gi capacity
    let _pv = create_test_pv(&storage, "small-pv", Some("fast".to_string()), 5).await;

    // Create PVC requesting 10Gi (PV has insufficient capacity)
    let _pvc = create_test_pvc(&storage, "big-pvc", "default", Some("fast".to_string()), 10).await;

    // Run binder
    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify binding did NOT succeed
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "big-pvc");
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();

    assert_eq!(updated_pvc.spec.volume_name, None);
    assert_eq!(
        updated_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending
    );
}

#[tokio::test]
async fn test_matches_access_modes() {
    let storage = setup_test().await;

    // Create PV with ReadWriteOnce access mode
    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), "10Gi".to_string());

    let pv = PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("rwo-pv");
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeSpec {
            capacity,
            host_path: Some(HostPathVolumeSource {
                path: "/tmp/test-pv/rwo-pv".to_string(),
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

    let pv_key = build_key("persistentvolumes", None, "rwo-pv");
    storage.create(&pv_key, &pv).await.unwrap();

    // Create PVC requesting ReadWriteMany (incompatible)
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
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteMany],
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

    // Run binder
    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify binding did NOT succeed (access mode mismatch)
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();

    assert_eq!(updated_pvc.spec.volume_name, None);
    assert_eq!(
        updated_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending
    );
}

#[tokio::test]
async fn test_skips_already_bound_pv() {
    let storage = setup_test().await;

    // Create PV that's already bound
    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), "10Gi".to_string());

    let pv = PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("bound-pv");
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeSpec {
            capacity,
            host_path: Some(HostPathVolumeSource {
                path: "/tmp/test-pv/bound-pv".to_string(),
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
            claim_ref: Some(ObjectReference {
                kind: Some("PersistentVolumeClaim".to_string()),
                namespace: Some("default".to_string()),
                name: Some("existing-claim".to_string()),
                uid: Some("existing-uid".to_string()),
                api_version: Some("v1".to_string()),
                resource_version: None,
                field_path: None,
            }),
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Bound,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        }),
    };

    let pv_key = build_key("persistentvolumes", None, "bound-pv");
    storage.create(&pv_key, &pv).await.unwrap();

    // The claim the PV is bound to must actually exist (with the matching UID),
    // otherwise the binder's release pass would correctly treat the PV as
    // dangling. Create it, bound to `bound-pv`.
    let bound_claim = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("existing-claim");
            meta.namespace = Some("default".to_string());
            meta.uid = "existing-uid".to_string();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                limits: None,
                requests: Some({
                    let mut r = HashMap::new();
                    r.insert("storage".to_string(), "10Gi".to_string());
                    r
                }),
            },
            volume_name: Some("bound-pv".to_string()),
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
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };
    storage
        .create(
            &build_key("persistentvolumeclaims", Some("default"), "existing-claim"),
            &bound_claim,
        )
        .await
        .unwrap();

    // Create new PVC
    let _pvc = create_test_pvc(&storage, "new-pvc", "default", Some("fast".to_string()), 5).await;

    // Run binder
    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify new PVC did NOT bind to already-bound PV
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "new-pvc");
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();

    assert_eq!(updated_pvc.spec.volume_name, None);
    assert_eq!(
        updated_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending
    );

    // Verify PV still bound to original claim
    let updated_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    assert_eq!(
        updated_pv
            .spec
            .claim_ref
            .as_ref()
            .unwrap()
            .name
            .as_deref()
            .unwrap(),
        "existing-claim"
    );
}

#[tokio::test]
async fn test_skips_already_bound_pvc() {
    let storage = setup_test().await;

    // Create two PVs
    let _pv1 = create_test_pv(&storage, "pv-1", Some("fast".to_string()), 10).await;
    let _pv2 = create_test_pv(&storage, "pv-2", Some("fast".to_string()), 10).await;

    // Create PVC already bound to pv-1
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
            volume_name: Some("pv-1".to_string()), // Already bound
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

    // Run binder
    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify PVC still bound to pv-1 (not re-bound to pv-2)
    let updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(updated_pvc.spec.volume_name, Some("pv-1".to_string()));

    // Verify pv-2 is still available
    let pv2_key = build_key("persistentvolumes", None, "pv-2");
    let pv2: PersistentVolume = storage.get(&pv2_key).await.unwrap();
    assert_eq!(
        pv2.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Available
    );
}
