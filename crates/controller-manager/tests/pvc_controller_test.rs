//! RED-state coverage for the PersistentVolumeClaim controller.
//!
//! This file ships alongside the `PvcController` stub in
//! `crates/controller-manager/src/controllers/pvc.rs`. Every test below is
//! marked `#[ignore]` with the reason `"RED-state: PvcController is a stub"`
//! so the suite stays green in CI while we incrementally implement the
//! controller.
//!
//! The behaviour pinned here is sourced from the upstream Kubernetes e2e
//! suite:
//! <https://github.com/kubernetes/kubernetes/blob/master/test/e2e/storage/persistent_volumes_claim.go>
//!
//! As real behaviour lands in `PvcController`, drop the `#[ignore]` attribute
//! on the corresponding test (one at a time) and watch it go GREEN.

use rusternetes_common::resources::volume::{
    HostPathType, HostPathVolumeSource, PersistentVolumeAccessMode, PersistentVolumeClaimPhase,
    PersistentVolumeClaimSpec, PersistentVolumeClaimStatus, PersistentVolumeMode,
    PersistentVolumePhase, PersistentVolumeReclaimPolicy, PersistentVolumeSpec,
    PersistentVolumeStatus, ResourceRequirements, TypedLocalObjectReference, VolumeBindingMode,
};
use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim, StorageClass};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::pvc::PvcController;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

async fn create_test_storage_class(
    storage: &Arc<MemoryStorage>,
    name: &str,
    binding_mode: Option<VolumeBindingMode>,
    is_default: bool,
    allow_expansion: bool,
) -> StorageClass {
    let mut meta = ObjectMeta::new(name);
    if is_default {
        let mut annotations = HashMap::new();
        annotations.insert(
            "storageclass.kubernetes.io/is-default-class".to_string(),
            "true".to_string(),
        );
        meta.annotations = Some(annotations);
    }

    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: meta,
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: binding_mode,
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
    storage_class: Option<String>,
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
    storage: &Arc<MemoryStorage>,
    name: &str,
    namespace: &str,
    storage_class: Option<String>,
    capacity_gi: u32,
    data_source: Option<TypedLocalObjectReference>,
) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), format!("{}Gi", capacity_gi));

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
            data_source,
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

/// Upstream: persistent_volumes_claim.go — "should provision storage with non-default volume binding mode".
///
/// PVCs against a StorageClass with `volumeBindingMode: Immediate` must bind
/// as soon as a matching PV is available. PVCs against a StorageClass with
/// `volumeBindingMode: WaitForFirstConsumer` must remain `Pending` until a
/// pod referencing the PVC is scheduled.
#[tokio::test]
#[ignore = "RED-state: PvcController is a stub"]
async fn pvc_binding_modes_immediate_vs_wait_for_first_consumer() {
    let storage = setup_test().await;

    let _sc_immediate = create_test_storage_class(
        &storage,
        "sc-immediate",
        Some(VolumeBindingMode::Immediate),
        false,
        false,
    )
    .await;
    let _sc_wffc = create_test_storage_class(
        &storage,
        "sc-wffc",
        Some(VolumeBindingMode::WaitForFirstConsumer),
        false,
        false,
    )
    .await;

    let _pv_imm = create_test_pv(
        &storage,
        "pv-immediate",
        Some("sc-immediate".to_string()),
        10,
    )
    .await;
    let _pv_wffc = create_test_pv(&storage, "pv-wffc", Some("sc-wffc".to_string()), 10).await;

    let _pvc_imm = create_test_pvc(
        &storage,
        "pvc-immediate",
        "default",
        Some("sc-immediate".to_string()),
        5,
        None,
    )
    .await;
    let _pvc_wffc = create_test_pvc(
        &storage,
        "pvc-wffc",
        "default",
        Some("sc-wffc".to_string()),
        5,
        None,
    )
    .await;

    PvcController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();

    let pvc_imm: PersistentVolumeClaim = storage
        .get(&build_key(
            "persistentvolumeclaims",
            Some("default"),
            "pvc-immediate",
        ))
        .await
        .unwrap();
    assert_eq!(
        pvc_imm.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
        "Immediate-mode PVC should bind during reconcile",
    );

    let pvc_wffc: PersistentVolumeClaim = storage
        .get(&build_key(
            "persistentvolumeclaims",
            Some("default"),
            "pvc-wffc",
        ))
        .await
        .unwrap();
    assert_eq!(
        pvc_wffc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending,
        "WaitForFirstConsumer PVC must stay Pending until a pod is scheduled",
    );
    assert!(
        pvc_wffc.spec.volume_name.is_none(),
        "WaitForFirstConsumer PVC must not have a volumeName until a consumer schedules",
    );
}

// NOTE: "default StorageClass for a PVC with no storageClassName" is NOT a
// PvcController concern. Upstream assigns it at PVC CREATE via the
// `DefaultStorageClass` admission plugin, and rusternetes mirrors that in
// `admission::set_default_storage_class` (PVC create handler) — covered by
// `api-server/tests/admission_test.rs`
// (`test_default_storage_class_{no_default,sets_default,already_set,beta_annotation}`).
// Binding a PVC to a PV of the matching class is the PV binder's job, covered by
// `pv_binder_test.rs::test_matches_storage_class`. The previous `#[ignore]`d
// controller test asserted this at the wrong layer (it would never flip green
// via PvcController) and has been removed. Remaining genuine PvcController gaps
// are tracked in indyjonesnl/rusternetes#1458.

/// Upstream: persistent_volumes_claim.go — "should allow expansion of an existing volume".
///
/// When a PVC's requested storage is increased and its StorageClass allows
/// expansion, the controller must record the new size in
/// `status.allocatedResources` / `status.capacity` and (eventually) leave the
/// PVC in `Bound`.
#[tokio::test]
#[ignore = "RED-state: PvcController is a stub"]
async fn pvc_resize_operation_online_expansion() {
    let storage = setup_test().await;

    let _sc = create_test_storage_class(
        &storage,
        "expandable",
        Some(VolumeBindingMode::Immediate),
        false,
        /*allow_expansion*/ true,
    )
    .await;

    let _pv = create_test_pv(&storage, "pv-grow", Some("expandable".to_string()), 20).await;

    // Pre-bound 5Gi PVC.
    let mut pvc = create_test_pvc(
        &storage,
        "pvc-grow",
        "default",
        Some("expandable".to_string()),
        5,
        None,
    )
    .await;
    pvc.spec.volume_name = Some("pv-grow".to_string());
    pvc.status = Some(PersistentVolumeClaimStatus {
        phase: PersistentVolumeClaimPhase::Bound,
        access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
        capacity: Some({
            let mut c = HashMap::new();
            c.insert("storage".to_string(), "5Gi".to_string());
            c
        }),
        conditions: None,
        allocated_resources: None,
        allocated_resource_statuses: None,
        resize_status: None,
        current_volume_attributes_class_name: None,
        modify_volume_status: None,
    });
    // Bump request to 10Gi -> triggers expansion.
    let mut new_requests = HashMap::new();
    new_requests.insert("storage".to_string(), "10Gi".to_string());
    pvc.spec.resources.requests = Some(new_requests);

    let key = build_key("persistentvolumeclaims", Some("default"), "pvc-grow");
    storage.update(&key, &pvc).await.unwrap();

    PvcController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();

    let resized: PersistentVolumeClaim = storage.get(&key).await.unwrap();
    let status = resized.status.as_ref().unwrap();
    assert_eq!(
        status.phase,
        PersistentVolumeClaimPhase::Bound,
        "expanding PVC stays Bound",
    );
    let allocated = status
        .allocated_resources
        .as_ref()
        .expect("allocatedResources must be populated during resize");
    assert_eq!(
        allocated.get("storage").map(String::as_str),
        Some("10Gi"),
        "allocatedResources must reflect the new requested size",
    );
    let cap = status
        .capacity
        .as_ref()
        .expect("status.capacity must be populated post-resize");
    assert_eq!(
        cap.get("storage").map(String::as_str),
        Some("10Gi"),
        "status.capacity must be updated to the new size after expansion completes",
    );
}

/// Upstream: persistent_volumes_claim.go — "should clone a volume from an existing PVC".
///
/// A PVC whose `dataSource` references an existing PVC (with kind
/// `PersistentVolumeClaim`) must be cloned: the controller must provision a
/// new PV with the source PVC's content and leave the clone PVC `Bound`.
#[tokio::test]
#[ignore = "RED-state: PvcController is a stub"]
async fn pvc_clone_operation_from_existing_pvc_and_snapshot() {
    let storage = setup_test().await;

    let _sc = create_test_storage_class(
        &storage,
        "cloneable",
        Some(VolumeBindingMode::Immediate),
        false,
        false,
    )
    .await;
    let _source_pv = create_test_pv(&storage, "pv-source", Some("cloneable".to_string()), 10).await;

    // Source PVC, already Bound.
    let mut source = create_test_pvc(
        &storage,
        "pvc-source",
        "default",
        Some("cloneable".to_string()),
        5,
        None,
    )
    .await;
    source.spec.volume_name = Some("pv-source".to_string());
    source.status.as_mut().unwrap().phase = PersistentVolumeClaimPhase::Bound;
    storage
        .update(
            &build_key("persistentvolumeclaims", Some("default"), "pvc-source"),
            &source,
        )
        .await
        .unwrap();

    // Clone PVC pointing at the source.
    let pvc_clone_ds = TypedLocalObjectReference {
        api_group: None,
        kind: "PersistentVolumeClaim".to_string(),
        name: "pvc-source".to_string(),
    };
    let _pvc_clone = create_test_pvc(
        &storage,
        "pvc-clone",
        "default",
        Some("cloneable".to_string()),
        5,
        Some(pvc_clone_ds),
    )
    .await;

    // Clone-from-snapshot PVC.
    let snap_ds = TypedLocalObjectReference {
        api_group: Some("snapshot.storage.k8s.io".to_string()),
        kind: "VolumeSnapshot".to_string(),
        name: "snap-1".to_string(),
    };
    let _pvc_from_snap = create_test_pvc(
        &storage,
        "pvc-from-snap",
        "default",
        Some("cloneable".to_string()),
        5,
        Some(snap_ds),
    )
    .await;

    PvcController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();

    let clone: PersistentVolumeClaim = storage
        .get(&build_key(
            "persistentvolumeclaims",
            Some("default"),
            "pvc-clone",
        ))
        .await
        .unwrap();
    assert_eq!(
        clone.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
        "PVC-to-PVC clone must end up Bound",
    );
    assert!(
        clone.spec.volume_name.is_some(),
        "PVC-to-PVC clone must have a freshly-provisioned PV",
    );
    assert_ne!(
        clone.spec.volume_name.as_deref(),
        Some("pv-source"),
        "clone must not reuse the source PV - cloning provisions a new volume",
    );

    let from_snap: PersistentVolumeClaim = storage
        .get(&build_key(
            "persistentvolumeclaims",
            Some("default"),
            "pvc-from-snap",
        ))
        .await
        .unwrap();
    assert_eq!(
        from_snap.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
        "snapshot-cloned PVC must end up Bound",
    );
}

/// Upstream: persistent_volumes_claim.go — "should restore from a VolumeSnapshot".
///
/// A PVC whose `dataSource` references a VolumeSnapshot must trigger the
/// volume-populator workflow: status flips through the populator conditions
/// and lands `Bound` with capacity matching the snapshot's restored size.
#[tokio::test]
#[ignore = "RED-state: PvcController is a stub"]
async fn pvc_datasource_population_from_snapshot() {
    let storage = setup_test().await;

    let _sc = create_test_storage_class(
        &storage,
        "restorable",
        Some(VolumeBindingMode::Immediate),
        false,
        false,
    )
    .await;

    let datasource = TypedLocalObjectReference {
        api_group: Some("snapshot.storage.k8s.io".to_string()),
        kind: "VolumeSnapshot".to_string(),
        name: "snap-source".to_string(),
    };
    let _pvc = create_test_pvc(
        &storage,
        "pvc-restored",
        "default",
        Some("restorable".to_string()),
        5,
        Some(datasource),
    )
    .await;

    PvcController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();

    let restored: PersistentVolumeClaim = storage
        .get(&build_key(
            "persistentvolumeclaims",
            Some("default"),
            "pvc-restored",
        ))
        .await
        .unwrap();
    let status = restored.status.as_ref().unwrap();
    assert_eq!(
        status.phase,
        PersistentVolumeClaimPhase::Bound,
        "snapshot-populated PVC must end up Bound",
    );
    let cap = status
        .capacity
        .as_ref()
        .expect("status.capacity must be set on snapshot-populated PVC");
    assert_eq!(
        cap.get("storage").map(String::as_str),
        Some("5Gi"),
        "snapshot-populated PVC capacity must match the request",
    );
    assert!(
        restored.spec.volume_name.is_some(),
        "snapshot-populated PVC must reference a provisioned PV",
    );
}

/// Upstream: persistent_volumes_claim.go — "should fail if datasource references a non-existent resource".
///
/// A PVC whose `dataSource` references a non-existent object must NOT be
/// bound. The controller should surface this through `status.conditions`
/// (or an event), and leave the PVC `Pending`.
#[tokio::test]
#[ignore = "RED-state: PvcController is a stub"]
async fn pvc_datasource_population_missing_source_stays_pending() {
    let storage = setup_test().await;

    let _sc = create_test_storage_class(
        &storage,
        "restorable",
        Some(VolumeBindingMode::Immediate),
        false,
        false,
    )
    .await;

    let missing = TypedLocalObjectReference {
        api_group: Some("snapshot.storage.k8s.io".to_string()),
        kind: "VolumeSnapshot".to_string(),
        name: "does-not-exist".to_string(),
    };
    let _pvc = create_test_pvc(
        &storage,
        "pvc-orphan",
        "default",
        Some("restorable".to_string()),
        5,
        Some(missing),
    )
    .await;

    PvcController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();

    let pvc: PersistentVolumeClaim = storage
        .get(&build_key(
            "persistentvolumeclaims",
            Some("default"),
            "pvc-orphan",
        ))
        .await
        .unwrap();
    assert_eq!(
        pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending,
        "PVC with missing dataSource must stay Pending",
    );
    assert!(
        pvc.spec.volume_name.is_none(),
        "PVC with missing dataSource must not be bound to a PV",
    );
}
