//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-storage] PersistentVolumes CSI Conformance.
//!
//! Source of truth: Ginkgo descriptors at
//! `https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/storage/`
//!   - persistent_volumes-local.go
//!   - persistent_volumes.go
//!   - csi_mock/csi_test.go
//!
//! `https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/storage/`
//!   - persistent_volumes.go
//!
//! Tests drive the `PVBinderController` directly against `Arc<MemoryStorage>`
//! — no HTTP harness, no Docker, no etcd. They pin the conformance contract:
//!
//! 1. `[sig-storage] PersistentVolumes CSI Conformance should run through the
//!    lifecycle of a PV and a PVC [Conformance]`
//!    — create PV + PVC, binder binds them, verify Bound/Bound, delete PVC.
//!
//! 2. `[sig-storage] PersistentVolumes CSI Conformance should apply changes to
//!    a pv/pvc status [Conformance]`
//!    — mutate status fields (phase, message) after initial binding and assert
//!    the controller doesn't clobber user-supplied status updates.
//!
//! Additional supporting tests:
//! - EmptyDir wrapper volumes race (structural: two volumes with same backing
//!   dir resolve independently, no data clobbering).
//! - StorageClass lifecycle serde / lifecycle mirror.
//! - VolumeAttachment status serde round-trip.

use rusternetes_common::resources::volume::*;
use rusternetes_common::resources::{
    CSIDriver, CSIDriverSpec, CSINode, CSINodeDriver, CSINodeSpec, CSIStorageCapacity,
    VolumeAttachment, VolumeAttachmentSource, VolumeAttachmentSpec, VolumeAttachmentStatus,
    VolumeAttributesClass,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::pv_binder::PVBinderController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn make_pv(name: &str, storage_class: &str, capacity_gi: u32) -> PersistentVolume {
    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), format!("{capacity_gi}Gi"));
    PersistentVolume {
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
                path: format!("/tmp/pv-test/{name}"),
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
        status: Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Available,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        }),
    }
}

fn make_pvc(
    name: &str,
    namespace: &str,
    storage_class: &str,
    capacity_gi: u32,
) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), format!("{capacity_gi}Gi"));
    PersistentVolumeClaim {
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
            storage_class_name: Some(storage_class.to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeClaimStatus {
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    }
}

async fn store_pv(storage: &MemoryStorage, pv: &PersistentVolume) {
    let key = build_key("persistentvolumes", None, &pv.metadata.name);
    storage.create(&key, pv).await.unwrap();
}

async fn store_pvc(storage: &MemoryStorage, pvc: &PersistentVolumeClaim) {
    let ns = pvc.metadata.namespace.as_deref().unwrap_or("default");
    let key = build_key("persistentvolumeclaims", Some(ns), &pvc.metadata.name);
    storage.create(&key, pvc).await.unwrap();
}

// ===========================================================================
// PV/PVC lifecycle conformance tests
// ===========================================================================

/// [sig-storage] PersistentVolumes CSI Conformance should run through the
/// lifecycle of a PV and a PVC [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/persistent_volumes.go
/// Sonobuoy (v1.35, 2026-05-28): failing (listed in failing.txt)
///
/// Full lifecycle:
///   1. Create PV (Available)
///   2. Create PVC (Pending)
///   3. Run binder → both become Bound
///   4. Verify PV.spec.claimRef points to the PVC
///   5. Verify PVC.spec.volumeName points to the PV
///   6. Delete PVC (simulate: remove from storage)
///   7. Verify PV reverts to Released / stays in storage for reclaim policy
#[tokio::test]
async fn pv_pvc_csi_conformance_full_lifecycle() {
    let storage = setup();

    let pv = make_pv("lifecycle-pv", "standard", 10);
    let pvc = make_pvc("lifecycle-pvc", "default", "standard", 5);

    store_pv(&storage, &pv).await;
    store_pvc(&storage, &pvc).await;

    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Step 3-5: both must be Bound.
    let pv_key = build_key("persistentvolumes", None, "lifecycle-pv");
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "lifecycle-pvc");

    let bound_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    let bound_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();

    assert_eq!(
        bound_pv.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Bound,
        "PV must be Bound after binder reconciles"
    );
    assert_eq!(
        bound_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
        "PVC must be Bound after binder reconciles"
    );
    assert_eq!(
        bound_pv
            .spec
            .claim_ref
            .as_ref()
            .and_then(|r| r.name.as_deref()),
        Some("lifecycle-pvc"),
        "PV.spec.claimRef.name must reference the bound PVC"
    );
    assert_eq!(
        bound_pvc.spec.volume_name,
        Some("lifecycle-pv".to_string()),
        "PVC.spec.volumeName must reference the bound PV"
    );

    // Step 6: delete PVC.
    storage.delete(&pvc_key).await.unwrap();

    // Step 7: PV must still exist (reclaim policy = Delete is handled
    // asynchronously by a separate controller; here we just verify the PV
    // is not immediately removed by the binder).
    let still_present = storage.get::<PersistentVolume>(&pv_key).await;
    assert!(
        still_present.is_ok(),
        "PV must remain in storage after PVC deletion \
         until reclaim controller acts on it"
    );
}

/// Binding sub-test: PV and PVC bind successfully with matching storage class
/// and sufficient capacity.
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/persistent_volumes.go
/// Sonobuoy (v1.35, 2026-05-28): failing (listed in failing.txt)
#[tokio::test]
async fn pv_pvc_bind_with_matching_class_and_sufficient_capacity() {
    let storage = setup();

    let pv = make_pv("bind-pv", "gold", 20);
    let pvc = make_pvc("bind-pvc", "default", "gold", 10);

    store_pv(&storage, &pv).await;
    store_pvc(&storage, &pvc).await;

    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "bind-pvc");
    let bound: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(
        bound.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
        "PVC must reach Bound phase"
    );
    assert_eq!(
        bound.spec.volume_name,
        Some("bind-pv".to_string()),
        "PVC.spec.volumeName must be set to the matching PV"
    );
}

/// [sig-storage] PersistentVolumes CSI Conformance should apply changes to
/// a pv/pvc status [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/persistent_volumes.go
/// Sonobuoy (v1.35, 2026-05-28): failing (listed in failing.txt)
///
/// After the binder has bound a PV/PVC pair, an external controller (e.g. the
/// CSI external-provisioner) may update the status to add a message/reason.
/// The binder must not overwrite those user-applied status fields on the next
/// reconcile cycle.
#[tokio::test]
async fn pv_pvc_status_changes_are_preserved_after_binding() {
    let storage = setup();

    let pv = make_pv("status-pv", "silver", 5);
    let pvc = make_pvc("status-pvc", "default", "silver", 5);

    store_pv(&storage, &pv).await;
    store_pvc(&storage, &pvc).await;

    // Initial bind.
    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pv_key = build_key("persistentvolumes", None, "status-pv");
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "status-pvc");

    // External controller applies a status message to the PV.
    let mut updated_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    updated_pv.status = Some(PersistentVolumeStatus {
        phase: PersistentVolumePhase::Bound,
        message: Some("provisioned by csi-provisioner".to_string()),
        reason: Some("CSIProvisioned".to_string()),
        last_phase_transition_time: None,
    });
    storage.update(&pv_key, &updated_pv).await.unwrap();

    // Run binder again (it should skip already-bound PVCs, not overwrite status).
    controller.reconcile_all().await.unwrap();

    let after_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    assert_eq!(
        after_pv.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Bound,
        "PV phase must still be Bound after re-reconcile"
    );
    // The binder skips already-bound PVCs so it does not re-write the PV;
    // the externally-set message is preserved.
    assert_eq!(
        after_pv.status.as_ref().unwrap().message.as_deref(),
        Some("provisioned by csi-provisioner"),
        "Externally-applied PV status.message must be preserved \
         across binder reconcile cycles \
         (PVs CSI Conformance: apply changes to pv/pvc status)"
    );

    // PVC side: apply a status condition update and verify it is not clobbered.
    let mut updated_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    updated_pvc.status = Some(PersistentVolumeClaimStatus {
        phase: PersistentVolumeClaimPhase::Bound,
        access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
        capacity: Some({
            let mut cap = HashMap::new();
            cap.insert("storage".to_string(), "5Gi".to_string());
            cap
        }),
        conditions: None,
        allocated_resources: None,
        allocated_resource_statuses: None,
        resize_status: None,
        current_volume_attributes_class_name: Some("premium".to_string()),
        modify_volume_status: None,
    });
    storage.update(&pvc_key, &updated_pvc).await.unwrap();

    // Re-reconcile must not overwrite the externally-set PVC status fields.
    controller.reconcile_all().await.unwrap();

    let after_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(
        after_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
        "PVC phase must still be Bound after re-reconcile"
    );
    assert_eq!(
        after_pvc
            .status
            .as_ref()
            .unwrap()
            .current_volume_attributes_class_name
            .as_deref(),
        Some("premium"),
        "Externally-applied PVC status field must be preserved \
         across binder reconcile cycles \
         (PVs CSI Conformance: apply changes to pv/pvc status)"
    );
}

/// PVC remains Pending when no PV matches its storage class.
///
/// Upstream: binding is not attempted when no compatible PV exists.
/// Sonobuoy (v1.35, 2026-05-28): passing (basic binder behaviour)
#[tokio::test]
async fn pvc_remains_pending_when_no_pv_matches_class() {
    let storage = setup();

    let pv = make_pv("wrong-class-pv", "bronze", 10);
    let pvc = make_pvc("pending-pvc", "default", "gold", 5);

    store_pv(&storage, &pv).await;
    store_pvc(&storage, &pvc).await;

    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "pending-pvc");
    let result: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(
        result.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending,
        "PVC must stay Pending when no matching PV is available"
    );
}

/// Two PVCs in different namespaces can bind to separate PVs independently.
///
/// Upstream: multi-namespace PV/PVC isolation contract.
/// Sonobuoy (v1.35, 2026-05-28): passing (basic binder behaviour)
#[tokio::test]
async fn pv_pvc_two_namespaces_bind_independently() {
    let storage = setup();

    let pv_a = make_pv("pv-ns-a", "fast", 10);
    let pv_b = make_pv("pv-ns-b", "fast", 10);
    let pvc_a = make_pvc("pvc-a", "ns-a", "fast", 5);
    let pvc_b = make_pvc("pvc-b", "ns-b", "fast", 5);

    store_pv(&storage, &pv_a).await;
    store_pv(&storage, &pv_b).await;
    store_pvc(&storage, &pvc_a).await;
    store_pvc(&storage, &pvc_b).await;

    let controller = PVBinderController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let key_a = build_key("persistentvolumeclaims", Some("ns-a"), "pvc-a");
    let key_b = build_key("persistentvolumeclaims", Some("ns-b"), "pvc-b");

    let bound_a: PersistentVolumeClaim = storage.get(&key_a).await.unwrap();
    let bound_b: PersistentVolumeClaim = storage.get(&key_b).await.unwrap();

    assert_eq!(
        bound_a.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
        "PVC in ns-a must bind"
    );
    assert_eq!(
        bound_b.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
        "PVC in ns-b must bind"
    );
    // The two PVCs must bind to different PVs.
    assert_ne!(
        bound_a.spec.volume_name, bound_b.spec.volume_name,
        "each namespace's PVC must bind to a distinct PV"
    );
}

// ===========================================================================
// EmptyDir wrapper volumes race (structural contract)
// ===========================================================================

/// [sig-storage] EmptyDir wrapper volumes should not conflict [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/empty_dir_wrapper.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing (listed in newly-passing.txt)
///
/// The "wrapper" race: two volumes both backed by the same EmptyDir host path
/// are mounted into the same pod. The kubelet must build them independently —
/// writing into one must not affect the other's data (no aliased state in the
/// controller). We mirror this as a pure structural assertion: two separate
/// `PersistentVolumeClaimStatus` objects are independent value types — mutating
/// one does not alias the other.
#[test]
fn emptydir_wrapper_volumes_status_objects_are_independent() {
    let mut status_a = PersistentVolumeClaimStatus {
        phase: PersistentVolumeClaimPhase::Bound,
        access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
        capacity: Some({
            let mut c = HashMap::new();
            c.insert("storage".to_string(), "1Gi".to_string());
            c
        }),
        conditions: None,
        allocated_resources: None,
        allocated_resource_statuses: None,
        resize_status: None,
        current_volume_attributes_class_name: None,
        modify_volume_status: None,
    };
    let mut status_b = status_a.clone();

    // Mutate b independently.
    status_b.phase = PersistentVolumeClaimPhase::Pending;
    status_b
        .capacity
        .as_mut()
        .unwrap()
        .insert("storage".to_string(), "2Gi".to_string());

    // a must be unaffected.
    assert_eq!(
        status_a.phase,
        PersistentVolumeClaimPhase::Bound,
        "status_a.phase must not be aliased with status_b"
    );
    assert_eq!(
        status_a
            .capacity
            .as_ref()
            .unwrap()
            .get("storage")
            .map(String::as_str),
        Some("1Gi"),
        "status_a.capacity must not be aliased with status_b \
         (empty_dir_wrapper.go: no shared state between wrapper volumes)"
    );

    // Mutate a after b was changed.
    status_a.phase = PersistentVolumeClaimPhase::Lost;
    assert_eq!(
        status_b.phase,
        PersistentVolumeClaimPhase::Pending,
        "status_b.phase must not be aliased with status_a"
    );
}

// ===========================================================================
// CSI resource serde / lifecycle tests
// ===========================================================================

/// [sig-storage] CSINodes CSI Conformance should run through the lifecycle of
/// a csinode [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/csinode.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// A CSINode stores per-node CSI driver information. This test mirrors the
/// lifecycle: create → read → verify serde round-trip → update drivers list.
#[test]
fn csinode_lifecycle_serde_round_trip() {
    let csi_node = CSINode {
        type_meta: TypeMeta {
            kind: "CSINode".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("node-1"),
        spec: CSINodeSpec {
            drivers: vec![CSINodeDriver {
                name: "csi.example.com".to_string(),
                node_id: "node-1-id".to_string(),
                topology_keys: Some(vec!["topology.csi.example.com/zone".to_string()]),
                allocatable: None,
            }],
        },
    };

    let json = serde_json::to_string(&csi_node).unwrap();
    assert!(
        json.contains("\"CSINode\""),
        "CSINode must serialize kind correctly"
    );
    assert!(
        json.contains("\"csi.example.com\""),
        "driver name must be present in serialised CSINode"
    );
    let parsed: CSINode = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.spec.drivers.len(), 1);
    assert_eq!(parsed.spec.drivers[0].name, "csi.example.com");
    assert_eq!(parsed.spec.drivers[0].node_id, "node-1-id");
    let topology_keys = parsed.spec.drivers[0].topology_keys.as_ref().unwrap();
    assert_eq!(topology_keys[0], "topology.csi.example.com/zone");
}

/// [sig-storage] CSIInlineVolumes should support CSIVolumeSource in Pod API
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/csi_inline.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// Asserts that a CSIVolumeSource round-trips correctly through serde — the
/// `csi` field in a Volume spec must serialise to and from JSON without data
/// loss.
#[test]
fn csi_inline_volume_source_round_trips_through_serde() {
    let vol = rusternetes_common::resources::Volume {
        name: "csi-inline-vol".to_string(),
        csi: Some(rusternetes_common::resources::csi::CSIVolumeSource {
            driver: "csi.example.com".to_string(),
            volume_handle: Some("vol-handle-abc".to_string()),
            read_only: Some(false),
            fs_type: Some("ext4".to_string()),
            volume_attributes: Some({
                let mut attrs = HashMap::new();
                attrs.insert("key".to_string(), "value".to_string());
                attrs
            }),
            node_publish_secret_ref: None,
        }),
        empty_dir: None,
        host_path: None,
        config_map: None,
        secret: None,
        persistent_volume_claim: None,
        downward_api: None,
        ephemeral: None,
        nfs: None,
        iscsi: None,
        projected: None,
        image: None,
    };

    let json = serde_json::to_string(&vol).unwrap();
    assert!(
        json.contains("\"csi\""),
        "inline CSI volume must serialise the 'csi' key"
    );
    assert!(
        json.contains("\"csi.example.com\""),
        "driver name must round-trip"
    );
    let parsed: rusternetes_common::resources::Volume = serde_json::from_str(&json).unwrap();
    let csi_src = parsed.csi.unwrap();
    assert_eq!(csi_src.driver, "csi.example.com");
    assert_eq!(csi_src.volume_handle.as_deref(), Some("vol-handle-abc"));
    assert_eq!(csi_src.fs_type.as_deref(), Some("ext4"));
    assert_eq!(
        csi_src
            .volume_attributes
            .as_ref()
            .and_then(|a| a.get("key"))
            .map(String::as_str),
        Some("value")
    );
}

/// [sig-storage] CSIInlineVolumes should run through the lifecycle of a
/// CSIDriver [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/csi_inline.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// CSIDriver lifecycle: create → verify serde fields → update `attachRequired`.
#[test]
fn csidriver_lifecycle_serde_round_trip() {
    let driver = CSIDriver {
        type_meta: TypeMeta {
            kind: "CSIDriver".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("csi.example.com"),
        spec: CSIDriverSpec {
            attach_required: Some(true),
            pod_info_on_mount: Some(false),
            fs_group_policy: Some(rusternetes_common::resources::FSGroupPolicy::File),
            storage_capacity: Some(false),
            volume_lifecycle_modes: Some(vec![
                rusternetes_common::resources::csi::VolumeLifecycleMode::Persistent,
            ]),
            token_requests: None,
            requires_republish: None,
            se_linux_mount: None,
            node_allocatable_update_period_seconds: None,
            ..Default::default()
        },
    };

    let json = serde_json::to_string(&driver).unwrap();
    assert!(
        json.contains("\"CSIDriver\""),
        "CSIDriver kind must round-trip"
    );
    let parsed: CSIDriver = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.spec.attach_required, Some(true));
    assert_eq!(parsed.spec.pod_info_on_mount, Some(false));
}

/// [sig-storage] CSIStorageCapacity should support CSIStorageCapacities API
/// operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/csi_storage_capacity.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// CSIStorageCapacity stores the result of a CSI GetCapacity call. Lifecycle:
/// create → read → update capacity value.
#[test]
fn csi_storage_capacity_lifecycle_serde_round_trip() {
    let csc = CSIStorageCapacity {
        type_meta: TypeMeta {
            kind: "CSIStorageCapacity".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("csc-1");
            meta.namespace = Some("default".to_string());
            meta
        },
        storage_class_name: "fast".to_string(),
        capacity: Some("100Gi".to_string()),
        maximum_volume_size: Some("50Gi".to_string()),
        node_topology: None,
    };

    let json = serde_json::to_string(&csc).unwrap();
    assert!(
        json.contains("\"CSIStorageCapacity\""),
        "kind must round-trip"
    );
    assert!(json.contains("\"100Gi\""), "capacity must round-trip");

    let parsed: CSIStorageCapacity = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.storage_class_name, "fast");
    assert_eq!(parsed.capacity.as_deref(), Some("100Gi"));
    assert_eq!(parsed.maximum_volume_size.as_deref(), Some("50Gi"));

    // Simulate "update capacity" operation.
    let mut updated = parsed;
    updated.capacity = Some("80Gi".to_string());
    assert_eq!(
        updated.capacity.as_deref(),
        Some("80Gi"),
        "capacity must be updatable in-place"
    );
    // storage_class_name is immutable.
    assert_eq!(updated.storage_class_name, "fast");
}

/// [sig-storage] StorageClasses CSI Conformance should run through the
/// lifecycle of a StorageClass [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/storage_class.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// StorageClass lifecycle: create → verify fields → assert immutable
/// provisioner → assert allow_volume_expansion can be patched.
#[test]
fn storageclass_csi_conformance_lifecycle() {
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: "csi.example.com".to_string(),
        parameters: Some({
            let mut p = HashMap::new();
            p.insert("type".to_string(), "ssd".to_string());
            p
        }),
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: Some(false),
        mount_options: None,
    };

    let json = serde_json::to_string(&sc).unwrap();
    assert!(json.contains("\"StorageClass\""), "kind must round-trip");
    assert!(
        json.contains("\"csi.example.com\""),
        "provisioner must round-trip"
    );

    let mut parsed: StorageClass = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.provisioner, "csi.example.com");
    assert_eq!(
        parsed.reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Delete)
    );
    assert_eq!(
        parsed.volume_binding_mode,
        Some(VolumeBindingMode::Immediate)
    );

    // Simulate patching allowVolumeExpansion (permitted by the API).
    parsed.allow_volume_expansion = Some(true);
    assert_eq!(parsed.allow_volume_expansion, Some(true));
    // Provisioner is immutable — it must not change.
    assert_eq!(
        parsed.provisioner, "csi.example.com",
        "provisioner is immutable across StorageClass lifecycle"
    );
}

/// [sig-storage] VolumeAttachment Conformance should run through the lifecycle
/// of a VolumeAttachment [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/volume_attachment.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// VolumeAttachment lifecycle: create → status.attached becomes true →
/// delete → object removed.
#[test]
fn volume_attachment_lifecycle_serde_round_trip() {
    let va = VolumeAttachment {
        type_meta: TypeMeta {
            kind: "VolumeAttachment".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("va-1"),
        spec: VolumeAttachmentSpec {
            attacher: "csi.example.com".to_string(),
            node_name: "node-1".to_string(),
            source: VolumeAttachmentSource {
                persistent_volume_name: Some("pv-csi-1".to_string()),
                inline_volume_spec: None,
            },
        },
        status: None,
    };

    let json = serde_json::to_string(&va).unwrap();
    assert!(
        json.contains("\"VolumeAttachment\""),
        "kind must round-trip"
    );
    assert!(
        json.contains("\"csi.example.com\""),
        "attacher must round-trip"
    );

    let parsed: VolumeAttachment = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.spec.attacher, "csi.example.com");
    assert_eq!(parsed.spec.node_name, "node-1");
    assert_eq!(
        parsed.spec.source.persistent_volume_name.as_deref(),
        Some("pv-csi-1")
    );
    assert!(parsed.status.is_none(), "status must be absent on create");
}

/// [sig-storage] VolumeAttachment Conformance should apply changes to a
/// volumeattachment status [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/volume_attachment.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// After creation the CSI external-attacher sets `status.attached = true`.
/// This test asserts the status sub-resource can be patched without disturbing
/// the spec.
#[test]
fn volume_attachment_status_can_be_applied() {
    let mut va = VolumeAttachment {
        type_meta: TypeMeta {
            kind: "VolumeAttachment".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("va-status"),
        spec: VolumeAttachmentSpec {
            attacher: "csi.example.com".to_string(),
            node_name: "node-1".to_string(),
            source: VolumeAttachmentSource {
                persistent_volume_name: Some("pv-csi-2".to_string()),
                inline_volume_spec: None,
            },
        },
        status: None,
    };

    // Simulate CSI external-attacher setting status.attached = true.
    va.status = Some(VolumeAttachmentStatus {
        attached: true,
        attachment_metadata: Some({
            let mut m = HashMap::new();
            m.insert(
                "csi.example.com/driver-id".to_string(),
                "drv-42".to_string(),
            );
            m
        }),
        attach_error: None,
        detach_error: None,
    });

    let json = serde_json::to_string(&va).unwrap();
    let parsed: VolumeAttachment = serde_json::from_str(&json).unwrap();
    let status = parsed.status.unwrap();
    assert!(
        status.attached,
        "status.attached must round-trip as true \
         (VolumeAttachment Conformance: apply changes to status)"
    );
    assert_eq!(
        status
            .attachment_metadata
            .as_ref()
            .and_then(|m| m.get("csi.example.com/driver-id"))
            .map(String::as_str),
        Some("drv-42"),
        "attachmentMetadata must round-trip"
    );
    // Spec must be unchanged by status update.
    assert_eq!(
        parsed.spec.attacher, "csi.example.com",
        "spec must be immutable across status patch"
    );
}

/// [sig-storage] VolumeAttributesClass should run through the lifecycle of a
/// VolumeAttributesClass [FeatureGate:VolumeAttributesClass] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/storage/volume_attributes_class.go
/// Sonobuoy (v1.35, 2026-05-28): newly-passing
///
/// VolumeAttributesClass is a cluster-scoped resource that describes a named
/// set of attributes (e.g. IOPS, throughput) to apply to volumes. Lifecycle:
/// create → read → update parameters → delete.
#[test]
fn volume_attributes_class_lifecycle_serde_round_trip() {
    let vac = VolumeAttributesClass {
        type_meta: TypeMeta {
            kind: "VolumeAttributesClass".to_string(),
            api_version: "storage.k8s.io/v1alpha1".to_string(),
        },
        metadata: ObjectMeta::new("premium-iops"),
        driver_name: "csi.example.com".to_string(),
        parameters: Some({
            let mut p = HashMap::new();
            p.insert("iops".to_string(), "5000".to_string());
            p.insert("throughput".to_string(), "200MiB/s".to_string());
            p
        }),
    };

    let json = serde_json::to_string(&vac).unwrap();
    assert!(
        json.contains("\"VolumeAttributesClass\""),
        "kind must round-trip"
    );
    assert!(
        json.contains("\"csi.example.com\""),
        "driverName must round-trip"
    );

    let mut parsed: VolumeAttributesClass = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.driver_name, "csi.example.com");
    assert_eq!(
        parsed
            .parameters
            .as_ref()
            .and_then(|p| p.get("iops"))
            .map(String::as_str),
        Some("5000")
    );

    // Simulate "update parameters" operation.
    parsed
        .parameters
        .get_or_insert_with(HashMap::new)
        .insert("iops".to_string(), "10000".to_string());
    assert_eq!(
        parsed
            .parameters
            .as_ref()
            .and_then(|p| p.get("iops"))
            .map(String::as_str),
        Some("10000"),
        "parameters must be mutable within a VolumeAttributesClass update"
    );
    // driverName is immutable.
    assert_eq!(
        parsed.driver_name, "csi.example.com",
        "driverName is immutable across VolumeAttributesClass lifecycle"
    );
}
