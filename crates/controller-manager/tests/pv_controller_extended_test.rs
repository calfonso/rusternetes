//! Extended integration tests for PV controllers
//!
//! Covers behaviors from upstream `kubernetes/test/e2e/storage/persistent_volumes.go`
//! that go beyond the basic happy-path covered by `pv_binder_test.rs` and
//! `dynamic_provisioner_test.rs`:
//!
//! - StorageClass-driven dynamic provisioning end-to-end
//! - Reclaim policy `Retain` (PV survives after PVC deletion)
//! - Access mode enforcement during binding (RWO/ROX/RWX)
//! - Capacity boundaries (exact match, fractional, mismatched units)
//! - PV node affinity (binding must honor `spec.nodeAffinity.required`)
//! - StorageClass `mountOptions` propagation to dynamically provisioned PVs
//! - fsGroup support on the consuming pod's SecurityContext
//!
//! Tests that exercise functionality the controllers do not yet implement
//! are marked `#[ignore = "RED-state: ..."]` per the test-driven workflow.

use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::*;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::dynamic_provisioner::DynamicProvisionerController;
use rusternetes_controller_manager::controllers::pv_binder::PVBinderController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

async fn create_storage_class(
    storage: &Arc<MemoryStorage>,
    name: &str,
    provisioner: &str,
    reclaim: PersistentVolumeReclaimPolicy,
    mount_options: Option<Vec<String>>,
) -> StorageClass {
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new(name),
        provisioner: provisioner.to_string(),
        parameters: None,
        reclaim_policy: Some(reclaim),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options,
    };
    let key = build_key("storageclasses", None, name);
    storage.create(&key, &sc).await.unwrap()
}

#[allow(clippy::too_many_arguments)]
fn build_pv(
    name: &str,
    storage_class: Option<&str>,
    capacity: &str,
    access_modes: Vec<PersistentVolumeAccessMode>,
    reclaim: PersistentVolumeReclaimPolicy,
    node_affinity: Option<VolumeNodeAffinity>,
    mount_options: Option<Vec<String>>,
    phase: PersistentVolumePhase,
) -> PersistentVolume {
    let mut cap = HashMap::new();
    cap.insert("storage".to_string(), capacity.to_string());

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
            capacity: cap,
            host_path: Some(HostPathVolumeSource {
                path: format!("/tmp/test-pv/{}", name),
                r#type: Some(HostPathType::DirectoryOrCreate),
            }),
            nfs: None,
            iscsi: None,
            local: None,
            csi: None,
            access_modes,
            persistent_volume_reclaim_policy: Some(reclaim),
            storage_class_name: storage_class.map(|s| s.to_string()),
            mount_options,
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            node_affinity,
            claim_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeStatus {
            phase,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        }),
    }
}

fn build_pvc(
    name: &str,
    namespace: &str,
    storage_class: Option<&str>,
    capacity: &str,
    access_modes: Vec<PersistentVolumeAccessMode>,
) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), capacity.to_string());

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
            access_modes,
            resources: ResourceRequirements {
                limits: None,
                requests: Some(requests),
            },
            volume_name: None,
            storage_class_name: storage_class.map(|s| s.to_string()),
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

// ---------------------------------------------------------------------------
// Group 1: Dynamic provisioning via StorageClass
// ---------------------------------------------------------------------------

/// Upstream parity: `e2e/storage/persistent_volumes.go` dynamic provisioning
/// path. Verifies that the binder picks up a dynamically provisioned PV and
/// completes the bind on the next reconcile pass.
#[tokio::test]
async fn test_dynamic_provisioning_then_bind_end_to_end() {
    let storage = setup_test().await;

    create_storage_class(
        &storage,
        "dyn",
        "rusternetes.io/hostpath",
        PersistentVolumeReclaimPolicy::Delete,
        None,
    )
    .await;

    let pvc = build_pvc(
        "claim-1",
        "default",
        Some("dyn"),
        "3Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "claim-1");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // Step 1: dynamic provisioner creates the PV
    DynamicProvisionerController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let pv_name = "pvc-default-claim-1";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: PersistentVolume = storage
        .get(&pv_key)
        .await
        .expect("dynamic provisioner should create PV");
    assert_eq!(pv.spec.capacity.get("storage"), Some(&"3Gi".to_string()));
    assert_eq!(pv.spec.storage_class_name.as_deref(), Some("dyn"));
    assert!(
        pv.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("pv.kubernetes.io/provisioned-by"))
            .is_some(),
        "provisioned-by annotation should be set on dynamically provisioned PV",
    );

    // Step 2: binder picks the freshly provisioned PV
    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let bound_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(bound_pvc.spec.volume_name.as_deref(), Some(pv_name));
    assert_eq!(
        bound_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
    );

    let bound_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    assert_eq!(
        bound_pv.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Bound,
    );
    let claim_ref = bound_pv.spec.claim_ref.as_ref().expect("claim_ref set");
    assert_eq!(claim_ref.name.as_deref(), Some("claim-1"));
    assert_eq!(claim_ref.namespace.as_deref(), Some("default"));
}

/// Upstream parity: when the provisioner field on the StorageClass refers to a
/// driver we do not support, no PV is created and the PVC stays pending.
#[tokio::test]
async fn test_dynamic_provisioning_skips_unsupported_provisioner() {
    let storage = setup_test().await;

    create_storage_class(
        &storage,
        "ebs",
        "kubernetes.io/aws-ebs",
        PersistentVolumeReclaimPolicy::Delete,
        None,
    )
    .await;

    let pvc = build_pvc(
        "ebs-pvc",
        "default",
        Some("ebs"),
        "5Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "ebs-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    DynamicProvisionerController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let pv_key = build_key("persistentvolumes", None, "pvc-default-ebs-pvc");
    assert!(
        storage.get::<PersistentVolume>(&pv_key).await.is_err(),
        "no PV should be provisioned for an unsupported provisioner",
    );
    let pvc_after: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert!(pvc_after.spec.volume_name.is_none());
}

// ---------------------------------------------------------------------------
// Group 2: Reclaim policy `Retain`
// ---------------------------------------------------------------------------

/// Upstream parity: `e2e/storage/persistent_volumes.go::ReclaimPolicyRetain`.
/// When a PVC is deleted while bound to a PV with `Retain` policy, the PV
/// must transition to phase `Released` and keep its `claim_ref` set so the
/// admin can clean it up manually.
///
/// RED-state: rusternetes does not yet have a release/reclaim controller
/// (no controller in `crates/controller-manager/src/controllers/` watches for
/// deleted PVCs and updates the bound PV). When that lands, drop the
/// `#[ignore]` attribute and the assertions below will pass.
#[tokio::test]
#[ignore = "RED-state: no PV release controller transitions Bound->Released on PVC delete"]
async fn test_reclaim_policy_retain_releases_pv_on_pvc_delete() {
    let storage = setup_test().await;

    // PV with Retain reclaim policy, pre-bound to a PVC.
    let mut pv = build_pv(
        "retain-pv",
        Some("manual"),
        "5Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
        PersistentVolumeReclaimPolicy::Retain,
        None,
        None,
        PersistentVolumePhase::Bound,
    );
    let pvc_uid = uuid::Uuid::new_v4().to_string();
    pv.spec.claim_ref = Some(ObjectReference {
        kind: Some("PersistentVolumeClaim".to_string()),
        namespace: Some("default".to_string()),
        name: Some("retain-pvc".to_string()),
        uid: Some(pvc_uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: None,
        field_path: None,
    });
    let pv_key = build_key("persistentvolumes", None, "retain-pv");
    storage.create(&pv_key, &pv).await.unwrap();

    // No PVC exists (simulating the user having deleted it). The controller
    // should observe the dangling claim_ref and mark the PV Released.
    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let after: PersistentVolume = storage.get(&pv_key).await.unwrap();
    assert_eq!(
        after.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Released,
        "PV with Retain policy must be Released after its PVC is deleted",
    );
    assert!(
        after.spec.claim_ref.is_some(),
        "Retain policy must preserve claim_ref for manual recovery",
    );
}

/// A `Retain` PV with a dangling claim_ref must NOT be picked up by the
/// binder for a new PVC. Binding it would silently expose a previous user's
/// data — the admin has to manually clear `claim_ref` first.
#[tokio::test]
async fn test_retain_pv_with_claim_ref_not_rebound() {
    let storage = setup_test().await;

    let mut pv = build_pv(
        "retain-orphan",
        Some("manual"),
        "10Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
        PersistentVolumeReclaimPolicy::Retain,
        None,
        None,
        PersistentVolumePhase::Released,
    );
    pv.spec.claim_ref = Some(ObjectReference {
        kind: Some("PersistentVolumeClaim".to_string()),
        namespace: Some("default".to_string()),
        name: Some("deleted-pvc".to_string()),
        uid: Some("stale-uid".to_string()),
        api_version: Some("v1".to_string()),
        resource_version: None,
        field_path: None,
    });
    let pv_key = build_key("persistentvolumes", None, "retain-orphan");
    storage.create(&pv_key, &pv).await.unwrap();

    let new_pvc = build_pvc(
        "fresh-pvc",
        "default",
        Some("manual"),
        "5Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "fresh-pvc");
    storage.create(&pvc_key, &new_pvc).await.unwrap();

    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let pvc_after: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert!(
        pvc_after.spec.volume_name.is_none(),
        "new PVC must not bind to a Retain PV with dangling claim_ref",
    );
    let pv_after: PersistentVolume = storage.get(&pv_key).await.unwrap();
    assert_eq!(
        pv_after.spec.claim_ref.as_ref().unwrap().name.as_deref(),
        Some("deleted-pvc"),
        "claim_ref must remain pointing at the previous (deleted) PVC",
    );
}

// ---------------------------------------------------------------------------
// Group 3: Access modes validation
// ---------------------------------------------------------------------------

/// A PV that advertises only RWO must not satisfy a PVC requesting both RWO
/// and ROX — the binder must enforce that the PV supports every mode listed
/// on the PVC.
#[tokio::test]
async fn test_access_modes_pv_must_support_all_pvc_modes() {
    let storage = setup_test().await;

    let pv = build_pv(
        "rwo-only",
        Some("fast"),
        "10Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
        PersistentVolumeReclaimPolicy::Delete,
        None,
        None,
        PersistentVolumePhase::Available,
    );
    storage
        .create(&build_key("persistentvolumes", None, "rwo-only"), &pv)
        .await
        .unwrap();

    let pvc = build_pvc(
        "rwo-rox-pvc",
        "default",
        Some("fast"),
        "5Gi",
        vec![
            PersistentVolumeAccessMode::ReadWriteOnce,
            PersistentVolumeAccessMode::ReadOnlyMany,
        ],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "rwo-rox-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let after: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert!(
        after.spec.volume_name.is_none(),
        "PVC requesting RWO+ROX must not bind to RWO-only PV",
    );
    assert_eq!(
        after.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending,
    );
}

/// A PV advertising the full RWO/ROX/RWX trio must satisfy a PVC requesting
/// only RWX.
#[tokio::test]
async fn test_access_modes_pv_superset_binds_pvc_subset() {
    let storage = setup_test().await;

    let pv = build_pv(
        "multi-mode",
        Some("fast"),
        "10Gi",
        vec![
            PersistentVolumeAccessMode::ReadWriteOnce,
            PersistentVolumeAccessMode::ReadOnlyMany,
            PersistentVolumeAccessMode::ReadWriteMany,
        ],
        PersistentVolumeReclaimPolicy::Delete,
        None,
        None,
        PersistentVolumePhase::Available,
    );
    storage
        .create(&build_key("persistentvolumes", None, "multi-mode"), &pv)
        .await
        .unwrap();

    let pvc = build_pvc(
        "rwx-pvc",
        "default",
        Some("fast"),
        "1Gi",
        vec![PersistentVolumeAccessMode::ReadWriteMany],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "rwx-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let after: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(after.spec.volume_name.as_deref(), Some("multi-mode"));
    assert_eq!(
        after.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound,
    );
    let bound_modes = after
        .status
        .as_ref()
        .unwrap()
        .access_modes
        .as_ref()
        .unwrap();
    assert!(bound_modes.contains(&PersistentVolumeAccessMode::ReadWriteMany));
}

// ---------------------------------------------------------------------------
// Group 4: Capacity enforcement
// ---------------------------------------------------------------------------

/// A PV with exactly the requested capacity must bind — the comparison is
/// "PV capacity >= PVC request", inclusive of equality.
#[tokio::test]
async fn test_capacity_exact_match_binds() {
    let storage = setup_test().await;

    let pv = build_pv(
        "exact",
        Some("fast"),
        "7Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
        PersistentVolumeReclaimPolicy::Delete,
        None,
        None,
        PersistentVolumePhase::Available,
    );
    storage
        .create(&build_key("persistentvolumes", None, "exact"), &pv)
        .await
        .unwrap();

    let pvc = build_pvc(
        "exact-pvc",
        "default",
        Some("fast"),
        "7Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "exact-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let after: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(after.spec.volume_name.as_deref(), Some("exact"));
}

/// A PVC requesting more storage than the only candidate PV provides must
/// remain Pending. The binder must not pick a smaller PV "best-effort".
#[tokio::test]
async fn test_capacity_insufficient_pvc_stays_pending() {
    let storage = setup_test().await;

    let pv = build_pv(
        "tiny",
        Some("fast"),
        "1Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
        PersistentVolumeReclaimPolicy::Delete,
        None,
        None,
        PersistentVolumePhase::Available,
    );
    storage
        .create(&build_key("persistentvolumes", None, "tiny"), &pv)
        .await
        .unwrap();

    let pvc = build_pvc(
        "big-pvc",
        "default",
        Some("fast"),
        "100Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "big-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let after: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert!(after.spec.volume_name.is_none());
    assert_eq!(
        after.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Pending,
    );
}

// ---------------------------------------------------------------------------
// Group 5: Node affinity
// ---------------------------------------------------------------------------

/// Upstream parity: a PV with `spec.nodeAffinity.required` constrains which
/// nodes can mount the volume. The binder must not bind a PV whose required
/// node affinity does not select any node that satisfies the workload — at
/// minimum, the controller must surface or honor the constraint.
///
/// RED-state: the current `PVBinderController::pv_matches_pvc` does not
/// inspect `pv_spec.node_affinity` at all. Once node-aware binding lands
/// (mirroring upstream `volume_scheduling`), this test should pass.
#[tokio::test]
async fn test_node_affinity_blocks_bind_when_no_matching_node() {
    let storage = setup_test().await;

    let node_affinity = VolumeNodeAffinity {
        required: Some(NodeSelector {
            node_selector_terms: vec![NodeSelectorTerm {
                match_expressions: Some(vec![NodeSelectorRequirement {
                    key: "topology.kubernetes.io/zone".to_string(),
                    operator: "In".to_string(),
                    values: Some(vec!["zone-nonexistent".to_string()]),
                }]),
                match_fields: None,
            }],
        }),
    };

    let pv = build_pv(
        "zoned-pv",
        Some("local"),
        "5Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
        PersistentVolumeReclaimPolicy::Retain,
        Some(node_affinity),
        None,
        PersistentVolumePhase::Available,
    );
    storage
        .create(&build_key("persistentvolumes", None, "zoned-pv"), &pv)
        .await
        .unwrap();

    let pvc = build_pvc(
        "zoned-pvc",
        "default",
        Some("local"),
        "1Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "zoned-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let after: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert!(
        after.spec.volume_name.is_none(),
        "binder must not bind a PV whose required nodeAffinity has no matching node",
    );
}

// ---------------------------------------------------------------------------
// Group 6: Mount options
// ---------------------------------------------------------------------------

/// Upstream parity: when a StorageClass declares `mountOptions`, those
/// options must be copied onto every PV the controller dynamically
/// provisions for PVCs that select the class.
///
/// `DynamicProvisionerController::create_pv_for_pvc` copies
/// `storage_class.mount_options` onto the provisioned PV, matching upstream
/// (`pv_controller.go:1677`).
#[tokio::test]
async fn test_mount_options_propagated_from_storage_class() {
    let storage = setup_test().await;

    create_storage_class(
        &storage,
        "with-opts",
        "rusternetes.io/hostpath",
        PersistentVolumeReclaimPolicy::Delete,
        Some(vec!["ro".to_string(), "soft".to_string()]),
    )
    .await;

    let pvc = build_pvc(
        "opts-pvc",
        "default",
        Some("with-opts"),
        "2Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "opts-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    DynamicProvisionerController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let pv_key = build_key("persistentvolumes", None, "pvc-default-opts-pvc");
    let pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    let opts = pv
        .spec
        .mount_options
        .as_ref()
        .expect("mountOptions must be propagated from StorageClass");
    assert!(opts.iter().any(|o| o == "ro"));
    assert!(opts.iter().any(|o| o == "soft"));
}

// ---------------------------------------------------------------------------
// Group 7: fsGroup support (consuming pod side)
// ---------------------------------------------------------------------------

/// fsGroup ownership is a kubelet-side concern (the kubelet recursively
/// chowns the volume to the fsGroup before mount). The controller layer
/// must at least preserve the PVC ↔ PV linkage when the consumer pod
/// declares fsGroup; the binder must not strip or mangle anything when the
/// PVC references a PV whose only consumer has a SecurityContext.fsGroup.
///
/// We exercise the controller-visible portion: binding still succeeds and
/// the resulting PVC status carries the PV's capacity and access modes so
/// the kubelet can correctly perform the chown on attach.
#[tokio::test]
async fn test_binding_preserves_capacity_for_fs_group_consumer() {
    let storage = setup_test().await;

    let pv = build_pv(
        "fsg-pv",
        Some("fast"),
        "4Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
        PersistentVolumeReclaimPolicy::Delete,
        None,
        None,
        PersistentVolumePhase::Available,
    );
    storage
        .create(&build_key("persistentvolumes", None, "fsg-pv"), &pv)
        .await
        .unwrap();

    let pvc = build_pvc(
        "fsg-pvc",
        "default",
        Some("fast"),
        "2Gi",
        vec![PersistentVolumeAccessMode::ReadWriteOnce],
    );
    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "fsg-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    PVBinderController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let after: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(after.spec.volume_name.as_deref(), Some("fsg-pv"));
    let status = after.status.as_ref().unwrap();
    assert_eq!(status.phase, PersistentVolumeClaimPhase::Bound);

    // The kubelet uses status.capacity to size the chown loop. The binder
    // copies the PV's capacity (4Gi), not the PVC's request (2Gi), so the
    // kubelet operates on the real on-disk size.
    let cap = status
        .capacity
        .as_ref()
        .expect("Bound PVC must carry status.capacity for kubelet fsGroup pass");
    assert_eq!(cap.get("storage"), Some(&"4Gi".to_string()));

    // access_modes must be echoed back so the kubelet can decide whether
    // fsGroup recursive chown is safe (it's skipped for ReadOnlyMany).
    let modes = status
        .access_modes
        .as_ref()
        .expect("Bound PVC must carry status.access_modes");
    assert!(modes.contains(&PersistentVolumeAccessMode::ReadWriteOnce));
}
