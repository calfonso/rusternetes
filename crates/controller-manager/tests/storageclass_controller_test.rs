// Integration tests for StorageClass Controller (Phase 2.3 RED-state)
//
// These tests pin the expected behaviour of the not-yet-implemented
// StorageClassController against an in-memory storage backend. Every
// test is annotated with `#[ignore]` until the controller grows real
// reconciliation logic — see
// `crates/controller-manager/src/controllers/storage_class.rs`.
//
// Upstream reference: kubernetes/test/e2e/storage/storage_class.go

use rusternetes_common::resources::volume::*;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::storage_class::{
    StorageClassController, IS_DEFAULT_STORAGE_CLASS_ANNOTATION,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn make_storage_class(
    name: &str,
    provisioner: &str,
    annotations: Option<HashMap<String, String>>,
    parameters: Option<HashMap<String, String>>,
    reclaim_policy: Option<PersistentVolumeReclaimPolicy>,
    mount_options: Option<Vec<String>>,
) -> StorageClass {
    let mut meta = ObjectMeta::new(name);
    meta.annotations = annotations;
    StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: meta,
        provisioner: provisioner.to_string(),
        parameters,
        reclaim_policy,
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options,
    }
}

/// Asserts the controller demotes any extra defaults so only a single
/// StorageClass remains marked as the cluster-wide default class.
///
/// Upstream behaviour: kube-controller-manager enforces the
/// `storageclass.kubernetes.io/is-default-class` invariant at admission
/// (the API server actually) and the controller backstops it. We assert
/// the steady-state invariant after reconcile.
#[tokio::test]
async fn test_storageclass_default_designation_single_default() {
    let storage = setup_test().await;

    let mut ann_true = HashMap::new();
    ann_true.insert(
        IS_DEFAULT_STORAGE_CLASS_ANNOTATION.to_string(),
        "true".to_string(),
    );

    // Two classes both claiming to be the default.
    let sc_a = make_storage_class(
        "default-a",
        "rusternetes.io/hostpath",
        Some(ann_true.clone()),
        None,
        Some(PersistentVolumeReclaimPolicy::Delete),
        None,
    );
    let sc_b = make_storage_class(
        "default-b",
        "rusternetes.io/hostpath",
        Some(ann_true.clone()),
        None,
        Some(PersistentVolumeReclaimPolicy::Delete),
        None,
    );

    storage
        .create(&build_key("storageclasses", None, "default-a"), &sc_a)
        .await
        .unwrap();
    storage
        .create(&build_key("storageclasses", None, "default-b"), &sc_b)
        .await
        .unwrap();

    let controller = StorageClassController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // After reconciliation, exactly one class should remain the default.
    let classes: Vec<StorageClass> = storage
        .list("/registry/storageclasses/")
        .await
        .unwrap_or_default();
    let default_count = classes
        .iter()
        .filter(|sc| {
            sc.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(IS_DEFAULT_STORAGE_CLASS_ANNOTATION))
                .map(|v| v == "true")
                .unwrap_or(false)
        })
        .count();
    assert_eq!(
        default_count, 1,
        "exactly one StorageClass should be marked default after reconcile"
    );
}

/// A class with the default annotation explicitly set to `"false"` must
/// remain untouched — the controller must never promote a non-default to
/// default on its own.
#[tokio::test]
async fn test_storageclass_default_designation_zero_defaults() {
    let storage = setup_test().await;

    let mut ann_false = HashMap::new();
    ann_false.insert(
        IS_DEFAULT_STORAGE_CLASS_ANNOTATION.to_string(),
        "false".to_string(),
    );

    let sc = make_storage_class(
        "non-default",
        "rusternetes.io/hostpath",
        Some(ann_false),
        None,
        Some(PersistentVolumeReclaimPolicy::Delete),
        None,
    );
    storage
        .create(&build_key("storageclasses", None, "non-default"), &sc)
        .await
        .unwrap();

    let controller = StorageClassController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let stored: StorageClass = storage
        .get(&build_key("storageclasses", None, "non-default"))
        .await
        .unwrap();

    let val = stored
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(IS_DEFAULT_STORAGE_CLASS_ANNOTATION))
        .cloned();
    assert_eq!(
        val,
        Some("false".to_string()),
        "non-default class must not be promoted to default"
    );
}

/// Provisioner-specific parameters supplied via `parameters` must be
/// preserved verbatim through reconciliation; the controller has no
/// business mutating opaque driver config.
#[tokio::test]
async fn test_storageclass_provisioner_parameters_preserved() {
    let storage = setup_test().await;

    let mut params = HashMap::new();
    params.insert("type".to_string(), "gp3".to_string());
    params.insert("iops".to_string(), "3000".to_string());
    params.insert("encrypted".to_string(), "true".to_string());

    let sc = make_storage_class(
        "ebs-fast",
        "ebs.csi.aws.com",
        None,
        Some(params.clone()),
        Some(PersistentVolumeReclaimPolicy::Delete),
        None,
    );
    storage
        .create(&build_key("storageclasses", None, "ebs-fast"), &sc)
        .await
        .unwrap();

    let controller = StorageClassController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let stored: StorageClass = storage
        .get(&build_key("storageclasses", None, "ebs-fast"))
        .await
        .unwrap();
    assert_eq!(
        stored.parameters.as_ref(),
        Some(&params),
        "provisioner parameters must round-trip through reconcile"
    );
    assert_eq!(stored.provisioner, "ebs.csi.aws.com");
}

/// Mount options declared on the StorageClass must be reflected on PV
/// objects bound through it. We assert the steady-state invariant
/// (PV.spec.mount_options == StorageClass.mount_options) and leave the
/// mechanism — set-at-provisioning vs. backfill-on-reconcile — to the
/// GREEN-state implementation. Upstream Kubernetes sets these at
/// provisioning time via the external provisioner; an in-process port
/// may legitimately choose to backfill instead.
#[tokio::test]
async fn test_storageclass_mount_options_propagation_to_pv() {
    let storage = setup_test().await;

    let mount_opts = vec!["ro".to_string(), "soft".to_string()];

    let sc = make_storage_class(
        "ro-class",
        "rusternetes.io/hostpath",
        None,
        None,
        Some(PersistentVolumeReclaimPolicy::Delete),
        Some(mount_opts.clone()),
    );
    storage
        .create(&build_key("storageclasses", None, "ro-class"), &sc)
        .await
        .unwrap();

    // Seed a PV that references this StorageClass but is missing mount
    // options — the controller is expected to backfill them.
    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), "5Gi".to_string());
    let pv = PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new("pv-needs-mount-opts");
            m.uid = uuid::Uuid::new_v4().to_string();
            m
        },
        spec: PersistentVolumeSpec {
            capacity,
            host_path: Some(HostPathVolumeSource {
                path: "/tmp/test-pv/needs-mount-opts".to_string(),
                r#type: Some(HostPathType::DirectoryOrCreate),
            }),
            nfs: None,
            iscsi: None,
            local: None,
            csi: None,
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
            storage_class_name: Some("ro-class".to_string()),
            mount_options: None,
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            node_affinity: None,
            claim_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    };
    storage
        .create(
            &build_key("persistentvolumes", None, "pv-needs-mount-opts"),
            &pv,
        )
        .await
        .unwrap();

    let controller = StorageClassController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let updated: PersistentVolume = storage
        .get(&build_key("persistentvolumes", None, "pv-needs-mount-opts"))
        .await
        .unwrap();
    assert_eq!(
        updated.spec.mount_options.as_ref(),
        Some(&mount_opts),
        "StorageClass mount options must propagate to bound PV"
    );
}

/// A StorageClass created without an explicit `reclaim_policy` must be
/// defaulted to `Delete` by the controller (matches upstream
/// `pkg/registry/storage/storageclass/strategy.go` defaulting).
#[tokio::test]
async fn test_storageclass_reclaim_policy_default_delete() {
    let storage = setup_test().await;

    let sc = make_storage_class(
        "no-policy",
        "rusternetes.io/hostpath",
        None,
        None,
        None, // intentionally omitted
        None,
    );
    storage
        .create(&build_key("storageclasses", None, "no-policy"), &sc)
        .await
        .unwrap();

    let controller = StorageClassController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let stored: StorageClass = storage
        .get(&build_key("storageclasses", None, "no-policy"))
        .await
        .unwrap();
    assert_eq!(
        stored.reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Delete),
        "missing reclaim policy must default to Delete"
    );
}

/// An explicit `Retain` reclaim policy must be preserved — the controller
/// must default only when the field is empty, never overwrite a valid
/// caller-supplied value.
#[tokio::test]
async fn test_storageclass_reclaim_policy_retain_preserved() {
    let storage = setup_test().await;

    let sc = make_storage_class(
        "retain-class",
        "rusternetes.io/hostpath",
        None,
        None,
        Some(PersistentVolumeReclaimPolicy::Retain),
        None,
    );
    storage
        .create(&build_key("storageclasses", None, "retain-class"), &sc)
        .await
        .unwrap();

    let controller = StorageClassController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let stored: StorageClass = storage
        .get(&build_key("storageclasses", None, "retain-class"))
        .await
        .unwrap();
    assert_eq!(
        stored.reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Retain),
        "Retain policy must not be overwritten by reconcile"
    );
}
