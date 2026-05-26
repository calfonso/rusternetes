//! RED-state integration tests for the (stub) `VolumeAttachmentController`.
//!
//! These tests document the behaviour required by upstream Kubernetes
//! conformance (`test/e2e/storage/volume_attachment.go`) and the CSI external
//! attacher: creation should drive `status.attached = true`, deletion should
//! detach + remove the object, and CSI errors should surface in
//! `status.attachError` / `status.detachError`.
//!
//! All tests are gated with `#[ignore = "RED-state: VolumeAttachmentController is a stub"]`
//! and will be un-ignored as the controller fills in.

use rusternetes_common::resources::{
    VolumeAttachment, VolumeAttachmentSource, VolumeAttachmentSpec, VolumeAttachmentStatus,
    VolumeError,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::volume_attachment::VolumeAttachmentController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn make_volume_attachment(name: &str, pv_name: &str, node_name: &str) -> VolumeAttachment {
    VolumeAttachment {
        type_meta: TypeMeta {
            kind: "VolumeAttachment".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            // VolumeAttachments are cluster-scoped — no namespace.
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeAttachmentSpec {
            attacher: "test.csi.example.com".to_string(),
            node_name: node_name.to_string(),
            source: VolumeAttachmentSource {
                persistent_volume_name: Some(pv_name.to_string()),
                inline_volume_spec: None,
            },
        },
        status: None,
    }
}

async fn create_va(storage: &MemoryStorage, va: &VolumeAttachment) {
    let key = build_key("volumeattachments", None, &va.metadata.name);
    storage.create(&key, va).await.unwrap();
}

/// CSI attach flow: creating a `VolumeAttachment` should drive the controller
/// to call the CSI driver's `ControllerPublishVolume` and reflect success by
/// setting `status.attached = true`.
#[tokio::test]
#[ignore = "RED-state: VolumeAttachmentController is a stub"]
async fn test_volume_attachment_creation_on_attach() {
    let storage = setup_test().await;

    let va = make_volume_attachment("attach-1", "pv-csi-1", "node-1");
    create_va(&storage, &va).await;

    let controller = VolumeAttachmentController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(100)).await;

    let key = build_key("volumeattachments", None, "attach-1");
    let updated: VolumeAttachment = storage.get(&key).await.unwrap();

    let status = updated
        .status
        .expect("VolumeAttachmentController should populate status after attach");
    assert!(
        status.attached,
        "status.attached should be true after successful CSI attach"
    );
    assert!(
        status.attach_error.is_none(),
        "no attachError expected on success"
    );
}

/// CSI detach flow: deleting (or marking for deletion via deletionTimestamp)
/// a `VolumeAttachment` should drive the controller to call
/// `ControllerUnpublishVolume` and remove the object once the detach
/// succeeds.
#[tokio::test]
#[ignore = "RED-state: VolumeAttachmentController is a stub"]
async fn test_volume_attachment_deletion_on_detach() {
    let storage = setup_test().await;

    // Create an already-attached VolumeAttachment.
    let mut va = make_volume_attachment("detach-1", "pv-csi-2", "node-2");
    va.status = Some(VolumeAttachmentStatus {
        attached: true,
        attachment_metadata: None,
        attach_error: None,
        detach_error: None,
    });
    create_va(&storage, &va).await;

    // Mark it for deletion (the API server would have set deletionTimestamp).
    let key = build_key("volumeattachments", None, "detach-1");
    let mut to_delete: VolumeAttachment = storage.get(&key).await.unwrap();
    to_delete.metadata.deletion_timestamp = Some(chrono::Utc::now());
    storage.update(&key, &to_delete).await.unwrap();

    let controller = VolumeAttachmentController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // After a successful detach the object should be gone from storage.
    let still_present = storage.get::<VolumeAttachment>(&key).await;
    assert!(
        still_present.is_err(),
        "VolumeAttachment should be removed once detach finishes; got {:?}",
        still_present.map(|va| va.metadata.name)
    );
}

/// Error path: when the CSI driver returns an attach error the controller
/// must surface it in `status.attachError` rather than silently retrying or
/// flipping `attached` to true. Symmetric behaviour applies for detach
/// failures via `status.detachError`.
#[tokio::test]
#[ignore = "RED-state: VolumeAttachmentController is a stub"]
async fn test_volume_attachment_error_handling() {
    let storage = setup_test().await;

    // Seed a VolumeAttachment whose backing PV will fail to attach. The test
    // harness for the real controller will wire a CSI mock that returns an
    // error; for now we just assert that *some* error surfaces.
    let va = make_volume_attachment("attach-err", "pv-broken", "node-3");
    create_va(&storage, &va).await;

    let controller = VolumeAttachmentController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(100)).await;

    let key = build_key("volumeattachments", None, "attach-err");
    let updated: VolumeAttachment = storage.get(&key).await.unwrap();

    let status = updated
        .status
        .expect("controller should populate status even on attach failure");
    assert!(
        !status.attached,
        "status.attached must remain false when attach fails"
    );
    let attach_error: VolumeError = status
        .attach_error
        .expect("status.attachError should describe the CSI failure");
    assert!(
        attach_error.message.is_some(),
        "attachError.message should be populated on failure"
    );
}
