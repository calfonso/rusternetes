//! Integration tests for VolumeAttachment handler
//!
//! Tests all CRUD operations, edge cases, and error handling for volumeattachments.
//! VolumeAttachment captures the intent to attach or detach volumes to/from nodes.

use rusternetes_common::resources::csi::{
    CSIVolumeSource, InlineVolumeSpec, VolumeAttachment, VolumeAttachmentSource,
    VolumeAttachmentSpec, VolumeAttachmentStatus, VolumeError,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test volumeattachment with persistent volume
fn create_test_volumeattachment(
    name: &str,
    attacher: &str,
    node: &str,
    pv: &str,
) -> VolumeAttachment {
    VolumeAttachment {
        type_meta: TypeMeta {
            api_version: "storage.k8s.io/v1".to_string(),
            kind: "VolumeAttachment".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            ..Default::default()
        },
        spec: VolumeAttachmentSpec {
            attacher: attacher.to_string(),
            node_name: node.to_string(),
            source: VolumeAttachmentSource {
                persistent_volume_name: Some(pv.to_string()),
                inline_volume_spec: None,
            },
        },
        status: None,
    }
}

#[tokio::test]
async fn test_volumeattachment_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let va = create_test_volumeattachment("test-va", "csi-driver", "node1", "pv-123");
    let key = build_key("volumeattachments", None, "test-va");

    // Create
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    assert_eq!(created.metadata.name, "test-va");
    assert!(!created.metadata.uid.is_empty());
    assert_eq!(created.spec.attacher, "csi-driver");
    assert_eq!(created.spec.node_name, "node1");
    assert_eq!(
        created.spec.source.persistent_volume_name,
        Some("pv-123".to_string())
    );

    // Get
    let retrieved: VolumeAttachment = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-va");
    assert_eq!(retrieved.spec.attacher, "csi-driver");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut va = create_test_volumeattachment("test-update-va", "csi-driver", "node1", "pv-123");
    let key = build_key("volumeattachments", None, "test-update-va");

    // Create
    storage.create(&key, &va).await.unwrap();

    // Update status to attached
    va.status = Some(VolumeAttachmentStatus {
        attached: true,
        attachment_metadata: None,
        attach_error: None,
        detach_error: None,
    });

    let updated: VolumeAttachment = storage.update(&key, &va).await.unwrap();
    assert!(updated.status.as_ref().unwrap().attached);

    // Verify update
    let retrieved: VolumeAttachment = storage.get(&key).await.unwrap();
    assert!(retrieved.status.as_ref().unwrap().attached);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let va = create_test_volumeattachment("test-delete-va", "csi-driver", "node1", "pv-123");
    let key = build_key("volumeattachments", None, "test-delete-va");

    // Create
    storage.create(&key, &va).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<VolumeAttachment>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_volumeattachment_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple volumeattachments
    let va1 = create_test_volumeattachment("va-1", "csi-driver-1", "node1", "pv-1");
    let va2 = create_test_volumeattachment("va-2", "csi-driver-2", "node2", "pv-2");
    let va3 = create_test_volumeattachment("va-3", "csi-driver-1", "node3", "pv-3");

    let key1 = build_key("volumeattachments", None, "va-1");
    let key2 = build_key("volumeattachments", None, "va-2");
    let key3 = build_key("volumeattachments", None, "va-3");

    storage.create(&key1, &va1).await.unwrap();
    storage.create(&key2, &va2).await.unwrap();
    storage.create(&key3, &va3).await.unwrap();

    // List
    let prefix = build_prefix("volumeattachments", None);
    let vas: Vec<VolumeAttachment> = storage.list(&prefix).await.unwrap();

    assert!(vas.len() >= 3);
    let names: Vec<String> = vas.iter().map(|v| v.metadata.name.clone()).collect();
    assert!(names.contains(&"va-1".to_string()));
    assert!(names.contains(&"va-2".to_string()));
    assert!(names.contains(&"va-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_with_status() {
    let storage = Arc::new(MemoryStorage::new());

    let mut metadata = HashMap::new();
    metadata.insert("devicePath".to_string(), "/dev/sdc".to_string());

    let mut va = create_test_volumeattachment("test-status", "csi-driver", "node1", "pv-123");
    va.status = Some(VolumeAttachmentStatus {
        attached: true,
        attachment_metadata: Some(metadata),
        attach_error: None,
        detach_error: None,
    });

    let key = build_key("volumeattachments", None, "test-status");

    // Create with status
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    assert!(created.status.as_ref().unwrap().attached);
    let metadata = created
        .status
        .as_ref()
        .unwrap()
        .attachment_metadata
        .as_ref()
        .unwrap();
    assert_eq!(metadata.get("devicePath"), Some(&"/dev/sdc".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_with_attach_error() {
    let storage = Arc::new(MemoryStorage::new());

    let mut va = create_test_volumeattachment("test-error", "csi-driver", "node1", "pv-123");
    va.status = Some(VolumeAttachmentStatus {
        attached: false,
        attachment_metadata: None,
        attach_error: Some(VolumeError {
            time: Some("2026-03-16T10:00:00Z".to_string()),
            message: Some("Failed to attach volume: volume not found".to_string()),
        }),
        detach_error: None,
    });

    let key = build_key("volumeattachments", None, "test-error");

    // Create with attach error
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    assert!(!created.status.as_ref().unwrap().attached);
    let error = created
        .status
        .as_ref()
        .unwrap()
        .attach_error
        .as_ref()
        .unwrap();
    assert_eq!(
        error.message,
        Some("Failed to attach volume: volume not found".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_with_detach_error() {
    let storage = Arc::new(MemoryStorage::new());

    let mut va = create_test_volumeattachment("test-detach-error", "csi-driver", "node1", "pv-123");
    va.status = Some(VolumeAttachmentStatus {
        attached: true,
        attachment_metadata: None,
        attach_error: None,
        detach_error: Some(VolumeError {
            time: Some("2026-03-16T11:00:00Z".to_string()),
            message: Some("Failed to detach volume: device busy".to_string()),
        }),
    });

    let key = build_key("volumeattachments", None, "test-detach-error");

    // Create with detach error
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    let error = created
        .status
        .as_ref()
        .unwrap()
        .detach_error
        .as_ref()
        .unwrap();
    assert_eq!(
        error.message,
        Some("Failed to detach volume: device busy".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_with_inline_volume() {
    let storage = Arc::new(MemoryStorage::new());

    let mut volume_attributes = HashMap::new();
    volume_attributes.insert(
        "storage.kubernetes.io/csiProvisionerIdentity".to_string(),
        "test-provisioner".to_string(),
    );

    let va = VolumeAttachment {
        type_meta: TypeMeta {
            api_version: "storage.k8s.io/v1".to_string(),
            kind: "VolumeAttachment".to_string(),
        },
        metadata: ObjectMeta::new("test-inline"),
        spec: VolumeAttachmentSpec {
            attacher: "csi-driver".to_string(),
            node_name: "node1".to_string(),
            source: VolumeAttachmentSource {
                persistent_volume_name: None,
                inline_volume_spec: Some(InlineVolumeSpec {
                    csi: Some(CSIVolumeSource {
                        driver: "csi.example.com".to_string(),
                        volume_handle: Some("vol-12345".to_string()),
                        read_only: Some(false),
                        fs_type: Some("ext4".to_string()),
                        volume_attributes: Some(volume_attributes),
                        node_publish_secret_ref: None,
                    }),
                }),
            },
        },
        status: None,
    };

    let key = build_key("volumeattachments", None, "test-inline");

    // Create with inline volume spec
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    assert!(created.spec.source.persistent_volume_name.is_none());
    assert!(created.spec.source.inline_volume_spec.is_some());
    let inline = created.spec.source.inline_volume_spec.as_ref().unwrap();
    let csi = inline.csi.as_ref().unwrap();
    assert_eq!(csi.driver, "csi.example.com");
    assert_eq!(csi.volume_handle, Some("vol-12345".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut va = create_test_volumeattachment("test-finalizers", "csi-driver", "node1", "pv-123");
    va.metadata.finalizers = Some(vec!["external-attacher/csi.example.com".to_string()]);

    let key = build_key("volumeattachments", None, "test-finalizers");

    // Create with finalizer
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["external-attacher/csi.example.com".to_string()])
    );

    // Verify finalizer is present
    let retrieved: VolumeAttachment = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["external-attacher/csi.example.com".to_string()])
    );

    // Clean up - remove finalizer first
    va.metadata.finalizers = None;
    storage.update(&key, &va).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let va = create_test_volumeattachment("test-immutable", "csi-driver", "node1", "pv-123");
    let key = build_key("volumeattachments", None, "test-immutable");

    // Create
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_va = created.clone();
    updated_va.status = Some(VolumeAttachmentStatus {
        attached: true,
        attachment_metadata: None,
        attach_error: None,
        detach_error: None,
    });

    let updated: VolumeAttachment = storage.update(&key, &updated_va).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert!(updated.status.as_ref().unwrap().attached);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("volumeattachments", None, "nonexistent");
    let result = storage.get::<VolumeAttachment>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_volumeattachment_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let va = create_test_volumeattachment("nonexistent", "csi-driver", "node1", "pv-123");
    let key = build_key("volumeattachments", None, "nonexistent");

    let result = storage.update(&key, &va).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_volumeattachment_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("environment".to_string(), "production".to_string());
    labels.insert("storage-tier".to_string(), "ssd".to_string());

    let mut va = create_test_volumeattachment("test-labels", "csi-driver", "node1", "pv-123");
    va.metadata.labels = Some(labels.clone());

    let key = build_key("volumeattachments", None, "test-labels");

    // Create with labels
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(
        created_labels.get("environment"),
        Some(&"production".to_string())
    );
    assert_eq!(created_labels.get("storage-tier"), Some(&"ssd".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert(
        "description".to_string(),
        "Production database volume attachment".to_string(),
    );
    annotations.insert("owner".to_string(), "storage-team".to_string());

    let mut va = create_test_volumeattachment("test-annotations", "csi-driver", "node1", "pv-123");
    va.metadata.annotations = Some(annotations.clone());

    let key = build_key("volumeattachments", None, "test-annotations");

    // Create with annotations
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    let created_annotations = created.metadata.annotations.unwrap();
    assert_eq!(
        created_annotations.get("description"),
        Some(&"Production database volume attachment".to_string())
    );
    assert_eq!(
        created_annotations.get("owner"),
        Some(&"storage-team".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_multiple_drivers() {
    let storage = Arc::new(MemoryStorage::new());

    let drivers = vec![
        ("va-ebs", "ebs.csi.aws.com"),
        ("va-efs", "efs.csi.aws.com"),
        ("va-gce", "pd.csi.storage.gke.io"),
    ];

    for (name, driver) in drivers {
        let va = create_test_volumeattachment(name, driver, "node1", "pv-123");
        let key = build_key("volumeattachments", None, name);

        let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
        assert_eq!(created.spec.attacher, driver);

        // Clean up
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_volumeattachment_attachment_lifecycle() {
    let storage = Arc::new(MemoryStorage::new());

    let mut va = create_test_volumeattachment("test-lifecycle", "csi-driver", "node1", "pv-123");
    let key = build_key("volumeattachments", None, "test-lifecycle");

    // Create - not attached yet
    storage.create(&key, &va).await.unwrap();

    // Transition 1: Attaching (no status yet)
    let retrieved1: VolumeAttachment = storage.get(&key).await.unwrap();
    assert!(retrieved1.status.is_none());

    // Transition 2: Attached successfully
    va.status = Some(VolumeAttachmentStatus {
        attached: true,
        attachment_metadata: Some({
            let mut m = HashMap::new();
            m.insert("devicePath".to_string(), "/dev/sdc".to_string());
            m
        }),
        attach_error: None,
        detach_error: None,
    });
    storage.update(&key, &va).await.unwrap();

    let retrieved2: VolumeAttachment = storage.get(&key).await.unwrap();
    assert!(retrieved2.status.as_ref().unwrap().attached);

    // Transition 3: Detached
    va.status = Some(VolumeAttachmentStatus {
        attached: false,
        attachment_metadata: None,
        attach_error: None,
        detach_error: None,
    });
    storage.update(&key, &va).await.unwrap();

    let retrieved3: VolumeAttachment = storage.get(&key).await.unwrap();
    assert!(!retrieved3.status.as_ref().unwrap().attached);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_all_fields() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("test".to_string(), "comprehensive".to_string());

    let mut metadata_map = HashMap::new();
    metadata_map.insert("devicePath".to_string(), "/dev/sdc".to_string());

    let mut volume_attrs = HashMap::new();
    volume_attrs.insert("provisioner".to_string(), "test".to_string());

    let va = VolumeAttachment {
        type_meta: TypeMeta {
            api_version: "storage.k8s.io/v1".to_string(),
            kind: "VolumeAttachment".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-all-fields".to_string(),
            labels: Some(labels),
            ..Default::default()
        },
        spec: VolumeAttachmentSpec {
            attacher: "csi.example.com".to_string(),
            node_name: "node1".to_string(),
            source: VolumeAttachmentSource {
                persistent_volume_name: None,
                inline_volume_spec: Some(InlineVolumeSpec {
                    csi: Some(CSIVolumeSource {
                        driver: "csi.example.com".to_string(),
                        volume_handle: Some("vol-abc123".to_string()),
                        read_only: Some(false),
                        fs_type: Some("ext4".to_string()),
                        volume_attributes: Some(volume_attrs),
                        node_publish_secret_ref: None,
                    }),
                }),
            },
        },
        status: Some(VolumeAttachmentStatus {
            attached: true,
            attachment_metadata: Some(metadata_map),
            attach_error: None,
            detach_error: None,
        }),
    };

    let key = build_key("volumeattachments", None, "test-all-fields");

    // Create with all fields populated
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    assert!(created.metadata.labels.is_some());
    assert!(created.status.is_some());
    assert!(created.spec.source.inline_volume_spec.is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_volumeattachment_node_affinity() {
    let storage = Arc::new(MemoryStorage::new());

    // Test multiple VolumeAttachments to different nodes
    let nodes = ["node1", "node2", "node3"];

    for (i, node) in nodes.iter().enumerate() {
        let name = format!("va-node-{}", i);
        let va = create_test_volumeattachment(&name, "csi-driver", node, &format!("pv-{}", i));
        let key = build_key("volumeattachments", None, &name);

        let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
        assert_eq!(created.spec.node_name, *node);

        // Clean up
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_volumeattachment_csi_volume_source_fields() {
    let storage = Arc::new(MemoryStorage::new());

    let mut volume_attrs = HashMap::new();
    volume_attrs.insert(
        "storage.kubernetes.io/csiProvisionerIdentity".to_string(),
        "provisioner-123".to_string(),
    );
    volume_attrs.insert("capacity".to_string(), "100Gi".to_string());

    let va = VolumeAttachment {
        type_meta: TypeMeta {
            api_version: "storage.k8s.io/v1".to_string(),
            kind: "VolumeAttachment".to_string(),
        },
        metadata: ObjectMeta::new("test-csi-fields"),
        spec: VolumeAttachmentSpec {
            attacher: "csi-driver".to_string(),
            node_name: "node1".to_string(),
            source: VolumeAttachmentSource {
                persistent_volume_name: None,
                inline_volume_spec: Some(InlineVolumeSpec {
                    csi: Some(CSIVolumeSource {
                        driver: "csi.example.com".to_string(),
                        volume_handle: Some("vol-xyz789".to_string()),
                        read_only: Some(true),
                        fs_type: Some("xfs".to_string()),
                        volume_attributes: Some(volume_attrs),
                        node_publish_secret_ref: None,
                    }),
                }),
            },
        },
        status: None,
    };

    let key = build_key("volumeattachments", None, "test-csi-fields");

    // Create and verify all CSI fields
    let created: VolumeAttachment = storage.create(&key, &va).await.unwrap();
    let inline = created.spec.source.inline_volume_spec.as_ref().unwrap();
    let csi = inline.csi.as_ref().unwrap();
    assert_eq!(csi.driver, "csi.example.com");
    assert_eq!(csi.volume_handle, Some("vol-xyz789".to_string()));
    assert_eq!(csi.read_only, Some(true));
    assert_eq!(csi.fs_type, Some("xfs".to_string()));
    assert!(csi.volume_attributes.is_some());
    let attrs = csi.volume_attributes.as_ref().unwrap();
    assert_eq!(attrs.get("capacity"), Some(&"100Gi".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}
