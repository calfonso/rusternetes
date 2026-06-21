//! Tests for VolumeAttachment validation (port of upstream `ValidateVolumeAttachment`).

use rusternetes_common::resources::csi::{
    VolumeAttachment, VolumeAttachmentSource, VolumeAttachmentSpec,
};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::volumeattachment::validate_volume_attachment;

fn va(attacher: &str, node: &str, source: VolumeAttachmentSource) -> VolumeAttachment {
    let mut x = VolumeAttachment {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec: VolumeAttachmentSpec {
            attacher: attacher.to_string(),
            node_name: node.to_string(),
            source,
        },
        status: None,
    };
    x.metadata.name = "csi-abc".to_string();
    x
}

fn pv_source(name: &str) -> VolumeAttachmentSource {
    VolumeAttachmentSource {
        persistent_volume_name: Some(name.to_string()),
        inline_volume_spec: None,
    }
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field == field)
}

#[test]
fn valid_volumeattachment_passes() {
    assert!(
        validate_volume_attachment(&va("csi.example.com", "node-1", pv_source("pv-1"))).is_empty()
    );
}

#[test]
fn missing_attacher_rejected() {
    let errs = validate_volume_attachment(&va("", "node-1", pv_source("pv-1")));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.attacher" && e.error_type == ErrorType::Required));
}

#[test]
fn missing_source_rejected() {
    let src = VolumeAttachmentSource {
        persistent_volume_name: None,
        inline_volume_spec: None,
    };
    let errs = validate_volume_attachment(&va("csi.example.com", "node-1", src));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.source" && e.error_type == ErrorType::Required));
}

#[test]
fn empty_pv_name_rejected() {
    let errs = validate_volume_attachment(&va("csi.example.com", "node-1", pv_source("")));
    assert!(has(&errs, "spec.source.persistentVolumeName"));
}

#[test]
fn missing_node_name_rejected() {
    assert!(has(
        &validate_volume_attachment(&va("csi.example.com", "", pv_source("pv-1"))),
        "spec.nodeName"
    ));
}

#[test]
fn invalid_node_name_rejected() {
    assert!(has(
        &validate_volume_attachment(&va("csi.example.com", "Bad_Node!", pv_source("pv-1"))),
        "spec.nodeName"
    ));
}
