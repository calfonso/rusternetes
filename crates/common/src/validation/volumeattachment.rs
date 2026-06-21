//! VolumeAttachment validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateVolumeAttachment`
//! (release-1.35).
//!
//! Covers `spec.attacher` (required), `spec.source` (exactly one of
//! `persistentVolumeName` / `inlineVolumeSpec`, PV name non-empty) and
//! `spec.nodeName` (DNS-subdomain node name). ObjectMeta and `status` are
//! validated separately; full `inlineVolumeSpec` PersistentVolumeSpec
//! validation is out of scope here (deferred). CSI is a non-negotiable contract.

use crate::resources::csi::VolumeAttachment;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;

/// Validate a `VolumeAttachment` on create. Mirrors upstream
/// `validateVolumeAttachmentSpec` (minus inline PV spec + status).
pub fn validate_volume_attachment(va: &VolumeAttachment) -> ErrorList {
    let spec = &va.spec;
    let spec_path = Path::new("spec");
    let mut errs: ErrorList = Vec::new();

    // attacher — required.
    if spec.attacher.is_empty() {
        errs.push(Error::required(&spec_path.child("attacher"), ""));
    }

    // source — exactly one of inlineVolumeSpec / persistentVolumeName.
    let source = &spec.source;
    let source_path = spec_path.child("source");
    match (&source.inline_volume_spec, &source.persistent_volume_name) {
        (None, None) => errs.push(Error::required(
            &source_path,
            "must specify exactly one of inlineVolumeSpec and persistentVolumeName",
        )),
        (Some(_), Some(_)) => errs.push(Error::forbidden(
            &source_path,
            "must specify exactly one of inlineVolumeSpec and persistentVolumeName",
        )),
        (None, Some(pv)) if pv.is_empty() => errs.push(Error::required(
            &source_path.child("persistentVolumeName"),
            "must specify non empty persistentVolumeName",
        )),
        // inlineVolumeSpec set: full PersistentVolumeSpec validation deferred.
        _ => {}
    }

    // nodeName — a DNS-subdomain node name (also rejects empty).
    for msg in is_dns1123_subdomain(&spec.node_name) {
        errs.push(Error::invalid(
            &spec_path.child("nodeName"),
            spec.node_name.clone(),
            msg,
        ));
    }

    errs
}

/// Validate a VolumeAttachment update — upstream `ValidateVolumeAttachmentUpdate`
/// (pkg/apis/storage/validation): the spec is read-only (immutable), plus full
/// re-validation of the new object.
pub fn validate_volume_attachment_update(
    new_va: &VolumeAttachment,
    old_va: &VolumeAttachment,
) -> ErrorList {
    let mut errs = validate_volume_attachment(new_va);
    if serde_json::to_value(&new_va.spec).ok() != serde_json::to_value(&old_va.spec).ok() {
        errs.push(Error::invalid(
            &Path::new("spec"),
            "<spec>".to_string(),
            "field is immutable",
        ));
    }
    errs
}
