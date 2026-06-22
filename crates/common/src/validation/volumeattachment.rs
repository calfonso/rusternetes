//! VolumeAttachment validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateVolumeAttachment`
//! and the v1-only extra checks in `ValidateVolumeAttachmentV1` (release-1.35).
//!
//! Covers `spec.attacher` (required + valid CSI driver name), `spec.source`
//! (exactly one of `persistentVolumeName` / `inlineVolumeSpec`; PV name
//! non-empty and a valid DNS-subdomain PV name), the full `inlineVolumeSpec`
//! PersistentVolumeSpec (via the shared PV-spec validator), `spec.nodeName`
//! (DNS-subdomain node name) and the whole `status` block (attachmentMetadata
//! total size, attach/detach error message length + non-negative errorCode).
//! ObjectMeta is validated separately. CSI is a non-negotiable contract.

use crate::resources::csi::{VolumeAttachment, VolumeAttachmentStatus, VolumeError};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;
use crate::validation::persistentvolume::validate_persistent_volume_spec;

/// `maxAttachedVolumeMetadataSize` — 256 kB (upstream validation.go:44).
const MAX_ATTACHED_VOLUME_METADATA_SIZE: usize = 256 * (1 << 10);
/// `maxVolumeErrorMessageSize` — 1024 (upstream validation.go:45).
const MAX_VOLUME_ERROR_MESSAGE_SIZE: usize = 1024;

/// Validate a `VolumeAttachment` on create. Mirrors upstream
/// `validateVolumeAttachmentSpec` (minus inline PV spec + status).
pub fn validate_volume_attachment(va: &VolumeAttachment) -> ErrorList {
    let spec = &va.spec;
    let spec_path = Path::new("spec");
    let mut errs: ErrorList = Vec::new();

    // attacher — required (validateAttacher) plus, on v1, a valid CSI driver
    // name: a DNS-subdomain ≤63 chars (ValidateVolumeAttachmentV1 :151,
    // ValidateCSIDriverName). Upstream lowercases before the DNS check.
    let attacher_path = spec_path.child("attacher");
    if spec.attacher.is_empty() {
        errs.push(Error::required(&attacher_path, ""));
    } else {
        if spec.attacher.len() > 63 {
            errs.push(Error::too_long(&attacher_path, 63));
        }
        for msg in is_dns1123_subdomain(&spec.attacher.to_lowercase()) {
            errs.push(Error::invalid(&attacher_path, spec.attacher.clone(), msg));
        }
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
        // inlineVolumeSpec set: run the full PersistentVolumeSpec validator
        // (upstream `ValidatePersistentVolumeSpec`).
        (Some(inline), None) => {
            errs.extend(validate_persistent_volume_spec(
                inline,
                &source_path.child("inlineVolumeSpec"),
            ));
        }
        _ => {}
    }

    // persistentVolumeName — a valid PV name (DNS-subdomain). v1-only check
    // (ValidateVolumeAttachmentV1 :153-158, ValidatePersistentVolumeName =
    // NameIsDNSSubdomain). Runs whenever the field is present (non-empty case;
    // the empty case is already flagged Required above).
    if let Some(pv) = &source.persistent_volume_name {
        if !pv.is_empty() {
            for msg in is_dns1123_subdomain(pv) {
                errs.push(Error::invalid(
                    &source_path.child("persistentVolumeName"),
                    pv.clone(),
                    msg,
                ));
            }
        }
    }

    // nodeName — a DNS-subdomain node name (also rejects empty).
    for msg in is_dns1123_subdomain(&spec.node_name) {
        errs.push(Error::invalid(
            &spec_path.child("nodeName"),
            spec.node_name.clone(),
            msg,
        ));
    }

    // status — attachmentMetadata size + attach/detach error checks
    // (validateVolumeAttachmentStatus :212-250).
    if let Some(status) = &va.status {
        errs.extend(validate_volume_attachment_status(
            status,
            &Path::new("status"),
        ));
    }

    errs
}

/// `validateVolumeAttachmentStatus` (validation.go:212-218): attachmentMetadata
/// total size and the attach/detach `VolumeError`s.
fn validate_volume_attachment_status(
    status: &VolumeAttachmentStatus,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // attachmentMetadata — total size (sum of key+value byte lengths) ≤ 256 kB.
    if let Some(metadata) = &status.attachment_metadata {
        let size: usize = metadata.iter().map(|(k, v)| k.len() + v.len()).sum();
        if size > MAX_ATTACHED_VOLUME_METADATA_SIZE {
            errs.push(Error::too_long(
                &fld_path.child("attachmentMetadata"),
                MAX_ATTACHED_VOLUME_METADATA_SIZE,
            ));
        }
    }

    if let Some(e) = &status.attach_error {
        errs.extend(validate_volume_error(e, &fld_path.child("attachError")));
    }
    if let Some(e) = &status.detach_error {
        errs.extend(validate_volume_error(e, &fld_path.child("detachError")));
    }
    errs
}

/// `validateVolumeError` (validation.go:233-250): message length ≤ 1024 and a
/// non-negative errorCode (`must be between 0 and 2147483647, inclusive`).
fn validate_volume_error(e: &VolumeError, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(message) = &e.message {
        if message.len() > MAX_VOLUME_ERROR_MESSAGE_SIZE {
            // Upstream passes maxAttachedVolumeMetadataSize as the limit here
            // (validation.go:240) — a known upstream quirk we mirror so the
            // emitted message matches.
            errs.push(Error::too_long(
                &fld_path.child("message"),
                MAX_ATTACHED_VOLUME_METADATA_SIZE,
            ));
        }
    }

    if let Some(value) = e.error_code {
        if value < 0 {
            errs.push(Error::invalid(
                &fld_path.child("errorCode"),
                value.to_string(),
                format!("must be between 0 and {}, inclusive", i32::MAX),
            ));
        }
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

#[cfg(test)]
mod inline_spec_tests {
    use super::*;

    fn errs(json: serde_json::Value) -> Vec<String> {
        let va: VolumeAttachment = serde_json::from_value(json).unwrap();
        validate_volume_attachment(&va)
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_inline_csi_spec_passes() {
        let e = errs(serde_json::json!({
            "metadata": {"name": "va"},
            "spec": {
                "attacher": "csi.example.com",
                "nodeName": "node-1",
                "source": {"inlineVolumeSpec": {
                    "capacity": {"storage": "1Gi"},
                    "accessModes": ["ReadWriteOnce"],
                    "csi": {"driver": "csi.example.com", "volumeHandle": "vol-1"}
                }}
            }
        }));
        assert!(e.iter().all(|m| !m.contains("inlineVolumeSpec")), "{e:?}");
    }

    #[test]
    fn invalid_inline_spec_is_validated() {
        // empty inline spec: PV-spec validation should flag the missing source/capacity.
        let e = errs(serde_json::json!({
            "metadata": {"name": "va"},
            "spec": {
                "attacher": "csi.example.com",
                "nodeName": "node-1",
                "source": {"inlineVolumeSpec": {}}
            }
        }));
        assert!(
            e.iter().any(|m| m.contains("inlineVolumeSpec")),
            "expected inline spec errors, got {e:?}"
        );
    }
}

#[cfg(test)]
mod parity_tests {
    use super::*;

    fn errs(json: serde_json::Value) -> Vec<String> {
        let va: VolumeAttachment = serde_json::from_value(json).unwrap();
        validate_volume_attachment(&va)
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    /// Base PV-name source: a fully valid VA we mutate per test.
    fn va(attacher: &str, pv_name: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "va"},
            "spec": {
                "attacher": attacher,
                "nodeName": "node-1",
                "source": {"persistentVolumeName": pv_name}
            }
        })
    }

    // --- spec.attacher: valid CSI driver name (DNS-subdomain ≤63) ---

    #[test]
    fn valid_attacher_passes() {
        let e = errs(va("csi.example.com", "pv-1"));
        assert!(e.is_empty(), "expected no errors, got {e:?}");
    }

    #[test]
    fn attacher_with_invalid_chars_is_rejected() {
        let e = errs(va("Bad Attacher!", "pv-1"));
        // uppercase is lowercased by upstream, but the space / '!' are invalid.
        assert!(
            e.iter().any(|m| m.contains("spec.attacher")),
            "expected attacher format error, got {e:?}"
        );
    }

    #[test]
    fn attacher_over_63_chars_is_too_long() {
        let long = format!("{}.example.com", "a".repeat(60));
        let e = errs(va(&long, "pv-1"));
        assert!(
            e.iter()
                .any(|m| m.contains("spec.attacher") && m.contains("more than 63")),
            "expected too-long attacher error, got {e:?}"
        );
    }

    #[test]
    fn empty_attacher_is_required() {
        let e = errs(va("", "pv-1"));
        assert!(
            e.iter()
                .any(|m| m.contains("spec.attacher") && m.contains("Required")
                    || m.contains("spec.attacher") && m.contains("required")),
            "expected required attacher error, got {e:?}"
        );
    }

    // --- source.persistentVolumeName: valid PV name (DNS-subdomain) ---

    #[test]
    fn invalid_pv_name_is_rejected() {
        let e = errs(va("csi.example.com", "Invalid_PV_Name"));
        assert!(
            e.iter()
                .any(|m| m.contains("spec.source.persistentVolumeName")),
            "expected pv-name format error, got {e:?}"
        );
    }

    #[test]
    fn empty_pv_name_is_required_not_format() {
        let e = errs(va("csi.example.com", ""));
        assert!(
            e.iter()
                .any(|m| m.contains("persistentVolumeName") && m.contains("non empty")),
            "expected non-empty pv-name error, got {e:?}"
        );
    }

    // --- status validation ---

    fn va_with_status(status: serde_json::Value) -> serde_json::Value {
        let mut base = va("csi.example.com", "pv-1");
        base["status"] = status;
        base
    }

    #[test]
    fn small_attachment_metadata_passes() {
        let e = errs(va_with_status(serde_json::json!({
            "attached": true,
            "attachmentMetadata": {"key": "value"}
        })));
        assert!(
            e.iter().all(|m| !m.contains("attachmentMetadata")),
            "expected no metadata error, got {e:?}"
        );
    }

    #[test]
    fn oversized_attachment_metadata_is_too_long() {
        let big = "x".repeat(256 * 1024 + 1);
        let e = errs(va_with_status(serde_json::json!({
            "attached": true,
            "attachmentMetadata": {"k": big}
        })));
        assert!(
            e.iter()
                .any(|m| m.contains("status.attachmentMetadata") && m.contains("more than")),
            "expected too-long metadata error, got {e:?}"
        );
    }

    #[test]
    fn oversized_attach_error_message_is_too_long() {
        let msg = "e".repeat(1025);
        let e = errs(va_with_status(serde_json::json!({
            "attached": false,
            "attachError": {"message": msg}
        })));
        assert!(
            e.iter()
                .any(|m| m.contains("status.attachError.message") && m.contains("more than")),
            "expected too-long attachError message, got {e:?}"
        );
    }

    #[test]
    fn ok_detach_error_message_passes() {
        let msg = "e".repeat(1024);
        let e = errs(va_with_status(serde_json::json!({
            "attached": false,
            "detachError": {"message": msg}
        })));
        assert!(
            e.iter().all(|m| !m.contains("detachError")),
            "expected no detachError error, got {e:?}"
        );
    }

    #[test]
    fn negative_error_code_is_rejected() {
        let e = errs(va_with_status(serde_json::json!({
            "attached": false,
            "attachError": {"message": "boom", "errorCode": -5}
        })));
        assert!(
            e.iter().any(|m| m.contains("status.attachError.errorCode")
                && m.contains("between 0 and 2147483647")),
            "expected negative errorCode error, got {e:?}"
        );
    }

    #[test]
    fn nonnegative_error_code_passes() {
        let e = errs(va_with_status(serde_json::json!({
            "attached": false,
            "detachError": {"message": "boom", "errorCode": 12}
        })));
        assert!(
            e.iter().all(|m| !m.contains("errorCode")),
            "expected no errorCode error, got {e:?}"
        );
    }
}
