//! PersistentVolumeClaim validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePersistentVolumeClaimSpec`
//! (release-1.35).
//!
//! Scope: the field-level checks that don't need cluster state — access modes,
//! the storage request, and storageClassName. `dataSource`/`dataSourceRef`
//! consistency, `volumeAttributesClassName`, and the `selector` (a distinct
//! `volume::LabelSelector` here) are left as a follow-up.

use crate::quantity::Quantity;
use crate::resources::volume::{
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimSpec,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;

/// Validate a `PersistentVolumeClaimSpec`. Mirrors upstream
/// `ValidatePersistentVolumeClaimSpec`.
pub fn validate_persistent_volume_claim_spec(
    spec: &PersistentVolumeClaimSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // accessModes: at least one is required. (Individual values are an enum, so
    // their validity is enforced at deserialization.)
    if spec.access_modes.is_empty() {
        errs.push(Error::required(
            &fld_path.child("accessModes"),
            "at least 1 access mode is required",
        ));
    }
    // ReadWriteOncePod may not be combined with any other access mode.
    let has_rwop = spec
        .access_modes
        .iter()
        .any(|m| matches!(m, PersistentVolumeAccessMode::ReadWriteOncePod));
    let has_other = spec
        .access_modes
        .iter()
        .any(|m| !matches!(m, PersistentVolumeAccessMode::ReadWriteOncePod));
    if has_rwop && has_other {
        errs.push(Error::forbidden(
            &fld_path.child("accessModes"),
            "may not use ReadWriteOncePod with other access modes",
        ));
    }

    // resources.requests[storage] is required and must be a positive quantity.
    let storage_path = fld_path
        .child("resources")
        .child("requests")
        .child("storage");
    match spec
        .resources
        .requests
        .as_ref()
        .and_then(|r| r.get("storage"))
    {
        None => errs.push(Error::required(&storage_path, "")),
        Some(val) => match Quantity::parse(val) {
            Err(_) => errs.push(Error::invalid(
                &storage_path,
                val.clone(),
                "must be a valid resource quantity",
            )),
            Ok(q) => {
                if q.is_negative() || q.is_zero() {
                    errs.push(Error::invalid(
                        &storage_path,
                        val.clone(),
                        "must be greater than 0",
                    ));
                }
            }
        },
    }

    // storageClassName, when set, must be a DNS-1123 subdomain (upstream
    // `ValidateClassName`).
    if let Some(scn) = &spec.storage_class_name {
        if !scn.is_empty() {
            for msg in is_dns1123_subdomain(scn) {
                errs.push(Error::invalid(
                    &fld_path.child("storageClassName"),
                    scn.clone(),
                    msg,
                ));
            }
        }
    }

    errs
}

/// Validate a new `PersistentVolumeClaim`. Mirrors upstream
/// `ValidatePersistentVolumeClaim`.
pub fn validate_persistent_volume_claim(pvc: &PersistentVolumeClaim) -> ErrorList {
    validate_persistent_volume_claim_spec(&pvc.spec, &Path::new("spec"))
}

/// Validate a `PersistentVolumeClaim` update. Ports the safe, conformance-tested
/// immutability subset of upstream `ValidatePersistentVolumeClaimUpdate`:
/// `volumeMode` is immutable, and the storage request may not shrink. The broad
/// "spec is immutable except resources.requests" deep-equal check (which needs
/// careful per-field normalization to avoid false positives on binding) is left
/// as a follow-up.
pub fn validate_persistent_volume_claim_update(
    new_pvc: &PersistentVolumeClaim,
    old_pvc: &PersistentVolumeClaim,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // volumeMode is immutable.
    if new_pvc.spec.volume_mode != old_pvc.spec.volume_mode {
        errs.push(Error::forbidden(
            &Path::new("volumeMode"),
            "field is immutable",
        ));
    }

    // resources.requests["storage"] may not decrease (Kubernetes supports growth
    // only, not shrinking).
    let storage = |spec: &PersistentVolumeClaimSpec| -> Option<String> {
        spec.resources
            .requests
            .as_ref()
            .and_then(|m| m.get("storage"))
            .cloned()
    };
    if let (Some(old_s), Some(new_s)) = (storage(&old_pvc.spec), storage(&new_pvc.spec)) {
        if let (Ok(o), Ok(n)) = (Quantity::parse(&old_s), Quantity::parse(&new_s)) {
            if n.cmp_value(&o) == std::cmp::Ordering::Less {
                errs.push(Error::forbidden(
                    &Path::new("spec")
                        .child("resources")
                        .child("requests")
                        .child("storage"),
                    "field can not be less than previous value",
                ));
            }
        }
    }

    errs
}
