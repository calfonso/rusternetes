//! PersistentVolume validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePersistentVolume` (release-1.35).
//!
//! Scope: capacity (storage required + non-negative), access modes (≥1 +
//! ReadWriteOncePod exclusivity), exactly one volume source, and
//! storageClassName. The per-source field validation, nodeAffinity-for-Local
//! requirement, and reclaim-policy/source compatibility are left as a follow-up.

use crate::quantity::Quantity;
use crate::resources::volume::{
    PersistentVolume, PersistentVolumeAccessMode, PersistentVolumeSpec,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;

/// Validate a `PersistentVolumeSpec`. Mirrors the core of upstream
/// `ValidatePersistentVolume`.
pub fn validate_persistent_volume_spec(spec: &PersistentVolumeSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // capacity.storage is required and must be a non-negative quantity.
    let cap_path = fld_path.child("capacity").child("storage");
    match spec.capacity.get("storage") {
        None => errs.push(Error::required(&cap_path, "")),
        Some(v) => match Quantity::parse(v) {
            Err(_) => errs.push(Error::invalid(
                &cap_path,
                v.clone(),
                "must be a valid resource quantity",
            )),
            Ok(q) => {
                if q.is_negative() {
                    errs.push(Error::invalid(
                        &cap_path,
                        v.clone(),
                        "must be greater than or equal to 0",
                    ));
                }
            }
        },
    }

    // accessModes: at least one; ReadWriteOncePod may not combine with others.
    if spec.access_modes.is_empty() {
        errs.push(Error::required(
            &fld_path.child("accessModes"),
            "at least 1 access mode is required",
        ));
    }
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

    // Exactly one volume source must be specified.
    let num_volumes = [
        spec.host_path.is_some(),
        spec.nfs.is_some(),
        spec.iscsi.is_some(),
        spec.local.is_some(),
        spec.csi.is_some(),
    ]
    .iter()
    .filter(|x| **x)
    .count();
    if num_volumes == 0 {
        errs.push(Error::required(fld_path, "must specify a volume type"));
    } else if num_volumes > 1 {
        errs.push(Error::forbidden(
            fld_path,
            "may not specify more than 1 volume type",
        ));
    }

    // storageClassName, when set, must be a DNS-1123 subdomain.
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

/// Validate a new `PersistentVolume`. Mirrors upstream `ValidatePersistentVolume`.
pub fn validate_persistent_volume(pv: &PersistentVolume) -> ErrorList {
    validate_persistent_volume_spec(&pv.spec, &Path::new("spec"))
}
