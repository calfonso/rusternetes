//! VolumeAttributesClass validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateVolumeAttributesClass`
//! (release-1.35).
//!
//! `driverName` and `parameters` reuse the exact StorageClass helpers
//! (`validateProvisioner` / `validateParameters`) upstream shares — except
//! `parameters` is validated with `allowEmpty=false` (at least one entry
//! required). ObjectMeta is validated separately (#1087 / #1277). CSI is a
//! non-negotiable contract.

use crate::resources::csi::VolumeAttributesClass;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::storageclass::{validate_parameters, validate_provisioner};

/// Validate a `VolumeAttributesClass` on create. Mirrors upstream
/// `ValidateVolumeAttributesClass` minus ObjectMeta.
pub fn validate_volume_attributes_class(vac: &VolumeAttributesClass) -> ErrorList {
    let mut errs = validate_provisioner(&vac.driver_name, &Path::new("driverName"));
    errs.extend(validate_parameters(
        vac.parameters.as_ref(),
        false, // allowEmpty=false: parameters must contain at least one pair
        &Path::new("parameters"),
    ));
    errs
}

/// Validate a VolumeAttributesClass update — upstream
/// `ValidateVolumeAttributesClassUpdate` (pkg/apis/storage/validation):
/// `driverName` and `parameters` are immutable, plus full re-validation.
pub fn validate_volume_attributes_class_update(
    new_vac: &VolumeAttributesClass,
    old_vac: &VolumeAttributesClass,
) -> ErrorList {
    let mut errs = ErrorList::new();
    if new_vac.driver_name != old_vac.driver_name {
        errs.push(Error::forbidden(
            &Path::new("driverName"),
            "updates to driverName are forbidden.",
        ));
    }
    if new_vac.parameters != old_vac.parameters {
        errs.push(Error::forbidden(
            &Path::new("parameters"),
            "updates to parameters are forbidden.",
        ));
    }
    errs.extend(validate_volume_attributes_class(new_vac));
    errs
}
