//! StorageClass validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateStorageClass` (release-1.35).
//!
//! Covers `provisioner` (required + qualified name), `parameters` (count/size
//! caps + non-empty keys), `reclaimPolicy` ({Delete, Retain}) and
//! `volumeBindingMode` ({Immediate, WaitForFirstConsumer}).
//!
//! ObjectMeta is validated separately by the handler (#1087 / #1277).
//! `allowedTopologies` (`ValidateTopologySelectorTerm`) is out of scope here.

use crate::resources::volume::{PersistentVolumeReclaimPolicy, StorageClass, VolumeBindingMode};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_qualified_name;

// Upstream constants (`pkg/apis/storage/validation/validation.go`).
const MAX_PROVISIONER_PARAMETER_LEN: usize = 512;
const MAX_PROVISIONER_PARAMETER_SIZE: usize = 256 * 1024;

/// Port of upstream `validateProvisioner`: required, and (lowercased) a valid
/// qualified name.
fn validate_provisioner(provisioner: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if provisioner.is_empty() {
        errs.push(Error::required(fld_path, provisioner.to_string()));
    } else {
        for msg in is_qualified_name(&provisioner.to_lowercase()) {
            errs.push(Error::invalid(fld_path, provisioner.to_string(), msg));
        }
    }
    errs
}

/// Port of upstream `validateParameters` with `allowEmpty = true`.
fn validate_parameters(
    params: Option<&std::collections::HashMap<String, String>>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(params) = params else {
        return errs;
    };
    if params.len() > MAX_PROVISIONER_PARAMETER_LEN {
        errs.push(Error::too_long(fld_path, MAX_PROVISIONER_PARAMETER_LEN));
        return errs;
    }
    let mut total_size: usize = 0;
    for (k, v) in params {
        if k.is_empty() {
            errs.push(Error::invalid(
                fld_path,
                k.clone(),
                "field can not be empty.",
            ));
        }
        total_size += k.len() + v.len();
    }
    if total_size > MAX_PROVISIONER_PARAMETER_SIZE {
        errs.push(Error::too_long(fld_path, MAX_PROVISIONER_PARAMETER_SIZE));
    }
    errs
}

/// Validate a `StorageClass` on create. Mirrors upstream `ValidateStorageClass`
/// minus ObjectMeta and `allowedTopologies`.
pub fn validate_storage_class(sc: &StorageClass) -> ErrorList {
    let mut errs = validate_provisioner(&sc.provisioner, &Path::new("provisioner"));
    errs.extend(validate_parameters(
        sc.parameters.as_ref(),
        &Path::new("parameters"),
    ));

    // reclaimPolicy: only Delete and Retain are valid for a StorageClass
    // (Recycle is rejected). Empty is allowed (defaulted to Delete upstream).
    if let Some(rp) = &sc.reclaim_policy {
        match rp {
            PersistentVolumeReclaimPolicy::Delete | PersistentVolumeReclaimPolicy::Retain => {}
            PersistentVolumeReclaimPolicy::Recycle => {
                errs.push(Error::not_supported(
                    &Path::new("reclaimPolicy"),
                    "Recycle",
                    &["Delete", "Retain"],
                ));
            }
        }
    }

    // volumeBindingMode is required (defaulted to Immediate upstream). The Rust
    // enum only admits the two valid variants, so the sole check is presence.
    match &sc.volume_binding_mode {
        None => errs.push(Error::required(&Path::new("volumeBindingMode"), "")),
        Some(VolumeBindingMode::Immediate | VolumeBindingMode::WaitForFirstConsumer) => {}
    }

    errs
}
