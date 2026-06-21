//! StorageClass validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateStorageClass` (release-1.35).
//!
//! Covers `provisioner` (required + qualified name), `parameters` (count/size
//! caps + non-empty keys), `reclaimPolicy` ({Delete, Retain}) and
//! `volumeBindingMode` ({Immediate, WaitForFirstConsumer}).
//!
//! ObjectMeta is validated separately by the handler (#1087 / #1277).

use crate::resources::volume::{
    PersistentVolumeReclaimPolicy, StorageClass, TopologySelectorTerm, VolumeBindingMode,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_qualified_name, validate_label_name};
use std::collections::{BTreeMap, BTreeSet};

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

/// Port of upstream `ValidateTopologySelectorTerm`: validates each
/// `matchLabelExpressions` requirement and rejects duplicate keys within the
/// term. Returns the term's normalized `key -> {values}` map so callers can
/// detect duplicate terms.
fn validate_topology_selector_term(
    term: &TopologySelectorTerm,
    fld_path: &Path,
) -> (BTreeMap<String, BTreeSet<String>>, ErrorList) {
    let mut errs: ErrorList = Vec::new();
    let mut expr_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let expr_path = fld_path.child("matchLabelExpressions");

    if let Some(reqs) = &term.match_label_expressions {
        for (i, req) in reqs.iter().enumerate() {
            let idx_path = expr_path.index(i);
            let values_path = idx_path.child("values");

            if req.values.is_empty() {
                errs.push(Error::required(&values_path, ""));
            }
            let mut value_set: BTreeSet<String> = BTreeSet::new();
            for (j, value) in req.values.iter().enumerate() {
                if !value_set.insert(value.clone()) {
                    errs.push(Error::duplicate(&values_path.index(j), value.clone()));
                }
            }
            errs.extend(validate_label_name(&req.key, &idx_path.child("key")));

            // Duplicate key within this term.
            if expr_map.contains_key(&req.key) {
                errs.push(Error::duplicate(&idx_path.child("key"), req.key.clone()));
            }
            expr_map.insert(req.key.clone(), value_set);
        }
    }

    (expr_map, errs)
}

/// Validate a `StorageClass` on create. Mirrors upstream `ValidateStorageClass`
/// minus ObjectMeta.
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

    // allowedTopologies: validate each term, and reject duplicate terms
    // (upstream validateAllowedTopologies).
    if let Some(topologies) = &sc.allowed_topologies {
        let at_path = Path::new("allowedTopologies");
        let mut seen: Vec<BTreeMap<String, BTreeSet<String>>> = Vec::new();
        for (i, term) in topologies.iter().enumerate() {
            let idx_path = at_path.index(i);
            let (expr_map, term_errs) = validate_topology_selector_term(term, &idx_path);
            errs.extend(term_errs);
            if seen.contains(&expr_map) {
                errs.push(Error::duplicate(
                    &idx_path.child("matchLabelExpressions"),
                    "",
                ));
            }
            seen.push(expr_map);
        }
    }

    errs
}
