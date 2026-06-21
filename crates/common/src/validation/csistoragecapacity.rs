//! CSIStorageCapacity validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateCSIStorageCapacity`
//! (release-1.35).
//!
//! Covers `nodeTopology` (a label selector), `storageClassName` (DNS-subdomain
//! class name), and `capacity` (a non-negative quantity). ObjectMeta is
//! validated separately (#1087 / #1277). CSI is a non-negotiable contract.

use crate::quantity::Quantity;
use crate::resources::csi::CSIStorageCapacity;
use crate::resources::volume::LabelSelector as VolumeLabelSelector;
use crate::types::{LabelSelector, LabelSelectorRequirement};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_subdomain, validate_label_selector, LabelSelectorValidationOptions,
};

/// The `nodeTopology` field uses the `volume` crate's structurally-identical
/// `LabelSelector`; convert it to the `types` one the metav1 validator expects.
fn to_types_selector(sel: &VolumeLabelSelector) -> LabelSelector {
    LabelSelector {
        match_labels: sel.match_labels.clone(),
        match_expressions: sel.match_expressions.as_ref().map(|reqs| {
            reqs.iter()
                .map(|r| LabelSelectorRequirement {
                    key: r.key.clone(),
                    operator: r.operator.clone(),
                    values: r.values.clone(),
                })
                .collect()
        }),
    }
}

/// Validate a `CSIStorageCapacity` on create. Mirrors upstream
/// `ValidateCSIStorageCapacity` minus ObjectMeta.
pub fn validate_csi_storage_capacity(csc: &CSIStorageCapacity) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(topology) = &csc.node_topology {
        errs.extend(validate_label_selector(
            &to_types_selector(topology),
            LabelSelectorValidationOptions::default(),
            &Path::new("nodeTopology"),
        ));
    }

    // storageClassName — a DNS-subdomain class name (also rejects empty).
    for msg in is_dns1123_subdomain(&csc.storage_class_name) {
        errs.push(Error::invalid(
            &Path::new("storageClassName"),
            csc.storage_class_name.clone(),
            msg,
        ));
    }

    // capacity — a non-negative quantity when present.
    if let Some(capacity) = &csc.capacity {
        let cap_path = Path::new("capacity");
        match Quantity::parse(capacity) {
            Ok(q) => {
                if q.is_negative() {
                    errs.push(Error::invalid(
                        &cap_path,
                        capacity.clone(),
                        "must be greater than or equal to 0",
                    ));
                }
            }
            Err(e) => errs.push(Error::invalid(&cap_path, capacity.clone(), e.to_string())),
        }
    }

    errs
}
