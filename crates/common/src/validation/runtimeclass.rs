//! RuntimeClass validation — port of upstream Kubernetes
//! `pkg/apis/node/validation/validation.go::ValidateRuntimeClass` (release-1.35).
//!
//! Covers `handler` (must be a DNS-1123 label), `overhead.podFixed` (valid
//! resource names + non-negative quantities, reusing the container-resource
//! checks), and `scheduling` (nodeSelector labels + tolerations). ObjectMeta is
//! validated separately (#1087 / #1277).

use crate::quantity::Quantity;
use crate::resources::runtimeclass::{Overhead, Scheduling};
use crate::resources::RuntimeClass;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_qualified_name, validate_labels};
use crate::validation::pod::validate_tolerations;

/// Port of upstream `validateOverhead` — `podFixed` is the `Limits` of a
/// container `ResourceRequirements`: each key a valid resource name, each value
/// a parseable, non-negative quantity.
fn validate_overhead(overhead: &Overhead, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(pod_fixed) = &overhead.pod_fixed else {
        return errs;
    };
    let pf_path = fld_path.child("podFixed");
    for (name, value) in pod_fixed {
        let kpath = pf_path.child(name);
        for msg in is_qualified_name(name) {
            errs.push(Error::invalid(&kpath, name.clone(), msg));
        }
        match Quantity::parse(value) {
            Ok(q) => {
                if q.is_negative() {
                    errs.push(Error::invalid(
                        &kpath,
                        value.clone(),
                        "must be greater than or equal to 0",
                    ));
                }
            }
            Err(e) => errs.push(Error::invalid(&kpath, value.clone(), e.to_string())),
        }
    }
    errs
}

/// Port of upstream `validateScheduling` — nodeSelector labels + tolerations.
fn validate_scheduling(scheduling: &Scheduling, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(node_selector) = &scheduling.node_selector {
        errs.extend(validate_labels(
            node_selector,
            &fld_path.child("nodeSelector"),
        ));
    }
    if let Some(tolerations) = &scheduling.tolerations {
        errs.extend(validate_tolerations(
            tolerations,
            &fld_path.child("tolerations"),
        ));
    }
    errs
}

/// Validate a `RuntimeClass` on create. Mirrors upstream `ValidateRuntimeClass`
/// minus ObjectMeta.
pub fn validate_runtime_class(rc: &RuntimeClass) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // handler must be a DNS-1123 label (also rejects empty).
    for msg in is_dns1123_label(&rc.handler) {
        errs.push(Error::invalid(
            &Path::new("handler"),
            rc.handler.clone(),
            msg,
        ));
    }

    if let Some(overhead) = &rc.overhead {
        errs.extend(validate_overhead(overhead, &Path::new("overhead")));
    }
    if let Some(scheduling) = &rc.scheduling {
        errs.extend(validate_scheduling(scheduling, &Path::new("scheduling")));
    }

    errs
}
