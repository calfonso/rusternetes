//! ReplicationController (core/v1) validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateReplicationControllerSpec`
//! (release-1.35).
//!
//! Intended to run *after* defaulting (the api-server defaults an absent
//! `selector` from the template labels), matching upstream where validation
//! sees the defaulted object.

use crate::resources::workloads::ReplicationControllerSpec;
use crate::validation::field::{Error, ErrorList, Path};

/// Validate a `ReplicationControllerSpec`. Mirrors upstream
/// `ValidateReplicationControllerSpec`: non-negative `replicas` /
/// `minReadySeconds`, a non-empty `selector`, and template labels that satisfy
/// it.
pub fn validate_replication_controller_spec(
    spec: &ReplicationControllerSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(r) = spec.replicas {
        if r < 0 {
            errs.push(Error::invalid(
                &fld_path.child("replicas"),
                r,
                "must be greater than or equal to 0",
            ));
        }
    }
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    let selector_empty = spec.selector.as_ref().is_none_or(|s| s.is_empty());
    if selector_empty {
        errs.push(Error::required(&fld_path.child("selector"), ""));
    } else {
        let selector = spec.selector.as_ref().unwrap();
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        let matches = selector
            .iter()
            .all(|(k, v)| template_labels.get(k) == Some(v));
        if !matches {
            errs.push(Error::invalid(
                &fld_path.child("template").child("metadata").child("labels"),
                template_labels
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(","),
                "`selector` does not match template `labels`",
            ));
        }
    }

    errs
}

/// Validate a new `ReplicationController`. Mirrors upstream
/// `ValidateReplicationController`. Run after defaulting.
pub fn validate_replication_controller(
    rc: &crate::resources::workloads::ReplicationController,
) -> ErrorList {
    validate_replication_controller_spec(&rc.spec, &Path::new("spec"))
}
