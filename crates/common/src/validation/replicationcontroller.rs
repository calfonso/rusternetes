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

    // Upstream `ValidateReplicationControllerSpec` (validation.go:7056-7060):
    // `replicas` is required, then must be non-negative.
    match spec.replicas {
        None => {
            errs.push(Error::required(&fld_path.child("replicas"), ""));
        }
        Some(r) if r < 0 => {
            errs.push(Error::invalid(
                &fld_path.child("replicas"),
                r,
                "must be greater than or equal to 0",
            ));
        }
        Some(_) => {}
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

    // Upstream `ValidatePodTemplateSpecForRC` (validation.go:7041-7046): the RC
    // pod template must use `restartPolicy: Always`, and `activeDeadlineSeconds`
    // is forbidden.
    let template_path = fld_path.child("template").child("spec");
    let restart_policy = spec.template.spec.restart_policy.as_deref();
    // An absent restartPolicy defaults to Always upstream; only a present,
    // non-Always value is rejected here (defaulting runs before validation).
    if let Some(rp) = restart_policy {
        if rp != "Always" {
            errs.push(Error::not_supported(
                &template_path.child("restartPolicy"),
                rp.to_string(),
                &["Always"],
            ));
        }
    }
    if spec.template.spec.active_deadline_seconds.is_some() {
        errs.push(Error::forbidden(
            &template_path.child("activeDeadlineSeconds"),
            "activeDeadlineSeconds in ReplicationController is not Supported",
        ));
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
