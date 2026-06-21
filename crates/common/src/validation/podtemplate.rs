//! PodTemplate validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePodTemplate` /
//! `ValidatePodTemplateSpec` (release-1.35).
//!
//! Validates the embedded `template`: its labels, annotations, and the pod spec
//! (reusing the shared [`validate_pod_spec`], which also forbids ephemeral
//! containers on create — upstream forbids them in a pod template too).
//! ObjectMeta of the PodTemplate itself is validated separately (#1087 / #1277).

use crate::resources::workloads::PodTemplate;
use crate::validation::field::{ErrorList, Path};
use crate::validation::metav1::validate_labels;
use crate::validation::objectmeta::validate_annotations;
use crate::validation::pod::validate_pod_spec;

/// Validate a `PodTemplate` on create. Mirrors upstream `ValidatePodTemplate`
/// minus the PodTemplate's own ObjectMeta.
pub fn validate_pod_template(pt: &PodTemplate) -> ErrorList {
    let tpath = Path::new("template");
    let mut errs: ErrorList = Vec::new();

    if let Some(meta) = &pt.template.metadata {
        if let Some(labels) = &meta.labels {
            errs.extend(validate_labels(labels, &tpath.child("labels")));
        }
        if let Some(annotations) = &meta.annotations {
            errs.extend(validate_annotations(
                annotations,
                &tpath.child("annotations"),
            ));
        }
    }

    // Pod spec (also forbids ephemeral containers — upstream forbids them in a
    // pod template). `allow_relaxed_dns_search` defaults off.
    errs.extend(validate_pod_spec(
        &pt.template.spec,
        &tpath.child("spec"),
        false,
    ));

    errs
}
