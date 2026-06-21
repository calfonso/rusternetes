//! HorizontalPodAutoscaler validation — port of upstream Kubernetes
//! `pkg/apis/autoscaling/validation/validation.go::ValidateHorizontalPodAutoscalerSpec`
//! (release-1.35).
//!
//! Scope: `minReplicas`/`maxReplicas` bounds and `scaleTargetRef`. The per-metric
//! and `behavior` scaling-policy validation are left as a follow-up.

use crate::resources::autoscaling::{HorizontalPodAutoscaler, HorizontalPodAutoscalerSpec};
use crate::validation::field::{Error, ErrorList, Path};

/// Validate a `HorizontalPodAutoscalerSpec`. Mirrors the core of upstream
/// `ValidateHorizontalPodAutoscalerSpec`.
pub fn validate_hpa_spec(spec: &HorizontalPodAutoscalerSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // minReplicas, when set, must be >= 1 (the lower bound without the
    // HPAScaleToZero feature).
    if let Some(min) = spec.min_replicas {
        if min < 1 {
            errs.push(Error::invalid(
                &fld_path.child("minReplicas"),
                min,
                "must be greater than or equal to 1",
            ));
        }
    }

    // maxReplicas must be > 0 and >= minReplicas.
    if spec.max_replicas < 1 {
        errs.push(Error::invalid(
            &fld_path.child("maxReplicas"),
            spec.max_replicas,
            "must be greater than 0",
        ));
    }
    if let Some(min) = spec.min_replicas {
        if spec.max_replicas < min {
            errs.push(Error::invalid(
                &fld_path.child("maxReplicas"),
                spec.max_replicas,
                "must be greater than or equal to `minReplicas`",
            ));
        }
    }

    // scaleTargetRef: kind and name are required.
    let ref_path = fld_path.child("scaleTargetRef");
    if spec.scale_target_ref.kind.is_empty() {
        errs.push(Error::required(&ref_path.child("kind"), ""));
    }
    if spec.scale_target_ref.name.is_empty() {
        errs.push(Error::required(&ref_path.child("name"), ""));
    }

    errs
}

/// Validate a new `HorizontalPodAutoscaler`. Mirrors upstream
/// `ValidateHorizontalPodAutoscaler`.
pub fn validate_horizontal_pod_autoscaler(hpa: &HorizontalPodAutoscaler) -> ErrorList {
    validate_hpa_spec(&hpa.spec, &Path::new("spec"))
}
