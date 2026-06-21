//! Tests for HorizontalPodAutoscaler field validation.
//!
//! Mirrors the core of upstream `ValidateHorizontalPodAutoscalerSpec`
//! (`pkg/apis/autoscaling/validation/validation.go`, release-1.35).

use rusternetes_common::resources::autoscaling::HorizontalPodAutoscaler;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::hpa::validate_horizontal_pod_autoscaler;
use serde_json::json;

fn hpa(spec: serde_json::Value) -> HorizontalPodAutoscaler {
    serde_json::from_value(json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {"name": "hpa", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

fn target() -> serde_json::Value {
    json!({"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"})
}

#[test]
fn valid_hpa_passes() {
    let errs = validate_horizontal_pod_autoscaler(&hpa(json!({
        "scaleTargetRef": target(),
        "minReplicas": 1,
        "maxReplicas": 10
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn min_replicas_below_one_rejected() {
    let errs = validate_horizontal_pod_autoscaler(&hpa(json!({
        "scaleTargetRef": target(),
        "minReplicas": 0,
        "maxReplicas": 10
    })));
    assert!(
        has(&errs, "spec.minReplicas", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn max_replicas_zero_rejected() {
    let errs = validate_horizontal_pod_autoscaler(&hpa(json!({
        "scaleTargetRef": target(),
        "maxReplicas": 0
    })));
    assert!(
        has(&errs, "spec.maxReplicas", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn max_below_min_rejected() {
    let errs = validate_horizontal_pod_autoscaler(&hpa(json!({
        "scaleTargetRef": target(),
        "minReplicas": 5,
        "maxReplicas": 3
    })));
    assert!(
        has(&errs, "spec.maxReplicas", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn missing_scale_target_kind_and_name_required() {
    let errs = validate_horizontal_pod_autoscaler(&hpa(json!({
        "scaleTargetRef": {"kind": "", "name": ""},
        "maxReplicas": 5
    })));
    assert!(
        has(&errs, "spec.scaleTargetRef.kind", ErrorType::Required)
            && has(&errs, "spec.scaleTargetRef.name", ErrorType::Required),
        "got: {errs:?}"
    );
}
