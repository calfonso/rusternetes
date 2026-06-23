//! Tests for LimitRange field validation.
//!
//! Mirrors upstream `ValidateLimitRange`
//! (`pkg/apis/core/validation/validation.go`, release-1.35).

use rusternetes_common::resources::policy::LimitRange;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::limitrange::validate_limit_range;
use serde_json::json;

fn lr(limits: serde_json::Value) -> LimitRange {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "lr", "namespace": "default"},
        "spec": {"limits": limits}
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_container_limits_pass() {
    let errs = validate_limit_range(&lr(json!([{
        "type": "Container",
        "min": {"cpu": "100m"},
        "default": {"cpu": "500m"},
        "defaultRequest": {"cpu": "200m"},
        "max": {"cpu": "1"}
    }])));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn unknown_type_not_supported() {
    let errs = validate_limit_range(&lr(json!([{"type": "Bogus", "max": {"cpu": "1"}}])));
    assert!(
        has(&errs, "spec.limits[0].type", ErrorType::NotSupported),
        "got: {errs:?}"
    );
}

#[test]
fn duplicate_type_rejected() {
    let errs = validate_limit_range(&lr(json!([
        {"type": "Container", "max": {"cpu": "1"}},
        {"type": "Container", "max": {"cpu": "2"}}
    ])));
    assert!(
        has(&errs, "spec.limits[1].type", ErrorType::Duplicate),
        "got: {errs:?}"
    );
}

#[test]
fn pod_type_forbids_default() {
    let errs = validate_limit_range(&lr(json!([{
        "type": "Pod",
        "max": {"cpu": "1"},
        "default": {"cpu": "500m"}
    }])));
    assert!(
        has(&errs, "spec.limits[0].default", ErrorType::Forbidden),
        "got: {errs:?}"
    );
}

#[test]
fn min_greater_than_max_rejected() {
    let errs = validate_limit_range(&lr(json!([{
        "type": "Container",
        "min": {"cpu": "2"},
        "max": {"cpu": "1"}
    }])));
    assert!(
        has(&errs, "spec.limits[0].min[cpu]", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn default_above_max_rejected() {
    let errs = validate_limit_range(&lr(json!([{
        "type": "Container",
        "max": {"cpu": "1"},
        "default": {"cpu": "2"}
    }])));
    assert!(
        has(&errs, "spec.limits[0].default[cpu]", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn default_request_above_default_rejected() {
    let errs = validate_limit_range(&lr(json!([{
        "type": "Container",
        "default": {"cpu": "500m"},
        "defaultRequest": {"cpu": "800m"}
    }])));
    assert!(
        has(
            &errs,
            "spec.limits[0].defaultRequest[cpu]",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

#[test]
fn pvc_requires_min_or_max_storage() {
    let errs = validate_limit_range(&lr(json!([{"type": "PersistentVolumeClaim"}])));
    assert!(
        has(&errs, "spec.limits[0].limits", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn pvc_with_max_storage_ok() {
    let errs = validate_limit_range(&lr(json!([{
        "type": "PersistentVolumeClaim",
        "max": {"storage": "10Gi"}
    }])));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn ratio_below_one_rejected() {
    let errs = validate_limit_range(&lr(json!([{
        "type": "Container",
        "maxLimitRequestRatio": {"cpu": "0"}
    }])));
    assert!(
        has(
            &errs,
            "spec.limits[0].maxLimitRequestRatio[cpu]",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}
