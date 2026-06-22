//! Tests for ResourceQuota field validation.
//!
//! Mirrors upstream `ValidateResourceQuotaSpec`
//! (`pkg/apis/core/validation/validation.go`, release-1.35).

use rusternetes_common::resources::policy::ResourceQuota;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::resourcequota::validate_resource_quota;
use serde_json::json;

fn rq(spec: serde_json::Value) -> ResourceQuota {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "q", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_quota_passes() {
    let errs = validate_resource_quota(&rq(json!({
        "hard": {"cpu": "10", "memory": "20Gi", "pods": "100"},
        "scopes": ["BestEffort"]
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn negative_hard_quantity_rejected() {
    let errs = validate_resource_quota(&rq(json!({"hard": {"cpu": "-1"}})));
    // Upstream keys map entries with field.Key, rendering `spec.hard[cpu]`.
    assert!(
        has(&errs, "spec.hard[cpu]", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

// Note: a syntactically-invalid hard quantity (e.g. "lots") is rejected at
// deserialization, before this validator runs — so the `Quantity::parse` Err
// arm is a backstop the API path can't reach (not unit-tested via `from_value`).

#[test]
fn unsupported_scope_rejected() {
    let errs = validate_resource_quota(&rq(json!({"hard": {"pods": "1"}, "scopes": ["Bogus"]})));
    assert!(
        has(&errs, "spec.scopes", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn conflicting_scopes_rejected() {
    let errs = validate_resource_quota(&rq(json!({
        "hard": {"pods": "1"},
        "scopes": ["BestEffort", "NotBestEffort"]
    })));
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.scopes" && e.detail == "conflicting scopes"),
        "got: {errs:?}"
    );
}

#[test]
fn terminating_pair_conflict_rejected() {
    let errs = validate_resource_quota(&rq(json!({
        "scopes": ["Terminating", "NotTerminating"]
    })));
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.scopes" && e.detail == "conflicting scopes"),
        "got: {errs:?}"
    );
}

#[test]
fn priority_class_scope_ok() {
    let errs = validate_resource_quota(&rq(json!({
        "hard": {"pods": "5"},
        "scopes": ["PriorityClass"]
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}
