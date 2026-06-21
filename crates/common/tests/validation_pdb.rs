//! Tests for PodDisruptionBudget field validation.
//!
//! Mirrors upstream `ValidatePodDisruptionBudgetSpec`
//! (`pkg/apis/policy/validation/validation.go`, release-1.35).

use rusternetes_common::resources::policy::PodDisruptionBudget;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::pdb::validate_pod_disruption_budget;
use serde_json::json;

fn pdb(spec: serde_json::Value) -> PodDisruptionBudget {
    serde_json::from_value(json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {"name": "pdb", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn min_available_int_passes() {
    let errs = validate_pod_disruption_budget(&pdb(json!({
        "minAvailable": 2,
        "selector": {"matchLabels": {"app": "web"}}
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn max_unavailable_percent_passes() {
    let errs = validate_pod_disruption_budget(&pdb(json!({
        "maxUnavailable": "30%",
        "selector": {"matchLabels": {"app": "web"}}
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn both_min_and_max_rejected() {
    let errs = validate_pod_disruption_budget(&pdb(json!({
        "minAvailable": 1,
        "maxUnavailable": 1,
        "selector": {"matchLabels": {"app": "web"}}
    })));
    assert!(has(&errs, "spec", ErrorType::Invalid), "got: {errs:?}");
}

#[test]
fn percent_over_100_rejected() {
    let errs = validate_pod_disruption_budget(&pdb(json!({
        "minAvailable": "150%",
        "selector": {"matchLabels": {"app": "web"}}
    })));
    assert!(
        has(&errs, "spec.minAvailable", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn negative_int_rejected() {
    let errs = validate_pod_disruption_budget(&pdb(json!({
        "maxUnavailable": -1,
        "selector": {"matchLabels": {"app": "web"}}
    })));
    assert!(
        has(&errs, "spec.maxUnavailable", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn bad_unhealthy_pod_eviction_policy_rejected() {
    let errs = validate_pod_disruption_budget(&pdb(json!({
        "minAvailable": 1,
        "selector": {"matchLabels": {"app": "web"}},
        "unhealthyPodEvictionPolicy": "Bogus"
    })));
    assert!(
        has(
            &errs,
            "spec.unhealthyPodEvictionPolicy",
            ErrorType::NotSupported
        ),
        "got: {errs:?}"
    );
}

#[test]
fn valid_unhealthy_pod_eviction_policy_passes() {
    let errs = validate_pod_disruption_budget(&pdb(json!({
        "minAvailable": 1,
        "selector": {"matchLabels": {"app": "web"}},
        "unhealthyPodEvictionPolicy": "AlwaysAllow"
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}
