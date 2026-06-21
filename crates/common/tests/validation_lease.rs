//! Tests for Lease validation (port of upstream coordination ValidateLeaseSpec).

use rusternetes_common::resources::coordination::{Lease, LeaseSpec};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::lease::validate_lease;

fn spec() -> LeaseSpec {
    LeaseSpec {
        holder_identity: Some("holder-1".to_string()),
        lease_duration_seconds: Some(15),
        acquire_time: None,
        renew_time: None,
        lease_transitions: Some(0),
        preferred_holder: None,
        strategy: None,
    }
}

fn lease(spec: Option<LeaseSpec>) -> Lease {
    let mut l = Lease {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec,
    };
    l.metadata.name = "my-lease".to_string();
    l
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field))
}

#[test]
fn valid_lease_passes() {
    assert!(validate_lease(&lease(Some(spec()))).is_empty());
}

#[test]
fn no_spec_ok() {
    assert!(validate_lease(&lease(None)).is_empty());
}

#[test]
fn nonpositive_duration_rejected() {
    let mut s = spec();
    s.lease_duration_seconds = Some(0);
    assert!(has(
        &validate_lease(&lease(Some(s))),
        "spec.leaseDurationSeconds"
    ));
}

#[test]
fn negative_transitions_rejected() {
    let mut s = spec();
    s.lease_transitions = Some(-1);
    assert!(has(
        &validate_lease(&lease(Some(s))),
        "spec.leaseTransitions"
    ));
}

#[test]
fn valid_builtin_strategy_ok() {
    let mut s = spec();
    s.strategy = Some("OldestEmulationVersion".to_string());
    assert!(validate_lease(&lease(Some(s))).is_empty());
}

#[test]
fn unknown_builtin_strategy_rejected() {
    let mut s = spec();
    s.strategy = Some("MyStrategy".to_string());
    let errs = validate_lease(&lease(Some(s)));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.strategy" && e.error_type == ErrorType::NotSupported));
}

#[test]
fn qualified_strategy_ok() {
    let mut s = spec();
    s.strategy = Some("example.com/my-strategy".to_string());
    assert!(validate_lease(&lease(Some(s))).is_empty());
}

#[test]
fn invalid_qualified_strategy_rejected() {
    let mut s = spec();
    s.strategy = Some("bad prefix/name".to_string());
    assert!(has(&validate_lease(&lease(Some(s))), "spec.strategy"));
}

#[test]
fn preferred_holder_without_strategy_forbidden() {
    let mut s = spec();
    s.preferred_holder = Some("other".to_string());
    s.strategy = None;
    let errs = validate_lease(&lease(Some(s)));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.preferredHolder" && e.error_type == ErrorType::Forbidden));
}

#[test]
fn preferred_holder_with_strategy_ok() {
    let mut s = spec();
    s.preferred_holder = Some("other".to_string());
    s.strategy = Some("OldestEmulationVersion".to_string());
    assert!(validate_lease(&lease(Some(s))).is_empty());
}
