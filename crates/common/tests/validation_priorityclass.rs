//! Tests for PriorityClass validation (port of upstream `ValidatePriorityClass`).

use rusternetes_common::resources::PriorityClass;
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::priorityclass::validate_priority_class;

fn pc(name: &str, value: i32) -> PriorityClass {
    let mut p = PriorityClass {
        type_meta: Default::default(),
        metadata: Default::default(),
        value,
        global_default: None,
        description: None,
        preemption_policy: None,
    };
    p.metadata.name = name.to_string();
    p
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field == field)
}

#[test]
fn valid_user_priority_class_passes() {
    assert!(validate_priority_class(&pc("high", 1000)).is_empty());
}

#[test]
fn user_value_at_cap_ok() {
    assert!(validate_priority_class(&pc("max", 1_000_000_000)).is_empty());
}

#[test]
fn user_value_above_cap_rejected() {
    let errs = validate_priority_class(&pc("toobig", 1_000_000_001));
    assert!(errs
        .iter()
        .any(|e| e.field == "value" && e.error_type == ErrorType::Forbidden));
}

#[test]
fn unknown_system_prefix_rejected() {
    let errs = validate_priority_class(&pc("system-made-up", 5));
    assert!(errs
        .iter()
        .any(|e| e.field == "metadata.name" && e.error_type == ErrorType::Forbidden));
}

#[test]
fn known_system_class_with_correct_value_ok() {
    assert!(validate_priority_class(&pc("system-cluster-critical", 2_000_000_000)).is_empty());
    assert!(validate_priority_class(&pc("system-node-critical", 2_000_001_000)).is_empty());
}

#[test]
fn known_system_class_with_wrong_value_rejected() {
    let errs = validate_priority_class(&pc("system-cluster-critical", 12345));
    assert!(has(&errs, "metadata.name"));
}

#[test]
fn system_class_with_global_default_rejected() {
    let mut p = pc("system-node-critical", 2_000_001_000);
    p.global_default = Some(true);
    assert!(has(&validate_priority_class(&p), "metadata.name"));
}

#[test]
fn bad_preemption_policy_rejected() {
    let mut p = pc("high", 100);
    p.preemption_policy = Some("Sometimes".to_string());
    let errs = validate_priority_class(&p);
    assert!(errs
        .iter()
        .any(|e| e.field == "preemptionPolicy" && e.error_type == ErrorType::NotSupported));
}

#[test]
fn valid_preemption_policies_ok() {
    let mut p = pc("high", 100);
    p.preemption_policy = Some("Never".to_string());
    assert!(validate_priority_class(&p).is_empty());
    p.preemption_policy = Some("PreemptLowerPriority".to_string());
    assert!(validate_priority_class(&p).is_empty());
}
