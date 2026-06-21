//! Tests for ServiceCIDR validation (port of upstream `ValidateServiceCIDR`).

use rusternetes_common::resources::servicecidr::{ServiceCIDR, ServiceCIDRSpec};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::servicecidr::validate_service_cidr;

fn sc(cidrs: Vec<&str>) -> ServiceCIDR {
    let mut x = ServiceCIDR {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec: Some(ServiceCIDRSpec {
            cidrs: cidrs.into_iter().map(|s| s.to_string()).collect(),
        }),
        status: None,
    };
    x.metadata.name = "scope".to_string();
    x
}

fn has(errs: &[rusternetes_common::validation::field::Error], field_substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field_substr))
}

#[test]
fn valid_single_ipv4_cidr_passes() {
    assert!(validate_service_cidr(&sc(vec!["10.96.0.0/12"])).is_empty());
}

#[test]
fn valid_dual_stack_passes() {
    let errs = validate_service_cidr(&sc(vec!["10.96.0.0/12", "2001:db8::/64"]));
    assert!(errs.is_empty(), "{:?}", errs);
}

#[test]
fn empty_cidrs_rejected() {
    let errs = validate_service_cidr(&sc(vec![]));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.cidrs" && e.error_type == ErrorType::Required));
}

#[test]
fn missing_spec_rejected() {
    let mut x = sc(vec!["10.96.0.0/12"]);
    x.spec = None;
    assert!(has(&validate_service_cidr(&x), "spec.cidrs"));
}

#[test]
fn three_cidrs_rejected() {
    let errs = validate_service_cidr(&sc(vec!["10.96.0.0/12", "2001:db8::/64", "10.0.0.0/8"]));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.cidrs" && e.error_type == ErrorType::Invalid));
}

#[test]
fn invalid_cidr_rejected() {
    assert!(has(
        &validate_service_cidr(&sc(vec!["not-a-cidr"])),
        "spec.cidrs[0]"
    ));
}

#[test]
fn out_of_range_prefix_rejected() {
    assert!(has(
        &validate_service_cidr(&sc(vec!["10.0.0.0/33"])),
        "spec.cidrs[0]"
    ));
}

#[test]
fn same_family_pair_rejected() {
    let errs = validate_service_cidr(&sc(vec!["10.96.0.0/12", "10.0.0.0/8"]));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.cidrs" && e.detail.contains("one IP for each IP family")));
}
