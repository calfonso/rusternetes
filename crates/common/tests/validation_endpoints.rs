//! Tests for Endpoints (core/v1) field validation.
//!
//! Mirrors the core of upstream `validateEndpointSubsets` / `ValidateEndpointIP`
//! (`pkg/apis/core/validation/validation.go`, release-1.35).

use rusternetes_common::resources::endpoints::Endpoints;
use rusternetes_common::validation::endpoints::validate_endpoints;
use rusternetes_common::validation::field::{Error, ErrorType};
use serde_json::json;

fn ep(subsets: serde_json::Value) -> Endpoints {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "ep", "namespace": "default"},
        "subsets": subsets
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_endpoints_pass() {
    let errs = validate_endpoints(&ep(json!([{
        "addresses": [{"ip": "10.0.0.1"}],
        "ports": [{"name": "http", "port": 80, "protocol": "TCP"}]
    }])));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn empty_subset_rejected() {
    let errs = validate_endpoints(&ep(json!([{"ports": [{"port": 80}]}])));
    assert!(
        has(&errs, "subsets[0]", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn invalid_ip_rejected() {
    let errs = validate_endpoints(&ep(json!([{"addresses": [{"ip": "nope"}]}])));
    assert!(
        has(&errs, "subsets[0].addresses[0].ip", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn loopback_ip_rejected() {
    let errs = validate_endpoints(&ep(json!([{"addresses": [{"ip": "127.0.0.1"}]}])));
    assert!(
        has(&errs, "subsets[0].addresses[0].ip", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn unspecified_ip_rejected() {
    let errs = validate_endpoints(&ep(json!([{"addresses": [{"ip": "0.0.0.0"}]}])));
    assert!(
        has(&errs, "subsets[0].addresses[0].ip", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn link_local_ip_rejected() {
    let errs = validate_endpoints(&ep(json!([{"addresses": [{"ip": "169.254.1.1"}]}])));
    assert!(
        has(&errs, "subsets[0].addresses[0].ip", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn multi_port_requires_name() {
    let errs = validate_endpoints(&ep(json!([{
        "addresses": [{"ip": "10.0.0.1"}],
        "ports": [{"port": 80}, {"port": 443}]
    }])));
    assert!(
        has(&errs, "subsets[0].ports[0].name", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn bad_port_protocol_rejected() {
    let errs = validate_endpoints(&ep(json!([{
        "addresses": [{"ip": "10.0.0.1"}],
        "ports": [{"port": 80, "protocol": "ICMP"}]
    }])));
    assert!(
        has(
            &errs,
            "subsets[0].ports[0].protocol",
            ErrorType::NotSupported
        ),
        "got: {errs:?}"
    );
}
