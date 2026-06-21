//! Tests for EndpointSlice field validation.
//!
//! Mirrors the core of upstream `ValidateEndpointSlice`
//! (`pkg/apis/discovery/validation/validation.go`, release-1.35).

use rusternetes_common::resources::endpointslice::EndpointSlice;
use rusternetes_common::validation::endpointslice::validate_endpoint_slice;
use rusternetes_common::validation::field::{Error, ErrorType};
use serde_json::json;

fn es(v: serde_json::Value) -> EndpointSlice {
    let mut obj = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {"name": "es", "namespace": "default"}
    });
    for (k, val) in v.as_object().unwrap() {
        obj[k] = val.clone();
    }
    serde_json::from_value(obj).unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_ipv4_slice_passes() {
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv4",
        "endpoints": [{"addresses": ["10.0.0.1"]}],
        "ports": [{"name": "http", "port": 80, "protocol": "TCP"}]
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn unknown_address_type_rejected() {
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "Bogus",
        "endpoints": [],
        "ports": []
    })));
    assert!(
        has(&errs, "addressType", ErrorType::NotSupported),
        "got: {errs:?}"
    );
}

#[test]
fn empty_addresses_required() {
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv4",
        "endpoints": [{"addresses": []}],
        "ports": []
    })));
    assert!(
        has(&errs, "endpoints[0].addresses", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn ipv6_address_in_ipv4_slice_rejected() {
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv4",
        "endpoints": [{"addresses": ["2001:db8::1"]}],
        "ports": []
    })));
    assert!(
        has(&errs, "endpoints[0].addresses[0]", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn non_ip_in_ipv4_slice_rejected() {
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv4",
        "endpoints": [{"addresses": ["not-an-ip"]}],
        "ports": []
    })));
    assert!(
        has(&errs, "endpoints[0].addresses[0]", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn valid_ipv6_slice_passes() {
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv6",
        "endpoints": [{"addresses": ["2001:db8::1"]}],
        "ports": []
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn bad_port_protocol_rejected() {
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv4",
        "endpoints": [{"addresses": ["10.0.0.1"]}],
        "ports": [{"port": 80, "protocol": "ICMP"}]
    })));
    assert!(
        has(&errs, "ports[0].protocol", ErrorType::NotSupported),
        "got: {errs:?}"
    );
}

#[test]
fn out_of_range_port_rejected() {
    let errs = validate_endpoint_slice(&es(json!({
        "addressType": "IPv4",
        "endpoints": [{"addresses": ["10.0.0.1"]}],
        "ports": [{"port": 99999}]
    })));
    assert!(
        has(&errs, "ports[0].port", ErrorType::Invalid),
        "got: {errs:?}"
    );
}
