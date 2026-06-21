//! Tests for NetworkPolicy field validation.
//!
//! Mirrors upstream `ValidateNetworkPolicySpec`
//! (`pkg/apis/networking/validation/validation.go`, release-1.35).

use rusternetes_common::resources::networking::NetworkPolicy;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::networkpolicy::validate_network_policy;
use serde_json::json;

fn np(spec: serde_json::Value) -> NetworkPolicy {
    serde_json::from_value(json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": "np", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn minimal_valid_passes() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "policyTypes": ["Ingress"]
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn full_valid_passes() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {"matchLabels": {"app": "web"}},
        "policyTypes": ["Ingress", "Egress"],
        "ingress": [{
            "ports": [{"protocol": "TCP", "port": 8080}],
            "from": [{"podSelector": {"matchLabels": {"app": "client"}}}]
        }],
        "egress": [{
            "ports": [{"protocol": "UDP", "port": 53, "endPort": 53}],
            "to": [{"ipBlock": {"cidr": "10.0.0.0/8", "except": ["10.1.0.0/16"]}}]
        }]
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn bad_protocol_rejected() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "ingress": [{"ports": [{"protocol": "ICMP", "port": 8080}]}]
    })));
    assert!(
        has(
            &errs,
            "spec.ingress[0].ports[0].protocol",
            ErrorType::NotSupported
        ),
        "got: {errs:?}"
    );
}

#[test]
fn out_of_range_port_rejected() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "ingress": [{"ports": [{"port": 70000}]}]
    })));
    assert!(
        has(&errs, "spec.ingress[0].ports[0].port", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn endport_below_port_rejected() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "ingress": [{"ports": [{"port": 100, "endPort": 50}]}]
    })));
    assert!(
        has(
            &errs,
            "spec.ingress[0].ports[0].endPort",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

#[test]
fn endport_with_named_port_rejected() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "ingress": [{"ports": [{"port": "http", "endPort": 80}]}]
    })));
    assert!(
        has(
            &errs,
            "spec.ingress[0].ports[0].endPort",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

#[test]
fn empty_peer_required() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "ingress": [{"from": [{}]}]
    })));
    assert!(
        has(&errs, "spec.ingress[0].from[0]", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn ipblock_with_selector_forbidden() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "egress": [{"to": [{
            "ipBlock": {"cidr": "10.0.0.0/8"},
            "podSelector": {"matchLabels": {"app": "x"}}
        }]}]
    })));
    assert!(
        has(&errs, "spec.egress[0].to[0]", ErrorType::Forbidden),
        "got: {errs:?}"
    );
}

#[test]
fn invalid_cidr_rejected() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "egress": [{"to": [{"ipBlock": {"cidr": "not-a-cidr"}}]}]
    })));
    assert!(
        has(
            &errs,
            "spec.egress[0].to[0].ipBlock.cidr",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

#[test]
fn too_many_policy_types_rejected() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "policyTypes": ["Ingress", "Egress", "Ingress"]
    })));
    assert!(
        has(&errs, "spec.policyTypes", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn unknown_policy_type_rejected() {
    let errs = validate_network_policy(&np(json!({
        "podSelector": {},
        "policyTypes": ["Sideways"]
    })));
    assert!(
        has(&errs, "spec.policyTypes[0]", ErrorType::NotSupported),
        "got: {errs:?}"
    );
}
