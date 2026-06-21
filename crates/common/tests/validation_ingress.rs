//! Tests for Ingress field validation.
//!
//! Mirrors upstream `ValidateIngressSpec`
//! (`pkg/apis/networking/validation/validation.go`, release-1.35).

use rusternetes_common::resources::ingress::Ingress;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::ingress::validate_ingress;
use serde_json::json;

fn ing(spec: serde_json::Value) -> Ingress {
    serde_json::from_value(json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {"name": "ing", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn svc_backend() -> serde_json::Value {
    json!({"service": {"name": "web", "port": {"number": 80}}})
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_rules_ingress_passes() {
    let errs = validate_ingress(&ing(json!({
        "rules": [{
            "host": "example.com",
            "http": {"paths": [{"path": "/", "pathType": "Prefix", "backend": svc_backend()}]}
        }]
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn default_backend_only_passes() {
    let errs = validate_ingress(&ing(json!({"defaultBackend": svc_backend()})));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn neither_rules_nor_default_backend_rejected() {
    let errs = validate_ingress(&ing(json!({})));
    assert!(has(&errs, "spec", ErrorType::Invalid), "got: {errs:?}");
}

#[test]
fn host_as_ip_rejected() {
    let errs = validate_ingress(&ing(json!({
        "rules": [{
            "host": "10.0.0.1",
            "http": {"paths": [{"pathType": "Prefix", "path": "/", "backend": svc_backend()}]}
        }]
    })));
    assert!(
        has(&errs, "spec.rules[0].host", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn empty_paths_required() {
    let errs = validate_ingress(&ing(json!({
        "rules": [{"http": {"paths": []}}]
    })));
    assert!(
        has(&errs, "spec.rules[0].http.paths", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn missing_path_type_required() {
    let errs = validate_ingress(&ing(json!({
        "rules": [{"http": {"paths": [{"path": "/", "pathType": "", "backend": svc_backend()}]}}]
    })));
    assert!(
        has(
            &errs,
            "spec.rules[0].http.paths[0].pathType",
            ErrorType::Required
        ),
        "got: {errs:?}"
    );
}

#[test]
fn unknown_path_type_not_supported() {
    let errs = validate_ingress(&ing(json!({
        "rules": [{"http": {"paths": [{"path": "/", "pathType": "Bogus", "backend": svc_backend()}]}}]
    })));
    assert!(
        has(
            &errs,
            "spec.rules[0].http.paths[0].pathType",
            ErrorType::NotSupported
        ),
        "got: {errs:?}"
    );
}

#[test]
fn non_absolute_prefix_path_rejected() {
    let errs = validate_ingress(&ing(json!({
        "rules": [{"http": {"paths": [{"path": "noslash", "pathType": "Prefix", "backend": svc_backend()}]}}]
    })));
    assert!(
        has(
            &errs,
            "spec.rules[0].http.paths[0].path",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

#[test]
fn backend_with_both_service_and_resource_rejected() {
    let errs = validate_ingress(&ing(json!({
        "defaultBackend": {
            "service": {"name": "web", "port": {"number": 80}},
            "resource": {"apiGroup": "k8s.example.com", "kind": "StorageBucket", "name": "static"}
        }
    })));
    assert!(
        has(&errs, "spec.defaultBackend", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn backend_port_name_and_number_rejected() {
    let errs = validate_ingress(&ing(json!({
        "defaultBackend": {"service": {"name": "web", "port": {"name": "http", "number": 80}}}
    })));
    assert!(
        has(&errs, "spec.defaultBackend", ErrorType::Invalid),
        "got: {errs:?}"
    );
}
