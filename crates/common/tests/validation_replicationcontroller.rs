//! Tests for ReplicationController field validation.
//!
//! Mirrors upstream `ValidateReplicationControllerSpec`
//! (`pkg/apis/core/validation/validation.go`, release-1.35). Runs after
//! defaulting, so a valid fixture carries an explicit selector.

use rusternetes_common::resources::workloads::ReplicationController;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::replicationcontroller::validate_replication_controller;
use serde_json::json;

fn rc(spec: serde_json::Value) -> ReplicationController {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "rc", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_rc_passes() {
    let errs = validate_replication_controller(&rc(json!({
        "replicas": 3,
        "selector": {"app": "web"},
        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": []}}
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn negative_replicas_rejected() {
    let errs = validate_replication_controller(&rc(json!({
        "replicas": -1,
        "selector": {"app": "web"},
        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": []}}
    })));
    assert!(
        has(&errs, "spec.replicas", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn empty_selector_required() {
    let errs = validate_replication_controller(&rc(json!({
        "selector": {},
        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": []}}
    })));
    assert!(
        has(&errs, "spec.selector", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn template_labels_must_match_selector() {
    let errs = validate_replication_controller(&rc(json!({
        "selector": {"app": "web"},
        "template": {"metadata": {"labels": {"app": "other"}}, "spec": {"containers": []}}
    })));
    assert!(
        errs.iter().any(|e| e.field.starts_with("spec.template")),
        "got: {errs:?}"
    );
}

#[test]
fn negative_min_ready_seconds_rejected() {
    let errs = validate_replication_controller(&rc(json!({
        "selector": {"app": "web"},
        "minReadySeconds": -5,
        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": []}}
    })));
    assert!(
        has(&errs, "spec.minReadySeconds", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn absent_replicas_required() {
    // Upstream `ValidateReplicationControllerSpec` (validation.go:7056-7057):
    // `replicas == nil` -> Required.
    let errs = validate_replication_controller(&rc(json!({
        "selector": {"app": "web"},
        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": []}}
    })));
    assert!(
        has(&errs, "spec.replicas", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn non_always_restart_policy_not_supported() {
    // Upstream `ValidatePodTemplateSpecForRC` (validation.go:7041-7042).
    let errs = validate_replication_controller(&rc(json!({
        "replicas": 1,
        "selector": {"app": "web"},
        "template": {
            "metadata": {"labels": {"app": "web"}},
            "spec": {"containers": [], "restartPolicy": "Never"}
        }
    })));
    assert!(
        has(
            &errs,
            "spec.template.spec.restartPolicy",
            ErrorType::NotSupported
        ),
        "got: {errs:?}"
    );
}

#[test]
fn always_restart_policy_ok() {
    let errs = validate_replication_controller(&rc(json!({
        "replicas": 1,
        "selector": {"app": "web"},
        "template": {
            "metadata": {"labels": {"app": "web"}},
            "spec": {"containers": [], "restartPolicy": "Always"}
        }
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn active_deadline_seconds_forbidden() {
    // Upstream `ValidatePodTemplateSpecForRC` (validation.go:7044-7046).
    let errs = validate_replication_controller(&rc(json!({
        "replicas": 1,
        "selector": {"app": "web"},
        "template": {
            "metadata": {"labels": {"app": "web"}},
            "spec": {"containers": [], "activeDeadlineSeconds": 30}
        }
    })));
    assert!(
        has(
            &errs,
            "spec.template.spec.activeDeadlineSeconds",
            ErrorType::Forbidden
        ),
        "got: {errs:?}"
    );
}
