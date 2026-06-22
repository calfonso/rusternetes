//! Tests for ReplicaSet field validation.
//!
//! Mirrors upstream `ValidateReplicaSetSpec`
//! (`pkg/apis/apps/validation/validation.go`, release-1.35).

use rusternetes_common::resources::workloads::ReplicaSet;
use rusternetes_common::validation::apps::validate_replicaset;
use rusternetes_common::validation::field::ErrorType;
use serde_json::json;

/// Build a ReplicaSet from a spec JSON fragment (metadata is fixed).
fn rs(spec: serde_json::Value) -> ReplicaSet {
    serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "rs", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn matching_spec() -> serde_json::Value {
    json!({
        "replicas": 3,
        "selector": {"matchLabels": {"app": "web"}},
        "template": {
            "metadata": {"labels": {"app": "web"}},
            "spec": {"containers": []}
        }
    })
}

#[test]
fn valid_replicaset_passes() {
    let errs = validate_replicaset(&rs(matching_spec()));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn negative_replicas_rejected() {
    let mut spec = matching_spec();
    spec["replicas"] = json!(-1);
    let errs = validate_replicaset(&rs(spec));
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.replicas" && e.error_type == ErrorType::Invalid),
        "expected spec.replicas Invalid, got: {errs:?}"
    );
}

#[test]
fn negative_min_ready_seconds_rejected() {
    let mut spec = matching_spec();
    spec["minReadySeconds"] = json!(-5);
    let errs = validate_replicaset(&rs(spec));
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.minReadySeconds" && e.error_type == ErrorType::Invalid),
        "expected spec.minReadySeconds Invalid, got: {errs:?}"
    );
}

#[test]
fn empty_selector_rejected() {
    let spec = json!({
        "replicas": 1,
        "selector": {},
        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": []}}
    });
    let errs = validate_replicaset(&rs(spec));
    // Upstream emits Invalid("empty selector is invalid for deployment") — ReplicaSet
    // reuses the "deployment" string verbatim (validation.go line 819).
    assert!(
        errs.iter().any(|e| e.field == "spec.selector"
            && e.error_type == ErrorType::Invalid
            && e.detail
                .contains("empty selector is invalid for deployment")),
        "expected spec.selector Invalid empty-selector, got: {errs:?}"
    );
}

#[test]
fn template_labels_must_match_selector() {
    let spec = json!({
        "replicas": 1,
        "selector": {"matchLabels": {"app": "web"}},
        // template labels do not satisfy the selector
        "template": {"metadata": {"labels": {"app": "other"}}, "spec": {"containers": []}}
    });
    let errs = validate_replicaset(&rs(spec));
    assert!(
        errs.iter().any(|e| e.field.starts_with("spec.template")),
        "expected a template-labels mismatch error, got: {errs:?}"
    );
}
