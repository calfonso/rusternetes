//! Tests for StatefulSet field validation.
//!
//! Mirrors upstream `ValidateStatefulSetSpec`
//! (`pkg/apis/apps/validation/validation.go`, release-1.35). The validator runs
//! after defaulting, so the valid fixture carries `podManagementPolicy` +
//! `updateStrategy` (which the api-server defaults).

use rusternetes_common::resources::workloads::StatefulSet;
use rusternetes_common::validation::apps::validate_statefulset;
use rusternetes_common::validation::field::ErrorType;
use serde_json::json;

fn ss(spec: serde_json::Value) -> StatefulSet {
    serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "web", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

/// A fully-defaulted, valid spec.
fn valid_spec() -> serde_json::Value {
    json!({
        "replicas": 3,
        "serviceName": "web-svc",
        "podManagementPolicy": "OrderedReady",
        "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": {"partition": 0}},
        "selector": {"matchLabels": {"app": "web"}},
        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": []}}
    })
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_statefulset_passes() {
    let errs = validate_statefulset(&ss(valid_spec()));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn bad_pod_management_policy_rejected() {
    let mut spec = valid_spec();
    spec["podManagementPolicy"] = json!("Bogus");
    let errs = validate_statefulset(&ss(spec));
    assert!(
        has(&errs, "spec.podManagementPolicy", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn missing_pod_management_policy_required() {
    let mut spec = valid_spec();
    spec.as_object_mut().unwrap().remove("podManagementPolicy");
    let errs = validate_statefulset(&ss(spec));
    assert!(
        has(&errs, "spec.podManagementPolicy", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn ondelete_with_rolling_update_rejected() {
    let mut spec = valid_spec();
    spec["updateStrategy"] = json!({"type": "OnDelete", "rollingUpdate": {"partition": 0}});
    let errs = validate_statefulset(&ss(spec));
    assert!(
        has(
            &errs,
            "spec.updateStrategy.rollingUpdate",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

#[test]
fn negative_partition_rejected() {
    let mut spec = valid_spec();
    spec["updateStrategy"] = json!({"type": "RollingUpdate", "rollingUpdate": {"partition": -1}});
    let errs = validate_statefulset(&ss(spec));
    assert!(
        has(
            &errs,
            "spec.updateStrategy.rollingUpdate.partition",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

#[test]
fn negative_replicas_rejected() {
    let mut spec = valid_spec();
    spec["replicas"] = json!(-2);
    let errs = validate_statefulset(&ss(spec));
    assert!(
        has(&errs, "spec.replicas", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn invalid_service_name_rejected() {
    let mut spec = valid_spec();
    spec["serviceName"] = json!("Web_Svc"); // uppercase + underscore -> not a DNS-1123 label
    let errs = validate_statefulset(&ss(spec));
    assert!(
        has(&errs, "spec.serviceName", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn empty_selector_required() {
    let mut spec = valid_spec();
    spec["selector"] = json!({});
    let errs = validate_statefulset(&ss(spec));
    // Upstream emits Invalid("empty selector is invalid for statefulset"), not Required.
    assert!(
        errs.iter().any(|e| e.field == "spec.selector"
            && e.error_type == ErrorType::Invalid
            && e.detail
                .contains("empty selector is invalid for statefulset")),
        "got: {errs:?}"
    );
}

#[test]
fn template_labels_must_match_selector() {
    let mut spec = valid_spec();
    spec["template"]["metadata"]["labels"] = json!({"app": "other"});
    let errs = validate_statefulset(&ss(spec));
    assert!(
        errs.iter().any(|e| e.field.starts_with("spec.template")),
        "got: {errs:?}"
    );
}
