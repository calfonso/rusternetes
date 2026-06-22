//! Tests for DaemonSet field validation.
//!
//! Mirrors upstream `ValidateDaemonSetSpec`
//! (`pkg/apis/apps/validation/validation.go`, release-1.35). Runs after
//! defaulting, so the valid fixture carries `updateStrategy`.

use rusternetes_common::resources::workloads::DaemonSet;
use rusternetes_common::validation::apps::validate_daemonset;
use rusternetes_common::validation::field::{Error, ErrorType};
use serde_json::json;

fn ds(spec: serde_json::Value) -> DaemonSet {
    serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "agent", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn valid_spec() -> serde_json::Value {
    json!({
        "selector": {"matchLabels": {"app": "agent"}},
        "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": "1"}},
        "template": {"metadata": {"labels": {"app": "agent"}}, "spec": {"containers": []}}
    })
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_daemonset_passes() {
    let errs = validate_daemonset(&ds(valid_spec()));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn ondelete_strategy_passes() {
    let mut spec = valid_spec();
    spec["updateStrategy"] = json!({"type": "OnDelete"});
    let errs = validate_daemonset(&ds(spec));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn empty_selector_required() {
    let mut spec = valid_spec();
    spec["selector"] = json!({});
    let errs = validate_daemonset(&ds(spec));
    // Upstream emits Invalid("empty selector is invalid for daemonset"), not Required.
    assert!(
        errs.iter().any(|e| e.field == "spec.selector"
            && e.error_type == ErrorType::Invalid
            && e.detail.contains("empty selector is invalid for daemonset")),
        "got: {errs:?}"
    );
}

#[test]
fn template_labels_must_match_selector() {
    let mut spec = valid_spec();
    spec["template"]["metadata"]["labels"] = json!({"app": "other"});
    let errs = validate_daemonset(&ds(spec));
    assert!(
        errs.iter().any(|e| e.field.starts_with("spec.template")),
        "got: {errs:?}"
    );
}

#[test]
fn negative_min_ready_seconds_rejected() {
    let mut spec = valid_spec();
    spec["minReadySeconds"] = json!(-1);
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(&errs, "spec.minReadySeconds", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn negative_revision_history_limit_rejected() {
    let mut spec = valid_spec();
    spec["revisionHistoryLimit"] = json!(-3);
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(&errs, "spec.revisionHistoryLimit", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn unknown_update_strategy_not_supported() {
    let mut spec = valid_spec();
    spec["updateStrategy"] = json!({"type": "Bogus"});
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(&errs, "spec.updateStrategy", ErrorType::NotSupported),
        "got: {errs:?}"
    );
}

#[test]
fn rolling_update_requires_rolling_update_block() {
    let mut spec = valid_spec();
    spec["updateStrategy"] = json!({"type": "RollingUpdate"});
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(
            &errs,
            "spec.updateStrategy.rollingUpdate",
            ErrorType::Required
        ),
        "got: {errs:?}"
    );
}

#[test]
fn out_of_range_max_unavailable_rejected() {
    let mut spec = valid_spec();
    spec["updateStrategy"]["rollingUpdate"]["maxUnavailable"] = json!("150%");
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(
            &errs,
            "spec.updateStrategy.rollingUpdate.maxUnavailable",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}
