//! Table-driven tests for Deployment field validation.
//!
//! Mirrors upstream Kubernetes v1.35 tests from:
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/apis/apps/validation/validation_test.go>
//!
//! Each test block corresponds to a `TestValidateDeployment*` function
//! in the upstream Go file. Error wording and field paths must match upstream
//! exactly so conformance log greps remain valid.

use rusternetes_common::resources::deployment::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment,
};
use rusternetes_common::resources::pod::PodSpec;
use rusternetes_common::resources::workloads::PodTemplateSpec;
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_common::validation::apps::{validate_deployment, validate_deployment_update};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn make_selector(labels: &[(&str, &str)]) -> LabelSelector {
    let mut m = HashMap::new();
    for (k, v) in labels {
        m.insert((*k).to_string(), (*v).to_string());
    }
    LabelSelector {
        match_labels: if m.is_empty() { None } else { Some(m) },
        match_expressions: None,
    }
}

fn make_labels(labels: &[(&str, &str)]) -> HashMap<String, String> {
    labels
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn make_template(labels: &[(&str, &str)]) -> PodTemplateSpec {
    PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: if labels.is_empty() {
                None
            } else {
                Some(make_labels(labels))
            },
            ..ObjectMeta::default()
        }),
        spec: PodSpec::default(),
    }
}

fn make_deployment(
    replicas: Option<i32>,
    selector: LabelSelector,
    template_labels: &[(&str, &str)],
    strategy: Option<DeploymentStrategy>,
) -> Deployment {
    Deployment {
        type_meta: TypeMeta {
            kind: "Deployment".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-deployment".to_string(),
            namespace: Some("default".to_string()),
            ..ObjectMeta::default()
        },
        spec: DeploymentSpec {
            replicas,
            selector,
            template: make_template(template_labels),
            strategy,
            ..DeploymentSpec::default()
        },
        status: None,
    }
}

fn rolling_update(
    max_unavailable: Option<serde_json::Value>,
    max_surge: Option<serde_json::Value>,
) -> DeploymentStrategy {
    DeploymentStrategy {
        strategy_type: "RollingUpdate".to_string(),
        rolling_update: Some(RollingUpdateDeployment {
            max_unavailable,
            max_surge,
        }),
    }
}

fn int_val(n: i64) -> serde_json::Value {
    serde_json::Value::Number(n.into())
}

fn pct_val(s: &str) -> serde_json::Value {
    serde_json::Value::String(s.to_string())
}

fn aggregate(errs: &[rusternetes_common::validation::field::Error]) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

// ---------------------------------------------------------------------------
// TestValidateDeployment — create-path, valid cases
// Mirrors upstream lines ~84-158
// ---------------------------------------------------------------------------

#[test]
fn test_validate_deployment_valid_cases() {
    let app_labels = [("app", "nginx")];
    let cases: Vec<(&str, Deployment)> = vec![
        (
            "valid with 1 replica and rolling update 25%/25%",
            make_deployment(
                Some(1),
                make_selector(&app_labels),
                &app_labels,
                Some(rolling_update(Some(pct_val("25%")), Some(pct_val("25%")))),
            ),
        ),
        (
            "valid with 0 replicas",
            make_deployment(Some(0), make_selector(&app_labels), &app_labels, None),
        ),
        (
            "valid with i32::MAX replicas",
            make_deployment(
                Some(i32::MAX),
                make_selector(&app_labels),
                &app_labels,
                None,
            ),
        ),
        (
            "valid Recreate strategy no rolling update",
            make_deployment(
                Some(1),
                make_selector(&app_labels),
                &app_labels,
                Some(DeploymentStrategy {
                    strategy_type: "Recreate".to_string(),
                    rolling_update: None,
                }),
            ),
        ),
        (
            "valid RollingUpdate maxUnavailable=0 maxSurge=1",
            make_deployment(
                Some(3),
                make_selector(&app_labels),
                &app_labels,
                Some(rolling_update(Some(int_val(0)), Some(int_val(1)))),
            ),
        ),
        (
            "valid RollingUpdate maxUnavailable=1 maxSurge=0",
            make_deployment(
                Some(3),
                make_selector(&app_labels),
                &app_labels,
                Some(rolling_update(Some(int_val(1)), Some(int_val(0)))),
            ),
        ),
        (
            "no strategy (defaults applied by handler)",
            make_deployment(Some(1), make_selector(&app_labels), &app_labels, None),
        ),
    ];

    for (name, deployment) in &cases {
        let errs = validate_deployment(deployment);
        assert!(
            errs.is_empty(),
            "case {name}: expected no errors, got: {}",
            aggregate(&errs)
        );
    }
}

// ---------------------------------------------------------------------------
// TestValidateDeployment — create-path, invalid cases
// Mirrors upstream lines ~160-260
// ---------------------------------------------------------------------------

#[test]
fn test_validate_deployment_invalid_replicas() {
    let app_labels = [("app", "nginx")];
    let mut d = make_deployment(Some(-1), make_selector(&app_labels), &app_labels, None);
    d.spec.replicas = Some(-1);
    let errs = validate_deployment(&d);
    assert!(!errs.is_empty(), "expected error for negative replicas");
    let agg = aggregate(&errs);
    assert!(
        agg.contains("spec.replicas"),
        "expected spec.replicas in error, got: {agg}"
    );
    assert!(
        agg.contains("must be greater than or equal to 0"),
        "expected non-negative detail, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_empty_selector() {
    // An empty selector must produce a Required error on spec.selector
    let mut d = make_deployment(
        Some(1),
        LabelSelector::default(), // empty
        &[("app", "nginx")],
        None,
    );
    d.spec.selector = LabelSelector::default();
    let errs = validate_deployment(&d);
    assert!(!errs.is_empty(), "expected error for empty selector");
    let agg = aggregate(&errs);
    assert!(
        agg.contains("spec.selector"),
        "expected spec.selector in error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_selector_not_matching_template_labels() {
    let selector_labels = [("app", "nginx")];
    let template_labels = [("app", "apache")]; // mismatch
    let d = make_deployment(
        Some(1),
        make_selector(&selector_labels),
        &template_labels,
        None,
    );
    let errs = validate_deployment(&d);
    assert!(!errs.is_empty(), "expected error for label mismatch");
    let agg = aggregate(&errs);
    assert!(
        agg.contains("selector") || agg.contains("labels"),
        "expected selector/labels error, got: {agg}"
    );
    assert!(
        agg.contains("does not match"),
        "expected 'does not match' detail, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_recreate_with_rolling_update_forbidden() {
    let app_labels = [("app", "nginx")];
    let d = make_deployment(
        Some(1),
        make_selector(&app_labels),
        &app_labels,
        Some(DeploymentStrategy {
            strategy_type: "Recreate".to_string(),
            rolling_update: Some(RollingUpdateDeployment {
                max_unavailable: Some(pct_val("25%")),
                max_surge: Some(pct_val("25%")),
            }),
        }),
    );
    let errs = validate_deployment(&d);
    assert!(
        !errs.is_empty(),
        "expected error for Recreate+rollingUpdate"
    );
    let agg = aggregate(&errs);
    assert!(
        agg.contains("rollingUpdate"),
        "expected rollingUpdate in error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_unsupported_strategy_type() {
    let app_labels = [("app", "nginx")];
    let d = make_deployment(
        Some(1),
        make_selector(&app_labels),
        &app_labels,
        Some(DeploymentStrategy {
            strategy_type: "BadType".to_string(),
            rolling_update: None,
        }),
    );
    let errs = validate_deployment(&d);
    assert!(
        !errs.is_empty(),
        "expected error for unsupported strategy type"
    );
    let agg = aggregate(&errs);
    assert!(
        agg.contains("BadType"),
        "expected bad value in error, got: {agg}"
    );
    assert!(
        agg.contains("Unsupported value"),
        "expected unsupported-value error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_rolling_update_both_zero() {
    let app_labels = [("app", "nginx")];
    let d = make_deployment(
        Some(3),
        make_selector(&app_labels),
        &app_labels,
        Some(rolling_update(Some(int_val(0)), Some(int_val(0)))),
    );
    let errs = validate_deployment(&d);
    assert!(
        !errs.is_empty(),
        "expected error when both maxUnavailable and maxSurge are 0"
    );
    let agg = aggregate(&errs);
    assert!(
        agg.contains("maxSurge") || agg.contains("rollingUpdate"),
        "expected rollingUpdate error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_rolling_update_negative_int() {
    let app_labels = [("app", "nginx")];
    // negative maxUnavailable
    let d = make_deployment(
        Some(3),
        make_selector(&app_labels),
        &app_labels,
        Some(rolling_update(Some(int_val(-1)), Some(pct_val("25%")))),
    );
    let errs = validate_deployment(&d);
    assert!(
        !errs.is_empty(),
        "expected error for negative maxUnavailable"
    );
    let agg = aggregate(&errs);
    assert!(
        agg.contains("maxUnavailable"),
        "expected maxUnavailable in error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_rolling_update_invalid_percent() {
    let app_labels = [("app", "nginx")];
    // 110% is out of range
    let d = make_deployment(
        Some(3),
        make_selector(&app_labels),
        &app_labels,
        Some(rolling_update(Some(pct_val("110%")), Some(pct_val("25%")))),
    );
    let errs = validate_deployment(&d);
    assert!(!errs.is_empty(), "expected error for 110% maxUnavailable");
    let agg = aggregate(&errs);
    assert!(
        agg.contains("maxUnavailable"),
        "expected maxUnavailable in error, got: {agg}"
    );
    assert!(
        agg.contains("100%") || agg.contains("between"),
        "expected percentage range detail, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_negative_min_ready_seconds() {
    let app_labels = [("app", "nginx")];
    let mut d = make_deployment(Some(1), make_selector(&app_labels), &app_labels, None);
    d.spec.min_ready_seconds = Some(-1);
    let errs = validate_deployment(&d);
    assert!(
        !errs.is_empty(),
        "expected error for negative minReadySeconds"
    );
    let agg = aggregate(&errs);
    assert!(
        agg.contains("minReadySeconds"),
        "expected minReadySeconds in error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_negative_revision_history_limit() {
    let app_labels = [("app", "nginx")];
    let mut d = make_deployment(Some(1), make_selector(&app_labels), &app_labels, None);
    d.spec.revision_history_limit = Some(-1);
    let errs = validate_deployment(&d);
    assert!(
        !errs.is_empty(),
        "expected error for negative revisionHistoryLimit"
    );
    let agg = aggregate(&errs);
    assert!(
        agg.contains("revisionHistoryLimit"),
        "expected revisionHistoryLimit in error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_progress_deadline_not_greater_than_min_ready() {
    let app_labels = [("app", "nginx")];
    let mut d = make_deployment(Some(1), make_selector(&app_labels), &app_labels, None);
    // minReadySeconds = 30, progressDeadlineSeconds = 30 → must be *greater*
    d.spec.min_ready_seconds = Some(30);
    d.spec.progress_deadline_seconds = Some(30);
    let errs = validate_deployment(&d);
    assert!(
        !errs.is_empty(),
        "expected error when progressDeadlineSeconds == minReadySeconds"
    );
    let agg = aggregate(&errs);
    assert!(
        agg.contains("progressDeadlineSeconds"),
        "expected progressDeadlineSeconds in error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_progress_deadline_zero() {
    let app_labels = [("app", "nginx")];
    let mut d = make_deployment(Some(1), make_selector(&app_labels), &app_labels, None);
    d.spec.progress_deadline_seconds = Some(0);
    let errs = validate_deployment(&d);
    assert!(
        !errs.is_empty(),
        "expected error for progressDeadlineSeconds=0"
    );
    let agg = aggregate(&errs);
    assert!(
        agg.contains("progressDeadlineSeconds"),
        "expected progressDeadlineSeconds in error, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_progress_deadline_valid_greater_than_min_ready() {
    let app_labels = [("app", "nginx")];
    let mut d = make_deployment(Some(1), make_selector(&app_labels), &app_labels, None);
    d.spec.min_ready_seconds = Some(30);
    d.spec.progress_deadline_seconds = Some(31); // just above minReadySeconds
    let errs = validate_deployment(&d);
    assert!(
        errs.is_empty(),
        "expected no error, got: {}",
        aggregate(&errs)
    );
}

// ---------------------------------------------------------------------------
// TestValidateDeploymentUpdate — update-path
// Mirrors upstream lines ~265-340
// ---------------------------------------------------------------------------

#[test]
fn test_validate_deployment_update_selector_immutable() {
    let old_labels = [("app", "nginx")];
    let new_labels = [("app", "apache")];

    let old = make_deployment(Some(1), make_selector(&old_labels), &old_labels, None);
    let mut new = make_deployment(Some(1), make_selector(&new_labels), &new_labels, None);
    new.spec.selector = make_selector(&new_labels);

    let errs = validate_deployment_update(&new, &old);
    assert!(!errs.is_empty(), "expected error for changed selector");
    let agg = aggregate(&errs);
    assert!(
        agg.contains("spec.selector"),
        "expected spec.selector in error, got: {agg}"
    );
    assert!(
        agg.contains("immutable"),
        "expected immutable detail, got: {agg}"
    );
}

#[test]
fn test_validate_deployment_update_selector_unchanged_is_valid() {
    let app_labels = [("app", "nginx")];
    let old = make_deployment(Some(1), make_selector(&app_labels), &app_labels, None);
    let new = make_deployment(Some(2), make_selector(&app_labels), &app_labels, None);

    let errs = validate_deployment_update(&new, &old);
    assert!(
        errs.is_empty(),
        "expected no error for valid update, got: {}",
        aggregate(&errs)
    );
}

#[test]
fn test_validate_deployment_update_inherits_spec_validation() {
    let app_labels = [("app", "nginx")];
    let old = make_deployment(Some(1), make_selector(&app_labels), &app_labels, None);
    let mut new = make_deployment(Some(-1), make_selector(&app_labels), &app_labels, None);
    new.spec.replicas = Some(-1); // invalid

    let errs = validate_deployment_update(&new, &old);
    assert!(!errs.is_empty(), "expected spec errors on update");
    let agg = aggregate(&errs);
    assert!(
        agg.contains("replicas"),
        "expected replicas in error, got: {agg}"
    );
}
