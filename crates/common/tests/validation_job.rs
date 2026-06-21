//! Tests for Job validation (port of upstream batch `ValidateJob`).

use rusternetes_common::resources::workloads::{Job, JobSpec};
use rusternetes_common::resources::PodTemplateSpec;
use rusternetes_common::types::LabelSelector;
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::job::validate_job;
use std::collections::HashMap;

fn template_with_labels(restart_policy: &str, labels: &[(&str, &str)]) -> PodTemplateSpec {
    let mut t = PodTemplateSpec::default();
    let meta = t.metadata.get_or_insert_with(Default::default);
    let l = meta.labels.get_or_insert_with(Default::default);
    for (k, v) in labels {
        l.insert(k.to_string(), v.to_string());
    }
    t.spec.restart_policy = Some(restart_policy.to_string());
    t
}

fn selector(labels: &[(&str, &str)]) -> LabelSelector {
    let mut m = HashMap::new();
    for (k, v) in labels {
        m.insert(k.to_string(), v.to_string());
    }
    LabelSelector {
        match_labels: Some(m),
        match_expressions: None,
    }
}

/// A valid (already-defaulted, selector-generated) Job.
fn valid_job() -> Job {
    let spec = JobSpec {
        template: template_with_labels("OnFailure", &[("controller-uid", "abc")]),
        completions: Some(3),
        parallelism: Some(1),
        backoff_limit: Some(6),
        active_deadline_seconds: None,
        selector: Some(selector(&[("controller-uid", "abc")])),
        manual_selector: None,
        suspend: None,
        ttl_seconds_after_finished: None,
        completion_mode: Some("NonIndexed".to_string()),
        backoff_limit_per_index: None,
        max_failed_indexes: None,
        pod_failure_policy: None,
        pod_replacement_policy: None,
        success_policy: None,
        managed_by: None,
    };
    let mut job = Job::new("my-job", "default", spec);
    job.metadata.name = "my-job".to_string();
    job
}

fn has(errs: &[rusternetes_common::validation::field::Error], field_substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field_substr))
}

#[test]
fn valid_job_passes() {
    assert!(
        validate_job(&valid_job()).is_empty(),
        "{:?}",
        validate_job(&valid_job())
    );
}

#[test]
fn negative_completions_rejected() {
    let mut j = valid_job();
    j.spec.completions = Some(-1);
    assert!(has(&validate_job(&j), "spec.completions"));
}

#[test]
fn bad_completion_mode_rejected() {
    let mut j = valid_job();
    j.spec.completion_mode = Some("Sometimes".to_string());
    let errs = validate_job(&j);
    assert!(errs
        .iter()
        .any(|e| e.field.contains("completionMode") && e.error_type == ErrorType::NotSupported));
}

#[test]
fn indexed_requires_completions() {
    let mut j = valid_job();
    j.spec.completion_mode = Some("Indexed".to_string());
    j.spec.completions = None;
    assert!(has(&validate_job(&j), "spec.completions"));
}

#[test]
fn indexed_parallelism_cap() {
    let mut j = valid_job();
    j.spec.completion_mode = Some("Indexed".to_string());
    j.spec.completions = Some(5);
    j.spec.parallelism = Some(100_001);
    assert!(has(&validate_job(&j), "spec.parallelism"));
}

#[test]
fn backoff_limit_per_index_requires_indexed() {
    let mut j = valid_job();
    j.spec.completion_mode = Some("NonIndexed".to_string());
    j.spec.backoff_limit_per_index = Some(2);
    let errs = validate_job(&j);
    assert!(errs.iter().any(|e| e.field.contains("backoffLimitPerIndex")
        && e.detail.contains("requires indexed completion mode")));
}

#[test]
fn max_failed_indexes_requires_backoff_limit_per_index() {
    let mut j = valid_job();
    j.spec.completion_mode = Some("Indexed".to_string());
    j.spec.completions = Some(5);
    j.spec.max_failed_indexes = Some(2);
    j.spec.backoff_limit_per_index = None;
    assert!(has(&validate_job(&j), "spec.backoffLimitPerIndex"));
}

#[test]
fn restart_policy_always_rejected() {
    let mut j = valid_job();
    j.spec.template = template_with_labels("Always", &[("controller-uid", "abc")]);
    assert!(has(&validate_job(&j), "restartPolicy"));
}

#[test]
fn missing_selector_rejected() {
    let mut j = valid_job();
    j.spec.selector = None;
    assert!(has(&validate_job(&j), "spec.selector"));
}

#[test]
fn selector_template_mismatch_rejected() {
    let mut j = valid_job();
    j.spec.selector = Some(selector(&[("controller-uid", "different")]));
    assert!(has(&validate_job(&j), "template"));
}

#[test]
fn indexed_name_too_long_for_hostname_rejected() {
    let mut j = valid_job();
    j.spec.completion_mode = Some("Indexed".to_string());
    j.spec.completions = Some(3);
    // 63-char DNS label limit: a long name + "-2" suffix overflows.
    j.metadata.name = "a".repeat(62);
    let errs = validate_job(&j);
    assert!(has(&errs, "metadata.name"), "{:?}", errs);
}
