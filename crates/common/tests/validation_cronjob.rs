//! Tests for CronJob validation (port of upstream batch `ValidateCronJobCreate`).

use rusternetes_common::resources::workloads::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use rusternetes_common::resources::PodTemplateSpec;
use rusternetes_common::validation::cronjob::validate_cron_job;

fn job_template() -> JobTemplateSpec {
    let mut t = PodTemplateSpec::default();
    t.spec.restart_policy = Some("OnFailure".to_string());
    JobTemplateSpec {
        metadata: None,
        spec: JobSpec {
            template: t,
            completions: None,
            parallelism: None,
            backoff_limit: None,
            active_deadline_seconds: None,
            selector: None,
            manual_selector: None,
            suspend: None,
            ttl_seconds_after_finished: None,
            completion_mode: None,
            backoff_limit_per_index: None,
            max_failed_indexes: None,
            pod_failure_policy: None,
            pod_replacement_policy: None,
            success_policy: None,
            managed_by: None,
        },
    }
}

fn valid_cronjob() -> CronJob {
    let spec = CronJobSpec {
        schedule: "*/5 * * * *".to_string(),
        job_template: job_template(),
        concurrency_policy: Some("Allow".to_string()),
        suspend: None,
        successful_jobs_history_limit: Some(3),
        failed_jobs_history_limit: Some(1),
        starting_deadline_seconds: None,
        time_zone: None,
    };
    let mut cj = CronJob::new("my-cron", "default", spec);
    cj.metadata.name = "my-cron".to_string();
    cj
}

fn has(errs: &[rusternetes_common::validation::field::Error], substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(substr))
}

#[test]
fn valid_cronjob_passes() {
    let errs = validate_cron_job(&valid_cronjob());
    assert!(errs.is_empty(), "{:?}", errs);
}

#[test]
fn empty_schedule_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.schedule = String::new();
    assert!(has(&validate_cron_job(&cj), "spec.schedule"));
}

#[test]
fn bad_schedule_syntax_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.schedule = "not a cron".to_string();
    assert!(has(&validate_cron_job(&cj), "spec.schedule"));
}

#[test]
fn inline_tz_in_schedule_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.schedule = "CRON_TZ=America/New_York 0 * * * *".to_string();
    assert!(has(&validate_cron_job(&cj), "spec.schedule"));
}

#[test]
fn descriptor_schedule_passes() {
    let mut cj = valid_cronjob();
    cj.spec.schedule = "@hourly".to_string();
    // @hourly is a valid cron descriptor; should not error on schedule.
    let errs = validate_cron_job(&cj);
    assert!(!has(&errs, "spec.schedule"), "{:?}", errs);
}

#[test]
fn bad_concurrency_policy_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.concurrency_policy = Some("Sometimes".to_string());
    assert!(has(&validate_cron_job(&cj), "concurrencyPolicy"));
}

#[test]
fn negative_starting_deadline_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.starting_deadline_seconds = Some(-1);
    assert!(has(&validate_cron_job(&cj), "startingDeadlineSeconds"));
}

#[test]
fn negative_history_limit_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.failed_jobs_history_limit = Some(-2);
    assert!(has(&validate_cron_job(&cj), "failedJobsHistoryLimit"));
}

#[test]
fn job_template_with_selector_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.job_template.spec.selector = Some(rusternetes_common::types::LabelSelector {
        match_labels: Some(std::collections::HashMap::new()),
        match_expressions: None,
    });
    let errs = validate_cron_job(&cj);
    assert!(errs
        .iter()
        .any(|e| e.field.contains("jobTemplate.spec.selector")
            && e.detail.contains("auto-generated")));
}

#[test]
fn job_template_bad_restart_policy_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.job_template.spec.template.spec.restart_policy = Some("Always".to_string());
    assert!(has(&validate_cron_job(&cj), "restartPolicy"));
}

#[test]
fn bad_timezone_rejected() {
    let mut cj = valid_cronjob();
    cj.spec.time_zone = Some("Local".to_string());
    assert!(has(&validate_cron_job(&cj), "timeZone"));
}

#[test]
fn name_too_long_rejected() {
    let mut cj = valid_cronjob();
    cj.metadata.name = "a".repeat(53);
    assert!(has(&validate_cron_job(&cj), "metadata.name"));
}
