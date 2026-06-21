//! CronJob validation — port of upstream Kubernetes
//! `pkg/apis/batch/validation/validation.go` (release-1.35).
//!
//! Covers `ValidateCronJobCreate` / `validateCronJobSpec`: schedule (required +
//! cron-syntax + no inline TZ), `startingDeadlineSeconds` ≥ 0, `timeZone` naming
//! rules, `concurrencyPolicy` enum, the embedded `jobTemplate`
//! (`ValidateJobTemplateSpec`), the history-limit non-negativity checks, and the
//! 52-character name cap (the controller appends an 11-char `-$TIMESTAMP`
//! suffix when creating Jobs).
//!
//! `time.LoadLocation`-style timezone-DB existence checking is omitted (no tz DB
//! dependency in this crate); the IANA naming-character rules are enforced.

use crate::resources::CronJob;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::job::validate_job_template_spec;
use crate::validation::objectmeta::validate_nonnegative_field;
use once_cell::sync::Lazy;
use regex::Regex;

const ALLOW_CONCURRENT: &str = "Allow";
const FORBID_CONCURRENT: &str = "Forbid";
const REPLACE_CONCURRENT: &str = "Replace";

/// IANA timezone name-component charset (mirrors upstream `validTimeZoneCharacters`).
static VALID_TZ_CHARS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[A-Za-z\.\-_0-9+]{1,14}$").unwrap());

/// Parse a Kubernetes 5-field cron schedule using the same normalization the
/// CronJob controller applies (`?`→`*`, pad to the `cron` crate's 7-field form),
/// so create-time validation accepts exactly what the controller will run.
fn parse_schedule(schedule: &str) -> Result<(), String> {
    let normalized = schedule.replace('?', "*");
    let normalized = match normalized.split_whitespace().count() {
        5 => format!("0 {} *", normalized),
        6 => format!("0 {}", normalized),
        _ => normalized,
    };
    cron::Schedule::try_from(normalized.as_str())
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Port of upstream `validateScheduleFormat` for the create path (`allowTZ=false`).
fn validate_schedule_format(schedule: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Err(e) = parse_schedule(schedule) {
        errs.push(Error::invalid(fld_path, schedule.to_string(), e));
    }
    // On create, an inline TZ (`TZ=`/`CRON_TZ=`) is never allowed.
    if schedule.contains("TZ") {
        errs.push(Error::invalid(
            fld_path,
            schedule.to_string(),
            "cannot use TZ or CRON_TZ in schedule, use timeZone field instead",
        ));
    }
    errs
}

/// Port of upstream `validateTimeZone` (naming rules only; tz-DB existence check
/// is omitted — see module docs).
fn validate_time_zone(time_zone: Option<&str>, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(tz) = time_zone else {
        return errs;
    };
    if tz.is_empty() {
        errs.push(Error::invalid(
            fld_path,
            String::new(),
            "timeZone must be nil or non-empty string",
        ));
        return errs;
    }
    for part in tz.split('/') {
        if part == "." || part == ".." || part.starts_with('-') || !VALID_TZ_CHARS.is_match(part) {
            errs.push(Error::invalid(
                fld_path,
                tz.to_string(),
                format!("unknown time zone {}", tz),
            ));
            return errs;
        }
    }
    if tz.eq_ignore_ascii_case("Local") {
        errs.push(Error::invalid(
            fld_path,
            tz.to_string(),
            "timeZone must be an explicit time zone as defined in https://www.iana.org/time-zones",
        ));
    }
    errs
}

/// Validate a `CronJob` on create. Mirrors upstream `ValidateCronJobCreate`
/// (minus ObjectMeta, which the handler validates separately).
pub fn validate_cron_job(cj: &CronJob) -> ErrorList {
    let spec_path = Path::new("spec");
    let mut errs: ErrorList = Vec::new();
    let spec = &cj.spec;

    // schedule
    if spec.schedule.is_empty() {
        errs.push(Error::required(&spec_path.child("schedule"), ""));
    } else {
        errs.extend(validate_schedule_format(
            &spec.schedule,
            &spec_path.child("schedule"),
        ));
    }

    // startingDeadlineSeconds
    if let Some(sds) = spec.starting_deadline_seconds {
        errs.extend(validate_nonnegative_field(
            sds,
            &spec_path.child("startingDeadlineSeconds"),
        ));
    }

    // timeZone
    errs.extend(validate_time_zone(
        spec.time_zone.as_deref(),
        &spec_path.child("timeZone"),
    ));

    // concurrencyPolicy (defaulted to Allow by the handler; validate when set)
    if let Some(cp) = &spec.concurrency_policy {
        if cp != ALLOW_CONCURRENT && cp != FORBID_CONCURRENT && cp != REPLACE_CONCURRENT {
            errs.push(Error::not_supported(
                &spec_path.child("concurrencyPolicy"),
                cp.clone(),
                &[ALLOW_CONCURRENT, FORBID_CONCURRENT, REPLACE_CONCURRENT],
            ));
        }
    }

    // jobTemplate
    errs.extend(validate_job_template_spec(
        &spec.job_template,
        &spec_path.child("jobTemplate"),
    ));

    // history limits (zero is valid)
    if let Some(s) = spec.successful_jobs_history_limit {
        errs.extend(validate_nonnegative_field(
            s as i64,
            &spec_path.child("successfulJobsHistoryLimit"),
        ));
    }
    if let Some(f) = spec.failed_jobs_history_limit {
        errs.extend(validate_nonnegative_field(
            f as i64,
            &spec_path.child("failedJobsHistoryLimit"),
        ));
    }

    // Name length: the controller appends an 11-char `-$TIMESTAMP` suffix, and
    // the resulting Job name must stay within the 63-char DNS-1035 label limit.
    if cj.metadata.name.len() > 52 {
        errs.push(Error::invalid(
            &Path::new("metadata").child("name"),
            cj.metadata.name.clone(),
            "must be no more than 52 characters",
        ));
    }

    errs
}
