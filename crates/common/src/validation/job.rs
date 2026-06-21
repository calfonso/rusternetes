//! Job validation — port of upstream Kubernetes
//! `pkg/apis/batch/validation/validation.go` (release-1.35).
//!
//! Covers the create-path spec validation exercised by clients and conformance:
//! non-negative numeric fields, `completionMode` enum + the Indexed-job rules
//! (completions required, parallelism/maxFailedIndexes caps, high-completions
//! soft limits), the indexed-job pod-hostname DNS-label check, the
//! `restartPolicy ∈ {OnFailure, Never}` rule, and selector validity + match.
//!
//! Run *after* the api-server has defaulted the spec and generated the selector
//! (`generateSelector`), mirroring upstream `ValidateJob`, which validates the
//! post-defaulting object.
//!
//! Niche policy sub-objects (`podFailurePolicy`, `successPolicy`,
//! `podReplacementPolicy`, `managedBy`) are intentionally out of scope here.

use crate::resources::workloads::{Job, JobSpec};
use crate::types::LabelSelector;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::validate_nonnegative_field;

// Upstream constants (`pkg/apis/batch/validation/validation.go`).
const MAX_PARALLELISM_FOR_INDEXED_JOB: i32 = 100_000;
const MAX_FAILED_INDEXES_FOR_INDEXED_JOB: i32 = 100_000;
const COMPLETIONS_SOFT_LIMIT: i32 = 100_000;
const PARALLELISM_LIMIT_FOR_HIGH_COMPLETIONS: i32 = 10_000;
const MAX_FAILED_INDEXES_LIMIT_FOR_HIGH_COMPLETIONS: i32 = 10_000;

const NON_INDEXED_COMPLETION: &str = "NonIndexed";
const INDEXED_COMPLETION: &str = "Indexed";

fn selector_is_empty(sel: &LabelSelector) -> bool {
    sel.match_labels.as_ref().is_none_or(|m| m.is_empty())
        && sel.match_expressions.as_ref().is_none_or(|m| m.is_empty())
}

/// Port of upstream `validateJobSpec` + the selector half of `ValidateJobSpec`.
fn validate_job_spec(spec: &JobSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(p) = spec.parallelism {
        errs.extend(validate_nonnegative_field(
            p as i64,
            &fld_path.child("parallelism"),
        ));
    }
    if let Some(c) = spec.completions {
        errs.extend(validate_nonnegative_field(
            c as i64,
            &fld_path.child("completions"),
        ));
    }
    if let Some(a) = spec.active_deadline_seconds {
        errs.extend(validate_nonnegative_field(
            a,
            &fld_path.child("activeDeadlineSeconds"),
        ));
    }
    if let Some(b) = spec.backoff_limit {
        errs.extend(validate_nonnegative_field(
            b as i64,
            &fld_path.child("backoffLimit"),
        ));
    }
    if let Some(t) = spec.ttl_seconds_after_finished {
        errs.extend(validate_nonnegative_field(
            t as i64,
            &fld_path.child("ttlSecondsAfterFinished"),
        ));
    }
    if let Some(bpi) = spec.backoff_limit_per_index {
        errs.extend(validate_nonnegative_field(
            bpi as i64,
            &fld_path.child("backoffLimitPerIndex"),
        ));
    }
    if let Some(mfi) = spec.max_failed_indexes {
        errs.extend(validate_nonnegative_field(
            mfi as i64,
            &fld_path.child("maxFailedIndexes"),
        ));
        if spec.backoff_limit_per_index.is_none() {
            errs.push(Error::required(
                &fld_path.child("backoffLimitPerIndex"),
                "when maxFailedIndexes is specified",
            ));
        }
    }

    let is_indexed = spec.completion_mode.as_deref() == Some(INDEXED_COMPLETION);
    if let Some(mode) = &spec.completion_mode {
        if mode != NON_INDEXED_COMPLETION && mode != INDEXED_COMPLETION {
            errs.push(Error::not_supported(
                &fld_path.child("completionMode"),
                mode.clone(),
                &[NON_INDEXED_COMPLETION, INDEXED_COMPLETION],
            ));
        }
        if is_indexed {
            if spec.completions.is_none() {
                errs.push(Error::required(
                    &fld_path.child("completions"),
                    format!("when completion mode is {}", INDEXED_COMPLETION),
                ));
            }
            if let Some(p) = spec.parallelism {
                if p > MAX_PARALLELISM_FOR_INDEXED_JOB {
                    errs.push(Error::invalid(
                        &fld_path.child("parallelism"),
                        p.to_string(),
                        format!(
                            "must be less than or equal to {} when completion mode is {}",
                            MAX_PARALLELISM_FOR_INDEXED_JOB, INDEXED_COMPLETION
                        ),
                    ));
                }
            }
            if let (Some(c), Some(mfi)) = (spec.completions, spec.max_failed_indexes) {
                if mfi > c {
                    errs.push(Error::invalid(
                        &fld_path.child("maxFailedIndexes"),
                        mfi.to_string(),
                        "must be less than or equal to completions",
                    ));
                }
            }
            if let Some(mfi) = spec.max_failed_indexes {
                if mfi > MAX_FAILED_INDEXES_FOR_INDEXED_JOB {
                    errs.push(Error::invalid(
                        &fld_path.child("maxFailedIndexes"),
                        mfi.to_string(),
                        format!(
                            "must be less than or equal to {}",
                            MAX_FAILED_INDEXES_FOR_INDEXED_JOB
                        ),
                    ));
                }
            }
            if let Some(c) = spec.completions {
                if c > COMPLETIONS_SOFT_LIMIT && spec.backoff_limit_per_index.is_some() {
                    if spec.max_failed_indexes.is_none() {
                        errs.push(Error::required(
                            &fld_path.child("maxFailedIndexes"),
                            format!(
                                "must be specified when completions is above {}",
                                COMPLETIONS_SOFT_LIMIT
                            ),
                        ));
                    }
                    if let Some(p) = spec.parallelism {
                        if p > PARALLELISM_LIMIT_FOR_HIGH_COMPLETIONS {
                            errs.push(Error::invalid(
                                &fld_path.child("parallelism"),
                                p.to_string(),
                                format!(
                                    "must be less than or equal to {} when completions are above {} and used with backoff limit per index",
                                    PARALLELISM_LIMIT_FOR_HIGH_COMPLETIONS, COMPLETIONS_SOFT_LIMIT
                                ),
                            ));
                        }
                    }
                    if let Some(mfi) = spec.max_failed_indexes {
                        if mfi > MAX_FAILED_INDEXES_LIMIT_FOR_HIGH_COMPLETIONS {
                            errs.push(Error::invalid(
                                &fld_path.child("maxFailedIndexes"),
                                mfi.to_string(),
                                format!(
                                    "must be less than or equal to {} when completions are above {} and used with backoff limit per index",
                                    MAX_FAILED_INDEXES_LIMIT_FOR_HIGH_COMPLETIONS, COMPLETIONS_SOFT_LIMIT
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }

    // backoffLimitPerIndex / maxFailedIndexes require indexed completion mode.
    if !is_indexed {
        if let Some(bpi) = spec.backoff_limit_per_index {
            errs.push(Error::invalid(
                &fld_path.child("backoffLimitPerIndex"),
                bpi.to_string(),
                "requires indexed completion mode",
            ));
        }
        if let Some(mfi) = spec.max_failed_indexes {
            errs.push(Error::invalid(
                &fld_path.child("maxFailedIndexes"),
                mfi.to_string(),
                "requires indexed completion mode",
            ));
        }
    }

    // template.spec.restartPolicy must be OnFailure or Never (upstream rejects
    // the SetDefaults_PodSpec-defaulted "Always"/empty for Jobs).
    let rp_path = fld_path
        .child("template")
        .child("spec")
        .child("restartPolicy");
    let restart_policy = spec.template.spec.restart_policy.as_deref().unwrap_or("");
    if restart_policy.is_empty() || restart_policy == "Always" {
        errs.push(Error::required(
            &rp_path,
            "valid values: \"OnFailure\", \"Never\"",
        ));
    } else if restart_policy != "OnFailure" && restart_policy != "Never" {
        errs.push(Error::not_supported(
            &rp_path,
            restart_policy.to_string(),
            &["OnFailure", "Never"],
        ));
    }

    // Selector: required, valid, and matching the template labels.
    match &spec.selector {
        None => errs.push(Error::required(&fld_path.child("selector"), "")),
        Some(sel) => {
            errs.extend(validate_label_selector(
                sel,
                LabelSelectorValidationOptions::default(),
                &fld_path.child("selector"),
            ));
            if let Some(match_labels) = &sel.match_labels {
                if !selector_is_empty(sel) {
                    let template_labels = spec
                        .template
                        .metadata
                        .as_ref()
                        .and_then(|m| m.labels.as_ref());
                    let matches = match template_labels {
                        Some(tl) => match_labels.iter().all(|(k, v)| tl.get(k) == Some(v)),
                        None => match_labels.is_empty(),
                    };
                    if !matches {
                        errs.push(Error::invalid(
                            &fld_path.child("template").child("metadata").child("labels"),
                            template_labels
                                .map(|tl| {
                                    tl.iter()
                                        .map(|(k, v)| format!("{k}={v}"))
                                        .collect::<Vec<_>>()
                                        .join(",")
                                })
                                .unwrap_or_default(),
                            "`selector` does not match template `labels`",
                        ));
                    }
                }
            }
        }
    }

    errs
}

/// Validate a `Job` on create. Mirrors upstream `ValidateJob` (minus ObjectMeta,
/// which the handler validates via `validate_create_object_meta`).
pub fn validate_job(job: &Job) -> ErrorList {
    let mut errs = validate_job_spec(&job.spec, &Path::new("spec"));

    // Indexed job pods get a `-$INDEX` hostname suffix; the max index is
    // `completions-1`. Reject names that would yield an invalid DNS-1123 label.
    if job.spec.completion_mode.as_deref() == Some(INDEXED_COMPLETION) {
        if let Some(c) = job.spec.completions {
            if c > 0 {
                let max_hostname = format!("{}-{}", job.metadata.name, c - 1);
                if !is_dns1123_label(&max_hostname).is_empty() {
                    errs.push(Error::invalid(
                        &Path::new("metadata").child("name"),
                        job.metadata.name.clone(),
                        format!(
                            "will not able to create pod with invalid DNS label: {}",
                            max_hostname
                        ),
                    ));
                }
            }
        }
    }

    errs
}
