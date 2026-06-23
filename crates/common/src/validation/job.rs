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
//! Also covers the policy sub-objects `podFailurePolicy`, `successPolicy`,
//! `podReplacementPolicy` and `managedBy` (#1326), including the
//! `successPolicy.rules[].succeededIndexes` interval-format validation
//! (`validateIndexesFormat`) and the `succeededCount <= totalIndexes`
//! cross-check (#1344).

use crate::resources::workloads::{
    Job, JobSpec, JobTemplateSpec, PodFailurePolicyRule, SuccessPolicyRule,
};
use crate::types::LabelSelector;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_qualified_name, label_selector_matches_labels,
    validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::validate_nonnegative_field;
use std::collections::HashSet;

// Upstream `pkg/apis/batch/types.go` label keys (prefix `batch.kubernetes.io/`).
const LEGACY_JOB_NAME_LABEL: &str = "job-name";
const LEGACY_CONTROLLER_UID_LABEL: &str = "controller-uid";
const JOB_NAME_LABEL: &str = "batch.kubernetes.io/job-name";
const CONTROLLER_UID_LABEL: &str = "batch.kubernetes.io/controller-uid";

// Upstream constants (`pkg/apis/batch/validation/validation.go`).
const MAX_PARALLELISM_FOR_INDEXED_JOB: i32 = 100_000;
const MAX_FAILED_INDEXES_FOR_INDEXED_JOB: i32 = 100_000;
const COMPLETIONS_SOFT_LIMIT: i32 = 100_000;
const PARALLELISM_LIMIT_FOR_HIGH_COMPLETIONS: i32 = 10_000;
const MAX_FAILED_INDEXES_LIMIT_FOR_HIGH_COMPLETIONS: i32 = 10_000;
const MAX_POD_FAILURE_POLICY_RULES: usize = 20;
const MAX_ON_EXIT_CODES_VALUES: usize = 255;
const MAX_ON_POD_CONDITIONS_PATTERNS: usize = 20;
const MAX_SUCCESS_POLICY_RULES: usize = 20;
/// Upstream `maxJobSuccessPolicySucceededIndexesLimit` — 64 KiB cap on the
/// `succeededIndexes` string.
const MAX_JOB_SUCCESS_POLICY_SUCCEEDED_INDEXES_LIMIT: usize = 64 * 1024;

/// Parse one `succeededIndexes` interval (`"3"` or `"3-5"`) against
/// `completions`, returning the inclusive `(start, end)`. Mirrors upstream
/// `parseIndexInterval` (pkg/apis/batch/validation).
fn parse_index_interval(interval: &str, completions: i32) -> Result<(i32, i32), String> {
    let limits: Vec<&str> = interval.split('-').collect();
    if limits.len() > 2 {
        return Err(format!(
            "the fragment {interval:?} violates the requirement that an index interval can have at most two parts separated by '-'"
        ));
    }
    let x: i32 = limits[0].parse().map_err(|_| {
        format!(
            "cannot convert string to integer for index: {:?}",
            limits[0]
        )
    })?;
    if x >= completions {
        return Err(format!("too large index: {:?}", limits[0]));
    }
    if limits.len() == 2 {
        let y: i32 = limits[1].parse().map_err(|_| {
            format!(
                "cannot convert string to integer for index: {:?}",
                limits[1]
            )
        })?;
        if y >= completions {
            return Err(format!("too large index: {:?}", limits[1]));
        }
        if x >= y {
            return Err(format!("non-increasing order, previous: {x}, current: {y}"));
        }
        return Ok((x, y));
    }
    Ok((x, x))
}

/// Parse a `succeededIndexes` string (`"1,3-5,7"`) against `completions`,
/// returning the total number of covered indexes. Intervals must be in strictly
/// increasing, non-overlapping order. Mirrors upstream `validateIndexesFormat`.
fn validate_indexes_format(indexes: &str, completions: i32) -> Result<i32, String> {
    if indexes.is_empty() {
        return Ok(0);
    }
    let mut last_index: Option<i32> = None;
    let mut total: i32 = 0;
    for interval in indexes.split(',') {
        let (x, y) = parse_index_interval(interval, completions)?;
        if let Some(last) = last_index {
            if last >= x {
                return Err(format!(
                    "non-increasing order, previous: {last}, current: {x}"
                ));
            }
        }
        total += y - x + 1;
        last_index = Some(y);
    }
    Ok(total)
}
const MAX_MANAGED_BY_LENGTH: usize = 63;

const NON_INDEXED_COMPLETION: &str = "NonIndexed";
const INDEXED_COMPLETION: &str = "Indexed";
const POD_FAILURE_POLICY_ACTIONS: [&str; 4] = ["FailJob", "FailIndex", "Ignore", "Count"];
const ON_EXIT_CODES_OPERATORS: [&str; 2] = ["In", "NotIn"];
const ON_POD_CONDITIONS_STATUSES: [&str; 3] = ["True", "False", "Unknown"];
const POD_REPLACEMENT_POLICIES: [&str; 2] = ["Failed", "TerminatingOrFailed"];

/// Port of upstream `validateJobSpec` (the inner validator, WITHOUT the
/// selector-required / selector-match checks that `ValidateJobSpec` adds). This
/// is the part shared with CronJob's `ValidateJobTemplateSpec`, whose template
/// must NOT carry a selector.
fn validate_job_spec_core(spec: &JobSpec, fld_path: &Path) -> ErrorList {
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
    // the SetDefaults_PodSpec-defaulted "Always"/empty for Jobs). With a
    // podFailurePolicy, only "Never" is permitted (upstream validation.go:287).
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
    } else if spec.pod_failure_policy.is_some() && restart_policy != "Never" {
        errs.push(Error::invalid(
            &rp_path,
            restart_policy.to_string(),
            "only \"Never\" is supported when podFailurePolicy is specified",
        ));
    }

    // managedBy — domain-prefixed path, length-bounded.
    if let Some(managed_by) = &spec.managed_by {
        let mb_path = fld_path.child("managedBy");
        errs.extend(validate_domain_prefixed_path(managed_by, &mb_path));
        if managed_by.len() > MAX_MANAGED_BY_LENGTH {
            errs.push(Error::too_long(&mb_path, MAX_MANAGED_BY_LENGTH));
        }
    }

    // podFailurePolicy.
    if let Some(pfp) = &spec.pod_failure_policy {
        errs.extend(validate_pod_failure_policy(
            spec,
            &pfp.rules,
            &fld_path.child("podFailurePolicy"),
        ));
    }

    // successPolicy — Indexed-only; then the per-rule checks.
    if let Some(sp) = &spec.success_policy {
        let sp_path = fld_path.child("successPolicy");
        if !is_indexed {
            errs.push(Error::invalid(
                &sp_path,
                String::new(),
                "requires indexed completion mode",
            ));
        } else {
            errs.extend(validate_success_policy(spec, &sp.rules, &sp_path));
        }
    }

    // podReplacementPolicy.
    errs.extend(validate_pod_replacement_policy(
        spec,
        &fld_path.child("podReplacementPolicy"),
    ));

    errs
}

/// Port of upstream `ValidateJobSpec`: the core spec checks plus the
/// selector-required / selector validity / selector-matches-template checks.
fn validate_job_spec(spec: &JobSpec, fld_path: &Path) -> ErrorList {
    let mut errs = validate_job_spec_core(spec, fld_path);

    // Selector: required and valid. Upstream `ValidateJobSpec` only requires +
    // validates the selector here; the match check below runs regardless.
    match &spec.selector {
        None => errs.push(Error::required(&fld_path.child("selector"), "")),
        Some(sel) => {
            errs.extend(validate_label_selector(
                sel,
                LabelSelectorValidationOptions::default(),
                &fld_path.child("selector"),
            ));
        }
    }

    // Whether manually or automatically generated, the selector of the job must
    // match the pods it will produce — honoring `matchExpressions`, not only
    // `matchLabels` (upstream validation.go:182-187 via `LabelSelectorAsSelector`).
    if let Some(sel) = &spec.selector {
        let empty_labels = std::collections::HashMap::new();
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.as_ref())
            .unwrap_or(&empty_labels);
        if !label_selector_matches_labels(sel, template_labels) {
            errs.push(Error::invalid(
                &fld_path.child("template").child("metadata").child("labels"),
                template_labels
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(","),
                "`selector` does not match template `labels`",
            ));
        }
    }

    errs
}

/// Port of upstream `ValidateJobTemplateSpec` — validates the embedded job spec
/// of a CronJob's `jobTemplate`. The template must NOT carry a selector (it is
/// auto-generated) and must not set `manualSelector: true`.
pub fn validate_job_template_spec(template: &JobTemplateSpec, fld_path: &Path) -> ErrorList {
    let spec_path = fld_path.child("spec");
    let mut errs = validate_job_spec_core(&template.spec, &spec_path);

    if template.spec.selector.is_some() {
        errs.push(Error::invalid(
            &spec_path.child("selector"),
            String::new(),
            "`selector` will be auto-generated",
        ));
    }
    if template.spec.manual_selector == Some(true) {
        errs.push(Error::not_supported(
            &spec_path.child("manualSelector"),
            "true",
            &["nil", "false"],
        ));
    }
    errs
}

/// Port of upstream `apivalidation.ValidateHasLabel`: the label `key` must be
/// present on `meta` and equal to `expected_value`.
fn validate_has_label(
    labels: Option<&std::collections::HashMap<String, String>>,
    fld_path: &Path,
    key: &str,
    expected_value: &str,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match labels.and_then(|l| l.get(key)) {
        None => errs.push(Error::required(
            &fld_path.child("labels").key(key),
            format!("must be '{expected_value}'"),
        )),
        Some(actual) if actual != expected_value => errs.push(Error::invalid(
            &fld_path.child("labels").key(key),
            labels
                .map(|l| {
                    l.iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default(),
            format!("must be '{expected_value}'"),
        )),
        _ => {}
    }
    errs
}

/// Port of upstream `validateGeneratedSelector`: when the selector is
/// auto-generated (not `manualSelector`), the pod template must carry the
/// generated `controller-uid` / `job-name` labels (prefixed + legacy) matching
/// the Job's `uid` / `name`, the Job's `uid` must be set, and the selector must
/// match those generated labels (else `selector` not auto-generated).
///
/// `validate_batch_labels` mirrors upstream `opts.RequirePrefixedLabels`; on the
/// create path it is `true`.
fn validate_generated_selector(job: &Job, validate_batch_labels: bool) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if job.spec.manual_selector == Some(true) {
        return errs;
    }
    // Already reported as required by the caller when absent.
    let Some(selector) = &job.spec.selector else {
        return errs;
    };

    // An unset uid would yield "controller-uid=" as the selector, which is bad.
    if job.metadata.uid.is_empty() {
        errs.push(Error::required(&Path::new("metadata").child("uid"), ""));
    }

    let template_meta = job.spec.template.metadata.as_ref();
    let template_labels = template_meta.and_then(|m| m.labels.as_ref());
    let template_path = Path::new("spec").child("template").child("metadata");

    // The expected (generated) labels the selector must match.
    let mut expected_labels: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    errs.extend(validate_has_label(
        template_labels,
        &template_path,
        LEGACY_CONTROLLER_UID_LABEL,
        &job.metadata.uid,
    ));
    errs.extend(validate_has_label(
        template_labels,
        &template_path,
        LEGACY_JOB_NAME_LABEL,
        &job.metadata.name,
    ));
    if validate_batch_labels {
        errs.extend(validate_has_label(
            template_labels,
            &template_path,
            CONTROLLER_UID_LABEL,
            &job.metadata.uid,
        ));
        errs.extend(validate_has_label(
            template_labels,
            &template_path,
            JOB_NAME_LABEL,
            &job.metadata.name,
        ));
        expected_labels.insert(CONTROLLER_UID_LABEL.to_string(), job.metadata.uid.clone());
        expected_labels.insert(JOB_NAME_LABEL.to_string(), job.metadata.name.clone());
    }
    // Labels created by the Kubernetes project carry a prefix; the legacy
    // (unprefixed) ones are set for backward compatibility.
    expected_labels.insert(
        LEGACY_CONTROLLER_UID_LABEL.to_string(),
        job.metadata.uid.clone(),
    );
    expected_labels.insert(LEGACY_JOB_NAME_LABEL.to_string(), job.metadata.name.clone());

    // The selector must match the generated labels.
    if !label_selector_matches_labels(selector, &expected_labels) {
        errs.push(Error::invalid(
            &Path::new("spec").child("selector"),
            selector_display(selector),
            "`selector` not auto-generated",
        ));
    }

    errs
}

/// Compact `key=value` rendering of a `LabelSelector` for error messages.
fn selector_display(sel: &LabelSelector) -> String {
    sel.match_labels
        .as_ref()
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

/// Validate a `Job` on create. Mirrors upstream `ValidateJob` (minus ObjectMeta,
/// which the handler validates via `validate_create_object_meta`). The create
/// path uses `RequirePrefixedLabels: true`.
pub fn validate_job(job: &Job) -> ErrorList {
    let mut errs = validate_generated_selector(job, true);
    errs.extend(validate_job_spec(&job.spec, &Path::new("spec")));

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

/// Port of upstream `IsDomainPrefixedPath` (structure + host subdomain). The
/// trailing path segment's `httpPathRegexp` is not replicated.
fn validate_domain_prefixed_path(value: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if value.is_empty() {
        errs.push(Error::required(fld_path, ""));
        return errs;
    }
    let segments: Vec<&str> = value.splitn(2, '/').collect();
    if segments.len() != 2 || segments[0].is_empty() || segments[1].is_empty() {
        errs.push(Error::invalid(
            fld_path,
            value.to_string(),
            "must be a domain-prefixed path (such as \"acme.io/foo\")",
        ));
        return errs;
    }
    for msg in is_dns1123_subdomain(segments[0]) {
        errs.push(Error::invalid(fld_path, segments[0].to_string(), msg));
    }
    errs
}

/// Port of upstream `validatePodReplacementPolicy`.
fn validate_pod_replacement_policy(spec: &JobSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(prp) = &spec.pod_replacement_policy {
        if spec.pod_failure_policy.is_some() {
            // With a podFailurePolicy, only "Failed" is allowed.
            if prp != "Failed" {
                errs.push(Error::not_supported(fld_path, prp.clone(), &["Failed"]));
            }
        } else if !POD_REPLACEMENT_POLICIES.contains(&prp.as_str()) {
            errs.push(Error::not_supported(
                fld_path,
                prp.clone(),
                &POD_REPLACEMENT_POLICIES,
            ));
        }
    }
    errs
}

/// Port of upstream `validatePodFailurePolicy` + `validatePodFailurePolicyRule`
/// (+ the onExitCodes / onPodConditions sub-validators).
fn validate_pod_failure_policy(
    spec: &JobSpec,
    rules: &[PodFailurePolicyRule],
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let rules_path = fld_path.child("rules");
    if rules.len() > MAX_POD_FAILURE_POLICY_RULES {
        errs.push(Error::too_many(&rules_path, MAX_POD_FAILURE_POLICY_RULES));
    }

    let mut container_names: HashSet<&str> = HashSet::new();
    for c in &spec.template.spec.containers {
        container_names.insert(c.name.as_str());
    }
    if let Some(inits) = &spec.template.spec.init_containers {
        for c in inits {
            container_names.insert(c.name.as_str());
        }
    }

    for (i, rule) in rules.iter().enumerate() {
        let rule_path = rules_path.index(i);
        let action_path = rule_path.child("action");
        if rule.action.is_empty() {
            errs.push(Error::required(
                &action_path,
                "valid values: \"Count\", \"FailIndex\", \"FailJob\", \"Ignore\"",
            ));
        } else if rule.action == "FailIndex" {
            if spec.backoff_limit_per_index.is_none() {
                errs.push(Error::invalid(
                    &action_path,
                    rule.action.clone(),
                    "requires the backoffLimitPerIndex to be set",
                ));
            }
        } else if !POD_FAILURE_POLICY_ACTIONS.contains(&rule.action.as_str()) {
            errs.push(Error::not_supported(
                &action_path,
                rule.action.clone(),
                &POD_FAILURE_POLICY_ACTIONS,
            ));
        }

        if let Some(on_exit) = &rule.on_exit_codes {
            let oec_path = rule_path.child("onExitCodes");
            let op_path = oec_path.child("operator");
            if on_exit.operator.is_empty() {
                errs.push(Error::required(&op_path, "valid values: \"In\", \"NotIn\""));
            } else if !ON_EXIT_CODES_OPERATORS.contains(&on_exit.operator.as_str()) {
                errs.push(Error::not_supported(
                    &op_path,
                    on_exit.operator.clone(),
                    &ON_EXIT_CODES_OPERATORS,
                ));
            }
            if let Some(cn) = &on_exit.container_name {
                if !container_names.contains(cn.as_str()) {
                    errs.push(Error::invalid(
                        &oec_path.child("containerName"),
                        cn.clone(),
                        "must be one of the container or initContainer names in the pod template",
                    ));
                }
            }
            let values_path = oec_path.child("values");
            if on_exit.values.is_empty() {
                errs.push(Error::invalid(
                    &values_path,
                    String::new(),
                    "at least one value is required",
                ));
            } else if on_exit.values.len() > MAX_ON_EXIT_CODES_VALUES {
                errs.push(Error::too_many(&values_path, MAX_ON_EXIT_CODES_VALUES));
            }
            let mut seen: HashSet<i32> = HashSet::new();
            let mut ordered = true;
            for (j, &v) in on_exit.values.iter().enumerate() {
                let vp = values_path.index(j);
                if on_exit.operator == "In" && v == 0 {
                    errs.push(Error::invalid(&vp, v, "must not be 0 for the In operator"));
                }
                if !seen.insert(v) {
                    errs.push(Error::duplicate(&vp, v));
                }
                if j > 0 && on_exit.values[j - 1] > v {
                    ordered = false;
                }
            }
            if !ordered {
                errs.push(Error::invalid(
                    &values_path,
                    String::new(),
                    "must be ordered",
                ));
            }
        }

        if !rule.on_pod_conditions.is_empty() {
            let opc_path = rule_path.child("onPodConditions");
            if rule.on_pod_conditions.len() > MAX_ON_POD_CONDITIONS_PATTERNS {
                errs.push(Error::too_many(&opc_path, MAX_ON_POD_CONDITIONS_PATTERNS));
            }
            for (j, pattern) in rule.on_pod_conditions.iter().enumerate() {
                let p_path = opc_path.index(j);
                for msg in is_qualified_name(&pattern.condition_type) {
                    errs.push(Error::invalid(
                        &p_path.child("type"),
                        pattern.condition_type.clone(),
                        msg,
                    ));
                }
                let status_path = p_path.child("status");
                match pattern.status.as_deref() {
                    None | Some("") => errs.push(Error::required(
                        &status_path,
                        "valid values: \"False\", \"True\", \"Unknown\"",
                    )),
                    Some(s) if !ON_POD_CONDITIONS_STATUSES.contains(&s) => {
                        errs.push(Error::not_supported(
                            &status_path,
                            s.to_string(),
                            &ON_POD_CONDITIONS_STATUSES,
                        ))
                    }
                    _ => {}
                }
            }
        }

        let has_exit = rule.on_exit_codes.is_some();
        let has_cond = !rule.on_pod_conditions.is_empty();
        if has_exit && has_cond {
            errs.push(Error::invalid(
                &rule_path,
                String::new(),
                "specifying both OnExitCodes and OnPodConditions is not supported",
            ));
        }
        if !has_exit && !has_cond {
            errs.push(Error::invalid(
                &rule_path,
                String::new(),
                "specifying one of OnExitCodes and OnPodConditions is required",
            ));
        }
    }
    errs
}

/// Port of upstream `validateSuccessPolicy` + `validateSuccessPolicyRule`. The
/// `succeededIndexes` interval-format parser (`validateIndexesFormat`) is left
/// as a follow-up; presence/count + `succeededCount` bounds are ported.
fn validate_success_policy(
    spec: &JobSpec,
    rules: &[SuccessPolicyRule],
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let rules_path = fld_path.child("rules");
    if rules.is_empty() {
        errs.push(Error::required(
            &rules_path,
            "at least one rules must be specified when the successPolicy is specified",
        ));
    }
    if rules.len() > MAX_SUCCESS_POLICY_RULES {
        errs.push(Error::too_many(&rules_path, MAX_SUCCESS_POLICY_RULES));
    }
    for (i, rule) in rules.iter().enumerate() {
        let rule_path = rules_path.index(i);
        if rule.succeeded_count.is_none() && rule.succeeded_indexes.is_none() {
            errs.push(Error::required(
                &rule_path,
                "at least one of succeededCount or succeededIndexes must be specified",
            ));
        }
        // succeededIndexes: length cap + interval-format parse (upstream
        // validateSuccessPolicyRule). `total_indexes` feeds the succeededCount
        // cross-check below.
        let mut total_indexes: i32 = 0;
        if let Some(indexes) = &rule.succeeded_indexes {
            let sip = rule_path.child("succeededIndexes");
            if indexes.len() > MAX_JOB_SUCCESS_POLICY_SUCCEEDED_INDEXES_LIMIT {
                errs.push(Error::too_long(
                    &sip,
                    MAX_JOB_SUCCESS_POLICY_SUCCEEDED_INDEXES_LIMIT,
                ));
            }
            let completions = spec.completions.unwrap_or(0);
            match validate_indexes_format(indexes, completions) {
                Ok(t) => total_indexes = t,
                Err(e) => errs.push(Error::invalid(
                    &sip,
                    indexes.clone(),
                    format!("error parsing succeededIndexes: {e}"),
                )),
            }
        }
        if let Some(count) = rule.succeeded_count {
            let cp = rule_path.child("succeededCount");
            errs.extend(validate_nonnegative_field(count as i64, &cp));
            if let Some(completions) = spec.completions {
                if count > completions {
                    errs.push(Error::invalid(
                        &cp,
                        count,
                        format!(
                            "must be less than or equal to {} (the number of specified completions)",
                            completions
                        ),
                    ));
                }
            }
            if rule.succeeded_indexes.is_some() && count > total_indexes {
                errs.push(Error::invalid(
                    &cp,
                    count,
                    format!(
                        "must be less than or equal to {} (the number of indexes in the specified succeededIndexes field)",
                        total_indexes
                    ),
                ));
            }
        }
    }
    errs
}

#[cfg(test)]
mod success_policy_indexes_tests {
    use super::{parse_index_interval, validate_indexes_format};

    #[test]
    fn single_and_range_intervals() {
        assert_eq!(parse_index_interval("3", 5), Ok((3, 3)));
        assert_eq!(parse_index_interval("1-3", 5), Ok((1, 3)));
    }

    #[test]
    fn index_out_of_range_rejected() {
        assert!(parse_index_interval("5", 5).is_err()); // >= completions
        assert!(parse_index_interval("2-5", 5).is_err());
    }

    #[test]
    fn non_increasing_interval_rejected() {
        assert!(parse_index_interval("3-1", 5).is_err());
    }

    #[test]
    fn total_count_for_valid_format() {
        assert_eq!(validate_indexes_format("", 5), Ok(0));
        assert_eq!(validate_indexes_format("0-2", 5), Ok(3));
        assert_eq!(validate_indexes_format("1,3-5,7", 8), Ok(5));
    }

    #[test]
    fn non_increasing_across_intervals_rejected() {
        // second interval starts at/below the previous end
        assert!(validate_indexes_format("0-3,2", 8).is_err());
        assert!(validate_indexes_format("2,2", 8).is_err());
    }

    #[test]
    fn three_part_interval_rejected() {
        assert!(parse_index_interval("1-2-3", 8).is_err());
    }
}

#[cfg(test)]
mod parity_tests {
    use super::*;
    use crate::resources::pod::{Container, PodSpec};
    use crate::resources::workloads::{
        Job, JobSpec, PodFailurePolicy, PodFailurePolicyOnPodConditionsPattern,
        PodFailurePolicyRule, PodTemplateSpec,
    };
    use crate::types::{LabelSelector, LabelSelectorRequirement, ObjectMeta, TypeMeta};
    use std::collections::HashMap;

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn container() -> Container {
        Container {
            name: "main".to_string(),
            ..Default::default()
        }
    }

    /// A Job whose auto-generated selector + template labels are consistent,
    /// as the api-server's `generateSelector` would produce: prefixed + legacy
    /// controller-uid / job-name labels, selector on the prefixed controller-uid.
    fn valid_generated_job() -> Job {
        let uid = "abc-123".to_string();
        let name = "myjob".to_string();
        let template_labels = labels(&[
            ("controller-uid", &uid),
            ("job-name", &name),
            ("batch.kubernetes.io/controller-uid", &uid),
            ("batch.kubernetes.io/job-name", &name),
        ]);
        let mut meta = ObjectMeta::new(&name);
        meta.uid = uid.clone();
        Job {
            type_meta: TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: meta,
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(template_labels),
                        ..ObjectMeta::default()
                    }),
                    spec: PodSpec {
                        containers: vec![container()],
                        restart_policy: Some("Never".to_string()),
                        ..PodSpec::default()
                    },
                },
                completions: None,
                parallelism: None,
                backoff_limit: None,
                active_deadline_seconds: None,
                selector: Some(LabelSelector {
                    match_labels: Some(labels(&[("batch.kubernetes.io/controller-uid", &uid)])),
                    match_expressions: None,
                }),
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
            status: None,
        }
    }

    fn has_error_containing(errs: &ErrorList, detail_needle: &str) -> bool {
        errs.iter().any(|e| e.detail.contains(detail_needle))
    }

    fn has_error_on_field(errs: &ErrorList, field_needle: &str) -> bool {
        errs.iter().any(|e| e.field.contains(field_needle))
    }

    // --- baseline -----------------------------------------------------------

    #[test]
    fn valid_generated_job_passes() {
        let errs = validate_job(&valid_generated_job());
        assert!(errs.is_empty(), "expected no errors, got: {errs:?}");
    }

    // --- rule 1: podFailurePolicy ⇒ restartPolicy must be Never --------------

    #[test]
    fn pod_failure_policy_requires_never_restart_policy() {
        let mut job = valid_generated_job();
        job.spec.template.spec.restart_policy = Some("OnFailure".to_string());
        job.spec.pod_failure_policy = Some(PodFailurePolicy {
            rules: vec![PodFailurePolicyRule {
                action: "Ignore".to_string(),
                on_exit_codes: None,
                on_pod_conditions: vec![PodFailurePolicyOnPodConditionsPattern {
                    condition_type: "DisruptionTarget".to_string(),
                    status: Some("True".to_string()),
                }],
            }],
        });
        let errs = validate_job(&job);
        assert!(
            has_error_containing(
                &errs,
                "only \"Never\" is supported when podFailurePolicy is specified"
            ),
            "got: {errs:?}"
        );
        assert!(has_error_on_field(&errs, "template.spec.restartPolicy"));
    }

    #[test]
    fn pod_failure_policy_with_never_restart_policy_ok() {
        let mut job = valid_generated_job();
        job.spec.template.spec.restart_policy = Some("Never".to_string());
        job.spec.pod_failure_policy = Some(PodFailurePolicy {
            rules: vec![PodFailurePolicyRule {
                action: "Ignore".to_string(),
                on_exit_codes: None,
                on_pod_conditions: vec![PodFailurePolicyOnPodConditionsPattern {
                    condition_type: "DisruptionTarget".to_string(),
                    status: Some("True".to_string()),
                }],
            }],
        });
        let errs = validate_job(&job);
        assert!(
            !has_error_containing(
                &errs,
                "only \"Never\" is supported when podFailurePolicy is specified"
            ),
            "got: {errs:?}"
        );
    }

    // --- rule 2: validateGeneratedSelector -----------------------------------

    #[test]
    fn generated_selector_missing_prefixed_labels_rejected() {
        let mut job = valid_generated_job();
        // Drop the prefixed labels, leaving only the legacy ones.
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[
            ("controller-uid", "abc-123"),
            ("job-name", "myjob"),
        ]));
        // Selector still references the prefixed key, so it won't match either.
        let errs = validate_job(&job);
        assert!(
            has_error_containing(&errs, "must be 'abc-123'")
                || has_error_containing(&errs, "must be 'myjob'"),
            "expected ValidateHasLabel errors, got: {errs:?}"
        );
        assert!(has_error_on_field(
            &errs,
            "spec.template.metadata.labels[batch.kubernetes.io/controller-uid]"
        ));
    }

    #[test]
    fn generated_selector_wrong_uid_label_rejected() {
        let mut job = valid_generated_job();
        // controller-uid label disagrees with metadata.uid.
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[
            ("controller-uid", "wrong"),
            ("job-name", "myjob"),
            ("batch.kubernetes.io/controller-uid", "wrong"),
            ("batch.kubernetes.io/job-name", "myjob"),
        ]));
        let errs = validate_job(&job);
        assert!(
            has_error_containing(&errs, "must be 'abc-123'"),
            "got: {errs:?}"
        );
    }

    #[test]
    fn generated_selector_mismatch_reported() {
        let mut job = valid_generated_job();
        // Selector points at a uid that no generated label carries.
        job.spec.selector = Some(LabelSelector {
            match_labels: Some(labels(&[(
                "batch.kubernetes.io/controller-uid",
                "different-uid",
            )])),
            match_expressions: None,
        });
        let errs = validate_job(&job);
        assert!(
            has_error_containing(&errs, "`selector` not auto-generated"),
            "got: {errs:?}"
        );
    }

    #[test]
    fn generated_selector_missing_uid_rejected() {
        let mut job = valid_generated_job();
        job.metadata.uid = String::new();
        // Keep labels matching the (now empty) uid so only the uid-required
        // error is the headline; selector still references the old uid value.
        let errs = validate_job(&job);
        assert!(has_error_on_field(&errs, "metadata.uid"), "got: {errs:?}");
    }

    #[test]
    fn manual_selector_skips_generated_checks() {
        let mut job = valid_generated_job();
        job.spec.manual_selector = Some(true);
        // Only matchLabels that the template carries; no prefixed labels needed.
        job.spec.selector = Some(LabelSelector {
            match_labels: Some(labels(&[("app", "x")])),
            match_expressions: None,
        });
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[("app", "x")]));
        let errs = validate_job(&job);
        assert!(
            !has_error_containing(&errs, "`selector` not auto-generated"),
            "got: {errs:?}"
        );
        assert!(
            !has_error_containing(&errs, "must be 'abc-123'"),
            "manualSelector should skip generated-label checks, got: {errs:?}"
        );
    }

    // --- rule 3: selector match honors matchExpressions ----------------------

    #[test]
    fn selector_match_expressions_satisfied_ok() {
        let mut job = valid_generated_job();
        job.spec.manual_selector = Some(true); // isolate the match check
        job.spec.selector = Some(LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "tier".to_string(),
                operator: "In".to_string(),
                values: Some(vec!["fe".to_string(), "be".to_string()]),
            }]),
        });
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[("tier", "fe")]));
        let errs = validate_job(&job);
        assert!(
            !has_error_containing(&errs, "`selector` does not match template `labels`"),
            "matchExpressions should be honored, got: {errs:?}"
        );
    }

    #[test]
    fn selector_match_expressions_violated_rejected() {
        let mut job = valid_generated_job();
        job.spec.manual_selector = Some(true);
        job.spec.selector = Some(LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "tier".to_string(),
                operator: "In".to_string(),
                values: Some(vec!["fe".to_string()]),
            }]),
        });
        // Template label value not in the In-set ⇒ no match.
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[("tier", "db")]));
        let errs = validate_job(&job);
        assert!(
            has_error_containing(&errs, "`selector` does not match template `labels`"),
            "matchExpressions mismatch must be caught, got: {errs:?}"
        );
    }
}
