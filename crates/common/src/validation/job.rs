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
//! `podReplacementPolicy` and `managedBy` (#1326). The `succeededIndexes`
//! interval-format parser (`validateIndexesFormat`) is the one remaining gap.

use crate::resources::workloads::{
    Job, JobSpec, JobTemplateSpec, PodFailurePolicyRule, SuccessPolicyRule,
};
use crate::types::LabelSelector;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_qualified_name, validate_label_selector,
    LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::validate_nonnegative_field;
use std::collections::HashSet;

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
const MAX_MANAGED_BY_LENGTH: usize = 63;

const NON_INDEXED_COMPLETION: &str = "NonIndexed";
const INDEXED_COMPLETION: &str = "Indexed";
const POD_FAILURE_POLICY_ACTIONS: [&str; 4] = ["FailJob", "FailIndex", "Ignore", "Count"];
const ON_EXIT_CODES_OPERATORS: [&str; 2] = ["In", "NotIn"];
const ON_POD_CONDITIONS_STATUSES: [&str; 3] = ["True", "False", "Unknown"];
const POD_REPLACEMENT_POLICIES: [&str; 2] = ["Failed", "TerminatingOrFailed"];

fn selector_is_empty(sel: &LabelSelector) -> bool {
    sel.match_labels.as_ref().is_none_or(|m| m.is_empty())
        && sel.match_expressions.as_ref().is_none_or(|m| m.is_empty())
}

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
        }
    }
    errs
}
