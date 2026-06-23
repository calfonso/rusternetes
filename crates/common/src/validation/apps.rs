//! Deployment field validation — port of upstream Kubernetes
//! `pkg/apis/apps/validation/validation.go` (release-1.35).
//!
//! Two public entry points:
//! * [`validate_deployment`] — create-path validation (all fields).
//! * [`validate_deployment_update`] — update-path validation (immutability +
//!   create checks on the new object).
//!
//! Mirrors upstream structure: validators return [`ErrorList`] and *accumulate*
//! every problem rather than short-circuiting on the first failure. Field paths
//! and error wording match upstream byte-for-byte so conformance log greps
//! stay valid.
//!
//! Upstream:
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/apis/apps/validation/validation.go>

use crate::resources::deployment::{Deployment, DeploymentStrategy, RollingUpdateDeployment};
use crate::resources::workloads::{
    DaemonSet, DaemonSetSpec, ReplicaSet, ReplicaSetSpec, StatefulSet, StatefulSetSpec,
};
use crate::types::LabelSelector;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, validate_label_selector, LabelSelectorValidationOptions,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the selector selects nothing (both fields absent or
/// empty). An empty selector is forbidden by upstream.
fn selector_is_empty(sel: &LabelSelector) -> bool {
    sel.match_labels.as_ref().is_none_or(|m| m.is_empty())
        && sel.match_expressions.as_ref().is_none_or(|m| m.is_empty())
}

use crate::validation::metav1::label_selector_matches_labels as selector_matches_labels;

/// Check that the template labels satisfy the selector — the full selector
/// (`matchLabels` + `matchExpressions`), mirroring upstream's
/// `LabelSelectorAsSelector(selector).Matches(template.Labels)` check in
/// `ValidateDeploymentSpec` / `ValidatePodTemplateSpecForRC` / the StatefulSet
/// and DaemonSet equivalents.
fn template_labels_match_selector(
    selector: &LabelSelector,
    template_labels: &std::collections::HashMap<String, String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if !selector_matches_labels(selector, template_labels) {
        errs.push(Error::invalid(
            fld_path,
            template_labels
                .iter()
                .map(|(k2, v2)| format!("{k2}={v2}"))
                .collect::<Vec<_>>()
                .join(","),
            "`selector` does not match template `labels`",
        ));
    }
    errs
}

/// Validate a workload pod template's `restartPolicy` / `activeDeadlineSeconds`
/// constraints shared by StatefulSet, DaemonSet and ReplicaSet. Mirrors upstream
/// (e.g. `ValidatePodTemplateSpecForReplicaSet` lines 847-852,
/// `ValidateStatefulSetSpec` lines 217-222, `ValidateDaemonSetSpec` 456-461):
///
/// * `restartPolicy` must be `Always` → otherwise `NotSupported` on
///   `<path>/restartPolicy`. `None`/empty is treated as `Always` because the
///   api-server defaults it before validation runs (these workloads default to
///   `Always`).
/// * `activeDeadlineSeconds` set → `Forbidden` "activeDeadlineSeconds in <Kind>
///   is not Supported".
///
/// `fld_path` is the `template/spec` path; `kind` is the workload kind used in
/// the Forbidden message (`StatefulSet` / `DaemonSet` / `ReplicaSet`).
fn validate_workload_pod_template_restart_policy(
    pod_spec: &crate::resources::pod::PodSpec,
    fld_path: &Path,
    kind: &str,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // restartPolicy must be Always (treat unset/empty as the defaulted Always).
    match pod_spec.restart_policy.as_deref() {
        None | Some("") | Some("Always") => {}
        Some(other) => errs.push(Error::not_supported(
            &fld_path.child("restartPolicy"),
            other.to_string(),
            &["Always"],
        )),
    }

    // activeDeadlineSeconds is not supported for these workloads.
    if pod_spec.active_deadline_seconds.is_some() {
        errs.push(Error::forbidden(
            &fld_path.child("activeDeadlineSeconds"),
            format!("activeDeadlineSeconds in {kind} is not Supported"),
        ));
    }

    errs
}

/// Parse an IntOrString value (serde_json::Value that is either a number or a
/// percent string like "25%"). Returns `(is_percent, value)` or an error
/// message.
fn parse_int_or_string(v: &serde_json::Value) -> Result<(bool, i64), String> {
    match v {
        serde_json::Value::Number(n) => {
            let i = n
                .as_i64()
                .ok_or_else(|| "must be an integer or percent string".to_string())?;
            Ok((false, i))
        }
        serde_json::Value::String(s) => {
            if let Some(pct) = s.strip_suffix('%') {
                let val: i64 = pct.parse().map_err(|_| format!("invalid percent: {s}"))?;
                Ok((true, val))
            } else {
                // bare string number
                s.parse::<i64>()
                    .map(|n| (false, n))
                    .map_err(|_| format!("must be an integer or percent string, got: {s}"))
            }
        }
        _ => Err("must be an integer or percent string".to_string()),
    }
}

/// Validate that an IntOrString value is a non-negative int or a valid percent
/// string. Mirrors upstream `ValidatePositiveIntOrPercent`
/// (`pkg/apis/apps/validation/validation.go` lines 545-559): integers must be
/// `>= 0`; percent strings must parse, but their magnitude (e.g. `200%`) is
/// **not** bounded here — the upstream `IsNotMoreThan100Percent` check is
/// applied separately, and only to the fields upstream actually bounds.
///
/// Returns `(is_zero, errors)`, where `is_zero` reflects the int/percent value
/// being `0` (used for the "both maxSurge and maxUnavailable are 0" rule).
fn validate_positive_int_or_percent(v: &serde_json::Value, fld_path: &Path) -> (bool, ErrorList) {
    let mut errs: ErrorList = Vec::new();
    match parse_int_or_string(v) {
        Err(msg) => {
            errs.push(Error::invalid(fld_path, format!("{v}"), msg));
            (false, errs)
        }
        Ok((_is_pct, val)) => {
            // Only integers are bounded below; percent strings of any size are
            // accepted here (upstream defers the upper bound to
            // IsNotMoreThan100Percent, applied per-field by the caller).
            if !_is_pct && val < 0 {
                errs.push(Error::invalid(
                    fld_path,
                    val,
                    "must be greater than or equal to 0",
                ));
                return (false, errs);
            }
            (val == 0, errs)
        }
    }
}

/// Mirrors upstream `IsNotMoreThan100Percent` (lines 581-591): if the value is
/// a percent string greater than 100%, emit `Invalid(... "must not be greater
/// than 100%")`. Integers and percents `<= 100` are accepted.
fn is_not_more_than_100_percent(v: &serde_json::Value, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Ok((true, val)) = parse_int_or_string(v) {
        if val > 100 {
            errs.push(Error::invalid(
                fld_path,
                format!("{v}"),
                "must not be greater than 100%",
            ));
        }
    }
    errs
}

// ---------------------------------------------------------------------------
// Strategy validation
// ---------------------------------------------------------------------------

/// Validate `spec.strategy`. Mirrors upstream `ValidateDeploymentStrategy`.
fn validate_deployment_strategy(strategy: &DeploymentStrategy, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    match strategy.strategy_type.as_str() {
        "Recreate" => {
            // rollingUpdate is forbidden when strategy == Recreate
            if strategy.rolling_update.is_some() {
                errs.push(Error::forbidden(
                    &fld_path.child("rollingUpdate"),
                    "may not be specified when strategy `type` is `Recreate`",
                ));
            }
        }
        "RollingUpdate" => {
            errs.extend(validate_rolling_update_deployment(
                strategy.rolling_update.as_ref(),
                &fld_path.child("rollingUpdate"),
            ));
        }
        other => {
            errs.push(Error::not_supported(
                &fld_path.child("type"),
                other.to_string(),
                &["Recreate", "RollingUpdate"],
            ));
        }
    }

    errs
}

/// Validate `spec.strategy.rollingUpdate`. Mirrors upstream
/// `ValidateRollingUpdateDeployment`.
fn validate_rolling_update_deployment(
    ru: Option<&RollingUpdateDeployment>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // Default: 25% / 25% (handled upstream by defaulting, but we still
    // validate whatever is present)
    let ru = match ru {
        None => return errs, // nothing to validate if absent
        Some(r) => r,
    };

    let max_unavailable_zero;
    let max_surge_zero;

    // maxUnavailable
    match &ru.max_unavailable {
        None => {
            max_unavailable_zero = true; // treat absent as 0 for the both-zero check
        }
        Some(v) => {
            let mu_path = fld_path.child("maxUnavailable");
            let (is_zero, sub_errs) = validate_positive_int_or_percent(v, &mu_path);
            max_unavailable_zero = is_zero;
            errs.extend(sub_errs);
        }
    }

    // maxSurge — upstream `ValidatePositiveIntOrPercent` only; NO 100% bound
    // (lines 596-597), so `maxSurge: 200%` is valid for a Deployment.
    match &ru.max_surge {
        None => {
            max_surge_zero = true; // treat absent as 0 for the both-zero check
        }
        Some(v) => {
            let (is_zero, sub_errs) =
                validate_positive_int_or_percent(v, &fld_path.child("maxSurge"));
            max_surge_zero = is_zero;
            errs.extend(sub_errs);
        }
    }

    // Both cannot be zero simultaneously (upstream reports on maxUnavailable).
    if max_unavailable_zero && max_surge_zero {
        errs.push(Error::invalid(
            &fld_path.child("maxUnavailable"),
            "".to_string(),
            "may not be 0 when `maxSurge` is 0",
        ));
    }

    // Validate that maxUnavailable is not more than 100% (upstream line 603).
    // maxSurge is intentionally NOT bounded here for Deployments.
    if let Some(v) = &ru.max_unavailable {
        errs.extend(is_not_more_than_100_percent(
            v,
            &fld_path.child("maxUnavailable"),
        ));
    }

    errs
}

// ---------------------------------------------------------------------------
// Spec validation
// ---------------------------------------------------------------------------

/// Validate `spec`. Mirrors upstream `ValidateDeploymentSpec`.
fn validate_deployment_spec(
    spec: &crate::resources::deployment::DeploymentSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // replicas must be non-negative
    if let Some(r) = spec.replicas {
        if r < 0 {
            errs.push(Error::invalid(
                &fld_path.child("replicas"),
                r,
                "must be greater than or equal to 0",
            ));
        }
    }

    // selector is required and must be non-empty. Upstream (lines 642-649)
    // first validates the selector structure, then emits
    // Invalid("empty selector is invalid for deployment") when both matchLabels
    // and matchExpressions are empty. (The `Required` "nil selector" arm cannot
    // happen here — `LabelSelector` is non-optional in rusternetes.)
    if selector_is_empty(&spec.selector) {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        errs.push(Error::invalid(
            &fld_path.child("selector"),
            "{}".to_string(),
            "empty selector is invalid for deployment",
        ));
    } else {
        // validate selector structure
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));

        // template labels must match selector
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();

        errs.extend(template_labels_match_selector(
            &spec.selector,
            &template_labels,
            &fld_path.child("template").child("metadata").child("labels"),
        ));
    }

    // strategy
    if let Some(ref strategy) = spec.strategy {
        errs.extend(validate_deployment_strategy(
            strategy,
            &fld_path.child("strategy"),
        ));
    }

    // minReadySeconds must be non-negative
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    // revisionHistoryLimit must be non-negative
    if let Some(rhl) = spec.revision_history_limit {
        if rhl < 0 {
            errs.push(Error::invalid(
                &fld_path.child("revisionHistoryLimit"),
                rhl,
                "must be greater than or equal to 0",
            ));
        }
    }

    // progressDeadlineSeconds must be > minReadySeconds
    if let Some(pds) = spec.progress_deadline_seconds {
        if pds <= 0 {
            errs.push(Error::invalid(
                &fld_path.child("progressDeadlineSeconds"),
                pds,
                "must be greater than 0",
            ));
        } else {
            let min_ready = spec.min_ready_seconds.unwrap_or(0);
            if pds <= min_ready {
                errs.push(Error::invalid(
                    &fld_path.child("progressDeadlineSeconds"),
                    pds,
                    format!("must be greater than minReadySeconds ({min_ready})"),
                ));
            }
        }
    }

    errs
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validate a new `Deployment`. Mirrors upstream `ValidateDeployment`.
///
/// Returns an empty `ErrorList` if the object is valid. Each entry in a
/// non-empty list corresponds to one invalid field.
pub fn validate_deployment(d: &Deployment) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // spec is effectively required (the struct always has one, but validate it)
    errs.extend(validate_deployment_spec(&d.spec, &Path::new("spec")));

    errs
}

/// Validate a `ReplicaSetSpec`. Mirrors upstream `ValidateReplicaSetSpec`
/// (`pkg/apis/apps/validation/validation.go`): non-negative `replicas` and
/// `minReadySeconds`, a required + structurally-valid `selector`, and template
/// labels that satisfy the selector.
fn validate_replicaset_spec(spec: &ReplicaSetSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // replicas must be non-negative
    if spec.replicas < 0 {
        errs.push(Error::invalid(
            &fld_path.child("replicas"),
            spec.replicas,
            "must be greater than or equal to 0",
        ));
    }

    // minReadySeconds must be non-negative
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    // selector is required and must be non-empty + structurally valid; the
    // template's labels must satisfy it. Upstream (lines 813-820) emits
    // Invalid("empty selector is invalid for deployment") — note ReplicaSet
    // reuses the "deployment" string verbatim.
    if selector_is_empty(&spec.selector) {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        errs.push(Error::invalid(
            &fld_path.child("selector"),
            "{}".to_string(),
            "empty selector is invalid for deployment",
        ));
    } else {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        errs.extend(template_labels_match_selector(
            &spec.selector,
            &template_labels,
            &fld_path.child("template").child("metadata").child("labels"),
        ));
    }

    // template.spec.restartPolicy must be Always; activeDeadlineSeconds
    // forbidden (upstream `ValidatePodTemplateSpecForReplicaSet`, lines 847-852).
    errs.extend(validate_workload_pod_template_restart_policy(
        &spec.template.spec,
        &fld_path.child("template").child("spec"),
        "ReplicaSet",
    ));

    errs
}

/// Validate a new `ReplicaSet`. Mirrors upstream `ValidateReplicaSet`.
pub fn validate_replicaset(rs: &ReplicaSet) -> ErrorList {
    validate_replicaset_spec(&rs.spec, &Path::new("spec"))
}

/// Validate a `StatefulSetSpec`. Mirrors upstream `ValidateStatefulSetSpec`
/// (`pkg/apis/apps/validation/validation.go`). Intended to run *after*
/// defaulting (the api-server defaults `podManagementPolicy` and
/// `updateStrategy`), so the "required when empty" arms match upstream without
/// rejecting objects that merely relied on defaulting.
fn validate_statefulset_spec(spec: &StatefulSetSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // podManagementPolicy: OrderedReady | Parallel.
    match spec.pod_management_policy.as_deref() {
        None | Some("") => errs.push(Error::required(&fld_path.child("podManagementPolicy"), "")),
        Some("OrderedReady") | Some("Parallel") => {}
        Some(other) => errs.push(Error::invalid(
            &fld_path.child("podManagementPolicy"),
            other.to_string(),
            "must be 'OrderedReady' or 'Parallel'",
        )),
    }

    // updateStrategy: RollingUpdate | OnDelete.
    let us_path = fld_path.child("updateStrategy");
    match &spec.update_strategy {
        None => errs.push(Error::required(&us_path, "")),
        Some(us) => match us.strategy_type.as_deref() {
            None | Some("") => errs.push(Error::required(&us_path, "")),
            Some("OnDelete") => {
                if us.rolling_update.is_some() {
                    errs.push(Error::invalid(
                        &us_path.child("rollingUpdate"),
                        "<set>".to_string(),
                        "only allowed for updateStrategy 'RollingUpdate'",
                    ));
                }
            }
            Some("RollingUpdate") => {
                if let Some(ru) = &us.rolling_update {
                    if let Some(p) = ru.partition {
                        if p < 0 {
                            errs.push(Error::invalid(
                                &us_path.child("rollingUpdate").child("partition"),
                                p,
                                "must be greater than or equal to 0",
                            ));
                        }
                    }
                }
            }
            Some(other) => errs.push(Error::invalid(
                &us_path,
                other.to_string(),
                "must be 'RollingUpdate' or 'OnDelete'",
            )),
        },
    }

    // replicas >= 0
    if let Some(r) = spec.replicas {
        if r < 0 {
            errs.push(Error::invalid(
                &fld_path.child("replicas"),
                r,
                "must be greater than or equal to 0",
            ));
        }
    }

    // minReadySeconds >= 0
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    // ordinals.start >= 0
    if let Some(ords) = &spec.ordinals {
        if let Some(start) = ords.start {
            if start < 0 {
                errs.push(Error::invalid(
                    &fld_path.child("ordinals.start"),
                    start,
                    "must be greater than or equal to 0",
                ));
            }
        }
    }

    // serviceName, when set, must be a DNS-1123 label.
    if !spec.service_name.is_empty() {
        for msg in is_dns1123_label(&spec.service_name) {
            errs.push(Error::invalid(
                &fld_path.child("serviceName"),
                spec.service_name.clone(),
                msg,
            ));
        }
    }

    // selector is required and must be non-empty + valid; template labels must
    // satisfy it. Upstream (lines 179-187) emits
    // Invalid("empty selector is invalid for statefulset") when present-but-empty.
    if selector_is_empty(&spec.selector) {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        errs.push(Error::invalid(
            &fld_path.child("selector"),
            "{}".to_string(),
            "empty selector is invalid for statefulset",
        ));
    } else {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        errs.extend(template_labels_match_selector(
            &spec.selector,
            &template_labels,
            &fld_path.child("template").child("metadata").child("labels"),
        ));
    }

    // template.spec.restartPolicy must be Always; activeDeadlineSeconds
    // forbidden (upstream lines 217-222).
    errs.extend(validate_workload_pod_template_restart_policy(
        &spec.template.spec,
        &fld_path.child("template").child("spec"),
        "StatefulSet",
    ));

    errs
}

/// Validate a new `StatefulSet`. Mirrors upstream `ValidateStatefulSet`.
/// Run after defaulting (see [`validate_statefulset_spec`]).
pub fn validate_statefulset(ss: &StatefulSet) -> ErrorList {
    validate_statefulset_spec(&ss.spec, &Path::new("spec"))
}

/// Validate a `DaemonSetSpec`. Mirrors upstream `ValidateDaemonSetSpec`
/// (`pkg/apis/apps/validation/validation.go`): a required + valid `selector`
/// whose template labels match, non-negative `minReadySeconds` /
/// `revisionHistoryLimit`, and an `updateStrategy` of `RollingUpdate`
/// (`rollingUpdate` required, with in-range `maxUnavailable`/`maxSurge`) or
/// `OnDelete`. Run after defaulting (the api-server defaults `updateStrategy`).
fn validate_daemonset_spec(spec: &DaemonSetSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // selector required + valid + non-empty; template labels must match.
    // Upstream (lines 450-452) emits Invalid("empty selector is invalid for
    // daemonset") when present-but-empty.
    if selector_is_empty(&spec.selector) {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        errs.push(Error::invalid(
            &fld_path.child("selector"),
            "{}".to_string(),
            "empty selector is invalid for daemonset",
        ));
    } else {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        errs.extend(template_labels_match_selector(
            &spec.selector,
            &template_labels,
            &fld_path.child("template").child("metadata").child("labels"),
        ));
    }

    // template.spec.restartPolicy must be Always; activeDeadlineSeconds
    // forbidden (upstream lines 456-461).
    errs.extend(validate_workload_pod_template_restart_policy(
        &spec.template.spec,
        &fld_path.child("template").child("spec"),
        "DaemonSet",
    ));

    // minReadySeconds >= 0
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    // revisionHistoryLimit >= 0
    if let Some(rhl) = spec.revision_history_limit {
        if rhl < 0 {
            errs.push(Error::invalid(
                &fld_path.child("revisionHistoryLimit"),
                rhl,
                "must be greater than or equal to 0",
            ));
        }
    }

    // updateStrategy: RollingUpdate | OnDelete.
    let us_path = fld_path.child("updateStrategy");
    match spec
        .update_strategy
        .as_ref()
        .and_then(|u| u.strategy_type.as_deref())
    {
        Some("OnDelete") => {}
        Some("RollingUpdate") => {
            // rollingUpdate must be present; validate its int-or-percent fields.
            match spec
                .update_strategy
                .as_ref()
                .and_then(|u| u.rolling_update.as_ref())
            {
                None => errs.push(Error::required(&us_path.child("rollingUpdate"), "")),
                Some(ru) => {
                    // Mirrors upstream `ValidateRollingUpdateDaemonSet`
                    // (lines 474-496): both fields are positive-int-or-percent,
                    // both are bounded to 100%, and exactly one of
                    // maxUnavailable/maxSurge must be non-zero. Absent fields
                    // count as 0 for the mutual-exclusion switch.
                    let ru_path = us_path.child("rollingUpdate");

                    let mut max_unavailable_zero = true;
                    if let Some(mu) = &ru.max_unavailable {
                        let v = serde_json::Value::String(mu.clone());
                        let (is_zero, sub) =
                            validate_positive_int_or_percent(&v, &ru_path.child("maxUnavailable"));
                        max_unavailable_zero = is_zero;
                        errs.extend(sub);
                        errs.extend(is_not_more_than_100_percent(
                            &v,
                            &ru_path.child("maxUnavailable"),
                        ));
                    }

                    let mut max_surge_zero = true;
                    if let Some(ms) = &ru.max_surge {
                        let v = serde_json::Value::String(ms.clone());
                        let (is_zero, sub) =
                            validate_positive_int_or_percent(&v, &ru_path.child("maxSurge"));
                        max_surge_zero = is_zero;
                        errs.extend(sub);
                        errs.extend(is_not_more_than_100_percent(&v, &ru_path.child("maxSurge")));
                    }

                    // Exactly one of maxSurge / maxUnavailable must be non-zero.
                    if !max_unavailable_zero && !max_surge_zero {
                        errs.push(Error::invalid(
                            &ru_path.child("maxSurge"),
                            ru.max_surge.clone().unwrap_or_default(),
                            "may not be set when maxUnavailable is non-zero",
                        ));
                    } else if max_unavailable_zero && max_surge_zero {
                        errs.push(Error::required(
                            &ru_path.child("maxUnavailable"),
                            "cannot be 0 when maxSurge is 0",
                        ));
                    }
                }
            }
        }
        // Unset/empty/unknown — upstream's default arm is NotSupported.
        other => errs.push(Error::not_supported(
            &us_path,
            other.unwrap_or("").to_string(),
            &["RollingUpdate", "OnDelete"],
        )),
    }

    errs
}

/// Validate a new `DaemonSet`. Mirrors upstream `ValidateDaemonSet`.
/// Run after defaulting (see [`validate_daemonset_spec`]).
pub fn validate_daemonset(ds: &DaemonSet) -> ErrorList {
    validate_daemonset_spec(&ds.spec, &Path::new("spec"))
}

/// Validate a `Deployment` update (`new` replaces `old`). Mirrors upstream
/// `ValidateDeploymentUpdate`.
///
/// Checks:
/// 1. selector is immutable (already enforced by the handler but re-checked
///    here for completeness / testing).
/// 2. All create-side constraints on the new object.
pub fn validate_deployment_update(new: &Deployment, old: &Deployment) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // selector is immutable
    if new.spec.selector != old.spec.selector {
        errs.push(Error::forbidden(
            &Path::new("spec").child("selector"),
            "field is immutable",
        ));
    }

    // Full spec validation on the new object
    errs.extend(validate_deployment_spec(&new.spec, &Path::new("spec")));

    errs
}

/// Validate a `StatefulSet` update — the immutability rule from upstream
/// `ValidateStatefulSetUpdate`. All spec fields except `replicas`, `ordinals`,
/// `template`, `updateStrategy`, `revisionHistoryLimit`,
/// `persistentVolumeClaimRetentionPolicy` and `minReadySeconds` are immutable.
///
/// Here we check the immutable fields explicitly (`serviceName`,
/// `podManagementPolicy`, `volumeClaimTemplates`) rather than a whole-spec
/// deep-equal, so legitimate mutations to the allowed fields never false-trip.
/// `selector` immutability is enforced by the handler's
/// `validate_selector_immutable` call.
pub fn validate_statefulset_update(new: &StatefulSet, old: &StatefulSet) -> ErrorList {
    // Upstream `ValidateStatefulSetUpdate` first re-validates the whole new spec
    // via `ValidateStatefulSetSpec` (it deliberately skips `ValidateStatefulSet`
    // only to avoid revalidating the immutable name). `validate_statefulset` here
    // already validates the spec alone (no name check), so we reuse it to catch
    // updates that would otherwise introduce an invalid selector/template/replicas.
    let mut errs: ErrorList = validate_statefulset(new);
    let immutable_changed = new.spec.service_name != old.spec.service_name
        || new.spec.pod_management_policy != old.spec.pod_management_policy
        || serde_json::to_value(&new.spec.volume_claim_templates).ok()
            != serde_json::to_value(&old.spec.volume_claim_templates).ok();
    if immutable_changed {
        errs.push(Error::forbidden(
            &Path::new("spec"),
            "updates to statefulset spec for fields other than 'replicas', 'ordinals', 'template', 'updateStrategy', 'revisionHistoryLimit', 'persistentVolumeClaimRetentionPolicy' and 'minReadySeconds' are forbidden",
        ));
    }
    errs
}

#[cfg(test)]
mod selector_match_tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn sel(json: serde_json::Value) -> LabelSelector {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn match_labels_still_enforced() {
        let s = sel(serde_json::json!({"matchLabels": {"app": "x"}}));
        assert!(selector_matches_labels(&s, &labels(&[("app", "x")])));
        assert!(!selector_matches_labels(&s, &labels(&[("app", "y")])));
        assert!(!selector_matches_labels(&s, &labels(&[("other", "x")])));
    }

    #[test]
    fn match_expressions_in_notin() {
        let in_sel = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "In", "values": ["fe", "be"]}
        ]}));
        assert!(selector_matches_labels(&in_sel, &labels(&[("tier", "fe")])));
        assert!(!selector_matches_labels(
            &in_sel,
            &labels(&[("tier", "db")])
        ));
        assert!(!selector_matches_labels(&in_sel, &labels(&[("x", "y")]))); // key absent

        let notin = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "NotIn", "values": ["db"]}
        ]}));
        assert!(selector_matches_labels(&notin, &labels(&[("tier", "fe")])));
        assert!(selector_matches_labels(&notin, &labels(&[("x", "y")]))); // absent => NotIn matches
        assert!(!selector_matches_labels(&notin, &labels(&[("tier", "db")])));
    }

    #[test]
    fn match_expressions_exists_doesnotexist() {
        let exists = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "Exists"}
        ]}));
        assert!(selector_matches_labels(
            &exists,
            &labels(&[("tier", "anything")])
        ));
        assert!(!selector_matches_labels(&exists, &labels(&[("x", "y")])));

        let dne = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "DoesNotExist"}
        ]}));
        assert!(selector_matches_labels(&dne, &labels(&[("x", "y")])));
        assert!(!selector_matches_labels(&dne, &labels(&[("tier", "fe")])));
    }

    #[test]
    fn template_mismatch_on_expressions_reports_error() {
        // selector requires tier In [fe]; template labels have tier=db -> mismatch
        let s = sel(serde_json::json!({"matchExpressions": [
            {"key": "tier", "operator": "In", "values": ["fe"]}
        ]}));
        let errs = template_labels_match_selector(
            &s,
            &labels(&[("tier", "db")]),
            &Path::new("spec").child("template"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("does not match template")),
            "{errs:?}"
        );
        // matching template -> no error
        assert!(
            template_labels_match_selector(&s, &labels(&[("tier", "fe")]), &Path::new("spec"))
                .is_empty()
        );
    }
}

#[cfg(test)]
mod workload_parity_tests {
    use super::*;

    fn agg(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(" | ")
    }

    fn deployment(json: serde_json::Value) -> Deployment {
        serde_json::from_value(json).unwrap()
    }
    fn statefulset(json: serde_json::Value) -> StatefulSet {
        serde_json::from_value(json).unwrap()
    }
    fn daemonset(json: serde_json::Value) -> DaemonSet {
        serde_json::from_value(json).unwrap()
    }
    fn replicaset(json: serde_json::Value) -> ReplicaSet {
        serde_json::from_value(json).unwrap()
    }

    /// A pod template whose labels match `{app: x}` and (optionally) carries an
    /// explicit restartPolicy / activeDeadlineSeconds.
    fn template(restart_policy: Option<&str>, ads: Option<i64>) -> serde_json::Value {
        let mut spec = serde_json::json!({
            "containers": [{"name": "c", "image": "nginx"}]
        });
        if let Some(rp) = restart_policy {
            spec["restartPolicy"] = serde_json::json!(rp);
        }
        if let Some(a) = ads {
            spec["activeDeadlineSeconds"] = serde_json::json!(a);
        }
        serde_json::json!({
            "metadata": {"labels": {"app": "x"}},
            "spec": spec,
        })
    }

    fn matching_selector() -> serde_json::Value {
        serde_json::json!({"matchLabels": {"app": "x"}})
    }

    // --- maxSurge > 100% is valid for a Deployment (upstream line 603 only
    //     bounds maxUnavailable) ----------------------------------------------

    #[test]
    fn deployment_max_surge_over_100_percent_is_valid() {
        let d = deployment(serde_json::json!({
            "metadata": {"name": "d"},
            "spec": {
                "replicas": 3,
                "selector": matching_selector(),
                "strategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": "25%", "maxSurge": "200%"}
                },
                "template": template(Some("Always"), None),
            }
        }));
        let errs = validate_deployment(&d);
        assert!(
            errs.is_empty(),
            "maxSurge: 200% must be accepted for a Deployment, got: {}",
            agg(&errs)
        );
    }

    #[test]
    fn deployment_max_unavailable_over_100_percent_rejected() {
        let d = deployment(serde_json::json!({
            "metadata": {"name": "d"},
            "spec": {
                "replicas": 3,
                "selector": matching_selector(),
                "strategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": "200%", "maxSurge": "25%"}
                },
                "template": template(Some("Always"), None),
            }
        }));
        let errs = validate_deployment(&d);
        let a = agg(&errs);
        assert!(
            a.contains("maxUnavailable") && a.contains("100%"),
            "maxUnavailable: 200% must be rejected, got: {a}"
        );
    }

    // --- Deployment empty-selector wording -----------------------------------

    #[test]
    fn deployment_empty_selector_wording() {
        let d = deployment(serde_json::json!({
            "metadata": {"name": "d"},
            "spec": {
                "selector": {},
                "template": template(Some("Always"), None),
            }
        }));
        let errs = validate_deployment(&d);
        assert!(
            agg(&errs).contains("empty selector is invalid for deployment"),
            "got: {}",
            agg(&errs)
        );
    }

    // --- StatefulSet restartPolicy / activeDeadlineSeconds -------------------

    fn base_statefulset(template: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "s"},
            "spec": {
                "serviceName": "svc",
                "selector": matching_selector(),
                "podManagementPolicy": "OrderedReady",
                "updateStrategy": {"type": "RollingUpdate"},
                "template": template,
            }
        })
    }

    #[test]
    fn statefulset_restart_policy_always_ok() {
        let s = statefulset(base_statefulset(template(Some("Always"), None)));
        assert!(
            validate_statefulset(&s).is_empty(),
            "got: {}",
            agg(&validate_statefulset(&s))
        );
        // unset restartPolicy also OK (defaulted to Always)
        let s2 = statefulset(base_statefulset(template(None, None)));
        assert!(validate_statefulset(&s2).is_empty());
    }

    #[test]
    fn statefulset_restart_policy_never_rejected() {
        let s = statefulset(base_statefulset(template(Some("Never"), None)));
        let a = agg(&validate_statefulset(&s));
        assert!(
            a.contains("template.spec.restartPolicy") && a.contains("supported values"),
            "got: {a}"
        );
    }

    #[test]
    fn statefulset_active_deadline_seconds_forbidden() {
        let s = statefulset(base_statefulset(template(Some("Always"), Some(30))));
        let a = agg(&validate_statefulset(&s));
        assert!(
            a.contains("template.spec.activeDeadlineSeconds")
                && a.contains("activeDeadlineSeconds in StatefulSet is not Supported"),
            "got: {a}"
        );
    }

    #[test]
    fn statefulset_empty_selector_wording() {
        let s = statefulset(serde_json::json!({
            "metadata": {"name": "s"},
            "spec": {
                "serviceName": "svc",
                "selector": {},
                "podManagementPolicy": "OrderedReady",
                "updateStrategy": {"type": "RollingUpdate"},
                "template": template(Some("Always"), None),
            }
        }));
        assert!(
            agg(&validate_statefulset(&s)).contains("empty selector is invalid for statefulset"),
            "got: {}",
            agg(&validate_statefulset(&s))
        );
    }

    // --- ReplicaSet restartPolicy / activeDeadlineSeconds --------------------

    fn base_replicaset(template: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "r"},
            "spec": {"replicas": 1, "selector": matching_selector(), "template": template}
        })
    }

    #[test]
    fn replicaset_restart_policy_always_ok() {
        let r = replicaset(base_replicaset(template(Some("Always"), None)));
        assert!(
            validate_replicaset(&r).is_empty(),
            "got: {}",
            agg(&validate_replicaset(&r))
        );
    }

    #[test]
    fn replicaset_restart_policy_onfailure_rejected() {
        let r = replicaset(base_replicaset(template(Some("OnFailure"), None)));
        let a = agg(&validate_replicaset(&r));
        assert!(
            a.contains("template.spec.restartPolicy") && a.contains("supported values"),
            "got: {a}"
        );
    }

    #[test]
    fn replicaset_active_deadline_seconds_forbidden() {
        let r = replicaset(base_replicaset(template(Some("Always"), Some(10))));
        let a = agg(&validate_replicaset(&r));
        assert!(
            a.contains("activeDeadlineSeconds in ReplicaSet is not Supported"),
            "got: {a}"
        );
    }

    #[test]
    fn replicaset_empty_selector_wording() {
        let r = replicaset(serde_json::json!({
            "metadata": {"name": "r"},
            "spec": {"replicas": 1, "selector": {}, "template": template(Some("Always"), None)}
        }));
        assert!(
            agg(&validate_replicaset(&r)).contains("empty selector is invalid for deployment"),
            "got: {}",
            agg(&validate_replicaset(&r))
        );
    }

    // --- DaemonSet restartPolicy / ADS / rollingUpdate mutual-exclusion ------

    fn daemonset_with(
        template: serde_json::Value,
        rolling_update: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "ds"},
            "spec": {
                "selector": matching_selector(),
                "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": rolling_update},
                "template": template,
            }
        })
    }

    #[test]
    fn daemonset_restart_policy_never_rejected_ads_forbidden() {
        let ds = daemonset(daemonset_with(
            template(Some("Never"), Some(5)),
            serde_json::json!({"maxUnavailable": "1", "maxSurge": "0"}),
        ));
        let a = agg(&validate_daemonset(&ds));
        assert!(a.contains("template.spec.restartPolicy"), "got: {a}");
        assert!(
            a.contains("activeDeadlineSeconds in DaemonSet is not Supported"),
            "got: {a}"
        );
    }

    #[test]
    fn daemonset_rolling_update_both_nonzero_rejected() {
        let ds = daemonset(daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": "1", "maxSurge": "1"}),
        ));
        let a = agg(&validate_daemonset(&ds));
        assert!(
            a.contains("maxSurge") && a.contains("may not be set when maxUnavailable is non-zero"),
            "got: {a}"
        );
    }

    #[test]
    fn daemonset_rolling_update_both_zero_rejected() {
        let ds = daemonset(daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": "0", "maxSurge": "0"}),
        ));
        let a = agg(&validate_daemonset(&ds));
        assert!(
            a.contains("maxUnavailable") && a.contains("cannot be 0 when maxSurge is 0"),
            "got: {a}"
        );
    }

    #[test]
    fn daemonset_rolling_update_surge_only_ok() {
        let ds = daemonset(daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": "0", "maxSurge": "1"}),
        ));
        assert!(
            validate_daemonset(&ds).is_empty(),
            "got: {}",
            agg(&validate_daemonset(&ds))
        );
    }

    #[test]
    fn daemonset_rolling_update_surge_over_100_percent_rejected() {
        // unlike Deployment, DaemonSet bounds BOTH fields to 100% (upstream 483)
        let ds = daemonset(daemonset_with(
            template(Some("Always"), None),
            serde_json::json!({"maxUnavailable": "0", "maxSurge": "200%"}),
        ));
        let a = agg(&validate_daemonset(&ds));
        assert!(
            a.contains("maxSurge") && a.contains("must not be greater than 100%"),
            "got: {a}"
        );
    }

    #[test]
    fn daemonset_empty_selector_wording() {
        let ds = daemonset(serde_json::json!({
            "metadata": {"name": "ds"},
            "spec": {
                "selector": {},
                "updateStrategy": {"type": "OnDelete"},
                "template": template(Some("Always"), None),
            }
        }));
        assert!(
            agg(&validate_daemonset(&ds)).contains("empty selector is invalid for daemonset"),
            "got: {}",
            agg(&validate_daemonset(&ds))
        );
    }
}
