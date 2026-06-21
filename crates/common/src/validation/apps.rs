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

/// Check that template labels contain every key/value from the selector's
/// `matchLabels`. This is the "template labels must match selector" check.
/// Upstream in `ValidatePodTemplateSpecForRC` / `ValidateDeploymentSpec`.
fn template_labels_match_selector(
    selector: &LabelSelector,
    template_labels: &std::collections::HashMap<String, String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(match_labels) = &selector.match_labels {
        for (k, v) in match_labels {
            match template_labels.get(k) {
                None => {
                    errs.push(Error::invalid(
                        fld_path,
                        template_labels
                            .iter()
                            .map(|(k2, v2)| format!("{k2}={v2}"))
                            .collect::<Vec<_>>()
                            .join(","),
                        "`selector` does not match template `labels`",
                    ));
                    return errs; // one error is enough to convey the mismatch
                }
                Some(tv) if tv != v => {
                    errs.push(Error::invalid(
                        fld_path,
                        template_labels
                            .iter()
                            .map(|(k2, v2)| format!("{k2}={v2}"))
                            .collect::<Vec<_>>()
                            .join(","),
                        "`selector` does not match template `labels`",
                    ));
                    return errs;
                }
                _ => {}
            }
        }
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

/// Validate a single IntOrString field (maxSurge or maxUnavailable).
/// Returns `(is_zero, errors)`.
fn validate_int_or_string_field(v: &serde_json::Value, fld_path: &Path) -> (bool, ErrorList) {
    let mut errs: ErrorList = Vec::new();
    match parse_int_or_string(v) {
        Err(msg) => {
            errs.push(Error::invalid(fld_path, format!("{v}"), msg));
            (false, errs)
        }
        Ok((is_pct, val)) => {
            if is_pct {
                if !(0..=100).contains(&val) {
                    errs.push(Error::invalid(
                        fld_path,
                        format!("{v}"),
                        "must be between 0% and 100%",
                    ));
                    return (false, errs);
                }
            } else if val < 0 {
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
            let (is_zero, sub_errs) =
                validate_int_or_string_field(v, &fld_path.child("maxUnavailable"));
            max_unavailable_zero = is_zero;
            errs.extend(sub_errs);
        }
    }

    // maxSurge
    match &ru.max_surge {
        None => {
            max_surge_zero = true; // treat absent as 0 for the both-zero check
        }
        Some(v) => {
            let (is_zero, sub_errs) = validate_int_or_string_field(v, &fld_path.child("maxSurge"));
            max_surge_zero = is_zero;
            errs.extend(sub_errs);
        }
    }

    // Both cannot be zero simultaneously
    if max_unavailable_zero && max_surge_zero {
        errs.push(Error::invalid(
            fld_path,
            "".to_string(),
            "may not be 0 when `maxSurge` is 0",
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

    // selector is required and must be non-empty
    if selector_is_empty(&spec.selector) {
        errs.push(Error::required(
            &fld_path.child("selector"),
            "must be specified",
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
    // template's labels must satisfy it.
    if selector_is_empty(&spec.selector) {
        errs.push(Error::required(
            &fld_path.child("selector"),
            "must be specified",
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
    // satisfy it.
    if selector_is_empty(&spec.selector) {
        errs.push(Error::required(
            &fld_path.child("selector"),
            "must be specified",
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
    if selector_is_empty(&spec.selector) {
        errs.push(Error::required(
            &fld_path.child("selector"),
            "must be specified",
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
                    let ru_path = us_path.child("rollingUpdate");
                    if let Some(mu) = &ru.max_unavailable {
                        let (_, sub) = validate_int_or_string_field(
                            &serde_json::Value::String(mu.clone()),
                            &ru_path.child("maxUnavailable"),
                        );
                        errs.extend(sub);
                    }
                    if let Some(ms) = &ru.max_surge {
                        let (_, sub) = validate_int_or_string_field(
                            &serde_json::Value::String(ms.clone()),
                            &ru_path.child("maxSurge"),
                        );
                        errs.extend(sub);
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
    let mut errs: ErrorList = Vec::new();
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
