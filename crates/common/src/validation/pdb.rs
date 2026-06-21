//! PodDisruptionBudget validation — port of upstream Kubernetes
//! `pkg/apis/policy/validation/validation.go::ValidatePodDisruptionBudgetSpec`
//! (release-1.35).

use crate::resources::policy::{IntOrString, PodDisruptionBudget, PodDisruptionBudgetSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{validate_label_selector, LabelSelectorValidationOptions};

/// Parse a percent string ("`N%`") to its integer value, or `None` if it is not
/// a valid percent (upstream `IsValidPercent`: `^[0-9]+%$`).
fn parse_percent(s: &str) -> Option<i64> {
    let digits = s.strip_suffix('%')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<i64>().ok()
}

/// Validate an `IntOrString` used as a disruption budget bound, combining
/// upstream `ValidatePositiveIntOrPercent` (non-negative int, or a valid
/// percent) and `IsNotMoreThan100Percent` (a percent may not exceed 100%).
fn validate_int_or_percent(v: &IntOrString, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match v {
        IntOrString::Int(n) => {
            if *n < 0 {
                errs.push(Error::invalid(
                    fld_path,
                    *n,
                    "must be greater than or equal to 0",
                ));
            }
        }
        IntOrString::String(s) => match parse_percent(s) {
            None => errs.push(Error::invalid(
                fld_path,
                s.clone(),
                "must be an integer or percentage (e.g '5%')",
            )),
            Some(pct) if pct > 100 => errs.push(Error::invalid(
                fld_path,
                s.clone(),
                "must not be greater than 100%",
            )),
            Some(_) => {}
        },
    }
    errs
}

/// Validate a `PodDisruptionBudgetSpec`. Mirrors upstream
/// `ValidatePodDisruptionBudgetSpec`.
pub fn validate_pod_disruption_budget_spec(
    spec: &PodDisruptionBudgetSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // minAvailable and maxUnavailable are mutually exclusive.
    if spec.min_available.is_some() && spec.max_unavailable.is_some() {
        errs.push(Error::invalid(
            fld_path,
            "{minAvailable, maxUnavailable}".to_string(),
            "minAvailable and maxUnavailable cannot be both set",
        ));
    }

    if let Some(mn) = &spec.min_available {
        errs.extend(validate_int_or_percent(mn, &fld_path.child("minAvailable")));
    }
    if let Some(mx) = &spec.max_unavailable {
        errs.extend(validate_int_or_percent(
            mx,
            &fld_path.child("maxUnavailable"),
        ));
    }

    errs.extend(validate_label_selector(
        &spec.selector,
        LabelSelectorValidationOptions::default(),
        &fld_path.child("selector"),
    ));

    // unhealthyPodEvictionPolicy, when set, must be a known value.
    if let Some(policy) = &spec.unhealthy_pod_eviction_policy {
        if policy != "IfHealthyBudget" && policy != "AlwaysAllow" {
            errs.push(Error::not_supported(
                &fld_path.child("unhealthyPodEvictionPolicy"),
                policy.clone(),
                &["AlwaysAllow", "IfHealthyBudget"],
            ));
        }
    }

    errs
}

/// Validate a new `PodDisruptionBudget`. Mirrors upstream
/// `ValidatePodDisruptionBudget`.
pub fn validate_pod_disruption_budget(pdb: &PodDisruptionBudget) -> ErrorList {
    validate_pod_disruption_budget_spec(&pdb.spec, &Path::new("spec"))
}
