//! PodDisruptionBudget validation — port of upstream Kubernetes
//! `pkg/apis/policy/validation/validation.go::ValidatePodDisruptionBudgetSpec`
//! (release-1.35).

use crate::resources::policy::{
    IntOrString, PodDisruptionBudget, PodDisruptionBudgetCondition, PodDisruptionBudgetSpec,
    PodDisruptionBudgetStatus,
};
use crate::types::Condition;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    validate_conditions, validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::validate_nonnegative_field;

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

/// Convert a PDB-specific condition into the generic `metav1.Condition`
/// understood by [`validate_conditions`]. Upstream stores PDB conditions as
/// `[]metav1.Condition` directly; our resource type carries a dedicated struct,
/// so we map field-for-field. Upstream treats a zero `metav1.Time` as
/// "missing"; an empty or unparseable `lastTransitionTime` string maps to
/// `None` so `ValidateCondition`'s required-time check fires identically.
fn to_metav1_condition(c: &PodDisruptionBudgetCondition) -> Condition {
    let last_transition_time = c
        .last_transition_time
        .as_deref()
        .filter(|s| !s.is_empty())
        .and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        });
    Condition {
        condition_type: c.condition_type.clone(),
        status: c.status.clone(),
        observed_generation: c.observed_generation,
        last_transition_time,
        reason: c.reason.clone(),
        message: c.message.clone(),
    }
}

/// Validate a `PodDisruptionBudgetStatus` on a status-subresource update.
/// Mirrors upstream `ValidatePodDisruptionBudgetStatusUpdate`
/// (`pkg/apis/policy/validation/validation.go`): validate the condition list,
/// then require the disruption counters to be non-negative.
///
/// Upstream takes `oldStatus` and `apiVersion` parameters. The `oldStatus` is
/// unused in the upstream body (no transition checks), and the `apiVersion`
/// guard only short-circuits the non-negative checks for the legacy
/// `policy/v1beta1` group — which rusternetes does not serve (target is
/// `policy/v1`, k8s v1.35), so the non-negative checks always run here.
pub fn validate_pod_disruption_budget_status(
    status: &PodDisruptionBudgetStatus,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(conditions) = &status.conditions {
        let metav1_conditions: Vec<Condition> =
            conditions.iter().map(to_metav1_condition).collect();
        errs.extend(validate_conditions(
            &metav1_conditions,
            &fld_path.child("conditions"),
        ));
    }

    errs.extend(validate_nonnegative_field(
        i64::from(status.disruptions_allowed),
        &fld_path.child("disruptionsAllowed"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(status.current_healthy),
        &fld_path.child("currentHealthy"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(status.desired_healthy),
        &fld_path.child("desiredHealthy"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(status.expected_pods),
        &fld_path.child("expectedPods"),
    ));

    errs
}

/// Validate a `PodDisruptionBudget` status-subresource update. Mirrors the
/// `status` path of upstream `ValidatePodDisruptionBudgetStatusUpdate`, rooted
/// at `status`. `_old` is accepted for upstream-signature parity (the upstream
/// body performs no old-vs-new transition checks).
pub fn validate_pod_disruption_budget_status_update(
    pdb: &PodDisruptionBudget,
    _old: &PodDisruptionBudget,
) -> ErrorList {
    match &pdb.status {
        Some(status) => validate_pod_disruption_budget_status(status, &Path::new("status")),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::policy::PodDisruptionBudgetSpec;
    use crate::types::LabelSelector;

    fn empty_status() -> PodDisruptionBudgetStatus {
        PodDisruptionBudgetStatus {
            current_healthy: 0,
            desired_healthy: 0,
            disruptions_allowed: 0,
            expected_pods: 0,
            observed_generation: None,
            conditions: None,
            disrupted_pods: None,
        }
    }

    fn pdb_with_status(status: PodDisruptionBudgetStatus) -> PodDisruptionBudget {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: None,
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };
        let mut pdb = PodDisruptionBudget::new("pdb", "default", spec);
        pdb.status = Some(status);
        pdb
    }

    #[test]
    fn status_update_accepts_valid_status() {
        let status = empty_status();
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn status_update_no_status_is_ok() {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: None,
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };
        let pdb = PodDisruptionBudget::new("pdb", "default", spec);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty());
    }

    #[test]
    fn status_update_rejects_negative_disruptions_allowed() {
        let mut status = empty_status();
        status.disruptions_allowed = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.disruptionsAllowed");
    }

    #[test]
    fn status_update_rejects_negative_current_healthy() {
        let mut status = empty_status();
        status.current_healthy = -5;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.currentHealthy");
    }

    #[test]
    fn status_update_rejects_negative_desired_healthy() {
        let mut status = empty_status();
        status.desired_healthy = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.desiredHealthy");
    }

    #[test]
    fn status_update_rejects_negative_expected_pods() {
        let mut status = empty_status();
        status.expected_pods = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.expectedPods");
    }

    #[test]
    fn status_update_accepts_valid_condition() {
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: Some("ok".to_string()),
            observed_generation: Some(1),
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn status_update_rejects_bad_condition_status() {
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "Maybe".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter()
                .any(|e| e.field == "status.conditions[0].status"),
            "expected condition status error, got {errs:?}"
        );
    }

    #[test]
    fn status_update_rejects_condition_missing_transition_time() {
        // Empty lastTransitionTime maps to None → required-time check fires.
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some(String::new()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter()
                .any(|e| e.field == "status.conditions[0].lastTransitionTime"),
            "expected lastTransitionTime required error, got {errs:?}"
        );
    }

    #[test]
    fn status_update_rejects_duplicate_condition_types() {
        let mut status = empty_status();
        let cond = PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        };
        status.conditions = Some(vec![cond.clone(), cond]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter().any(|e| e.field == "status.conditions[1].type"),
            "expected duplicate-type error, got {errs:?}"
        );
    }
}
