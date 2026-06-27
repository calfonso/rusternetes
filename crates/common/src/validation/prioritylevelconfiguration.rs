//! PriorityLevelConfiguration (APF) validation — port of upstream Kubernetes
//! `pkg/apis/flowcontrol/validation/validation.go` (release-1.35).
//!
//! Covers the `type` ↔ name(`exempt`) coupling, the exempt/limited field
//! coupling, `limited` (nominalConcurrencyShares / borrowingLimitPercent /
//! limitResponse + queuing), `exempt` numeric bounds, and `status.conditions`.
//! ObjectMeta is validated separately (#1087 / #1277).
//!
//! Note: the rusternetes type carries `lendable_percent` (upstream
//! `lendablePercent`), validated 0–100 in `validate_limited` / `validate_exempt`
//! (upstream lines 432-434 / 447-449). The legacy `lending_concurrency_limit`
//! field is retained for wire/proto compatibility but is not range-checked. The
//! shuffle-sharding entropy-bits check on `handSize`/`queues` is also omitted
//! (exotic); the positivity, max-queues, and `handSize <= queues` checks are
//! ported.

use std::collections::HashSet;

use crate::resources::flowcontrol::{
    ExemptPriorityLevelConfiguration, LimitResponse, LimitResponseType,
    LimitedPriorityLevelConfiguration, PriorityLevelConfiguration,
    PriorityLevelConfigurationCondition, PriorityLevelConfigurationStatus, PriorityLevelType,
    QueuingConfiguration,
};
use crate::validation::field::{Error, ErrorList, Path};

const MAX_QUEUES: i32 = 10 * 1000 * 1000; // 10^7

fn validate_queuing(q: &QueuingConfiguration, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if q.queue_length_limit <= 0 {
        errs.push(Error::invalid(
            &fld_path.child("queueLengthLimit"),
            q.queue_length_limit,
            "must be positive",
        ));
    }
    if q.queues <= 0 {
        errs.push(Error::invalid(
            &fld_path.child("queues"),
            q.queues,
            "must be positive",
        ));
    } else if q.queues > MAX_QUEUES {
        errs.push(Error::invalid(
            &fld_path.child("queues"),
            q.queues,
            format!("must not be greater than {}", MAX_QUEUES),
        ));
    }
    if q.hand_size <= 0 {
        errs.push(Error::invalid(
            &fld_path.child("handSize"),
            q.hand_size,
            "must be positive",
        ));
    } else if q.hand_size > q.queues {
        errs.push(Error::invalid(
            &fld_path.child("handSize"),
            q.hand_size,
            format!("should not be greater than queues ({})", q.queues),
        ));
    }
    errs
}

fn validate_limit_response(lr: &LimitResponse, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match lr.type_ {
        LimitResponseType::Reject => {
            if lr.queuing.is_some() {
                errs.push(Error::forbidden(
                    &fld_path.child("queuing"),
                    "must be nil if limited.limitResponse.type is not Limited",
                ));
            }
        }
        LimitResponseType::Queue => match &lr.queuing {
            None => errs.push(Error::required(
                &fld_path.child("queuing"),
                "must not be empty if limited.limitResponse.type is Limited",
            )),
            Some(q) => errs.extend(validate_queuing(q, &fld_path.child("queuing"))),
        },
    }
    errs
}

fn validate_limited(lplc: &LimitedPriorityLevelConfiguration, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(n) = lplc.nominal_concurrency_shares {
        if n < 0 {
            errs.push(Error::invalid(
                &fld_path.child("nominalConcurrencyShares"),
                n,
                "must be a non-negative integer",
            ));
        }
    }
    if let Some(b) = lplc.borrowing_limit_percent {
        if b < 0 {
            errs.push(Error::invalid(
                &fld_path.child("borrowingLimitPercent"),
                b,
                "if specified, must be a non-negative integer",
            ));
        }
    }
    // lendablePercent must be 0..=100 (upstream validation.go:432-434).
    if let Some(lp) = lplc.lendable_percent {
        if !(0..=100).contains(&lp) {
            errs.push(Error::invalid(
                &fld_path.child("lendablePercent"),
                lp,
                "must be between 0 and 100, inclusive",
            ));
        }
    }
    if let Some(lr) = &lplc.limit_response {
        errs.extend(validate_limit_response(
            lr,
            &fld_path.child("limitResponse"),
        ));
    }
    errs
}

fn validate_exempt(eplc: &ExemptPriorityLevelConfiguration, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(n) = eplc.nominal_concurrency_shares {
        if n < 0 {
            errs.push(Error::invalid(
                &fld_path.child("nominalConcurrencyShares"),
                n,
                "must be a non-negative integer",
            ));
        }
    }
    // lendablePercent must be 0..=100 (upstream validation.go:447-449).
    if let Some(lp) = eplc.lendable_percent {
        if !(0..=100).contains(&lp) {
            errs.push(Error::invalid(
                &fld_path.child("lendablePercent"),
                lp,
                "must be between 0 and 100, inclusive",
            ));
        }
    }
    errs
}

/// Validate a `PriorityLevelConfiguration`'s `status`. Mirrors upstream
/// `ValidatePriorityLevelConfigurationStatus`: each condition's `type` must be
/// unique within the list and non-empty.
fn validate_status(status: &PriorityLevelConfigurationStatus, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let conditions = status.conditions.as_deref().unwrap_or(&[]);
    let cp = fld_path.child("conditions");
    let mut keys: HashSet<&str> = HashSet::new();
    for (i, condition) in conditions.iter().enumerate() {
        if keys.contains(condition.type_.as_str()) {
            errs.push(Error::duplicate(
                &cp.index(i).child("type"),
                condition.type_.clone(),
            ));
        }
        keys.insert(condition.type_.as_str());
        errs.extend(validate_condition(condition, &cp.index(i)));
    }
    errs
}

/// Mirrors upstream `ValidatePriorityLevelConfigurationCondition`: condition
/// `type` is required.
fn validate_condition(
    condition: &PriorityLevelConfigurationCondition,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if condition.type_.is_empty() {
        errs.push(Error::required(&fld_path.child("type"), ""));
    }
    errs
}

/// Validate a `PriorityLevelConfiguration` on create. Mirrors upstream
/// `ValidatePriorityLevelConfiguration` (spec + status) minus ObjectMeta.
pub fn validate_priority_level_configuration(plc: &PriorityLevelConfiguration) -> ErrorList {
    let spec_path = Path::new("spec");
    let mut errs: ErrorList = Vec::new();
    let spec = &plc.spec;

    let is_exempt_type = matches!(spec.type_, PriorityLevelType::Exempt);
    let is_exempt_name = plc.metadata.name == "exempt";
    if is_exempt_name != is_exempt_type {
        errs.push(Error::invalid(
            &spec_path.child("type"),
            if is_exempt_type { "Exempt" } else { "Limited" },
            "must be 'Exempt' if and only if `name` is 'exempt'",
        ));
    }

    match spec.type_ {
        PriorityLevelType::Exempt => {
            if spec.limited.is_some() {
                errs.push(Error::forbidden(
                    &spec_path.child("limited"),
                    "must be nil if the type is not Limited",
                ));
            }
            if let Some(exempt) = &spec.exempt {
                errs.extend(validate_exempt(exempt, &spec_path.child("exempt")));
            }
        }
        PriorityLevelType::Limited => {
            if spec.exempt.is_some() {
                errs.push(Error::forbidden(
                    &spec_path.child("exempt"),
                    "must be nil if the type is Limited",
                ));
            }
            match &spec.limited {
                None => errs.push(Error::required(
                    &spec_path.child("limited"),
                    "must not be empty when type is Limited",
                )),
                Some(limited) => {
                    errs.extend(validate_limited(limited, &spec_path.child("limited")))
                }
            }
        }
    }

    if let Some(status) = &plc.status {
        errs.extend(validate_status(status, &Path::new("status")));
    }

    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::field::ErrorType;

    fn cond(type_: &str) -> PriorityLevelConfigurationCondition {
        PriorityLevelConfigurationCondition {
            type_: type_.to_string(),
            status: "True".to_string(),
            last_transition_time: None,
            reason: None,
            message: None,
        }
    }

    #[test]
    fn status_condition_type_required() {
        let status = PriorityLevelConfigurationStatus {
            conditions: Some(vec![cond("")]),
        };
        let errs = validate_status(&status, &Path::new("status"));
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error_type, ErrorType::Required);
        assert_eq!(errs[0].field, "status.conditions[0].type");
    }

    #[test]
    fn status_condition_duplicate_type() {
        let status = PriorityLevelConfigurationStatus {
            conditions: Some(vec![cond("ConcurrencyShared"), cond("ConcurrencyShared")]),
        };
        let errs = validate_status(&status, &Path::new("status"));
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error_type, ErrorType::Duplicate);
        assert_eq!(errs[0].field, "status.conditions[1].type");
    }

    #[test]
    fn status_conditions_unique_ok() {
        let status = PriorityLevelConfigurationStatus {
            conditions: Some(vec![cond("ConcurrencyShared"), cond("Ready")]),
        };
        assert!(validate_status(&status, &Path::new("status")).is_empty());
    }
}
