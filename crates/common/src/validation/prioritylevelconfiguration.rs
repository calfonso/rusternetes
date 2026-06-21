//! PriorityLevelConfiguration (APF) validation — port of upstream Kubernetes
//! `pkg/apis/flowcontrol/validation/validation.go` (release-1.35).
//!
//! Covers the `type` ↔ name(`exempt`) coupling, the exempt/limited field
//! coupling, `limited` (nominalConcurrencyShares / borrowingLimitPercent /
//! limitResponse + queuing), and `exempt` numeric bounds. ObjectMeta is
//! validated separately (#1087 / #1277).
//!
//! Note: the rusternetes type uses `lending_concurrency_limit` rather than
//! upstream's `lendablePercent`, so the 0–100 lendable-percent check has no
//! field to apply to and is omitted. The shuffle-sharding entropy-bits check on
//! `handSize`/`queues` is also omitted (exotic); the positivity, max-queues, and
//! `handSize <= queues` checks are ported.

use crate::resources::flowcontrol::{
    ExemptPriorityLevelConfiguration, LimitResponse, LimitResponseType,
    LimitedPriorityLevelConfiguration, PriorityLevelConfiguration, PriorityLevelType,
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
    errs
}

/// Validate a `PriorityLevelConfiguration` on create. Mirrors upstream
/// `ValidatePriorityLevelConfigurationSpec` minus ObjectMeta.
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

    errs
}
