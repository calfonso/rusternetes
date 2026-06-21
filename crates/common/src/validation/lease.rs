//! Lease validation — port of upstream Kubernetes
//! `pkg/apis/coordination/validation/validation.go::ValidateLease` (release-1.35).
//!
//! Covers `spec`: `leaseDurationSeconds > 0`, `leaseTransitions >= 0`, the
//! `strategy` coordinated-leader-election value, and the `preferredHolder`↔
//! `strategy` coupling. ObjectMeta is validated separately (#1087 / #1277).

use crate::resources::coordination::Lease;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_qualified_name;

/// Built-in coordinated-lease strategies (`validLeaseStrategies`).
const VALID_LEASE_STRATEGIES: [&str; 1] = ["OldestEmulationVersion"];

/// Validate a `Lease` on create. Mirrors upstream `ValidateLeaseSpec`.
pub fn validate_lease(lease: &Lease) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(spec) = &lease.spec else {
        return errs;
    };
    let spec_path = Path::new("spec");

    if let Some(d) = spec.lease_duration_seconds {
        if d <= 0 {
            errs.push(Error::invalid(
                &spec_path.child("leaseDurationSeconds"),
                d,
                "must be greater than 0",
            ));
        }
    }
    if let Some(t) = spec.lease_transitions {
        if t < 0 {
            errs.push(Error::invalid(
                &spec_path.child("leaseTransitions"),
                t,
                "must be greater than or equal to 0",
            ));
        }
    }

    if let Some(strategy) = &spec.strategy {
        let sp = spec_path.child("strategy");
        // Single-segment names must be Kubernetes-defined; a "/"-qualified name
        // must be a valid qualified name.
        if strategy.contains('/') {
            for msg in is_qualified_name(strategy) {
                errs.push(Error::invalid(&sp, strategy.clone(), msg));
            }
        } else if !VALID_LEASE_STRATEGIES.contains(&strategy.as_str()) {
            errs.push(Error::not_supported(
                &sp,
                strategy.clone(),
                &VALID_LEASE_STRATEGIES,
            ));
        }
    }

    // preferredHolder may only be set when a strategy is defined.
    if let Some(ph) = &spec.preferred_holder {
        if !ph.is_empty() && spec.strategy.as_deref().unwrap_or("").is_empty() {
            errs.push(Error::forbidden(
                &spec_path.child("preferredHolder"),
                "may only be specified if `strategy` is defined",
            ));
        }
    }

    errs
}
