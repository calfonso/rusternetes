//! PriorityClass validation — port of upstream Kubernetes
//! `pkg/apis/scheduling/validation/validation.go::ValidatePriorityClass`
//! (release-1.35).
//!
//! Covers the system-prefix reservation (`system-*` names must be a known
//! system PriorityClass with the exact value/globalDefault), the
//! `HighestUserDefinablePriority` cap on user-defined classes, and the
//! `preemptionPolicy` enum. ObjectMeta is validated separately (#1087 / #1277).

use crate::resources::PriorityClass;
use crate::validation::field::{Error, ErrorList, Path};

// Upstream constants (`pkg/apis/scheduling/types.go`).
const HIGHEST_USER_DEFINABLE_PRIORITY: i32 = 1_000_000_000;
const SYSTEM_CRITICAL_PRIORITY: i32 = 2 * HIGHEST_USER_DEFINABLE_PRIORITY;
const SYSTEM_PRIORITY_CLASS_PREFIX: &str = "system-";
const SYSTEM_CLUSTER_CRITICAL: &str = "system-cluster-critical";
const SYSTEM_NODE_CRITICAL: &str = "system-node-critical";

/// Port of upstream `IsKnownSystemPriorityClass`. Returns `Ok(())` if the name
/// is a known system PriorityClass whose value and globalDefault match, else an
/// error message mirroring upstream wording.
fn is_known_system_priority_class(
    name: &str,
    value: i32,
    global_default: bool,
) -> Result<(), String> {
    // (name, value, globalDefault) for the two upstream system classes.
    let known: [(&str, i32); 2] = [
        (SYSTEM_NODE_CRITICAL, SYSTEM_CRITICAL_PRIORITY + 1000),
        (SYSTEM_CLUSTER_CRITICAL, SYSTEM_CRITICAL_PRIORITY),
    ];
    for (n, v) in known {
        if n == name {
            if v != value {
                return Err(format!("value of {} PriorityClass must be {}", n, v));
            }
            // All system classes have globalDefault = false.
            if global_default {
                return Err(format!(
                    "globalDefault of {} PriorityClass must be {}",
                    n, false
                ));
            }
            return Ok(());
        }
    }
    Err(format!("{} is not a known system priority class", name))
}

/// Validate a `PriorityClass` on create. Mirrors upstream `ValidatePriorityClass`
/// minus ObjectMeta.
pub fn validate_priority_class(pc: &PriorityClass) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let global_default = pc.global_default.unwrap_or(false);

    if pc.metadata.name.starts_with(SYSTEM_PRIORITY_CLASS_PREFIX) {
        if let Err(e) = is_known_system_priority_class(&pc.metadata.name, pc.value, global_default)
        {
            errs.push(Error::forbidden(
                &Path::new("metadata").child("name"),
                format!(
                    "priority class names with '{}' prefix are reserved for system use only. error: {}",
                    SYSTEM_PRIORITY_CLASS_PREFIX, e
                ),
            ));
        }
    } else if pc.value > HIGHEST_USER_DEFINABLE_PRIORITY {
        errs.push(Error::forbidden(
            &Path::new("value"),
            format!(
                "maximum allowed value of a user defined priority is {}",
                HIGHEST_USER_DEFINABLE_PRIORITY
            ),
        ));
    }

    if let Some(pp) = &pc.preemption_policy {
        // Port of apivalidation.ValidatePreemptionPolicy.
        if pp.is_empty() {
            errs.push(Error::required(&Path::new("preemptionPolicy"), ""));
        } else if pp != "PreemptLowerPriority" && pp != "Never" {
            errs.push(Error::not_supported(
                &Path::new("preemptionPolicy"),
                pp.clone(),
                &["PreemptLowerPriority", "Never"],
            ));
        }
    }

    errs
}
