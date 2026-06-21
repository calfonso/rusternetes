//! ResourceQuota validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateResourceQuotaSpec`
//! (release-1.35).
//!
//! Scope: `hard` quantities (valid, non-negative) and `scopes` (known values,
//! no conflicting pairs). The per-resource integer check, deep resource-name
//! qualification, and `scopeSelector` match-expression validation are left as a
//! follow-up.

use crate::quantity::Quantity;
use crate::resources::policy::{ResourceQuota, ResourceQuotaSpec};
use crate::validation::field::{Error, ErrorList, Path};

/// The standard ResourceQuota scopes (upstream `IsStandardResourceQuotaScope`).
const VALID_SCOPES: &[&str] = &[
    "Terminating",
    "NotTerminating",
    "BestEffort",
    "NotBestEffort",
    "PriorityClass",
    "CrossNamespacePodAffinity",
];

/// Validate a `ResourceQuotaSpec`. Mirrors upstream `ValidateResourceQuotaSpec`.
pub fn validate_resource_quota_spec(spec: &ResourceQuotaSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // hard: each value must be a valid, non-negative quantity.
    if let Some(hard) = &spec.hard {
        let hard_path = fld_path.child("hard");
        for (k, v) in hard {
            let p = hard_path.child(k);
            match Quantity::parse(v) {
                Err(_) => errs.push(Error::invalid(
                    &p,
                    v.clone(),
                    "must be a valid resource quantity",
                )),
                Ok(q) => {
                    if q.is_negative() {
                        errs.push(Error::invalid(
                            &p,
                            v.clone(),
                            "must be greater than or equal to 0",
                        ));
                    }
                }
            }
        }
    }

    // scopes: each must be a known value; certain pairs conflict.
    if let Some(scopes) = &spec.scopes {
        if !scopes.is_empty() {
            let scopes_path = fld_path.child("scopes");
            for s in scopes {
                if !VALID_SCOPES.contains(&s.as_str()) {
                    errs.push(Error::invalid(&scopes_path, s.clone(), "unsupported scope"));
                }
            }
            let has = |x: &str| scopes.iter().any(|s| s == x);
            if has("BestEffort") && has("NotBestEffort") {
                errs.push(Error::invalid(
                    &scopes_path,
                    "BestEffort,NotBestEffort".to_string(),
                    "conflicting scopes",
                ));
            }
            if has("Terminating") && has("NotTerminating") {
                errs.push(Error::invalid(
                    &scopes_path,
                    "Terminating,NotTerminating".to_string(),
                    "conflicting scopes",
                ));
            }
        }
    }

    errs
}

/// Validate a new `ResourceQuota`. Mirrors upstream `ValidateResourceQuota`.
pub fn validate_resource_quota(rq: &ResourceQuota) -> ErrorList {
    validate_resource_quota_spec(&rq.spec, &Path::new("spec"))
}

/// Validate a ResourceQuota update — upstream `ValidateResourceQuotaUpdate`
/// (pkg/apis/core/validation): re-validate the spec, and `spec.scopes` is
/// immutable (compared as a set).
pub fn validate_resource_quota_update(new_rq: &ResourceQuota, old_rq: &ResourceQuota) -> ErrorList {
    let mut errs = validate_resource_quota_spec(&new_rq.spec, &Path::new("spec"));

    let new_scopes: std::collections::HashSet<&String> =
        new_rq.spec.scopes.iter().flatten().collect();
    let old_scopes: std::collections::HashSet<&String> =
        old_rq.spec.scopes.iter().flatten().collect();
    if new_scopes != old_scopes {
        let shown = new_rq.spec.scopes.clone().unwrap_or_default().join(",");
        errs.push(Error::invalid(
            &Path::new("spec").child("scopes"),
            shown,
            "field is immutable",
        ));
    }

    errs
}
