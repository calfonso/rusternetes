//! ResourceQuota validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateResourceQuotaSpec`
//! (release-1.35).
//!
//! Scope: `hard` quantities (valid, non-negative), `scopes` (known values, no
//! conflicting pairs), and `scopeSelector.matchExpressions` (scope name +
//! operator/values consistency + conflicting pairs). The per-resource integer
//! check and deep resource-name qualification are left as a follow-up.

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

    // scopeSelector.matchExpressions
    errs.extend(validate_scope_selector(spec, fld_path));

    errs
}

/// Scopes that must use the `Exists` operator in a scopeSelector (only
/// `PriorityClass` supports `In`/`NotIn` with values).
const EXISTS_ONLY_SCOPES: &[&str] = &[
    "BestEffort",
    "NotBestEffort",
    "Terminating",
    "NotTerminating",
    "CrossNamespacePodAffinity",
];

/// Port of upstream `validateScopedResourceSelectorRequirement`: each
/// `scopeSelector.matchExpressions` entry has a known scopeName, an operator
/// consistent with that scope, and values consistent with the operator; and no
/// conflicting scope pair appears across the expressions.
fn validate_scope_selector(spec: &ResourceQuotaSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(sel) = &spec.scope_selector else {
        return errs;
    };
    let me_path = fld_path.child("scopeSelector").child("matchExpressions");
    let mut seen_scopes: Vec<&str> = Vec::new();

    for req in &sel.match_expressions {
        if !VALID_SCOPES.contains(&req.scope_name.as_str()) {
            errs.push(Error::invalid(
                &me_path.child("scopeName"),
                req.scope_name.clone(),
                "unsupported scope",
            ));
        }
        // Exists-only scopes reject any other operator.
        if EXISTS_ONLY_SCOPES.contains(&req.scope_name.as_str()) && req.operator != "Exists" {
            errs.push(Error::invalid(
                &me_path.child("operator"),
                req.operator.clone(),
                "must be 'Exists' when scope is any of ResourceQuotaScopeTerminating, ResourceQuotaScopeNotTerminating, ResourceQuotaScopeBestEffort, ResourceQuotaScopeNotBestEffort or ResourceQuotaScopeCrossNamespacePodAffinity",
            ));
        }
        let values_len = req.values.as_ref().map_or(0, |v| v.len());
        match req.operator.as_str() {
            "In" | "NotIn" => {
                if values_len == 0 {
                    errs.push(Error::required(
                        &me_path.child("values"),
                        "must be at least one value when `operator` is 'In' or 'NotIn' for scope selector",
                    ));
                }
            }
            "Exists" | "DoesNotExist" => {
                if values_len != 0 {
                    errs.push(Error::invalid(
                        &me_path.child("values"),
                        req.values.clone().unwrap_or_default(),
                        "must be no value when `operator` is 'Exist' or 'DoesNotExist' for scope selector",
                    ));
                }
            }
            other => {
                errs.push(Error::invalid(
                    &me_path.child("operator"),
                    other.to_string(),
                    "not a valid selector operator",
                ));
            }
        }
        seen_scopes.push(req.scope_name.as_str());
    }

    let has = |x: &str| seen_scopes.contains(&x);
    if has("BestEffort") && has("NotBestEffort") || has("Terminating") && has("NotTerminating") {
        errs.push(Error::invalid(
            &me_path,
            String::new(),
            "conflicting scopes",
        ));
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

#[cfg(test)]
mod scope_selector_tests {
    use super::*;

    fn errs(json: serde_json::Value) -> Vec<String> {
        let spec: ResourceQuotaSpec = serde_json::from_value(json).unwrap();
        validate_resource_quota_spec(&spec, &Path::new("spec"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn priorityclass_in_with_values_passes() {
        assert!(errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "PriorityClass", "operator": "In", "values": ["high"]}
            ]}
        }))
        .is_empty());
    }

    #[test]
    fn besteffort_must_use_exists() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "BestEffort", "operator": "In", "values": ["x"]}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("must be 'Exists'")), "{e:?}");
    }

    #[test]
    fn in_operator_requires_values() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "PriorityClass", "operator": "In"}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("at least one value")), "{e:?}");
    }

    #[test]
    fn exists_rejects_values() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "Terminating", "operator": "Exists", "values": ["x"]}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("no value")), "{e:?}");
    }

    #[test]
    fn bad_scope_and_operator_rejected() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "Nope", "operator": "Weird"}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("unsupported scope")), "{e:?}");
        assert!(
            e.iter()
                .any(|m| m.contains("not a valid selector operator")),
            "{e:?}"
        );
    }

    #[test]
    fn conflicting_scope_pair_rejected() {
        let e = errs(serde_json::json!({
            "scopeSelector": {"matchExpressions": [
                {"scopeName": "BestEffort", "operator": "Exists"},
                {"scopeName": "NotBestEffort", "operator": "Exists"}
            ]}
        }));
        assert!(e.iter().any(|m| m.contains("conflicting scopes")), "{e:?}");
    }
}
