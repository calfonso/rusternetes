//! ResourceQuota validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateResourceQuotaSpec`
//! (release-1.35).
//!
//! Scope: `hard` resource names (qualified-name + standard-quota-resource
//! qualification) and quantities (valid, non-negative, integer-only resources
//! must be whole), `scopes` (known values, no conflicting pairs),
//! `scopeSelector.matchExpressions` (scope name + operator/values consistency +
//! conflicting pairs), and status updates (`resourceVersion` required + the
//! same name/quantity checks over `status.hard` / `status.used`).

use crate::quantity::Quantity;
use crate::resources::policy::{ResourceQuota, ResourceQuotaSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_qualified_name;
use std::collections::HashMap;

/// The standard ResourceQuota scopes (upstream `IsStandardResourceQuotaScope`).
const VALID_SCOPES: &[&str] = &[
    "Terminating",
    "NotTerminating",
    "BestEffort",
    "NotBestEffort",
    "PriorityClass",
    "CrossNamespacePodAffinity",
];

/// Standard resource names known to the quota tracking system. Mirrors
/// upstream `helper.standardQuotaResources`.
const STANDARD_QUOTA_RESOURCES: &[&str] = &[
    "cpu",
    "memory",
    "ephemeral-storage",
    "requests.cpu",
    "requests.memory",
    "requests.storage",
    "requests.ephemeral-storage",
    "limits.cpu",
    "limits.memory",
    "limits.ephemeral-storage",
    "pods",
    "resourcequotas",
    "services",
    "replicationcontrollers",
    "secrets",
    "persistentvolumeclaims",
    "configmaps",
    "services.nodeports",
    "services.loadbalancers",
];

/// Resources that are measured in whole-integer values. Mirrors upstream
/// `helper.integerResources` (extended resources are also integer, see
/// [`is_integer_resource_name`]).
const INTEGER_RESOURCES: &[&str] = &[
    "pods",
    "resourcequotas",
    "services",
    "replicationcontrollers",
    "secrets",
    "configmaps",
    "persistentvolumeclaims",
    "services.nodeports",
    "services.loadbalancers",
];

const HUGEPAGES_PREFIX: &str = "hugepages-";
const REQUESTS_HUGEPAGES_PREFIX: &str = "requests.hugepages-";
const DEFAULT_REQUESTS_PREFIX: &str = "requests.";
const NATIVE_NAMESPACE_PREFIX: &str = "kubernetes.io/";

/// `helper.IsQuotaHugePageResourceName`.
fn is_quota_hugepage_resource_name(name: &str) -> bool {
    name.starts_with(HUGEPAGES_PREFIX) || name.starts_with(REQUESTS_HUGEPAGES_PREFIX)
}

/// `helper.IsStandardQuotaResourceName`.
fn is_standard_quota_resource_name(name: &str) -> bool {
    STANDARD_QUOTA_RESOURCES.contains(&name) || is_quota_hugepage_resource_name(name)
}

/// `helper.IsNativeResource`: unprefixed names, or names in `kubernetes.io/`.
fn is_native_resource(name: &str) -> bool {
    !name.contains('/') || name.contains(NATIVE_NAMESPACE_PREFIX)
}

/// `helper.IsExtendedResourceName`.
fn is_extended_resource_name(name: &str) -> bool {
    if is_native_resource(name) || name.starts_with(DEFAULT_REQUESTS_PREFIX) {
        return false;
    }
    let name_for_quota = format!("{DEFAULT_REQUESTS_PREFIX}{name}");
    is_qualified_name(&name_for_quota).is_empty()
}

/// `helper.IsIntegerResourceName`.
fn is_integer_resource_name(name: &str) -> bool {
    INTEGER_RESOURCES.contains(&name) || is_extended_resource_name(name)
}

/// Port of upstream `ValidateResourceQuotaResourceName`: the key is a valid
/// qualified name, and — when unprefixed (no `/`) — a standard quota resource.
fn validate_resource_quota_resource_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for msg in is_qualified_name(name) {
        errs.push(Error::invalid(fld_path, name.to_string(), msg));
    }
    if !errs.is_empty() {
        return errs;
    }
    // Unprefixed names (a single "/"-split segment) must be standard.
    if name.split('/').count() == 1 && !is_standard_quota_resource_name(name) {
        errs.push(Error::invalid(
            fld_path,
            name.to_string(),
            "must be a standard resource for quota",
        ));
    }
    errs
}

/// Port of upstream `ValidateResourceQuantityValue`: the quantity is a valid,
/// non-negative quantity, and — for integer-only resources — a whole integer
/// (`MilliValue() % 1000 == 0`).
fn validate_resource_quantity_value(name: &str, value: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match Quantity::parse(value) {
        Err(_) => {
            errs.push(Error::invalid(
                fld_path,
                value.to_string(),
                "must be a valid resource quantity",
            ));
            return errs;
        }
        Ok(q) => {
            if q.is_negative() {
                errs.push(Error::invalid(
                    fld_path,
                    value.to_string(),
                    "must be greater than or equal to 0",
                ));
            }
            if is_integer_resource_name(name) && q.milli_value() % 1000 != 0 {
                errs.push(Error::invalid(
                    fld_path,
                    value.to_string(),
                    "must be an integer",
                ));
            }
        }
    }
    errs
}

/// Validate a `resourceName: quantity` map (`spec.hard`, `status.hard`,
/// `status.used`) under `fld_path`, keying each entry with `.key()` to match
/// upstream `fldPath.Key(string(k))`.
fn validate_resource_quota_resources(
    resources: &HashMap<String, String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (k, v) in resources {
        let res_path = fld_path.key(k.clone());
        errs.extend(validate_resource_quota_resource_name(k, &res_path));
        errs.extend(validate_resource_quantity_value(k, v, &res_path));
    }
    errs
}

/// Validate a `ResourceQuotaSpec`. Mirrors upstream `ValidateResourceQuotaSpec`.
pub fn validate_resource_quota_spec(spec: &ResourceQuotaSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // hard: each key must be a qualified, standard quota resource name and each
    // value a valid, non-negative (integer-where-required) quantity.
    if let Some(hard) = &spec.hard {
        errs.extend(validate_resource_quota_resources(
            hard,
            &fld_path.child("hard"),
        ));
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

/// Validate a ResourceQuota status update — upstream
/// `ValidateResourceQuotaStatusUpdate` (pkg/apis/core/validation): the new
/// object must carry a `resourceVersion`, and every `status.hard` / `status.used`
/// entry must pass the same resource-name and quantity-value checks as the spec.
///
/// (ObjectMeta-update validation is performed by the caller / object-meta
/// validator; this mirrors only the status-specific rules.)
pub fn validate_resource_quota_status_update(
    new_rq: &ResourceQuota,
    _old_rq: &ResourceQuota,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if new_rq
        .metadata
        .resource_version
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        errs.push(Error::required(&Path::new("resourceVersion"), ""));
    }

    if let Some(status) = &new_rq.status {
        let status_path = Path::new("status");
        if let Some(hard) = &status.hard {
            errs.extend(validate_resource_quota_resources(
                hard,
                &status_path.child("hard"),
            ));
        }
        if let Some(used) = &status.used {
            errs.extend(validate_resource_quota_resources(
                used,
                &status_path.child("used"),
            ));
        }
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

#[cfg(test)]
mod hard_resource_tests {
    use super::*;

    fn errs(json: serde_json::Value) -> Vec<String> {
        let spec: ResourceQuotaSpec = serde_json::from_value(json).unwrap();
        validate_resource_quota_spec(&spec, &Path::new("spec"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn standard_quota_resource_passes() {
        assert!(errs(
            serde_json::json!({ "hard": { "pods": "10", "requests.cpu": "2", "memory": "1Gi" } })
        )
        .is_empty());
    }

    #[test]
    fn hugepages_quota_resource_passes() {
        assert!(errs(serde_json::json!({ "hard": {
                "hugepages-2Mi": "100Mi",
                "requests.hugepages-2Mi": "100Mi"
            } }))
        .is_empty());
    }

    #[test]
    fn fully_qualified_resource_passes() {
        // A "/"-prefixed name is not standard-quota-checked, only qualified.
        assert!(
            errs(serde_json::json!({ "hard": { "example.com/dongle": "4" } })).is_empty(),
            "{:?}",
            errs(serde_json::json!({ "hard": { "example.com/dongle": "4" } }))
        );
    }

    #[test]
    fn unprefixed_nonstandard_resource_rejected() {
        let e = errs(serde_json::json!({ "hard": { "bananas": "10" } }));
        assert!(
            e.iter()
                .any(|m| m.contains("must be a standard resource for quota")),
            "{e:?}"
        );
    }

    #[test]
    fn invalid_qualified_name_rejected() {
        let e = errs(serde_json::json!({ "hard": { "Bad Name!": "10" } }));
        assert!(!e.is_empty(), "expected a qualified-name error, got {e:?}");
        // A bad qualified name short-circuits the standard-resource check.
        assert!(
            !e.iter()
                .any(|m| m.contains("must be a standard resource for quota")),
            "{e:?}"
        );
    }

    #[test]
    fn integer_resource_with_fraction_rejected() {
        let e = errs(serde_json::json!({ "hard": { "pods": "10.5" } }));
        assert!(e.iter().any(|m| m.contains("must be an integer")), "{e:?}");
    }

    #[test]
    fn integer_resource_with_milli_rejected() {
        // 500m of pods is 0.5 → MilliValue() % 1000 != 0.
        let e = errs(serde_json::json!({ "hard": { "services": "500m" } }));
        assert!(e.iter().any(|m| m.contains("must be an integer")), "{e:?}");
    }

    #[test]
    fn integer_resource_whole_value_passes() {
        assert!(errs(serde_json::json!({ "hard": { "pods": "10" } })).is_empty());
        // 1000m == 1, which is a whole integer.
        assert!(errs(serde_json::json!({ "hard": { "pods": "1000m" } })).is_empty());
    }

    #[test]
    fn fractional_cpu_allowed_not_integer_resource() {
        // cpu is not an integer-only resource; 500m is fine.
        assert!(errs(serde_json::json!({ "hard": { "requests.cpu": "500m" } })).is_empty());
    }

    #[test]
    fn negative_quantity_rejected() {
        let e = errs(serde_json::json!({ "hard": { "memory": "-1" } }));
        assert!(
            e.iter().any(|m| m.contains("greater than or equal to 0")),
            "{e:?}"
        );
    }

    // Note: a syntactically-invalid quantity (e.g. "notaquantity") is rejected
    // at deserialization by the custom Quantity deserializer, before this
    // validator runs — so the `Quantity::parse` Err arm is a backstop the API
    // path can't reach via `from_value` (not unit-tested here).
}

#[cfg(test)]
mod status_update_tests {
    use super::*;

    fn rq(json: serde_json::Value) -> ResourceQuota {
        serde_json::from_value(json).unwrap()
    }

    fn msgs(new: &ResourceQuota, old: &ResourceQuota) -> Vec<String> {
        validate_resource_quota_status_update(new, old)
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_status_update_passes() {
        let new = rq(serde_json::json!({
            "metadata": { "name": "q", "resourceVersion": "42" },
            "spec": {},
            "status": { "hard": { "pods": "10" }, "used": { "pods": "3" } }
        }));
        let old = rq(serde_json::json!({
            "metadata": { "name": "q", "resourceVersion": "41" },
            "spec": {}
        }));
        assert!(msgs(&new, &old).is_empty(), "{:?}", msgs(&new, &old));
    }

    #[test]
    fn missing_resource_version_rejected() {
        let new = rq(serde_json::json!({
            "metadata": { "name": "q" },
            "spec": {},
            "status": { "hard": { "pods": "10" } }
        }));
        let old = rq(serde_json::json!({ "metadata": { "name": "q" }, "spec": {} }));
        let e = msgs(&new, &old);
        assert!(
            e.iter()
                .any(|m| m.contains("resourceVersion") && m.contains("Required value")),
            "{e:?}"
        );
    }

    #[test]
    fn status_used_bad_resource_name_rejected() {
        let new = rq(serde_json::json!({
            "metadata": { "name": "q", "resourceVersion": "7" },
            "spec": {},
            "status": { "used": { "bananas": "1" } }
        }));
        let old = rq(serde_json::json!({ "metadata": { "name": "q" }, "spec": {} }));
        let e = msgs(&new, &old);
        assert!(
            e.iter().any(|m| m.contains("status.used")
                && m.contains("must be a standard resource for quota")),
            "{e:?}"
        );
    }

    #[test]
    fn status_hard_integer_fraction_rejected() {
        let new = rq(serde_json::json!({
            "metadata": { "name": "q", "resourceVersion": "7" },
            "spec": {},
            "status": { "hard": { "pods": "2500m" } }
        }));
        let old = rq(serde_json::json!({ "metadata": { "name": "q" }, "spec": {} }));
        let e = msgs(&new, &old);
        assert!(
            e.iter()
                .any(|m| m.contains("status.hard") && m.contains("must be an integer")),
            "{e:?}"
        );
    }
}
