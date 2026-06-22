//! RuntimeClass validation — port of upstream Kubernetes
//! `pkg/apis/node/validation/validation.go::ValidateRuntimeClass` (release-1.35).
//!
//! Covers `handler` (must be a DNS-1123 label), `overhead.podFixed` (full
//! container-resource-requirements checks: valid container resource names,
//! non-negative + integer-divisible quantities, and the "HugePages require cpu
//! or memory" rule), and `scheduling` (nodeSelector labels + tolerations,
//! including the RuntimeClass-specific toleration-uniqueness rule). ObjectMeta
//! is validated separately (#1087 / #1277).

use crate::quantity::Quantity;
use crate::resources::pod::Toleration;
use crate::resources::runtimeclass::{Overhead, Scheduling};
use crate::resources::RuntimeClass;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_qualified_name, validate_labels};
use crate::validation::pod::validate_tolerations;

/// Standard container resource names (no `/` namespace). Mirrors upstream
/// `standardContainerResources` in `pkg/apis/core/helper/helpers.go`.
const STANDARD_CONTAINER_RESOURCES: &[&str] = &["cpu", "memory", "ephemeral-storage"];

/// Standard (system-known) resource names. Mirrors upstream
/// `standardResources` in `pkg/apis/core/helper/helpers.go`.
const STANDARD_RESOURCES: &[&str] = &[
    "cpu",
    "memory",
    "ephemeral-storage",
    "requests.cpu",
    "requests.memory",
    "requests.ephemeral-storage",
    "limits.cpu",
    "limits.memory",
    "limits.ephemeral-storage",
    "pods",
    "resourcequotas",
    "services",
    "replicationcontrollers",
    "secrets",
    "configmaps",
    "persistentvolumeclaims",
    "storage",
    "requests.storage",
    "services.nodeports",
    "services.loadbalancers",
];

/// Upstream `core.ResourceHugePagesPrefix`.
const HUGEPAGES_PREFIX: &str = "hugepages-";
/// Upstream `core.ResourceRequestsHugePagesPrefix`.
const REQUESTS_HUGEPAGES_PREFIX: &str = "requests.hugepages-";
/// Upstream `core.ResourceDefaultNamespacePrefix` (`kubernetes.io/`).
const DEFAULT_NAMESPACE_PREFIX: &str = "kubernetes.io/";
/// Upstream `core.DefaultResourceRequestsPrefix` (`requests.`).
const DEFAULT_RESOURCE_REQUESTS_PREFIX: &str = "requests.";

/// Mirrors upstream `IsHugePageResourceName`.
fn is_huge_page_resource_name(name: &str) -> bool {
    name.starts_with(HUGEPAGES_PREFIX)
}

/// Mirrors upstream `IsQuotaHugePageResourceName`.
fn is_quota_huge_page_resource_name(name: &str) -> bool {
    name.starts_with(HUGEPAGES_PREFIX) || name.starts_with(REQUESTS_HUGEPAGES_PREFIX)
}

/// Mirrors upstream `IsStandardResourceName`.
fn is_standard_resource_name(name: &str) -> bool {
    STANDARD_RESOURCES.contains(&name) || is_quota_huge_page_resource_name(name)
}

/// Mirrors upstream `IsStandardContainerResourceName`.
fn is_standard_container_resource_name(name: &str) -> bool {
    STANDARD_CONTAINER_RESOURCES.contains(&name) || is_huge_page_resource_name(name)
}

/// Mirrors upstream `IsNativeResource`: partially-qualified (no `/`) names are
/// implicitly in the `kubernetes.io/` namespace.
fn is_native_resource(name: &str) -> bool {
    !name.contains('/') || name.contains(DEFAULT_NAMESPACE_PREFIX)
}

/// Mirrors upstream `IsExtendedResourceName`.
fn is_extended_resource_name(name: &str) -> bool {
    if is_native_resource(name) || name.starts_with(DEFAULT_RESOURCE_REQUESTS_PREFIX) {
        return false;
    }
    // Ensure it satisfies IsQualifiedName after conversion into a quota name.
    let name_for_quota = format!("{DEFAULT_RESOURCE_REQUESTS_PREFIX}{name}");
    is_qualified_name(&name_for_quota).is_empty()
}

/// Mirrors upstream `validateResourceName` (`pkg/apis/core/validation`):
/// must be a qualified name, and if unprefixed must be a standard resource.
fn validate_resource_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for msg in is_qualified_name(name) {
        errs.push(Error::invalid(fld_path, name.to_string(), msg));
    }
    if !errs.is_empty() {
        return errs;
    }
    if !name.contains('/') && !is_standard_resource_name(name) {
        errs.push(Error::invalid(
            fld_path,
            name.to_string(),
            "must be a standard resource type or fully qualified",
        ));
    }
    errs
}

/// Mirrors upstream `validateContainerResourceName`.
fn validate_container_resource_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs = validate_resource_name(name, fld_path);
    if !name.contains('/') {
        if !is_standard_container_resource_name(name) {
            errs.push(Error::invalid(
                fld_path,
                name.to_string(),
                "must be a standard resource for containers",
            ));
        }
    } else if !is_native_resource(name) && !is_extended_resource_name(name) {
        errs.push(Error::invalid(
            fld_path,
            name.to_string(),
            "doesn't follow extended resource name standard",
        ));
    }
    errs
}

/// Port of upstream `validateOverhead`, which reuses
/// `ValidateContainerResourceRequirements` with only `Limits` populated from
/// `podFixed`. Because `Requests` is always empty here, the request/limit
/// comparison and required-limit branches never fire; we implement the limit
/// branch only: per-entry container-resource-name validation,
/// non-negative + integer-divisible quantity validation, and the
/// "HugePages require cpu or memory" cross-field check.
fn validate_overhead(overhead: &Overhead, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(pod_fixed) = &overhead.pod_fixed else {
        return errs;
    };
    let pf_path = fld_path.child("podFixed");

    let mut contains_cpu_or_memory = false;
    let mut contains_huge_pages = false;

    for (name, value) in pod_fixed {
        let kpath = pf_path.key(name);

        // Validate resource name (container-resource rules).
        errs.extend(validate_container_resource_name(name, &kpath));

        // Validate resource quantity: must parse, must be non-negative, and
        // for integer (extended) resources must be a whole number.
        match Quantity::parse(value) {
            Ok(q) => {
                if q.is_negative() {
                    errs.push(Error::invalid(
                        &kpath,
                        value.clone(),
                        "must be greater than or equal to 0",
                    ));
                }
                // Upstream ValidateResourceQuantityValue: integer resources
                // (extended resources) must not carry a fractional value.
                if is_extended_resource_name(name) && !q.is_integer() {
                    errs.push(Error::invalid(&kpath, value.clone(), "must be an integer"));
                }
            }
            Err(e) => errs.push(Error::invalid(&kpath, value.clone(), e.to_string())),
        }

        if is_huge_page_resource_name(name) {
            contains_huge_pages = true;
        }
        if name == "cpu" || name == "memory" {
            contains_cpu_or_memory = true;
        }
    }

    // Upstream: HugePages limits require an accompanying cpu or memory entry.
    if !contains_cpu_or_memory && contains_huge_pages {
        errs.push(Error::forbidden(
            &pf_path,
            "HugePages require cpu or memory",
        ));
    }

    errs
}

/// Port of upstream node-validation `validateTolerations`: the shared pod
/// toleration checks, plus a RuntimeClass-specific uniqueness check. The
/// shared `validate_tolerations` (pod.rs) does NOT dedupe — upstream's
/// `pkg/apis/core/validation::ValidateTolerations` doesn't either; the
/// uniqueness check is added only by the node validator's wrapper.
fn validate_scheduling_tolerations(tolerations: &[Toleration], fld_path: &Path) -> ErrorList {
    let mut errs = validate_tolerations(tolerations, fld_path);

    // Ensure uniqueness of tolerations by (Key, Operator, Value, Effect).
    // Toleration is not Hash/Eq, so we linear-scan the already-seen list; the
    // toleration count is tiny so this is cheap. tolerationSeconds is NOT part
    // of the list-key (matches upstream's listKey).
    let mut seen: Vec<&Toleration> = Vec::new();
    for (i, t) in tolerations.iter().enumerate() {
        let is_dup = seen.iter().any(|s| {
            s.key == t.key && s.operator == t.operator && s.value == t.value && s.effect == t.effect
        });
        if is_dup {
            // Upstream passes the whole toleration as the duplicate value.
            let value = serde_json::to_value(t).unwrap_or(serde_json::Value::Null);
            errs.push(Error::duplicate(&fld_path.index(i), value));
        } else {
            seen.push(t);
        }
    }

    errs
}

/// Port of upstream `validateScheduling` — nodeSelector labels + tolerations.
fn validate_scheduling(scheduling: &Scheduling, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(node_selector) = &scheduling.node_selector {
        errs.extend(validate_labels(
            node_selector,
            &fld_path.child("nodeSelector"),
        ));
    }
    if let Some(tolerations) = &scheduling.tolerations {
        errs.extend(validate_scheduling_tolerations(
            tolerations,
            &fld_path.child("tolerations"),
        ));
    }
    errs
}

/// Validate a `RuntimeClass` on create. Mirrors upstream `ValidateRuntimeClass`
/// minus ObjectMeta.
pub fn validate_runtime_class(rc: &RuntimeClass) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // handler must be a DNS-1123 label (also rejects empty).
    for msg in is_dns1123_label(&rc.handler) {
        errs.push(Error::invalid(
            &Path::new("handler"),
            rc.handler.clone(),
            msg,
        ));
    }

    if let Some(overhead) = &rc.overhead {
        errs.extend(validate_overhead(overhead, &Path::new("overhead")));
    }
    if let Some(scheduling) = &rc.scheduling {
        errs.extend(validate_scheduling(scheduling, &Path::new("scheduling")));
    }

    errs
}

/// Validate a RuntimeClass update — upstream `ValidateRuntimeClassUpdate`
/// (pkg/apis/node/validation): `handler` is immutable.
pub fn validate_runtime_class_update(new_rc: &RuntimeClass, old_rc: &RuntimeClass) -> ErrorList {
    let mut errs = ErrorList::new();
    if new_rc.handler != old_rc.handler {
        errs.push(Error::invalid(
            &Path::new("handler"),
            new_rc.handler.clone(),
            "field is immutable",
        ));
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::pod::Toleration;
    use crate::validation::field::ErrorType;
    use std::collections::HashMap;

    fn tol(key: &str, op: &str, val: &str, effect: &str) -> Toleration {
        Toleration {
            key: Some(key.to_string()),
            operator: Some(op.to_string()),
            value: Some(val.to_string()),
            effect: Some(effect.to_string()),
            toleration_seconds: None,
        }
    }

    fn overhead_with(pairs: &[(&str, &str)]) -> Overhead {
        let mut m = HashMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), (*v).to_string());
        }
        Overhead { pod_fixed: Some(m) }
    }

    // ---- overhead.podFixed -------------------------------------------------

    #[test]
    fn overhead_valid_standard_resources() {
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("cpu", "250m"), ("memory", "128Mi")]));
        assert!(validate_runtime_class(&rc).is_empty());
    }

    #[test]
    fn overhead_valid_extended_integer_resource() {
        // Extended resource with whole-number quantity is OK.
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("example.com/gpu", "2")]));
        assert!(validate_runtime_class(&rc).is_empty());
    }

    #[test]
    fn overhead_rejects_fractional_extended_resource() {
        // Extended (integer) resources must not be fractional.
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("example.com/gpu", "1500m")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter()
                .any(|e| e.field == "overhead.podFixed[example.com/gpu]"
                    && e.detail == "must be an integer"),
            "expected integer error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_rejects_unprefixed_nonstandard_resource() {
        // A bare (unprefixed) name that is not a standard resource is rejected.
        let rc = RuntimeClass::new("rc", "runc").with_overhead(overhead_with(&[("widgets", "1")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter().any(|e| e.field == "overhead.podFixed[widgets]"
                && e.detail == "must be a standard resource type or fully qualified"),
            "expected standard-resource error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_rejects_negative_quantity() {
        let rc = RuntimeClass::new("rc", "runc").with_overhead(overhead_with(&[("memory", "-1")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter().any(|e| e.field == "overhead.podFixed[memory]"
                && e.detail == "must be greater than or equal to 0"),
            "expected non-negative error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_rejects_unparseable_quantity() {
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("cpu", "not-a-number")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter()
                .any(|e| e.field == "overhead.podFixed[cpu]" && e.error_type == ErrorType::Invalid),
            "expected parse error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_hugepages_require_cpu_or_memory() {
        // hugepages-2Mi without cpu/memory → forbidden.
        let rc = RuntimeClass::new("rc", "runc")
            .with_overhead(overhead_with(&[("hugepages-2Mi", "2Mi")]));
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter().any(|e| e.field == "overhead.podFixed"
                && e.error_type == ErrorType::Forbidden
                && e.detail == "HugePages require cpu or memory"),
            "expected hugepages-forbidden error, got {errs:?}"
        );
    }

    #[test]
    fn overhead_hugepages_with_memory_ok() {
        let rc = RuntimeClass::new("rc", "runc").with_overhead(overhead_with(&[
            ("hugepages-2Mi", "2Mi"),
            ("memory", "64Mi"),
        ]));
        let errs = validate_runtime_class(&rc);
        assert!(
            !errs
                .iter()
                .any(|e| e.detail == "HugePages require cpu or memory"),
            "did not expect hugepages-forbidden error, got {errs:?}"
        );
    }

    // ---- scheduling.tolerations uniqueness --------------------------------

    #[test]
    fn tolerations_unique_ok() {
        let scheduling = Scheduling {
            node_selector: None,
            tolerations: Some(vec![
                tol("k1", "Equal", "v1", "NoSchedule"),
                tol("k2", "Equal", "v2", "NoSchedule"),
            ]),
        };
        let rc = RuntimeClass::new("rc", "runc").with_scheduling(scheduling);
        assert!(validate_runtime_class(&rc).is_empty());
    }

    #[test]
    fn tolerations_duplicate_rejected() {
        let dup = tol("k1", "Equal", "v1", "NoSchedule");
        let scheduling = Scheduling {
            node_selector: None,
            tolerations: Some(vec![dup.clone(), dup]),
        };
        let rc = RuntimeClass::new("rc", "runc").with_scheduling(scheduling);
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter()
                .any(|e| e.field == "scheduling.tolerations[1]"
                    && e.error_type == ErrorType::Duplicate),
            "expected duplicate error at index 1, got {errs:?}"
        );
    }

    #[test]
    fn tolerations_differ_by_effect_not_duplicate() {
        let scheduling = Scheduling {
            node_selector: None,
            tolerations: Some(vec![
                tol("k1", "Equal", "v1", "NoSchedule"),
                tol("k1", "Equal", "v1", "NoExecute"),
            ]),
        };
        let rc = RuntimeClass::new("rc", "runc").with_scheduling(scheduling);
        let errs = validate_runtime_class(&rc);
        assert!(
            !errs.iter().any(|e| e.error_type == ErrorType::Duplicate),
            "did not expect duplicate error, got {errs:?}"
        );
    }

    #[test]
    fn tolerations_duplicate_ignores_toleration_seconds() {
        // tolerationSeconds is NOT part of the list-key, so two NoExecute
        // tolerations differing only in tolerationSeconds are duplicates.
        let mut a = tol("k1", "Equal", "v1", "NoExecute");
        a.toleration_seconds = Some(10);
        let mut b = tol("k1", "Equal", "v1", "NoExecute");
        b.toleration_seconds = Some(20);
        let scheduling = Scheduling {
            node_selector: None,
            tolerations: Some(vec![a, b]),
        };
        let rc = RuntimeClass::new("rc", "runc").with_scheduling(scheduling);
        let errs = validate_runtime_class(&rc);
        assert!(
            errs.iter().any(|e| e.error_type == ErrorType::Duplicate),
            "expected duplicate error ignoring tolerationSeconds, got {errs:?}"
        );
    }
}
