//! LimitRange validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateLimitRange` (release-1.35).
//!
//! Scope: limit `type` validity + uniqueness, the Pod-type default/defaultRequest
//! ban, the PVC-type min/max-storage requirement, the per-resource
//! min ≤ defaultRequest ≤ default ≤ max ordering, `maxLimitRequestRatio` ≥ 1 and
//! its `ratio ≤ max/min` ceiling, plus the non-overcommittable-resource
//! `default == defaultRequest` constraint.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::quantity::Quantity;
use crate::resources::policy::{LimitRange, LimitRangeItem};
use crate::validation::field::{Error, ErrorList, Path};

/// Parse a `name -> quantity-string` map into `name -> (raw, Quantity)`,
/// skipping entries whose quantity doesn't parse (those are rejected at
/// deserialization).
fn parse_quantities(m: &Option<HashMap<String, String>>) -> HashMap<String, (String, Quantity)> {
    let mut out = HashMap::new();
    if let Some(map) = m {
        for (k, v) in map {
            if let Ok(q) = Quantity::parse(v) {
                out.insert(k.clone(), (v.clone(), q));
            }
        }
    }
    out
}

/// Upstream `resource.MaxMilliValue = ((1 << 63) - 1) / 1000`. The
/// `maxLimitRequestRatio` ceiling switches to milli-precision arithmetic only
/// when the ratio and both bounds stay strictly below this.
const MAX_MILLI_VALUE: i128 = ((1i128 << 63) - 1) / 1000;

/// Port of `apis/core/helper.IsNativeResource`: a name is native when it has no
/// `/` separator (implicitly `kubernetes.io/`) or is explicitly prefixed with
/// `kubernetes.io/`.
fn is_native_resource(name: &str) -> bool {
    !name.contains('/') || name.contains("kubernetes.io/")
}

/// Port of `apis/core/helper.IsOvercommitAllowed`: native and not a hugepage
/// resource. For these, `default` must equal `defaultRequest` when both are set.
fn is_overcommit_allowed(name: &str) -> bool {
    is_native_resource(name) && !name.starts_with("hugepages-")
}

fn validate_item(item: &LimitRangeItem, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // type must be one of the known limit types.
    if !matches!(
        item.item_type.as_str(),
        "Pod" | "Container" | "PersistentVolumeClaim"
    ) {
        errs.push(Error::not_supported(
            &fld_path.child("type"),
            item.item_type.clone(),
            &["Pod", "Container", "PersistentVolumeClaim"],
        ));
    }

    let min = parse_quantities(&item.min);
    let max = parse_quantities(&item.max);

    // Pod limits may not carry defaults; Container may.
    if item.item_type == "Pod" {
        if item.default.as_ref().is_some_and(|m| !m.is_empty()) {
            errs.push(Error::forbidden(
                &fld_path.child("default"),
                "may not be specified when `type` is 'Pod'",
            ));
        }
        if item.default_request.as_ref().is_some_and(|m| !m.is_empty()) {
            errs.push(Error::forbidden(
                &fld_path.child("defaultRequest"),
                "may not be specified when `type` is 'Pod'",
            ));
        }
    }

    // PVC limits require at least one of min/max storage.
    if item.item_type == "PersistentVolumeClaim"
        && !min.contains_key("storage")
        && !max.contains_key("storage")
    {
        errs.push(Error::required(
            &fld_path.child("limits"),
            "either minimum or maximum storage value is required, but neither was provided",
        ));
    }

    let defaults = parse_quantities(&item.default);
    let default_requests = parse_quantities(&item.default_request);
    let ratios = parse_quantities(&item.max_limit_request_ratio);

    let mut keys: HashSet<&String> = HashSet::new();
    keys.extend(min.keys());
    keys.extend(max.keys());
    keys.extend(defaults.keys());
    keys.extend(default_requests.keys());
    keys.extend(ratios.keys());

    let gt = |a: &Quantity, b: &Quantity| a.cmp_value(b) == Ordering::Greater;

    for k in keys {
        let mn = min.get(k);
        let mx = max.get(k);
        let df = defaults.get(k);
        let dr = default_requests.get(k);
        let ratio = ratios.get(k);

        if let (Some(mn), Some(mx)) = (mn, mx) {
            if gt(&mn.1, &mx.1) {
                errs.push(Error::invalid(
                    &fld_path.child("min").key(k),
                    mn.0.clone(),
                    format!("min value {} is greater than max value {}", mn.0, mx.0),
                ));
            }
        }
        if let (Some(dr), Some(mn)) = (dr, mn) {
            if gt(&mn.1, &dr.1) {
                errs.push(Error::invalid(
                    &fld_path.child("defaultRequest").key(k),
                    dr.0.clone(),
                    format!(
                        "min value {} is greater than default request value {}",
                        mn.0, dr.0
                    ),
                ));
            }
        }
        if let (Some(dr), Some(mx)) = (dr, mx) {
            if gt(&dr.1, &mx.1) {
                errs.push(Error::invalid(
                    &fld_path.child("defaultRequest").key(k),
                    dr.0.clone(),
                    format!(
                        "default request value {} is greater than max value {}",
                        dr.0, mx.0
                    ),
                ));
            }
        }
        if let (Some(dr), Some(df)) = (dr, df) {
            if gt(&dr.1, &df.1) {
                errs.push(Error::invalid(
                    &fld_path.child("defaultRequest").key(k),
                    dr.0.clone(),
                    format!(
                        "default request value {} is greater than default limit value {}",
                        dr.0, df.0
                    ),
                ));
            }
        }
        if let (Some(df), Some(mn)) = (df, mn) {
            if gt(&mn.1, &df.1) {
                errs.push(Error::invalid(
                    &fld_path.child("default").key(k),
                    mn.0.clone(),
                    format!("min value {} is greater than default value {}", mn.0, df.0),
                ));
            }
        }
        if let (Some(df), Some(mx)) = (df, mx) {
            if gt(&df.1, &mx.1) {
                errs.push(Error::invalid(
                    &fld_path.child("default").key(k),
                    mx.0.clone(),
                    format!("default value {} is greater than max value {}", df.0, mx.0),
                ));
            }
        }
        if let Some(ratio) = ratio {
            if ratio.1.cmp_value(&Quantity::parse("1").unwrap()) == Ordering::Less {
                errs.push(Error::invalid(
                    &fld_path.child("maxLimitRequestRatio").key(k),
                    ratio.0.clone(),
                    format!("ratio {} is less than 1", ratio.0),
                ));
            }
            // ratio ≤ max/min ceiling. Mirrors upstream: use integer values,
            // but drop to milli-precision when ratio and both bounds stay below
            // MaxMilliValue (so e.g. cpu "100m"/"200m" compares correctly).
            if let (Some(mn), Some(mx)) = (mn, mx) {
                let mut max_ratio_value = ratio.1.value() as f64;
                let mut min_value = mn.1.value();
                let mut max_value = mx.1.value();
                if ratio.1.value() < MAX_MILLI_VALUE
                    && min_value < MAX_MILLI_VALUE
                    && max_value < MAX_MILLI_VALUE
                {
                    max_ratio_value = ratio.1.milli_value() as f64 / 1000.0;
                    min_value = mn.1.milli_value();
                    max_value = mx.1.milli_value();
                }
                if min_value != 0 {
                    let max_ratio_limit = max_value as f64 / min_value as f64;
                    if max_ratio_value > max_ratio_limit {
                        errs.push(Error::invalid(
                            &fld_path.child("maxLimitRequestRatio").key(k),
                            ratio.0.clone(),
                            format!(
                                "ratio {} is greater than max/min = {max_ratio_limit:.6}",
                                ratio.0
                            ),
                        ));
                    }
                }
            }
        }

        // For GPU, hugepages and other non-overcommittable resources, default
        // and defaultRequest must match when both are specified.
        if !is_overcommit_allowed(k) {
            if let (Some(df), Some(dr)) = (df, dr) {
                if df.1.cmp_value(&dr.1) != Ordering::Equal {
                    errs.push(Error::invalid(
                        &fld_path.child("defaultRequest").key(k),
                        dr.0.clone(),
                        format!(
                            "default value {} must equal to defaultRequest value {} in {k}",
                            df.0, dr.0
                        ),
                    ));
                }
            }
        }
    }

    errs
}

/// Validate a `LimitRange`. Mirrors upstream `ValidateLimitRange`.
pub fn validate_limit_range(lr: &LimitRange) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let fld_path = Path::new("spec").child("limits");
    let mut seen_types: HashSet<&str> = HashSet::new();
    for (i, item) in lr.spec.limits.iter().enumerate() {
        let idx_path = fld_path.index(i);
        if !seen_types.insert(item.item_type.as_str()) {
            errs.push(Error::duplicate(
                &idx_path.child("type"),
                item.item_type.clone(),
            ));
        }
        errs.extend(validate_item(item, &idx_path));
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::policy::{LimitRange, LimitRangeItem, LimitRangeSpec};

    fn map(pairs: &[(&str, &str)]) -> Option<HashMap<String, String>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn item(item_type: &str) -> LimitRangeItem {
        LimitRangeItem {
            item_type: item_type.to_string(),
            max: None,
            min: None,
            default: None,
            default_request: None,
            max_limit_request_ratio: None,
        }
    }

    fn lr(items: Vec<LimitRangeItem>) -> LimitRange {
        LimitRange::new("lr", "default", LimitRangeSpec { limits: items })
    }

    /// Assert that some error's field-path ends with `suffix` and detail
    /// contains `needle`.
    fn has_err(errs: &ErrorList, suffix: &str, needle: &str) -> bool {
        errs.iter()
            .any(|e| e.field.ends_with(suffix) && e.detail.contains(needle))
    }

    #[test]
    fn limit_range_valid_container_ordering_passes() {
        let mut it = item("Container");
        it.min = map(&[("cpu", "100m")]);
        it.default_request = map(&[("cpu", "200m")]);
        it.default = map(&[("cpu", "300m")]);
        it.max = map(&[("cpu", "400m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn limit_range_unknown_type_rejected() {
        let errs = validate_limit_range(&lr(vec![item("Bogus")]));
        assert!(has_err(&errs, "type", "supported"));
    }

    #[test]
    fn limit_range_duplicate_type_rejected() {
        use crate::validation::field::ErrorType;
        let errs = validate_limit_range(&lr(vec![item("Container"), item("Container")]));
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("type") && e.error_type == ErrorType::Duplicate),
            "got {errs:?}"
        );
    }

    #[test]
    fn limit_range_min_greater_than_max_rejected() {
        let mut it = item("Container");
        it.min = map(&[("cpu", "500m")]);
        it.max = map(&[("cpu", "200m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(
            &errs,
            "min[cpu]",
            "min value 500m is greater than max value 200m"
        ));
    }

    #[test]
    fn limit_range_ratio_less_than_one_rejected() {
        let mut it = item("Container");
        it.max_limit_request_ratio = map(&[("cpu", "0.5")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(
            &errs,
            "maxLimitRequestRatio[cpu]",
            "is less than 1"
        ));
    }

    #[test]
    fn limit_range_ratio_above_max_min_rejected() {
        // max/min = 400/100 = 4, ratio 5 exceeds it.
        let mut it = item("Container");
        it.min = map(&[("cpu", "100m")]);
        it.max = map(&[("cpu", "400m")]);
        it.max_limit_request_ratio = map(&[("cpu", "5")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(
            has_err(
                &errs,
                "maxLimitRequestRatio[cpu]",
                "is greater than max/min"
            ),
            "got {errs:?}"
        );
    }

    #[test]
    fn limit_range_ratio_within_max_min_passes() {
        // max/min = 400/100 = 4, ratio 4 is allowed (not strictly greater).
        let mut it = item("Container");
        it.min = map(&[("cpu", "100m")]);
        it.max = map(&[("cpu", "400m")]);
        it.max_limit_request_ratio = map(&[("cpu", "4")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn limit_range_pvc_without_storage_rejected() {
        let mut it = item("PersistentVolumeClaim");
        it.min = map(&[("cpu", "1")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(
            &errs,
            "limits",
            "either minimum or maximum storage value is required"
        ));
    }

    #[test]
    fn limit_range_pvc_with_storage_passes() {
        let mut it = item("PersistentVolumeClaim");
        it.max = map(&[("storage", "10Gi")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn limit_range_pod_with_default_rejected() {
        let mut it = item("Pod");
        it.default = map(&[("cpu", "100m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(&errs, "default", "may not be specified"));
    }

    #[test]
    fn limit_range_pod_with_default_request_rejected() {
        let mut it = item("Pod");
        it.default_request = map(&[("cpu", "100m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(has_err(&errs, "defaultRequest", "may not be specified"));
    }

    #[test]
    fn limit_range_non_overcommit_default_mismatch_rejected() {
        // hugepages-2Mi is non-overcommittable: default must equal defaultRequest.
        let mut it = item("Container");
        it.default = map(&[("hugepages-2Mi", "4Mi")]);
        it.default_request = map(&[("hugepages-2Mi", "2Mi")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(
            has_err(&errs, "defaultRequest[hugepages-2Mi]", "must equal to"),
            "got {errs:?}"
        );
    }

    #[test]
    fn limit_range_non_overcommit_default_equal_passes() {
        let mut it = item("Container");
        it.default = map(&[("hugepages-2Mi", "2Mi")]);
        it.default_request = map(&[("hugepages-2Mi", "2Mi")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn limit_range_overcommit_resource_default_mismatch_allowed() {
        // cpu is overcommittable: default != defaultRequest is fine (as long as
        // ordering holds).
        let mut it = item("Container");
        it.default_request = map(&[("cpu", "100m")]);
        it.default = map(&[("cpu", "200m")]);
        let errs = validate_limit_range(&lr(vec![it]));
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }
}
