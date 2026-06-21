//! LimitRange validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateLimitRange` (release-1.35).
//!
//! Scope: limit `type` validity + uniqueness, the Pod-type default/defaultRequest
//! ban, the PVC-type min/max-storage requirement, and the per-resource
//! min ≤ defaultRequest ≤ default ≤ max ordering plus `maxLimitRequestRatio` ≥ 1.
//! The max/min-ratio ceiling and the non-overcommit default==defaultRequest
//! check are left as a follow-up.

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
                    &fld_path.child("min").child(k),
                    mn.0.clone(),
                    format!("min value {} is greater than max value {}", mn.0, mx.0),
                ));
            }
        }
        if let (Some(dr), Some(mn)) = (dr, mn) {
            if gt(&mn.1, &dr.1) {
                errs.push(Error::invalid(
                    &fld_path.child("defaultRequest").child(k),
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
                    &fld_path.child("defaultRequest").child(k),
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
                    &fld_path.child("defaultRequest").child(k),
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
                    &fld_path.child("default").child(k),
                    mn.0.clone(),
                    format!("min value {} is greater than default value {}", mn.0, df.0),
                ));
            }
        }
        if let (Some(df), Some(mx)) = (df, mx) {
            if gt(&df.1, &mx.1) {
                errs.push(Error::invalid(
                    &fld_path.child("default").child(k),
                    mx.0.clone(),
                    format!("default value {} is greater than max value {}", df.0, mx.0),
                ));
            }
        }
        if let Some(ratio) = ratio {
            if ratio.1.cmp_value(&Quantity::parse("1").unwrap()) == Ordering::Less {
                errs.push(Error::invalid(
                    &fld_path.child("maxLimitRequestRatio").child(k),
                    ratio.0.clone(),
                    format!("ratio {} is less than 1", ratio.0),
                ));
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
