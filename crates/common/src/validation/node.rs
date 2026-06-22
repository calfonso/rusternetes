//! Node validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateNode` (release-1.35).
//!
//! Covers `spec.taints` (`validateNodeTaints`) and `spec.podCIDRs` (valid CIDRs
//! plus dual-stack one-per-family). ObjectMeta is validated separately by the
//! handler (see #1087 / #1277); node status, resources and swap are out of scope.

use crate::resources::node::{ConfigMapNodeConfigSource, Node, NodeConfigSource, NodeConfigStatus};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_valid_label_value, validate_label_name,
};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashSet;
use std::net::IpAddr;

const TAINT_EFFECTS: [&str; 3] = ["NoSchedule", "PreferNoSchedule", "NoExecute"];

/// Upstream `validation.DNS1123SubdomainMaxLength`.
const DNS1123_SUBDOMAIN_MAX_LENGTH: usize = 253;

/// Upstream `nodeDeclaredFeatureRegexp`: UpperCamelCase segments separated by '/'.
static NODE_DECLARED_FEATURE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[A-Z][a-zA-Z0-9]*(/[a-zA-Z][a-zA-Z0-9]*)*$").expect("feature regex")
});

#[derive(PartialEq, Eq, Clone, Copy)]
enum IpFamily {
    V4,
    V6,
}

fn parse_cidr(cidr: &str) -> Option<IpFamily> {
    let (ip, prefix) = cidr.split_once('/')?;
    let prefix: u8 = prefix.parse().ok()?;
    match ip.parse::<IpAddr>().ok()? {
        IpAddr::V4(_) if prefix <= 32 => Some(IpFamily::V4),
        IpAddr::V6(_) if prefix <= 128 => Some(IpFamily::V6),
        _ => None,
    }
}

/// Validate a `Node` on create. Mirrors upstream `ValidateNode`'s taints +
/// podCIDRs checks (minus ObjectMeta / status / resources).
pub fn validate_node(node: &Node) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(spec) = &node.spec else {
        return errs;
    };

    // spec.taints — key (label name), value (label value), effect enum, unique
    // by (key, effect).
    if let Some(taints) = &spec.taints {
        let taints_path = Path::new("spec").child("taints");
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for (i, taint) in taints.iter().enumerate() {
            let tp = taints_path.index(i);
            errs.extend(validate_label_name(&taint.key, &tp.child("key")));
            let value = taint.value.as_deref().unwrap_or("");
            for msg in is_valid_label_value(value) {
                errs.push(Error::invalid(&tp.child("value"), value.to_string(), msg));
            }
            if !TAINT_EFFECTS.contains(&taint.effect.as_str()) {
                errs.push(Error::not_supported(
                    &tp.child("effect"),
                    taint.effect.clone(),
                    &TAINT_EFFECTS,
                ));
            }
            if !seen.insert((taint.key.clone(), taint.effect.clone())) {
                let mut e = Error::duplicate(&tp, taint.key.clone());
                e.detail = "taints must be unique by key and effect pair".to_string();
                errs.push(e);
            }
        }
    }

    // spec.podCIDRs — each a valid CIDR; >1 must be a dual-stack pair.
    if let Some(cidrs) = &spec.pod_cidrs {
        if !cidrs.is_empty() {
            let cidrs_path = Path::new("spec").child("podCIDRs");
            let mut families: Vec<Option<IpFamily>> = Vec::with_capacity(cidrs.len());
            for (i, c) in cidrs.iter().enumerate() {
                let fam = parse_cidr(c);
                if fam.is_none() {
                    errs.push(Error::invalid(
                        &cidrs_path.index(i),
                        c.clone(),
                        "must be a valid CIDR value, (e.g. 10.9.8.0/24 or 2001:db8::/64)",
                    ));
                }
                families.push(fam);
            }
            if cidrs.len() > 1 {
                let dual_stack = cidrs.len() == 2
                    && matches!((families[0], families[1]), (Some(a), Some(b)) if a != b);
                if !dual_stack {
                    errs.push(Error::invalid(
                        &cidrs_path,
                        cidrs.join(","),
                        "may specify no more than one CIDR for each IP family",
                    ));
                }
            }
        }
    }

    errs
}

/// Validate a Node update — the spec-immutability subset of upstream
/// `ValidateNodeUpdate` (pkg/apis/core/validation): `podCIDRs` and `providerID`
/// are immutable once set (they may only go from empty to a value, as the
/// controller-manager assigns them). `unschedulable`, `taints` and
/// `configSource` remain mutable. Status-field checks (address dedup,
/// declaredFeatures, config) are not covered here.
pub fn validate_node_update(new_node: &Node, old_node: &Node) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let spec = Path::new("spec");

    let empty: Vec<String> = Vec::new();
    let old_cidrs = old_node
        .spec
        .as_ref()
        .and_then(|s| s.pod_cidrs.as_ref())
        .unwrap_or(&empty);
    let new_cidrs = new_node
        .spec
        .as_ref()
        .and_then(|s| s.pod_cidrs.as_ref())
        .unwrap_or(&empty);
    if !old_cidrs.is_empty() && old_cidrs != new_cidrs {
        errs.push(Error::forbidden(
            &spec.child("podCIDRs"),
            "node updates may not change podCIDR except from \"\" to valid",
        ));
    }

    let old_pid = old_node
        .spec
        .as_ref()
        .and_then(|s| s.provider_id.as_deref())
        .unwrap_or("");
    let new_pid = new_node
        .spec
        .as_ref()
        .and_then(|s| s.provider_id.as_deref())
        .unwrap_or("");
    if !old_pid.is_empty() && old_pid != new_pid {
        errs.push(Error::forbidden(
            &spec.child("providerID"),
            "node updates may not change providerID except from \"\" to valid",
        ));
    }

    errs
}

/// Port of upstream `validateNodeDeclaredFeatureName`.
fn validate_node_declared_feature_name(name: &str) -> Option<String> {
    if name.len() > DNS1123_SUBDOMAIN_MAX_LENGTH {
        return Some(format!(
            "invalid feature name {name:?}: must be no more than {DNS1123_SUBDOMAIN_MAX_LENGTH} characters"
        ));
    }
    if !NODE_DECLARED_FEATURE_REGEX.is_match(name) {
        return Some(format!(
            "invalid feature name {name:?}: must start with an UpperCamelCase segment, with subsequent segments separated by '/' (e.g., MyFeature or MyFeature/mySubFeature), and contain only alphanumeric characters and slashes"
        ));
    }
    None
}

/// Port of upstream `validateNodeDeclaredFeatures`: each name valid, list
/// sorted alphabetically with no adjacent duplicates.
fn validate_node_declared_features(features: &[String], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (i, feature) in features.iter().enumerate() {
        if let Some(msg) = validate_node_declared_feature_name(feature) {
            errs.push(Error::invalid(&fld_path.index(i), feature.clone(), msg));
        }
        if i + 1 < features.len() {
            let next = &features[i + 1];
            if feature == next {
                errs.push(Error::duplicate(&fld_path.index(i + 1), next.clone()));
            } else if feature.as_str() > next.as_str() {
                errs.push(Error::invalid(
                    &fld_path.index(i + 1),
                    next.clone(),
                    "list must be sorted alphabetically".to_string(),
                ));
            }
        }
    }
    errs
}

/// Validate `status.capacity` / `status.allocatable` quantities — port of
/// upstream `ValidateNodeResources` (non-negative; unparseable rejected).
fn validate_node_resources(
    map: Option<&std::collections::HashMap<String, String>>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(map) = map else { return errs };
    for (name, value) in map {
        let res_path = fld_path.child(name);
        match crate::quantity::Quantity::parse(value) {
            Ok(q) => {
                if q.is_negative() {
                    errs.push(Error::invalid(
                        &res_path,
                        value.clone(),
                        "must be greater than or equal to 0".to_string(),
                    ));
                }
            }
            Err(_) => {
                errs.push(Error::invalid(
                    &res_path,
                    value.clone(),
                    "must be a valid resource quantity".to_string(),
                ));
            }
        }
    }
    errs
}

/// Validate the status-subresource fields of a `Node` update. Mirrors the
/// status-field portion of upstream `ValidateNodeUpdate`:
///
/// - `status.addresses` — no duplicate `NodeAddress` (type+address) entries.
/// - `status.declaredFeatures` — `validateNodeDeclaredFeatures` (name format,
///   sorted, no adjacent duplicates).
/// - `status.capacity` / `status.allocatable` — `ValidateNodeResources`
///   (non-negative quantities).
///
/// - `status.config` — `validateNodeConfigStatus` (the assigned/active/
///   lastKnownGood ConfigMap references). This targets the removed
///   DynamicKubeletConfig feature and is never populated by a current kubelet,
///   but is ported for parity with upstream `ValidateNodeUpdate`.
///
/// These checks compare only against the new object (upstream's status portion
/// does not consult the old object).
pub fn validate_node_status_update(new_node: &Node) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(status) = new_node.status.as_ref() else {
        return errs;
    };
    let status_path = Path::new("status");

    if let Some(features) = &status.declared_features {
        errs.extend(validate_node_declared_features(
            features,
            &status_path.child("declaredFeatures"),
        ));
    }

    if let Some(addresses) = &status.addresses {
        let addr_path = status_path.child("addresses");
        let mut seen: HashSet<(&str, &str)> = HashSet::new();
        for (i, addr) in addresses.iter().enumerate() {
            if !seen.insert((addr.address_type.as_str(), addr.address.as_str())) {
                errs.push(Error::duplicate(
                    &addr_path.index(i),
                    format!("{}/{}", addr.address_type, addr.address),
                ));
            }
        }
    }

    errs.extend(validate_node_resources(
        status.capacity.as_ref(),
        &status_path.child("capacity"),
    ));
    errs.extend(validate_node_resources(
        status.allocatable.as_ref(),
        &status_path.child("allocatable"),
    ));

    if let Some(config) = &status.config {
        errs.extend(validate_node_config_status(
            config,
            &status_path.child("config"),
        ));
    }

    errs
}

/// Port of upstream `IsConfigMapKey`: ≤253 chars, `[-._a-zA-Z0-9]+`, not `.`/`..`.
fn is_config_map_key(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > 253 {
        errs.push("must be no more than 253 characters".to_string());
    }
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
    {
        errs.push(
            "a valid config key must consist of alphanumeric characters, '-', '_' or '.'"
                .to_string(),
        );
    }
    if value == "." || value == ".." {
        errs.push("must not be '.' or '..'".to_string());
    }
    errs
}

/// Port of upstream `validateConfigMapNodeConfigSource`: target ConfigMap
/// namespace (DNS-1123 label), name (DNS-1123 subdomain), and `kubeletConfigKey`
/// (a valid ConfigMap key) are all required and well-formed.
fn validate_config_map_node_config_source(
    source: &ConfigMapNodeConfigSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if source.namespace.is_empty() {
        errs.push(Error::required(&fld_path.child("namespace"), ""));
    } else {
        for msg in is_dns1123_label(&source.namespace) {
            errs.push(Error::invalid(
                &fld_path.child("namespace"),
                source.namespace.clone(),
                msg,
            ));
        }
    }
    if source.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    } else {
        for msg in is_dns1123_subdomain(&source.name) {
            errs.push(Error::invalid(
                &fld_path.child("name"),
                source.name.clone(),
                msg,
            ));
        }
    }
    let key = source.kubelet_config_key.as_deref().unwrap_or("");
    if key.is_empty() {
        errs.push(Error::required(&fld_path.child("kubeletConfigKey"), ""));
    } else {
        for msg in is_config_map_key(key) {
            errs.push(Error::invalid(
                &fld_path.child("kubeletConfigKey"),
                key.to_string(),
                msg,
            ));
        }
    }
    errs
}

/// Port of upstream `validateConfigMapNodeConfigSourceStatus`: a status source
/// additionally requires `uid` and `resourceVersion`.
fn validate_config_map_node_config_source_status(
    source: &ConfigMapNodeConfigSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if source.uid.as_deref().unwrap_or("").is_empty() {
        errs.push(Error::required(&fld_path.child("uid"), ""));
    }
    if source.resource_version.as_deref().unwrap_or("").is_empty() {
        errs.push(Error::required(&fld_path.child("resourceVersion"), ""));
    }
    errs.extend(validate_config_map_node_config_source(source, fld_path));
    errs
}

/// Port of upstream `validateNodeConfigSourceStatus`: exactly one reference
/// subfield (currently only `configMap`) must be set.
fn validate_node_config_source_status(source: &NodeConfigSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut count = 0;
    if let Some(cm) = &source.config_map {
        count += 1;
        errs.extend(validate_config_map_node_config_source_status(
            cm,
            &fld_path.child("configMap"),
        ));
    }
    if count != 1 {
        errs.push(Error::invalid(
            fld_path,
            "<configSource>".to_string(),
            "exactly one reference subfield must be non-nil",
        ));
    }
    errs
}

/// Port of upstream `validateNodeConfigStatus`: validate the assigned / active /
/// lastKnownGood config sources when set.
fn validate_node_config_status(config: &NodeConfigStatus, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(assigned) = &config.assigned {
        errs.extend(validate_node_config_source_status(
            assigned,
            &fld_path.child("assigned"),
        ));
    }
    if let Some(active) = &config.active {
        errs.extend(validate_node_config_source_status(
            active,
            &fld_path.child("active"),
        ));
    }
    if let Some(lkg) = &config.last_known_good {
        errs.extend(validate_node_config_source_status(
            lkg,
            &fld_path.child("lastKnownGood"),
        ));
    }
    errs
}

#[cfg(test)]
mod status_update_tests {
    use super::*;

    fn node_with_status(status: serde_json::Value) -> Node {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "node-1"},
            "status": status,
        }))
        .expect("node decodes")
    }

    #[test]
    fn clean_status_passes() {
        let node = node_with_status(serde_json::json!({
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.1"},
                {"type": "Hostname", "address": "node-1"}
            ],
            "capacity": {"cpu": "4", "memory": "8Gi"},
            "allocatable": {"cpu": "4", "memory": "8Gi"},
            "declaredFeatures": ["GuaranteedQoSPodCPUResize"]
        }));
        assert!(validate_node_status_update(&node).is_empty());
    }

    #[test]
    fn duplicate_address_is_rejected() {
        let node = node_with_status(serde_json::json!({
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.1"},
                {"type": "InternalIP", "address": "10.0.0.1"}
            ]
        }));
        let errs = validate_node_status_update(&node);
        assert!(
            errs.iter().any(|e| e.field.contains("addresses")),
            "{errs:?}"
        );
    }

    #[test]
    fn negative_capacity_is_rejected() {
        let node = node_with_status(serde_json::json!({
            "capacity": {"cpu": "-1"}
        }));
        let errs = validate_node_status_update(&node);
        assert!(
            errs.iter()
                .any(|e| e.field.contains("capacity") && e.detail.contains("greater than or equal")),
            "{errs:?}"
        );
    }

    #[test]
    fn unsorted_declared_features_rejected() {
        let node = node_with_status(serde_json::json!({
            "declaredFeatures": ["ZebraFeature", "AlphaFeature"]
        }));
        let errs = validate_node_status_update(&node);
        assert!(
            errs.iter()
                .any(|e| e.detail.contains("sorted alphabetically")),
            "{errs:?}"
        );
    }

    #[test]
    fn bad_declared_feature_name_rejected() {
        let node = node_with_status(serde_json::json!({
            "declaredFeatures": ["lowercaseStart"]
        }));
        let errs = validate_node_status_update(&node);
        assert!(
            errs.iter().any(|e| e.detail.contains("UpperCamelCase")),
            "{errs:?}"
        );
    }

    #[test]
    fn empty_status_passes() {
        let node = node_with_status(serde_json::json!({}));
        assert!(validate_node_status_update(&node).is_empty());
    }

    #[test]
    fn config_status_valid_assigned_passes() {
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {"configMap": {
                "namespace": "kube-system", "name": "my-config",
                "uid": "abc-123", "resourceVersion": "42", "kubeletConfigKey": "kubelet"
            }}}
        }));
        assert!(
            validate_node_status_update(&node).is_empty(),
            "{:?}",
            validate_node_status_update(&node)
        );
    }

    #[test]
    fn config_status_requires_uid_and_resource_version() {
        let node = node_with_status(serde_json::json!({
            "config": {"active": {"configMap": {
                "namespace": "kube-system", "name": "my-config", "kubeletConfigKey": "kubelet"
            }}}
        }));
        let errs = validate_node_status_update(&node);
        assert!(errs.iter().any(|e| e.field.ends_with("uid")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field.ends_with("resourceVersion")),
            "{errs:?}"
        );
    }

    #[test]
    fn config_status_empty_source_rejected() {
        // a NodeConfigSource with no subfield set -> "exactly one reference subfield"
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {}}
        }));
        let errs = validate_node_status_update(&node);
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("exactly one reference subfield")),
            "{errs:?}"
        );
    }

    #[test]
    fn config_status_bad_namespace_and_key_rejected() {
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {"configMap": {
                "namespace": "Bad_NS", "name": "cfg", "uid": "u", "resourceVersion": "1",
                "kubeletConfigKey": "bad key!"
            }}}
        }));
        let errs = validate_node_status_update(&node);
        assert!(
            errs.iter().any(|e| e.field.ends_with("namespace")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e.field.ends_with("kubeletConfigKey")),
            "{errs:?}"
        );
    }
}
