//! ResourceSlice (resource.k8s.io / DRA) validation — port of upstream
//! Kubernetes `pkg/apis/resource/validation/validation.go::ValidateResourceSlice`
//! (release-1.35).
//!
//! Scope (create path): `driver` (DNS-1123 subdomain), `pool`
//! (name segments + generation/resourceSliceCount bounds), the exactly-one
//! node-selection rule (`nodeName`/`nodeSelector`/`allNodes`/
//! `perDeviceNodeSelection`), and the `devices` set (size cap + unique
//! non-empty names). Deep `nodeSelector` term validation and per-device field
//! validation are tracked in #1442. ObjectMeta is validated separately.

use crate::resources::{ResourceSlice, ResourceSliceSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;
use std::collections::HashSet;

const POOL_NAME_MAX_LENGTH: usize = 253;
const RESOURCE_SLICE_MAX_DEVICES: usize = 128;
const RESOURCE_SLICE_MAX_DEVICES_WITH_COUNTERS: usize = 64;

/// Validate a `ResourceSlice` on create. Mirrors upstream `ValidateResourceSlice`
/// (minus ObjectMeta and the deeper nodeSelector/per-device checks).
pub fn validate_resource_slice(slice: &ResourceSlice) -> ErrorList {
    validate_resource_slice_spec(&slice.spec, &Path::new("spec"))
}

/// Validate a `ResourceSlice` on update. Mirrors upstream
/// `ValidateResourceSliceUpdate`: re-run the create validation, then enforce
/// that `pool.name`, `driver`, and `nodeName` are immutable.
pub fn validate_resource_slice_update(new: &ResourceSlice, old: &ResourceSlice) -> ErrorList {
    let mut errs = validate_resource_slice(new);
    let spec = Path::new("spec");
    if new.spec.pool.name != old.spec.pool.name {
        errs.push(Error::invalid(
            &spec.child("pool").child("name"),
            new.spec.pool.name.clone(),
            "field is immutable",
        ));
    }
    if new.spec.driver != old.spec.driver {
        errs.push(Error::invalid(
            &spec.child("driver"),
            new.spec.driver.clone(),
            "field is immutable",
        ));
    }
    if new.spec.node_name != old.spec.node_name {
        errs.push(Error::invalid(
            &spec.child("nodeName"),
            new.spec.node_name.clone().unwrap_or_default(),
            "field is immutable",
        ));
    }
    errs
}

fn validate_resource_slice_spec(spec: &ResourceSliceSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // driver — required, DNS-1123 subdomain.
    let driver_path = fld_path.child("driver");
    if spec.driver.is_empty() {
        errs.push(Error::required(&driver_path, ""));
    } else {
        for msg in is_dns1123_subdomain(&spec.driver) {
            errs.push(Error::invalid(&driver_path, spec.driver.clone(), msg));
        }
    }

    // pool.
    let pool_path = fld_path.child("pool");
    let name_path = pool_path.child("name");
    if spec.pool.name.is_empty() {
        errs.push(Error::required(&name_path, ""));
    } else {
        if spec.pool.name.len() > POOL_NAME_MAX_LENGTH {
            errs.push(Error::too_long(&name_path, POOL_NAME_MAX_LENGTH));
        }
        for part in spec.pool.name.split('/') {
            for msg in is_dns1123_subdomain(part) {
                errs.push(Error::invalid(&name_path, spec.pool.name.clone(), msg));
            }
        }
    }
    if spec.pool.resource_slice_count <= 0 {
        errs.push(Error::invalid(
            &pool_path.child("resourceSliceCount"),
            spec.pool.resource_slice_count,
            "must be greater than zero",
        ));
    }
    if spec.pool.generation < 0 {
        errs.push(Error::invalid(
            &pool_path.child("generation"),
            spec.pool.generation,
            "must be greater than or equal to zero",
        ));
    }

    // Exactly one of nodeName / nodeSelector / allNodes / perDeviceNodeSelection.
    let mut set_fields: Vec<&str> = Vec::new();
    if let Some(node_name) = &spec.node_name {
        if node_name.is_empty() {
            errs.push(Error::invalid(
                &fld_path.child("nodeName"),
                String::new(),
                "must be either unset or set to a non-empty string",
            ));
        } else {
            set_fields.push("`nodeName`");
            for msg in is_dns1123_subdomain(node_name) {
                errs.push(Error::invalid(
                    &fld_path.child("nodeName"),
                    node_name.clone(),
                    msg,
                ));
            }
        }
    }
    if spec.node_selector.is_some() {
        set_fields.push("`nodeSelector`");
    }
    if let Some(all_nodes) = spec.all_nodes {
        if all_nodes {
            set_fields.push("`allNodes`");
        } else {
            errs.push(Error::invalid(
                &fld_path.child("allNodes"),
                false,
                "must be either unset or set to true",
            ));
        }
    }
    if let Some(per_device) = spec.per_device_node_selection {
        if per_device {
            set_fields.push("`perDeviceNodeSelection`");
        } else {
            errs.push(Error::invalid(
                &fld_path.child("perDeviceNodeSelection"),
                false,
                "must be either unset or set to true",
            ));
        }
    }
    if set_fields.is_empty() {
        errs.push(Error::required(
            fld_path,
            "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required",
        ));
    } else if set_fields.len() > 1 {
        errs.push(Error::invalid(
            fld_path,
            format!("{{{}}}", set_fields.join(", ")),
            "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required, but multiple fields are set",
        ));
    }

    // devices: size cap + unique non-empty names.
    let devices_path = fld_path.child("devices");
    let has_counters = spec.devices.iter().any(|d| !d.consumes_counters.is_empty());
    let max_devices = if has_counters {
        RESOURCE_SLICE_MAX_DEVICES_WITH_COUNTERS
    } else {
        RESOURCE_SLICE_MAX_DEVICES
    };
    if spec.devices.len() > max_devices {
        errs.push(Error::too_many(&devices_path, max_devices));
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, device) in spec.devices.iter().enumerate() {
        let dp = devices_path.index(i);
        if device.name.is_empty() {
            errs.push(Error::required(&dp.child("name"), ""));
        } else if !seen.insert(device.name.as_str()) {
            errs.push(Error::duplicate(&dp.child("name"), device.name.clone()));
        }
    }

    errs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slice(json: serde_json::Value) -> ResourceSlice {
        serde_json::from_value(json).unwrap()
    }

    fn base() -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "slice-1"},
            "spec": {
                "driver": "dra.example.com",
                "pool": {"name": "pool-1", "generation": 1, "resourceSliceCount": 1},
                "allNodes": true,
                "devices": [{"name": "gpu-0"}]
            }
        })
    }

    fn errs(spec: serde_json::Value) -> Vec<String> {
        let mut v = base();
        v["spec"] = spec;
        validate_resource_slice(&slice(v))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_slice_passes() {
        assert!(validate_resource_slice(&slice(base())).is_empty());
    }

    #[test]
    fn driver_required_and_dns() {
        assert!(errs(serde_json::json!({
            "driver": "", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": []
        }))
        .iter()
        .any(|m| m.contains("driver")));
        assert!(errs(serde_json::json!({
            "driver": "Bad_Driver", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": []
        }))
        .iter()
        .any(|m| m.contains("driver")));
    }

    #[test]
    fn pool_bounds() {
        let e = errs(serde_json::json!({
            "driver": "d.example.com",
            "pool": {"name": "p", "generation": -1, "resourceSliceCount": 0},
            "allNodes": true, "devices": []
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("resourceSliceCount") && m.contains("greater than zero")),
            "{e:?}"
        );
        assert!(
            e.iter()
                .any(|m| m.contains("generation") && m.contains("greater than or equal")),
            "{e:?}"
        );
    }

    #[test]
    fn node_selection_exactly_one() {
        // none set
        let none = errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "devices": []
        }));
        assert!(
            none.iter().any(|m| m.contains("exactly one of")),
            "{none:?}"
        );
        // two set
        let two = errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "nodeName": "node-1", "devices": []
        }));
        assert!(
            two.iter().any(|m| m.contains("multiple fields are set")),
            "{two:?}"
        );
        // allNodes:false invalid
        let f = errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": false, "devices": []
        }));
        assert!(f.iter().any(|m| m.contains("set to true")), "{f:?}");
    }

    #[test]
    fn duplicate_device_names_rejected() {
        let e = errs(serde_json::json!({
            "driver": "d.example.com", "pool": {"name": "p", "generation": 0, "resourceSliceCount": 1},
            "allNodes": true, "devices": [{"name": "a"}, {"name": "a"}]
        }));
        assert!(
            e.iter().any(|m| m.to_lowercase().contains("duplicate")),
            "{e:?}"
        );
    }

    #[test]
    fn update_immutable_driver_pool_node() {
        let old = slice(base());
        // unchanged -> ok
        assert!(validate_resource_slice_update(&slice(base()), &old).is_empty());
        // changed driver -> immutable
        let mut v = base();
        v["spec"]["driver"] = serde_json::json!("other.example.com");
        let errs = validate_resource_slice_update(&slice(v), &old);
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("driver") && e.detail == "field is immutable"),
            "{errs:?}"
        );
        // changed pool name -> immutable
        let mut v2 = base();
        v2["spec"]["pool"]["name"] = serde_json::json!("pool-2");
        let errs2 = validate_resource_slice_update(&slice(v2), &old);
        assert!(
            errs2
                .iter()
                .any(|e| e.field.ends_with("pool.name") && e.detail == "field is immutable"),
            "{errs2:?}"
        );
    }
}
