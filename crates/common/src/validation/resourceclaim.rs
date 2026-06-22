//! ResourceClaim (resource.k8s.io / DRA) validation — port of upstream
//! Kubernetes `pkg/apis/resource/validation/validation.go::ValidateResourceClaim`
//! (release-1.35).
//!
//! Scope: the CEL-free structural validation of `spec.devices.requests` —
//! request count cap + unique names, per-request `exactly`/`firstAvailable`
//! mutual exclusivity, `deviceClassName` (required + DNS subdomain),
//! `allocationMode` enum + `count` coupling, and the `firstAvailable`
//! sub-request set. The CEL `selectors`, `constraints`, and `config` are
//! tracked in #1442 (they need a DRA CEL environment). ObjectMeta is validated
//! separately.

use crate::resources::{
    DeviceAllocationMode, DeviceClaim, ExactDeviceRequest, ResourceClaim, ResourceClaimSpec,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
use std::collections::HashSet;

const DEVICE_REQUESTS_MAX_SIZE: usize = 32;
const FIRST_AVAILABLE_MAX_SIZE: usize = 8;

/// Validate a `ResourceClaim` on create. Mirrors the structural portion of
/// upstream `ValidateResourceClaim` (minus CEL/constraints/config — see #1442).
pub fn validate_resource_claim(claim: &ResourceClaim) -> ErrorList {
    validate_resource_claim_spec(&claim.spec, &Path::new("spec"))
}

/// Validate a `ResourceClaim` on update. Mirrors `ValidateResourceClaimUpdate`:
/// `spec` is immutable after creation, plus the create validation.
pub fn validate_resource_claim_update(new: &ResourceClaim, old: &ResourceClaim) -> ErrorList {
    let mut errs = validate_resource_claim(new);
    if serde_json::to_value(&new.spec).ok() != serde_json::to_value(&old.spec).ok() {
        errs.push(Error::invalid(
            &Path::new("spec"),
            "<spec>".to_string(),
            "field is immutable",
        ));
    }
    errs
}

/// Validate a `ResourceClaimSpec` at the given path. Shared by `ResourceClaim`
/// and `ResourceClaimTemplate` (upstream `validateResourceClaimSpec`).
pub fn validate_resource_claim_spec(spec: &ResourceClaimSpec, fld_path: &Path) -> ErrorList {
    validate_device_claim(&spec.devices, &fld_path.child("devices"))
}

/// Validate a `ResourceClaimTemplate` on create. Mirrors upstream
/// `ValidateResourceClaimTemplate`: validates the embedded `spec.spec`
/// (a `ResourceClaimSpec`). ObjectMeta is validated separately.
pub fn validate_resource_claim_template(
    template: &crate::resources::ResourceClaimTemplate,
) -> ErrorList {
    validate_resource_claim_spec(&template.spec.spec, &Path::new("spec").child("spec"))
}

fn validate_device_claim(claim: &DeviceClaim, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let requests_path = fld_path.child("requests");

    if claim.requests.len() > DEVICE_REQUESTS_MAX_SIZE {
        errs.push(Error::too_many(&requests_path, DEVICE_REQUESTS_MAX_SIZE));
    }

    let mut seen: HashSet<&str> = HashSet::new();
    for (i, request) in claim.requests.iter().enumerate() {
        let rp = requests_path.index(i);

        // name — required, DNS-1123 label, unique across requests.
        if request.name.is_empty() {
            errs.push(Error::required(&rp.child("name"), ""));
        } else {
            for msg in is_dns1123_label(&request.name) {
                errs.push(Error::invalid(&rp.child("name"), request.name.clone(), msg));
            }
            if !seen.insert(request.name.as_str()) {
                errs.push(Error::duplicate(&rp.child("name"), request.name.clone()));
            }
        }

        // exactly one of `exactly` / `firstAvailable`.
        let has_exactly = request.exactly.is_some();
        let has_first = !request.first_available.is_empty();
        match (has_exactly, has_first) {
            (false, false) => errs.push(Error::required(
                &rp,
                "exactly one of `exactly` or `firstAvailable` is required",
            )),
            (true, true) => errs.push(Error::invalid(
                &rp,
                String::new(),
                "exactly one of `exactly` or `firstAvailable` is required, but multiple fields are set",
            )),
            (true, false) => {
                errs.extend(validate_exact_device_request(
                    request.exactly.as_ref().unwrap(),
                    &rp.child("exactly"),
                ));
            }
            (false, true) => {
                let fa_path = rp.child("firstAvailable");
                if request.first_available.len() > FIRST_AVAILABLE_MAX_SIZE {
                    errs.push(Error::too_many(&fa_path, FIRST_AVAILABLE_MAX_SIZE));
                }
                let mut sub_seen: HashSet<&str> = HashSet::new();
                for (j, sub) in request.first_available.iter().enumerate() {
                    let sp = fa_path.index(j);
                    if sub.name.is_empty() {
                        errs.push(Error::required(&sp.child("name"), ""));
                    } else {
                        for msg in is_dns1123_label(&sub.name) {
                            errs.push(Error::invalid(&sp.child("name"), sub.name.clone(), msg));
                        }
                        if !sub_seen.insert(sub.name.as_str()) {
                            errs.push(Error::duplicate(&sp.child("name"), sub.name.clone()));
                        }
                    }
                    errs.extend(validate_device_class_name(
                        &sub.device_class_name,
                        &sp.child("deviceClassName"),
                    ));
                }
            }
        }
    }
    errs
}

fn validate_exact_device_request(req: &ExactDeviceRequest, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    errs.extend(validate_device_class_name(
        &req.device_class_name,
        &fld_path.child("deviceClassName"),
    ));
    // allocationMode + count coupling (only when allocationMode is set).
    if let Some(mode) = &req.allocation_mode {
        let count = req.count.unwrap_or(0);
        match mode {
            DeviceAllocationMode::All => {
                if count != 0 {
                    errs.push(Error::invalid(
                        &fld_path.child("count"),
                        count,
                        "must not be specified when allocationMode is 'All'",
                    ));
                }
            }
            DeviceAllocationMode::ExactCount => {
                if count <= 0 {
                    errs.push(Error::invalid(
                        &fld_path.child("count"),
                        count,
                        "must be greater than zero",
                    ));
                }
            }
        }
    }
    errs
}

/// `deviceClassName` is required and a DNS-1123 subdomain (upstream
/// `validateDeviceClass`).
fn validate_device_class_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if name.is_empty() {
        errs.push(Error::required(fld_path, ""));
    } else {
        for msg in is_dns1123_subdomain(name) {
            errs.push(Error::invalid(fld_path, name.to_string(), msg));
        }
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(devices: serde_json::Value) -> ResourceClaim {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "claim-1"},
            "spec": {"devices": devices},
        }))
        .unwrap()
    }

    fn errs(devices: serde_json::Value) -> Vec<String> {
        validate_resource_claim(&claim(devices))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_exact_request_passes() {
        assert!(errs(serde_json::json!({
            "requests": [{
                "name": "gpu",
                "exactly": {"deviceClassName": "gpu.example.com", "allocationMode": "ExactCount", "count": 2}
            }]
        }))
        .is_empty());
    }

    #[test]
    fn request_needs_exactly_one_of_exactly_or_first_available() {
        // neither
        assert!(errs(serde_json::json!({"requests": [{"name": "r"}]}))
            .iter()
            .any(|m| m.contains("exactly one of")));
        // both
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r",
                "exactly": {"deviceClassName": "c.example.com"},
                "firstAvailable": [{"name": "s", "deviceClassName": "c.example.com"}]}]
        }))
        .iter()
        .any(|m| m.contains("multiple fields are set")));
    }

    #[test]
    fn duplicate_request_names_rejected() {
        let e = errs(serde_json::json!({
            "requests": [
                {"name": "r", "exactly": {"deviceClassName": "c.example.com"}},
                {"name": "r", "exactly": {"deviceClassName": "c.example.com"}}
            ]
        }));
        assert!(
            e.iter().any(|m| m.to_lowercase().contains("duplicate")),
            "{e:?}"
        );
    }

    #[test]
    fn allocation_mode_count_coupling() {
        // ExactCount with count 0 -> error
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 0}}]
        }))
        .iter()
        .any(|m| m.contains("greater than zero")));
        // All with a count -> error
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "All", "count": 3}}]
        }))
        .iter()
        .any(|m| m.contains("must not be specified when allocationMode is 'All'")));
    }

    #[test]
    fn bad_device_class_name_rejected() {
        let e = errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": ""}}]
        }));
        assert!(e.iter().any(|m| m.contains("deviceClassName")), "{e:?}");
    }

    #[test]
    fn spec_immutable_on_update() {
        let old = claim(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 1}}]
        }));
        let new = claim(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 2}}]
        }));
        let e = validate_resource_claim_update(&new, &old);
        assert!(
            e.iter()
                .any(|x| x.field == "spec" && x.detail == "field is immutable"),
            "{e:?}"
        );
    }

    #[test]
    fn template_validates_embedded_spec() {
        use crate::resources::ResourceClaimTemplate;
        let tmpl: ResourceClaimTemplate = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "t"},
            "spec": {"spec": {"devices": {"requests": [
                {"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 0}}
            ]}}}
        })).unwrap();
        let errs = validate_resource_claim_template(&tmpl);
        // count 0 with ExactCount -> error, attached under spec.spec.devices...
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("greater than zero")),
            "{errs:?}"
        );
        assert!(
            errs.iter()
                .any(|e| e.field.starts_with("spec.spec.devices")),
            "{errs:?}"
        );
    }
}
