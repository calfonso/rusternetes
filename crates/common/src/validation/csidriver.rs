//! CSIDriver validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateCSIDriver` (release-1.35).
//!
//! The typed `FSGroupPolicy` / `VolumeLifecycleMode` enums already reject
//! invalid values at decode time, so the meaningful create-time checks ported
//! here are the required-field presence checks (`attachRequired`,
//! `podInfoOnMount`, `storageCapacity`), the `nodeAllocatableUpdatePeriodSeconds`
//! lower bound, the `serviceAccountTokenInSecrets`/`tokenRequests` cross-field
//! check, and `tokenRequests` (duplicate audience + expiration bounds).
//! ObjectMeta is validated separately (#1087 / #1277). CSI conformance is
//! non-negotiable for this project.

use crate::resources::csi::CSIDriver;
use crate::validation::field::{Error, ErrorList, Path};
use std::collections::HashSet;

// Upstream `validateTokenRequests` bounds.
const TOKEN_EXPIRATION_MIN_SECONDS: i64 = 600; // 10 minutes
const TOKEN_EXPIRATION_MAX_SECONDS: i64 = 1 << 32;

/// Validate a `CSIDriver` on create. Mirrors upstream `ValidateCSIDriver` /
/// `validateCSIDriverSpec` minus ObjectMeta and the enum/bool checks the Rust
/// type system already enforces.
pub fn validate_csi_driver(driver: &CSIDriver) -> ErrorList {
    let spec = &driver.spec;
    let spec_path = Path::new("spec");
    let mut errs: ErrorList = Vec::new();

    // attachRequired is required (defaulted to true by the handler; this guards
    // raw/storage-seeded objects). Upstream uses the `attachedRequired` path.
    if spec.attach_required.is_none() {
        errs.push(Error::required(&spec_path.child("attachedRequired"), ""));
    }

    // podInfoOnMount required — validateCSIDriverSpec/validatePodInfoOnMount
    // (validation.go:452 / 483-490).
    if spec.pod_info_on_mount.is_none() {
        errs.push(Error::required(&spec_path.child("podInfoOnMount"), ""));
    }

    // storageCapacity required — validateCSIDriverSpec/validateStorageCapacity
    // (validation.go:453 / 493-500).
    if spec.storage_capacity.is_none() {
        errs.push(Error::required(&spec_path.child("storageCapacity"), ""));
    }

    // nodeAllocatableUpdatePeriodSeconds must be >= 10 when set —
    // validateNodeAllocatableUpdatePeriodSeconds (validation.go:458 / 464-470).
    if let Some(period) = spec.node_allocatable_update_period_seconds {
        if period < 10 {
            errs.push(Error::invalid(
                &spec_path.child("nodeAllocatableUpdatePeriodSeconds"),
                period,
                "must be greater than or equal to 10 seconds",
            ));
        }
    }

    if let Some(token_requests) = &spec.token_requests {
        let tr_path = spec_path.child("tokenRequests");
        let mut audiences: HashSet<&str> = HashSet::new();
        for (i, tr) in token_requests.iter().enumerate() {
            let p = tr_path.index(i);
            if !audiences.insert(tr.audience.as_str()) {
                errs.push(Error::duplicate(&p.child("audience"), tr.audience.clone()));
                continue;
            }
            if let Some(exp) = tr.expiration_seconds {
                if exp < TOKEN_EXPIRATION_MIN_SECONDS {
                    errs.push(Error::invalid(
                        &p.child("expirationSeconds"),
                        exp,
                        "may not specify a duration less than 10 minutes",
                    ));
                }
                if exp > TOKEN_EXPIRATION_MAX_SECONDS {
                    errs.push(Error::invalid(
                        &p.child("expirationSeconds"),
                        exp,
                        "may not specify a duration larger than 2^32 seconds",
                    ));
                }
            }
        }
    }

    // serviceAccountTokenInSecrets set but tokenRequests empty → Invalid —
    // validateServiceAccountTokenInSecrets (validation.go:459 / 577-584).
    // Upstream gates on `len(tokenRequests) == 0`, which is true for both a
    // nil slice and an empty one.
    if let Some(in_secrets) = spec.service_account_token_in_secrets {
        let token_requests_empty = spec.token_requests.as_ref().is_none_or(|t| t.is_empty());
        if token_requests_empty {
            errs.push(Error::invalid(
                &spec_path.child("serviceAccountTokenInSecrets"),
                in_secrets,
                "serviceAccountTokenInSecrets is set but no tokenRequests are specified",
            ));
        }
    }

    errs
}

/// Validate a CSIDriver update — upstream `ValidateCSIDriverUpdate`
/// (pkg/apis/storage/validation): `attachRequired` and `volumeLifecycleModes`
/// are immutable, plus full re-validation of the new object.
pub fn validate_csi_driver_update(new_d: &CSIDriver, old_d: &CSIDriver) -> ErrorList {
    let mut errs = validate_csi_driver(new_d);
    if new_d.spec.attach_required != old_d.spec.attach_required {
        errs.push(Error::invalid(
            &Path::new("spec").child("attachRequired"),
            "<changed>".to_string(),
            "field is immutable",
        ));
    }
    if serde_json::to_value(&new_d.spec.volume_lifecycle_modes).ok()
        != serde_json::to_value(&old_d.spec.volume_lifecycle_modes).ok()
    {
        errs.push(Error::invalid(
            &Path::new("spec").child("volumeLifecycleModes"),
            "<changed>".to_string(),
            "field is immutable",
        ));
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::csi::{CSIDriverSpec, TokenRequest};
    use crate::types::{ObjectMeta, TypeMeta};
    use crate::validation::field::ErrorType;

    /// A spec that passes every create-time rule, so each test can mutate one
    /// field and assert the single resulting error.
    fn valid_spec() -> CSIDriverSpec {
        CSIDriverSpec {
            attach_required: Some(true),
            pod_info_on_mount: Some(false),
            storage_capacity: Some(true),
            node_allocatable_update_period_seconds: None,
            service_account_token_in_secrets: None,
            ..Default::default()
        }
    }

    fn driver(spec: CSIDriverSpec) -> CSIDriver {
        CSIDriver {
            type_meta: TypeMeta {
                kind: "CSIDriver".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-driver"),
            spec,
        }
    }

    fn has(errs: &ErrorList, field: &str, ty: ErrorType) -> bool {
        errs.iter().any(|e| e.field == field && e.error_type == ty)
    }

    #[test]
    fn fully_valid_spec_passes() {
        assert!(validate_csi_driver(&driver(valid_spec())).is_empty());
    }

    #[test]
    fn pod_info_on_mount_required() {
        let mut spec = valid_spec();
        spec.pod_info_on_mount = None;
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(&errs, "spec.podInfoOnMount", ErrorType::Required),
            "{errs:?}"
        );
    }

    #[test]
    fn storage_capacity_required() {
        let mut spec = valid_spec();
        spec.storage_capacity = None;
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(&errs, "spec.storageCapacity", ErrorType::Required),
            "{errs:?}"
        );
    }

    #[test]
    fn node_allocatable_update_period_below_min_invalid() {
        let mut spec = valid_spec();
        spec.node_allocatable_update_period_seconds = Some(9);
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(
                &errs,
                "spec.nodeAllocatableUpdatePeriodSeconds",
                ErrorType::Invalid
            ),
            "{errs:?}"
        );
    }

    #[test]
    fn node_allocatable_update_period_at_min_ok() {
        let mut spec = valid_spec();
        spec.node_allocatable_update_period_seconds = Some(10);
        assert!(validate_csi_driver(&driver(spec)).is_empty());
    }

    #[test]
    fn service_account_token_in_secrets_without_token_requests_invalid() {
        let mut spec = valid_spec();
        spec.service_account_token_in_secrets = Some(true);
        spec.token_requests = None;
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(
                &errs,
                "spec.serviceAccountTokenInSecrets",
                ErrorType::Invalid
            ),
            "{errs:?}"
        );

        // Empty (non-nil) tokenRequests is treated the same as nil upstream.
        let mut spec = valid_spec();
        spec.service_account_token_in_secrets = Some(false);
        spec.token_requests = Some(vec![]);
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            has(
                &errs,
                "spec.serviceAccountTokenInSecrets",
                ErrorType::Invalid
            ),
            "{errs:?}"
        );
    }

    #[test]
    fn service_account_token_in_secrets_with_token_requests_ok() {
        let mut spec = valid_spec();
        spec.service_account_token_in_secrets = Some(true);
        spec.token_requests = Some(vec![TokenRequest {
            audience: "aud".to_string(),
            expiration_seconds: None,
        }]);
        let errs = validate_csi_driver(&driver(spec));
        assert!(
            !has(
                &errs,
                "spec.serviceAccountTokenInSecrets",
                ErrorType::Invalid
            ),
            "{errs:?}"
        );
    }
}
