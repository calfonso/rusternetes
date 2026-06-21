//! CSIDriver validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateCSIDriver` (release-1.35).
//!
//! The typed `FSGroupPolicy` / `VolumeLifecycleMode` enums already reject
//! invalid values at decode time, so the meaningful create-time checks ported
//! here are `attachRequired` presence (after defaulting) and `tokenRequests`
//! (duplicate audience + expiration bounds). ObjectMeta is validated separately
//! (#1087 / #1277). CSI conformance is non-negotiable for this project.

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

    errs
}
