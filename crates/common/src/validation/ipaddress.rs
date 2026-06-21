//! Validation for `networking.k8s.io` IPAddress, ported from upstream
//! `ValidateIPAddress` / `validateIPAddressParentReference`
//! (`pkg/apis/networking/validation/validation.go`).
//!
//! The `metadata.name` must be a canonical IP (upstream `ValidateIPAddressName`)
//! — that is enforced by the api-server create handler via `NameKind::Ip`.
//! This module covers `spec.parentRef`.

use crate::resources::ipaddress::{IPAddress, ParentReference};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;
use crate::validation::objectmeta::name_is_path_segment;

/// Validate an IPAddress on create — upstream `ValidateIPAddress` (minus the
/// name check, which the handler does via `NameKind::Ip`).
pub fn validate_ip_address(ip: &IPAddress) -> ErrorList {
    let spec_path = Path::new("spec");
    match &ip.spec {
        // A missing spec is a missing parentRef (which is required).
        None => vec![Error::required(&spec_path.child("parentRef"), "")],
        Some(spec) => validate_parent_reference(&spec.parent_ref, &spec_path),
    }
}

/// Upstream `validateIPAddressParentReference`.
fn validate_parent_reference(pr: &ParentReference, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    let p = fld_path.child("parentRef");

    // group is required, but the core group (used by Services) is the empty
    // value and so cannot be enforced; only validate it when present.
    if let Some(group) = &pr.group {
        if !group.is_empty() {
            for msg in is_dns1123_subdomain(group) {
                errs.push(Error::invalid(&p.child("group"), group.clone(), msg));
            }
        }
    }

    // resource is required.
    if pr.resource.is_empty() {
        errs.push(Error::required(&p.child("resource"), ""));
    } else {
        for msg in name_is_path_segment(&pr.resource, false) {
            errs.push(Error::invalid(
                &p.child("resource"),
                pr.resource.clone(),
                msg,
            ));
        }
    }

    // name is required.
    if pr.name.is_empty() {
        errs.push(Error::required(&p.child("name"), ""));
    } else {
        for msg in name_is_path_segment(&pr.name, false) {
            errs.push(Error::invalid(&p.child("name"), pr.name.clone(), msg));
        }
    }

    // namespace is optional.
    if let Some(ns) = &pr.namespace {
        if !ns.is_empty() {
            for msg in name_is_path_segment(ns, false) {
                errs.push(Error::invalid(&p.child("namespace"), ns.clone(), msg));
            }
        }
    }

    errs
}
