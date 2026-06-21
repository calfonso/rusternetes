//! ServiceCIDR validation — port of upstream Kubernetes
//! `pkg/apis/networking/validation/validation.go::ValidateServiceCIDR`
//! (release-1.35).
//!
//! Covers `spec.cidrs`: at least one, at most two, each a valid CIDR, and a
//! dual-stack pair must be one CIDR per IP family. ObjectMeta is validated
//! separately (#1087 / #1277). Networking/CNI compatibility is a non-negotiable
//! project contract.

use crate::resources::servicecidr::ServiceCIDR;
use crate::validation::field::{Error, ErrorList, Path};
use std::net::IpAddr;

#[derive(PartialEq, Eq, Clone, Copy)]
enum IpFamily {
    V4,
    V6,
}

/// Parse a `ip/prefix` CIDR string, returning its IP family if valid. Mirrors
/// the intent of upstream `IsValidCIDR` (valid IP + in-range prefix length);
/// like upstream's sloppy parser it does not require canonical (host-bits-zero)
/// form.
fn parse_cidr(cidr: &str) -> Option<IpFamily> {
    let (ip, prefix) = cidr.split_once('/')?;
    let prefix: u8 = prefix.parse().ok()?;
    match ip.parse::<IpAddr>().ok()? {
        IpAddr::V4(_) if prefix <= 32 => Some(IpFamily::V4),
        IpAddr::V6(_) if prefix <= 128 => Some(IpFamily::V6),
        _ => None,
    }
}

/// Validate a `ServiceCIDR` on create. Mirrors upstream `validateServiceCIDRSpec`.
pub fn validate_service_cidr(sc: &ServiceCIDR) -> ErrorList {
    let cidrs_path = Path::new("spec").child("cidrs");
    let mut errs: ErrorList = Vec::new();

    let cidrs: &[String] = match &sc.spec {
        Some(spec) => &spec.cidrs,
        None => &[],
    };

    if cidrs.is_empty() {
        errs.push(Error::required(&cidrs_path, "at least one CIDR required"));
        return errs;
    }
    if cidrs.len() > 2 {
        errs.push(Error::invalid(
            &cidrs_path,
            cidrs.join(","),
            "may only hold up to 2 values",
        ));
        return errs;
    }

    let mut families: Vec<Option<IpFamily>> = Vec::with_capacity(cidrs.len());
    for (i, cidr) in cidrs.iter().enumerate() {
        let fam = parse_cidr(cidr);
        if fam.is_none() {
            errs.push(Error::invalid(
                &cidrs_path.index(i),
                cidr.clone(),
                "must be a valid CIDR value, (e.g. 10.9.8.0/24 or 2001:db8::/64)",
            ));
        }
        families.push(fam);
    }

    // A dual-stack pair must be one CIDR per IP family.
    if cidrs.len() == 2 {
        if let (Some(a), Some(b)) = (families[0], families[1]) {
            if a == b {
                errs.push(Error::invalid(
                    &cidrs_path,
                    cidrs.join(","),
                    "may specify no more than one IP for each IP family, i.e 192.168.0.0/24 and 2001:db8::/64",
                ));
            }
        }
    }

    errs
}

/// Validate a ServiceCIDR update — upstream `ValidateServiceCIDRUpdate`
/// (pkg/apis/networking/validation): `spec.cidrs` is immutable, except a
/// single-stack CIDR may be expanded to dual-stack by appending one CIDR (the
/// existing entry must not change; the new entry is fully validated).
pub fn validate_service_cidr_update(new_sc: &ServiceCIDR, old_sc: &ServiceCIDR) -> ErrorList {
    let mut errs = ErrorList::new();
    let p = Path::new("spec").child("cidrs");
    let empty: Vec<String> = Vec::new();
    let old = old_sc.spec.as_ref().map(|s| &s.cidrs).unwrap_or(&empty);
    let new = new_sc.spec.as_ref().map(|s| &s.cidrs).unwrap_or(&empty);
    if old.len() == new.len() {
        for (i, ip) in old.iter().enumerate() {
            if *ip != new[i] {
                errs.push(Error::invalid(
                    &p.index(i),
                    new[i].clone(),
                    "field is immutable",
                ));
            }
        }
    } else if old.len() == 1 && new.len() == 2 {
        if new[0] != old[0] {
            errs.push(Error::invalid(
                &p.index(0),
                new[0].clone(),
                "field is immutable",
            ));
        }
        // Validate the (now dual-stack) cidrs set.
        errs.extend(validate_service_cidr(new_sc));
    } else {
        errs.push(Error::invalid(&p, new.join(","), "field is immutable"));
    }
    errs
}
