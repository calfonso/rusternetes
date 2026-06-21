//! Endpoints (core/v1) validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::validateEndpointSubsets`
//! (release-1.35).
//!
//! Scope: each subset must carry addresses (the endpoint IPs must be valid and
//! non-special) and well-formed ports. The hostname/nodeName/targetRef detail is
//! left as a follow-up.

use std::net::IpAddr;
use std::str::FromStr;

use crate::resources::endpoints::{EndpointPort, Endpoints};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_label;

/// Upstream `IsValidPortName`: IANA_SVC_NAME (DNS-1123 label ≤15 chars w/ a letter).
fn is_valid_port_name(s: &str) -> bool {
    s.len() <= 15 && is_dns1123_label(s).is_empty() && s.chars().any(|c| c.is_ascii_alphabetic())
}

/// Mirrors upstream `ValidateEndpointIP`: a valid IP that is not unspecified,
/// loopback, or link-local.
fn validate_endpoint_ip(ip: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Ok(parsed) = IpAddr::from_str(ip) else {
        errs.push(Error::invalid(
            fld_path,
            ip.to_string(),
            "must be a valid IP address",
        ));
        return errs;
    };
    if parsed.is_unspecified() {
        errs.push(Error::invalid(
            fld_path,
            ip.to_string(),
            format!("may not be unspecified ({ip})"),
        ));
    }
    if parsed.is_loopback() {
        errs.push(Error::invalid(
            fld_path,
            ip.to_string(),
            "may not be in the loopback range (127.0.0.0/8, ::1/128)",
        ));
    }
    let link_local = match parsed {
        IpAddr::V4(v4) => v4.is_link_local(),
        // fe80::/10
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
    };
    if link_local {
        errs.push(Error::invalid(
            fld_path,
            ip.to_string(),
            "may not be in the link-local range (169.254.0.0/16, fe80::/10)",
        ));
    }
    errs
}

fn validate_port(port: &EndpointPort, require_name: bool, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match &port.name {
        Some(n) if !n.is_empty() => {
            if !is_valid_port_name(n) {
                errs.push(Error::invalid(
                    &fld_path.child("name"),
                    n.clone(),
                    "must be an IANA_SVC_NAME",
                ));
            }
        }
        _ => {
            if require_name {
                errs.push(Error::required(&fld_path.child("name"), ""));
            }
        }
    }
    if !(1..=65535).contains(&(port.port as i32)) {
        errs.push(Error::invalid(
            &fld_path.child("port"),
            port.port as i32,
            "must be between 1 and 65535, inclusive",
        ));
    }
    if let Some(proto) = &port.protocol {
        if !matches!(proto.as_str(), "TCP" | "UDP" | "SCTP") {
            errs.push(Error::not_supported(
                &fld_path.child("protocol"),
                proto.clone(),
                &["TCP", "UDP", "SCTP"],
            ));
        }
    }
    errs
}

/// Validate an `Endpoints` object. Mirrors the core of upstream `ValidateEndpoints`.
pub fn validate_endpoints(endpoints: &Endpoints) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let subsets_path = Path::new("subsets");

    for (i, ss) in endpoints.subsets.iter().enumerate() {
        let idx = subsets_path.index(i);
        let addrs = ss.addresses.as_deref().unwrap_or(&[]);
        let not_ready = ss.not_ready_addresses.as_deref().unwrap_or(&[]);
        if addrs.is_empty() && not_ready.is_empty() {
            errs.push(Error::required(
                &idx,
                "must specify `addresses` or `notReadyAddresses`",
            ));
        }
        for (j, a) in addrs.iter().enumerate() {
            errs.extend(validate_endpoint_ip(
                &a.ip,
                &idx.child("addresses").index(j).child("ip"),
            ));
        }
        for (j, a) in not_ready.iter().enumerate() {
            errs.extend(validate_endpoint_ip(
                &a.ip,
                &idx.child("notReadyAddresses").index(j).child("ip"),
            ));
        }
        if let Some(ports) = &ss.ports {
            let require_name = ports.len() > 1;
            for (j, p) in ports.iter().enumerate() {
                errs.extend(validate_port(p, require_name, &idx.child("ports").index(j)));
            }
        }
    }

    errs
}
