//! EndpointSlice validation — port of upstream Kubernetes
//! `pkg/apis/discovery/validation/validation.go::ValidateEndpointSlice`
//! (release-1.35).
//!
//! Scope: `addressType`, each endpoint's addresses (≥1, ≤100, valid for the
//! address type), each port (protocol/number/name), and the slice-level size
//! caps (≤1000 endpoints, ≤20000 ports). nodeName/hostname/topology/hints
//! detail is left as a follow-up.

use std::net::IpAddr;
use std::str::FromStr;

use crate::resources::endpointslice::{EndpointPort, EndpointSlice};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};

// Upstream `pkg/apis/discovery/validation` size limits.
const MAX_ENDPOINTS: usize = 1000;
const MAX_ADDRESSES: usize = 100;
const MAX_PORTS: usize = 20000;

/// Upstream `IsValidPortName`: IANA_SVC_NAME (DNS-1123 label ≤15 chars w/ a letter).
fn is_valid_port_name(s: &str) -> bool {
    s.len() <= 15 && is_dns1123_label(s).is_empty() && s.chars().any(|c| c.is_ascii_alphabetic())
}

fn validate_port(port: &EndpointPort, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(name) = &port.name {
        if !name.is_empty() && !is_valid_port_name(name) {
            errs.push(Error::invalid(
                &fld_path.child("name"),
                name.clone(),
                "must be an IANA_SVC_NAME",
            ));
        }
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
    if let Some(p) = port.port {
        if !(1..=65535).contains(&p) {
            errs.push(Error::invalid(
                &fld_path.child("port"),
                p,
                "must be between 1 and 65535, inclusive",
            ));
        }
    }
    errs
}

/// Validate an `EndpointSlice`. Mirrors the core of upstream `ValidateEndpointSlice`.
pub fn validate_endpoint_slice(slice: &EndpointSlice) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // addressType must be one of the supported values.
    if !matches!(slice.address_type.as_str(), "IPv4" | "IPv6" | "FQDN") {
        errs.push(Error::not_supported(
            &Path::new("addressType"),
            slice.address_type.clone(),
            &["IPv4", "IPv6", "FQDN"],
        ));
    }

    let eps_path = Path::new("endpoints");
    // Upstream caps the number of endpoints per slice (maxEndpoints).
    if slice.endpoints.len() > MAX_ENDPOINTS {
        errs.push(Error::too_many(&eps_path, MAX_ENDPOINTS));
    }
    for (i, ep) in slice.endpoints.iter().enumerate() {
        let addr_path = eps_path.index(i).child("addresses");
        if ep.addresses.is_empty() {
            errs.push(Error::required(
                &addr_path,
                "must contain at least 1 address",
            ));
        } else if ep.addresses.len() > MAX_ADDRESSES {
            errs.push(Error::too_many(&addr_path, MAX_ADDRESSES));
        }
        for (j, addr) in ep.addresses.iter().enumerate() {
            let p = addr_path.index(j);
            match slice.address_type.as_str() {
                "IPv4" => match IpAddr::from_str(addr) {
                    Ok(IpAddr::V4(_)) => {}
                    _ => errs.push(Error::invalid(&p, addr.clone(), "must be an IPv4 address")),
                },
                "IPv6" => match IpAddr::from_str(addr) {
                    Ok(IpAddr::V6(_)) => {}
                    _ => errs.push(Error::invalid(&p, addr.clone(), "must be an IPv6 address")),
                },
                "FQDN" => {
                    for msg in is_dns1123_subdomain(addr) {
                        errs.push(Error::invalid(&p, addr.clone(), msg));
                    }
                }
                _ => {} // unknown type: addresses not validated (matches upstream)
            }
        }
    }

    let ports_path = Path::new("ports");
    // Upstream caps the number of ports per slice (maxPorts).
    if slice.ports.len() > MAX_PORTS {
        errs.push(Error::too_many(&ports_path, MAX_PORTS));
    }
    for (i, port) in slice.ports.iter().enumerate() {
        errs.extend(validate_port(port, &ports_path.index(i)));
    }

    errs
}

/// Validate an `EndpointSlice` update — upstream `ValidateEndpointSliceUpdate`
/// (pkg/apis/discovery/validation): full field validation of the new slice,
/// plus `addressType` is immutable.
pub fn validate_endpoint_slice_update(
    new_slice: &EndpointSlice,
    old_slice: &EndpointSlice,
) -> ErrorList {
    let mut errs = validate_endpoint_slice(new_slice);
    if new_slice.address_type != old_slice.address_type {
        errs.push(Error::invalid(
            &Path::new("addressType"),
            new_slice.address_type.clone(),
            "field is immutable",
        ));
    }
    errs
}

#[cfg(test)]
mod size_cap_tests {
    use super::*;

    fn slice_with(endpoints: usize, addrs_per: usize, ports: usize) -> EndpointSlice {
        let eps: Vec<_> = (0..endpoints)
            .map(|_| serde_json::json!({"addresses": vec!["10.0.0.1"; addrs_per.max(1)]}))
            .collect();
        let ps: Vec<_> = (0..ports)
            .map(|_| serde_json::json!({"port": 80}))
            .collect();
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "es"},
            "addressType": "IPv4",
            "endpoints": eps,
            "ports": ps
        }))
        .unwrap()
    }

    #[test]
    fn within_caps_passes() {
        let s = slice_with(2, 1, 2);
        assert!(validate_endpoint_slice(&s)
            .iter()
            .all(|e| !e.to_string().contains("at most")));
    }

    #[test]
    fn too_many_endpoints_rejected() {
        let s = slice_with(MAX_ENDPOINTS + 1, 1, 1);
        let errs = validate_endpoint_slice(&s);
        assert!(
            errs.iter()
                .any(|e| e.field == "endpoints" && e.to_string().contains("at most")),
            "{errs:?}"
        );
    }

    #[test]
    fn too_many_addresses_rejected() {
        let s = slice_with(1, MAX_ADDRESSES + 1, 1);
        let errs = validate_endpoint_slice(&s);
        assert!(
            errs.iter()
                .any(|e| e.field.contains("addresses") && e.to_string().contains("at most")),
            "{errs:?}"
        );
    }

    #[test]
    fn too_many_ports_rejected() {
        let s = slice_with(1, 1, MAX_PORTS + 1);
        let errs = validate_endpoint_slice(&s);
        assert!(
            errs.iter()
                .any(|e| e.field == "ports" && e.to_string().contains("at most")),
            "{errs:?}"
        );
    }
}
