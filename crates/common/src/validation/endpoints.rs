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
    // name: required when multi-port; when present it must be a DNS-1123 label.
    // Upstream `validateEndpointPort` (validation.go:8338-8342) validates the
    // port name with `ValidateDNS1123Label` — NOT the 15-char IANA_SVC_NAME
    // `IsValidPortName` rule (that one is reserved for ContainerPort names and
    // string targetPorts).
    match &port.name {
        Some(n) if !n.is_empty() => {
            for msg in is_dns1123_label(n) {
                errs.push(Error::invalid(&fld_path.child("name"), n.clone(), msg));
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
    // protocol: required, then must be TCP/UDP/SCTP. Upstream
    // `validateEndpointPort` (validation.go:8346-8350) emits Required when
    // empty, NotSupported otherwise.
    match port.protocol.as_str() {
        "" => {
            errs.push(Error::required(&fld_path.child("protocol"), ""));
        }
        "TCP" | "UDP" | "SCTP" => {}
        other => {
            errs.push(Error::not_supported(
                &fld_path.child("protocol"),
                other.to_string(),
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

#[cfg(test)]
mod port_tests {
    use super::*;
    use crate::validation::field::ErrorType;

    fn ep(subsets: serde_json::Value) -> Endpoints {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "ep", "namespace": "default"},
            "subsets": subsets
        }))
        .unwrap()
    }

    fn has(errs: &ErrorList, field: &str, ty: ErrorType) -> bool {
        errs.iter().any(|e| e.field == field && e.error_type == ty)
    }

    // Upstream validateEndpointPort: protocol empty → Required (validation.go:8346).
    // A *missing* protocol is defaulted to TCP by serde, so the empty case is
    // exercised with an explicit "" (the String unset sentinel).
    #[test]
    fn empty_protocol_is_required() {
        let errs = validate_endpoints(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"port": 80, "protocol": ""}]
        }])));
        assert!(
            has(&errs, "subsets[0].ports[0].protocol", ErrorType::Required),
            "{errs:?}"
        );
    }

    #[test]
    fn tcp_protocol_passes() {
        let errs = validate_endpoints(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"port": 80, "protocol": "TCP"}]
        }])));
        assert!(
            !errs
                .iter()
                .any(|e| e.field == "subsets[0].ports[0].protocol"),
            "{errs:?}"
        );
    }

    // Upstream validates EndpointPort.Name as a DNS-1123 label (≤63 chars),
    // NOT the 15-char IANA_SVC_NAME rule (validation.go:8341).
    #[test]
    fn long_dns_label_port_name_passes() {
        let errs = validate_endpoints(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"name": "tcp-prometheus-servicemonitor", "port": 80, "protocol": "TCP"}]
        }])));
        assert!(
            !errs.iter().any(|e| e.field == "subsets[0].ports[0].name"),
            "{errs:?}"
        );
    }

    #[test]
    fn uppercase_port_name_rejected_as_invalid_label() {
        let errs = validate_endpoints(&ep(serde_json::json!([{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"name": "HTTP", "port": 80, "protocol": "TCP"}]
        }])));
        assert!(
            has(&errs, "subsets[0].ports[0].name", ErrorType::Invalid),
            "{errs:?}"
        );
    }
}
