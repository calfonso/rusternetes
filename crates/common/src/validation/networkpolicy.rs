//! NetworkPolicy validation — port of upstream Kubernetes
//! `pkg/apis/networking/validation/validation.go::ValidateNetworkPolicySpec`
//! (release-1.35).
//!
//! Note: the ipBlock `except` "strict subset of cidr" check needs subnet
//! arithmetic (a CIDR type we don't pull in here); we validate that the cidr
//! and each except are syntactically valid CIDRs and leave the containment
//! check as a follow-up.

use std::net::IpAddr;
use std::str::FromStr;

use crate::resources::networking::{
    IPBlock, NetworkPolicy, NetworkPolicyPeer, NetworkPolicyPort, NetworkPolicySpec,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, validate_label_selector, LabelSelectorValidationOptions,
};

const MIN_PORT: i64 = 1;
const MAX_PORT: i64 = 65535;

/// Syntactic CIDR validity: `IP/prefix`, prefix within the address family's
/// bit-width. Mirrors the validity half of upstream `IsValidCIDR`.
fn is_valid_cidr(s: &str) -> bool {
    let Some((ip, prefix)) = s.split_once('/') else {
        return false;
    };
    let max_prefix = match IpAddr::from_str(ip) {
        Ok(IpAddr::V4(_)) => 32u8,
        Ok(IpAddr::V6(_)) => 128u8,
        Err(_) => return false,
    };
    matches!(prefix.parse::<u8>(), Ok(p) if p <= max_prefix)
}

/// Upstream `IsValidPortName`: an IANA_SVC_NAME — a DNS-1123 label ≤15 chars
/// containing at least one letter.
fn is_valid_port_name(s: &str) -> bool {
    s.len() <= 15 && is_dns1123_label(s).is_empty() && s.chars().any(|c| c.is_ascii_alphabetic())
}

/// Port-level validation. Mirrors upstream `ValidateNetworkPolicyPort`.
fn validate_port(port: &NetworkPolicyPort, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(proto) = &port.protocol {
        if !matches!(proto.as_str(), "TCP" | "UDP" | "SCTP") {
            errs.push(Error::not_supported(
                &fld_path.child("protocol"),
                proto.clone(),
                &["TCP", "UDP", "SCTP"],
            ));
        }
    }

    match &port.port {
        None => {
            if let Some(ep) = port.end_port {
                errs.push(Error::invalid(
                    &fld_path.child("endPort"),
                    ep,
                    "may not be specified when `port` is not specified",
                ));
            }
        }
        Some(serde_json::Value::Number(n)) => {
            let p = n.as_i64().unwrap_or(0);
            if !(MIN_PORT..=MAX_PORT).contains(&p) {
                errs.push(Error::invalid(
                    &fld_path.child("port"),
                    p,
                    "must be between 1 and 65535, inclusive",
                ));
            }
            if let Some(ep) = port.end_port {
                if (ep as i64) < p {
                    errs.push(Error::invalid(
                        &fld_path.child("endPort"),
                        ep,
                        "must be greater than or equal to `port`",
                    ));
                }
                if !(MIN_PORT..=MAX_PORT).contains(&(ep as i64)) {
                    errs.push(Error::invalid(
                        &fld_path.child("endPort"),
                        ep,
                        "must be between 1 and 65535, inclusive",
                    ));
                }
            }
        }
        Some(serde_json::Value::String(s)) => {
            if let Some(ep) = port.end_port {
                errs.push(Error::invalid(
                    &fld_path.child("endPort"),
                    ep,
                    "may not be specified when `port` is non-numeric",
                ));
            }
            if !is_valid_port_name(s) {
                errs.push(Error::invalid(
                    &fld_path.child("port"),
                    s.clone(),
                    "must be an IANA_SVC_NAME (at most 15 characters, matching regex [a-z0-9]([a-z0-9-]*[a-z0-9])* and it must contain at least one letter [a-z])",
                ));
            }
        }
        Some(_) => errs.push(Error::invalid(
            &fld_path.child("port"),
            "<non-port>".to_string(),
            "must be an integer or string",
        )),
    }

    errs
}

/// IPBlock validation. Mirrors the syntactic half of upstream `ValidateIPBlock`.
fn validate_ip_block(ipb: &IPBlock, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if ipb.cidr.is_empty() {
        errs.push(Error::required(&fld_path.child("cidr"), ""));
        return errs;
    }
    if !is_valid_cidr(&ipb.cidr) {
        errs.push(Error::invalid(
            &fld_path.child("cidr"),
            ipb.cidr.clone(),
            "must be a valid CIDR (e.g. 10.0.0.0/8)",
        ));
    }
    if let Some(except) = &ipb.except {
        for (i, ex) in except.iter().enumerate() {
            if !is_valid_cidr(ex) {
                errs.push(Error::invalid(
                    &fld_path.child("except").index(i),
                    ex.clone(),
                    "must be a valid CIDR (e.g. 10.0.0.0/8)",
                ));
            }
        }
    }
    errs
}

/// Peer validation. Mirrors upstream `ValidateNetworkPolicyPeer`.
fn validate_peer(peer: &NetworkPolicyPeer, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut num_peers = 0;

    if let Some(ps) = &peer.pod_selector {
        num_peers += 1;
        errs.extend(validate_label_selector(
            ps,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("podSelector"),
        ));
    }
    if let Some(ns) = &peer.namespace_selector {
        num_peers += 1;
        errs.extend(validate_label_selector(
            ns,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("namespaceSelector"),
        ));
    }
    if let Some(ipb) = &peer.ip_block {
        num_peers += 1;
        errs.extend(validate_ip_block(ipb, &fld_path.child("ipBlock")));
    }

    if num_peers == 0 {
        errs.push(Error::required(fld_path, "must specify a peer"));
    } else if num_peers > 1 && peer.ip_block.is_some() {
        errs.push(Error::forbidden(
            fld_path,
            "may not specify both ipBlock and another peer",
        ));
    }

    errs
}

/// Validate a `NetworkPolicySpec`. Mirrors upstream `ValidateNetworkPolicySpec`.
pub fn validate_network_policy_spec(spec: &NetworkPolicySpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    errs.extend(validate_label_selector(
        &spec.pod_selector,
        LabelSelectorValidationOptions::default(),
        &fld_path.child("podSelector"),
    ));

    if let Some(ingress) = &spec.ingress {
        for (i, rule) in ingress.iter().enumerate() {
            let rule_path = fld_path.child("ingress").index(i);
            if let Some(ports) = &rule.ports {
                for (j, p) in ports.iter().enumerate() {
                    errs.extend(validate_port(p, &rule_path.child("ports").index(j)));
                }
            }
            if let Some(from) = &rule.from {
                for (j, peer) in from.iter().enumerate() {
                    errs.extend(validate_peer(peer, &rule_path.child("from").index(j)));
                }
            }
        }
    }
    if let Some(egress) = &spec.egress {
        for (i, rule) in egress.iter().enumerate() {
            let rule_path = fld_path.child("egress").index(i);
            if let Some(ports) = &rule.ports {
                for (j, p) in ports.iter().enumerate() {
                    errs.extend(validate_port(p, &rule_path.child("ports").index(j)));
                }
            }
            if let Some(to) = &rule.to {
                for (j, peer) in to.iter().enumerate() {
                    errs.extend(validate_peer(peer, &rule_path.child("to").index(j)));
                }
            }
        }
    }

    // policyTypes: at most two, each Ingress or Egress.
    if let Some(types) = &spec.policy_types {
        if types.len() > 2 {
            errs.push(Error::invalid(
                &fld_path.child("policyTypes"),
                types.join(","),
                "may not specify more than two policyTypes",
            ));
            return errs;
        }
        for (i, t) in types.iter().enumerate() {
            if t != "Ingress" && t != "Egress" {
                errs.push(Error::not_supported(
                    &fld_path.child("policyTypes").index(i),
                    t.clone(),
                    &["Ingress", "Egress"],
                ));
            }
        }
    }

    errs
}

/// Validate a new `NetworkPolicy`. Mirrors upstream `ValidateNetworkPolicy`.
pub fn validate_network_policy(np: &NetworkPolicy) -> ErrorList {
    validate_network_policy_spec(&np.spec, &Path::new("spec"))
}
