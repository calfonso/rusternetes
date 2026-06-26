//! NetworkPolicy validation — port of upstream Kubernetes
//! `pkg/apis/networking/validation/validation.go::ValidateNetworkPolicySpec`
//! (release-1.35).
//!
//! ipBlock is fully validated: the cidr and each `except` are syntactically
//! valid CIDRs, and each `except` is a strict subset of the cidr (contained in
//! it, with a longer prefix) — mirroring upstream `ValidateIPBlock`.

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

/// Mask `bits` to its leading `prefix` bits within a `width`-bit address.
fn mask_bits(bits: u128, prefix: u32, width: u32) -> u128 {
    if prefix == 0 {
        return 0;
    }
    if prefix >= width {
        return bits;
    }
    let host = width - prefix;
    (bits >> host) << host
}

/// Parse a CIDR into `(network_address, prefix, is_v6)`, where the address is
/// already masked to the prefix. Returns `None` on any parse failure.
fn parse_cidr_network(s: &str) -> Option<(u128, u8, bool)> {
    let (ip, prefix) = s.split_once('/')?;
    let prefix: u8 = prefix.parse().ok()?;
    match IpAddr::from_str(ip).ok()? {
        IpAddr::V4(v4) if prefix <= 32 => {
            let bits = u32::from(v4) as u128;
            Some((mask_bits(bits, prefix as u32, 32), prefix, false))
        }
        IpAddr::V6(v6) if prefix <= 128 => {
            let bits = u128::from(v6);
            Some((mask_bits(bits, prefix as u32, 128), prefix, true))
        }
        _ => None,
    }
}

/// Syntactic CIDR validity: `IP/prefix`, prefix within the address family's
/// bit-width. Mirrors the validity half of upstream `IsValidCIDR`.
fn is_valid_cidr(s: &str) -> bool {
    parse_cidr_network(s).is_some()
}

/// True iff `except` is a *strict* subset of `cidr` — same family, contained in
/// the cidr's network, and with a strictly longer prefix. Mirrors upstream
/// `ValidateIPBlock`'s `!cidr.Contains(except.IP) || cidrMask >= exceptMask`.
fn is_strict_subset(except: &str, cidr: &str) -> bool {
    let Some((ex_net, ex_prefix, ex_v6)) = parse_cidr_network(except) else {
        return false;
    };
    let Some((cidr_net, cidr_prefix, cidr_v6)) = parse_cidr_network(cidr) else {
        return false;
    };
    if ex_v6 != cidr_v6 {
        return false;
    }
    let width = if cidr_v6 { 128 } else { 32 };
    let ex_within_cidr = mask_bits(ex_net, cidr_prefix as u32, width) == cidr_net;
    ex_within_cidr && ex_prefix > cidr_prefix
}

/// Upstream `IsValidPortName`: an IANA_SVC_NAME — a DNS-1123 label ≤15 chars
/// containing at least one letter.
fn is_valid_port_name(s: &str) -> bool {
    s.len() <= 15 && is_dns1123_label(s).is_empty() && s.chars().any(|c| c.is_ascii_alphabetic())
}

/// Port-level validation. Mirrors upstream `ValidateNetworkPolicyPort`.
fn validate_port(port: &NetworkPolicyPort, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if !matches!(port.protocol.as_str(), "TCP" | "UDP" | "SCTP") {
        errs.push(Error::not_supported(
            &fld_path.child("protocol"),
            port.protocol.clone(),
            &["TCP", "UDP", "SCTP"],
        ));
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
            } else if is_valid_cidr(&ipb.cidr) && !is_strict_subset(ex, &ipb.cidr) {
                errs.push(Error::invalid(
                    &fld_path.child("except").index(i),
                    ex.clone(),
                    "must be a strict subset of `cidr`",
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

#[cfg(test)]
mod ip_block_tests {
    use super::*;

    fn ipb_errs(json: serde_json::Value) -> Vec<String> {
        let ipb: IPBlock = serde_json::from_value(json).unwrap();
        validate_ip_block(&ipb, &Path::new("ipBlock"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_strict_subset_passes() {
        assert!(ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["10.1.0.0/16", "10.2.3.0/24"]
        }))
        .is_empty());
    }

    #[test]
    fn except_not_contained_rejected() {
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["192.168.0.0/16"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn except_equal_or_shorter_prefix_rejected() {
        // same prefix as cidr -> not strict (prefix must be longer)
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["10.0.0.0/8"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn ipv6_strict_subset_passes() {
        assert!(ipb_errs(serde_json::json!({
            "cidr": "2001:db8::/32", "except": ["2001:db8:1::/48"]
        }))
        .is_empty());
    }

    #[test]
    fn mixed_family_except_rejected() {
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["2001:db8::/64"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn invalid_cidr_still_rejected() {
        let e = ipb_errs(serde_json::json!({"cidr": "10.0.0.0/8", "except": ["notacidr"]}));
        assert!(e.iter().any(|m| m.contains("valid CIDR")), "{e:?}");
    }
}
