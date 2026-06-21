//! Node validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateNode` (release-1.35).
//!
//! Covers `spec.taints` (`validateNodeTaints`) and `spec.podCIDRs` (valid CIDRs
//! plus dual-stack one-per-family). ObjectMeta is validated separately by the
//! handler (see #1087 / #1277); node status, resources and swap are out of scope.

use crate::resources::node::Node;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_valid_label_value, validate_label_name};
use std::collections::HashSet;
use std::net::IpAddr;

const TAINT_EFFECTS: [&str; 3] = ["NoSchedule", "PreferNoSchedule", "NoExecute"];

#[derive(PartialEq, Eq, Clone, Copy)]
enum IpFamily {
    V4,
    V6,
}

fn parse_cidr(cidr: &str) -> Option<IpFamily> {
    let (ip, prefix) = cidr.split_once('/')?;
    let prefix: u8 = prefix.parse().ok()?;
    match ip.parse::<IpAddr>().ok()? {
        IpAddr::V4(_) if prefix <= 32 => Some(IpFamily::V4),
        IpAddr::V6(_) if prefix <= 128 => Some(IpFamily::V6),
        _ => None,
    }
}

/// Validate a `Node` on create. Mirrors upstream `ValidateNode`'s taints +
/// podCIDRs checks (minus ObjectMeta / status / resources).
pub fn validate_node(node: &Node) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(spec) = &node.spec else {
        return errs;
    };

    // spec.taints — key (label name), value (label value), effect enum, unique
    // by (key, effect).
    if let Some(taints) = &spec.taints {
        let taints_path = Path::new("spec").child("taints");
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for (i, taint) in taints.iter().enumerate() {
            let tp = taints_path.index(i);
            errs.extend(validate_label_name(&taint.key, &tp.child("key")));
            let value = taint.value.as_deref().unwrap_or("");
            for msg in is_valid_label_value(value) {
                errs.push(Error::invalid(&tp.child("value"), value.to_string(), msg));
            }
            if !TAINT_EFFECTS.contains(&taint.effect.as_str()) {
                errs.push(Error::not_supported(
                    &tp.child("effect"),
                    taint.effect.clone(),
                    &TAINT_EFFECTS,
                ));
            }
            if !seen.insert((taint.key.clone(), taint.effect.clone())) {
                let mut e = Error::duplicate(&tp, taint.key.clone());
                e.detail = "taints must be unique by key and effect pair".to_string();
                errs.push(e);
            }
        }
    }

    // spec.podCIDRs — each a valid CIDR; >1 must be a dual-stack pair.
    if let Some(cidrs) = &spec.pod_cidrs {
        if !cidrs.is_empty() {
            let cidrs_path = Path::new("spec").child("podCIDRs");
            let mut families: Vec<Option<IpFamily>> = Vec::with_capacity(cidrs.len());
            for (i, c) in cidrs.iter().enumerate() {
                let fam = parse_cidr(c);
                if fam.is_none() {
                    errs.push(Error::invalid(
                        &cidrs_path.index(i),
                        c.clone(),
                        "must be a valid CIDR value, (e.g. 10.9.8.0/24 or 2001:db8::/64)",
                    ));
                }
                families.push(fam);
            }
            if cidrs.len() > 1 {
                let dual_stack = cidrs.len() == 2
                    && matches!((families[0], families[1]), (Some(a), Some(b)) if a != b);
                if !dual_stack {
                    errs.push(Error::invalid(
                        &cidrs_path,
                        cidrs.join(","),
                        "may specify no more than one CIDR for each IP family",
                    ));
                }
            }
        }
    }

    errs
}
