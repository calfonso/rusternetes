//! Tests for Node validation (port of upstream ValidateNode taints + podCIDRs).

use rusternetes_common::resources::node::{Node, NodeSpec, Taint};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::node::validate_node;

fn node(spec: NodeSpec) -> Node {
    let mut n = Node {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec: Some(spec),
        status: None,
    };
    n.metadata.name = "node-1".to_string();
    n
}

fn taint(key: &str, value: Option<&str>, effect: &str) -> Taint {
    Taint {
        key: key.to_string(),
        value: value.map(|s| s.to_string()),
        effect: effect.to_string(),
        time_added: None,
    }
}

fn spec_taints(taints: Vec<Taint>) -> NodeSpec {
    NodeSpec {
        pod_cidr: None,
        pod_cidrs: None,
        provider_id: None,
        unschedulable: None,
        taints: Some(taints),
    }
}

fn spec_cidrs(cidrs: Vec<&str>) -> NodeSpec {
    NodeSpec {
        pod_cidr: None,
        pod_cidrs: Some(cidrs.into_iter().map(|s| s.to_string()).collect()),
        provider_id: None,
        unschedulable: None,
        taints: None,
    }
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field))
}

#[test]
fn empty_spec_ok() {
    assert!(validate_node(&node(NodeSpec {
        pod_cidr: None,
        pod_cidrs: None,
        provider_id: None,
        unschedulable: None,
        taints: None
    }))
    .is_empty());
}

#[test]
fn valid_taint_passes() {
    assert!(validate_node(&node(spec_taints(vec![taint(
        "dedicated",
        Some("gpu"),
        "NoSchedule"
    )])))
    .is_empty());
}

#[test]
fn bad_taint_key_rejected() {
    assert!(has(
        &validate_node(&node(spec_taints(vec![taint(
            "bad key!",
            None,
            "NoSchedule"
        )]))),
        "taints[0].key"
    ));
}

#[test]
fn bad_taint_value_rejected() {
    assert!(has(
        &validate_node(&node(spec_taints(vec![taint(
            "k",
            Some("bad value!"),
            "NoSchedule"
        )]))),
        "taints[0].value"
    ));
}

#[test]
fn bad_taint_effect_rejected() {
    let errs = validate_node(&node(spec_taints(vec![taint("k", None, "Banish")])));
    assert!(errs
        .iter()
        .any(|e| e.field.contains("taints[0].effect") && e.error_type == ErrorType::NotSupported));
}

#[test]
fn duplicate_taint_key_effect_rejected() {
    let errs = validate_node(&node(spec_taints(vec![
        taint("k", Some("a"), "NoSchedule"),
        taint("k", Some("b"), "NoSchedule"),
    ])));
    assert!(errs.iter().any(|e| e.error_type == ErrorType::Duplicate));
}

#[test]
fn same_key_different_effect_ok() {
    assert!(validate_node(&node(spec_taints(vec![
        taint("k", None, "NoSchedule"),
        taint("k", None, "NoExecute"),
    ])))
    .is_empty());
}

#[test]
fn valid_single_podcidr_passes() {
    assert!(validate_node(&node(spec_cidrs(vec!["10.244.0.0/24"]))).is_empty());
}

#[test]
fn valid_dual_stack_podcidrs_pass() {
    assert!(validate_node(&node(spec_cidrs(vec!["10.244.0.0/24", "2001:db8::/64"]))).is_empty());
}

#[test]
fn invalid_podcidr_rejected() {
    assert!(has(
        &validate_node(&node(spec_cidrs(vec!["nonsense"]))),
        "spec.podCIDRs[0]"
    ));
}

#[test]
fn same_family_podcidrs_rejected() {
    let errs = validate_node(&node(spec_cidrs(vec!["10.244.0.0/24", "10.0.0.0/8"])));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.podCIDRs" && e.detail.contains("no more than one CIDR")));
}
