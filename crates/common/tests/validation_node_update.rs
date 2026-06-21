use rusternetes_common::resources::Node;
use rusternetes_common::validation::node::validate_node_update;
use serde_json::json;

fn node(spec: serde_json::Value) -> Node {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": "n1"},
        "spec": spec
    }))
    .unwrap()
}

#[test]
fn assigning_pod_cidr_from_empty_is_allowed() {
    let old = node(json!({}));
    let new = node(json!({"podCIDRs": ["10.0.0.0/24"]}));
    assert!(
        validate_node_update(&new, &old).is_empty(),
        "empty -> set is allowed"
    );
}

#[test]
fn changing_pod_cidr_is_forbidden() {
    let old = node(json!({"podCIDRs": ["10.0.0.0/24"]}));
    let new = node(json!({"podCIDRs": ["10.0.1.0/24"]}));
    let errs = validate_node_update(&new, &old);
    assert!(
        errs.iter().any(|e| e.to_string().contains("podCIDR")),
        "{errs:?}"
    );
}

#[test]
fn assigning_provider_id_from_empty_is_allowed() {
    let old = node(json!({}));
    let new = node(json!({"providerID": "aws:///i-123"}));
    assert!(validate_node_update(&new, &old).is_empty());
}

#[test]
fn changing_provider_id_is_forbidden() {
    let old = node(json!({"providerID": "aws:///i-123"}));
    let new = node(json!({"providerID": "aws:///i-456"}));
    let errs = validate_node_update(&new, &old);
    assert!(
        errs.iter().any(|e| e.to_string().contains("providerID")),
        "{errs:?}"
    );
}

#[test]
fn mutable_fields_allowed() {
    let old = node(json!({"podCIDRs": ["10.0.0.0/24"], "providerID": "aws:///i-123"}));
    let new = node(json!({
        "podCIDRs": ["10.0.0.0/24"], "providerID": "aws:///i-123",
        "unschedulable": true,
        "taints": [{"key": "k", "effect": "NoSchedule"}]
    }));
    assert!(
        validate_node_update(&new, &old).is_empty(),
        "unschedulable/taints are mutable"
    );
}
