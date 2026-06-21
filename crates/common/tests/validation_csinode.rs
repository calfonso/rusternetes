//! Tests for CSINode validation (port of upstream `ValidateCSINode`).

use rusternetes_common::resources::csi::{
    CSINode, CSINodeDriver, CSINodeSpec, VolumeNodeResources,
};
use rusternetes_common::validation::csinode::validate_csi_node;
use rusternetes_common::validation::field::ErrorType;

fn driver(name: &str, node_id: &str) -> CSINodeDriver {
    CSINodeDriver {
        name: name.to_string(),
        node_id: node_id.to_string(),
        topology_keys: None,
        allocatable: None,
    }
}

fn node(drivers: Vec<CSINodeDriver>) -> CSINode {
    let mut n = CSINode {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec: CSINodeSpec { drivers },
    };
    n.metadata.name = "node-1".to_string();
    n
}

fn has(errs: &[rusternetes_common::validation::field::Error], field_substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field_substr))
}

#[test]
fn valid_csinode_passes() {
    assert!(validate_csi_node(&node(vec![driver("csi.example.com", "node-abc")])).is_empty());
}

#[test]
fn empty_drivers_ok() {
    assert!(validate_csi_node(&node(vec![])).is_empty());
}

#[test]
fn empty_driver_name_rejected() {
    let errs = validate_csi_node(&node(vec![driver("", "node-abc")]));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.drivers[0].name" && e.error_type == ErrorType::Required));
}

#[test]
fn bad_driver_name_rejected() {
    assert!(has(
        &validate_csi_node(&node(vec![driver("bad name!", "node-abc")])),
        "spec.drivers[0].name"
    ));
}

#[test]
fn missing_node_id_rejected() {
    let errs = validate_csi_node(&node(vec![driver("csi.example.com", "")]));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.drivers[0].nodeID" && e.error_type == ErrorType::Required));
}

#[test]
fn too_long_node_id_rejected() {
    let errs = validate_csi_node(&node(vec![driver("csi.example.com", &"a".repeat(193))]));
    assert!(has(&errs, "spec.drivers[0].nodeID"));
}

#[test]
fn duplicate_driver_name_rejected() {
    let errs = validate_csi_node(&node(vec![
        driver("csi.example.com", "id1"),
        driver("csi.example.com", "id2"),
    ]));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.drivers[1].name" && e.error_type == ErrorType::Duplicate));
}

#[test]
fn negative_allocatable_count_rejected() {
    let mut d = driver("csi.example.com", "id1");
    d.allocatable = Some(VolumeNodeResources { count: Some(-1) });
    assert!(has(
        &validate_csi_node(&node(vec![d])),
        "spec.drivers[0].allocatable.count"
    ));
}

#[test]
fn invalid_topology_key_rejected() {
    let mut d = driver("csi.example.com", "id1");
    d.topology_keys = Some(vec!["bad key!".to_string()]);
    assert!(has(&validate_csi_node(&node(vec![d])), "spec.drivers[0]"));
}

#[test]
fn duplicate_topology_key_rejected() {
    let mut d = driver("csi.example.com", "id1");
    d.topology_keys = Some(vec!["topology.kubernetes.io/zone".to_string(); 2]);
    let errs = validate_csi_node(&node(vec![d]));
    assert!(errs.iter().any(|e| e.error_type == ErrorType::Duplicate));
}
