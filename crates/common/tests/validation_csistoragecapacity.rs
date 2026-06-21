//! Tests for CSIStorageCapacity validation (upstream ValidateCSIStorageCapacity).

use rusternetes_common::resources::csi::CSIStorageCapacity;
use rusternetes_common::resources::volume::{LabelSelector, LabelSelectorRequirement};
use rusternetes_common::validation::csistoragecapacity::validate_csi_storage_capacity;

fn csc(sc_name: &str, capacity: Option<&str>) -> CSIStorageCapacity {
    let mut c = CSIStorageCapacity {
        type_meta: Default::default(),
        metadata: Default::default(),
        storage_class_name: sc_name.to_string(),
        capacity: capacity.map(|s| s.to_string()),
        maximum_volume_size: None,
        node_topology: None,
    };
    c.metadata.name = "csc-1".to_string();
    c
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field))
}

#[test]
fn valid_passes() {
    assert!(validate_csi_storage_capacity(&csc("fast-ssd", Some("100Gi"))).is_empty());
}

#[test]
fn empty_storage_class_name_rejected() {
    assert!(has(
        &validate_csi_storage_capacity(&csc("", None)),
        "storageClassName"
    ));
}

#[test]
fn invalid_storage_class_name_rejected() {
    assert!(has(
        &validate_csi_storage_capacity(&csc("Bad_Name", None)),
        "storageClassName"
    ));
}

#[test]
fn negative_capacity_rejected() {
    assert!(has(
        &validate_csi_storage_capacity(&csc("fast-ssd", Some("-1"))),
        "capacity"
    ));
}

#[test]
fn unparseable_capacity_rejected() {
    assert!(has(
        &validate_csi_storage_capacity(&csc("fast-ssd", Some("notaqty"))),
        "capacity"
    ));
}

#[test]
fn valid_node_topology_passes() {
    let mut c = csc("fast-ssd", Some("1Gi"));
    let mut ml = std::collections::HashMap::new();
    ml.insert(
        "topology.kubernetes.io/zone".to_string(),
        "us-east-1a".to_string(),
    );
    c.node_topology = Some(LabelSelector {
        match_labels: Some(ml),
        match_expressions: None,
    });
    assert!(
        validate_csi_storage_capacity(&c).is_empty(),
        "{:?}",
        validate_csi_storage_capacity(&c)
    );
}

#[test]
fn invalid_node_topology_operator_rejected() {
    let mut c = csc("fast-ssd", Some("1Gi"));
    c.node_topology = Some(LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: "zone".to_string(),
            operator: "Bogus".to_string(),
            values: Some(vec!["a".to_string()]),
        }]),
    });
    assert!(has(&validate_csi_storage_capacity(&c), "nodeTopology"));
}
