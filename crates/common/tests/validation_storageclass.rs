//! Tests for StorageClass validation (port of upstream `ValidateStorageClass`).

use rusternetes_common::resources::volume::{
    PersistentVolumeReclaimPolicy, StorageClass, VolumeBindingMode,
};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::storageclass::validate_storage_class;
use std::collections::HashMap;

fn valid_sc() -> StorageClass {
    StorageClass {
        type_meta: Default::default(),
        metadata: Default::default(),
        provisioner: "kubernetes.io/aws-ebs".to_string(),
        parameters: None,
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    }
}

fn has(errs: &[rusternetes_common::validation::field::Error], substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(substr))
}

#[test]
fn valid_sc_passes() {
    let errs = validate_storage_class(&valid_sc());
    assert!(errs.is_empty(), "{:?}", errs);
}

#[test]
fn empty_provisioner_rejected() {
    let mut sc = valid_sc();
    sc.provisioner = String::new();
    let errs = validate_storage_class(&sc);
    assert!(errs
        .iter()
        .any(|e| e.field == "provisioner" && e.error_type == ErrorType::Required));
}

#[test]
fn invalid_provisioner_name_rejected() {
    let mut sc = valid_sc();
    sc.provisioner = "Bad Provisioner!".to_string();
    assert!(has(&validate_storage_class(&sc), "provisioner"));
}

#[test]
fn recycle_reclaim_policy_rejected() {
    let mut sc = valid_sc();
    sc.reclaim_policy = Some(PersistentVolumeReclaimPolicy::Recycle);
    let errs = validate_storage_class(&sc);
    assert!(errs
        .iter()
        .any(|e| e.field == "reclaimPolicy" && e.error_type == ErrorType::NotSupported));
}

#[test]
fn retain_reclaim_policy_ok() {
    let mut sc = valid_sc();
    sc.reclaim_policy = Some(PersistentVolumeReclaimPolicy::Retain);
    assert!(validate_storage_class(&sc).is_empty());
}

#[test]
fn missing_volume_binding_mode_rejected() {
    let mut sc = valid_sc();
    sc.volume_binding_mode = None;
    let errs = validate_storage_class(&sc);
    assert!(errs
        .iter()
        .any(|e| e.field == "volumeBindingMode" && e.error_type == ErrorType::Required));
}

#[test]
fn empty_parameter_key_rejected() {
    let mut sc = valid_sc();
    let mut p = HashMap::new();
    p.insert(String::new(), "v".to_string());
    sc.parameters = Some(p);
    assert!(has(&validate_storage_class(&sc), "parameters"));
}

#[test]
fn too_many_parameters_rejected() {
    let mut sc = valid_sc();
    let mut p = HashMap::new();
    for i in 0..513 {
        p.insert(format!("k{i}"), "v".to_string());
    }
    sc.parameters = Some(p);
    let errs = validate_storage_class(&sc);
    assert!(errs
        .iter()
        .any(|e| e.field == "parameters" && e.error_type == ErrorType::TooLong));
}

#[test]
fn oversize_parameters_rejected() {
    let mut sc = valid_sc();
    let mut p = HashMap::new();
    // One huge value pushes the combined key+value size past 256 KiB.
    p.insert("big".to_string(), "x".repeat(256 * 1024 + 1));
    sc.parameters = Some(p);
    let errs = validate_storage_class(&sc);
    assert!(errs
        .iter()
        .any(|e| e.field == "parameters" && e.error_type == ErrorType::TooLong));
}

// --- allowedTopologies (#1330) ---
use rusternetes_common::resources::volume::{
    TopologySelectorLabelRequirement, TopologySelectorTerm,
};

fn term(key: &str, values: &[&str]) -> TopologySelectorTerm {
    TopologySelectorTerm {
        match_label_expressions: Some(vec![TopologySelectorLabelRequirement {
            key: key.to_string(),
            values: values.iter().map(|s| s.to_string()).collect(),
        }]),
    }
}

#[test]
fn valid_allowed_topologies_pass() {
    let mut sc = valid_sc();
    sc.allowed_topologies = Some(vec![term("topology.kubernetes.io/zone", &["us-east-1a"])]);
    assert!(
        validate_storage_class(&sc).is_empty(),
        "{:?}",
        validate_storage_class(&sc)
    );
}

#[test]
fn topology_empty_values_rejected() {
    let mut sc = valid_sc();
    sc.allowed_topologies = Some(vec![term("topology.kubernetes.io/zone", &[])]);
    let errs = validate_storage_class(&sc);
    assert!(errs
        .iter()
        .any(|e| e.field.contains("matchLabelExpressions[0].values")
            && e.error_type == ErrorType::Required));
}

#[test]
fn topology_bad_key_rejected() {
    let mut sc = valid_sc();
    sc.allowed_topologies = Some(vec![term("bad key!", &["v"])]);
    let errs = validate_storage_class(&sc);
    assert!(errs
        .iter()
        .any(|e| e.field.contains("matchLabelExpressions[0].key")));
}

#[test]
fn topology_duplicate_values_rejected() {
    let mut sc = valid_sc();
    sc.allowed_topologies = Some(vec![term("topology.kubernetes.io/zone", &["a", "a"])]);
    let errs = validate_storage_class(&sc);
    assert!(errs.iter().any(|e| e.error_type == ErrorType::Duplicate));
}

#[test]
fn topology_duplicate_terms_rejected() {
    let mut sc = valid_sc();
    let t = term("topology.kubernetes.io/zone", &["us-east-1a"]);
    sc.allowed_topologies = Some(vec![t.clone(), t]);
    let errs = validate_storage_class(&sc);
    assert!(errs.iter().any(|e| e
        .field
        .contains("allowedTopologies[1].matchLabelExpressions")
        && e.error_type == ErrorType::Duplicate));
}
