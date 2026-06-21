//! Tests for StorageClass update validation (upstream ValidateStorageClassUpdate).

use rusternetes_common::resources::volume::{
    PersistentVolumeReclaimPolicy, StorageClass, VolumeBindingMode,
};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::storageclass::validate_storage_class_update;
use std::collections::HashMap;

fn sc() -> StorageClass {
    StorageClass {
        type_meta: Default::default(),
        metadata: Default::default(),
        provisioner: "kubernetes.io/aws-ebs".to_string(),
        parameters: Some(HashMap::from([("type".to_string(), "gp3".to_string())])),
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: Some(false),
        mount_options: None,
    }
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter()
        .any(|e| e.field == field && e.error_type == ErrorType::Forbidden)
}

#[test]
fn identical_update_ok() {
    assert!(validate_storage_class_update(&sc(), &sc()).is_empty());
}

#[test]
fn allow_volume_expansion_change_ok() {
    let old = sc();
    let mut new = sc();
    new.allow_volume_expansion = Some(true);
    assert!(validate_storage_class_update(&new, &old).is_empty());
}

#[test]
fn provisioner_change_rejected() {
    let old = sc();
    let mut new = sc();
    new.provisioner = "kubernetes.io/gce-pd".to_string();
    assert!(has(
        &validate_storage_class_update(&new, &old),
        "provisioner"
    ));
}

#[test]
fn parameters_change_rejected() {
    let old = sc();
    let mut new = sc();
    new.parameters = Some(HashMap::from([("type".to_string(), "io2".to_string())]));
    assert!(has(
        &validate_storage_class_update(&new, &old),
        "parameters"
    ));
}

#[test]
fn reclaim_policy_change_rejected() {
    let old = sc();
    let mut new = sc();
    new.reclaim_policy = Some(PersistentVolumeReclaimPolicy::Retain);
    assert!(has(
        &validate_storage_class_update(&new, &old),
        "reclaimPolicy"
    ));
}

#[test]
fn volume_binding_mode_change_rejected() {
    let old = sc();
    let mut new = sc();
    new.volume_binding_mode = Some(VolumeBindingMode::WaitForFirstConsumer);
    assert!(has(
        &validate_storage_class_update(&new, &old),
        "volumeBindingMode"
    ));
}
