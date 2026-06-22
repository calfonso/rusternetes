//! Tests for PVC update validation (immutability subset of upstream
//! ValidatePersistentVolumeClaimUpdate).

use rusternetes_common::resources::volume::{
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    PersistentVolumeMode, ResourceRequirements,
};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::pvc::validate_persistent_volume_claim_update;
use std::collections::HashMap;

fn pvc(storage: &str, mode: Option<PersistentVolumeMode>) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), storage.to_string());
    PersistentVolumeClaim {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec: PersistentVolumeClaimSpec {
            // A real update target satisfies the create-time spec rules, which
            // `validate_persistent_volume_claim_update` re-runs (upstream
            // `ValidatePersistentVolumeClaimUpdate` calls `ValidatePersistentVolumeClaim`).
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                limits: None,
                requests: Some(requests),
            },
            volume_name: None,
            storage_class_name: None,
            volume_mode: mode,
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    }
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field == field)
}

#[test]
fn grow_storage_ok() {
    let old = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    let new = pvc("2Gi", Some(PersistentVolumeMode::Filesystem));
    assert!(validate_persistent_volume_claim_update(&new, &old).is_empty());
}

#[test]
fn equal_storage_ok() {
    let old = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    let new = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    assert!(validate_persistent_volume_claim_update(&new, &old).is_empty());
}

#[test]
fn shrink_storage_rejected() {
    let old = pvc("2Gi", Some(PersistentVolumeMode::Filesystem));
    let new = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    let errs = validate_persistent_volume_claim_update(&new, &old);
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.resources.requests.storage"
            && e.error_type == ErrorType::Forbidden));
}

#[test]
fn volume_mode_change_rejected() {
    let old = pvc("1Gi", Some(PersistentVolumeMode::Filesystem));
    let new = pvc("1Gi", Some(PersistentVolumeMode::Block));
    let errs = validate_persistent_volume_claim_update(&new, &old);
    assert!(errs
        .iter()
        .any(|e| e.field == "volumeMode" && e.error_type == ErrorType::Forbidden));
}

#[test]
fn same_volume_mode_ok() {
    let old = pvc("1Gi", Some(PersistentVolumeMode::Block));
    let new = pvc("1Gi", Some(PersistentVolumeMode::Block));
    assert!(validate_persistent_volume_claim_update(&new, &old).is_empty());
}

#[test]
fn shrink_with_units_detected() {
    // 1024Mi == 1Gi; 512Mi < 1Gi → shrink
    let old = pvc("1Gi", None);
    let new = pvc("512Mi", None);
    assert!(has(
        &validate_persistent_volume_claim_update(&new, &old),
        "spec.resources.requests.storage"
    ));
}
