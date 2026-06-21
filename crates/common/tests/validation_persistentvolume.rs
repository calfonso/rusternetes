//! Tests for PersistentVolume field validation.
//!
//! Mirrors the core of upstream `ValidatePersistentVolume`
//! (`pkg/apis/core/validation/validation.go`, release-1.35).

use rusternetes_common::resources::volume::PersistentVolume;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::persistentvolume::validate_persistent_volume;
use serde_json::json;

fn pv(spec: serde_json::Value) -> PersistentVolume {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv"},
        "spec": spec
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_pv_passes() {
    let errs = validate_persistent_volume(&pv(json!({
        "capacity": {"storage": "10Gi"},
        "accessModes": ["ReadWriteOnce"],
        "hostPath": {"path": "/data"}
    })));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn missing_storage_capacity_required() {
    let errs = validate_persistent_volume(&pv(json!({
        "capacity": {},
        "accessModes": ["ReadWriteOnce"],
        "hostPath": {"path": "/data"}
    })));
    assert!(
        has(&errs, "spec.capacity.storage", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn empty_access_modes_required() {
    let errs = validate_persistent_volume(&pv(json!({
        "capacity": {"storage": "10Gi"},
        "accessModes": [],
        "hostPath": {"path": "/data"}
    })));
    assert!(
        has(&errs, "spec.accessModes", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn rwoncepod_with_others_forbidden() {
    let errs = validate_persistent_volume(&pv(json!({
        "capacity": {"storage": "10Gi"},
        "accessModes": ["ReadWriteOncePod", "ReadWriteOnce"],
        "hostPath": {"path": "/data"}
    })));
    assert!(
        has(&errs, "spec.accessModes", ErrorType::Forbidden),
        "got: {errs:?}"
    );
}

#[test]
fn no_volume_source_required() {
    let errs = validate_persistent_volume(&pv(json!({
        "capacity": {"storage": "10Gi"},
        "accessModes": ["ReadWriteOnce"]
    })));
    assert!(has(&errs, "spec", ErrorType::Required), "got: {errs:?}");
}

#[test]
fn multiple_volume_sources_forbidden() {
    let errs = validate_persistent_volume(&pv(json!({
        "capacity": {"storage": "10Gi"},
        "accessModes": ["ReadWriteOnce"],
        "hostPath": {"path": "/data"},
        "nfs": {"server": "10.0.0.1", "path": "/exports"}
    })));
    assert!(has(&errs, "spec", ErrorType::Forbidden), "got: {errs:?}");
}

#[test]
fn invalid_storage_class_name_rejected() {
    let errs = validate_persistent_volume(&pv(json!({
        "capacity": {"storage": "10Gi"},
        "accessModes": ["ReadWriteOnce"],
        "hostPath": {"path": "/data"},
        "storageClassName": "Bad_Class"
    })));
    assert!(
        has(&errs, "spec.storageClassName", ErrorType::Invalid),
        "got: {errs:?}"
    );
}
