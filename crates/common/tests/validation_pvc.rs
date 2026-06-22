//! Tests for PersistentVolumeClaim field validation.
//!
//! Mirrors upstream `ValidatePersistentVolumeClaimSpec`
//! (`pkg/apis/core/validation/validation.go`, release-1.35).

use rusternetes_common::resources::volume::PersistentVolumeClaim;
use rusternetes_common::validation::field::{Error, ErrorType};
use rusternetes_common::validation::pvc::validate_persistent_volume_claim;
use serde_json::json;

fn pvc(spec: serde_json::Value) -> PersistentVolumeClaim {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "data", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn valid_spec() -> serde_json::Value {
    json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": "1Gi"}}
    })
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_pvc_passes() {
    let errs = validate_persistent_volume_claim(&pvc(valid_spec()));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn empty_access_modes_required() {
    let mut spec = valid_spec();
    spec["accessModes"] = json!([]);
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.accessModes", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn rwoncepod_with_others_forbidden() {
    let mut spec = valid_spec();
    spec["accessModes"] = json!(["ReadWriteOncePod", "ReadWriteOnce"]);
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.accessModes", ErrorType::Forbidden),
        "got: {errs:?}"
    );
}

#[test]
fn rwoncepod_alone_ok() {
    let mut spec = valid_spec();
    spec["accessModes"] = json!(["ReadWriteOncePod"]);
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn missing_storage_request_required() {
    let mut spec = valid_spec();
    spec["resources"] = json!({"requests": {}});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(
            &errs,
            "spec.resources.requests.storage",
            ErrorType::Required
        ),
        "got: {errs:?}"
    );
}

#[test]
fn zero_storage_request_rejected() {
    let mut spec = valid_spec();
    spec["resources"] = json!({"requests": {"storage": "0"}});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.resources.requests.storage", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

// Note: a syntactically-invalid quantity (e.g. "notaquantity") is rejected at
// deserialization by the resources-map quantity deserializer, before this
// validator runs — so the `Quantity::parse` Err arm is a defensive backstop the
// API path can't reach, and is not unit-tested through `from_value`.

#[test]
fn invalid_storage_class_name_rejected() {
    let mut spec = valid_spec();
    spec["storageClassName"] = json!("Bad_Class");
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.storageClassName", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn invalid_selector_match_expression_rejected() {
    let mut spec = valid_spec();
    spec["selector"] = json!({
        "matchExpressions": [{"key": "k", "operator": "BogusOp"}]
    });
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(
            &errs,
            "spec.selector.matchExpressions[0].operator",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

#[test]
fn valid_selector_ok() {
    let mut spec = valid_spec();
    spec["selector"] = json!({"matchLabels": {"app": "db"}});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn data_source_default_apigroup_requires_pvc_kind() {
    let mut spec = valid_spec();
    // Empty apiGroup with a non-PVC kind is invalid.
    spec["dataSource"] = json!({"kind": "Secret", "name": "src"});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.dataSource", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn data_source_pvc_kind_ok() {
    let mut spec = valid_spec();
    spec["dataSource"] = json!({"kind": "PersistentVolumeClaim", "name": "src"});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn data_source_missing_name_required() {
    let mut spec = valid_spec();
    spec["dataSource"] = json!({"kind": "PersistentVolumeClaim", "name": ""});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.dataSource.name", ErrorType::Required),
        "got: {errs:?}"
    );
}

#[test]
fn data_source_invalid_apigroup_rejected() {
    let mut spec = valid_spec();
    spec["dataSource"] = json!({"apiGroup": "Bad_Group", "kind": "Foo", "name": "src"});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.dataSource.apiGroup", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn data_source_ref_namespace_forbids_data_source() {
    let mut spec = valid_spec();
    spec["dataSource"] = json!({"kind": "PersistentVolumeClaim", "name": "src"});
    spec["dataSourceRef"] = json!({
        "kind": "PersistentVolumeClaim", "name": "src", "namespace": "other"
    });
    let errs = validate_persistent_volume_claim(&pvc(spec));
    // dataSource may not be set when dataSourceRef.namespace is specified.
    assert!(has(&errs, "spec", ErrorType::Invalid), "got: {errs:?}");
}

#[test]
fn data_source_must_match_data_source_ref() {
    let mut spec = valid_spec();
    spec["dataSource"] = json!({"kind": "PersistentVolumeClaim", "name": "a"});
    spec["dataSourceRef"] = json!({"kind": "PersistentVolumeClaim", "name": "b"});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(has(&errs, "spec", ErrorType::Invalid), "got: {errs:?}");
}

#[test]
fn data_source_equal_data_source_ref_ok() {
    let mut spec = valid_spec();
    spec["dataSource"] = json!({"kind": "PersistentVolumeClaim", "name": "a"});
    spec["dataSourceRef"] = json!({"kind": "PersistentVolumeClaim", "name": "a"});
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn data_source_ref_invalid_namespace_rejected() {
    let mut spec = valid_spec();
    spec["dataSourceRef"] = json!({
        "kind": "PersistentVolumeClaim", "name": "src", "namespace": "Bad_NS"
    });
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.dataSourceRef.namespace", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn invalid_volume_attributes_class_name_rejected() {
    let mut spec = valid_spec();
    spec["volumeAttributesClassName"] = json!("Bad_Class");
    let errs = validate_persistent_volume_claim(&pvc(spec));
    assert!(
        has(&errs, "spec.volumeAttributesClassName", ErrorType::Invalid),
        "got: {errs:?}"
    );
}
