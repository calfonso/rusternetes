//! Tests for ControllerRevision validation (upstream ValidateControllerRevisionCreate).

use rusternetes_common::resources::ControllerRevision;
use rusternetes_common::validation::controllerrevision::validate_controller_revision;
use rusternetes_common::validation::field::ErrorType;
use serde_json::json;

fn cr(data: Option<serde_json::Value>) -> ControllerRevision {
    let mut c = ControllerRevision {
        type_meta: Default::default(),
        metadata: Default::default(),
        data,
        revision: 1,
    };
    c.metadata.name = "rev-1".to_string();
    c
}

#[test]
fn valid_object_data_passes() {
    assert!(validate_controller_revision(&cr(Some(json!({"spec": {"replicas": 3}})))).is_empty());
}

#[test]
fn missing_data_rejected() {
    let errs = validate_controller_revision(&cr(None));
    assert!(errs
        .iter()
        .any(|e| e.field == "data" && e.error_type == ErrorType::Required));
}

#[test]
fn null_data_rejected() {
    let errs = validate_controller_revision(&cr(Some(serde_json::Value::Null)));
    assert!(errs
        .iter()
        .any(|e| e.field == "data" && e.error_type == ErrorType::Required));
}

#[test]
fn non_object_data_rejected() {
    // a JSON array / scalar is not a valid object
    let errs = validate_controller_revision(&cr(Some(json!([1, 2, 3]))));
    assert!(errs
        .iter()
        .any(|e| e.field == "data" && e.detail.contains("valid JSON object")));
    let errs2 = validate_controller_revision(&cr(Some(json!("a string"))));
    assert!(errs2.iter().any(|e| e.field == "data"));
}

#[test]
fn negative_revision_rejected() {
    // Upstream `validateControllerRevision` (apps validation.go:336):
    // ValidateNonnegativeField(revision.Revision, ...).
    let mut c = cr(Some(json!({"spec": {"replicas": 3}})));
    c.revision = -1;
    let errs = validate_controller_revision(&c);
    assert!(
        errs.iter()
            .any(|e| e.field == "revision" && e.error_type == ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn zero_revision_ok() {
    let mut c = cr(Some(json!({"spec": {"replicas": 3}})));
    c.revision = 0;
    assert!(validate_controller_revision(&c).is_empty());
}
