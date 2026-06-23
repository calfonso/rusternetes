//! Tests for CSIDriver validation (port of upstream `ValidateCSIDriver`).

use rusternetes_common::resources::csi::{CSIDriver, CSIDriverSpec, TokenRequest};
use rusternetes_common::validation::csidriver::validate_csi_driver;
use rusternetes_common::validation::field::ErrorType;

fn driver(spec: CSIDriverSpec) -> CSIDriver {
    let mut d = CSIDriver {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec,
    };
    d.metadata.name = "csi.example.com".to_string();
    d
}

/// A post-default "valid" spec. The api-server handler defaults
/// `podInfoOnMount` and `storageCapacity` to `false` before validation, so a
/// real object always has them set. The pure validator now requires all three
/// presence fields (upstream `validateCSIDriverSpec`), so the fixture must
/// supply them to represent a defaulted, valid object.
fn defaulted_spec() -> CSIDriverSpec {
    CSIDriverSpec {
        attach_required: Some(true),
        pod_info_on_mount: Some(false),
        storage_capacity: Some(false),
        ..Default::default()
    }
}

fn tr(audience: &str, expiration: Option<i64>) -> TokenRequest {
    TokenRequest {
        audience: audience.to_string(),
        expiration_seconds: expiration,
    }
}

fn has(errs: &[rusternetes_common::validation::field::Error], field_substr: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field_substr))
}

#[test]
fn valid_driver_passes() {
    assert!(validate_csi_driver(&driver(defaulted_spec())).is_empty());
}

#[test]
fn missing_attach_required_rejected() {
    let spec = CSIDriverSpec {
        attach_required: None,
        ..Default::default()
    };
    let errs = validate_csi_driver(&driver(spec));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.attachedRequired" && e.error_type == ErrorType::Required));
}

#[test]
fn valid_token_requests_pass() {
    let mut spec = defaulted_spec();
    spec.token_requests = Some(vec![tr("aud1", Some(3600)), tr("aud2", None)]);
    assert!(validate_csi_driver(&driver(spec)).is_empty());
}

#[test]
fn duplicate_audience_rejected() {
    let mut spec = defaulted_spec();
    spec.token_requests = Some(vec![tr("same", Some(3600)), tr("same", Some(3600))]);
    let errs = validate_csi_driver(&driver(spec));
    assert!(errs
        .iter()
        .any(|e| e.field.contains("tokenRequests[1].audience")
            && e.error_type == ErrorType::Duplicate));
}

#[test]
fn expiration_below_min_rejected() {
    let mut spec = defaulted_spec();
    spec.token_requests = Some(vec![tr("aud", Some(599))]); // < 600s (10 min)
    assert!(has(
        &validate_csi_driver(&driver(spec)),
        "tokenRequests[0].expirationSeconds"
    ));
}

#[test]
fn expiration_at_min_ok() {
    let mut spec = defaulted_spec();
    spec.token_requests = Some(vec![tr("aud", Some(600))]);
    assert!(validate_csi_driver(&driver(spec)).is_empty());
}

#[test]
fn expiration_above_max_rejected() {
    let mut spec = defaulted_spec();
    spec.token_requests = Some(vec![tr("aud", Some((1i64 << 32) + 1))]);
    assert!(has(
        &validate_csi_driver(&driver(spec)),
        "tokenRequests[0].expirationSeconds"
    ));
}
