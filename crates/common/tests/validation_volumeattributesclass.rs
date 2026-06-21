//! Tests for VolumeAttributesClass validation (upstream ValidateVolumeAttributesClass).

use rusternetes_common::resources::csi::VolumeAttributesClass;
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::volumeattributesclass::validate_volume_attributes_class;
use std::collections::HashMap;

fn vac(driver: &str, params: Option<HashMap<String, String>>) -> VolumeAttributesClass {
    let mut v = VolumeAttributesClass {
        type_meta: Default::default(),
        metadata: Default::default(),
        driver_name: driver.to_string(),
        parameters: params,
    };
    v.metadata.name = "gold".to_string();
    v
}

fn one_param() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("iops".to_string(), "1000".to_string());
    m
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field == field)
}

#[test]
fn valid_vac_passes() {
    assert!(
        validate_volume_attributes_class(&vac("csi.example.com", Some(one_param()))).is_empty()
    );
}

#[test]
fn empty_driver_name_rejected() {
    let errs = validate_volume_attributes_class(&vac("", Some(one_param())));
    assert!(errs
        .iter()
        .any(|e| e.field == "driverName" && e.error_type == ErrorType::Required));
}

#[test]
fn invalid_driver_name_rejected() {
    assert!(has(
        &validate_volume_attributes_class(&vac("Bad Driver!", Some(one_param()))),
        "driverName"
    ));
}

#[test]
fn missing_parameters_rejected() {
    // allowEmpty=false → parameters required.
    let errs = validate_volume_attributes_class(&vac("csi.example.com", None));
    assert!(errs
        .iter()
        .any(|e| e.field == "parameters" && e.error_type == ErrorType::Required));
}

#[test]
fn empty_parameters_rejected() {
    let errs = validate_volume_attributes_class(&vac("csi.example.com", Some(HashMap::new())));
    assert!(errs
        .iter()
        .any(|e| e.field == "parameters" && e.error_type == ErrorType::Required));
}

#[test]
fn empty_parameter_key_rejected() {
    let mut m = HashMap::new();
    m.insert(String::new(), "v".to_string());
    assert!(has(
        &validate_volume_attributes_class(&vac("csi.example.com", Some(m))),
        "parameters"
    ));
}
