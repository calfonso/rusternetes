use rusternetes_common::resources::csi::VolumeAttributesClass;
use rusternetes_common::validation::volumeattributesclass::validate_volume_attributes_class_update;
use serde_json::json;

fn vac(driver: &str, params: serde_json::Value) -> VolumeAttributesClass {
    serde_json::from_value(json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "VolumeAttributesClass",
        "metadata": {"name": "vac"},
        "driverName": driver,
        "parameters": params
    }))
    .unwrap()
}

#[test]
fn unchanged_passes() {
    let old = vac("csi.example.com", json!({"iops": "100"}));
    let new = vac("csi.example.com", json!({"iops": "100"}));
    assert!(validate_volume_attributes_class_update(&new, &old).is_empty());
}

#[test]
fn driver_name_immutable() {
    let old = vac("csi.example.com", json!({"iops": "100"}));
    let new = vac("csi.other.com", json!({"iops": "100"}));
    let errs = validate_volume_attributes_class_update(&new, &old);
    assert!(
        errs.iter().any(|e| e.to_string().contains("driverName")),
        "{errs:?}"
    );
}

#[test]
fn parameters_immutable() {
    let old = vac("csi.example.com", json!({"iops": "100"}));
    let new = vac("csi.example.com", json!({"iops": "200"}));
    let errs = validate_volume_attributes_class_update(&new, &old);
    assert!(
        errs.iter().any(|e| e.to_string().contains("parameters")),
        "{errs:?}"
    );
}
