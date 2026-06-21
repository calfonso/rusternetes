use rusternetes_common::resources::ipaddress::IPAddress;
use rusternetes_common::validation::ipaddress::validate_ip_address;
use serde_json::json;

fn ip(parent_ref: serde_json::Value) -> IPAddress {
    serde_json::from_value(json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IPAddress",
        "metadata": {"name": "10.0.0.1"},
        "spec": {"parentRef": parent_ref}
    }))
    .unwrap()
}

#[test]
fn valid_parent_ref_passes() {
    let ok = ip(json!({"resource": "services", "name": "kubernetes", "namespace": "default"}));
    assert!(validate_ip_address(&ok).is_empty());
}

#[test]
fn missing_spec_requires_parent_ref() {
    let no_spec: IPAddress = serde_json::from_value(json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "IPAddress",
        "metadata": {"name": "10.0.0.1"}
    }))
    .unwrap();
    let errs = validate_ip_address(&no_spec);
    assert!(
        errs.iter().any(|e| e.to_string().contains("parentRef")),
        "{errs:?}"
    );
}

#[test]
fn empty_resource_required() {
    let errs = validate_ip_address(&ip(json!({"resource": "", "name": "kubernetes"})));
    assert!(
        errs.iter().any(|e| e.to_string().contains("resource")),
        "{errs:?}"
    );
}

#[test]
fn empty_name_required() {
    let errs = validate_ip_address(&ip(json!({"resource": "services", "name": ""})));
    assert!(
        errs.iter().any(|e| e.to_string().contains("name")),
        "{errs:?}"
    );
}

#[test]
fn invalid_group_rejected() {
    let errs = validate_ip_address(&ip(json!({
        "group": "Bad Group", "resource": "services", "name": "kubernetes"
    })));
    assert!(
        errs.iter().any(|e| e.to_string().contains("group")),
        "{errs:?}"
    );
}

#[test]
fn invalid_namespace_path_segment_rejected() {
    let errs = validate_ip_address(&ip(json!({
        "resource": "services", "name": "kubernetes", "namespace": "bad/ns"
    })));
    assert!(
        errs.iter().any(|e| e.to_string().contains("namespace")),
        "{errs:?}"
    );
}

#[test]
fn empty_group_skipped() {
    // core group is the empty string; must not error.
    let ok = ip(json!({"group": "", "resource": "services", "name": "kubernetes"}));
    assert!(validate_ip_address(&ok).is_empty());
}
