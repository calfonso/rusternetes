//! Tests for ConfigMap validation (port of upstream `ValidateConfigMap`).

use rusternetes_common::resources::ConfigMap;
use rusternetes_common::validation::configmap::validate_config_map;
use std::collections::HashMap;

fn cm() -> ConfigMap {
    ConfigMap {
        type_meta: Default::default(),
        metadata: Default::default(),
        data: None,
        binary_data: None,
        immutable: None,
    }
}

#[test]
fn valid_keys_pass() {
    let mut c = cm();
    let mut d = HashMap::new();
    d.insert("app.properties".to_string(), "x=1".to_string());
    d.insert("KEY_NAME-1".to_string(), "v".to_string());
    c.data = Some(d);
    assert!(validate_config_map(&c).is_empty());
}

#[test]
fn invalid_key_char_rejected() {
    let mut c = cm();
    let mut d = HashMap::new();
    d.insert("bad/key".to_string(), "v".to_string());
    c.data = Some(d);
    let errs = validate_config_map(&c);
    assert_eq!(errs.len(), 1);
    assert!(errs[0].field.contains("data"));
}

#[test]
fn dot_dot_key_rejected() {
    let mut c = cm();
    let mut d = HashMap::new();
    d.insert("..".to_string(), "v".to_string());
    c.data = Some(d);
    let errs = validate_config_map(&c);
    // ".." fails the chdir check (regex allows "." chars so it passes the regex).
    assert!(errs.iter().any(|e| e.detail.contains("'..'")));
}

#[test]
fn duplicate_across_bags_rejected() {
    let mut c = cm();
    let mut d = HashMap::new();
    d.insert("shared".to_string(), "v".to_string());
    c.data = Some(d);
    let mut b = HashMap::new();
    b.insert("shared".to_string(), vec![1u8, 2, 3]);
    c.binary_data = Some(b);
    let errs = validate_config_map(&c);
    assert!(errs
        .iter()
        .any(|e| e.detail.contains("duplicate of key present in binaryData")));
}

#[test]
fn invalid_binary_key_rejected() {
    let mut c = cm();
    let mut b = HashMap::new();
    b.insert("bad key".to_string(), vec![0u8]);
    c.binary_data = Some(b);
    let errs = validate_config_map(&c);
    assert_eq!(errs.len(), 1);
    assert!(errs[0].field.contains("binaryData"));
}

#[test]
fn empty_configmap_ok() {
    assert!(validate_config_map(&cm()).is_empty());
}

#[test]
fn oversize_configmap_rejected() {
    use rusternetes_common::validation::configmap::MAX_SECRET_SIZE;
    let mut c = cm();
    let mut d = HashMap::new();
    // Split the payload across data + binaryData to prove the cap is on the
    // combined total, not a single value.
    d.insert("a".to_string(), "x".repeat(MAX_SECRET_SIZE / 2));
    c.data = Some(d);
    let mut b = HashMap::new();
    b.insert("b".to_string(), vec![0u8; MAX_SECRET_SIZE / 2 + 1]);
    c.binary_data = Some(b);
    let errs = validate_config_map(&c);
    assert!(
        errs.iter()
            .any(|e| e.error_type == rusternetes_common::validation::field::ErrorType::TooLong),
        "expected a TooLong error, got: {:?}",
        errs
    );
}

#[test]
fn at_limit_configmap_ok() {
    use rusternetes_common::validation::configmap::MAX_SECRET_SIZE;
    let mut c = cm();
    let mut d = HashMap::new();
    d.insert("a".to_string(), "x".repeat(MAX_SECRET_SIZE));
    c.data = Some(d);
    // Exactly at the limit is allowed (upstream uses `>`, not `>=`).
    assert!(validate_config_map(&c)
        .iter()
        .all(|e| e.error_type != rusternetes_common::validation::field::ErrorType::TooLong));
}
