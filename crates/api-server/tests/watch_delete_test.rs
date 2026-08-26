//! Tests for watch DELETE event delivery (Conformance failure #4).
//!
//! Verifies that DELETE events from watch streams are always delivered,
//! even when the prev_kv (previous value) is empty or cannot be deserialized
//! into the typed struct. This is critical for tests like
//! "should complete a service status lifecycle" where the watcher must see
//! the DELETED event after a service is deleted.

use rusternetes_api_server::handlers::watch::{build_delete_fallback_json, extract_rv_from_json};

/// Test that extract_rv_from_json extracts resourceVersion from valid JSON.
#[test]
fn test_extract_rv_from_valid_json() {
    let json = r#"{"metadata":{"name":"test","resourceVersion":"42"}}"#;
    assert_eq!(extract_rv_from_json(json), Some("42".to_string()));
}

/// Test that extract_rv_from_json returns None for empty JSON.
#[test]
fn test_extract_rv_from_empty_json() {
    assert_eq!(extract_rv_from_json(""), None);
}

/// Test that extract_rv_from_json returns None for JSON without resourceVersion.
#[test]
fn test_extract_rv_from_json_without_rv() {
    let json = r#"{"metadata":{"name":"test"}}"#;
    assert_eq!(extract_rv_from_json(json), None);
}

/// Test that build_delete_fallback_json constructs valid DELETE event from raw JSON.
#[test]
fn test_build_delete_fallback_from_valid_json() {
    let prev_value =
        r#"{"metadata":{"name":"my-svc","namespace":"default","resourceVersion":"10"},"spec":{}}"#;
    let key = "/registry/services/default/my-svc";

    let json = build_delete_fallback_json(key, prev_value).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"].as_str(), Some("DELETED"));
    assert_eq!(
        parsed["object"]["metadata"]["name"].as_str(),
        Some("my-svc")
    );
    assert_eq!(
        parsed["object"]["metadata"]["namespace"].as_str(),
        Some("default")
    );
}

/// Test that build_delete_fallback_json constructs DELETE event from empty prev_value
/// by extracting name/namespace from the key.
#[test]
fn test_build_delete_fallback_from_empty_prev_value() {
    let key = "/registry/services/default/my-svc";

    let json = build_delete_fallback_json(key, "").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"].as_str(), Some("DELETED"));
    assert_eq!(
        parsed["object"]["metadata"]["name"].as_str(),
        Some("my-svc")
    );
    assert_eq!(
        parsed["object"]["metadata"]["namespace"].as_str(),
        Some("default")
    );
}

/// Test that build_delete_fallback_json handles cluster-scoped keys
/// (no namespace in key path).
#[test]
fn test_build_delete_fallback_cluster_scoped() {
    let key = "/registry/nodes/node-1";

    let json = build_delete_fallback_json(key, "").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"].as_str(), Some("DELETED"));
    assert_eq!(
        parsed["object"]["metadata"]["name"].as_str(),
        Some("node-1")
    );
}

/// Test that build_delete_fallback_json preserves the full object from prev_value
/// when it's valid JSON (even if it can't be typed-deserialized).
#[test]
fn test_build_delete_fallback_preserves_full_object() {
    // JSON that represents a Service with extra unknown fields that might
    // fail typed deserialization but is valid JSON.
    let prev_value = r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"test-svc","namespace":"ns","resourceVersion":"55","labels":{"app":"web"}},"spec":{"clusterIP":"10.0.0.1"}}"#;
    let key = "/registry/services/ns/test-svc";

    let json = build_delete_fallback_json(key, prev_value).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"].as_str(), Some("DELETED"));
    assert_eq!(parsed["object"]["kind"].as_str(), Some("Service"));
    assert_eq!(
        parsed["object"]["metadata"]["labels"]["app"].as_str(),
        Some("web")
    );
    assert_eq!(
        parsed["object"]["spec"]["clusterIP"].as_str(),
        Some("10.0.0.1")
    );
}

/// Test that build_delete_fallback_json handles minimal metadata-only JSON
/// (like what etcd constructs when prev_kv is missing after compaction).
#[test]
fn test_build_delete_fallback_minimal_metadata() {
    let prev_value = r#"{"metadata":{"name":"svc","namespace":"default","resourceVersion":"100"}}"#;
    let key = "/registry/services/default/svc";

    let json = build_delete_fallback_json(key, prev_value).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["type"].as_str(), Some("DELETED"));
    assert_eq!(parsed["object"]["metadata"]["name"].as_str(), Some("svc"));
    assert_eq!(
        parsed["object"]["metadata"]["resourceVersion"].as_str(),
        Some("100")
    );
}
