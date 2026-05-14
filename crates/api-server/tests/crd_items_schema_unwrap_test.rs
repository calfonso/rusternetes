//! Tests for CRD items schema unwrapping in OpenAPI v2 generation.
//!
//! K8s CRD schemas store "items" as {"schema": {...}} (Go's JSONSchemaPropsOrArray
//! serialization). OpenAPI v2 expects "items" to be a direct schema object.
//! The fix in ecb67b7 unwraps that wrapper inside strip_false_extensions.

use rusternetes_api_server::handlers::openapi::strip_false_extensions;

/// Verify that items wrapped as {"schema": {...}} are unwrapped to the inner schema.
/// This is the shape produced by Go's JSONSchemaPropsOrArray serialization when a CRD
/// has a single-schema items field (the common case).
#[test]
fn test_items_schema_wrapper_is_unwrapped() {
    let inner = serde_json::json!({
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        }
    });

    // Wrapped form: what K8s CRD storage produces
    let mut schema = serde_json::json!({
        "type": "array",
        "items": {
            "schema": inner.clone()
        }
    });

    strip_false_extensions(&mut schema);

    // After unwrapping, items must equal the inner schema directly —
    // NOT the {"schema": {...}} wrapper.
    let items = schema.get("items").expect("items must be present");
    assert_eq!(
        items, &inner,
        "items should be unwrapped to the inner schema, not the {{\"schema\": ...}} wrapper"
    );
    assert!(
        items.get("schema").is_none(),
        "unwrapped items must not have a nested 'schema' key"
    );
    assert_eq!(
        items.get("type").and_then(|v| v.as_str()),
        Some("object"),
        "unwrapped items.type should be 'object'"
    );
}

/// Verify that deeply-nested items wrappers are also unwrapped (recursion).
#[test]
fn test_nested_array_items_schema_wrapper_is_unwrapped() {
    let leaf_inner = serde_json::json!({"type": "string"});

    let mut schema = serde_json::json!({
        "type": "object",
        "properties": {
            "tags": {
                "type": "array",
                "items": {
                    "schema": leaf_inner.clone()
                }
            }
        }
    });

    strip_false_extensions(&mut schema);

    let items = schema
        .pointer("/properties/tags/items")
        .expect("nested items must be present");

    assert_eq!(
        items, &leaf_inner,
        "nested items should be unwrapped to the inner schema"
    );
    assert!(
        items.get("schema").is_none(),
        "unwrapped nested items must not have a nested 'schema' key"
    );
}

/// Verify that items that are already a direct schema (no wrapper) are left unchanged.
/// This ensures we don't double-unwrap schemas that are already in the correct form.
#[test]
fn test_direct_items_schema_is_not_modified() {
    let direct_items = serde_json::json!({
        "type": "string"
    });

    let mut schema = serde_json::json!({
        "type": "array",
        "items": direct_items.clone()
    });

    strip_false_extensions(&mut schema);

    let items = schema.get("items").expect("items must be present");
    assert_eq!(
        items, &direct_items,
        "direct (non-wrapped) items should be left unchanged"
    );
}

/// Verify that items with multiple keys (not the single-key {"schema"} pattern)
/// are not modified — they are not a JSONSchemaPropsOrArray wrapper.
#[test]
fn test_items_with_multiple_keys_is_not_unwrapped() {
    let items_with_two_keys = serde_json::json!({
        "schema": {"type": "string"},
        "type": "object"   // two keys → not a wrapper
    });

    let mut schema = serde_json::json!({
        "type": "array",
        "items": items_with_two_keys.clone()
    });

    strip_false_extensions(&mut schema);

    let items = schema.get("items").expect("items must be present");
    assert_eq!(
        items, &items_with_two_keys,
        "items with multiple keys should not be unwrapped"
    );
}
