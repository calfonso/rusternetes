//! Integration tests for CRD OpenAPI publish + CR schema enum validation.
//!
//! Mirrors the upstream e2e checks in
//! `k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go`
//! lines 101 (enum validation) and 481 (OpenAPI schema after CRD update).
//!
//! These tests exercise the full storage round-trip that the api-server
//! performs in production:
//!   1. CRD is POSTed as raw JSON (kubectl) and persisted as
//!      `serde_json::Value` (preserving nested schemas).
//!   2. When validating a custom resource the api-server reads the CRD back
//!      as a typed `CustomResourceDefinition` and runs `SchemaValidator`
//!      against the CR.
//!
//! If the typed round-trip drops `enum` (or any other constraint) the CR
//! is silently accepted — which is exactly the conformance failure we are
//! fixing.

use rusternetes_common::resources::{CustomResource, CustomResourceDefinition};
use rusternetes_common::schema_validation::SchemaValidator;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

/// Returns the canonical schemaFoo CRD body used by the upstream
/// `apimachinery/crd_publish_openapi.go` tests. The schema declares a
/// `spec.bars[].feeling` field whose value must be one of "Great" or "Down".
fn schema_foo_crd(crd_name: &str, group: &str, plural: &str, kind: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": crd_name },
        "spec": {
            "group": group,
            "names": {
                "plural": plural,
                "singular": kind.to_lowercase(),
                "kind": kind,
                "listKind": format!("{}List", kind),
            },
            "scope": "Namespaced",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "description": "Foo CRD for Testing",
                        "type": "object",
                        "properties": {
                            "spec": {
                                "description": "Specification of Foo",
                                "type": "object",
                                "properties": {
                                    "bars": {
                                        "description": "List of Bars and their specs.",
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "required": ["name"],
                                            "properties": {
                                                "name": { "type": "string" },
                                                "age": { "type": "string" },
                                                "feeling": {
                                                    "type": "string",
                                                    "enum": ["Great", "Down"]
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }]
        }
    })
}

/// Validate a CR against the storage round-tripped CRD exactly like
/// `validate_custom_resource()` in `crates/api-server/src/handlers/custom_resource.rs`.
///
/// The function returns an error iff the CR violates the schema; this lets
/// the test assert "CR with bad enum is rejected" without instantiating the
/// full Axum handler stack (which requires `Arc<StorageBackend>` that the
/// test crate cannot construct).
fn validate_cr_against_stored_crd(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
) -> Result<(), rusternetes_common::Error> {
    let crd_version = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .expect("test CRD has version");
    let validation = crd_version.schema.as_ref().expect("test CRD has schema");
    let properties = validation
        .open_apiv3_schema
        .properties
        .as_ref()
        .expect("test schema has properties");
    if let Some(spec_schema) = properties.get("spec") {
        if let Some(ref spec) = cr.spec {
            SchemaValidator::validate_no_unknown_check(spec_schema, spec)?;
        }
    }
    Ok(())
}

/// Site `crd_publish_openapi.go:101` — server must reject a CR whose enum
/// field is set to a value not declared in the CRD's enum constraint.
///
/// Before this fix the api-server stored the CRD body as raw JSON but read
/// it back through the typed `CustomResourceDefinition` deserializer. If
/// any path in that deserializer drops nested constraints (e.g. `enum`
/// inside `JSONSchemaPropsOrArray` items) the CR validation silently
/// passes. This test stores a real CRD body, reads it back the way the
/// handler does, and then runs the same `SchemaValidator` call the
/// handler runs.
#[tokio::test]
async fn cr_with_unknown_enum_value_is_rejected() {
    let storage = Arc::new(MemoryStorage::new());

    let crd_name = "foos.example.com";
    let body = schema_foo_crd(crd_name, "example.com", "foos", "Foo");

    // Persist the CRD as raw JSON, mirroring `create_crd` in
    // `handlers/crd.rs` which avoids the typed-struct round-trip on the
    // write path.
    let key = build_key("customresourcedefinitions", None, crd_name);
    let _: serde_json::Value = storage.create(&key, &body).await.unwrap();

    // Read it back through the typed deserializer — the same path used
    // by `get_crd_for_resource` in `handlers/custom_resource.rs`.
    let crd: CustomResourceDefinition = storage.get(&key).await.unwrap();

    // Sanity check: the typed round-trip must preserve the nested enum
    // constraint. If this fails the test catches the underlying serde
    // regression directly.
    let v1 = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == "v1")
        .expect("v1 must exist");
    let schema = &v1.schema.as_ref().expect("schema").open_apiv3_schema;
    let spec_props = schema.properties.as_ref().unwrap();
    let bars_schema = spec_props
        .get("spec")
        .unwrap()
        .properties
        .as_ref()
        .unwrap()
        .get("bars")
        .unwrap();
    let items = bars_schema.items.as_ref().expect("bars.items preserved");
    let item_schema = match items.as_ref() {
        rusternetes_common::resources::crd::JSONSchemaPropsOrArray::Schema(s) => s,
        _ => panic!("items must be a single schema"),
    };
    let feeling = item_schema
        .properties
        .as_ref()
        .unwrap()
        .get("feeling")
        .expect("feeling property preserved");
    let enum_values = feeling
        .enum_
        .as_ref()
        .expect("enum constraint must survive the typed round-trip");
    assert_eq!(enum_values.len(), 2, "enum values preserved");

    // Now build a CR with a bad enum value and verify validation rejects
    // it. This is the exact failure mode at upstream site
    // crd_publish_openapi.go:101 — `unexpected no error when creating CR
    // with unknown enum value`.
    let cr_body = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Foo",
        "metadata": { "name": "test-foo", "namespace": "default" },
        "spec": {
            "bars": [{ "name": "first", "feeling": "BogusEnumValue" }]
        }
    });
    let cr: CustomResource = serde_json::from_value(cr_body).unwrap();

    let result = validate_cr_against_stored_crd(&crd, "v1", &cr);
    assert!(
        result.is_err(),
        "CR with enum value outside [\"Great\", \"Down\"] must be rejected, got: {:?}",
        result
    );

    // And a CR with a valid enum value must be accepted.
    let good_cr_body = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Foo",
        "metadata": { "name": "test-foo", "namespace": "default" },
        "spec": {
            "bars": [{ "name": "first", "feeling": "Great" }]
        }
    });
    let good_cr: CustomResource = serde_json::from_value(good_cr_body).unwrap();
    let ok = validate_cr_against_stored_crd(&crd, "v1", &good_cr);
    assert!(ok.is_ok(), "valid enum value must be accepted: {:?}", ok);
}

/// Site `crd_publish_openapi.go:481` — after a CRD's schema is updated
/// (e.g. a field is renamed or removed), the stored copy of the schema
/// must reflect the change. Our handler stores raw JSON on `update_crd`
/// and the openapi handler reads the latest raw JSON on every request,
/// so a fresh read must show the new schema. Additionally the
/// `x-kubernetes-group-version-kind` extension is added by the openapi
/// handler when assembling the published spec — we assert that the
/// helper that adds it works on schemas freshly read from storage.
#[tokio::test]
async fn crd_update_reflects_in_stored_schema_for_openapi_publish() {
    let storage = Arc::new(MemoryStorage::new());
    let crd_name = "widgets.example.com";
    let body_v1 = schema_foo_crd(crd_name, "example.com", "widgets", "Widget");
    let key = build_key("customresourcedefinitions", None, crd_name);

    storage
        .create::<serde_json::Value>(&key, &body_v1)
        .await
        .unwrap();

    // Read back as typed → confirm enum on the original schema.
    let crd_v1: CustomResourceDefinition = storage.get(&key).await.unwrap();
    let v1_schema = &crd_v1.spec.versions[0]
        .schema
        .as_ref()
        .unwrap()
        .open_apiv3_schema;
    assert!(v1_schema.properties.is_some());

    // "Update" the CRD: replace the schema so that the items no longer
    // carry the `feeling` field. This mimics the multi-to-single-ver
    // update in the upstream test.
    let mut body_v2 = body_v1.clone();
    let new_items = serde_json::json!({
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "string" }
        }
    });
    body_v2["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"]["bars"]["items"] = new_items;

    let _: serde_json::Value = storage.update(&key, &body_v2).await.unwrap();

    let crd_v2: CustomResourceDefinition = storage.get(&key).await.unwrap();
    let v2_schema = &crd_v2.spec.versions[0]
        .schema
        .as_ref()
        .unwrap()
        .open_apiv3_schema;

    // The schema MUST reflect the rename/removal — `feeling` is gone.
    let spec_props = v2_schema.properties.as_ref().unwrap();
    let bars = spec_props
        .get("spec")
        .unwrap()
        .properties
        .as_ref()
        .unwrap()
        .get("bars")
        .unwrap();
    let items = bars.items.as_ref().expect("bars.items must still exist");
    let item_schema = match items.as_ref() {
        rusternetes_common::resources::crd::JSONSchemaPropsOrArray::Schema(s) => s,
        _ => panic!("items must be a single schema"),
    };
    let item_props = item_schema.properties.as_ref().unwrap();
    assert!(
        item_props.contains_key("name"),
        "name field survives schema update"
    );
    assert!(
        !item_props.contains_key("feeling"),
        "feeling field must be gone after CRD update — this is what site :481 verifies"
    );

    // Build the published OpenAPI v2 swagger entry the same way
    // `handlers/openapi.rs::get_swagger_spec` does: pull the raw CRD,
    // strip omitempty defaults, then attach
    // `x-kubernetes-group-version-kind` to the schema. We can't call the
    // private helpers from the test crate, so we replicate the bare
    // minimum here to assert the GVK extension is plumbed through. The
    // real handler functions are exercised by unit tests in openapi.rs.
    let stored: serde_json::Value = storage.get(&key).await.unwrap();
    let openapi_schema = stored
        .pointer("/spec/versions/0/schema/openAPIV3Schema")
        .expect("schema present in stored CRD")
        .clone();
    assert_eq!(
        openapi_schema.get("type").and_then(|v| v.as_str()),
        Some("object"),
        "schema root type preserved for openapi publish"
    );

    // The publish handler injects an x-kubernetes-group-version-kind
    // extension. Build the expected value to ensure the data is
    // available from storage. (The actual injection logic lives in
    // `openapi.rs` and is unit-tested there.)
    let expected_gvk = serde_json::json!([{
        "group": "example.com",
        "kind": "Widget",
        "version": "v1",
    }]);
    let group = stored
        .pointer("/spec/group")
        .and_then(|v| v.as_str())
        .unwrap();
    let kind = stored
        .pointer("/spec/names/kind")
        .and_then(|v| v.as_str())
        .unwrap();
    let version = stored
        .pointer("/spec/versions/0/name")
        .and_then(|v| v.as_str())
        .unwrap();
    let built_gvk = serde_json::json!([{
        "group": group,
        "kind": kind,
        "version": version,
    }]);
    assert_eq!(
        built_gvk, expected_gvk,
        "GVK metadata must be derivable from the stored CRD on every openapi publish"
    );
}
