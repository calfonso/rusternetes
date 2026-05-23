//! JSON roundtrip tests for CRD `JSONSchemaProps` (OpenAPI v3 validation schema).
//!
//! Mirrors upstream `staging/src/k8s.io/apiextensions-apiserver/pkg/apis/`
//! `apiextensions/v1/types_jsonschema.go` and its testing layer at
//! `staging/src/k8s.io/apimachinery/pkg/api/apitesting/roundtrip/`.
//!
//! The fixtures exercise representative payloads that stress the recursive
//! schema shape: at least 4 levels of property nesting, the trio of
//! `x-kubernetes-*` boolean extensions, `oneOf`/`anyOf`/`allOf` arrays of
//! sub-schemas, `default` / `example` with non-trivial JSON, a heterogeneous
//! `enum` (string, number, null, object) and a `not` clause.
//!
//! For each fixture we assert the same four properties as
//! `roundtrip_core_v1.rs`:
//!
//!   1. `serde_json::from_str::<T>(&fixture)` succeeds (initial decode)
//!   2. `serde_json::to_string(&decoded)` succeeds (re-encode)
//!   3. `serde_json::from_str::<T>(&re_encoded)` succeeds (second decode)
//!   4. The two decoded values are equal (compared via `serde_json::Value`)
//!
//! In addition, several tests assert specific load-bearing fields survived the
//! roundtrip — e.g. that `x-kubernetes-preserve-unknown-fields: true` is still
//! `Some(true)` after the trip, and that the deeply-nested leaf property keeps
//! its `type` and `format`. This catches regressions where serde flattening or
//! `skip_serializing_if` predicates silently drop data.

use rusternetes_common::resources::{
    JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
};
use serde::{de::DeserializeOwned, Serialize};

/// Run the four-step roundtrip assertion for a typed payload.
///
/// 1. decode fixture -> `T`
/// 2. encode `T` -> JSON
/// 3. decode JSON -> `T`
/// 4. compare the two decoded `T`s via their `serde_json::Value` projection
///
/// We compare as `Value` rather than via `PartialEq` because the goal of the
/// layer is to verify the *wire* shape survives — that's exactly what
/// Value-equality measures (and `JSONSchemaProps` does derive `PartialEq`, but
/// the Value comparison gives a much more readable diff on failure).
fn assert_roundtrip<T>(fixture: &str)
where
    T: Serialize + DeserializeOwned,
{
    let decoded: T = serde_json::from_str(fixture)
        .unwrap_or_else(|e| panic!("initial decode failed: {e}\nfixture: {fixture}"));
    let re_encoded = serde_json::to_string(&decoded).expect("re-encode failed");
    let re_decoded: T = serde_json::from_str(&re_encoded)
        .unwrap_or_else(|e| panic!("second decode failed: {e}\nre_encoded: {re_encoded}"));

    let decoded_value = serde_json::to_value(&decoded).expect("decoded -> Value");
    let re_decoded_value = serde_json::to_value(&re_decoded).expect("re_decoded -> Value");
    assert_eq!(
        decoded_value, re_decoded_value,
        "roundtrip not stable\nfirst:  {decoded_value}\nsecond: {re_decoded_value}",
    );
}

// =============================================================================
// 1. Deep property nesting — at least 4 levels
// =============================================================================

/// Exercises `properties.spec.properties.template.items.properties.containers`
/// — 5 levels of nesting (spec -> template -> items -> array element ->
/// containers). This is the canonical shape every PodSpec-like CRD has, so a
/// regression here would torch most real CRDs.
#[test]
fn roundtrip_jsonschema_deep_nested_properties() {
    let fixture = r#"{
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "template": {
                        "type": "object",
                        "properties": {
                            "containers": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "image": {"type": "string"},
                                        "ports": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "containerPort": {
                                                        "type": "integer",
                                                        "format": "int32"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    // Reach into the typed tree and confirm the deepest leaf survived intact —
    // this guards against a silent loss of any intermediate `properties` map.
    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let containers = decoded
        .properties
        .as_ref()
        .unwrap()
        .get("spec")
        .unwrap()
        .properties
        .as_ref()
        .unwrap()
        .get("template")
        .unwrap()
        .properties
        .as_ref()
        .unwrap()
        .get("containers")
        .unwrap();

    let items = containers.items.as_ref().expect("containers.items");
    match items.as_ref() {
        JSONSchemaPropsOrArray::Schema(item_schema) => {
            let ports = item_schema
                .properties
                .as_ref()
                .unwrap()
                .get("ports")
                .unwrap();
            let port_items = ports.items.as_ref().expect("ports.items");
            match port_items.as_ref() {
                JSONSchemaPropsOrArray::Schema(port_item) => {
                    let container_port = port_item
                        .properties
                        .as_ref()
                        .unwrap()
                        .get("containerPort")
                        .unwrap();
                    assert_eq!(container_port.type_.as_deref(), Some("integer"));
                    assert_eq!(container_port.format.as_deref(), Some("int32"));
                }
                JSONSchemaPropsOrArray::Schemas(_) => {
                    panic!("ports.items should be a single Schema, not an array");
                }
            }
        }
        JSONSchemaPropsOrArray::Schemas(_) => {
            panic!("containers.items should be a single Schema, not an array");
        }
    }
}

// =============================================================================
// 2. x-kubernetes-preserve-unknown-fields: true
// =============================================================================

#[test]
fn roundtrip_jsonschema_preserve_unknown_fields_true() {
    let fixture = r#"{
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true,
        "properties": {
            "data": {
                "type": "object",
                "x-kubernetes-preserve-unknown-fields": true
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    assert_eq!(decoded.x_kubernetes_preserve_unknown_fields, Some(true));
    let data = decoded.properties.as_ref().unwrap().get("data").unwrap();
    assert_eq!(data.x_kubernetes_preserve_unknown_fields, Some(true));

    // The wire form must keep the dashed key, not the snake_case Rust field
    // name — otherwise the api-server emits a schema that no real K8s client
    // can parse.
    let re_encoded = serde_json::to_string(&decoded).unwrap();
    assert!(
        re_encoded.contains("\"x-kubernetes-preserve-unknown-fields\":true"),
        "expected dashed key in wire form, got: {re_encoded}",
    );
}

// =============================================================================
// 3. x-kubernetes-int-or-string: true
// =============================================================================

#[test]
fn roundtrip_jsonschema_int_or_string_true() {
    let fixture = r#"{
        "type": "object",
        "properties": {
            "port": {
                "x-kubernetes-int-or-string": true
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let port = decoded.properties.as_ref().unwrap().get("port").unwrap();
    assert_eq!(port.x_kubernetes_int_or_string, Some(true));

    let re_encoded = serde_json::to_string(&decoded).unwrap();
    assert!(
        re_encoded.contains("\"x-kubernetes-int-or-string\":true"),
        "expected dashed key in wire form, got: {re_encoded}",
    );
}

// =============================================================================
// 4. x-kubernetes-embedded-resource: true
// =============================================================================

#[test]
fn roundtrip_jsonschema_embedded_resource_true() {
    let fixture = r#"{
        "type": "object",
        "properties": {
            "template": {
                "type": "object",
                "x-kubernetes-embedded-resource": true,
                "x-kubernetes-preserve-unknown-fields": true,
                "properties": {
                    "metadata": {"type": "object"},
                    "spec": {"type": "object"}
                }
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let template = decoded
        .properties
        .as_ref()
        .unwrap()
        .get("template")
        .unwrap();
    assert_eq!(template.x_kubernetes_embedded_resource, Some(true));
    assert_eq!(template.x_kubernetes_preserve_unknown_fields, Some(true));
}

// =============================================================================
// 5. oneOf / anyOf / allOf arrays of JSONSchemaProps
// =============================================================================

#[test]
fn roundtrip_jsonschema_oneof_anyof_allof() {
    let fixture = r#"{
        "type": "object",
        "properties": {
            "value": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "integer"},
                    {"type": "null"}
                ],
                "anyOf": [
                    {"required": ["foo"]},
                    {"required": ["bar"]}
                ],
                "allOf": [
                    {"type": "object"},
                    {"required": ["id"]}
                ]
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let value = decoded.properties.as_ref().unwrap().get("value").unwrap();

    let one_of = value.one_of.as_ref().expect("oneOf");
    assert_eq!(one_of.len(), 3);
    assert_eq!(one_of[0].type_.as_deref(), Some("string"));
    assert_eq!(one_of[1].type_.as_deref(), Some("integer"));
    assert_eq!(one_of[2].type_.as_deref(), Some("null"));

    let any_of = value.any_of.as_ref().expect("anyOf");
    assert_eq!(any_of.len(), 2);
    assert_eq!(
        any_of[0].required.as_deref(),
        Some(&["foo".to_string()][..])
    );
    assert_eq!(
        any_of[1].required.as_deref(),
        Some(&["bar".to_string()][..])
    );

    let all_of = value.all_of.as_ref().expect("allOf");
    assert_eq!(all_of.len(), 2);
    assert_eq!(all_of[0].type_.as_deref(), Some("object"));
    assert_eq!(all_of[1].required.as_deref(), Some(&["id".to_string()][..]));
}

// =============================================================================
// 6. default / example with non-trivial JSON payloads
// =============================================================================

/// `default` and `example` are typed as `serde_json::Value` in our model
/// (matching upstream's `JSON` / `RawExtension` shape). They must survive
/// arbitrary nested JSON: arrays, objects, numbers, nulls.
#[test]
fn roundtrip_jsonschema_default_and_example() {
    let fixture = r#"{
        "type": "object",
        "properties": {
            "replicas": {
                "type": "integer",
                "default": 3,
                "example": 5
            },
            "selector": {
                "type": "object",
                "default": {"matchLabels": {"app": "demo"}},
                "example": {"matchLabels": {"app": "example", "tier": "frontend"}}
            },
            "args": {
                "type": "array",
                "default": ["--verbose", "--port=8080"],
                "example": ["--debug", "--port=9090", "--config=/etc/foo.yaml"]
            },
            "raw": {
                "default": {
                    "nested": {
                        "arr": [1, 2, {"k": "v", "n": null}],
                        "bool": false,
                        "num": 1.5
                    }
                }
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let replicas = decoded
        .properties
        .as_ref()
        .unwrap()
        .get("replicas")
        .unwrap();
    assert_eq!(replicas.default, Some(serde_json::json!(3)));
    assert_eq!(replicas.example, Some(serde_json::json!(5)));

    let selector = decoded
        .properties
        .as_ref()
        .unwrap()
        .get("selector")
        .unwrap();
    assert_eq!(
        selector.default,
        Some(serde_json::json!({"matchLabels": {"app": "demo"}})),
    );

    let raw = decoded.properties.as_ref().unwrap().get("raw").unwrap();
    assert_eq!(
        raw.default,
        Some(serde_json::json!({
            "nested": {
                "arr": [1, 2, {"k": "v", "n": null}],
                "bool": false,
                "num": 1.5
            }
        })),
    );
}

// =============================================================================
// 7. Heterogeneous enum: string, number, null, object
// =============================================================================

/// Upstream `JSONSchemaProps.Enum` is `[]JSON` (raw JSON values) — anything
/// goes. Our model uses `Vec<serde_json::Value>`. This test forces every JSON
/// scalar shape through the roundtrip plus an object, which is the case that
/// historically catches `untagged` enum bugs.
#[test]
fn roundtrip_jsonschema_heterogeneous_enum() {
    let fixture = r#"{
        "type": "object",
        "properties": {
            "anything": {
                "enum": ["a", 1, null, {"k": "v"}, [1, 2, 3], 2.5, true]
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let anything = decoded
        .properties
        .as_ref()
        .unwrap()
        .get("anything")
        .unwrap();
    let enum_ = anything.enum_.as_ref().expect("enum");
    assert_eq!(enum_.len(), 7);
    assert_eq!(enum_[0], serde_json::json!("a"));
    assert_eq!(enum_[1], serde_json::json!(1));
    assert_eq!(enum_[2], serde_json::Value::Null);
    assert_eq!(enum_[3], serde_json::json!({"k": "v"}));
    assert_eq!(enum_[4], serde_json::json!([1, 2, 3]));
    assert_eq!(enum_[5], serde_json::json!(2.5));
    assert_eq!(enum_[6], serde_json::json!(true));
}

// =============================================================================
// 8. not: { type: "string" }
// =============================================================================

#[test]
fn roundtrip_jsonschema_not_clause() {
    let fixture = r#"{
        "type": "object",
        "properties": {
            "value": {
                "not": {"type": "string"}
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let value = decoded.properties.as_ref().unwrap().get("value").unwrap();
    let not = value.not.as_ref().expect("not");
    assert_eq!(not.type_.as_deref(), Some("string"));
}

/// Nested `not` — a `not` whose body is itself a non-trivial schema with its
/// own properties. The `Box<JSONSchemaProps>` indirection is the place where
/// recursion bugs surface; this forces the whole chain to roundtrip.
#[test]
fn roundtrip_jsonschema_not_with_nested_schema() {
    let fixture = r#"{
        "type": "object",
        "not": {
            "type": "object",
            "properties": {
                "forbidden": {"type": "string"}
            },
            "required": ["forbidden"]
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let not = decoded.not.as_ref().expect("not");
    assert_eq!(not.type_.as_deref(), Some("object"));
    assert_eq!(
        not.required.as_deref(),
        Some(&["forbidden".to_string()][..])
    );
    let forbidden = not.properties.as_ref().unwrap().get("forbidden").unwrap();
    assert_eq!(forbidden.type_.as_deref(), Some("string"));
}

// =============================================================================
// Combined kitchen-sink: every feature in one fixture
// =============================================================================

/// Real CRDs from the wild combine all of the above in a single schema. This
/// catches interactions — e.g. a `default` inside a `oneOf` branch, or
/// `x-kubernetes-preserve-unknown-fields` on a deeply-nested property.
#[test]
fn roundtrip_jsonschema_kitchen_sink() {
    let fixture = r#"{
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true,
        "required": ["spec"],
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 100,
                        "default": 1
                    },
                    "strategy": {
                        "x-kubernetes-int-or-string": true,
                        "anyOf": [
                            {"type": "integer"},
                            {"type": "string", "pattern": "^[0-9]+%$"}
                        ]
                    },
                    "template": {
                        "type": "object",
                        "x-kubernetes-embedded-resource": true,
                        "x-kubernetes-preserve-unknown-fields": true,
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "containers": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "required": ["name"],
                                            "properties": {
                                                "name": {
                                                    "type": "string",
                                                    "enum": ["a", "b", 1, null]
                                                },
                                                "policy": {
                                                    "not": {"type": "null"}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "oneOf": [
                    {"required": ["replicas"]},
                    {"required": ["template"]}
                ],
                "allOf": [
                    {"type": "object"}
                ]
            }
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    // Spot-check the most easily-dropped fields after the roundtrip.
    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    assert_eq!(decoded.x_kubernetes_preserve_unknown_fields, Some(true));

    let spec = decoded.properties.as_ref().unwrap().get("spec").unwrap();
    assert_eq!(spec.one_of.as_ref().map(|v| v.len()), Some(2));
    assert_eq!(spec.all_of.as_ref().map(|v| v.len()), Some(1));

    let template = spec.properties.as_ref().unwrap().get("template").unwrap();
    assert_eq!(template.x_kubernetes_embedded_resource, Some(true));
    assert_eq!(template.x_kubernetes_preserve_unknown_fields, Some(true));
}

// =============================================================================
// additionalProperties as schema vs bool — JSONSchemaPropsOrBool union
// =============================================================================

/// `additionalProperties` can be either `true` / `false` or a sub-schema. Both
/// forms must roundtrip without coercing one into the other.
#[test]
fn roundtrip_jsonschema_additional_properties_bool() {
    let fixture = r#"{
        "type": "object",
        "additionalProperties": false
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let ap = decoded.additional_properties.as_ref().expect("ap");
    match ap.as_ref() {
        JSONSchemaPropsOrBool::Bool(b) => assert!(!b),
        JSONSchemaPropsOrBool::Schema(_) => {
            panic!("additionalProperties: false must decode as Bool, not Schema");
        }
    }
}

#[test]
fn roundtrip_jsonschema_additional_properties_schema() {
    let fixture = r#"{
        "type": "object",
        "additionalProperties": {
            "type": "string",
            "maxLength": 64
        }
    }"#;
    assert_roundtrip::<JSONSchemaProps>(fixture);

    let decoded: JSONSchemaProps = serde_json::from_str(fixture).unwrap();
    let ap = decoded.additional_properties.as_ref().expect("ap");
    match ap.as_ref() {
        JSONSchemaPropsOrBool::Schema(s) => {
            assert_eq!(s.type_.as_deref(), Some("string"));
            assert_eq!(s.max_length, Some(64));
        }
        JSONSchemaPropsOrBool::Bool(_) => {
            panic!("additionalProperties as schema must decode as Schema, not Bool");
        }
    }
}
