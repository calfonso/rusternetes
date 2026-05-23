//! Gnostic OpenAPI v2 / v3 protobuf encoding.
//!
//! Converts JSON OpenAPI specs to gnostic Document protobuf format.
//! K8s client-go's OpenAPISchema() expects this format.
//! K8s ref: vendor/k8s.io/kube-openapi/pkg/handler/handler.go — ToProtoBinary()
//! K8s ref (v3): staging/src/k8s.io/client-go/openapi3 — negotiates
//!   `application/com.github.proto-openapi.spec.v3@v1.0+protobuf`.

#[allow(clippy::all, non_camel_case_types, dead_code, unused_imports)]
pub mod openapi_v2 {
    include!(concat!(env!("OUT_DIR"), "/openapi.v2.rs"));
}

#[allow(clippy::all, non_camel_case_types, dead_code, unused_imports)]
pub mod openapi_v3 {
    include!(concat!(env!("OUT_DIR"), "/openapi.v3.rs"));
}

use openapi_v2::*;
use prost::Message;

/// Convert a JSON swagger spec to gnostic protobuf bytes.
/// Equivalent to K8s ToProtoBinary: JSON → ParseDocument → proto.Marshal
pub fn swagger_json_to_protobuf(json_bytes: &[u8]) -> Result<Vec<u8>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(json_bytes).map_err(|e| format!("JSON parse error: {}", e))?;

    let doc = json_to_document(&value)?;
    let mut buf = Vec::new();
    doc.encode(&mut buf)
        .map_err(|e| format!("protobuf encode error: {}", e))?;
    Ok(buf)
}

fn json_to_document(v: &serde_json::Value) -> Result<Document, String> {
    let obj = v.as_object().ok_or("swagger spec must be an object")?;

    Ok(Document {
        swagger: get_str(obj, "swagger").unwrap_or("2.0").to_string(),
        info: obj.get("info").map(json_to_info),
        host: get_str(obj, "host").unwrap_or("").to_string(),
        base_path: get_str(obj, "basePath").unwrap_or("").to_string(),
        schemes: vec![],
        consumes: vec![],
        produces: vec![],
        paths: obj.get("paths").map(json_to_paths),
        definitions: obj.get("definitions").map(json_to_definitions),
        parameters: None,
        responses: None,
        security: vec![],
        security_definitions: None,
        tags: vec![],
        external_docs: None,
        vendor_extension: vec![],
    })
}

fn get_str<'a>(obj: &'a serde_json::Map<String, serde_json::Value>, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(|v| v.as_str())
}

fn json_to_info(v: &serde_json::Value) -> Info {
    let obj = v.as_object();
    Info {
        title: obj
            .and_then(|o| get_str(o, "title"))
            .unwrap_or("")
            .to_string(),
        version: obj
            .and_then(|o| get_str(o, "version"))
            .unwrap_or("")
            .to_string(),
        description: obj
            .and_then(|o| get_str(o, "description"))
            .unwrap_or("")
            .to_string(),
        terms_of_service: String::new(),
        contact: None,
        license: None,
        vendor_extension: vec![],
    }
}

fn json_to_paths(v: &serde_json::Value) -> Paths {
    let obj = match v.as_object() {
        Some(o) => o,
        None => {
            return Paths {
                vendor_extension: vec![],
                path: vec![],
            }
        }
    };
    Paths {
        vendor_extension: vec![],
        path: obj
            .iter()
            .filter(|(k, _)| k.starts_with('/'))
            .map(|(k, v)| NamedPathItem {
                name: k.clone(),
                value: Some(json_to_path_item(v)),
            })
            .collect(),
    }
}

fn json_to_path_item(v: &serde_json::Value) -> PathItem {
    let obj = v.as_object();
    PathItem {
        r#ref: String::new(),
        get: obj.and_then(|o| o.get("get")).map(json_to_operation),
        put: obj.and_then(|o| o.get("put")).map(json_to_operation),
        post: obj.and_then(|o| o.get("post")).map(json_to_operation),
        delete: obj.and_then(|o| o.get("delete")).map(json_to_operation),
        options: None,
        head: None,
        patch: obj.and_then(|o| o.get("patch")).map(json_to_operation),
        parameters: vec![],
        vendor_extension: vec![],
    }
}

fn json_to_operation(v: &serde_json::Value) -> Operation {
    let obj = v.as_object();
    Operation {
        tags: vec![],
        summary: String::new(),
        description: obj
            .and_then(|o| get_str(o, "description"))
            .unwrap_or("")
            .to_string(),
        external_docs: None,
        operation_id: String::new(),
        produces: vec![],
        consumes: vec![],
        parameters: vec![],
        responses: None,
        schemes: vec![],
        deprecated: false,
        security: vec![],
        vendor_extension: vec![],
    }
}

fn json_to_definitions(v: &serde_json::Value) -> Definitions {
    let obj = match v.as_object() {
        Some(o) => o,
        None => {
            return Definitions {
                additional_properties: vec![],
            }
        }
    };
    Definitions {
        additional_properties: obj
            .iter()
            .map(|(k, v)| NamedSchema {
                name: k.clone(),
                value: Some(json_to_schema(v)),
            })
            .collect(),
    }
}

fn empty_schema() -> Schema {
    Schema {
        r#ref: String::new(),
        format: String::new(),
        title: String::new(),
        description: String::new(),
        default: None,
        multiple_of: 0.0,
        maximum: 0.0,
        exclusive_maximum: false,
        minimum: 0.0,
        exclusive_minimum: false,
        max_length: 0,
        min_length: 0,
        pattern: String::new(),
        max_items: 0,
        min_items: 0,
        unique_items: false,
        max_properties: 0,
        min_properties: 0,
        required: vec![],
        r#enum: vec![],
        additional_properties: None,
        r#type: None,
        items: None,
        all_of: vec![],
        properties: None,
        discriminator: String::new(),
        read_only: false,
        xml: None,
        external_docs: None,
        example: None,
        vendor_extension: vec![],
    }
}

fn json_to_schema(v: &serde_json::Value) -> Schema {
    let obj = match v.as_object() {
        Some(o) => o,
        None => return empty_schema(),
    };

    Schema {
        r#ref: get_str(obj, "$ref").unwrap_or("").to_string(),
        format: get_str(obj, "format").unwrap_or("").to_string(),
        title: get_str(obj, "title").unwrap_or("").to_string(),
        description: get_str(obj, "description").unwrap_or("").to_string(),
        default: None,
        multiple_of: 0.0,
        maximum: obj.get("maximum").and_then(|v| v.as_f64()).unwrap_or(0.0),
        exclusive_maximum: obj
            .get("exclusiveMaximum")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        minimum: obj.get("minimum").and_then(|v| v.as_f64()).unwrap_or(0.0),
        exclusive_minimum: obj
            .get("exclusiveMinimum")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        max_length: obj.get("maxLength").and_then(|v| v.as_i64()).unwrap_or(0),
        min_length: obj.get("minLength").and_then(|v| v.as_i64()).unwrap_or(0),
        pattern: get_str(obj, "pattern").unwrap_or("").to_string(),
        max_items: obj.get("maxItems").and_then(|v| v.as_i64()).unwrap_or(0),
        min_items: obj.get("minItems").and_then(|v| v.as_i64()).unwrap_or(0),
        unique_items: obj
            .get("uniqueItems")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        max_properties: obj
            .get("maxProperties")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        min_properties: obj
            .get("minProperties")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        required: obj
            .get("required")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        r#enum: obj
            .get("enum")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().map(json_to_any).collect())
            .unwrap_or_default(),
        additional_properties: None,
        r#type: obj.get("type").map(|v| {
            if let Some(s) = v.as_str() {
                TypeItem {
                    value: vec![s.to_string()],
                }
            } else if let Some(arr) = v.as_array() {
                TypeItem {
                    value: arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect(),
                }
            } else {
                TypeItem { value: vec![] }
            }
        }),
        items: obj.get("items").map(|v| {
            if let Some(arr) = v.as_array() {
                // Array of schemas
                ItemsItem {
                    schema: arr.iter().map(json_to_schema).collect(),
                }
            } else {
                // Single schema
                ItemsItem {
                    schema: vec![json_to_schema(v)],
                }
            }
        }),
        all_of: obj
            .get("allOf")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().map(json_to_schema).collect())
            .unwrap_or_default(),
        properties: obj.get("properties").map(json_to_properties),
        discriminator: get_str(obj, "discriminator").unwrap_or("").to_string(),
        read_only: obj
            .get("readOnly")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        xml: None,
        external_docs: None,
        example: None,
        vendor_extension: json_to_vendor_extensions(obj),
    }
}

fn json_to_properties(v: &serde_json::Value) -> Properties {
    let obj = match v.as_object() {
        Some(o) => o,
        None => {
            return Properties {
                additional_properties: vec![],
            }
        }
    };
    Properties {
        additional_properties: obj
            .iter()
            .map(|(k, v)| NamedSchema {
                name: k.clone(),
                value: Some(json_to_schema(v)),
            })
            .collect(),
    }
}

fn json_to_vendor_extensions(obj: &serde_json::Map<String, serde_json::Value>) -> Vec<NamedAny> {
    obj.iter()
        .filter(|(k, _)| k.starts_with("x-"))
        .map(|(k, v)| NamedAny {
            name: k.clone(),
            value: Some(json_to_any(v)),
        })
        .collect()
}

fn json_to_any(v: &serde_json::Value) -> Any {
    // Gnostic's Any wraps values as YAML strings or google.protobuf.Any.
    // For simple values (strings, booleans), use the yaml field.
    // For complex values, serialize to JSON and wrap in prost_types::Any.
    match v {
        serde_json::Value::String(s) => Any {
            value: None,
            yaml: s.clone(),
        },
        _ => {
            let json_bytes = serde_json::to_vec(v).unwrap_or_default();
            Any {
                value: Some(prost_types::Any {
                    type_url: String::new(),
                    value: json_bytes,
                }),
                yaml: String::new(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// OpenAPI v3 → gnostic openapi.v3.Document protobuf
//
// Mirrors `swagger_json_to_protobuf` (above) for the v3 spec. Modelled as a
// thin JSON-to-message translation rather than a full type-driven build so it
// stays decoupled from the `openapiv3` crate types used to generate the spec.
// Only Document.openapi / Document.info / Document.paths are populated — the
// upstream gnostic schema carries many more fields, but unknown fields are
// tolerated by proto3 readers so this stays wire-compatible with client-go.
// ---------------------------------------------------------------------------

/// Convert an OpenAPI v3 JSON document to gnostic openapi.v3.Document bytes.
/// Equivalent of K8s `ToProtoBinary` for the v3 spec.
pub fn openapi_v3_json_to_protobuf(json_bytes: &[u8]) -> Result<Vec<u8>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(json_bytes).map_err(|e| format!("JSON parse error: {}", e))?;

    let doc = json_to_v3_document(&value)?;
    let mut buf = Vec::new();
    doc.encode(&mut buf)
        .map_err(|e| format!("protobuf encode error: {}", e))?;
    Ok(buf)
}

fn json_to_v3_document(v: &serde_json::Value) -> Result<openapi_v3::Document, String> {
    let obj = v.as_object().ok_or("OpenAPI v3 spec must be an object")?;

    Ok(openapi_v3::Document {
        openapi: get_str(obj, "openapi").unwrap_or("3.0.0").to_string(),
        info: obj.get("info").map(json_to_v3_info),
        paths: obj.get("paths").map(json_to_v3_paths),
        specification_extension: vec![],
    })
}

fn json_to_v3_info(v: &serde_json::Value) -> openapi_v3::Info {
    let obj = v.as_object();
    openapi_v3::Info {
        title: obj
            .and_then(|o| get_str(o, "title"))
            .unwrap_or("")
            .to_string(),
        description: obj
            .and_then(|o| get_str(o, "description"))
            .unwrap_or("")
            .to_string(),
        terms_of_service: obj
            .and_then(|o| get_str(o, "termsOfService"))
            .unwrap_or("")
            .to_string(),
        version: obj
            .and_then(|o| get_str(o, "version"))
            .unwrap_or("")
            .to_string(),
        specification_extension: vec![],
    }
}

fn json_to_v3_paths(v: &serde_json::Value) -> openapi_v3::Paths {
    let obj = match v.as_object() {
        Some(o) => o,
        None => {
            return openapi_v3::Paths {
                path: vec![],
                specification_extension: vec![],
            }
        }
    };
    openapi_v3::Paths {
        path: obj
            .iter()
            .filter(|(k, _)| k.starts_with('/'))
            .map(|(k, v)| openapi_v3::NamedPathItem {
                name: k.clone(),
                value: Some(json_to_v3_path_item(v)),
            })
            .collect(),
        specification_extension: vec![],
    }
}

fn json_to_v3_path_item(v: &serde_json::Value) -> openapi_v3::PathItem {
    let obj = v.as_object();
    openapi_v3::PathItem {
        r#ref: obj
            .and_then(|o| get_str(o, "$ref"))
            .unwrap_or("")
            .to_string(),
        summary: obj
            .and_then(|o| get_str(o, "summary"))
            .unwrap_or("")
            .to_string(),
        description: obj
            .and_then(|o| get_str(o, "description"))
            .unwrap_or("")
            .to_string(),
    }
}

#[cfg(test)]
mod v3_tests {
    use super::*;

    #[test]
    fn roundtrip_minimal_v3_document() {
        // Hand-craft a tiny v3 spec, encode it, decode it back, and assert the
        // top-level Document fields survive the wire format.
        let json = serde_json::json!({
            "openapi": "3.0.0",
            "info": {
                "title": "Rusternetes Test",
                "version": "v1.35.0",
                "description": "test doc"
            },
            "paths": {
                "/api/v1/pods": {
                    "summary": "pods collection",
                    "description": "list / create pods"
                }
            }
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let proto = openapi_v3_json_to_protobuf(&json_bytes).expect("encode v3");
        let decoded = openapi_v3::Document::decode(proto.as_slice()).expect("decode v3");

        assert_eq!(decoded.openapi, "3.0.0");
        let info = decoded.info.expect("info present");
        assert_eq!(info.title, "Rusternetes Test");
        assert_eq!(info.version, "v1.35.0");
        assert_eq!(info.description, "test doc");

        let paths = decoded.paths.expect("paths present");
        assert_eq!(paths.path.len(), 1, "expected one path entry");
        assert_eq!(paths.path[0].name, "/api/v1/pods");
        let item = paths.path[0].value.as_ref().expect("path item present");
        assert_eq!(item.summary, "pods collection");
        assert_eq!(item.description, "list / create pods");
    }

    #[test]
    fn v3_path_keys_must_start_with_slash() {
        // Discovery sub-documents include a `components` sibling that isn't a
        // path — make sure non-`/` keys are skipped, matching the v2 helper.
        let json = serde_json::json!({
            "openapi": "3.0.0",
            "info": { "title": "T", "version": "v1" },
            "paths": {
                "/ok": { "summary": "kept" },
                "components": { "summary": "dropped" }
            }
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let proto = openapi_v3_json_to_protobuf(&json_bytes).unwrap();
        let decoded = openapi_v3::Document::decode(proto.as_slice()).unwrap();
        let paths = decoded.paths.unwrap();
        assert_eq!(paths.path.len(), 1);
        assert_eq!(paths.path[0].name, "/ok");
    }

    #[test]
    fn rejects_non_object_input() {
        assert!(openapi_v3_json_to_protobuf(b"\"not an object\"").is_err());
    }
}
