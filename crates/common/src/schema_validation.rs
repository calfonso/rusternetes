// OpenAPI v3 Schema Validation for Custom Resources
//
// This module provides JSON Schema validation for custom resources based on
// OpenAPI v3 schemas defined in CustomResourceDefinitions.
//
// K8s architecture (customresource_handler.go):
// 1. GetObjectMetaWithOptions — validates top-level metadata, collects unknown meta fields
// 2. PruneWithOptions — walks structural schema, collects unknown field paths
// 3. CoerceWithOptions — validates embedded resource metadata
// All unknown field paths are collected and returned together.
// See: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/algorithm.go

use crate::error::Error;
use crate::resources::crd::JSONSchemaProps;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

/// Cache of compiled regexes keyed by pattern string. CRD validation can hit the
/// same pattern repeatedly on every admission call; recompiling each time is
/// expensive enough to show up in profiles for cluster-wide reconciles.
static REGEX_CACHE: LazyLock<RwLock<HashMap<String, Regex>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

fn compile_regex_cached(pattern: &str) -> Result<Regex, regex::Error> {
    if let Some(re) = REGEX_CACHE
        .read()
        .ok()
        .and_then(|g| g.get(pattern).cloned())
    {
        return Ok(re);
    }
    let re = Regex::new(pattern)?;
    if let Ok(mut g) = REGEX_CACHE.write() {
        g.insert(pattern.to_string(), re.clone());
    }
    Ok(re)
}

/// Validator validates JSON values against JSONSchemaProps
pub struct SchemaValidator;

impl SchemaValidator {
    /// Validate a JSON value against a schema.
    /// Collects ALL unknown field paths and returns them as a single error,
    /// matching K8s behavior where PruneWithOptions collects all paths.
    pub fn validate(schema: &JSONSchemaProps, value: &Value) -> Result<(), Error> {
        let mut unknown_fields = Vec::new();
        Self::validate_with_path(schema, value, "", &mut unknown_fields)?;
        if !unknown_fields.is_empty() {
            unknown_fields.sort();
            let msg = unknown_fields
                .iter()
                .map(|p| format!("{}: field not declared in schema", p))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::InvalidResource(msg));
        }
        Ok(())
    }

    /// Validate a JSON value against a schema WITHOUT checking for unknown fields.
    /// Used for normal (non-strict) validation where unknown fields are pruned
    /// rather than rejected. Still validates types, required fields, enums, etc.
    pub fn validate_no_unknown_check(schema: &JSONSchemaProps, value: &Value) -> Result<(), Error> {
        let mut dummy = Vec::new();
        Self::validate_with_path_skip_unknown(schema, value, "", &mut dummy)
    }

    /// Validate with strict mode — returns unknown fields as K8s-formatted errors.
    /// Returns `strict decoding error: unknown field "spec.foo"` format.
    pub fn validate_strict(
        schema: &JSONSchemaProps,
        value: &Value,
        base_path: &str,
    ) -> Result<(), Error> {
        let mut unknown_fields = Vec::new();
        Self::validate_with_path(schema, value, "", &mut unknown_fields)?;
        if !unknown_fields.is_empty() {
            unknown_fields.sort();
            let msg = unknown_fields
                .iter()
                .map(|p| {
                    // Convert ".foo" to "spec.foo" by prepending base_path
                    let field = p.trim_start_matches('.');
                    let full_path = if base_path.is_empty() {
                        field.to_string()
                    } else {
                        format!("{}.{}", base_path, field)
                    };
                    format!("strict decoding error: unknown field \"{}\"", full_path)
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::InvalidResource(msg));
        }
        Ok(())
    }

    /// Validate with path but skip unknown field detection.
    /// Validates types, required fields, enums, patterns, etc.
    fn validate_with_path_skip_unknown(
        schema: &JSONSchemaProps,
        value: &Value,
        path: &str,
        _unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        // Validate type
        if let Some(ref type_) = schema.type_ {
            Self::validate_type(type_, value, path)?;
        }

        // Validate based on value type
        match value {
            Value::Object(obj) => Self::validate_object_skip_unknown(schema, obj, path)?,
            Value::Array(arr) => {
                let mut dummy = Vec::new();
                Self::validate_array(schema, arr, path, &mut dummy)?;
            }
            Value::String(s) => Self::validate_string(schema, s, path)?,
            Value::Number(n) => Self::validate_number(schema, n, path)?,
            Value::Bool(_) => {}
            Value::Null => {
                if let Some(false) = schema.nullable {
                    return Err(Error::InvalidResource(format!(
                        "Field at {} cannot be null",
                        path
                    )));
                }
            }
        }

        // Validate enum
        if let Some(ref enum_values) = schema.enum_ {
            if !enum_values.contains(value) {
                let val_str = match value {
                    Value::String(s) => format!("\"{}\"", s),
                    other => other.to_string(),
                };
                return Err(Error::InvalidResource(format!(
                    "Unsupported value: {}",
                    val_str
                )));
            }
        }

        Ok(())
    }

    /// Validate object properties without checking for unknown fields.
    fn validate_object_skip_unknown(
        schema: &JSONSchemaProps,
        obj: &serde_json::Map<String, Value>,
        path: &str,
    ) -> Result<(), Error> {
        // Validate required fields
        if let Some(ref required) = schema.required {
            for field in required {
                if !obj.contains_key(field) {
                    let field_path = if path.is_empty() {
                        field.clone()
                    } else {
                        format!("{}.{}", path, field)
                    };
                    return Err(Error::InvalidResource(format!(
                        "{}: Required value",
                        field_path
                    )));
                }
            }
        }

        // Validate known properties
        if let Some(ref properties) = schema.properties {
            for (key, value) in obj {
                if let Some(prop_schema) = properties.get(key) {
                    let new_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    let mut dummy = Vec::new();
                    Self::validate_with_path_skip_unknown(
                        prop_schema,
                        value,
                        &new_path,
                        &mut dummy,
                    )?;
                }
                // Unknown fields are silently ignored (they'll be pruned)
            }
        }

        Ok(())
    }

    /// Validate with a path for error reporting.
    /// Unknown fields are collected into `unknown_fields` instead of returning
    /// immediately, matching K8s PruneWithOptions behavior.
    fn validate_with_path(
        schema: &JSONSchemaProps,
        value: &Value,
        path: &str,
        unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        // Validate type
        if let Some(ref type_) = schema.type_ {
            Self::validate_type(type_, value, path)?;
        }

        // Validate based on value type
        match value {
            Value::Object(obj) => Self::validate_object(schema, obj, path, unknown_fields)?,
            Value::Array(arr) => Self::validate_array(schema, arr, path, unknown_fields)?,
            Value::String(s) => Self::validate_string(schema, s, path)?,
            Value::Number(n) => Self::validate_number(schema, n, path)?,
            Value::Bool(_) => {}
            Value::Null => {
                if let Some(false) = schema.nullable {
                    return Err(Error::InvalidResource(format!(
                        "Field at {} cannot be null",
                        path
                    )));
                }
            }
        }

        // Validate enum
        if let Some(ref enum_values) = schema.enum_ {
            if !enum_values.contains(value) {
                // K8s format: Unsupported value: "NonExistentValue": supported values: "Great", "Down"
                let val_str = match value {
                    Value::String(s) => format!("\"{}\"", s),
                    other => other.to_string(),
                };
                return Err(Error::InvalidResource(format!(
                    "Unsupported value: {}",
                    val_str
                )));
            }
        }

        // Validate oneOf
        if let Some(ref one_of) = schema.one_of {
            let mut dummy = Vec::new();
            let matches: Vec<_> = one_of
                .iter()
                .filter(|s| Self::validate_with_path(s, value, path, &mut dummy).is_ok())
                .collect();

            if matches.len() != 1 {
                return Err(Error::InvalidResource(format!(
                    "Field at {} must match exactly one schema (matched {})",
                    path,
                    matches.len()
                )));
            }
        }

        // Validate anyOf
        if let Some(ref any_of) = schema.any_of {
            let mut dummy = Vec::new();
            let matches = any_of
                .iter()
                .any(|s| Self::validate_with_path(s, value, path, &mut dummy).is_ok());

            if !matches {
                return Err(Error::InvalidResource(format!(
                    "Field at {} must match at least one schema",
                    path
                )));
            }
        }

        // Validate allOf
        if let Some(ref all_of) = schema.all_of {
            for sub_schema in all_of {
                Self::validate_with_path(sub_schema, value, path, unknown_fields)?;
            }
        }

        // Validate not
        if let Some(ref not) = schema.not {
            let mut dummy = Vec::new();
            if Self::validate_with_path(not, value, path, &mut dummy).is_ok() {
                return Err(Error::InvalidResource(format!(
                    "Field at {} must not match the schema",
                    path
                )));
            }
        }

        Ok(())
    }

    fn validate_type(type_: &str, value: &Value, path: &str) -> Result<(), Error> {
        let matches = match (type_, value) {
            ("object", Value::Object(_)) => true,
            ("array", Value::Array(_)) => true,
            ("string", Value::String(_)) => true,
            ("number", Value::Number(_)) => true,
            ("integer", Value::Number(n)) => n.is_i64() || n.is_u64(),
            ("boolean", Value::Bool(_)) => true,
            ("null", Value::Null) => true,
            _ => false,
        };

        if !matches {
            return Err(Error::InvalidResource(format!(
                "Field at {} must be of type {}, got {:?}",
                path,
                type_,
                Self::value_type(value)
            )));
        }

        Ok(())
    }

    fn validate_object(
        schema: &JSONSchemaProps,
        obj: &serde_json::Map<String, Value>,
        path: &str,
        unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        // Validate required fields
        if let Some(ref required) = schema.required {
            for field in required {
                if !obj.contains_key(field) {
                    // K8s format: spec.bars[0].name: Required value
                    // Also accepted: missing required field "name"
                    let field_path = if path.is_empty() {
                        field.clone()
                    } else {
                        format!("{}.{}", path, field)
                    };
                    return Err(Error::InvalidResource(format!(
                        "{}: Required value",
                        field_path
                    )));
                }
            }
        }

        // Validate min/max properties
        if let Some(min) = schema.min_properties {
            if (obj.len() as i64) < min {
                return Err(Error::InvalidResource(format!(
                    "Object at {} must have at least {} properties",
                    path, min
                )));
            }
        }

        if let Some(max) = schema.max_properties {
            if (obj.len() as i64) > max {
                return Err(Error::InvalidResource(format!(
                    "Object at {} must have at most {} properties",
                    path, max
                )));
            }
        }

        // K8s embedded resource meta fields are always allowed:
        // apiVersion, kind, metadata (case-sensitive).
        // See: pruning/algorithm.go:51-55, 77
        let is_embedded = schema.x_kubernetes_embedded_resource == Some(true);

        // Validate properties
        if let Some(ref properties) = schema.properties {
            for (key, value) in obj {
                if let Some(prop_schema) = properties.get(key) {
                    let new_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    Self::validate_with_path(prop_schema, value, &new_path, unknown_fields)?;
                } else if is_embedded && (key == "apiVersion" || key == "kind" || key == "metadata")
                {
                    // Embedded resource meta fields are implicitly allowed
                    continue;
                } else if let Some(ref additional) = schema.additional_properties {
                    let new_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    Self::validate_additional_properties(
                        additional,
                        value,
                        &new_path,
                        unknown_fields,
                    )?;
                } else if schema.x_kubernetes_preserve_unknown_fields != Some(true) {
                    // Unknown field — collect path instead of returning immediately.
                    // K8s PruneWithOptions collects all unknown field paths.
                    let field_path = if path.is_empty() {
                        format!(".{}", key)
                    } else {
                        format!(".{}.{}", path, key)
                    };
                    unknown_fields.push(field_path);
                }
            }
        } else if let Some(ref additional) = schema.additional_properties {
            for (key, value) in obj {
                let new_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };
                Self::validate_additional_properties(additional, value, &new_path, unknown_fields)?;
            }
        }

        Ok(())
    }

    fn validate_additional_properties(
        additional: &crate::resources::crd::JSONSchemaPropsOrBool,
        value: &Value,
        path: &str,
        unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        use crate::resources::crd::JSONSchemaPropsOrBool;

        match additional {
            JSONSchemaPropsOrBool::Schema(schema) => {
                Self::validate_with_path(schema, value, path, unknown_fields)?;
            }
            JSONSchemaPropsOrBool::Bool(false) => {
                return Err(Error::InvalidResource(format!(
                    "Additional property at {} not allowed",
                    path
                )));
            }
            JSONSchemaPropsOrBool::Bool(true) => {}
        }

        Ok(())
    }

    fn validate_array(
        schema: &JSONSchemaProps,
        arr: &[Value],
        path: &str,
        unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        if let Some(min) = schema.min_items {
            if (arr.len() as i64) < min {
                return Err(Error::InvalidResource(format!(
                    "Array at {} must have at least {} items",
                    path, min
                )));
            }
        }

        if let Some(max) = schema.max_items {
            if (arr.len() as i64) > max {
                return Err(Error::InvalidResource(format!(
                    "Array at {} must have at most {} items",
                    path, max
                )));
            }
        }

        if let Some(true) = schema.unique_items {
            let mut seen = Vec::new();
            for item in arr {
                if seen.contains(item) {
                    return Err(Error::InvalidResource(format!(
                        "Array at {} must have unique items",
                        path
                    )));
                }
                seen.push(item.clone());
            }
        }

        if let Some(ref items) = schema.items {
            use crate::resources::crd::JSONSchemaPropsOrArray;

            match items.as_ref() {
                JSONSchemaPropsOrArray::Schema(item_schema) => {
                    for (i, item) in arr.iter().enumerate() {
                        let new_path = format!("{}[{}]", path, i);
                        Self::validate_with_path(item_schema, item, &new_path, unknown_fields)?;
                    }
                }
                JSONSchemaPropsOrArray::Schemas(schemas) => {
                    for (i, item) in arr.iter().enumerate() {
                        if i < schemas.len() {
                            let new_path = format!("{}[{}]", path, i);
                            Self::validate_with_path(&schemas[i], item, &new_path, unknown_fields)?;
                        } else if let Some(ref additional) = schema.additional_items {
                            let new_path = format!("{}[{}]", path, i);
                            Self::validate_additional_items(additional, item, &new_path)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_additional_items(
        additional: &crate::resources::crd::JSONSchemaPropsOrBool,
        value: &Value,
        path: &str,
    ) -> Result<(), Error> {
        use crate::resources::crd::JSONSchemaPropsOrBool;

        match additional {
            JSONSchemaPropsOrBool::Schema(schema) => {
                let mut dummy = Vec::new();
                Self::validate_with_path(schema, value, path, &mut dummy)?;
            }
            JSONSchemaPropsOrBool::Bool(false) => {
                return Err(Error::InvalidResource(format!(
                    "Additional item at {} not allowed",
                    path
                )));
            }
            JSONSchemaPropsOrBool::Bool(true) => {}
        }

        Ok(())
    }

    fn validate_string(schema: &JSONSchemaProps, s: &str, path: &str) -> Result<(), Error> {
        if let Some(min) = schema.min_length {
            if (s.len() as i64) < min {
                return Err(Error::InvalidResource(format!(
                    "String at {} must be at least {} characters",
                    path, min
                )));
            }
        }

        if let Some(max) = schema.max_length {
            if (s.len() as i64) > max {
                return Err(Error::InvalidResource(format!(
                    "String at {} must be at most {} characters",
                    path, max
                )));
            }
        }

        if let Some(ref pattern) = schema.pattern {
            let re = compile_regex_cached(pattern)
                .map_err(|e| Error::InvalidResource(format!("Invalid regex pattern: {}", e)))?;

            if !re.is_match(s) {
                return Err(Error::InvalidResource(format!(
                    "String at {} must match pattern '{}'",
                    path, pattern
                )));
            }
        }

        if let Some(ref format) = schema.format {
            Self::validate_format(format, s, path)?;
        }

        Ok(())
    }

    fn validate_number(
        schema: &JSONSchemaProps,
        n: &serde_json::Number,
        path: &str,
    ) -> Result<(), Error> {
        let value = n.as_f64().unwrap_or(0.0);

        if let Some(min) = schema.minimum {
            let exclusive = schema.exclusive_minimum.unwrap_or(false);
            if exclusive {
                if value <= min {
                    return Err(Error::InvalidResource(format!(
                        "Number at {} must be greater than {}",
                        path, min
                    )));
                }
            } else if value < min {
                return Err(Error::InvalidResource(format!(
                    "Number at {} must be at least {}",
                    path, min
                )));
            }
        }

        if let Some(max) = schema.maximum {
            let exclusive = schema.exclusive_maximum.unwrap_or(false);
            if exclusive {
                if value >= max {
                    return Err(Error::InvalidResource(format!(
                        "Number at {} must be less than {}",
                        path, max
                    )));
                }
            } else if value > max {
                return Err(Error::InvalidResource(format!(
                    "Number at {} must be at most {}",
                    path, max
                )));
            }
        }

        Ok(())
    }

    fn validate_format(format: &str, value: &str, path: &str) -> Result<(), Error> {
        match format {
            "date-time" => {
                if !value.contains('T') || !value.contains(':') {
                    return Err(Error::InvalidResource(format!(
                        "String at {} must be a valid date-time",
                        path
                    )));
                }
            }
            "email" => {
                if !value.contains('@') || !value.contains('.') {
                    return Err(Error::InvalidResource(format!(
                        "String at {} must be a valid email",
                        path
                    )));
                }
            }
            "uri" | "url" => {
                if !value.starts_with("http://") && !value.starts_with("https://") {
                    return Err(Error::InvalidResource(format!(
                        "String at {} must be a valid URL",
                        path
                    )));
                }
            }
            "uuid" => {
                if value.len() != 36 || value.chars().filter(|c| *c == '-').count() != 4 {
                    return Err(Error::InvalidResource(format!(
                        "String at {} must be a valid UUID",
                        path
                    )));
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn value_type(value: &Value) -> &str {
        match value {
            Value::Object(_) => "object",
            Value::Array(_) => "array",
            Value::String(_) => "string",
            Value::Number(_) => "number",
            Value::Bool(_) => "boolean",
            Value::Null => "null",
        }
    }

    /// Apply default values from a JSONSchemaProps to a JSON value.
    pub fn apply_defaults(schema: &JSONSchemaProps, value: &mut Value) {
        Self::apply_defaults_recursive(schema, value);
    }

    fn apply_defaults_recursive(schema: &JSONSchemaProps, value: &mut Value) {
        if let Value::Object(ref mut map) = value {
            if let Some(ref properties) = schema.properties {
                for (key, prop_schema) in properties {
                    if map.contains_key(key) {
                        if let Some(val) = map.get_mut(key) {
                            Self::apply_defaults_recursive(prop_schema, val);
                        }
                    } else if let Some(ref default_val) = prop_schema.default {
                        map.insert(key.clone(), default_val.clone());
                    }
                }
            }
        }

        if let Value::Array(ref mut arr) = value {
            if let Some(ref items) = schema.items {
                use crate::resources::crd::JSONSchemaPropsOrArray;
                match items.as_ref() {
                    JSONSchemaPropsOrArray::Schema(item_schema) => {
                        for item in arr.iter_mut() {
                            Self::apply_defaults_recursive(item_schema, item);
                        }
                    }
                    JSONSchemaPropsOrArray::Schemas(schemas) => {
                        for (i, item) in arr.iter_mut().enumerate() {
                            if i < schemas.len() {
                                Self::apply_defaults_recursive(&schemas[i], item);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_validate_type_object() {
        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            ..Default::default()
        };

        let valid = json!({"key": "value"});
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!("string");
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    #[test]
    fn test_validate_required_fields() {
        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );

        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            required: Some(vec!["name".to_string()]),
            properties: Some(properties),
            ..Default::default()
        };

        let valid = json!({"name": "test"});
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!({});
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    #[test]
    fn test_validate_string_length() {
        let schema = JSONSchemaProps {
            type_: Some("string".to_string()),
            min_length: Some(3),
            max_length: Some(10),
            ..Default::default()
        };

        let valid = json!("hello");
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let too_short = json!("hi");
        assert!(SchemaValidator::validate(&schema, &too_short).is_err());

        let too_long = json!("hello world!");
        assert!(SchemaValidator::validate(&schema, &too_long).is_err());
    }

    #[test]
    fn test_validate_number_range() {
        let schema = JSONSchemaProps {
            type_: Some("number".to_string()),
            minimum: Some(0.0),
            maximum: Some(100.0),
            ..Default::default()
        };

        let valid = json!(50);
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let too_small = json!(-1);
        assert!(SchemaValidator::validate(&schema, &too_small).is_err());

        let too_large = json!(101);
        assert!(SchemaValidator::validate(&schema, &too_large).is_err());
    }

    #[test]
    fn test_validate_array_items() {
        let schema = JSONSchemaProps {
            type_: Some("array".to_string()),
            items: Some(Box::new(
                crate::resources::crd::JSONSchemaPropsOrArray::Schema(JSONSchemaProps {
                    type_: Some("string".to_string()),
                    ..Default::default()
                }),
            )),
            ..Default::default()
        };

        let valid = json!(["a", "b", "c"]);
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!(["a", 1, "c"]);
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    #[test]
    fn test_validate_enum() {
        let schema = JSONSchemaProps {
            type_: Some("string".to_string()),
            enum_: Some(vec![json!("red"), json!("green"), json!("blue")]),
            ..Default::default()
        };

        let valid = json!("red");
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!("yellow");
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    #[test]
    fn test_validate_pattern() {
        let schema = JSONSchemaProps {
            type_: Some("string".to_string()),
            pattern: Some("^[a-z]+$".to_string()),
            ..Default::default()
        };

        let valid = json!("hello");
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!("Hello123");
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    /// Test that unknown fields are collected (not returned on first error)
    /// matching K8s PruneWithOptions behavior.
    #[test]
    fn test_collects_all_unknown_fields() {
        let mut properties = HashMap::new();
        properties.insert(
            "known".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );

        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(properties),
            ..Default::default()
        };

        // Two unknown fields — both should appear in the error
        let value = json!({"known": "ok", "unknown1": "bad", "unknown2": "also bad"});
        let err = SchemaValidator::validate(&schema, &value).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(".unknown1") && msg.contains(".unknown2"),
            "Error should contain ALL unknown fields, got: {}",
            msg
        );
    }

    /// Test that embedded resource allows apiVersion/kind/metadata (case-sensitive)
    /// but rejects other unknown fields like 'apiversion' (lowercase).
    /// K8s ref: pruning/algorithm.go:51-55, 77
    #[test]
    fn test_embedded_resource_meta_fields() {
        let mut template_properties = HashMap::new();
        template_properties.insert(
            "name".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );

        let mut properties = HashMap::new();
        properties.insert(
            "template".to_string(),
            JSONSchemaProps {
                type_: Some("object".to_string()),
                x_kubernetes_embedded_resource: Some(true),
                properties: Some(template_properties),
                ..Default::default()
            },
        );

        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(properties),
            ..Default::default()
        };

        // apiVersion (camelCase) should be allowed on embedded resource
        let valid = json!({"template": {"apiVersion": "v1", "kind": "Pod", "name": "test"}});
        assert!(
            SchemaValidator::validate(&schema, &valid).is_ok(),
            "apiVersion/kind should be allowed on embedded resource"
        );

        // apiversion (lowercase) is NOT a meta field — should be rejected
        let invalid = json!({"template": {"apiversion": "v1", "name": "test"}});
        let err = SchemaValidator::validate(&schema, &invalid).unwrap_err();
        assert!(
            err.to_string().contains("apiversion"),
            "lowercase 'apiversion' should be rejected: {}",
            err
        );
    }

    /// Test enum validation inside array items (conformance test scenario).
    /// Schema: spec.bars[].feeling has enum: ["Great", "Down"]
    /// Invalid value "NonExistentValue" should be rejected.
    #[test]
    fn test_validate_enum_in_array_items() {
        use crate::resources::crd::JSONSchemaPropsOrArray;

        // Build the schema matching the conformance test:
        // spec.bars[].feeling with enum: ["Great", "Down"]
        let mut feeling_props = HashMap::new();
        feeling_props.insert(
            "name".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );
        feeling_props.insert(
            "feeling".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                enum_: Some(vec![json!("Great"), json!("Down")]),
                ..Default::default()
            },
        );

        let item_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            required: Some(vec!["name".to_string()]),
            properties: Some(feeling_props),
            ..Default::default()
        };

        let mut spec_properties = HashMap::new();
        spec_properties.insert(
            "bars".to_string(),
            JSONSchemaProps {
                type_: Some("array".to_string()),
                items: Some(Box::new(JSONSchemaPropsOrArray::Schema(item_schema))),
                ..Default::default()
            },
        );

        let spec_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(spec_properties),
            ..Default::default()
        };

        // Valid value: feeling = "Great"
        let valid_cr = json!({"bars": [{"name": "test-bar", "feeling": "Great"}]});
        assert!(
            SchemaValidator::validate_no_unknown_check(&spec_schema, &valid_cr).is_ok(),
            "Valid enum value 'Great' should be accepted"
        );

        // Invalid value: feeling = "NonExistentValue"
        let invalid_cr = json!({"bars": [{"name": "test-bar", "feeling": "NonExistentValue"}]});
        let result = SchemaValidator::validate_no_unknown_check(&spec_schema, &invalid_cr);
        assert!(
            result.is_err(),
            "Invalid enum value 'NonExistentValue' should be rejected"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Unsupported value") && err_msg.contains("NonExistentValue"),
            "Error should mention unsupported value: {}",
            err_msg
        );
    }

    /// Test that x-kubernetes-preserve-unknown-fields allows arbitrary fields
    #[test]
    fn test_preserve_unknown_fields() {
        let mut properties = HashMap::new();
        properties.insert(
            "known".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );

        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(properties),
            x_kubernetes_preserve_unknown_fields: Some(true),
            ..Default::default()
        };

        // Unknown fields should be allowed when preserve-unknown-fields is true
        let value = json!({"known": "ok", "arbitrary": "allowed", "anything": 123});
        assert!(
            SchemaValidator::validate(&schema, &value).is_ok(),
            "preserve-unknown-fields should allow arbitrary fields"
        );
    }

    // ── Regex cache tests ────────────────────────────────────────────────

    /// First call should populate the cache; second call should hit it
    /// without recompiling. Verified by inspecting the static cache
    /// directly — the cached entry must be present after a single call.
    #[test]
    fn regex_cache_populates_on_first_call() {
        // Use a pattern unique to this test so other tests / runs don't
        // pollute the assertion.
        let pattern = "^cache-populate-test-[a-z0-9]+$";

        // Ensure the cache starts without this entry.
        REGEX_CACHE.write().unwrap().remove(pattern);
        assert!(
            !REGEX_CACHE.read().unwrap().contains_key(pattern),
            "precondition: cache must not contain our test pattern"
        );

        // First call compiles + inserts into cache.
        let re1 = compile_regex_cached(pattern).expect("compile");
        assert!(re1.is_match("cache-populate-test-abc123"));
        assert!(
            REGEX_CACHE.read().unwrap().contains_key(pattern),
            "cache must contain pattern after first call"
        );

        // Second call returns a regex that still matches — proving the cached
        // copy is functionally equivalent. We can't compare Regex by identity
        // (no PartialEq) so functional check is the strongest assertion.
        let re2 = compile_regex_cached(pattern).expect("compile (cached)");
        assert!(re2.is_match("cache-populate-test-xyz789"));
    }

    /// Bad regex still surfaces an error and does NOT get cached.
    #[test]
    fn regex_cache_does_not_store_invalid_pattern() {
        let bad = "[unclosed";
        REGEX_CACHE.write().unwrap().remove(bad);
        let result = compile_regex_cached(bad);
        assert!(result.is_err(), "invalid regex should error");
        assert!(
            !REGEX_CACHE.read().unwrap().contains_key(bad),
            "invalid regex must not be cached"
        );
    }

    /// Microbenchmark: prove the cache makes a hot path meaningfully faster.
    /// Marked `#[ignore]` because timing-based asserts are inherently noisy
    /// in CI — run explicitly with:
    ///
    ///   cargo test -p rusternetes-common --release -- --ignored regex_cache_speedup --nocapture
    ///
    /// Compares two equivalent workloads on the same input:
    ///   1. cached: validate via SchemaValidator (hits compile_regex_cached)
    ///   2. uncached: `Regex::new(pattern)` per iteration (the pre-cache path)
    ///
    /// On a warm machine the cached path is typically 50–500× faster than
    /// re-compiling. The assertion uses a conservative 10× lower bound so
    /// it doesn't flake on a slow runner; the printed ratio is the real
    /// signal — read it from `--nocapture` output.
    #[test]
    #[ignore = "perf microbenchmark; run with --ignored on --release"]
    fn regex_cache_speedup() {
        use std::time::Instant;

        // A pattern with character classes + quantifiers + anchors so
        // compile work is non-trivial. Same shape as common K8s patterns
        // (DNS label, semver, label-value).
        let pattern = r"^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$";
        let input = "subdomain.example.com";
        let n: u32 = 5_000;

        let schema = JSONSchemaProps {
            type_: Some("string".to_string()),
            pattern: Some(pattern.to_string()),
            ..Default::default()
        };
        let value = serde_json::Value::String(input.to_string());

        // Warm the cache once so the timed loop measures hit-path only.
        SchemaValidator::validate(&schema, &value).unwrap();

        // Cached path: every call hits compile_regex_cached via the
        // public validate() entry point. Mirrors real CRD admission.
        let cached = {
            let t = Instant::now();
            for _ in 0..n {
                SchemaValidator::validate(&schema, &value).unwrap();
            }
            t.elapsed()
        };

        // Uncached path: what the code does on fork/main without this
        // PR — Regex::new + is_match per call.
        let uncached = {
            let t = Instant::now();
            for _ in 0..n {
                let re = regex::Regex::new(pattern).unwrap();
                assert!(re.is_match(input));
            }
            t.elapsed()
        };

        let ratio = uncached.as_nanos() as f64 / cached.as_nanos().max(1) as f64;
        eprintln!(
            "regex_cache_speedup: n={} cached={:?} uncached={:?} speedup={:.1}x",
            n, cached, uncached, ratio
        );

        assert!(
            ratio >= 10.0,
            "expected cache to give >=10x speedup; got {:.1}x (cached={:?} uncached={:?})",
            ratio,
            cached,
            uncached
        );
    }
}
