//! Resource name validation utilities
//!
//! Implements Kubernetes-compatible name validation rules:
//! - DNS subdomain names (RFC 1123): lowercase alphanumeric, '-', '.', max 253 chars
//! - DNS label names: lowercase alphanumeric, '-', max 63 chars

use rusternetes_common::Error;
use std::collections::{HashMap, HashSet};

/// Find duplicate JSON keys at any nesting level of a JSON object string.
/// Returns the first duplicate key found (just the key name, not dotted path), or None.
/// This scans each `{...}` object at every depth for duplicate keys within that object.
fn find_duplicate_json_key(json_str: &str) -> Option<String> {
    let dups = find_all_duplicate_json_keys(json_str);
    dups.into_iter().next()
}

/// Find ALL duplicate JSON keys at any nesting level of a JSON object string.
/// Returns dotted paths (e.g., "spec.replicas") for each duplicate found.
fn find_all_duplicate_json_keys(json_str: &str) -> Vec<String> {
    let trimmed = json_str.trim();
    if !trimmed.starts_with('{') {
        return Vec::new();
    }

    let bytes = trimmed.as_bytes();
    let mut results = Vec::new();
    find_duplicates_in_object(bytes, 0, "", &mut results);
    results
}

/// Parse a JSON object starting at `start` (which should point to '{'),
/// collecting all duplicate key paths into `results`.
/// Returns the position after the closing '}', or None on parse error.
fn find_duplicates_in_object(
    bytes: &[u8],
    start: usize,
    prefix: &str,
    results: &mut Vec<String>,
) -> Option<usize> {
    if start >= bytes.len() || bytes[start] != b'{' {
        return None;
    }

    let mut seen_keys: HashSet<String> = HashSet::new();
    let mut pos = start + 1;

    loop {
        // Skip whitespace
        pos = skip_whitespace(bytes, pos);
        if pos >= bytes.len() {
            return None;
        }

        // Check for end of object
        if bytes[pos] == b'}' {
            return Some(pos + 1);
        }

        // Skip comma between entries
        if bytes[pos] == b',' {
            pos += 1;
            pos = skip_whitespace(bytes, pos);
            if pos >= bytes.len() {
                return None;
            }
        }

        // Check for end of object again (after comma)
        if bytes[pos] == b'}' {
            return Some(pos + 1);
        }

        // Expect a key string
        if bytes[pos] != b'"' {
            // Not a valid JSON key, skip
            return None;
        }

        // Extract key
        let (key, key_end) = extract_string(bytes, pos)?;
        pos = key_end;

        // Build the dotted path for this key
        let dotted_path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        // Skip whitespace and colon
        pos = skip_whitespace(bytes, pos);
        if pos >= bytes.len() || bytes[pos] != b':' {
            return None;
        }
        pos += 1;
        pos = skip_whitespace(bytes, pos);

        // Check for duplicate key in this object
        if !seen_keys.insert(key.clone()) {
            results.push(dotted_path.clone());
        }

        // Now we need to skip the value, but also recurse into objects/arrays
        // to check for nested duplicates
        match collect_value_duplicates(bytes, pos, &dotted_path, results) {
            Some(end) => {
                pos = end;
            }
            None => return None,
        }
    }
}

/// Skip a JSON value starting at `pos`, also checking nested objects for duplicates.
/// Collects all duplicate key paths into `results`.
/// Returns the position after the value, or None on parse error.
fn collect_value_duplicates(
    bytes: &[u8],
    pos: usize,
    prefix: &str,
    results: &mut Vec<String>,
) -> Option<usize> {
    if pos >= bytes.len() {
        return None;
    }

    match bytes[pos] {
        b'{' => {
            // Recurse into object to check for duplicates
            find_duplicates_in_object(bytes, pos, prefix, results)
        }
        b'[' => {
            // Recurse into array elements
            let mut p = pos + 1;
            let mut idx = 0;
            loop {
                p = skip_whitespace(bytes, p);
                if p >= bytes.len() {
                    return None;
                }
                if bytes[p] == b']' {
                    return Some(p + 1);
                }
                if bytes[p] == b',' {
                    p += 1;
                    continue;
                }

                let elem_prefix = format!("{}[{}]", prefix, idx);
                match collect_value_duplicates(bytes, p, &elem_prefix, results) {
                    Some(end) => {
                        p = end;
                        idx += 1;
                    }
                    None => return None,
                }
            }
        }
        _ => {
            // Scalar value — just skip it
            skip_json_value(bytes, pos)
        }
    }
}

/// Skip whitespace characters
fn skip_whitespace(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && matches!(bytes[pos], b' ' | b'\t' | b'\n' | b'\r') {
        pos += 1;
    }
    pos
}

/// Extract a JSON string starting at `pos` (which should point to '"').
/// Returns (string_content, position_after_closing_quote).
fn extract_string(bytes: &[u8], pos: usize) -> Option<(String, usize)> {
    if pos >= bytes.len() || bytes[pos] != b'"' {
        return None;
    }
    let mut i = pos + 1;
    let mut s = String::new();
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 1;
            if i < bytes.len() {
                s.push(bytes[i] as char);
            }
            i += 1;
        } else if bytes[i] == b'"' {
            return Some((s, i + 1));
        } else {
            s.push(bytes[i] as char);
            i += 1;
        }
    }
    None
}

/// Skip an entire JSON value (string, number, object, array, bool, null)
/// starting at `pos`. Returns the position after the value.
fn skip_json_value(bytes: &[u8], pos: usize) -> Option<usize> {
    if pos >= bytes.len() {
        return None;
    }
    match bytes[pos] {
        b'"' => {
            // String
            let mut i = pos + 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    return Some(i + 1);
                }
                i += 1;
            }
            None
        }
        b'{' => {
            // Object — skip matching braces
            let mut depth = 1;
            let mut i = pos + 1;
            let mut in_str = false;
            while i < bytes.len() && depth > 0 {
                if in_str {
                    if bytes[i] == b'\\' {
                        i += 1;
                    } else if bytes[i] == b'"' {
                        in_str = false;
                    }
                } else {
                    match bytes[i] {
                        b'"' => in_str = true,
                        b'{' => depth += 1,
                        b'}' => depth -= 1,
                        _ => {}
                    }
                }
                i += 1;
            }
            Some(i)
        }
        b'[' => {
            // Array — skip matching brackets
            let mut depth = 1;
            let mut i = pos + 1;
            let mut in_str = false;
            while i < bytes.len() && depth > 0 {
                if in_str {
                    if bytes[i] == b'\\' {
                        i += 1;
                    } else if bytes[i] == b'"' {
                        in_str = false;
                    }
                } else {
                    match bytes[i] {
                        b'"' => in_str = true,
                        b'[' => depth += 1,
                        b']' => depth -= 1,
                        _ => {}
                    }
                }
                i += 1;
            }
            Some(i)
        }
        b't' => {
            // true
            if pos + 4 <= bytes.len() {
                Some(pos + 4)
            } else {
                None
            }
        }
        b'f' => {
            // false
            if pos + 5 <= bytes.len() {
                Some(pos + 5)
            } else {
                None
            }
        }
        b'n' => {
            // null
            if pos + 4 <= bytes.len() {
                Some(pos + 4)
            } else {
                None
            }
        }
        b'-' | b'0'..=b'9' => {
            // Number
            let mut i = pos;
            if i < bytes.len() && bytes[i] == b'-' {
                i += 1;
            }
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'.' {
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
            if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
                i += 1;
                if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
                    i += 1;
                }
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
            Some(i)
        }
        _ => None,
    }
}

/// Public wrapper for `find_duplicate_json_key` so handlers can call it directly
/// (e.g. for CRD creation where serde silently merges duplicate keys).
pub fn find_duplicate_json_key_public(json_str: &str) -> Option<String> {
    find_duplicate_json_key(json_str)
}

/// Recursively find fields in `original` that are not present in `canonical`.
/// Returns a list of dotted field paths for unknown fields.
fn find_unknown_fields_recursive(
    original: &serde_json::Value,
    canonical: &serde_json::Value,
    prefix: &str,
    unknown: &mut Vec<String>,
) {
    match (original, canonical) {
        (serde_json::Value::Object(orig_map), serde_json::Value::Object(canon_map)) => {
            for (key, orig_val) in orig_map {
                let field_path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                if let Some(canon_val) = canon_map.get(key) {
                    // Recurse into nested objects
                    find_unknown_fields_recursive(orig_val, canon_val, &field_path, unknown);
                } else if !orig_val.is_null() {
                    // A field present in the original but missing from the
                    // canonical (re-serialized) JSON is unknown — unless its
                    // value is `null`.  Optional fields annotated with
                    // `skip_serializing_if = "Option::is_none"` deserialize
                    // `null` as `None` and then disappear on re-serialization,
                    // so a `null` value is never evidence of an unknown field.
                    // This matches upstream k8s behaviour: clients routinely
                    // send `"creationTimestamp": null` and similar.
                    unknown.push(field_path);
                }
            }
        }
        (serde_json::Value::Array(orig_arr), serde_json::Value::Array(canon_arr)) => {
            // For arrays, check element-by-element if both have the same length
            for (i, (orig_elem, canon_elem)) in orig_arr.iter().zip(canon_arr.iter()).enumerate() {
                let field_path = format!("{}[{}]", prefix, i);
                find_unknown_fields_recursive(orig_elem, canon_elem, &field_path, unknown);
            }
        }
        _ => {
            // Scalar values — nothing to check
        }
    }
}

/// Field-validation mode resolved from the `?fieldValidation=` query param.
///
/// Mirrors upstream k8s `apimachinery/pkg/runtime/serializer/json/json.go`
/// validation directive parsing. Starting with Kubernetes 1.25 (`PR #107807`,
/// promoted to GA in 1.27), the server-side default when the param is absent
/// is `Strict`. Earlier clients that omit the param therefore now get the same
/// behaviour as if they had asked for it explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldValidationMode {
    /// Reject unknown / duplicate fields with a 400 BadRequest.
    Strict,
    /// Accept unknown fields but emit a `Warning: 299` response header per
    /// offending field path.
    Warn,
    /// Accept unknown fields silently (drop them on read-back).
    Ignore,
}

impl FieldValidationMode {
    /// Resolve the mode from the query param map, defaulting to `Strict` when
    /// the param is absent. Unknown values fall back to `Strict` to match
    /// upstream's conservative behaviour.
    pub fn from_query(params: &HashMap<String, String>) -> Self {
        match params.get("fieldValidation").map(|v| v.as_str()) {
            Some("Warn") => Self::Warn,
            Some("Ignore") => Self::Ignore,
            // Strict, missing, or unknown values: default to Strict per
            // K8s 1.25+ server-side default.
            _ => Self::Strict,
        }
    }
}

/// Build the canonical `strict decoding error: ...` message from the collected
/// unknown / duplicate field paths.
fn build_strict_decoding_message(unknown: &[String], duplicates: &[String]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for field in unknown {
        parts.push(format!("unknown field \"{}\"", field));
    }
    for field in duplicates {
        parts.push(format!("duplicate field \"{}\"", field));
    }
    format!("strict decoding error: {}", parts.join(", "))
}

/// Validate the request body against the parsed resource per the
/// `?fieldValidation=` directive.
///
/// Behaviour by mode (matches upstream k8s 1.35):
/// - `Strict` (or absent param, since 1.25): unknown / duplicate fields are
///   rejected with `Error::BadRequest` → HTTP 400 reason=BadRequest. Message
///   format: `strict decoding error: unknown field "spec.foo", duplicate field
///   "spec.bar"`.
/// - `Warn`: unknown fields are returned in the `Ok(Vec<String>)` so the
///   handler can emit one `Warning: 299 - "..."` response header per field.
///   Duplicate fields are NOT enforced in Warn mode (matches upstream — only
///   strict decoding splits on duplicates).
/// - `Ignore`: empty vec, no enforcement.
///
/// On success the returned vector contains zero or more `unknown field "..."`
/// strings ready to be wrapped in the RFC 7234 Warning header value.
pub fn validate_strict_fields(
    params: &HashMap<String, String>,
    original_body: &[u8],
    parsed_resource: &impl serde::Serialize,
) -> Result<Vec<String>, Error> {
    let mode = FieldValidationMode::from_query(params);
    if matches!(mode, FieldValidationMode::Ignore) {
        return Ok(Vec::new());
    }

    // Parse original as generic JSON. A serde_json failure here is a true
    // syntactic error → BadRequest regardless of mode (matches upstream:
    // duplicate-field detection at the decoder level can't be downgraded by
    // ?fieldValidation=Warn).
    let original: serde_json::Value = serde_json::from_slice(original_body).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("duplicate field") {
            if let Some(field) = msg.split('`').nth(1) {
                return Error::BadRequest(format!(
                    "strict decoding error: json: unknown field \"{}\"",
                    field
                ));
            }
        }
        Error::BadRequest(msg)
    })?;

    // Re-serialize the parsed struct to get canonical JSON.
    let canonical =
        serde_json::to_value(parsed_resource).map_err(|e| Error::Internal(e.to_string()))?;

    // Collect unknown field paths.
    let mut unknown = Vec::new();
    find_unknown_fields_recursive(&original, &canonical, "", &mut unknown);

    // Collect duplicate field paths from the raw bytes (serde_json silently
    // takes the last duplicate so we must rescan).
    let duplicates: Vec<String> = std::str::from_utf8(original_body)
        .map(find_all_duplicate_json_keys)
        .unwrap_or_default();

    match mode {
        FieldValidationMode::Strict => {
            if unknown.is_empty() && duplicates.is_empty() {
                return Ok(Vec::new());
            }
            Err(Error::BadRequest(build_strict_decoding_message(
                &unknown,
                &duplicates,
            )))
        }
        FieldValidationMode::Warn => {
            // Warn mode: unknown fields become per-field warnings. Drop
            // duplicates here — Warn does not surface duplicate keys (they
            // would have already been merged by serde_json without raising).
            Ok(unknown
                .into_iter()
                .map(|field| format!("unknown field \"{}\"", field))
                .collect())
        }
        FieldValidationMode::Ignore => Ok(Vec::new()), // unreachable, handled above
    }
}

/// Build the RFC 7234 `Warning` header value for an unknown-field warning.
///
/// Upstream k8s (`apimachinery/pkg/util/warning/warning.go`) uses warn-code
/// `299` ("Miscellaneous persistent warning") and the agent token `-` to mean
/// "no agent identifier". Each unknown field becomes one header value:
///
/// ```text
/// Warning: 299 - "unknown field \"spec.foo\""
/// ```
pub fn format_warning_header(warning_text: &str) -> String {
    // `299 <agent> "<quoted-string>"` — agent token `-` is used when no
    // host:port identifier is available, matching upstream.
    let escaped = warning_text.replace('\\', "\\\\").replace('"', "\\\"");
    format!("299 - \"{}\"", escaped)
}

/// Validate that a resource name is a valid DNS subdomain name (RFC 1123).
///
/// Rules:
/// - Must be non-empty
/// - Must be <= 253 characters
/// - Must consist of lowercase alphanumeric characters, '-' or '.'
/// - Must start and end with an alphanumeric character
///
/// This is the standard validation for most Kubernetes resource names.
pub fn validate_resource_name(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::InvalidResource("name must be non-empty".to_string()));
    }

    if name.len() > 253 {
        return Err(Error::InvalidResource(format!(
            "name '{}' is too long: must be no more than 253 characters",
            name
        )));
    }

    // Check each character
    for c in name.chars() {
        if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-' && c != '.' {
            return Err(Error::InvalidResource(format!(
                "name '{}' is invalid: a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')",
                name
            )));
        }
    }

    // Must start and end with alphanumeric
    let first = name.chars().next().unwrap();
    let last = name.chars().last().unwrap();

    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(Error::InvalidResource(format!(
            "name '{}' is invalid: must start with an alphanumeric character",
            name
        )));
    }

    if !last.is_ascii_lowercase() && !last.is_ascii_digit() {
        return Err(Error::InvalidResource(format!(
            "name '{}' is invalid: must end with an alphanumeric character",
            name
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_names() {
        assert!(validate_resource_name("my-config").is_ok());
        assert!(validate_resource_name("test-123").is_ok());
        assert!(validate_resource_name("a").is_ok());
        assert!(validate_resource_name("my.config.map").is_ok());
        assert!(validate_resource_name("123").is_ok());
    }

    #[test]
    fn test_invalid_names() {
        // Empty
        assert!(validate_resource_name("").is_err());

        // Uppercase
        assert!(validate_resource_name("MyConfig").is_err());

        // Starts with dash
        assert!(validate_resource_name("-my-config").is_err());

        // Ends with dash
        assert!(validate_resource_name("my-config-").is_err());

        // Contains underscore
        assert!(validate_resource_name("my_config").is_err());

        // Contains space
        assert!(validate_resource_name("my config").is_err());

        // Too long (254 chars)
        let long_name = "a".repeat(254);
        assert!(validate_resource_name(&long_name).is_err());

        // Max length is OK (253 chars)
        let max_name = "a".repeat(253);
        assert!(validate_resource_name(&max_name).is_ok());
    }

    // --- Duplicate JSON key detection tests ---

    #[test]
    fn test_duplicate_key_top_level() {
        let json = r#"{"name": "a", "name": "b"}"#;
        assert_eq!(find_duplicate_json_key(json), Some("name".to_string()));
    }

    #[test]
    fn test_no_duplicate_keys() {
        let json = r#"{"name": "a", "value": "b"}"#;
        assert_eq!(find_duplicate_json_key(json), None);
    }

    #[test]
    fn test_duplicate_key_nested() {
        // Duplicate "replicas" inside "spec" — should be detected with dotted path
        let json = r#"{"metadata": {"name": "test"}, "spec": {"replicas": 1, "replicas": 2}}"#;
        assert_eq!(
            find_duplicate_json_key(json),
            Some("spec.replicas".to_string())
        );
    }

    #[test]
    fn test_duplicate_key_deeply_nested() {
        let json = r#"{"a": {"b": {"c": 1, "c": 2}}}"#;
        assert_eq!(find_duplicate_json_key(json), Some("a.b.c".to_string()));
    }

    #[test]
    fn test_duplicate_key_in_array_element() {
        let json = r#"{"items": [{"x": 1, "x": 2}]}"#;
        assert_eq!(
            find_duplicate_json_key(json),
            Some("items[0].x".to_string())
        );
    }

    #[test]
    fn test_no_duplicate_same_key_different_objects() {
        // "name" appears in both objects but each object has it once — no duplicate
        let json = r#"{"a": {"name": "x"}, "b": {"name": "y"}}"#;
        assert_eq!(find_duplicate_json_key(json), None);
    }

    #[test]
    fn test_empty_object() {
        assert_eq!(find_duplicate_json_key("{}"), None);
    }

    #[test]
    fn test_non_object() {
        assert_eq!(find_duplicate_json_key("[]"), None);
        assert_eq!(find_duplicate_json_key("42"), None);
    }

    // --- Strict field validation tests ---

    #[test]
    fn test_strict_validation_no_unknown_fields() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
            value: i32,
        }

        let body = br#"{"name": "test", "value": 42}"#;
        let parsed = Simple {
            name: "test".to_string(),
            value: 42,
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let warnings = validate_strict_fields(&params, body, &parsed).unwrap();
        assert!(warnings.is_empty(), "no warnings on clean body");
    }

    #[test]
    fn test_strict_validation_unknown_field() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "test", "extra": "field"}"#;
        let parsed = Simple {
            name: "test".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("unknown field"),
            "Expected 'unknown field' in error: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_duplicate_field() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "a", "name": "b"}"#;
        let parsed = Simple {
            name: "b".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("duplicate field"),
            "Expected 'duplicate field' in error: {}",
            err_msg
        );
        assert!(
            err_msg.contains("name"),
            "Expected field name in error: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_default_is_strict() {
        // K8s 1.25+ default: missing ?fieldValidation= behaves like
        // ?fieldValidation=Strict, so unknown fields must be rejected.
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "test", "extra": "field"}"#;
        let parsed = Simple {
            name: "test".to_string(),
        };
        let params = HashMap::new(); // no fieldValidation param

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(
            result.is_err(),
            "default mode must reject unknown fields (K8s 1.25+ default is Strict)"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("strict decoding error") && err_msg.contains("unknown field"),
            "default rejection should match strict decoder format: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_ignore_mode_allows_unknown() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "test", "extra": "field"}"#;
        let parsed = Simple {
            name: "test".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Ignore".to_string());

        let warnings = validate_strict_fields(&params, body, &parsed).unwrap();
        assert!(warnings.is_empty(), "Ignore mode must not surface warnings");
    }

    #[test]
    fn test_strict_validation_warn_mode_returns_warnings() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "test", "extra": "field"}"#;
        let parsed = Simple {
            name: "test".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Warn".to_string());

        // Warn mode must surface unknown fields as warning strings (no error).
        let warnings = validate_strict_fields(&params, body, &parsed).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("unknown field") && warnings[0].contains("extra"),
            "warning must identify the unknown field: {:?}",
            warnings
        );
    }

    #[test]
    fn test_strict_validation_nested_duplicate() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Outer {
            spec: Inner,
        }
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Inner {
            replicas: i32,
        }

        let body = br#"{"spec": {"replicas": 1, "replicas": 2}}"#;
        let parsed = Outer {
            spec: Inner { replicas: 2 },
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("duplicate field"),
            "Expected 'duplicate field' in error: {}",
            err_msg
        );
        assert!(
            err_msg.contains("spec.replicas"),
            "Expected 'spec.replicas' dotted path in error: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_error_format_matches_k8s() {
        // K8s returns: strict decoding error: json: unknown field "fieldName"
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "a", "name": "b"}"#;
        let parsed = Simple {
            name: "b".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains(r#"strict decoding error:"#)
                && err_msg.contains(r#"duplicate field "name""#),
            "Error format must match K8s duplicate field detection: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_combined_unknown_and_duplicate() {
        // K8s returns both unknown and duplicate field errors in a single message
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Outer {
            spec: Inner,
        }
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Inner {
            replicas: i32,
        }

        // Body has unknown field "spec.unknownField" AND duplicate "spec.replicas"
        let body = br#"{"spec": {"unknownField": "foo", "replicas": 1, "replicas": 2}}"#;
        let parsed = Outer {
            spec: Inner { replicas: 2 },
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        // Should contain both errors
        assert!(
            err_msg.contains(r#"unknown field "spec.unknownField""#),
            "Expected unknown field error: {}",
            err_msg
        );
        assert!(
            err_msg.contains(r#"duplicate field "spec.replicas""#),
            "Expected duplicate field error: {}",
            err_msg
        );
        // Should be combined in a single strict decoding error
        assert!(
            err_msg.contains("strict decoding error:"),
            "Expected strict decoding error prefix: {}",
            err_msg
        );
    }

    #[test]
    fn test_find_all_duplicate_json_keys_multiple() {
        // Test that we find ALL duplicate keys, not just the first
        let json = r#"{"a": 1, "a": 2, "b": {"c": 1, "c": 2}}"#;
        let dups = find_all_duplicate_json_keys(json);
        assert_eq!(dups.len(), 2, "Expected 2 duplicates, got: {:?}", dups);
        assert!(dups.contains(&"a".to_string()));
        assert!(dups.contains(&"b.c".to_string()));
    }

    #[test]
    fn test_find_all_duplicate_json_keys_dotted_paths() {
        let json = r#"{"spec": {"replicas": 1, "replicas": 2}}"#;
        let dups = find_all_duplicate_json_keys(json);
        assert_eq!(dups, vec!["spec.replicas".to_string()]);
    }

    #[test]
    fn test_format_warning_header_shape() {
        // Upstream emits `Warning: 299 - "..."` per RFC 7234.
        let header = format_warning_header(r#"unknown field "spec.bogus""#);
        assert!(
            header.starts_with("299 "),
            "header must start with warn-code 299: {}",
            header
        );
        assert!(
            header.contains("spec.bogus"),
            "header must mention the unknown field: {}",
            header
        );
        assert!(
            header.contains(r#"\"spec.bogus\""#),
            "field name should be quoted/escaped: {}",
            header
        );
    }

    #[test]
    fn test_field_validation_mode_from_query_defaults_to_strict() {
        let empty = HashMap::new();
        assert_eq!(
            FieldValidationMode::from_query(&empty),
            FieldValidationMode::Strict
        );

        let mut warn = HashMap::new();
        warn.insert("fieldValidation".to_string(), "Warn".to_string());
        assert_eq!(
            FieldValidationMode::from_query(&warn),
            FieldValidationMode::Warn
        );

        let mut ignore = HashMap::new();
        ignore.insert("fieldValidation".to_string(), "Ignore".to_string());
        assert_eq!(
            FieldValidationMode::from_query(&ignore),
            FieldValidationMode::Ignore
        );

        let mut strict = HashMap::new();
        strict.insert("fieldValidation".to_string(), "Strict".to_string());
        assert_eq!(
            FieldValidationMode::from_query(&strict),
            FieldValidationMode::Strict
        );
    }
}
