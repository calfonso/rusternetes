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

/// Returns true if `value` is one that our resource structs' `skip_serializing_if`
/// helpers legitimately DROP on serialize: `null` (`Option::is_none`), `false`
/// (`skip_false_or_none`), `""` (`skip_empty_string`), `[]` (`skip_empty_vec`),
/// and `{}` (`skip_empty_map` / collapsed empty struct).
///
/// When the canonical round-trip omits a key whose original value is one of
/// these, the absence is AMBIGUOUS — it could be a genuinely-unknown field OR a
/// known field whose zero value was dropped. The diff-based detector cannot
/// tell the two apart, so it must defer to the schema-aware (`serde_ignored`)
/// pass, which inspects the target type's field declarations directly and never
/// mistakes a modelled-but-zero-valued field (e.g. JSONSchemaProps'
/// `exclusiveMaximum: false`) for an unknown one. Without this, a CRD whose
/// validation schema explicitly sets such fields to their zero value was
/// rejected with a spurious `strict decoding error: unknown field ...`.
///
/// Numbers are intentionally excluded: no struct field drops a numeric zero
/// (those use `Option::is_none`, not a value-based skip), so an omitted numeric
/// key genuinely is unknown.
fn is_droppable_default(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => true,
        serde_json::Value::Bool(b) => !b,
        serde_json::Value::String(s) => s.is_empty(),
        serde_json::Value::Array(a) => a.is_empty(),
        serde_json::Value::Object(o) => o.is_empty(),
        serde_json::Value::Number(_) => false,
    }
}

/// Render a `serde_ignored::Path` chain into the dotted+bracket format the
/// upstream k8s strict-decoder uses (`spec.containers[0].image`).
///
/// `serde_ignored::Path`'s own `Display` impl renders sequence indices with
/// dots (`spec.containers.0.image`), which doesn't match the
/// `strict decoding error: unknown field "spec.containers[0].image"` shape
/// every parity test pins against upstream output.
fn format_ignored_path(path: &serde_ignored::Path<'_>) -> String {
    use serde_ignored::Path;
    match path {
        Path::Root => String::new(),
        Path::Seq { parent, index } => {
            let p = format_ignored_path(parent);
            if p.is_empty() {
                format!("[{}]", index)
            } else {
                format!("{}[{}]", p, index)
            }
        }
        Path::Map { parent, key } => {
            let p = format_ignored_path(parent);
            if p.is_empty() {
                key.clone()
            } else {
                format!("{}.{}", p, key)
            }
        }
        Path::Some { parent }
        | Path::NewtypeStruct { parent }
        | Path::NewtypeVariant { parent } => format_ignored_path(parent),
    }
}

/// Collect every field path in `original` that is NOT declared on the
/// target type `T`, using schema-aware re-deserialization via the
/// `serde_ignored` crate.
///
/// This is the precise (value-agnostic) half of the unknown-field check
/// — it catches unknown keys regardless of whether their value is null,
/// `{}`, or anything else. It closes the long-standing false-negative
/// where the older canonical-vs-original diff couldn't distinguish
/// "truly unknown null-valued field" from "legit `Option<...>` field
/// round-trip-dropped because the value was null".
///
/// Upstream k8s detects unknown fields at the decoder level
/// (`apimachinery/pkg/runtime/serializer/json/json.go`'s
/// `UnmarshalCaseSensitivePreserveInts` + `DisallowUnknownFields`); the
/// `serde_ignored` callback is the Rust equivalent — it fires for every
/// key the target type's `Visitor` did not consume.
///
/// Known limitation: `#[serde(flatten)]` short-circuits serde's
/// per-field tracking — leftover keys get funnelled through
/// `FlatMapDeserializer`, which doesn't propagate ignored-key events
/// back out to the outer callback. The diff-based helper below covers
/// that gap (it only inspects the canonical round-trip output, so it's
/// unaffected by flatten internals).
fn find_unknown_fields_via_schema<T>(original: &serde_json::Value) -> Vec<String>
where
    T: serde::de::DeserializeOwned,
{
    let mut unknown: Vec<String> = Vec::new();
    // The result is intentionally discarded — strict decoding only cares
    // about whether unknown keys were encountered, not whether the typed
    // deserialise itself succeeded. The handler has already produced a
    // valid `T` from the same body; if `from_value` here returns an Err
    // we still want to surface the ignored-key callbacks that fired
    // before the failure point.
    let _ = serde_ignored::deserialize::<_, _, T>(original, |path| {
        unknown.push(format_ignored_path(&path));
    });
    unknown
}

/// Diff-based unknown-field discovery — the fallback that catches keys
/// `serde_ignored` misses because `#[serde(flatten)]` short-circuits its
/// per-field tracking.
///
/// Compares every key in `original` against the canonical round-trip of
/// the typed parse. A key present in `original` but absent from
/// `canonical` is reported as unknown — EXCEPT when its value is JSON
/// `null` or `{}`, which a typed deserialiser may legitimately have
/// folded into `Option::None` and the canonical serialise dropped via
/// `skip_serializing_if = "Option::is_none"`. Those ambiguous cases are
/// the schema-aware helper's responsibility (it can distinguish "legit
/// `Option` field" from "truly unknown" without value-based heuristics).
fn find_unknown_fields_via_diff(
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
                    find_unknown_fields_via_diff(orig_val, canon_val, &field_path, unknown);
                } else if is_droppable_default(orig_val) {
                    // Ambiguous case — the canonical round-trip dropped this key,
                    // but its value (null / false / "" / [] / {}) is exactly what
                    // a `skip_serializing_if` helper drops for a KNOWN field. Let
                    // the schema-aware helper decide: it reads the target type's
                    // field declarations directly via serde and won't be fooled
                    // by a modelled-but-zero-valued field whose round-trip drops
                    // the key (e.g. JSONSchemaProps' `exclusiveMaximum: false`).
                } else {
                    unknown.push(field_path);
                }
            }
        }
        (serde_json::Value::Array(orig_arr), serde_json::Value::Array(canon_arr)) => {
            // Walk element-wise when the array lengths line up so nested
            // unknown keys inside array elements (e.g.
            // `spec.containers[0].bogus`) get reported with their full
            // dotted+bracket path.
            for (i, (orig_elem, canon_elem)) in orig_arr.iter().zip(canon_arr.iter()).enumerate() {
                let field_path = format!("{}[{}]", prefix, i);
                find_unknown_fields_via_diff(orig_elem, canon_elem, &field_path, unknown);
            }
        }
        _ => {
            // Scalar values — nothing to check.
        }
    }
}

/// Union of the schema-aware (value-agnostic) and diff-based (flatten-
/// resilient) unknown-field detectors. Each catches what the other
/// misses; together they reproduce the upstream k8s strict-decoder's
/// behaviour for the resource shapes the api-server handles.
///
/// Returns `Err(Error::Internal)` if `to_value` on the parsed resource
/// fails. A silent fallback to an empty canonical would make the diff
/// pass flag every legit top-level field as unknown — propagating the
/// error preserves the pre-existing behaviour (PR #687-era code raised
/// `Error::Internal` here too).
fn find_unknown_fields_combined<T>(
    original: &serde_json::Value,
    parsed_resource: &T,
) -> Result<Vec<String>, Error>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let mut unknown = find_unknown_fields_via_schema::<T>(original);
    // Defensive: drop any empty-string path the schema collector could
    // produce (only possible if `serde_ignored` ever fired with
    // `Path::Root`). `unknown field ""` would mask the real problem.
    unknown.retain(|p| !p.is_empty());

    // Serialize the parsed struct so the diff-based helper has a
    // canonical reference.
    let canonical = serde_json::to_value(parsed_resource).map_err(|e| {
        Error::Internal(format!(
            "strict decoding: failed to canonicalise parsed resource for diff pass: {}",
            e
        ))
    })?;
    let mut diff_unknown: Vec<String> = Vec::new();
    find_unknown_fields_via_diff(original, &canonical, "", &mut diff_unknown);

    // Union without duplicates while preserving discovery order so the
    // resulting error message is deterministic across runs.
    let mut seen: HashSet<String> = unknown.iter().cloned().collect();
    for path in diff_unknown {
        if seen.insert(path.clone()) {
            unknown.push(path);
        }
    }
    Ok(unknown)
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
pub fn validate_strict_fields<T>(
    params: &HashMap<String, String>,
    original_body: &[u8],
    parsed_resource: &T,
) -> Result<Vec<String>, Error>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
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

    // Collect unknown field paths via the combined detector. The
    // schema-aware (serde_ignored) pass catches null- and empty-object-
    // valued unknowns that the diff couldn't distinguish from legit
    // `Option<...>` fields whose round-trip drops the key; the
    // diff-based pass catches keys hidden from serde_ignored by
    // `#[serde(flatten)]` short-circuits (e.g. wrong-cased `APIVersion`
    // at a TypeMeta-flattened top level).
    let unknown = find_unknown_fields_combined::<T>(&original, parsed_resource)?;

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

    /// A CRD whose validation schema sets standard JSON-Schema fields to their
    /// zero value (`exclusiveMaximum: false`, `nullable: false`,
    /// `x-kubernetes-int-or-string: false`, `uniqueItems: false`) and uses an
    /// array property with `items` must pass strict field validation — those
    /// fields ARE modelled on JSONSchemaProps. They were falsely reported as
    /// `unknown field` because the diff-based detector re-serialises the parsed
    /// CRD (which drops `false`/empty values via `skip_serializing_if`) and
    /// flagged every dropped-but-known key.
    ///
    /// Reproduces the CustomResourcePublishOpenAPI / CustomResourceDefinition /
    /// FieldValidation conformance cluster, all of which failed at CRD creation
    /// with `strict decoding error: unknown field
    /// "spec.versions[0].schema.openAPIV3Schema.exclusiveMaximum", ...`.
    #[test]
    fn crd_with_zero_valued_schema_fields_passes_strict() {
        use rusternetes_common::resources::CustomResourceDefinition;

        let body = br#"{
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "foos.stable.example.com"},
            "spec": {
                "group": "stable.example.com",
                "names": {"plural": "foos", "singular": "foo", "kind": "Foo", "listKind": "FooList"},
                "scope": "Namespaced",
                "versions": [{
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "nullable": false,
                            "exclusiveMaximum": false,
                            "exclusiveMinimum": false,
                            "uniqueItems": false,
                            "x-kubernetes-int-or-string": false,
                            "x-kubernetes-embedded-resource": false,
                            "properties": {
                                "spec": {
                                    "type": "object",
                                    "nullable": false,
                                    "exclusiveMaximum": false,
                                    "properties": {
                                        "cronSpec": {"type": "string", "nullable": false, "exclusiveMaximum": false},
                                        "bars": {
                                            "description": "List of Bars and their specs.",
                                            "type": "array",
                                            "nullable": false,
                                            "items": {
                                                "type": "object",
                                                "nullable": false,
                                                "uniqueItems": false,
                                                "x-kubernetes-int-or-string": false,
                                                "required": ["name"],
                                                "properties": {
                                                    "name": {"type": "string", "nullable": false},
                                                    "feeling": {"type": "string", "enum": ["Great", "Down"]},
                                                    "bazs": {
                                                        "description": "List of Bazs.",
                                                        "type": "array",
                                                        "nullable": false,
                                                        "items": {"type": "string", "nullable": false}
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
        }"#;

        let parsed: CustomResourceDefinition =
            serde_json::from_slice(body).expect("CRD must deserialize");
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(
            result.is_ok() && result.as_ref().unwrap().is_empty(),
            "CRD with zero-valued JSONSchemaProps fields must pass strict validation, got: {:?}",
            result
        );
    }

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
