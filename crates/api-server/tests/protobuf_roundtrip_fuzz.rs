//! Roundtrip / fuzz serialization harness.
//!
//! A Rust analog of upstream k8s `apimachinery/pkg/api/apitesting/roundtrip`
//! (`TestRoundTripTypes`). For every message schema registered in
//! `ProtoRegistry`, it synthesizes a representative JSON value — deliberately
//! seeding string fields with an *adversarial corpus* (embedded JSON, brace
//! characters, the `k8s\0` magic, quotes) — and asserts the value survives a
//! lossless roundtrip through the protobuf codec.
//!
//! Why this exists: every wire-decode bug we have hit (the brace-scan #495, the
//! CBOR struct-form #954, the empty protobuf schemas #43/#44, the missing-field
//! decodes #10/#11/#19) lived in bespoke byte-level code, not in serde. Those
//! bugs only surfaced end-to-end via conformance, as cryptic errors five layers
//! deep. This harness makes that whole class fail at `cargo test` speed.
//!
//! The headline invariant is registry symmetry:
//!   `decode_message(kind, encode_message(kind, v)) == v`
//! which must hold for any `v` built only from values the codec preserves
//! (no empty/default values, which protobuf omits). The synthesizer below is
//! careful to only emit such values.

use rusternetes_api_server::protobuf::{FieldType, MessageSchema, ProtoRegistry};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

/// Adversarial string corpus. Every entry is a string that has, at some point,
/// confused a hand-written decoder. The most important are the ones containing
/// embedded JSON (issue #495) and the protobuf envelope magic.
const ADVERSARIAL_STRINGS: &[&str] = &[
    "plain-value",
    r#"--post-data={"Source": "prestop"}"#, // issue #495: embedded JSON in a command arg
    r#"{"nested": {"a": 1}}"#,              // a full JSON object as a string value
    r#"[1,2,3]"#,                           // a JSON array as a string value
    "has \"quotes\" and \\ backslash",      // string-escaping edge cases
    "k8s\u{0}magic-prefix",                 // the protobuf envelope magic inside a value
    "}{ unbalanced braces }{",              // brace-scan confusion
    "trailing-brace}",                      // partial JSON
];

/// Pick an adversarial string deterministically from a rotating salt so
/// different fields in the same message get different (but reproducible) values.
fn adversarial(salt: usize) -> Value {
    Value::String(ADVERSARIAL_STRINGS[salt % ADVERSARIAL_STRINGS.len()].to_string())
}

/// Synthesize a JSON value for a field type that the codec is expected to
/// roundtrip *exactly*. Returns `None` for field types we intentionally skip in
/// this harness (those whose encode/decode is asymmetric by design — inline
/// messages flatten, JsonRaw re-defaults — and would produce false positives).
fn synth(
    ft: &FieldType,
    schemas: &HashMap<String, MessageSchema>,
    salt: usize,
    depth: usize,
) -> Option<Value> {
    match ft {
        FieldType::String => Some(adversarial(salt)),
        FieldType::Int => Some(json!(7 + (salt as i64 % 11))),
        FieldType::Double => Some(json!(1.5)),
        FieldType::Bool => Some(json!(true)),
        FieldType::IntOrString => Some(adversarial(salt)), // string branch
        FieldType::Quantity => Some(json!("100m")),
        FieldType::Bytes => Some(json!("aGk=")), // base64("hi"), canonical
        FieldType::StringMap => {
            Some(json!({ "k1": ADVERSARIAL_STRINGS[salt % ADVERSARIAL_STRINGS.len()] }))
        }
        FieldType::BytesMap => Some(json!({ "k1": "aGk=" })),
        FieldType::QuantityMap => Some(json!({ "cpu": "100m" })),
        FieldType::Message(name) => synth_message(name, schemas, depth),
        FieldType::MessageMap(name) => {
            synth_message(name, schemas, depth).map(|m| json!({ "k1": m }))
        }
        FieldType::Repeated(inner) => {
            synth(inner, schemas, salt, depth).map(|v| Value::Array(vec![v]))
        }
        // Skipped: asymmetric-by-design, would create harness false positives.
        FieldType::InlineMessage(_) => None,
        FieldType::JsonRaw => None,
    }
}

/// Synthesize a full message object from its schema. `depth` bounds recursion so
/// self-referential schemas (e.g. JSONSchemaProps) terminate.
fn synth_message(
    name: &str,
    schemas: &HashMap<String, MessageSchema>,
    depth: usize,
) -> Option<Value> {
    // Time-like messages are represented on the wire as a {seconds,nanos}
    // submessage but decode to / encode from a canonical RFC3339 string in JSON
    // (matching how real clients send them). Synthesize the string form so the
    // roundtrip is exact. Use whole-second values: Time has second precision and
    // MicroTime microsecond precision, so sub-resolution digits would be dropped.
    match name {
        "Time" => return Some(Value::String("2020-01-02T03:04:05Z".to_string())),
        "MicroTime" => return Some(Value::String("2020-01-02T03:04:05.000000Z".to_string())),
        _ => {}
    }
    if depth == 0 {
        return None;
    }
    let schema = schemas.get(name)?;
    let mut obj = Map::new();
    // Deterministic field order by field number.
    let mut fields: Vec<(&u32, &(String, FieldType))> = schema.fields.iter().collect();
    fields.sort_by_key(|(num, _)| **num);
    for (num, (json_name, ft)) in fields {
        if let Some(v) = synth(ft, schemas, *num as usize, depth - 1) {
            obj.insert(json_name.clone(), v);
        }
    }
    if obj.is_empty() {
        return None;
    }
    Some(Value::Object(obj))
}

/// Collect every registered schema into a name -> schema map.
fn all_schemas(reg: &ProtoRegistry) -> HashMap<String, MessageSchema> {
    reg.iter_schemas()
        .map(|(name, schema)| (name.to_string(), schema.clone()))
        .collect()
}

#[test]
fn registry_encode_decode_is_symmetric_for_every_kind() {
    let reg = ProtoRegistry::new();
    let schemas = all_schemas(&reg);
    assert!(!schemas.is_empty(), "registry has no schemas");

    let mut failures: Vec<String> = Vec::new();
    let mut tested = 0usize;
    let mut skipped: Vec<String> = Vec::new();

    let mut names: Vec<&String> = schemas.keys().collect();
    names.sort();

    for name in names {
        // Time/MicroTime are scalar-string helper types, never sent as a
        // top-level request body — synthesizing them at the top level produces
        // a bare string that isn't a valid standalone message. They are still
        // exercised thoroughly as nested fields and map values elsewhere.
        if name == "Time" || name == "MicroTime" {
            skipped.push(name.clone());
            continue;
        }
        // Build a representative value for this kind (depth 4 covers nested
        // structures while terminating self-referential schemas).
        let value = match synth_message(name, &schemas, 4) {
            Some(v) => v,
            None => {
                skipped.push(name.clone());
                continue;
            }
        };

        let bytes = match reg.encode_message(name, &value) {
            Some(b) => b,
            None => {
                failures.push(format!("{name}: encode_message returned None"));
                continue;
            }
        };
        let decoded = match reg.decode_message(name, &bytes) {
            Some(v) => v,
            None => {
                failures.push(format!("{name}: decode_message returned None"));
                continue;
            }
        };
        tested += 1;

        if decoded != value {
            failures.push(format!(
                "{name}: roundtrip mismatch\n  in : {}\n  out: {}",
                serde_json::to_string(&value).unwrap(),
                serde_json::to_string(&decoded).unwrap(),
            ));
        }
    }

    eprintln!(
        "roundtrip harness: {} kinds tested, {} skipped (inline/jsonraw-only), {} failures",
        tested,
        skipped.len(),
        failures.len()
    );

    assert!(
        failures.is_empty(),
        "{} roundtrip failures:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
