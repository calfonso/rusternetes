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

/// Assert `expected` is a deep subset of `actual`: every key/element present in
/// `expected` must appear identically in `actual`. Extra keys in `actual` are
/// tolerated (the envelope decoder injects `apiVersion`/`kind` from TypeMeta,
/// and the codec may default-fill fields). This catches the two failure modes
/// that matter for request decoding — a field being *corrupted* (issue #495,
/// where an embedded-JSON string was replaced) or *lost* (the map<string,Time>
/// bug) — without flagging benign additions.
fn json_contains(expected: &Value, actual: &Value) -> Result<(), String> {
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => {
            for (k, ev) in e {
                match a.get(k) {
                    Some(av) => json_contains(ev, av).map_err(|m| format!("/{k}{m}"))?,
                    None => return Err(format!("/{k} MISSING (expected {ev})")),
                }
            }
            Ok(())
        }
        (Value::Array(e), Value::Array(a)) => {
            if e.len() != a.len() {
                return Err(format!(" array len {} != {}", e.len(), a.len()));
            }
            for (i, (ev, av)) in e.iter().zip(a).enumerate() {
                json_contains(ev, av).map_err(|m| format!("/[{i}]{m}"))?;
            }
            Ok(())
        }
        _ => {
            if expected == actual {
                Ok(())
            } else {
                Err(format!(" CHANGED: {expected} -> {actual}"))
            }
        }
    }
}

/// Append a protobuf length-delimited field (`wire type 2`) to `buf`.
fn push_len_delim(buf: &mut Vec<u8>, field: u8, data: &[u8]) {
    buf.push((field << 3) | 2);
    let mut len = data.len();
    loop {
        let mut b = (len & 0x7f) as u8;
        len >>= 7;
        if len != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if len == 0 {
            break;
        }
    }
    buf.extend_from_slice(data);
}

/// Build the `k8s\0` protobuf envelope a typed client sends: an `Unknown`
/// message with `typeMeta` (field 1, carrying apiVersion+kind) and `raw`
/// (field 2, the native-protobuf-encoded resource).
fn k8s_envelope(api_version: &str, kind: &str, raw: &[u8]) -> Vec<u8> {
    let mut type_meta = Vec::new();
    push_len_delim(&mut type_meta, 1, api_version.as_bytes()); // apiVersion
    push_len_delim(&mut type_meta, 2, kind.as_bytes()); // kind
    let mut env = b"k8s\0".to_vec();
    push_len_delim(&mut env, 1, &type_meta); // Unknown.typeMeta
    push_len_delim(&mut env, 2, raw); // Unknown.raw
    env
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

#[test]
fn json_value_survives_cbor_roundtrip() {
    // CBOR is the 1.35 default wire format for several clients (KEP-4222). The
    // request middleware decodes it via `decode_cbor_to_json`. A value encoded
    // to CBOR and back must be byte-for-byte identical — number widening, string
    // mangling, or map reordering here would silently corrupt every CBOR
    // request. (The #954 CRD struct-form bug was this class, one layer up.)
    use rusternetes_api_server::cbor::{decode_cbor_to_json, encode_json_to_cbor};

    let reg = ProtoRegistry::new();
    let schemas = all_schemas(&reg);
    let mut failures: Vec<String> = Vec::new();
    let mut tested = 0usize;

    let mut names: Vec<&String> = schemas.keys().collect();
    names.sort();

    for name in names {
        let value = match synth_message(name, &schemas, 4) {
            Some(v) => v,
            None => continue,
        };
        let cbor = match encode_json_to_cbor(&value) {
            Ok(b) => b,
            Err(e) => {
                failures.push(format!("{name}: encode_json_to_cbor error: {e}"));
                continue;
            }
        };
        let back = match decode_cbor_to_json(&cbor) {
            Ok(v) => v,
            Err(e) => {
                failures.push(format!("{name}: decode_cbor_to_json error: {e}"));
                continue;
            }
        };
        tested += 1;
        if back != value {
            failures.push(format!(
                "{name}: CBOR roundtrip mismatch\n  in : {}\n  out: {}",
                serde_json::to_string(&value).unwrap(),
                serde_json::to_string(&back).unwrap(),
            ));
        }
    }

    eprintln!(
        "cbor roundtrip: {tested} kinds tested, {} failures",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} CBOR roundtrip failures:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

#[test]
fn resource_survives_protobuf_envelope_request_path() {
    // The real request decode path: a typed client encodes a resource to native
    // protobuf, wraps it in the `k8s\0` Unknown envelope, and POSTs it. The
    // middleware runs `decode_k8s_protobuf_request_body` (extract → schema
    // registry → CRD decoder → brace-scan). This is exactly where issue #495
    // lived: an adversarial string in a command field was mistaken for the body.
    //
    // We seed every metadata-bearing (i.e. top-level) resource with the
    // adversarial corpus, push it through the genuine middleware function, and
    // assert no synthesized field was corrupted or lost.
    use rusternetes_api_server::middleware::decode_k8s_protobuf_request_body;

    let reg = ProtoRegistry::new();
    let schemas = all_schemas(&reg);
    let mut failures: Vec<String> = Vec::new();
    let mut tested = 0usize;

    let mut names: Vec<&String> = schemas.keys().collect();
    names.sort();

    for name in names {
        if name == "Time" || name == "MicroTime" {
            continue;
        }
        let value = match synth_message(name, &schemas, 4) {
            Some(v) => v,
            None => continue,
        };
        // Only exercise real top-level resources (those carrying ObjectMeta) —
        // sub-messages are never sent as a standalone request body.
        if !value.get("metadata").is_some_and(|m| m.is_object()) {
            continue;
        }

        let raw = match reg.encode_message(name, &value) {
            Some(b) if !b.is_empty() => b,
            _ => continue,
        };
        // raw must be native protobuf (not literal JSON) so we hit the decode
        // cascade the bug lived in, not the raw-is-already-JSON shortcut.
        assert_ne!(
            raw.first(),
            Some(&b'{'),
            "{name}: encoded protobuf unexpectedly starts with '{{'"
        );

        let envelope = k8s_envelope("v1", name, &raw);
        let json_bytes = decode_k8s_protobuf_request_body(&envelope);
        let decoded: Value = match serde_json::from_slice(&json_bytes) {
            Ok(v) => v,
            Err(e) => {
                failures.push(format!(
                    "{name}: middleware output is not valid JSON: {e}\n  body: {}",
                    String::from_utf8_lossy(&json_bytes)
                ));
                continue;
            }
        };
        tested += 1;
        if let Err(path) = json_contains(&value, &decoded) {
            failures.push(format!(
                "{name}: field {path}\n  sent   : {}\n  decoded: {}",
                serde_json::to_string(&value).unwrap(),
                serde_json::to_string(&decoded).unwrap(),
            ));
        }
    }

    eprintln!(
        "envelope request-path: {tested} resources tested, {} failures",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} envelope request-path failures:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
