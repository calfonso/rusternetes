//! Protobuf wire-decode test for `Secret.data`.
//!
//! Upstream `Secret` in `staging/src/k8s.io/api/core/v1/generated.proto`:
//!
//! ```proto
//! message Secret {
//!   optional ObjectMeta             metadata   = 1;
//!   map<string, bytes>              data       = 2;
//!   optional string                 type       = 3;
//!   map<string, string>             stringData = 4;
//!   optional bool                   immutable  = 5;
//! }
//! ```
//!
//! On the wire, a `map<string, bytes>` is a repeated `MapEntry` message
//! whose key is a `string` field 1 and value is a `bytes` field 2. K8s
//! clients (client-go, kubectl with `vnd.kubernetes.protobuf`) base64-encode
//! the bytes when projecting the message back into JSON, so that the JSON
//! wire shape stays identical to what JSON-native clients write.
//!
//! `crates/api-server/src/protobuf.rs` registers `Secret.data` as
//! `FieldType::BytesMap` (proto field 2). This test pins that
//! registration end-to-end:
//!
//!   - hand-build a `Secret` proto body that carries a single `data` entry
//!     with `key="api-key"` and `value=[0x00, 0xFF, 0x42]`
//!   - decode through `ProtoRegistry::decode_message("Secret", ...)`
//!   - assert the resulting JSON contains
//!     `{"data": {"api-key": "AP9C"}}` — the standard-padded base64
//!     encoding of those three bytes
//!
//! If the schema regresses (field number drifts, field type swaps to
//! `StringMap`, value side stops being base64-encoded), this test catches
//! it before a conformance run does. It mirrors the layer that
//! `conformance_configmap_envfrom_prefixes_test.rs` adds for the
//! `ConfigMapEnvSource` / `SecretEnvSource` inline-flattening fix.

use base64::Engine;
use rusternetes_api_server::protobuf::ProtoRegistry;

/// Proto wire constants. Mirror the values used in the existing
/// `conformance_configmap_envfrom_prefixes_test.rs` so they stay consistent
/// across the test suite. `(field_number << 3) | wire_type` is the canonical
/// tag encoding from upstream `encoding/protowire`.
const WIRE_LENGTH_DELIMITED: u8 = 2;

fn tag(field_number: u8, wire_type: u8) -> u8 {
    (field_number << 3) | wire_type
}

/// Build a `MapEntry` proto for `map<string, bytes>`:
///
/// ```proto
/// message MapEntry { string key = 1; bytes value = 2; }
/// ```
///
/// The key (field 1) and value (field 2) are both length-delimited.
/// Lengths above 127 require multi-byte varints; the inputs used by this
/// test stay well below that so we encode the length as a single byte and
/// `debug_assert!` the bound — easier to read than wiring up a full varint
/// encoder for two callers.
fn map_entry_string_bytes(key: &str, value: &[u8]) -> Vec<u8> {
    debug_assert!(
        key.len() < 128,
        "key length must fit in a single-byte varint"
    );
    debug_assert!(
        value.len() < 128,
        "value length must fit in a single-byte varint"
    );

    let mut entry = Vec::with_capacity(4 + key.len() + value.len());
    // field 1, length-delimited string
    entry.push(tag(1, WIRE_LENGTH_DELIMITED));
    entry.push(key.len() as u8);
    entry.extend_from_slice(key.as_bytes());
    // field 2, length-delimited bytes
    entry.push(tag(2, WIRE_LENGTH_DELIMITED));
    entry.push(value.len() as u8);
    entry.extend_from_slice(value);
    entry
}

/// Build a `Secret` proto body with a single `data` MapEntry. Field 2 of
/// `Secret` is the `data` map, encoded as repeated length-delimited
/// `MapEntry` submessages.
fn secret_with_single_data_entry(key: &str, value: &[u8]) -> Vec<u8> {
    let entry = map_entry_string_bytes(key, value);
    debug_assert!(entry.len() < 128, "entry must fit in a single-byte length");

    let mut buf = Vec::with_capacity(2 + entry.len());
    // Secret.data = field 2, length-delimited (one MapEntry submessage)
    buf.push(tag(2, WIRE_LENGTH_DELIMITED));
    buf.push(entry.len() as u8);
    buf.extend_from_slice(&entry);
    buf
}

/// Canonical end-to-end shape check: a single `data` entry with binary
/// payload must surface in the decoded JSON as
/// `{"data": {"<key>": "<standard-padded base64>"}}`.
///
/// The bytes `[0x00, 0xFF, 0x42]` are not valid UTF-8, so this also doubles
/// as a regression test for any future change that accidentally tries to
/// stringify the `bytes` value before base64-encoding it.
#[test]
fn test_secret_data_proto_decode_base64_encodes_bytes() {
    let registry = ProtoRegistry::new();
    let payload: [u8; 3] = [0x00, 0xFF, 0x42];
    let key = "api-key";
    let bytes = secret_with_single_data_entry(key, &payload);

    let decoded = registry
        .decode_message("Secret", &bytes)
        .expect("Secret schema must be registered");

    let data = decoded
        .get("data")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("decoded Secret must have a `data` object; got {decoded}"));

    let expected_b64 = base64::engine::general_purpose::STANDARD.encode(payload);
    assert_eq!(
        expected_b64, "AP9C",
        "sanity-check the base64 of the upstream sample bytes",
    );

    let got = data
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("data must contain key {key:?}; got {decoded}"));
    assert_eq!(
        got, expected_b64,
        "Secret.data[{key}] must surface as standard-padded base64; got {decoded}",
    );
}

/// Empty `bytes` value (`b""`) is the boundary case for the BytesMap
/// decoder — the value field is length-delimited with `len=0`, and the
/// base64 of `[]` is `""`. Pinning this catches a regression where the
/// decoder might short-circuit a zero-length value and drop the key, or
/// emit `null` instead of an empty string.
#[test]
fn test_secret_data_proto_decode_empty_value_yields_empty_base64() {
    let registry = ProtoRegistry::new();
    let bytes = secret_with_single_data_entry("blank", b"");

    let decoded = registry
        .decode_message("Secret", &bytes)
        .expect("Secret schema must be registered");

    let data = decoded
        .get("data")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("decoded Secret must have a `data` object; got {decoded}"));
    assert_eq!(
        data.get("blank").and_then(|v| v.as_str()),
        Some(""),
        "empty bytes value must surface as empty-string base64; got {decoded}",
    );
}

/// Two `data` entries must both survive decode. `map<string,bytes>` is
/// proto-encoded as a *repeated* `MapEntry` field, so the decoder must
/// accumulate every entry under the same `data` key rather than overwriting.
#[test]
fn test_secret_data_proto_decode_multiple_entries_accumulate() {
    let registry = ProtoRegistry::new();

    let mut bytes = Vec::new();
    for (k, v) in [
        ("username", b"admin".as_slice()),
        ("password", b"hunter2".as_slice()),
    ] {
        let entry = map_entry_string_bytes(k, v);
        bytes.push(tag(2, WIRE_LENGTH_DELIMITED));
        bytes.push(entry.len() as u8);
        bytes.extend_from_slice(&entry);
    }

    let decoded = registry
        .decode_message("Secret", &bytes)
        .expect("Secret schema must be registered");
    let data = decoded
        .get("data")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("decoded Secret must have a `data` object; got {decoded}"));

    let std = base64::engine::general_purpose::STANDARD;
    assert_eq!(
        data.get("username").and_then(|v| v.as_str()),
        Some(std.encode(b"admin").as_str()),
        "first map entry must be preserved; got {decoded}",
    );
    assert_eq!(
        data.get("password").and_then(|v| v.as_str()),
        Some(std.encode(b"hunter2").as_str()),
        "second map entry must be preserved; got {decoded}",
    );
    assert_eq!(data.len(), 2, "exactly two entries decoded; got {decoded}");
}

/// End-to-end: the decoded JSON must round-trip through the typed
/// `rusternetes_common::resources::Secret` deserializer. That's the path
/// taken by the proto→JSON middleware in `handlers::secret::create`, so a
/// failure here would surface as a `missing field` / `invalid type` panic
/// in the real api-server, exactly like the conformance bug that
/// `conformance_configmap_envfrom_prefixes_test.rs` was written to pin.
#[test]
fn test_secret_data_proto_decode_round_trips_through_typed_decoder() {
    use rusternetes_common::resources::Secret;

    let registry = ProtoRegistry::new();
    let payload: [u8; 3] = [0x00, 0xFF, 0x42];
    let bytes = secret_with_single_data_entry("api-key", &payload);

    let decoded = registry
        .decode_message("Secret", &bytes)
        .expect("Secret schema must be registered");

    // Inject the metadata client-go would have written — the proto-only
    // body has no metadata, but the typed `Secret` decoder needs `name`
    // to deserialize. This mirrors how the real middleware augments the
    // decoded JSON with information from the request URL before handing
    // it to the typed handler.
    let mut value = decoded;
    if let serde_json::Value::Object(ref mut obj) = value {
        obj.insert(
            "metadata".to_string(),
            serde_json::json!({"name": "binary", "namespace": "default"}),
        );
    }

    let json_bytes = serde_json::to_vec(&value).unwrap();
    let secret: Secret = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!("Secret with binary data must round-trip through typed decoder; proto JSON: {value}; serde error: {e}")
    });

    let data = secret.data.as_ref().expect("data must be set after decode");
    assert_eq!(
        data.get("api-key").map(Vec::as_slice),
        Some(payload.as_slice()),
        "binary bytes must survive proto -> JSON -> typed decode",
    );
}
