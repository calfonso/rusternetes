//! Regression tests for protobuf wire-format decoding of
//! `runtime.RawExtension` and every field that the rusternetes
//! [`ProtoRegistry`] models with [`FieldType::JsonRaw`].
//!
//! Upstream reference:
//! * `staging/src/k8s.io/apimachinery/pkg/runtime/types.go`
//!   defines `RawExtension { optional bytes raw = 1; }` (see
//!   `crates/api-server/proto/upstream/v1.35/k8s.io/apimachinery/pkg/runtime/generated.proto`).
//! * `staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/types_jsonschema.go`
//!   declares `default`, `enum[]` and `example` on `JSONSchemaProps` as the
//!   `JSON` message — which on the wire has the same single
//!   `optional bytes raw = 1;` shape as `RawExtension`.
//!
//! On the wire a parent message that embeds a `RawExtension`/`JSON` field
//! sees:
//!
//! ```text
//!   tag(parent_field) | length | tag(1) | length | <raw JSON bytes>
//! ```
//!
//! Our protobuf→JSON middleware (`ProtoRegistry::decode_message`) MUST
//! parse the inner `raw` bytes as JSON and inline that value at the parent
//! field. The wrong behaviour — also the default for `FieldType::Bytes` —
//! would be to emit `{"<field>": {"raw": "<base64>"}}` or
//! `{"<field>": "<base64>"}`, which would break every CRD `default` /
//! `enum` value and every webhook `AdmissionReview.request.object` round
//! trip.
//!
//! These tests pin that behaviour for the three documented
//! [`FieldType::JsonRaw`] consumers:
//! * `JSONSchemaProps.default`         — singular `JsonRaw`.
//! * `JSONSchemaProps.enum[*]`         — repeated `JsonRaw`.
//! * `ControllerRevision.data`         — RawExtension-shaped payload
//!   (the only RawExtension field registered today; the upstream
//!   `AdmissionReview.request.object` carries the identical wire shape).

use rusternetes_api_server::protobuf::ProtoRegistry;
use serde_json::{json, Value};

/// Encode a varint per the protobuf spec. Used for tag and length fields.
fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Tag byte for a length-delimited field at `field_number`.
const fn ld_tag(field_number: u32) -> u32 {
    (field_number << 3) | 2
}

/// Wrap `payload` as a length-delimited field at the given field number.
/// Produces `tag | varint(len) | payload`.
fn ld_field(field_number: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 10);
    write_varint(&mut out, ld_tag(field_number) as u64);
    write_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(payload);
    out
}

/// Build the wire bytes for a `RawExtension`-shaped message whose `raw`
/// bytes (field 1) are the JSON encoding of `value`. Same shape as
/// `runtime.RawExtension`, `apiextensions/v1.JSON`, and the wrapper used
/// by every [`FieldType::JsonRaw`] consumer.
fn raw_extension_bytes(value: &Value) -> Vec<u8> {
    let payload = serde_json::to_vec(value).expect("payload must serialize");
    ld_field(1, &payload)
}

/// The canonical sample payload — exercises a string scalar, a nested
/// array, and integer elements. Anything more exotic would only pad the
/// tests; this matches the shape used by upstream CRD samples and
/// AdmissionReview test fixtures.
fn sample_json() -> Value {
    json!({"foo": "bar", "nested": [1, 2, 3]})
}

// -------- RawExtension (bytes round-trip) --------------------------------

/// Direct wire-format check for the `RawExtension` shape:
/// `JSONSchemaProps.default` is a `FieldType::JsonRaw` field whose
/// payload IS a `RawExtension`-shaped message. Decoding it must inline
/// the inner JSON at the `default` key, not base64-encode the bytes.
#[test]
fn raw_extension_inlines_inner_json_for_jsonschemaprops_default() {
    let registry = ProtoRegistry::new();

    let inner = sample_json();
    let raw_ext = raw_extension_bytes(&inner);
    // JSONSchemaProps.default is field 8.
    let mut wire = Vec::new();
    write_varint(&mut wire, ld_tag(8) as u64);
    write_varint(&mut wire, raw_ext.len() as u64);
    wire.extend_from_slice(&raw_ext);

    let decoded = registry
        .decode_message("JSONSchemaProps", &wire)
        .expect("JSONSchemaProps schema must be registered");

    let default = decoded
        .get("default")
        .unwrap_or_else(|| panic!("`default` must be present; got {decoded}"));
    assert_eq!(
        default, &inner,
        "JsonRaw must inline JSON at the parent field, not wrap as bytes; got {decoded}"
    );
    // Guard against the historic base64 / RawExtension wrapper bug:
    assert!(
        default.get("raw").is_none(),
        "decoded value must NOT expose a `raw` field — that's the wire-level \
         RawExtension wrapper which must be unwrapped; got {decoded}",
    );
    assert!(
        !default.is_string(),
        "decoded value must NOT be a base64 string — it must be inlined JSON; got {decoded}",
    );
}

// -------- JSONSchemaProps.enum[*] (repeated JsonRaw) --------------------

/// Repeated `JsonRaw` round-trip. `enum` on `JSONSchemaProps` is the
/// only repeated `JsonRaw` field in the registry; each element is its
/// own `RawExtension`-shaped message and must be inlined back to the
/// original JSON value.
#[test]
fn raw_extension_inlines_each_enum_entry_on_jsonschemaprops() {
    let registry = ProtoRegistry::new();

    let entries = vec![
        json!("RED"),
        json!(42),
        json!({"foo": "bar", "nested": [1, 2, 3]}),
    ];

    let mut wire = Vec::new();
    for entry in &entries {
        // Each enum entry is a separate length-delimited field at #20.
        let raw_ext = raw_extension_bytes(entry);
        write_varint(&mut wire, ld_tag(20) as u64);
        write_varint(&mut wire, raw_ext.len() as u64);
        wire.extend_from_slice(&raw_ext);
    }

    let decoded = registry
        .decode_message("JSONSchemaProps", &wire)
        .expect("JSONSchemaProps schema must be registered");

    let arr = decoded
        .get("enum")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("`enum` must decode to a JSON array; got {decoded}"));

    assert_eq!(arr.len(), entries.len(), "every enum entry must survive");
    for (got, want) in arr.iter().zip(entries.iter()) {
        assert_eq!(
            got, want,
            "each enum entry must be inlined JSON, not a RawExtension wrapper; got {decoded}",
        );
    }
}

// -------- ControllerRevision.data (RawExtension nesting) ----------------

/// `ControllerRevision.data` is the only registered field that upstream
/// declares as `runtime.RawExtension` directly (DaemonSet / StatefulSet
/// rollout snapshots). The wire encoding is identical to
/// `AdmissionReview.request.object.raw` — `RawExtension { bytes raw = 1; }`
/// nested under a parent field — so pinning the inline behaviour here
/// also pins it for webhook payloads.
#[test]
fn raw_extension_inlines_inner_json_for_controllerrevision_data() {
    let registry = ProtoRegistry::new();

    let inner = sample_json();
    // ControllerRevision: metadata(1) message, data(2) JsonRaw, revision(3) int.
    let raw_ext = raw_extension_bytes(&inner);
    let mut wire = Vec::new();
    write_varint(&mut wire, ld_tag(2) as u64);
    write_varint(&mut wire, raw_ext.len() as u64);
    wire.extend_from_slice(&raw_ext);
    // revision = 7 (varint @ field 3)
    write_varint(&mut wire, (3 << 3) as u64);
    write_varint(&mut wire, 7);

    let decoded = registry
        .decode_message("ControllerRevision", &wire)
        .expect("ControllerRevision schema must be registered");

    let data = decoded
        .get("data")
        .unwrap_or_else(|| panic!("`data` must be present; got {decoded}"));
    assert_eq!(
        data, &inner,
        "RawExtension payload must be inlined as JSON, not base64; got {decoded}",
    );
    assert!(
        data.get("raw").is_none(),
        "decoded `data` must NOT expose the wire-level `raw` wrapper; got {decoded}",
    );
    assert_eq!(
        decoded.get("revision").and_then(Value::as_i64),
        Some(7),
        "scalar sibling fields must still decode normally; got {decoded}",
    );
}

// -------- Deep nesting through CustomResourceValidation ------------------

/// End-to-end nesting check: `CustomResourceDefinition.spec.versions[*]
/// .schema.openAPIV3Schema.default` reaches `JsonRaw` four levels deep.
/// Decode just the leaf wrapper (`CustomResourceValidation`) and confirm
/// the inline behaviour survives one level of nested-message dispatch.
#[test]
fn raw_extension_inlines_through_nested_jsonschemaprops() {
    let registry = ProtoRegistry::new();

    let inner = sample_json();
    let raw_ext = raw_extension_bytes(&inner);
    // JSONSchemaProps.default at field 8.
    let mut props = Vec::new();
    write_varint(&mut props, ld_tag(8) as u64);
    write_varint(&mut props, raw_ext.len() as u64);
    props.extend_from_slice(&raw_ext);

    // CustomResourceValidation.openAPIV3Schema = field 1 (Message).
    let mut wire = Vec::new();
    write_varint(&mut wire, ld_tag(1) as u64);
    write_varint(&mut wire, props.len() as u64);
    wire.extend_from_slice(&props);

    let decoded = registry
        .decode_message("CustomResourceValidation", &wire)
        .expect("CustomResourceValidation schema must be registered");

    let default = decoded
        .pointer("/openAPIV3Schema/default")
        .unwrap_or_else(|| panic!("nested default must decode; got {decoded}"));
    assert_eq!(
        default, &inner,
        "nested JsonRaw must inline through Message dispatch too; got {decoded}",
    );
}

// -------- Empty / non-JSON payload sanity check --------------------------

/// Defensive: the decoder must not panic when the inner `raw` bytes are
/// not valid JSON — it must surface the raw text as a string. We do not
/// rely on this in any production path, but pinning it here documents
/// the contract for callers that hit malformed wire data.
#[test]
fn raw_extension_falls_back_to_string_on_non_json_bytes() {
    let registry = ProtoRegistry::new();

    // RawExtension { raw: "not-valid-json" }
    let payload = b"not-valid-json";
    let raw_ext = ld_field(1, payload);
    let mut wire = Vec::new();
    write_varint(&mut wire, ld_tag(8) as u64);
    write_varint(&mut wire, raw_ext.len() as u64);
    wire.extend_from_slice(&raw_ext);

    let decoded = registry
        .decode_message("JSONSchemaProps", &wire)
        .expect("JSONSchemaProps schema must be registered");

    let default = decoded
        .get("default")
        .unwrap_or_else(|| panic!("`default` must be present; got {decoded}"));
    assert_eq!(
        default,
        &Value::String("not-valid-json".into()),
        "non-JSON payload must surface as a string, not panic and not base64; got {decoded}",
    );
}
