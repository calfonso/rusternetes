//! Watch event protobuf envelope wire shape — parity with upstream
//! `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go` (`WatchEvent`)
//! and `staging/src/k8s.io/apimachinery/pkg/watch/watch.go` (streaming
//! convention).
//!
//! Companion to `watch_event_envelope_test.rs`, which pins the JSON-side
//! contract. This file pins the same `{type, object}` envelope when the
//! wire encoding is protobuf, as negotiated by
//! `Accept: application/vnd.kubernetes.protobuf` on a `watch=true` endpoint.
//!
//! ## Envelope contract (proto)
//!
//! Upstream `generated.proto` (apimachinery/pkg/apis/meta/v1):
//!
//! ```proto
//! message WatchEvent {
//!   optional string type   = 1;
//!   optional .k8s.io.apimachinery.pkg.runtime.RawExtension object = 2;
//! }
//! message RawExtension {
//!   optional bytes raw = 1;
//! }
//! ```
//!
//! `RawExtension.raw` carries the full payload of the watched object as
//! bytes — for rusternetes that means the `k8s\x00` magic prefix followed by
//! a protobuf-encoded `runtime.Unknown` whose `raw` field holds the
//! resource's JSON body (see `rusternetes_common::protobuf::encode_protobuf`).
//!
//! ## Streaming convention
//!
//! Per `watch.go`, each `WatchEvent` on the wire is **length-delimited**:
//! the framer writes `varint(eventLen) || eventBytes` so a streaming client
//! can decode events one at a time without a record terminator. This is the
//! same convention `prost::Message::encode_length_delimited` implements.
//!
//! ## What this file pins
//!
//! - **ADDED / MODIFIED / DELETED / BOOKMARK / ERROR**: each event type
//!   round-trips through the WatchEvent / RawExtension / Unknown chain with
//!   the exact field tags and wire types upstream uses, and the inner
//!   payload (`object.raw`) decodes back to the original resource JSON via
//!   `decode_protobuf`.
//! - **Tag/wire constants**: an explicit byte-level assertion that the
//!   first bytes of a WatchEvent encoding are
//!   `[0x0a, len, 'A','D','D','E','D', 0x12, ...]` — tag 1/wire-type 2
//!   (`type` string) followed by tag 2/wire-type 2 (`object` length-
//!   delimited). Any change to field numbers or wire types is caught here.
//! - **Multi-event stream**: three concatenated length-delimited
//!   WatchEvents decode independently in order, mirroring how a
//!   `watch=true` HTTP body is consumed by `kubectl`/client-go.

use prost::Message;
use rusternetes_common::{
    protobuf::{decode_protobuf, encode_protobuf, is_protobuf, TypeMeta, PROTOBUF_MAGIC},
    resources::pod::Pod,
    types::{ObjectMeta, Status, TypeMeta as ResourceTypeMeta},
};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Local proto types — mirrors `apimachinery/pkg/apis/meta/v1/generated.proto`.
// These deliberately live in the test, not the production tree: any drift
// between this schema and upstream's WatchEvent will show up here as a
// failed assertion against `expected_envelope_prefix`.
// ---------------------------------------------------------------------------

/// `runtime.RawExtension` — single `raw bytes` field at tag 1.
#[derive(Clone, PartialEq, Message)]
struct RawExtension {
    #[prost(bytes, tag = "1")]
    raw: Vec<u8>,
}

/// `meta/v1.WatchEvent` — string `type` at tag 1, `RawExtension object` at
/// tag 2. Both `optional` in proto2; in prost we represent that via the
/// default-empty semantics of `string` / nested message presence.
#[derive(Clone, PartialEq, Message)]
struct WatchEventProto {
    #[prost(string, tag = "1")]
    r#type: String,
    #[prost(message, optional, tag = "2")]
    object: Option<RawExtension>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const TEST_NS: &str = "watchprotons";

/// Build a minimal Pod we can compare against after round-tripping. We use
/// a plain `serde_json::Value` because `Pod` does not implement `PartialEq`,
/// and JSON is the wire format inside the Unknown.raw payload.
fn pod_value(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
            "uid": format!("uid-{name}"),
            "resourceVersion": "42",
        },
        "spec": {
            "containers": [{"name": "c", "image": "busybox"}]
        }
    })
}

/// Build a metav1.Status object — the `object` payload for ERROR events.
fn status_for_error() -> Status {
    Status {
        kind: "Status".to_string(),
        api_version: "v1".to_string(),
        metadata: None,
        status: Some("Failure".to_string()),
        message: Some("watch channel terminated".to_string()),
        reason: Some("Expired".to_string()),
        details: None,
        code: Some(410),
    }
}

/// Wrap a JSON-serializable object as a WatchEvent.object payload — i.e.
/// the bytes that go inside `RawExtension.raw`. Returns the `k8s\x00`-
/// prefixed Unknown blob produced by `encode_protobuf`.
fn unknown_payload<T: serde::Serialize>(obj: &T, kind: &str) -> Vec<u8> {
    encode_protobuf(obj, "v1", kind).expect("encode_protobuf must succeed")
}

/// Assemble a complete `WatchEvent` proto message: `type` + `RawExtension`
/// wrapping the magic-prefixed Unknown blob.
fn watch_event(event_type: &str, raw: Vec<u8>) -> WatchEventProto {
    WatchEventProto {
        r#type: event_type.to_string(),
        object: Some(RawExtension { raw }),
    }
}

/// Decode a `WatchEvent` and pull out `(type, RawExtension.raw)`.
/// Panics with a descriptive message if the envelope is malformed.
fn assert_envelope(bytes: &[u8], expected_type: &str) -> Vec<u8> {
    let decoded = WatchEventProto::decode(bytes)
        .unwrap_or_else(|e| panic!("WatchEvent must decode (type={expected_type}): {e}"));
    assert_eq!(
        decoded.r#type, expected_type,
        "WatchEvent.type mismatch: got {:?}, want {expected_type}",
        decoded.r#type
    );
    let raw_ext = decoded
        .object
        .unwrap_or_else(|| panic!("WatchEvent.object must be set on {expected_type}"));
    assert!(
        !raw_ext.raw.is_empty(),
        "WatchEvent.object.raw must be non-empty on {expected_type}"
    );
    assert!(
        is_protobuf(&raw_ext.raw),
        "WatchEvent.object.raw must start with the k8s\\x00 magic prefix \
         (got first bytes: {:?}, expected: {PROTOBUF_MAGIC:?})",
        &raw_ext.raw[..raw_ext.raw.len().min(4)]
    );
    raw_ext.raw
}

// ---------------------------------------------------------------------------
// Per event-type envelope shape
// ---------------------------------------------------------------------------

/// ADDED envelope: `type = "ADDED"`, `object` carries a freshly created Pod.
#[test]
fn watch_event_proto_added_carries_pod_payload() {
    let pod = pod_value("envelope-add");
    let raw = unknown_payload(&pod, "Pod");
    let event = watch_event("ADDED", raw);

    let mut buf = Vec::new();
    event.encode(&mut buf).expect("encode WatchEvent");

    let raw = assert_envelope(&buf, "ADDED");
    let (decoded, type_meta): (Value, TypeMeta) =
        decode_protobuf(&raw).expect("inner Unknown.raw must decode as JSON-via-Unknown");
    assert_eq!(type_meta.api_version, "v1", "Unknown.apiVersion");
    assert_eq!(type_meta.kind, "Pod", "Unknown.kind");
    assert_eq!(
        decoded.pointer("/metadata/name").and_then(|v| v.as_str()),
        Some("envelope-add"),
        "decoded payload must echo the original Pod name"
    );
}

/// MODIFIED envelope: same shape as ADDED, only `type` differs.
#[test]
fn watch_event_proto_modified_carries_updated_pod() {
    let mut pod = pod_value("envelope-mod");
    pod["spec"]["containers"][0]["image"] = json!("busybox:1.36");
    let raw = unknown_payload(&pod, "Pod");
    let event = watch_event("MODIFIED", raw);

    let mut buf = Vec::new();
    event.encode(&mut buf).expect("encode WatchEvent");

    let raw = assert_envelope(&buf, "MODIFIED");
    let (decoded, _): (Value, TypeMeta) = decode_protobuf(&raw).expect("decode_protobuf");
    assert_eq!(
        decoded
            .pointer("/spec/containers/0/image")
            .and_then(|v| v.as_str()),
        Some("busybox:1.36"),
        "MODIFIED payload must carry the post-mutation state"
    );
}

/// DELETED envelope: per `watch.Event` semantics, `object` is the resource
/// state at deletion time (prev-value). We mirror that here by encoding the
/// pre-delete Pod under the DELETED type.
#[test]
fn watch_event_proto_deleted_carries_prior_pod() {
    let pod = pod_value("envelope-del");
    let raw = unknown_payload(&pod, "Pod");
    let event = watch_event("DELETED", raw);

    let mut buf = Vec::new();
    event.encode(&mut buf).expect("encode WatchEvent");

    let raw = assert_envelope(&buf, "DELETED");
    let (decoded, type_meta): (Value, TypeMeta) = decode_protobuf(&raw).expect("decode_protobuf");
    assert_eq!(type_meta.kind, "Pod");
    assert_eq!(
        decoded
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str()),
        Some(TEST_NS),
        "DELETED payload must echo the resource state at deletion"
    );
    assert_eq!(
        decoded.pointer("/metadata/name").and_then(|v| v.as_str()),
        Some("envelope-del"),
    );
}

/// BOOKMARK envelope: `object` is a minimal resource of the watched Kind
/// carrying only kind / apiVersion / metadata.resourceVersion. We use a
/// `Pod` (`spec` omitted) to mirror what the rusternetes JSON handler emits
/// for bookmarks; protobuf framing is identical.
#[test]
fn watch_event_proto_bookmark_carries_resource_version() {
    let bookmark = Pod {
        type_meta: ResourceTypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            resource_version: Some("9001".to_string()),
            ..ObjectMeta::default()
        },
        spec: None,
        status: None,
    };
    let raw = unknown_payload(&bookmark, "Pod");
    let event = watch_event("BOOKMARK", raw);

    let mut buf = Vec::new();
    event.encode(&mut buf).expect("encode WatchEvent");

    let raw = assert_envelope(&buf, "BOOKMARK");
    let (decoded, type_meta): (Value, TypeMeta) = decode_protobuf(&raw).expect("decode_protobuf");
    assert_eq!(type_meta.kind, "Pod", "BOOKMARK.object.kind");
    assert_eq!(type_meta.api_version, "v1", "BOOKMARK.object.apiVersion");
    assert_eq!(
        decoded
            .pointer("/metadata/resourceVersion")
            .and_then(|v| v.as_str()),
        Some("9001"),
        "BOOKMARK payload must carry resourceVersion"
    );
}

/// ERROR envelope: `object` is a `metav1.Status` describing the failure.
/// Upstream recommends Status but allows other types per the proto comment;
/// we pin the recommended path here.
#[test]
fn watch_event_proto_error_carries_status() {
    let status = status_for_error();
    let raw = unknown_payload(&status, "Status");
    let event = watch_event("ERROR", raw);

    let mut buf = Vec::new();
    event.encode(&mut buf).expect("encode WatchEvent");

    let raw = assert_envelope(&buf, "ERROR");
    let (decoded, type_meta): (Value, TypeMeta) = decode_protobuf(&raw).expect("decode_protobuf");
    assert_eq!(type_meta.kind, "Status", "ERROR.object.kind must be Status");
    assert_eq!(type_meta.api_version, "v1");
    assert_eq!(
        decoded.get("code").and_then(|v| v.as_u64()),
        Some(410),
        "ERROR.object.code must round-trip"
    );
    assert_eq!(
        decoded.get("reason").and_then(|v| v.as_str()),
        Some("Expired"),
        "ERROR.object.reason must round-trip"
    );
}

// ---------------------------------------------------------------------------
// Tag / wire-type byte-level pin
// ---------------------------------------------------------------------------

/// Byte-level assertion: the first bytes of an encoded `WatchEvent` reflect
/// upstream's field numbers and wire types exactly.
///
/// `tag = (field_number << 3) | wire_type`:
///   - field 1 (type), wire type 2 (length-delimited) → 0x0a
///   - field 2 (object), wire type 2 (length-delimited) → 0x12
///
/// Any change to the upstream WatchEvent schema (re-numbering, switching
/// type to e.g. an enum) breaks this — which is the point: this test exists
/// to catch silent wire-format drift.
#[test]
fn watch_event_proto_first_bytes_match_upstream_field_numbers() {
    let event = watch_event("ADDED", vec![0xde, 0xad, 0xbe, 0xef]);
    let mut buf = Vec::new();
    event.encode(&mut buf).expect("encode WatchEvent");

    // type = "ADDED" (5 bytes) → tag 0x0a, len 0x05, "ADDED"
    assert_eq!(
        &buf[0..7],
        &[0x0a, 0x05, b'A', b'D', b'D', b'E', b'D'],
        "WatchEvent.type must be field 1, wire-type 2, value \"ADDED\" (got: {:?})",
        &buf[..buf.len().min(16)]
    );

    // object = RawExtension{raw=4 bytes} →
    //   outer tag 0x12 (field 2, wire-type 2)
    //   outer len = 6 (inner tag 0x0a + inner len 0x04 + 4 raw bytes)
    //   inner tag 0x0a (field 1, wire-type 2)
    //   inner len 0x04
    //   raw  0xde 0xad 0xbe 0xef
    assert_eq!(
        &buf[7..15],
        &[0x12, 0x06, 0x0a, 0x04, 0xde, 0xad, 0xbe, 0xef],
        "WatchEvent.object must be field 2, wire-type 2, wrapping RawExtension.raw at tag 1 \
         (got after-type bytes: {:?})",
        &buf[7..buf.len().min(16)]
    );
}

// ---------------------------------------------------------------------------
// Multi-event stream framing
// ---------------------------------------------------------------------------

/// Streaming convention: concatenate three length-delimited WatchEvents and
/// confirm each decodes independently in order. Mirrors how a
/// `watch=true` HTTP body is consumed frame-by-frame.
#[test]
fn watch_event_proto_multi_event_stream_decodes_each_independently() {
    let events = [
        ("ADDED", pod_value("stream-1")),
        ("MODIFIED", {
            let mut p = pod_value("stream-1");
            p["spec"]["containers"][0]["image"] = json!("busybox:edge");
            p
        }),
        ("DELETED", pod_value("stream-1")),
    ];

    // Frame: varint(len) || encoded WatchEvent
    let mut stream = Vec::new();
    for (kind, payload) in &events {
        let raw = unknown_payload(payload, "Pod");
        let event = watch_event(kind, raw);
        event
            .encode_length_delimited(&mut stream)
            .expect("encode_length_delimited");
    }

    // Decode each frame back out using `decode_length_delimited`, which
    // consumes the varint prefix and then exactly that many bytes. We
    // track the cursor by re-slicing the remaining tail after each decode.
    let mut cursor: &[u8] = &stream;
    let mut decoded_types = Vec::new();
    let mut decoded_names = Vec::new();
    for (expected_kind, _) in &events {
        let event = WatchEventProto::decode_length_delimited(&mut cursor)
            .unwrap_or_else(|e| panic!("frame for {expected_kind} must decode: {e}"));
        decoded_types.push(event.r#type.clone());
        let raw = event
            .object
            .as_ref()
            .unwrap_or_else(|| panic!("{expected_kind}.object must be set"))
            .raw
            .clone();
        let (payload, _): (Value, TypeMeta) =
            decode_protobuf(&raw).expect("inner Unknown.raw must decode");
        decoded_names.push(
            payload
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
                .unwrap_or_default(),
        );
    }

    assert!(
        cursor.is_empty(),
        "exactly three frames must consume the entire stream (left over: {} bytes)",
        cursor.len()
    );
    assert_eq!(
        decoded_types,
        vec!["ADDED", "MODIFIED", "DELETED"],
        "frames must decode in stream order"
    );
    assert!(
        decoded_names.iter().all(|n| n == "stream-1"),
        "every decoded payload must echo the original resource (got: {decoded_names:?})"
    );
}

/// A heterogeneous stream — ADDED Pod, BOOKMARK, ERROR Status — exercises
/// the same framing across three different `object` payload types, ensuring
/// the length-delimited convention is independent of the Kind inside.
#[test]
fn watch_event_proto_stream_handles_mixed_payload_types() {
    let bookmark = Pod {
        type_meta: ResourceTypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            resource_version: Some("100".to_string()),
            ..ObjectMeta::default()
        },
        spec: None,
        status: None,
    };

    let mut stream = Vec::new();
    watch_event("ADDED", unknown_payload(&pod_value("mix-pod"), "Pod"))
        .encode_length_delimited(&mut stream)
        .unwrap();
    watch_event("BOOKMARK", unknown_payload(&bookmark, "Pod"))
        .encode_length_delimited(&mut stream)
        .unwrap();
    watch_event("ERROR", unknown_payload(&status_for_error(), "Status"))
        .encode_length_delimited(&mut stream)
        .unwrap();

    let mut cursor: &[u8] = &stream;

    let first = WatchEventProto::decode_length_delimited(&mut cursor).expect("frame 1");
    assert_eq!(first.r#type, "ADDED");
    let (_, tm): (Value, TypeMeta) =
        decode_protobuf(&first.object.unwrap().raw).expect("frame 1 payload");
    assert_eq!(tm.kind, "Pod");

    let second = WatchEventProto::decode_length_delimited(&mut cursor).expect("frame 2");
    assert_eq!(second.r#type, "BOOKMARK");
    let (val, _): (Value, TypeMeta) =
        decode_protobuf(&second.object.unwrap().raw).expect("frame 2 payload");
    assert_eq!(
        val.pointer("/metadata/resourceVersion")
            .and_then(|v| v.as_str()),
        Some("100"),
        "bookmark frame must carry rv"
    );

    let third = WatchEventProto::decode_length_delimited(&mut cursor).expect("frame 3");
    assert_eq!(third.r#type, "ERROR");
    let (val, tm): (Value, TypeMeta) =
        decode_protobuf(&third.object.unwrap().raw).expect("frame 3 payload");
    assert_eq!(tm.kind, "Status");
    assert_eq!(val.get("code").and_then(|v| v.as_u64()), Some(410));

    assert!(
        cursor.is_empty(),
        "stream must be fully consumed after 3 frames"
    );
}
