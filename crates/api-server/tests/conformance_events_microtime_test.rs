//! Regression tests for upstream conformance
//! `[sig-instrumentation] Events API should delete a collection of events
//! [Conformance]` (`k8s.io/kubernetes/test/e2e/instrumentation/events.go:217`).
//!
//! The conformance canary
//! (https://github.com/indyjonesnl/rusternetes/actions/runs/26315095760/job/77472467511)
//! reported:
//!
//!     422 UnexpectedServerResponse: 'Failed to deserialize the JSON body
//!     into the target type: eventTime: invalid type: map, expected a
//!     string at line 1 column 88'
//!
//! Two bugs combine to produce that surface:
//!
//! 1. The `ProtoRegistry` only registered the `core/v1.Event` schema and
//!    dispatched lookups by bare `kind`. `events.k8s.io/v1.Event` has a
//!    completely different proto field numbering (`eventTime` is field 2,
//!    `action` is field 6, …) so feeding events.k8s.io wire bytes through
//!    the core/v1 schema produced garbage JSON whose `eventTime` carried
//!    binary-noise interpreted as a `MicroTime` message.
//!
//! 2. Even on the right schema, `MicroTime` was decoded by the generic
//!    message decoder, which emits `{seconds, nanos}` objects. K8s clients
//!    serialize `metav1.MicroTime` as an RFC3339 microsecond string in
//!    JSON, and our typed `Event` deserializer (`event_time:
//!    Option<DateTime<Utc>>`) uses a string-only custom deserializer, so
//!    the object form fails with `invalid type: map, expected a string`.
//!
//! Both fixes live in `crates/api-server/src/protobuf.rs`: a new
//! group-qualified schema key `events.k8s.io/v1.Event`, dispatch via
//! `<apiVersion>.<kind>` with a bare-kind fallback, and a `MicroTime`
//! special-case in `decode_field_value` that calls `decode_micro_timestamp`.

use rusternetes_api_server::protobuf::ProtoRegistry;

/// Wire bytes for `MicroTime { seconds: <s>, nanos: <n> }`. Both fields are
/// proto-encoded as varints under tags 0x08 (field 1) and 0x10 (field 2).
fn micro_time_bytes(seconds: u64, nanos: u32) -> Vec<u8> {
    fn write_varint(out: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            out.push(((v & 0x7f) as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    let mut bytes = Vec::new();
    bytes.push(0x08); // field 1, varint
    write_varint(&mut bytes, seconds);
    bytes.push(0x10); // field 2, varint
    write_varint(&mut bytes, nanos as u64);
    bytes
}

/// `coordination.k8s.io/v1.LeaseSpec.acquireTime` is a `MicroTime`, same
/// proto type as `events.k8s.io/v1.Event.eventTime`. A parent message
/// carrying a `MicroTime` must inline an RFC3339 microsecond string at the
/// top level — not a `{seconds, nanos}` object — or downstream typed
/// deserializers reject it.
#[test]
fn test_lease_spec_acquire_time_decodes_to_microsecond_rfc3339_string() {
    let registry = ProtoRegistry::new();
    // LeaseSpec.acquireTime is proto field 3 (length-delimited message);
    // see k8s.io/api/coordination/v1/generated.proto.
    let mt = micro_time_bytes(1_779_491_200, 123_456_000);
    let mut spec = Vec::new();
    spec.push((3 << 3) | 2);
    spec.push(mt.len() as u8);
    spec.extend_from_slice(&mt);

    let decoded = registry
        .decode_message("LeaseSpec", &spec)
        .expect("LeaseSpec schema must be registered");

    let at = decoded
        .get("acquireTime")
        .unwrap_or_else(|| panic!("acquireTime missing in {decoded}"));
    let s = at
        .as_str()
        .unwrap_or_else(|| panic!("acquireTime must be a JSON string, got: {at}"));
    assert_eq!(
        s, "2026-05-22T23:06:40.123456Z",
        "acquireTime must serialize as RFC3339 with microsecond precision",
    );
}

/// `core/v1.Event.firstTimestamp` is a `Time` (second precision). Already
/// handled correctly pre-fix, but guarded here so the MicroTime patch
/// doesn't accidentally route Time fields through the microsecond
/// formatter.
#[test]
fn test_core_event_first_timestamp_decodes_to_second_precision_rfc3339() {
    let registry = ProtoRegistry::new();
    // core/v1.Event.firstTimestamp is proto field 6 (Time, length-delimited).
    let t = micro_time_bytes(1_779_491_200, 0);
    let mut event = Vec::new();
    event.push((6 << 3) | 2);
    event.push(t.len() as u8);
    event.extend_from_slice(&t);

    let decoded = registry
        .decode_message("Event", &event)
        .expect("Event schema must be registered");

    let ft = decoded
        .get("firstTimestamp")
        .unwrap_or_else(|| panic!("firstTimestamp missing in {decoded}"));
    let s = ft
        .as_str()
        .unwrap_or_else(|| panic!("firstTimestamp must be a JSON string, got: {ft}"));
    assert_eq!(
        s, "2026-05-22T23:06:40Z",
        "firstTimestamp must serialize as RFC3339 with SECOND precision",
    );
}

/// Full hydrophone failure surface: build an envelope-wrapped
/// `events.k8s.io/v1.Event` proto exactly as client-go negotiates over
/// the wire, run it through `decode_k8s_resource`, and check that the
/// resulting JSON has the right fields in the right places.
///
/// Pre-fix the schema dispatch picked `core/v1.Event` (because lookup was
/// kind-only), `eventTime` came back as `{seconds, nanos}`, and the typed
/// `Event` deserializer rejected it with `invalid type: map, expected a
/// string`. Post-fix the group-qualified key `events.k8s.io/v1.Event` is
/// preferred and `MicroTime` decodes to a microsecond RFC3339 string.
#[test]
fn test_events_k8s_io_v1_event_decodes_via_group_qualified_schema() {
    let registry = ProtoRegistry::new();

    // events.k8s.io/v1.Event inner proto:
    //   field 2 = eventTime (MicroTime)
    //   field 4 = reportingController (string)
    //   field 6 = action (string)
    //   field 7 = reason (string)
    //   field 10 = note (string)
    //   field 11 = type (string)
    let mt = micro_time_bytes(1_505_828_956, 0);
    let mut inner = Vec::new();
    inner.push((2 << 3) | 2);
    inner.push(mt.len() as u8);
    inner.extend_from_slice(&mt);
    inner.push((4 << 3) | 2);
    inner.push(b"test-controller".len() as u8);
    inner.extend_from_slice(b"test-controller");
    inner.push((6 << 3) | 2);
    inner.push(b"Do".len() as u8);
    inner.extend_from_slice(b"Do");
    inner.push((7 << 3) | 2);
    inner.push(b"Test".len() as u8);
    inner.extend_from_slice(b"Test");
    inner.push((10 << 3) | 2);
    inner.push(b"This is test-event-1".len() as u8);
    inner.extend_from_slice(b"This is test-event-1");
    inner.push((11 << 3) | 2);
    inner.push(b"Normal".len() as u8);
    inner.extend_from_slice(b"Normal");

    // Wrap in the `k8s\0` envelope with TypeMeta carrying apiVersion + kind.
    let mut envelope = Vec::new();
    envelope.extend_from_slice(b"k8s\0");
    let mut typemeta = Vec::new();
    typemeta.push((1 << 3) | 2);
    typemeta.push(b"events.k8s.io/v1".len() as u8);
    typemeta.extend_from_slice(b"events.k8s.io/v1");
    typemeta.push((2 << 3) | 2);
    typemeta.push(b"Event".len() as u8);
    typemeta.extend_from_slice(b"Event");
    envelope.push((1 << 3) | 2);
    envelope.push(typemeta.len() as u8);
    envelope.extend_from_slice(&typemeta);
    envelope.push((2 << 3) | 2);
    envelope.push(inner.len() as u8);
    envelope.extend_from_slice(&inner);

    let json_bytes = registry
        .decode_k8s_resource(&envelope)
        .expect("decode_k8s_resource must accept the envelope");
    let decoded: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();

    assert_eq!(decoded["apiVersion"], "events.k8s.io/v1");
    assert_eq!(decoded["kind"], "Event");
    assert_eq!(
        decoded["eventTime"], "2017-09-19T13:49:16.000000Z",
        "eventTime must decode as RFC3339 microsecond string",
    );
    assert_eq!(decoded["reportingController"], "test-controller");
    assert_eq!(decoded["action"], "Do");
    assert_eq!(decoded["reason"], "Test");
    assert_eq!(decoded["note"], "This is test-event-1");
    assert_eq!(decoded["type"], "Normal");
    // Crucially: NO core/v1 fields should appear. Pre-fix this body had
    // `involvedObject.kind: 1505828956`, `firstTimestamp: <bogus Time>`,
    // and `count: 10` (from misinterpreting events.k8s.io action / reason
    // / etc bytes via the core/v1 schema). The group-qualified key keeps
    // those out.
    for field in [
        "involvedObject",
        "firstTimestamp",
        "lastTimestamp",
        "count",
        "source",
        "message",
    ] {
        assert!(
            decoded.get(field).is_none() || decoded[field] == serde_json::Value::Null,
            "core/v1.Event-only field {field:?} must not appear in events.k8s.io/v1 output; got {decoded}",
        );
    }
}

/// Backward compatibility: a `core/v1.Event` envelope must still decode
/// through the existing bare-`Event` schema. The new group-qualified
/// lookup falls back to the bare kind for every type that doesn't have a
/// per-group entry, so this path stays green.
#[test]
fn test_core_v1_event_still_decodes_via_bare_event_schema() {
    let registry = ProtoRegistry::new();
    // core/v1.Event.firstTimestamp = field 6 (Time)
    // core/v1.Event.reason = field 3
    let t = micro_time_bytes(1_779_491_200, 0);
    let mut inner = Vec::new();
    inner.push((6 << 3) | 2);
    inner.push(t.len() as u8);
    inner.extend_from_slice(&t);
    inner.push((3 << 3) | 2);
    inner.push(b"CoreV1Event".len() as u8);
    inner.extend_from_slice(b"CoreV1Event");

    let mut envelope = Vec::new();
    envelope.extend_from_slice(b"k8s\0");
    let mut typemeta = Vec::new();
    typemeta.push((1 << 3) | 2);
    typemeta.push(b"v1".len() as u8);
    typemeta.extend_from_slice(b"v1");
    typemeta.push((2 << 3) | 2);
    typemeta.push(b"Event".len() as u8);
    typemeta.extend_from_slice(b"Event");
    envelope.push((1 << 3) | 2);
    envelope.push(typemeta.len() as u8);
    envelope.extend_from_slice(&typemeta);
    envelope.push((2 << 3) | 2);
    envelope.push(inner.len() as u8);
    envelope.extend_from_slice(&inner);

    let json_bytes = registry.decode_k8s_resource(&envelope).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Event");
    assert_eq!(decoded["firstTimestamp"], "2026-05-22T23:06:40Z");
    assert_eq!(decoded["reason"], "CoreV1Event");
}

/// Pins the wire-layout of `events.k8s.io/v1.Event` fields 12–15 to the
/// upstream `k8s.io/api/events/v1/generated.proto` (release-1.35).
///
/// Pre-fix the schema had:
///   12 = deprecatedFirstTimestamp, 13 = deprecatedLastTimestamp,
///   14 = deprecatedCount, 15 = deprecatedSource
/// — shifted by one slot from upstream:
///   12 = deprecatedSource, 13 = deprecatedFirstTimestamp,
///   14 = deprecatedLastTimestamp, 15 = deprecatedCount
///
/// A client-go conformance event always sends field 15 with
/// `WIRE_VARINT 0` (proto2 presence-tracked `DeprecatedCount` defaults to
/// zero but is still serialized). Our `decode_with_schema` varint branch
/// blindly inserts the value as a number regardless of the declared
/// FieldType, so the off-by-one made the response include
/// `"deprecatedSource": 0`. The conformance binary then failed with
/// `json: cannot unmarshal number into Go struct field
/// Event.deprecatedSource of type v1.EventSource`
/// ([sig-instrumentation] Events API canary 2026-05-27).
///
/// This test sends a body that mirrors the upstream wire layout for those
/// four deprecated fields and asserts each lands in the right JSON shape.
#[test]
fn test_events_k8s_io_v1_event_deprecated_fields_match_upstream_wire_layout() {
    let registry = ProtoRegistry::new();

    // Build an inner Event proto body that uses field numbers 12-15 in
    // the upstream order.
    let mut inner = Vec::new();

    // field 12 = deprecatedSource (Message EventSource{component:"src"})
    let mut ds = Vec::new();
    ds.push((1 << 3) | 2); // EventSource.component = field 1, length-delimited
    ds.push(b"src".len() as u8);
    ds.extend_from_slice(b"src");
    inner.push((12 << 3) | 2); // wire-type 2 = length-delimited (message)
    inner.push(ds.len() as u8);
    inner.extend_from_slice(&ds);

    // field 13 = deprecatedFirstTimestamp (Time)
    let t = micro_time_bytes(1_505_828_956, 0);
    inner.push((13 << 3) | 2);
    inner.push(t.len() as u8);
    inner.extend_from_slice(&t);

    // field 14 = deprecatedLastTimestamp (Time)
    inner.push((14 << 3) | 2);
    inner.push(t.len() as u8);
    inner.extend_from_slice(&t);

    // field 15 = deprecatedCount (int32 varint = 7)
    // wire-type 0 = varint; tag = (field_num << 3) | wire_type
    inner.push(15 << 3);
    inner.push(7);

    // Wrap in the `k8s\0` envelope with TypeMeta = events.k8s.io/v1 Event.
    let mut envelope = Vec::new();
    envelope.extend_from_slice(b"k8s\0");
    let mut typemeta = Vec::new();
    typemeta.push((1 << 3) | 2);
    typemeta.push(b"events.k8s.io/v1".len() as u8);
    typemeta.extend_from_slice(b"events.k8s.io/v1");
    typemeta.push((2 << 3) | 2);
    typemeta.push(b"Event".len() as u8);
    typemeta.extend_from_slice(b"Event");
    envelope.push((1 << 3) | 2);
    envelope.push(typemeta.len() as u8);
    envelope.extend_from_slice(&typemeta);
    envelope.push((2 << 3) | 2);
    envelope.push(inner.len() as u8);
    envelope.extend_from_slice(&inner);

    let json_bytes = registry
        .decode_k8s_resource(&envelope)
        .expect("decode_k8s_resource must accept the envelope");
    let decoded: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();

    // deprecatedSource MUST be an object — pre-fix this was a number, which
    // is exactly what made client-go's typed decoder reject the response.
    assert!(
        decoded["deprecatedSource"].is_object(),
        "deprecatedSource must decode as a JSON object (EventSource); got {}",
        decoded["deprecatedSource"]
    );
    assert_eq!(
        decoded["deprecatedSource"]["component"], "src",
        "EventSource.component must round-trip; got {}",
        decoded["deprecatedSource"]
    );

    // The Time fields must come back as RFC3339 strings (second precision).
    assert_eq!(
        decoded["deprecatedFirstTimestamp"], "2017-09-19T13:49:16Z",
        "deprecatedFirstTimestamp must decode as RFC3339 string"
    );
    assert_eq!(
        decoded["deprecatedLastTimestamp"], "2017-09-19T13:49:16Z",
        "deprecatedLastTimestamp must decode as RFC3339 string"
    );

    // deprecatedCount is the only varint among the four — must be the int 7.
    assert_eq!(
        decoded["deprecatedCount"], 7,
        "deprecatedCount must decode as the integer payload"
    );
}
