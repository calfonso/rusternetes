//! Wire-format parity tests for "duration-shaped" optional int64-seconds
//! fields. These are NOT `metav1.Duration` (which upstream encodes as a
//! string-typed wrapper like `"30s"`); they are bare `*int64` seconds
//! pointers on the Go side, with `omitempty` JSON tags and proto varint
//! encoding. The semantic difference between *unset* (`nil`) and *zero*
//! (`*x = 0`) is load-bearing for several of them — most notoriously
//! `Toleration.tolerationSeconds`, where `nil` means "tolerate forever"
//! and `0` means "evict immediately". Pinning the wire shape here so a
//! schema edit that flips one of these to a non-Int field type, or that
//! starts emitting `0` for absent fields, is caught before it reaches the
//! kubelet eviction path.
//!
//! Fields covered (proto field numbers per upstream
//! `staging/src/k8s.io/api/core/v1/generated.proto` v1.35 and
//! `staging/src/k8s.io/api/batch/v1/generated.proto`):
//!
//!   * `PodSpec.terminationGracePeriodSeconds` (field 4)
//!   * `PodSpec.activeDeadlineSeconds`         (field 5)
//!   * `JobSpec.activeDeadlineSeconds`         (field 3)
//!   * `Toleration.tolerationSeconds`          (field 5)
//!
//! For each, we hand-craft the proto bytes, decode via the api-server's
//! `ProtoRegistry`, and assert:
//!
//!   1. set field → JSON has a bare integer (not a stringified number,
//!      not a `{seconds, nanos}` Duration object).
//!   2. unset field → JSON omits the key entirely (no `tolerationSeconds:
//!      null`, no `terminationGracePeriodSeconds: 0`).
//!
//! Then we round-trip a `Toleration` through the typed
//! `rusternetes_common::resources::Toleration` deserializer (mirroring
//! `crates/common/tests/roundtrip_core_v1.rs::assert_roundtrip`) to
//! confirm the most surprising case: `tolerationSeconds: 0` survives as
//! `Some(0)`, while an absent field surfaces as `None` — the two are
//! distinguishable end-to-end. Upstream Go relies on this distinction
//! via `*int64`; Rust mirrors it with `Option<i64>`.

use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_common::resources::Toleration;
use serde_json::Value;

/// Encode a u64 as a protobuf varint and append it to `out`.
fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Emit `(field_num, wire_type=0, value)` — proto varint encoding for
/// int32/int64/uint32/uint64 scalar fields. Negative int64 values are
/// reinterpreted as `u64` (two's complement bits) before varint encoding;
/// this matches upstream Go's `proto.Marshal` behavior and produces the
/// canonical 10-byte form.
fn write_int_field(out: &mut Vec<u8>, field_num: u32, value: i64) {
    // wire type 0 = varint; tag = (field_num << 3) | wire_type
    let tag = field_num << 3;
    write_varint(out, tag as u64);
    write_varint(out, value as u64);
}

/// Emit `(field_num, wire_type=2, len, bytes)` — length-delimited form
/// for embedded string fields (used for required scalars in the host
/// messages so the decoder has a valid path to walk).
fn write_string_field(out: &mut Vec<u8>, field_num: u32, value: &str) {
    let tag = (field_num << 3) | 2;
    write_varint(out, tag as u64);
    write_varint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

// -- PodSpec.terminationGracePeriodSeconds (field 4) -------------------------

/// `PodSpec.terminationGracePeriodSeconds` set to 30 must decode to a
/// bare JSON integer `30`, not `"30"` and not `{seconds: 30}`. The
/// upstream JSON tag is `omitempty`, but a *present* `30` is, of
/// course, not empty.
#[test]
fn test_pod_spec_termination_grace_period_seconds_set_decodes_as_bare_int() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    write_int_field(&mut bytes, 4, 30);

    let decoded = registry
        .decode_message("PodSpec", &bytes)
        .expect("PodSpec schema must be registered");

    let v = decoded
        .get("terminationGracePeriodSeconds")
        .unwrap_or_else(|| panic!("terminationGracePeriodSeconds missing in {decoded}"));
    assert_eq!(
        v,
        &Value::from(30_i64),
        "terminationGracePeriodSeconds must decode to a bare JSON integer; got {v}",
    );
}

/// `PodSpec.terminationGracePeriodSeconds = 0` is the explicit "kill
/// immediately" sentinel — upstream `omitempty` does *not* fire for
/// proto-decoded payloads since the field is present on the wire. The
/// decoder must surface `0` rather than dropping the field.
#[test]
fn test_pod_spec_termination_grace_period_seconds_zero_is_preserved() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    write_int_field(&mut bytes, 4, 0);

    let decoded = registry
        .decode_message("PodSpec", &bytes)
        .expect("PodSpec schema must be registered");

    let v = decoded
        .get("terminationGracePeriodSeconds")
        .unwrap_or_else(|| panic!("terminationGracePeriodSeconds=0 missing in {decoded}"));
    assert_eq!(
        v.as_i64(),
        Some(0),
        "terminationGracePeriodSeconds=0 must survive decode as integer 0; got {v}",
    );
}

/// Absent `terminationGracePeriodSeconds` (the field never appears on the
/// wire) must *not* synthesize a zero, null, or stringified value in the
/// JSON output. Typed downstream decoders distinguish `None` from
/// `Some(0)` — see the toleration round-trip below.
#[test]
fn test_pod_spec_termination_grace_period_seconds_absent_is_omitted() {
    let registry = ProtoRegistry::new();
    // PodSpec with only `restartPolicy` set — terminationGracePeriodSeconds
    // is intentionally not encoded.
    let mut bytes = Vec::new();
    write_string_field(&mut bytes, 3, "Always");

    let decoded = registry
        .decode_message("PodSpec", &bytes)
        .expect("PodSpec schema must be registered");

    assert!(
        decoded.get("terminationGracePeriodSeconds").is_none(),
        "absent terminationGracePeriodSeconds must NOT appear in JSON; got {decoded}",
    );
}

// -- PodSpec.activeDeadlineSeconds (field 5) ---------------------------------

/// `PodSpec.activeDeadlineSeconds` populated with a large positive value
/// (24h = 86400s) must round-trip as a bare JSON int — same wire shape
/// and JSON projection as `terminationGracePeriodSeconds`, pinned
/// separately so a schema shuffle that touches one but not the other
/// flips this test red.
#[test]
fn test_pod_spec_active_deadline_seconds_set_decodes_as_bare_int() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    write_int_field(&mut bytes, 5, 86_400);

    let decoded = registry
        .decode_message("PodSpec", &bytes)
        .expect("PodSpec schema must be registered");

    let v = decoded
        .get("activeDeadlineSeconds")
        .unwrap_or_else(|| panic!("activeDeadlineSeconds missing in {decoded}"));
    assert_eq!(
        v,
        &Value::from(86_400_i64),
        "PodSpec.activeDeadlineSeconds must decode to a bare JSON int; got {v}",
    );
}

#[test]
fn test_pod_spec_active_deadline_seconds_absent_is_omitted() {
    let registry = ProtoRegistry::new();
    // PodSpec with only `restartPolicy` set.
    let mut bytes = Vec::new();
    write_string_field(&mut bytes, 3, "OnFailure");

    let decoded = registry
        .decode_message("PodSpec", &bytes)
        .expect("PodSpec schema must be registered");

    assert!(
        decoded.get("activeDeadlineSeconds").is_none(),
        "absent activeDeadlineSeconds must NOT appear in JSON; got {decoded}",
    );
}

// -- JobSpec.activeDeadlineSeconds (field 3) ---------------------------------

/// `batch/v1.JobSpec.activeDeadlineSeconds` is field 3, distinct from the
/// PodSpec sibling. The JSON wire shape (`activeDeadlineSeconds: 3600`)
/// is identical, but the proto schemas are independent registry entries
/// so we pin Job separately.
#[test]
fn test_job_spec_active_deadline_seconds_set_decodes_as_bare_int() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    write_int_field(&mut bytes, 3, 3_600);

    let decoded = registry
        .decode_message("JobSpec", &bytes)
        .expect("JobSpec schema must be registered");

    let v = decoded
        .get("activeDeadlineSeconds")
        .unwrap_or_else(|| panic!("activeDeadlineSeconds missing in {decoded}"));
    assert_eq!(
        v,
        &Value::from(3_600_i64),
        "JobSpec.activeDeadlineSeconds must decode to a bare JSON int; got {v}",
    );
}

#[test]
fn test_job_spec_active_deadline_seconds_absent_is_omitted() {
    let registry = ProtoRegistry::new();
    // JobSpec with only `parallelism` (field 1) set.
    let mut bytes = Vec::new();
    write_int_field(&mut bytes, 1, 2);

    let decoded = registry
        .decode_message("JobSpec", &bytes)
        .expect("JobSpec schema must be registered");

    assert!(
        decoded.get("activeDeadlineSeconds").is_none(),
        "absent JobSpec.activeDeadlineSeconds must NOT appear in JSON; got {decoded}",
    );
}

// -- Toleration.tolerationSeconds (field 5) ----------------------------------
//
// The most semantically loaded of the four. Per upstream
// `staging/src/k8s.io/api/core/v1/types.go`:
//
//     // TolerationSeconds represents the period of time the toleration
//     // ... If the value is nil, this toleration is taken into account
//     // forever (do not evict). Zero and negative values will be treated
//     // as 0 (evict immediately) by the system.
//     // +optional
//     TolerationSeconds *int64 `... json:"tolerationSeconds,omitempty"`
//
// So three states must be distinguishable: nil (forever), 0 (now), and
// positive (after N seconds). The proto wire format collapses nil into
// "field absent" — and the JSON projection must mirror that exactly.

/// Positive `tolerationSeconds: 300` — typical "evict after 5 minutes"
/// pod-eviction config produced by the node-lifecycle controller.
#[test]
fn test_toleration_seconds_positive_decodes_as_bare_int() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    write_int_field(&mut bytes, 5, 300);

    let decoded = registry
        .decode_message("Toleration", &bytes)
        .expect("Toleration schema must be registered");

    let v = decoded
        .get("tolerationSeconds")
        .unwrap_or_else(|| panic!("tolerationSeconds missing in {decoded}"));
    assert_eq!(
        v,
        &Value::from(300_i64),
        "tolerationSeconds=300 must decode to a bare JSON int; got {v}",
    );
}

/// `tolerationSeconds: 0` — explicit "evict immediately". The proto
/// decoder must surface the zero rather than treating it as a default
/// fill-in, because typed clients distinguish `Some(0)` from `None`.
#[test]
fn test_toleration_seconds_zero_is_preserved() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    // Effect = "NoExecute" (field 4) — the only effect for which
    // tolerationSeconds is meaningful upstream; including it makes the
    // wire shape match a realistic payload, though strictly the decoder
    // doesn't care.
    write_string_field(&mut bytes, 4, "NoExecute");
    write_int_field(&mut bytes, 5, 0);

    let decoded = registry
        .decode_message("Toleration", &bytes)
        .expect("Toleration schema must be registered");

    let v = decoded
        .get("tolerationSeconds")
        .unwrap_or_else(|| panic!("tolerationSeconds=0 missing in {decoded}"));
    assert_eq!(
        v.as_i64(),
        Some(0),
        "tolerationSeconds=0 (evict immediately) must survive decode as integer 0; got {v}",
    );
}

/// Absent `tolerationSeconds` — upstream semantics is "tolerate
/// forever". The decoder must not synthesize a key here; the typed
/// downstream `Toleration { toleration_seconds: Option<i64> }` relies on
/// the field's absence to produce `None`.
#[test]
fn test_toleration_seconds_absent_is_omitted() {
    let registry = ProtoRegistry::new();
    // Key only — tolerationSeconds intentionally not encoded.
    let mut bytes = Vec::new();
    write_string_field(&mut bytes, 1, "node.kubernetes.io/unreachable");
    write_string_field(&mut bytes, 2, "Exists");
    write_string_field(&mut bytes, 4, "NoExecute");

    let decoded = registry
        .decode_message("Toleration", &bytes)
        .expect("Toleration schema must be registered");

    assert!(
        decoded.get("tolerationSeconds").is_none(),
        "absent tolerationSeconds must NOT appear in JSON (means 'tolerate forever'); \
         got {decoded}",
    );
}

/// Negative `tolerationSeconds` on the wire — upstream Go's behavior is
/// "treated as 0 (evict immediately) by the system", but the *decoder*
/// must still round-trip the value faithfully. Validation/clamping is a
/// separate layer (and would reject the request before this code ran).
/// We pin the wire→JSON path here so a future "let's clamp during decode"
/// patch doesn't silently change semantics.
#[test]
fn test_toleration_seconds_negative_round_trips_through_decoder() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    write_int_field(&mut bytes, 5, -1);

    let decoded = registry
        .decode_message("Toleration", &bytes)
        .expect("Toleration schema must be registered");

    let v = decoded
        .get("tolerationSeconds")
        .unwrap_or_else(|| panic!("tolerationSeconds=-1 missing in {decoded}"));
    assert_eq!(
        v.as_i64(),
        Some(-1),
        "negative tolerationSeconds must round-trip through the decoder unchanged \
         (validation clamps elsewhere); got {v}",
    );
}

// -- Round-trip: nil vs 0 must remain distinguishable end-to-end -------------

/// End-to-end: take proto bytes with `tolerationSeconds = 0`, decode →
/// JSON, then feed the JSON through the typed
/// `rusternetes_common::resources::Toleration` deserializer. The result
/// must be `Some(0)`. This is the "if proto decoder drops the zero,
/// we'd see None here" check.
#[test]
fn test_toleration_seconds_zero_round_trips_through_typed_decoder() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    write_string_field(&mut bytes, 1, "key");
    write_string_field(&mut bytes, 2, "Equal");
    write_string_field(&mut bytes, 3, "value");
    write_string_field(&mut bytes, 4, "NoExecute");
    write_int_field(&mut bytes, 5, 0);

    let decoded = registry
        .decode_message("Toleration", &bytes)
        .expect("Toleration schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).expect("decoded JSON must re-serialize");
    let typed: Toleration = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!(
            "Toleration must round-trip through typed decoder; \
             decoder produced {decoded}; serde error: {e}",
        )
    });

    assert_eq!(
        typed.toleration_seconds,
        Some(0),
        "tolerationSeconds=0 must surface as Some(0) on the typed Toleration, \
         not None — Some(0) means 'evict immediately', None means 'forever'",
    );
}

/// Same end-to-end shape, but `tolerationSeconds` is absent from the
/// proto bytes. The typed Toleration must surface `None`, distinguishing
/// "tolerate forever" from "evict immediately" via the Option layer.
#[test]
fn test_toleration_seconds_absent_round_trips_to_none_through_typed_decoder() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    write_string_field(&mut bytes, 1, "key");
    write_string_field(&mut bytes, 2, "Equal");
    write_string_field(&mut bytes, 3, "value");
    write_string_field(&mut bytes, 4, "NoExecute");
    // tolerationSeconds (field 5) intentionally omitted.

    let decoded = registry
        .decode_message("Toleration", &bytes)
        .expect("Toleration schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).expect("decoded JSON must re-serialize");
    let typed: Toleration = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!(
            "Toleration must round-trip through typed decoder; \
             decoder produced {decoded}; serde error: {e}",
        )
    });

    assert_eq!(
        typed.toleration_seconds, None,
        "absent tolerationSeconds must surface as None on the typed Toleration \
         ('tolerate forever' semantics) — not Some(0); decoded JSON: {decoded}",
    );
}

/// JSON-only parity layer: feed a fixture with `tolerationSeconds: 0`
/// straight into the serde decoder (no proto) and confirm it survives a
/// JSON re-encode → decode cycle as `Some(0)` rather than being elided.
/// Mirrors `crates/common/tests/roundtrip_core_v1.rs::assert_roundtrip`.
#[test]
fn test_toleration_seconds_zero_survives_json_only_roundtrip() {
    let fixture = r#"{
        "key": "node.kubernetes.io/not-ready",
        "operator": "Exists",
        "effect": "NoExecute",
        "tolerationSeconds": 0
    }"#;
    let decoded: Toleration = serde_json::from_str(fixture).expect("initial decode");
    assert_eq!(decoded.toleration_seconds, Some(0));

    let re_encoded = serde_json::to_string(&decoded).expect("re-encode");
    let re_decoded: Toleration = serde_json::from_str(&re_encoded).expect("second decode");
    assert_eq!(
        re_decoded.toleration_seconds,
        Some(0),
        "Some(0) must survive a JSON encode/decode round-trip; \
         re-encoded body: {re_encoded}",
    );

    // And re-encoded JSON must actually contain the 0 — `skip_serializing_if
    // = Option::is_none"` skips None, NOT Some(0). If a future patch
    // tightens this to `is_none_or_zero`, the semantic distinction breaks.
    let re_value: Value = serde_json::from_str(&re_encoded).expect("re-encoded must be JSON");
    assert_eq!(
        re_value.get("tolerationSeconds"),
        Some(&Value::from(0_i64)),
        "re-encoded JSON must keep `tolerationSeconds: 0`; got {re_encoded}",
    );
}

/// And the inverse: a fixture with `tolerationSeconds` absent must
/// decode to `None`, re-encode without the key, and re-decode to `None`.
/// This is the "forever" branch, which is what the eviction controller
/// in `node_lifecycle_controller` produces for the default
/// `node.kubernetes.io/{not-ready,unreachable}` tolerations when the
/// `--pod-eviction-timeout` flag is absent.
#[test]
fn test_toleration_seconds_absent_survives_json_only_roundtrip() {
    let fixture = r#"{
        "key": "node.kubernetes.io/unreachable",
        "operator": "Exists",
        "effect": "NoExecute"
    }"#;
    let decoded: Toleration = serde_json::from_str(fixture).expect("initial decode");
    assert_eq!(decoded.toleration_seconds, None);

    let re_encoded = serde_json::to_string(&decoded).expect("re-encode");
    let re_value: Value = serde_json::from_str(&re_encoded).expect("re-encoded must be JSON");
    assert!(
        re_value.get("tolerationSeconds").is_none(),
        "absent tolerationSeconds must NOT appear in re-encoded JSON \
         ('forever' semantics); got {re_encoded}",
    );

    let re_decoded: Toleration = serde_json::from_str(&re_encoded).expect("second decode");
    assert_eq!(re_decoded.toleration_seconds, None);
}
