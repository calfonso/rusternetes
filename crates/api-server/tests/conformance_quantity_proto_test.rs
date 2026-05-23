//! Wire-format tests for K8s `Quantity` and `map<string, Quantity>` decoding.
//!
//! Upstream `Quantity` is a protobuf message with a single optional string
//! field (`message Quantity { optional string string = 1; }`), see
//! `k8s.io/apimachinery/pkg/api/resource/generated.proto`. Its JSON form is
//! the canonical string: `"100m"`, `"128Mi"`, `"1.5"`, `"1e6"`, never a bare
//! number. The same rule applies to every `map<string, Quantity>` field —
//! the JSON shape is `{"cpu":"100m","memory":"128Mi"}` (object of quoted
//! strings), not `{"cpu":100,"memory":128}`.
//!
//! These tests pin the contract for our `ProtoRegistry`:
//!
//! 1. `ResourceRequirements` (Container.resources): `limits`/`requests` are
//!    `QuantityMap`s and surface as JSON objects with quoted canonical
//!    strings.
//! 2. `VolumeResourceRequirements` (PersistentVolumeClaim.spec.resources):
//!    same shape, distinct message type upstream.
//! 3. `ResourceQuotaSpec.hard` and `ResourceQuotaStatus.{hard,used}`: same
//!    `QuantityMap` shape, registered with field numbers from
//!    `k8s.io/api/core/v1/generated.proto`.
//! 4. Edge cases for the canonical string form: integer `"1"`, decimal
//!    `"1.5"`, binary SI `"128Mi"`, scientific `"1e6"`, and zero `"0"`.
//! 5. A bare `Quantity` field (`ResourceFieldSelector.divisor`) decodes
//!    directly to the quoted canonical string at the parent JSON level —
//!    NOT to a nested `{"string":"…"}` object.
//!
//! Reference for tag construction:
//!   tag = (field_number << 3) | wire_type
//!   wire_type 2 = length-delimited (string / bytes / message / packed)
//!   length-delimited payload: `varint(len) + bytes`
//!
//! If any of these fail, the protobuf middleware will emit JSON that the
//! typed deserializers in `rusternetes_common::resources` reject — most
//! visibly via `deserialize_quantity_map`, which expects each entry to be
//! a string. The conformance suite's `[sig-scheduling]` and
//! `[sig-storage]` tests all post Pods/PVCs over the protobuf wire, so a
//! regression here breaks roughly half of upstream conformance.

use rusternetes_api_server::protobuf::ProtoRegistry;
use serde_json::Value;

// ---------- wire helpers ---------------------------------------------------

/// Length-delimited (wire type 2) tag for `field_number`. Field numbers
/// above 15 require a multi-byte varint and are written explicitly in the
/// tests that need them.
fn ld_tag_single_byte(field_number: u32) -> u8 {
    assert!(field_number < 16, "use explicit varint for fields >= 16");
    ((field_number << 3) | 2) as u8
}

/// Encode an unsigned varint per protobuf spec.
fn varint(mut value: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    while value >= 0x80 {
        buf.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
    buf
}

/// `field_number` (1..) + length-delimited (wire type 2) + the given payload.
fn length_delimited(field_number: u32, payload: &[u8]) -> Vec<u8> {
    let tag = (u64::from(field_number) << 3) | 2;
    let mut out = varint(tag);
    out.extend(varint(payload.len() as u64));
    out.extend_from_slice(payload);
    out
}

/// Build a `Quantity` message (`field 1 = canonical string`).
fn quantity_msg(canonical: &str) -> Vec<u8> {
    length_delimited(1, canonical.as_bytes())
}

/// Build a `map<string, Quantity>` MapEntry: field 1 = key, field 2 = Quantity message.
fn quantity_map_entry(key: &str, canonical: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend(length_delimited(1, key.as_bytes()));
    buf.extend(length_delimited(2, &quantity_msg(canonical)));
    buf
}

// ---------- ResourceRequirements (Container.resources) --------------------

/// `Container.resources.limits["cpu"] = "100m"` /
/// `requests["memory"] = "128Mi"` — confirms our `ResourceRequirements`
/// schema turns proto MapEntries into a JSON object of quoted canonical
/// strings, exactly as upstream `metav1.ObjectMeta`-style clients expect.
#[test]
fn test_resource_requirements_quantity_map_decodes_to_string_object() {
    let registry = ProtoRegistry::new();

    let mut bytes = Vec::new();
    // limits = field 1, repeated MapEntry
    bytes.extend(length_delimited(1, &quantity_map_entry("cpu", "100m")));
    bytes.extend(length_delimited(1, &quantity_map_entry("memory", "128Mi")));
    // requests = field 2, repeated MapEntry
    bytes.extend(length_delimited(2, &quantity_map_entry("cpu", "50m")));
    bytes.extend(length_delimited(2, &quantity_map_entry("memory", "64Mi")));

    let decoded = registry
        .decode_message("ResourceRequirements", &bytes)
        .expect("ResourceRequirements schema must be registered");

    let limits = decoded
        .get("limits")
        .and_then(Value::as_object)
        .expect("limits must decode to a JSON object");
    assert_eq!(
        limits.get("cpu").and_then(Value::as_str),
        Some("100m"),
        "limits.cpu must be the canonical quoted string '100m'; got {decoded}",
    );
    assert_eq!(
        limits.get("memory").and_then(Value::as_str),
        Some("128Mi"),
        "limits.memory must be the canonical quoted string '128Mi'; got {decoded}",
    );

    let requests = decoded
        .get("requests")
        .and_then(Value::as_object)
        .expect("requests must decode to a JSON object");
    assert_eq!(
        requests.get("cpu").and_then(Value::as_str),
        Some("50m"),
        "requests.cpu must be a quoted string; got {decoded}",
    );
    assert_eq!(
        requests.get("memory").and_then(Value::as_str),
        Some("64Mi"),
        "requests.memory must be a quoted string; got {decoded}",
    );
}

/// Strongest guard: NO map entry value should ever decode as a bare JSON
/// number. The conformance bug we're guarding against is the decoder
/// short-circuiting `Quantity` to a numeric literal (e.g. interpreting
/// `"100"` as `100`), which would break every typed consumer.
#[test]
fn test_resource_requirements_no_value_decodes_as_bare_number() {
    let registry = ProtoRegistry::new();

    let mut bytes = Vec::new();
    bytes.extend(length_delimited(1, &quantity_map_entry("cpu", "1")));
    bytes.extend(length_delimited(1, &quantity_map_entry("memory", "1")));

    let decoded = registry
        .decode_message("ResourceRequirements", &bytes)
        .expect("ResourceRequirements schema must be registered");

    let limits = decoded
        .get("limits")
        .and_then(Value::as_object)
        .expect("limits must decode to a JSON object");
    for (k, v) in limits {
        assert!(
            v.is_string(),
            "limits.{k} must be a quoted string, got {v:?} (bare number = canary regression)",
        );
        assert!(
            !v.is_number(),
            "limits.{k} must NOT be a JSON number; got {v:?}",
        );
    }
}

/// Edge cases for the canonical string form. Upstream `Quantity.String()`
/// preserves the original suffix style — integer / decimal / binary SI /
/// scientific notation — and the decoder must pass each through verbatim.
#[test]
fn test_resource_requirements_canonical_quantity_forms_pass_through() {
    let registry = ProtoRegistry::new();

    let cases = [
        ("int", "1"),
        ("decimal", "1.5"),
        ("binary", "128Mi"),
        ("scientific", "1e6"),
        ("zero", "0"),
        ("milli", "100m"),
        ("decimal-si", "32M"),
    ];

    let mut bytes = Vec::new();
    for (k, v) in &cases {
        bytes.extend(length_delimited(1, &quantity_map_entry(k, v)));
    }

    let decoded = registry
        .decode_message("ResourceRequirements", &bytes)
        .expect("ResourceRequirements schema must be registered");
    let limits = decoded
        .get("limits")
        .and_then(Value::as_object)
        .expect("limits must decode to a JSON object");

    for (k, expected) in cases {
        let got = limits
            .get(k)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("missing limits.{k} in {decoded}"));
        assert_eq!(
            got, expected,
            "limits.{k} must round-trip the canonical form verbatim; got {got:?}",
        );
    }
}

/// End-to-end shape check: decode a `Container` carrying `resources.limits`
/// and `resources.requests`, then feed the resulting JSON to the typed
/// `rusternetes_common::resources::Container`. This is exactly what the
/// pod handler does after the protobuf middleware rewrites the body to
/// JSON — if the decoder emitted bare numbers, `deserialize_quantity_map`
/// would reject the request with `expected string`.
#[test]
fn test_container_resources_round_trip_through_typed_container() {
    use rusternetes_common::resources::Container;

    let registry = ProtoRegistry::new();

    // Build ResourceRequirements with both limits and requests.
    let mut rr = Vec::new();
    rr.extend(length_delimited(1, &quantity_map_entry("cpu", "100m")));
    rr.extend(length_delimited(1, &quantity_map_entry("memory", "128Mi")));
    rr.extend(length_delimited(2, &quantity_map_entry("cpu", "50m")));
    rr.extend(length_delimited(2, &quantity_map_entry("memory", "64Mi")));

    // Container { name="c", image="busybox", resources=<rr> }
    // Container fields per generated.proto: name=1, image=2, resources=8.
    let mut container_bytes = Vec::new();
    container_bytes.push(ld_tag_single_byte(1)); // name
    container_bytes.push(1);
    container_bytes.extend_from_slice(b"c");
    container_bytes.push(ld_tag_single_byte(2)); // image
    container_bytes.push(7);
    container_bytes.extend_from_slice(b"busybox");
    container_bytes.extend(length_delimited(8, &rr)); // resources

    let decoded = registry
        .decode_message("Container", &container_bytes)
        .expect("Container schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).unwrap();
    let container: Container = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!(
            "Container with resources must round-trip; decoder produced {decoded}; \
             serde error: {e}",
        )
    });

    let resources = container.resources.expect("resources must be set");
    let limits = resources.limits.expect("limits must be set");
    assert_eq!(limits.get("cpu").map(String::as_str), Some("100m"));
    assert_eq!(limits.get("memory").map(String::as_str), Some("128Mi"));
    let requests = resources.requests.expect("requests must be set");
    assert_eq!(requests.get("cpu").map(String::as_str), Some("50m"));
    assert_eq!(requests.get("memory").map(String::as_str), Some("64Mi"));
}

// ---------- VolumeResourceRequirements (PVC.spec.resources) ---------------

/// Distinct upstream type (`message VolumeResourceRequirements`) but
/// identical field layout for limits/requests. Pin it separately so a
/// future PVC-schema edit can't silently change the shape on this side.
#[test]
fn test_volume_resource_requirements_quantity_map_decodes_to_strings() {
    let registry = ProtoRegistry::new();

    let mut bytes = Vec::new();
    // requests["storage"] = "1Gi"
    bytes.extend(length_delimited(2, &quantity_map_entry("storage", "1Gi")));

    let decoded = registry
        .decode_message("VolumeResourceRequirements", &bytes)
        .expect("VolumeResourceRequirements schema must be registered");

    let requests = decoded
        .get("requests")
        .and_then(Value::as_object)
        .expect("requests must decode to a JSON object");
    assert_eq!(
        requests.get("storage").and_then(Value::as_str),
        Some("1Gi"),
        "requests.storage must be the canonical quoted string '1Gi'; got {decoded}",
    );
}

/// End-to-end via `PersistentVolumeClaimSpec.resources.requests.storage`.
/// Builds the proto wire form upstream clients send when creating a PVC,
/// then round-trips through the typed `PersistentVolumeClaimSpec`. The
/// inner `Quantity` value is verified via the parent message — there's no
/// "bare Quantity field" in PVCSpec, but `storage` is the canonical bare
/// scalar exposed in conformance tests like `[sig-storage] Dynamic
/// Provisioning Should provision storage with non-default reclaim policy
/// [Conformance]`.
#[test]
fn test_pvc_spec_resources_requests_storage_round_trips() {
    use rusternetes_common::resources::PersistentVolumeClaimSpec;

    let registry = ProtoRegistry::new();

    // VolumeResourceRequirements { requests["storage"] = "10Gi" }
    let rr = length_delimited(2, &quantity_map_entry("storage", "10Gi"));
    // PersistentVolumeClaimSpec { resources = <rr> } — resources is field 2.
    let spec_bytes = length_delimited(2, &rr);

    let decoded = registry
        .decode_message("PersistentVolumeClaimSpec", &spec_bytes)
        .expect("PersistentVolumeClaimSpec schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).unwrap();
    let spec: PersistentVolumeClaimSpec = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!("PVC spec must round-trip; decoder produced {decoded}; serde error: {e}",)
    });

    let requests = spec
        .resources
        .requests
        .expect("PVC spec.resources.requests must be set");
    assert_eq!(
        requests.get("storage").map(String::as_str),
        Some("10Gi"),
        "PVC spec.resources.requests.storage must round-trip as a quoted '10Gi'",
    );
}

// ---------- ResourceQuotaSpec.hard ----------------------------------------

/// `ResourceQuotaSpec.hard` is `map<string, Quantity>` at field 1 in
/// upstream `k8s.io/api/core/v1/generated.proto`. The decoded JSON must
/// be an object of quoted canonical strings so the typed
/// `rusternetes_common::resources::ResourceQuotaSpec` deserializer
/// (`deserialize_quantity_map`) accepts it.
#[test]
fn test_resource_quota_spec_hard_decodes_to_quoted_strings() {
    let registry = ProtoRegistry::new();

    let mut bytes = Vec::new();
    bytes.extend(length_delimited(1, &quantity_map_entry("pods", "10")));
    bytes.extend(length_delimited(
        1,
        &quantity_map_entry("requests.cpu", "1"),
    ));
    bytes.extend(length_delimited(
        1,
        &quantity_map_entry("requests.memory", "1Gi"),
    ));
    bytes.extend(length_delimited(1, &quantity_map_entry("limits.cpu", "2")));
    bytes.extend(length_delimited(
        1,
        &quantity_map_entry("limits.memory", "2Gi"),
    ));

    let decoded = registry
        .decode_message("ResourceQuotaSpec", &bytes)
        .expect("ResourceQuotaSpec schema must be registered");

    let hard = decoded
        .get("hard")
        .and_then(Value::as_object)
        .expect("ResourceQuotaSpec.hard must decode to a JSON object");

    for (k, expected) in [
        ("pods", "10"),
        ("requests.cpu", "1"),
        ("requests.memory", "1Gi"),
        ("limits.cpu", "2"),
        ("limits.memory", "2Gi"),
    ] {
        let got = hard
            .get(k)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("missing hard.{k} in {decoded}"));
        assert_eq!(
            got, expected,
            "ResourceQuotaSpec.hard.{k} must be the canonical quoted string",
        );
    }
}

/// End-to-end via the typed `ResourceQuotaSpec`. Mirrors what the
/// admission/quota controllers parse out of the wire on every
/// ResourceQuota create/update.
#[test]
fn test_resource_quota_spec_round_trips_through_typed_struct() {
    use rusternetes_common::resources::ResourceQuotaSpec;

    let registry = ProtoRegistry::new();

    let mut bytes = Vec::new();
    bytes.extend(length_delimited(1, &quantity_map_entry("pods", "10")));
    bytes.extend(length_delimited(1, &quantity_map_entry("services", "5")));

    let decoded = registry
        .decode_message("ResourceQuotaSpec", &bytes)
        .expect("ResourceQuotaSpec schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).unwrap();
    let spec: ResourceQuotaSpec = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!(
            "ResourceQuotaSpec must round-trip; decoder produced {decoded}; \
             serde error: {e}",
        )
    });
    let hard = spec.hard.expect("hard must be set");
    assert_eq!(hard.get("pods").map(String::as_str), Some("10"));
    assert_eq!(hard.get("services").map(String::as_str), Some("5"));
}

/// `ResourceQuotaStatus` has both `hard` (field 1) and `used` (field 2),
/// both `map<string, Quantity>`. Confirms both decode as quoted-string
/// objects — the kube-apiserver returns this shape on every quota read.
#[test]
fn test_resource_quota_status_hard_and_used_decode_to_strings() {
    let registry = ProtoRegistry::new();

    let mut bytes = Vec::new();
    bytes.extend(length_delimited(1, &quantity_map_entry("pods", "10")));
    bytes.extend(length_delimited(2, &quantity_map_entry("pods", "3")));

    let decoded = registry
        .decode_message("ResourceQuotaStatus", &bytes)
        .expect("ResourceQuotaStatus schema must be registered");

    let hard = decoded
        .get("hard")
        .and_then(Value::as_object)
        .expect("hard must decode to a JSON object");
    assert_eq!(hard.get("pods").and_then(Value::as_str), Some("10"));

    let used = decoded
        .get("used")
        .and_then(Value::as_object)
        .expect("used must decode to a JSON object");
    assert_eq!(used.get("pods").and_then(Value::as_str), Some("3"));
}

// ---------- bare Quantity field --------------------------------------------

/// `ResourceFieldSelector.divisor` is a bare (non-map) `Quantity` field.
/// At the JSON level it must surface as a single quoted canonical string
/// — NOT a nested `{"string":"…"}` object that would leak the proto
/// internals.
#[test]
fn test_resource_field_selector_divisor_bare_quantity_is_quoted_string() {
    let registry = ProtoRegistry::new();

    // ResourceFieldSelector { containerName="c", resource="limits.cpu", divisor="1m" }
    // Field numbers: containerName=1, resource=2, divisor=3.
    let mut bytes = Vec::new();
    bytes.push(ld_tag_single_byte(1));
    bytes.push(1);
    bytes.extend_from_slice(b"c");
    bytes.push(ld_tag_single_byte(2));
    bytes.push(10);
    bytes.extend_from_slice(b"limits.cpu");
    bytes.extend(length_delimited(3, &quantity_msg("1m")));

    let decoded = registry
        .decode_message("ResourceFieldSelector", &bytes)
        .expect("ResourceFieldSelector schema must be registered");

    let divisor = decoded.get("divisor");
    assert_eq!(
        divisor.and_then(Value::as_str),
        Some("1m"),
        "divisor must surface as the bare quoted canonical string '1m', \
         not a nested object; got {decoded}",
    );
    assert!(
        divisor.map(|v| v.is_string()).unwrap_or(false),
        "divisor must be a JSON string, got {divisor:?}",
    );
    assert!(
        divisor.map(|v| !v.is_object()).unwrap_or(true),
        "divisor must NOT be a nested object exposing the Quantity proto internals",
    );
}

/// Edge case: a `Quantity` field whose proto payload is the empty
/// canonical form. Upstream `Quantity{}` serializes to `""` and our
/// decoder must mirror that — anything else (e.g. `null`, missing key)
/// breaks string round-trip parity.
#[test]
fn test_bare_quantity_empty_canonical_form_decodes_to_empty_string() {
    let registry = ProtoRegistry::new();

    // ResourceFieldSelector { divisor = Quantity{} } — divisor field 3,
    // with an empty Quantity message (no inner string).
    let mut bytes = Vec::new();
    bytes.extend(length_delimited(3, &[])); // divisor = Quantity {}

    let decoded = registry
        .decode_message("ResourceFieldSelector", &bytes)
        .expect("ResourceFieldSelector schema must be registered");

    assert_eq!(
        decoded.get("divisor").and_then(Value::as_str),
        Some(""),
        "empty Quantity message must decode to an empty quoted string; got {decoded}",
    );
}
