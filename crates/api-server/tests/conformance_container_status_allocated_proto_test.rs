//! Protobuf wire-format regression for
//! `ContainerStatus.allocatedResources` (KEP-1287 in-place pod resize).
//!
//! The six failing `[sig-node] Pod InPlace Resize Container ...` conformance
//! tests assert, right after a pod becomes Running, that each container
//! status's `allocatedResources` equals the container's
//! `spec.resources.requests`:
//!
//!     container[c1] status allocatedResources mismatch:
//!       Expected object to be comparable, diff: v1.ResourceList(
//!       - nil,
//!       + {cpu: 20m, memory: 20Mi})
//!
//! i.e. the actual `allocatedResources` came back `nil`. The kubelet *does*
//! write `allocatedResources` into the stored pod (= the spec's requests),
//! but the conformance client (client-go) fetches the pod over **protobuf**,
//! and our `ProtoRegistry`'s `ContainerStatus` schema previously *omitted*
//! field 10 (`allocatedResources`). The encoder therefore dropped the field
//! on the wire and the client decoded it as `nil` — while the adjacent
//! `resources` field (11), which *was* registered, came through, matching the
//! observed "only allocatedResources mismatches" symptom.
//!
//! These tests pin field 10 as a `map<string, Quantity>` and prove it
//! survives both decode (client receiving) and a JSON→proto→JSON round-trip
//! (the api-server encoding a stored pod for a protobuf GET).
//!
//! K8s ref: `k8s.io/api/core/v1/generated.proto:1137` (release-1.35):
//!   `map<string, .k8s.io...Quantity> allocatedResources = 10;`

use rusternetes_api_server::protobuf::ProtoRegistry;
use serde_json::{json, Value};

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

/// `field_number` + length-delimited (wire type 2) + payload.
fn length_delimited(field_number: u32, payload: &[u8]) -> Vec<u8> {
    let tag = (u64::from(field_number) << 3) | 2;
    let mut out = varint(tag);
    out.extend(varint(payload.len() as u64));
    out.extend_from_slice(payload);
    out
}

/// `Quantity` message: field 1 = canonical string.
fn quantity_msg(canonical: &str) -> Vec<u8> {
    length_delimited(1, canonical.as_bytes())
}

/// `map<string, Quantity>` MapEntry: field 1 = key, field 2 = Quantity message.
fn quantity_map_entry(key: &str, canonical: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend(length_delimited(1, key.as_bytes()));
    buf.extend(length_delimited(2, &quantity_msg(canonical)));
    buf
}

/// Decoding a wire `ContainerStatus` whose field 10 carries
/// `allocatedResources` must surface it as a JSON object of quoted canonical
/// strings — not drop it to `nil`.
#[test]
fn container_status_allocated_resources_decodes_to_string_object() {
    let registry = ProtoRegistry::new();

    let mut bytes = Vec::new();
    // name = field 1
    bytes.extend(length_delimited(1, b"c1"));
    // allocatedResources = field 10, repeated MapEntry
    bytes.extend(length_delimited(10, &quantity_map_entry("cpu", "20m")));
    bytes.extend(length_delimited(10, &quantity_map_entry("memory", "20Mi")));

    let decoded = registry
        .decode_message("ContainerStatus", &bytes)
        .expect("ContainerStatus schema must be registered");

    let allocated = decoded
        .get("allocatedResources")
        .and_then(Value::as_object)
        .unwrap_or_else(|| {
            panic!("allocatedResources must decode to a JSON object, got: {decoded}")
        });
    assert_eq!(
        allocated.get("cpu").and_then(Value::as_str),
        Some("20m"),
        "allocatedResources.cpu must be the canonical quoted string; got {decoded}",
    );
    assert_eq!(
        allocated.get("memory").and_then(Value::as_str),
        Some("20Mi"),
        "allocatedResources.memory must be the canonical quoted string; got {decoded}",
    );
}

/// The api-server's actual path for a protobuf GET: the stored pod's
/// `ContainerStatus` JSON is **encoded** to protobuf, sent to the client,
/// and decoded. `allocatedResources` (and `resources`) must survive the full
/// JSON -> proto -> JSON round trip. Before the fix the encoder silently
/// dropped field 10, so the round-tripped JSON lost `allocatedResources`.
#[test]
fn container_status_allocated_resources_survives_json_proto_round_trip() {
    let registry = ProtoRegistry::new();

    let original = json!({
        "name": "c1",
        "ready": true,
        "restartCount": 0,
        "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
        "started": true,
        "allocatedResources": { "cpu": "20m", "memory": "20Mi" },
        "resources": {
            "requests": { "cpu": "20m", "memory": "20Mi" },
            "limits": { "cpu": "20m", "memory": "20Mi" }
        }
    });

    let encoded = registry
        .encode_message("ContainerStatus", &original)
        .expect("ContainerStatus must encode");
    let decoded = registry
        .decode_message("ContainerStatus", &encoded)
        .expect("ContainerStatus must decode");

    assert_eq!(
        decoded.get("allocatedResources"),
        Some(&json!({ "cpu": "20m", "memory": "20Mi" })),
        "allocatedResources must survive JSON->proto->JSON; got {decoded}",
    );
    // Sanity: the previously-registered `resources` field still round-trips,
    // matching the conformance symptom where only allocatedResources was nil.
    assert_eq!(
        decoded.pointer("/resources/requests"),
        Some(&json!({ "cpu": "20m", "memory": "20Mi" })),
        "resources.requests must also survive; got {decoded}",
    );
}

/// End-to-end via the typed `ContainerStatus`: decode the wire form, feed the
/// JSON into the typed struct (what the pod handler does after the protobuf
/// middleware), and confirm `allocated_resources` is populated.
#[test]
fn container_status_allocated_resources_round_trips_through_typed_struct() {
    use rusternetes_common::resources::ContainerStatus;

    let registry = ProtoRegistry::new();

    // Build the wire form from a full JSON ContainerStatus (so the typed
    // struct's required fields — ready, restartCount — are present), then
    // decode it back as a protobuf client would.
    let original = json!({
        "name": "c1",
        "ready": true,
        "restartCount": 0,
        "allocatedResources": { "cpu": "2m" }
    });
    let bytes = registry
        .encode_message("ContainerStatus", &original)
        .expect("ContainerStatus must encode");

    let decoded = registry
        .decode_message("ContainerStatus", &bytes)
        .expect("ContainerStatus schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).unwrap();
    let cs: ContainerStatus = serde_json::from_slice(&json_bytes)
        .unwrap_or_else(|e| panic!("ContainerStatus must round-trip; {decoded}; serde error: {e}"));

    let allocated = cs
        .allocated_resources
        .expect("allocatedResources must be set after proto decode");
    assert_eq!(allocated.get("cpu").map(String::as_str), Some("2m"));
}
