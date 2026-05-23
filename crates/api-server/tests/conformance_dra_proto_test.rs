//! Wire-format proto tests for the four top-level DRA (Dynamic Resource
//! Allocation) kinds in `resource.k8s.io/v1`:
//!
//!   - `ResourceClaim`
//!   - `ResourceClaimTemplate`
//!   - `DeviceClass`
//!   - `ResourceSlice`
//!
//! Verifies the nested types listed in the parent task survive a
//! proto-bytes -> JSON decode: `DeviceRequest`, `DeviceClaim`,
//! `ExactDeviceRequest`, `AllocationResult`, `DeviceAllocationResult`,
//! `ResourcePool`, `DeviceClassSpec`, `DeviceClassConfiguration`,
//! `CELDeviceSelector`, `DeviceSelector`, `Device`,
//! `DeviceAllocationMode` (string enum).
//!
//! Wire layout follows
//! `k8s.io/api/resource/v1/generated.proto` in upstream Kubernetes
//! release-1.35 (vendored DRA proto is not yet bundled under
//! `crates/api-server/proto/upstream/v1.35/`).
//!
//! Registry coverage status
//! ------------------------
//! The four top-level DRA kinds and their nested message schemas are
//! registered in `ProtoRegistry::new()` under group-qualified keys (see
//! `register_resource_v1` in `src/protobuf.rs`). The unqualified
//! `ResourceClaim` slot is intentionally left as the
//! `core/v1.ResourceClaim` PodSpec sub-message (`{ name, request }`),
//! which has a completely different wire layout from the DRA top-level
//! kind and must not be used to decode DRA resources — the bare-name
//! guard test at the bottom of this file enforces that invariant.
//!
//! Each test below builds the exact upstream wire payload and pins every
//! nested field's `(field_number → json_name → value_shape)` triple. If
//! `decode_message` returns `None` (i.e. somebody removed the schema),
//! the test fails fast rather than silently skipping.
//!
//! Why a group-qualified registry key
//! ----------------------------------
//! `resource.k8s.io/v1.ResourceClaim` collides on bare name with
//! `core/v1.ResourceClaim`. `decode_k8s_resource` already tries
//! `<apiVersion>.<kind>` first and falls back to the bare kind (same
//! pattern used for `events.k8s.io/v1.Event` vs. `core/v1.Event`), so
//! these tests look up the DRA kinds under their group-qualified keys.

use rusternetes_api_server::protobuf::ProtoRegistry;
use serde_json::Value;

// --------------------------------------------------------------------------
// Wire-format helpers
// --------------------------------------------------------------------------

/// Tag byte for a field at `field_number` with wire type 2 (length-delimited).
/// Panics if `field_number > 15` because callers below all stay within the
/// single-byte varint range; for larger field numbers the test would need
/// to emit a two-byte varint tag (e.g. `0x9a 0x01` for field 19, wire 2).
fn ld_tag(field_number: u32) -> u8 {
    assert!(
        field_number <= 15,
        "ld_tag helper only handles single-byte tags; field {field_number} would need a 2-byte varint"
    );
    ((field_number << 3) | 2) as u8
}

/// Tag byte for a field at `field_number` with wire type 0 (varint). Same
/// single-byte caveat as [`ld_tag`]. Wire type 0 is the additive identity
/// when OR'd into the shifted field number, so the tag byte is just
/// `field_number << 3` — but the constant is kept explicit (via the
/// `WIRE_VARINT` symbol) to keep the parity with [`ld_tag`] readable.
fn varint_tag(field_number: u32) -> u8 {
    const WIRE_VARINT: u32 = 0;
    assert!(
        field_number <= 15,
        "varint_tag helper only handles single-byte tags; field {field_number} would need a 2-byte varint"
    );
    ((field_number << 3) | WIRE_VARINT) as u8
}

/// Append a length-delimited (string / bytes / message) field. `len` must
/// fit in a single varint byte (<= 127) which is true for every payload in
/// this file.
fn push_ld(buf: &mut Vec<u8>, field_number: u32, payload: &[u8]) {
    assert!(
        payload.len() <= 127,
        "push_ld payload {} bytes exceeds single-byte varint; split or extend helper",
        payload.len()
    );
    buf.push(ld_tag(field_number));
    buf.push(payload.len() as u8);
    buf.extend_from_slice(payload);
}

/// Append a UTF-8 string field at `field_number`.
fn push_string(buf: &mut Vec<u8>, field_number: u32, s: &str) {
    push_ld(buf, field_number, s.as_bytes());
}

/// Append a varint scalar (int32/int64/uint*/bool) at `field_number`.
/// Multi-byte varints are emitted when needed.
fn push_varint(buf: &mut Vec<u8>, field_number: u32, value: u64) {
    buf.push(varint_tag(field_number));
    let mut v = value;
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

/// Append a bool field. Protobuf bools encode as varint 1 / 0.
fn push_bool(buf: &mut Vec<u8>, field_number: u32, value: bool) {
    push_varint(buf, field_number, u64::from(value));
}

/// Decode `msg_type` from `bytes`, asserting the schema is registered.
/// Fails the test with a structured message pointing at the missing
/// registry entry if `decode_message` returns `None`.
fn decode_expect(registry: &ProtoRegistry, msg_type: &str, bytes: &[u8]) -> Value {
    registry.decode_message(msg_type, bytes).unwrap_or_else(|| {
        panic!(
            "`{msg_type}` is not registered in ProtoRegistry::new(). The four \
             top-level resource.k8s.io/v1 kinds (ResourceClaim, \
             ResourceClaimTemplate, DeviceClass, ResourceSlice) must live \
             under group-qualified keys, and the nested DRA messages must \
             be registered under their bare names — see `register_resource_v1` \
             in src/protobuf.rs."
        )
    })
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

/// `resource.k8s.io/v1.ResourceClaim { metadata { name }, spec { devices {
/// requests: [ DeviceRequest { name, exactly: ExactDeviceRequest {
/// deviceClassName, allocationMode, count } } ] } } }`.
///
/// Once the schemas land, this pins the full nesting chain through
/// `ResourceClaimSpec`, `DeviceClaim`, `DeviceRequest`,
/// `ExactDeviceRequest`. Any of those missing from `ProtoRegistry`
/// collapses the corresponding sub-object to `{}` (or to a wrong value),
/// failing the assertions below.
#[test]
fn test_resource_claim_proto_decodes_full_request_chain() {
    let registry = ProtoRegistry::new();

    // ExactDeviceRequest { deviceClassName="gpu.example.com",
    //                      allocationMode="ExactCount", count=2 }
    let mut exactly = Vec::new();
    push_string(&mut exactly, 1, "gpu.example.com"); // deviceClassName
    push_string(&mut exactly, 3, "ExactCount"); // allocationMode (string enum)
    push_varint(&mut exactly, 4, 2); // count

    // DeviceRequest { name="gpu-req", exactly=<above> }
    let mut device_request = Vec::new();
    push_string(&mut device_request, 1, "gpu-req");
    push_ld(&mut device_request, 2, &exactly);

    // DeviceClaim { requests=[<DeviceRequest>] }
    let mut device_claim = Vec::new();
    push_ld(&mut device_claim, 1, &device_request);

    // ResourceClaimSpec { devices=<DeviceClaim> }
    let mut spec = Vec::new();
    push_ld(&mut spec, 1, &device_claim);

    // ObjectMeta { name="my-claim" }
    let mut metadata = Vec::new();
    push_string(&mut metadata, 1, "my-claim");

    // ResourceClaim { metadata, spec }
    let mut claim = Vec::new();
    push_ld(&mut claim, 1, &metadata);
    push_ld(&mut claim, 2, &spec);

    let decoded = decode_expect(&registry, "resource.k8s.io/v1.ResourceClaim", &claim);

    assert_eq!(
        decoded
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(Value::as_str),
        Some("my-claim"),
        "ResourceClaim.metadata.name must round-trip; got {decoded}",
    );

    let request = decoded
        .pointer("/spec/devices/requests/0")
        .unwrap_or_else(|| panic!("spec.devices.requests[0] must decode; got {decoded}"));
    assert_eq!(
        request.get("name").and_then(Value::as_str),
        Some("gpu-req"),
        "DeviceRequest.name must round-trip; got {request}",
    );

    let exact = request
        .get("exactly")
        .unwrap_or_else(|| panic!("DeviceRequest.exactly must decode; got {request}"));
    assert_eq!(
        exact.get("deviceClassName").and_then(Value::as_str),
        Some("gpu.example.com"),
        "ExactDeviceRequest.deviceClassName must round-trip; got {exact}",
    );
    assert_eq!(
        exact.get("allocationMode").and_then(Value::as_str),
        Some("ExactCount"),
        "ExactDeviceRequest.allocationMode (DeviceAllocationMode enum) must \
         survive as the upstream string form; got {exact}",
    );
    assert_eq!(
        exact.get("count").and_then(Value::as_i64),
        Some(2),
        "ExactDeviceRequest.count must round-trip; got {exact}",
    );
}

/// `resource.k8s.io/v1.ResourceClaim { status { allocation { devices {
/// results: [ DeviceRequestAllocationResult ] } } } }`.
///
/// Exercises the allocation path: `ResourceClaimStatus` →
/// `AllocationResult` → `DeviceAllocationResult` →
/// `DeviceRequestAllocationResult`. Without those four schemas, the
/// allocation tuple silently decodes to `{}` and the scheduler-driven
/// status round-trip drops every field.
#[test]
fn test_resource_claim_proto_decodes_allocation_status() {
    let registry = ProtoRegistry::new();

    // DeviceRequestAllocationResult { request, driver, pool, device }
    let mut alloc_entry = Vec::new();
    push_string(&mut alloc_entry, 1, "gpu-req");
    push_string(&mut alloc_entry, 2, "gpu-driver.example.com");
    push_string(&mut alloc_entry, 3, "gpu-pool-1");
    push_string(&mut alloc_entry, 4, "gpu-0");

    // DeviceAllocationResult { results: [<alloc_entry>] }
    let mut device_alloc = Vec::new();
    push_ld(&mut device_alloc, 1, &alloc_entry);

    // AllocationResult { devices: <device_alloc> }
    let mut allocation = Vec::new();
    push_ld(&mut allocation, 1, &device_alloc);

    // ResourceClaimStatus { allocation: <above> }
    let mut status = Vec::new();
    push_ld(&mut status, 1, &allocation);

    // ResourceClaim { status }
    let mut claim = Vec::new();
    push_ld(&mut claim, 3, &status);

    let decoded = decode_expect(&registry, "resource.k8s.io/v1.ResourceClaim", &claim);

    let result = decoded
        .pointer("/status/allocation/devices/results/0")
        .unwrap_or_else(|| {
            panic!(
                "ResourceClaimStatus → AllocationResult → DeviceAllocationResult → \
                 DeviceRequestAllocationResult chain must decode; got {decoded}"
            )
        });
    assert_eq!(
        result.get("request").and_then(Value::as_str),
        Some("gpu-req"),
    );
    assert_eq!(
        result.get("driver").and_then(Value::as_str),
        Some("gpu-driver.example.com"),
    );
    assert_eq!(
        result.get("pool").and_then(Value::as_str),
        Some("gpu-pool-1"),
    );
    assert_eq!(result.get("device").and_then(Value::as_str), Some("gpu-0"));
}

/// `resource.k8s.io/v1.ResourceClaimTemplate { spec { metadata, spec
/// (ResourceClaimSpec) } }`.
///
/// The template wraps a `ResourceClaimTemplateSpec` whose inner `spec`
/// re-uses `ResourceClaimSpec` — a schema collision would either drop the
/// inner template metadata or shadow the inner claim spec.
#[test]
fn test_resource_claim_template_proto_decodes_nested_spec() {
    let registry = ProtoRegistry::new();

    // ExactDeviceRequest { deviceClassName="ml-gpu.example.com",
    //                      allocationMode="All" }
    let mut exactly = Vec::new();
    push_string(&mut exactly, 1, "ml-gpu.example.com");
    push_string(&mut exactly, 3, "All");

    let mut device_request = Vec::new();
    push_string(&mut device_request, 1, "ml-gpu-req");
    push_ld(&mut device_request, 2, &exactly);

    let mut device_claim = Vec::new();
    push_ld(&mut device_claim, 1, &device_request);

    let mut inner_spec = Vec::new();
    push_ld(&mut inner_spec, 1, &device_claim);

    // Template's own metadata (applied to ResourceClaims produced from it)
    let mut inner_meta = Vec::new();
    push_string(&mut inner_meta, 1, "ml-claim-from-template");

    // ResourceClaimTemplateSpec { metadata, spec }
    let mut template_spec = Vec::new();
    push_ld(&mut template_spec, 1, &inner_meta);
    push_ld(&mut template_spec, 2, &inner_spec);

    // Template's outer metadata
    let mut outer_meta = Vec::new();
    push_string(&mut outer_meta, 1, "my-template");

    // ResourceClaimTemplate { metadata, spec }
    let mut template = Vec::new();
    push_ld(&mut template, 1, &outer_meta);
    push_ld(&mut template, 2, &template_spec);

    let decoded = decode_expect(
        &registry,
        "resource.k8s.io/v1.ResourceClaimTemplate",
        &template,
    );

    assert_eq!(
        decoded.pointer("/metadata/name").and_then(Value::as_str),
        Some("my-template"),
        "ResourceClaimTemplate.metadata.name must round-trip; got {decoded}",
    );
    assert_eq!(
        decoded
            .pointer("/spec/metadata/name")
            .and_then(Value::as_str),
        Some("ml-claim-from-template"),
        "ResourceClaimTemplateSpec.metadata.name must survive; got {decoded}",
    );
    assert_eq!(
        decoded
            .pointer("/spec/spec/devices/requests/0/name")
            .and_then(Value::as_str),
        Some("ml-gpu-req"),
        "Inner ResourceClaimSpec must keep its DeviceRequest; got {decoded}",
    );
    assert_eq!(
        decoded
            .pointer("/spec/spec/devices/requests/0/exactly/allocationMode")
            .and_then(Value::as_str),
        Some("All"),
        "DeviceAllocationMode 'All' must round-trip through template; got {decoded}",
    );
}

/// `resource.k8s.io/v1.DeviceClass { metadata, spec { selectors: [
/// DeviceSelector { cel: CELDeviceSelector { expression } } ], config: [
/// DeviceClassConfiguration { deviceConfiguration { opaque } } ] } }`.
///
/// Pins the full selector + config chain: `DeviceClassSpec` →
/// `DeviceSelector` → `CELDeviceSelector`, plus
/// `DeviceClassConfiguration` → `DeviceConfiguration` →
/// `OpaqueDeviceConfiguration`. `DeviceConfiguration` is inlined per the
/// upstream JSON tag (`json:",inline"`); a non-inline registration would
/// produce `{"deviceConfiguration": {"opaque": ...}}` and break the typed
/// `DeviceClassConfiguration` decoder.
#[test]
fn test_device_class_proto_decodes_selectors_and_config() {
    let registry = ProtoRegistry::new();

    // CELDeviceSelector { expression="device.driver == \"nvidia.com/gpu\"" }
    let mut cel = Vec::new();
    push_string(&mut cel, 1, "device.driver == \"nvidia.com/gpu\"");

    // DeviceSelector { cel=<above> }
    let mut selector = Vec::new();
    push_ld(&mut selector, 1, &cel);

    // OpaqueDeviceConfiguration { driver, parameters (RawExtension) }
    // RawExtension is a message with a single `raw` bytes field
    // containing the JSON body. The registry decodes RawExtension fields
    // via FieldType::JsonRaw which reads field 1 (bytes) as a JSON
    // payload.
    let raw_json = br#"{"mode":"performance"}"#;
    let mut raw_extension = Vec::new();
    push_ld(&mut raw_extension, 1, raw_json);

    let mut opaque = Vec::new();
    push_string(&mut opaque, 1, "example.com/gpu");
    push_ld(&mut opaque, 2, &raw_extension);

    // DeviceConfiguration { opaque }
    let mut device_config = Vec::new();
    push_ld(&mut device_config, 1, &opaque);

    // DeviceClassConfiguration { deviceConfiguration }
    let mut class_config = Vec::new();
    push_ld(&mut class_config, 1, &device_config);

    // DeviceClassSpec { selectors=[selector], config=[class_config] }
    let mut spec = Vec::new();
    push_ld(&mut spec, 1, &selector);
    push_ld(&mut spec, 2, &class_config);

    let mut metadata = Vec::new();
    push_string(&mut metadata, 1, "nvidia-gpu-a100");

    let mut device_class = Vec::new();
    push_ld(&mut device_class, 1, &metadata);
    push_ld(&mut device_class, 2, &spec);

    let decoded = decode_expect(&registry, "resource.k8s.io/v1.DeviceClass", &device_class);

    assert_eq!(
        decoded.pointer("/metadata/name").and_then(Value::as_str),
        Some("nvidia-gpu-a100"),
    );

    let cel_expr = decoded
        .pointer("/spec/selectors/0/cel/expression")
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic!(
                "DeviceClassSpec → DeviceSelector → CELDeviceSelector.expression must \
                 round-trip; got {decoded}"
            )
        });
    assert!(
        cel_expr.contains("nvidia.com/gpu"),
        "CELDeviceSelector.expression body should survive; got {cel_expr:?}",
    );

    // DeviceClassConfiguration inlines DeviceConfiguration per upstream
    // JSON tags, so `opaque` is hoisted to the configuration entry's top
    // level rather than nested under `deviceConfiguration`.
    let config_entry = decoded
        .pointer("/spec/config/0")
        .unwrap_or_else(|| panic!("DeviceClassSpec.config[0] must decode; got {decoded}"));
    let opaque_obj = config_entry.get("opaque").unwrap_or_else(|| {
        panic!(
            "DeviceClassConfiguration must inline DeviceConfiguration so `opaque` \
             lands at the top level (matches upstream `json:\",inline\"` tag); \
             got {config_entry}"
        )
    });
    assert_eq!(
        opaque_obj.get("driver").and_then(Value::as_str),
        Some("example.com/gpu"),
        "OpaqueDeviceConfiguration.driver must round-trip; got {opaque_obj}",
    );
    // parameters is RawExtension → JsonRaw; assert the JSON value made
    // it through.
    let parameters = opaque_obj.get("parameters").unwrap_or_else(|| {
        panic!("OpaqueDeviceConfiguration.parameters must decode as JSON; got {opaque_obj}")
    });
    assert_eq!(
        parameters.get("mode").and_then(Value::as_str),
        Some("performance"),
        "RawExtension parameters JSON body must round-trip; got {parameters}",
    );
}

/// `resource.k8s.io/v1.ResourceSlice { metadata, spec { driver, pool {
/// name, generation, resourceSliceCount }, nodeName, devices: [ Device {
/// name, allNodes } ] } }`.
///
/// Pins `ResourceSliceSpec`, `ResourcePool`, and `Device`. The
/// `Device.allNodes` field exercises a bool scalar inside a repeated
/// message; without `Device` registered the slice's `devices[]` array
/// would decode as a list of `{}`.
#[test]
fn test_resource_slice_proto_decodes_pool_and_devices() {
    let registry = ProtoRegistry::new();

    // ResourcePool { name, generation, resourceSliceCount }
    let mut pool = Vec::new();
    push_string(&mut pool, 1, "gpu-pool-1");
    push_varint(&mut pool, 2, 3); // generation
    push_varint(&mut pool, 3, 5); // resourceSliceCount

    // Device { name, allNodes=true }
    let mut device = Vec::new();
    push_string(&mut device, 1, "gpu-0");
    push_bool(&mut device, 7, true);

    // ResourceSliceSpec { driver, pool, nodeName, devices=[device] }
    let mut spec = Vec::new();
    push_string(&mut spec, 1, "gpu-driver.example.com"); // driver
    push_ld(&mut spec, 2, &pool);
    push_string(&mut spec, 3, "node-1"); // nodeName
    push_ld(&mut spec, 6, &device); // devices (repeated, single entry)

    let mut metadata = Vec::new();
    push_string(&mut metadata, 1, "node-1-gpu-resources");

    let mut slice = Vec::new();
    push_ld(&mut slice, 1, &metadata);
    push_ld(&mut slice, 2, &spec);

    let decoded = decode_expect(&registry, "resource.k8s.io/v1.ResourceSlice", &slice);

    assert_eq!(
        decoded.pointer("/metadata/name").and_then(Value::as_str),
        Some("node-1-gpu-resources"),
    );
    assert_eq!(
        decoded.pointer("/spec/driver").and_then(Value::as_str),
        Some("gpu-driver.example.com"),
    );
    assert_eq!(
        decoded.pointer("/spec/nodeName").and_then(Value::as_str),
        Some("node-1"),
    );

    let pool_obj = decoded
        .pointer("/spec/pool")
        .unwrap_or_else(|| panic!("ResourceSliceSpec.pool must decode; got {decoded}"));
    assert_eq!(
        pool_obj.get("name").and_then(Value::as_str),
        Some("gpu-pool-1"),
    );
    assert_eq!(pool_obj.get("generation").and_then(Value::as_i64), Some(3));
    assert_eq!(
        pool_obj.get("resourceSliceCount").and_then(Value::as_i64),
        Some(5),
        "ResourcePool.resourceSliceCount (proto field 3) must round-trip",
    );

    let dev = decoded
        .pointer("/spec/devices/0")
        .unwrap_or_else(|| panic!("ResourceSliceSpec.devices[0] must decode; got {decoded}"));
    assert_eq!(dev.get("name").and_then(Value::as_str), Some("gpu-0"));
    assert_eq!(
        dev.get("allNodes").and_then(Value::as_bool),
        Some(true),
        "Device.allNodes (proto field 7) must decode as bool; got {dev}",
    );
}

/// Cross-cutting asymmetry guard: the DRA top-level kinds must be
/// registered under group-qualified keys, not under their bare names. A
/// bare-name registration silently collides with `core/v1.ResourceClaim`
/// (PodSpec sub-message) and one of the two would misdecode.
///
/// This test passes today because nothing currently registers the DRA
/// `ResourceClaim` under the bare name. The guard exists so a future PR
/// that adds DRA support but forgets the group-qualified key (and
/// instead overwrites the bare-name slot) lights up red.
#[test]
fn test_bare_resource_claim_does_not_shadow_core_v1_pod_ref() {
    let registry = ProtoRegistry::new();

    // Wire bytes for the DRA shape: ResourceClaim { metadata { name } }.
    // The core/v1 ResourceClaim has field 1 = `name` (string), so any
    // bare-name decode of these bytes either fails (the metadata
    // sub-message bytes aren't valid UTF-8 for the leading length-prefix
    // byte) or surfaces a non-string `name`. Either way it must NOT
    // round-trip the DRA metadata as `/metadata/name`.
    let mut metadata = Vec::new();
    push_string(&mut metadata, 1, "dra-claim");
    let mut dra_claim = Vec::new();
    push_ld(&mut dra_claim, 1, &metadata);

    if let Some(json) = registry.decode_message("ResourceClaim", &dra_claim) {
        let has_nested_metadata =
            json.pointer("/metadata/name").and_then(Value::as_str) == Some("dra-claim");
        assert!(
            !has_nested_metadata,
            "bare `ResourceClaim` schema must not decode the DRA wire shape — \
             it would collide with core/v1.ResourceClaim (PodSpec entry). Use \
             the group-qualified key `resource.k8s.io/v1.ResourceClaim` for DRA. \
             Got: {json}"
        );
    }
}
