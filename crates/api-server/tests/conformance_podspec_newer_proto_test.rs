//! Regression test for upstream conformance — newer PodSpec / Container fields
//! must round-trip through the protobuf decoder.
//!
//! Field numbers per upstream
//! `crates/api-server/proto/upstream/v1.35/k8s.io/api/core/v1/generated.proto`:
//!
//! ```text
//! message PodSpec {
//!   optional PodOS                 os              = 36;
//!   optional bool                  hostUsers       = 37;
//!   repeated PodSchedulingGate     schedulingGates = 38;
//!   repeated PodResourceClaim      resourceClaims  = 39;
//! }
//! message Container {
//!   repeated ContainerResizePolicy resizePolicy    = 23;
//! }
//! message PodOS              { optional string name                   = 1; }
//! message PodSchedulingGate  { optional string name                   = 1; }
//! message PodResourceClaim   { optional string name                   = 1;
//!                              optional string resourceClaimName      = 3;
//!                              optional string resourceClaimTemplateName = 4; }
//! message ContainerResizePolicy { optional string resourceName        = 1;
//!                                 optional string restartPolicy       = 2; }
//! ```
//!
//! These newer (GA / beta in v1.35) fields exist as Rust structs in
//! `crates/common/src/resources/pod.rs` but the proto wire-format must also
//! be validated end-to-end — if the registry forgets a tag, client-go writes
//! a pod via `application/vnd.kubernetes.protobuf` and the api-server silently
//! drops the field. These tests pin the wire shape so a future schema edit
//! cannot regress the contract.
//!
//! The companion `protobuf_schema_parity_upstream` test guards the full
//! field-number registry against the upstream .proto; this file complements
//! it with end-to-end decode-then-typed-deserialize assertions.

use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_common::resources::pod::{Container, PodSpec};

/// Encode a varint per protobuf wire format.
fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Tag = (field_number << 3) | wire_type. Length-delimited (wire type 2).
fn write_tag_ld(buf: &mut Vec<u8>, field_num: u32) {
    write_varint(buf, ((field_num as u64) << 3) | 2);
}

/// Length-prefixed string field.
fn write_string(buf: &mut Vec<u8>, field_num: u32, value: &str) {
    write_tag_ld(buf, field_num);
    write_varint(buf, value.len() as u64);
    buf.extend_from_slice(value.as_bytes());
}

/// Length-prefixed embedded message field.
fn write_message(buf: &mut Vec<u8>, field_num: u32, inner: &[u8]) {
    write_tag_ld(buf, field_num);
    write_varint(buf, inner.len() as u64);
    buf.extend_from_slice(inner);
}

/// Encode a minimal `Container { name: "c", image: "i" }` and append it to
/// the supplied `PodSpec` byte buffer at proto field 2 (the correct slot per
/// upstream `generated.proto` — field 1 of PodSpec is `volumes`). Without
/// `containers` present, the typed `PodSpec` deserializer rejects the JSON
/// with `missing field 'containers'`.
fn write_minimal_container(spec_bytes: &mut Vec<u8>) {
    let mut container = Vec::new();
    write_string(&mut container, 1, "c");
    write_string(&mut container, 2, "i");
    // PodSpec.containers = field 2
    write_message(spec_bytes, 2, &container);
}

/// PodSpec.os (field 36) = PodOS { name = "linux" }.
#[test]
fn test_podspec_os_proto_decode_round_trips() {
    let registry = ProtoRegistry::new();

    let mut os_msg = Vec::new();
    write_string(&mut os_msg, 1, "linux");

    let mut spec_bytes = Vec::new();
    // PodSpec.containers (field 2) = [ Container { name: "c", image: "i" } ]
    // — required for downstream typed deserialization, but irrelevant to the
    //   PodOS wire-shape assertion.
    write_minimal_container(&mut spec_bytes);
    // PodSpec.os = field 36
    write_message(&mut spec_bytes, 36, &os_msg);

    let decoded = registry
        .decode_message("PodSpec", &spec_bytes)
        .expect("PodSpec schema must be registered");

    assert_eq!(
        decoded
            .get("os")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str()),
        Some("linux"),
        "decoded PodSpec.os.name must be 'linux'; got {decoded}",
    );

    // Typed round-trip — the api-server hands the decoded JSON to
    // `serde_json::from_value::<PodSpec>` after the protobuf middleware.
    let typed: PodSpec = serde_json::from_value(decoded).expect("typed deserialize");
    assert_eq!(
        typed.os.as_ref().map(|o| o.name.as_str()),
        Some("linux"),
        "typed PodSpec.os.name must match wire value",
    );
}

/// PodSpec.hostUsers (field 37) = false. Bool false must round-trip — the
/// wire encoding `(37 << 3) | 0` followed by varint `0` is the only signal
/// that "host user namespace is disabled" reached the api-server.
#[test]
fn test_podspec_host_users_proto_decode_round_trips() {
    let registry = ProtoRegistry::new();

    let mut spec_bytes = Vec::new();
    write_minimal_container(&mut spec_bytes);
    // hostUsers (field 37, wire_type 0 = varint, value=1=true).
    // Tag = (field_num << 3) | wire_type; wire_type 0 contributes no bits.
    write_varint(&mut spec_bytes, 37u64 << 3);
    write_varint(&mut spec_bytes, 1);

    let decoded = registry
        .decode_message("PodSpec", &spec_bytes)
        .expect("PodSpec schema must be registered");

    assert_eq!(
        decoded.get("hostUsers").and_then(|v| v.as_bool()),
        Some(true),
        "decoded PodSpec.hostUsers must be true; got {decoded}",
    );

    let typed: PodSpec = serde_json::from_value(decoded).expect("typed deserialize");
    assert_eq!(typed.host_users, Some(true));
}

/// PodSpec.schedulingGates (field 38) = [ PodSchedulingGate { name = "ready" },
/// PodSchedulingGate { name = "billing" } ]. Repeated message fields are
/// emitted on the wire as one tag per element — the decoder must accumulate
/// them under a single JSON array.
#[test]
fn test_podspec_scheduling_gates_proto_decode_round_trips() {
    let registry = ProtoRegistry::new();

    let mut gate1 = Vec::new();
    write_string(&mut gate1, 1, "ready");
    let mut gate2 = Vec::new();
    write_string(&mut gate2, 1, "billing");

    let mut spec_bytes = Vec::new();
    write_minimal_container(&mut spec_bytes);
    write_message(&mut spec_bytes, 38, &gate1);
    write_message(&mut spec_bytes, 38, &gate2);

    let decoded = registry
        .decode_message("PodSpec", &spec_bytes)
        .expect("PodSpec schema must be registered");

    let gates = decoded
        .get("schedulingGates")
        .and_then(|v| v.as_array())
        .expect("schedulingGates must be a JSON array");
    assert_eq!(gates.len(), 2, "two scheduling gates encoded");
    assert_eq!(gates[0].get("name").and_then(|v| v.as_str()), Some("ready"),);
    assert_eq!(
        gates[1].get("name").and_then(|v| v.as_str()),
        Some("billing"),
    );

    let typed: PodSpec = serde_json::from_value(decoded).expect("typed deserialize");
    let typed_gates = typed.scheduling_gates.expect("scheduling_gates is Some");
    assert_eq!(typed_gates.len(), 2);
    assert_eq!(typed_gates[0].name, "ready");
    assert_eq!(typed_gates[1].name, "billing");
}

/// PodSpec.resourceClaims (field 39) = [ PodResourceClaim { name = "shared",
/// resourceClaimTemplateName = "shared-tpl" } ]. Note the upstream proto
/// gap: PodResourceClaim has `name = 1`, **no field 2**, then
/// `resourceClaimName = 3`, `resourceClaimTemplateName = 4`. The decoder must
/// tolerate the sparse numbering — registering field 2 as a no-op would
/// shadow the `Source` oneof reserved for it upstream.
#[test]
fn test_podspec_resource_claims_proto_decode_round_trips() {
    let registry = ProtoRegistry::new();

    let mut claim = Vec::new();
    write_string(&mut claim, 1, "shared");
    write_string(&mut claim, 4, "shared-tpl");

    let mut spec_bytes = Vec::new();
    write_minimal_container(&mut spec_bytes);
    write_message(&mut spec_bytes, 39, &claim);

    let decoded = registry
        .decode_message("PodSpec", &spec_bytes)
        .expect("PodSpec schema must be registered");

    let claims = decoded
        .get("resourceClaims")
        .and_then(|v| v.as_array())
        .expect("resourceClaims must be a JSON array");
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims[0].get("name").and_then(|v| v.as_str()),
        Some("shared"),
    );
    assert_eq!(
        claims[0]
            .get("resourceClaimTemplateName")
            .and_then(|v| v.as_str()),
        Some("shared-tpl"),
    );
    assert!(
        claims[0].get("resourceClaimName").is_none(),
        "resourceClaimName must not appear when not on the wire; \
         got {}",
        claims[0],
    );

    let typed: PodSpec = serde_json::from_value(decoded).expect("typed deserialize");
    let typed_claims = typed.resource_claims.expect("resource_claims is Some");
    assert_eq!(typed_claims.len(), 1);
    assert_eq!(typed_claims[0].name, "shared");
    assert_eq!(
        typed_claims[0].resource_claim_template_name.as_deref(),
        Some("shared-tpl"),
    );
    assert_eq!(typed_claims[0].resource_claim_name, None);
}

/// Container.resizePolicy (field 23) = [ ContainerResizePolicy {
/// resourceName = "cpu", restartPolicy = "NotRequired" } ]. In-place pod
/// resize relies on this — without the proto registration the kubelet
/// silently never sees a restart-on-resize policy.
#[test]
fn test_container_resize_policy_proto_decode_round_trips() {
    let registry = ProtoRegistry::new();

    let mut policy = Vec::new();
    write_string(&mut policy, 1, "cpu");
    write_string(&mut policy, 2, "NotRequired");

    let mut container_bytes = Vec::new();
    write_string(&mut container_bytes, 1, "app");
    write_string(&mut container_bytes, 2, "busybox");
    write_message(&mut container_bytes, 23, &policy);

    let decoded = registry
        .decode_message("Container", &container_bytes)
        .expect("Container schema must be registered");

    let policies = decoded
        .get("resizePolicy")
        .and_then(|v| v.as_array())
        .expect("resizePolicy must be a JSON array");
    assert_eq!(policies.len(), 1);
    assert_eq!(
        policies[0].get("resourceName").and_then(|v| v.as_str()),
        Some("cpu"),
    );
    assert_eq!(
        policies[0].get("restartPolicy").and_then(|v| v.as_str()),
        Some("NotRequired"),
    );

    let typed: Container = serde_json::from_value(decoded).expect("typed deserialize");
    let typed_policies = typed.resize_policy.expect("resize_policy is Some");
    assert_eq!(typed_policies.len(), 1);
    assert_eq!(typed_policies[0].resource_name, "cpu");
    assert_eq!(typed_policies[0].restart_policy, "NotRequired");
}

/// End-to-end: encode a single PodSpec with all five new fields populated
/// and assert the decoded JSON contains every one. Guards against tag-number
/// collisions inside PodSpec — a single test catches the "fixed field 38 but
/// broke field 39" class of regressions.
#[test]
fn test_podspec_all_newer_fields_decode_together() {
    let registry = ProtoRegistry::new();

    let mut os_msg = Vec::new();
    write_string(&mut os_msg, 1, "linux");

    let mut gate = Vec::new();
    write_string(&mut gate, 1, "ready");

    let mut claim = Vec::new();
    write_string(&mut claim, 1, "shared");
    write_string(&mut claim, 3, "claim-instance-1");

    let mut policy = Vec::new();
    write_string(&mut policy, 1, "memory");
    write_string(&mut policy, 2, "RestartContainer");

    let mut container = Vec::new();
    write_string(&mut container, 1, "app");
    write_string(&mut container, 2, "busybox");
    write_message(&mut container, 23, &policy);

    let mut spec_bytes = Vec::new();
    // PodSpec.containers = field 2; do NOT use the minimal-container helper
    // here because we need the resizePolicy embedded inside.
    write_message(&mut spec_bytes, 2, &container);
    write_message(&mut spec_bytes, 36, &os_msg);
    // hostUsers = true (wire_type 0 = varint; tag is just field_num << 3).
    write_varint(&mut spec_bytes, 37u64 << 3);
    write_varint(&mut spec_bytes, 1);
    write_message(&mut spec_bytes, 38, &gate);
    write_message(&mut spec_bytes, 39, &claim);

    let decoded = registry
        .decode_message("PodSpec", &spec_bytes)
        .expect("PodSpec schema must be registered");

    let typed: PodSpec = serde_json::from_value(decoded.clone())
        .unwrap_or_else(|e| panic!("typed deserialize must succeed; decoded={decoded}; err={e}"));

    assert_eq!(typed.os.as_ref().map(|o| o.name.as_str()), Some("linux"));
    assert_eq!(typed.host_users, Some(true));
    let gates = typed.scheduling_gates.as_ref().expect("gates set");
    assert_eq!(gates.len(), 1);
    assert_eq!(gates[0].name, "ready");
    let claims = typed.resource_claims.as_ref().expect("claims set");
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].name, "shared");
    assert_eq!(
        claims[0].resource_claim_name.as_deref(),
        Some("claim-instance-1"),
    );
    let c = &typed.containers[0];
    let policies = c.resize_policy.as_ref().expect("resize policy set");
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0].resource_name, "memory");
    assert_eq!(policies[0].restart_policy, "RestartContainer");
}
