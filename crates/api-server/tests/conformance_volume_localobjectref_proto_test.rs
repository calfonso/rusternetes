//! Regression tests for the volume-source family of `LocalObjectReference`
//! embeddings — the volume-side counterpart of PR #722, which fixed the
//! identical bug for `EnvFromSource`'s `ConfigMapEnvSource` and
//! `SecretEnvSource`.
//!
//! Upstream `staging/src/k8s.io/api/core/v1/types.go` (release-1.35) embeds
//! `LocalObjectReference` into several volume-source structs with the Go tag
//! `json:",inline"`, meaning the embedded `name` field surfaces at the parent
//! object's top level in JSON:
//!
//!     type ConfigMapVolumeSource struct {
//!         LocalObjectReference `json:",inline"  protobuf:"bytes,1,opt,..."`
//!         ...
//!     }
//!
//! In proto, however, the same field is wire-tag 1 as a nested message. The
//! protobuf→JSON middleware must therefore decode field 1 with
//! `FieldType::InlineMessage("LocalObjectReference")` so the reconstructed
//! JSON reads `{"name":"…"}` rather than `{"localObjectReference":{"name":"…"}}`.
//! Otherwise downstream `serde_json::from_slice::<…>` calls in the typed
//! handlers fail with `missing field 'name'` — the exact failure mode PR #722
//! documented for envFrom.
//!
//! `SecretVolumeSource` is a deliberate exception. Field 1 there is a plain
//! `secretName: string` (NOT an inlined `LocalObjectReference`). This is the
//! historical inconsistency between env-source and volume-source for Secret
//! that conformance keeps depending on; we pin it explicitly so a future
//! "consistency cleanup" can't silently break wire compatibility.
//!
//! Non-inlined embeddings (`CSIVolumeSource.nodePublishSecretRef`,
//! `RBDVolumeSource.secretRef`, `ISCSIVolumeSource.secretRef`, …) use a plain
//! `*LocalObjectReference` with the regular `json:"…,omitempty"` tag — those
//! must surface as nested `{"secretRef":{"name":"…"}}` JSON objects, NOT
//! flattened. Each non-inlined entry below pins that nesting too.
//!
//! Mirrors `conformance_configmap_envfrom_prefixes_test.rs` for the envFrom
//! side; together they cover both halves of the `LocalObjectReference` story.

use rusternetes_api_server::protobuf::ProtoRegistry;

/// Encode a varint into `buf`.
fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Wire-tag for `(field_number, wire_type)`.
fn tag(field: u32, wire_type: u32) -> u64 {
    ((field as u64) << 3) | (wire_type as u64)
}

/// Append a length-delimited field with the given field number and payload.
fn push_length_delimited(buf: &mut Vec<u8>, field: u32, payload: &[u8]) {
    write_varint(buf, tag(field, 2));
    write_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

/// Append a string field at the given proto field number.
fn push_string(buf: &mut Vec<u8>, field: u32, s: &str) {
    push_length_delimited(buf, field, s.as_bytes());
}

/// Build proto bytes for `LocalObjectReference { name: <name> }`.
fn local_object_reference(name: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    push_string(&mut buf, 1, name);
    buf
}

/// Decode and assert that field 1 of `msg_type` is an *inlined*
/// `LocalObjectReference`: the decoded JSON has `name` at the top level and
/// does NOT introduce a `localObjectReference` nesting layer.
fn assert_local_object_ref_inlined(msg_type: &str, name: &str) {
    let registry = ProtoRegistry::new();
    let inner = local_object_reference(name);
    let mut outer = Vec::new();
    push_length_delimited(&mut outer, 1, &inner);

    let decoded = registry
        .decode_message(msg_type, &outer)
        .unwrap_or_else(|| panic!("{msg_type} schema must be registered"));

    assert_eq!(
        decoded.get("name").and_then(|v| v.as_str()),
        Some(name),
        "{msg_type}: decoded JSON must inline LocalObjectReference.name at \
         the top level; got {decoded}",
    );
    assert!(
        decoded.get("localObjectReference").is_none(),
        "{msg_type}: decoded JSON must NOT wrap fields in `localObjectReference`; \
         got {decoded}",
    );
}

// ---------------------------------------------------------------------------
// Inlined LocalObjectReference embeddings.
// ---------------------------------------------------------------------------

/// `ConfigMapVolumeSource` field 1 is an embedded `LocalObjectReference` whose
/// Go JSON tag is `json:",inline"`. The decoder must surface
/// `{"name":"my-cm"}` so the typed `ConfigMapVolumeSource` (which has
/// `pub name: Option<String>` directly) round-trips.
#[test]
fn test_configmap_volume_source_proto_inlines_local_object_reference() {
    assert_local_object_ref_inlined("ConfigMapVolumeSource", "my-cm");
}

/// `ConfigMapProjection` (the projected-volume variant) shares the same
/// inlined embedding. Mirrors the `ConfigMapVolumeSource` test so a future
/// schema edit can't fix one and forget the other.
#[test]
fn test_configmap_projection_proto_inlines_local_object_reference() {
    assert_local_object_ref_inlined("ConfigMapProjection", "projected-cm");
}

/// `SecretProjection` mirrors `ConfigMapProjection`: field 1 is an inlined
/// `LocalObjectReference`. The struct's `name: Option<String>` field must
/// receive the value directly, not via a wrapping `localObjectReference` key.
#[test]
fn test_secret_projection_proto_inlines_local_object_reference() {
    assert_local_object_ref_inlined("SecretProjection", "projected-secret");
}

/// `SecretVolumeSource` is the deliberate exception — field 1 is a plain
/// `secretName: string`, NOT a `LocalObjectReference`. This is the historical
/// volume-source / env-source inconsistency for Secret. Pin it so a future
/// "make it consistent" cleanup can't silently break wire compatibility.
///
/// Concretely, with the bytes `tag=1 (length-delimited) + len + "my-secret"`,
/// the decoded JSON must read `{"secretName":"my-secret"}` — *not*
/// `{"name":"my-secret"}` (which would be the case if someone "fixed" it to
/// match ConfigMapVolumeSource).
#[test]
fn test_secret_volume_source_proto_uses_secret_name_string() {
    let registry = ProtoRegistry::new();
    // Field 1 is `secretName: string`, not a wrapped LocalObjectReference. So
    // we emit a *raw* string at field 1, not a nested message.
    let mut bytes = Vec::new();
    push_string(&mut bytes, 1, "my-secret");

    let decoded = registry
        .decode_message("SecretVolumeSource", &bytes)
        .expect("SecretVolumeSource schema must be registered");

    assert_eq!(
        decoded.get("secretName").and_then(|v| v.as_str()),
        Some("my-secret"),
        "SecretVolumeSource: field 1 must decode as secretName (string), not \
         as an inlined LocalObjectReference; got {decoded}",
    );
    assert!(
        decoded.get("name").is_none(),
        "SecretVolumeSource: must NOT surface a top-level `name` key — that \
         would indicate field 1 was wrongly registered as an inlined \
         LocalObjectReference; got {decoded}",
    );
}

// ---------------------------------------------------------------------------
// Non-inlined LocalObjectReference embeddings: must remain nested in JSON.
// ---------------------------------------------------------------------------

/// Decode and assert that the named field of `msg_type` is a *nested*
/// `LocalObjectReference`: the decoded JSON has `{field_name: {"name": …}}`
/// and does NOT flatten `name` to the parent.
fn assert_local_object_ref_nested(msg_type: &str, proto_field: u32, json_field: &str, name: &str) {
    let registry = ProtoRegistry::new();
    let inner = local_object_reference(name);
    let mut outer = Vec::new();
    push_length_delimited(&mut outer, proto_field, &inner);

    let decoded = registry
        .decode_message(msg_type, &outer)
        .unwrap_or_else(|| panic!("{msg_type} schema must be registered"));

    let nested = decoded.get(json_field).unwrap_or_else(|| {
        panic!(
            "{msg_type}: expected nested `{json_field}` object in decoded JSON; \
             got {decoded}",
        )
    });
    assert_eq!(
        nested.get("name").and_then(|v| v.as_str()),
        Some(name),
        "{msg_type}: nested `{json_field}.name` must round-trip; got {decoded}",
    );
    assert!(
        decoded.get("name").is_none(),
        "{msg_type}: must NOT flatten LocalObjectReference.name to the top \
         level — that's only correct for inlined embeddings; got {decoded}",
    );
}

/// `CSIVolumeSource.nodePublishSecretRef` is a `*LocalObjectReference` with a
/// plain `json:"nodePublishSecretRef,omitempty"` tag (NOT `json:",inline"`).
/// JSON must therefore surface as `{"nodePublishSecretRef":{"name":"…"}}`.
#[test]
fn test_csi_volume_source_node_publish_secret_ref_proto_is_nested() {
    assert_local_object_ref_nested("CSIVolumeSource", 5, "nodePublishSecretRef", "csi-secret");
}

/// `RBDVolumeSource.secretRef` is a `*LocalObjectReference` with a plain
/// `json:"secretRef,omitempty"` tag. The nested shape is load-bearing for the
/// typed `RBDVolumeSource.secret_ref: Option<LocalObjectReference>` field.
#[test]
fn test_rbd_volume_source_secret_ref_proto_is_nested() {
    assert_local_object_ref_nested("RBDVolumeSource", 7, "secretRef", "rbd-secret");
}

/// `ISCSIVolumeSource.secretRef` matches the RBD shape — proto field 10 is a
/// nested, non-inlined `LocalObjectReference`. Pin the nesting so a future
/// edit can't accidentally flatten it.
#[test]
fn test_iscsi_volume_source_secret_ref_proto_is_nested() {
    assert_local_object_ref_nested("ISCSIVolumeSource", 10, "secretRef", "iscsi-secret");
}

// ---------------------------------------------------------------------------
// End-to-end round-trip through the typed deserializer — the single most
// important assertion since it mirrors what the protobuf middleware feeds to
// `handlers::pod::create` after rewriting the request body.
// ---------------------------------------------------------------------------

/// Decode a `ConfigMapVolumeSource` proto with both an inlined name and an
/// `items` list, then feed the resulting JSON to the typed
/// `ConfigMapVolumeSource::deserialize` — exactly the call site the protobuf
/// middleware exercises after rewriting the request body for a pod create
/// that mounts a configmap.
#[test]
fn test_configmap_volume_source_round_trips_through_typed_deserializer() {
    use rusternetes_common::resources::ConfigMapVolumeSource;

    let registry = ProtoRegistry::new();
    // ConfigMapVolumeSource { localObjectReference { name: "my-cm" }, items: [], defaultMode: 420 }
    let inner = local_object_reference("my-cm");
    let mut bytes = Vec::new();
    push_length_delimited(&mut bytes, 1, &inner);
    // defaultMode (field 3, varint)
    write_varint(&mut bytes, tag(3, 0));
    write_varint(&mut bytes, 420);

    let decoded = registry
        .decode_message("ConfigMapVolumeSource", &bytes)
        .expect("ConfigMapVolumeSource schema must be registered");

    let json = serde_json::to_vec(&decoded).unwrap();
    let typed: ConfigMapVolumeSource = serde_json::from_slice(&json).unwrap_or_else(|e| {
        panic!(
            "ConfigMapVolumeSource must round-trip through serde; decoder \
             produced {decoded}; serde error: {e}",
        )
    });

    assert_eq!(
        typed.name.as_deref(),
        Some("my-cm"),
        "ConfigMapVolumeSource.name must round-trip from the inlined \
         LocalObjectReference.name proto field",
    );
    assert_eq!(typed.default_mode, Some(420));
}

/// Same e2e round-trip for `SecretVolumeSource` — verifies that the explicit
/// `secretName` field (NOT an inlined LocalObjectReference) lands on the
/// typed struct's `secret_name` field correctly.
#[test]
fn test_secret_volume_source_round_trips_through_typed_deserializer() {
    use rusternetes_common::resources::SecretVolumeSource;

    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    push_string(&mut bytes, 1, "my-secret");
    write_varint(&mut bytes, tag(3, 0));
    write_varint(&mut bytes, 384);

    let decoded = registry
        .decode_message("SecretVolumeSource", &bytes)
        .expect("SecretVolumeSource schema must be registered");

    let json = serde_json::to_vec(&decoded).unwrap();
    let typed: SecretVolumeSource = serde_json::from_slice(&json).unwrap_or_else(|e| {
        panic!(
            "SecretVolumeSource must round-trip through serde; decoder \
             produced {decoded}; serde error: {e}",
        )
    });

    assert_eq!(
        typed.secret_name.as_deref(),
        Some("my-secret"),
        "SecretVolumeSource.secret_name must round-trip from the secretName \
         (string, NOT LocalObjectReference) proto field",
    );
    assert_eq!(typed.default_mode, Some(384));
}

/// Same e2e round-trip for `ConfigMapProjection` — the projected-volume
/// counterpart. The typed struct's `name` field must receive the value from
/// the inlined `LocalObjectReference.name` proto field.
#[test]
fn test_configmap_projection_round_trips_through_typed_deserializer() {
    use rusternetes_common::resources::ConfigMapProjection;

    let registry = ProtoRegistry::new();
    let inner = local_object_reference("projected-cm");
    let mut bytes = Vec::new();
    push_length_delimited(&mut bytes, 1, &inner);

    let decoded = registry
        .decode_message("ConfigMapProjection", &bytes)
        .expect("ConfigMapProjection schema must be registered");

    let json = serde_json::to_vec(&decoded).unwrap();
    let typed: ConfigMapProjection = serde_json::from_slice(&json).unwrap_or_else(|e| {
        panic!(
            "ConfigMapProjection must round-trip through serde; decoder \
             produced {decoded}; serde error: {e}",
        )
    });

    assert_eq!(typed.name.as_deref(), Some("projected-cm"));
}

/// Same e2e round-trip for `SecretProjection`.
#[test]
fn test_secret_projection_round_trips_through_typed_deserializer() {
    use rusternetes_common::resources::SecretProjection;

    let registry = ProtoRegistry::new();
    let inner = local_object_reference("projected-secret");
    let mut bytes = Vec::new();
    push_length_delimited(&mut bytes, 1, &inner);

    let decoded = registry
        .decode_message("SecretProjection", &bytes)
        .expect("SecretProjection schema must be registered");

    let json = serde_json::to_vec(&decoded).unwrap();
    let typed: SecretProjection = serde_json::from_slice(&json).unwrap_or_else(|e| {
        panic!(
            "SecretProjection must round-trip through serde; decoder \
             produced {decoded}; serde error: {e}",
        )
    });

    assert_eq!(typed.name.as_deref(), Some("projected-secret"));
}
