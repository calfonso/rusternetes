//! Regression test for upstream conformance
//! `[sig-node] ConfigMap should be consumable as environment variable names
//! with various prefixes [Conformance]`
//! (`k8s.io/kubernetes/test/e2e/common/node/configmap.go:261`, k8s v1.34+).
//!
//! The conformance canary
//! (https://github.com/indyjonesnl/rusternetes/actions/runs/26249262860/job/77255760479)
//! reported:
//!
//!     Error creating Pod: failed to decode: missing field `name` at line 1 column 311
//!
//! Root cause is in the protobuf→JSON middleware, not the JSON decoder:
//! client-go negotiates `application/vnd.kubernetes.protobuf` for write paths,
//! so the Pod body lands as native protobuf. The `ProtoRegistry` schema for
//! `ConfigMapEnvSource` / `SecretEnvSource` declares field 1 as
//! `FieldType::Message("LocalObjectReference")` — but upstream JSON tags
//! flatten that field's contents into the parent object (`json:",inline"`).
//! The reconstructed JSON therefore reads
//!
//!     {"localObjectReference":{"name":"…"}}
//!
//! whereas our typed `ConfigMapEnvSource` (and stock client-go on read-back)
//! expects `{"name":"…"}`. serde_json correctly raises
//! `missing field 'name'` because the flattened field is buried one level
//! deeper. The fix is to mark that field as `FieldType::InlineMessage`, the
//! same variant already used for every other `LocalObjectReference`
//! embedding (`ConfigMapVolumeSource`, `SecretProjection`, …).

use rusternetes_api_server::protobuf::ProtoRegistry;

/// Hand-crafted protobuf bytes for `ConfigMapEnvSource { localObjectReference
/// { name: "x" } }` per `k8s.io/api/core/v1/generated.proto`.
fn configmap_env_source_with_name(name: &str) -> Vec<u8> {
    let mut inner = Vec::new();
    // LocalObjectReference.name (field 1, length-delimited)
    inner.push(0x0a);
    inner.push(name.len() as u8);
    inner.extend_from_slice(name.as_bytes());

    let mut outer = Vec::new();
    // ConfigMapEnvSource.localObjectReference (field 1, length-delimited)
    outer.push(0x0a);
    outer.push(inner.len() as u8);
    outer.extend_from_slice(&inner);
    outer
}

/// Field 1 of `ConfigMapEnvSource` is an embedded `LocalObjectReference` in
/// proto, but upstream JSON inlines its contents into the parent (Go tag is
/// `json:",inline"`). Our `Rust` `ConfigMapEnvSource` has `pub name: String`
/// directly — so the protobuf→JSON decoder must surface `{"name":"…"}` rather
/// than `{"localObjectReference":{"name":"…"}}`. Otherwise the downstream
/// `serde_json::from_slice::<Pod>` in `handlers::pod::create` fails with
/// `missing field 'name'`, matching the exact wire error from the canary.
#[test]
fn test_configmap_env_source_proto_decode_inlines_local_object_reference() {
    let registry = ProtoRegistry::new();
    let bytes = configmap_env_source_with_name("my-cm");
    let decoded = registry
        .decode_message("ConfigMapEnvSource", &bytes)
        .expect("ConfigMapEnvSource schema must be registered");

    assert_eq!(
        decoded.get("name").and_then(|v| v.as_str()),
        Some("my-cm"),
        "decoded JSON must inline LocalObjectReference.name at the top level; \
         got {decoded}",
    );
    assert!(
        decoded.get("localObjectReference").is_none(),
        "decoded JSON must NOT wrap fields in `localObjectReference`; got {decoded}",
    );
}

/// Same parity bug applies to `SecretEnvSource` — identical wire shape, same
/// upstream JSON inlining. Pin it here so a future schema edit doesn't fix
/// one and forget the other.
#[test]
fn test_secret_env_source_proto_decode_inlines_local_object_reference() {
    let registry = ProtoRegistry::new();
    let bytes = configmap_env_source_with_name("my-secret"); // same wire shape
    let decoded = registry
        .decode_message("SecretEnvSource", &bytes)
        .expect("SecretEnvSource schema must be registered");

    assert_eq!(
        decoded.get("name").and_then(|v| v.as_str()),
        Some("my-secret"),
        "decoded JSON must inline LocalObjectReference.name at the top level; \
         got {decoded}",
    );
    assert!(
        decoded.get("localObjectReference").is_none(),
        "decoded JSON must NOT wrap fields in `localObjectReference`; got {decoded}",
    );
}

/// End-to-end shape check: decode a `Container` whose `envFrom[0]` is a
/// `ConfigMapEnvSource` with no `prefix` set (the first entry in the
/// upstream conformance pod). The resulting JSON must round-trip through
/// `rusternetes_common::resources::Container` without losing `name`.
#[test]
fn test_container_with_envfrom_configmap_round_trips_through_pod_decoder() {
    use rusternetes_common::resources::Container;

    let registry = ProtoRegistry::new();
    // Container { name: "env-test", image: "busybox", envFrom: [ ConfigMapEnvSource{...} ] }
    // Proto field numbers per generated.proto:
    //   Container.name = 1, image = 2, envFrom = 19
    let cm_env = configmap_env_source_with_name("my-cm");
    let envfrom_entry = {
        // EnvFromSource.configMapRef = field 2, length-delimited
        let mut buf = Vec::new();
        buf.push((2 << 3) | 2);
        buf.push(cm_env.len() as u8);
        buf.extend_from_slice(&cm_env);
        buf
    };

    let mut container_bytes = Vec::new();
    // name = "env-test"
    container_bytes.push(0x0a); // field 1, length-delimited
    container_bytes.push(8);
    container_bytes.extend_from_slice(b"env-test");
    // image = "busybox"
    container_bytes.push(0x12); // field 2, length-delimited
    container_bytes.push(7);
    container_bytes.extend_from_slice(b"busybox");
    // envFrom = repeated, field 19 → tag = (19 << 3) | 2 = 154 (0x9a 0x01 as varint)
    container_bytes.push(0x9a);
    container_bytes.push(0x01);
    container_bytes.push(envfrom_entry.len() as u8);
    container_bytes.extend_from_slice(&envfrom_entry);

    let decoded = registry
        .decode_message("Container", &container_bytes)
        .expect("Container schema must be registered");

    // Re-serialize the decoded JSON value and feed it to the typed Container
    // deserializer — this is exactly what the pod handler does after the
    // protobuf middleware rewrites the body to JSON.
    let json_bytes = serde_json::to_vec(&decoded).unwrap();
    let container: Container = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!(
            "Container with envFrom must round-trip; decoder produced {decoded}; \
             serde error: {e}",
        )
    });

    let env_from = container.env_from.as_ref().expect("envFrom must be set");
    assert_eq!(env_from.len(), 1, "exactly one envFrom entry");
    let cm_ref = env_from[0]
        .config_map_ref
        .as_ref()
        .expect("first entry has configMapRef");
    assert_eq!(cm_ref.name, "my-cm", "configMapRef.name must round-trip");
}
