//! Proto-wire parity tests for `EnvVarSource` — the union under
//! `Container.env[*].valueFrom` in upstream core/v1.
//!
//! Upstream proto field numbers (per `staging/src/k8s.io/api/core/v1/generated.proto`):
//!
//! ```text
//! message EnvVarSource {
//!     optional ObjectFieldSelector    fieldRef         = 1;
//!     optional ResourceFieldSelector  resourceFieldRef = 2;
//!     optional ConfigMapKeySelector   configMapKeyRef  = 3;
//!     optional SecretKeySelector      secretKeyRef     = 4;
//! }
//! ```
//!
//! `EnvVarSource` is a "convention union": the wire format does not enforce
//! exactly-one-of, but every well-formed client (kubectl, client-go,
//! controller-runtime) sets exactly one branch. These tests pin that
//! convention for each of the four branches: build wire bytes containing
//! only one branch, decode through `ProtoRegistry::decode_message`, and
//! assert the resulting JSON has the matching key populated and the other
//! three absent.
//!
//! `ConfigMapKeySelector` / `SecretKeySelector` both embed
//! `LocalObjectReference` at proto field 1 with `json:",inline"` on the Go
//! side, which our schema models as `FieldType::InlineMessage`. The decoded
//! JSON therefore reads `{"name": "...", "key": "...", ...}`, not
//! `{"localObjectReference": {"name": "..."}, ...}` — same parity bug
//! pinned by `conformance_configmap_envfrom_prefixes_test.rs` for the
//! `EnvFromSource` siblings.
//!
//! The end-to-end test wraps `EnvVarSource` inside a `Container` with
//! `env[0].valueFrom = {configMapKeyRef: {...}}` and round-trips it through
//! the typed `rusternetes_common::resources::Container` deserializer —
//! mirroring exactly what `handlers::pod::create` does after the
//! protobuf→JSON middleware.

use rusternetes_api_server::protobuf::ProtoRegistry;

// --- low-level wire helpers ---------------------------------------------

/// Encode a protobuf length-delimited tag (wire type 2) for `field_number`,
/// followed by the varint length of `payload`, then the payload bytes.
///
/// Panics if `field_number` does not fit in a single-byte varint tag or
/// `payload.len()` does not fit in a single-byte varint length — both of
/// which hold for every payload used by these tests (small, short field
/// numbers ≤ 15).
fn push_length_delimited(buf: &mut Vec<u8>, field_number: u32, payload: &[u8]) {
    assert!(
        field_number < 16,
        "test helper only encodes short field tags"
    );
    assert!(
        payload.len() < 128,
        "test helper only encodes short payloads"
    );
    buf.push(((field_number << 3) | 2) as u8);
    buf.push(payload.len() as u8);
    buf.extend_from_slice(payload);
}

/// `LocalObjectReference { name: <name> }` on the wire — proto field 1,
/// string.
fn local_object_reference(name: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    push_length_delimited(&mut buf, 1, name.as_bytes());
    buf
}

/// `ObjectFieldSelector { apiVersion, fieldPath }` on the wire.
/// Field numbers per upstream proto: apiVersion=1, fieldPath=2.
fn object_field_selector(api_version: &str, field_path: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    push_length_delimited(&mut buf, 1, api_version.as_bytes());
    push_length_delimited(&mut buf, 2, field_path.as_bytes());
    buf
}

/// `ResourceFieldSelector { containerName, resource, divisor }` on the
/// wire. Field numbers: containerName=1, resource=2, divisor=3 (Quantity).
/// The Quantity message has `string=1` as its canonical text form (see
/// `decode_quantity` in `crates/api-server/src/protobuf.rs`).
fn resource_field_selector(container_name: &str, resource: &str, divisor: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    push_length_delimited(&mut buf, 1, container_name.as_bytes());
    push_length_delimited(&mut buf, 2, resource.as_bytes());
    let mut quantity = Vec::new();
    push_length_delimited(&mut quantity, 1, divisor.as_bytes());
    push_length_delimited(&mut buf, 3, &quantity);
    buf
}

/// `ConfigMapKeySelector` / `SecretKeySelector` share the same wire shape:
///   field 1 = LocalObjectReference (inline message)
///   field 2 = key (string)
///   field 3 = optional (bool, varint)
fn key_selector(name: &str, key: &str, optional: Option<bool>) -> Vec<u8> {
    let mut buf = Vec::new();
    let lor = local_object_reference(name);
    push_length_delimited(&mut buf, 1, &lor);
    push_length_delimited(&mut buf, 2, key.as_bytes());
    if let Some(opt) = optional {
        // field 3, wire type 0 (varint)
        buf.push(3 << 3);
        buf.push(if opt { 1 } else { 0 });
    }
    buf
}

/// Wrap a child message as a single branch of `EnvVarSource` at the given
/// field number.
fn env_var_source_branch(field_number: u32, child: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    push_length_delimited(&mut buf, field_number, child);
    buf
}

// --- branch tests --------------------------------------------------------

/// Asserts that exactly one of the four `EnvVarSource` branches is present
/// in the decoded JSON value.
fn assert_only_branch(decoded: &serde_json::Value, expected: &str) {
    let all_branches = [
        "fieldRef",
        "resourceFieldRef",
        "configMapKeyRef",
        "secretKeyRef",
    ];
    assert!(
        decoded.get(expected).is_some(),
        "expected branch {expected} to be present in {decoded}",
    );
    for branch in all_branches {
        if branch == expected {
            continue;
        }
        assert!(
            decoded.get(branch).is_none(),
            "branch {branch} must be absent when only {expected} was on the wire; got {decoded}",
        );
    }
}

#[test]
fn test_env_var_source_field_ref_branch_only() {
    let registry = ProtoRegistry::new();
    let child = object_field_selector("v1", "metadata.name");
    let bytes = env_var_source_branch(1, &child);

    let decoded = registry
        .decode_message("EnvVarSource", &bytes)
        .expect("EnvVarSource schema must be registered");

    assert_only_branch(&decoded, "fieldRef");
    let field_ref = decoded.get("fieldRef").unwrap();
    assert_eq!(
        field_ref.get("apiVersion").and_then(|v| v.as_str()),
        Some("v1"),
        "fieldRef.apiVersion must decode; got {decoded}",
    );
    assert_eq!(
        field_ref.get("fieldPath").and_then(|v| v.as_str()),
        Some("metadata.name"),
        "fieldRef.fieldPath must decode; got {decoded}",
    );
}

#[test]
fn test_env_var_source_resource_field_ref_branch_only() {
    let registry = ProtoRegistry::new();
    let child = resource_field_selector("app", "limits.cpu", "1m");
    let bytes = env_var_source_branch(2, &child);

    let decoded = registry
        .decode_message("EnvVarSource", &bytes)
        .expect("EnvVarSource schema must be registered");

    assert_only_branch(&decoded, "resourceFieldRef");
    let rref = decoded.get("resourceFieldRef").unwrap();
    assert_eq!(
        rref.get("containerName").and_then(|v| v.as_str()),
        Some("app"),
        "resourceFieldRef.containerName must decode; got {decoded}",
    );
    assert_eq!(
        rref.get("resource").and_then(|v| v.as_str()),
        Some("limits.cpu"),
        "resourceFieldRef.resource must decode; got {decoded}",
    );
    assert_eq!(
        rref.get("divisor").and_then(|v| v.as_str()),
        Some("1m"),
        "resourceFieldRef.divisor must decode as its canonical string form; got {decoded}",
    );
}

#[test]
fn test_env_var_source_config_map_key_ref_branch_only() {
    let registry = ProtoRegistry::new();
    let child = key_selector("my-cm", "MY_KEY", Some(true));
    let bytes = env_var_source_branch(3, &child);

    let decoded = registry
        .decode_message("EnvVarSource", &bytes)
        .expect("EnvVarSource schema must be registered");

    assert_only_branch(&decoded, "configMapKeyRef");
    let cm_ref = decoded.get("configMapKeyRef").unwrap();
    // LocalObjectReference is `json:",inline"` upstream — `name` must
    // surface at the top of the selector, not nested under
    // `localObjectReference`. This is the same parity bug pinned by
    // `conformance_configmap_envfrom_prefixes_test.rs` for `EnvFromSource`.
    assert_eq!(
        cm_ref.get("name").and_then(|v| v.as_str()),
        Some("my-cm"),
        "configMapKeyRef.name must inline LocalObjectReference.name; got {decoded}",
    );
    assert!(
        cm_ref.get("localObjectReference").is_none(),
        "configMapKeyRef must NOT wrap name under `localObjectReference`; got {decoded}",
    );
    assert_eq!(
        cm_ref.get("key").and_then(|v| v.as_str()),
        Some("MY_KEY"),
        "configMapKeyRef.key must decode; got {decoded}",
    );
    assert_eq!(
        cm_ref.get("optional").and_then(|v| v.as_bool()),
        Some(true),
        "configMapKeyRef.optional must decode; got {decoded}",
    );
}

#[test]
fn test_env_var_source_secret_key_ref_branch_only() {
    let registry = ProtoRegistry::new();
    let child = key_selector("my-secret", "password", Some(false));
    let bytes = env_var_source_branch(4, &child);

    let decoded = registry
        .decode_message("EnvVarSource", &bytes)
        .expect("EnvVarSource schema must be registered");

    assert_only_branch(&decoded, "secretKeyRef");
    let sec_ref = decoded.get("secretKeyRef").unwrap();
    assert_eq!(
        sec_ref.get("name").and_then(|v| v.as_str()),
        Some("my-secret"),
        "secretKeyRef.name must inline LocalObjectReference.name; got {decoded}",
    );
    assert!(
        sec_ref.get("localObjectReference").is_none(),
        "secretKeyRef must NOT wrap name under `localObjectReference`; got {decoded}",
    );
    assert_eq!(
        sec_ref.get("key").and_then(|v| v.as_str()),
        Some("password"),
        "secretKeyRef.key must decode; got {decoded}",
    );
    assert_eq!(
        sec_ref.get("optional").and_then(|v| v.as_bool()),
        Some(false),
        "secretKeyRef.optional=false must decode (not skipped); got {decoded}",
    );
}

/// End-to-end: a full `Container` with one `env` entry whose `valueFrom`
/// carries a `configMapKeyRef`. The bytes are exactly what client-go would
/// send for `kubectl set env --from=configmap/foo` over the protobuf
/// content type. The decoded JSON must round-trip through the typed
/// `rusternetes_common::resources::Container` deserializer used by the pod
/// handler — anything less would surface as the same `missing field 'name'`
/// canary failure documented in the EnvFromSource sibling test.
#[test]
fn test_container_env_value_from_configmap_round_trips_through_pod_decoder() {
    use rusternetes_common::resources::Container;

    let registry = ProtoRegistry::new();

    // EnvVarSource { configMapKeyRef: { name: "my-cm", key: "MY_KEY" } }
    let cm_selector = key_selector("my-cm", "MY_KEY", None);
    let value_from = env_var_source_branch(3, &cm_selector);

    // EnvVar { name: "GREETING", valueFrom: <above> }
    // Proto fields: name=1 (string), value=2 (string, unset), valueFrom=3 (msg)
    let mut env_var = Vec::new();
    push_length_delimited(&mut env_var, 1, b"GREETING");
    push_length_delimited(&mut env_var, 3, &value_from);

    // Container { name: "env-test", image: "busybox", env: [<above>] }
    // Proto fields: name=1, image=2, env=7 (repeated EnvVar).
    let mut container_bytes = Vec::new();
    push_length_delimited(&mut container_bytes, 1, b"env-test");
    push_length_delimited(&mut container_bytes, 2, b"busybox");
    push_length_delimited(&mut container_bytes, 7, &env_var);

    let decoded = registry
        .decode_message("Container", &container_bytes)
        .expect("Container schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).unwrap();
    let container: Container = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!(
            "Container with env[*].valueFrom.configMapKeyRef must round-trip; \
             decoder produced {decoded}; serde error: {e}",
        )
    });

    let env = container.env.as_ref().expect("env must be set");
    assert_eq!(env.len(), 1, "exactly one env entry");
    assert_eq!(env[0].name, "GREETING");
    let value_from = env[0]
        .value_from
        .as_ref()
        .expect("env[0].valueFrom must be set");
    let cm_key_ref = value_from
        .config_map_key_ref
        .as_ref()
        .expect("env[0].valueFrom.configMapKeyRef must be set");
    assert_eq!(
        cm_key_ref.name, "my-cm",
        "configMapKeyRef.name must round-trip",
    );
    assert_eq!(
        cm_key_ref.key, "MY_KEY",
        "configMapKeyRef.key must round-trip",
    );
    // All other branches must be absent.
    assert!(
        value_from.secret_key_ref.is_none(),
        "secretKeyRef must be absent",
    );
    assert!(value_from.field_ref.is_none(), "fieldRef must be absent");
    assert!(
        value_from.resource_field_ref.is_none(),
        "resourceFieldRef must be absent",
    );
}
