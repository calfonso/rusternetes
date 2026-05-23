//! Sweep regression: every upstream Go `json:",inline"` site whose embedded
//! type also carries a `protobuf:` field number — the case where the proto
//! wire format keeps the inner struct as a NESTED message at its declared
//! field, while the JSON wire FLATTENS the inner fields into the parent.
//!
//! PR #722 / commit `dde55a8e` fixed this specific asymmetry for the two
//! `LocalObjectReference`-bearing env sources (`ConfigMapEnvSource` /
//! `SecretEnvSource`). This file generalises that regression test across the
//! whole core/v1 surface so a future schema edit can't quietly slip back to
//! `FieldType::Message(...)` (which would re-introduce the
//! `missing field 'name'` deserialise failure the canary first surfaced).
//!
//! Inline sites covered (all derived from
//! `staging/src/k8s.io/api/core/v1/types.go` on `release-1.35`, grepping for
//! `` `json:",inline"` `` and excluding the `TypeMeta` flavour, which has
//! `protobuf:"-"` and therefore no wire field at all):
//!
//! | Parent struct                | Embedded type            | Proto field |
//! |------------------------------|--------------------------|------------:|
//! | `Volume`                     | `VolumeSource`           | 2 |
//! | `PersistentVolumeSpec`       | `PersistentVolumeSource` | 2 |
//! | `Probe`                      | `ProbeHandler`           | 1 |
//! | `EphemeralContainer`         | `EphemeralContainerCommon` | 1 |
//! | `SecretProjection`           | `LocalObjectReference`   | 1 |
//! | `ConfigMapVolumeSource`      | `LocalObjectReference`   | 1 |
//! | `ConfigMapProjection`        | `LocalObjectReference`   | 1 |
//! | `ConfigMapKeySelector`       | `LocalObjectReference`   | 1 |
//! | `SecretKeySelector`          | `LocalObjectReference`   | 1 |
//! | `ConfigMapEnvSource`         | `LocalObjectReference`   | 1 |
//! | `SecretEnvSource`            | `LocalObjectReference`   | 1 |
//!
//! For each row we craft minimal protobuf wire bytes that set ONE primitive
//! key inside the embedded message, decode through
//! [`ProtoRegistry::decode_message`], and assert that:
//!
//!   1. the flattened key (`name`, `path`, `httpGet`, …) lives at the
//!      parent JSON level; and
//!   2. the declared embed name (`localObjectReference`, `handler`,
//!      `volumeSource`, …) does NOT appear as a wrapper object.
//!
//! Sites where the Rust struct ALREADY matches the inline shape (e.g. the
//! seven `LocalObjectReference` embeds fixed in earlier PRs) are pinned
//! here too — losing inline on any of them would break read-back of
//! configmap-/secret-projected pods.

use rusternetes_api_server::protobuf::ProtoRegistry;

// ---------- tiny wire-bytes helpers -------------------------------------

/// Encode a varint into the buffer.
fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Append a length-delimited (wire type 2) field with the given proto field
/// number and payload bytes.
fn put_len_delimited(buf: &mut Vec<u8>, field_number: u32, payload: &[u8]) {
    let tag = (field_number << 3) | 2;
    put_varint(buf, tag as u64);
    put_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

/// Build a `LocalObjectReference { name: <name> }` payload (field 1 = name).
fn local_object_reference(name: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    put_len_delimited(&mut buf, 1, name.as_bytes());
    buf
}

/// Wrap an inner payload as the single field N of an outer message.
fn wrap(field_number: u32, inner: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_len_delimited(&mut buf, field_number, inner);
    buf
}

// ---------- generic assert helper ---------------------------------------

/// Decode `outer_type` from `bytes` and assert that:
///   - `flat_key` appears at the parent JSON level with the expected
///     primitive value (None ⇒ assert presence only); and
///   - `wrapper_key` does NOT appear at the parent level (no Go `,inline`
///     embed name leaking through).
fn assert_inline_decode(
    outer_type: &str,
    bytes: &[u8],
    flat_key: &str,
    wrapper_key: &str,
    expected_value: Option<&str>,
) {
    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message(outer_type, bytes)
        .unwrap_or_else(|| {
            panic!("{outer_type} schema must be registered in ProtoRegistry::new()")
        });

    if let Some(expected) = expected_value {
        let got = decoded.get(flat_key).and_then(|v| v.as_str());
        assert_eq!(
            got,
            Some(expected),
            "{outer_type}: flattened key `{flat_key}` must surface at the top of \
             the decoded object (Go `json:\",inline\"` semantics). \
             expected={expected:?}; decoded={decoded}",
        );
    } else {
        assert!(
            decoded.get(flat_key).is_some(),
            "{outer_type}: flattened key `{flat_key}` must surface at the top of \
             the decoded object (Go `json:\",inline\"` semantics). \
             decoded={decoded}",
        );
    }

    assert!(
        decoded.get(wrapper_key).is_none(),
        "{outer_type}: must NOT wrap inlined fields under `{wrapper_key}` (Go uses \
         `json:\",inline\"` on the embed). decoded={decoded}",
    );
}

// ---------- LocalObjectReference embeds (already inlined, pin them) -----

/// All seven core/v1 messages that embed `LocalObjectReference` at field 1
/// with `json:",inline"`. The Rust types expose `name` directly; the
/// decoded JSON must reflect that.
const LOCAL_OBJECT_REFERENCE_INLINE_SITES: &[&str] = &[
    "ConfigMapEnvSource",
    "SecretEnvSource",
    "ConfigMapKeySelector",
    "SecretKeySelector",
    "ConfigMapVolumeSource",
    "ConfigMapProjection",
    "SecretProjection",
];

#[test]
fn test_local_object_reference_inline_across_all_core_v1_sites() {
    // Same wire shape for every site: field 1 = LocalObjectReference{name="x"}.
    let inner = local_object_reference("pinned-name");
    let bytes = wrap(1, &inner);

    for outer_type in LOCAL_OBJECT_REFERENCE_INLINE_SITES {
        assert_inline_decode(
            outer_type,
            &bytes,
            "name",
            "localObjectReference",
            Some("pinned-name"),
        );
    }
}

// ---------- Volume → VolumeSource (already inlined, pin it) -------------

/// `Volume { name, volumeSource{ hostPath{ path } } }`. Upstream Go inlines
/// `VolumeSource` into `Volume`, so the decoded JSON must read
/// `{"name":"...","hostPath":{"path":"..."}}`, never
/// `{"volumeSource":{...}}`.
///
/// Field numbers (core/v1.proto, release-1.35):
///   - Volume.name = 1, Volume.volumeSource = 2
///   - VolumeSource.hostPath = 1
///   - HostPathVolumeSource.path = 1
#[test]
fn test_volume_inlines_volume_source() {
    let host_path_payload = {
        let mut buf = Vec::new();
        put_len_delimited(&mut buf, 1, b"/tmp/x");
        buf
    };
    let volume_source_payload = wrap(1, &host_path_payload); // VolumeSource.hostPath = 1
    let mut volume_bytes = Vec::new();
    put_len_delimited(&mut volume_bytes, 1, b"my-vol"); // Volume.name
    put_len_delimited(&mut volume_bytes, 2, &volume_source_payload); // Volume.volumeSource

    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("Volume", &volume_bytes)
        .expect("Volume schema must be registered");

    assert_eq!(
        decoded.get("name").and_then(|v| v.as_str()),
        Some("my-vol"),
        "Volume.name must round-trip; decoded={decoded}",
    );
    let host_path = decoded
        .get("hostPath")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("Volume must inline VolumeSource.hostPath; decoded={decoded}"));
    assert_eq!(
        host_path.get("path").and_then(|v| v.as_str()),
        Some("/tmp/x"),
        "hostPath.path must round-trip; decoded={decoded}",
    );
    assert!(
        decoded.get("volumeSource").is_none(),
        "Volume must NOT wrap VolumeSource fields under `volumeSource`; decoded={decoded}",
    );
}

// ---------- PersistentVolumeSpec → PersistentVolumeSource ---------------

/// `PersistentVolumeSpec { persistentVolumeSource{ hostPath{ path } } }`.
/// Upstream Go inlines `PersistentVolumeSource` into the spec, so the
/// decoded JSON must surface `hostPath` at the spec level — matching the
/// flat Rust `PersistentVolumeSpec` struct
/// (`crates/common/src/resources/volume.rs`). Without this, the typed
/// deserialiser fails to find `hostPath` (and every other PV source flavour)
/// during proto→JSON round-trips.
///
/// Field numbers (core/v1.proto, release-1.35):
///   - PersistentVolumeSpec.persistentVolumeSource = 2
///   - PersistentVolumeSource.hostPath = 3
///   - HostPathVolumeSource.path = 1
#[test]
fn test_persistent_volume_spec_inlines_persistent_volume_source() {
    let host_path_payload = {
        let mut buf = Vec::new();
        put_len_delimited(&mut buf, 1, b"/mnt/data");
        buf
    };
    let pv_source_payload = wrap(3, &host_path_payload); // PersistentVolumeSource.hostPath = 3
    let pv_spec_bytes = wrap(2, &pv_source_payload); // PersistentVolumeSpec.persistentVolumeSource = 2

    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("PersistentVolumeSpec", &pv_spec_bytes)
        .expect("PersistentVolumeSpec schema must be registered");

    let host_path = decoded
        .get("hostPath")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| {
            panic!(
                "PersistentVolumeSpec must inline PersistentVolumeSource.hostPath; \
             decoded={decoded}",
            )
        });
    assert_eq!(
        host_path.get("path").and_then(|v| v.as_str()),
        Some("/mnt/data"),
        "hostPath.path must round-trip; decoded={decoded}",
    );
    assert!(
        decoded.get("persistentVolumeSource").is_none(),
        "PersistentVolumeSpec must NOT wrap source fields under `persistentVolumeSource`; \
         decoded={decoded}",
    );
}

// ---------- Probe → ProbeHandler ----------------------------------------

/// `Probe { handler{ tcpSocket{ host } }, initialDelaySeconds }`. Upstream
/// Go inlines `ProbeHandler` into `Probe`, so the decoded JSON must show
/// `tcpSocket` (or any other action variant) at the Probe level. Our Rust
/// `Probe` (`crates/common/src/resources/pod.rs`) exposes `http_get`,
/// `tcp_socket`, `exec`, `grpc` as direct fields — no `handler` wrapper.
///
/// Field numbers (core/v1.proto, release-1.35):
///   - Probe.handler = 1, Probe.initialDelaySeconds = 2
///   - ProbeHandler.tcpSocket = 3
///   - TCPSocketAction.host = 2 (string)
#[test]
fn test_probe_inlines_probe_handler() {
    let tcp_socket_payload = {
        let mut buf = Vec::new();
        // TCPSocketAction.host (field 2, string)
        put_len_delimited(&mut buf, 2, b"127.0.0.1");
        buf
    };
    let handler_payload = wrap(3, &tcp_socket_payload); // ProbeHandler.tcpSocket = 3
    let mut probe_bytes = Vec::new();
    put_len_delimited(&mut probe_bytes, 1, &handler_payload); // Probe.handler = 1
                                                              // initialDelaySeconds = 2 (varint), tag = (2 << 3) | 0 = 0x10
    probe_bytes.push(0x10);
    probe_bytes.push(0x05); // value 5

    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("Probe", &probe_bytes)
        .expect("Probe schema must be registered");

    let tcp_socket = decoded
        .get("tcpSocket")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("Probe must inline ProbeHandler.tcpSocket; decoded={decoded}"));
    assert_eq!(
        tcp_socket.get("host").and_then(|v| v.as_str()),
        Some("127.0.0.1"),
        "tcpSocket.host must round-trip; decoded={decoded}",
    );
    assert_eq!(
        decoded.get("initialDelaySeconds").and_then(|v| v.as_i64()),
        Some(5),
        "Probe.initialDelaySeconds (sibling of inlined handler) must round-trip; \
         decoded={decoded}",
    );
    assert!(
        decoded.get("handler").is_none(),
        "Probe must NOT wrap action fields under `handler`; decoded={decoded}",
    );
}

// ---------- EphemeralContainer → EphemeralContainerCommon ---------------

/// `EphemeralContainer { ephemeralContainerCommon{ name, image },
/// targetContainerName }`. Upstream Go inlines `EphemeralContainerCommon`
/// into `EphemeralContainer`, so the decoded JSON must look like
/// `{"name":"...","image":"...","targetContainerName":"..."}`. Without
/// this, `kubectl debug` round-trips through proto would surface
/// `{"ephemeralContainerCommon":{"name":...}}` and the typed
/// `EphemeralContainer` deserialiser would reject the body.
///
/// Field numbers (core/v1.proto, release-1.35):
///   - EphemeralContainer.ephemeralContainerCommon = 1
///   - EphemeralContainer.targetContainerName = 2
///   - EphemeralContainerCommon.name = 1, .image = 2
#[test]
fn test_ephemeral_container_inlines_common() {
    let common_payload = {
        let mut buf = Vec::new();
        put_len_delimited(&mut buf, 1, b"debugger"); // .name
        put_len_delimited(&mut buf, 2, b"busybox"); // .image
        buf
    };
    let mut ephemeral_bytes = Vec::new();
    put_len_delimited(&mut ephemeral_bytes, 1, &common_payload); // .ephemeralContainerCommon
    put_len_delimited(&mut ephemeral_bytes, 2, b"app"); // .targetContainerName

    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("EphemeralContainer", &ephemeral_bytes)
        .expect("EphemeralContainer schema must be registered");

    assert_eq!(
        decoded.get("name").and_then(|v| v.as_str()),
        Some("debugger"),
        "EphemeralContainerCommon.name must surface at the EphemeralContainer level; \
         decoded={decoded}",
    );
    assert_eq!(
        decoded.get("image").and_then(|v| v.as_str()),
        Some("busybox"),
        "EphemeralContainerCommon.image must surface at the EphemeralContainer level; \
         decoded={decoded}",
    );
    assert_eq!(
        decoded.get("targetContainerName").and_then(|v| v.as_str()),
        Some("app"),
        "EphemeralContainer.targetContainerName (sibling of inlined common) must \
         round-trip; decoded={decoded}",
    );
    assert!(
        decoded.get("ephemeralContainerCommon").is_none(),
        "EphemeralContainer must NOT wrap common fields under \
         `ephemeralContainerCommon`; decoded={decoded}",
    );
}

// ---------- Round-trip through the typed deserialiser -------------------

/// End-to-end shape check: a `Probe` decoded from proto must feed straight
/// into `rusternetes_common::resources::Probe`. This mirrors the
/// `Container with envFrom` round-trip in
/// `conformance_configmap_envfrom_prefixes_test.rs`.
#[test]
fn test_probe_proto_decode_round_trips_through_typed_deserializer() {
    use rusternetes_common::resources::Probe;

    let exec_payload = {
        // ExecAction.command (repeated string, field 1) — two entries.
        let mut buf = Vec::new();
        put_len_delimited(&mut buf, 1, b"sh");
        put_len_delimited(&mut buf, 1, b"-c");
        buf
    };
    let handler_payload = wrap(1, &exec_payload); // ProbeHandler.exec = 1
    let mut probe_bytes = Vec::new();
    put_len_delimited(&mut probe_bytes, 1, &handler_payload); // Probe.handler

    let registry = ProtoRegistry::new();
    let decoded = registry
        .decode_message("Probe", &probe_bytes)
        .expect("Probe schema must be registered");

    let json = serde_json::to_vec(&decoded).expect("re-serialise decoded JSON");
    let probe: Probe = serde_json::from_slice(&json).unwrap_or_else(|e| {
        panic!(
            "Probe must round-trip through the typed deserialiser without wrapping \
             the action under `handler`; decoded={decoded}; serde err={e}",
        )
    });

    let exec = probe.exec.expect("exec must be present");
    assert_eq!(exec.command, vec!["sh".to_string(), "-c".to_string()]);
}
