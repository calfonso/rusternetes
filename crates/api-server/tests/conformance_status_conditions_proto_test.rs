//! Wire-format parity tests for repeated `Condition` arrays embedded in
//! resource statuses. Each `*Condition` message is encoded by the typed
//! Kubernetes client as a sequence of length-delimited submessages under the
//! parent status's `conditions` field (proto field 2 for PodStatus, field 6
//! for DeploymentStatus, etc.). The upstream contract — exercised by
//! conformance tests like `[sig-node] Pods should run through the lifecycle
//! of Pods and PodStatus` and `[sig-apps] Deployment should run the lifecycle
//! of a Deployment` — is:
//!
//! 1. Repeated `conditions` entries decode in wire order (protobuf preserves
//!    repetition order on the wire).
//! 2. `metav1.Time` fields (`lastTransitionTime`, `lastProbeTime`,
//!    `lastUpdateTime`, `lastHeartbeatTime`) decode to RFC3339 *second*
//!    precision strings (`"2026-05-23T10:00:00Z"`), NOT `{seconds, nanos}`
//!    objects. This is distinct from `MicroTime` on `Event.eventTime` /
//!    `LeaseSpec.acquireTime`, which carries microsecond precision (see
//!    `conformance_events_microtime_test.rs`).
//! 3. Optional string fields (`reason`, `message`) are absent from the
//!    decoded JSON when they were not present on the wire — neither emitted
//!    as `""` nor as `null`. That matters because downstream typed structs
//!    use `#[serde(skip_serializing_if = "Option::is_none")]` and a
//!    spuriously-emitted empty string would round-trip differently than the
//!    upstream Go client expects.
//!
//! The four condition shapes covered here all share the `type`/`status`/
//! `reason`/`message` quartet plus 1-2 `Time` fields, but they live under
//! different parent messages with different proto field numbers — so a
//! schema edit that fixes one and forgets another would silently regress
//! conformance for that family. Pinning each shape separately catches that
//! class of drift.
//!
//! Note: `JobStatus` and `NodeStatus` parent schemas are currently empty in
//! the registry (they decode as `{}`), so we test `JobCondition` and
//! `NodeCondition` standalone. Once those parent statuses gain their
//! `conditions` fields, the tests should be extended to round-trip through
//! the parent — same pattern as the `PodStatus` / `DeploymentStatus` cases
//! below.

use rusternetes_api_server::protobuf::ProtoRegistry;
use serde_json::Value;

/// Wire bytes for `Time { seconds: <s>, nanos: <n> }`. Both fields are
/// proto-encoded as varints under tags 0x08 (field 1) and 0x10 (field 2).
/// The decoder only consults `seconds` when formatting (see
/// `decode_timestamp` in `protobuf.rs`), so `nanos` here is just for shape
/// fidelity with what the typed client emits.
fn time_bytes(seconds: u64, nanos: u32) -> Vec<u8> {
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

/// Encode a length-delimited field: tag byte followed by length-varint then
/// payload. All `conditions` entries and all `Time` submessages use this
/// shape (wire type 2 = LEN). Returns the appended length so we can keep the
/// byte budget visible for each entry.
fn push_len_delim(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    out.push(tag);
    // All payloads in this test are <128 bytes, so length is a single varint.
    assert!(
        payload.len() < 128,
        "test payloads stay below 1-byte varint"
    );
    out.push(payload.len() as u8);
    out.extend_from_slice(payload);
}

/// Encode a length-delimited string field. Used for `type`, `status`,
/// `reason`, `message`.
fn push_string(out: &mut Vec<u8>, field_num: u32, value: &str) {
    let tag = ((field_num << 3) | 2) as u8;
    push_len_delim(out, tag, value.as_bytes());
}

// ============================================================================
// PodStatus.conditions[*]  (PodCondition)
// ============================================================================

/// Build a single `PodCondition` payload. Field numbers per
/// `k8s.io/api/core/v1/generated.proto`:
///   1 = type           (string)
///   2 = status         (string)
///   3 = lastProbeTime  (Time)
///   4 = lastTransitionTime (Time)
///   5 = reason         (string)
///   6 = message        (string)
fn pod_condition_bytes(
    ty: &str,
    status: &str,
    last_probe: Option<(u64, u32)>,
    last_transition: Option<(u64, u32)>,
    reason: Option<&str>,
    message: Option<&str>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    push_string(&mut buf, 1, ty);
    push_string(&mut buf, 2, status);
    if let Some((s, n)) = last_probe {
        let t = time_bytes(s, n);
        push_len_delim(&mut buf, (3 << 3) | 2, &t);
    }
    if let Some((s, n)) = last_transition {
        let t = time_bytes(s, n);
        push_len_delim(&mut buf, (4 << 3) | 2, &t);
    }
    if let Some(r) = reason {
        push_string(&mut buf, 5, r);
    }
    if let Some(m) = message {
        push_string(&mut buf, 6, m);
    }
    buf
}

/// Three PodConditions on the wire — Initialized/Ready/ContainersReady — must
/// decode in the same order they were emitted. This is what the upstream
/// pod lifecycle conformance test expects when it reads back a Pod after
/// patching `status.conditions`.
#[test]
fn test_pod_status_conditions_decode_in_wire_order_with_rfc3339_times() {
    let registry = ProtoRegistry::new();

    // Three conditions, distinct lastTransitionTime values so we can pin
    // ordering by inspecting timestamps as well as `type`.
    let c1 = pod_condition_bytes(
        "Initialized",
        "True",
        None,
        Some((1_779_530_400, 0)), // 2026-05-23T10:00:00Z
        None,
        None,
    );
    let c2 = pod_condition_bytes(
        "Ready",
        "True",
        None,
        Some((1_779_534_000, 0)), // 2026-05-23T11:00:00Z
        None,
        None,
    );
    let c3 = pod_condition_bytes(
        "ContainersReady",
        "True",
        None,
        Some((1_779_537_600, 0)), // 2026-05-23T12:00:00Z
        None,
        None,
    );

    // PodStatus.conditions is field 2 (length-delimited, repeated).
    let mut status_bytes = Vec::new();
    for c in [&c1, &c2, &c3] {
        push_len_delim(&mut status_bytes, (2 << 3) | 2, c);
    }

    let decoded = registry
        .decode_message("PodStatus", &status_bytes)
        .expect("PodStatus schema must be registered");

    let conds = decoded
        .get("conditions")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("conditions must decode as JSON array; got {decoded}"));
    assert_eq!(conds.len(), 3, "all three conditions must round-trip");

    // Order is preserved.
    let types: Vec<&str> = conds
        .iter()
        .map(|c| c.get("type").and_then(Value::as_str).unwrap_or(""))
        .collect();
    assert_eq!(
        types,
        vec!["Initialized", "Ready", "ContainersReady"],
        "proto repeated fields must decode in wire order",
    );

    // lastTransitionTime decodes to RFC3339 with second precision (NOT a
    // {seconds, nanos} object).
    let transitions: Vec<&str> = conds
        .iter()
        .map(|c| {
            c.get("lastTransitionTime")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("lastTransitionTime must be a JSON string in {c}"))
        })
        .collect();
    assert_eq!(
        transitions,
        vec![
            "2026-05-23T10:00:00Z",
            "2026-05-23T11:00:00Z",
            "2026-05-23T12:00:00Z",
        ],
        "Time fields must serialize as RFC3339 with second precision",
    );

    // None of these conditions set reason/message/lastProbeTime — those
    // optional keys must be absent (not "" or null), so downstream serde
    // can apply `skip_serializing_if = Option::is_none` cleanly.
    for c in conds {
        assert!(
            c.get("reason").is_none(),
            "unset reason must be absent, got {c}",
        );
        assert!(
            c.get("message").is_none(),
            "unset message must be absent, got {c}",
        );
        assert!(
            c.get("lastProbeTime").is_none(),
            "unset lastProbeTime must be absent, got {c}",
        );
    }
}

/// A PodCondition that DOES set `reason`, `message`, and `lastProbeTime`
/// must surface those fields verbatim. Pairs with the previous test:
/// together they cover both "field set" and "field unset" branches of the
/// decoder so a future change that always emits the keys (e.g. with empty
/// strings) gets caught.
#[test]
fn test_pod_condition_with_all_optional_fields_set_round_trips() {
    let registry = ProtoRegistry::new();

    let cond = pod_condition_bytes(
        "Ready",
        "False",
        Some((1_779_530_400, 0)), // 2026-05-23T10:00:00Z
        Some((1_779_534_000, 0)), // 2026-05-23T11:00:00Z
        Some("ContainersNotReady"),
        Some("containers with unready status: [agnhost]"),
    );

    let mut status_bytes = Vec::new();
    push_len_delim(&mut status_bytes, (2 << 3) | 2, &cond);

    let decoded = registry
        .decode_message("PodStatus", &status_bytes)
        .expect("PodStatus schema must be registered");
    let c = &decoded.get("conditions").and_then(Value::as_array).unwrap()[0];

    assert_eq!(c.get("type").and_then(Value::as_str), Some("Ready"));
    assert_eq!(c.get("status").and_then(Value::as_str), Some("False"));
    assert_eq!(
        c.get("lastProbeTime").and_then(Value::as_str),
        Some("2026-05-23T10:00:00Z"),
    );
    assert_eq!(
        c.get("lastTransitionTime").and_then(Value::as_str),
        Some("2026-05-23T11:00:00Z"),
    );
    assert_eq!(
        c.get("reason").and_then(Value::as_str),
        Some("ContainersNotReady"),
    );
    assert_eq!(
        c.get("message").and_then(Value::as_str),
        Some("containers with unready status: [agnhost]"),
    );
}

// ============================================================================
// DeploymentStatus.conditions[*]  (DeploymentCondition)
// ============================================================================

/// Build a `DeploymentCondition` payload. Field numbers per
/// `k8s.io/api/apps/v1/generated.proto` — note these differ from
/// PodCondition: DeploymentCondition has no `lastProbeTime`, and the two
/// Time fields live at field 6 (`lastUpdateTime`) and field 7
/// (`lastTransitionTime`). Reason/message also shift to fields 4/5.
///   1 = type
///   2 = status
///   4 = reason
///   5 = message
///   6 = lastUpdateTime    (Time)
///   7 = lastTransitionTime (Time)
fn deployment_condition_bytes(
    ty: &str,
    status: &str,
    reason: Option<&str>,
    message: Option<&str>,
    last_update: Option<(u64, u32)>,
    last_transition: Option<(u64, u32)>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    push_string(&mut buf, 1, ty);
    push_string(&mut buf, 2, status);
    if let Some(r) = reason {
        push_string(&mut buf, 4, r);
    }
    if let Some(m) = message {
        push_string(&mut buf, 5, m);
    }
    if let Some((s, n)) = last_update {
        let t = time_bytes(s, n);
        push_len_delim(&mut buf, (6 << 3) | 2, &t);
    }
    if let Some((s, n)) = last_transition {
        let t = time_bytes(s, n);
        push_len_delim(&mut buf, (7 << 3) | 2, &t);
    }
    buf
}

/// `DeploymentStatus.conditions` is proto field 6 (length-delimited,
/// repeated). Two DeploymentConditions on the wire — `Available` and
/// `Progressing` — must decode in order, with their `lastUpdateTime` /
/// `lastTransitionTime` rendered as RFC3339 second-precision strings.
#[test]
fn test_deployment_status_conditions_decode_in_wire_order_with_rfc3339_times() {
    let registry = ProtoRegistry::new();

    let c1 = deployment_condition_bytes(
        "Available",
        "True",
        Some("MinimumReplicasAvailable"),
        Some("Deployment has minimum availability."),
        Some((1_779_530_400, 0)), // lastUpdateTime     2026-05-23T10:00:00Z
        Some((1_779_534_000, 0)), // lastTransitionTime 2026-05-23T11:00:00Z
    );
    let c2 = deployment_condition_bytes(
        "Progressing",
        "True",
        Some("NewReplicaSetAvailable"),
        Some("ReplicaSet \"d-7\" has successfully progressed."),
        Some((1_779_537_600, 0)), // 2026-05-23T12:00:00Z
        Some((1_779_534_000, 0)), // 2026-05-23T11:00:00Z
    );

    // DeploymentStatus.conditions is field 6 → tag (6<<3)|2 = 0x32.
    let mut status_bytes = Vec::new();
    push_len_delim(&mut status_bytes, (6 << 3) | 2, &c1);
    push_len_delim(&mut status_bytes, (6 << 3) | 2, &c2);

    let decoded = registry
        .decode_message("DeploymentStatus", &status_bytes)
        .expect("DeploymentStatus schema must be registered");

    let conds = decoded
        .get("conditions")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("conditions must be array; got {decoded}"));
    assert_eq!(conds.len(), 2);

    let types: Vec<&str> = conds
        .iter()
        .map(|c| c.get("type").and_then(Value::as_str).unwrap_or(""))
        .collect();
    assert_eq!(
        types,
        vec!["Available", "Progressing"],
        "wire order preserved",
    );

    assert_eq!(
        conds[0].get("lastUpdateTime").and_then(Value::as_str),
        Some("2026-05-23T10:00:00Z"),
    );
    assert_eq!(
        conds[0].get("lastTransitionTime").and_then(Value::as_str),
        Some("2026-05-23T11:00:00Z"),
    );
    assert_eq!(
        conds[1].get("lastUpdateTime").and_then(Value::as_str),
        Some("2026-05-23T12:00:00Z"),
    );
    assert_eq!(
        conds[1].get("lastTransitionTime").and_then(Value::as_str),
        Some("2026-05-23T11:00:00Z"),
    );
    assert_eq!(
        conds[0].get("reason").and_then(Value::as_str),
        Some("MinimumReplicasAvailable"),
    );
    assert_eq!(
        conds[1].get("reason").and_then(Value::as_str),
        Some("NewReplicaSetAvailable"),
    );
}

/// Mirror of the PodCondition "unset optionals" check: a DeploymentCondition
/// with only `type` and `status` populated must NOT spuriously surface
/// `reason`, `message`, or either of the Time fields.
#[test]
fn test_deployment_condition_unset_optionals_are_absent() {
    let registry = ProtoRegistry::new();

    let c = deployment_condition_bytes("Available", "Unknown", None, None, None, None);

    let mut status_bytes = Vec::new();
    push_len_delim(&mut status_bytes, (6 << 3) | 2, &c);

    let decoded = registry
        .decode_message("DeploymentStatus", &status_bytes)
        .expect("DeploymentStatus must decode");
    let cond = &decoded.get("conditions").and_then(Value::as_array).unwrap()[0];

    assert_eq!(cond.get("type").and_then(Value::as_str), Some("Available"));
    assert_eq!(cond.get("status").and_then(Value::as_str), Some("Unknown"));
    for absent in ["reason", "message", "lastUpdateTime", "lastTransitionTime"] {
        assert!(
            cond.get(absent).is_none(),
            "unset {absent} must be absent from {cond}",
        );
    }
}

// ============================================================================
// JobCondition — JobStatus parent schema is empty in the registry, so test
// the JobCondition decoder standalone. Once `JobStatus.conditions` is
// registered, fold this into a parent round-trip like the PodStatus case.
// ============================================================================

/// Build a `JobCondition` payload. Field numbers per
/// `k8s.io/api/batch/v1/generated.proto`:
///   1 = type
///   2 = status
///   3 = lastProbeTime      (Time)
///   4 = lastTransitionTime (Time)
///   5 = reason
///   6 = message
fn job_condition_bytes(
    ty: &str,
    status: &str,
    last_probe: Option<(u64, u32)>,
    last_transition: Option<(u64, u32)>,
    reason: Option<&str>,
    message: Option<&str>,
) -> Vec<u8> {
    // Same field layout as PodCondition.
    pod_condition_bytes(ty, status, last_probe, last_transition, reason, message)
}

#[test]
fn test_job_condition_decodes_with_rfc3339_times() {
    let registry = ProtoRegistry::new();

    let bytes = job_condition_bytes(
        "Complete",
        "True",
        Some((1_779_530_400, 0)), // 2026-05-23T10:00:00Z
        Some((1_779_534_000, 0)), // 2026-05-23T11:00:00Z
        Some("CompletionsReached"),
        Some("Reached expected number of succeeded pods"),
    );

    let decoded = registry
        .decode_message("JobCondition", &bytes)
        .expect("JobCondition schema must be registered");

    assert_eq!(
        decoded.get("type").and_then(Value::as_str),
        Some("Complete")
    );
    assert_eq!(decoded.get("status").and_then(Value::as_str), Some("True"));
    assert_eq!(
        decoded.get("lastProbeTime").and_then(Value::as_str),
        Some("2026-05-23T10:00:00Z"),
    );
    assert_eq!(
        decoded.get("lastTransitionTime").and_then(Value::as_str),
        Some("2026-05-23T11:00:00Z"),
    );
    assert_eq!(
        decoded.get("reason").and_then(Value::as_str),
        Some("CompletionsReached"),
    );
    assert_eq!(
        decoded.get("message").and_then(Value::as_str),
        Some("Reached expected number of succeeded pods"),
    );
}

#[test]
fn test_job_condition_unset_optionals_are_absent() {
    let registry = ProtoRegistry::new();

    let bytes = job_condition_bytes("Failed", "True", None, None, None, None);

    let decoded = registry
        .decode_message("JobCondition", &bytes)
        .expect("JobCondition schema must be registered");

    assert_eq!(decoded.get("type").and_then(Value::as_str), Some("Failed"));
    assert_eq!(decoded.get("status").and_then(Value::as_str), Some("True"));
    for absent in ["lastProbeTime", "lastTransitionTime", "reason", "message"] {
        assert!(
            decoded.get(absent).is_none(),
            "unset {absent} must be absent from {decoded}",
        );
    }
}

// ============================================================================
// NodeCondition — NodeStatus parent schema is also empty; test standalone.
// NodeCondition has a `lastHeartbeatTime` rather than `lastProbeTime` and
// uses field 3 for the heartbeat — different field name shape from
// PodCondition / JobCondition, so the decode path is genuinely separate.
// ============================================================================

/// Build a `NodeCondition` payload. Field numbers per
/// `k8s.io/api/core/v1/generated.proto`:
///   1 = type
///   2 = status
///   3 = lastHeartbeatTime  (Time)
///   4 = lastTransitionTime (Time)
///   5 = reason
///   6 = message
fn node_condition_bytes(
    ty: &str,
    status: &str,
    last_heartbeat: Option<(u64, u32)>,
    last_transition: Option<(u64, u32)>,
    reason: Option<&str>,
    message: Option<&str>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    push_string(&mut buf, 1, ty);
    push_string(&mut buf, 2, status);
    if let Some((s, n)) = last_heartbeat {
        let t = time_bytes(s, n);
        push_len_delim(&mut buf, (3 << 3) | 2, &t);
    }
    if let Some((s, n)) = last_transition {
        let t = time_bytes(s, n);
        push_len_delim(&mut buf, (4 << 3) | 2, &t);
    }
    if let Some(r) = reason {
        push_string(&mut buf, 5, r);
    }
    if let Some(m) = message {
        push_string(&mut buf, 6, m);
    }
    buf
}

#[test]
fn test_node_condition_decodes_heartbeat_and_transition_as_rfc3339() {
    let registry = ProtoRegistry::new();

    let bytes = node_condition_bytes(
        "Ready",
        "True",
        Some((1_779_530_400, 0)), // 2026-05-23T10:00:00Z
        Some((1_779_534_000, 0)), // 2026-05-23T11:00:00Z
        Some("KubeletReady"),
        Some("kubelet is posting ready status"),
    );

    let decoded = registry
        .decode_message("NodeCondition", &bytes)
        .expect("NodeCondition schema must be registered");

    assert_eq!(decoded.get("type").and_then(Value::as_str), Some("Ready"));
    assert_eq!(decoded.get("status").and_then(Value::as_str), Some("True"));
    // NodeCondition has `lastHeartbeatTime`, distinct from PodCondition's
    // `lastProbeTime`. Both are Time (second precision), but the JSON key
    // shape must match the schema.
    assert_eq!(
        decoded.get("lastHeartbeatTime").and_then(Value::as_str),
        Some("2026-05-23T10:00:00Z"),
    );
    assert!(
        decoded.get("lastProbeTime").is_none(),
        "NodeCondition must NOT surface lastProbeTime; got {decoded}",
    );
    assert_eq!(
        decoded.get("lastTransitionTime").and_then(Value::as_str),
        Some("2026-05-23T11:00:00Z"),
    );
    assert_eq!(
        decoded.get("reason").and_then(Value::as_str),
        Some("KubeletReady"),
    );
    assert_eq!(
        decoded.get("message").and_then(Value::as_str),
        Some("kubelet is posting ready status"),
    );
}

#[test]
fn test_node_condition_unset_optionals_are_absent() {
    let registry = ProtoRegistry::new();

    let bytes = node_condition_bytes("MemoryPressure", "False", None, None, None, None);

    let decoded = registry
        .decode_message("NodeCondition", &bytes)
        .expect("NodeCondition schema must be registered");

    assert_eq!(
        decoded.get("type").and_then(Value::as_str),
        Some("MemoryPressure"),
    );
    assert_eq!(decoded.get("status").and_then(Value::as_str), Some("False"));
    for absent in [
        "lastHeartbeatTime",
        "lastTransitionTime",
        "reason",
        "message",
    ] {
        assert!(
            decoded.get(absent).is_none(),
            "unset {absent} must be absent from {decoded}",
        );
    }
}
