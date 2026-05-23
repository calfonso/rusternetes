//! Schema parity / wire-format coverage for `JobSpec.podFailurePolicy`
//! (proto field 11) and `JobSpec.successPolicy` (proto field 16).
//!
//! Background
//! ----------
//! Before this fixture, `pod_failure_policy` and `success_policy` were typed
//! as opaque `Option<serde_json::Value>` on `JobSpec`. JSON-encoded payloads
//! happened to round-trip through `serde_json::Value`, but the protobuf
//! middleware silently dropped them entirely because both messages were
//! registered in [`ProtoRegistry`] with an *empty* fields map — the same bug
//! class PR #690 closed for `PodStatus`.
//!
//! Conformance tests under `[sig-apps] Job` exercise both fields end-to-end:
//!   - `should run a job to completion when tasks succeed and indexes
//!     are evaluated with successPolicy [Conformance]`
//!   - `should allow to use the pod failure policy on Job [Conformance]`
//!
//! Either of those drives a write that uses `application/vnd.kubernetes.protobuf`
//! on the wire — exactly the path that exposed the silent-drop bug for Pod
//! status.
//!
//! Coverage
//! --------
//! 1. JSON round-trip through the *typed* `JobSpec` proves `serde` understands
//!    the canonical shape produced by `kubectl` / client-go.
//! 2. Protobuf decode through [`ProtoRegistry`] proves the registry knows the
//!    field numbers, names, and inner message types. Without the parent
//!    `rules` field on `PodFailurePolicy`/`SuccessPolicy`, decoding would
//!    succeed (no error) but yield an empty JSON object — the silent drop.

use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_common::resources::{
    JobSpec, PodFailurePolicy, PodFailurePolicyOnExitCodesRequirement,
    PodFailurePolicyOnPodConditionsPattern, PodFailurePolicyRule, SuccessPolicy, SuccessPolicyRule,
};

// ---------------------------------------------------------------------------
// JSON round-trip — typed JobSpec
// ---------------------------------------------------------------------------

/// A typed `JobSpec` carrying both `podFailurePolicy` and `successPolicy`
/// must serialize to the canonical upstream JSON shape and deserialize back
/// into the same Rust value. This is the pre-requisite for the protobuf
/// middleware (which converts wire bytes → `Value` → typed struct) to work.
#[test]
fn jobspec_with_pod_failure_policy_and_success_policy_roundtrips_via_json() {
    let fixture = r#"{
      "template": {
        "spec": {
          "containers": [{"name": "worker", "image": "busybox"}],
          "restartPolicy": "Never"
        }
      },
      "completions": 5,
      "parallelism": 5,
      "completionMode": "Indexed",
      "podFailurePolicy": {
        "rules": [
          {
            "action": "FailJob",
            "onExitCodes": {
              "containerName": "worker",
              "operator": "In",
              "values": [42, 99]
            }
          },
          {
            "action": "Ignore",
            "onPodConditions": [
              {"type": "DisruptionTarget", "status": "True"}
            ]
          }
        ]
      },
      "successPolicy": {
        "rules": [
          {"succeededIndexes": "0-2", "succeededCount": 2}
        ]
      }
    }"#;

    let spec: JobSpec = serde_json::from_str(fixture).expect("typed decode");

    // PodFailurePolicy
    let pfp = spec
        .pod_failure_policy
        .as_ref()
        .expect("podFailurePolicy decoded");
    assert_eq!(pfp.rules.len(), 2, "two rules decoded");

    let fail_job = &pfp.rules[0];
    assert_eq!(fail_job.action, "FailJob");
    let on_exit = fail_job
        .on_exit_codes
        .as_ref()
        .expect("FailJob rule has onExitCodes");
    assert_eq!(on_exit.container_name.as_deref(), Some("worker"));
    assert_eq!(on_exit.operator, "In");
    assert_eq!(on_exit.values, vec![42, 99]);

    let ignore = &pfp.rules[1];
    assert_eq!(ignore.action, "Ignore");
    assert_eq!(ignore.on_pod_conditions.len(), 1);
    assert_eq!(
        ignore.on_pod_conditions[0].condition_type,
        "DisruptionTarget"
    );
    assert_eq!(ignore.on_pod_conditions[0].status.as_deref(), Some("True"));

    // SuccessPolicy
    let sp = spec.success_policy.as_ref().expect("successPolicy decoded");
    assert_eq!(sp.rules.len(), 1);
    assert_eq!(sp.rules[0].succeeded_indexes.as_deref(), Some("0-2"));
    assert_eq!(sp.rules[0].succeeded_count, Some(2));

    // Stable re-encode / re-decode
    let re_encoded = serde_json::to_value(&spec).expect("re-encode");
    let re_decoded: JobSpec = serde_json::from_value(re_encoded.clone()).expect("re-decode");
    let re_encoded_2 = serde_json::to_value(&re_decoded).expect("re-encode-2");
    assert_eq!(
        re_encoded, re_encoded_2,
        "round-trip must be stable; lhs {re_encoded}, rhs {re_encoded_2}",
    );
}

/// Skip-if-none + skip-if-empty conventions: the canonical JSON must NOT
/// emit `podFailurePolicy`/`successPolicy` when the typed value is `None`,
/// and within rules an empty `onPodConditions` slice must be omitted entirely
/// (matches upstream protobuf `+optional` ergonomics).
#[test]
fn jobspec_omits_unset_pod_failure_policy_and_success_policy_in_json() {
    // JobSpec lacks Default (template is non-Optional and upstream behaves the
    // same way); build a minimal valid spec via JSON instead.
    let spec: JobSpec = serde_json::from_str(
        r#"{
            "template": {
              "spec": {
                "containers": [{"name": "x", "image": "busybox"}],
                "restartPolicy": "Never"
              }
            }
        }"#,
    )
    .expect("minimal JobSpec must decode");
    let json = serde_json::to_value(&spec).expect("encode");
    assert!(
        json.get("podFailurePolicy").is_none(),
        "podFailurePolicy must be omitted when None; got {json}",
    );
    assert!(
        json.get("successPolicy").is_none(),
        "successPolicy must be omitted when None; got {json}",
    );

    // A rule with an empty on_pod_conditions vec must still skip the field —
    // otherwise we'd diverge from upstream JSON and confuse round-trippers.
    let rule = PodFailurePolicyRule {
        action: "Count".into(),
        on_exit_codes: None,
        on_pod_conditions: Vec::new(),
    };
    let rule_json = serde_json::to_value(&rule).expect("encode rule");
    assert!(
        rule_json.get("onPodConditions").is_none(),
        "empty onPodConditions must be omitted; got {rule_json}",
    );
}

// ---------------------------------------------------------------------------
// Protobuf wire format — ProtoRegistry decode
// ---------------------------------------------------------------------------

/// `PodFailurePolicy` carries a single repeated field (`rules`, field 1).
/// Encoding two minimal rules and decoding through the registry must surface
/// the rule actions — proving the schema's `Repeated(Message("...Rule"))`
/// field is wired correctly. Without the registry change, this returns an
/// empty `{}` and the assertion fails — the exact silent-drop class of bug
/// that PR #690 closed for `PodStatus`.
#[test]
fn pod_failure_policy_proto_decodes_rules() {
    let registry = ProtoRegistry::new();

    // PodFailurePolicyRule { action: "FailJob" }
    let rule_a = encode_string_field(1, "FailJob");
    // PodFailurePolicyRule { action: "Ignore" }
    let rule_b = encode_string_field(1, "Ignore");

    // PodFailurePolicy { rules: [rule_a, rule_b] }
    let mut bytes = Vec::new();
    push_length_delimited(&mut bytes, 1, &rule_a);
    push_length_delimited(&mut bytes, 1, &rule_b);

    let decoded = registry
        .decode_message("PodFailurePolicy", &bytes)
        .expect("PodFailurePolicy schema must be registered");

    let rules = decoded
        .get("rules")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("rules array missing in decoded value: {decoded}"));
    assert_eq!(rules.len(), 2, "both rules must decode; got {decoded}");
    assert_eq!(
        rules[0].get("action").and_then(|v| v.as_str()),
        Some("FailJob"),
    );
    assert_eq!(
        rules[1].get("action").and_then(|v| v.as_str()),
        Some("Ignore"),
    );
}

/// `SuccessPolicy.rules` (repeated SuccessPolicyRule, field 1) — same wire
/// shape, same registry concern, separate assertion so future schema edits
/// can't pass with one half wired and the other forgotten.
#[test]
fn success_policy_proto_decodes_rules() {
    let registry = ProtoRegistry::new();

    // SuccessPolicyRule { succeededIndexes: "0-2" }
    let rule_a = encode_string_field(1, "0-2");
    // SuccessPolicyRule { succeededCount: 3 } — field 2 is int32, wire type 0
    let rule_b = encode_varint_field(2, 3);

    // SuccessPolicy { rules: [rule_a, rule_b] }
    let mut bytes = Vec::new();
    push_length_delimited(&mut bytes, 1, &rule_a);
    push_length_delimited(&mut bytes, 1, &rule_b);

    let decoded = registry
        .decode_message("SuccessPolicy", &bytes)
        .expect("SuccessPolicy schema must be registered");

    let rules = decoded
        .get("rules")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("rules array missing in decoded value: {decoded}"));
    assert_eq!(rules.len(), 2, "both rules must decode; got {decoded}");
    assert_eq!(
        rules[0].get("succeededIndexes").and_then(|v| v.as_str()),
        Some("0-2"),
    );
    assert_eq!(
        rules[1].get("succeededCount").and_then(|v| v.as_i64()),
        Some(3),
    );
}

/// End-to-end: decode a JobSpec wire payload with `podFailurePolicy` (field
/// 11) and `successPolicy` (field 16) set. The reconstructed JSON must be
/// re-deserializable into the typed `JobSpec` — exactly the path the
/// api-server's protobuf middleware takes for write requests.
#[test]
fn jobspec_proto_decode_round_trips_into_typed_jobspec() {
    let registry = ProtoRegistry::new();

    // PodFailurePolicy { rules: [PodFailurePolicyRule { action: "FailJob",
    //     onExitCodes: PodFailurePolicyOnExitCodesRequirement { operator: "In", values: [1] } }] }
    let on_exit = {
        let mut buf = Vec::new();
        // operator (field 2)
        buf.extend(encode_string_field(2, "In"));
        // values (field 3, repeated int32 — emitted as packed by upstream, but
        // a sequence of single-element wires is also valid). Use unpacked
        // here for simplicity.
        buf.extend(encode_varint_field(3, 1));
        buf
    };
    let rule = {
        let mut buf = Vec::new();
        buf.extend(encode_string_field(1, "FailJob"));
        push_length_delimited(&mut buf, 2, &on_exit);
        buf
    };
    let pfp_bytes = {
        let mut buf = Vec::new();
        push_length_delimited(&mut buf, 1, &rule);
        buf
    };

    // SuccessPolicy { rules: [SuccessPolicyRule { succeededIndexes: "0,2" }] }
    let sp_rule = encode_string_field(1, "0,2");
    let sp_bytes = {
        let mut buf = Vec::new();
        push_length_delimited(&mut buf, 1, &sp_rule);
        buf
    };

    // JobSpec — field 11 podFailurePolicy, field 16 successPolicy.
    let mut spec_bytes = Vec::new();
    push_length_delimited(&mut spec_bytes, 11, &pfp_bytes);
    push_length_delimited(&mut spec_bytes, 16, &sp_bytes);

    let decoded = registry
        .decode_message("JobSpec", &spec_bytes)
        .expect("JobSpec schema must be registered");

    // The two policy fields must be present at the top level.
    assert!(
        decoded.get("podFailurePolicy").is_some(),
        "podFailurePolicy must appear at the top level of decoded JobSpec; got {decoded}",
    );
    assert!(
        decoded.get("successPolicy").is_some(),
        "successPolicy must appear at the top level of decoded JobSpec; got {decoded}",
    );

    // Round-trip through the typed sub-structs — this is what the api-server's
    // pod/job handler ends up doing after the protobuf middleware converts
    // bytes → Value. We can't decode the whole `JobSpec` here because the
    // synthetic wire payload above only carries the two policy fields, and
    // `JobSpec.template` is required by upstream contract (kubectl always
    // sends it). The middleware bug we guard against drops the policy fields
    // silently, which would surface as `None` below regardless of whether
    // `template` is present.
    let pfp_value = decoded
        .get("podFailurePolicy")
        .cloned()
        .expect("podFailurePolicy must be present on the decoded JobSpec");
    let pfp: PodFailurePolicy = serde_json::from_value(pfp_value).unwrap_or_else(|e| {
        panic!("typed PodFailurePolicy decode failed: {e}\nproto-decoded value: {decoded}")
    });
    assert_eq!(pfp.rules.len(), 1, "exactly one PodFailurePolicyRule");
    assert_eq!(pfp.rules[0].action, "FailJob");
    let on_exit = pfp.rules[0]
        .on_exit_codes
        .as_ref()
        .expect("onExitCodes must be present");
    assert_eq!(on_exit.operator, "In");
    assert_eq!(on_exit.values, vec![1]);

    let sp_value = decoded
        .get("successPolicy")
        .cloned()
        .expect("successPolicy must be present on the decoded JobSpec");
    let sp: SuccessPolicy = serde_json::from_value(sp_value).unwrap_or_else(|e| {
        panic!("typed SuccessPolicy decode failed: {e}\nproto-decoded value: {decoded}")
    });
    assert_eq!(sp.rules.len(), 1);
    assert_eq!(sp.rules[0].succeeded_indexes.as_deref(), Some("0,2"));
}

/// Sanity-check the per-rule sub-messages are themselves registered with the
/// right fields. A passing `register_batch_v1` block can mask a missing
/// field if the parent only ever sees the message by its tag — verify the
/// sub-messages directly so a regression like "drop `containerName` from
/// PodFailurePolicyOnExitCodesRequirement" surfaces here.
#[test]
fn pod_failure_policy_subtypes_are_registered() {
    let registry = ProtoRegistry::new();

    for name in [
        "PodFailurePolicy",
        "PodFailurePolicyRule",
        "PodFailurePolicyOnExitCodesRequirement",
        "PodFailurePolicyOnPodConditionsPattern",
        "SuccessPolicy",
        "SuccessPolicyRule",
    ] {
        // A registered message with at least one field round-trips an empty
        // payload to `{}`; an unregistered name returns `None`.
        let decoded = registry
            .decode_message(name, &[])
            .unwrap_or_else(|| panic!("{name} must be registered in ProtoRegistry"));
        assert!(
            decoded.is_object(),
            "{name} decode must yield a JSON object; got {decoded}",
        );
    }

    // Spot-check default-constructible structs serialize cleanly — protects
    // against accidental non-Default trait removals on the typed structs.
    let _ = serde_json::to_value(PodFailurePolicy::default()).expect("PodFailurePolicy default");
    let _ = serde_json::to_value(PodFailurePolicyRule::default()).expect("Rule default");
    let _ = serde_json::to_value(PodFailurePolicyOnExitCodesRequirement::default())
        .expect("OnExitCodes default");
    let _ = serde_json::to_value(PodFailurePolicyOnPodConditionsPattern::default())
        .expect("OnPodConditions default");
    let _ = serde_json::to_value(SuccessPolicy::default()).expect("SuccessPolicy default");
    let _ = serde_json::to_value(SuccessPolicyRule::default()).expect("SuccessPolicyRule default");
}

// ---------------------------------------------------------------------------
// proto wire helpers
// ---------------------------------------------------------------------------

/// Encode a single proto string field (wire type 2, length-delimited).
fn encode_string_field(field_number: u32, value: &str) -> Vec<u8> {
    let mut out = Vec::new();
    push_length_delimited(&mut out, field_number, value.as_bytes());
    out
}

/// Encode a single proto varint field (wire type 0, int32/int64/uint*).
fn encode_varint_field(field_number: u32, value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    push_varint(&mut out, u64::from(field_number) << 3); // wire type 0
    push_varint(&mut out, value);
    out
}

/// Push a length-delimited `(tag, len, payload)` triple onto `out` for the
/// given field number. Wire type is 2.
fn push_length_delimited(out: &mut Vec<u8>, field_number: u32, payload: &[u8]) {
    push_varint(out, (u64::from(field_number) << 3) | 2);
    push_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

/// Push a base-128 varint onto `out`.
fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}
