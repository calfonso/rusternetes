//! JSON roundtrip tests for `ObjectMeta.managedFields[*]`.
//!
//! `ManagedFieldsEntry` mirrors the upstream
//! `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go` definition:
//!
//! ```text
//! repeated ManagedFieldsEntry {
//!     manager     string
//!     operation   string   // "Apply" or "Update"
//!     apiVersion  string
//!     time        Time
//!     fieldsType  string   // always "FieldsV1" today
//!     fieldsV1    FieldsV1 // bytes on the wire, JSON-passthrough object
//!     subresource string
//! }
//! ```
//!
//! `FieldsV1` is interesting: on the wire (proto) it is `bytes`, but the
//! payload is a serialized JSON object such as `{"f:metadata":{"f:labels":{}}}`
//! that must survive a decode -> re-encode -> decode cycle without mutation.
//! These tests pin that JSON-semantic stability for several realistic shapes.
//!
//! Mirrors the helper pattern in `roundtrip_core_v1.rs` -- the local helper
//! there is not `pub`, so we inline the same four-step assertion here.

use rusternetes_common::resources::Pod;
use serde::{de::DeserializeOwned, Serialize};

/// Run the four-step roundtrip assertion for a typed payload.
///
/// 1. decode fixture -> `T`
/// 2. encode `T` -> JSON
/// 3. decode JSON -> `T`
/// 4. compare the two decoded `T`s via their `serde_json::Value` projection
///
/// We compare as `Value` rather than via `PartialEq` because many core/v1
/// structs (Pod in particular) don't currently derive `PartialEq` and the goal
/// of the layer is to verify the *wire* shape survives -- that's exactly what
/// Value-equality measures.
fn assert_roundtrip<T>(fixture: &str)
where
    T: Serialize + DeserializeOwned,
{
    let decoded: T = serde_json::from_str(fixture)
        .unwrap_or_else(|e| panic!("initial decode failed: {e}\nfixture: {fixture}"));
    let re_encoded = serde_json::to_string(&decoded).expect("re-encode failed");
    let re_decoded: T = serde_json::from_str(&re_encoded)
        .unwrap_or_else(|e| panic!("second decode failed: {e}\nre_encoded: {re_encoded}"));

    let decoded_value = serde_json::to_value(&decoded).expect("decoded -> Value");
    let re_decoded_value = serde_json::to_value(&re_decoded).expect("re_decoded -> Value");
    assert_eq!(
        decoded_value, re_decoded_value,
        "roundtrip not stable\nfirst:  {decoded_value}\nsecond: {re_decoded_value}",
    );
}

// =============================================================================
// Single manager with a realistic FieldsV1 payload.
// =============================================================================

#[test]
fn roundtrip_pod_managed_fields_single_entry() {
    // Mirrors what kubectl apply leaves on a Pod created via SSA: one entry
    // with manager=kubectl, operation=Apply, a populated FieldsV1 tree, and
    // a wall-clock timestamp.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ssa-applied",
            "namespace": "default",
            "managedFields": [{
                "manager": "kubectl",
                "operation": "Apply",
                "apiVersion": "v1",
                "time": "2026-01-01T00:00:00Z",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:metadata": {
                        "f:labels": {
                            "f:app": {}
                        }
                    },
                    "f:spec": {
                        "f:containers": {
                            "k:{\"name\":\"c\"}": {
                                ".": {},
                                "f:name": {},
                                "f:image": {}
                            }
                        }
                    }
                }
            }]
        },
        "spec": {
            "containers": [{"name": "c", "image": "nginx"}]
        }
    }"#;
    assert_roundtrip::<Pod>(fixture);
}

// =============================================================================
// Multiple managers — order must be preserved across the roundtrip.
// =============================================================================

#[test]
fn roundtrip_pod_managed_fields_multiple_managers_order_preserved() {
    // Three managers in a deliberate non-alphabetical order: kubectl (Apply),
    // then the kubelet (Update on /status), then a custom controller. Upstream
    // semantics keep this list ordered, so the roundtrip must too.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "multi-mgr",
            "namespace": "default",
            "managedFields": [
                {
                    "manager": "kubectl",
                    "operation": "Apply",
                    "apiVersion": "v1",
                    "time": "2026-01-01T00:00:00Z",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": {
                        "f:metadata": {"f:labels": {"f:app": {}}},
                        "f:spec": {"f:containers": {}}
                    }
                },
                {
                    "manager": "kubelet",
                    "operation": "Update",
                    "apiVersion": "v1",
                    "time": "2026-01-01T00:00:05Z",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": {
                        "f:status": {
                            "f:phase": {},
                            "f:podIP": {},
                            "f:hostIP": {}
                        }
                    },
                    "subresource": "status"
                },
                {
                    "manager": "custom-controller",
                    "operation": "Update",
                    "apiVersion": "v1",
                    "time": "2026-01-01T00:00:10Z",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": {
                        "f:metadata": {
                            "f:annotations": {
                                "f:my.controller/reconciled": {}
                            }
                        }
                    }
                }
            ]
        },
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    }"#;

    // Use the standard helper for serde stability, then verify the manager
    // ordering is preserved across a decode -> encode -> decode cycle.
    assert_roundtrip::<Pod>(fixture);

    let decoded: Pod = serde_json::from_str(fixture).expect("decode");
    let re_encoded = serde_json::to_string(&decoded).expect("encode");
    let re_decoded: Pod = serde_json::from_str(&re_encoded).expect("re-decode");
    let managers: Vec<Option<String>> = re_decoded
        .metadata
        .managed_fields
        .as_ref()
        .expect("managedFields present")
        .iter()
        .map(|e| e.manager.clone())
        .collect();
    assert_eq!(
        managers,
        vec![
            Some("kubectl".to_string()),
            Some("kubelet".to_string()),
            Some("custom-controller".to_string()),
        ],
        "manager order must be preserved across the roundtrip",
    );
}

// =============================================================================
// Empty FieldsV1 — `{}` must survive as `{}`.
// =============================================================================

#[test]
fn roundtrip_pod_managed_fields_empty_fields_v1() {
    // Upstream emits `fieldsV1: {}` on entries that own no fields (e.g.
    // immediately after a manager has been cleared). The empty object must
    // round-trip as `{}`, not be normalised to null/omitted.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "empty-fv1",
            "namespace": "default",
            "managedFields": [{
                "manager": "noop",
                "operation": "Update",
                "apiVersion": "v1",
                "time": "2026-01-01T00:00:00Z",
                "fieldsType": "FieldsV1",
                "fieldsV1": {}
            }]
        },
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    }"#;

    assert_roundtrip::<Pod>(fixture);

    let decoded: Pod = serde_json::from_str(fixture).expect("decode");
    let entry = &decoded
        .metadata
        .managed_fields
        .as_ref()
        .expect("managedFields present")[0];
    let fv1 = entry.fields_v1.as_ref().expect("fieldsV1 present");
    assert!(
        fv1.is_object() && fv1.as_object().unwrap().is_empty(),
        "fieldsV1 must remain an empty JSON object, got: {fv1}",
    );
}

// =============================================================================
// Subresource = "status" — kubelet-style update path.
// =============================================================================

#[test]
fn roundtrip_pod_managed_fields_subresource_status() {
    // kubelet patches /status, so the resulting managedFields entry carries
    // `subresource: "status"` and a FieldsV1 tree rooted at `f:status`.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "status-sub",
            "namespace": "default",
            "managedFields": [{
                "manager": "kubelet",
                "operation": "Update",
                "apiVersion": "v1",
                "time": "2026-01-01T00:00:00Z",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:status": {
                        "f:phase": {},
                        "f:conditions": {
                            "k:{\"type\":\"Ready\"}": {
                                ".": {},
                                "f:type": {},
                                "f:status": {}
                            }
                        },
                        "f:containerStatuses": {}
                    }
                },
                "subresource": "status"
            }]
        },
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    }"#;

    assert_roundtrip::<Pod>(fixture);

    let decoded: Pod = serde_json::from_str(fixture).expect("decode");
    let entry = &decoded
        .metadata
        .managed_fields
        .as_ref()
        .expect("managedFields present")[0];
    assert_eq!(
        entry.subresource.as_deref(),
        Some("status"),
        "subresource must survive roundtrip",
    );
}

// =============================================================================
// Operation = Apply with no time / no subresource — sparse field coverage.
// =============================================================================

#[test]
fn roundtrip_pod_managed_fields_sparse_optionals_omitted() {
    // Several fields are optional in the meta/v1 schema. A sparse entry that
    // omits time, fieldsType, fieldsV1, and subresource must round-trip
    // without any of them gaining default values.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "sparse",
            "namespace": "default",
            "managedFields": [{
                "manager": "sparse-mgr",
                "operation": "Apply",
                "apiVersion": "v1"
            }]
        },
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    }"#;

    assert_roundtrip::<Pod>(fixture);

    let decoded: Pod = serde_json::from_str(fixture).expect("decode");
    let entry = &decoded
        .metadata
        .managed_fields
        .as_ref()
        .expect("managedFields present")[0];
    assert!(entry.time.is_none(), "time must stay None when omitted");
    assert!(
        entry.fields_type.is_none(),
        "fieldsType must stay None when omitted",
    );
    assert!(
        entry.fields_v1.is_none(),
        "fieldsV1 must stay None when omitted",
    );
    assert!(
        entry.subresource.is_none(),
        "subresource must stay None when omitted",
    );
}
