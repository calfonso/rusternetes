//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-node] RuntimeClass — extended scenarios.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/runtimeclass.go`
//!     https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/node/runtimeclass.go
//!   - `staging/src/k8s.io/api/node/v1/types.go`
//!
//! Conformance descriptors covered here (Sonobuoy round from
//! `conformance-skip-20260528-235000/failing.txt`):
//!
//!   - "should support RuntimeClasses API operations"               [Conformance]   → FAIL
//!   - "should reject a Pod requesting a deleted RuntimeClass"      [Conformance]   → FAIL
//!   - "should schedule a Pod requesting a RuntimeClass without
//!     PodOverhead"                                                  [Conformance]   → FAIL
//!   - "should schedule a Pod requesting a RuntimeClass and
//!     initialize its Overhead"                                      [Conformance]   → FAIL
//!
//! The first conformance test in `conformance_node_runtimeclass.rs`
//! ("should reject a Pod requesting a non-existent RuntimeClass") is
//! passing and lives in the companion file. This file holds the four
//! that are still failing in the live cluster — they need a live API
//! server + scheduler to schedule or reject pods, so they are annotated
//! `#[ignore]`. The pure-shape invariants in each test (struct building,
//! serde round-trips) **do** run and must pass.
//!
//! Style: pure-function `#[test]` where possible; `#[ignore]` for the
//! live-runtime assertions that need a running cluster.

use rusternetes_common::resources::pod::Toleration;
use rusternetes_common::resources::runtimeclass::{Overhead, RuntimeClass, Scheduling};
use rusternetes_common::resources::{Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

/// Build a minimal Pod that references a named RuntimeClass.
fn make_pod_with_runtime_class(name: &str, runtime_class_name: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            runtime_class_name: Some(runtime_class_name.to_string()),
            containers: vec![],
            ..Default::default()
        }),
        status: None,
    }
}

/// Build a RuntimeClass with PodOverhead set.
fn make_rc_with_overhead(name: &str, handler: &str, cpu: &str, mem: &str) -> RuntimeClass {
    let mut pod_fixed = HashMap::new();
    pod_fixed.insert("cpu".to_string(), cpu.to_string());
    pod_fixed.insert("memory".to_string(), mem.to_string());
    RuntimeClass::new(name, handler).with_overhead(Overhead {
        pod_fixed: Some(pod_fixed),
    })
}

/// Build a RuntimeClass with Scheduling constraints (nodeSelector + toleration).
fn make_rc_with_scheduling(
    name: &str,
    handler: &str,
    selector_key: &str,
    selector_val: &str,
) -> RuntimeClass {
    let mut node_selector = HashMap::new();
    node_selector.insert(selector_key.to_string(), selector_val.to_string());
    RuntimeClass::new(name, handler).with_scheduling(Scheduling {
        node_selector: Some(node_selector),
        tolerations: Some(vec![Toleration {
            key: Some(selector_key.to_string()),
            operator: Some("Equal".to_string()),
            value: Some(selector_val.to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }]),
    })
}

// ===========================================================================
// 1. RuntimeClass API operations
//
// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
//   "should support RuntimeClasses API operations" [Conformance]
//
// Sonobuoy (conformance-skip-20260528-235000): FAIL — API CRUD operations
// require a live API server.
//
// Pure invariants (struct shape, serde) DO run. The live CRUD sequence
// (create → get → list → patch → delete) is `#[ignore]`d.
// ===========================================================================

/// [sig-node] RuntimeClass should support RuntimeClasses API operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): FAIL — requires live API server for CRUD.
///
/// Pure-shape sub-test: the RuntimeClass struct round-trips through JSON
/// with all fields (handler, overhead.podFixed, scheduling) intact.
#[test]
fn runtimeclass_api_operations_struct_roundtrip() {
    let rc = make_rc_with_overhead("runc", "runc", "250m", "64Mi").with_scheduling(Scheduling {
        node_selector: Some({
            let mut m = HashMap::new();
            m.insert("runtime".to_string(), "runc".to_string());
            m
        }),
        tolerations: None,
    });
    // Kind + apiVersion survive serde.
    let v = serde_json::to_value(&rc).unwrap();
    assert_eq!(v["kind"], "RuntimeClass");
    assert_eq!(v["apiVersion"], "node.k8s.io/v1");
    assert_eq!(v["handler"], "runc");
    assert_eq!(v["overhead"]["podFixed"]["cpu"], "250m");
    assert_eq!(v["overhead"]["podFixed"]["memory"], "64Mi");
    assert_eq!(v["scheduling"]["nodeSelector"]["runtime"], "runc");
}

/// [sig-node] RuntimeClass API operations — deserialise from canonical JSON
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): pure-shape sub-test (PASS).
#[test]
fn runtimeclass_deserialises_from_canonical_json() {
    let json = serde_json::json!({
        "kind": "RuntimeClass",
        "apiVersion": "node.k8s.io/v1",
        "metadata": { "name": "gvisor" },
        "handler": "runsc",
        "overhead": { "podFixed": { "cpu": "500m", "memory": "256Mi" } },
        "scheduling": {
            "nodeSelector": { "sandbox.gke.io/runtime": "gvisor" },
            "tolerations": [
                {
                    "key": "sandbox.gke.io/runtime",
                    "operator": "Equal",
                    "value": "gvisor",
                    "effect": "NoSchedule"
                }
            ]
        }
    });
    let rc: RuntimeClass = serde_json::from_value(json).unwrap();
    assert_eq!(rc.handler, "runsc");
    assert_eq!(rc.metadata.name, "gvisor");
    let overhead = rc.overhead.expect("overhead must be present");
    let pf = overhead.pod_fixed.expect("podFixed must be present");
    assert_eq!(pf.get("cpu").map(|s| s.as_str()), Some("500m"));
    assert_eq!(pf.get("memory").map(|s| s.as_str()), Some("256Mi"));
    let sched = rc.scheduling.expect("scheduling must be present");
    let ns = sched.node_selector.expect("nodeSelector must be present");
    assert_eq!(
        ns.get("sandbox.gke.io/runtime").map(|s| s.as_str()),
        Some("gvisor")
    );
    let tols = sched.tolerations.expect("tolerations must be present");
    assert_eq!(tols.len(), 1);
    assert_eq!(tols[0].effect.as_deref(), Some("NoSchedule"));
}

/// [sig-node] RuntimeClass API operations — live CRUD
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): FAIL — live API server required for
/// create/get/list/patch/delete.
#[test]
#[ignore = "GAP: live API server required for RuntimeClass CRUD; upstream runtimeclass.go"]
fn runtimeclass_api_crud_lifecycle() {
    // create → get → list → patch (update handler) → delete
    // Checked by the e2e suite against a live cluster; not exercisable
    // from a pure unit test.
}

// ===========================================================================
// 2. Reject Pod requesting a DELETED RuntimeClass
//
// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
//   "should reject a Pod requesting a deleted RuntimeClass" [Conformance]
//
// Sonobuoy (2026-05-28): FAIL — requires live API server to delete the
// RuntimeClass then observe Pod admission rejection.
// ===========================================================================

/// [sig-node] RuntimeClass should reject a Pod requesting a deleted RuntimeClass [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): FAIL — requires a live API server + admission webhook.
///
/// Pure shape sub-test: a Pod spec that sets `runtimeClassName` round-trips
/// the field correctly so the admission controller can inspect it.
#[test]
fn pod_with_deleted_runtime_class_spec_preserves_runtime_class_name() {
    let pod = make_pod_with_runtime_class("test-pod", "deleted-runtime");
    // The field must survive a spec round-trip so the API server can reject it.
    let spec = pod.spec.as_ref().expect("spec must be Some");
    assert_eq!(
        spec.runtime_class_name.as_deref(),
        Some("deleted-runtime"),
        "runtimeClassName must round-trip through PodSpec"
    );
}

/// [sig-node] RuntimeClass reject deleted — live admission rejection
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): FAIL — live API server required.
#[test]
#[ignore = "GAP: live API server + admission required to reject Pod for deleted RuntimeClass; upstream runtimeclass.go"]
fn pod_requesting_deleted_runtime_class_is_rejected_on_admission() {
    // 1. Create RuntimeClass "myrc".
    // 2. Submit Pod referencing "myrc" — Pod is accepted.
    // 3. Delete RuntimeClass "myrc".
    // 4. Submit a second Pod referencing "myrc" — must be rejected at admission.
    // 5. Verify original Pod still runs (deletion only gates new pods).
}

// ===========================================================================
// 3. Schedule Pod with RuntimeClass but WITHOUT PodOverhead
//
// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
//   "should schedule a Pod requesting a RuntimeClass without PodOverhead"
//   [NodeConformance] [Conformance]
//
// Sonobuoy (2026-05-28): FAIL — requires live scheduler.
// ===========================================================================

/// [sig-node] RuntimeClass should schedule a Pod requesting a RuntimeClass without PodOverhead [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): FAIL — live scheduler required.
///
/// Pure shape sub-test: a RuntimeClass without overhead is valid JSON.
#[test]
fn runtimeclass_without_overhead_serialises_correctly() {
    let rc = RuntimeClass::new("runc", "runc");
    let v = serde_json::to_value(&rc).unwrap();
    // `overhead` must NOT appear in the serialised form when absent.
    assert!(
        v.get("overhead").is_none() || v["overhead"].is_null(),
        "overhead must be absent / null when not set"
    );
    assert_eq!(v["handler"], "runc");
}

/// [sig-node] RuntimeClass schedule without PodOverhead — overhead absent means
/// no resource inflation
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): pure-shape invariant (PASS).
///
/// When `overhead` is not set, the scheduler must NOT add extra resources to
/// the pod's effective resource requirements. We verify this at the spec level:
/// the `overhead` field being `None` must survive a round-trip so the scheduler
/// can distinguish "no overhead" from "overhead not yet resolved".
#[test]
fn runtimeclass_none_overhead_round_trips_as_none() {
    let rc = RuntimeClass::new("no-overhead", "runc");
    assert!(rc.overhead.is_none(), "overhead must be None when not set");
    // Re-deserialise from the serialised form to confirm the field is absent.
    let json = serde_json::to_value(&rc).unwrap();
    let rc2: RuntimeClass = serde_json::from_value(json).unwrap();
    assert!(
        rc2.overhead.is_none(),
        "overhead must remain None after serde round-trip"
    );
}

/// [sig-node] RuntimeClass schedule without PodOverhead — live scheduling
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): FAIL — live scheduler required.
#[test]
#[ignore = "GAP: live scheduler required to schedule Pod with RuntimeClass and no PodOverhead; upstream runtimeclass.go"]
fn pod_with_runtime_class_no_overhead_scheduled_and_running() {
    // 1. Create RuntimeClass without overhead.
    // 2. Create Pod referencing that RuntimeClass.
    // 3. Verify Pod reaches Running phase without extra overhead on node.
}

// ===========================================================================
// 4. Schedule Pod with RuntimeClass AND PodOverhead
//
// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
//   "should schedule a Pod requesting a RuntimeClass and initialize its
//   Overhead" [NodeConformance] [Conformance]
//
// Sonobuoy (2026-05-28): FAIL — requires live scheduler + overhead injection.
// ===========================================================================

/// [sig-node] RuntimeClass should schedule a Pod requesting a RuntimeClass and initialize its Overhead [NodeConformance] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): FAIL — live scheduler + overhead injection required.
///
/// Pure shape sub-test: PodOverhead is expressed through the RuntimeClass's
/// `overhead.podFixed` map and must survive a serde round-trip so the
/// scheduler can read and apply it.
#[test]
fn runtimeclass_with_overhead_roundtrips_pod_fixed_values() {
    let rc = make_rc_with_overhead("kata", "kata-runtime", "250m", "120Mi");
    let v = serde_json::to_value(&rc).unwrap();
    assert_eq!(v["overhead"]["podFixed"]["cpu"], "250m");
    assert_eq!(v["overhead"]["podFixed"]["memory"], "120Mi");
    // Deserialise back and confirm both keys.
    let rc2: RuntimeClass = serde_json::from_value(v).unwrap();
    let pf = rc2
        .overhead
        .as_ref()
        .and_then(|o| o.pod_fixed.as_ref())
        .expect("podFixed must survive round-trip");
    assert_eq!(pf.get("cpu").map(|s| s.as_str()), Some("250m"));
    assert_eq!(pf.get("memory").map(|s| s.as_str()), Some("120Mi"));
}

/// [sig-node] RuntimeClass with PodOverhead — overhead fields use camelCase
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): pure-shape invariant (PASS).
///
/// Kubernetes uses `podFixed` (camelCase) in JSON; a snake_case field name
/// would cause kubectl / the API server to silently drop the value.
#[test]
fn runtimeclass_overhead_pod_fixed_serialises_as_camel_case() {
    let rc = make_rc_with_overhead("vm-runtime", "virtiofs", "100m", "32Mi");
    let json_str = serde_json::to_string(&rc).unwrap();
    assert!(
        json_str.contains("\"podFixed\""),
        "overhead must use camelCase 'podFixed', got: {json_str}"
    );
    assert!(
        !json_str.contains("\"pod_fixed\""),
        "snake_case 'pod_fixed' must NOT appear in serialised output, got: {json_str}"
    );
}

/// [sig-node] RuntimeClass with PodOverhead — scheduling toleration is merged
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go (scheduling merge)
/// Sonobuoy (2026-05-28): pure-shape invariant (PASS).
///
/// When scheduling.tolerations is set on a RuntimeClass, the scheduler merges
/// them into the pod's toleration list at admission. We verify the struct
/// preserves the toleration after a serde round-trip.
#[test]
fn runtimeclass_scheduling_toleration_survives_roundtrip() {
    let rc = make_rc_with_scheduling("secure", "gvisor", "sandbox.gke.io/runtime", "gvisor");
    let v = serde_json::to_value(&rc).unwrap();
    let rc2: RuntimeClass = serde_json::from_value(v).unwrap();
    let tols = rc2
        .scheduling
        .as_ref()
        .and_then(|s| s.tolerations.as_ref())
        .expect("tolerations must survive round-trip");
    assert_eq!(tols.len(), 1);
    assert_eq!(tols[0].key.as_deref(), Some("sandbox.gke.io/runtime"));
    assert_eq!(tols[0].effect.as_deref(), Some("NoSchedule"));
}

/// [sig-node] RuntimeClass schedule with PodOverhead — live scheduling
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/runtimeclass.go
/// Sonobuoy (2026-05-28): FAIL — live scheduler + overhead injection required.
#[test]
#[ignore = "GAP: live scheduler required to inject PodOverhead from RuntimeClass into Pod resource accounting; upstream runtimeclass.go"]
fn pod_with_runtime_class_overhead_is_scheduled_and_overhead_initialised() {
    // 1. Create RuntimeClass with overhead.podFixed = {cpu: 250m, memory: 120Mi}.
    // 2. Create Pod referencing that RuntimeClass.
    // 3. Scheduler must add overhead to pod.spec.overhead before binding.
    // 4. Verify pod.spec.overhead matches RuntimeClass.overhead.podFixed.
    // 5. Pod reaches Running phase.
}
