//! Regression test for the upstream conformance test
//! `[sig-node] Pods should run through the lifecycle of Pods and PodStatus [Conformance]`.
//!
//! Upstream test source (release-1.35):
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/common/node/pods.go#L1044>
//!
//! The relevant fragment (paraphrased):
//!
//! ```text
//! podStatusUpdate, err = clientset.CoreV1().Pods(ns).UpdateStatus(ctx, &podStatusUpdated, ...)
//! ...
//! for _, cond := range podStatusUpdate.Status.Conditions {
//!     if (cond.Type == PodReady && cond.Status == ConditionFalse) ||
//!        (cond.Type == ContainersReady && cond.Status == ConditionFalse) {
//!         podStatusFieldPatchCount++
//!     }
//! }
//! Expect(podStatusFieldPatchCount).To(Equal(2),
//!     "failed to update PodStatus - field patch count doesn't match the total")
//! ```
//!
//! That is: after PUTting a full Pod to /status with `Ready` and
//! `ContainersReady` flipped to `False`, the server's response **must**
//! contain both flipped conditions. This exercises:
//!
//!   1. the `/status` subresource PUT path (typed `UpdateStatus`),
//!   2. spec preservation by the server,
//!   3. metadata stripping (resourceVersion etc.),
//!   4. that the new status fully replaces the old (no condition merge on PUT).
//!
//! We test the pure logic via the extracted helper
//! `build_updated_resource_for_status`, which is the exact function
//! `crates/api-server/src/handlers/status.rs::update_status` calls to
//! produce the value it persists.

use rusternetes_api_server::handlers::status::build_updated_resource_for_status;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::{json, Value};
use std::sync::Arc;

/// Build a Pod JSON value mimicking what the kubelet writes once a pod is
/// `Running`: phase Running, all four pod conditions `True`,
/// container_statuses populated. This is what a /status GET returns to the
/// e2e test just before it patches conditions to `False`.
fn running_pod_with_true_conditions(namespace: &str, name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": "test-uid-pod-status",
            "resourceVersion": "42",
            "labels": {
                "test-pod-static": "true"
            }
        },
        "spec": {
            "terminationGracePeriodSeconds": 1,
            "containers": [{
                "name": "pod-test",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.40"
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.1.2.3",
            "startTime": "2024-01-01T00:00:00Z",
            "qosClass": "BestEffort",
            "conditions": [
                {"type": "Initialized",    "status": "True",  "lastTransitionTime": "2024-01-01T00:00:00Z"},
                {"type": "ContainersReady","status": "True",  "lastTransitionTime": "2024-01-01T00:00:01Z"},
                {"type": "Ready",          "status": "True",  "lastTransitionTime": "2024-01-01T00:00:02Z"},
                {"type": "PodScheduled",   "status": "True",  "lastTransitionTime": "2024-01-01T00:00:00Z"}
            ],
            "containerStatuses": [{
                "name": "pod-test",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.40",
                "imageID": "registry.k8s.io/e2e-test-images/agnhost@sha256:deadbeef",
                "ready": true,
                "restartCount": 0,
                "started": true,
                "state": {"running": {"startedAt": "2024-01-01T00:00:01Z"}}
            }]
        }
    })
}

/// Mirror of the e2e test sequence:
/// 1. Get pod via /status (returns full pod).
/// 2. Flip `Ready` and `ContainersReady` to `False` in the typed pod.
/// 3. UpdateStatus (PUT) the full pod back.
/// 4. Inspect the response: it must contain `Ready=False` and `ContainersReady=False`.
fn flip_pod_ready_and_containers_ready(mut pod: Value) -> Value {
    if let Some(conds) = pod
        .get_mut("status")
        .and_then(|s| s.get_mut("conditions"))
        .and_then(|c| c.as_array_mut())
    {
        for cond in conds.iter_mut() {
            let ty = cond.get("type").and_then(|t| t.as_str()).unwrap_or("");
            if (ty == "Ready" || ty == "ContainersReady")
                && cond.get("status").and_then(|s| s.as_str()) == Some("True")
            {
                cond["status"] = Value::String("False".to_string());
            }
        }
    }
    pod
}

fn count_false_ready_conds(pod: &Value) -> usize {
    pod.get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .map(|conds| {
            conds
                .iter()
                .filter(|c| {
                    let ty = c.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    let st = c.get("status").and_then(|s| s.as_str()).unwrap_or("");
                    (ty == "Ready" || ty == "ContainersReady") && st == "False"
                })
                .count()
        })
        .unwrap_or(0)
}

/// Replicates upstream e2e test
/// `[sig-node] Pods should run through the lifecycle of Pods and PodStatus`,
/// specifically the assertion at
/// `k8s.io/kubernetes/test/e2e/common/node/pods.go:1044`:
/// "failed to update PodStatus - field patch count doesn't match the total".
///
/// After PUT /status with both `Ready` and `ContainersReady` set to `False`,
/// the response and the persisted pod must contain those two conditions
/// flipped to `False`.
#[tokio::test]
async fn put_pod_status_preserves_flipped_ready_conditions() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();

    let ns = "e2e";
    let name = "pod-test";
    let key = build_key("pods", Some(ns), name);

    // 1. Pod is Running with both Ready conditions = True (as kubelet sets).
    let current = running_pod_with_true_conditions(ns, name);
    storage.create(&key, &current).await.unwrap();

    // 2. Simulate the e2e dynamic-client Get + flip + UpdateStatus body.
    //    UpdateStatus marshals the *full* typed Pod, so the body looks like the
    //    current pod with both Ready/ContainersReady flipped to False.
    let fetched: Value = storage.get(&key).await.unwrap();
    assert_eq!(
        fetched
            .pointer("/status/conditions/1/type")
            .and_then(|v| v.as_str()),
        Some("ContainersReady")
    );
    assert_eq!(
        fetched
            .pointer("/status/conditions/2/type")
            .and_then(|v| v.as_str()),
        Some("Ready")
    );

    let request_body = flip_pod_ready_and_containers_ready(fetched.clone());
    // Sanity: pre-PUT, the request body must already have 2 False conditions.
    // This mirrors the in-loop check upstream at pods.go:1031.
    assert_eq!(
        count_false_ready_conds(&request_body),
        2,
        "test fixture broken: body should have 2 flipped conditions"
    );

    // 3. Drive the *exact* handler logic. content-type is application/json (typed
    //    client UpdateStatus, NOT a merge-patch) so is_merge_patch is false.
    let updated = build_updated_resource_for_status(
        &fetched,      // current_resource (loaded by the handler)
        &request_body, // new_resource (parsed from body)
        false,         // is_merge_patch
        "pods",        // resource_type
    )
    .expect("build_updated_resource_for_status should succeed");

    // Persist (as the handler does) and read the response back.
    let saved: Value = storage.update(&key, &updated).await.unwrap();

    // 4. Upstream e2e assertion:
    //    podStatusFieldPatchCount must equal podStatusFieldPatchCountTotal (=2).
    assert_eq!(
        count_false_ready_conds(&saved),
        2,
        "failed to update PodStatus - field patch count doesn't match the total \
         (saved.status.conditions = {})",
        saved
            .pointer("/status/conditions")
            .cloned()
            .unwrap_or(Value::Null)
    );

    // Spec must be preserved (status subresource cannot mutate spec).
    assert_eq!(
        saved.pointer("/spec/containers/0/name"),
        Some(&Value::String("pod-test".to_string()))
    );

    // Other status fields (podIP, phase, containerStatuses) must be preserved
    // because the typed UpdateStatus body included them.
    assert_eq!(
        saved.pointer("/status/phase"),
        Some(&Value::String("Running".to_string()))
    );
    assert_eq!(
        saved.pointer("/status/podIP"),
        Some(&Value::String("10.1.2.3".to_string()))
    );
}

/// Same test but using a strategic-merge-patch body, modelling the
/// "should patch a pod status" conformance test
/// (`test/e2e/common/node/pods.go` "should patch a pod status").
/// A sparse status patch must merge cleanly with the existing status.
#[tokio::test]
async fn strategic_merge_patch_pod_status_preserves_other_fields() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();

    let ns = "e2e";
    let name = "pod-test";
    let key = build_key("pods", Some(ns), name);

    let current = running_pod_with_true_conditions(ns, name);
    storage.create(&key, &current).await.unwrap();

    // Sparse patch — only updates status.message and status.reason.
    let patch_body = json!({
        "metadata": {"annotations": {"patchedstatus": "true"}},
        "status": {"message": "Patched by e2e test", "reason": "E2E"}
    });

    let fetched: Value = storage.get(&key).await.unwrap();
    let updated =
        build_updated_resource_for_status(&fetched, &patch_body, true /* merge */, "pods")
            .expect("build_updated_resource_for_status");
    let saved: Value = storage.update(&key, &updated).await.unwrap();

    // The patched fields land.
    assert_eq!(
        saved.pointer("/status/message"),
        Some(&Value::String("Patched by e2e test".to_string()))
    );
    assert_eq!(
        saved.pointer("/status/reason"),
        Some(&Value::String("E2E".to_string()))
    );
    assert_eq!(
        saved.pointer("/metadata/annotations/patchedstatus"),
        Some(&Value::String("true".to_string()))
    );

    // Existing status fields must be preserved (no wipe on merge).
    assert_eq!(
        saved.pointer("/status/phase"),
        Some(&Value::String("Running".to_string()))
    );
    assert_eq!(
        saved.pointer("/status/podIP"),
        Some(&Value::String("10.1.2.3".to_string()))
    );
    // Conditions should be preserved (the patch didn't touch them).
    let conds = saved
        .pointer("/status/conditions")
        .and_then(|c| c.as_array())
        .expect("conditions preserved");
    assert_eq!(conds.len(), 4, "all original conditions preserved");
}
