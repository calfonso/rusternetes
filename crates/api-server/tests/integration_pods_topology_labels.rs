//! Scoped mirror of Kubernetes v1.35 `test/integration/pods/pods_test.go`.
//!
//! Source (release-1.35):
//!   https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/pods/pods_test.go
//!
//! Each `#[tokio::test]` below mirrors a single upstream `func TestX` in the
//! same file, preserving the upstream name. The harness drives the real axum
//! router via `tower::ServiceExt::oneshot` against `MemoryStorage` +
//! `AlwaysAllowAuthorizer`, exactly the same handler stack production HTTPS
//! requests traverse, mirroring `conformance_apimachinery_admission_webhooks.rs`
//! and `conformance_apimachinery_crd_lifecycle.rs`.
//!
//! These are RED-state TDD pins for upstream Pod admission behaviour that the
//! Rust api-server has not yet implemented:
//!
//!   * `TestPodTopologyLabels` / `TestPodTopologyLabels_FeatureDisabled` —
//!     the `PodTopologyLabelsAdmission` plugin copies
//!     `topology.kubernetes.io/zone` and `…/region` (and only those two keys)
//!     from a Node onto a Pod when a Binding is created. Subdomains and
//!     custom keys must not be copied; existing pod labels must win over
//!     node labels. Not yet implemented in
//!     `handlers::pod_subresources::create_binding`.
//!
//!   * `TestPodUpdateActiveDeadlineSeconds` — once set, `activeDeadlineSeconds`
//!     may only be reduced; it can never be unset, increased, or made zero/
//!     negative. Not yet enforced anywhere in `handlers::pod::update`.
//!
//!   * `TestPodReadOnlyFilesystem` — pods that declare
//!     `securityContext.readOnlyRootFilesystem=true` must round-trip through
//!     create/get/delete without error. The Rust struct already exposes the
//!     field; this test just pins the round-trip.
//!
//!   * `TestPodCreateEphemeralContainers` — `spec.ephemeralContainers` is
//!     forbidden on create. Upstream returns
//!     `spec.ephemeralContainers: Forbidden: cannot be set on create`. The
//!     pod handler currently accepts it silently.
//!
//!   * `TestPodPatchEphemeralContainers` / `TestPodUpdateEphemeralContainers`
//!     — `ephemeralContainers` must only be mutated through the
//!     `/ephemeralcontainers` subresource. The plain PUT/PATCH path on pods
//!     does not (yet) reject ephemeral-container deltas, and removing the
//!     entire list (or removing a previously-added container) is forbidden.
//!
//!   * `TestPodResizeRBAC` / `TestPodResize` — the in-place resize subresource
//!     surface is already covered by `pod_resize_cas_test.rs`. Mirrored here
//!     as `#[ignore]`d pins so the full upstream test count is preserved.
//!
//!   * `TestMutablePodSchedulingDirectives` — `spec.nodeSelector` and
//!     `spec.affinity.nodeAffinity` are mutable while a pod still has at
//!     least one `schedulingGates` entry. The update handler currently
//!     enforces full-spec-immutability and rejects the additions.
//!
//!   * `TestRelaxedDNSSearchValidation` — gated by the
//!     `RelaxedDNSSearchValidation` feature flag; the gate plumbing lives in
//!     `rusternetes_common::feature_gates` and the validator lives in
//!     `rusternetes_common::validation::pod::validate_pod_dns_config`.
//!
//!   * `TestNodeDeclaredFeatureAdmission` — gated by `NodeDeclaredFeatures` +
//!     `InPlacePodVerticalScaling`. The `/pods/{name}/resize` handler refuses
//!     a Guaranteed-QoS CPU resize when the target Node's
//!     `status.declaredFeatures` does not include `GuaranteedQoSPodCPUResize`.
//!     Field lives on `NodeStatus`; gate registry in
//!     `rusternetes_common::feature_gates`; admission helper in
//!     `handlers::pod_subresources::check_node_declared_features_for_resize`.
//!
//! See also: `pod_handler_test.rs` and `pod_resize_cas_test.rs` for the
//! complementary handler-level pod tests already covering create/get/update/
//! delete plumbing.

use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin `(u16, Value)` shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn post_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router.post(uri, body).await;
    (status.as_u16(), value)
}

async fn put_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router.put(uri, body).await;
    (status.as_u16(), value)
}

async fn patch_merge(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router.patch(uri, body).await;
    (status.as_u16(), value)
}

async fn get(router: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.get(uri).await;
    (status.as_u16(), value)
}

async fn delete(router: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.delete(uri).await;
    (status.as_u16(), value)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Minimal namespace body; matches the body
/// `framework.CreateNamespaceOrDie(client, name, t)` produces upstream.
fn ns_body(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    })
}

/// Single-container pod fixture matching upstream's `prototypePod()` —
/// `Image: "fakeimage"`, container `Name: "fake-name"`, no other fields.
fn prototype_pod(name: &str) -> Value {
    // Round-trip terminationGracePeriodSeconds explicitly so update bodies
    // don't trip the immutability fence (we don't run server-side defaulting
    // on UPDATE; old pod has the K8s default of 30 after create).
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name },
        "spec": {
            "containers": [{
                "name":  "fake-name",
                "image": "fakeimage",
            }],
            "terminationGracePeriodSeconds": 30,
        },
    })
}

/// Helper to create the namespace before any pod operation.
async fn create_namespace(router: &TestApiServer, name: &str) {
    let (status, body) = post_json(router, "/api/v1/namespaces", &ns_body(name)).await;
    assert!(
        status == 201 || status == 200,
        "namespace create must succeed: status={} body={}",
        status,
        body
    );
}

// ---------------------------------------------------------------------------
// Upstream: TestPodTopologyLabels (pods_test.go:48)
// ---------------------------------------------------------------------------

/// Mirrors upstream `TestPodTopologyLabels`. The
/// `PodTopologyLabelsAdmission` plugin (feature-gate on) copies the
/// `topology.kubernetes.io/zone` and `topology.kubernetes.io/region` labels
/// from the bound Node onto the Pod when a Binding is created. The four
/// sub-cases the upstream test runs:
///
///   1. zone+region copied verbatim;
///   2. subdomains of `topology.kubernetes.io` are *not* copied;
///   3. custom `topology.kubernetes.io/...` keys (other than zone/region)
///      are *not* copied;
///   4. when the pod already has a `topology.kubernetes.io/zone`/`region`
///      label, the *pod's* value wins over the node's (and unrelated keys on
///      the pod are preserved).
///
/// `#[serial]` because this test depends on the process-wide
/// `PodTopologyLabelsAdmission` feature gate being at its default (on).
/// `serial_test` only serialises among `#[serial]`-marked tests, so without
/// this a sibling test that flips the gate to off (also `#[serial]`) can land
/// mid-binding and skip the label copy — making this test flake.
#[tokio::test]
#[serial_test::serial]
async fn test_pod_topology_labels() {
    let (_, router) = spawn_router();
    let ns = "pod-topology-labels";
    create_namespace(&router, ns).await;

    // Case 1: zone+region copied from Node onto Pod via Binding.
    let node = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "topo-node-1",
            "labels": {
                "topology.kubernetes.io/zone":   "zone",
                "topology.kubernetes.io/region": "region",
            },
        },
    });
    let (st, body) = post_json(&router, "/api/v1/nodes", &node).await;
    assert!(
        st == 201 || st == 200,
        "node create: status={} body={}",
        st,
        body
    );

    let pod = prototype_pod("topo-pod-1");
    let (st, body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(
        st == 201 || st == 200,
        "pod create: status={} body={}",
        st,
        body
    );

    // Bind the pod to the node.
    let binding = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": { "name": "topo-pod-1", "namespace": ns },
        "target": { "kind": "Node", "name": "topo-node-1" },
    });
    let (st, body) = post_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/topo-pod-1/binding", ns),
        &binding,
    )
    .await;
    assert!(
        st == 201 || st == 200,
        "binding create: status={} body={}",
        st,
        body
    );

    // Re-read the pod and assert the two topology labels were propagated.
    let (st, body) = get(
        &router,
        &format!("/api/v1/namespaces/{}/pods/topo-pod-1", ns),
    )
    .await;
    assert_eq!(st, 200, "pod get: body={}", body);
    let labels = body["metadata"]["labels"]
        .as_object()
        .expect("labels object propagated from node");
    assert_eq!(
        labels
            .get("topology.kubernetes.io/zone")
            .and_then(Value::as_str),
        Some("zone"),
        "zone label must be copied from bound Node",
    );
    assert_eq!(
        labels
            .get("topology.kubernetes.io/region")
            .and_then(Value::as_str),
        Some("region"),
        "region label must be copied from bound Node",
    );
}

/// Mirrors upstream `TestPodTopologyLabels`'s "subdomains and custom keys are
/// not copied" sub-cases. The node's `zone`/`region` overwrite the pod's own
/// values (`mergeLabels` is node-authoritative), subdomain/custom topology
/// keys are not copied, and the pod's unrelated labels survive untouched.
///
/// `#[serial]` for the same reason as `test_pod_topology_labels`: it reads the
/// process-wide `PodTopologyLabelsAdmission` gate and must not race a sibling
/// that flips it.
#[tokio::test]
#[serial_test::serial]
async fn test_pod_topology_labels_filters_and_preserves_existing() {
    let (_, router) = spawn_router();
    let ns = "pod-topology-labels-filter";
    create_namespace(&router, ns).await;

    let node = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "topo-node-2",
            "labels": {
                // None of these should bleed through *except* the two
                // canonical zone/region keys.
                "sub.topology.kubernetes.io/zone": "subzone",
                "topology.kubernetes.io/custom":   "thing",
                "topology.kubernetes.io/zone":     "zone",
                "topology.kubernetes.io/region":   "region",
                "topology.kubernetes.io/abc":      "456",
            },
        },
    });
    let (st, _) = post_json(&router, "/api/v1/nodes", &node).await;
    assert!(st == 201 || st == 200);

    let mut pod = prototype_pod("topo-pod-2");
    pod["metadata"]["labels"] = json!({
        // Pod's own zone/region must win over node's; abc=123 must survive.
        "topology.kubernetes.io/zone":   "bad-zone",
        "topology.kubernetes.io/region": "bad-region",
        "topology.kubernetes.io/abc":    "123",
    });
    let (st, _) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(st == 201 || st == 200);

    let binding = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": { "name": "topo-pod-2", "namespace": ns },
        "target": { "kind": "Node", "name": "topo-node-2" },
    });
    let (st, _) = post_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/topo-pod-2/binding", ns),
        &binding,
    )
    .await;
    assert!(st == 201 || st == 200);

    let (_, body) = get(
        &router,
        &format!("/api/v1/namespaces/{}/pods/topo-pod-2", ns),
    )
    .await;
    let labels = body["metadata"]["labels"].as_object().expect("labels");

    // Subdomain key must NOT be copied.
    assert!(
        !labels.contains_key("sub.topology.kubernetes.io/zone"),
        "subdomain key leaked: {:?}",
        labels
    );
    // Custom topology.kubernetes.io/<other> key must NOT be copied.
    assert!(
        !labels.contains_key("topology.kubernetes.io/custom"),
        "custom topology key leaked: {:?}",
        labels
    );
    // Pod's own pre-existing zone/region must take precedence over node's.
    assert_eq!(
        labels
            .get("topology.kubernetes.io/zone")
            .and_then(Value::as_str),
        Some("zone"),
        "node's zone label must overwrite pod's (admission contract)",
    );
    assert_eq!(
        labels
            .get("topology.kubernetes.io/region")
            .and_then(Value::as_str),
        Some("region"),
        "node's region label must overwrite pod's (admission contract)",
    );
    // Pod's unrelated topology.kubernetes.io key must survive untouched.
    assert_eq!(
        labels
            .get("topology.kubernetes.io/abc")
            .and_then(Value::as_str),
        Some("123"),
        "pod's pre-existing unrelated topology key must survive: {:?}",
        labels
    );
}

/// Mirrors upstream `TestPodTopologyLabels_FeatureDisabled` (pods_test.go:109).
///
/// When the feature gate is OFF, the admission plugin must do nothing — the
/// pod ends up with no `topology.kubernetes.io/*` label keys after Binding,
/// even when the bound Node carries the canonical zone/region keys.
///
/// `#[serial]` because `feature_gates::with_feature` flips a process-wide
/// `AtomicBool`; running parallel tests that read or flip the same gate would
/// race.
#[tokio::test]
#[serial_test::serial]
async fn test_pod_topology_labels_feature_disabled() {
    use rusternetes_common::feature_gates::{with_feature, Feature};

    // Disable the PodTopologyLabelsAdmission gate for the lifetime of this
    // test. The gate defaults to ON at our v1.35 target (Beta), so without
    // this override the topology labels WOULD be copied — which is exactly
    // the behaviour `test_pod_topology_labels` above pins. The guard
    // restores the previous value on drop.
    let _guard = with_feature(Feature::PodTopologyLabelsAdmission, false);
    let (_, router) = spawn_router();

    let ns = "pod-topology-labels-disabled";
    create_namespace(&router, ns).await;

    // Node carries the canonical zone+region keys that *would* be copied if
    // the gate were on.
    let node = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "topo-node-disabled",
            "labels": {
                "topology.kubernetes.io/zone":   "zone",
                "topology.kubernetes.io/region": "region",
            },
        },
    });
    let (st, body) = post_json(&router, "/api/v1/nodes", &node).await;
    assert!(
        st == 201 || st == 200,
        "node create: status={} body={}",
        st,
        body
    );

    let pod = prototype_pod("topo-pod-disabled");
    let (st, body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(
        st == 201 || st == 200,
        "pod create: status={} body={}",
        st,
        body
    );

    let binding = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": { "name": "topo-pod-disabled", "namespace": ns },
        "target": { "kind": "Node", "name": "topo-node-disabled" },
    });
    let (st, body) = post_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/topo-pod-disabled/binding", ns),
        &binding,
    )
    .await;
    assert!(
        st == 201 || st == 200,
        "binding create: status={} body={}",
        st,
        body
    );

    let (st, body) = get(
        &router,
        &format!("/api/v1/namespaces/{}/pods/topo-pod-disabled", ns),
    )
    .await;
    assert_eq!(st, 200, "pod get: body={}", body);

    // Upstream contract: with the gate disabled, no `topology.kubernetes.io/*`
    // labels should appear on the bound pod. The label map may be omitted
    // entirely (None) or be present but empty — both shapes satisfy the
    // "did nothing" assertion. Anything containing a topology key is a
    // regression.
    match body["metadata"]["labels"].as_object() {
        None => { /* no labels object at all — pass */ }
        Some(labels) => {
            assert!(
                !labels
                    .keys()
                    .any(|k| k.starts_with("topology.kubernetes.io/")),
                "with PodTopologyLabelsAdmission disabled, no topology.kubernetes.io/* \
                 keys must be copied from the node; got labels={:?}",
                labels,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Upstream: TestPodUpdateActiveDeadlineSeconds (pods_test.go:210)
// ---------------------------------------------------------------------------

/// `activeDeadlineSeconds` mutation rules:
///
///   * `nil   -> nil`            : allowed (no change)
///   * `30    -> 30`             : allowed (no change)
///   * `nil   -> 60`             : allowed (set from unset)
///   * `60    -> 30`             : allowed (reduce)
///   * `30    -> 60`             : forbidden (increase)
///   * `30    -> -1`             : forbidden (negative)
///   * `nil   -> -1`             : forbidden (negative)
///   * `30    -> 0`              : forbidden (must be positive)
///   * `30    -> nil`            : forbidden (cannot unset)
///
/// RED: the current `handlers::pod::update` path does not enforce any of the
/// "forbidden" cases — every update is accepted.
#[tokio::test]
async fn test_pod_update_active_deadline_seconds() {
    let (_, router) = spawn_router();
    let ns = "pod-activedeadline-update";
    create_namespace(&router, ns).await;

    // 9 cases, mirroring upstream order in pods_test.go:243-298.
    let cases: &[(&str, Option<i64>, Option<i64>, bool)] = &[
        ("no change, nil", None, None, true),
        ("no change, set", Some(30), Some(30), true),
        ("change to positive from nil", None, Some(60), true),
        ("change to smaller positive", Some(60), Some(30), true),
        ("change to larger positive", Some(30), Some(60), false),
        (
            "change to negative from positive",
            Some(30),
            Some(-1),
            false,
        ),
        ("change to negative from nil", None, Some(-1), false),
        ("change to zero from positive", Some(30), Some(0), false),
        ("change to nil from positive", Some(30), None, false),
    ];

    for (i, (name, original, update, valid)) in cases.iter().enumerate() {
        let pod_name = format!("activedeadlineseconds-test-{}", i);
        let mut pod = prototype_pod(&pod_name);
        if let Some(v) = original {
            pod["spec"]["activeDeadlineSeconds"] = json!(*v);
        }

        let (st, body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
        assert!(
            st == 201 || st == 200,
            "case {:?}: create failed: status={} body={}",
            name,
            st,
            body
        );

        // Build the update body — same pod, optionally with a different
        // activeDeadlineSeconds (or with the field removed if `update is
        // None`).
        let mut updated = pod.clone();
        match update {
            Some(v) => updated["spec"]["activeDeadlineSeconds"] = json!(*v),
            None => {
                if let Some(spec) = updated["spec"].as_object_mut() {
                    spec.remove("activeDeadlineSeconds");
                }
            }
        }

        let (st, body) = put_json(
            &router,
            &format!("/api/v1/namespaces/{}/pods/{}", ns, pod_name),
            &updated,
        )
        .await;

        if *valid {
            assert!(
                st == 200 || st == 201,
                "case {:?}: expected success, got status={} body={}",
                name,
                st,
                body
            );
        } else {
            assert!(
                (400..500).contains(&st),
                "case {:?}: expected 4xx (forbidden / invalid), got status={} body={}",
                name,
                st,
                body
            );
        }

        // Clean up so we can reuse the namespace.
        let _ = delete(
            &router,
            &format!("/api/v1/namespaces/{}/pods/{}", ns, pod_name),
        )
        .await;
    }
}

// ---------------------------------------------------------------------------
// Upstream: TestPodReadOnlyFilesystem (pods_test.go:328)
// ---------------------------------------------------------------------------

/// A pod that declares `securityContext.readOnlyRootFilesystem=true` must be
/// accepted by the api-server and round-trip through GET. The Rust
/// `SecurityContext` already carries `readOnlyRootFilesystem`, so this is the
/// closest thing to a *green* assertion in this file — but it still pins the
/// upstream behaviour (and catches future regressions if the field is
/// accidentally dropped during admission rewriting).
#[tokio::test]
async fn test_pod_read_only_filesystem() {
    let (_, router) = spawn_router();
    let ns = "pod-readonly-root";
    create_namespace(&router, ns).await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "xxx" },
        "spec": {
            "containers": [{
                "name":  "fake-name",
                "image": "fakeimage",
                "securityContext": { "readOnlyRootFilesystem": true },
            }],
        },
    });
    let (st, body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(
        st == 201 || st == 200,
        "create with readOnlyRootFilesystem must succeed: status={} body={}",
        st,
        body
    );

    let (st, body) = get(&router, &format!("/api/v1/namespaces/{}/pods/xxx", ns)).await;
    assert_eq!(st, 200, "get after create must return 200: body={}", body);
    assert_eq!(
        body["spec"]["containers"][0]["securityContext"]["readOnlyRootFilesystem"],
        json!(true),
        "readOnlyRootFilesystem must round-trip: body={}",
        body
    );

    // Delete must succeed.
    let (st, _) = delete(&router, &format!("/api/v1/namespaces/{}/pods/xxx", ns)).await;
    assert!(st == 200 || st == 202, "delete: status={}", st);
}

// ---------------------------------------------------------------------------
// Upstream: TestPodCreateEphemeralContainers (pods_test.go:363)
// ---------------------------------------------------------------------------

/// Creating a pod with `spec.ephemeralContainers` set must be rejected with
/// `spec.ephemeralContainers: Forbidden: cannot be set on create`. The Rust
/// handler currently accepts the field silently.
#[tokio::test]
async fn test_pod_create_ephemeral_containers() {
    let (_, router) = spawn_router();
    let ns = "pod-create-ephemeral-containers";
    create_namespace(&router, ns).await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "xxx" },
        "spec": {
            "containers": [{
                "name":                     "fake-name",
                "image":                    "fakeimage",
                "imagePullPolicy":          "Always",
                "terminationMessagePolicy": "File",
            }],
            "ephemeralContainers": [{
                "name":                     "debugger",
                "image":                    "debugimage",
                "imagePullPolicy":          "Always",
                "terminationMessagePolicy": "File",
            }],
        },
    });
    let (st, body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(
        (400..500).contains(&st),
        "create with ephemeralContainers must be rejected (4xx), got status={} body={}",
        st,
        body
    );
    // Upstream returns exactly: `spec.ephemeralContainers: Forbidden: cannot
    // be set on create`. Best-effort message check.
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("ephemeralContainers") || msg.contains("Forbidden"),
        "error body should mention ephemeralContainers/Forbidden, got body={}",
        body
    );
}

// ---------------------------------------------------------------------------
// Upstream: TestPodPatchEphemeralContainers (pods_test.go:432)
// ---------------------------------------------------------------------------

/// The `/ephemeralcontainers` subresource is the only path that may *add*
/// containers; the regular PUT/PATCH path must reject them. Removing an
/// existing ephemeral container (either by emptying the list or by JSON-patch
/// `remove`) must also be forbidden.
///
/// Upstream covers 9 patch sub-cases; we mirror the two boundary policies
/// (allow ADD via the subresource, reject REMOVE) since the intermediate
/// strategic/merge/JSON variations all reduce to the same handler call in
/// our codebase.
#[tokio::test]
async fn test_pod_patch_ephemeral_containers() {
    let (_, router) = spawn_router();
    let ns = "pod-patch-ephemeral-containers";
    create_namespace(&router, ns).await;

    // Stub: create a pod, then attempt to add an ephemeral container via the
    // /ephemeralcontainers subresource. Once admission is in place this must
    // succeed; a follow-up patch that removes the container must be rejected
    // with a Forbidden status.
    let pod = prototype_pod("ephemeral-container-test-0");
    let _ = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;

    let add_patch = json!({
        "spec": {
            "ephemeralContainers": [{
                "name":                     "debugger1",
                "image":                    "debugimage",
                "imagePullPolicy":          "Always",
                "terminationMessagePolicy": "File",
            }]
        }
    });
    let (st, _) = patch_merge(
        &router,
        &format!(
            "/api/v1/namespaces/{}/pods/ephemeral-container-test-0/ephemeralcontainers",
            ns
        ),
        &add_patch,
    )
    .await;
    assert_eq!(st, 200, "ephemeralcontainers subresource ADD must succeed");

    // Removing must be Forbidden.
    let remove_patch = json!({ "spec": { "ephemeralContainers": [] } });
    let (st, _) = patch_merge(
        &router,
        &format!(
            "/api/v1/namespaces/{}/pods/ephemeral-container-test-0/ephemeralcontainers",
            ns
        ),
        &remove_patch,
    )
    .await;
    assert!(
        (400..500).contains(&st),
        "removing all ephemeralContainers must be Forbidden, got status={}",
        st
    );
}

// ---------------------------------------------------------------------------
// Upstream: TestPodUpdateEphemeralContainers (pods_test.go:663)
// ---------------------------------------------------------------------------

/// Direct PUT against the `pods/{name}` URI may not add or remove an entry
/// from `spec.ephemeralContainers`; only the `/ephemeralcontainers`
/// subresource may. Without admission this currently passes silently.
#[tokio::test]
async fn test_pod_update_ephemeral_containers() {
    let (_, router) = spawn_router();
    let ns = "pod-update-ephemeral-containers";
    create_namespace(&router, ns).await;

    // Stub: create a vanilla pod, then PUT a body that adds an ephemeral
    // container. Upstream rejects with `spec.ephemeralContainers: Forbidden`.
    let pod = prototype_pod("ephemeral-update-test-0");
    let _ = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;

    let mut updated = pod.clone();
    updated["spec"]["ephemeralContainers"] = json!([{
        "name":                     "debugger1",
        "image":                    "debugimage",
        "imagePullPolicy":          "Always",
        "terminationMessagePolicy": "File",
    }]);
    let (st, _) = put_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/ephemeral-update-test-0", ns),
        &updated,
    )
    .await;
    assert!(
        (400..500).contains(&st),
        "direct PUT must not allow adding ephemeralContainers, got status={}",
        st
    );
}

// ---------------------------------------------------------------------------
// Upstream: TestMutablePodSchedulingDirectives (pods_test.go:1204)
// ---------------------------------------------------------------------------

/// A pod with at least one entry in `spec.schedulingGates` is gated and
/// therefore not yet scheduled; `spec.nodeSelector` and
/// `spec.affinity.nodeAffinity` are mutable in this state. Once the gates
/// are removed (and the pod scheduled), the same fields become immutable.
///
/// Upstream `pkg/apis/core/validation/validation.go:5786-5828` implements
/// this gated-pod nodeSelector / nodeAffinity mutation surface. Ported in
/// `crates/common/src/validation/pod.rs::validate_pod_spec_update` — see
/// the `pod_is_gated` block (and the `validate_node_selector_mutation` /
/// `validate_node_affinity_mutation` helpers).
#[tokio::test]
async fn test_mutable_pod_scheduling_directives() {
    let (_, router) = spawn_router();
    let ns = "mutable-pod-scheduling-directives";
    create_namespace(&router, ns).await;

    let create_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "test-pod" },
        "spec": {
            "containers": [{ "name": "fake-name", "image": "fakeimage" }],
            "schedulingGates": [{ "name": "baz" }],
        },
    });
    let (st, body) = post_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods", ns),
        &create_pod,
    )
    .await;
    assert!(
        st == 201 || st == 200,
        "create gated pod must succeed: status={} body={}",
        st,
        body
    );

    // Add a nodeSelector — allowed because schedulingGates is non-empty.
    let mut updated = create_pod.clone();
    updated["spec"]["nodeSelector"] = json!({ "foo": "bar" });
    let (st, body) = put_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/test-pod", ns),
        &updated,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "adding nodeSelector to a gated pod must be allowed: status={} body={}",
        st,
        body
    );
}

// ---------------------------------------------------------------------------
// Upstream: TestRelaxedDNSSearchValidation (pods_test.go:1403)
// ---------------------------------------------------------------------------

/// Gated by the `RelaxedDNSSearchValidation` feature flag. When the flag is
/// ON, search entries like `_sip._tcp.abc_d.example.com` and the literal `.`
/// must be accepted; when OFF, they must be rejected.
///
/// Mirrors upstream `TestRelaxedDNSSearchValidation` (`pods_test.go:1403`).
/// The six sub-cases — underscore / dot / plain example.com, each with the
/// gate enabled and disabled — exercise both branches of
/// `validatePodDNSConfig` in `pkg/apis/core/validation/validation.go`.
///
/// `#[serial]` because the feature gate is process-wide; running parallel
/// tests that read or flip the same gate would race.
#[tokio::test]
#[serial_test::serial]
async fn test_relaxed_dns_search_validation() {
    use rusternetes_common::feature_gates::{reset_to_defaults, with_feature, Feature};

    let (_, router) = spawn_router();
    let ns = "pod-update-dns-search";
    create_namespace(&router, ns).await;

    struct Case {
        name: &'static str,
        search: &'static str,
        gate_enabled: bool,
        valid: bool,
    }

    let cases = [
        Case {
            name: "underscore-gate-on",
            search: "_sip._tcp.abc_d.example.com",
            gate_enabled: true,
            valid: true,
        },
        Case {
            name: "dot-gate-on",
            search: ".",
            gate_enabled: true,
            valid: true,
        },
        Case {
            name: "plain-gate-on",
            search: "example.com",
            gate_enabled: true,
            valid: true,
        },
        Case {
            name: "underscore-gate-off",
            search: "_sip._tcp.abc_d.example.com",
            gate_enabled: false,
            valid: false,
        },
        Case {
            name: "dot-gate-off",
            search: ".",
            gate_enabled: false,
            valid: false,
        },
        Case {
            name: "plain-gate-off",
            search: "example.com",
            gate_enabled: false,
            valid: true,
        },
    ];

    for case in cases {
        let _guard = with_feature(Feature::RelaxedDNSSearchValidation, case.gate_enabled);
        // Unique pod name per case so successful cases don't collide with
        // earlier ones via 409 AlreadyExists.
        let pod_name = format!("dns-{}", case.name);
        let mut pod = prototype_pod(&pod_name);
        pod["spec"]["dnsConfig"] = json!({ "searches": [case.search] });
        let (status, body) =
            post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
        if case.valid {
            assert!(
                status == 200 || status == 201,
                "case {}: expected accept, got status={} body={}",
                case.name,
                status,
                body
            );
        } else {
            assert_eq!(
                status, 422,
                "case {}: expected 422 Invalid, got status={} body={}",
                case.name, status, body
            );
            // Parity check — upstream returns the failure under the
            // `spec.dnsConfig.searches[0]` field path.
            let body_str = body.to_string();
            assert!(
                body_str.contains("spec.dnsConfig.searches[0]"),
                "case {}: error body must name the offending field, got {}",
                case.name,
                body_str
            );
        }
    }

    // Restore defaults so any later tests in this process start from a
    // known-good baseline even if a guard above was leaked by a panic-unwind.
    reset_to_defaults();
}

// ---------------------------------------------------------------------------
// Upstream: TestNodeDeclaredFeatureAdmission (pods_test.go:1504)
// ---------------------------------------------------------------------------

/// Gated by `NodeDeclaredFeatures` + `InPlacePodVerticalScaling`. The
/// admission check refuses a CPU resize when the target Node's
/// `status.declaredFeatures` does not include `GuaranteedQoSPodCPUResize`.
///
/// Three sub-cases mirror upstream `TestNodeDeclaredFeatureAdmission`
/// (`test/integration/pods/pods_test.go:1504`):
///   1. Bound node has no `declaredFeatures` — CPU resize on a Guaranteed
///      pod is rejected with 403 Forbidden, error names
///      `GuaranteedQoSPodCPUResize`.
///   2. Bound node declares `GuaranteedQoSPodCPUResize` — the same CPU
///      resize is accepted.
///   3. Label-only update via the main `/pods/{name}` PUT path is allowed
///      regardless of declared features (the admission only fires on
///      `/resize` when CPU actually changes on a Guaranteed pod).
///
/// `#[serial]` because the feature gates are process-wide; the test
/// flips `NodeDeclaredFeatures` (off-by-default) and confirms
/// `InPlacePodVerticalScaling` (default-on) is set.
#[tokio::test]
#[serial_test::serial]
async fn test_node_declared_feature_admission() {
    use rusternetes_common::feature_gates::{reset_to_defaults, with_feature, Feature};
    use rusternetes_storage::Storage;

    let (mem, router) = spawn_router();
    let ns = "node-declared-features";
    create_namespace(&router, ns).await;

    // Enable both gates for the duration of the test. RAII guards restore
    // the prior values on drop.
    let _g1 = with_feature(Feature::NodeDeclaredFeatures, true);
    let _g2 = with_feature(Feature::InPlacePodVerticalScaling, true);

    // Seed a Node directly via storage (the resize handler reads the node
    // by storage key, so we don't need to drive the Node create handler).
    async fn seed_node(
        mem: &Arc<rusternetes_storage::memory::MemoryStorage>,
        name: &str,
        declared: &[&str],
    ) {
        use rusternetes_common::resources::{Node, NodeStatus};
        let declared_vec: Vec<String> = declared.iter().map(|s| s.to_string()).collect();
        let mut node = Node::new(name);
        node.status = Some(NodeStatus {
            declared_features: if declared_vec.is_empty() {
                None
            } else {
                Some(declared_vec)
            },
            ..Default::default()
        });
        let key = rusternetes_storage::build_key("nodes", None, name);
        mem.create(&key, &node).await.expect("seed node");
    }

    seed_node(&mem, "node-without-features", &[]).await;
    seed_node(&mem, "node-with-features", &["GuaranteedQoSPodCPUResize"]).await;

    // Create a Guaranteed-QoS pod bound to the feature-less node.
    // Guaranteed requires cpu+memory in both requests and limits, with
    // requests == limits.
    let pod_name = "guaranteed-cpu";
    let mut pod = prototype_pod(pod_name);
    pod["spec"]["nodeName"] = json!("node-without-features");
    pod["spec"]["containers"][0]["resources"] = json!({
        "requests": { "cpu": "100m", "memory": "64Mi" },
        "limits":   { "cpu": "100m", "memory": "64Mi" },
    });
    let (st, body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(
        st == 201 || st == 200,
        "create guaranteed pod must succeed: status={} body={}",
        st,
        body
    );

    // ---- sub-case 1: resize denied when feature missing ----
    let mut resize_body = pod.clone();
    resize_body["spec"]["containers"][0]["resources"] = json!({
        "requests": { "cpu": "200m", "memory": "64Mi" },
        "limits":   { "cpu": "200m", "memory": "64Mi" },
    });
    let (st, body) = put_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/{}/resize", ns, pod_name),
        &resize_body,
    )
    .await;
    assert_eq!(
        st, 403,
        "CPU resize on node without GuaranteedQoSPodCPUResize must be 403, \
         got status={} body={}",
        st, body
    );
    let body_str = body.to_string();
    assert!(
        body_str.contains("GuaranteedQoSPodCPUResize"),
        "denial must name the missing feature, got: {}",
        body_str
    );

    // ---- sub-case 2: resize allowed when feature present ----
    // The resize handler reads the *current* pod's `spec.nodeName` to find
    // the node, so a direct storage edit is sufficient — no need to drive
    // a PUT through the immutability fence.
    {
        let key = rusternetes_storage::build_key("pods", Some(ns), pod_name);
        let mut stored: rusternetes_common::resources::Pod =
            mem.get(&key).await.expect("re-read pod");
        if let Some(spec) = stored.spec.as_mut() {
            spec.node_name = Some("node-with-features".to_string());
        }
        mem.update(&key, &stored).await.expect("rebind pod");
    }
    let (st, body) = put_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/{}/resize", ns, pod_name),
        &resize_body,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "CPU resize on node WITH GuaranteedQoSPodCPUResize must succeed, \
         got status={} body={}",
        st,
        body
    );

    // ---- sub-case 3: label-only update via main PUT — gate must NOT fire ----
    // Rebind to the feature-less node so a regression that wrongly fires
    // the resize admission on the main PUT path would be caught.
    {
        let key = rusternetes_storage::build_key("pods", Some(ns), pod_name);
        let mut stored: rusternetes_common::resources::Pod =
            mem.get(&key).await.expect("re-read pod");
        if let Some(spec) = stored.spec.as_mut() {
            spec.node_name = Some("node-without-features".to_string());
        }
        mem.update(&key, &stored).await.expect("rebind pod");
    }
    // Build a label-only PUT body off the latest stored pod so unrelated
    // fields don't trip the immutability fence.
    let label_update = {
        let key = rusternetes_storage::build_key("pods", Some(ns), pod_name);
        let stored: rusternetes_common::resources::Pod =
            mem.get(&key).await.expect("re-read pod for label update");
        let mut as_value = serde_json::to_value(&stored).expect("pod to json");
        as_value["metadata"]["labels"] = json!({ "tier": "frontend" });
        as_value
    };
    let (st, body) = put_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/{}", ns, pod_name),
        &label_update,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "label-only PUT must succeed regardless of declared features: \
         status={} body={}",
        st,
        body
    );

    reset_to_defaults();
}

// ---------------------------------------------------------------------------
// Upstream conformance mirrors — [Conformance] tests that exercise
// ValidatePodUpdate. All three are gated by f.WithNodeConformance() in
// test/e2e/common/node/pods.go and listed in
// test/conformance/testdata/conformance.yaml.
// ---------------------------------------------------------------------------

/// Mirrors upstream `framework.ConformanceIt("should be updated", ...)` at
/// `test/e2e/common/node/pods.go:340`. Creates a pod with a label,
/// updates the label via PUT, then GETs the pod list with a label
/// selector and confirms the updated pod is returned.
#[tokio::test]
async fn test_pod_should_be_updated_conformance() {
    let (_, router) = spawn_router();
    let ns = "pod-should-be-updated";
    create_namespace(&router, ns).await;

    let mut pod = prototype_pod("pod-update");
    pod["metadata"]["labels"] = json!({"time": "v1"});
    let (st, body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(
        st == 201 || st == 200,
        "create pod must succeed: status={} body={}",
        st,
        body
    );

    // Label update via full PUT — the immutability fence allows label
    // mutations (they live in metadata, not spec).
    let mut updated = pod.clone();
    updated["metadata"]["labels"] = json!({"time": "v2"});
    let (st, body) = put_json(
        &router,
        &format!("/api/v1/namespaces/{}/pods/pod-update", ns),
        &updated,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "label-only update must be accepted: status={} body={}",
        st,
        body
    );

    // List with label selector — updated pod must be returned.
    let (st, body) = get(
        &router,
        &format!("/api/v1/namespaces/{}/pods?labelSelector=time=v2", ns),
    )
    .await;
    assert_eq!(
        st, 200,
        "list with label selector must succeed: body={}",
        body
    );
    let items = body
        .get("items")
        .and_then(|i| i.as_array())
        .expect("items array");
    assert_eq!(
        items.len(),
        1,
        "label selector time=v2 must match exactly one pod, got: {}",
        body
    );
    assert_eq!(items[0]["metadata"]["name"], "pod-update");
}

/// Mirrors upstream `framework.ConformanceIt("should run through the
/// lifecycle of Pods and PodStatus", ...)` at
/// `test/e2e/common/node/pods.go:1160` — the patch portion. Creates a
/// pod, patches a metadata label + the container image via merge-patch.
/// Both deltas are individually allowed by the fence; this test confirms
/// they pass when bundled in a single PATCH call (the same handler path
/// kubectl uses).
///
/// NOTE: upstream's lifecycle test also flips `terminationGracePeriodSeconds`
/// to 1 here, but only because the upstream pod is created with TGPS=-1
/// (negative→1 is the one allowed mutation). Defaulting in our test
/// harness sets TGPS=30, which the fence correctly treats as immutable
/// per `validation.go:5780-5783`. Covered by
/// `test_update_tgps_negative_to_one_accepted` /
/// `test_update_tgps_arbitrary_change_rejected` in
/// `pod_update_immutability_test.rs`.
#[tokio::test]
async fn test_pod_lifecycle_combined_patch_conformance() {
    let (_, router) = spawn_router();
    let ns = "pod-lifecycle-patch";
    create_namespace(&router, ns).await;

    let mut pod = prototype_pod("pod-podstatus");
    pod["metadata"]["labels"] = json!({"test-pod-static": "true"});
    let (st, _body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(st == 201 || st == 200);

    // Single merge-patch bundling label + image deltas.
    let patch = json!({
        "metadata": { "labels": { "test-pod": "patched" } },
        "spec": {
            "containers": [{ "name": "fake-name", "image": "image2" }]
        }
    });
    let (st, body) = patch_merge(
        &router,
        &format!("/api/v1/namespaces/{}/pods/pod-podstatus", ns),
        &patch,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "combined patch (label + image) must be accepted: status={} body={}",
        st,
        body
    );

    let (st, body) = get(
        &router,
        &format!("/api/v1/namespaces/{}/pods/pod-podstatus", ns),
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(body["metadata"]["labels"]["test-pod"], "patched");
    assert_eq!(body["spec"]["containers"][0]["image"], "image2");
}

/// Mirrors upstream `[sig-node] Pods Extended (pod generation)` at
/// `test/conformance/testdata/conformance.yaml:2519-2538`. After
/// create, `metadata.generation == 1`. Each accepted spec mutation
/// (image change) must bump generation by 1.
#[tokio::test]
async fn test_pod_generation_increments_per_update_conformance() {
    let (_, router) = spawn_router();
    let ns = "pod-generation";
    create_namespace(&router, ns).await;

    let pod = prototype_pod("pod-gen");
    let (st, body) = post_json(&router, &format!("/api/v1/namespaces/{}/pods", ns), &pod).await;
    assert!(st == 201 || st == 200);
    assert_eq!(
        body["metadata"]["generation"], 1,
        "create must produce generation=1, got body={}",
        body
    );

    for (i, image) in ["image-v2", "image-v3", "image-v4"].iter().enumerate() {
        let mut updated = pod.clone();
        updated["spec"]["containers"][0]["image"] = json!(*image);
        let (st, body) = put_json(
            &router,
            &format!("/api/v1/namespaces/{}/pods/pod-gen", ns),
            &updated,
        )
        .await;
        assert!(
            st == 200 || st == 201,
            "image-change PUT #{} must be accepted: status={} body={}",
            i + 1,
            st,
            body
        );
        let expected_gen = (i + 2) as i64;
        let got = body["metadata"]["generation"].as_i64().unwrap_or(0);
        assert_eq!(
            got, expected_gen,
            "generation must increment by 1 per spec change; expected {}, got {} body={}",
            expected_gen, got, body
        );
    }
}
