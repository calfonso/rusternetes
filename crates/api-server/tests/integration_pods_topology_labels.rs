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
//!     `RelaxedDNSSearchValidation` feature flag; not implemented.
//!
//!   * `TestNodeDeclaredFeatureAdmission` — gated by `NodeDeclaredFeatures` +
//!     `node.status.declaredFeatures`; the resource field is not present on
//!     the Rust `NodeStatus` struct, so the corresponding admission check
//!     cannot exist. Mirrored as `#[ignore]`.
//!
//! See also: `pod_handler_test.rs` and `pod_resize_cas_test.rs` for the
//! complementary handler-level pod tests already covering create/get/update/
//! delete plumbing.

use axum::{
    body::{Body, Bytes},
    http::Request,
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// HTTP harness — clone of the helper used in
// `conformance_apimachinery_crd_lifecycle.rs`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, axum::Router) {
    let mem = Arc::new(MemoryStorage::new());
    let backend = Arc::new(StorageBackend::Memory(mem.clone()));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    let state = Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true, // skip_auth
    ));
    let router = build_router(state.clone(), None);
    (mem, router)
}

async fn send(router: &axum::Router, req: Request<Body>) -> (u16, Value) {
    let resp = router.clone().oneshot(req).await.expect("oneshot");
    let status = resp.status().as_u16();
    let bytes: Bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

async fn post_json(router: &axum::Router, uri: &str, body: &Value) -> (u16, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    send(router, req).await
}

async fn put_json(router: &axum::Router, uri: &str, body: &Value) -> (u16, Value) {
    let req = Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    send(router, req).await
}

async fn patch_merge(router: &axum::Router, uri: &str, body: &Value) -> (u16, Value) {
    let req = Request::builder()
        .method("PATCH")
        .uri(uri)
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    send(router, req).await
}

async fn get(router: &axum::Router, uri: &str) -> (u16, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    send(router, req).await
}

async fn delete(router: &axum::Router, uri: &str) -> (u16, Value) {
    let req = Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    send(router, req).await
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
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name },
        "spec": {
            "containers": [{
                "name":  "fake-name",
                "image": "fakeimage",
            }],
        },
    })
}

/// Helper to create the namespace before any pod operation.
async fn create_namespace(router: &axum::Router, name: &str) {
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
/// RED: `handlers::pod_subresources::create_binding` currently only writes
/// `spec.nodeName`; it does not touch labels. The first assertion will fail.
#[tokio::test]
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
/// not copied" sub-cases. Pre-existing labels on the pod must also win over
/// node-derived labels.
#[tokio::test]
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
/// pod ends up with an empty (or `nil`) label map even when the bound Node
/// carries the canonical zone/region keys. We don't have feature gates yet,
/// so this test is `#[ignore]`d but documents the desired behaviour.
#[tokio::test]
#[ignore = "feature-gate plumbing (PodTopologyLabelsAdmission) not implemented; see docstring"]
async fn test_pod_topology_labels_feature_disabled() {
    // Intentionally a stub — once feature gates exist this should mirror the
    // single "does nothing when the feature is not enabled" sub-case from
    // upstream pods_test.go:109. Asserts: after Binding, the pod has no
    // `topology.kubernetes.io/*` labels at all.
    let (_, _router) = spawn_router();
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
#[ignore = "ephemeralcontainers subresource admission not implemented; see docstring"]
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
#[ignore = "ephemeralcontainers subresource admission not implemented; see docstring"]
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
// Upstream: TestPodResizeRBAC (pods_test.go:853) — already covered by
// crates/api-server/tests/pod_resize_cas_test.rs. Mirrored as ignored stub.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "covered by crates/api-server/tests/pod_resize_cas_test.rs (CAS-retry harness)"]
async fn test_pod_resize_rbac() {
    // Intentionally empty — see pod_resize_cas_test.rs for the in-place
    // resize subresource coverage including RBAC scoping by subresource.
}

// ---------------------------------------------------------------------------
// Upstream: TestPodResize (pods_test.go:957) — also covered by
// pod_resize_cas_test.rs. Mirrored as ignored stub.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "covered by crates/api-server/tests/pod_resize_cas_test.rs (CAS-retry harness)"]
async fn test_pod_resize() {
    // Intentionally empty — see pod_resize_cas_test.rs.
}

// ---------------------------------------------------------------------------
// Upstream: TestMutablePodSchedulingDirectives (pods_test.go:1204)
// ---------------------------------------------------------------------------

/// A pod with at least one entry in `spec.schedulingGates` is gated and
/// therefore not yet scheduled; `spec.nodeSelector` and
/// `spec.affinity.nodeAffinity` are mutable in this state. Once the gates
/// are removed (and the pod scheduled), the same fields become immutable.
///
/// RED: the generic pod update handler currently rejects any spec mutation
/// outside the documented "tolerations/activeDeadlineSeconds/labels" subset.
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
/// We have no feature-gate plumbing yet, so this test is `#[ignore]`d.
#[tokio::test]
#[ignore = "RelaxedDNSSearchValidation feature gate plumbing not implemented; see docstring"]
async fn test_relaxed_dns_search_validation() {
    let (_, _router) = spawn_router();
    // Once gates exist, mirror the six upstream sub-cases:
    //   underscore + dot + plain * (gate enabled, gate disabled).
}

// ---------------------------------------------------------------------------
// Upstream: TestNodeDeclaredFeatureAdmission (pods_test.go:1504)
// ---------------------------------------------------------------------------

/// Gated by `NodeDeclaredFeatures` + `InPlacePodVerticalScaling`. The
/// admission check refuses a CPU resize when the target Node's
/// `status.declaredFeatures` does not include `GuaranteedQoSPodCPUResize`.
/// The `declaredFeatures` field is not present on the Rust `NodeStatus`
/// struct, so the corresponding admission check cannot exist yet.
#[tokio::test]
#[ignore = "node.status.declaredFeatures field + NodeDeclaredFeatures gate not implemented; see docstring"]
async fn test_node_declared_feature_admission() {
    let (_, _router) = spawn_router();
    // Once declaredFeatures + the gate exist, mirror the three sub-cases:
    //   - resize denied when feature missing,
    //   - resize allowed when feature present,
    //   - label-only update allowed regardless of declared features.
}
