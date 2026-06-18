//! RED-state TDD mirror of upstream Kubernetes integration tests for the
//! Pod `eviction` subresource.
//!
//! Source: https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/evictions/evictions_test.go
//!
//! Each `#[tokio::test]` below preserves the upstream `TestX` name and pins
//! one behavior the eviction endpoint must satisfy. The HTTP layer is
//! exercised through tower's `oneshot`, identical to the harness used in
//! `patch_cas_retry_test.rs` and `conformance_apimachinery_admission_webhooks.rs`:
//! an `Arc<MemoryStorage>` is wrapped in `StorageBackend::Memory`, the router
//! is built via `build_router`, and the eviction request is `POST`ed to
//! `/api/v1/namespaces/{ns}/pods/{pod}/eviction`.
//!
//! Upstream `Test*` (6 functions) → Rust translations:
//!   1. TestConcurrentEvictionRequests           → red (PDB+race semantics not implemented)
//!   2. TestTerminalPodEviction                  → red (terminal-phase PDB bypass missing)
//!   3. TestEvictionVersions                     → red (v1beta1 + method-not-allowed not wired)
//!   4. TestEvictionWithFinalizers               → red (DisruptionTarget condition not emitted)
//!   5. TestEvictionWithUnhealthyPodEvictionPolicy → red (unhealthyPodEvictionPolicy unsupported)
//!   6. TestEvictionWithPrecondition             → red (deleteOptions preconditions ignored)
//!
//! Status legend:
//!   * **red** — test asserts the upstream behavior; the assertion currently
//!     fails because the handler returns the wrong status / shape / side effect.
//!
//! Part of the `/batch` landing upstream integration-test mirrors as RED-state
//! TDD pins. When a feature lands, drop the `#[ignore]` on the green
//! assertion and delete the red one — never weaken the upstream assertion.

use axum::http::StatusCode;
use rusternetes_common::resources::{
    Container, IntOrString, Pod, PodDisruptionBudget, PodDisruptionBudgetSpec, PodSpec, PodStatus,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

/// `(storage, router)` factory — each test owns its own backend so they
/// remain trivially parallelizable.
fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// POST JSON helper for the eviction subresource.
async fn post_eviction(
    router: TestApiServer,
    namespace: &str,
    name: &str,
    body: &Value,
) -> (StatusCode, Value) {
    let uri = format!("/api/v1/namespaces/{}/pods/{}/eviction", namespace, name);
    router
        .send("POST", &uri, Some("application/json"), Some(body))
        .await
}

/// Generic method dispatcher for `TestEvictionVersions` — we need to verify
/// non-POST verbs return MethodNotAllowed.
async fn request_eviction(
    router: TestApiServer,
    method: &str,
    namespace: &str,
    name: &str,
    body: Option<&Value>,
) -> StatusCode {
    let uri = format!("/api/v1/namespaces/{}/pods/{}/eviction", namespace, name);
    let content_type = body.as_ref().map(|_| "application/json");
    router.send(method, &uri, content_type, body).await.0
}

// ---------------------------------------------------------------------------
// Fixture builders.
// ---------------------------------------------------------------------------

fn pod_with_labels(name: &str, namespace: &str, labels: HashMap<String, String>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            labels: if labels.is_empty() {
                None
            } else {
                Some(labels)
            },
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:latest".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        }),
    }
}

fn pdb_with_selector(
    name: &str,
    namespace: &str,
    min_available: i32,
    match_labels: HashMap<String, String>,
) -> PodDisruptionBudget {
    PodDisruptionBudget {
        type_meta: TypeMeta {
            api_version: "policy/v1".to_string(),
            kind: "PodDisruptionBudget".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
        status: None,
    }
}

/// Build a `policy/v1` Eviction body matching upstream `policy.Eviction{...}`.
fn eviction_body_v1(pod_name: &str, namespace: &str) -> Value {
    json!({
        "apiVersion": "policy/v1",
        "kind": "Eviction",
        "metadata": {
            "name": pod_name,
            "namespace": namespace,
        }
    })
}

// ---------------------------------------------------------------------------
// 1. TestConcurrentEvictionRequests
// ---------------------------------------------------------------------------

/// Upstream: TestConcurrentEvictionRequests
/// (https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/evictions/evictions_test.go)
///
/// Creates `N` Running pods + a PDB with `minAvailable: 0`, then races `N`
/// concurrent eviction POSTs. Upstream expects:
///   * Every POST eventually returns 200 (no 409 Conflict leakage).
///   * All pods are deleted at the end.
///   * The PDB controller does not deadlock under the concurrent CAS load.
///
/// RED state: our handler doesn't perform a proper CAS retry on the PDB
/// status update and doesn't decrement `disruptionsAllowed` atomically across
/// concurrent evictions, so this races today.
#[tokio::test]
async fn test_concurrent_eviction_requests() {
    let (mem, router) = spawn_router();
    let ns = "concurrent-eviction-ns";
    const N: usize = 10;

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "evict-me".to_string());

    // Seed PDB allowing arbitrary disruptions.
    let pdb = pdb_with_selector("pdb-concurrent", ns, 0, labels.clone());
    mem.create(
        &build_key("poddisruptionbudgets", Some(ns), "pdb-concurrent"),
        &pdb,
    )
    .await
    .unwrap();

    // Seed N matching running pods.
    let mut pod_names = Vec::with_capacity(N);
    for i in 0..N {
        let name = format!("pod-{}", i);
        let pod = pod_with_labels(&name, ns, labels.clone());
        mem.create(&build_key("pods", Some(ns), &name), &pod)
            .await
            .unwrap();
        pod_names.push(name);
    }

    // Race N concurrent evictions.
    let mut handles = Vec::with_capacity(N);
    for name in pod_names.clone() {
        let r = router.clone();
        let ns = ns.to_string();
        handles.push(tokio::spawn(async move {
            let body = eviction_body_v1(&name, &ns);
            post_eviction(r, &ns, &name, &body).await
        }));
    }
    let mut success = 0;
    for h in handles {
        let (status, _body) = h.await.unwrap();
        if status == StatusCode::OK || status == StatusCode::CREATED {
            success += 1;
        }
    }

    assert_eq!(
        success, N,
        "all {} concurrent evictions must succeed (200/201) — upstream \
         TestConcurrentEvictionRequests forbids 409/429 leakage when PDB \
         minAvailable=0",
        N,
    );

    // Verify every pod is gone.
    for name in pod_names {
        let key = build_key("pods", Some(ns), &name);
        let got: Result<Pod, _> = mem.get(&key).await;
        assert!(
            got.is_err(),
            "pod {} must be deleted after eviction (got Ok)",
            name
        );
    }
}

// ---------------------------------------------------------------------------
// 2. TestTerminalPodEviction
// ---------------------------------------------------------------------------

/// Upstream: TestTerminalPodEviction
///
/// A pod in a terminal phase (`Succeeded` or `Failed`) must be evictable even
/// when the PDB would otherwise block disruption. Upstream additionally
/// asserts the PDB `status.observedGeneration` is **not** bumped (i.e. the
/// PDB controller short-circuits and never evaluates the PDB).
///
/// RED state: our handler currently runs the full PDB check regardless of
/// pod phase, so a Succeeded pod under a tight PDB is denied with 429.
#[tokio::test]
async fn test_terminal_pod_eviction() {
    let (mem, router) = spawn_router();
    let ns = "terminal-eviction-ns";

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "terminal".to_string());

    // Tight PDB: minAvailable=1 covering exactly the one pod we are evicting.
    let pdb = pdb_with_selector("pdb-terminal", ns, 1, labels.clone());
    mem.create(
        &build_key("poddisruptionbudgets", Some(ns), "pdb-terminal"),
        &pdb,
    )
    .await
    .unwrap();

    // Pod in Succeeded (terminal) phase.
    let mut pod = pod_with_labels("term-pod", ns, labels.clone());
    pod.status = Some(PodStatus {
        phase: Some(Phase::Succeeded),
        ..Default::default()
    });
    mem.create(&build_key("pods", Some(ns), "term-pod"), &pod)
        .await
        .unwrap();

    let body = eviction_body_v1("term-pod", ns);
    let (status, resp_body) = post_eviction(router, ns, "term-pod", &body).await;

    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "terminal pods must bypass PDB and evict with 200/201 \
         (got {}, body={})",
        status,
        resp_body,
    );

    // Pod must be gone.
    let key = build_key("pods", Some(ns), "term-pod");
    let got: Result<Pod, _> = mem.get(&key).await;
    assert!(got.is_err(), "terminal pod must be deleted after eviction");
}

// ---------------------------------------------------------------------------
// 3. TestEvictionVersions
// ---------------------------------------------------------------------------

/// Upstream: TestEvictionVersions
///
/// Verifies the eviction endpoint:
///   * Accepts both `policy/v1` and `policy/v1beta1` Eviction bodies (with
///     or without explicit `apiVersion`/`kind`).
///   * Rejects unknown versions (e.g. `policy/v2`) with a 4xx.
///   * Returns 405 MethodNotAllowed for GET / PATCH / PUT (only POST is
///     allowed).
///
/// RED state: our router only accepts `policy/v1` shape and doesn't enforce
/// a 405 on non-POST verbs (they 404).
#[tokio::test]
async fn test_eviction_versions() {
    let (mem, router) = spawn_router();
    let ns = "eviction-versions-ns";

    // Seed a single Running pod with no PDB so the only thing under test is
    // body-version validation.
    let pod = pod_with_labels("v-pod", ns, HashMap::new());
    mem.create(&build_key("pods", Some(ns), "v-pod"), &pod)
        .await
        .unwrap();

    // (a) policy/v1 — must succeed.
    let body_v1 = json!({
        "apiVersion": "policy/v1",
        "kind": "Eviction",
        "metadata": {"name": "v-pod", "namespace": ns},
    });
    let (s_v1, _) = post_eviction(router.clone(), ns, "v-pod", &body_v1).await;
    assert!(
        s_v1 == StatusCode::OK || s_v1 == StatusCode::CREATED,
        "policy/v1 Eviction must succeed (got {})",
        s_v1,
    );

    // Re-seed the pod so subsequent versions have a target.
    mem.create(
        &build_key("pods", Some(ns), "v-pod-beta"),
        &pod_with_labels("v-pod-beta", ns, HashMap::new()),
    )
    .await
    .unwrap();

    // (b) policy/v1beta1 — must succeed (upstream still accepts the legacy
    // version on the eviction endpoint).
    let body_beta = json!({
        "apiVersion": "policy/v1beta1",
        "kind": "Eviction",
        "metadata": {"name": "v-pod-beta", "namespace": ns},
    });
    let (s_beta, _) = post_eviction(router.clone(), ns, "v-pod-beta", &body_beta).await;
    assert!(
        s_beta == StatusCode::OK || s_beta == StatusCode::CREATED,
        "policy/v1beta1 Eviction must succeed (got {}) — upstream \
         TestEvictionVersions explicitly covers v1beta1",
        s_beta,
    );

    // (c) policy/v2 — must reject with a 4xx (not 500, not silent 200).
    mem.create(
        &build_key("pods", Some(ns), "v-pod-bad"),
        &pod_with_labels("v-pod-bad", ns, HashMap::new()),
    )
    .await
    .unwrap();
    let body_bad = json!({
        "apiVersion": "policy/v2",
        "kind": "Eviction",
        "metadata": {"name": "v-pod-bad", "namespace": ns},
    });
    let (s_bad, body_bad_resp) = post_eviction(router.clone(), ns, "v-pod-bad", &body_bad).await;
    assert!(
        s_bad.is_client_error(),
        "unknown Eviction apiVersion must return 4xx (got {}, body={})",
        s_bad,
        body_bad_resp,
    );

    // (d) Non-POST verbs must return 405 MethodNotAllowed.
    let s_get = request_eviction(router.clone(), "GET", ns, "v-pod-bad", None).await;
    assert_eq!(
        s_get,
        StatusCode::METHOD_NOT_ALLOWED,
        "GET on /pods/{{name}}/eviction must be 405 (got {})",
        s_get,
    );
    let s_patch =
        request_eviction(router, "PATCH", ns, "v-pod-bad", Some(&json!({"spec": {}}))).await;
    assert_eq!(
        s_patch,
        StatusCode::METHOD_NOT_ALLOWED,
        "PATCH on /pods/{{name}}/eviction must be 405 (got {})",
        s_patch,
    );
}

// ---------------------------------------------------------------------------
// 4. TestEvictionWithFinalizers
// ---------------------------------------------------------------------------

/// Upstream: TestEvictionWithFinalizers
///
/// When a pod carries a finalizer and is evicted, upstream asserts:
///   * The eviction POST returns 200 (or 201).
///   * The pod is **not** removed (the finalizer holds it).
///   * A `DisruptionTarget` condition is appended to `pod.status.conditions`
///     with `status=True` and `reason=EvictionByEvictionAPI`.
///   * Dry-run evictions (`?dryRun=All`) must skip the condition update.
///
/// RED state: our handler today calls `storage.delete` unconditionally —
/// no finalizer respect, no DisruptionTarget condition emission.
#[tokio::test]
async fn test_eviction_with_finalizers() {
    let (mem, router) = spawn_router();
    let ns = "eviction-finalizers-ns";

    let mut pod = pod_with_labels("fin-pod", ns, HashMap::new());
    pod.metadata.finalizers = Some(vec!["example.com/keep-me".to_string()]);
    mem.create(&build_key("pods", Some(ns), "fin-pod"), &pod)
        .await
        .unwrap();

    let body = eviction_body_v1("fin-pod", ns);
    let (status, resp_body) = post_eviction(router, ns, "fin-pod", &body).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "eviction with finalizers must return 200/201 (got {}, body={})",
        status,
        resp_body,
    );

    // Pod must still exist (finalizer blocks physical delete).
    let key = build_key("pods", Some(ns), "fin-pod");
    let after: Pod = mem
        .get(&key)
        .await
        .expect("pod with finalizer must survive eviction");

    // DisruptionTarget condition must be present.
    let conds = after
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .cloned()
        .unwrap_or_default();
    let dt = conds
        .iter()
        .find(|c| c.condition_type == "DisruptionTarget");
    assert!(
        dt.is_some(),
        "DisruptionTarget condition must be added to pod.status.conditions \
         after eviction; got conditions={:?}",
        conds,
    );
    let dt = dt.unwrap();
    assert_eq!(
        dt.status, "True",
        "DisruptionTarget.status must be True (got {:?})",
        dt.status,
    );
    assert_eq!(
        dt.reason.as_deref(),
        Some("EvictionByEvictionAPI"),
        "DisruptionTarget.reason must be EvictionByEvictionAPI (got {:?})",
        dt.reason,
    );
}

// ---------------------------------------------------------------------------
// 5. TestEvictionWithUnhealthyPodEvictionPolicy
// ---------------------------------------------------------------------------

/// Upstream: TestEvictionWithUnhealthyPodEvictionPolicy
///
/// PDB v1 introduced `spec.unhealthyPodEvictionPolicy`. When set to
/// `AlwaysAllow`, evicting a *not-yet-Ready* pod must succeed on the first
/// POST even if the PDB would normally block (i.e. unhealthy pods are
/// considered freely disruptable).
///
/// RED state: our PDB struct accepts the field, but the eviction handler
/// ignores it — unhealthy pods are still counted toward `currentHealthy`,
/// so the eviction is denied with 429.
#[tokio::test]
async fn test_eviction_with_unhealthy_pod_eviction_policy() {
    let (mem, router) = spawn_router();
    let ns = "eviction-uppolicy-ns";

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "unhealthy".to_string());

    // PDB with minAvailable=1, AlwaysAllow on unhealthy.
    let mut pdb = pdb_with_selector("pdb-uppolicy", ns, 1, labels.clone());
    pdb.spec.unhealthy_pod_eviction_policy = Some("AlwaysAllow".to_string());
    mem.create(
        &build_key("poddisruptionbudgets", Some(ns), "pdb-uppolicy"),
        &pdb,
    )
    .await
    .unwrap();

    // Two matching pods: one Running, one Pending (unhealthy).
    let healthy = pod_with_labels("healthy-pod", ns, labels.clone());
    mem.create(&build_key("pods", Some(ns), "healthy-pod"), &healthy)
        .await
        .unwrap();

    let mut unhealthy = pod_with_labels("unhealthy-pod", ns, labels.clone());
    unhealthy.status = Some(PodStatus {
        phase: Some(Phase::Pending),
        ..Default::default()
    });
    mem.create(&build_key("pods", Some(ns), "unhealthy-pod"), &unhealthy)
        .await
        .unwrap();

    let body = eviction_body_v1("unhealthy-pod", ns);
    let (status, resp_body) = post_eviction(router, ns, "unhealthy-pod", &body).await;

    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "AlwaysAllow unhealthyPodEvictionPolicy must let unhealthy pod evict \
         on the FIRST POST (got {}, body={})",
        status,
        resp_body,
    );

    let key = build_key("pods", Some(ns), "unhealthy-pod");
    assert!(
        mem.get::<Pod>(&key).await.is_err(),
        "unhealthy-pod must be gone after AlwaysAllow eviction",
    );
}

// ---------------------------------------------------------------------------
// 6. TestEvictionWithPrecondition
// ---------------------------------------------------------------------------

/// Upstream: TestEvictionWithPrecondition
///
/// The Eviction body's `deleteOptions.preconditions` (uid + resourceVersion)
/// must be honored by the endpoint:
///   * Matching preconditions → 200/201.
///   * Mismatching uid → the request fails (upstream surfaces a Conflict or
///     Invalid error) and the pod is **not** deleted.
///
/// RED state: our handler reads `deleteOptions.gracePeriodSeconds` but
/// silently ignores `deleteOptions.preconditions`, so a bogus uid still
/// succeeds in deleting the pod.
#[tokio::test]
async fn test_eviction_with_precondition() {
    let (mem, router) = spawn_router();
    let ns = "eviction-precond-ns";

    let pod = pod_with_labels("pre-pod", ns, HashMap::new());
    let real_uid = pod.metadata.uid.clone();
    mem.create(&build_key("pods", Some(ns), "pre-pod"), &pod)
        .await
        .unwrap();

    // Case 1: matching uid precondition succeeds.
    let body_ok = json!({
        "apiVersion": "policy/v1",
        "kind": "Eviction",
        "metadata": {"name": "pre-pod", "namespace": ns},
        "deleteOptions": {
            "preconditions": {"uid": real_uid},
        }
    });
    let (s_ok, _) = post_eviction(router.clone(), ns, "pre-pod", &body_ok).await;
    assert!(
        s_ok == StatusCode::OK || s_ok == StatusCode::CREATED,
        "matching uid precondition must succeed (got {})",
        s_ok,
    );

    // Re-seed pod for the mismatch case.
    let pod2 = pod_with_labels("pre-pod-2", ns, HashMap::new());
    mem.create(&build_key("pods", Some(ns), "pre-pod-2"), &pod2)
        .await
        .unwrap();

    // Case 2: mismatched uid precondition must fail AND leave the pod alive.
    let body_bad = json!({
        "apiVersion": "policy/v1",
        "kind": "Eviction",
        "metadata": {"name": "pre-pod-2", "namespace": ns},
        "deleteOptions": {
            "preconditions": {"uid": "00000000-0000-0000-0000-000000000000"},
        }
    });
    let (s_bad, body_bad_resp) = post_eviction(router, ns, "pre-pod-2", &body_bad).await;
    assert!(
        s_bad.is_client_error(),
        "mismatched uid precondition must return 4xx (got {}, body={})",
        s_bad,
        body_bad_resp,
    );

    let key = build_key("pods", Some(ns), "pre-pod-2");
    assert!(
        mem.get::<Pod>(&key).await.is_ok(),
        "pod must survive a failed precondition check",
    );
}
