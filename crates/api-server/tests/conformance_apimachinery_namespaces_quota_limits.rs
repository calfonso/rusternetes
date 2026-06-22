//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-api-machinery] Namespaces + ResourceQuota + LimitRange.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//! Specifically:
//!   - test/e2e/apimachinery/namespace.go
//!   - test/e2e/apimachinery/resource_quota.go
//!   - test/e2e/apimachinery/limit_range.go
//!
//! See `docs/conformance/apimachinery-namespaces-quota-limits.md` for the
//! test-by-test status table and the cross-reference into `docs/CONFORMANCE.md`
//! (Round 160 "Other" bucket — ResourceQuota pod lifecycle).
//!
//! The known Round-160 failure
//!   `[sig-api-machinery] ResourceQuota should create a ResourceQuota and
//!    capture the life of a pod. [Conformance]`
//! (e2e.log:15500 → upstream `resource_quota.go:312`) was addressed by
//! PR #45 (`fix(conformance): ResourceQuota usage recompute on object
//! delete`, commit `748241cf`). This file re-verifies the controller path
//! the fix added (`reconcile_one()` on `ResourceQuotaController`) and the
//! REST-surface contract for ResourceQuota + LimitRange + Namespace so
//! either side of the regression is caught locally in <1s of `cargo test`.
//!
//! All non-`#[ignore]`d tests mirror Sonobuoy-passing scenarios. The
//! upstream `resource_quota.go:312` failure is mirrored here as a
//! `#[ignore]`d test that documents the failure mode — the rest of the
//! file demonstrates each individual lifecycle step still works.

use rusternetes_common::resources::{
    Container, LimitRange, LimitRangeItem, LimitRangeSpec, Pod, PodSpec, PodStatus, ResourceQuota,
    ResourceQuotaSpec,
};
use rusternetes_common::types::{ObjectMeta, Phase, ResourceRequirements, TypeMeta};
use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `mem` is the
// backing store so tests seed/inspect storage and drive the quota controller.
// ---------------------------------------------------------------------------

fn spawn_router() -> (TestApiServer, Arc<MemoryStorage>) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (api, mem)
}

/// Issue a request and return `(status, parsed body)`.
/// Pass `body=None` for verb-only requests (GET/DELETE); otherwise the body
/// is JSON-encoded with the given `content_type`.
async fn send(
    router: TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
    content_type: &str,
) -> (u16, Value) {
    let (status, value) = router.send(method, uri, Some(content_type), body).await;
    (status.as_u16(), value)
}

async fn send_json(
    router: TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    send(router, method, uri, body, "application/json").await
}

async fn send_patch(
    router: TestApiServer,
    uri: &str,
    body: &Value,
    content_type: &str,
) -> (u16, Value) {
    send(router, "PATCH", uri, Some(body), content_type).await
}

// ---------------------------------------------------------------------------
// Body helpers
// ---------------------------------------------------------------------------

fn ns_body(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name }
    })
}

fn quota_body(name: &str, hard: &[(&str, &str)]) -> Value {
    let mut hard_map = serde_json::Map::new();
    for (k, v) in hard {
        hard_map.insert((*k).to_string(), json!(*v));
    }
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": name },
        "spec": { "hard": Value::Object(hard_map) }
    })
}

fn limitrange_body(name: &str, item: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": { "name": name },
        "spec": { "limits": [item] }
    })
}

/// Build a Pod with a single container and the given phase. If
/// `cpu_mem` is `Some((cpu, mem))` the container declares those compute
/// requests; if `None` the container has no resources block (BestEffort
/// QoS class — important for the scope-selector tests).
fn make_pod(name: &str, namespace: &str, phase: Phase, cpu_mem: Option<(&str, &str)>) -> Pod {
    let resources = cpu_mem.map(|(cpu, memory)| {
        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), cpu.to_string());
        requests.insert("memory".to_string(), memory.to_string());
        ResourceRequirements {
            requests: Some(requests),
            limits: None,
            claims: None,
        }
    });
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources,
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(phase),
            ..Default::default()
        }),
    }
}

fn pod_with_compute(name: &str, namespace: &str, cpu: &str, memory: &str) -> Pod {
    make_pod(name, namespace, Phase::Running, Some((cpu, memory)))
}

// ===========================================================================
// Namespace lifecycle
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/namespace.go
// ===========================================================================

/// [sig-api-machinery] Namespaces should ensure that all pods are removed when
/// a namespace is deleted [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/namespace.go:75 (family)
/// Sonobuoy (Round 160): PASS
///
/// The api-server marks the namespace `Terminating` and adds a
/// `deletionTimestamp`. Cleanup is then performed asynchronously by the
/// namespace controller; this test mirrors the **api-server contract** the
/// upstream e2e relies on (DELETE returns immediately with the namespace in
/// the Terminating phase and a finalizer present).
#[tokio::test]
async fn namespace_delete_marks_terminating_and_keeps_finalizer() {
    let (router, _mem) = spawn_router();

    // Create.
    let (status, body) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces",
        Some(&ns_body("ns-delete-test")),
    )
    .await;
    assert_eq!(
        status, 201,
        "namespace create must return 201: body={}",
        body
    );
    assert_eq!(body["metadata"]["name"], "ns-delete-test");
    // Server-side defaulting must produce phase=Active.
    assert_eq!(body["status"]["phase"], "Active");
    // And the `kubernetes` finalizer must be injected so the controller
    // can drive cleanup.
    let finalizers = body["metadata"]["finalizers"]
        .as_array()
        .expect("finalizers array");
    assert!(
        finalizers.iter().any(|f| f == "kubernetes"),
        "expected 'kubernetes' finalizer to be added, got {:?}",
        finalizers
    );

    // Delete returns the namespace in the Terminating phase.
    let (status, body) =
        send_json(router, "DELETE", "/api/v1/namespaces/ns-delete-test", None).await;
    assert_eq!(
        status, 200,
        "namespace delete must return 200: body={}",
        body
    );
    assert_eq!(
        body["status"]["phase"], "Terminating",
        "phase must be Terminating after DELETE, got body={}",
        body
    );
    assert!(
        body["metadata"]["deletionTimestamp"].is_string(),
        "deletionTimestamp must be set after DELETE, got body={}",
        body
    );
}

/// [sig-api-machinery] Namespaces should patch a Namespace [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/namespace.go:262
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn namespace_patch_updates_labels() {
    let (router, _mem) = spawn_router();
    let (_, _) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces",
        Some(&ns_body("ns-patch-test")),
    )
    .await;

    let patch = json!({
        "metadata": { "labels": { "patched": "yes", "tier": "alpha" } }
    });
    let (status, body) = send_patch(
        router,
        "/api/v1/namespaces/ns-patch-test",
        &patch,
        "application/merge-patch+json",
    )
    .await;
    assert_eq!(status, 200, "patch must return 200: body={}", body);
    assert_eq!(body["metadata"]["labels"]["patched"], "yes");
    assert_eq!(body["metadata"]["labels"]["tier"], "alpha");
}

/// [sig-api-machinery] Namespaces should be created and listed [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/namespace.go:188 (family)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn namespace_create_then_list_contains_it() {
    let (router, _mem) = spawn_router();
    for n in ["ns-list-a", "ns-list-b", "ns-list-c"] {
        let (s, _) = send_json(
            router.clone(),
            "POST",
            "/api/v1/namespaces",
            Some(&ns_body(n)),
        )
        .await;
        assert_eq!(s, 201, "create {} must succeed", n);
    }

    let (status, body) = send_json(router, "GET", "/api/v1/namespaces", None).await;
    assert_eq!(status, 200);
    assert_eq!(body["kind"], "NamespaceList");
    let names: Vec<String> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["metadata"]["name"].as_str().unwrap_or("").to_string())
        .collect();
    for expected in ["ns-list-a", "ns-list-b", "ns-list-c"] {
        assert!(
            names.iter().any(|n| n == expected),
            "list must contain {}, got {:?}",
            expected,
            names
        );
    }
}

/// [sig-api-machinery] ResourceQuota should manage the lifecycle of a
/// ResourceQuota [Conformance] — the early step: *"Attempt to list all
/// namespaces with a label selector which MUST succeed. One list MUST be
/// found."*
///
/// Upstream: test/e2e/apimachinery/resource_quota.go (the "manage the lifecycle"
/// case). Tracked as issue #276. Pairs with
/// `resource_quota_deletecollection_by_label_selector`.
///
/// Locks the `apply_selectors` filtering on the namespace LIST handler: only the
/// label-matching namespace is returned.
#[tokio::test]
async fn namespace_list_by_label_selector_returns_only_matches() {
    let (router, _mem) = spawn_router();

    let labeled = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": "ns-rq-lifecycle", "labels": { "e2e-ns": "rq-lifecycle" } }
    });
    let (s, _) = send_json(router.clone(), "POST", "/api/v1/namespaces", Some(&labeled)).await;
    assert_eq!(s, 201);
    // A second namespace without the label must be filtered out.
    let (s, _) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces",
        Some(&ns_body("ns-rq-other")),
    )
    .await;
    assert_eq!(s, 201);

    let (status, body) = send_json(
        router,
        "GET",
        "/api/v1/namespaces?labelSelector=e2e-ns=rq-lifecycle",
        None,
    )
    .await;
    assert_eq!(status, 200, "labelled namespace list MUST succeed");
    assert_eq!(body["kind"], "NamespaceList");
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["ns-rq-lifecycle"], "exactly one list MUST be found");
}

/// [sig-api-machinery] Namespaces creating namespaces should auto-create the
/// default ServiceAccount [Conformance]
///
/// Upstream: tested indirectly by the framework helper
/// `WaitForServiceAccountInNamespace` (every conformance test relies on it).
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn namespace_create_auto_provisions_default_service_account() {
    let (router, mem) = spawn_router();
    let (status, _) = send_json(
        router,
        "POST",
        "/api/v1/namespaces",
        Some(&ns_body("ns-sa-test")),
    )
    .await;
    assert_eq!(status, 201);

    // Direct storage check — the namespace handler must have created the
    // default ServiceAccount object as part of the create handler.
    let sa_key = build_key("serviceaccounts", Some("ns-sa-test"), "default");
    let sa: Value = mem.get(&sa_key).await.expect("default SA must exist");
    assert_eq!(sa["metadata"]["name"], "default");
    assert_eq!(sa["metadata"]["namespace"], "ns-sa-test");
}

/// [sig-api-machinery] Namespaces server-side finalize entrypoint
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/namespace.go finalize subresource family
/// Sonobuoy (Round 160): PASS
///
/// The `/finalize` PUT entrypoint allows a controller to remove its
/// finalizer; the api-server must accept the PUT and persist the updated
/// `spec.finalizers`.
#[tokio::test]
async fn namespace_finalize_subresource_removes_finalizer() {
    let (router, _mem) = spawn_router();
    let (_, created) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces",
        Some(&ns_body("ns-final-test")),
    )
    .await;
    let uid = created["metadata"]["uid"].as_str().unwrap();
    assert!(!uid.is_empty());

    // PUT the namespace back with empty finalizers (simulating the namespace
    // controller calling `/finalize` after cleanup).
    let mut finalized = created.clone();
    finalized["metadata"]["finalizers"] = json!([]);
    let (status, body) = send_json(
        router,
        "PUT",
        "/api/v1/namespaces/ns-final-test/finalize",
        Some(&finalized),
    )
    .await;
    assert_eq!(status, 200, "/finalize PUT must return 200: body={}", body);
    let finalizers = body["metadata"]["finalizers"].as_array();
    assert!(
        finalizers.is_none_or(|f| f.is_empty()),
        "finalize must clear finalizers, got {:?}",
        finalizers
    );
}

/// [sig-api-machinery] Namespaces GET on a missing namespace returns 404
/// (NotFound StatusReason).
///
/// Upstream: negative case companion to `namespace.go:188`.
/// Sonobuoy (Round 160): PASS (implicit — used by every test cleanup path)
#[tokio::test]
async fn namespace_get_unknown_returns_not_found() {
    let (router, _mem) = spawn_router();
    let (status, body) = send_json(router, "GET", "/api/v1/namespaces/does-not-exist", None).await;
    assert_eq!(status, 404, "missing namespace must be 404: body={}", body);
    assert_eq!(body["kind"], "Status");
    assert_eq!(body["reason"], "NotFound");
}

// ===========================================================================
// ResourceQuota lifecycle + usage tracking
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/resource_quota.go
// ===========================================================================

/// [sig-api-machinery] ResourceQuota should create a ResourceQuota and
/// ensure its status is promptly calculated. [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/resource_quota.go:90
/// Sonobuoy (Round 160): PASS
///
/// The handler must seed `status.hard` from spec and initialize
/// `status.used` to "0" for every tracked resource key.
#[tokio::test]
async fn resource_quota_create_seeds_status_used_to_zero() {
    let (router, _mem) = spawn_router();
    // The handler needs the namespace path param, but the underlying
    // storage doesn't enforce that the namespace exists for resourcequotas
    // (so we don't need a prior create).
    let body = quota_body(
        "test-quota",
        &[
            ("pods", "5"),
            ("requests.cpu", "1"),
            ("requests.memory", "500Mi"),
            ("configmaps", "10"),
        ],
    );

    let (status, body) = send_json(
        router,
        "POST",
        "/api/v1/namespaces/default/resourcequotas",
        Some(&body),
    )
    .await;
    assert_eq!(status, 201, "create quota must return 201: body={}", body);
    let used = body["status"]["used"]
        .as_object()
        .expect("status.used populated by handler");
    for key in ["pods", "requests.cpu", "requests.memory", "configmaps"] {
        assert_eq!(
            used.get(key).and_then(|v| v.as_str()),
            Some("0"),
            "every hard key must initialize to 0 in status.used; got {:?} for key={}",
            used.get(key),
            key
        );
    }
    let hard = body["status"]["hard"]
        .as_object()
        .expect("status.hard mirrors spec.hard");
    assert_eq!(hard.get("pods").and_then(|v| v.as_str()), Some("5"));
}

/// [sig-api-machinery] ResourceQuota should track the lifecycle of a
/// ConfigMap (object-count quota) — list + update + delete the quota
/// itself.
///
/// Upstream: resource_quota.go:412
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn resource_quota_crud_round_trip_over_http() {
    let (router, _mem) = spawn_router();
    let body = quota_body("rq-crud", &[("pods", "5")]);

    let (status, _) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces/default/resourcequotas",
        Some(&body),
    )
    .await;
    assert_eq!(status, 201);

    let (status, get_body) = send_json(
        router.clone(),
        "GET",
        "/api/v1/namespaces/default/resourcequotas/rq-crud",
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(get_body["spec"]["hard"]["pods"], "5");

    // List in the namespace.
    let (status, list) = send_json(
        router.clone(),
        "GET",
        "/api/v1/namespaces/default/resourcequotas",
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(list["kind"], "ResourceQuotaList");
    assert!(list["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|i| i["metadata"]["name"] == "rq-crud"));

    // Update via PUT — bump the limit.
    let updated = quota_body("rq-crud", &[("pods", "10")]);
    let (status, after_put) = send_json(
        router.clone(),
        "PUT",
        "/api/v1/namespaces/default/resourcequotas/rq-crud",
        Some(&updated),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(after_put["spec"]["hard"]["pods"], "10");

    // Delete.
    let (status, _) = send_json(
        router.clone(),
        "DELETE",
        "/api/v1/namespaces/default/resourcequotas/rq-crud",
        None,
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = send_json(
        router,
        "GET",
        "/api/v1/namespaces/default/resourcequotas/rq-crud",
        None,
    )
    .await;
    assert_eq!(status, 404, "DELETE must remove the quota");
}

/// [sig-api-machinery] ResourceQuota should be listable across all
/// namespaces via `/api/v1/resourcequotas` [Conformance]
///
/// Upstream: resource_quota.go:412 (cross-namespace family)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn resource_quota_list_all_namespaces() {
    let (router, _mem) = spawn_router();
    for ns in ["rq-ns-a", "rq-ns-b"] {
        let (s, _) = send_json(
            router.clone(),
            "POST",
            &format!("/api/v1/namespaces/{}/resourcequotas", ns),
            Some(&quota_body("global", &[("pods", "1")])),
        )
        .await;
        assert_eq!(s, 201, "create in ns={}", ns);
    }

    let (status, body) = send_json(router, "GET", "/api/v1/resourcequotas", None).await;
    assert_eq!(status, 200);
    assert_eq!(body["kind"], "ResourceQuotaList");
    let namespaces: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|i| i["metadata"]["namespace"].as_str())
        .collect();
    assert!(namespaces.contains(&"rq-ns-a"));
    assert!(namespaces.contains(&"rq-ns-b"));
}

/// [sig-api-machinery] ResourceQuota should create a ResourceQuota and
/// capture the life of a pod. [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/resource_quota.go:243
/// Sonobuoy (Round 160): FAIL — failure recorded at
/// `resource_quota.go:312` ("Ensuring a pod cannot update its resource
/// requirements" → "Expected an error to have occurred. Got: nil").
///
/// PR #45 (`748241cf  fix(conformance): ResourceQuota usage recompute on
/// object delete`) added the `reconcile_one(ns, name)` controller entry
/// point + watch-fanout. This test now also asserts the second half of
/// the upstream scenario: a Pod **UPDATE** that would push usage past the
/// quota `.spec.hard` budget must be rejected with `403 Forbidden`
/// ("exceeded quota"), with delta-usage semantics so an in-budget update
/// (new request − old request ≤ remaining budget) still passes.
///
/// See `docs/conformance/apimachinery-namespaces-quota-limits.md`.
#[tokio::test]
async fn resource_quota_captures_full_pod_lifecycle() {
    let (router, mem) = spawn_router();
    let ns = "rq-lifecycle";

    // 1. Create the quota: 5 pods, 1 CPU, 500Mi memory.
    let body = quota_body(
        "test-quota",
        &[
            ("pods", "5"),
            ("requests.cpu", "1"),
            ("requests.memory", "500Mi"),
        ],
    );
    let (s, _) = send_json(
        router.clone(),
        "POST",
        &format!("/api/v1/namespaces/{}/resourcequotas", ns),
        Some(&body),
    )
    .await;
    assert_eq!(s, 201);

    // 2. Create a pod that fits the quota (cpu=300m, memory=200Mi).
    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p1", "namespace": ns },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": {
                    "requests": { "cpu": "300m", "memory": "200Mi" }
                }
            }]
        }
    });
    let (s, body) = send_json(
        router.clone(),
        "POST",
        &format!("/api/v1/namespaces/{}/pods", ns),
        Some(&pod_body),
    )
    .await;
    assert_eq!(s, 201, "initial pod create must succeed: body={}", body);

    // 3. Reconcile so quota.status.used reflects the pod we just created.
    ResourceQuotaController::new(mem.clone())
        .reconcile_one(ns, "test-quota")
        .await
        .unwrap();

    // 4. RESIZE the pod with resource requests that would exceed the
    //    quota (cpu=2 > 1). Expect 403 Forbidden / "exceeded quota".
    //    Without delta-usage admission, this would silently succeed
    //    (the Round-160 failure).
    //
    //    NOTE: Resource mutations go via the /resize subresource (KEP-1287),
    //    not plain PUT. Plain PUT now rejects any container body change
    //    other than image, matching upstream ValidatePodUpdate
    //    (pkg/apis/core/validation/validation.go:5695).
    let over_quota_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p1", "namespace": ns },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": {
                    "requests": { "cpu": "2", "memory": "200Mi" }
                }
            }]
        }
    });
    let (s, body) = send_json(
        router.clone(),
        "PUT",
        &format!("/api/v1/namespaces/{}/pods/p1/resize", ns),
        Some(&over_quota_body),
    )
    .await;
    assert_eq!(
        s, 403,
        "RESIZE that pushes requests.cpu past quota must be 403: body={}",
        body
    );

    // 5. An in-budget RESIZE (cpu=500m: delta = 500m − 300m = +200m,
    //    new total = 500m ≤ 1000m) must still succeed — delta-usage
    //    semantics, not a flat reject.
    let in_budget_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p1", "namespace": ns },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": {
                    "requests": { "cpu": "500m", "memory": "200Mi" }
                }
            }]
        }
    });
    let (s, body) = send_json(
        router.clone(),
        "PUT",
        &format!("/api/v1/namespaces/{}/pods/p1/resize", ns),
        Some(&in_budget_body),
    )
    .await;
    assert_eq!(
        s, 200,
        "in-budget RESIZE (delta within remaining quota) must succeed: body={}",
        body
    );

    // 6. RESIZE path: same delta semantics for over-budget memory.
    //    A RESIZE that pushes memory past the quota (memory=600Mi >
    //    500Mi) must be rejected.
    let over_quota_mem_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "p1", "namespace": ns },
        "spec": {
            "containers": [{
                "name": "c",
                "image": "pause:latest",
                "resources": {
                    "requests": { "cpu": "500m", "memory": "600Mi" }
                }
            }]
        }
    });
    let (s, body) = send_json(
        router,
        "PUT",
        &format!("/api/v1/namespaces/{}/pods/p1/resize", ns),
        Some(&over_quota_mem_body),
    )
    .await;
    assert_eq!(
        s, 403,
        "RESIZE that pushes requests.memory past quota must be 403: body={}",
        body
    );
}

/// PR #45 regression guard (HTTP surface): after a tracked pod is deleted
/// from storage, a `reconcile_one()` cycle on the ResourceQuotaController
/// must observe the deletion and decrement `status.used.pods`. This
/// asserts the **REST round-trip** of the recomputed quota — the storage
/// path is unit-tested in
/// `crates/controller-manager/tests/resource_quota_usage_recompute_test.rs`.
///
/// Upstream scenario: resource_quota.go:243 → ".status.used.pods drops
/// back to 0 after the pod is deleted".
/// Sonobuoy (Round 160): FAIL — see test above.
#[tokio::test]
async fn resource_quota_usage_recomputes_on_pod_delete_via_http() {
    let (router, mem) = spawn_router();
    let ns = "rq-recompute";

    // Seed quota via REST.
    let (s, _) = send_json(
        router.clone(),
        "POST",
        &format!("/api/v1/namespaces/{}/resourcequotas", ns),
        Some(&quota_body("q", &[("pods", "5"), ("requests.cpu", "2")])),
    )
    .await;
    assert_eq!(s, 201);

    // Seed a pod directly in storage (the pod admission path is out of
    // scope here — the controller drives quota usage from storage state).
    let pod_key = build_key("pods", Some(ns), "pod-a");
    mem.create(&pod_key, &pod_with_compute("pod-a", ns, "500m", "256Mi"))
        .await
        .unwrap();

    // Drive the controller's reconcile-one path (the entry point PR #45
    // introduced).
    let controller = ResourceQuotaController::new(mem.clone());
    controller.reconcile_one(ns, "q").await.unwrap();

    // GET the quota back through the api-server and assert used.pods == 1.
    let (status, body) = send_json(
        router.clone(),
        "GET",
        &format!("/api/v1/namespaces/{}/resourcequotas/q", ns),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["status"]["used"]["pods"], "1",
        "after pod create + reconcile, used.pods must be 1: body={}",
        body
    );

    // Delete pod, reconcile, verify recompute.
    mem.delete(&pod_key).await.unwrap();
    controller.reconcile_one(ns, "q").await.unwrap();

    let (status, body) = send_json(
        router,
        "GET",
        &format!("/api/v1/namespaces/{}/resourcequotas/q", ns),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["status"]["used"]["pods"], "0",
        "after pod delete + reconcile, used.pods must be 0 (PR #45 regression guard): body={}",
        body
    );
    assert_eq!(
        body["status"]["used"]["requests.cpu"], "0m",
        "after pod delete, used.requests.cpu must be 0: body={}",
        body
    );
}

/// [sig-api-machinery] ResourceQuota status subresource is updated by
/// reconciliation and returned via `/status`.
///
/// Upstream: resource_quota.go (status subresource family)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn resource_quota_status_subresource_returns_used() {
    let (router, mem) = spawn_router();
    let ns = "rq-status";
    let (s, _) = send_json(
        router.clone(),
        "POST",
        &format!("/api/v1/namespaces/{}/resourcequotas", ns),
        Some(&quota_body("q", &[("pods", "3")])),
    )
    .await;
    assert_eq!(s, 201);

    // Add a pod and reconcile.
    let pod_key = build_key("pods", Some(ns), "p1");
    mem.create(&pod_key, &pod_with_compute("p1", ns, "100m", "64Mi"))
        .await
        .unwrap();
    ResourceQuotaController::new(mem.clone())
        .reconcile_one(ns, "q")
        .await
        .unwrap();

    let (status, body) = send_json(
        router,
        "GET",
        &format!("/api/v1/namespaces/{}/resourcequotas/q/status", ns),
        None,
    )
    .await;
    assert_eq!(
        status, 200,
        "status subresource GET must return 200: body={}",
        body
    );
    assert_eq!(
        body["status"]["used"]["pods"], "1",
        "/status GET must reflect reconciled used.pods: body={}",
        body
    );
}

/// [sig-api-machinery] ResourceQuota PATCH (merge patch) of `spec.hard`
/// updates the persisted hard limits and survives a subsequent GET.
///
/// Upstream: resource_quota.go:412 (update family)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn resource_quota_patch_spec_hard_persists() {
    let (router, _mem) = spawn_router();
    let (s, _) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces/default/resourcequotas",
        Some(&quota_body("rq-patch", &[("pods", "3")])),
    )
    .await;
    assert_eq!(s, 201);

    let patch = json!({ "spec": { "hard": { "pods": "12", "configmaps": "5" } } });
    let (status, body) = send_patch(
        router.clone(),
        "/api/v1/namespaces/default/resourcequotas/rq-patch",
        &patch,
        "application/merge-patch+json",
    )
    .await;
    assert_eq!(status, 200, "PATCH must return 200: body={}", body);
    assert_eq!(body["spec"]["hard"]["pods"], "12");
    assert_eq!(body["spec"]["hard"]["configmaps"], "5");

    let (_, get_body) = send_json(
        router,
        "GET",
        "/api/v1/namespaces/default/resourcequotas/rq-patch",
        None,
    )
    .await;
    assert_eq!(get_body["spec"]["hard"]["pods"], "12");
}

/// [sig-api-machinery] ResourceQuota with scopes — controller filters by
/// scope when computing usage.
///
/// Upstream: resource_quota.go:1063 (BestEffort / NotBestEffort scopes
/// family)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn resource_quota_scopes_best_effort_filter() {
    let (_router, mem) = spawn_router();
    let ns = "rq-scope";

    // BestEffort quota (counts pods with no resource requests/limits).
    let be_spec = ResourceQuotaSpec {
        hard: Some({
            let mut m = HashMap::new();
            m.insert("pods".to_string(), "10".to_string());
            m
        }),
        scopes: Some(vec!["BestEffort".to_string()]),
        scope_selector: None,
    };
    let be_quota = ResourceQuota::new("be-quota", ns, be_spec);
    mem.create(
        &build_key("resourcequotas", Some(ns), "be-quota"),
        &be_quota,
    )
    .await
    .unwrap();

    // One BestEffort pod (no resources block at all).
    let be_pod = make_pod("be-pod", ns, Phase::Running, None);
    mem.create(&build_key("pods", Some(ns), "be-pod"), &be_pod)
        .await
        .unwrap();
    // One Burstable pod (has requests, so NOT BestEffort).
    mem.create(
        &build_key("pods", Some(ns), "burst-pod"),
        &pod_with_compute("burst-pod", ns, "200m", "128Mi"),
    )
    .await
    .unwrap();

    ResourceQuotaController::new(mem.clone())
        .reconcile_one(ns, "be-quota")
        .await
        .unwrap();

    let updated: ResourceQuota = mem
        .get(&build_key("resourcequotas", Some(ns), "be-quota"))
        .await
        .unwrap();
    assert_eq!(
        updated
            .status
            .as_ref()
            .and_then(|s| s.used.as_ref())
            .and_then(|u| u.get("pods"))
            .map(|s| s.as_str()),
        Some("1"),
        "BestEffort scope must include only the BE pod, got {:?}",
        updated.status
    );
}

/// [sig-api-machinery] ResourceQuota terminal pods (Succeeded/Failed) must
/// NOT be counted against `pods` usage.
///
/// Upstream: resource_quota.go (PodEvaluator semantics)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn resource_quota_terminal_pods_not_counted() {
    let (_router, mem) = spawn_router();
    let ns = "rq-terminal";
    let q = ResourceQuota::new(
        "q",
        ns,
        ResourceQuotaSpec {
            hard: Some({
                let mut m = HashMap::new();
                m.insert("pods".to_string(), "10".to_string());
                m
            }),
            scopes: None,
            scope_selector: None,
        },
    );
    mem.create(&build_key("resourcequotas", Some(ns), "q"), &q)
        .await
        .unwrap();

    // 1 Running pod (counts) + 1 Succeeded + 1 Failed (do NOT count).
    for (name, phase) in [
        ("running-pod", Phase::Running),
        ("succeeded-pod", Phase::Succeeded),
        ("failed-pod", Phase::Failed),
    ] {
        let pod = make_pod(name, ns, phase, Some(("100m", "64Mi")));
        mem.create(&build_key("pods", Some(ns), name), &pod)
            .await
            .unwrap();
    }

    ResourceQuotaController::new(mem.clone())
        .reconcile_one(ns, "q")
        .await
        .unwrap();

    let updated: ResourceQuota = mem
        .get(&build_key("resourcequotas", Some(ns), "q"))
        .await
        .unwrap();
    assert_eq!(
        updated
            .status
            .as_ref()
            .and_then(|s| s.used.as_ref())
            .and_then(|u| u.get("pods"))
            .map(|s| s.as_str()),
        Some("1"),
        "only the Running pod must count; Succeeded + Failed pods must not, got {:?}",
        updated.status
    );
}

/// [sig-api-machinery] ResourceQuota DELETE removes the resource and
/// subsequent GET returns 404 with `reason=NotFound`.
///
/// Upstream: resource_quota.go (every test cleans up via DELETE)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn resource_quota_delete_then_get_returns_not_found() {
    let (router, _mem) = spawn_router();
    let (_, _) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces/default/resourcequotas",
        Some(&quota_body("rq-del", &[("pods", "1")])),
    )
    .await;
    let (s, _) = send_json(
        router.clone(),
        "DELETE",
        "/api/v1/namespaces/default/resourcequotas/rq-del",
        None,
    )
    .await;
    assert_eq!(s, 200);
    let (s, body) = send_json(
        router,
        "GET",
        "/api/v1/namespaces/default/resourcequotas/rq-del",
        None,
    )
    .await;
    assert_eq!(s, 404, "expected 404 after DELETE, got body={}", body);
    assert_eq!(body["reason"], "NotFound");
}

/// [sig-api-machinery] ResourceQuota should manage the lifecycle of a
/// ResourceQuota [Conformance] — the final lifecycle step: *"It MUST succeed at
/// deleting a collection of ResourceQuota via a label selector."*
///
/// Upstream: test/e2e/apimachinery/resource_quota.go (the "manage the lifecycle"
/// case, promoted v1.25). Tracked as issue #276.
///
/// Mirrors the DELETE-collection-by-`labelSelector` contract: only the quotas
/// whose labels match the selector are removed; non-matching quotas survive.
/// This locks the `apply_selectors` filtering in `deletecollection_resourcequotas`.
#[tokio::test]
async fn resource_quota_deletecollection_by_label_selector() {
    let (router, _mem) = spawn_router();

    // Two quotas carrying the selector label, plus one without it.
    let labeled = |name: &str| {
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": { "name": name, "labels": { "e2e-rq": "manage-lifecycle" } },
            "spec": { "hard": { "cpu": "1", "memory": "500Mi" } }
        })
    };
    for body in [labeled("rq-sel-a"), labeled("rq-sel-b")] {
        let (s, _) = send_json(
            router.clone(),
            "POST",
            "/api/v1/namespaces/default/resourcequotas",
            Some(&body),
        )
        .await;
        assert_eq!(s, 201);
    }
    let (s, _) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces/default/resourcequotas",
        Some(&quota_body("rq-keep", &[("pods", "3")])),
    )
    .await;
    assert_eq!(s, 201);

    // List filtered by the selector MUST return exactly the two labeled quotas.
    let (s, list) = send_json(
        router.clone(),
        "GET",
        "/api/v1/namespaces/default/resourcequotas?labelSelector=e2e-rq=manage-lifecycle",
        None,
    )
    .await;
    assert_eq!(s, 200);
    let names: Vec<&str> = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names.len(),
        2,
        "label list should match 2 quotas, got {names:?}"
    );
    assert!(
        !names.contains(&"rq-keep"),
        "unlabeled quota must not match"
    );

    // DeleteCollection scoped by the same label selector MUST succeed.
    let (s, _) = send_json(
        router.clone(),
        "DELETE",
        "/api/v1/namespaces/default/resourcequotas?labelSelector=e2e-rq=manage-lifecycle",
        None,
    )
    .await;
    assert_eq!(s, 200);

    // The labeled quotas are gone; the non-matching one survives.
    for gone in ["rq-sel-a", "rq-sel-b"] {
        let (s, _) = send_json(
            router.clone(),
            "GET",
            &format!("/api/v1/namespaces/default/resourcequotas/{gone}"),
            None,
        )
        .await;
        assert_eq!(s, 404, "{gone} should have been deleted by collection");
    }
    let (s, _) = send_json(
        router,
        "GET",
        "/api/v1/namespaces/default/resourcequotas/rq-keep",
        None,
    )
    .await;
    assert_eq!(s, 200, "non-matching quota must survive deletecollection");
}

// ===========================================================================
// LimitRange lifecycle + admission enforcement
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/limit_range.go
// ===========================================================================

/// [sig-api-machinery] LimitRange should create a LimitRange with defaults
/// and ensure pod has correct min/max defaults injected [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/limit_range.go:60
/// Sonobuoy (Round 160): PASS
///
/// This test verifies the **handler contract** (LimitRange survives a
/// round-trip with all four fields preserved). The admission path that
/// injects defaults is exercised by `pod_admission_applies_limitrange_defaults`
/// below.
#[tokio::test]
async fn limit_range_crud_round_trip_over_http() {
    let (router, _mem) = spawn_router();
    let item = json!({
        "type": "Container",
        "min": { "cpu": "100m", "memory": "100Mi" },
        "max": { "cpu": "2", "memory": "2Gi" },
        "default": { "cpu": "500m", "memory": "500Mi" },
        "defaultRequest": { "cpu": "200m", "memory": "200Mi" },
        "maxLimitRequestRatio": { "cpu": "4" }
    });
    let body = limitrange_body("lr-full", item);

    let (status, created) = send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces/default/limitranges",
        Some(&body),
    )
    .await;
    assert_eq!(status, 201, "create must return 201: body={}", created);
    assert_eq!(created["spec"]["limits"][0]["type"], "Container");
    assert_eq!(created["spec"]["limits"][0]["default"]["cpu"], "500m");
    assert_eq!(
        created["spec"]["limits"][0]["defaultRequest"]["cpu"],
        "200m"
    );
    assert_eq!(
        created["spec"]["limits"][0]["maxLimitRequestRatio"]["cpu"], "4",
        "maxLimitRequestRatio must survive the serde round-trip"
    );

    let (status, got) = send_json(
        router.clone(),
        "GET",
        "/api/v1/namespaces/default/limitranges/lr-full",
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(got["spec"]["limits"][0]["min"]["memory"], "100Mi");
    assert_eq!(got["spec"]["limits"][0]["max"]["cpu"], "2");

    // Delete.
    let (status, _) = send_json(
        router,
        "DELETE",
        "/api/v1/namespaces/default/limitranges/lr-full",
        None,
    )
    .await;
    assert_eq!(status, 200);
}

/// [sig-api-machinery] LimitRange list + namespace isolation
///
/// Upstream: limit_range.go:60 (list family)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn limit_range_list_is_namespace_scoped() {
    let (router, _mem) = spawn_router();
    let item = json!({
        "type": "Container",
        "max": { "cpu": "2" }
    });
    for ns in ["lr-ns-a", "lr-ns-b"] {
        let (s, _) = send_json(
            router.clone(),
            "POST",
            &format!("/api/v1/namespaces/{}/limitranges", ns),
            Some(&limitrange_body("lr", item.clone())),
        )
        .await;
        assert_eq!(s, 201);
    }

    let (status, body) = send_json(
        router.clone(),
        "GET",
        "/api/v1/namespaces/lr-ns-a/limitranges",
        None,
    )
    .await;
    assert_eq!(status, 200);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["metadata"]["namespace"], "lr-ns-a");

    // Cross-namespace listing must include both.
    let (status, body) = send_json(router, "GET", "/api/v1/limitranges", None).await;
    assert_eq!(status, 200);
    let nsset: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|i| i["metadata"]["namespace"].as_str())
        .collect();
    assert!(nsset.contains(&"lr-ns-a") && nsset.contains(&"lr-ns-b"));
}

/// [sig-api-machinery] LimitRange admission injects defaults onto a Pod
/// container that has none, and accepts the result.
///
/// Upstream: limit_range.go:60 (default-injection family)
/// Sonobuoy (Round 160): PASS
///
/// The admission helper `apply_limit_range_with` is the production code
/// path the api-server invokes during pod create — exercising it directly
/// keeps the test sub-millisecond while still mirroring the upstream
/// observable contract ("pod gets the configured defaults injected").
#[tokio::test]
async fn pod_admission_applies_limitrange_defaults() {
    let mut default = HashMap::new();
    default.insert("cpu".to_string(), "500m".to_string());
    default.insert("memory".to_string(), "512Mi".to_string());
    let mut default_request = HashMap::new();
    default_request.insert("cpu".to_string(), "250m".to_string());
    default_request.insert("memory".to_string(), "256Mi".to_string());

    let lr = LimitRange {
        type_meta: TypeMeta {
            kind: "LimitRange".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("lr").with_namespace("default"),
        spec: LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "Container".to_string(),
                max: None,
                min: None,
                default: Some(default.clone()),
                default_request: Some(default_request.clone()),
                max_limit_request_ratio: None,
            }],
        },
    };

    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("p").with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources: None,
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };

    let admitted = rusternetes_api_server::admission::apply_limit_range_with(&mut pod, &vec![lr])
        .expect("admission helper must succeed");
    assert!(
        admitted,
        "pod with no resources must be admitted after defaults injection"
    );

    let resources = pod.spec.unwrap().containers[0]
        .resources
        .clone()
        .expect("resources block injected");
    let limits = resources.limits.expect("limits injected");
    assert_eq!(limits.get("cpu"), Some(&"500m".to_string()));
    assert_eq!(limits.get("memory"), Some(&"512Mi".to_string()));
    let requests = resources.requests.expect("requests injected");
    assert_eq!(requests.get("cpu"), Some(&"250m".to_string()));
    assert_eq!(requests.get("memory"), Some(&"256Mi".to_string()));
}

/// [sig-api-machinery] LimitRange admission rejects a pod whose container
/// requests/limits violate the configured max.
///
/// Upstream: limit_range.go:60 (max-constraint family)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn pod_admission_rejects_pod_violating_limit_range_max() {
    let mut max = HashMap::new();
    max.insert("cpu".to_string(), "1".to_string());
    max.insert("memory".to_string(), "1Gi".to_string());
    let lr = LimitRange {
        type_meta: TypeMeta {
            kind: "LimitRange".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("lr").with_namespace("default"),
        spec: LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "Container".to_string(),
                max: Some(max),
                min: None,
                default: None,
                default_request: None,
                max_limit_request_ratio: None,
            }],
        },
    };

    // Pod requests cpu=4 (> max=1) — must be rejected.
    let mut huge_requests = HashMap::new();
    huge_requests.insert("cpu".to_string(), "4".to_string());
    huge_requests.insert("memory".to_string(), "2Gi".to_string());
    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("big-pod").with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:latest".to_string(),
                resources: Some(ResourceRequirements {
                    requests: Some(huge_requests.clone()),
                    limits: Some(huge_requests),
                    claims: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };
    let admitted = rusternetes_api_server::admission::apply_limit_range_with(&mut pod, &vec![lr])
        .expect("helper returns Ok with bool, not Err");
    assert!(
        !admitted,
        "pod exceeding LimitRange max must NOT be admitted, got admitted=true"
    );
}

/// [sig-api-machinery] LimitRange admission rejects a pod whose container
/// is below the configured min.
///
/// Upstream: limit_range.go:60 (min-constraint family)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn pod_admission_rejects_pod_below_limit_range_min() {
    let mut min = HashMap::new();
    min.insert("cpu".to_string(), "200m".to_string());
    let lr = LimitRange {
        type_meta: TypeMeta {
            kind: "LimitRange".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("lr").with_namespace("default"),
        spec: LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "Container".to_string(),
                max: None,
                min: Some(min),
                default: None,
                default_request: None,
                max_limit_request_ratio: None,
            }],
        },
    };

    let mut tiny_requests = HashMap::new();
    tiny_requests.insert("cpu".to_string(), "10m".to_string());
    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("tiny-pod").with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources: Some(ResourceRequirements {
                    requests: Some(tiny_requests),
                    limits: None,
                    claims: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };
    let admitted = rusternetes_api_server::admission::apply_limit_range_with(&mut pod, &vec![lr])
        .expect("helper returns Ok with bool, not Err");
    assert!(
        !admitted,
        "pod below LimitRange min must NOT be admitted, got admitted=true"
    );
}

/// [sig-api-machinery] LimitRange admission is a no-op when no LimitRanges
/// exist in the namespace (every conformance test relies on this so pods
/// without explicit resources are admittable in clean namespaces).
///
/// Upstream: limit_range.go (precondition for all pod-create tests)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn pod_admission_passes_when_no_limit_range_present() {
    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("p").with_namespace("clean"),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources: None,
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };
    let admitted = rusternetes_api_server::admission::apply_limit_range_with(&mut pod, &vec![])
        .expect("no LR present must not surface an error");
    assert!(admitted, "no LRs configured must admit any pod");
    assert!(
        pod.spec.unwrap().containers[0].resources.is_none(),
        "empty LR list must NOT inject any defaults"
    );
}
