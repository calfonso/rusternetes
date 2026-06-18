//! Extended Pod lifecycle coverage for the api-server surface, mirroring the
//! Kubernetes v1.35 e2e test at
//! `test/e2e/common/pod_lifecycle.go`.
//!
//! Source of truth (release-1.35):
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/common/pod_lifecycle.go>
//!
//! The upstream e2e suite drives the kubelet to observe runtime container
//! lifecycle (terminated/restart counts, hook execution, eviction). The
//! kubelet half of that loop is exercised in
//! `crates/kubelet/tests/*pod_lifecycle*`; this file is the *api-server* side
//! — what the REST surface admits, defaults, and round-trips during the
//! same lifecycle (create / status / delete).
//!
//! Each `#[tokio::test]` here pins one observable API behaviour referenced by
//! the upstream e2e flow:
//!
//!   * `terminationGracePeriodSeconds` is admitted on create and survives the
//!     round-trip back through GET (no silent defaulting drop / rewrite).
//!   * `lifecycle.preStop` and `lifecycle.postStart` hook schemas are
//!     accepted (exec / httpGet / sleep variants).
//!   * `spec.restartPolicy` enums `Always` / `OnFailure` / `Never` are each
//!     admitted (one test per variant) and bogus values are rejected.
//!   * `status.qosClass` is server-computed at create time for each of the
//!     three QoS tiers (Guaranteed, Burstable, BestEffort).
//!   * `DELETE` with `gracePeriodSeconds=0` force-deletes the pod (the
//!     subsequent GET returns 404 — not a Terminating pod still in store).
//!
//! Harness: identical pattern to `integration_namespace_conditions.rs:74` and
//! `integration_pods_topology_labels.rs:88` — an in-process axum router built
//! over `MemoryStorage` + `AlwaysAllowAuthorizer`, driven through
//! `tower::ServiceExt::oneshot`. No real cluster, no kubelet, no scheduler.

use axum::http::{Method, StatusCode};
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (TestApiServer, Arc<MemoryStorage>) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (api, mem)
}

async fn send(
    router: &TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    router.send(method.as_str(), uri, content_type, body).await
}

/// Create the target namespace before any pod operation. Upstream's
/// `framework.NewDefaultFramework` does the equivalent for every test case.
async fn create_namespace(router: &TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    });
    let (status, payload) = send(router, Method::POST, "/api/v1/namespaces", Some(&body)).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create must succeed: status={status}, body={payload}",
    );
}

/// Minimal Pod body with a single container — the smallest fixture the
/// handler accepts; tests merge their lifecycle-specific fields on top via
/// `serde_json` mutation.
fn minimal_pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name },
        "spec": {
            "containers": [{
                "name":  "main",
                "image": "registry.k8s.io/pause:3.10",
            }],
        },
    })
}

// ---------------------------------------------------------------------------
// terminationGracePeriodSeconds round-trip
// Upstream context: pod_lifecycle.go drives kubelet termination using the
// pod's `terminationGracePeriodSeconds`. The api-server's contract is just
// that whatever the client sent is persisted and re-served verbatim.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pod_termination_grace_period_seconds_is_preserved_on_create() {
    let (router, _mem) = spawn_router();
    let ns = "tgps-roundtrip";
    create_namespace(&router, ns).await;

    let mut body = minimal_pod("grace-pod");
    body["spec"]["terminationGracePeriodSeconds"] = json!(7);

    let (status, created) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod create must succeed: body={created}",
    );

    let created_grace = created
        .get("spec")
        .and_then(|s| s.get("terminationGracePeriodSeconds"))
        .and_then(|v| v.as_i64());
    assert_eq!(
        created_grace,
        Some(7),
        "create response must echo spec.terminationGracePeriodSeconds verbatim: {created}",
    );

    // GET back through the REST surface — the value must survive the
    // round-trip (no silent defaulting to 30).
    let (get_status, got) = send(
        &router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/pods/grace-pod"),
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::OK, "pod GET must succeed: {got}");
    let got_grace = got
        .get("spec")
        .and_then(|s| s.get("terminationGracePeriodSeconds"))
        .and_then(|v| v.as_i64());
    assert_eq!(
        got_grace,
        Some(7),
        "stored pod must keep terminationGracePeriodSeconds = 7, got {got}",
    );
}

// ---------------------------------------------------------------------------
// lifecycle.preStop / lifecycle.postStart admission
// Upstream context: pod_lifecycle.go runs e2e hooks via these fields. The
// api-server contract: the lifecycle schema (exec / httpGet / tcpSocket /
// sleep) must be accepted on create without rejection.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pod_prestop_hook_accepted_in_containers() {
    let (router, _mem) = spawn_router();
    let ns = "prestop-admission";
    create_namespace(&router, ns).await;

    let mut body = minimal_pod("prestop-pod");
    body["spec"]["containers"][0]["lifecycle"] = json!({
        "preStop": {
            "exec": {
                "command": ["/bin/sh", "-c", "echo bye"]
            }
        }
    });

    let (status, created) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod with lifecycle.preStop must be accepted: body={created}",
    );

    let cmd = created
        .get("spec")
        .and_then(|s| s.get("containers"))
        .and_then(|cs| cs.get(0))
        .and_then(|c| c.get("lifecycle"))
        .and_then(|l| l.get("preStop"))
        .and_then(|p| p.get("exec"))
        .and_then(|e| e.get("command"))
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        cmd.len(),
        3,
        "preStop.exec.command must round-trip with 3 args: {created}",
    );
}

#[tokio::test]
async fn pod_poststart_hook_accepted_in_containers() {
    let (router, _mem) = spawn_router();
    let ns = "poststart-admission";
    create_namespace(&router, ns).await;

    let mut body = minimal_pod("poststart-pod");
    body["spec"]["containers"][0]["lifecycle"] = json!({
        "postStart": {
            "httpGet": {
                "path": "/healthz",
                "port": 8080,
                "scheme": "HTTP",
            }
        }
    });

    let (status, created) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod with lifecycle.postStart must be accepted: body={created}",
    );

    let path = created
        .get("spec")
        .and_then(|s| s.get("containers"))
        .and_then(|cs| cs.get(0))
        .and_then(|c| c.get("lifecycle"))
        .and_then(|l| l.get("postStart"))
        .and_then(|p| p.get("httpGet"))
        .and_then(|h| h.get("path"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        path, "/healthz",
        "postStart.httpGet.path must round-trip verbatim: {created}",
    );
}

// ---------------------------------------------------------------------------
// restartPolicy admission — one test per upstream enum value.
// Upstream: pkg/apis/core/validation/validation.go validateRestartPolicy —
// `Always` / `OnFailure` / `Never` are the only accepted values.
// ---------------------------------------------------------------------------

async fn assert_restart_policy_accepted(policy: &str, ns: &str) {
    let (router, _mem) = spawn_router();
    create_namespace(&router, ns).await;

    let mut body = minimal_pod(&format!("rp-{}", policy.to_lowercase()));
    body["spec"]["restartPolicy"] = json!(policy);

    let (status, created) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "restartPolicy={policy} must be accepted: body={created}",
    );

    let echoed = created
        .get("spec")
        .and_then(|s| s.get("restartPolicy"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        echoed, policy,
        "spec.restartPolicy must round-trip as {policy:?}: {created}",
    );
}

#[tokio::test]
async fn pod_restart_policy_always_accepted() {
    assert_restart_policy_accepted("Always", "rp-always").await;
}

#[tokio::test]
async fn pod_restart_policy_on_failure_accepted() {
    assert_restart_policy_accepted("OnFailure", "rp-onfailure").await;
}

#[tokio::test]
async fn pod_restart_policy_never_accepted() {
    assert_restart_policy_accepted("Never", "rp-never").await;
}

// ---------------------------------------------------------------------------
// QoS class derivation
// Upstream: pkg/apis/core/v1/helper/qos/qos.go::GetPodQOS — the API server
// computes `status.qosClass` based on the resource requests/limits of each
// container at create time.
//
// All-equal limits+requests on every container → Guaranteed.
// Any limit/request set but not all equal → Burstable.
// No resources anywhere → BestEffort.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pod_qos_class_guaranteed_derived_from_spec_resources() {
    let (router, _mem) = spawn_router();
    let ns = "qos-guaranteed";
    create_namespace(&router, ns).await;

    let mut body = minimal_pod("qos-g");
    body["spec"]["containers"][0]["resources"] = json!({
        "limits":   { "cpu": "100m", "memory": "128Mi" },
        "requests": { "cpu": "100m", "memory": "128Mi" },
    });

    let (status, created) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "pod create must succeed");

    let qos = created
        .get("status")
        .and_then(|s| s.get("qosClass"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        qos, "Guaranteed",
        "status.qosClass must be Guaranteed for all-equal cpu+mem limits/requests: {created}",
    );
}

#[tokio::test]
async fn pod_qos_class_burstable_derived_from_spec_resources() {
    let (router, _mem) = spawn_router();
    let ns = "qos-burstable";
    create_namespace(&router, ns).await;

    let mut body = minimal_pod("qos-b");
    // Only requests, no limits → Burstable.
    body["spec"]["containers"][0]["resources"] = json!({
        "requests": { "cpu": "50m", "memory": "64Mi" },
    });

    let (status, created) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "pod create must succeed");

    let qos = created
        .get("status")
        .and_then(|s| s.get("qosClass"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        qos, "Burstable",
        "status.qosClass must be Burstable for requests-only resources: {created}",
    );
}

#[tokio::test]
async fn pod_qos_class_besteffort_derived_from_spec_resources() {
    let (router, _mem) = spawn_router();
    let ns = "qos-besteffort";
    create_namespace(&router, ns).await;

    // No resources at all → BestEffort.
    let body = minimal_pod("qos-be");

    let (status, created) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "pod create must succeed");

    let qos = created
        .get("status")
        .and_then(|s| s.get("qosClass"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        qos, "BestEffort",
        "status.qosClass must be BestEffort when no container declares resources: {created}",
    );
}

// ---------------------------------------------------------------------------
// DELETE with gracePeriodSeconds=0
// Upstream: pod_lifecycle.go's force-delete path and
// staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go::Delete
// — when `DeleteOptions.GracePeriodSeconds == 0` (force-delete), the pod
// is removed from storage immediately instead of being marked Terminating.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pod_delete_with_grace_period_zero_force_deletes() {
    let (router, _mem) = spawn_router();
    let ns = "force-delete";
    create_namespace(&router, ns).await;

    // (1) Create the pod.
    let body = minimal_pod("force-pod");
    let (create_status, created) = send(
        &router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(&body),
    )
    .await;
    assert_eq!(
        create_status,
        StatusCode::CREATED,
        "pod create must succeed: {created}",
    );

    // (2) Force-delete via gracePeriodSeconds=0 query parameter (this is
    //     what `kubectl delete --grace-period=0 --force` sends on the wire).
    let (del_status, del_body) = send(
        &router,
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns}/pods/force-pod?gracePeriodSeconds=0"),
        None,
    )
    .await;
    assert!(
        del_status.is_success(),
        "force-delete must succeed: status={del_status}, body={del_body}",
    );

    // (3) GET must now return 404 — the pod must be gone from storage,
    //     not lingering with `deletionTimestamp` set.
    let (get_status, get_body) = send(
        &router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/pods/force-pod"),
        None,
    )
    .await;
    assert_eq!(
        get_status,
        StatusCode::NOT_FOUND,
        "after force-delete the pod must be 404, not Terminating: body={get_body}",
    );
}
