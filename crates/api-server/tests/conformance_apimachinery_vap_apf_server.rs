//! Scoped mirror of Kubernetes v1.35 conformance for [sig-api-machinery]:
//!   * ValidatingAdmissionPolicy API operations (create/get/list/update/patch/delete)
//!   * API Priority and Fairness — FlowSchema + PriorityLevelConfiguration CRUD
//!   * Server version (`/version` endpoint)
//!   * Servers with Table transformation (406 for unimplemented backend)
//!   * Watchers — restart from last resourceVersion observed by prior watch
//!
//! Source of truth: Ginkgo descriptors at
//!   https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//!     - validating_admission_policy.go
//!     - flowcontrol.go
//!     - server_version.go
//!     - table.go
//!     - watch.go
//!
//! Harness: in-process axum router over `StorageBackend::Memory` driven via
//! `tower::ServiceExt::oneshot`. No Docker, no etcd, no kubelet.
//!
//! Tests mirroring GREEN Sonobuoy outcomes (newly-passing.txt) are plain
//! `#[tokio::test]` and must pass. Tests mirroring still-FAILING outcomes
//! (failing.txt) are `#[ignore = "GAP: …; upstream …"]`.

use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness
// ---------------------------------------------------------------------------

/// Returns `(router, storage)` backed by a fresh `MemoryStorage` with
/// `skip_auth=true` so tests can drive requests without bearer tokens,
/// exactly as the upstream Ginkgo suite uses an admin client.
fn spawn_router() -> (TestApiServer, Arc<MemoryStorage>) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (api, mem)
}

/// Issue a request and return `(status_u16, parsed_json_body)`.
async fn send(
    router: &TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    let (status, value) = router.send(method, uri, content_type, body).await;
    (status.as_u16(), value)
}

/// POST a JSON body with an explicit `Accept` header; return `(status, body)`.
async fn send_post_with_accept(
    router: &TestApiServer,
    uri: &str,
    body: &Value,
    accept: &str,
) -> (u16, Value) {
    let bytes = serde_json::to_vec(body).unwrap();
    let (status, _h, _b, value) = router
        .send_with_headers(
            "POST",
            uri,
            &[("content-type", "application/json"), ("accept", accept)],
            Some(bytes),
        )
        .await;
    (status.as_u16(), value)
}

async fn patch_json(
    router: &TestApiServer,
    uri: &str,
    body: &Value,
    content_type: &str,
) -> (u16, Value) {
    let (status, value) = router
        .send("PATCH", uri, Some(content_type), Some(body))
        .await;
    (status.as_u16(), value)
}

// ===========================================================================
// [sig-api-machinery] server version should find the server version
// [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/server_version.go
// Sonobuoy: newly-passing.txt
// ===========================================================================

/// [sig-api-machinery] server version should find the server version
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/server_version.go:38
/// Sonobuoy (2026-05-29): PASS
///
/// GET /version must return 200 and a JSON body with `gitVersion`,
/// `major`, and `minor` fields — the K8s `VersionInfo` struct.
#[tokio::test]
async fn server_version_should_find_the_server_version() {
    let (router, _mem) = spawn_router();
    let (status, body) = send(&router, "GET", "/version", None).await;
    assert_eq!(status, 200, "GET /version must return 200; body: {body}");

    // Upstream test asserts gitVersion is non-empty.
    let git_version = body["gitVersion"].as_str().unwrap_or("").to_string();
    assert!(
        !git_version.is_empty(),
        "/version must contain non-empty gitVersion; got {body}"
    );

    // major + minor must be present (may be empty strings in dev builds).
    assert!(
        body.get("major").is_some(),
        "/version must contain 'major'; got {body}"
    );
    assert!(
        body.get("minor").is_some(),
        "/version must contain 'minor'; got {body}"
    );
}

// ===========================================================================
// [sig-api-machinery] ValidatingAdmissionPolicy [Privileged:ClusterAdmin]
// should support ValidatingAdmissionPolicy API operations [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/validating_admission_policy.go
// Sonobuoy: newly-passing.txt
// ===========================================================================

/// [sig-api-machinery] ValidatingAdmissionPolicy should support
/// ValidatingAdmissionPolicy API operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/validating_admission_policy.go:75
/// Sonobuoy (2026-05-29): PASS
///
/// Full CRUD lifecycle: create → get → list → update → patch → delete.
#[tokio::test]
async fn vap_should_support_validating_admission_policy_api_operations() {
    let (router, _mem) = spawn_router();
    let base = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies";

    let policy = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "vap-lifecycle-test"},
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": ["apps"],
                    "apiVersions": ["v1"],
                    "operations": ["CREATE", "UPDATE"],
                    "resources": ["deployments"]
                }]
            },
            "validations": [{
                "expression": "object.spec.replicas <= 5",
                "message": "too many replicas"
            }]
        }
    });

    // CREATE
    let (status, body) = send(&router, "POST", base, Some(&policy)).await;
    assert_eq!(status, 201, "create VAP must return 201; body: {body}");

    // GET
    let (status, body) = send(&router, "GET", &format!("{base}/vap-lifecycle-test"), None).await;
    assert_eq!(status, 200, "get VAP must return 200; body: {body}");
    assert_eq!(
        body["metadata"]["name"].as_str(),
        Some("vap-lifecycle-test"),
        "get must return correct name; body: {body}"
    );

    // LIST — must include the created policy
    let (status, list) = send(&router, "GET", base, None).await;
    assert_eq!(status, 200, "list VAPs must return 200; body: {list}");
    let items = list["items"]
        .as_array()
        .expect("list must have items array");
    let names: Vec<&str> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(
        names.contains(&"vap-lifecycle-test"),
        "list must contain created VAP; got {names:?}"
    );

    // UPDATE (PUT) — change the validation message
    let mut updated = body.clone();
    if let Some(spec) = updated.get_mut("spec") {
        if let Some(validations) = spec.get_mut("validations") {
            if let Some(first) = validations.get_mut(0) {
                first["message"] = json!("too many replicas (updated)");
            }
        }
    }
    let (status, body2) = send(
        &router,
        "PUT",
        &format!("{base}/vap-lifecycle-test"),
        Some(&updated),
    )
    .await;
    assert_eq!(status, 200, "update VAP must return 200; body: {body2}");

    // PATCH — add a label via merge patch
    let patch = json!({"metadata": {"labels": {"conformance": "true"}}});
    let (status, body3) = patch_json(
        &router,
        &format!("{base}/vap-lifecycle-test"),
        &patch,
        "application/merge-patch+json",
    )
    .await;
    assert_eq!(status, 200, "patch VAP must return 200; body: {body3}");
    assert_eq!(
        body3["metadata"]["labels"]["conformance"].as_str(),
        Some("true"),
        "patch must apply label; body: {body3}"
    );

    // DELETE
    let (status, _) = send(
        &router,
        "DELETE",
        &format!("{base}/vap-lifecycle-test"),
        None,
    )
    .await;
    assert!(
        status == 200 || status == 202 || status == 204,
        "delete VAP must return 2xx; got {status}"
    );

    // Confirm it's gone
    let (status, _) = send(&router, "GET", &format!("{base}/vap-lifecycle-test"), None).await;
    assert_eq!(status, 404, "get after delete must return 404");
}

/// [sig-api-machinery] ValidatingAdmissionPolicy should support
/// ValidatingAdmissionPolicyBinding API operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/validating_admission_policy.go:175
/// Sonobuoy (2026-05-29): PASS
///
/// Full CRUD lifecycle on `ValidatingAdmissionPolicyBinding`.
#[tokio::test]
async fn vap_should_support_validating_admission_policy_binding_api_operations() {
    let (router, _mem) = spawn_router();
    let base = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings";

    let binding = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": "vap-binding-lifecycle"},
        "spec": {
            "policyName": "some-policy",
            "validationActions": ["Deny"]
        }
    });

    // CREATE
    let (status, body) = send(&router, "POST", base, Some(&binding)).await;
    assert_eq!(
        status, 201,
        "create VAPBinding must return 201; body: {body}"
    );

    // GET
    let (status, body) = send(
        &router,
        "GET",
        &format!("{base}/vap-binding-lifecycle"),
        None,
    )
    .await;
    assert_eq!(status, 200, "get VAPBinding must return 200; body: {body}");
    assert_eq!(
        body["metadata"]["name"].as_str(),
        Some("vap-binding-lifecycle")
    );

    // LIST
    let (status, list) = send(&router, "GET", base, None).await;
    assert_eq!(
        status, 200,
        "list VAPBindings must return 200; body: {list}"
    );
    let items = list["items"].as_array().expect("list must have items");
    let names: Vec<&str> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(
        names.contains(&"vap-binding-lifecycle"),
        "list must contain created binding; got {names:?}"
    );

    // PATCH — add annotation
    let patch = json!({"metadata": {"annotations": {"test": "conformance"}}});
    let (status, body2) = patch_json(
        &router,
        &format!("{base}/vap-binding-lifecycle"),
        &patch,
        "application/merge-patch+json",
    )
    .await;
    assert_eq!(
        status, 200,
        "patch VAPBinding must return 200; body: {body2}"
    );

    // DELETE
    let (status, _) = send(
        &router,
        "DELETE",
        &format!("{base}/vap-binding-lifecycle"),
        None,
    )
    .await;
    assert!(
        status == 200 || status == 202 || status == 204,
        "delete VAPBinding must return 2xx; got {status}"
    );
}

/// [sig-api-machinery] ValidatingAdmissionPolicy should allow expressions to
/// refer variables [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/validating_admission_policy.go:256
/// Sonobuoy (2026-05-29): PASS
///
/// Creates a VAP whose `variables[]` section defines a named CEL variable
/// and a `validations[]` expression that references it via `variables.<name>`.
/// Asserts the policy round-trips through the API with the variables intact.
#[tokio::test]
async fn vap_should_allow_expressions_to_refer_variables() {
    let (router, _mem) = spawn_router();
    let base = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies";

    let policy = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "vap-variables-test"},
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": ["apps"],
                    "apiVersions": ["v1"],
                    "operations": ["CREATE"],
                    "resources": ["deployments"]
                }]
            },
            "variables": [
                {
                    "name": "maxReplicas",
                    "expression": "5"
                }
            ],
            "validations": [{
                "expression": "object.spec.replicas <= variables.maxReplicas",
                "message": "replica count exceeds variable-defined limit"
            }]
        }
    });

    // CREATE — must succeed (the API layer stores the variables without executing them).
    let (status, body) = send(&router, "POST", base, Some(&policy)).await;
    assert_eq!(
        status, 201,
        "create VAP with variables must return 201; body: {body}"
    );

    // Verify the variables round-trip through the API.
    let (status, body) = send(&router, "GET", &format!("{base}/vap-variables-test"), None).await;
    assert_eq!(status, 200, "get VAP must return 200; body: {body}");

    let variables = body["spec"]["variables"]
        .as_array()
        .expect("spec.variables must be an array");
    assert_eq!(variables.len(), 1, "must have one variable; body: {body}");
    assert_eq!(
        variables[0]["name"].as_str(),
        Some("maxReplicas"),
        "variable name must round-trip; body: {body}"
    );
    assert_eq!(
        variables[0]["expression"].as_str(),
        Some("5"),
        "variable expression must round-trip; body: {body}"
    );

    // Cleanup.
    send(
        &router,
        "DELETE",
        &format!("{base}/vap-variables-test"),
        None,
    )
    .await;
}

/// [sig-api-machinery] ValidatingAdmissionPolicy should validate against a
/// Deployment [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/validating_admission_policy.go:319
/// Sonobuoy (2026-05-29): PASS
///
/// Asserts the VAP resource itself is persisted correctly; the actual
/// admission enforcement through the full create-deployment path is an
/// integration concern covered in `cel_vap_end_to_end_test.rs`.
#[tokio::test]
async fn vap_should_validate_against_a_deployment() {
    let (router, _mem) = spawn_router();
    let base = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies";
    let bindings_base = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings";

    // Policy: reject Deployments with more than 2 replicas.
    let policy = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "vap-deployment-validator"},
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": ["apps"],
                    "apiVersions": ["v1"],
                    "operations": ["CREATE", "UPDATE"],
                    "resources": ["deployments"]
                }]
            },
            "validations": [{
                "expression": "object.spec.replicas <= 2",
                "message": "deployments may have at most 2 replicas"
            }]
        }
    });

    let (status, body) = send(&router, "POST", base, Some(&policy)).await;
    assert_eq!(
        status, 201,
        "create deployment-validator VAP must return 201; body: {body}"
    );

    // Binding: apply to all namespaces.
    let binding = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": "vap-deployment-binding"},
        "spec": {
            "policyName": "vap-deployment-validator",
            "validationActions": ["Deny"]
        }
    });

    let (status, body) = send(&router, "POST", bindings_base, Some(&binding)).await;
    assert_eq!(
        status, 201,
        "create deployment binding must return 201; body: {body}"
    );

    // Verify the policy is listed.
    let (status, list) = send(&router, "GET", base, None).await;
    assert_eq!(status, 200);
    let items = list["items"].as_array().expect("items array");
    let names: Vec<&str> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(
        names.contains(&"vap-deployment-validator"),
        "list must contain the deployment validator; got {names:?}"
    );

    // Cleanup.
    send(
        &router,
        "DELETE",
        &format!("{base}/vap-deployment-validator"),
        None,
    )
    .await;
    send(
        &router,
        "DELETE",
        &format!("{bindings_base}/vap-deployment-binding"),
        None,
    )
    .await;
}

// ===========================================================================
// [sig-api-machinery] API priority and fairness [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/flowcontrol.go
// Sonobuoy: failing.txt
// ===========================================================================

/// [sig-api-machinery] API priority and fairness should support
/// PriorityLevelConfiguration API operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/flowcontrol.go:56
/// Sonobuoy (2026-05-29): FAIL — included as GAP stub
///
/// Full CRUD on PriorityLevelConfiguration via
/// `/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations`.
#[tokio::test]
async fn apf_should_support_priority_level_configuration_api_operations() {
    let (router, _mem) = spawn_router();
    let base = "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations";

    let plc = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {"name": "conformance-plc"},
        "spec": {
            "type": "Limited",
            "limited": {
                "nominalConcurrencyShares": 10,
                "limitResponse": {
                    "type": "Queue",
                    "queuing": {
                        "queues": 8,
                        "handSize": 2,
                        "queueLengthLimit": 50
                    }
                }
            }
        }
    });

    // CREATE
    let (status, body) = send(&router, "POST", base, Some(&plc)).await;
    assert_eq!(
        status, 201,
        "create PriorityLevelConfiguration must return 201; body: {body}"
    );

    // GET
    let (status, body) = send(&router, "GET", &format!("{base}/conformance-plc"), None).await;
    assert_eq!(status, 200, "get PLC must return 200; body: {body}");
    assert_eq!(body["metadata"]["name"].as_str(), Some("conformance-plc"));

    // LIST
    let (status, list) = send(&router, "GET", base, None).await;
    assert_eq!(status, 200);
    let items = list["items"].as_array().expect("items");
    let names: Vec<&str> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(
        names.contains(&"conformance-plc"),
        "list must contain PLC; got {names:?}"
    );

    // UPDATE (PUT)
    let mut updated = body.clone();
    if let Some(spec) = updated.get_mut("spec") {
        if let Some(limited) = spec.get_mut("limited") {
            limited["nominalConcurrencyShares"] = json!(20);
        }
    }
    let (status, _) = send(
        &router,
        "PUT",
        &format!("{base}/conformance-plc"),
        Some(&updated),
    )
    .await;
    assert_eq!(status, 200, "update PLC must return 200");

    // PATCH
    let patch = json!({"metadata": {"labels": {"conformance": "true"}}});
    let (status, _) = patch_json(
        &router,
        &format!("{base}/conformance-plc"),
        &patch,
        "application/merge-patch+json",
    )
    .await;
    assert_eq!(status, 200, "patch PLC must return 200");

    // DELETE
    let (status, _) = send(&router, "DELETE", &format!("{base}/conformance-plc"), None).await;
    assert!(
        status == 200 || status == 202 || status == 204,
        "delete PLC must return 2xx; got {status}"
    );

    // Confirm deleted
    let (status, _) = send(&router, "GET", &format!("{base}/conformance-plc"), None).await;
    assert_eq!(status, 404, "get after delete must return 404");
}

/// [sig-api-machinery] API priority and fairness should support FlowSchema
/// API operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/flowcontrol.go:134
/// Sonobuoy (2026-05-29): FAIL — included as GAP stub
///
/// Full CRUD on FlowSchema via
/// `/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas`.
#[tokio::test]
async fn apf_should_support_flow_schema_api_operations() {
    let (router, _mem) = spawn_router();
    let base = "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas";

    let fs = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "FlowSchema",
        "metadata": {"name": "conformance-fs"},
        "spec": {
            "matchingPrecedence": 1000,
            "priorityLevelConfiguration": {"name": "workload-high"},
            "rules": [{
                "subjects": [
                    {
                        "kind": "Group",
                        "group": {"name": "system:masters"}
                    }
                ],
                "resourceRules": [{
                    "verbs": ["*"],
                    "apiGroups": ["*"],
                    "resources": ["*"]
                }]
            }]
        }
    });

    // CREATE
    let (status, body) = send(&router, "POST", base, Some(&fs)).await;
    assert_eq!(
        status, 201,
        "create FlowSchema must return 201; body: {body}"
    );

    // GET
    let (status, body) = send(&router, "GET", &format!("{base}/conformance-fs"), None).await;
    assert_eq!(status, 200, "get FlowSchema must return 200; body: {body}");
    assert_eq!(body["metadata"]["name"].as_str(), Some("conformance-fs"));

    // LIST
    let (status, list) = send(&router, "GET", base, None).await;
    assert_eq!(status, 200);
    let items = list["items"].as_array().expect("items");
    let names: Vec<&str> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(
        names.contains(&"conformance-fs"),
        "list must contain FlowSchema; got {names:?}"
    );

    // UPDATE (PUT) — bump matchingPrecedence
    let mut updated = body.clone();
    if let Some(spec) = updated.get_mut("spec") {
        spec["matchingPrecedence"] = json!(900);
    }
    let (status, _) = send(
        &router,
        "PUT",
        &format!("{base}/conformance-fs"),
        Some(&updated),
    )
    .await;
    assert_eq!(status, 200, "update FlowSchema must return 200");

    // PATCH
    let patch = json!({"metadata": {"labels": {"conformance": "true"}}});
    let (status, _) = patch_json(
        &router,
        &format!("{base}/conformance-fs"),
        &patch,
        "application/merge-patch+json",
    )
    .await;
    assert_eq!(status, 200, "patch FlowSchema must return 200");

    // DELETE
    let (status, _) = send(&router, "DELETE", &format!("{base}/conformance-fs"), None).await;
    assert!(
        status == 200 || status == 202 || status == 204,
        "delete FlowSchema must return 2xx; got {status}"
    );

    // Confirm deleted
    let (status, _) = send(&router, "GET", &format!("{base}/conformance-fs"), None).await;
    assert_eq!(status, 404, "get after delete must return 404");
}

// ===========================================================================
// [sig-api-machinery] Watchers — restart from last RV [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/watch.go
// Sonobuoy: newly-passing.txt
// ===========================================================================

/// [sig-api-machinery] Watchers should be able to restart watching from the
/// last resource version observed by the previous watch [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/watch.go (ResumeWatch)
/// Sonobuoy (2026-05-29): PASS
///
/// The K8s contract: if a client stores the `resourceVersion` from the
/// last event it received and opens a new watch with `?resourceVersion=<rv>`,
/// the server must deliver all events that occurred after that revision —
/// nothing is missed and nothing before that RV is replayed.
///
/// We verify the wire contract at the storage/handler layer:
/// `MemoryStorage::watch_from_revision` is a broadcast channel that delivers
/// future events after the watch is established. The "resume watch" means:
/// (1) subscribe, (2) make writes, (3) observe only those writes — not any
/// writes that preceded the subscription. The HTTP `?resourceVersion` parse
/// and the end-to-end replay semantics are covered by
/// `integration_watch_rv_test.rs`; here we assert the storage-level
/// subscription contract and the `normalize_resource_version` wire helper
/// that the watch handler uses to parse the query parameter.
#[tokio::test]
async fn watchers_should_restart_from_last_resource_version_observed() {
    use rusternetes_api_server::handlers::watch::normalize_resource_version;
    use rusternetes_common::resources::ConfigMap;
    use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, WatchEvent};
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    let storage = Arc::new(MemoryStorage::new());

    // Write two configmaps before establishing the "resumed" watch, to
    // simulate events that belong to a completed watch session.
    let key1 = build_key("configmaps", Some("default"), "rv-cm-first");
    let key2 = build_key("configmaps", Some("default"), "rv-cm-second");
    let key3 = build_key("configmaps", Some("default"), "rv-cm-third");

    storage
        .create(&key1, &ConfigMap::new("rv-cm-first", "default"))
        .await
        .unwrap();
    storage
        .create(&key2, &ConfigMap::new("rv-cm-second", "default"))
        .await
        .unwrap();

    // Capture the revision after two writes — this is the "last RV observed
    // by the first watch session".
    let checkpoint_rv = storage.current_revision().await.unwrap();

    // The resumed watch subscribes at (or after) checkpoint_rv.
    let mut stream = storage
        .watch_from_revision("/registry/configmaps/default/", checkpoint_rv)
        .await
        .unwrap();

    // Third write happens AFTER the stream is established — it must be delivered.
    storage
        .create(&key3, &ConfigMap::new("rv-cm-third", "default"))
        .await
        .unwrap();

    let ev = timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("resumed watch must deliver the post-checkpoint event within 500ms")
        .expect("stream must not be closed")
        .expect("stream must not error");

    match ev {
        WatchEvent::Added(key, _) => {
            assert!(
                key.contains("rv-cm-third"),
                "resumed watch must deliver the write that followed the checkpoint; got key {key}"
            );
        }
        other => panic!(
            "resumed watch must deliver an ADDED event for rv-cm-third; got {:?}",
            other
        ),
    }

    // Verify the wire-level `normalize_resource_version` contract:
    // empty → None (start from current); numeric string → Some(string).
    assert_eq!(
        normalize_resource_version(Some(checkpoint_rv.to_string())),
        Some(checkpoint_rv.to_string()),
        "numeric RV string must be preserved by normalize_resource_version"
    );
    assert_eq!(
        normalize_resource_version(Some(String::new())),
        None,
        "empty RV must normalize to None (watch from current)"
    );
    assert_eq!(
        normalize_resource_version(None),
        None,
        "absent RV must normalize to None"
    );
}

// ===========================================================================
// [sig-api-machinery] Servers with support for Table transformation
// [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/table.go
// Sonobuoy: failing.txt
// ===========================================================================

/// [sig-api-machinery] Servers with support for Table transformation should
/// return a 406 for a backend which does not implement metadata [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/table_conversion.go:154
///
/// The 406 contract is NARROW: it applies only to a *metadata-less* virtual
/// backend. Upstream posts a `SelfSubjectAccessReview` (a synthetic "review"
/// object with no ObjectMeta) with the Table Accept header and asserts 406.
/// Normal resources that carry ObjectMeta — including printer-less ones like
/// configmaps/secrets/podtemplates — must instead return a 200 default
/// NAME/AGE Table (covered in `decoder_accept_header_test.rs`).
///
/// Regression guard: PR #918 wrongly extended the 406 to ~12 common kinds via
/// a converter allowlist. The correct rule is "406 iff no ObjectMeta", which
/// only the review backends hit.
#[tokio::test]
async fn table_transformation_should_return_406_for_backend_without_metadata() {
    let (router, _mem) = spawn_router();

    // POST a SelfSubjectAccessReview asking for the Table format. The review
    // backend has no ObjectMeta, so its handler rejects the projection with
    // 406 Not Acceptable.
    let ssar = serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "spec": {
            "resourceAttributes": {
                "namespace": "default",
                "verb": "get",
                "resource": "pods",
            }
        }
    });
    let (status, _body) = send_post_with_accept(
        &router,
        "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
        &ssar,
        "application/json;as=Table;v=v1beta1;g=meta.k8s.io",
    )
    .await;

    // K8s contract: 406 Not Acceptable for a metadata-less review backend.
    assert_eq!(
        status, 406,
        "Table request for a metadata-less review backend must return 406; got {status}"
    );
}

// ===========================================================================
// [sig-api-machinery] Servers with support for API chunking
// compaction support [Conformance]
//
// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/chunking.go
// Sonobuoy: failing.txt
// ===========================================================================

/// [sig-api-machinery] Servers with support for API chunking should support
/// continue listing from the last key if the original version has been
/// compacted away, though the list is inconsistent [Slow] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/chunking.go:214
/// Sonobuoy (2026-05-29): FAIL — included as GAP stub
///
/// After etcd compaction, a continue token whose resourceVersion is older
/// than the compaction watermark MUST still allow the client to resume from
/// the last key (returning a possibly-inconsistent list with
/// `metadata.remainingItemCount=0`). Rusternetes currently returns 410 Gone
/// (the strict behaviour), not the lenient resume-from-last-key behaviour
/// required by this conformance test.
#[tokio::test]
#[ignore = "GAP: chunking continue-from-last-key-after-compaction not implemented; server returns 410 instead of resuming; upstream chunking.go:214"]
async fn chunking_should_continue_from_last_key_after_compaction() {
    // This test body is a documentation stub only. The corresponding
    // GREEN (strict 410) test lives in conformance_apimachinery_watch_chunking_gc.rs
    // as `chunking_continue_after_compaction_returns_410_expired`.
}

// ===========================================================================
// Harness self-checks
// ===========================================================================

/// Verify the harness helper itself wires up correctly: GET /apis must
/// return 200 (the APIGroupList endpoint is always present).
#[tokio::test]
async fn harness_router_responds_to_api_group_list() {
    let (router, _mem) = spawn_router();
    let (status, body) = send(&router, "GET", "/apis", None).await;
    assert_eq!(status, 200, "GET /apis must return 200; body: {body}");
    assert!(
        body.get("kind").is_some() || body.get("groups").is_some(),
        "GET /apis body must be APIGroupList shape; got {body}"
    );
}
