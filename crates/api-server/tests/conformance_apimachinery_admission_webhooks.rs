//! Scoped mirror of Kubernetes v1.35 conformance for [sig-api-machinery]
//! Admission webhooks (Validating + Mutating).
//!
//! Source: https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//! Status table: docs/conformance/apimachinery-admission-webhooks.md
//!
//! Each test mirrors a single `framework.ConformanceIt(...)` block from
//! `test/e2e/apimachinery/webhook.go` (line numbers preserved in per-test
//! docstrings). The HTTP layer is exercised via an inline `spawn_router()`
//! helper that calls `rusternetes_api_server::router::build_router` against an
//! `ApiServerState` backed by `MemoryStorage` and `AlwaysAllowAuthorizer`.
//! The webhook *backends* themselves are tiny warp mocks (the same pattern
//! used by `admission_webhook_e2e_test.rs`) — every webhook configuration
//! either targets one of these mocks or `https://0.0.0.0:1/...` for the
//! "fail closed without CA bundle" scenario.

use axum::http::StatusCode;
use rusternetes_api_server::admission_webhook::AdmissionWebhookManager;
use rusternetes_common::{
    admission::{
        AdmissionResponse, AdmissionReview, AdmissionReviewResponse, GroupVersionKind,
        GroupVersionResource, Operation, PatchOp, PatchOperation, UserInfo,
    },
    resources::{
        FailurePolicy, MatchCondition, MutatingWebhook, MutatingWebhookConfiguration,
        OperationType, ReinvocationPolicy, Rule, RuleWithOperations, SideEffectClass,
        ValidatingWebhook, ValidatingWebhookConfiguration, WebhookClientConfig,
    },
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::oneshot;
use warp::Filter;

// ---------------------------------------------------------------------------
// HTTP harness
// ---------------------------------------------------------------------------

/// Build a fully-wired `ApiServerState` backed by an in-memory storage. The
/// authorizer is `AlwaysAllow` and `skip_auth=true` so the router uses
/// `skip_auth_middleware` and no token is needed (mirrors the
/// `patch_cas_retry_test.rs` helper exactly).
/// `(storage, router)` factory used by every HTTP-driven test. Each test
/// owns its own storage so the tests are trivially parallel.
fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// HTTP helper: POST JSON, return `(status, body)`.
async fn post_json(router: TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    router.post(uri, body).await
}

/// HTTP helper: GET JSON, return `(status, body)`.
async fn get_json(router: TestApiServer, uri: &str) -> (StatusCode, Value) {
    router.get(uri).await
}

/// HTTP helper: PUT JSON, return `(status, body)`.
async fn put_json(router: TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    router.put(uri, body).await
}

/// HTTP helper: DELETE, return status (some delete handlers return an empty
/// body or a tombstone; we only care about the status code here).
async fn delete_status(router: TestApiServer, uri: &str) -> StatusCode {
    router.delete(uri).await.0
}

/// HTTP helper: DELETE, return `(status, body)` so the caller can assert on the
/// webhook-denial Status payload.
async fn delete_json(router: TestApiServer, uri: &str) -> (StatusCode, Value) {
    let (status, v) = router.delete(uri).await;
    (status, v)
}

// ---------------------------------------------------------------------------
// Webhook backend mocks (warp). These mirror the `sample-webhook-deployment`
// behaviours used by the upstream Ginkgo tests — allow, deny, mutate, slow.
// ---------------------------------------------------------------------------

/// Generic admission response shim used by every mock below.
fn wrap(response: AdmissionReviewResponse) -> AdmissionReview {
    AdmissionReview {
        api_version: "admission.k8s.io/v1".to_string(),
        kind: "AdmissionReview".to_string(),
        request: None,
        response: Some(response),
    }
}

async fn start_deny_validator(reason: String) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let uid = r.request.map(|req| req.uid).unwrap_or_else(|| "u".into());
            warp::reply::json(&wrap(AdmissionReviewResponse::deny(uid, reason.clone())))
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Validating mock used for DELETE admission. It denies only when the request
/// carries an `oldObject` and no `object` (the K8s DELETE AdmissionReview
/// shape), which lets the test assert both the deny *and* that the api-server
/// populated the review correctly. Any other shape is allowed.
async fn start_delete_deny_validator(reason: String) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let request = match r.request {
                Some(req) => req,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let is_delete = matches!(request.operation, Operation::Delete);
            let has_old = request.old_object.is_some();
            let no_new = request.object.is_none();
            if is_delete && has_old && no_new {
                warp::reply::json(&wrap(AdmissionReviewResponse::deny(
                    request.uid,
                    reason.clone(),
                )))
            } else {
                warp::reply::json(&wrap(AdmissionReviewResponse::allow(request.uid)))
            }
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Mutating mock that adds `/metadata/labels/{key}={value}`.
async fn start_mutator_label(key: String, value: String) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let request = match r.request {
                Some(r) => r,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let patch = vec![PatchOperation {
                op: PatchOp::Add,
                path: format!("/metadata/labels/{}", key),
                value: Some(json!(value.clone())),
                from: None,
            }];
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&patch).unwrap());
            warp::reply::json(&wrap(AdmissionReviewResponse {
                uid: request.uid,
                allowed: true,
                status: None,
                patch: Some(b64),
                patch_type: Some("JSONPatch".to_string()),
                audit_annotations: None,
                warnings: None,
            }))
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Mutating mock that adds an arbitrary JSON pointer path → value. Used by the
/// structural-schema pruning test: the path lands inside the CR's `spec` so
/// the api-server's post-mutation pruning pass can strip it when the CRD
/// schema doesn't declare the field.
async fn start_mutator_path(path: String, value: Value) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |r: AdmissionReview| {
            let request = match r.request {
                Some(r) => r,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let patch = vec![PatchOperation {
                op: PatchOp::Add,
                path: path.clone(),
                value: Some(value.clone()),
                from: None,
            }];
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&patch).unwrap());
            warp::reply::json(&wrap(AdmissionReviewResponse {
                uid: request.uid,
                allowed: true,
                status: None,
                patch: Some(b64),
                patch_type: Some("JSONPatch".to_string()),
                audit_annotations: None,
                warnings: None,
            }))
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Mutating mock that adds an init container to a pod (`/spec/initContainers`).
/// Mirrors the upstream `addPodSpec`-style mutation that the
/// "mutate pod and apply defaults after mutation" test relies on.
async fn start_mutator_init_container() -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route = warp::post()
        .and(warp::body::json())
        .map(|r: AdmissionReview| {
            let request = match r.request {
                Some(r) => r,
                None => {
                    return warp::reply::json(&wrap(AdmissionReviewResponse::allow("u".into())))
                }
            };
            let patch = vec![PatchOperation {
                op: PatchOp::Add,
                path: "/spec/initContainers".to_string(),
                value: Some(json!([{
                    "name": "webhook-added-init",
                    "image": "registry.k8s.io/pause:3.10",
                }])),
                from: None,
            }];
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&patch).unwrap());
            warp::reply::json(&wrap(AdmissionReviewResponse {
                uid: request.uid,
                allowed: true,
                status: None,
                patch: Some(b64),
                patch_type: Some("JSONPatch".to_string()),
                audit_annotations: None,
                warnings: None,
            }))
        });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

/// Slow validator — sleeps `delay` before responding `allow`. Used by the
/// `should honor timeout` mirror.
async fn start_slow_validator(delay: std::time::Duration) -> (String, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let route =
        warp::post()
            .and(warp::body::json())
            .and_then(move |r: AdmissionReview| async move {
                tokio::time::sleep(delay).await;
                let uid = r.request.map(|q| q.uid).unwrap_or_else(|| "u".into());
                Ok::<_, warp::Rejection>(warp::reply::json(&wrap(AdmissionReviewResponse::allow(
                    uid,
                ))))
            });
    let (addr, srv) = warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
        rx.await.ok();
    });
    tokio::spawn(srv);
    (format!("http://{}", addr), tx)
}

// ---------------------------------------------------------------------------
// Builders for compact webhook configurations.
// ---------------------------------------------------------------------------

fn rule_for(api_group: &str, version: &str, resource: &str) -> RuleWithOperations {
    RuleWithOperations {
        operations: vec![OperationType::Create],
        rule: Rule {
            api_groups: vec![api_group.to_string()],
            api_versions: vec![version.to_string()],
            resources: vec![resource.to_string()],
            scope: None,
        },
    }
}

fn delete_rule_for(api_group: &str, version: &str, resource: &str) -> RuleWithOperations {
    RuleWithOperations {
        operations: vec![OperationType::Delete],
        rule: Rule {
            api_groups: vec![api_group.to_string()],
            api_versions: vec![version.to_string()],
            resources: vec![resource.to_string()],
            scope: None,
        },
    }
}

fn validating(
    name: &str,
    url: String,
    rules: Vec<RuleWithOperations>,
    failure_policy: Option<FailurePolicy>,
    timeout: Option<i32>,
) -> ValidatingWebhook {
    ValidatingWebhook {
        name: name.to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules,
        failure_policy,
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: timeout,
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
    }
}

fn mutating(
    name: &str,
    url: String,
    rules: Vec<RuleWithOperations>,
    failure_policy: Option<FailurePolicy>,
    reinvocation: Option<ReinvocationPolicy>,
) -> MutatingWebhook {
    MutatingWebhook {
        name: name.to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules,
        failure_policy,
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: None,
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
        reinvocation_policy: reinvocation,
    }
}

fn admission_request(
    api_group: &str,
    version: &str,
    kind: &str,
    resource: &str,
    namespace: Option<&str>,
    name: &str,
    obj: Value,
) -> rusternetes_common::admission::AdmissionReviewRequest {
    rusternetes_common::admission::AdmissionReviewRequest {
        uid: format!("uid-{}", name),
        kind: GroupVersionKind {
            group: api_group.to_string(),
            version: version.to_string(),
            kind: kind.to_string(),
        },
        resource: GroupVersionResource {
            group: api_group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
        },
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: name.to_string(),
        namespace: namespace.map(|s| s.to_string()),
        operation: Operation::Create,
        user_info: UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        },
        object: Some(obj),
        old_object: None,
        dry_run: None,
        options: None,
    }
}

fn admin_user_info() -> UserInfo {
    UserInfo {
        username: "admin".to_string(),
        uid: "admin-uid".to_string(),
        groups: vec!["system:masters".to_string()],
    }
}

// ===========================================================================
// Mirrors of `[sig-api-machinery] AdmissionWebhook [Privileged:ClusterAdmin]`
// from k8s.io/kubernetes/test/e2e/apimachinery/webhook.go (release-1.35).
// ===========================================================================

/// [sig-api-machinery] AdmissionWebhook should include webhook resources in
/// discovery documents [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:96
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Verifies the api-server's `/apis/admissionregistration.k8s.io/v1`
/// discovery document lists `validatingwebhookconfigurations`,
/// `mutatingwebhookconfigurations`, `validatingadmissionpolicies` and
/// `validatingadmissionpolicybindings` with the expected verbs.
#[tokio::test]
async fn should_include_webhook_resources_in_discovery_documents() {
    let (_mem, router) = spawn_router();
    let (status, body) = get_json(router, "/apis/admissionregistration.k8s.io/v1").await;
    assert_eq!(status, StatusCode::OK, "discovery must return 200: {body}");

    let resources = body["resources"]
        .as_array()
        .expect("APIResourceList.resources must be an array");
    let names: Vec<&str> = resources
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();

    for required in [
        "validatingwebhookconfigurations",
        "mutatingwebhookconfigurations",
        "validatingadmissionpolicies",
        "validatingadmissionpolicybindings",
    ] {
        assert!(
            names.contains(&required),
            "discovery must list {required}; got {names:?}"
        );
    }

    // Each webhook resource must list the standard verbs (the upstream test
    // only asserts presence, but the verb list is the contract that
    // kubectl + client-go rely on).
    let vwc = resources
        .iter()
        .find(|r| r["name"] == "validatingwebhookconfigurations")
        .unwrap();
    let verbs: Vec<&str> = vwc["verbs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    for v in [
        "create", "delete", "get", "list", "patch", "update", "watch",
    ] {
        assert!(
            verbs.contains(&v),
            "VWC must support verb {v}; got {verbs:?}"
        );
    }
}

/// [sig-api-machinery] AdmissionWebhook should be able to deny pod and
/// configmap creation [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:167
/// Integration-tests the deny path via `AdmissionWebhookManager`: a
/// ValidatingWebhookConfiguration with rules for `pods` and `configmaps`
/// (CREATE) must produce `AdmissionResponse::Deny` for matching requests.
#[tokio::test]
async fn should_be_able_to_deny_pod_and_configmap_creation() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_deny_validator("denied by webhook".to_string()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-pod-cm"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "pods"), rule_for("", "v1", "configmaps")],
            ..validating("deny.k8s.io", url, vec![], Some(FailurePolicy::Fail), None)
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-pod-cm"),
        &cfg,
    )
    .await
    .unwrap();

    for (resource, kind) in [("pods", "Pod"), ("configmaps", "ConfigMap")] {
        let resp = manager
            .run_validating_webhooks(
                &Operation::Create,
                &GroupVersionKind {
                    group: "".into(),
                    version: "v1".into(),
                    kind: kind.into(),
                },
                &GroupVersionResource {
                    group: "".into(),
                    version: "v1".into(),
                    resource: resource.into(),
                },
                Some("default"),
                "obj",
                Some(json!({"metadata": {"name": "obj"}})),
                None,
                &admin_user_info(),
            )
            .await
            .unwrap();
        match resp {
            AdmissionResponse::Deny(reason) => {
                assert!(
                    reason.contains("denied by webhook"),
                    "deny reason: {reason}"
                );
            }
            other => panic!("{resource}: expected Deny, got {other:?}"),
        }
    }
}

/// [sig-api-machinery] AdmissionWebhook should be able to deny attaching pod
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:180
/// Asserts a webhook scoped to the `pods/attach` subresource denies the
/// operation. `resource_matches` splits the request's `pods/attach` GVR on
/// `/` and matches the rule's `pods/attach` entry; the operation is CONNECT
/// because attach is a streaming subresource in K8s.
#[tokio::test]
async fn should_be_able_to_deny_attaching_pod() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_deny_validator("attach denied".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-attach"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Connect],
                rule: Rule {
                    api_groups: vec!["".into()],
                    api_versions: vec!["v1".into()],
                    resources: vec!["pods/attach".into()],
                    scope: None,
                },
            }],
            ..validating(
                "deny.attach.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-attach"),
        &cfg,
    )
    .await
    .unwrap();

    // K8s models POST /pods/<name>/attach as a CONNECT admission operation
    // (see kubernetes/staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go
    // — Connect verbs map to admission.Connect). The dispatcher matches the
    // rule's `pods/attach` subresource against the request's resource string
    // via the `<resource>/<sub>` split in `resource_matches`.
    let resp = manager
        .run_validating_webhooks(
            &Operation::Connect,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "PodAttachOptions".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "pods/attach".into(),
            },
            Some("default"),
            "target-pod",
            Some(json!({"kind":"PodAttachOptions"})),
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    match resp {
        AdmissionResponse::Deny(_) => {}
        other => panic!("attach must be denied, got {other:?}"),
    }
}

/// [sig-api-machinery] AdmissionWebhook should be able to deny custom
/// resource creation, update and deletion [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:193
/// Verifies a webhook bound to a CRD's resource (`example.com/v1/foos`)
/// denies all of CREATE/UPDATE/DELETE — the dispatcher routes by
/// `(apiGroup, version, resource)` exactly like any built-in resource.
#[tokio::test]
async fn should_be_able_to_deny_custom_resource_creation_update_and_deletion() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_deny_validator("cr denied".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-cr"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![RuleWithOperations {
                operations: vec![
                    OperationType::Create,
                    OperationType::Update,
                    OperationType::Delete,
                ],
                rule: Rule {
                    api_groups: vec!["example.com".into()],
                    api_versions: vec!["v1".into()],
                    resources: vec!["foos".into()],
                    scope: None,
                },
            }],
            ..validating("deny.cr.io", url, vec![], Some(FailurePolicy::Fail), None)
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-cr"),
        &cfg,
    )
    .await
    .unwrap();

    for op in [Operation::Create, Operation::Update, Operation::Delete] {
        let resp = manager
            .run_validating_webhooks(
                &op,
                &GroupVersionKind {
                    group: "example.com".into(),
                    version: "v1".into(),
                    kind: "Foo".into(),
                },
                &GroupVersionResource {
                    group: "example.com".into(),
                    version: "v1".into(),
                    resource: "foos".into(),
                },
                Some("default"),
                "my-foo",
                Some(json!({"apiVersion":"example.com/v1","kind":"Foo","metadata":{"name":"my-foo"}})),
                None,
                &admin_user_info(),
            )
            .await
            .unwrap();
        match resp {
            AdmissionResponse::Deny(_) => {}
            other => panic!("op {op:?} must be denied, got {other:?}"),
        }
    }
}

/// [sig-api-machinery] AdmissionWebhook should unconditionally reject
/// operations on fail closed webhook [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:212
/// Sonobuoy (Round 160): PASS
///
/// A webhook with `failurePolicy: Fail` and no reachable backend must
/// cause matching operations to be rejected.
#[tokio::test]
async fn should_unconditionally_reject_operations_on_fail_closed_webhook() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());

    // Unreachable URL + FailurePolicy::Fail + short timeout → manager must
    // surface the failure as an error (which the api-server pipeline maps
    // to a 500/Deny depending on call site).
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("fail-closed"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            timeout_seconds: Some(1),
            ..validating(
                "fail.closed.io",
                "http://127.0.0.1:1/unreachable".to_string(),
                vec![],
                Some(FailurePolicy::Fail),
                Some(1),
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "fail-closed"),
        &cfg,
    )
    .await
    .unwrap();

    let result = manager
        .run_validating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            Some("default"),
            "cm-fail-closed",
            Some(json!({"metadata":{"name":"cm-fail-closed"}})),
            None,
            &admin_user_info(),
        )
        .await;

    assert!(
        result.is_err(),
        "fail-closed webhook with unreachable backend must surface an error, got {result:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should mutate configmap [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:226
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_mutate_configmap() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_mutator_label("mutation-stage-1".into(), "yes".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cm"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            ..mutating("mutate.cm.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cm"),
        &cfg,
    )
    .await
    .unwrap();

    let object = Some(json!({"metadata":{"name":"cm-1","labels":{}}}));
    let (_resp, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            Some("default"),
            "cm-1",
            object,
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    let obj = mutated.expect("mutated object");
    assert_eq!(obj["metadata"]["labels"]["mutation-stage-1"], json!("yes"));
}

/// [sig-api-machinery] AdmissionWebhook should mutate pod and apply defaults
/// after mutation [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:240
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_mutate_pod_and_apply_defaults_after_mutation() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_mutator_init_container().await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-pod"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "pods")],
            ..mutating("mutate.pod.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-pod"),
        &cfg,
    )
    .await
    .unwrap();

    let pod = Some(json!({
        "metadata": {"name": "p1", "labels": {}},
        "spec": {"containers": [{"name": "main", "image": "nginx"}]}
    }));
    let (_resp, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "Pod".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "pods".into(),
            },
            Some("default"),
            "p1",
            pod,
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    let obj = mutated.expect("mutated object");
    let init = obj["spec"]["initContainers"]
        .as_array()
        .expect("initContainers must be present after mutation");
    assert_eq!(init.len(), 1);
    assert_eq!(init[0]["name"], json!("webhook-added-init"));
}

/// [sig-api-machinery] AdmissionWebhook should not be able to mutate or
/// prevent deletion of webhook configuration objects [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:254
/// Sonobuoy (Round 160): PASS
///
/// A deny-everything webhook that registers itself against admissionregistration
/// resources must NOT be invoked on those resources — that would lock out the
/// cluster. We assert that even with such a webhook configured, the
/// `webhook_matches()` filter excludes its own resource kind. We exercise via
/// HTTP: register the deny webhook, then PUT an update to its own config and
/// expect 200, then DELETE it and expect 200.
#[tokio::test]
async fn should_not_be_able_to_mutate_or_prevent_deletion_of_webhook_configuration_objects() {
    let (mem, router) = spawn_router();
    let (raw_url, _shutdown) = start_deny_validator("would deny everything".into()).await;
    // clientConfig.url must use the https scheme (upstream validateWebhookURL,
    // enforced on the update path here). This test updates the webhook config
    // itself through the router, so the URL is validated. The deny server is
    // only contacted if the self-targeting protection regressed and the webhook
    // actually fired — where the scheme/transport mismatch still yields a
    // non-200, keeping the assertion meaningful.
    let url = raw_url.replacen("http://", "https://", 1);

    // Register a deny-all webhook directly in storage to skip the create
    // round-trip's CEL validation overhead.
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("self-targeting"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for(
                "admissionregistration.k8s.io",
                "v1",
                "validatingwebhookconfigurations",
            )],
            ..validating(
                "self.target.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "self-targeting"),
        &cfg,
    )
    .await
    .unwrap();

    // Update via PUT through the real router. If the webhook were invoked
    // we'd see a 5xx; if the protection works we see 200.
    let mut updated = cfg.clone();
    updated.metadata.labels = Some({
        let mut m = std::collections::HashMap::new();
        m.insert("touched".into(), "true".into());
        m
    });
    let (status, body) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/self-targeting",
        &serde_json::to_value(&updated).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update must succeed: {body}");

    // DELETE must also succeed (200/202/204 — any 2xx).
    let status = delete_status(
        router,
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/self-targeting",
    )
    .await;
    assert!(status.is_success(), "delete must succeed, got {status}");
}

/// [sig-api-machinery] AdmissionWebhook should mutate custom resource
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:270
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_mutate_custom_resource() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_mutator_label("mutated-by".into(), "webhook".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cr"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("example.com", "v1", "foos")],
            ..mutating("mutate.cr.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cr"),
        &cfg,
    )
    .await
    .unwrap();

    let cr = Some(json!({
        "apiVersion": "example.com/v1",
        "kind": "Foo",
        "metadata": {"name": "cr-1", "labels": {}}
    }));
    let (_resp, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "example.com".into(),
                version: "v1".into(),
                kind: "Foo".into(),
            },
            &GroupVersionResource {
                group: "example.com".into(),
                version: "v1".into(),
                resource: "foos".into(),
            },
            Some("default"),
            "cr-1",
            cr,
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    let obj = mutated.expect("mutated CR");
    assert_eq!(obj["metadata"]["labels"]["mutated-by"], json!("webhook"));
}

/// [sig-api-machinery] AdmissionWebhook should deny crd creation [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:288
/// Sonobuoy (Round 160): PASS
///
/// A validating webhook scoped to
/// `apiextensions.k8s.io/v1.customresourcedefinitions` must deny CRD CREATE.
#[tokio::test]
async fn should_deny_crd_creation() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_deny_validator("crd denied".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-crd"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for(
                "apiextensions.k8s.io",
                "v1",
                "customresourcedefinitions",
            )],
            ..validating("deny.crd.io", url, vec![], Some(FailurePolicy::Fail), None)
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-crd"),
        &cfg,
    )
    .await
    .unwrap();

    let resp = manager
        .run_validating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "apiextensions.k8s.io".into(),
                version: "v1".into(),
                kind: "CustomResourceDefinition".into(),
            },
            &GroupVersionResource {
                group: "apiextensions.k8s.io".into(),
                version: "v1".into(),
                resource: "customresourcedefinitions".into(),
            },
            None,
            "foos.example.com",
            Some(json!({"metadata":{"name":"foos.example.com"}})),
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    match resp {
        AdmissionResponse::Deny(reason) => assert!(reason.contains("crd denied")),
        other => panic!("expected Deny, got {other:?}"),
    }
}

/// [sig-api-machinery] AdmissionWebhook should mutate custom resource with
/// different stored version [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:304
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_mutate_custom_resource_with_different_stored_version() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_mutator_label("crv".into(), "v2".into()).await;

    // Register a webhook matching both v1 and v2 of the CR. The manager must
    // dispatch to it regardless of the stored version (the upstream test
    // creates the CR via v1, then again via v2, and asserts both are mutated).
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cr-vers"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["example.com".into()],
                    api_versions: vec!["v1".into(), "v2".into()],
                    resources: vec!["foos".into()],
                    scope: None,
                },
            }],
            ..mutating("mutate.cr.vers.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cr-vers"),
        &cfg,
    )
    .await
    .unwrap();

    for version in ["v1", "v2"] {
        let cr = Some(json!({
            "apiVersion": format!("example.com/{version}"),
            "kind": "Foo",
            "metadata": {"name": "cv", "labels": {}}
        }));
        let (_resp, mutated) = manager
            .run_mutating_webhooks(
                &Operation::Create,
                &GroupVersionKind {
                    group: "example.com".into(),
                    version: version.into(),
                    kind: "Foo".into(),
                },
                &GroupVersionResource {
                    group: "example.com".into(),
                    version: version.into(),
                    resource: "foos".into(),
                },
                Some("default"),
                "cv",
                cr,
                None,
                &admin_user_info(),
            )
            .await
            .unwrap();
        let obj = mutated.expect("mutated CR");
        assert_eq!(
            obj["metadata"]["labels"]["crv"],
            json!("v2"),
            "version {version}"
        );
    }
}

/// [sig-api-machinery] AdmissionWebhook should mutate custom resource with
/// pruning [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:323
///
/// Verifies the K8s contract: after a mutating webhook injects a field into
/// a CR via JSON patch, the api-server runs structural-schema pruning. Any
/// field absent from the CRD's `openAPIV3Schema` must be stripped (unless
/// `x-kubernetes-preserve-unknown-fields` is set).
///
/// Setup: a CRD declares `spec.replicas` only. A mutating webhook adds an
/// extra field `/spec/notInSchema`. After `run_mutating_webhooks`, the
/// returned object must have `spec.replicas` intact and `spec.notInSchema`
/// removed by the pruning pass.
#[tokio::test]
async fn should_mutate_custom_resource_with_pruning() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());

    // Persist a CRD whose structural schema declares only `spec.replicas`.
    // Pruning must remove any other field a webhook injects under `spec`.
    let crd_json = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "foos.example.com"},
        "spec": {
            "group": "example.com",
            "names": {"plural": "foos", "singular": "foo", "kind": "Foo", "listKind": "FooList"},
            "scope": "Namespaced",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "replicas": {"type": "integer"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    });
    let crd: rusternetes_common::resources::CustomResourceDefinition =
        serde_json::from_value(crd_json).unwrap();
    mem.create(
        &build_key("customresourcedefinitions", None, "foos.example.com"),
        &crd,
    )
    .await
    .unwrap();

    // Mutator adds an unknown field under `spec`. With the CRD above this
    // field is NOT in the schema, so the api-server must strip it after the
    // webhook returns.
    let (url, _shutdown) =
        start_mutator_path("/spec/notInSchema".into(), json!("should-be-pruned")).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-prune"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("example.com", "v1", "foos")],
            ..mutating("mutate.prune.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-prune"),
        &cfg,
    )
    .await
    .unwrap();

    let cr = Some(json!({
        "apiVersion": "example.com/v1",
        "kind": "Foo",
        "metadata": {"name": "cr-prune"},
        "spec": {"replicas": 3}
    }));
    let (_resp, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "example.com".into(),
                version: "v1".into(),
                kind: "Foo".into(),
            },
            &GroupVersionResource {
                group: "example.com".into(),
                version: "v1".into(),
                resource: "foos".into(),
            },
            Some("default"),
            "cr-prune",
            cr,
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    let obj = mutated.expect("mutated CR");
    assert_eq!(
        obj["spec"]["replicas"],
        json!(3),
        "declared schema fields must survive pruning; got {obj}"
    );
    assert!(
        obj["spec"].get("notInSchema").is_none(),
        "schema pruning must remove the webhook-added field after mutation; got {obj}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should honor timeout [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:358
/// Sonobuoy (Round 160+): PASS — the slow webhook is aborted at the
/// `timeoutSeconds` boundary and the surfaced error includes the upstream
/// "HTTP/dial timeout" phrase the conformance suite asserts the literal text
/// of. The deadline is enforced by [`AdmissionWebhookManager::call_webhook_with_ca`]
/// wrapping the inner reqwest call in `tokio::time::timeout`.
#[tokio::test]
async fn should_honor_timeout() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    // The webhook's own response latency. The 1s `timeoutSeconds` below must
    // abort the call long before this elapses. Keeping a wide gap lets the
    // wall-clock assertion tolerate tokio-timer wakeup jitter under
    // CPU-saturated parallel test load — the old fixed `< 3s` bound flaked at
    // ~3.1s on a loaded runner even though the deadline fired correctly. When
    // the timeout works the test still returns in ~1s; this longer sleep only
    // matters as an upper bound if the timeout regresses.
    let slow_webhook_sleep = std::time::Duration::from_secs(10);
    let (url, _shutdown) = start_slow_validator(slow_webhook_sleep).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("slow-fail"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            timeout_seconds: Some(1),
            ..validating("slow.io", url, vec![], Some(FailurePolicy::Fail), Some(1))
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "slow-fail"),
        &cfg,
    )
    .await
    .unwrap();

    let started = std::time::Instant::now();
    let res = manager
        .run_validating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            Some("default"),
            "cm-slow",
            Some(json!({"metadata":{"name":"cm-slow"}})),
            None,
            &admin_user_info(),
        )
        .await;
    let elapsed = started.elapsed();

    // Primary, load-independent proof the deadline fired: the slow validator
    // only ever returns `allow`, so an Err can only originate from the 1s
    // timeout aborting the call (the message assertion below pins it to the
    // timeout specifically). The wall-clock check is a secondary guard that the
    // call was aborted *early* rather than blocking for the webhook's full
    // response — asserted relative to `slow_webhook_sleep` so it does not flake
    // when the runtime's timers are delayed under parallel-test CPU pressure.
    assert!(res.is_err(), "slow webhook + FailurePolicy=Fail must error");
    assert!(
        elapsed < slow_webhook_sleep,
        "1s deadline must abort before the {slow_webhook_sleep:?} webhook response; took {elapsed:?}"
    );
    // Upstream expects the literal substring "HTTP/dial timeout" — the gap.
    let msg = format!("{}", res.unwrap_err()).to_lowercase();
    assert!(
        msg.contains("http/dial timeout"),
        "upstream test asserts literal 'HTTP/dial timeout' substring; got {msg:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook patching/updating a validating
/// webhook should work [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:391
/// Sonobuoy (Round 160): PASS
///
/// Verifies that POST → GET → PUT → GET round-trips a ValidatingWebhookConfiguration
/// through the REST API and preserves an update to the `rules` field.
#[tokio::test]
async fn patching_updating_a_validating_webhook_should_work() {
    let (_mem, router) = spawn_router();

    // Build a minimal config with a single rule on pods.
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("vwc-patchable"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "pods")],
            ..validating(
                "vwc.patch.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, _body) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Read back; mutate rules to also cover configmaps; PUT.
    let (_, body) = get_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-patchable",
    )
    .await;
    let mut updated: ValidatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    if let Some(ref mut hooks) = updated.webhooks {
        hooks[0].rules.push(rule_for("", "v1", "configmaps"));
    }
    let (status, _) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-patchable",
        &serde_json::to_value(&updated).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Verify the PUT stuck.
    let (_, body2) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-patchable",
    )
    .await;
    let final_cfg: ValidatingWebhookConfiguration = serde_json::from_value(body2).unwrap();
    let rules = &final_cfg.webhooks.unwrap()[0].rules;
    let resources: Vec<&str> = rules
        .iter()
        .flat_map(|r| r.rule.resources.iter().map(|s| s.as_str()))
        .collect();
    assert!(resources.contains(&"pods"));
    assert!(resources.contains(&"configmaps"));
}

/// [sig-api-machinery] AdmissionWebhook patching/updating a mutating webhook
/// should work [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:492
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn patching_updating_a_mutating_webhook_should_work() {
    let (_mem, router) = spawn_router();

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mwc-patchable"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "pods")],
            ..mutating(
                "mwc.patch.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, _body) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (_, body) = get_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-patchable",
    )
    .await;
    let mut updated: MutatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    if let Some(ref mut hooks) = updated.webhooks {
        hooks[0].reinvocation_policy = Some(ReinvocationPolicy::IfNeeded);
    }
    let (status, _) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-patchable",
        &serde_json::to_value(&updated).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body2) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-patchable",
    )
    .await;
    let final_cfg: MutatingWebhookConfiguration = serde_json::from_value(body2).unwrap();
    assert_eq!(
        final_cfg.webhooks.unwrap()[0].reinvocation_policy,
        Some(ReinvocationPolicy::IfNeeded)
    );
}

/// [sig-api-machinery] AdmissionWebhook listing validating webhooks should
/// work [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:594
/// Sonobuoy (Round 160): PASS
///
/// Creates several VWCs, lists them, then deletes the collection and
/// asserts the list is empty.
#[tokio::test]
async fn listing_validating_webhooks_should_work() {
    let (_mem, router) = spawn_router();

    for name in ["v-list-a", "v-list-b", "v-list-c"] {
        let cfg = ValidatingWebhookConfiguration {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
            metadata: rusternetes_common::types::ObjectMeta::new(name),
            webhooks: Some(vec![ValidatingWebhook {
                rules: vec![rule_for("", "v1", "configmaps")],
                ..validating(
                    &format!("{name}.k8s.io"),
                    "https://example.invalid/hook".to_string(),
                    vec![],
                    Some(FailurePolicy::Ignore),
                    None,
                )
            }]),
        };
        let (status, _) = post_json(
            router.clone(),
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            &serde_json::to_value(&cfg).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "create {name}");
    }

    let (status, body) = get_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().expect("list must have items");
    let names: std::collections::HashSet<&str> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    for n in ["v-list-a", "v-list-b", "v-list-c"] {
        assert!(names.contains(n), "list missing {n}");
    }

    // DeleteCollection.
    let status = delete_status(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
    )
    .await;
    assert!(status.is_success());

    let (_, body) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
    )
    .await;
    let items = body["items"].as_array().expect("list must have items");
    assert!(
        items.is_empty(),
        "list must be empty after deletecollection; got {items:?}"
    );
}

/// [sig-api-machinery] AdmissionWebhook listing mutating webhooks should
/// work [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:669
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn listing_mutating_webhooks_should_work() {
    let (_mem, router) = spawn_router();

    for name in ["m-list-a", "m-list-b", "m-list-c"] {
        let cfg = MutatingWebhookConfiguration {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "MutatingWebhookConfiguration".to_string(),
            metadata: rusternetes_common::types::ObjectMeta::new(name),
            webhooks: Some(vec![MutatingWebhook {
                rules: vec![rule_for("", "v1", "configmaps")],
                ..mutating(
                    &format!("{name}.k8s.io"),
                    "https://example.invalid/hook".to_string(),
                    vec![],
                    Some(FailurePolicy::Ignore),
                    None,
                )
            }]),
        };
        let (status, _) = post_json(
            router.clone(),
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
            &serde_json::to_value(&cfg).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "create {name}");
    }

    let (_, body) = get_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
    )
    .await;
    let items = body["items"].as_array().expect("list must have items");
    let names: std::collections::HashSet<&str> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    for n in ["m-list-a", "m-list-b", "m-list-c"] {
        assert!(names.contains(n), "list missing {n}");
    }

    let status = delete_status(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
    )
    .await;
    assert!(status.is_success());

    let (_, body) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
    )
    .await;
    let items = body["items"].as_array().expect("list must have items");
    assert!(
        items.is_empty(),
        "list must be empty after deletecollection"
    );
}

/// [sig-api-machinery] AdmissionWebhook should be able to create and update
/// validating webhook configurations with match conditions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:744
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_be_able_to_create_and_update_validating_webhook_configurations_with_match_conditions(
) {
    let (_mem, router) = spawn_router();

    let mc = vec![MatchCondition {
        name: "exclude-leases".into(),
        // Use a CEL expression that the api-server's permissive matcher
        // accepts (references undeclared variables fall through the
        // type-checker per handlers/admission_webhook.rs:75-82).
        expression: "object.metadata.name != 'leases'".into(),
    }];

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("vwc-match-cond"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(mc.clone()),
            ..validating(
                "match.cond.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, body) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");

    // Update: add a second match condition.
    let mut updated: ValidatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    if let Some(ref mut hooks) = updated.webhooks {
        let conds = hooks[0].match_conditions.get_or_insert_with(Vec::new);
        conds.push(MatchCondition {
            name: "exclude-events".into(),
            expression: "object.kind != 'Event'".into(),
        });
    }
    let (status, _) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-match-cond",
        &serde_json::to_value(&updated).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/vwc-match-cond",
    )
    .await;
    let final_cfg: ValidatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    let conds = final_cfg.webhooks.unwrap()[0]
        .match_conditions
        .clone()
        .unwrap();
    assert_eq!(conds.len(), 2);
}

/// [sig-api-machinery] AdmissionWebhook should be able to create and update
/// mutating webhook configurations with match conditions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:799
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_be_able_to_create_and_update_mutating_webhook_configurations_with_match_conditions()
{
    let (_mem, router) = spawn_router();

    let mc = vec![MatchCondition {
        name: "exclude-system".into(),
        expression: "object.metadata.namespace != 'kube-system'".into(),
    }];

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mwc-match-cond"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(mc),
            ..mutating(
                "mwc.match.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, body) = post_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");

    let mut updated: MutatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    if let Some(ref mut hooks) = updated.webhooks {
        let conds = hooks[0].match_conditions.get_or_insert_with(Vec::new);
        conds.push(MatchCondition {
            name: "exclude-priv".into(),
            expression: "object.metadata.name != 'priv'".into(),
        });
    }
    let (status, _) = put_json(
        router.clone(),
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-match-cond",
        &serde_json::to_value(&updated).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = get_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/mwc-match-cond",
    )
    .await;
    let final_cfg: MutatingWebhookConfiguration = serde_json::from_value(body).unwrap();
    let conds = final_cfg.webhooks.unwrap()[0]
        .match_conditions
        .clone()
        .unwrap();
    assert_eq!(conds.len(), 2);
}

/// [sig-api-machinery] AdmissionWebhook should reject validating webhook
/// configurations with invalid match conditions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:854
/// Sonobuoy (Round 160): PASS
///
/// The api-server compiles every CEL `matchConditions[].expression` at
/// admission time; invalid syntax must produce a 4xx (the handler maps it
/// to `InvalidResource` → 422).
#[tokio::test]
async fn should_reject_validating_webhook_configurations_with_invalid_match_conditions() {
    let (_mem, router) = spawn_router();

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("vwc-invalid-mc"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(vec![MatchCondition {
                name: "bad".into(),
                // Empty expression is the cheapest path to "invalid" that
                // the handler explicitly rejects (admission_webhook.rs:47).
                expression: "".into(),
            }]),
            ..validating(
                "invalid.mc.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, body) = post_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert!(
        status.is_client_error(),
        "create with invalid CEL must be 4xx; got {status} {body}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should reject mutating webhook
/// configurations with invalid match conditions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:884
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn should_reject_mutating_webhook_configurations_with_invalid_match_conditions() {
    let (_mem, router) = spawn_router();

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mwc-invalid-mc"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            match_conditions: Some(vec![MatchCondition {
                name: "".into(), // empty name → InvalidResource (handler:53)
                expression: "true".into(),
            }]),
            ..mutating(
                "invalid.mwc.io",
                "https://example.invalid/hook".to_string(),
                vec![],
                Some(FailurePolicy::Ignore),
                None,
            )
        }]),
    };
    let (status, body) = post_json(
        router,
        "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
        &serde_json::to_value(&cfg).unwrap(),
    )
    .await;
    assert!(
        status.is_client_error(),
        "create with invalid match condition must be 4xx; got {status} {body}"
    );
}

/// [sig-api-machinery] AdmissionWebhook should mutate everything except
/// 'skip-me' configmaps [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/webhook.go:914
/// Sonobuoy (Round 160): PASS
///
/// Verifies an `objectSelector` based on labels excludes specific objects
/// from the mutating webhook. `webhook_matches()` evaluates the selector
/// before dispatching; an object missing the `skip` label must be mutated.
#[tokio::test]
async fn should_mutate_everything_except_skip_me_configmaps() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());
    let (url, _shutdown) = start_mutator_label("mutated".into(), "1".into()).await;

    use std::collections::HashMap;
    let mut match_labels = HashMap::new();
    match_labels.insert("skip-me".into(), "false".into());

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("skip-me"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            object_selector: Some(rusternetes_common::resources::LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            }),
            ..mutating("skip.me.io", url, vec![], None, None)
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "skip-me"),
        &cfg,
    )
    .await
    .unwrap();

    // Object with `skip-me=true` must NOT be mutated.
    let skip_obj_value = json!({
        "metadata": {"name": "cm-skip", "labels": {"skip-me": "true"}}
    });
    let (_, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            Some("default"),
            "cm-skip",
            Some(skip_obj_value.clone()),
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    // Object unchanged: labels still {skip-me: true}, no `mutated` key.
    let obj = mutated.unwrap_or(skip_obj_value);
    assert!(
        obj["metadata"]["labels"].get("mutated").is_none(),
        "objectSelector must skip objects with skip-me=true; got {obj}"
    );

    // Object with `skip-me=false` (matches selector) MUST be mutated.
    let go_obj = Some(json!({
        "metadata": {"name": "cm-go", "labels": {"skip-me": "false"}}
    }));
    let (_, mutated2) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".into(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            &GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "configmaps".into(),
            },
            Some("default"),
            "cm-go",
            go_obj,
            None,
            &admin_user_info(),
        )
        .await
        .unwrap();
    let obj2 = mutated2.expect("matching object must be mutated");
    assert_eq!(obj2["metadata"]["labels"]["mutated"], json!("1"));
}

/// [sig-api-machinery] AdmissionWebhook — a validating webhook scoped to the
/// DELETE operation on a CORE resource must be invoked when that resource is
/// deleted through the HTTP DELETE handler, and its denial must turn into a
/// 403 with the upstream "admission webhook denied the request" prefix.
///
/// This exercises the api-server's *handler wiring* (not just the manager):
/// before this change, only custom resources ran webhooks on DELETE, so core
/// resources (configmaps/pods/secrets) silently skipped DELETE admission.
///
/// Upstream parity: webhook.go's "deny creation/deletion" specs configure rules
/// with both CREATE and DELETE; the DELETE AdmissionReview sets `object=nil`
/// and `oldObject=<resource>`. The mock here asserts exactly that shape.
#[tokio::test]
async fn should_deny_configmap_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("nope, keep it".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-cm-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("", "v1", "configmaps")],
            ..validating(
                "deny.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-cm-delete"),
        &cfg,
    )
    .await
    .unwrap();

    // Seed a configmap directly in storage.
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "protected", "namespace": "default"},
        "data": {"k": "v"},
    });
    let cm_typed: rusternetes_common::resources::ConfigMap = serde_json::from_value(cm).unwrap();
    mem.create(
        &build_key("configmaps", Some("default"), "protected"),
        &cm_typed,
    )
    .await
    .unwrap();

    let (status, body) =
        delete_json(router, "/api/v1/namespaces/default/configmaps/protected").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request") && msg.contains("nope, keep it"),
        "deny message should carry the webhook reason: {body}"
    );

    // The configmap must still exist (deletion was rejected pre-storage).
    let still: rusternetes_common::resources::ConfigMap = mem
        .get(&build_key("configmaps", Some("default"), "protected"))
        .await
        .expect("configmap must survive a denied delete");
    assert_eq!(still.metadata.name, "protected");
}

/// Sibling of the above for pods, going through `delete_pod`. A DELETE-scoped
/// webhook must reject the pod deletion with 403; the pod stays in storage and
/// never gets a `deletionTimestamp`.
#[tokio::test]
async fn should_deny_pod_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("pod is load-bearing".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-pod-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("", "v1", "pods")],
            ..validating(
                "deny.pod.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-pod-delete"),
        &cfg,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "keepme", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
    });
    let pod_typed: rusternetes_common::resources::Pod = serde_json::from_value(pod).unwrap();
    mem.create(&build_key("pods", Some("default"), "keepme"), &pod_typed)
        .await
        .unwrap();

    let (status, body) = delete_json(router, "/api/v1/namespaces/default/pods/keepme").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "pod delete must be denied: {body}"
    );

    // The pod must still exist with no deletionTimestamp (delete was blocked).
    let still: rusternetes_common::resources::Pod = mem
        .get(&build_key("pods", Some("default"), "keepme"))
        .await
        .expect("pod must survive a denied delete");
    assert!(
        still.metadata.deletion_timestamp.is_none(),
        "denied delete must not set deletionTimestamp"
    );
}

/// Control case: with a DELETE webhook configured but scoped to a *different*
/// resource, deleting a configmap is unaffected and succeeds. Guards against an
/// over-broad matcher that would invoke the webhook for every resource.
#[tokio::test]
async fn delete_webhook_scoped_to_other_resource_does_not_block() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("should not fire".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-secret-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            // Scoped to secrets, NOT configmaps.
            rules: vec![delete_rule_for("", "v1", "secrets")],
            ..validating(
                "deny.secret.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-secret-delete",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "free", "namespace": "default"},
        "data": {},
    });
    let cm_typed: rusternetes_common::resources::ConfigMap = serde_json::from_value(cm).unwrap();
    mem.create(&build_key("configmaps", Some("default"), "free"), &cm_typed)
        .await
        .unwrap();

    let status = delete_status(router, "/api/v1/namespaces/default/configmaps/free").await;
    assert!(
        status.is_success(),
        "configmap delete must succeed when webhook targets a different resource, got {status}"
    );
}

// ===========================================================================
// Convenience harness self-checks. These confirm the in-file helpers behave
// the way every test above relies on. Not Ginkgo mirrors, hence private
// names and no docstrings beyond the comment.
// ===========================================================================

#[tokio::test]
async fn harness_request_builder_compiles_an_admission_request() {
    let req = admission_request(
        "",
        "v1",
        "ConfigMap",
        "configmaps",
        Some("default"),
        "cm",
        json!({"metadata": {"name": "cm"}}),
    );
    assert_eq!(req.uid, "uid-cm");
    assert_eq!(req.namespace.as_deref(), Some("default"));
    assert!(matches!(req.operation, Operation::Create));
}

// ===========================================================================
// DELETE admission webhook wiring — extended resource coverage (PR #901).
// These mirror the configmap/pod/secret delete-deny tests above but exercise
// the newly wired handlers: deployments (apps/v1), services (core v1),
// jobs (batch/v1), and namespaces (cluster-scoped core v1).
// ===========================================================================

/// A validating webhook scoped to DELETE on apps/v1 deployments must be
/// invoked by `delete_deployment` and its denial must produce 403.
#[tokio::test]
async fn should_deny_deployment_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("no deployers allowed".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-deploy-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("apps", "v1", "deployments")],
            ..validating(
                "deny.deploy.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-deploy-delete",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "my-deploy", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });
    let typed: rusternetes_common::resources::Deployment = serde_json::from_value(deploy).unwrap();
    mem.create(
        &build_key("deployments", Some("default"), "my-deploy"),
        &typed,
    )
    .await
    .unwrap();

    let (status, body) = delete_json(
        router,
        "/apis/apps/v1/namespaces/default/deployments/my-deploy",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "deployment delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request")
            && msg.contains("no deployers allowed"),
        "deny message should carry the webhook reason: {body}"
    );

    // Deployment must still exist.
    let _: rusternetes_common::resources::Deployment = mem
        .get(&build_key("deployments", Some("default"), "my-deploy"))
        .await
        .expect("deployment must survive a denied delete");
}

/// A validating webhook scoped to DELETE on core/v1 services must be invoked
/// by `delete_service` and its denial must produce 403.
#[tokio::test]
async fn should_deny_service_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("service is precious".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-svc-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("", "v1", "services")],
            ..validating(
                "deny.svc.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-svc-delete"),
        &cfg,
    )
    .await
    .unwrap();

    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "my-svc", "namespace": "default"},
        "spec": {"selector": {"app": "x"}, "ports": [{"port": 80}]}
    });
    let typed: rusternetes_common::resources::Service = serde_json::from_value(svc).unwrap();
    mem.create(&build_key("services", Some("default"), "my-svc"), &typed)
        .await
        .unwrap();

    let (status, body) = delete_json(router, "/api/v1/namespaces/default/services/my-svc").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "service delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request") && msg.contains("service is precious"),
        "deny message should carry the webhook reason: {body}"
    );

    let _: rusternetes_common::resources::Service = mem
        .get(&build_key("services", Some("default"), "my-svc"))
        .await
        .expect("service must survive a denied delete");
}

/// A validating webhook scoped to DELETE on batch/v1 jobs must be invoked
/// by `delete_job` and its denial must produce 403.
#[tokio::test]
async fn should_deny_job_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("job is still running".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-job-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("batch", "v1", "jobs")],
            ..validating(
                "deny.job.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-job-delete"),
        &cfg,
    )
    .await
    .unwrap();

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "my-job", "namespace": "default"},
        "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "busybox"}], "restartPolicy": "Never"}}}
    });
    let typed: rusternetes_common::resources::Job = serde_json::from_value(job).unwrap();
    mem.create(&build_key("jobs", Some("default"), "my-job"), &typed)
        .await
        .unwrap();

    let (status, body) = delete_json(router, "/apis/batch/v1/namespaces/default/jobs/my-job").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "job delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request")
            && msg.contains("job is still running"),
        "deny message should carry the webhook reason: {body}"
    );

    let _: rusternetes_common::resources::Job = mem
        .get(&build_key("jobs", Some("default"), "my-job"))
        .await
        .expect("job must survive a denied delete");
}

/// A validating webhook scoped to DELETE on cluster-scoped namespaces must
/// be invoked by `delete_ns` and its denial must produce 403.
#[tokio::test]
async fn should_deny_namespace_deletion_via_http_handler() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("namespace is protected".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-ns-delete"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![delete_rule_for("", "v1", "namespaces")],
            ..validating(
                "deny.ns.delete.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "deny-ns-delete"),
        &cfg,
    )
    .await
    .unwrap();

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "my-ns"},
    });
    let typed: rusternetes_common::resources::Namespace = serde_json::from_value(ns).unwrap();
    mem.create(&build_key("namespaces", None, "my-ns"), &typed)
        .await
        .unwrap();

    let (status, body) = delete_json(router, "/api/v1/namespaces/my-ns").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "namespace delete must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("admission webhook denied the request")
            && msg.contains("namespace is protected"),
        "deny message should carry the webhook reason: {body}"
    );

    let _: rusternetes_common::resources::Namespace = mem
        .get(&build_key("namespaces", None, "my-ns"))
        .await
        .expect("namespace must survive a denied delete");
}

/// Control: a DELETE webhook scoped to a *different* resource (secrets) must
/// NOT fire when deleting a deployment. The deployment delete must succeed.
#[tokio::test]
async fn delete_webhook_scoped_to_secrets_does_not_block_deployment_delete() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_delete_deny_validator("should not fire".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-secret-not-deploy"),
        webhooks: Some(vec![ValidatingWebhook {
            // Scoped to secrets, NOT deployments.
            rules: vec![delete_rule_for("", "v1", "secrets")],
            ..validating(
                "deny.secret.not.deploy.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-secret-not-deploy",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "free-deploy", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });
    let typed: rusternetes_common::resources::Deployment = serde_json::from_value(deploy).unwrap();
    mem.create(
        &build_key("deployments", Some("default"), "free-deploy"),
        &typed,
    )
    .await
    .unwrap();

    let status = delete_status(
        router,
        "/apis/apps/v1/namespaces/default/deployments/free-deploy",
    )
    .await;
    assert!(
        status.is_success(),
        "deployment delete must succeed when webhook targets secrets, got {status}"
    );
}

// ---------------------------------------------------------------------------
// CREATE-path invocation through the real HTTP handler.
//
// The pre-existing `should_mutate_configmap` / `should_be_able_to_deny_*`
// tests invoke `AdmissionWebhookManager::run_*_webhooks(...)` DIRECTLY — they
// never exercise the configmap/pod POST handler. That left a gap: live
// conformance showed core ConfigMaps/Pods coming back UNMUTATED even though
// the manager unit tests were green, because nothing verified that the
// `create` handler (a) calls the webhook and (b) PERSISTS the mutated object.
//
// These tests drive a real `POST /api/v1/namespaces/.../{configmaps,pods}`
// through `build_router`, then assert on both the HTTP response body AND the
// stored object. They are the regression guard for "core resources are not
// actually mutated/denied" (k8s webhook.go:226/240, deny pod+configmap).
// ---------------------------------------------------------------------------

/// [sig-api-machinery] AdmissionWebhook should mutate configmap [Conformance]
/// — but driven through the real HTTP create handler, asserting the mutation
/// is applied BEFORE persistence (response body + stored object both carry the
/// injected label).
#[tokio::test]
async fn mutating_webhook_mutates_configmap_through_http_create() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_mutator_label("mutated-by-webhook".into(), "yes".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-cm-http"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            ..mutating(
                "mutate.cm.http.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-cm-http"),
        &cfg,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm-http", "namespace": "default", "labels": {}},
        "data": {"k": "v"},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/configmaps", &cm).await;

    assert_eq!(status, StatusCode::CREATED, "create must succeed: {body}");
    assert_eq!(
        body["metadata"]["labels"]["mutated-by-webhook"],
        json!("yes"),
        "response body must carry the webhook-injected label: {body}"
    );

    // Crucially: the PERSISTED object must also carry the mutation.
    let stored: rusternetes_common::resources::ConfigMap = mem
        .get(&build_key("configmaps", Some("default"), "cm-http"))
        .await
        .expect("configmap must be stored");
    assert_eq!(
        stored
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("mutated-by-webhook"))
            .map(String::as_str),
        Some("yes"),
        "stored configmap must carry the webhook mutation, not just the response"
    );
}

/// Mutation must reach a Pod through the real HTTP create handler too
/// (k8s webhook.go:240 "should mutate pod and apply defaults after mutation").
#[tokio::test]
async fn mutating_webhook_mutates_pod_through_http_create() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_mutator_label("mutated-by-webhook".into(), "yes".into()).await;

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("mutate-pod-http"),
        webhooks: Some(vec![MutatingWebhook {
            rules: vec![rule_for("", "v1", "pods")],
            ..mutating(
                "mutate.pod.http.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "mutate-pod-http"),
        &cfg,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod-http", "namespace": "default", "labels": {}},
        "spec": {"containers": [{"name": "main", "image": "nginx"}]},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/pods", &pod).await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod create must succeed: {body}"
    );
    assert_eq!(
        body["metadata"]["labels"]["mutated-by-webhook"],
        json!("yes"),
        "pod response must carry the webhook-injected label: {body}"
    );

    let stored: rusternetes_common::resources::Pod = mem
        .get(&build_key("pods", Some("default"), "pod-http"))
        .await
        .expect("pod must be stored");
    assert_eq!(
        stored
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("mutated-by-webhook"))
            .map(String::as_str),
        Some("yes"),
        "stored pod must carry the webhook mutation"
    );
}

/// A validating webhook denial on ConfigMap CREATE must return 403 from the
/// real HTTP handler and must NOT persist the object
/// (k8s webhook.go: "should be able to deny pod and configmap creation").
#[tokio::test]
async fn validating_webhook_denies_configmap_through_http_create() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_deny_validator("configmap rejected by webhook".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-cm-create-http"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            ..validating(
                "deny.cm.create.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-cm-create-http",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "denied-cm", "namespace": "default"},
        "data": {"k": "v"},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/configmaps", &cm).await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "create must be denied by webhook: {body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("configmap rejected by webhook"),
        "deny message must carry the webhook reason: {body}"
    );
    assert!(
        mem.get::<rusternetes_common::resources::ConfigMap>(&build_key(
            "configmaps",
            Some("default"),
            "denied-cm"
        ))
        .await
        .is_err(),
        "denied configmap must NOT be persisted"
    );
}

/// Sibling deny test for Pod CREATE through the real HTTP handler.
#[tokio::test]
async fn validating_webhook_denies_pod_through_http_create() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_deny_validator("pod rejected by webhook".into()).await;

    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("deny-pod-create-http"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "pods")],
            ..validating(
                "deny.pod.create.io",
                url,
                vec![],
                Some(FailurePolicy::Fail),
                None,
            )
        }]),
    };
    mem.create(
        &build_key(
            "validatingwebhookconfigurations",
            None,
            "deny-pod-create-http",
        ),
        &cfg,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "denied-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/pods", &pod).await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "pod create must be denied by webhook: {body}"
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("pod rejected by webhook"),
        "deny message must carry the webhook reason: {body}"
    );
    assert!(
        mem.get::<rusternetes_common::resources::Pod>(&build_key(
            "pods",
            Some("default"),
            "denied-pod"
        ))
        .await
        .is_err(),
        "denied pod must NOT be persisted"
    );
}

/// failurePolicy=Fail: when the validating webhook backend is unreachable, a
/// ConfigMap CREATE must be REJECTED (fail-closed), not silently admitted.
/// This is the regression for "unconditionally reject operations on fail
/// closed webhook" on a core resource going through the HTTP handler.
#[tokio::test]
async fn fail_closed_validating_webhook_rejects_configmap_create_when_unreachable() {
    let (mem, router) = spawn_router();

    // Point at a closed port so the call errors; failurePolicy=Fail => reject.
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("fail-closed-cm"),
        webhooks: Some(vec![ValidatingWebhook {
            rules: vec![rule_for("", "v1", "configmaps")],
            ..validating(
                "fail.closed.cm.io",
                "https://127.0.0.1:1/admit".to_string(),
                vec![],
                Some(FailurePolicy::Fail),
                Some(2),
            )
        }]),
    };
    mem.create(
        &build_key("validatingwebhookconfigurations", None, "fail-closed-cm"),
        &cfg,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "fail-closed-cm-obj", "namespace": "default"},
        "data": {"k": "v"},
    });
    let (status, _body) = post_json(router, "/api/v1/namespaces/default/configmaps", &cm).await;

    assert!(
        status.is_client_error() || status.is_server_error(),
        "fail-closed webhook with unreachable backend must block the create, got {status}"
    );
    assert!(
        mem.get::<rusternetes_common::resources::ConfigMap>(&build_key(
            "configmaps",
            Some("default"),
            "fail-closed-cm-obj"
        ))
        .await
        .is_err(),
        "configmap must NOT be persisted when a fail-closed webhook is unreachable"
    );
}

/// objectSelector gating: a mutating webhook scoped by objectSelector must NOT
/// fire for an object whose labels don't match, so the ConfigMap is created
/// UNMUTATED (k8s "should mutate everything except 'skip-me' configmaps"). The
/// no-call path is what lets `skip-me` configmaps pass through unchanged.
#[tokio::test]
async fn object_selector_skips_webhook_for_nonmatching_configmap() {
    let (mem, router) = spawn_router();
    let (url, _shutdown) = start_mutator_label("should-not-appear".into(), "yes".into()).await;

    let mut webhook = MutatingWebhook {
        rules: vec![rule_for("", "v1", "configmaps")],
        ..mutating("skip.cm.io", url, vec![], Some(FailurePolicy::Fail), None)
    };
    // Only fire for configmaps labelled mutate=please.
    webhook.object_selector = Some(rusternetes_common::resources::LabelSelector {
        match_labels: Some(
            [("mutate".to_string(), "please".to_string())]
                .into_iter()
                .collect(),
        ),
        match_expressions: None,
    });

    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: rusternetes_common::types::ObjectMeta::new("skip-cm"),
        webhooks: Some(vec![webhook]),
    };
    mem.create(
        &build_key("mutatingwebhookconfigurations", None, "skip-cm"),
        &cfg,
    )
    .await
    .unwrap();

    // This configmap does NOT carry the mutate=please label → webhook skipped.
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "skip-me", "namespace": "default", "labels": {"other": "x"}},
        "data": {"k": "v"},
    });
    let (status, body) = post_json(router, "/api/v1/namespaces/default/configmaps", &cm).await;

    assert_eq!(status, StatusCode::CREATED, "create must succeed: {body}");
    assert!(
        body["metadata"]["labels"]["should-not-appear"].is_null(),
        "webhook must be skipped for non-matching objectSelector: {body}"
    );
    let stored: rusternetes_common::resources::ConfigMap = mem
        .get(&build_key("configmaps", Some("default"), "skip-me"))
        .await
        .expect("configmap must be stored");
    assert!(
        stored
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("should-not-appear"))
            .is_none(),
        "stored configmap must be unmutated when objectSelector does not match"
    );
}
