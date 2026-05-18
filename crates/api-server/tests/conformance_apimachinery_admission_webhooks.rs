//! Scoped mirror of Kubernetes v1.35 conformance for [sig-api-machinery]
//! Admission webhooks (Validating + Mutating).
//!
//! Source: https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//! Sonobuoy capture: .rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log
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

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use rusternetes_api_server::admission_webhook::AdmissionWebhookManager;
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    admission::{
        AdmissionResponse, AdmissionReview, AdmissionReviewResponse, GroupVersionKind,
        GroupVersionResource, Operation, PatchOp, PatchOperation, UserInfo,
    },
    auth::TokenManager,
    authz::AlwaysAllowAuthorizer,
    observability::MetricsRegistry,
    resources::{
        FailurePolicy, MatchCondition, MutatingWebhook, MutatingWebhookConfiguration,
        OperationType, ReinvocationPolicy, Rule, RuleWithOperations, SideEffectClass,
        ValidatingWebhook, ValidatingWebhookConfiguration, WebhookClientConfig,
    },
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::oneshot;
use tower::ServiceExt;
use warp::Filter;

// ---------------------------------------------------------------------------
// HTTP harness
// ---------------------------------------------------------------------------

/// Build a fully-wired `ApiServerState` backed by an in-memory storage. The
/// authorizer is `AlwaysAllow` and `skip_auth=true` so the router uses
/// `skip_auth_middleware` and no token is needed (mirrors the
/// `patch_cas_retry_test.rs` helper exactly).
fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true, // skip_auth
    ))
}

/// `(storage, router)` factory used by every HTTP-driven test. Each test
/// owns its own storage so the tests are trivially parallel.
fn spawn_router() -> (Arc<MemoryStorage>, axum::Router) {
    let mem = Arc::new(MemoryStorage::new());
    let router = build_router(make_state(mem.clone()), None);
    (mem, router)
}

/// HTTP helper: POST JSON, return `(status, body)`.
async fn post_json(router: axum::Router, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// HTTP helper: GET JSON, return `(status, body)`.
async fn get_json(router: axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// HTTP helper: PUT JSON, return `(status, body)`.
async fn put_json(router: axum::Router, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// HTTP helper: DELETE, return status (some delete handlers return an empty
/// body or a tombstone; we only care about the status code here).
async fn delete_status(router: axum::Router, uri: &str) -> StatusCode {
    let req = Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    router.oneshot(req).await.unwrap().status()
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
/// Sonobuoy (Round 160): FAIL — see docs/CONFORMANCE.md:46 "Webhook
/// admission" bucket. The deny path is integration-tested via
/// `AdmissionWebhookManager` here; the e2e regression is in the api-server
/// admission pipeline wiring that runs webhooks on pod/configmap CREATE.
#[tokio::test]
#[ignore = "Conformance failure tracker — see docs/conformance/apimachinery-admission-webhooks.md"]
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
/// Sonobuoy (Round 160): FAIL — same "Webhook admission" bucket. Mirror
/// asserts a webhook scoped to the `pods/attach` subresource denies
/// the operation when fired with `sub_resource = Some("attach")`.
#[tokio::test]
#[ignore = "Conformance failure tracker — see docs/conformance/apimachinery-admission-webhooks.md"]
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

    // The manager doesn't currently dispatch by subresource — that's the gap
    // tracked by this conformance failure. We assert the deny *would* fire
    // for a pods/attach Connect request once the dispatcher routes it. The
    // current bug: the request matches only `pods` rules, so the attach rule
    // above is skipped, hence the test runs as expected only after the fix
    // ships and is `#[ignore]`d in the meantime.
    let resp = manager
        .run_validating_webhooks(
            &Operation::Create,
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
/// Sonobuoy (Round 160): FAIL — "Webhook admission" bucket. Verifies a
/// webhook bound to a CRD's resource denies all of CREATE/UPDATE/DELETE.
#[tokio::test]
#[ignore = "Conformance failure tracker — see docs/conformance/apimachinery-admission-webhooks.md"]
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
    let (url, _shutdown) = start_deny_validator("would deny everything".into()).await;

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
/// Sonobuoy (Round 160): FAIL — "Webhook admission" bucket. The expected
/// behaviour is: webhook adds a field, then schema-driven pruning removes
/// it because the CRD's `openAPIV3Schema` does not declare it. The current
/// gap is that the mutate+prune pipeline in the api-server doesn't run
/// pruning after the mutating webhook patch.
#[tokio::test]
#[ignore = "Conformance failure tracker — see docs/conformance/apimachinery-admission-webhooks.md"]
async fn should_mutate_custom_resource_with_pruning() {
    let mem = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(mem.clone());

    // Mutator adds a label key that the (notional) CRD schema doesn't
    // declare. Pruning would strip it; in the regression it survives.
    let (url, _shutdown) =
        start_mutator_label("not-in-schema".into(), "should-be-pruned".into()).await;

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
        "metadata": {"name": "cr-prune", "labels": {}}
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
    assert!(
        obj["metadata"]["labels"].get("not-in-schema").is_none(),
        "schema pruning must remove the webhook-added label after mutation; got {obj}"
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
    let (url, _shutdown) = start_slow_validator(std::time::Duration::from_secs(5)).await;

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

    assert!(res.is_err(), "slow webhook + FailurePolicy=Fail must error");
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "deadline must be enforced; took {elapsed:?}"
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
                    &format!("{name}.io"),
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
                    &format!("{name}.io"),
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
