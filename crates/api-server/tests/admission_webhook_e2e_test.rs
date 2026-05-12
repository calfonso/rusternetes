// E2E tests for admission webhooks
//
// These tests verify the full admission webhook flow:
// 1. Webhook configurations stored in etcd/MemoryStorage
// 2. API server calls external webhook HTTP servers
// 3. Webhooks validate and mutate resources
// 4. API server applies patches and enforces denials
//
// Unlike the unit tests in admission_webhook.rs which test individual functions,
// these E2E tests verify the complete integration including HTTP communication.

use rusternetes_api_server::admission_webhook::{AdmissionWebhookClient, AdmissionWebhookManager};
use rusternetes_common::{
    admission::{
        AdmissionReview, AdmissionReviewRequest, AdmissionReviewResponse, GroupVersionKind,
        GroupVersionResource, Operation, PatchOp, PatchOperation, UserInfo,
    },
    resources::{
        FailurePolicy, MutatingWebhook, MutatingWebhookConfiguration, OperationType,
        ReinvocationPolicy, Rule, RuleWithOperations, SideEffectClass, ValidatingWebhook,
        ValidatingWebhookConfiguration, WebhookClientConfig,
    },
    types::ObjectMeta,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::oneshot;
use warp::Filter;

// ===== Mock Webhook Server Helpers =====

/// Start a mock validating webhook server that allows all requests
async fn start_mock_validating_allow_server() -> (String, oneshot::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let route = warp::post()
        .and(warp::body::json())
        .map(|review: AdmissionReview| {
            let response = if let Some(request) = review.request {
                AdmissionReviewResponse::allow(request.uid)
            } else {
                AdmissionReviewResponse {
                    uid: "unknown".to_string(),
                    allowed: true,
                    status: None,
                    patch: None,
                    patch_type: None,
                    audit_annotations: None,
                    warnings: None,
                }
            };

            let response_review = AdmissionReview {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                request: None,
                response: Some(response),
            };

            warp::reply::json(&response_review)
        });

    let (addr, server) =
        warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
            shutdown_rx.await.ok();
        });

    tokio::spawn(server);

    let url = format!("http://{}", addr);
    (url, shutdown_tx)
}

/// Start a mock validating webhook server that denies all requests
async fn start_mock_validating_deny_server(reason: String) -> (String, oneshot::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let route = warp::post()
        .and(warp::body::json())
        .map(move |review: AdmissionReview| {
            let response = if let Some(request) = review.request {
                AdmissionReviewResponse::deny(request.uid, reason.clone())
            } else {
                AdmissionReviewResponse {
                    uid: "unknown".to_string(),
                    allowed: false,
                    status: Some(rusternetes_common::admission::AdmissionStatus {
                        status: "Failure".to_string(),
                        code: Some(403),
                        message: Some(reason.clone()),
                        reason: Some("Forbidden".to_string()),
                        metadata: None,
                    }),
                    patch: None,
                    patch_type: None,
                    audit_annotations: None,
                    warnings: None,
                }
            };

            let response_review = AdmissionReview {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                request: None,
                response: Some(response),
            };

            warp::reply::json(&response_review)
        });

    let (addr, server) =
        warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
            shutdown_rx.await.ok();
        });

    tokio::spawn(server);

    let url = format!("http://{}", addr);
    (url, shutdown_tx)
}

/// Start a mock mutating webhook server that adds a label
async fn start_mock_mutating_server(
    label_key: String,
    label_value: String,
) -> (String, oneshot::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let route = warp::post()
        .and(warp::body::json())
        .map(move |review: AdmissionReview| {
            let response = if let Some(request) = review.request {
                // Create a JSON patch to add a label
                let patch = vec![PatchOperation {
                    op: PatchOp::Add,
                    path: format!("/metadata/labels/{}", label_key),
                    value: Some(json!(label_value.clone())),
                    from: None,
                }];

                // Encode patch as base64
                use base64::Engine;
                let patch_json = serde_json::to_string(&patch).unwrap();
                let patch_base64 =
                    base64::engine::general_purpose::STANDARD.encode(patch_json.as_bytes());

                AdmissionReviewResponse {
                    uid: request.uid,
                    allowed: true,
                    status: None,
                    patch: Some(patch_base64),
                    patch_type: Some("JSONPatch".to_string()),
                    audit_annotations: None,
                    warnings: None,
                }
            } else {
                AdmissionReviewResponse {
                    uid: "unknown".to_string(),
                    allowed: true,
                    status: None,
                    patch: None,
                    patch_type: None,
                    audit_annotations: None,
                    warnings: None,
                }
            };

            let response_review = AdmissionReview {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                request: None,
                response: Some(response),
            };

            warp::reply::json(&response_review)
        });

    let (addr, server) =
        warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
            shutdown_rx.await.ok();
        });

    tokio::spawn(server);

    let url = format!("http://{}", addr);
    (url, shutdown_tx)
}

/// Start a mock validating webhook server that sleeps longer than the request
/// timeout. Used to verify timeoutSeconds enforcement.
async fn start_mock_slow_validating_server(
    delay: std::time::Duration,
) -> (String, oneshot::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let route =
        warp::post()
            .and(warp::body::json())
            .and_then(move |review: AdmissionReview| async move {
                tokio::time::sleep(delay).await;
                let response = if let Some(request) = review.request {
                    AdmissionReviewResponse::allow(request.uid)
                } else {
                    AdmissionReviewResponse {
                        uid: "unknown".to_string(),
                        allowed: true,
                        status: None,
                        patch: None,
                        patch_type: None,
                        audit_annotations: None,
                        warnings: None,
                    }
                };
                let response_review = AdmissionReview {
                    api_version: "admission.k8s.io/v1".to_string(),
                    kind: "AdmissionReview".to_string(),
                    request: None,
                    response: Some(response),
                };
                Ok::<_, warp::Rejection>(warp::reply::json(&response_review))
            });

    let (addr, server) =
        warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
            shutdown_rx.await.ok();
        });

    tokio::spawn(server);

    let url = format!("http://{}", addr);
    (url, shutdown_tx)
}

/// Start a mock mutating webhook server whose label value reflects the call
/// count. Used to verify reinvocation: the second invocation returns a
/// different value than the first.
async fn start_mock_counting_mutating_server(
    label_key: String,
) -> (
    String,
    oneshot::Sender<()>,
    Arc<std::sync::atomic::AtomicU32>,
) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counter_clone = counter.clone();

    let route = warp::post()
        .and(warp::body::json())
        .map(move |review: AdmissionReview| {
            let call = counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let label_value = format!("call-{}", call);
            let response = if let Some(request) = review.request {
                let patch = vec![PatchOperation {
                    op: PatchOp::Add,
                    path: format!("/metadata/labels/{}", label_key),
                    value: Some(json!(label_value)),
                    from: None,
                }];
                use base64::Engine;
                let patch_json = serde_json::to_string(&patch).unwrap();
                let patch_base64 =
                    base64::engine::general_purpose::STANDARD.encode(patch_json.as_bytes());
                AdmissionReviewResponse {
                    uid: request.uid,
                    allowed: true,
                    status: None,
                    patch: Some(patch_base64),
                    patch_type: Some("JSONPatch".to_string()),
                    audit_annotations: None,
                    warnings: None,
                }
            } else {
                AdmissionReviewResponse {
                    uid: "unknown".to_string(),
                    allowed: true,
                    status: None,
                    patch: None,
                    patch_type: None,
                    audit_annotations: None,
                    warnings: None,
                }
            };
            let response_review = AdmissionReview {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                request: None,
                response: Some(response),
            };
            warp::reply::json(&response_review)
        });

    let (addr, server) =
        warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
            shutdown_rx.await.ok();
        });

    tokio::spawn(server);

    let url = format!("http://{}", addr);
    (url, shutdown_tx, counter)
}

// ===== Webhook Client Tests =====

#[tokio::test]
async fn test_webhook_client_calls_validating_allow() {
    let (url, _shutdown) = start_mock_validating_allow_server().await;

    let client = AdmissionWebhookClient::new();
    let webhook = ValidatingWebhook {
        name: "test-validator".to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules: vec![],
        failure_policy: None,
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: None,
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
    };

    let request = AdmissionReviewRequest {
        uid: "test-uid-123".to_string(),
        kind: GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        resource: GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        },
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: "test-pod".to_string(),
        namespace: Some("default".to_string()),
        operation: Operation::Create,
        user_info: UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        },
        object: Some(json!({"metadata": {"name": "test-pod"}})),
        old_object: None,
        dry_run: None,
        options: None,
    };

    let response = client
        .call_validating_webhook(&webhook, &request)
        .await
        .unwrap();
    assert!(response.allowed, "Webhook should allow the request");
    assert_eq!(response.uid, "test-uid-123");
}

#[tokio::test]
async fn test_webhook_client_calls_validating_deny() {
    let deny_reason = "Pod name not allowed".to_string();
    let (url, _shutdown) = start_mock_validating_deny_server(deny_reason.clone()).await;

    let client = AdmissionWebhookClient::new();
    let webhook = ValidatingWebhook {
        name: "test-validator-deny".to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules: vec![],
        failure_policy: None,
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: None,
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
    };

    let request = AdmissionReviewRequest {
        uid: "test-uid-456".to_string(),
        kind: GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        resource: GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        },
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: "bad-pod".to_string(),
        namespace: Some("default".to_string()),
        operation: Operation::Create,
        user_info: UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        },
        object: Some(json!({"metadata": {"name": "bad-pod"}})),
        old_object: None,
        dry_run: None,
        options: None,
    };

    let response = client
        .call_validating_webhook(&webhook, &request)
        .await
        .unwrap();
    assert!(!response.allowed, "Webhook should deny the request");
    assert_eq!(response.uid, "test-uid-456");
    assert!(response.status.is_some());
    let status = response.status.unwrap();
    assert_eq!(status.message, Some(deny_reason));
}

#[tokio::test]
async fn test_webhook_client_calls_mutating() {
    let (url, _shutdown) =
        start_mock_mutating_server("app".to_string(), "mutated".to_string()).await;

    let client = AdmissionWebhookClient::new();
    let webhook = MutatingWebhook {
        name: "test-mutator".to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules: vec![],
        failure_policy: None,
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: None,
        admission_review_versions: vec!["v1".to_string()],
        reinvocation_policy: None,
        match_conditions: None,
    };

    let request = AdmissionReviewRequest {
        uid: "test-uid-789".to_string(),
        kind: GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        resource: GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        },
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: "test-pod".to_string(),
        namespace: Some("default".to_string()),
        operation: Operation::Create,
        user_info: UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        },
        object: Some(json!({"metadata": {"name": "test-pod", "labels": {}}})),
        old_object: None,
        dry_run: None,
        options: None,
    };

    let response = client
        .call_mutating_webhook(&webhook, &request)
        .await
        .unwrap();
    assert!(response.allowed, "Webhook should allow the request");
    assert!(response.patch.is_some(), "Webhook should return a patch");
    assert_eq!(response.patch_type, Some("JSONPatch".to_string()));
}

#[tokio::test]
async fn test_webhook_client_failure_policy_ignore() {
    // Use invalid URL to trigger failure
    let client = AdmissionWebhookClient::new();
    let webhook = ValidatingWebhook {
        name: "test-validator-failure".to_string(),
        client_config: WebhookClientConfig {
            url: Some("http://localhost:1/invalid".to_string()), // Invalid URL
            service: None,
            ca_bundle: None,
        },
        rules: vec![],
        failure_policy: Some(FailurePolicy::Ignore),
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: Some(1), // Short timeout
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
    };

    let request = AdmissionReviewRequest {
        uid: "test-uid-failure".to_string(),
        kind: GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        resource: GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        },
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: "test-pod".to_string(),
        namespace: Some("default".to_string()),
        operation: Operation::Create,
        user_info: UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        },
        object: Some(json!({"metadata": {"name": "test-pod"}})),
        old_object: None,
        dry_run: None,
        options: None,
    };

    // Should not fail despite webhook being unreachable (FailurePolicy::Ignore)
    let response = client
        .call_validating_webhook(&webhook, &request)
        .await
        .unwrap();
    assert!(
        response.allowed,
        "Request should be allowed when FailurePolicy is Ignore"
    );
}

// ===== Webhook Manager Integration Tests =====

#[tokio::test]
async fn test_webhook_manager_runs_validating_webhooks() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    // Start mock webhook server
    let (url, _shutdown) = start_mock_validating_allow_server().await;

    // Create webhook configuration
    let config = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("test-webhook-config"),
        webhooks: Some(vec![ValidatingWebhook {
            name: "test-validator".to_string(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["pods".to_string()],
                    scope: None,
                },
            }],
            failure_policy: None,
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: None,
            admission_review_versions: vec!["v1".to_string()],
            match_conditions: None,
        }]),
    };

    let key = build_key(
        "validatingwebhookconfigurations",
        None,
        "test-webhook-config",
    );
    storage.create(&key, &config).await.unwrap();

    // Run webhooks
    let response = manager
        .run_validating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".to_string(),
                version: "v1".to_string(),
                kind: "Pod".to_string(),
            },
            &GroupVersionResource {
                group: "".to_string(),
                version: "v1".to_string(),
                resource: "pods".to_string(),
            },
            Some("default"),
            "test-pod",
            Some(json!({"metadata": {"name": "test-pod"}})),
            None,
            &UserInfo {
                username: "admin".to_string(),
                uid: "admin-uid".to_string(),
                groups: vec!["system:masters".to_string()],
            },
        )
        .await
        .unwrap();

    match response {
        rusternetes_common::admission::AdmissionResponse::Allow => {
            // Expected
        }
        rusternetes_common::admission::AdmissionResponse::Deny(reason) => {
            panic!("Webhook should allow request, but denied with: {}", reason);
        }
        _ => {
            panic!("Unexpected response type");
        }
    }
}

#[tokio::test]
async fn test_webhook_manager_runs_mutating_webhooks() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    // Start mock webhook server
    let (url, _shutdown) =
        start_mock_mutating_server("injected".to_string(), "true".to_string()).await;

    // Create webhook configuration
    let config = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("test-mutating-config"),
        webhooks: Some(vec![MutatingWebhook {
            name: "test-mutator".to_string(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["pods".to_string()],
                    scope: None,
                },
            }],
            failure_policy: None,
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: None,
            admission_review_versions: vec!["v1".to_string()],
            reinvocation_policy: None,
            match_conditions: None,
        }]),
    };

    let key = build_key(
        "mutatingwebhookconfigurations",
        None,
        "test-mutating-config",
    );
    storage.create(&key, &config).await.unwrap();

    // Run webhooks
    let object = Some(json!({"metadata": {"name": "test-pod", "labels": {}}}));
    let (response, mutated_object) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".to_string(),
                version: "v1".to_string(),
                kind: "Pod".to_string(),
            },
            &GroupVersionResource {
                group: "".to_string(),
                version: "v1".to_string(),
                resource: "pods".to_string(),
            },
            Some("default"),
            "test-pod",
            object,
            None,
            &UserInfo {
                username: "admin".to_string(),
                uid: "admin-uid".to_string(),
                groups: vec!["system:masters".to_string()],
            },
        )
        .await
        .unwrap();

    match response {
        rusternetes_common::admission::AdmissionResponse::AllowWithPatch(patches) => {
            assert!(!patches.is_empty(), "Should have patches");
            assert!(mutated_object.is_some(), "Object should be mutated");
            let obj = mutated_object.unwrap();
            // Verify the label was added
            assert!(obj["metadata"]["labels"]["injected"] == json!("true"));
        }
        rusternetes_common::admission::AdmissionResponse::Allow => {
            panic!("Expected AllowWithPatch but got Allow");
        }
        rusternetes_common::admission::AdmissionResponse::Deny(reason) => {
            panic!("Webhook should allow request, but denied with: {}", reason);
        }
    }
}

#[tokio::test]
async fn test_webhook_manager_denial_stops_request() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    // Start mock webhook server that denies
    let (url, _shutdown) =
        start_mock_validating_deny_server("Resource not allowed".to_string()).await;

    // Create webhook configuration
    let config = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("deny-webhook-config"),
        webhooks: Some(vec![ValidatingWebhook {
            name: "deny-validator".to_string(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["pods".to_string()],
                    scope: None,
                },
            }],
            failure_policy: None,
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: None,
            admission_review_versions: vec!["v1".to_string()],
            match_conditions: None,
        }]),
    };

    let key = build_key(
        "validatingwebhookconfigurations",
        None,
        "deny-webhook-config",
    );
    storage.create(&key, &config).await.unwrap();

    // Run webhooks
    let response = manager
        .run_validating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".to_string(),
                version: "v1".to_string(),
                kind: "Pod".to_string(),
            },
            &GroupVersionResource {
                group: "".to_string(),
                version: "v1".to_string(),
                resource: "pods".to_string(),
            },
            Some("default"),
            "test-pod",
            Some(json!({"metadata": {"name": "test-pod"}})),
            None,
            &UserInfo {
                username: "admin".to_string(),
                uid: "admin-uid".to_string(),
                groups: vec!["system:masters".to_string()],
            },
        )
        .await
        .unwrap();

    match response {
        rusternetes_common::admission::AdmissionResponse::Deny(reason) => {
            assert!(reason.contains("Resource not allowed"));
        }
        rusternetes_common::admission::AdmissionResponse::Allow => {
            panic!("Expected Deny but got Allow");
        }
        _ => {
            panic!("Unexpected response type");
        }
    }
}

// ===== Timeout & Reinvocation Tests =====

/// A webhook that sleeps longer than `timeoutSeconds` must be aborted at the
/// deadline. With FailurePolicy=Fail the request fails.
#[tokio::test]
async fn test_webhook_timeout_fail_policy_aborts_at_deadline() {
    // Server sleeps 5s, webhook timeout is 1s.
    let (url, _shutdown) =
        start_mock_slow_validating_server(std::time::Duration::from_secs(5)).await;

    let client = AdmissionWebhookClient::new();
    let webhook = ValidatingWebhook {
        name: "slow-validator".to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules: vec![],
        failure_policy: Some(FailurePolicy::Fail),
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: Some(1),
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
    };

    let request = AdmissionReviewRequest {
        uid: "timeout-uid".to_string(),
        kind: GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        resource: GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        },
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: "slow-pod".to_string(),
        namespace: Some("default".to_string()),
        operation: Operation::Create,
        user_info: UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        },
        object: Some(json!({"metadata": {"name": "slow-pod"}})),
        old_object: None,
        dry_run: None,
        options: None,
    };

    let start = std::time::Instant::now();
    let result = client.call_validating_webhook(&webhook, &request).await;
    let elapsed = start.elapsed();

    assert!(
        result.is_err(),
        "Expected timeout error with FailurePolicy=Fail, got {:?}",
        result
    );
    // Total call time must be bounded near the 1s timeout, not the 5s sleep.
    // Allow 3s slack for CI flakiness while still proving the deadline aborted.
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "Webhook call exceeded deadline: {:?}",
        elapsed
    );
    let msg = format!("{}", result.unwrap_err()).to_lowercase();
    assert!(
        msg.contains("deadline") || msg.contains("timeout") || msg.contains("timed out"),
        "Expected deadline/timeout message, got: {}",
        msg
    );
}

/// With FailurePolicy=Ignore, a slow webhook must not block the request once
/// the deadline elapses.
#[tokio::test]
async fn test_webhook_timeout_ignore_policy_allows_request() {
    let (url, _shutdown) =
        start_mock_slow_validating_server(std::time::Duration::from_secs(5)).await;

    let client = AdmissionWebhookClient::new();
    let webhook = ValidatingWebhook {
        name: "slow-validator-ignore".to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules: vec![],
        failure_policy: Some(FailurePolicy::Ignore),
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: Some(1),
        admission_review_versions: vec!["v1".to_string()],
        match_conditions: None,
    };

    let request = AdmissionReviewRequest {
        uid: "timeout-ignore-uid".to_string(),
        kind: GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        resource: GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        },
        sub_resource: None,
        request_kind: None,
        request_resource: None,
        request_sub_resource: None,
        name: "slow-pod".to_string(),
        namespace: Some("default".to_string()),
        operation: Operation::Create,
        user_info: UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        },
        object: Some(json!({"metadata": {"name": "slow-pod"}})),
        old_object: None,
        dry_run: None,
        options: None,
    };

    let start = std::time::Instant::now();
    let response = client
        .call_validating_webhook(&webhook, &request)
        .await
        .expect("Ignore policy must not propagate error");
    let elapsed = start.elapsed();

    assert!(response.allowed, "FailurePolicy=Ignore must allow request");
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "Webhook call exceeded deadline: {:?}",
        elapsed
    );
}

/// reinvocationPolicy=IfNeeded triggers when a later mutating webhook changes
/// the object after this webhook's first call.
///
/// Both A and B have IfNeeded policy. MemoryStorage iterates as a HashMap, so
/// the order in which the two webhooks fire is non-deterministic; whichever
/// runs first must be reinvoked after the other mutates the object. We verify
/// the order-independent invariant: one of {A, B} is called twice and the
/// other is called once.
#[tokio::test]
async fn test_mutating_webhook_reinvocation_if_needed_triggers_when_changed() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    let (url_a, _shutdown_a, counter_a) =
        start_mock_counting_mutating_server("a".to_string()).await;
    let (url_b, _shutdown_b, counter_b) =
        start_mock_counting_mutating_server("b".to_string()).await;

    let mk_webhook = |name: &str, url: String| MutatingWebhook {
        name: name.to_string(),
        client_config: WebhookClientConfig {
            url: Some(url),
            service: None,
            ca_bundle: None,
        },
        rules: vec![RuleWithOperations {
            operations: vec![OperationType::Create],
            rule: Rule {
                api_groups: vec!["".to_string()],
                api_versions: vec!["v1".to_string()],
                resources: vec!["pods".to_string()],
                scope: None,
            },
        }],
        failure_policy: None,
        match_policy: None,
        namespace_selector: None,
        object_selector: None,
        side_effects: SideEffectClass::None,
        timeout_seconds: None,
        admission_review_versions: vec!["v1".to_string()],
        reinvocation_policy: Some(ReinvocationPolicy::IfNeeded),
        match_conditions: None,
    };

    let config_a = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("a-mutating"),
        webhooks: Some(vec![mk_webhook("a-mutator", url_a)]),
    };
    let config_b = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("b-mutating"),
        webhooks: Some(vec![mk_webhook("b-mutator", url_b)]),
    };

    storage
        .create(
            &build_key("mutatingwebhookconfigurations", None, "a-mutating"),
            &config_a,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("mutatingwebhookconfigurations", None, "b-mutating"),
            &config_b,
        )
        .await
        .unwrap();

    let object = Some(json!({"metadata": {"name": "rein-pod", "labels": {}}}));
    let (_response, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".to_string(),
                version: "v1".to_string(),
                kind: "Pod".to_string(),
            },
            &GroupVersionResource {
                group: "".to_string(),
                version: "v1".to_string(),
                resource: "pods".to_string(),
            },
            Some("default"),
            "rein-pod",
            object,
            None,
            &UserInfo {
                username: "admin".to_string(),
                uid: "admin-uid".to_string(),
                groups: vec!["system:masters".to_string()],
            },
        )
        .await
        .unwrap();

    let a_calls = counter_a.load(std::sync::atomic::Ordering::SeqCst);
    let b_calls = counter_b.load(std::sync::atomic::Ordering::SeqCst);

    // Both A and B have IfNeeded and both mutated the object. After the first
    // pass each one's snapshot diverges from the final object (because the
    // other added a label after it), so both are reinvoked. K8s bounds
    // reinvocation to one extra round per webhook, so each is called at most
    // twice.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/dispatcher.go
    assert!(
        a_calls >= 1 && b_calls >= 1,
        "Both webhooks must run at least once (a={}, b={})",
        a_calls,
        b_calls
    );
    assert!(
        a_calls <= 2 && b_calls <= 2,
        "Each webhook must be reinvoked at most once (a={}, b={})",
        a_calls,
        b_calls
    );
    // At least one webhook must have been reinvoked — both mutated the object,
    // so the webhook that ran first must see the second one's change.
    assert!(
        a_calls + b_calls >= 3,
        "Expected at least one reinvocation (a={}, b={})",
        a_calls,
        b_calls
    );

    // Final object must carry both labels. The webhook(s) reinvoked have
    // call-2; any not reinvoked stays at call-1.
    let obj = mutated.expect("object should exist");
    let label_a = obj["metadata"]["labels"]["a"].as_str().unwrap_or("");
    let label_b = obj["metadata"]["labels"]["b"].as_str().unwrap_or("");
    let expected_a = if a_calls == 2 { "call-2" } else { "call-1" };
    let expected_b = if b_calls == 2 { "call-2" } else { "call-1" };
    assert_eq!(label_a, expected_a, "label A mismatch");
    assert_eq!(label_b, expected_b, "label B mismatch");
}

/// reinvocationPolicy=IfNeeded must NOT trigger when no later webhook mutates
/// the object after this webhook's call.
#[tokio::test]
async fn test_mutating_webhook_reinvocation_if_needed_skipped_when_unchanged() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    let (url, _shutdown, counter) = start_mock_counting_mutating_server("a".to_string()).await;

    let config = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("lone-mutating"),
        webhooks: Some(vec![MutatingWebhook {
            name: "lone-mutator".to_string(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["pods".to_string()],
                    scope: None,
                },
            }],
            failure_policy: None,
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: None,
            admission_review_versions: vec!["v1".to_string()],
            reinvocation_policy: Some(ReinvocationPolicy::IfNeeded),
            match_conditions: None,
        }]),
    };

    storage
        .create(
            &build_key("mutatingwebhookconfigurations", None, "lone-mutating"),
            &config,
        )
        .await
        .unwrap();

    let object = Some(json!({"metadata": {"name": "lone-pod", "labels": {}}}));
    manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".to_string(),
                version: "v1".to_string(),
                kind: "Pod".to_string(),
            },
            &GroupVersionResource {
                group: "".to_string(),
                version: "v1".to_string(),
                resource: "pods".to_string(),
            },
            Some("default"),
            "lone-pod",
            object,
            None,
            &UserInfo {
                username: "admin".to_string(),
                uid: "admin-uid".to_string(),
                groups: vec!["system:masters".to_string()],
            },
        )
        .await
        .unwrap();

    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "Webhook must be called exactly once when nothing else mutates"
    );
}

/// reinvocationPolicy=Never (default) must never reinvoke.
#[tokio::test]
async fn test_mutating_webhook_reinvocation_never_does_not_reinvoke() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    let (url_a, _shutdown_a, counter_a) =
        start_mock_counting_mutating_server("a".to_string()).await;
    let (url_b, _shutdown_b) =
        start_mock_mutating_server("b".to_string(), "from-b".to_string()).await;

    let config_a = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("a-never-mutating"),
        webhooks: Some(vec![MutatingWebhook {
            name: "a-never-mutator".to_string(),
            client_config: WebhookClientConfig {
                url: Some(url_a),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["pods".to_string()],
                    scope: None,
                },
            }],
            failure_policy: None,
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: None,
            admission_review_versions: vec!["v1".to_string()],
            reinvocation_policy: Some(ReinvocationPolicy::Never),
            match_conditions: None,
        }]),
    };
    let config_b = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "MutatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new("b-never-mutating"),
        webhooks: Some(vec![MutatingWebhook {
            name: "b-never-mutator".to_string(),
            client_config: WebhookClientConfig {
                url: Some(url_b),
                service: None,
                ca_bundle: None,
            },
            rules: vec![RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["pods".to_string()],
                    scope: None,
                },
            }],
            failure_policy: None,
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: None,
            admission_review_versions: vec!["v1".to_string()],
            reinvocation_policy: None,
            match_conditions: None,
        }]),
    };

    storage
        .create(
            &build_key("mutatingwebhookconfigurations", None, "a-never-mutating"),
            &config_a,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("mutatingwebhookconfigurations", None, "b-never-mutating"),
            &config_b,
        )
        .await
        .unwrap();

    let object = Some(json!({"metadata": {"name": "rein-never-pod", "labels": {}}}));
    let (_response, mutated) = manager
        .run_mutating_webhooks(
            &Operation::Create,
            &GroupVersionKind {
                group: "".to_string(),
                version: "v1".to_string(),
                kind: "Pod".to_string(),
            },
            &GroupVersionResource {
                group: "".to_string(),
                version: "v1".to_string(),
                resource: "pods".to_string(),
            },
            Some("default"),
            "rein-never-pod",
            object,
            None,
            &UserInfo {
                username: "admin".to_string(),
                uid: "admin-uid".to_string(),
                groups: vec!["system:masters".to_string()],
            },
        )
        .await
        .unwrap();

    assert_eq!(
        counter_a.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "A with reinvocationPolicy=Never must NOT be reinvoked"
    );
    let obj = mutated.expect("object should exist");
    assert_eq!(obj["metadata"]["labels"]["a"], json!("call-1"));
    assert_eq!(obj["metadata"]["labels"]["b"], json!("from-b"));
}
