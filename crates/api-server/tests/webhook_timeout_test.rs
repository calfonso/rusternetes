// Integration tests for admission webhook `timeoutSeconds` enforcement.
//
// Mirrors upstream e2e site `apimachinery/webhook.go:2491`:
//   "webhook should be invoked with `timeoutSeconds` honored."
//
// A webhook that takes longer than `timeoutSeconds` to respond must be
// aborted. The outcome then depends on `failurePolicy`:
//   * `Fail`   → admission is rejected (error propagates to the caller).
//   * `Ignore` → admission is allowed (the error is swallowed).
//
// We also assert the call returns within roughly `timeoutSeconds + slack`
// rather than waiting for the slow webhook to finish.

use rusternetes_api_server::admission_webhook::AdmissionWebhookManager;
use rusternetes_common::{
    admission::{
        AdmissionResponse, AdmissionReview, AdmissionReviewResponse, GroupVersionKind,
        GroupVersionResource, Operation, UserInfo,
    },
    resources::{
        FailurePolicy, OperationType, Rule, RuleWithOperations, SideEffectClass, ValidatingWebhook,
        ValidatingWebhookConfiguration, WebhookClientConfig,
    },
    types::ObjectMeta,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use warp::Filter;

/// Mock webhook server that sleeps `delay` before responding "allow".
/// Used to deterministically trigger a timeout when `timeoutSeconds < delay`.
async fn start_slow_validating_server(delay: Duration) -> (String, oneshot::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let route =
        warp::post()
            .and(warp::body::json())
            .and_then(move |review: AdmissionReview| async move {
                tokio::time::sleep(delay).await;
                let uid = review
                    .request
                    .as_ref()
                    .map(|r| r.uid.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                let response_review = AdmissionReview {
                    api_version: "admission.k8s.io/v1".to_string(),
                    kind: "AdmissionReview".to_string(),
                    request: None,
                    response: Some(AdmissionReviewResponse::allow(uid)),
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

fn build_pod_create_gvk_gvr() -> (GroupVersionKind, GroupVersionResource) {
    (
        GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        },
    )
}

fn admin_user() -> UserInfo {
    UserInfo {
        username: "admin".to_string(),
        uid: "admin-uid".to_string(),
        groups: vec!["system:masters".to_string()],
    }
}

async fn register_validating_webhook(
    storage: &Arc<MemoryStorage>,
    name: &str,
    url: String,
    timeout_seconds: i32,
    failure_policy: FailurePolicy,
) {
    let config = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".to_string(),
        kind: "ValidatingWebhookConfiguration".to_string(),
        metadata: ObjectMeta::new(name),
        webhooks: Some(vec![ValidatingWebhook {
            name: format!("{}.example.com", name),
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
            failure_policy: Some(failure_policy),
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: Some(timeout_seconds),
            admission_review_versions: vec!["v1".to_string()],
            match_conditions: None,
        }]),
    };
    let key = build_key("validatingwebhookconfigurations", None, name);
    storage.create(&key, &config).await.unwrap();
}

/// `failurePolicy: Fail` — slow webhook must time out and reject the admission.
#[tokio::test]
async fn slow_webhook_with_fail_policy_rejects_within_timeout() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    // Server sleeps 5s; webhook has 1s timeout.
    let (url, _shutdown) = start_slow_validating_server(Duration::from_secs(5)).await;
    register_validating_webhook(&storage, "slow-fail-webhook", url, 1, FailurePolicy::Fail).await;

    let (gvk, gvr) = build_pod_create_gvk_gvr();
    let start = Instant::now();
    let result = manager
        .run_validating_webhooks(
            &Operation::Create,
            &gvk,
            &gvr,
            Some("default"),
            "test-pod",
            Some(json!({"metadata": {"name": "test-pod"}})),
            None,
            &admin_user(),
        )
        .await;
    let elapsed = start.elapsed();

    // Must return well before the 5s server sleep — proves the timeout fired.
    assert!(
        elapsed < Duration::from_secs(4),
        "expected timeout enforcement (~1s), call took {:?}",
        elapsed
    );

    match result {
        Err(e) => {
            // Acceptable: the error chain mentions timeout / deadline.
            let msg = format!("{}", e).to_lowercase();
            assert!(
                msg.contains("timeout") || msg.contains("deadline") || msg.contains("timed out"),
                "expected timeout-related error, got: {}",
                msg
            );
        }
        Ok(AdmissionResponse::Deny(reason)) => {
            let r = reason.to_lowercase();
            assert!(
                r.contains("timeout") || r.contains("deadline") || r.contains("timed out"),
                "expected timeout-related denial, got: {}",
                reason
            );
        }
        Ok(other) => panic!(
            "expected timeout failure with FailurePolicy::Fail, got: {:?}",
            other
        ),
    }
}

/// `failurePolicy: Ignore` — slow webhook must time out and the admission is allowed.
#[tokio::test]
async fn slow_webhook_with_ignore_policy_admits_within_timeout() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    let (url, _shutdown) = start_slow_validating_server(Duration::from_secs(5)).await;
    register_validating_webhook(
        &storage,
        "slow-ignore-webhook",
        url,
        1,
        FailurePolicy::Ignore,
    )
    .await;

    let (gvk, gvr) = build_pod_create_gvk_gvr();
    let start = Instant::now();
    let response = manager
        .run_validating_webhooks(
            &Operation::Create,
            &gvk,
            &gvr,
            Some("default"),
            "test-pod",
            Some(json!({"metadata": {"name": "test-pod"}})),
            None,
            &admin_user(),
        )
        .await
        .expect("FailurePolicy::Ignore must swallow timeout errors");
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(4),
        "expected timeout enforcement (~1s), call took {:?}",
        elapsed
    );

    match response {
        AdmissionResponse::Allow => {}
        other => panic!(
            "expected Allow under FailurePolicy::Ignore on timeout, got: {:?}",
            other
        ),
    }
}

/// Sanity check: a webhook that responds inside `timeoutSeconds` is not aborted.
#[tokio::test]
async fn fast_webhook_completes_normally() {
    let storage = Arc::new(MemoryStorage::new());
    let manager = AdmissionWebhookManager::new(storage.clone());

    // Server responds quickly; webhook has 5s timeout — must succeed.
    let (url, _shutdown) = start_slow_validating_server(Duration::from_millis(50)).await;
    register_validating_webhook(&storage, "fast-webhook", url, 5, FailurePolicy::Fail).await;

    let (gvk, gvr) = build_pod_create_gvk_gvr();
    let response = manager
        .run_validating_webhooks(
            &Operation::Create,
            &gvk,
            &gvr,
            Some("default"),
            "test-pod",
            Some(json!({"metadata": {"name": "test-pod"}})),
            None,
            &admin_user(),
        )
        .await
        .expect("fast webhook must succeed");

    match response {
        AdmissionResponse::Allow => {}
        other => panic!("expected Allow from fast webhook, got: {:?}", other),
    }
}
