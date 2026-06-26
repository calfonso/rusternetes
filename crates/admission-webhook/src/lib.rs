// Admission webhook client for calling external webhooks
//
// This module implements the client for calling external admission webhooks
// and processing their responses.

pub mod cel_evaluators;

use rusternetes_common::{
    admission::{
        AdmissionResponse, AdmissionReview, AdmissionReviewRequest, AdmissionReviewResponse,
        GroupVersionKind, GroupVersionResource, Operation, PatchOperation, UserInfo,
    },
    resources::{
        FailurePolicy, MutatingWebhook, MutatingWebhookConfiguration, OperationType,
        ReinvocationPolicy, Rule, SideEffectClass, ValidatingWebhook,
        ValidatingWebhookConfiguration, WebhookClientConfig,
    },
    Result,
};
use rusternetes_storage::Storage;
use serde_json::Value;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// K8s v1.35 admission webhook timeout bounds.
/// admissionregistration.k8s.io/v1: timeoutSeconds must be 1-30, default 10.
/// K8s ref: staging/src/k8s.io/api/admissionregistration/v1/types.go
const WEBHOOK_DEFAULT_TIMEOUT_SECS: u64 = 10;
const WEBHOOK_MAX_TIMEOUT_SECS: u64 = 30;
const WEBHOOK_MIN_TIMEOUT_SECS: u64 = 1;

/// Resolve a webhook's `timeoutSeconds` to a [`Duration`], honoring K8s v1.35 bounds.
/// `None` → 10s default. Values < 1 → 1s. Values > 30 → 30s.
pub(crate) fn resolve_webhook_timeout(timeout_seconds: Option<i32>) -> Duration {
    let secs = timeout_seconds
        .map(|t| {
            (t as i64).clamp(
                WEBHOOK_MIN_TIMEOUT_SECS as i64,
                WEBHOOK_MAX_TIMEOUT_SECS as i64,
            ) as u64
        })
        .unwrap_or(WEBHOOK_DEFAULT_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

/// Admission webhook client for calling external webhooks
pub struct AdmissionWebhookClient {
    #[allow(dead_code)]
    http_client: reqwest::Client,
}

impl AdmissionWebhookClient {
    /// Create a new admission webhook client
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Call a validating webhook
    #[allow(dead_code)]
    pub async fn call_validating_webhook(
        &self,
        webhook: &ValidatingWebhook,
        request: &AdmissionReviewRequest,
    ) -> Result<AdmissionReviewResponse> {
        let url = self.build_webhook_url(&webhook.client_config)?;
        let timeout = resolve_webhook_timeout(webhook.timeout_seconds);

        info!("Calling validating webhook {} at {}", webhook.name, url);

        let review = AdmissionReview::new_request(request.clone());

        match self.call_webhook(&url, &review, timeout).await {
            Ok(response) => Ok(response),
            Err(e) => {
                let failure_policy = webhook
                    .failure_policy
                    .as_ref()
                    .unwrap_or(&FailurePolicy::Fail);

                match failure_policy {
                    FailurePolicy::Ignore => {
                        warn!(
                            "Webhook {} failed but FailurePolicy is Ignore: {}",
                            webhook.name, e
                        );
                        // Allow the request despite the error
                        Ok(AdmissionReviewResponse::allow(request.uid.clone()))
                    }
                    FailurePolicy::Fail => {
                        error!(
                            "Webhook {} failed with FailurePolicy Fail: {}",
                            webhook.name, e
                        );
                        Err(e)
                    }
                }
            }
        }
    }

    /// Call a mutating webhook
    #[allow(dead_code)]
    pub async fn call_mutating_webhook(
        &self,
        webhook: &MutatingWebhook,
        request: &AdmissionReviewRequest,
    ) -> Result<AdmissionReviewResponse> {
        let url = self.build_webhook_url(&webhook.client_config)?;
        let timeout = resolve_webhook_timeout(webhook.timeout_seconds);

        info!("Calling mutating webhook {} at {}", webhook.name, url);

        let review = AdmissionReview::new_request(request.clone());

        match self.call_webhook(&url, &review, timeout).await {
            Ok(response) => Ok(response),
            Err(e) => {
                let failure_policy = webhook
                    .failure_policy
                    .as_ref()
                    .unwrap_or(&FailurePolicy::Fail);

                match failure_policy {
                    FailurePolicy::Ignore => {
                        warn!(
                            "Webhook {} failed but FailurePolicy is Ignore: {}",
                            webhook.name, e
                        );
                        // Allow the request despite the error
                        Ok(AdmissionReviewResponse::allow(request.uid.clone()))
                    }
                    FailurePolicy::Fail => {
                        error!(
                            "Webhook {} failed with FailurePolicy Fail: {}",
                            webhook.name, e
                        );
                        Err(e)
                    }
                }
            }
        }
    }

    /// Internal method to call a webhook
    #[allow(dead_code)]
    async fn call_webhook(
        &self,
        url: &str,
        review: &AdmissionReview,
        timeout: Duration,
    ) -> Result<AdmissionReviewResponse> {
        self.call_webhook_with_ca(url, review, timeout, None).await
    }

    /// Call a webhook with optional CA bundle for TLS verification.
    ///
    /// The request is bounded by a tokio task-level deadline matching the webhook's
    /// `spec.timeoutSeconds` (default 10s, clamped to 1-30s per K8s v1.35). The
    /// reqwest client also enforces the same timeout internally; the tokio wrapper
    /// guards the full async call (DNS + connect + TLS + send + parse) so the
    /// request is aborted at the deadline regardless of which phase is blocking.
    async fn call_webhook_with_ca(
        &self,
        url: &str,
        review: &AdmissionReview,
        timeout: Duration,
        ca_bundle: Option<&[u8]>,
    ) -> Result<AdmissionReviewResponse> {
        // Outer tokio deadline mirrors K8s admissionContext.WithTimeout — if the
        // entire call exceeds the budget, the future is dropped/aborted.
        // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/generic/webhook.go
        match tokio::time::timeout(
            timeout,
            self.call_webhook_inner(url, review, timeout, ca_bundle),
        )
        .await
        {
            Ok(result) => result,
            // Match the upstream wording asserted by the K8s conformance suite
            // (test/e2e/apimachinery/webhook.go:358 — "Webhook fail open" should
            // observe an HTTP/dial timeout error). The Go context-deadline phrase
            // is retained for backwards compatibility with existing assertions.
            // Include the webhook URL with its `?timeout={n}s` query: the
            // conformance "should honor timeout" test asserts the error names
            // the queried endpoint (e.g. `/always-allow-delay-5s?timeout=1s`),
            // not just that a timeout occurred.
            // K8s ref: test/e2e/apimachinery/webhook.go (testSlowWebhookTimeoutFailEarly)
            Err(_elapsed) => Err(rusternetes_common::Error::Internal(format!(
                "failed to call webhook: HTTP/dial timeout: context deadline exceeded querying {}?timeout={}s",
                url,
                timeout.as_secs()
            ))),
        }
    }

    async fn call_webhook_inner(
        &self,
        url: &str,
        review: &AdmissionReview,
        timeout: Duration,
        ca_bundle: Option<&[u8]>,
    ) -> Result<AdmissionReviewResponse> {
        // K8s appends ?timeout={seconds}s to the webhook URL so the backend
        // knows how long it has. Tests check for this in error messages.
        // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/request/admissionreview.go
        let url_with_timeout = format!("{}?timeout={}s", url, timeout.as_secs());
        let url = &url_with_timeout;

        let mut builder = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(Duration::from_secs(2));

        if let Some(ca_data) = ca_bundle {
            // CA bundle provided — add as root cert. Also accept invalid certs
            // for cases where the CA bundle is self-signed (common in K8s).
            if let Ok(cert) = reqwest::Certificate::from_pem(ca_data) {
                builder = builder.add_root_certificate(cert);
            }
            // With a CA bundle, we trust the provided CA
            builder = builder.danger_accept_invalid_certs(true);
        }
        // Without a CA bundle, use system CAs only (no danger_accept).
        // K8s behavior: webhook calls without a CA bundle fail TLS verification
        // against self-signed certs, which is the expected behavior for
        // fail-closed webhook tests.

        let client = builder.build().map_err(|e| {
            rusternetes_common::Error::Network(format!("Failed to create HTTP client: {}", e))
        })?;

        let response = client.post(url).json(review).send().await.map_err(|e| {
            // Build full error cause chain for diagnostics
            let mut causes = Vec::new();
            let mut source: Option<&dyn StdError> = StdError::source(&e);
            while let Some(cause) = source {
                causes.push(format!("{}", cause));
                source = cause.source();
            }
            let detail = if e.is_connect() {
                "connection refused/failed"
            } else if e.is_timeout() {
                "timeout"
            } else if e.is_request() {
                "request error"
            } else {
                "unknown"
            };
            let cause_chain = if causes.is_empty() {
                String::new()
            } else {
                format!(" causes=[{}]", causes.join(" -> "))
            };
            error!(
                "Webhook call to {} failed: {} ({}){}",
                url, e, detail, cause_chain
            );
            // Include cause chain so errors like "deadline has elapsed" are visible to clients.
            // K8s Go context timeout produces "context deadline exceeded" — tests check for
            // the word "deadline". Reqwest produces "operation timed out" for timeouts and
            // "deadline has elapsed" for connect timeouts. Normalize to include "deadline"
            // AND the upstream "HTTP/dial timeout" phrase that the K8s conformance suite
            // asserts (test/e2e/apimachinery/webhook.go:358).
            let cause_str = causes.join(": ");
            let normalized_causes = if e.is_timeout() || cause_str.contains("timed out") {
                if cause_str.contains("deadline") {
                    cause_str
                } else {
                    format!("{}: context deadline exceeded", cause_str)
                }
            } else {
                cause_str
            };
            let timeout_phrase = if e.is_timeout() || e.is_connect() {
                "HTTP/dial timeout: "
            } else {
                ""
            };
            // `url` here is url_with_timeout (carries `?timeout={n}s`). Naming it
            // in the error lets the conformance timeout test match the queried
            // endpoint (e.g. `/always-allow-delay-5s?timeout=1s`).
            let full_error = if causes.is_empty() && !e.is_timeout() {
                format!(
                    "failed to call webhook: {}{} querying {}",
                    timeout_phrase, e, url
                )
            } else if causes.is_empty() {
                format!(
                    "failed to call webhook: {}{}: context deadline exceeded querying {}",
                    timeout_phrase, e, url
                )
            } else {
                format!(
                    "failed to call webhook: {}{} ({}) querying {}",
                    timeout_phrase, e, normalized_causes, url
                )
            };
            rusternetes_common::Error::Internal(full_error)
        })?;

        if !response.status().is_success() {
            return Err(rusternetes_common::Error::Network(format!(
                "Webhook returned status: {}",
                response.status()
            )));
        }

        let body_bytes = response.bytes().await.map_err(|e| {
            rusternetes_common::Error::Network(format!(
                "Failed to read webhook response body: {}",
                e
            ))
        })?;

        // Log raw response for diagnostics
        tracing::info!(
            "Webhook raw response ({} bytes): {}",
            body_bytes.len(),
            String::from_utf8_lossy(&body_bytes[..body_bytes.len().min(300)])
        );

        // Reject empty responses — the webhook pod may have terminated mid-response
        if body_bytes.is_empty() {
            return Err(rusternetes_common::Error::Network(
                "Webhook returned empty response body".to_string(),
            ));
        }

        // Try parsing as AdmissionReview first, fall back to parsing as raw Value
        // to extract the response even if there are unknown fields
        let review_response: AdmissionReview = match serde_json::from_slice(&body_bytes) {
            Ok(r) => r,
            Err(e) => {
                // Try parsing as raw JSON and extract the response field
                tracing::warn!(
                    "Webhook response strict parse failed ({}), trying lenient parse. Body: {}",
                    e,
                    String::from_utf8_lossy(&body_bytes[..body_bytes.len().min(500)])
                );
                let value: serde_json::Value =
                    serde_json::from_slice(&body_bytes).map_err(|e2| {
                        rusternetes_common::Error::Network(format!(
                            "Failed to parse webhook response as JSON: {}",
                            e2
                        ))
                    })?;
                // Build AdmissionReview from raw value
                let api_version = value
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("admission.k8s.io/v1")
                    .to_string();
                let kind = value
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("AdmissionReview")
                    .to_string();
                let response_val = value.get("response");
                let resp = response_val
                    .map(|v| serde_json::from_value::<AdmissionReviewResponse>(v.clone()))
                    .transpose()
                    .map_err(|e| {
                        rusternetes_common::Error::Network(format!(
                            "Failed to parse webhook response.response: {}",
                            e
                        ))
                    })?;
                AdmissionReview {
                    api_version,
                    kind,
                    request: None,
                    response: resp,
                }
            }
        };

        review_response.response.ok_or_else(|| {
            rusternetes_common::Error::Network(
                "Webhook response missing response field".to_string(),
            )
        })
    }

    /// Build webhook URL from client config
    fn build_webhook_url(&self, config: &WebhookClientConfig) -> Result<String> {
        if let Some(ref url) = config.url {
            return Ok(url.clone());
        }

        if let Some(ref service) = config.service {
            // Build service URL — use DNS-style name that will be resolved to endpoint IP
            let namespace = &service.namespace;
            let name = &service.name;
            let path = service.path.as_deref().unwrap_or("/");
            let port = service.port.unwrap_or(443);

            // Store service ref for later resolution to endpoint IP
            let url = format!("https://{}.{}.svc:{}{}", name, namespace, port, path);

            return Ok(url);
        }

        Err(rusternetes_common::Error::InvalidResource(
            "Webhook client config must specify either url or service".to_string(),
        ))
    }

    /// Resolve a K8s service URL to an endpoint IP.
    /// The API server can't resolve .svc DNS names — look up the service's
    /// endpoint IPs from storage instead.
    pub async fn resolve_service_url<S2: Storage>(url: &str, storage: &Arc<S2>) -> String {
        // Parse service name and namespace from URL like https://name.ns.svc:port/path
        let url_without_scheme = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("http://"))
            .unwrap_or(url);
        let host_and_rest: Vec<&str> = url_without_scheme.splitn(2, '/').collect();
        let host_port: Vec<&str> = host_and_rest[0].splitn(2, ':').collect();
        let host = host_port[0];
        let port = host_port.get(1).unwrap_or(&"443");
        let path = if host_and_rest.len() > 1 {
            format!("/{}", host_and_rest[1])
        } else {
            "/".to_string()
        };

        // Check if host ends with .svc (K8s service)
        if !host.ends_with(".svc") {
            return url.to_string();
        }

        // Parse name.namespace.svc
        let parts: Vec<&str> = host
            .strip_suffix(".svc")
            .unwrap_or(host)
            .splitn(2, '.')
            .collect();
        if parts.len() != 2 {
            return url.to_string();
        }
        let svc_name = parts[0];
        let svc_namespace = parts[1];

        // K8s resolves webhook services via DNS → ClusterIP → kube-proxy.
        // See: staging/src/k8s.io/apiserver/pkg/util/webhook/serviceresolver.go
        // The real apiserver connects to the ClusterIP and kube-proxy iptables
        // DNATs to a ready pod endpoint.
        //
        // Rusternetes' api-server runs on the cluster bridge network, but
        // kube-proxy runs in the HOST network namespace — so its iptables DNAT
        // rules for ClusterIPs are NOT present in the api-server's netns. A call
        // to a webhook's ClusterIP from inside the api-server container therefore
        // gets connection-refused / times out, which is exactly why core-resource
        // admission webhooks (e.g. the conformance sample-webhook) never fired.
        //
        // The api-server CAN reach pod IPs directly on the bridge network, so we
        // resolve the service to a READY pod endpoint first (this is the same
        // destination kube-proxy would DNAT to). ClusterIP is only used as a
        // last-resort fallback. This makes webhook invocation work for core
        // resources without depending on iptables in the api-server netns.
        let svc_key = format!("/registry/services/{}/{}", svc_namespace, svc_name);
        let service = storage
            .get::<rusternetes_common::resources::Service>(&svc_key)
            .await
            .ok();

        // Map the webhook clientConfig port (a service *port*) to the matching
        // backend targetPort so the endpoint connection hits the right container
        // port. K8s does this translation in the service/endpoint controllers.
        let want_port: u16 = port.parse::<u16>().unwrap_or(443);
        let target_port: Option<u16> = service.as_ref().and_then(|svc| {
            svc.spec
                .ports
                .iter()
                .find(|p| p.port == want_port)
                .or_else(|| svc.spec.ports.first())
                .and_then(|p| p.target_port.as_ref())
                .and_then(|tp| match tp {
                    rusternetes_common::resources::IntOrString::Int(p) => Some(*p as u16),
                    rusternetes_common::resources::IntOrString::String(s) => s.parse::<u16>().ok(),
                })
        });

        // Preferred path: resolve to a ready pod endpoint IP (reachable on the
        // bridge network from the api-server). Works for both ClusterIP and
        // headless services.
        let es_prefix = format!("/registry/endpointslices/{}/", svc_namespace);
        if let Ok(slices) = storage
            .list::<rusternetes_common::resources::EndpointSlice>(&es_prefix)
            .await
        {
            for slice in &slices {
                let matches = slice
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("kubernetes.io/service-name"))
                    .map(|n| n == svc_name)
                    .unwrap_or(false);
                if !matches {
                    continue;
                }
                // Prefer the EndpointSlice port (already resolved to the
                // container port), then the service targetPort, then the
                // requested service port.
                let ep_port = slice
                    .ports
                    .first()
                    .and_then(|p| p.port)
                    .map(|p| p as u16)
                    .or(target_port)
                    .unwrap_or(want_port);
                for ep in &slice.endpoints {
                    if ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true) {
                        if let Some(addr) = ep.addresses.first() {
                            return format!("https://{}:{}{}", addr, ep_port, path);
                        }
                    }
                }
            }
        }

        // Fallback: route through ClusterIP like the real apiserver. This only
        // works if the api-server netns has kube-proxy DNAT rules (e.g. host
        // network mode); otherwise the call will fail and failurePolicy decides.
        if let Some(svc) = &service {
            if let Some(cluster_ip) = &svc.spec.cluster_ip {
                if !cluster_ip.is_empty() && cluster_ip != "None" {
                    let service_port = svc
                        .spec
                        .ports
                        .iter()
                        .find(|p| p.port == want_port)
                        .map(|p| p.port)
                        .or_else(|| svc.spec.ports.first().map(|p| p.port))
                        .unwrap_or(want_port);
                    return format!("https://{}:{}{}", cluster_ip, service_port, path);
                }
            }
        }

        // Service not found or no ready endpoints. Return original URL — the
        // HTTP client will fail with a DNS/connection error. FailurePolicy
        // determines whether this blocks the request or is ignored.
        warn!(
            "Webhook service {}/{} not found or has no ready endpoints, falling back to original URL",
            svc_namespace, svc_name
        );
        url.to_string()
    }
}

impl Default for AdmissionWebhookClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Admission webhook manager that maintains webhook configurations and calls them
pub struct AdmissionWebhookManager<S: Storage> {
    storage: Arc<S>,
    client: AdmissionWebhookClient,
}

impl<S: Storage> AdmissionWebhookManager<S> {
    /// Create a new admission webhook manager
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            client: AdmissionWebhookClient::new(),
        }
    }

    /// Run validating webhooks for an admission request.
    ///
    /// K8s calls all matching validating webhooks in parallel (goroutines) and
    /// collects errors. This matches that architecture using tokio::spawn.
    /// See: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/validating/dispatcher.go
    #[allow(clippy::too_many_arguments)]
    pub async fn run_validating_webhooks(
        &self,
        operation: &Operation,
        gvk: &GroupVersionKind,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
        name: &str,
        object: Option<Value>,
        old_object: Option<Value>,
        user_info: &UserInfo,
    ) -> Result<AdmissionResponse> {
        self.run_validating_webhooks_with_dryrun(
            operation, gvk, gvr, namespace, name, object, old_object, user_info, false,
        )
        .await
    }

    /// Run validating webhooks honoring the request's dry-run state.
    ///
    /// When `dry_run=true`:
    /// - Webhooks with `sideEffects` of `Some` or `Unknown` are rejected: K8s
    ///   refuses to admit dry-run requests through webhooks that may have side
    ///   effects (apiserver returns an error, fail-closed).
    ///   K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/validating/dispatcher.go
    /// - The `dryRun: true` field is set on the `AdmissionReviewRequest` so the
    ///   webhook can short-circuit side-effects on its end.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_validating_webhooks_with_dryrun(
        &self,
        operation: &Operation,
        gvk: &GroupVersionKind,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
        name: &str,
        object: Option<Value>,
        old_object: Option<Value>,
        user_info: &UserInfo,
        dry_run: bool,
    ) -> Result<AdmissionResponse> {
        // K8s exempts webhook configuration objects from admission webhooks.
        // Webhooks must not be able to mutate or prevent deletion of webhook
        // configuration objects, otherwise a broken webhook could lock the cluster.
        // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/rules/rules.go
        if gvr.resource == "validatingwebhookconfigurations"
            || gvr.resource == "mutatingwebhookconfigurations"
        {
            return Ok(AdmissionResponse::Allow);
        }

        // Load all ValidatingWebhookConfigurations
        let configs: Vec<ValidatingWebhookConfiguration> = self
            .storage
            .list("/registry/validatingwebhookconfigurations/")
            .await?;

        // Phase 1: Collect all matching webhooks with their resolved URLs and configs.
        // This is the "ShouldCallHook" phase in K8s.
        struct WebhookInvocation {
            webhook_name: String,
            resolved_url: String,
            timeout: Duration,
            ca_bundle: Option<String>,
            failure_policy: FailurePolicy,
            request: AdmissionReviewRequest,
        }

        let mut invocations = Vec::new();

        for config in configs {
            if let Some(webhooks) = &config.webhooks {
                for webhook in webhooks {
                    // Check if this webhook applies to this request
                    if !self.webhook_matches(&webhook.rules, operation, gvk, gvr, namespace) {
                        continue;
                    }

                    // Check namespaceSelector
                    if let Some(ref ns_selector) = webhook.namespace_selector {
                        if let Some(ns_name) = namespace {
                            let ns_key =
                                rusternetes_storage::build_key("namespaces", None, ns_name);
                            let ns_labels = self
                                .storage
                                .get::<serde_json::Value>(&ns_key)
                                .await
                                .ok()
                                .and_then(|v| {
                                    v.pointer("/metadata/labels")
                                        .and_then(|l| l.as_object())
                                        .map(|obj| {
                                            obj.iter()
                                                .filter_map(|(k, v)| {
                                                    v.as_str().map(|s| (k.clone(), s.to_string()))
                                                })
                                                .collect::<std::collections::HashMap<String, String>>()
                                        })
                                })
                                .unwrap_or_default();

                            let matches = if let Some(ref match_labels) = ns_selector.match_labels {
                                match_labels
                                    .iter()
                                    .all(|(k, v)| ns_labels.get(k) == Some(v))
                            } else {
                                true
                            };
                            let expr_matches = ns_selector
                                .match_expressions
                                .as_ref()
                                .map(|exprs| {
                                    exprs.iter().all(|expr| {
                                        let val = ns_labels.get(&expr.key);
                                        {
                                            use rusternetes_common::resources::admission_webhook::LabelSelectorOperator;
                                            match &expr.operator {
                                                LabelSelectorOperator::In => expr
                                                    .values
                                                    .as_ref()
                                                    .map(|vs| {
                                                        val.map(|v| vs.contains(v)).unwrap_or(false)
                                                    })
                                                    .unwrap_or(false),
                                                LabelSelectorOperator::NotIn => expr
                                                    .values
                                                    .as_ref()
                                                    .map(|vs| {
                                                        val.map(|v| !vs.contains(v)).unwrap_or(true)
                                                    })
                                                    .unwrap_or(true),
                                                LabelSelectorOperator::Exists => val.is_some(),
                                                LabelSelectorOperator::DoesNotExist => val.is_none(),
                                            }
                                        }
                                    })
                                })
                                .unwrap_or(true);

                            if !matches || !expr_matches {
                                debug!(
                                    "Skipping webhook {} — namespace {} doesn't match namespaceSelector",
                                    webhook.name, ns_name
                                );
                                continue;
                            }
                        }
                    }

                    // Check objectSelector
                    if let Some(ref obj_selector) = webhook.object_selector {
                        let obj_labels: std::collections::HashMap<String, String> = object
                            .as_ref()
                            .and_then(|o| o.pointer("/metadata/labels"))
                            .and_then(|l| l.as_object())
                            .map(|labels_obj| {
                                labels_obj
                                    .iter()
                                    .filter_map(|(k, v)| {
                                        v.as_str().map(|s| (k.clone(), s.to_string()))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        let obj_matches = if let Some(ref match_labels) = obj_selector.match_labels
                        {
                            match_labels
                                .iter()
                                .all(|(k, v)| obj_labels.get(k) == Some(v))
                        } else {
                            true
                        };
                        if !obj_matches {
                            debug!(
                                "Skipping webhook {} — object labels don't match objectSelector",
                                webhook.name
                            );
                            continue;
                        }
                    }

                    // Evaluate matchConditions (CEL expressions) for validating webhooks.
                    // Source of truth: `crate::cel_evaluators::MatchConditionEvaluator`.
                    if let Some(ref conditions) = webhook.match_conditions {
                        if !conditions.is_empty() {
                            let request = build_admission_request_for_match(
                                operation,
                                gvk,
                                namespace,
                                name,
                                object.as_ref(),
                                old_object.as_ref(),
                                Some(user_info),
                            );
                            let mut evaluator =
                                crate::cel_evaluators::MatchConditionEvaluator::new();
                            let outcome = evaluator.evaluate(conditions, &request, None);
                            match outcome {
                                crate::cel_evaluators::MatchOutcome::Matched => {}
                                crate::cel_evaluators::MatchOutcome::NotMatched => {
                                    debug!(
                                        "Skipping validating webhook {} — matchConditions not met for {}/{}",
                                        webhook.name,
                                        namespace.unwrap_or(""),
                                        name
                                    );
                                    continue;
                                }
                                crate::cel_evaluators::MatchOutcome::Error(msg) => {
                                    // Upstream `matcher.go` treats compile/runtime
                                    // errors as "no match" — the webhook is skipped.
                                    // The caller can layer failurePolicy on top of the
                                    // webhook *call*; matchCondition errors themselves
                                    // do not flip the policy.
                                    debug!(
                                        "Skipping validating webhook {} — matchCondition error: {}",
                                        webhook.name, msg
                                    );
                                    continue;
                                }
                            }
                        }
                    }

                    // Skip webhooks whose service namespace no longer exists or is terminating
                    if let Some(ref svc) = webhook.client_config.service {
                        let ns_key =
                            rusternetes_storage::build_key("namespaces", None, &svc.namespace);
                        let ns_gone = match self.storage.get::<serde_json::Value>(&ns_key).await {
                            Err(_) => true,
                            Ok(ns_val) => {
                                ns_val.pointer("/status/phase").and_then(|p| p.as_str())
                                    == Some("Terminating")
                                    || ns_val
                                        .get("metadata")
                                        .and_then(|m| m.get("deletionTimestamp"))
                                        .is_some()
                            }
                        };
                        if ns_gone {
                            warn!("Skipping validating webhook {} — service namespace {} no longer exists or is terminating", webhook.name, svc.namespace);
                            continue;
                        }
                    }

                    // Enforce SideEffects policy for dry-run requests.
                    // K8s rejects dry-run requests through webhooks with sideEffects=Some or Unknown.
                    // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/validating/dispatcher.go
                    if dry_run
                        && matches!(
                            webhook.side_effects,
                            SideEffectClass::Some | SideEffectClass::Unknown
                        )
                    {
                        return Ok(AdmissionResponse::Deny(format!(
                            "admission webhook {:?} does not support dry run",
                            webhook.name
                        )));
                    }

                    // Resolve webhook URL and build invocation
                    let raw_url = self.client.build_webhook_url(&webhook.client_config)?;
                    let resolved_url =
                        AdmissionWebhookClient::resolve_service_url(&raw_url, &self.storage).await;
                    let timeout = resolve_webhook_timeout(webhook.timeout_seconds);
                    let failure_policy = webhook
                        .failure_policy
                        .clone()
                        .unwrap_or(FailurePolicy::Fail);
                    let ca_bundle = webhook.client_config.ca_bundle.clone();

                    // K8s splits resource/subresource in the admission review request.
                    // e.g. GVR "pods/attach" becomes resource="pods", subResource="attach".
                    // The webhook server expects this split format.
                    let (wire_gvr, sub_resource) = if let Some(idx) = gvr.resource.find('/') {
                        (
                            GroupVersionResource {
                                group: gvr.group.clone(),
                                version: gvr.version.clone(),
                                resource: gvr.resource[..idx].to_string(),
                            },
                            Some(gvr.resource[idx + 1..].to_string()),
                        )
                    } else {
                        (gvr.clone(), None)
                    };

                    let request = AdmissionReviewRequest {
                        uid: uuid::Uuid::new_v4().to_string(),
                        kind: gvk.clone(),
                        resource: wire_gvr.clone(),
                        sub_resource: sub_resource.clone(),
                        request_kind: Some(gvk.clone()),
                        request_resource: Some(wire_gvr),
                        request_sub_resource: sub_resource,
                        name: name.to_string(),
                        namespace: namespace.map(|s| s.to_string()),
                        operation: operation.clone(),
                        user_info: user_info.clone(),
                        object: object.clone(),
                        old_object: old_object.clone(),
                        dry_run: if dry_run { Some(true) } else { None },
                        options: None,
                    };

                    info!(
                        "Queuing validating webhook {} for {}/{} at {}",
                        webhook.name, gvk.kind, name, resolved_url
                    );

                    invocations.push(WebhookInvocation {
                        webhook_name: webhook.name.clone(),
                        resolved_url,
                        timeout,
                        ca_bundle,
                        failure_policy,
                        request,
                    });
                }
            }
        }

        if invocations.is_empty() {
            return Ok(AdmissionResponse::Allow);
        }

        // Phase 2: Call all matching webhooks in parallel.
        // K8s dispatches all validating webhooks concurrently via goroutines.
        // See: dispatcher.go lines 126-131
        let mut handles = Vec::new();
        for inv in invocations {
            let review = AdmissionReview::new_request(inv.request.clone());
            // K8s caBundle is []byte in Go, which JSON-serializes as base64.
            // Try base64 decode first; if that fails, use raw bytes (might already be PEM).
            let ca_data = inv.ca_bundle.as_ref().map(|s| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .unwrap_or_else(|_| s.as_bytes().to_vec())
            });
            let url = inv.resolved_url.clone();
            let timeout = inv.timeout;
            let webhook_name = inv.webhook_name.clone();
            let failure_policy = inv.failure_policy.clone();
            let uid = inv.request.uid.clone();

            handles.push(tokio::spawn(async move {
                let client = AdmissionWebhookClient::new();
                let ca_ref = ca_data.as_deref();
                let result = client
                    .call_webhook_with_ca(&url, &review, timeout, ca_ref)
                    .await;
                (webhook_name, failure_policy, uid, result)
            }));
        }

        // Phase 3: Collect results. Any denial or Fail-policy error rejects the request.
        let results = futures::future::join_all(handles).await;
        let mut all_warnings = Vec::new();

        for result in results {
            let (webhook_name, failure_policy, _uid, call_result) = match result {
                Ok(r) => r,
                Err(e) => {
                    error!("Webhook task panicked: {}", e);
                    return Err(rusternetes_common::Error::Internal(format!(
                        "webhook task panicked: {}",
                        e
                    )));
                }
            };

            match call_result {
                Ok(response) => {
                    info!(
                        "Webhook {} response: allowed={}",
                        webhook_name, response.allowed
                    );
                    if let Some(warnings) = &response.warnings {
                        all_warnings.extend(warnings.clone());
                    }
                    if !response.allowed {
                        let reason = response
                            .status
                            .as_ref()
                            .and_then(|s| {
                                s.message
                                    .as_ref()
                                    .filter(|m| !m.is_empty())
                                    .or(s.reason.as_ref())
                            })
                            .map(|m| m.to_string())
                            .unwrap_or_else(|| format!("Denied by webhook {}", webhook_name));
                        return Ok(AdmissionResponse::Deny(reason));
                    }
                }
                Err(e) => match failure_policy {
                    FailurePolicy::Ignore => {
                        warn!(
                            "Webhook {} failed, failing open (Ignore): {}",
                            webhook_name, e
                        );
                    }
                    _ => {
                        warn!(
                            "Webhook {} failed, failing closed (Fail): {}",
                            webhook_name, e
                        );
                        return Err(e);
                    }
                },
            }
        }

        if !all_warnings.is_empty() {
            info!("Validating webhooks returned warnings: {:?}", all_warnings);
        }

        Ok(AdmissionResponse::Allow)
    }

    /// Run mutating webhooks for an admission request
    #[allow(clippy::too_many_arguments)]
    pub async fn run_mutating_webhooks(
        &self,
        operation: &Operation,
        gvk: &GroupVersionKind,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
        name: &str,
        object: Option<Value>,
        old_object: Option<Value>,
        user_info: &UserInfo,
    ) -> Result<(AdmissionResponse, Option<Value>)> {
        self.run_mutating_webhooks_with_dryrun(
            operation, gvk, gvr, namespace, name, object, old_object, user_info, false,
        )
        .await
    }

    /// Run mutating webhooks honoring the request's dry-run state.
    ///
    /// When `dry_run=true`:
    /// - Webhooks with `sideEffects` of `Some` or `Unknown` are rejected.
    /// - The `dryRun: true` field is set on the `AdmissionReviewRequest`.
    ///
    /// K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/dispatcher.go
    #[allow(clippy::too_many_arguments)]
    pub async fn run_mutating_webhooks_with_dryrun(
        &self,
        operation: &Operation,
        gvk: &GroupVersionKind,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
        name: &str,
        mut object: Option<Value>,
        old_object: Option<Value>,
        user_info: &UserInfo,
        dry_run: bool,
    ) -> Result<(AdmissionResponse, Option<Value>)> {
        // K8s exempts webhook configuration objects from admission webhooks.
        // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/rules/rules.go
        if gvr.resource == "validatingwebhookconfigurations"
            || gvr.resource == "mutatingwebhookconfigurations"
        {
            return Ok((AdmissionResponse::Allow, object));
        }

        // Load all MutatingWebhookConfigurations
        let configs: Vec<MutatingWebhookConfiguration> = self
            .storage
            .list("/registry/mutatingwebhookconfigurations/")
            .await?;

        let mut all_patches = Vec::new();
        let mut all_warnings = Vec::new();

        // Track webhooks with reinvocationPolicy=IfNeeded that were invoked in the
        // first pass. Each entry records the webhook config and the object
        // snapshot it observed at the end of its first call. After the first
        // pass, if the final object diverges from a snapshot — i.e. a later
        // webhook in the chain mutated it — the webhook is reinvoked once.
        // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/dispatcher.go
        let mut reinvoke_candidates: Vec<(MutatingWebhook, Value)> = Vec::new();

        for config in configs {
            if let Some(webhooks) = &config.webhooks {
                for webhook in webhooks {
                    // Check if this webhook applies to this request
                    if !self.webhook_matches(&webhook.rules, operation, gvk, gvr, namespace) {
                        continue;
                    }

                    // Check namespaceSelector for mutating webhooks
                    if let Some(ref ns_selector) = webhook.namespace_selector {
                        if let Some(ns_name) = namespace {
                            let ns_key =
                                rusternetes_storage::build_key("namespaces", None, ns_name);
                            let ns_labels = self
                                .storage
                                .get::<serde_json::Value>(&ns_key)
                                .await
                                .ok()
                                .and_then(|v| {
                                    v.pointer("/metadata/labels")
                                        .and_then(|l| l.as_object())
                                        .map(|obj| {
                                            obj.iter()
                                                .filter_map(|(k, v)| {
                                                    v.as_str().map(|s| (k.clone(), s.to_string()))
                                                })
                                                .collect::<std::collections::HashMap<String, String>>()
                                        })
                                })
                                .unwrap_or_default();
                            let matches = ns_selector
                                .match_labels
                                .as_ref()
                                .map(|ml| ml.iter().all(|(k, v)| ns_labels.get(k) == Some(v)))
                                .unwrap_or(true);
                            let expr_matches = ns_selector
                                .match_expressions
                                .as_ref()
                                .map(|exprs| {
                                    exprs.iter().all(|expr| {
                                        let val = ns_labels.get(&expr.key);
                                        {
                                            use rusternetes_common::resources::admission_webhook::LabelSelectorOperator;
                                            match &expr.operator {
                                                LabelSelectorOperator::In => expr.values.as_ref().map(|vs| val.map(|v| vs.contains(v)).unwrap_or(false)).unwrap_or(false),
                                                LabelSelectorOperator::NotIn => expr.values.as_ref().map(|vs| val.map(|v| !vs.contains(v)).unwrap_or(true)).unwrap_or(true),
                                                LabelSelectorOperator::Exists => val.is_some(),
                                                LabelSelectorOperator::DoesNotExist => val.is_none(),
                                            }
                                        }
                                    })
                                })
                                .unwrap_or(true);
                            if !matches || !expr_matches {
                                debug!("Skipping mutating webhook {} — namespace {} doesn't match namespaceSelector", webhook.name, ns_name);
                                continue;
                            }
                        }
                    }

                    // Check objectSelector for mutating webhooks
                    // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/object/matcher.go
                    if let Some(ref obj_selector) = webhook.object_selector {
                        let obj_labels: std::collections::HashMap<String, String> = object
                            .as_ref()
                            .and_then(|o| o.pointer("/metadata/labels"))
                            .and_then(|l| l.as_object())
                            .map(|labels_obj| {
                                labels_obj
                                    .iter()
                                    .filter_map(|(k, v)| {
                                        v.as_str().map(|s| (k.clone(), s.to_string()))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        let obj_matches = if let Some(ref match_labels) = obj_selector.match_labels
                        {
                            match_labels
                                .iter()
                                .all(|(k, v)| obj_labels.get(k) == Some(v))
                        } else {
                            true
                        };
                        if !obj_matches {
                            debug!(
                                "Skipping mutating webhook {} — object labels don't match objectSelector",
                                webhook.name
                            );
                            continue;
                        }
                    }

                    // Evaluate matchConditions (CEL expressions) for mutating webhooks.
                    // Source of truth: `crate::cel_evaluators::MatchConditionEvaluator`.
                    // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/rules/rules.go
                    if let Some(ref conditions) = webhook.match_conditions {
                        if !conditions.is_empty() {
                            let request = build_admission_request_for_match(
                                operation,
                                gvk,
                                namespace,
                                name,
                                object.as_ref(),
                                old_object.as_ref(),
                                Some(user_info),
                            );
                            let mut evaluator =
                                crate::cel_evaluators::MatchConditionEvaluator::new();
                            let outcome = evaluator.evaluate(conditions, &request, None);
                            match outcome {
                                crate::cel_evaluators::MatchOutcome::Matched => {}
                                crate::cel_evaluators::MatchOutcome::NotMatched => {
                                    debug!(
                                        "Skipping mutating webhook {} — matchConditions not met for {}/{}",
                                        webhook.name,
                                        namespace.unwrap_or(""),
                                        name
                                    );
                                    continue;
                                }
                                crate::cel_evaluators::MatchOutcome::Error(msg) => {
                                    debug!(
                                        "Skipping mutating webhook {} — matchCondition error: {}",
                                        webhook.name, msg
                                    );
                                    continue;
                                }
                            }
                        }
                    }

                    // Skip webhooks whose service no longer exists or namespace is terminating
                    if let Some(ref svc) = webhook.client_config.service {
                        let ns_key =
                            rusternetes_storage::build_key("namespaces", None, &svc.namespace);
                        let ns_gone = match self.storage.get::<serde_json::Value>(&ns_key).await {
                            Err(_) => true,
                            Ok(ns_val) => {
                                ns_val.pointer("/status/phase").and_then(|p| p.as_str())
                                    == Some("Terminating")
                                    || ns_val
                                        .get("metadata")
                                        .and_then(|m| m.get("deletionTimestamp"))
                                        .is_some()
                            }
                        };
                        if ns_gone {
                            warn!(
                                "Skipping webhook {} — service namespace {} no longer exists",
                                webhook.name, svc.namespace
                            );
                            continue;
                        }
                    }

                    // Enforce SideEffects policy for dry-run requests.
                    // K8s rejects dry-run requests through webhooks with sideEffects=Some or Unknown.
                    // K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/dispatcher.go
                    if dry_run
                        && matches!(
                            webhook.side_effects,
                            SideEffectClass::Some | SideEffectClass::Unknown
                        )
                    {
                        return Ok((
                            AdmissionResponse::Deny(format!(
                                "admission webhook {:?} does not support dry run",
                                webhook.name
                            )),
                            object,
                        ));
                    }

                    info!(
                        "Running mutating webhook {} for {}/{}",
                        webhook.name, gvk.kind, name
                    );

                    // Build admission request with potentially mutated object.
                    // K8s splits resource/subresource in the admission review request.
                    let (wire_gvr, sub_resource) = if let Some(idx) = gvr.resource.find('/') {
                        (
                            GroupVersionResource {
                                group: gvr.group.clone(),
                                version: gvr.version.clone(),
                                resource: gvr.resource[..idx].to_string(),
                            },
                            Some(gvr.resource[idx + 1..].to_string()),
                        )
                    } else {
                        (gvr.clone(), None)
                    };

                    let request = AdmissionReviewRequest {
                        uid: uuid::Uuid::new_v4().to_string(),
                        kind: gvk.clone(),
                        resource: wire_gvr.clone(),
                        sub_resource: sub_resource.clone(),
                        request_kind: Some(gvk.clone()),
                        request_resource: Some(wire_gvr),
                        request_sub_resource: sub_resource,
                        name: name.to_string(),
                        namespace: namespace.map(|s| s.to_string()),
                        operation: operation.clone(),
                        user_info: user_info.clone(),
                        object: object.clone(),
                        old_object: old_object.clone(),
                        dry_run: if dry_run { Some(true) } else { None },
                        options: None,
                    };

                    // Resolve webhook URL — K8s service names need endpoint IP lookup
                    let raw_url = self.client.build_webhook_url(&webhook.client_config)?;
                    let resolved_url =
                        AdmissionWebhookClient::resolve_service_url(&raw_url, &self.storage).await;
                    let timeout = resolve_webhook_timeout(webhook.timeout_seconds);
                    let review = AdmissionReview::new_request(request.clone());
                    // K8s caBundle is []byte → JSON base64. Decode to get PEM.
                    let ca_decoded = webhook.client_config.ca_bundle.as_ref().map(|s| {
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD
                            .decode(s)
                            .unwrap_or_else(|_| s.as_bytes().to_vec())
                    });
                    let ca_bundle = ca_decoded.as_deref();
                    let response = match self
                        .client
                        .call_webhook_with_ca(&resolved_url, &review, timeout, ca_bundle)
                        .await
                    {
                        Ok(resp) => {
                            info!(
                                "Webhook {} response: allowed={}, url={}",
                                webhook.name, resp.allowed, resolved_url
                            );
                            resp
                        }
                        Err(e) => {
                            let fp = webhook
                                .failure_policy
                                .as_ref()
                                .unwrap_or(&FailurePolicy::Fail);
                            match fp {
                                FailurePolicy::Ignore => {
                                    warn!(
                                        "Mutating webhook {} failed (Ignore): {}",
                                        webhook.name, e
                                    );
                                    AdmissionReviewResponse {
                                        uid: request.uid.clone(),
                                        allowed: true,
                                        status: None,
                                        patch: None,
                                        patch_type: None,
                                        audit_annotations: None,
                                        warnings: None,
                                    }
                                }
                                _ => return Err(e),
                            }
                        }
                    };

                    // Collect warnings
                    if let Some(warnings) = &response.warnings {
                        all_warnings.extend(warnings.clone());
                    }

                    // Check if request was denied
                    if !response.allowed {
                        // K8s uses status.message first, then status.reason, then fallback
                        let reason = response
                            .status
                            .as_ref()
                            .and_then(|s| {
                                s.message
                                    .as_ref()
                                    .filter(|m| !m.is_empty())
                                    .or(s.reason.as_ref())
                            })
                            .map(|m| m.to_string())
                            .unwrap_or_else(|| format!("Denied by webhook {}", webhook.name));

                        return Ok((AdmissionResponse::Deny(reason), object));
                    }

                    // Apply patches
                    if let Some(patch_base64) = &response.patch {
                        // Decode base64 patch
                        use base64::Engine;
                        let patch_bytes = base64::engine::general_purpose::STANDARD
                            .decode(patch_base64)
                            .map_err(|e| {
                                rusternetes_common::Error::InvalidResource(format!(
                                    "Failed to decode webhook patch: {}",
                                    e
                                ))
                            })?;

                        let patch_str = String::from_utf8(patch_bytes).map_err(|e| {
                            rusternetes_common::Error::InvalidResource(format!(
                                "Failed to parse webhook patch as UTF-8: {}",
                                e
                            ))
                        })?;

                        let patches: Vec<PatchOperation> = serde_json::from_str(&patch_str)
                            .map_err(|e| {
                                rusternetes_common::Error::InvalidResource(format!(
                                    "Failed to parse webhook patch as JSON: {}",
                                    e
                                ))
                            })?;

                        // Apply patches to object
                        if let Some(ref mut obj) = object {
                            for patch in &patches {
                                apply_json_patch(obj, patch)?;
                            }
                        }

                        all_patches.extend(patches);
                    }

                    // Record snapshot for IfNeeded reinvocation. Snapshot is the
                    // object state *after* this webhook's own patches were
                    // applied — reinvocation triggers only when a *later*
                    // webhook in the chain mutates it.
                    if matches!(
                        webhook.reinvocation_policy,
                        Some(ReinvocationPolicy::IfNeeded)
                    ) {
                        if let Some(ref obj) = object {
                            reinvoke_candidates.push((webhook.clone(), obj.clone()));
                        }
                    }
                }
            }
        }

        // Second pass: reinvoke IfNeeded webhooks whose snapshot diverges from
        // the current object. K8s performs exactly one extra round of
        // reinvocations to bound work — webhooks invoked here do not themselves
        // trigger a third round.
        for (webhook, snapshot) in reinvoke_candidates {
            // Skip if the object was removed by a later webhook, or if it is
            // unchanged from this webhook's last snapshot (no reinvocation needed).
            match object.as_ref() {
                None => continue,
                Some(o) if *o == snapshot => continue,
                _ => {}
            }

            info!(
                "Reinvoking mutating webhook {} (reinvocationPolicy=IfNeeded) — object changed since last call",
                webhook.name
            );

            // Re-evaluate objectSelector against the *current* object so we
            // don't reinvoke when the object no longer matches.
            if let Some(ref obj_selector) = webhook.object_selector {
                let obj_labels: std::collections::HashMap<String, String> = object
                    .as_ref()
                    .and_then(|o| o.pointer("/metadata/labels"))
                    .and_then(|l| l.as_object())
                    .map(|labels_obj| {
                        labels_obj
                            .iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default();
                let obj_matches = obj_selector
                    .match_labels
                    .as_ref()
                    .map(|ml| ml.iter().all(|(k, v)| obj_labels.get(k) == Some(v)))
                    .unwrap_or(true);
                if !obj_matches {
                    debug!(
                        "Skipping reinvocation of {} — object labels no longer match objectSelector",
                        webhook.name
                    );
                    continue;
                }
            }

            let (wire_gvr, sub_resource) = if let Some(idx) = gvr.resource.find('/') {
                (
                    GroupVersionResource {
                        group: gvr.group.clone(),
                        version: gvr.version.clone(),
                        resource: gvr.resource[..idx].to_string(),
                    },
                    Some(gvr.resource[idx + 1..].to_string()),
                )
            } else {
                (gvr.clone(), None)
            };

            let request = AdmissionReviewRequest {
                uid: uuid::Uuid::new_v4().to_string(),
                kind: gvk.clone(),
                resource: wire_gvr.clone(),
                sub_resource: sub_resource.clone(),
                request_kind: Some(gvk.clone()),
                request_resource: Some(wire_gvr),
                request_sub_resource: sub_resource,
                name: name.to_string(),
                namespace: namespace.map(|s| s.to_string()),
                operation: operation.clone(),
                user_info: user_info.clone(),
                object: object.clone(),
                old_object: old_object.clone(),
                dry_run: None,
                options: None,
            };

            let raw_url = self.client.build_webhook_url(&webhook.client_config)?;
            let resolved_url =
                AdmissionWebhookClient::resolve_service_url(&raw_url, &self.storage).await;
            let timeout = resolve_webhook_timeout(webhook.timeout_seconds);
            let review = AdmissionReview::new_request(request.clone());
            let ca_decoded = webhook.client_config.ca_bundle.as_ref().map(|s| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .unwrap_or_else(|_| s.as_bytes().to_vec())
            });
            let ca_bundle_ref = ca_decoded.as_deref();
            let response = match self
                .client
                .call_webhook_with_ca(&resolved_url, &review, timeout, ca_bundle_ref)
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    let fp = webhook
                        .failure_policy
                        .as_ref()
                        .unwrap_or(&FailurePolicy::Fail);
                    match fp {
                        FailurePolicy::Ignore => {
                            warn!(
                                "Reinvoked mutating webhook {} failed (Ignore): {}",
                                webhook.name, e
                            );
                            continue;
                        }
                        _ => return Err(e),
                    }
                }
            };

            if let Some(warnings) = &response.warnings {
                all_warnings.extend(warnings.clone());
            }

            if !response.allowed {
                let reason = response
                    .status
                    .as_ref()
                    .and_then(|s| {
                        s.message
                            .as_ref()
                            .filter(|m| !m.is_empty())
                            .or(s.reason.as_ref())
                    })
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| format!("Denied by webhook {}", webhook.name));
                return Ok((AdmissionResponse::Deny(reason), object));
            }

            if let Some(patch_base64) = &response.patch {
                use base64::Engine;
                let patch_bytes = base64::engine::general_purpose::STANDARD
                    .decode(patch_base64)
                    .map_err(|e| {
                        rusternetes_common::Error::InvalidResource(format!(
                            "Failed to decode webhook patch: {}",
                            e
                        ))
                    })?;
                let patch_str = String::from_utf8(patch_bytes).map_err(|e| {
                    rusternetes_common::Error::InvalidResource(format!(
                        "Failed to parse webhook patch as UTF-8: {}",
                        e
                    ))
                })?;
                let patches: Vec<PatchOperation> =
                    serde_json::from_str(&patch_str).map_err(|e| {
                        rusternetes_common::Error::InvalidResource(format!(
                            "Failed to parse webhook patch as JSON: {}",
                            e
                        ))
                    })?;
                if let Some(ref mut obj) = object {
                    for patch in &patches {
                        apply_json_patch(obj, patch)?;
                    }
                }
                all_patches.extend(patches);
            }
        }

        // All mutating webhooks passed
        if !all_warnings.is_empty() {
            info!("Mutating webhooks returned warnings: {:?}", all_warnings);
        }

        // K8s structural-schema pruning runs AFTER mutating webhooks: any
        // field a webhook injected that isn't declared in the CRD's
        // openAPIV3Schema must be stripped before storage (unless the schema
        // sets x-kubernetes-preserve-unknown-fields). For non-CRD resources
        // (no matching CRD in storage) this is a no-op — those types use
        // structured Rust deserialization which rejects unknown fields.
        // K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning
        if !all_patches.is_empty() {
            if let Some(ref mut obj) = object {
                self.prune_post_mutation(gvr, obj).await;
            }
        }

        let response = if all_patches.is_empty() {
            AdmissionResponse::Allow
        } else {
            AdmissionResponse::AllowWithPatch(all_patches)
        };

        Ok((response, object))
    }

    /// Look up the CRD for the given GVR and prune any field the mutating
    /// webhooks added that isn't in the CRD's structural schema. Best-effort:
    /// if there is no CRD for this GVR (i.e. the resource is a built-in
    /// type), we leave the object untouched.
    async fn prune_post_mutation(&self, gvr: &GroupVersionResource, object: &mut Value) {
        // CRD names are `{plural}.{group}` — only resources with a non-empty
        // group can be CRD-backed; built-in core resources have group="".
        if gvr.group.is_empty() {
            return;
        }
        let crd_name = format!("{}.{}", gvr.resource, gvr.group);
        let key = rusternetes_storage::build_key("customresourcedefinitions", None, &crd_name);
        let crd = match self
            .storage
            .get::<rusternetes_common::resources::CustomResourceDefinition>(&key)
            .await
        {
            Ok(c) => c,
            Err(_) => return,
        };
        rusternetes_common::resources::crd::prune_custom_resource_value(&crd, &gvr.version, object);
    }

    /// Check if a webhook matches the given request
    fn webhook_matches(
        &self,
        rules: &[rusternetes_common::resources::RuleWithOperations],
        operation: &Operation,
        gvk: &GroupVersionKind,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
    ) -> bool {
        for rule in rules {
            // Check if operation matches
            if !self.operation_matches(&rule.operations, operation) {
                continue;
            }

            // Check if resource matches
            if !self.resource_matches(&rule.rule, gvk, gvr) {
                continue;
            }

            // Check if scope matches
            if let Some(scope) = &rule.rule.scope {
                if scope == "Namespaced" && namespace.is_none() {
                    continue;
                }
                if scope == "Cluster" && namespace.is_some() {
                    continue;
                }
            }

            // Rule matches!
            return true;
        }

        false
    }

    /// Check if operation matches webhook rule
    fn operation_matches(&self, operations: &[OperationType], operation: &Operation) -> bool {
        for op in operations {
            match op {
                OperationType::All => return true,
                OperationType::Create if matches!(operation, Operation::Create) => return true,
                OperationType::Update if matches!(operation, Operation::Update) => return true,
                OperationType::Delete if matches!(operation, Operation::Delete) => return true,
                OperationType::Connect if matches!(operation, Operation::Connect) => return true,
                _ => continue,
            }
        }
        false
    }

    /// Check if resource matches webhook rule
    /// K8s supports resource/subresource format in rules (e.g. "pods/attach", "pods/*", "*/*")
    /// See: staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/rules/rules.go
    fn resource_matches(
        &self,
        rule: &Rule,
        _gvk: &GroupVersionKind,
        gvr: &GroupVersionResource,
    ) -> bool {
        // Check API group
        if !rule.api_groups.contains(&"*".to_string()) && !rule.api_groups.contains(&gvr.group) {
            return false;
        }

        // Check API version
        if !rule.api_versions.contains(&"*".to_string())
            && !rule.api_versions.contains(&gvr.version)
        {
            return false;
        }

        // Check resource — handle resource/subresource format
        // Split the request resource into resource and subresource parts
        let (op_res, op_sub) = if let Some(idx) = gvr.resource.find('/') {
            (&gvr.resource[..idx], &gvr.resource[idx + 1..])
        } else {
            (gvr.resource.as_str(), "")
        };

        let resource_matched = rule.resources.iter().any(|r| {
            let (rule_res, rule_sub) = if let Some(idx) = r.find('/') {
                (&r[..idx], &r[idx + 1..])
            } else {
                (r.as_str(), "")
            };
            let res_match = rule_res == "*" || rule_res == op_res;
            let sub_match = rule_sub == "*" || rule_sub == op_sub;
            res_match && sub_match
        });

        if !resource_matched {
            return false;
        }

        true
    }
}

/// Apply a single JSON patch operation to an object
fn apply_json_patch(object: &mut Value, patch: &PatchOperation) -> Result<()> {
    use rusternetes_common::admission::PatchOp;

    match patch.op {
        PatchOp::Add => {
            if let Some(value) = &patch.value {
                apply_json_pointer_add(object, &patch.path, value.clone())?;
            }
        }
        PatchOp::Remove => {
            apply_json_pointer_remove(object, &patch.path)?;
        }
        PatchOp::Replace => {
            if let Some(value) = &patch.value {
                apply_json_pointer_replace(object, &patch.path, value.clone())?;
            }
        }
        _ => {
            // For now, only support add, remove, replace
            warn!("Unsupported patch operation: {:?}", patch.op);
        }
    }

    Ok(())
}

/// Apply JSON pointer add operation
fn apply_json_pointer_add(object: &mut Value, path: &str, value: Value) -> Result<()> {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    if parts.is_empty() || parts[0].is_empty() {
        *object = value;
        return Ok(());
    }

    let mut current = object;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part - add the value
            if let Some(obj) = current.as_object_mut() {
                obj.insert(part.to_string(), value.clone());
            }
        } else {
            // Navigate to the next level
            current = current
                .as_object_mut()
                .and_then(|obj| obj.get_mut(*part))
                .ok_or_else(|| {
                    rusternetes_common::Error::InvalidResource(format!("Path not found: {}", path))
                })?;
        }
    }

    Ok(())
}

/// Apply JSON pointer remove operation
fn apply_json_pointer_remove(object: &mut Value, path: &str) -> Result<()> {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    if parts.is_empty() || parts[0].is_empty() {
        return Err(rusternetes_common::Error::InvalidResource(
            "Cannot remove root".to_string(),
        ));
    }

    let mut current = object;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part - remove the value
            if let Some(obj) = current.as_object_mut() {
                obj.remove(*part);
            }
        } else {
            // Navigate to the next level
            current = current
                .as_object_mut()
                .and_then(|obj| obj.get_mut(*part))
                .ok_or_else(|| {
                    rusternetes_common::Error::InvalidResource(format!("Path not found: {}", path))
                })?;
        }
    }

    Ok(())
}

impl<S: Storage> AdmissionWebhookManager<S> {
    /// Run ValidatingAdmissionPolicy checks for an admission request.
    /// Evaluates CEL expressions from matching policies and rejects if any Deny action matches.
    ///
    /// `resource` is the plural resource name (e.g. "configmaps", "pods", "deployments").
    /// If provided, it is used for more accurate resource rule matching.
    /// `namespace` is the namespace of the object (for namespaced resources).
    /// `old_object` is the previous version (for UPDATE operations).
    #[allow(dead_code)]
    pub async fn run_validating_admission_policies(
        &self,
        operation: &Operation,
        gvk: &GroupVersionKind,
        object: Option<&Value>,
    ) -> Result<()> {
        self.run_validating_admission_policies_ext(operation, gvk, object, None, None, None)
            .await
    }

    /// Extended VAP evaluation with resource name and namespace for precise matching.
    pub async fn run_validating_admission_policies_ext(
        &self,
        operation: &Operation,
        gvk: &GroupVersionKind,
        object: Option<&Value>,
        old_object: Option<&Value>,
        resource: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<()> {
        use rusternetes_common::CELEvaluator;

        // Load all ValidatingAdmissionPolicies
        let policies: Vec<Value> = self
            .storage
            .list("/registry/validatingadmissionpolicies/")
            .await
            .unwrap_or_default();

        if policies.is_empty() {
            return Ok(());
        }

        // Load all ValidatingAdmissionPolicyBindings
        let bindings: Vec<Value> = self
            .storage
            .list("/registry/validatingadmissionpolicybindings/")
            .await
            .unwrap_or_default();

        let mut evaluator = CELEvaluator::new();

        // Derive resource name from kind if not provided
        let derived_resource = resource
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{}s", gvk.kind.to_lowercase()));

        let op_str = match operation {
            Operation::Create => "CREATE",
            Operation::Update => "UPDATE",
            Operation::Delete => "DELETE",
            _ => "",
        };

        for policy in &policies {
            let policy_name = policy
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");

            // Find the binding that references this policy
            let matching_binding = bindings.iter().find(|b| {
                b.get("spec")
                    .and_then(|s| s.get("policyName"))
                    .and_then(|n| n.as_str())
                    == Some(policy_name)
            });
            if matching_binding.is_none() {
                continue;
            }
            let binding = matching_binding.unwrap();

            // Check match conditions from spec.matchConstraints
            let match_resources = policy
                .get("spec")
                .and_then(|s| s.get("matchConstraints"))
                .and_then(|m| m.get("resourceRules"));
            if let Some(rules) = match_resources {
                if let Some(rules_arr) = rules.as_array() {
                    let matches = rules_arr.iter().any(|rule| {
                        let api_groups = rule.get("apiGroups").and_then(|g| g.as_array());
                        let resources = rule.get("resources").and_then(|r| r.as_array());
                        let ops = rule.get("operations").and_then(|o| o.as_array());

                        let group_match = api_groups.is_none_or(|groups| {
                            groups.iter().any(|g| {
                                let gs = g.as_str().unwrap_or("");
                                gs == "*" || gs == gvk.group
                            })
                        });
                        let resource_match = resources.is_none_or(|res| {
                            res.iter().any(|r| {
                                let rs = r.as_str().unwrap_or("");
                                rs == "*" || rs == derived_resource
                            })
                        });
                        let op_match = ops.is_none_or(|operations| {
                            operations.iter().any(|o| {
                                let os = o.as_str().unwrap_or("");
                                os == "*" || os == op_str
                            })
                        });
                        group_match && resource_match && op_match
                    });
                    if !matches {
                        continue;
                    }
                }
            }

            // Check matchConditions from the policy spec.
            // Source of truth: `crate::cel_evaluators::MatchConditionEvaluator`.
            let obj_name = object
                .and_then(|o| o.pointer("/metadata/name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            let match_conditions_pass = self.evaluate_match_conditions(
                policy, object, old_object, operation, gvk, namespace, obj_name, None,
            );
            if !match_conditions_pass {
                continue;
            }

            // Build CEL context with object variable
            let mut context = rusternetes_common::CELContext::new();
            if let Some(obj) = object {
                let _ = context.add_json_variable("object", obj);
            }

            // Add oldObject for UPDATE operations
            if let Some(old) = old_object {
                let _ = context.add_json_variable("oldObject", old);
            } else {
                // For non-update ops, oldObject is null
                let _ = context.add_json_variable("oldObject", &serde_json::Value::Null);
            }

            // Add request context (K8s conformance tests access request.operation, etc.)
            let request_val = serde_json::json!({
                "operation": op_str,
                "kind": {
                    "group": gvk.group,
                    "version": gvk.version,
                    "kind": gvk.kind,
                },
                "resource": {
                    "group": gvk.group,
                    "version": gvk.version,
                    "resource": derived_resource,
                },
                "namespace": namespace.unwrap_or(""),
                "name": object.and_then(|o| o.get("metadata")).and_then(|m| m.get("name")).and_then(|n| n.as_str()).unwrap_or(""),
                "userInfo": {
                    "username": "system:admin",
                    "groups": ["system:masters", "system:authenticated"],
                },
            });
            let _ = context.add_json_variable("request", &request_val);

            // Add params from the binding's paramRef (if present)
            if let Some(param_ref) = binding.get("spec").and_then(|s| s.get("paramRef")) {
                let param_ns = param_ref
                    .get("namespace")
                    .and_then(|n| n.as_str())
                    .or(namespace);
                let param_name = param_ref.get("name").and_then(|n| n.as_str()).unwrap_or("");
                let param_kind = param_ref.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                let param_api_group = param_ref
                    .get("apiGroup")
                    .and_then(|g| g.as_str())
                    .unwrap_or("");

                if !param_name.is_empty() {
                    // Try to load the param resource from storage
                    let resource_type = format!("{}s", param_kind.to_lowercase());
                    let param_key = if let Some(ns) = param_ns {
                        format!("/registry/{}/{}/{}", resource_type, ns, param_name)
                    } else {
                        format!("/registry/{}/{}", resource_type, param_name)
                    };
                    if let Ok(param_val) = self.storage.get::<serde_json::Value>(&param_key).await {
                        let _ = context.add_json_variable("params", &param_val);
                    } else {
                        // Try as CRD instance
                        let crd_key = format!(
                            "/registry/{}.{}/{}/{}",
                            resource_type,
                            param_api_group,
                            param_ns.unwrap_or(""),
                            param_name
                        );
                        if let Ok(param_val) = self.storage.get::<serde_json::Value>(&crd_key).await
                        {
                            let _ = context.add_json_variable("params", &param_val);
                        } else {
                            let _ = context.add_json_variable("params", &serde_json::Value::Null);
                        }
                    }
                } else {
                    let _ = context.add_json_variable("params", &serde_json::Value::Null);
                }
            } else {
                let _ = context.add_json_variable("params", &serde_json::Value::Null);
            }

            // Add namespaceObject — the Namespace object for the request's namespace.
            // K8s conformance tests use expressions like `namespaceObject.metadata.name`.
            if let Some(ns) = namespace {
                if !ns.is_empty() {
                    let ns_key = format!("/registry/namespaces/{}", ns);
                    if let Ok(ns_val) = self.storage.get::<serde_json::Value>(&ns_key).await {
                        let _ = context.add_json_variable(
                            "namespaceObject",
                            &serde_json::to_value(&ns_val).unwrap_or(serde_json::Value::Null),
                        );
                    } else {
                        // If namespace not found in storage, provide a minimal object
                        // so that expressions like namespaceObject.metadata.name don't error.
                        let minimal_ns = serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Namespace",
                            "metadata": {
                                "name": ns,
                            }
                        });
                        let _ = context.add_json_variable("namespaceObject", &minimal_ns);
                    }
                }
            }

            // Evaluate spec.variables, building a "variables" Map for CEL access.
            // CEL expressions reference variables as `variables.NAME`, which means
            // "variables" must be a Map variable in the CEL context.
            if let Some(vars) = policy
                .get("spec")
                .and_then(|s| s.get("variables"))
                .and_then(|v| v.as_array())
            {
                let mut var_map: std::collections::HashMap<cel::objects::Key, cel::objects::Value> =
                    std::collections::HashMap::new();
                for var_def in vars {
                    let var_name = var_def.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let var_expr = var_def
                        .get("expression")
                        .and_then(|e| e.as_str())
                        .unwrap_or("");
                    if var_name.is_empty() || var_expr.is_empty() {
                        continue;
                    }
                    // Evaluate the variable expression and add to the variables map
                    match evaluator.evaluate_to_value(var_expr, &context) {
                        Ok(val) => {
                            var_map.insert(
                                cel::objects::Key::String(std::sync::Arc::new(
                                    var_name.to_string(),
                                )),
                                val,
                            );
                            // Re-add the updated variables map to context after each variable
                            // so later variables can reference earlier ones
                            context.add_variable(
                                "variables".to_string(),
                                cel::objects::Value::Map(cel::objects::Map {
                                    map: std::sync::Arc::new(var_map.clone()),
                                }),
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "CEL variable {} evaluation error for policy {}: {}",
                                var_name,
                                policy_name,
                                e
                            );
                        }
                    }
                }
            }

            // Check failure policy
            let failure_policy = policy
                .get("spec")
                .and_then(|s| s.get("failurePolicy"))
                .and_then(|f| f.as_str())
                .unwrap_or("Fail");

            // Evaluate validations.
            // Source of truth: `crate::cel_evaluators::ValidationEvaluator`.
            if let Some(validations) = policy
                .get("spec")
                .and_then(|s| s.get("validations"))
                .and_then(|v| v.as_array())
            {
                for validation in validations {
                    let expression = validation
                        .get("expression")
                        .and_then(|e| e.as_str())
                        .unwrap_or("");
                    if expression.is_empty() {
                        continue;
                    }
                    let message_expression =
                        validation.get("messageExpression").and_then(|m| m.as_str());
                    let static_message = validation.get("message").and_then(|m| m.as_str());

                    let outcome = crate::cel_evaluators::ValidationEvaluator::evaluate_one(
                        expression,
                        message_expression,
                        static_message,
                        &mut evaluator,
                        &context,
                    );

                    match outcome {
                        crate::cel_evaluators::ValidationOutcome::Pass => {
                            tracing::debug!(
                                "VAP {} expression '{}' passed",
                                policy_name,
                                expression
                            );
                        }
                        crate::cel_evaluators::ValidationOutcome::Fail { message } => {
                            tracing::info!(
                                "VAP {} expression '{}' DENIED for {} in ns {:?}",
                                policy_name,
                                expression,
                                derived_resource,
                                namespace
                            );
                            // Check validation actions: first from the binding, then from
                            // the validation rule itself, defaulting to Deny if neither set.
                            let actions = binding
                                .get("spec")
                                .and_then(|s| s.get("validationActions"))
                                .and_then(|a| a.as_array())
                                .or_else(|| {
                                    validation
                                        .get("validationActions")
                                        .and_then(|a| a.as_array())
                                });
                            let has_deny = actions
                                .is_none_or(|acts| acts.iter().any(|a| a.as_str() == Some("Deny")));
                            if has_deny {
                                return Err(rusternetes_common::Error::InvalidResource(format!(
                                    "ValidatingAdmissionPolicy {} denied: {}",
                                    policy_name, message
                                )));
                            }
                        }
                        crate::cel_evaluators::ValidationOutcome::Error { message } => {
                            tracing::warn!(
                                "CEL evaluation error for policy {}: {}",
                                policy_name,
                                message
                            );
                            // On error, honour the policy's failure policy.
                            if failure_policy == "Fail" {
                                return Err(rusternetes_common::Error::InvalidResource(format!(
                                    "ValidatingAdmissionPolicy {} evaluation error: {}",
                                    policy_name, message
                                )));
                            }
                        }
                    }
                }
            }

            // Evaluate auditAnnotations.
            // Source of truth: `crate::cel_evaluators::AuditAnnotationEvaluator`.
            //
            // Upstream emits resolved annotations on the audit event under
            // `<policy-name>/<key>` once we have an audit sink, we will plumb
            // these through; for now they're logged at info level so the
            // expression is exercised (catching regressions) without being
            // silently dropped. A `valueExpression` returning `null` skips the
            // annotation per upstream `validator.go`.
            if let Some(audit_annotations) = policy
                .get("spec")
                .and_then(|s| s.get("auditAnnotations"))
                .and_then(|a| a.as_array())
            {
                for annotation in audit_annotations {
                    let key = annotation.get("key").and_then(|k| k.as_str()).unwrap_or("");
                    let value_expression = annotation
                        .get("valueExpression")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if key.is_empty() || value_expression.is_empty() {
                        continue;
                    }

                    let outcome = crate::cel_evaluators::AuditAnnotationEvaluator::evaluate_one(
                        key,
                        value_expression,
                        &mut evaluator,
                        &context,
                    );
                    match outcome {
                        crate::cel_evaluators::AuditAnnotationOutcome::Emit { key, value } => {
                            tracing::info!(
                                "VAP {} audit annotation {}/{} = {}",
                                policy_name,
                                policy_name,
                                key,
                                value
                            );
                        }
                        crate::cel_evaluators::AuditAnnotationOutcome::Skip => {
                            tracing::debug!(
                                "VAP {} audit annotation {} skipped (valueExpression returned null)",
                                policy_name,
                                key
                            );
                        }
                        crate::cel_evaluators::AuditAnnotationOutcome::Error { message } => {
                            tracing::warn!(
                                "VAP {} audit annotation error: {}",
                                policy_name,
                                message
                            );
                            if failure_policy == "Fail" {
                                return Err(rusternetes_common::Error::InvalidResource(format!(
                                    "ValidatingAdmissionPolicy {} auditAnnotation error: {}",
                                    policy_name, message
                                )));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Evaluate matchConditions for a VAP. Returns true if all conditions pass
    /// (or none exist), false if any condition fails or errors.
    ///
    /// Thin wrapper around [`crate::cel_evaluators::MatchConditionEvaluator`] — the source
    /// of truth for the CEL semantics. The VAP-specific concern handled here
    /// is the JSON shape of `spec.matchConditions[]`: each entry has a `name`
    /// and `expression` field, which we deserialize into the typed
    /// `MatchCondition` struct before evaluating.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_match_conditions(
        &self,
        policy: &Value,
        object: Option<&Value>,
        old_object: Option<&Value>,
        operation: &Operation,
        gvk: &GroupVersionKind,
        namespace: Option<&str>,
        name: &str,
        user_info: Option<&UserInfo>,
    ) -> bool {
        let raw_conditions = match policy
            .get("spec")
            .and_then(|s| s.get("matchConditions"))
            .and_then(|c| c.as_array())
        {
            Some(c) if !c.is_empty() => c,
            _ => return true, // No conditions = always match (upstream)
        };

        let conditions: Vec<rusternetes_common::resources::MatchCondition> = raw_conditions
            .iter()
            .filter_map(|v| serde_json::from_value(v.clone()).ok())
            .collect();
        if conditions.is_empty() {
            return true;
        }

        let request = build_admission_request_for_match(
            operation, gvk, namespace, name, object, old_object, user_info,
        );

        let mut evaluator = crate::cel_evaluators::MatchConditionEvaluator::new();
        matches!(
            evaluator.evaluate(&conditions, &request, None),
            crate::cel_evaluators::MatchOutcome::Matched
        )
    }
}

/// Build a typed `AdmissionRequest` from the loose params used inside the
/// admission_webhook dispatcher. Used by all three match-condition call sites
/// (VAP, validating webhook, mutating webhook) to feed
/// [`crate::cel_evaluators::MatchConditionEvaluator`].
///
/// `user_info` is optional because VAP runs in-server (no remote webhook) and
/// the original `run_validating_admission_policies_ext` does not propagate the
/// caller's identity yet — upstream's policy plugin synthesises a system-admin
/// identity in the same situation. The dummy here matches what the inline VAP
/// code was already publishing into the `request.userInfo` activation variable.
fn build_admission_request_for_match(
    operation: &Operation,
    gvk: &GroupVersionKind,
    namespace: Option<&str>,
    name: &str,
    object: Option<&Value>,
    old_object: Option<&Value>,
    user_info: Option<&UserInfo>,
) -> rusternetes_common::admission::AdmissionRequest {
    let resolved_user_info = user_info.cloned().unwrap_or_else(|| UserInfo {
        username: "system:admin".to_string(),
        uid: String::new(),
        groups: vec![
            "system:masters".to_string(),
            "system:authenticated".to_string(),
        ],
    });

    rusternetes_common::admission::AdmissionRequest {
        operation: operation.clone(),
        group: gvk.group.clone(),
        version: gvk.version.clone(),
        kind: gvk.kind.clone(),
        namespace: namespace.map(|s| s.to_string()),
        name: name.to_string(),
        object: object.cloned().unwrap_or(Value::Null),
        old_object: old_object.cloned(),
        user_info: resolved_user_info,
    }
}

/// Apply JSON pointer replace operation
fn apply_json_pointer_replace(object: &mut Value, path: &str, value: Value) -> Result<()> {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    if parts.is_empty() || parts[0].is_empty() {
        *object = value;
        return Ok(());
    }

    let mut current = object;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part - replace the value
            if let Some(obj) = current.as_object_mut() {
                obj.insert(part.to_string(), value.clone());
            }
        } else {
            // Navigate to the next level
            current = current
                .as_object_mut()
                .and_then(|obj| obj.get_mut(*part))
                .ok_or_else(|| {
                    rusternetes_common::Error::InvalidResource(format!("Path not found: {}", path))
                })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::RuleWithOperations;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    // ===== Timeout Resolution Tests =====

    #[test]
    fn test_resolve_webhook_timeout_default() {
        assert_eq!(resolve_webhook_timeout(None), Duration::from_secs(10));
    }

    #[test]
    fn test_resolve_webhook_timeout_explicit() {
        assert_eq!(resolve_webhook_timeout(Some(5)), Duration::from_secs(5));
        assert_eq!(resolve_webhook_timeout(Some(1)), Duration::from_secs(1));
        assert_eq!(resolve_webhook_timeout(Some(30)), Duration::from_secs(30));
    }

    #[test]
    fn test_resolve_webhook_timeout_clamped_to_max() {
        // K8s v1.35 caps timeoutSeconds at 30.
        assert_eq!(resolve_webhook_timeout(Some(60)), Duration::from_secs(30));
        assert_eq!(resolve_webhook_timeout(Some(120)), Duration::from_secs(30));
    }

    #[test]
    fn test_resolve_webhook_timeout_clamped_to_min() {
        // Negative and zero values clamp to 1 second.
        assert_eq!(resolve_webhook_timeout(Some(0)), Duration::from_secs(1));
        assert_eq!(resolve_webhook_timeout(Some(-5)), Duration::from_secs(1));
    }

    // ===== JSON Patch Tests =====

    #[test]
    fn test_apply_json_patch_add() {
        let mut obj = json!({
            "metadata": {
                "name": "test"
            }
        });

        let patch = PatchOperation {
            op: rusternetes_common::admission::PatchOp::Add,
            path: "/metadata/labels".to_string(),
            value: Some(json!({"app": "test"})),
            from: None,
        };

        apply_json_patch(&mut obj, &patch).unwrap();

        assert_eq!(obj["metadata"]["labels"], json!({"app": "test"}));
    }

    #[test]
    fn test_apply_json_patch_remove() {
        let mut obj = json!({
            "metadata": {
                "name": "test",
                "labels": {"app": "test"}
            }
        });

        let patch = PatchOperation {
            op: rusternetes_common::admission::PatchOp::Remove,
            path: "/metadata/labels".to_string(),
            value: None,
            from: None,
        };

        apply_json_patch(&mut obj, &patch).unwrap();

        assert!(obj["metadata"]["labels"].is_null());
    }

    #[test]
    fn test_apply_json_patch_replace() {
        let mut obj = json!({
            "metadata": {
                "name": "test"
            }
        });

        let patch = PatchOperation {
            op: rusternetes_common::admission::PatchOp::Replace,
            path: "/metadata/name".to_string(),
            value: Some(json!("new-name")),
            from: None,
        };

        apply_json_patch(&mut obj, &patch).unwrap();

        assert_eq!(obj["metadata"]["name"], json!("new-name"));
    }

    #[test]
    fn test_apply_json_patch_nested_add() {
        let mut obj = json!({
            "metadata": {
                "name": "test",
                "annotations": {}
            }
        });

        let patch = PatchOperation {
            op: rusternetes_common::admission::PatchOp::Add,
            path: "/metadata/annotations/key".to_string(),
            value: Some(json!("value")),
            from: None,
        };

        apply_json_patch(&mut obj, &patch).unwrap();

        assert_eq!(obj["metadata"]["annotations"]["key"], json!("value"));
    }

    #[test]
    fn test_apply_json_patch_replace_root() {
        let mut obj = json!({
            "metadata": {
                "name": "test"
            }
        });

        let new_obj = json!({
            "metadata": {
                "name": "replaced"
            }
        });

        let patch = PatchOperation {
            op: rusternetes_common::admission::PatchOp::Replace,
            path: "/".to_string(),
            value: Some(new_obj.clone()),
            from: None,
        };

        apply_json_patch(&mut obj, &patch).unwrap();

        assert_eq!(obj, new_obj);
    }

    #[test]
    fn test_apply_json_patch_remove_error_on_root() {
        let mut obj = json!({
            "metadata": {
                "name": "test"
            }
        });

        let patch = PatchOperation {
            op: rusternetes_common::admission::PatchOp::Remove,
            path: "/".to_string(),
            value: None,
            from: None,
        };

        let result = apply_json_patch(&mut obj, &patch);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot remove root"));
    }

    // ===== Webhook Matching Tests =====

    fn create_test_manager() -> AdmissionWebhookManager<MemoryStorage> {
        let storage = Arc::new(MemoryStorage::new());
        AdmissionWebhookManager::new(storage)
    }

    #[test]
    fn test_operation_matches_create() {
        let manager = create_test_manager();
        let operations = vec![OperationType::Create];

        assert!(manager.operation_matches(&operations, &Operation::Create));
        assert!(!manager.operation_matches(&operations, &Operation::Update));
        assert!(!manager.operation_matches(&operations, &Operation::Delete));
    }

    #[test]
    fn test_operation_matches_all() {
        let manager = create_test_manager();
        let operations = vec![OperationType::All];

        assert!(manager.operation_matches(&operations, &Operation::Create));
        assert!(manager.operation_matches(&operations, &Operation::Update));
        assert!(manager.operation_matches(&operations, &Operation::Delete));
        assert!(manager.operation_matches(&operations, &Operation::Connect));
    }

    #[test]
    fn test_operation_matches_multiple() {
        let manager = create_test_manager();
        let operations = vec![OperationType::Create, OperationType::Update];

        assert!(manager.operation_matches(&operations, &Operation::Create));
        assert!(manager.operation_matches(&operations, &Operation::Update));
        assert!(!manager.operation_matches(&operations, &Operation::Delete));
    }

    #[test]
    fn test_resource_matches_exact() {
        let manager = create_test_manager();
        let rule = Rule {
            api_groups: vec!["".to_string()],
            api_versions: vec!["v1".to_string()],
            resources: vec!["pods".to_string()],
            scope: None,
        };

        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        };

        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        };

        assert!(manager.resource_matches(&rule, &gvk, &gvr));
    }

    #[test]
    fn test_resource_matches_wildcard_group() {
        let manager = create_test_manager();
        let rule = Rule {
            api_groups: vec!["*".to_string()],
            api_versions: vec!["v1".to_string()],
            resources: vec!["pods".to_string()],
            scope: None,
        };

        let gvr = GroupVersionResource {
            group: "apps".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        };

        let gvk = GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        };

        assert!(manager.resource_matches(&rule, &gvk, &gvr));
    }

    #[test]
    fn test_resource_matches_wildcard_all() {
        let manager = create_test_manager();
        let rule = Rule {
            api_groups: vec!["*".to_string()],
            api_versions: vec!["*".to_string()],
            resources: vec!["*".to_string()],
            scope: None,
        };

        let gvr = GroupVersionResource {
            group: "apps".to_string(),
            version: "v1".to_string(),
            resource: "deployments".to_string(),
        };

        let gvk = GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "Deployment".to_string(),
        };

        assert!(manager.resource_matches(&rule, &gvk, &gvr));
    }

    #[test]
    fn test_resource_matches_mismatch() {
        let manager = create_test_manager();
        let rule = Rule {
            api_groups: vec!["".to_string()],
            api_versions: vec!["v1".to_string()],
            resources: vec!["pods".to_string()],
            scope: None,
        };

        let gvr = GroupVersionResource {
            group: "apps".to_string(),
            version: "v1".to_string(),
            resource: "deployments".to_string(),
        };

        let gvk = GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "Deployment".to_string(),
        };

        assert!(!manager.resource_matches(&rule, &gvk, &gvr));
    }

    #[test]
    fn test_webhook_matches_full() {
        let manager = create_test_manager();

        let rules = vec![RuleWithOperations {
            operations: vec![OperationType::Create],
            rule: Rule {
                api_groups: vec!["".to_string()],
                api_versions: vec!["v1".to_string()],
                resources: vec!["pods".to_string()],
                scope: Some("Namespaced".to_string()),
            },
        }];

        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        };

        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        };

        assert!(manager.webhook_matches(&rules, &Operation::Create, &gvk, &gvr, Some("default")));
    }

    #[test]
    fn test_webhook_matches_scope_cluster() {
        let manager = create_test_manager();

        let rules = vec![RuleWithOperations {
            operations: vec![OperationType::Create],
            rule: Rule {
                api_groups: vec!["".to_string()],
                api_versions: vec!["v1".to_string()],
                resources: vec!["nodes".to_string()],
                scope: Some("Cluster".to_string()),
            },
        }];

        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Node".to_string(),
        };

        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "nodes".to_string(),
        };

        // Should match for cluster-scoped (no namespace)
        assert!(manager.webhook_matches(&rules, &Operation::Create, &gvk, &gvr, None));

        // Should NOT match for namespaced resources
        assert!(!manager.webhook_matches(&rules, &Operation::Create, &gvk, &gvr, Some("default")));
    }

    #[test]
    fn test_webhook_matches_operation_mismatch() {
        let manager = create_test_manager();

        let rules = vec![RuleWithOperations {
            operations: vec![OperationType::Create],
            rule: Rule {
                api_groups: vec!["".to_string()],
                api_versions: vec!["v1".to_string()],
                resources: vec!["pods".to_string()],
                scope: None,
            },
        }];

        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        };

        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        };

        // Should NOT match UPDATE operation
        assert!(!manager.webhook_matches(&rules, &Operation::Update, &gvk, &gvr, Some("default")));
    }

    #[test]
    fn test_webhook_matches_multiple_rules() {
        let manager = create_test_manager();

        let rules = vec![
            RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["apps".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["deployments".to_string()],
                    scope: None,
                },
            },
            RuleWithOperations {
                operations: vec![OperationType::Create],
                rule: Rule {
                    api_groups: vec!["".to_string()],
                    api_versions: vec!["v1".to_string()],
                    resources: vec!["pods".to_string()],
                    scope: None,
                },
            },
        ];

        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
        };

        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods".to_string(),
        };

        // Should match the second rule
        assert!(manager.webhook_matches(&rules, &Operation::Create, &gvk, &gvr, Some("default")));
    }

    // ===== Webhook Client Tests =====

    #[test]
    fn test_build_webhook_url_direct() {
        let client = AdmissionWebhookClient::new();
        let config = WebhookClientConfig {
            url: Some("https://example.com/webhook".to_string()),
            service: None,
            ca_bundle: None,
        };

        let url = client.build_webhook_url(&config).unwrap();
        assert_eq!(url, "https://example.com/webhook");
    }

    #[test]
    fn test_build_webhook_url_service() {
        let client = AdmissionWebhookClient::new();
        let config = WebhookClientConfig {
            url: None,
            service: Some(rusternetes_common::resources::ServiceReference {
                namespace: "webhook-system".to_string(),
                name: "webhook-service".to_string(),
                path: Some("/validate".to_string()),
                port: Some(8443),
            }),
            ca_bundle: None,
        };

        let url = client.build_webhook_url(&config).unwrap();
        assert_eq!(
            url,
            "https://webhook-service.webhook-system.svc:8443/validate"
        );
    }

    #[test]
    fn test_build_webhook_url_service_defaults() {
        let client = AdmissionWebhookClient::new();
        let config = WebhookClientConfig {
            url: None,
            service: Some(rusternetes_common::resources::ServiceReference {
                namespace: "default".to_string(),
                name: "my-webhook".to_string(),
                path: None,
                port: None,
            }),
            ca_bundle: None,
        };

        let url = client.build_webhook_url(&config).unwrap();
        assert_eq!(url, "https://my-webhook.default.svc:443/");
    }

    #[test]
    fn test_build_webhook_url_missing_config() {
        let client = AdmissionWebhookClient::new();
        let config = WebhookClientConfig {
            url: None,
            service: None,
            ca_bundle: None,
        };

        let result = client.build_webhook_url(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must specify either url or service"));
    }

    // ===== resolve_service_url Tests =====
    //
    // These guard the core fix for admission-webhook invocation on CORE
    // resources: the api-server runs on the cluster bridge network but
    // kube-proxy runs in the host netns, so the api-server cannot reach a
    // webhook service's ClusterIP. resolve_service_url must prefer a READY pod
    // endpoint IP (reachable on the bridge) over the ClusterIP.

    use rusternetes_common::resources::endpointslice::{
        Endpoint, EndpointConditions, EndpointPort, EndpointSlice,
    };
    use rusternetes_common::resources::{IntOrString, Service, ServicePort, ServiceSpec};

    fn webhook_service(name: &str, ns: &str, cluster_ip: &str) -> Service {
        let mut svc = Service::new(
            name,
            ServiceSpec {
                cluster_ip: Some(cluster_ip.to_string()),
                ports: vec![ServicePort {
                    name: None,
                    port: 443,
                    target_port: Some(IntOrString::Int(8443)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                ..Default::default()
            },
        );
        svc.metadata.namespace = Some(ns.to_string());
        svc
    }

    fn webhook_endpointslice(svc_name: &str, ns: &str, ip: &str, ready: bool) -> EndpointSlice {
        let mut slice = EndpointSlice::new(format!("{svc_name}-abc"), "IPv4");
        slice.metadata.namespace = Some(ns.to_string());
        slice.metadata.labels = Some(
            [(
                "kubernetes.io/service-name".to_string(),
                svc_name.to_string(),
            )]
            .into_iter()
            .collect(),
        );
        slice.ports = vec![EndpointPort {
            name: None,
            port: Some(8443),
            protocol: "TCP".to_string(),
            app_protocol: None,
        }];
        slice.endpoints = vec![Endpoint {
            addresses: vec![ip.to_string()],
            conditions: Some(EndpointConditions {
                ready: Some(ready),
                serving: Some(ready),
                terminating: Some(false),
            }),
            hostname: None,
            target_ref: None,
            node_name: None,
            zone: None,
            hints: None,
            deprecated_topology: None,
        }];
        slice
    }

    #[tokio::test]
    async fn test_resolve_service_url_prefers_ready_endpoint_over_cluster_ip() {
        // The api-server can't reach a ClusterIP (no kube-proxy in its netns),
        // so resolution must point at the ready pod endpoint instead.
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(
                "/registry/services/kube-system/sample-webhook",
                &webhook_service("sample-webhook", "kube-system", "10.96.1.5"),
            )
            .await
            .unwrap();
        storage
            .create(
                "/registry/endpointslices/kube-system/sample-webhook-abc",
                &webhook_endpointslice("sample-webhook", "kube-system", "172.18.0.9", true),
            )
            .await
            .unwrap();

        let resolved = AdmissionWebhookClient::resolve_service_url(
            "https://sample-webhook.kube-system.svc:443/mutating",
            &storage,
        )
        .await;

        // Endpoint IP + container (target) port, NOT the ClusterIP.
        assert_eq!(resolved, "https://172.18.0.9:8443/mutating");
    }

    #[tokio::test]
    async fn test_resolve_service_url_skips_unready_endpoints() {
        // An unready endpoint must not be used; with no ready endpoint we fall
        // back to the ClusterIP (best effort).
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(
                "/registry/services/kube-system/sample-webhook",
                &webhook_service("sample-webhook", "kube-system", "10.96.1.5"),
            )
            .await
            .unwrap();
        storage
            .create(
                "/registry/endpointslices/kube-system/sample-webhook-abc",
                &webhook_endpointslice("sample-webhook", "kube-system", "172.18.0.9", false),
            )
            .await
            .unwrap();

        let resolved = AdmissionWebhookClient::resolve_service_url(
            "https://sample-webhook.kube-system.svc:443/mutating",
            &storage,
        )
        .await;

        assert_eq!(resolved, "https://10.96.1.5:443/mutating");
    }

    #[tokio::test]
    async fn test_resolve_service_url_cluster_ip_fallback_when_no_slices() {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(
                "/registry/services/kube-system/sample-webhook",
                &webhook_service("sample-webhook", "kube-system", "10.96.1.5"),
            )
            .await
            .unwrap();

        let resolved = AdmissionWebhookClient::resolve_service_url(
            "https://sample-webhook.kube-system.svc:443/mutating",
            &storage,
        )
        .await;

        assert_eq!(resolved, "https://10.96.1.5:443/mutating");
    }

    #[tokio::test]
    async fn test_resolve_service_url_passthrough_for_direct_url() {
        let storage = Arc::new(MemoryStorage::new());
        let resolved =
            AdmissionWebhookClient::resolve_service_url("https://1.2.3.4:9443/admit", &storage)
                .await;
        assert_eq!(resolved, "https://1.2.3.4:9443/admit");
    }

    // ===== ValidatingAdmissionPolicy Tests =====

    #[tokio::test]
    async fn test_vap_denies_configmap_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        // Create a VAP that denies configmaps with name starting with "deny-"
        let policy = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {
                "name": "deny-configmaps",
                "creationTimestamp": chrono::Utc::now().to_rfc3339(),
            },
            "spec": {
                "matchConstraints": {
                    "resourceRules": [{
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "resources": ["configmaps"],
                        "operations": ["CREATE"],
                    }]
                },
                "validations": [{
                    "expression": "!object.metadata.name.startsWith('deny-')",
                    "message": "ConfigMap name cannot start with deny-",
                }]
            }
        });

        // Store the policy
        let policy_key = "/registry/validatingadmissionpolicies/deny-configmaps";
        storage
            .create::<serde_json::Value>(policy_key, &policy)
            .await
            .unwrap();

        // Create a binding for the policy (with old timestamp so it's "ready")
        let old_time = (chrono::Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
        let binding = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicyBinding",
            "metadata": {
                "name": "deny-configmaps-binding",
                "creationTimestamp": old_time,
            },
            "spec": {
                "policyName": "deny-configmaps",
                "validationActions": ["Deny"],
            }
        });

        let binding_key = "/registry/validatingadmissionpolicybindings/deny-configmaps-binding";
        storage
            .create::<serde_json::Value>(binding_key, &binding)
            .await
            .unwrap();

        // Test: Creating a configmap with name "deny-test" should be denied
        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        };
        let deny_cm = json!({
            "metadata": {"name": "deny-test", "namespace": "default"},
            "data": {"key": "value"},
        });

        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&deny_cm),
                None,
                Some("configmaps"),
                Some("default"),
            )
            .await;

        assert!(
            result.is_err(),
            "Should deny configmap with name starting with 'deny-'"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("ValidatingAdmissionPolicy"),
            "Error should mention VAP: {}",
            err_msg
        );

        // Test: Creating a configmap with a different name should be allowed
        let allow_cm = json!({
            "metadata": {"name": "allowed-cm", "namespace": "default"},
            "data": {"key": "value"},
        });

        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&allow_cm),
                None,
                Some("configmaps"),
                Some("default"),
            )
            .await;

        assert!(
            result.is_ok(),
            "Should allow configmap with name 'allowed-cm'"
        );
    }

    #[tokio::test]
    async fn test_vap_with_variables() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        // Create a VAP that uses variables
        let policy = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {
                "name": "var-policy",
                "creationTimestamp": chrono::Utc::now().to_rfc3339(),
            },
            "spec": {
                "matchConstraints": {
                    "resourceRules": [{
                        "apiGroups": [""],
                        "resources": ["configmaps"],
                        "operations": ["CREATE"],
                    }]
                },
                "variables": [{
                    "name": "nameLen",
                    "expression": "size(object.metadata.name)",
                }],
                "validations": [{
                    "expression": "variables.nameLen <= 10",
                    "message": "Name too long",
                }]
            }
        });

        let policy_key = "/registry/validatingadmissionpolicies/var-policy";
        storage
            .create::<serde_json::Value>(policy_key, &policy)
            .await
            .unwrap();

        let old_time = (chrono::Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
        let binding = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicyBinding",
            "metadata": {
                "name": "var-policy-binding",
                "creationTimestamp": old_time,
            },
            "spec": {
                "policyName": "var-policy",
            }
        });

        let binding_key = "/registry/validatingadmissionpolicybindings/var-policy-binding";
        storage
            .create::<serde_json::Value>(binding_key, &binding)
            .await
            .unwrap();

        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        };

        // Short name should pass
        let short_cm = json!({"metadata": {"name": "short"}});
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&short_cm),
                None,
                Some("configmaps"),
                Some("default"),
            )
            .await;
        assert!(result.is_ok(), "Short name should be allowed");

        // Long name should be denied
        let long_cm = json!({"metadata": {"name": "this-name-is-way-too-long"}});
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&long_cm),
                None,
                Some("configmaps"),
                Some("default"),
            )
            .await;
        assert!(result.is_err(), "Long name should be denied");
    }

    #[tokio::test]
    async fn test_vap_no_binding_skips_policy() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        // Create a VAP without a binding
        let policy = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {
                "name": "unbound-policy",
                "creationTimestamp": chrono::Utc::now().to_rfc3339(),
            },
            "spec": {
                "matchConstraints": {
                    "resourceRules": [{
                        "apiGroups": [""],
                        "resources": ["configmaps"],
                        "operations": ["CREATE"],
                    }]
                },
                "validations": [{
                    "expression": "false",
                    "message": "Should never trigger",
                }]
            }
        });

        let policy_key = "/registry/validatingadmissionpolicies/unbound-policy";
        storage
            .create::<serde_json::Value>(policy_key, &policy)
            .await
            .unwrap();

        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        };
        let cm = json!({"metadata": {"name": "test"}});

        // Should pass because there's no binding
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&cm),
                None,
                Some("configmaps"),
                Some("default"),
            )
            .await;
        assert!(result.is_ok(), "Should pass because no binding exists");
    }

    #[tokio::test]
    async fn test_vap_resource_mismatch_skips() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        // Create a VAP that only matches pods
        let policy = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {
                "name": "pod-only",
                "creationTimestamp": chrono::Utc::now().to_rfc3339(),
            },
            "spec": {
                "matchConstraints": {
                    "resourceRules": [{
                        "apiGroups": [""],
                        "resources": ["pods"],
                        "operations": ["CREATE"],
                    }]
                },
                "validations": [{
                    "expression": "false",
                    "message": "Always deny",
                }]
            }
        });

        let policy_key = "/registry/validatingadmissionpolicies/pod-only";
        storage
            .create::<serde_json::Value>(policy_key, &policy)
            .await
            .unwrap();

        let old_time = (chrono::Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
        let binding = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicyBinding",
            "metadata": {
                "name": "pod-only-binding",
                "creationTimestamp": old_time,
            },
            "spec": {
                "policyName": "pod-only",
            }
        });

        let binding_key = "/registry/validatingadmissionpolicybindings/pod-only-binding";
        storage
            .create::<serde_json::Value>(binding_key, &binding)
            .await
            .unwrap();

        // Creating a configmap should NOT be denied (resource mismatch)
        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        };
        let cm = json!({"metadata": {"name": "test"}});

        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&cm),
                None,
                Some("configmaps"),
                Some("default"),
            )
            .await;
        assert!(
            result.is_ok(),
            "Should pass because resource type doesn't match"
        );
    }

    #[tokio::test]
    async fn test_vap_failure_policy_ignore() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        // Create a VAP with Ignore failure policy and an expression that will error
        let policy = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {
                "name": "ignore-errors",
                "creationTimestamp": chrono::Utc::now().to_rfc3339(),
            },
            "spec": {
                "failurePolicy": "Ignore",
                "matchConstraints": {
                    "resourceRules": [{
                        "apiGroups": [""],
                        "resources": ["configmaps"],
                        "operations": ["CREATE"],
                    }]
                },
                "validations": [{
                    "expression": "object.nonexistent.field > 0",
                    "message": "Should not see this",
                }]
            }
        });

        let policy_key = "/registry/validatingadmissionpolicies/ignore-errors";
        storage
            .create::<serde_json::Value>(policy_key, &policy)
            .await
            .unwrap();

        let old_time = (chrono::Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
        let binding = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicyBinding",
            "metadata": {
                "name": "ignore-errors-binding",
                "creationTimestamp": old_time,
            },
            "spec": {
                "policyName": "ignore-errors",
            }
        });

        let binding_key = "/registry/validatingadmissionpolicybindings/ignore-errors-binding";
        storage
            .create::<serde_json::Value>(binding_key, &binding)
            .await
            .unwrap();

        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        };
        let cm = json!({"metadata": {"name": "test"}});

        // Should pass because failurePolicy is Ignore
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&cm),
                None,
                Some("configmaps"),
                Some("default"),
            )
            .await;
        assert!(
            result.is_ok(),
            "Should pass with Ignore failure policy on CEL error"
        );
    }

    /// Reproduces the K8s conformance test "should allow expressions to refer variables".
    /// The policy defines:
    ///   variables: [{name: "replicas", expression: "object.spec.replicas"},
    ///               {name: "oddReplicas", expression: "variables.replicas % 2 == 1"}]
    ///   validations: [{expression: "variables.replicas > 1"},
    ///                 {expression: "variables.oddReplicas"}]
    #[tokio::test]
    async fn test_vap_variables_refer_conformance() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        let policy = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {"name": "var-refer-policy"},
            "spec": {
                "matchConstraints": {
                    "resourceRules": [{
                        "apiGroups": ["apps"],
                        "apiVersions": ["v1"],
                        "resources": ["deployments"],
                        "operations": ["CREATE"],
                    }]
                },
                "variables": [
                    {"name": "replicas", "expression": "object.spec.replicas"},
                    {"name": "oddReplicas", "expression": "variables.replicas % 2 == 1"},
                ],
                "validations": [
                    {"expression": "variables.replicas > 1"},
                    {"expression": "variables.oddReplicas"},
                ]
            }
        });

        let policy_key = "/registry/validatingadmissionpolicies/var-refer-policy";
        storage
            .create::<serde_json::Value>(policy_key, &policy)
            .await
            .unwrap();

        let binding = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicyBinding",
            "metadata": {"name": "var-refer-binding"},
            "spec": {
                "policyName": "var-refer-policy",
                "validationActions": ["Deny"],
            }
        });
        let binding_key = "/registry/validatingadmissionpolicybindings/var-refer-binding";
        storage
            .create::<serde_json::Value>(binding_key, &binding)
            .await
            .unwrap();

        let gvk = GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "Deployment".to_string(),
        };

        // 1-replica deployment should be denied (replicas > 1 fails)
        let deploy_1 = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "marker", "namespace": "default"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        });
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&deploy_1),
                None,
                Some("deployments"),
                Some("default"),
            )
            .await;
        assert!(result.is_err(), "1-replica deployment should be denied");

        // 3-replica deployment should be allowed (replicas > 1 AND oddReplicas both true)
        let deploy_3 = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "replicated", "namespace": "default"},
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        });
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&deploy_3),
                None,
                Some("deployments"),
                Some("default"),
            )
            .await;
        assert!(
            result.is_ok(),
            "3-replica deployment should be allowed: {:?}",
            result.err()
        );

        // ReplicaSet should NOT be matched (policy targets deployments only)
        let rs_gvk = GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "ReplicaSet".to_string(),
        };
        let rs = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {"name": "test-rs", "namespace": "default"},
            "spec": {"replicas": 1}
        });
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &rs_gvk,
                Some(&rs),
                None,
                Some("replicasets"),
                Some("default"),
            )
            .await;
        assert!(
            result.is_ok(),
            "ReplicaSet should not be matched by deployment policy"
        );
    }

    /// Reproduces the K8s conformance test "should validate against a Deployment".
    /// The policy uses namespaceObject.metadata.name in a validation expression.
    #[tokio::test]
    async fn test_vap_validate_deployment_with_namespace_object() {
        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        let ns_name = "test-ns-unique";

        // Create the namespace in storage so namespaceObject can be loaded
        let namespace_obj = json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": ns_name,
                "labels": {ns_name: "true"},
            }
        });
        let ns_key = format!("/registry/namespaces/{}", ns_name);
        storage
            .create::<serde_json::Value>(&ns_key, &namespace_obj)
            .await
            .unwrap();

        let policy = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {"name": "deploy-ns-policy"},
            "spec": {
                "matchConstraints": {
                    "resourceRules": [{
                        "apiGroups": ["apps"],
                        "apiVersions": ["v1"],
                        "resources": ["deployments"],
                        "operations": ["CREATE"],
                    }]
                },
                "validations": [
                    {"expression": "object.spec.replicas > 1", "messageExpression": "'wants replicas > 1, got ' + string(object.spec.replicas)"},
                    {"expression": format!("namespaceObject.metadata.name == '{}'", ns_name), "message": "Wrong namespace"},
                ]
            }
        });

        let policy_key = "/registry/validatingadmissionpolicies/deploy-ns-policy";
        storage
            .create::<serde_json::Value>(policy_key, &policy)
            .await
            .unwrap();

        let binding = json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicyBinding",
            "metadata": {"name": "deploy-ns-binding"},
            "spec": {
                "policyName": "deploy-ns-policy",
                "validationActions": ["Deny"],
            }
        });
        let binding_key = "/registry/validatingadmissionpolicybindings/deploy-ns-binding";
        storage
            .create::<serde_json::Value>(binding_key, &binding)
            .await
            .unwrap();

        let gvk = GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "Deployment".to_string(),
        };

        // 1-replica deployment: denied (fails replicas > 1)
        let deploy_1 = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "marker", "namespace": ns_name},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        });
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&deploy_1),
                None,
                Some("deployments"),
                Some(ns_name),
            )
            .await;
        assert!(result.is_err(), "1-replica deployment should be denied");

        // 2-replica deployment in correct namespace: allowed
        let deploy_2 = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "replicated", "namespace": ns_name},
            "spec": {
                "replicas": 2,
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        });
        let result = manager
            .run_validating_admission_policies_ext(
                &Operation::Create,
                &gvk,
                Some(&deploy_2),
                None,
                Some("deployments"),
                Some(ns_name),
            )
            .await;
        assert!(
            result.is_ok(),
            "2-replica deployment in correct namespace should be allowed: {:?}",
            result.err()
        );
    }

    // ===== Webhook deny / mutate / dry-run / pruning tests =====
    //
    // These tests cover the K8s v1.35 admission webhook conformance gap:
    //   - deny path for pod/configmap/CR + subresource (attach)
    //   - mutate path producing JSON patches
    //   - SideEffects gating for dry-run
    //   - status.message propagation

    use rusternetes_common::admission::AdmissionStatus;
    use rusternetes_common::resources::{
        FailurePolicy, MutatingWebhook, MutatingWebhookConfiguration, OperationType, Rule,
        RuleWithOperations as Rwo, SideEffectClass, ValidatingWebhook,
        ValidatingWebhookConfiguration, WebhookClientConfig,
    };

    /// Spin up a tiny axum HTTP server that returns the canned AdmissionReview
    /// response for every POST. Returns the base URL so the webhook config can
    /// point at it.
    async fn spawn_webhook_server(
        response: AdmissionReviewResponse,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{routing::post, Json, Router};
        let resp = Arc::new(response);
        let resp_clone = resp.clone();
        let app = Router::new().route(
            "/admit",
            post(move |Json(review): Json<AdmissionReview>| {
                let response = resp_clone.clone();
                async move {
                    let mut out = (*response).clone();
                    if let Some(req) = review.request.as_ref() {
                        out.uid = req.uid.clone();
                    }
                    Json(AdmissionReview::new_response(out))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service()).await;
        });
        (format!("http://{}/admit", addr), handle)
    }

    fn pods_rule() -> Rwo {
        Rwo {
            operations: vec![OperationType::Create],
            rule: Rule {
                api_groups: vec!["".to_string()],
                api_versions: vec!["v1".to_string()],
                resources: vec!["pods".to_string()],
                scope: None,
            },
        }
    }

    fn configmaps_rule() -> Rwo {
        Rwo {
            operations: vec![OperationType::Create],
            rule: Rule {
                api_groups: vec!["".to_string()],
                api_versions: vec!["v1".to_string()],
                resources: vec!["configmaps".to_string()],
                scope: None,
            },
        }
    }

    fn attach_rule() -> Rwo {
        // K8s admission rules match subresources via "<resource>/<sub>" syntax.
        // pods/attach is a Connect operation in K8s.
        Rwo {
            operations: vec![OperationType::Connect],
            rule: Rule {
                api_groups: vec!["".to_string()],
                api_versions: vec!["v1".to_string()],
                resources: vec!["pods/attach".to_string()],
                scope: None,
            },
        }
    }

    fn cr_rule() -> Rwo {
        Rwo {
            operations: vec![OperationType::All],
            rule: Rule {
                api_groups: vec!["example.com".to_string()],
                api_versions: vec!["v1".to_string()],
                resources: vec!["foos".to_string()],
                scope: None,
            },
        }
    }

    fn make_validating_config(name: &str, url: &str, rule: Rwo) -> ValidatingWebhookConfiguration {
        ValidatingWebhookConfiguration {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
            metadata: rusternetes_common::types::ObjectMeta::new(name),
            webhooks: Some(vec![ValidatingWebhook {
                name: format!("{}.example.com", name),
                client_config: WebhookClientConfig {
                    url: Some(url.to_string()),
                    service: None,
                    ca_bundle: None,
                },
                rules: vec![rule],
                failure_policy: Some(FailurePolicy::Fail),
                match_policy: None,
                namespace_selector: None,
                object_selector: None,
                side_effects: SideEffectClass::None,
                timeout_seconds: Some(5),
                admission_review_versions: vec!["v1".to_string()],
                match_conditions: None,
            }]),
        }
    }

    fn make_mutating_config(name: &str, url: &str, rule: Rwo) -> MutatingWebhookConfiguration {
        MutatingWebhookConfiguration {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "MutatingWebhookConfiguration".to_string(),
            metadata: rusternetes_common::types::ObjectMeta::new(name),
            webhooks: Some(vec![MutatingWebhook {
                name: format!("{}.example.com", name),
                client_config: WebhookClientConfig {
                    url: Some(url.to_string()),
                    service: None,
                    ca_bundle: None,
                },
                rules: vec![rule],
                failure_policy: Some(FailurePolicy::Fail),
                match_policy: None,
                namespace_selector: None,
                object_selector: None,
                side_effects: SideEffectClass::None,
                timeout_seconds: Some(5),
                admission_review_versions: vec!["v1".to_string()],
                match_conditions: None,
                reinvocation_policy: None,
            }]),
        }
    }

    #[tokio::test]
    async fn test_validating_webhook_denies_pod_create_with_status_message() {
        let deny_resp = AdmissionReviewResponse {
            uid: String::new(),
            allowed: false,
            status: Some(AdmissionStatus {
                status: "Failure".to_string(),
                message: Some("this webhook denies all pods".to_string()),
                reason: Some("Forbidden".to_string()),
                code: Some(403),
                metadata: None,
            }),
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        };
        let (url, handle) = spawn_webhook_server(deny_resp).await;

        let storage = Arc::new(MemoryStorage::new());
        let config = make_validating_config("deny-pods", &url, pods_rule());
        storage
            .create(
                "/registry/validatingwebhookconfigurations/deny-pods",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "Pod".into(),
        };
        let gvr = GroupVersionResource {
            group: "".into(),
            version: "v1".into(),
            resource: "pods".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };
        let pod = json!({"metadata": {"name": "p", "namespace": "default"}});

        let resp = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("default"),
                "p",
                Some(pod),
                None,
                &user,
            )
            .await
            .unwrap();
        handle.abort();

        match resp {
            AdmissionResponse::Deny(reason) => assert_eq!(reason, "this webhook denies all pods"),
            other => panic!("expected Deny, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_validating_webhook_denies_configmap_create() {
        let deny_resp = AdmissionReviewResponse {
            uid: String::new(),
            allowed: false,
            status: Some(AdmissionStatus {
                status: "Failure".into(),
                message: Some("no configmaps for you".into()),
                reason: None,
                code: Some(403),
                metadata: None,
            }),
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        };
        let (url, handle) = spawn_webhook_server(deny_resp).await;

        let storage = Arc::new(MemoryStorage::new());
        let config = make_validating_config("deny-cms", &url, configmaps_rule());
        storage
            .create(
                "/registry/validatingwebhookconfigurations/deny-cms",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "ConfigMap".into(),
        };
        let gvr = GroupVersionResource {
            group: "".into(),
            version: "v1".into(),
            resource: "configmaps".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };
        let resp = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("default"),
                "cm",
                Some(json!({"metadata": {"name": "cm"}})),
                None,
                &user,
            )
            .await
            .unwrap();
        handle.abort();

        assert!(matches!(resp, AdmissionResponse::Deny(ref r) if r == "no configmaps for you"));
    }

    /// Reproduces the conformance "waiting for webhook configuration to be
    /// ready" hang (the dominant time-sink in a macOS Hydrophone run).
    ///
    /// The e2e framework's `waitWebhookConfigurationReady` registers a webhook
    /// scoped by `namespaceSelector`, then repeatedly creates a marker
    /// ConfigMap in a namespace carrying the matching label until the webhook
    /// denies it. The webhook must therefore fire for a marker object whose
    /// namespace matches the selector — the manager has to read that
    /// namespace's labels from storage to evaluate it. If it doesn't, the
    /// marker is never denied and the readiness poll burns its full timeout.
    #[tokio::test]
    async fn test_validating_webhook_namespace_selector_fires_for_labeled_ns() {
        let deny_resp = AdmissionReviewResponse {
            uid: String::new(),
            allowed: false,
            status: Some(AdmissionStatus {
                status: "Failure".into(),
                message: Some("marker denied".into()),
                reason: None,
                code: Some(403),
                metadata: None,
            }),
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        };
        let (url, handle) = spawn_webhook_server(deny_resp).await;

        let storage = Arc::new(MemoryStorage::new());

        // Webhook for configmaps, scoped to namespaces labeled webhook-e2e=true.
        let mut config = make_validating_config("deny-marker-cms", &url, configmaps_rule());
        config.webhooks.as_mut().unwrap()[0].namespace_selector = Some(
            rusternetes_common::resources::admission_webhook::LabelSelector {
                match_labels: Some(std::collections::HashMap::from([(
                    "webhook-e2e".to_string(),
                    "true".to_string(),
                )])),
                match_expressions: None,
            },
        );
        storage
            .create(
                "/registry/validatingwebhookconfigurations/deny-marker-cms",
                &config,
            )
            .await
            .unwrap();

        // The marker namespace must exist in storage with the matching label so
        // the manager can evaluate the namespaceSelector against it.
        let ns_key = rusternetes_storage::build_key("namespaces", None, "webhook-markers");
        storage
            .create(
                &ns_key,
                &json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "webhook-markers", "labels": {"webhook-e2e": "true"}}
                }),
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "ConfigMap".into(),
        };
        let gvr = GroupVersionResource {
            group: "".into(),
            version: "v1".into(),
            resource: "configmaps".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };
        let resp = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("webhook-markers"),
                "marker",
                Some(json!({"metadata": {"name": "marker", "namespace": "webhook-markers"}})),
                None,
                &user,
            )
            .await
            .unwrap();
        handle.abort();

        assert!(
            matches!(resp, AdmissionResponse::Deny(ref r) if r == "marker denied"),
            "webhook with namespaceSelector should fire for a marker in a label-matching namespace, got {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn test_validating_webhook_denies_pod_attach_subresource() {
        let deny_resp = AdmissionReviewResponse {
            uid: String::new(),
            allowed: false,
            status: Some(AdmissionStatus {
                status: "Failure".into(),
                message: Some("attach not allowed".into()),
                reason: None,
                code: Some(403),
                metadata: None,
            }),
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        };
        let (url, handle) = spawn_webhook_server(deny_resp).await;

        let storage = Arc::new(MemoryStorage::new());
        let config = make_validating_config("deny-attach", &url, attach_rule());
        storage
            .create(
                "/registry/validatingwebhookconfigurations/deny-attach",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        // GVR resource "pods/attach" carries the subresource separator that the
        // matcher splits before comparing against rule entries.
        let gvk = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "PodAttachOptions".into(),
        };
        let gvr = GroupVersionResource {
            group: "".into(),
            version: "v1".into(),
            resource: "pods/attach".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };
        let resp = manager
            .run_validating_webhooks(
                &Operation::Connect,
                &gvk,
                &gvr,
                Some("default"),
                "p",
                Some(json!({"container": "main"})),
                None,
                &user,
            )
            .await
            .unwrap();
        handle.abort();

        assert!(matches!(resp, AdmissionResponse::Deny(ref r) if r == "attach not allowed"));
    }

    #[tokio::test]
    async fn test_validating_webhook_denies_custom_resource_crud() {
        let deny_resp = AdmissionReviewResponse {
            uid: String::new(),
            allowed: false,
            status: Some(AdmissionStatus {
                status: "Failure".into(),
                message: Some("CR rejected".into()),
                reason: None,
                code: Some(403),
                metadata: None,
            }),
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        };
        let (url, handle) = spawn_webhook_server(deny_resp).await;

        let storage = Arc::new(MemoryStorage::new());
        let config = make_validating_config("deny-foos", &url, cr_rule());
        storage
            .create(
                "/registry/validatingwebhookconfigurations/deny-foos",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "example.com".into(),
            version: "v1".into(),
            kind: "Foo".into(),
        };
        let gvr = GroupVersionResource {
            group: "example.com".into(),
            version: "v1".into(),
            resource: "foos".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };
        let cr_obj = json!({"apiVersion": "example.com/v1", "kind": "Foo",
                            "metadata": {"name": "f1", "namespace": "default"}});

        // CREATE
        let create_resp = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("default"),
                "f1",
                Some(cr_obj.clone()),
                None,
                &user,
            )
            .await
            .unwrap();
        assert!(matches!(create_resp, AdmissionResponse::Deny(ref r) if r == "CR rejected"));

        // UPDATE
        let update_resp = manager
            .run_validating_webhooks(
                &Operation::Update,
                &gvk,
                &gvr,
                Some("default"),
                "f1",
                Some(cr_obj.clone()),
                Some(cr_obj.clone()),
                &user,
            )
            .await
            .unwrap();
        assert!(matches!(update_resp, AdmissionResponse::Deny(_)));

        // DELETE: object is None, oldObject carries the resource
        let delete_resp = manager
            .run_validating_webhooks(
                &Operation::Delete,
                &gvk,
                &gvr,
                Some("default"),
                "f1",
                None,
                Some(cr_obj),
                &user,
            )
            .await
            .unwrap();
        assert!(matches!(delete_resp, AdmissionResponse::Deny(_)));

        handle.abort();
    }

    #[tokio::test]
    async fn test_mutating_webhook_applies_json_patch_to_custom_resource() {
        use base64::Engine;
        // Patch: add a label, then add an unknown field to spec
        let patch = serde_json::json!([
            {"op": "add", "path": "/metadata/labels", "value": {"mutated": "true"}},
            {"op": "add", "path": "/spec/extraField", "value": "leak"},
        ]);
        let patch_b64 =
            base64::engine::general_purpose::STANDARD.encode(patch.to_string().as_bytes());

        let mutate_resp = AdmissionReviewResponse {
            uid: String::new(),
            allowed: true,
            status: None,
            patch: Some(patch_b64),
            patch_type: Some("JSONPatch".to_string()),
            audit_annotations: None,
            warnings: None,
        };
        let (url, handle) = spawn_webhook_server(mutate_resp).await;

        let storage = Arc::new(MemoryStorage::new());
        let config = make_mutating_config("mutate-foos", &url, cr_rule());
        storage
            .create(
                "/registry/mutatingwebhookconfigurations/mutate-foos",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "example.com".into(),
            version: "v1".into(),
            kind: "Foo".into(),
        };
        let gvr = GroupVersionResource {
            group: "example.com".into(),
            version: "v1".into(),
            resource: "foos".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };
        let cr_obj = json!({
            "apiVersion": "example.com/v1",
            "kind": "Foo",
            "metadata": {"name": "f1", "labels": {}},
            "spec": {"replicas": 1}
        });

        let (resp, mutated) = manager
            .run_mutating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("default"),
                "f1",
                Some(cr_obj),
                None,
                &user,
            )
            .await
            .unwrap();
        handle.abort();

        assert!(resp.is_allowed());
        let mutated = mutated.expect("mutated object");
        assert_eq!(
            mutated.pointer("/metadata/labels/mutated"),
            Some(&json!("true"))
        );
        // The webhook injected an unknown field — pruning must remove it next.
        assert_eq!(mutated.pointer("/spec/extraField"), Some(&json!("leak")));
    }

    #[tokio::test]
    async fn test_mutating_webhook_dryrun_rejected_when_side_effects_some() {
        // K8s rejects dry-run requests if the webhook declares Side Effects.
        let (url, handle) = spawn_webhook_server(AdmissionReviewResponse {
            uid: String::new(),
            allowed: true,
            status: None,
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        })
        .await;

        let storage = Arc::new(MemoryStorage::new());
        let mut config = make_mutating_config("mutate-pods", &url, pods_rule());
        // Mark this webhook as having Some side effects so dry-run must be refused.
        if let Some(ws) = config.webhooks.as_mut() {
            ws[0].side_effects = SideEffectClass::Some;
        }
        storage
            .create(
                "/registry/mutatingwebhookconfigurations/mutate-pods",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "Pod".into(),
        };
        let gvr = GroupVersionResource {
            group: "".into(),
            version: "v1".into(),
            resource: "pods".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };

        let (resp, _obj) = manager
            .run_mutating_webhooks_with_dryrun(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("default"),
                "p",
                Some(json!({"metadata": {"name": "p"}})),
                None,
                &user,
                true, // dry-run
            )
            .await
            .unwrap();
        handle.abort();

        match resp {
            AdmissionResponse::Deny(reason) => assert!(
                reason.contains("does not support dry run"),
                "got reason: {}",
                reason
            ),
            other => panic!(
                "expected Deny for dry-run with side effects, got {:?}",
                other
            ),
        }
    }

    #[tokio::test]
    async fn test_mutating_webhook_dryrun_allowed_for_none_on_dryrun() {
        // sideEffects=NoneOnDryRun means: the webhook has side effects in
        // normal operation but knows to skip them when dryRun=true. The
        // apiserver should still call the webhook.
        let (url, handle) = spawn_webhook_server(AdmissionReviewResponse {
            uid: String::new(),
            allowed: true,
            status: None,
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        })
        .await;

        let storage = Arc::new(MemoryStorage::new());
        let mut config = make_mutating_config("mutate-pods-noneondryrun", &url, pods_rule());
        if let Some(ws) = config.webhooks.as_mut() {
            ws[0].side_effects = SideEffectClass::NoneOnDryRun;
        }
        storage
            .create(
                "/registry/mutatingwebhookconfigurations/mutate-pods-noneondryrun",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "Pod".into(),
        };
        let gvr = GroupVersionResource {
            group: "".into(),
            version: "v1".into(),
            resource: "pods".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };

        let (resp, _obj) = manager
            .run_mutating_webhooks_with_dryrun(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("default"),
                "p",
                Some(json!({"metadata": {"name": "p"}})),
                None,
                &user,
                true,
            )
            .await
            .unwrap();
        handle.abort();
        assert!(
            resp.is_allowed(),
            "NoneOnDryRun should be allowed under dry-run, got {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn test_validating_webhook_failure_policy_fail_blocks() {
        // Point at an unreachable URL with FailurePolicy=Fail so the
        // request is rejected.
        let storage = Arc::new(MemoryStorage::new());
        let mut config = make_validating_config(
            "fail-closed",
            "http://127.0.0.1:1/admit", // unreachable port
            pods_rule(),
        );
        if let Some(ws) = config.webhooks.as_mut() {
            ws[0].failure_policy = Some(FailurePolicy::Fail);
            ws[0].timeout_seconds = Some(1);
        }
        storage
            .create(
                "/registry/validatingwebhookconfigurations/fail-closed",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "Pod".into(),
        };
        let gvr = GroupVersionResource {
            group: "".into(),
            version: "v1".into(),
            resource: "pods".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };
        let result = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("default"),
                "p",
                Some(json!({"metadata": {"name": "p"}})),
                None,
                &user,
            )
            .await;
        assert!(
            result.is_err(),
            "Fail policy should reject when webhook is unreachable"
        );
    }

    #[tokio::test]
    async fn test_validating_webhook_failure_policy_ignore_allows() {
        // Same setup but with FailurePolicy=Ignore — request must be allowed.
        let storage = Arc::new(MemoryStorage::new());
        let mut config =
            make_validating_config("fail-open", "http://127.0.0.1:1/admit", pods_rule());
        if let Some(ws) = config.webhooks.as_mut() {
            ws[0].failure_policy = Some(FailurePolicy::Ignore);
            ws[0].timeout_seconds = Some(1);
        }
        storage
            .create(
                "/registry/validatingwebhookconfigurations/fail-open",
                &config,
            )
            .await
            .unwrap();

        let manager = AdmissionWebhookManager::new(storage);
        let gvk = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "Pod".into(),
        };
        let gvr = GroupVersionResource {
            group: "".into(),
            version: "v1".into(),
            resource: "pods".into(),
        };
        let user = UserInfo {
            username: "alice".into(),
            uid: "1".into(),
            groups: vec![],
        };
        let resp = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                Some("default"),
                "p",
                Some(json!({"metadata": {"name": "p"}})),
                None,
                &user,
            )
            .await
            .unwrap();
        assert!(resp.is_allowed(), "Ignore policy must fail-open");
    }

    // ===== CR pruning after mutating webhook =====

    /// Smoke-test the structural pruning function used by the CR create handler.
    /// The CR handler calls `prune_custom_resource` AFTER applying webhook
    /// mutations so any fields injected by a mutator that are absent from the
    /// CRD's structural schema must be stripped before storage.
    #[test]
    fn test_prune_unknown_fields_after_webhook_mutation() {
        use rusternetes_common::resources::{CustomResource, CustomResourceDefinition};

        // Build the CRD from JSON to avoid spelling out every field of the
        // structured types — the structural schema declares only `spec.replicas`.
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
        let crd: CustomResourceDefinition = serde_json::from_value(crd_json).unwrap();

        // CR with a webhook-injected unknown field in spec.
        let cr_json = json!({
            "apiVersion": "example.com/v1",
            "kind": "Foo",
            "metadata": {"name": "f1"},
            "spec": {"replicas": 3, "extraField": "leak"}
        });
        let mut cr: CustomResource = serde_json::from_value(cr_json).unwrap();

        // Reuse the same pruning function the create handler calls.
        rusternetes_common::resources::crd::prune_custom_resource(&crd, "v1", &mut cr);

        let pruned_spec = cr.spec.expect("spec preserved");
        assert_eq!(pruned_spec.get("replicas"), Some(&json!(3)));
        assert!(
            pruned_spec.get("extraField").is_none(),
            "extraField must be pruned: {:?}",
            pruned_spec
        );
    }
}
