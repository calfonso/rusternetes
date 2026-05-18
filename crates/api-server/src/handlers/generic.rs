//! Generic CRUD handlers for resource types stored as serde_json::Value.
//! Used for resources we don't have dedicated types for (e.g., APIService).
//!
//! Also home to the API aggregator proxy helpers: looking up an APIService for
//! a `/apis/{group}/{version}` request, resolving the backing service to a
//! reachable host/port, and forwarding the request to that backend while
//! preserving auth/impersonation headers.

use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::authz::{Decision, RequestAttributes};
use rusternetes_storage::{build_key, build_prefix, Storage};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

// --- APIService handlers ---

pub async fn create_apiservice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Json(mut value): Json<Value>,
) -> rusternetes_common::Result<(StatusCode, Json<Value>)> {
    let name = value
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    info!("Creating APIService: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "apiservices")
        .with_api_group("apiregistration.k8s.io");
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    value["kind"] = Value::String("APIService".to_string());
    value["apiVersion"] = Value::String("apiregistration.k8s.io/v1".to_string());
    if value.get("metadata").and_then(|m| m.get("uid")).is_none() {
        value["metadata"]["uid"] = Value::String(uuid::Uuid::new_v4().to_string());
    }
    if value
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .is_none()
    {
        value["metadata"]["creationTimestamp"] = Value::String(chrono::Utc::now().to_rfc3339());
    }

    // Initial status conditions:
    //   - local APIService (no spec.service): Available=True immediately.
    //   - remote APIService (spec.service set): Available=Unknown until the
    //     APIServiceAvailabilityController probes the backing service. This
    //     matches kube-aggregator behaviour and keeps tests deterministic.
    let now = chrono::Utc::now().to_rfc3339();
    let has_service_backend = value.pointer("/spec/service").is_some_and(|v| !v.is_null());
    let (status, reason, message) = if has_service_backend {
        (
            "Unknown",
            "Pending",
            "waiting for APIService controller probe",
        )
    } else {
        ("True", "Local", "Local APIService is always available")
    };
    value["status"] = serde_json::json!({
        "conditions": [{
            "type": "Available",
            "status": status,
            "lastTransitionTime": now,
            "reason": reason,
            "message": message,
        }]
    });

    let key = build_key("apiservices", None, &name);
    let created: Value = state.storage.create(&key, &value).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_apiservice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> rusternetes_common::Result<Json<Value>> {
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "apiservices")
        .with_api_group("apiregistration.k8s.io")
        .with_name(&name);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("apiservices", None, &name);
    let mut value: Value = state.storage.get(&key).await?;
    value["kind"] = Value::String("APIService".to_string());
    value["apiVersion"] = Value::String("apiregistration.k8s.io/v1".to_string());
    Ok(Json(value))
}

pub async fn update_apiservice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Json(mut value): Json<Value>,
) -> rusternetes_common::Result<Json<Value>> {
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "apiservices")
        .with_api_group("apiregistration.k8s.io")
        .with_name(&name);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    value["kind"] = Value::String("APIService".to_string());
    value["apiVersion"] = Value::String("apiregistration.k8s.io/v1".to_string());
    value["metadata"]["name"] = Value::String(name.clone());

    let key = build_key("apiservices", None, &name);
    let result: Value = match state.storage.update(&key, &value).await {
        Ok(v) => v,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &value).await?,
        Err(e) => return Err(e),
    };
    Ok(Json(result))
}

pub async fn update_apiservice_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Json(mut value): Json<Value>,
) -> rusternetes_common::Result<Json<Value>> {
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "apiservices/status")
        .with_api_group("apiregistration.k8s.io")
        .with_name(&name);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    value["kind"] = Value::String("APIService".to_string());
    value["apiVersion"] = Value::String("apiregistration.k8s.io/v1".to_string());

    let key = build_key("apiservices", None, &name);
    let result: Value = match state.storage.update(&key, &value).await {
        Ok(v) => v,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &value).await?,
        Err(e) => return Err(e),
    };
    Ok(Json(result))
}

pub async fn delete_apiservice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> rusternetes_common::Result<Json<Value>> {
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "apiservices")
        .with_api_group("apiregistration.k8s.io")
        .with_name(&name);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("apiservices", None, &name);
    let deleted: Value = state.storage.get(&key).await?;
    state.storage.delete(&key).await?;
    Ok(Json(deleted))
}

pub async fn list_apiservices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> rusternetes_common::Result<axum::response::Response> {
    // Intercept watch
    if params
        .get("watch")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
    {
        let watch_params = crate::handlers::watch::WatchParams {
            resource_version: crate::handlers::watch::normalize_resource_version(
                params.get("resourceVersion").cloned(),
            ),
            timeout_seconds: params
                .get("timeoutSeconds")
                .and_then(|v| v.parse::<u64>().ok()),
            label_selector: params.get("labelSelector").cloned(),
            field_selector: params.get("fieldSelector").cloned(),
            watch: Some(true),
            allow_watch_bookmarks: params
                .get("allowWatchBookmarks")
                .and_then(|v| v.parse::<bool>().ok()),
            send_initial_events: params
                .get("sendInitialEvents")
                .and_then(|v| v.parse::<bool>().ok()),
        };
        return crate::handlers::watch::watch_cluster_scoped_json(
            state,
            auth_ctx,
            "apiservices",
            "apiregistration.k8s.io",
            watch_params,
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "apiservices")
        .with_api_group("apiregistration.k8s.io");
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let prefix = build_prefix("apiservices", None);
    let items: Vec<Value> = state.storage.list(&prefix).await.unwrap_or_default();

    let list = serde_json::json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIServiceList",
        "metadata": { "resourceVersion": match state.storage.current_revision().await { Ok(rev) => rev.to_string(), Err(_) => "1".to_string() } },
        "items": items
    });
    Ok(Json(list).into_response())
}

// --- API aggregator proxy helpers ---

/// Resolved network target for an aggregated APIService.
#[derive(Debug, Clone)]
pub struct AggregatorTarget {
    pub host: String,
    pub port: u16,
    pub insecure_skip_tls_verify: bool,
    pub ca_bundle: Option<Vec<u8>>,
    /// URL scheme used when forwarding. Always `"https"` in production; tests
    /// may override to `"http"` to drive a plain warp mock backend.
    pub scheme: &'static str,
}

/// Look up the APIService registered for `{group}/{version}` and resolve a
/// reachable backend address through the backing Service / Endpoints.
///
/// Returns `Ok(None)` when no APIService is registered, `Err(503)` when the
/// APIService exists but the backing service is unreachable.
pub async fn resolve_aggregator_target(
    state: &Arc<ApiServerState>,
    group: &str,
    version: &str,
) -> Result<Option<AggregatorTarget>, Response> {
    resolve_aggregator_target_with_storage(state.storage.as_ref(), group, version).await
}

/// Storage-only flavour of [`resolve_aggregator_target`] — exposed for
/// integration tests that want to exercise the resolver without spinning up
/// the whole `ApiServerState`.
pub async fn resolve_aggregator_target_with_storage<S: Storage + Send + Sync>(
    storage: &S,
    group: &str,
    version: &str,
) -> Result<Option<AggregatorTarget>, Response> {
    let apiservice_name = format!("{}.{}", version, group);
    let apiservice_key = rusternetes_storage::build_key("apiservices", None, &apiservice_name);
    let Ok(apiservice) = storage.get::<Value>(&apiservice_key).await else {
        return Ok(None);
    };

    let svc_name = apiservice
        .pointer("/spec/service/name")
        .and_then(|v| v.as_str());
    let svc_ns = apiservice
        .pointer("/spec/service/namespace")
        .and_then(|v| v.as_str());
    let (svc_name, svc_ns) = match (svc_name, svc_ns) {
        (Some(n), Some(ns)) => (n, ns),
        _ => return Ok(None), // local APIService (no service backend)
    };

    let insecure_skip_tls_verify = apiservice
        .pointer("/spec/insecureSkipTLSVerify")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let ca_bundle = apiservice
        .pointer("/spec/caBundle")
        .and_then(|v| v.as_str())
        .and_then(decode_ca_bundle);

    let svc_port = apiservice
        .pointer("/spec/service/port")
        .and_then(|v| v.as_i64())
        .map(|p| p as u16);

    let svc_key = rusternetes_storage::build_key("services", Some(svc_ns), svc_name);
    let resolved = if let Ok(svc) = storage
        .get::<rusternetes_common::resources::Service>(&svc_key)
        .await
    {
        let cluster_ip = svc
            .spec
            .cluster_ip
            .clone()
            .filter(|ip| !ip.is_empty() && ip != "None");
        let port = svc_port
            .or_else(|| svc.spec.ports.first().map(|p| p.port))
            .unwrap_or(443u16);
        cluster_ip.map(|ip| (ip, port))
    } else {
        let ep_key = rusternetes_storage::build_key("endpoints", Some(svc_ns), svc_name);
        if let Ok(ep) = storage
            .get::<rusternetes_common::resources::Endpoints>(&ep_key)
            .await
        {
            ep.subsets
                .iter()
                .flat_map(|s| s.addresses.iter().flatten())
                .next()
                .map(|addr| {
                    let port = svc_port
                        .or_else(|| {
                            ep.subsets
                                .iter()
                                .flat_map(|s| s.ports.iter().flatten())
                                .next()
                                .map(|p| p.port)
                        })
                        .unwrap_or(443u16);
                    (addr.ip.clone(), port)
                })
        } else {
            None
        }
    };

    match resolved {
        Some((host, port)) => Ok(Some(AggregatorTarget {
            host,
            port,
            insecure_skip_tls_verify,
            ca_bundle,
            scheme: "https",
        })),
        None => {
            warn!(
                "API aggregation: service {}/{} not available for {}/{}",
                svc_ns, svc_name, group, version
            );
            Err(service_unavailable_response(&format!(
                "no endpoints available for service \"{}/{}\"",
                svc_ns, svc_name
            )))
        }
    }
}

/// Build the set of HTTP headers the aggregator forwards on a proxied request.
///
/// Returns a deterministic (sorted) list of `(name, value)` pairs:
///   * `X-Remote-User`, `X-Remote-Group` (one per group),
///     `X-Remote-Extra-<key>` (one per value) — impersonation identity.
///   * Allow-listed pass-through of `Accept`, `Accept-Encoding`,
///     `Content-Type`, `User-Agent`, `X-Forwarded-*` from the inbound request.
///
/// Hop-by-hop headers and the inbound `Authorization` are intentionally
/// dropped — the backend trusts the X-Remote-* identity, signed via mTLS, not
/// the original client's bearer token. This matches kube-aggregator behaviour.
pub fn build_proxy_headers(
    auth_ctx: &AuthContext,
    request_headers: &HeaderMap,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    out.push(("X-Remote-User".to_string(), auth_ctx.user.username.clone()));
    for group in &auth_ctx.user.groups {
        out.push(("X-Remote-Group".to_string(), group.clone()));
    }
    // Sort extras for deterministic ordering.
    let mut extras: Vec<(&String, &Vec<String>)> = auth_ctx.user.extra.iter().collect();
    extras.sort_by(|a, b| a.0.cmp(b.0));
    for (k, vs) in extras {
        let header_name = format!("X-Remote-Extra-{}", k);
        for v in vs {
            out.push((header_name.clone(), v.clone()));
        }
    }
    for (name, value) in request_headers.iter() {
        let n = name.as_str().to_ascii_lowercase();
        if matches!(
            n.as_str(),
            "accept"
                | "accept-encoding"
                | "content-type"
                | "user-agent"
                | "x-forwarded-for"
                | "x-forwarded-proto"
                | "x-forwarded-host"
        ) {
            if let Ok(s) = value.to_str() {
                out.push((name.as_str().to_string(), s.to_string()));
            }
        }
    }
    out
}

/// Forward a request to an aggregated APIService backend.
///
/// Preserves the request's path and query string. Forwards `Accept`,
/// `Content-Type`, and impersonation headers (`X-Remote-User`, `X-Remote-Group`,
/// `X-Remote-Extra-*`, plus `X-Forwarded-*`) so the backend can authorise the
/// caller. The body is read fully (up to 10 MiB) and sent verbatim.
pub async fn forward_to_aggregator(
    target: &AggregatorTarget,
    auth_ctx: &AuthContext,
    method: Method,
    path_and_query: &str,
    request_headers: &HeaderMap,
    body_bytes: Vec<u8>,
) -> Response {
    let target_url = format!(
        "{}://{}:{}{}",
        target.scheme, target.host, target.port, path_and_query
    );
    debug!(
        "API aggregation proxy: {} {} -> {}",
        method, path_and_query, target_url
    );

    let mut client_builder = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none());
    if target.insecure_skip_tls_verify {
        client_builder = client_builder.danger_accept_invalid_certs(true);
    } else if let Some(ref pem) = target.ca_bundle {
        match reqwest::tls::Certificate::from_pem(pem) {
            Ok(cert) => {
                client_builder = client_builder.add_root_certificate(cert);
            }
            Err(e) => {
                warn!(
                    "APIService caBundle is not valid PEM: {} — falling back to insecure",
                    e
                );
                client_builder = client_builder.danger_accept_invalid_certs(true);
            }
        }
    } else {
        // No caBundle and not marked insecure — kube-aggregator would refuse,
        // but we accept invalid certs to keep dev clusters functional. Real
        // deployments should populate caBundle or set insecureSkipTLSVerify.
        client_builder = client_builder.danger_accept_invalid_certs(true);
    }

    let client = match client_builder.build() {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to build aggregator client: {}", e);
            return service_unavailable_response(&format!("aggregator client error: {}", e));
        }
    };

    let reqwest_method = match method {
        Method::GET => reqwest::Method::GET,
        Method::POST => reqwest::Method::POST,
        Method::PUT => reqwest::Method::PUT,
        Method::DELETE => reqwest::Method::DELETE,
        Method::PATCH => reqwest::Method::PATCH,
        Method::HEAD => reqwest::Method::HEAD,
        Method::OPTIONS => reqwest::Method::OPTIONS,
        _ => reqwest::Method::GET,
    };

    let mut req_builder = client.request(reqwest_method, &target_url);
    for (name, value) in build_proxy_headers(auth_ctx, request_headers) {
        req_builder = req_builder.header(&name, &value);
    }

    if !body_bytes.is_empty() {
        req_builder = req_builder.body(body_bytes);
    }

    match req_builder.send().await {
        Ok(resp) => {
            let status = StatusCode::from_u16(resp.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            // Capture response headers we want to surface back to the client.
            let mut out_content_type: Option<HeaderValue> = None;
            let mut out_etag: Option<HeaderValue> = None;
            for (k, v) in resp.headers() {
                let lname = k.as_str().to_ascii_lowercase();
                if lname == "content-type" {
                    out_content_type = HeaderValue::from_bytes(v.as_bytes()).ok();
                } else if lname == "etag" {
                    out_etag = HeaderValue::from_bytes(v.as_bytes()).ok();
                }
            }
            let body = resp.bytes().await.unwrap_or_default();
            let mut builder = Response::builder().status(status);
            builder = builder.header(
                axum::http::header::CONTENT_TYPE,
                out_content_type.unwrap_or_else(|| HeaderValue::from_static("application/json")),
            );
            if let Some(etag) = out_etag {
                builder = builder.header(axum::http::header::ETAG, etag);
            }
            builder.body(Body::from(body)).unwrap_or_else(|_| {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap()
            })
        }
        Err(e) => {
            warn!("API aggregation proxy error: {}", e);
            service_unavailable_response(&format!("aggregated API server unavailable: {}", e))
        }
    }
}

fn decode_ca_bundle(s: &str) -> Option<Vec<u8>> {
    // caBundle in APIService spec is base64-encoded DER or PEM. Try base64
    // first, then fall back to the raw bytes (already-PEM).
    use base64::Engine;
    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s) {
        return Some(bytes);
    }
    Some(s.as_bytes().to_vec())
}

/// Public test hook for [`decode_ca_bundle`]. Not part of the stable public
/// surface — kept here so integration tests can verify the base64/PEM logic
/// without going through the resolver.
#[doc(hidden)]
#[allow(dead_code)]
pub fn decode_ca_bundle_for_test(s: &str) -> Option<Vec<u8>> {
    decode_ca_bundle(s)
}

fn service_unavailable_response(message: &str) -> Response {
    let status_body = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "message": message,
        "reason": "ServiceUnavailable",
        "code": 503,
    });
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(status_body.to_string()))
        .unwrap()
}

/// Discovery merge: produce APIGroup entries for APIServices whose backing
/// `{group}/{version}` is not one of the built-in groups. Caller is
/// responsible for filtering out built-in groups it already exposes.
pub async fn list_registered_apiservice_groups(state: &Arc<ApiServerState>) -> Vec<Value> {
    list_registered_apiservice_groups_with_storage(state.storage.as_ref()).await
}

/// Storage-only flavour of [`list_registered_apiservice_groups`].
pub async fn list_registered_apiservice_groups_with_storage<S: Storage + Send + Sync>(
    storage: &S,
) -> Vec<Value> {
    let prefix = build_prefix("apiservices", None);
    let items: Vec<Value> = storage.list(&prefix).await.unwrap_or_default();

    let mut by_group: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for item in &items {
        let group = item.pointer("/spec/group").and_then(|v| v.as_str());
        let version = item.pointer("/spec/version").and_then(|v| v.as_str());
        let priority = item
            .pointer("/spec/versionPriority")
            .and_then(|v| v.as_i64())
            .unwrap_or(100) as i32;
        if let (Some(g), Some(v)) = (group, version) {
            by_group
                .entry(g.to_string())
                .or_default()
                .push((v.to_string(), priority));
        }
    }

    let mut out = Vec::new();
    for (group, mut versions) in by_group {
        // Highest priority first; ties keep insertion order (sort_by_key is stable).
        versions.sort_by_key(|v| std::cmp::Reverse(v.1));
        let versions_arr: Vec<Value> = versions
            .iter()
            .map(|(v, _)| {
                serde_json::json!({
                    "groupVersion": format!("{}/{}", group, v),
                    "version": v,
                })
            })
            .collect();
        let preferred = versions_arr
            .first()
            .cloned()
            .unwrap_or(serde_json::json!({}));
        out.push(serde_json::json!({
            "name": group,
            "versions": versions_arr,
            "preferredVersion": preferred,
        }));
    }
    out
}
