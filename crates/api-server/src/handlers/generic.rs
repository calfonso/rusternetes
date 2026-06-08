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
use rusternetes_common::dump::DumpingJson;
use rusternetes_storage::{build_key, build_prefix, Storage};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

// --- APIService handlers ---

pub async fn create_apiservice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    DumpingJson(mut value): DumpingJson<Value>,
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
    DumpingJson(mut value): DumpingJson<Value>,
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
    DumpingJson(mut value): DumpingJson<Value>,
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

pub async fn patch_apiservice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> rusternetes_common::Result<Json<Value>> {
    let attrs = RequestAttributes::new(auth_ctx.user, "patch", "apiservices")
        .with_api_group("apiregistration.k8s.io")
        .with_name(&name);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    // `normalize_content_type_middleware` rewrites the request Content-Type to
    // application/json (so Axum's JSON extractor accepts the body) and stashes
    // the real patch MIME in `x-original-content-type`. Prefer that.
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/merge-patch+json");
    let patch_type = crate::patch::PatchType::from_content_type(content_type)
        .map_err(|e| rusternetes_common::Error::BadRequest(e.to_string()))?;
    let patch: Value = serde_json::from_slice(&body)
        .map_err(|e| rusternetes_common::Error::BadRequest(format!("invalid patch body: {}", e)))?;

    let key = build_key("apiservices", None, &name);
    let original: Value = state.storage.get(&key).await?;
    let mut patched = crate::patch::apply_patch(&original, &patch, patch_type)
        .map_err(|e| rusternetes_common::Error::BadRequest(e.to_string()))?;
    patched["kind"] = Value::String("APIService".to_string());
    patched["apiVersion"] = Value::String("apiregistration.k8s.io/v1".to_string());

    let result: Value = state.storage.update(&key, &patched).await?;
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

/// DELETE on the APIService collection (`deletecollection`), honouring an
/// optional `labelSelector`. Used by clients that clean up APIServices by label
/// (e.g. the aggregator conformance test). Returns a success `Status`.
pub async fn deletecollection_apiservices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> rusternetes_common::Result<Json<Value>> {
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "apiservices")
        .with_api_group("apiregistration.k8s.io");
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let selector = params
        .get("labelSelector")
        .map(|s| s.as_str())
        .unwrap_or("");
    let parsed = rusternetes_common::label_selector::LabelSelector::parse(selector)
        .map_err(|e| rusternetes_common::Error::BadRequest(e.to_string()))?;

    let prefix = build_prefix("apiservices", None);
    let items: Vec<Value> = state.storage.list(&prefix).await.unwrap_or_default();
    for item in &items {
        if !parsed.matches_resource(item) {
            continue;
        }
        if let Some(name) = item.pointer("/metadata/name").and_then(|v| v.as_str()) {
            let key = build_key("apiservices", None, name);
            let _ = state.storage.delete(&key).await;
        }
    }

    Ok(Json(serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Success",
        "details": { "group": "apiregistration.k8s.io", "kind": "apiservices" },
    })))
}

pub async fn list_apiservices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> rusternetes_common::Result<axum::response::Response> {
    // Intercept watch
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
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
    /// The address we actually open the TCP connection to — a backend pod IP
    /// (the api-server cannot reach ClusterIPs).
    pub host: String,
    pub port: u16,
    pub insecure_skip_tls_verify: bool,
    pub ca_bundle: Option<Vec<u8>>,
    /// URL scheme used when forwarding. Always `"https"` in production; tests
    /// may override to `"http"` to drive a plain warp mock backend.
    pub scheme: &'static str,
    /// TLS server name (and request `Host`) to present, e.g.
    /// `<service>.<namespace>.svc`. The aggregated apiserver's serving cert is
    /// issued for this DNS name, not the pod IP, so we verify the cert against
    /// it while connecting to `host` via reqwest's `resolve()`. `None` ⇒ use
    /// `host` directly (e.g. the http test backend).
    pub server_name: Option<String>,
}

/// Look up the APIService registered for `{group}/{version}` and resolve a
/// reachable backend address through the backing Service / Endpoints.
///
/// Returns `Ok(None)` when no APIService is registered, `Err(503)` when the
/// APIService exists but the backing service is unreachable.
/// Resolve a reachable `(ip, port)` for an aggregated APIService backend
/// service. Prefers a ready pod IP (EndpointSlices → Endpoints) because the
/// api-server cannot reach ClusterIPs; falls back to the ClusterIP only if no
/// endpoints exist. `svc_port` is the APIService's `spec.service.port`.
async fn resolve_aggregator_backend_address<S: Storage + Send + Sync>(
    storage: &S,
    svc_ns: &str,
    svc_name: &str,
    svc_port: Option<u16>,
) -> Option<(String, u16)> {
    // Strategy 1: EndpointSlices (preferred — gives a ready pod IP + targetPort).
    let es_prefix = rusternetes_storage::build_prefix("endpointslices", Some(svc_ns));
    let slices: Vec<rusternetes_common::resources::EndpointSlice> =
        storage.list(&es_prefix).await.unwrap_or_default();
    for es in &slices {
        let owns = es
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
            .map(|s| s == svc_name)
            .unwrap_or(false);
        if !owns {
            continue;
        }
        // Proxying straight to a pod IP, so the endpoint targetPort wins over
        // the Service port (which only applies to ClusterIP access).
        let ep_port = es.ports.first().and_then(|p| p.port).map(|p| p as u16);
        for ep in &es.endpoints {
            if ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true) {
                if let Some(addr) = ep.addresses.first() {
                    return Some((addr.clone(), ep_port.or(svc_port).unwrap_or(443)));
                }
            }
        }
    }

    // Strategy 2: legacy Endpoints object.
    let ep_key = rusternetes_storage::build_key("endpoints", Some(svc_ns), svc_name);
    if let Ok(ep) = storage
        .get::<rusternetes_common::resources::Endpoints>(&ep_key)
        .await
    {
        if let Some(addr) = ep
            .subsets
            .iter()
            .flat_map(|s| s.addresses.iter().flatten())
            .next()
        {
            let ep_port = ep
                .subsets
                .iter()
                .flat_map(|s| s.ports.iter().flatten())
                .next()
                .map(|p| p.port);
            return Some((addr.ip.clone(), ep_port.or(svc_port).unwrap_or(443)));
        }
    }

    // Strategy 3: last resort — ClusterIP (only works if the api-server's netns
    // happens to have kube-proxy rules, which it usually does not).
    let svc_key = rusternetes_storage::build_key("services", Some(svc_ns), svc_name);
    if let Ok(svc) = storage
        .get::<rusternetes_common::resources::Service>(&svc_key)
        .await
    {
        if let Some(ip) = svc
            .spec
            .cluster_ip
            .clone()
            .filter(|ip| !ip.is_empty() && ip != "None")
        {
            let port = svc_port
                .or_else(|| svc.spec.ports.first().map(|p| p.port))
                .unwrap_or(443);
            return Some((ip, port));
        }
    }

    None
}

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

    // Resolve a reachable backend address. The api-server runs in its own
    // network namespace WITHOUT kube-proxy iptables, so it cannot reach the
    // backend's ClusterIP — it must talk to a backing pod IP directly (same as
    // the node/service proxy handlers). Prefer EndpointSlices, then the legacy
    // Endpoints object, and only fall back to ClusterIP as a last resort.
    let resolved = resolve_aggregator_backend_address(storage, svc_ns, svc_name, svc_port).await;

    match resolved {
        Some((host, port)) => Ok(Some(AggregatorTarget {
            host,
            port,
            insecure_skip_tls_verify,
            ca_bundle,
            scheme: "https",
            // The serving cert is issued for the service DNS name; verify TLS
            // against it while connecting to the resolved pod IP.
            server_name: Some(format!("{}.{}.svc", svc_name, svc_ns)),
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
    // The URL host (and thus TLS SNI + cert verification name) is the service
    // DNS name when set; we then pin DNS resolution of that name to the backend
    // pod IP via `resolve()`. This lets us connect to a reachable pod IP while
    // still verifying the cert that was issued for `<service>.<ns>.svc`.
    let url_host = target.server_name.as_deref().unwrap_or(&target.host);
    let target_url = format!(
        "{}://{}:{}{}",
        target.scheme, url_host, target.port, path_and_query
    );
    debug!(
        "API aggregation proxy: {} {} -> {} (connect {}:{})",
        method, path_and_query, target_url, target.host, target.port
    );

    let mut client_builder = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none());
    if target.server_name.is_some() {
        if let Ok(ip) = target.host.parse::<std::net::IpAddr>() {
            client_builder =
                client_builder.resolve(url_host, std::net::SocketAddr::new(ip, target.port));
        }
    }
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

/// Build a reqwest client configured to reach an aggregated backend: TLS trust
/// per the APIService (caBundle / insecure), and DNS of the service name pinned
/// to the resolved pod IP (so the cert — issued for the service DNS name — still
/// verifies while we connect to a reachable address).
#[allow(dead_code)]
fn build_aggregator_client(target: &AggregatorTarget) -> Option<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none());
    if let Some(name) = target.server_name.as_deref() {
        if let Ok(ip) = target.host.parse::<std::net::IpAddr>() {
            b = b.resolve(name, std::net::SocketAddr::new(ip, target.port));
        }
    }
    if target.insecure_skip_tls_verify {
        b = b.danger_accept_invalid_certs(true);
    } else if let Some(pem) = target.ca_bundle.as_ref() {
        match reqwest::tls::Certificate::from_pem(pem) {
            Ok(cert) => b = b.add_root_certificate(cert),
            Err(_) => b = b.danger_accept_invalid_certs(true),
        }
    } else {
        b = b.danger_accept_invalid_certs(true);
    }
    b.build().ok()
}

/// Fetch the legacy discovery (`APIResourceList`) for `group/version` from an
/// aggregated backend and convert it to the aggregated-discovery resource
/// entries (`apidiscovery.k8s.io` shape). Used to inline an aggregated group's
/// resources into the `/apis` aggregated-discovery document. Returns `None`
/// when there is no backend, it is unreachable, or it serves nothing.
#[allow(dead_code)]
pub async fn aggregated_discovery_resources(
    state: &Arc<ApiServerState>,
    group: &str,
    version: &str,
) -> Option<Vec<Value>> {
    let target = resolve_aggregator_target(state, group, version)
        .await
        .ok()
        .flatten()?;
    let client = build_aggregator_client(&target)?;
    let url_host = target.server_name.as_deref().unwrap_or(&target.host);
    let url = format!(
        "{}://{}:{}/apis/{}/{}",
        target.scheme, url_host, target.port, group, version
    );
    let resp = client
        .get(&url)
        .header("Accept", "application/json")
        // Identify as the aggregator so the backend authorises discovery.
        .header("X-Remote-User", "system:kube-aggregator")
        .header("X-Remote-Group", "system:masters")
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: Value = resp.json().await.ok()?;
    let resources = body.get("resources").and_then(|v| v.as_array())?;
    let mut out = Vec::new();
    for r in resources {
        let Some(name) = r.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        // Subresources (e.g. "flunders/status") are nested under their parent in
        // aggregated discovery; skip them as standalone entries.
        if name.contains('/') {
            continue;
        }
        let kind = r.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let namespaced = r
            .get("namespaced")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let singular = r.get("singularName").and_then(|v| v.as_str()).unwrap_or("");
        let verbs = r
            .get("verbs")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
        out.push(serde_json::json!({
            "resource": name,
            "responseKind": { "group": group, "version": version, "kind": kind },
            "scope": if namespaced { "Namespaced" } else { "Cluster" },
            "singularResource": singular,
            "verbs": verbs,
        }));
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
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
#[allow(dead_code)]
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
