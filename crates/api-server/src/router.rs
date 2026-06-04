use crate::{handlers, middleware, state::ApiServerState};
use axum::{
    extract::{Request, State},
    http::{Method, StatusCode, Uri},
    middleware as axum_middleware,
    response::{IntoResponse, Response},
    routing::{any, get, post, put},
    Extension, Router,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::resources::CustomResourceDefinition;
use rusternetes_storage::{build_key, Storage};
use std::path::Path;
use std::sync::Arc;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

/// Fallback handler for custom resources defined by CRDs
/// This handler is called for any route not matched by the static routes
/// It checks if the request matches a CRD and routes to the appropriate handler
async fn custom_resource_fallback(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<crate::middleware::AuthContext>,
    uri: Uri,
    method: Method,
    req: Request,
) -> Result<Response, StatusCode> {
    let path = uri.path();
    debug!("Fallback handler called for path: {}", path);

    // Parse URI to extract custom resource information
    // Expected formats:
    //  - /apis/{group}/{version}/{plural}  (list cluster-scoped)
    //  - /apis/{group}/{version}/{plural}/{name}  (get/update/delete cluster-scoped)
    //  - /apis/{group}/{version}/namespaces/{namespace}/{plural}  (list namespaced)
    //  - /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}  (get/update/delete namespaced)
    //  - /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}/status  (status subresource)
    //  - /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}/scale  (scale subresource)

    if !path.starts_with("/apis/") {
        return Err(StatusCode::NOT_FOUND);
    }

    let parts: Vec<&str> = path.trim_start_matches("/apis/").split('/').collect();

    // Check for API aggregation — if an APIService is registered for this
    // group/version, proxy the request to the backing service's pod.
    if parts.len() >= 2 {
        let group = parts[0];
        let version = parts[1];
        match handlers::generic::resolve_aggregator_target(&state, group, version).await {
            Ok(Some(target)) => {
                let path_and_query = match uri.query() {
                    Some(q) => format!("{}?{}", uri.path(), q),
                    None => uri.path().to_string(),
                };
                let headers = req.headers().clone();
                let body_bytes = axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024)
                    .await
                    .map_err(|_| StatusCode::BAD_REQUEST)?
                    .to_vec();
                debug!(
                    "API aggregation: proxying {}/{} -> {}:{}",
                    group, version, target.host, target.port
                );
                let resp = handlers::generic::forward_to_aggregator(
                    &target,
                    &auth_ctx,
                    method,
                    &path_and_query,
                    &headers,
                    body_bytes,
                )
                .await;
                return Ok(resp);
            }
            Ok(None) => { /* not an aggregated group; continue to CRD logic */ }
            Err(resp) => return Ok(resp),
        }
    }

    // Handle /apis/{group} — return APIGroup info for the group
    if parts.len() == 1 && !parts[0].is_empty() {
        let group_name = parts[0];
        // Look up CRDs for this group to determine the actual versions
        let prefix = rusternetes_storage::build_prefix("customresourcedefinitions", None);
        let crds: Vec<CustomResourceDefinition> =
            state.storage.list(&prefix).await.unwrap_or_default();
        let mut versions = Vec::new();
        for crd in &crds {
            if crd.spec.group == group_name {
                for ver in &crd.spec.versions {
                    if ver.served
                        && !versions.iter().any(|v: &serde_json::Value| {
                            v.get("version").and_then(|v| v.as_str()) == Some(&ver.name)
                        })
                    {
                        versions.push(serde_json::json!({
                            "groupVersion": format!("{}/{}", group_name, ver.name),
                            "version": ver.name,
                        }));
                    }
                }
            }
        }
        if versions.is_empty() {
            versions.push(serde_json::json!({
                "groupVersion": format!("{}/v1", group_name),
                "version": "v1",
            }));
        }
        let group_info = serde_json::json!({
            "kind": "APIGroup",
            "apiVersion": "v1",
            "name": group_name,
            "versions": versions,
            "preferredVersion": versions[0],
        });
        return Ok(axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(group_info.to_string()))
            .unwrap());
    }

    // Handle /apis/{group}/{version} — return APIResourceList for CRDs in this group/version
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        let group_name = parts[0];
        let version_name = parts[1];
        let prefix = rusternetes_storage::build_prefix("customresourcedefinitions", None);
        let crds: Vec<CustomResourceDefinition> =
            state.storage.list(&prefix).await.unwrap_or_default();
        let mut resources = Vec::new();
        for crd in &crds {
            if crd.spec.group != group_name {
                continue;
            }
            let has_version = crd
                .spec
                .versions
                .iter()
                .any(|v| v.name == version_name && v.served);
            if !has_version {
                continue;
            }
            let namespaced =
                crd.spec.scope == rusternetes_common::resources::ResourceScope::Namespaced;
            let verbs = vec![
                "create",
                "delete",
                "deletecollection",
                "get",
                "list",
                "patch",
                "update",
                "watch",
            ];
            let mut res = serde_json::json!({
                "name": crd.spec.names.plural,
                "singularName": crd.spec.names.singular,
                "namespaced": namespaced,
                "kind": crd.spec.names.kind,
                "verbs": verbs,
                "storageVersionHash": "",
            });
            if let Some(ref short) = crd.spec.names.short_names {
                res["shortNames"] = serde_json::json!(short);
            }
            if let Some(ref categories) = crd.spec.names.categories {
                res["categories"] = serde_json::json!(categories);
            }
            resources.push(res);
            // Add status and scale subresources if defined
            if let Some(ver) = crd.spec.versions.iter().find(|v| v.name == version_name) {
                if let Some(ref sub) = ver.subresources {
                    if sub.status.is_some() {
                        resources.push(serde_json::json!({
                            "name": format!("{}/status", crd.spec.names.plural),
                            "singularName": "",
                            "namespaced": namespaced,
                            "kind": crd.spec.names.kind,
                            "verbs": ["get", "patch", "update"],
                        }));
                    }
                    if sub.scale.is_some() {
                        resources.push(serde_json::json!({
                            "name": format!("{}/scale", crd.spec.names.plural),
                            "singularName": "",
                            "namespaced": namespaced,
                            "kind": "Scale",
                            "group": "autoscaling",
                            "version": "v1",
                            "verbs": ["get", "patch", "update"],
                        }));
                    }
                }
            }
        }
        let resource_list = serde_json::json!({
            "kind": "APIResourceList",
            "apiVersion": "v1",
            "groupVersion": format!("{}/{}", group_name, version_name),
            "resources": resources,
        });
        return Ok(axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(resource_list.to_string()))
            .unwrap());
    }

    // Parse the path components
    let (group, version, plural, namespace, name, subresource) = match parts.as_slice() {
        // Namespaced: /apis/{group}/{version}/namespaces/{namespace}/{plural}
        [group, version, "namespaces", namespace, plural]
            if method == Method::GET || method == Method::POST =>
        {
            (*group, *version, *plural, Some(*namespace), None, None)
        }
        // Namespaced resource: /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}
        [group, version, "namespaces", namespace, plural, name] => (
            *group,
            *version,
            *plural,
            Some(*namespace),
            Some(*name),
            None,
        ),
        // Namespaced subresource: /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}/{subresource}
        [group, version, "namespaces", namespace, plural, name, subresource] => (
            *group,
            *version,
            *plural,
            Some(*namespace),
            Some(*name),
            Some(*subresource),
        ),
        // Cluster-scoped: /apis/{group}/{version}/{plural}
        [group, version, plural] if method == Method::GET || method == Method::POST => {
            (*group, *version, *plural, None, None, None)
        }
        // Cluster-scoped resource: /apis/{group}/{version}/{plural}/{name}
        [group, version, plural, name] => (*group, *version, *plural, None, Some(*name), None),
        // Cluster-scoped subresource: /apis/{group}/{version}/{plural}/{name}/{subresource}
        [group, version, plural, name, subresource] => (
            *group,
            *version,
            *plural,
            None,
            Some(*name),
            Some(*subresource),
        ),
        _ => {
            return Err(StatusCode::NOT_FOUND);
        }
    };

    // Build CRD name from plural and group
    let crd_name = format!("{}.{}", plural, group);
    let crd_key = build_key("customresourcedefinitions", None, &crd_name);

    // Check if this CRD exists
    let crd: Result<CustomResourceDefinition, _> = state.storage.get(&crd_key).await;

    if crd.is_err() {
        debug!("No CRD found for {}.{}, returning 404", plural, group);
        return Err(StatusCode::NOT_FOUND);
    }

    let _crd = crd.unwrap();
    debug!("Found CRD {} for request", crd_name);

    // Route to the appropriate custom resource handler based on method and path
    let response = match (method.clone(), name, subresource) {
        // POST to list endpoint = create
        (Method::POST, None, None) => {
            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .map_err(|_| StatusCode::BAD_REQUEST)?;

            // Parse query parameters from URI
            let query_params: std::collections::HashMap<String, String> = uri
                .query()
                .map(|q| {
                    url::form_urlencoded::parse(q.as_bytes())
                        .into_owned()
                        .collect()
                })
                .unwrap_or_default();

            match handlers::custom_resource::create_custom_resource(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                )),
                axum::extract::Query(query_params),
                body,
            )
            .await
            {
                Ok((status, json)) => (status, json).into_response(),
                Err(e) => {
                    warn!("Error creating custom resource: {}", e);
                    e.into_response()
                }
            }
        }
        // GET with no name = list (or watch if ?watch=true)
        (Method::GET, None, None) => {
            // Parse query params to check for watch
            let query_params: std::collections::HashMap<String, String> = uri
                .query()
                .map(|q| {
                    url::form_urlencoded::parse(q.as_bytes())
                        .into_owned()
                        .collect()
                })
                .unwrap_or_default();
            let is_watch = query_params
                .get("watch")
                .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
                .unwrap_or(false);

            if is_watch {
                // Watch custom resources using the JSON watch handler
                let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
                let watch_params = crate::handlers::watch::WatchParams {
                    resource_version: crate::handlers::watch::normalize_resource_version(
                        query_params.get("resourceVersion").cloned(),
                    ),
                    timeout_seconds: query_params
                        .get("timeoutSeconds")
                        .and_then(|v| v.parse::<u64>().ok()),
                    label_selector: query_params.get("labelSelector").cloned(),
                    field_selector: query_params.get("fieldSelector").cloned(),
                    watch: Some(true),
                    allow_watch_bookmarks: query_params
                        .get("allowWatchBookmarks")
                        .and_then(|v| v.parse::<bool>().ok()),
                    send_initial_events: query_params
                        .get("sendInitialEvents")
                        .and_then(|v| v.parse::<bool>().ok()),
                };
                if let Some(ns) = namespace {
                    match crate::handlers::watch::watch_namespaced::<
                        rusternetes_common::resources::CustomResource,
                    >(
                        state.clone(),
                        auth_ctx.clone(),
                        ns.to_string(),
                        &resource_type,
                        group,
                        watch_params,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        Err(e) => {
                            warn!("Error watching custom resources: {}", e);
                            e.into_response()
                        }
                    }
                } else {
                    match crate::handlers::watch::watch_cluster_scoped::<
                        rusternetes_common::resources::CustomResource,
                    >(
                        state.clone(),
                        auth_ctx.clone(),
                        &resource_type,
                        group,
                        watch_params,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        Err(e) => {
                            warn!("Error watching custom resources: {}", e);
                            e.into_response()
                        }
                    }
                }
            } else {
                match handlers::custom_resource::list_custom_resources(
                    State(state.clone()),
                    Extension(auth_ctx.clone()),
                    axum::extract::Path((
                        group.to_string(),
                        version.to_string(),
                        plural.to_string(),
                        namespace.map(|s| s.to_string()),
                    )),
                    axum::extract::Query(query_params),
                )
                .await
                {
                    Ok(json) => json.into_response(),
                    Err(e) => {
                        warn!("Error listing custom resources: {}", e);
                        e.into_response()
                    }
                }
            }
        }
        // GET with name = get
        (Method::GET, Some(name), None) => {
            match handlers::custom_resource::get_custom_resource(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
            )
            .await
            {
                Ok(json) => json.into_response(),
                Err(e) => {
                    warn!("Error getting custom resource: {}", e);
                    e.into_response()
                }
            }
        }
        // PUT with name = update
        (Method::PUT, Some(name), None) => {
            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .map_err(|_| StatusCode::BAD_REQUEST)?;

            // Parse query parameters from URI
            let query_params: std::collections::HashMap<String, String> = uri
                .query()
                .map(|q| {
                    url::form_urlencoded::parse(q.as_bytes())
                        .into_owned()
                        .collect()
                })
                .unwrap_or_default();

            match handlers::custom_resource::update_custom_resource(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
                axum::extract::Query(query_params),
                body,
            )
            .await
            {
                Ok(json) => json.into_response(),
                Err(e) => {
                    warn!("Error updating custom resource: {}", e);
                    e.into_response()
                }
            }
        }
        // DELETE with name = delete
        (Method::DELETE, Some(name), None) => {
            // Parse query parameters from URI
            let query_params: std::collections::HashMap<String, String> = uri
                .query()
                .map(|q| {
                    url::form_urlencoded::parse(q.as_bytes())
                        .into_owned()
                        .collect()
                })
                .unwrap_or_default();

            match handlers::custom_resource::delete_custom_resource(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
                axum::extract::Query(query_params),
            )
            .await
            {
                Ok(status) => status.into_response(),
                Err(e) => {
                    warn!("Error deleting custom resource: {}", e);
                    e.into_response()
                }
            }
        }
        // PATCH with name = patch
        (Method::PATCH, Some(name), None) => {
            // Reconstruct the request for the patch handler
            let (parts, body) = req.into_parts();
            let reconstructed_req = Request::from_parts(parts, body);

            match handlers::custom_resource::patch_custom_resource(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
                reconstructed_req,
            )
            .await
            {
                Ok(json) => json.into_response(),
                Err(e) => {
                    // Use Error's IntoResponse which returns proper K8s
                    // Status JSON with correct HTTP status codes (422 for
                    // InvalidResource, etc). Previously returned 500 text/plain.
                    e.into_response()
                }
            }
        }
        // Status subresource endpoints
        (Method::GET, Some(name), Some("status")) => {
            match handlers::custom_resource::get_custom_resource_status(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
            )
            .await
            {
                Ok(json) => json.into_response(),
                Err(e) => {
                    warn!("Error getting custom resource status: {}", e);
                    e.into_response()
                }
            }
        }
        (Method::PUT, Some(name), Some("status")) => {
            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            let status: serde_json::Value =
                serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;

            match handlers::custom_resource::update_custom_resource_status(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
                DumpingJson(status),
            )
            .await
            {
                Ok(json) => json.into_response(),
                Err(e) => {
                    warn!("Error updating custom resource status: {}", e);
                    e.into_response()
                }
            }
        }
        (Method::PATCH, Some(name), Some("status")) => {
            // Reconstruct the request for the patch handler
            let (parts, body) = req.into_parts();
            let reconstructed_req = Request::from_parts(parts, body);

            match handlers::custom_resource::patch_custom_resource_status(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
                reconstructed_req,
            )
            .await
            {
                Ok(json) => json.into_response(),
                Err(e) => {
                    warn!("Error patching custom resource status: {}", e);
                    e.into_response()
                }
            }
        }
        // Scale subresource endpoints
        (Method::GET, Some(name), Some("scale")) => {
            match handlers::custom_resource::get_custom_resource_scale(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
            )
            .await
            {
                Ok(json) => json.into_response(),
                Err(e) => {
                    warn!("Error getting custom resource scale: {}", e);
                    e.into_response()
                }
            }
        }
        (Method::PUT, Some(name), Some("scale")) => {
            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            let scale: handlers::custom_resource::Scale =
                serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;

            match handlers::custom_resource::update_custom_resource_scale(
                State(state.clone()),
                Extension(auth_ctx.clone()),
                axum::extract::Path((
                    group.to_string(),
                    version.to_string(),
                    plural.to_string(),
                    namespace.map(|s| s.to_string()),
                    name.to_string(),
                )),
                DumpingJson(scale),
            )
            .await
            {
                Ok(json) => json.into_response(),
                Err(e) => {
                    warn!("Error updating custom resource scale: {}", e);
                    e.into_response()
                }
            }
        }
        _ => {
            return Err(StatusCode::METHOD_NOT_ALLOWED);
        }
    };

    Ok(response)
}

pub fn build_router(state: Arc<ApiServerState>, console_dir: Option<&Path>) -> Router {
    let skip_auth = state.skip_auth;

    // Routes that don't require authentication
    let public_routes = Router::new()
        .route("/healthz", get(handlers::health::healthz))
        .route("/healthz/verbose", get(handlers::health::healthz_verbose))
        .route("/livez", get(handlers::health::healthz))
        .route("/readyz", get(handlers::health::readyz))
        .route("/metrics", get(handlers::health::metrics))
        // OIDC discovery endpoints for service account issuer
        .route(
            "/.well-known/openid-configuration",
            get(handlers::health::openid_configuration),
        )
        .route("/openid/v1/jwks", get(handlers::health::openid_jwks));

    // Discovery and OpenAPI endpoints get Cache-Control: no-cache, private
    // so kubectl always re-fetches and sees newly created CRDs.
    // K8s ref: aggregated discovery uses ETag; non-aggregated uses no-cache.
    let discovery_routes = Router::new()
        .route("/api", get(handlers::discovery::get_core_api))
        .route("/api/", get(handlers::discovery::get_core_api))
        .route("/api/v1", get(handlers::discovery::get_core_resources))
        .route("/api/v1/", get(handlers::discovery::get_core_resources))
        .route("/apis", get(handlers::discovery::get_api_groups))
        .route("/apis/", get(handlers::discovery::get_api_groups))
        .route("/apis/:group/", get(handlers::discovery::get_api_group))
        .route(
            "/apis/apps/v1",
            get(handlers::discovery::get_apps_v1_resources),
        )
        .route(
            "/apis/batch/v1",
            get(handlers::discovery::get_batch_v1_resources),
        )
        .route(
            "/apis/networking.k8s.io/v1",
            get(handlers::discovery::get_networking_v1_resources),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1",
            get(handlers::discovery::get_rbac_v1_resources),
        )
        .route(
            "/apis/storage.k8s.io/v1",
            get(handlers::discovery::get_storage_v1_resources),
        )
        .route(
            "/apis/scheduling.k8s.io/v1",
            get(handlers::discovery::get_scheduling_v1_resources),
        )
        .route(
            "/apis/apiextensions.k8s.io/v1",
            get(handlers::discovery::get_apiextensions_v1_resources),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1",
            get(handlers::discovery::get_admissionregistration_v1_resources),
        )
        .route(
            "/apis/coordination.k8s.io/v1",
            get(handlers::discovery::get_coordination_v1_resources),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1",
            get(handlers::discovery::get_flowcontrol_v1_resources),
        )
        .route(
            "/apis/certificates.k8s.io/v1",
            get(handlers::discovery::get_certificates_v1_resources),
        )
        .route(
            "/apis/snapshot.storage.k8s.io/v1",
            get(handlers::discovery::get_snapshot_v1_resources),
        )
        .route(
            "/apis/discovery.k8s.io/v1",
            get(handlers::discovery::get_discovery_v1_resources),
        )
        .route(
            "/apis/autoscaling/v1",
            get(handlers::discovery::get_autoscaling_v1_resources),
        )
        .route(
            "/apis/autoscaling/v2",
            get(handlers::discovery::get_autoscaling_v2_resources),
        )
        .route(
            "/apis/policy/v1",
            get(handlers::discovery::get_policy_v1_resources),
        )
        .route(
            "/apis/node.k8s.io/v1",
            get(handlers::discovery::get_node_v1_resources),
        )
        .route(
            "/apis/authentication.k8s.io/v1",
            get(handlers::discovery::get_authentication_v1_resources),
        )
        .route(
            "/apis/authorization.k8s.io/v1",
            get(handlers::discovery::get_authorization_v1_resources),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1",
            get(handlers::discovery::get_metrics_v1beta1_resources),
        )
        .route(
            "/apis/custom.metrics.k8s.io/v1beta2",
            get(handlers::discovery::get_custom_metrics_v1beta2_resources),
        )
        .route(
            "/apis/resource.k8s.io/v1",
            get(handlers::discovery::get_resource_v1_resources),
        )
        .route(
            "/apis/events.k8s.io/v1",
            get(handlers::discovery::get_events_v1_resources),
        )
        .route(
            "/apis/apiregistration.k8s.io/v1",
            get(handlers::discovery::get_apiregistration_v1_resources),
        )
        // APIService CRUD routes live in protected_routes (below) because the
        // handlers consume `Extension<AuthContext>` which is only injected by
        // the protected_routes auth middleware. Wiring them here previously
        // produced a 500 on every request — the extension extractor failed
        // before the handler ran.
        .route("/version", get(handlers::discovery::get_version))
        // OpenAPI spec endpoints
        .route("/openapi/v2", get(handlers::openapi::get_swagger_spec))
        .route("/openapi/v3", get(handlers::openapi::get_openapi_spec))
        .route(
            "/openapi/v3/*path",
            get(handlers::openapi::get_openapi_spec_path),
        )
        .route("/swagger.json", get(handlers::openapi::get_swagger_spec))
        .layer(axum_middleware::map_response(
            |mut response: Response| async move {
                response.headers_mut().insert(
                    axum::http::header::CACHE_CONTROL,
                    "no-cache, private".parse().unwrap(),
                );
                response
            },
        ));

    let public_routes = public_routes.merge(discovery_routes);

    // Routes that require authentication (unless skip_auth is enabled)
    let mut protected_routes = Router::new()
        // Core v1 API
        .route("/api/v1/namespaces", get(handlers::namespace::list)
            .post(handlers::namespace::create)
            .delete(handlers::namespace::deletecollection_namespaces))
        .route(
            "/api/v1/namespaces/:name",
            get(handlers::namespace::get)
                .put(handlers::namespace::update)
                .patch(handlers::namespace::patch)
                .delete(handlers::namespace::delete_ns),
        )
        .route(
            "/api/v1/namespaces/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        .route(
            "/api/v1/namespaces/:name/finalize",
            put(handlers::namespace::update),
        )
        // Watch namespaces (cluster-scoped)
        .route(
            "/api/v1/watch/namespaces",
            get(handlers::watch::watch_namespaces),
        )
        // ComponentStatus (cluster-scoped, deprecated but still used)
        .route(
            "/api/v1/componentstatuses",
            get(handlers::componentstatus::list),
        )
        .route(
            "/api/v1/componentstatuses/:name",
            get(handlers::componentstatus::get),
        )
        // Pods
        .route(
            "/api/v1/namespaces/:namespace/pods",
            get(handlers::pod::list).post(handlers::pod::create).delete(handlers::pod::deletecollection_pods),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name",
            get(handlers::pod::get)
                .put(handlers::pod::update)
                .patch(handlers::pod::patch)
                .delete(handlers::pod::delete_pod),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/log",
            get(handlers::pod_subresources::get_logs),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/exec",
            get(handlers::pod_subresources::exec)
                .post(handlers::pod_subresources::exec),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/attach",
            get(handlers::pod_subresources::attach)
                .post(handlers::pod_subresources::attach),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/portforward",
            get(handlers::pod_subresources::portforward)
                .post(handlers::pod_subresources::portforward),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/binding",
            post(handlers::pod_subresources::create_binding),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/eviction",
            post(handlers::pod_subresources::create_eviction),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/resize",
            // PUT goes through the dedicated subresource handler so we can do
            // CAS-retry on resourceVersion conflicts (KEP-1287). GET/PATCH fall
            // back to the generic pod handlers — only PUT is the resize hot path.
            get(handlers::pod::get)
                .put(handlers::pod_subresources::resize_pod)
                .patch(handlers::pod::patch),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/ephemeralcontainers",
            get(handlers::pod::get)
                .put(handlers::pod::update)
                .patch(handlers::pod::patch),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/proxy",
            any(handlers::proxy::proxy_pod_root),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/proxy/",
            any(handlers::proxy::proxy_pod_root),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/proxy/*path",
            any(handlers::proxy::proxy_pod),
        )
        // Watch pods in a namespace
        .route(
            "/api/v1/watch/namespaces/:namespace/pods",
            get(handlers::watch::watch_pods),
        )
        // Pods (all namespaces)
        .route(
            "/api/v1/pods",
            get(handlers::pod::list_all_pods),
        )
        // Services
        .route(
            "/api/v1/namespaces/:namespace/services",
            get(handlers::service::list).post(handlers::service::create).delete(handlers::service::deletecollection_services),
        )
        .route(
            "/api/v1/namespaces/:namespace/services/:name",
            get(handlers::service::get)
                .put(handlers::service::update)
                .patch(handlers::service::patch)
                .delete(handlers::service::delete_service),
        )
        .route(
            "/api/v1/namespaces/:namespace/services/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        .route(
            "/api/v1/namespaces/:namespace/services/:name/proxy",
            any(handlers::proxy::proxy_service_root),
        )
        .route(
            "/api/v1/namespaces/:namespace/services/:name/proxy/",
            any(handlers::proxy::proxy_service_root),
        )
        .route(
            "/api/v1/namespaces/:namespace/services/:name/proxy/*path",
            any(handlers::proxy::proxy_service),
        )
        // Watch services in a namespace
        .route(
            "/api/v1/watch/namespaces/:namespace/services",
            get(handlers::watch::watch_services),
        )
        // Services (all namespaces)
        .route(
            "/api/v1/services",
            get(handlers::service::list_all_services),
        )
        // Endpoints
        .route(
            "/api/v1/namespaces/:namespace/endpoints",
            get(handlers::endpoints::list_endpoints).post(handlers::endpoints::create_endpoints).delete(handlers::endpoints::deletecollection_endpoints),
        )
        .route(
            "/api/v1/namespaces/:namespace/endpoints/:name",
            get(handlers::endpoints::get_endpoints)
                .put(handlers::endpoints::update_endpoints)
                .patch(handlers::endpoints::patch_endpoints)
                .delete(handlers::endpoints::delete_endpoints),
        )
        .route(
            "/api/v1/endpoints",
            get(handlers::endpoints::list_all_endpoints),
        )
        // Watch endpoints in a namespace
        .route(
            "/api/v1/watch/namespaces/:namespace/endpoints",
            get(handlers::watch::watch_endpoints),
        )
        // ConfigMaps
        .route(
            "/api/v1/namespaces/:namespace/configmaps",
            get(handlers::configmap::list).post(handlers::configmap::create).delete(handlers::configmap::deletecollection_configmaps),
        )
        .route(
            "/api/v1/namespaces/:namespace/configmaps/:name",
            get(handlers::configmap::get)
                .put(handlers::configmap::update)
                .patch(handlers::configmap::patch)
                .delete(handlers::configmap::delete_configmap),
        )
        // ConfigMaps (all namespaces)
        .route(
            "/api/v1/configmaps",
            get(handlers::configmap::list_all_configmaps),
        )
        // Watch configmaps in a namespace
        .route(
            "/api/v1/watch/namespaces/:namespace/configmaps",
            get(handlers::watch::watch_configmaps),
        )
        // Secrets
        .route(
            "/api/v1/namespaces/:namespace/secrets",
            get(handlers::secret::list).post(handlers::secret::create).delete(handlers::secret::deletecollection_secrets),
        )
        .route(
            "/api/v1/namespaces/:namespace/secrets/:name",
            get(handlers::secret::get)
                .put(handlers::secret::update)
                .patch(handlers::secret::patch)
                .delete(handlers::secret::delete_secret),
        )
        // Secrets (all namespaces)
        .route(
            "/api/v1/secrets",
            get(handlers::secret::list_all_secrets),
        )
        // Watch secrets in a namespace
        .route(
            "/api/v1/watch/namespaces/:namespace/secrets",
            get(handlers::watch::watch_secrets),
        )
        // Nodes
        .route(
            "/api/v1/nodes",
            get(handlers::node::list).post(handlers::node::create).delete(handlers::node::deletecollection_nodes),
        )
        .route(
            "/api/v1/nodes/:name",
            get(handlers::node::get)
                .put(handlers::node::update)
                .patch(handlers::node::patch)
                .delete(handlers::node::delete_node),
        )
        .route(
            "/api/v1/nodes/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        .route(
            "/api/v1/nodes/:name/proxy/*path",
            any(handlers::proxy::proxy_node),
        )
        // Watch nodes (cluster-scoped)
        .route(
            "/api/v1/watch/nodes",
            get(handlers::watch::watch_nodes),
        )
        // Apps v1 API - Deployments
        .route(
            "/apis/apps/v1/namespaces/:namespace/deployments",
            get(handlers::deployment::list).post(handlers::deployment::create).delete(handlers::deployment::deletecollection_deployments),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/deployments/:name",
            get(handlers::deployment::get)
                .put(handlers::deployment::update)
                .patch(handlers::deployment::patch)
                .delete(handlers::deployment::delete_deployment),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/deployments/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/deployments/:name/scale",
            get(handlers::scale::get_scale)
                .put(handlers::scale::update_scale)
                .patch(handlers::scale::patch_scale),
        )
        // Deployments (all namespaces)
        .route(
            "/apis/apps/v1/deployments",
            get(handlers::deployment::list_all_deployments),
        )
        // Watch deployments in a namespace
        .route(
            "/apis/apps/v1/watch/namespaces/:namespace/deployments",
            get(handlers::watch::watch_deployments),
        )
        // Apps v1 API - ReplicaSets
        .route(
            "/apis/apps/v1/namespaces/:namespace/replicasets",
            get(handlers::replicaset::list).post(handlers::replicaset::create).delete(handlers::replicaset::deletecollection_replicasets),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/replicasets/:name",
            get(handlers::replicaset::get)
                .put(handlers::replicaset::update)
                .patch(handlers::replicaset::patch)
                .delete(handlers::replicaset::delete_replicaset),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/replicasets/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/replicasets/:name/scale",
            get(handlers::scale::get_scale)
                .put(handlers::scale::update_scale)
                .patch(handlers::scale::patch_scale),
        )
        // ReplicaSets (all namespaces)
        .route(
            "/apis/apps/v1/replicasets",
            get(handlers::replicaset::list_all_replicasets),
        )
        // Watch replicasets in a namespace
        .route(
            "/apis/apps/v1/watch/namespaces/:namespace/replicasets",
            get(handlers::watch::watch_replicasets),
        )
        // Apps v1 API - StatefulSets
        .route(
            "/apis/apps/v1/namespaces/:namespace/statefulsets",
            get(handlers::statefulset::list).post(handlers::statefulset::create).delete(handlers::statefulset::deletecollection_statefulsets),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/statefulsets/:name",
            get(handlers::statefulset::get)
                .put(handlers::statefulset::update)
                .patch(handlers::statefulset::patch)
                .delete(handlers::statefulset::delete_statefulset),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/statefulsets/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/statefulsets/:name/scale",
            get(handlers::scale::get_scale)
                .put(handlers::scale::update_scale)
                .patch(handlers::scale::patch_scale),
        )
        // StatefulSets (all namespaces)
        .route(
            "/apis/apps/v1/statefulsets",
            get(handlers::statefulset::list_all_statefulsets),
        )
        // Watch statefulsets in a namespace
        .route(
            "/apis/apps/v1/watch/namespaces/:namespace/statefulsets",
            get(handlers::watch::watch_statefulsets),
        )
        // Apps v1 API - DaemonSets
        .route(
            "/apis/apps/v1/namespaces/:namespace/daemonsets",
            get(handlers::daemonset::list).post(handlers::daemonset::create).delete(handlers::daemonset::deletecollection_daemonsets),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/daemonsets/:name",
            get(handlers::daemonset::get)
                .put(handlers::daemonset::update)
                .patch(handlers::daemonset::patch)
                .delete(handlers::daemonset::delete_daemonset),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/daemonsets/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/daemonsets/:name/scale",
            get(handlers::scale::get_scale)
                .put(handlers::scale::update_scale)
                .patch(handlers::scale::patch_scale),
        )
        // DaemonSets (all namespaces)
        .route(
            "/apis/apps/v1/daemonsets",
            get(handlers::daemonset::list_all_daemonsets),
        )
        // Watch daemonsets in a namespace
        .route(
            "/apis/apps/v1/watch/namespaces/:namespace/daemonsets",
            get(handlers::watch::watch_daemonsets),
        )
        // Batch v1 API - Jobs
        .route(
            "/apis/batch/v1/namespaces/:namespace/jobs",
            get(handlers::job::list).post(handlers::job::create).delete(handlers::job::deletecollection_jobs),
        )
        .route(
            "/apis/batch/v1/namespaces/:namespace/jobs/:name",
            get(handlers::job::get)
                .put(handlers::job::update)
                .patch(handlers::job::patch)
                .delete(handlers::job::delete_job),
        )
        .route(
            "/apis/batch/v1/namespaces/:namespace/jobs/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        // Jobs (all namespaces)
        .route(
            "/apis/batch/v1/jobs",
            get(handlers::job::list_all_jobs),
        )
        // Watch jobs in a namespace
        .route(
            "/apis/batch/v1/watch/namespaces/:namespace/jobs",
            get(handlers::watch::watch_jobs),
        )
        // Batch v1 API - CronJobs
        .route(
            "/apis/batch/v1/namespaces/:namespace/cronjobs",
            get(handlers::cronjob::list).post(handlers::cronjob::create).delete(handlers::cronjob::deletecollection_cronjobs),
        )
        .route(
            "/apis/batch/v1/namespaces/:namespace/cronjobs/:name",
            get(handlers::cronjob::get)
                .put(handlers::cronjob::update)
                .patch(handlers::cronjob::patch)
                .delete(handlers::cronjob::delete_cronjob),
        )
        .route(
            "/apis/batch/v1/namespaces/:namespace/cronjobs/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        // CronJobs (all namespaces)
        .route(
            "/apis/batch/v1/cronjobs",
            get(handlers::cronjob::list_all_cronjobs),
        )
        // Watch cronjobs in a namespace
        .route(
            "/apis/batch/v1/watch/namespaces/:namespace/cronjobs",
            get(handlers::watch::watch_cronjobs),
        )
        // ServiceAccounts
        .route(
            "/api/v1/namespaces/:namespace/serviceaccounts",
            get(handlers::service_account::list).post(handlers::service_account::create).delete(handlers::service_account::deletecollection_serviceaccounts),
        )
        .route(
            "/api/v1/namespaces/:namespace/serviceaccounts/:name",
            get(handlers::service_account::get)
                .put(handlers::service_account::update)
                .patch(handlers::service_account::patch)
                .delete(handlers::service_account::delete_service_account),
        )
        // ServiceAccounts (all namespaces)
        .route(
            "/api/v1/serviceaccounts",
            get(handlers::service_account::list_all_serviceaccounts),
        )
        // Watch serviceaccounts in a namespace
        .route(
            "/api/v1/watch/namespaces/:namespace/serviceaccounts",
            get(handlers::watch::watch_serviceaccounts),
        )
        // RBAC - Roles
        .route(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/:namespace/roles",
            get(handlers::rbac::list_roles).post(handlers::rbac::create_role).delete(handlers::rbac::deletecollection_roles),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/:namespace/roles/:name",
            get(handlers::rbac::get_role)
                .put(handlers::rbac::update_role)
                .patch(handlers::rbac::patch_role)
                .delete(handlers::rbac::delete_role),
        )
        // Roles (all namespaces)
        .route(
            "/apis/rbac.authorization.k8s.io/v1/roles",
            get(handlers::rbac::list_all_roles),
        )
        // RBAC - RoleBindings
        .route(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/:namespace/rolebindings",
            get(handlers::rbac::list_rolebindings).post(handlers::rbac::create_rolebinding).delete(handlers::rbac::deletecollection_rolebindings),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/:namespace/rolebindings/:name",
            get(handlers::rbac::get_rolebinding)
                .put(handlers::rbac::update_rolebinding)
                .patch(handlers::rbac::patch_rolebinding)
                .delete(handlers::rbac::delete_rolebinding),
        )
        // RoleBindings (all namespaces)
        .route(
            "/apis/rbac.authorization.k8s.io/v1/rolebindings",
            get(handlers::rbac::list_all_rolebindings),
        )
        // RBAC - ClusterRoles
        .route(
            "/apis/rbac.authorization.k8s.io/v1/clusterroles",
            get(handlers::rbac::list_clusterroles).post(handlers::rbac::create_clusterrole).delete(handlers::rbac::deletecollection_clusterroles),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/:name",
            get(handlers::rbac::get_clusterrole)
                .put(handlers::rbac::update_clusterrole)
                .patch(handlers::rbac::patch_clusterrole)
                .delete(handlers::rbac::delete_clusterrole),
        )
        // RBAC - ClusterRoleBindings
        .route(
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
            get(handlers::rbac::list_clusterrolebindings).post(handlers::rbac::create_clusterrolebinding).delete(handlers::rbac::deletecollection_clusterrolebindings),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/:name",
            get(handlers::rbac::get_clusterrolebinding)
                .put(handlers::rbac::update_clusterrolebinding)
                .patch(handlers::rbac::patch_clusterrolebinding)
                .delete(handlers::rbac::delete_clusterrolebinding),
        )
        // Storage v1 API - PersistentVolumes (cluster-scoped)
        .route(
            "/api/v1/persistentvolumes",
            get(handlers::persistentvolume::list_pvs).post(handlers::persistentvolume::create_pv)
            .delete(handlers::persistentvolume::deletecollection_persistentvolumes),
        )
        .route(
            "/api/v1/persistentvolumes/:name",
            get(handlers::persistentvolume::get_pv)
                .put(handlers::persistentvolume::update_pv)
                .patch(handlers::persistentvolume::patch_pv)
                .delete(handlers::persistentvolume::delete_pv),
        )
        .route(
            "/api/v1/persistentvolumes/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        // Watch persistentvolumes (cluster-scoped)
        .route(
            "/api/v1/watch/persistentvolumes",
            get(handlers::watch::watch_persistentvolumes),
        )
        // PersistentVolumeClaims (namespace-scoped)
        .route(
            "/api/v1/namespaces/:namespace/persistentvolumeclaims",
            get(handlers::persistentvolumeclaim::list_pvcs)
                .post(handlers::persistentvolumeclaim::create_pvc)
                .delete(handlers::persistentvolumeclaim::deletecollection_persistentvolumeclaims),
        )
        .route(
            "/api/v1/namespaces/:namespace/persistentvolumeclaims/:name",
            get(handlers::persistentvolumeclaim::get_pvc)
                .put(handlers::persistentvolumeclaim::update_pvc)
                .patch(handlers::persistentvolumeclaim::patch_pvc)
                .delete(handlers::persistentvolumeclaim::delete_pvc),
        )
        .route(
            "/api/v1/namespaces/:namespace/persistentvolumeclaims/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        // PersistentVolumeClaims (all namespaces)
        .route(
            "/api/v1/persistentvolumeclaims",
            get(handlers::persistentvolumeclaim::list_all_pvcs)
            .delete(handlers::persistentvolumeclaim::deletecollection_persistentvolumeclaims),
        )
        // Watch persistentvolumeclaims in a namespace
        .route(
            "/api/v1/watch/namespaces/:namespace/persistentvolumeclaims",
            get(handlers::watch::watch_persistentvolumeclaims),
        )
        // StorageClasses (cluster-scoped)
        .route(
            "/apis/storage.k8s.io/v1/storageclasses",
            get(handlers::storageclass::list_storageclasses)
                .post(handlers::storageclass::create_storageclass)
                .delete(handlers::storageclass::deletecollection_storageclasses),
        )
        .route(
            "/apis/storage.k8s.io/v1/storageclasses/:name",
            get(handlers::storageclass::get_storageclass)
                .put(handlers::storageclass::update_storageclass)
                .patch(handlers::storageclass::patch_storageclass)
                .delete(handlers::storageclass::delete_storageclass),
        )
        // Networking v1 API - Ingresses
        .route(
            "/apis/networking.k8s.io/v1/namespaces/:namespace/ingresses",
            get(handlers::ingress::list).post(handlers::ingress::create).delete(handlers::ingress::deletecollection_ingresses),
        )
        .route(
            "/apis/networking.k8s.io/v1/namespaces/:namespace/ingresses/:name",
            get(handlers::ingress::get)
                .put(handlers::ingress::update)
                .patch(handlers::ingress::patch)
                .delete(handlers::ingress::delete_ingress),
        )
        .route(
            "/apis/networking.k8s.io/v1/namespaces/:namespace/ingresses/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        // Ingresses (all namespaces)
        .route(
            "/apis/networking.k8s.io/v1/ingresses",
            get(handlers::ingress::list_all_ingresses),
        )
        // Networking v1 API - NetworkPolicies
        .route(
            "/apis/networking.k8s.io/v1/namespaces/:namespace/networkpolicies",
            get(handlers::networkpolicy::list).post(handlers::networkpolicy::create).delete(handlers::networkpolicy::deletecollection_networkpolicies),
        )
        .route(
            "/apis/networking.k8s.io/v1/namespaces/:namespace/networkpolicies/:name",
            get(handlers::networkpolicy::get)
                .put(handlers::networkpolicy::update)
                .patch(handlers::networkpolicy::patch)
                .delete(handlers::networkpolicy::delete_networkpolicy),
        )
        // NetworkPolicies (all namespaces)
        .route(
            "/apis/networking.k8s.io/v1/networkpolicies",
            get(handlers::networkpolicy::list_all_networkpolicies),
        )
        // Snapshot storage API - VolumeSnapshotClasses (cluster-scoped)
        .route(
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses",
            get(handlers::volumesnapshotclass::list_volumesnapshotclasses)
                .post(handlers::volumesnapshotclass::create_volumesnapshotclass)
                .delete(handlers::volumesnapshotclass::deletecollection_volumesnapshotclasses),
        )
        .route(
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses/:name",
            get(handlers::volumesnapshotclass::get_volumesnapshotclass)
                .put(handlers::volumesnapshotclass::update_volumesnapshotclass)
                .patch(handlers::volumesnapshotclass::patch_volumesnapshotclass)
                .delete(handlers::volumesnapshotclass::delete_volumesnapshotclass),
        )
        // VolumeSnapshots (namespace-scoped)
        .route(
            "/apis/snapshot.storage.k8s.io/v1/namespaces/:namespace/volumesnapshots",
            get(handlers::volumesnapshot::list_volumesnapshots)
                .post(handlers::volumesnapshot::create_volumesnapshot)
                .delete(handlers::volumesnapshot::deletecollection_volumesnapshots),
        )
        .route(
            "/apis/snapshot.storage.k8s.io/v1/namespaces/:namespace/volumesnapshots/:name",
            get(handlers::volumesnapshot::get_volumesnapshot)
                .put(handlers::volumesnapshot::update_volumesnapshot)
                .patch(handlers::volumesnapshot::patch_volumesnapshot)
                .delete(handlers::volumesnapshot::delete_volumesnapshot),
        )
        // VolumeSnapshots (all namespaces)
        .route(
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshots",
            get(handlers::volumesnapshot::list_all_volumesnapshots)
            .delete(handlers::volumesnapshot::deletecollection_volumesnapshots),
        )
        // VolumeSnapshotContents (cluster-scoped)
        .route(
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents",
            get(handlers::volumesnapshotcontent::list_volumesnapshotcontents)
                .post(handlers::volumesnapshotcontent::create_volumesnapshotcontent)
                .delete(handlers::volumesnapshotcontent::deletecollection_volumesnapshotcontents),
        )
        .route(
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents/:name",
            get(handlers::volumesnapshotcontent::get_volumesnapshotcontent)
                .put(handlers::volumesnapshotcontent::update_volumesnapshotcontent)
                .patch(handlers::volumesnapshotcontent::patch_volumesnapshotcontent)
                .delete(handlers::volumesnapshotcontent::delete_volumesnapshotcontent),
        )
        // Events (namespace-scoped)
        .route(
            "/api/v1/namespaces/:namespace/events",
            get(handlers::event::list).post(handlers::event::create).delete(handlers::event::deletecollection_events),
        )
        .route(
            "/api/v1/namespaces/:namespace/events/:name",
            get(handlers::event::get)
                .put(handlers::event::update)
                .patch(handlers::event::patch)
                .delete(handlers::event::delete),
        )
        // Events (all namespaces)
        .route(
            "/api/v1/events",
            get(handlers::event::list_all),
        )
        // Watch events in a namespace
        .route(
            "/api/v1/watch/namespaces/:namespace/events",
            get(handlers::watch::watch_events),
        )
        // Events via events.k8s.io/v1 API group (separate handlers for correct apiVersion)
        .route(
            "/apis/events.k8s.io/v1/namespaces/:namespace/events",
            get(handlers::event::list_events_v1).post(handlers::event::create_events_v1).delete(handlers::event::deletecollection_events),
        )
        .route(
            "/apis/events.k8s.io/v1/namespaces/:namespace/events/:name",
            get(handlers::event::get_events_v1)
                .put(handlers::event::update_events_v1)
                .patch(handlers::event::patch_events_v1)
                .delete(handlers::event::delete_events_v1),
        )
        .route(
            "/apis/events.k8s.io/v1/events",
            get(handlers::event::list_all_events_v1),
        )
        // ResourceQuotas (namespace-scoped)
        .route(
            "/api/v1/namespaces/:namespace/resourcequotas",
            get(handlers::resourcequota::list).post(handlers::resourcequota::create).delete(handlers::resourcequota::deletecollection_resourcequotas),
        )
        .route(
            "/api/v1/namespaces/:namespace/resourcequotas/:name",
            get(handlers::resourcequota::get)
                .put(handlers::resourcequota::update)
                .patch(handlers::resourcequota::patch)
                .delete(handlers::resourcequota::delete),
        )
        .route(
            "/api/v1/namespaces/:namespace/resourcequotas/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        .route(
            "/api/v1/watch/namespaces/:namespace/resourcequotas",
            get(handlers::watch::watch_resourcequotas),
        )
        // ResourceQuotas (all namespaces)
        .route(
            "/api/v1/resourcequotas",
            get(handlers::resourcequota::list_all),
        )
        .route(
            "/api/v1/watch/resourcequotas",
            get(handlers::watch::watch_resourcequotas_all),
        )
        // LimitRanges (namespace-scoped)
        .route(
            "/api/v1/namespaces/:namespace/limitranges",
            get(handlers::limitrange::list).post(handlers::limitrange::create).delete(handlers::limitrange::deletecollection_limitranges),
        )
        .route(
            "/api/v1/namespaces/:namespace/limitranges/:name",
            get(handlers::limitrange::get)
                .put(handlers::limitrange::update)
                .patch(handlers::limitrange::patch)
                .delete(handlers::limitrange::delete),
        )
        // LimitRanges (all namespaces)
        .route(
            "/api/v1/limitranges",
            get(handlers::limitrange::list_all),
        )
        // PriorityClasses (cluster-scoped)
        .route(
            "/apis/scheduling.k8s.io/v1/priorityclasses",
            get(handlers::priorityclass::list).post(handlers::priorityclass::create).delete(handlers::priorityclass::deletecollection_priorityclasses),
        )
        .route(
            "/apis/scheduling.k8s.io/v1/priorityclasses/:name",
            get(handlers::priorityclass::get)
                .put(handlers::priorityclass::update)
                .patch(handlers::priorityclass::patch)
                .delete(handlers::priorityclass::delete),
        )
        // CustomResourceDefinitions (cluster-scoped)
        .route(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            get(handlers::crd::list_crds).post(handlers::crd::create_crd)
            .delete(handlers::crd::deletecollection_customresourcedefinitions),
        )
        .route(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name",
            get(handlers::crd::get_crd)
                .put(handlers::crd::update_crd)
                .patch(handlers::crd::patch_crd)
                .delete(handlers::crd::delete_crd),
        )
        .route(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        // APIService (cluster-scoped, aggregator layer). Handlers require
        // Extension<AuthContext> for authz checks, so these routes must live
        // in protected_routes — the public discovery route at
        // /apis/apiregistration.k8s.io/v1 stays public (no AuthContext).
        .route(
            "/apis/apiregistration.k8s.io/v1/apiservices",
            get(handlers::generic::list_apiservices)
                .post(handlers::generic::create_apiservice)
                .delete(handlers::generic::deletecollection_apiservices),
        )
        .route(
            "/apis/apiregistration.k8s.io/v1/apiservices/:name",
            get(handlers::generic::get_apiservice)
                .put(handlers::generic::update_apiservice)
                .patch(handlers::generic::patch_apiservice)
                .delete(handlers::generic::delete_apiservice),
        )
        .route(
            "/apis/apiregistration.k8s.io/v1/apiservices/:name/status",
            get(handlers::generic::get_apiservice)
                .put(handlers::generic::update_apiservice_status)
                .patch(handlers::generic::patch_apiservice),
        )
        // ValidatingWebhookConfiguration (cluster-scoped)
        .route(
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            get(handlers::admission_webhook::list_validating_webhooks)
                .post(handlers::admission_webhook::create_validating_webhook)
                .delete(handlers::admission_webhook::deletecollection_validatingwebhookconfigurations),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/:name",
            get(handlers::admission_webhook::get_validating_webhook)
                .put(handlers::admission_webhook::update_validating_webhook)
                .patch(handlers::admission_webhook::patch_validating_webhook)
                .delete(handlers::admission_webhook::delete_validating_webhook),
        )
        // MutatingWebhookConfiguration (cluster-scoped)
        .route(
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
            get(handlers::admission_webhook::list_mutating_webhooks)
                .post(handlers::admission_webhook::create_mutating_webhook)
                .delete(handlers::admission_webhook::deletecollection_mutatingwebhookconfigurations),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/:name",
            get(handlers::admission_webhook::get_mutating_webhook)
                .put(handlers::admission_webhook::update_mutating_webhook)
                .patch(handlers::admission_webhook::patch_mutating_webhook)
                .delete(handlers::admission_webhook::delete_mutating_webhook),
        )
        // Coordination v1 API - Leases (namespace-scoped)
        .route(
            "/apis/coordination.k8s.io/v1/namespaces/:namespace/leases",
            get(handlers::lease::list).post(handlers::lease::create).delete(handlers::lease::deletecollection_leases),
        )
        .route(
            "/apis/coordination.k8s.io/v1/namespaces/:namespace/leases/:name",
            get(handlers::lease::get)
                .put(handlers::lease::update)
                .patch(handlers::lease::patch)
                .delete(handlers::lease::delete_lease),
        )
        // Leases (all namespaces)
        .route(
            "/apis/coordination.k8s.io/v1/leases",
            get(handlers::lease::list_all_leases),
        )
        // FlowControl API Priority and Fairness - PriorityLevelConfigurations (cluster-scoped)
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations",
            get(handlers::flowcontrol::list_priority_level_configurations)
                .post(handlers::flowcontrol::create_priority_level_configuration)
                .delete(handlers::flowcontrol::deletecollection_prioritylevelconfigurations),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations/:name",
            get(handlers::flowcontrol::get_priority_level_configuration)
                .put(handlers::flowcontrol::update_priority_level_configuration)
                .patch(handlers::flowcontrol::patch_priority_level_configuration)
                .delete(handlers::flowcontrol::delete_priority_level_configuration),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        // FlowControl API - FlowSchemas (cluster-scoped)
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas",
            get(handlers::flowcontrol::list_flow_schemas)
                .post(handlers::flowcontrol::create_flow_schema)
                .delete(handlers::flowcontrol::deletecollection_flowschemas),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas/:name",
            get(handlers::flowcontrol::get_flow_schema)
                .put(handlers::flowcontrol::update_flow_schema)
                .patch(handlers::flowcontrol::patch_flow_schema)
                .delete(handlers::flowcontrol::delete_flow_schema),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        // Certificates API - CertificateSigningRequests (cluster-scoped)
        .route(
            "/apis/certificates.k8s.io/v1/certificatesigningrequests",
            get(handlers::certificates::list_certificate_signing_requests)
                .post(handlers::certificates::create_certificate_signing_request)
                .delete(handlers::certificates::deletecollection_certificatesigningrequests),
        )
        .route(
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/:name",
            get(handlers::certificates::get_certificate_signing_request)
                .put(handlers::certificates::update_certificate_signing_request)
                .patch(handlers::certificates::patch_certificate_signing_request)
                .delete(handlers::certificates::delete_certificate_signing_request),
        )
        .route(
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/:name/status",
            get(handlers::certificates::get_certificate_signing_request_status)
                .put(handlers::certificates::update_certificate_signing_request_status)
                .patch(handlers::certificates::update_certificate_signing_request_status),
        )
        .route(
            "/apis/certificates.k8s.io/v1/certificatesigningrequests/:name/approval",
            get(handlers::certificates::get_certificate_signing_request)
                .put(handlers::certificates::approve_certificate_signing_request)
                .patch(handlers::certificates::patch_certificate_signing_request),
        )
        // Discovery API - EndpointSlices (namespace-scoped)
        .route(
            "/apis/discovery.k8s.io/v1/namespaces/:namespace/endpointslices",
            get(handlers::endpointslice::list_endpointslices)
                .post(handlers::endpointslice::create_endpointslice)
                .delete(handlers::endpointslice::deletecollection_endpointslices),
        )
        .route(
            "/apis/discovery.k8s.io/v1/namespaces/:namespace/endpointslices/:name",
            get(handlers::endpointslice::get_endpointslice)
                .put(handlers::endpointslice::update_endpointslice)
                .patch(handlers::endpointslice::patch_endpointslice)
                .delete(handlers::endpointslice::delete_endpointslice),
        )
        // EndpointSlices (all namespaces)
        .route(
            "/apis/discovery.k8s.io/v1/endpointslices",
            get(handlers::endpointslice::list_all_endpointslices)
            .delete(handlers::endpointslice::deletecollection_endpointslices),
        )
        // Watch endpointslices in a namespace
        .route(
            "/apis/discovery.k8s.io/v1/watch/namespaces/:namespace/endpointslices",
            get(handlers::watch::watch_endpointslices),
        )
        // Autoscaling v1 API - HorizontalPodAutoscalers (namespace-scoped)
        .route(
            "/apis/autoscaling/v1/namespaces/:namespace/horizontalpodautoscalers",
            get(handlers::horizontalpodautoscaler::list)
                .post(handlers::horizontalpodautoscaler::create)
                .delete(handlers::horizontalpodautoscaler::deletecollection_horizontalpodautoscalers),
        )
        .route(
            "/apis/autoscaling/v1/namespaces/:namespace/horizontalpodautoscalers/:name",
            get(handlers::horizontalpodautoscaler::get)
                .put(handlers::horizontalpodautoscaler::update)
                .patch(handlers::horizontalpodautoscaler::patch)
                .delete(handlers::horizontalpodautoscaler::delete),
        )
        .route(
            "/apis/autoscaling/v1/namespaces/:namespace/horizontalpodautoscalers/:name/status",
            get(handlers::horizontalpodautoscaler::get_status)
                .put(handlers::horizontalpodautoscaler::update_status)
                .patch(handlers::horizontalpodautoscaler::update_status),
        )
        .route(
            "/apis/autoscaling/v1/horizontalpodautoscalers",
            get(handlers::horizontalpodautoscaler::list_all)
            .delete(handlers::horizontalpodautoscaler::deletecollection_horizontalpodautoscalers),
        )
        // Autoscaling v2 API - HorizontalPodAutoscalers (namespace-scoped)
        .route(
            "/apis/autoscaling/v2/namespaces/:namespace/horizontalpodautoscalers",
            get(handlers::horizontalpodautoscaler::list)
                .post(handlers::horizontalpodautoscaler::create)
                .delete(handlers::horizontalpodautoscaler::deletecollection_horizontalpodautoscalers),
        )
        .route(
            "/apis/autoscaling/v2/namespaces/:namespace/horizontalpodautoscalers/:name",
            get(handlers::horizontalpodautoscaler::get)
                .put(handlers::horizontalpodautoscaler::update)
                .patch(handlers::horizontalpodautoscaler::patch)
                .delete(handlers::horizontalpodautoscaler::delete),
        )
        .route(
            "/apis/autoscaling/v2/namespaces/:namespace/horizontalpodautoscalers/:name/status",
            get(handlers::horizontalpodautoscaler::get_status)
                .put(handlers::horizontalpodautoscaler::update_status)
                .patch(handlers::horizontalpodautoscaler::update_status),
        )
        // HorizontalPodAutoscalers (all namespaces)
        .route(
            "/apis/autoscaling/v2/horizontalpodautoscalers",
            get(handlers::horizontalpodautoscaler::list_all)
            .delete(handlers::horizontalpodautoscaler::deletecollection_horizontalpodautoscalers),
        )
        // Policy v1 API - PodDisruptionBudgets (namespace-scoped)
        .route(
            "/apis/policy/v1/namespaces/:namespace/poddisruptionbudgets",
            get(handlers::poddisruptionbudget::list)
                .post(handlers::poddisruptionbudget::create)
                .delete(handlers::poddisruptionbudget::deletecollection_poddisruptionbudgets),
        )
        .route(
            "/apis/policy/v1/namespaces/:namespace/poddisruptionbudgets/:name",
            get(handlers::poddisruptionbudget::get)
                .put(handlers::poddisruptionbudget::update)
                .patch(handlers::poddisruptionbudget::patch)
                .delete(handlers::poddisruptionbudget::delete),
        )
        .route(
            "/apis/policy/v1/namespaces/:namespace/poddisruptionbudgets/:name/status",
            get(handlers::poddisruptionbudget::get_status)
                .put(handlers::poddisruptionbudget::update_status)
                .patch(handlers::status::update_status),
        )
        // PodDisruptionBudgets (all namespaces)
        .route(
            "/apis/policy/v1/poddisruptionbudgets",
            get(handlers::poddisruptionbudget::list_all),
        )
        // Storage v1 API - CSIStorageCapacity (namespace-scoped)
        .route(
            "/apis/storage.k8s.io/v1/namespaces/:namespace/csistoragecapacities",
            get(handlers::csistoragecapacity::list_csistoragecapacities)
                .post(handlers::csistoragecapacity::create_csistoragecapacity)
                .delete(handlers::csistoragecapacity::deletecollection_csistoragecapacities),
        )
        .route(
            "/apis/storage.k8s.io/v1/namespaces/:namespace/csistoragecapacities/:name",
            get(handlers::csistoragecapacity::get_csistoragecapacity)
                .put(handlers::csistoragecapacity::update_csistoragecapacity)
                .patch(handlers::csistoragecapacity::patch_csistoragecapacity)
                .delete(handlers::csistoragecapacity::delete_csistoragecapacity),
        )
        // CSIStorageCapacity (all namespaces)
        .route(
            "/apis/storage.k8s.io/v1/csistoragecapacities",
            get(handlers::csistoragecapacity::list_all_csistoragecapacities)
            .delete(handlers::csistoragecapacity::deletecollection_csistoragecapacities),
        )
        // Resource v1 API - ResourceClaims (namespace-scoped)
        .route(
            "/apis/resource.k8s.io/v1/namespaces/:namespace/resourceclaims",
            get(handlers::resourceclaim::list_resourceclaims)
                .post(handlers::resourceclaim::create_resourceclaim)
                .delete(handlers::resourceclaim::deletecollection_resourceclaims),
        )
        .route(
            "/apis/resource.k8s.io/v1/namespaces/:namespace/resourceclaims/:name",
            get(handlers::resourceclaim::get_resourceclaim)
                .put(handlers::resourceclaim::update_resourceclaim)
                .patch(handlers::resourceclaim::patch_resourceclaim)
                .delete(handlers::resourceclaim::delete_resourceclaim),
        )
        .route(
            "/apis/resource.k8s.io/v1/namespaces/:namespace/resourceclaims/:name/status",
            get(handlers::status::get_status)
                .put(handlers::resourceclaim::update_resourceclaim_status)
                .patch(handlers::status::update_status),
        )
        // ResourceClaims (all namespaces)
        .route(
            "/apis/resource.k8s.io/v1/resourceclaims",
            get(handlers::resourceclaim::list_all_resourceclaims)
            .delete(handlers::resourceclaim::deletecollection_resourceclaims),
        )
        // Resource v1 API - ResourceClaimTemplates (namespace-scoped)
        .route(
            "/apis/resource.k8s.io/v1/namespaces/:namespace/resourceclaimtemplates",
            get(handlers::resourceclaimtemplate::list_resourceclaimtemplates)
                .post(handlers::resourceclaimtemplate::create_resourceclaimtemplate)
                .delete(handlers::resourceclaimtemplate::deletecollection_resourceclaimtemplates),
        )
        .route(
            "/apis/resource.k8s.io/v1/namespaces/:namespace/resourceclaimtemplates/:name",
            get(handlers::resourceclaimtemplate::get_resourceclaimtemplate)
                .put(handlers::resourceclaimtemplate::update_resourceclaimtemplate)
                .patch(handlers::resourceclaimtemplate::patch_resourceclaimtemplate)
                .delete(handlers::resourceclaimtemplate::delete_resourceclaimtemplate),
        )
        // ResourceClaimTemplates (all namespaces)
        .route(
            "/apis/resource.k8s.io/v1/resourceclaimtemplates",
            get(handlers::resourceclaimtemplate::list_all_resourceclaimtemplates)
            .delete(handlers::resourceclaimtemplate::deletecollection_resourceclaimtemplates),
        )
        // Resource v1 API - DeviceClasses (cluster-scoped)
        .route(
            "/apis/resource.k8s.io/v1/deviceclasses",
            get(handlers::deviceclass::list_deviceclasses)
                .post(handlers::deviceclass::create_deviceclass)
                .delete(handlers::deviceclass::deletecollection_deviceclasses),
        )
        .route(
            "/apis/resource.k8s.io/v1/deviceclasses/:name",
            get(handlers::deviceclass::get_deviceclass)
                .put(handlers::deviceclass::update_deviceclass)
                .patch(handlers::deviceclass::patch_deviceclass)
                .delete(handlers::deviceclass::delete_deviceclass),
        )
        // Resource v1 API - ResourceSlices (cluster-scoped)
        .route(
            "/apis/resource.k8s.io/v1/resourceslices",
            get(handlers::resourceslice::list_resourceslices)
                .post(handlers::resourceslice::create_resourceslice)
                .delete(handlers::resourceslice::deletecollection_resourceslices),
        )
        .route(
            "/apis/resource.k8s.io/v1/resourceslices/:name",
            get(handlers::resourceslice::get_resourceslice)
                .put(handlers::resourceslice::update_resourceslice)
                .patch(handlers::resourceslice::patch_resourceslice)
                .delete(handlers::resourceslice::delete_resourceslice),
        )
        // Storage v1 API - CSIDrivers (cluster-scoped)
        .route(
            "/apis/storage.k8s.io/v1/csidrivers",
            get(handlers::csidriver::list_csidrivers)
                .post(handlers::csidriver::create_csidriver)
                .delete(handlers::csidriver::deletecollection_csidrivers),
        )
        .route(
            "/apis/storage.k8s.io/v1/csidrivers/:name",
            get(handlers::csidriver::get_csidriver)
                .put(handlers::csidriver::update_csidriver)
                .patch(handlers::csidriver::patch_csidriver)
                .delete(handlers::csidriver::delete_csidriver),
        )
        // Storage v1 API - CSINodes (cluster-scoped)
        .route(
            "/apis/storage.k8s.io/v1/csinodes",
            get(handlers::csinode::list_csinodes)
                .post(handlers::csinode::create_csinode)
                .delete(handlers::csinode::deletecollection_csinodes),
        )
        .route(
            "/apis/storage.k8s.io/v1/csinodes/:name",
            get(handlers::csinode::get_csinode)
                .put(handlers::csinode::update_csinode)
                .patch(handlers::csinode::patch_csinode)
                .delete(handlers::csinode::delete_csinode),
        )
        // Storage v1 API - VolumeAttachments (cluster-scoped)
        .route(
            "/apis/storage.k8s.io/v1/volumeattachments",
            get(handlers::volumeattachment::list_volumeattachments)
                .post(handlers::volumeattachment::create_volumeattachment)
                .delete(handlers::volumeattachment::deletecollection_volumeattachments),
        )
        .route(
            "/apis/storage.k8s.io/v1/volumeattachments/:name",
            get(handlers::volumeattachment::get_volumeattachment)
                .put(handlers::volumeattachment::update_volumeattachment)
                .patch(handlers::volumeattachment::patch_volumeattachment)
                .delete(handlers::volumeattachment::delete_volumeattachment),
        )
        .route(
            "/apis/storage.k8s.io/v1/volumeattachments/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        // Storage v1 API - VolumeAttributesClasses (cluster-scoped)
        .route(
            "/apis/storage.k8s.io/v1/volumeattributesclasses",
            get(handlers::volumeattributesclass::list_volumeattributesclasses)
                .post(handlers::volumeattributesclass::create_volumeattributesclass)
                .delete(handlers::volumeattributesclass::deletecollection_volumeattributesclasses),
        )
        .route(
            "/apis/storage.k8s.io/v1/volumeattributesclasses/:name",
            get(handlers::volumeattributesclass::get_volumeattributesclass)
                .put(handlers::volumeattributesclass::update_volumeattributesclass)
                .patch(handlers::volumeattributesclass::patch_volumeattributesclass)
                .delete(handlers::volumeattributesclass::delete_volumeattributesclass),
        )
        // Admission v1 API - ValidatingAdmissionPolicies (cluster-scoped)
        .route(
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies",
            get(handlers::validating_admission_policy::list_validating_admission_policies)
                .post(handlers::validating_admission_policy::create_validating_admission_policy)
                .delete(handlers::validating_admission_policy::deletecollection_validatingadmissionpolicies),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/:name",
            get(handlers::validating_admission_policy::get_validating_admission_policy)
                .put(handlers::validating_admission_policy::update_validating_admission_policy)
                .patch(handlers::validating_admission_policy::patch_validating_admission_policy)
                .delete(handlers::validating_admission_policy::delete_validating_admission_policy),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        // Admission v1 API - ValidatingAdmissionPolicyBindings (cluster-scoped)
        .route(
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings",
            get(handlers::validating_admission_policy::list_validating_admission_policy_bindings)
                .post(handlers::validating_admission_policy::create_validating_admission_policy_binding)
                .delete(handlers::validating_admission_policy::deletecollection_validatingadmissionpolicybindings),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/:name",
            get(handlers::validating_admission_policy::get_validating_admission_policy_binding)
                .put(handlers::validating_admission_policy::update_validating_admission_policy_binding)
                .patch(handlers::validating_admission_policy::patch_validating_admission_policy_binding)
                .delete(handlers::validating_admission_policy::delete_validating_admission_policy_binding),
        )
        // Networking v1 API - ServiceCIDRs (cluster-scoped)
        .route(
            "/apis/networking.k8s.io/v1/servicecidrs",
            get(handlers::servicecidr::list_servicecidrs)
                .post(handlers::servicecidr::create_servicecidr)
                .delete(handlers::servicecidr::deletecollection_servicecidrs),
        )
        .route(
            "/apis/networking.k8s.io/v1/servicecidrs/:name",
            get(handlers::servicecidr::get_servicecidr)
                .put(handlers::servicecidr::update_servicecidr)
                .patch(handlers::servicecidr::patch_servicecidr)
                .delete(handlers::servicecidr::delete_servicecidr),
        )
        .route(
            "/apis/networking.k8s.io/v1/watch/servicecidrs",
            get(handlers::watch::watch_servicecidrs),
        )
        .route(
            "/apis/networking.k8s.io/v1/servicecidrs/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        // Networking v1 API - IPAddresses (cluster-scoped)
        .route(
            "/apis/networking.k8s.io/v1/ipaddresses",
            get(handlers::ipaddress::list_ipaddresses)
                .post(handlers::ipaddress::create_ipaddress)
                .delete(handlers::ipaddress::deletecollection_ipaddresses),
        )
        .route(
            "/apis/networking.k8s.io/v1/ipaddresses/:name",
            get(handlers::ipaddress::get_ipaddress)
                .put(handlers::ipaddress::update_ipaddress)
                .patch(handlers::ipaddress::patch_ipaddress)
                .delete(handlers::ipaddress::delete_ipaddress),
        )
        .route(
            "/apis/networking.k8s.io/v1/watch/ipaddresses",
            get(handlers::watch::watch_ipaddresses),
        )
        .route(
            "/apis/networking.k8s.io/v1/ipaddresses/:name/status",
            get(handlers::status::get_cluster_status)
                .put(handlers::status::update_cluster_status)
                .patch(handlers::status::update_cluster_status),
        )
        // Networking v1 API - IngressClasses (cluster-scoped)
        .route(
            "/apis/networking.k8s.io/v1/ingressclasses",
            get(handlers::ingressclass::list_ingressclasses)
                .post(handlers::ingressclass::create_ingressclass)
                .delete(handlers::ingressclass::deletecollection_ingressclasses),
        )
        .route(
            "/apis/networking.k8s.io/v1/ingressclasses/:name",
            get(handlers::ingressclass::get_ingressclass)
                .put(handlers::ingressclass::update_ingressclass)
                .patch(handlers::ingressclass::patch_ingressclass)
                .delete(handlers::ingressclass::delete_ingressclass),
        )
        // Node v1 API - RuntimeClasses (cluster-scoped)
        .route(
            "/apis/node.k8s.io/v1/runtimeclasses",
            get(handlers::runtimeclass::list_runtimeclasses)
                .post(handlers::runtimeclass::create_runtimeclass)
                .delete(handlers::runtimeclass::deletecollection_runtimeclasses),
        )
        .route(
            "/apis/node.k8s.io/v1/runtimeclasses/:name",
            get(handlers::runtimeclass::get_runtimeclass)
                .put(handlers::runtimeclass::update_runtimeclass)
                .patch(handlers::runtimeclass::patch_runtimeclass)
                .delete(handlers::runtimeclass::delete_runtimeclass),
        )
        .route(
            "/apis/node.k8s.io/v1/watch/runtimeclasses",
            get(handlers::watch::watch_runtimeclasses),
        )
        // Core v1 API - PodTemplates (namespace-scoped)
        .route(
            "/api/v1/namespaces/:namespace/podtemplates",
            get(handlers::podtemplate::list_podtemplates)
                .post(handlers::podtemplate::create_podtemplate)
                .delete(handlers::podtemplate::deletecollection_podtemplates),
        )
        .route(
            "/api/v1/namespaces/:namespace/podtemplates/:name",
            get(handlers::podtemplate::get_podtemplate)
                .put(handlers::podtemplate::update_podtemplate)
                .patch(handlers::podtemplate::patch_podtemplate)
                .delete(handlers::podtemplate::delete_podtemplate),
        )
        // PodTemplates (all namespaces)
        .route(
            "/api/v1/podtemplates",
            get(handlers::podtemplate::list_all_podtemplates),
        )
        // Core v1 API - ReplicationControllers (namespace-scoped)
        .route(
            "/api/v1/namespaces/:namespace/replicationcontrollers",
            get(handlers::replicationcontroller::list_replicationcontrollers)
                .post(handlers::replicationcontroller::create_replicationcontroller)
                .delete(handlers::replicationcontroller::deletecollection_replicationcontrollers),
        )
        .route(
            "/api/v1/namespaces/:namespace/replicationcontrollers/:name",
            get(handlers::replicationcontroller::get_replicationcontroller)
                .put(handlers::replicationcontroller::update_replicationcontroller)
                .patch(handlers::replicationcontroller::patch_replicationcontroller)
                .delete(handlers::replicationcontroller::delete_replicationcontroller),
        )
        .route(
            "/api/v1/namespaces/:namespace/replicationcontrollers/:name/status",
            get(handlers::status::get_status)
                .put(handlers::status::update_status)
                .patch(handlers::status::update_status),
        )
        .route(
            "/api/v1/namespaces/:namespace/replicationcontrollers/:name/scale",
            get(handlers::scale::get_scale)
                .put(handlers::scale::update_scale)
                .patch(handlers::scale::patch_scale),
        )
        // ReplicationControllers (all namespaces)
        .route(
            "/api/v1/replicationcontrollers",
            get(handlers::replicationcontroller::list_all_replicationcontrollers)
            .delete(handlers::replicationcontroller::deletecollection_replicationcontrollers),
        )
        // Apps v1 API - ControllerRevisions (namespace-scoped)
        .route(
            "/apis/apps/v1/namespaces/:namespace/controllerrevisions",
            get(handlers::controllerrevision::list_controllerrevisions)
                .post(handlers::controllerrevision::create_controllerrevision)
                .delete(handlers::controllerrevision::deletecollection_controllerrevisions),
        )
        .route(
            "/apis/apps/v1/namespaces/:namespace/controllerrevisions/:name",
            get(handlers::controllerrevision::get_controllerrevision)
                .put(handlers::controllerrevision::update_controllerrevision)
                .patch(handlers::controllerrevision::patch_controllerrevision)
                .delete(handlers::controllerrevision::delete_controllerrevision),
        )
        // ControllerRevisions (all namespaces)
        .route(
            "/apis/apps/v1/controllerrevisions",
            get(handlers::controllerrevision::list_all_controllerrevisions)
            .delete(handlers::controllerrevision::deletecollection_controllerrevisions),
        )
        // Authentication API - authentication.k8s.io/v1
        .route(
            "/apis/authentication.k8s.io/v1/tokenreviews",
            post(handlers::authentication::create_token_review),
        )
        .route(
            "/apis/authentication.k8s.io/v1/selfsubjectreviews",
            post(handlers::authentication::create_self_subject_review),
        )
        .route(
            "/api/v1/namespaces/:namespace/serviceaccounts/:service_account_name/token",
            post(handlers::authentication::create_token_request),
        )
        // Authorization API - authorization.k8s.io/v1
        .route(
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            post(handlers::authorization::create_subject_access_review),
        )
        .route(
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            post(handlers::authorization::create_self_subject_access_review),
        )
        .route(
            "/apis/authorization.k8s.io/v1/namespaces/:namespace/localsubjectaccessreviews",
            post(handlers::authorization::create_local_subject_access_review),
        )
        .route(
            "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
            post(handlers::authorization::create_self_subject_rules_review),
        )
        // Metrics API - metrics.k8s.io/v1beta1
        .route(
            "/apis/metrics.k8s.io/v1beta1/nodes/:name",
            get(handlers::metrics::get_node_metrics),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1/nodes",
            get(handlers::metrics::list_node_metrics),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1/namespaces/:namespace/pods/:name",
            get(handlers::metrics::get_pod_metrics),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1/namespaces/:namespace/pods",
            get(handlers::metrics::list_pod_metrics),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1/pods",
            get(handlers::metrics::list_all_pod_metrics),
        )
        // Custom Metrics API - custom.metrics.k8s.io/v1beta2
        .route(
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/:namespace/:resource/:name/:metric",
            get(handlers::custom_metrics::get_custom_metric),
        )
        .route(
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/:namespace/:resource/:metric",
            get(handlers::custom_metrics::list_custom_metrics),
        )
        .route(
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/:namespace/metrics/:metric",
            get(handlers::custom_metrics::get_namespace_metric),
        )
        .route(
            "/apis/custom.metrics.k8s.io/v1beta2/:resource/:name/:metric",
            get(handlers::custom_metrics::get_cluster_metric),
        )
        // Watch routes for remaining resource types
        .route(
            "/apis/apiextensions.k8s.io/v1/watch/customresourcedefinitions",
            get(handlers::watch::watch_crds),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/watch/validatingwebhookconfigurations",
            get(handlers::watch::watch_validatingwebhookconfigurations),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/watch/mutatingwebhookconfigurations",
            get(handlers::watch::watch_mutatingwebhookconfigurations),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/watch/validatingadmissionpolicies",
            get(handlers::watch::watch_validatingadmissionpolicies),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/watch/validatingadmissionpolicybindings",
            get(handlers::watch::watch_validatingadmissionpolicybindings),
        )
        .route(
            "/apis/policy/v1/watch/namespaces/:namespace/poddisruptionbudgets",
            get(handlers::watch::watch_poddisruptionbudgets),
        )
        .route(
            "/apis/policy/v1/watch/poddisruptionbudgets",
            get(handlers::watch::watch_poddisruptionbudgets_all),
        )
        .route(
            "/api/v1/watch/namespaces/:namespace/limitranges",
            get(handlers::watch::watch_limitranges),
        )
        .route(
            "/api/v1/watch/namespaces/:namespace/replicationcontrollers",
            get(handlers::watch::watch_replicationcontrollers),
        )
        .route(
            "/apis/scheduling.k8s.io/v1/watch/priorityclasses",
            get(handlers::watch::watch_priorityclasses),
        )
        .route(
            "/apis/storage.k8s.io/v1/watch/storageclasses",
            get(handlers::watch::watch_storageclasses),
        )
        .route(
            "/apis/autoscaling/v1/watch/namespaces/:namespace/horizontalpodautoscalers",
            get(handlers::watch::watch_horizontalpodautoscalers),
        )
        .route(
            "/apis/autoscaling/v2/watch/namespaces/:namespace/horizontalpodautoscalers",
            get(handlers::watch::watch_horizontalpodautoscalers),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1/watch/clusterroles",
            get(handlers::watch::watch_clusterroles),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1/watch/clusterrolebindings",
            get(handlers::watch::watch_clusterrolebindings),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1/watch/namespaces/:namespace/roles",
            get(handlers::watch::watch_roles),
        )
        .route(
            "/apis/rbac.authorization.k8s.io/v1/watch/namespaces/:namespace/rolebindings",
            get(handlers::watch::watch_rolebindings),
        )
        .route(
            "/apis/coordination.k8s.io/v1/watch/namespaces/:namespace/leases",
            get(handlers::watch::watch_leases),
        )
        .route(
            "/apis/networking.k8s.io/v1/watch/namespaces/:namespace/ingresses",
            get(handlers::watch::watch_ingresses),
        )
        .route(
            "/apis/networking.k8s.io/v1/watch/namespaces/:namespace/networkpolicies",
            get(handlers::watch::watch_networkpolicies),
        )
        .route(
            "/apis/certificates.k8s.io/v1/watch/certificatesigningrequests",
            get(handlers::watch::watch_certificatesigningrequests),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/watch/flowschemas",
            get(handlers::watch::watch_flowschemas),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/watch/prioritylevelconfigurations",
            get(handlers::watch::watch_prioritylevelconfigurations),
        )
        .route(
            "/api/v1/watch/namespaces/:namespace/podtemplates",
            get(handlers::watch::watch_podtemplates),
        )
        .route(
            "/apis/apps/v1/watch/namespaces/:namespace/controllerrevisions",
            get(handlers::watch::watch_controllerrevisions),
        )
        // CRD fallback — must be inside protected_routes so auth middleware applies
        .fallback(custom_resource_fallback);

    // Conditionally apply authentication middleware
    if skip_auth {
        // In skip-auth mode, inject a default admin user context
        protected_routes = protected_routes
            .layer(axum_middleware::from_fn(
                middleware::normalize_content_type_middleware,
            ))
            .layer(axum_middleware::from_fn(middleware::skip_auth_middleware))
            // The impersonation gate inside skip_auth_middleware authorizes the
            // `impersonate` verb against this authorizer.
            .layer(Extension(state.authorizer.clone()));
    } else {
        // In normal mode, apply full authentication
        protected_routes = protected_routes
            .layer(axum_middleware::from_fn(
                middleware::normalize_content_type_middleware,
            ))
            .layer(axum_middleware::from_fn(middleware::auth_middleware))
            .layer(Extension(state.token_manager.clone()))
            .layer(Extension(state.bootstrap_token_manager.clone()))
            .layer(Extension(state.storage.clone()))
            .layer(Extension(state.authorizer.clone()));
    }

    // Combine routes and add shared state.
    // K8s (Go) treats /path and /path/ identically. Axum doesn't, so we
    // normalize URIs by stripping trailing slashes before routing.
    let mut app = Router::new().merge(public_routes).merge(protected_routes);

    // Serve the console SPA at /console/ when a console directory is configured.
    if let Some(dir) = console_dir {
        info!("Console UI enabled, serving from {:?} at /console/", dir);
        let index_file = dir.join("index.html");
        let serve_dir = ServeDir::new(dir).fallback(ServeFile::new(&index_file));
        app = app.nest_service("/console", serve_dir);
    }

    app.layer(axum_middleware::map_request(
        |mut req: axum::extract::Request| async move {
            let path = req.uri().path();
            // Strip trailing slash for non-root paths (but not /console/ paths,
            // which are handled by ServeDir and need trailing slashes intact)
            if path.len() > 1 && path.ends_with('/') && !path.starts_with("/console") {
                let new_path = path.trim_end_matches('/');
                if let Ok(new_uri) = axum::http::Uri::builder()
                    .path_and_query(if let Some(q) = req.uri().query() {
                        format!("{}?{}", new_path, q)
                    } else {
                        new_path.to_string()
                    })
                    .build()
                {
                    *req.uri_mut() = new_uri;
                }
            }
            req
        },
    ))
    .layer(TraceLayer::new_for_http())
    .with_state(state)
    .layer(axum::middleware::from_fn(
        crate::middleware::capture_payload,
    ))
}
