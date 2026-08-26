use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::Deployment,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<(StatusCode, HeaderMap, Json<Deployment>)> {
    // Parse the body manually so we can do strict field validation against the
    // raw bytes. In any mode that may need to report duplicate keys (Strict —
    // now the K8s 1.25+ default — or Warn) we re-parse via serde_json::Value
    // so validate_strict_fields can report unknown + duplicate issues in the
    // canonical `strict decoding error: ...` format.
    let mut deployment: Deployment = match serde_json::from_slice(&body) {
        Ok(d) => d,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("duplicate field") {
                let value: serde_json::Value = serde_json::from_slice(&body).map_err(|e2| {
                    rusternetes_common::Error::BadRequest(format!("failed to decode: {}", e2))
                })?;
                serde_json::from_value(value).map_err(|e2| {
                    rusternetes_common::Error::BadRequest(format!("failed to decode: {}", e2))
                })?
            } else {
                return Err(rusternetes_common::Error::BadRequest(format!(
                    "failed to decode: {}",
                    msg
                )));
            }
        }
    };

    info!(
        "Creating deployment: {}/{}",
        namespace, deployment.metadata.name
    );

    // Strict field validation: reject unknown fields when requested. Warn
    // mode surfaces warnings as `Warning: 299` response headers (per RFC
    // 7234), matching upstream apimachinery util/warning behaviour.
    let warnings =
        crate::handlers::validation::validate_strict_fields(&params, &body, &deployment)?;
    let response_headers = build_warning_headers(&warnings);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "deployments")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    deployment.metadata.namespace = Some(namespace.clone());

    // Run ValidatingAdmissionPolicy checks
    let deploy_value = serde_json::to_value(&deployment).ok();
    let gvk = rusternetes_common::admission::GroupVersionKind {
        group: "apps".to_string(),
        version: "v1".to_string(),
        kind: "Deployment".to_string(),
    };
    state
        .webhook_manager
        .run_validating_admission_policies_ext(
            &rusternetes_common::admission::Operation::Create,
            &gvk,
            deploy_value.as_ref(),
            None,
            Some("deployments"),
            Some(&namespace),
        )
        .await?;

    deployment.metadata.ensure_uid();
    deployment.metadata.ensure_creation_timestamp();
    crate::handlers::lifecycle::set_initial_generation(&mut deployment.metadata);

    // Apply K8s defaults (SetDefaults_Deployment + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_deployment_defaults(&mut deployment);

    // Set initial revision annotation if not already present
    let annotations = deployment
        .metadata
        .annotations
        .get_or_insert_with(std::collections::HashMap::new);
    annotations
        .entry("deployment.kubernetes.io/revision".to_string())
        .or_insert_with(|| "1".to_string());

    let key = build_key("deployments", Some(&namespace), &deployment.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Deployment {}/{} validated successfully (not created)",
            namespace, deployment.metadata.name
        );
        return Ok((StatusCode::CREATED, response_headers, Json(deployment)));
    }

    let created = state.storage.create(&key, &deployment).await?;

    Ok((StatusCode::CREATED, response_headers, Json(created)))
}

/// Convert the `validate_strict_fields` warning strings into a `HeaderMap`
/// holding RFC 7234 `Warning: 299 - "..."` entries. Empty input → empty map,
/// which keeps response shape identical for non-Warn callers.
fn build_warning_headers(warnings: &[String]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for warning in warnings {
        let value = crate::handlers::validation::format_warning_header(warning);
        if let Ok(hv) = axum::http::HeaderValue::from_str(&value) {
            headers.append(axum::http::header::WARNING, hv);
        }
    }
    headers
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Deployment>> {
    debug!("Getting deployment: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "deployments")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("deployments", Some(&namespace), &name);
    let deployment = state.storage.get(&key).await?;

    Ok(Json(deployment))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<Deployment>> {
    let mut deployment: Deployment = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!("Updating deployment: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "deployments")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    deployment.metadata.name = name.clone();
    deployment.metadata.namespace = Some(namespace.clone());
    // Ensure TypeMeta — clients may omit kind/apiVersion in PUT body
    if deployment.type_meta.kind.is_empty() {
        deployment.type_meta.kind = "Deployment".to_string();
    }
    if deployment.type_meta.api_version.is_empty() {
        deployment.type_meta.api_version = "apps/v1".to_string();
    }

    // Apply K8s defaults (SetDefaults_Deployment + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_deployment_defaults(&mut deployment);

    let key = build_key("deployments", Some(&namespace), &name);

    // Get the old deployment for concurrency control and generation tracking
    let old_deployment: Deployment = state.storage.get(&key).await?;

    // Check resourceVersion for optimistic concurrency control
    crate::handlers::lifecycle::check_resource_version(
        old_deployment.metadata.resource_version.as_deref(),
        deployment.metadata.resource_version.as_deref(),
        &name,
    )?;

    // Increment generation if spec changed
    let old_value = serde_json::to_value(&old_deployment)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    let new_value = serde_json::to_value(&deployment)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    crate::handlers::lifecycle::maybe_increment_generation(
        &old_value,
        &new_value,
        &mut deployment.metadata,
    );

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Deployment {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(deployment));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &deployment).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &deployment).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_deployment(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Json<Deployment>> {
    info!("Deleting deployment: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "deployments")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("deployments", Some(&namespace), &name);

    // Get the deployment to check for finalizers
    let deployment: Deployment = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Deployment {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(deployment));
    }

    // Extract propagation policy from query params or request body (DeleteOptions)
    let body_propagation: Option<String> = if !body.is_empty() {
        serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| {
                v.get("propagationPolicy")
                    .and_then(|p| p.as_str())
                    .map(|s| s.to_string())
            })
    } else {
        None
    };
    let propagation_policy = params
        .get("propagationPolicy")
        .map(|s| s.as_str())
        .or(body_propagation.as_deref());

    // Handle deletion with finalizers and propagation policy
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers_and_propagation(
            &state.storage,
            &key,
            &deployment,
            propagation_policy,
        )
        .await?;

    if deleted_immediately {
        Ok(Json(deployment))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Deployment = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
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
        return crate::handlers::watch::watch_namespaced::<Deployment>(
            state,
            auth_ctx,
            namespace,
            "deployments",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing deployments in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "deployments")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("deployments", Some(&namespace));
    let mut deployments: Vec<Deployment> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut deployments, &params)?;

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            deployments,
            Some(resource_version.to_string()),
            "Deployment",
        );
        return Ok(axum::Json(table).into_response());
    }

    let list = List::new("DeploymentList", "apps/v1", deployments);
    Ok(Json(list).into_response())
}

/// List all deployments across all namespaces
pub async fn list_all_deployments(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
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
        return crate::handlers::watch::watch_cluster_scoped::<Deployment>(
            state,
            auth_ctx,
            "deployments",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing all deployments");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "deployments").with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("deployments", None);
    let mut deployments = state.storage.list::<Deployment>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut deployments, &params)?;

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            deployments,
            Some(resource_version.to_string()),
            "Deployment",
        );
        return Ok(axum::Json(table).into_response());
    }

    let list = List::new("DeploymentList", "apps/v1", deployments);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, Deployment, "deployments", "apps");

pub async fn deletecollection_deployments(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection deployments in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "deployments")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Deployment collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all deployments in the namespace
    let prefix = build_prefix("deployments", Some(&namespace));
    let mut items = state.storage.list::<Deployment>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("deployments", Some(&namespace), &item.metadata.name);

        // Handle deletion with finalizers
        let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &item,
        )
        .await?;

        if deleted_immediately {
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} deployments deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
