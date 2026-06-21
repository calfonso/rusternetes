use crate::{handlers::watch::WatchParams, middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::Endpoints,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Create endpoints
pub async fn create_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut endpoints): DumpingJson<Endpoints>,
) -> Result<(StatusCode, Json<Endpoints>)> {
    info!(
        "Creating endpoints: {}/{}",
        namespace, endpoints.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "endpoints")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &endpoints.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Field validation (mirrors upstream ValidateEndpoints).
    {
        let errs = rusternetes_common::validation::endpoints::validate_endpoints(&endpoints);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    endpoints.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    endpoints.metadata.ensure_uid();
    endpoints.metadata.ensure_creation_timestamp();

    let key = build_key("endpoints", Some(&namespace), &endpoints.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Endpoints {}/{} validated successfully (not created)",
            namespace, endpoints.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(endpoints)));
    }

    let created = state.storage.create(&key, &endpoints).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

/// Get endpoints
pub async fn get_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Endpoints>> {
    debug!("Getting endpoints: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "endpoints")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("endpoints", Some(&namespace), &name);
    let endpoints = state.storage.get(&key).await?;

    Ok(Json(endpoints))
}

/// List endpoints in namespace
pub async fn list_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    debug!("Listing endpoints in namespace: {}", namespace);

    // Check if this is a watch request
    if params.watch.unwrap_or(false) {
        return crate::handlers::watch::watch_endpoints(
            State(state),
            Extension(auth_ctx),
            Path(namespace),
            Query(params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "endpoints")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("endpoints", Some(&namespace));
    let mut endpoints = state.storage.list::<Endpoints>(&prefix).await?;

    // Apply field and label selector filtering
    let mut params_map = HashMap::new();
    if let Some(fs) = params.field_selector {
        params_map.insert("fieldSelector".to_string(), fs);
    }
    if let Some(ls) = params.label_selector {
        params_map.insert("labelSelector".to_string(), ls);
    }
    crate::handlers::filtering::apply_selectors(&mut endpoints, &params_map)?;

    let mut list = List::new("EndpointsList", "v1", endpoints);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all endpoints across all namespaces
pub async fn list_all_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    debug!("Listing all endpoints");

    // Check if this is a watch request
    if params.watch.unwrap_or(false) {
        return crate::handlers::watch::watch_cluster_scoped::<Endpoints>(
            state,
            auth_ctx,
            "endpoints",
            "",
            params,
        )
        .await;
    }

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "endpoints").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("endpoints", None);
    let mut endpoints = state.storage.list::<Endpoints>(&prefix).await?;

    // Apply field and label selector filtering
    let mut params_map = HashMap::new();
    if let Some(fs) = params.field_selector {
        params_map.insert("fieldSelector".to_string(), fs);
    }
    if let Some(ls) = params.label_selector {
        params_map.insert("labelSelector".to_string(), ls);
    }
    crate::handlers::filtering::apply_selectors(&mut endpoints, &params_map)?;

    let mut list = List::new("EndpointsList", "v1", endpoints);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// Update endpoints
pub async fn update_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut endpoints): DumpingJson<Endpoints>,
) -> Result<Json<Endpoints>> {
    info!("Updating endpoints: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "endpoints")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    endpoints.metadata.name = name.clone();
    endpoints.metadata.namespace = Some(namespace.clone());

    let key = build_key("endpoints", Some(&namespace), &name);

    // Field validation on update (upstream ValidateEndpointsUpdate re-runs
    // ValidateEndpoints on the new object). The create path validated but the
    // update path previously persisted PUTs unchecked.
    {
        let errs = rusternetes_common::validation::endpoints::validate_endpoints(&endpoints);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Endpoints {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(endpoints));
    }

    let updated = state.storage.update(&key, &endpoints).await?;

    Ok(Json(updated))
}

/// Delete endpoints
pub async fn delete_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Endpoints>> {
    info!("Deleting endpoints: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "endpoints")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("endpoints", Some(&namespace), &name);

    // Get the resource to check if it exists
    let endpoints: Endpoints = state.storage.get(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=endpoints).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "Endpoints",
        "endpoints",
        Some(&namespace),
        &name,
        &endpoints,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Endpoints {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(endpoints));
    }

    let has_finalizers = crate::handlers::finalizers::handle_delete_with_finalizers(
        &*state.storage,
        &key,
        &endpoints,
    )
    .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Endpoints = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(endpoints))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch_endpoints, Endpoints, "endpoints", "");

pub async fn deletecollection_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection endpoints in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "endpoints")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Endpoints collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all endpoints in the namespace
    let prefix = build_prefix("endpoints", Some(&namespace));
    let mut items = state.storage.list::<Endpoints>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("endpoints", Some(&namespace), &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "Endpoints",
            "endpoints",
            Some(&namespace),
            &item.metadata.name,
            &item,
            &user_for_webhook,
            false,
        )
        .await?;

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
        "DeleteCollection completed: {} endpoints deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
