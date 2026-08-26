use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::CronJob,
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
    Json(mut cronjob): Json<CronJob>,
) -> Result<(StatusCode, Json<CronJob>)> {
    info!("Creating cronjob: {}/{}", namespace, cronjob.metadata.name);

    // Check if this is a dry-run request
    let _is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "cronjobs")
        .with_namespace(&namespace)
        .with_api_group("batch");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    cronjob.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    cronjob.metadata.ensure_uid();
    cronjob.metadata.ensure_creation_timestamp();

    // Apply K8s defaults (SetDefaults_PodSpec + SetDefaults_Container for job template)
    crate::handlers::defaults::apply_cronjob_defaults(&mut cronjob);

    let key = build_key("cronjobs", Some(&namespace), &cronjob.metadata.name);
    let created = state.storage.create(&key, &cronjob).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<CronJob>> {
    debug!("Getting cronjob: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "cronjobs")
        .with_namespace(&namespace)
        .with_api_group("batch")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("cronjobs", Some(&namespace), &name);
    let cronjob = state.storage.get(&key).await?;

    Ok(Json(cronjob))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut cronjob): Json<CronJob>,
) -> Result<Json<CronJob>> {
    info!("Updating cronjob: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "cronjobs")
        .with_namespace(&namespace)
        .with_api_group("batch")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    cronjob.metadata.name = name.clone();
    cronjob.metadata.namespace = Some(namespace.clone());

    // Apply K8s defaults (SetDefaults_PodSpec + SetDefaults_Container for job template)
    crate::handlers::defaults::apply_cronjob_defaults(&mut cronjob);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: CronJob {:}/{:} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(cronjob));
    }
    let key = build_key("cronjobs", Some(&namespace), &name);
    let updated = state.storage.update(&key, &cronjob).await?;

    Ok(Json(updated))
}

pub async fn delete_cronjob(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<CronJob>> {
    info!("Deleting cronjob: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "cronjobs")
        .with_namespace(&namespace)
        .with_api_group("batch")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("cronjobs", Some(&namespace), &name);

    // Get the resource to check if it exists
    let cronjob: CronJob = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: CronJob {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(cronjob));
    }

    let propagation_policy = params.get("propagationPolicy").map(|s| s.as_str());
    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers_and_propagation(
            &*state.storage,
            &key,
            &cronjob,
            propagation_policy,
        )
        .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: CronJob = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(cronjob))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
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
        return crate::handlers::watch::watch_namespaced::<CronJob>(
            state,
            auth_ctx,
            namespace,
            "cronjobs",
            "batch",
            watch_params,
        )
        .await;
    }

    debug!("Listing cronjobs in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "cronjobs")
        .with_namespace(&namespace)
        .with_api_group("batch");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("cronjobs", Some(&namespace));
    let mut cronjobs: Vec<CronJob> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut cronjobs, &params)?;

    let list = List::new("CronJobList", "batch/v1", cronjobs);
    Ok(Json(list).into_response())
}

/// List all cronjobs across all namespaces
pub async fn list_all_cronjobs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
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
        return crate::handlers::watch::watch_cluster_scoped::<CronJob>(
            state,
            auth_ctx,
            "cronjobs",
            "batch",
            watch_params,
        )
        .await;
    }

    debug!("Listing all cronjobs");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "cronjobs").with_api_group("batch");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("cronjobs", None);
    let mut cronjobs = state.storage.list::<CronJob>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut cronjobs, &params)?;

    let list = List::new("CronJobList", "batch/v1", cronjobs);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, CronJob, "cronjobs", "batch");

pub async fn deletecollection_cronjobs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection cronjobs in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "cronjobs")
        .with_namespace(&namespace)
        .with_api_group("batch");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CronJob collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all cronjobs in the namespace
    let prefix = build_prefix("cronjobs", Some(&namespace));
    let mut items = state.storage.list::<CronJob>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("cronjobs", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} cronjobs deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
