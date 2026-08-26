use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::DaemonSet,
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
) -> Result<(StatusCode, Json<DaemonSet>)> {
    let mut daemonset: DaemonSet = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!(
        "Creating daemonset: {}/{}",
        namespace, daemonset.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "daemonsets")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    daemonset.metadata.namespace = Some(namespace.clone());
    daemonset.metadata.ensure_uid();
    daemonset.metadata.ensure_creation_timestamp();

    // Apply K8s defaults (SetDefaults_DaemonSet + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_daemonset_defaults(&mut daemonset);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: DaemonSet {}/{} validated successfully (not created)",
            namespace, daemonset.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(daemonset)));
    }

    let key = build_key("daemonsets", Some(&namespace), &daemonset.metadata.name);
    let created = state.storage.create(&key, &daemonset).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<DaemonSet>> {
    debug!("Getting daemonset: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "daemonsets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("daemonsets", Some(&namespace), &name);
    let daemonset = state.storage.get(&key).await?;

    Ok(Json(daemonset))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<DaemonSet>> {
    let mut daemonset: DaemonSet = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!("Updating daemonset: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "daemonsets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    daemonset.metadata.name = name.clone();
    daemonset.metadata.namespace = Some(namespace.clone());

    // Apply K8s defaults (SetDefaults_DaemonSet + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_daemonset_defaults(&mut daemonset);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: DaemonSet {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(daemonset));
    }

    let key = build_key("daemonsets", Some(&namespace), &name);

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &daemonset).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &daemonset).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_daemonset(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<DaemonSet>> {
    info!("Deleting daemonset: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "daemonsets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("daemonsets", Some(&namespace), &name);

    // Get the resource to check if it exists
    let daemonset: DaemonSet = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: DaemonSet {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(daemonset));
    }

    let propagation_policy = params.get("propagationPolicy").map(|s| s.as_str());
    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers_and_propagation(
            &*state.storage,
            &key,
            &daemonset,
            propagation_policy,
        )
        .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: DaemonSet = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(daemonset))
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
        return crate::handlers::watch::watch_namespaced::<DaemonSet>(
            state,
            auth_ctx,
            namespace,
            "daemonsets",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing daemonsets in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "daemonsets")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("daemonsets", Some(&namespace));
    let mut daemonsets: Vec<DaemonSet> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut daemonsets, &params)?;

    let list = List::new("DaemonSetList", "apps/v1", daemonsets);
    Ok(Json(list).into_response())
}

/// List all daemonsets across all namespaces
pub async fn list_all_daemonsets(
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
        return crate::handlers::watch::watch_cluster_scoped::<DaemonSet>(
            state,
            auth_ctx,
            "daemonsets",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing all daemonsets");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "daemonsets").with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("daemonsets", None);
    let mut daemonsets = state.storage.list::<DaemonSet>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut daemonsets, &params)?;

    let list = List::new("DaemonSetList", "apps/v1", daemonsets);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, DaemonSet, "daemonsets", "apps");

pub async fn deletecollection_daemonsets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection daemonsets in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "daemonsets")
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
        info!("Dry-run: DaemonSet collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all daemonsets in the namespace
    let prefix = build_prefix("daemonsets", Some(&namespace));
    let mut items = state.storage.list::<DaemonSet>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("daemonsets", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} daemonsets deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
