use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::Lease,
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
    Json(mut lease): Json<Lease>,
) -> Result<(StatusCode, Json<Lease>)> {
    info!("Creating lease: {}/{}", namespace, lease.metadata.name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "leases")
        .with_namespace(&namespace)
        .with_api_group("coordination.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    lease.metadata.namespace = Some(namespace.clone());
    lease.metadata.ensure_uid();
    lease.metadata.ensure_creation_timestamp();

    let key = build_key("leases", Some(&namespace), &lease.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Lease {}/{} validated successfully (not created)",
            namespace, lease.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(lease)));
    }

    let created = state.storage.create(&key, &lease).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Lease>> {
    debug!("Getting lease: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "leases")
        .with_namespace(&namespace)
        .with_api_group("coordination.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("leases", Some(&namespace), &name);
    let lease = state.storage.get(&key).await?;

    Ok(Json(lease))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut lease): Json<Lease>,
) -> Result<Json<Lease>> {
    info!("Updating lease: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "leases")
        .with_namespace(&namespace)
        .with_api_group("coordination.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    lease.metadata.name = name.clone();
    lease.metadata.namespace = Some(namespace.clone());

    let key = build_key("leases", Some(&namespace), &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Lease {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(lease));
    }

    let result = match state.storage.update(&key, &lease).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &lease).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_lease(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Lease>> {
    info!("Deleting lease: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "leases")
        .with_namespace(&namespace)
        .with_api_group("coordination.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("leases", Some(&namespace), &name);

    // Get the lease for finalizer handling
    let lease: Lease = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Lease {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(lease));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &lease)
            .await?;

    if deleted_immediately {
        Ok(Json(lease))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Lease = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
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
        return crate::handlers::watch::watch_namespaced::<Lease>(
            state,
            auth_ctx,
            namespace,
            "leases",
            "coordination.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing leases in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "leases")
        .with_namespace(&namespace)
        .with_api_group("coordination.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("leases", Some(&namespace));
    let mut leases: Vec<Lease> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut leases, &params)?;

    let list = List::new("LeaseList", "coordination.k8s.io/v1", leases);
    Ok(Json(list).into_response())
}

/// List all leases across all namespaces
pub async fn list_all_leases(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
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
        return crate::handlers::watch::watch_cluster_scoped::<Lease>(
            state,
            auth_ctx,
            "leases",
            "coordination.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all leases");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "leases")
        .with_api_group("coordination.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("leases", None);
    let mut leases = state.storage.list::<Lease>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut leases, &params)?;

    let list = List::new("LeaseList", "coordination.k8s.io/v1", leases);
    Ok(Json(list).into_response())
}

crate::patch_handler_namespaced!(patch, Lease, "leases", "coordination.k8s.io");

pub async fn deletecollection_leases(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection leases in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "leases")
        .with_namespace(&namespace)
        .with_api_group("coordination.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Lease collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all leases in the namespace
    let prefix = build_prefix("leases", Some(&namespace));
    let mut items = state.storage.list::<Lease>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("leases", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} leases deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
