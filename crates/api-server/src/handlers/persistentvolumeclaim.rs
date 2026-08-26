use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::PersistentVolumeClaim,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_pvc(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut pvc): Json<PersistentVolumeClaim>,
) -> Result<(StatusCode, Json<PersistentVolumeClaim>)> {
    info!(
        "Creating PersistentVolumeClaim: {}/{}",
        namespace, pvc.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    pvc.metadata.namespace = Some(namespace.clone());

    // Apply DefaultStorageClass admission (sets default storage class if not specified)
    if let Err(e) = crate::admission::set_default_storage_class(&state.storage, &mut pvc).await {
        tracing::warn!(
            "Error applying DefaultStorageClass admission for PVC {}/{}: {}",
            namespace,
            pvc.metadata.name,
            e
        );
        // Continue anyway - don't fail PVC creation if default storage class can't be set
    }

    pvc.metadata.ensure_uid();
    pvc.metadata.ensure_creation_timestamp();

    let key = build_key(
        "persistentvolumeclaims",
        Some(&namespace),
        &pvc.metadata.name,
    );

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolumeClaim {}/{} validated successfully (not created)",
            namespace, pvc.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(pvc)));
    }

    let created = state.storage.create(&key, &pvc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_pvc(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<PersistentVolumeClaim>> {
    debug!("Getting PersistentVolumeClaim: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("persistentvolumeclaims", Some(&namespace), &name);
    let pvc = state.storage.get(&key).await?;

    Ok(Json(pvc))
}

pub async fn list_pvcs(
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
        return crate::handlers::watch::watch_namespaced::<PersistentVolumeClaim>(
            state,
            auth_ctx,
            namespace,
            "persistentvolumeclaims",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing PersistentVolumeClaims in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("persistentvolumeclaims", Some(&namespace));
    let mut pvcs: Vec<PersistentVolumeClaim> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pvcs, &params)?;

    let list = List::new("PersistentVolumeClaimList", "v1", pvcs);
    Ok(Json(list).into_response())
}

/// List all persistentvolumeclaims across all namespaces
pub async fn list_all_pvcs(
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
        return crate::handlers::watch::watch_cluster_scoped::<PersistentVolumeClaim>(
            state,
            auth_ctx,
            "persistentvolumeclaims",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all persistentvolumeclaims");

    // Check authorization (cluster-wide list)
    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "persistentvolumeclaims").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("persistentvolumeclaims", None);
    let mut pvcs = state.storage.list::<PersistentVolumeClaim>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pvcs, &params)?;

    let list = List::new("PersistentVolumeClaimList", "v1", pvcs);
    Ok(Json(list).into_response())
}

pub async fn update_pvc(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut pvc): Json<PersistentVolumeClaim>,
) -> Result<Json<PersistentVolumeClaim>> {
    info!("Updating PersistentVolumeClaim: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    pvc.metadata.name = name.clone();
    pvc.metadata.namespace = Some(namespace.clone());

    let key = build_key("persistentvolumeclaims", Some(&namespace), &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolumeClaim {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(pvc));
    }

    let updated = state.storage.update(&key, &pvc).await?;

    Ok(Json(updated))
}

pub async fn delete_pvc(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PersistentVolumeClaim>> {
    info!("Deleting PersistentVolumeClaim: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("persistentvolumeclaims", Some(&namespace), &name);

    // Get the resource to check if it exists
    let pvc: PersistentVolumeClaim = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolumeClaim {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(pvc));
    }

    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers(&*state.storage, &key, &pvc)
            .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: PersistentVolumeClaim = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(pvc))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(
    patch_pvc,
    PersistentVolumeClaim,
    "persistentvolumeclaims",
    ""
);

pub async fn deletecollection_persistentvolumeclaims(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection persistentvolumeclaims in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "persistentvolumeclaims")
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
        info!("Dry-run: PersistentVolumeClaim collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all persistentvolumeclaims in the namespace
    let prefix = build_prefix("persistentvolumeclaims", Some(&namespace));
    let mut items = state.storage.list::<PersistentVolumeClaim>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key(
            "persistentvolumeclaims",
            Some(&namespace),
            &item.metadata.name,
        );

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
        "DeleteCollection completed: {} persistentvolumeclaims deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
