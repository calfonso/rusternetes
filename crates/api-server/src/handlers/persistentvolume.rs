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
    resources::PersistentVolume,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_pv(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<(StatusCode, Json<PersistentVolume>)> {
    let mut pv: PersistentVolume = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!("Creating PersistentVolume: {}", pv.metadata.name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization (cluster-scoped)
    let attrs =
        RequestAttributes::new(auth_ctx.user, "create", "persistentvolumes").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    pv.metadata.ensure_uid();
    pv.metadata.ensure_creation_timestamp();

    // Ensure status exists with a default phase so Go clients can deserialize it
    if pv.status.is_none() {
        pv.status = Some(rusternetes_common::resources::PersistentVolumeStatus {
            phase: rusternetes_common::resources::PersistentVolumePhase::Pending,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        });
    }

    let key = build_key("persistentvolumes", None, &pv.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolume {} validated successfully (not created)",
            pv.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(pv)));
    }

    let created = state.storage.create(&key, &pv).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_pv(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<PersistentVolume>> {
    debug!("Getting PersistentVolume: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "persistentvolumes")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("persistentvolumes", None, &name);
    let pv = state.storage.get(&key).await?;

    Ok(Json(pv))
}

pub async fn list_pvs(
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
        return crate::handlers::watch::watch_cluster_scoped::<PersistentVolume>(
            state,
            auth_ctx,
            "persistentvolumes",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all PersistentVolumes");

    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "persistentvolumes").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("persistentvolumes", None);
    let mut pvs: Vec<PersistentVolume> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pvs, &params)?;

    let list = List::new("PersistentVolumeList", "v1", pvs);
    Ok(Json(list).into_response())
}

pub async fn update_pv(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut pv): Json<PersistentVolume>,
) -> Result<Json<PersistentVolume>> {
    info!("Updating PersistentVolume: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "persistentvolumes")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    pv.metadata.name = name.clone();

    let key = build_key("persistentvolumes", None, &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolume {} validated successfully (not updated)",
            name
        );
        return Ok(Json(pv));
    }

    let updated = state.storage.update(&key, &pv).await?;

    Ok(Json(updated))
}

pub async fn delete_pv(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PersistentVolume>> {
    info!("Deleting PersistentVolume: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "persistentvolumes")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("persistentvolumes", None, &name);

    // Get the resource to check if it exists
    let pv: PersistentVolume = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolume {} validated successfully (not deleted)",
            name
        );
        return Ok(Json(pv));
    }

    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers(&*state.storage, &key, &pv)
            .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: PersistentVolume = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(pv))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(patch_pv, PersistentVolume, "persistentvolumes", "");

pub async fn deletecollection_persistentvolumes(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection persistentvolumes with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "persistentvolumes")
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
        info!("Dry-run: PersistentVolume collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all persistentvolumes
    let prefix = build_prefix("persistentvolumes", None);
    let mut items = state.storage.list::<PersistentVolume>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("persistentvolumes", None, &item.metadata.name);

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
        "DeleteCollection completed: {} persistentvolumes deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
