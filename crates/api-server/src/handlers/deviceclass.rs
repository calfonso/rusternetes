use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::DeviceClass,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_deviceclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut dc): Json<DeviceClass>,
) -> Result<(StatusCode, Json<DeviceClass>)> {
    info!(
        "Creating DeviceClass: {}",
        dc.metadata
            .as_ref()
            .map(|m| m.name.as_deref().unwrap_or(""))
            .unwrap_or("")
    );

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "deviceclasses")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Set kind and apiVersion
    dc.kind = "DeviceClass".to_string();
    dc.api_version = "resource.k8s.io/v1".to_string();

    // Ensure metadata exists and set defaults
    let metadata = dc.metadata.get_or_insert_with(Default::default);

    // Generate UID and timestamp if not present
    if metadata.uid.is_none() {
        metadata.uid = Some(uuid::Uuid::new_v4().to_string());
    }
    if metadata.creation_timestamp.is_none() {
        metadata.creation_timestamp = Some(chrono::Utc::now());
    }

    let name = metadata.name.as_ref().ok_or_else(|| {
        rusternetes_common::Error::InvalidResource("metadata.name is required".to_string())
    })?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: DeviceClass validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(dc)));
    }

    let key = build_key("deviceclasses", None, name);
    let created = state.storage.create(&key, &dc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_deviceclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<DeviceClass>> {
    debug!("Getting DeviceClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "deviceclasses")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("deviceclasses", None, &name);
    let mut dc: DeviceClass = state.storage.get(&key).await?;

    // Ensure kind and apiVersion are set in the response
    dc.kind = "DeviceClass".to_string();
    dc.api_version = "resource.k8s.io/v1".to_string();

    Ok(Json(dc))
}

pub async fn list_deviceclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Intercept watch requests
    if params
        .get("watch")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
    {
        info!("Starting watch for deviceclasses");
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
            "deviceclasses",
            "resource.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all DeviceClasses");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "deviceclasses")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("deviceclasses", None);
    let mut dcs: Vec<DeviceClass> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut dcs, &params)?;

    let list = List::new("DeviceClassList", "resource.k8s.io/v1", dcs);
    Ok(Json(list).into_response())
}

pub async fn update_deviceclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut dc): Json<DeviceClass>,
) -> Result<Json<DeviceClass>> {
    info!("Updating DeviceClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "deviceclasses")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure kind and apiVersion are set
    dc.kind = "DeviceClass".to_string();
    dc.api_version = "resource.k8s.io/v1".to_string();

    // Ensure metadata and set name
    let metadata = dc.metadata.get_or_insert_with(Default::default);
    metadata.name = Some(name.clone());

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: DeviceClass validated successfully (not updated)");
        return Ok(Json(dc));
    }

    let key = build_key("deviceclasses", None, &name);
    let updated = state.storage.update(&key, &dc).await?;

    Ok(Json(updated))
}

pub async fn delete_deviceclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<DeviceClass>> {
    info!("Deleting DeviceClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "deviceclasses")
        .with_api_group("resource.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("deviceclasses", None, &name);

    // Get the resource before deletion
    let resource: DeviceClass = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: DeviceClass validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // NOTE: DRA resources use dra::ObjectMeta which is incompatible with finalizers.
    // We perform a simple delete without finalizer support.
    state.storage.delete(&key).await?;

    Ok(Json(resource))
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_deviceclass,
    DeviceClass,
    "deviceclasses",
    "resource.k8s.io"
);

pub async fn deletecollection_deviceclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection deviceclasses with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "deviceclasses")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: DeviceClass collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all deviceclasses
    let prefix = build_prefix("deviceclasses", None);
    let mut items = state.storage.list::<DeviceClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        // Extract name from metadata (handle Option)
        if let Some(metadata) = &item.metadata {
            if let Some(name) = &metadata.name {
                let key = build_key("deviceclasses", None, name);

                // NOTE: DRA resources use dra::ObjectMeta which is incompatible with finalizers.
                // We perform a simple delete without finalizer support.
                state.storage.delete(&key).await?;
                deleted_count += 1;
            }
        }
    }

    info!(
        "DeleteCollection completed: {} deviceclasses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
