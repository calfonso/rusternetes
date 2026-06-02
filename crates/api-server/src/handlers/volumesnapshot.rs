use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::VolumeSnapshot,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_volumesnapshot(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut vs): DumpingJson<VolumeSnapshot>,
) -> Result<(StatusCode, Json<VolumeSnapshot>)> {
    info!(
        "Creating VolumeSnapshot: {}/{}",
        namespace, vs.metadata.name
    );

    // Check authorization (namespace-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "volumesnapshots")
        .with_api_group("snapshot.storage.k8s.io")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    vs.metadata.namespace = Some(namespace.clone());
    vs.metadata.ensure_uid();
    vs.metadata.ensure_creation_timestamp();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeSnapshot validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(vs)));
    }

    let key = build_key("volumesnapshots", Some(&namespace), &vs.metadata.name);
    let created = state.storage.create(&key, &vs).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_volumesnapshot(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<VolumeSnapshot>> {
    debug!("Getting VolumeSnapshot: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "volumesnapshots")
        .with_api_group("snapshot.storage.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumesnapshots", Some(&namespace), &name);
    let vs = state.storage.get(&key).await?;

    Ok(Json(vs))
}

pub async fn list_volumesnapshots(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_namespaced::<VolumeSnapshot>(
            state,
            auth_ctx,
            namespace,
            "volumesnapshots",
            "snapshot.storage.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    debug!("Listing VolumeSnapshots in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "volumesnapshots")
        .with_api_group("snapshot.storage.k8s.io")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("volumesnapshots", Some(&namespace));
    let mut vss = state.storage.list::<VolumeSnapshot>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vss, &params)?;

    let mut list = List::new("VolumeSnapshotList", "snapshot.storage.k8s.io/v1", vss);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::response::IntoResponse::into_response(Json(list)))
}

pub async fn list_all_volumesnapshots(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<VolumeSnapshot>(
            state,
            auth_ctx,
            "volumesnapshots",
            "snapshot.storage.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    debug!("Listing all VolumeSnapshots");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "volumesnapshots")
        .with_api_group("snapshot.storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("volumesnapshots", None);
    let mut vss = state.storage.list::<VolumeSnapshot>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vss, &params)?;

    let mut list = List::new("VolumeSnapshotList", "snapshot.storage.k8s.io/v1", vss);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::response::IntoResponse::into_response(Json(list)))
}

pub async fn update_volumesnapshot(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut vs): DumpingJson<VolumeSnapshot>,
) -> Result<Json<VolumeSnapshot>> {
    info!("Updating VolumeSnapshot: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "volumesnapshots")
        .with_api_group("snapshot.storage.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    vs.metadata.name = name.clone();
    vs.metadata.namespace = Some(namespace.clone());

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeSnapshot validated successfully (not updated)");
        return Ok(Json(vs));
    }

    let key = build_key("volumesnapshots", Some(&namespace), &name);
    let updated = state.storage.update(&key, &vs).await?;

    Ok(Json(updated))
}

pub async fn delete_volumesnapshot(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<VolumeSnapshot>> {
    info!("Deleting VolumeSnapshot: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "volumesnapshots")
        .with_api_group("snapshot.storage.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumesnapshots", Some(&namespace), &name);

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Get the resource for finalizer handling
    let resource: VolumeSnapshot = state.storage.get(&key).await?;

    if is_dry_run {
        info!("Dry-run: VolumeSnapshot validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &resource,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(resource))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: VolumeSnapshot = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(
    patch_volumesnapshot,
    VolumeSnapshot,
    "volumesnapshots",
    "snapshot.storage.k8s.io"
);

pub async fn deletecollection_volumesnapshots(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection volumesnapshots in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "volumesnapshots")
        .with_namespace(&namespace)
        .with_api_group("snapshot.storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeSnapshot collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all volumesnapshots in the namespace
    let prefix = build_prefix("volumesnapshots", Some(&namespace));
    let mut items = state.storage.list::<VolumeSnapshot>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("volumesnapshots", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} volumesnapshots deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
