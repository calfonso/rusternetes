use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::VolumeSnapshotClass,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_volumesnapshotclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut vsc): DumpingJson<VolumeSnapshotClass>,
) -> Result<(StatusCode, Json<VolumeSnapshotClass>)> {
    info!("Creating VolumeSnapshotClass: {}", vsc.metadata.name);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &vsc.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "volumesnapshotclasses")
        .with_api_group("snapshot.storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    vsc.metadata.ensure_uid();
    vsc.metadata.ensure_creation_timestamp();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeSnapshotClass validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(vsc)));
    }

    let key = build_key("volumesnapshotclasses", None, &vsc.metadata.name);
    let created = state.storage.create(&key, &vsc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_volumesnapshotclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<VolumeSnapshotClass>> {
    debug!("Getting VolumeSnapshotClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "volumesnapshotclasses")
        .with_api_group("snapshot.storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumesnapshotclasses", None, &name);
    let vsc = state.storage.get(&key).await?;

    Ok(Json(vsc))
}

pub async fn list_volumesnapshotclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing all VolumeSnapshotClasses");

    // Honor `?watch=true` on the collection endpoint (informer/Lens path).
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<VolumeSnapshotClass>(
            state,
            auth_ctx,
            "volumesnapshotclasses",
            "snapshot.storage.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "volumesnapshotclasses")
        .with_api_group("snapshot.storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("volumesnapshotclasses", None);
    let mut vscs = state.storage.list::<VolumeSnapshotClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vscs, &params)?;

    let mut list = List::new(
        "VolumeSnapshotClassList",
        "snapshot.storage.k8s.io/v1",
        vscs,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::response::IntoResponse::into_response(Json(list)))
}

pub async fn update_volumesnapshotclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut vsc): DumpingJson<VolumeSnapshotClass>,
) -> Result<Json<VolumeSnapshotClass>> {
    info!("Updating VolumeSnapshotClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "volumesnapshotclasses")
        .with_api_group("snapshot.storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    vsc.metadata.name = name.clone();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeSnapshotClass validated successfully (not updated)");
        return Ok(Json(vsc));
    }

    let key = build_key("volumesnapshotclasses", None, &name);
    let updated = state.storage.update(&key, &vsc).await?;

    Ok(Json(updated))
}

pub async fn delete_volumesnapshotclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<VolumeSnapshotClass>> {
    info!("Deleting VolumeSnapshotClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "volumesnapshotclasses")
        .with_api_group("snapshot.storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumesnapshotclasses", None, &name);

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Get the resource for finalizer handling
    let resource: VolumeSnapshotClass = state.storage.get(&key).await?;

    if is_dry_run {
        info!("Dry-run: VolumeSnapshotClass validated successfully (not deleted)");
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
        let updated: VolumeSnapshotClass = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_volumesnapshotclass,
    VolumeSnapshotClass,
    "volumesnapshotclasses",
    "snapshot.storage.k8s.io"
);

pub async fn deletecollection_volumesnapshotclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection volumesnapshotclasses with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "volumesnapshotclasses")
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
        info!("Dry-run: VolumeSnapshotClass collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all volumesnapshotclasses
    let prefix = build_prefix("volumesnapshotclasses", None);
    let mut items = state.storage.list::<VolumeSnapshotClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("volumesnapshotclasses", None, &item.metadata.name);

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
        "DeleteCollection completed: {} volumesnapshotclasses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
