use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::VolumeSnapshotContent,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_volumesnapshotcontent(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut vsc): DumpingJson<VolumeSnapshotContent>,
) -> Result<(StatusCode, Json<VolumeSnapshotContent>)> {
    info!("Creating VolumeSnapshotContent: {}", vsc.metadata.name);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &vsc.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "volumesnapshotcontents")
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
        info!("Dry-run: VolumeSnapshotContent validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(vsc)));
    }

    let key = build_key("volumesnapshotcontents", None, &vsc.metadata.name);
    let created = state.storage.create(&key, &vsc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_volumesnapshotcontent(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<VolumeSnapshotContent>> {
    debug!("Getting VolumeSnapshotContent: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "volumesnapshotcontents")
        .with_api_group("snapshot.storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumesnapshotcontents", None, &name);
    let vsc = state.storage.get(&key).await?;

    Ok(Json(vsc))
}

pub async fn list_volumesnapshotcontents(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing all VolumeSnapshotContents");

    // Honor `?watch=true` on the collection endpoint (informer/Lens path).
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<VolumeSnapshotContent>(
            state,
            auth_ctx,
            "volumesnapshotcontents",
            "snapshot.storage.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "volumesnapshotcontents")
        .with_api_group("snapshot.storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("volumesnapshotcontents", None);
    let mut vscs = state.storage.list::<VolumeSnapshotContent>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vscs, &params)?;

    let mut list = List::new(
        "VolumeSnapshotContentList",
        "snapshot.storage.k8s.io/v1",
        vscs,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::response::IntoResponse::into_response(Json(list)))
}

pub async fn update_volumesnapshotcontent(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut vsc): DumpingJson<VolumeSnapshotContent>,
) -> Result<Json<VolumeSnapshotContent>> {
    info!("Updating VolumeSnapshotContent: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "volumesnapshotcontents")
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
        info!("Dry-run: VolumeSnapshotContent validated successfully (not updated)");
        return Ok(Json(vsc));
    }

    let key = build_key("volumesnapshotcontents", None, &name);
    let updated = state.storage.update(&key, &vsc).await?;

    Ok(Json(updated))
}

pub async fn delete_volumesnapshotcontent(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<VolumeSnapshotContent>> {
    info!("Deleting VolumeSnapshotContent: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "volumesnapshotcontents")
        .with_api_group("snapshot.storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumesnapshotcontents", None, &name);

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Get the resource for finalizer handling
    let resource: VolumeSnapshotContent = state.storage.get(&key).await?;

    if is_dry_run {
        info!("Dry-run: VolumeSnapshotContent validated successfully (not deleted)");
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
        let updated: VolumeSnapshotContent = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_volumesnapshotcontent,
    VolumeSnapshotContent,
    "volumesnapshotcontents",
    "snapshot.storage.k8s.io"
);

pub async fn deletecollection_volumesnapshotcontents(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection volumesnapshotcontents with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "volumesnapshotcontents")
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
        info!("Dry-run: VolumeSnapshotContent collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all volumesnapshotcontents
    let prefix = build_prefix("volumesnapshotcontents", None);
    let mut items = state.storage.list::<VolumeSnapshotContent>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("volumesnapshotcontents", None, &item.metadata.name);

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
        "DeleteCollection completed: {} volumesnapshotcontents deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
