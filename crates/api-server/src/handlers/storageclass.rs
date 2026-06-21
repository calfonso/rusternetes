use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::StorageClass,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_storageclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut sc): DumpingJson<StorageClass>,
) -> Result<(StatusCode, Json<StorageClass>)> {
    info!("Creating StorageClass: {}", sc.metadata.name);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &sc.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "storageclasses")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    sc.metadata.ensure_uid();
    sc.metadata.ensure_creation_timestamp();

    // SetDefaults_StorageClass: reclaimPolicy → Delete, volumeBindingMode →
    // Immediate when unset (upstream pkg/apis/storage/v1/defaults.go).
    if sc.reclaim_policy.is_none() {
        sc.reclaim_policy =
            Some(rusternetes_common::resources::volume::PersistentVolumeReclaimPolicy::Delete);
    }
    if sc.volume_binding_mode.is_none() {
        sc.volume_binding_mode =
            Some(rusternetes_common::resources::volume::VolumeBindingMode::Immediate);
    }

    // Validate the (defaulted) StorageClass — upstream ValidateStorageClass.
    let errs = rusternetes_common::validation::storageclass::validate_storage_class(&sc);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: StorageClass validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(sc)));
    }

    let key = build_key("storageclasses", None, &sc.metadata.name);
    let created = state.storage.create(&key, &sc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_storageclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<StorageClass>> {
    debug!("Getting StorageClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "storageclasses")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("storageclasses", None, &name);
    let sc = state.storage.get(&key).await?;

    Ok(Json(sc))
}

pub async fn list_storageclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<StorageClass>(
            state,
            auth_ctx,
            "storageclasses",
            "storage.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all StorageClasses");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "storageclasses")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("storageclasses", None);
    let mut scs = state.storage.list::<StorageClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut scs, &params)?;

    let mut list = List::new("StorageClassList", "storage.k8s.io/v1", scs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

pub async fn update_storageclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut sc): DumpingJson<StorageClass>,
) -> Result<Json<StorageClass>> {
    info!("Updating StorageClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "storageclasses")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    sc.metadata.name = name.clone();

    // SetDefaults_StorageClass on the incoming object so an omitted-but-defaulted
    // field isn't read as a forbidden change against the stored (defaulted) one.
    if sc.reclaim_policy.is_none() {
        sc.reclaim_policy =
            Some(rusternetes_common::resources::volume::PersistentVolumeReclaimPolicy::Delete);
    }
    if sc.volume_binding_mode.is_none() {
        sc.volume_binding_mode =
            Some(rusternetes_common::resources::volume::VolumeBindingMode::Immediate);
    }

    let key = build_key("storageclasses", None, &name);

    // Enforce update immutability (upstream ValidateStorageClassUpdate):
    // parameters / provisioner / reclaimPolicy / volumeBindingMode are immutable.
    if let Ok(existing) = state.storage.get::<StorageClass>(&key).await {
        let errs = rusternetes_common::validation::storageclass::validate_storage_class_update(
            &sc, &existing,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: StorageClass validated successfully (not updated)");
        return Ok(Json(sc));
    }

    let updated = state.storage.update(&key, &sc).await?;

    Ok(Json(updated))
}

pub async fn delete_storageclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<StorageClass>> {
    info!("Deleting StorageClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "storageclasses")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("storageclasses", None, &name);

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Get the resource for finalizer handling
    let resource: StorageClass = state.storage.get(&key).await?;

    if is_dry_run {
        info!("Dry-run: StorageClass validated successfully (not deleted)");
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
        let updated: StorageClass = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_storageclass,
    StorageClass,
    "storageclasses",
    "storage.k8s.io"
);

pub async fn deletecollection_storageclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection storageclasses with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "storageclasses")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: StorageClass collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all storageclasses
    let prefix = build_prefix("storageclasses", None);
    let mut items = state.storage.list::<StorageClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("storageclasses", None, &item.metadata.name);

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
        "DeleteCollection completed: {} storageclasses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
