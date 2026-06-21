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
    resources::RuntimeClass,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_runtimeclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut runtime_class): DumpingJson<RuntimeClass>,
) -> Result<(StatusCode, Json<RuntimeClass>)> {
    info!("Creating RuntimeClass: {}", runtime_class.metadata.name);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &runtime_class.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Validate the RuntimeClass (upstream node ValidateRuntimeClass): handler
    // DNS label, overhead.podFixed quantities, scheduling selector/tolerations.
    let errs = rusternetes_common::validation::runtimeclass::validate_runtime_class(&runtime_class);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "runtimeclasses")
        .with_api_group("node.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Enrich metadata with system fields
    runtime_class.metadata.ensure_uid();
    runtime_class.metadata.ensure_creation_timestamp();

    let key = build_key("runtimeclasses", None, &runtime_class.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: RuntimeClass {} validated successfully (not created)",
            runtime_class.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(runtime_class)));
    }

    let created = state.storage.create(&key, &runtime_class).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_runtimeclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<RuntimeClass>> {
    debug!("Getting RuntimeClass: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "runtimeclasses")
        .with_api_group("node.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("runtimeclasses", None, &name);
    let runtime_class = state.storage.get(&key).await?;

    Ok(Json(runtime_class))
}

pub async fn update_runtimeclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut runtime_class): DumpingJson<RuntimeClass>,
) -> Result<Json<RuntimeClass>> {
    info!("Updating RuntimeClass: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "runtimeclasses")
        .with_api_group("node.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    runtime_class.metadata.name = name.clone();

    let key = build_key("runtimeclasses", None, &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: RuntimeClass {} validated successfully (not updated)",
            name
        );
        return Ok(Json(runtime_class));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &runtime_class).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &runtime_class).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_runtimeclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<RuntimeClass>> {
    info!("Deleting RuntimeClass: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "runtimeclasses")
        .with_api_group("node.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("runtimeclasses", None, &name);

    // Get the runtime class for finalizer handling
    let runtime_class: RuntimeClass = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: RuntimeClass {} validated successfully (not deleted)",
            name
        );
        return Ok(Json(runtime_class));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &runtime_class,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(runtime_class))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: RuntimeClass = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_runtimeclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
        .unwrap_or(false)
    {
        debug!("Watching RuntimeClasses");
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
        return crate::handlers::watch::watch_cluster_scoped::<RuntimeClass>(
            state,
            auth_ctx,
            "runtimeclasses",
            "node.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing RuntimeClasses");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "runtimeclasses")
        .with_api_group("node.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("runtimeclasses", None);
    let mut runtime_classes: Vec<RuntimeClass> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut runtime_classes, &params)?;

    let mut list = List::new("RuntimeClassList", "node.k8s.io/v1", runtime_classes);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::Json(list).into_response())
}

// Use the macro to create a PATCH handler for cluster-scoped RuntimeClass
crate::patch_handler_cluster!(
    patch_runtimeclass,
    RuntimeClass,
    "runtimeclasses",
    "node.k8s.io"
);

pub async fn deletecollection_runtimeclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection runtimeclasses with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "runtimeclasses")
        .with_api_group("node.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: RuntimeClass collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all runtimeclasses
    let prefix = build_prefix("runtimeclasses", None);
    let mut items = state.storage.list::<RuntimeClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("runtimeclasses", None, &item.metadata.name);

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
        "DeleteCollection completed: {} runtimeclasses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
