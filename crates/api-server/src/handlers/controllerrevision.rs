use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::ControllerRevision,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_controllerrevision(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut cr): DumpingJson<ControllerRevision>,
) -> Result<(StatusCode, Json<ControllerRevision>)> {
    info!(
        "Creating controllerrevision: {} in namespace: {}",
        cr.metadata.name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "controllerrevisions")
        .with_api_group("apps")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &cr.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Ensure namespace is set from the URL path
    cr.metadata.namespace = Some(namespace.clone());

    // Validate (upstream apps ValidateControllerRevisionCreate): data mandatory.
    let errs =
        rusternetes_common::validation::controllerrevision::validate_controller_revision(&cr);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Enrich metadata with system fields
    cr.metadata.ensure_uid();
    cr.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ControllerRevision validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(cr)));
    }

    let key = build_key("controllerrevisions", Some(&namespace), &cr.metadata.name);
    let created = state.storage.create(&key, &cr).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_controllerrevision(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ControllerRevision>> {
    info!(
        "Getting controllerrevision: {} in namespace: {}",
        name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "controllerrevisions")
        .with_api_group("apps")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("controllerrevisions", Some(&namespace), &name);
    let cr = state.storage.get(&key).await?;

    Ok(Json(cr))
}

pub async fn update_controllerrevision(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut cr): DumpingJson<ControllerRevision>,
) -> Result<Json<ControllerRevision>> {
    info!(
        "Updating controllerrevision: {} in namespace: {}",
        name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "controllerrevisions")
        .with_api_group("apps")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    cr.metadata.name = name.clone();
    cr.metadata.namespace = Some(namespace.clone());

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ControllerRevision validated successfully (not updated)");
        return Ok(Json(cr));
    }

    let key = build_key("controllerrevisions", Some(&namespace), &name);

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &cr).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &cr).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_controllerrevision(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ControllerRevision>> {
    info!(
        "Deleting controllerrevision: {} in namespace: {}",
        name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "controllerrevisions")
        .with_api_group("apps")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("controllerrevisions", Some(&namespace), &name);

    // Get the resource for finalizer handling
    let cr: ControllerRevision = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ControllerRevision validated successfully (not deleted)");
        return Ok(Json(cr));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &cr)
            .await?;

    if deleted_immediately {
        Ok(Json(cr))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ControllerRevision = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_controllerrevisions(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;

    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
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
        return crate::handlers::watch::watch_namespaced::<ControllerRevision>(
            state,
            auth_ctx,
            namespace,
            "controllerrevisions",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing controllerrevisions in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "controllerrevisions")
        .with_api_group("apps")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("controllerrevisions", Some(&namespace));
    let mut crs: Vec<ControllerRevision> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut crs, &params)?;

    let mut list = List::new("ControllerRevisionList", "apps/v1", crs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all controllerrevisions across all namespaces
pub async fn list_all_controllerrevisions(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;

    // Check if this is a watch request
    if params
        .get("watch")
        .and_then(|v| crate::handlers::watch::parse_k8s_bool(v))
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
        return crate::handlers::watch::watch_cluster_scoped::<ControllerRevision>(
            state,
            auth_ctx,
            "controllerrevisions",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing all controllerrevisions");

    // Check authorization (cluster-wide list)
    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "controllerrevisions").with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("controllerrevisions", None);
    let mut crs: Vec<ControllerRevision> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut crs, &params)?;

    let mut list = List::new("ControllerRevisionList", "apps/v1", crs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(
    patch_controllerrevision,
    ControllerRevision,
    "controllerrevisions",
    "apps"
);

pub async fn deletecollection_controllerrevisions(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection controllerrevisions in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "controllerrevisions")
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
        info!("Dry-run: ControllerRevision collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all controllerrevisions in the namespace
    let prefix = build_prefix("controllerrevisions", Some(&namespace));
    let mut items = state.storage.list::<ControllerRevision>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("controllerrevisions", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} controllerrevisions deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
