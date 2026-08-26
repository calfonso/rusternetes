use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::ResourceClaimTemplate,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_resourceclaimtemplate(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut template): Json<ResourceClaimTemplate>,
) -> Result<(StatusCode, Json<ResourceClaimTemplate>)> {
    info!(
        "Creating ResourceClaimTemplate: {}/{}",
        namespace,
        template
            .metadata
            .as_ref()
            .map(|m| m.name.as_deref().unwrap_or(""))
            .unwrap_or("")
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "resourceclaimtemplates")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure kind and apiVersion are set
    template.kind = "ResourceClaimTemplate".to_string();
    template.api_version = "resource.k8s.io/v1".to_string();

    // Ensure metadata exists and set defaults
    let metadata = template.metadata.get_or_insert_with(Default::default);
    metadata.namespace = Some(namespace.clone());

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
        info!("Dry-run: ResourceClaimTemplate validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(template)));
    }

    let key = build_key("resourceclaimtemplates", Some(&namespace), name);
    let created = state.storage.create(&key, &template).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_resourceclaimtemplate(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ResourceClaimTemplate>> {
    debug!("Getting ResourceClaimTemplate: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "resourceclaimtemplates")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("resourceclaimtemplates", Some(&namespace), &name);
    let mut template: ResourceClaimTemplate = state.storage.get(&key).await?;

    // Ensure kind and apiVersion are set in the response
    template.kind = "ResourceClaimTemplate".to_string();
    template.api_version = "resource.k8s.io/v1".to_string();

    Ok(Json(template))
}

pub async fn list_resourceclaimtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if params
        .get("watch")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
    {
        info!(
            "Starting watch for resourceclaimtemplates in namespace: {}",
            namespace
        );
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
        return crate::handlers::watch::watch_namespaced_json(
            state,
            auth_ctx,
            namespace,
            "resourceclaimtemplates",
            "resource.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing ResourceClaimTemplates in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "resourceclaimtemplates")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("resourceclaimtemplates", Some(&namespace));
    let mut templates: Vec<ResourceClaimTemplate> = state.storage.list(&prefix).await?;

    crate::handlers::filtering::apply_selectors(&mut templates, &params)?;

    let list = List::new("ResourceClaimTemplateList", "resource.k8s.io/v1", templates);
    Ok(Json(list).into_response())
}

pub async fn list_all_resourceclaimtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if params
        .get("watch")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
    {
        info!("Starting watch for all resourceclaimtemplates");
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
            "resourceclaimtemplates",
            "resource.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all ResourceClaimTemplates");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "resourceclaimtemplates")
        .with_api_group("resource.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("resourceclaimtemplates", None);
    let mut templates: Vec<ResourceClaimTemplate> = state.storage.list(&prefix).await?;

    crate::handlers::filtering::apply_selectors(&mut templates, &params)?;

    let list = List::new("ResourceClaimTemplateList", "resource.k8s.io/v1", templates);
    Ok(Json(list).into_response())
}

pub async fn update_resourceclaimtemplate(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut template): Json<ResourceClaimTemplate>,
) -> Result<Json<ResourceClaimTemplate>> {
    info!("Updating ResourceClaimTemplate: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "resourceclaimtemplates")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure kind and apiVersion are set
    template.kind = "ResourceClaimTemplate".to_string();
    template.api_version = "resource.k8s.io/v1".to_string();

    // Ensure metadata and set namespace/name
    let metadata = template.metadata.get_or_insert_with(Default::default);
    metadata.namespace = Some(namespace.clone());
    metadata.name = Some(name.clone());

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ResourceClaimTemplate validated successfully (not updated)");
        return Ok(Json(template));
    }

    let key = build_key("resourceclaimtemplates", Some(&namespace), &name);
    let updated = state.storage.update(&key, &template).await?;

    Ok(Json(updated))
}

pub async fn delete_resourceclaimtemplate(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ResourceClaimTemplate>> {
    info!("Deleting ResourceClaimTemplate: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "resourceclaimtemplates")
        .with_api_group("resource.k8s.io")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("resourceclaimtemplates", Some(&namespace), &name);

    // Get the resource before deletion
    let resource: ResourceClaimTemplate = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: ResourceClaimTemplate validated successfully (not deleted)");
        return Ok(Json(resource));
    }

    // NOTE: DRA resources use dra::ObjectMeta which is incompatible with finalizers.
    // We perform a simple delete without finalizer support.
    state.storage.delete(&key).await?;

    Ok(Json(resource))
}

// Use the macro to create a PATCH handler (namespace-scoped)
crate::patch_handler_namespaced!(
    patch_resourceclaimtemplate,
    ResourceClaimTemplate,
    "resourceclaimtemplates",
    "resource.k8s.io"
);

pub async fn deletecollection_resourceclaimtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection resourceclaimtemplates in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "resourceclaimtemplates")
        .with_namespace(&namespace)
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
        info!("Dry-run: ResourceClaimTemplate collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all resourceclaimtemplates in the namespace
    let prefix = build_prefix("resourceclaimtemplates", Some(&namespace));
    let mut items = state.storage.list::<ResourceClaimTemplate>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        // Extract name from metadata (handle Option)
        if let Some(metadata) = &item.metadata {
            if let Some(name) = &metadata.name {
                let key = build_key("resourceclaimtemplates", Some(&namespace), name);

                // NOTE: DRA resources use dra::ObjectMeta which is incompatible with finalizers.
                // We perform a simple delete without finalizer support.
                state.storage.delete(&key).await?;
                deleted_count += 1;
            }
        }
    }

    info!(
        "DeleteCollection completed: {} resourceclaimtemplates deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
