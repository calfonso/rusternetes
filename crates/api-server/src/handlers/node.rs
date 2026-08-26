use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::Node,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut node): Json<Node>,
) -> Result<(StatusCode, Json<Node>)> {
    info!("Creating node: {}", node.metadata.name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "nodes").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Enrich metadata with system fields
    node.metadata.ensure_uid();
    node.metadata.ensure_creation_timestamp();

    let key = build_key("nodes", None, &node.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Node {} validated successfully (not created)",
            node.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(node)));
    }

    let created = state.storage.create(&key, &node).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<Node>> {
    debug!("Getting node: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "nodes")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("nodes", None, &name);
    let node = state.storage.get(&key).await?;

    Ok(Json(node))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut node): Json<Node>,
) -> Result<Json<Node>> {
    info!("Updating node: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "nodes")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    node.metadata.name = name.clone();

    let key = build_key("nodes", None, &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Node {} validated successfully (not updated)",
            name
        );
        return Ok(Json(node));
    }

    let updated = state.storage.update(&key, &node).await?;

    Ok(Json(updated))
}

pub async fn delete_node(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Node>> {
    info!("Deleting node: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "nodes")
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("nodes", None, &name);

    // Get the node for finalizer handling
    let node: Node = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Node {} validated successfully (not deleted)",
            name
        );
        return Ok(Json(node));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &node)
            .await?;

    if deleted_immediately {
        Ok(Json(node))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Node = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
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
        return crate::handlers::watch::watch_cluster_scoped::<Node>(
            state,
            auth_ctx,
            "nodes",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing nodes");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "nodes").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("nodes", None);
    let mut nodes: Vec<Node> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut nodes, &params)?;

    let list = List::new("NodeList", "v1", nodes);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(patch, Node, "nodes", "");

pub async fn deletecollection_nodes(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection nodes with params: {:?}", params);

    // Check authorization
    let attrs =
        RequestAttributes::new(auth_ctx.user, "deletecollection", "nodes").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Node collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all nodes
    let prefix = build_prefix("nodes", None);
    let mut items = state.storage.list::<Node>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("nodes", None, &item.metadata.name);

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
        "DeleteCollection completed: {} nodes deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
