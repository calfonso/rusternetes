use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::ReplicationController,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_replicationcontroller(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Json(mut rc): Json<ReplicationController>,
) -> Result<(StatusCode, Json<ReplicationController>)> {
    info!(
        "Creating replicationcontroller: {} in namespace: {}",
        rc.metadata.name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace is set from the URL path
    rc.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    rc.metadata.ensure_uid();
    rc.metadata.ensure_creation_timestamp();

    // K8s defaults RC.Spec.Selector from Template.Labels when not provided.
    // See: pkg/registry/core/replicationcontroller/strategy.go
    if rc.spec.selector.is_none() {
        rc.spec.selector = rc
            .spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone());
    }

    // Apply K8s defaults to pod template
    crate::handlers::defaults::apply_pod_template_defaults(&mut rc.spec.template);

    let key = build_key(
        "replicationcontrollers",
        Some(&namespace),
        &rc.metadata.name,
    );
    let created = state.storage.create(&key, &rc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_replicationcontroller(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ReplicationController>> {
    info!(
        "Getting replicationcontroller: {} in namespace: {}",
        name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("replicationcontrollers", Some(&namespace), &name);
    let rc = state.storage.get(&key).await?;

    Ok(Json(rc))
}

pub async fn update_replicationcontroller(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Json(mut rc): Json<ReplicationController>,
) -> Result<Json<ReplicationController>> {
    info!(
        "Updating replicationcontroller: {} in namespace: {}",
        name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    rc.metadata.name = name.clone();
    rc.metadata.namespace = Some(namespace.clone());

    let key = build_key("replicationcontrollers", Some(&namespace), &name);

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &rc).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &rc).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_replicationcontroller(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Json<ReplicationController>> {
    info!(
        "Deleting replicationcontroller: {} in namespace: {}",
        name, namespace
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("replicationcontrollers", Some(&namespace), &name);

    // Get the resource to check if it exists
    let rc: ReplicationController = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: ReplicationController {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(rc));
    }

    // Extract propagation policy from query params or request body (DeleteOptions)
    let body_propagation: Option<String> = if !body.is_empty() {
        serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| {
                v.get("propagationPolicy")
                    .and_then(|p| p.as_str())
                    .map(|s| s.to_string())
            })
    } else {
        None
    };
    let propagation_policy = params
        .get("propagationPolicy")
        .map(|s| s.as_str())
        .or(body_propagation.as_deref());
    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers_and_propagation(
            &*state.storage,
            &key,
            &rc,
            propagation_policy,
        )
        .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ReplicationController = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(rc))
    }
}

pub async fn list_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<ReplicationController>(
            state,
            auth_ctx,
            namespace,
            "replicationcontrollers",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing replicationcontrollers in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicationcontrollers", Some(&namespace));
    let mut rcs = state.storage.list::<ReplicationController>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rcs, &params)?;

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            rcs,
            Some(resource_version.clone()),
            "ReplicationController",
        );
        return Ok(axum::Json(table).into_response());
    }

    let list = List::new("ReplicationControllerList", "v1", rcs);
    Ok(Json(list).into_response())
}

/// List all replicationcontrollers across all namespaces
pub async fn list_all_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ReplicationController>(
            state,
            auth_ctx,
            "replicationcontrollers",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all replicationcontrollers");

    // Check authorization (cluster-wide list)
    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "replicationcontrollers").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicationcontrollers", None);
    let mut rcs = state.storage.list::<ReplicationController>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rcs, &params)?;

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            rcs,
            Some(resource_version.clone()),
            "ReplicationController",
        );
        return Ok(axum::Json(table).into_response());
    }

    let list = List::new("ReplicationControllerList", "v1", rcs);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(
    patch_replicationcontroller,
    ReplicationController,
    "replicationcontrollers",
    ""
);

pub async fn deletecollection_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection replicationcontrollers in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "replicationcontrollers")
        .with_namespace(&namespace)
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
        info!("Dry-run: ReplicationController collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all replicationcontrollers in the namespace
    let prefix = build_prefix("replicationcontrollers", Some(&namespace));
    let mut items = state.storage.list::<ReplicationController>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key(
            "replicationcontrollers",
            Some(&namespace),
            &item.metadata.name,
        );

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
        "DeleteCollection completed: {} replicationcontrollers deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
