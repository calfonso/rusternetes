use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::NetworkPolicy,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut network_policy): Json<NetworkPolicy>,
) -> Result<(StatusCode, Json<NetworkPolicy>)> {
    info!(
        "Creating networkpolicy: {}/{}",
        namespace, network_policy.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "networkpolicies")
        .with_namespace(&namespace)
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    network_policy.metadata.namespace = Some(namespace.clone());
    network_policy.metadata.ensure_uid();
    network_policy.metadata.ensure_creation_timestamp();

    let key = build_key(
        "networkpolicies",
        Some(&namespace),
        &network_policy.metadata.name,
    );

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: NetworkPolicy {}/{} validated successfully (not created)",
            namespace, network_policy.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(network_policy)));
    }

    let created = state.storage.create(&key, &network_policy).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<NetworkPolicy>> {
    debug!("Getting networkpolicy: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "networkpolicies")
        .with_namespace(&namespace)
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("networkpolicies", Some(&namespace), &name);
    let network_policy = state.storage.get(&key).await?;

    Ok(Json(network_policy))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut network_policy): Json<NetworkPolicy>,
) -> Result<Json<NetworkPolicy>> {
    info!("Updating networkpolicy: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "networkpolicies")
        .with_namespace(&namespace)
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    network_policy.metadata.name = name.clone();
    network_policy.metadata.namespace = Some(namespace.clone());

    let key = build_key("networkpolicies", Some(&namespace), &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: NetworkPolicy {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(network_policy));
    }

    let result = match state.storage.update(&key, &network_policy).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &network_policy).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_networkpolicy(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<NetworkPolicy>> {
    info!("Deleting networkpolicy: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "networkpolicies")
        .with_namespace(&namespace)
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("networkpolicies", Some(&namespace), &name);

    // Get the resource to check if it exists
    let networkpolicy: NetworkPolicy = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: NetworkPolicy {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(networkpolicy));
    }

    let has_finalizers = crate::handlers::finalizers::handle_delete_with_finalizers(
        &*state.storage,
        &key,
        &networkpolicy,
    )
    .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: NetworkPolicy = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(networkpolicy))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
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
        return crate::handlers::watch::watch_namespaced::<NetworkPolicy>(
            state,
            auth_ctx,
            namespace,
            "networkpolicies",
            "networking.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing networkpolicies in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "networkpolicies")
        .with_namespace(&namespace)
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("networkpolicies", Some(&namespace));
    let mut network_policies: Vec<NetworkPolicy> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut network_policies, &params)?;

    let list = List::new(
        "NetworkPolicyList",
        "networking.k8s.io/v1",
        network_policies,
    );
    Ok(Json(list).into_response())
}

/// List all networkpolicies across all namespaces
pub async fn list_all_networkpolicies(
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
        return crate::handlers::watch::watch_cluster_scoped::<NetworkPolicy>(
            state,
            auth_ctx,
            "networkpolicies",
            "networking.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all networkpolicies");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "networkpolicies")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("networkpolicies", None);
    let mut network_policies = state.storage.list::<NetworkPolicy>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut network_policies, &params)?;

    let list = List::new(
        "NetworkPolicyList",
        "networking.k8s.io/v1",
        network_policies,
    );
    Ok(Json(list).into_response())
}

crate::patch_handler_namespaced!(patch, NetworkPolicy, "networkpolicies", "networking.k8s.io");

pub async fn deletecollection_networkpolicies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection networkpolicies in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "networkpolicies")
        .with_namespace(&namespace)
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: NetworkPolicy collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all networkpolicies in the namespace
    let prefix = build_prefix("networkpolicies", Some(&namespace));
    let mut items = state.storage.list::<NetworkPolicy>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("networkpolicies", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} networkpolicies deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
