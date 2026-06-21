use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::IPAddress,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_ipaddress(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut ipaddress): DumpingJson<IPAddress>,
) -> Result<(StatusCode, Json<IPAddress>)> {
    info!("Creating IPAddress: {}", ipaddress.metadata.name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "ipaddresses")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Full create-time ValidateObjectMeta (#1087). IPAddress is cluster-scoped
    // and its name must be a canonical IP address (ValidateIPAddressName).
    crate::handlers::validation::validate_create_object_meta(
        &ipaddress.metadata,
        None,
        crate::handlers::validation::NameKind::Ip,
    )?;

    // Field validation (mirrors upstream ValidateIPAddress — spec.parentRef).
    {
        let errs = rusternetes_common::validation::ipaddress::validate_ip_address(&ipaddress);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Enrich metadata with system fields
    ipaddress.metadata.ensure_uid();
    ipaddress.metadata.ensure_creation_timestamp();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: IPAddress validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(ipaddress)));
    }

    // IPAddress is cluster-scoped (no namespace)
    let key = build_key("ipaddresses", None, &ipaddress.metadata.name);
    let created = state.storage.create(&key, &ipaddress).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_ipaddress(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<IPAddress>> {
    debug!("Getting IPAddress: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "ipaddresses")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("ipaddresses", None, &name);
    let ipaddress = state.storage.get(&key).await?;

    Ok(Json(ipaddress))
}

pub async fn update_ipaddress(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut ipaddress): DumpingJson<IPAddress>,
) -> Result<Json<IPAddress>> {
    info!("Updating IPAddress: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "ipaddresses")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    ipaddress.metadata.name = name.clone();

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: IPAddress validated successfully (not updated)");
        return Ok(Json(ipaddress));
    }

    let key = build_key("ipaddresses", None, &name);
    let updated = state.storage.update(&key, &ipaddress).await?;

    Ok(Json(updated))
}

pub async fn delete_ipaddress(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<IPAddress>> {
    info!("Deleting IPAddress: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "ipaddresses")
        .with_api_group("networking.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("ipaddresses", None, &name);

    // Get the resource for finalizer handling
    let ipaddress: IPAddress = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: IPAddress validated successfully (not deleted)");
        return Ok(Json(ipaddress));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &ipaddress,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(ipaddress))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: IPAddress = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_ipaddresses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response> {
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
        return crate::handlers::watch::watch_cluster_scoped::<IPAddress>(
            state,
            auth_ctx,
            "ipaddresses",
            "networking.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing IPAddresses");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "ipaddresses")
        .with_api_group("networking.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("ipaddresses", None);
    let mut ipaddresses = state.storage.list::<IPAddress>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut ipaddresses, &params)?;

    let mut list = List::new("IPAddressList", "networking.k8s.io/v1", ipaddresses);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_ipaddress,
    IPAddress,
    "ipaddresses",
    "networking.k8s.io"
);

pub async fn deletecollection_ipaddresses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection ipaddresses with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "ipaddresses")
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
        info!("Dry-run: IPAddress collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all ipaddresses
    let prefix = build_prefix("ipaddresses", None);
    let mut items = state.storage.list::<IPAddress>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("ipaddresses", None, &item.metadata.name);

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
        "DeleteCollection completed: {} ipaddresses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
