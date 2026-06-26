use crate::{handlers::watch::WatchParams, middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::EndpointSlice,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Create endpointslice
pub async fn create_endpointslice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut endpointslice): DumpingJson<EndpointSlice>,
) -> Result<(StatusCode, Json<EndpointSlice>)> {
    info!(
        "Creating endpointslice: {}/{}",
        namespace, endpointslice.metadata.name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "endpointslices")
        .with_namespace(&namespace)
        .with_api_group("discovery.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &endpointslice.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // SetDefaults_EndpointSlice: each port protocol defaults to TCP.
    for p in endpointslice.ports.iter_mut() {
        if p.protocol.is_empty() {
            p.protocol = "TCP".to_string();
        }
    }

    // Field validation (mirrors upstream ValidateEndpointSlice).
    {
        let errs =
            rusternetes_common::validation::endpointslice::validate_endpoint_slice(&endpointslice);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    endpointslice.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    endpointslice.metadata.ensure_uid();
    endpointslice.metadata.ensure_creation_timestamp();

    // Ensure managed-by label is set
    endpointslice
        .metadata
        .labels
        .get_or_insert_with(Default::default)
        .entry("endpointslice.kubernetes.io/managed-by".to_string())
        .or_insert_with(|| "endpointslice-controller.k8s.io".to_string());

    // Check for dry-run
    if crate::handlers::dryrun::is_dry_run(&params) {
        return Ok((StatusCode::OK, Json(endpointslice)));
    }

    let key = build_key(
        "endpointslices",
        Some(&namespace),
        &endpointslice.metadata.name,
    );
    let created = state.storage.create(&key, &endpointslice).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

/// Get endpointslice
pub async fn get_endpointslice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<EndpointSlice>> {
    debug!("Getting endpointslice: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "endpointslices")
        .with_namespace(&namespace)
        .with_api_group("discovery.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("endpointslices", Some(&namespace), &name);
    let endpointslice = state.storage.get(&key).await?;

    Ok(Json(endpointslice))
}

/// List endpointslices in namespace
pub async fn list_endpointslices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    debug!("Listing endpointslices in namespace: {}", namespace);

    // Check if this is a watch request
    if params.watch.unwrap_or(false) {
        return crate::handlers::watch::watch_endpointslices(
            State(state),
            Extension(auth_ctx),
            Path(namespace),
            Query(params),
        )
        .await;
    }

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "endpointslices")
        .with_namespace(&namespace)
        .with_api_group("discovery.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("endpointslices", Some(&namespace));
    let mut endpointslices = state.storage.list::<EndpointSlice>(&prefix).await?;

    // Apply field and label selector filtering
    let mut params_map = HashMap::new();
    if let Some(fs) = params.field_selector {
        params_map.insert("fieldSelector".to_string(), fs);
    }
    if let Some(ls) = params.label_selector {
        params_map.insert("labelSelector".to_string(), ls);
    }
    crate::handlers::filtering::apply_selectors(&mut endpointslices, &params_map)?;

    let mut list = List::new("EndpointSliceList", "discovery.k8s.io/v1", endpointslices);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all endpointslices across all namespaces
pub async fn list_all_endpointslices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    debug!("Listing all endpointslices");

    // Check if this is a watch request
    if params.watch.unwrap_or(false) {
        return crate::handlers::watch::watch_cluster_scoped::<EndpointSlice>(
            state,
            auth_ctx,
            "endpointslices",
            "discovery.k8s.io",
            params,
        )
        .await;
    }

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "endpointslices")
        .with_api_group("discovery.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("endpointslices", None);
    let mut endpointslices = state.storage.list::<EndpointSlice>(&prefix).await?;

    // Apply field and label selector filtering
    let mut params_map = HashMap::new();
    if let Some(fs) = params.field_selector {
        params_map.insert("fieldSelector".to_string(), fs);
    }
    if let Some(ls) = params.label_selector {
        params_map.insert("labelSelector".to_string(), ls);
    }
    crate::handlers::filtering::apply_selectors(&mut endpointslices, &params_map)?;

    let mut list = List::new("EndpointSliceList", "discovery.k8s.io/v1", endpointslices);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// Update endpointslice
pub async fn update_endpointslice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut endpointslice): DumpingJson<EndpointSlice>,
) -> Result<Json<EndpointSlice>> {
    info!("Updating endpointslice: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "endpointslices")
        .with_namespace(&namespace)
        .with_api_group("discovery.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    endpointslice.metadata.name = name.clone();
    endpointslice.metadata.namespace = Some(namespace.clone());

    let key = build_key("endpointslices", Some(&namespace), &name);

    // Field validation + addressType immutability (upstream
    // ValidateEndpointSliceUpdate). addressType (IPv4/IPv6/FQDN) cannot change.
    {
        let old_slice: EndpointSlice = state.storage.get(&key).await?;
        let errs = rusternetes_common::validation::endpointslice::validate_endpoint_slice_update(
            &endpointslice,
            &old_slice,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Check for dry-run
    if crate::handlers::dryrun::is_dry_run(&params) {
        return Ok(Json(endpointslice));
    }

    let updated = state.storage.update(&key, &endpointslice).await?;

    Ok(Json(updated))
}

/// Delete endpointslice
pub async fn delete_endpointslice(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<EndpointSlice>> {
    info!("Deleting endpointslice: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "endpointslices")
        .with_namespace(&namespace)
        .with_api_group("discovery.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("endpointslices", Some(&namespace), &name);

    // Get the resource to check if it exists
    let endpointslice: EndpointSlice = state.storage.get(&key).await?;

    // Check for dry-run
    if crate::handlers::dryrun::is_dry_run(&params) {
        info!(
            "Dry-run: EndpointSlice {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(endpointslice));
    }

    let has_finalizers = crate::handlers::finalizers::handle_delete_with_finalizers(
        &*state.storage,
        &key,
        &endpointslice,
    )
    .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: EndpointSlice = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(endpointslice))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(
    patch_endpointslice,
    EndpointSlice,
    "endpointslices",
    "discovery.k8s.io"
);

pub async fn deletecollection_endpointslices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection endpointslices in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "endpointslices")
        .with_namespace(&namespace)
        .with_api_group("discovery.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: EndpointSlice collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all endpointslices in the namespace
    let prefix = build_prefix("endpointslices", Some(&namespace));
    let mut items = state.storage.list::<EndpointSlice>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("endpointslices", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} endpointslices deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
