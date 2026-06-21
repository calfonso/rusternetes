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
    resources::{FlowSchema, PriorityLevelConfiguration},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

// PriorityLevelConfiguration handlers

pub async fn create_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut plc): DumpingJson<PriorityLevelConfiguration>,
) -> Result<(StatusCode, Json<PriorityLevelConfiguration>)> {
    info!("Creating PriorityLevelConfiguration: {}", plc.metadata.name);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "prioritylevelconfigurations")
        .with_api_group("flowcontrol.apiserver.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &plc.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Validate spec (upstream APF ValidatePriorityLevelConfigurationSpec):
    // type/name coupling, exempt/limited coupling, limited + queuing config.
    let errs = rusternetes_common::validation::prioritylevelconfiguration::validate_priority_level_configuration(&plc);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Enrich metadata with system fields
    plc.metadata.ensure_uid();
    plc.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PriorityLevelConfiguration validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(plc)));
    }

    let key = build_key("prioritylevelconfigurations", None, &plc.metadata.name);
    let created = state.storage.create(&key, &plc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<PriorityLevelConfiguration>> {
    debug!("Getting PriorityLevelConfiguration: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "prioritylevelconfigurations")
        .with_api_group("flowcontrol.apiserver.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("prioritylevelconfigurations", None, &name);
    let plc = state.storage.get(&key).await?;

    Ok(Json(plc))
}

pub async fn update_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut plc): DumpingJson<PriorityLevelConfiguration>,
) -> Result<Json<PriorityLevelConfiguration>> {
    info!("Updating PriorityLevelConfiguration: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "prioritylevelconfigurations")
        .with_api_group("flowcontrol.apiserver.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    plc.metadata.name = name.clone();
    plc.kind = "PriorityLevelConfiguration".to_string();
    plc.api_version = "flowcontrol.apiserver.k8s.io/v1".to_string();

    // Field validation on update (upstream re-runs ValidatePriorityLevelConfiguration
    // on the new object — the spec is mutable, no immutability). The create path
    // validated but the update path previously persisted PUTs unchecked.
    {
        let errs = rusternetes_common::validation::prioritylevelconfiguration::validate_priority_level_configuration(&plc);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PriorityLevelConfiguration validated successfully (not updated)");
        return Ok(Json(plc));
    }

    let key = build_key("prioritylevelconfigurations", None, &name);

    // Get existing for resourceVersion check
    let existing: Option<PriorityLevelConfiguration> = state.storage.get(&key).await.ok();
    if let Some(ref existing) = existing {
        crate::handlers::lifecycle::check_resource_version(
            existing.metadata.resource_version.as_deref(),
            plc.metadata.resource_version.as_deref(),
            &name,
        )?;
    }

    // Preserve status from existing
    if let Some(ref existing) = existing {
        if plc.status.is_none() {
            plc.status = existing.status.clone();
        }
    }

    let result = match state.storage.update(&key, &plc).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &plc).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_priority_level_configuration(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PriorityLevelConfiguration>> {
    info!("Deleting PriorityLevelConfiguration: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "prioritylevelconfigurations")
        .with_api_group("flowcontrol.apiserver.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("prioritylevelconfigurations", None, &name);

    // Get the resource for finalizer handling
    let resource: PriorityLevelConfiguration = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PriorityLevelConfiguration validated successfully (not deleted)");
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
        let updated: PriorityLevelConfiguration = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_priority_level_configurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<PriorityLevelConfiguration>(
            state,
            auth_ctx,
            "prioritylevelconfigurations",
            "flowcontrol.apiserver.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing PriorityLevelConfigurations");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "prioritylevelconfigurations")
        .with_api_group("flowcontrol.apiserver.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let prefix = build_prefix("prioritylevelconfigurations", None);
    let mut items = state
        .storage
        .list::<PriorityLevelConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new(
        "PriorityLevelConfigurationList",
        "flowcontrol.apiserver.k8s.io/v1",
        items,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

crate::patch_handler_cluster!(
    patch_priority_level_configuration,
    PriorityLevelConfiguration,
    "prioritylevelconfigurations",
    "flowcontrol.apiserver.k8s.io"
);

// FlowSchema handlers

pub async fn create_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut fs): DumpingJson<FlowSchema>,
) -> Result<(StatusCode, Json<FlowSchema>)> {
    info!("Creating FlowSchema: {}", fs.metadata.name);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "flowschemas")
        .with_api_group("flowcontrol.apiserver.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &fs.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Validate spec (upstream APF ValidateFlowSchemaSpec): matchingPrecedence,
    // priorityLevelConfiguration ref, and rules subjects/resource/nonResource.
    let errs = rusternetes_common::validation::flowschema::validate_flow_schema(&fs);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Enrich metadata with system fields
    fs.metadata.ensure_uid();
    fs.metadata.ensure_creation_timestamp();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: FlowSchema validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(fs)));
    }

    let key = build_key("flowschemas", None, &fs.metadata.name);
    let created = state.storage.create(&key, &fs).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<FlowSchema>> {
    debug!("Getting FlowSchema: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "flowschemas")
        .with_api_group("flowcontrol.apiserver.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("flowschemas", None, &name);
    let fs = state.storage.get(&key).await?;

    Ok(Json(fs))
}

pub async fn update_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut fs): DumpingJson<FlowSchema>,
) -> Result<Json<FlowSchema>> {
    info!("Updating FlowSchema: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "flowschemas")
        .with_api_group("flowcontrol.apiserver.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    fs.metadata.name = name.clone();

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: FlowSchema validated successfully (not updated)");
        return Ok(Json(fs));
    }

    let key = build_key("flowschemas", None, &name);
    let result = match state.storage.update(&key, &fs).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &fs).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_flow_schema(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<FlowSchema>> {
    info!("Deleting FlowSchema: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "flowschemas")
        .with_api_group("flowcontrol.apiserver.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let key = build_key("flowschemas", None, &name);

    // Get the resource for finalizer handling
    let resource: FlowSchema = state.storage.get(&key).await?;

    // Check for dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: FlowSchema validated successfully (not deleted)");
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
        let updated: FlowSchema = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list_flow_schemas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<FlowSchema>(
            state,
            auth_ctx,
            "flowschemas",
            "flowcontrol.apiserver.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing FlowSchemas");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "flowschemas")
        .with_api_group("flowcontrol.apiserver.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }

    let prefix = build_prefix("flowschemas", None);
    let mut items = state.storage.list::<FlowSchema>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    let mut list = List::new("FlowSchemaList", "flowcontrol.apiserver.k8s.io/v1", items);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

crate::patch_handler_cluster!(
    patch_flow_schema,
    FlowSchema,
    "flowschemas",
    "flowcontrol.apiserver.k8s.io"
);

pub async fn deletecollection_prioritylevelconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection prioritylevelconfigurations with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "prioritylevelconfigurations",
    )
    .with_api_group("flowcontrol.apiserver.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PriorityLevelConfiguration collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all prioritylevelconfigurations
    let prefix = build_prefix("prioritylevelconfigurations", None);
    let mut items = state
        .storage
        .list::<PriorityLevelConfiguration>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("prioritylevelconfigurations", None, &item.metadata.name);

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
        "DeleteCollection completed: {} prioritylevelconfigurations deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

pub async fn deletecollection_flowschemas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection flowschemas");
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "flowschemas")
        .with_api_group("flowcontrol.apiserver.k8s.io");
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => return Err(rusternetes_common::Error::Forbidden(reason)),
    }
    if crate::handlers::dryrun::is_dry_run(&params) {
        return Ok(StatusCode::OK);
    }
    let prefix = build_prefix("flowschemas", None);
    let items: Vec<FlowSchema> = state.storage.list(&prefix).await?;
    for item in items {
        let key = build_key("flowschemas", None, &item.metadata.name);
        let _ = state.storage.delete(&key).await;
    }
    Ok(StatusCode::OK)
}
