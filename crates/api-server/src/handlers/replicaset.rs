use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{ReplicaSet, ReplicaSetStatus},
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
    body: Bytes,
) -> Result<(StatusCode, Json<ReplicaSet>)> {
    let mut replicaset: ReplicaSet = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!(
        "Creating replicaset: {}/{}",
        namespace, replicaset.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "replicasets")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &replicaset.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Field validation (mirrors upstream ValidateReplicaSet).
    {
        let errs = rusternetes_common::validation::apps::validate_replicaset(&replicaset);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    replicaset.metadata.namespace = Some(namespace.clone());
    replicaset.metadata.ensure_uid();
    replicaset.metadata.ensure_creation_timestamp();
    crate::handlers::lifecycle::set_initial_generation(&mut replicaset.metadata);

    // Apply K8s defaults (SetDefaults_ReplicaSet + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_replicaset_defaults(&mut replicaset);

    // Initialize status if not present
    if replicaset.status.is_none() {
        replicaset.status = Some(ReplicaSetStatus {
            replicas: 0,
            fully_labeled_replicas: Some(0),
            ready_replicas: 0,
            available_replicas: 0,
            observed_generation: Some(0),
            conditions: None,
            terminating_replicas: None,
        });
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ReplicaSet {}/{} validated successfully (not created)",
            namespace, replicaset.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(replicaset)));
    }

    let key = build_key("replicasets", Some(&namespace), &replicaset.metadata.name);
    let created = state.storage.create(&key, &replicaset).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ReplicaSet>> {
    debug!("Getting replicaset: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "replicasets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("replicasets", Some(&namespace), &name);
    let replicaset = state.storage.get(&key).await?;

    Ok(Json(replicaset))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<ReplicaSet>> {
    let mut replicaset: ReplicaSet = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!("Updating replicaset: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "replicasets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    replicaset.metadata.name = name.clone();
    replicaset.metadata.namespace = Some(namespace.clone());
    if replicaset.type_meta.kind.is_empty() {
        replicaset.type_meta.kind = "ReplicaSet".to_string();
    }
    if replicaset.type_meta.api_version.is_empty() {
        replicaset.type_meta.api_version = "apps/v1".to_string();
    }

    // Apply K8s defaults (SetDefaults_ReplicaSet + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_replicaset_defaults(&mut replicaset);

    let key = build_key("replicasets", Some(&namespace), &name);

    // Load stored object so we can enforce upstream Strategy.PrepareForUpdate
    // (status reset + selector immutability) and ValidateReplicaSetUpdate.
    let old_replicaset: ReplicaSet = state.storage.get(&key).await?;

    crate::handlers::lifecycle::check_resource_version(
        old_replicaset.metadata.resource_version.as_deref(),
        replicaset.metadata.resource_version.as_deref(),
        &name,
    )?;

    crate::handlers::lifecycle::validate_selector_immutable(
        &old_replicaset.spec.selector,
        &replicaset.spec.selector,
        "ReplicaSet",
    )?;

    // Full spec validation on update (upstream ValidateReplicaSetUpdate re-runs
    // ValidateReplicaSetSpec on the new object). Only selector immutability was
    // checked before, so an otherwise-invalid spec slipped through on update.
    {
        let errs = rusternetes_common::validation::apps::validate_replicaset(&replicaset);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Status only mutates via /status; mirror upstream PrepareForUpdate.
    replicaset.status = old_replicaset.status.clone();

    // Increment generation if spec changed
    let old_value = serde_json::to_value(&old_replicaset)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    let new_value = serde_json::to_value(&replicaset)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    crate::handlers::lifecycle::maybe_increment_generation(
        &old_value,
        &new_value,
        &mut replicaset.metadata,
    );

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ReplicaSet {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(replicaset));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &replicaset).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &replicaset).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_replicaset(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ReplicaSet>> {
    info!("Deleting replicaset: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "replicasets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("replicasets", Some(&namespace), &name);
    let replicaset: ReplicaSet = state.storage.get(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=replicaset).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "apps",
        "v1",
        "ReplicaSet",
        "replicasets",
        Some(&namespace),
        &name,
        &replicaset,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: ReplicaSet {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(replicaset));
    }

    // Handle deletion with finalizers and propagation policy
    let propagation_policy = params.get("propagationPolicy").map(|s| s.as_str());
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers_and_propagation(
            &state.storage,
            &key,
            &replicaset,
            propagation_policy,
        )
        .await?;

    if deleted_immediately {
        Ok(Json(replicaset))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ReplicaSet = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
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
        return crate::handlers::watch::watch_namespaced::<ReplicaSet>(
            state,
            auth_ctx,
            namespace,
            "replicasets",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing replicasets in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "replicasets")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicasets", Some(&namespace));
    let mut replicasets: Vec<ReplicaSet> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut replicasets, &params)?;

    // Get the store revision for the list; never "0"/"" so informers can watch.
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &replicasets).await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            replicasets,
            Some(resource_version),
            "ReplicaSet",
        );
        return Ok(Json(table).into_response());
    }

    let mut list = List::new("ReplicaSetList", "apps/v1", replicasets);
    list.metadata.resource_version = Some(resource_version);
    Ok(Json(list).into_response())
}

/// List all replicasets across all namespaces
pub async fn list_all_replicasets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
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
        return crate::handlers::watch::watch_cluster_scoped::<ReplicaSet>(
            state,
            auth_ctx,
            "replicasets",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing all replicasets");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "replicasets").with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicasets", None);
    let mut replicasets = state.storage.list::<ReplicaSet>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut replicasets, &params)?;

    // Get the store revision for the list; never "0"/"" so informers can watch.
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &replicasets).await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            replicasets,
            Some(resource_version),
            "ReplicaSet",
        );
        return Ok(Json(table).into_response());
    }

    let mut list = List::new("ReplicaSetList", "apps/v1", replicasets);
    list.metadata.resource_version = Some(resource_version);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, ReplicaSet, "replicasets", "apps");

pub async fn deletecollection_replicasets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection replicasets in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "replicasets")
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
        info!("Dry-run: ReplicaSet collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all replicasets in the namespace
    let prefix = build_prefix("replicasets", Some(&namespace));
    let mut items = state.storage.list::<ReplicaSet>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("replicasets", Some(&namespace), &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "apps",
            "v1",
            "ReplicaSet",
            "replicasets",
            Some(&namespace),
            &item.metadata.name,
            &item,
            &user_for_webhook,
            false,
        )
        .await?;

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
        "DeleteCollection completed: {} replicasets deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
