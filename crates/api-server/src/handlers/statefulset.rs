use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::StatefulSet,
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
) -> Result<(StatusCode, Json<StatefulSet>)> {
    let mut statefulset: StatefulSet = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!(
        "Creating statefulset: {}/{}",
        namespace, statefulset.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "statefulsets")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    statefulset.metadata.namespace = Some(namespace.clone());
    statefulset.metadata.ensure_uid();
    statefulset.metadata.ensure_creation_timestamp();

    // Apply K8s defaults (SetDefaults_StatefulSet + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_statefulset_defaults(&mut statefulset);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: StatefulSet {}/{} validated successfully (not created)",
            namespace, statefulset.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(statefulset)));
    }

    let key = build_key("statefulsets", Some(&namespace), &statefulset.metadata.name);
    let created = state.storage.create(&key, &statefulset).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<StatefulSet>> {
    debug!("Getting statefulset: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "statefulsets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("statefulsets", Some(&namespace), &name);
    let statefulset = state.storage.get(&key).await?;

    Ok(Json(statefulset))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<StatefulSet>> {
    let mut statefulset: StatefulSet = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!("Updating statefulset: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "statefulsets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    statefulset.metadata.name = name.clone();
    statefulset.metadata.namespace = Some(namespace.clone());

    // Apply K8s defaults (SetDefaults_StatefulSet + SetDefaults_PodSpec + SetDefaults_Container)
    crate::handlers::defaults::apply_statefulset_defaults(&mut statefulset);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: StatefulSet {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(statefulset));
    }

    let key = build_key("statefulsets", Some(&namespace), &name);

    // Compute the updateRevision from the new template. If the template changed,
    // this produces a different hash. K8s conformance tests expect updateRevision
    // to be set immediately after an update, not on the next controller cycle.
    {
        use sha2::{Digest, Sha256};
        let tmpl_value = serde_json::to_value(&statefulset.spec.template).unwrap_or_default();
        let serialized = serde_json::to_string(&tmpl_value).unwrap_or_default();
        let hash = Sha256::digest(serialized.as_bytes());
        let new_revision = format!(
            "{:010x}",
            u64::from_be_bytes(hash[..8].try_into().unwrap_or([0u8; 8]))
        );

        let status = statefulset.status.get_or_insert({
            rusternetes_common::resources::StatefulSetStatus {
                replicas: 0,
                ready_replicas: None,
                current_replicas: None,
                updated_replicas: None,
                available_replicas: None,
                collision_count: None,
                observed_generation: None,
                current_revision: None,
                update_revision: None,
                conditions: None,
            }
        });
        let old_update_rev = status.update_revision.clone();
        status.update_revision = Some(new_revision.clone());
        // If currentRevision wasn't set yet, initialize it to the updateRevision
        if status.current_revision.is_none() {
            status.current_revision = Some(new_revision);
        }
        info!(
            "StatefulSet {}/{} update: old_updateRevision={:?}, new_updateRevision={:?}",
            namespace, name, old_update_rev, status.update_revision
        );
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &statefulset).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &statefulset).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_statefulset(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<StatefulSet>> {
    info!("Deleting statefulset: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "statefulsets")
        .with_namespace(&namespace)
        .with_api_group("apps")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("statefulsets", Some(&namespace), &name);

    // Get the resource to check if it exists
    let statefulset: StatefulSet = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: StatefulSet {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(statefulset));
    }

    let propagation_policy = params.get("propagationPolicy").map(|s| s.as_str());
    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers_and_propagation(
            &*state.storage,
            &key,
            &statefulset,
            propagation_policy,
        )
        .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: StatefulSet = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(statefulset))
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
        return crate::handlers::watch::watch_namespaced::<StatefulSet>(
            state,
            auth_ctx,
            namespace,
            "statefulsets",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing statefulsets in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "statefulsets")
        .with_namespace(&namespace)
        .with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("statefulsets", Some(&namespace));
    let mut statefulsets: Vec<StatefulSet> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut statefulsets, &params)?;

    let list = List::new("StatefulSetList", "apps/v1", statefulsets);
    Ok(Json(list).into_response())
}

/// List all statefulsets across all namespaces
pub async fn list_all_statefulsets(
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
        return crate::handlers::watch::watch_cluster_scoped::<StatefulSet>(
            state,
            auth_ctx,
            "statefulsets",
            "apps",
            watch_params,
        )
        .await;
    }

    debug!("Listing all statefulsets");

    // Check authorization (cluster-wide list)
    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "statefulsets").with_api_group("apps");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("statefulsets", None);
    let mut statefulsets = state.storage.list::<StatefulSet>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut statefulsets, &params)?;

    let list = List::new("StatefulSetList", "apps/v1", statefulsets);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, StatefulSet, "statefulsets", "apps");

pub async fn deletecollection_statefulsets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    info!(
        "DeleteCollection statefulsets in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "statefulsets")
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
        info!("Dry-run: StatefulSet collection would be deleted (not deleted)");
        return Ok(Json(serde_json::json!({
            "kind": "Status", "apiVersion": "v1", "metadata": {},
            "status": "Success", "code": 200
        })));
    }

    // Get all statefulsets in the namespace
    let prefix = build_prefix("statefulsets", Some(&namespace));
    let mut items = state.storage.list::<StatefulSet>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("statefulsets", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} statefulsets deleted",
        deleted_count
    );
    // K8s returns a Status object for deleteCollection
    // See: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/delete.go:340
    Ok(Json(serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Success",
        "code": 200,
        "details": {
            "kind": "StatefulSet"
        }
    })))
}
