use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    admission::{GroupVersionKind, Operation},
    authz::{Decision, RequestAttributes},
    resources::ConfigMap,
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
    DumpingJson(mut configmap): DumpingJson<ConfigMap>,
) -> Result<(StatusCode, Json<ConfigMap>)> {
    info!(
        "Creating configmap: {} in namespace: {}",
        configmap.metadata.name, namespace
    );

    // Validate resource name
    crate::handlers::validation::validate_resource_name(&configmap.metadata.name)?;

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace is set from the URL path
    configmap.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    configmap.metadata.ensure_uid();
    configmap.metadata.ensure_creation_timestamp();

    // Run ValidatingAdmissionPolicy checks
    let gvk = GroupVersionKind {
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
    };
    let cm_value = serde_json::to_value(&configmap).ok();
    state
        .webhook_manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &gvk,
            cm_value.as_ref(),
            None,
            Some("configmaps"),
            Some(&namespace),
        )
        .await?;

    // Run admission webhooks (mutating + validating)
    {
        let gvr = rusternetes_common::admission::GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "configmaps".to_string(),
        };
        let user = &user_for_webhook;
        let user_info = rusternetes_common::admission::UserInfo {
            username: user.username.clone(),
            uid: user.uid.clone(),
            groups: user.groups.clone(),
        };
        let cm_val = serde_json::to_value(&configmap).ok();
        // Run mutating webhooks
        let (_response, mutated_obj) = state
            .webhook_manager
            .run_mutating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Create,
                &gvk,
                &gvr,
                Some(&namespace),
                &configmap.metadata.name,
                cm_val.clone(),
                None,
                &user_info,
                is_dry_run,
            )
            .await?;
        // Check if the mutating webhook DENIED the request.
        // K8s mutating webhooks CAN deny — the denial must be enforced.
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = &_response {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
        if let Some(mutated) = mutated_obj {
            if let Ok(m) = serde_json::from_value::<ConfigMap>(mutated) {
                configmap = m;
            }
        }
        // Run validating webhooks
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Create,
                &gvk,
                &gvr,
                Some(&namespace),
                &configmap.metadata.name,
                serde_json::to_value(&configmap).ok(),
                None,
                &user_info,
                is_dry_run,
            )
            .await?
        {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    let key = build_key("configmaps", Some(&namespace), &configmap.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ConfigMap {}/{} validated successfully (not created)",
            namespace, configmap.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(configmap)));
    }

    let created = state.storage.create(&key, &configmap).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ConfigMap>> {
    debug!("Getting configmap: {} in namespace: {}", name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("configmaps", Some(&namespace), &name);
    let configmap = state.storage.get(&key).await?;

    Ok(Json(configmap))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut configmap): DumpingJson<ConfigMap>,
) -> Result<Json<ConfigMap>> {
    info!("Updating configmap: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    configmap.metadata.name = name.clone();
    configmap.metadata.namespace = Some(namespace.clone());

    // Run ValidatingAdmissionPolicy checks for UPDATE
    let gvk = GroupVersionKind {
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
    };
    let cm_value = serde_json::to_value(&configmap).ok();
    state
        .webhook_manager
        .run_validating_admission_policies_ext(
            &Operation::Update,
            &gvk,
            cm_value.as_ref(),
            None,
            Some("configmaps"),
            Some(&namespace),
        )
        .await?;

    // Run admission webhooks (mutating + validating) for UPDATE
    {
        let gvr = rusternetes_common::admission::GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "configmaps".to_string(),
        };
        let user = &user_for_webhook;
        let user_info = rusternetes_common::admission::UserInfo {
            username: user.username.clone(),
            uid: user.uid.clone(),
            groups: user.groups.clone(),
        };
        let cm_val = serde_json::to_value(&configmap).ok();
        // Run mutating webhooks
        let (_response, mutated_obj) = state
            .webhook_manager
            .run_mutating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Update,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                cm_val.clone(),
                None,
                &user_info,
                is_dry_run,
            )
            .await?;
        // Check if the mutating webhook DENIED the request.
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = &_response {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
        if let Some(mutated) = mutated_obj {
            if let Ok(m) = serde_json::from_value::<ConfigMap>(mutated) {
                configmap = m;
            }
        }
        // Run validating webhooks
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &rusternetes_common::admission::Operation::Update,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                serde_json::to_value(&configmap).ok(),
                None,
                &user_info,
            )
            .await?
        {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    let key = build_key("configmaps", Some(&namespace), &name);

    // Check if existing configmap is immutable — K8s only prevents changes to
    // data, binaryData, and immutable fields. Metadata changes (labels, annotations)
    // are still allowed.
    if let Ok(existing) = state.storage.get::<ConfigMap>(&key).await {
        if existing.immutable == Some(true) {
            let data_changed = existing.data != configmap.data;
            let binary_data_changed = existing.binary_data != configmap.binary_data;
            let immutable_changed =
                configmap.immutable != Some(true) && configmap.immutable != existing.immutable;
            if data_changed || binary_data_changed || immutable_changed {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "ConfigMap \"{}/{}\" is immutable",
                    namespace, name
                )));
            }
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ConfigMap {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(configmap));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &configmap).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &configmap).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_configmap(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Json<ConfigMap>> {
    info!("Deleting configmap: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("configmaps", Some(&namespace), &name);

    // Get the resource to check if it exists
    let configmap: ConfigMap = state.storage.get(&key).await?;

    // Enforce deleteOptions.preconditions.{resourceVersion,uid} before mutating
    // anything. Upstream: pkg/registry/generic/registry/store.go::Delete calls
    // preconditions.Check() before invoking storage.Delete; a mismatch returns
    // 409 Conflict with reason `Conflict`.
    crate::handlers::lifecycle::check_delete_preconditions(&body, &configmap.metadata, &name)?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: ConfigMap {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(configmap));
    }

    // Handle deletion with finalizers
    let has_finalizers = crate::handlers::finalizers::handle_delete_with_finalizers(
        &*state.storage,
        &key,
        &configmap,
    )
    .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ConfigMap = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(configmap))
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
        info!(
            "Configmap watch request for namespace {}: rv={:?}, sendInitialEvents={:?}",
            namespace,
            params.get("resourceVersion"),
            params.get("sendInitialEvents"),
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
        return crate::handlers::watch::watch_namespaced::<ConfigMap>(
            state,
            auth_ctx,
            namespace,
            "configmaps",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing configmaps in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("configmaps", Some(&namespace));
    let mut configmaps: Vec<ConfigMap> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configmaps, &params)?;

    // Deterministic sort by name so `?continue` chains are stable
    // regardless of underlying storage iteration order.
    configmaps.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));

    paginate_configmaps_response(&state, configmaps, &params, "ConfigMapList").await
}

/// List all configmaps across all namespaces
pub async fn list_all_configmaps(
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
        return crate::handlers::watch::watch_cluster_scoped::<ConfigMap>(
            state,
            auth_ctx,
            "configmaps",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all configmaps");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "configmaps").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("configmaps", None);
    let mut configmaps = state.storage.list::<ConfigMap>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configmaps, &params)?;

    // Cluster-scoped LIST sorts by (namespace, name) so paginated
    // chains across namespaces are deterministic.
    configmaps.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    paginate_configmaps_response(&state, configmaps, &params, "ConfigMapList").await
}

/// Apply `?limit` / `?continue` semantics over an already-sorted slice
/// of ConfigMaps and wrap the response as a `ConfigMapList` with
/// `metadata.continue`, `metadata.remainingItemCount`, and
/// `metadata.resourceVersion`. Mirrors the pattern used by the Pod
/// handler.
async fn paginate_configmaps_response(
    state: &Arc<ApiServerState>,
    configmaps: Vec<ConfigMap>,
    params: &HashMap<String, String>,
    list_kind: &str,
) -> Result<axum::response::Response> {
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();

    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => crate::handlers::list_resource_version(&configmaps),
    };

    let paginated =
        match rusternetes_common::paginate(configmaps, pagination_params, &resource_version) {
            Ok(p) => p,
            Err(e) => {
                if e.message.contains("410 Gone") {
                    let mut status =
                        rusternetes_common::Status::failure(&e.message, "Expired", 410);
                    if let Some(token) = e.fresh_continue_token {
                        status.metadata = Some(rusternetes_common::ListMeta {
                            resource_version: Some(resource_version),
                            continue_token: Some(token),
                            remaining_item_count: None,
                        });
                    }
                    return Ok((axum::http::StatusCode::GONE, axum::Json(status)).into_response());
                }
                // Malformed continue token / encoding error -> 400 BadRequest
                // Status object (kind=Status), matching upstream apiserver.
                return Err(rusternetes_common::Error::BadRequest(e.message));
            }
        };

    let mut list = List::new(list_kind, "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);
    Ok(axum::Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, ConfigMap, "configmaps", "");

pub async fn deletecollection_configmaps(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection configmaps in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "configmaps")
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
        info!("Dry-run: ConfigMap collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all configmaps in the namespace
    let prefix = build_prefix("configmaps", Some(&namespace));
    let mut items = state.storage.list::<ConfigMap>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("configmaps", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} configmaps deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
