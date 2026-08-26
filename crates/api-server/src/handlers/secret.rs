use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::Secret,
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
    Json(mut secret): Json<Secret>,
) -> Result<(StatusCode, Json<Secret>)> {
    info!(
        "Creating secret: {} in namespace: {}",
        secret.metadata.name, namespace
    );

    // Validate resource name
    crate::handlers::validation::validate_resource_name(&secret.metadata.name)?;

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "secrets")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace is set from the URL path
    secret.metadata.namespace = Some(namespace.clone());

    // Validate secret data keys (must be valid path segments)
    if let Some(ref data) = secret.data {
        for key in data.keys() {
            if key.is_empty()
                || key == "."
                || key == ".."
                || key.contains('/')
                || key.contains('\\')
            {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "Invalid key name \"{}\": a valid config key must consist of alphanumeric characters, '-', '_' or '.'",
                    key
                )));
            }
        }
    }
    if let Some(ref string_data) = secret.string_data {
        for key in string_data.keys() {
            if key.is_empty()
                || key == "."
                || key == ".."
                || key.contains('/')
                || key.contains('\\')
            {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "Invalid key name \"{}\": a valid config key must consist of alphanumeric characters, '-', '_' or '.'",
                    key
                )));
            }
        }
    }

    // Enrich metadata with system fields
    secret.metadata.ensure_uid();
    secret.metadata.ensure_creation_timestamp();

    // Normalize: convert stringData to base64-encoded data
    secret.normalize();

    let key = build_key("secrets", Some(&namespace), &secret.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Secret {}/{} validated successfully (not created)",
            namespace, secret.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(secret)));
    }

    let created = state.storage.create(&key, &secret).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Secret>> {
    debug!("Getting secret: {} in namespace: {}", name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "secrets")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("secrets", Some(&namespace), &name);
    let secret = state.storage.get(&key).await?;

    Ok(Json(secret))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut secret): Json<Secret>,
) -> Result<Json<Secret>> {
    info!("Updating secret: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "secrets")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    secret.metadata.name = name.clone();
    secret.metadata.namespace = Some(namespace.clone());

    // Normalize: convert stringData to base64-encoded data
    secret.normalize();

    let key = build_key("secrets", Some(&namespace), &name);

    // Check if existing secret is immutable — only reject data/stringData changes
    if let Ok(existing) = state.storage.get::<Secret>(&key).await {
        if existing.immutable == Some(true) {
            // Compare data and stringData — reject if changed
            let data_changed = existing.data != secret.data;
            let string_data_changed = existing.string_data != secret.string_data;
            // Also reject changing immutable from true to false
            let immutable_changed = secret.immutable != Some(true) && secret.immutable.is_some();
            if data_changed || string_data_changed || immutable_changed {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "Secret \"{}\" is immutable",
                    name
                )));
            }
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Secret {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(secret));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &secret).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &secret).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_secret(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Secret>> {
    info!("Deleting secret: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "secrets")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("secrets", Some(&namespace), &name);

    // Get the resource to check if it exists
    let secret: Secret = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Secret {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(secret));
    }

    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers(&*state.storage, &key, &secret)
            .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Secret = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(secret))
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
        return crate::handlers::watch::watch_namespaced::<Secret>(
            state,
            auth_ctx,
            namespace,
            "secrets",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing secrets in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "secrets")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("secrets", Some(&namespace));
    let mut secrets: Vec<Secret> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut secrets, &params)?;

    let list = List::new("SecretList", "v1", secrets);
    Ok(Json(list).into_response())
}

/// List all secrets across all namespaces
pub async fn list_all_secrets(
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
        return crate::handlers::watch::watch_cluster_scoped::<Secret>(
            state,
            auth_ctx,
            "secrets",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all secrets");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "secrets").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("secrets", None);
    let mut secrets = state.storage.list::<Secret>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut secrets, &params)?;

    let list = List::new("SecretList", "v1", secrets);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, Secret, "secrets", "");

pub async fn deletecollection_secrets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection secrets in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "secrets")
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
        info!("Dry-run: Secret collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all secrets in the namespace
    let prefix = build_prefix("secrets", Some(&namespace));
    let mut items = state.storage.list::<Secret>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("secrets", Some(&namespace), &item.metadata.name);

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
        "DeleteCollection completed: {} secrets deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
