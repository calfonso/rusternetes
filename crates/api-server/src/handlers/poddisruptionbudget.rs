use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::PodDisruptionBudget,
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
    Json(mut pdb): Json<PodDisruptionBudget>,
) -> Result<(StatusCode, Json<PodDisruptionBudget>)> {
    info!(
        "Creating poddisruptionbudget: {}/{}",
        namespace, pdb.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "poddisruptionbudgets")
        .with_namespace(&namespace)
        .with_api_group("policy");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    pdb.metadata.namespace = Some(namespace.clone());
    pdb.metadata.ensure_uid();
    pdb.metadata.ensure_creation_timestamp();

    let key = build_key("poddisruptionbudgets", Some(&namespace), &pdb.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PodDisruptionBudget {}/{} validated successfully (not created)",
            namespace, pdb.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(pdb)));
    }

    let created = state.storage.create(&key, &pdb).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<PodDisruptionBudget>> {
    debug!("Getting poddisruptionbudget: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "poddisruptionbudgets")
        .with_namespace(&namespace)
        .with_api_group("policy")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("poddisruptionbudgets", Some(&namespace), &name);
    let pdb = state.storage.get(&key).await?;

    Ok(Json(pdb))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut pdb): Json<PodDisruptionBudget>,
) -> Result<Json<PodDisruptionBudget>> {
    info!("Updating poddisruptionbudget: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "poddisruptionbudgets")
        .with_namespace(&namespace)
        .with_api_group("policy")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    pdb.metadata.name = name.clone();
    pdb.metadata.namespace = Some(namespace.clone());

    let key = build_key("poddisruptionbudgets", Some(&namespace), &name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PodDisruptionBudget {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(pdb));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &pdb).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &pdb).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PodDisruptionBudget>> {
    info!("Deleting poddisruptionbudget: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "poddisruptionbudgets")
        .with_namespace(&namespace)
        .with_api_group("policy")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("poddisruptionbudgets", Some(&namespace), &name);

    // Get the PDB for finalizer handling
    let pdb: PodDisruptionBudget = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: PodDisruptionBudget {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(pdb));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &pdb)
            .await?;

    if deleted_immediately {
        Ok(Json(pdb))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: PodDisruptionBudget = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
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
        return crate::handlers::watch::watch_namespaced::<PodDisruptionBudget>(
            state,
            auth_ctx,
            namespace,
            "poddisruptionbudgets",
            "policy",
            watch_params,
        )
        .await;
    }

    debug!("Listing poddisruptionbudgets in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "poddisruptionbudgets")
        .with_namespace(&namespace)
        .with_api_group("policy");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("poddisruptionbudgets", Some(&namespace));
    let mut pdbs: Vec<PodDisruptionBudget> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pdbs, &params)?;

    let list = List::new("PodDisruptionBudgetList", "policy/v1", pdbs);
    Ok(Json(list).into_response())
}

pub async fn list_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
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
        return crate::handlers::watch::watch_cluster_scoped::<PodDisruptionBudget>(
            state,
            auth_ctx,
            "poddisruptionbudgets",
            "policy",
            watch_params,
        )
        .await;
    }

    debug!("Listing all poddisruptionbudgets");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "poddisruptionbudgets")
        .with_api_group("policy");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("poddisruptionbudgets", None);
    let mut pdbs: Vec<PodDisruptionBudget> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pdbs, &params)?;

    let list = List::new("PodDisruptionBudgetList", "policy/v1", pdbs);
    Ok(Json(list).into_response())
}

// Status subresource handlers
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<PodDisruptionBudget>> {
    debug!("Getting poddisruptionbudget status: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "poddisruptionbudgets/status")
        .with_namespace(&namespace)
        .with_api_group("policy")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("poddisruptionbudgets", Some(&namespace), &name);
    let pdb = state.storage.get(&key).await?;

    Ok(Json(pdb))
}

pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Json(pdb): Json<PodDisruptionBudget>,
) -> Result<Json<PodDisruptionBudget>> {
    info!(
        "Updating poddisruptionbudget status: {}/{}",
        namespace, name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "poddisruptionbudgets/status")
        .with_namespace(&namespace)
        .with_api_group("policy")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("poddisruptionbudgets", Some(&namespace), &name);

    // Get existing PDB to preserve spec
    let mut existing: PodDisruptionBudget = state.storage.get(&key).await?;

    // Update only the status field
    existing.status = pdb.status;

    let updated = state.storage.update(&key, &existing).await?;

    Ok(Json(updated))
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, PodDisruptionBudget, "poddisruptionbudgets", "policy");

pub async fn deletecollection_poddisruptionbudgets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection poddisruptionbudgets in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "poddisruptionbudgets")
        .with_namespace(&namespace)
        .with_api_group("policy");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PodDisruptionBudget collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all poddisruptionbudgets in the namespace
    let prefix = build_prefix("poddisruptionbudgets", Some(&namespace));
    let mut items = state.storage.list::<PodDisruptionBudget>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key(
            "poddisruptionbudgets",
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
        "DeleteCollection completed: {} poddisruptionbudgets deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{IntOrString, PodDisruptionBudgetSpec};
    use rusternetes_common::types::LabelSelector;
    use std::collections::HashMap;

    #[test]
    fn test_pdb_handler_structure() {
        // Basic test to ensure handler structure is correct
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(2)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("test-pdb", "default", spec);
        assert_eq!(pdb.metadata.name, "test-pdb");
        assert_eq!(pdb.metadata.namespace, Some("default".to_string()));
    }
}
