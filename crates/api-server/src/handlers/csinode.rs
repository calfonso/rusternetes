use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::CSINode,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_csinode(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<(StatusCode, Json<CSINode>)> {
    let mut node: CSINode = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!("Creating CSINode: {}", node.metadata.name);

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "csinodes")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    node.metadata.ensure_uid();
    node.metadata.ensure_creation_timestamp();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CSINode validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(node)));
    }

    let key = build_key("csinodes", None, &node.metadata.name);
    let created = state.storage.create(&key, &node).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_csinode(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<CSINode>> {
    debug!("Getting CSINode: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "csinodes")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("csinodes", None, &name);
    let node = state.storage.get(&key).await?;

    Ok(Json(node))
}

pub async fn list_csinodes(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<List<CSINode>>> {
    debug!("Listing all CSINodes");

    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "csinodes").with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("csinodes", None);
    let mut nodes = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut nodes, &params)?;

    let list = List::new("CSINodeList", "storage.k8s.io/v1", nodes);
    Ok(Json(list))
}

pub async fn update_csinode(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut node): Json<CSINode>,
) -> Result<Json<CSINode>> {
    info!("Updating CSINode: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "csinodes")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    node.metadata.name = name.clone();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CSINode validated successfully (not updated)");
        return Ok(Json(node));
    }

    let key = build_key("csinodes", None, &name);
    let updated = state.storage.update(&key, &node).await?;

    Ok(Json(updated))
}

pub async fn delete_csinode(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<CSINode>> {
    info!("Deleting CSINode: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "csinodes")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("csinodes", None, &name);

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Get the resource for finalizer handling
    let resource: CSINode = state.storage.get(&key).await?;

    if is_dry_run {
        info!("Dry-run: CSINode validated successfully (not deleted)");
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
        let updated: CSINode = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(patch_csinode, CSINode, "csinodes", "storage.k8s.io");

pub async fn deletecollection_csinodes(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection csinodes with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "csinodes")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CSINode collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all csinodes
    let prefix = build_prefix("csinodes", None);
    let mut items = state.storage.list::<CSINode>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("csinodes", None, &item.metadata.name);

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
        "DeleteCollection completed: {} csinodes deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::{
        resources::{CSINodeDriver, CSINodeSpec},
        types::{ObjectMeta, TypeMeta},
    };

    fn create_test_node(name: &str) -> CSINode {
        CSINode {
            type_meta: TypeMeta {
                kind: "CSINode".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: "test-driver".to_string(),
                    node_id: "node1-id".to_string(),
                    topology_keys: None,
                    allocatable: None,
                }],
            },
        }
    }

    #[tokio::test]
    async fn test_csinode_serialization() {
        let node = create_test_node("node1");
        let json = serde_json::to_string(&node).unwrap();
        let deserialized: CSINode = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "node1");
    }
}
