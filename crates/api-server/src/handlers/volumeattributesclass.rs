use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::VolumeAttributesClass,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut vac): Json<VolumeAttributesClass>,
) -> Result<(StatusCode, Json<VolumeAttributesClass>)> {
    info!("Creating VolumeAttributesClass: {}", vac.metadata.name);

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "volumeattributesclasses")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    vac.metadata.ensure_uid();
    vac.metadata.ensure_creation_timestamp();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeAttributesClass validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(vac)));
    }

    let key = build_key("volumeattributesclasses", None, &vac.metadata.name);
    let created = state.storage.create(&key, &vac).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<VolumeAttributesClass>> {
    debug!("Getting VolumeAttributesClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "volumeattributesclasses")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumeattributesclasses", None, &name);
    let vac = state.storage.get(&key).await?;

    Ok(Json(vac))
}

pub async fn list_volumeattributesclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<List<VolumeAttributesClass>>> {
    debug!("Listing all VolumeAttributesClasses");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "volumeattributesclasses")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("volumeattributesclasses", None);
    let mut vacs = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vacs, &params)?;

    let list = List::new("VolumeAttributesClassList", "storage.k8s.io/v1", vacs);
    Ok(Json(list))
}

pub async fn update_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(mut vac): Json<VolumeAttributesClass>,
) -> Result<Json<VolumeAttributesClass>> {
    info!("Updating VolumeAttributesClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "volumeattributesclasses")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    vac.metadata.name = name.clone();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeAttributesClass validated successfully (not updated)");
        return Ok(Json(vac));
    }

    let key = build_key("volumeattributesclasses", None, &name);
    let updated = state.storage.update(&key, &vac).await?;

    Ok(Json(updated))
}

pub async fn delete_volumeattributesclass(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<VolumeAttributesClass>> {
    info!("Deleting VolumeAttributesClass: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "volumeattributesclasses")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumeattributesclasses", None, &name);

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Get the resource for finalizer handling
    let resource: VolumeAttributesClass = state.storage.get(&key).await?;

    if is_dry_run {
        info!("Dry-run: VolumeAttributesClass validated successfully (not deleted)");
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
        let updated: VolumeAttributesClass = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_volumeattributesclass,
    VolumeAttributesClass,
    "volumeattributesclasses",
    "storage.k8s.io"
);

pub async fn deletecollection_volumeattributesclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection volumeattributesclasses with params: {:?}",
        params
    );

    // Check authorization
    let attrs =
        RequestAttributes::new(auth_ctx.user, "deletecollection", "volumeattributesclasses")
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
        info!("Dry-run: VolumeAttributesClass collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all volumeattributesclasses
    let prefix = build_prefix("volumeattributesclasses", None);
    let mut items = state.storage.list::<VolumeAttributesClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("volumeattributesclasses", None, &item.metadata.name);

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
        "DeleteCollection completed: {} volumeattributesclasses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use std::collections::HashMap;

    fn create_test_vac(name: &str) -> VolumeAttributesClass {
        let mut params = HashMap::new();
        params.insert("type".to_string(), "ssd".to_string());
        params.insert("iops".to_string(), "3000".to_string());

        VolumeAttributesClass {
            type_meta: TypeMeta {
                kind: "VolumeAttributesClass".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            driver_name: "test-driver".to_string(),
            parameters: Some(params),
        }
    }

    #[tokio::test]
    async fn test_volumeattributesclass_serialization() {
        let vac = create_test_vac("fast-storage");
        let json = serde_json::to_string(&vac).unwrap();
        let deserialized: VolumeAttributesClass = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "fast-storage");
        assert_eq!(deserialized.driver_name, "test-driver");
    }
}
