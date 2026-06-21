use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::VolumeAttachment,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_volumeattachment(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut va): DumpingJson<VolumeAttachment>,
) -> Result<(StatusCode, Json<VolumeAttachment>)> {
    info!("Creating VolumeAttachment: {}", va.metadata.name);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &va.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Validate spec (upstream storage ValidateVolumeAttachment): attacher,
    // source exactly-one, nodeName.
    let errs = rusternetes_common::validation::volumeattachment::validate_volume_attachment(&va);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "volumeattachments")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    va.metadata.ensure_uid();
    va.metadata.ensure_creation_timestamp();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeAttachment validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(va)));
    }

    let key = build_key("volumeattachments", None, &va.metadata.name);
    let created = state.storage.create(&key, &va).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_volumeattachment(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<VolumeAttachment>> {
    debug!("Getting VolumeAttachment: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "volumeattachments")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumeattachments", None, &name);
    let va = state.storage.get(&key).await?;

    Ok(Json(va))
}

pub async fn list_volumeattachments(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing all VolumeAttachments");

    // Honor `?watch=true` on the collection endpoint (informer/Lens path).
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<VolumeAttachment>(
            state,
            auth_ctx,
            "volumeattachments",
            "storage.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "volumeattachments")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("volumeattachments", None);
    let mut vas = state.storage.list::<VolumeAttachment>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut vas, &params)?;

    let mut list = List::new("VolumeAttachmentList", "storage.k8s.io/v1", vas);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::response::IntoResponse::into_response(Json(list)))
}

pub async fn update_volumeattachment(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut va): DumpingJson<VolumeAttachment>,
) -> Result<Json<VolumeAttachment>> {
    info!("Updating VolumeAttachment: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "volumeattachments")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    va.metadata.name = name.clone();

    let key = build_key("volumeattachments", None, &name);

    // Spec is read-only on update (upstream ValidateVolumeAttachmentUpdate).
    if let Ok(old) = state
        .storage
        .get::<rusternetes_common::resources::VolumeAttachment>(&key)
        .await
    {
        let errs =
            rusternetes_common::validation::volumeattachment::validate_volume_attachment_update(
                &va, &old,
            );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: VolumeAttachment validated successfully (not updated)");
        return Ok(Json(va));
    }

    let updated = state.storage.update(&key, &va).await?;

    Ok(Json(updated))
}

pub async fn delete_volumeattachment(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<VolumeAttachment>> {
    info!("Deleting VolumeAttachment: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "volumeattachments")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("volumeattachments", None, &name);

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Get the resource for finalizer handling
    let resource: VolumeAttachment = state.storage.get(&key).await?;

    if is_dry_run {
        info!("Dry-run: VolumeAttachment validated successfully (not deleted)");
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
        let updated: VolumeAttachment = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(
    patch_volumeattachment,
    VolumeAttachment,
    "volumeattachments",
    "storage.k8s.io"
);

pub async fn deletecollection_volumeattachments(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection volumeattachments with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "volumeattachments")
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
        info!("Dry-run: VolumeAttachment collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all volumeattachments
    let prefix = build_prefix("volumeattachments", None);
    let mut items = state.storage.list::<VolumeAttachment>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("volumeattachments", None, &item.metadata.name);

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
        "DeleteCollection completed: {} volumeattachments deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::{
        resources::{VolumeAttachmentSource, VolumeAttachmentSpec},
        types::{ObjectMeta, TypeMeta},
    };

    fn create_test_volume_attachment(name: &str) -> VolumeAttachment {
        VolumeAttachment {
            type_meta: TypeMeta {
                kind: "VolumeAttachment".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: VolumeAttachmentSpec {
                attacher: "test-driver".to_string(),
                node_name: "node1".to_string(),
                source: VolumeAttachmentSource {
                    persistent_volume_name: Some("pv-123".to_string()),
                    inline_volume_spec: None,
                },
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn test_volumeattachment_serialization() {
        let va = create_test_volume_attachment("test-va");
        let json = serde_json::to_string(&va).unwrap();
        let deserialized: VolumeAttachment = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "test-va");
    }
}
