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
    resources::PersistentVolumeClaim,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn create_pvc(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut pvc): DumpingJson<PersistentVolumeClaim>,
) -> Result<(StatusCode, Json<PersistentVolumeClaim>)> {
    info!(
        "Creating PersistentVolumeClaim: {}/{}",
        namespace, pvc.metadata.name
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "create", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &pvc.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Field validation (mirrors upstream ValidatePersistentVolumeClaim).
    {
        let errs = rusternetes_common::validation::pvc::validate_persistent_volume_claim(&pvc);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    pvc.metadata.namespace = Some(namespace.clone());

    // Apply DefaultStorageClass admission (sets default storage class if not specified)
    if let Err(e) = crate::admission::set_default_storage_class(&state.storage, &mut pvc).await {
        tracing::warn!(
            "Error applying DefaultStorageClass admission for PVC {}/{}: {}",
            namespace,
            pvc.metadata.name,
            e
        );
        // Continue anyway - don't fail PVC creation if default storage class can't be set
    }

    // LimitRange admission — reject PVCs whose storage request falls outside
    // the namespace's `type: PersistentVolumeClaim` min/max bounds.
    match crate::admission::apply_limit_range_to_pvc(&state.storage, &namespace, &mut pvc).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "PVC {}/{} violates a LimitRange constraint",
                namespace, pvc.metadata.name
            )));
        }
        Err(e) => {
            tracing::warn!(
                "Error applying LimitRange admission for PVC {}/{}: {}",
                namespace,
                pvc.metadata.name,
                e
            );
            // Continue on storage hiccup rather than blocking PVC creation.
        }
    }

    pvc.metadata.ensure_uid();
    pvc.metadata.ensure_creation_timestamp();

    // SetDefaults_PersistentVolumeClaimSpec: volumeMode defaults to Filesystem.
    crate::handlers::defaults::apply_pvc_spec_defaults(&mut pvc.spec);

    let key = build_key(
        "persistentvolumeclaims",
        Some(&namespace),
        &pvc.metadata.name,
    );

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolumeClaim {}/{} validated successfully (not created)",
            namespace, pvc.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(pvc)));
    }

    let created = state.storage.create(&key, &pvc).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_pvc(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<PersistentVolumeClaim>> {
    debug!("Getting PersistentVolumeClaim: {}/{}", namespace, name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("persistentvolumeclaims", Some(&namespace), &name);
    let pvc = state.storage.get(&key).await?;

    Ok(Json(pvc))
}

pub async fn list_pvcs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
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
        return crate::handlers::watch::watch_namespaced::<PersistentVolumeClaim>(
            state,
            auth_ctx,
            namespace,
            "persistentvolumeclaims",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing PersistentVolumeClaims in namespace: {}", namespace);

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("persistentvolumeclaims", Some(&namespace));
    let mut pvcs: Vec<PersistentVolumeClaim> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pvcs, &params)?;

    let mut list = List::new("PersistentVolumeClaimList", "v1", pvcs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all persistentvolumeclaims across all namespaces
pub async fn list_all_pvcs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
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
        return crate::handlers::watch::watch_cluster_scoped::<PersistentVolumeClaim>(
            state,
            auth_ctx,
            "persistentvolumeclaims",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all persistentvolumeclaims");

    // Check authorization (cluster-wide list)
    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "persistentvolumeclaims").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("persistentvolumeclaims", None);
    let mut pvcs = state.storage.list::<PersistentVolumeClaim>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut pvcs, &params)?;

    let mut list = List::new("PersistentVolumeClaimList", "v1", pvcs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

pub async fn update_pvc(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut pvc): DumpingJson<PersistentVolumeClaim>,
) -> Result<Json<PersistentVolumeClaim>> {
    info!("Updating PersistentVolumeClaim: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    pvc.metadata.name = name.clone();
    pvc.metadata.namespace = Some(namespace.clone());

    // SetDefaults_PersistentVolumeClaimSpec runs on update too; default
    // volumeMode before the immutability check so an update that omits it
    // (→ Filesystem) is not falsely rejected against a defaulted old value.
    crate::handlers::defaults::apply_pvc_spec_defaults(&mut pvc.spec);

    let key = build_key("persistentvolumeclaims", Some(&namespace), &name);

    // Enforce update immutability (upstream ValidatePersistentVolumeClaimUpdate):
    // volumeMode immutable + storage request may not shrink.
    if let Ok(mut existing) = state
        .storage
        .get::<rusternetes_common::resources::PersistentVolumeClaim>(&key)
        .await
    {
        // Default the stored object the same way as the incoming one before the
        // immutability comparison. The new PVC is defaulted above, so a stored
        // object that predates volumeMode defaulting would otherwise look like a
        // forbidden volumeMode change (None → "Filesystem"). Defaulting is
        // idempotent, matching upstream's defaulted-new-vs-defaulted-old compare.
        crate::handlers::defaults::apply_pvc_spec_defaults(&mut existing.spec);
        let errs = rusternetes_common::validation::pvc::validate_persistent_volume_claim_update(
            &pvc, &existing,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolumeClaim {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(pvc));
    }

    let updated = state.storage.update(&key, &pvc).await?;

    Ok(Json(updated))
}

pub async fn delete_pvc(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PersistentVolumeClaim>> {
    info!("Deleting PersistentVolumeClaim: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "persistentvolumeclaims")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("persistentvolumeclaims", Some(&namespace), &name);

    // Get the resource to check if it exists
    let pvc: PersistentVolumeClaim = state.storage.get(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=pvc).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "PersistentVolumeClaim",
        "persistentvolumeclaims",
        Some(&namespace),
        &name,
        &pvc,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: PersistentVolumeClaim {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(pvc));
    }

    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers(&*state.storage, &key, &pvc)
            .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: PersistentVolumeClaim = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(pvc))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(
    patch_pvc,
    PersistentVolumeClaim,
    "persistentvolumeclaims",
    ""
);

pub async fn deletecollection_persistentvolumeclaims(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection persistentvolumeclaims in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "persistentvolumeclaims")
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
        info!("Dry-run: PersistentVolumeClaim collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all persistentvolumeclaims in the namespace
    let prefix = build_prefix("persistentvolumeclaims", Some(&namespace));
    let mut items = state.storage.list::<PersistentVolumeClaim>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key(
            "persistentvolumeclaims",
            Some(&namespace),
            &item.metadata.name,
        );

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "PersistentVolumeClaim",
            "persistentvolumeclaims",
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
        "DeleteCollection completed: {} persistentvolumeclaims deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
