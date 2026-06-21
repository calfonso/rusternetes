use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::CSIDriver,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// SetDefaults_CSIDriver (pkg/apis/storage/v1/defaults.go): fill in the optional
/// bool/enum fields. Runs on both create and update (upstream re-defaults on
/// update), so the update-path immutability checks compare like-for-like.
fn apply_csidriver_spec_defaults(driver: &mut rusternetes_common::resources::CSIDriver) {
    use rusternetes_common::resources::csi::{FSGroupPolicy, VolumeLifecycleMode};
    let s = &mut driver.spec;
    if s.attach_required.is_none() {
        s.attach_required = Some(true);
    }
    if s.pod_info_on_mount.is_none() {
        s.pod_info_on_mount = Some(false);
    }
    if s.storage_capacity.is_none() {
        s.storage_capacity = Some(false);
    }
    if s.fs_group_policy.is_none() {
        s.fs_group_policy = Some(FSGroupPolicy::ReadWriteOnceWithFSType);
    }
    if s.volume_lifecycle_modes
        .as_ref()
        .is_none_or(|v| v.is_empty())
    {
        s.volume_lifecycle_modes = Some(vec![VolumeLifecycleMode::Persistent]);
    }
    if s.requires_republish.is_none() {
        s.requires_republish = Some(false);
    }
    if s.se_linux_mount.is_none() {
        s.se_linux_mount = Some(false);
    }
}

pub async fn create_csidriver(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut driver): DumpingJson<CSIDriver>,
) -> Result<(StatusCode, Json<CSIDriver>)> {
    info!("Creating CSIDriver: {}", driver.metadata.name);

    // Check authorization (cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "csidrivers")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &driver.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // SetDefaults_CSIDriver (pkg/apis/storage/v1/defaults.go): fill in the
    // optional bool/enum fields before validation, matching upstream's
    // default-then-validate ordering.
    apply_csidriver_spec_defaults(&mut driver);

    // Validate the (defaulted) CSIDriver — upstream ValidateCSIDriver.
    let errs = rusternetes_common::validation::csidriver::validate_csi_driver(&driver);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    driver.metadata.ensure_uid();
    driver.metadata.ensure_creation_timestamp();

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CSIDriver validated successfully (not created)");
        return Ok((StatusCode::CREATED, Json(driver)));
    }

    let key = build_key("csidrivers", None, &driver.metadata.name);
    let created = state.storage.create(&key, &driver).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_csidriver(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<CSIDriver>> {
    debug!("Getting CSIDriver: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "csidrivers")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("csidrivers", None, &name);
    let driver = state.storage.get(&key).await?;

    Ok(Json(driver))
}

pub async fn list_csidrivers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    debug!("Listing all CSIDrivers");

    // Honor `?watch=true` on the collection endpoint (informer/Lens path).
    if crate::handlers::watch::is_watch_request(&params) {
        return crate::handlers::watch::watch_cluster_scoped::<CSIDriver>(
            state,
            auth_ctx,
            "csidrivers",
            "storage.k8s.io",
            crate::handlers::watch::watch_params_from_query(&params),
        )
        .await;
    }

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "csidrivers")
        .with_api_group("storage.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("csidrivers", None);
    let mut drivers = state.storage.list::<CSIDriver>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut drivers, &params)?;

    let mut list = List::new("CSIDriverList", "storage.k8s.io/v1", drivers);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(axum::response::IntoResponse::into_response(Json(list)))
}

pub async fn update_csidriver(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut driver): DumpingJson<CSIDriver>,
) -> Result<Json<CSIDriver>> {
    info!("Updating CSIDriver: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "csidrivers")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    driver.metadata.name = name.clone();

    // SetDefaults_CSIDriver runs on update too, so the immutable-field checks
    // compare defaulted-against-defaulted (e.g. volumeLifecycleModes defaults to
    // [Persistent]); without this an update omitting a defaulted field would
    // falsely trip the immutability check.
    apply_csidriver_spec_defaults(&mut driver);

    let key = build_key("csidrivers", None, &name);

    // Immutability + re-validation on update (upstream ValidateCSIDriverUpdate):
    // attachRequired and volumeLifecycleModes are immutable.
    if let Ok(old) = state
        .storage
        .get::<rusternetes_common::resources::CSIDriver>(&key)
        .await
    {
        let errs =
            rusternetes_common::validation::csidriver::validate_csi_driver_update(&driver, &old);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CSIDriver validated successfully (not updated)");
        return Ok(Json(driver));
    }

    let updated = state.storage.update(&key, &driver).await?;

    Ok(Json(updated))
}

pub async fn delete_csidriver(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<CSIDriver>> {
    info!("Deleting CSIDriver: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "csidrivers")
        .with_api_group("storage.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("csidrivers", None, &name);

    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Get the resource for finalizer handling
    let resource: CSIDriver = state.storage.get(&key).await?;

    if is_dry_run {
        info!("Dry-run: CSIDriver validated successfully (not deleted)");
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
        let updated: CSIDriver = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

// Use the macro to create a PATCH handler
crate::patch_handler_cluster!(patch_csidriver, CSIDriver, "csidrivers", "storage.k8s.io");

pub async fn deletecollection_csidrivers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection csidrivers with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "csidrivers")
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
        info!("Dry-run: CSIDriver collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all csidrivers
    let prefix = build_prefix("csidrivers", None);
    let mut items = state.storage.list::<CSIDriver>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("csidrivers", None, &item.metadata.name);

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
        "DeleteCollection completed: {} csidrivers deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::CSIDriverSpec;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};

    fn create_test_driver(name: &str) -> CSIDriver {
        CSIDriver {
            type_meta: TypeMeta {
                kind: "CSIDriver".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: CSIDriverSpec {
                attach_required: Some(true),
                pod_info_on_mount: Some(false),
                fs_group_policy: None,
                storage_capacity: Some(true),
                volume_lifecycle_modes: None,
                token_requests: None,
                requires_republish: Some(false),
                se_linux_mount: Some(false),
                node_allocatable_update_period_seconds: None,
                ..Default::default()
            },
        }
    }

    #[tokio::test]
    async fn test_csidriver_serialization() {
        let driver = create_test_driver("test-driver");
        let json = serde_json::to_string(&driver).unwrap();
        let deserialized: CSIDriver = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "test-driver");
    }
}
