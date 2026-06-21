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
    resources::PriorityClass,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Find an existing PriorityClass marked `globalDefault: true`, if any.
///
/// Mirrors the upstream priority admission plugin's `getDefaultPriorityClass`
/// (plugin/pkg/admission/priority/admission.go): when more than one default
/// exists (race), the one with the lowest `value` wins. Returns the name of
/// the chosen default.
async fn find_default_priority_class(state: &Arc<ApiServerState>) -> Result<Option<String>> {
    let prefix = build_prefix("priorityclasses", None);
    let existing = state.storage.list::<PriorityClass>(&prefix).await?;
    let mut default: Option<&PriorityClass> = None;
    for pc in &existing {
        if pc.global_default == Some(true) {
            match default {
                Some(cur) if cur.value <= pc.value => {}
                _ => default = Some(pc),
            }
        }
    }
    Ok(default.map(|pc| pc.metadata.name.clone()))
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut priority_class): DumpingJson<PriorityClass>,
) -> Result<(StatusCode, Json<PriorityClass>)> {
    info!("Creating PriorityClass: {}", priority_class.metadata.name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "priorityclasses")
        .with_api_group("scheduling.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &priority_class.metadata,
        None,
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Validate the PriorityClass (upstream scheduling ValidatePriorityClass):
    // system-prefix reservation, user-priority cap, preemptionPolicy enum.
    let errs =
        rusternetes_common::validation::priorityclass::validate_priority_class(&priority_class);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Enrich metadata with system fields
    priority_class.metadata.ensure_uid();
    priority_class.metadata.ensure_creation_timestamp();

    // At most one PriorityClass may be globalDefault (upstream priority
    // admission plugin). On create, any existing default conflicts.
    if priority_class.global_default == Some(true) {
        if let Some(existing_default) = find_default_priority_class(&state).await? {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "PriorityClass {existing_default} is already marked as default. Only one default can exist"
            )));
        }
    }

    let key = build_key("priorityclasses", None, &priority_class.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PriorityClass {} validated successfully (not created)",
            priority_class.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(priority_class)));
    }

    let created = state.storage.create(&key, &priority_class).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<PriorityClass>> {
    debug!("Getting PriorityClass: {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "priorityclasses")
        .with_api_group("scheduling.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("priorityclasses", None, &name);
    let priority_class = state.storage.get(&key).await?;

    Ok(Json(priority_class))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut priority_class): DumpingJson<PriorityClass>,
) -> Result<Json<PriorityClass>> {
    info!("Updating PriorityClass: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "priorityclasses")
        .with_api_group("scheduling.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    priority_class.metadata.name = name.clone();

    let key = build_key("priorityclasses", None, &name);

    // Validate immutable fields — value cannot be changed after creation
    if let Ok(existing) = state.storage.get::<PriorityClass>(&key).await {
        if existing.value != priority_class.value {
            return Err(rusternetes_common::Error::InvalidResource(format!(
                "PriorityClass.value: Invalid value: \"{}\": field is immutable",
                priority_class.value
            )));
        }
    }

    // At most one PriorityClass may be globalDefault (upstream priority
    // admission plugin). On update, a different existing default conflicts;
    // re-marking the same class is allowed.
    if priority_class.global_default == Some(true) {
        if let Some(existing_default) = find_default_priority_class(&state).await? {
            if existing_default != name {
                return Err(rusternetes_common::Error::Forbidden(format!(
                    "PriorityClass {existing_default} is already marked as default. Only one default can exist"
                )));
            }
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: PriorityClass {} validated successfully (not updated)",
            name
        );
        return Ok(Json(priority_class));
    }

    let result = match state.storage.update(&key, &priority_class).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &priority_class).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<PriorityClass>> {
    info!("Deleting PriorityClass: {}", name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "priorityclasses")
        .with_api_group("scheduling.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("priorityclasses", None, &name);

    // Get the priority class for finalizer handling
    let priority_class: PriorityClass = state.storage.get(&key).await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: PriorityClass {} validated successfully (not deleted)",
            name
        );
        return Ok(Json(priority_class));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &priority_class,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(priority_class))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: PriorityClass = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<PriorityClass>(
            state,
            auth_ctx,
            "priorityclasses",
            "scheduling.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing PriorityClasses");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "priorityclasses")
        .with_api_group("scheduling.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("priorityclasses", None);
    let mut priority_classes = state.storage.list::<PriorityClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut priority_classes, &params)?;

    let mut list = List::new(
        "PriorityClassList",
        "scheduling.k8s.io/v1",
        priority_classes,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
pub async fn patch(
    state: axum::extract::State<std::sync::Arc<crate::state::ApiServerState>>,
    auth_ctx: axum::Extension<crate::middleware::AuthContext>,
    path: axum::extract::Path<String>,
    query: axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> rusternetes_common::Result<axum::Json<PriorityClass>> {
    let name = path.0.clone();
    // Get existing value before patch to check immutability
    let key = build_key("priorityclasses", None, &name);
    let existing_pc = state.storage.get::<PriorityClass>(&key).await.ok();
    let existing_value = existing_pc.as_ref().map(|pc| pc.value);

    // Apply the patch to compute the result without writing yet.
    // We need to check immutability BEFORE persisting.
    let result = crate::handlers::generic_patch::patch_cluster_resource::<PriorityClass>(
        state.clone(),
        auth_ctx,
        axum::extract::Path(name.clone()),
        query,
        headers,
        body,
        "priorityclasses",
        "scheduling.k8s.io",
    )
    .await?;

    // Validate immutable field wasn't changed — if it was, revert by
    // writing back the original value (using the new resource version)
    if let Some(old_value) = existing_value {
        if result.0.value != old_value {
            // Revert: restore original PriorityClass with the new resource version
            // so the update succeeds (the old resource version is stale)
            if let Some(mut original) = existing_pc {
                original.metadata.resource_version = result.0.metadata.resource_version.clone();
                let _ = state.storage.update(&key, &original).await;
            }
            return Err(rusternetes_common::Error::InvalidResource(format!(
                "PriorityClass.value: Invalid value: \"{}\": field is immutable",
                result.0.value
            )));
        }
    }

    Ok(result)
}

pub async fn deletecollection_priorityclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!("DeleteCollection priorityclasses with params: {:?}", params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "priorityclasses")
        .with_api_group("scheduling.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: PriorityClass collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all priorityclasses
    let prefix = build_prefix("priorityclasses", None);
    let mut items = state.storage.list::<PriorityClass>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("priorityclasses", None, &item.metadata.name);

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
        "DeleteCollection completed: {} priorityclasses deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
