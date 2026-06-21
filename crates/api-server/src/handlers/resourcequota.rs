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
    resources::{ResourceQuota, ResourceQuotaStatus},
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
    DumpingJson(mut quota): DumpingJson<ResourceQuota>,
) -> Result<(StatusCode, Json<ResourceQuota>)> {
    info!(
        "Creating ResourceQuota: {} in namespace: {}",
        quota.metadata.name, namespace
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "resourcequotas")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &quota.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Field validation (mirrors upstream ValidateResourceQuota).
    {
        let errs = rusternetes_common::validation::resourcequota::validate_resource_quota(&quota);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    quota.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    quota.metadata.ensure_uid();
    quota.metadata.ensure_creation_timestamp();

    // Set kind/apiVersion
    quota.type_meta.kind = "ResourceQuota".to_string();
    quota.type_meta.api_version = "v1".to_string();

    // Initialize status with hard limits and zero usage
    if quota.status.is_none() {
        let used = quota
            .spec
            .hard
            .as_ref()
            .map(|hard| hard.keys().map(|k| (k.clone(), "0".to_string())).collect());
        quota.status = Some(ResourceQuotaStatus {
            hard: quota.spec.hard.clone(),
            used,
        });
    }

    let key = build_key("resourcequotas", Some(&namespace), &quota.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ResourceQuota {}/{} validated successfully (not created)",
            namespace, quota.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(quota)));
    }

    let created = state.storage.create(&key, &quota).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ResourceQuota>> {
    info!(
        "Getting ResourceQuota: {} in namespace: {}",
        name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "resourcequotas")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("resourcequotas", Some(&namespace), &name);
    let quota = state.storage.get(&key).await?;

    Ok(Json(quota))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut quota): DumpingJson<ResourceQuota>,
) -> Result<Json<ResourceQuota>> {
    info!(
        "Updating ResourceQuota: {} in namespace: {}",
        name, namespace
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "resourcequotas")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    quota.metadata.name = name.clone();
    quota.metadata.namespace = Some(namespace.clone());

    let key = build_key("resourcequotas", Some(&namespace), &name);

    // Upstream resourcequotaStrategy.PrepareForUpdate copies the stored object's
    // status onto the incoming object so status (used/hard) only mutates via the
    // /status subresource. Without this, a spec-only PUT carries an empty status
    // and wipes the controller-computed usage until the next reconcile.
    let old_quota = match state.storage.get::<ResourceQuota>(&key).await {
        Ok(old) => Some(old),
        Err(rusternetes_common::Error::NotFound(_)) => None,
        Err(e) => return Err(e),
    };
    if let Some(old) = &old_quota {
        // Field validation on update (upstream ValidateResourceQuotaUpdate):
        // re-validate the spec and reject changes to the immutable spec.scopes.
        let errs = rusternetes_common::validation::resourcequota::validate_resource_quota_update(
            &quota, old,
        );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
        // PrepareForUpdate: status only mutates via the /status subresource.
        quota.status = old.status.clone();
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ResourceQuota {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(quota));
    }

    let result = match state.storage.update(&key, &quota).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &quota).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<ResourceQuota>> {
    info!(
        "Deleting ResourceQuota: {} in namespace: {}",
        name, namespace
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "resourcequotas")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("resourcequotas", Some(&namespace), &name);

    // Get the resource quota for finalizer handling
    let quota: ResourceQuota = state.storage.get(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=quota).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "ResourceQuota",
        "resourcequotas",
        Some(&namespace),
        &name,
        &quota,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: ResourceQuota {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(quota));
    }

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &quota)
            .await?;

    if deleted_immediately {
        Ok(Json(quota))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ResourceQuota = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<ResourceQuota>(
            state,
            auth_ctx,
            namespace,
            "resourcequotas",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing ResourceQuotas in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "resourcequotas")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("resourcequotas", Some(&namespace));
    let mut quotas = state.storage.list::<ResourceQuota>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut quotas, &params)?;

    let mut list = List::new("ResourceQuotaList", "v1", quotas);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

pub async fn list_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ResourceQuota>(
            state,
            auth_ctx,
            "resourcequotas",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all ResourceQuotas");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "resourcequotas").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("resourcequotas", None);
    let mut quotas = state.storage.list::<ResourceQuota>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut quotas, &params)?;

    let mut list = List::new("ResourceQuotaList", "v1", quotas);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, ResourceQuota, "resourcequotas", "");

pub async fn deletecollection_resourcequotas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection resourcequotas in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "resourcequotas")
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
        info!("Dry-run: ResourceQuota collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all resourcequotas in the namespace
    let prefix = build_prefix("resourcequotas", Some(&namespace));
    let mut items = state.storage.list::<ResourceQuota>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("resourcequotas", Some(&namespace), &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "ResourceQuota",
            "resourcequotas",
            Some(&namespace),
            &item.metadata.name,
            &item,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers. DeleteCollection is idempotent: if an
        // item vanished between the list and this delete (e.g. the quota
        // controller updated status concurrently and the backend delete races to
        // 0 rows), treat NotFound as already-deleted instead of failing the whole
        // collection delete with a 404.
        match crate::handlers::finalizers::handle_delete_with_finalizers(
            &state.storage,
            &key,
            &item,
        )
        .await
        {
            Ok(false) => deleted_count += 1, // deleted immediately (no finalizers)
            Ok(true) => {}                   // finalizers pending — not counted
            Err(rusternetes_common::Error::NotFound(_)) => deleted_count += 1,
            Err(e) => return Err(e),
        }
    }

    info!(
        "DeleteCollection completed: {} resourcequotas deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
