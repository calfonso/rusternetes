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
    resources::LimitRange,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Port of upstream `SetDefaults_LimitRangeItem` (pkg/apis/core/v1/defaults.go).
/// For `Container`-type limits: default `Default` from `Max`, then
/// `DefaultRequest` from `Default`, then `DefaultRequest` from `Min`. This is
/// what lets a LimitRange that only specifies `max`/`min` still inject limit
/// and request defaults into admitted pods.
fn apply_limit_range_defaults(lr: &mut LimitRange) {
    for item in &mut lr.spec.limits {
        if item.item_type != "Container" {
            continue;
        }
        // Default <- Max (for keys not already in Default).
        let default = item.default.get_or_insert_with(HashMap::new);
        if let Some(max) = &item.max {
            for (k, v) in max {
                default.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        // DefaultRequest <- Default, then <- Min.
        let default_snapshot = default.clone();
        let default_request = item.default_request.get_or_insert_with(HashMap::new);
        for (k, v) in &default_snapshot {
            default_request
                .entry(k.clone())
                .or_insert_with(|| v.clone());
        }
        if let Some(min) = &item.min {
            for (k, v) in min {
                default_request
                    .entry(k.clone())
                    .or_insert_with(|| v.clone());
            }
        }
    }
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut limit_range): DumpingJson<LimitRange>,
) -> Result<(StatusCode, Json<LimitRange>)> {
    info!(
        "Creating LimitRange: {} in namespace: {}",
        limit_range.metadata.name, namespace
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "limitranges")
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
        &limit_range.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Defaulting (SetDefaults_LimitRangeItem) runs before validation upstream.
    apply_limit_range_defaults(&mut limit_range);

    // Field validation (mirrors upstream ValidateLimitRange).
    {
        let errs = rusternetes_common::validation::limitrange::validate_limit_range(&limit_range);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    limit_range.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    limit_range.metadata.ensure_uid();
    limit_range.metadata.ensure_creation_timestamp();

    let key = build_key("limitranges", Some(&namespace), &limit_range.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: LimitRange {}/{} validated successfully (not created)",
            namespace, limit_range.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(limit_range)));
    }

    let created = state.storage.create(&key, &limit_range).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<LimitRange>> {
    debug!("Getting LimitRange: {} in namespace: {}", name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "limitranges")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("limitranges", Some(&namespace), &name);
    let limit_range = state.storage.get(&key).await?;

    Ok(Json(limit_range))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut limit_range): DumpingJson<LimitRange>,
) -> Result<Json<LimitRange>> {
    info!("Updating LimitRange: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "limitranges")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    limit_range.metadata.name = name.clone();
    limit_range.metadata.namespace = Some(namespace.clone());

    let key = build_key("limitranges", Some(&namespace), &name);

    // Defaulting (SetDefaults_LimitRangeItem) runs before validation upstream.
    apply_limit_range_defaults(&mut limit_range);

    // Field validation on update (upstream LimitRange update strategy re-runs
    // ValidateLimitRange on the new object). The create path validated but the
    // update path previously persisted PUTs unchecked.
    {
        let errs = rusternetes_common::validation::limitrange::validate_limit_range(&limit_range);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: LimitRange {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(limit_range));
    }

    let result = match state.storage.update(&key, &limit_range).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &limit_range).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<LimitRange>> {
    info!("Deleting LimitRange: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "limitranges")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("limitranges", Some(&namespace), &name);

    // Get the limit range for finalizer handling
    let limit_range: LimitRange = state.storage.get(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=limit_range).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "LimitRange",
        "limitranges",
        Some(&namespace),
        &name,
        &limit_range,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: LimitRange {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(limit_range));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &limit_range,
    )
    .await?;

    if deleted_immediately {
        Ok(Json(limit_range))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: LimitRange = state.storage.get(&key).await?;
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
        return crate::handlers::watch::watch_namespaced::<LimitRange>(
            state,
            auth_ctx,
            namespace,
            "limitranges",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing LimitRanges in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "limitranges")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("limitranges", Some(&namespace));
    let mut limit_ranges = state.storage.list::<LimitRange>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut limit_ranges, &params)?;

    let mut list = List::new("LimitRangeList", "v1", limit_ranges);
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
        return crate::handlers::watch::watch_cluster_scoped::<LimitRange>(
            state,
            auth_ctx,
            "limitranges",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all LimitRanges");

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "limitranges").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("limitranges", None);
    let mut limit_ranges = state.storage.list::<LimitRange>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut limit_ranges, &params)?;

    let mut list = List::new("LimitRangeList", "v1", limit_ranges);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(patch, LimitRange, "limitranges", "");

pub async fn deletecollection_limitranges(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection limitranges in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "limitranges")
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
        info!("Dry-run: LimitRange collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all limitranges in the namespace
    let prefix = build_prefix("limitranges", Some(&namespace));
    let mut items = state.storage.list::<LimitRange>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("limitranges", Some(&namespace), &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "LimitRange",
            "limitranges",
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
        "DeleteCollection completed: {} limitranges deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod default_tests {
    use super::*;

    fn defaulted(json: serde_json::Value) -> LimitRange {
        let mut lr: LimitRange = serde_json::from_value(json).unwrap();
        apply_limit_range_defaults(&mut lr);
        lr
    }

    #[test]
    fn container_default_from_max_then_request_chain() {
        let lr = defaulted(serde_json::json!({
            "metadata": {"name": "lr"},
            "spec": {"limits": [
                {"type": "Container", "max": {"cpu": "2"}, "min": {"memory": "64Mi"}}
            ]}
        }));
        let item = &lr.spec.limits[0];
        // Default <- Max
        assert_eq!(
            item.default
                .as_ref()
                .unwrap()
                .get("cpu")
                .map(String::as_str),
            Some("2")
        );
        // DefaultRequest <- Default (cpu) and <- Min (memory)
        let dr = item.default_request.as_ref().unwrap();
        assert_eq!(dr.get("cpu").map(String::as_str), Some("2"));
        assert_eq!(dr.get("memory").map(String::as_str), Some("64Mi"));
    }

    #[test]
    fn explicit_default_is_preserved() {
        let lr = defaulted(serde_json::json!({
            "metadata": {"name": "lr"},
            "spec": {"limits": [
                {"type": "Container", "max": {"cpu": "2"}, "default": {"cpu": "1"}}
            ]}
        }));
        // explicit default cpu=1 not overwritten by max
        assert_eq!(
            lr.spec.limits[0]
                .default
                .as_ref()
                .unwrap()
                .get("cpu")
                .map(String::as_str),
            Some("1")
        );
        // defaultRequest derived from the explicit default
        assert_eq!(
            lr.spec.limits[0]
                .default_request
                .as_ref()
                .unwrap()
                .get("cpu")
                .map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn non_container_type_untouched() {
        let lr = defaulted(serde_json::json!({
            "metadata": {"name": "lr"},
            "spec": {"limits": [
                {"type": "Pod", "max": {"cpu": "2"}}
            ]}
        }));
        assert!(
            lr.spec.limits[0].default.is_none(),
            "Pod-type limits get no Default defaulting"
        );
    }
}
