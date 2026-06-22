use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::ReplicationController,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Port of upstream `SetDefaults_ReplicationController` (pkg/apis/core/v1) plus
/// the declarative `spec.replicas` default:
/// - `spec.replicas` defaults to 1;
/// - when the pod template has labels, an unset `spec.selector` and an unset
///   top-level `metadata.labels` both default to those template labels.
fn apply_replicationcontroller_defaults(rc: &mut ReplicationController) {
    if rc.spec.replicas.is_none() {
        rc.spec.replicas = Some(1);
    }
    let template_labels = rc
        .spec
        .template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone());
    if let Some(labels) = template_labels {
        if !labels.is_empty() {
            if rc.spec.selector.is_none() {
                rc.spec.selector = Some(labels.clone());
            }
            if rc.metadata.labels.as_ref().is_none_or(|l| l.is_empty()) {
                rc.metadata.labels = Some(labels);
            }
        }
    }
}

pub async fn create_replicationcontroller(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<(StatusCode, HeaderMap, Json<ReplicationController>)> {
    // Decode the body manually so any deserialization failure is surfaced as a
    // proper `metav1.Status` (HTTP 400 / reason=BadRequest) rather than the bare
    // plain-text rejection axum's `Json`/`DumpingJson` extractor produces.
    // client-go translates a missing Status body into the opaque "the server
    // rejected our request due to an error in our request (post
    // replicationcontrollers)" message, which previously masked every malformed
    // POST here. Mirrors the deployment handler.
    let mut rc: ReplicationController = decode_rc_body(&body)?;

    info!(
        "Creating replicationcontroller: {} in namespace: {}",
        rc.metadata.name, namespace
    );

    // Strict field validation: reject/warn on unknown fields when requested.
    let warnings = crate::handlers::validation::validate_strict_fields(&params, &body, &rc)?;
    let response_headers = build_warning_headers(&warnings);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &rc.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace is set from the URL path
    rc.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    rc.metadata.ensure_uid();
    rc.metadata.ensure_creation_timestamp();

    // Apply RC defaulting (replicas, selector + labels from the template).
    apply_replicationcontroller_defaults(&mut rc);

    // Apply K8s defaults to pod template
    crate::handlers::defaults::apply_pod_template_defaults(&mut rc.spec.template);

    // Field validation (mirrors upstream ValidateReplicationController). Runs
    // after defaulting so the selector is populated from the template labels.
    {
        let errs =
            rusternetes_common::validation::replicationcontroller::validate_replication_controller(
                &rc,
            );
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    let key = build_key(
        "replicationcontrollers",
        Some(&namespace),
        &rc.metadata.name,
    );
    let created = state.storage.create(&key, &rc).await?;

    Ok((StatusCode::CREATED, response_headers, Json(created)))
}

/// Decode a ReplicationController request body into the typed struct, mapping
/// any deserialization failure to `Error::BadRequest` so the client receives a
/// proper `metav1.Status` instead of a bare plain-text 400. Preserves the
/// `RUSTERNETES_DUMP_PAYLOADS` conformance-debugging dump that the `DumpingJson`
/// extractor would otherwise have emitted.
fn decode_rc_body(body: &[u8]) -> Result<ReplicationController> {
    match serde_json::from_slice::<ReplicationController>(body) {
        Ok(rc) => Ok(rc),
        Err(e) => {
            let msg = e.to_string();
            // Retry duplicate-field failures via Value so they decode (strict
            // duplicate-key detection happens in validate_strict_fields).
            if msg.contains("duplicate field") {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) {
                    if let Ok(rc) = serde_json::from_value::<ReplicationController>(value) {
                        return Ok(rc);
                    }
                }
            }
            if rusternetes_common::dump::dumps_enabled() {
                let redacted = rusternetes_common::dump::redact_secret_like(body);
                tracing::error!(
                    rejection = %msg,
                    payload = %String::from_utf8_lossy(&redacted),
                    "ReplicationController JSON body decode failed"
                );
            }
            Err(rusternetes_common::Error::BadRequest(format!(
                "failed to decode: {}",
                msg
            )))
        }
    }
}

/// Build `Warning` response headers from strict field-validation warnings.
fn build_warning_headers(warnings: &[String]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for warning in warnings {
        let value = crate::handlers::validation::format_warning_header(warning);
        if let Ok(hv) = axum::http::HeaderValue::from_str(&value) {
            headers.append(axum::http::header::WARNING, hv);
        }
    }
    headers
}

pub async fn get_replicationcontroller(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ReplicationController>> {
    info!(
        "Getting replicationcontroller: {} in namespace: {}",
        name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("replicationcontrollers", Some(&namespace), &name);
    let rc = state.storage.get(&key).await?;

    Ok(Json(rc))
}

pub async fn update_replicationcontroller(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<ReplicationController>> {
    // Decode manually so a malformed PUT yields a proper `metav1.Status`
    // instead of a bare plain-text rejection (see create handler).
    let mut rc: ReplicationController = decode_rc_body(&body)?;

    info!(
        "Updating replicationcontroller: {} in namespace: {}",
        name, namespace
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    rc.metadata.name = name.clone();
    rc.metadata.namespace = Some(namespace.clone());

    let key = build_key("replicationcontrollers", Some(&namespace), &name);

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &rc).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &rc).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_replicationcontroller(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Json<ReplicationController>> {
    info!(
        "Deleting replicationcontroller: {} in namespace: {}",
        name, namespace
    );

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("replicationcontrollers", Some(&namespace), &name);

    // Get the resource to check if it exists
    let rc: ReplicationController = state.storage.get(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=rc).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "ReplicationController",
        "replicationcontrollers",
        Some(&namespace),
        &name,
        &rc,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: ReplicationController {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(rc));
    }

    // Extract propagation policy from query params or request body (DeleteOptions)
    let body_propagation: Option<String> = if !body.is_empty() {
        serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| {
                v.get("propagationPolicy")
                    .and_then(|p| p.as_str())
                    .map(|s| s.to_string())
            })
    } else {
        None
    };
    let propagation_policy = params
        .get("propagationPolicy")
        .map(|s| s.as_str())
        .or(body_propagation.as_deref());
    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers_and_propagation(
            &*state.storage,
            &key,
            &rc,
            propagation_policy,
        )
        .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ReplicationController = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(rc))
    }
}

pub async fn list_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_namespaced::<ReplicationController>(
            state,
            auth_ctx,
            namespace,
            "replicationcontrollers",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing replicationcontrollers in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "replicationcontrollers")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicationcontrollers", Some(&namespace));
    let mut rcs = state.storage.list::<ReplicationController>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rcs, &params)?;

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            rcs,
            Some(resource_version.clone()),
            "ReplicationController",
        );
        return Ok(axum::Json(table).into_response());
    }

    let mut list = List::new("ReplicationControllerList", "v1", rcs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all replicationcontrollers across all namespaces
pub async fn list_all_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped::<ReplicationController>(
            state,
            auth_ctx,
            "replicationcontrollers",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all replicationcontrollers");

    // Check authorization (cluster-wide list)
    let attrs =
        RequestAttributes::new(auth_ctx.user, "list", "replicationcontrollers").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("replicationcontrollers", None);
    let mut rcs = state.storage.list::<ReplicationController>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut rcs, &params)?;

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => "1".to_string(),
    };

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            rcs,
            Some(resource_version.clone()),
            "ReplicationController",
        );
        return Ok(axum::Json(table).into_response());
    }

    let mut list = List::new("ReplicationControllerList", "v1", rcs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Use the macro to create a PATCH handler
crate::patch_handler_namespaced!(
    patch_replicationcontroller,
    ReplicationController,
    "replicationcontrollers",
    ""
);

pub async fn deletecollection_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection replicationcontrollers in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "replicationcontrollers")
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
        info!("Dry-run: ReplicationController collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all replicationcontrollers in the namespace
    let prefix = build_prefix("replicationcontrollers", Some(&namespace));
    let mut items = state.storage.list::<ReplicationController>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key(
            "replicationcontrollers",
            Some(&namespace),
            &item.metadata.name,
        );

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "ReplicationController",
            "replicationcontrollers",
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
        "DeleteCollection completed: {} replicationcontrollers deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod default_tests {
    use super::*;

    fn rc(json: serde_json::Value) -> ReplicationController {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn defaults_replicas_selector_and_labels_from_template() {
        let mut r = rc(serde_json::json!({
            "metadata": {"name": "r"},
            "spec": {
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        }));
        apply_replicationcontroller_defaults(&mut r);
        assert_eq!(r.spec.replicas, Some(1));
        assert_eq!(
            r.spec
                .selector
                .as_ref()
                .unwrap()
                .get("app")
                .map(String::as_str),
            Some("web")
        );
        assert_eq!(
            r.metadata
                .labels
                .as_ref()
                .unwrap()
                .get("app")
                .map(String::as_str),
            Some("web")
        );
    }

    #[test]
    fn explicit_values_preserved() {
        let mut r = rc(serde_json::json!({
            "metadata": {"name": "r", "labels": {"team": "x"}},
            "spec": {
                "replicas": 3,
                "selector": {"app": "explicit"},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        }));
        apply_replicationcontroller_defaults(&mut r);
        assert_eq!(r.spec.replicas, Some(3));
        assert_eq!(
            r.spec
                .selector
                .as_ref()
                .unwrap()
                .get("app")
                .map(String::as_str),
            Some("explicit")
        );
        // top-level labels were already set -> not overwritten by template labels
        assert_eq!(
            r.metadata
                .labels
                .as_ref()
                .unwrap()
                .get("team")
                .map(String::as_str),
            Some("x")
        );
        assert!(!r.metadata.labels.as_ref().unwrap().contains_key("app"));
    }
}
