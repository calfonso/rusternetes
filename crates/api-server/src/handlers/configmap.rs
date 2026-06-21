use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    admission::{GroupVersionKind, Operation},
    authz::{Decision, RequestAttributes},
    resources::ConfigMap,
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
    DumpingJson(mut configmap): DumpingJson<ConfigMap>,
) -> Result<(StatusCode, Json<ConfigMap>)> {
    info!(
        "Creating configmap: {} in namespace: {}",
        configmap.metadata.name, namespace
    );

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &configmap.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;

    // Validate resource name
    crate::handlers::validation::validate_resource_name(&configmap.metadata.name)?;

    // Validate ConfigMap data/binaryData keys (upstream ValidateConfigMap).
    let errs = rusternetes_common::validation::configmap::validate_config_map(&configmap);
    if !errs.is_empty() {
        return Err(rusternetes_common::Error::Invalid(errs));
    }

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace is set from the URL path
    configmap.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    configmap.metadata.ensure_uid();
    configmap.metadata.ensure_creation_timestamp();

    // Run ValidatingAdmissionPolicy checks
    let gvk = GroupVersionKind {
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
    };
    let cm_value = serde_json::to_value(&configmap).ok();
    state
        .webhook_manager
        .run_validating_admission_policies_ext(
            &Operation::Create,
            &gvk,
            cm_value.as_ref(),
            None,
            Some("configmaps"),
            Some(&namespace),
        )
        .await?;

    // Run admission webhooks (mutating + validating)
    {
        let gvr = rusternetes_common::admission::GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "configmaps".to_string(),
        };
        let user = &user_for_webhook;
        let user_info = rusternetes_common::admission::UserInfo {
            username: user.username.clone(),
            uid: user.uid.clone(),
            groups: user.groups.clone(),
        };
        let cm_val = serde_json::to_value(&configmap).ok();
        // Run mutating webhooks
        let (_response, mutated_obj) = state
            .webhook_manager
            .run_mutating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Create,
                &gvk,
                &gvr,
                Some(&namespace),
                &configmap.metadata.name,
                cm_val.clone(),
                None,
                &user_info,
                is_dry_run,
            )
            .await?;
        // Check if the mutating webhook DENIED the request.
        // K8s mutating webhooks CAN deny — the denial must be enforced.
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = &_response {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
        if let Some(mutated) = mutated_obj {
            if let Ok(m) = serde_json::from_value::<ConfigMap>(mutated) {
                configmap = m;
            }
        }
        // Run validating webhooks
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Create,
                &gvk,
                &gvr,
                Some(&namespace),
                &configmap.metadata.name,
                serde_json::to_value(&configmap).ok(),
                None,
                &user_info,
                is_dry_run,
            )
            .await?
        {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    let key = build_key("configmaps", Some(&namespace), &configmap.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ConfigMap {}/{} validated successfully (not created)",
            namespace, configmap.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(configmap)));
    }

    let created = state.storage.create(&key, &configmap).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<ConfigMap>> {
    debug!("Getting configmap: {} in namespace: {}", name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("configmaps", Some(&namespace), &name);
    let configmap = state.storage.get(&key).await?;

    Ok(Json(configmap))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut configmap): DumpingJson<ConfigMap>,
) -> Result<Json<ConfigMap>> {
    info!("Updating configmap: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    configmap.metadata.name = name.clone();
    configmap.metadata.namespace = Some(namespace.clone());

    // Run ValidatingAdmissionPolicy checks for UPDATE
    let gvk = GroupVersionKind {
        group: "".to_string(),
        version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
    };
    let cm_value = serde_json::to_value(&configmap).ok();
    state
        .webhook_manager
        .run_validating_admission_policies_ext(
            &Operation::Update,
            &gvk,
            cm_value.as_ref(),
            None,
            Some("configmaps"),
            Some(&namespace),
        )
        .await?;

    // Run admission webhooks (mutating + validating) for UPDATE
    {
        let gvr = rusternetes_common::admission::GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "configmaps".to_string(),
        };
        let user = &user_for_webhook;
        let user_info = rusternetes_common::admission::UserInfo {
            username: user.username.clone(),
            uid: user.uid.clone(),
            groups: user.groups.clone(),
        };
        let cm_val = serde_json::to_value(&configmap).ok();
        // Run mutating webhooks
        let (_response, mutated_obj) = state
            .webhook_manager
            .run_mutating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Update,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                cm_val.clone(),
                None,
                &user_info,
                is_dry_run,
            )
            .await?;
        // Check if the mutating webhook DENIED the request.
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = &_response {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
        if let Some(mutated) = mutated_obj {
            if let Ok(m) = serde_json::from_value::<ConfigMap>(mutated) {
                configmap = m;
            }
        }
        // Run validating webhooks
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &rusternetes_common::admission::Operation::Update,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                serde_json::to_value(&configmap).ok(),
                None,
                &user_info,
            )
            .await?
        {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    let key = build_key("configmaps", Some(&namespace), &name);

    // Check if existing configmap is immutable — K8s only prevents changes to
    // data, binaryData, and immutable fields. Metadata changes (labels, annotations)
    // are still allowed.
    if let Ok(existing) = state.storage.get::<ConfigMap>(&key).await {
        if existing.immutable == Some(true) {
            let data_changed = existing.data != configmap.data;
            let binary_data_changed = existing.binary_data != configmap.binary_data;
            let immutable_changed =
                configmap.immutable != Some(true) && configmap.immutable != existing.immutable;
            if data_changed || binary_data_changed || immutable_changed {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "ConfigMap \"{}/{}\" is immutable",
                    namespace, name
                )));
            }
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: ConfigMap {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(configmap));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &configmap).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => {
            state.storage.create(&key, &configmap).await?
        }
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_configmap(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Json<ConfigMap>> {
    info!("Deleting configmap: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("configmaps", Some(&namespace), &name);

    // Get the resource to check if it exists
    let configmap: ConfigMap = state.storage.get(&key).await?;

    // Enforce deleteOptions.preconditions.{resourceVersion,uid} before mutating
    // anything. Upstream: pkg/registry/generic/registry/store.go::Delete calls
    // preconditions.Check() before invoking storage.Delete; a mismatch returns
    // 409 Conflict with reason `Conflict`.
    crate::handlers::lifecycle::check_delete_preconditions(&body, &configmap.metadata, &name)?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=configmap).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "ConfigMap",
        "configmaps",
        Some(&namespace),
        &name,
        &configmap,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: ConfigMap {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(configmap));
    }

    // Handle deletion with finalizers
    let has_finalizers = crate::handlers::finalizers::handle_delete_with_finalizers(
        &*state.storage,
        &key,
        &configmap,
    )
    .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: ConfigMap = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(configmap))
    }
}

pub async fn list(
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
        info!(
            "Configmap watch request for namespace {}: rv={:?}, sendInitialEvents={:?}",
            namespace,
            params.get("resourceVersion"),
            params.get("sendInitialEvents"),
        );
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
        return crate::handlers::watch::watch_namespaced::<ConfigMap>(
            state,
            auth_ctx,
            namespace,
            "configmaps",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing configmaps in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("configmaps", Some(&namespace));
    let mut configmaps: Vec<ConfigMap> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configmaps, &params)?;

    // Deterministic sort by name so `?continue` chains are stable
    // regardless of underlying storage iteration order.
    configmaps.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));

    paginate_configmaps_response(&state, configmaps, &params, "ConfigMapList").await
}

/// List all configmaps across all namespaces
pub async fn list_all_configmaps(
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
        return crate::handlers::watch::watch_cluster_scoped::<ConfigMap>(
            state,
            auth_ctx,
            "configmaps",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all configmaps");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "configmaps").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("configmaps", None);
    let mut configmaps = state.storage.list::<ConfigMap>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut configmaps, &params)?;

    // Cluster-scoped LIST sorts by (namespace, name) so paginated
    // chains across namespaces are deterministic.
    configmaps.sort_by(|a, b| {
        a.metadata
            .namespace
            .cmp(&b.metadata.namespace)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });

    paginate_configmaps_response(&state, configmaps, &params, "ConfigMapList").await
}

/// Apply `?limit` / `?continue` semantics over an already-sorted slice
/// of ConfigMaps and wrap the response as a `ConfigMapList` with
/// `metadata.continue`, `metadata.remainingItemCount`, and
/// `metadata.resourceVersion`. Mirrors the pattern used by the Pod
/// handler.
async fn paginate_configmaps_response(
    state: &Arc<ApiServerState>,
    configmaps: Vec<ConfigMap>,
    params: &HashMap<String, String>,
    list_kind: &str,
) -> Result<axum::response::Response> {
    let limit = params.get("limit").and_then(|l| l.parse::<i64>().ok());
    let continue_token = params.get("continue").cloned();

    let pagination_params = rusternetes_common::PaginationParams {
        limit,
        continue_token,
    };

    let resource_version = match state.storage.current_revision().await {
        Ok(rev) => rev.to_string(),
        Err(_) => crate::handlers::list_resource_version(&configmaps),
    };

    let paginated =
        match rusternetes_common::paginate(configmaps, pagination_params, &resource_version) {
            Ok(p) => p,
            Err(e) => {
                if e.message.contains("410 Gone") {
                    let mut status =
                        rusternetes_common::Status::failure(&e.message, "Expired", 410);
                    if let Some(token) = e.fresh_continue_token {
                        status.metadata = Some(rusternetes_common::ListMeta {
                            resource_version: Some(resource_version),
                            continue_token: Some(token),
                            remaining_item_count: None,
                        });
                    }
                    return Ok((axum::http::StatusCode::GONE, axum::Json(status)).into_response());
                }
                // Malformed continue token / encoding error -> 400 BadRequest
                // Status object (kind=Status), matching upstream apiserver.
                return Err(rusternetes_common::Error::BadRequest(e.message));
            }
        };

    let mut list = List::new(list_kind, "v1", paginated.items);
    list.metadata.continue_token = paginated.continue_token;
    list.metadata.remaining_item_count = paginated.remaining_item_count;
    list.metadata.resource_version = Some(paginated.resource_version);
    Ok(axum::Json(list).into_response())
}

// Generic PATCH handler used for all non-SSA patch types (strategic merge,
// JSON merge, JSON patch). The wrapper above intercepts SSA before
// delegating here so this macro still drives all the legacy patch paths.
crate::patch_handler_namespaced!(patch_legacy, ConfigMap, "configmaps", "");

/// ConfigMap PATCH dispatcher.
///
/// Branches on `Content-Type`:
///
/// - `application/apply-patch+yaml` / `application/apply-patch+json` →
///   structural-merge SSA via [`crate::ssa::apply_configmap`].
/// - everything else → the legacy [`patch_legacy`] handler (strategic
///   merge, JSON merge, JSON patch).
///
/// This is the SCAFFOLD: ConfigMap is the only resource wired to the new
/// SSA module today. Other resources still go through the legacy
/// top-level-key SSA in `rusternetes_common::server_side_apply` via the
/// generic patch macro.
pub async fn patch(
    state: axum::extract::State<Arc<ApiServerState>>,
    auth_ctx: axum::Extension<AuthContext>,
    path: axum::extract::Path<(String, String)>,
    query: axum::extract::Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::response::Response> {
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if content_type.contains("apply-patch") {
        return apply_configmap_ssa(state, auth_ctx, path, query, &content_type, body).await;
    }

    // Delegate to the legacy patch handler.
    let response = patch_legacy(state, auth_ctx, path, query, headers, body).await?;
    Ok(response.into_response())
}

/// Server-Side Apply branch for ConfigMap PATCH.
///
/// Translates the HTTP request into an [`crate::ssa::ApplyOptions`] +
/// desired-state value, runs [`crate::ssa::apply_configmap`], and maps the
/// outcome to a Response:
///
/// - new object → HTTP 201 Created
/// - merged object → HTTP 200 OK
/// - conflicts without `?force=true` → HTTP 409 Conflict (Status body)
async fn apply_configmap_ssa(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    content_type: &str,
    body: axum::body::Bytes,
) -> Result<axum::response::Response> {
    info!(
        "SSA apply configmap {}/{} (Content-Type: {})",
        namespace, name, content_type
    );

    // Save user info for webhooks before RBAC check consumes it.
    let webhook_user = auth_ctx.user.clone();

    // RBAC: SSA uses the `patch` verb.
    let attrs = RequestAttributes::new(auth_ctx.user, "patch", "configmaps")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // ?fieldManager= is mandatory for SSA; upstream returns 400 when
    // missing.
    let field_manager = params.get("fieldManager").cloned().ok_or_else(|| {
        rusternetes_common::Error::BadRequest(
            "fieldManager query parameter is required for apply-patch requests".to_string(),
        )
    })?;
    let force = params
        .get("force")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false);
    let opts = crate::ssa::ApplyOptions::new(field_manager).with_force(force);

    // Decode the body — apply-patch+yaml or apply-patch+json.
    let mut desired = crate::ssa::decode_apply_body(content_type, &body)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;
    // Path-coerce name/namespace so the body cannot rename the object.
    if let Some(meta) = desired
        .as_object_mut()
        .and_then(|o| o.get_mut("metadata"))
        .and_then(|m| m.as_object_mut())
    {
        meta.insert("name".to_string(), serde_json::Value::String(name.clone()));
        meta.insert(
            "namespace".to_string(),
            serde_json::Value::String(namespace.clone()),
        );
    }

    let key = build_key("configmaps", Some(&namespace), &name);

    // Load current object (if any) for the merge.
    let current: Option<ConfigMap> = match state.storage.get::<ConfigMap>(&key).await {
        Ok(cm) => Some(cm),
        Err(rusternetes_common::Error::NotFound(_)) => None,
        Err(e) => return Err(e),
    };

    let outcome = crate::ssa::apply_configmap(current.as_ref(), &desired, &opts)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Immutability check — mirrors the `update` handler. An immutable
    // ConfigMap rejects any change to `data`, `binaryData`, or the
    // `immutable` flag itself. SSA must not bypass this guard, otherwise
    // a client could mutate an immutable ConfigMap via apply-patch.
    if let (Some(existing), crate::ssa::ApplyOutcome::Applied { ref object, .. }) =
        (current.as_ref(), &outcome)
    {
        if existing.immutable == Some(true) {
            let data_changed = existing.data != object.data;
            let binary_data_changed = existing.binary_data != object.binary_data;
            let immutable_changed =
                object.immutable != Some(true) && object.immutable != existing.immutable;
            if data_changed || binary_data_changed || immutable_changed {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "ConfigMap \"{}/{}\" is immutable",
                    namespace, name
                )));
            }
        }
    }

    match outcome {
        crate::ssa::ApplyOutcome::Applied {
            object: boxed,
            created,
        } => {
            // The SSA module returns the object boxed to keep ApplyOutcome
            // small (clippy::large_enum_variant); unbox once here so the rest
            // of the handler can treat it as a plain ConfigMap value.
            let mut object: ConfigMap = *boxed;
            // Ensure path-derived metadata is set even when the merge
            // started from a brand-new body.
            object.metadata.name = name.clone();
            object.metadata.namespace = Some(namespace.clone());
            if created {
                object.metadata.ensure_uid();
                object.metadata.ensure_creation_timestamp();
            }
            let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

            // Run mutating + validating admission webhooks on the
            // SSA-produced object, mirroring the non-SSA PATCH path. The
            // operation is Create for new objects, Update for merges, so
            // policies attached to either bucket fire correctly.
            let op = if created {
                Operation::Create
            } else {
                Operation::Update
            };
            let gvk = GroupVersionKind {
                group: "".to_string(),
                version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
            };
            let cm_val = serde_json::to_value(&object).ok();
            state
                .webhook_manager
                .run_validating_admission_policies_ext(
                    &op,
                    &gvk,
                    cm_val.as_ref(),
                    None,
                    Some("configmaps"),
                    Some(&namespace),
                )
                .await?;
            {
                let gvr = rusternetes_common::admission::GroupVersionResource {
                    group: "".to_string(),
                    version: "v1".to_string(),
                    resource: "configmaps".to_string(),
                };
                let user_info = rusternetes_common::admission::UserInfo {
                    username: webhook_user.username.clone(),
                    uid: webhook_user.uid.clone(),
                    groups: webhook_user.groups.clone(),
                };
                let (response, mutated_obj) = state
                    .webhook_manager
                    .run_mutating_webhooks_with_dryrun(
                        &op,
                        &gvk,
                        &gvr,
                        Some(&namespace),
                        &name,
                        cm_val.clone(),
                        None,
                        &user_info,
                        is_dry_run,
                    )
                    .await?;
                if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = &response {
                    return Err(rusternetes_common::Error::Forbidden(format!(
                        "admission webhook denied the request: {}",
                        reason
                    )));
                }
                if let Some(mutated) = mutated_obj {
                    if let Ok(m) = serde_json::from_value::<ConfigMap>(mutated) {
                        object = m;
                    }
                }
                if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
                    .webhook_manager
                    .run_validating_webhooks_with_dryrun(
                        &op,
                        &gvk,
                        &gvr,
                        Some(&namespace),
                        &name,
                        serde_json::to_value(&object).ok(),
                        None,
                        &user_info,
                        is_dry_run,
                    )
                    .await?
                {
                    return Err(rusternetes_common::Error::Forbidden(format!(
                        "admission webhook denied the request: {}",
                        reason
                    )));
                }
            }

            let saved: ConfigMap = if is_dry_run {
                object
            } else if created {
                state.storage.create::<ConfigMap>(&key, &object).await?
            } else {
                state.storage.update::<ConfigMap>(&key, &object).await?
            };
            let status = if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            Ok((status, axum::Json(saved)).into_response())
        }
        crate::ssa::ApplyOutcome::Conflicts(conflicts) => {
            // Mirror upstream: 409 Conflict with reason=Conflict.
            let detail = conflicts
                .iter()
                .map(|c| {
                    format!(
                        ".{} is managed by {}",
                        c.path.replace('/', "."),
                        c.current_manager
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            Err(rusternetes_common::Error::Conflict(format!(
                "Apply failed with {} conflict{}: {}",
                conflicts.len(),
                if conflicts.len() == 1 { "" } else { "s" },
                detail
            )))
        }
    }
}

pub async fn deletecollection_configmaps(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection configmaps in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "configmaps")
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
        info!("Dry-run: ConfigMap collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all configmaps in the namespace
    let prefix = build_prefix("configmaps", Some(&namespace));
    let mut items = state.storage.list::<ConfigMap>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("configmaps", Some(&namespace), &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "ConfigMap",
            "configmaps",
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
        "DeleteCollection completed: {} configmaps deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
