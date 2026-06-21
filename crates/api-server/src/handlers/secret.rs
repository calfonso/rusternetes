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
    resources::{PodSpec, Secret},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// K8s default file-mode bits for Secret/ConfigMap/DownwardAPI/Projected
/// volumes when `defaultMode` is omitted by the client. Decimal 420 == 0o644.
///
/// K8s ref: `pkg/apis/core/v1/defaults.go`:
/// - `SetDefaults_SecretVolumeSource`
/// - `SetDefaults_ConfigMapVolumeSource`
/// - `SetDefaults_DownwardAPIVolumeSource`
/// - `SetDefaults_ProjectedVolumeSource`
#[allow(dead_code)]
pub const DEFAULT_VOLUME_FILE_MODE: i32 = 0o644;

/// Apply K8s defaulting for volume `defaultMode` bits to every Secret,
/// ConfigMap, DownwardAPI, and Projected volume source on a [`PodSpec`].
///
/// Per-item `mode` (on `KeyToPath` / `DownwardAPIVolumeFile`) is intentionally
/// left untouched — the kubelet falls back to `defaultMode` when a per-item
/// mode is unset, which is the K8s contract.
///
/// Explicit caller-supplied values (including `0`) are never overwritten:
/// only `None` is defaulted. This matches the upstream Go defaulting which
/// only fills in nil pointers.
#[allow(dead_code)]
pub fn apply_volume_mode_defaults(spec: &mut PodSpec) {
    let Some(volumes) = spec.volumes.as_mut() else {
        return;
    };
    for vol in volumes.iter_mut() {
        if let Some(sv) = vol.secret.as_mut() {
            if sv.default_mode.is_none() {
                sv.default_mode = Some(DEFAULT_VOLUME_FILE_MODE);
            }
        }
        if let Some(cm) = vol.config_map.as_mut() {
            if cm.default_mode.is_none() {
                cm.default_mode = Some(DEFAULT_VOLUME_FILE_MODE);
            }
        }
        if let Some(dapi) = vol.downward_api.as_mut() {
            if dapi.default_mode.is_none() {
                dapi.default_mode = Some(DEFAULT_VOLUME_FILE_MODE);
            }
        }
        if let Some(proj) = vol.projected.as_mut() {
            if proj.default_mode.is_none() {
                proj.default_mode = Some(DEFAULT_VOLUME_FILE_MODE);
            }
        }
    }
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut secret): DumpingJson<Secret>,
) -> Result<(StatusCode, Json<Secret>)> {
    // Server-side name generation (metadata.generateName) is applied centrally
    // by generate_name_middleware before this handler runs (#1052), so by here
    // an unnamed-but-generateName Secret already has a synthesised name.
    info!(
        "Creating secret: {} in namespace: {}",
        secret.metadata.name, namespace
    );

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &secret.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsSubdomain,
    )?;
    // Validate resource name
    crate::handlers::validation::validate_resource_name(&secret.metadata.name)?;

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "secrets")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure namespace is set from the URL path
    secret.metadata.namespace = Some(namespace.clone());

    // Validate secret data keys (must be valid path segments)
    if let Some(ref data) = secret.data {
        for key in data.keys() {
            if key.is_empty()
                || key == "."
                || key == ".."
                || key.contains('/')
                || key.contains('\\')
            {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "Invalid key name \"{}\": a valid config key must consist of alphanumeric characters, '-', '_' or '.'",
                    key
                )));
            }
        }
    }
    if let Some(ref string_data) = secret.string_data {
        for key in string_data.keys() {
            if key.is_empty()
                || key == "."
                || key == ".."
                || key.contains('/')
                || key.contains('\\')
            {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "Invalid key name \"{}\": a valid config key must consist of alphanumeric characters, '-', '_' or '.'",
                    key
                )));
            }
        }
    }

    // Enforce the MaxSecretSize (1 MiB) total-size cap (upstream ValidateSecret).
    // stringData is merged into data before persistence, so it counts toward the
    // same budget; sum both. Upstream attaches the error to the `data` path.
    {
        use rusternetes_common::validation::configmap::MAX_SECRET_SIZE;
        let mut total_size: usize = 0;
        if let Some(ref data) = secret.data {
            total_size += data.values().map(|v| v.len()).sum::<usize>();
        }
        if let Some(ref string_data) = secret.string_data {
            total_size += string_data.values().map(|v| v.len()).sum::<usize>();
        }
        if total_size > MAX_SECRET_SIZE {
            use rusternetes_common::validation::field::{Error as FieldError, Path};
            return Err(rusternetes_common::Error::Invalid(vec![
                FieldError::too_long(&Path::new("data"), MAX_SECRET_SIZE),
            ]));
        }
    }

    // Enrich metadata with system fields
    secret.metadata.ensure_uid();
    secret.metadata.ensure_creation_timestamp();

    // Normalize: convert stringData to base64-encoded data
    secret.normalize();

    // Type-specific required-key validation (upstream ValidateSecret's
    // `switch secret.Type`). Runs after normalize() so keys supplied via
    // stringData are present in `data`.
    {
        let errs = rusternetes_common::validation::secret::validate_secret_type(&secret);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    let key = build_key("secrets", Some(&namespace), &secret.metadata.name);

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Secret {}/{} validated successfully (not created)",
            namespace, secret.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(secret)));
    }

    let created = state.storage.create(&key, &secret).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Secret>> {
    debug!("Getting secret: {} in namespace: {}", name, namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "secrets")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("secrets", Some(&namespace), &name);
    let secret = state.storage.get(&key).await?;

    Ok(Json(secret))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut secret): DumpingJson<Secret>,
) -> Result<Json<Secret>> {
    info!("Updating secret: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "secrets")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    secret.metadata.name = name.clone();
    secret.metadata.namespace = Some(namespace.clone());

    // Normalize: convert stringData to base64-encoded data
    secret.normalize();

    let key = build_key("secrets", Some(&namespace), &name);

    // Check if existing secret is immutable — only reject data/stringData changes
    if let Ok(existing) = state.storage.get::<Secret>(&key).await {
        // Enforce `.type` immutability post-create. Upstream:
        // `pkg/registry/core/secret/strategy.go::ValidateUpdate` calls
        // `validation.ValidateSecretUpdate`, which appends
        // `ValidateImmutableField(newSecret.Type, oldSecret.Type, field.NewPath("type"))`
        // unconditionally — independent of `Secret.immutable`. The wire error is
        // `field.Invalid(field.NewPath("type"), newSecret.Type, "field is immutable")`.
        //
        // Default the missing-type case to "Opaque" on both sides to mirror
        // `pkg/apis/core/v1/defaults.go::SetDefaults_Secret`, so an UPDATE body
        // that omits `.type` (the field is omitempty) compares equal to an
        // existing secret whose type was server-defaulted to "Opaque" on
        // create.
        // Mirror upstream `SetDefaults_Secret`: treat both missing AND empty
        // string as `Opaque`, so a client that sends `"type": ""` (or omits
        // the field) on UPDATE matches an existing secret whose type was
        // server-defaulted or left unset.
        let old_type = match existing.secret_type.as_deref() {
            None | Some("") => "Opaque",
            Some(t) => t,
        };
        let new_type = match secret.secret_type.as_deref() {
            None | Some("") => "Opaque",
            Some(t) => t,
        };
        if old_type != new_type {
            return Err(rusternetes_common::Error::InvalidResource(format!(
                "type: Invalid value: \"{new_type}\": field is immutable"
            )));
        }

        if existing.immutable == Some(true) {
            // Compare data and stringData — reject if changed
            let data_changed = existing.data != secret.data;
            let string_data_changed = existing.string_data != secret.string_data;
            // Also reject changing immutable from true to false
            let immutable_changed = secret.immutable != Some(true) && secret.immutable.is_some();
            if data_changed || string_data_changed || immutable_changed {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "Secret \"{}\" is immutable",
                    name
                )));
            }
        }
    }

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        info!(
            "Dry-run: Secret {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(secret));
    }

    // Try to update first, if not found then create (upsert behavior)
    let result = match state.storage.update(&key, &secret).await {
        Ok(updated) => updated,
        Err(rusternetes_common::Error::NotFound(_)) => state.storage.create(&key, &secret).await?,
        Err(e) => return Err(e),
    };

    Ok(Json(result))
}

pub async fn delete_secret(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Json<Secret>> {
    info!("Deleting secret: {} in namespace: {}", name, namespace);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "secrets")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("secrets", Some(&namespace), &name);

    // Get the resource to check if it exists
    let secret: Secret = state.storage.get(&key).await?;

    // Enforce deleteOptions.preconditions.{resourceVersion,uid} before mutating
    // anything. Upstream: pkg/registry/generic/registry/store.go::Delete calls
    // preconditions.Check() before invoking storage.Delete; a mismatch returns
    // 409 Conflict with reason `Conflict`.
    crate::handlers::lifecycle::check_delete_preconditions(&body, &secret.metadata, &name)?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=secret).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "Secret",
        "secrets",
        Some(&namespace),
        &name,
        &secret,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Secret {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(secret));
    }

    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers(&*state.storage, &key, &secret)
            .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Secret = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(secret))
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
        return crate::handlers::watch::watch_namespaced::<Secret>(
            state,
            auth_ctx,
            namespace,
            "secrets",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing secrets in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "secrets")
        .with_api_group("")
        .with_namespace(&namespace);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("secrets", Some(&namespace));
    let mut secrets: Vec<Secret> = state.storage.list(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut secrets, &params)?;

    let mut list = List::new("SecretList", "v1", secrets);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all secrets across all namespaces
pub async fn list_all_secrets(
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
        return crate::handlers::watch::watch_cluster_scoped::<Secret>(
            state,
            auth_ctx,
            "secrets",
            "",
            watch_params,
        )
        .await;
    }

    debug!("Listing all secrets");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "secrets").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("secrets", None);
    let mut secrets = state.storage.list::<Secret>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut secrets, &params)?;

    let mut list = List::new("SecretList", "v1", secrets);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

// Generic PATCH handler used for all non-SSA patch types (strategic merge,
// JSON merge, JSON patch). The dispatcher below intercepts SSA before
// delegating here so this macro still drives all the legacy patch paths.
crate::patch_handler_namespaced!(patch_legacy, Secret, "secrets", "");

/// Secret PATCH dispatcher.
///
/// Branches on `Content-Type`:
///
/// - `application/apply-patch+yaml` / `application/apply-patch+json` →
///   schema-driven SSA via [`crate::ssa::apply_secret`].
/// - everything else → the legacy [`patch_legacy`] handler (strategic
///   merge, JSON merge, JSON patch).
///
/// ConfigMap and Secret are the two resources wired to the new SSA module
/// today. Other resources (Pod / Deployment / Service / …) still go through
/// the legacy top-level-key SSA in `rusternetes_common::server_side_apply`
/// via the generic patch macro — see `handlers::configmap::patch` for the
/// pattern this mirrors.
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
        return apply_secret_ssa(state, auth_ctx, path, query, &content_type, body).await;
    }

    // Delegate to the legacy patch handler.
    let response = patch_legacy(state, auth_ctx, path, query, headers, body).await?;
    Ok(response.into_response())
}

/// Server-Side Apply branch for Secret PATCH.
///
/// Mirrors `handlers::configmap::apply_configmap_ssa` line-for-line — the
/// only Secret-specific bits are:
///
/// 1. `Secret.type` immutability fence post-create. Upstream
///    `pkg/registry/core/secret/strategy.go::ValidateUpdate` calls
///    `apivalidation.ValidateImmutableField(newSecret.Type, oldSecret.Type, …)`
///    unconditionally — SSA must honour that too, otherwise an applier
///    could flip `type: Opaque` to `type: kubernetes.io/basic-auth` after
///    create.
/// 2. `Secret::normalize()` is called after the merge so the `stringData`
///    entries the applier supplied are folded into `data` (base64-encoded)
///    before storage. SSA ownership is tracked against the raw
///    `/stringData/<key>` paths so the applier can later release them by
///    re-applying without that key.
/// 3. The `immutable: true` fence is wider than ConfigMap's — Secret
///    blocks `data`, `stringData`, and `type` changes once locked.
async fn apply_secret_ssa(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    content_type: &str,
    body: axum::body::Bytes,
) -> Result<axum::response::Response> {
    info!(
        "SSA apply secret {}/{} (Content-Type: {})",
        namespace, name, content_type
    );

    // Save user info for webhooks before RBAC check consumes it.
    let webhook_user = auth_ctx.user.clone();

    // RBAC: SSA uses the `patch` verb.
    let attrs = RequestAttributes::new(auth_ctx.user, "patch", "secrets")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&name);
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // ?fieldManager= is mandatory for SSA; upstream returns 400 when missing.
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

    let key = build_key("secrets", Some(&namespace), &name);

    // Load current object (if any) for the merge.
    let current: Option<Secret> = match state.storage.get::<Secret>(&key).await {
        Ok(s) => Some(s),
        Err(rusternetes_common::Error::NotFound(_)) => None,
        Err(e) => return Err(e),
    };

    let outcome = crate::ssa::apply_secret(current.as_ref(), &desired, &opts)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Immutability + type-fence checks — both run only when there's an
    // existing object. They cannot bypass via SSA, otherwise an applier
    // could mutate immutable Secrets or flip the `type` field.
    if let (Some(existing), crate::ssa::ApplyOutcome::Applied { ref object, .. }) =
        (current.as_ref(), &outcome)
    {
        // `Secret.type` is immutable post-create per upstream strategy.
        // Compare the merged object's type against the existing object's
        // type — if the applier flipped it, reject. We allow the applier
        // to omit `type` entirely (in which case the merger preserved the
        // current value).
        if existing.secret_type != object.secret_type {
            return Err(rusternetes_common::Error::InvalidResource(format!(
                "Secret \"{}/{}\" field is immutable: type",
                namespace, name
            )));
        }

        if existing.immutable == Some(true) {
            // For an immutable Secret, reject any change to `data`,
            // `stringData`, or the `immutable` flag itself. We compare
            // both `data` and `stringData` because the merger preserves
            // them as separate fields until `normalize()` runs below.
            let data_changed = existing.data != object.data;
            let string_data_changed = existing.string_data != object.string_data;
            let immutable_changed =
                object.immutable != Some(true) && object.immutable != existing.immutable;
            if data_changed || string_data_changed || immutable_changed {
                return Err(rusternetes_common::Error::InvalidResource(format!(
                    "Secret \"{}/{}\" is immutable",
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
            let mut object: Secret = *boxed;
            // Ensure path-derived metadata is set even when the merge
            // started from a brand-new body.
            object.metadata.name = name.clone();
            object.metadata.namespace = Some(namespace.clone());
            if created {
                object.metadata.ensure_uid();
                object.metadata.ensure_creation_timestamp();
            }
            // Fold stringData into base64-encoded data before storage —
            // matches `Secret::PrepareForCreate` / `PrepareForUpdate`
            // semantics in upstream Go where `stringData` is a write-only
            // convenience that never round-trips to clients.
            object.normalize();

            let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

            // Run mutating + validating admission webhooks on the
            // SSA-produced object, mirroring the non-SSA PATCH path.
            let op = if created {
                Operation::Create
            } else {
                Operation::Update
            };
            let gvk = GroupVersionKind {
                group: "".to_string(),
                version: "v1".to_string(),
                kind: "Secret".to_string(),
            };
            let s_val = serde_json::to_value(&object).ok();
            state
                .webhook_manager
                .run_validating_admission_policies_ext(
                    &op,
                    &gvk,
                    s_val.as_ref(),
                    None,
                    Some("secrets"),
                    Some(&namespace),
                )
                .await?;
            {
                let gvr = rusternetes_common::admission::GroupVersionResource {
                    group: "".to_string(),
                    version: "v1".to_string(),
                    resource: "secrets".to_string(),
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
                        s_val.clone(),
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
                    if let Ok(m) = serde_json::from_value::<Secret>(mutated) {
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

            let saved: Secret = if is_dry_run {
                object
            } else if created {
                state.storage.create::<Secret>(&key, &object).await?
            } else {
                state.storage.update::<Secret>(&key, &object).await?
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

pub async fn deletecollection_secrets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection secrets in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "secrets")
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
        info!("Dry-run: Secret collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all secrets in the namespace
    let prefix = build_prefix("secrets", Some(&namespace));
    let mut items = state.storage.list::<Secret>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("secrets", Some(&namespace), &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "Secret",
            "secrets",
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
        "DeleteCollection completed: {} secrets deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}
