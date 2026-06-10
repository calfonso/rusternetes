// CustomResourceDefinition API handlers

use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use rusternetes_common::{
    admission::{AdmissionResponse, GroupVersionKind, GroupVersionResource, Operation},
    authz::{Decision, RequestAttributes},
    resources::CustomResourceDefinition,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Create a new CustomResourceDefinition
pub async fn create_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<HashMap<String, String>>,
    _headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<serde_json::Value>)> {
    // Reject empty request bodies with a clear error message.
    if body.is_empty() {
        return Err(rusternetes_common::Error::InvalidResource(
            "request body must not be empty".to_string(),
        ));
    }

    // Detect binary (protobuf) bodies early — before attempting JSON parse.
    // CBOR (`application/cbor` / `application/apply-patch+cbor`) is decoded
    // upstream of this handler by `normalize_content_type_middleware`, which
    // transcodes the request body to JSON before it reaches us. So at this
    // point a non-JSON first byte means an unhandled binary format
    // (typically the `k8s\0` protobuf envelope) — we still reject those
    // because the CRD handler has no native binary decode path.
    if !body.is_empty()
        && !matches!(
            body[0],
            b'{' | b'[' | b'"' | b'0'..=b'9' | b't' | b'f' | b'n' | b' ' | b'\t' | b'\n' | b'\r'
        )
    {
        return Err(rusternetes_common::Error::UnsupportedMediaType(
            "the body is not valid JSON (protobuf content type is not supported for this resource)"
                .to_string(),
        ));
    }

    // Check for strict field validation mode
    let is_strict = params.get("fieldValidation").map(|v| v.as_str()) == Some("Strict");

    // Try to parse the body as JSON. If it fails, return appropriate error.
    let mut crd: CustomResourceDefinition =
        match serde_json::from_slice::<CustomResourceDefinition>(&body) {
            Ok(c) => c,
            Err(e) => {
                // If strict mode, check for duplicate fields before falling through
                if is_strict {
                    if let Ok(body_str) = std::str::from_utf8(&body) {
                        if let Some(dup_field) =
                            crate::handlers::validation::find_duplicate_json_key_public(body_str)
                        {
                            return Err(rusternetes_common::Error::InvalidResource(format!(
                                "strict decoding error: json: duplicate field \"{}\"",
                                dup_field
                            )));
                        }
                    }
                }

                // Try parsing as Value first and inject missing defaults
                if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(&body) {
                    if val.get("apiVersion").is_none() {
                        val["apiVersion"] =
                            serde_json::Value::String("apiextensions.k8s.io/v1".to_string());
                    }
                    if val.get("kind").is_none() {
                        val["kind"] =
                            serde_json::Value::String("CustomResourceDefinition".to_string());
                    }
                    if val.get("metadata").is_none() {
                        val["metadata"] = serde_json::json!({});
                    }
                    serde_json::from_value(val).map_err(|e2| {
                        rusternetes_common::Error::InvalidResource(format!(
                            "failed to decode CRD: {}",
                            e2
                        ))
                    })?
                } else {
                    return Err(rusternetes_common::Error::InvalidResource(format!(
                        "failed to decode CRD: {}",
                        e
                    )));
                }
            }
        };

    // Strict field validation: reject unknown or duplicate fields when requested
    crate::handlers::validation::validate_strict_fields(&params, &body, &crd)?;
    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::require_object_name(&crd.metadata)?;
    let crd_name = crd.metadata.name.clone();
    info!("Creating CustomResourceDefinition: {}", crd_name);

    // Build user info for admission webhooks (before auth_ctx.user is moved)
    let user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization (CRDs are cluster-scoped)
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "customresourcedefinitions")
        .with_api_group("apiextensions.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Validate CRD spec
    validate_crd(&crd)?;

    // Ensure metadata fields
    crd.metadata.ensure_uid();
    crd.metadata.ensure_creation_timestamp();

    // Initialize status with Established and NamesAccepted conditions
    if crd.status.is_none() {
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        crd.status = Some(
            rusternetes_common::resources::CustomResourceDefinitionStatus {
                conditions: Some(vec![
                    rusternetes_common::resources::CustomResourceDefinitionCondition {
                        type_: "Established".to_string(),
                        status: "True".to_string(),
                        last_transition_time: Some(now.clone()),
                        reason: Some("InitialNamesAccepted".to_string()),
                        message: Some("the initial names have been accepted".to_string()),
                    },
                    rusternetes_common::resources::CustomResourceDefinitionCondition {
                        type_: "NamesAccepted".to_string(),
                        status: "True".to_string(),
                        last_transition_time: Some(now),
                        reason: Some("NoConflicts".to_string()),
                        message: Some("no conflicts found".to_string()),
                    },
                ]),
                accepted_names: Some(crd.spec.names.clone()),
                stored_versions: Some(
                    crd.spec
                        .versions
                        .iter()
                        .filter(|v| v.storage)
                        .map(|v| v.name.clone())
                        .collect(),
                ),
            },
        );
    }

    // Run admission webhooks for CRD creation
    let gvk = GroupVersionKind {
        group: "apiextensions.k8s.io".to_string(),
        version: "v1".to_string(),
        kind: "CustomResourceDefinition".to_string(),
    };
    let gvr = GroupVersionResource {
        group: "apiextensions.k8s.io".to_string(),
        version: "v1".to_string(),
        resource: "customresourcedefinitions".to_string(),
    };

    let crd_value_for_webhook = serde_json::to_value(&crd)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    // Run mutating webhooks
    let (mutation_response, mutated_crd_value) = state
        .webhook_manager
        .run_mutating_webhooks(
            &Operation::Create,
            &gvk,
            &gvr,
            None,
            &crd_name,
            Some(crd_value_for_webhook),
            None,
            &user_info,
        )
        .await?;

    match mutation_response {
        AdmissionResponse::Deny(reason) => {
            warn!("Mutating webhooks denied CRD creation: {}", reason);
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
        AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_) => {
            if let Some(mutated_value) = mutated_crd_value {
                crd = serde_json::from_value(mutated_value)
                    .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
                info!("CRD mutated by webhooks: {}", crd_name);
            }
        }
    }

    // Run validating webhooks
    let final_crd_value = serde_json::to_value(&crd)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    let validation_response = state
        .webhook_manager
        .run_validating_webhooks(
            &Operation::Create,
            &gvk,
            &gvr,
            None,
            &crd_name,
            Some(final_crd_value),
            None,
            &user_info,
        )
        .await?;

    match validation_response {
        AdmissionResponse::Deny(reason) => {
            warn!("Validating webhooks denied CRD creation: {}", reason);
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
        AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_) => {
            info!("Validating webhooks passed for CRD {}", crd_name);
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CustomResourceDefinition validated successfully (not created)");
        let value = serde_json::to_value(&crd)
            .map_err(|e| rusternetes_common::Error::Internal(format!("serialize: {}", e)))?;
        return Ok((StatusCode::CREATED, Json(value)));
    }

    let key = build_key("customresourcedefinitions", None, &crd_name);

    // Store the CRD using the ORIGINAL JSON body to preserve all schema fields.
    // Going through the typed CustomResourceDefinition struct loses nested schemas
    // (like items in JSONSchemaPropsOrArray) due to serde untagged enum limitations.
    // K8s stores CRDs as raw JSON in etcd.
    let mut crd_value: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| rusternetes_common::Error::Internal(format!("re-parse: {}", e)))?;
    // Apply metadata enrichment from the typed struct
    if let Some(obj) = crd_value.as_object_mut() {
        if let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            meta.insert("uid".to_string(), serde_json::json!(crd.metadata.uid));
            if let Some(ts) = &crd.metadata.creation_timestamp {
                meta.insert("creationTimestamp".to_string(), serde_json::json!(ts));
            }
            meta.insert(
                "generation".to_string(),
                serde_json::json!(crd.metadata.generation),
            );
            meta.insert("name".to_string(), serde_json::json!(crd.metadata.name));
        }
    }
    // Merge the typed struct's status into the raw JSON value.
    // The original body from the client usually lacks status, but our typed
    // `crd` struct has the Established/NamesAccepted conditions set above.
    // K8s always returns status in the CRD response and stores it.
    if let Ok(status_json) = serde_json::to_value(&crd.status) {
        if let Some(obj) = crd_value.as_object_mut() {
            obj.insert("status".to_string(), status_json);
        }
    }

    let created: serde_json::Value = match state.storage.create(&key, &crd_value).await {
        Ok(v) => v,
        Err(rusternetes_common::Error::AlreadyExists(_)) => {
            return Err(rusternetes_common::Error::AlreadyExists(format!(
                "customresourcedefinitions.apiextensions.k8s.io \"{}\" already exists",
                crd_name
            )));
        }
        Err(e) => return Err(e),
    };
    info!("CRD created: {}", crd_name);

    // Generate a MODIFIED watch event by touching the CRD status.
    // K8s clients watch from the CREATE's resourceVersion for a MODIFIED event
    // containing the Established condition. Do this synchronously to ensure
    // the event exists before returning the CREATE response.
    // Retry up to 3 times in case of CAS conflict.
    for attempt in 0..3 {
        match state.storage.get::<serde_json::Value>(&key).await {
            Ok(mut val) => {
                if let Some(status) = val.get_mut("status").and_then(|s| s.as_object_mut()) {
                    status.insert("observedGeneration".to_string(), serde_json::json!(1));
                }
                match state.storage.update(&key, &val).await {
                    Ok(_) => {
                        tracing::info!(
                            "CRD {} status update succeeded (attempt {})",
                            crd_name,
                            attempt + 1
                        );
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "CRD {} status update failed (attempt {}): {}",
                            crd_name,
                            attempt + 1,
                            e
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "CRD {} status get failed (attempt {}): {}",
                    crd_name,
                    attempt + 1,
                    e
                );
            }
        }
    }

    Ok((StatusCode::CREATED, Json(created)))
}

/// Get a specific CustomResourceDefinition
pub async fn get_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
) -> Result<Json<CustomResourceDefinition>> {
    debug!("Getting CustomResourceDefinition: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "get", "customresourcedefinitions")
        .with_api_group("apiextensions.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("customresourcedefinitions", None, &name);
    let crd = state.storage.get(&key).await?;

    Ok(Json(crd))
}

/// List all CustomResourceDefinitions
pub async fn list_crds(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<axum::response::Response> {
    if crate::handlers::watch::is_watch_request(&params) {
        let watch_params = crate::handlers::watch::watch_params_from_query(&params);
        return crate::handlers::watch::watch_cluster_scoped_json(
            state,
            auth_ctx,
            "customresourcedefinitions",
            "apiextensions.k8s.io",
            watch_params,
        )
        .await;
    }

    debug!("Listing all CustomResourceDefinitions");

    let attrs = RequestAttributes::new(auth_ctx.user, "list", "customresourcedefinitions")
        .with_api_group("apiextensions.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("customresourcedefinitions", None);
    let mut crds = state
        .storage
        .list::<CustomResourceDefinition>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut crds, &params)?;

    let mut list = List::new(
        "CustomResourceDefinitionList",
        "apiextensions.k8s.io/v1",
        crds,
    );
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// Update a CustomResourceDefinition
pub async fn update_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    _headers: HeaderMap,
    body: Bytes,
) -> Result<Json<CustomResourceDefinition>> {
    // Reject empty or binary-protobuf request bodies
    if body.is_empty() {
        return Err(rusternetes_common::Error::InvalidResource(
            "request body must not be empty".to_string(),
        ));
    }
    if !matches!(
        body[0],
        b'{' | b'[' | b'"' | b'0'..=b'9' | b't' | b'f' | b'n' | b' ' | b'\t' | b'\n' | b'\r'
    ) {
        return Err(rusternetes_common::Error::UnsupportedMediaType(
            "request body is not valid JSON; protobuf content type is not supported".to_string(),
        ));
    }

    // Check for strict field validation mode
    let is_strict = params.get("fieldValidation").map(|v| v.as_str()) == Some("Strict");

    // Parse the body manually for better error handling
    let mut crd: CustomResourceDefinition = serde_json::from_slice(&body).map_err(|e| {
        let msg = e.to_string();
        if is_strict && msg.contains("duplicate field") {
            if let Some(field) = msg.split('`').nth(1) {
                return rusternetes_common::Error::InvalidResource(format!(
                    "strict decoding error: json: duplicate field \"{}\"",
                    field
                ));
            }
        }
        // Also check with our manual duplicate detector
        if is_strict {
            if let Ok(body_str) = std::str::from_utf8(&body) {
                if let Some(dup_field) =
                    crate::handlers::validation::find_duplicate_json_key_public(body_str)
                {
                    return rusternetes_common::Error::InvalidResource(format!(
                        "strict decoding error: json: duplicate field \"{}\"",
                        dup_field
                    ));
                }
            }
        }
        rusternetes_common::Error::InvalidResource(format!("failed to decode CRD: {}", msg))
    })?;

    // Strict field validation: reject unknown or duplicate fields when requested
    crate::handlers::validation::validate_strict_fields(&params, &body, &crd)?;

    info!("Updating CustomResourceDefinition: {}", name);

    // Build user info for admission webhooks (before auth_ctx.user is moved)
    let user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    let attrs = RequestAttributes::new(auth_ctx.user, "update", "customresourcedefinitions")
        .with_api_group("apiextensions.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Validate CRD spec
    validate_crd(&crd)?;

    crd.metadata.name = name.clone();

    // Get old object for webhook old_object field
    let key = build_key("customresourcedefinitions", None, &name);
    let old_crd_value: Option<serde_json::Value> =
        state.storage.get::<serde_json::Value>(&key).await.ok();

    // Run admission webhooks for CRD update
    let gvk = GroupVersionKind {
        group: "apiextensions.k8s.io".to_string(),
        version: "v1".to_string(),
        kind: "CustomResourceDefinition".to_string(),
    };
    let gvr = GroupVersionResource {
        group: "apiextensions.k8s.io".to_string(),
        version: "v1".to_string(),
        resource: "customresourcedefinitions".to_string(),
    };

    let crd_value_for_webhook = serde_json::to_value(&crd)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    // Run mutating webhooks
    let (mutation_response, mutated_crd_value) = state
        .webhook_manager
        .run_mutating_webhooks(
            &Operation::Update,
            &gvk,
            &gvr,
            None,
            &name,
            Some(crd_value_for_webhook),
            old_crd_value.clone(),
            &user_info,
        )
        .await?;

    match mutation_response {
        AdmissionResponse::Deny(reason) => {
            warn!("Mutating webhooks denied CRD update: {}", reason);
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
        AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_) => {
            if let Some(mutated_value) = mutated_crd_value {
                crd = serde_json::from_value(mutated_value)
                    .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
                info!("CRD mutated by webhooks: {}", name);
            }
        }
    }

    // Run validating webhooks
    let final_crd_value = serde_json::to_value(&crd)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    let validation_response = state
        .webhook_manager
        .run_validating_webhooks(
            &Operation::Update,
            &gvk,
            &gvr,
            None,
            &name,
            Some(final_crd_value),
            old_crd_value.clone(),
            &user_info,
        )
        .await?;

    match validation_response {
        AdmissionResponse::Deny(reason) => {
            warn!("Validating webhooks denied CRD update: {}", reason);
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
        AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_) => {
            info!("Validating webhooks passed for CRD {}", name);
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CustomResourceDefinition validated successfully (not updated)");
        return Ok(Json(crd));
    }

    // Store as raw JSON (not typed struct) to preserve nested schemas.
    // The typed CustomResourceDefinition loses fields like "enum" in nested
    // JSONSchemaProps due to serde untagged enum limitations with JSONSchemaPropsOrArray.
    // K8s ref: CRD storage preserves original schema bytes.
    let mut raw_value: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| rusternetes_common::Error::Internal(format!("re-parse for storage: {}", e)))?;

    enrich_updated_crd(&mut raw_value, &crd, old_crd_value.as_ref());
    let updated: serde_json::Value = state.storage.update(&key, &raw_value).await?;

    // Notify dynamic route manager about CRD update
    info!("CRD updated: {}", name);

    Ok(Json(serde_json::from_value(updated).unwrap_or(crd)))
}

/// Delete a CustomResourceDefinition
pub async fn delete_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<CustomResourceDefinition>> {
    info!("Deleting CustomResourceDefinition: {}", name);

    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "customresourcedefinitions")
        .with_api_group("apiextensions.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get CRD to validate it exists
    let key = build_key("customresourcedefinitions", None, &name);
    let crd: CustomResourceDefinition = state.storage.get(&key).await?;

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CustomResourceDefinition validated successfully (not deleted)");
        return Ok(Json(crd));
    }

    // K8s does NOT block CRD deletion when custom resources exist.
    // Instead, the CRD finalizer controller (customresourcecleanup.apiextensions.k8s.io)
    // handles cleanup of custom resources after deletionTimestamp is set.

    // Handle deletion with finalizers
    let deleted_immediately =
        !crate::handlers::finalizers::handle_delete_with_finalizers(&state.storage, &key, &crd)
            .await?;

    if deleted_immediately {
        info!("CRD deleted: {}", name);
        Ok(Json(crd))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: CustomResourceDefinition = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

/// Validate a CustomResourceDefinition
fn validate_crd(crd: &CustomResourceDefinition) -> Result<()> {
    // Validate that at least one version is defined
    if crd.spec.versions.is_empty() {
        return Err(rusternetes_common::Error::InvalidResource(
            "CRD must have at least one version".to_string(),
        ));
    }

    // Validate that exactly one version is marked as storage
    let storage_versions: Vec<_> = crd.spec.versions.iter().filter(|v| v.storage).collect();

    if storage_versions.is_empty() {
        return Err(rusternetes_common::Error::InvalidResource(
            "CRD must have exactly one storage version".to_string(),
        ));
    }

    if storage_versions.len() > 1 {
        return Err(rusternetes_common::Error::InvalidResource(
            "CRD can only have one storage version".to_string(),
        ));
    }

    // Validate that all served versions either have storage=true or are marked as served
    for version in &crd.spec.versions {
        if version.storage && !version.served {
            warn!(
                "Version {} is marked as storage but not served, this is unusual",
                version.name
            );
        }
    }

    // Validate group name is not empty
    if crd.spec.group.is_empty() {
        return Err(rusternetes_common::Error::InvalidResource(
            "CRD group cannot be empty".to_string(),
        ));
    }

    // Validate names
    if crd.spec.names.plural.is_empty() {
        return Err(rusternetes_common::Error::InvalidResource(
            "CRD plural name cannot be empty".to_string(),
        ));
    }

    if crd.spec.names.kind.is_empty() {
        return Err(rusternetes_common::Error::InvalidResource(
            "CRD kind cannot be empty".to_string(),
        ));
    }

    // Validate that the CRD name follows the convention: <plural>.<group>
    let expected_name = format!("{}.{}", crd.spec.names.plural, crd.spec.group);
    if crd.metadata.name != expected_name {
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "CRD name must be '{}'",
            expected_name
        )));
    }

    // Validate every served version's x-kubernetes-validations rules:
    // syntax check, unknown-property check, and estimated-cost check.
    // K8s rejects CRD creation when any rule is malformed.
    crate::handlers::cel_validation::validate_crd_versions(crd)?;

    Ok(())
}

/// Custom PATCH handler for CustomResourceDefinitions that stores raw JSON.
///
/// CRDs cannot use the generic typed PATCH handler because round-tripping through
/// the `CustomResourceDefinition` struct loses nested schema fields (like `enum`
/// in `JSONSchemaPropsOrArray`) due to serde untagged enum limitations.
/// K8s stores CRDs as raw JSON in etcd — we must do the same.
///
/// This mirrors the approach already used by create_crd and update_crd (Fix 24).
pub async fn patch_crd(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<serde_json::Value>> {
    info!("Patching cluster-scoped customresourcedefinitions {}", name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "patch", "customresourcedefinitions")
        .with_api_group("apiextensions.k8s.io")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Detect server-side apply
    let is_apply = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("apply-patch"))
        .unwrap_or(false);

    let key = build_key("customresourcedefinitions", None, &name);

    if is_apply {
        if let Some(field_manager) = params.get("fieldManager") {
            info!(
                "Server-side apply for customresourcedefinitions {} by manager {}",
                name, field_manager
            );

            // Get current resource as raw JSON (if exists)
            let current_json: Option<serde_json::Value> =
                state.storage.get::<serde_json::Value>(&key).await.ok();

            // Parse desired resource
            let desired_json: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
                rusternetes_common::Error::InvalidResource(format!("Invalid resource: {}", e))
            })?;

            let force = params
                .get("force")
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(false);

            let apply_params = if force {
                rusternetes_common::server_side_apply::ApplyParams::new(field_manager.clone())
                    .with_force()
            } else {
                rusternetes_common::server_side_apply::ApplyParams::new(field_manager.clone())
            };

            let result = rusternetes_common::server_side_apply::server_side_apply(
                current_json.as_ref(),
                &desired_json,
                &apply_params,
            )
            .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

            match result {
                rusternetes_common::server_side_apply::ApplyResult::Success(mut applied_json) => {
                    // Set the last-applied-configuration annotation
                    if let Some(metadata) = applied_json.get_mut("metadata") {
                        if let Some(obj) = metadata.as_object_mut() {
                            let ann = obj
                                .entry("annotations")
                                .or_insert_with(|| serde_json::json!({}));
                            if let Some(ann_obj) = ann.as_object_mut() {
                                ann_obj.insert(
                                    "kubectl.kubernetes.io/last-applied-configuration".to_string(),
                                    serde_json::Value::String(
                                        serde_json::to_string(&desired_json).unwrap_or_default(),
                                    ),
                                );
                            }
                        }
                    }

                    // Store raw JSON directly — no typed round-trip
                    let saved: serde_json::Value = if current_json.is_some() {
                        state.storage.update(&key, &applied_json).await?
                    } else {
                        state.storage.create(&key, &applied_json).await?
                    };

                    return Ok(Json(saved));
                }
                rusternetes_common::server_side_apply::ApplyResult::Conflicts(conflicts) => {
                    let conflict_details: Vec<String> = conflicts
                        .iter()
                        .map(|c| {
                            format!(
                                "Field '{}' is owned by '{}' (applying as '{}')",
                                c.field, c.current_manager, c.applying_manager
                            )
                        })
                        .collect();

                    return Err(rusternetes_common::Error::Conflict(format!(
                        "Apply conflict: {}. Use force=true to override.",
                        conflict_details.join("; ")
                    )));
                }
            }
        }
    }

    // Standard PATCH operation (not server-side apply)
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/strategic-merge-patch+json");

    let patch_type = crate::patch::PatchType::from_content_type(content_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Get current resource as raw JSON — preserves all nested schema fields
    let current_json: serde_json::Value = state.storage.get(&key).await?;

    // Parse patch document
    let patch_json: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| rusternetes_common::Error::InvalidResource(format!("Invalid patch: {}", e)))?;

    // Apply patch to raw JSON
    let mut patched_json = crate::patch::apply_patch(&current_json, &patch_json, patch_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Increment metadata.generation when spec changes
    {
        let old_spec = current_json.get("spec");
        let new_spec = patched_json.get("spec");
        let spec_changed = match (old_spec, new_spec) {
            (Some(old), Some(new)) => old != new,
            (None, Some(_)) => true,
            (Some(_), None) => true,
            (None, None) => false,
        };
        if spec_changed {
            if let Some(metadata) = patched_json.get_mut("metadata") {
                if let Some(meta_obj) = metadata.as_object_mut() {
                    let current_gen = meta_obj
                        .get("generation")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    meta_obj.insert("generation".to_string(), serde_json::json!(current_gen + 1));
                }
            }
        }
    }

    // Validate that the patched JSON can still be parsed as a CRD (catch structural errors)
    // but do NOT use the typed struct for storage — store the raw JSON directly.
    let _validate: CustomResourceDefinition = serde_json::from_value(patched_json.clone())
        .map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Patched CRD is not valid: {}", e))
        })?;

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!(
            "Dry-run: customresourcedefinitions {} patch validated (not applied)",
            name
        );
        return Ok(Json(patched_json));
    }

    // Store raw JSON directly — preserves enum, nested schemas, etc.
    let updated: serde_json::Value = state.storage.update(&key, &patched_json).await?;

    Ok(Json(updated))
}

/// Enrich a CRD raw JSON value being stored on update with derived metadata
/// and status fields. Specifically:
///
/// - Sets `metadata.uid`/`metadata.name` from the typed CRD.
/// - Bumps `metadata.generation` when the spec differs from the previously
///   stored version.
/// - Preserves the previously stored `status` when the new body omits it
///   (a PUT without a status subresource must not erase Established/NamesAccepted
///   conditions — losing them would prevent the OpenAPI publisher from
///   regenerating x-kubernetes-group-version-kind correctly after a version
///   rename).
/// - Refreshes `status.acceptedNames` from the new spec names.
/// - Appends any new storage versions to `status.storedVersions` without
///   dropping historical entries (K8s leaves removal to the migration
///   controller).
fn enrich_updated_crd(
    raw_value: &mut serde_json::Value,
    crd: &CustomResourceDefinition,
    old_crd_value: Option<&serde_json::Value>,
) {
    let spec_changed = match (
        old_crd_value.and_then(|v| v.get("spec")),
        raw_value.get("spec"),
    ) {
        (Some(old), Some(new)) => old != new,
        (None, Some(_)) => true,
        _ => false,
    };

    let obj = match raw_value.as_object_mut() {
        Some(o) => o,
        None => return,
    };

    if let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert("uid".to_string(), serde_json::json!(crd.metadata.uid));
        meta.insert("name".to_string(), serde_json::json!(crd.metadata.name));

        let old_gen = old_crd_value
            .and_then(|v| v.pointer("/metadata/generation"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let new_gen = if spec_changed { old_gen + 1 } else { old_gen };
        meta.insert("generation".to_string(), serde_json::json!(new_gen));
    }

    // If the new body omitted status, restore the previously stored status so
    // Established/NamesAccepted survive a spec-only update like a rename.
    let body_has_status = obj.get("status").map(|s| !s.is_null()).unwrap_or(false);
    if !body_has_status {
        if let Some(prev_status) = old_crd_value.and_then(|v| v.get("status")).cloned() {
            obj.insert("status".to_string(), prev_status);
        } else if let Ok(status_val) = serde_json::to_value(&crd.status) {
            obj.insert("status".to_string(), status_val);
        }
    }

    // Refresh acceptedNames + storedVersions from the new spec.
    let new_storage: Vec<String> = crd
        .spec
        .versions
        .iter()
        .filter(|v| v.storage)
        .map(|v| v.name.clone())
        .collect();
    let names_val = serde_json::to_value(&crd.spec.names).unwrap_or(serde_json::Value::Null);
    if let Some(status) = obj.get_mut("status").and_then(|s| s.as_object_mut()) {
        status.insert("acceptedNames".to_string(), names_val);
        let mut stored: Vec<String> = status
            .get("storedVersions")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        for v in new_storage {
            if !stored.contains(&v) {
                stored.push(v);
            }
        }
        status.insert("storedVersions".to_string(), serde_json::json!(stored));
    }
}

pub async fn deletecollection_customresourcedefinitions(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection customresourcedefinitions with params: {:?}",
        params
    );

    // Check authorization
    let attrs = RequestAttributes::new(
        auth_ctx.user,
        "deletecollection",
        "customresourcedefinitions",
    )
    .with_api_group("apiextensions.k8s.io");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: CustomResourceDefinition collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all customresourcedefinitions
    let prefix = build_prefix("customresourcedefinitions", None);
    let mut items = state
        .storage
        .list::<CustomResourceDefinition>(&prefix)
        .await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("customresourcedefinitions", None, &item.metadata.name);

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
        "DeleteCollection completed: {} customresourcedefinitions deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        CustomResourceDefinition, CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, ResourceScope,
    };
    use rusternetes_common::types::ObjectMeta;

    fn create_test_crd() -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: ObjectMeta::new("crontabs.stable.example.com"),
            spec: CustomResourceDefinitionSpec {
                group: "stable.example.com".to_string(),
                names: CustomResourceDefinitionNames {
                    plural: "crontabs".to_string(),
                    singular: Some("crontab".to_string()),
                    kind: "CronTab".to_string(),
                    short_names: Some(vec!["ct".to_string()]),
                    categories: None,
                    list_kind: Some("CronTabList".to_string()),
                },
                scope: ResourceScope::Namespaced,
                versions: vec![CustomResourceDefinitionVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: true,
                    deprecated: None,
                    deprecation_warning: None,
                    schema: None,
                    subresources: None,
                    additional_printer_columns: None,
                    selectable_fields: None,
                }],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_validate_crd_success() {
        let crd = create_test_crd();
        assert!(validate_crd(&crd).is_ok());
    }

    #[test]
    fn test_validate_crd_no_versions() {
        let mut crd = create_test_crd();
        crd.spec.versions.clear();
        assert!(validate_crd(&crd).is_err());
    }

    #[test]
    fn test_validate_crd_no_storage_version() {
        let mut crd = create_test_crd();
        crd.spec.versions[0].storage = false;
        assert!(validate_crd(&crd).is_err());
    }

    #[test]
    fn test_validate_crd_multiple_storage_versions() {
        let mut crd = create_test_crd();
        crd.spec.versions.push(CustomResourceDefinitionVersion {
            name: "v2".to_string(),
            served: true,
            storage: true,
            deprecated: None,
            deprecation_warning: None,
            schema: None,
            subresources: None,
            additional_printer_columns: None,
            selectable_fields: None,
        });
        assert!(validate_crd(&crd).is_err());
    }

    #[test]
    fn test_validate_crd_empty_group() {
        let mut crd = create_test_crd();
        crd.spec.group = String::new();
        assert!(validate_crd(&crd).is_err());
    }

    #[test]
    fn test_validate_crd_wrong_name() {
        let mut crd = create_test_crd();
        crd.metadata.name = "wrong-name".to_string();
        assert!(validate_crd(&crd).is_err());
    }

    #[tokio::test]
    async fn test_validating_webhook_called_for_crd_creation() {
        use crate::admission_webhook::AdmissionWebhookManager;
        use rusternetes_common::admission::{
            GroupVersionKind, GroupVersionResource, Operation, UserInfo,
        };
        use rusternetes_common::resources::{
            FailurePolicy, OperationType, Rule, RuleWithOperations, SideEffectClass,
            ValidatingWebhook, ValidatingWebhookConfiguration, WebhookClientConfig,
        };
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        // Create a ValidatingWebhookConfiguration targeting CRDs with FailurePolicy::Fail
        let webhook_config = ValidatingWebhookConfiguration {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
            metadata: rusternetes_common::types::ObjectMeta::new("deny-crd-creation"),
            webhooks: Some(vec![ValidatingWebhook {
                name: "deny-crd.example.com".to_string(),
                client_config: WebhookClientConfig {
                    url: Some("https://127.0.0.1:19443/deny-crd".to_string()),
                    service: None,
                    ca_bundle: None,
                },
                rules: vec![RuleWithOperations {
                    operations: vec![OperationType::Create],
                    rule: Rule {
                        api_groups: vec!["apiextensions.k8s.io".to_string()],
                        api_versions: vec!["v1".to_string()],
                        resources: vec!["customresourcedefinitions".to_string()],
                        scope: None,
                    },
                }],
                failure_policy: Some(FailurePolicy::Fail),
                match_policy: None,
                namespace_selector: None,
                object_selector: None,
                side_effects: SideEffectClass::None,
                timeout_seconds: Some(1),
                admission_review_versions: vec!["v1".to_string()],
                match_conditions: None,
            }]),
        };

        // Store the webhook configuration
        storage
            .create(
                "/registry/validatingwebhookconfigurations/deny-crd-creation",
                &webhook_config,
            )
            .await
            .unwrap();

        // Build a CRD object and webhook parameters
        let crd = create_test_crd();
        let crd_value = serde_json::to_value(&crd).unwrap();

        let gvk = GroupVersionKind {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "customresourcedefinitions".to_string(),
        };
        let user_info = UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        };

        // Call run_validating_webhooks — the webhook URL is unreachable,
        // and FailurePolicy is Fail, so this should return an error,
        // proving the webhook infrastructure was consulted for CRDs.
        let result = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                None,
                &crd.metadata.name,
                Some(crd_value),
                None,
                &user_info,
            )
            .await;

        assert!(
            result.is_err(),
            "Expected webhook call to fail (proving webhook was consulted for CRD creation), but got: {:?}",
            result
        );
    }

    /// After renaming a CRD's served/storage version, the stored representation
    /// must reflect the new version names so the OpenAPI publisher emits a
    /// correct x-kubernetes-group-version-kind extension. Specifically:
    ///   * status conditions (Established / NamesAccepted) must be preserved
    ///     even when the PUT body omits status — otherwise downstream
    ///     publishers refuse to advertise the CRD.
    ///   * metadata.generation must bump on a spec change so watchers reload.
    ///   * status.storedVersions must accumulate any new storage version while
    ///     preserving prior entries (the migration controller — not the API
    ///     server — is responsible for trimming).
    ///   * the resulting spec.versions list drives the GVK extension, so each
    ///     served version must map cleanly back to {group, version, kind}.
    #[test]
    fn test_gvk_after_version_rename_preserves_status_and_bumps_generation() {
        // Previously stored CRD with version "v1beta1" as the storage version.
        let old_stored = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {
                "name": "examples.test.example.com",
                "uid": "abc-123",
                "generation": 1
            },
            "spec": {
                "group": "test.example.com",
                "names": { "plural": "examples", "kind": "Example" },
                "scope": "Namespaced",
                "versions": [
                    { "name": "v1beta1", "served": true, "storage": true }
                ]
            },
            "status": {
                "conditions": [
                    { "type": "Established", "status": "True" },
                    { "type": "NamesAccepted", "status": "True" }
                ],
                "acceptedNames": { "plural": "examples", "kind": "Example" },
                "storedVersions": ["v1beta1"]
            }
        });

        // Client renames v1beta1 -> v1beta2 (same kind) via PUT — no status.
        let renamed_body = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": { "name": "examples.test.example.com" },
            "spec": {
                "group": "test.example.com",
                "names": { "plural": "examples", "kind": "Example" },
                "scope": "Namespaced",
                "versions": [
                    { "name": "v1beta2", "served": true, "storage": true }
                ]
            }
        });

        let typed: CustomResourceDefinition =
            serde_json::from_value(renamed_body.clone()).expect("decode renamed CRD");
        let mut raw = renamed_body;
        enrich_updated_crd(&mut raw, &typed, Some(&old_stored));

        // Status preserved across the rename — Established condition still present.
        let conditions = raw.pointer("/status/conditions").expect("conditions");
        let established = conditions
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c.get("type").and_then(|v| v.as_str()) == Some("Established"))
            .expect("Established condition preserved");
        assert_eq!(
            established.get("status").and_then(|v| v.as_str()),
            Some("True")
        );

        // Generation bumped because spec changed.
        assert_eq!(
            raw.pointer("/metadata/generation").and_then(|v| v.as_i64()),
            Some(2),
            "generation must bump from 1 -> 2 on spec change"
        );

        // storedVersions accumulates the new storage version (both old + new).
        let stored: Vec<String> = raw
            .pointer("/status/storedVersions")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(stored.contains(&"v1beta1".to_string()), "old version kept");
        assert!(stored.contains(&"v1beta2".to_string()), "new version added");

        // acceptedNames refreshed from the new spec.
        assert_eq!(
            raw.pointer("/status/acceptedNames/kind")
                .and_then(|v| v.as_str()),
            Some("Example")
        );

        // GVK extension synthesis from the served versions yields the renamed
        // version (and only that one).
        let gvks: Vec<serde_json::Value> = raw
            .pointer("/spec/versions")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter(|v| v.get("served").and_then(|s| s.as_bool()) == Some(true))
            .map(|v| {
                serde_json::json!({
                    "group": raw.pointer("/spec/group").and_then(|g| g.as_str()).unwrap_or(""),
                    "version": v.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                    "kind": raw.pointer("/spec/names/kind").and_then(|k| k.as_str()).unwrap_or(""),
                })
            })
            .collect();
        assert_eq!(
            gvks,
            vec![serde_json::json!({
                "group": "test.example.com",
                "version": "v1beta2",
                "kind": "Example"
            })],
            "after rename the GVK extension must reflect only the new served version"
        );
    }

    /// A no-op update (same spec sent back) must not bump generation and must
    /// not duplicate entries in storedVersions.
    #[test]
    fn test_noop_update_keeps_generation_and_storedversions() {
        let old_stored = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {
                "name": "examples.test.example.com",
                "uid": "abc-123",
                "generation": 3
            },
            "spec": {
                "group": "test.example.com",
                "names": { "plural": "examples", "kind": "Example" },
                "scope": "Namespaced",
                "versions": [
                    { "name": "v1", "served": true, "storage": true }
                ]
            },
            "status": { "storedVersions": ["v1"] }
        });
        // PUT body has identical spec — no rename.
        let same_body = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": { "name": "examples.test.example.com" },
            "spec": {
                "group": "test.example.com",
                "names": { "plural": "examples", "kind": "Example" },
                "scope": "Namespaced",
                "versions": [
                    { "name": "v1", "served": true, "storage": true }
                ]
            }
        });

        let typed: CustomResourceDefinition =
            serde_json::from_value(same_body.clone()).expect("decode CRD");
        let mut raw = same_body;
        enrich_updated_crd(&mut raw, &typed, Some(&old_stored));

        assert_eq!(
            raw.pointer("/metadata/generation").and_then(|v| v.as_i64()),
            Some(3),
            "no-op update must not bump generation"
        );
        let stored: Vec<&str> = raw
            .pointer("/status/storedVersions")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(stored, vec!["v1"], "storedVersions must not duplicate");
    }
}
