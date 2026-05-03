/// Generic PATCH handler that works for any resource type
///
/// This module provides a generic implementation of PATCH operations
/// that can be used across all Kubernetes resource types.
use crate::{
    middleware::AuthContext,
    patch::{apply_patch, PatchType},
    state::ApiServerState,
};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    server_side_apply::{server_side_apply, ApplyParams, ApplyResult},
    Result,
};
use rusternetes_storage::{build_key, Storage};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Generic PATCH handler for namespaced resources
///
/// # Type Parameters
/// - `T`: The resource type (must implement Serialize + DeserializeOwned)
///
/// # Parameters
/// - `state`: API server state
/// - `auth_ctx`: Authentication context
/// - `namespace`: Resource namespace
/// - `name`: Resource name
/// - `headers`: HTTP headers (contains Content-Type for patch type)
/// - `body`: Patch document
/// - `resource_type`: Resource type name (e.g., "deployments", "services")
/// - `api_group`: API group (e.g., "apps", "" for core)
///
/// # Returns
/// Updated resource after applying patch
#[allow(clippy::too_many_arguments)]
pub async fn patch_namespaced_resource<T>(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
    resource_type: &str,
    api_group: &str,
) -> Result<Json<T>>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    info!("Patching {} {}/{}", resource_type, namespace, name);

    // Save user info for webhooks before RBAC check consumes it
    let webhook_user = auth_ctx.user.clone();

    // Check authorization - use 'patch' verb for RBAC
    let attrs = RequestAttributes::new(auth_ctx.user, "patch", resource_type)
        .with_namespace(&namespace)
        .with_api_group(api_group)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // SSA is ONLY used with Content-Type: application/apply-patch+yaml.
    // Regular patches with fieldManager just track ownership, not SSA.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go
    let is_apply = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("apply-patch"))
        .unwrap_or(false);

    if is_apply {
        if let Some(field_manager) = params.get("fieldManager") {
            info!(
                "Server-side apply for {} {}/{} by manager {}",
                resource_type, namespace, name, field_manager
            );

            // Get current resource (if exists)
            let key = build_key(resource_type, Some(&namespace), &name);
            let current_json = match state.storage.get::<T>(&key).await {
                Ok(current) => Some(
                    serde_json::to_value(&current)
                        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?,
                ),
                Err(rusternetes_common::Error::NotFound(_)) => None,
                Err(e) => return Err(e),
            };

            // Parse desired resource
            let desired_json: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
                rusternetes_common::Error::InvalidResource(format!("Invalid resource: {}", e))
            })?;

            // Apply with server-side apply semantics
            let force = params
                .get("force")
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(false);

            let apply_params = if force {
                ApplyParams::new(field_manager.clone()).with_force()
            } else {
                ApplyParams::new(field_manager.clone())
            };

            let result = server_side_apply(current_json.as_ref(), &desired_json, &apply_params)
                .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

            match result {
                ApplyResult::Success(mut applied_json) => {
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

                    // Convert to resource type
                    let applied_resource: T =
                        serde_json::from_value(applied_json).map_err(|e| {
                            rusternetes_common::Error::InvalidResource(format!(
                                "Invalid result: {}",
                                e
                            ))
                        })?;

                    // Save to storage (create or update)
                    let saved = if current_json.is_some() {
                        state.storage.update(&key, &applied_resource).await?
                    } else {
                        state.storage.create(&key, &applied_resource).await?
                    };

                    return Ok(Json(saved));
                }
                ApplyResult::Conflicts(conflicts) => {
                    // Return 409 Conflict with details
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
    // Get Content-Type header — check X-Original-Content-Type first (set by middleware
    // when normalizing patch content types to application/json for Axum compatibility)
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/strategic-merge-patch+json");

    // Parse patch type
    let patch_type = PatchType::from_content_type(content_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Get current resource
    let key = build_key(resource_type, Some(&namespace), &name);
    let current_resource: T = state.storage.get(&key).await?;

    // Convert to JSON for patching
    let current_json = serde_json::to_value(&current_resource)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    // Parse patch document
    let patch_json: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| rusternetes_common::Error::InvalidResource(format!("Invalid patch: {}", e)))?;

    // Apply patch (clone patch_type for potential retry on rv conflict)
    let patch_type_for_retry = patch_type.clone();
    let mut patched_json = apply_patch(&current_json, &patch_json, patch_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Increment metadata.generation only when spec changes.
    // K8s only tracks generation for resources WITH a spec field
    // (Deployments, StatefulSets, Services, etc.). Resources without
    // spec (Events, ConfigMaps, Secrets) don't increment generation.
    // See: staging/src/k8s.io/apiserver/pkg/registry/rest/update.go
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

    // Convert back to resource type — use lenient deserialization
    let mut patched_resource: T = serde_json::from_value(patched_json.clone()).map_err(|e| {
        // If strict deserialization fails, try storing as raw JSON and retrieving
        tracing::warn!("Patch result deserialization warning (storing raw): {}", e);
        rusternetes_common::Error::InvalidResource(format!("Invalid result: {}", e))
    })?;

    // For resources with metadata, ensure name/namespace can't be changed via patch
    // This is handled by updating in storage with the original key

    // Run admission webhooks on the patched resource (same as update path)
    {
        use rusternetes_common::admission;
        let gvk = admission::GroupVersionKind {
            group: api_group.to_string(),
            version: "v1".to_string(),
            kind: resource_type.to_string(),
        };
        let gvr = admission::GroupVersionResource {
            group: api_group.to_string(),
            version: "v1".to_string(),
            resource: resource_type.to_string(),
        };
        let user_info = admission::UserInfo {
            username: webhook_user.username.clone(),
            uid: webhook_user.uid.clone(),
            groups: webhook_user.groups.clone(),
        };

        // Mutating webhooks
        let (response, mutated_obj) = state
            .webhook_manager
            .run_mutating_webhooks(
                &admission::Operation::Update,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                Some(patched_json.clone()),
                Some(current_json.clone()),
                &user_info,
            )
            .await?;
        if let admission::AdmissionResponse::Deny(reason) = &response {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
        if let Some(mutated) = mutated_obj {
            if let Ok(m) = serde_json::from_value::<T>(mutated) {
                patched_resource = m;
                patched_json = serde_json::to_value(&patched_resource)
                    .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
            }
        }

        // Validating webhooks
        if let admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &admission::Operation::Update,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                Some(patched_json),
                Some(current_json),
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

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!(
            "Dry-run: {} {}/{} patch validated (not applied)",
            resource_type, namespace, name
        );
        return Ok(Json(patched_resource));
    }

    // Update in storage with retry on conflict.
    // PATCH operations are idempotent — on rv conflict, re-read the resource,
    // re-apply the patch, and retry. This handles the common case where the
    // kubelet updates pod status between the client's read and patch.
    match state.storage.update(&key, &patched_resource).await {
        Ok(updated) => Ok(Json(updated)),
        Err(rusternetes_common::Error::Conflict(_)) => {
            // Re-read, re-apply patch, retry once
            let fresh: T = state.storage.get(&key).await?;
            let fresh_json = serde_json::to_value(&fresh)
                .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
            let re_patched = apply_patch(&fresh_json, &patch_json, patch_type_for_retry)
                .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;
            let re_patched_resource: T = serde_json::from_value(re_patched).map_err(|e| {
                rusternetes_common::Error::InvalidResource(format!("Invalid result: {}", e))
            })?;
            let updated = state.storage.update(&key, &re_patched_resource).await?;
            Ok(Json(updated))
        }
        Err(e) => Err(e),
    }
}

/// Generic PATCH handler for cluster-scoped resources
///
/// # Type Parameters
/// - `T`: The resource type (must implement Serialize + DeserializeOwned)
///
/// # Parameters
/// - `state`: API server state
/// - `auth_ctx`: Authentication context
/// - `name`: Resource name
/// - `headers`: HTTP headers (contains Content-Type for patch type)
/// - `body`: Patch document
/// - `resource_type`: Resource type name (e.g., "nodes", "clusterroles")
/// - `api_group`: API group (e.g., "rbac.authorization.k8s.io", "" for core)
///
/// # Returns
/// Updated resource after applying patch
#[allow(clippy::too_many_arguments)]
pub async fn patch_cluster_resource<T>(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
    resource_type: &str,
    api_group: &str,
) -> Result<Json<T>>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    info!("Patching cluster-scoped {} {}", resource_type, name);

    // Check authorization - use 'patch' verb for RBAC
    let attrs = RequestAttributes::new(auth_ctx.user, "patch", resource_type)
        .with_api_group(api_group)
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // SSA is ONLY used with Content-Type: application/apply-patch+yaml.
    // Regular patches with fieldManager just track ownership, not SSA.
    // K8s ref: staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go
    let is_apply = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("apply-patch"))
        .unwrap_or(false);

    if is_apply {
        if let Some(field_manager) = params.get("fieldManager") {
            info!(
                "Server-side apply for {} {} by manager {}",
                resource_type, name, field_manager
            );

            // Get current resource (if exists)
            let key = build_key(resource_type, None, &name);
            let current_json = match state.storage.get::<T>(&key).await {
                Ok(current) => Some(
                    serde_json::to_value(&current)
                        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?,
                ),
                Err(rusternetes_common::Error::NotFound(_)) => None,
                Err(e) => return Err(e),
            };

            // Parse desired resource
            let desired_json: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
                rusternetes_common::Error::InvalidResource(format!("Invalid resource: {}", e))
            })?;

            // Apply with server-side apply semantics
            let force = params
                .get("force")
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(false);

            let apply_params = if force {
                ApplyParams::new(field_manager.clone()).with_force()
            } else {
                ApplyParams::new(field_manager.clone())
            };

            let result = server_side_apply(current_json.as_ref(), &desired_json, &apply_params)
                .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

            match result {
                ApplyResult::Success(mut applied_json) => {
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

                    // Convert to resource type
                    let applied_resource: T =
                        serde_json::from_value(applied_json).map_err(|e| {
                            rusternetes_common::Error::InvalidResource(format!(
                                "Invalid result: {}",
                                e
                            ))
                        })?;

                    // Save to storage (create or update)
                    let saved = if current_json.is_some() {
                        state.storage.update(&key, &applied_resource).await?
                    } else {
                        state.storage.create(&key, &applied_resource).await?
                    };

                    return Ok(Json(saved));
                }
                ApplyResult::Conflicts(conflicts) => {
                    // Return 409 Conflict with details
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
    // Get Content-Type header — check X-Original-Content-Type first (set by middleware
    // when normalizing patch content types to application/json for Axum compatibility)
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/strategic-merge-patch+json");

    // Parse patch type
    let patch_type = PatchType::from_content_type(content_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Get current resource
    let key = build_key(resource_type, None, &name);
    let current_resource: T = state.storage.get(&key).await?;

    // Convert to JSON for patching
    let current_json = serde_json::to_value(&current_resource)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;

    // Parse patch document
    let patch_json: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| rusternetes_common::Error::InvalidResource(format!("Invalid patch: {}", e)))?;

    // Apply patch (clone patch_type for potential retry on rv conflict)
    let _patch_type_for_retry = patch_type.clone();
    let mut patched_json = apply_patch(&current_json, &patch_json, patch_type)
        .map_err(|e| rusternetes_common::Error::InvalidResource(e.to_string()))?;

    // Increment metadata.generation only when spec changes.
    // K8s only tracks generation for resources WITH a spec field
    // (Deployments, StatefulSets, Services, etc.). Resources without
    // spec (Events, ConfigMaps, Secrets) don't increment generation.
    // See: staging/src/k8s.io/apiserver/pkg/registry/rest/update.go
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

    // Convert back to resource type
    let patched_resource: T = serde_json::from_value(patched_json).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("Invalid result: {}", e))
    })?;

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!(
            "Dry-run: {} {} patch validated (not applied)",
            resource_type, name
        );
        return Ok(Json(patched_resource));
    }

    // Update in storage
    let updated = state.storage.update(&key, &patched_resource).await?;

    Ok(Json(updated))
}

/// Macro to create a PATCH handler for a namespaced resource
///
/// This macro generates a handler function that calls the generic patch implementation
/// with the appropriate resource type and metadata.
///
/// # Example
/// ```ignore
/// patch_handler_namespaced!(patch_deployment, Deployment, "deployments", "apps");
/// ```
#[macro_export]
macro_rules! patch_handler_namespaced {
    ($handler_name:ident, $resource_type:ty, $resource_name:expr, $api_group:expr) => {
        pub async fn $handler_name(
            state: axum::extract::State<std::sync::Arc<$crate::state::ApiServerState>>,
            auth_ctx: axum::Extension<$crate::middleware::AuthContext>,
            path: axum::extract::Path<(String, String)>,
            query: axum::extract::Query<std::collections::HashMap<String, String>>,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> rusternetes_common::Result<axum::Json<$resource_type>> {
            $crate::handlers::generic_patch::patch_namespaced_resource::<$resource_type>(
                state,
                auth_ctx,
                path,
                query,
                headers,
                body,
                $resource_name,
                $api_group,
            )
            .await
        }
    };
}

/// Macro to create a PATCH handler for a cluster-scoped resource
///
/// This macro generates a handler function that calls the generic patch implementation
/// with the appropriate resource type and metadata.
///
/// # Example
/// ```ignore
/// patch_handler_cluster!(patch_node, Node, "nodes", "");
/// ```
#[macro_export]
macro_rules! patch_handler_cluster {
    ($handler_name:ident, $resource_type:ty, $resource_name:expr, $api_group:expr) => {
        pub async fn $handler_name(
            state: axum::extract::State<std::sync::Arc<$crate::state::ApiServerState>>,
            auth_ctx: axum::Extension<$crate::middleware::AuthContext>,
            path: axum::extract::Path<String>,
            query: axum::extract::Query<std::collections::HashMap<String, String>>,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> rusternetes_common::Result<axum::Json<$resource_type>> {
            $crate::handlers::generic_patch::patch_cluster_resource::<$resource_type>(
                state,
                auth_ctx,
                path,
                query,
                headers,
                body,
                $resource_name,
                $api_group,
            )
            .await
        }
    };
}
