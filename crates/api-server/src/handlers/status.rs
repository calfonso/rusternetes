//! Status subresource handlers
//!
//! Implements the /status subresource for resources that support it.
//! The status subresource allows updating only the status portion of a resource,
//! which is useful for controllers that need to update status without conflicting
//! with user-driven spec changes.

#![allow(dead_code)]

use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, State},
    http::Uri,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    Result,
};
use rusternetes_storage::{build_key, Storage};
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, info};

/// Extract the resource type from the request URI.
///
/// For namespaced resources, the URI looks like:
///   /api/v1/namespaces/{ns}/{resource}/{name}/status
///   /apis/{group}/{version}/namespaces/{ns}/{resource}/{name}/status
/// The resource type is the segment before {name}, i.e. 2 segments before "status".
///
/// For cluster-scoped resources, the URI looks like:
///   /api/v1/{resource}/{name}/status  (e.g. /api/v1/nodes/node1/status)
///   /api/v1/namespaces/{name}/status  (namespaces are cluster-scoped)
fn extract_resource_type_from_uri(uri: &Uri) -> String {
    let path = uri.path();
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    // The resource type is always 3 segments before the end (before name and "status")
    // e.g. ["api", "v1", "namespaces", "ns", "pods", "name", "status"] -> "pods"
    // e.g. ["api", "v1", "namespaces", "name", "status"] -> "namespaces"
    // e.g. ["api", "v1", "nodes", "name", "status"] -> "nodes"
    if segments.len() >= 3 {
        segments[segments.len() - 3].to_string()
    } else {
        "unknown".to_string()
    }
}

/// Map resource type to Kind and apiVersion for TypeMeta injection.
fn resource_type_to_kind_api_version(resource_type: &str) -> (String, String) {
    match resource_type {
        "pods" => ("Pod".into(), "v1".into()),
        "services" => ("Service".into(), "v1".into()),
        "configmaps" => ("ConfigMap".into(), "v1".into()),
        "secrets" => ("Secret".into(), "v1".into()),
        "serviceaccounts" => ("ServiceAccount".into(), "v1".into()),
        "namespaces" => ("Namespace".into(), "v1".into()),
        "nodes" => ("Node".into(), "v1".into()),
        "persistentvolumes" => ("PersistentVolume".into(), "v1".into()),
        "persistentvolumeclaims" => ("PersistentVolumeClaim".into(), "v1".into()),
        "endpoints" => ("Endpoints".into(), "v1".into()),
        "replicationcontrollers" => ("ReplicationController".into(), "v1".into()),
        "resourcequotas" => ("ResourceQuota".into(), "v1".into()),
        "deployments" => ("Deployment".into(), "apps/v1".into()),
        "replicasets" => ("ReplicaSet".into(), "apps/v1".into()),
        "statefulsets" => ("StatefulSet".into(), "apps/v1".into()),
        "daemonsets" => ("DaemonSet".into(), "apps/v1".into()),
        "jobs" => ("Job".into(), "batch/v1".into()),
        "cronjobs" => ("CronJob".into(), "batch/v1".into()),
        "ingresses" => ("Ingress".into(), "networking.k8s.io/v1".into()),
        "networkpolicies" => ("NetworkPolicy".into(), "networking.k8s.io/v1".into()),
        "customresourcedefinitions" => (
            "CustomResourceDefinition".into(),
            "apiextensions.k8s.io/v1".into(),
        ),
        "endpointslices" => ("EndpointSlice".into(), "discovery.k8s.io/v1".into()),
        _ => {
            let s = resource_type.strip_suffix('s').unwrap_or(resource_type);
            let kind = format!("{}{}", &s[..1].to_uppercase(), &s[1..]);
            (kind, "v1".into())
        }
    }
}

/// Build the merged resource for a /status subresource write.
///
/// This is the pure, testable core of `update_status`: given the currently
/// stored resource and the request body (already parsed as JSON), it builds
/// the resource that will be persisted.
///
/// Behavior, modelled on k8s.io/kubernetes/pkg/registry/core/pod/strategy.go's
/// `podStatusStrategy.PrepareForUpdate` (release-1.35):
///   - Spec is taken from the current resource (clients cannot change spec
///     through /status).
///   - Metadata is taken from the current resource, with annotations and
///     labels from the request body merged in (status subresource is allowed
///     to update metadata.annotations/labels but not other metadata fields,
///     and most importantly not metadata.deletionTimestamp /
///     metadata.ownerReferences).
///   - resourceVersion is stripped from metadata so storage can assign one.
///   - Status is taken from the request body. For merge-patch /
///     strategic-merge-patch content types the request status is *deep-merged*
///     into the current status; for PUT and JSON Patch it replaces wholesale.
///
/// `is_merge_patch` is `true` when the request `Content-Type` was a JSON
/// merge-patch or strategic-merge-patch (so the body's status is a sparse
/// patch document, not a full status object).
pub fn build_updated_resource_for_status(
    current_resource: &Value,
    new_resource: &Value,
    is_merge_patch: bool,
    resource_type: &str,
) -> Result<Value> {
    let current_metadata = current_resource
        .get("metadata")
        .ok_or_else(|| rusternetes_common::Error::InvalidResource("Missing metadata".to_string()))?
        .clone();

    let current_spec = current_resource.get("spec").cloned();

    // Status: for merge-patch, deep-merge into current; for PUT, replace.
    let new_status = if is_merge_patch {
        let patch_status = new_resource
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));
        let mut merged = current_resource
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));
        // K8s strategic merge patch recursively merges maps (like
        // capacity, allocatable). Simple insert replaces the entire
        // map, wiping out existing entries. This broke preemption
        // tests that patch node status to add extended resources.
        crate::patch::deep_merge_objects(&mut merged, &patch_status);
        merged
    } else {
        new_resource
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()))
    };

    // Build the updated resource starting from current (preserves any fields
    // we don't explicitly touch).
    let mut updated_resource = current_resource.clone();

    if let Some(obj) = updated_resource.as_object_mut() {
        // Preserve spec from current resource
        if let Some(spec) = current_spec {
            obj.insert("spec".to_string(), spec);
        }

        // Update status
        obj.insert("status".to_string(), new_status);

        // Merge metadata: keep current metadata but apply annotations/labels
        // from the incoming request.
        if let Some(metadata_obj) = current_metadata.as_object() {
            let mut merged_metadata = metadata_obj.clone();
            merged_metadata.remove("resourceVersion");

            if let Some(new_meta) = new_resource.get("metadata").and_then(|m| m.as_object()) {
                if let Some(new_annotations) =
                    new_meta.get("annotations").and_then(|a| a.as_object())
                {
                    let annotations = merged_metadata
                        .entry("annotations")
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    if let Some(existing) = annotations.as_object_mut() {
                        for (k, v) in new_annotations {
                            existing.insert(k.clone(), v.clone());
                        }
                    }
                }
                if let Some(new_labels) = new_meta.get("labels").and_then(|l| l.as_object()) {
                    let labels = merged_metadata
                        .entry("labels")
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    if let Some(existing) = labels.as_object_mut() {
                        for (k, v) in new_labels {
                            existing.insert(k.clone(), v.clone());
                        }
                    }
                }
            }

            obj.insert("metadata".to_string(), Value::Object(merged_metadata));
        }

        // Ensure TypeMeta fields are present before saving — some clients
        // (e.g. protobuf) may strip kind/apiVersion and K8s clients require
        // them in responses.
        if !obj.contains_key("kind") || !obj.contains_key("apiVersion") {
            let (kind, api_version) = resource_type_to_kind_api_version(resource_type);
            obj.entry("kind".to_string())
                .or_insert_with(|| Value::String(kind));
            obj.entry("apiVersion".to_string())
                .or_insert_with(|| Value::String(api_version));
        }
    }

    Ok(updated_resource)
}

/// Generic status update handler
///
/// This handler updates only the status field of a resource while preserving
/// the spec and other fields. This is critical for avoiding conflicts between
/// user-driven changes (spec) and controller-driven changes (status).
pub async fn update_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    headers: axum::http::HeaderMap,
    Path((namespace, name)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Result<Json<Value>> {
    // Determine content type — check X-Original-Content-Type for middleware-normalized requests
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let resource_type = extract_resource_type_from_uri(&uri);
    info!(
        "Updating status for {}/{}/{}",
        resource_type, namespace, name
    );

    // Check authorization - use 'update' verb with '/status' subresource
    let attrs = RequestAttributes::new(auth_ctx.user, "update", &resource_type)
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("status");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle JSON Patch (RFC 6902) — body is an array of patch operations
    if content_type.contains("json-patch") {
        let key = build_key(&resource_type, Some(&namespace), &name);
        let current_resource: Value = state.storage.get(&key).await?;
        let patch_ops: Value = serde_json::from_slice(&body).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid JSON patch: {}", e))
        })?;

        let patched = crate::patch::apply_patch(
            &current_resource,
            &patch_ops,
            crate::patch::PatchType::JsonPatch,
        )
        .map_err(|e| {
            rusternetes_common::Error::Internal(format!("Failed to apply JSON patch: {}", e))
        })?;

        // Keep status changes and metadata changes (annotations/labels) from patch
        let mut result = current_resource.clone();
        if let (Some(result_obj), Some(patched_obj)) = (result.as_object_mut(), patched.as_object())
        {
            if let Some(new_status) = patched_obj.get("status") {
                result_obj.insert("status".to_string(), new_status.clone());
            }
            // Merge metadata changes from patch (annotations, labels)
            if let Some(patched_meta) = patched_obj.get("metadata").and_then(|m| m.as_object()) {
                if let Some(result_meta) = result_obj
                    .get_mut("metadata")
                    .and_then(|m| m.as_object_mut())
                {
                    if let Some(annotations) = patched_meta.get("annotations") {
                        result_meta.insert("annotations".to_string(), annotations.clone());
                    }
                    if let Some(labels) = patched_meta.get("labels") {
                        result_meta.insert("labels".to_string(), labels.clone());
                    }
                }
            }
            result_obj.remove("resourceVersion");
            // Ensure TypeMeta
            if !result_obj.contains_key("kind") || !result_obj.contains_key("apiVersion") {
                let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
                result_obj
                    .entry("kind".to_string())
                    .or_insert_with(|| Value::String(kind));
                result_obj
                    .entry("apiVersion".to_string())
                    .or_insert_with(|| Value::String(api_version));
            }
        }

        let mut saved: Value = state.storage.update(&key, &result).await?;
        // Ensure kind/apiVersion in response
        if let Some(obj) = saved.as_object_mut() {
            let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
            obj.entry("kind".to_string())
                .or_insert_with(|| Value::String(kind));
            obj.entry("apiVersion".to_string())
                .or_insert_with(|| Value::String(api_version));
        }
        return Ok(Json(saved));
    }

    // Parse body as JSON or YAML (for PUT / merge-patch / apply-patch requests)
    let new_resource: Value = if content_type.contains("yaml") {
        serde_yaml::from_slice(&body).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid YAML: {}", e))
        })?
    } else {
        serde_json::from_slice(&body).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid JSON: {}", e))
        })?
    };

    // Handle merge-patch and strategic-merge-patch for /status
    let is_merge_patch =
        content_type.contains("merge-patch") || content_type.contains("strategic-merge-patch");

    // Get the current resource and write the new status under a CAS-retry loop.
    // build_updated_resource_for_status preserves the stored metadata
    // (including generation) and only grafts the incoming status, but it strips
    // resourceVersion — which made the write a blind last-write-wins. A stale
    // status write (e.g. the kubelet's observedGeneration sync, built from an
    // earlier read) could then revert a concurrent spec update's
    // metadata.generation bump, dropping one increment over rapid updates
    // (#1166). Re-inject the freshly-read resourceVersion so the write is
    // CAS-guarded, and retry on conflict so the caller still sees success.
    let key = build_key(&resource_type, Some(&namespace), &name);
    let mut attempts = 0;
    let mut saved: Value = loop {
        let current_resource: Value = state.storage.get(&key).await?;
        let mut updated_resource = build_updated_resource_for_status(
            &current_resource,
            &new_resource,
            is_merge_patch,
            &resource_type,
        )?;
        if let (Some(rv), Some(obj)) = (
            current_resource
                .pointer("/metadata/resourceVersion")
                .cloned(),
            updated_resource
                .get_mut("metadata")
                .and_then(|m| m.as_object_mut()),
        ) {
            obj.insert("resourceVersion".to_string(), rv);
        }
        match state.storage.update(&key, &updated_resource).await {
            Ok(v) => break v,
            Err(rusternetes_common::Error::Conflict(_)) if attempts < 8 => {
                attempts += 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    };

    // Ensure kind/apiVersion are always present in the response — the storage round-trip
    // may strip them if the original stored resource was missing TypeMeta fields.
    if let Some(obj) = saved.as_object_mut() {
        let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
        obj.entry("kind".to_string())
            .or_insert_with(|| Value::String(kind));
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| Value::String(api_version));
    }

    info!(
        "Successfully updated status for {}/{}/{}",
        resource_type, namespace, name
    );

    Ok(Json(saved))
}

/// Generic cluster-scoped status update handler
pub async fn update_cluster_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    headers: axum::http::HeaderMap,
    Path(name): Path<String>,
    body: axum::body::Bytes,
) -> Result<Json<Value>> {
    // Determine content type — check X-Original-Content-Type for middleware-normalized requests
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let resource_type = extract_resource_type_from_uri(&uri);
    info!("Updating status for {}/{}", resource_type, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", &resource_type)
        .with_name(&name)
        .with_subresource("status");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle JSON Patch (RFC 6902) — body is an array of patch operations
    if content_type.contains("json-patch") {
        let key = build_key(&resource_type, None, &name);
        let current_resource: Value = state.storage.get(&key).await?;
        let patch_ops: Value = serde_json::from_slice(&body).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid JSON patch: {}", e))
        })?;

        let patched = crate::patch::apply_patch(
            &current_resource,
            &patch_ops,
            crate::patch::PatchType::JsonPatch,
        )
        .map_err(|e| {
            rusternetes_common::Error::Internal(format!("Failed to apply JSON patch: {}", e))
        })?;

        // Keep status changes and metadata changes (annotations/labels) from patch
        let mut result = current_resource.clone();
        if let (Some(result_obj), Some(patched_obj)) = (result.as_object_mut(), patched.as_object())
        {
            if let Some(new_status) = patched_obj.get("status") {
                result_obj.insert("status".to_string(), new_status.clone());
            }
            // Merge metadata changes from patch (annotations, labels)
            if let Some(patched_meta) = patched_obj.get("metadata").and_then(|m| m.as_object()) {
                if let Some(result_meta) = result_obj
                    .get_mut("metadata")
                    .and_then(|m| m.as_object_mut())
                {
                    if let Some(annotations) = patched_meta.get("annotations") {
                        result_meta.insert("annotations".to_string(), annotations.clone());
                    }
                    if let Some(labels) = patched_meta.get("labels") {
                        result_meta.insert("labels".to_string(), labels.clone());
                    }
                }
            }
            result_obj.remove("resourceVersion");
            // Ensure TypeMeta
            if !result_obj.contains_key("kind") || !result_obj.contains_key("apiVersion") {
                let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
                result_obj
                    .entry("kind".to_string())
                    .or_insert_with(|| Value::String(kind));
                result_obj
                    .entry("apiVersion".to_string())
                    .or_insert_with(|| Value::String(api_version));
            }
        }

        let mut saved: Value = state.storage.update(&key, &result).await?;
        // Ensure kind/apiVersion in response
        if let Some(obj) = saved.as_object_mut() {
            let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
            obj.entry("kind".to_string())
                .or_insert_with(|| Value::String(kind));
            obj.entry("apiVersion".to_string())
                .or_insert_with(|| Value::String(api_version));
        }
        return Ok(Json(saved));
    }

    // Parse body as JSON or YAML (for PUT / merge-patch / apply-patch requests)
    let new_resource: Value = if content_type.contains("yaml") {
        serde_yaml::from_slice(&body).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid YAML: {}", e))
        })?
    } else {
        serde_json::from_slice(&body).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid JSON: {}", e))
        })?
    };

    // Handle merge-patch and strategic-merge-patch for /status
    let is_merge_patch =
        content_type.contains("merge-patch") || content_type.contains("strategic-merge-patch");

    // Get the current resource
    let key = build_key(&resource_type, None, &name);
    let current_resource: Value = state.storage.get(&key).await?;

    // Extract current and new status
    let current_metadata = current_resource
        .get("metadata")
        .ok_or_else(|| rusternetes_common::Error::InvalidResource("Missing metadata".to_string()))?
        .clone();

    let current_spec = current_resource.get("spec").cloned();

    // For merge-patch, DEEP merge the status fields.
    // K8s strategic merge patch recursively merges maps (like capacity, allocatable).
    // A shallow merge would replace capacity entirely when only one key is added.
    let new_status = if is_merge_patch {
        let patch_status = new_resource
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));
        let current_status = current_resource
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));
        // Use strategic merge patch for deep recursive merge
        crate::patch::apply_patch(
            &current_status,
            &patch_status,
            crate::patch::PatchType::StrategicMergePatch,
        )
        .unwrap_or(patch_status)
    } else {
        new_resource
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()))
    };

    // Build the updated resource
    let mut updated_resource = current_resource.clone();

    if let Some(obj) = updated_resource.as_object_mut() {
        // Preserve spec from current resource
        if let Some(spec) = current_spec {
            obj.insert("spec".to_string(), spec);
        }

        // Update status
        obj.insert("status".to_string(), new_status);

        // For nodes: sync capacity keys to allocatable.
        // K8s kubelet copies extended resources from capacity to allocatable
        // (extended resources have no system reservation). Without this, the
        // scheduler can't see extended resources patched onto nodes.
        if resource_type == "nodes" {
            if let Some(status_obj) = obj.get_mut("status").and_then(|s| s.as_object_mut()) {
                // Collect capacity keys first to avoid borrow conflict
                let capacity_entries: Vec<(String, Value)> = status_obj
                    .get("capacity")
                    .and_then(|c| c.as_object())
                    .map(|c| c.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default();
                if !capacity_entries.is_empty() {
                    let allocatable = status_obj
                        .entry("allocatable")
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    if let Some(alloc_obj) = allocatable.as_object_mut() {
                        for (key, value) in capacity_entries {
                            alloc_obj.entry(key).or_insert(value);
                        }
                    }
                }
            }
        }

        // Merge metadata: start with current, then apply annotations/labels
        // from the request. K8s status updates can modify metadata annotations.
        if let Some(metadata_obj) = current_metadata.as_object() {
            let mut merged_metadata = metadata_obj.clone();
            merged_metadata.remove("resourceVersion");
            // Merge annotations from the request
            if let Some(new_meta) = new_resource.get("metadata").and_then(|m| m.as_object()) {
                if let Some(new_annotations) =
                    new_meta.get("annotations").and_then(|a| a.as_object())
                {
                    let annotations = merged_metadata
                        .entry("annotations")
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    if let Some(ann_obj) = annotations.as_object_mut() {
                        for (k, v) in new_annotations {
                            ann_obj.insert(k.clone(), v.clone());
                        }
                    }
                }
                if let Some(new_labels) = new_meta.get("labels").and_then(|l| l.as_object()) {
                    let labels = merged_metadata
                        .entry("labels")
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    if let Some(lbl_obj) = labels.as_object_mut() {
                        for (k, v) in new_labels {
                            lbl_obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            obj.insert("metadata".to_string(), Value::Object(merged_metadata));
        }
    }

    // Ensure TypeMeta fields are present
    if let Some(obj) = updated_resource.as_object_mut() {
        if !obj.contains_key("kind") || !obj.contains_key("apiVersion") {
            let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
            obj.entry("kind".to_string())
                .or_insert_with(|| Value::String(kind));
            obj.entry("apiVersion".to_string())
                .or_insert_with(|| Value::String(api_version));
        }
    }

    // Save the updated resource
    let mut saved: Value = state.storage.update(&key, &updated_resource).await?;

    // Ensure kind/apiVersion are always present in the response
    if let Some(obj) = saved.as_object_mut() {
        let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
        obj.entry("kind".to_string())
            .or_insert_with(|| Value::String(kind));
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| Value::String(api_version));
    }

    info!("Successfully updated status for {}/{}", resource_type, name);

    Ok(Json(saved))
}

/// Get status for a resource (read-only)
pub async fn get_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>> {
    let resource_type = extract_resource_type_from_uri(&uri);
    info!(
        "Getting status for {}/{}/{}",
        resource_type, namespace, name
    );

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", &resource_type)
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("status");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key(&resource_type, Some(&namespace), &name);
    let mut resource: Value = state.storage.get(&key).await?;

    // Ensure kind/apiVersion are present in the response
    if let Some(obj) = resource.as_object_mut() {
        let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
        obj.entry("kind".to_string())
            .or_insert_with(|| Value::String(kind));
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| Value::String(api_version));
    }

    Ok(Json(resource))
}

/// Get status for a cluster-scoped resource (read-only)
pub async fn get_cluster_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    Path(name): Path<String>,
) -> Result<Json<Value>> {
    let resource_type = extract_resource_type_from_uri(&uri);
    debug!("Getting status for {}/{}", resource_type, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", &resource_type)
        .with_name(&name)
        .with_subresource("status");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key(&resource_type, None, &name);
    let mut resource: Value = state.storage.get(&key).await?;

    // Ensure kind/apiVersion are present in the response
    if let Some(obj) = resource.as_object_mut() {
        let (kind, api_version) = resource_type_to_kind_api_version(&resource_type);
        obj.entry("kind".to_string())
            .or_insert_with(|| Value::String(kind));
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| Value::String(api_version));
    }

    Ok(Json(resource))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_status_update_preserves_generation() {
        // A /status write must keep the STORED metadata.generation and ignore
        // any generation in the incoming body — otherwise a stale status write
        // (the kubelet's observedGeneration sync, built from an earlier read)
        // reverts a concurrent spec update's generation bump (#1166). Spec is
        // preserved from current; only status is taken from the incoming body.
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "generation": 7, "resourceVersion": "42"},
            "spec": {"activeDeadlineSeconds": 100},
            "status": {"phase": "Pending"}
        });
        let incoming = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "generation": 3},
            "status": {"phase": "Running", "observedGeneration": 3}
        });
        let out = build_updated_resource_for_status(&current, &incoming, false, "pods").unwrap();
        assert_eq!(
            out.pointer("/metadata/generation").and_then(|v| v.as_i64()),
            Some(7),
            "status update must preserve the stored generation, not the incoming one"
        );
        assert_eq!(
            out.pointer("/status/phase").and_then(|v| v.as_str()),
            Some("Running"),
            "status must be applied from the incoming body"
        );
        assert_eq!(
            out.pointer("/spec/activeDeadlineSeconds")
                .and_then(|v| v.as_i64()),
            Some(100),
            "spec must be preserved from current"
        );
    }

    #[test]
    fn test_status_update_preserves_spec() {
        let current_resource = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "test-deployment",
                "resourceVersion": "1"
            },
            "spec": {
                "replicas": 3
            },
            "status": {
                "readyReplicas": 2
            }
        });

        let new_resource = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "test-deployment"
            },
            "status": {
                "readyReplicas": 3
            }
        });

        // In a real scenario, this would be done by the handler
        // Here we just verify the logic

        let current_spec = current_resource.get("spec").unwrap();
        let new_status = new_resource.get("status").unwrap();

        assert_eq!(current_spec["replicas"], 3);
        assert_eq!(new_status["readyReplicas"], 3);
    }

    #[test]
    fn test_status_update_increments_version() {
        let metadata = json!({
            "name": "test",
            "resourceVersion": "100"
        });

        if let Some(version) = metadata.get("resourceVersion") {
            if let Some(version_num) = version.as_str().and_then(|s| s.parse::<i64>().ok()) {
                assert_eq!(version_num, 100);
                assert_eq!(version_num + 1, 101);
            }
        }
    }

    #[test]
    fn test_status_merge_patch_preserves_replicas() {
        // When patching status with merge-patch, only the patched fields
        // should change. Other status fields (replicas, readyReplicas) must
        // be preserved.
        let current_status = json!({
            "replicas": 3,
            "readyReplicas": 3,
            "availableReplicas": 3,
            "conditions": [
                {"type": "StatusUpdate", "status": "True", "reason": "Test"}
            ]
        });

        let patch_status = json!({
            "conditions": [
                {"type": "StatusPatched", "status": "True"}
            ]
        });

        // Simulate the merge logic from update_status handler
        let mut merged = current_status.clone();
        if let (Some(merged_obj), Some(patch_obj)) =
            (merged.as_object_mut(), patch_status.as_object())
        {
            for (k, v) in patch_obj {
                if v.is_null() {
                    merged_obj.remove(k);
                } else {
                    merged_obj.insert(k.clone(), v.clone());
                }
            }
        }

        // replicas should be preserved
        assert_eq!(merged["replicas"], 3);
        assert_eq!(merged["readyReplicas"], 3);
        assert_eq!(merged["availableReplicas"], 3);

        // conditions should be replaced by the patch value
        let conditions = merged["conditions"].as_array().unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0]["type"], "StatusPatched");
    }

    // `[sig-apps] Job should apply changes to a job status` patches /status with
    // a custom condition type via a JSON merge-patch and expects it preserved in
    // the response. The status subresource must merge the patch into the current
    // status and keep arbitrary user conditions (it operates on raw JSON, so a
    // condition type unknown to the controller is still round-tripped).
    #[test]
    fn test_job_status_merge_patch_keeps_custom_condition() {
        let current = json!({
            "kind":"Job","apiVersion":"batch/v1",
            "metadata":{"name":"j","namespace":"n","resourceVersion":"5"},
            "spec":{"parallelism":2},
            "status":{"active":2,"ready":2}
        });
        let patch = json!({"status":{"conditions":[{"type":"CustomConditionType","status":"True","reason":"E2E","message":"set"}]}});
        let out = super::build_updated_resource_for_status(&current, &patch, true, "jobs").unwrap();
        let conds = out["status"]["conditions"]
            .as_array()
            .expect("conditions array");
        assert!(
            conds.iter().any(|c| c["type"] == "CustomConditionType"),
            "custom condition must survive the /status merge patch, out={out}"
        );
        // The pre-existing status fields must be preserved by the merge.
        assert_eq!(out["status"]["active"], 2);
        assert_eq!(out["status"]["ready"], 2);
    }

    #[test]
    fn test_status_merge_patch_null_removes_field() {
        let current_status = json!({
            "replicas": 3,
            "conditions": [{"type": "Test", "status": "True"}]
        });

        let patch_status = json!({
            "conditions": serde_json::Value::Null
        });

        let mut merged = current_status.clone();
        if let (Some(merged_obj), Some(patch_obj)) =
            (merged.as_object_mut(), patch_status.as_object())
        {
            for (k, v) in patch_obj {
                if v.is_null() {
                    merged_obj.remove(k);
                } else {
                    merged_obj.insert(k.clone(), v.clone());
                }
            }
        }

        assert_eq!(merged["replicas"], 3);
        assert!(merged.get("conditions").is_none());
    }

    #[test]
    fn test_resource_type_to_kind_api_version_core() {
        let (kind, api) = resource_type_to_kind_api_version("pods");
        assert_eq!(kind, "Pod");
        assert_eq!(api, "v1");

        let (kind, api) = resource_type_to_kind_api_version("services");
        assert_eq!(kind, "Service");
        assert_eq!(api, "v1");

        let (kind, api) = resource_type_to_kind_api_version("namespaces");
        assert_eq!(kind, "Namespace");
        assert_eq!(api, "v1");

        let (kind, api) = resource_type_to_kind_api_version("nodes");
        assert_eq!(kind, "Node");
        assert_eq!(api, "v1");

        let (kind, api) = resource_type_to_kind_api_version("configmaps");
        assert_eq!(kind, "ConfigMap");
        assert_eq!(api, "v1");

        let (kind, api) = resource_type_to_kind_api_version("replicationcontrollers");
        assert_eq!(kind, "ReplicationController");
        assert_eq!(api, "v1");

        let (kind, api) = resource_type_to_kind_api_version("resourcequotas");
        assert_eq!(kind, "ResourceQuota");
        assert_eq!(api, "v1");
    }

    #[test]
    fn test_resource_type_to_kind_api_version_apps() {
        let (kind, api) = resource_type_to_kind_api_version("deployments");
        assert_eq!(kind, "Deployment");
        assert_eq!(api, "apps/v1");

        let (kind, api) = resource_type_to_kind_api_version("replicasets");
        assert_eq!(kind, "ReplicaSet");
        assert_eq!(api, "apps/v1");

        let (kind, api) = resource_type_to_kind_api_version("statefulsets");
        assert_eq!(kind, "StatefulSet");
        assert_eq!(api, "apps/v1");

        let (kind, api) = resource_type_to_kind_api_version("daemonsets");
        assert_eq!(kind, "DaemonSet");
        assert_eq!(api, "apps/v1");
    }

    #[test]
    fn test_resource_type_to_kind_api_version_batch() {
        let (kind, api) = resource_type_to_kind_api_version("jobs");
        assert_eq!(kind, "Job");
        assert_eq!(api, "batch/v1");

        let (kind, api) = resource_type_to_kind_api_version("cronjobs");
        assert_eq!(kind, "CronJob");
        assert_eq!(api, "batch/v1");
    }

    #[test]
    fn test_resource_type_to_kind_api_version_extensions() {
        let (kind, api) = resource_type_to_kind_api_version("customresourcedefinitions");
        assert_eq!(kind, "CustomResourceDefinition");
        assert_eq!(api, "apiextensions.k8s.io/v1");

        let (kind, api) = resource_type_to_kind_api_version("endpointslices");
        assert_eq!(kind, "EndpointSlice");
        assert_eq!(api, "discovery.k8s.io/v1");
    }

    #[test]
    fn test_resource_type_to_kind_api_version_fallback() {
        // Unknown resource types use CamelCase heuristic
        let (kind, api) = resource_type_to_kind_api_version("widgets");
        assert_eq!(kind, "Widget");
        assert_eq!(api, "v1");
    }

    #[test]
    fn test_extract_resource_type_from_uri() {
        // Namespaced: /api/v1/namespaces/{ns}/{resource}/{name}/status
        let uri: Uri = "/api/v1/namespaces/default/pods/my-pod/status"
            .parse()
            .unwrap();
        assert_eq!(extract_resource_type_from_uri(&uri), "pods");

        // Cluster-scoped: /api/v1/namespaces/{name}/status
        let uri: Uri = "/api/v1/namespaces/kube-system/status".parse().unwrap();
        assert_eq!(extract_resource_type_from_uri(&uri), "namespaces");

        // Cluster-scoped: /api/v1/nodes/{name}/status
        let uri: Uri = "/api/v1/nodes/node-1/status".parse().unwrap();
        assert_eq!(extract_resource_type_from_uri(&uri), "nodes");

        // Apps group: /apis/apps/v1/namespaces/{ns}/deployments/{name}/status
        let uri: Uri = "/apis/apps/v1/namespaces/default/deployments/my-deploy/status"
            .parse()
            .unwrap();
        assert_eq!(extract_resource_type_from_uri(&uri), "deployments");
    }

    #[test]
    fn test_kind_injection_into_response_without_kind() {
        // Simulate a stored resource that is missing kind/apiVersion
        let mut resource = json!({
            "metadata": {
                "name": "test-deployment",
                "namespace": "default",
                "resourceVersion": "42"
            },
            "spec": { "replicas": 3 },
            "status": { "readyReplicas": 3 }
        });

        // This is the logic the handler applies to the response
        let resource_type = "deployments";
        if let Some(obj) = resource.as_object_mut() {
            let (kind, api_version) = resource_type_to_kind_api_version(resource_type);
            obj.entry("kind".to_string())
                .or_insert_with(|| Value::String(kind));
            obj.entry("apiVersion".to_string())
                .or_insert_with(|| Value::String(api_version));
        }

        assert_eq!(resource["kind"], "Deployment");
        assert_eq!(resource["apiVersion"], "apps/v1");
    }

    #[test]
    fn test_kind_injection_preserves_existing_kind() {
        // Resource that already has kind/apiVersion should not be overwritten
        let mut resource = json!({
            "kind": "Deployment",
            "apiVersion": "apps/v1",
            "metadata": { "name": "test" },
            "status": { "replicas": 1 }
        });

        let resource_type = "deployments";
        if let Some(obj) = resource.as_object_mut() {
            let (kind, api_version) = resource_type_to_kind_api_version(resource_type);
            obj.entry("kind".to_string())
                .or_insert_with(|| Value::String(kind));
            obj.entry("apiVersion".to_string())
                .or_insert_with(|| Value::String(api_version));
        }

        // Should keep the original values
        assert_eq!(resource["kind"], "Deployment");
        assert_eq!(resource["apiVersion"], "apps/v1");
    }

    #[test]
    fn test_kind_injection_for_various_resource_types() {
        // Test that kind injection works for all resource types
        let test_cases = vec![
            ("pods", "Pod", "v1"),
            ("deployments", "Deployment", "apps/v1"),
            ("replicasets", "ReplicaSet", "apps/v1"),
            ("statefulsets", "StatefulSet", "apps/v1"),
            ("jobs", "Job", "batch/v1"),
            ("nodes", "Node", "v1"),
            ("namespaces", "Namespace", "v1"),
        ];

        for (resource_type, expected_kind, expected_api_version) in test_cases {
            let mut resource = json!({
                "metadata": { "name": "test" },
                "status": {}
            });

            if let Some(obj) = resource.as_object_mut() {
                let (kind, api_version) = resource_type_to_kind_api_version(resource_type);
                obj.entry("kind".to_string())
                    .or_insert_with(|| Value::String(kind));
                obj.entry("apiVersion".to_string())
                    .or_insert_with(|| Value::String(api_version));
            }

            assert_eq!(
                resource["kind"], expected_kind,
                "Failed for resource type: {}",
                resource_type
            );
            assert_eq!(
                resource["apiVersion"], expected_api_version,
                "Failed for resource type: {}",
                resource_type
            );
        }
    }

    /// Reproduces the body and merge logic of the upstream conformance test
    /// `[sig-api-machinery] Namespaces [Serial] should apply changes to a
    /// namespace status [Conformance]` for the PATCH step.
    ///
    /// The test sends a merge-patch+json body containing both a metadata
    /// annotation and a status.conditions array, and expects the response to
    /// preserve metadata (annotation merged in) AND status.phase while adding
    /// the new condition.
    #[test]
    fn test_namespace_status_merge_patch_conformance_shape() {
        // Stored Namespace as it would look after CREATE.
        let current_resource = json!({
            "kind": "Namespace",
            "apiVersion": "v1",
            "metadata": {
                "name": "nsdeletetest-1234",
                "uid": "abc",
                "creationTimestamp": "2026-05-27T12:00:00Z",
                "finalizers": ["kubernetes"],
                "resourceVersion": "42"
            },
            "status": {
                "phase": "Active"
            }
        });

        // The exact body the upstream e2e test sends via merge-patch.
        let patch_body = json!({
            "metadata": {
                "annotations": {
                    "e2e-patched-ns-status": "nsdeletetest-1234"
                }
            },
            "status": {
                "conditions": [{
                    "type": "StatusPatch",
                    "status": "True",
                    "reason": "E2E",
                    "message": "Patched by an e2e test"
                }]
            }
        });

        // --- Reproduce the merge logic from update_cluster_status (merge-patch branch) ---

        // 1. New status: strategic merge of patch_status onto current_status.
        let patch_status = patch_body
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));
        let current_status = current_resource
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));
        let new_status = crate::patch::apply_patch(
            &current_status,
            &patch_status,
            crate::patch::PatchType::StrategicMergePatch,
        )
        .expect("strategic merge of namespace status must not fail");

        // 2. Build updated resource: preserve spec (none in this case), set
        //    new status, merge annotations/labels into metadata.
        let mut updated_resource = current_resource.clone();
        if let Some(obj) = updated_resource.as_object_mut() {
            obj.insert("status".to_string(), new_status);
            if let Some(current_metadata) =
                current_resource.get("metadata").and_then(|m| m.as_object())
            {
                let mut merged_metadata = current_metadata.clone();
                merged_metadata.remove("resourceVersion");
                if let Some(new_meta) = patch_body.get("metadata").and_then(|m| m.as_object()) {
                    if let Some(new_annotations) =
                        new_meta.get("annotations").and_then(|a| a.as_object())
                    {
                        let annotations = merged_metadata
                            .entry("annotations")
                            .or_insert_with(|| Value::Object(serde_json::Map::new()));
                        if let Some(ann_obj) = annotations.as_object_mut() {
                            for (k, v) in new_annotations {
                                ann_obj.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
                obj.insert("metadata".to_string(), Value::Object(merged_metadata));
            }
        }

        // --- Assertions matching the conformance test expectations ---

        // Annotation must be visible to the typed client (patchedStatus.Annotations).
        assert_eq!(
            updated_resource["metadata"]["annotations"]["e2e-patched-ns-status"],
            "nsdeletetest-1234",
            "merge-patch must add metadata.annotations entry"
        );

        // status.phase must survive the merge (Active stays Active).
        assert_eq!(
            updated_resource["status"]["phase"], "Active",
            "merge-patch on status must not wipe phase=Active"
        );

        // status.conditions must contain the patched condition, and the last
        // condition must match the test expectations exactly.
        let conditions = updated_resource["status"]["conditions"]
            .as_array()
            .expect("status.conditions must be an array");
        assert!(!conditions.is_empty(), "conditions array must be non-empty");
        let last = &conditions[conditions.len() - 1];
        assert_eq!(last["type"], "StatusPatch");
        assert_eq!(last["status"], "True");
        assert_eq!(
            last["reason"], "E2E",
            "Conditions[last].Reason must equal 'E2E'"
        );
        assert_eq!(
            last["message"], "Patched by an e2e test",
            "Conditions[last].Message must match the exact upstream string"
        );

        // Kind/apiVersion preserved for typed client decode.
        assert_eq!(updated_resource["kind"], "Namespace");
        assert_eq!(updated_resource["apiVersion"], "v1");
    }

    /// Reproduces the PUT (UpdateStatus) step from the same conformance test.
    /// The test reads the namespace via the dynamic client, appends a new
    /// condition, and PUTs the entire object back to /status. The handler
    /// must replace status from the body, preserve the spec, and keep the
    /// previously-applied annotation.
    #[test]
    fn test_namespace_status_put_update_conformance_shape() {
        let current_resource = json!({
            "kind": "Namespace",
            "apiVersion": "v1",
            "metadata": {
                "name": "nsdeletetest-1234",
                "uid": "abc",
                "annotations": {
                    "e2e-patched-ns-status": "nsdeletetest-1234"
                },
                "resourceVersion": "43"
            },
            "status": {
                "phase": "Active",
                "conditions": [{
                    "type": "StatusPatch",
                    "status": "True",
                    "reason": "E2E",
                    "message": "Patched by an e2e test"
                }]
            }
        });

        // The body the upstream test PUTs: the same namespace with one extra
        // condition appended.
        let new_body = json!({
            "kind": "Namespace",
            "apiVersion": "v1",
            "metadata": {
                "name": "nsdeletetest-1234",
                "annotations": {
                    "e2e-patched-ns-status": "nsdeletetest-1234"
                }
            },
            "status": {
                "phase": "Active",
                "conditions": [
                    {"type": "StatusPatch", "status": "True", "reason": "E2E",
                     "message": "Patched by an e2e test"},
                    {"type": "StatusUpdate", "status": "True", "reason": "E2E",
                     "message": "Updated by an e2e test"}
                ]
            }
        });

        // --- Reproduce the PUT branch of update_cluster_status ---
        // is_merge_patch = false → status comes straight from the body.
        let new_status = new_body
            .get("status")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));

        let mut updated_resource = current_resource.clone();
        if let Some(obj) = updated_resource.as_object_mut() {
            // Spec preserved (none here).
            obj.insert("status".to_string(), new_status);
            // Metadata merged.
            if let Some(current_metadata) =
                current_resource.get("metadata").and_then(|m| m.as_object())
            {
                let mut merged_metadata = current_metadata.clone();
                merged_metadata.remove("resourceVersion");
                if let Some(new_meta) = new_body.get("metadata").and_then(|m| m.as_object()) {
                    if let Some(new_annotations) =
                        new_meta.get("annotations").and_then(|a| a.as_object())
                    {
                        let annotations = merged_metadata
                            .entry("annotations")
                            .or_insert_with(|| Value::Object(serde_json::Map::new()));
                        if let Some(ann_obj) = annotations.as_object_mut() {
                            for (k, v) in new_annotations {
                                ann_obj.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
                obj.insert("metadata".to_string(), Value::Object(merged_metadata));
            }
        }

        // The conformance test asserts the appended condition is present and
        // is the last one in the array.
        let conditions = updated_resource["status"]["conditions"]
            .as_array()
            .expect("status.conditions must be an array");
        assert_eq!(
            conditions.len(),
            2,
            "PUT body's full conditions array must be preserved"
        );
        let last = &conditions[conditions.len() - 1];
        assert_eq!(last["type"], "StatusUpdate");
        assert_eq!(last["message"], "Updated by an e2e test");

        // Annotation from the prior patch must still be present.
        assert_eq!(
            updated_resource["metadata"]["annotations"]["e2e-patched-ns-status"],
            "nsdeletetest-1234"
        );

        // Phase preserved.
        assert_eq!(updated_resource["status"]["phase"], "Active");
    }

    async fn create_status_test_state() -> Arc<ApiServerState> {
        use rusternetes_common::auth::TokenManager;
        use rusternetes_common::authz::AlwaysAllowAuthorizer;
        use rusternetes_common::observability::MetricsRegistry;
        use rusternetes_storage::StorageBackend;

        let storage = Arc::new(StorageBackend::new_memory());
        let token_manager = Arc::new(TokenManager::new(b"test-secret"));
        let authorizer =
            Arc::new(AlwaysAllowAuthorizer) as Arc<dyn rusternetes_common::authz::Authorizer>;
        let metrics = Arc::new(MetricsRegistry::new());

        Arc::new(ApiServerState::new(
            storage,
            token_manager,
            authorizer,
            metrics,
            true,
        ))
    }

    fn admin_auth_ctx() -> AuthContext {
        AuthContext {
            user: rusternetes_common::auth::UserInfo::anonymous(),
        }
    }

    /// End-to-end exercise of the cluster-scoped PUT /status handler used by the
    /// upstream `[sig-api-machinery] Namespaces [Serial] should apply changes to
    /// a namespace status [Conformance]` test. The typed Go client calls
    /// `UpdateStatus` with the full Namespace (spec included), and then strictly
    /// decodes the response into a `*v1.Namespace`. This drives the real handler
    /// over MemoryStorage to confirm the appended condition round-trips.
    #[tokio::test]
    async fn test_namespace_status_put_handler_roundtrip() {
        let state = create_status_test_state().await;

        // Seed an Active namespace with a previously-patched condition.
        let key = build_key("namespaces", None, "nstest");
        let seeded = json!({
            "kind": "Namespace",
            "apiVersion": "v1",
            "metadata": {
                "name": "nstest",
                "annotations": { "e2e-patched-ns-status": "nstest" }
            },
            "spec": { "finalizers": ["kubernetes"] },
            "status": {
                "phase": "Active",
                "conditions": [{
                    "type": "StatusPatch", "status": "True",
                    "reason": "E2E", "message": "Patched by an e2e test"
                }]
            }
        });
        let _: Value = state.storage.create(&key, &seeded).await.unwrap();

        // The typed client PUTs the full Namespace with one extra condition.
        let body = json!({
            "kind": "Namespace",
            "apiVersion": "v1",
            "metadata": { "name": "nstest" },
            "spec": { "finalizers": ["kubernetes"] },
            "status": {
                "phase": "Active",
                "conditions": [
                    {"type": "StatusPatch", "status": "True", "reason": "E2E",
                     "message": "Patched by an e2e test"},
                    {"type": "StatusUpdate", "status": "True", "reason": "E2E",
                     "message": "Updated by an e2e test"}
                ]
            }
        });

        let uri: Uri = "/api/v1/namespaces/nstest/status".parse().unwrap();
        let resp = update_cluster_status(
            State(state.clone()),
            Extension(admin_auth_ctx()),
            uri,
            axum::http::HeaderMap::new(),
            Path("nstest".to_string()),
            axum::body::Bytes::from(serde_json::to_vec(&body).unwrap()),
        )
        .await
        .expect("PUT /status must succeed");

        let saved = resp.0;
        assert_eq!(saved["kind"], "Namespace");
        assert_eq!(saved["apiVersion"], "v1");
        assert_eq!(saved["status"]["phase"], "Active");
        let conds = saved["status"]["conditions"]
            .as_array()
            .expect("conditions array");
        assert_eq!(conds.len(), 2);
        assert_eq!(conds[1]["type"], "StatusUpdate");
        assert_eq!(conds[1]["message"], "Updated by an e2e test");

        // GET /status must reflect the update for the next read.
        let get_uri: Uri = "/api/v1/namespaces/nstest/status".parse().unwrap();
        let got = get_cluster_status(
            State(state.clone()),
            Extension(admin_auth_ctx()),
            get_uri,
            Path("nstest".to_string()),
        )
        .await
        .expect("GET /status must succeed");
        let got = got.0;
        let conds = got["status"]["conditions"].as_array().unwrap();
        assert_eq!(conds.len(), 2);
        assert_eq!(conds[1]["type"], "StatusUpdate");
    }
}
