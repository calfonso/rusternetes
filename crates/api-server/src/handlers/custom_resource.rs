//! Custom Resource (CR) API handlers for dynamically created CRDs
//!
//! This module handles CRUD operations for custom resources defined by CRDs.

#![allow(dead_code)]

use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{CustomResource, CustomResourceDefinition},
    schema_validation::SchemaValidator,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Create a new custom resource instance
pub async fn create_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace)): Path<(String, String, String, Option<String>)>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<(StatusCode, Json<CustomResource>)> {
    // Parse the body manually so we can do strict field validation against the raw bytes
    let mut cr: CustomResource = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;

    let cr_name = cr.metadata.name.clone();
    info!(
        "Creating custom resource {}/{}/{}: {}",
        group, version, plural, cr_name
    );

    // Look up the CRD first — we need it to check preserve-unknown-fields
    // before strict validation. CRDs with preserve-unknown-fields allow
    // arbitrary top-level fields even with fieldValidation=Strict.
    let crd_name_for_lookup = format!("{}.{}", plural, group);
    let crd_for_validation = get_crd_for_resource(&state, &crd_name_for_lookup).await?;
    let crd_preserves = crd_for_validation.spec.preserve_unknown_fields == Some(true);
    let schema_preserves = crd_for_validation
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .and_then(|v| v.schema.as_ref())
        .map(|s| s.open_apiv3_schema.x_kubernetes_preserve_unknown_fields == Some(true))
        .unwrap_or(false);

    // Strict field validation for CRDs:
    // K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/customresource_handler.go
    if params.get("fieldValidation").map(|v| v.as_str()) == Some("Strict") {
        // Check unknown top-level fields — but SKIP if CRD preserves unknown fields
        if !cr.extra.is_empty() && !crd_preserves && !schema_preserves {
            let unknown: Vec<&String> = cr.extra.keys().collect();
            return Err(rusternetes_common::Error::InvalidResource(format!(
                "strict decoding error: unknown field \"{}\"",
                unknown[0]
            )));
        }
        // Check unknown metadata fields — K8s validates ObjectMeta strictly,
        // BOTH at root AND in embedded objects (x-kubernetes-embedded-resource).
        // K8s ref: apiextensions-apiserver/pkg/apiserver/schema/objectmeta/validation.go
        if let Ok(body_json) = serde_json::from_slice::<serde_json::Value>(&body) {
            const KNOWN_META: &[&str] = &[
                "name",
                "generateName",
                "namespace",
                "selfLink",
                "uid",
                "resourceVersion",
                "generation",
                "creationTimestamp",
                "deletionTimestamp",
                "deletionGracePeriodSeconds",
                "labels",
                "annotations",
                "ownerReferences",
                "finalizers",
                "managedFields",
                "clusterName",
            ];

            // Recursively find all "metadata" objects in the body and validate them
            fn check_metadata_fields(
                value: &serde_json::Value,
                path: &str,
                known: &[&str],
            ) -> Option<String> {
                if let Some(obj) = value.as_object() {
                    // Check if this object has a "metadata" field
                    if let Some(meta) = obj.get("metadata").and_then(|m| m.as_object()) {
                        let meta_path = if path.is_empty() {
                            ".metadata".to_string()
                        } else {
                            format!("{}.metadata", path)
                        };
                        for key in meta.keys() {
                            if !known.contains(&key.as_str()) {
                                return Some(format!(
                                    "{}.{}: field not declared in schema",
                                    meta_path, key
                                ));
                            }
                        }
                    }
                    // Recurse into all nested objects
                    for (key, val) in obj {
                        if key == "metadata" {
                            continue; // Already checked above
                        }
                        let child_path = if path.is_empty() {
                            format!(".{}", key)
                        } else {
                            format!("{}.{}", path, key)
                        };
                        if let Some(err) = check_metadata_fields(val, &child_path, known) {
                            return Some(err);
                        }
                    }
                } else if let Some(arr) = value.as_array() {
                    for item in arr {
                        if let Some(err) = check_metadata_fields(item, path, known) {
                            return Some(err);
                        }
                    }
                }
                None
            }

            if let Some(err_msg) = check_metadata_fields(&body_json, "", KNOWN_META) {
                return Err(rusternetes_common::Error::InvalidResource(err_msg));
            }
        }
    }
    crate::handlers::validation::validate_strict_fields(&params, &body, &cr)?;

    // Use the CRD we already looked up for preserve-unknown-fields check
    let crd = crd_for_validation;

    // Strict schema validation for nested unknown fields.
    // When fieldValidation=Strict, validate nested fields against the CRD schema
    // and reject unknown fields with K8s-format errors.
    // K8s ref: apiextensions-apiserver/pkg/apiserver/schema/pruning — PruneWithOptions
    if params.get("fieldValidation").map(|v| v.as_str()) == Some("Strict")
        && !crd_preserves
        && !schema_preserves
    {
        if let Some(crd_version) = crd.spec.versions.iter().find(|v| v.name == version) {
            if let Some(ref validation) = crd_version.schema {
                if let Some(ref properties) = validation.open_apiv3_schema.properties {
                    if let Some(spec_schema) = properties.get("spec") {
                        if let Some(ref spec) = cr.spec {
                            SchemaValidator::validate_strict(spec_schema, spec, "spec")?;
                        }
                    }
                }
            }
        }
    }

    // Apply schema defaults before validation
    apply_schema_defaults(&crd, &version, &mut cr);

    // Validate the resource against CRD schema
    validate_custom_resource(&crd, &version, &cr)?;

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "create", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
    } else {
        RequestAttributes::new(auth_ctx.user.clone(), "create", &plural).with_api_group(&group)
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure metadata fields
    cr.metadata.ensure_uid();
    cr.metadata.ensure_creation_timestamp();

    // Set API version and kind
    let kind = crd.spec.names.kind.clone();
    cr.api_version = format!("{}/{}", group, version);
    cr.kind = kind.clone();

    // Run admission webhooks (mutating + validating) for custom resources
    // K8s runs webhooks for ALL resource types including CRDs
    let cr_is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    {
        let gvk = rusternetes_common::admission::GroupVersionKind {
            group: group.clone(),
            version: version.clone(),
            kind: kind.clone(),
        };
        let gvr = rusternetes_common::admission::GroupVersionResource {
            group: group.clone(),
            version: version.clone(),
            resource: plural.clone(),
        };
        let user_info = rusternetes_common::admission::UserInfo {
            username: auth_ctx.user.username.clone(),
            uid: auth_ctx.user.uid.clone(),
            groups: auth_ctx.user.groups.clone(),
        };
        let cr_val = serde_json::to_value(&cr).ok();
        // Run mutating webhooks
        let (mutation_response, mutated_obj) = state
            .webhook_manager
            .run_mutating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Create,
                &gvk,
                &gvr,
                namespace.as_deref(),
                &cr_name,
                cr_val.clone(),
                None,
                &user_info,
                cr_is_dry_run,
            )
            .await?;
        // K8s mutating webhooks CAN deny — enforce the denial.
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = &mutation_response {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
        if let Some(mutated) = mutated_obj {
            if let Ok(m) = serde_json::from_value::<CustomResource>(mutated) {
                cr = m;
            }
        }
        // Run validating webhooks
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Create,
                &gvk,
                &gvr,
                namespace.as_deref(),
                &cr_name,
                serde_json::to_value(&cr).ok(),
                None,
                &user_info,
                cr_is_dry_run,
            )
            .await?
        {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    // K8s structural pruning: remove unknown fields from the CR based on
    // the CRD schema, unless x-kubernetes-preserve-unknown-fields is set.
    // This happens AFTER webhook mutation so webhook-added fields not in
    // the schema are pruned. K8s ref: apiextensions-apiserver/pkg/apiserver/schema/pruning
    prune_custom_resource(&crd, &version, &mut cr);

    // Check for dry-run
    if cr_is_dry_run {
        return Ok((StatusCode::OK, Json(cr)));
    }

    // Build storage key
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &cr_name)
    } else {
        build_key(&resource_type, None, &cr_name)
    };

    let created = state.storage.create(&key, &cr).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

/// Get a specific custom resource instance
pub async fn get_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
) -> Result<Json<CustomResource>> {
    info!(
        "Getting custom resource {}/{}/{}: {}",
        group, version, plural, name
    );

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "get", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
            .with_name(&name)
    } else {
        RequestAttributes::new(auth_ctx.user, "get", &plural)
            .with_api_group(&group)
            .with_name(&name)
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Build storage key
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &name)
    } else {
        build_key(&resource_type, None, &name)
    };

    let mut cr: CustomResource = state.storage.get(&key).await?;

    // Apply schema defaults on read (K8s "defaulting on read")
    apply_schema_defaults(&crd, &version, &mut cr);

    // Convert the CR if the request version differs from the stored version.
    // For `Webhook` strategy this round-trips through the configured webhook;
    // for the default `None` strategy this just rewrites `apiVersion`.
    cr = crate::conversion::convert_custom_resource(&crd, cr, &version).await?;

    Ok(Json(cr))
}

/// List custom resource instances
pub async fn list_custom_resources(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace)): Path<(String, String, String, Option<String>)>,
) -> Result<Json<List<CustomResource>>> {
    debug!("Listing custom resources {}/{}/{}", group, version, plural);

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "list", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
    } else {
        RequestAttributes::new(auth_ctx.user, "list", &plural).with_api_group(&group)
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Build storage prefix
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let prefix = if let Some(ref ns) = namespace {
        build_prefix(&resource_type, Some(ns))
    } else {
        build_prefix(&resource_type, None)
    };

    let mut crs: Vec<CustomResource> = state.storage.list(&prefix).await?;

    // Apply schema defaults on read (K8s "defaulting on read")
    for cr in &mut crs {
        apply_schema_defaults(&crd, &version, cr);
    }

    // Convert any CR whose stored version differs from the request version.
    // `convert_custom_resources` batches Webhook calls into a single
    // ConversionReview round-trip (mirrors upstream non-homogeneous list path).
    crs = crate::conversion::convert_custom_resources(&crd, crs, &version).await?;

    let list = List::new("List", "v1", crs);
    Ok(Json(list))
}

/// Update a custom resource instance
pub async fn update_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<CustomResource>> {
    // Parse the body manually so we can do strict field validation against the raw bytes
    let mut cr: CustomResource = serde_json::from_slice(&body).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;

    info!(
        "Updating custom resource {}/{}/{}: {}",
        group, version, plural, name
    );

    // Strict field validation: reject unknown top-level fields for CRDs
    let is_strict = params.get("fieldValidation").map(|v| v.as_str()) == Some("Strict");
    if is_strict && !cr.extra.is_empty() {
        let unknown: Vec<&String> = cr.extra.keys().collect();
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "strict decoding error: unknown field \"{}\"",
            unknown[0]
        )));
    }
    crate::handlers::validation::validate_strict_fields(&params, &body, &cr)?;

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Strict schema validation for nested unknown fields
    if is_strict {
        let crd_preserves = crd.spec.preserve_unknown_fields == Some(true);
        let schema_preserves = crd
            .spec
            .versions
            .iter()
            .find(|v| v.name == version)
            .and_then(|v| v.schema.as_ref())
            .map(|s| s.open_apiv3_schema.x_kubernetes_preserve_unknown_fields == Some(true))
            .unwrap_or(false);
        if !crd_preserves && !schema_preserves {
            if let Some(crd_version) = crd.spec.versions.iter().find(|v| v.name == version) {
                if let Some(ref validation) = crd_version.schema {
                    if let Some(ref properties) = validation.open_apiv3_schema.properties {
                        if let Some(spec_schema) = properties.get("spec") {
                            if let Some(ref spec) = cr.spec {
                                SchemaValidator::validate_strict(spec_schema, spec, "spec")?;
                            }
                        }
                    }
                }
            }
        }
    }

    // Apply schema defaults before validation
    apply_schema_defaults(&crd, &version, &mut cr);

    // Load the prior version so transition rules (`oldSelf`) can be evaluated.
    // Missing prior versions are not an error — the resource may be PUTting
    // a fresh object (which K8s allows as create-on-PUT for some clients).
    let resource_type_for_lookup = format!("{}_{}", group.replace('.', "_"), plural);
    let old_key = if let Some(ref ns) = namespace {
        build_key(&resource_type_for_lookup, Some(ns), &name)
    } else {
        build_key(&resource_type_for_lookup, None, &name)
    };
    let old_cr: Option<CustomResource> = state.storage.get(&old_key).await.ok();

    // Validate the resource against CRD schema + CEL rules
    validate_custom_resource_with_old(&crd, &version, &cr, old_cr.as_ref())?;

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "update", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
            .with_name(&name)
    } else {
        RequestAttributes::new(auth_ctx.user, "update", &plural)
            .with_api_group(&group)
            .with_name(&name)
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Ensure name matches
    cr.metadata.name = name.clone();
    cr.api_version = format!("{}/{}", group, version);
    cr.kind = crd.spec.names.kind.clone();

    let cr_is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Run validating webhooks for UPDATE operations.
    // K8s runs webhooks on all mutating operations (CREATE, UPDATE, DELETE),
    // and webhooks fire even for dry-run requests so deny decisions are honored.
    {
        use rusternetes_common::admission::{
            AdmissionResponse, GroupVersionKind, GroupVersionResource, Operation,
        };
        let gvk = GroupVersionKind {
            group: group.clone(),
            version: version.clone(),
            kind: cr.kind.clone(),
        };
        let gvr = GroupVersionResource {
            group: group.clone(),
            version: version.clone(),
            resource: plural.clone(),
        };
        let user_info = rusternetes_common::admission::UserInfo {
            username: "admin".to_string(),
            uid: "system:admin".to_string(),
            groups: vec!["system:masters".to_string()],
        };
        let cr_value = serde_json::to_value(&cr).ok();
        if let AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks_with_dryrun(
                &Operation::Update,
                &gvk,
                &gvr,
                namespace.as_deref(),
                &name,
                cr_value,
                None,
                &user_info,
                cr_is_dry_run,
            )
            .await?
        {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Skip persistence for dry-run after webhooks have run.
    if cr_is_dry_run {
        return Ok(Json(cr));
    }

    // Build storage key
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &name)
    } else {
        build_key(&resource_type, None, &name)
    };

    let updated = state.storage.update(&key, &cr).await?;

    Ok(Json(updated))
}

/// Patch a custom resource instance (JSON Patch, JSON Merge Patch, or Strategic Merge Patch)
pub async fn patch_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    req: axum::extract::Request,
) -> Result<Json<CustomResource>> {
    use axum::body::to_bytes;

    info!(
        "Patching custom resource {}/{}/{}: {}",
        group, version, plural, name
    );

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "patch", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
            .with_name(&name)
    } else {
        RequestAttributes::new(auth_ctx.user, "patch", &plural)
            .with_api_group(&group)
            .with_name(&name)
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the current resource
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &name)
    } else {
        build_key(&resource_type, None, &name)
    };

    // Get current resource (may not exist for server-side apply)
    let current_result: rusternetes_common::Result<CustomResource> = state.storage.get(&key).await;

    // Split request into parts to avoid borrow/move conflict
    let (parts, body) = req.into_parts();

    // Get Content-Type header to determine patch type
    // Check X-Original-Content-Type first (set by middleware when normalizing)
    let content_type = parts
        .headers
        .get("x-original-content-type")
        .or_else(|| parts.headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json-patch+json");

    // Read the patch body
    let body_bytes = to_bytes(body, usize::MAX).await.map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("Failed to read patch body: {}", e))
    })?;

    // Parse body as JSON or YAML depending on content type
    let patch_value: serde_json::Value = if content_type.contains("yaml") {
        // YAML body (server-side apply uses application/apply-patch+yaml)
        // Check for duplicate keys in strict mode (K8s uses Go yaml.v2 which
        // reports "key already set in map")
        let is_strict = parts
            .uri
            .query()
            .and_then(|q| {
                url::form_urlencoded::parse(q.as_bytes())
                    .find(|(k, _)| k == "fieldValidation")
                    .map(|(_, v)| v.to_string())
            })
            .as_deref()
            == Some("Strict");
        if is_strict {
            if let Ok(yaml_str) = std::str::from_utf8(&body_bytes) {
                // Simple duplicate key detection: parse YAML lines looking for
                // repeated keys at the same indentation level
                let mut seen_keys: std::collections::HashMap<(usize, String), usize> =
                    std::collections::HashMap::new();
                for (line_num, line) in yaml_str.lines().enumerate() {
                    let trimmed = line.trim_start();
                    if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('-') {
                        continue;
                    }
                    let indent = line.len() - trimmed.len();
                    if let Some(colon_pos) = trimmed.find(':') {
                        let key = trimmed[..colon_pos].trim().trim_matches('"');
                        if !key.is_empty() && !key.contains(' ') {
                            let map_key = (indent, key.to_string());
                            if let Some(_prev_line) = seen_keys.get(&map_key) {
                                return Err(rusternetes_common::Error::InvalidResource(format!(
                                    "line {}: key {:?} already set in map",
                                    line_num + 1,
                                    key
                                )));
                            }
                            // Clear keys at deeper indentation when we encounter a new key
                            seen_keys.retain(|(ind, _), _| *ind <= indent);
                            seen_keys.insert(map_key, line_num + 1);
                        }
                    }
                }
            }
        }
        serde_yaml::from_slice(&body_bytes).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid patch YAML: {}", e))
        })?
    } else {
        serde_json::from_slice(&body_bytes).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid patch JSON: {}", e))
        })?
    };

    // For server-side apply (application/apply-patch+yaml), create if not found
    let is_apply = content_type.contains("apply-patch");

    let patched_json = if let Ok(current) = &current_result {
        // Resource exists — apply patch
        let current_json = serde_json::to_value(current).map_err(|e| {
            rusternetes_common::Error::Internal(format!(
                "Failed to serialize current resource: {}",
                e
            ))
        })?;
        let patch_type = crate::patch::PatchType::from_content_type(content_type).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!(
                "Unsupported patch content type: {}",
                e
            ))
        })?;
        crate::patch::apply_patch(&current_json, &patch_value, patch_type).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Failed to apply patch: {}", e))
        })?
    } else if is_apply {
        // Resource doesn't exist + server-side apply = create from patch body
        patch_value.clone()
    } else {
        // Resource doesn't exist + regular patch = error
        return Err(current_result.unwrap_err());
    };

    // Deserialize the patched JSON back to CustomResource
    let mut patched: CustomResource = serde_json::from_value(patched_json).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!(
            "Failed to deserialize patched resource: {}",
            e
        ))
    })?;

    // Strict field validation for patched CRDs: reject unknown top-level fields
    // when the CRD does NOT have preserveUnknownFields.
    // K8s prunes unknown fields and returns them as errors with fieldValidation=Strict.
    let is_strict = parts
        .uri
        .query()
        .and_then(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .find(|(k, _)| k == "fieldValidation")
                .map(|(_, v)| v.to_string())
        })
        .as_deref()
        == Some("Strict");
    if is_strict && !patched.extra.is_empty() {
        let crd_preserves = crd.spec.preserve_unknown_fields == Some(true);
        if !crd_preserves {
            let unknown: Vec<&String> = patched.extra.keys().collect();
            return Err(rusternetes_common::Error::InvalidResource(format!(
                ".{}: field not declared in schema",
                unknown[0]
            )));
        }
    }
    // Also validate embedded metadata fields in the patched result
    if is_strict {
        let patched_value = serde_json::to_value(&patched).unwrap_or_default();
        const KNOWN_META: &[&str] = &[
            "name",
            "generateName",
            "namespace",
            "selfLink",
            "uid",
            "resourceVersion",
            "generation",
            "creationTimestamp",
            "deletionTimestamp",
            "deletionGracePeriodSeconds",
            "labels",
            "annotations",
            "ownerReferences",
            "finalizers",
            "managedFields",
            "clusterName",
        ];
        fn check_embedded_meta(
            value: &serde_json::Value,
            path: &str,
            known: &[&str],
        ) -> Option<String> {
            if let Some(obj) = value.as_object() {
                if let Some(meta) = obj.get("metadata").and_then(|m| m.as_object()) {
                    let mp = if path.is_empty() {
                        ".metadata".to_string()
                    } else {
                        format!("{}.metadata", path)
                    };
                    for key in meta.keys() {
                        if !known.contains(&key.as_str()) {
                            return Some(format!("{}.{}: field not declared in schema", mp, key));
                        }
                    }
                }
                for (key, val) in obj {
                    if key == "metadata" {
                        continue;
                    }
                    let cp = if path.is_empty() {
                        format!(".{}", key)
                    } else {
                        format!("{}.{}", path, key)
                    };
                    if let Some(err) = check_embedded_meta(val, &cp, known) {
                        return Some(err);
                    }
                }
            } else if let Some(arr) = value.as_array() {
                for item in arr {
                    if let Some(err) = check_embedded_meta(item, path, known) {
                        return Some(err);
                    }
                }
            }
            None
        }
        if let Some(err_msg) = check_embedded_meta(&patched_value, "", KNOWN_META) {
            return Err(rusternetes_common::Error::InvalidResource(err_msg));
        }
    }

    // Strict schema validation for nested unknown fields in patched resource
    if is_strict {
        let crd_preserves = crd.spec.preserve_unknown_fields == Some(true);
        let schema_preserves = crd
            .spec
            .versions
            .iter()
            .find(|v| v.name == version)
            .and_then(|v| v.schema.as_ref())
            .map(|s| s.open_apiv3_schema.x_kubernetes_preserve_unknown_fields == Some(true))
            .unwrap_or(false);
        if !crd_preserves && !schema_preserves {
            if let Some(crd_version) = crd.spec.versions.iter().find(|v| v.name == version) {
                if let Some(ref validation) = crd_version.schema {
                    if let Some(ref properties) = validation.open_apiv3_schema.properties {
                        if let Some(spec_schema) = properties.get("spec") {
                            if let Some(ref spec) = patched.spec {
                                SchemaValidator::validate_strict(spec_schema, spec, "spec")?;
                            }
                        }
                    }
                }
            }
        }
    }

    // Validate the patched resource against CRD schema + CEL rules.
    // Pass the prior version (when one exists — server-side apply may create)
    // so transition rules are evaluated.
    validate_custom_resource_with_old(&crd, &version, &patched, current_result.as_ref().ok())?;

    // Ensure name matches
    patched.metadata.name = name.clone();
    patched.api_version = format!("{}/{}", group, version);
    patched.kind = crd.spec.names.kind.clone();

    // Update or create the resource in storage
    let updated = if current_result.is_ok() {
        state.storage.update(&key, &patched).await?
    } else {
        // Server-side apply creates new resource
        patched.metadata.ensure_uid();
        patched.metadata.ensure_creation_timestamp();
        state.storage.create(&key, &patched).await?
    };

    Ok(Json(updated))
}

/// Patch the status subresource of a custom resource
pub async fn patch_custom_resource_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    req: axum::extract::Request,
) -> Result<Json<CustomResource>> {
    use axum::body::to_bytes;

    info!(
        "Patching custom resource status {}/{}/{}: {}",
        group, version, plural, name
    );

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check if status subresource is enabled
    let version_spec = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(format!(
                "Version {} not found in CRD",
                version
            ))
        })?;

    if version_spec.subresources.is_none()
        || version_spec.subresources.as_ref().unwrap().status.is_none()
    {
        return Err(rusternetes_common::Error::InvalidResource(
            "Status subresource not enabled for this CRD".to_string(),
        ));
    }

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "patch", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
            .with_name(&name)
            .with_subresource("status")
    } else {
        RequestAttributes::new(auth_ctx.user, "patch", &plural)
            .with_api_group(&group)
            .with_name(&name)
            .with_subresource("status")
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the current resource
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &name)
    } else {
        build_key(&resource_type, None, &name)
    };

    let mut current: CustomResource = state.storage.get(&key).await?;

    // Split request into parts to avoid borrow/move conflict
    let (parts, body) = req.into_parts();

    // Get Content-Type header to determine patch type
    // Check X-Original-Content-Type first (set by middleware when normalizing)
    let content_type = parts
        .headers
        .get("x-original-content-type")
        .or_else(|| parts.headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json-patch+json");

    // Read the patch body
    let body_bytes = to_bytes(body, usize::MAX).await.map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("Failed to read patch body: {}", e))
    })?;
    let patch_value: serde_json::Value = serde_json::from_slice(&body_bytes).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("Invalid patch JSON: {}", e))
    })?;

    // Apply the patch to the status field only
    let current_status = current
        .status
        .as_ref()
        .unwrap_or(&serde_json::Value::Null)
        .clone();

    let patch_type = crate::patch::PatchType::from_content_type(content_type).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("Unsupported patch content type: {}", e))
    })?;

    let patched_status = crate::patch::apply_patch(&current_status, &patch_value, patch_type)
        .map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!(
                "Failed to apply status patch: {}",
                e
            ))
        })?;

    // Update only the status field
    current.status = Some(patched_status);

    // Save the updated resource
    let updated = state.storage.update(&key, &current).await?;

    Ok(Json(updated))
}

/// Delete a custom resource instance
pub async fn delete_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<CustomResource>> {
    info!(
        "Deleting custom resource {}/{}/{}: {}",
        group, version, plural, name
    );

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "delete", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
            .with_name(&name)
    } else {
        RequestAttributes::new(auth_ctx.user, "delete", &plural)
            .with_api_group(&group)
            .with_name(&name)
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Build storage key
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &name)
    } else {
        build_key(&resource_type, None, &name)
    };

    // Get the resource to check if it exists
    let cr: CustomResource = state.storage.get(&key).await?;

    // Run validating webhooks for DELETE operations.
    // K8s runs validating admission (including webhooks) before deletion.
    // Mutating webhooks are NOT called on DELETE in K8s — only validating.
    {
        let kind = crd.spec.names.kind.clone();
        let gvk = rusternetes_common::admission::GroupVersionKind {
            group: group.clone(),
            version: version.clone(),
            kind: kind.clone(),
        };
        let gvr = rusternetes_common::admission::GroupVersionResource {
            group: group.clone(),
            version: version.clone(),
            resource: plural.clone(),
        };
        let user_info = rusternetes_common::admission::UserInfo {
            username: user_for_webhook.username.clone(),
            uid: user_for_webhook.uid.clone(),
            groups: user_for_webhook.groups.clone(),
        };
        let cr_value = serde_json::to_value(&cr).ok();
        // K8s DELETE AdmissionReview: object is nil, oldObject has the resource being deleted.
        // The webhook inspects oldObject to decide whether to allow deletion.
        let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks_with_dryrun(
                &rusternetes_common::admission::Operation::Delete,
                &gvk,
                &gvr,
                namespace.as_deref(),
                &name,
                None,
                cr_value,
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

    // Check for dry-run
    if crate::handlers::dryrun::is_dry_run(&params) {
        info!(
            "Dry-run: CustomResource {}/{}/{} validated successfully (not deleted)",
            group, plural, name
        );
        return Ok(Json(cr));
    }

    let has_finalizers =
        crate::handlers::finalizers::handle_delete_with_finalizers(&*state.storage, &key, &cr)
            .await?;

    if has_finalizers {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: CustomResource = state.storage.get(&key).await?;
        Ok(Json(updated))
    } else {
        Ok(Json(cr))
    }
}

/// Helper to get CRD from storage
async fn get_crd_for_resource(
    state: &ApiServerState,
    crd_name: &str,
) -> Result<CustomResourceDefinition> {
    let key = build_key("customresourcedefinitions", None, crd_name);
    state.storage.get(&key).await
}

/// Apply schema defaults from the CRD to a custom resource's spec.
/// Walks the CRD's JSONSchemaProps and for each property with a `default`
/// value, sets it on the CR if the field is missing.
fn apply_schema_defaults(crd: &CustomResourceDefinition, version: &str, cr: &mut CustomResource) {
    // Find the version in the CRD
    let crd_version = match crd.spec.versions.iter().find(|v| v.name == version) {
        Some(v) => v,
        None => return,
    };

    // Apply defaults from schema if present
    if let Some(ref validation) = crd_version.schema {
        // The CRD schema's top-level properties typically include "spec", "status", etc.
        // We need to find the "spec" property schema and apply defaults to cr.spec.
        if let Some(ref properties) = validation.open_apiv3_schema.properties {
            if let Some(spec_schema) = properties.get("spec") {
                let spec = cr.spec.get_or_insert_with(|| serde_json::json!({}));
                SchemaValidator::apply_defaults(spec_schema, spec);
            }
        }
        // Also apply defaults at the top level for the whole object
        // (handles cases where the schema defines defaults for top-level fields like "a")
        let mut cr_value = serde_json::to_value(&*cr).unwrap_or_default();
        SchemaValidator::apply_defaults(&validation.open_apiv3_schema, &mut cr_value);
        // Update spec from applied defaults
        if let Some(spec_val) = cr_value.get("spec") {
            cr.spec = Some(spec_val.clone());
        }
        // Update extra fields from applied defaults (top-level fields beyond
        // apiVersion/kind/metadata/spec/status)
        if let Some(obj) = cr_value.as_object() {
            for (k, v) in obj {
                if k != "apiVersion"
                    && k != "kind"
                    && k != "metadata"
                    && k != "spec"
                    && k != "status"
                {
                    cr.extra.insert(k.clone(), v.clone());
                }
            }
        }
    }
}

/// Test-only re-export of `prune_custom_resource` so admission webhook tests
/// can verify the pruning path applied after mutating webhooks.
#[cfg(test)]
pub(crate) fn test_prune_custom_resource(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &mut CustomResource,
) {
    prune_custom_resource(crd, version, cr)
}

/// Prune unknown fields from a CR based on the CRD schema.
/// K8s removes fields not in the schema unless x-kubernetes-preserve-unknown-fields is set.
/// This runs AFTER webhook mutation so webhook-added fields not in the schema are removed.
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning
fn prune_custom_resource(crd: &CustomResourceDefinition, version: &str, cr: &mut CustomResource) {
    let crd_version = match crd.spec.versions.iter().find(|v| v.name == version) {
        Some(v) => v,
        None => return,
    };

    // Check if the CRD preserves unknown fields globally
    if crd.spec.preserve_unknown_fields == Some(true) {
        return;
    }

    let schema = match &crd_version.schema {
        Some(s) => &s.open_apiv3_schema,
        None => return,
    };

    // Check if root schema preserves unknown fields
    if schema.x_kubernetes_preserve_unknown_fields == Some(true) {
        return;
    }

    // Get the schema properties for the "data" field (or whatever top-level fields exist)
    // K8s prunes against spec/status/metadata + any additional properties
    let schema_properties: std::collections::HashSet<String> = schema
        .properties
        .as_ref()
        .map(|props| props.keys().cloned().collect())
        .unwrap_or_default();

    // Prune extra fields on the CR that aren't in the schema
    // K8s preserves: apiVersion, kind, metadata (always)
    // Everything else is checked against schema properties
    let known_top_level: std::collections::HashSet<&str> =
        ["apiVersion", "kind", "metadata"].iter().copied().collect();

    let cr_keys: Vec<String> = cr.extra.keys().cloned().collect();
    for key in cr_keys {
        if !known_top_level.contains(key.as_str()) && !schema_properties.contains(&key) {
            tracing::debug!("Pruning unknown field '{}' from CR", key);
            cr.extra.remove(&key);
        }
    }

    // Also prune within known fields like "data", "spec", "status"
    // by checking their nested schema properties.
    if let Some(schema_props) = &schema.properties {
        for (field_name, field_schema) in schema_props {
            // Check if this field preserves unknown fields
            if field_schema.x_kubernetes_preserve_unknown_fields == Some(true) {
                continue;
            }
            // The CustomResource type lifts "spec" and "status" out of the flat
            // map and stores them on their own fields. Walk those as well so
            // webhook-injected unknown fields under spec/status get pruned.
            if field_name == "spec" {
                if let Some(spec_val) = cr.spec.as_mut() {
                    prune_value_against_schema(spec_val, field_schema, "spec");
                }
                continue;
            }
            if field_name == "status" {
                if let Some(status_val) = cr.status.as_mut() {
                    prune_value_against_schema(status_val, field_schema, "status");
                }
                continue;
            }
            if let Some(field_props) = &field_schema.properties {
                let allowed_keys: std::collections::HashSet<String> =
                    field_props.keys().cloned().collect();

                // Prune from cr.extra if this field is there
                if let Some(field_val) = cr.extra.get_mut(field_name) {
                    if let Some(obj) = field_val.as_object_mut() {
                        let obj_keys: Vec<String> = obj.keys().cloned().collect();
                        for k in obj_keys {
                            if !allowed_keys.contains(&k) {
                                tracing::debug!(
                                    "Pruning unknown field '{}.{}' from CR",
                                    field_name,
                                    k
                                );
                                obj.remove(&k);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Recursively prune unknown fields from a JSON value against a JSONSchema.
/// Fields not declared as a property are removed unless the schema (or the
/// enclosing object) carries x-kubernetes-preserve-unknown-fields.
///
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/prune.go
pub(crate) fn prune_value_against_schema(
    value: &mut serde_json::Value,
    schema: &rusternetes_common::resources::JSONSchemaProps,
    path: &str,
) {
    if schema.x_kubernetes_preserve_unknown_fields == Some(true) {
        return;
    }
    match value {
        serde_json::Value::Object(obj) => {
            let Some(props) = schema.properties.as_ref() else {
                return;
            };
            obj.retain(|k, _| {
                let keep = props.contains_key(k);
                if !keep {
                    tracing::debug!("Pruning unknown field '{}.{}'", path, k);
                }
                keep
            });
            for (k, child_schema) in props {
                if let Some(child_val) = obj.get_mut(k) {
                    prune_value_against_schema(child_val, child_schema, &format!("{}.{}", path, k));
                }
            }
        }
        serde_json::Value::Array(arr) => {
            use rusternetes_common::resources::JSONSchemaPropsOrArray;
            if let Some(JSONSchemaPropsOrArray::Schema(item_schema)) = schema.items.as_deref() {
                for (i, item) in arr.iter_mut().enumerate() {
                    prune_value_against_schema(item, item_schema, &format!("{}[{}]", path, i));
                }
            }
        }
        _ => {}
    }
}

/// Prune a CR represented as a raw JSON `Value` against its CRD's structural
/// schema. Used by the admission webhook pipeline after mutating webhooks
/// apply JSON patches, so any unknown field a webhook injects is stripped
/// before the value is persisted (mirrors the in-handler `prune_custom_resource`
/// path used on the `CustomResource` struct).
///
/// Top-level `apiVersion`, `kind`, and `metadata` are never pruned — they are
/// owned by the Kubernetes object meta, not the structural schema.
///
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/prune.go
pub(crate) fn prune_custom_resource_value(
    crd: &CustomResourceDefinition,
    version: &str,
    value: &mut serde_json::Value,
) {
    let Some(crd_version) = crd.spec.versions.iter().find(|v| v.name == version) else {
        return;
    };

    // Spec-level preserveUnknownFields disables structural-schema pruning.
    if crd.spec.preserve_unknown_fields == Some(true) {
        return;
    }

    let Some(validation) = &crd_version.schema else {
        return;
    };
    let schema = &validation.open_apiv3_schema;

    // Root-level preserveUnknownFields disables pruning for the whole object.
    if schema.x_kubernetes_preserve_unknown_fields == Some(true) {
        return;
    }

    let Some(obj) = value.as_object_mut() else {
        return;
    };

    // Top-level keys allowed even without an explicit schema entry — these
    // are k8s object meta, not part of the CRD's structural schema.
    const META_KEYS: &[&str] = &["apiVersion", "kind", "metadata"];

    let schema_props = schema.properties.as_ref();
    let allowed_top: std::collections::HashSet<&str> = schema_props
        .map(|p| p.keys().map(String::as_str).collect())
        .unwrap_or_default();

    obj.retain(|k, _| META_KEYS.contains(&k.as_str()) || allowed_top.contains(k.as_str()));

    if let Some(props) = schema_props {
        for (k, child_schema) in props {
            if let Some(child_val) = obj.get_mut(k) {
                prune_value_against_schema(child_val, child_schema, k);
            }
        }
    }
}

/// Validate a custom resource against its CRD schema.
///
/// On UPDATE/PATCH callers should use [`validate_custom_resource_with_old`]
/// so transition rules (`oldSelf`) are evaluated against the prior version.
fn validate_custom_resource(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
) -> Result<()> {
    validate_custom_resource_with_old(crd, version, cr, None)
}

/// Like [`validate_custom_resource`] but also evaluates `x-kubernetes-validations`
/// transition rules using `old_cr` as the prior version for `oldSelf` bindings.
fn validate_custom_resource_with_old(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
    old_cr: Option<&CustomResource>,
) -> Result<()> {
    validate_custom_resource_schema(crd, version, cr)?;
    crate::handlers::cel_validation::validate_cr_against_crd(crd, version, cr, old_cr)
}

/// Schema-only portion of CR validation, kept as a separate helper so the
/// CEL pass can run after structural validation is known to succeed.
fn validate_custom_resource_schema(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
) -> Result<()> {
    // Find the version in the CRD
    let crd_version = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(format!(
                "Version {} not found in CRD",
                version
            ))
        })?;

    // Check if version is served
    if !crd_version.served {
        warn!("Version {} is not served", version);
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "Version {} is not served",
            version
        )));
    }

    // Validate against schema if present.
    // K8s skips structural pruning/validation when CRD has preserveUnknownFields: true
    // (line 1432 in customresource_handler.go). Only metadata coercion runs.
    // Also skip if the schema root has x-kubernetes-preserve-unknown-fields: true.
    let crd_preserves = crd.spec.preserve_unknown_fields == Some(true);
    if let Some(ref validation) = crd_version.schema {
        let schema_preserves = validation
            .open_apiv3_schema
            .x_kubernetes_preserve_unknown_fields
            == Some(true);

        if !crd_preserves && !schema_preserves {
            // Validate ALL top-level properties against the schema, not just spec.
            // K8s validates the entire CR object. Some CRDs define enum fields
            // at the top level (e.g., cronies), not under spec.
            if let Some(ref properties) = validation.open_apiv3_schema.properties {
                // Validate spec if present
                if let Some(ref spec) = cr.spec {
                    if let Some(spec_schema) = properties.get("spec") {
                        SchemaValidator::validate_no_unknown_check(spec_schema, spec)?;
                    }
                }
                // Validate status if present
                if let Some(ref status) = cr.status {
                    if let Some(status_schema) = properties.get("status") {
                        SchemaValidator::validate_no_unknown_check(status_schema, status)?;
                    }
                }
                // Validate extra top-level fields (e.g., cronies, hostPort)
                for (key, value) in &cr.extra {
                    if key == "metadata" || key == "apiVersion" || key == "kind" {
                        continue;
                    }
                    if let Some(field_schema) = properties.get(key) {
                        SchemaValidator::validate_no_unknown_check(field_schema, value)?;
                    }
                }
            }
        }
        // When preserveUnknownFields is true, skip structural validation.
        // K8s only runs metadata coercion (CoerceWithOptions) in this case.
    }

    Ok(())
}

/// Get the status subresource of a custom resource
pub async fn get_custom_resource_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
) -> Result<Json<serde_json::Value>> {
    info!(
        "Getting custom resource status {}/{}/{}: {}",
        group, version, plural, name
    );

    // Get the full resource first
    let cr: CustomResource = get_custom_resource(
        State(state.clone()),
        Extension(auth_ctx),
        Path((group, version, plural, namespace, name)),
    )
    .await?
    .0;

    // Extract and return just the status field
    let status = cr.status.unwrap_or(serde_json::Value::Null);
    Ok(Json(status))
}

/// Update the status subresource of a custom resource
pub async fn update_custom_resource_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    Json(status): Json<serde_json::Value>,
) -> Result<Json<CustomResource>> {
    info!(
        "Updating custom resource status {}/{}/{}: {}",
        group, version, plural, name
    );

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check if status subresource is enabled
    let version_spec = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(format!(
                "Version {} not found in CRD",
                version
            ))
        })?;

    if version_spec.subresources.is_none()
        || version_spec.subresources.as_ref().unwrap().status.is_none()
    {
        return Err(rusternetes_common::Error::InvalidResource(
            "Status subresource not enabled for this CRD".to_string(),
        ));
    }

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "update", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
            .with_name(&name)
            .with_subresource("status")
    } else {
        RequestAttributes::new(auth_ctx.user, "update", &plural)
            .with_api_group(&group)
            .with_name(&name)
            .with_subresource("status")
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the existing resource
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &name)
    } else {
        build_key(&resource_type, None, &name)
    };

    let mut cr: CustomResource = state.storage.get(&key).await?;

    // Update only the status field (optimistic concurrency control)
    cr.status = Some(status);

    // Save the updated resource
    let updated = state.storage.update(&key, &cr).await?;

    Ok(Json(updated))
}

/// Get the scale subresource of a custom resource
pub async fn get_custom_resource_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
) -> Result<Json<Scale>> {
    info!(
        "Getting custom resource scale {}/{}/{}: {}",
        group, version, plural, name
    );

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check if scale subresource is enabled and get the configuration
    let version_spec = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(format!(
                "Version {} not found in CRD",
                version
            ))
        })?;

    let scale_config = version_spec
        .subresources
        .as_ref()
        .and_then(|s| s.scale.as_ref())
        .ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(
                "Scale subresource not enabled for this CRD".to_string(),
            )
        })?;

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "get", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
            .with_name(&name)
            .with_subresource("scale")
    } else {
        RequestAttributes::new(auth_ctx.user, "get", &plural)
            .with_api_group(&group)
            .with_name(&name)
            .with_subresource("scale")
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the existing resource
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &name)
    } else {
        build_key(&resource_type, None, &name)
    };

    let cr: CustomResource = state.storage.get(&key).await?;

    // Per apiextensions.k8s.io/v1 CustomResourceSubresourceScale:
    //   specReplicasPath   MUST be a JSONPath under .spec
    //   statusReplicasPath MUST be a JSONPath under .status
    //   labelSelectorPath  MUST be a JSONPath under .status
    // Strip the root segment so we can resolve against the already-narrowed
    // cr.spec / cr.status objects.
    let spec_replicas = extract_json_path(
        &cr.spec,
        strip_root_prefix(&scale_config.spec_replicas_path, "spec"),
    )
    .and_then(|v| v.as_i64())
    .unwrap_or(0) as i32;

    let status_replicas = extract_json_path(
        &cr.status,
        strip_root_prefix(&scale_config.status_replicas_path, "status"),
    )
    .and_then(|v| v.as_i64())
    .unwrap_or(0) as i32;

    let label_selector = if let Some(ref selector_path) = scale_config.label_selector_path {
        extract_json_path(&cr.status, strip_root_prefix(selector_path, "status"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    } else {
        None
    };

    let scale = Scale {
        api_version: "autoscaling/v1".to_string(),
        kind: "Scale".to_string(),
        metadata: cr.metadata.clone(),
        spec: ScaleSpec {
            replicas: spec_replicas,
        },
        status: Some(ScaleStatus {
            replicas: status_replicas,
            selector: label_selector,
        }),
    };

    Ok(Json(scale))
}

/// Update the scale subresource of a custom resource
pub async fn update_custom_resource_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    Json(scale): Json<Scale>,
) -> Result<Json<Scale>> {
    info!(
        "Updating custom resource scale {}/{}/{}: {}",
        group, version, plural, name
    );

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check if scale subresource is enabled and get the configuration
    let version_spec = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(format!(
                "Version {} not found in CRD",
                version
            ))
        })?;

    let scale_config = version_spec
        .subresources
        .as_ref()
        .and_then(|s| s.scale.as_ref())
        .ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(
                "Scale subresource not enabled for this CRD".to_string(),
            )
        })?;

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "update", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
            .with_name(&name)
            .with_subresource("scale")
    } else {
        RequestAttributes::new(auth_ctx.user.clone(), "update", &plural)
            .with_api_group(&group)
            .with_name(&name)
            .with_subresource("scale")
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the existing resource
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let key = if let Some(ref ns) = namespace {
        build_key(&resource_type, Some(ns), &name)
    } else {
        build_key(&resource_type, None, &name)
    };

    let mut cr: CustomResource = state.storage.get(&key).await?;

    // Update the replica count in the spec using JSONPath.
    // specReplicasPath is rooted at .spec (see GET path above) — strip the
    // root segment before writing into the narrowed cr.spec object.
    if let Some(ref mut spec) = cr.spec {
        set_json_path(
            spec,
            strip_root_prefix(&scale_config.spec_replicas_path, "spec"),
            scale.spec.replicas,
        );
    }

    // Save the updated resource
    let _updated = state.storage.update(&key, &cr).await?;

    // Return the updated scale representation
    get_custom_resource_scale(
        State(state),
        Extension(auth_ctx),
        Path((group, version, plural, namespace, name)),
    )
    .await
}

/// Strip the CRD scale subresource root segment (`spec` or `status`) from a
/// JSONPath so it can be resolved against the already-narrowed `cr.spec` /
/// `cr.status` value. Handles both `.spec.replicas` and `spec.replicas`
/// input forms. Returns the path **unchanged** if the prefix isn't present
/// or doesn't form a full path segment, so a caller passing the wrong root
/// surfaces the mismatch instead of silently rewriting the path.
fn strip_root_prefix<'a>(path: &'a str, root: &str) -> &'a str {
    let trimmed = path.strip_prefix('.').unwrap_or(path);
    match trimmed.strip_prefix(root) {
        Some(rest) if rest.starts_with('.') => &rest[1..],
        Some("") => "",
        _ => path,
    }
}

/// Helper to extract a value from a JSON object using a simple JSONPath
fn extract_json_path<'a>(
    json: &'a Option<serde_json::Value>,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let json = json.as_ref()?;
    let parts: Vec<&str> = path.trim_start_matches('.').split('.').collect();

    let mut current = json;
    for part in parts {
        current = current.get(part)?;
    }

    Some(current)
}

/// Helper to set a value in a JSON object using a simple JSONPath
fn set_json_path(json: &mut serde_json::Value, path: &str, value: i32) {
    let parts: Vec<&str> = path.trim_start_matches('.').split('.').collect();

    if parts.is_empty() {
        return;
    }

    // Ensure we're working with an object
    if !json.is_object() {
        *json = serde_json::json!({});
    }

    let mut current = json;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last part - set the value
            if let Some(obj) = current.as_object_mut() {
                obj.insert(part.to_string(), serde_json::Value::Number(value.into()));
            }
        } else {
            // Intermediate part - navigate or create
            let obj = current.as_object_mut().unwrap();
            current = obj
                .entry(part.to_string())
                .or_insert_with(|| serde_json::json!({}));
        }
    }
}

/// Scale represents the scale subresource of a custom resource
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scale {
    pub api_version: String,
    pub kind: String,
    pub metadata: rusternetes_common::types::ObjectMeta,
    pub spec: ScaleSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ScaleStatus>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScaleSpec {
    pub replicas: i32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScaleStatus {
    pub replicas: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, ResourceScope,
    };
    use rusternetes_common::types::ObjectMeta;
    use serde_json::json;

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
                }],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    fn create_test_custom_resource() -> CustomResource {
        CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: {
                let mut meta = ObjectMeta::new("my-crontab");
                meta.namespace = Some("default".to_string());
                meta
            },
            spec: Some(json!({
                "cronSpec": "* * * * */5",
                "image": "my-cron-image"
            })),
            status: None,
            extra: Default::default(),
        }
    }

    #[test]
    fn test_validate_custom_resource_success() {
        let crd = create_test_crd();
        let cr = create_test_custom_resource();

        assert!(validate_custom_resource(&crd, "v1", &cr).is_ok());
    }

    #[test]
    fn test_validate_custom_resource_invalid_version() {
        let crd = create_test_crd();
        let cr = create_test_custom_resource();

        let result = validate_custom_resource(&crd, "v2", &cr);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_custom_resource_not_served() {
        let mut crd = create_test_crd();
        crd.spec.versions[0].served = false;
        let cr = create_test_custom_resource();

        let result = validate_custom_resource(&crd, "v1", &cr);
        assert!(result.is_err());
    }

    /// Helper: create a CRD with a schema that has default values
    fn create_crd_with_defaults() -> CustomResourceDefinition {
        use rusternetes_common::resources::crd::JSONSchemaProps;
        use rusternetes_common::resources::CustomResourceValidation;

        let mut spec_properties = std::collections::HashMap::new();
        spec_properties.insert(
            "cronSpec".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "image".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                default: Some(json!("default-image:latest")),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "replicas".to_string(),
            JSONSchemaProps {
                type_: Some("integer".to_string()),
                default: Some(json!(1)),
                ..Default::default()
            },
        );
        // Nested object with defaults
        let mut nested_props = std::collections::HashMap::new();
        nested_props.insert(
            "enabled".to_string(),
            JSONSchemaProps {
                type_: Some("boolean".to_string()),
                default: Some(json!(true)),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "logging".to_string(),
            JSONSchemaProps {
                type_: Some("object".to_string()),
                properties: Some(nested_props),
                ..Default::default()
            },
        );

        let spec_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(spec_properties),
            ..Default::default()
        };

        let mut top_properties = std::collections::HashMap::new();
        top_properties.insert("spec".to_string(), spec_schema);

        let top_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(top_properties),
            ..Default::default()
        };

        let mut crd = create_test_crd();
        crd.spec.versions[0].schema = Some(CustomResourceValidation {
            open_apiv3_schema: top_schema,
        });
        crd
    }

    #[test]
    fn test_schema_defaults_applied_to_missing_fields() {
        let crd = create_crd_with_defaults();
        // CR with only cronSpec set — image and replicas should get defaults
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "* * * * */5"
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(spec["cronSpec"], json!("* * * * */5"));
        assert_eq!(
            spec["image"],
            json!("default-image:latest"),
            "Default for 'image' should be applied"
        );
        assert_eq!(
            spec["replicas"],
            json!(1),
            "Default for 'replicas' should be applied"
        );
    }

    #[test]
    fn test_schema_defaults_do_not_overwrite_existing() {
        let crd = create_crd_with_defaults();
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "0 0 * * *",
                "image": "my-custom-image:v2",
                "replicas": 3
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(
            spec["image"],
            json!("my-custom-image:v2"),
            "Existing value should not be overwritten"
        );
        assert_eq!(
            spec["replicas"],
            json!(3),
            "Existing value should not be overwritten"
        );
    }

    #[test]
    fn test_schema_defaults_nested_objects() {
        let crd = create_crd_with_defaults();
        // CR has a logging object but without 'enabled' — default should be applied
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "* * * * */5",
                "logging": {}
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(
            spec["logging"]["enabled"],
            json!(true),
            "Nested default for 'logging.enabled' should be applied"
        );
    }

    #[test]
    fn test_strict_field_validation_rejects_unknown_cr_fields() {
        // Simulate strict field validation on a CR body with an unknown top-level field
        let cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({"cronSpec": "* * * * */5"})),
            status: None,
            extra: Default::default(),
        };

        // Body includes an unknown field "unknownTopLevel"
        let body = br#"{"apiVersion":"stable.example.com/v1","kind":"CronTab","metadata":{"name":"my-crontab"},"spec":{"cronSpec":"* * * * */5"},"unknownTopLevel":"bad"}"#;

        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = crate::handlers::validation::validate_strict_fields(&params, body, &cr);
        assert!(
            result.is_err(),
            "Strict validation should reject unknown fields"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("strict decoding error"),
            "Error should mention strict decoding: {}",
            err_msg
        );
        assert!(
            err_msg.contains("unknownTopLevel"),
            "Error should mention the unknown field name: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_field_validation_allows_valid_cr() {
        let cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({"cronSpec": "* * * * */5"})),
            status: None,
            extra: Default::default(),
        };

        let body = br#"{"apiVersion":"stable.example.com/v1","kind":"CronTab","metadata":{"name":"my-crontab"},"spec":{"cronSpec":"* * * * */5"}}"#;

        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = crate::handlers::validation::validate_strict_fields(&params, body, &cr);
        assert!(
            result.is_ok(),
            "Strict validation should pass for valid CR: {:?}",
            result
        );
    }

    /// Regression: `get_custom_resource_scale` resolved `.spec.replicas`
    /// against the already-narrowed `cr.spec` object, ending up at the
    /// non-existent `spec.spec.replicas` path → replicas always 0.
    #[test]
    fn strip_root_prefix_resolves_scale_subresource_paths() {
        // K8s-conventional CRD paths
        assert_eq!(strip_root_prefix(".spec.replicas", "spec"), "replicas");
        assert_eq!(strip_root_prefix(".status.replicas", "status"), "replicas");
        assert_eq!(strip_root_prefix(".status.selector", "status"), "selector");

        // Nested paths under .spec / .status
        assert_eq!(
            strip_root_prefix(".spec.scaling.replicas", "spec"),
            "scaling.replicas"
        );

        // Tolerate missing leading dot
        assert_eq!(strip_root_prefix("spec.replicas", "spec"), "replicas");

        // Tolerate already-stripped path (no-op)
        assert_eq!(strip_root_prefix("replicas", "spec"), "replicas");

        // Wrong root prefix is left untouched (caller mismatch — surfaces as
        // empty lookup, not silent rewrite)
        assert_eq!(
            strip_root_prefix(".status.replicas", "spec"),
            ".status.replicas"
        );
    }

    /// End-to-end: a `.spec.replicas`-pathed scale subresource read through
    /// `extract_json_path` after `strip_root_prefix` returns the actual
    /// replicas count, not the pre-fix `0` it produced before.
    #[test]
    fn extract_scale_replicas_after_strip_returns_expected_value() {
        let spec = Some(serde_json::json!({ "replicas": 7 }));
        let path = strip_root_prefix(".spec.replicas", "spec");
        let got = extract_json_path(&spec, path).and_then(|v| v.as_i64());
        assert_eq!(got, Some(7));
    }

    /// And the symmetric write path: setting `.spec.replicas` after strip
    /// stamps `replicas` on the spec root, not `spec.replicas` underneath it.
    #[test]
    fn set_scale_replicas_after_strip_writes_at_spec_root() {
        let mut spec = serde_json::json!({});
        set_json_path(&mut spec, strip_root_prefix(".spec.replicas", "spec"), 5);
        assert_eq!(spec, serde_json::json!({ "replicas": 5 }));
    }
}
