//! Scale subresource handlers
//!
//! Implements the /scale subresource for resources that support scaling.
//! The scale subresource allows getting and setting the replica count
//! for workload resources like Deployments, StatefulSets, and ReplicaSets.

use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, State},
    http::Uri,
    Extension, Json,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    Error, Result,
};
use rusternetes_storage::{build_key, Storage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tracing::info;

/// Scale represents the scale of a resource
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scale {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ScaleMetadata,
    pub spec: ScaleSpec,
    pub status: ScaleStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleMetadata {
    pub name: String,
    pub namespace: String,
    #[serde(rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(rename = "creationTimestamp", skip_serializing_if = "Option::is_none")]
    pub creation_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleSpec {
    pub replicas: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleStatus {
    pub replicas: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
}

/// Extract group, version, and resource type from a scale subresource URI.
/// e.g. "/apis/apps/v1/namespaces/default/deployments/foo/scale" -> ("apps", "v1", "deployments")
/// e.g. "/api/v1/namespaces/default/replicationcontrollers/foo/scale" -> ("", "v1", "replicationcontrollers")
fn parse_scale_uri(uri: &Uri) -> (String, String, String) {
    let path = uri.path();
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    // Resource is 3 segments from the end (before name and "scale")
    let resource = if segments.len() >= 3 {
        segments[segments.len() - 3].to_string()
    } else {
        "unknown".to_string()
    };

    // Detect group and version from path prefix
    if segments.first() == Some(&"apis") && segments.len() >= 3 {
        // /apis/{group}/{version}/...
        (segments[1].to_string(), segments[2].to_string(), resource)
    } else {
        // /api/{version}/... (core group)
        let version = if segments.len() >= 2 {
            segments[1].to_string()
        } else {
            "v1".to_string()
        };
        ("".to_string(), version, resource)
    }
}

/// GET /apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}/scale
/// Returns the scale subresource for a resource
pub async fn get_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Scale>> {
    let (group, version, resource) = parse_scale_uri(&uri);
    info!(
        "Getting scale for {}/{}/{}/{}",
        group, resource, namespace, name
    );

    // Check authorization — use resource name directly (not group-qualified)
    let attrs = RequestAttributes::new(auth_ctx.user, "get", &resource)
        .with_namespace(&namespace)
        .with_api_group(&group)
        .with_name(&name)
        .with_subresource("scale");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the resource
    let key = build_key(&resource, Some(&namespace), &name);
    let resource_obj: Value = state.storage.get(&key).await?;

    // Extract scale information
    let scale = extract_scale(&resource_obj, &namespace, &name, &group, &version)?;

    Ok(Json(scale))
}

/// PUT /apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}/scale
/// Updates the scale subresource for a resource
pub async fn update_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    Path((namespace, name)): Path<(String, String)>,
    Json(scale): Json<Scale>,
) -> Result<Json<Scale>> {
    let (group, version, resource) = parse_scale_uri(&uri);
    info!(
        "Updating scale for {}/{}/{}/{}",
        group, resource, namespace, name
    );

    // Check authorization — use resource name directly (not group-qualified)
    let attrs = RequestAttributes::new(auth_ctx.user, "update", &resource)
        .with_namespace(&namespace)
        .with_api_group(&group)
        .with_name(&name)
        .with_subresource("scale");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the current resource
    let key = build_key(&resource, Some(&namespace), &name);
    let mut resource_obj: Value = state.storage.get(&key).await?;

    // Update the replicas in the spec
    if let Some(spec) = resource_obj.get_mut("spec") {
        if let Some(spec_obj) = spec.as_object_mut() {
            spec_obj.insert(
                "replicas".to_string(),
                Value::Number(scale.spec.replicas.into()),
            );
        }
    }

    // Save the updated resource
    let updated_resource: Value = state.storage.update(&key, &resource_obj).await?;

    // Extract and return the updated scale
    let updated_scale = extract_scale(&updated_resource, &namespace, &name, &group, &version)?;

    info!(
        "Successfully updated scale for {}/{}/{}/{}",
        group, resource, namespace, name
    );

    Ok(Json(updated_scale))
}

/// PATCH /apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}/scale
/// Patches the scale subresource for a resource
pub async fn patch_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    Path((namespace, name)): Path<(String, String)>,
    body: String,
) -> Result<Json<Scale>> {
    let (group, version, resource) = parse_scale_uri(&uri);
    info!(
        "Patching scale for {}/{}/{}/{}",
        group, resource, namespace, name
    );

    // Check authorization — use the resource name directly (not group-qualified)
    // K8s authorizes scale subresource as "patch" on the parent resource
    let attrs = RequestAttributes::new(auth_ctx.user, "patch", &resource)
        .with_namespace(&namespace)
        .with_api_group(&group)
        .with_name(&name)
        .with_subresource("scale");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Parse the patch body as JSON
    let patch: Value = serde_json::from_str(&body)
        .map_err(|e| Error::InvalidResource(format!("Invalid patch body: {}", e)))?;

    // Get the current resource and apply patch with retry on conflict.
    // K8s PATCH reads the latest version and re-applies on RV mismatch.
    let key = build_key(&resource, Some(&namespace), &name);

    // Extract the new replicas from the patch
    // Patch may be {"spec":{"replicas":N}} or a full Scale object
    let new_replicas = patch
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|r| r.as_i64())
        .map(|r| r as i32);

    let mut updated_resource: Value = Value::Null;
    for _retry in 0..5 {
        let mut resource_obj: Value = state.storage.get(&key).await?;

        if let Some(replicas) = new_replicas {
            // Update the replicas in the resource spec
            if let Some(spec) = resource_obj.get_mut("spec") {
                if let Some(spec_obj) = spec.as_object_mut() {
                    spec_obj.insert("replicas".to_string(), Value::Number(replicas.into()));
                }
            }
        }

        // Save the updated resource — retry on conflict
        match state.storage.update(&key, &resource_obj).await {
            Ok(v) => {
                updated_resource = v;
                break;
            }
            Err(Error::Conflict(_)) => {
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    if updated_resource.is_null() {
        return Err(Error::Conflict(
            "scale patch failed after retries".to_string(),
        ));
    }

    // Extract and return the updated scale
    let updated_scale = extract_scale(&updated_resource, &namespace, &name, &group, &version)?;

    info!(
        "Successfully patched scale for {}/{}/{}/{}",
        group, resource, namespace, name
    );

    Ok(Json(updated_scale))
}

/// Extract scale information from a resource object
fn extract_scale(
    resource: &Value,
    namespace: &str,
    name: &str,
    _group: &str,
    _version: &str,
) -> Result<Scale> {
    let metadata = resource
        .get("metadata")
        .ok_or_else(|| Error::InvalidResource("Missing metadata".to_string()))?;

    let spec = resource
        .get("spec")
        .ok_or_else(|| Error::InvalidResource("Missing spec".to_string()))?;

    let status = resource.get("status");

    let resource_version = metadata
        .get("resourceVersion")
        .and_then(|v| v.as_str())
        .map(String::from);

    let creation_timestamp = metadata
        .get("creationTimestamp")
        .and_then(|v| v.as_str())
        .map(String::from);

    let replicas_spec = spec.get("replicas").and_then(|v| v.as_i64()).unwrap_or(1) as i32;

    let replicas_status = status
        .and_then(|s| s.get("replicas"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;

    // Extract selector from spec — convert to label selector string format.
    // K8s returns selector as "key1=value1,key2=value2" (not JSON).
    // For RCs, selector is a map; for Deployments/RS/SS, it's a matchLabels object.
    let selector = spec.get("selector").and_then(|s| {
        if let Some(obj) = s.as_object() {
            // Direct map selector (ReplicationController)
            if obj.contains_key("matchLabels") || obj.contains_key("matchExpressions") {
                // LabelSelector — extract matchLabels
                if let Some(ml) = obj.get("matchLabels").and_then(|v| v.as_object()) {
                    let parts: Vec<String> = ml
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                        .collect();
                    Some(parts.join(","))
                } else {
                    None
                }
            } else {
                // Simple map selector (RC style)
                let parts: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                    .collect();
                Some(parts.join(","))
            }
        } else {
            s.as_str().map(|str_val| str_val.to_string())
        }
    });

    Ok(Scale {
        api_version: "autoscaling/v1".to_string(),
        kind: "Scale".to_string(),
        metadata: ScaleMetadata {
            name: name.to_string(),
            namespace: namespace.to_string(),
            resource_version,
            creation_timestamp,
        },
        spec: ScaleSpec {
            replicas: replicas_spec,
        },
        status: ScaleStatus {
            replicas: replicas_status,
            selector,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_scale() {
        let resource = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "test-deployment",
                "namespace": "default",
                "resourceVersion": "100",
                "creationTimestamp": "2026-03-10T00:00:00Z"
            },
            "spec": {
                "replicas": 3,
                "selector": {
                    "matchLabels": {
                        "app": "test"
                    }
                }
            },
            "status": {
                "replicas": 3,
                "readyReplicas": 3
            }
        });

        let scale = extract_scale(&resource, "default", "test-deployment", "apps", "v1").unwrap();

        assert_eq!(scale.kind, "Scale");
        assert_eq!(scale.api_version, "autoscaling/v1");
        assert_eq!(scale.metadata.name, "test-deployment");
        assert_eq!(scale.metadata.namespace, "default");
        assert_eq!(scale.spec.replicas, 3);
        assert_eq!(scale.status.replicas, 3);
    }

    #[test]
    fn test_extract_scale_no_status() {
        let resource = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "test-deployment",
                "namespace": "default"
            },
            "spec": {
                "replicas": 5
            }
        });

        let scale = extract_scale(&resource, "default", "test-deployment", "apps", "v1").unwrap();

        assert_eq!(scale.spec.replicas, 5);
        assert_eq!(scale.status.replicas, 0); // No status, so 0
    }
}
