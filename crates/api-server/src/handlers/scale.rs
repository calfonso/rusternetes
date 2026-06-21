//! Scale subresource handlers
//!
//! Implements the /scale subresource for resources that support scaling.
//! The scale subresource allows getting and setting the replica count
//! for workload resources like Deployments, StatefulSets, and ReplicaSets.

use crate::{
    middleware::AuthContext,
    patch::{apply_patch, PatchType},
    response::{negotiate_content_type, ContentType},
    state::ApiServerState,
};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode, Uri},
    response::Response,
    Extension,
};
use rusternetes_common::dump::DumpingJson;
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
/// Returns the scale subresource for a resource.
///
/// Honors content negotiation: when the client sends
/// `Accept: application/vnd.kubernetes.protobuf`, the Scale is emitted as a
/// `k8s\0`-prefixed Unknown envelope (matching upstream kube-apiserver's
/// protobuf serializer). Otherwise JSON is returned.
pub async fn get_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    headers: HeaderMap,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response> {
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

    Ok(scale_response(&scale, &headers, StatusCode::OK))
}

/// PUT /apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}/scale
/// Updates the scale subresource for a resource.
///
/// Honors content negotiation on the response side: clients that send
/// `Accept: application/vnd.kubernetes.protobuf` receive the updated Scale
/// wrapped in the K8s `k8s\0` Unknown envelope. Request body is parsed via
/// `Json<Scale>` — proto-encoded request bodies are decoded by the
/// `normalize_content_type_middleware` into JSON before reaching this
/// handler, so a single extractor covers both wire formats.
pub async fn update_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    headers: HeaderMap,
    Path((namespace, name)): Path<(String, String)>,
    DumpingJson(scale): DumpingJson<Scale>,
) -> Result<Response> {
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

    // Scale.spec.replicas must be non-negative (upstream ValidateScale).
    if scale.spec.replicas < 0 {
        return Err(Error::Invalid(vec![
            rusternetes_common::validation::field::Error::invalid(
                &rusternetes_common::validation::field::Path::new("spec").child("replicas"),
                scale.spec.replicas,
                "must be greater than or equal to 0",
            ),
        ]));
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

    Ok(scale_response(&updated_scale, &headers, StatusCode::OK))
}

/// PATCH /apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}/scale
/// Patches the scale subresource for a resource.
///
/// Same content-negotiation semantics as [`get_scale`] / [`update_scale`]:
/// a protobuf Accept header produces a `k8s\0`-framed Unknown envelope on
/// the response side.
///
/// Supports all three K8s patch content types:
/// - `application/strategic-merge-patch+json` (default for `kubectl scale` /
///   the `Deployment should have a working scale subresource` conformance test)
/// - `application/merge-patch+json` (RFC 7386)
/// - `application/json-patch+json` (RFC 6902 — array of ops like
///   `{"op":"replace","path":"/spec/replicas","value":N}`)
///
/// Patches are applied against a virtual `autoscaling/v1.Scale` document
/// constructed from the parent resource — mirroring upstream behaviour where
/// the patch operates on the Scale shape, not the full Deployment / RS / SS
/// body. Only the new replica count is then written back into the parent's
/// `spec.replicas`; the rest of the parent is left untouched.
pub async fn patch_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    uri: Uri,
    headers: HeaderMap,
    Path((namespace, name)): Path<(String, String)>,
    body: String,
) -> Result<Response> {
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

    // Determine patch type from Content-Type. The normalize middleware preserves
    // the original patch content type in X-Original-Content-Type when normalising
    // to application/json for Axum compatibility.
    let content_type = headers
        .get("x-original-content-type")
        .or_else(|| headers.get("content-type"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/strategic-merge-patch+json");

    // Default to strategic merge patch when the content type isn't recognised —
    // `kubectl scale` uses StrategicMergePatchType, and SMP is a superset of
    // JSON merge patch for the simple `{spec:{replicas:N}}` shape that the
    // conformance test sends.
    let patch_type = PatchType::from_content_type(content_type.split(';').next().unwrap_or(""))
        .unwrap_or(PatchType::StrategicMergePatch);

    // Parse the patch body as JSON (JSON Patch is also JSON — an array of ops).
    let patch: Value = serde_json::from_str(&body)
        .map_err(|e| Error::InvalidResource(format!("Invalid patch body: {}", e)))?;

    // Get the current resource and apply patch with retry on conflict.
    // K8s PATCH reads the latest version and re-applies on RV mismatch.
    let key = build_key(&resource, Some(&namespace), &name);

    let mut updated_resource: Value = Value::Null;
    let mut last_err: Option<Error> = None;
    for _retry in 0..5 {
        let mut resource_obj: Value = state.storage.get(&key).await?;

        // Build the current Scale shape, apply the patch to it, then read back
        // the new replicas count. This handles strategic merge / JSON merge /
        // JSON Patch uniformly — including the RFC 6902 case
        // `[{"op":"replace","path":"/spec/replicas","value":N}]` which the
        // previous handler ignored (it only looked for `body.spec.replicas`
        // in the raw patch document).
        let current_scale = serde_json::to_value(extract_scale(
            &resource_obj,
            &namespace,
            &name,
            &group,
            &version,
        )?)
        .map_err(|e| Error::Internal(format!("Failed to serialize Scale: {}", e)))?;

        let patched_scale = apply_patch(&current_scale, &patch, patch_type.clone())
            .map_err(|e| Error::InvalidResource(format!("Failed to apply scale patch: {}", e)))?;

        let new_replicas = patched_scale
            .get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(|r| r.as_i64())
            .map(|r| r as i32);

        if let Some(replicas) = new_replicas {
            // Scale.spec.replicas must be non-negative (upstream ValidateScale).
            if replicas < 0 {
                return Err(Error::Invalid(vec![
                    rusternetes_common::validation::field::Error::invalid(
                        &rusternetes_common::validation::field::Path::new("spec").child("replicas"),
                        replicas,
                        "must be greater than or equal to 0",
                    ),
                ]));
            }
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
                last_err = None;
                break;
            }
            Err(Error::Conflict(msg)) => {
                last_err = Some(Error::Conflict(msg));
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    if updated_resource.is_null() {
        return Err(last_err
            .unwrap_or_else(|| Error::Conflict("scale patch failed after retries".to_string())));
    }

    // Extract and return the updated scale
    let updated_scale = extract_scale(&updated_resource, &namespace, &name, &group, &version)?;

    info!(
        "Successfully patched scale for {}/{}/{}/{}",
        group, resource, namespace, name
    );

    Ok(scale_response(&updated_scale, &headers, StatusCode::OK))
}

/// Build a Scale response that honors the client's Accept header.
///
/// Per upstream `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/`:
/// `application/vnd.kubernetes.protobuf` produces a `k8s\0`-prefixed Unknown
/// envelope carrying TypeMeta (`autoscaling/v1`, `Scale`) and the JSON-encoded
/// body. Anything else falls back to plain JSON. The Scale schema itself is
/// registered in [`crate::protobuf::ProtoRegistry::register_autoscaling_v1`]
/// for clients that decode the proto bytes back to a typed Go/Rust struct.
fn scale_response(scale: &Scale, headers: &HeaderMap, status: StatusCode) -> Response {
    match negotiate_content_type(headers) {
        ContentType::Protobuf => {
            // Encode the Scale as NATIVE protobuf via the registered schema.
            // The polymorphic scale client (k8s.io/client-go/scale) uses a
            // protobuf-only codec that proto-decodes `Unknown.raw` directly, so
            // the JSON-in-raw fallback fails with `proto: illegal wireType`.
            // `encode_native_or_wrapped` emits native bytes when the Scale
            // schema round-trips and only falls back to JSON if it can't.
            let json = match serde_json::to_vec(scale) {
                Ok(j) => j,
                Err(e) => {
                    tracing::warn!("scale JSON serialize failed; returning JSON body: {}", e);
                    return json_response(scale, status);
                }
            };
            let bytes =
                crate::response::encode_native_or_wrapped(&json, &scale.api_version, &scale.kind);
            Response::builder()
                .status(status)
                .header(header::CONTENT_TYPE, ContentType::Protobuf.mime_type())
                .body(Body::from(bytes))
                .unwrap()
        }
        ContentType::Json => json_response(scale, status),
    }
}

fn json_response(scale: &Scale, status: StatusCode) -> Response {
    match serde_json::to_vec(scale) {
        Ok(body) => Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, ContentType::Json.mime_type())
            .body(Body::from(body))
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Failed to serialize Scale: {}", e)))
            .unwrap(),
    }
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

    /// Apply a patch against the Scale shape and read back the new replicas count.
    /// Exercises the same logic that `patch_scale` runs against storage, but
    /// without the HTTP/storage scaffolding — useful for unit-testing each patch
    /// type in isolation.
    fn replicas_after_scale_patch(
        resource: &Value,
        patch: &Value,
        patch_type: PatchType,
    ) -> Option<i32> {
        let current_scale =
            serde_json::to_value(extract_scale(resource, "default", "d", "apps", "v1").unwrap())
                .unwrap();
        let patched = apply_patch(&current_scale, patch, patch_type).unwrap();
        patched
            .get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(|r| r.as_i64())
            .map(|r| r as i32)
    }

    fn sample_deployment(replicas: i32) -> Value {
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "d", "namespace": "default", "resourceVersion": "1"},
            "spec": {
                "replicas": replicas,
                "selector": {"matchLabels": {"app": "test"}}
            },
            "status": {"replicas": replicas}
        })
    }

    #[test]
    fn test_scale_patch_strategic_merge() {
        // kubectl scale uses StrategicMergePatchType with body
        // `{"spec":{"replicas":N}}` — this is what the "Deployment should have a
        // working scale subresource" conformance test sends in its final Patch
        // step.
        let resource = sample_deployment(2);
        let patch = json!({"spec": {"replicas": 4}});
        assert_eq!(
            replicas_after_scale_patch(&resource, &patch, PatchType::StrategicMergePatch),
            Some(4)
        );
    }

    #[test]
    fn test_scale_patch_json_merge() {
        // RFC 7386 merge patch — same wire shape as strategic merge for this case.
        let resource = sample_deployment(1);
        let patch = json!({"spec": {"replicas": 7}});
        assert_eq!(
            replicas_after_scale_patch(&resource, &patch, PatchType::JsonMergePatch),
            Some(7)
        );
    }

    #[test]
    fn test_scale_patch_json_patch_replace() {
        // RFC 6902 JSON Patch — the previous handler ignored the array shape
        // and would leave replicas unchanged. Now we apply the ops against the
        // virtual Scale document so /spec/replicas resolves correctly.
        let resource = sample_deployment(2);
        let patch = json!([
            {"op": "replace", "path": "/spec/replicas", "value": 5}
        ]);
        assert_eq!(
            replicas_after_scale_patch(&resource, &patch, PatchType::JsonPatch),
            Some(5)
        );
    }

    #[test]
    fn test_scale_patch_unrelated_fields_ignored() {
        // Patch must only affect replicas; touching status.selector etc. via the
        // Scale document must not write garbage back to the parent's spec.
        let resource = sample_deployment(3);
        let patch = json!({"status": {"selector": "ignored=true"}});
        // Strategic merge / json merge against the Scale shape will set
        // status.selector on the patched Scale object, but spec.replicas stays
        // unchanged — and our handler only writes new_replicas back, so the
        // parent resource is untouched.
        assert_eq!(
            replicas_after_scale_patch(&resource, &patch, PatchType::JsonMergePatch),
            Some(3)
        );
    }
}
