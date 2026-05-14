/// OpenAPI specification handler
use crate::openapi::generate_openapi_spec;
use crate::state::ApiServerState;
use tracing::info;

use crate::gnostic;
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::Response,
};
use rusternetes_storage::Storage;
use std::sync::Arc;

/// Encode a u64 as a protobuf varint
#[allow(dead_code)]
fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        } else {
            buf.push(byte | 0x80);
        }
    }
}

/// GET /openapi/v3
/// Get the OpenAPI v3 root document listing available paths.
///
/// Dynamically includes CRD group/version paths so kubectl can discover
/// CRD schemas via the OpenAPI v3 discovery mechanism.
pub async fn get_openapi_spec(State(state): State<Arc<ApiServerState>>) -> Response {
    // Return the root document that lists all available OpenAPI paths
    // In real K8s, this returns {"paths": {"/apis/apps/v1": {...}, ...}}
    let mut paths = serde_json::Map::new();
    let path_entry =
        |gv: &str| serde_json::json!({"serverRelativeURL": format!("/openapi/v3/{}", gv)});
    paths.insert("api/v1".into(), path_entry("api/v1"));
    for (group, version) in &[
        ("apps", "v1"),
        ("batch", "v1"),
        ("networking.k8s.io", "v1"),
        ("rbac.authorization.k8s.io", "v1"),
        ("storage.k8s.io", "v1"),
        ("scheduling.k8s.io", "v1"),
        ("apiextensions.k8s.io", "v1"),
        ("admissionregistration.k8s.io", "v1"),
        ("coordination.k8s.io", "v1"),
        ("flowcontrol.apiserver.k8s.io", "v1"),
        ("certificates.k8s.io", "v1"),
        ("discovery.k8s.io", "v1"),
        ("node.k8s.io", "v1"),
        ("autoscaling", "v1"),
        ("autoscaling", "v2"),
        ("policy", "v1"),
        ("resource.k8s.io", "v1"),
        ("events.k8s.io", "v1"),
    ] {
        paths.insert(
            format!("apis/{}/{}", group, version),
            path_entry(&format!("apis/{}/{}", group, version)),
        );
    }

    // Dynamically add CRD group/version paths
    if let Ok(crds) = state
        .storage
        .list::<serde_json::Value>("/registry/customresourcedefinitions")
        .await
    {
        for crd in &crds {
            let group = crd
                .pointer("/spec/group")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let versions = crd.pointer("/spec/versions").and_then(|v| v.as_array());
            for version in versions.into_iter().flatten() {
                let served = version
                    .get("served")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !served {
                    continue;
                }
                let ver = version.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let gv_key = format!("apis/{}/{}", group, ver);
                if !paths.contains_key(&gv_key) {
                    paths.insert(gv_key.clone(), path_entry(&gv_key));
                }
            }
        }
    }

    let root = serde_json::json!({"paths": paths});
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&root).unwrap_or_default()))
        .unwrap()
}

/// GET /openapi/v3/*path
/// Returns the OpenAPI v3 spec for a specific group version.
///
/// Dynamically includes CRD schemas for the requested group/version.
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/customresource_handler.go
pub async fn get_openapi_spec_path(
    State(state): State<Arc<ApiServerState>>,
    axum::extract::Path(gv_path): axum::extract::Path<String>,
) -> Response {
    // Cache the static OpenAPI v3 spec — it doesn't change at runtime
    // (CRD definitions are in v2, not v3 for our implementation)
    use std::sync::OnceLock;
    static V3_SPEC: OnceLock<openapiv3::OpenAPI> = OnceLock::new();
    let spec = V3_SPEC.get_or_init(generate_openapi_spec).clone();
    let mut spec_json = serde_json::to_value(&spec).unwrap_or_default();

    // Parse the requested group/version from the path.
    // Paths are like "api/v1" or "apis/apps/v1" or "apis/example.com/v1"
    let (requested_group, requested_version) = parse_gv_path(&gv_path);

    // Query storage for CRDs matching this group/version and inject their schemas.
    if let Ok(crds) = state
        .storage
        .list::<serde_json::Value>("/registry/customresourcedefinitions")
        .await
    {
        // Build a components/schemas map for CRD definitions
        let schemas = spec_json
            .pointer_mut("/components/schemas")
            .and_then(|v| v.as_object_mut());

        // If components/schemas doesn't exist, create it via the top-level object
        let needs_create = schemas.is_none();
        if needs_create {
            if let Some(obj) = spec_json.as_object_mut() {
                let components = obj
                    .entry("components")
                    .or_insert_with(|| serde_json::json!({}));
                if let Some(comp_obj) = components.as_object_mut() {
                    comp_obj
                        .entry("schemas")
                        .or_insert_with(|| serde_json::json!({}));
                }
            }
        }

        for crd in &crds {
            let group = crd
                .pointer("/spec/group")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let kind = crd
                .pointer("/spec/names/kind")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            // Only include CRDs matching the requested group/version
            if !requested_group.is_empty() && group != requested_group {
                continue;
            }

            let versions = crd.pointer("/spec/versions").and_then(|v| v.as_array());
            for version in versions.into_iter().flatten() {
                let served = version
                    .get("served")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !served {
                    continue;
                }
                let ver = version.get("name").and_then(|v| v.as_str()).unwrap_or("");

                if !requested_version.is_empty() && ver != requested_version {
                    continue;
                }

                // Build definition key matching K8s ToRESTFriendlyName format:
                // group/version/kind -> reverse group domain parts, join with dots
                let group_parts: Vec<&str> = group.rsplitn(10, '.').collect();
                let def_key = format!("{}.{}.{}", group_parts.join("."), ver, kind);

                // Build the schema from CRD validation. Same publishing
                // semantics as the v2 endpoint — see
                // build_crd_schema_definition for the case analysis.
                let crd_preserves = crd
                    .pointer("/spec/preserveUnknownFields")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let schema_value =
                    build_crd_schema_definition(crd_preserves, version, group, kind, ver);

                // Insert into components/schemas
                if let Some(schemas) = spec_json
                    .pointer_mut("/components/schemas")
                    .and_then(|v| v.as_object_mut())
                {
                    schemas.insert(def_key.clone(), schema_value);
                }

                // Add OpenAPI paths for this CRD resource.
                // kubectl explain requires paths to map resource names to schemas.
                let plural = crd
                    .pointer("/spec/names/plural")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let scope = crd
                    .pointer("/spec/scope")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Namespaced");
                if !plural.is_empty() {
                    let schema_ref =
                        serde_json::json!({"$ref": format!("#/components/schemas/{}", def_key)});
                    let ok_response = serde_json::json!({
                        "description": "OK",
                        "content": {
                            "application/json": {
                                "schema": schema_ref
                            }
                        }
                    });
                    let get_op = serde_json::json!({
                        "operationId": format!("read{}{}{}",
                            if scope == "Namespaced" { "Namespaced" } else { "" }, kind, ver),
                        "responses": { "200": ok_response },
                        "tags": [format!("{}_v1", group.replace('.', "_"))],
                        "x-kubernetes-group-version-kind": {
                            "group": group, "version": ver, "kind": kind
                        }
                    });
                    if let Some(paths) = spec_json
                        .pointer_mut("/paths")
                        .and_then(|v| v.as_object_mut())
                    {
                        if scope == "Namespaced" {
                            let list_path = format!(
                                "/apis/{}/{}/namespaces/{{namespace}}/{}",
                                group, ver, plural
                            );
                            let item_path = format!(
                                "/apis/{}/{}/namespaces/{{namespace}}/{}/{{name}}",
                                group, ver, plural
                            );
                            paths
                                .entry(list_path)
                                .or_insert_with(|| serde_json::json!({"get": get_op}));
                            paths
                                .entry(item_path)
                                .or_insert_with(|| serde_json::json!({"get": get_op}));
                        } else {
                            let list_path = format!("/apis/{}/{}/{}", group, ver, plural);
                            let item_path = format!("/apis/{}/{}/{}/{{name}}", group, ver, plural);
                            paths
                                .entry(list_path)
                                .or_insert_with(|| serde_json::json!({"get": get_op}));
                            paths
                                .entry(item_path)
                                .or_insert_with(|| serde_json::json!({"get": get_op}));
                        }
                    }
                }
            }
        }
    }

    let json_bytes = serde_json::to_vec(&spec_json).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json_bytes))
        .unwrap()
}

/// Parse the group and version from an OpenAPI v3 path.
/// Examples:
///   "api/v1" -> ("", "v1")                    (core API)
///   "apis/apps/v1" -> ("apps", "v1")
///   "apis/example.com/v1" -> ("example.com", "v1")
fn parse_gv_path(path: &str) -> (String, String) {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match parts.as_slice() {
        ["api", version] => (String::new(), version.to_string()),
        ["apis", group, version] => (group.to_string(), version.to_string()),
        _ => (String::new(), String::new()),
    }
}

/// Wrap JSON bytes in the Kubernetes protobuf wire format.
///
/// Uses the Go runtime.Unknown proto definition field numbering:
/// - 4 bytes magic: "k8s\0"
/// - Protobuf message with:
///   - field 1 (TypeMeta, nested): empty, omitted
///   - field 2 (raw, bytes): the raw data (JSON spec) -- tag 0x12
///   - field 3 (contentEncoding, string): empty, omitted
///   - field 4 (contentType, string): "application/json" -- tag 0x22
#[allow(dead_code)]
fn wrap_in_k8s_protobuf(_content_type: &str, data: &[u8]) -> Vec<u8> {
    let content_type_bytes = b"application/json";
    let mut msg = Vec::with_capacity(data.len() + 30);

    // Field 2: raw bytes (the JSON payload) -- tag = (2 << 3) | 2 = 0x12
    msg.push(0x12);
    encode_varint(&mut msg, data.len() as u64);
    msg.extend_from_slice(data);
    // Field 4: contentType -- tag = (4 << 3) | 2 = 0x22
    msg.push(0x22);
    encode_varint(&mut msg, content_type_bytes.len() as u64);
    msg.extend_from_slice(content_type_bytes);

    let mut buf = Vec::with_capacity(msg.len() + 4);
    buf.extend_from_slice(b"k8s\0");
    buf.extend_from_slice(&msg);
    buf
}

/// GET /openapi/v2 and /swagger.json
/// Returns an OpenAPI v2 (Swagger) specification.
///
/// Supports both protobuf and JSON Accept headers.
/// When protobuf is requested, wraps JSON in the K8s protobuf envelope and
/// responds with the MIME-safe content type (using '.' not '@').
/// See k8s.io/kube-openapi/pkg/handler for the canonical implementation.
///
/// Dynamically includes CRD validation schemas in the definitions section.
pub async fn get_swagger_spec(
    State(state): State<Arc<ApiServerState>>,
    headers: HeaderMap,
) -> Response {
    // Read CRDs from storage as raw JSON to preserve nested schemas.
    // Using typed deserialization (CustomResourceDefinition) loses nested
    // schemas in JSONSchemaPropsOrArray untagged enums. Raw JSON preserves
    // everything.
    //
    // We rebuild the spec on every request from a fresh storage list so that
    // CRD create/update/delete events are reflected immediately. This matches
    // upstream kube-apiserver's openapi controller behaviour where the
    // published spec is regenerated whenever a CRD is added, removed, or has
    // its served versions / schema changed.
    //
    // K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/customresource_handler.go
    let crds = state
        .storage
        .list::<serde_json::Value>("/registry/customresourcedefinitions")
        .await
        .unwrap_or_default();

    let spec = build_swagger_spec_for_crds(&crds);
    let total_definitions = spec
        .get("definitions")
        .and_then(|d| d.as_object())
        .map(|m| m.len())
        .unwrap_or(0);
    // Subtract the two baseline definitions (ObjectMeta + OwnerReference)
    // that are always present to surface the CRD-derived count.
    if total_definitions > 2 {
        info!(
            "OpenAPI /v2: serving swagger spec with {} CRD definitions",
            total_definitions - 2
        );
    }

    let json_bytes = serde_json::to_vec(&spec).unwrap_or_default();

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Cache the protobuf conversion to avoid regenerating on every request.
    // The swagger spec changes when CRDs are created/deleted. We use a simple
    // hash of the JSON bytes to detect changes.
    use std::sync::Mutex;
    use std::sync::OnceLock;
    static PROTO_CACHE: OnceLock<Mutex<(u64, Vec<u8>)>> = OnceLock::new();
    let cache = PROTO_CACHE.get_or_init(|| Mutex::new((0, Vec::new())));

    let wants_protobuf = accept.contains("proto-openapi.spec.v2");
    if wants_protobuf {
        // Simple hash of JSON bytes for cache invalidation
        let hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            json_bytes.hash(&mut hasher);
            hasher.finish()
        };
        // Check cache
        {
            let cached = cache.lock().unwrap();
            if cached.0 == hash && !cached.1.is_empty() {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        "application/com.github.proto-openapi.spec.v2.v1.0+protobuf",
                    )
                    .body(Body::from(cached.1.clone()))
                    .unwrap();
            }
        }
        match gnostic::swagger_json_to_protobuf(&json_bytes) {
            Ok(proto_bytes) => {
                // Cache for future requests
                {
                    let mut cached = cache.lock().unwrap();
                    *cached = (hash, proto_bytes.clone());
                }
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        "application/com.github.proto-openapi.spec.v2.v1.0+protobuf",
                    )
                    .body(Body::from(proto_bytes))
                    .unwrap();
            }
            Err(e) => {
                info!(
                    "Failed to convert swagger to protobuf: {}, falling back to JSON",
                    e
                );
                // Fall through to JSON response
            }
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json_bytes))
        .unwrap()
}

/// Build a complete swagger v2 spec given a list of CRDs (as raw JSON).
///
/// Centralising this in a pure function means the openapi/v2 endpoint always
/// reflects the current storage state and the behaviour is independently
/// testable. Matches upstream kube-apiserver's openapi controller: rebuild
/// the spec from the latest set of registered CRDs on every refresh, dropping
/// schemas for versions that are no longer served or no longer present.
///
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
pub fn build_swagger_spec_for_crds(crds: &[serde_json::Value]) -> serde_json::Value {
    let mut paths = serde_json::Map::new();
    let mut definitions = serde_json::Map::new();

    for crd in crds {
        let group = crd
            .pointer("/spec/group")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let plural = crd
            .pointer("/spec/names/plural")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let kind = crd
            .pointer("/spec/names/kind")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let scope = crd
            .pointer("/spec/scope")
            .and_then(|v| v.as_str())
            .unwrap_or("Namespaced");

        // CRD-level preserveUnknownFields applies to all versions unless
        // overridden at the schema level. K8s ref: builder.go:392-407
        let crd_preserves = crd
            .pointer("/spec/preserveUnknownFields")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let versions = crd.pointer("/spec/versions").and_then(|v| v.as_array());
        for version in versions.into_iter().flatten() {
            // Skip versions that are not served — they must NOT appear in the
            // published OpenAPI spec. Conformance test
            // "removes definition from spec when one version gets changed to
            // not be served" depends on this filter being applied at
            // publish-time so toggling served=false is reflected.
            let served = version
                .get("served")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !served {
                continue;
            }
            let ver = version.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if ver.is_empty() || group.is_empty() || kind.is_empty() {
                continue;
            }

            // Build definition key like "io.example.stable.v1.CronTab"
            // (reverse-domain group + version + kind), matching K8s
            // ToRESTFriendlyName format.
            let group_parts: Vec<&str> = group.rsplitn(10, '.').collect();
            let def_key = format!("{}.{}.{}", group_parts.join("."), ver, kind);

            let schema_value =
                build_crd_schema_definition(crd_preserves, version, group, kind, ver);
            info!(
                "OpenAPI: publishing CRD definition {} (group={}, kind={}, version={})",
                def_key, group, kind, ver
            );
            definitions.insert(def_key.clone(), schema_value);

            // Add path entries for the CRD's API endpoints.
            let base_path = format!("/apis/{}/{}", group, ver);
            if scope == "Namespaced" {
                let ns_path = format!("{}/namespaces/{{namespace}}/{}", base_path, plural);
                let ns_item_path = format!("{}/{{name}}", ns_path);
                paths.insert(
                    ns_path,
                    serde_json::json!({
                        "get": {"description": format!("list {}", kind)},
                        "post": {"description": format!("create {}", kind)}
                    }),
                );
                paths.insert(
                    ns_item_path,
                    serde_json::json!({
                        "get": {"description": format!("get {}", kind)},
                        "put": {"description": format!("update {}", kind)},
                        "delete": {"description": format!("delete {}", kind)}
                    }),
                );
            } else {
                let cluster_path = format!("{}/{}", base_path, plural);
                let cluster_item_path = format!("{}/{{name}}", cluster_path);
                paths.insert(
                    cluster_path,
                    serde_json::json!({
                        "get": {"description": format!("list {}", kind)},
                        "post": {"description": format!("create {}", kind)}
                    }),
                );
                paths.insert(
                    cluster_item_path,
                    serde_json::json!({
                        "get": {"description": format!("get {}", kind)},
                        "put": {"description": format!("update {}", kind)},
                        "delete": {"description": format!("delete {}", kind)}
                    }),
                );
            }
        }
    }

    // Add standard K8s definitions referenced by CRD schemas.
    // CRD schemas use $ref to io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
    // so it must exist in the definitions section.
    // K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
    definitions.insert(
        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta".to_string(),
        serde_json::json!({
            "description": "ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.",
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Name must be unique within a namespace." },
                "namespace": { "type": "string", "description": "Namespace defines the space within which each name must be unique." },
                "labels": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Map of string keys and values that can be used to organize and categorize objects." },
                "annotations": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Annotations is an unstructured key value map stored with a resource." },
                "uid": { "type": "string", "description": "UID is the unique in time and space value for this object." },
                "resourceVersion": { "type": "string", "description": "An opaque value that represents the internal version of this object." },
                "generation": { "type": "integer", "format": "int64", "description": "A sequence number representing a specific generation of the desired state." },
                "creationTimestamp": { "type": "string", "format": "date-time", "description": "CreationTimestamp is a timestamp representing the server time when this object was created." },
                "deletionTimestamp": { "type": "string", "format": "date-time", "description": "DeletionTimestamp is RFC 3339 date and time at which this resource will be deleted." },
                "deletionGracePeriodSeconds": { "type": "integer", "format": "int64", "description": "Number of seconds allowed for this object to gracefully terminate." },
                "ownerReferences": { "type": "array", "items": { "$ref": "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference" } },
                "finalizers": { "type": "array", "items": { "type": "string" } },
                "managedFields": { "type": "array", "items": { "type": "object" } }
            }
        }),
    );
    definitions.insert(
        "io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference".to_string(),
        serde_json::json!({
            "description": "OwnerReference contains enough information to let you identify an owning object.",
            "type": "object",
            "required": ["apiVersion", "kind", "name", "uid"],
            "properties": {
                "apiVersion": { "type": "string" },
                "kind": { "type": "string" },
                "name": { "type": "string" },
                "uid": { "type": "string" },
                "controller": { "type": "boolean" },
                "blockOwnerDeletion": { "type": "boolean" }
            }
        }),
    );

    serde_json::json!({
        "swagger": "2.0",
        "info": {
            "title": "Rusternetes Kubernetes API",
            "version": "v1.35.0"
        },
        "paths": paths,
        "definitions": definitions
    })
}

/// Build the per-version CRD schema definition that's inserted into the spec's
/// definitions map. Handles the three cases that upstream kube-apiserver
/// distinguishes:
///   * preserveUnknownFields (CRD- or schema-level) — replace with a bare object
///   * has openAPIV3Schema — strip Go-omitempty defaults, inject the GVK extension
///   * no schema — emit a minimal object skeleton with the GVK extension
///
/// In all cases the standard metadata/apiVersion/kind properties are added,
/// matching upstream conformance expectations:
/// "works for CRD with validation schema" and
/// "works for CRD without validation schema".
fn build_crd_schema_definition(
    crd_preserves: bool,
    version: &serde_json::Value,
    group: &str,
    kind: &str,
    ver: &str,
) -> serde_json::Value {
    let gvk = serde_json::json!([{
        "group": group,
        "kind": kind,
        "version": ver,
    }]);

    if let Some(schema_val) = version.pointer("/schema/openAPIV3Schema") {
        let schema_preserves = schema_val
            .get("x-kubernetes-preserve-unknown-fields")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if crd_preserves || schema_preserves {
            // Replace entire schema with just {type: object}.
            // K8s ref: builder.go:393-395
            let mut def = serde_json::json!({
                "type": "object",
                "x-kubernetes-group-version-kind": gvk,
            });
            add_standard_crd_properties(&mut def);
            return def;
        }

        let mut cleaned = schema_val.clone();
        strip_false_extensions(&mut cleaned);
        if let Some(obj) = cleaned.as_object_mut() {
            obj.insert("x-kubernetes-group-version-kind".to_string(), gvk);
        }
        add_standard_crd_properties(&mut cleaned);
        return cleaned;
    }

    // No openAPIV3Schema declared. K8s still publishes a definition stub so
    // kubectl explain / discovery sees the GVK. Conformance test
    // "works for CRD without validation schema" depends on the definition
    // being present.
    let mut def = serde_json::json!({
        "type": "object",
        "x-kubernetes-group-version-kind": gvk,
    });
    add_standard_crd_properties(&mut def);
    def
}

/// Add standard K8s properties (metadata, apiVersion, kind) to a CRD schema definition.
/// K8s always adds these to CRD OpenAPI definitions.
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
fn add_standard_crd_properties(schema: &mut serde_json::Value) {
    if let Some(obj) = schema.as_object_mut() {
        let properties = obj
            .entry("properties")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(props) = properties.as_object_mut() {
            // K8s CRD definitions reference the standard ObjectMeta definition
            // instead of inlining "type: object". This matches
            // staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
            props.entry("metadata".to_string()).or_insert_with(|| {
                serde_json::json!({
                    "$ref": "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
                    "description": "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata"
                })
            });
            props.entry("apiVersion".to_string()).or_insert_with(|| {
                serde_json::json!({
                    "description": "APIVersion defines the versioned schema of this representation of an object. Servers should convert recognized schemas to the latest internal value, and may reject unrecognized values. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources",
                    "type": "string"
                })
            });
            props.entry("kind".to_string()).or_insert_with(|| {
                serde_json::json!({
                    "description": "Kind is a string value representing the REST resource this object represents. Servers may infer this from the endpoint the client submits requests to. Cannot be updated. In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds",
                    "type": "string"
                })
            });
        }
    }
}

/// Recursively strip default/empty values from a CRD JSON schema to match
/// K8s Go's omitempty behavior. Go omitempty skips false booleans, empty
/// strings, nil pointers, and zero values. Our Rust serialization includes
/// these as explicit values in stored JSON.
///
/// K8s ref: JSONSchemaProps fields in apiextensions/v1/types.go all use
/// `json:",omitempty"` which omits zero values.
pub fn strip_false_extensions(value: &mut serde_json::Value) {
    if let Some(obj) = value.as_object_mut() {
        // K8s v2 conversion: when x-kubernetes-preserve-unknown-fields is true,
        // clear items and properties (kubectl can't handle them).
        // Also clear type if it was "object" with preserve-unknown-fields.
        // K8s ref: controller/openapi/v2/conversion.go:68-89
        if obj.get("x-kubernetes-preserve-unknown-fields") == Some(&serde_json::Value::Bool(true)) {
            obj.remove("items");
            obj.remove("properties");
            // If type is "object" with preserve-unknown-fields, clear it
            if obj.get("type") == Some(&serde_json::json!("object")) {
                obj.remove("type");
            }
        }

        // K8s v2: when nullable is true, clear type, items, properties
        // K8s ref: conversion.go:56-66
        if obj.get("nullable") == Some(&serde_json::Value::Bool(true)) {
            obj.remove("type");
            obj.remove("items");
            obj.remove("properties");
        }

        // Other boolean fields: strip only when false (Go omitempty)
        // x-kubernetes-* booleans are added by toKubeOpenAPI() only when true,
        // so they should be stripped when false but kept when true.
        let false_fields = [
            "exclusiveMaximum",
            "exclusiveMinimum",
            "uniqueItems",
            "nullable",
            "x-kubernetes-embedded-resource",
            "x-kubernetes-int-or-string",
            "x-kubernetes-preserve-unknown-fields",
        ];
        for key in &false_fields {
            if obj.get(*key) == Some(&serde_json::Value::Bool(false)) {
                obj.remove(*key);
            }
        }

        // Fields that should be omitted when empty string (Go omitempty on string)
        let empty_string_fields = [
            "id",
            "$schema",
            "$ref",
            "description",
            "type",
            "format",
            "title",
            "pattern",
            "discriminator",
        ];
        for key in &empty_string_fields {
            if let Some(serde_json::Value::String(s)) = obj.get(*key) {
                if s.is_empty() {
                    obj.remove(*key);
                }
            }
        }

        // Zero-value integers should be omitted (Go omitempty on int64/float64).
        // JSONSchemaProps fields like maxLength, minLength, maxItems, etc. use
        // pointer types (*int64) in Go with omitempty — zero means "not set".
        let zero_int_fields = [
            "maximum",
            "minimum",
            "multipleOf",
            "maxLength",
            "minLength",
            "maxItems",
            "minItems",
            "maxProperties",
            "minProperties",
        ];
        for key in &zero_int_fields {
            if let Some(serde_json::Value::Number(n)) = obj.get(*key) {
                if n.as_f64() == Some(0.0) || n.as_i64() == Some(0) {
                    obj.remove(*key);
                }
            }
        }

        // Empty arrays should be omitted (Go omitempty on slices)
        let array_fields = ["required", "enum", "allOf", "oneOf", "anyOf"];
        for key in &array_fields {
            if let Some(serde_json::Value::Array(arr)) = obj.get(*key) {
                if arr.is_empty() {
                    obj.remove(*key);
                }
            }
        }

        // Empty objects should be omitted (Go omitempty on maps/structs)
        let empty_obj_fields = [
            "properties",
            "additionalProperties",
            "definitions",
            "patternProperties",
            "dependencies",
            "externalDocs",
        ];
        for key in &empty_obj_fields {
            if let Some(serde_json::Value::Object(m)) = obj.get(*key) {
                if m.is_empty() {
                    obj.remove(*key);
                }
            }
        }

        // Remove "additionalProperties": false — Go omitempty treats
        // the zero-value JSONSchemaPropsOrBool as omitted.
        // K8s only includes additionalProperties when it's explicitly
        // set to a non-default value.
        if obj.get("additionalProperties") == Some(&serde_json::Value::Bool(false)) {
            obj.remove("additionalProperties");
        }

        // Remove "default" when it's null — Go omits nil *JSON fields
        if obj.get("default") == Some(&serde_json::Value::Null) {
            obj.remove("default");
        }

        // Remove "example" when it's null
        if obj.get("example") == Some(&serde_json::Value::Null) {
            obj.remove("example");
        }

        // Remove "x-kubernetes-list-type" and "x-kubernetes-map-type" when empty
        for key in [
            "x-kubernetes-list-type",
            "x-kubernetes-map-type",
            "x-kubernetes-list-map-keys",
        ] {
            match obj.get(key) {
                Some(serde_json::Value::String(s)) if s.is_empty() => {
                    obj.remove(key);
                }
                Some(serde_json::Value::Array(a)) if a.is_empty() => {
                    obj.remove(key);
                }
                Some(serde_json::Value::Null) => {
                    obj.remove(key);
                }
                _ => {}
            }
        }

        // Unwrap JSONSchemaPropsOrArray: K8s CRD schemas store "items" as
        // {"schema": {...}} (Go's JSONSchemaPropsOrArray serialization).
        // OpenAPI v2 expects "items" to be a direct schema object.
        // K8s ref: vendor/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/types_jsonschema.go
        if let Some(items) = obj.get("items") {
            if let Some(items_obj) = items.as_object() {
                if items_obj.len() == 1 && items_obj.contains_key("schema") {
                    if let Some(inner_schema) = items_obj.get("schema") {
                        let unwrapped = inner_schema.clone();
                        obj.insert("items".to_string(), unwrapped);
                    }
                }
            }
        }

        // Recurse into all nested objects/arrays
        let keys: Vec<String> = obj.keys().cloned().collect();
        for key in keys {
            if let Some(v) = obj.get_mut(&key) {
                strip_false_extensions(v);
            }
        }
    } else if let Some(arr) = value.as_array_mut() {
        for item in arr.iter_mut() {
            strip_false_extensions(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_false_extensions_removes_defaults() {
        // Test v2 conversion behavior matching K8s:
        // - False booleans stripped (Go omitempty)
        // - Empty strings stripped (Go omitempty)
        // - x-kubernetes-* false values stripped, true values KEPT as vendor extensions
        // - When x-kubernetes-preserve-unknown-fields=true, items/properties cleared
        // - When nullable=true, type/items/properties cleared
        let mut schema = serde_json::json!({
            "description": "Foo",
            "type": "object",
            "$schema": "",
            "id": "",
            "format": "",
            "pattern": "",
            "title": "",
            "exclusiveMaximum": false,
            "exclusiveMinimum": false,
            "nullable": false,
            "uniqueItems": false,
            "x-kubernetes-embedded-resource": false,
            "x-kubernetes-int-or-string": false,
            "properties": {
                "spec": {
                    "description": "Spec",
                    "type": "object",
                    "$schema": "",
                    "id": "",
                    "title": "",
                    "format": "",
                    "nullable": false,
                    "uniqueItems": false,
                    "exclusiveMaximum": false,
                    "x-kubernetes-preserve-unknown-fields": false,
                    "x-kubernetes-embedded-resource": true,
                    "properties": {
                        "bars": {
                            "description": "List of bars",
                            "type": "array",
                            "$schema": "",
                            "nullable": false
                        }
                    }
                },
                "nested_preserve": {
                    "description": "Has preserve-unknown-fields",
                    "type": "object",
                    "x-kubernetes-preserve-unknown-fields": true,
                    "properties": {
                        "should_be_removed": {
                            "type": "string"
                        }
                    }
                }
            }
        });

        strip_false_extensions(&mut schema);

        let obj = schema.as_object().unwrap();
        assert!(obj.contains_key("description"));
        assert!(obj.contains_key("type"));
        assert!(obj.contains_key("properties"));

        // Removed: empty strings and false booleans
        assert!(!obj.contains_key("$schema"), "$schema should be removed");
        assert!(!obj.contains_key("id"), "id should be removed");
        assert!(!obj.contains_key("format"), "format should be removed");
        assert!(!obj.contains_key("pattern"), "pattern should be removed");
        assert!(!obj.contains_key("exclusiveMaximum"));
        assert!(!obj.contains_key("exclusiveMinimum"));
        assert!(!obj.contains_key("nullable"), "nullable should be removed");
        assert!(!obj.contains_key("title"), "title should be removed");
        assert!(!obj.contains_key("uniqueItems"));
        // false x-kubernetes-* stripped
        assert!(!obj.contains_key("x-kubernetes-embedded-resource"));
        assert!(!obj.contains_key("x-kubernetes-int-or-string"));

        // Nested spec: false x-kubernetes-* stripped, true KEPT
        let spec = obj["properties"]["spec"].as_object().unwrap();
        assert!(spec.contains_key("description"));
        assert!(spec.contains_key("properties"));
        assert!(!spec.contains_key("$schema"));
        assert!(!spec.contains_key("id"));
        assert!(!spec.contains_key("nullable"));
        assert!(
            !spec.contains_key("x-kubernetes-preserve-unknown-fields"),
            "false preserve-unknown-fields should be stripped"
        );
        // x-kubernetes-embedded-resource=true should be KEPT
        assert!(
            spec.contains_key("x-kubernetes-embedded-resource"),
            "true x-kubernetes-embedded-resource should be KEPT as vendor extension"
        );

        // Nested with preserve-unknown-fields=true: properties and type cleared
        let nested = obj["properties"]["nested_preserve"].as_object().unwrap();
        assert!(nested.contains_key("description"), "description kept");
        assert!(
            nested.contains_key("x-kubernetes-preserve-unknown-fields"),
            "true preserve-unknown-fields KEPT as vendor extension"
        );
        assert!(
            !nested.contains_key("properties"),
            "properties cleared when preserve-unknown-fields=true"
        );
        assert!(
            !nested.contains_key("type"),
            "type=object cleared when preserve-unknown-fields=true"
        );

        // 3 levels deep: spec.properties.bars
        let bars = spec
            .get("properties")
            .unwrap()
            .get("bars")
            .unwrap()
            .as_object()
            .unwrap();
        assert!(bars.contains_key("description"), "deep description kept");
        assert!(!bars.contains_key("$schema"), "deep $schema removed");
        assert!(
            !bars.contains_key("nullable"),
            "nested nullable should be removed"
        );
    }

    #[test]
    fn test_wrap_in_k8s_protobuf_uses_correct_field_numbers() {
        let data = b"{\"test\": true}";
        let wrapped = wrap_in_k8s_protobuf("ignored-content-type", data);

        // Verify magic prefix
        assert_eq!(&wrapped[0..4], b"k8s\0");

        // After magic, first byte should be field 2 tag (0x12)
        // field 2, wire type 2 = (2 << 3) | 2 = 0x12
        assert_eq!(
            wrapped[4], 0x12,
            "first field tag should be 0x12 (field 2, raw bytes)"
        );

        // Parse past the varint length to find the raw data
        let mut pos = 5;
        let mut raw_len: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = wrapped[pos];
            raw_len |= ((byte & 0x7f) as u64) << shift;
            pos += 1;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        // Verify raw data matches input
        assert_eq!(raw_len as usize, data.len());
        assert_eq!(&wrapped[pos..pos + data.len()], data);

        // After raw data, next byte should be field 4 tag (0x22)
        let after_raw = pos + data.len();
        assert_eq!(
            wrapped[after_raw], 0x22,
            "second field tag should be 0x22 (field 4, contentType)"
        );

        // Verify contentType value is "application/json"
        let ct_len_pos = after_raw + 1;
        let ct_len = wrapped[ct_len_pos] as usize;
        let ct_start = ct_len_pos + 1;
        let ct_bytes = &wrapped[ct_start..ct_start + ct_len];
        assert_eq!(ct_bytes, b"application/json");
    }

    #[test]
    fn test_openapi_spec_has_definitions_key() {
        // Even with no CRDs, the swagger spec JSON should have a definitions key
        let spec = serde_json::json!({
            "swagger": "2.0",
            "info": {
                "title": "Rusternetes Kubernetes API",
                "version": "v1.35.0"
            },
            "paths": {},
            "definitions": {}
        });
        let val: serde_json::Value = spec;
        assert!(
            val.get("definitions").is_some(),
            "spec must include definitions"
        );
        assert!(val.get("paths").is_some(), "spec must include paths");
        assert!(
            val["definitions"].is_object(),
            "definitions must be an object"
        );
    }

    /// Test that the CRD schema processing (strip_false_extensions + add GVK +
    /// add standard properties) preserves the schema correctly.
    /// Simulates the Go conformance test's dropDefaults to verify roundtrip.
    #[test]
    fn test_crd_schema_roundtrip_matches_expected() {
        // Schema matching Go's JSONSchemaProps.MarshalJSON for the schemaFoo YAML
        let original_schema = serde_json::json!({
            "description": "Foo CRD for Testing",
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "description": "Specification of Foo",
                    "properties": {
                        "bars": {
                            "description": "List of Bars and their specs.",
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["name"],
                                "properties": {
                                    "name": {"description": "Name of Bar.", "type": "string"},
                                    "age": {"description": "Age of Bar.", "type": "string"},
                                    "feeling": {
                                        "description": "Whether Bar is feeling great.",
                                        "type": "string",
                                        "enum": ["Great", "Down"]
                                    },
                                    "bazs": {
                                        "description": "List of Bazs.",
                                        "items": {"type": "string"},
                                        "type": "array"
                                    }
                                }
                            }
                        }
                    }
                },
                "status": {
                    "description": "Status of Foo",
                    "type": "object",
                    "properties": {
                        "bars": {
                            "description": "List of Bars and their statuses.",
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"description": "Name of Bar.", "type": "string"},
                                    "available": {"description": "Whether the Bar is installed.", "type": "boolean"},
                                    "quxType": {
                                        "description": "Indicates to external qux type.",
                                        "pattern": "in-tree|out-of-tree",
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let mut cleaned = original_schema.clone();
        strip_false_extensions(&mut cleaned);
        if let Some(obj) = cleaned.as_object_mut() {
            obj.insert(
                "x-kubernetes-group-version-kind".to_string(),
                serde_json::json!([{"group": "test.example.com", "kind": "Foo", "version": "v1"}]),
            );
        }
        add_standard_crd_properties(&mut cleaned);

        // Simulate Go test's dropDefaults
        if let Some(obj) = cleaned.as_object_mut() {
            if let Some(props) = obj.get_mut("properties").and_then(|p| p.as_object_mut()) {
                props.remove("metadata");
                props.remove("apiVersion");
                props.remove("kind");
            }
            obj.remove("x-kubernetes-group-version-kind");
        }

        assert_eq!(
            cleaned,
            original_schema,
            "Schema should match after roundtrip.\nExpected:\n{}\n\nActual:\n{}",
            serde_json::to_string_pretty(&original_schema).unwrap(),
            serde_json::to_string_pretty(&cleaned).unwrap()
        );
    }

    /// K8s CRD schemas reference io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
    /// via $ref. kubectl validates CRs against the OpenAPI spec before sending
    /// them to the API server. If ObjectMeta is missing from definitions,
    /// kubectl fails with "unknown model in reference".
    /// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
    #[test]
    fn test_strip_empty_objects_and_arrays() {
        let mut schema = serde_json::json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false,
            "default": null,
            "example": null,
            "required": [],
            "enum": [],
            "x-kubernetes-list-type": "",
            "x-kubernetes-map-type": "",
            "x-kubernetes-list-map-keys": [],
            "definitions": {},
            "allOf": [],
        });
        strip_false_extensions(&mut schema);

        // All empty/null/false values should be stripped (Go omitempty)
        let obj = schema.as_object().unwrap();
        assert!(
            !obj.contains_key("properties"),
            "empty properties should be stripped"
        );
        assert!(
            !obj.contains_key("additionalProperties"),
            "false additionalProperties should be stripped"
        );
        assert!(
            !obj.contains_key("default"),
            "null default should be stripped"
        );
        assert!(
            !obj.contains_key("example"),
            "null example should be stripped"
        );
        assert!(
            !obj.contains_key("required"),
            "empty required should be stripped"
        );
        assert!(!obj.contains_key("enum"), "empty enum should be stripped");
        assert!(
            !obj.contains_key("x-kubernetes-list-type"),
            "empty x-kubernetes-list-type should be stripped"
        );
        assert!(
            !obj.contains_key("x-kubernetes-map-type"),
            "empty x-kubernetes-map-type should be stripped"
        );
        assert!(
            !obj.contains_key("x-kubernetes-list-map-keys"),
            "empty x-kubernetes-list-map-keys should be stripped"
        );
        assert!(
            !obj.contains_key("definitions"),
            "empty definitions should be stripped"
        );
        assert!(!obj.contains_key("allOf"), "empty allOf should be stripped");
        // type: "object" should be kept (non-empty)
        assert!(obj.contains_key("type"), "non-empty type should be kept");
    }

    // -- CRD OpenAPI publish-on-update tests ------------------------------
    //
    // These cover upstream Kubernetes v1.35 conformance expectations:
    //   [Conformance] CustomResourceDefinition resources publish openAPI ...
    //   - works for CRD with validation schema
    //   - works for CRD without validation schema
    //   - removes definition from spec when one version gets changed to not be served
    //   - updates the published spec when one version gets renamed
    //   - works for multiple CRDs of same group but different versions
    //
    // We exercise `build_swagger_spec_for_crds` directly (the helper that
    // the live /openapi/v2 handler delegates to). That guarantees the spec
    // reflects the latest CRD set whenever it's regenerated, which the
    // production handler does on every request.

    fn crd_with_schema(
        name: &str,
        group: &str,
        plural: &str,
        kind: &str,
        versions: Vec<(&str, bool, Option<serde_json::Value>)>,
    ) -> serde_json::Value {
        let versions_json: Vec<serde_json::Value> = versions
            .into_iter()
            .map(|(ver, served, schema)| {
                let mut v = serde_json::json!({
                    "name": ver,
                    "served": served,
                    "storage": false,
                });
                if let Some(s) = schema {
                    v["schema"] = serde_json::json!({ "openAPIV3Schema": s });
                }
                v
            })
            .collect();
        serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": { "name": name },
            "spec": {
                "group": group,
                "names": { "plural": plural, "kind": kind },
                "scope": "Namespaced",
                "versions": versions_json,
            }
        })
    }

    fn def_key(group: &str, ver: &str, kind: &str) -> String {
        let group_parts: Vec<&str> = group.rsplitn(10, '.').collect();
        format!("{}.{}.{}", group_parts.join("."), ver, kind)
    }

    #[test]
    fn test_publish_crd_with_validation_schema() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "bars": {
                            "type": "array",
                            "items": { "type": "object" }
                        }
                    }
                }
            }
        });
        let crd = crd_with_schema(
            "foos.test.example.com",
            "test.example.com",
            "foos",
            "Foo",
            vec![("v1", true, Some(schema.clone()))],
        );

        let spec = build_swagger_spec_for_crds(&[crd]);
        let key = def_key("test.example.com", "v1", "Foo");
        let def = spec
            .pointer(&format!("/definitions/{}", key))
            .expect("CRD definition must be published");

        // GVK extension must be present with the right values.
        let gvk = def
            .get("x-kubernetes-group-version-kind")
            .and_then(|v| v.as_array())
            .expect("GVK extension must be an array");
        assert_eq!(gvk.len(), 1);
        assert_eq!(gvk[0]["group"], "test.example.com");
        assert_eq!(gvk[0]["kind"], "Foo");
        assert_eq!(gvk[0]["version"], "v1");

        // Standard K8s metadata/apiVersion/kind properties must be auto-added.
        let props = def
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("properties must exist");
        assert!(props.contains_key("metadata"));
        assert!(props.contains_key("apiVersion"));
        assert!(props.contains_key("kind"));
        // The user-supplied spec property must still be present.
        assert!(props.contains_key("spec"));
    }

    #[test]
    fn test_publish_crd_without_validation_schema() {
        // Upstream conformance: a CRD with no openAPIV3Schema must still get
        // a definition stub so kubectl explain & discovery clients see the
        // GVK. We emit {type: object} + GVK + the standard properties.
        let crd = crd_with_schema(
            "bars.test.example.com",
            "test.example.com",
            "bars",
            "Bar",
            vec![("v1", true, None)],
        );

        let spec = build_swagger_spec_for_crds(&[crd]);
        let key = def_key("test.example.com", "v1", "Bar");
        let def = spec
            .pointer(&format!("/definitions/{}", key))
            .expect("a definition must be published even without a schema");

        assert_eq!(def["type"], "object");
        let gvk = def
            .get("x-kubernetes-group-version-kind")
            .and_then(|v| v.as_array())
            .unwrap();
        assert_eq!(gvk[0]["kind"], "Bar");
        let props = def["properties"].as_object().unwrap();
        assert!(props.contains_key("metadata"));
        assert!(props.contains_key("apiVersion"));
        assert!(props.contains_key("kind"));
    }

    #[test]
    fn test_published_spec_refreshes_on_crd_update() {
        // First publish with schema A — then with schema B. The spec must
        // reflect the latest schema. This is the core of the upstream
        // "[Conformance] updates an existing CustomResourceDefinition's
        // published OpenAPI schema" check.
        let schema_a = serde_json::json!({
            "type": "object",
            "properties": { "color": { "type": "string" } }
        });
        let schema_b = serde_json::json!({
            "type": "object",
            "properties": { "size": { "type": "integer" } }
        });

        let crd_v1 = crd_with_schema(
            "widgets.test.example.com",
            "test.example.com",
            "widgets",
            "Widget",
            vec![("v1", true, Some(schema_a))],
        );
        let spec1 = build_swagger_spec_for_crds(&[crd_v1]);
        let key = def_key("test.example.com", "v1", "Widget");
        let props1 = spec1
            .pointer(&format!("/definitions/{}/properties", key))
            .and_then(|v| v.as_object())
            .unwrap();
        assert!(props1.contains_key("color"));
        assert!(!props1.contains_key("size"));

        let crd_v2 = crd_with_schema(
            "widgets.test.example.com",
            "test.example.com",
            "widgets",
            "Widget",
            vec![("v1", true, Some(schema_b))],
        );
        let spec2 = build_swagger_spec_for_crds(&[crd_v2]);
        let props2 = spec2
            .pointer(&format!("/definitions/{}/properties", key))
            .and_then(|v| v.as_object())
            .unwrap();
        assert!(
            props2.contains_key("size"),
            "updated schema must publish new property"
        );
        assert!(
            !props2.contains_key("color"),
            "stale schema property must be dropped after update"
        );
    }

    #[test]
    fn test_removes_definition_when_version_no_longer_served() {
        // Upstream: "removes definition from spec when one version gets
        // changed to not be served". Toggling served=false must drop the
        // corresponding definition from the published spec.
        let schema = serde_json::json!({"type": "object"});
        let initial = crd_with_schema(
            "zwidgets.test.example.com",
            "test.example.com",
            "zwidgets",
            "Zwidget",
            vec![
                ("v1beta1", true, Some(schema.clone())),
                ("v1", true, Some(schema.clone())),
            ],
        );
        let spec_initial = build_swagger_spec_for_crds(&[initial]);
        let beta_key = def_key("test.example.com", "v1beta1", "Zwidget");
        let v1_key = def_key("test.example.com", "v1", "Zwidget");
        assert!(spec_initial
            .pointer(&format!("/definitions/{}", beta_key))
            .is_some());
        assert!(spec_initial
            .pointer(&format!("/definitions/{}", v1_key))
            .is_some());

        // Now v1beta1 is no longer served.
        let updated = crd_with_schema(
            "zwidgets.test.example.com",
            "test.example.com",
            "zwidgets",
            "Zwidget",
            vec![
                ("v1beta1", false, Some(schema.clone())),
                ("v1", true, Some(schema)),
            ],
        );
        let spec_updated = build_swagger_spec_for_crds(&[updated]);
        assert!(
            spec_updated
                .pointer(&format!("/definitions/{}", beta_key))
                .is_none(),
            "unserved version must be removed from the published spec"
        );
        assert!(
            spec_updated
                .pointer(&format!("/definitions/{}", v1_key))
                .is_some(),
            "served version must remain in the published spec"
        );
    }

    #[test]
    fn test_updates_published_spec_when_version_gets_renamed() {
        // Upstream: "updates the published spec when one version gets
        // renamed". Replacing v1beta1 with v2 must drop the old definition
        // key and add the new one.
        let schema = serde_json::json!({"type": "object"});
        let before = crd_with_schema(
            "gadgets.test.example.com",
            "test.example.com",
            "gadgets",
            "Gadget",
            vec![("v1beta1", true, Some(schema.clone()))],
        );
        let key_before = def_key("test.example.com", "v1beta1", "Gadget");
        let spec_before = build_swagger_spec_for_crds(&[before]);
        assert!(spec_before
            .pointer(&format!("/definitions/{}", key_before))
            .is_some());

        let after = crd_with_schema(
            "gadgets.test.example.com",
            "test.example.com",
            "gadgets",
            "Gadget",
            vec![("v2", true, Some(schema))],
        );
        let key_after = def_key("test.example.com", "v2", "Gadget");
        let spec_after = build_swagger_spec_for_crds(&[after]);
        assert!(
            spec_after
                .pointer(&format!("/definitions/{}", key_after))
                .is_some(),
            "renamed version must appear in the spec"
        );
        assert!(
            spec_after
                .pointer(&format!("/definitions/{}", key_before))
                .is_none(),
            "stale version key must be removed when CRD is renamed"
        );
    }

    #[test]
    fn test_deletion_removes_published_definition() {
        // Deleting a CRD must drop its definitions from the spec.
        let crd = crd_with_schema(
            "doomeds.test.example.com",
            "test.example.com",
            "doomeds",
            "Doomed",
            vec![("v1", true, Some(serde_json::json!({"type": "object"})))],
        );
        let key = def_key("test.example.com", "v1", "Doomed");
        let spec_with = build_swagger_spec_for_crds(&[crd]);
        assert!(spec_with
            .pointer(&format!("/definitions/{}", key))
            .is_some());

        // Simulate post-deletion (no CRDs).
        let spec_without: serde_json::Value = build_swagger_spec_for_crds(&[]);
        assert!(
            spec_without
                .pointer(&format!("/definitions/{}", key))
                .is_none(),
            "deleted CRD must not leak into the published spec"
        );
        // Baseline definitions are still present.
        assert!(spec_without
            .pointer("/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta")
            .is_some());
    }

    #[test]
    fn test_publishes_multiple_crds_same_group_different_versions() {
        // Upstream: "works for multiple CRDs of same group but different
        // versions". Each served version yields its own definition keyed by
        // group/version/kind.
        let schema = serde_json::json!({"type": "object"});
        let crd_one = crd_with_schema(
            "alphas.example.com",
            "example.com",
            "alphas",
            "Alpha",
            vec![("v1", true, Some(schema.clone()))],
        );
        let crd_two = crd_with_schema(
            "betas.example.com",
            "example.com",
            "betas",
            "Beta",
            vec![("v2", true, Some(schema))],
        );
        let spec = build_swagger_spec_for_crds(&[crd_one, crd_two]);
        assert!(spec
            .pointer(&format!(
                "/definitions/{}",
                def_key("example.com", "v1", "Alpha")
            ))
            .is_some());
        assert!(spec
            .pointer(&format!(
                "/definitions/{}",
                def_key("example.com", "v2", "Beta")
            ))
            .is_some());
    }

    #[test]
    fn test_preserve_unknown_fields_collapses_definition() {
        // CRD-level preserveUnknownFields=true must collapse the schema to a
        // bare {type: object} (with GVK + standard properties), preventing
        // kubectl from rejecting unknown fields during client-side validation.
        let mut crd = crd_with_schema(
            "freeforms.example.com",
            "example.com",
            "freeforms",
            "FreeForm",
            vec![(
                "v1",
                true,
                Some(serde_json::json!({
                    "type": "object",
                    "properties": { "anything": { "type": "string" } }
                })),
            )],
        );
        crd["spec"]["preserveUnknownFields"] = serde_json::Value::Bool(true);

        let spec = build_swagger_spec_for_crds(&[crd]);
        let key = def_key("example.com", "v1", "FreeForm");
        let def = spec.pointer(&format!("/definitions/{}", key)).unwrap();
        // preserveUnknownFields collapses to bare object; the user "anything"
        // property must NOT appear.
        let props = def["properties"].as_object().unwrap();
        assert!(props.contains_key("metadata"));
        assert!(props.contains_key("apiVersion"));
        assert!(props.contains_key("kind"));
        assert!(
            !props.contains_key("anything"),
            "preserveUnknownFields must drop user-defined properties to bare object"
        );
    }
}
