//! Test layer #5 (openapi-discovery-shape) of the upstream Kubernetes test
//! mirror. Pins the response shape of the OpenAPI v2/v3 endpoints and the
//! core discovery surface (`/api`, `/apis`, `/api/v1`, `/apis/<group>/<ver>`).
//!
//! Upstream coverage mirrored:
//!   * `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go::APIVersions`,
//!     `APIGroupList`, `APIResourceList` shapes.
//!   * `staging/src/k8s.io/kube-openapi/pkg/handler` (v2) and
//!     `staging/src/k8s.io/kube-openapi/pkg/handler3` (v3) — the public
//!     contract is "swagger" / "openapi" version key, `paths` map, and a
//!     `definitions` / `components.schemas` map.
//!
//! Each test spawns the in-process Axum router with a fresh `MemoryStorage`
//! and does a single `oneshot` GET, then asserts field presence + values on
//! the parsed JSON body. Byte-level diff against upstream is intentionally
//! out of scope here — those live in dedicated `byte_diff_*` mirrors.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::Value;

// Harness: `TestApiServer` (rusternetes-test-support) — `build_router` on
// `MemoryStorage` with `--skip-auth`, driven via `tower::oneshot`.
fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

async fn get_json(api: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    api.get(uri).await
}

// ---------------------------------------------------------------------------
// /openapi/v2 — Swagger 2.0 envelope.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_openapi_v2_shape() {
    let router = spawn_router();
    let (status, body) = get_json(&router, "/openapi/v2").await;
    assert_eq!(status, StatusCode::OK, "GET /openapi/v2 must return 200");

    assert_eq!(
        body.get("swagger").and_then(|v| v.as_str()),
        Some("2.0"),
        "swagger field must equal \"2.0\" (Swagger 2.0 envelope)"
    );

    let info = body
        .get("info")
        .expect("top-level `info` object must exist");
    assert!(
        info.is_object(),
        "`info` must be a JSON object, got {info:?}"
    );

    // `paths` MAY be empty when no CRDs are registered — only its presence as
    // an object is part of the contract.
    body.get("paths")
        .and_then(|v| v.as_object())
        .expect("top-level `paths` must be an object");

    let definitions = body
        .get("definitions")
        .and_then(|v| v.as_object())
        .expect("top-level `definitions` must be an object");
    assert!(
        !definitions.is_empty(),
        "`definitions` must be a non-empty object (baseline ObjectMeta/OwnerReference)"
    );
}

/// Pins the upstream contract that the published OpenAPI v2 spec includes a
/// schema definition for `io.k8s.api.core.v1.Pod`. Upstream kube-apiserver
/// publishes one definition per built-in GVK via kube-openapi's
/// `pkg/builder` — see `staging/src/k8s.io/kube-openapi/pkg/builder/openapi.go`.
///
/// Rusternetes publishes hand-written stubs for the most commonly referenced
/// built-in GVKs (Pod, Service, Node, Deployment, ...). See
/// `core_v1_builtin_definitions` in `crates/api-server/src/handlers/openapi.rs`.
#[tokio::test]
async fn test_openapi_v2_shape_includes_core_pod_definition() {
    let router = spawn_router();
    let (status, body) = get_json(&router, "/openapi/v2").await;
    assert_eq!(status, StatusCode::OK);
    let definitions = body
        .get("definitions")
        .and_then(|v| v.as_object())
        .expect("definitions object");
    assert!(
        definitions.contains_key("io.k8s.api.core.v1.Pod"),
        "definitions must contain `io.k8s.api.core.v1.Pod`; got keys {:?}",
        definitions.keys().collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// /openapi/v3 — root + sub-document.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_openapi_v3_shape() {
    let router = spawn_router();
    let (status, body) = get_json(&router, "/openapi/v3").await;
    assert_eq!(status, StatusCode::OK, "GET /openapi/v3 must return 200");

    let paths = body
        .get("paths")
        .and_then(|v| v.as_object())
        .expect("`paths` must be an object at the root of /openapi/v3");
    assert!(
        !paths.is_empty(),
        "`paths` must be non-empty (built-in group/versions must be listed)"
    );

    // Spot-check a built-in group/version key the openapi handler emits
    // (`apis/apps/v1`), per `handlers/openapi.rs::get_openapi_spec`.
    let apps_v1 = paths
        .get("apis/apps/v1")
        .expect("`paths['apis/apps/v1']` must be listed in /openapi/v3 root");
    let server_relative_url = apps_v1
        .get("serverRelativeURL")
        .and_then(|v| v.as_str())
        .expect("each sub-document entry must carry `serverRelativeURL`");
    assert!(
        server_relative_url.contains("apps/v1"),
        "serverRelativeURL must point at the apps/v1 sub-document, got {server_relative_url:?}"
    );
}

#[tokio::test]
async fn test_openapi_v3_subdocument_shape() {
    // First fetch the v3 root, then follow one of the listed sub-document
    // URLs and assert the body shape (`openapi: 3.0.x`, `paths`,
    // `components.schemas`).
    let router = spawn_router();
    let (status, root) = get_json(&router, "/openapi/v3").await;
    assert_eq!(status, StatusCode::OK);

    let paths = root
        .get("paths")
        .and_then(|v| v.as_object())
        .expect("/openapi/v3 root must have a `paths` map");
    let (gv_key, entry) = paths
        .iter()
        .next()
        .expect("/openapi/v3 root must list at least one group/version");
    let sub_url = entry
        .get("serverRelativeURL")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("sub-document {gv_key} has no serverRelativeURL"));
    assert!(
        sub_url.starts_with('/'),
        "serverRelativeURL must start with `/` (server-relative), got {sub_url:?}"
    );

    let (sub_status, sub) = get_json(&router, sub_url).await;
    assert_eq!(
        sub_status,
        StatusCode::OK,
        "GET {sub_url} must return 200; body={sub:?}"
    );

    let openapi_version = sub
        .get("openapi")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("sub-doc {sub_url} missing `openapi` version key: {sub:?}"));
    assert!(
        openapi_version.starts_with("3.0."),
        "sub-doc openapi version must be 3.0.x, got {openapi_version:?}"
    );

    assert!(
        sub.get("paths").is_some(),
        "sub-doc {sub_url} must have a `paths` map"
    );

    let components = sub
        .get("components")
        .expect("sub-doc must have a `components` map");
    let schemas = components
        .get("schemas")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("sub-doc {sub_url} missing components.schemas object"));
    assert!(
        !schemas.is_empty(),
        "sub-doc {sub_url} components.schemas must be non-empty"
    );
}

// ---------------------------------------------------------------------------
// /api — APIVersions.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_api_root_shape() {
    let router = spawn_router();
    let (status, body) = get_json(&router, "/api").await;
    assert_eq!(status, StatusCode::OK, "GET /api must return 200");

    assert_eq!(
        body.get("kind").and_then(|v| v.as_str()),
        Some("APIVersions"),
        "kind must be `APIVersions`"
    );

    let versions = body
        .get("versions")
        .and_then(|v| v.as_array())
        .expect("`versions` must be an array");
    let versions_strs: Vec<&str> = versions.iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(
        versions_strs,
        vec!["v1"],
        "versions must equal `[\"v1\"]`, got {versions_strs:?}"
    );

    let cidrs = body
        .get("serverAddressByClientCIDRs")
        .and_then(|v| v.as_array())
        .expect("`serverAddressByClientCIDRs` must be an array");
    assert!(
        !cidrs.is_empty(),
        "serverAddressByClientCIDRs must have at least one entry"
    );
}

// ---------------------------------------------------------------------------
// /apis — APIGroupList.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_apis_root_shape() {
    let router = spawn_router();
    let (status, body) = get_json(&router, "/apis").await;
    assert_eq!(status, StatusCode::OK, "GET /apis must return 200");

    assert_eq!(
        body.get("kind").and_then(|v| v.as_str()),
        Some("APIGroupList"),
        "kind must be `APIGroupList`"
    );

    let groups = body
        .get("groups")
        .and_then(|v| v.as_array())
        .expect("`groups` must be an array");
    let names: Vec<&str> = groups
        .iter()
        .filter_map(|g| g.get("name").and_then(|n| n.as_str()))
        .collect();

    for required in [
        "apps",
        "batch",
        "networking.k8s.io",
        "rbac.authorization.k8s.io",
    ] {
        assert!(
            names.contains(&required),
            "/apis groups must include {required:?}; got {names:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// /api/v1 — APIResourceList.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_api_v1_resources_shape() {
    let router = spawn_router();
    let (status, body) = get_json(&router, "/api/v1").await;
    assert_eq!(status, StatusCode::OK, "GET /api/v1 must return 200");

    assert_eq!(
        body.get("kind").and_then(|v| v.as_str()),
        Some("APIResourceList"),
        "kind must be `APIResourceList`"
    );
    assert_eq!(
        body.get("groupVersion").and_then(|v| v.as_str()),
        Some("v1"),
        "groupVersion must equal `v1`"
    );

    let resources = body
        .get("resources")
        .and_then(|v| v.as_array())
        .expect("`resources` must be an array");
    let names: Vec<&str> = resources
        .iter()
        .filter_map(|r| r.get("name").and_then(|n| n.as_str()))
        .collect();

    for required in [
        "pods",
        "services",
        "configmaps",
        "secrets",
        "namespaces",
        "nodes",
        "events",
    ] {
        assert!(
            names.contains(&required),
            "core/v1 resources must include {required:?}; got {names:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// /apis/apps/v1 — APIResourceList with full APIResource shape per entry.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_apis_apps_v1_resources_shape() {
    let router = spawn_router();
    let (status, body) = get_json(&router, "/apis/apps/v1").await;
    assert_eq!(status, StatusCode::OK, "GET /apis/apps/v1 must return 200");

    assert_eq!(
        body.get("kind").and_then(|v| v.as_str()),
        Some("APIResourceList"),
        "kind must be `APIResourceList`"
    );
    assert_eq!(
        body.get("groupVersion").and_then(|v| v.as_str()),
        Some("apps/v1"),
        "groupVersion must equal `apps/v1`"
    );

    let resources = body
        .get("resources")
        .and_then(|v| v.as_array())
        .expect("`resources` must be an array");

    let required = [
        "deployments",
        "replicasets",
        "statefulsets",
        "daemonsets",
        "controllerrevisions",
    ];
    for resource_name in required {
        let entry = resources
            .iter()
            .find(|r| r.get("name").and_then(|n| n.as_str()) == Some(resource_name))
            .unwrap_or_else(|| {
                let names: Vec<&str> = resources
                    .iter()
                    .filter_map(|r| r.get("name").and_then(|n| n.as_str()))
                    .collect();
                panic!("apps/v1 must list {resource_name:?}; got {names:?}");
            });

        // Each APIResource entry has the K8s-standard 5 required fields.
        // K8s ref: `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go`.
        assert!(
            entry.get("name").and_then(|v| v.as_str()).is_some(),
            "{resource_name}: missing `name`"
        );
        assert!(
            entry.get("singularName").and_then(|v| v.as_str()).is_some(),
            "{resource_name}: missing `singularName`"
        );
        assert!(
            entry.get("namespaced").and_then(|v| v.as_bool()).is_some(),
            "{resource_name}: missing or non-bool `namespaced`"
        );
        assert!(
            entry.get("kind").and_then(|v| v.as_str()).is_some(),
            "{resource_name}: missing `kind`"
        );
        assert!(
            entry
                .get("verbs")
                .and_then(|v| v.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false),
            "{resource_name}: missing or empty `verbs`"
        );
    }
}
