//! Integration-test mirror of upstream Kubernetes `TestConfigMap`.
//!
//! Source: <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/configmap/configmap_test.go>
//!
//! Upstream is a single monolithic test (`TestConfigMap` -> `DoTestConfigMap`)
//! that:
//!   1. boots an integration apiserver,
//!   2. creates a namespace (`config-map`),
//!   3. creates a ConfigMap named `configmap` with three data entries
//!      (`data-1`/`data-2`/`data-3` -> `value-1`/`value-2`/`value-3`),
//!   4. creates a Pod (`uses-configmap`) whose container env injects those
//!      keys via `ConfigMapKeyRef`,
//!   5. and tears both objects down via `deleteConfigMapOrErrorf` /
//!      `integration.DeletePodOrErrorf`.
//!
//! This Rust mirror keeps the same single-test shape (`test_configmap`) but
//! also exercises the natural CRUD + PATCH surface that the upstream
//! integration suite implicitly relies on (GET / PUT / PATCH / LIST / DELETE),
//! so a single RED-state failure here pins the whole ConfigMap REST contract.
//!
//! Harness: `Arc<MemoryStorage>` + `build_router(...)` + `tower::ServiceExt::oneshot`,
//! matching the pattern in
//! `crates/api-server/tests/conformance_apimachinery_admission_webhooks.rs`
//! and `crates/api-server/tests/patch_cas_retry_test.rs`. `skip_auth = true`
//! routes through `skip_auth_middleware`, so no bearer token is required.
//!
//! Part of the /batch landing upstream integration-test mirrors as RED-state
//! TDD pins.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// HTTP harness
// ---------------------------------------------------------------------------

/// Build a fully-wired `ApiServerState` backed by an in-memory storage. The
/// authorizer is `AlwaysAllow` and `skip_auth=true` so the router uses
/// `skip_auth_middleware` and no token is needed.
fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true, // skip_auth
    ))
}

/// Build a fresh router for every request — `oneshot` consumes the service.
fn router_for(state: &Arc<ApiServerState>) -> axum::Router {
    build_router(state.clone(), None)
}

async fn read_body(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, body)
}

async fn post_json(state: &Arc<ApiServerState>, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router_for(state).oneshot(req).await.unwrap();
    read_body(response).await
}

async fn get_json(state: &Arc<ApiServerState>, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = router_for(state).oneshot(req).await.unwrap();
    read_body(response).await
}

async fn put_json(state: &Arc<ApiServerState>, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router_for(state).oneshot(req).await.unwrap();
    read_body(response).await
}

async fn patch_json(state: &Arc<ApiServerState>, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("PATCH")
        .uri(uri)
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router_for(state).oneshot(req).await.unwrap();
    read_body(response).await
}

async fn delete_json(state: &Arc<ApiServerState>, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = router_for(state).oneshot(req).await.unwrap();
    read_body(response).await
}

// ---------------------------------------------------------------------------
// Mirror of upstream TestConfigMap / DoTestConfigMap
// ---------------------------------------------------------------------------

/// Mirror of upstream `TestConfigMap` (`test/integration/configmap/configmap_test.go`).
///
/// Upstream flow (verbatim from `DoTestConfigMap`):
///   - create ConfigMap `configmap` in namespace `config-map` with
///     `data: {data-1: value-1, data-2: value-2, data-3: value-3}`,
///   - create Pod `uses-configmap` referencing each key via `ConfigMapKeyRef`
///     for env vars `CONFIG_DATA_1`, `CONFIG_DATA_2`, `CONFIG_DATA_3`,
///   - deferred cleanup deletes the Pod then the ConfigMap.
///
/// Rust mirror keeps that exact create/use/delete arc and additionally pins
/// the CRUD + PATCH REST contract the upstream suite implicitly depends on:
///   GET (single + list), PUT (replace), PATCH (merge-patch), DELETE.
///
/// RED-state expectation: this test is intended to *pass* once the api-server
/// REST surface for ConfigMaps faithfully implements every leg below. Any
/// regression on namespace creation, configmap CRUD, patch semantics, or pod
/// admission with `configMapKeyRef` flips this red.
#[tokio::test]
async fn test_configmap() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);

    // -----------------------------------------------------------------------
    // Upstream: framework.CreateNamespaceOrDie(client, "config-map", t)
    // -----------------------------------------------------------------------
    let ns_name = "config-map";
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": ns_name }
    });
    let (status, body) = post_json(&state, "/api/v1/namespaces", &ns_body).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create must return 201/200, got {} body={}",
        status,
        body
    );

    // -----------------------------------------------------------------------
    // Upstream step 1: create ConfigMap "configmap" with three data entries.
    // -----------------------------------------------------------------------
    let cm_name = "configmap";
    let cm_body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": cm_name,
            "namespace": ns_name,
        },
        "data": {
            "data-1": "value-1",
            "data-2": "value-2",
            "data-3": "value-3",
        }
    });
    let cm_list_uri = format!("/api/v1/namespaces/{}/configmaps", ns_name);
    let cm_item_uri = format!("/api/v1/namespaces/{}/configmaps/{}", ns_name, cm_name);

    let (status, body) = post_json(&state, &cm_list_uri, &cm_body).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "ConfigMap create must return 201 Created (got {}, body={})",
        status,
        body
    );
    assert_eq!(body["metadata"]["name"], cm_name);
    assert_eq!(body["metadata"]["namespace"], ns_name);
    assert_eq!(body["data"]["data-1"], "value-1");
    assert_eq!(body["data"]["data-2"], "value-2");
    assert_eq!(body["data"]["data-3"], "value-3");
    assert!(
        !body["metadata"]["uid"].as_str().unwrap_or("").is_empty(),
        "server must assign a non-empty UID, got body={}",
        body
    );

    // GET — pin the read path.
    let (status, body) = get_json(&state, &cm_item_uri).await;
    assert_eq!(status, StatusCode::OK, "ConfigMap GET must return 200");
    assert_eq!(body["data"]["data-2"], "value-2");

    // LIST — pin the list path.
    let (status, body) = get_json(&state, &cm_list_uri).await;
    assert_eq!(status, StatusCode::OK, "ConfigMap LIST must return 200");
    let items = body["items"].as_array().expect("LIST must return .items");
    assert!(
        items.iter().any(|it| it["metadata"]["name"] == cm_name),
        "LIST must contain the just-created configmap, items={:?}",
        items
    );

    // -----------------------------------------------------------------------
    // Upstream step 2: create Pod "uses-configmap" that consumes the ConfigMap
    // via `configMapKeyRef` for env vars CONFIG_DATA_{1,2,3}.
    // -----------------------------------------------------------------------
    let pod_name = "uses-configmap";
    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": ns_name,
        },
        "spec": {
            "containers": [{
                "name": "use-configmap",
                "image": "registry.k8s.io/busybox",
                "env": [
                    {
                        "name": "CONFIG_DATA_1",
                        "valueFrom": {
                            "configMapKeyRef": {
                                "name": cm_name,
                                "key": "data-1",
                            }
                        }
                    },
                    {
                        "name": "CONFIG_DATA_2",
                        "valueFrom": {
                            "configMapKeyRef": {
                                "name": cm_name,
                                "key": "data-2",
                            }
                        }
                    },
                    {
                        "name": "CONFIG_DATA_3",
                        "valueFrom": {
                            "configMapKeyRef": {
                                "name": cm_name,
                                "key": "data-3",
                            }
                        }
                    },
                ]
            }]
        }
    });
    let pod_list_uri = format!("/api/v1/namespaces/{}/pods", ns_name);
    let pod_item_uri = format!("/api/v1/namespaces/{}/pods/{}", ns_name, pod_name);

    let (status, body) = post_json(&state, &pod_list_uri, &pod_body).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "Pod create must return 201 Created (got {}, body={})",
        status,
        body
    );
    let env = &body["spec"]["containers"][0]["env"];
    assert_eq!(env[0]["name"], "CONFIG_DATA_1");
    assert_eq!(env[0]["valueFrom"]["configMapKeyRef"]["name"], cm_name);
    assert_eq!(env[0]["valueFrom"]["configMapKeyRef"]["key"], "data-1");
    assert_eq!(env[1]["valueFrom"]["configMapKeyRef"]["key"], "data-2");
    assert_eq!(env[2]["valueFrom"]["configMapKeyRef"]["key"], "data-3");

    // -----------------------------------------------------------------------
    // Extra REST contract: PUT replaces the ConfigMap, PATCH merges into it.
    // Upstream relies on these working even though `DoTestConfigMap` itself
    // exercises only create + delete.
    // -----------------------------------------------------------------------
    let mut replace_body = cm_body.clone();
    replace_body["data"] = json!({
        "data-1": "value-1",
        "data-2": "value-2",
        "data-3": "value-3-replaced",
    });
    let (status, body) = put_json(&state, &cm_item_uri, &replace_body).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "ConfigMap PUT must return 200, got {} body={}",
        status,
        body
    );
    assert_eq!(body["data"]["data-3"], "value-3-replaced");

    // merge-patch: add data-4, mutate data-1.
    let patch = json!({
        "data": {
            "data-1": "value-1-patched",
            "data-4": "value-4",
        }
    });
    let (status, body) = patch_json(&state, &cm_item_uri, &patch).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "ConfigMap PATCH must return 200, got {} body={}",
        status,
        body
    );
    assert_eq!(body["data"]["data-1"], "value-1-patched");
    assert_eq!(body["data"]["data-2"], "value-2");
    assert_eq!(body["data"]["data-3"], "value-3-replaced");
    assert_eq!(body["data"]["data-4"], "value-4");

    // -----------------------------------------------------------------------
    // Upstream deferred cleanup:
    //   integration.DeletePodOrErrorf(t, client, ns.Name, pod.Name)
    //   deleteConfigMapOrErrorf(t, client, ns.Name, configMap.Name)
    // -----------------------------------------------------------------------
    let (status, _) = delete_json(&state, &pod_item_uri).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "Pod DELETE must return 200/202, got {}",
        status
    );

    let (status, _) = delete_json(&state, &cm_item_uri).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "ConfigMap DELETE must return 200/202, got {}",
        status
    );

    let (status, _) = get_json(&state, &cm_item_uri).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "GET on deleted ConfigMap must return 404, got {}",
        status
    );
}
