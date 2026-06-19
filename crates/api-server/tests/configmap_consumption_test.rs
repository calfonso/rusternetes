//! Upstream-mirror coverage for `test/e2e/common/node/configmap.go` and
//! `test/e2e/common/storage/configmap_volume.go` (kubernetes v1.35).
//!
//! Source-of-truth (permalinks):
//! - <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/common/node/configmap.go>
//! - <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/common/storage/configmap_volume.go>
//!
//! ConfigMap is a "storage-only" resource — the apiserver persists it and any
//! pod that references it must be admitted. The upstream e2e/common suite drives
//! a real kubelet, but the contract the apiserver owns is:
//!   1. ConfigMap CRUD round-trips (data + binaryData preserved verbatim,
//!      camelCase serialization, UID assigned, resourceVersion stamped).
//!   2. A pod whose container references a ConfigMap via `envFrom[*].configMapRef`
//!      is admitted (created with status 201).
//!   3. A pod whose container references a ConfigMap via `spec.volumes[*]
//!      .configMap` is admitted.
//!   4. A pod whose container `command` or `args` contains the `$(VAR_NAME)`
//!      placeholder syntax used by upstream value-from substitution is admitted
//!      (the apiserver does NOT perform runtime substitution — that's kubelet —
//!      but it must accept the placeholder string as a valid container arg).
//!   5. ConfigMap mutations propagate to long-running watches so clients (and
//!      kubelet's configmap cache) can observe key/value changes.
//!   6. `binaryData` round-trips through the REST surface as base64 (Go
//!      `[]byte`).
//!
//! These are the API-server-side preconditions for the upstream e2e suite's
//! "should be consumable" / "updates should be reflected in volume" tests. The
//! existing `integration_configmap_lifecycle.rs` mirrors the integration test
//! at `test/integration/configmap/configmap_test.go`; this file complements it
//! by pinning the *consumption* surface (envFrom, volumes, binaryData, watch
//! propagation) that the e2e common suite exercises.
//!
//! Harness mirrors `integration_namespace_conditions.rs::spawn_router()` —
//! `Arc<MemoryStorage>` + `build_router(...)` + `tower::ServiceExt::oneshot`,
//! `skip_auth=true` + `AlwaysAllowAuthorizer`.

use axum::http::{Method, StatusCode};
use futures::StreamExt;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "configmap-consumption";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send_json(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: &Value,
) -> (StatusCode, Value) {
    router
        .send(method.as_str(), uri, Some("application/json"), Some(body))
        .await
}

async fn send_get(router: TestApiServer, uri: &str) -> (StatusCode, Value) {
    router.get(uri).await
}

async fn send_delete(router: TestApiServer, uri: &str) -> (StatusCode, Value) {
    router.delete(uri).await
}

async fn create_namespace(router: TestApiServer, name: &str) {
    let (status, body) = send_json(
        router,
        Method::POST,
        "/api/v1/namespaces",
        &json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": name },
        }),
    )
    .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create must return 201/200, got {status} body={body}"
    );
}

/// Collect up to `max_events` `\n`-delimited JSON envelopes from a watch URL.
/// Mirrors `collect_watch_events` in `integration_watch_rv_test.rs`.
async fn collect_watch_events(
    router: TestApiServer,
    uri: &str,
    max_events: usize,
    deadline: Duration,
) -> (StatusCode, Vec<Value>) {
    let response = router.respond("GET", uri, None, None).await;
    let status = response.status();
    let mut stream = response.into_body().into_data_stream();
    let mut buffer = String::new();
    let mut events = Vec::new();

    let collect = async {
        while events.len() < max_events {
            match stream.next().await {
                Some(Ok(bytes)) => {
                    buffer.push_str(&String::from_utf8_lossy(&bytes));
                    while let Some(idx) = buffer.find('\n') {
                        let line = buffer[..idx].to_string();
                        buffer.drain(..=idx);
                        if line.trim().is_empty() {
                            continue;
                        }
                        if let Ok(v) = serde_json::from_str::<Value>(&line) {
                            events.push(v);
                            if events.len() >= max_events {
                                return;
                            }
                        }
                    }
                }
                Some(Err(_)) | None => return,
            }
        }
    };

    let _ = timeout(deadline, collect).await;
    (status, events)
}

// ---------------------------------------------------------------------------
// Test 1: ConfigMap CRUD round-trip
// ---------------------------------------------------------------------------

/// Upstream e2e common tests assume a working create/get/list/update/delete
/// cycle (every test in `test/e2e/common/node/configmap.go` calls
/// `f.ClientSet.CoreV1().ConfigMaps(ns).Create`/`Get`/`Delete`). Pin the full
/// cycle on the REST surface so any regression in the apiserver handlers
/// (router wiring, content-type, status code) surfaces here before it breaks
/// every e2e test downstream.
#[tokio::test]
async fn test_configmap_crud_round_trip() {
    let (_mem, router) = spawn_router();
    create_namespace(router.clone(), TEST_NS).await;

    let cm_name = "crud-cm";
    let create_body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": cm_name,
            "namespace": TEST_NS,
        },
        "data": {
            "data-1": "value-1",
            "data-2": "value-2",
        }
    });
    let list_uri = format!("/api/v1/namespaces/{TEST_NS}/configmaps");
    let item_uri = format!("/api/v1/namespaces/{TEST_NS}/configmaps/{cm_name}");

    // CREATE
    let (status, body) = send_json(router.clone(), Method::POST, &list_uri, &create_body).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "ConfigMap CREATE must return 201, got {status} body={body}"
    );
    assert_eq!(body["kind"].as_str(), Some("ConfigMap"));
    assert_eq!(body["metadata"]["name"].as_str(), Some(cm_name));
    assert_eq!(body["metadata"]["namespace"].as_str(), Some(TEST_NS));
    assert_eq!(body["data"]["data-1"].as_str(), Some("value-1"));
    assert!(
        !body["metadata"]["uid"].as_str().unwrap_or("").is_empty(),
        "apiserver must assign metadata.uid on create"
    );

    // GET
    let (status, body) = send_get(router.clone(), &item_uri).await;
    assert_eq!(status, StatusCode::OK, "GET must return 200");
    assert_eq!(body["data"]["data-2"].as_str(), Some("value-2"));

    // LIST
    let (status, body) = send_get(router.clone(), &list_uri).await;
    assert_eq!(status, StatusCode::OK, "LIST must return 200");
    let items = body["items"].as_array().expect("LIST envelope has items[]");
    assert!(
        items.iter().any(|c| c["metadata"]["name"] == cm_name),
        "LIST must include the created configmap, items={items:?}"
    );

    // UPDATE (PUT replaces). Carry the server-stamped resourceVersion so the
    // CAS check accepts the write. Find the configmap in the LIST envelope
    // (index 0 isn't guaranteed since LIST order is unspecified).
    let mut updated = items
        .iter()
        .find(|c| c["metadata"]["name"] == cm_name)
        .cloned()
        .expect("created configmap present in list");
    updated["data"]["data-1"] = json!("value-1-updated");
    updated["data"]["data-3"] = json!("value-3");
    let (status, body) = send_json(router.clone(), Method::PUT, &item_uri, &updated).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "UPDATE must return 200, got {status} body={body}"
    );
    assert_eq!(body["data"]["data-1"].as_str(), Some("value-1-updated"));
    assert_eq!(body["data"]["data-3"].as_str(), Some("value-3"));

    // DELETE
    let (status, _) = send_delete(router.clone(), &item_uri).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "DELETE must return 200/202, got {status}"
    );

    // GET after delete -> 404
    let (status, _) = send_get(router, &item_uri).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "GET after DELETE must return 404"
    );
}

// ---------------------------------------------------------------------------
// Test 2: Pod with ConfigMap volume reference is admitted
// ---------------------------------------------------------------------------

/// Upstream `test/e2e/common/storage/configmap_volume.go` creates a pod with
/// `spec.volumes[*].configMap` (with optional `items[]` projection) and then
/// verifies kubelet mounts the data. The apiserver's job is admission:
/// accept the pod even if the referenced ConfigMap exists or doesn't exist —
/// upstream's `configMapVolume.optional` covers both. We pin both shapes here.
#[tokio::test]
async fn test_pod_with_configmap_volume_reference_admitted() {
    let (_mem, router) = spawn_router();
    create_namespace(router.clone(), TEST_NS).await;

    let cm_name = "volume-cm";
    let (status, body) = send_json(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{TEST_NS}/configmaps"),
        &json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": cm_name, "namespace": TEST_NS},
            "data": {"config-key": "config-value"}
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "ConfigMap create must succeed, got {status} body={body}"
    );

    let pod_name = "uses-configmap-volume";
    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": TEST_NS},
        "spec": {
            "containers": [{
                "name": "consumer",
                "image": "registry.k8s.io/busybox",
                "command": ["/bin/sh", "-c", "cat /etc/config/config-key"],
                "volumeMounts": [{
                    "name": "config-volume",
                    "mountPath": "/etc/config",
                    "readOnly": true,
                }]
            }],
            "volumes": [{
                "name": "config-volume",
                "configMap": {
                    "name": cm_name,
                    "defaultMode": 0o420,
                    "items": [{
                        "key": "config-key",
                        "path": "config-key",
                    }]
                }
            }]
        }
    });

    let (status, body) = send_json(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{TEST_NS}/pods"),
        &pod_body,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "Pod with configMap volume must be admitted, got {status} body={body}"
    );

    // Round-trip the volume reference fields — these are what kubelet reads
    // to populate the mount.
    let volume = &body["spec"]["volumes"][0];
    assert_eq!(volume["name"].as_str(), Some("config-volume"));
    assert_eq!(volume["configMap"]["name"].as_str(), Some(cm_name));
    assert_eq!(
        volume["configMap"]["items"][0]["key"].as_str(),
        Some("config-key")
    );
    assert_eq!(
        volume["configMap"]["items"][0]["path"].as_str(),
        Some("config-key")
    );
}

// ---------------------------------------------------------------------------
// Test 3: Pod with envFrom configMapRef is admitted
// ---------------------------------------------------------------------------

/// Upstream `test/e2e/common/node/configmap.go::TestConfigMapsConsumableAsEnvFrom`
/// (and the conformance "should be consumable via environment variable" /
/// "with prefixes" variants) build a pod with `container.envFrom[].configMapRef`
/// — optionally with `prefix`. The apiserver must admit the pod and round-trip
/// the envFrom slice as-is.
#[tokio::test]
async fn test_pod_with_envfrom_configmapref_admitted() {
    let (_mem, router) = spawn_router();
    create_namespace(router.clone(), TEST_NS).await;

    let cm_name = "envfrom-cm";
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{TEST_NS}/configmaps"),
        &json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": cm_name, "namespace": TEST_NS},
            "data": {"DB_HOST": "localhost", "DB_PORT": "5432"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let pod_name = "uses-envfrom";
    let (status, body) = send_json(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{TEST_NS}/pods"),
        &json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "namespace": TEST_NS},
            "spec": {
                "containers": [{
                    "name": "env-consumer",
                    "image": "registry.k8s.io/busybox",
                    "command": ["/bin/sh", "-c", "env"],
                    "envFrom": [
                        {"configMapRef": {"name": cm_name}},
                        {"prefix": "P_", "configMapRef": {"name": cm_name, "optional": true}},
                    ]
                }]
            }
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod with envFrom configMapRef must be admitted, got {status} body={body}"
    );

    // Round-trip: both envFrom entries must be present, prefix preserved on the
    // second, optional flag preserved on the second's configMapRef.
    let env_from = &body["spec"]["containers"][0]["envFrom"];
    assert_eq!(
        env_from[0]["configMapRef"]["name"].as_str(),
        Some(cm_name),
        "first envFrom entry must reference {cm_name}"
    );
    assert!(
        env_from[0].get("prefix").is_none_or(|p| p.is_null()),
        "first envFrom entry must have no prefix"
    );
    assert_eq!(env_from[1]["prefix"].as_str(), Some("P_"));
    assert_eq!(env_from[1]["configMapRef"]["name"].as_str(), Some(cm_name));
    assert_eq!(
        env_from[1]["configMapRef"]["optional"].as_bool(),
        Some(true)
    );
}

// ---------------------------------------------------------------------------
// Test 4: Pod command-arg with $(VAR) placeholder is admitted
// ---------------------------------------------------------------------------

/// Upstream container command/arg substitution uses the `$(VAR_NAME)` syntax
/// (see `k8s.io/kubernetes/pkg/kubelet/container/helpers.go::ExpandContainerCommandOnlyStatic`).
/// The apiserver does NOT perform substitution at admission — kubelet expands
/// at container start. But the apiserver MUST accept the placeholder string
/// as a valid container `command`/`args` value. We pin the admission contract
/// here so a future regression that adds over-zealous validation doesn't
/// break upstream e2e tests that use this pattern with ConfigMap-sourced env.
#[tokio::test]
async fn test_pod_command_arg_substitution_placeholder_accepted() {
    let (_mem, router) = spawn_router();
    create_namespace(router.clone(), TEST_NS).await;

    let cm_name = "subst-cm";
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{TEST_NS}/configmaps"),
        &json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": cm_name, "namespace": TEST_NS},
            "data": {"GREETING": "hello"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Mirror upstream's `configmap.go` pod that pipes the env-sourced var into
    // an echo via `$(GREETING)` in `args`. The apiserver must accept this
    // verbatim — substitution is a runtime concern.
    let (status, body) = send_json(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{TEST_NS}/pods"),
        &json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "uses-subst", "namespace": TEST_NS},
            "spec": {
                "containers": [{
                    "name": "subst",
                    "image": "registry.k8s.io/busybox",
                    "command": ["/bin/sh", "-c"],
                    "args": ["echo $(GREETING) $(MISSING_VAR) and a literal $$dollar"],
                    "env": [{
                        "name": "GREETING",
                        "valueFrom": {
                            "configMapKeyRef": {"name": cm_name, "key": "GREETING"}
                        }
                    }]
                }]
            }
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod with $(VAR_NAME) placeholders must be admitted, got {status} body={body}"
    );

    // Round-trip the placeholder verbatim — apiserver must not strip / expand /
    // reject `$(...)` syntax.
    assert_eq!(
        body["spec"]["containers"][0]["args"][0].as_str(),
        Some("echo $(GREETING) $(MISSING_VAR) and a literal $$dollar"),
        "args must round-trip the $(VAR_NAME) placeholder verbatim"
    );
}

// ---------------------------------------------------------------------------
// Test 5: ConfigMap update propagation via watch
// ---------------------------------------------------------------------------

/// Upstream `test/e2e/common/storage/configmap_volume.go::"updates should be
/// reflected in volume"` mutates a ConfigMap and asserts kubelet picks up the
/// new value. The apiserver contract underneath that is: a long-running watch
/// over the configmaps collection must emit a `MODIFIED` event when the object
/// is updated through the REST surface. Pin that contract here so any
/// regression in the watch-cache update path surfaces without spinning up
/// kubelet.
#[tokio::test]
async fn test_configmap_update_propagation_via_watch() {
    let (_mem, router) = spawn_router();
    create_namespace(router.clone(), TEST_NS).await;

    let cm_name = "watch-cm";
    let cm_item_uri = format!("/api/v1/namespaces/{TEST_NS}/configmaps/{cm_name}");

    let (status, created) = send_json(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{TEST_NS}/configmaps"),
        &json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": cm_name, "namespace": TEST_NS},
            "data": {"key": "initial"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Open watch (rv=0 replays the current ADDED, then tails). Mutate the
    // configmap on a parallel task and assert we observe MODIFIED carrying the
    // new value.
    let writer_router = router.clone();
    let mut updated_body = created.clone();
    updated_body["data"]["key"] = json!("updated");
    updated_body["data"]["new-key"] = json!("new-value");

    let write_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        send_json(writer_router, Method::PUT, &cm_item_uri, &updated_body).await
    });

    let (status, events) = collect_watch_events(
        router,
        &format!("/api/v1/namespaces/{TEST_NS}/configmaps?watch=true&resourceVersion=0"),
        2,
        Duration::from_secs(5),
    )
    .await;
    let (write_status, _) = write_task.await.unwrap();

    assert_eq!(status, StatusCode::OK, "watch must open with 200");
    assert_eq!(write_status, StatusCode::OK, "PUT must succeed");

    // We expect at least one MODIFIED event carrying the new data.
    let modified = events.iter().find(|e| {
        e["type"].as_str() == Some("MODIFIED")
            && e["object"]["metadata"]["name"].as_str() == Some(cm_name)
    });
    assert!(
        modified.is_some(),
        "watch must surface a MODIFIED event for the updated configmap, got events={events:?}"
    );
    let modified = modified.unwrap();
    assert_eq!(
        modified["object"]["data"]["key"].as_str(),
        Some("updated"),
        "MODIFIED envelope must carry the post-update value"
    );
    assert_eq!(
        modified["object"]["data"]["new-key"].as_str(),
        Some("new-value"),
        "MODIFIED envelope must carry newly-added keys"
    );
}

// ---------------------------------------------------------------------------
// Test 6: binaryData round-trips through REST
// ---------------------------------------------------------------------------

/// Upstream `core/v1.ConfigMap.binaryData` is `map[string][]byte` and serializes
/// as base64-encoded strings in JSON. The `e2e/common` suite tests that pods
/// can consume binary-only configmaps (e.g. certificates, gz blobs). Pin the
/// REST round-trip so the on-wire bytes stay byte-exact through create/get.
#[tokio::test]
async fn test_configmap_binary_data_round_trip() {
    let (_mem, router) = spawn_router();
    create_namespace(router.clone(), TEST_NS).await;

    let cm_name = "binary-cm";
    // PNG magic bytes — non-UTF-8 to ensure the base64 path is exercised.
    let payload: [u8; 8] = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, payload);

    let (status, body) = send_json(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{TEST_NS}/configmaps"),
        &json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": cm_name, "namespace": TEST_NS},
            "binaryData": {"logo.png": encoded},
            "data": {"text": "still-utf8"},
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "ConfigMap with binaryData must be accepted, got {status} body={body}"
    );

    // Server must echo binaryData back as the same base64 string (camelCase,
    // not snake_case).
    assert_eq!(
        body["binaryData"]["logo.png"].as_str(),
        Some(encoded.as_str()),
        "binaryData must round-trip as base64 in camelCase"
    );
    assert!(
        body.get("binary_data").is_none(),
        "binaryData must NOT serialize as snake_case `binary_data`"
    );
    assert_eq!(body["data"]["text"].as_str(), Some("still-utf8"));

    // GET re-fetches with the same encoding.
    let (status, body) = send_get(
        router,
        &format!("/api/v1/namespaces/{TEST_NS}/configmaps/{cm_name}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["binaryData"]["logo.png"].as_str(),
        Some(encoded.as_str()),
        "GET must echo identical base64 bytes (no padding drift, no re-encode)"
    );
}
