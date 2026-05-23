//! Conformance test: native-protobuf response envelope for the Pod
//! resource (GET, LIST, CREATE).
//!
//! Upstream contract
//! -----------------
//! `client-go`'s default `Accept` header is
//! `application/vnd.kubernetes.protobuf, application/json` (see
//! `staging/src/k8s.io/client-go/rest/config.go::SetKubernetesDefaults`).
//! Upstream `kube-apiserver` honours it by serialising the response into
//! a `k8s\0`-framed `runtime.Unknown` envelope whose `raw` field holds
//! the native protobuf bytes of the resource — produced by the generated
//! `pb.go` `Marshal` methods (`staging/src/k8s.io/apimachinery/pkg/runtime/serializer/protobuf`).
//! Clients dispatch on the envelope's `typeMeta { apiVersion, kind }`
//! before fully decoding the body.
//!
//! Until rusternetes ships per-resource native encoders, the Unknown
//! envelope's `contentType` field is set to `application/json` and `raw`
//! carries the JSON-serialised resource — a valid envelope that
//! `Unknown`-aware clients round-trip via the `contentType` hint. The
//! relevant code paths:
//!
//! - `crates/api-server/src/response.rs::{ProtoEncoder, NativeProtoOptIn,
//!   WrappedJsonProtoEncoder}` — the trait + marker + default encoder.
//! - `crates/api-server/src/middleware.rs` — the response-wrapping
//!   middleware that picks up the marker and runs the encoder.
//! - `crates/api-server/src/handlers/pod.rs` — first opt-in consumer
//!   (`get`, `list`, `create`).
//! - `crates/common/src/protobuf.rs::decode_protobuf` — round-trips the
//!   `Unknown` envelope back into a typed Rust resource.
//!
//! These tests pin:
//! 1. GET Pod with protobuf `Accept` returns a `k8s\0`-framed envelope
//!    whose `decode_protobuf<Pod>` round-trips to the seeded value.
//! 2. LIST Pods with protobuf `Accept` returns a `k8s\0`-framed envelope
//!    whose `decode_protobuf<List<Pod>>` round-trips and has `kind=PodList`.
//! 3. CREATE Pod with protobuf `Accept` returns a `k8s\0`-framed envelope
//!    with status 201 and `decode_protobuf<Pod>` round-trips.
//! 4. GET Pod with `Accept: application/json` keeps JSON shape (no
//!    regression for the JSON path).
//! 5. The `Accept: application/vnd.kubernetes.protobuf, application/json`
//!    multi-codec header — what real `client-go` actually sends — is
//!    honoured by emitting protobuf.
//! 6. Non-opted-in resources (e.g. ConfigMap) still fall back to JSON.
//! 7. WATCH requests (Accept stream=watch) do NOT get protobuf-wrapped;
//!    chunked watch frames have their own encoder.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager,
    authz::AlwaysAllowAuthorizer,
    observability::MetricsRegistry,
    protobuf::{decode_protobuf, is_protobuf, PROTOBUF_MAGIC},
    resources::Pod,
    List,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const TEST_NS: &str = "default";
const PROTOBUF_ACCEPT: &str = "application/vnd.kubernetes.protobuf";
const CLIENT_GO_ACCEPT: &str = "application/vnd.kubernetes.protobuf, application/json";

// ---------------------------------------------------------------------------
// Harness — mirrors `decoder_accept_header_test.rs`.
// ---------------------------------------------------------------------------

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

fn spawn_router() -> (Arc<MemoryStorage>, axum::Router) {
    let mem = Arc::new(MemoryStorage::new());
    let router = build_router(make_state(mem.clone()), None);
    (mem, router)
}

async fn seed_pod(mem: &Arc<MemoryStorage>, name: &str) {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
        },
        "spec": {
            "containers": [{"name": "c", "image": "busybox"}]
        }
    });
    let key = build_key("pods", Some(TEST_NS), name);
    mem.create(&key, &pod).await.expect("seed pod");
}

async fn http_request(
    router: axum::Router,
    method: Method,
    uri: &str,
    accept: Option<&str>,
    body: Option<(&str, Vec<u8>)>,
) -> (StatusCode, String, Vec<u8>) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(a) = accept {
        req = req.header(axum::http::header::ACCEPT, a);
    }
    let body = if let Some((ct, bytes)) = body {
        req = req.header(axum::http::header::CONTENT_TYPE, ct);
        Body::from(bytes)
    } else {
        Body::empty()
    };
    let response = router.oneshot(req.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, content_type, bytes.to_vec())
}

// ---------------------------------------------------------------------------
// 1. GET single Pod — protobuf round-trip
// ---------------------------------------------------------------------------

/// `GET /api/v1/namespaces/X/pods/Y` with
/// `Accept: application/vnd.kubernetes.protobuf` must return:
/// - HTTP 200
/// - `Content-Type: application/vnd.kubernetes.protobuf`
/// - body starts with the `k8s\0` magic prefix
/// - body decodes via `decode_protobuf::<Pod>` to the seeded Pod with the
///   same `metadata.name`
/// - decoded `TypeMeta` reports `apiVersion=v1` and `kind=Pod`
#[tokio::test]
async fn get_pod_with_protobuf_accept_returns_native_envelope() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "proto-get").await;

    let (status, ct, body) = http_request(
        router,
        Method::GET,
        "/api/v1/namespaces/default/pods/proto-get",
        Some(PROTOBUF_ACCEPT),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "status={status}");
    assert!(
        ct.starts_with(PROTOBUF_ACCEPT),
        "Content-Type must be protobuf; got {ct}"
    );
    assert!(
        body.starts_with(PROTOBUF_MAGIC),
        "body must start with k8s\\0 magic; first bytes={:?}",
        &body[..body.len().min(8)]
    );
    assert!(is_protobuf(&body), "is_protobuf helper must agree");

    let (pod, type_meta): (Pod, _) =
        decode_protobuf(&body).expect("decode_protobuf must round-trip Pod");
    assert_eq!(
        pod.metadata.name, "proto-get",
        "decoded Pod.metadata.name mismatch"
    );
    assert_eq!(type_meta.api_version, "v1");
    assert_eq!(type_meta.kind, "Pod");
}

// ---------------------------------------------------------------------------
// 2. LIST Pods — protobuf round-trip
// ---------------------------------------------------------------------------

/// `GET /api/v1/namespaces/X/pods` with protobuf Accept must return a
/// `k8s\0`-framed envelope that round-trips to `List<Pod>` with
/// `kind=PodList`.
#[tokio::test]
async fn list_pods_with_protobuf_accept_returns_native_envelope() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "proto-list-1").await;
    seed_pod(&mem, "proto-list-2").await;

    let (status, ct, body) = http_request(
        router,
        Method::GET,
        "/api/v1/namespaces/default/pods",
        Some(PROTOBUF_ACCEPT),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "status={status}");
    assert!(
        ct.starts_with(PROTOBUF_ACCEPT),
        "Content-Type must be protobuf; got {ct}"
    );
    assert!(
        body.starts_with(PROTOBUF_MAGIC),
        "body must start with k8s\\0 magic"
    );

    let (list, type_meta): (List<Pod>, _) =
        decode_protobuf(&body).expect("decode_protobuf must round-trip PodList");
    assert_eq!(type_meta.api_version, "v1");
    assert_eq!(type_meta.kind, "PodList");
    assert_eq!(list.items.len(), 2, "expected two pods; got {list:?}");
    let mut names: Vec<_> = list.items.iter().map(|p| p.metadata.name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["proto-list-1", "proto-list-2"]);
}

// ---------------------------------------------------------------------------
// 3. CREATE Pod — protobuf round-trip with 201 status
// ---------------------------------------------------------------------------

/// `POST /api/v1/namespaces/X/pods` with protobuf Accept and JSON body
/// must return HTTP 201 + a `k8s\0`-framed envelope.
#[tokio::test]
async fn create_pod_with_protobuf_accept_returns_native_envelope() {
    let (_mem, router) = spawn_router();

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "proto-create", "namespace": TEST_NS },
        "spec": { "containers": [{"name": "c", "image": "busybox"}] }
    });
    let body_bytes = serde_json::to_vec(&pod_json).unwrap();

    let (status, ct, body) = http_request(
        router,
        Method::POST,
        "/api/v1/namespaces/default/pods",
        Some(PROTOBUF_ACCEPT),
        Some(("application/json", body_bytes)),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED, "status={status}");
    assert!(
        ct.starts_with(PROTOBUF_ACCEPT),
        "Content-Type must be protobuf; got {ct}"
    );
    assert!(body.starts_with(PROTOBUF_MAGIC));

    let (pod, type_meta): (Pod, _) =
        decode_protobuf(&body).expect("decode_protobuf must round-trip Pod");
    assert_eq!(pod.metadata.name, "proto-create");
    assert_eq!(type_meta.api_version, "v1");
    assert_eq!(type_meta.kind, "Pod");
}

// ---------------------------------------------------------------------------
// 4. JSON path is untouched
// ---------------------------------------------------------------------------

/// `GET /api/v1/namespaces/X/pods/Y` with `Accept: application/json`
/// must still return plain JSON — opt-in must NOT regress the JSON path.
#[tokio::test]
async fn get_pod_with_json_accept_still_returns_json() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "json-get").await;

    let (status, ct, body) = http_request(
        router,
        Method::GET,
        "/api/v1/namespaces/default/pods/json-get",
        Some("application/json"),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.starts_with("application/json"),
        "Content-Type must be JSON; got {ct}"
    );
    assert!(
        !body.starts_with(PROTOBUF_MAGIC),
        "body must NOT be protobuf-wrapped; first bytes={:?}",
        &body[..body.len().min(8)]
    );
    let v: Value = serde_json::from_slice(&body).expect("body must parse as JSON");
    assert_eq!(v["kind"], "Pod");
    assert_eq!(v["metadata"]["name"], "json-get");
}

// ---------------------------------------------------------------------------
// 5. client-go's exact multi-codec Accept header
// ---------------------------------------------------------------------------

/// `Accept: application/vnd.kubernetes.protobuf, application/json` —
/// the literal default from
/// `staging/src/k8s.io/client-go/rest/config.go::SetKubernetesDefaults`.
/// First media type is protobuf so the server must emit protobuf.
#[tokio::test]
async fn get_pod_with_client_go_default_accept_returns_protobuf() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "clientgo").await;

    let (status, ct, body) = http_request(
        router,
        Method::GET,
        "/api/v1/namespaces/default/pods/clientgo",
        Some(CLIENT_GO_ACCEPT),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.starts_with(PROTOBUF_ACCEPT),
        "client-go default Accept must produce protobuf; got {ct}"
    );
    assert!(body.starts_with(PROTOBUF_MAGIC));
    let (pod, _): (Pod, _) = decode_protobuf(&body).expect("decode_protobuf");
    assert_eq!(pod.metadata.name, "clientgo");
}

// ---------------------------------------------------------------------------
// 6. Non-opted-in resource still falls back to JSON
// ---------------------------------------------------------------------------

/// ConfigMap has not opted in to native-protobuf yet, so a protobuf
/// `Accept` against `/api/v1/namespaces/X/configmaps` must still produce
/// JSON. This is the safety property: the opt-in mechanism must NOT
/// silently widen to resources whose handlers have not been updated.
#[tokio::test]
async fn configmap_get_without_opt_in_falls_back_to_json() {
    let (mem, router) = spawn_router();

    // Seed a ConfigMap.
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "no-opt-in", "namespace": TEST_NS },
        "data": { "k": "v" }
    });
    let key = build_key("configmaps", Some(TEST_NS), "no-opt-in");
    mem.create(&key, &cm).await.expect("seed cm");

    let (status, ct, body) = http_request(
        router,
        Method::GET,
        "/api/v1/namespaces/default/configmaps/no-opt-in",
        Some(PROTOBUF_ACCEPT),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.starts_with("application/json"),
        "ConfigMap is not opted in; must fall back to JSON; got {ct}"
    );
    assert!(
        !body.starts_with(PROTOBUF_MAGIC),
        "non-opted-in resource must not produce protobuf envelope"
    );
}

// ---------------------------------------------------------------------------
// 7. Watch requests are NOT protobuf-wrapped by the single-response path
// ---------------------------------------------------------------------------

/// Watch responses are chunked frame streams (one JSON line per event) and
/// have their own protobuf-stream encoder. The single-response wrapper
/// must skip them — otherwise we'd collapse the stream into a single
/// envelope and break every watch client. Use `watch=true` query param to
/// trigger the watch code path. Even with a protobuf Accept the response
/// must NOT carry the single-response `application/vnd.kubernetes.protobuf`
/// content-type (it stays as JSON or `;stream=watch` per the watch encoder).
#[tokio::test]
async fn list_pods_watch_does_not_wrap_in_single_response_protobuf() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "watched").await;

    // Use `?watch=true` AND a short `timeoutSeconds` so the test doesn't
    // hang on the watch stream. We just need to confirm the response
    // Content-Type is not the single-response protobuf envelope.
    let (_status, ct, body) = http_request(
        router,
        Method::GET,
        "/api/v1/namespaces/default/pods?watch=true&timeoutSeconds=1",
        Some(PROTOBUF_ACCEPT),
        None,
    )
    .await;

    // The single-response wrapping middleware must not have converted the
    // watch stream into a `k8s\0` envelope. The body either is a chunked
    // newline-delimited JSON stream or the watch handler's own format.
    assert!(
        !body.starts_with(PROTOBUF_MAGIC),
        "watch response must not be wrapped as a single-shot protobuf \
         envelope; first bytes={:?}",
        &body[..body.len().min(8)]
    );
    // Content-Type assertion is lax — different watch encoders may set
    // different types — but it should not be the bare single-response
    // protobuf header.
    assert!(
        ct != PROTOBUF_ACCEPT,
        "watch response must not advertise the single-response \
         protobuf Content-Type; got {ct}"
    );
}
