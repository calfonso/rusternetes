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
//! Rusternetes' Pod encoder (post-2026-05-24) emits real native protobuf
//! bytes via `crate::protobuf::PROTO_REGISTRY.encode_message("Pod", …)`
//! and wraps them in the Unknown envelope. The relevant code paths:
//!
//! - `crates/api-server/src/response.rs::{ProtoEncoder, NativeProtoOptIn,
//!   NativePodProtoEncoder, WrappedJsonProtoEncoder, encoder_for}` —
//!   the trait + marker + per-kind encoder dispatch.
//! - `crates/api-server/src/middleware.rs` — the response-wrapping
//!   middleware that picks up the marker and runs the encoder.
//! - `crates/api-server/src/handlers/pod.rs` — first opt-in consumer
//!   (`get`, `list`, `create`).
//! - `crates/api-server/src/protobuf.rs::ProtoRegistry::encode_message` —
//!   the schema-driven encoder.
//!
//! These tests round-trip the envelope through
//! `PROTO_REGISTRY.decode_message`: encode-then-decode reproduces the
//! resource JSON. They pin:
//! 1. GET Pod with protobuf `Accept` returns a `k8s\0`-framed envelope
//!    whose `Unknown.raw` decodes back to the seeded Pod.
//! 2. LIST Pods with protobuf `Accept` returns a `k8s\0`-framed envelope
//!    decoding to a PodList.
//! 3. CREATE Pod with protobuf `Accept` returns a `k8s\0`-framed envelope
//!    with HTTP 201.
//! 4. GET Pod with `Accept: application/json` keeps JSON shape.
//! 5. `client-go`'s multi-codec Accept is honoured as protobuf.
//! 6. Non-opted-in resources (e.g. ConfigMap) fall back to JSON.
//! 7. WATCH requests (Accept stream=watch) do NOT get protobuf-wrapped.

use axum::http::{Method, StatusCode};
use rusternetes_api_server::protobuf::PROTO_REGISTRY;
use rusternetes_common::protobuf::{is_protobuf, Unknown, PROTOBUF_MAGIC};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

/// Round-trip a `k8s\0`-framed envelope back into its JSON form by
/// decoding `Unknown.raw` through `PROTO_REGISTRY.decode_message(kind, …)`.
/// Returns `(apiVersion, kind, decoded JSON value)`.
fn decode_envelope(body: &[u8], schema_kind: &str) -> (String, String, Value) {
    assert!(
        body.starts_with(PROTOBUF_MAGIC),
        "body missing k8s\\0 magic prefix; first bytes={:?}",
        &body[..body.len().min(8)]
    );
    use prost::Message;
    let unknown = Unknown::decode(&body[PROTOBUF_MAGIC.len()..]).expect("Unknown decode");
    let tm = unknown.type_meta.expect("typeMeta must be present");
    let decoded = PROTO_REGISTRY
        .decode_message(schema_kind, &unknown.raw)
        .unwrap_or_else(|| panic!("registry has no schema for {schema_kind}"));
    (tm.api_version, tm.kind, decoded)
}

const TEST_NS: &str = "default";
const PROTOBUF_ACCEPT: &str = "application/vnd.kubernetes.protobuf";
const CLIENT_GO_ACCEPT: &str = "application/vnd.kubernetes.protobuf, application/json";

// ---------------------------------------------------------------------------
// Harness — mirrors `decoder_accept_header_test.rs`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
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
    router: TestApiServer,
    method: Method,
    uri: &str,
    accept: Option<&str>,
    body: Option<(&str, Vec<u8>)>,
) -> (StatusCode, String, Vec<u8>) {
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if let Some(a) = accept {
        headers.push(("accept", a));
    }
    let body_bytes = body.map(|(ct, bytes)| {
        headers.push(("content-type", ct));
        bytes
    });
    let (status, hmap, bytes, _) = router
        .send_with_headers(method.as_str(), uri, &headers, body_bytes)
        .await;
    let content_type = hmap
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    (status, content_type, bytes)
}

// ---------------------------------------------------------------------------
// 1. GET single Pod — protobuf round-trip
// ---------------------------------------------------------------------------

/// `GET /api/v1/namespaces/X/pods/Y` with
/// `Accept: application/vnd.kubernetes.protobuf` must return:
/// - HTTP 200
/// - `Content-Type: application/vnd.kubernetes.protobuf`
/// - body starts with the `k8s\0` magic prefix
/// - body decodes via `PROTO_REGISTRY.decode_message("Pod", raw)` to the
///   seeded Pod with the same `metadata.name`
/// - envelope `TypeMeta` reports `apiVersion=v1` and `kind=Pod`
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

    let (api_version, kind, pod) = decode_envelope(&body, "Pod");
    assert_eq!(api_version, "v1");
    assert_eq!(kind, "Pod");
    assert_eq!(
        pod.pointer("/metadata/name"),
        Some(&Value::String("proto-get".into())),
        "decoded Pod.metadata.name mismatch; got {pod}"
    );
}

// ---------------------------------------------------------------------------
// 2. LIST Pods — protobuf round-trip
// ---------------------------------------------------------------------------

/// `GET /api/v1/namespaces/X/pods` with protobuf Accept must return a
/// `k8s\0`-framed envelope that round-trips to a PodList with
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

    let (api_version, kind, list) = decode_envelope(&body, "PodList");
    assert_eq!(api_version, "v1");
    assert_eq!(kind, "PodList");
    let items = list
        .get("items")
        .and_then(|i| i.as_array())
        .expect("decoded PodList.items must be an array");
    assert_eq!(items.len(), 2, "expected two pods; got {list}");
    let mut names: Vec<String> = items
        .iter()
        .map(|p| {
            p.pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        })
        .collect();
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

    let (api_version, kind, pod) = decode_envelope(&body, "Pod");
    assert_eq!(api_version, "v1");
    assert_eq!(kind, "Pod");
    assert_eq!(
        pod.pointer("/metadata/name"),
        Some(&Value::String("proto-create".into())),
    );
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
    let (_, _, pod) = decode_envelope(&body, "Pod");
    assert_eq!(
        pod.pointer("/metadata/name"),
        Some(&Value::String("clientgo".into())),
    );
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
