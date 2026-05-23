//! Wire-format conformance tests for the `Scale` subresource over the
//! Kubernetes protobuf content type.
//!
//! Upstream conformance background
//! -------------------------------
//! The autoscaling/v1 `Scale` shape is the request/response payload for the
//! `/scale` subresource exposed by every scalable kind (Deployment,
//! StatefulSet, ReplicaSet, ReplicationController). It is exercised by the
//! `[sig-apps] Deployment` and `[sig-apps] StatefulSet basic-scale`
//! conformance tests, all of which run with client-go's default
//! `Accept: application/vnd.kubernetes.protobuf, application/json` header.
//! When the server emits JSON the client logs a warning and falls back, but
//! the wire-format expectation is that GETs against `/scale` produce a
//! `k8s\0`-prefixed Unknown envelope carrying TypeMeta (`autoscaling/v1`,
//! `Scale`) and a Scale payload.
//!
//! Two complementary surfaces are covered:
//!
//! 1. **Registry parity** — the `Scale` / `ScaleSpec` / `ScaleStatus`
//!    messages must be present in [`ProtoRegistry`] with the field numbers
//!    from upstream `k8s.io/api/autoscaling/v1/generated.proto`. This
//!    catches a future schema edit that forgets one of the three shapes
//!    and silently regresses native-protobuf decoders.
//!
//! 2. **Handler round-trip** — `GET /apis/apps/v1/namespaces/{ns}/{kind}/
//!    {name}/scale` with `Accept: application/vnd.kubernetes.protobuf` must
//!    return `Content-Type: application/vnd.kubernetes.protobuf` and a body
//!    that decodes via [`decode_protobuf::<Scale>`]. The decoded TypeMeta
//!    must report `autoscaling/v1` / `Scale` and the Scale payload must
//!    surface the seeded Deployment's replicas.
//!
//! The handler-level coverage uses the same in-process axum harness as
//! `decoder_accept_header_test.rs` so that the entire request pipeline
//! (router → middleware → handler → response) runs end-to-end without
//! depending on a real apiserver.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use rusternetes_api_server::{
    handlers::scale::Scale, protobuf::ProtoRegistry, router::build_router, state::ApiServerState,
};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
    protobuf::decode_protobuf,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

const TEST_NS: &str = "default";
const PROTO_ACCEPT: &str = "application/vnd.kubernetes.protobuf";

// ---------------------------------------------------------------------------
// Registry parity — the three autoscaling/v1 Scale messages must be present.
// ---------------------------------------------------------------------------

/// Field numbers from
/// `crates/api-server/proto/upstream/v1.35/k8s.io/api/autoscaling/v1/generated.proto`:
///   Scale: 1=metadata (ObjectMeta), 2=spec (ScaleSpec), 3=status (ScaleStatus)
///   ScaleSpec: 1=replicas (int32)
///   ScaleStatus: 1=replicas (int32), 2=selector (string)
///
/// A `decode_message` returning `None` means the schema is missing entirely;
/// returning an empty object on non-empty bytes would be the silent variant
/// of the same regression (caught by the round-trip tests below).
#[test]
fn test_scale_schemas_registered() {
    let registry = ProtoRegistry::new();
    for name in ["Scale", "ScaleSpec", "ScaleStatus"] {
        assert!(
            registry.decode_message(name, &[]).is_some(),
            "{name} schema must be registered in ProtoRegistry::new (decoder returned None)",
        );
    }
}

/// Hand-craft a `ScaleSpec { replicas: 7 }` and decode it back to JSON
/// through the registry. This pins both the field number (1) and the integer
/// wire decoding for the single field on the spec side.
#[test]
fn test_scale_spec_proto_decode_replicas() {
    let registry = ProtoRegistry::new();
    // ScaleSpec field 1 is int32, varint. tag = (1 << 3) | 0 = 0x08.
    let bytes = vec![0x08, 7];

    let decoded = registry
        .decode_message("ScaleSpec", &bytes)
        .expect("ScaleSpec must decode");
    assert_eq!(
        decoded.get("replicas").and_then(|v| v.as_i64()),
        Some(7),
        "ScaleSpec.replicas (field 1) must decode to 7; got {decoded}",
    );
}

/// `ScaleStatus { replicas: 3, selector: "app=web" }` exercises both fields
/// on the status side, including the optional selector string that
/// controllers populate after computing the parent's selector.
#[test]
fn test_scale_status_proto_decode_replicas_and_selector() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    // field 1 = replicas (int32, varint)
    bytes.push(0x08);
    bytes.push(3);
    // field 2 = selector (string, length-delimited)
    bytes.push(0x12);
    let selector = "app=web";
    bytes.push(selector.len() as u8);
    bytes.extend_from_slice(selector.as_bytes());

    let decoded = registry
        .decode_message("ScaleStatus", &bytes)
        .expect("ScaleStatus must decode");
    assert_eq!(
        decoded.get("replicas").and_then(|v| v.as_i64()),
        Some(3),
        "ScaleStatus.replicas mismatch; got {decoded}",
    );
    assert_eq!(
        decoded.get("selector").and_then(|v| v.as_str()),
        Some("app=web"),
        "ScaleStatus.selector mismatch; got {decoded}",
    );
}

/// Full `Scale { spec, status }` round-trip — proves the nested message
/// dispatch on the top-level Scale message points at the right schemas.
/// Drops `metadata` (field 1) to keep the byte budget small; metadata's
/// decode path is already covered by `ObjectMeta` parity tests elsewhere.
#[test]
fn test_scale_full_proto_decode_with_spec_and_status() {
    let registry = ProtoRegistry::new();

    // ScaleSpec { replicas: 5 } — field 1, varint
    let spec_bytes = vec![0x08, 5];
    // ScaleStatus { replicas: 4, selector: "app=web,tier=front" }
    let mut status_bytes = Vec::new();
    status_bytes.push(0x08);
    status_bytes.push(4);
    status_bytes.push(0x12);
    let sel = "app=web,tier=front";
    status_bytes.push(sel.len() as u8);
    status_bytes.extend_from_slice(sel.as_bytes());

    // Scale: field 2 = spec, field 3 = status (length-delimited submessages)
    let mut scale_bytes = Vec::new();
    scale_bytes.push((2 << 3) | 2);
    scale_bytes.push(spec_bytes.len() as u8);
    scale_bytes.extend_from_slice(&spec_bytes);
    scale_bytes.push((3 << 3) | 2);
    scale_bytes.push(status_bytes.len() as u8);
    scale_bytes.extend_from_slice(&status_bytes);

    let decoded = registry
        .decode_message("Scale", &scale_bytes)
        .expect("Scale must decode");

    let spec = decoded.get("spec").expect("Scale.spec must be present");
    assert_eq!(
        spec.get("replicas").and_then(|v| v.as_i64()),
        Some(5),
        "Scale.spec.replicas mismatch; got {decoded}",
    );

    let status = decoded
        .get("status")
        .expect("Scale.status must be present");
    assert_eq!(
        status.get("replicas").and_then(|v| v.as_i64()),
        Some(4),
        "Scale.status.replicas mismatch; got {decoded}",
    );
    assert_eq!(
        status.get("selector").and_then(|v| v.as_str()),
        Some("app=web,tier=front"),
        "Scale.status.selector mismatch; got {decoded}",
    );
}

// ---------------------------------------------------------------------------
// Handler round-trip — `GET /apis/apps/v1/.../scale` with proto Accept.
// ---------------------------------------------------------------------------
//
// Mirrors the harness used by `decoder_accept_header_test.rs`: build a
// router over `MemoryStorage` with `skip_auth = true`, seed a Deployment
// with `spec.replicas`, and drive a real HTTP request via `oneshot`.

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

/// Seed an apps/v1 Deployment with `spec.replicas = 4`. The /scale handler
/// reads the Deployment from storage and synthesizes a Scale view, so the
/// underlying kind just needs the `spec.replicas` and a `metadata.name`.
async fn seed_deployment(mem: &Arc<MemoryStorage>, name: &str, replicas: i64) {
    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
            "resourceVersion": "100",
        },
        "spec": {
            "replicas": replicas,
            "selector": {
                "matchLabels": { "app": name }
            },
            "template": {
                "metadata": { "labels": { "app": name } },
                "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
            }
        },
        "status": {
            "replicas": replicas,
            "readyReplicas": replicas
        }
    });
    let key = build_key("deployments", Some(TEST_NS), name);
    mem.create(&key, &deployment).await.expect("seed deployment");
}

/// `GET /apis/apps/v1/namespaces/default/deployments/{name}/scale` with
/// `Accept: application/vnd.kubernetes.protobuf` must:
///   * Return 200 OK.
///   * Set `Content-Type: application/vnd.kubernetes.protobuf` (not the JSON
///     default — the handler must honor the Accept header).
///   * Frame the body with the `k8s\0` magic prefix that identifies a K8s
///     `Unknown` envelope.
///   * Be decodable via [`decode_protobuf::<Scale>`], which extracts the
///     embedded TypeMeta and deserializes the inner JSON payload back to a
///     typed `Scale`. The decoded Scale must report the seeded replica
///     count so we know the handler's data path is intact, not only the
///     envelope shape.
#[tokio::test]
async fn get_scale_with_protobuf_accept_returns_k8s_envelope() {
    let (mem, router) = spawn_router();
    seed_deployment(&mem, "scale-proto", 4).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/apis/apps/v1/namespaces/default/deployments/scale-proto/scale")
        .header(header::ACCEPT, PROTO_ACCEPT)
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.expect("router oneshot");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GET /scale with proto Accept must return 200",
    );

    let ct = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("application/vnd.kubernetes.protobuf"),
        "Content-Type must be application/vnd.kubernetes.protobuf; got {ct:?}",
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert!(
        body.starts_with(b"k8s\0"),
        "response body must begin with the k8s\\0 magic prefix; first 8 bytes={:?}",
        &body[..body.len().min(8)],
    );

    let (scale, type_meta) = decode_protobuf::<Scale>(&body)
        .expect("body must decode via common::protobuf::decode_protobuf::<Scale>");

    assert_eq!(
        type_meta.api_version, "autoscaling/v1",
        "TypeMeta.apiVersion must be autoscaling/v1; got {type_meta:?}",
    );
    assert_eq!(
        type_meta.kind, "Scale",
        "TypeMeta.kind must be Scale; got {type_meta:?}",
    );
    assert_eq!(
        scale.metadata.name, "scale-proto",
        "decoded Scale.metadata.name must echo the parent kind; got {:?}",
        scale.metadata.name,
    );
    assert_eq!(
        scale.metadata.namespace, TEST_NS,
        "decoded Scale.metadata.namespace must echo the parent namespace",
    );
    assert_eq!(
        scale.spec.replicas, 4,
        "decoded Scale.spec.replicas must mirror the seeded Deployment",
    );
    assert_eq!(
        scale.status.replicas, 4,
        "decoded Scale.status.replicas must mirror the seeded Deployment",
    );
}

/// Sanity check: the same endpoint with `Accept: application/json` keeps
/// returning JSON. Without this the handler change could regress the JSON
/// path (the dominant production format) while satisfying the proto test.
#[tokio::test]
async fn get_scale_with_json_accept_returns_json() {
    let (mem, router) = spawn_router();
    seed_deployment(&mem, "scale-json", 2).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/apis/apps/v1/namespaces/default/deployments/scale-json/scale")
        .header(header::ACCEPT, "application/json")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.expect("router oneshot");

    assert_eq!(response.status(), StatusCode::OK);
    let ct = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("application/json"),
        "JSON Accept must keep returning JSON; got {ct:?}",
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert!(
        !body.starts_with(b"k8s\0"),
        "JSON response must NOT be wrapped in the protobuf envelope; first 4 bytes={:?}",
        &body[..body.len().min(4)],
    );
    let scale: Scale = serde_json::from_slice(&body).expect("JSON body must deserialize as Scale");
    assert_eq!(scale.api_version, "autoscaling/v1");
    assert_eq!(scale.kind, "Scale");
    assert_eq!(scale.spec.replicas, 2);
}

/// `PUT /scale` with `Accept: application/vnd.kubernetes.protobuf` and a
/// JSON request body must also produce a proto-framed response carrying the
/// updated replica count. The middleware decodes proto request bodies to
/// JSON before they reach the handler, so the handler's response path is
/// the only place that needs the negotiation logic for writes.
#[tokio::test]
async fn put_scale_with_protobuf_accept_returns_k8s_envelope() {
    let (mem, router) = spawn_router();
    seed_deployment(&mem, "scale-put-proto", 1).await;

    let body = json!({
        "apiVersion": "autoscaling/v1",
        "kind": "Scale",
        "metadata": {
            "name": "scale-put-proto",
            "namespace": TEST_NS,
        },
        "spec": { "replicas": 9 },
        "status": { "replicas": 1 }
    })
    .to_string();

    let req = Request::builder()
        .method(Method::PUT)
        .uri("/apis/apps/v1/namespaces/default/deployments/scale-put-proto/scale")
        .header(header::ACCEPT, PROTO_ACCEPT)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let response = router.oneshot(req).await.expect("router oneshot");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "PUT /scale with proto Accept must return 200",
    );

    let ct = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("application/vnd.kubernetes.protobuf"),
        "PUT response Content-Type must be proto; got {ct:?}",
    );

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert!(
        bytes.starts_with(b"k8s\0"),
        "PUT response body must begin with k8s\\0; first 4 bytes={:?}",
        &bytes[..bytes.len().min(4)],
    );

    let (scale, type_meta) =
        decode_protobuf::<Scale>(&bytes).expect("PUT response must decode as Scale");
    assert_eq!(type_meta.api_version, "autoscaling/v1");
    assert_eq!(type_meta.kind, "Scale");
    assert_eq!(
        scale.spec.replicas, 9,
        "PUT must persist the new replica count; got {}",
        scale.spec.replicas,
    );
}
