//! OpenAPI v3 protobuf content negotiation.
//!
//! Pins the contract that GET `/openapi/v3/<group>/<version>` honours the
//! gnostic v3 proto Accept header
//! (`application/com.github.proto-openapi.spec.v3@v1.0+protobuf`) by emitting
//! the matching content type and a prost-decodable `openapi.v3.Document` body.
//!
//! Upstream surface:
//! - `staging/src/k8s.io/client-go/openapi3/root.go` — issues GETs against
//!   the sub-document URLs with the proto Accept header and decodes the body
//!   with gnostic's openapiv3 schema.
//! - `staging/src/k8s.io/kube-openapi/pkg/handler3` — server side that picks
//!   between JSON and protobuf based on Accept.
//!
//! Harness mirrors `openapi_discovery_test.rs` (in-process axum router over
//! `StorageBackend::Memory`, driven via `tower::ServiceExt::oneshot`).

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use prost::Message;
use rusternetes_api_server::gnostic::openapi_v3::Document;
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// HTTP harness — mirrors `openapi_discovery_test.rs`.
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

fn spawn_router() -> axum::Router {
    let mem = Arc::new(MemoryStorage::new());
    build_router(make_state(mem), None)
}

const V3_PROTO_ACCEPT: &str = "application/com.github.proto-openapi.spec.v3@v1.0+protobuf";

async fn get_v3_subdoc_with_accept(
    router: axum::Router,
    accept: &str,
) -> (StatusCode, String, Vec<u8>) {
    let req = Request::builder()
        .method(Method::GET)
        .uri("/openapi/v3/apis/apps/v1")
        .header(header::ACCEPT, accept)
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, content_type, bytes.to_vec())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Baseline: the v3 sub-document is JSON by default (no Accept header).
/// Establishes that the route exists and our proto branch is the divergence,
/// not the default path.
#[tokio::test]
async fn v3_subdoc_defaults_to_json() {
    let router = spawn_router();
    let req = Request::builder()
        .method(Method::GET)
        .uri("/openapi/v3/apis/apps/v1")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let ct = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("application/json"),
        "default sub-doc Content-Type must be JSON; got {ct}"
    );
}

/// With `Accept: <v3 proto>`, the server must respond with the proto content
/// type and a body that decodes as `openapi.v3.Document`.
#[tokio::test]
async fn v3_subdoc_proto_accept_returns_proto_document() {
    let router = spawn_router();
    let (status, content_type, body) = get_v3_subdoc_with_accept(router, V3_PROTO_ACCEPT).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "v3 proto sub-doc must return 200; body len={}",
        body.len()
    );
    assert!(
        content_type.contains("proto-openapi.spec.v3"),
        "Content-Type must signal v3 proto; got {content_type}"
    );
    assert!(
        content_type.contains("+protobuf"),
        "Content-Type must end in +protobuf; got {content_type}"
    );

    // The body must NOT be a JSON payload. We deliberately do NOT blacklist
    // 0x0a here: that byte is the proto wire tag for Document.openapi field 1,
    // wire type 2 (`(1 << 3) | 2 = 0x0a`), which happens to coincide with
    // ASCII '\n'. serde_json::to_vec never prepends whitespace, so '{' / '['
    // are the safe JSON signatures to reject.
    let first = body.first().copied().unwrap_or(0);
    assert!(
        first != b'{' && first != b'[',
        "body must not start like JSON; first byte={first:#x}"
    );

    // Prost decode — proves the bytes are a valid Document on the gnostic
    // openapi.v3 schema. Same shape as client-go's
    // openapi3.parseGroupVersionPaths consumer.
    let doc = Document::decode(body.as_slice())
        .unwrap_or_else(|e| panic!("body must decode as openapi.v3.Document: {e}"));

    assert!(
        !doc.openapi.is_empty(),
        "Document.openapi version field must be populated"
    );
    assert!(
        doc.openapi.starts_with("3."),
        "Document.openapi must report a 3.x version; got {:?}",
        doc.openapi
    );

    let info = doc.info.expect("Document.info must be present");
    assert!(
        !info.title.is_empty(),
        "Document.info.title must be populated"
    );
    assert!(
        !info.version.is_empty(),
        "Document.info.version must be populated"
    );

    let paths = doc.paths.expect("Document.paths must be present");
    assert!(
        paths.path.iter().all(|p| p.name.starts_with('/')),
        "all path keys must start with '/'; got {:?}",
        paths.path.iter().map(|p| &p.name).collect::<Vec<_>>()
    );
}

/// Client-go always sends `<proto>, application/json` to allow JSON fallback.
/// Server must still pick proto when the first token matches.
#[tokio::test]
async fn v3_subdoc_proto_with_json_fallback_still_picks_proto() {
    let router = spawn_router();
    let accept = format!("{}, application/json", V3_PROTO_ACCEPT);
    let (status, content_type, body) = get_v3_subdoc_with_accept(router, &accept).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        content_type.contains("proto-openapi.spec.v3"),
        "Content-Type must remain proto when client lists JSON as fallback; got {content_type}"
    );
    Document::decode(body.as_slice()).expect("body must decode as openapi.v3.Document");
}

/// `Accept: application/json` (no proto token) must keep JSON behaviour.
#[tokio::test]
async fn v3_subdoc_json_accept_unchanged() {
    let router = spawn_router();
    let (status, content_type, body) = get_v3_subdoc_with_accept(router, "application/json").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        content_type.starts_with("application/json"),
        "JSON Accept must yield JSON Content-Type; got {content_type}"
    );
    serde_json::from_slice::<serde_json::Value>(&body)
        .expect("JSON Accept body must parse as JSON");
}
