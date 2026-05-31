//! Transitioning a Service from `ExternalName` to `ClusterIP` via a full PUT
//! that leaves `spec.externalName` populated must succeed — upstream drops
//! externalName on non-ExternalName types rather than rejecting it.
//!
//! Reproduces `[sig-network] DNS should provide DNS for ExternalName services`
//! (dns.go:406), which flips the Service type with a GET-modify-PUT that does
//! not clear externalName, and previously hit
//! `spec.externalName: Forbidden: may not be set for non-ExternalName services`.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

fn spawn_router() -> axum::Router {
    let mem = Arc::new(MemoryStorage::new());
    let state = Arc::new(ApiServerState::new(
        Arc::new(StorageBackend::Memory(mem)),
        Arc::new(TokenManager::new(b"test-secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        true,
    ));
    build_router(state, None)
}

async fn send(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: &Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn externalname_to_clusterip_full_put_clears_externalname() {
    let router = spawn_router();
    let ns = "/api/v1/namespaces/default/services";

    // 1. Create an ExternalName service.
    let (cs, _created) = send(
        &router,
        Method::POST,
        ns,
        &json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "extn", "namespace": "default"},
            "spec": {"type": "ExternalName", "externalName": "foo.example.com"}
        }),
    )
    .await;
    assert_eq!(
        cs,
        StatusCode::CREATED,
        "ExternalName create should succeed"
    );

    // 2. Flip to ClusterIP via a full PUT that STILL carries externalName
    //    (what the DNS e2e does). Must NOT be rejected.
    let (us, updated) = send(
        &router,
        Method::PUT,
        "/api/v1/namespaces/default/services/extn",
        &json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "extn", "namespace": "default"},
            "spec": {
                "type": "ClusterIP",
                "externalName": "foo.example.com",
                "ports": [{"port": 80}]
            }
        }),
    )
    .await;

    assert_eq!(
        us,
        StatusCode::OK,
        "ExternalName->ClusterIP must be accepted (externalName dropped), got {}: {}",
        us,
        updated
    );
    assert_eq!(
        updated.pointer("/spec/type").and_then(|v| v.as_str()),
        Some("ClusterIP")
    );
    assert!(
        updated
            .pointer("/spec/externalName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .is_empty(),
        "externalName must be cleared on a non-ExternalName service; got {updated}"
    );
    let cip = updated
        .pointer("/spec/clusterIP")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !cip.is_empty() && cip != "None",
        "a ClusterIP should have been allocated on the transition; got {cip:?}"
    );
}
