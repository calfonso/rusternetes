//! The proxy subresource must forward ALL HTTP methods to the backend, not
//! just GET/POST/PUT/PATCH/DELETE. The `[sig-network] Proxy version v1` e2e
//! sends OPTIONS (and others) and parses the backend's response; when OPTIONS
//! wasn't a registered method on the proxy route, axum returned 405 and the
//! body was empty ("unexpected end of JSON input"). The routes are now `any()`.
//!
//! We can't stand up a real backend in-process, so we assert the weaker but
//! decisive property: OPTIONS/HEAD are ROUTED to the proxy handler (same
//! outcome as GET) rather than rejected with 405 Method Not Allowed.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{memory::MemoryStorage, Storage, StorageBackend};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

async fn status_for(method: Method) -> StatusCode {
    let mem = Arc::new(MemoryStorage::new());
    // Seed a pod (no podIP) so the handler runs and hits a deterministic,
    // non-405 outcome regardless of method.
    mem.create(
        "/api/v1/namespaces/default/pods/agnhost",
        &json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "agnhost", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "agnhost"}]},
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();
    let state = Arc::new(ApiServerState::new(
        Arc::new(StorageBackend::Memory(mem)),
        Arc::new(TokenManager::new(b"test-secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        true,
    ));
    let router = build_router(state, None);
    let resp = router
        .oneshot(
            Request::builder()
                .method(method)
                .uri("/api/v1/namespaces/default/pods/agnhost/proxy")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    resp.status()
}

#[tokio::test]
async fn proxy_routes_all_methods_not_405() {
    let get = status_for(Method::GET).await;
    assert_ne!(
        get,
        StatusCode::METHOD_NOT_ALLOWED,
        "GET on pod proxy must be routed"
    );
    // The methods the e2e exercises that previously weren't registered.
    for m in [Method::OPTIONS, Method::HEAD] {
        let s = status_for(m.clone()).await;
        assert_ne!(
            s,
            StatusCode::METHOD_NOT_ALLOWED,
            "{m} on the proxy subresource must be routed to the handler, not 405"
        );
        assert_eq!(
            s, get,
            "{m} should reach the same proxy handler outcome as GET (got {s} vs {get})"
        );
    }
}
