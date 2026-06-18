//! The proxy subresource must forward ALL HTTP methods to the backend, not
//! just GET/POST/PUT/PATCH/DELETE. The `[sig-network] Proxy version v1` e2e
//! sends OPTIONS (and others) and parses the backend's response; when OPTIONS
//! wasn't a registered method on the proxy route, axum returned 405 and the
//! body was empty ("unexpected end of JSON input"). The routes are now `any()`.
//!
//! We can't stand up a real backend in-process, so we assert the weaker but
//! decisive property: OPTIONS/HEAD are ROUTED to the proxy handler (same
//! outcome as GET) rather than rejected with 405 Method Not Allowed.

use axum::http::StatusCode;
use rusternetes_storage::Storage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

async fn status_for(method: &str) -> StatusCode {
    let api = TestApiServer::new();
    // Seed a pod (no podIP) so the handler runs and hits a deterministic,
    // non-405 outcome regardless of method.
    api.storage
        .create(
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
    let (status, _raw, _body) = api
        .send_raw(
            method,
            "/api/v1/namespaces/default/pods/agnhost/proxy",
            None,
            None,
        )
        .await;
    status
}

#[tokio::test]
async fn proxy_routes_all_methods_not_405() {
    // GET/HEAD on a bare `.../proxy` (no trailing slash) 301-redirect to
    // `.../proxy/`, matching the upstream apiserver (#410). Both are still
    // "routed" — the property this test guards is that no method hits a
    // 405 Method Not Allowed.
    for m in ["GET", "HEAD"] {
        let s = status_for(m).await;
        assert_eq!(
            s,
            StatusCode::MOVED_PERMANENTLY,
            "{m} on a bare proxy path must 301-redirect to the trailing-slash form"
        );
    }
    // The verb-test methods (the e2e sends OPTIONS, which previously wasn't a
    // registered route → 405) are proxied THROUGH to the handler, not
    // redirected. With no backend reachable the handler returns a
    // deterministic non-405 status (and crucially not the GET/HEAD 301).
    for m in ["OPTIONS"] {
        let s = status_for(m).await;
        assert_ne!(
            s,
            StatusCode::METHOD_NOT_ALLOWED,
            "{m} on the proxy subresource must be routed to the handler, not 405"
        );
        assert_ne!(
            s,
            StatusCode::MOVED_PERMANENTLY,
            "{m} must be proxied through, not redirected"
        );
    }
}
