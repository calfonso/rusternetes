//! Regression test for issue #1061: a main (non-`/status`) PUT to a
//! ResourceQuota must NOT wipe the controller-computed status.
//!
//! Upstream `resourcequotaStrategy.PrepareForUpdate` copies the stored
//! object's status onto the incoming object so a spec-only PUT (which carries
//! an empty status) cannot clobber `used`/`hard`. This mirrors the symmetric
//! `/status` strategy fixed in #268.

use axum::body::Body;
use axum::http::Request;
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

fn spawn_router() -> (axum::Router, Arc<MemoryStorage>) {
    let mem = Arc::new(MemoryStorage::new());
    let backend = Arc::new(StorageBackend::Memory(mem.clone()));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    let state = Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true,
    ));
    (build_router(state, None), mem)
}

async fn send_json(router: axum::Router, method: &str, uri: &str, body: Option<&Value>) -> Value {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    let body = match body {
        Some(b) => Body::from(serde_json::to_vec(b).unwrap()),
        None => {
            builder = builder.header("content-length", "0");
            Body::empty()
        }
    };
    let req = builder.body(body).unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

#[tokio::test]
async fn spec_put_preserves_controller_status() {
    let (router, mem) = spawn_router();

    // Create the quota with hard limits (status will be auto-initialized).
    let create = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "q", "namespace": "default" },
        "spec": { "hard": { "pods": "10" } },
    });
    send_json(
        router.clone(),
        "POST",
        "/api/v1/namespaces/default/resourcequotas",
        Some(&create),
    )
    .await;

    // Simulate the controller computing usage by writing status directly.
    let key = build_key("resourcequotas", Some("default"), "q");
    let mut stored: Value = mem.get(&key).await.unwrap();
    stored["status"] = json!({
        "hard": { "pods": "10" },
        "used": { "pods": "3" },
    });
    mem.update(&key, &stored).await.unwrap();

    // Client PUTs a spec-only update with an empty/absent status.
    let put = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "q", "namespace": "default" },
        "spec": { "hard": { "pods": "20" } },
    });
    let resp = send_json(
        router.clone(),
        "PUT",
        "/api/v1/namespaces/default/resourcequotas/q",
        Some(&put),
    )
    .await;

    // Spec change applied...
    assert_eq!(resp["spec"]["hard"]["pods"], json!("20"));
    // ...but controller-computed used status survived the spec PUT.
    assert_eq!(
        resp["status"]["used"]["pods"],
        json!("3"),
        "spec PUT must not wipe controller status: {resp}"
    );

    // And a subsequent GET still sees the preserved status.
    let got = send_json(
        router,
        "GET",
        "/api/v1/namespaces/default/resourcequotas/q",
        None,
    )
    .await;
    assert_eq!(got["status"]["used"]["pods"], json!("3"));
}
