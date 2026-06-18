//! In-process api-server test harness.
//!
//! Boots the real Axum router (`build_router`) on a `MemoryStorage` backend
//! with auth skipped, and drives it via `tower::oneshot` — no sockets, no TLS.
//! Extracted from the `spawn_router`/`oneshot` pattern duplicated across
//! `crates/api-server/tests/`.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::Value;
use tower::ServiceExt;

/// A ready-to-drive api-server: the `MemoryStorage` backend (for direct seeding
/// / assertions) plus the built router.
pub struct TestApiServer {
    pub storage: Arc<MemoryStorage>,
    pub router: axum::Router,
}

impl TestApiServer {
    /// Build a fresh api-server with empty in-memory storage and `--skip-auth`.
    pub fn new() -> Self {
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
            true, // skip_auth
        ));
        let router = build_router(state, None);
        Self {
            storage: mem,
            router,
        }
    }

    /// Issue one request, returning the status and raw response body bytes.
    pub async fn request(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Vec<u8>) {
        let mut builder = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(v) => {
                builder = builder.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&v).expect("serialize request body"))
            }
            None => Body::empty(),
        };
        let req = builder.body(body).expect("build request");
        let resp = self
            .router
            .clone()
            .oneshot(req)
            .await
            .expect("router oneshot");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read response body");
        (status, bytes.to_vec())
    }

    /// GET, decoding the body as JSON (`Value::Null` if the body isn't JSON).
    pub async fn get(&self, uri: &str) -> (StatusCode, Value) {
        let (status, bytes) = self.request(Method::GET, uri, None).await;
        (status, json_or_null(&bytes))
    }

    /// POST a JSON body, decoding the response as JSON.
    pub async fn post(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        let (status, bytes) = self.request(Method::POST, uri, Some(body)).await;
        (status, json_or_null(&bytes))
    }

    /// DELETE, decoding the response as JSON.
    pub async fn delete(&self, uri: &str) -> (StatusCode, Value) {
        let (status, bytes) = self.request(Method::DELETE, uri, None).await;
        (status, json_or_null(&bytes))
    }
}

impl Default for TestApiServer {
    fn default() -> Self {
        Self::new()
    }
}

fn json_or_null(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}
