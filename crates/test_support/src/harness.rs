//! In-process api-server test harness.
//!
//! Boots the real Axum router (`build_router`) on a `MemoryStorage` backend
//! with auth skipped, and drives it via `tower::oneshot` — no sockets, no TLS.
//! Extracted from the `spawn_router`/`oneshot` pattern duplicated across
//! `crates/api-server/tests/`.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
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

    /// Low-level request primitive — returns status, raw body bytes, and the
    /// body parsed as JSON (`Value::Null` if it isn't JSON). Mirrors the
    /// `send`/`read_body` helpers duplicated across `crates/api-server/tests/`.
    pub async fn send_raw(
        &self,
        method: &str,
        uri: &str,
        content_type: Option<&str>,
        body: Option<&Value>,
    ) -> (StatusCode, Vec<u8>, Value) {
        let bytes = body.map(|b| serde_json::to_vec(b).expect("serialize body"));
        self.send_bytes(method, uri, content_type, bytes).await
    }

    /// Lowest-level primitive: send an arbitrary raw byte body (or `None` for an
    /// empty body) and return `(status, raw bytes, parsed-or-null JSON)`. Use
    /// this for malformed-input, non-UTF-8, or non-JSON content-type tests that
    /// cannot route their body through a `serde_json::Value`.
    pub async fn send_bytes(
        &self,
        method: &str,
        uri: &str,
        content_type: Option<&str>,
        body: Option<Vec<u8>>,
    ) -> (StatusCode, Vec<u8>, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(ct) = content_type {
            builder = builder.header("content-type", ct);
        }
        let req = match body {
            Some(b) => builder.body(Body::from(b)).expect("build request"),
            None => builder.body(Body::empty()).expect("build request"),
        };
        let resp = self
            .router
            .clone()
            .oneshot(req)
            .await
            .expect("router oneshot");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read response body")
            .to_vec();
        let value = json_or_null(&bytes);
        (status, bytes, value)
    }

    /// As [`send_raw`](Self::send_raw) but drops the raw bytes.
    pub async fn send(
        &self,
        method: &str,
        uri: &str,
        content_type: Option<&str>,
        body: Option<&Value>,
    ) -> (StatusCode, Value) {
        let (status, _bytes, value) = self.send_raw(method, uri, content_type, body).await;
        (status, value)
    }

    /// GET, decoding the body as JSON.
    pub async fn get(&self, uri: &str) -> (StatusCode, Value) {
        self.send("GET", uri, None, None).await
    }

    /// POST a JSON body.
    pub async fn post(&self, uri: &str, body: &Value) -> (StatusCode, Value) {
        self.send("POST", uri, Some("application/json"), Some(body))
            .await
    }

    /// PUT a JSON body.
    pub async fn put(&self, uri: &str, body: &Value) -> (StatusCode, Value) {
        self.send("PUT", uri, Some("application/json"), Some(body))
            .await
    }

    /// PATCH with `application/merge-patch+json` (the common test default; use
    /// [`send`](Self::send) for JSON-patch / strategic-merge content types).
    pub async fn patch(&self, uri: &str, body: &Value) -> (StatusCode, Value) {
        self.send(
            "PATCH",
            uri,
            Some("application/merge-patch+json"),
            Some(body),
        )
        .await
    }

    /// DELETE.
    pub async fn delete(&self, uri: &str) -> (StatusCode, Value) {
        self.send("DELETE", uri, None, None).await
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
