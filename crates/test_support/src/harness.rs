//! In-process api-server test harness.
//!
//! Boots the real Axum router (`build_router`) on a `MemoryStorage` backend
//! with auth skipped, and drives it via `tower::oneshot` — no sockets, no TLS.
//! Extracted from the `spawn_router`/`oneshot` pattern duplicated across
//! `crates/api-server/tests/`.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode},
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
///
/// `Clone` is cheap and shares state: both the `Arc<MemoryStorage>` and the
/// `axum::Router` clone to handles backed by the *same* storage, mirroring the
/// `router.clone()` pattern the per-file harnesses used for repeated `oneshot`s.
#[derive(Clone)]
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
        let (status, _headers, bytes, value) =
            self.send_full(method, uri, content_type, None, body).await;
        (status, bytes, value)
    }

    /// Like [`send_bytes`](Self::send_bytes) but also lets the caller set a
    /// `content-type` and/or `accept` request header and returns the response
    /// [`HeaderMap`] — for tests that assert on response headers (e.g. strict-
    /// decoding `Warning:` headers) or drive content negotiation (`Accept`).
    /// For other request headers use [`send_with_headers`](Self::send_with_headers).
    pub async fn send_full(
        &self,
        method: &str,
        uri: &str,
        content_type: Option<&str>,
        accept: Option<&str>,
        body: Option<Vec<u8>>,
    ) -> (StatusCode, HeaderMap, Vec<u8>, Value) {
        let mut headers: Vec<(&str, &str)> = Vec::new();
        if let Some(ct) = content_type {
            headers.push(("content-type", ct));
        }
        if let Some(a) = accept {
            headers.push(("accept", a));
        }
        self.send_with_headers(method, uri, &headers, body).await
    }

    /// Fullest primitive: send a request with an arbitrary set of request
    /// headers and an optional raw body; returns status, the response
    /// [`HeaderMap`], the raw body bytes, and the body parsed as JSON
    /// (`Value::Null` if it isn't JSON).
    pub async fn send_with_headers(
        &self,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: Option<Vec<u8>>,
    ) -> (StatusCode, HeaderMap, Vec<u8>, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
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
        let headers = resp.headers().clone();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read response body")
            .to_vec();
        let value = json_or_null(&bytes);
        (status, headers, bytes, value)
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
