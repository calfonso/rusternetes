//! Integration tests for the conformance payload-dump middleware.
//!
//! Each test sets `RUSTERNETES_DUMP_PAYLOADS=1` via a process-wide guard
//! before the first call to `dumps_enabled()`. Tests run with
//! `--test-threads=1` because the env gate is read once per process.

use axum::{body::Body, http::Request, routing::post, Router};
use rusternetes_api_server::middleware::capture_payload;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Default, Clone)]
struct LogSink(Arc<Mutex<Vec<String>>>);

impl<S> tracing_subscriber::Layer<S> for LogSink
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut v = String::new();
        let mut visitor = Visitor(&mut v);
        event.record(&mut visitor);
        self.0.lock().unwrap().push(v);
    }
}
struct Visitor<'a>(&'a mut String);
impl tracing::field::Visit for Visitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, " {}={:?}", field.name(), value);
    }
}

fn install_capture() -> (LogSink, tracing::subscriber::DefaultGuard) {
    std::env::set_var("RUSTERNETES_DUMP_PAYLOADS", "1");
    let sink = LogSink::default();
    let sub = tracing_subscriber::registry().with(sink.clone());
    let guard = tracing::subscriber::set_default(sub);
    (sink, guard)
}

#[tokio::test]
async fn dumps_body_on_5xx() {
    let (sink, _guard) = install_capture();
    let app = Router::new()
        .route(
            "/boom",
            post(|_b: String| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "kaboom") }),
        )
        .layer(axum::middleware::from_fn(capture_payload));
    let req = Request::builder()
        .method("POST")
        .uri("/boom")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"kind":"Pod","name":"sentinel"}"#))
        .unwrap();
    let _ = app.oneshot(req).await.unwrap();
    let logs = sink.0.lock().unwrap().clone();
    assert!(
        logs.iter()
            .any(|l| l.contains("sentinel") && l.contains("5xx")),
        "expected 5xx dump containing sentinel; got {logs:?}"
    );
}

#[tokio::test]
async fn redacts_secret_data_on_5xx() {
    let (sink, _guard) = install_capture();
    let app = Router::new()
        .route(
            "/boom",
            post(|_b: String| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "kaboom") }),
        )
        .layer(axum::middleware::from_fn(capture_payload));
    let req = Request::builder()
        .method("POST")
        .uri("/boom")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"kind":"Secret","data":{"k":"YWJjZA=="}}"#))
        .unwrap();
    let _ = app.oneshot(req).await.unwrap();
    let logs = sink.0.lock().unwrap().clone();
    assert!(
        logs.iter()
            .any(|l| l.contains("redacted len=4") && !l.contains("YWJjZA==")),
        "expected redacted secret in logs; got {logs:?}"
    );
}

#[tokio::test]
async fn does_not_dump_on_2xx() {
    let (sink, _guard) = install_capture();
    let app = Router::new()
        .route("/ok", post(|_b: String| async { "ok" }))
        .layer(axum::middleware::from_fn(capture_payload));
    let req = Request::builder()
        .method("POST")
        .uri("/ok")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"sentinel":true}"#))
        .unwrap();
    let _ = app.oneshot(req).await.unwrap();
    let logs = sink.0.lock().unwrap().clone();
    assert!(
        !logs.iter().any(|l| l.contains("sentinel")),
        "should not have dumped on 2xx; got {logs:?}"
    );
}

#[tokio::test]
async fn marks_payload_truncated_when_oversized() {
    let (sink, _guard) = install_capture();
    // Body intentionally larger than MAX_DUMP_BODY (4 MiB). Use a payload
    // 5 MiB long so to_bytes hits the cap.
    let big = vec![b'a'; 5 * 1024 * 1024];
    let app = Router::new()
        .route(
            "/boom",
            post(|_b: axum::body::Bytes| async {
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "kaboom")
            }),
        )
        .layer(axum::middleware::from_fn(capture_payload));
    let req = Request::builder()
        .method("POST")
        .uri("/boom")
        .header("content-type", "application/octet-stream")
        .body(Body::from(big))
        .unwrap();
    let _ = app.oneshot(req).await.unwrap();
    let logs = sink.0.lock().unwrap().clone();
    assert!(
        logs.iter()
            .any(|l| l.contains("payload_truncated=true") && l.contains("<truncated>")),
        "expected truncated log line; got {logs:?}"
    );
}
