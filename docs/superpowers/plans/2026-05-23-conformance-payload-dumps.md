# Conformance Payload Dumps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a conformance run triggers a Rust panic, a 5xx response, or a JSON-body decode failure in api-server or kubelet, log the offending payload (redacted for Secrets) on `tracing::error!` alongside the existing error.

**Architecture:** A new `rusternetes_common::dump` module holds a `tokio::task_local!` payload slot, a `redact_secret_like` pure function, and an `install_panic_hook` that reads the slot. api-server adds one outermost tower middleware to buffer the request body into the slot and to log on 5xx; a `DumpingJson<T>` extractor replaces `axum::Json<T>` in handler signatures so decode failures see the buffered body. Kubelet wraps `sync_pod` and watch-event dispatch in `CURRENT_PAYLOAD.scope(...)`. Everything is no-op unless `RUSTERNETES_DUMP_PAYLOADS=1`.

**Tech Stack:** Rust 2021, Axum 0.7, Tower, tokio, tracing, serde_json, bytes. Tests use `tracing-subscriber` test layer and `tokio::test`.

**Spec:** `docs/superpowers/specs/2026-05-23-conformance-payload-dumps-design.md`

**Pre-task hygiene (every commit):**

```bash
cargo fmt --all
cargo fmt --all -- --check    # must exit 0
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "..."
```

No `Co-Authored-By: Claude` trailer. Subject ≤72 chars.

---

## File Structure

| File | Status | Responsibility |
|------|--------|----------------|
| `crates/common/src/dump.rs` | Create | task_local, env gate, redaction, panic hook, `DumpingJson` extractor |
| `crates/common/src/lib.rs` | Modify | `pub mod dump;` |
| `crates/common/Cargo.toml` | Modify | Add `axum`, `bytes`, `tower` deps if missing |
| `crates/api-server/src/middleware/dump.rs` | Create | `capture_payload` tower middleware |
| `crates/api-server/src/middleware/mod.rs` | Create or modify | export `dump` module |
| `crates/api-server/src/router.rs` | Modify | Add `capture_payload` layer at outermost position (~L2405) |
| `crates/api-server/src/main.rs` | Modify | Call `dump::install_panic_hook("api-server")` after tracing init (~L111) |
| `crates/api-server/src/handlers/*.rs` | Modify | Replace `Json(x): Json<T>` extractors with `DumpingJson(x): DumpingJson<T>` (mechanical) |
| `crates/api-server/tests/payload_dump.rs` | Create | Integration tests: panic, 5xx, decode-fail, large body |
| `crates/kubelet/src/main.rs` | Modify | Call `dump::install_panic_hook("kubelet")` after tracing init (~L263) |
| `crates/kubelet/src/kubelet.rs` | Modify | Wrap two `sync_pod` call sites (~L911, ~L1324) in `CURRENT_PAYLOAD.scope(...)` |
| `scripts/conformance-canary-run.sh` | Modify | `export RUSTERNETES_DUMP_PAYLOADS=1` |
| `.github/workflows/conformance-canary.yml` | Modify | Set `RUSTERNETES_DUMP_PAYLOADS=1` in compose env |

---

## Task 1: Scaffold `dump` module with env gate

**Files:**
- Create: `crates/common/src/dump.rs`
- Modify: `crates/common/src/lib.rs`
- Modify: `crates/common/Cargo.toml` (add `axum`, `bytes` if missing)

- [ ] **Step 1: Inspect existing common Cargo.toml**

Run:
```bash
grep -E '^(axum|bytes|tower|tracing|serde_json)' crates/common/Cargo.toml
```
Confirm presence. If `axum` or `bytes` is missing, add under `[dependencies]`:
```toml
axum = { version = "0.7", default-features = false, features = ["json"] }
bytes = "1"
```
(`tracing` and `serde_json` are already there.)

- [ ] **Step 2: Write the failing test for `dumps_enabled()`**

Create `crates/common/src/dump.rs`:
```rust
//! Payload-dump instrumentation for conformance debugging.
//!
//! When `RUSTERNETES_DUMP_PAYLOADS=1`, panics, 5xx responses, and JSON decode
//! failures emit a `tracing::error!` containing the offending request body
//! (with Secret data redacted). All entrypoints are no-ops when the env var
//! is unset.

use std::sync::OnceLock;

static DUMPS_ENABLED: OnceLock<bool> = OnceLock::new();

/// Returns true iff `RUSTERNETES_DUMP_PAYLOADS=1` was set when this process
/// started.
pub fn dumps_enabled() -> bool {
    *DUMPS_ENABLED.get_or_init(|| {
        std::env::var("RUSTERNETES_DUMP_PAYLOADS").is_ok_and(|v| v == "1")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dumps_enabled_reads_env_once() {
        // Cannot reliably mutate process env across tests, so just assert
        // the function does not panic and returns a stable bool.
        let a = dumps_enabled();
        let b = dumps_enabled();
        assert_eq!(a, b);
    }
}
```

- [ ] **Step 3: Register module**

In `crates/common/src/lib.rs`, add `pub mod dump;` in alphabetical position between `deletion` and `encryption`:
```rust
pub mod deletion;
pub mod dump;
pub mod encryption;
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cargo test -p rusternetes-common dump::tests
```
Expected: `test dump::tests::dumps_enabled_reads_env_once ... ok`

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/common/src/dump.rs crates/common/src/lib.rs crates/common/Cargo.toml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(common): scaffold dump module with env gate"
```

---

## Task 2: Implement `redact_secret_like`

**Files:**
- Modify: `crates/common/src/dump.rs`

- [ ] **Step 1: Write failing tests**

Append to `#[cfg(test)] mod tests` in `crates/common/src/dump.rs`:
```rust
#[test]
fn redact_passthrough_for_plain_pod() {
    let input = br#"{"kind":"Pod","metadata":{"name":"p"}}"#;
    assert_eq!(&*redact_secret_like(input), input);
}

#[test]
fn redact_replaces_secret_data_values() {
    let input = br#"{"kind":"Secret","data":{"token":"YWJjZA==","empty":""}}"#;
    let out = redact_secret_like(input);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["data"]["token"], "<redacted len=4>");
    assert_eq!(v["data"]["empty"], "<redacted len=0>");
}

#[test]
fn redact_replaces_secret_string_data_values() {
    let input = br#"{"kind":"Secret","stringData":{"pw":"hunter2"}}"#;
    let out = redact_secret_like(input);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["stringData"]["pw"], "<redacted len=7>");
}

#[test]
fn redact_walks_secret_list_items() {
    let input = br#"{"kind":"SecretList","items":[
        {"kind":"Secret","data":{"k":"YWI="}},
        {"kind":"Secret","stringData":{"k":"v"}}
    ]}"#;
    let out = redact_secret_like(input);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["items"][0]["data"]["k"], "<redacted len=2>");
    assert_eq!(v["items"][1]["stringData"]["k"], "<redacted len=1>");
}

#[test]
fn redact_walks_admission_review_object() {
    let input = br#"{"kind":"AdmissionReview","request":{
        "object":{"kind":"Secret","data":{"k":"YWI="}},
        "oldObject":{"kind":"Secret","stringData":{"k":"x"}}
    }}"#;
    let out = redact_secret_like(input);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["request"]["object"]["data"]["k"], "<redacted len=2>");
    assert_eq!(v["request"]["oldObject"]["stringData"]["k"], "<redacted len=1>");
}

#[test]
fn redact_passthrough_for_malformed_json() {
    let input = b"not json at all";
    assert_eq!(&*redact_secret_like(input), input);
}

#[test]
fn redact_leaves_configmap_alone() {
    let input = br#"{"kind":"ConfigMap","data":{"k":"v"}}"#;
    let out = redact_secret_like(input);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["data"]["k"], "v");
}
```

- [ ] **Step 2: Run tests to confirm failure**

Run:
```bash
cargo test -p rusternetes-common dump::tests::redact -- --nocapture
```
Expected: compile error — `redact_secret_like` not in scope.

- [ ] **Step 3: Implement `redact_secret_like`**

Append to `crates/common/src/dump.rs` (above the `tests` module):
```rust
use std::borrow::Cow;

/// Length of a base64-encoded payload's decoded bytes, or the encoded length
/// if decoding fails. Used for the redaction marker so the dump still hints
/// at the value's size.
fn decoded_len(b64: &str) -> usize {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map(|v| v.len())
        .unwrap_or_else(|_| b64.len())
}

fn redact_data_map(map: &mut serde_json::Map<String, serde_json::Value>) {
    for v in map.values_mut() {
        if let Some(s) = v.as_str() {
            let n = decoded_len(s);
            *v = serde_json::Value::String(format!("<redacted len={n}>"));
        }
    }
}

fn redact_string_data_map(map: &mut serde_json::Map<String, serde_json::Value>) {
    for v in map.values_mut() {
        if let Some(s) = v.as_str() {
            let n = s.len();
            *v = serde_json::Value::String(format!("<redacted len={n}>"));
        }
    }
}

fn redact_one(value: &mut serde_json::Value) {
    let Some(obj) = value.as_object_mut() else { return };
    let kind = obj.get("kind").and_then(|k| k.as_str()).unwrap_or("").to_string();
    match kind.as_str() {
        "Secret" => {
            if let Some(serde_json::Value::Object(m)) = obj.get_mut("data") {
                redact_data_map(m);
            }
            if let Some(serde_json::Value::Object(m)) = obj.get_mut("stringData") {
                redact_string_data_map(m);
            }
        }
        "SecretList" => {
            if let Some(serde_json::Value::Array(items)) = obj.get_mut("items") {
                for item in items {
                    redact_one(item);
                }
            }
        }
        "AdmissionReview" => {
            if let Some(serde_json::Value::Object(req)) = obj.get_mut("request") {
                for key in ["object", "oldObject"] {
                    if let Some(v) = req.get_mut(key) {
                        redact_one(v);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Replace `data` / `stringData` values in any embedded `Secret` with
/// `"<redacted len=N>"`. Pass-through for non-JSON, non-Secret bodies.
pub fn redact_secret_like(bytes: &[u8]) -> Cow<'_, [u8]> {
    let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Cow::Borrowed(bytes);
    };
    let before = v.clone();
    redact_one(&mut v);
    if v == before {
        return Cow::Borrowed(bytes);
    }
    match serde_json::to_vec(&v) {
        Ok(out) => Cow::Owned(out),
        Err(_) => Cow::Borrowed(bytes),
    }
}
```

- [ ] **Step 4: Add `base64` dep if missing**

Run:
```bash
grep '^base64' crates/common/Cargo.toml
```
If absent, add to `[dependencies]`:
```toml
base64 = "0.22"
```

- [ ] **Step 5: Run tests**

Run:
```bash
cargo test -p rusternetes-common dump::tests::redact -- --nocapture
```
Expected: 7 tests pass.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/common/src/dump.rs crates/common/Cargo.toml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(common): redact Secret data in dump payloads"
```

---

## Task 3: Add `CURRENT_PAYLOAD` task_local and `with_payload`

**Files:**
- Modify: `crates/common/src/dump.rs`

- [ ] **Step 1: Write the failing test**

Append to `tests` module:
```rust
#[tokio::test]
async fn with_payload_makes_bytes_visible_inside_scope() {
    let body = bytes::Bytes::from_static(b"hello");
    let seen = with_payload(body.clone(), async {
        CURRENT_PAYLOAD.with(|cell| cell.borrow().clone())
    })
    .await;
    assert_eq!(seen.as_deref(), Some(b"hello".as_ref()));
}

#[tokio::test]
async fn current_payload_outside_scope_returns_err() {
    let res = CURRENT_PAYLOAD.try_with(|cell| cell.borrow().clone());
    assert!(res.is_err());
}
```

- [ ] **Step 2: Implement**

Append above `tests`:
```rust
use std::cell::RefCell;

tokio::task_local! {
    pub static CURRENT_PAYLOAD: RefCell<Option<bytes::Bytes>>;
}

/// Run `fut` with `body` accessible via `CURRENT_PAYLOAD` for the duration
/// of the future (and any tasks it spawns that inherit the task-local).
pub async fn with_payload<F, T>(body: bytes::Bytes, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CURRENT_PAYLOAD.scope(RefCell::new(Some(body)), fut).await
}
```

- [ ] **Step 3: Verify `tokio` features include `macros`**

Run:
```bash
grep '^tokio' crates/common/Cargo.toml
```
If `macros` not in features list, add it. (Needed for `tokio::task_local!` and `#[tokio::test]`.)

- [ ] **Step 4: Run tests**

Run:
```bash
cargo test -p rusternetes-common dump::tests::with_payload \
                                dump::tests::current_payload
```
Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/common/src/dump.rs crates/common/Cargo.toml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(common): add CURRENT_PAYLOAD task_local and with_payload"
```

---

## Task 4: Install panic hook

**Files:**
- Modify: `crates/common/src/dump.rs`

- [ ] **Step 1: Write the failing test**

Append to `tests`:
```rust
#[tokio::test]
async fn panic_hook_logs_payload_under_scope() {
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Default, Clone)]
    struct Capture(Arc<Mutex<Vec<String>>>);

    impl<S> tracing_subscriber::Layer<S> for Capture
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = StrVisitor::default();
            event.record(&mut visitor);
            self.0.lock().unwrap().push(visitor.0);
        }
    }

    #[derive(Default)]
    struct StrVisitor(String);
    impl tracing::field::Visit for StrVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write;
            let _ = write!(self.0, " {}={:?}", field.name(), value);
        }
    }

    let capture = Capture::default();
    let sub = tracing_subscriber::registry().with(capture.clone());
    let _guard = tracing::subscriber::set_default(sub);

    install_panic_hook("test-component");

    let body = bytes::Bytes::from_static(br#"{"kind":"Pod"}"#);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        futures::executor::block_on(with_payload(body, async {
            panic!("boom");
        }))
    }));

    let logs = capture.0.lock().unwrap().clone();
    assert!(
        logs.iter().any(|l| l.contains("test-component") && l.contains("Pod")),
        "no dump log captured; got: {logs:?}"
    );
}
```

If `futures` is not already a dev-dependency, add to `[dev-dependencies]` of `crates/common/Cargo.toml`:
```toml
futures = "0.3"
```

- [ ] **Step 2: Implement `install_panic_hook`**

Append above `tests`:
```rust
use std::sync::Once;

static INSTALL: Once = Once::new();

/// Install a panic hook that, when a panic fires inside a `with_payload`
/// scope, emits one `tracing::error!` with the component name and the
/// redacted payload. Chains over (does not replace) the previous hook so
/// the default backtrace continues to print. Safe to call multiple times;
/// only the first call wins.
pub fn install_panic_hook(component: &'static str) {
    INSTALL.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = CURRENT_PAYLOAD
                .try_with(|cell| cell.borrow().clone())
                .ok()
                .flatten();
            if let Some(body) = payload {
                let redacted = redact_secret_like(&body);
                let preview = String::from_utf8_lossy(&redacted);
                tracing::error!(
                    component = component,
                    panic = %info,
                    payload = %preview,
                    "panic with in-flight payload"
                );
            }
            prev(info);
        }));
    });
}
```

- [ ] **Step 3: Run test**

Run:
```bash
cargo test -p rusternetes-common dump::tests::panic_hook -- --nocapture --test-threads=1
```
Expected: 1 test passes. (`--test-threads=1` because the panic hook is process-global.)

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/common/src/dump.rs crates/common/Cargo.toml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(common): install_panic_hook dumps in-flight payload"
```

---

## Task 5: `DumpingJson<T>` extractor in common

**Files:**
- Modify: `crates/common/src/dump.rs`

- [ ] **Step 1: Write the failing test**

Append to `tests`:
```rust
#[tokio::test]
async fn dumping_json_decodes_valid_body() {
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::{header, Request};

    #[derive(serde::Deserialize)]
    struct Foo { x: i32 }

    let req = Request::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"x":7}"#))
        .unwrap();
    let DumpingJson(foo) = DumpingJson::<Foo>::from_request(req, &()).await.unwrap();
    assert_eq!(foo.x, 7);
}

#[tokio::test]
async fn dumping_json_rejects_invalid_body_with_same_status_as_json() {
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::{header, Request, StatusCode};
    use axum::response::IntoResponse;

    #[derive(serde::Deserialize)]
    struct Foo { _x: i32 }

    let req = Request::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("not json"))
        .unwrap();
    let err = DumpingJson::<Foo>::from_request(req, &()).await.unwrap_err();
    let resp = err.into_response();
    assert!(resp.status() == StatusCode::BAD_REQUEST
        || resp.status() == StatusCode::UNPROCESSABLE_ENTITY);
}
```

- [ ] **Step 2: Implement `DumpingJson<T>`**

Append above `tests`:
```rust
use axum::{
    body::{Body, Bytes as AxumBytes},
    extract::{rejection::JsonRejection, FromRequest, Request},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};

/// Drop-in replacement for `axum::Json<T>` that, when payload dumps are
/// enabled, buffers the request body into `CURRENT_PAYLOAD` before
/// delegating to `axum::Json<T>` and logs the body on decode failure.
pub struct DumpingJson<T>(pub T);

impl<T, S> FromRequest<S> for DumpingJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = DumpingJsonRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        if !dumps_enabled() {
            // Fast path: just delegate to axum::Json.
            let Json(t) = Json::<T>::from_request(req, state)
                .await
                .map_err(DumpingJsonRejection::Json)?;
            return Ok(DumpingJson(t));
        }

        // Slow path: buffer body, store, then re-create a request for Json.
        let (parts, body) = req.into_parts();
        let bytes = AxumBytes::from_request(
            Request::from_parts(parts.clone(), body),
            state,
        )
        .await
        .map_err(|_| DumpingJsonRejection::BodyRead)?;

        // Stash in task-local for the panic-hook path.
        let _ = CURRENT_PAYLOAD.try_with(|cell| {
            *cell.borrow_mut() = Some(bytes.clone());
        });

        let rebuilt = Request::from_parts(parts, Body::from(bytes.clone()));
        match Json::<T>::from_request(rebuilt, state).await {
            Ok(Json(t)) => Ok(DumpingJson(t)),
            Err(rej) => {
                let redacted = redact_secret_like(&bytes);
                tracing::error!(
                    rejection = %rej,
                    payload = %String::from_utf8_lossy(&redacted),
                    "JSON body decode failed"
                );
                Err(DumpingJsonRejection::Json(rej))
            }
        }
    }
}

#[derive(Debug)]
pub enum DumpingJsonRejection {
    Json(JsonRejection),
    BodyRead,
}

impl IntoResponse for DumpingJsonRejection {
    fn into_response(self) -> Response {
        match self {
            Self::Json(r) => r.into_response(),
            Self::BodyRead => (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "text/plain")],
                "failed to read request body",
            )
                .into_response(),
        }
    }
}

impl std::fmt::Display for DumpingJsonRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(r) => write!(f, "{r}"),
            Self::BodyRead => write!(f, "failed to read request body"),
        }
    }
}

impl std::error::Error for DumpingJsonRejection {}
```

- [ ] **Step 3: Run tests**

Run:
```bash
cargo test -p rusternetes-common dump::tests::dumping_json
```
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/common/src/dump.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(common): DumpingJson extractor logs body on decode fail"
```

---

## Task 6: api-server `capture_payload` middleware

**Files:**
- Create: `crates/api-server/src/middleware/dump.rs`
- Create/Modify: `crates/api-server/src/middleware/mod.rs`
- Modify: `crates/api-server/src/lib.rs` (or wherever the `middleware` parent is declared)

- [ ] **Step 1: Determine middleware module location**

Run:
```bash
grep -n "pub mod middleware\|mod middleware" crates/api-server/src/lib.rs crates/api-server/src/main.rs 2>/dev/null
```
If `middleware` module does not exist yet, add `pub mod middleware;` to `crates/api-server/src/lib.rs` and create `crates/api-server/src/middleware/mod.rs` containing `pub mod dump;`.

If it already exists, just append `pub mod dump;` to the existing `mod.rs`.

- [ ] **Step 2: Write the failing integration test**

Create `crates/api-server/tests/payload_dump.rs`:
```rust
//! Integration tests for the conformance payload-dump middleware.
//!
//! Each test sets `RUSTERNETES_DUMP_PAYLOADS=1` via a process-wide guard
//! before the first call to `dumps_enabled()`. Tests run with
//! `--test-threads=1` because the env gate is read once per process.

use axum::{body::Body, http::Request, routing::post, Router};
use rusternetes_api_server::middleware::dump::capture_payload;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Default, Clone)]
struct LogSink(Arc<Mutex<Vec<String>>>);

impl<S> tracing_subscriber::Layer<S> for LogSink
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
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

fn install_capture() -> LogSink {
    std::env::set_var("RUSTERNETES_DUMP_PAYLOADS", "1");
    let sink = LogSink::default();
    let sub = tracing_subscriber::registry().with(sink.clone());
    let _ = tracing::subscriber::set_global_default(sub);
    sink
}

#[tokio::test]
async fn dumps_body_on_5xx() {
    let sink = install_capture();
    let app = Router::new()
        .route("/boom", post(|_b: String| async {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "kaboom")
        }))
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
        logs.iter().any(|l| l.contains("sentinel") && l.contains("5xx")),
        "expected 5xx dump containing sentinel; got {logs:?}"
    );
}

#[tokio::test]
async fn redacts_secret_data_on_5xx() {
    let sink = install_capture();
    let app = Router::new()
        .route("/boom", post(|_b: String| async {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "kaboom")
        }))
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
        logs.iter().any(|l| l.contains("redacted len=4") && !l.contains("YWJjZA==")),
        "expected redacted secret in logs; got {logs:?}"
    );
}

#[tokio::test]
async fn does_not_dump_on_2xx() {
    let sink = install_capture();
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
```

- [ ] **Step 3: Run tests to confirm compile failure**

Run:
```bash
cargo test -p rusternetes-api-server --test payload_dump
```
Expected: compile error — `capture_payload` not found.

- [ ] **Step 4: Implement `capture_payload`**

Create `crates/api-server/src/middleware/dump.rs`:
```rust
//! Request-body capture middleware for conformance payload dumps.
//!
//! Outermost layer in the router. Buffers the request body (up to
//! `MAX_DUMP_BODY`) into `rusternetes_common::dump::CURRENT_PAYLOAD`,
//! then logs the body on any 5xx response. No-op when
//! `RUSTERNETES_DUMP_PAYLOADS` is unset.

use axum::{
    body::{to_bytes, Body},
    extract::Request,
    middleware::Next,
    response::Response,
};
use rusternetes_common::dump::{self, redact_secret_like, CURRENT_PAYLOAD};
use std::cell::RefCell;

/// 4 MiB — matches Kubernetes' default max request size.
const MAX_DUMP_BODY: usize = 4 * 1024 * 1024;

pub async fn capture_payload(req: Request, next: Next) -> Response {
    if !dump::dumps_enabled() {
        return next.run(req).await;
    }

    let (parts, body) = req.into_parts();
    let bytes = match to_bytes(body, MAX_DUMP_BODY).await {
        Ok(b) => b,
        Err(_) => {
            // Body too large or read failed: pass through with no payload.
            return CURRENT_PAYLOAD
                .scope(RefCell::new(None), async {
                    next.run(Request::from_parts(parts, Body::empty())).await
                })
                .await;
        }
    };

    let rebuilt = Request::from_parts(parts.clone(), Body::from(bytes.clone()));
    let bytes_for_scope = bytes.clone();
    let resp = CURRENT_PAYLOAD
        .scope(RefCell::new(Some(bytes_for_scope)), next.run(rebuilt))
        .await;

    if resp.status().is_server_error() {
        let redacted = redact_secret_like(&bytes);
        tracing::error!(
            method = %parts.method,
            uri = %parts.uri,
            status = %resp.status(),
            kind = "5xx",
            payload = %String::from_utf8_lossy(&redacted),
            "request handler returned 5xx"
        );
    }

    resp
}
```

- [ ] **Step 5: Run tests**

Run:
```bash
cargo test -p rusternetes-api-server --test payload_dump -- --test-threads=1
```
Expected: 3 tests pass.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/api-server/src/middleware/ crates/api-server/src/lib.rs \
        crates/api-server/tests/payload_dump.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(api-server): capture_payload middleware dumps on 5xx"
```

---

## Task 7: Migrate handlers from `Json<T>` to `DumpingJson<T>` (extractor sites only)

**Files:**
- Modify: every file under `crates/api-server/src/handlers/` that uses `Json(x): Json<T>` as a function-parameter extractor.

The response-side `Json<T>` (e.g. `Result<Json<Pod>>` return type) must NOT change — `DumpingJson` is extractor-only.

- [ ] **Step 1: Inspect current extractor sites**

Run:
```bash
grep -rEn 'Json\([A-Za-z_][A-Za-z0-9_]*\): Json<' crates/api-server/src/handlers/ | wc -l
```
Record the count.

- [ ] **Step 2: Mechanical rewrite of extractor sites**

Run:
```bash
perl -i -pe 's/\bJson\((mut\s+)?([A-Za-z_][A-Za-z0-9_]*)\): Json</DumpingJson($1$2): DumpingJson</g' \
  crates/api-server/src/handlers/*.rs
```

This rewrites `Json(x): Json<T>` and `Json(mut x): Json<T>` → `DumpingJson(...): DumpingJson<T>` and leaves `Result<Json<T>>` return types untouched. If a handler uses a tuple-destructure form like `Json((a, b)): Json<(A, B)>`, the regex skips it — fix those (rare) by hand after the build error.

- [ ] **Step 3: Verify no response-type sites were touched**

Run:
```bash
grep -rn 'DumpingJson<' crates/api-server/src/handlers/ | grep -v ': DumpingJson<' || echo OK
```
Expected: `OK` (every occurrence is on the `: DumpingJson<` extractor side).

- [ ] **Step 4: Add `DumpingJson` import everywhere it is now referenced**

Run:
```bash
for f in $(grep -rln 'DumpingJson' crates/api-server/src/handlers/); do
  if ! grep -q 'use rusternetes_common::dump::DumpingJson' "$f"; then
    # Insert after the first `use` line.
    awk 'NR==FNR{next} /^use / && !ins { print; print "use rusternetes_common::dump::DumpingJson;"; ins=1; next } { print }' "$f" "$f" > "$f.tmp" && mv "$f.tmp" "$f"
  fi
done
```

- [ ] **Step 5: Build**

Run:
```bash
cargo build -p rusternetes-api-server
```
Expected: clean build. If a handler had a non-standard `Json` use (e.g. an alias) and the perl rewrite missed it, the compiler will pinpoint the file; fix manually.

- [ ] **Step 6: Run the existing api-server test suite**

Run:
```bash
cargo test -p rusternetes-api-server
```
Expected: all tests still pass — `DumpingJson` is a transparent stand-in.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/api-server/src/handlers/
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "refactor(api-server): use DumpingJson extractor in handlers"
```

---

## Task 8: Wire api-server main + router

**Files:**
- Modify: `crates/api-server/src/main.rs` (~L111, after `init_basic_tracing`)
- Modify: `crates/api-server/src/router.rs` (~L2405, where `app` is built)

- [ ] **Step 1: Add panic hook install in `main.rs`**

In `crates/api-server/src/main.rs`, immediately after the existing `init_basic_tracing(...)?;` call, add:
```rust
    rusternetes_common::dump::install_panic_hook("api-server");
```

- [ ] **Step 2: Add middleware layer in `router.rs`**

Find the line `let mut app = Router::new().merge(public_routes).merge(protected_routes);` (~L2405) and append:
```rust
    let app = app.layer(axum::middleware::from_fn(
        crate::middleware::dump::capture_payload,
    ));
```
(Drop the `mut` on the prior line if it becomes unused after this addition.)

- [ ] **Step 3: Build**

Run:
```bash
cargo build -p rusternetes-api-server
```
Expected: clean build.

- [ ] **Step 4: Manual smoke (optional, local only)**

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
export RUSTERNETES_DUMP_PAYLOADS=1
docker compose -f compose.yml -f compose.dind.yml up -d --build api-server
sleep 5
curl -sk -X POST -H 'content-type: application/json' \
  --data 'garbage' https://localhost:6443/api/v1/namespaces/default/pods || true
docker compose -f compose.yml -f compose.dind.yml logs api-server | tail -20
```
Expected: log contains `JSON body decode failed` and the literal string `garbage`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/api-server/src/main.rs crates/api-server/src/router.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(api-server): wire panic hook + capture_payload layer"
```

---

## Task 9: Wire kubelet

**Files:**
- Modify: `crates/kubelet/src/main.rs` (~L263, after `init_basic_tracing`)
- Modify: `crates/kubelet/src/kubelet.rs` (~L911 and ~L1324, the two `sync_pod` call sites)

- [ ] **Step 1: Add panic hook install**

In `crates/kubelet/src/main.rs`, immediately after the existing `init_basic_tracing(...)?;` call, add:
```rust
    rusternetes_common::dump::install_panic_hook("kubelet");
```

- [ ] **Step 2: Wrap `sync_pod` call site #1 (~L911)**

Both `sync_pod` call sites pass the future into `tokio::time::timeout(...)`. The future passed to `timeout` must be re-wrapped in `with_payload`. Locate:
```rust
let result = tokio::time::timeout(
    std::time::Duration::from_secs(timeout_secs),
    kubelet.sync_pod(&pod),
)
.await;
```
Replace with:
```rust
let result = tokio::time::timeout(
    std::time::Duration::from_secs(timeout_secs),
    rusternetes_common::dump::with_payload(
        serde_json::to_vec(&pod).unwrap_or_default().into(),
        kubelet.sync_pod(&pod),
    ),
)
.await;
```

- [ ] **Step 3: Wrap `sync_pod` call site #2 (~L1324)**

Apply the same transformation. The exact lines are:
```rust
match tokio::time::timeout(
    Duration::from_secs(timeout_secs),
    kubelet.sync_pod(&pod),
)
.await
```
becomes:
```rust
match tokio::time::timeout(
    Duration::from_secs(timeout_secs),
    rusternetes_common::dump::with_payload(
        serde_json::to_vec(&pod).unwrap_or_default().into(),
        kubelet.sync_pod(&pod),
    ),
)
.await
```

- [ ] **Step 4: Build**

Run:
```bash
cargo build -p rusternetes-kubelet
```
Expected: clean build.

- [ ] **Step 5: Run the kubelet test suite**

Run:
```bash
cargo test -p rusternetes-kubelet
```
Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/kubelet/src/main.rs crates/kubelet/src/kubelet.rs
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "feat(kubelet): scope sync_pod in CURRENT_PAYLOAD for panic dumps"
```

---

## Task 10: Enable env var in conformance scripts + CI

**Files:**
- Modify: `scripts/conformance-canary-run.sh`
- Modify: `.github/workflows/conformance-canary.yml`

- [ ] **Step 1: Add env export to the canary script**

Open `scripts/conformance-canary-run.sh`. Near the top, after the `set -euo pipefail` (or equivalent strict-mode line), add:
```bash
# Enable payload dumps in api-server/kubelet so any panic / 5xx / decode
# failure during conformance logs the offending request body.
export RUSTERNETES_DUMP_PAYLOADS=1
```

- [ ] **Step 2: Pass env var into compose services in CI**

In `.github/workflows/conformance-canary.yml`, locate the step that runs `docker compose ... up -d` and add an `env:` block alongside it:
```yaml
      - name: Bring up cluster
        env:
          RUSTERNETES_DUMP_PAYLOADS: "1"
        run: |
          export KUBELET_VOLUMES_PATH=$PWD/.rusternetes/volumes
          docker compose -f compose.yml -f compose.dind.yml up -d
```

Verify (or add) that `compose.yml`'s `api-server` and `kubelet-*` services inherit the variable via either:
- an `environment:` entry `RUSTERNETES_DUMP_PAYLOADS: ${RUSTERNETES_DUMP_PAYLOADS:-}`, or
- adding `RUSTERNETES_DUMP_PAYLOADS` to each service's existing `env_file`.

Use whichever pattern the other tunables (e.g. `RUST_LOG`) already use in `compose.yml` for consistency.

- [ ] **Step 3: Verify shell script is still valid**

Run:
```bash
bash -n scripts/conformance-canary-run.sh
```
Expected: no output (syntactically valid).

- [ ] **Step 4: Commit**

```bash
git add scripts/conformance-canary-run.sh .github/workflows/conformance-canary.yml compose.yml
GIT_AUTHOR_NAME='Indy Jones' GIT_AUTHOR_EMAIL='indyjonesnl@gmail.com' \
GIT_COMMITTER_NAME='Indy Jones' GIT_COMMITTER_EMAIL='indyjonesnl@gmail.com' \
git commit -m "chore(conformance): enable RUSTERNETES_DUMP_PAYLOADS in canary"
```

---

## Final verification

- [ ] **Pre-push:**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -p rusternetes-common
cargo test -p rusternetes-api-server
cargo test -p rusternetes-kubelet
```
All must pass.

- [ ] **Branch audit:**

```bash
git log fork/main..HEAD --format="%h %an <%ae> | %cn <%ce>"
```
Every line must read `Indy Jones <indyjonesnl@gmail.com>` in both columns.

- [ ] **Open PR against `indyjonesnl/rusternetes`:**

```bash
git push -u fork feat/tracing-envfilter
gh pr create --repo indyjonesnl/rusternetes \
  --base main \
  --title "feat: dump request payload on panic/5xx/decode-fail during conformance" \
  --body "$(cat <<'EOF'
## Summary
- Adds `rusternetes_common::dump`: `CURRENT_PAYLOAD` task_local, redaction, panic hook, `DumpingJson` extractor.
- api-server: outermost `capture_payload` middleware logs body on 5xx; handler `Json<T>` extractors swapped for `DumpingJson<T>` so decode failures log the body.
- kubelet: `sync_pod` call sites scoped with `with_payload` so panics dump the pod spec.
- Gated by `RUSTERNETES_DUMP_PAYLOADS=1`; enabled in `conformance-canary-run.sh` and the canary workflow.

## Spec
`docs/superpowers/specs/2026-05-23-conformance-payload-dumps-design.md`

## Test plan
- [ ] `cargo test -p rusternetes-common dump::`
- [ ] `cargo test -p rusternetes-api-server --test payload_dump -- --test-threads=1`
- [ ] Conformance canary green in CI
- [ ] Smoke: locally `RUSTERNETES_DUMP_PAYLOADS=1 docker compose ...up`, POST garbage to /api/v1/..., observe dump in `docker compose logs api-server`
EOF
)"
```
