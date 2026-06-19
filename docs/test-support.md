# Shared test fixtures: `rusternetes-test-support`

`crates/test_support` consolidates fixtures that test files across the workspace
were duplicating, and is the substrate for porting upstream Kubernetes Go unit
tests into idiomatic Rust. It is wired into other crates **only** under
`[dev-dependencies]`.

## What it provides

- **Builders** (`rusternetes_test_support::builders`, default feature) —
  JSON-backed `pod()`, `service()`, `node()`, `endpoint_slice()` with fluent
  setters (`namespace`, `label`, `container`, `restart_policy`, `merge`, …) and
  `build::<T>()`. JSON-backed because the resource structs deserialize but don't
  all derive `Default`, and JSON gives precise control over the (often
  deliberately invalid) inputs validation tests need.
- **Harness** (`rusternetes_test_support::harness`, feature
  `apiserver-harness`) — `TestApiServer` boots the real `build_router` on
  `MemoryStorage` with `--skip-auth` and drives it via `tower::oneshot`. It is the
  canonical in-process api-server harness: every MemoryStorage-backed test file
  under `crates/api-server/tests/` drives it (the sole exception,
  `watch_delivery_rhino_test.rs`, deliberately exercises the SQLite/Rhino backend
  the MemoryStorage harness doesn't model).

## Using the harness

```rust
use rusternetes_test_support::harness::TestApiServer;

#[tokio::test]
async fn my_router_test() {
    let api = TestApiServer::new();
    let (status, body) = api.post("/api/v1/namespaces", &json!({ /* … */ })).await;
    let (status, raw, body) = api.send_raw("GET", "/api/v1/pods", None, None).await;
}
```

`api.storage` exposes the backing `Arc<MemoryStorage>` for direct
seeding/assertions, and `TestApiServer` is `Clone` (cheap — the `Arc` and the
`axum::Router` clone to handles over the *same* storage), so a test can drive
concurrent requests / spawn a background watcher while mutating on the main task.

### Request primitives (lowest → highest level)

- `send_with_headers(method, uri, &[(name, value)], body: Option<Vec<u8>>)` —
  fullest primitive; arbitrary request headers + raw body, returns
  `(StatusCode, HeaderMap, bytes, Value)`. Use for impersonation / bearer /
  `Accept-Encoding` / multi-header cases and for asserting on **response**
  headers.
- `respond(method, uri, content_type, body) -> axum::response::Response` — returns
  the un-consumed `Response` so the caller can stream the body
  (`resp.into_body().into_data_stream()`). The only primitive that works for a
  non-self-closing `?watch=true` stream; the buffering helpers below block to EOF.
- `send_full(method, uri, content_type, accept, body)` — convenience over
  `send_with_headers` for the common content-type + `Accept` case; returns
  `(StatusCode, HeaderMap, bytes, Value)`.
- `send_bytes(method, uri, content_type, body: Option<Vec<u8>>)` — raw byte body
  (malformed JSON, non-UTF-8, protobuf/CBOR/YAML wire bytes); returns
  `(StatusCode, bytes, Value)`.
- `send_raw(method, uri, content_type, body: Option<&Value>)` — serialises a
  `Value` to JSON bytes; returns `(StatusCode, bytes, Value)`.
- `send(method, uri, content_type, body)` — as `send_raw` but drops the bytes.
- `get` / `post` / `put` / `patch` (merge-patch+json) / `delete` — JSON
  conveniences over `send`. A self-closing `?watch=true&timeoutSeconds=N` watch
  can be collected through `send_full`/`get` since the body arrives at EOF.

### Non-default harnesses (the builder)

`TestApiServer::builder()` covers the auth/RBAC/CA shapes (`new()` is
`builder().build()`):

```rust
// real bearer-token + RBAC pipeline
let api = TestApiServer::builder().rbac().skip_auth(false).secret(b"my-secret").build();
// inject a CA cert so the namespace handler seeds kube-root-ca.crt
let api = TestApiServer::builder().ca_cert_pem(pem).build();
```

`.skip_auth(bool)` · `.rbac()` (real `RBACAuthorizer` over the same storage) ·
`.secret(&[u8])` (token-signing secret — match a test's own `TokenManager`) ·
`.ca_cert_pem(..)`.

Add it to a crate that needs the harness:

```toml
[dev-dependencies]
rusternetes-test-support = { path = "../test_support", features = ["apiserver-harness"] }
```

This forms a dev-dependency cycle (`test_support` → `api-server` as a normal dep,
`api-server` → `test_support` as a dev-dep). Cargo permits dev-dependency cycles,
so it builds fine; crates that only need builders omit the feature and don't pull
api-server in.

## Migrating an existing test onto the harness

Replace the per-file `make_state` / `router_for` / `send` / `post` / `get` …
helpers with `TestApiServer`:

- `let state = make_state();` → `let state = TestApiServer::new();`
- `post(&state, uri, &body)` → `state.post(uri, &body)`
- a custom `get_list` returning raw bytes → `state.send_raw("GET", uri, None, None)`
- a streaming `?watch=true` collector that did `router.oneshot(req)…into_data_stream()`
  → `let resp = api.respond("GET", uri, None, None).await;` then keep the existing
  stream-collection loop
- a proto/CBOR/YAML wire-body POST → `api.send_bytes(method, uri, Some(ct), Some(bytes))`
- a test that asserts on a response header (e.g. strict-decoding `Warning:`,
  negotiated `Content-Type`) → `send_full` / `send_with_headers` and read the
  returned `HeaderMap`

Then drop the now-unused imports (`build_router`, `ApiServerState`,
`TokenManager`, `StorageBackend`, `tower`, `axum::body`, …). See
`crates/api-server/tests/runtimeclass_router_test.rs` and
`list_resource_version_router_test.rs` for the basic shape, and
`watch_event_envelope_test.rs` (streaming), `conformance_pod_proto_response_test.rs`
(protobuf), `conformance_network_services_proxy.rs` (a `TcpListener` proxy
backend) for the harder cases.

External servers stay put. When a test spins a real backend — a `TcpListener`
fake kubelet / proxy target, or a `warp` aggregated-apiserver / admission-webhook
server — that server is the *thing under test's dependency*, not the harness:
migrate only the api-server router calls onto `TestApiServer` and leave the
external server (and any `AdmissionWebhookManager` built from `api.storage`) as-is.

## Porting upstream Go tests

When porting from `../kubernetes` (release-1.35), reimplement idiomatically —
never copy verbatim. Translate Go table structs into Rust (a `Vec<Case>` loop, or
the repo's one-test-per-case idiom), Go `require/assert` into Rust assertions, and
Go fakes into Rust analogs in this crate. Preserve upstream test-case names and
expected strings/rule text as the contract (substring matching where upstream's
`ErrorMatcher` does). Cite the source by GitHub URL in a doc-comment, e.g.
`https://github.com/kubernetes/kubernetes/blob/release-1.35/<go path>`. Both
projects are Apache-2.0; add an attribution header on substantially-derived files.
