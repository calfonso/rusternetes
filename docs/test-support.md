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
  `MemoryStorage` with `--skip-auth` and drives it via `tower::oneshot`.

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

`send_raw(method, uri, content_type, body)` is the low-level primitive (returns
status + raw bytes + parsed JSON); `get` / `post` / `put` / `patch`
(merge-patch+json) / `delete` are conveniences over it. `api.storage` exposes the
backing `MemoryStorage` for direct seeding/assertions.

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

Then drop the now-unused imports (`build_router`, `ApiServerState`,
`TokenManager`, `StorageBackend`, `tower`, `axum::body`, …). See
`crates/api-server/tests/runtimeclass_router_test.rs` and
`list_resource_version_router_test.rs` for migrated examples.

## Porting upstream Go tests

When porting from `../kubernetes` (release-1.35), reimplement idiomatically —
never copy verbatim. Translate Go table structs into Rust (a `Vec<Case>` loop, or
the repo's one-test-per-case idiom), Go `require/assert` into Rust assertions, and
Go fakes into Rust analogs in this crate. Preserve upstream test-case names and
expected strings/rule text as the contract (substring matching where upstream's
`ErrorMatcher` does). Cite the source by GitHub URL in a doc-comment, e.g.
`https://github.com/kubernetes/kubernetes/blob/release-1.35/<go path>`. Both
projects are Apache-2.0; add an attribution header on substantially-derived files.
