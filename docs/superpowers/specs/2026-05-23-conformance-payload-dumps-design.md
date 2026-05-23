# Conformance payload dumps on panic / 5xx / decode failure

**Status:** Draft
**Date:** 2026-05-23
**Branch:** `feat/tracing-envfilter`

## Problem

When a conformance test triggers a panic, a 5xx response, or a request body
decode failure in api-server or kubelet, today the operator sees a stack trace
or an opaque error message — but not the JSON / YAML payload that caused it.
Reconstructing the offending object from the conformance test source is slow.

The goal: on any of those three failure modes, log the request body (api-server)
or the object being reconciled (kubelet) alongside the existing error, so the
payload is one `grep` away in container / CI logs.

## Non-goals

- Capturing payloads on process-level crashes (SIGSEGV, SIGKILL, OOM kill).
  A Rust panic hook only fires for unwinding panics; abort-on-OOM and
  signal-delivered termination are out of scope.
- Persisting dumps to disk or a dump directory — stderr / tracing only.
- Capturing payloads in scheduler, controller-manager, kube-proxy, or
  cloud-providers. Those components do not receive client-supplied bodies
  directly; their inputs come from the api-server watch stream and are easier
  to reproduce from the api-server logs.

## Triggers

A payload dump is emitted when any of the following happens **and**
`RUSTERNETES_DUMP_PAYLOADS=1` is set in the component's environment:

1. **Panic** — any unwinding `panic!` (or equivalent) inside a task that is
   currently scoped with a captured payload.
2. **5xx response** — any HTTP response with `status >= 500` returned by the
   api-server router.
3. **Decode failure** — any request body that fails JSON deserialization in an
   api-server handler.

When the env var is unset (the default), all instrumentation is a no-op: no
body buffering, no scope wrapping, no extra allocations beyond a `OnceLock`
read.

## Sink

`tracing::error!` events on stderr. The events land in:

- Local dev: `docker compose logs api-server` / `docker compose logs kubelet-1`.
- CI: the existing conformance-canary workflow uploads container logs as
  artifacts; the dump events are inside those artifacts unchanged.

No new files, no new mounts, no new artifact uploads.

## Redaction

Before logging, the payload is passed through `redact_secret_like`:

- `Secret` objects (`kind == "Secret"`): every value in `data` and
  `stringData` is replaced with the literal string `"<redacted len=N>"`,
  where `N` is the decoded byte length for `data` and the raw character
  length for `stringData`. Keys are preserved (they are field names, not
  secret values).
- `SecretList`: same treatment applied to each item.
- `AdmissionReview`: `request.object` and `request.oldObject` are walked
  recursively; embedded Secrets get the same treatment.
- All other resource kinds (including `ConfigMap`) pass through unchanged.
  Conformance ConfigMaps are public test fixtures and are often the bug.
- Non-JSON payloads (proto bytes, malformed JSON) pass through unchanged.

The redaction function is pure, sync, and unit-testable. No I/O.

## Architecture

### New module: `crates/common/src/dump.rs`

```rust
tokio::task_local! {
    pub static CURRENT_PAYLOAD: std::cell::RefCell<Option<bytes::Bytes>>;
}

pub fn dumps_enabled() -> bool;
pub fn install_panic_hook(component: &'static str);
pub async fn with_payload<F, T>(body: bytes::Bytes, fut: F) -> T
where F: std::future::Future<Output = T>;
pub fn redact_secret_like(body: &[u8]) -> std::borrow::Cow<'_, [u8]>;
```

- `dumps_enabled()` reads `RUSTERNETES_DUMP_PAYLOADS` once via `OnceLock`.
- `install_panic_hook(component)` chains over the existing panic hook (does
  not replace it). On panic: reads `CURRENT_PAYLOAD` via `try_with`; if a
  payload is in scope, emits one `tracing::error!` with the component name,
  panic message, location, and redacted payload, then delegates to the
  previous hook so the default backtrace still prints.
- `with_payload` is the convenience wrapper for `CURRENT_PAYLOAD.scope(...)`.

Re-exported from `rusternetes_common::dump`.

### api-server wiring

A tower middleware `capture_payload` added as the outermost layer of the
router so it sees the raw body before any extractor:

1. Short-circuit if `!dumps_enabled()`.
2. `axum::body::to_bytes(body, MAX_DUMP_BODY)` where `MAX_DUMP_BODY = 4 MiB`
   (matches K8s default request size). If the body exceeds the cap, the
   request passes through with an empty captured payload and a
   `payload_truncated = true` log field on any subsequent error.
3. Re-build the inner request with the buffered bytes so handlers see the
   original body.
4. Wrap the inner service call in `CURRENT_PAYLOAD.scope(...)`.
5. After the response: if `status.is_server_error()`, emit a
   `tracing::error!` with method, URI, status, and redacted payload.

Decode failures are handled by a thin extractor wrapper, `DumpingJson<T>`,
exposed from `common::dump`. It delegates to `axum::Json<T>`; on
`JsonRejection`, it emits a `tracing::error!` with the redacted body, then
returns the same K8s-shaped `Status` error the existing handlers produce.
Migration is a mechanical `Json<X>` → `DumpingJson<X>` rename across
`crates/api-server/src/handlers/`.

### kubelet wiring

No middleware. Two scope sites:

1. **Watch event dispatch** — wherever a `WatchEvent<T>` is decoded and
   routed to a handler, wrap the per-event handler in `with_payload`,
   passing `serde_json::to_vec(&event.object)` as the body.
2. **Pod sync loop** — each `sync_pod(pod)` call wrapped the same way, so a
   panic in volume mounting, probe logic, or container startup dumps the
   full pod spec.

Watch-decode failures: the existing decode-error log lines are enriched
with the raw bytes when `dumps_enabled()`.

### Binary wiring

Each component's `main.rs` calls `dump::install_panic_hook("<component>")`
immediately after `init_basic_tracing`. Only api-server and kubelet need
this for v1, but installing it in the other binaries is cheap and means
future `with_payload` sites in those components work without further
plumbing.

## Configuration

| Env var                       | Default | Effect                                              |
|-------------------------------|---------|-----------------------------------------------------|
| `RUSTERNETES_DUMP_PAYLOADS`   | unset   | When `1`, enables all payload-dump instrumentation. |

Set in:

- `scripts/conformance-canary-run.sh` (local + CI both shell into this).
- `.github/workflows/conformance-canary.yml` job env, defense in depth.

Not set in `compose.yml` itself; production-shaped invocations stay default-off.

## Testing

### Unit (`crates/common/src/dump.rs`)

- `redact_secret_like`:
  - Plain Pod passes through byte-equal.
  - Single Secret has `data` + `stringData` values replaced; keys + other
    fields preserved.
  - `SecretList` walks `items[]`.
  - `AdmissionReview` with embedded Secret in `request.object` is redacted.
  - Malformed JSON passes through byte-equal.
  - Secret with empty `data: {}` is a no-op.
- `with_payload` + `CURRENT_PAYLOAD`: scoped future sees the bytes, outside
  the scope `try_with` returns `Err`.
- `install_panic_hook`: install a test-only sink via `Mutex<Option<...>>`,
  spawn a task, scope a payload, panic inside `catch_unwind`, assert the
  hook fired with the expected payload and component name.

### Integration (`crates/api-server/tests/payload_dump.rs`)

- Router with a handler that returns `500` + JSON body — assert a
  `tracing` event was captured containing the body.
- Same handler with a Secret request body — assert the body in the log is
  redacted.
- POST malformed JSON → assert the decode-failure log line contains the
  raw bytes.
- Body larger than `MAX_DUMP_BODY` → assert handler still runs, error log
  carries `payload_truncated = true` and no body.

Events captured via `tracing-subscriber`'s test layer.

### Kubelet

- Unit test around the `scope`-wrapped event handler: `CURRENT_PAYLOAD` is
  populated during the inner future and clear afterward.
- True panic-path coverage piggybacks on the api-server integration test
  (same hook code, same scope mechanism).

## Out of scope (explicit deferrals)

- Per-request rate limiting on dump emission. If a conformance run produces
  thousands of 5xx responses, logs will be loud. Acceptable for v1; revisit
  if it bites.
- Structured dump format (JSON-per-line vs pretty). v1 emits pretty JSON in
  the `payload` field; tracing-subscriber JSON formatter consumers get
  it as a string.
- Capturing query string and headers separately. The error event already
  carries method + URI; headers are out of scope (auth headers leak risk).
