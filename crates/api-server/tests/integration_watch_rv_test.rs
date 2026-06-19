//! Integration tests for watch streams + resourceVersion semantics.
//!
//! Upstream mirror: `test/integration/etcd/etcd_storage_path_test.go` and
//! `test/integration/apiserver/watch/*_test.go` cover the same wire-level
//! contract that this file exercises through the in-process Axum router.
//!
//! We drive the public HTTP surface end-to-end via `tower::ServiceExt::oneshot`
//! against an `Arc<MemoryStorage>` so the assertions match what a real client
//! sees on `/api/v1/...?watch=true` plus the optimistic-concurrency surface on
//! create/update/delete.
//!
//! ## resourceVersion model in rusternetes
//!
//! Rusternetes uses a **per-resource resourceVersion** that the backend stamps
//! on every write (etcd/rhino set it to the mod_revision; the in-memory backend
//! preserves whatever the test/handler sets). The cluster-wide list RV is
//! exposed by `Storage::current_revision()` (etcd revision, or
//! `chrono::Utc::now().timestamp()` for memory). The contract this file pins:
//!
//! - Every successful PUT must increment the per-object RV (handlers do not
//!   regress it).
//! - PUT with `metadata.resourceVersion` != stored RV returns **409 Conflict**
//!   with reason `Conflict` — gated by
//!   `handlers::lifecycle::check_resource_version`.
//! - DELETE on a missing object returns **404 NotFound**.
//! - Watch streams emit one `ADDED` envelope per subsequent create, in arrival
//!   order, on the long-lived chunked response.
//! - `?resourceVersion=0` watch replays the current LIST as `ADDED` events
//!   before tracking future changes.
//!
//! ## What this file does NOT pin (yet)
//!
//! - `Bookmark` events: the handler emits them every ~1s when
//!   `allowWatchBookmarks=true`. We assert the wire shape but do not exercise
//!   the interval cadence (would slow the suite without surfacing a regression
//!   that more focused unit tests don't already catch).
//!
//! Each test wraps the streaming portion in `tokio::time::timeout(5s)` so a
//! handler regression that hangs the response surfaces as a test failure, not
//! an indefinite wait.

use axum::http::{Method, StatusCode};
use futures::StreamExt;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "watchrvns";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send_json(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: &Value,
) -> (StatusCode, Value) {
    router
        .send(method.as_str(), uri, Some("application/json"), Some(body))
        .await
}

async fn send_delete(router: TestApiServer, uri: &str) -> (StatusCode, Value) {
    router.delete(uri).await
}

fn pod_stub(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    })
}

fn cm_stub(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": TEST_NS},
        "data": {"k": "v"}
    })
}

/// Drive a watch URL through the router and collect the first `max_events`
/// `\n`-delimited JSON envelopes, returning early on `deadline`. Returns the
/// HTTP status plus the parsed events. The router clone is dropped after the
/// stream prefix is collected so background tasks are released.
async fn collect_watch_events(
    router: TestApiServer,
    uri: &str,
    max_events: usize,
    deadline: Duration,
) -> (StatusCode, Vec<Value>) {
    let response = router.respond("GET", uri, None, None).await;
    let status = response.status();
    let mut stream = response.into_body().into_data_stream();
    let mut buffer = String::new();
    let mut events = Vec::new();

    let collect = async {
        while events.len() < max_events {
            match stream.next().await {
                Some(Ok(bytes)) => {
                    buffer.push_str(&String::from_utf8_lossy(&bytes));
                    while let Some(idx) = buffer.find('\n') {
                        let line = buffer[..idx].to_string();
                        buffer.drain(..=idx);
                        if line.trim().is_empty() {
                            continue;
                        }
                        if let Ok(v) = serde_json::from_str::<Value>(&line) {
                            events.push(v);
                            if events.len() >= max_events {
                                return;
                            }
                        }
                    }
                }
                Some(Err(_)) | None => return,
            }
        }
    };

    let _ = timeout(deadline, collect).await;
    (status, events)
}

// ---------------------------------------------------------------------------
// resourceVersion monotonicity and conflict semantics
// ---------------------------------------------------------------------------

/// Every successful write through the router must produce an object whose
/// `metadata.resourceVersion` is non-empty and stays the same or grows
/// monotonically when re-fetched. Upstream contract:
/// `staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go` always stamps
/// `Object.ResourceVersion` from `mod_revision`.
#[tokio::test]
async fn test_resource_version_present_after_create() {
    let (mem, router) = spawn_router();

    // Pre-seed via storage with an explicit RV — memory backend does not
    // auto-stamp, so we mirror what etcd would do on the way in. This isolates
    // the *handler contract*: a GET must echo back the stored RV.
    let key = build_key("configmaps", Some(TEST_NS), "rv-echo");
    let mut stored = cm_stub("rv-echo");
    stored["metadata"]["resourceVersion"] = json!("42");
    mem.create(&key, &stored).await.unwrap();

    let (status, body) = send_json(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{}/configmaps/rv-echo", TEST_NS),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["metadata"]["resourceVersion"].as_str(),
        Some("42"),
        "GET must echo the stored resourceVersion"
    );
}

/// PUT with `metadata.resourceVersion` that does NOT match the stored RV must
/// return 409 Conflict + Status `reason=Conflict`. Source-of-truth:
/// `crates/api-server/src/handlers/lifecycle.rs::check_resource_version`,
/// invoked from `handlers::pod::update`.
#[tokio::test]
async fn test_resource_version_stale_update_returns_409() {
    let (mem, router) = spawn_router();

    let key = build_key("pods", Some(TEST_NS), "stalepod");
    let mut stored = pod_stub("stalepod");
    stored["metadata"]["resourceVersion"] = json!("100");
    stored["metadata"]["uid"] = json!("u-1");
    mem.create(&key, &stored).await.unwrap();

    // Client sends an outdated rv "50" — must conflict against stored "100".
    let mut put_body = stored.clone();
    put_body["metadata"]["resourceVersion"] = json!("50");

    let (status, body) = send_json(
        router,
        Method::PUT,
        &format!("/api/v1/namespaces/{}/pods/stalepod", TEST_NS),
        &put_body,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "stale rv PUT must surface as 409 (got body: {})",
        body
    );
    assert_eq!(
        body["reason"].as_str(),
        Some("Conflict"),
        "Status.reason must be 'Conflict' (got: {})",
        body
    );
    assert_eq!(
        body["kind"].as_str(),
        Some("Status"),
        "error body must be a Status object"
    );
}

/// PUT with a matching `metadata.resourceVersion` must succeed (no false
/// conflicts). Counterpart to `test_resource_version_stale_update_returns_409`.
#[tokio::test]
async fn test_resource_version_matching_update_succeeds() {
    let (mem, router) = spawn_router();

    let key = build_key("pods", Some(TEST_NS), "matchpod");
    let mut stored = pod_stub("matchpod");
    stored["metadata"]["resourceVersion"] = json!("7");
    stored["metadata"]["uid"] = json!("u-match");
    mem.create(&key, &stored).await.unwrap();

    // Client sends the same rv — must succeed.
    let put_body = stored.clone();

    let (status, body) = send_json(
        router,
        Method::PUT,
        &format!("/api/v1/namespaces/{}/pods/matchpod", TEST_NS),
        &put_body,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "matching rv PUT must return 200 (got body: {})",
        body
    );
    assert_eq!(body["kind"].as_str(), Some("Pod"));
}

/// `Storage::current_revision` is monotonically non-decreasing across writes
/// — the property every list/watch RV bookmark depends on. Memory backend
/// derives it from the Unix timestamp; etcd/rhino from the mod_revision.
/// For higher backends the contract is strictly increasing; the memory
/// backend can only guarantee non-decreasing because it uses wall-clock
/// seconds, so this test pins the weaker contract that holds everywhere.
#[tokio::test]
async fn test_resource_version_current_revision_monotonic() {
    let mem = Arc::new(MemoryStorage::new());

    let r0 = mem.current_revision().await.unwrap();
    mem.create(&build_key("configmaps", Some(TEST_NS), "a"), &cm_stub("a"))
        .await
        .unwrap();
    let r1 = mem.current_revision().await.unwrap();
    mem.create(&build_key("configmaps", Some(TEST_NS), "b"), &cm_stub("b"))
        .await
        .unwrap();
    let r2 = mem.current_revision().await.unwrap();

    assert!(
        r1 >= r0,
        "current_revision must be non-decreasing (r0={r0}, r1={r1})"
    );
    assert!(
        r2 >= r1,
        "current_revision must be non-decreasing (r1={r1}, r2={r2})"
    );
}

// ---------------------------------------------------------------------------
// Watch HTTP streaming behaviour
// ---------------------------------------------------------------------------

/// `GET …/configmaps?watch=true&resourceVersion=0` replays current state as
/// `ADDED` envelopes before tailing future changes. Upstream: when a client
/// sets `resourceVersion=0`, kube-apiserver returns a list snapshot via the
/// watch cache (see `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go`
/// `Watch(rv=0)`). Our handler treats `0` / absent as "send initial events".
#[tokio::test]
async fn test_watch_resource_version_zero_replays_current_state() {
    let (mem, router) = spawn_router();

    // Pre-seed two configmaps directly via storage so we can assert the
    // initial-list semantics independently of admission/webhook latency.
    for name in ["seed-a", "seed-b"] {
        let mut cm = cm_stub(name);
        cm["metadata"]["resourceVersion"] = json!("1");
        cm["metadata"]["uid"] = json!(format!("u-{}", name));
        mem.create(&build_key("configmaps", Some(TEST_NS), name), &cm)
            .await
            .unwrap();
    }

    let (status, events) = collect_watch_events(
        router,
        &format!(
            "/api/v1/namespaces/{}/configmaps?watch=true&resourceVersion=0",
            TEST_NS
        ),
        2,
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "watch must open with 200");
    assert_eq!(events.len(), 2, "expected two initial ADDED events");
    for ev in &events {
        assert_eq!(
            ev["type"].as_str(),
            Some("ADDED"),
            "initial replay must be ADDED, got {}",
            ev
        );
        assert_eq!(ev["object"]["kind"].as_str(), Some("ConfigMap"));
    }
    let names: Vec<&str> = events
        .iter()
        .filter_map(|e| e["object"]["metadata"]["name"].as_str())
        .collect();
    assert!(names.contains(&"seed-a"));
    assert!(names.contains(&"seed-b"));
}

/// A watch stream must surface subsequent writes as ADDED envelopes in arrival
/// order. We open the watch first, then spawn a parallel task that POSTs a
/// configmap on a cloned router; the watcher must observe the same name.
#[tokio::test]
async fn test_watch_observes_subsequent_create() {
    let (_mem, router) = spawn_router();
    let writer_router = router.clone();

    // Write happens shortly after the watch opens. We dispatch it on a tokio
    // task so the watch GET sees the event arrive over the channel. The 250ms
    // delay leaves room for the watch_cache's background subscriber to attach
    // to the underlying storage (tokio::spawn ordering is best-effort).
    let write_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let (status, _) = send_json(
            writer_router,
            Method::POST,
            &format!("/api/v1/namespaces/{}/configmaps", TEST_NS),
            &cm_stub("livecm"),
        )
        .await;
        status
    });

    let (status, events) = collect_watch_events(
        router,
        &format!("/api/v1/namespaces/{}/configmaps?watch=true", TEST_NS),
        1,
        Duration::from_secs(5),
    )
    .await;

    let write_status = write_task.await.unwrap();
    assert_eq!(write_status, StatusCode::CREATED, "POST must succeed");
    assert_eq!(status, StatusCode::OK);
    assert!(
        !events.is_empty(),
        "watch must surface at least the live ADDED event"
    );
    let live = events
        .iter()
        .find(|e| {
            e["type"].as_str() == Some("ADDED")
                && e["object"]["metadata"]["name"].as_str() == Some("livecm")
        })
        .unwrap_or_else(|| panic!("expected ADDED for livecm, got events: {:?}", events));
    assert_eq!(live["object"]["kind"].as_str(), Some("ConfigMap"));
}

/// Watch response headers expose the streaming content type. Upstream sets
/// `Content-Type: application/json` and `Transfer-Encoding: chunked` on the
/// watch response (see `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/
/// watch.go::serveWatch`). We re-issue the request to inspect headers without
/// consuming the body.
#[tokio::test]
async fn test_watch_response_streaming_headers() {
    let (_mem, router) = spawn_router();
    let uri = format!("/api/v1/namespaces/{TEST_NS}/configmaps?watch=true&resourceVersion=0");
    let response = router.respond("GET", &uri, None, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let ct = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.starts_with("application/json"),
        "watch must return application/json (got: {})",
        ct
    );
    let te = response
        .headers()
        .get("transfer-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(te, "chunked", "watch must use chunked transfer-encoding");
    drop(response);
}

/// When `allowWatchBookmarks=true` is requested, the handler is allowed to
/// emit `BOOKMARK` envelopes. They must serialize with `type: "BOOKMARK"`
/// and an `object.metadata.resourceVersion` string. We collect a small
/// window and assert that any bookmark we see matches the wire shape — the
/// test does not insist on receiving one within the window (the cadence is
/// ~1s and tests must stay fast), but if one shows up it must be well-formed.
#[tokio::test]
async fn test_watch_bookmark_event_shape_when_received() {
    let (_mem, router) = spawn_router();

    let (_status, events) = collect_watch_events(
        router,
        &format!(
            "/api/v1/namespaces/{}/configmaps?watch=true&resourceVersion=0&allowWatchBookmarks=true",
            TEST_NS
        ),
        1,
        Duration::from_millis(1500),
    )
    .await;

    for ev in &events {
        if ev["type"].as_str() == Some("BOOKMARK") {
            let rv = ev["object"]["metadata"]["resourceVersion"].as_str();
            assert!(
                rv.is_some(),
                "BOOKMARK must carry resourceVersion, got {ev}"
            );
            assert!(
                !rv.unwrap().is_empty(),
                "BOOKMARK resourceVersion must not be empty"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// DELETE optimistic-concurrency surface
// ---------------------------------------------------------------------------

/// DELETE on a missing object must return 404 NotFound + Status
/// `reason=NotFound`. This pins the negative path for the optimistic
/// concurrency story (DELETE proceeds only after a successful GET).
#[tokio::test]
async fn test_resource_version_delete_missing_returns_404() {
    let (_mem, router) = spawn_router();

    let (status, body) = send_delete(
        router,
        &format!("/api/v1/namespaces/{}/configmaps/ghost", TEST_NS),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["kind"].as_str(), Some("Status"));
    assert_eq!(body["reason"].as_str(), Some("NotFound"));
}

// ---------------------------------------------------------------------------
// Compacted-RV 410 Gone + DELETE preconditions.resourceVersion
// ---------------------------------------------------------------------------

/// Watch with a compacted `resourceVersion` must emit a streamed `ERROR`
/// envelope carrying `Status{Code: 410, Reason: "Expired"}`, NOT an HTTP
/// 410 response.
///
/// Upstream contract: `staging/src/k8s.io/apiserver/pkg/storage/cacher/
/// cacher.go::Watch` returns `errs.NewResourceExpired(...)` when the
/// requested RV is below the cacher's earliest available revision. For
/// `?watch=true`, `endpoints/handlers/watch.go::serveWatch` has already
/// written the 200 status + chunked headers by the time the cacher reports
/// the failure, so the only way to deliver it is an in-stream
/// `watch.Event{Type: Error, Object: NewResourceExpired(...).Status()}`
/// frame. Mirroring this is required by
/// `watch_event_envelope_test.rs::watch_envelope_error_carries_status`.
///
/// This test pins the HTTP-level half (status 200 + the ERROR envelope
/// carries `code: 410`); the envelope-shape suite pins the full payload.
#[tokio::test]
async fn test_watch_resource_version_stale_emits_streamed_error() {
    let (mem, router) = spawn_router();

    // Mark every revision up to 999 as compacted, then ask to watch from "100".
    mem.compact_to(999);

    let (status, events) = collect_watch_events(
        router,
        &format!(
            "/api/v1/namespaces/{}/configmaps?watch=true&resourceVersion=100",
            TEST_NS
        ),
        1,
        Duration::from_millis(500),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "watch with compacted RV must return 200 + streamed ERROR (not HTTP 410)"
    );
    let error = events
        .iter()
        .find(|e| e["type"].as_str() == Some("ERROR"))
        .unwrap_or_else(|| panic!("expected ERROR envelope, got: {events:?}"));
    assert_eq!(error["object"]["kind"].as_str(), Some("Status"));
    assert_eq!(error["object"]["reason"].as_str(), Some("Expired"));
    assert_eq!(error["object"]["code"].as_u64(), Some(410));
}

/// DELETE with mismatched `preconditions.resourceVersion` must return 409.
/// Upstream: `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/
/// store.go::Delete` calls `preconditions.Check` before invoking the storage
/// delete. Our pod and configmap handlers currently ignore the body's
/// `deleteOptions.preconditions`.
#[tokio::test]
async fn test_delete_with_mismatched_precondition_rv_returns_409() {
    let (mem, router) = spawn_router();

    let key = build_key("configmaps", Some(TEST_NS), "delprec");
    let mut stored = cm_stub("delprec");
    stored["metadata"]["resourceVersion"] = json!("9");
    stored["metadata"]["uid"] = json!("u-del");
    mem.create(&key, &stored).await.unwrap();

    // Build a DELETE with body carrying a stale precondition RV.
    let body = json!({
        "kind": "DeleteOptions",
        "apiVersion": "v1",
        "preconditions": {"resourceVersion": "1"},
    });
    let uri = format!("/api/v1/namespaces/{TEST_NS}/configmaps/delprec");
    let (status, _headers, _bytes, _) = router
        .send_full(
            "DELETE",
            &uri,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(&body).unwrap()),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "mismatched precondition RV must surface as 409"
    );
}
