//! Watch event JSON envelope shape per event type.
//!
//! Upstream mirror: `staging/src/k8s.io/apimachinery/pkg/watch/json/json_test.go`
//! pins the wire-level contract for `watch.Event` serialization. This file
//! pins the same contract through the in-process Axum router so that any
//! handler change that breaks the `{type, object}` envelope shape fails fast.
//!
//! ## Envelope contract
//!
//! Every newline-delimited frame on a `?watch=true` response body is one
//! `watch.Event`, serialized as JSON with exactly two keys:
//!
//! - `type` — one of `ADDED`, `MODIFIED`, `DELETED`, `BOOKMARK`, `ERROR`.
//! - `object` — for `ADDED` / `MODIFIED` / `DELETED`, the full resource at
//!   that moment in time. For `BOOKMARK`, a minimal object carrying
//!   `kind`, `apiVersion`, and `metadata.resourceVersion`. For `ERROR`,
//!   a `metav1.Status` with `code` / `reason` / `message`.
//!
//! ## What this file pins per event type
//!
//! - `ADDED`: open `?watch=true`, POST a configmap on a cloned router, assert
//!   the first frame is `{type:"ADDED", object:{kind:"ConfigMap", …}}`.
//! - `MODIFIED`: pre-seed a configmap via storage so the initial-replay path
//!   delivers it, then PUT a mutated copy, assert a `MODIFIED` envelope
//!   carrying the *updated* `data`.
//! - `DELETED`: pre-seed a configmap, open watch, DELETE it, assert a
//!   `DELETED` envelope carrying the resource as it existed *at deletion
//!   time* (matches upstream `watch.Event.Object` semantics).
//! - `BOOKMARK`: open with `allowWatchBookmarks=true` against an empty
//!   namespace and assert a well-formed `BOOKMARK` envelope appears within
//!   the cadence window (the handler sends an immediate bookmark when there
//!   are no initial events; see `handlers/watch.rs::322-342`).
//! - `ERROR`: upstream emits a streamed `{type:"ERROR", object:Status}`
//!   envelope when the watch backend reports an unrecoverable failure —
//!   the canonical case being a `resourceVersion` that is below the
//!   cacher's earliest available revision (compaction). Rusternetes
//!   mirrors this: a compacted RV on `?watch=true` returns HTTP 200 with
//!   a single streamed `{type:"ERROR", object:Status{code:410,
//!   reason:"Expired"}}` envelope, then EOF. See
//!   `handlers/watch.rs::build_watch_error_response`.
//!
//! Each scenario wraps frame collection in `tokio::time::timeout(5s)` so a
//! handler regression that hangs the stream surfaces as a failed test, not
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
// HTTP harness — inlined per task scope (no shared module).
// Mirrors integration_watch_rv_test.rs:64-186.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "envns";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

fn cm_stub(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": TEST_NS},
        "data": {"k": "v"}
    })
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

/// Drive a watch URL through the router and collect newline-delimited JSON
/// frames until either `max_events` are gathered or `deadline` elapses.
/// Returns the HTTP status plus the parsed envelopes — empty if the stream
/// produced nothing in time.
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

/// Assert that an envelope has exactly the shape `{type:<expected>, object:{…}}`
/// — `type` is a string equal to `expected_type`, `object` is a JSON object
/// (never a primitive, array, or null). Returns the `object` for further
/// inspection by the caller.
fn assert_envelope_shape<'a>(envelope: &'a Value, expected_type: &str) -> &'a Value {
    let type_field = envelope
        .get("type")
        .unwrap_or_else(|| panic!("envelope missing 'type': {envelope}"));
    assert_eq!(
        type_field.as_str(),
        Some(expected_type),
        "envelope type mismatch (want {expected_type}, got {envelope})"
    );
    let object = envelope
        .get("object")
        .unwrap_or_else(|| panic!("envelope missing 'object': {envelope}"));
    assert!(
        object.is_object(),
        "envelope object must be a JSON object, got {object} (full: {envelope})"
    );
    object
}

// ---------------------------------------------------------------------------
// ADDED
// ---------------------------------------------------------------------------

/// `ADDED` envelope: `{"type":"ADDED","object":<full resource>}`.
///
/// We open the watch first, then POST a configmap on a cloned router and
/// wait for the next stream frame. The 250ms delay leaves room for the
/// watch_cache subscriber to attach before the write fires — tokio::spawn
/// ordering alone is not enough.
#[tokio::test]
async fn watch_envelope_added_carries_full_resource() {
    let (_mem, router) = spawn_router();
    let writer_router = router.clone();

    let write_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        send_json(
            writer_router,
            Method::POST,
            &format!("/api/v1/namespaces/{}/configmaps", TEST_NS),
            &cm_stub("envelope-add"),
        )
        .await
    });

    let (status, events) = collect_watch_events(
        router,
        &format!("/api/v1/namespaces/{}/configmaps?watch=true", TEST_NS),
        1,
        Duration::from_secs(5),
    )
    .await;

    let (post_status, _) = write_task.await.unwrap();
    assert_eq!(post_status, StatusCode::CREATED, "POST must succeed");
    assert_eq!(status, StatusCode::OK, "watch must open with 200");

    let added = events
        .iter()
        .find(|e| {
            e.get("type").and_then(|t| t.as_str()) == Some("ADDED")
                && e.pointer("/object/metadata/name").and_then(|n| n.as_str())
                    == Some("envelope-add")
        })
        .unwrap_or_else(|| panic!("expected ADDED for envelope-add, got events: {events:?}"));

    let object = assert_envelope_shape(added, "ADDED");
    assert_eq!(
        object.get("kind").and_then(|k| k.as_str()),
        Some("ConfigMap"),
        "ADDED.object.kind must be ConfigMap"
    );
    assert_eq!(
        object.get("apiVersion").and_then(|v| v.as_str()),
        Some("v1"),
        "ADDED.object.apiVersion must be v1"
    );
    assert_eq!(
        object
            .pointer("/metadata/namespace")
            .and_then(|n| n.as_str()),
        Some(TEST_NS),
        "ADDED.object.metadata.namespace must match"
    );
    assert_eq!(
        object.pointer("/data/k").and_then(|v| v.as_str()),
        Some("v"),
        "ADDED.object must carry the resource data verbatim"
    );
}

// ---------------------------------------------------------------------------
// MODIFIED
// ---------------------------------------------------------------------------

/// `MODIFIED` envelope: `{"type":"MODIFIED","object":<resource after update>}`.
///
/// Pre-seed via storage with an explicit resourceVersion so the update has
/// a matching rv to satisfy optimistic concurrency; then PUT a mutated copy
/// and assert the watch surfaces the mutation, not the pre-update state.
#[tokio::test]
async fn watch_envelope_modified_carries_updated_resource() {
    let (mem, router) = spawn_router();

    let key = build_key("configmaps", Some(TEST_NS), "envelope-mod");
    let mut seed = cm_stub("envelope-mod");
    seed["metadata"]["resourceVersion"] = json!("1");
    seed["metadata"]["uid"] = json!("u-mod");
    mem.create(&key, &seed).await.unwrap();

    let writer_router = router.clone();
    let seed_for_writer = seed.clone();
    let write_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let mut updated = seed_for_writer;
        updated["data"]["k"] = json!("v2");
        send_json(
            writer_router,
            Method::PUT,
            &format!("/api/v1/namespaces/{}/configmaps/envelope-mod", TEST_NS),
            &updated,
        )
        .await
    });

    // Open with rv=0 so we get the initial ADDED, *then* the MODIFIED.
    let (status, events) = collect_watch_events(
        router,
        &format!(
            "/api/v1/namespaces/{}/configmaps?watch=true&resourceVersion=0",
            TEST_NS
        ),
        4,
        Duration::from_secs(5),
    )
    .await;

    let (put_status, put_body) = write_task.await.unwrap();
    assert_eq!(put_status, StatusCode::OK, "PUT must succeed: {put_body}");
    assert_eq!(status, StatusCode::OK);

    let modified = events
        .iter()
        .find(|e| {
            e.get("type").and_then(|t| t.as_str()) == Some("MODIFIED")
                && e.pointer("/object/metadata/name").and_then(|n| n.as_str())
                    == Some("envelope-mod")
        })
        .unwrap_or_else(|| panic!("expected MODIFIED for envelope-mod, got events: {events:?}"));

    let object = assert_envelope_shape(modified, "MODIFIED");
    assert_eq!(
        object.get("kind").and_then(|k| k.as_str()),
        Some("ConfigMap")
    );
    assert_eq!(
        object.pointer("/data/k").and_then(|v| v.as_str()),
        Some("v2"),
        "MODIFIED.object must carry the *updated* data, not pre-update state"
    );
}

// ---------------------------------------------------------------------------
// DELETED
// ---------------------------------------------------------------------------

/// `DELETED` envelope: `{"type":"DELETED","object":<resource at deletion>}`.
///
/// Per upstream `watch.Event` semantics the object in a DELETED envelope is
/// the resource as it existed *at deletion time* (i.e. the prev-value the
/// storage layer captured). The rusternetes handler implements this by
/// deserializing `WatchEvent::Deleted(_, prev_value)`; see
/// `handlers/watch.rs::472-509`.
#[tokio::test]
async fn watch_envelope_deleted_carries_prior_resource() {
    let (mem, router) = spawn_router();

    let key = build_key("configmaps", Some(TEST_NS), "envelope-del");
    let mut seed = cm_stub("envelope-del");
    seed["metadata"]["resourceVersion"] = json!("1");
    seed["metadata"]["uid"] = json!("u-del");
    mem.create(&key, &seed).await.unwrap();

    let writer_router = router.clone();
    let delete_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        send_delete(
            writer_router,
            &format!("/api/v1/namespaces/{}/configmaps/envelope-del", TEST_NS),
        )
        .await
    });

    let (status, events) = collect_watch_events(
        router,
        &format!(
            "/api/v1/namespaces/{}/configmaps?watch=true&resourceVersion=0",
            TEST_NS
        ),
        4,
        Duration::from_secs(5),
    )
    .await;

    let (del_status, _) = delete_task.await.unwrap();
    assert_eq!(del_status, StatusCode::OK, "DELETE must succeed");
    assert_eq!(status, StatusCode::OK);

    let deleted = events
        .iter()
        .find(|e| {
            e.get("type").and_then(|t| t.as_str()) == Some("DELETED")
                && e.pointer("/object/metadata/name").and_then(|n| n.as_str())
                    == Some("envelope-del")
        })
        .unwrap_or_else(|| panic!("expected DELETED for envelope-del, got events: {events:?}"));

    let object = assert_envelope_shape(deleted, "DELETED");
    assert_eq!(
        object.get("kind").and_then(|k| k.as_str()),
        Some("ConfigMap")
    );
    assert_eq!(
        object
            .pointer("/metadata/namespace")
            .and_then(|n| n.as_str()),
        Some(TEST_NS)
    );
    // The object payload reflects the resource as it existed at delete time.
    assert_eq!(
        object.pointer("/data/k").and_then(|v| v.as_str()),
        Some("v"),
        "DELETED.object must echo the resource state at deletion (got: {object})"
    );
}

// ---------------------------------------------------------------------------
// BOOKMARK
// ---------------------------------------------------------------------------

/// `BOOKMARK` envelope:
/// `{"type":"BOOKMARK","object":{"kind":<Kind>,"apiVersion":<…>,"metadata":{"resourceVersion":<str>, …}}}`.
///
/// When `allowWatchBookmarks=true` is set and there are no initial events to
/// replay, the handler emits an immediate bookmark so the HTTP/2 client sees
/// data right away (see `handlers/watch.rs::322-342`). We collect within a
/// short window and assert the wire shape on the first bookmark we see.
#[tokio::test]
async fn watch_envelope_bookmark_carries_resource_version() {
    let (_mem, router) = spawn_router();

    let (status, events) = collect_watch_events(
        router,
        &format!(
            "/api/v1/namespaces/{}/configmaps?watch=true&resourceVersion=0&allowWatchBookmarks=true",
            TEST_NS
        ),
        1,
        Duration::from_secs(3),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let bookmark = match events
        .iter()
        .find(|e| e.get("type").and_then(|t| t.as_str()) == Some("BOOKMARK"))
    {
        Some(b) => b,
        None => panic!(
            "expected a BOOKMARK envelope within the timeout (allowWatchBookmarks=true), got: {events:?}"
        ),
    };

    let object = assert_envelope_shape(bookmark, "BOOKMARK");
    assert_eq!(
        object.get("kind").and_then(|k| k.as_str()),
        Some("ConfigMap"),
        "BOOKMARK.object.kind must equal the resource Kind, got {object}"
    );
    assert_eq!(
        object.get("apiVersion").and_then(|v| v.as_str()),
        Some("v1"),
        "BOOKMARK.object.apiVersion must be set, got {object}"
    );
    let rv = object
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str());
    assert!(
        rv.is_some(),
        "BOOKMARK.object.metadata.resourceVersion must be present, got {object}"
    );
    assert!(
        !rv.unwrap().is_empty(),
        "BOOKMARK.object.metadata.resourceVersion must be non-empty"
    );
}

// ---------------------------------------------------------------------------
// ERROR
// ---------------------------------------------------------------------------

/// `ERROR` envelope: `{"type":"ERROR","object":<metav1.Status>}`.
///
/// Upstream emits a streamed `ERROR` event whenever the watch backend reports
/// an unrecoverable failure (e.g. the requested `resourceVersion` is below
/// the cacher's earliest available revision — compaction observed *after*
/// the stream opened, or detected at `Watch()`-time but reported via the
/// event channel rather than the HTTP status). The handler at
/// `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go::serveWatch`
/// writes the 200 status + headers before the cacher reports anything, so
/// the fatal condition is delivered as a `watch.Event{Type: Error,
/// Object: NewResourceExpired(...).Status()}` frame followed by stream
/// close.
///
/// Rusternetes mirrors this contract: a stale (compacted) `resourceVersion`
/// on a `?watch=true` request returns HTTP 200 with a single streamed
/// `{type:"ERROR", object:Status{code:410, reason:"Expired", …}}` envelope,
/// then EOF. See `handlers/watch.rs::resource_expired_status` /
/// `build_watch_error_response`.
#[tokio::test]
async fn watch_envelope_error_carries_status() {
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
        "watch with compacted RV must return HTTP 200 + in-stream ERROR \
         envelope (not a pre-stream HTTP 410), upstream parity"
    );

    let error = events
        .iter()
        .find(|e| e.get("type").and_then(|t| t.as_str()) == Some("ERROR"))
        .unwrap_or_else(|| panic!("expected ERROR envelope, got: {events:?}"));

    let object = assert_envelope_shape(error, "ERROR");
    assert_eq!(
        object.get("kind").and_then(|k| k.as_str()),
        Some("Status"),
        "ERROR.object must be a metav1.Status"
    );
    assert_eq!(
        object.get("reason").and_then(|r| r.as_str()),
        Some("Expired"),
        "ERROR.object.reason must be Expired for a stale-RV failure"
    );
    assert_eq!(
        object.get("code").and_then(|c| c.as_u64()),
        Some(410),
        "ERROR.object.code must be 410"
    );
    assert!(
        object
            .get("message")
            .and_then(|m| m.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "ERROR.object.message must be non-empty"
    );
}
