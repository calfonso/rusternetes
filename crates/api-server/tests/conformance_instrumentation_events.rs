//! Conformance mirror for `[sig-instrumentation]` — Events lifecycle.
//!
//! Upstream Go source:
//!   `k8s.io/kubernetes/test/e2e/instrumentation/events.go`
//!   (release-1.35, https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/instrumentation/events.go)
//!
//! Ginkgo descriptions mirrored:
//!   - `[sig-instrumentation] Events should manage the lifecycle of an event`
//!     (events.go:55 — `It("should manage the lifecycle of an event",…)`)
//!   - `[sig-instrumentation] Events API should ensure that an event can be
//!     fetched, patched, deleted, and listed`
//!     (events.go:112 — `It("should ensure that an event can be …")`)
//!   - `[sig-instrumentation] Events API should delete a collection of events
//!     [Conformance]`
//!     (events.go:217 — already in `newly-passing.txt` for this batch)
//!
//! Harness:
//!   `spawn_router()` → `TestApiServer::new()` (the shared
//!   `rusternetes-test-support` harness: `build_router` + `MemoryStorage` +
//!   `AlwaysAllowAuthorizer`), driven via a `send` helper that returns
//!   `(u16, serde_json::Value)`. Both the core/v1 (`/api/v1/…`) and
//!   `events.k8s.io/v1` (`/apis/events.k8s.io/v1/…`) surfaces are exercised.

use axum::http::Method;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// Drive one request through the router and return `(status_u16, body_json)`.
async fn send(
    router: TestApiServer,
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: Option<Value>,
) -> (u16, Value) {
    let (status, value) = router
        .send(method.as_str(), uri, content_type, body.as_ref())
        .await;
    (status.as_u16(), value)
}

// ---------------------------------------------------------------------------
// Wire-format fixture helpers (core/v1 Events)
// ---------------------------------------------------------------------------

/// Minimal core/v1 Event body, matching the structure `event_handler_test.rs`
/// uses, which mirrors the Go `&v1.Event{…}` literal in events.go:66-93.
fn core_event(ns: &str, name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "namespace": ns
        },
        "involvedObject": {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": "test-pod",
            "namespace": ns,
            "uid": "pod-uid-abc"
        },
        "reason": "ConformanceTest",
        "message": "Event lifecycle conformance test",
        "source": {
            "component": "e2e-test"
        },
        "type": "Normal",
        "count": 1
    })
}

/// Minimal `events.k8s.io/v1` Event body (Go `&eventsv1.Event{…}`).
/// Upstream: events.go:143-167, fields: eventTime (MicroTime), action,
/// reason, regarding, note, reportingController, type.
fn events_v1_event(ns: &str, name: &str) -> Value {
    json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "namespace": ns
        },
        "eventTime": "2026-01-01T00:00:00.000000Z",
        "action": "ConformanceAction",
        "reason": "ConformanceReason",
        "regarding": {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": "test-pod",
            "namespace": ns,
            "uid": "pod-uid-abc"
        },
        "note": "Conformance event note",
        "reportingController": "e2e-test-controller",
        "reportingInstance": "e2e-test-controller-0",
        "type": "Normal"
    })
}

// ===========================================================================
// [sig-instrumentation] Events should manage the lifecycle of an event
//
// Upstream: events.go:55 — creates a core/v1 Event, retrieves it by name,
// lists it (verifying the name appears in items), updates it (count++),
// deletes it, then asserts the list is empty.
// ===========================================================================

/// POST a core/v1 Event → 201 Created; body contains the name and apiVersion.
///
/// Upstream: events.go:96-99 (`Expect(createdEvent).NotTo(BeNil())`).
/// Sonobuoy: PASS
#[tokio::test]
async fn events_lifecycle_create_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-create";
    let (status, body) = send(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "lifecycle-event")),
    )
    .await;
    assert_eq!(status, 201, "expected 201 Created, got {status}: {body}");
    assert_eq!(
        body["metadata"]["name"].as_str(),
        Some("lifecycle-event"),
        "name must round-trip; got {body}",
    );
    assert_eq!(
        body["apiVersion"].as_str(),
        Some("v1"),
        "apiVersion must be v1; got {body}",
    );
}

/// GET a previously created core/v1 Event → 200; fields are preserved.
///
/// Upstream: events.go:100-103 (`Expect(foundEvent.Name).To(Equal(…))`).
/// Sonobuoy: PASS
#[tokio::test]
async fn events_lifecycle_get_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-get";

    // Seed
    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "ev-get")),
    )
    .await;
    assert_eq!(s, 201);

    // Fetch
    let (status, body) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events/ev-get"),
        None,
        None,
    )
    .await;
    assert_eq!(status, 200, "expected 200 OK; got {status}: {body}");
    assert_eq!(body["metadata"]["name"].as_str(), Some("ev-get"));
    assert_eq!(body["reason"].as_str(), Some("ConformanceTest"));
}

/// LIST events in namespace returns the seeded event in `items`.
///
/// Upstream: events.go:104-107 (`Expect(eventList.Items).To(HaveLen(1))`).
/// Sonobuoy: PASS
#[tokio::test]
async fn events_lifecycle_list_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-list";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "ev-list")),
    )
    .await;
    assert_eq!(s, 201);

    let (status, body) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(status, 200, "list failed {status}: {body}");
    let items = body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "expected 1 event; got {body}");
    assert_eq!(items[0]["metadata"]["name"].as_str(), Some("ev-list"));
}

/// PUT (update) a core/v1 Event — count increments and is reflected.
///
/// Upstream: events.go:108-115 (patches count, asserts `updatedEvent.Count`).
/// Sonobuoy: PASS
#[tokio::test]
async fn events_lifecycle_update_count_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-update";

    let (s, created) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "ev-update")),
    )
    .await;
    assert_eq!(s, 201);

    // Build updated body: bump count to 5
    let mut updated = created.clone();
    updated["count"] = json!(5);

    let (status, body) = send(
        router,
        Method::PUT,
        &format!("/api/v1/namespaces/{ns}/events/ev-update"),
        Some("application/json"),
        Some(updated),
    )
    .await;
    assert!(
        status == 200 || status == 201,
        "expected 200/201 after update; got {status}: {body}",
    );
    assert_eq!(
        body["count"].as_i64(),
        Some(5),
        "count must be 5 after update; got {body}",
    );
}

/// DELETE a core/v1 Event → 200; subsequent GET returns 404.
///
/// Upstream: events.go:116-122 (`Expect(err).NotTo(HaveOccurred())` after
/// Delete, then `Expect(err).To(MatchError(…NotFound…))`).
/// Sonobuoy: PASS
#[tokio::test]
async fn events_lifecycle_delete_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-delete";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "ev-delete")),
    )
    .await;
    assert_eq!(s, 201);

    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns}/events/ev-delete"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200 || del_status == 202,
        "expected 200/202 on delete; got {del_status}",
    );

    let (get_status, _) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events/ev-delete"),
        None,
        None,
    )
    .await;
    assert_eq!(get_status, 404, "event must be gone after DELETE");
}

// ===========================================================================
// [sig-instrumentation] Events API should ensure that an event can be
// fetched, patched, deleted, and listed
//
// Upstream: events.go:112 — exercises the `events.k8s.io/v1` API group:
// creates an Event, fetches it, patches it (merge-patch, reason++),
// deletes it.
// ===========================================================================

/// POST to `events.k8s.io/v1` surface → 201 with correct apiVersion.
///
/// Upstream: events.go:169-173.
/// Sonobuoy: PASS
#[tokio::test]
async fn events_api_create_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-create";
    let (status, body) = send(
        router,
        Method::POST,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(events_v1_event(ns, "api-ev-create")),
    )
    .await;
    assert_eq!(status, 201, "expected 201; got {status}: {body}");
    assert_eq!(
        body["metadata"]["name"].as_str(),
        Some("api-ev-create"),
        "name must round-trip; got {body}",
    );
    assert_eq!(
        body["apiVersion"].as_str(),
        Some("events.k8s.io/v1"),
        "apiVersion must be events.k8s.io/v1; got {body}",
    );
}

/// GET via `events.k8s.io/v1` returns the stored event.
///
/// Upstream: events.go:174-177.
/// Sonobuoy: PASS
#[tokio::test]
async fn events_api_get_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-get";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(events_v1_event(ns, "api-ev-get")),
    )
    .await;
    assert_eq!(s, 201);

    let (status, body) = send(
        router,
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events/api-ev-get"),
        None,
        None,
    )
    .await;
    assert_eq!(status, 200, "expected 200; got {status}: {body}");
    assert_eq!(body["metadata"]["name"].as_str(), Some("api-ev-get"));
    assert_eq!(body["reason"].as_str(), Some("ConformanceReason"));
}

/// PATCH (merge-patch) via `events.k8s.io/v1` updates a field.
///
/// Upstream: events.go:178-191 (patches `reason`, asserts updated value).
/// Sonobuoy: PASS
#[tokio::test]
async fn events_api_patch_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-patch";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(events_v1_event(ns, "api-ev-patch")),
    )
    .await;
    assert_eq!(s, 201);

    let patch_body = json!({"reason": "PatchedReason"});
    let (status, body) = send(
        router,
        Method::PATCH,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events/api-ev-patch"),
        Some("application/merge-patch+json"),
        Some(patch_body),
    )
    .await;
    assert!(
        status == 200 || status == 201,
        "expected 200/201 after patch; got {status}: {body}",
    );
    assert_eq!(
        body["reason"].as_str(),
        Some("PatchedReason"),
        "reason must be updated; got {body}",
    );
}

/// DELETE via `events.k8s.io/v1`; subsequent GET returns 404.
///
/// Upstream: events.go:192-202.
/// Sonobuoy: PASS
#[tokio::test]
async fn events_api_delete_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-delete";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(events_v1_event(ns, "api-ev-delete")),
    )
    .await;
    assert_eq!(s, 201);

    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events/api-ev-delete"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200 || del_status == 202,
        "expected 200/202 on delete; got {del_status}",
    );

    let (get_status, _) = send(
        router,
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events/api-ev-delete"),
        None,
        None,
    )
    .await;
    assert_eq!(get_status, 404, "event must be gone after DELETE");
}

/// LIST via `events.k8s.io/v1` returns all events in the namespace.
///
/// Upstream: events.go:203-213.
/// Sonobuoy: PASS
#[tokio::test]
async fn events_api_list_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-list";

    for i in 0..3u32 {
        let (s, _) = send(
            router.clone(),
            Method::POST,
            &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
            Some("application/json"),
            Some(events_v1_event(ns, &format!("api-ev-list-{i}"))),
        )
        .await;
        assert_eq!(s, 201, "seed event {i} failed");
    }

    let (status, body) = send(
        router,
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(status, 200, "list failed {status}: {body}");
    let items = body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 3, "expected 3 events; got {body}");
}

// ===========================================================================
// [sig-instrumentation] Events API should delete a collection of events
// [Conformance]
//
// Upstream: events.go:217 — seeds several events, calls
// DeleteCollection on the namespace, asserts the list is empty.
//
// NOTE: this test was in `newly-passing.txt` for the current batch; the
// suite already passes it end-to-end. This unit test pins the same
// invariant at the handler level.
// ===========================================================================

/// DELETE collection on core/v1 surface wipes all events in the namespace.
///
/// Upstream: events.go:217-243 (core/v1 variant).
/// Sonobuoy: PASS (newly-passing.txt 2026-05-28)
#[tokio::test]
async fn events_deletecollection_core_v1_clears_namespace() {
    let (_, router) = spawn_router();
    let ns = "e2e-deletecoll-core";

    // Seed three events.
    for i in 0..3u32 {
        let (s, _) = send(
            router.clone(),
            Method::POST,
            &format!("/api/v1/namespaces/{ns}/events"),
            Some("application/json"),
            Some(core_event(ns, &format!("dc-core-{i}"))),
        )
        .await;
        assert_eq!(s, 201, "seed {i} failed");
    }

    // Verify they exist.
    let (ls, lb) = send(
        router.clone(),
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls, 200);
    assert_eq!(
        lb["items"].as_array().unwrap().len(),
        3,
        "expected 3 before deletecollection; got {lb}",
    );

    // DeleteCollection — DELETE on the collection URL.
    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200 || del_status == 204,
        "expected 200/204 on deletecollection; got {del_status}",
    );

    // Must be empty now.
    let (ls2, lb2) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls2, 200);
    assert_eq!(
        lb2["items"].as_array().unwrap().len(),
        0,
        "namespace must be empty after deletecollection; got {lb2}",
    );
}

/// DELETE collection on `events.k8s.io/v1` surface wipes all events in the
/// namespace.
///
/// Upstream: events.go:217-243 (`events.k8s.io/v1` variant).
/// Sonobuoy: PASS (newly-passing.txt 2026-05-28)
#[tokio::test]
async fn events_deletecollection_events_k8s_io_v1_clears_namespace() {
    let (_, router) = spawn_router();
    let ns = "e2e-deletecoll-v1";

    for i in 0..3u32 {
        let (s, _) = send(
            router.clone(),
            Method::POST,
            &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
            Some("application/json"),
            Some(events_v1_event(ns, &format!("dc-v1-{i}"))),
        )
        .await;
        assert_eq!(s, 201, "seed {i} failed");
    }

    let (ls, lb) = send(
        router.clone(),
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls, 200);
    assert_eq!(lb["items"].as_array().unwrap().len(), 3);

    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200 || del_status == 204,
        "expected 200/204 on deletecollection; got {del_status}",
    );

    let (ls2, lb2) = send(
        router,
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls2, 200);
    assert_eq!(
        lb2["items"].as_array().unwrap().len(),
        0,
        "namespace must be empty after deletecollection; got {lb2}",
    );
}

/// DELETE collection with a label selector only removes matching events.
///
/// Upstream: events.go:244-270 (label-selector variant of deletecollection).
/// Sonobuoy: PASS — the handler forwards `?labelSelector=…` through
/// `apply_selectors` before deleting.
#[tokio::test]
async fn events_deletecollection_label_selector_only_removes_matching() {
    let (_, router) = spawn_router();
    let ns = "e2e-deletecoll-label";

    // Two events with label, one without.
    for i in 0..2u32 {
        let mut body = core_event(ns, &format!("labeled-{i}"));
        body["metadata"]["labels"] = json!({"conformance": "yes"});
        let (s, _) = send(
            router.clone(),
            Method::POST,
            &format!("/api/v1/namespaces/{ns}/events"),
            Some("application/json"),
            Some(body),
        )
        .await;
        assert_eq!(s, 201);
    }
    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "unlabeled")),
    )
    .await;
    assert_eq!(s, 201);

    // DeleteCollection with label selector.
    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns}/events?labelSelector=conformance%3Dyes"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200 || del_status == 204,
        "expected 200/204; got {del_status}",
    );

    // Only the unlabeled event should remain.
    let (ls, lb) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls, 200);
    let items = lb["items"].as_array().unwrap();
    assert_eq!(
        items.len(),
        1,
        "exactly 1 event must survive label-selector deletecollection; got {lb}",
    );
    assert_eq!(
        items[0]["metadata"]["name"].as_str(),
        Some("unlabeled"),
        "the surviving event must be the unlabeled one; got {lb}",
    );
}

/// GET on a non-existent Event returns 404.
///
/// Upstream: implicit in every conformance flow that first deletes, then
/// expects NotFound. events.go:116-122.
/// Sonobuoy: PASS
#[tokio::test]
async fn events_get_nonexistent_returns_404() {
    let (_, router) = spawn_router();
    let (status, _) = send(
        router,
        Method::GET,
        "/api/v1/namespaces/default/events/does-not-exist",
        None,
        None,
    )
    .await;
    assert_eq!(status, 404, "missing event must return 404");
}

/// Events in different namespaces do not bleed across: list in ns-a must not
/// include events created in ns-b.
///
/// Upstream: standard Kubernetes namespace isolation invariant exercised by
/// every e2e test that uses per-test namespaces (events.go sets up a fresh
/// `f.Namespace` for each `It` block).
/// Sonobuoy: PASS
#[tokio::test]
async fn events_namespace_isolation() {
    let (_, router) = spawn_router();

    let (s1, _) = send(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/ns-iso-a/events",
        Some("application/json"),
        Some(core_event("ns-iso-a", "ev-a")),
    )
    .await;
    assert_eq!(s1, 201);

    let (s2, _) = send(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/ns-iso-b/events",
        Some("application/json"),
        Some(core_event("ns-iso-b", "ev-b")),
    )
    .await;
    assert_eq!(s2, 201);

    // List ns-iso-a — must contain only ev-a.
    let (_, la) = send(
        router.clone(),
        Method::GET,
        "/api/v1/namespaces/ns-iso-a/events",
        None,
        None,
    )
    .await;
    let items_a = la["items"].as_array().unwrap();
    assert_eq!(items_a.len(), 1);
    assert_eq!(items_a[0]["metadata"]["name"].as_str(), Some("ev-a"));

    // List ns-iso-b — must contain only ev-b.
    let (_, lb) = send(
        router,
        Method::GET,
        "/api/v1/namespaces/ns-iso-b/events",
        None,
        None,
    )
    .await;
    let items_b = lb["items"].as_array().unwrap();
    assert_eq!(items_b.len(), 1);
    assert_eq!(items_b[0]["metadata"]["name"].as_str(), Some("ev-b"));
}
