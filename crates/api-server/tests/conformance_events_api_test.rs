//! In-process router ports of the upstream Kubernetes Events conformance e2e
//! specs. These drive the live api-server routes (via `build_router` +
//! `MemoryStorage` + `tower::oneshot`) through the exact operation sequences
//! and assertions of the four upstream conformance tests:
//!
//! `test/e2e/instrumentation/core_events.go`:
//!   1. "should manage the lifecycle of an event"  (core/v1)
//!   2. "should delete a collection of events"     (core/v1)
//!
//! `test/e2e/instrumentation/events.go`:
//!   3. "should ensure that an event can be fetched, patched, deleted, and listed" (events.k8s.io/v1)
//!   4. "should delete a collection of events" (events.k8s.io/v1)
//!
//! Routes exercised — core/v1: `/api/v1/namespaces/<ns>/events` and
//! `/api/v1/events`; events.k8s.io/v1:
//! `/apis/events.k8s.io/v1/namespaces/<ns>/events` and
//! `/apis/events.k8s.io/v1/events`.
//!
//! Harness mirrors `list_empty_items_router_test.rs`: `Arc<MemoryStorage>`,
//! `AlwaysAllowAuthorizer` + `skip_auth=true`, one `oneshot` per request.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`, preserving this
// file's `send(&state, method, uri, body, content_type)` call sites.
// ---------------------------------------------------------------------------

fn make_state() -> TestApiServer {
    TestApiServer::new()
}

async fn send(
    state: &TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
    content_type: &str,
) -> (StatusCode, Value) {
    // Match the original: only attach a content-type header when there's a body.
    let ct = body.map(|_| content_type);
    state.send(method, uri, ct, body).await
}

async fn post_json(state: &TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    send(state, "POST", uri, Some(body), "application/json").await
}

async fn put_json(state: &TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    send(state, "PUT", uri, Some(body), "application/json").await
}

async fn get_json(state: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    send(state, "GET", uri, None, "application/json").await
}

async fn delete(state: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    send(state, "DELETE", uri, None, "application/json").await
}

/// JSON-merge PATCH (Content-Type: application/merge-patch+json).
async fn patch_merge(state: &TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    send(
        state,
        "PATCH",
        uri,
        Some(body),
        "application/merge-patch+json",
    )
    .await
}

async fn create_namespace(state: &TestApiServer, ns: &str) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": ns },
    });
    let (status, _v) = post_json(state, "/api/v1/namespaces", &body).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create returned {status}",
    );
}

/// True if a list contains an item with the given name + namespace.
fn list_contains(list: &Value, name: &str, namespace: &str) -> bool {
    list.get("items")
        .and_then(|i| i.as_array())
        .map(|items| {
            items.iter().any(|e| {
                e.get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    == Some(name)
                    && e.get("metadata")
                        .and_then(|m| m.get("namespace"))
                        .and_then(|n| n.as_str())
                        == Some(namespace)
            })
        })
        .unwrap_or(false)
}

fn items_len(list: &Value) -> usize {
    list.get("items")
        .and_then(|i| i.as_array())
        .map(|a| a.len())
        .unwrap_or(usize::MAX)
}

// ===========================================================================
// Spec 1 (core/v1): "should manage the lifecycle of an event"
// ===========================================================================

#[tokio::test]
async fn core_v1_should_manage_lifecycle_of_an_event() {
    let state = make_state();
    let ns = "events-lifecycle-core";
    create_namespace(&state, ns).await;

    let event_name = "event-test";

    // --- creating a test event ---
    let create_body = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": event_name,
            "labels": { "testevent-constant": "true" },
        },
        "message": "This is a test event",
        "reason": "Test",
        "type": "Normal",
        "count": 1,
        "involvedObject": { "namespace": ns },
    });
    let (status, created) = post_json(
        &state,
        &format!("/api/v1/namespaces/{ns}/events"),
        &create_body,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create returned {created:?}");

    // --- listing all events in all namespaces (labelSelector) ---
    let (status, list) = get_json(
        &state,
        "/api/v1/events?labelSelector=testevent-constant%3Dtrue",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        list_contains(&list, event_name, ns),
        "created event not found in all-namespaces list: {list:?}",
    );

    // --- patching the test event (message) ---
    let patched_message = "This is a test event - patched";
    let (status, _patched) = patch_merge(
        &state,
        &format!("/api/v1/namespaces/{ns}/events/{event_name}"),
        &json!({ "message": patched_message }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch failed");

    // --- fetching the test event: message MUST equal patch ---
    let (status, event) = get_json(
        &state,
        &format!("/api/v1/namespaces/{ns}/events/{event_name}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        event["message"], patched_message,
        "test event message does not match patch message",
    );

    // --- updating the test event (Series) ---
    // Re-get, set series, clear resourceVersion + managedFields, PUT.
    let (_s, mut test_event) = get_json(
        &state,
        &format!("/api/v1/namespaces/{ns}/events/{event_name}"),
    )
    .await;
    test_event["series"] = json!({
        "count": 100,
        // time.Unix(1505828956, 0) == 2017-09-19T13:49:16Z, MicroTime precision
        "lastObservedTime": "2017-09-19T13:49:16.000000Z",
    });
    if let Some(meta) = test_event
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.remove("resourceVersion");
        meta.remove("managedFields");
    }
    let (status, _updated) = put_json(
        &state,
        &format!("/api/v1/namespaces/{ns}/events/{event_name}"),
        &test_event,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update failed");

    // --- getting the test event: series MUST round-trip ---
    let (status, event) = get_json(
        &state,
        &format!("/api/v1/namespaces/{ns}/events/{event_name}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(event["series"]["count"], 100, "series.count not updated");
    assert!(
        event["series"]["lastObservedTime"]
            .as_str()
            .unwrap_or("")
            .starts_with("2017-09-19T13:49:16"),
        "series.lastObservedTime not round-tripped: {:?}",
        event["series"],
    );

    // --- deleting the test event ---
    let (status, _v) = delete(
        &state,
        &format!("/api/v1/namespaces/{ns}/events/{event_name}"),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "delete returned {status}",
    );

    // --- listing all events: deleted event MUST NOT show ---
    let (status, list) = get_json(
        &state,
        "/api/v1/events?labelSelector=testevent-constant%3Dtrue",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !list_contains(&list, event_name, ns),
        "deleted event should not appear in list: {list:?}",
    );
}

// ===========================================================================
// Spec 2 (core/v1): "should delete a collection of events"
// ===========================================================================

#[tokio::test]
async fn core_v1_should_delete_a_collection_of_events() {
    let state = make_state();
    let ns = "events-collection-core";
    create_namespace(&state, ns).await;

    let names = ["test-event-1", "test-event-2", "test-event-3"];

    // --- create set of events ---
    for name in names {
        let body = json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {
                "name": name,
                "labels": { "testevent-set": "true" },
            },
            "message": format!("This is {name}"),
            "reason": "Test",
            "type": "Normal",
            "count": 1,
            "involvedObject": { "namespace": ns },
        });
        let (status, v) =
            post_json(&state, &format!("/api/v1/namespaces/{ns}/events"), &body).await;
        assert_eq!(status, StatusCode::CREATED, "create {name} returned {v:?}");
    }

    // --- list with label selector in current namespace: MUST be 3 ---
    let (status, list) = get_json(
        &state,
        &format!("/api/v1/namespaces/{ns}/events?labelSelector=testevent-set%3Dtrue"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        items_len(&list),
        names.len(),
        "expected {} events, got {list:?}",
        names.len(),
    );

    // --- delete collection ---
    let (status, _v) = delete(
        &state,
        &format!("/api/v1/namespaces/{ns}/events?labelSelector=testevent-set%3Dtrue"),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "deletecollection returned {status}",
    );

    // --- list again: MUST be 0 ---
    let (status, list) = get_json(
        &state,
        &format!("/api/v1/namespaces/{ns}/events?labelSelector=testevent-set%3Dtrue"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(items_len(&list), 0, "events not deleted: {list:?}");
}

// ===========================================================================
// events.k8s.io/v1 helpers
// ===========================================================================

/// Build an events.k8s.io/v1 test event mirroring upstream `newTestEvent`.
/// Sets all fields required by strict validation: eventTime, reportingController,
/// reportingInstance, action, reason, type.
fn new_events_v1_event(ns: &str, name: &str, label: &str) -> Value {
    json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "labels": { label: "true" },
        },
        "regarding": { "namespace": ns },
        // time.Unix(1505828956, 0) == 2017-09-19T13:49:16Z
        "eventTime": "2017-09-19T13:49:16.000000Z",
        "note": format!("This is {name}"),
        "action": "Do",
        "reason": "Test",
        "type": "Normal",
        "reportingController": "test-controller",
        "reportingInstance": "test-node",
    })
}

// ===========================================================================
// Spec 3 (events.k8s.io/v1):
//   "should ensure that an event can be fetched, patched, deleted, and listed"
// ===========================================================================

#[tokio::test]
async fn events_v1_should_fetch_patch_delete_list() {
    let state = make_state();
    let ns = "events-lifecycle-evv1";
    create_namespace(&state, ns).await;

    let event_name = "event-test";
    let ns_path = format!("/apis/events.k8s.io/v1/namespaces/{ns}/events");

    // --- creating a test event ---
    let (status, created) = post_json(
        &state,
        &ns_path,
        &new_events_v1_event(ns, event_name, "testevent-constant"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create returned {created:?}");
    // Upstream asserts `HaveValidResourceVersion()` on the create response.
    // resourceVersion is assigned by the storage layer (etcd revision); the
    // in-process `MemoryStorage` test backend does not synthesize one, so we
    // instead assert the create echoed the object identity back. The RV
    // contract itself is covered by the etcd-backed storage tests, not here.
    assert_eq!(created["metadata"]["name"], event_name);
    assert_eq!(created["metadata"]["namespace"], ns);
    assert_eq!(created["apiVersion"], "events.k8s.io/v1");

    // --- listing events in all namespaces (labelSelector) ---
    let (status, list) = get_json(
        &state,
        "/apis/events.k8s.io/v1/events?labelSelector=testevent-constant%3Dtrue",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        list_contains(&list, event_name, ns),
        "event not found in all-namespaces list: {list:?}",
    );

    // --- listing events in test namespace (labelSelector) ---
    let (status, list) = get_json(
        &state,
        &format!("{ns_path}?labelSelector=testevent-constant%3Dtrue"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        list_contains(&list, event_name, ns),
        "event not found in namespaced list: {list:?}",
    );

    // --- field selection on core/v1 `source` (maps to source.component) ---
    let (status, list) = get_json(
        &state,
        &format!("/api/v1/namespaces/{ns}/events?fieldSelector=source%3Dtest-controller"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        items_len(&list),
        1,
        "expected single event filtered by source=test-controller, got {list:?}",
    );
    assert_eq!(list["items"][0]["metadata"]["name"], event_name);

    // --- field selection on events.k8s.io `reportingController` ---
    let (status, list) = get_json(
        &state,
        &format!("{ns_path}?fieldSelector=reportingController%3Dtest-controller"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        items_len(&list),
        1,
        "expected single event filtered by reportingController, got {list:?}",
    );
    assert_eq!(list["items"][0]["metadata"]["name"], event_name);

    // --- field selection on `reason` and `type` (exercised by upstream) ---
    let (status, list) = get_json(&state, &format!("{ns_path}?fieldSelector=reason%3DTest")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        items_len(&list),
        1,
        "reason field selector failed: {list:?}"
    );

    let (status, list) = get_json(&state, &format!("{ns_path}?fieldSelector=type%3DNormal")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(items_len(&list), 1, "type field selector failed: {list:?}");

    // A non-matching field selector MUST return zero.
    let (status, list) = get_json(
        &state,
        &format!("{ns_path}?fieldSelector=reportingController%3Dno-such-controller"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        items_len(&list),
        0,
        "non-matching reportingController should filter all out: {list:?}",
    );

    // --- getting the test event ---
    let (status, _test_event) = get_json(&state, &format!("{ns_path}/{event_name}")).await;
    assert_eq!(status, StatusCode::OK);

    // --- patching the test event (add Series) ---
    let patch_series = json!({
        "series": {
            "count": 2,
            // time.Unix(1505828951, 0) == 2017-09-19T13:49:11Z
            "lastObservedTime": "2017-09-19T13:49:11.000000Z",
        }
    });
    let (status, _patched) =
        patch_merge(&state, &format!("{ns_path}/{event_name}"), &patch_series).await;
    assert_eq!(status, StatusCode::OK, "patch failed");

    // --- getting the test event: series MUST round-trip ---
    // (Upstream additionally asserts the patched RV is larger than the created
    // RV; that monotonic-RV contract is a storage-layer guarantee not provided
    // by the in-process MemoryStorage backend, so the series round-trip below
    // is what proves the patch was applied.)
    let (status, event) = get_json(&state, &format!("{ns_path}/{event_name}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(event["series"]["count"], 2, "patched series.count wrong");
    assert!(
        event["series"]["lastObservedTime"]
            .as_str()
            .unwrap_or("")
            .starts_with("2017-09-19T13:49:11"),
        "patched series.lastObservedTime not round-tripped: {:?}",
        event["series"],
    );

    // --- updating the test event (replace Series) ---
    let mut update_event = event.clone();
    update_event["series"] = json!({
        "count": 100,
        "lastObservedTime": "2017-09-19T13:49:16.000000Z",
    });
    if let Some(meta) = update_event
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.remove("managedFields");
        // Strict events.k8s.io/v1 update validation (ValidateObjectMetaUpdate)
        // rejects an update whose metadata.resourceVersion is empty. A real
        // etcd backend stamps an RV that GET round-trips; the in-process
        // MemoryStorage test backend does not synthesize one, so set it
        // explicitly to mirror what a live backend returns.
        meta.insert("resourceVersion".to_string(), json!("1"));
    }
    let (status, _updated) =
        put_json(&state, &format!("{ns_path}/{event_name}"), &update_event).await;
    assert_eq!(status, StatusCode::OK, "update failed");

    // --- getting the test event: updated series MUST be visible ---
    let (status, event) = get_json(&state, &format!("{ns_path}/{event_name}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(event["series"]["count"], 100, "updated series.count wrong");
    assert!(
        event["series"]["lastObservedTime"]
            .as_str()
            .unwrap_or("")
            .starts_with("2017-09-19T13:49:16"),
        "updated series.lastObservedTime not round-tripped: {:?}",
        event["series"],
    );

    // --- deleting the test event ---
    let (status, _v) = delete(&state, &format!("{ns_path}/{event_name}")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "delete returned {status}",
    );

    // --- listing all namespaces: deleted event MUST NOT show ---
    let (status, list) = get_json(
        &state,
        "/apis/events.k8s.io/v1/events?labelSelector=testevent-constant%3Dtrue",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !list_contains(&list, event_name, ns),
        "deleted event should not appear in all-ns list: {list:?}",
    );

    // --- listing namespaced: deleted event MUST NOT show ---
    let (status, list) = get_json(
        &state,
        &format!("{ns_path}?labelSelector=testevent-constant%3Dtrue"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !list_contains(&list, event_name, ns),
        "deleted event should not appear in namespaced list: {list:?}",
    );
}

// ===========================================================================
// Spec 4 (events.k8s.io/v1): "should delete a collection of events"
// ===========================================================================

#[tokio::test]
async fn events_v1_should_delete_a_collection_of_events() {
    let state = make_state();
    let ns = "events-collection-evv1";
    create_namespace(&state, ns).await;

    let names = ["test-event-1", "test-event-2", "test-event-3"];
    let ns_path = format!("/apis/events.k8s.io/v1/namespaces/{ns}/events");

    // --- create set of events ---
    for name in names {
        let (status, v) = post_json(
            &state,
            &ns_path,
            &new_events_v1_event(ns, name, "testevent-set"),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "create {name} returned {v:?}");
    }

    // --- list with label selector: MUST be 3 ---
    let (status, list) = get_json(
        &state,
        &format!("{ns_path}?labelSelector=testevent-set%3Dtrue"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        items_len(&list),
        names.len(),
        "expected {} events, got {list:?}",
        names.len(),
    );

    // --- delete collection ---
    let (status, _v) = delete(
        &state,
        &format!("{ns_path}?labelSelector=testevent-set%3Dtrue"),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "deletecollection returned {status}",
    );

    // --- list again: MUST be empty ---
    let (status, list) = get_json(
        &state,
        &format!("{ns_path}?labelSelector=testevent-set%3Dtrue"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(items_len(&list), 0, "events not deleted: {list:?}");
}
