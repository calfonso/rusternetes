//! Watch should skip stored objects that can't be deserialized into the typed `T`.
//!
//! Regression guard for the bug where `storage.list::<T>` propagated a
//! deserialization error and caused the entire watch request to return HTTP 400.
//!
//! Harness: in-process Axum router over `MemoryStorage`. We bypass the
//! normal POST path and inject a partial Deployment (no `spec`) directly into
//! storage using `storage.create::<serde_json::Value>` so the raw JSON is
//! stored verbatim, but `list::<Deployment>` would fail on it. A second, valid
//! Deployment is also seeded. We then open
//! `GET /apis/apps/v1/namespaces/<ns>/deployments?watch=true&resourceVersion=0`
//! and assert:
//!   (a) the response is HTTP 200 (not 400),
//!   (b) the valid Deployment's ADDED event arrives in the stream.
//!
//! Upstream behavior: Kubernetes never fails a watch because of one bad stored
//! object. It simply skips it and continues streaming valid objects.

use axum::http::StatusCode;
use futures::StreamExt;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const NS: &str = "watch-skip-test";

/// Open a watch stream, collect up to `max` newline-delimited JSON events
/// within `deadline`, and return them. Uses the harness `respond` escape hatch
/// to get the un-buffered streaming `Response`.
async fn collect_watch_events(
    router: &TestApiServer,
    uri: String,
    max: usize,
    deadline: Duration,
) -> (StatusCode, Vec<Value>) {
    let resp = router.respond("GET", &uri, None, None).await;
    let status = resp.status();

    // If not 200, return immediately — no events to collect.
    if status != StatusCode::OK {
        return (status, Vec::new());
    }

    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut events = Vec::new();

    let run = async {
        while events.len() < max {
            match stream.next().await {
                Some(Ok(bytes)) => {
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    while let Some(i) = buf.find('\n') {
                        let line = buf[..i].to_string();
                        buf.drain(..=i);
                        if line.trim().is_empty() {
                            continue;
                        }
                        if let Ok(v) = serde_json::from_str::<Value>(&line) {
                            // Only count non-bookmark events toward `max`.
                            let event_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                            if event_type != "BOOKMARK" {
                                events.push(v);
                                if events.len() >= max {
                                    return;
                                }
                            }
                        }
                    }
                }
                _ => return,
            }
        }
    };
    let _ = timeout(deadline, run).await;
    (status, events)
}

/// Seed a valid Deployment and a partial/malformed Deployment (no `spec`) into
/// raw storage, bypassing the router.
///
/// The partial Deployment has only `apiVersion`, `kind`, and `metadata` — no
/// `spec` field. When `list::<Deployment>` tries to deserialize it, serde will
/// return an error ("missing field `spec`"). The watch handler must skip it
/// rather than propagating the error.
async fn seed_deployments(mem: &MemoryStorage) {
    // Seed the namespace so that namespaced resources don't get rejected.
    let _ = mem
        .create(
            &build_key("namespaces", None, NS),
            &json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": NS}
            }),
        )
        .await;

    // Valid Deployment — fully populated with spec/selector/template.
    let _ = mem
        .create(
            &build_key("deployments", Some(NS), "valid-deploy"),
            &json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "valid-deploy", "namespace": NS},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "valid"}},
                    "template": {
                        "metadata": {"labels": {"app": "valid"}},
                        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                    }
                }
            }),
        )
        .await;

    // Partial/malformed Deployment — only apiVersion, kind, metadata, NO spec.
    // This is stored as serde_json::Value so the MemoryStorage accepts it.
    // Deserializing this into Deployment will fail ("missing field `spec`").
    let _ = mem
        .create(
            &build_key("deployments", Some(NS), "partial-deploy"),
            &json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "partial-deploy", "namespace": NS}
                // Intentionally omitted: "spec"
            }),
        )
        .await;
}

/// A watch over a collection must:
///   (a) return HTTP 200, not 400, even when one stored object can't be
///       deserialized into the typed `T`;
///   (b) still deliver ADDED events for every valid object.
#[tokio::test]
async fn watch_skips_undeserializable_object_and_delivers_valid_objects() {
    let api = TestApiServer::new();
    seed_deployments(&api.storage).await;

    let uri = format!("/apis/apps/v1/namespaces/{NS}/deployments?watch=true&resourceVersion=0");

    // Collect 1 non-bookmark event (the ADDED for "valid-deploy") with a 3-second
    // deadline. The partial Deployment must be skipped; if it caused a 400 the
    // test fails on the status check before even reading events.
    let (status, events) = collect_watch_events(&api, uri, 1, Duration::from_secs(3)).await;

    // (a) Must be 200, not 400.
    assert_eq!(
        status,
        StatusCode::OK,
        "watch returned {status} instead of 200; a bad stored object must not abort the stream"
    );

    // (b) The valid Deployment's ADDED event must have arrived.
    let added_names: Vec<&str> = events
        .iter()
        .filter(|e| e.get("type").and_then(|t| t.as_str()) == Some("ADDED"))
        .filter_map(|e| e.pointer("/object/metadata/name").and_then(|n| n.as_str()))
        .collect();

    assert!(
        added_names.contains(&"valid-deploy"),
        "expected ADDED event for 'valid-deploy' but got events: {events:?}"
    );
}
