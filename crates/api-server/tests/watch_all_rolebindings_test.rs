//! `?watch=true` on the all-namespaces RoleBinding collection must stream
//! events, not return a one-shot list. The cross-namespace
//! `/apis/rbac.authorization.k8s.io/v1/rolebindings` list handler previously
//! ignored the watch param (only the per-namespace and legacy `/watch/` paths
//! streamed), so informers/Lens watching cluster-wide RoleBindings saw nothing.
//!
//! Drives the in-process router over `MemoryStorage`: open the watch, create a
//! RoleBinding in a namespace, and assert an `ADDED` envelope arrives on the
//! cluster-wide stream.

use axum::http::StatusCode;
use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

async fn collect(router: TestApiServer, uri: &str, max: usize, deadline: Duration) -> Vec<Value> {
    let resp = router.respond("GET", uri, None, None).await;
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
                            events.push(v);
                            if events.len() >= max {
                                return;
                            }
                        }
                    }
                }
                _ => return,
            }
        }
    };
    let _ = timeout(deadline, run).await;
    events
}

#[tokio::test]
async fn all_namespaces_rolebinding_watch_streams_added() {
    let router = spawn_router();

    // Open the cross-namespace watch and collect in the background.
    let watch_uri = "/apis/rbac.authorization.k8s.io/v1/rolebindings?watch=true&resourceVersion=0";
    let watch_router = router.clone();
    let handle =
        tokio::spawn(
            async move { collect(watch_router, watch_uri, 1, Duration::from_secs(4)).await },
        );

    tokio::time::sleep(Duration::from_millis(250)).await;

    // Create a RoleBinding in a namespace.
    let rb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "rb-watch", "namespace": "default"},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "r"},
        "subjects": [{"kind": "ServiceAccount", "name": "default", "namespace": "default"}]
    });
    let (status, _) = router
        .post(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings",
            &rb,
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "RoleBinding create should succeed"
    );

    let events = handle.await.unwrap();
    let added = events.iter().any(|e| {
        e.get("type").and_then(|t| t.as_str()) == Some("ADDED")
            && e.pointer("/object/metadata/name").and_then(|n| n.as_str()) == Some("rb-watch")
    });
    assert!(
        added,
        "all-namespaces RoleBinding watch must deliver an ADDED event for the new \
         binding; got {events:?}"
    );
}
