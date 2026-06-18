//! [sig-api-machinery] Chunked list responses must page through the full
//! collection in a stable, total order — regression pin for GitHub #280
//! (`Servers with support for API chunking should support continue listing
//! from the last key if the original version has been compacted away, though
//! the list is inconsistent`).
//!
//! The upstream test lists PodTemplates with `?limit`, follows every
//! `continue` token to the end, and asserts that **all** items are returned
//! exactly once **in key order**. The api-server LIST handler paginates with
//! the offset-based `rusternetes_common::paginate` helper over
//! `Storage::list`. `MemoryStorage::list` iterates a `HashMap`, so its
//! iteration order is NOT key order — without a deterministic sort in the
//! handler the continue chain returns items out of order (and, once the map
//! mutates, can drop or duplicate items across pages).
//!
//! This pins the fix: `list_podtemplates` / `list_all_podtemplates` sort by
//! `(namespace, name)` before paginating, matching the `/registry/...` storage
//! key order the upstream contract expects. The 410-Gone / inconsistent-token
//! mechanism itself is covered separately by
//! `conformance_apimachinery_watch_chunking_gc::chunking_continue_after_compaction_returns_410_expired`.
//!
//! Harness mirrors `list_resource_version_router_test.rs`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// Harness: `TestApiServer` (rusternetes-test-support) — `build_router` on
// `MemoryStorage` with `--skip-auth`, driven via `tower::oneshot`.

async fn post_podtemplate(state: &TestApiServer, namespace: &str, name: &str) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "PodTemplate",
        "metadata": { "name": name, "namespace": namespace },
        "template": {
            "metadata": { "labels": { "app": "chunk" } },
            "spec": { "containers": [ { "name": "c", "image": "pause" } ] }
        }
    });
    let (status, body) = state
        .post(
            &format!("/api/v1/namespaces/{namespace}/podtemplates"),
            &body,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "POST {name} failed: {body}");
}

async fn get_list(state: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    state.get(uri).await
}

fn page_names(body: &Value) -> Vec<String> {
    body["items"]
        .as_array()
        .expect("list has items array")
        .iter()
        .map(|it| it["metadata"]["name"].as_str().unwrap().to_string())
        .collect()
}

/// Walk the entire continue chain at `limit` and assert every item is
/// returned exactly once, globally in sorted (key) order, with each page
/// bounded by `limit`.
#[tokio::test]
async fn chunked_podtemplate_list_pages_in_key_order_without_gaps() {
    let state = TestApiServer::new();

    // Insert in an order that is deliberately NOT sorted, so a handler that
    // pages over raw storage iteration order (or insertion order) would
    // return them jumbled. Names chosen so sorted != insertion order.
    let insert_order = [
        "pt-07", "pt-01", "pt-11", "pt-04", "pt-09", "pt-02", "pt-12", "pt-06", "pt-03", "pt-10",
        "pt-05", "pt-08",
    ];
    for name in insert_order {
        post_podtemplate(&state, "default", name).await;
    }
    let mut expected: Vec<String> = insert_order.iter().map(|s| s.to_string()).collect();
    expected.sort();

    let limit = 5;
    let mut collected: Vec<String> = Vec::new();
    let mut continue_token: Option<String> = None;
    let mut pages = 0;

    loop {
        pages += 1;
        assert!(pages <= 100, "continue chain did not terminate");

        let uri = match &continue_token {
            Some(tok) => {
                format!("/api/v1/namespaces/default/podtemplates?limit={limit}&continue={tok}")
            }
            None => format!("/api/v1/namespaces/default/podtemplates?limit={limit}"),
        };
        let (status, body) = get_list(&state, &uri).await;
        assert_eq!(status, StatusCode::OK, "list page {pages} failed: {body}");

        let names = page_names(&body);
        assert!(
            names.len() <= limit,
            "page {pages} exceeded limit: {names:?}"
        );
        collected.extend(names);

        match body["metadata"]["continue"].as_str() {
            Some(tok) if !tok.is_empty() => continue_token = Some(tok.to_string()),
            _ => break,
        }
    }

    // Every item exactly once.
    assert_eq!(
        collected.len(),
        expected.len(),
        "expected {} items across pages, got {} (dupes or gaps): {:?}",
        expected.len(),
        collected.len(),
        collected
    );
    // Globally key-ordered across the whole continue chain — this is the
    // assertion that fails without the deterministic sort in the handler.
    assert_eq!(
        collected, expected,
        "paged items must be returned in (namespace, name) key order"
    );
}
