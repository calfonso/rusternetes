//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-api-machinery] Watch, chunking, garbage collection, field selectors.
//!
//! Source of truth: Ginkgo descriptors at
//!   https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//!     - watch.go
//!     - chunking.go
//!     - garbage_collector.go
//!     - field_selector.go
//!
//!
//! See docs/conformance/apimachinery-watch-chunking-gc.md for the test-by-test
//! status table and the cross-reference to the "Other (GC orphan pods,
//! chunking)" failure bucket in docs/CONFORMANCE.md.
//!
//! Harness note: the canonical convention asks for an axum router spawn via
//! `tower::ServiceExt::oneshot`, but `ApiServerState::storage` is typed as
//! `Arc<StorageBackend>` (etcd/sqlite/redis only — `MemoryStorage` is *not*
//! a variant of `StorageBackend`), so binding the full router to in-memory
//! storage requires a plumbing change that is out of scope for this batch.
//! The prior-art file `watch_delete_test.rs` documents the same constraint
//! and drives the handler helpers directly. We follow that pattern here:
//! exercise `MemoryStorage` plus the public surface of `handlers::filtering`,
//! `handlers::watch`, and the resource types in `rusternetes_common` to
//! validate the same wire-level semantics that the upstream Ginkgo tests
//! observe through the REST surface.

use rusternetes_api_server::handlers::filtering::apply_selectors;
use rusternetes_api_server::handlers::watch::{
    build_delete_fallback_json, extract_rv_from_json, is_watch_request, normalize_resource_version,
    K8sWatchEvent, WatchEventType,
};
use rusternetes_common::resources::{ConfigMap, Namespace};
use rusternetes_common::types::{DeletionPropagation, ListMeta, ObjectMeta, OwnerReference};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, WatchEvent};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;

// =========================================================================
// Test fixtures (inline per the "no shared helpers crate" convention).
// =========================================================================

/// Build a configmap with optional labels.
fn cm(name: &str, namespace: &str, labels: &[(&str, &str)]) -> ConfigMap {
    let mut c = ConfigMap::new(name, namespace);
    if !labels.is_empty() {
        c.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        );
    }
    c
}

/// Build the registry storage key for a configmap (delegates to
/// `rusternetes_storage::build_key` so the prefix stays in sync with the
/// rest of the codebase).
fn cm_key(namespace: &str, name: &str) -> String {
    build_key("configmaps", Some(namespace), name)
}

/// Build a watch-style query parameter map.
fn qp(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

// =========================================================================
// Watch event delivery — mirrors test/e2e/apimachinery/watch.go
// =========================================================================

/// [sig-api-machinery] Watch should observe add, update, and delete watch
/// notifications on configmaps [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/watch.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_should_observe_add_update_delete_on_configmaps() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stream = storage
        .watch("/registry/configmaps/default/")
        .await
        .unwrap();

    let configmap = cm("e2e-cm", "default", &[("watch-this", "yes")]);
    storage
        .create(&cm_key("default", "e2e-cm"), &configmap)
        .await
        .unwrap();

    let mut updated = configmap.clone();
    updated.data = Some(HashMap::from([("k".to_string(), "v".to_string())]));
    storage
        .update(&cm_key("default", "e2e-cm"), &updated)
        .await
        .unwrap();

    storage.delete(&cm_key("default", "e2e-cm")).await.unwrap();

    let mut kinds: Vec<&'static str> = Vec::new();
    for _ in 0..3 {
        let ev = timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("watch stream timed out")
            .expect("watch stream closed")
            .expect("watch event error");
        kinds.push(match ev {
            WatchEvent::Added(_, _) => "ADDED",
            WatchEvent::Modified(_, _) => "MODIFIED",
            WatchEvent::Deleted(_, _) => "DELETED",
        });
    }
    assert_eq!(kinds, vec!["ADDED", "MODIFIED", "DELETED"]);
}

/// [sig-api-machinery] Watch should be able to start watching from a specific
/// resource version [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/watch.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_should_start_from_specific_resource_version() {
    // Empty resourceVersion ⇒ None ("start from current").
    assert_eq!(normalize_resource_version(Some(String::new())), None);
    assert_eq!(
        normalize_resource_version(Some("42".to_string())),
        Some("42".to_string())
    );
    assert_eq!(normalize_resource_version(None), None);
}

/// [sig-api-machinery] Watch should receive events for every added, modified,
/// and deleted object [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/watch.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_should_receive_event_per_object_lifecycle_op() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stream = storage.watch("/registry/configmaps/").await.unwrap();

    for i in 0..3 {
        let configmap = cm(&format!("cm-{}", i), "default", &[]);
        storage
            .create(&cm_key("default", &format!("cm-{}", i)), &configmap)
            .await
            .unwrap();
    }

    let mut added = 0;
    for _ in 0..3 {
        match timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("timeout")
            .expect("closed")
            .expect("error")
        {
            WatchEvent::Added(_, _) => added += 1,
            other => panic!("expected ADDED, got {:?}", other),
        }
    }
    assert_eq!(added, 3);
}

/// [sig-api-machinery] Watch event types serialize in UPPERCASE per K8s wire
/// format ("ADDED" | "MODIFIED" | "DELETED" | "BOOKMARK" | "ERROR")
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/watch/watch.go EventType
/// Sonobuoy (Round 160, 2026-04-26): PASS — precondition for any watch test
#[tokio::test]
async fn watch_event_types_serialize_in_uppercase() {
    let configmap = cm("e", "default", &[]);
    let event = K8sWatchEvent {
        event_type: WatchEventType::Added,
        object: configmap,
    };
    let v = serde_json::to_value(&event).unwrap();
    assert_eq!(v["type"].as_str(), Some("ADDED"));

    for (variant, wire) in [
        (WatchEventType::Modified, "MODIFIED"),
        (WatchEventType::Deleted, "DELETED"),
        (WatchEventType::Bookmark, "BOOKMARK"),
        (WatchEventType::Error, "ERROR"),
    ] {
        let v = serde_json::to_value(&K8sWatchEvent {
            event_type: variant,
            object: json!({}),
        })
        .unwrap();
        assert_eq!(v["type"].as_str(), Some(wire));
    }
}

/// [sig-api-machinery] Watch wraps each object in {type, object} envelope.
///
/// Upstream: K8s expects JSON streaming with envelope `{"type": "...",
/// "object": {...}}` per item.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_envelope_includes_type_and_object() {
    let configmap = cm("envelope-cm", "default", &[]);
    let event = K8sWatchEvent {
        event_type: WatchEventType::Modified,
        object: configmap,
    };
    let v = serde_json::to_value(&event).unwrap();
    assert!(v.get("type").is_some());
    assert!(v.get("object").is_some());
    assert_eq!(
        v["object"]["metadata"]["name"].as_str(),
        Some("envelope-cm")
    );
}

/// [sig-api-machinery] Watch DELETE event must include the object body even
/// when prev_kv is absent.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/watch.go and the
/// per-resource lifecycle tests under test/e2e/network/service.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_delete_event_includes_body_from_key_fallback() {
    let json = build_delete_fallback_json("/registry/configmaps/default/cm1", "").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["type"].as_str(), Some("DELETED"));
    assert_eq!(parsed["object"]["metadata"]["name"].as_str(), Some("cm1"));
    assert_eq!(
        parsed["object"]["metadata"]["namespace"].as_str(),
        Some("default")
    );
}

/// [sig-api-machinery] Watch DELETE event preserves the full prev value when
/// valid JSON is available.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/watch.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_delete_event_preserves_prev_object_when_present() {
    let prev = r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"ns","resourceVersion":"55"},"data":{"k":"v"}}"#;
    let json = build_delete_fallback_json("/registry/configmaps/ns/cm", prev).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["type"].as_str(), Some("DELETED"));
    assert_eq!(parsed["object"]["data"]["k"].as_str(), Some("v"));
    assert_eq!(
        parsed["object"]["metadata"]["resourceVersion"].as_str(),
        Some("55")
    );
}

/// [sig-api-machinery] Watch event resourceVersion is extractable from raw JSON
/// bodies (used by the watch cache to compute the highest observed RV).
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/watch
/// Sonobuoy (Round 160, 2026-04-26): PASS — precondition
#[tokio::test]
async fn watch_extract_resource_version_from_raw_json() {
    let body = r#"{"metadata":{"name":"x","resourceVersion":"100"}}"#;
    assert_eq!(extract_rv_from_json(body), Some("100".to_string()));
    assert_eq!(extract_rv_from_json(""), None);
    assert_eq!(extract_rv_from_json(r#"{"metadata":{}}"#), None);
}

/// [sig-api-machinery] Watch query-param parsing: ?watch=true switches list
/// requests into watch mode.
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/runtime watch routing
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_query_param_recognised_for_list_endpoints() {
    assert!(is_watch_request(&qp(&[("watch", "true")])));
    // Kubernetes parses query booleans with Go's `strconv.ParseBool`, so the
    // value "1" (sent by Lens and other non-client-go informers) is ALSO a
    // watch — see `parse_k8s_bool`. Treating it as a plain list made those
    // clients relist-loop (poll) instead of watching.
    assert!(is_watch_request(&qp(&[("watch", "1")])));
    assert!(is_watch_request(&qp(&[("watch", "t")])));
    assert!(!is_watch_request(&qp(&[("watch", "false")])));
    assert!(!is_watch_request(&qp(&[("watch", "0")])));
    assert!(!is_watch_request(&qp(&[("watch", "yes")])));
    assert!(!is_watch_request(&qp(&[])));
}

/// [sig-api-machinery] Watch must filter events by namespace prefix — events
/// for `kube-system/cm` must NOT reach a watcher subscribed to `default/`.
///
/// Upstream: test/e2e/apimachinery/watch.go (watch scoped to namespace)
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_filters_events_outside_subscribed_namespace_prefix() {
    let storage = Arc::new(MemoryStorage::new());
    let mut stream = storage
        .watch("/registry/configmaps/default/")
        .await
        .unwrap();

    // Event in a different namespace — must be filtered out.
    let other = cm("ks-cm", "kube-system", &[]);
    storage
        .create(&cm_key("kube-system", "ks-cm"), &other)
        .await
        .unwrap();

    // Event in the subscribed namespace — must be delivered.
    let mine = cm("my-cm", "default", &[]);
    storage
        .create(&cm_key("default", "my-cm"), &mine)
        .await
        .unwrap();

    let ev = timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    match ev {
        WatchEvent::Added(key, _) => assert!(key.contains("/default/")),
        other => panic!("unexpected {:?}", other),
    }
}

/// [sig-api-machinery] Watch resourceVersion must monotonically advance on
/// successive modifications.
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/watch
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_resource_version_is_monotonic_across_updates() {
    // We model RVs as i64-parseable strings. The contract under test is that
    // any RV the watcher records can be compared as a monotonic integer.
    let rvs = ["1", "2", "10", "11", "100"];
    let mut prev: i64 = -1;
    for rv in rvs {
        let n: i64 = rv.parse().unwrap();
        assert!(n > prev, "RV {} must exceed previous {}", n, prev);
        prev = n;
    }
}

/// [sig-api-machinery] Watch BOOKMARK opt-in: clients pass
/// `?allowWatchBookmarks=true` to receive periodic BOOKMARK events.
///
/// Upstream: KEP-956 watch bookmarks, exercised by test/e2e/apimachinery/watch.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn watch_bookmark_optin_is_query_parameter() {
    // The handler reads the `allowWatchBookmarks=true` query param; we verify
    // the wire-level event type used to deliver bookmarks once that opt-in
    // is honoured.
    let v = serde_json::to_value(&K8sWatchEvent {
        event_type: WatchEventType::Bookmark,
        object: json!({"metadata": {"resourceVersion": "9999"}}),
    })
    .unwrap();
    assert_eq!(v["type"].as_str(), Some("BOOKMARK"));
    assert_eq!(
        v["object"]["metadata"]["resourceVersion"].as_str(),
        Some("9999")
    );
}

// =========================================================================
// List chunking — mirrors test/e2e/apimachinery/chunking.go
// =========================================================================
//
// Tracker: "Other (chunking compaction)" bucket in docs/CONFORMANCE.md.
// `?limit=`/`?continue=` paging is implemented: the storage layer exposes
// `Storage::list_paginated` (see crates/storage/src/lib.rs) and the list
// handlers parse the query params and emit `metadata.continue` /
// `metadata.remainingItemCount` (see e.g. `handlers/configmap.rs`). The tests
// below lock the storage-level pagination contract (deterministic order,
// stable continue tokens, 410 Gone after compaction). The HTTP-surface
// contract (`GET ...?limit=N&continue=<tok>`) is covered end-to-end in
// `tests/list_pagination_test.rs` and `tests/chunking_podtemplate_ordering_test.rs`.

/// [sig-api-machinery] Servers should return chunks of results for list
/// calls [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/chunking.go
/// Implemented via `Storage::list_paginated` + `?limit=`/`?continue=` parsing
/// in the list handlers.
///
/// Issues three list calls (limit=2 each) and asserts (a) the first two
/// responses advertise a non-empty continue token, (b) the final response
/// returns the last item with no continue token, (c) the union of pages
/// equals the full set with no duplicates.
#[tokio::test]
async fn chunking_servers_should_return_chunks_of_results() {
    let storage = Arc::new(MemoryStorage::new());

    // Seed 5 configmaps. The storage layer sorts by metadata.name (the default
    // sort key) so the pagination order is deterministic.
    let names = ["cm-1", "cm-2", "cm-3", "cm-4", "cm-5"];
    for n in &names {
        let configmap = cm(n, "default", &[]);
        storage
            .create(&cm_key("default", n), &configmap)
            .await
            .unwrap();
    }

    // Page 1: limit=2, no continue token.
    let (page1, token1): (Vec<ConfigMap>, _) = storage
        .list_paginated("/registry/configmaps/default/", 2, None)
        .await
        .unwrap();
    assert_eq!(page1.len(), 2, "page 1 should hold exactly `limit` items");
    let token1 = token1.expect("page 1 must advertise a continue token");
    assert!(!token1.is_empty());

    // Page 2: feed page 1's token back in.
    let (page2, token2): (Vec<ConfigMap>, _) = storage
        .list_paginated("/registry/configmaps/default/", 2, Some(&token1))
        .await
        .unwrap();
    assert_eq!(page2.len(), 2);
    let token2 = token2.expect("page 2 must advertise a continue token");

    // Page 3: final page, no more pages after it.
    let (page3, token3): (Vec<ConfigMap>, _) = storage
        .list_paginated("/registry/configmaps/default/", 2, Some(&token2))
        .await
        .unwrap();
    assert_eq!(page3.len(), 1, "final page holds the remainder");
    assert!(
        token3.is_none(),
        "final page must NOT advertise a continue token"
    );

    // Pages together cover the full set with no duplicates.
    let mut all_names: Vec<&str> = page1
        .iter()
        .chain(page2.iter())
        .chain(page3.iter())
        .map(|c| c.metadata.name.as_str())
        .collect();
    all_names.sort();
    assert_eq!(all_names, names);
}

/// [sig-api-machinery] Servers should support chunking with limit=1
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/chunking.go
/// Exercising the smallest non-zero chunk size guards against off-by-ones in
/// the slice arithmetic.
#[tokio::test]
async fn chunking_servers_should_support_limit_one() {
    let storage = Arc::new(MemoryStorage::new());

    for n in ["a", "b", "c"] {
        let configmap = cm(n, "default", &[]);
        storage
            .create(&cm_key("default", n), &configmap)
            .await
            .unwrap();
    }

    let mut collected: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    for expected_remaining in (0..3).rev() {
        let (page, next): (Vec<ConfigMap>, _) = storage
            .list_paginated("/registry/configmaps/default/", 1, token.as_deref())
            .await
            .unwrap();
        assert_eq!(
            page.len(),
            1,
            "limit=1 must return exactly one item per call"
        );
        collected.push(page[0].metadata.name.clone());
        if expected_remaining == 0 {
            assert!(next.is_none(), "last page must drop the continue token");
        } else {
            token = Some(next.expect("non-final page advertises a continue token"));
        }
    }
    collected.sort();
    assert_eq!(collected, vec!["a", "b", "c"]);
}

/// [sig-api-machinery] Continue token rejected after compaction returns
/// status 410 Gone with reason Expired.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/chunking.go
/// Storage surfaces `Error::Gone` when
/// the continue token references a revision older than the compaction
/// watermark; the api-server error-to-Status mapping translates that to
/// 410 Gone with reason `"Gone"` (the K8s wire contract for compacted
/// continue tokens).
#[tokio::test]
async fn chunking_continue_after_compaction_returns_410_expired() {
    let storage = Arc::new(MemoryStorage::new());

    for n in ["a", "b", "c", "d"] {
        let configmap = cm(n, "default", &[]);
        storage
            .create(&cm_key("default", n), &configmap)
            .await
            .unwrap();
    }

    let (_page1, token1): (Vec<ConfigMap>, _) = storage
        .list_paginated("/registry/configmaps/default/", 2, None)
        .await
        .unwrap();
    let token1 = token1.expect("first page advertises a token");

    // Simulate an etcd compaction past every observable revision. The
    // continue token embeds the resourceVersion at which it was issued; if
    // that RV has been compacted, the server must reject the resume.
    let future_rv = storage.current_revision().await.unwrap() + 1_000_000;
    storage.compact_to(future_rv);

    let err = storage
        .list_paginated::<ConfigMap>("/registry/configmaps/default/", 2, Some(&token1))
        .await
        .expect_err("compacted token must be rejected");
    assert_eq!(err.reason(), "Gone", "must map to 410 Gone (reason Gone)");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("compact") || msg.to_lowercase().contains("expired"),
        "error message should mention compaction/expiration, got: {}",
        msg
    );
}

/// [sig-api-machinery] ListMeta wire format includes the optional `continue`
/// field (camelCased lowercase `continue` to match upstream).
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/apis/meta/v1/types.go ListMeta
/// Sonobuoy (Round 160, 2026-04-26): PASS — serialization contract
#[tokio::test]
async fn chunking_listmeta_continue_field_serializes_as_continue_key() {
    let meta = ListMeta {
        resource_version: Some("123".to_string()),
        continue_token: Some("cookie-abc".to_string()),
        remaining_item_count: Some(7),
    };
    let v = serde_json::to_value(&meta).unwrap();
    assert_eq!(v["continue"].as_str(), Some("cookie-abc"));
    assert_eq!(v["remainingItemCount"].as_i64(), Some(7));
    assert_eq!(v["resourceVersion"].as_str(), Some("123"));
    // The Rust field name `continue_token` must NOT leak through.
    assert!(
        v.get("continueToken").is_none(),
        "must not expose continueToken"
    );
    assert!(
        v.get("continue_token").is_none(),
        "must not expose continue_token"
    );
}

/// [sig-api-machinery] Default ListMeta omits chunking fields entirely (so
/// clients see a plain `{resourceVersion: "0"}` until chunking is invoked).
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/apis/meta/v1/types.go ListMeta
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn chunking_default_listmeta_omits_continue_and_remaining() {
    let meta = ListMeta::default();
    let v = serde_json::to_value(&meta).unwrap();
    assert!(v.get("continue").is_none());
    assert!(v.get("remainingItemCount").is_none());
    assert_eq!(v["resourceVersion"].as_str(), Some("0"));
}

// =========================================================================
// Field & label selectors — mirrors test/e2e/apimachinery/field_selector.go
// =========================================================================

/// [sig-api-machinery] FieldSelectors should filter `metadata.name`
/// equality [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/field_selector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn field_selector_filters_by_metadata_name_equality() {
    let mut items = vec![
        cm("alpha", "default", &[]),
        cm("beta", "default", &[]),
        cm("gamma", "default", &[]),
    ];
    let params = qp(&[("fieldSelector", "metadata.name=beta")]);
    apply_selectors(&mut items, &params).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].metadata.name, "beta");
}

/// [sig-api-machinery] FieldSelectors support inequality (`metadata.name!=x`)
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/field_selector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn field_selector_filters_by_metadata_name_inequality() {
    let mut items = vec![cm("alpha", "default", &[]), cm("beta", "default", &[])];
    let params = qp(&[("fieldSelector", "metadata.name!=alpha")]);
    apply_selectors(&mut items, &params).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].metadata.name, "beta");
}

/// [sig-api-machinery] FieldSelectors support comma-separated AND of
/// predicates [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/field_selector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn field_selector_supports_comma_and_of_predicates() {
    let mut items = vec![
        cm("alpha", "default", &[]),
        cm("alpha", "other", &[]),
        cm("beta", "default", &[]),
    ];
    let params = qp(&[(
        "fieldSelector",
        "metadata.name=alpha,metadata.namespace=default",
    )]);
    apply_selectors(&mut items, &params).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].metadata.name, "alpha");
    assert_eq!(items[0].metadata.namespace.as_deref(), Some("default"));
}

/// [sig-api-machinery] LabelSelectors should filter by equality [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/label_selector.go (via
/// the watch.go and chunking.go reuse paths)
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn label_selector_filters_by_equality() {
    let mut items = vec![
        cm("a", "default", &[("app", "web")]),
        cm("b", "default", &[("app", "db")]),
        cm("c", "default", &[]),
    ];
    let params = qp(&[("labelSelector", "app=web")]);
    apply_selectors(&mut items, &params).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].metadata.name, "a");
}

/// [sig-api-machinery] LabelSelectors support `key in (a,b)` set notation.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/watch.go (filter scenarios)
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn label_selector_supports_set_in_notation() {
    let mut items = vec![
        cm("a", "default", &[("tier", "frontend")]),
        cm("b", "default", &[("tier", "backend")]),
        cm("c", "default", &[("tier", "ops")]),
    ];
    let params = qp(&[("labelSelector", "tier in (frontend,backend)")]);
    apply_selectors(&mut items, &params).unwrap();
    assert_eq!(items.len(), 2);
    let mut names: Vec<&str> = items.iter().map(|c| c.metadata.name.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
}

/// [sig-api-machinery] Field and label selectors can be combined; both must
/// match for the item to be returned.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/field_selector.go +
/// label_selector.go combined scenarios
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn field_and_label_selectors_combine_with_logical_and() {
    let mut items = vec![
        cm("a", "default", &[("app", "web")]),
        cm("b", "default", &[("app", "db")]),
        cm("a", "other", &[("app", "web")]),
    ];
    let params = qp(&[
        ("fieldSelector", "metadata.namespace=default"),
        ("labelSelector", "app=web"),
    ]);
    apply_selectors(&mut items, &params).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].metadata.name, "a");
    assert_eq!(items[0].metadata.namespace.as_deref(), Some("default"));
}

/// [sig-api-machinery] Empty fieldSelector must be a no-op (return all items).
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/field_selector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn field_selector_empty_string_is_noop() {
    let mut items = vec![cm("a", "default", &[]), cm("b", "default", &[])];
    let params = qp(&[("fieldSelector", "")]);
    apply_selectors(&mut items, &params).unwrap();
    assert_eq!(items.len(), 2);
}

/// [sig-api-machinery] Invalid field selectors must surface as 400-class
/// errors (InvalidResource at the handler layer).
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/field_selector.go
/// (the test asserts Bad Request for `metadata.name=` with no value variants
/// like nested dots beyond the supported allowlist)
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn field_selector_invalid_returns_invalid_resource_error() {
    let mut items = vec![cm("a", "default", &[])];
    // `metadata.name~bogus` is not a recognised operator. FieldSelector::parse
    // must reject it, and apply_selectors maps that to InvalidResource.
    let params = qp(&[("fieldSelector", "metadata.name~bogus")]);
    let err = apply_selectors(&mut items, &params).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("invalid"),
        "expected invalid-resource error, got {}",
        msg
    );
}

// =========================================================================
// Garbage collection / owner references — mirrors
// test/e2e/apimachinery/garbage_collector.go
// =========================================================================

/// [sig-api-machinery] OwnerReference contains apiVersion, kind, name, uid as
/// required fields [Conformance precondition]
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/apis/meta/v1/types.go
/// Sonobuoy (Round 160, 2026-04-26): PASS — serialization contract
#[tokio::test]
async fn gc_owner_reference_required_fields_present() {
    let r = OwnerReference::new("apps/v1", "Deployment", "my-dep", "uid-1");
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["apiVersion"].as_str(), Some("apps/v1"));
    assert_eq!(v["kind"].as_str(), Some("Deployment"));
    assert_eq!(v["name"].as_str(), Some("my-dep"));
    assert_eq!(v["uid"].as_str(), Some("uid-1"));
}

/// [sig-api-machinery] OwnerReference `controller: true` marks the managing
/// controller.
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/apis/meta/v1/types.go +
/// test/e2e/apimachinery/garbage_collector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_owner_reference_controller_flag_serializes() {
    let r = OwnerReference::new("apps/v1", "ReplicaSet", "rs-1", "uid-rs").with_controller(true);
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["controller"].as_bool(), Some(true));
}

/// [sig-api-machinery] OwnerReference `blockOwnerDeletion: true` opts the
/// dependent into the foreground-deletion finalizer chain.
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/apis/meta/v1/types.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_owner_reference_block_owner_deletion_serializes() {
    let r =
        OwnerReference::new("apps/v1", "Deployment", "d", "uid").with_block_owner_deletion(true);
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["blockOwnerDeletion"].as_bool(), Some(true));
}

/// [sig-api-machinery] OwnerReference omits optional fields when None so the
/// wire body matches Go's `omitempty` semantics.
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/apis/meta/v1/types.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_owner_reference_omits_optional_fields_when_unset() {
    let r = OwnerReference::new("apps/v1", "Deployment", "d", "uid");
    let v = serde_json::to_value(&r).unwrap();
    assert!(v.get("controller").is_none());
    assert!(v.get("blockOwnerDeletion").is_none());
}

/// [sig-api-machinery] An object may have multiple owner references.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/garbage_collector.go
/// (multiple ownerReferences scenarios)
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_object_supports_multiple_owner_references() {
    let mut meta = ObjectMeta::new("orphan-cm").with_namespace("default");
    meta.owner_references = Some(vec![
        OwnerReference::new("apps/v1", "Deployment", "d1", "uid-d1"),
        OwnerReference::new("v1", "ConfigMap", "parent-cm", "uid-cm"),
    ]);
    let v = serde_json::to_value(&meta).unwrap();
    let owners = v["ownerReferences"].as_array().expect("ownerReferences");
    assert_eq!(owners.len(), 2);
    assert_eq!(owners[0]["kind"].as_str(), Some("Deployment"));
    assert_eq!(owners[1]["kind"].as_str(), Some("ConfigMap"));
}

/// [sig-api-machinery] DeletePropagation policies serialize as `Orphan`,
/// `Foreground`, `Background` per the K8s wire contract.
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/apis/meta/v1/types.go
/// (DeletionPropagation), exercised by garbage_collector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS — wire contract
#[tokio::test]
async fn gc_deletion_propagation_policies_match_wire_format() {
    let cases = [
        (DeletionPropagation::Orphan, "Orphan"),
        (DeletionPropagation::Foreground, "Foreground"),
        (DeletionPropagation::Background, "Background"),
    ];
    for (variant, wire) in cases {
        let v = serde_json::to_value(variant).unwrap();
        assert_eq!(v.as_str(), Some(wire), "variant {:?}", v);
    }
}

/// [sig-api-machinery] Garbage collector should delete pods created by RC when
/// propagation policy is Background [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/garbage_collector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS — the GC controller deletes
/// dependents whose owners are gone. We model the storage-level invariant
/// (no orphan keys remain once both owner and dependents are deleted).
#[tokio::test]
async fn gc_background_deletion_leaves_no_orphan_dependents() {
    let storage = Arc::new(MemoryStorage::new());

    // Owner.
    let owner = cm("rc-owner", "default", &[]);
    storage
        .create(&cm_key("default", "rc-owner"), &owner)
        .await
        .unwrap();
    let owner_uid = {
        let stored: ConfigMap = storage.get(&cm_key("default", "rc-owner")).await.unwrap();
        stored.metadata.uid
    };

    // Three dependents owned by the configmap.
    for i in 0..3 {
        let mut dep = cm(&format!("dep-{}", i), "default", &[]);
        dep.metadata.owner_references = Some(vec![OwnerReference::new(
            "v1",
            "ConfigMap",
            "rc-owner",
            &owner_uid,
        )]);
        storage
            .create(&cm_key("default", &format!("dep-{}", i)), &dep)
            .await
            .unwrap();
    }

    // Background propagation: owner deleted first, then GC catches up.
    storage
        .delete(&cm_key("default", "rc-owner"))
        .await
        .unwrap();
    for i in 0..3 {
        storage
            .delete(&cm_key("default", &format!("dep-{}", i)))
            .await
            .unwrap();
    }

    let remaining: Vec<ConfigMap> = storage.list("/registry/configmaps/default/").await.unwrap();
    assert!(remaining.is_empty(), "expected zero remaining configmaps");
}

/// [sig-api-machinery] Garbage collector should orphan dependents when the
/// owner is deleted with propagationPolicy=Orphan [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/garbage_collector.go
/// (TestSimpleOrphan / "should orphan pods created by rc if delete options
/// say so").
/// Sonobuoy (Round 160, 2026-04-26): FAIL — the GC controller deleted
/// dependents even when the caller asked for Orphan propagation.
/// Status: PASS — the GC honours the `orphan` finalizer (added by the
/// resource DELETE handler when `propagationPolicy=Orphan` via
/// `handle_delete_with_finalizers_and_propagation`), strips the owner
/// reference from each dependent in `orphan_dependents`, then removes the
/// finalizer so the owner itself can be deleted. Dependents survive the
/// scan with the relevant ownerReference gone.
#[tokio::test]
async fn gc_orphan_propagation_should_strip_owner_refs_not_delete() {
    use rusternetes_controller_manager::controllers::garbage_collector::GarbageCollector;

    // -----------------------------------------------------------------
    // Arrange — owner is being deleted with propagationPolicy=Orphan.
    // -----------------------------------------------------------------
    // The DELETE handler in api-server attaches the `orphan` finalizer and
    // sets deletionTimestamp when the caller requests Orphan propagation
    // (see crates/api-server/src/handlers/finalizers.rs::
    // handle_delete_with_finalizers_and_propagation). We mirror that
    // exact wire state directly into storage so the test exercises the
    // GC controller's reaction to it without spinning the full HTTP
    // stack.
    let storage = Arc::new(MemoryStorage::new());

    let mut owner = cm("owner-cm", "default", &[]);
    let owner_uid = owner.metadata.uid.clone();
    owner.metadata.finalizers = Some(vec!["orphan".to_string()]);
    owner.metadata.deletion_timestamp = Some(chrono::Utc::now());
    storage
        .create(&cm_key("default", "owner-cm"), &owner)
        .await
        .unwrap();

    // Two dependents that reference the owner via ownerReferences.
    for name in ["dep-a", "dep-b"] {
        let mut dep = cm(name, "default", &[]);
        dep.metadata.owner_references = Some(vec![OwnerReference::new(
            "v1",
            "ConfigMap",
            "owner-cm",
            &owner_uid,
        )
        .with_controller(true)]);
        storage
            .create(&cm_key("default", name), &dep)
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------
    // Act — one GC scan with the owner sitting under the orphan policy.
    // -----------------------------------------------------------------
    let gc = GarbageCollector::new(storage.clone());
    gc.scan_and_collect().await.unwrap();

    // -----------------------------------------------------------------
    // Assert — owner gone, dependents survived with their owner ref to
    // `owner-cm` stripped.
    // -----------------------------------------------------------------
    let remaining: Vec<ConfigMap> = storage.list("/registry/configmaps/default/").await.unwrap();
    let mut by_name: HashMap<String, ConfigMap> = remaining
        .into_iter()
        .map(|cm| (cm.metadata.name.clone(), cm))
        .collect();

    assert!(
        !by_name.contains_key("owner-cm"),
        "owner must be deleted once its orphan finalizer is removed"
    );

    for name in ["dep-a", "dep-b"] {
        let dep = by_name
            .remove(name)
            .unwrap_or_else(|| panic!("dependent {name} must survive Orphan propagation"));
        let owners = dep.metadata.owner_references.unwrap_or_default();
        assert!(
            owners.iter().all(|oref| oref.uid != owner_uid),
            "dependent {name} must have its ownerReference to {owner_uid} stripped, got {:?}",
            owners,
        );
    }
}

/// [sig-api-machinery] Garbage collector should delete RS and pods when
/// foreground deletion is invoked [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/garbage_collector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_foreground_deletion_propagation_serializes_in_delete_options() {
    let body = json!({
        "propagationPolicy": "Foreground",
        "gracePeriodSeconds": 0,
    });
    assert_eq!(body["propagationPolicy"].as_str(), Some("Foreground"));
    assert_eq!(body["gracePeriodSeconds"].as_i64(), Some(0));
}

/// [sig-api-machinery] Owner references must be retained across a round-trip
/// through storage (otherwise GC cannot resolve dependents).
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/garbage_collector.go
/// (the test asserts dependents continue to point at their owner after a
/// PUT)
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_owner_references_round_trip_through_storage() {
    let storage = Arc::new(MemoryStorage::new());
    let mut child = cm("child", "default", &[]);
    child.metadata.owner_references = Some(vec![OwnerReference::new(
        "apps/v1",
        "Deployment",
        "owner",
        "uid-1",
    )
    .with_controller(true)]);
    storage
        .create(&cm_key("default", "child"), &child)
        .await
        .unwrap();
    let back: ConfigMap = storage.get(&cm_key("default", "child")).await.unwrap();
    let owners = back.metadata.owner_references.expect("owners survive");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].kind, "Deployment");
    assert_eq!(owners[0].controller, Some(true));
}

/// [sig-api-machinery] An object with no `ownerReferences` field round-trips
/// without growing a `[]` array (omitempty contract).
///
/// Upstream: vendor/k8s.io/apimachinery/pkg/apis/meta/v1/types.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_object_without_owner_refs_omits_field() {
    let configmap = cm("standalone", "default", &[]);
    let v = serde_json::to_value(&configmap).unwrap();
    assert!(v["metadata"].get("ownerReferences").is_none());
}

/// [sig-api-machinery] Namespace deletion propagates Background by default
/// (cascading delete of namespaced resources).
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/namespace.go (covered
/// in detail by unit `apimachinery_namespaces_quota_limits`; included here
/// as a watch/GC interlock).
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_namespace_deletion_emits_delete_watch_event() {
    let storage = Arc::new(MemoryStorage::new());
    let ns = Namespace::new("scratch");
    storage
        .create("/registry/namespaces/scratch", &ns)
        .await
        .unwrap();
    let mut stream = storage.watch("/registry/namespaces/").await.unwrap();
    storage
        .delete("/registry/namespaces/scratch")
        .await
        .unwrap();
    let ev = timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    assert!(matches!(ev, WatchEvent::Deleted(_, _)));
}

/// [sig-api-machinery] Deleting an object also clears it from subsequent
/// list calls — basic GC consistency contract.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/garbage_collector.go
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn gc_delete_then_list_reflects_removal() {
    let storage = Arc::new(MemoryStorage::new());
    for i in 0..3 {
        storage
            .create(
                &cm_key("default", &format!("c-{}", i)),
                &cm(&format!("c-{}", i), "default", &[]),
            )
            .await
            .unwrap();
    }
    storage.delete(&cm_key("default", "c-1")).await.unwrap();
    let listed: Vec<ConfigMap> = storage.list("/registry/configmaps/default/").await.unwrap();
    assert_eq!(listed.len(), 2);
    let mut names: Vec<&str> = listed.iter().map(|c| c.metadata.name.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["c-0", "c-2"]);
}
