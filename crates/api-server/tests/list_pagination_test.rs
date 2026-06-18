//! Upstream-parity tests for LIST pagination + continue tokens (`?limit=N` /
//! `?continue=<token>`).
//!
//! Source of truth: the Kubernetes apiserver pagination contract, exercised
//! by upstream tests under
//! `apiserver/pkg/storage/.../continue_test.go` and
//! `test/e2e/apimachinery/chunking.go`. The semantics this file pins:
//!
//!   1. `?limit=N` — first page returns N items, plus a non-empty
//!      `metadata.continue` token, plus `metadata.remainingItemCount`.
//!   2. `?limit=N&continue=<token>` — second page returns the next N items,
//!      with a new token; iterating to exhaustion yields every item exactly
//!      once, in sorted order.
//!   3. `?limit=200` (limit greater than seed size) — returns all items,
//!      no continue token.
//!   4. `?limit=0` — returns all items (upstream contract: `limit=0` is
//!      "no chunking", not "empty page"). Pinned as the upstream
//!      behaviour rusternetes is expected to match.
//!   5. `?limit=N` + `resourceVersion=<old>` — pagination chain is
//!      consistent at the snapshot resourceVersion.
//!   6. `?continue=<malformed>` — must return a `Status` body with HTTP
//!      `4xx`/`410 Gone`, not a partial list.
//!   7. `?continue=<expired>` — if the storage layer implements compaction,
//!      expired tokens return `410 Gone` with `reason: Expired`. Otherwise
//!      pinned as `#[ignore]` blocked on compaction implementation.
//!   8. Mid-pagination mutation — a continue chain represents a snapshot at
//!      the original list's resourceVersion; items created mid-chain MUST NOT
//!      appear in subsequent pages of that chain.
//!
//! Harness: `Arc<MemoryStorage>` + `build_router(...)` + `oneshot`, same as
//! `tests/integration_configmap_lifecycle.rs`. The authorizer is
//! `AlwaysAllow` and `skip_auth=true`, so no bearer token is required.
//!
//! Status: ConfigMap LIST handler (`crates/api-server/src/handlers/configmap.rs`)
//! does NOT currently wire `?limit`/`?continue` — it calls `storage.list()`
//! and returns the unpaginated result. The HTTP-surface tests that depend on
//! pagination wiring are marked `#[ignore = "blocked on issue #TBD: ..."]`
//! until the handler adopts the pattern from
//! `crates/api-server/src/handlers/pod.rs` (or `podtemplate.rs`). The
//! pagination *primitive* (`rusternetes_common::paginate` and
//! `Storage::list_paginated`) is implemented and unit-tested elsewhere; this
//! file is the wire-level contract pin.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Harness — thin shims over the shared `TestApiServer`, preserving this file's
// `post_json(&state, …)` / `get_json(&state, …)` call sites.
// ---------------------------------------------------------------------------

fn make_state() -> TestApiServer {
    TestApiServer::new()
}

async fn post_json(state: &TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    state.post(uri, body).await
}

async fn get_json(state: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    state.get(uri).await
}

/// Issue a paginated LIST and return `(status, body)`. The caller assembles
/// the `?limit=...&continue=...` query string explicitly so test failures
/// surface the exact URL.
async fn list_with_paging(
    state: &TestApiServer,
    namespace: &str,
    query: &str,
) -> (StatusCode, Value) {
    let uri = if query.is_empty() {
        format!("/api/v1/namespaces/{}/configmaps", namespace)
    } else {
        format!("/api/v1/namespaces/{}/configmaps?{}", namespace, query)
    };
    get_json(state, &uri).await
}

/// Create namespace `ns` via the REST surface so the per-namespace LIST
/// routes resolve. Idempotent for the test setup — we only need it to
/// succeed once per test.
async fn create_namespace(state: &TestApiServer, ns: &str) {
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": ns }
    });
    let (status, body) = post_json(state, "/api/v1/namespaces", &ns_body).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK || status == StatusCode::CONFLICT,
        "namespace create must return 200/201/409 (already-exists), got {} body={}",
        status,
        body
    );
}

/// Seed `n` ConfigMaps named `cm-NNN` (zero-padded so lexicographic sort ==
/// numeric sort). Returns the sorted list of seeded names.
async fn seed_configmaps(state: &TestApiServer, ns: &str, n: usize) -> Vec<String> {
    create_namespace(state, ns).await;
    let mut names = Vec::with_capacity(n);
    for i in 0..n {
        let name = format!("cm-{:03}", i);
        let body = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": ns },
            "data": { "key": format!("value-{}", i) }
        });
        let uri = format!("/api/v1/namespaces/{}/configmaps", ns);
        let (status, resp) = post_json(state, &uri, &body).await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "seed: create {} returned {} body={}",
            name,
            status,
            resp
        );
        names.push(name);
    }
    names.sort();
    names
}

/// Extract the `items[].metadata.name` slice from a LIST response body.
fn item_names(body: &Value) -> Vec<String> {
    body["items"]
        .as_array()
        .expect("LIST must return .items array")
        .iter()
        .map(|it| {
            it["metadata"]["name"]
                .as_str()
                .expect("each item must have metadata.name")
                .to_string()
        })
        .collect()
}

/// URL-encode a continue token. Tokens are base64 (`A-Za-z0-9+/=`); `+`
/// and `=` must be percent-encoded when placed in a query string.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(c),
            _ => {
                let mut buf = [0_u8; 4];
                for b in c.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// 1. First page: ?limit=N
// ---------------------------------------------------------------------------

/// Upstream parity: `?limit=10` returns exactly 10 items, a non-empty
/// `metadata.continue` token, and (per upstream contract) a
/// `metadata.remainingItemCount` reflecting the items not yet sent.
#[tokio::test]
async fn limit_first_page_returns_n_plus_continue_token() {
    let state = make_state();
    let ns = "pag-first";
    seed_configmaps(&state, ns, 100).await;

    let (status, body) = list_with_paging(&state, ns, "limit=10").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "first-page LIST must return 200, body={}",
        body
    );

    let names = item_names(&body);
    assert_eq!(
        names.len(),
        10,
        "first page must contain exactly limit items, got {}: {:?}",
        names.len(),
        names
    );

    let cont = body["metadata"]["continue"].as_str().unwrap_or("");
    assert!(
        !cont.is_empty(),
        "first page must advertise a non-empty metadata.continue token, body={}",
        body
    );

    // upstream contract: remainingItemCount, when present, equals
    // total - sent. Some servers omit the field; tolerate that, but if
    // present it must equal 90.
    if let Some(rem) = body["metadata"]["remainingItemCount"].as_i64() {
        assert_eq!(
            rem, 90,
            "remainingItemCount must equal seeded total - page size, got {}",
            rem
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Iterate to exhaustion: ?limit=N + ?continue=<token>
// ---------------------------------------------------------------------------

/// Iterate `?limit=10` to exhaustion. Verifies (a) total count equals seed
/// count, (b) no duplicates, (c) every seeded name appears, (d) the final
/// page omits the continue token, (e) results are sorted.
#[tokio::test]
async fn limit_continue_chain_covers_all_items_exactly_once() {
    let state = make_state();
    let ns = "pag-chain";
    let seeded = seed_configmaps(&state, ns, 100).await;

    let mut collected: Vec<String> = Vec::new();
    let mut next_token: Option<String> = None;
    let mut iterations = 0_usize;

    loop {
        iterations += 1;
        assert!(
            iterations <= 50,
            "pagination did not terminate in {} iterations — token chain loop?",
            iterations
        );

        let query = match &next_token {
            None => "limit=10".to_string(),
            Some(t) => format!("limit=10&continue={}", urlencode(t)),
        };
        let (status, body) = list_with_paging(&state, ns, &query).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "page {} LIST must return 200, body={}",
            iterations,
            body
        );

        let page = item_names(&body);
        // every page except possibly the last has exactly limit items;
        // the last page has <= limit.
        assert!(
            page.len() <= 10,
            "page must not exceed limit, got {}: {:?}",
            page.len(),
            page
        );

        // Sorted within the page.
        let mut sorted = page.clone();
        sorted.sort();
        assert_eq!(page, sorted, "page items must be sorted, got {:?}", page);

        // First name on this page must come after the last name on the
        // previous page.
        if let Some(prev_last) = collected.last() {
            if let Some(this_first) = page.first() {
                assert!(
                    this_first > prev_last,
                    "page boundary out of order: prev_last={} this_first={}",
                    prev_last,
                    this_first
                );
            }
        }

        collected.extend(page);

        next_token = body["metadata"]["continue"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        if next_token.is_none() {
            break;
        }
    }

    assert_eq!(
        collected.len(),
        seeded.len(),
        "total items across all pages must equal seed count: collected={} seeded={}",
        collected.len(),
        seeded.len()
    );

    // No duplicates.
    let mut dedup = collected.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(
        dedup.len(),
        collected.len(),
        "pagination must not return duplicates"
    );

    // Set equality with the seeded names.
    let mut all_sorted = collected.clone();
    all_sorted.sort();
    assert_eq!(all_sorted, seeded);
}

// ---------------------------------------------------------------------------
// 3. Limit greater than the seed set
// ---------------------------------------------------------------------------

/// `?limit=200` against a 100-item seed returns all 100 items and no
/// continue token. This case must succeed even on a handler that does
/// NOT yet honour pagination, because the unpaginated list is a
/// superset of "limit=200 returns everything"; we keep it un-ignored so
/// the basic LIST surface stays pinned.
#[tokio::test]
async fn limit_greater_than_total_returns_all_no_continue() {
    let state = make_state();
    let ns = "pag-over";
    let seeded = seed_configmaps(&state, ns, 100).await;

    let (status, body) = list_with_paging(&state, ns, "limit=200").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "limit>total LIST must return 200, body={}",
        body
    );

    let mut names = item_names(&body);
    names.sort();
    assert_eq!(names, seeded);

    assert!(
        body["metadata"]["continue"]
            .as_str()
            .unwrap_or("")
            .is_empty(),
        "limit>total must NOT advertise a continue token, got {:?}",
        body["metadata"]["continue"]
    );
}

// ---------------------------------------------------------------------------
// 4. ?limit=0 — upstream contract: "no chunking, return everything"
// ---------------------------------------------------------------------------

/// Upstream contract: `limit=0` means "no chunking, return everything",
/// not "empty page". Pin the expected upstream wire behaviour.
#[tokio::test]
async fn limit_zero_returns_all_items() {
    let state = make_state();
    let ns = "pag-zero";
    let seeded = seed_configmaps(&state, ns, 100).await;

    let (status, body) = list_with_paging(&state, ns, "limit=0").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "limit=0 LIST must return 200, body={}",
        body
    );

    let mut names = item_names(&body);
    names.sort();
    assert_eq!(
        names, seeded,
        "limit=0 must return every item (upstream: limit=0 == unlimited)"
    );

    assert!(
        body["metadata"]["continue"]
            .as_str()
            .unwrap_or("")
            .is_empty(),
        "limit=0 must NOT advertise a continue token, got {:?}",
        body["metadata"]["continue"]
    );
}

// ---------------------------------------------------------------------------
// 5. resourceVersion-pinned pagination
// ---------------------------------------------------------------------------

/// `?limit=10` combined with an explicit `?resourceVersion=<rv>` from a
/// prior list must yield a stable snapshot view across pages — items
/// created after the first page MUST NOT appear in subsequent pages of
/// the same continue chain.
#[tokio::test]
async fn limit_with_explicit_resource_version_is_consistent() {
    let state = make_state();
    let ns = "pag-rv";
    seed_configmaps(&state, ns, 100).await;

    // Capture the current resourceVersion via a no-arg LIST.
    let (status, body) = list_with_paging(&state, ns, "").await;
    assert_eq!(status, StatusCode::OK);
    let snapshot_rv = body["metadata"]["resourceVersion"]
        .as_str()
        .expect("LIST must return metadata.resourceVersion")
        .to_string();

    // Page 1 at the snapshot RV.
    let q1 = format!("limit=10&resourceVersion={}", snapshot_rv);
    let (status, page1) = list_with_paging(&state, ns, &q1).await;
    assert_eq!(status, StatusCode::OK);
    let token = page1["metadata"]["continue"]
        .as_str()
        .expect("page 1 must advertise a continue token")
        .to_string();
    assert!(!token.is_empty());

    // Inject a NEW item between pages. It must not surface in page 2 of
    // the snapshot-pinned chain.
    let intruder = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-zzz-intruder", "namespace": ns },
        "data": { "k": "v" }
    });
    let (st, _) = post_json(
        &state,
        &format!("/api/v1/namespaces/{}/configmaps", ns),
        &intruder,
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);

    // Page 2 reusing the snapshot token must not include the intruder.
    let q2 = format!(
        "limit=10&resourceVersion={}&continue={}",
        snapshot_rv,
        urlencode(&token)
    );
    let (status, page2) = list_with_paging(&state, ns, &q2).await;
    assert_eq!(status, StatusCode::OK);
    let names = item_names(&page2);
    assert!(
        !names.iter().any(|n| n == "cm-zzz-intruder"),
        "intruder MUST NOT appear in snapshot-pinned page 2, got names={:?}",
        names
    );
}

// ---------------------------------------------------------------------------
// 6. Malformed continue token
// ---------------------------------------------------------------------------

/// A malformed `?continue=<garbage>` token must NOT silently return a
/// partial list. Upstream returns either `400 Bad Request` or `410 Gone`
/// with a `Status` body (`kind: Status`).
#[tokio::test]
async fn malformed_continue_token_returns_status_error() {
    let state = make_state();
    let ns = "pag-malformed";
    seed_configmaps(&state, ns, 5).await;

    let (status, body) =
        list_with_paging(&state, ns, "limit=2&continue=this-is-not-a-valid-token!!").await;

    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::GONE,
        "malformed continue token must return 400 or 410, got {} body={}",
        status,
        body
    );

    // The body must be a Status object, not a list of items. Either
    // `kind: Status` OR `apiVersion: v1` + `status: Failure` is acceptable.
    let kind = body["kind"].as_str().unwrap_or("");
    let status_field = body["status"].as_str().unwrap_or("");
    assert!(
        kind == "Status" || status_field == "Failure",
        "malformed-token response must be a Status object (kind=Status), got body={}",
        body
    );
}

// ---------------------------------------------------------------------------
// 7. Expired continue token (compaction)
// ---------------------------------------------------------------------------

/// If the storage backend implements compaction (e.g. etcd compacts old
/// revisions after `--etcd-compaction-interval`), a `?continue=<expired>`
/// token must return `410 Gone` with `reason: Expired` and a fresh
/// continue token so clients can restart the list cleanly.
///
/// `MemoryStorage` (and the rhino backends) do NOT yet implement
/// compaction, so this scenario is pinned as `#[ignore]` rather than
/// asserting a behaviour the system can't yet exhibit.
#[tokio::test]
async fn expired_continue_token_returns_410_gone() {
    let state = make_state();
    let ns = "pag-expired";
    seed_configmaps(&state, ns, 5).await;

    // Without compaction we cannot force a token to expire, so we
    // simulate by submitting a syntactically valid but stale token
    // referencing a fake old revision. Once compaction lands, replace
    // this stub with a real "create token, compact past RV, replay"
    // sequence.
    let stale_token = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        r#"{"start":2,"resource_version":"1","filters":{},"nonce":0,"total_at_creation":0,"created_at":1}"#,
    );

    let (status, body) = list_with_paging(
        &state,
        ns,
        &format!("limit=2&continue={}", urlencode(&stale_token)),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::GONE,
        "expired token must return 410 Gone, got {} body={}",
        status,
        body
    );
    assert_eq!(body["reason"].as_str().unwrap_or(""), "Expired");
}

// ---------------------------------------------------------------------------
// 8. Mid-pagination mutation
// ---------------------------------------------------------------------------

/// Seed 100, paginate halfway through, INSERT a new item, then resume the
/// continue chain. The intruder MUST NOT appear in subsequent pages of
/// the same chain — the chain is conceptually a snapshot at the original
/// list's RV.
#[tokio::test]
async fn mid_pagination_insert_not_visible_in_continue_chain() {
    let state = make_state();
    let ns = "pag-midmut";
    seed_configmaps(&state, ns, 100).await;

    // Walk 5 pages of 10, capturing the running token.
    let mut token: Option<String> = None;
    let mut collected: Vec<String> = Vec::new();
    for page_idx in 0..5 {
        let query = match &token {
            None => "limit=10".to_string(),
            Some(t) => format!("limit=10&continue={}", urlencode(t)),
        };
        let (status, body) = list_with_paging(&state, ns, &query).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "page {} LIST must succeed, body={}",
            page_idx,
            body
        );
        collected.extend(item_names(&body));
        token = body["metadata"]["continue"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        assert!(
            token.is_some(),
            "page {} should still have a continue token (50/100 collected)",
            page_idx
        );
    }
    assert_eq!(collected.len(), 50);

    // Inject a name that sorts AFTER everything seeded so it would
    // naturally appear on a "fresh" page 6+ if snapshotting were broken.
    let intruder = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-zzz-mid-intruder", "namespace": ns },
        "data": { "k": "v" }
    });
    let (st, _) = post_json(
        &state,
        &format!("/api/v1/namespaces/{}/configmaps", ns),
        &intruder,
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);

    // Drain the remaining pages using the captured token chain.
    while let Some(t) = token.clone() {
        let (status, body) =
            list_with_paging(&state, ns, &format!("limit=10&continue={}", urlencode(&t))).await;
        assert_eq!(status, StatusCode::OK);
        let names = item_names(&body);
        assert!(
            !names.iter().any(|n| n == "cm-zzz-mid-intruder"),
            "intruder MUST NOT surface in continue-chain page, got {:?}",
            names
        );
        collected.extend(names);
        token = body["metadata"]["continue"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
    }

    // Total seen across the chain equals the original seed (100), not 101.
    let mut deduped = collected.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        100,
        "continue chain must yield exactly the snapshot's items, got {}",
        deduped.len()
    );
    assert!(
        !deduped.iter().any(|n| n == "cm-zzz-mid-intruder"),
        "intruder must not appear in any page of the snapshot chain"
    );
}

// ---------------------------------------------------------------------------
// Sanity pin (not ignored): the no-paging LIST surface still works.
// ---------------------------------------------------------------------------

/// Sanity check that does not depend on pagination: a no-query LIST of
/// 100 items returns 100 items. This pins that the harness, namespace
/// creation, and seed loop themselves work end-to-end so that an
/// `#[ignore]`d pagination test isn't masking a harness regression.
#[tokio::test]
async fn baseline_no_paging_list_returns_all_items() {
    let state = make_state();
    let ns = "pag-baseline";
    let seeded = seed_configmaps(&state, ns, 100).await;

    let (status, body) = list_with_paging(&state, ns, "").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "baseline LIST must return 200, body={}",
        body
    );

    let mut names = item_names(&body);
    names.sort();
    assert_eq!(
        names.len(),
        seeded.len(),
        "baseline LIST must return every seeded item, got {} expected {}",
        names.len(),
        seeded.len()
    );
    assert_eq!(names, seeded);
}
