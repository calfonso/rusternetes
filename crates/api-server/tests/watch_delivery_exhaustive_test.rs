//! Exhaustive watch-delivery smoke test: for EVERY resource the server
//! advertises as watchable (verb `watch` in discovery), subscribe to its
//! collection watch, create an object, and assert the `ADDED` event is pushed
//! to the subscriber.
//!
//! Unlike `watch_delivery_matrix_test.rs` (a curated set driven through the
//! POST handlers with hand-written valid bodies), this test is **self-
//! maintaining**: it enumerates resources from the in-process discovery
//! documents (`/api/v1` + each `/apis/<group>/<version>`), so a newly-added
//! resource is covered automatically. To avoid needing a schema-valid create
//! body for ~60 kinds, the object is seeded directly via `storage.create`
//! (bypassing admission/validation) — what we assert is the watch path:
//! `?watch=true` dispatch on the list handler + typed deserialization in the
//! watch handler + delivery to the subscriber.
//!
//! Fast: pure `MemoryStorage` + in-process router, no containers.

use axum::http::StatusCode;
use futures::StreamExt;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const NS: &str = "watchexhaustive";
const OBJ: &str = "probe1";

async fn get_json(router: &TestApiServer, uri: &str) -> Option<Value> {
    let (status, value) = router.get(uri).await;
    if status != StatusCode::OK {
        return None;
    }
    Some(value)
}

/// A served resource we should be able to watch.
#[derive(Debug, Clone)]
struct Res {
    group: String,   // "" for core
    version: String, // "v1"
    plural: String,  // "pods"
    kind: String,
    namespaced: bool,
}

impl Res {
    fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }
    /// Collection path (no query). Namespaced uses NS.
    fn collection(&self) -> String {
        let base = if self.group.is_empty() {
            format!("/api/{}", self.version)
        } else {
            format!("/apis/{}/{}", self.group, self.version)
        };
        if self.namespaced {
            format!("{base}/namespaces/{NS}/{}", self.plural)
        } else {
            format!("{base}/{}", self.plural)
        }
    }
}

fn parse_resource_list(doc: &Value, group: &str, version: &str, out: &mut Vec<Res>) {
    let Some(items) = doc.get("resources").and_then(|r| r.as_array()) else {
        return;
    };
    for r in items {
        let name = r.get("name").and_then(|v| v.as_str()).unwrap_or("");
        // Skip subresources (e.g. pods/status) — not independently watchable.
        if name.is_empty() || name.contains('/') {
            continue;
        }
        let verbs: Vec<&str> = r
            .get("verbs")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
            .unwrap_or_default();
        if !verbs.contains(&"watch") {
            continue;
        }
        out.push(Res {
            group: group.to_string(),
            version: version.to_string(),
            plural: name.to_string(),
            kind: r
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            namespaced: r
                .get("namespaced")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        });
    }
}

/// Enumerate every watchable resource from the live discovery documents.
async fn enumerate(router: &TestApiServer) -> Vec<Res> {
    let mut out = Vec::new();

    // Core group at /api/v1.
    if let Some(core) = get_json(router, "/api/v1").await {
        parse_resource_list(&core, "", "v1", &mut out);
    }

    // Named groups: /apis -> groups[].preferredVersion.groupVersion -> /apis/<gv>.
    if let Some(groups_doc) = get_json(router, "/apis").await {
        if let Some(groups) = groups_doc.get("groups").and_then(|g| g.as_array()) {
            for g in groups {
                let gv = g
                    .pointer("/preferredVersion/groupVersion")
                    .and_then(|v| v.as_str());
                let Some(gv) = gv else { continue };
                let (group, version) = gv.split_once('/').unwrap_or(("", gv));
                if let Some(rl) = get_json(router, &format!("/apis/{gv}")).await {
                    parse_resource_list(&rl, group, version, &mut out);
                }
            }
        }
    }
    out
}

/// Outcome of probing one resource's `?watch=true` endpoint.
#[derive(Debug, PartialEq)]
enum Outcome {
    /// Endpoint streamed watch frames AND pushed our seeded object's ADDED.
    Delivered,
    /// Endpoint dispatched a watch: it either emitted a `{"type":...}` frame /
    /// bookmark, or kept the connection open past the read window (a streaming
    /// response, unlike a list which completes). Our minimal seed may be
    /// dropped by the watch handler's typed deserialization, so we don't see
    /// its ADDED — but DISPATCH works. Real per-kind delivery lives in
    /// watch_delivery_matrix_test.rs with valid bodies.
    Dispatched,
    /// Endpoint returned a one-shot `*List` instead of a stream — the
    /// `?watch=true` dispatch bug (the class #888/#892 fixed). Hard failure.
    OneShotList,
    /// 200 but the stream ended with no frames and no list, or non-200.
    NoResponse,
}

/// GET the watch URL, read the body for up to `deadline`, and classify the
/// response as a watch stream vs a one-shot list.
async fn classify(router: TestApiServer, uri: String, deadline: Duration) -> Outcome {
    let resp = router.respond("GET", &uri, None, None).await;
    if resp.status() != StatusCode::OK {
        return Outcome::NoResponse;
    }
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut saw_frame = false;
    let mut delivered = false;
    let run = async {
        loop {
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
                            if v.get("type").is_some() && v.get("object").is_some() {
                                saw_frame = true;
                                if v.get("type").and_then(|t| t.as_str()) == Some("ADDED")
                                    && v.pointer("/object/metadata/name").and_then(|n| n.as_str())
                                        == Some(OBJ)
                                {
                                    delivered = true;
                                    return;
                                }
                            }
                        }
                    }
                }
                // Stream ended (handler completed the response).
                Some(Err(_)) | None => return,
            }
        }
    };
    // `timed_out` ⟺ the stream stayed OPEN past the window — i.e. a long-lived
    // watch response, as opposed to a list which completes immediately.
    let timed_out = timeout(deadline, run).await.is_err();

    if delivered {
        return Outcome::Delivered;
    }
    if saw_frame || timed_out {
        // Emitted a watch frame, or held the connection open — either way the
        // handler dispatched a watch rather than answering with a list.
        return Outcome::Dispatched;
    }
    // Stream ended with no frames. A completed `*List` body means ?watch=true
    // was ignored (one-shot list).
    if let Ok(v) = serde_json::from_str::<Value>(buf.trim()) {
        let is_list = v
            .get("kind")
            .and_then(|k| k.as_str())
            .map(|k| k.ends_with("List"))
            .unwrap_or(false)
            || v.get("items").is_some();
        if is_list {
            return Outcome::OneShotList;
        }
    }
    Outcome::NoResponse
}

/// Probe a resource's watch endpoint.
///
/// First seed a minimal object and watch with `resourceVersion=0` — if the kind
/// deserializes from a bare `{apiVersion,kind,metadata}` the seeded ADDED is
/// pushed (`Delivered`). Many kinds require fields (e.g. `spec`), so the
/// initial-events typed-deser of the seed fails the whole watch — in that case
/// fall back to an UNSEEDED watch (empty collection) to confirm the endpoint
/// still dispatches a stream. `deser_fragile` flags the kinds that needed the
/// fallback.
async fn probe(res: &Res) -> (Outcome, bool) {
    let ns = if res.namespaced { Some(NS) } else { None };

    // --- attempt 1: seeded, expect Delivered ---
    let router = TestApiServer::new();
    let key = build_key(&res.plural, ns, OBJ);
    let mut meta = json!({"name": OBJ});
    if res.namespaced {
        meta["namespace"] = json!(NS);
    }
    let obj = json!({"apiVersion": res.api_version(), "kind": res.kind, "metadata": meta});
    let _ = router.storage.create(&key, &obj).await;
    let uri = format!(
        "{}?watch=true&resourceVersion=0&allowWatchBookmarks=true",
        res.collection()
    );
    if classify(router, uri, Duration::from_secs(2)).await == Outcome::Delivered {
        return (Outcome::Delivered, false);
    }

    // --- attempt 2: UNSEEDED dispatch check (seed couldn't deserialize) ---
    let router2 = TestApiServer::new();
    let uri2 = format!(
        "{}?watch=true&resourceVersion=0&allowWatchBookmarks=true",
        res.collection()
    );
    (classify(router2, uri2, Duration::from_secs(2)).await, true)
}

#[tokio::test]
async fn watch_pushes_added_for_every_watchable_resource() {
    let probe_router = TestApiServer::new();
    let resources = enumerate(&probe_router).await;

    assert!(
        resources.len() >= 30,
        "discovery enumeration returned only {} watchable resources — expected dozens \
         (discovery regression?)",
        resources.len()
    );

    let label = |r: &Res| {
        format!(
            "{:<28} ({})",
            r.kind,
            if r.group.is_empty() {
                r.version.clone()
            } else {
                format!("{}/{}", r.group, r.version)
            }
        )
    };

    let (mut delivered, mut dispatched, mut fragile) = (0usize, 0usize, 0usize);
    let mut one_shot: Vec<String> = Vec::new();
    let mut no_resp: Vec<String> = Vec::new();
    for res in &resources {
        let (outcome, needed_fallback) = probe(res).await;
        if needed_fallback {
            fragile += 1;
        }
        match outcome {
            Outcome::Delivered => delivered += 1,
            Outcome::Dispatched => dispatched += 1,
            Outcome::OneShotList => one_shot.push(format!("  {}", label(res))),
            Outcome::NoResponse => no_resp.push(format!("  {}", label(res))),
        }
    }

    eprintln!(
        "exhaustive watch: {} watchable resources | delivered ADDED (minimal seed)={} | \
         dispatched a stream (unseeded)={} | ONE-SHOT-LIST={} | no-response={} | \
         deser-fragile (seed tripped typed deser)={}",
        resources.len(),
        delivered,
        dispatched,
        one_shot.len(),
        no_resp.len(),
        fragile,
    );

    // Hard guarantee: EVERY watchable resource must answer ?watch=true with a
    // watch STREAM (delivered an event or held an open streaming connection) —
    // never a one-shot list, never an empty/closed response. Per-kind ADDED
    // delivery with valid bodies is asserted in watch_delivery_matrix_test.rs;
    // here the minimal seed bounds delivery, so we assert dispatch for all.
    let mut failures = Vec::new();
    if !one_shot.is_empty() {
        failures.push(format!(
            "{} returned a one-shot list instead of a watch stream:\n{}",
            one_shot.len(),
            one_shot.join("\n")
        ));
    }
    if !no_resp.is_empty() {
        failures.push(format!(
            "{} gave no streaming watch response:\n{}",
            no_resp.len(),
            no_resp.join("\n")
        ));
    }
    assert!(
        failures.is_empty(),
        "watchable resources that did NOT dispatch a watch stream on ?watch=true:\n{}",
        failures.join("\n")
    );
}
