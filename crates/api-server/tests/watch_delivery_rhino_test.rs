//! Watch-delivery contract against the REAL rhino/SQLite storage backend.
//!
//! The sibling matrix tests (`watch_delivery_matrix_test.rs`,
//! `watch_delivery_exhaustive_test.rs`) run only against `MemoryStorage`. But
//! the original watch field-delivery bug (PR #885: create must surface `ADDED`,
//! not `MODIFIED`) manifested on the real **rhino** backend — `MemoryStorage`
//! never reproduced it. This test exercises the watch lifecycle
//! (`ADDED` → `MODIFIED` → `DELETED`) over `RhinoStorage<SqliteBackend>` so a
//! backend-specific regression in the rhino watch translation is caught.
//!
//! The whole file is gated behind the `sqlite` feature so the default
//! (no-feature) build/CI compiles it out entirely. A plain `cargo test` /
//! nextest WITHOUT `--features sqlite` will NOT run these cases — see the
//! module note in the PR / report for wiring a sqlite CI job.
//!
//! Harness mirrors `watch_delivery_matrix_test.rs`: an in-process Axum router
//! over the chosen backend, driven through the public HTTP surface via
//! `oneshot`. Each case opens `?watch=true&resourceVersion=0`, then
//! create / update / delete the object and asserts the three envelopes arrive
//! (filtered to the object we touched by `object.metadata.name`).
#![cfg(feature = "sqlite")]

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use futures::StreamExt;
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{build_key, RhinoStorage, Storage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tower::ServiceExt;

const NS: &str = "watchrhino";
const OBJ: &str = "w1";

/// Fresh on-disk SQLite DB path per test, unique by uuid, under the temp dir.
fn fresh_db_path() -> String {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "rusternetes-watch-rhino-{}.db",
        uuid::Uuid::new_v4()
    ));
    p.to_string_lossy().into_owned()
}

/// Build an `ApiServerState` over a freshly created rhino/SQLite backend.
/// Returns the state plus the db path so the caller can clean it up.
async fn make_sqlite_state() -> (Arc<ApiServerState>, String) {
    let db_path = fresh_db_path();
    let store = RhinoStorage::new(&db_path)
        .await
        .expect("create rhino SQLite backend");
    let backend = Arc::new(StorageBackend::Sqlite(store));
    let state = Arc::new(ApiServerState::new(
        backend,
        Arc::new(TokenManager::new(b"test-secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        true, // skip_auth
    ));
    (state, db_path)
}

async fn send(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let req = if let Some(b) = body {
        req = req.header("content-type", "application/json");
        req.body(Body::from(serde_json::to_vec(b).unwrap()))
            .unwrap()
    } else {
        req.body(Body::empty()).unwrap()
    };
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, v)
}

/// Collect up to `max` `\n`-delimited watch envelopes from a watch URI, giving
/// up at `deadline`. Runs as its own task so the caller can mutate concurrently.
async fn collect(router: axum::Router, uri: String, max: usize, deadline: Duration) -> Vec<Value> {
    let req = Request::builder()
        .method(Method::GET)
        .uri(&uri)
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
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

struct Case {
    /// Collection path with `{ns}` already substituted (no query string).
    collection: String,
    namespaced: bool,
    stub: Value,
}

/// Event types observed for our object name, in arrival order.
fn types_for_obj(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .filter(|e| {
            e.pointer("/object/metadata/name").and_then(|v| v.as_str()) == Some(OBJ)
                // CREATE/DELETE on cluster-scoped have no namespace; namespaced match NS.
                && e.pointer("/object/metadata/namespace")
                    .and_then(|v| v.as_str())
                    .map(|n| n == NS)
                    .unwrap_or(true)
        })
        .filter_map(|e| e.get("type").and_then(|v| v.as_str()).map(String::from))
        .collect()
}

/// Drive one kind through watch→create→update→delete over a real rhino/SQLite
/// backend; return the list of problems (empty == full lifecycle delivered).
async fn run_case(case: &Case) -> Vec<String> {
    let (state, db_path) = make_sqlite_state().await;
    let router = build_router(state.clone(), None);
    let mut problems = Vec::new();

    // Seed the namespace so namespaced creates aren't rejected for a missing ns.
    if case.namespaced {
        let _ = state
            .storage
            .create(
                &build_key("namespaces", None, NS),
                &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":NS}}),
            )
            .await;
    }

    // Open the watch and start collecting (ADDED, MODIFIED, DELETED = 3).
    let watch_uri = format!("{}?watch=true&resourceVersion=0", case.collection);
    let watch_router = router.clone();
    let handle = tokio::spawn(collect(watch_router, watch_uri, 3, Duration::from_secs(6)));

    // Give the watch task time to subscribe before we mutate.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let item = format!("{}/{}", case.collection, OBJ);

    // CREATE
    let (cs, created) = send(&router, Method::POST, &case.collection, Some(&case.stub)).await;
    if !cs.is_success() {
        problems.push(format!(
            "create failed: {} {}",
            cs,
            created
                .pointer("/message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
        ));
    }

    // UPDATE — re-fetch then PUT with a new label so optimistic concurrency
    // (if enforced) sees the current resourceVersion.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let (_, mut current) = send(&router, Method::GET, &item, None).await;
    if current.is_object() {
        current["metadata"]["labels"] = json!({"watch-test": "updated"});
        let (us, ub) = send(&router, Method::PUT, &item, Some(&current)).await;
        if !us.is_success() {
            problems.push(format!(
                "update failed: {} {}",
                us,
                ub.pointer("/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            ));
        }
    }

    // DELETE — force grace period 0 so kinds with graceful deletion (pods)
    // are removed immediately rather than only marked, yielding a real DELETED.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let del_uri = format!("{item}?gracePeriodSeconds=0");
    let (ds, db) = send(&router, Method::DELETE, &del_uri, None).await;
    if !ds.is_success() {
        problems.push(format!(
            "delete failed: {} {}",
            ds,
            db.pointer("/message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
        ));
    }

    let events = handle.await.unwrap();
    let got = types_for_obj(&events);
    for w in ["ADDED", "MODIFIED", "DELETED"] {
        if !got.iter().any(|t| t == w) {
            problems.push(format!("missing {w} event (saw {got:?})"));
        }
    }

    // Clean up the temp DB file (best-effort).
    let _ = std::fs::remove_file(&db_path);

    problems
}

fn pod_template() -> Value {
    json!({
        "metadata": {"labels": {"app": "w"}},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    })
}

/// Representative kinds: several namespaced (ConfigMap, Deployment, Pod, Secret)
/// plus one cluster-scoped (ClusterRole).
fn cases() -> Vec<(&'static str, Case)> {
    let n = NS;
    vec![
        (
            "ConfigMap",
            Case {
                collection: format!("/api/v1/namespaces/{n}/configmaps"),
                namespaced: true,
                stub: json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":OBJ,"namespace":n},"data":{"k":"v"}}),
            },
        ),
        (
            "Secret",
            Case {
                collection: format!("/api/v1/namespaces/{n}/secrets"),
                namespaced: true,
                stub: json!({"apiVersion":"v1","kind":"Secret","metadata":{"name":OBJ,"namespace":n},"data":{}}),
            },
        ),
        (
            "Pod",
            Case {
                collection: format!("/api/v1/namespaces/{n}/pods"),
                namespaced: true,
                stub: json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":OBJ,"namespace":n},
                    "spec":{"containers":[{"name":"c","image":"busybox"}]}}),
            },
        ),
        (
            "Deployment",
            Case {
                collection: format!("/apis/apps/v1/namespaces/{n}/deployments"),
                namespaced: true,
                stub: json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":OBJ,"namespace":n},
                    "spec":{"replicas":1,"selector":{"matchLabels":{"app":"w"}},"template":pod_template()}}),
            },
        ),
        (
            "ClusterRole",
            Case {
                collection: "/apis/rbac.authorization.k8s.io/v1/clusterroles".to_string(),
                namespaced: false,
                stub: json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole",
                    "metadata":{"name":OBJ},"rules":[]}),
            },
        ),
    ]
}

/// Drive every representative kind through the full watch lifecycle against the
/// real rhino/SQLite backend; assert none drop an event. The failure message is
/// a per-kind report so a single run pinpoints any broken backend translation.
#[tokio::test]
async fn rhino_sqlite_watch_delivers_added_modified_deleted() {
    let cases = cases();
    let mut broken: Vec<String> = Vec::new();
    let mut ok = 0usize;

    for (kind, c) in &cases {
        let problems = run_case(c).await;
        if problems.is_empty() {
            ok += 1;
        } else {
            broken.push(format!("  {:<16} -> {}", kind, problems.join("; ")));
        }
    }

    eprintln!(
        "rhino/SQLite watch-delivery: {}/{} kinds fully working",
        ok,
        cases.len()
    );
    assert!(
        broken.is_empty(),
        "{} of {} kinds do not deliver the full ADDED/MODIFIED/DELETED watch \
         lifecycle on the rhino/SQLite backend:\n{}",
        broken.len(),
        cases.len(),
        broken.join("\n")
    );
}
