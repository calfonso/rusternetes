//! Watch-delivery matrix: every watchable kind must stream an event for each
//! mutation a client makes — `ADDED` on create, `MODIFIED` on update,
//! `DELETED` on delete. This is the contract Lens (and every informer-based
//! client) relies on to keep a live view; a kind that silently drops events
//! shows up as a stale/empty pane in the UI.
//!
//! The map says watch is wired generically for all kinds, so in principle this
//! should be uniformly green. It is written as a *matrix that reports every
//! broken kind at once* (rather than one assert per kind) precisely because the
//! field report is "most don't work" — the failure list is the deliverable.
//!
//! Harness: in-process Axum router over `MemoryStorage`, driven through the
//! public HTTP surface via `oneshot`, mirroring `integration_watch_rv_test.rs`.
//! For each kind we open `?watch=true&resourceVersion=0`, then create / update /
//! delete the object and assert the three envelopes arrive (filtered to the
//! object we touched, by `object.metadata.name`).

use axum::http::{Method, StatusCode};
use futures::StreamExt;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const NS: &str = "watchmatrix";
const OBJ: &str = "w1";

async fn send(
    router: &TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    router.send(method.as_str(), uri, content_type, body).await
}

/// Collect up to `max` `\n`-delimited watch envelopes from a watch URI, giving
/// up at `deadline`. Runs as its own task so the caller can mutate concurrently.
async fn collect(router: TestApiServer, uri: String, max: usize, deadline: Duration) -> Vec<Value> {
    let resp = router.respond("GET", &uri, None, None).await;
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
    kind: &'static str,
    /// Collection path with `{ns}` already substituted (no query string).
    collection: String,
    namespaced: bool,
    stub: Value,
}

/// Whether a DELETE must surface a `DELETED` event. False for kinds whose
/// deletion is finalizer-gated and only completes once a controller clears the
/// finalizer — Namespace gets the `kubernetes` finalizer on create and stays
/// Terminating with no controller in this harness (tracked separately as the
/// "namespace deletion stuck Terminating" bug). Those kinds still must emit
/// ADDED + MODIFIED (the MODIFIED is the deletion-timestamp mark).
fn expects_delete_event(kind: &str) -> bool {
    !matches!(kind, "Namespace")
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

/// Drive one kind through watch→create→update→delete; return the list of
/// problems (empty == fully working).
async fn run_case(case: &Case) -> Vec<String> {
    let router = TestApiServer::new();
    let mut problems = Vec::new();

    // Seed the namespace so namespaced creates aren't rejected for a missing ns.
    if case.namespaced {
        let _ = router
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
    let handle = tokio::spawn(collect(watch_router, watch_uri, 3, Duration::from_secs(4)));

    // Give the watch task time to subscribe before we mutate.
    tokio::time::sleep(Duration::from_millis(250)).await;

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
    tokio::time::sleep(Duration::from_millis(60)).await;
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
    tokio::time::sleep(Duration::from_millis(60)).await;
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
    let mut want = vec!["ADDED", "MODIFIED"];
    if expects_delete_event(case.kind) {
        want.push("DELETED");
    }
    for w in want {
        if !got.iter().any(|t| t == w) {
            problems.push(format!("missing {w} event (saw {got:?})"));
        }
    }
    problems
}

fn pod_template() -> Value {
    json!({
        "metadata": {"labels": {"app": "w"}},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    })
}

fn cases() -> Vec<Case> {
    let n = NS;
    let ns_pod_template = pod_template();
    vec![
        Case {
            kind: "Pod",
            collection: format!("/api/v1/namespaces/{n}/pods"),
            namespaced: true,
            stub: json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":OBJ,"namespace":n},
                "spec":{"containers":[{"name":"c","image":"busybox"}]}}),
        },
        Case {
            kind: "ConfigMap",
            collection: format!("/api/v1/namespaces/{n}/configmaps"),
            namespaced: true,
            stub: json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":OBJ,"namespace":n},"data":{"k":"v"}}),
        },
        Case {
            kind: "Secret",
            collection: format!("/api/v1/namespaces/{n}/secrets"),
            namespaced: true,
            stub: json!({"apiVersion":"v1","kind":"Secret","metadata":{"name":OBJ,"namespace":n},"data":{}}),
        },
        Case {
            kind: "Service",
            collection: format!("/api/v1/namespaces/{n}/services"),
            namespaced: true,
            stub: json!({"apiVersion":"v1","kind":"Service","metadata":{"name":OBJ,"namespace":n},
                "spec":{"ports":[{"port":80,"protocol":"TCP"}]}}),
        },
        Case {
            kind: "ServiceAccount",
            collection: format!("/api/v1/namespaces/{n}/serviceaccounts"),
            namespaced: true,
            stub: json!({"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":OBJ,"namespace":n}}),
        },
        Case {
            kind: "Endpoints",
            collection: format!("/api/v1/namespaces/{n}/endpoints"),
            namespaced: true,
            stub: json!({"apiVersion":"v1","kind":"Endpoints","metadata":{"name":OBJ,"namespace":n},
                "subsets":[]}),
        },
        Case {
            kind: "Deployment",
            collection: format!("/apis/apps/v1/namespaces/{n}/deployments"),
            namespaced: true,
            stub: json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":OBJ,"namespace":n},
                "spec":{"replicas":1,"selector":{"matchLabels":{"app":"w"}},"template":ns_pod_template}}),
        },
        Case {
            kind: "ReplicaSet",
            collection: format!("/apis/apps/v1/namespaces/{n}/replicasets"),
            namespaced: true,
            stub: json!({"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":OBJ,"namespace":n},
                "spec":{"replicas":1,"selector":{"matchLabels":{"app":"w"}},"template":pod_template()}}),
        },
        Case {
            kind: "DaemonSet",
            collection: format!("/apis/apps/v1/namespaces/{n}/daemonsets"),
            namespaced: true,
            stub: json!({"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":OBJ,"namespace":n},
                "spec":{"selector":{"matchLabels":{"app":"w"}},"template":pod_template()}}),
        },
        Case {
            kind: "StatefulSet",
            collection: format!("/apis/apps/v1/namespaces/{n}/statefulsets"),
            namespaced: true,
            stub: json!({"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":OBJ,"namespace":n},
                "spec":{"serviceName":"w","replicas":1,"selector":{"matchLabels":{"app":"w"}},"template":pod_template()}}),
        },
        Case {
            kind: "Job",
            collection: format!("/apis/batch/v1/namespaces/{n}/jobs"),
            namespaced: true,
            stub: json!({"apiVersion":"batch/v1","kind":"Job","metadata":{"name":OBJ,"namespace":n},
                "spec":{"template":{"metadata":{"labels":{"app":"w"}},
                    "spec":{"restartPolicy":"Never","containers":[{"name":"c","image":"busybox"}]}}}}),
        },
        Case {
            kind: "EndpointSlice",
            collection: format!("/apis/discovery.k8s.io/v1/namespaces/{n}/endpointslices"),
            namespaced: true,
            stub: json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice",
                "metadata":{"name":OBJ,"namespace":n},"addressType":"IPv4","endpoints":[]}),
        },
        Case {
            kind: "NetworkPolicy",
            collection: format!("/apis/networking.k8s.io/v1/namespaces/{n}/networkpolicies"),
            namespaced: true,
            stub: json!({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy",
                "metadata":{"name":OBJ,"namespace":n},"spec":{"podSelector":{}}}),
        },
        Case {
            kind: "Ingress",
            collection: format!("/apis/networking.k8s.io/v1/namespaces/{n}/ingresses"),
            namespaced: true,
            stub: json!({"apiVersion":"networking.k8s.io/v1","kind":"Ingress",
                "metadata":{"name":OBJ,"namespace":n},
                "spec":{"defaultBackend":{"service":{"name":"svc","port":{"number":80}}}}}),
        },
        // ---- cluster-scoped ----
        Case {
            kind: "Namespace",
            collection: "/api/v1/namespaces".to_string(),
            namespaced: false,
            stub: json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":OBJ}}),
        },
        Case {
            kind: "Node",
            collection: "/api/v1/nodes".to_string(),
            namespaced: false,
            stub: json!({"apiVersion":"v1","kind":"Node","metadata":{"name":OBJ},"spec":{}}),
        },
        Case {
            kind: "PersistentVolume",
            collection: "/api/v1/persistentvolumes".to_string(),
            namespaced: false,
            stub: json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":OBJ},
                "spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],"hostPath":{"path":"/tmp/w"}}}),
        },
        Case {
            kind: "ClusterRole",
            collection: "/apis/rbac.authorization.k8s.io/v1/clusterroles".to_string(),
            namespaced: false,
            stub: json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole",
                "metadata":{"name":OBJ},"rules":[]}),
        },
        Case {
            kind: "ClusterRoleBinding",
            collection: "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings".to_string(),
            namespaced: false,
            stub: json!({"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRoleBinding",
                "metadata":{"name":OBJ},
                "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"w"},
                "subjects":[]}),
        },
        Case {
            kind: "CSIDriver",
            collection: "/apis/storage.k8s.io/v1/csidrivers".to_string(),
            namespaced: false,
            stub: json!({"apiVersion":"storage.k8s.io/v1","kind":"CSIDriver",
                "metadata":{"name":OBJ},"spec":{}}),
        },
        Case {
            kind: "VolumeAttachment",
            collection: "/apis/storage.k8s.io/v1/volumeattachments".to_string(),
            namespaced: false,
            stub: json!({"apiVersion":"storage.k8s.io/v1","kind":"VolumeAttachment",
                "metadata":{"name":OBJ},
                "spec":{"attacher":"a","nodeName":"node-1","source":{"persistentVolumeName":"pv"}}}),
        },
        Case {
            kind: "ServiceCIDR",
            collection: "/apis/networking.k8s.io/v1/servicecidrs".to_string(),
            namespaced: false,
            stub: json!({"apiVersion":"networking.k8s.io/v1","kind":"ServiceCIDR",
                "metadata":{"name":OBJ},"spec":{"cidrs":["10.96.0.0/24"]}}),
        },
    ]
}

/// The whole matrix in one test: drive every kind, then assert none are broken.
/// The failure message is the per-kind report the field bug ("most don't work")
/// calls for.
#[tokio::test]
async fn watch_delivers_added_modified_deleted_for_every_kind() {
    let cases = cases();
    let mut broken: Vec<String> = Vec::new();
    let mut ok = 0usize;

    for c in &cases {
        let problems = run_case(c).await;
        if problems.is_empty() {
            ok += 1;
        } else {
            broken.push(format!("  {:<16} -> {}", c.kind, problems.join("; ")));
        }
    }

    eprintln!(
        "watch-delivery matrix: {}/{} kinds fully working",
        ok,
        cases.len()
    );
    assert!(
        broken.is_empty(),
        "{} of {} kinds do not deliver the full ADDED/MODIFIED/DELETED watch \
         lifecycle:\n{}",
        broken.len(),
        cases.len(),
        broken.join("\n")
    );
}
