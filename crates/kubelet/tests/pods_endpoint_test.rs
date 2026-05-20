//! Regression tests for the kubelet `/pods` endpoint.
//!
//! Upstream conformance test `[sig-node] Pods Extended Delete Grace Period
//! should be submitted and removed` polls
//! `GET /api/v1/nodes/<node>/proxy/pods` via the api-server's node proxy and
//! expects to see `metadata.deletionTimestamp` populated on a pod within
//! 3× its grace period. The api-server forwards that to the kubelet's
//! `/pods` endpoint (`pkg/kubelet/server/server.go#getPods` in upstream).
//!
//! These tests exercise the pure data assembly function that backs that
//! endpoint — the actual axum route is a thin wrapper.

use chrono::{TimeZone, Utc};
use rusternetes_common::resources::{Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kubelet::server::pods_for_node;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::sync::Arc;

fn pod_on_node(name: &str, node: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some(node.to_string()),
            ..Default::default()
        }),
        status: None,
    }
}

async fn seed(storage: &StorageBackend, pod: &Pod) {
    let key = build_key(
        "pods",
        pod.metadata.namespace.as_deref(),
        &pod.metadata.name,
    );
    storage.create(&key, pod).await.expect("seed pod");
}

#[tokio::test]
async fn returns_podlist_typemeta() {
    let storage = Arc::new(StorageBackend::new_memory());
    let list = pods_for_node(&storage, "node-1").await.expect("list");
    assert_eq!(list.kind, "PodList");
    assert_eq!(list.api_version, "v1");
    assert!(list.items.is_empty());
}

#[tokio::test]
async fn only_returns_pods_bound_to_this_node() {
    let storage = Arc::new(StorageBackend::new_memory());
    seed(&storage, &pod_on_node("on-me", "node-1")).await;
    seed(&storage, &pod_on_node("other-node", "node-2")).await;
    let unbound = Pod {
        spec: Some(PodSpec {
            node_name: None,
            ..Default::default()
        }),
        ..pod_on_node("unscheduled", "")
    };
    seed(&storage, &unbound).await;

    let list = pods_for_node(&storage, "node-1").await.expect("list");
    let names: Vec<&str> = list
        .items
        .iter()
        .map(|p| p.metadata.name.as_str())
        .collect();
    assert_eq!(names, vec!["on-me"]);
}

#[tokio::test]
async fn reflects_deletion_timestamp_set_in_storage() {
    let storage = Arc::new(StorageBackend::new_memory());
    let mut pod = pod_on_node("terminating", "node-1");
    pod.metadata.deletion_timestamp = Some(Utc.with_ymd_and_hms(2026, 5, 19, 12, 0, 0).unwrap());
    seed(&storage, &pod).await;

    let list = pods_for_node(&storage, "node-1").await.expect("list");
    assert_eq!(list.items.len(), 1);
    assert!(
        list.items[0].metadata.deletion_timestamp.is_some(),
        "deletionTimestamp from storage must be visible in /pods response"
    );
}
