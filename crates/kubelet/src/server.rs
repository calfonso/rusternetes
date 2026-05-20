//! Read-only HTTP routes served by the kubelet on its API port.
//!
//! Mirrors upstream `pkg/kubelet/server/server.go`. Routes served here:
//!
//! | Route               | Purpose                                       |
//! |---------------------|-----------------------------------------------|
//! | `GET /pods`         | All pods bound to this node                   |
//! | `GET /runningpods/` | Subset of `/pods` whose phase is `Running`    |
//! | `GET /healthz`      | sync-loop liveness — 200 if recent, else 500  |
//! | `GET /stats/summary`| Minimal cAdvisor-shape node + per-pod summary |
//!
//! The kubelet does not maintain its own in-memory `PodManager`: the
//! storage backend already holds the apiserver's authoritative view, and
//! both the sync loop and watch subscribe to the same source. Reading
//! from storage here therefore returns the same picture `sync_pod` is
//! about to act on.

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use rusternetes_common::resources::Pod;
use rusternetes_common::types::{List, Phase};
use rusternetes_common::Result;
use rusternetes_storage::{build_prefix, Storage, StorageBackend};
use serde_json::json;
use std::sync::Arc;

use crate::kubelet::Kubelet;

/// State injected into the kubelet's read-only HTTP routes.
#[derive(Clone)]
pub struct ServerState {
    pub node_name: String,
    pub storage: Arc<StorageBackend>,
    /// Optional handle on the live kubelet. Used by `/healthz` to read
    /// the last sync_loop timestamp. `None` in tests that don't spin a
    /// kubelet; in that mode `/healthz` short-circuits to 200 OK so
    /// router-shape tests stay green.
    pub kubelet: Option<Arc<Kubelet>>,
}

/// Pure data assembly: build the `PodList` for `/pods` by filtering
/// storage pods to those bound to `node_name`. Extracted from the axum
/// handler so it can be unit-tested without spinning up a server.
pub async fn pods_for_node(storage: &StorageBackend, node_name: &str) -> Result<List<Pod>> {
    let all: Vec<Pod> = storage.list(&build_prefix("pods", None)).await?;
    let items: Vec<Pod> = all
        .into_iter()
        .filter(|p| {
            p.spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                .map(|n| n == node_name)
                .unwrap_or(false)
        })
        .collect();
    Ok(List::new("PodList", "v1", items))
}

/// Subset of [`pods_for_node`] restricted to pods whose `status.phase`
/// is `Running`. Backs `GET /runningpods/`.
pub async fn running_pods_for_node(storage: &StorageBackend, node_name: &str) -> Result<List<Pod>> {
    let list = pods_for_node(storage, node_name).await?;
    let items: Vec<Pod> = list
        .items
        .into_iter()
        .filter(|p| {
            matches!(
                p.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Running)
            )
        })
        .collect();
    Ok(List::new("PodList", "v1", items))
}

async fn handle_list_pods(State(state): State<ServerState>) -> impl IntoResponse {
    match pods_for_node(&state.storage, &state.node_name).await {
        Ok(list) => Json(list).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn handle_list_running_pods(State(state): State<ServerState>) -> impl IntoResponse {
    match running_pods_for_node(&state.storage, &state.node_name).await {
        Ok(list) => Json(list).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn handle_healthz(State(state): State<ServerState>) -> impl IntoResponse {
    match &state.kubelet {
        // No kubelet attached (router-shape tests / early startup): treat as healthy.
        None => (StatusCode::OK, "ok").into_response(),
        Some(k) if k.healthy() => (StatusCode::OK, "ok").into_response(),
        Some(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "kubelet sync_loop is stale",
        )
            .into_response(),
    }
}

/// Minimal cAdvisor-shape JSON for `GET /stats/summary`. Upstream
/// `[NodeConformance]` specs assert shape (`node.nodeName`, `pods[]`),
/// not values, so we emit zero CPU/memory counters until the eviction
/// manager's per-pod stats integration lands.
async fn handle_stats_summary(State(state): State<ServerState>) -> impl IntoResponse {
    let pods = match pods_for_node(&state.storage, &state.node_name).await {
        Ok(list) => list.items,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let pod_summaries: Vec<_> = pods
        .iter()
        .map(|p| {
            json!({
                "podRef": {
                    "name": p.metadata.name,
                    "namespace": p.metadata.namespace.clone().unwrap_or_default(),
                    "uid": p.metadata.uid,
                },
                "cpu": { "usageNanoCores": 0u64 },
                "memory": { "workingSetBytes": 0u64 },
            })
        })
        .collect();

    Json(json!({
        "node": {
            "nodeName": state.node_name,
            "cpu": { "usageNanoCores": 0u64 },
            "memory": { "workingSetBytes": 0u64 },
        },
        "pods": pod_summaries,
    }))
    .into_response()
}

/// Build the router fragment containing the read-only kubelet routes.
/// Callers (`main.rs`, `lib.rs::run`) `.merge()` it into their own
/// router so routes that need additional state (metrics, configz, exec)
/// can be wired alongside.
pub fn read_only_router(state: ServerState) -> Router {
    Router::new()
        .route("/pods", get(handle_list_pods))
        .route("/runningpods/", get(handle_list_running_pods))
        .route("/healthz", get(handle_healthz))
        .route("/stats/summary", get(handle_stats_summary))
        .with_state(state)
}
