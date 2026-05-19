//! Read-only HTTP routes served by the kubelet on its API port.
//!
//! Mirrors upstream `pkg/kubelet/server/server.go`. Today this only
//! contains the `/pods` endpoint, which the node-proxy conformance tests
//! poll to observe `metadata.deletionTimestamp` propagation. Other
//! upstream routes (`/runningpods`, `/stats`, `/healthz`) can be layered
//! on later without changing the data-assembly function.
//!
//! The kubelet does not maintain its own in-memory `PodManager`: the
//! storage backend already holds the apiserver's authoritative view, and
//! both the sync loop and watch subscribe to the same source. Reading
//! from storage here therefore returns the same picture `sync_pod` is
//! about to act on.

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use rusternetes_common::resources::Pod;
use rusternetes_common::types::List;
use rusternetes_common::Result;
use rusternetes_storage::{build_prefix, Storage, StorageBackend};
use std::sync::Arc;

/// State injected into the kubelet's read-only HTTP routes.
#[derive(Clone)]
pub struct ServerState {
    pub node_name: String,
    pub storage: Arc<StorageBackend>,
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

async fn handle_list_pods(State(state): State<ServerState>) -> impl IntoResponse {
    match pods_for_node(&state.storage, &state.node_name).await {
        Ok(list) => Json(list).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Build the router fragment containing the read-only kubelet routes.
/// Callers (`main.rs`, `lib.rs::run`) `.merge()` it into their own
/// router so routes that need additional state (metrics, configz, exec)
/// can be wired alongside.
pub fn read_only_router(state: ServerState) -> Router {
    Router::new()
        .route("/pods", get(handle_list_pods))
        .with_state(state)
}
