//! The scheduler's data plane: every read/write the scheduling loop performs,
//! behind one [`DataPlane`] enum so the SAME scheduling logic
//! (`scheduler.rs` + `advanced.rs`) runs against either storage directly
//! (all-in-one binary) or the api-server over HTTP (in-cluster static pod).
//!
//! ## Why a seam, not two schedulers
//!
//! `scheduler.rs` had 22 `self.storage.<op>` sites. Duplicating the
//! ~2000-line scheduling algorithm for an API client would be a maintenance
//! hazard, so instead every site dispatches through this thin layer. The
//! classification of the original 22 sites → the method that now serves them is
//! the review artifact in `tests/api_mode_test.rs` ("no raw storage call
//! survives in API mode").
//!
//! ## Storage mode (default; all-in-one)
//!
//! [`DataPlane::Storage`] wraps `Arc<S: Storage>` and is byte-for-byte the old
//! behavior: full-object `update` writes (binding sets `spec.nodeName` in the
//! same write that sets the `PodScheduled` condition), `watch` over
//! `/registry/pods`, etc. The all-in-one binary depends on this.
//!
//! ## API mode (in-cluster static pod)
//!
//! [`DataPlane::Api`] bundles an [`ApiClient`], two [`Reflector`]s (pods +
//! nodes informers) and a [`ClientEventRecorder`]:
//! - reads come from the reflector stores (no per-cycle list round-trip);
//! - the pod "watch" the scheduler subscribes to is the pods reflector's
//!   `subscribe()` broadcast channel;
//! - `bind` POSTs to the binding subresource
//!   (`/api/v1/namespaces/{ns}/pods/{name}/binding`) — `spec.nodeName` is
//!   immutable via a plain PUT, so binding MUST go through the subresource;
//! - other pod mutations (priority backstop, `nominatedNodeName`, the
//!   `Unschedulable` condition, preemption eviction) PUT the whole pod with a
//!   re-GET-on-409 retry, mirroring the storage path's conflict handling;
//! - events POST through [`ClientEventRecorder`].

use std::sync::Arc;

use rusternetes_client::events::ClientEventRecorder;
use rusternetes_client::http::{ApiClient, GetError};
use rusternetes_client::reflector::Reflector;
use rusternetes_common::resources::{EventType, Node, Pod, PriorityClass};
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, Storage};
use serde_json::json;

/// Build the `Binding` body the api-server's `create_binding` handler accepts.
/// The handler only requires `target.name`; the rest mirrors what `kubectl`
/// and client-go send so the wire object is a valid `v1.Binding`.
pub fn binding_body(pod_name: &str, node_name: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": { "name": pod_name },
        "target": {
            "apiVersion": "v1",
            "kind": "Node",
            "name": node_name,
        },
    })
}

/// Read-side key function for the pods reflector store: `pods/{ns}/{name}`,
/// matching the work-queue key shape the scheduler already uses.
fn pod_store_key(p: &Pod) -> String {
    format!(
        "{}/{}",
        p.metadata.namespace.as_deref().unwrap_or("default"),
        p.metadata.name
    )
}

fn node_store_key(n: &Node) -> String {
    n.metadata.name.clone()
}

fn priority_class_store_key(pc: &PriorityClass) -> String {
    pc.metadata.name.clone()
}

/// A reflector keyed by a plain function pointer (no captured state), shared
/// behind an `Arc` so the run loops and the read paths reference the same store.
type PodReflector = Arc<Reflector<Pod, fn(&Pod) -> String>>;
type NodeReflector = Arc<Reflector<Node, fn(&Node) -> String>>;
type PriorityClassReflector = Arc<Reflector<PriorityClass, fn(&PriorityClass) -> String>>;

/// The api-server-backed data plane: client + informers + event recorder.
pub struct ApiBackend {
    pub(crate) client: Arc<ApiClient>,
    pub(crate) pods: PodReflector,
    pub(crate) nodes: NodeReflector,
    /// PriorityClasses are reflector-backed (not per-cycle GETs): under heavy
    /// scheduling load a per-cycle live LIST occasionally came back incomplete,
    /// collapsing pod priorities to 0 and breaking preemption ordering. A
    /// local informer store is always complete and current.
    pub(crate) priority_classes: PriorityClassReflector,
    pub(crate) recorder: ClientEventRecorder,
}

impl ApiBackend {
    /// Wire an API backend from a connected [`ApiClient`]. Builds the pods and
    /// nodes reflectors over `/api/v1/pods` and `/api/v1/nodes`.
    pub fn new(client: Arc<ApiClient>, scheduler_name: &str) -> Self {
        use rusternetes_client::reflector::ApiListWatch;
        let pods_lw = Arc::new(ApiListWatch::new(Arc::clone(&client), "/api/v1/pods"));
        let nodes_lw = Arc::new(ApiListWatch::new(Arc::clone(&client), "/api/v1/nodes"));
        let pc_lw = Arc::new(ApiListWatch::new(
            Arc::clone(&client),
            "/apis/scheduling.k8s.io/v1/priorityclasses",
        ));
        let pods: PodReflector =
            Arc::new(Reflector::new(pods_lw, pod_store_key as fn(&Pod) -> String));
        let nodes: NodeReflector = Arc::new(Reflector::new(
            nodes_lw,
            node_store_key as fn(&Node) -> String,
        ));
        let priority_classes: PriorityClassReflector = Arc::new(Reflector::new(
            pc_lw,
            priority_class_store_key as fn(&PriorityClass) -> String,
        ));
        let recorder = ClientEventRecorder::new(Arc::clone(&client), scheduler_name.to_string());
        Self {
            client,
            pods,
            nodes,
            priority_classes,
            recorder,
        }
    }

    fn pod_path(ns: &str, name: &str) -> String {
        format!("/api/v1/namespaces/{}/pods/{}", ns, name)
    }
}

/// One of the two backends the scheduler can run against. Cheap to share via
/// the `Arc<S>` / `Arc<…>` interiors.
pub enum DataPlane<S: Storage + Send + Sync + 'static> {
    Storage(Arc<S>),
    Api(ApiBackend),
}

impl<S: Storage + Send + Sync + 'static> DataPlane<S> {
    /// List every pod in the cluster. Storage: `list(/registry/pods)`. API:
    /// snapshot of the pods reflector store.
    pub async fn list_pods(&self) -> Result<Vec<Pod>> {
        match self {
            DataPlane::Storage(s) => {
                s.list(&rusternetes_storage::build_prefix("pods", None))
                    .await
            }
            DataPlane::Api(a) => Ok(a.pods.store().items()),
        }
    }

    /// List every node. Storage: `list(/registry/nodes)`. API: nodes reflector
    /// store snapshot.
    pub async fn list_nodes(&self) -> Result<Vec<Node>> {
        match self {
            DataPlane::Storage(s) => {
                s.list(&rusternetes_storage::build_prefix("nodes", None))
                    .await
            }
            DataPlane::Api(a) => Ok(a.nodes.store().items()),
        }
    }

    /// List every PriorityClass. Storage: `list(/registry/priorityclasses)`.
    /// API: GET the collection (priority classes change rarely, so no informer).
    pub async fn list_priority_classes(&self) -> Result<Vec<PriorityClass>> {
        match self {
            DataPlane::Storage(s) => {
                s.list(&rusternetes_storage::build_prefix("priorityclasses", None))
                    .await
            }
            DataPlane::Api(a) => Ok(a.priority_classes.store().items()),
        }
    }

    /// Get one pod by namespace/name. Returns [`Error::NotFound`] when absent.
    pub async fn get_pod(&self, ns: &str, name: &str) -> Result<Pod> {
        match self {
            DataPlane::Storage(s) => s.get(&build_key("pods", Some(ns), name)).await,
            DataPlane::Api(a) => a
                .client
                .get(&ApiBackend::pod_path(ns, name))
                .await
                .map_err(get_err_to_common),
        }
    }

    /// Get one ResourceClaim by namespace/name (DRA device availability check).
    pub async fn get_resource_claim<T>(&self, ns: &str, name: &str) -> Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        match self {
            DataPlane::Storage(s) => s.get(&build_key("resourceclaims", Some(ns), name)).await,
            DataPlane::Api(a) => a
                .client
                .get(&format!(
                    "/apis/resource.k8s.io/v1/namespaces/{}/resourceclaims/{}",
                    ns, name
                ))
                .await
                .map_err(get_err_to_common),
        }
    }

    /// List every ResourceSlice (DRA device topology).
    pub async fn list_resource_slices<T>(&self) -> Result<Vec<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        match self {
            DataPlane::Storage(s) => {
                s.list(&rusternetes_storage::build_prefix("resourceslices", None))
                    .await
            }
            DataPlane::Api(a) => a
                .client
                .get_list("/apis/resource.k8s.io/v1/resourceslices")
                .await
                .map_err(get_err_to_common),
        }
    }

    /// Persist a full pod object. Storage: `update`. API: PUT the pod (used for
    /// the priority backstop, `nominatedNodeName`, the `Unschedulable`
    /// condition, and preemption eviction — NOT for binding, which goes through
    /// [`Self::bind`]). Propagates [`Error::Conflict`] on a 409 so callers keep
    /// their existing re-GET-and-retry behavior.
    pub async fn update_pod(&self, ns: &str, name: &str, pod: &Pod) -> Result<()> {
        match self {
            DataPlane::Storage(s) => {
                s.update(&build_key("pods", Some(ns), name), pod).await?;
                Ok(())
            }
            DataPlane::Api(a) => {
                let _: Pod = a
                    .client
                    .put(&ApiBackend::pod_path(ns, name), pod)
                    .await
                    .map_err(api_err_to_common)?;
                Ok(())
            }
        }
    }

    /// Evict a preemption victim.
    ///
    /// `mutated_pod` is the victim with the DisruptionTarget condition (and, in
    /// storage mode, `deletionTimestamp` + Preempted status) already applied.
    ///
    /// Storage mode writes the whole pod in one shot — the kubelet observes the
    /// `deletionTimestamp` and terminates it. API mode cannot: the api-server
    /// owns `deletionTimestamp` and rejects a PUT that sets it (pod spec/meta is
    /// immutable on update). So API mode stamps the DisruptionTarget condition
    /// via the `/status` subresource (best-effort, for the conformance specs
    /// that observe it) and then issues a real `DELETE` with the grace period —
    /// the only way to actually terminate the victim and free its resources.
    pub async fn evict_pod_for_preemption(
        &self,
        ns: &str,
        name: &str,
        mutated_pod: &Pod,
        grace_period_seconds: i64,
    ) -> Result<()> {
        match self {
            DataPlane::Storage(s) => {
                s.update(&build_key("pods", Some(ns), name), mutated_pod)
                    .await?;
                Ok(())
            }
            DataPlane::Api(a) => {
                // Best-effort DisruptionTarget condition via /status; the DELETE
                // below is what actually frees resources, so a status hiccup
                // must not abort the eviction.
                let _: std::result::Result<Pod, _> = a
                    .client
                    .put(
                        &format!("/api/v1/namespaces/{}/pods/{}/status", ns, name),
                        mutated_pod,
                    )
                    .await;
                let body = json!({ "gracePeriodSeconds": grace_period_seconds });
                a.client
                    .delete_with_options(&ApiBackend::pod_path(ns, name), &[], Some(&body))
                    .await
                    .map_err(|e| Error::Internal(format!("evict DELETE failed: {e}")))?;
                Ok(())
            }
        }
    }

    /// Bind a pod to a node.
    ///
    /// Storage mode preserves the old single-write behavior: the caller passes
    /// the fully-mutated pod (spec.nodeName + PodScheduled condition set) and we
    /// `update` it in one shot.
    ///
    /// API mode POSTs the binding subresource (the only legal way to set the
    /// immutable `spec.nodeName`), then PUTs `/status` to stamp the
    /// `PodScheduled=True` condition that conformance specs observe — the
    /// binding handler itself does not touch status.
    pub async fn bind(&self, ns: &str, pod_with_node: &Pod, node_name: &str) -> Result<()> {
        match self {
            DataPlane::Storage(s) => {
                let key = build_key("pods", Some(ns), &pod_with_node.metadata.name);
                // One re-GET-on-conflict retry, mirroring the original
                // bind_pod_to_node behavior: on a resourceVersion conflict,
                // re-read the pod and re-apply spec.nodeName + the PodScheduled
                // condition (already present on `pod_with_node`).
                match s.update(&key, pod_with_node).await {
                    Ok(_) => Ok(()),
                    Err(Error::Conflict(_)) => {
                        let mut fresh: Pod = s.get(&key).await?;
                        if let Some(spec) = fresh.spec.as_mut() {
                            spec.node_name = Some(node_name.to_string());
                        }
                        if let (Some(fresh_status), Some(src_status)) =
                            (fresh.status.as_mut(), pod_with_node.status.as_ref())
                        {
                            if let Some(sched) = src_status.conditions.as_ref().and_then(|cs| {
                                cs.iter().find(|c| c.condition_type == "PodScheduled")
                            }) {
                                let conditions =
                                    fresh_status.conditions.get_or_insert_with(Vec::new);
                                conditions.retain(|c| c.condition_type != "PodScheduled");
                                conditions.push(sched.clone());
                            }
                        }
                        s.update(&key, &fresh).await?;
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            DataPlane::Api(a) => {
                let name = &pod_with_node.metadata.name;
                let body = binding_body(name, node_name);
                let _: serde_json::Value = a
                    .client
                    .post(
                        &format!("/api/v1/namespaces/{}/pods/{}/binding", ns, name),
                        &body,
                    )
                    .await
                    .map_err(api_err_to_common)?;
                // Stamp the PodScheduled condition via the status subresource so
                // [sig-scheduling] specs that wait on it succeed. Best-effort:
                // the bind already succeeded, so a status hiccup must not fail
                // the schedule.
                let _: std::result::Result<Pod, _> = a
                    .client
                    .put(
                        &format!("/api/v1/namespaces/{}/pods/{}/status", ns, name),
                        pod_with_node,
                    )
                    .await;
                Ok(())
            }
        }
    }

    /// Emit a pod-scoped event. Storage mode routes through the unified
    /// [`EventRecorder`](rusternetes_storage::EventRecorder) (correlator +
    /// dedup); the scheduler owns that recorder, so `Storage` here is a no-op
    /// placeholder — the scheduler calls its own recorder directly in storage
    /// mode and only routes through here in API mode.
    pub async fn emit_event_api(
        &self,
        ns: &str,
        reason: &str,
        message: &str,
        event_type: EventType,
        involved: (&str, &str, &str, &str),
    ) {
        if let DataPlane::Api(a) = self {
            let type_str = match event_type {
                EventType::Normal => "Normal",
                EventType::Warning => "Warning",
            };
            a.recorder
                .event(ns, reason, message, type_str, involved)
                .await;
        }
    }
}

/// Map a client [`GetError`] (which models 404 separately) onto the common
/// [`Error`] the scheduler propagates, preserving NotFound so `get_pod` callers
/// that match on it behave identically to storage mode.
fn get_err_to_common(e: GetError) -> Error {
    match e {
        GetError::NotFound => Error::NotFound("resource not found".to_string()),
        GetError::Other(err) => Error::Internal(err.to_string()),
    }
}

/// Map a generic client `anyhow::Error` onto the common [`Error`]. A 409 from
/// the api-server surfaces in the message as `Error from server (Conflict)`; we
/// translate that to [`Error::Conflict`] so the scheduler's bind/update retry
/// loops trip on it exactly as they do against storage.
fn api_err_to_common(e: anyhow::Error) -> Error {
    let msg = format!("{e:#}");
    if msg.contains("(Conflict)") || msg.contains("(AlreadyExists)") {
        Error::Conflict(msg)
    } else if msg.contains("(NotFound)") {
        Error::NotFound(msg)
    } else {
        Error::Internal(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_body_has_target_name_and_metadata_name() {
        let b = binding_body("web-1", "node-2");
        assert_eq!(b["target"]["name"], "node-2");
        assert_eq!(b["metadata"]["name"], "web-1");
        assert_eq!(b["kind"], "Binding");
    }

    #[test]
    fn api_err_conflict_maps_to_conflict() {
        let e = anyhow::anyhow!("Error from server (Conflict): pod already assigned");
        assert!(matches!(api_err_to_common(e), Error::Conflict(_)));
    }
}
