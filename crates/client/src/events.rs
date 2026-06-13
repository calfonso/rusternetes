//! Client-side event recorder.
//!
//! The storage-backed [`EventRecorder`](../../storage/src/event_recorder.rs)
//! routes every emission through the shared `EventCorrelator` before writing an
//! `Event` object straight to storage. A component that talks to the api-server
//! over HTTP (no direct storage handle) cannot do that — it must POST the
//! `Event` to `/api/v1/namespaces/{ns}/events` like upstream's
//! `EventSinkImpl.Create`.
//!
//! This module mirrors the storage recorder's **field set** and its
//! **best-effort error policy** (emit failures are logged, never propagated — a
//! failed event must never abort the scheduling/bind decision it describes), but
//! does NOT run the correlator: the api-server is the single writer and already
//! deduplicates on the stable `(object.reason.uid)` name (see
//! `Event::generate_name`), so a fresh POST for a recurring `(involved, reason)`
//! collapses onto the same object server-side. This matches how the storage
//! recorder keys events; we reproduce the same name scheme here so emissions
//! from an API-mode component dedup against any other source.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::http::ApiClient;

/// An involved-object reference for [`build_event`]:
/// `(kind, namespace, name, uid)`.
pub type Involved<'a> = (&'a str, &'a str, &'a str, &'a str);

/// Build a `v1` `Event` object (as a [`serde_json::Value`]) ready to POST to
/// `/api/v1/namespaces/{ns}/events`.
///
/// The field set mirrors `crates/storage/src/event_recorder.rs`: a stable,
/// message-independent `metadata.name` (`{object}.{reason}.{uidPrefix}`) so the
/// api-server deduplicates recurrences, `count: 1`, matching
/// `firstTimestamp`/`lastTimestamp`, and `source.component` set to the
/// reporting component.
pub fn build_event(
    namespace: &str,
    reason: &str,
    message: &str,
    event_type: &str,
    involved: Involved<'_>,
    component: &str,
) -> Value {
    let (kind, obj_ns, name, uid) = involved;
    let now = chrono::Utc::now().to_rfc3339();
    // Stable name identical to storage's `Event::generate_name`:
    // `{name}.{reason_lowercase}.{uid_prefix(8)}`.
    let uid_prefix = &uid[..8.min(uid.len())];
    let event_name = format!(
        "{}.{}.{}",
        name,
        reason.to_lowercase(),
        uid_prefix.to_lowercase()
    );

    json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": event_name,
            "namespace": namespace,
        },
        "involvedObject": {
            "apiVersion": "v1",
            "kind": kind,
            "namespace": obj_ns,
            "name": name,
            "uid": uid,
        },
        "reason": reason,
        "message": message,
        "type": event_type,
        "source": {
            "component": component,
        },
        "firstTimestamp": now,
        "lastTimestamp": now,
        "count": 1,
    })
}

/// Records Kubernetes events on behalf of an API-mode component by POSTing them
/// to the api-server. Cheap to clone (shares the `Arc<ApiClient>`).
#[derive(Clone)]
pub struct ClientEventRecorder {
    client: Arc<ApiClient>,
    /// `source.component` / `reportingComponent` — e.g. `default-scheduler`.
    component: String,
}

impl ClientEventRecorder {
    pub fn new(client: Arc<ApiClient>, component: impl Into<String>) -> Self {
        Self {
            client,
            component: component.into(),
        }
    }

    /// Emit an event about `involved`. Best-effort: a POST failure is logged at
    /// `warn` and swallowed — events must never abort the decision they
    /// describe (same policy as the storage recorder).
    pub async fn event(
        &self,
        namespace: &str,
        reason: &str,
        message: &str,
        event_type: &str,
        involved: Involved<'_>,
    ) {
        let body = build_event(
            namespace,
            reason,
            message,
            event_type,
            involved,
            &self.component,
        );
        let path = format!("/api/v1/namespaces/{}/events", namespace);
        // The api-server returns the created (or, on a recurrence, the existing)
        // Event; we don't need the decoded body, only success/failure.
        if let Err(e) = self.client.post::<Value, Value>(&path, &body).await {
            let (_, _, name, _) = involved;
            tracing::warn!("failed to record {reason} event for {namespace}/{name}: {e:#}");
        }
    }
}
