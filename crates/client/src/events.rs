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
//! failed event must never abort the scheduling/bind decision it describes). It
//! does NOT run the full correlator, but it does reproduce upstream's
//! create-or-aggregate behavior: events key on the stable
//! `(object.reason.uid)` name (see `Event::generate_name`), and on a recurrence
//! the POST collides (`AlreadyExists`). client-go's `recordEvent`
//! (`tools/record/event.go`) handles exactly this — it Creates the first
//! occurrence and **Patches** subsequent ones to bump `count` /
//! `lastTimestamp`. We do the same: POST, and on `AlreadyExists` GET the
//! existing Event, increment `count`, refresh `lastTimestamp`, and PATCH it.
//! Without this, a recurring `(involved, reason)` (e.g. per-cycle
//! `FailedScheduling`) logged a WARN every loop and the Event's `count` stayed
//! pinned at 1 instead of aggregating.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::http::ApiClient;

/// An involved-object reference for [`build_event`]:
/// `(kind, namespace, name, uid)`.
pub type Involved<'a> = (&'a str, &'a str, &'a str, &'a str);

/// Stable, message-independent Event name identical to storage's
/// `Event::generate_name`: `{name}.{reason_lowercase}.{uid_prefix(8)}`. Two
/// emissions for the same `(object, reason, uid)` collide on this name, which is
/// what drives server-side aggregation.
pub fn event_name(name: &str, reason: &str, uid: &str) -> String {
    let uid_prefix = &uid[..8.min(uid.len())];
    format!(
        "{}.{}.{}",
        name,
        reason.to_lowercase(),
        uid_prefix.to_lowercase()
    )
}

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
    let event_name = event_name(name, reason, uid);

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
        let (_, _, name, uid) = involved;
        match self.client.post::<Value, Value>(&path, &body).await {
            Ok(_) => {}
            // Recurrence: the stable name already exists. Aggregate onto it
            // (count++ / lastTimestamp) like client-go's `recordEvent` Patch
            // path, instead of re-logging the same WARN every cycle.
            Err(e) if is_already_exists(&e) => {
                let ev_name = event_name(name, reason, uid);
                if let Err(pe) = self.aggregate_existing(namespace, &ev_name, message).await {
                    tracing::warn!(
                        "failed to aggregate recurring {reason} event for {namespace}/{name}: {pe:#}"
                    );
                }
            }
            Err(e) => {
                tracing::warn!("failed to record {reason} event for {namespace}/{name}: {e:#}");
            }
        }
    }

    /// Bump `count` + `lastTimestamp` on an already-existing aggregated Event,
    /// mirroring upstream's recurrence Patch. GET the current object to read the
    /// live `count` (a blind merge-patch can't increment), then merge-patch the
    /// new values. Retries once on a resourceVersion conflict from a racing
    /// writer.
    async fn aggregate_existing(
        &self,
        namespace: &str,
        name: &str,
        message: &str,
    ) -> anyhow::Result<()> {
        let object_path = format!("/api/v1/namespaces/{}/events/{}", namespace, name);
        const MAX_ATTEMPTS: usize = 3;
        for attempt in 0..MAX_ATTEMPTS {
            let current: Value = self.client.get(&object_path).await?;
            let count = current.get("count").and_then(Value::as_i64).unwrap_or(1);
            let now = chrono::Utc::now().to_rfc3339();
            let patch = json!({
                "count": count + 1,
                "lastTimestamp": now,
                "message": message,
            });
            match self
                .client
                .patch::<Value, Value>(&object_path, &patch, "application/merge-patch+json")
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) if is_conflict(&e) && attempt + 1 < MAX_ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// The api-server reports a name collision as a `Status` with reason
/// `AlreadyExists` (HTTP 409); [`crate::http::ApiClient`] surfaces that verbatim
/// in the error message.
fn is_already_exists(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("AlreadyExists")
}

/// A resourceVersion mismatch on PATCH surfaces as reason `Conflict` (HTTP 409).
fn is_conflict(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("Conflict")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_name_matches_storage_scheme() {
        // {name}.{reason_lowercase}.{uid_prefix(8)} — stable + message-independent.
        assert_eq!(
            event_name("rs-pod1", "FailedScheduling", "abcdef0123456789"),
            "rs-pod1.failedscheduling.abcdef01"
        );
        // Two emissions for the same (object, reason, uid) collide on the name,
        // regardless of message — this is what drives aggregation.
        assert_eq!(
            event_name("p", "Scheduled", "ab"),
            event_name("p", "Scheduled", "ab")
        );
    }

    #[test]
    fn event_name_handles_short_uid() {
        assert_eq!(event_name("p", "Pulled", "ab"), "p.pulled.ab");
    }

    #[test]
    fn classifies_already_exists_and_conflict() {
        // Mirrors `format_status_error`'s rendering of a Status failure.
        let ae = anyhow::anyhow!(
            "Error from server (AlreadyExists): events \"p.scheduled.ab\" already exists"
        );
        assert!(is_already_exists(&ae));
        assert!(!is_conflict(&ae));

        let conflict = anyhow::anyhow!(
            "Error from server (Conflict): Operation cannot be fulfilled on events"
        );
        assert!(is_conflict(&conflict));
        assert!(!is_already_exists(&conflict));

        let other = anyhow::anyhow!("Error from server (NotFound): events not found");
        assert!(!is_already_exists(&other));
        assert!(!is_conflict(&other));
    }
}
