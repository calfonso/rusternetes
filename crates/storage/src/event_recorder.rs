//! A unified event recorder shared by every component that emits Kubernetes
//! events (controller-manager, scheduler, kubelet).
//!
//! Before this existed, each component wrote `Event` objects straight to storage
//! with its own ad-hoc de-duplication. That meant the spam-filter and
//! aggregation stages of [`EventCorrelator`] (ported in
//! `crates/common/src/event_correlator.rs`) were never exercised at emit time —
//! only the count/de-dup branch was, and only inside the controller-manager's
//! `EventsController`.
//!
//! [`EventRecorder`] routes every emission through the correlator before it
//! touches storage, mirroring upstream's `recordToSink`:
//!
//! 1. **Correlate** — run the event through spam-filter → aggregate → count.
//!    If the spam-filter trips, the event is dropped (`skip`).
//! 2. **Persist** — for a fresh `(involvedObject, reason)` create a new `Event`;
//!    for a recurrence bump `count` / `lastTimestamp` / `series` on the stored
//!    object.
//!
//! Storage remains the authority on `count` (it is read back and bumped), so a
//! process restart — which empties the correlator's in-memory logger cache —
//! never regresses a stored count. The correlator contributes the two behaviours
//! storage alone cannot: dropping flooding events (`skip`) and rewriting an
//! over-threshold message to the aggregate "(combined from similar events)"
//! form.

use std::sync::{Arc, Mutex};

use rusternetes_common::event_correlator::{
    CorrelatorEvent, CorrelatorEventSource, CorrelatorEventType, CorrelatorObjectReference,
    EventCorrelator, RealClock,
};
use rusternetes_common::resources::{Event, EventSeries, EventSource, EventType, ObjectReference};
use rusternetes_common::Result;

use crate::Storage;

/// Records Kubernetes events on behalf of a component, routing each emission
/// through a shared [`EventCorrelator`] before writing to `storage`.
///
/// Cheap to `clone` for sharing across tasks — the correlator state lives behind
/// an `Arc<Mutex<..>>` so all clones share one spam-filter / aggregation
/// timeline (matching upstream where one broadcaster fronts one correlator).
pub struct EventRecorder<S: Storage + ?Sized> {
    storage: Arc<S>,
    /// The correlator is locked only for the synchronous `event_correlate`
    /// call; the guard is always dropped before any `await`, so no storage I/O
    /// ever happens while holding it.
    correlator: Arc<Mutex<EventCorrelator<RealClock>>>,
}

impl<S: Storage + ?Sized> Clone for EventRecorder<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            correlator: Arc::clone(&self.correlator),
        }
    }
}

impl<S: Storage + ?Sized> EventRecorder<S> {
    /// Build a recorder backed by `storage` with the upstream-default
    /// correlator (burst 25, qps 1/300, aggregate after 10 in 600s).
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            correlator: Arc::new(Mutex::new(EventCorrelator::new(RealClock))),
        }
    }

    /// Emit an event about `involved` from `source`.
    ///
    /// Returns `Ok(())` both when the event is persisted and when the
    /// spam-filter drops it — a dropped event is normal back-pressure, not an
    /// error. Storage failures propagate.
    pub async fn event(
        &self,
        involved: &ObjectReference,
        source: &EventSource,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) -> Result<()> {
        let correlator_event =
            build_correlator_event(involved, source, &event_type, reason, message);

        // Lock only for the synchronous correlate call; drop before any await.
        let result = {
            let mut correlator = self
                .correlator
                .lock()
                .expect("event correlator mutex poisoned");
            correlator.event_correlate(&correlator_event)
        };

        if result.skip {
            // Spam-filter back-pressure: drop silently, as upstream does.
            return Ok(());
        }

        // The correlated event carries the (possibly aggregate-rewritten)
        // reason + message. `reason` is never rewritten by aggregation, but
        // `message` may collapse to the "(combined from similar events)" form
        // once the per-reason flood crosses the aggregation threshold.
        let correlated = result
            .event
            .expect("a non-skipped correlation always yields an event");

        self.persist(involved, source, event_type, &correlated)
            .await
    }

    /// Create a fresh `Event`, or bump the existing one for this
    /// `(involvedObject, reason)`.
    async fn persist(
        &self,
        involved: &ObjectReference,
        source: &EventSource,
        event_type: EventType,
        correlated: &CorrelatorEvent,
    ) -> Result<()> {
        let namespace = involved.namespace.as_deref().unwrap_or("default");
        // Stable, message-independent name (object.reason.uid) — identical to
        // the key the controller-manager already uses, so emissions from any
        // component de-duplicate against the same stored object.
        let name = Event::generate_name(involved, &correlated.reason);
        let key = format!("/registry/events/{}/{}", namespace, name);

        if let Ok(mut existing) = self.storage.get::<Event>(&key).await {
            // Recurrence: bump count. Storage is the authority, so we take
            // `max(stored + 1, correlator_count)` — this never regresses a
            // count the correlator's freshly-restarted logger has forgotten.
            let now = correlated.last_timestamp;
            let bumped = existing.count.saturating_add(1).max(correlated.count);
            existing.count = bumped;
            existing.last_timestamp = Some(now);
            existing.message = correlated.message.clone();
            existing.series = Some(EventSeries {
                count: bumped,
                last_observed_time: now,
            });
            self.storage.update(&key, &existing).await?;
            return Ok(());
        }

        // First occurrence: create.
        let mut event = Event::new(
            name,
            namespace.to_string(),
            involved.clone(),
            correlated.reason.clone(),
            correlated.message.clone(),
            event_type,
        );
        event.source = source.clone();
        event.count = correlated.count.max(1);
        event.first_timestamp = Some(correlated.first_timestamp);
        event.last_timestamp = Some(correlated.last_timestamp);
        self.storage.create(&key, &event).await?;
        Ok(())
    }
}

/// Translate the wire `(involved, source, type, reason, message)` tuple into the
/// correlator's minimal event model.
fn build_correlator_event(
    involved: &ObjectReference,
    source: &EventSource,
    event_type: &EventType,
    reason: &str,
    message: &str,
) -> CorrelatorEvent {
    let now = chrono::Utc::now();
    CorrelatorEvent {
        name: String::new(),
        namespace: involved.namespace.clone().unwrap_or_default(),
        resource_version: String::new(),
        reason: reason.to_string(),
        message: message.to_string(),
        involved_object: CorrelatorObjectReference {
            kind: involved.kind.clone().unwrap_or_default(),
            namespace: involved.namespace.clone().unwrap_or_default(),
            name: involved.name.clone().unwrap_or_default(),
            field_path: involved.field_path.clone().unwrap_or_default(),
            uid: involved.uid.clone().unwrap_or_default(),
            api_version: involved.api_version.clone().unwrap_or_default(),
        },
        source: CorrelatorEventSource {
            component: source.component.clone(),
            host: source.host.clone().unwrap_or_default(),
        },
        event_type: match event_type {
            EventType::Normal => CorrelatorEventType::Normal,
            EventType::Warning => CorrelatorEventType::Warning,
        },
        reporting_controller: source.component.clone(),
        reporting_instance: source.host.clone().unwrap_or_default(),
        count: 1,
        first_timestamp: now,
        last_timestamp: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorage;

    fn obj_ref(name: &str, namespace: &str) -> ObjectReference {
        ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some(namespace.to_string()),
            name: Some(name.to_string()),
            uid: Some(format!("{}-uid-0000", name)),
            api_version: Some("v1".to_string()),
            ..Default::default()
        }
    }

    fn source() -> EventSource {
        EventSource {
            component: "kubelet".to_string(),
            host: Some("node-1".to_string()),
        }
    }

    async fn stored(storage: &MemoryStorage, ns: &str, name: &str, reason: &str) -> Option<Event> {
        let obj = obj_ref(name, ns);
        let event_name = Event::generate_name(&obj, reason);
        let key = format!("/registry/events/{}/{}", ns, event_name);
        storage.get::<Event>(&key).await.ok()
    }

    #[tokio::test]
    async fn first_emission_creates_event_with_source() {
        let storage = Arc::new(MemoryStorage::new());
        let recorder = EventRecorder::new(Arc::clone(&storage));

        recorder
            .event(
                &obj_ref("web", "default"),
                &source(),
                EventType::Normal,
                "Started",
                "Started container web",
            )
            .await
            .unwrap();

        let ev = stored(&storage, "default", "web", "Started")
            .await
            .expect("event should be created");
        assert_eq!(ev.reason, "Started");
        assert_eq!(ev.message, "Started container web");
        assert_eq!(ev.count, 1);
        assert_eq!(ev.source.component, "kubelet");
        assert_eq!(ev.source.host.as_deref(), Some("node-1"));
    }

    #[tokio::test]
    async fn recurrence_bumps_count_and_series() {
        let storage = Arc::new(MemoryStorage::new());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let obj = obj_ref("web", "default");

        recorder
            .event(&obj, &source(), EventType::Normal, "Pulled", "pulled v1")
            .await
            .unwrap();
        recorder
            .event(&obj, &source(), EventType::Normal, "Pulled", "pulled v2")
            .await
            .unwrap();

        let ev = stored(&storage, "default", "web", "Pulled").await.unwrap();
        assert_eq!(ev.count, 2, "second occurrence bumps count");
        assert_eq!(ev.message, "pulled v2", "message advances to latest");
        let series = ev.series.expect("series should track count");
        assert_eq!(series.count, 2);
    }

    #[tokio::test]
    async fn spam_filter_caps_a_flood_at_the_burst_size() {
        // 40 identical emissions in a tight loop. The spam key excludes reason
        // and message, so all share one token bucket: burst is 25, refill is
        // 1/300s (negligible in <1s), so exactly the first 25 are accepted and
        // the rest are dropped. Since all 25 share one (object, reason) name,
        // they collapse onto a single Event whose count equals the burst.
        let storage = Arc::new(MemoryStorage::new());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let obj = obj_ref("noisy", "default");

        for i in 0..40 {
            recorder
                .event(
                    &obj,
                    &source(),
                    EventType::Warning,
                    "BackOff",
                    &format!("back-off restarting #{i}"),
                )
                .await
                .unwrap();
        }

        let ev = stored(&storage, "default", "noisy", "BackOff")
            .await
            .unwrap();
        assert_eq!(
            ev.count, 25,
            "spam filter must cap the flood at burst=25, got {}",
            ev.count
        );
    }
}
