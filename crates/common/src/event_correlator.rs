//! Event correlation: aggregation, de-duplication/counting, and spam filtering.
//!
//! This is a faithful port of upstream Kubernetes'
//! `staging/src/k8s.io/client-go/tools/record/events_cache.go` (release-1.35).
//!
//! The correlator processes incoming events and performs analysis to avoid
//! overwhelming the system:
//!
//! - **Aggregation**: similar events (differing only by message) seen more than
//!   `maxEvents` (10) times within `maxIntervalInSeconds` (600s) are combined
//!   into a single synthetic "(combined from similar events): ..." event.
//! - **De-duplication / counting**: the *exact* same event (including message)
//!   seen multiple times is compacted into one event with an increasing
//!   `count` and an updated `lastTimestamp`, plus a JSON merge patch describing
//!   the delta to send to the server.
//! - **Spam filtering**: a token-bucket per source+object limits the burst of
//!   events about a single object (`burst` = 25, refill `1/300s`).
//!
//! The logic is pure: there is no storage or IO. A [`PassiveClock`] trait is
//! injected so the time-windowed behaviour (aggregation interval, token
//! refill) is deterministically testable, mirroring upstream's
//! `testclocks.SimpleIntervalClock`.

use chrono::{DateTime, Utc};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Maximum entries kept in the LRU caches. Matches upstream `maxLruCacheEntries`.
pub const MAX_LRU_CACHE_ENTRIES: usize = 4096;

/// If we see the same event (varying only by message) more than this many
/// times within [`DEFAULT_AGGREGATE_INTERVAL_IN_SECONDS`], aggregate it.
/// Matches upstream `defaultAggregateMaxEvents`.
pub const DEFAULT_AGGREGATE_MAX_EVENTS: usize = 10;

/// The rolling interval (seconds) over which aggregation is evaluated.
/// Matches upstream `defaultAggregateIntervalInSeconds`.
pub const DEFAULT_AGGREGATE_INTERVAL_IN_SECONDS: u64 = 600;

/// By default, allow a source to send this many events about an object as a
/// burst. Matches upstream `defaultSpamBurst`.
pub const DEFAULT_SPAM_BURST: usize = 25;

/// Token bucket refill rate (queries per second). 1 new event every 300s.
/// Matches upstream `defaultSpamQPS` (`1. / 300.`).
pub const DEFAULT_SPAM_QPS: f64 = 1.0 / 300.0;

/// A passive clock returning "now" — injectable so time windows are testable.
///
/// Mirrors upstream's `clock.PassiveClock`.
pub trait PassiveClock {
    fn now(&self) -> DateTime<Utc>;
}

/// A clock that advances by a fixed interval on every call to [`PassiveClock::now`].
///
/// Faithful port of `k8s.io/utils/clock/testing.SimpleIntervalClock`: the
/// returned time is incremented by `duration` after each read.
///
/// The state is shared (`Rc<Cell<..>>`) so that cloning the clock — as the
/// [`EventCorrelator`] does to give each sub-component its own handle — still
/// observes a single, monotonically advancing timeline. This mirrors upstream
/// where the test passes `&clock` (a pointer) to every component.
#[derive(Clone)]
pub struct SimpleIntervalClock {
    time: Rc<Cell<DateTime<Utc>>>,
    duration: chrono::Duration,
}

impl SimpleIntervalClock {
    pub fn new(start: DateTime<Utc>, duration: chrono::Duration) -> Self {
        Self {
            time: Rc::new(Cell::new(start)),
            duration,
        }
    }
}

impl PassiveClock for SimpleIntervalClock {
    fn now(&self) -> DateTime<Utc> {
        let current = self.time.get();
        self.time.set(current + self.duration);
        current
    }
}

/// A clock that always returns the same fixed time. Useful for assertions that
/// do not depend on the passage of time.
#[derive(Clone)]
pub struct FixedClock {
    time: DateTime<Utc>,
}

impl FixedClock {
    pub fn new(time: DateTime<Utc>) -> Self {
        Self { time }
    }
}

impl PassiveClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.time
    }
}

/// Event type discriminator. Mirrors `v1.EventTypeNormal` / `v1.EventTypeWarning`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CorrelatorEventType {
    #[default]
    Normal,
    Warning,
}

impl CorrelatorEventType {
    fn as_str(&self) -> &'static str {
        match self {
            CorrelatorEventType::Normal => "Normal",
            CorrelatorEventType::Warning => "Warning",
        }
    }
}

/// Source of an event (component + host). Mirrors `v1.EventSource`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorrelatorEventSource {
    pub component: String,
    pub host: String,
}

/// Reference to the object an event is about. Mirrors the subset of
/// `v1.ObjectReference` used by the correlator key functions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorrelatorObjectReference {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub field_path: String,
    pub uid: String,
    pub api_version: String,
}

/// A minimal event model mirroring `v1.Event`, holding exactly the fields the
/// correlator reads or writes. This keeps the ported logic and tests a
/// faithful 1:1 translation of upstream, independent of the richer
/// [`crate::resources::Event`] wire type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelatorEvent {
    pub name: String,
    pub namespace: String,
    pub resource_version: String,
    pub reason: String,
    pub message: String,
    pub involved_object: CorrelatorObjectReference,
    pub source: CorrelatorEventSource,
    pub event_type: CorrelatorEventType,
    pub reporting_controller: String,
    pub reporting_instance: String,
    pub count: i32,
    pub first_timestamp: DateTime<Utc>,
    pub last_timestamp: DateTime<Utc>,
}

impl CorrelatorEvent {
    /// Builds the full event key based on source, involvedObject, type, reason
    /// and message. Mirrors upstream `getEventKey`.
    fn event_key(&self) -> String {
        [
            self.source.component.as_str(),
            self.source.host.as_str(),
            self.involved_object.kind.as_str(),
            self.involved_object.namespace.as_str(),
            self.involved_object.name.as_str(),
            self.involved_object.field_path.as_str(),
            self.involved_object.uid.as_str(),
            self.involved_object.api_version.as_str(),
            self.event_type.as_str(),
            self.reason.as_str(),
            self.message.as_str(),
        ]
        .concat()
    }
}

/// Builds a unique spam key based on source + involvedObject + type. Mirrors
/// upstream `getSpamKey` (message excluded).
pub fn get_spam_key(event: &CorrelatorEvent) -> String {
    [
        event.source.component.as_str(),
        event.source.host.as_str(),
        event.involved_object.kind.as_str(),
        event.involved_object.namespace.as_str(),
        event.involved_object.name.as_str(),
        event.involved_object.uid.as_str(),
        event.involved_object.api_version.as_str(),
        event.event_type.as_str(),
    ]
    .concat()
}

/// Function that produces a unique spam key for an event.
pub type EventSpamKeyFunc = fn(&CorrelatorEvent) -> String;

/// Aggregates events by exact match on source, involvedObject, type, reason,
/// reportingController and reportingInstance — message is EXCLUDED. Returns
/// `(aggregateKey, localKey)` where `localKey` is the message. Mirrors
/// upstream `EventAggregatorByReasonFunc`.
pub fn event_aggregator_by_reason_func(event: &CorrelatorEvent) -> (String, String) {
    let aggregate_key = [
        event.source.component.as_str(),
        event.source.host.as_str(),
        event.involved_object.kind.as_str(),
        event.involved_object.namespace.as_str(),
        event.involved_object.name.as_str(),
        event.involved_object.uid.as_str(),
        event.involved_object.api_version.as_str(),
        event.event_type.as_str(),
        event.reason.as_str(),
        event.reporting_controller.as_str(),
        event.reporting_instance.as_str(),
    ]
    .concat();
    (aggregate_key, event.message.clone())
}

/// Produces the aggregate message by prefixing the incoming message. Mirrors
/// upstream `EventAggregatorByReasonMessageFunc`.
pub fn event_aggregator_by_reason_message_func(event: &CorrelatorEvent) -> String {
    format!("(combined from similar events): {}", event.message)
}

/// Function that groups events for aggregation.
pub type EventAggregatorKeyFunc = fn(&CorrelatorEvent) -> (String, String);

/// Function that produces an aggregation message.
pub type EventAggregatorMessageFunc = fn(&CorrelatorEvent) -> String;

/// Tracks aggregation state for an aggregate key.
#[derive(Clone, Default)]
struct AggregateRecord {
    local_keys: HashSet<String>,
    last_timestamp: Option<DateTime<Utc>>,
}

/// Identifies similar events and aggregates them into a single event.
///
/// Faithful port of upstream `EventAggregator`.
pub struct EventAggregator<C: PassiveClock> {
    cache: HashMap<String, AggregateRecord>,
    key_func: EventAggregatorKeyFunc,
    message_func: EventAggregatorMessageFunc,
    max_events: usize,
    max_interval_in_seconds: u64,
    clock: C,
}

impl<C: PassiveClock> EventAggregator<C> {
    pub fn new(
        key_func: EventAggregatorKeyFunc,
        message_func: EventAggregatorMessageFunc,
        max_events: usize,
        max_interval_in_seconds: u64,
        clock: C,
    ) -> Self {
        Self {
            cache: HashMap::new(),
            key_func,
            message_func,
            max_events,
            max_interval_in_seconds,
            clock,
        }
    }

    /// Checks if a similar event has been seen according to the aggregation
    /// configuration and returns the (possibly synthetic) event to create plus
    /// the cache key for correlation. Mirrors upstream `EventAggregate`.
    pub fn event_aggregate(&mut self, new_event: &CorrelatorEvent) -> (CorrelatorEvent, String) {
        let now = self.clock.now();
        // eventKey is the full cache key for this event.
        let event_key = new_event.event_key();
        // aggregateKey is for the aggregate event, if one is needed.
        let (aggregate_key, local_key) = (self.key_func)(new_event);

        let mut record = self.cache.get(&aggregate_key).cloned().unwrap_or_default();

        // Is the previous record too old? If so, make a fresh one. If we didn't
        // find a record, its last_timestamp is None and we treat it as new.
        let max_interval = chrono::Duration::seconds(self.max_interval_in_seconds as i64);
        let too_old = match record.last_timestamp {
            Some(ts) => (now - ts) > max_interval,
            None => true,
        };
        if too_old {
            record = AggregateRecord::default();
        }

        // Write the new event into the aggregation record and cache it.
        record.local_keys.insert(local_key);
        record.last_timestamp = Some(now);
        self.cache.insert(aggregate_key.clone(), record.clone());

        // Not yet over the threshold for unique events — don't correlate.
        if record.local_keys.len() < self.max_events {
            return (new_event.clone(), event_key);
        }

        // Do not grow the local key set larger than max: drop one entry, then
        // persist the trimmed record so the cache size stays bounded.
        if let Some(any) = record.local_keys.iter().next().cloned() {
            record.local_keys.remove(&any);
        }
        self.cache.insert(aggregate_key.clone(), record);

        // Create a synthetic aggregate event, returning the aggregateKey as the
        // cache key (so it can be overwritten on subsequent observations).
        let event_copy = CorrelatorEvent {
            name: format!(
                "{}.{:x}",
                new_event.involved_object.name,
                now.timestamp_nanos_opt().unwrap_or(0)
            ),
            namespace: new_event.namespace.clone(),
            resource_version: String::new(),
            count: 1,
            first_timestamp: now,
            last_timestamp: now,
            involved_object: new_event.involved_object.clone(),
            message: (self.message_func)(new_event),
            event_type: new_event.event_type.clone(),
            reason: new_event.reason.clone(),
            source: new_event.source.clone(),
            reporting_controller: new_event.reporting_controller.clone(),
            reporting_instance: new_event.reporting_instance.clone(),
        };
        (event_copy, aggregate_key)
    }
}

/// Records data about when an event was observed. Mirrors upstream `eventLog`.
#[derive(Clone, Default)]
struct EventLog {
    count: u32,
    first_timestamp: Option<DateTime<Utc>>,
    name: String,
    resource_version: String,
}

/// Logs occurrences of an event and produces count-bump patches on repeats.
///
/// Faithful port of upstream `eventLogger`.
pub struct EventLogger<C: PassiveClock> {
    cache: HashMap<String, EventLog>,
    #[allow(dead_code)]
    clock: C,
}

impl<C: PassiveClock> EventLogger<C> {
    pub fn new(clock: C) -> Self {
        Self {
            cache: HashMap::new(),
            clock,
        }
    }

    /// Records an event, or updates an existing one if `key` is a cache hit.
    /// Returns the (possibly count-incremented) event and an optional JSON
    /// merge patch describing the delta. Mirrors upstream `eventObserve`.
    pub fn event_observe(
        &mut self,
        new_event: &CorrelatorEvent,
        key: &str,
    ) -> (CorrelatorEvent, Option<Vec<u8>>) {
        let mut event = new_event.clone();
        let mut patch: Option<Vec<u8>> = None;

        let last_observation = self.cache.get(key).cloned().unwrap_or_default();

        // If we found a prior observation, prepare a patch.
        if last_observation.count > 0 {
            event.name = last_observation.name.clone();
            event.resource_version = last_observation.resource_version.clone();
            if let Some(ts) = last_observation.first_timestamp {
                event.first_timestamp = ts;
            }
            event.count = last_observation.count as i32 + 1;

            // JSON merge patch describing the count / lastTimestamp / RV delta.
            // (Upstream emits a strategic-merge patch; for our single-document
            // Event the equivalent JSON merge patch carries the same fields.)
            let patch_json = serde_json::json!({
                "count": event.count,
                "lastTimestamp": event.last_timestamp.to_rfc3339(),
                "metadata": { "resourceVersion": event.resource_version },
            });
            patch = Some(patch_json.to_string().into_bytes());
        }

        // Record our new observation.
        self.cache.insert(
            key.to_string(),
            EventLog {
                count: event.count.max(0) as u32,
                first_timestamp: Some(event.first_timestamp),
                name: event.name.clone(),
                resource_version: event.resource_version.clone(),
            },
        );

        (event, patch)
    }

    /// Updates internal tracking based on the latest server state. Mirrors
    /// upstream `updateState`.
    pub fn update_state(&mut self, event: &CorrelatorEvent) {
        let key = event.event_key();
        self.cache.insert(
            key,
            EventLog {
                count: event.count.max(0) as u32,
                first_timestamp: Some(event.first_timestamp),
                name: event.name.clone(),
                resource_version: event.resource_version.clone(),
            },
        );
    }
}

/// A token-bucket rate limiter faithful to `golang.org/x/time/rate.Limiter` as
/// driven by `flowcontrol.NewTokenBucketPassiveRateLimiterWithClock`.
///
/// Tokens start full at `burst`, refill at `qps` tokens/second capped at
/// `burst`. `try_accept(now)` succeeds iff at least one token is available at
/// `now`, consuming it.
#[derive(Clone)]
struct TokenBucket {
    qps: f64,
    burst: f64,
    tokens: f64,
    // The instant the bucket state was last advanced. `None` means "never",
    // which upstream models with the zero time (so the first call always sees
    // the bucket as full).
    last: Option<DateTime<Utc>>,
}

impl TokenBucket {
    fn new(qps: f64, burst: usize) -> Self {
        Self {
            qps,
            burst: burst as f64,
            tokens: burst as f64,
            last: None,
        }
    }

    /// Returns the number of tokens available at `t` accounting for refill,
    /// capped at `burst`. Mirrors `rate.Limiter.advance`.
    fn advance(&self, t: DateTime<Utc>) -> f64 {
        let last = match self.last {
            Some(l) if l <= t => l,
            Some(_) => t, // t before last: clamp
            None => t,    // first call: elapsed measured from t == 0 refill
        };
        let elapsed = (t - last).num_microseconds().unwrap_or(0) as f64 / 1_000_000.0;
        let delta = elapsed * self.qps;
        (self.tokens + delta).min(self.burst)
    }

    /// Attempts to consume one token at time `t`. Mirrors `AllowN(t, 1)`.
    fn try_accept(&mut self, t: DateTime<Utc>) -> bool {
        let tokens = self.advance(t) - 1.0;
        // ok iff no wait is required (tokens non-negative) — burst >= 1 always
        // holds for our configs.
        let ok = tokens >= 0.0;
        if ok {
            self.last = Some(t);
            self.tokens = tokens;
        }
        ok
    }
}

/// Holds the rate limiter used for spam decisions about one source+object.
#[derive(Clone)]
struct SpamRecord {
    rate_limiter: TokenBucket,
}

/// Throttles the amount of events a source+object can produce.
///
/// Faithful port of upstream `EventSourceObjectSpamFilter`.
pub struct EventSourceObjectSpamFilter<C: PassiveClock> {
    cache: HashMap<String, SpamRecord>,
    burst: usize,
    qps: f64,
    clock: C,
    spam_key_func: EventSpamKeyFunc,
}

impl<C: PassiveClock> EventSourceObjectSpamFilter<C> {
    pub fn new(burst: usize, qps: f64, clock: C, spam_key_func: EventSpamKeyFunc) -> Self {
        Self {
            cache: HashMap::new(),
            burst,
            qps,
            clock,
            spam_key_func,
        }
    }

    /// Returns `true` if the event should be DROPPED (rate exceeded), `false`
    /// if it should be allowed. Mirrors upstream `Filter`.
    pub fn filter(&mut self, event: &CorrelatorEvent) -> bool {
        let event_key = (self.spam_key_func)(event);
        let mut record = self.cache.get(&event_key).cloned().unwrap_or(SpamRecord {
            rate_limiter: TokenBucket::new(self.qps, self.burst),
        });

        // True == drop (no token available).
        let filtered = !record.rate_limiter.try_accept(self.clock.now());

        self.cache.insert(event_key, record.clone());
        let _ = &mut record;
        filtered
    }
}

/// The result of an [`EventCorrelator::event_correlate`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCorrelateResult {
    /// The event after correlation (None if skipped).
    pub event: Option<CorrelatorEvent>,
    /// If present, a patch to apply when updating the server record.
    pub patch: Option<Vec<u8>>,
    /// If true, do no further processing of the event.
    pub skip: bool,
}

/// Options to configure an [`EventCorrelator`]. Mirrors upstream
/// `CorrelatorOptions`; unset (`None`/zero) fields fall back to defaults.
pub struct CorrelatorOptions<C: PassiveClock> {
    pub clock: C,
    pub burst_size: Option<usize>,
    pub qps: Option<f64>,
    pub key_func: Option<EventAggregatorKeyFunc>,
    pub message_func: Option<EventAggregatorMessageFunc>,
    pub max_events: Option<usize>,
    pub max_interval_in_seconds: Option<u64>,
    pub spam_key_func: Option<EventSpamKeyFunc>,
}

/// Processes incoming events: filter → aggregate → count/de-duplicate.
///
/// Faithful port of upstream `EventCorrelator`. The three stages run in the
/// same order as upstream `EventCorrelate`.
pub struct EventCorrelator<C: PassiveClock + Clone> {
    spam_filter: EventSourceObjectSpamFilter<C>,
    aggregator: EventAggregator<C>,
    logger: EventLogger<C>,
}

impl<C: PassiveClock + Clone> EventCorrelator<C> {
    /// Builds a correlator with the production defaults (burst 25, qps 1/300,
    /// aggregate after 10 in 600s). Mirrors upstream `NewEventCorrelator`.
    pub fn new(clock: C) -> Self {
        Self::with_options(CorrelatorOptions {
            clock,
            burst_size: None,
            qps: None,
            key_func: None,
            message_func: None,
            max_events: None,
            max_interval_in_seconds: None,
            spam_key_func: None,
        })
    }

    /// Builds a correlator from explicit options, applying upstream defaults to
    /// any unset field. Mirrors upstream `NewEventCorrelatorWithOptions`.
    pub fn with_options(options: CorrelatorOptions<C>) -> Self {
        let burst = options.burst_size.unwrap_or(DEFAULT_SPAM_BURST);
        let qps = options.qps.unwrap_or(DEFAULT_SPAM_QPS);
        let key_func = options.key_func.unwrap_or(event_aggregator_by_reason_func);
        let message_func = options
            .message_func
            .unwrap_or(event_aggregator_by_reason_message_func);
        let max_events = options.max_events.unwrap_or(DEFAULT_AGGREGATE_MAX_EVENTS);
        let max_interval = options
            .max_interval_in_seconds
            .unwrap_or(DEFAULT_AGGREGATE_INTERVAL_IN_SECONDS);
        let spam_key_func = options.spam_key_func.unwrap_or(get_spam_key);

        Self {
            spam_filter: EventSourceObjectSpamFilter::new(
                burst,
                qps,
                options.clock.clone(),
                spam_key_func,
            ),
            aggregator: EventAggregator::new(
                key_func,
                message_func,
                max_events,
                max_interval,
                options.clock.clone(),
            ),
            logger: EventLogger::new(options.clock),
        }
    }

    /// Filters, aggregates, counts and de-duplicates an incoming event.
    /// Mirrors upstream `EventCorrelate`.
    pub fn event_correlate(&mut self, new_event: &CorrelatorEvent) -> EventCorrelateResult {
        let (aggregate_event, ckey) = self.aggregator.event_aggregate(new_event);
        let (observed_event, patch) = self.logger.event_observe(&aggregate_event, &ckey);
        if self.spam_filter.filter(&observed_event) {
            return EventCorrelateResult {
                event: None,
                patch: None,
                skip: true,
            };
        }
        EventCorrelateResult {
            event: Some(observed_event),
            patch,
            skip: false,
        }
    }

    /// Updates internal logger state from the latest observed server state.
    /// Mirrors upstream `UpdateState`.
    pub fn update_state(&mut self, event: &CorrelatorEvent) {
        self.logger.update_state(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_object_reference(kind: &str, name: &str, namespace: &str) -> CorrelatorObjectReference {
        CorrelatorObjectReference {
            kind: kind.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
            uid: "C934D34AFB20242".to_string(),
            api_version: "version".to_string(),
            field_path: "spec.containers{mycontainer}".to_string(),
        }
    }

    fn make_event(
        reason: &str,
        message: &str,
        involved_object: CorrelatorObjectReference,
    ) -> CorrelatorEvent {
        let now = Utc::now();
        CorrelatorEvent {
            name: String::new(),
            namespace: String::new(),
            resource_version: String::new(),
            reason: reason.to_string(),
            message: message.to_string(),
            involved_object,
            source: CorrelatorEventSource {
                component: "kubelet".to_string(),
                host: "kublet.node1".to_string(),
            },
            event_type: CorrelatorEventType::Normal,
            reporting_controller: String::new(),
            reporting_instance: String::new(),
            count: 1,
            first_timestamp: now,
            last_timestamp: now,
        }
    }

    fn make_events(num: usize, template: &CorrelatorEvent) -> Vec<CorrelatorEvent> {
        (0..num).map(|_| template.clone()).collect()
    }

    fn make_unique_events(num: usize) -> Vec<CorrelatorEvent> {
        let kind = "Pod";
        (0..num)
            .map(|i| {
                let reason = format!("reason-{i}");
                let message = format!("message-{i}");
                let name = format!("pod-{i}");
                let namespace = format!("ns-{i}");
                make_event(
                    &reason,
                    &message,
                    make_object_reference(kind, &name, &namespace),
                )
            })
            .collect()
    }

    fn make_similar_events(
        num: usize,
        template: &CorrelatorEvent,
        message_prefix: &str,
    ) -> Vec<CorrelatorEvent> {
        let mut events = make_events(num, template);
        for (i, ev) in events.iter_mut().enumerate() {
            ev.message = format!("{}-{}-{}", message_prefix, i, ev.message);
        }
        events
    }

    fn set_count(mut event: CorrelatorEvent, count: i32) -> CorrelatorEvent {
        event.count = count;
        event
    }

    /// Faithful port of upstream `validateEvent`: checks timestamps reflect
    /// compression, name prefix, and field-by-field equality (ignoring the
    /// timestamps that we don't control exactly).
    fn validate_event(
        message_prefix: &str,
        actual_event: &CorrelatorEvent,
        expected_event: &CorrelatorEvent,
    ) {
        let mut recv_event = actual_event.clone();
        let expect_compression = expected_event.count > 1;

        let actual_first = recv_event.first_timestamp;
        let actual_last = recv_event.last_timestamp;
        if actual_first == actual_last {
            if expect_compression {
                panic!(
                    "{message_prefix} - FirstTimestamp ({actual_first}) and LastTimestamp \
                     ({actual_last}) must differ to indicate compression, but were the same"
                );
            }
        } else if expected_event.count == 1 {
            panic!(
                "{message_prefix} - FirstTimestamp ({actual_first}) and LastTimestamp \
                 ({actual_last}) must be equal for a single occurrence, but differed"
            );
        }

        // Temp clear timestamps for comparison (actual values don't matter).
        recv_event.first_timestamp = expected_event.first_timestamp;
        recv_event.last_timestamp = expected_event.last_timestamp;

        // reportingController is copied from expected (upstream does the same).
        recv_event.reporting_controller = expected_event.reporting_controller.clone();

        // Name must carry the expected prefix.
        assert!(
            recv_event.name.starts_with(&expected_event.name),
            "{message_prefix} - Name '{}' does not contain prefix '{}'",
            recv_event.name,
            expected_event.name
        );
        recv_event.name = expected_event.name.clone();

        // resource_version is server-assigned; not compared (always empty in
        // these pure-logic scenarios on both sides).
        assert_eq!(
            expected_event, &recv_event,
            "{message_prefix} - events differ"
        );
    }

    // Port of TestEventAggregatorByReasonFunc.
    #[test]
    fn test_event_aggregator_by_reason_func() {
        let event1 = make_event(
            "end-of-world",
            "it was fun",
            make_object_reference("Pod", "pod1", "other"),
        );
        let event2 = make_event(
            "end-of-world",
            "it was awful",
            make_object_reference("Pod", "pod1", "other"),
        );
        let event3 = make_event(
            "nevermind",
            "it was a bug",
            make_object_reference("Pod", "pod1", "other"),
        );

        let (agg_key1, local_key1) = event_aggregator_by_reason_func(&event1);
        let (agg_key2, local_key2) = event_aggregator_by_reason_func(&event2);
        let (agg_key3, _) = event_aggregator_by_reason_func(&event3);

        assert_eq!(agg_key1, agg_key2, "Expected {agg_key1} equal {agg_key2}");
        assert_ne!(
            local_key1, local_key2,
            "Expected local keys to differ for different messages"
        );
        assert_ne!(
            agg_key1, agg_key3,
            "Expected aggregate keys to differ for different reasons"
        );
    }

    // Port of TestEventAggregatorByReasonMessageFunc.
    #[test]
    fn test_event_aggregator_by_reason_message_func() {
        let expected_prefix = "(combined from similar events): ";
        let event1 = make_event(
            "end-of-world",
            "it was fun",
            make_object_reference("Pod", "pod1", "other"),
        );
        let actual = event_aggregator_by_reason_message_func(&event1);
        assert!(
            actual.starts_with(expected_prefix),
            "Expected {actual} to begin with prefix {expected_prefix}"
        );
    }

    struct CorrelatorScenario {
        previous_events: Vec<CorrelatorEvent>,
        new_event: CorrelatorEvent,
        expected_event: Option<CorrelatorEvent>,
        interval_seconds: i64,
        expected_skip: bool,
    }

    // Port of TestEventCorrelator (all 8 scenarios).
    #[test]
    fn test_event_correlator() {
        let first_event = make_event(
            "first",
            "i am first",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let duplicate_event = make_event(
            "duplicate",
            "me again",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let unique_event = make_event(
            "unique",
            "snowflake",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let mut similar_event = make_event(
            "similar",
            "similar message",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        similar_event.involved_object.field_path = "spec.containers{container1}".to_string();
        let aggregate_event = make_event(
            &similar_event.reason,
            &event_aggregator_by_reason_message_func(&similar_event),
            similar_event.involved_object.clone(),
        );
        let mut similar_but_different_container_event = similar_event.clone();
        similar_but_different_container_event
            .involved_object
            .field_path = "spec.containers{container2}".to_string();

        let scenarios: Vec<(&str, CorrelatorScenario)> = vec![
            (
                "create-a-single-event",
                CorrelatorScenario {
                    previous_events: vec![],
                    new_event: first_event.clone(),
                    expected_event: Some(set_count(first_event.clone(), 1)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "the-same-event-should-just-count",
                CorrelatorScenario {
                    previous_events: make_events(1, &duplicate_event),
                    new_event: duplicate_event.clone(),
                    expected_event: Some(set_count(duplicate_event.clone(), 2)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "the-same-event-should-just-count-even-if-more-than-aggregate",
                CorrelatorScenario {
                    previous_events: make_events(DEFAULT_AGGREGATE_MAX_EVENTS, &duplicate_event),
                    new_event: duplicate_event.clone(),
                    expected_event: Some(set_count(
                        duplicate_event.clone(),
                        DEFAULT_AGGREGATE_MAX_EVENTS as i32 + 1,
                    )),
                    interval_seconds: 30, // larger interval induces aggregation but not spam.
                    expected_skip: false,
                },
            ),
            (
                "the-same-event-is-spam-if-happens-too-frequently",
                CorrelatorScenario {
                    previous_events: make_events(DEFAULT_SPAM_BURST + 1, &duplicate_event),
                    new_event: duplicate_event.clone(),
                    expected_event: None,
                    interval_seconds: 1,
                    expected_skip: true,
                },
            ),
            (
                "create-many-unique-events",
                CorrelatorScenario {
                    previous_events: make_unique_events(30),
                    new_event: unique_event.clone(),
                    expected_event: Some(set_count(unique_event.clone(), 1)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "similar-events-should-aggregate-event",
                CorrelatorScenario {
                    previous_events: make_similar_events(
                        DEFAULT_AGGREGATE_MAX_EVENTS - 1,
                        &similar_event,
                        &similar_event.message,
                    ),
                    new_event: similar_event.clone(),
                    expected_event: Some(set_count(aggregate_event.clone(), 1)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "similar-events-many-times-should-count-the-aggregate",
                CorrelatorScenario {
                    previous_events: make_similar_events(
                        DEFAULT_AGGREGATE_MAX_EVENTS,
                        &similar_event,
                        &similar_event.message,
                    ),
                    new_event: similar_event.clone(),
                    expected_event: Some(set_count(aggregate_event.clone(), 2)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "events-from-different-containers-do-not-aggregate",
                CorrelatorScenario {
                    previous_events: make_events(1, &similar_but_different_container_event),
                    new_event: similar_event.clone(),
                    expected_event: Some(set_count(similar_event.clone(), 1)),
                    interval_seconds: 5,
                    expected_skip: false,
                },
            ),
            (
                "similar-events-whose-interval-is-greater-than-aggregate-interval-do-not-aggregate",
                CorrelatorScenario {
                    previous_events: make_similar_events(
                        DEFAULT_AGGREGATE_MAX_EVENTS - 1,
                        &similar_event,
                        &similar_event.message,
                    ),
                    new_event: similar_event.clone(),
                    expected_event: Some(set_count(similar_event.clone(), 1)),
                    interval_seconds: DEFAULT_AGGREGATE_INTERVAL_IN_SECONDS as i64,
                    expected_skip: false,
                },
            ),
        ];

        for (name, input) in scenarios {
            let event_interval = chrono::Duration::seconds(input.interval_seconds);
            // Shared clock handle: the correlator clones it internally, and the
            // test reads it directly to stamp events — mirroring upstream's
            // `&clock` pointer shared across all components.
            let clock = SimpleIntervalClock::new(Utc::now(), event_interval);
            let mut correlator = EventCorrelator::new(clock.clone());

            for prev in &input.previous_events {
                let mut event = prev.clone();
                // Snapshot clock for first/last timestamps (advances on read).
                let now = clock.now();
                event.first_timestamp = now;
                event.last_timestamp = now;
                let result = correlator.event_correlate(&event);
                if !result.skip {
                    correlator.update_state(result.event.as_ref().unwrap());
                }
            }

            let now = clock.now();
            let mut new_event = input.new_event.clone();
            new_event.first_timestamp = now;
            new_event.last_timestamp = now;
            let result = correlator.event_correlate(&new_event);

            assert_eq!(
                result.skip, input.expected_skip,
                "scenario {name}: expected skip {}, got {}",
                input.expected_skip, result.skip
            );

            if input.expected_skip {
                continue;
            }

            validate_event(
                name,
                result.event.as_ref().unwrap(),
                input.expected_event.as_ref().unwrap(),
            );
        }
    }

    // Port of TestEventSpamFilter.
    #[test]
    fn test_event_spam_filter() {
        fn spam_key_func_based_on_objects_and_reason(e: &CorrelatorEvent) -> String {
            [
                e.source.component.as_str(),
                e.source.host.as_str(),
                e.involved_object.kind.as_str(),
                e.involved_object.namespace.as_str(),
                e.involved_object.name.as_str(),
                e.involved_object.uid.as_str(),
                e.involved_object.api_version.as_str(),
                e.reason.as_str(),
            ]
            .concat()
        }

        let burst_size = 1;
        let event_interval = chrono::Duration::seconds(1);
        let original_event = make_event(
            "original",
            "i am first",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let different_reason_event = make_event(
            "duplicate",
            "me again",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let spam_event = make_event(
            "original",
            "me again",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );

        struct SpamCase {
            new_event: CorrelatorEvent,
            expected_event: Option<CorrelatorEvent>,
            expected_skip: bool,
            spam_key_func: Option<EventSpamKeyFunc>,
        }

        let cases: Vec<(&str, SpamCase)> = vec![
            (
                "spam if object reference is the same for default spam filter",
                SpamCase {
                    new_event: different_reason_event.clone(),
                    expected_event: None,
                    expected_skip: true,
                    spam_key_func: None,
                },
            ),
            (
                "not spam if object same but reason differs for custom spam filter",
                SpamCase {
                    new_event: different_reason_event.clone(),
                    expected_event: Some(different_reason_event.clone()),
                    expected_skip: false,
                    spam_key_func: Some(spam_key_func_based_on_objects_and_reason),
                },
            ),
            (
                "spam if object+reason same but message differs for custom spam filter",
                SpamCase {
                    new_event: spam_event.clone(),
                    expected_event: None,
                    expected_skip: true,
                    spam_key_func: Some(spam_key_func_based_on_objects_and_reason),
                },
            ),
        ];

        for (desc, input) in cases {
            let clock = SimpleIntervalClock::new(Utc::now(), event_interval);
            let mut correlator = EventCorrelator::with_options(CorrelatorOptions {
                clock,
                burst_size: Some(burst_size),
                qps: None,
                key_func: None,
                message_func: None,
                max_events: None,
                max_interval_in_seconds: None,
                spam_key_func: input.spam_key_func,
            });

            // Emit the original event.
            let result = correlator.event_correlate(&original_event);
            if !result.skip {
                correlator.update_state(result.event.as_ref().unwrap());
            }

            let result = correlator.event_correlate(&input.new_event);

            assert_eq!(
                result.skip, input.expected_skip,
                "scenario {desc}: expected skip {}, got {}",
                input.expected_skip, result.skip
            );

            if input.expected_skip {
                continue;
            }

            validate_event(
                desc,
                result.event.as_ref().unwrap(),
                input.expected_event.as_ref().unwrap(),
            );
        }
    }

    /// The de-duplication logger produces a count-incremented event and a patch
    /// on the second observation of the exact same event.
    #[test]
    fn test_event_observe_produces_count_bump_and_patch() {
        let clock = FixedClock::new(Utc::now());
        let mut logger = EventLogger::new(clock);
        let event = make_event(
            "Started",
            "started pod",
            make_object_reference("Pod", "my-pod", "my-ns"),
        );
        let key = event.event_key();

        let (first, patch1) = logger.event_observe(&event, &key);
        assert_eq!(first.count, 1, "first observation count should be 1");
        assert!(patch1.is_none(), "no patch on first observation");

        let (second, patch2) = logger.event_observe(&event, &key);
        assert_eq!(second.count, 2, "second observation count should be 2");
        let patch = patch2.expect("patch expected on repeat observation");
        let patch_str = String::from_utf8(patch).unwrap();
        assert!(
            patch_str.contains("\"count\":2"),
            "patch should carry count=2, got {patch_str}"
        );
    }
}
