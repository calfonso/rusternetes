//! Table-driven tests ported from upstream
//! `pkg/apis/core/validation/events_test.go` (release-1.35).
//!
//! Covers `TestValidateEventForCoreV1Events`,
//! `TestValidateEventCreateForNewV1Events`,
//! `TestValidateEventUpdateForNewV1Events`, and
//! `TestEventV1EventTimeImmutability`.

use super::*;
use crate::resources::event::{EventSeries, EventSource, EventType};
use crate::resources::ObjectReference;
use crate::types::ObjectMeta;
use chrono::{DateTime, TimeZone, Utc};

/// `time.Unix(1505828956, 0)` upstream — the canonical "someTime".
fn some_time() -> DateTime<Utc> {
    Utc.timestamp_opt(1_505_828_956, 0).unwrap()
}

/// Build a bare event with everything zero/empty. Tests override fields.
fn base_event() -> Event {
    Event {
        api_version: "events.k8s.io/v1".to_string(),
        kind: "Event".to_string(),
        metadata: ObjectMeta::default(),
        involved_object: ObjectReference::default(),
        reason: String::new(),
        message: String::new(),
        source: EventSource::default(),
        event_type: EventType::Normal,
        first_timestamp: None,
        last_timestamp: None,
        count: 0,
        action: None,
        related: None,
        series: None,
        event_time: None,
        reporting_component: None,
        reporting_instance: None,
        note: None,
        regarding: None,
        extra: None,
    }
}

fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: name.to_string(),
        namespace: if namespace.is_empty() {
            None
        } else {
            Some(namespace.to_string())
        },
        ..Default::default()
    }
}

fn involved(api_version: &str, kind: &str, namespace: &str) -> ObjectReference {
    ObjectReference {
        api_version: Some(api_version.to_string()),
        kind: Some(kind.to_string()),
        namespace: if namespace.is_empty() {
            None
        } else {
            Some(namespace.to_string())
        },
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// TestValidateEventForCoreV1Events
// ---------------------------------------------------------------------------

#[test]
fn validate_event_for_core_v1_events() {
    struct Case {
        name: &'static str,
        meta_ns: &'static str,
        involved: ObjectReference,
        valid: bool,
    }
    let cases = vec![
        Case {
            name: "test1",
            meta_ns: "foo",
            involved: involved("", "Pod", "bar"),
            valid: false,
        },
        Case {
            name: "test2",
            meta_ns: "aoeu-_-aoeu",
            involved: involved("", "Pod", "aoeu-_-aoeu"),
            valid: false,
        },
        Case {
            name: "test3",
            meta_ns: "default",
            involved: involved("v1", "Node", ""),
            valid: true,
        },
        Case {
            name: "test4",
            meta_ns: "default",
            involved: involved("v1", "Namespace", ""),
            valid: true,
        },
        Case {
            name: "test5",
            meta_ns: "default",
            involved: involved("apps/v1", "NoKind", "default"),
            valid: true,
        },
        Case {
            name: "test6",
            meta_ns: "default",
            involved: involved("batch/v1", "Job", "foo"),
            valid: false,
        },
        Case {
            name: "test7",
            meta_ns: "default",
            involved: involved("batch/v1", "Job", "default"),
            valid: true,
        },
        Case {
            name: "test8",
            meta_ns: "default",
            involved: involved("other/v1beta1", "Job", "foo"),
            valid: false,
        },
        Case {
            name: "test9",
            meta_ns: "foo",
            involved: involved("other/v1beta1", "Job", "foo"),
            valid: true,
        },
        Case {
            name: "test10",
            meta_ns: "default",
            involved: involved("batch", "Job", "foo"),
            valid: false,
        },
        Case {
            name: "test11",
            meta_ns: "foo",
            involved: involved("batch/v1", "Job", "foo"),
            valid: true,
        },
        Case {
            name: "test12",
            meta_ns: "foo",
            involved: involved("other/v1beta1", "FooBar", "bar"),
            valid: false,
        },
        Case {
            name: "test13",
            meta_ns: "",
            involved: involved("other/v1beta1", "FooBar", "bar"),
            valid: false,
        },
        Case {
            name: "test14",
            meta_ns: "foo",
            involved: involved("other/v1beta1", "FooBar", ""),
            valid: false,
        },
    ];

    for c in &cases {
        let mut ev = base_event();
        ev.metadata = meta(c.name, c.meta_ns);
        ev.involved_object = c.involved.clone();

        let create_errs = validate_event_create(&ev, RequestVersion::CoreV1);
        assert_eq!(
            create_errs.is_empty(),
            c.valid,
            "{}: create expected valid={}, got errs={:?}",
            c.name,
            c.valid,
            create_errs
        );

        let update_errs = validate_event_update(&ev, &base_event(), RequestVersion::CoreV1);
        assert_eq!(
            update_errs.is_empty(),
            c.valid,
            "{}: update expected valid={}, got errs={:?}",
            c.name,
            c.valid,
            update_errs
        );
    }
}

// ---------------------------------------------------------------------------
// TestValidateEventCreateForNewV1Events
// ---------------------------------------------------------------------------

#[test]
fn validate_event_create_for_new_v1_events() {
    // valid new event
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "kube-system");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("k8s.io/my-controller".to_string());
        ev.reporting_instance = Some("node-xyz".to_string());
        ev.action = Some("Do".to_string());
        ev.reason = "Because".to_string();
        ev.event_type = EventType::Normal;
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(errs.is_empty(), "valid new event: got errs={errs:?}");
    }

    // missing name in objectMeta
    {
        let mut ev = base_event();
        ev.metadata = meta("", "kube-system");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("k8s.io/my-controller".to_string());
        ev.reporting_instance = Some("node-xyz".to_string());
        ev.reason = "Because".to_string();
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "missing name should be invalid");
    }

    // missing namespace in objectMeta
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("k8s.io/my-controller".to_string());
        ev.reporting_instance = Some("node-xyz".to_string());
        ev.reason = "Because".to_string();
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "missing namespace should be invalid");
    }

    // missing EventTime
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "default");
        ev.involved_object = involved("v1", "Node", "");
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "missing eventTime should be invalid");
    }

    // not qualified reportingController
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "kube-system");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("my-contr@ller".to_string());
        ev.reporting_instance = Some("node-xyz".to_string());
        ev.action = Some("Do".to_string());
        ev.reason = "Because".to_string();
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "unqualified reportingController invalid");
    }

    // too long reporting instance
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "kube-system");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("k8s.io/my-controller".to_string());
        ev.reporting_instance = Some(format!("node-{}", "z".repeat(140)));
        ev.action = Some("Do".to_string());
        ev.reason = "Because".to_string();
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "too long reportingInstance invalid");
    }

    // missing reason
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "kube-system");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("k8s.io/my-controller".to_string());
        ev.reporting_instance = Some("node-xyz".to_string());
        ev.action = Some("Do".to_string());
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "missing reason invalid");
    }

    // missing action
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "kube-system");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("k8s.io/my-controller".to_string());
        ev.reporting_instance = Some("node-xyz".to_string());
        ev.reason = "Because".to_string();
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "missing action invalid");
    }

    // too long message
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "kube-system");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("k8s.io/my-controller".to_string());
        ev.reporting_instance = Some("node-xyz".to_string());
        ev.action = Some("Do".to_string());
        ev.reason = "Because".to_string();
        ev.message = "z".repeat(NOTE_LENGTH_LIMIT + 1);
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "too long message invalid");
    }

    // invalid type — representable here only as the modeled enum, so the
    // structurally-impossible "invalid-type" string case is covered by the
    // serde layer. We assert the two valid types pass and document the gap.
    {
        let mut ev = base_event();
        ev.metadata = meta("test", "kube-system");
        ev.involved_object = involved("v1", "Node", "");
        ev.event_time = Some(some_time());
        ev.reporting_component = Some("k8s.io/my-controller".to_string());
        ev.reporting_instance = Some("node-xyz".to_string());
        ev.action = Some("Do".to_string());
        ev.reason = "Because".to_string();
        ev.event_type = EventType::Warning;
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(errs.is_empty(), "Warning type is valid: {errs:?}");
    }

    // non-empty firstTimestamp
    {
        let mut ev = valid_v1_create_event();
        ev.first_timestamp = Some(some_time());
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "firstTimestamp must be unset");
    }

    // non-empty lastTimestamp
    {
        let mut ev = valid_v1_create_event();
        ev.last_timestamp = Some(some_time());
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "lastTimestamp must be unset");
    }

    // non-empty count
    {
        let mut ev = valid_v1_create_event();
        ev.count = 123;
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "count must be unset");
    }

    // non-empty source
    {
        let mut ev = valid_v1_create_event();
        ev.source = EventSource {
            component: String::new(),
            host: Some("host".to_string()),
        };
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "source must be unset");
    }

    // non-nil series with count < 2
    {
        let mut ev = valid_v1_create_event();
        ev.series = Some(EventSeries {
            count: 0,
            last_observed_time: some_time(),
        });
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "series count < 2 invalid");
    }

    // non-nil series with empty lastObservedTime
    {
        let mut ev = valid_v1_create_event();
        ev.series = Some(EventSeries {
            count: 2,
            last_observed_time: Utc.timestamp_opt(0, 0).unwrap(),
        });
        let errs = validate_event_create(&ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "series empty lastObservedTime invalid");
    }
}

/// A baseline valid events.k8s.io/v1 create event (Normal type).
fn valid_v1_create_event() -> Event {
    let mut ev = base_event();
    ev.metadata = meta("test", "kube-system");
    ev.involved_object = involved("v1", "Node", "");
    ev.event_time = Some(some_time());
    ev.reporting_component = Some("k8s.io/my-controller".to_string());
    ev.reporting_instance = Some("node-xyz".to_string());
    ev.action = Some("Do".to_string());
    ev.reason = "Because".to_string();
    ev.event_type = EventType::Normal;
    ev
}

// ---------------------------------------------------------------------------
// TestValidateEventUpdateForNewV1Events
// ---------------------------------------------------------------------------

/// A baseline valid events.k8s.io/v1 event for update tests (has a
/// resourceVersion since ValidateObjectMetaUpdate requires one).
fn valid_v1_update_event() -> Event {
    let mut ev = base_event();
    ev.metadata = meta("test", "kube-system");
    ev.metadata.resource_version = Some("1".to_string());
    ev.involved_object = involved("v1", "Node", "");
    ev.event_time = Some(some_time());
    ev.reporting_component = Some("k8s.io/my-controller".to_string());
    ev.reporting_instance = Some("node-xyz".to_string());
    ev.action = Some("Do".to_string());
    ev.reason = "Yeees".to_string();
    ev.event_type = EventType::Normal;
    ev
}

#[test]
fn validate_event_update_for_new_v1_events() {
    // valid updated event (series count bumped from 1 to 2)
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.metadata.resource_version = Some("2".to_string());
        new_ev.involved_object = involved("v2", "Node", "");
        new_ev.series = Some(EventSeries {
            count: 2,
            last_observed_time: some_time(),
        });
        let mut old_ev = new_ev.clone();
        old_ev.series = Some(EventSeries {
            count: 1,
            last_observed_time: some_time(),
        });
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(errs.is_empty(), "valid updated event: {errs:?}");
    }

    // forbidden updates to involvedObject
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.involved_object = involved("v2", "Node", "");
        let mut old_ev = valid_v1_update_event();
        old_ev.involved_object = involved("v1", "Node", "");
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "involvedObject is immutable");
    }

    // forbidden updates to reason
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.reason = "Yeees-new".to_string();
        let old_ev = valid_v1_update_event();
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "reason is immutable");
    }

    // forbidden updates to message
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.message = "new-message".to_string();
        let mut old_ev = valid_v1_update_event();
        old_ev.message = "message".to_string();
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "message is immutable");
    }

    // forbidden updates to source
    {
        let new_ev = valid_v1_update_event();
        let mut old_ev = valid_v1_update_event();
        old_ev.source = EventSource {
            component: String::new(),
            host: Some("host".to_string()),
        };
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "source is immutable");
    }

    // forbidden updates to firstTimestamp
    {
        let new_ev = valid_v1_update_event();
        let mut old_ev = valid_v1_update_event();
        old_ev.first_timestamp = Some(some_time());
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "firstTimestamp is immutable");
    }

    // forbidden updates to lastTimestamp
    {
        let new_ev = valid_v1_update_event();
        let mut old_ev = valid_v1_update_event();
        old_ev.last_timestamp = Some(some_time());
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "lastTimestamp is immutable");
    }

    // forbidden updates to count
    {
        let new_ev = valid_v1_update_event();
        let mut old_ev = valid_v1_update_event();
        old_ev.count = 2;
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "count is immutable");
    }

    // forbidden updates to type
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.event_type = EventType::Warning;
        let old_ev = valid_v1_update_event();
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "type is immutable");
    }

    // forbidden updates to eventTime (seconds-level change)
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.event_time = Some(Utc.timestamp_opt(1_505_828_999, 0).unwrap());
        let old_ev = valid_v1_update_event();
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "eventTime seconds change is immutable");
    }

    // forbidden updates to action
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.action = Some("Undo".to_string());
        let old_ev = valid_v1_update_event();
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "action is immutable");
    }

    // forbidden updates to related
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.related = Some(ObjectReference {
            api_version: Some("v1".to_string()),
            ..Default::default()
        });
        let old_ev = valid_v1_update_event();
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "related is immutable");
    }

    // forbidden updates to reportingController
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.reporting_component = Some("k8s.io/my-controller/new".to_string());
        let old_ev = valid_v1_update_event();
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "reportingController is immutable");
    }

    // forbidden updates to reportingInstance
    {
        let mut new_ev = valid_v1_update_event();
        new_ev.reporting_instance = Some("node-xyz-new".to_string());
        let old_ev = valid_v1_update_event();
        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert!(!errs.is_empty(), "reportingInstance is immutable");
    }
}

// ---------------------------------------------------------------------------
// TestEventV1EventTimeImmutability
// ---------------------------------------------------------------------------

#[test]
fn event_v1_event_time_immutability() {
    let micro = 1_000i64; // nanoseconds in a microsecond
    let nano = 1i64;

    struct Case {
        name: &'static str,
        old: DateTime<Utc>,
        new: DateTime<Utc>,
        valid: bool,
    }
    let cases = vec![
        Case {
            name: "noop microsecond precision",
            old: Utc.timestamp_opt(100, (5 * micro) as u32).unwrap(),
            new: Utc.timestamp_opt(100, (5 * micro) as u32).unwrap(),
            valid: true,
        },
        Case {
            name: "noop nanosecond precision",
            old: Utc.timestamp_opt(100, (5 * nano) as u32).unwrap(),
            new: Utc.timestamp_opt(100, (5 * nano) as u32).unwrap(),
            valid: true,
        },
        Case {
            name: "modify nanoseconds within the same microsecond",
            old: Utc.timestamp_opt(100, (5 * nano) as u32).unwrap(),
            new: Utc.timestamp_opt(100, (6 * nano) as u32).unwrap(),
            valid: true,
        },
        Case {
            name: "modify microseconds",
            old: Utc.timestamp_opt(100, (5 * micro) as u32).unwrap(),
            new: Utc.timestamp_opt(100, (5 * micro - nano) as u32).unwrap(),
            valid: false,
        },
        Case {
            name: "modify seconds",
            old: Utc.timestamp_opt(100, 0).unwrap(),
            new: Utc.timestamp_opt(101, 0).unwrap(),
            valid: false,
        },
    ];

    for c in &cases {
        let mut old_ev = valid_v1_update_event();
        old_ev.metadata.resource_version = Some("2".to_string());
        old_ev.involved_object = involved("v2", "Node", "");
        old_ev.series = Some(EventSeries {
            count: 2,
            last_observed_time: c.old,
        });
        old_ev.event_time = Some(c.old);

        let mut new_ev = old_ev.clone();
        new_ev.event_time = Some(c.new);

        let errs = validate_event_update(&new_ev, &old_ev, RequestVersion::EventsV1);
        assert_eq!(
            errs.is_empty(),
            c.valid,
            "{}: expected valid={}, got errs={:?}",
            c.name,
            c.valid,
            errs
        );
    }
}
