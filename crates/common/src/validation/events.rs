//! Event field validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/events.go` (release-1.35).
//!
//! Two public entry points:
//! * [`validate_event_create`] — create-path validation.
//! * [`validate_event_update`] — update-path validation (immutability +
//!   series checks).
//!
//! Both take a [`RequestVersion`] discriminating the API group/version the
//! request came in on. `core/v1` (and the legacy `events.k8s.io/v1beta1`)
//! path only runs [`legacy_validate_event`] for backwards compatibility; the
//! `events.k8s.io/v1` path additionally enforces the strict rules
//! (`eventTime` required, `type` must be Normal/Warning, new-style timestamp
//! fields must be unset, etc.).
//!
//! Validators *accumulate* every problem into an [`ErrorList`] rather than
//! short-circuiting, and field paths / error wording match upstream so
//! conformance log greps stay valid.
//!
//! Upstream:
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/apis/core/validation/events.go>

use crate::resources::event::{Event, EventType};
use crate::types::ObjectMeta;
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;
use crate::validation::metav1::is_qualified_name;
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use chrono::{DateTime, Utc};

/// Upstream length limits (`events.go` consts).
const REPORTING_INSTANCE_LENGTH_LIMIT: usize = 128;
const ACTION_LENGTH_LIMIT: usize = 128;
const REASON_LENGTH_LIMIT: usize = 128;
const NOTE_LENGTH_LIMIT: usize = 1024;

/// The API group/version a request arrived on. Mirrors upstream
/// `schema.GroupVersion`, but only the cases the validator branches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestVersion {
    /// core `v1` Event endpoint (`/api/v1/.../events`).
    CoreV1,
    /// legacy `events.k8s.io/v1beta1` endpoint.
    EventsV1Beta1,
    /// strict `events.k8s.io/v1` endpoint.
    EventsV1,
}

impl RequestVersion {
    /// True for the versions that skip strict validation for backwards
    /// compatibility (core/v1 and events.k8s.io/v1beta1).
    fn is_legacy_only(self) -> bool {
        matches!(self, RequestVersion::CoreV1 | RequestVersion::EventsV1Beta1)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Upstream `ValidateQualifiedName` — turns `IsQualifiedName` messages into
/// `field.Invalid` errors.
fn validate_qualified_name(value: &str, fld_path: &Path) -> ErrorList {
    is_qualified_name(value)
        .into_iter()
        .map(|msg| Error::invalid(fld_path, value, msg))
        .collect()
}

/// Upstream `validateV1EventSeries`.
fn validate_v1_event_series(event: &Event) -> ErrorList {
    let mut errs = ErrorList::new();
    if let Some(series) = &event.series {
        if series.count < 2 {
            errs.push(Error::invalid(
                &Path::new("series.count"),
                BadValue::Omit,
                "should be at least 2",
            ));
        }
        // `last_observed_time` is a required (non-Option) field on our model,
        // but the zero value (Unix epoch) stands in for "unset" the same way
        // Go's zero `time.Time` does upstream.
        if is_zero_time(&series.last_observed_time) {
            errs.push(Error::required(&Path::new("series.lastObservedTime"), ""));
        }
    }
    errs
}

/// Mirror of Go's `time.Time{}.IsZero()` check for our optional MicroTime
/// fields. A `None` (field absent) is treated as zero, as is the Unix epoch
/// which is what an all-zero serialized MicroTime decodes to.
fn is_zero_time(t: &DateTime<Utc>) -> bool {
    t.timestamp() == 0 && t.timestamp_subsec_nanos() == 0
}

fn opt_is_zero_time(t: &Option<DateTime<Utc>>) -> bool {
    match t {
        None => true,
        Some(t) => is_zero_time(t),
    }
}

/// Truncate a timestamp to microsecond precision (drop sub-microsecond
/// nanoseconds). Mirrors `event.EventTime.Truncate(time.Microsecond)` upstream.
fn truncate_to_micros(t: &DateTime<Utc>) -> DateTime<Utc> {
    let nanos = t.timestamp_subsec_nanos();
    let micros_floor = (nanos / 1_000) * 1_000;
    // Rebuild from seconds + truncated subsec nanos.
    DateTime::from_timestamp(t.timestamp(), micros_floor).unwrap_or(*t)
}

/// Compare two optional event-times at microsecond precision. `None` is
/// treated as the zero time so an absent value compares equal to an explicit
/// epoch.
fn event_time_equal_micros(a: &Option<DateTime<Utc>>, b: &Option<DateTime<Utc>>) -> bool {
    let za = a.map(|t| truncate_to_micros(&t));
    let zb = b.map(|t| truncate_to_micros(&t));
    match (za, zb) {
        (None, None) => true,
        (Some(x), Some(y)) => x == y,
        (Some(x), None) | (None, Some(x)) => is_zero_time(&x),
    }
}

/// Upstream `ValidateImmutableField` for arbitrary serializable values.
fn validate_immutable<T: serde::Serialize + ?Sized>(
    new_val: &T,
    old_val: &T,
    fld_path: &Path,
) -> ErrorList {
    let new_json = serde_json::to_value(new_val).unwrap_or(serde_json::Value::Null);
    let old_json = serde_json::to_value(old_val).unwrap_or(serde_json::Value::Null);
    if new_json == old_json {
        return ErrorList::new();
    }
    vec![Error::invalid(fld_path, new_json, "field is immutable")]
}

/// The effective message body: events.k8s.io/v1 calls it `note`, core/v1
/// calls it `message`. We model both and prefer `note` when set.
fn effective_message(event: &Event) -> &str {
    if let Some(note) = &event.note {
        if !note.is_empty() {
            return note;
        }
    }
    &event.message
}

/// The effective involved-object namespace. events.k8s.io/v1 uses `regarding`,
/// core/v1 uses `involvedObject`; handlers map `regarding` → `involvedObject`
/// before storage, so reading `involved_object` here covers both.
fn involved_namespace(event: &Event) -> &str {
    event
        .involved_object
        .namespace
        .as_deref()
        .unwrap_or_default()
}

fn reporting_controller(event: &Event) -> &str {
    event.reporting_component.as_deref().unwrap_or_default()
}

fn reporting_instance(event: &Event) -> &str {
    event.reporting_instance.as_deref().unwrap_or_default()
}

fn action(event: &Event) -> &str {
    event.action.as_deref().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// legacy validation (core/v1 + shared)
// ---------------------------------------------------------------------------

/// Upstream `legacyValidateEvent`. Runs for *every* request version.
pub fn legacy_validate_event(event: &Event, request_version: RequestVersion) -> ErrorList {
    let mut errs = ErrorList::new();

    // core/v1 calls the field "reportingComponent"; everything else
    // "reportingController". Only affects the Required error path.
    let reporting_controller_field = if request_version == RequestVersion::CoreV1 {
        "reportingComponent"
    } else {
        "reportingController"
    };

    let namespace = event.metadata.namespace.as_deref().unwrap_or_default();
    let involved_ns = involved_namespace(event);

    if opt_is_zero_time(&event.event_time) {
        // "Old" event (no eventTime): event.namespace must agree with
        // involvedObject.namespace.
        if involved_ns.is_empty() {
            // event.Namespace must be empty or "default".
            if !namespace.is_empty() && namespace != "default" {
                errs.push(Error::invalid(
                    &Path::new("involvedObject").child("namespace"),
                    involved_ns.to_string(),
                    "does not match event.namespace",
                ));
            }
        } else if namespace != involved_ns {
            errs.push(Error::invalid(
                &Path::new("involvedObject").child("namespace"),
                involved_ns.to_string(),
                "does not match event.namespace",
            ));
        }
    } else {
        // "New" event (eventTime set): stricter reporting requirements.
        if involved_ns.is_empty() && namespace != "default" && namespace != "kube-system" {
            errs.push(Error::invalid(
                &Path::new("involvedObject").child("namespace"),
                involved_ns.to_string(),
                "does not match event.namespace",
            ));
        }
        let rc = reporting_controller(event);
        if rc.is_empty() {
            errs.push(Error::required(&Path::new(reporting_controller_field), ""));
        }
        errs.extend(validate_qualified_name(
            rc,
            &Path::new(reporting_controller_field),
        ));
        let ri = reporting_instance(event);
        if ri.is_empty() {
            errs.push(Error::required(&Path::new("reportingInstance"), ""));
        }
        if ri.len() > REPORTING_INSTANCE_LENGTH_LIMIT {
            errs.push(Error::invalid(
                &Path::new("reportingInstance"),
                BadValue::Omit,
                format!("can have at most {REPORTING_INSTANCE_LENGTH_LIMIT} characters"),
            ));
        }
        let act = action(event);
        if act.is_empty() {
            errs.push(Error::required(&Path::new("action"), ""));
        }
        if act.len() > ACTION_LENGTH_LIMIT {
            errs.push(Error::invalid(
                &Path::new("action"),
                BadValue::Omit,
                format!("can have at most {ACTION_LENGTH_LIMIT} characters"),
            ));
        }
        if event.reason.is_empty() {
            errs.push(Error::required(&Path::new("reason"), ""));
        }
        if event.reason.len() > REASON_LENGTH_LIMIT {
            errs.push(Error::invalid(
                &Path::new("reason"),
                BadValue::Omit,
                format!("can have at most {REASON_LENGTH_LIMIT} characters"),
            ));
        }
        if effective_message(event).len() > NOTE_LENGTH_LIMIT {
            errs.push(Error::invalid(
                &Path::new("message"),
                BadValue::Omit,
                format!("can have at most {NOTE_LENGTH_LIMIT} characters"),
            ));
        }
    }

    for msg in is_dns1123_subdomain(namespace) {
        errs.push(Error::invalid(&Path::new("namespace"), namespace, msg));
    }

    errs
}

// ---------------------------------------------------------------------------
// create
// ---------------------------------------------------------------------------

/// Upstream `ValidateEventCreate`.
pub fn validate_event_create(event: &Event, request_version: RequestVersion) -> ErrorList {
    // Make sure events always pass legacy validation.
    let mut errs = legacy_validate_event(event, request_version);
    if request_version.is_legacy_only() {
        // No further validation for backwards compatibility.
        return errs;
    }

    // Strict validation applies to creation via events.k8s.io/v1 and newer.
    errs.extend(validate_object_meta_create(&event.metadata));
    errs.extend(validate_v1_event_series(event));

    if opt_is_zero_time(&event.event_time) {
        errs.push(Error::required(&Path::new("eventTime"), ""));
    }
    if event.event_type != EventType::Normal && event.event_type != EventType::Warning {
        // Unreachable while EventType only models Normal/Warning, but kept for
        // parity with upstream in case the enum grows.
        errs.push(Error::invalid(
            &Path::new("type"),
            BadValue::Omit,
            format!("has invalid value: {:?}", event.event_type),
        ));
    }
    if !opt_is_zero_time(&event.first_timestamp) {
        errs.push(Error::invalid(
            &Path::new("firstTimestamp"),
            BadValue::Omit,
            "needs to be unset",
        ));
    }
    if !opt_is_zero_time(&event.last_timestamp) {
        errs.push(Error::invalid(
            &Path::new("lastTimestamp"),
            BadValue::Omit,
            "needs to be unset",
        ));
    }
    if event.count != 0 {
        errs.push(Error::invalid(
            &Path::new("count"),
            BadValue::Omit,
            "needs to be unset",
        ));
    }
    if !event.source.component.is_empty() || !event.source.host.as_deref().unwrap_or("").is_empty()
    {
        errs.push(Error::invalid(
            &Path::new("source"),
            BadValue::Omit,
            "needs to be unset",
        ));
    }
    errs
}

/// `ValidateObjectMeta(&meta, requireNamespace=true, NameIsDNSSubdomain, "metadata")`.
fn validate_object_meta_create(meta: &ObjectMeta) -> ErrorList {
    validate_object_meta(
        meta,
        true,
        name_is_dns_subdomain as fn(&str, bool) -> Vec<String>,
        &Path::new("metadata"),
    )
}

// ---------------------------------------------------------------------------
// update
// ---------------------------------------------------------------------------

/// Upstream `ValidateEventUpdate`.
pub fn validate_event_update(
    new_event: &Event,
    old_event: &Event,
    request_version: RequestVersion,
) -> ErrorList {
    // Make sure the new event always passes legacy validation.
    let mut errs = legacy_validate_event(new_event, request_version);
    if request_version.is_legacy_only() {
        // No further validation for backwards compatibility.
        return errs;
    }

    // Strict validation applies to update via events.k8s.io/v1 and newer.
    errs.extend(validate_object_meta_update(
        &new_event.metadata,
        &old_event.metadata,
        &Path::new("metadata"),
    ));

    // If the series was modified, validate the new data.
    if new_event.series != old_event.series {
        errs.extend(validate_v1_event_series(new_event));
    }

    errs.extend(validate_immutable(
        &new_event.involved_object,
        &old_event.involved_object,
        &Path::new("involvedObject"),
    ));
    errs.extend(validate_immutable(
        &new_event.reason,
        &old_event.reason,
        &Path::new("reason"),
    ));
    errs.extend(validate_immutable(
        effective_message(new_event),
        effective_message(old_event),
        &Path::new("message"),
    ));
    errs.extend(validate_immutable(
        &new_event.source,
        &old_event.source,
        &Path::new("source"),
    ));
    errs.extend(validate_immutable(
        &new_event.first_timestamp,
        &old_event.first_timestamp,
        &Path::new("firstTimestamp"),
    ));
    errs.extend(validate_immutable(
        &new_event.last_timestamp,
        &old_event.last_timestamp,
        &Path::new("lastTimestamp"),
    ));
    errs.extend(validate_immutable(
        &new_event.count,
        &old_event.count,
        &Path::new("count"),
    ));
    errs.extend(validate_immutable(
        &new_event.event_type,
        &old_event.event_type,
        &Path::new("type"),
    ));

    // Disallow changes to eventTime greater than microsecond-level precision.
    // Tolerating sub-microsecond changes accommodates clients that truncate to
    // microseconds (or that emit incorrect nanosecond-precision protobuf).
    // See https://github.com/kubernetes/kubernetes/issues/111928
    if !event_time_equal_micros(&new_event.event_time, &old_event.event_time) {
        errs.extend(validate_immutable(
            &new_event.event_time,
            &old_event.event_time,
            &Path::new("eventTime"),
        ));
    }

    errs.extend(validate_immutable(
        &new_event.action,
        &old_event.action,
        &Path::new("action"),
    ));
    errs.extend(validate_immutable(
        &new_event.related,
        &old_event.related,
        &Path::new("related"),
    ));
    errs.extend(validate_immutable(
        &new_event.reporting_component,
        &old_event.reporting_component,
        &Path::new("reportingController"),
    ));
    errs.extend(validate_immutable(
        &new_event.reporting_instance,
        &old_event.reporting_instance,
        &Path::new("reportingInstance"),
    ));

    errs
}

#[cfg(test)]
#[path = "events_tests.rs"]
mod tests;
