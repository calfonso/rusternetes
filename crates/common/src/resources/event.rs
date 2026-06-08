use crate::resources::ObjectReference;
use crate::types::ObjectMeta;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Module for serializing/deserializing Kubernetes Time format (without fractional seconds).
/// Kubernetes Time uses the format: "2006-01-02T15:04:05Z"
/// but must also accept timestamps with fractional seconds.
mod k8s_time {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(dt) => {
                // Kubernetes Time format: WITHOUT microseconds (RFC3339 basic)
                let s = dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
                serializer.serialize_str(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                // Accept RFC3339 with or without fractional seconds
                if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                    return Ok(Some(dt.with_timezone(&Utc)));
                }
                s.parse::<DateTime<Utc>>()
                    .map(Some)
                    .map_err(serde::de::Error::custom)
            }
            None => Ok(None),
        }
    }
}

/// Module for serializing/deserializing MicroTime format (with microsecond precision).
/// Kubernetes MicroTime requires the format: "2006-01-02T15:04:05.000000Z"
/// but must also accept plain RFC3339 timestamps without fractional seconds.
mod micro_time {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(dt) => {
                // Always include microseconds for MicroTime — K8s Events v1 client
                // uses time.Parse("2006-01-02T15:04:05.000000Z07:00") which requires .000000
                let s = dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
                serializer.serialize_str(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                // Try parsing with microseconds first, then plain RFC3339
                if let Ok(dt) = DateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.6fZ") {
                    return Ok(Some(dt.with_timezone(&Utc)));
                }
                if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                    return Ok(Some(dt.with_timezone(&Utc)));
                }
                s.parse::<DateTime<Utc>>()
                    .map(Some)
                    .map_err(serde::de::Error::custom)
            }
            None => Ok(None),
        }
    }

    /// Serialize a required DateTime in MicroTime format (non-Option)
    pub fn serialize_required<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = date.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
        serializer.serialize_str(&s)
    }

    /// Deserialize a required DateTime from MicroTime format (non-Option)
    pub fn deserialize_required<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        if let Ok(dt) = DateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.6fZ") {
            return Ok(dt.with_timezone(&Utc));
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
            return Ok(dt.with_timezone(&Utc));
        }
        s.parse::<DateTime<Utc>>().map_err(serde::de::Error::custom)
    }
}

/// Event represents a single event in the system
///
/// Many fields are optional to tolerate varied event formats from conformance tests
/// and different Kubernetes client implementations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(default = "default_api_version")]
    pub api_version: String,

    #[serde(default = "default_kind")]
    pub kind: String,

    pub metadata: ObjectMeta,

    /// InvolvedObject is the object this event is about
    #[serde(default)]
    pub involved_object: ObjectReference,

    /// Reason is a short, machine understandable string that gives the reason for this event
    #[serde(default)]
    pub reason: String,

    /// Message is a human-readable description of the status of this operation
    #[serde(default)]
    pub message: String,

    /// Source component generating the event
    #[serde(default)]
    pub source: EventSource,

    /// Type of this event (Normal, Warning), new types could be added in the future
    #[serde(rename = "type", default)]
    pub event_type: EventType,

    /// The time at which the event was first recorded
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize"
    )]
    pub first_timestamp: Option<DateTime<Utc>>,

    /// The time at which the most recent occurrence of this event was recorded
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "k8s_time::serialize",
        deserialize_with = "k8s_time::deserialize"
    )]
    pub last_timestamp: Option<DateTime<Utc>>,

    /// The number of times this event has occurred
    #[serde(default)]
    pub count: i32,

    /// Optional action that was taken/failed regarding the Regarding object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,

    /// Optional related object (e.g., Node for Pod event)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related: Option<ObjectReference>,

    /// Event series data (for aggregated events)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<EventSeries>,

    /// Time when this Event was first observed (MicroTime precision)
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "micro_time::serialize",
        deserialize_with = "micro_time::deserialize"
    )]
    pub event_time: Option<DateTime<Utc>>,

    /// Name of the controller that emitted this Event (e.g., "kubernetes.io/kubelet")
    /// K8s events.k8s.io/v1 calls this "reportingController"
    #[serde(
        rename = "reportingController",
        skip_serializing_if = "Option::is_none",
        alias = "reportingComponent"
    )]
    pub reporting_component: Option<String>,

    /// ID of the controller instance (e.g., "kubelet-xyzf")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reporting_instance: Option<String>,

    /// Note is a human-readable description (used by events.k8s.io/v1 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,

    /// Regarding contains the object this Event is about (events.k8s.io/v1 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regarding: Option<ObjectReference>,

    /// Catch-all for any additional fields not explicitly defined
    #[serde(flatten)]
    pub extra: Option<std::collections::HashMap<String, serde_json::Value>>,
}

impl Event {
    /// Create a new Event
    pub fn new(
        name: String,
        namespace: String,
        involved_object: ObjectReference,
        reason: String,
        message: String,
        event_type: EventType,
    ) -> Self {
        let now = Utc::now();
        Self {
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            involved_object,
            reason,
            message,
            source: EventSource {
                component: "rusternetes".to_string(),
                host: None,
            },
            event_type,
            first_timestamp: Some(now),
            last_timestamp: Some(now),
            count: 1,
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

    /// Generate event name based on involved object and reason
    pub fn generate_name(involved_object: &ObjectReference, reason: &str) -> String {
        let obj_name = involved_object.name.as_deref().unwrap_or("unknown");
        let uid = involved_object.uid.as_deref().unwrap_or("unknown");
        // Use a stable name (no timestamp) so duplicate events can be detected
        // and deduplicated. The event's first/last timestamp fields track timing.
        format!(
            "{}.{}.{}",
            obj_name,
            reason.to_lowercase(),
            uid[..8.min(uid.len())].to_lowercase()
        )
    }
}

/// EventSource contains information for an event
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EventSource {
    /// Component from which the event is generated
    #[serde(default)]
    pub component: String,

    /// Node name on which the event is generated (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

/// EventType is the type of an event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum EventType {
    #[default]
    Normal,
    Warning,
}

/// EventSeries contains information on series of events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EventSeries {
    /// Number of occurrences in this series up to the last heartbeat time
    pub count: i32,

    /// Time of the last occurrence observed (MicroTime format for K8s compatibility)
    #[serde(
        serialize_with = "micro_time::serialize_required",
        deserialize_with = "micro_time::deserialize_required"
    )]
    pub last_observed_time: DateTime<Utc>,
}

/// EventList is a list of events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventList {
    #[serde(default = "default_api_version")]
    pub api_version: String,

    #[serde(default = "default_kind_list")]
    pub kind: String,

    pub metadata: crate::types::ListMeta,

    pub items: Vec<Event>,
}

impl Default for EventList {
    fn default() -> Self {
        Self {
            api_version: "v1".to_string(),
            kind: "EventList".to_string(),
            metadata: crate::types::ListMeta::default(),
            items: Vec::new(),
        }
    }
}

fn default_api_version() -> String {
    "v1".to_string()
}

fn default_kind() -> String {
    "Event".to_string()
}

fn default_kind_list() -> String {
    "EventList".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref.clone(),
            "PodStarted".to_string(),
            "Pod test-pod started successfully".to_string(),
            EventType::Normal,
        );

        assert_eq!(event.metadata.name, "test-event");
        assert_eq!(event.metadata.namespace, Some("default".to_string()));
        assert_eq!(event.reason, "PodStarted");
        assert_eq!(event.message, "Pod test-pod started successfully");
        assert_eq!(event.event_type, EventType::Normal);
        assert_eq!(event.count, 1);
        assert_eq!(event.involved_object.kind, Some("Pod".to_string()));
    }

    #[test]
    fn test_generate_event_name() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123def456".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let name = Event::generate_name(&obj_ref, "PodStarted");
        assert!(name.starts_with("test-pod.podstarted.abc123de"));
    }

    #[test]
    fn test_event_serialization() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref,
            "PodStarted".to_string(),
            "Pod test-pod started successfully".to_string(),
            EventType::Normal,
        );

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("PodStarted"));
        assert!(json.contains("Pod test-pod started successfully"));
        assert!(json.contains("Normal"));

        // Test deserialization
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.reason, event.reason);
        assert_eq!(deserialized.message, event.message);
        assert_eq!(deserialized.event_type, event.event_type);
    }

    #[test]
    fn test_event_list() {
        let list = EventList::default();
        assert_eq!(list.api_version, "v1");
        assert_eq!(list.kind, "EventList");
        assert_eq!(list.items.len(), 0);
    }

    #[test]
    fn test_event_field_selector_involved_object() {
        use crate::field_selector::FieldSelector;

        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("my-pod".to_string()),
            uid: Some("uid-123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref,
            "Started".to_string(),
            "Pod started".to_string(),
            EventType::Normal,
        );

        let json_val = serde_json::to_value(&event).unwrap();

        // Field selector on involvedObject.name should work (camelCase serialization)
        let selector = FieldSelector::parse("involvedObject.name=my-pod").unwrap();
        assert!(
            selector.matches(&json_val),
            "involvedObject.name selector should match"
        );

        // Non-matching selector
        let selector = FieldSelector::parse("involvedObject.name=other-pod").unwrap();
        assert!(
            !selector.matches(&json_val),
            "Non-matching selector should not match"
        );

        // Field selector on metadata.namespace
        let selector = FieldSelector::parse("metadata.namespace=default").unwrap();
        assert!(
            selector.matches(&json_val),
            "metadata.namespace selector should match"
        );

        // Combined selectors
        let selector =
            FieldSelector::parse("involvedObject.name=my-pod,involvedObject.kind=Pod").unwrap();
        assert!(
            selector.matches(&json_val),
            "Combined selector should match"
        );
    }
}
