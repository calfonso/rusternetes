use crate::time::micro_time;
use crate::types::{ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Lease defines a lease concept
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lease {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<LeaseSpec>,
}

impl Lease {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Lease".to_string(),
                api_version: "coordination.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec: None,
        }
    }

    pub fn with_spec(mut self, spec: LeaseSpec) -> Self {
        self.spec = Some(spec);
        self
    }
}

/// LeaseSpec is a specification of a Lease
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaseSpec {
    /// holderIdentity contains the identity of the holder of a current lease
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder_identity: Option<String>,

    /// leaseDurationSeconds is a duration that candidates for a lease need to wait to force acquire it
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_duration_seconds: Option<i32>,

    /// acquireTime is the time when the current lease was acquired (MicroTime format)
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "micro_time::serialize",
        deserialize_with = "micro_time::deserialize"
    )]
    pub acquire_time: Option<DateTime<Utc>>,

    /// renewTime is the time when the current holder of a lease has last updated the lease (MicroTime format)
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "micro_time::serialize",
        deserialize_with = "micro_time::deserialize"
    )]
    pub renew_time: Option<DateTime<Utc>>,

    /// leaseTransitions is the number of transitions of a lease between holders
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_transitions: Option<i32>,

    /// preferredHolder signals to a lease holder that the lease has a more optimal holder
    /// and should be given up. This field can only be set if Strategy is also set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_holder: Option<String>,

    /// strategy indicates the strategy for picking the leader for coordinated leader election.
    /// If the field is not specified, there is no active coordination for this lease.
    /// (OldestEmulationVersion)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_creation() {
        let lease = Lease::new("test-lease", "default");
        assert_eq!(lease.metadata.name, "test-lease");
        assert_eq!(lease.type_meta.kind, "Lease");
        assert_eq!(lease.type_meta.api_version, "coordination.k8s.io/v1");
    }

    #[test]
    fn test_lease_spec_serialization() {
        let spec = LeaseSpec {
            holder_identity: Some("node-1".to_string()),
            lease_duration_seconds: Some(15),
            acquire_time: None,
            renew_time: None,
            lease_transitions: Some(3),
            preferred_holder: Some("node-2".to_string()),
            strategy: Some("OldestEmulationVersion".to_string()),
        };

        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"preferredHolder\":\"node-2\""));
        assert!(json.contains("\"strategy\":\"OldestEmulationVersion\""));
        assert!(json.contains("\"leaseTransitions\":3"));

        let deserialized: LeaseSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.preferred_holder, Some("node-2".to_string()));
        assert_eq!(
            deserialized.strategy,
            Some("OldestEmulationVersion".to_string())
        );
    }

    #[test]
    fn test_lease_micro_time_format() {
        let spec = LeaseSpec {
            holder_identity: Some("node-1".to_string()),
            lease_duration_seconds: Some(15),
            acquire_time: Some(Utc::now()),
            renew_time: Some(Utc::now()),
            lease_transitions: Some(0),
            preferred_holder: None,
            strategy: None,
        };

        let json = serde_json::to_string(&spec).unwrap();
        // Verify MicroTime format includes microseconds (6 decimal places)
        assert!(
            json.contains("."),
            "MicroTime should include fractional seconds: {}",
            json
        );

        // Verify round-trip
        let deserialized: LeaseSpec = serde_json::from_str(&json).unwrap();
        assert!(deserialized.acquire_time.is_some());
        assert!(deserialized.renew_time.is_some());
    }

    #[test]
    fn test_lease_micro_time_deserialize_various_formats() {
        // Without microseconds
        let json = r#"{"holderIdentity":"n","leaseDurationSeconds":15,"acquireTime":"2024-01-01T00:00:00Z"}"#;
        let spec: LeaseSpec = serde_json::from_str(json).unwrap();
        assert!(spec.acquire_time.is_some());

        // With microseconds
        let json = r#"{"holderIdentity":"n","leaseDurationSeconds":15,"acquireTime":"2024-01-01T00:00:00.000000Z"}"#;
        let spec: LeaseSpec = serde_json::from_str(json).unwrap();
        assert!(spec.acquire_time.is_some());
    }

    #[test]
    fn test_lease_spec_deserialization_from_kubernetes() {
        let json = r#"{
            "holderIdentity": "apiserver-abc123",
            "leaseDurationSeconds": 15,
            "leaseTransitions": 5,
            "preferredHolder": "apiserver-def456",
            "strategy": "OldestEmulationVersion"
        }"#;

        let spec: LeaseSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.holder_identity, Some("apiserver-abc123".to_string()));
        assert_eq!(spec.lease_duration_seconds, Some(15));
        assert_eq!(spec.preferred_holder, Some("apiserver-def456".to_string()));
    }
}
