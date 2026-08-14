use crate::types::{ObjectMeta, Phase, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Namespace provides a scope for resource names
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Namespace {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<NamespaceSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<NamespaceStatus>,
}

impl Namespace {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Namespace".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: None,
            status: Some(NamespaceStatus {
                phase: Some(Phase::Active),
                conditions: None,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceSpec {
    /// Finalizers is a list of finalizers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalizers: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceStatus {
    /// Phase is the current lifecycle phase of the namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,

    /// Conditions describe the current conditions of a namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<NamespaceCondition>>,
}

/// NamespaceCondition contains details about the current condition of a namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceCondition {
    /// Type of namespace condition (NamespaceDeletionDiscoveryFailure, NamespaceDeletionGroupVersionParsingFailure, etc.)
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::time::k8s_time::serialize",
        deserialize_with = "crate::time::k8s_time::deserialize"
    )]
    pub last_transition_time: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
