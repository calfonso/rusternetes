use crate::resources::workloads::PodTemplateSpec;
use crate::types::{LabelSelector, ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Deployment provides declarative updates for Pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deployment {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: DeploymentSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<DeploymentStatus>,
}

impl Deployment {
    pub fn new(name: impl Into<String>, spec: DeploymentSpec) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec,
            status: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,

    #[serde(default)]
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<DeploymentStrategy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision_history_limit: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_deadline_seconds: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentStrategy {
    #[serde(rename = "type", default = "default_rolling_update_strategy")]
    pub strategy_type: String, // Recreate or RollingUpdate

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<RollingUpdateDeployment>,
}

fn default_rolling_update_strategy() -> String {
    "RollingUpdate".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollingUpdateDeployment {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_unavailable: Option<serde_json::Value>, // IntOrString: int or "25%"

    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_surge: Option<serde_json::Value>, // IntOrString: int or "25%"
}

/// Deserialize a value that can be either an integer or a string into Option<String>
pub fn deserialize_int_or_string_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s)),
        Some(serde_json::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(other) => Ok(Some(other.to_string())),
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_replicas: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_replicas: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<DeploymentCondition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub collision_count: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminating_replicas: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentCondition {
    #[serde(rename = "type")]
    pub condition_type: String,

    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::time::k8s_time::serialize",
        deserialize_with = "crate::time::k8s_time::deserialize"
    )]
    pub last_update_time: Option<DateTime<Utc>>,

    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::time::k8s_time::serialize",
        deserialize_with = "crate::time::k8s_time::deserialize"
    )]
    pub last_transition_time: Option<DateTime<Utc>>,
}
