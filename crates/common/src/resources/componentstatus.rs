use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// ComponentStatus represents the status of a component (scheduler, controller-manager, etcd)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentStatus {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    /// Conditions holds the status of the component
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<ComponentCondition>>,
}

impl ComponentStatus {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ComponentStatus".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            conditions: None,
        }
    }

    pub fn with_conditions(mut self, conditions: Vec<ComponentCondition>) -> Self {
        self.conditions = Some(conditions);
        self
    }

    pub fn healthy(name: impl Into<String>) -> Self {
        Self::new(name).with_conditions(vec![ComponentCondition {
            condition_type: "Healthy".to_string(),
            status: "True".to_string(),
            message: None,
            error: None,
        }])
    }

    pub fn unhealthy(name: impl Into<String>, error: impl Into<String>) -> Self {
        Self::new(name).with_conditions(vec![ComponentCondition {
            condition_type: "Healthy".to_string(),
            status: "False".to_string(),
            message: Some("Component is unhealthy".to_string()),
            error: Some(error.into()),
        }])
    }
}

/// ComponentCondition represents a condition of a component
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ComponentCondition {
    /// Type of condition (e.g., "Healthy")
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    /// Human-readable message indicating details about the condition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Error message if status is False
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_component_status_healthy() {
        let cs = ComponentStatus::healthy("scheduler");

        assert_eq!(cs.metadata.name, "scheduler");
        assert_eq!(cs.type_meta.kind, "ComponentStatus");
        assert_eq!(cs.type_meta.api_version, "v1");

        let conditions = cs.conditions.unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].condition_type, "Healthy");
        assert_eq!(conditions[0].status, "True");
    }

    #[test]
    fn test_component_status_unhealthy() {
        let cs = ComponentStatus::unhealthy("controller-manager", "Connection refused");

        assert_eq!(cs.metadata.name, "controller-manager");

        let conditions = cs.conditions.unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].condition_type, "Healthy");
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[0].error, Some("Connection refused".to_string()));
    }
}
