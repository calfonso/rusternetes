use crate::types::{LabelSelector, ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ResourceQuota sets aggregate quota restrictions enforced per namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuota {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: ResourceQuotaSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ResourceQuotaStatus>,
}

impl ResourceQuota {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: ResourceQuotaSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// ResourceQuotaSpec defines the desired hard limits to enforce for Quota
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuotaSpec {
    /// Hard is the set of desired hard limits for each named resource
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard: Option<HashMap<String, String>>,

    /// A collection of filters that must match each object tracked by a quota
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<String>>,

    /// ScopeSelector is also a collection of filters like scopes that must match each object
    #[serde(skip_serializing_if = "Option::is_none", rename = "scopeSelector")]
    pub scope_selector: Option<ScopeSelector>,
}

/// ScopeSelector represents the AND of the selectors
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSelector {
    /// A list of scope selector requirements by scope
    pub match_expressions: Vec<ScopedResourceSelectorRequirement>,
}

/// ScopedResourceSelectorRequirement is a selector that contains values, a scope name, and an operator
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopedResourceSelectorRequirement {
    /// The name of the scope that the selector applies to
    pub scope_name: String,

    /// Operator: In, NotIn, Exists, DoesNotExist
    pub operator: String,

    /// An array of string values
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// ResourceQuotaStatus defines the enforced hard limits and observed use
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuotaStatus {
    /// Hard is the set of enforced hard limits for each named resource
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard: Option<HashMap<String, String>>,

    /// Used is the current observed total usage of the resource in the namespace
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used: Option<HashMap<String, String>>,
}

/// LimitRange sets resource usage limits for each kind of resource in a Namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRange {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: LimitRangeSpec,
}

impl LimitRange {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: LimitRangeSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "LimitRange".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
        }
    }
}

/// LimitRangeSpec defines a min/max usage limit for resources that match on kind
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRangeSpec {
    /// Limits is a list of LimitRangeItem objects
    pub limits: Vec<LimitRangeItem>,
}

/// LimitRangeItem defines a min/max usage limit for any resource that matches on kind
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRangeItem {
    /// Type of resource that this limit applies to
    /// Valid values: Pod, Container, PersistentVolumeClaim
    #[serde(rename = "type")]
    pub item_type: String,

    /// Max usage constraints on this kind by resource name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<HashMap<String, String>>,

    /// Min usage constraints on this kind by resource name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<HashMap<String, String>>,

    /// Default resource requirement limit value by resource name if not specified
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<HashMap<String, String>>,

    /// DefaultRequest is the default resource requirement request value by resource name if not specified
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_request: Option<HashMap<String, String>>,

    /// MaxLimitRequestRatio represents the max burst value for the named resource
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_limit_request_ratio: Option<HashMap<String, String>>,
}

/// PriorityClass defines mapping from a priority class name to the priority integer value
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorityClass {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    /// The value of this priority class
    /// Higher values indicate higher priority
    /// Typical range: -2147483648 to 1000000000
    /// Values above 1000000000 are reserved for system use
    pub value: i32,

    /// GlobalDefault specifies whether this PriorityClass should be considered as the default
    #[serde(skip_serializing_if = "Option::is_none", rename = "globalDefault")]
    pub global_default: Option<bool>,

    /// Description is an arbitrary string that usually provides guidelines on when this priority class should be used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// PreemptionPolicy is the Policy for preempting pods with lower priority
    /// Valid values: PreemptLowerPriority, Never
    #[serde(skip_serializing_if = "Option::is_none", rename = "preemptionPolicy")]
    pub preemption_policy: Option<String>,
}

impl PriorityClass {
    pub fn new(name: impl Into<String>, value: i32) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "PriorityClass".to_string(),
                api_version: "scheduling.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            value,
            global_default: None,
            description: None,
            preemption_policy: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_global_default(mut self, global_default: bool) -> Self {
        self.global_default = Some(global_default);
        self
    }

    pub fn with_preemption_policy(mut self, policy: impl Into<String>) -> Self {
        self.preemption_policy = Some(policy.into());
        self
    }
}

/// PodDisruptionBudget limits the number of pods that can be disrupted during voluntary disruptions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudget {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: PodDisruptionBudgetSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PodDisruptionBudgetStatus>,
}

impl PodDisruptionBudget {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: PodDisruptionBudgetSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "PodDisruptionBudget".to_string(),
                api_version: "policy/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// PodDisruptionBudgetSpec describes what disruptions are allowed
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudgetSpec {
    /// Minimum number of pods that must be available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_available: Option<IntOrString>,

    /// Maximum number of pods that can be unavailable
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<IntOrString>,

    /// Label selector to identify the set of pods
    pub selector: LabelSelector,

    /// UnhealthyPodEvictionPolicy controls when unhealthy pods should be considered for eviction
    /// Valid values: IfHealthyBudget, AlwaysAllow
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unhealthy_pod_eviction_policy: Option<String>,
}

/// IntOrString can be an integer or a string percentage (e.g., "20%")
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum IntOrString {
    Int(i32),
    String(String),
}

impl std::fmt::Display for IntOrString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntOrString::Int(i) => write!(f, "{}", i),
            IntOrString::String(s) => write!(f, "{}", s),
        }
    }
}

/// PodDisruptionBudgetStatus represents the current status of a PDB
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudgetStatus {
    /// Current number of healthy pods
    #[serde(default)]
    pub current_healthy: i32,

    /// Minimum desired healthy pods
    #[serde(default)]
    pub desired_healthy: i32,

    /// Number of pod disruptions that are currently allowed
    #[serde(default)]
    pub disruptions_allowed: i32,

    /// Total number of pods counted by this disruption budget
    #[serde(default)]
    pub expected_pods: i32,

    /// Most recent generation observed when updating this PDB status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Conditions contain the latest available observations of the PDB's state
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<PodDisruptionBudgetCondition>>,

    /// DisruptedPods contains information about pods whose eviction was
    /// processed by the API server eviction subresource handler but has not
    /// yet been observed by the PodDisruptionBudget controller.
    /// A pod will be in this map from the time when the API server processed the
    /// eviction request to the time when the pod is seen by PDB controller
    /// as having been marked for deletion (or after a timeout). The key in the map is the name of the pod
    /// and the value is the time when the API server processed the eviction request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disrupted_pods: Option<HashMap<String, DateTime<Utc>>>,
}

/// PodDisruptionBudgetCondition contains details for the current condition of this PDB
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDisruptionBudgetCondition {
    /// Type of PDB condition
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    /// Last time the condition transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_quota_creation() {
        let mut hard = HashMap::new();
        hard.insert("pods".to_string(), "10".to_string());
        hard.insert("requests.cpu".to_string(), "4".to_string());
        hard.insert("requests.memory".to_string(), "8Gi".to_string());

        let spec = ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        };

        let quota = ResourceQuota::new("test-quota", "default", spec);

        assert_eq!(quota.metadata.name, "test-quota");
        assert_eq!(quota.metadata.namespace, Some("default".to_string()));
        assert_eq!(quota.type_meta.api_version, "v1");
        assert!(quota.spec.hard.is_some());
        assert_eq!(quota.spec.hard.as_ref().unwrap().get("pods").unwrap(), "10");
    }

    #[test]
    fn test_limit_range_creation() {
        let mut max = HashMap::new();
        max.insert("memory".to_string(), "1Gi".to_string());
        max.insert("cpu".to_string(), "2".to_string());

        let mut default = HashMap::new();
        default.insert("memory".to_string(), "512Mi".to_string());
        default.insert("cpu".to_string(), "500m".to_string());

        let limit_item = LimitRangeItem {
            item_type: "Container".to_string(),
            max: Some(max),
            min: None,
            default: Some(default),
            default_request: None,
            max_limit_request_ratio: None,
        };

        let spec = LimitRangeSpec {
            limits: vec![limit_item],
        };

        let limit_range = LimitRange::new("test-limits", "default", spec);

        assert_eq!(limit_range.metadata.name, "test-limits");
        assert_eq!(limit_range.type_meta.api_version, "v1");
        assert_eq!(limit_range.spec.limits.len(), 1);
        assert_eq!(limit_range.spec.limits[0].item_type, "Container");
    }

    #[test]
    fn test_priority_class_creation() {
        let pc = PriorityClass::new("high-priority", 1000)
            .with_description("High priority workloads")
            .with_preemption_policy("PreemptLowerPriority");

        assert_eq!(pc.metadata.name, "high-priority");
        assert_eq!(pc.value, 1000);
        assert_eq!(pc.type_meta.api_version, "scheduling.k8s.io/v1");
        assert_eq!(pc.description.as_ref().unwrap(), "High priority workloads");
        assert_eq!(
            pc.preemption_policy.as_ref().unwrap(),
            "PreemptLowerPriority"
        );
    }

    #[test]
    fn test_priority_class_global_default() {
        let pc = PriorityClass::new("default-priority", 0).with_global_default(true);

        assert_eq!(pc.global_default, Some(true));
    }

    #[test]
    fn test_priority_class_serialization() {
        let pc = PriorityClass::new("test", 500);
        let json = serde_json::to_string(&pc).unwrap();

        let deserialized: PriorityClass = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "test");
        assert_eq!(deserialized.value, 500);
    }

    #[test]
    fn test_resource_quota_serialization() {
        let json = r#"{
  "apiVersion": "v1",
  "kind": "ResourceQuota",
  "metadata": {
    "name": "compute-quota",
    "namespace": "default"
  },
  "spec": {
    "hard": {
      "pods": "10",
      "requests.cpu": "4",
      "requests.memory": "8Gi"
    }
  }
}"#;

        let quota: ResourceQuota = serde_json::from_str(json).unwrap();
        assert_eq!(quota.metadata.name, "compute-quota");
        assert!(quota.spec.hard.is_some());

        let serialized = serde_json::to_string(&quota).unwrap();
        let _: ResourceQuota = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn test_limit_range_serialization() {
        let json = r#"{
  "apiVersion": "v1",
  "kind": "LimitRange",
  "metadata": {
    "name": "mem-limit",
    "namespace": "default"
  },
  "spec": {
    "limits": [
      {
        "type": "Container",
        "max": {
          "memory": "2Gi"
        },
        "default": {
          "memory": "1Gi"
        }
      }
    ]
  }
}"#;

        let limit_range: LimitRange = serde_json::from_str(json).unwrap();
        assert_eq!(limit_range.metadata.name, "mem-limit");
        assert_eq!(limit_range.spec.limits.len(), 1);

        let serialized = serde_json::to_string(&limit_range).unwrap();
        let _: LimitRange = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn test_pod_disruption_budget_creation() {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(2)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("test-pdb", "default", spec);

        assert_eq!(pdb.metadata.name, "test-pdb");
        assert_eq!(pdb.metadata.namespace, Some("default".to_string()));
        assert_eq!(pdb.type_meta.api_version, "policy/v1");
        assert!(pdb.spec.min_available.is_some());
    }

    #[test]
    fn test_pod_disruption_budget_int_or_string() {
        // Test integer value
        let int_spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(3)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("int-pdb", "default", int_spec);
        match &pdb.spec.min_available {
            Some(IntOrString::Int(n)) => assert_eq!(*n, 3),
            _ => panic!("Expected IntOrString::Int"),
        }

        // Test percentage string value
        let percent_spec = PodDisruptionBudgetSpec {
            min_available: None,
            max_unavailable: Some(IntOrString::String("20%".to_string())),
            selector: LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        };

        let pdb2 = PodDisruptionBudget::new("percent-pdb", "default", percent_spec);
        match &pdb2.spec.max_unavailable {
            Some(IntOrString::String(s)) => assert_eq!(s, "20%"),
            _ => panic!("Expected IntOrString::String"),
        }
    }

    #[test]
    fn test_pod_disruption_budget_serialization() {
        let json = r#"{
  "apiVersion": "policy/v1",
  "kind": "PodDisruptionBudget",
  "metadata": {
    "name": "web-pdb",
    "namespace": "default"
  },
  "spec": {
    "minAvailable": 2,
    "selector": {
      "matchLabels": {
        "app": "web"
      }
    }
  }
}"#;

        let pdb: PodDisruptionBudget = serde_json::from_str(json).unwrap();
        assert_eq!(pdb.metadata.name, "web-pdb");
        assert!(pdb.spec.min_available.is_some());

        match &pdb.spec.min_available {
            Some(IntOrString::Int(n)) => assert_eq!(*n, 2),
            _ => panic!("Expected IntOrString::Int"),
        }

        let serialized = serde_json::to_string(&pdb).unwrap();
        let _: PodDisruptionBudget = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn test_pod_disruption_budget_with_percentage() {
        let json = r#"{
  "apiVersion": "policy/v1",
  "kind": "PodDisruptionBudget",
  "metadata": {
    "name": "db-pdb",
    "namespace": "production"
  },
  "spec": {
    "maxUnavailable": "25%",
    "selector": {
      "matchLabels": {
        "app": "database"
      }
    }
  }
}"#;

        let pdb: PodDisruptionBudget = serde_json::from_str(json).unwrap();
        assert_eq!(pdb.metadata.name, "db-pdb");

        match &pdb.spec.max_unavailable {
            Some(IntOrString::String(s)) => assert_eq!(s, "25%"),
            _ => panic!("Expected IntOrString::String"),
        }
    }
}
