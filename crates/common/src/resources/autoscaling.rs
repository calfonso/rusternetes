use crate::types::{ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// HorizontalPodAutoscaler automatically scales the number of pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HorizontalPodAutoscaler {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: HorizontalPodAutoscalerSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<HorizontalPodAutoscalerStatus>,
}

impl HorizontalPodAutoscaler {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: HorizontalPodAutoscalerSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "HorizontalPodAutoscaler".to_string(),
                api_version: "autoscaling/v2".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// HorizontalPodAutoscalerSpec describes the desired behavior of the autoscaler
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HorizontalPodAutoscalerSpec {
    /// Reference to the scaled resource (Deployment, ReplicaSet, StatefulSet, etc.)
    pub scale_target_ref: CrossVersionObjectReference,

    /// Minimum number of replicas
    pub min_replicas: Option<i32>,

    /// Maximum number of replicas
    pub max_replicas: i32,

    /// Metrics to use for scaling decisions
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Vec<MetricSpec>>,

    /// Behavior configures the scaling behavior (v2 API)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behavior: Option<HorizontalPodAutoscalerBehavior>,
}

/// CrossVersionObjectReference contains information to identify the referenced resource
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossVersionObjectReference {
    /// Kind of the referent (e.g., Deployment)
    pub kind: String,

    /// Name of the referent
    pub name: String,

    /// API version of the referent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

/// MetricSpec specifies how to scale based on a metric
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricSpec {
    /// Type of metric: Resource, Pods, Object, External, ContainerResource
    #[serde(rename = "type")]
    pub metric_type: String,

    /// Resource metric (CPU, memory)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<ResourceMetricSource>,

    /// Pods metric
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pods: Option<PodsMetricSource>,

    /// Object metric
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<ObjectMetricSource>,

    /// External metric
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external: Option<ExternalMetricSource>,

    /// Container resource metric
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_resource: Option<ContainerResourceMetricSource>,
}

/// ResourceMetricSource indicates how to scale on a resource metric (e.g., CPU)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMetricSource {
    /// Name of the resource (cpu, memory)
    pub name: String,

    /// Target specifies the target value for the metric
    pub target: MetricTarget,
}

/// PodsMetricSource indicates how to scale on a metric describing each pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodsMetricSource {
    /// Metric identifies the target metric
    pub metric: MetricIdentifier,

    /// Target specifies the target value for the metric
    pub target: MetricTarget,
}

/// ObjectMetricSource indicates how to scale on a metric describing a kubernetes object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMetricSource {
    /// Described object
    pub described_object: CrossVersionObjectReference,

    /// Metric identifies the target metric
    pub metric: MetricIdentifier,

    /// Target specifies the target value for the metric
    pub target: MetricTarget,
}

/// ExternalMetricSource indicates how to scale on a metric not associated with any k8s object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMetricSource {
    /// Metric identifies the target metric
    pub metric: MetricIdentifier,

    /// Target specifies the target value for the metric
    pub target: MetricTarget,
}

/// ContainerResourceMetricSource indicates how to scale on a resource metric for a specific container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResourceMetricSource {
    /// Name of the resource (cpu, memory)
    pub name: String,

    /// Container name
    pub container: String,

    /// Target specifies the target value for the metric
    pub target: MetricTarget,
}

/// MetricIdentifier defines the name and optionally selector for a metric
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricIdentifier {
    /// Name of the metric
    pub name: String,

    /// Selector to refine the metric
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<crate::types::LabelSelector>,
}

/// MetricTarget defines the target value, average value, or average utilization
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricTarget {
    /// Type: Utilization, Value, AverageValue
    #[serde(rename = "type")]
    pub target_type: String,

    /// Value is the target value of the metric (as a quantity)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// AverageValue is the target value of the average of the metric across all pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_value: Option<String>,

    /// AverageUtilization is the target value of the average resource utilization (percentage)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_utilization: Option<i32>,
}

/// HorizontalPodAutoscalerBehavior configures the scaling behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HorizontalPodAutoscalerBehavior {
    /// Scale up behavior
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale_up: Option<HPAScalingRules>,

    /// Scale down behavior
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale_down: Option<HPAScalingRules>,
}

/// HPAScalingRules configures the scaling behavior in one direction
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HPAScalingRules {
    /// StabilizationWindowSeconds is the number of seconds for which past recommendations should be considered
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stabilization_window_seconds: Option<i32>,

    /// SelectPolicy is used to specify which policy should be used (Min, Max, Disabled)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub select_policy: Option<String>,

    /// Policies is a list of potential scaling policies
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policies: Option<Vec<HPAScalingPolicy>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tolerance: Option<String>,
}

/// HPAScalingPolicy defines a single policy for scaling
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HPAScalingPolicy {
    /// Type is used to specify the scaling policy (Pods or Percent)
    #[serde(rename = "type")]
    pub policy_type: String,

    /// Value contains the amount of change permitted
    pub value: i32,

    /// PeriodSeconds specifies the window of time for which the policy should hold true
    pub period_seconds: i32,
}

/// HorizontalPodAutoscalerStatus describes the current status of the autoscaler
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HorizontalPodAutoscalerStatus {
    /// Most recent generation observed by this autoscaler
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Last time the autoscaler scaled the number of pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_scale_time: Option<DateTime<Utc>>,

    /// Current number of replicas
    pub current_replicas: i32,

    /// Desired number of replicas
    pub desired_replicas: i32,

    /// Current metric values
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_metrics: Option<Vec<MetricStatus>>,

    /// Conditions describe the current state of the autoscaler
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<HorizontalPodAutoscalerCondition>>,
}

/// MetricStatus describes the current value of a metric
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricStatus {
    /// Type of metric
    #[serde(rename = "type")]
    pub metric_type: String,

    /// Resource metric status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<ResourceMetricStatus>,

    /// Pods metric status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pods: Option<PodsMetricStatus>,

    /// Object metric status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<ObjectMetricStatus>,

    /// External metric status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external: Option<ExternalMetricStatus>,

    /// ContainerResource metric status (per-container resource metric)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_resource: Option<ContainerResourceMetricStatus>,
}

/// ContainerResourceMetricStatus indicates the current value of a container resource metric
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResourceMetricStatus {
    /// Name of the resource
    pub name: String,

    /// Container is the name of the container the metric applies to
    pub container: String,

    /// Current contains the current value for the metric
    pub current: MetricValueStatus,
}

/// ResourceMetricStatus indicates the current value of a resource metric
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMetricStatus {
    /// Name of the resource
    pub name: String,

    /// Current contains the current value for the metric
    pub current: MetricValueStatus,
}

/// PodsMetricStatus indicates the current value of a pods metric
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodsMetricStatus {
    /// Metric identifies the target metric
    pub metric: MetricIdentifier,

    /// Current contains the current value for the metric
    pub current: MetricValueStatus,
}

/// ObjectMetricStatus indicates the current value of a metric describing a k8s object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMetricStatus {
    /// Metric identifies the target metric
    pub metric: MetricIdentifier,

    /// Current contains the current value for the metric
    pub current: MetricValueStatus,

    /// Described object
    pub described_object: CrossVersionObjectReference,
}

/// ExternalMetricStatus indicates the current value of a global metric
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMetricStatus {
    /// Metric identifies the target metric
    pub metric: MetricIdentifier,

    /// Current contains the current value for the metric
    pub current: MetricValueStatus,
}

/// MetricValueStatus holds the current value for a metric
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricValueStatus {
    /// Value is the current value of the metric
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// AverageValue is the current value of the average of the metric
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_value: Option<String>,

    /// AverageUtilization is the current value of the average resource utilization (percentage)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_utilization: Option<i32>,
}

/// HorizontalPodAutoscalerCondition describes the current state of the autoscaler
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HorizontalPodAutoscalerCondition {
    /// Type of condition (ScalingActive, AbleToScale, ScalingLimited)
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    /// Last time the condition transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,

    /// Reason for the condition's last transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// VerticalPodAutoscaler automatically adjusts CPU and memory requests
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerticalPodAutoscaler {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: VerticalPodAutoscalerSpec,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<VerticalPodAutoscalerStatus>,
}

impl VerticalPodAutoscaler {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: VerticalPodAutoscalerSpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "VerticalPodAutoscaler".to_string(),
                api_version: "autoscaling.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
            status: None,
        }
    }
}

/// VerticalPodAutoscalerSpec describes the desired behavior of the VPA
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerticalPodAutoscalerSpec {
    /// TargetRef points to the controller managing the set of pods
    pub target_ref: CrossVersionObjectReference,

    /// UpdatePolicy controls how changes are applied to pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_policy: Option<PodUpdatePolicy>,

    /// ResourcePolicy controls which resources VPA should manage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_policy: Option<PodResourcePolicy>,

    /// Recommenders lists the names of vertical pod autoscaler recommenders to use
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommenders: Option<Vec<VerticalPodAutoscalerRecommenderSelector>>,
}

/// PodUpdatePolicy describes the rules for updating pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodUpdatePolicy {
    /// Mode controls whether VPA updates resources (Off, Initial, Recreate, Auto)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_mode: Option<String>,
}

/// PodResourcePolicy controls how VPA computes resource recommendations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodResourcePolicy {
    /// Per-container resource policies
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_policies: Option<Vec<ContainerResourcePolicy>>,
}

/// ContainerResourcePolicy controls how VPA makes recommendations for a container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResourcePolicy {
    /// Name of the container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,

    /// Mode controls whether VPA updates the container (Off, Auto)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// MinAllowed specifies the minimum resource amounts
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_allowed: Option<std::collections::HashMap<String, String>>,

    /// MaxAllowed specifies the maximum resource amounts
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_allowed: Option<std::collections::HashMap<String, String>>,

    /// ControlledResources specifies which resource types are controlled (cpu, memory)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controlled_resources: Option<Vec<String>>,
}

/// VerticalPodAutoscalerRecommenderSelector selects a specific VPA recommender
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerticalPodAutoscalerRecommenderSelector {
    /// Name of the recommender
    pub name: String,
}

/// VerticalPodAutoscalerStatus describes the current status of the VPA
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerticalPodAutoscalerStatus {
    /// Recommendation holds the recommended resource amounts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<RecommendedPodResources>,

    /// Conditions describe the current state of the VPA
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<VerticalPodAutoscalerCondition>>,
}

/// RecommendedPodResources holds the recommended resource amounts for pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecommendedPodResources {
    /// Recommended resources for each container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_recommendations: Option<Vec<RecommendedContainerResources>>,
}

/// RecommendedContainerResources holds the recommended resource amounts for a container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecommendedContainerResources {
    /// Container name
    pub container_name: String,

    /// Recommended resource amounts
    pub target: std::collections::HashMap<String, String>,

    /// Lower bound on resource amounts
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<std::collections::HashMap<String, String>>,

    /// Upper bound on resource amounts
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<std::collections::HashMap<String, String>>,

    /// Uncapped target resource amounts (without limits)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncapped_target: Option<std::collections::HashMap<String, String>>,
}

/// VerticalPodAutoscalerCondition describes the current state of the VPA
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerticalPodAutoscalerCondition {
    /// Type of condition (RecommendationProvided, LowConfidence, NoPodsMatched, etc.)
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    /// Last time the condition transitioned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<DateTime<Utc>>,

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
    fn test_hpa_creation() {
        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "web-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(2),
            max_replicas: 10,
            metrics: Some(vec![MetricSpec {
                metric_type: "Resource".to_string(),
                resource: Some(ResourceMetricSource {
                    name: "cpu".to_string(),
                    target: MetricTarget {
                        target_type: "Utilization".to_string(),
                        value: None,
                        average_value: None,
                        average_utilization: Some(80),
                    },
                }),
                pods: None,
                object: None,
                external: None,
                container_resource: None,
            }]),
            behavior: None,
        };

        let hpa = HorizontalPodAutoscaler::new("web-hpa", "default", spec);

        assert_eq!(hpa.metadata.name, "web-hpa");
        assert_eq!(hpa.type_meta.api_version, "autoscaling/v2");
        assert_eq!(hpa.spec.max_replicas, 10);
    }

    #[test]
    fn test_vpa_creation() {
        let spec = VerticalPodAutoscalerSpec {
            target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "api-server".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            update_policy: Some(PodUpdatePolicy {
                update_mode: Some("Auto".to_string()),
            }),
            resource_policy: None,
            recommenders: None,
        };

        let vpa = VerticalPodAutoscaler::new("api-vpa", "default", spec);

        assert_eq!(vpa.metadata.name, "api-vpa");
        assert_eq!(vpa.type_meta.api_version, "autoscaling.k8s.io/v1");
        assert_eq!(vpa.spec.target_ref.kind, "Deployment");
    }

    #[test]
    fn test_hpa_serialization() {
        let json = r#"{
  "apiVersion": "autoscaling/v2",
  "kind": "HorizontalPodAutoscaler",
  "metadata": {
    "name": "test-hpa",
    "namespace": "default"
  },
  "spec": {
    "scaleTargetRef": {
      "kind": "Deployment",
      "name": "test-app"
    },
    "minReplicas": 1,
    "maxReplicas": 5,
    "metrics": [
      {
        "type": "Resource",
        "resource": {
          "name": "cpu",
          "target": {
            "type": "Utilization",
            "averageUtilization": 70
          }
        }
      }
    ]
  }
}"#;

        let hpa: HorizontalPodAutoscaler = serde_json::from_str(json).unwrap();
        assert_eq!(hpa.metadata.name, "test-hpa");
        assert_eq!(hpa.spec.max_replicas, 5);

        let serialized = serde_json::to_string(&hpa).unwrap();
        let _: HorizontalPodAutoscaler = serde_json::from_str(&serialized).unwrap();
    }
}
