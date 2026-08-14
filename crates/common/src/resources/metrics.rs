// Metrics API Resources (metrics.k8s.io/v1beta1)
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// NodeMetrics contains resource usage metrics for a node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeMetrics {
    pub api_version: String,
    pub kind: String,
    pub metadata: NodeMetricsMetadata,
    #[serde(
        serialize_with = "crate::time::k8s_time::serialize_required",
        deserialize_with = "crate::time::k8s_time::deserialize_required"
    )]
    pub timestamp: DateTime<Utc>,
    pub window: String,
    pub usage: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeMetricsMetadata {
    pub name: String,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::time::k8s_time::serialize",
        deserialize_with = "crate::time::k8s_time::deserialize"
    )]
    pub creation_timestamp: Option<DateTime<Utc>>,
}

/// PodMetrics contains resource usage metrics for a pod
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodMetrics {
    pub api_version: String,
    pub kind: String,
    pub metadata: PodMetricsMetadata,
    #[serde(
        serialize_with = "crate::time::k8s_time::serialize_required",
        deserialize_with = "crate::time::k8s_time::deserialize_required"
    )]
    pub timestamp: DateTime<Utc>,
    pub window: String,
    pub containers: Vec<ContainerMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodMetricsMetadata {
    pub name: String,
    pub namespace: String,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::time::k8s_time::serialize",
        deserialize_with = "crate::time::k8s_time::deserialize"
    )]
    pub creation_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContainerMetrics {
    pub name: String,
    pub usage: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::SubsecRound;

    #[test]
    fn test_node_metrics_serialization() {
        let node_metrics = NodeMetrics {
            api_version: "metrics.k8s.io/v1beta1".to_string(),
            kind: "NodeMetrics".to_string(),
            metadata: NodeMetricsMetadata {
                name: "node-1".to_string(),
                creation_timestamp: Some(Utc::now().trunc_subsecs(0)),
            },
            timestamp: Utc::now().trunc_subsecs(0),
            window: "30s".to_string(),
            usage: BTreeMap::from([
                ("cpu".to_string(), "100m".to_string()),
                ("memory".to_string(), "1Gi".to_string()),
            ]),
        };

        let json = serde_json::to_string(&node_metrics).unwrap();
        let deserialized: NodeMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(node_metrics, deserialized);
    }

    #[test]
    fn test_pod_metrics_serialization() {
        let pod_metrics = PodMetrics {
            api_version: "metrics.k8s.io/v1beta1".to_string(),
            kind: "PodMetrics".to_string(),
            metadata: PodMetricsMetadata {
                name: "test-pod".to_string(),
                namespace: "default".to_string(),
                creation_timestamp: Some(Utc::now().trunc_subsecs(0)),
            },
            timestamp: Utc::now().trunc_subsecs(0),
            window: "30s".to_string(),
            containers: vec![ContainerMetrics {
                name: "nginx".to_string(),
                usage: BTreeMap::from([
                    ("cpu".to_string(), "50m".to_string()),
                    ("memory".to_string(), "512Mi".to_string()),
                ]),
            }],
        };

        let json = serde_json::to_string(&pod_metrics).unwrap();
        let deserialized: PodMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(pod_metrics, deserialized);
    }

    #[test]
    fn test_node_metrics_fields() {
        let node_metrics = NodeMetrics {
            api_version: "metrics.k8s.io/v1beta1".to_string(),
            kind: "NodeMetrics".to_string(),
            metadata: NodeMetricsMetadata {
                name: "node-1".to_string(),
                creation_timestamp: None,
            },
            timestamp: Utc::now().trunc_subsecs(0),
            window: "30s".to_string(),
            usage: BTreeMap::new(),
        };

        assert_eq!(node_metrics.api_version, "metrics.k8s.io/v1beta1");
        assert_eq!(node_metrics.kind, "NodeMetrics");
        assert_eq!(node_metrics.metadata.name, "node-1");
    }

    #[test]
    fn test_pod_metrics_fields() {
        let pod_metrics = PodMetrics {
            api_version: "metrics.k8s.io/v1beta1".to_string(),
            kind: "PodMetrics".to_string(),
            metadata: PodMetricsMetadata {
                name: "test-pod".to_string(),
                namespace: "default".to_string(),
                creation_timestamp: None,
            },
            timestamp: Utc::now().trunc_subsecs(0),
            window: "30s".to_string(),
            containers: vec![],
        };

        assert_eq!(pod_metrics.api_version, "metrics.k8s.io/v1beta1");
        assert_eq!(pod_metrics.kind, "PodMetrics");
        assert_eq!(pod_metrics.metadata.name, "test-pod");
        assert_eq!(pod_metrics.metadata.namespace, "default");
    }
}
