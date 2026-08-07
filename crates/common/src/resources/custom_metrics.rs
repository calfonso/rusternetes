// Custom Metrics API Resources (custom.metrics.k8s.io/v1beta2)
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// MetricValue represents a single metric value for a given object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricValue {
    pub api_version: String,
    pub kind: String,
    pub described_object: ObjectReference,
    pub metric_name: String,
    #[serde(
        serialize_with = "crate::time::k8s_time::serialize_required",
        deserialize_with = "crate::time::k8s_time::deserialize_required"
    )]
    pub timestamp: DateTime<Utc>,
    pub window: Option<String>,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<MetricSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectReference {
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricSelector {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<BTreeMap<String, String>>,
}

/// MetricValueList contains a list of metric values
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricValueList {
    pub api_version: String,
    pub kind: String,
    pub metadata: ListMetadata,
    pub items: Vec<MetricValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_link: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::SubsecRound;

    #[test]
    fn test_metric_value_serialization() {
        let metric_value = MetricValue {
            api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
            kind: "MetricValue".to_string(),
            described_object: ObjectReference {
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "test-pod".to_string(),
                api_version: Some("v1".to_string()),
            },
            metric_name: "http_requests_per_second".to_string(),
            timestamp: Utc::now().trunc_subsecs(0),
            window: Some("60s".to_string()),
            value: "100".to_string(),
            selector: Some(MetricSelector {
                match_labels: Some(BTreeMap::from([("app".to_string(), "nginx".to_string())])),
            }),
        };

        let json = serde_json::to_string(&metric_value).unwrap();
        let deserialized: MetricValue = serde_json::from_str(&json).unwrap();
        assert_eq!(metric_value, deserialized);
    }

    #[test]
    fn test_metric_value_list_serialization() {
        let metric_value_list = MetricValueList {
            api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
            kind: "MetricValueList".to_string(),
            metadata: ListMetadata {
                self_link: Some("/apis/custom.metrics.k8s.io/v1beta2/namespaces/default/pods/*/http_requests_per_second".to_string()),
            },
            items: vec![
                MetricValue {
                    api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
                    kind: "MetricValue".to_string(),
                    described_object: ObjectReference {
                        kind: "Pod".to_string(),
                        namespace: Some("default".to_string()),
                        name: "test-pod-1".to_string(),
                        api_version: Some("v1".to_string()),
                    },
                    metric_name: "http_requests_per_second".to_string(),
                    timestamp: Utc::now().trunc_subsecs(0),
                    window: Some("60s".to_string()),
                    value: "100".to_string(),
                    selector: None,
                },
            ],
        };

        let json = serde_json::to_string(&metric_value_list).unwrap();
        let deserialized: MetricValueList = serde_json::from_str(&json).unwrap();
        assert_eq!(metric_value_list, deserialized);
    }

    #[test]
    fn test_metric_value_fields() {
        let metric_value = MetricValue {
            api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
            kind: "MetricValue".to_string(),
            described_object: ObjectReference {
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "test-pod".to_string(),
                api_version: None,
            },
            metric_name: "http_requests".to_string(),
            timestamp: Utc::now().trunc_subsecs(0),
            window: None,
            value: "50".to_string(),
            selector: None,
        };

        assert_eq!(metric_value.api_version, "custom.metrics.k8s.io/v1beta2");
        assert_eq!(metric_value.kind, "MetricValue");
        assert_eq!(metric_value.described_object.name, "test-pod");
        assert_eq!(
            metric_value.described_object.namespace,
            Some("default".to_string())
        );
    }
}
