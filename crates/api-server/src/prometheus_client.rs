//! Prometheus client for custom metrics backend integration
//!
//! This module provides integration with Prometheus for querying custom metrics
//! used by the HorizontalPodAutoscaler and other consumers of custom.metrics.k8s.io

use anyhow::{Context, Result};
use prometheus_http_query::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Prometheus client for custom metrics queries
pub struct PrometheusClient {
    client: Client,
    /// Cache of query results with TTL
    cache: Arc<RwLock<MetricsCache>>,
    /// Prometheus server URL
    #[allow(dead_code)]
    prometheus_url: String,
}

/// Cached metrics with TTL
struct MetricsCache {
    entries: HashMap<String, CacheEntry>,
}

struct CacheEntry {
    value: String,
    timestamp: chrono::DateTime<chrono::Utc>,
    ttl: Duration,
}

impl PrometheusClient {
    /// Create a new Prometheus client
    ///
    /// # Arguments
    /// * `prometheus_url` - URL of the Prometheus server (e.g., "http://localhost:9090")
    pub fn new(prometheus_url: impl Into<String>) -> Result<Self> {
        let url = prometheus_url.into();
        let client =
            Client::try_from(url.as_str()).context("Failed to create Prometheus client")?;

        Ok(Self {
            client,
            cache: Arc::new(RwLock::new(MetricsCache {
                entries: HashMap::new(),
            })),
            prometheus_url: url,
        })
    }

    /// Query a custom metric for a specific object
    ///
    /// # Arguments
    /// * `metric_name` - The metric name (e.g., "http_requests_total", "custom_queue_depth")
    /// * `namespace` - Kubernetes namespace
    /// * `resource_type` - Type of resource (e.g., "pods", "services")
    /// * `resource_name` - Name of the resource
    /// * `labels` - Additional label selectors
    pub async fn query_object_metric(
        &self,
        metric_name: &str,
        namespace: &str,
        resource_type: &str,
        resource_name: &str,
        labels: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        // Build PromQL query
        let query =
            self.build_object_query(metric_name, namespace, resource_type, resource_name, labels);

        debug!("Querying Prometheus for object metric: {}", query);

        // Check cache first
        if let Some(cached_value) = self.get_cached(&query).await {
            debug!("Returning cached metric value for query: {}", query);
            return Ok(cached_value);
        }

        // Query Prometheus
        let value = self.execute_instant_query(&query).await?;

        // Cache the result
        self.cache_value(&query, &value, Duration::from_secs(60))
            .await;

        Ok(value)
    }

    /// Query a custom metric for multiple objects (list query)
    ///
    /// Returns a map of object names to metric values
    pub async fn query_list_metric(
        &self,
        metric_name: &str,
        namespace: &str,
        resource_type: &str,
        labels: Option<&HashMap<String, String>>,
    ) -> Result<HashMap<String, String>> {
        // Build PromQL query without specific resource name
        let query = self.build_list_query(metric_name, namespace, resource_type, labels);

        debug!("Querying Prometheus for list metric: {}", query);

        // Execute query
        let response = self
            .client
            .query(&query)
            .get()
            .await
            .context("Failed to execute Prometheus query")?;

        // Parse results
        let mut results = HashMap::new();

        if let Some(instant_vector) = response.data().as_vector() {
            for sample in instant_vector {
                // Extract resource name from labels
                if let Some(name_label) = self.extract_resource_name_label(resource_type) {
                    if let Some(resource_name) = sample.metric().get(name_label.as_str()) {
                        let value = sample.sample().value().to_string();
                        results.insert(resource_name.to_string(), value);
                    }
                }
            }
        }

        debug!("Found {} metric values for list query", results.len());

        Ok(results)
    }

    /// Query a namespace-level metric
    pub async fn query_namespace_metric(
        &self,
        metric_name: &str,
        namespace: &str,
    ) -> Result<String> {
        // Build PromQL query for namespace-level metric
        let query = format!("sum({}{{namespace=\"{}\"}})", metric_name, namespace);

        debug!("Querying Prometheus for namespace metric: {}", query);

        // Check cache
        if let Some(cached_value) = self.get_cached(&query).await {
            return Ok(cached_value);
        }

        // Execute query
        let value = self.execute_instant_query(&query).await?;

        // Cache result
        self.cache_value(&query, &value, Duration::from_secs(60))
            .await;

        Ok(value)
    }

    /// Query a cluster-level metric (no namespace)
    pub async fn query_cluster_metric(
        &self,
        metric_name: &str,
        resource_type: &str,
        resource_name: &str,
        labels: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        // Build PromQL query without namespace
        let mut label_selectors = vec![];

        // Add resource name label
        if let Some(name_label) = self.extract_resource_name_label(resource_type) {
            label_selectors.push(format!("{}=\"{}\"", name_label, resource_name));
        }

        // Add additional labels
        if let Some(labels_map) = labels {
            for (key, value) in labels_map {
                label_selectors.push(format!("{}=\"{}\"", key, value));
            }
        }

        let query = if label_selectors.is_empty() {
            metric_name.to_string()
        } else {
            format!("{}{{{}}}", metric_name, label_selectors.join(","))
        };

        debug!("Querying Prometheus for cluster metric: {}", query);

        // Check cache
        if let Some(cached_value) = self.get_cached(&query).await {
            return Ok(cached_value);
        }

        // Execute query
        let value = self.execute_instant_query(&query).await?;

        // Cache result
        self.cache_value(&query, &value, Duration::from_secs(60))
            .await;

        Ok(value)
    }

    /// Build PromQL query for a specific object
    fn build_object_query(
        &self,
        metric_name: &str,
        namespace: &str,
        resource_type: &str,
        resource_name: &str,
        labels: Option<&HashMap<String, String>>,
    ) -> String {
        let mut label_selectors = vec![format!("namespace=\"{}\"", namespace)];

        // Add resource name label based on resource type
        if let Some(name_label) = self.extract_resource_name_label(resource_type) {
            label_selectors.push(format!("{}=\"{}\"", name_label, resource_name));
        }

        // Add additional labels
        if let Some(labels_map) = labels {
            for (key, value) in labels_map {
                label_selectors.push(format!("{}=\"{}\"", key, value));
            }
        }

        format!("{}{{{}}}", metric_name, label_selectors.join(","))
    }

    /// Build PromQL query for listing objects
    fn build_list_query(
        &self,
        metric_name: &str,
        namespace: &str,
        resource_type: &str,
        labels: Option<&HashMap<String, String>>,
    ) -> String {
        let mut label_selectors = vec![format!("namespace=\"{}\"", namespace)];

        // Add resource type label if applicable
        if resource_type != "namespaces" {
            // Most Prometheus metrics have a label indicating the resource type
            // This is a convention, may need customization per deployment
            if let Some(type_label) = self.extract_resource_type_label(resource_type) {
                label_selectors.push(format!("{}=\"{}\"", type_label, resource_type));
            }
        }

        // Add additional labels
        if let Some(labels_map) = labels {
            for (key, value) in labels_map {
                label_selectors.push(format!("{}=\"{}\"", key, value));
            }
        }

        format!("{}{{{}}}", metric_name, label_selectors.join(","))
    }

    /// Extract the Prometheus label name used for resource names
    ///
    /// Different exporters use different label conventions:
    /// - kube-state-metrics: pod, service, deployment, etc.
    /// - Standard Prometheus: instance, job, etc.
    fn extract_resource_name_label(&self, resource_type: &str) -> Option<String> {
        match resource_type {
            "pods" => Some("pod".to_string()),
            "services" => Some("service".to_string()),
            "deployments" => Some("deployment".to_string()),
            "replicasets" => Some("replicaset".to_string()),
            "statefulsets" => Some("statefulset".to_string()),
            "daemonsets" => Some("daemonset".to_string()),
            "nodes" => Some("node".to_string()),
            "namespaces" => Some("namespace".to_string()),
            "jobs" => Some("job".to_string()),
            "cronjobs" => Some("cronjob".to_string()),
            _ => {
                // Fallback: try to extract from resource_type by removing 's'
                let singular = resource_type.trim_end_matches('s');
                Some(singular.to_string())
            }
        }
    }

    /// Extract the Prometheus label name used for resource types
    fn extract_resource_type_label(&self, _resource_type: &str) -> Option<String> {
        // Most metrics use "resource" or "kind" labels
        // This may need customization based on your Prometheus setup
        Some("resource".to_string())
    }

    /// Execute an instant query and return the first value
    async fn execute_instant_query(&self, query: &str) -> Result<String> {
        let response = self
            .client
            .query(query)
            .get()
            .await
            .context("Failed to execute Prometheus instant query")?;

        // Extract the first value from the response
        if let Some(instant_vector) = response.data().as_vector() {
            if let Some(sample) = instant_vector.first() {
                return Ok(sample.sample().value().to_string());
            }
        }

        // No data found
        warn!("No data found for query: {}", query);
        Ok("0".to_string()) // Return 0 if no data
    }

    /// Get a cached value if it exists and is not expired
    async fn get_cached(&self, query: &str) -> Option<String> {
        let cache = self.cache.read().await;
        if let Some(entry) = cache.entries.get(query) {
            let now = chrono::Utc::now();
            let age = now.signed_duration_since(entry.timestamp);

            if age.num_seconds() < entry.ttl.as_secs() as i64 {
                return Some(entry.value.clone());
            }
        }
        None
    }

    /// Cache a value with TTL
    async fn cache_value(&self, query: &str, value: &str, ttl: Duration) {
        let mut cache = self.cache.write().await;
        cache.entries.insert(
            query.to_string(),
            CacheEntry {
                value: value.to_string(),
                timestamp: chrono::Utc::now(),
                ttl,
            },
        );

        // Cleanup old entries (simple approach: remove expired on insert)
        cache.entries.retain(|_, entry| {
            let now = chrono::Utc::now();
            let age = now.signed_duration_since(entry.timestamp);
            age.num_seconds() < entry.ttl.as_secs() as i64
        });
    }

    /// Get the Prometheus server URL
    #[allow(dead_code)]
    pub fn url(&self) -> &str {
        &self.prometheus_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_resource_name_label() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        assert_eq!(
            client.extract_resource_name_label("pods"),
            Some("pod".to_string())
        );
        assert_eq!(
            client.extract_resource_name_label("services"),
            Some("service".to_string())
        );
        assert_eq!(
            client.extract_resource_name_label("deployments"),
            Some("deployment".to_string())
        );
        assert_eq!(
            client.extract_resource_name_label("nodes"),
            Some("node".to_string())
        );
    }

    #[test]
    fn test_build_object_query() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let query =
            client.build_object_query("http_requests_total", "default", "pods", "nginx-pod", None);

        assert!(query.contains("namespace=\"default\""));
        assert!(query.contains("pod=\"nginx-pod\""));
        assert!(query.contains("http_requests_total"));
    }

    #[test]
    fn test_build_object_query_with_labels() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("env".to_string(), "production".to_string());

        let query = client.build_object_query(
            "custom_metric",
            "kube-system",
            "services",
            "kube-dns",
            Some(&labels),
        );

        assert!(query.contains("namespace=\"kube-system\""));
        assert!(query.contains("service=\"kube-dns\""));
        assert!(query.contains("app=\"nginx\""));
        assert!(query.contains("env=\"production\""));
    }

    #[test]
    fn test_build_list_query() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let query = client.build_list_query("memory_usage_bytes", "default", "pods", None);

        assert!(query.contains("namespace=\"default\""));
        assert!(query.contains("memory_usage_bytes"));
    }

    #[tokio::test]
    async fn test_cache_value_and_get() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let query = "test_metric{namespace=\"default\"}";
        let value = "42.5";

        // Cache the value
        client
            .cache_value(query, value, Duration::from_secs(60))
            .await;

        // Should retrieve from cache
        let cached = client.get_cached(query).await;
        assert_eq!(cached, Some(value.to_string()));
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let query = "test_metric{namespace=\"default\"}";
        let value = "42.5";

        // Cache with very short TTL
        client
            .cache_value(query, value, Duration::from_millis(10))
            .await;

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Should not retrieve from cache
        let cached = client.get_cached(query).await;
        assert_eq!(cached, None);
    }
}
