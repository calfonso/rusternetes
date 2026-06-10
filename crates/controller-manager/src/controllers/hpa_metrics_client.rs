//! Metrics source abstraction for the HPA controller.
//!
//! Mirrors k8s `pkg/controller/podautoscaler/metrics/interfaces.go`. The real
//! impl (`HttpMetricsClient`) queries the api-server metrics endpoints over
//! mTLS; tests inject `FakeMetricsClient`.
//!
//! The trait surface + test double land ahead of the consumer (a later task
//! wires the HPA controller to it), so the whole module is `allow(dead_code)`
//! until then — matching the pattern used by other staged controllers in this
//! crate.
#![allow(dead_code)]

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusternetes_common::resources::CrossVersionObjectReference;
use rusternetes_common::types::LabelSelector;
use std::collections::HashMap;

/// Per-pod metric reading. `value` is the raw metric in milli-units for cpu
/// (e.g. 500 = 500m) or whole units otherwise; `utilization` is the percentage
/// of the pod's request (resource metrics only).
#[derive(Debug, Clone)]
pub struct PodMetric {
    pub value: i64,
    /// Utilization percent vs request, resource metrics only.
    pub utilization: Option<i32>,
    pub timestamp: DateTime<Utc>,
}

/// Map of pod name -> metric reading.
pub type PodMetricsInfo = HashMap<String, PodMetric>;

#[async_trait]
pub trait MetricsClient: Send + Sync {
    /// Per-pod resource utilization (cpu/memory) for pods matching `selector`.
    async fn get_resource_metric(
        &self,
        resource: &str,
        namespace: &str,
        selector: &LabelSelector,
    ) -> Result<PodMetricsInfo>;

    /// Per-pod custom ("Pods" type) metric.
    async fn get_raw_metric(
        &self,
        metric: &str,
        namespace: &str,
        selector: &LabelSelector,
    ) -> Result<PodMetricsInfo>;

    /// Single ("Object" type) metric value for a referenced object.
    async fn get_object_metric(
        &self,
        metric: &str,
        namespace: &str,
        object_ref: &CrossVersionObjectReference,
    ) -> Result<(i64, DateTime<Utc>)>;

    /// External metric value(s).
    async fn get_external_metric(
        &self,
        metric: &str,
        namespace: &str,
        selector: &LabelSelector,
    ) -> Result<(Vec<i64>, DateTime<Utc>)>;
}

/// Test double. Each map is keyed by metric name; resource map keyed by resource.
#[derive(Default, Clone)]
pub struct FakeMetricsClient {
    pub resource: HashMap<String, PodMetricsInfo>,
    pub pods: HashMap<String, PodMetricsInfo>,
    pub object: HashMap<String, i64>,
    pub external: HashMap<String, Vec<i64>>,
}

impl FakeMetricsClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Helper: build a PodMetricsInfo where each named pod reports `value`
    /// (and optional utilization percent).
    pub fn pods_info(pods: &[(&str, i64, Option<i32>)]) -> PodMetricsInfo {
        let now = Utc::now();
        pods.iter()
            .map(|(name, value, util)| {
                (
                    name.to_string(),
                    PodMetric {
                        value: *value,
                        utilization: *util,
                        timestamp: now,
                    },
                )
            })
            .collect()
    }
}

#[async_trait]
impl MetricsClient for FakeMetricsClient {
    async fn get_resource_metric(
        &self,
        resource: &str,
        _namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        self.resource
            .get(resource)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no resource metric for {resource}"))
    }

    async fn get_raw_metric(
        &self,
        metric: &str,
        _namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        self.pods
            .get(metric)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no pods metric for {metric}"))
    }

    async fn get_object_metric(
        &self,
        metric: &str,
        _namespace: &str,
        _object_ref: &CrossVersionObjectReference,
    ) -> Result<(i64, DateTime<Utc>)> {
        self.object
            .get(metric)
            .map(|v| (*v, Utc::now()))
            .ok_or_else(|| anyhow::anyhow!("no object metric for {metric}"))
    }

    async fn get_external_metric(
        &self,
        metric: &str,
        _namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<(Vec<i64>, DateTime<Utc>)> {
        self.external
            .get(metric)
            .map(|v| (v.clone(), Utc::now()))
            .ok_or_else(|| anyhow::anyhow!("no external metric for {metric}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_returns_seeded_pods_metric() {
        let mut fake = FakeMetricsClient::new();
        fake.pods.insert(
            "requests-per-second".into(),
            FakeMetricsClient::pods_info(&[("p1", 200, None), ("p2", 200, None)]),
        );
        let sel = LabelSelector::default();
        let info = fake
            .get_raw_metric("requests-per-second", "default", &sel)
            .await
            .unwrap();
        assert_eq!(info.len(), 2);
        assert_eq!(info["p1"].value, 200);
    }

    #[tokio::test]
    async fn fake_errors_on_missing_metric() {
        let fake = FakeMetricsClient::new();
        let sel = LabelSelector::default();
        assert!(fake
            .get_resource_metric("cpu", "default", &sel)
            .await
            .is_err());
    }
}
