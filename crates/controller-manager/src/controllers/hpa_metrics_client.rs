//! Metrics source abstraction for the HPA controller.
//!
//! Mirrors k8s `pkg/controller/podautoscaler/metrics/interfaces.go`. The real
//! impl (`HttpMetricsClient`) queries the api-server metrics endpoints over
//! mTLS; tests inject `FakeMetricsClient`.
//!
//! Some surface (the `PodMetric::timestamp` field, the `pods_info` test helper)
//! is only exercised in tests, so the module keeps `allow(dead_code)` to stay
//! clean under `--all-targets` clippy on the binary crate.
#![allow(dead_code)]

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
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

/// Config for the real metrics client.
#[derive(Clone, Debug)]
pub struct HttpMetricsConfig {
    pub api_server_url: String,
    pub ca_cert_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    /// Skip server-certificate chain verification. The cluster ships a
    /// self-signed api-server cert (the CA file is a copy of the leaf), which
    /// rustls rejects as `CaUsedAsEndEntity`. Matches the repo convention for
    /// the other in-cluster api-server clients (e.g. kubectl).
    pub insecure_skip_tls_verify: bool,
}

impl Default for HttpMetricsConfig {
    fn default() -> Self {
        Self {
            api_server_url: "https://api-server:6443".to_string(),
            ca_cert_path: "/etc/kubernetes/pki/ca.crt".to_string(),
            client_cert_path: "/etc/kubernetes/pki/api-server.crt".to_string(),
            client_key_path: "/etc/kubernetes/pki/api-server.key".to_string(),
            insecure_skip_tls_verify: true,
        }
    }
}

pub struct HttpMetricsClient {
    base: String,
    http: Client,
}

impl HttpMetricsClient {
    pub fn new(cfg: HttpMetricsConfig) -> Result<Self> {
        // rustls 0.23 requires a process-default CryptoProvider before any
        // ClientConfig is built. The controller-manager binary never constructs
        // a TlsConfig (unlike the api-server), so nothing installs one and
        // reqwest's `.build()` fails with a "builder error". Install the
        // aws-lc-rs provider idempotently here (no-op if already set).
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );

        let mut identity_pem = std::fs::read(&cfg.client_cert_path)
            .map_err(|e| anyhow::anyhow!("read client cert {}: {e}", cfg.client_cert_path))?;
        let mut key = std::fs::read(&cfg.client_key_path)
            .map_err(|e| anyhow::anyhow!("read client key {}: {e}", cfg.client_key_path))?;
        identity_pem.push(b'\n');
        identity_pem.append(&mut key);

        // Force the rustls backend: another workspace crate (kubectl) pulls
        // reqwest with default features, so native-tls is unified in and would
        // otherwise be the default backend — incompatible with the rustls
        // identity built by `Identity::from_pem`.
        let mut builder = Client::builder()
            .use_rustls_tls()
            .identity(reqwest::Identity::from_pem(&identity_pem)?);
        if cfg.insecure_skip_tls_verify {
            builder = builder.danger_accept_invalid_certs(true);
        } else {
            let ca = std::fs::read(&cfg.ca_cert_path)
                .map_err(|e| anyhow::anyhow!("read CA {}: {e}", cfg.ca_cert_path))?;
            builder = builder.add_root_certificate(reqwest::Certificate::from_pem(&ca)?);
        }
        let http = builder.build()?;
        Ok(Self {
            base: cfg.api_server_url.trim_end_matches('/').to_string(),
            http,
        })
    }
}

#[async_trait]
impl MetricsClient for HttpMetricsClient {
    async fn get_resource_metric(
        &self,
        resource: &str,
        namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        let url = format!(
            "{}/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods",
            self.base, namespace
        );
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let mut info = PodMetricsInfo::new();
        let now = Utc::now();
        if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let name = item
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string();
                let mut usage = 0i64;
                if let Some(containers) = item.get("containers").and_then(|c| c.as_array()) {
                    for c in containers {
                        if let Some(q) = c
                            .get("usage")
                            .and_then(|u| u.get(resource))
                            .and_then(|v| v.as_str())
                        {
                            usage += parse_quantity_milli(q, resource);
                        }
                    }
                }
                // PLACEHOLDER: real utilization = usage * 100 / pod-request, but
                // metrics.k8s.io here only reports usage (and rusternetes
                // synthesizes usage = request), so we cannot compute a true
                // percentage without fetching each pod's resource requests.
                // Hardcoded 100% until that is wired — tracked in
                // indyjonesnl/rusternetes#1077. Resource-utilization HPAs are
                // therefore not load-accurate on the live path yet.
                info.insert(
                    name,
                    PodMetric {
                        value: usage,
                        utilization: Some(100),
                        timestamp: now,
                    },
                );
            }
        }
        if info.is_empty() {
            anyhow::bail!("no pod metrics for namespace {namespace}");
        }
        Ok(info)
    }

    async fn get_raw_metric(
        &self,
        metric: &str,
        namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        let url = format!(
            "{}/apis/custom.metrics.k8s.io/v1beta2/namespaces/{}/pods/*/{}",
            self.base, namespace, metric
        );
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let mut info = PodMetricsInfo::new();
        let now = Utc::now();
        if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let name = item
                    .get("describedObject")
                    .and_then(|o| o.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string();
                let value = item
                    .get("value")
                    .and_then(|v| v.as_str())
                    .map(|q| parse_quantity_milli(q, metric))
                    .unwrap_or(0);
                info.insert(
                    name,
                    PodMetric {
                        value,
                        utilization: None,
                        timestamp: now,
                    },
                );
            }
        }
        if info.is_empty() {
            anyhow::bail!("no custom pod metrics for {metric}");
        }
        Ok(info)
    }

    async fn get_object_metric(
        &self,
        metric: &str,
        namespace: &str,
        object_ref: &CrossVersionObjectReference,
    ) -> Result<(i64, DateTime<Utc>)> {
        let url = format!(
            "{}/apis/custom.metrics.k8s.io/v1beta2/namespaces/{}/{}/{}/{}",
            self.base,
            namespace,
            object_ref.kind.to_lowercase(),
            object_ref.name,
            metric
        );
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let value = body
            .get("value")
            .and_then(|v| v.as_str())
            .map(|q| parse_quantity_milli(q, metric))
            .ok_or_else(|| anyhow::anyhow!("object metric {metric} missing value"))?;
        Ok((value, Utc::now()))
    }

    async fn get_external_metric(
        &self,
        metric: &str,
        namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<(Vec<i64>, DateTime<Utc>)> {
        // external.metrics.k8s.io is not implemented server-side (deferred).
        let url = format!(
            "{}/apis/external.metrics.k8s.io/v1beta1/namespaces/{}/{}",
            self.base, namespace, metric
        );
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let values = body
            .get("items")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|i| i.get("value").and_then(|v| v.as_str()))
                    .map(|q| parse_quantity_milli(q, metric))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if values.is_empty() {
            anyhow::bail!("no external metric values for {metric}");
        }
        Ok((values, Utc::now()))
    }
}

/// Parse a k8s quantity to an integer. CPU is returned in millicores; everything
/// else as a whole number (best-effort; strips known suffixes).
fn parse_quantity_milli(q: &str, resource: &str) -> i64 {
    let q = q.trim();
    if resource == "cpu" {
        if let Some(stripped) = q.strip_suffix('m') {
            return stripped.parse::<i64>().unwrap_or(0);
        }
        if let Ok(cores) = q.parse::<f64>() {
            return (cores * 1000.0) as i64;
        }
    }
    let digits: String = q.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<i64>().unwrap_or(0)
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
