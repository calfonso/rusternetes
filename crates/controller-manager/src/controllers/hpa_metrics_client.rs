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

    /// Per-pod resource utilization for a single named container (cpu/memory),
    /// for the ContainerResource HPA metric type. Usage and the request
    /// denominator are both scoped to `container`.
    async fn get_container_resource_metric(
        &self,
        resource: &str,
        container: &str,
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

    async fn get_container_resource_metric(
        &self,
        resource: &str,
        container: &str,
        _namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        // Test double keys per-container readings as "{resource}/{container}",
        // falling back to the plain resource map for convenience.
        self.resource
            .get(&format!("{resource}/{container}"))
            .or_else(|| self.resource.get(resource))
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("no container resource metric for {resource}/{container}")
            })
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

    /// Shared resource-utilization path for both whole-pod (`container == None`)
    /// and single-container (`container == Some`) metrics. Reads usage from
    /// `metrics.k8s.io`, divides by each pod's resource `requests` to produce a
    /// true utilization percentage.
    async fn resource_metric_impl(
        &self,
        resource: &str,
        container: Option<&str>,
        namespace: &str,
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

        // Per-pod request denominator (sum of the relevant containers' requests).
        let requests = self
            .fetch_pod_requests(resource, container, namespace)
            .await;

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
                        // For ContainerResource, only the named container counts.
                        if let Some(want) = container {
                            let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                            if cname != want {
                                continue;
                            }
                        }
                        if let Some(q) = c
                            .get("usage")
                            .and_then(|u| u.get(resource))
                            .and_then(|v| v.as_str())
                        {
                            usage += parse_quantity_milli(q, resource);
                        }
                    }
                }
                // True utilization = usage * 100 / request. If the request
                // denominator is missing/zero we cannot express a percentage,
                // so leave it None (the replica calculator skips such pods).
                let utilization = requests
                    .get(&name)
                    .filter(|r| **r > 0)
                    .map(|r| ((usage * 100) / *r) as i32);
                info.insert(
                    name,
                    PodMetric {
                        value: usage,
                        utilization,
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

    /// Fetch each pod's resource `requests` denominator for `resource`, summed
    /// over either all containers (`container == None`) or just the named
    /// container. Returns a pod-name -> request (millicores for cpu, bytes for
    /// memory) map. Best-effort: a failed list yields an empty map (utilization
    /// then reported as unknown rather than fabricated).
    async fn fetch_pod_requests(
        &self,
        resource: &str,
        container: Option<&str>,
        namespace: &str,
    ) -> HashMap<String, i64> {
        let mut out = HashMap::new();
        let url = format!("{}/api/v1/namespaces/{}/pods", self.base, namespace);
        let body: serde_json::Value = match self
            .http
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(resp) => match resp.json().await {
                Ok(b) => b,
                Err(_) => return out,
            },
            Err(_) => return out,
        };
        if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let name = item
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string();
                let mut sum = 0i64;
                if let Some(containers) = item
                    .get("spec")
                    .and_then(|s| s.get("containers"))
                    .and_then(|c| c.as_array())
                {
                    for c in containers {
                        if let Some(want) = container {
                            let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                            if cname != want {
                                continue;
                            }
                        }
                        if let Some(q) = c
                            .get("resources")
                            .and_then(|r| r.get("requests"))
                            .and_then(|req| req.get(resource))
                            .and_then(|v| v.as_str())
                        {
                            sum += parse_quantity_milli(q, resource);
                        }
                    }
                }
                if sum > 0 {
                    out.insert(name, sum);
                }
            }
        }
        out
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
        self.resource_metric_impl(resource, None, namespace).await
    }

    async fn get_container_resource_metric(
        &self,
        resource: &str,
        container: &str,
        namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        self.resource_metric_impl(resource, Some(container), namespace)
            .await
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

/// Parse a k8s quantity to an integer in units consistent per resource so a
/// usage/request ratio is meaningful: CPU in millicores, memory in bytes
/// (binary `Ki/Mi/Gi/...` and SI `k/M/G/...` suffixes honoured), and any other
/// metric as a best-effort whole number (leading digits).
fn parse_quantity_milli(q: &str, resource: &str) -> i64 {
    let q = q.trim();
    if resource == "cpu" {
        if let Some(stripped) = q.strip_suffix('m') {
            return stripped.parse::<i64>().unwrap_or(0);
        }
        if let Ok(cores) = q.parse::<f64>() {
            return (cores * 1000.0) as i64;
        }
        return 0;
    }
    if resource == "memory" {
        return parse_memory_bytes(q);
    }
    let digits: String = q.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<i64>().unwrap_or(0)
}

/// Parse a k8s memory quantity into bytes. Honours binary (Ki, Mi, Gi, Ti, Pi,
/// Ei) and decimal SI (k, M, G, T, P, E) suffixes; a bare number is bytes.
fn parse_memory_bytes(q: &str) -> i64 {
    let q = q.trim();
    let (num, mult): (&str, f64) = if let Some(n) = q.strip_suffix("Ki") {
        (n, 1024.0)
    } else if let Some(n) = q.strip_suffix("Mi") {
        (n, 1024f64.powi(2))
    } else if let Some(n) = q.strip_suffix("Gi") {
        (n, 1024f64.powi(3))
    } else if let Some(n) = q.strip_suffix("Ti") {
        (n, 1024f64.powi(4))
    } else if let Some(n) = q.strip_suffix("Pi") {
        (n, 1024f64.powi(5))
    } else if let Some(n) = q.strip_suffix("Ei") {
        (n, 1024f64.powi(6))
    } else if let Some(n) = q.strip_suffix('k') {
        (n, 1e3)
    } else if let Some(n) = q.strip_suffix('M') {
        (n, 1e6)
    } else if let Some(n) = q.strip_suffix('G') {
        (n, 1e9)
    } else if let Some(n) = q.strip_suffix('T') {
        (n, 1e12)
    } else if let Some(n) = q.strip_suffix('P') {
        (n, 1e15)
    } else if let Some(n) = q.strip_suffix('E') {
        (n, 1e18)
    } else {
        (q, 1.0)
    };
    num.trim()
        .parse::<f64>()
        .map(|v| (v * mult) as i64)
        .unwrap_or(0)
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

    #[test]
    fn parse_quantity_cpu_and_memory_units() {
        // CPU -> millicores
        assert_eq!(parse_quantity_milli("500m", "cpu"), 500);
        assert_eq!(parse_quantity_milli("2", "cpu"), 2000);
        // Memory -> bytes, binary + SI suffixes
        assert_eq!(parse_quantity_milli("128Mi", "memory"), 128 * 1024 * 1024);
        assert_eq!(parse_quantity_milli("1Gi", "memory"), 1024 * 1024 * 1024);
        assert_eq!(parse_quantity_milli("1M", "memory"), 1_000_000);
        assert_eq!(parse_quantity_milli("1048576", "memory"), 1_048_576);
        // Other metrics -> leading digits, best-effort
        assert_eq!(parse_quantity_milli("42", "requests-per-second"), 42);
    }

    #[tokio::test]
    async fn fake_container_resource_metric_lookup() {
        let mut fake = FakeMetricsClient::new();
        fake.resource.insert(
            "cpu/app".to_string(),
            FakeMetricsClient::pods_info(&[("p1", 50, Some(75))]),
        );
        let sel = LabelSelector::default();
        let info = fake
            .get_container_resource_metric("cpu", "app", "default", &sel)
            .await
            .unwrap();
        assert_eq!(info["p1"].utilization, Some(75));
        // Missing container -> error.
        assert!(fake
            .get_container_resource_metric("cpu", "missing", "default", &sel)
            .await
            .is_err());
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
