//! DownwardAPI field/resource resolution — pure helpers.
//!
//! This module owns the pure logic that maps a Kubernetes DownwardAPI
//! `fieldRef.fieldPath` or `resourceFieldRef` to its rendered string value.
//! It is extracted into its own module so the invariants pinned by upstream
//! conformance tests (`test/e2e/common/node/downwardapi.go` and
//! `test/e2e/common/storage/downwardapi_volume.go`) can be verified without
//! a live Docker daemon.
//!
//! The runtime in [`crate::runtime`] uses methods with identical semantics
//! (`Runtime::get_pod_field_value`, `Runtime::get_container_resource_value`)
//! — those wrappers stay private to the runtime, while these pure functions
//! are the conformance-test surface.
//!
//! K8s references:
//!   - `pkg/kubelet/kubelet_pods.go::podFieldSelectorRuntimeValue`
//!   - `pkg/api/v1/resource/helpers.go::ExtractContainerResourceValue`

use rusternetes_common::resources::{Pod, ResourceFieldSelector};

use crate::runtime::{parse_cpu_quantity, parse_memory_quantity};

/// Error returned when a DownwardAPI field path or resource selector is
/// unsupported or cannot be resolved against the supplied pod.
#[derive(Debug, PartialEq, Eq)]
pub enum DownwardError {
    /// The supplied `fieldRef.fieldPath` is not one the kubelet supports
    /// (e.g. `spec.unknownField`).
    UnsupportedField(String),
    /// The supplied `resourceFieldRef.resource` is not one the kubelet
    /// supports (e.g. `limits.unknown`).
    UnsupportedResource(String),
    /// The pod has no `spec` (degenerate object — should never reach the
    /// kubelet, but defensive in tests).
    MissingSpec,
    /// `resourceFieldRef.containerName` was set but no such container
    /// exists in the pod spec.
    ContainerNotFound(String),
    /// `resourceFieldRef.containerName` was unset and the pod has no
    /// containers (degenerate).
    NoContainers,
}

impl std::fmt::Display for DownwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedField(p) => write!(f, "Unsupported field path: {p}"),
            Self::UnsupportedResource(r) => write!(f, "Unsupported resource field: {r}"),
            Self::MissingSpec => f.write_str("Pod has no spec"),
            Self::ContainerNotFound(n) => write!(f, "Container {n} not found"),
            Self::NoContainers => f.write_str("Pod has no containers"),
        }
    }
}

impl std::error::Error for DownwardError {}

/// Resolve a DownwardAPI `fieldRef.fieldPath` against the pod.
///
/// Supported paths (mirrors upstream
/// `pkg/kubelet/kubelet_pods.go::podFieldSelectorRuntimeValue`):
///
/// - `metadata.name`, `metadata.namespace`, `metadata.uid`
/// - `metadata.labels`, `metadata.annotations` (rendered as
///   `key="value"\n` lines, sorted by key)
/// - `metadata.labels['key']`, `metadata.annotations['key']` (single value)
/// - `spec.nodeName`, `spec.serviceAccountName`
/// - `status.podIP`, `status.hostIP`
pub fn resolve_pod_field(pod: &Pod, field_path: &str) -> Result<String, DownwardError> {
    let value = match field_path {
        "metadata.name" => pod.metadata.name.clone(),
        "metadata.namespace" => pod
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        "metadata.uid" => pod.metadata.uid.clone(),
        "spec.nodeName" => pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .unwrap_or_default(),
        "spec.serviceAccountName" => pod
            .spec
            .as_ref()
            .and_then(|s| s.service_account_name.clone())
            .unwrap_or_else(|| "default".to_string()),
        "status.podIP" => pod
            .status
            .as_ref()
            .and_then(|s| s.pod_ip.clone())
            .unwrap_or_default(),
        "status.hostIP" => pod
            .status
            .as_ref()
            .and_then(|s| s.host_ip.clone())
            .unwrap_or_else(|| "127.0.0.1".to_string()),
        "metadata.labels" => render_kv_map(pod.metadata.labels.as_ref()),
        "metadata.annotations" => render_kv_map(pod.metadata.annotations.as_ref()),
        other => {
            if let Some(key) = strip_bracket_key(other, "metadata.labels[") {
                pod.metadata
                    .labels
                    .as_ref()
                    .and_then(|m| m.get(key))
                    .cloned()
                    .unwrap_or_default()
            } else if let Some(key) = strip_bracket_key(other, "metadata.annotations[") {
                pod.metadata
                    .annotations
                    .as_ref()
                    .and_then(|m| m.get(key))
                    .cloned()
                    .unwrap_or_default()
            } else {
                return Err(DownwardError::UnsupportedField(other.to_string()));
            }
        }
    };
    Ok(value)
}

/// Resolve a DownwardAPI `resourceFieldRef` against the pod.
///
/// Mirrors upstream
/// `pkg/api/v1/resource/helpers.go::ExtractContainerResourceValue`:
/// - CPU resources return millicores (or cores when divisor == "1"), with
///   ceiling division.
/// - Memory / ephemeral-storage / hugepages return bytes (or scaled units
///   under a divisor), with ceiling division.
/// - Unset limits default to node allocatable (4 cores / 8 GiB) per the
///   `should provide default limits.*` conformance variants.
///
/// `divisor` of `"0"` or missing means default (== `"1"`).
pub fn resolve_container_resource(
    pod: &Pod,
    sel: &ResourceFieldSelector,
) -> Result<String, DownwardError> {
    let spec = pod.spec.as_ref().ok_or(DownwardError::MissingSpec)?;

    let container = if let Some(ref name) = sel.container_name {
        spec.containers
            .iter()
            .find(|c| &c.name == name)
            .ok_or_else(|| DownwardError::ContainerNotFound(name.clone()))?
    } else {
        spec.containers.first().ok_or(DownwardError::NoContainers)?
    };

    let is_cpu = sel.resource.contains("cpu") || sel.resource.contains("hugepages");
    let is_memory = sel.resource.contains("memory") || sel.resource.contains("ephemeral-storage");

    let raw = match sel.resource.as_str() {
        "limits.cpu" => lookup_resource(container, true, "cpu"),
        "limits.memory" => lookup_resource(container, true, "memory"),
        "limits.ephemeral-storage" => lookup_resource(container, true, "ephemeral-storage"),
        "requests.cpu" => lookup_resource(container, false, "cpu"),
        "requests.memory" => lookup_resource(container, false, "memory"),
        "requests.ephemeral-storage" => lookup_resource(container, false, "ephemeral-storage"),
        other => return Err(DownwardError::UnsupportedResource(other.to_string())),
    };

    let divisor_str = sel.divisor.as_deref().unwrap_or("0");

    if is_cpu {
        // Default node allocatable: 4 cores = 4000m.
        let millicores = raw.as_deref().map(parse_cpu_quantity).unwrap_or(4000);
        let divisor_millicores = if divisor_str == "0" || divisor_str == "1" {
            1000
        } else {
            parse_cpu_quantity(divisor_str).max(1)
        };
        let result = ceil_div(millicores, divisor_millicores);
        Ok(result.to_string())
    } else if is_memory {
        // Default node allocatable: 8 GiB.
        let bytes = raw
            .as_deref()
            .map(parse_memory_quantity)
            .unwrap_or(8 * 1024 * 1024 * 1024);
        let divisor_bytes = if divisor_str == "0" || divisor_str == "1" {
            1
        } else {
            parse_memory_quantity(divisor_str).max(1)
        };
        let result = ceil_div(bytes, divisor_bytes);
        Ok(result.to_string())
    } else {
        Ok(raw.unwrap_or_else(|| "0".to_string()))
    }
}

/// Ceiling division for non-negative integers, matching K8s
/// `resource.Quantity.AsScale`'s rounding mode used by
/// `ExtractContainerResourceValue`.
fn ceil_div(numerator: i64, denominator: i64) -> i64 {
    (numerator + denominator - 1) / denominator
}

fn lookup_resource(
    container: &rusternetes_common::resources::Container,
    limits: bool,
    key: &str,
) -> Option<String> {
    let res = container.resources.as_ref()?;
    let map = if limits {
        res.limits.as_ref()
    } else {
        res.requests.as_ref()
    }?;
    map.get(key).cloned()
}

fn render_kv_map(map: Option<&std::collections::HashMap<String, String>>) -> String {
    let Some(map) = map else {
        return String::new();
    };
    let mut pairs: Vec<_> = map.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let mut out = pairs
        .iter()
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect::<Vec<_>>()
        .join("\n");
    if !out.is_empty() {
        out.push('\n');
    }
    out
}

/// Strip a `prefix[` and trailing `']` to recover the bracketed key in
/// `metadata.labels['key']` / `metadata.annotations['key']`. Returns
/// `None` when the path does not match.
fn strip_bracket_key<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = path.strip_prefix(prefix)?;
    let inner = rest.strip_suffix(']')?;
    // K8s allows both single and double quotes, plus unquoted keys.
    let unquoted = inner
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| inner.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(inner);
    Some(unquoted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Container, Pod, PodSpec, PodStatus};
    use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
    use std::collections::HashMap;

    fn make_pod() -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("p").with_namespace("ns"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "c".to_string(),
                    image: "x".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        }
    }

    #[test]
    fn metadata_name_resolves() {
        let p = make_pod();
        assert_eq!(resolve_pod_field(&p, "metadata.name").unwrap(), "p");
    }

    #[test]
    fn metadata_namespace_defaults_to_default_when_missing() {
        let mut p = make_pod();
        p.metadata.namespace = None;
        assert_eq!(
            resolve_pod_field(&p, "metadata.namespace").unwrap(),
            "default"
        );
    }

    #[test]
    fn unknown_field_returns_unsupported_error() {
        let p = make_pod();
        let err = resolve_pod_field(&p, "spec.unknownField").unwrap_err();
        assert!(matches!(err, DownwardError::UnsupportedField(_)));
    }

    #[test]
    fn status_host_ip_defaults_to_loopback() {
        let p = make_pod();
        assert_eq!(resolve_pod_field(&p, "status.hostIP").unwrap(), "127.0.0.1");
    }

    #[test]
    fn labels_subscript_resolves_value() {
        let mut p = make_pod();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());
        p.metadata.labels = Some(labels);
        assert_eq!(
            resolve_pod_field(&p, "metadata.labels['app']").unwrap(),
            "web"
        );
    }

    #[test]
    fn limits_cpu_default_to_node_allocatable() {
        let p = make_pod();
        let sel = ResourceFieldSelector {
            container_name: Some("c".into()),
            resource: "limits.cpu".into(),
            divisor: None,
        };
        // 4000 millicores ÷ 1000 (cores divisor) = 4
        assert_eq!(resolve_container_resource(&p, &sel).unwrap(), "4");
    }

    #[test]
    fn limits_memory_explicit_returns_bytes() {
        let mut p = make_pod();
        let mut limits = HashMap::new();
        limits.insert("memory".to_string(), "64Mi".to_string());
        if let Some(ref mut spec) = p.spec {
            spec.containers[0].resources = Some(ResourceRequirements {
                limits: Some(limits),
                requests: None,
                claims: None,
            });
        }
        let sel = ResourceFieldSelector {
            container_name: Some("c".into()),
            resource: "limits.memory".into(),
            divisor: None,
        };
        // 64 MiB = 67_108_864 bytes
        assert_eq!(
            resolve_container_resource(&p, &sel).unwrap(),
            (64 * 1024 * 1024).to_string()
        );
    }

    #[test]
    fn pod_status_pod_ip_round_trips() {
        let mut p = make_pod();
        p.status = Some(PodStatus {
            pod_ip: Some("10.244.0.5".into()),
            ..Default::default()
        });
        assert_eq!(resolve_pod_field(&p, "status.podIP").unwrap(), "10.244.0.5");
    }
}
