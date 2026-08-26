use crate::client::ApiClient;
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Build the API path to GET a resource by type and name.
fn resource_path(resource_type: &str, namespace: &str, name: &str) -> Result<String> {
    match resource_type {
        "pod" | "pods" | "po" => Ok(format!(
            "/api/v1/namespaces/{}/pods/{}",
            namespace, name
        )),
        "service" | "services" | "svc" => Ok(format!(
            "/api/v1/namespaces/{}/services/{}",
            namespace, name
        )),
        "replicationcontroller" | "replicationcontrollers" | "rc" => Ok(format!(
            "/api/v1/namespaces/{}/replicationcontrollers/{}",
            namespace, name
        )),
        "deployment" | "deployments" | "deploy" => Ok(format!(
            "/apis/apps/v1/namespaces/{}/deployments/{}",
            namespace, name
        )),
        "replicaset" | "replicasets" | "rs" => Ok(format!(
            "/apis/apps/v1/namespaces/{}/replicasets/{}",
            namespace, name
        )),
        _ => anyhow::bail!(
            "cannot expose a {}; supported resource types: pod, service, replicationcontroller, deployment, replicaset",
            resource_type
        ),
    }
}

/// Extract selector labels from a resource JSON object.
///
/// - For Pods: uses `.metadata.labels`
/// - For Services: uses `.spec.selector`
/// - For Deployments/ReplicaSets: uses `.spec.selector.matchLabels`
/// - For ReplicationControllers: uses `.spec.selector`
pub fn extract_selector(resource_type: &str, resource: &Value) -> Result<BTreeMap<String, String>> {
    let selector_value = match resource_type {
        "pod" | "pods" | "po" => resource
            .pointer("/metadata/labels")
            .ok_or_else(|| anyhow::anyhow!("pod has no labels to use as selector"))?,
        "service" | "services" | "svc" => resource
            .pointer("/spec/selector")
            .ok_or_else(|| anyhow::anyhow!("service has no selector"))?,
        "replicationcontroller" | "replicationcontrollers" | "rc" => resource
            .pointer("/spec/selector")
            .ok_or_else(|| anyhow::anyhow!("replication controller has no selector"))?,
        "deployment" | "deployments" | "deploy" | "replicaset" | "replicasets" | "rs" => resource
            .pointer("/spec/selector/matchLabels")
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "resource selector is not convertible to a map-based selector (matchLabels required)"
                )
            })?,
        _ => anyhow::bail!("unsupported resource type for expose: {}", resource_type),
    };

    let map = selector_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("selector is not a JSON object"))?;

    let mut selector = BTreeMap::new();
    for (k, v) in map {
        if let Some(s) = v.as_str() {
            selector.insert(k.clone(), s.to_string());
        }
    }

    if selector.is_empty() {
        anyhow::bail!("couldn't retrieve selectors from the resource");
    }

    Ok(selector)
}

/// Extract container ports from a resource to auto-detect --port when not specified.
fn extract_ports(resource: &Value) -> Vec<i32> {
    let containers = resource
        .pointer("/spec/containers")
        .or_else(|| resource.pointer("/spec/template/spec/containers"));

    let mut ports = Vec::new();
    if let Some(Value::Array(containers)) = containers {
        for container in containers {
            if let Some(Value::Array(cports)) = container.get("ports") {
                for p in cports {
                    if let Some(port) = p.get("containerPort").and_then(|v| v.as_i64()) {
                        ports.push(port as i32);
                    }
                }
            }
        }
    }
    ports
}

/// Build a Service JSON object from the given parameters.
pub fn build_service(
    name: &str,
    namespace: &str,
    selector: &BTreeMap<String, String>,
    port: i32,
    target_port: Option<i32>,
    protocol: &str,
    service_type: Option<&str>,
) -> Value {
    let tp = target_port.unwrap_or(port);

    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": namespace
        },
        "spec": {
            "selector": selector,
            "ports": [
                {
                    "protocol": protocol,
                    "port": port,
                    "targetPort": tp
                }
            ]
        }
    });

    if let Some(svc_type) = service_type {
        service["spec"]["type"] = json!(svc_type);
    }

    service
}

/// Execute the `kubectl expose` command.
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    resource_name: &str,
    namespace: &str,
    port: Option<i32>,
    target_port: Option<i32>,
    protocol: &str,
    name: Option<&str>,
    service_type: Option<&str>,
) -> Result<()> {
    // 1. GET the resource from the API server
    let path = resource_path(resource_type, namespace, resource_name)?;
    let resource: Value = client.get(&path).await.map_err(|e| {
        anyhow::anyhow!(
            "failed to get resource {}/{}: {}",
            resource_type,
            resource_name,
            e
        )
    })?;

    // 2. Extract selector labels
    let selector = extract_selector(resource_type, &resource)?;

    // 3. Determine the port
    let port = match port {
        Some(p) => p,
        None => {
            let ports = extract_ports(&resource);
            match ports.len() {
                0 => anyhow::bail!(
                    "couldn't find port via --port flag or introspection; use --port to specify"
                ),
                1 => ports[0],
                _ => anyhow::bail!(
                    "resource has multiple ports ({}); use --port to specify which one",
                    ports
                        .iter()
                        .map(|p| p.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        }
    };

    // 4. Service name defaults to the resource name
    let service_name = name.unwrap_or(resource_name);

    // 5. Build the Service
    let service = build_service(
        service_name,
        namespace,
        &selector,
        port,
        target_port,
        protocol,
        service_type,
    );

    // 6. POST the Service to the API server
    let svc_path = format!("/api/v1/namespaces/{}/services", namespace);
    let result: Value = client
        .post(&svc_path, &service)
        .await
        .context("Failed to create service")?;

    let created_name = result
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or(service_name);
    println!("service/{} exposed", created_name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_selector_deployment() {
        let deployment = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "nginx",
                "labels": {"app": "nginx"}
            },
            "spec": {
                "selector": {
                    "matchLabels": {
                        "app": "nginx"
                    }
                },
                "template": {
                    "spec": {
                        "containers": [{"name": "nginx", "image": "nginx", "ports": [{"containerPort": 80}]}]
                    }
                }
            }
        });

        let selector = extract_selector("deployment", &deployment).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("app".to_string(), "nginx".to_string());
        assert_eq!(selector, expected);
    }

    #[test]
    fn test_extract_selector_pod() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "nginx",
                "labels": {
                    "app": "nginx",
                    "tier": "frontend"
                }
            },
            "spec": {
                "containers": [{"name": "nginx", "image": "nginx"}]
            }
        });

        let selector = extract_selector("pod", &pod).unwrap();
        assert_eq!(selector.get("app").unwrap(), "nginx");
        assert_eq!(selector.get("tier").unwrap(), "frontend");
        assert_eq!(selector.len(), 2);
    }

    #[test]
    fn test_extract_selector_replication_controller() {
        let rc = json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "nginx"},
            "spec": {
                "selector": {
                    "app": "nginx"
                },
                "template": {
                    "spec": {
                        "containers": [{"name": "nginx", "image": "nginx"}]
                    }
                }
            }
        });

        let selector = extract_selector("rc", &rc).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("app".to_string(), "nginx".to_string());
        assert_eq!(selector, expected);
    }

    #[test]
    fn test_extract_selector_replicaset() {
        let rs = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {"name": "nginx"},
            "spec": {
                "selector": {
                    "matchLabels": {
                        "app": "nginx",
                        "version": "v1"
                    }
                }
            }
        });

        let selector = extract_selector("rs", &rs).unwrap();
        assert_eq!(selector.get("app").unwrap(), "nginx");
        assert_eq!(selector.get("version").unwrap(), "v1");
    }

    #[test]
    fn test_extract_selector_service() {
        let svc = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "nginx"},
            "spec": {
                "selector": {
                    "app": "nginx"
                },
                "ports": [{"port": 80}]
            }
        });

        let selector = extract_selector("svc", &svc).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("app".to_string(), "nginx".to_string());
        assert_eq!(selector, expected);
    }

    #[test]
    fn test_extract_selector_missing_labels_fails() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "nginx"},
            "spec": {}
        });

        let result = extract_selector("pod", &pod);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_service_basic() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "nginx".to_string());

        let svc = build_service("nginx", "default", &selector, 80, None, "TCP", None);

        assert_eq!(svc["metadata"]["name"], "nginx");
        assert_eq!(svc["metadata"]["namespace"], "default");
        assert_eq!(svc["spec"]["selector"]["app"], "nginx");
        assert_eq!(svc["spec"]["ports"][0]["port"], 80);
        assert_eq!(svc["spec"]["ports"][0]["targetPort"], 80);
        assert_eq!(svc["spec"]["ports"][0]["protocol"], "TCP");
        assert!(svc["spec"]["type"].is_null());
    }

    #[test]
    fn test_build_service_with_target_port() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "web".to_string());

        let svc = build_service(
            "web-svc",
            "production",
            &selector,
            80,
            Some(8080),
            "TCP",
            None,
        );

        assert_eq!(svc["spec"]["ports"][0]["port"], 80);
        assert_eq!(svc["spec"]["ports"][0]["targetPort"], 8080);
    }

    #[test]
    fn test_build_service_with_type() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "frontend".to_string());

        let svc = build_service(
            "frontend",
            "default",
            &selector,
            443,
            None,
            "TCP",
            Some("NodePort"),
        );

        assert_eq!(svc["spec"]["type"], "NodePort");
    }

    #[test]
    fn test_build_service_udp_protocol() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "dns".to_string());

        let svc = build_service("dns-svc", "kube-system", &selector, 53, None, "UDP", None);

        assert_eq!(svc["spec"]["ports"][0]["protocol"], "UDP");
    }

    #[test]
    fn test_build_service_with_custom_name() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "nginx".to_string());

        let svc = build_service(
            "my-custom-name",
            "default",
            &selector,
            80,
            None,
            "TCP",
            Some("LoadBalancer"),
        );

        assert_eq!(svc["metadata"]["name"], "my-custom-name");
        assert_eq!(svc["spec"]["type"], "LoadBalancer");
    }

    #[test]
    fn test_extract_ports_from_pod() {
        let pod = json!({
            "spec": {
                "containers": [{
                    "name": "nginx",
                    "ports": [{"containerPort": 80}, {"containerPort": 443}]
                }]
            }
        });

        let ports = extract_ports(&pod);
        assert_eq!(ports, vec![80, 443]);
    }

    #[test]
    fn test_extract_ports_from_deployment_template() {
        let deploy = json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "app",
                            "ports": [{"containerPort": 8080}]
                        }]
                    }
                }
            }
        });

        let ports = extract_ports(&deploy);
        assert_eq!(ports, vec![8080]);
    }

    #[test]
    fn test_extract_ports_no_ports() {
        let resource = json!({
            "spec": {
                "containers": [{
                    "name": "app"
                }]
            }
        });

        let ports = extract_ports(&resource);
        assert!(ports.is_empty());
    }

    #[test]
    fn test_resource_path_valid_types() {
        assert_eq!(
            resource_path("pod", "default", "nginx").unwrap(),
            "/api/v1/namespaces/default/pods/nginx"
        );
        assert_eq!(
            resource_path("deploy", "prod", "web").unwrap(),
            "/apis/apps/v1/namespaces/prod/deployments/web"
        );
        assert_eq!(
            resource_path("rc", "default", "app").unwrap(),
            "/api/v1/namespaces/default/replicationcontrollers/app"
        );
        assert_eq!(
            resource_path("rs", "default", "app").unwrap(),
            "/apis/apps/v1/namespaces/default/replicasets/app"
        );
        assert_eq!(
            resource_path("svc", "default", "app").unwrap(),
            "/api/v1/namespaces/default/services/app"
        );
    }

    #[test]
    fn test_resource_path_invalid_type() {
        assert!(resource_path("configmap", "default", "foo").is_err());
    }

    #[test]
    fn test_extract_selector_unsupported_type() {
        let resource = json!({"spec": {}});
        let result = extract_selector("configmap", &resource);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_selector_empty_labels_fails() {
        let pod = json!({
            "metadata": {
                "labels": {}
            }
        });
        let result = extract_selector("pod", &pod);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("selectors"));
    }

    #[test]
    fn test_extract_ports_multiple_containers() {
        let resource = json!({
            "spec": {
                "containers": [
                    {"name": "web", "ports": [{"containerPort": 80}]},
                    {"name": "api", "ports": [{"containerPort": 8080}]}
                ]
            }
        });
        let ports = extract_ports(&resource);
        assert_eq!(ports, vec![80, 8080]);
    }

    #[test]
    fn test_extract_ports_no_containers() {
        let resource = json!({"spec": {}});
        let ports = extract_ports(&resource);
        assert!(ports.is_empty());
    }

    #[test]
    fn test_build_service_multiple_selectors() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "web".to_string());
        selector.insert("version".to_string(), "v2".to_string());

        let svc = build_service(
            "web",
            "staging",
            &selector,
            443,
            Some(8443),
            "TCP",
            Some("ClusterIP"),
        );
        assert_eq!(svc["spec"]["selector"]["app"], "web");
        assert_eq!(svc["spec"]["selector"]["version"], "v2");
        assert_eq!(svc["spec"]["type"], "ClusterIP");
        assert_eq!(svc["metadata"]["namespace"], "staging");
    }

    #[test]
    fn test_resource_path_all_aliases() {
        // Test all aliases resolve correctly
        assert_eq!(
            resource_path("po", "default", "x").unwrap(),
            "/api/v1/namespaces/default/pods/x"
        );
        assert_eq!(
            resource_path("pods", "default", "x").unwrap(),
            "/api/v1/namespaces/default/pods/x"
        );
        assert_eq!(
            resource_path("services", "default", "x").unwrap(),
            "/api/v1/namespaces/default/services/x"
        );
        assert_eq!(
            resource_path("replicationcontrollers", "default", "x").unwrap(),
            "/api/v1/namespaces/default/replicationcontrollers/x"
        );
        assert_eq!(
            resource_path("deployments", "default", "x").unwrap(),
            "/apis/apps/v1/namespaces/default/deployments/x"
        );
        assert_eq!(
            resource_path("replicasets", "default", "x").unwrap(),
            "/apis/apps/v1/namespaces/default/replicasets/x"
        );
    }

    #[test]
    fn test_extract_selector_deployment_missing_match_labels() {
        let deploy = json!({
            "spec": {
                "selector": {
                    "matchExpressions": [{"key": "app", "operator": "In", "values": ["nginx"]}]
                }
            }
        });
        let result = extract_selector("deployment", &deploy);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_ports_empty_ports_array() {
        let resource = json!({
            "spec": {
                "containers": [{
                    "name": "app",
                    "ports": []
                }]
            }
        });
        let ports = extract_ports(&resource);
        assert!(ports.is_empty());
    }

    #[test]
    fn test_build_service_apiversion_and_kind() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "test".to_string());

        let svc = build_service("test", "default", &selector, 80, None, "TCP", None);
        assert_eq!(svc["apiVersion"], "v1");
        assert_eq!(svc["kind"], "Service");
    }

    #[test]
    fn test_extract_selector_service_missing_selector() {
        let svc = json!({
            "spec": {
                "ports": [{"port": 80}]
            }
        });
        let result = extract_selector("svc", &svc);
        assert!(result.is_err());
    }

    #[test]
    fn test_resource_path_rc_aliases() {
        // Both "replicationcontroller" and "rc" must resolve to the core v1 API path
        assert_eq!(
            resource_path("replicationcontroller", "default", "my-rc").unwrap(),
            "/api/v1/namespaces/default/replicationcontrollers/my-rc"
        );
        assert_eq!(
            resource_path("rc", "my-ns", "web-rc").unwrap(),
            "/api/v1/namespaces/my-ns/replicationcontrollers/web-rc"
        );
        assert_eq!(
            resource_path("replicationcontrollers", "prod", "app").unwrap(),
            "/api/v1/namespaces/prod/replicationcontrollers/app"
        );
    }

    #[test]
    fn test_extract_selector_rc_uses_spec_selector() {
        // RC selectors live at .spec.selector (flat map), not .spec.selector.matchLabels
        let rc = json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "web-rc"},
            "spec": {
                "selector": {
                    "app": "web",
                    "tier": "frontend"
                },
                "template": {
                    "spec": {
                        "containers": [{"name": "nginx", "image": "nginx"}]
                    }
                }
            }
        });

        // All three aliases should work
        for alias in &["replicationcontroller", "replicationcontrollers", "rc"] {
            let selector = extract_selector(alias, &rc).unwrap();
            assert_eq!(selector.get("app").unwrap(), "web");
            assert_eq!(selector.get("tier").unwrap(), "frontend");
            assert_eq!(selector.len(), 2);
        }
    }
}
