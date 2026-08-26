use crate::client::ApiClient;
use crate::types::TopCommands;
use anyhow::Result;
use serde::Deserialize;

#[derive(Deserialize)]
struct NodeMetrics {
    metadata: MetricsMetadata,
    usage: ResourceUsage,
}

#[derive(Deserialize)]
struct PodMetrics {
    metadata: MetricsMetadata,
    containers: Vec<ContainerMetrics>,
}

#[derive(Deserialize)]
struct MetricsMetadata {
    name: String,
}

#[derive(Deserialize)]
struct ResourceUsage {
    cpu: String,
    memory: String,
}

#[derive(Deserialize)]
struct ContainerMetrics {
    name: String,
    usage: ResourceUsage,
}

/// Execute top commands for resource usage
pub async fn execute(
    client: &ApiClient,
    command: TopCommands,
    default_namespace: &str,
) -> Result<()> {
    match command {
        TopCommands::Node { name, selector: _ } => {
            // Try to fetch node metrics from metrics server API
            let path = if let Some(n) = name {
                format!("/apis/metrics.k8s.io/v1beta1/nodes/{}", n)
            } else {
                "/apis/metrics.k8s.io/v1beta1/nodes".to_string()
            };

            match client.get::<serde_json::Value>(&path).await {
                Ok(response) => {
                    if let Some(items) = response.get("items").and_then(|i| i.as_array()) {
                        println!(
                            "{:<20} {:<15} {:<15}",
                            "NAME", "CPU(cores)", "MEMORY(bytes)"
                        );
                        for item in items {
                            if let Ok(metrics) = serde_json::from_value::<NodeMetrics>(item.clone())
                            {
                                println!(
                                    "{:<20} {:<15} {:<15}",
                                    metrics.metadata.name, metrics.usage.cpu, metrics.usage.memory
                                );
                            }
                        }
                    } else if let Ok(metrics) = serde_json::from_value::<NodeMetrics>(response) {
                        println!(
                            "{:<20} {:<15} {:<15}",
                            "NAME", "CPU(cores)", "MEMORY(bytes)"
                        );
                        println!(
                            "{:<20} {:<15} {:<15}",
                            metrics.metadata.name, metrics.usage.cpu, metrics.usage.memory
                        );
                    }
                }
                Err(_) => {
                    println!(
                        "Error: Metrics API not available. Metrics Server might not be installed."
                    );
                    println!("Install metrics-server: kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml");
                }
            }
        }
        TopCommands::Pod {
            name,
            namespace,
            all_namespaces,
            selector: _,
            containers,
        } => {
            let ns = if all_namespaces {
                None
            } else {
                Some(namespace.as_deref().unwrap_or(default_namespace))
            };

            let path = if let Some(n) = name {
                format!(
                    "/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods/{}",
                    ns.unwrap_or("default"),
                    n
                )
            } else if let Some(namespace) = ns {
                format!("/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods", namespace)
            } else {
                "/apis/metrics.k8s.io/v1beta1/pods".to_string()
            };

            match client.get::<serde_json::Value>(&path).await {
                Ok(response) => {
                    if let Some(items) = response.get("items").and_then(|i| i.as_array()) {
                        if containers {
                            println!(
                                "{:<30} {:<20} {:<15} {:<15}",
                                "POD", "CONTAINER", "CPU(cores)", "MEMORY(bytes)"
                            );
                        } else {
                            println!("{:<30} {:<15} {:<15}", "POD", "CPU(cores)", "MEMORY(bytes)");
                        }

                        for item in items {
                            if let Ok(metrics) = serde_json::from_value::<PodMetrics>(item.clone())
                            {
                                if containers {
                                    for container in &metrics.containers {
                                        println!(
                                            "{:<30} {:<20} {:<15} {:<15}",
                                            metrics.metadata.name,
                                            container.name,
                                            container.usage.cpu,
                                            container.usage.memory
                                        );
                                    }
                                } else {
                                    let total_cpu: String = metrics
                                        .containers
                                        .first()
                                        .map(|c| c.usage.cpu.clone())
                                        .unwrap_or_default();
                                    let total_mem: String = metrics
                                        .containers
                                        .first()
                                        .map(|c| c.usage.memory.clone())
                                        .unwrap_or_default();
                                    println!(
                                        "{:<30} {:<15} {:<15}",
                                        metrics.metadata.name, total_cpu, total_mem
                                    );
                                }
                            }
                        }
                    } else if let Ok(metrics) = serde_json::from_value::<PodMetrics>(response) {
                        if containers {
                            println!(
                                "{:<30} {:<20} {:<15} {:<15}",
                                "POD", "CONTAINER", "CPU(cores)", "MEMORY(bytes)"
                            );
                            for container in &metrics.containers {
                                println!(
                                    "{:<30} {:<20} {:<15} {:<15}",
                                    metrics.metadata.name,
                                    container.name,
                                    container.usage.cpu,
                                    container.usage.memory
                                );
                            }
                        } else {
                            println!("{:<30} {:<15} {:<15}", "POD", "CPU(cores)", "MEMORY(bytes)");
                            let total_cpu: String = metrics
                                .containers
                                .first()
                                .map(|c| c.usage.cpu.clone())
                                .unwrap_or_default();
                            let total_mem: String = metrics
                                .containers
                                .first()
                                .map(|c| c.usage.memory.clone())
                                .unwrap_or_default();
                            println!(
                                "{:<30} {:<15} {:<15}",
                                metrics.metadata.name, total_cpu, total_mem
                            );
                        }
                    }
                }
                Err(_) => {
                    println!(
                        "Error: Metrics API not available. Metrics Server might not be installed."
                    );
                    println!("Install metrics-server: kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml");
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_metrics_deserialization() {
        let json = r#"{
            "metadata": {"name": "node-1"},
            "usage": {"cpu": "250m", "memory": "1024Mi"}
        }"#;
        let metrics: NodeMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.metadata.name, "node-1");
        assert_eq!(metrics.usage.cpu, "250m");
        assert_eq!(metrics.usage.memory, "1024Mi");
    }

    #[test]
    fn test_pod_metrics_deserialization() {
        let json = r#"{
            "metadata": {"name": "web-pod"},
            "containers": [
                {"name": "nginx", "usage": {"cpu": "100m", "memory": "64Mi"}},
                {"name": "sidecar", "usage": {"cpu": "50m", "memory": "32Mi"}}
            ]
        }"#;
        let metrics: PodMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.metadata.name, "web-pod");
        assert_eq!(metrics.containers.len(), 2);
        assert_eq!(metrics.containers[0].name, "nginx");
        assert_eq!(metrics.containers[1].usage.cpu, "50m");
    }

    #[test]
    fn test_metrics_api_path_node() {
        let path = "/apis/metrics.k8s.io/v1beta1/nodes";
        assert!(path.contains("metrics.k8s.io"));
    }

    #[test]
    fn test_metrics_api_path_specific_node() {
        let name = "node-1";
        let path = format!("/apis/metrics.k8s.io/v1beta1/nodes/{}", name);
        assert_eq!(path, "/apis/metrics.k8s.io/v1beta1/nodes/node-1");
    }

    #[test]
    fn test_metrics_api_path_pod_in_namespace() {
        let ns = "default";
        let path = format!("/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods", ns);
        assert_eq!(path, "/apis/metrics.k8s.io/v1beta1/namespaces/default/pods");
    }

    #[test]
    fn test_container_metrics_deserialization() {
        let json = r#"{"name": "sidecar", "usage": {"cpu": "10m", "memory": "16Mi"}}"#;
        let metrics: ContainerMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.name, "sidecar");
        assert_eq!(metrics.usage.cpu, "10m");
        assert_eq!(metrics.usage.memory, "16Mi");
    }

    #[test]
    fn test_pod_metrics_empty_containers() {
        let json = r#"{
            "metadata": {"name": "empty-pod"},
            "containers": []
        }"#;
        let metrics: PodMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.metadata.name, "empty-pod");
        assert!(metrics.containers.is_empty());
    }
}
