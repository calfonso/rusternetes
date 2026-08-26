use crate::client::{ApiClient, GetError};
use anyhow::Result;
use chrono::Utc;
use rusternetes_common::resources::{Deployment, Namespace, Node, Pod, Service};

pub async fn execute_enhanced(
    client: &ApiClient,
    resource_type: &str,
    name: Option<&str>,
    namespace: &str,
    selector: Option<&str>,
    all_namespaces: bool,
) -> Result<()> {
    if let Some(sel) = selector {
        println!("Selector: {}", sel);
        println!("Note: Selector-based describe not yet implemented");
        return Ok(());
    }
    if all_namespaces {
        println!("All namespaces");
        println!("Note: All-namespaces describe not yet implemented");
        return Ok(());
    }
    if let Some(n) = name {
        execute(client, resource_type, n, Some(namespace)).await
    } else {
        anyhow::bail!("Resource name required for describe");
    }
}

// Helper to convert GetError to anyhow::Error
fn map_get_error(err: GetError) -> anyhow::Error {
    match err {
        GetError::NotFound => anyhow::anyhow!("Resource not found"),
        GetError::Other(e) => e,
    }
}

fn format_duration(duration: chrono::Duration) -> String {
    let days = duration.num_days();
    if days > 0 {
        return format!("{}d", days);
    }
    let hours = duration.num_hours();
    if hours > 0 {
        return format!("{}h", hours);
    }
    let minutes = duration.num_minutes();
    if minutes > 0 {
        return format!("{}m", minutes);
    }
    let seconds = duration.num_seconds();
    format!("{}s", seconds)
}

pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: Option<&str>,
) -> Result<()> {
    let default_namespace = "default";
    let ns = namespace.unwrap_or(default_namespace);

    match resource_type {
        "pod" | "pods" => {
            let pod: Pod = client
                .get(&format!("/api/v1/namespaces/{}/pods/{}", ns, name))
                .await
                .map_err(map_get_error)?;
            describe_pod(&pod);
        }
        "service" | "services" | "svc" => {
            let service: Service = client
                .get(&format!("/api/v1/namespaces/{}/services/{}", ns, name))
                .await
                .map_err(map_get_error)?;
            describe_service(&service);
        }
        "deployment" | "deployments" | "deploy" => {
            let deployment: Deployment = client
                .get(&format!(
                    "/apis/apps/v1/namespaces/{}/deployments/{}",
                    ns, name
                ))
                .await
                .map_err(map_get_error)?;
            describe_deployment(&deployment);
        }
        "node" | "nodes" => {
            let node: Node = client
                .get(&format!("/api/v1/nodes/{}", name))
                .await
                .map_err(map_get_error)?;
            describe_node(&node);
        }
        "namespace" | "namespaces" | "ns" => {
            let namespace: Namespace = client
                .get(&format!("/api/v1/namespaces/{}", name))
                .await
                .map_err(map_get_error)?;
            describe_namespace(&namespace);
        }
        _ => anyhow::bail!("Unknown resource type: {}", resource_type),
    }

    Ok(())
}

fn describe_pod(pod: &Pod) {
    println!("Name:         {}", pod.metadata.name);
    println!(
        "Namespace:    {}",
        pod.metadata.namespace.as_deref().unwrap_or("default")
    );

    if let Some(labels) = &pod.metadata.labels {
        println!(
            "Labels:       {}",
            labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("\n              ")
        );
    }

    if let Some(annotations) = &pod.metadata.annotations {
        println!(
            "Annotations:  {}",
            annotations
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("\n              ")
        );
    }

    if let Some(status) = &pod.status {
        println!("Status:       {:?}", status.phase);
        if let Some(pod_ip) = &status.pod_ip {
            println!("IP:           {}", pod_ip);
        }
    }

    if let Some(spec) = &pod.spec {
        if let Some(node_name) = &spec.node_name {
            println!("Node:         {}", node_name);
        }

        println!("\nContainers:");
        for container in &spec.containers {
            println!("  {}:", container.name);
            println!("    Image:      {}", container.image);
            if let Some(ports) = &container.ports {
                if !ports.is_empty() {
                    println!(
                        "    Ports:      {}",
                        ports
                            .iter()
                            .map(|p| format!(
                                "{}/{}",
                                p.container_port,
                                p.protocol.as_deref().unwrap_or("TCP")
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
            if let Some(resources) = &container.resources {
                if let Some(limits) = &resources.limits {
                    println!("    Limits:");
                    for (k, v) in limits {
                        println!("      {}: {}", k, v);
                    }
                }
                if let Some(requests) = &resources.requests {
                    println!("    Requests:");
                    for (k, v) in requests {
                        println!("      {}: {}", k, v);
                    }
                }
            }
        }
    }

    if let Some(ts) = pod.metadata.creation_timestamp {
        let age = format_duration(Utc::now().signed_duration_since(ts));
        println!("\nAge:          {}", age);
    }
}

fn describe_service(service: &Service) {
    println!("Name:         {}", service.metadata.name);
    println!(
        "Namespace:    {}",
        service.metadata.namespace.as_deref().unwrap_or("default")
    );

    if let Some(labels) = &service.metadata.labels {
        println!(
            "Labels:       {}",
            labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("\n              ")
        );
    }

    if let Some(service_type) = &service.spec.service_type {
        println!("Type:         {:?}", service_type);
    } else {
        println!("Type:         ClusterIP");
    }

    if let Some(cluster_ip) = &service.spec.cluster_ip {
        println!("IP:           {}", cluster_ip);
    }

    println!(
        "Ports:        {}",
        service
            .spec
            .ports
            .iter()
            .map(|p| format!(
                "{}/{} -> {}",
                p.port,
                p.protocol.as_deref().unwrap_or("TCP"),
                match &p.target_port {
                    Some(rusternetes_common::resources::IntOrString::Int(tp)) => tp.to_string(),
                    Some(rusternetes_common::resources::IntOrString::String(tp)) => tp.clone(),
                    None => "default".to_string(),
                }
            ))
            .collect::<Vec<_>>()
            .join("\n              ")
    );

    if let Some(selector) = &service.spec.selector {
        if !selector.is_empty() {
            println!(
                "Selector:     {}",
                selector
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
    }

    if let Some(ts) = service.metadata.creation_timestamp {
        let age = format_duration(Utc::now().signed_duration_since(ts));
        println!("\nAge:          {}", age);
    }
}

fn describe_deployment(deployment: &Deployment) {
    println!("Name:         {}", deployment.metadata.name);
    println!(
        "Namespace:    {}",
        deployment
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default")
    );

    if let Some(labels) = &deployment.metadata.labels {
        println!(
            "Labels:       {}",
            labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("\n              ")
        );
    }

    println!(
        "Replicas:     {} desired",
        deployment.spec.replicas.unwrap_or(1)
    );

    if let Some(status) = &deployment.status {
        if let Some(ready) = status.ready_replicas {
            println!("              {} ready", ready);
        }
        if let Some(updated) = status.updated_replicas {
            println!("              {} updated", updated);
        }
        if let Some(available) = status.available_replicas {
            println!("              {} available", available);
        }
    }

    println!(
        "Selector:     match_labels={:?}",
        deployment.spec.selector.match_labels
    );

    if let Some(ts) = deployment.metadata.creation_timestamp {
        let age = format_duration(Utc::now().signed_duration_since(ts));
        println!("\nAge:          {}", age);
    }
}

fn describe_node(node: &Node) {
    println!("Name:         {}", node.metadata.name);

    if let Some(labels) = &node.metadata.labels {
        println!(
            "Labels:       {}",
            labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("\n              ")
        );
    }

    if let Some(status) = &node.status {
        if let Some(conditions) = &status.conditions {
            println!("\nConditions:");
            for condition in conditions {
                println!("  {}:  {}", condition.condition_type, condition.status);
            }
        }

        if let Some(addresses) = &status.addresses {
            println!("\nAddresses:");
            for addr in addresses {
                println!("  {}: {}", addr.address_type, addr.address);
            }
        }

        if let Some(capacity) = &status.capacity {
            println!("\nCapacity:");
            for (k, v) in capacity {
                println!("  {}: {}", k, v);
            }
        }

        if let Some(allocatable) = &status.allocatable {
            println!("\nAllocatable:");
            for (k, v) in allocatable {
                println!("  {}: {}", k, v);
            }
        }
    }

    if let Some(ts) = node.metadata.creation_timestamp {
        let age = format_duration(Utc::now().signed_duration_since(ts));
        println!("\nAge:          {}", age);
    }
}

fn describe_namespace(namespace: &Namespace) {
    println!("Name:         {}", namespace.metadata.name);

    if let Some(labels) = &namespace.metadata.labels {
        println!(
            "Labels:       {}",
            labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("\n              ")
        );
    }

    if let Some(status) = &namespace.status {
        println!("Status:       {:?}", status.phase);
    }

    if let Some(ts) = namespace.metadata.creation_timestamp {
        let age = format_duration(Utc::now().signed_duration_since(ts));
        println!("\nAge:          {}", age);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration_days() {
        let dur = chrono::Duration::days(5);
        assert_eq!(format_duration(dur), "5d");
    }

    #[test]
    fn test_format_duration_hours() {
        let dur = chrono::Duration::hours(3);
        assert_eq!(format_duration(dur), "3h");
    }

    #[test]
    fn test_format_duration_minutes() {
        let dur = chrono::Duration::minutes(42);
        assert_eq!(format_duration(dur), "42m");
    }

    #[test]
    fn test_format_duration_seconds() {
        let dur = chrono::Duration::seconds(15);
        assert_eq!(format_duration(dur), "15s");
    }

    #[test]
    fn test_format_duration_zero() {
        let dur = chrono::Duration::seconds(0);
        assert_eq!(format_duration(dur), "0s");
    }

    #[test]
    fn test_format_duration_boundary_hours_to_days() {
        let dur = chrono::Duration::hours(24);
        assert_eq!(format_duration(dur), "1d");
        let dur = chrono::Duration::hours(23);
        assert_eq!(format_duration(dur), "23h");
    }

    #[test]
    fn test_format_duration_boundary_minutes_to_hours() {
        let dur = chrono::Duration::minutes(60);
        assert_eq!(format_duration(dur), "1h");
        let dur = chrono::Duration::minutes(59);
        assert_eq!(format_duration(dur), "59m");
    }

    fn make_test_pod() -> Pod {
        use rusternetes_common::resources::pod::{Container, ContainerPort, PodSpec, PodStatus};
        use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
        use std::collections::HashMap;

        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "nginx-pod".to_string(),
                namespace: Some("test-ns".to_string()),
                labels: Some(HashMap::from([("app".to_string(), "nginx".to_string())])),
                annotations: Some(HashMap::from([("note".to_string(), "test".to_string())])),
                creation_timestamp: Some(Utc::now() - chrono::Duration::hours(2)),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "nginx".to_string(),
                    image: "nginx:latest".to_string(),
                    ports: Some(vec![ContainerPort {
                        container_port: 80,
                        protocol: Some("TCP".to_string()),
                        name: None,
                        host_port: None,
                        host_ip: None,
                    }]),
                    ..Default::default()
                }],
                node_name: Some("node-1".to_string()),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.5".to_string()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_describe_pod_output() {
        let pod = make_test_pod();
        describe_pod(&pod);
    }

    #[test]
    fn test_describe_pod_minimal() {
        use rusternetes_common::resources::pod::{Container, PodSpec};
        use rusternetes_common::types::{ObjectMeta, TypeMeta};

        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "bare-pod".to_string(),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        describe_pod(&pod);
    }

    #[test]
    fn test_describe_service_output() {
        use rusternetes_common::resources::service::{ServicePort, ServiceSpec};
        use rusternetes_common::resources::IntOrString;
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        use std::collections::HashMap;

        let service = Service {
            type_meta: TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "my-svc".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                creation_timestamp: Some(Utc::now() - chrono::Duration::days(3)),
                ..Default::default()
            },
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    port: 80,
                    target_port: Some(IntOrString::Int(8080)),
                    protocol: Some("TCP".to_string()),
                    name: Some("http".to_string()),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: Some("10.96.0.100".to_string()),
                selector: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        describe_service(&service);
    }

    #[test]
    fn test_describe_service_no_type_defaults_to_clusterip() {
        use rusternetes_common::resources::service::{ServicePort, ServiceSpec};
        use rusternetes_common::types::{ObjectMeta, TypeMeta};

        let service = Service {
            type_meta: TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "headless".to_string(),
                ..Default::default()
            },
            spec: ServiceSpec {
                service_type: None,
                ports: vec![ServicePort {
                    port: 443,
                    target_port: None,
                    protocol: None,
                    name: None,
                    node_port: None,
                    app_protocol: None,
                }],
                ..Default::default()
            },
            status: None,
        };
        describe_service(&service);
    }

    #[test]
    fn test_describe_deployment_output() {
        use rusternetes_common::resources::deployment::{DeploymentSpec, DeploymentStatus};
        use rusternetes_common::resources::pod::{Container, PodSpec};
        use rusternetes_common::resources::workloads::PodTemplateSpec;
        use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
        use std::collections::HashMap;

        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "web-deploy".to_string(),
                namespace: Some("prod".to_string()),
                labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                creation_timestamp: Some(Utc::now() - chrono::Duration::days(10)),
                ..Default::default()
            },
            spec: DeploymentSpec {
                replicas: Some(3),
                selector: LabelSelector {
                    match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                    ..Default::default()
                },
                template: PodTemplateSpec {
                    metadata: None,
                    spec: PodSpec {
                        containers: vec![Container {
                            name: "web".to_string(),
                            image: "nginx:1.25".to_string(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                },
                ..Default::default()
            },
            status: Some(DeploymentStatus {
                ready_replicas: Some(3),
                updated_replicas: Some(3),
                available_replicas: Some(3),
                ..Default::default()
            }),
        };
        describe_deployment(&deployment);
    }

    #[test]
    fn test_describe_node_output() {
        use rusternetes_common::resources::node::{NodeAddress, NodeCondition, NodeStatus};
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        use std::collections::HashMap;

        let node = Node {
            type_meta: TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "worker-1".to_string(),
                labels: Some(HashMap::from([(
                    "kubernetes.io/hostname".to_string(),
                    "worker-1".to_string(),
                )])),
                creation_timestamp: Some(Utc::now() - chrono::Duration::days(30)),
                ..Default::default()
            },
            spec: None,
            status: Some(NodeStatus {
                conditions: Some(vec![
                    NodeCondition {
                        condition_type: "Ready".to_string(),
                        status: "True".to_string(),
                        last_heartbeat_time: None,
                        last_transition_time: None,
                        reason: None,
                        message: None,
                    },
                    NodeCondition {
                        condition_type: "MemoryPressure".to_string(),
                        status: "False".to_string(),
                        last_heartbeat_time: None,
                        last_transition_time: None,
                        reason: None,
                        message: None,
                    },
                ]),
                addresses: Some(vec![
                    NodeAddress {
                        address_type: "InternalIP".to_string(),
                        address: "192.168.1.10".to_string(),
                    },
                    NodeAddress {
                        address_type: "Hostname".to_string(),
                        address: "worker-1".to_string(),
                    },
                ]),
                capacity: Some(HashMap::from([
                    ("cpu".to_string(), "4".to_string()),
                    ("memory".to_string(), "8Gi".to_string()),
                ])),
                allocatable: Some(HashMap::from([
                    ("cpu".to_string(), "3800m".to_string()),
                    ("memory".to_string(), "7Gi".to_string()),
                ])),
                ..Default::default()
            }),
        };
        describe_node(&node);
    }

    #[test]
    fn test_describe_namespace_output() {
        use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
        use std::collections::HashMap;

        let ns = Namespace {
            type_meta: TypeMeta {
                kind: "Namespace".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "kube-system".to_string(),
                labels: Some(HashMap::from([(
                    "kubernetes.io/metadata.name".to_string(),
                    "kube-system".to_string(),
                )])),
                creation_timestamp: Some(Utc::now() - chrono::Duration::days(90)),
                ..Default::default()
            },
            spec: None,
            status: Some(rusternetes_common::resources::namespace::NamespaceStatus {
                phase: Some(Phase::Active),
                conditions: None,
            }),
        };
        describe_namespace(&ns);
    }

    #[test]
    fn test_describe_namespace_no_status() {
        use rusternetes_common::types::{ObjectMeta, TypeMeta};

        let ns = Namespace {
            type_meta: TypeMeta {
                kind: "Namespace".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "empty-ns".to_string(),
                ..Default::default()
            },
            spec: None,
            status: None,
        };
        describe_namespace(&ns);
    }

    #[test]
    fn test_map_get_error_not_found() {
        let err = map_get_error(crate::client::GetError::NotFound);
        assert_eq!(err.to_string(), "Resource not found");
    }

    #[test]
    fn test_map_get_error_other() {
        let err = map_get_error(crate::client::GetError::Other(anyhow::anyhow!(
            "connection refused"
        )));
        assert_eq!(err.to_string(), "connection refused");
    }

    // ===== 10 additional tests for untested functions =====

    fn make_test_client() -> ApiClient {
        ApiClient::new("http://127.0.0.1:1", true, None).unwrap()
    }

    #[tokio::test]
    async fn test_execute_pod_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute(&client, "pod", "nginx", Some("default")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_service_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute(&client, "service", "my-svc", Some("default")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_deployment_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute(&client, "deployment", "web", Some("default")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_node_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute(&client, "node", "worker-1", Some("default")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_namespace_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = execute(&client, "ns", "kube-system", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_unknown_resource_type_returns_err() {
        let client = make_test_client();
        let result = execute(&client, "foobar", "test", Some("default")).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown resource type"));
    }

    #[tokio::test]
    async fn test_execute_enhanced_with_name() {
        let client = make_test_client();
        let result = execute_enhanced(&client, "pod", Some("nginx"), "default", None, false).await;
        assert!(result.is_err()); // connection error
    }

    #[tokio::test]
    async fn test_execute_enhanced_no_name_returns_err() {
        let client = make_test_client();
        let result = execute_enhanced(&client, "pod", None, "default", None, false).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Resource name required"));
    }

    #[tokio::test]
    async fn test_execute_enhanced_with_selector_returns_ok() {
        let client = make_test_client();
        let result =
            execute_enhanced(&client, "pod", None, "default", Some("app=nginx"), false).await;
        // Selector-based describe just prints a message and returns Ok
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_enhanced_all_namespaces_returns_ok() {
        let client = make_test_client();
        let result = execute_enhanced(&client, "pod", None, "default", None, true).await;
        // All-namespaces describe just prints a message and returns Ok
        assert!(result.is_ok());
    }
}
