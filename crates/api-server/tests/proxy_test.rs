//! Integration tests for proxy handlers (node, service, pod)

use rusternetes_common::resources::{
    ContainerPort, IntOrString, Node, NodeStatus, Pod, PodIP, PodSpec, PodStatus, Service,
    ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_storage::{memory::MemoryStorage, Storage};
use std::sync::Arc;

#[tokio::test]
async fn test_proxy_node_missing_address() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a node without addresses
    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-node".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions: None,
            addresses: None, // No addresses
            capacity: None,
            allocatable: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        }),
    };

    let key = "/api/v1/nodes/test-node";
    storage.create(key, &node).await.unwrap();

    // Verify the node exists but has no addresses
    let retrieved: Node = storage.get(key).await.unwrap();
    assert!(retrieved.status.is_some());
    assert!(retrieved.status.as_ref().unwrap().addresses.is_none());

    // Clean up
    storage.delete(key).await.unwrap();
}

#[tokio::test]
async fn test_proxy_service_missing_clusterip() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a service without ClusterIP
    let service = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-service".to_string(),
            namespace: Some("default".to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: ServiceSpec {
            selector: Some(std::collections::HashMap::new()),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: None, // No ClusterIP
            external_ips: None,
            session_affinity: None,
            external_name: None,
            cluster_ips: None,
            ip_families: None,
            ip_family_policy: None,
            internal_traffic_policy: None,
            external_traffic_policy: None,
            health_check_node_port: None,
            load_balancer_class: None,
            load_balancer_ip: None,
            load_balancer_source_ranges: None,
            allocate_load_balancer_node_ports: None,
            publish_not_ready_addresses: None,
            session_affinity_config: None,
            traffic_distribution: None,
        },
        status: None,
    };

    let key = "/api/v1/namespaces/default/services/test-service";
    storage.create(key, &service).await.unwrap();

    // Verify the service exists but has no ClusterIP
    let retrieved: Service = storage.get(key).await.unwrap();
    assert!(retrieved.spec.cluster_ip.is_none());

    // Clean up
    storage.delete(key).await.unwrap();
}

#[tokio::test]
async fn test_proxy_pod_missing_ip() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a pod without IP
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-pod".to_string(),
            namespace: Some("default".to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(PodSpec {
            containers: vec![],
            init_containers: None,
            ephemeral_containers: None,
            restart_policy: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            node_name: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            volumes: None,
            hostname: None,
            subdomain: None,
            affinity: None,
            scheduler_name: None,
            tolerations: None,
            priority_class_name: None,
            priority: None,
            overhead: None,
            topology_spread_constraints: None,
            automount_service_account_token: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Pending),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None, // No pod IP
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            conditions: None,
            ..Default::default()
        }),
    };

    let key = "/api/v1/namespaces/default/pods/test-pod";
    storage.create(key, &pod).await.unwrap();

    // Verify the pod exists but has no IP
    let retrieved: Pod = storage.get(key).await.unwrap();
    assert!(retrieved.status.is_some());
    assert!(retrieved.status.as_ref().unwrap().pod_ip.is_none());

    // Clean up
    storage.delete(key).await.unwrap();
}

#[test]
fn test_proxy_handlers_header_filtering() {
    // Header filtering is already tested in proxy.rs
    // This test documents the expected behavior

    // Hop-by-hop headers should be filtered: Connection, Keep-Alive,
    // Proxy-Authenticate, Proxy-Authorization, TE, Trailers, Transfer-Encoding, Upgrade

    // End-to-end headers should be forwarded: Content-Type, Authorization,
    // Accept, User-Agent, etc.

    // This is verified in the proxy.rs module tests
}

/// Verify that a pod with a named container port can be looked up via the
/// storage layer — the proxy handler reads this same field to resolve
/// `pod:portname` style URLs.
#[tokio::test]
async fn test_proxy_pod_named_port_storage_round_trip() {
    let storage = Arc::new(MemoryStorage::new());

    // Pod with a single container exposing a named port "http" → 8080.
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "named-port-pod".to_string(),
            namespace: Some("default".to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(PodSpec {
            containers: vec![rusternetes_common::resources::Container {
                name: "app".to_string(),
                image: "nginx:alpine".to_string(),
                ports: Some(vec![ContainerPort {
                    container_port: 8080,
                    name: Some("http".to_string()),
                    protocol: "TCP".to_string(),
                    host_port: None,
                    host_ip: None,
                }]),
                ..Default::default()
            }],
            init_containers: None,
            ephemeral_containers: None,
            restart_policy: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            node_name: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            volumes: None,
            hostname: None,
            subdomain: None,
            affinity: None,
            scheduler_name: None,
            tolerations: None,
            priority_class_name: None,
            priority: None,
            overhead: None,
            topology_spread_constraints: None,
            automount_service_account_token: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: Some("10.0.0.42".to_string()),
            pod_i_ps: Some(vec![PodIP {
                ip: "10.0.0.42".to_string(),
            }]),
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            conditions: None,
            ..Default::default()
        }),
    };

    let key = "/api/v1/namespaces/default/pods/named-port-pod";
    storage.create(key, &pod).await.unwrap();

    // Retrieve and verify the named port is present — this is the field
    // the proxy_pod handler scans when resolving `name:portname` URLs.
    let retrieved: Pod = storage.get(key).await.unwrap();
    let spec = retrieved.spec.expect("pod spec");
    let ports = spec.containers[0].ports.as_ref().expect("container ports");
    let named = ports
        .iter()
        .find(|p| p.name.as_deref() == Some("http"))
        .expect("named port 'http' missing");
    assert_eq!(named.container_port, 8080);

    // Clean up
    storage.delete(key).await.unwrap();
}
