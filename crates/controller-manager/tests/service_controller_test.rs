//! Integration tests for ServiceController

use rusternetes_common::resources::{IntOrString, Service, ServicePort, ServiceSpec, ServiceType};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::service::ServiceController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

#[tokio::test]
async fn test_service_controller_creation_and_initialization() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceController::new(storage.clone());

    // Initialize the controller
    controller.initialize().await.unwrap();
}

#[tokio::test]
async fn test_service_clusterip_allocation() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceController::new(storage.clone());
    controller.initialize().await.unwrap();

    // Create a ClusterIP service without IP
    let service = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-service-clusterip".to_string(),
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
                protocol: Some("TCP".to_string()),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: None, // Should be allocated
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

    let key = build_key("services", Some("default"), "test-service-clusterip");
    storage.create(&key, &service).await.unwrap();

    // Reconcile should allocate ClusterIP
    controller.reconcile_all().await.unwrap();

    // Verify ClusterIP was allocated
    let retrieved: Service = storage.get(&key).await.unwrap();
    assert!(retrieved.spec.cluster_ip.is_some());
    assert_ne!(retrieved.spec.cluster_ip.as_ref().unwrap(), "");

    // ClusterIP should be in valid range (10.96.0.0/12)
    let cluster_ip = retrieved.spec.cluster_ip.as_ref().unwrap();
    assert!(cluster_ip.starts_with("10.96."));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_service_nodeport_allocation() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceController::new(storage.clone());
    controller.initialize().await.unwrap();

    // Create a NodePort service
    let service = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-service-nodeport".to_string(),
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
                protocol: Some("TCP".to_string()),
                node_port: None, // Should be allocated
                app_protocol: None,
            }],
            service_type: Some(ServiceType::NodePort),
            cluster_ip: None, // Should also be allocated
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

    let key = build_key("services", Some("default"), "test-service-nodeport");
    storage.create(&key, &service).await.unwrap();

    // Reconcile should allocate both ClusterIP and NodePort
    controller.reconcile_all().await.unwrap();

    // Verify allocations
    let retrieved: Service = storage.get(&key).await.unwrap();
    assert!(retrieved.spec.cluster_ip.is_some());
    assert!(retrieved.spec.ports[0].node_port.is_some());

    // NodePort should be in valid range (30000-32767)
    let node_port = retrieved.spec.ports[0].node_port.unwrap();
    assert!((30000..=32767).contains(&node_port));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_service_headless_no_clusterip() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceController::new(storage.clone());
    controller.initialize().await.unwrap();

    // Create a headless service
    let service = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-service-headless".to_string(),
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
                protocol: Some("TCP".to_string()),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: Some("None".to_string()), // Headless service
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

    let key = build_key("services", Some("default"), "test-service-headless");
    storage.create(&key, &service).await.unwrap();

    // Reconcile should not change headless service
    controller.reconcile_all().await.unwrap();

    // Verify ClusterIP is still "None"
    let retrieved: Service = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.cluster_ip.as_ref().unwrap(), "None");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_service_multiple_allocations_unique() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceController::new(storage.clone());
    controller.initialize().await.unwrap();

    // Create multiple services
    let mut services = Vec::new();
    for i in 0..5 {
        let service = Service {
            type_meta: TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: format!("test-service-{}", i),
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
                    protocol: Some("TCP".to_string()),
                    node_port: None,
                    app_protocol: None,
                }],
                service_type: Some(ServiceType::NodePort),
                cluster_ip: None,
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

        let key = build_key("services", Some("default"), &format!("test-service-{}", i));
        storage.create(&key, &service).await.unwrap();
        services.push(key);
    }

    // Reconcile all
    controller.reconcile_all().await.unwrap();

    // Collect allocated IPs and ports
    let mut cluster_ips = std::collections::HashSet::new();
    let mut node_ports = std::collections::HashSet::new();

    for key in &services {
        let service: Service = storage.get(key).await.unwrap();
        if let Some(ip) = service.spec.cluster_ip {
            cluster_ips.insert(ip);
        }
        if let Some(port) = service.spec.ports[0].node_port {
            node_ports.insert(port);
        }
    }

    // All IPs and ports should be unique
    assert_eq!(cluster_ips.len(), 5);
    assert_eq!(node_ports.len(), 5);

    // Clean up
    for key in services {
        storage.delete(&key).await.unwrap();
    }
}
