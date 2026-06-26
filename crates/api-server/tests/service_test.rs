// Integration tests for Service resource
//
// These tests verify:
// 1. ExternalName service creation and validation
// 2. Dual-stack IPv4/IPv6 support
// 3. Traffic policy configuration
// 4. ClusterIP allocation for different service types

use rusternetes_common::resources::{
    IPFamily, IPFamilyPolicy, IntOrString, Service, ServiceExternalTrafficPolicy,
    ServiceInternalTrafficPolicy, ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use std::collections::HashMap;

fn create_basic_service_spec() -> ServiceSpec {
    ServiceSpec {
        selector: Some({
            let mut selector = HashMap::new();
            selector.insert("app".to_string(), "test".to_string());
            selector
        }),
        ports: vec![ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: Some(IntOrString::Int(8080)),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        }],
        service_type: Some(ServiceType::ClusterIP),
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
    }
}

fn create_external_name_service(name: &str, namespace: &str, external_name: &str) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: ServiceSpec {
            selector: None, // ExternalName services typically don't have selectors
            ports: vec![],  // ExternalName services don't need ports
            service_type: Some(ServiceType::ExternalName),
            cluster_ip: None,
            external_ips: None,
            session_affinity: None,
            external_name: Some(external_name.to_string()),
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
    }
}

fn create_dual_stack_service(name: &str, namespace: &str) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: ServiceSpec {
            selector: Some({
                let mut selector = HashMap::new();
                selector.insert("app".to_string(), "dual-stack-app".to_string());
                selector
            }),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: None,
            external_ips: None,
            session_affinity: None,
            external_name: None,
            cluster_ips: None,
            ip_families: Some(vec![IPFamily::IPv4, IPFamily::IPv6]),
            ip_family_policy: Some(IPFamilyPolicy::PreferDualStack),
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
    }
}

// ===== ExternalName Service Tests =====

#[test]
fn test_external_name_service_has_external_name() {
    let service = create_external_name_service("test-svc", "default", "example.com");

    assert_eq!(service.spec.service_type, Some(ServiceType::ExternalName));
    assert_eq!(service.spec.external_name, Some("example.com".to_string()));
    assert!(service.spec.selector.is_none());
    assert!(service.spec.ports.is_empty());
}

#[test]
fn test_external_name_service_serialization() {
    let service = create_external_name_service("test-svc", "default", "external.example.com");

    // Serialize to JSON
    let json = serde_json::to_string(&service).unwrap();

    // Deserialize back
    let deserialized: Service = serde_json::from_str(&json).unwrap();

    assert_eq!(
        deserialized.spec.service_type,
        Some(ServiceType::ExternalName)
    );
    assert_eq!(
        deserialized.spec.external_name,
        Some("external.example.com".to_string())
    );
}

// ===== Dual-Stack Service Tests =====

#[test]
fn test_dual_stack_service_has_ip_families() {
    let service = create_dual_stack_service("dual-stack-svc", "default");

    assert_eq!(
        service.spec.ip_families,
        Some(vec![IPFamily::IPv4, IPFamily::IPv6])
    );
    assert_eq!(
        service.spec.ip_family_policy,
        Some(IPFamilyPolicy::PreferDualStack)
    );
}

#[test]
fn test_dual_stack_service_serialization() {
    let service = create_dual_stack_service("dual-stack-svc", "default");

    // Serialize to JSON
    let json = serde_json::to_string(&service).unwrap();

    // Deserialize back
    let deserialized: Service = serde_json::from_str(&json).unwrap();

    assert_eq!(
        deserialized.spec.ip_families,
        Some(vec![IPFamily::IPv4, IPFamily::IPv6])
    );
    assert_eq!(
        deserialized.spec.ip_family_policy,
        Some(IPFamilyPolicy::PreferDualStack)
    );
}

#[test]
fn test_ip_family_policy_variants() {
    let mut spec = create_basic_service_spec();

    // Test SingleStack
    spec.ip_family_policy = Some(IPFamilyPolicy::SingleStack);
    let json = serde_json::to_string(&spec).unwrap();
    assert!(json.contains("SingleStack"));

    // Test PreferDualStack
    spec.ip_family_policy = Some(IPFamilyPolicy::PreferDualStack);
    let json = serde_json::to_string(&spec).unwrap();
    assert!(json.contains("PreferDualStack"));

    // Test RequireDualStack
    spec.ip_family_policy = Some(IPFamilyPolicy::RequireDualStack);
    let json = serde_json::to_string(&spec).unwrap();
    assert!(json.contains("RequireDualStack"));
}

// ===== Traffic Policy Tests =====

#[test]
fn test_internal_traffic_policy() {
    let mut spec = create_basic_service_spec();

    // Test Cluster policy
    spec.internal_traffic_policy = Some(ServiceInternalTrafficPolicy::Cluster);
    let json = serde_json::to_string(&spec).unwrap();
    let deserialized: ServiceSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(
        deserialized.internal_traffic_policy,
        Some(ServiceInternalTrafficPolicy::Cluster)
    );

    // Test Local policy
    spec.internal_traffic_policy = Some(ServiceInternalTrafficPolicy::Local);
    let json = serde_json::to_string(&spec).unwrap();
    let deserialized: ServiceSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(
        deserialized.internal_traffic_policy,
        Some(ServiceInternalTrafficPolicy::Local)
    );
}

#[test]
fn test_external_traffic_policy() {
    let mut spec = create_basic_service_spec();
    spec.service_type = Some(ServiceType::LoadBalancer);

    // Test Cluster policy
    spec.external_traffic_policy = Some(ServiceExternalTrafficPolicy::Cluster);
    let json = serde_json::to_string(&spec).unwrap();
    let deserialized: ServiceSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(
        deserialized.external_traffic_policy,
        Some(ServiceExternalTrafficPolicy::Cluster)
    );

    // Test Local policy
    spec.external_traffic_policy = Some(ServiceExternalTrafficPolicy::Local);
    let json = serde_json::to_string(&spec).unwrap();
    let deserialized: ServiceSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(
        deserialized.external_traffic_policy,
        Some(ServiceExternalTrafficPolicy::Local)
    );
}

// ===== Service Type Tests =====

#[test]
fn test_service_types() {
    let types = vec![
        ServiceType::ClusterIP,
        ServiceType::NodePort,
        ServiceType::LoadBalancer,
        ServiceType::ExternalName,
    ];

    for service_type in types {
        let mut spec = create_basic_service_spec();
        spec.service_type = Some(service_type.clone());

        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: ServiceSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.service_type, Some(service_type));
    }
}

// ===== ClusterIPs (Dual-Stack) Tests =====

#[test]
fn test_cluster_ips_for_dual_stack() {
    let mut spec = create_basic_service_spec();
    spec.cluster_ips = Some(vec!["10.96.0.10".to_string(), "fd00::1234".to_string()]);
    spec.ip_families = Some(vec![IPFamily::IPv4, IPFamily::IPv6]);
    spec.ip_family_policy = Some(IPFamilyPolicy::RequireDualStack);

    let json = serde_json::to_string(&spec).unwrap();
    let deserialized: ServiceSpec = serde_json::from_str(&json).unwrap();

    assert_eq!(
        deserialized.cluster_ips,
        Some(vec!["10.96.0.10".to_string(), "fd00::1234".to_string()])
    );
    assert_eq!(
        deserialized.ip_families,
        Some(vec![IPFamily::IPv4, IPFamily::IPv6])
    );
}

#[test]
fn test_empty_ports_for_external_name() {
    let service = create_external_name_service("test-svc", "default", "example.com");

    // ExternalName services typically don't have ports
    assert!(service.spec.ports.is_empty());

    // Serialize and verify ports field is either empty array or not present
    let json = serde_json::to_string(&service).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    // Check if ports is present and empty, or not present at all
    if let Some(ports) = parsed["spec"]["ports"].as_array() {
        assert!(
            ports.is_empty(),
            "ports should be an empty array for ExternalName services"
        );
    }
}

#[test]
fn test_nodeport_with_external_traffic_policy_local() {
    let mut spec = create_basic_service_spec();
    spec.service_type = Some(ServiceType::NodePort);
    spec.external_traffic_policy = Some(ServiceExternalTrafficPolicy::Local);
    spec.ports = vec![ServicePort {
        name: Some("http".to_string()),
        port: 80,
        target_port: Some(IntOrString::Int(8080)),
        protocol: "TCP".to_string(),
        node_port: Some(30080),
        app_protocol: None,
    }];

    let service = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("nodeport-svc").with_namespace("default"),
        spec,
        status: None,
    };

    // Serialize and deserialize
    let json = serde_json::to_string(&service).unwrap();
    let deserialized: Service = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.spec.service_type, Some(ServiceType::NodePort));
    assert_eq!(
        deserialized.spec.external_traffic_policy,
        Some(ServiceExternalTrafficPolicy::Local)
    );
    assert_eq!(deserialized.spec.ports[0].node_port, Some(30080));
}
