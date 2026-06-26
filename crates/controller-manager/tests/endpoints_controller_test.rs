use rusternetes_common::resources::{
    Container, ContainerStatus, IntOrString, Pod, PodCondition, PodSpec, PodStatus, Service,
    ServicePort, ServiceSpec,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::endpoints::EndpointsController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn create_test_service(name: &str, namespace: &str, selector: HashMap<String, String>) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: ServiceSpec {
            selector: Some(selector),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                node_port: None,
                app_protocol: None,
            }],
            cluster_ip: Some("10.96.0.1".to_string()),
            service_type: None,
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
    }
}

fn create_test_pod(
    name: &str,
    namespace: &str,
    labels: HashMap<String, String>,
    pod_ip: Option<String>,
    ready: bool,
) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.labels = Some(labels);
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:latest".to_string(),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ports: Some(vec![]),
                env: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                resources: None,
                working_dir: None,
                command: None,
                args: None,
                restart_policy: None,
                resize_policy: None,
                security_context: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            init_containers: None,
            ephemeral_containers: None,
            volumes: None,
            restart_policy: Some("Always".to_string()),
            node_name: Some("node-1".to_string()),
            node_selector: None,
            service_account_name: None,
            service_account: None,
            automount_service_account_token: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority: None,
            priority_class_name: None,
            scheduler_name: None,
            overhead: None,
            topology_spread_constraints: None,
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
            phase: if ready {
                Some(Phase::Running)
            } else {
                Some(Phase::Pending)
            },
            message: None,
            reason: None,
            host_ip: Some("192.168.1.10".to_string()),
            host_i_ps: None,
            pod_ip,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: if ready {
                Some(vec![PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }])
            } else {
                None
            },
            container_statuses: if ready {
                Some(vec![ContainerStatus {
                    name: "nginx".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: None,
                    last_state: None,
                    image: Some("nginx:latest".to_string()),
                    image_id: None,
                    container_id: Some("container-123".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }])
            } else {
                None
            },
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        }),
    }
}

#[tokio::test]
async fn test_endpoints_created_for_service_with_matching_pods() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create service with selector
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "web".to_string());
    let service = create_test_service("web-service", "default", selector.clone());
    storage
        .create(
            &build_key("services", Some("default"), "web-service"),
            &service,
        )
        .await
        .unwrap();

    // Create matching pods
    let pod1 = create_test_pod(
        "web-pod-1",
        "default",
        selector.clone(),
        Some("10.244.0.1".to_string()),
        true,
    );
    let pod2 = create_test_pod(
        "web-pod-2",
        "default",
        selector.clone(),
        Some("10.244.0.2".to_string()),
        true,
    );
    storage
        .create(&build_key("pods", Some("default"), "web-pod-1"), &pod1)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("default"), "web-pod-2"), &pod2)
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify endpoints were created
    let endpoints: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("default"), "web-service"))
        .await
        .unwrap();

    assert_eq!(endpoints.metadata.name, "web-service");
    assert_eq!(endpoints.subsets.len(), 1);

    let subset = &endpoints.subsets[0];
    assert!(subset.addresses.is_some());
    let addresses = subset.addresses.as_ref().unwrap();
    assert_eq!(addresses.len(), 2);

    // Verify IPs
    let ips: Vec<&str> = addresses.iter().map(|a| a.ip.as_str()).collect();
    assert!(ips.contains(&"10.244.0.1"));
    assert!(ips.contains(&"10.244.0.2"));
}

#[tokio::test]
async fn test_endpoints_separates_ready_and_not_ready_pods() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create service
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "db".to_string());
    let service = create_test_service("db-service", "default", selector.clone());
    storage
        .create(
            &build_key("services", Some("default"), "db-service"),
            &service,
        )
        .await
        .unwrap();

    // Create ready and not-ready pods
    let ready_pod = create_test_pod(
        "db-pod-1",
        "default",
        selector.clone(),
        Some("10.244.0.10".to_string()),
        true,
    );
    let not_ready_pod = create_test_pod(
        "db-pod-2",
        "default",
        selector.clone(),
        Some("10.244.0.11".to_string()),
        false,
    );

    storage
        .create(&build_key("pods", Some("default"), "db-pod-1"), &ready_pod)
        .await
        .unwrap();
    storage
        .create(
            &build_key("pods", Some("default"), "db-pod-2"),
            &not_ready_pod,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify endpoints
    let endpoints: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("default"), "db-service"))
        .await
        .unwrap();

    assert_eq!(endpoints.subsets.len(), 1);
    let subset = &endpoints.subsets[0];

    // Ready addresses
    assert!(subset.addresses.is_some());
    let ready_addresses = subset.addresses.as_ref().unwrap();
    assert_eq!(ready_addresses.len(), 1);
    assert_eq!(ready_addresses[0].ip, "10.244.0.10");

    // Not ready addresses
    assert!(subset.not_ready_addresses.is_some());
    let not_ready_addresses = subset.not_ready_addresses.as_ref().unwrap();
    assert_eq!(not_ready_addresses.len(), 1);
    assert_eq!(not_ready_addresses[0].ip, "10.244.0.11");
}

#[tokio::test]
async fn test_endpoints_skips_pods_without_ip() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create service
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "api".to_string());
    let service = create_test_service("api-service", "default", selector.clone());
    storage
        .create(
            &build_key("services", Some("default"), "api-service"),
            &service,
        )
        .await
        .unwrap();

    // Create pods - one with IP, one without
    let pod_with_ip = create_test_pod(
        "api-pod-1",
        "default",
        selector.clone(),
        Some("10.244.0.20".to_string()),
        true,
    );
    let pod_without_ip = create_test_pod("api-pod-2", "default", selector.clone(), None, true);

    storage
        .create(
            &build_key("pods", Some("default"), "api-pod-1"),
            &pod_with_ip,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("pods", Some("default"), "api-pod-2"),
            &pod_without_ip,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify endpoints only includes pod with IP
    let endpoints: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("default"), "api-service"))
        .await
        .unwrap();

    assert_eq!(endpoints.subsets.len(), 1);
    let subset = &endpoints.subsets[0];
    assert!(subset.addresses.is_some());
    let addresses = subset.addresses.as_ref().unwrap();
    assert_eq!(addresses.len(), 1, "Should only include pod with IP");
    assert_eq!(addresses[0].ip, "10.244.0.20");
}

#[tokio::test]
async fn test_endpoints_respects_service_selector() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create service with specific selector
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "frontend".to_string());
    selector.insert("tier".to_string(), "web".to_string());
    let service = create_test_service("frontend-service", "default", selector.clone());
    storage
        .create(
            &build_key("services", Some("default"), "frontend-service"),
            &service,
        )
        .await
        .unwrap();

    // Create matching pod
    let matching_pod = create_test_pod(
        "frontend-pod",
        "default",
        selector.clone(),
        Some("10.244.0.30".to_string()),
        true,
    );

    // Create non-matching pod (missing tier label)
    let mut partial_labels = HashMap::new();
    partial_labels.insert("app".to_string(), "frontend".to_string());
    let non_matching_pod = create_test_pod(
        "other-pod",
        "default",
        partial_labels,
        Some("10.244.0.31".to_string()),
        true,
    );

    storage
        .create(
            &build_key("pods", Some("default"), "frontend-pod"),
            &matching_pod,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("pods", Some("default"), "other-pod"),
            &non_matching_pod,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify endpoints only includes matching pod
    let endpoints: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("default"), "frontend-service"))
        .await
        .unwrap();

    assert_eq!(endpoints.subsets.len(), 1);
    let subset = &endpoints.subsets[0];
    assert!(subset.addresses.is_some());
    let addresses = subset.addresses.as_ref().unwrap();
    assert_eq!(
        addresses.len(),
        1,
        "Should only include pod matching all selector labels"
    );
    assert_eq!(addresses[0].ip, "10.244.0.30");
}

#[tokio::test]
async fn test_endpoints_skips_service_without_selector() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create service without selector (headless service)
    let service = create_test_service("headless-service", "default", HashMap::new());
    storage
        .create(
            &build_key("services", Some("default"), "headless-service"),
            &service,
        )
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify endpoints were NOT created
    let result: Result<rusternetes_common::resources::Endpoints, _> = storage
        .get(&build_key("endpoints", Some("default"), "headless-service"))
        .await;

    assert!(
        result.is_err(),
        "Endpoints should not be created for service without selector"
    );
}

#[tokio::test]
async fn test_endpoints_updates_when_pods_change() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create service
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "cache".to_string());
    let service = create_test_service("cache-service", "default", selector.clone());
    storage
        .create(
            &build_key("services", Some("default"), "cache-service"),
            &service,
        )
        .await
        .unwrap();

    // Create initial pod
    let pod1 = create_test_pod(
        "cache-pod-1",
        "default",
        selector.clone(),
        Some("10.244.0.40".to_string()),
        true,
    );
    storage
        .create(&build_key("pods", Some("default"), "cache-pod-1"), &pod1)
        .await
        .unwrap();

    // First reconcile
    controller.reconcile_all().await.unwrap();

    // Verify initial endpoints
    let endpoints: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("default"), "cache-service"))
        .await
        .unwrap();
    assert_eq!(endpoints.subsets[0].addresses.as_ref().unwrap().len(), 1);

    // Add another pod
    let pod2 = create_test_pod(
        "cache-pod-2",
        "default",
        selector.clone(),
        Some("10.244.0.41".to_string()),
        true,
    );
    storage
        .create(&build_key("pods", Some("default"), "cache-pod-2"), &pod2)
        .await
        .unwrap();

    // Second reconcile
    controller.reconcile_all().await.unwrap();

    // Verify updated endpoints
    let updated_endpoints: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("default"), "cache-service"))
        .await
        .unwrap();
    assert_eq!(
        updated_endpoints.subsets[0]
            .addresses
            .as_ref()
            .unwrap()
            .len(),
        2,
        "Endpoints should be updated to include new pod"
    );
}

#[tokio::test]
async fn test_endpoints_multiple_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create services in different namespaces
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "worker".to_string());

    let service_ns1 = create_test_service("worker-service", "ns1", selector.clone());
    let service_ns2 = create_test_service("worker-service", "ns2", selector.clone());

    storage
        .create(
            &build_key("services", Some("ns1"), "worker-service"),
            &service_ns1,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("services", Some("ns2"), "worker-service"),
            &service_ns2,
        )
        .await
        .unwrap();

    // Create pods in different namespaces
    let pod_ns1 = create_test_pod(
        "worker-pod-1",
        "ns1",
        selector.clone(),
        Some("10.244.1.1".to_string()),
        true,
    );
    let pod_ns2 = create_test_pod(
        "worker-pod-2",
        "ns2",
        selector.clone(),
        Some("10.244.2.1".to_string()),
        true,
    );

    storage
        .create(&build_key("pods", Some("ns1"), "worker-pod-1"), &pod_ns1)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("ns2"), "worker-pod-2"), &pod_ns2)
        .await
        .unwrap();

    // Reconcile all
    controller.reconcile_all().await.unwrap();

    // Verify endpoints in ns1
    let endpoints_ns1: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("ns1"), "worker-service"))
        .await
        .unwrap();
    assert_eq!(endpoints_ns1.metadata.namespace.as_deref().unwrap(), "ns1");
    assert_eq!(
        endpoints_ns1.subsets[0].addresses.as_ref().unwrap().len(),
        1
    );
    assert_eq!(
        endpoints_ns1.subsets[0].addresses.as_ref().unwrap()[0].ip,
        "10.244.1.1"
    );

    // Verify endpoints in ns2
    let endpoints_ns2: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("ns2"), "worker-service"))
        .await
        .unwrap();
    assert_eq!(endpoints_ns2.metadata.namespace.as_deref().unwrap(), "ns2");
    assert_eq!(
        endpoints_ns2.subsets[0].addresses.as_ref().unwrap().len(),
        1
    );
    assert_eq!(
        endpoints_ns2.subsets[0].addresses.as_ref().unwrap()[0].ip,
        "10.244.2.1"
    );
}

#[tokio::test]
async fn test_endpoints_includes_target_ref() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create service
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "metrics".to_string());
    let service = create_test_service("metrics-service", "default", selector.clone());
    storage
        .create(
            &build_key("services", Some("default"), "metrics-service"),
            &service,
        )
        .await
        .unwrap();

    // Create pod
    let pod = create_test_pod(
        "metrics-pod",
        "default",
        selector.clone(),
        Some("10.244.0.50".to_string()),
        true,
    );
    let pod_uid = pod.metadata.uid.clone();
    storage
        .create(&build_key("pods", Some("default"), "metrics-pod"), &pod)
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify endpoints include target_ref
    let endpoints: rusternetes_common::resources::Endpoints = storage
        .get(&build_key("endpoints", Some("default"), "metrics-service"))
        .await
        .unwrap();

    let subset = &endpoints.subsets[0];
    let address = &subset.addresses.as_ref().unwrap()[0];
    assert!(
        address.target_ref.is_some(),
        "Endpoint address should have target_ref"
    );

    let target_ref = address.target_ref.as_ref().unwrap();
    assert_eq!(target_ref.kind, Some("Pod".to_string()));
    assert_eq!(target_ref.name, Some("metrics-pod".to_string()));
    assert_eq!(target_ref.namespace, Some("default".to_string()));
    assert_eq!(target_ref.uid, Some(pod_uid));
}

#[tokio::test]
async fn test_endpoints_includes_port_mapping() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    // Create service with multiple ports
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "multi-port".to_string());

    let mut service = create_test_service("multi-port-service", "default", selector.clone());
    service.spec.ports = vec![
        ServicePort {
            name: Some("http".to_string()),
            protocol: "TCP".to_string(),
            port: 80,
            target_port: Some(IntOrString::Int(8080)),
            node_port: None,
            app_protocol: None,
        },
        ServicePort {
            name: Some("https".to_string()),
            protocol: "TCP".to_string(),
            port: 443,
            target_port: Some(IntOrString::Int(8443)),
            node_port: None,
            app_protocol: None,
        },
    ];

    storage
        .create(
            &build_key("services", Some("default"), "multi-port-service"),
            &service,
        )
        .await
        .unwrap();

    // Create pod
    let pod = create_test_pod(
        "multi-port-pod",
        "default",
        selector.clone(),
        Some("10.244.0.60".to_string()),
        true,
    );
    storage
        .create(&build_key("pods", Some("default"), "multi-port-pod"), &pod)
        .await
        .unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Verify endpoints include port mappings
    let endpoints: rusternetes_common::resources::Endpoints = storage
        .get(&build_key(
            "endpoints",
            Some("default"),
            "multi-port-service",
        ))
        .await
        .unwrap();

    let subset = &endpoints.subsets[0];
    assert!(subset.ports.is_some(), "Endpoints should have ports");

    let ports = subset.ports.as_ref().unwrap();
    assert_eq!(ports.len(), 2, "Should have 2 ports");

    // Verify HTTP port
    let http_port = ports
        .iter()
        .find(|p| p.name == Some("http".to_string()))
        .unwrap();
    assert_eq!(http_port.port, 8080);
    assert_eq!(http_port.protocol, "TCP");

    // Verify HTTPS port
    let https_port = ports
        .iter()
        .find(|p| p.name == Some("https".to_string()))
        .unwrap();
    assert_eq!(https_port.port, 8443);
    assert_eq!(https_port.protocol, "TCP");
}
