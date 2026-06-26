//! Integration tests for conformance fixes covering:
//!   - upstream e2e sites network/service.go:3459,4291
//!   - upstream e2e site service_latency.go:145
//!   - upstream e2e site proxy.go:503
//!
//! Sub-bugs:
//!   1. Service deletion must promptly remove its Endpoints (and EndpointSlices).
//!   2. LoadBalancer Service status.loadBalancer.ingress must be populated even
//!      when no cloud provider is configured (the e2e harness has none).
//!   3. Endpoints for a Service+matching ready pods must be created on the
//!      first reconcile (latency budget).
//!   4. The api-server proxy subresource path resolver must find a pod IP via
//!      Endpoints when EndpointSlices are not yet mirrored.

use rusternetes_common::resources::service::LoadBalancerIngress;
use rusternetes_common::resources::{
    Container, ContainerStatus, EndpointAddress, EndpointPort, EndpointSubset, Endpoints,
    IntOrString, Pod, PodCondition, PodSpec, PodStatus, Service, ServicePort, ServiceSpec,
    ServiceType,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::endpoints::EndpointsController;
use rusternetes_controller_manager::controllers::loadbalancer::LoadBalancerController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn make_service(name: &str, namespace: &str, selector: HashMap<String, String>) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.namespace = Some(namespace.to_string());
            m
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
            cluster_ip: Some("10.96.5.5".to_string()),
            service_type: Some(ServiceType::ClusterIP),
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

fn make_ready_pod(name: &str, namespace: &str, labels: HashMap<String, String>, ip: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.namespace = Some(namespace.to_string());
            m.labels = Some(labels);
            m
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
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("192.168.1.10".to_string()),
            host_i_ps: None,
            pod_ip: Some(ip.to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            container_statuses: Some(vec![ContainerStatus {
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
            }]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        }),
    }
}

fn make_lb_service(name: &str, namespace: &str) -> Service {
    let mut svc = make_service(name, namespace, HashMap::new());
    svc.spec.service_type = Some(ServiceType::LoadBalancer);
    svc.spec.ports[0].node_port = Some(30080);
    svc
}

/// Sub-bug 1: e2e `service.go:3459` — Endpoints cleanup after Service deletion.
#[tokio::test]
async fn test_endpoints_cleaned_up_after_service_deletion() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "deletion-cleanup".to_string());

    let svc = make_service("svc-delete", "default", selector.clone());
    let svc_key = build_key("services", Some("default"), "svc-delete");
    storage.create(&svc_key, &svc).await.unwrap();

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "deletion-cleanup".to_string());
    let pod = make_ready_pod("p1", "default", labels, "10.244.0.1");
    let pod_key = build_key("pods", Some("default"), "p1");
    storage.create(&pod_key, &pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // Endpoints should now exist
    let ep_key = build_key("endpoints", Some("default"), "svc-delete");
    let _: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints should be created on reconcile");

    // Delete the Service
    storage.delete(&svc_key).await.unwrap();

    // Reconcile again — controller MUST garbage-collect orphan Endpoints
    controller.reconcile_all().await.unwrap();

    let after: Result<Endpoints, _> = storage.get(&ep_key).await;
    assert!(
        after.is_err(),
        "Endpoints {} must be deleted after its Service is deleted (e2e service.go:3459)",
        ep_key
    );
}

/// Sub-bug 2: e2e `service.go:4291` — LoadBalancer status.loadBalancer.ingress
/// must be populated. When no cloud provider is configured the controller
/// still needs to mark the service so the e2e finalization step does not hang.
#[tokio::test]
async fn test_loadbalancer_status_populated_without_cloud_provider() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = LoadBalancerController::new(
        storage.clone(),
        None, // no cloud provider — typical for conformance
        "rusternetes-test".to_string(),
        30,
    );

    let svc = make_lb_service("lb-svc", "default");
    let svc_key = build_key("services", Some("default"), "lb-svc");
    storage.create(&svc_key, &svc).await.unwrap();

    // Run the no-cloud-provider reconciliation. With no provider, the
    // controller must still populate ingress with at least an empty stub
    // so e2e LB-status checks complete.
    controller
        .reconcile_no_cloud_provider()
        .await
        .expect("reconcile_no_cloud_provider should succeed");

    let after: Service = storage.get(&svc_key).await.unwrap();
    let status = after
        .status
        .as_ref()
        .expect("LoadBalancer service must have status set");
    let lb = status
        .load_balancer
        .as_ref()
        .expect("status.loadBalancer must be populated");
    assert!(
        !lb.ingress.is_empty(),
        "status.loadBalancer.ingress must contain at least one entry (e2e service.go:4291)"
    );
    // Verify the ingress entry has some addressable value (ip or hostname)
    let ing: &LoadBalancerIngress = &lb.ingress[0];
    assert!(
        ing.ip.is_some() || ing.hostname.is_some(),
        "ingress entry must carry an ip or hostname"
    );
}

/// Sub-bug 3: e2e `service_latency.go:145` — Endpoints must be published with
/// matching ready pod addresses on the very first reconcile (no extra ticks).
#[tokio::test]
async fn test_endpoints_published_immediately() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "latency".to_string());

    let svc = make_service("latency-svc", "default", selector.clone());
    let svc_key = build_key("services", Some("default"), "latency-svc");
    storage.create(&svc_key, &svc).await.unwrap();

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "latency".to_string());
    let pod = make_ready_pod("p-latency", "default", labels, "10.244.0.7");
    let pod_key = build_key("pods", Some("default"), "p-latency");
    storage.create(&pod_key, &pod).await.unwrap();

    let started = std::time::Instant::now();
    controller.reconcile_all().await.unwrap();
    let elapsed = started.elapsed();

    let ep_key = build_key("endpoints", Some("default"), "latency-svc");
    let ep: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints must exist after first reconcile (latency budget)");

    let ready: Vec<&EndpointAddress> = ep
        .subsets
        .iter()
        .flat_map(|s| s.addresses.as_deref().unwrap_or(&[]))
        .collect();
    assert!(
        ready.iter().any(|a| a.ip == "10.244.0.7"),
        "Endpoints must contain the ready pod IP 10.244.0.7 on first reconcile"
    );
    // A single reconcile must finish well below the upstream 5s budget.
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "first endpoints publish exceeded 2s budget: {:?}",
        elapsed
    );
}

/// Sub-bug 4: e2e `proxy.go:503` — service proxy subresource must resolve to
/// a backend endpoint via Endpoints when no EndpointSlice has been mirrored
/// yet. This test seeds the same shapes the proxy handler reads and asserts
/// that the data the handler depends on is present after reconciliation.
#[tokio::test]
async fn test_service_proxy_resolves_endpoint_from_endpoints() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "proxy-backend".to_string());

    let svc = make_service("proxy-svc", "default", selector.clone());
    let svc_key = build_key("services", Some("default"), "proxy-svc");
    storage.create(&svc_key, &svc).await.unwrap();

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "proxy-backend".to_string());
    let pod = make_ready_pod("proxy-pod", "default", labels, "10.244.0.42");
    let pod_key = build_key("pods", Some("default"), "proxy-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let ep_key = build_key("endpoints", Some("default"), "proxy-svc");
    let ep: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints must exist for proxy resolution");

    // The proxy handler reads `subsets[*].addresses[*].ip` and the matching
    // `subsets[*].ports[*]`. Both must be present for proxy.go:503 to pass.
    let subset: &EndpointSubset = ep
        .subsets
        .first()
        .expect("Endpoints subset required for proxy");
    let addr: &EndpointAddress = subset
        .addresses
        .as_ref()
        .and_then(|a| a.first())
        .expect("Endpoints address required for proxy");
    assert_eq!(addr.ip, "10.244.0.42");

    let ports: &Vec<EndpointPort> = subset
        .ports
        .as_ref()
        .expect("Endpoints subset must include ports for proxy");
    assert!(
        ports.iter().any(|p| p.port == 8080),
        "Endpoints must expose targetPort 8080 for proxy.go:503"
    );
}
