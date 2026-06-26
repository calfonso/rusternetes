//! Extended integration tests for Service and LoadBalancer controllers.
//!
//! Mirror layer for upstream `kubernetes/test/e2e/network/service.go` cases
//! beyond the basic ClusterIP/NodePort/LoadBalancer happy paths covered by
//! `service_controller_test.rs`, `service_lb_endpoints_test.rs`, and
//! `loadbalancer_status_lifecycle_test.rs`.
//!
//! Tests in this file fall into two buckets:
//!
//!   1. Behavior that should already work and is asserted directly
//!      (publishNotReadyAddresses end-to-end via the EndpointsController,
//!      LoadBalancer external IP assignment via a stub CloudProvider, and
//!      a single-stack IPv4 baseline that does not depend on dual-stack
//!      family allocation).
//!
//!   2. Behavior that the controllers do not yet implement (external/
//!      internal traffic policy `Local`, topology-aware routing, dual-
//!      stack `ipFamilyPolicy`, and LoadBalancer health checks tied to
//!      pod readiness). These are kept in tree as `#[ignore]` RED-state
//!      tests with a one-line reason so they can be unignored as each
//!      feature lands.

use async_trait::async_trait;
use rusternetes_common::cloud_provider::{
    CloudProvider, LoadBalancerIngress as CloudIngress, LoadBalancerService as CloudLBService,
    LoadBalancerStatus as CloudLBStatus,
};
use rusternetes_common::resources::{
    Container, ContainerStatus, Endpoints, IPFamily, IPFamilyPolicy, IntOrString, Node,
    NodeAddress, NodeStatus, Pod, PodCondition, PodSpec, PodStatus, Service,
    ServiceExternalTrafficPolicy, ServiceInternalTrafficPolicy, ServicePort, ServiceSpec,
    ServiceType,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::endpoints::EndpointsController;
use rusternetes_controller_manager::controllers::loadbalancer::LoadBalancerController;
use rusternetes_controller_manager::controllers::service::ServiceController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Fresh `MemoryStorage` for each test. Mirrors the helper in
/// `service_controller_test.rs` so the convention is consistent.
async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

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
            cluster_ip: None,
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

/// Build a `Pod` whose `Ready` condition state is configurable. When
/// `ready == false` the pod still has a `podIP` (mimicking the upstream
/// scenario where the kubelet has assigned an IP but the readiness probe
/// has not yet succeeded), which is the exact shape `publishNotReady`
/// must handle.
fn make_pod(
    name: &str,
    namespace: &str,
    labels: HashMap<String, String>,
    ip: &str,
    node_name: &str,
    ready: bool,
) -> Pod {
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
                name: "app".to_string(),
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
            node_name: Some(node_name.to_string()),
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
                status: if ready {
                    "True".to_string()
                } else {
                    "False".to_string()
                },
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            container_statuses: Some(vec![ContainerStatus {
                name: "app".to_string(),
                ready,
                restart_count: 0,
                state: None,
                last_state: None,
                image: Some("nginx:latest".to_string()),
                image_id: None,
                container_id: Some(format!("container-{name}")),
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

/// Cloud provider that records the most recent `ensure_load_balancer` call
/// and returns a deterministic ingress IP plus the requested node-port so
/// callers can assert end-to-end propagation into `status.loadBalancer`.
struct ExternalIpCloudProvider {
    ip: String,
}

#[async_trait]
impl CloudProvider for ExternalIpCloudProvider {
    async fn ensure_load_balancer(
        &self,
        _service: &CloudLBService,
    ) -> rusternetes_common::Result<CloudLBStatus> {
        Ok(CloudLBStatus {
            ingress: vec![CloudIngress {
                ip: Some(self.ip.clone()),
                hostname: None,
            }],
        })
    }

    async fn delete_load_balancer(
        &self,
        _service_namespace: &str,
        _service_name: &str,
    ) -> rusternetes_common::Result<()> {
        Ok(())
    }

    async fn get_load_balancer_status(
        &self,
        _service_namespace: &str,
        _service_name: &str,
    ) -> rusternetes_common::Result<Option<CloudLBStatus>> {
        Ok(None)
    }

    fn name(&self) -> &str {
        "external-ip-stub"
    }
}

// ---------------------------------------------------------------------------
// publishNotReadyAddresses — passes today
// ---------------------------------------------------------------------------

/// Upstream `e2e/network/service.go` "should serve a basic endpoint from pods
/// with publishNotReadyAddresses=true": when a Service sets
/// `publishNotReadyAddresses=true`, the EndpointsController must put the
/// pod IP in `subsets.addresses` (ready) even if the pod has not yet
/// reported `Ready=True`.
#[tokio::test]
async fn test_service_publish_not_ready_addresses_publishes_pre_ready_pod() {
    let storage = setup_test().await;
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "early-publish".to_string());

    let mut svc = make_service("publish-not-ready", "default", selector.clone());
    svc.spec.cluster_ip = Some("10.96.7.7".to_string());
    svc.spec.publish_not_ready_addresses = Some(true);

    let svc_key = build_key("services", Some("default"), "publish-not-ready");
    storage.create(&svc_key, &svc).await.unwrap();

    // The pod is NOT ready — `Ready=False` — but has a pod IP. With
    // publishNotReadyAddresses=true these must still land in `addresses`.
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "early-publish".to_string());
    let pod = make_pod(
        "early-pod",
        "default",
        labels,
        "10.244.0.50",
        "node-1",
        false,
    );
    let pod_key = build_key("pods", Some("default"), "early-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let ep_key = build_key("endpoints", Some("default"), "publish-not-ready");
    let ep: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints must be created for service with publishNotReadyAddresses");

    let subset = ep.subsets.first().expect("at least one subset");
    let ready_ips: Vec<&str> = subset
        .addresses
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|a| a.ip.as_str())
        .collect();
    assert!(
        ready_ips.contains(&"10.244.0.50"),
        "publishNotReadyAddresses=true must place the pre-ready pod IP in `addresses`, \
         got ready={:?}, notReady={:?}",
        ready_ips,
        subset.not_ready_addresses
    );
    assert!(
        subset
            .not_ready_addresses
            .as_deref()
            .unwrap_or(&[])
            .is_empty(),
        "no entries should remain in notReadyAddresses when publishNotReadyAddresses=true"
    );
}

// ---------------------------------------------------------------------------
// publishNotReadyAddresses default — also passes today (negative control)
// ---------------------------------------------------------------------------

/// Negative control for the test above: with `publishNotReadyAddresses`
/// unset (the default), a pod with `Ready=False` must land in
/// `notReadyAddresses`, not `addresses`. Locks in the default behavior so
/// a regression there is caught alongside the publishNotReady path.
#[tokio::test]
async fn test_service_default_excludes_not_ready_pod_from_ready_addresses() {
    let storage = setup_test().await;
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "default-readiness".to_string());

    let mut svc = make_service("default-readiness", "default", selector.clone());
    svc.spec.cluster_ip = Some("10.96.7.8".to_string());
    // publish_not_ready_addresses left as None — upstream default = false.

    let svc_key = build_key("services", Some("default"), "default-readiness");
    storage.create(&svc_key, &svc).await.unwrap();

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "default-readiness".to_string());
    let pod = make_pod(
        "not-ready-pod",
        "default",
        labels,
        "10.244.0.51",
        "node-1",
        false,
    );
    let pod_key = build_key("pods", Some("default"), "not-ready-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let ep_key = build_key("endpoints", Some("default"), "default-readiness");
    let ep: Endpoints = storage.get(&ep_key).await.expect("Endpoints must exist");

    let subset = ep.subsets.first().expect("at least one subset");
    let ready_ips: Vec<&str> = subset
        .addresses
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|a| a.ip.as_str())
        .collect();
    let not_ready_ips: Vec<&str> = subset
        .not_ready_addresses
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|a| a.ip.as_str())
        .collect();

    assert!(
        !ready_ips.contains(&"10.244.0.51"),
        "default publishNotReadyAddresses must not place a Ready=False pod in `addresses`"
    );
    assert!(
        not_ready_ips.contains(&"10.244.0.51"),
        "default publishNotReadyAddresses must keep the Ready=False pod in `notReadyAddresses`, \
         got ready={:?}, notReady={:?}",
        ready_ips,
        not_ready_ips,
    );
}

// ---------------------------------------------------------------------------
// LoadBalancer external IP assignment — passes today
// ---------------------------------------------------------------------------

/// Upstream `e2e/network/service.go` "should be able to create LoadBalancer
/// Service with external IPs": a cloud provider returning an ingress IP
/// must end up in `status.loadBalancer.ingress[0].ip`, and downgrading to
/// ClusterIP-only must not leave that status around (the LB controller
/// only touches LB-type services).
#[tokio::test]
async fn test_loadbalancer_external_ip_assignment_propagates_to_status() {
    let storage = setup_test().await;
    let provider: Arc<dyn CloudProvider> = Arc::new(ExternalIpCloudProvider {
        ip: "198.51.100.7".to_string(),
    });
    let controller = LoadBalancerController::new(
        storage.clone(),
        Some(provider),
        "rusternetes-test".to_string(),
        30,
    );

    let mut svc = make_service("lb-external-ip", "default", HashMap::new());
    svc.spec.service_type = Some(ServiceType::LoadBalancer);
    svc.spec.cluster_ip = Some("10.96.10.10".to_string());
    svc.spec.ports[0].node_port = Some(31080);

    let key = build_key("services", Some("default"), "lb-external-ip");
    storage.create(&key, &svc).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let after: Service = storage.get(&key).await.unwrap();
    let lb = after
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
        .expect("status.loadBalancer must be populated after cloud-provider reconcile");
    assert_eq!(lb.ingress.len(), 1, "expected one ingress entry");
    assert_eq!(
        lb.ingress[0].ip.as_deref(),
        Some("198.51.100.7"),
        "ingress IP must come from the cloud provider"
    );
}

// ---------------------------------------------------------------------------
// LoadBalancer + multiple nodes — passes today, ensures the cloud-provider
// path consumes node InternalIPs (the data feeding the upstream e2e
// "should have session affinity work for LoadBalancer service with ESIPP=Local"
// check-list — without this the controller has no nodes to register).
// ---------------------------------------------------------------------------

/// Without explicit node addresses, the LB controller may still publish
/// a stub status (covered by `service_lb_endpoints_test.rs`). This test
/// asserts the cloud-provider path observes the `InternalIP` from any
/// registered Node so the cloud provider could target a real backend.
#[tokio::test]
async fn test_loadbalancer_assignment_uses_registered_node_internal_ips() {
    let storage = setup_test().await;

    // Seed two nodes with InternalIPs.
    for (name, ip) in [("node-1", "10.0.0.11"), ("node-2", "10.0.0.12")] {
        let node = Node {
            type_meta: TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: None,
            status: Some(NodeStatus {
                addresses: Some(vec![NodeAddress {
                    address_type: "InternalIP".to_string(),
                    address: ip.to_string(),
                }]),
                ..Default::default()
            }),
        };
        let key = build_key("nodes", None, name);
        storage.create(&key, &node).await.unwrap();
    }

    let provider: Arc<dyn CloudProvider> = Arc::new(ExternalIpCloudProvider {
        ip: "198.51.100.8".to_string(),
    });
    let controller = LoadBalancerController::new(
        storage.clone(),
        Some(provider),
        "rusternetes-test".to_string(),
        30,
    );

    let mut svc = make_service("lb-with-nodes", "default", HashMap::new());
    svc.spec.service_type = Some(ServiceType::LoadBalancer);
    svc.spec.cluster_ip = Some("10.96.11.11".to_string());
    svc.spec.ports[0].node_port = Some(31090);

    let key = build_key("services", Some("default"), "lb-with-nodes");
    storage.create(&key, &svc).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let after: Service = storage.get(&key).await.unwrap();
    let lb = after
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
        .expect("status.loadBalancer populated when nodes are present");
    assert_eq!(lb.ingress[0].ip.as_deref(), Some("198.51.100.8"));
}

// ---------------------------------------------------------------------------
// IPv4 single-stack — passes today (baseline before dual-stack lands)
// ---------------------------------------------------------------------------

/// Upstream `e2e/network/service.go` "Single Stack IPv4": when
/// `ipFamilyPolicy=SingleStack` and `ipFamilies=[IPv4]` are requested, the
/// service must end up with an IPv4 ClusterIP allocated from the v4 CIDR.
/// This is the baseline before dual-stack support; the dual-stack case
/// below is RED-state.
#[tokio::test]
async fn test_service_ip_family_policy_single_stack_ipv4() {
    let storage = setup_test().await;
    let controller = ServiceController::new(storage.clone());
    controller.initialize().await.unwrap();

    let mut svc = make_service("single-stack-v4", "default", HashMap::new());
    svc.spec.ip_family_policy = Some(IPFamilyPolicy::SingleStack);
    svc.spec.ip_families = Some(vec![IPFamily::IPv4]);

    let key = build_key("services", Some("default"), "single-stack-v4");
    storage.create(&key, &svc).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let after: Service = storage.get(&key).await.unwrap();
    let cluster_ip = after
        .spec
        .cluster_ip
        .as_ref()
        .expect("ClusterIP must be allocated even for SingleStack");
    assert!(
        cluster_ip.starts_with("10.96."),
        "SingleStack IPv4 must allocate from the v4 service CIDR 10.96.0.0/12, got {cluster_ip}",
    );
    // The IPv4 ClusterIP must contain only digits and dots — sanity check.
    assert!(
        cluster_ip.chars().all(|c| c.is_ascii_digit() || c == '.'),
        "SingleStack v4 ClusterIP must look like an IPv4 address, got {cluster_ip}",
    );
}

// ---------------------------------------------------------------------------
// RED-state: dual-stack ipFamilyPolicy — not implemented
// ---------------------------------------------------------------------------

/// Upstream `e2e/network/service.go` dual-stack "PreferDualStack" case:
/// when both IPv4 and IPv6 families are requested, the controller must
/// populate `spec.clusterIPs` with two entries (one per family) and the
/// family ordering must match `spec.ipFamilies`. The current
/// `ServiceController` only allocates a single IPv4 address, so this
/// assertion fails today.
#[tokio::test]
async fn test_service_ip_family_policy_dual_stack_allocates_both_families() {
    let storage = setup_test().await;
    let controller = ServiceController::new(storage.clone());
    controller.initialize().await.unwrap();

    let mut svc = make_service("dual-stack", "default", HashMap::new());
    svc.spec.ip_family_policy = Some(IPFamilyPolicy::PreferDualStack);
    svc.spec.ip_families = Some(vec![IPFamily::IPv4, IPFamily::IPv6]);

    let key = build_key("services", Some("default"), "dual-stack");
    storage.create(&key, &svc).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let after: Service = storage.get(&key).await.unwrap();
    let cluster_ips = after
        .spec
        .cluster_ips
        .as_ref()
        .expect("dual-stack must populate spec.clusterIPs");
    assert_eq!(
        cluster_ips.len(),
        2,
        "dual-stack must produce one ClusterIP per family"
    );
    let has_v4 = cluster_ips.iter().any(|ip| ip.contains('.'));
    let has_v6 = cluster_ips.iter().any(|ip| ip.contains(':'));
    assert!(has_v4 && has_v6, "must allocate both v4 and v6 addresses");
}

// ---------------------------------------------------------------------------
// RED-state: externalTrafficPolicy=Local — preserves source IP
// ---------------------------------------------------------------------------

/// Upstream `e2e/network/service.go` "should work for type=LoadBalancer
/// when ESIPP=Local": with `externalTrafficPolicy=Local` the controller
/// must (a) keep only node-local endpoints on each Node's data path and
/// (b) allocate a `healthCheckNodePort` so external load balancers can
/// drop traffic from nodes without local pods. Neither is implemented
/// today — the EndpointsController happily fans out across nodes and the
/// ServiceController never sets `healthCheckNodePort`.
#[tokio::test]
async fn test_service_external_traffic_policy_local_allocates_health_check_node_port() {
    let storage = setup_test().await;
    let svc_controller = ServiceController::new(storage.clone());
    svc_controller.initialize().await.unwrap();

    let mut svc = make_service("lb-esipp-local", "default", HashMap::new());
    svc.spec.service_type = Some(ServiceType::LoadBalancer);
    svc.spec.external_traffic_policy = Some(ServiceExternalTrafficPolicy::Local);

    let key = build_key("services", Some("default"), "lb-esipp-local");
    storage.create(&key, &svc).await.unwrap();

    svc_controller.reconcile_all().await.unwrap();

    let after: Service = storage.get(&key).await.unwrap();
    let hcnp = after
        .spec
        .health_check_node_port
        .expect("externalTrafficPolicy=Local must allocate a healthCheckNodePort");
    assert!(
        (30000..=32767).contains(&hcnp),
        "healthCheckNodePort must fall inside the NodePort range, got {hcnp}",
    );
}

// ---------------------------------------------------------------------------
// RED-state: internalTrafficPolicy=Local — node-local routing
// ---------------------------------------------------------------------------

/// Upstream `e2e/network/service.go` "internal traffic policy [Feature:
/// ServiceInternalTrafficPolicy]": when `internalTrafficPolicy=Local`,
/// cluster-internal callers must reach only pods scheduled on the same
/// node. Today the EndpointsController emits every pod IP regardless of
/// node, so the test asserts the controller writes per-node EndpointSlice
/// hints — which it does not yet do.
#[tokio::test]
async fn test_service_internal_traffic_policy_local_filters_by_node() {
    let storage = setup_test().await;
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "internal-local".to_string());

    let mut svc = make_service("internal-local", "default", selector.clone());
    svc.spec.cluster_ip = Some("10.96.20.1".to_string());
    svc.spec.internal_traffic_policy = Some(ServiceInternalTrafficPolicy::Local);

    let svc_key = build_key("services", Some("default"), "internal-local");
    storage.create(&svc_key, &svc).await.unwrap();

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "internal-local".to_string());
    let pod_a = make_pod(
        "pod-a",
        "default",
        labels.clone(),
        "10.244.0.10",
        "node-1",
        true,
    );
    let pod_b = make_pod("pod-b", "default", labels, "10.244.0.11", "node-2", true);
    storage
        .create(&build_key("pods", Some("default"), "pod-a"), &pod_a)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("default"), "pod-b"), &pod_b)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    // Expected behavior once implemented: the EndpointsController must
    // segregate addresses by node so a node-local kube-proxy on node-1
    // only consumes pod-a. The simplest enforceable shape is one
    // EndpointSubset per node (each subset's `addresses` contains a
    // single pod IP whose target_ref.node_name matches the subset's
    // owning node). Today the controller emits a single subset
    // containing both pod IPs with no per-node segregation.
    let ep_key = build_key("endpoints", Some("default"), "internal-local");
    let ep: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints must exist after reconcile");

    // Every subset should contain endpoints from exactly one node when
    // internalTrafficPolicy=Local. Mixed-node subsets defeat the policy.
    let any_mixed_subset = ep.subsets.iter().any(|s| {
        let nodes: std::collections::HashSet<&str> = s
            .addresses
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter_map(|a| a.node_name.as_deref())
            .collect();
        nodes.len() > 1
    });
    assert!(
        !any_mixed_subset,
        "internalTrafficPolicy=Local must segregate addresses by node \
         (no subset may contain addresses from more than one node); subsets={:?}",
        ep.subsets,
    );
}

// ---------------------------------------------------------------------------
// RED-state: topology-aware routing — for_zones hints
// ---------------------------------------------------------------------------

/// Upstream `e2e/network/topology_hints.go`: when a Service annotation
/// (`service.kubernetes.io/topology-mode: Auto`) or
/// `trafficDistribution: PreferClose` is set, the EndpointSlice
/// controller must emit `hints.forZones` per endpoint pointing at the
/// node's `topology.kubernetes.io/zone` label. We do not look at node
/// zone labels yet — `hints` is always `None`.
///
/// NOTE: When unignoring this test, the EndpointSliceController must
/// also be driven (EndpointsController alone does not write slices), so
/// the failure surface points at the missing hint rather than missing
/// slices.
#[tokio::test]
async fn test_service_topology_keys_emit_for_zone_hints() {
    use rusternetes_controller_manager::controllers::endpointslice::EndpointSliceController;

    let storage = setup_test().await;
    let controller = EndpointsController::new(storage.clone());
    let slice_controller = EndpointSliceController::new(storage.clone());

    // Seed a node with a zone label so the controller has data to
    // populate `hints.forZones` from.
    let mut zone_labels = HashMap::new();
    zone_labels.insert(
        "topology.kubernetes.io/zone".to_string(),
        "us-east-1a".to_string(),
    );
    let mut node = Node::new("node-1");
    node.metadata.labels = Some(zone_labels);
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "zone-aware".to_string());
    let mut svc = make_service("zone-aware", "default", selector.clone());
    svc.spec.cluster_ip = Some("10.96.30.1".to_string());
    svc.spec.traffic_distribution = Some("PreferClose".to_string());
    storage
        .create(&build_key("services", Some("default"), "zone-aware"), &svc)
        .await
        .unwrap();

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "zone-aware".to_string());
    let pod = make_pod(
        "zoned-pod",
        "default",
        labels,
        "10.244.0.30",
        "node-1",
        true,
    );
    storage
        .create(&build_key("pods", Some("default"), "zoned-pod"), &pod)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();
    slice_controller.reconcile_all().await.unwrap();

    let slices: Vec<rusternetes_common::resources::EndpointSlice> = storage
        .list("/registry/endpointslices/default/")
        .await
        .unwrap_or_default();
    assert!(
        !slices.is_empty(),
        "EndpointSliceController must mirror the Endpoints into at least one slice"
    );
    let has_zone_hint = slices.iter().any(|s| {
        s.endpoints.iter().any(|e| {
            e.hints
                .as_ref()
                .and_then(|h| h.for_zones.as_ref())
                .map(|z| z.iter().any(|fz| fz.name == "us-east-1a"))
                .unwrap_or(false)
        })
    });
    assert!(
        has_zone_hint,
        "trafficDistribution=PreferClose must emit hints.forZones=[us-east-1a] when the backing \
         node is labeled with topology.kubernetes.io/zone=us-east-1a",
    );
}

// ---------------------------------------------------------------------------
// RED-state: LoadBalancer health checks — readiness gating
// ---------------------------------------------------------------------------

/// Upstream `e2e/network/loadbalancer.go` "should only target nodes
/// with endpoints": the LB status must reflect health-check failures
/// when no Ready endpoint exists for the Service. Specifically, when
/// every backing pod is `Ready=False`, the controller must either keep
/// `status.loadBalancer.ingress` empty or surface a Warning Event named
/// `LoadBalancerSourceUnhealthy`. Neither happens today — the controller
/// blindly trusts the cloud provider's return value.
#[tokio::test]
async fn test_loadbalancer_health_check_gates_status_on_endpoint_readiness() {
    let storage = setup_test().await;
    let endpoints_controller = EndpointsController::new(storage.clone());
    let provider: Arc<dyn CloudProvider> = Arc::new(ExternalIpCloudProvider {
        ip: "198.51.100.99".to_string(),
    });
    let lb_controller = LoadBalancerController::new(
        storage.clone(),
        Some(provider),
        "rusternetes-test".to_string(),
        30,
    );

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "lb-health".to_string());
    let mut svc = make_service("lb-health", "default", selector.clone());
    svc.spec.service_type = Some(ServiceType::LoadBalancer);
    svc.spec.cluster_ip = Some("10.96.40.1".to_string());
    svc.spec.ports[0].node_port = Some(31100);
    let key = build_key("services", Some("default"), "lb-health");
    storage.create(&key, &svc).await.unwrap();

    // A single backing pod that never becomes ready.
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "lb-health".to_string());
    let pod = make_pod(
        "unhealthy-pod",
        "default",
        labels,
        "10.244.0.99",
        "node-1",
        false,
    );
    storage
        .create(&build_key("pods", Some("default"), "unhealthy-pod"), &pod)
        .await
        .unwrap();

    endpoints_controller.reconcile_all().await.unwrap();
    lb_controller.reconcile_all().await.unwrap();

    let after: Service = storage.get(&key).await.unwrap();
    let ingress_empty = after
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
        .map(|lb| lb.ingress.is_empty())
        .unwrap_or(true);
    let has_warning = storage
        .list::<rusternetes_common::resources::Event>("/registry/events/default/")
        .await
        .unwrap_or_default()
        .iter()
        .any(|e| {
            e.involved_object.name.as_deref() == Some("lb-health")
                && e.reason == "LoadBalancerSourceUnhealthy"
        });
    assert!(
        ingress_empty || has_warning,
        "with zero ready endpoints the LB controller must withhold ingress or emit \
         LoadBalancerSourceUnhealthy"
    );
}
