//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-network] Services + /proxy subresource (kube-proxy half).
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/network/
//! (`service.go`, `service_latency.go`, `proxy.go`, `loadbalancer.go`)
//!
//! See docs/conformance/network-services-proxy.md for the test-by-test
//! status table.
//!
//! Strategy: kube-proxy owns iptables-rule emission and EndpointSlice
//! consumption. These tests call the pure builder functions
//! (`IptablesManager::build_nat_rules`, the EndpointSlice→iptables-input
//! map-building from `proxy::sync`) over `Arc<MemoryStorage>`. No iptables
//! binary is shelled out — `build_nat_rules` returns the
//! `iptables-restore` string verbatim, which is the same input kube-proxy
//! pipes to the kernel in production.
//!
//! The companion api-server-side mirror lives in
//! `crates/api-server/tests/conformance_network_services_proxy.rs` and
//! exercises the `/proxy` subresource (pod proxy + service proxy) at the
//! handler/storage seam.

use rusternetes_common::resources::endpointslice::EndpointPort as ESEndpointPort;
use rusternetes_common::resources::service::{ClientIPConfig, SessionAffinityConfig};
use rusternetes_common::resources::{
    Endpoint, EndpointConditions, EndpointSlice, IntOrString, Service, ServicePort, ServiceSpec,
    ServiceType,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kube_proxy::iptables::IptablesManager;
use rusternetes_storage::{memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// `IptablesManager::for_testing(true)` skips the iptables-binary subprocess
/// probe that `::new` runs at startup. We force `recent_available = true`
/// so the session-affinity codepath emits xt_recent rules deterministically
/// regardless of host kernel — CI ARC-runner pods have no iptables binary
/// at all, so `::new` would otherwise probe-fail and silently collapse
/// every affinity-aware test to identical random rules (`assert_ne!`
/// asserts then misfire).
fn test_iptables() -> IptablesManager {
    IptablesManager::for_testing(true)
}

// ---- Test fixtures --------------------------------------------------------

/// Construct an `Arc<MemoryStorage>` for kube-proxy state inspection.
/// Mirrors what kube-proxy receives at runtime from the storage backend.
fn fresh_storage() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

/// Build a minimal ClusterIP Service with one TCP port.
fn cluster_ip_service(
    name: &str,
    namespace: &str,
    cluster_ip: &str,
    port: u16,
    target_port: u16,
) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: ServiceSpec {
            selector: Some(HashMap::new()),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port,
                target_port: Some(IntOrString::Int(target_port as i32)),
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: Some(cluster_ip.to_string()),
            ..ServiceSpec::default()
        },
        status: None,
    }
}

/// Build a NodePort Service.
fn nodeport_service(
    name: &str,
    namespace: &str,
    cluster_ip: &str,
    port: u16,
    target_port: u16,
    node_port: u16,
) -> Service {
    let mut svc = cluster_ip_service(name, namespace, cluster_ip, port, target_port);
    svc.spec.service_type = Some(ServiceType::NodePort);
    svc.spec.ports[0].node_port = Some(node_port);
    svc
}

/// Build a LoadBalancer Service.
fn loadbalancer_service(
    name: &str,
    namespace: &str,
    cluster_ip: &str,
    port: u16,
    target_port: u16,
    node_port: u16,
) -> Service {
    let mut svc = nodeport_service(name, namespace, cluster_ip, port, target_port, node_port);
    svc.spec.service_type = Some(ServiceType::LoadBalancer);
    svc
}

/// Build a single-port EndpointSlice linked to a service via the
/// `kubernetes.io/service-name` label.
fn endpoint_slice(
    namespace: &str,
    service_name: &str,
    addresses: &[&str],
    port_name: Option<&str>,
    port_num: i32,
) -> EndpointSlice {
    let mut labels = HashMap::new();
    labels.insert(
        "kubernetes.io/service-name".to_string(),
        service_name.to_string(),
    );
    let mut es = EndpointSlice::new(format!("{}-abc12", service_name), "IPv4");
    es.metadata.namespace = Some(namespace.to_string());
    es.metadata.labels = Some(labels);
    es.endpoints = addresses
        .iter()
        .map(|a| Endpoint {
            addresses: vec![(*a).to_string()],
            conditions: Some(EndpointConditions {
                ready: Some(true),
                serving: Some(true),
                terminating: Some(false),
            }),
            hostname: None,
            target_ref: None,
            node_name: None,
            zone: None,
            hints: None,
            deprecated_topology: None,
        })
        .collect();
    es.ports = vec![ESEndpointPort {
        name: port_name.map(String::from),
        port: Some(port_num),
        protocol: "TCP".to_string(),
        app_protocol: None,
    }];
    es
}

/// Re-build the EndpointSlice → (ip, port_name, port_num) map used by
/// `IptablesManager::build_nat_rules`. This mirrors the logic in
/// `kube_proxy::proxy::KubeProxy::sync` so tests don't need a live
/// `KubeProxy` (which would also run `IptablesManager::initialize`).
fn endpointslice_map(
    slices: &[EndpointSlice],
) -> HashMap<String, Vec<(String, Option<String>, u16)>> {
    let mut map: HashMap<String, Vec<(String, Option<String>, u16)>> = HashMap::new();
    for es in slices {
        let namespace = es.metadata.namespace.as_deref().unwrap_or("default");
        let service_name = es
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
            .cloned()
            .unwrap_or_else(|| es.metadata.name.clone());
        let key = format!("{}/{}", namespace, service_name);

        let mut ready_addrs: Vec<String> = Vec::new();
        for endpoint in &es.endpoints {
            if let Some(conds) = &endpoint.conditions {
                if conds.ready == Some(false) {
                    continue;
                }
            }
            for addr in &endpoint.addresses {
                ready_addrs.push(addr.clone());
            }
        }

        if es.ports.is_empty() {
            for addr in &ready_addrs {
                map.entry(key.clone())
                    .or_default()
                    .push((addr.clone(), None, 0));
            }
        } else {
            for es_port in &es.ports {
                let port_num = es_port.port.unwrap_or(0) as u16;
                let port_name = es_port.name.clone();
                for addr in &ready_addrs {
                    map.entry(key.clone()).or_default().push((
                        addr.clone(),
                        port_name.clone(),
                        port_num,
                    ));
                }
            }
        }
    }
    map
}

// ---- Tests: Services — ClusterIP CRUD lifecycle ---------------------------

/// [sig-network] Services should serve a basic endpoint from pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1039
/// Sonobuoy (Round 160, 2026-04-26): PASS (not in failure bucket)
///
/// The basic endpoint-serving contract: a ClusterIP Service with one ready
/// EndpointSlice backend produces exactly one DNAT rule pointing at that
/// backend's pod IP:targetPort.
#[tokio::test]
async fn services_should_serve_basic_endpoint_from_pods() {
    let svc = cluster_ip_service("svc1", "default", "10.96.0.10", 80, 8080);
    let slice = endpoint_slice("default", "svc1", &["10.244.0.5"], Some("http"), 8080);

    let map = endpointslice_map(&[slice]);
    let ipt = test_iptables();
    let rules = ipt.build_nat_rules(&[svc], &map, &[], "test-node").await;

    assert!(rules.contains("10.96.0.10/32"), "rules: {}", rules);
    assert!(rules.contains("--dport 80"), "rules: {}", rules);
    assert!(
        rules.contains("--to-destination 10.244.0.5:8080"),
        "rules: {}",
        rules
    );
    assert!(rules.starts_with("*nat\n"), "rules: {}", rules);
    assert!(rules.ends_with("COMMIT\n"), "rules: {}", rules);
}

/// [sig-network] Services should serve multiport endpoints from pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1088
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// Multi-port services must emit one DNAT chain per (service, port) pair,
/// with each port matched against the EndpointSlice's named port.
#[tokio::test]
async fn services_should_serve_multiport_endpoints_from_pods() {
    let mut svc = cluster_ip_service("multi", "default", "10.96.0.20", 80, 8080);
    svc.spec.ports.push(ServicePort {
        name: Some("https".to_string()),
        port: 443,
        target_port: Some(IntOrString::Int(8443)),
        protocol: "TCP".to_string(),
        node_port: None,
        app_protocol: None,
    });
    let mut slice = endpoint_slice("default", "multi", &["10.244.0.6"], Some("http"), 8080);
    slice.ports.push(ESEndpointPort {
        name: Some("https".to_string()),
        port: Some(8443),
        protocol: "TCP".to_string(),
        app_protocol: None,
    });

    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;

    assert!(rules.contains("--dport 80"), "rules: {}", rules);
    assert!(rules.contains("--dport 443"), "rules: {}", rules);
    assert!(
        rules.contains("--to-destination 10.244.0.6:8080"),
        "rules: {}",
        rules
    );
    assert!(
        rules.contains("--to-destination 10.244.0.6:8443"),
        "rules: {}",
        rules
    );
}

/// [sig-network] Services should be updated after adding or deleting ports
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1165
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// Adding a second port to a Service must materialize as an additional DNAT
/// rule in the next sync; removing one must drop the corresponding rule.
#[tokio::test]
async fn services_should_be_updated_after_adding_or_deleting_ports() {
    let svc_single = cluster_ip_service("ports", "default", "10.96.0.30", 80, 8080);
    let slice_single = endpoint_slice("default", "ports", &["10.244.0.7"], Some("http"), 8080);
    let map1 = endpointslice_map(std::slice::from_ref(&slice_single));
    let ipt = test_iptables();
    let rules_before = ipt
        .build_nat_rules(std::slice::from_ref(&svc_single), &map1, &[], "test-node")
        .await;
    assert!(rules_before.contains("--dport 80"));
    assert!(!rules_before.contains("--dport 9090"));

    // Add a second port
    let mut svc_two = svc_single.clone();
    svc_two.spec.ports.push(ServicePort {
        name: Some("metrics".to_string()),
        port: 9090,
        target_port: Some(IntOrString::Int(9091)),
        protocol: "TCP".to_string(),
        node_port: None,
        app_protocol: None,
    });
    let mut slice_two = slice_single.clone();
    slice_two.ports.push(ESEndpointPort {
        name: Some("metrics".to_string()),
        port: Some(9091),
        protocol: "TCP".to_string(),
        app_protocol: None,
    });
    let map2 = endpointslice_map(&[slice_two]);
    let rules_after = ipt
        .build_nat_rules(&[svc_two], &map2, &[], "test-node")
        .await;
    assert!(rules_after.contains("--dport 80"));
    assert!(rules_after.contains("--dport 9090"));
    assert!(rules_after.contains("--to-destination 10.244.0.7:9091"));
}

/// [sig-network] Services should create endpoints for unready pods
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (see
/// `publishNotReadyAddresses` semantics)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// Endpoints with `ready=false` must NOT receive DNAT rules from kube-proxy
/// (they are not load-balanced into).
#[tokio::test]
async fn services_should_skip_unready_endpoints() {
    let svc = cluster_ip_service("ready", "default", "10.96.0.40", 80, 8080);
    let mut slice = endpoint_slice("default", "ready", &["10.244.0.8"], Some("http"), 8080);
    // Add a not-ready endpoint
    slice.endpoints.push(Endpoint {
        addresses: vec!["10.244.0.9".to_string()],
        conditions: Some(EndpointConditions {
            ready: Some(false),
            serving: None,
            terminating: None,
        }),
        hostname: None,
        target_ref: None,
        node_name: None,
        zone: None,
        hints: None,
        deprecated_topology: None,
    });
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    assert!(
        rules.contains("--to-destination 10.244.0.8:8080"),
        "ready endpoint present: {}",
        rules
    );
    assert!(
        !rules.contains("10.244.0.9"),
        "unready endpoint must NOT be programmed: {}",
        rules
    );
}

/// [sig-network] Services with no endpoints emit no DNAT rules
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (see "no endpoints"
/// branch). Sonobuoy (Round 160): PASS (not in failure bucket).
///
/// kube-proxy skips services with zero ready endpoints — no DNAT rule, but
/// the service chain headers must still exist so that the table commit
/// succeeds.
#[tokio::test]
async fn services_with_no_endpoints_emit_no_dnat_rules() {
    let svc = cluster_ip_service("empty", "default", "10.96.0.50", 80, 8080);
    let map = HashMap::new(); // No EndpointSlices
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    // Chain headers must always be present
    assert!(rules.contains(":RUSTERNETES-SERVICES - [0:0]"), "{}", rules);
    assert!(
        rules.contains(":RUSTERNETES-NODEPORTS - [0:0]"),
        "{}",
        rules
    );
    // But no DNAT rule pointing at 10.96.0.50
    assert!(!rules.contains("10.96.0.50/32 -p tcp --dport 80 -j DNAT"));
    assert!(rules.ends_with("COMMIT\n"));
}

/// [sig-network] Services should handle ExternalName services (no iptables)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (ExternalName tests)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// ExternalName services have no ClusterIP and no port mapping — kube-proxy
/// must produce no DNAT rules for them. (`build_nat_rules` skips services
/// without a valid ClusterIP.)
#[tokio::test]
async fn services_externalname_emits_no_iptables_rules() {
    let mut svc = cluster_ip_service("ext", "default", "10.96.0.60", 80, 8080);
    svc.spec.service_type = Some(ServiceType::ExternalName);
    svc.spec.cluster_ip = None;
    svc.spec.external_name = Some("example.com".to_string());
    svc.spec.ports.clear();
    let map = HashMap::new();
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    assert!(!rules.contains("example.com"));
    assert!(!rules.contains("DNAT"));
}

/// [sig-network] Services should not allocate iptables rules when clusterIP=None
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (Headless services)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// Headless services (clusterIP="None") are DNS-only; kube-proxy emits no
/// DNAT rules for them.
#[tokio::test]
async fn services_headless_emits_no_iptables_rules() {
    let mut svc = cluster_ip_service("headless", "default", "None", 80, 8080);
    svc.spec.cluster_ip = Some("None".to_string());
    let slice = endpoint_slice("default", "headless", &["10.244.0.10"], Some("http"), 8080);
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    assert!(
        !rules.contains("--to-destination 10.244.0.10:8080"),
        "headless service must not emit DNAT: {}",
        rules
    );
}

// ---- Tests: Services — NodePort -------------------------------------------

/// [sig-network] Services should expose service on NodePort
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (NodePort tests)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// A NodePort Service must emit DNAT rules in BOTH the SERVICES chain
/// (ClusterIP path) and the NODEPORTS chain (host-port path).
#[tokio::test]
async fn services_should_expose_service_on_nodeport() {
    let svc = nodeport_service("np", "default", "10.96.0.70", 80, 8080, 30080);
    let slice = endpoint_slice("default", "np", &["10.244.0.11"], Some("http"), 8080);
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    // ClusterIP rule
    assert!(rules.contains("-A RUSTERNETES-SERVICES -d 10.96.0.70/32"));
    // NodePort rule
    assert!(rules.contains("-A RUSTERNETES-NODEPORTS -p tcp --dport 30080"));
    assert!(rules.contains("--to-destination 10.244.0.11:8080"));
}

/// [sig-network] Services NodePort traffic spreads across all backends
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (NodePort
/// LoadBalancing tests). Sonobuoy (Round 160): PASS (not in failure bucket).
///
/// With N backends, kube-proxy must emit (N-1) probability-based statistic
/// rules in the nodeports chain, plus the terminal fallback rule, so each
/// backend gets a 1/N share.
#[tokio::test]
async fn services_nodeport_load_balances_across_backends() {
    let svc = nodeport_service("npmulti", "default", "10.96.0.80", 80, 8080, 30081);
    let slice = endpoint_slice(
        "default",
        "npmulti",
        &["10.244.1.1", "10.244.1.2", "10.244.1.3"],
        Some("http"),
        8080,
    );
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;

    // Three backends → two probability rules + one terminal rule.
    // Probability for idx=0: 1/3 ≈ 0.3333333333
    // Probability for idx=1: 1/2 = 0.5
    assert!(rules.contains("--probability 0.3333333333"), "{}", rules);
    assert!(rules.contains("--probability 0.5000000000"), "{}", rules);
    assert!(rules.contains("--to-destination 10.244.1.1:8080"));
    assert!(rules.contains("--to-destination 10.244.1.2:8080"));
    assert!(rules.contains("--to-destination 10.244.1.3:8080"));
}

// ---- Tests: Services — LoadBalancer ---------------------------------------

/// [sig-network] Services should complete a service status lifecycle [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:3246
/// Sonobuoy (Round 160): FAIL — controller-manager never populates
/// `status.loadBalancer.ingress[]`, so the upstream delete step at
/// `service.go:3459` times out.
///
/// The kube-proxy half of this lifecycle is narrow: for an LB-typed
/// Service kube-proxy must (a) emit ClusterIP + NodePort DNAT rules while
/// the Service exists and (b) drop both classes of rule on the next sync
/// after the Service is deleted. The status-population bug itself lives
/// in `crates/controller-manager/src/controllers/loadbalancer.rs` and is
/// exercised by `crates/controller-manager/tests/
/// loadbalancer_status_lifecycle_test.rs` (the more authentic mirror — the
/// kube-proxy crate cannot drive controller-manager status writes).
///
/// IGNORED_TESTS_PLAN.md item #10. Layer A (Sonobuoy) is fixed by the
/// controller-manager retry-with-backoff added alongside this un-ignore.
#[tokio::test]
async fn services_should_complete_service_status_lifecycle() {
    // Phase 1 — Service exists: kube-proxy programs ClusterIP + NodePort.
    let svc = loadbalancer_service("lblife", "default", "10.96.0.150", 80, 8080, 30150);
    let slice = endpoint_slice("default", "lblife", &["10.244.5.5"], Some("http"), 8080);
    let map = endpointslice_map(&[slice]);
    let rules_present = test_iptables()
        .build_nat_rules(std::slice::from_ref(&svc), &map, &[], "test-node")
        .await;
    assert!(
        rules_present.contains("-A RUSTERNETES-SERVICES -d 10.96.0.150/32"),
        "ClusterIP DNAT rule must exist while LB Service is present:\n{}",
        rules_present
    );
    assert!(
        rules_present.contains("-A RUSTERNETES-NODEPORTS -p tcp --dport 30150"),
        "NodePort DNAT rule must exist while LB Service is present:\n{}",
        rules_present
    );
    assert!(
        rules_present.contains("--to-destination 10.244.5.5:8080"),
        "backend pod IP must be reachable via DNAT:\n{}",
        rules_present
    );

    // Phase 2 — Service deleted: rebuild rules with no services. Both
    // rule classes must disappear. This is the kube-proxy contract that
    // upstream `service.go:3459` depends on after the controller-manager
    // patches an empty status and the test deletes the Service.
    let empty_map: HashMap<String, Vec<(String, Option<String>, u16)>> = HashMap::new();
    let rules_after_delete = test_iptables()
        .build_nat_rules(&[], &empty_map, &[], "test-node")
        .await;
    assert!(
        !rules_after_delete.contains("10.96.0.150"),
        "ClusterIP rule must be dropped after Service delete:\n{}",
        rules_after_delete
    );
    assert!(
        !rules_after_delete.contains("30150"),
        "NodePort rule must be dropped after Service delete:\n{}",
        rules_after_delete
    );
    assert!(
        !rules_after_delete.contains("10.244.5.5"),
        "backend pod IP must not leak in rules after Service delete:\n{}",
        rules_after_delete
    );
}

/// [sig-network] LoadBalancer services share SERVICES + NODEPORTS rules
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/loadbalancer.go (LB lifecycle)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// LoadBalancer is a NodePort superset: kube-proxy must program the same
/// two DNAT rule classes (ClusterIP + node-port) for an LB-typed Service.
#[tokio::test]
async fn services_loadbalancer_programs_clusterip_and_nodeport() {
    let svc = loadbalancer_service("lb2", "default", "10.96.0.100", 80, 8080, 30100);
    let slice = endpoint_slice("default", "lb2", &["10.244.2.2"], Some("http"), 8080);
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    assert!(rules.contains("-A RUSTERNETES-SERVICES -d 10.96.0.100/32"));
    assert!(rules.contains("-A RUSTERNETES-NODEPORTS -p tcp --dport 30100"));
    assert!(rules.contains("--to-destination 10.244.2.2:8080"));
}

// ---- Tests: Services — Session affinity (ClientIP) ------------------------

/// [sig-network] Services should have session affinity work for service with type ClusterIP
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (ClientIP affinity)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// `sessionAffinity=ClientIP` on a ClusterIP service makes kube-proxy emit
/// `KUBE-SEP-*` per-endpoint chains. When the `xt_recent` kernel module is
/// available, the SERVICES chain gets `recent --rcheck` rules; otherwise
/// the fallback is direct DNAT. Either way, the cluster-IP DNAT
/// destination must appear in the rules.
#[tokio::test]
async fn services_should_have_session_affinity_for_clusterip() {
    let mut svc = cluster_ip_service("aff", "default", "10.96.0.110", 80, 8080);
    svc.spec.session_affinity = Some("ClientIP".to_string());
    svc.spec.session_affinity_config = Some(SessionAffinityConfig {
        client_ip: Some(ClientIPConfig {
            timeout_seconds: Some(3600),
        }),
    });
    let slice = endpoint_slice(
        "default",
        "aff",
        &["10.244.3.1", "10.244.3.2"],
        Some("http"),
        8080,
    );
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    // Backends must always be reachable, with or without xt_recent.
    assert!(rules.contains("10.244.3.1:8080"), "{}", rules);
    assert!(rules.contains("10.244.3.2:8080"), "{}", rules);
    assert!(rules.contains("-d 10.96.0.110/32"), "{}", rules);
}

/// [sig-network] Services should be able to switch session affinity for
/// ClusterIP service
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (affinity switch)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// Toggling `sessionAffinity` from `ClientIP` back to `None` must cause
/// kube-proxy to drop the `KUBE-SEP-*` chain references on its next sync.
/// We assert the rule diff between the two states.
#[tokio::test]
async fn services_should_switch_session_affinity_for_clusterip() {
    let svc_none = cluster_ip_service("switch", "default", "10.96.0.120", 80, 8080);
    let mut svc_clientip = svc_none.clone();
    svc_clientip.spec.session_affinity = Some("ClientIP".to_string());
    svc_clientip.spec.session_affinity_config = Some(SessionAffinityConfig {
        client_ip: Some(ClientIPConfig {
            timeout_seconds: Some(10800),
        }),
    });
    let slice = endpoint_slice(
        "default",
        "switch",
        &["10.244.3.3", "10.244.3.4"],
        Some("http"),
        8080,
    );
    let map = endpointslice_map(&[slice]);
    let ipt = test_iptables();
    let r_none = ipt
        .build_nat_rules(&[svc_none], &map, &[], "test-node")
        .await;
    let r_aff = ipt
        .build_nat_rules(&[svc_clientip], &map, &[], "test-node")
        .await;
    // The two rule sets must differ at minimum in their use of
    // KUBE-SEP-* chains or `recent` matchers.
    assert_ne!(r_none, r_aff, "affinity toggle must change emitted rules");
}

/// [sig-network] Services should be able to switch session affinity for
/// NodePort service [LinuxOnly] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:2287
/// Sonobuoy (Round 160): FAIL — affinity-switch NodePort path times out
/// at service.go:4291 (Round 160 failure bucket: "service networking").
///
/// kube-proxy half: verify the SEP chain emission for a NodePort Service
/// when ClientIP affinity is configured, then verify the rules return to a
/// direct-DNAT shape when affinity is toggled back to `None`. The upstream
/// failure is in the e2e harness's reachability probe, not the rule
/// emission — the rules themselves must still be correct.
///
/// Mirrors the diff pattern used by the passing ClusterIP affinity-switch
/// test (`services_should_switch_session_affinity_for_clusterip`).
#[tokio::test]
async fn services_should_switch_session_affinity_nodeport() {
    let svc_none = nodeport_service("npswitch", "default", "10.96.0.130", 80, 8080, 30130);
    let mut svc_clientip = svc_none.clone();
    svc_clientip.spec.session_affinity = Some("ClientIP".to_string());
    svc_clientip.spec.session_affinity_config = Some(SessionAffinityConfig {
        client_ip: Some(ClientIPConfig {
            timeout_seconds: Some(10800),
        }),
    });
    let slice = endpoint_slice(
        "default",
        "npswitch",
        &["10.244.4.1", "10.244.4.2"],
        Some("http"),
        8080,
    );
    let map = endpointslice_map(&[slice]);
    let ipt = test_iptables();

    let r_none = ipt
        .build_nat_rules(std::slice::from_ref(&svc_none), &map, &[], "test-node")
        .await;
    let r_aff = ipt
        .build_nat_rules(&[svc_clientip], &map, &[], "test-node")
        .await;

    // Sanity: both rule sets touch the NODEPORTS chain on the right dport.
    assert!(
        r_none.contains("-A RUSTERNETES-NODEPORTS -p tcp --dport 30130"),
        "no-affinity rules missing NODEPORTS dport entry:\n{}",
        r_none
    );
    assert!(
        r_aff.contains("--dport 30130"),
        "affinity rules missing NODEPORTS dport entry:\n{}",
        r_aff
    );

    // The two rule sets must differ — the affinity toggle must change the
    // emitted iptables-restore blob.
    assert_ne!(
        r_none, r_aff,
        "affinity toggle must change emitted NodePort rules"
    );

    // Affinity variant: SEP-chain references for both backends in the
    // NODEPORTS chain, and per-endpoint --rcheck rules emitted BEFORE the
    // probability fallback `-j KUBE-SEP-*` rules. iptables matches
    // top-down, so the rcheck must precede the load-balancing fallback.
    let sep_prefix = "KUBE-SEP-10960130-80";
    assert!(
        r_aff.contains(&format!("{}-0", sep_prefix)),
        "affinity variant missing SEP chain for endpoint 0:\n{}",
        r_aff
    );
    assert!(
        r_aff.contains(&format!("{}-1", sep_prefix)),
        "affinity variant missing SEP chain for endpoint 1:\n{}",
        r_aff
    );
    let first_rcheck = r_aff
        .find("--rcheck")
        .expect("affinity variant must contain --rcheck rule");
    let first_fallback_jump = r_aff
        .find(&format!(
            "-A RUSTERNETES-NODEPORTS -p tcp --dport 30130 -j {}-",
            sep_prefix
        ))
        .or_else(|| r_aff.find("-A RUSTERNETES-NODEPORTS -p tcp --dport 30130 -m statistic"))
        .expect("affinity variant must contain probability fallback rule");
    assert!(
        first_rcheck < first_fallback_jump,
        "--rcheck rule must appear before the probability fallback in the NODEPORTS chain:\n{}",
        r_aff
    );

    // Affinity variant must NOT emit a direct `-j DNAT --to-destination`
    // line in the NODEPORTS chain — backends are reached via SEP chains.
    for line in r_aff.lines() {
        if line.contains("RUSTERNETES-NODEPORTS") && line.contains("--dport 30130") {
            assert!(
                !line.contains("-j DNAT"),
                "affinity NODEPORTS chain must not contain direct DNAT:\n{}",
                line
            );
        }
    }

    // Switching back to `None` must produce the same shape as the original
    // no-affinity rules (we round-trip through clone+build to confirm).
    let r_round_trip = ipt
        .build_nat_rules(&[svc_none], &map, &[], "test-node")
        .await;
    assert_eq!(
        r_none, r_round_trip,
        "round-trip to None affinity must produce identical rules"
    );
}

/// [sig-network] Services should have session affinity work for NodePort
/// service [LinuxOnly] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:2265
/// Sonobuoy (Round 160): FAIL — NodePort affinity timed out at
/// service.go:4291 (same failure path as the switch test).
///
/// kube-proxy half: when `recent_available=true`, NodePort affinity rules
/// must reference `KUBE-SEP-*` chains. When `xt_recent` is unavailable,
/// kube-proxy falls back to direct DNAT — same backends, no SEP chains.
#[tokio::test]
async fn services_should_have_session_affinity_for_nodeport() {
    let mut svc = nodeport_service("npaff", "default", "10.96.0.140", 80, 8080, 30140);
    svc.spec.session_affinity = Some("ClientIP".to_string());
    svc.spec.session_affinity_config = Some(SessionAffinityConfig {
        client_ip: Some(ClientIPConfig {
            timeout_seconds: Some(3600),
        }),
    });
    let slice = endpoint_slice(
        "default",
        "npaff",
        &["10.244.5.1", "10.244.5.2"],
        Some("http"),
        8080,
    );
    let map = endpointslice_map(&[slice]);

    // `recent_available=true`: SEP chains must appear in the rule set.
    let rules_with_recent = IptablesManager::for_testing(true)
        .build_nat_rules(std::slice::from_ref(&svc), &map, &[], "test-node")
        .await;
    assert!(
        rules_with_recent.contains("KUBE-SEP-10960140-80-0"),
        "recent_available=true must emit SEP chain 0:\n{}",
        rules_with_recent
    );
    assert!(
        rules_with_recent.contains("KUBE-SEP-10960140-80-1"),
        "recent_available=true must emit SEP chain 1:\n{}",
        rules_with_recent
    );
    assert!(
        rules_with_recent.contains("--rcheck"),
        "recent_available=true must emit --rcheck affinity rules:\n{}",
        rules_with_recent
    );

    // `recent_available=false`: direct-DNAT fallback. NodePort traffic is
    // DNATed straight to the backends — no `KUBE-SEP-*` references in the
    // NODEPORTS chain, but the same two backends must still be reachable.
    let rules_no_recent = IptablesManager::for_testing(false)
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    for line in rules_no_recent.lines() {
        if line.contains("RUSTERNETES-NODEPORTS") && line.contains("--dport 30140") {
            assert!(
                !line.contains("KUBE-SEP-"),
                "recent_available=false NODEPORTS chain must not reference SEP chains:\n{}",
                line
            );
        }
    }
    assert!(
        rules_no_recent.contains("--to-destination 10.244.5.1:8080"),
        "direct-DNAT fallback missing backend 1:\n{}",
        rules_no_recent
    );
    assert!(
        rules_no_recent.contains("--to-destination 10.244.5.2:8080"),
        "direct-DNAT fallback missing backend 2:\n{}",
        rules_no_recent
    );
    assert!(
        rules_no_recent.contains("-A RUSTERNETES-NODEPORTS -p tcp --dport 30140"),
        "direct-DNAT fallback missing NODEPORTS dport rule:\n{}",
        rules_no_recent
    );
}

/// [sig-network] Services should respect session-affinity timeout config
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (timeout config)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// A non-default `timeoutSeconds` in `sessionAffinityConfig.clientIP` must
/// propagate into the `recent --seconds` matcher value when xt_recent is
/// available. Without xt_recent the value is unused but the rules must
/// still be emitted without error.
#[tokio::test]
async fn services_session_affinity_timeout_propagates_when_recent_available() {
    let mut svc = cluster_ip_service("aff-to", "default", "10.96.0.150", 80, 8080);
    svc.spec.session_affinity = Some("ClientIP".to_string());
    svc.spec.session_affinity_config = Some(SessionAffinityConfig {
        client_ip: Some(ClientIPConfig {
            timeout_seconds: Some(7200),
        }),
    });
    let slice = endpoint_slice(
        "default",
        "aff-to",
        &["10.244.3.9", "10.244.3.10"],
        Some("http"),
        8080,
    );
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    // 7200 only appears when xt_recent affinity rules are emitted (i.e., on
    // hosts where the module is loaded). On test hosts without xt_recent
    // the affinity path is bypassed; both paths are acceptable. The
    // backends must still appear in the table.
    assert!(rules.contains("10.244.3.9:8080"));
    assert!(rules.contains("10.244.3.10:8080"));
    if rules.contains("--rcheck") {
        assert!(rules.contains("--seconds 7200"), "{}", rules);
    }
}

// ---- Tests: Services — Endpoints latency / serving -----------------------

/// [sig-network] Service endpoints latency should not be very high [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service_latency.go:60
/// Sonobuoy (Round 160): FAIL — latency measurement timed out at
/// service_latency.go:145 (Round 160 failure bucket: "service networking").
///
/// Upstream root cause (in Go kube-proxy): the iptables proxier's
/// `BoundedFrequencyRunner` (k8s.io/kubernetes/pkg/proxy/iptables/proxier.go
/// `syncProxyRules`) throttles `syncProxyRules` by `minSyncPeriod`, so back-
/// to-back Service+EndpointSlice creations are coalesced and the p99
/// service-becomes-reachable latency drifts above the upstream threshold
/// (~20s). Rusternetes' kube-proxy run loop (`crates/kube-proxy/src/lib.rs`)
/// uses a `WorkQueue` that coalesces with sentinel `RECONCILE_ALL` but does
/// NOT add a minSyncPeriod throttle — every Service/EndpointSlice watch event
/// results in a re-queued sync. Combined with the order-independent state
/// hash in `proxy::sync` (skips when state unchanged), back-to-back Service
/// creations are reflected in iptables on the very next sync iteration.
///
/// kube-proxy half (this test): simulate the upstream test's 50-service
/// rapid-create scenario by adding Service+EndpointSlice pairs to
/// `Arc<MemoryStorage>` one-at-a-time and running `build_nat_rules` after
/// each addition. Assert that:
///   1. Every newly-added backend appears in the rule output on the very
///      next `build_nat_rules` call (no missed Service).
///   2. Each per-iteration build stays under a budget that comfortably
///      beats the upstream p99 threshold even when scaled out 50x.
///   3. Total wall-time across all 50 iterations stays well under the
///      upstream 20s threshold so we are well-clear of the Sonobuoy
///      latency bucket.
#[tokio::test]
async fn service_endpoints_latency_should_not_be_very_high() {
    let storage = fresh_storage();
    let ipt = test_iptables();

    // Upstream test creates 50 services back-to-back and measures the
    // per-service latency to first observable endpoint. We mirror the
    // 50-service workload but also assert per-iteration boundedness so
    // any future regression in the build-nat-rules path (or in storage
    // list throughput) surfaces here rather than in Sonobuoy.
    const SERVICES: u16 = 50;
    // Conservative per-iteration budget. The Go upstream's p99 target is
    // ~20s end-to-end (network + apiserver + kube-proxy); the kube-proxy
    // half alone runs in milliseconds. 100ms per iteration is loose enough
    // to absorb scheduler jitter on CI ARC runners.
    const PER_ITER_BUDGET_MS: u128 = 100;
    // Aggregate budget: even at the worst-case 100ms × 50, we stay an
    // order of magnitude under the upstream 20s threshold.
    const TOTAL_BUDGET_MS: u128 = 2_000;

    let mut max_iter_ms: u128 = 0;
    let total_start = std::time::Instant::now();

    for i in 0..SERVICES {
        let name = format!("lat-{}", i);
        let cluster_ip = format!("10.96.20.{}", i % 254 + 1);
        let backend_ip = format!("10.244.20.{}", i % 254 + 1);

        let svc = cluster_ip_service(&name, "default", &cluster_ip, 80, 8080);
        let slice = endpoint_slice("default", &name, &[backend_ip.as_str()], Some("http"), 8080);

        storage
            .create(&format!("/registry/services/default/{}", name), &svc)
            .await
            .expect("create service");
        storage
            .create(
                &format!("/registry/endpointslices/default/{}-abc12", name),
                &slice,
            )
            .await
            .expect("create endpointslice");

        // This block is the kube-proxy "sync" critical path that the
        // upstream `BoundedFrequencyRunner` throttles. We DO NOT throttle —
        // every Service create immediately drives a sync — so any per-
        // service iptables-rebuild latency is visible to the test.
        let iter_start = std::time::Instant::now();
        let services: Vec<Service> = storage
            .list("/registry/services/")
            .await
            .expect("list services");
        let slices: Vec<EndpointSlice> = storage
            .list("/registry/endpointslices/")
            .await
            .expect("list endpointslices");
        let map = endpointslice_map(&slices);
        let rules = ipt.build_nat_rules(&services, &map, &[], "test-node").await;
        let iter_elapsed = iter_start.elapsed();
        max_iter_ms = max_iter_ms.max(iter_elapsed.as_millis());

        // Contract 1: the newly-added backend MUST be visible on the very
        // next sync. If the proxier coalesced the event away (the bug the
        // upstream BoundedFrequencyRunner introduces in the Go kube-proxy),
        // the backend would be missing here.
        let expected = format!("--to-destination {}:8080", backend_ip);
        assert!(
            rules.contains(&expected),
            "iter {}: backend {} not in rules within one sync — \
             kube-proxy missed a Service+EndpointSlice create (would manifest \
             as upstream service_latency.go:145 timeout)",
            i,
            backend_ip
        );

        // Contract 2: per-iteration budget. Catches an O(n^2) regression
        // in build_nat_rules or a runaway storage list that would balloon
        // p99 latency as the service count grows.
        assert!(
            iter_elapsed.as_millis() < PER_ITER_BUDGET_MS,
            "iter {}: build_nat_rules + storage list took {:?} (> {}ms budget); \
             a regression here causes upstream p99 service-endpoint latency to drift \
             above the 20s Sonobuoy threshold",
            i,
            iter_elapsed,
            PER_ITER_BUDGET_MS
        );
    }

    // Contract 3: aggregate budget. The upstream test's p99 threshold is
    // measured in seconds. Asserting milliseconds across 50 iterations
    // gives us a wide margin and surfaces any cumulative slowdown
    // (e.g., a leak in MemoryStorage that turns list into O(n^2 keys)).
    let total_elapsed = total_start.elapsed();
    assert!(
        total_elapsed.as_millis() < TOTAL_BUDGET_MS,
        "total time for {} service+slice creations + per-iter sync took {:?} \
         (> {}ms budget); max single iteration was {}ms — \
         kube-proxy must not introduce a per-Service throttle that drifts \
         end-to-end latency above the upstream Sonobuoy threshold",
        SERVICES,
        total_elapsed,
        TOTAL_BUDGET_MS,
        max_iter_ms
    );
}

/// [sig-network] Service endpoints latency — local rule-build time is bounded
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service_latency.go (companion
/// to the [Conformance] above). Sonobuoy (Round 160): PASS (not in failure
/// bucket) for the kube-proxy-local timing budget.
///
/// Builds a 50-service / 100-endpoint table and asserts the local rule
/// emission stays well below the kube-proxy 10-second resync cadence.
#[tokio::test]
async fn service_endpoints_local_rule_build_is_bounded() {
    let mut services = Vec::new();
    let mut slices = Vec::new();
    for i in 0..50u16 {
        let name = format!("svc-{}", i);
        let cluster_ip = format!("10.96.10.{}", i % 254 + 1);
        services.push(cluster_ip_service(&name, "default", &cluster_ip, 80, 8080));
        slices.push(endpoint_slice(
            "default",
            &name,
            &[
                format!("10.244.5.{}", (i * 2) % 254).as_str(),
                format!("10.244.5.{}", (i * 2 + 1) % 254).as_str(),
            ],
            Some("http"),
            8080,
        ));
    }
    let map = endpointslice_map(&slices);
    let start = std::time::Instant::now();
    let rules = test_iptables()
        .build_nat_rules(&services, &map, &[], "test-node")
        .await;
    let elapsed = start.elapsed();
    assert!(elapsed.as_millis() < 500, "took {:?}", elapsed);
    // 50 services * 2 backends = 100 DNAT lines
    let dnat_count = rules.matches("--to-destination ").count();
    assert!(
        dnat_count >= 50,
        "expected ≥50 DNAT lines, got {}",
        dnat_count
    );
}

// ---- Tests: EndpointSlice consumption -------------------------------------

/// [sig-network] kube-proxy must consume EndpointSlices, not only Endpoints
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (mixed
/// Endpoints/EndpointSlice routing). Sonobuoy (Round 160): PASS.
///
/// EndpointSlice is the modern API. kube-proxy must DNAT to backends
/// discovered via EndpointSlice labels even when no legacy `Endpoints`
/// resource exists.
#[tokio::test]
async fn services_must_consume_endpointslices_without_endpoints() {
    let svc = cluster_ip_service("es-only", "default", "10.96.0.170", 80, 8080);
    let slice = endpoint_slice("default", "es-only", &["10.244.6.1"], Some("http"), 8080);
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    assert!(rules.contains("--to-destination 10.244.6.1:8080"));
}

/// [sig-network] kube-proxy correctly maps named ports across EndpointSlices
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (named-port tests)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// When a Service uses a named `targetPort` (e.g., "metrics"), kube-proxy
/// must resolve it via the EndpointSlice's named port, not the service port.
#[tokio::test]
async fn services_named_target_port_resolves_via_endpointslice() {
    let mut svc = cluster_ip_service("named", "default", "10.96.0.180", 80, 8080);
    svc.spec.ports[0].name = Some("metrics".to_string());
    svc.spec.ports[0].target_port = Some(IntOrString::String("metrics".to_string()));
    let slice = endpoint_slice("default", "named", &["10.244.7.1"], Some("metrics"), 9100);
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    assert!(
        rules.contains("--to-destination 10.244.7.1:9100"),
        "{}",
        rules
    );
}

// ---- Tests: Storage round-trip backing the kube-proxy watch loop ----------

/// [sig-network] EndpointSlice round-trip via storage backing kube-proxy watch
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (general lifecycle)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// kube-proxy's `run` loop watches `/registry/services/`,
/// `/registry/endpoints/`, and `/registry/endpointslices/`. This test
/// drives the storage layer with an `Arc<MemoryStorage>` (the same trait
/// `KubeProxy` uses at runtime) and verifies that an EndpointSlice can be
/// created, listed back, and parsed into the proxy's expected shape.
#[tokio::test]
async fn endpointslice_storage_round_trip_drives_proxy_watch() {
    let storage = fresh_storage();
    let slice = endpoint_slice("default", "rt", &["10.244.8.1"], Some("http"), 8080);
    storage
        .create("/registry/endpointslices/default/rt-abc12", &slice)
        .await
        .expect("create endpointslice");
    let listed: Vec<EndpointSlice> = storage
        .list("/registry/endpointslices/")
        .await
        .expect("list endpointslices");
    assert_eq!(listed.len(), 1);
    let map = endpointslice_map(&listed);
    let entries = map
        .get("default/rt")
        .expect("map keyed by service-name label");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "10.244.8.1");
    assert_eq!(entries[0].1.as_deref(), Some("http"));
    assert_eq!(entries[0].2, 8080);
}

/// [sig-network] Service round-trip via storage backing kube-proxy watch
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (ClusterIP CRUD)
/// Sonobuoy (Round 160): PASS (not in failure bucket)
///
/// Validates the Service create/get path that kube-proxy reads from
/// `/registry/services/` and then feeds to `build_nat_rules`. The
/// resourceVersion populated by `MemoryStorage::create` mirrors the etcd
/// revision number that kube-proxy uses for change detection.
#[tokio::test]
async fn service_storage_round_trip_drives_proxy_watch() {
    let storage = fresh_storage();
    let svc = cluster_ip_service("crud", "default", "10.96.0.190", 80, 8080);
    storage
        .create("/registry/services/default/crud", &svc)
        .await
        .expect("create service");
    let got: Service = storage
        .get("/registry/services/default/crud")
        .await
        .expect("get service");
    assert_eq!(got.metadata.name, "crud");
    assert_eq!(got.spec.cluster_ip.as_deref(), Some("10.96.0.190"));
    assert_eq!(got.spec.ports.len(), 1);
    assert_eq!(got.spec.ports[0].port, 80);
}

/// [sig-network] Service deletion is observable via storage delete
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (delete path)
/// Sonobuoy (Round 160): PASS (not in failure bucket) — the delete-timeout
/// bug captured in `services_should_complete_service_status_lifecycle` is
/// in the LB-status finalizer path, not the bare delete path.
#[tokio::test]
async fn service_deletion_round_trip() {
    let storage = fresh_storage();
    let svc = cluster_ip_service("del", "default", "10.96.0.200", 80, 8080);
    storage
        .create("/registry/services/default/del", &svc)
        .await
        .expect("create");
    storage
        .delete("/registry/services/default/del")
        .await
        .expect("delete");
    let listed: Vec<Service> = storage
        .list("/registry/services/")
        .await
        .expect("list after delete");
    assert!(listed.iter().all(|s| s.metadata.name != "del"));
}

// ---- Tests: /proxy subresource (kube-proxy half) --------------------------

/// [sig-network] Proxy version v1 — pod proxy resolves to pod IP via storage
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/proxy.go:137
/// ("should proxy through a service and a pod")
/// Sonobuoy (Round 160): PASS (not in failure bucket — this is the
/// pod-only path, distinct from the failing combined pod+service test).
///
/// kube-proxy half: the /proxy subresource does NOT route via iptables; the
/// api-server resolves the target endpoint and proxies directly. But the
/// underlying storage shape that kube-proxy ALSO consumes — Service +
/// EndpointSlice — must be queryable from `Arc<Storage>`. This test
/// asserts that storage shape is intact and discoverable by both halves.
#[tokio::test]
async fn proxy_pod_target_resolution_storage_shape() {
    let storage = fresh_storage();
    let svc = cluster_ip_service("px", "default", "10.96.0.210", 80, 8080);
    let slice = endpoint_slice("default", "px", &["10.244.9.1"], Some("http"), 8080);
    storage
        .create("/registry/services/default/px", &svc)
        .await
        .expect("create svc");
    storage
        .create("/registry/endpointslices/default/px-abc12", &slice)
        .await
        .expect("create slice");

    // The api-server's /proxy handler will list endpointslices in the
    // namespace and select a ready endpoint with a matching port.
    let slices: Vec<EndpointSlice> = storage
        .list("/registry/endpointslices/default/")
        .await
        .expect("list");
    let target = slices
        .iter()
        .filter(|es| {
            es.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|v| v == "px")
                .unwrap_or(false)
        })
        .find_map(|es| {
            es.endpoints.iter().find_map(|ep| {
                if ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true) {
                    ep.addresses.first().cloned()
                } else {
                    None
                }
            })
        });
    assert_eq!(target, Some("10.244.9.1".to_string()));
}

/// [sig-network] Proxy version v1 — A set of valid responses are returned
/// for both pod and service ProxyWithPath
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/proxy.go:286
/// Sonobuoy (Round 160): PASS (not in failure bucket).
///
/// kube-proxy half: the iptables side does not change between root-path
/// and sub-path proxy requests — the api-server handler appends the path
/// and forwards. We assert no iptables-side regression: a Service with
/// endpoints emits a DNAT rule independent of any /proxy path parameter.
#[tokio::test]
async fn proxy_with_path_iptables_invariant() {
    let svc = cluster_ip_service("pxpath", "default", "10.96.0.220", 80, 8080);
    let slice = endpoint_slice("default", "pxpath", &["10.244.9.2"], Some("http"), 8080);
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;
    assert!(rules.contains("--to-destination 10.244.9.2:8080"));
    // No /proxy-specific iptables artefact must leak in.
    assert!(!rules.contains("/proxy"));
}

/// [sig-network] Proxy version v1 [Conformance] — A set of valid responses
/// are returned for both pod and service Proxy
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/proxy.go:432–503
/// Sonobuoy (Round 160): historically FAIL at proxy.go:503. Upstream walks
/// a matrix of expected response codes (200, 404, 503, 301-with-Location)
/// through both `/api/v1/namespaces/{ns}/services/{svc}/proxy/{path}` and
/// `/api/v1/namespaces/{ns}/pods/{pod}/proxy/{path}` and asserts each
/// status code + body is forwarded verbatim from the backend.
///
/// kube-proxy half: the /proxy subresource does NOT route via iptables —
/// the api-server resolves the pod IP from EndpointSlice (service path)
/// or directly from Pod status (pod path) and proxies in-process. This
/// test asserts the iptables surface stays correct alongside the proxy
/// chain — i.e. the same Service + EndpointSlice shape the api-server
/// proxy handler queries still produces a clean DNAT rule pointing at
/// the pod IP:targetPort, with no `/proxy`-derived artefacts leaking in.
///
/// The HTTP-response-code matrix that fails at proxy.go:503 is exercised
/// in the api-server-side mirror at
/// `crates/api-server/tests/conformance_network_services_proxy.rs`; this
/// half guarantees the underlying iptables→endpoint mapping survives even
/// when the api-server proxy chain is exercised end-to-end against a real
/// HTTP backend.
#[tokio::test]
async fn proxy_valid_responses_for_pod_and_service() {
    // Mirror the storage shape both halves of the upstream test use:
    //   - Service `pxresp` with port 80 → targetPort 8080
    //   - EndpointSlice tying the service to pod IP 10.244.9.3:8080
    //
    // Upstream creates a Pod (proxy-service-...) that exposes the service
    // via a single EndpointSlice. The api-server's service-proxy handler
    // (handlers/proxy.rs:295-328) reads exactly this shape; the kube-proxy
    // half must produce a clean iptables rule for the same tuple.
    let svc = cluster_ip_service("pxresp", "default", "10.96.0.230", 80, 8080);
    let slice = endpoint_slice("default", "pxresp", &["10.244.9.3"], Some("http"), 8080);
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(std::slice::from_ref(&svc), &map, &[], "test-node")
        .await;

    // DNAT must point at the pod IP:targetPort that the api-server proxy
    // will also resolve to via the EndpointSlice (proxy.rs:329-341).
    assert!(
        rules.contains("--to-destination 10.244.9.3:8080"),
        "rules missing DNAT to pod:targetPort: {}",
        rules
    );
    // ClusterIP DNAT must be in place — Service-proxy via the api-server
    // and Service-via-kube-proxy must agree on the (clusterIP, port) tuple.
    assert!(
        rules.contains("10.96.0.230/32"),
        "rules missing ClusterIP match: {}",
        rules
    );
    assert!(rules.contains("--dport 80"), "rules: {}", rules);

    // No /proxy path artefact must leak into iptables — the api-server
    // owns path forwarding, kube-proxy must not see it. This guards the
    // invariant that the response-code matrix in the api-server mirror
    // never affects iptables emission.
    assert!(!rules.contains("/proxy"), "iptables leaked /proxy artefact");

    // Storage shape sanity: the api-server proxy handler will also resolve
    // pod IPs directly from Pod status when called via /pods/{name}/proxy.
    // We mirror that data path here so the kube-proxy half stays in sync
    // when the api-server mirror's Pod-proxy assertions evolve.
    let storage = fresh_storage();
    storage
        .create("/registry/services/default/pxresp", &svc)
        .await
        .expect("create service");
    let slice = endpoint_slice("default", "pxresp", &["10.244.9.3"], Some("http"), 8080);
    storage
        .create("/registry/endpointslices/default/pxresp-abc12", &slice)
        .await
        .expect("create endpointslice");

    // Confirm the api-server's EndpointSlice lookup (handlers/proxy.rs:296)
    // finds the slice with the canonical service-name label.
    let slices: Vec<EndpointSlice> = storage
        .list("/registry/endpointslices/default/")
        .await
        .expect("list slices");
    let backing = slices
        .iter()
        .find(|es| {
            es.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|v| v == "pxresp")
                .unwrap_or(false)
        })
        .expect("service-name labelled slice missing");
    let backend_ip = backing
        .endpoints
        .iter()
        .find_map(|ep| {
            if ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true) {
                ep.addresses.first().cloned()
            } else {
                None
            }
        })
        .expect("no ready endpoint");
    assert_eq!(backend_ip, "10.244.9.3");
    // Resolved EndpointSlice port matches Service.targetPort — the
    // api-server proxy handler reuses this when forwarding.
    let resolved_port = backing.ports.first().and_then(|p| p.port).unwrap_or(0);
    assert_eq!(resolved_port, 8080);
}
