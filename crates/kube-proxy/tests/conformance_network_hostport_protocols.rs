//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-network] HostPort and multi-protocol service routing.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/network/
//! (`hostport.go`, `service.go`)
//!
//! Strategy: kube-proxy owns iptables-rule emission for hostPort DNAT rules
//! (`build_hostport_rules`) and multi-protocol service DNAT rules
//! (`build_nat_rules`). These tests call both builder functions and assert the
//! resulting `iptables-restore` strings contain the expected rules.
//!
//! Connectivity specs (packets arriving at nodeIP:hostPort reaching podIP)
//! are verified at the rule-generation level: the connectivity the upstream
//! e2e dial exercises is a property of the emitted DNAT rule, so we assert the
//! rule is present, scoped (hostIP), and routes to the correct per-protocol
//! target. The privileged live-socket dial itself belongs to the cluster e2e
//! suite, not this unit test.

use rusternetes_common::resources::endpointslice::EndpointPort as ESEndpointPort;
use rusternetes_common::resources::{
    Container, ContainerPort, Endpoint, EndpointConditions, EndpointSlice, IntOrString, Pod,
    PodSpec, PodStatus, Service, ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kube_proxy::iptables::{
    IptablesManager, DEFAULT_CLUSTER_CIDR, DEFAULT_NODEPORT_RANGE,
};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_iptables() -> IptablesManager {
    IptablesManager::for_testing(true)
}

fn pod_with_hostport(
    name: &str,
    namespace: &str,
    node_name: &str,
    pod_ip: &str,
    container_port: u16,
    host_port: u16,
    protocol: &str,
) -> Pod {
    pod_with_hostport_ip(
        name,
        namespace,
        node_name,
        pod_ip,
        container_port,
        host_port,
        protocol,
        None,
    )
}

/// Like [`pod_with_hostport`] but additionally pins the hostPort to a specific
/// `hostIP`. Passing `Some("10.0.0.5")` exercises the `-d <hostIP>/32` DNAT
/// destination match; `None` (or `0.0.0.0`) emits a wildcard rule.
#[allow(clippy::too_many_arguments)]
fn pod_with_hostport_ip(
    name: &str,
    namespace: &str,
    node_name: &str,
    pod_ip: &str,
    container_port: u16,
    host_port: u16,
    protocol: &str,
    host_ip: Option<&str>,
) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: format!("uid-{}", name),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            node_name: Some(node_name.to_string()),
            containers: vec![Container {
                name: "c".to_string(),
                image: "registry.k8s.io/e2e-test-images/agnhost:2.55".to_string(),
                ports: Some(vec![ContainerPort {
                    container_port,
                    name: Some("http".to_string()),
                    protocol: protocol.to_string(),
                    host_port: Some(host_port),
                    host_ip: host_ip.map(String::from),
                }]),
                ..Default::default()
            }],
            ..PodSpec::default()
        }),
        status: Some(PodStatus {
            pod_ip: Some(pod_ip.to_string()),
            ..PodStatus::default()
        }),
    }
}

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

fn endpoint_slice_with_proto(
    namespace: &str,
    service_name: &str,
    addresses: &[&str],
    port_name: Option<&str>,
    port_num: i32,
    protocol: &str,
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
        protocol: protocol.to_string(),
        app_protocol: None,
    }];
    es
}

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

        // Mirror the canonical helper in conformance_network_services_proxy.rs:
        // a slice with no ports still contributes its ready addresses with a
        // null port name and port 0.
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

// ---------------------------------------------------------------------------
// HostPort conflict rules
// Upstream: test/e2e/network/hostport.go
// ---------------------------------------------------------------------------

/// [sig-network] HostPort validates that there is no conflict between pods
/// with same hostPort but different hostIP and protocol [LinuxOnly]
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go:219
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// kube-proxy must emit independent DNAT rules for pod1 (TCP, hostPort A)
/// and pod2 (TCP, hostPort B). Two pods with different host-ports must
/// coexist — neither rule should be absent.
#[test]
fn hostport_no_conflict_different_hostport_values() {
    let mgr = IptablesManager::new(
        DEFAULT_CLUSTER_CIDR.to_string(),
        DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![
        pod_with_hostport(
            "pod-a",
            "default",
            "node-1",
            "10.244.0.10",
            8080,
            54321,
            "TCP",
        ),
        pod_with_hostport(
            "pod-b",
            "default",
            "node-1",
            "10.244.0.11",
            8080,
            54322,
            "TCP",
        ),
    ];

    let rules = mgr.build_hostport_rules(&pods, "node-1");

    // Both DNAT targets must appear — no clobbering.
    assert!(
        rules.contains("--dport 54321"),
        "pod-a hostPort 54321 must be present: {}",
        rules
    );
    assert!(
        rules.contains("--dport 54322"),
        "pod-b hostPort 54322 must be present: {}",
        rules
    );
    assert!(
        rules.contains("-j DNAT --to-destination 10.244.0.10:8080"),
        "pod-a DNAT target: {}",
        rules
    );
    assert!(
        rules.contains("-j DNAT --to-destination 10.244.0.11:8080"),
        "pod-b DNAT target: {}",
        rules
    );
}

/// [sig-network] HostPort — same hostPort different protocols coexist
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go (TCP vs UDP
/// variant). Sonobuoy (Round 160): FAIL (failing.txt — sub-case).
///
/// Pod A uses TCP on hostPort 9999; pod B uses UDP on hostPort 9999. Both
/// must produce distinct DNAT rules since the protocol forms part of the
/// match tuple (`-p tcp` vs `-p udp`). Neither rule should be absent.
#[test]
fn hostport_same_port_different_protocol_both_rules_present() {
    let mgr = IptablesManager::new(
        DEFAULT_CLUSTER_CIDR.to_string(),
        DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![
        pod_with_hostport(
            "tcp-pod",
            "default",
            "node-1",
            "10.244.0.20",
            7070,
            9999,
            "TCP",
        ),
        pod_with_hostport(
            "udp-pod",
            "default",
            "node-1",
            "10.244.0.21",
            7070,
            9999,
            "UDP",
        ),
    ];

    let rules = mgr.build_hostport_rules(&pods, "node-1");

    // The TCP rule must reference port 9999 with tcp protocol.
    assert!(
        rules.contains("-p tcp") && rules.contains("--dport 9999"),
        "TCP hostPort 9999 rule must be present: {}",
        rules
    );
    // The UDP rule must also reference port 9999 with udp protocol.
    assert!(
        rules.contains("-p udp") && rules.contains("--dport 9999"),
        "UDP hostPort 9999 rule must be present: {}",
        rules
    );
    // Both DNAT destinations must be in the output.
    assert!(
        rules.contains("10.244.0.20:7070"),
        "TCP pod DNAT target must be present: {}",
        rules
    );
    assert!(
        rules.contains("10.244.0.21:7070"),
        "UDP pod DNAT target must be present: {}",
        rules
    );
}

/// [sig-network] HostPort — KUBE-HOSTPORTS chain must be declared for all
/// node pods with hostPorts
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go (chain structure)
/// Sonobuoy (Round 160): FAIL (failing.txt — sub-case)
///
/// The iptables-restore blob must begin with `*nat` and contain the
/// `:KUBE-HOSTPORTS - [0:0]` chain declaration so the kernel creates/clears
/// the chain even if the previous pod list was empty.
#[test]
fn hostport_chain_header_always_emitted() {
    let mgr = IptablesManager::new(
        DEFAULT_CLUSTER_CIDR.to_string(),
        DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![pod_with_hostport(
        "single",
        "default",
        "node-1",
        "10.244.0.30",
        8080,
        31000,
        "TCP",
    )];

    let rules = mgr.build_hostport_rules(&pods, "node-1");
    assert!(
        rules.starts_with("*nat\n"),
        "iptables-restore blob must start with *nat: {}",
        rules
    );
    assert!(
        rules.contains(":KUBE-HOSTPORTS - [0:0]"),
        "KUBE-HOSTPORTS chain header must be present: {}",
        rules
    );
    assert!(
        rules.ends_with("COMMIT\n"),
        "iptables-restore blob must end with COMMIT: {}",
        rules
    );
}

/// [sig-network] HostPort — pods on remote nodes must be excluded
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go (per-node
/// isolation). Sonobuoy (Round 160): FAIL (failing.txt — sub-case).
///
/// kube-proxy is per-node: it must install DNAT rules only for pods
/// scheduled to its own node. A pod on a different node is handled by THAT
/// node's kube-proxy and must not appear in local rules.
#[test]
fn hostport_remote_node_pods_excluded() {
    let mgr = IptablesManager::new(
        DEFAULT_CLUSTER_CIDR.to_string(),
        DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![
        pod_with_hostport(
            "local",
            "default",
            "node-1",
            "10.244.0.40",
            8080,
            31001,
            "TCP",
        ),
        pod_with_hostport(
            "remote",
            "default",
            "node-2",
            "10.244.1.40",
            8080,
            31002,
            "TCP",
        ),
    ];

    let rules = mgr.build_hostport_rules(&pods, "node-1");

    assert!(
        rules.contains("--dport 31001"),
        "local pod's hostPort must be programmed: {}",
        rules
    );
    assert!(
        !rules.contains("--dport 31002"),
        "remote pod's hostPort must NOT be programmed: {}",
        rules
    );
    assert!(
        !rules.contains("10.244.1.40"),
        "remote pod IP must not appear in local DNAT rules: {}",
        rules
    );
}

/// [sig-network] HostPort — DNAT rule wires nodeIP:hostPort to podIP:port
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go:219
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// In-process equivalent of the upstream connectivity dial: the upstream e2e
/// test verifies a packet arriving at nodeIP:hostPort reaches the backend
/// pod. The mechanism that makes that work is a single KUBE-HOSTPORTS DNAT
/// rule that rewrites the destination to podIP:containerPort. This asserts
/// that exact rule is present and complete (proto, dport, DNAT target) — the
/// connectivity is a property of the rule, which is what we can verify in a
/// unit test. The privileged live-socket dial belongs to the e2e suite.
#[test]
fn hostport_dnat_rule_maps_hostport_to_pod() {
    let mgr = IptablesManager::new(
        DEFAULT_CLUSTER_CIDR.to_string(),
        DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![pod_with_hostport(
        "web",
        "default",
        "node-1",
        "10.244.0.50",
        8080,
        31000,
        "TCP",
    )];

    let rules = mgr.build_hostport_rules(&pods, "node-1");

    // The DNAT rule must live in the KUBE-HOSTPORTS chain and carry every
    // match component the connectivity dial depends on, in one rule.
    let dnat_line = rules
        .lines()
        .find(|l| l.contains("--dport 31000"))
        .unwrap_or_else(|| panic!("hostPort 31000 DNAT rule missing: {}", rules));
    assert!(
        dnat_line.contains("-A KUBE-HOSTPORTS"),
        "DNAT rule must append to KUBE-HOSTPORTS: {}",
        dnat_line
    );
    assert!(
        dnat_line.contains("-p tcp"),
        "DNAT rule must match tcp protocol: {}",
        dnat_line
    );
    assert!(
        dnat_line.contains("-j DNAT --to-destination 10.244.0.50:8080"),
        "DNAT rule must rewrite to podIP:containerPort: {}",
        dnat_line
    );
    // A wildcard hostPort (no hostIP) must NOT carry a `-d` destination match —
    // it has to accept traffic to any local address (nodeIP, 127.0.0.1, ...).
    assert!(
        !dnat_line.contains(" -d "),
        "wildcard hostPort rule must not pin a destination IP: {}",
        dnat_line
    );
}

/// [sig-network] HostPort — hostIP-scoped DNAT binds the port to one address
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go:219 ("different
/// hostIP" sub-case). Sonobuoy (Round 160): FAIL (failing.txt).
///
/// When a containerPort sets `hostIP`, the hostPort must only be reachable on
/// THAT host address, so kube-proxy must scope the DNAT rule with a
/// `-d <hostIP>/32` destination match. Two pods sharing the same hostPort
/// number but bound to different hostIPs must each get their own scoped rule
/// and must not collide.
#[test]
fn hostport_hostip_scoped_dnat_rule() {
    let mgr = IptablesManager::new(
        DEFAULT_CLUSTER_CIDR.to_string(),
        DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![
        pod_with_hostport_ip(
            "bound-a",
            "default",
            "node-1",
            "10.244.0.60",
            8080,
            8888,
            "TCP",
            Some("10.0.0.5"),
        ),
        pod_with_hostport_ip(
            "bound-b",
            "default",
            "node-1",
            "10.244.0.61",
            8080,
            8888,
            "TCP",
            Some("10.0.0.6"),
        ),
    ];

    let rules = mgr.build_hostport_rules(&pods, "node-1");

    // pod bound-a: rule scoped to 10.0.0.5, DNAT to 10.244.0.60:8080.
    let line_a = rules
        .lines()
        .find(|l| l.contains("10.244.0.60:8080"))
        .unwrap_or_else(|| panic!("bound-a DNAT rule missing: {}", rules));
    assert!(
        line_a.contains("-d 10.0.0.5/32"),
        "bound-a rule must be scoped to hostIP 10.0.0.5: {}",
        line_a
    );
    assert!(
        line_a.contains("--dport 8888"),
        "bound-a rule must match hostPort 8888: {}",
        line_a
    );

    // pod bound-b: scoped to a DIFFERENT hostIP, same hostPort number — no
    // conflict, because the destination match disambiguates them.
    let line_b = rules
        .lines()
        .find(|l| l.contains("10.244.0.61:8080"))
        .unwrap_or_else(|| panic!("bound-b DNAT rule missing: {}", rules));
    assert!(
        line_b.contains("-d 10.0.0.6/32"),
        "bound-b rule must be scoped to hostIP 10.0.0.6: {}",
        line_b
    );
    assert!(
        line_b.contains("--dport 8888"),
        "bound-b rule must match hostPort 8888: {}",
        line_b
    );
}

/// [sig-network] HostPort — hostIP 0.0.0.0 is treated as wildcard
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go (hostIP semantics).
///
/// A `hostIP` of `0.0.0.0` is the unspecified address and means "all
/// interfaces" — identical to omitting hostIP. kube-proxy must therefore emit
/// a wildcard rule with no `-d` destination match, never `-d 0.0.0.0/32`
/// (which would match nothing).
#[test]
fn hostport_hostip_unspecified_is_wildcard() {
    let mgr = IptablesManager::new(
        DEFAULT_CLUSTER_CIDR.to_string(),
        DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![pod_with_hostport_ip(
        "wild",
        "default",
        "node-1",
        "10.244.0.70",
        8080,
        9090,
        "TCP",
        Some("0.0.0.0"),
    )];

    let rules = mgr.build_hostport_rules(&pods, "node-1");
    let line = rules
        .lines()
        .find(|l| l.contains("--dport 9090"))
        .unwrap_or_else(|| panic!("hostPort 9090 rule missing: {}", rules));
    assert!(
        !line.contains(" -d "),
        "hostIP 0.0.0.0 must produce a wildcard rule, not a -d match: {}",
        line
    );
    assert!(
        !rules.contains("0.0.0.0/32"),
        "must never emit -d 0.0.0.0/32: {}",
        rules
    );
}

// ---------------------------------------------------------------------------
// Services — same port different protocols (kube-proxy NAT rules)
// Upstream: test/e2e/network/service.go:2398
// ---------------------------------------------------------------------------

/// [sig-network] Services should serve endpoints on same port and different
/// protocols [Conformance] — kube-proxy half
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:2398
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// kube-proxy must emit DNAT rules for both a TCP and UDP service that
/// share the same port number. The rules are independent entries in the
/// iptables-restore blob — they must not clobber each other.
#[tokio::test]
async fn services_endpoints_same_port_different_protocols_iptables() {
    // TCP service on port 80, ClusterIP 10.97.0.1
    let svc_tcp = cluster_ip_service("dual-tcp", "default", "10.97.0.1", 80, 8080);
    // UDP service on port 80, ClusterIP 10.97.0.2
    let mut svc_udp = cluster_ip_service("dual-udp", "default", "10.97.0.2", 80, 8080);
    svc_udp.spec.ports[0].protocol = "UDP".to_string();

    let tcp_slice = endpoint_slice_with_proto(
        "default",
        "dual-tcp",
        &["10.244.60.1"],
        Some("http"),
        8080,
        "TCP",
    );
    let udp_slice = endpoint_slice_with_proto(
        "default",
        "dual-udp",
        &["10.244.60.2"],
        Some("http"),
        8080,
        "UDP",
    );

    let map = endpointslice_map(&[tcp_slice, udp_slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc_tcp, svc_udp], &map, &[], "test-node")
        .await;

    // TCP service ClusterIP DNAT rule must be present.
    assert!(
        rules.contains("-A RUSTERNETES-SERVICES -d 10.97.0.1/32"),
        "TCP ClusterIP rule missing: {}",
        rules
    );
    // UDP service ClusterIP DNAT rule must be present.
    assert!(
        rules.contains("-A RUSTERNETES-SERVICES -d 10.97.0.2/32"),
        "UDP ClusterIP rule missing: {}",
        rules
    );
    // Both DNAT targets.
    assert!(
        rules.contains("--to-destination 10.244.60.1:8080"),
        "TCP DNAT target missing: {}",
        rules
    );
    assert!(
        rules.contains("--to-destination 10.244.60.2:8080"),
        "UDP DNAT target missing: {}",
        rules
    );
}

/// [sig-network] Services should serve endpoints on same port and different
/// protocols [Conformance] — per-protocol endpoint selection
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:2398
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// In-process equivalent of the upstream dual-socket dial. The upstream test
/// uses a SINGLE service exposing the same port number on both TCP and UDP,
/// each protocol backed by its own named endpoint, then dials both sockets
/// and asserts independent routing. The mechanism is two DNAT rules sharing
/// the ClusterIP+dport but differing in `-p tcp` / `-p udp`, each selecting
/// its OWN protocol's endpoint. This asserts that per-protocol target
/// selection — strictly stronger than the sibling
/// `services_endpoints_same_port_different_protocols_iptables`, which only
/// checks two *separate* services' rules coexist. The privileged live dial
/// belongs to the e2e suite.
#[tokio::test]
async fn services_same_port_dual_protocol_routes_to_correct_target() {
    // One service, ClusterIP 10.97.5.1, exposing port 80 on BOTH protocols.
    // Each protocol uses a distinct named port so it resolves to its own
    // backend endpoint (TCP -> :8080, UDP -> :9090).
    let mut svc = cluster_ip_service("dual", "default", "10.97.5.1", 80, 8080);
    svc.spec.ports = vec![
        ServicePort {
            name: Some("tcp-p".to_string()),
            port: 80,
            target_port: Some(IntOrString::Int(8080)),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        },
        ServicePort {
            name: Some("udp-p".to_string()),
            port: 80,
            target_port: Some(IntOrString::Int(9090)),
            protocol: "UDP".to_string(),
            node_port: None,
            app_protocol: None,
        },
    ];

    // Two slices for the SAME service, one per protocol/named-port.
    let tcp_slice = endpoint_slice_with_proto(
        "default",
        "dual",
        &["10.244.70.1"],
        Some("tcp-p"),
        8080,
        "TCP",
    );
    let udp_slice = endpoint_slice_with_proto(
        "default",
        "dual",
        &["10.244.70.2"],
        Some("udp-p"),
        9090,
        "UDP",
    );

    // Both slices map under key `default/dual` (keyed by the service-name
    // label), so their endpoints merge — the per-port-name filter in
    // build_nat_rules is what splits them back apart per protocol.
    let map = endpointslice_map(&[tcp_slice, udp_slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;

    // Isolate the TCP and UDP ClusterIP rules for the shared dport 80.
    let tcp_rule = rules
        .lines()
        .find(|l| l.contains("-d 10.97.5.1/32") && l.contains("-p tcp") && l.contains("--dport 80"))
        .unwrap_or_else(|| panic!("TCP ClusterIP rule for dport 80 missing: {}", rules));
    let udp_rule = rules
        .lines()
        .find(|l| l.contains("-d 10.97.5.1/32") && l.contains("-p udp") && l.contains("--dport 80"))
        .unwrap_or_else(|| panic!("UDP ClusterIP rule for dport 80 missing: {}", rules));

    // Each protocol must route to its OWN endpoint — not the other's.
    assert!(
        tcp_rule.contains("--to-destination 10.244.70.1:8080"),
        "TCP rule must DNAT to the TCP backend 10.244.70.1:8080: {}",
        tcp_rule
    );
    assert!(
        !tcp_rule.contains("10.244.70.2"),
        "TCP rule must NOT route to the UDP backend: {}",
        tcp_rule
    );
    assert!(
        udp_rule.contains("--to-destination 10.244.70.2:9090"),
        "UDP rule must DNAT to the UDP backend 10.244.70.2:9090: {}",
        udp_rule
    );
    assert!(
        !udp_rule.contains("10.244.70.1"),
        "UDP rule must NOT route to the TCP backend: {}",
        udp_rule
    );
}

// ---------------------------------------------------------------------------
// Services NodePort — functioning service iptables assertion
// Upstream: test/e2e/network/service.go
// ---------------------------------------------------------------------------

/// [sig-network] Services should be able to create a functioning NodePort
/// service [Conformance] — iptables-rule assertion
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1687
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// kube-proxy must program both the SERVICES (ClusterIP) and NODEPORTS
/// chains for a NodePort service. This is the iptables half — no live dial
/// required.
#[tokio::test]
async fn services_functioning_nodeport_iptables_rules_present() {
    let mut sel = HashMap::new();
    sel.insert("app".to_string(), "echo".to_string());
    let svc = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("fnp-svc").with_namespace("default"),
        spec: ServiceSpec {
            selector: Some(sel),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                protocol: "TCP".to_string(),
                node_port: Some(31234),
                app_protocol: None,
            }],
            service_type: Some(ServiceType::NodePort),
            cluster_ip: Some("10.97.1.1".to_string()),
            ..ServiceSpec::default()
        },
        status: None,
    };
    let slice = endpoint_slice_with_proto(
        "default",
        "fnp-svc",
        &["10.244.61.1"],
        Some("http"),
        8080,
        "TCP",
    );
    let map = endpointslice_map(&[slice]);
    let rules = test_iptables()
        .build_nat_rules(&[svc], &map, &[], "test-node")
        .await;

    assert!(
        rules.contains("-A RUSTERNETES-SERVICES -d 10.97.1.1/32"),
        "NodePort SERVICES chain rule: {}",
        rules
    );
    assert!(
        rules.contains("-A RUSTERNETES-NODEPORTS -p tcp --dport 31234"),
        "NodePort NODEPORTS chain rule: {}",
        rules
    );
    assert!(
        rules.contains("--to-destination 10.244.61.1:8080"),
        "NodePort DNAT target: {}",
        rules
    );
}
