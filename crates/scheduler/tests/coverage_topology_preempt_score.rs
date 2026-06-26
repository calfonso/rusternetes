//! Unit-test coverage for pure scheduler helpers: topology-spread constraints,
//! host-port conflict detection, preemption victim selection (plain + PDB-aware),
//! and resource scoring.
//!
//! Upstream references (Kubernetes v1.35):
//! - Topology spread:
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/scheduler/framework/plugins/podtopologyspread/filtering_test.go>
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/scheduler/framework/plugins/podtopologyspread/scoring_test.go>
//! - Node resources (scoring):
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/scheduler/framework/plugins/noderesources/fit_test.go>
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/scheduler/framework/plugins/noderesources/balanced_allocation_test.go>
//! - Host ports:
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/scheduler/framework/plugins/nodeports/node_ports_test.go>
//! - Preemption:
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/scheduler/framework/preemption/preemption_test.go>
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/scheduler/framework/plugins/defaultpreemption/default_preemption_test.go>

use std::collections::HashMap;

use rusternetes_common::resources::{
    Container, ContainerPort, IntOrString, Node, NodeStatus, Pod, PodDisruptionBudget,
    PodDisruptionBudgetSpec, PodSpec, PodStatus, TopologySpreadConstraint,
};
use rusternetes_common::types::{LabelSelector, Phase, ResourceRequirements};
use rusternetes_scheduler::advanced::{
    calculate_resource_score, calculate_resource_score_with_pods, check_host_port_conflicts,
    check_preemption, check_preemption_with_pdbs, check_topology_spread_constraints,
};

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

fn make_node_with_labels(
    name: &str,
    cpu: &str,
    memory: &str,
    labels: HashMap<String, String>,
) -> Node {
    let mut allocatable = HashMap::new();
    allocatable.insert("cpu".to_string(), cpu.to_string());
    allocatable.insert("memory".to_string(), memory.to_string());
    let mut node = Node::new(name);
    node.status = Some(NodeStatus {
        capacity: Some(allocatable.clone()),
        allocatable: Some(allocatable),
        conditions: None,
        addresses: None,
        node_info: None,
        images: None,
        volumes_in_use: None,
        volumes_attached: None,
        daemon_endpoints: None,
        config: None,
        features: None,
        runtime_handlers: None,
        declared_features: None,
    });
    node.metadata.labels = Some(labels);
    node
}

fn make_node(name: &str, cpu: &str, memory: &str) -> Node {
    make_node_with_labels(name, cpu, memory, HashMap::new())
}

fn make_container_req(cpu: &str, memory: &str) -> Container {
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), cpu.to_string());
    requests.insert("memory".to_string(), memory.to_string());
    Container {
        name: "main".to_string(),
        image: "registry.k8s.io/pause:3.10".to_string(),
        command: None,
        args: None,
        working_dir: None,
        ports: None,
        env: None,
        env_from: None,
        resources: Some(ResourceRequirements {
            requests: Some(requests),
            limits: None,
            claims: None,
        }),
        volume_mounts: None,
        volume_devices: None,
        image_pull_policy: None,
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
        security_context: None,
        restart_policy: None,
        resize_policy: None,
        lifecycle: None,
        termination_message_path: None,
        termination_message_policy: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        ..Default::default()
    }
}

fn make_container_with_port(
    name: &str,
    host_port: u16,
    protocol: &str,
    host_ip: Option<&str>,
) -> Container {
    Container {
        name: name.to_string(),
        image: "registry.k8s.io/pause:3.10".to_string(),
        command: None,
        args: None,
        working_dir: None,
        ports: Some(vec![ContainerPort {
            container_port: host_port,
            name: None,
            protocol: protocol.to_string(),
            host_port: Some(host_port),
            host_ip: host_ip.map(|s| s.to_string()),
        }]),
        env: None,
        env_from: None,
        resources: None,
        volume_mounts: None,
        volume_devices: None,
        image_pull_policy: None,
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
        security_context: None,
        restart_policy: None,
        resize_policy: None,
        lifecycle: None,
        termination_message_path: None,
        termination_message_policy: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        ..Default::default()
    }
}

fn scheduled_pod(name: &str, priority: i32, cpu: &str, mem: &str, node_name: &str) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container_req(cpu, mem)],
        priority: Some(priority),
        node_name: Some(node_name.to_string()),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    pod
}

fn incoming_pod(name: &str, priority: i32, cpu: &str, mem: &str) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container_req(cpu, mem)],
        priority: Some(priority),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod
}

fn pod_on_node_with_labels(
    name: &str,
    node_name: &str,
    pod_labels: HashMap<String, String>,
) -> Pod {
    let spec = PodSpec {
        node_name: Some(node_name.to_string()),
        containers: vec![make_container_req("100m", "64Mi")],
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.labels = Some(pod_labels);
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    pod
}

fn make_label_selector(key: &str, value: &str) -> LabelSelector {
    let mut m = HashMap::new();
    m.insert(key.to_string(), value.to_string());
    LabelSelector {
        match_labels: Some(m),
        match_expressions: None,
    }
}

fn make_pdb(
    name: &str,
    namespace: &str,
    selector_key: &str,
    selector_val: &str,
    min_available: i32,
) -> PodDisruptionBudget {
    PodDisruptionBudget::new(
        name,
        namespace,
        PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            max_unavailable: None,
            selector: make_label_selector(selector_key, selector_val),
            unhealthy_pod_eviction_policy: None,
        },
    )
}

// ---------------------------------------------------------------------------
// check_topology_spread_constraints
// ---------------------------------------------------------------------------

/// No constraints → always passes, zero penalty.
///
/// Upstream: filtering_test.go "no topology spread constraints"
#[test]
fn topology_no_constraints_always_passes() {
    let node = make_node("n1", "2", "4Gi");
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_req("100m", "64Mi")],
            topology_spread_constraints: None,
            ..Default::default()
        };
        Pod::new("p", spec)
    };
    let (passes, penalty) =
        check_topology_spread_constraints(&node, &pod, &[], std::slice::from_ref(&node));
    assert!(passes, "no constraints must always pass");
    assert_eq!(penalty, 0);
}

/// maxSkew=1, DoNotSchedule, both zones empty — placing on any node is OK.
///
/// Upstream: filtering_test.go "two zones, even spread, maxSkew=1"
#[test]
fn topology_even_spread_two_zones_passes() {
    let mut labels_a = HashMap::new();
    labels_a.insert("zone".to_string(), "a".to_string());
    let mut labels_b = HashMap::new();
    labels_b.insert("zone".to_string(), "b".to_string());

    let node_a = make_node_with_labels("n-a", "2", "4Gi", labels_a);
    let node_b = make_node_with_labels("n-b", "2", "4Gi", labels_b);

    let constraint = TopologySpreadConstraint {
        max_skew: 1,
        topology_key: "zone".to_string(),
        when_unsatisfiable: "DoNotSchedule".to_string(),
        label_selector: Some(make_label_selector("app", "spread-test")),
        min_domains: None,
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    };
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_req("100m", "64Mi")],
            topology_spread_constraints: Some(vec![constraint]),
            ..Default::default()
        };
        let mut p = Pod::new("incoming", spec);
        p.metadata.labels = Some({
            let mut m = HashMap::new();
            m.insert("app".to_string(), "spread-test".to_string());
            m
        });
        p
    };

    // No existing pods — both zones are empty; placing on node_a is fine.
    let (passes, _) =
        check_topology_spread_constraints(&node_a, &pod, &[], &[node_a.clone(), node_b]);
    assert!(passes, "empty zones → even spread → must pass");
}

/// maxSkew=1, DoNotSchedule: zone-a has 2 pods, zone-b has 0.
/// Placing on zone-b (count 0+1=1) → skew = 2-1 = 1 ≤ maxSkew=1 → passes.
///
/// Upstream: filtering_test.go "imbalanced zones, scheduling on lower zone passes"
#[test]
fn topology_skew_one_within_max_passes() {
    let mut zone_a_labels = HashMap::new();
    zone_a_labels.insert("zone".to_string(), "a".to_string());
    let mut zone_b_labels = HashMap::new();
    zone_b_labels.insert("zone".to_string(), "b".to_string());

    let node_a = make_node_with_labels("n-a", "4", "8Gi", zone_a_labels);
    let node_b = make_node_with_labels("n-b", "4", "8Gi", zone_b_labels);

    // Two pods in zone-a.
    let p1 = pod_on_node_with_labels("p1", "n-a", {
        let mut m = HashMap::new();
        m.insert("app".to_string(), "spread-test".to_string());
        m
    });
    let p2 = pod_on_node_with_labels("p2", "n-a", {
        let mut m = HashMap::new();
        m.insert("app".to_string(), "spread-test".to_string());
        m
    });

    let constraint = TopologySpreadConstraint {
        max_skew: 1,
        topology_key: "zone".to_string(),
        when_unsatisfiable: "DoNotSchedule".to_string(),
        label_selector: Some(make_label_selector("app", "spread-test")),
        min_domains: None,
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    };
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_req("100m", "64Mi")],
            topology_spread_constraints: Some(vec![constraint]),
            ..Default::default()
        };
        let mut p = Pod::new("incoming", spec);
        p.metadata.labels = Some({
            let mut m = HashMap::new();
            m.insert("app".to_string(), "spread-test".to_string());
            m
        });
        p
    };

    // Placing on zone-b: zone-a=2, zone-b=0+1=1 → skew=2-1=1 ≤ maxSkew(1) → passes.
    let (passes, _) =
        check_topology_spread_constraints(&node_b, &pod, &[p1, p2], &[node_a, node_b.clone()]);
    assert!(passes, "skew of 1 meets maxSkew=1");
}

/// maxSkew=1, DoNotSchedule: zone-a has 2 pods, zone-b has 0.
/// Placing on zone-a (2+1=3) → skew = 3-0 = 3 > maxSkew=1 → rejected.
///
/// Upstream: filtering_test.go "skew exceeds maxSkew, DoNotSchedule → fail"
#[test]
fn topology_skew_exceeds_max_do_not_schedule_fails() {
    let mut zone_a_labels = HashMap::new();
    zone_a_labels.insert("zone".to_string(), "a".to_string());
    let mut zone_b_labels = HashMap::new();
    zone_b_labels.insert("zone".to_string(), "b".to_string());

    let node_a = make_node_with_labels("n-a", "4", "8Gi", zone_a_labels);
    let node_b = make_node_with_labels("n-b", "4", "8Gi", zone_b_labels);

    let p1 = pod_on_node_with_labels("p1", "n-a", {
        let mut m = HashMap::new();
        m.insert("app".to_string(), "spread-test".to_string());
        m
    });
    let p2 = pod_on_node_with_labels("p2", "n-a", {
        let mut m = HashMap::new();
        m.insert("app".to_string(), "spread-test".to_string());
        m
    });

    let constraint = TopologySpreadConstraint {
        max_skew: 1,
        topology_key: "zone".to_string(),
        when_unsatisfiable: "DoNotSchedule".to_string(),
        label_selector: Some(make_label_selector("app", "spread-test")),
        min_domains: None,
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    };
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_req("100m", "64Mi")],
            topology_spread_constraints: Some(vec![constraint]),
            ..Default::default()
        };
        let mut p = Pod::new("incoming", spec);
        p.metadata.labels = Some({
            let mut m = HashMap::new();
            m.insert("app".to_string(), "spread-test".to_string());
            m
        });
        p
    };

    // Placing on zone-a: new count = 3, zone-b = 0 → skew = 3 > 1 → must fail.
    let (passes, _) =
        check_topology_spread_constraints(&node_a, &pod, &[p1, p2], &[node_a.clone(), node_b]);
    assert!(!passes, "skew 3 > maxSkew 1 with DoNotSchedule must reject");
}

/// maxSkew=1, ScheduleAnyway with same imbalanced setup — must pass with penalty > 0.
///
/// Upstream: scoring_test.go "ScheduleAnyway allows but penalizes skew violations"
#[test]
fn topology_schedule_anyway_allows_skew_violation_with_penalty() {
    let mut zone_a_labels = HashMap::new();
    zone_a_labels.insert("zone".to_string(), "a".to_string());
    let mut zone_b_labels = HashMap::new();
    zone_b_labels.insert("zone".to_string(), "b".to_string());

    let node_a = make_node_with_labels("n-a", "4", "8Gi", zone_a_labels);
    let node_b = make_node_with_labels("n-b", "4", "8Gi", zone_b_labels);

    let p1 = pod_on_node_with_labels("p1", "n-a", {
        let mut m = HashMap::new();
        m.insert("app".to_string(), "spread-test".to_string());
        m
    });
    let p2 = pod_on_node_with_labels("p2", "n-a", {
        let mut m = HashMap::new();
        m.insert("app".to_string(), "spread-test".to_string());
        m
    });

    let constraint = TopologySpreadConstraint {
        max_skew: 1,
        topology_key: "zone".to_string(),
        when_unsatisfiable: "ScheduleAnyway".to_string(),
        label_selector: Some(make_label_selector("app", "spread-test")),
        min_domains: None,
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    };
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_req("100m", "64Mi")],
            topology_spread_constraints: Some(vec![constraint]),
            ..Default::default()
        };
        let mut p = Pod::new("incoming", spec);
        p.metadata.labels = Some({
            let mut m = HashMap::new();
            m.insert("app".to_string(), "spread-test".to_string());
            m
        });
        p
    };

    // ScheduleAnyway on the heavy zone must pass but incur a positive penalty.
    let (passes, penalty) =
        check_topology_spread_constraints(&node_a, &pod, &[p1, p2], &[node_a.clone(), node_b]);
    assert!(
        passes,
        "ScheduleAnyway must not hard-reject even with skew > maxSkew"
    );
    assert!(
        penalty > 0,
        "skew violation with ScheduleAnyway must carry a positive penalty, got {penalty}"
    );
}

/// Node missing the topologyKey label → DoNotSchedule must reject.
///
/// Upstream: filtering_test.go "node without topologyKey label is not eligible"
#[test]
fn topology_node_missing_topology_key_do_not_schedule_rejects() {
    // Node has NO "zone" label.
    let node = make_node("n-unlabeled", "2", "4Gi");

    let constraint = TopologySpreadConstraint {
        max_skew: 1,
        topology_key: "zone".to_string(),
        when_unsatisfiable: "DoNotSchedule".to_string(),
        label_selector: Some(make_label_selector("app", "spread-test")),
        min_domains: None,
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    };
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_req("100m", "64Mi")],
            topology_spread_constraints: Some(vec![constraint]),
            ..Default::default()
        };
        Pod::new("incoming", spec)
    };

    let (passes, _) =
        check_topology_spread_constraints(&node, &pod, &[], std::slice::from_ref(&node));
    assert!(
        !passes,
        "node missing topology key with DoNotSchedule must be rejected"
    );
}

/// Node missing the topologyKey label → ScheduleAnyway must still pass.
///
/// Upstream: filtering_test.go "node without topologyKey, ScheduleAnyway → allowed"
#[test]
fn topology_node_missing_topology_key_schedule_anyway_passes() {
    let node = make_node("n-unlabeled", "2", "4Gi");

    let constraint = TopologySpreadConstraint {
        max_skew: 1,
        topology_key: "zone".to_string(),
        when_unsatisfiable: "ScheduleAnyway".to_string(),
        label_selector: Some(make_label_selector("app", "spread-test")),
        min_domains: None,
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    };
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_req("100m", "64Mi")],
            topology_spread_constraints: Some(vec![constraint]),
            ..Default::default()
        };
        Pod::new("incoming", spec)
    };

    let (passes, _) =
        check_topology_spread_constraints(&node, &pod, &[], std::slice::from_ref(&node));
    assert!(
        passes,
        "node missing topology key with ScheduleAnyway must be allowed"
    );
}

/// maxSkew=2 — a larger skew budget allows the same imbalanced setup to pass.
///
/// Upstream: filtering_test.go "maxSkew=2 allows higher imbalance"
#[test]
fn topology_larger_max_skew_permits_more_imbalance() {
    let mut zone_a_labels = HashMap::new();
    zone_a_labels.insert("zone".to_string(), "a".to_string());
    let mut zone_b_labels = HashMap::new();
    zone_b_labels.insert("zone".to_string(), "b".to_string());

    let node_a = make_node_with_labels("n-a", "4", "8Gi", zone_a_labels);
    let node_b = make_node_with_labels("n-b", "4", "8Gi", zone_b_labels);

    // Three pods in zone-a, zero in zone-b.
    let pods: Vec<Pod> = (1..=3)
        .map(|i| {
            pod_on_node_with_labels(&format!("p{i}"), "n-a", {
                let mut m = HashMap::new();
                m.insert("app".to_string(), "spread-test".to_string());
                m
            })
        })
        .collect();

    let constraint = TopologySpreadConstraint {
        max_skew: 2,
        topology_key: "zone".to_string(),
        when_unsatisfiable: "DoNotSchedule".to_string(),
        label_selector: Some(make_label_selector("app", "spread-test")),
        min_domains: None,
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    };
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_req("100m", "64Mi")],
            topology_spread_constraints: Some(vec![constraint]),
            ..Default::default()
        };
        let mut p = Pod::new("incoming", spec);
        p.metadata.labels = Some({
            let mut m = HashMap::new();
            m.insert("app".to_string(), "spread-test".to_string());
            m
        });
        p
    };

    // zone-b gets 0+1=1 pod; zone-a=3 → skew=3-1=2 ≤ maxSkew(2) → passes.
    let (passes, _) =
        check_topology_spread_constraints(&node_b, &pod, &pods, &[node_a, node_b.clone()]);
    assert!(passes, "skew=2 equals maxSkew=2, should pass");
}

// ---------------------------------------------------------------------------
// check_host_port_conflicts
// ---------------------------------------------------------------------------

/// No pods on node → no conflict.
///
/// Upstream: node_ports_test.go "no existing pods → always allowed"
#[test]
fn hostport_no_existing_pods_no_conflict() {
    let node = make_node("n", "2", "4Gi");
    let pod = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 8080, "TCP", None)],
            ..Default::default()
        };
        Pod::new("p", spec)
    };
    assert!(
        check_host_port_conflicts(&node, &pod, &[]),
        "no existing pods → no conflict"
    );
}

/// Pod has no host ports → always OK regardless of what's on the node.
///
/// Upstream: node_ports_test.go "pod without hostPort never conflicts"
#[test]
fn hostport_pod_without_host_port_never_conflicts() {
    let node = make_node("n", "2", "4Gi");
    let existing = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 8080, "TCP", None)],
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut p = Pod::new("existing", spec);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        p
    };
    // Incoming pod has no hostPort at all.
    let incoming = incoming_pod("inc", 0, "100m", "64Mi");
    assert!(
        check_host_port_conflicts(&node, &incoming, &[existing]),
        "pod without hostPort must never conflict"
    );
}

/// Same port + same protocol + same specific hostIP → conflict.
///
/// Upstream: node_ports_test.go "same (port, protocol, hostIP) → FitError"
#[test]
fn hostport_same_port_protocol_ip_conflicts() {
    let node = make_node("n", "2", "4Gi");
    let existing = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", Some("10.0.0.1"))],
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut p = Pod::new("existing", spec);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        p
    };
    let incoming = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", Some("10.0.0.1"))],
            ..Default::default()
        };
        Pod::new("inc", spec)
    };
    assert!(
        !check_host_port_conflicts(&node, &incoming, &[existing]),
        "same (port, protocol, ip) must conflict"
    );
}

/// Same port + same protocol but different specific IPs → no conflict.
///
/// Upstream: node_ports_test.go "different hostIP, same port → no conflict"
#[test]
fn hostport_different_specific_ips_same_port_no_conflict() {
    let node = make_node("n", "2", "4Gi");
    let existing = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", Some("10.0.0.1"))],
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut p = Pod::new("existing", spec);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        p
    };
    let incoming = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", Some("10.0.0.2"))],
            ..Default::default()
        };
        Pod::new("inc", spec)
    };
    assert!(
        check_host_port_conflicts(&node, &incoming, &[existing]),
        "different specific hostIPs must not conflict on same port"
    );
}

/// Same port + different protocol → no conflict.
///
/// Upstream: node_ports_test.go "same port, TCP vs UDP → no conflict"
#[test]
fn hostport_different_protocol_no_conflict() {
    let node = make_node("n", "2", "4Gi");
    let existing = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", None)],
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut p = Pod::new("existing", spec);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        p
    };
    let incoming = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "UDP", None)],
            ..Default::default()
        };
        Pod::new("inc", spec)
    };
    assert!(
        check_host_port_conflicts(&node, &incoming, &[existing]),
        "TCP vs UDP on same port must not conflict"
    );
}

/// Wildcard "0.0.0.0" existing → conflicts with any specific IP on same port+proto.
///
/// Upstream: node_ports_test.go "0.0.0.0 wildcard conflicts with specific IP"
#[test]
fn hostport_wildcard_zero_conflicts_with_specific() {
    let node = make_node("n", "2", "4Gi");
    let existing = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", Some("0.0.0.0"))],
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut p = Pod::new("existing", spec);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        p
    };
    let incoming = {
        let spec = PodSpec {
            containers: vec![make_container_with_port(
                "c",
                9000,
                "TCP",
                Some("192.168.1.5"),
            )],
            ..Default::default()
        };
        Pod::new("inc", spec)
    };
    assert!(
        !check_host_port_conflicts(&node, &incoming, &[existing]),
        "0.0.0.0 existing must conflict with specific IP on same port"
    );
}

/// IPv6 wildcard "::" existing → conflicts with any specific IP.
///
/// Upstream: node_ports_test.go ":: wildcard (IPv6) conflicts with specific host"
#[test]
fn hostport_ipv6_wildcard_conflicts_with_specific() {
    let node = make_node("n", "2", "4Gi");
    let existing = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", Some("::"))],
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut p = Pod::new("existing", spec);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        p
    };
    let incoming = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", Some("10.1.2.3"))],
            ..Default::default()
        };
        Pod::new("inc", spec)
    };
    assert!(
        !check_host_port_conflicts(&node, &incoming, &[existing]),
        ":: (IPv6 wildcard) must conflict with specific IP on same port"
    );
}

/// Succeeded (terminal) pods don't block the port.
///
/// Upstream: node_ports_test.go "Succeeded pod releases hostPort"
#[test]
fn hostport_terminal_pod_releases_port() {
    let node = make_node("n", "2", "4Gi");
    let mut existing = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", None)],
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        Pod::new("existing", spec)
    };
    existing.status = Some(PodStatus {
        phase: Some(Phase::Succeeded),
        ..Default::default()
    });
    let incoming = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", None)],
            ..Default::default()
        };
        Pod::new("inc", spec)
    };
    assert!(
        check_host_port_conflicts(&node, &incoming, &[existing]),
        "Succeeded pod must not block hostPort"
    );
}

/// Pod on a different node doesn't block this node's port.
///
/// Upstream: node_ports_test.go "existing pod on different node → no conflict"
#[test]
fn hostport_pod_on_different_node_no_conflict() {
    let node = make_node("n1", "2", "4Gi");
    let existing = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", None)],
            node_name: Some("n2".to_string()),
            ..Default::default()
        };
        let mut p = Pod::new("existing", spec);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        p
    };
    let incoming = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9000, "TCP", None)],
            ..Default::default()
        };
        Pod::new("inc", spec)
    };
    assert!(
        check_host_port_conflicts(&node, &incoming, &[existing]),
        "pod on different node must not cause a conflict on this node"
    );
}

/// Multiple containers each with a unique host port — no conflict.
///
/// Upstream: node_ports_test.go "multiple containers, distinct hostPorts → no conflict"
#[test]
fn hostport_multi_container_different_ports_no_conflict() {
    let node = make_node("n", "4", "8Gi");
    let existing = {
        let spec = PodSpec {
            containers: vec![
                make_container_with_port("c1", 8001, "TCP", None),
                make_container_with_port("c2", 8002, "TCP", None),
            ],
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut p = Pod::new("existing", spec);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        p
    };
    let incoming = {
        let spec = PodSpec {
            containers: vec![make_container_with_port("c", 9999, "TCP", None)],
            ..Default::default()
        };
        Pod::new("inc", spec)
    };
    assert!(
        check_host_port_conflicts(&node, &incoming, &[existing]),
        "distinct ports across containers must not conflict"
    );
}

// ---------------------------------------------------------------------------
// check_preemption  /  check_preemption_with_pdbs
// ---------------------------------------------------------------------------

/// High-priority pod preempts a single lower-priority pod.
///
/// Upstream: preemption_test.go "basic preemption, single victim"
#[test]
fn preemption_basic_single_victim() {
    let node = make_node("n", "1", "2Gi");
    let victim = scheduled_pod("low", 10, "1", "1Gi", "n");
    let preemptor = incoming_pod("high", 1000, "1", "1Gi");

    let (ok, victims) = check_preemption(&node, &preemptor, &[victim]);
    assert!(ok, "high-priority pod should preempt lower-priority pod");
    assert_eq!(victims, vec!["low"]);
}

/// Pod cannot preempt an equal-priority pod.
///
/// Upstream: preemption_test.go "equal priority → no preemption"
#[test]
fn preemption_equal_priority_no_victims() {
    let node = make_node("n", "1", "2Gi");
    let existing = scheduled_pod("same", 500, "1", "1Gi", "n");
    let preemptor = incoming_pod("also-500", 500, "1", "1Gi");

    let (ok, victims) = check_preemption(&node, &preemptor, &[existing]);
    assert!(!ok, "equal priority must not trigger preemption");
    assert!(victims.is_empty());
}

/// Pod fits without evicting anyone → preempt returns ok=true, no victims.
///
/// Upstream: preemption_test.go "pod fits without eviction"
#[test]
fn preemption_pod_fits_no_victims_needed() {
    let node = make_node("n", "4", "8Gi");
    let existing = scheduled_pod("low", 10, "1", "1Gi", "n");
    // Preemptor wants only 100m — plenty of room.
    let preemptor = incoming_pod("high", 1000, "100m", "64Mi");

    let (ok, victims) = check_preemption(&node, &preemptor, &[existing]);
    assert!(ok, "pod that fits without eviction should return ok=true");
    assert!(
        victims.is_empty(),
        "no eviction needed when pod already fits"
    );
}

/// preemptionPolicy=Never → no preemption even when resources are tight.
///
/// Upstream: preemption_test.go "PreemptionPolicy: Never → skip preemption"
#[test]
fn preemption_policy_never_is_respected() {
    let node = make_node("n", "1", "2Gi");
    let victim = scheduled_pod("low", 10, "1", "1Gi", "n");
    let mut preemptor = incoming_pod("nice", 1000, "1", "1Gi");
    preemptor.spec.as_mut().unwrap().preemption_policy = Some("Never".to_string());

    let (ok, victims) = check_preemption(&node, &preemptor, &[victim]);
    assert!(!ok, "preemptionPolicy=Never must not preempt");
    assert!(victims.is_empty());
}

/// system-cluster-critical pod (priority 2_000_000_000) cannot be preempted by lower priority.
///
/// Upstream: preemption_test.go "system-critical pod protection"
#[test]
fn preemption_protects_system_critical() {
    let node = make_node("n", "1", "2Gi");
    let critical = scheduled_pod("dns", 2_000_000_000, "1", "1Gi", "n");
    let preemptor = incoming_pod("app", 1_000_000, "1", "1Gi");

    let (ok, victims) = check_preemption(&node, &preemptor, &[critical]);
    assert!(
        !ok || victims.is_empty(),
        "system-critical pod must not be evicted by lower priority"
    );
}

/// Reprieve logic: only the minimal (lowest-priority) set is evicted.
///
/// Upstream: default_preemption_test.go "selectVictimsOnNode evicts minimal set"
#[test]
fn preemption_reprieve_keeps_higher_priority_if_possible() {
    // Node has 3 CPU total. Used: 1 by high-existing (pri 500) + 1 by low-existing (pri 10) = 2.
    // Free = 1 CPU. Preemptor needs 2 CPU. Evicting low-existing frees 1 → total free = 2. Fits.
    // So high-existing should be reprieved.
    let node = make_node("n", "3", "8Gi");
    let high_pod = scheduled_pod("high-existing", 500, "1", "1Gi", "n");
    let low_pod = scheduled_pod("low-existing", 10, "1", "1Gi", "n");
    let preemptor = incoming_pod("preemptor", 1000, "2", "1Gi");

    let (ok, victims) = check_preemption(&node, &preemptor, &[high_pod, low_pod]);
    assert!(ok, "preemption should succeed");
    assert!(
        victims.contains(&"low-existing".to_string()),
        "lowest-priority pod must be in victims"
    );
    assert!(
        !victims.contains(&"high-existing".to_string()),
        "higher-priority pod should be reprieved when not needed"
    );
}

/// Incoming pod priority ≤ 0 → never preempts.
///
/// Upstream: preemption_test.go "priority=0 pod cannot preempt"
#[test]
fn preemption_zero_priority_never_preempts() {
    let node = make_node("n", "1", "2Gi");
    let victim = scheduled_pod("victim", 0, "1", "1Gi", "n");
    let preemptor = incoming_pod("zero", 0, "1", "1Gi");

    let (ok, _) = check_preemption(&node, &preemptor, &[victim]);
    assert!(!ok, "priority=0 pod must not preempt");
}

/// Multiple victims: preemptor needs more resources than any single victim can free.
///
/// Upstream: preemption_test.go "multi-victim eviction"
#[test]
fn preemption_multiple_victims_evicted() {
    // Node: 4 CPU. Three pods use 1 CPU each (3 used, 1 free). Preemptor needs 3 CPU.
    // Evicting only v1+v2 (lowest) frees 2 → 1+2=3 available. Fits.
    // v3 (pri 500) should be reprieved.
    let node = make_node("n", "4", "8Gi");
    let p1 = scheduled_pod("v1", 10, "1", "1Gi", "n");
    let p2 = scheduled_pod("v2", 20, "1", "1Gi", "n");
    let p3 = scheduled_pod("v3", 500, "1", "1Gi", "n");
    let preemptor = incoming_pod("big", 1000, "3", "1Gi");

    let (ok, victims) = check_preemption(&node, &preemptor, &[p1, p2, p3]);
    assert!(ok, "preemption of multiple pods must succeed");
    assert!(
        victims.contains(&"v1".to_string()),
        "v1 (pri 10) must be a victim"
    );
    assert!(
        victims.contains(&"v2".to_string()),
        "v2 (pri 20) must be a victim"
    );
    assert!(
        !victims.contains(&"v3".to_string()),
        "v3 (pri 500) should be reprieved — evicting v1+v2 frees enough"
    );
}

/// Node can't fit preemptor even after removing all candidates → returns false.
///
/// Upstream: preemption_test.go "pod too large for node even after full eviction"
#[test]
fn preemption_node_too_small_even_after_full_eviction() {
    let node = make_node("n", "2", "4Gi");
    let victim = scheduled_pod("low", 10, "1", "1Gi", "n");
    // Preemptor wants 10 CPU — more than the node can ever provide.
    let preemptor = incoming_pod("giant", 1000, "10", "1Gi");

    let (ok, _) = check_preemption(&node, &preemptor, &[victim]);
    assert!(!ok, "node too small to ever fit preemptor");
}

/// PDB-aware preemption: prefers evicting non-PDB-covered pods.
///
/// Upstream: preemption_test.go "PDB-covered victim protected, evict non-PDB pod instead"
#[test]
fn preemption_pdb_covered_pod_protected_when_alternative_exists() {
    // Node: 2 CPU. p1-protected (PDB minAvailable=2) and p2-free each use 1 CPU.
    // Preemptor needs 1 CPU → only one victim needed.
    // PDB would be violated if p1 is evicted (healthy drops from 2 to 1 < minAvailable=2).
    // Scheduler must choose p2 instead.
    let node = make_node("n", "2", "4Gi");

    let mut labels_p1 = HashMap::new();
    labels_p1.insert("app".to_string(), "protected".to_string());
    let p1 = {
        let spec = PodSpec {
            containers: vec![make_container_req("1", "1Gi")],
            priority: Some(5),
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new("p1-protected", spec);
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.labels = Some(labels_p1);
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        pod
    };

    let p2 = scheduled_pod("p2-free", 5, "1", "1Gi", "n");

    // PDB: minAvailable=2 means we need 2 healthy pods; evicting p1 drops to 1 → violation.
    let pdb = make_pdb("pdb1", "default", "app", "protected", 2);

    let all_pods = vec![p1, p2];
    let preemptor = incoming_pod("high", 1000, "1", "1Gi");

    let (ok, victims) = check_preemption_with_pdbs(
        &node,
        &preemptor,
        &all_pods,
        &[pdb],
        &std::collections::HashMap::new(),
    );
    assert!(ok, "preemption must succeed — p2 can be evicted");
    assert!(
        victims.contains(&"p2-free".to_string()),
        "p2 (no PDB) must be evicted, got {victims:?}"
    );
    assert!(
        !victims.contains(&"p1-protected".to_string()),
        "p1 (PDB-covered) must be reprieved, got {victims:?}"
    );
}

/// PDB-aware preemption: PDB-covered pods are considered last in victim selection;
/// when only the PDB-covered pod remains and the resource need is tight, the
/// PDB bias ensures it is NOT reprieved (i.e. it is still evicted when there is
/// no non-PDB alternative), which mirrors the upstream k8s behavior that
/// minimises PDB disruptions rather than eliminating them entirely.
///
/// This test verifies that when there are TWO victims and the PDB covers one of
/// them, the PDB-covered pod is included in victims only if strictly necessary.
/// When the non-PDB pod alone frees enough resources, the PDB-covered pod must
/// be reprieved.
///
/// Upstream: preemption_test.go "PDB-covered pod reprieved when non-PDB victim suffices"
#[test]
fn preemption_pdb_covered_pod_reprieved_when_non_pdb_victim_suffices() {
    // Node: 3 CPU. Two pods each use 1 CPU (2 used, 1 free).
    // Preemptor needs 2 CPU total → must free 1 CPU.
    // p-pdb is covered by a PDB (minAvailable=2: evicting it would drop healthy from 2 to 1 < 2).
    // p-free has no PDB and also uses 1 CPU.
    // Freeing p-free alone: 1 (existing free) + 1 (freed) = 2 = needed. Fits.
    // The PDB-aware algorithm should therefore reprieve p-pdb and evict only p-free.
    let node = make_node("n", "3", "8Gi");

    let mut pdb_labels = HashMap::new();
    pdb_labels.insert("app".to_string(), "protected".to_string());

    let p_pdb = {
        let spec = PodSpec {
            containers: vec![make_container_req("1", "1Gi")],
            priority: Some(5),
            node_name: Some("n".to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new("p-pdb", spec);
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.labels = Some(pdb_labels.clone());
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        pod
    };

    let p_free = scheduled_pod("p-free", 5, "1", "1Gi", "n");

    // PDB: minAvailable=2. healthy_now=2 (both p_pdb + p_free are running).
    // Evicting p_pdb → healthy=1 < 2 → violation.
    let pdb = make_pdb("pdb1", "default", "app", "protected", 2);

    let preemptor = incoming_pod("high", 1000, "2", "1Gi");
    let all_pods = vec![p_pdb, p_free];

    let (ok, victims) = check_preemption_with_pdbs(
        &node,
        &preemptor,
        &all_pods,
        &[pdb],
        &std::collections::HashMap::new(),
    );
    assert!(ok, "preemption must succeed — p-free can be evicted");
    assert!(
        victims.contains(&"p-free".to_string()),
        "p-free (no PDB) must be evicted, got {victims:?}"
    );
    assert!(
        !victims.contains(&"p-pdb".to_string()),
        "p-pdb (PDB-covered) must be reprieved when p-free suffices, got {victims:?}"
    );
}

// ---------------------------------------------------------------------------
// calculate_resource_score / calculate_resource_score_with_pods
// ---------------------------------------------------------------------------

/// Fully empty node → score close to 100.
///
/// Upstream: balanced_allocation_test.go "empty node → max score"
#[test]
fn resource_score_empty_node_high_score() {
    let node = make_node("n", "4", "8Gi");
    let pod = incoming_pod("p", 0, "100m", "64Mi");
    let score = calculate_resource_score(&node, &pod);
    assert!(
        score > 50,
        "empty node with small pod should score high, got {score}"
    );
}

/// Pod that exactly fills the node → score 0 (no headroom).
///
/// Upstream: fit_test.go "node exactly fits pod → score 0"
#[test]
fn resource_score_exact_fit_scores_zero() {
    let node = make_node("n", "1", "1Gi");
    let pod = incoming_pod("p", 0, "1", "1Gi");
    let score = calculate_resource_score(&node, &pod);
    assert_eq!(score, 0, "pod that exactly fills node should score 0");
}

/// Pod larger than node capacity → score 0.
///
/// Upstream: fit_test.go "pod exceeds node capacity → score 0"
#[test]
fn resource_score_oversized_pod_scores_zero() {
    let node = make_node("n", "1", "1Gi");
    let pod = incoming_pod("p", 0, "2", "2Gi");
    let score = calculate_resource_score(&node, &pod);
    assert_eq!(score, 0, "oversized pod must score 0");
}

/// Node with zero allocatable CPU → score 0.
///
/// Upstream: fit_test.go "node with no CPU → pod cannot fit"
#[test]
fn resource_score_node_no_cpu_scores_zero() {
    let node = make_node("n", "0", "8Gi");
    let pod = incoming_pod("p", 0, "100m", "64Mi");
    let score = calculate_resource_score(&node, &pod);
    assert_eq!(
        score, 0,
        "node with zero CPU must score 0 for any non-trivial pod"
    );
}

/// Score decreases as node fills up.
///
/// Upstream: balanced_allocation_test.go "score decreases as utilization increases"
#[test]
fn resource_score_decreases_as_node_fills() {
    let node = make_node("n", "8", "16Gi");
    let pod = incoming_pod("p", 0, "1", "1Gi");

    // Score on fresh node.
    let score_empty = calculate_resource_score_with_pods(&node, &pod, &[]);

    // Add 4 CPU worth of existing pods.
    let existing_pods: Vec<Pod> = (1..=4)
        .map(|i| scheduled_pod(&format!("e{i}"), 0, "1", "1Gi", "n"))
        .collect();
    let score_half_full = calculate_resource_score_with_pods(&node, &pod, &existing_pods);

    assert!(
        score_empty > score_half_full,
        "score must decrease as node fills; empty={score_empty}, half-full={score_half_full}"
    );
}

/// Terminated pods don't count against available resources.
///
/// When a Succeeded pod is on the node, its resources must be treated as free.
/// If terminal pods were counted, the large incoming pod (requesting 1 CPU out of
/// 2 total) would find only 500m left (after "subtracting" the 1500m Succeeded pod).
/// Because terminal pods are correctly excluded, the full 2 CPU is free and the
/// incoming pod is schedulable (score > 0).
///
/// Upstream: fit_test.go "Succeeded/Failed pods don't consume resources"
#[test]
fn resource_score_terminal_pods_dont_consume_resources() {
    // Node: 2 CPU, 4Gi. One Succeeded pod "consumes" 1500m CPU.
    // If terminal pods counted → only 500m left → 1-CPU incoming pod can't fit (score 0).
    // If terminal pods are correctly excluded → 2 CPU free → pod fits (score > 0).
    let node = make_node("n", "2", "4Gi");

    let mut done = scheduled_pod("done", 0, "1500m", "1Gi", "n");
    done.status = Some(PodStatus {
        phase: Some(Phase::Succeeded),
        ..Default::default()
    });

    // Incoming pod requests 1 CPU: fits when terminal pod is excluded, overflows when counted.
    let pod = incoming_pod("p", 0, "1", "512Mi");

    let score = calculate_resource_score_with_pods(&node, &pod, &[done]);
    assert!(
        score > 0,
        "Succeeded pod must not block scheduling — full node capacity should be free, got score={score}"
    );
}

/// Extended resource requested but not available on node → score 0.
///
/// Upstream: fit_test.go "extended resource not present on node → FitError"
#[test]
fn resource_score_extended_resource_missing_scores_zero() {
    let node = make_node("n", "4", "8Gi");
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), "100m".to_string());
    requests.insert("memory".to_string(), "64Mi".to_string());
    requests.insert("example.com/gpu".to_string(), "1".to_string());
    let pod = {
        let container = Container {
            name: "main".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: Some(ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            }),
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            ..Default::default()
        };
        let spec = PodSpec {
            containers: vec![container],
            ..Default::default()
        };
        Pod::new("gpu-pod", spec)
    };

    let score = calculate_resource_score(&node, &pod);
    assert_eq!(
        score, 0,
        "pod requesting missing extended resource must score 0"
    );
}

/// Extended resource available on node is counted correctly.
///
/// Upstream: fit_test.go "extended resource present and fits → non-zero score"
#[test]
fn resource_score_extended_resource_present_allows_scheduling() {
    let mut alloc = HashMap::new();
    alloc.insert("cpu".to_string(), "4".to_string());
    alloc.insert("memory".to_string(), "8Gi".to_string());
    alloc.insert("example.com/gpu".to_string(), "2".to_string());
    let mut node = Node::new("n");
    node.status = Some(NodeStatus {
        capacity: Some(alloc.clone()),
        allocatable: Some(alloc),
        conditions: None,
        addresses: None,
        node_info: None,
        images: None,
        volumes_in_use: None,
        volumes_attached: None,
        daemon_endpoints: None,
        config: None,
        features: None,
        runtime_handlers: None,
        declared_features: None,
    });

    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), "100m".to_string());
    requests.insert("memory".to_string(), "64Mi".to_string());
    requests.insert("example.com/gpu".to_string(), "1".to_string());
    let pod = {
        let container = Container {
            name: "main".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: Some(ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            }),
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            ..Default::default()
        };
        let spec = PodSpec {
            containers: vec![container],
            ..Default::default()
        };
        Pod::new("gpu-pod", spec)
    };

    let score = calculate_resource_score(&node, &pod);
    assert!(
        score > 0,
        "pod fitting with extended resource must score > 0, got {score}"
    );
}

/// `calculate_resource_score` and `_with_pods(&[], &[])` must agree.
///
/// Upstream: internal consistency invariant in noderesources plugin.
#[test]
fn resource_score_no_pods_variants_agree() {
    let node = make_node("n", "4", "8Gi");
    let pod = incoming_pod("p", 0, "500m", "512Mi");

    let score_plain = calculate_resource_score(&node, &pod);
    let score_with_empty = calculate_resource_score_with_pods(&node, &pod, &[]);
    assert_eq!(
        score_plain, score_with_empty,
        "calculate_resource_score must equal calculate_resource_score_with_pods with empty pods slice"
    );
}
