//! Integration tests for HostPort DNAT rule generation.
//!
//! Covers the upstream e2e test
//! `test/e2e/network/hostport.go` — "validates two pods with different
//! HostPorts can coexist on the same node" (line 219).
//!
//! kube-proxy must install DNAT rules so that traffic to `nodeIP:hostPort`
//! is forwarded to `podIP:containerPort`. Different hostPorts on the same
//! node must each get their own rule and not clobber each other.

use rusternetes_common::resources::{Container, ContainerPort, Pod, PodSpec, PodStatus};
use rusternetes_common::types::ObjectMeta;
use rusternetes_kube_proxy::iptables::IptablesManager;

/// Build a minimal Pod that exposes a single hostPort.
fn pod_with_hostport(
    name: &str,
    namespace: &str,
    node_name: &str,
    pod_ip: &str,
    container_port: u16,
    host_port: u16,
    protocol: &str,
) -> Pod {
    let mut pod = Pod {
        type_meta: rusternetes_common::types::TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            node_name: Some(node_name.to_string()),
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx".to_string(),
                ports: Some(vec![ContainerPort {
                    container_port,
                    name: Some("http".to_string()),
                    protocol: protocol.to_string(),
                    host_port: Some(host_port),
                    host_ip: None,
                }]),
                command: None,
                args: None,
                working_dir: None,
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
            }],
            ..PodSpec::default()
        }),
        status: Some(PodStatus {
            pod_ip: Some(pod_ip.to_string()),
            ..PodStatus::default()
        }),
    };
    // Ensure default values are reasonable
    pod.metadata.uid = format!("uid-{}", name);
    pod
}

#[test]
fn hostport_dnat_rule_is_generated() {
    // A single pod with a hostPort must produce a DNAT rule that maps
    // `nodeIP:hostPort -> podIP:containerPort` in the KUBE-HOSTPORTS chain.
    let mgr = IptablesManager::new(
        rusternetes_kube_proxy::iptables::DEFAULT_CLUSTER_CIDR.to_string(),
        rusternetes_kube_proxy::iptables::DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![pod_with_hostport(
        "pod1",
        "default",
        "node-1",
        "10.244.0.5",
        8080,
        30080,
        "TCP",
    )];

    let rules = mgr.build_hostport_rules(&pods, "node-1");

    // The chain header must be declared so iptables-restore creates/clears it.
    assert!(
        rules.contains(":KUBE-HOSTPORTS - [0:0]"),
        "expected KUBE-HOSTPORTS chain header in rules:\n{}",
        rules
    );

    // The DNAT rule must target the pod IP and container port.
    assert!(
        rules.contains("--dport 30080"),
        "expected --dport 30080 in rules:\n{}",
        rules
    );
    assert!(
        rules.contains("-j DNAT --to-destination 10.244.0.5:8080"),
        "expected DNAT to pod IP+containerPort in rules:\n{}",
        rules
    );
    // Protocol must be lowercased per iptables convention.
    assert!(
        rules.contains("-p tcp"),
        "expected -p tcp in rules:\n{}",
        rules
    );
}

#[test]
fn distinct_hostports_coexist_on_same_node() {
    // Two pods on the same node with DIFFERENT hostPorts must each produce
    // their own DNAT rule. Neither should clobber the other.
    // This mirrors upstream e2e network/hostport.go:219.
    let mgr = IptablesManager::new(
        rusternetes_kube_proxy::iptables::DEFAULT_CLUSTER_CIDR.to_string(),
        rusternetes_kube_proxy::iptables::DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![
        pod_with_hostport(
            "pod1",
            "default",
            "node-1",
            "10.244.0.5",
            8080,
            54321,
            "TCP",
        ),
        pod_with_hostport(
            "pod2",
            "default",
            "node-1",
            "10.244.0.6",
            8080,
            54322,
            "TCP",
        ),
    ];

    let rules = mgr.build_hostport_rules(&pods, "node-1");

    // Both DNAT rules must be present.
    assert!(
        rules.contains("--dport 54321"),
        "expected --dport 54321 in rules:\n{}",
        rules
    );
    assert!(
        rules.contains("--dport 54322"),
        "expected --dport 54322 in rules:\n{}",
        rules
    );
    assert!(
        rules.contains("-j DNAT --to-destination 10.244.0.5:8080"),
        "expected DNAT for pod1 in rules:\n{}",
        rules
    );
    assert!(
        rules.contains("-j DNAT --to-destination 10.244.0.6:8080"),
        "expected DNAT for pod2 in rules:\n{}",
        rules
    );
}

#[test]
fn hostport_rules_skip_pods_on_other_nodes() {
    // kube-proxy runs per-node; it must only install hostPort DNAT rules
    // for pods scheduled on its own node. Pods on other nodes are handled
    // by the kube-proxy running there.
    let mgr = IptablesManager::new(
        rusternetes_kube_proxy::iptables::DEFAULT_CLUSTER_CIDR.to_string(),
        rusternetes_kube_proxy::iptables::DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let pods = vec![
        pod_with_hostport(
            "local-pod",
            "default",
            "node-1",
            "10.244.0.5",
            8080,
            30080,
            "TCP",
        ),
        pod_with_hostport(
            "remote-pod",
            "default",
            "node-2",
            "10.244.1.5",
            8080,
            30081,
            "TCP",
        ),
    ];

    let rules = mgr.build_hostport_rules(&pods, "node-1");

    assert!(
        rules.contains("--dport 30080"),
        "expected local hostPort in rules:\n{}",
        rules
    );
    assert!(
        !rules.contains("--dport 30081"),
        "remote node's hostPort must NOT appear in rules:\n{}",
        rules
    );
}

#[test]
fn hostport_rules_skip_pods_without_pod_ip() {
    // A hostPort with no podIP yet (pod still pending) cannot have a DNAT
    // target — skip it. The next sync will pick it up once the pod is running.
    let mgr = IptablesManager::new(
        rusternetes_kube_proxy::iptables::DEFAULT_CLUSTER_CIDR.to_string(),
        rusternetes_kube_proxy::iptables::DEFAULT_NODEPORT_RANGE.to_string(),
    );
    let mut pod = pod_with_hostport("pending-pod", "default", "node-1", "", 8080, 30080, "TCP");
    // Clear the pod IP to simulate a pending pod.
    pod.status = Some(PodStatus::default());

    let rules = mgr.build_hostport_rules(&[pod], "node-1");

    // The chain header is fine, but no DNAT rule should exist.
    assert!(
        !rules.contains("--dport 30080"),
        "pending pod (no podIP) must not produce a DNAT rule:\n{}",
        rules
    );
}
