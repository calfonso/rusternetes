//! Predicate plugin integration tests
//!
//! Mirrors upstream e2e `scheduling/predicates.go:1041,1102` cases:
//! - MatchNodeSelector with `NotIn` operator (NodeAffinityPlugin)
//! - PodToleratesNodeTaints with `Exists` operator and empty value (TaintTolerationPlugin)
//! - HostPort conflict avoidance (HostPortPlugin)

use rusternetes_common::resources::node::{NodeSpec, NodeStatus, Taint};
use rusternetes_common::resources::pod::{
    Affinity, Container, ContainerPort, NodeAffinity, NodeSelector, NodeSelectorRequirement,
    NodeSelectorTerm, PodSpec, PodStatus, Toleration,
};
use rusternetes_common::resources::{Node, Pod};
use rusternetes_common::types::Phase;
use rusternetes_scheduler::framework::{CycleState, FilterPlugin, FrameworkHandle};
use rusternetes_scheduler::plugins::{HostPortPlugin, NodeAffinityPlugin, TaintTolerationPlugin};
use std::collections::HashMap;

// ---------- Helpers ----------

fn node_with_labels(name: &str, labels: &[(&str, &str)]) -> Node {
    let mut node = Node::new(name);
    if !labels.is_empty() {
        let mut map = HashMap::new();
        for (k, v) in labels {
            map.insert((*k).to_string(), (*v).to_string());
        }
        node.metadata.labels = Some(map);
    }
    node
}

fn node_with_taints(name: &str, taints: Vec<Taint>) -> Node {
    let mut node = Node::new(name);
    node.spec = Some(NodeSpec {
        pod_cidr: None,
        pod_cidrs: None,
        provider_id: None,
        unschedulable: Some(false),
        taints: Some(taints),
    });
    node.status = Some(NodeStatus::default());
    node
}

fn empty_pod(name: &str) -> Pod {
    Pod::new(
        name,
        PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "busybox".to_string(),
                command: None,
                args: None,
                working_dir: None,
                ports: None,
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
            ..Default::default()
        },
    )
}

fn pod_with_affinity(name: &str, affinity: Affinity) -> Pod {
    let mut pod = empty_pod(name);
    if let Some(spec) = pod.spec.as_mut() {
        spec.affinity = Some(affinity);
    }
    pod
}

fn pod_with_tolerations(name: &str, tolerations: Vec<Toleration>) -> Pod {
    let mut pod = empty_pod(name);
    if let Some(spec) = pod.spec.as_mut() {
        spec.tolerations = Some(tolerations);
    }
    pod
}

fn pod_with_host_port(name: &str, node: Option<&str>, port: u16, protocol: &str, ip: &str) -> Pod {
    let mut pod = empty_pod(name);
    if let Some(spec) = pod.spec.as_mut() {
        spec.containers[0].ports = Some(vec![ContainerPort {
            container_port: 80,
            name: None,
            protocol: protocol.to_string(),
            host_port: Some(port),
            host_ip: if ip.is_empty() {
                None
            } else {
                Some(ip.to_string())
            },
        }]);
        spec.node_name = node.map(|n| n.to_string());
    }
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    pod
}

fn required_node_affinity(reqs: Vec<NodeSelectorRequirement>) -> Affinity {
    Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(reqs),
                    match_fields: None,
                }],
            }),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
        pod_affinity: None,
        pod_anti_affinity: None,
    }
}

async fn run_filter<P: FilterPlugin>(
    plugin: &P,
    pod: &Pod,
    node: &Node,
    all_pods: Vec<Pod>,
) -> bool {
    let state = CycleState::new();
    let handle = FrameworkHandle::new(all_pods, vec![node.clone()]);
    plugin.filter(&state, pod, node, &handle).await.is_success()
}

// ---------- NodeAffinity / NotIn ----------

#[tokio::test]
async fn node_affinity_notin_rejects_matching_label() {
    // Pod requires `env NotIn [prod]`. Node has env=prod → reject.
    let node = node_with_labels("node-prod", &[("env", "prod")]);
    let pod = pod_with_affinity(
        "p",
        required_node_affinity(vec![NodeSelectorRequirement {
            key: "env".to_string(),
            operator: "NotIn".to_string(),
            values: Some(vec!["prod".to_string()]),
        }]),
    );
    let plugin = NodeAffinityPlugin;
    assert!(
        !run_filter(&plugin, &pod, &node, vec![]).await,
        "NotIn must reject when label value is in the set"
    );
}

#[tokio::test]
async fn node_affinity_notin_accepts_nonmatching_label() {
    // Pod requires `env NotIn [prod]`. Node has env=dev → accept.
    let node = node_with_labels("node-dev", &[("env", "dev")]);
    let pod = pod_with_affinity(
        "p",
        required_node_affinity(vec![NodeSelectorRequirement {
            key: "env".to_string(),
            operator: "NotIn".to_string(),
            values: Some(vec!["prod".to_string()]),
        }]),
    );
    let plugin = NodeAffinityPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "NotIn must accept when label value is not in the set"
    );
}

#[tokio::test]
async fn node_affinity_notin_accepts_missing_label() {
    // Pod requires `env NotIn [prod]`. Node has no `env` label → accept
    // (standard K8s semantic: absent label is treated as "not in").
    let node = node_with_labels("node-bare", &[("role", "worker")]);
    let pod = pod_with_affinity(
        "p",
        required_node_affinity(vec![NodeSelectorRequirement {
            key: "env".to_string(),
            operator: "NotIn".to_string(),
            values: Some(vec!["prod".to_string()]),
        }]),
    );
    let plugin = NodeAffinityPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "NotIn must accept when label key is absent"
    );
}

// ---------- TaintToleration / Exists ----------

#[tokio::test]
async fn taint_toleration_exists_with_empty_value_field() {
    // Toleration: key="dedicated", operator=Exists, value="" (empty, often
    // omitted in real specs). Taint: key="dedicated", value="gpu",
    // effect=NoSchedule. The Exists operator MUST match purely on key,
    // ignoring the value field on the toleration.
    let node = node_with_taints(
        "tainted",
        vec![Taint {
            key: "dedicated".to_string(),
            value: Some("gpu".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }],
    );
    let pod = pod_with_tolerations(
        "p",
        vec![Toleration {
            key: Some("dedicated".to_string()),
            operator: Some("Exists".to_string()),
            value: Some(String::new()), // empty string value field
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }],
    );
    let plugin = TaintTolerationPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "Exists toleration must match regardless of value field"
    );
}

#[tokio::test]
async fn taint_toleration_exists_with_empty_key_matches_any_taint() {
    // K8s wildcard toleration: operator=Exists with no key tolerates ALL taints.
    let node = node_with_taints(
        "tainted",
        vec![Taint {
            key: "any-key".to_string(),
            value: Some("any-value".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }],
    );
    let pod = pod_with_tolerations(
        "p",
        vec![Toleration {
            key: None,
            operator: Some("Exists".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        }],
    );
    let plugin = TaintTolerationPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "Empty-key Exists toleration must tolerate any taint"
    );
}

#[tokio::test]
async fn taint_toleration_equal_treats_none_and_empty_value_as_equivalent() {
    // K8s treats an unset `value` and an empty `value` as equivalent (both
    // represent "no value"). Equal op with toleration value=None should
    // match a taint with value=Some("").
    let node = node_with_taints(
        "tainted",
        vec![Taint {
            key: "k".to_string(),
            value: Some(String::new()), // empty string
            effect: "NoSchedule".to_string(),
            time_added: None,
        }],
    );
    let pod = pod_with_tolerations(
        "p",
        vec![Toleration {
            key: Some("k".to_string()),
            operator: Some("Equal".to_string()),
            value: None, // unset
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }],
    );
    let plugin = TaintTolerationPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "Equal toleration must treat None and Some(\"\") as equivalent"
    );
}

#[tokio::test]
async fn taint_toleration_equal_rejects_value_mismatch() {
    let node = node_with_taints(
        "tainted",
        vec![Taint {
            key: "k".to_string(),
            value: Some("a".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }],
    );
    let pod = pod_with_tolerations(
        "p",
        vec![Toleration {
            key: Some("k".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("b".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }],
    );
    let plugin = TaintTolerationPlugin;
    assert!(
        !run_filter(&plugin, &pod, &node, vec![]).await,
        "Equal toleration must reject when value differs"
    );
}

// ---------- HostPort conflict ----------

#[tokio::test]
async fn host_port_plugin_rejects_overlapping_wildcard() {
    let node = node_with_labels("node-1", &[]);
    let existing = pod_with_host_port("a", Some("node-1"), 8080, "TCP", "");
    let incoming = pod_with_host_port("b", None, 8080, "TCP", "");
    let plugin = HostPortPlugin;
    assert!(
        !run_filter(&plugin, &incoming, &node, vec![existing]).await,
        "Same hostPort+protocol with wildcard hostIP on same node must conflict"
    );
}

#[tokio::test]
async fn host_port_plugin_allows_distinct_protocol_or_ip() {
    let node = node_with_labels("node-1", &[]);
    let existing = pod_with_host_port("a", Some("node-1"), 8080, "TCP", "10.0.0.1");
    let incoming_diff_proto = pod_with_host_port("b", None, 8080, "UDP", "10.0.0.1");
    let incoming_diff_ip = pod_with_host_port("c", None, 8080, "TCP", "10.0.0.2");
    let plugin = HostPortPlugin;
    assert!(
        run_filter(&plugin, &incoming_diff_proto, &node, vec![existing.clone()]).await,
        "Different protocol must not conflict"
    );
    assert!(
        run_filter(&plugin, &incoming_diff_ip, &node, vec![existing]).await,
        "Different specific hostIP must not conflict"
    );
}

#[tokio::test]
async fn host_port_plugin_no_host_port_always_passes() {
    let node = node_with_labels("node-1", &[]);
    let pod = empty_pod("p"); // no hostPort
    let plugin = HostPortPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "Pod without hostPort must always pass"
    );
}
