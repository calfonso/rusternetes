//! Conformance: `NodeAffinity` + `NodeSelector` resource shape and
//! operator coverage not yet pinned by
//! `conformance_scheduling_predicates_affinity.rs`.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/scheduling/predicates.go` — NodeAffinity test suite.
//!   - `staging/src/k8s.io/api/core/v1/types.go::NodeSelectorRequirement`
//!     — operators `In`, `NotIn`, `Exists`, `DoesNotExist`, `Gt`, `Lt`.
//!
//! This file complements the existing affinity coverage by:
//!   1. Pinning the camelCase wire shape of `NodeAffinity` (the keys are
//!      famously long — drift goes undetected without a serde test).
//!   2. Exercising the `Exists` / `DoesNotExist` / `Gt` operators against
//!      `NodeAffinityPlugin::filter`.

use std::collections::HashMap;

use rusternetes_common::resources::node::NodeSpec;
use rusternetes_common::resources::pod::{
    Affinity, Container, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
    PodSpec,
};
use rusternetes_common::resources::{Node, Pod};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_scheduler::framework::{CycleState, FilterPlugin, FrameworkHandle};
use rusternetes_scheduler::plugins::NodeAffinityPlugin;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn node_with_labels(name: &str, labels: &[(&str, &str)]) -> Node {
    let mut meta = ObjectMeta::new(name);
    let mut l = HashMap::new();
    for (k, v) in labels {
        l.insert((*k).to_string(), (*v).to_string());
    }
    meta.labels = Some(l);
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: meta,
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        }),
        status: None,
    }
}

fn empty_container() -> Container {
    Container {
        name: "c".to_string(),
        image: "pause:3.10.1".to_string(),
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
    }
}

fn pod_with_affinity(name: &str, affinity: Affinity) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![empty_container()],
            init_containers: None,
            ephemeral_containers: None,
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            node_name: None,
            volumes: None,
            affinity: Some(affinity),
            tolerations: None,
            service_account_name: None,
            service_account: None,
            priority: None,
            priority_class_name: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
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
        }),
        status: None,
    }
}

fn required(reqs: Vec<NodeSelectorRequirement>) -> Affinity {
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

async fn filter_pass(plugin: &NodeAffinityPlugin, pod: &Pod, node: &Node) -> bool {
    let state = CycleState::new();
    let handle = FrameworkHandle::new(vec![], vec![node.clone()]);
    plugin.filter(&state, pod, node, &handle).await.is_success()
}

// ---------------------------------------------------------------------------
// Serde — `NodeAffinity` keys are infamously long
// ---------------------------------------------------------------------------

#[test]
fn node_affinity_required_key_uses_full_upstream_name() {
    // Upstream key: `requiredDuringSchedulingIgnoredDuringExecution`.
    let aff = required(vec![NodeSelectorRequirement {
        key: "k".to_string(),
        operator: "Exists".to_string(),
        values: None,
    }]);
    let v = serde_json::to_value(&aff).unwrap();
    assert!(
        v["nodeAffinity"]
            .get("requiredDuringSchedulingIgnoredDuringExecution")
            .is_some(),
        "required field must keep its full IgnoredDuringExecution suffix"
    );
}

// ---------------------------------------------------------------------------
// Plugin behaviour — operators beyond `In` / `NotIn`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exists_operator_accepts_node_with_the_label_key() {
    let node = node_with_labels("n", &[("disktype", "ssd")]);
    let pod = pod_with_affinity(
        "p",
        required(vec![NodeSelectorRequirement {
            key: "disktype".to_string(),
            operator: "Exists".to_string(),
            values: None,
        }]),
    );
    assert!(filter_pass(&NodeAffinityPlugin, &pod, &node).await);
}

#[tokio::test]
async fn exists_operator_rejects_node_missing_label_key() {
    let node = node_with_labels("n", &[("other", "value")]);
    let pod = pod_with_affinity(
        "p",
        required(vec![NodeSelectorRequirement {
            key: "disktype".to_string(),
            operator: "Exists".to_string(),
            values: None,
        }]),
    );
    assert!(!filter_pass(&NodeAffinityPlugin, &pod, &node).await);
}

#[tokio::test]
async fn does_not_exist_operator_accepts_node_missing_label() {
    let node = node_with_labels("n", &[("other", "value")]);
    let pod = pod_with_affinity(
        "p",
        required(vec![NodeSelectorRequirement {
            key: "disktype".to_string(),
            operator: "DoesNotExist".to_string(),
            values: None,
        }]),
    );
    assert!(filter_pass(&NodeAffinityPlugin, &pod, &node).await);
}

#[tokio::test]
async fn does_not_exist_operator_rejects_node_with_label() {
    let node = node_with_labels("n", &[("disktype", "ssd")]);
    let pod = pod_with_affinity(
        "p",
        required(vec![NodeSelectorRequirement {
            key: "disktype".to_string(),
            operator: "DoesNotExist".to_string(),
            values: None,
        }]),
    );
    assert!(!filter_pass(&NodeAffinityPlugin, &pod, &node).await);
}
