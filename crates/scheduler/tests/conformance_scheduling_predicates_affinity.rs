//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-scheduling] SchedulerPredicates + node/pod affinity + taints/tolerations.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/scheduling/
//! (`predicates.go`, `taints.go`, `priorities.go`)
//!
//! A Sonobuoy v1.35 conformance run reports two
//! [sig-scheduling] SchedulerPredicates [Conformance] Ginkgo descriptors:
//!
//!   - predicates.go:333 `validates resource limits of pods that are allowed
//!     to run` — Round 160: FAIL (`predicates.go:1102 context deadline exceeded`,
//!     "Other" bucket).
//!   - predicates.go:445 `validates that NodeSelector is respected if not
//!     matching` — Round 160: PASS.
//!
//! The remaining tests in this file mirror non-Conformance-tagged but
//! semantically-equivalent scenarios from `predicates.go`, `taints.go`, and
//! `priorities.go` (NodeAffinity required/preferred, PodAffinity / PodAntiAffinity
//! required/preferred, taint effects NoSchedule / PreferNoSchedule / NoExecute).
//! These are unit-level mirrors of the upstream filter/score plugins
//! (`pkg/scheduler/framework/plugins/{nodeaffinity,interpodaffinity,tainttoleration,
//! noderesources}`).
//!
//! NO HTTP harness — scheduler logic is exercised by direct calls into
//! `rusternetes_scheduler::plugins` and `rusternetes_scheduler::advanced`.
//! Fixtures live as plain `Node`/`Pod` values; no `MemoryStorage` round-trip
//! is needed because the framework's `FrameworkHandle` accepts the pod/node
//! slices directly.
//!
//! See `docs/conformance/scheduling-predicates-affinity.md` for the
//! test-by-test status table.

use std::collections::HashMap;

use rusternetes_common::resources::node::{NodeSpec, NodeStatus, Taint};
use rusternetes_common::resources::pod::{
    Affinity, Container, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
    PodAffinity, PodAffinityTerm, PodAntiAffinity, PodSpec, PodStatus, PreferredSchedulingTerm,
    Toleration, WeightedPodAffinityTerm,
};
use rusternetes_common::resources::{Node, Pod};
use rusternetes_common::types::{LabelSelector, Phase, ResourceRequirements};
use rusternetes_scheduler::advanced::{
    calculate_resource_score_with_pods, check_pod_affinity, check_taints_tolerations,
};
use rusternetes_scheduler::framework::{CycleState, FilterPlugin, FrameworkHandle, ScorePlugin};
use rusternetes_scheduler::plugins::{
    NodeAffinityPlugin, NodeAffinityScoringPlugin, NodeSelectorPlugin, PodAffinityPlugin,
    PodAntiAffinityPlugin, TaintTolerationPlugin,
};

// ---------------------------------------------------------------------------
// Fixtures
//
// Per the scoped-conformance plan, helper duplication across files is
// acceptable for this batch; a future PR consolidates shared fixtures into a
// `test_support` module. These mirror the equivalents in
// `crates/scheduler/tests/predicates_test.rs`.
// ---------------------------------------------------------------------------

fn container_with_cpu(cpu_request: Option<&str>) -> Container {
    let mut requests = HashMap::new();
    if let Some(cpu) = cpu_request {
        requests.insert("cpu".to_string(), cpu.to_string());
    }
    Container {
        name: "c".to_string(),
        image: "registry.k8s.io/pause:3.10.1".to_string(),
        command: None,
        args: None,
        working_dir: None,
        ports: None,
        env: None,
        env_from: None,
        resources: if requests.is_empty() {
            None
        } else {
            Some(ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            })
        },
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

fn node_with(name: &str, labels: &[(&str, &str)], cpu: &str, memory: &str) -> Node {
    let mut allocatable = HashMap::new();
    allocatable.insert("cpu".to_string(), cpu.to_string());
    allocatable.insert("memory".to_string(), memory.to_string());
    let mut node = Node::new(name);
    if !labels.is_empty() {
        let mut map = HashMap::new();
        for (k, v) in labels {
            map.insert((*k).to_string(), (*v).to_string());
        }
        node.metadata.labels = Some(map);
    }
    node.spec = Some(NodeSpec {
        pod_cidr: None,
        pod_cidrs: None,
        provider_id: None,
        unschedulable: Some(false),
        taints: None,
    });
    node.status = Some(NodeStatus {
        capacity: Some(allocatable.clone()),
        allocatable: Some(allocatable),
        ..Default::default()
    });
    node
}

fn node_tainted(name: &str, taints: Vec<Taint>) -> Node {
    let mut node = node_with(name, &[], "4", "8Gi");
    if let Some(spec) = node.spec.as_mut() {
        spec.taints = Some(taints);
    }
    node
}

fn empty_pod(name: &str) -> Pod {
    Pod::new(
        name,
        PodSpec {
            containers: vec![container_with_cpu(None)],
            ..Default::default()
        },
    )
}

fn pod_with_cpu_request(name: &str, cpu: &str) -> Pod {
    Pod::new(
        name,
        PodSpec {
            containers: vec![container_with_cpu(Some(cpu))],
            ..Default::default()
        },
    )
}

fn pod_running_on(name: &str, node: &str, cpu_request: Option<&str>) -> Pod {
    let mut pod = Pod::new(
        name,
        PodSpec {
            containers: vec![container_with_cpu(cpu_request)],
            node_name: Some(node.to_string()),
            ..Default::default()
        },
    );
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    pod
}

fn pod_with_labels(name: &str, node: Option<&str>, labels: &[(&str, &str)]) -> Pod {
    let mut pod = empty_pod(name);
    let mut map = HashMap::new();
    for (k, v) in labels {
        map.insert((*k).to_string(), (*v).to_string());
    }
    pod.metadata.labels = Some(map);
    if let Some(n) = node {
        if let Some(spec) = pod.spec.as_mut() {
            spec.node_name = Some(n.to_string());
        }
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
    }
    pod
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

fn preferred_node_affinity(weight: i32, reqs: Vec<NodeSelectorRequirement>) -> Affinity {
    Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: None,
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                PreferredSchedulingTerm {
                    weight,
                    preference: NodeSelectorTerm {
                        match_expressions: Some(reqs),
                        match_fields: None,
                    },
                },
            ]),
        }),
        pod_affinity: None,
        pod_anti_affinity: None,
    }
}

fn required_pod_affinity(topology_key: &str, match_labels: &[(&str, &str)]) -> Affinity {
    let mut ml = HashMap::new();
    for (k, v) in match_labels {
        ml.insert((*k).to_string(), (*v).to_string());
    }
    Affinity {
        node_affinity: None,
        pod_affinity: Some(PodAffinity {
            required_during_scheduling_ignored_during_execution: Some(vec![PodAffinityTerm {
                label_selector: LabelSelector {
                    match_labels: Some(ml),
                    match_expressions: None,
                },
                namespaces: None,
                topology_key: topology_key.to_string(),
                ..Default::default()
            }]),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
        pod_anti_affinity: None,
    }
}

fn preferred_pod_affinity(
    weight: i32,
    topology_key: &str,
    match_labels: &[(&str, &str)],
) -> Affinity {
    let mut ml = HashMap::new();
    for (k, v) in match_labels {
        ml.insert((*k).to_string(), (*v).to_string());
    }
    Affinity {
        node_affinity: None,
        pod_affinity: Some(PodAffinity {
            required_during_scheduling_ignored_during_execution: None,
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                WeightedPodAffinityTerm {
                    weight,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: LabelSelector {
                            match_labels: Some(ml),
                            match_expressions: None,
                        },
                        namespaces: None,
                        topology_key: topology_key.to_string(),
                        ..Default::default()
                    },
                },
            ]),
        }),
        pod_anti_affinity: None,
    }
}

fn required_pod_anti_affinity(topology_key: &str, match_labels: &[(&str, &str)]) -> Affinity {
    let mut ml = HashMap::new();
    for (k, v) in match_labels {
        ml.insert((*k).to_string(), (*v).to_string());
    }
    Affinity {
        node_affinity: None,
        pod_affinity: None,
        pod_anti_affinity: Some(PodAntiAffinity {
            required_during_scheduling_ignored_during_execution: Some(vec![PodAffinityTerm {
                label_selector: LabelSelector {
                    match_labels: Some(ml),
                    match_expressions: None,
                },
                namespaces: None,
                topology_key: topology_key.to_string(),
                ..Default::default()
            }]),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
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

async fn run_score<P: ScorePlugin>(plugin: &P, pod: &Pod, node: &Node, all_pods: Vec<Pod>) -> i64 {
    let state = CycleState::new();
    let handle = FrameworkHandle::new(all_pods, vec![node.clone()]);
    plugin.score(&state, pod, node, &handle).await.unwrap_or(0)
}

// ---------------------------------------------------------------------------
// SchedulerPredicates — Conformance tests captured in Sonobuoy log
// ---------------------------------------------------------------------------

/// [sig-scheduling] SchedulerPredicates [Serial] validates resource limits of
/// pods that are allowed to run [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/predicates.go:333
/// Sonobuoy (Round 160, 2026-05-12): FAIL — "context deadline exceeded" at
/// predicates.go:1102 (Other bucket, see docs/CONFORMANCE.md:53).
///
/// The upstream test fills each node to ~70% CPU with filler pods, then asserts
/// that an additional pod requesting more CPU than the cluster has free is
/// rejected (PodScheduled=False, Reason=Unschedulable). The cluster-level
/// orchestration that fails upstream is outside the scheduler unit boundary —
/// this scoped mirror verifies the underlying predicate: a pod whose CPU
/// request exceeds remaining allocatable on every node receives a
/// `NodeResourcesFit` score of 0 (== infeasible), regardless of how many
/// filler pods are already running.
#[tokio::test]
async fn predicates_validates_resource_limits_rejects_oversized_pod() {
    // Two 4-CPU nodes with ~2800m of CPU already consumed by filler pods.
    let node_1 = node_with(
        "node-1",
        &[("kubernetes.io/hostname", "node-1")],
        "4",
        "8Gi",
    );
    let node_2 = node_with(
        "node-2",
        &[("kubernetes.io/hostname", "node-2")],
        "4",
        "8Gi",
    );
    let filler_1 = pod_running_on("filler-1", "node-1", Some("2800m"));
    let filler_2 = pod_running_on("filler-2", "node-2", Some("2800m"));
    let all_pods = vec![filler_1, filler_2];

    // Pod asks for 2000m; only ~1200m remains free on each node. Use the
    // pod-aware fit calculator that mirrors the scheduler's accounting of
    // already-scheduled pods (`NodeResourcesFitPlugin::score` itself does
    // not subtract running pods — that path is exercised in
    // `node_resources_fit_accounts_for_running_pod_requests`).
    let oversized = pod_with_cpu_request("additional", "2000m");
    assert_eq!(
        calculate_resource_score_with_pods(&node_1, &oversized, &all_pods),
        0,
        "oversized pod must be infeasible on node-1 (score=0)"
    );
    assert_eq!(
        calculate_resource_score_with_pods(&node_2, &oversized, &all_pods),
        0,
        "oversized pod must be infeasible on node-2 (score=0)"
    );
}

/// [sig-scheduling] SchedulerPredicates [Serial] validates that NodeSelector is
/// respected if not matching [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/predicates.go:445
/// Sonobuoy (Round 160, 2026-05-12): PASS.
///
/// Upstream test creates a pod with a `nodeSelector` that does not match any
/// node, then asserts the pod stays Pending with `PodScheduled=False,
/// Reason=Unschedulable`. The scoped mirror calls the `NodeSelectorPlugin`
/// filter directly and verifies it rejects nodes that lack the requested
/// label.
#[tokio::test]
async fn predicates_validates_nodeselector_rejects_unmatched_nodes() {
    let node = node_with(
        "node-1",
        &[("kubernetes.io/hostname", "node-1")],
        "4",
        "8Gi",
    );
    let mut pod = empty_pod("p");
    if let Some(spec) = pod.spec.as_mut() {
        let mut sel = HashMap::new();
        sel.insert(
            "kubernetes.io/e2e-az-name".to_string(),
            "e2e-az1".to_string(),
        );
        spec.node_selector = Some(sel);
    }

    let plugin = NodeSelectorPlugin;
    assert!(
        !run_filter(&plugin, &pod, &node, vec![]).await,
        "node without the requested label must be filtered out"
    );
}

// ---------------------------------------------------------------------------
// NodeAffinity — required + preferred (predicates.go / NodeAffinity feature)
// ---------------------------------------------------------------------------

/// [sig-scheduling] NodeAffinity required In operator accepts matching node
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/predicates.go (NodeAffinity
/// suite, `Should be able to schedule a pod when a NodeAffinity rule matches`).
/// Sonobuoy (Round 160): not enumerated separately (Conformance suite folds
/// this into the SchedulerPredicates parent — covered by the PASS-ing
/// NodeSelector test above).
#[tokio::test]
async fn node_affinity_required_in_operator_accepts_matching_node() {
    let node = node_with(
        "node-az1",
        &[("kubernetes.io/e2e-az-name", "e2e-az1")],
        "4",
        "8Gi",
    );
    let pod = pod_with_affinity(
        "p",
        required_node_affinity(vec![NodeSelectorRequirement {
            key: "kubernetes.io/e2e-az-name".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["e2e-az1".to_string(), "e2e-az2".to_string()]),
        }]),
    );
    let plugin = NodeAffinityPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "In operator must accept a node whose label is one of the requested values"
    );
}

/// [sig-scheduling] NodeAffinity required In operator rejects non-matching node
///
/// Upstream: predicates.go (NodeAffinity `should not be able to schedule when
/// no node matches the required affinity`).
#[tokio::test]
async fn node_affinity_required_in_operator_rejects_nonmatching_node() {
    let node = node_with(
        "node-az3",
        &[("kubernetes.io/e2e-az-name", "e2e-az3")],
        "4",
        "8Gi",
    );
    let pod = pod_with_affinity(
        "p",
        required_node_affinity(vec![NodeSelectorRequirement {
            key: "kubernetes.io/e2e-az-name".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["e2e-az1".to_string(), "e2e-az2".to_string()]),
        }]),
    );
    let plugin = NodeAffinityPlugin;
    assert!(
        !run_filter(&plugin, &pod, &node, vec![]).await,
        "In operator must reject a node whose label is not in the requested set"
    );
}

/// [sig-scheduling] NodeAffinity required Exists operator accepts labelled node
///
/// Upstream: predicates.go (NodeAffinity Exists / DoesNotExist semantics).
#[tokio::test]
async fn node_affinity_required_exists_accepts_labelled_node() {
    let node = node_with(
        "node-gpu",
        &[("hardware.example.com/gpu", "nvidia-a100")],
        "4",
        "8Gi",
    );
    let pod = pod_with_affinity(
        "p",
        required_node_affinity(vec![NodeSelectorRequirement {
            key: "hardware.example.com/gpu".to_string(),
            operator: "Exists".to_string(),
            values: None,
        }]),
    );
    let plugin = NodeAffinityPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "Exists operator must accept any node carrying the key"
    );
}

/// [sig-scheduling] NodeAffinity preferred raises score on matching node
///
/// Upstream: predicates.go (NodeAffinity preferredDuringSchedulingIgnoredDuringExecution
/// scoring path). Verifies the soft-affinity weight is added to the node score
/// when the term matches, leaving non-matching nodes at the baseline.
#[tokio::test]
async fn node_affinity_preferred_adds_weight_to_score() {
    let preferred_node = node_with("node-prefer", &[("disktype", "ssd")], "4", "8Gi");
    let plain_node = node_with("node-plain", &[("disktype", "hdd")], "4", "8Gi");
    let pod = pod_with_affinity(
        "p",
        preferred_node_affinity(
            42,
            vec![NodeSelectorRequirement {
                key: "disktype".to_string(),
                operator: "In".to_string(),
                values: Some(vec!["ssd".to_string()]),
            }],
        ),
    );
    let plugin = NodeAffinityScoringPlugin;
    let preferred_score = run_score(&plugin, &pod, &preferred_node, vec![]).await;
    let plain_score = run_score(&plugin, &pod, &plain_node, vec![]).await;
    assert_eq!(
        preferred_score, 42,
        "preferred affinity must add weight to matching node score"
    );
    assert_eq!(plain_score, 0, "non-matching node must score 0");
}

// ---------------------------------------------------------------------------
// PodAffinity / PodAntiAffinity — required + preferred
// ---------------------------------------------------------------------------

/// [sig-scheduling] PodAffinity required matches when a labelled pod is on a
/// node carrying the topology key
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/predicates.go (inter-pod
/// affinity suite). Verifies that pod affinity with a matching pod already
/// running grants the candidate node a pass.
#[tokio::test]
async fn pod_affinity_required_matches_when_target_pod_is_present() {
    let node = node_with(
        "node-zone-a",
        &[("topology.kubernetes.io/zone", "zone-a")],
        "4",
        "8Gi",
    );
    let target = pod_with_labels("target", Some("node-zone-a"), &[("app", "web")]);
    let pod = pod_with_affinity(
        "incoming",
        required_pod_affinity("topology.kubernetes.io/zone", &[("app", "web")]),
    );
    let plugin = PodAffinityPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![target]).await,
        "PodAffinity must allow nodes whose topology already hosts a matching pod"
    );
}

/// [sig-scheduling] PodAffinity required rejects nodes when no matching pod
/// has been placed yet
///
/// Upstream: predicates.go inter-pod affinity (negative path). The scheduler
/// must filter the node out when no peer pod matches the label selector.
#[tokio::test]
async fn pod_affinity_required_rejects_when_no_target_pod() {
    let node = node_with(
        "node-zone-b",
        &[("topology.kubernetes.io/zone", "zone-b")],
        "4",
        "8Gi",
    );
    let pod = pod_with_affinity(
        "incoming",
        required_pod_affinity("topology.kubernetes.io/zone", &[("app", "web")]),
    );
    let plugin = PodAffinityPlugin;
    assert!(
        !run_filter(&plugin, &pod, &node, vec![]).await,
        "PodAffinity must reject when no peer pod matches the selector"
    );
}

/// [sig-scheduling] PodAntiAffinity required rejects nodes with conflicting pod
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/predicates.go (anti-affinity
/// suite). A pod with required anti-affinity must not co-locate with a pod
/// matching its selector on the same topology key.
#[tokio::test]
async fn pod_anti_affinity_required_rejects_conflicting_node() {
    let node = node_with(
        "node-zone-c",
        &[("topology.kubernetes.io/zone", "zone-c")],
        "4",
        "8Gi",
    );
    let conflict = pod_with_labels("rival", Some("node-zone-c"), &[("app", "web")]);
    let pod = pod_with_affinity(
        "incoming",
        required_pod_anti_affinity("topology.kubernetes.io/zone", &[("app", "web")]),
    );
    let plugin = PodAntiAffinityPlugin;
    assert!(
        !run_filter(&plugin, &pod, &node, vec![conflict]).await,
        "PodAntiAffinity must reject a node already hosting a matching pod in the same topology"
    );
}

/// [sig-scheduling] PodAffinity preferred contributes positive score
///
/// Upstream: predicates.go inter-pod affinity preferred path. The framework
/// returns a positive score from `check_pod_affinity` proportional to the
/// matching weighted terms.
#[tokio::test]
async fn pod_affinity_preferred_returns_positive_score() {
    let node = node_with(
        "node-zone-d",
        &[("topology.kubernetes.io/zone", "zone-d")],
        "4",
        "8Gi",
    );
    let target = pod_with_labels("target", Some("node-zone-d"), &[("app", "cache")]);
    let pod = pod_with_affinity(
        "incoming",
        preferred_pod_affinity(30, "topology.kubernetes.io/zone", &[("app", "cache")]),
    );

    let (passes, score) = check_pod_affinity(&node, &pod, &[target], std::slice::from_ref(&node));
    assert!(passes, "no required term so the node must remain feasible");
    assert_eq!(
        score, 30,
        "preferred pod-affinity must contribute its weight to the score"
    );
}

// ---------------------------------------------------------------------------
// Taints + Tolerations — NoSchedule / PreferNoSchedule / NoExecute
// ---------------------------------------------------------------------------

/// [sig-scheduling] NoSchedule taint repels pods without a matching toleration
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/taints.go (the canonical
/// NoSchedule effect test in the suite).
#[tokio::test]
async fn taint_noschedule_repels_pod_without_toleration() {
    let node = node_tainted(
        "tainted",
        vec![Taint {
            key: "dedicated".to_string(),
            value: Some("gpu".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }],
    );
    let pod = empty_pod("p");
    let plugin = TaintTolerationPlugin;
    assert!(
        !run_filter(&plugin, &pod, &node, vec![]).await,
        "NoSchedule taint must filter out pods that lack a matching toleration"
    );
}

/// [sig-scheduling] NoSchedule taint accepts pod with matching toleration
///
/// Upstream: taints.go (positive path for NoSchedule). The toleration with
/// matching key, value, and effect lets the pod schedule.
#[tokio::test]
async fn taint_noschedule_accepts_pod_with_matching_toleration() {
    let node = node_tainted(
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
            operator: Some("Equal".to_string()),
            value: Some("gpu".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }],
    );
    let plugin = TaintTolerationPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "matching toleration must permit scheduling onto the tainted node"
    );
}

/// [sig-scheduling] PreferNoSchedule taint is treated as a soft constraint
///
/// Upstream: taints.go (PreferNoSchedule semantics). PreferNoSchedule is a
/// soft hint — `check_taints_tolerations` must always return `true` because
/// the scheduler honours the preference only via scoring, never filtering.
#[tokio::test]
async fn taint_prefer_no_schedule_does_not_reject_pod() {
    let node = node_tainted(
        "soft-tainted",
        vec![Taint {
            key: "preferred".to_string(),
            value: Some("nope".to_string()),
            effect: "PreferNoSchedule".to_string(),
            time_added: None,
        }],
    );
    let pod = empty_pod("p");
    assert!(
        check_taints_tolerations(&node, &pod),
        "PreferNoSchedule is a soft constraint and must not filter pods"
    );
}

/// [sig-scheduling] NoExecute taint without toleration filters pod
///
/// Upstream: taints.go (`Pods are evicted from nodes after a NoExecute taint
/// is added`). The scheduler-side concern: `NoExecute` behaves like
/// `NoSchedule` at admission — pods lacking a matching toleration are
/// rejected.
#[tokio::test]
async fn taint_noexecute_repels_pod_without_toleration() {
    let node = node_tainted(
        "evict",
        vec![Taint {
            key: "node.kubernetes.io/not-ready".to_string(),
            value: None,
            effect: "NoExecute".to_string(),
            time_added: None,
        }],
    );
    let pod = empty_pod("p");
    let plugin = TaintTolerationPlugin;
    assert!(
        !run_filter(&plugin, &pod, &node, vec![]).await,
        "NoExecute taint must filter out pods that lack a matching toleration"
    );
}

/// [sig-scheduling] NoExecute taint accepts pod with Exists toleration
///
/// Upstream: taints.go (positive NoExecute path with a wildcard toleration).
/// Verifies the `Exists` operator matches the taint key regardless of value.
#[tokio::test]
async fn taint_noexecute_accepts_pod_with_exists_toleration() {
    let node = node_tainted(
        "evict",
        vec![Taint {
            key: "node.kubernetes.io/unreachable".to_string(),
            value: None,
            effect: "NoExecute".to_string(),
            time_added: None,
        }],
    );
    let pod = pod_with_tolerations(
        "p",
        vec![Toleration {
            key: Some("node.kubernetes.io/unreachable".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoExecute".to_string()),
            toleration_seconds: Some(60),
        }],
    );
    let plugin = TaintTolerationPlugin;
    assert!(
        run_filter(&plugin, &pod, &node, vec![]).await,
        "Exists toleration must permit scheduling onto a NoExecute-tainted node"
    );
}

// ---------------------------------------------------------------------------
// Resource-fit / score sanity checks (priorities.go-flavoured)
// ---------------------------------------------------------------------------

/// [sig-scheduling] NodeResourcesFit score subtracts running pod requests
///
/// Upstream: priorities.go (NodeResourcesFit / LeastAllocated balancing). When
/// pods are already scheduled on a node their CPU/memory requests must be
/// deducted from allocatable when scoring further candidates.
#[tokio::test]
async fn node_resources_fit_accounts_for_running_pod_requests() {
    let node = node_with("node-1", &[], "4", "8Gi");
    let busy = pod_running_on("busy", "node-1", Some("3000m"));
    let small = pod_with_cpu_request("small", "500m");
    let oversized = pod_with_cpu_request("oversized", "1500m");

    // small (0.5 CPU) fits in the 1 CPU left, oversized (1.5 CPU) does not.
    let busy_slice = std::slice::from_ref(&busy);
    let small_score = calculate_resource_score_with_pods(&node, &small, busy_slice);
    let oversized_score = calculate_resource_score_with_pods(&node, &oversized, busy_slice);
    assert!(
        small_score > 0,
        "small pod must remain feasible after busy pod's 3000m is deducted"
    );
    assert_eq!(
        oversized_score, 0,
        "oversized pod must be infeasible once busy pod's requests are deducted"
    );
}
