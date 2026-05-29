//! Table-driven unit tests for the pure predicate functions in
//! `crates/scheduler/src/advanced.rs`.
//!
//! Upstream Go reference tests used to derive cases:
//! - <https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/framework/plugins/tainttoleration/taint_toleration_test.go>
//! - <https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/framework/plugins/nodeaffinity/node_affinity_test.go>
//! - <https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/framework/plugins/interpodaffinity/filtering_test.go>
//! - <https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/framework/plugins/interpodaffinity/scoring_test.go>

use rusternetes_common::resources::{
    node::Taint,
    pod::{
        Affinity, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
        PodAffinity, PodAffinityTerm, PodAntiAffinity, PodSpec, PreferredSchedulingTerm,
        Toleration, WeightedPodAffinityTerm,
    },
    Node, Pod,
};
use rusternetes_common::types::{LabelSelector, LabelSelectorRequirement, ObjectMeta, TypeMeta};
use rusternetes_scheduler::advanced::{
    check_node_affinity, check_pod_affinity, check_pod_anti_affinity, check_taints_tolerations,
};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// In-file builder helpers
// ---------------------------------------------------------------------------

/// Create a bare Node with given name (no labels, no taints, no status).
fn node(name: &str) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name),
        spec: None,
        status: None,
    }
}

/// Create a Node and attach the supplied labels to it.
fn node_with_labels(name: &str, labels: &[(&str, &str)]) -> Node {
    let mut n = node(name);
    let mut map = HashMap::new();
    for (k, v) in labels {
        map.insert(k.to_string(), v.to_string());
    }
    n.metadata.labels = Some(map);
    n
}

/// Create a Node with taints and (optionally) labels.
fn node_with_taints(name: &str, labels: &[(&str, &str)], taints: Vec<Taint>) -> Node {
    use rusternetes_common::resources::node::NodeSpec;
    let mut n = node_with_labels(name, labels);
    n.spec = Some(NodeSpec {
        pod_cidr: None,
        pod_cidrs: None,
        provider_id: None,
        taints: Some(taints),
        unschedulable: None,
    });
    n
}

/// Build a Taint.
fn taint(key: &str, value: Option<&str>, effect: &str) -> Taint {
    Taint {
        key: key.to_string(),
        value: value.map(|v| v.to_string()),
        effect: effect.to_string(),
        time_added: None,
    }
}

/// Build a minimal Pod with empty spec (no tolerations, no affinity).
fn pod_bare(name: &str) -> Pod {
    let spec = PodSpec {
        containers: vec![],
        ..Default::default()
    };
    Pod::new(name, spec)
}

/// Build a Pod that carries the given tolerations.
fn pod_with_tolerations(name: &str, tolerations: Vec<Toleration>) -> Pod {
    let spec = PodSpec {
        containers: vec![],
        tolerations: Some(tolerations),
        ..Default::default()
    };
    Pod::new(name, spec)
}

/// Build a toleration with Equal operator (key + value required).
fn toleration_equal(key: &str, value: &str, effect: Option<&str>) -> Toleration {
    Toleration {
        key: Some(key.to_string()),
        operator: Some("Equal".to_string()),
        value: Some(value.to_string()),
        effect: effect.map(|e| e.to_string()),
        toleration_seconds: None,
    }
}

/// Build an Exists toleration (matches any value for the key).
fn toleration_exists(key: Option<&str>, effect: Option<&str>) -> Toleration {
    Toleration {
        key: key.map(|k| k.to_string()),
        operator: Some("Exists".to_string()),
        value: None,
        effect: effect.map(|e| e.to_string()),
        toleration_seconds: None,
    }
}

/// Build a Pod whose affinity contains a required NodeAffinity selector.
fn pod_with_required_node_affinity(name: &str, terms: Vec<NodeSelectorTerm>) -> Pod {
    let affinity = Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: terms,
            }),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
        pod_affinity: None,
        pod_anti_affinity: None,
    };
    let spec = PodSpec {
        containers: vec![],
        affinity: Some(affinity),
        ..Default::default()
    };
    Pod::new(name, spec)
}

/// Build a Pod whose affinity contains preferred NodeAffinity terms.
fn pod_with_preferred_node_affinity(name: &str, terms: Vec<PreferredSchedulingTerm>) -> Pod {
    let affinity = Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: None,
            preferred_during_scheduling_ignored_during_execution: Some(terms),
        }),
        pod_affinity: None,
        pod_anti_affinity: None,
    };
    let spec = PodSpec {
        containers: vec![],
        affinity: Some(affinity),
        ..Default::default()
    };
    Pod::new(name, spec)
}

/// Build a NodeSelectorTerm from a single match-expression.
fn nst_expr(key: &str, op: &str, values: &[&str]) -> NodeSelectorTerm {
    NodeSelectorTerm {
        match_expressions: Some(vec![NodeSelectorRequirement {
            key: key.to_string(),
            operator: op.to_string(),
            values: if values.is_empty() {
                None
            } else {
                Some(values.iter().map(|v| v.to_string()).collect())
            },
        }]),
        match_fields: None,
    }
}

/// Build a NodeSelectorTerm from a single match-field requirement.
fn nst_field(key: &str, op: &str, values: &[&str]) -> NodeSelectorTerm {
    NodeSelectorTerm {
        match_expressions: None,
        match_fields: Some(vec![NodeSelectorRequirement {
            key: key.to_string(),
            operator: op.to_string(),
            values: if values.is_empty() {
                None
            } else {
                Some(values.iter().map(|v| v.to_string()).collect())
            },
        }]),
    }
}

/// Build a LabelSelector that matches all pods with the given labels map.
fn label_sel(labels: &[(&str, &str)]) -> LabelSelector {
    let mut map = HashMap::new();
    for (k, v) in labels {
        map.insert(k.to_string(), v.to_string());
    }
    LabelSelector {
        match_labels: Some(map),
        match_expressions: None,
    }
}

/// Build a LabelSelector using matchExpressions.
fn label_sel_expr(key: &str, op: &str, values: &[&str]) -> LabelSelector {
    LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: key.to_string(),
            operator: op.to_string(),
            values: if values.is_empty() {
                None
            } else {
                Some(values.iter().map(|v| v.to_string()).collect())
            },
        }]),
    }
}

/// Build a scheduled Pod (has spec.node_name set) with optional labels.
fn scheduled_pod(name: &str, node_name: &str, labels: &[(&str, &str)]) -> Pod {
    let spec = PodSpec {
        containers: vec![],
        node_name: Some(node_name.to_string()),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    if !labels.is_empty() {
        let mut map = HashMap::new();
        for (k, v) in labels {
            map.insert(k.to_string(), v.to_string());
        }
        pod.metadata.labels = Some(map);
    }
    pod
}

/// Build a Pod with required pod-affinity.
fn pod_with_required_pod_affinity(
    name: &str,
    selector: LabelSelector,
    topology_key: &str,
    namespaces: Option<Vec<String>>,
) -> Pod {
    let affinity = Affinity {
        node_affinity: None,
        pod_affinity: Some(PodAffinity {
            required_during_scheduling_ignored_during_execution: Some(vec![PodAffinityTerm {
                label_selector: selector,
                namespaces,
                topology_key: topology_key.to_string(),
            }]),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
        pod_anti_affinity: None,
    };
    let spec = PodSpec {
        containers: vec![],
        affinity: Some(affinity),
        ..Default::default()
    };
    Pod::new(name, spec)
}

/// Build a Pod with preferred pod-affinity (weighted).
fn pod_with_preferred_pod_affinity(
    name: &str,
    weight: i32,
    selector: LabelSelector,
    topology_key: &str,
) -> Pod {
    let affinity = Affinity {
        node_affinity: None,
        pod_affinity: Some(PodAffinity {
            required_during_scheduling_ignored_during_execution: None,
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                WeightedPodAffinityTerm {
                    weight,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: selector,
                        namespaces: None,
                        topology_key: topology_key.to_string(),
                    },
                },
            ]),
        }),
        pod_anti_affinity: None,
    };
    let spec = PodSpec {
        containers: vec![],
        affinity: Some(affinity),
        ..Default::default()
    };
    Pod::new(name, spec)
}

/// Build a Pod with required pod-anti-affinity.
fn pod_with_required_anti_affinity(name: &str, selector: LabelSelector, topology_key: &str) -> Pod {
    let affinity = Affinity {
        node_affinity: None,
        pod_affinity: None,
        pod_anti_affinity: Some(PodAntiAffinity {
            required_during_scheduling_ignored_during_execution: Some(vec![PodAffinityTerm {
                label_selector: selector,
                namespaces: None,
                topology_key: topology_key.to_string(),
            }]),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
    };
    let spec = PodSpec {
        containers: vec![],
        affinity: Some(affinity),
        ..Default::default()
    };
    Pod::new(name, spec)
}

/// Build a Pod with preferred pod-anti-affinity (weighted).
fn pod_with_preferred_anti_affinity(
    name: &str,
    weight: i32,
    selector: LabelSelector,
    topology_key: &str,
) -> Pod {
    let affinity = Affinity {
        node_affinity: None,
        pod_affinity: None,
        pod_anti_affinity: Some(PodAntiAffinity {
            required_during_scheduling_ignored_during_execution: None,
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                WeightedPodAffinityTerm {
                    weight,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: selector,
                        namespaces: None,
                        topology_key: topology_key.to_string(),
                    },
                },
            ]),
        }),
    };
    let spec = PodSpec {
        containers: vec![],
        affinity: Some(affinity),
        ..Default::default()
    };
    Pod::new(name, spec)
}

// ===========================================================================
// check_taints_tolerations — taint/toleration predicate
// ===========================================================================

/// Upstream ref: taint_toleration_test.go — "pod schedules onto taint-free node"
#[test]
fn taints_no_taints_on_node_always_passes() {
    let node = node("n1");
    let pod = pod_bare("p1");
    assert!(check_taints_tolerations(&node, &pod));
}

/// Upstream ref: taint_toleration_test.go — "pod has no spec (bare pod tolerates all)"
#[test]
fn taints_node_no_spec_passes() {
    // node has no spec at all → treated as no taints
    let pod = pod_bare("p1");
    let n = node("n");
    assert!(check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "pod does not tolerate NoSchedule taint"
#[test]
fn taints_no_toleration_noschedule_fails() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![taint("dedicated", Some("gpu"), "NoSchedule")],
    );
    let pod = pod_bare("p1");
    assert!(!check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "pod does not tolerate NoExecute taint"
#[test]
fn taints_no_toleration_noexecute_fails() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![taint("node.kubernetes.io/not-ready", None, "NoExecute")],
    );
    let pod = pod_bare("p1");
    assert!(!check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "PreferNoSchedule is always tolerated (soft)"
#[test]
fn taints_prefer_no_schedule_is_soft_always_passes() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![taint("disktype", Some("hdd"), "PreferNoSchedule")],
    );
    let pod = pod_bare("p1");
    // PreferNoSchedule is a soft constraint and never blocks hard scheduling
    assert!(check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "matching Equal toleration allows scheduling"
#[test]
fn taints_equal_toleration_matches() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![taint("dedicated", Some("gpu"), "NoSchedule")],
    );
    let pod = pod_with_tolerations(
        "p1",
        vec![toleration_equal("dedicated", "gpu", Some("NoSchedule"))],
    );
    assert!(check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "Equal toleration with wrong value does not match"
#[test]
fn taints_equal_toleration_wrong_value_fails() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![taint("dedicated", Some("gpu"), "NoSchedule")],
    );
    let pod = pod_with_tolerations(
        "p1",
        vec![toleration_equal("dedicated", "cpu", Some("NoSchedule"))],
    );
    assert!(!check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "Exists toleration with matching key"
#[test]
fn taints_exists_toleration_matching_key() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![taint("dedicated", Some("anything"), "NoSchedule")],
    );
    let pod = pod_with_tolerations("p1", vec![toleration_exists(Some("dedicated"), None)]);
    assert!(check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "wildcard Exists toleration (empty key) tolerates all"
#[test]
fn taints_exists_wildcard_toleration_tolerates_all() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![
            taint("key1", Some("v1"), "NoSchedule"),
            taint("key2", None, "NoExecute"),
        ],
    );
    // key=None with operator=Exists → matches any taint key
    let pod = pod_with_tolerations("p1", vec![toleration_exists(None, None)]);
    assert!(check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "effect-specific toleration only matches that effect"
#[test]
fn taints_effect_specific_toleration() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![
            taint("key1", Some("v1"), "NoSchedule"),
            taint("key1", Some("v1"), "NoExecute"),
        ],
    );
    // Toleration only covers NoSchedule; the NoExecute taint is not tolerated
    let pod = pod_with_tolerations(
        "p1",
        vec![toleration_equal("key1", "v1", Some("NoSchedule"))],
    );
    assert!(!check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "toleration with no effect matches any effect"
#[test]
fn taints_toleration_no_effect_matches_all_effects() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![
            taint("key1", Some("v1"), "NoSchedule"),
            taint("key1", Some("v1"), "NoExecute"),
        ],
    );
    // Toleration has no effect specified → matches any effect
    let pod = pod_with_tolerations("p1", vec![toleration_equal("key1", "v1", None)]);
    assert!(check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "multiple taints, all tolerated"
#[test]
fn taints_multiple_taints_all_tolerated() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![
            taint("key1", Some("v1"), "NoSchedule"),
            taint("key2", None, "NoSchedule"),
        ],
    );
    let pod = pod_with_tolerations(
        "p1",
        vec![
            toleration_equal("key1", "v1", Some("NoSchedule")),
            toleration_exists(Some("key2"), Some("NoSchedule")),
        ],
    );
    assert!(check_taints_tolerations(&n, &pod));
}

/// Upstream ref: taint_toleration_test.go — "multiple taints, one untolerated blocks scheduling"
#[test]
fn taints_multiple_taints_one_untolerated_fails() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![
            taint("key1", Some("v1"), "NoSchedule"),
            taint("key2", Some("v2"), "NoSchedule"),
        ],
    );
    let pod = pod_with_tolerations(
        "p1",
        vec![toleration_equal("key1", "v1", Some("NoSchedule"))],
    );
    assert!(!check_taints_tolerations(&n, &pod));
}

// ===========================================================================
// check_node_affinity — node-affinity predicate + scoring
// ===========================================================================

/// Upstream ref: node_affinity_test.go — "no affinity: always passes with score 0"
#[test]
fn node_affinity_no_affinity_passes_with_zero_score() {
    let n = node_with_labels("n1", &[("zone", "us-east-1a")]);
    let pod = pod_bare("p1");
    assert_eq!(check_node_affinity(&n, &pod), (true, 0));
}

/// Upstream ref: node_affinity_test.go — "required In: node has matching label"
#[test]
fn node_affinity_required_in_matches() {
    let n = node_with_labels("n1", &[("zone", "us-east-1a")]);
    let pod = pod_with_required_node_affinity(
        "p1",
        vec![nst_expr("zone", "In", &["us-east-1a", "us-east-1b"])],
    );
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(passes);
}

/// Upstream ref: node_affinity_test.go — "required In: node does NOT have matching label"
#[test]
fn node_affinity_required_in_no_match_fails() {
    let n = node_with_labels("n1", &[("zone", "eu-west-1")]);
    let pod = pod_with_required_node_affinity(
        "p1",
        vec![nst_expr("zone", "In", &["us-east-1a", "us-east-1b"])],
    );
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(!passes);
}

/// Upstream ref: node_affinity_test.go — "required NotIn: label absent → passes"
#[test]
fn node_affinity_required_notin_label_absent_passes() {
    let n = node_with_labels("n1", &[("other", "val")]);
    let pod =
        pod_with_required_node_affinity("p1", vec![nst_expr("env", "NotIn", &["production"])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(passes);
}

/// Upstream ref: node_affinity_test.go — "required NotIn: label present with forbidden value fails"
#[test]
fn node_affinity_required_notin_forbidden_value_fails() {
    let n = node_with_labels("n1", &[("env", "production")]);
    let pod =
        pod_with_required_node_affinity("p1", vec![nst_expr("env", "NotIn", &["production"])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(!passes);
}

/// Upstream ref: node_affinity_test.go — "required Exists: key present passes"
#[test]
fn node_affinity_required_exists_key_present_passes() {
    let n = node_with_labels("n1", &[("tier", "frontend")]);
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("tier", "Exists", &[])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(passes);
}

/// Upstream ref: node_affinity_test.go — "required Exists: key absent fails"
#[test]
fn node_affinity_required_exists_key_absent_fails() {
    let n = node_with_labels("n1", &[("other", "x")]);
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("tier", "Exists", &[])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(!passes);
}

/// Upstream ref: node_affinity_test.go — "required DoesNotExist: key absent passes"
#[test]
fn node_affinity_required_doesnotexist_key_absent_passes() {
    let n = node_with_labels("n1", &[("other", "x")]);
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("tier", "DoesNotExist", &[])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(passes);
}

/// Upstream ref: node_affinity_test.go — "required DoesNotExist: key present fails"
#[test]
fn node_affinity_required_doesnotexist_key_present_fails() {
    let n = node_with_labels("n1", &[("tier", "frontend")]);
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("tier", "DoesNotExist", &[])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(!passes);
}

/// Upstream ref: node_affinity_test.go — "required Gt: node value greater passes"
#[test]
fn node_affinity_required_gt_passes() {
    let n = node_with_labels("n1", &[("storage-gb", "200")]);
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("storage-gb", "Gt", &["100"])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(passes);
}

/// Upstream ref: node_affinity_test.go — "required Gt: node value equal (not strictly greater) fails"
#[test]
fn node_affinity_required_gt_equal_fails() {
    let n = node_with_labels("n1", &[("storage-gb", "100")]);
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("storage-gb", "Gt", &["100"])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(!passes);
}

/// Upstream ref: node_affinity_test.go — "required Lt: node value less passes"
#[test]
fn node_affinity_required_lt_passes() {
    let n = node_with_labels("n1", &[("cpu-count", "4")]);
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("cpu-count", "Lt", &["8"])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(passes);
}

/// Upstream ref: node_affinity_test.go — "required Lt: node value equal (not strictly less) fails"
#[test]
fn node_affinity_required_lt_equal_fails() {
    let n = node_with_labels("n1", &[("cpu-count", "8")]);
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("cpu-count", "Lt", &["8"])]);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(!passes);
}

/// Upstream ref: node_affinity_test.go — "matchFields: metadata.name matches"
#[test]
fn node_affinity_required_match_fields_name_matches() {
    let n = node("specific-node");
    let pod = pod_with_required_node_affinity(
        "p1",
        vec![nst_field("metadata.name", "In", &["specific-node"])],
    );
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(passes);
}

/// Upstream ref: node_affinity_test.go — "matchFields: metadata.name does not match"
#[test]
fn node_affinity_required_match_fields_name_no_match_fails() {
    let n = node("other-node");
    let pod = pod_with_required_node_affinity(
        "p1",
        vec![nst_field("metadata.name", "In", &["specific-node"])],
    );
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(!passes);
}

/// Upstream ref: node_affinity_test.go — "OR logic: at least one term matching passes"
#[test]
fn node_affinity_required_or_logic_first_term_matches() {
    let n = node_with_labels("n1", &[("zone", "us-east-1a")]);
    let pod = pod_with_required_node_affinity(
        "p1",
        vec![
            nst_expr("zone", "In", &["us-east-1a"]),
            nst_expr("zone", "In", &["us-west-2a"]),
        ],
    );
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(passes);
}

/// Upstream ref: node_affinity_test.go — "OR logic: only the SECOND term matches".
/// Guards against an OR→AND regression: the first term fails, the second matches,
/// and the node must still pass because terms are ORed.
#[test]
fn node_affinity_required_or_logic_second_term_matches() {
    let n = node_with_labels("n1", &[("zone", "us-west-2a")]);
    let pod = pod_with_required_node_affinity(
        "p1",
        vec![
            // First term does NOT match (node is not us-east-1a) ...
            nst_expr("zone", "In", &["us-east-1a"]),
            // ... but the second term does.
            nst_expr("zone", "In", &["us-west-2a"]),
        ],
    );
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(
        passes,
        "OR of node selector terms must pass when only the second term matches"
    );
}

/// Upstream ref: node_affinity_test.go — "OR logic: neither term matches fails".
#[test]
fn node_affinity_required_or_logic_no_term_matches_fails() {
    let n = node_with_labels("n1", &[("zone", "eu-central-1")]);
    let pod = pod_with_required_node_affinity(
        "p1",
        vec![
            nst_expr("zone", "In", &["us-east-1a"]),
            nst_expr("zone", "In", &["us-west-2a"]),
        ],
    );
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(
        !passes,
        "OR of node selector terms must fail when none match"
    );
}

/// Upstream ref: node_affinity_test.go — "preferred In: matching term contributes weight"
#[test]
fn node_affinity_preferred_in_matches_score_accumulated() {
    let n = node_with_labels("n1", &[("instance-type", "m5.large")]);
    let pod = pod_with_preferred_node_affinity(
        "p1",
        vec![PreferredSchedulingTerm {
            weight: 50,
            preference: nst_expr("instance-type", "In", &["m5.large"]),
        }],
    );
    let (passes, score) = check_node_affinity(&n, &pod);
    assert!(passes);
    assert_eq!(score, 50);
}

/// Upstream ref: node_affinity_test.go — "preferred: non-matching term contributes 0"
#[test]
fn node_affinity_preferred_no_match_score_zero() {
    let n = node_with_labels("n1", &[("instance-type", "c5.xlarge")]);
    let pod = pod_with_preferred_node_affinity(
        "p1",
        vec![PreferredSchedulingTerm {
            weight: 50,
            preference: nst_expr("instance-type", "In", &["m5.large"]),
        }],
    );
    let (passes, score) = check_node_affinity(&n, &pod);
    assert!(passes); // preferred is soft, always passes
    assert_eq!(score, 0);
}

/// Upstream ref: node_affinity_test.go — "multiple preferred terms: weights accumulate"
#[test]
fn node_affinity_preferred_multiple_terms_weights_accumulate() {
    let n = node_with_labels(
        "n1",
        &[("zone", "us-east-1a"), ("instance-type", "m5.large")],
    );
    let pod = pod_with_preferred_node_affinity(
        "p1",
        vec![
            PreferredSchedulingTerm {
                weight: 30,
                preference: nst_expr("zone", "In", &["us-east-1a"]),
            },
            PreferredSchedulingTerm {
                weight: 70,
                preference: nst_expr("instance-type", "In", &["m5.large"]),
            },
        ],
    );
    let (passes, score) = check_node_affinity(&n, &pod);
    assert!(passes);
    assert_eq!(score, 100);
}

/// Upstream ref: node_affinity_test.go — "required + preferred: required must pass"
#[test]
fn node_affinity_required_fails_preferred_irrelevant() {
    let n = node_with_labels("n1", &[("zone", "eu-west-1")]);
    // Required: must be us-east-1a (fails); Preferred: zone in eu-west-1 (would score)
    let affinity = Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: vec![nst_expr("zone", "In", &["us-east-1a"])],
            }),
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                PreferredSchedulingTerm {
                    weight: 100,
                    preference: nst_expr("zone", "In", &["eu-west-1"]),
                },
            ]),
        }),
        pod_affinity: None,
        pod_anti_affinity: None,
    };
    let spec = PodSpec {
        containers: vec![],
        affinity: Some(affinity),
        ..Default::default()
    };
    let pod = Pod::new("p1", spec);
    let (passes, _) = check_node_affinity(&n, &pod);
    assert!(!passes, "Required affinity failure should block scheduling");
}

// ===========================================================================
// check_pod_affinity — inter-pod affinity predicate + scoring
// ===========================================================================

/// Upstream ref: filtering_test.go — "no affinity on pod: always passes"
#[test]
fn pod_affinity_no_affinity_passes() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_bare("p1");
    assert_eq!(check_pod_affinity(&n, &pod, &[]), (true, 0));
}

/// Upstream ref: filtering_test.go — "required affinity: node missing topology key fails"
#[test]
fn pod_affinity_required_node_missing_topology_key_fails() {
    // Node has NO topology label at all
    let n = node("n1");
    let pod = pod_with_required_pod_affinity(
        "p1",
        label_sel(&[("app", "cache")]),
        "kubernetes.io/hostname",
        None,
    );
    let cache_pod = scheduled_pod("cache", "n1", &[("app", "cache")]);
    let (passes, _) = check_pod_affinity(&n, &pod, &[cache_pod]);
    assert!(!passes);
}

/// Upstream ref: filtering_test.go — "required affinity: a matching scheduled pod exists".
///
/// NOTE: this verifies only that a matching, already-scheduled pod is found — it
/// does NOT verify topology-key correctness. Rusternetes' `matches_pod_affinity_term`
/// (advanced.rs ~395-422) does not compare the matching pod's topology domain to
/// the candidate node's, so this test would pass identically even if `cache` were
/// on a completely different node. The topology-correct behaviour is pinned by the
/// `#[ignore]` GAP test below.
#[test]
fn pod_affinity_required_matching_scheduled_pod_found() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_pod_affinity(
        "incoming",
        label_sel(&[("app", "cache")]),
        "kubernetes.io/hostname",
        None,
    );
    let cache_pod = scheduled_pod("cache", "n1", &[("app", "cache")]);
    let (passes, _) = check_pod_affinity(&n, &pod, &[cache_pod]);
    assert!(passes);
}

/// Topology-CORRECT expectation for pod affinity: a matching pod in a DIFFERENT
/// topology domain (different hostname) must NOT satisfy hostname-scoped affinity,
/// so scheduling onto n1 (which has no co-located matching pod) should FAIL.
/// Rusternetes currently treats it as satisfied because it ignores topologyKey.
#[test]
#[ignore = "GAP: pod (anti-)affinity not topology-aware; advanced.rs matches_pod_affinity_term ignores the matching pod's node/topologyKey"]
fn pod_affinity_required_matching_pod_different_topology_should_fail() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_pod_affinity(
        "incoming",
        label_sel(&[("app", "cache")]),
        "kubernetes.io/hostname",
        None,
    );
    // The only matching pod is in the n2 topology domain, not n1's.
    let cache_pod = scheduled_pod("cache", "n2", &[("app", "cache")]);
    let (passes, _) = check_pod_affinity(&n, &pod, &[cache_pod]);
    assert!(
        !passes,
        "hostname-scoped affinity must not be satisfied by a pod in a different topology domain"
    );
}

/// Upstream ref: filtering_test.go — "required affinity: no matching pods in cluster fails"
#[test]
fn pod_affinity_required_no_matching_pods_fails() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_pod_affinity(
        "incoming",
        label_sel(&[("app", "cache")]),
        "kubernetes.io/hostname",
        None,
    );
    // No existing pods in the cluster
    let (passes, _) = check_pod_affinity(&n, &pod, &[]);
    assert!(!passes);
}

/// Upstream ref: filtering_test.go — "required affinity: pod only scheduled if node_name set"
#[test]
fn pod_affinity_required_unscheduled_pod_not_counted() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_pod_affinity(
        "incoming",
        label_sel(&[("app", "cache")]),
        "kubernetes.io/hostname",
        None,
    );
    // Matching pod exists but is NOT scheduled (no node_name)
    let unscheduled = pod_bare("cache");
    let (passes, _) = check_pod_affinity(&n, &pod, &[unscheduled]);
    assert!(!passes);
}

/// Upstream ref: filtering_test.go — "required affinity: namespace filtering respected"
#[test]
fn pod_affinity_required_namespace_filtering() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    // The affinity only allows namespace "ns-a"
    let pod = pod_with_required_pod_affinity(
        "incoming",
        label_sel(&[("app", "cache")]),
        "kubernetes.io/hostname",
        Some(vec!["ns-a".to_string()]),
    );
    // Matching pod is in "ns-b" — should not count
    let mut cache_pod = scheduled_pod("cache", "n1", &[("app", "cache")]);
    cache_pod.metadata.namespace = Some("ns-b".to_string());
    let (passes, _) = check_pod_affinity(&n, &pod, &[cache_pod]);
    assert!(!passes);
}

/// Upstream ref: filtering_test.go — "required affinity with matchExpressions In"
#[test]
fn pod_affinity_required_match_expressions_in() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_pod_affinity(
        "incoming",
        label_sel_expr("app", "In", &["cache", "db"]),
        "kubernetes.io/hostname",
        None,
    );
    let cache_pod = scheduled_pod("cache", "n1", &[("app", "cache")]);
    let (passes, _) = check_pod_affinity(&n, &pod, &[cache_pod]);
    assert!(passes);
}

/// Upstream ref: scoring_test.go — "preferred affinity: matching pod adds weight to score"
#[test]
fn pod_affinity_preferred_matching_pod_scores() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_preferred_pod_affinity(
        "incoming",
        80,
        label_sel(&[("tier", "backend")]),
        "kubernetes.io/hostname",
    );
    let backend_pod = scheduled_pod("backend", "n1", &[("tier", "backend")]);
    let (passes, score) = check_pod_affinity(&n, &pod, &[backend_pod]);
    assert!(passes);
    assert_eq!(score, 80);
}

/// Upstream ref: scoring_test.go — "preferred affinity: no matching pod scores 0"
#[test]
fn pod_affinity_preferred_no_match_scores_zero() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_preferred_pod_affinity(
        "incoming",
        80,
        label_sel(&[("tier", "backend")]),
        "kubernetes.io/hostname",
    );
    // No existing backend pods
    let (passes, score) = check_pod_affinity(&n, &pod, &[]);
    assert!(passes);
    assert_eq!(score, 0);
}

// ===========================================================================
// check_pod_anti_affinity — inter-pod anti-affinity predicate + scoring
// ===========================================================================

/// Upstream ref: filtering_test.go — "no anti-affinity: always passes"
#[test]
fn pod_anti_affinity_no_anti_affinity_passes() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_bare("p1");
    assert_eq!(check_pod_anti_affinity(&n, &pod, &[]), (true, 0));
}

/// Upstream ref: filtering_test.go — "required anti-affinity: matching pod on node fails"
#[test]
fn pod_anti_affinity_required_matching_pod_blocks() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_anti_affinity(
        "incoming",
        label_sel(&[("app", "web")]),
        "kubernetes.io/hostname",
    );
    let web_pod = scheduled_pod("web-1", "n1", &[("app", "web")]);
    let (passes, _) = check_pod_anti_affinity(&n, &pod, &[web_pod]);
    assert!(!passes);
}

/// Upstream ref: filtering_test.go — "required anti-affinity: no matching pods passes"
#[test]
fn pod_anti_affinity_required_no_matching_pods_passes() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_anti_affinity(
        "incoming",
        label_sel(&[("app", "web")]),
        "kubernetes.io/hostname",
    );
    // No existing web pods
    let (passes, _) = check_pod_anti_affinity(&n, &pod, &[]);
    assert!(passes);
}

/// Upstream ref: filtering_test.go — "required anti-affinity: node has no topology key passes"
#[test]
fn pod_anti_affinity_required_node_missing_topology_key_passes() {
    // Node does NOT have the topology label → cannot find conflicting pod on same topology
    let n = node("n1-no-labels");
    let pod = pod_with_required_anti_affinity(
        "incoming",
        label_sel(&[("app", "web")]),
        "kubernetes.io/hostname",
    );
    let web_pod = scheduled_pod("web-1", "n1-no-labels", &[("app", "web")]);
    // Because the node lacks the topology key, matches_pod_affinity_term returns false
    let (passes, _) = check_pod_anti_affinity(&n, &pod, &[web_pod]);
    assert!(passes);
}

/// Upstream ref: filtering_test.go — "required anti-affinity: matching pod on different node".
///
/// Asserts the ACTUAL (and incorrect) Rusternetes behaviour: the existing "web"
/// pod is scheduled on a DIFFERENT node (n2), yet anti-affinity still BLOCKS
/// scheduling onto n1. This happens because `matches_pod_affinity_term`
/// (advanced.rs ~395-422) ignores the matching pod's node/topologyKey — it only
/// checks whether the candidate node carries the topology label and whether ANY
/// scheduled pod matches the selector, then returns `!matching_pods.is_empty()`.
/// The `#[ignore]` test below pins the topology-correct expectation.
#[test]
fn pod_anti_affinity_required_matching_pod_different_node_blocks_current_behaviour() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_anti_affinity(
        "incoming",
        label_sel(&[("app", "web")]),
        "kubernetes.io/hostname",
    );
    // Existing "web" pod is scheduled on n2 (a different topology domain).
    let web_pod = scheduled_pod("web-1", "n2", &[("app", "web")]);
    let (passes, _) = check_pod_anti_affinity(&n, &pod, &[web_pod]);
    // Current impl is NOT topology-aware, so it incorrectly blocks scheduling.
    assert!(
        !passes,
        "current (non-topology-aware) impl blocks even when the conflicting pod is on a different node"
    );
}

/// Topology-CORRECT expectation for the case above. In real Kubernetes a pod in
/// a different topology domain (different hostname) must NOT trigger
/// hostname-scoped anti-affinity, so scheduling onto n1 should PASS.
#[test]
#[ignore = "GAP: pod (anti-)affinity not topology-aware; advanced.rs matches_pod_affinity_term ignores the matching pod's node/topologyKey"]
fn pod_anti_affinity_required_matching_pod_different_node_should_pass_topology_aware() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_anti_affinity(
        "incoming",
        label_sel(&[("app", "web")]),
        "kubernetes.io/hostname",
    );
    // Conflicting "web" pod lives in the n2 topology domain, not n1's.
    let web_pod = scheduled_pod("web-1", "n2", &[("app", "web")]);
    let (passes, _) = check_pod_anti_affinity(&n, &pod, &[web_pod]);
    assert!(
        passes,
        "hostname-scoped anti-affinity must not block when the conflicting pod is in a different topology domain"
    );
}

/// Upstream ref: filtering_test.go — "required anti-affinity: unscheduled pod not counted"
#[test]
fn pod_anti_affinity_required_unscheduled_pod_not_counted() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_anti_affinity(
        "incoming",
        label_sel(&[("app", "web")]),
        "kubernetes.io/hostname",
    );
    // Matching pod exists but is NOT scheduled
    let unscheduled = pod_bare("web-unscheduled");
    let (passes, _) = check_pod_anti_affinity(&n, &pod, &[unscheduled]);
    assert!(passes);
}

/// Upstream ref: filtering_test.go — "required anti-affinity: matchExpressions Exists"
#[test]
fn pod_anti_affinity_required_match_expressions_exists() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_required_anti_affinity(
        "incoming",
        label_sel_expr("app", "Exists", &[]),
        "kubernetes.io/hostname",
    );
    let web_pod = scheduled_pod("web-1", "n1", &[("app", "web")]);
    let (passes, _) = check_pod_anti_affinity(&n, &pod, &[web_pod]);
    assert!(!passes);
}

/// Upstream ref: scoring_test.go — "preferred anti-affinity: matching pod adds penalty"
#[test]
fn pod_anti_affinity_preferred_matching_pod_penalises() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_preferred_anti_affinity(
        "incoming",
        60,
        label_sel(&[("app", "db")]),
        "kubernetes.io/hostname",
    );
    let db_pod = scheduled_pod("db-1", "n1", &[("app", "db")]);
    let (passes, penalty) = check_pod_anti_affinity(&n, &pod, &[db_pod]);
    assert!(passes);
    assert_eq!(penalty, 60);
}

/// Upstream ref: scoring_test.go — "preferred anti-affinity: no matching pod scores 0"
#[test]
fn pod_anti_affinity_preferred_no_match_scores_zero() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    let pod = pod_with_preferred_anti_affinity(
        "incoming",
        60,
        label_sel(&[("app", "db")]),
        "kubernetes.io/hostname",
    );
    let (passes, penalty) = check_pod_anti_affinity(&n, &pod, &[]);
    assert!(passes);
    assert_eq!(penalty, 0);
}

/// Upstream ref: scoring_test.go — "preferred anti-affinity: multiple terms, weights accumulate"
#[test]
fn pod_anti_affinity_preferred_multiple_terms_accumulate() {
    let n = node_with_labels("n1", &[("kubernetes.io/hostname", "n1")]);
    // Two preferred anti-affinity terms
    let affinity = Affinity {
        node_affinity: None,
        pod_affinity: None,
        pod_anti_affinity: Some(PodAntiAffinity {
            required_during_scheduling_ignored_during_execution: None,
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                WeightedPodAffinityTerm {
                    weight: 30,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: label_sel(&[("app", "db")]),
                        namespaces: None,
                        topology_key: "kubernetes.io/hostname".to_string(),
                    },
                },
                WeightedPodAffinityTerm {
                    weight: 20,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: label_sel(&[("tier", "backend")]),
                        namespaces: None,
                        topology_key: "kubernetes.io/hostname".to_string(),
                    },
                },
            ]),
        }),
    };
    let spec = PodSpec {
        containers: vec![],
        affinity: Some(affinity),
        ..Default::default()
    };
    let pod = Pod::new("incoming", spec);

    let db_pod = scheduled_pod("db-1", "n1", &[("app", "db")]);
    let be_pod = scheduled_pod("be-1", "n1", &[("tier", "backend")]);

    let (passes, penalty) = check_pod_anti_affinity(&n, &pod, &[db_pod, be_pod]);
    assert!(passes);
    assert_eq!(penalty, 50);
}

// ===========================================================================
// Combined scenarios
// ===========================================================================

/// Upstream ref: taint_toleration_test.go — "Exists + effect filter: wildcard key only for NoSchedule"
#[test]
fn taints_exists_with_effect_filter() {
    let n = node_with_taints(
        "n1",
        &[],
        vec![
            taint("k1", Some("v1"), "NoSchedule"),
            taint("k2", None, "NoExecute"),
        ],
    );
    // Toleration: key=None, operator=Exists, effect=NoSchedule → only covers NoSchedule taints
    let pod = pod_with_tolerations(
        "p1",
        vec![Toleration {
            key: None,
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }],
    );
    // NoExecute is NOT tolerated
    assert!(!check_taints_tolerations(&n, &pod));
}

/// "node affinity required + taint required: both must pass"
#[test]
fn node_affinity_and_taint_both_must_pass() {
    // Node: zone=us-east-1a and has a NoSchedule taint
    let n = {
        use rusternetes_common::resources::node::NodeSpec;
        let mut n = node_with_labels("n1", &[("zone", "us-east-1a")]);
        n.spec = Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            taints: Some(vec![taint("dedicated", Some("gpu"), "NoSchedule")]),
            unschedulable: None,
        });
        n
    };

    // Pod with matching affinity but NO toleration
    let pod = pod_with_required_node_affinity("p1", vec![nst_expr("zone", "In", &["us-east-1a"])]);

    // Affinity passes, but taint is not tolerated → scheduling must fail
    let (affinity_ok, _) = check_node_affinity(&n, &pod);
    let taint_ok = check_taints_tolerations(&n, &pod);
    assert!(affinity_ok, "node affinity should pass");
    assert!(!taint_ok, "untolerated taint should block");
}
