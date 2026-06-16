//! Affinity scheduling predicates shared between the scheduler and the
//! controller-manager.
//!
//! These are pure functions over `(node, pod, all_pods, all_nodes)` with no
//! scheduler-internal state. They were relocated from
//! `crates/scheduler/src/advanced.rs` so that both the scheduler and the
//! DaemonSet controller can depend on `rusternetes-common` instead of the
//! controller-manager depending on the scheduler crate (which created a
//! cross-crate dependency edge — the scheduler already dev-depends on the
//! controller-manager).

use crate::resources::{Node, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, Pod};
use tracing::debug;

/// Check node affinity requirements.
///
/// Returns `(passes_hard_requirements, preferred_score)`.
pub fn check_node_affinity(node: &Node, pod: &Pod) -> (bool, i32) {
    let affinity = match &pod.spec.as_ref().unwrap().affinity {
        Some(a) => a,
        None => return (true, 0), // No affinity requirements
    };

    let node_affinity = match &affinity.node_affinity {
        Some(na) => na,
        None => return (true, 0),
    };

    // Check required node affinity (hard requirement)
    if let Some(ref required) = node_affinity.required_during_scheduling_ignored_during_execution {
        if !matches_node_selector(node, required) {
            return (false, 0);
        }
    }

    // Calculate score from preferred node affinity (soft requirement)
    let mut score = 0;
    if let Some(ref preferred) = node_affinity.preferred_during_scheduling_ignored_during_execution
    {
        for pref in preferred {
            if matches_node_selector_term(node, &pref.preference) {
                score += pref.weight;
            }
        }
    }

    (true, score)
}

/// Check pod anti-affinity requirements.
///
/// Returns `(passes_hard_requirements, score_penalty)`.
pub fn check_pod_anti_affinity(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    all_nodes: &[Node],
) -> (bool, i32) {
    let affinity = match &pod.spec.as_ref().unwrap().affinity {
        Some(a) => a,
        None => return (true, 0), // No anti-affinity requirements
    };

    let pod_anti_affinity = match &affinity.pod_anti_affinity {
        Some(paa) => paa,
        None => return (true, 0),
    };

    // Check required pod anti-affinity (hard requirement)
    if let Some(ref required) =
        pod_anti_affinity.required_during_scheduling_ignored_during_execution
    {
        for term in required {
            // For anti-affinity, a conflict exists only when a matching pod runs
            // in the SAME topology domain as the candidate node. A matching pod
            // in a different domain must not block scheduling.
            if matches_pod_affinity_term(node, pod, term, all_pods, all_nodes, false) {
                debug!(
                    "Pod {} violates hard pod anti-affinity requirement on node {}",
                    pod.metadata.name, node.metadata.name
                );
                return (false, 0);
            }
        }
    }

    // Calculate score penalty from preferred pod anti-affinity (soft requirement)
    let mut penalty = 0;
    if let Some(ref preferred) =
        pod_anti_affinity.preferred_during_scheduling_ignored_during_execution
    {
        for weighted_term in preferred {
            if matches_pod_affinity_term(
                node,
                pod,
                &weighted_term.pod_affinity_term,
                all_pods,
                all_nodes,
                false,
            ) {
                penalty += weighted_term.weight;
            }
        }
    }

    (true, penalty)
}

/// Check if node matches a node selector.
pub fn matches_node_selector(node: &Node, selector: &NodeSelector) -> bool {
    // At least one term must match (OR logic)
    selector
        .node_selector_terms
        .iter()
        .any(|term| matches_node_selector_term(node, term))
}

/// Check if node matches a single node selector term.
pub fn matches_node_selector_term(node: &Node, term: &NodeSelectorTerm) -> bool {
    // Check match expressions (labels)
    if let Some(ref expressions) = term.match_expressions {
        if !expressions
            .iter()
            .all(|expr| matches_node_selector_requirement(node, expr, true))
        {
            return false;
        }
    }

    // Check match fields
    if let Some(ref fields) = term.match_fields {
        if !fields
            .iter()
            .all(|expr| matches_node_selector_requirement(node, expr, false))
        {
            return false;
        }
    }

    true
}

/// Check if node matches a selector requirement.
pub fn matches_node_selector_requirement(
    node: &Node,
    requirement: &NodeSelectorRequirement,
    is_label: bool,
) -> bool {
    let value = if is_label {
        // Get from node labels
        node.metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(&requirement.key))
            .map(|s| s.as_str())
    } else {
        // Get from node fields
        get_node_field(node, &requirement.key)
    };

    let values = requirement.values.as_deref().unwrap_or(&[]);

    match requirement.operator.as_str() {
        "In" => value
            .map(|v| values.contains(&v.to_string()))
            .unwrap_or(false),
        "NotIn" => !value
            .map(|v| values.contains(&v.to_string()))
            .unwrap_or(false),
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "Gt" => {
            if let Some(v) = value {
                if let Ok(node_val) = v.parse::<i64>() {
                    if !values.is_empty() {
                        if let Ok(req_val) = values[0].parse::<i64>() {
                            return node_val > req_val;
                        }
                    }
                }
            }
            false
        }
        "Lt" => {
            if let Some(v) = value {
                if let Ok(node_val) = v.parse::<i64>() {
                    if !values.is_empty() {
                        if let Ok(req_val) = values[0].parse::<i64>() {
                            return node_val < req_val;
                        }
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// Get a field value from a node.
fn get_node_field<'a>(node: &'a Node, field: &str) -> Option<&'a str> {
    match field {
        "metadata.name" => Some(&node.metadata.name),
        "metadata.namespace" => node.metadata.namespace.as_deref(),
        _ => None,
    }
}

/// Match a label selector against a set of labels.
pub fn match_selector(
    selector: &crate::types::LabelSelector,
    labels: &Option<std::collections::HashMap<String, String>>,
) -> bool {
    // Check matchLabels
    if let Some(ref match_labels) = selector.match_labels {
        let pod_labels = match labels {
            Some(l) => l,
            None => return match_labels.is_empty(),
        };

        for (key, value) in match_labels {
            if pod_labels.get(key) != Some(value) {
                return false;
            }
        }
    }

    // Check matchExpressions
    if let Some(ref match_expressions) = selector.match_expressions {
        let pod_labels = labels.as_ref();

        for expr in match_expressions {
            let label_value = pod_labels.and_then(|l| l.get(&expr.key));
            let values = expr.values.as_deref().unwrap_or(&[]);

            let matches = match expr.operator.as_str() {
                "In" => label_value
                    .map(|v| values.contains(&v.as_str().to_string()))
                    .unwrap_or(false),
                "NotIn" => !label_value
                    .map(|v| values.contains(&v.as_str().to_string()))
                    .unwrap_or(false),
                "Exists" => label_value.is_some(),
                "DoesNotExist" => label_value.is_none(),
                _ => false,
            };

            if !matches {
                return false;
            }
        }
    }

    true
}

/// Check if a pod affinity term is satisfied by the candidate `node`.
///
/// A term is satisfied when there is at least one already-scheduled pod that
/// (a) matches the term's label selector and namespace constraint, and
/// (b) runs on a node that shares the **same** `topologyKey` label value as the
/// candidate node. This mirrors upstream Kubernetes
/// (`pkg/scheduler/framework/plugins/interpodaffinity/filtering.go`), where a
/// matching pod only contributes a count to the `topologyPair{key, value}` of
/// the node it runs on, and the candidate node satisfies the term iff the count
/// for *its own* topology pair is positive.
///
/// For affinity this means: schedule only where a matching pod already exists in
/// the same topology domain. For anti-affinity the same predicate is used to
/// detect a *conflict*: a matching pod in the candidate node's topology domain
/// means the candidate node violates the anti-affinity term.
///
/// Namespace semantics (k8s spec): the term's `namespaces` field lists the
/// namespaces the `labelSelector` applies to. When `namespaces` is empty/None
/// (and, in upstream, no `namespaceSelector` is set — this type does not model a
/// `namespaceSelector`), the term defaults to the **namespace of the pod that
/// carries the affinity** (the `candidate_pod` here), NOT all namespaces.
///
/// A matching pod whose node cannot be resolved in `all_nodes`, or whose node
/// lacks the `topologyKey` label, contributes nothing (it cannot define a
/// topology pair) — exactly as in upstream's `update`/`append` helpers.
pub fn matches_pod_affinity_term(
    node: &Node,
    candidate_pod: &Pod,
    term: &crate::resources::PodAffinityTerm,
    all_pods: &[Pod],
    all_nodes: &[Node],
    _is_affinity: bool,
) -> bool {
    // Get the topology key value from the candidate node. If the candidate node
    // does not carry the topology label, it cannot belong to any topology domain
    // for this term, so the term is not satisfied.
    let candidate_topology_value = match node.metadata.labels.as_ref() {
        Some(labels) => match labels.get(&term.topology_key) {
            Some(v) => v.as_str(),
            None => return false,
        },
        None => return false,
    };

    // The namespace of the pod that carries the affinity term. Used as the
    // default match namespace when the term lists no explicit namespaces.
    let candidate_namespace = candidate_pod
        .metadata
        .namespace
        .as_deref()
        .unwrap_or("default");

    all_pods.iter().any(|p| {
        // Skip pods that aren't scheduled yet.
        let node_name = match p.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
            Some(name) => name,
            None => return false,
        };

        // Check if pod matches the label selector.
        if !match_selector(&term.label_selector, &p.metadata.labels) {
            return false;
        }

        // Check namespace constraint. Per the k8s spec, an empty/None
        // `namespaces` list (with no namespaceSelector, which this type does not
        // model) means the term applies to the candidate pod's own namespace —
        // NOT all namespaces.
        let pod_ns = p.metadata.namespace.as_deref().unwrap_or("default");
        match &term.namespaces {
            Some(namespaces) if !namespaces.is_empty() => {
                if !namespaces.contains(&pod_ns.to_string()) {
                    return false;
                }
            }
            _ => {
                // No explicit namespaces: restrict to the candidate pod's own namespace.
                if pod_ns != candidate_namespace {
                    return false;
                }
            }
        }

        // Resolve the node the matching pod runs on, then read its topology
        // value. The matching pod only counts when its node shares the same
        // topology value as the candidate node.
        let pod_node = match all_nodes.iter().find(|n| &n.metadata.name == node_name) {
            Some(n) => n,
            None => return false,
        };
        let pod_topology_value = pod_node
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(&term.topology_key));
        pod_topology_value.map(|v| v.as_str()) == Some(candidate_topology_value)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{Affinity, Node, Pod};
    use crate::resources::{PodAffinity, PodAffinityTerm, PodSpec, PodStatus};
    use crate::types::{LabelSelector, Phase};
    use std::collections::HashMap;

    /// Build a node carrying a single topology label.
    fn node_with_topology(name: &str, topo_key: &str, topo_val: &str) -> Node {
        let mut node = Node::new(name);
        let mut labels = HashMap::new();
        labels.insert(topo_key.to_string(), topo_val.to_string());
        node.metadata.labels = Some(labels);
        node
    }

    /// Build a scheduled pod with the given labels and namespace, on `node_name`.
    fn scheduled_pod(name: &str, namespace: &str, labels: &[(&str, &str)], node_name: &str) -> Pod {
        let mut label_map = HashMap::new();
        for (k, v) in labels {
            label_map.insert(k.to_string(), v.to_string());
        }
        let spec = PodSpec {
            node_name: Some(node_name.to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.metadata = pod.metadata.with_namespace(namespace);
        pod.metadata.labels = Some(label_map);
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        pod
    }

    /// Build a candidate pod (the one carrying the affinity) in `namespace` with
    /// a required podAffinity term selecting `app=web` over `topology_key`, and
    /// no explicit namespaces.
    fn candidate_with_affinity(name: &str, namespace: &str, topology_key: &str) -> Pod {
        let mut match_labels = HashMap::new();
        match_labels.insert("app".to_string(), "web".to_string());
        let term = PodAffinityTerm {
            label_selector: LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            },
            namespaces: None,
            topology_key: topology_key.to_string(),
            ..Default::default()
        };
        let spec = PodSpec {
            affinity: Some(Affinity {
                node_affinity: None,
                pod_affinity: Some(PodAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![term]),
                    preferred_during_scheduling_ignored_during_execution: None,
                }),
                pod_anti_affinity: None,
            }),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.metadata = pod.metadata.with_namespace(namespace);
        pod
    }

    /// #23: a podAffinityTerm with `namespaces == None` (and no namespaceSelector)
    /// must default to the candidate pod's OWN namespace, not all namespaces.
    #[test]
    fn affinity_term_default_namespace_is_candidate_namespace() {
        let topo_key = "kubernetes.io/hostname";
        let node = node_with_topology("node-1", topo_key, "node-1");

        // Candidate pod lives in namespace "ns-a".
        let candidate = candidate_with_affinity("candidate", "ns-a", topo_key);

        let term = match &candidate
            .spec
            .as_ref()
            .unwrap()
            .affinity
            .as_ref()
            .unwrap()
            .pod_affinity
            .as_ref()
            .unwrap()
            .required_during_scheduling_ignored_during_execution
        {
            Some(terms) => &terms[0],
            None => unreachable!(),
        };

        // A matching pod (app=web) on node-1 but in a DIFFERENT namespace must
        // NOT satisfy the term (cross-namespace default is same-namespace).
        let other_ns_pod = scheduled_pod("web-other", "ns-b", &[("app", "web")], "node-1");
        assert!(
            !matches_pod_affinity_term(
                &node,
                &candidate,
                term,
                std::slice::from_ref(&other_ns_pod),
                std::slice::from_ref(&node),
                true,
            ),
            "a pod in a different namespace must NOT match a namespaces=None term"
        );

        // A matching pod (app=web) on node-1 in the SAME namespace MUST satisfy it.
        let same_ns_pod = scheduled_pod("web-same", "ns-a", &[("app", "web")], "node-1");
        assert!(
            matches_pod_affinity_term(
                &node,
                &candidate,
                term,
                std::slice::from_ref(&same_ns_pod),
                std::slice::from_ref(&node),
                true,
            ),
            "a pod in the same namespace MUST match a namespaces=None term"
        );
    }
}
