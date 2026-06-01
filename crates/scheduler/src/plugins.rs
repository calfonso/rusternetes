//! Built-in scheduling plugins
//!
//! These plugins implement the standard Kubernetes scheduling policies using the scheduling framework.

use crate::advanced::{
    calculate_resource_score, check_host_port_conflicts, check_node_affinity, check_pod_affinity,
    check_pod_anti_affinity, check_taints_tolerations, check_topology_spread_constraints,
};
use crate::framework::*;
use async_trait::async_trait;
use rusternetes_common::resources::{Node, Pod};
use std::collections::HashMap;

/// K8s treats an empty string and an unset field as equivalent for toleration
/// `value`/`effect` and taint `value`, but `check_taints_tolerations` in
/// `advanced.rs` does strict `Option<&String>` comparisons. These helpers
/// normalize the relevant fields, cloning only when normalization is actually
/// needed so the hot path stays free of allocations.
fn empty_string(opt: &Option<String>) -> bool {
    opt.as_deref() == Some("")
}

fn pod_needs_toleration_normalization(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.tolerations.as_ref())
        .map(|tols| {
            tols.iter()
                .any(|t| empty_string(&t.value) || empty_string(&t.effect))
        })
        .unwrap_or(false)
}

fn node_needs_taint_normalization(node: &Node) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|taints| taints.iter().any(|t| empty_string(&t.value)))
        .unwrap_or(false)
}

fn normalize_pod_tolerations(pod: &Pod) -> Pod {
    let mut normalized = pod.clone();
    if let Some(tols) = normalized
        .spec
        .as_mut()
        .and_then(|s| s.tolerations.as_mut())
    {
        for t in tols.iter_mut() {
            if empty_string(&t.value) {
                t.value = None;
            }
            if empty_string(&t.effect) {
                t.effect = None;
            }
        }
    }
    normalized
}

fn normalize_node_taints(node: &Node) -> Node {
    let mut normalized = node.clone();
    if let Some(taints) = normalized.spec.as_mut().and_then(|s| s.taints.as_mut()) {
        for t in taints.iter_mut() {
            if empty_string(&t.value) {
                t.value = None;
            }
        }
    }
    normalized
}

// ============================================================================
// Filter Plugins
// ============================================================================

/// NodeUnschedulable filters nodes that are marked unschedulable
pub struct NodeUnschedulablePlugin;

#[async_trait]
impl FilterPlugin for NodeUnschedulablePlugin {
    fn name(&self) -> &'static str {
        "NodeUnschedulable"
    }

    async fn filter(
        &self,
        _state: &CycleState,
        _pod: &Pod,
        node: &Node,
        _handle: &FrameworkHandle,
    ) -> PluginResult {
        let is_unschedulable = node
            .spec
            .as_ref()
            .and_then(|s| s.unschedulable)
            .unwrap_or(false);

        if is_unschedulable {
            PluginResult::unschedulable(format!(
                "Node {} is marked unschedulable",
                node.metadata.name
            ))
        } else {
            PluginResult::success()
        }
    }
}

/// TaintToleration filters nodes based on taints and tolerations
pub struct TaintTolerationPlugin;

#[async_trait]
impl FilterPlugin for TaintTolerationPlugin {
    fn name(&self) -> &'static str {
        "TaintToleration"
    }

    async fn filter(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        _handle: &FrameworkHandle,
    ) -> PluginResult {
        // Clone only when an empty-string field is actually present; the
        // common case (well-formed specs with None or non-empty strings) hits
        // the underlying helper with zero extra allocations.
        let pod_owned;
        let node_owned;
        let pod_ref = if pod_needs_toleration_normalization(pod) {
            pod_owned = normalize_pod_tolerations(pod);
            &pod_owned
        } else {
            pod
        };
        let node_ref = if node_needs_taint_normalization(node) {
            node_owned = normalize_node_taints(node);
            &node_owned
        } else {
            node
        };
        if check_taints_tolerations(node_ref, pod_ref) {
            PluginResult::success()
        } else {
            PluginResult::unschedulable(format!(
                "Pod does not tolerate taints on node {}",
                node.metadata.name
            ))
        }
    }
}

/// NodeSelector filters nodes based on pod's node selector
pub struct NodeSelectorPlugin;

#[async_trait]
impl FilterPlugin for NodeSelectorPlugin {
    fn name(&self) -> &'static str {
        "NodeSelector"
    }

    async fn filter(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        _handle: &FrameworkHandle,
    ) -> PluginResult {
        let selector = match pod.spec.as_ref().and_then(|s| s.node_selector.as_ref()) {
            Some(s) => s,
            None => return PluginResult::success(), // No selector, all nodes match
        };

        let node_labels = node.metadata.labels.as_ref();

        if node_labels.is_none() {
            if selector.is_empty() {
                return PluginResult::success();
            } else {
                return PluginResult::unschedulable(format!(
                    "Node {} has no labels but pod requires selector {:?}",
                    node.metadata.name, selector
                ));
            }
        }

        let labels = node_labels.unwrap();

        for (key, value) in selector {
            if labels.get(key) != Some(value) {
                return PluginResult::unschedulable(format!(
                    "Node {} does not match node selector {}={}",
                    node.metadata.name, key, value
                ));
            }
        }

        PluginResult::success()
    }
}

/// NodeAffinity filters nodes based on node affinity requirements
pub struct NodeAffinityPlugin;

#[async_trait]
impl FilterPlugin for NodeAffinityPlugin {
    fn name(&self) -> &'static str {
        "NodeAffinity"
    }

    async fn filter(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        _handle: &FrameworkHandle,
    ) -> PluginResult {
        let (passes, _score) = check_node_affinity(node, pod);
        if passes {
            PluginResult::success()
        } else {
            PluginResult::unschedulable(format!(
                "Node {} does not meet node affinity requirements",
                node.metadata.name
            ))
        }
    }
}

/// PodAffinity filters nodes based on pod affinity requirements
pub struct PodAffinityPlugin;

#[async_trait]
impl FilterPlugin for PodAffinityPlugin {
    fn name(&self) -> &'static str {
        "PodAffinity"
    }

    async fn filter(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        handle: &FrameworkHandle,
    ) -> PluginResult {
        let (passes, _score) = check_pod_affinity(node, pod, &handle.all_pods, &handle.all_nodes);
        if passes {
            PluginResult::success()
        } else {
            PluginResult::unschedulable(format!(
                "Node {} does not meet pod affinity requirements",
                node.metadata.name
            ))
        }
    }
}

/// PodAntiAffinity filters nodes based on pod anti-affinity requirements
pub struct PodAntiAffinityPlugin;

#[async_trait]
impl FilterPlugin for PodAntiAffinityPlugin {
    fn name(&self) -> &'static str {
        "PodAntiAffinity"
    }

    async fn filter(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        handle: &FrameworkHandle,
    ) -> PluginResult {
        let (passes, _penalty) =
            check_pod_anti_affinity(node, pod, &handle.all_pods, &handle.all_nodes);
        if passes {
            PluginResult::success()
        } else {
            PluginResult::unschedulable(format!(
                "Node {} violates pod anti-affinity requirements",
                node.metadata.name
            ))
        }
    }
}

/// TopologySpreadConstraints filters nodes based on topology spread constraints
pub struct TopologySpreadConstraintsPlugin;

#[async_trait]
impl FilterPlugin for TopologySpreadConstraintsPlugin {
    fn name(&self) -> &'static str {
        "TopologySpreadConstraints"
    }

    async fn filter(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        handle: &FrameworkHandle,
    ) -> PluginResult {
        let (passes, _penalty) =
            check_topology_spread_constraints(node, pod, &handle.all_pods, &handle.all_nodes);
        if passes {
            PluginResult::success()
        } else {
            PluginResult::unschedulable(format!(
                "Node {} violates topology spread constraints",
                node.metadata.name
            ))
        }
    }
}

/// HostPort filters nodes where the pod's hostPort requirements would
/// conflict with hostPorts already in use by pods already scheduled on the
/// node. Two pods conflict if they share `(hostPort, protocol)` with
/// overlapping `hostIP` values (wildcard IPs `0.0.0.0`, `::`, or empty overlap
/// with anything).
pub struct HostPortPlugin;

#[async_trait]
impl FilterPlugin for HostPortPlugin {
    fn name(&self) -> &'static str {
        "HostPort"
    }

    async fn filter(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        handle: &FrameworkHandle,
    ) -> PluginResult {
        if check_host_port_conflicts(node, pod, &handle.all_pods) {
            PluginResult::success()
        } else {
            PluginResult::unschedulable(format!(
                "Pod hostPort conflicts with an existing pod on node {}",
                node.metadata.name
            ))
        }
    }
}

// ============================================================================
// Score Plugins
// ============================================================================

/// NodeResourcesFit scores nodes based on resource availability
pub struct NodeResourcesFitPlugin;

#[async_trait]
impl ScorePlugin for NodeResourcesFitPlugin {
    fn name(&self) -> &'static str {
        "NodeResourcesFit"
    }

    async fn score(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        _handle: &FrameworkHandle,
    ) -> Result<i64, String> {
        let score = calculate_resource_score(node, pod);
        Ok(score as i64)
    }
}

/// NodeAffinity scores nodes based on preferred node affinity
pub struct NodeAffinityScoringPlugin;

#[async_trait]
impl ScorePlugin for NodeAffinityScoringPlugin {
    fn name(&self) -> &'static str {
        "NodeAffinityScoring"
    }

    async fn score(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        _handle: &FrameworkHandle,
    ) -> Result<i64, String> {
        let (_passes, score) = check_node_affinity(node, pod);
        Ok(score as i64)
    }
}

/// PodAffinity scores nodes based on preferred pod affinity
pub struct PodAffinityScoringPlugin;

#[async_trait]
impl ScorePlugin for PodAffinityScoringPlugin {
    fn name(&self) -> &'static str {
        "PodAffinityScoring"
    }

    async fn score(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        handle: &FrameworkHandle,
    ) -> Result<i64, String> {
        let (_passes, score) = check_pod_affinity(node, pod, &handle.all_pods, &handle.all_nodes);
        Ok(score as i64)
    }
}

/// PodAntiAffinity scores nodes based on preferred pod anti-affinity (negative score)
pub struct PodAntiAffinityScoringPlugin;

#[async_trait]
impl ScorePlugin for PodAntiAffinityScoringPlugin {
    fn name(&self) -> &'static str {
        "PodAntiAffinityScoring"
    }

    async fn score(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        handle: &FrameworkHandle,
    ) -> Result<i64, String> {
        let (_passes, penalty) =
            check_pod_anti_affinity(node, pod, &handle.all_pods, &handle.all_nodes);
        // Return negative score (penalty)
        Ok(-(penalty as i64))
    }
}

/// TopologySpreadConstraints scores nodes to promote balanced spreading
pub struct TopologySpreadConstraintsScoringPlugin;

#[async_trait]
impl ScorePlugin for TopologySpreadConstraintsScoringPlugin {
    fn name(&self) -> &'static str {
        "TopologySpreadConstraintsScoring"
    }

    async fn score(
        &self,
        _state: &CycleState,
        pod: &Pod,
        node: &Node,
        handle: &FrameworkHandle,
    ) -> Result<i64, String> {
        let (_passes, penalty) =
            check_topology_spread_constraints(node, pod, &handle.all_pods, &handle.all_nodes);
        // Return negative score (penalty)
        Ok(-(penalty as i64))
    }
}

// ============================================================================
// Plugin Factory
// ============================================================================

/// Get default plugin registry with all built-in plugins
pub fn get_default_plugins() -> PluginRegistry {
    let mut registry = PluginRegistry::new();

    // Register filter plugins
    registry.register_filter_plugin(std::sync::Arc::new(NodeUnschedulablePlugin));
    registry.register_filter_plugin(std::sync::Arc::new(TaintTolerationPlugin));
    registry.register_filter_plugin(std::sync::Arc::new(NodeSelectorPlugin));
    registry.register_filter_plugin(std::sync::Arc::new(NodeAffinityPlugin));
    registry.register_filter_plugin(std::sync::Arc::new(PodAffinityPlugin));
    registry.register_filter_plugin(std::sync::Arc::new(PodAntiAffinityPlugin));
    registry.register_filter_plugin(std::sync::Arc::new(TopologySpreadConstraintsPlugin));
    registry.register_filter_plugin(std::sync::Arc::new(HostPortPlugin));

    // Register score plugins
    registry.register_score_plugin(std::sync::Arc::new(NodeResourcesFitPlugin));
    registry.register_score_plugin(std::sync::Arc::new(NodeAffinityScoringPlugin));
    registry.register_score_plugin(std::sync::Arc::new(PodAffinityScoringPlugin));
    registry.register_score_plugin(std::sync::Arc::new(PodAntiAffinityScoringPlugin));
    registry.register_score_plugin(std::sync::Arc::new(TopologySpreadConstraintsScoringPlugin));

    registry
}

/// Plugin configuration for customizing plugin weights
#[derive(Debug, Clone)]
pub struct PluginWeights {
    pub weights: HashMap<String, i64>,
}

impl PluginWeights {
    pub fn default_weights() -> Self {
        let mut weights = HashMap::new();
        weights.insert("NodeResourcesFit".to_string(), 25);
        weights.insert("NodeAffinityScoring".to_string(), 20);
        weights.insert("PodAffinityScoring".to_string(), 18);
        weights.insert("PodAntiAffinityScoring".to_string(), 12);
        weights.insert("TopologySpreadConstraintsScoring".to_string(), 10);

        Self { weights }
    }

    pub fn get_weight(&self, plugin_name: &str) -> i64 {
        self.weights.get(plugin_name).copied().unwrap_or(1)
    }
}
