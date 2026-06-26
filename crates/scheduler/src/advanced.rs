use rusternetes_common::resources::{
    IntOrString, Node, Pod, PodDisruptionBudget, PriorityClass, Taint, Toleration,
    TopologySpreadConstraint,
};
use std::collections::HashMap;
use tracing::debug;

// Affinity predicates relocated to `rusternetes_common::affinity`. Re-exported
// here so existing callers (`crate::advanced::check_node_affinity`, etc. in
// scheduler.rs / plugins.rs) keep resolving unchanged, and so the helpers used
// by scheduler-only predicates below (`match_selector`, `matches_pod_affinity_term`)
// are in scope.
pub use rusternetes_common::affinity::{
    check_node_affinity, check_pod_anti_affinity, match_selector, matches_pod_affinity_term,
};

/// Scoring result for a node
#[derive(Debug, Clone)]
pub struct NodeScore {
    pub node_name: String,
    pub score: i32,
}

/// Check if pod tolerates all node taints
pub fn check_taints_tolerations(node: &Node, pod: &Pod) -> bool {
    let node_taints = match &node.spec {
        Some(spec) => match &spec.taints {
            Some(taints) => taints,
            None => return true, // No taints, pod can be scheduled
        },
        None => return true, // No spec, no taints
    };

    let pod_tolerations = match &pod.spec.as_ref().unwrap().tolerations {
        Some(tolerations) => tolerations,
        None => {
            // No tolerations, check if there are any NoSchedule or NoExecute taints
            return !node_taints
                .iter()
                .any(|t| t.effect == "NoSchedule" || t.effect == "NoExecute");
        }
    };

    // Check each taint to see if there's a matching toleration
    for taint in node_taints {
        if !taint_is_tolerated(taint, pod_tolerations) {
            debug!(
                "Pod {} does not tolerate taint {:?} on node {}",
                pod.metadata.name, taint, node.metadata.name
            );
            return false;
        }
    }

    true
}

/// Check if a specific taint is tolerated by any of the tolerations
fn taint_is_tolerated(taint: &Taint, tolerations: &[Toleration]) -> bool {
    // PreferNoSchedule is a soft constraint, always tolerated for hard scheduling
    if taint.effect == "PreferNoSchedule" {
        return true;
    }

    for toleration in tolerations {
        if toleration_matches_taint(toleration, taint) {
            return true;
        }
    }

    false
}

/// Check if a toleration matches a taint
fn toleration_matches_taint(toleration: &Toleration, taint: &Taint) -> bool {
    let operator = toleration.operator.as_deref().unwrap_or("Equal");

    // Check effect
    if let Some(ref effect) = toleration.effect {
        if effect != &taint.effect {
            return false;
        }
    }

    // Check operator
    match operator {
        "Exists" => {
            // If key is empty, tolerate all taints
            if toleration.key.is_none() {
                return true;
            }
            // Otherwise check key matches
            toleration.key.as_ref() == Some(&taint.key)
        }
        "Equal" => {
            // Both key and value must match
            toleration.key.as_ref() == Some(&taint.key)
                && toleration.value.as_ref() == taint.value.as_ref()
        }
        _ => false,
    }
}

/// Check pod affinity requirements
/// Returns (passes_hard_requirements, score)
pub fn check_pod_affinity(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    all_nodes: &[Node],
) -> (bool, i32) {
    let affinity = match &pod.spec.as_ref().unwrap().affinity {
        Some(a) => a,
        None => return (true, 0), // No affinity requirements
    };

    let pod_affinity = match &affinity.pod_affinity {
        Some(pa) => pa,
        None => return (true, 0),
    };

    // Check required pod affinity (hard requirement)
    if let Some(ref required) = pod_affinity.required_during_scheduling_ignored_during_execution {
        for term in required {
            if !matches_pod_affinity_term(node, pod, term, all_pods, all_nodes, true) {
                debug!(
                    "Pod {} does not meet hard pod affinity requirement on node {}",
                    pod.metadata.name, node.metadata.name
                );
                return (false, 0);
            }
        }
    }

    // Calculate score from preferred pod affinity (soft requirement)
    let mut score = 0;
    if let Some(ref preferred) = pod_affinity.preferred_during_scheduling_ignored_during_execution {
        for weighted_term in preferred {
            if matches_pod_affinity_term(
                node,
                pod,
                &weighted_term.pod_affinity_term,
                all_pods,
                all_nodes,
                true,
            ) {
                score += weighted_term.weight;
            }
        }
    }

    (true, score)
}

/// Check if a pod's hostPort requirements conflict with pods already scheduled on the node.
/// Two pods conflict if they use the same hostPort AND the same protocol AND overlapping hostIPs.
/// A hostIP of "0.0.0.0", "::", or "" (empty/unset) means "all interfaces" and overlaps with
/// any other hostIP.
pub fn check_host_port_conflicts(node: &Node, pod: &Pod, all_pods: &[Pod]) -> bool {
    // Collect hostPorts requested by the incoming pod
    let incoming_ports = collect_host_ports(pod);
    if incoming_ports.is_empty() {
        return true; // No hostPort requirements, no conflict possible
    }

    let node_name = &node.metadata.name;

    // Collect hostPorts already in use on this node
    for existing_pod in all_pods {
        // Only consider pods scheduled on this node
        let on_this_node = existing_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .map(|n| n == node_name)
            .unwrap_or(false);
        if !on_this_node {
            continue;
        }

        // K8s tracks UsedPorts for ALL pods assigned to a node (including
        // Pending). Only skip terminal pods (Succeeded/Failed) and terminating.
        let phase = existing_pod.status.as_ref().and_then(|s| s.phase.as_ref());
        if matches!(
            phase,
            Some(rusternetes_common::types::Phase::Succeeded)
                | Some(rusternetes_common::types::Phase::Failed)
        ) {
            continue;
        }
        if existing_pod.metadata.deletion_timestamp.is_some() {
            continue;
        }

        let existing_ports = collect_host_ports(existing_pod);
        for (inc_port, inc_protocol, inc_ip) in &incoming_ports {
            for (ex_port, ex_protocol, ex_ip) in &existing_ports {
                if inc_port == ex_port
                    && inc_protocol == ex_protocol
                    && host_ips_overlap(inc_ip, ex_ip)
                {
                    debug!(
                        "HostPort conflict: port {} protocol {} hostIP {} vs {} on node {}",
                        inc_port, inc_protocol, inc_ip, ex_ip, node_name
                    );
                    return false;
                }
            }
        }
    }

    true
}

/// Collect all (hostPort, protocol, hostIP) tuples from a pod's containers.
fn collect_host_ports(pod: &Pod) -> Vec<(u16, String, String)> {
    let mut result = Vec::new();
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ports) = &container.ports {
                for port in ports {
                    // hostPort == 0 means "no host port" — it must NOT be tracked
                    // as a used port, otherwise two pods that both leave hostPort
                    // unset (0) would falsely conflict. The conformance netexec
                    // pods (Networking Granular Checks, HostPort) declare
                    // containerPort with hostPort: 0; treating 0 as a real port
                    // made the 2nd/3rd such pod unschedulable on every node.
                    if let Some(host_port) = port.host_port.filter(|&p| p != 0) {
                        let protocol = port.protocol.clone();
                        let host_ip = port.host_ip.clone().unwrap_or_default();
                        result.push((host_port, protocol, host_ip));
                    }
                }
            }
        }
        // Also check init containers
        if let Some(init_containers) = &spec.init_containers {
            for container in init_containers {
                if let Some(ports) = &container.ports {
                    for port in ports {
                        if let Some(host_port) = port.host_port.filter(|&p| p != 0) {
                            let protocol = port.protocol.clone();
                            let host_ip = port.host_ip.clone().unwrap_or_default();
                            result.push((host_port, protocol, host_ip));
                        }
                    }
                }
            }
        }
    }
    result
}

/// Check if two hostIP values overlap.
/// "0.0.0.0", "::", and "" all mean "all interfaces" and overlap with everything.
fn host_ips_overlap(ip1: &str, ip2: &str) -> bool {
    let wildcard = |ip: &str| ip.is_empty() || ip == "0.0.0.0" || ip == "::";
    if wildcard(ip1) || wildcard(ip2) {
        return true;
    }
    ip1 == ip2
}

/// Calculate resource-based node score
pub fn calculate_resource_score(node: &Node, pod: &Pod) -> i32 {
    calculate_resource_score_with_pods(node, pod, &[])
}

/// Calculate resource score accounting for pods already scheduled on the node.
/// Returns 0 if the node can't fit the pod, otherwise a score 1-100.
pub fn calculate_resource_score_with_pods(node: &Node, pod: &Pod, all_pods: &[Pod]) -> i32 {
    let allocatable = match &node.status {
        Some(status) => match &status.allocatable {
            Some(a) => a,
            None => return 50,
        },
        None => return 50,
    };

    // Get pod resource requests
    let mut cpu_request = 0i64;
    let mut memory_request = 0i64;

    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ref resources) = container.resources {
                if let Some(ref requests) = resources.requests {
                    if let Some(cpu) = requests.get("cpu") {
                        cpu_request += parse_resource_quantity(cpu, "cpu");
                    }
                    if let Some(memory) = requests.get("memory") {
                        memory_request += parse_resource_quantity(memory, "memory");
                    }
                }
            }
        }
    }

    // Calculate total allocatable
    let total_cpu = allocatable
        .get("cpu")
        .map(|s| parse_resource_quantity(s, "cpu"))
        .unwrap_or(0);
    let total_memory = allocatable
        .get("memory")
        .map(|s| parse_resource_quantity(s, "memory"))
        .unwrap_or(0);

    // Subtract resources used by pods already scheduled on this node.
    // K8s checks ALL resources (cpu, memory, AND extended resources like fakecpu).
    let mut used_cpu = 0i64;
    let mut used_memory = 0i64;
    let mut used_extended: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let node_name = &node.metadata.name;

    // Candidate identity + priority, for nominated-pod space reservation below.
    let candidate_priority = pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);
    let candidate_name = pod.metadata.name.as_str();
    let candidate_ns = pod.metadata.namespace.as_deref().unwrap_or("");

    for existing_pod in all_pods {
        let scheduled_on_this_node = existing_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .map(|n| n == node_name)
            .unwrap_or(false);

        // Reserve space for higher-or-equal-priority pods NOMINATED to this node
        // by preemption but not yet bound. Without this, a lower-priority pod
        // schedules into the space the preemptor just freed, gets preempted
        // again, and the owning controller recreates it — an endless
        // preemption live-lock (#542 PreemptionExecutionPath). Mirrors upstream
        // `addNominatedPods`, which runs the fit predicate with nominated pods
        // of >= priority added to the node.
        let is_self = existing_pod.metadata.name == candidate_name
            && existing_pod.metadata.namespace.as_deref().unwrap_or("") == candidate_ns;
        let nominated_here = existing_pod
            .status
            .as_ref()
            .and_then(|s| s.nominated_node_name.as_ref())
            .map(|n| n == node_name)
            .unwrap_or(false);
        let existing_priority = existing_pod
            .spec
            .as_ref()
            .and_then(|s| s.priority)
            .unwrap_or(0);
        let reserve_for_nominee =
            !is_self && nominated_here && existing_priority >= candidate_priority;

        if !scheduled_on_this_node && !reserve_for_nominee {
            continue;
        }
        // K8s tracks UsedPorts for ALL pods assigned to a node (including
        // Pending). Only skip terminal pods (Succeeded/Failed) and terminating.
        let phase = existing_pod.status.as_ref().and_then(|s| s.phase.as_ref());
        if matches!(
            phase,
            Some(rusternetes_common::types::Phase::Succeeded)
                | Some(rusternetes_common::types::Phase::Failed)
        ) {
            continue;
        }
        if existing_pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        if let Some(spec) = &existing_pod.spec {
            for container in &spec.containers {
                if let Some(ref resources) = container.resources {
                    if let Some(ref requests) = resources.requests {
                        if let Some(cpu) = requests.get("cpu") {
                            used_cpu += parse_resource_quantity(cpu, "cpu");
                        }
                        if let Some(memory) = requests.get("memory") {
                            used_memory += parse_resource_quantity(memory, "memory");
                        }
                        // Track extended resource usage. Quantities use the
                        // full k8s format (e.g. "1k" == 1000), so parse with
                        // parse_resource_quantity, not a raw i64 parse — a
                        // canonicalized value like "1k" would otherwise parse
                        // to 0 and silently under-count usage.
                        for (key, val) in requests {
                            if key != "cpu" && key != "memory" && key != "ephemeral-storage" {
                                *used_extended.entry(key.clone()).or_insert(0) +=
                                    parse_resource_quantity(val, key);
                            }
                        }
                    }
                }
            }
        }
    }

    // Check extended resources requested by the pod against node allocatable.
    // If ANY extended resource is insufficient, return 0 (can't schedule).
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ref resources) = container.resources {
                if let Some(ref requests) = resources.requests {
                    for (key, val) in requests {
                        if key != "cpu" && key != "memory" && key != "ephemeral-storage" {
                            // Both the request and the node's advertised capacity
                            // use the full k8s quantity format. The conformance
                            // `AddExtendedResource` helper writes `resource.MustParse`d
                            // values, which serialize canonically (e.g. 1000 -> "1k"),
                            // so a raw i64 parse of the node capacity yields 0 and
                            // wrongly rejects the pod. Parse both with
                            // parse_resource_quantity. (#542)
                            let requested = parse_resource_quantity(val, key);
                            let node_capacity = allocatable
                                .get(key)
                                .map(|s| parse_resource_quantity(s, key))
                                .unwrap_or(0);
                            let used = used_extended.get(key).copied().unwrap_or(0);
                            if used + requested > node_capacity {
                                return 0; // Extended resource insufficient
                            }
                        }
                    }
                }
            }
        }
    }

    let available_cpu = total_cpu - used_cpu;
    let available_memory = total_memory - used_memory;

    // If node can't fit the pod, return 0
    if cpu_request > available_cpu || memory_request > available_memory {
        return 0;
    }

    // Check extended resources (non-cpu/memory/pods/ephemeral-storage)
    // K8s scheduler checks all requested resources against node allocatable.
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ref resources) = container.resources {
                if let Some(ref requests) = resources.requests {
                    for (res_name, req_qty) in requests {
                        if res_name == "cpu"
                            || res_name == "memory"
                            || res_name == "pods"
                            || res_name == "ephemeral-storage"
                        {
                            continue; // Already handled above or not tracked
                        }
                        // Extended resource — check node allocatable
                        let total = allocatable
                            .get(res_name)
                            .map(|s| parse_resource_quantity(s, res_name))
                            .unwrap_or(0);
                        if total == 0 {
                            return 0; // Node doesn't have this resource
                        }
                        let requested = parse_resource_quantity(req_qty, res_name);
                        // Count used by other pods
                        let mut used = 0i64;
                        for existing_pod in all_pods {
                            let on_node = existing_pod
                                .spec
                                .as_ref()
                                .and_then(|s| s.node_name.as_ref())
                                .map(|n| n == node_name)
                                .unwrap_or(false);
                            if !on_node {
                                continue;
                            }
                            let phase = existing_pod.status.as_ref().and_then(|s| s.phase.as_ref());
                            if !matches!(phase, Some(rusternetes_common::types::Phase::Running)) {
                                continue;
                            }
                            if existing_pod.metadata.deletion_timestamp.is_some() {
                                continue;
                            }
                            if let Some(spec) = &existing_pod.spec {
                                for c in &spec.containers {
                                    if let Some(ref r) = c.resources {
                                        if let Some(ref reqs) = r.requests {
                                            if let Some(q) = reqs.get(res_name) {
                                                used += parse_resource_quantity(q, res_name);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if requested > total - used {
                            return 0; // Not enough extended resource
                        }
                    }
                }
            }
        }
    }

    // Calculate score based on remaining capacity (0-100)
    // Higher remaining capacity = higher score (balanced scheduling)
    let cpu_score = if available_cpu > 0 {
        ((available_cpu - cpu_request) * 100 / available_cpu) as i32
    } else {
        0
    };

    let memory_score = if available_memory > 0 {
        ((available_memory - memory_request) * 100 / available_memory) as i32
    } else {
        0
    };

    // Return average score
    (cpu_score + memory_score) / 2
}

/// Parse resource quantity (simplified)
/// Handles K8s resource formats:
///   CPU: "100m" (millicores), "0.5" or "1.5" (decimal cores), "2" (whole cores)
///   Memory: "128974848" (bytes), "129e6" (scientific), "129M" (SI), "123Mi" (binary)
///   Supported suffixes: Ki, Mi, Gi, Ti, k, M, G, T, E, P
fn parse_resource_quantity(quantity: &str, resource_type: &str) -> i64 {
    let quantity = quantity.trim();

    if resource_type == "cpu" {
        // CPU: support m (millicores), decimal, and integer cores
        if let Some(stripped) = quantity.strip_suffix('m') {
            stripped.parse::<i64>().unwrap_or(0)
        } else if let Ok(val) = quantity.parse::<f64>() {
            // Handles both "2" and "0.8" and "1.5"
            (val * 1000.0) as i64
        } else {
            0
        }
    } else {
        // Memory: binary suffixes (Ki, Mi, Gi, Ti) and SI suffixes (k, M, G, T, E, P)
        if let Some(stripped) = quantity.strip_suffix("Ki") {
            stripped.parse::<i64>().unwrap_or(0) * 1024
        } else if let Some(stripped) = quantity.strip_suffix("Mi") {
            stripped.parse::<i64>().unwrap_or(0) * 1024 * 1024
        } else if let Some(stripped) = quantity.strip_suffix("Gi") {
            stripped.parse::<i64>().unwrap_or(0) * 1024 * 1024 * 1024
        } else if let Some(stripped) = quantity.strip_suffix("Ti") {
            stripped.parse::<i64>().unwrap_or(0) * 1024 * 1024 * 1024 * 1024
        } else if let Some(stripped) = quantity.strip_suffix('T') {
            stripped.parse::<i64>().unwrap_or(0) * 1_000_000_000_000
        } else if let Some(stripped) = quantity.strip_suffix('G') {
            stripped.parse::<i64>().unwrap_or(0) * 1_000_000_000
        } else if let Some(stripped) = quantity.strip_suffix('M') {
            stripped.parse::<i64>().unwrap_or(0) * 1_000_000
        } else if let Some(stripped) = quantity.strip_suffix('k') {
            stripped.parse::<i64>().unwrap_or(0) * 1000
        } else if let Some(stripped) = quantity.strip_suffix('E') {
            stripped.parse::<i64>().unwrap_or(0) * 1_000_000_000_000_000_000
        } else if let Some(stripped) = quantity.strip_suffix('P') {
            stripped.parse::<i64>().unwrap_or(0) * 1_000_000_000_000_000
        } else if let Ok(val) = quantity.parse::<f64>() {
            // Plain number (bytes) or scientific notation like "129e6"
            val as i64
        } else {
            0
        }
    }
}

/// System-critical priority threshold. Pods at or above this priority
/// can only be preempted by pods with strictly higher priority.
#[allow(dead_code)]
const SYSTEM_CRITICAL_PRIORITY: i32 = 2_000_000_000;

/// Check if preemption should occur and return pods to evict.
///
/// This is the PDB-unaware entry point retained for callers that don't have
/// PodDisruptionBudgets loaded. Equivalent to
/// `check_preemption_with_pdbs(node, pod, all_pods, &[], &HashMap::new())`.
///
/// Returns (should_preempt, pods_to_evict).
///
/// Passes an empty PriorityClass map, so the `preemptionPolicy: Never` check
/// honours only the pod's own `spec.preemptionPolicy` (no class fallback) — its
/// callers don't load PriorityClasses. Use [`check_preemption_with_pdbs`] with a
/// populated map when the class fallback is needed.
///
/// `dead_code`: invoked from integration tests under `crates/scheduler/tests/`
/// and from the legacy direct-scheduling path in `scheduler.rs`. The bin's
/// Framework-based path doesn't call it.
#[allow(dead_code)]
pub fn check_preemption(node: &Node, pod: &Pod, all_pods: &[Pod]) -> (bool, Vec<String>) {
    check_preemption_with_pdbs(node, pod, all_pods, &[], &HashMap::new())
}

/// PDB-aware preemption victim selection.
///
/// The base algorithm (resource-fit, lowest-priority-first eviction with the
/// "remove all, then reprieve" reprieve pass) is identical to
/// `check_preemption`. After candidate victims are chosen, we run a second
/// reprieve pass that swaps PDB-covered victims for non-PDB-covered candidates
/// when resources still fit. This mirrors upstream Kubernetes' behavior at
/// `pkg/scheduler/framework/preemption/preemption.go::selectVictimsOnNode`,
/// which sorts candidates so that PDB-violating evictions are picked last.
///
/// Returns (should_preempt, pods_to_evict).
///
/// `dead_code`: exercised by integration tests; bin uses Framework path.
#[allow(dead_code)]
pub fn check_preemption_with_pdbs(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    pdbs: &[PodDisruptionBudget],
    priority_classes: &HashMap<String, PriorityClass>,
) -> (bool, Vec<String>) {
    // Get the priority of the incoming pod
    let incoming_priority = pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);

    // If incoming pod has priority <= 0, don't preempt
    if incoming_priority <= 0 {
        return (false, vec![]);
    }

    // Check preemptionPolicy — if "Never", do not preempt. Fall back to the
    // PriorityClass's policy when the pod spec doesn't carry one (mirrors
    // scheduler.rs::try_preempt: admission normally copies the class policy
    // onto the pod, but pods written directly to storage may miss it).
    let preemption_policy = pod
        .spec
        .as_ref()
        .and_then(|s| s.preemption_policy.as_deref())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|s| s.priority_class_name.as_deref())
                .and_then(|name| priority_classes.get(name))
                .and_then(|pc| pc.preemption_policy.as_deref())
        })
        .unwrap_or("PreemptLowerPriority");
    if preemption_policy == "Never" {
        debug!(
            "Pod {} has preemptionPolicy=Never, skipping preemption",
            pod.metadata.name
        );
        return (false, vec![]);
    }

    // Find all non-terminal pods on this node (K8s considers Pending and
    // Running pods as resource consumers and potential preemption victims).
    // Only skip terminal pods (Succeeded/Failed) and pods already terminating.
    let node_pods: Vec<&Pod> = all_pods
        .iter()
        .filter(|p| {
            let on_this_node = p
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .map(|n| n == &node.metadata.name)
                .unwrap_or(false);
            if !on_this_node {
                return false;
            }
            // Skip terminal pods (Succeeded/Failed) and terminating pods
            let phase = p.status.as_ref().and_then(|s| s.phase.as_ref());
            if matches!(
                phase,
                Some(rusternetes_common::types::Phase::Succeeded)
                    | Some(rusternetes_common::types::Phase::Failed)
            ) {
                return false;
            }
            p.metadata.deletion_timestamp.is_none()
        })
        .collect();

    // Find pods with lower priority that could be evicted
    // System-critical pods (priority >= 2000000000) are protected:
    // only pods with strictly higher priority may preempt them.
    let mut candidates: Vec<(&Pod, i32)> = node_pods
        .iter()
        .filter_map(|p| {
            let pod_priority = p.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);
            if pod_priority >= incoming_priority {
                return None; // Can't evict equal or higher priority
            }
            // Protect system-critical pods — only strictly higher priority can preempt
            if pod_priority >= SYSTEM_CRITICAL_PRIORITY && incoming_priority <= pod_priority {
                return None;
            }
            Some((*p, pod_priority))
        })
        .collect();

    // If no candidates, can't preempt
    if candidates.is_empty() {
        return (false, vec![]);
    }

    // Sort by priority (lowest first) for eviction
    candidates.sort_by_key(|(_, priority)| *priority);

    // Calculate ALL resources needed by incoming pod (cpu, memory, AND extended resources)
    // K8s preemption considers all resource types, not just cpu/memory.
    // See: pkg/scheduler/framework/preemption/preemption.go
    let mut resources_needed: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ref resources) = container.resources {
                if let Some(ref requests) = resources.requests {
                    for (key, val) in requests {
                        let amount = parse_resource_quantity(val, key);
                        *resources_needed.entry(key.clone()).or_insert(0) += amount;
                    }
                }
            }
        }
    }

    // Get node's total allocatable resources (all types)
    let allocatable: &std::collections::HashMap<String, String> =
        match node.status.as_ref().and_then(|s| s.allocatable.as_ref()) {
            Some(a) => a,
            None => return (false, vec![]),
        };
    let mut total_resources: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    for (key, val) in allocatable {
        total_resources.insert(key.clone(), parse_resource_quantity(val, key));
    }

    // If the pod can't fit even on a completely empty node, preemption won't help
    for (key, needed) in &resources_needed {
        let total = total_resources.get(key).copied().unwrap_or(0);
        if *needed > total {
            return (false, vec![]);
        }
    }

    // Calculate resources used by ALL pods on this node (including non-candidates)
    let mut total_used: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for p in &node_pods {
        if let Some(spec) = &p.spec {
            for container in &spec.containers {
                if let Some(ref resources) = container.resources {
                    if let Some(ref requests) = resources.requests {
                        for (key, val) in requests {
                            *total_used.entry(key.clone()).or_insert(0) +=
                                parse_resource_quantity(val, key);
                        }
                    }
                }
            }
        }
    }

    // Current remaining resources (before any eviction)
    let remaining = |key: &str| -> i64 {
        let total = total_resources.get(key).copied().unwrap_or(0);
        let used = total_used.get(key).copied().unwrap_or(0);
        total - used
    };

    // Check if all resources fit without eviction
    let all_fit = resources_needed
        .iter()
        .all(|(key, needed)| remaining(key) >= *needed);
    if all_fit {
        return (true, vec![]);
    }

    // K8s preemption algorithm: "remove all, then reprieve"
    // 1. Remove ALL lower-priority candidates and check if pod fits
    // 2. If it doesn't fit even with all removed → node not suitable
    // 3. Try to add back (reprieve) candidates from highest to lowest priority
    // 4. If adding a candidate back still lets the pod fit → reprieve it
    // 5. Final victims = candidates that could NOT be reprieved
    // See: pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:233-300

    // Calculate total freed resources if ALL candidates are evicted
    let mut total_freed: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for (candidate_pod, _) in &candidates {
        if let Some(spec) = &candidate_pod.spec {
            for container in &spec.containers {
                if let Some(ref resources) = container.resources {
                    if let Some(ref requests) = resources.requests {
                        for (key, val) in requests {
                            *total_freed.entry(key.clone()).or_insert(0) +=
                                parse_resource_quantity(val, key);
                        }
                    }
                }
            }
        }
    }

    // Check if pod fits even with ALL candidates removed
    let fits_without_all = resources_needed.iter().all(|(key, needed)| {
        let rem = remaining(key);
        let free = total_freed.get(key).copied().unwrap_or(0);
        (rem + free) >= *needed
    });
    if !fits_without_all {
        return (false, vec![]);
    }

    // Sort candidates by DESCENDING priority (highest first) for reprieve pass
    // K8s tries to reprieve higher-priority pods first
    let mut candidates_for_reprieve = candidates.clone();
    candidates_for_reprieve.sort_by_key(|(_, priority)| std::cmp::Reverse(*priority));

    // Track which candidates are victims (start with all as victims)
    let mut reprieved: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Try to reprieve each candidate (highest priority first)
    for (candidate_pod, _) in &candidates_for_reprieve {
        // Calculate resources freed by all NON-reprieved candidates (excluding this one)
        let mut freed_without_this: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (other_pod, _) in &candidates {
            if other_pod.metadata.name == candidate_pod.metadata.name {
                continue; // Skip the candidate we're trying to reprieve
            }
            if reprieved.contains(&other_pod.metadata.name) {
                continue; // Skip already-reprieved pods
            }
            if let Some(spec) = &other_pod.spec {
                for container in &spec.containers {
                    if let Some(ref resources) = container.resources {
                        if let Some(ref requests) = resources.requests {
                            for (key, val) in requests {
                                *freed_without_this.entry(key.clone()).or_insert(0) +=
                                    parse_resource_quantity(val, key);
                            }
                        }
                    }
                }
            }
        }

        // Check if pod still fits without evicting this candidate
        let fits_without = resources_needed.iter().all(|(key, needed)| {
            let rem = remaining(key);
            let free = freed_without_this.get(key).copied().unwrap_or(0);
            (rem + free) >= *needed
        });

        if fits_without {
            // Pod fits without evicting this candidate → reprieve it
            reprieved.insert(candidate_pod.metadata.name.clone());
        }
        // else: must evict this candidate
    }

    // PDB-aware reprieve: if any PDBs are supplied, re-run the reprieve pass
    // with PDB-covered candidates considered FIRST. The reprieve pass favors
    // keeping candidates that are visited early (they get their "fits without
    // me?" check while the rest of the candidates are still presumed to be
    // evicted, which gives them the best chance of being reprieved). Visiting
    // PDB-covered candidates first therefore biases the algorithm toward
    // evicting PDB-free pods. This mirrors the upstream Kubernetes scheduler's
    // dryRunPreemption/selectVictimsOnNode preference for non-PDB-violating
    // victims at pkg/scheduler/framework/preemption/preemption.go.
    if !pdbs.is_empty() {
        let pdb_covered: std::collections::HashSet<String> = candidates
            .iter()
            .filter(|(p, _)| pod_violates_any_pdb(p, pdbs, all_pods))
            .map(|(p, _)| p.metadata.name.clone())
            .collect();

        let mut pdb_aware_order = candidates.clone();
        // Primary key: PDB-covered first (reverse: true → 0, false → 1).
        // Secondary key: highest priority first (existing behavior).
        pdb_aware_order.sort_by_key(|(p, priority)| {
            let covered = pdb_covered.contains(&p.metadata.name);
            (if covered { 0 } else { 1 }, std::cmp::Reverse(*priority))
        });

        let mut reprieved_pdb: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (candidate_pod, _) in &pdb_aware_order {
            let mut freed_without_this: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            for (other_pod, _) in &candidates {
                if other_pod.metadata.name == candidate_pod.metadata.name {
                    continue;
                }
                if reprieved_pdb.contains(&other_pod.metadata.name) {
                    continue;
                }
                if let Some(spec) = &other_pod.spec {
                    for container in &spec.containers {
                        if let Some(ref resources) = container.resources {
                            if let Some(ref requests) = resources.requests {
                                for (key, val) in requests {
                                    *freed_without_this.entry(key.clone()).or_insert(0) +=
                                        parse_resource_quantity(val, key);
                                }
                            }
                        }
                    }
                }
            }

            let fits_without = resources_needed.iter().all(|(key, needed)| {
                let rem = remaining(key);
                let free = freed_without_this.get(key).copied().unwrap_or(0);
                (rem + free) >= *needed
            });

            if fits_without {
                reprieved_pdb.insert(candidate_pod.metadata.name.clone());
            }
        }

        reprieved = reprieved_pdb;
    }

    // Collect final victims (candidates that were NOT reprieved)
    let pods_to_evict: Vec<String> = candidates
        .iter()
        .filter(|(p, _)| !reprieved.contains(&p.metadata.name))
        .map(|(p, _)| p.metadata.name.clone())
        .collect();

    if pods_to_evict.is_empty() {
        // All candidates were reprieved — pod fits without any eviction
        return (true, vec![]);
    }

    debug!(
        "Preemption possible on node {}: evicting {} pods (reprieved {})",
        node.metadata.name,
        pods_to_evict.len(),
        reprieved.len()
    );
    (true, pods_to_evict)
}

/// Returns true if evicting `victim` would violate any of the supplied PDBs.
///
/// Conservative definition: any PDB whose selector matches the victim's labels
/// is considered to cover it. If the PDB's `minAvailable` is an integer, we
/// compare against the current count of running matching pods; if removing
/// `victim` drops the count below `minAvailable`, eviction is a violation.
/// Percentage minAvailable values are also evaluated against the current
/// matching population. `maxUnavailable` is treated as "1 disruption allowed"
/// from the current matching set (we don't track historical disruptions here).
fn pod_violates_any_pdb(victim: &Pod, pdbs: &[PodDisruptionBudget], all_pods: &[Pod]) -> bool {
    for pdb in pdbs {
        if !pdb_covers_pod(pdb, victim) {
            continue;
        }
        let healthy_now = all_pods
            .iter()
            .filter(|p| pdb_covers_pod(pdb, p))
            .filter(|p| !is_pod_terminal(p))
            .count() as i32;

        if let Some(ref min_avail) = pdb.spec.min_available {
            let min_avail_i = match min_avail {
                IntOrString::Int(n) => *n,
                IntOrString::String(s) => parse_min_available_percent(s, healthy_now),
            };
            if healthy_now - 1 < min_avail_i {
                return true;
            }
        }
        if let Some(ref max_unavail) = pdb.spec.max_unavailable {
            let max_unavail_i = match max_unavail {
                IntOrString::Int(n) => *n,
                IntOrString::String(s) => parse_min_available_percent(s, healthy_now),
            };
            // A single eviction counts as one unavailable.
            if max_unavail_i < 1 {
                return true;
            }
        }
    }
    false
}

/// True if the PDB's selector matches the pod's labels and the pod's namespace
/// equals the PDB's namespace.
fn pdb_covers_pod(pdb: &PodDisruptionBudget, pod: &Pod) -> bool {
    if pdb.metadata.namespace != pod.metadata.namespace
        && pdb.metadata.namespace.is_some()
        && pod.metadata.namespace.is_some()
    {
        return false;
    }
    match_selector(&pdb.spec.selector, &pod.metadata.labels)
}

fn is_pod_terminal(p: &Pod) -> bool {
    let phase = p.status.as_ref().and_then(|s| s.phase.as_ref());
    if matches!(
        phase,
        Some(rusternetes_common::types::Phase::Succeeded)
            | Some(rusternetes_common::types::Phase::Failed)
    ) {
        return true;
    }
    p.metadata.deletion_timestamp.is_some()
}

/// Parse a percentage string like "20%" into an absolute count given the
/// current healthy population. Falls back to 0 on parse error.
fn parse_min_available_percent(s: &str, healthy_now: i32) -> i32 {
    if let Some(num) = s.strip_suffix('%') {
        if let Ok(pct) = num.trim().parse::<f32>() {
            // Round up like K8s does (ceil): minimum of pct% of healthy.
            return ((healthy_now as f32) * pct / 100.0).ceil() as i32;
        }
    }
    s.trim().parse::<i32>().unwrap_or(0)
}

/// Check topology spread constraints for a pod
/// Returns (passes_hard_constraints, score_penalty)
pub fn check_topology_spread_constraints(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    all_nodes: &[Node],
) -> (bool, i32) {
    let constraints = match &pod.spec {
        Some(spec) => match &spec.topology_spread_constraints {
            Some(c) => c,
            None => return (true, 0), // No constraints
        },
        None => return (true, 0),
    };

    let mut total_penalty = 0;

    for constraint in constraints {
        let (passes, penalty) =
            check_single_topology_constraint(node, pod, constraint, all_pods, all_nodes);

        if !passes {
            return (false, 0); // Hard constraint failed
        }

        total_penalty += penalty;
    }

    (true, total_penalty)
}

/// Check a single topology spread constraint
fn check_single_topology_constraint(
    node: &Node,
    _pod: &Pod,
    constraint: &TopologySpreadConstraint,
    all_pods: &[Pod],
    all_nodes: &[Node],
) -> (bool, i32) {
    // Get the topology value for the candidate node
    let node_topology_value = match node.metadata.labels.as_ref() {
        Some(labels) => match labels.get(&constraint.topology_key) {
            Some(v) => v.clone(),
            None => {
                // Node doesn't have the topology key
                // If whenUnsatisfiable is DoNotSchedule, we can't schedule here
                if constraint.when_unsatisfiable == "DoNotSchedule" {
                    return (false, 0);
                }
                return (true, 0);
            }
        },
        None => {
            if constraint.when_unsatisfiable == "DoNotSchedule" {
                return (false, 0);
            }
            return (true, 0);
        }
    };

    // Find all pods that match the label selector
    let matching_pods: Vec<&Pod> = all_pods
        .iter()
        .filter(|p| {
            // Skip unscheduled pods
            if p.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_none() {
                return false;
            }

            // Check if pod matches the label selector
            if let Some(ref selector) = constraint.label_selector {
                match_selector(selector, &p.metadata.labels)
            } else {
                // No label selector means match all pods
                true
            }
        })
        .collect();

    // Count pods per topology domain
    let mut domain_counts: HashMap<String, i32> = HashMap::new();

    // Initialize counts for all domains
    for n in all_nodes {
        if let Some(labels) = &n.metadata.labels {
            if let Some(topology_value) = labels.get(&constraint.topology_key) {
                domain_counts.entry(topology_value.clone()).or_insert(0);
            }
        }
    }

    // Count matching pods per domain
    for p in &matching_pods {
        if let Some(spec) = &p.spec {
            if let Some(node_name) = &spec.node_name {
                // Find the node this pod is on
                if let Some(pod_node) = all_nodes.iter().find(|n| &n.metadata.name == node_name) {
                    if let Some(labels) = &pod_node.metadata.labels {
                        if let Some(topology_value) = labels.get(&constraint.topology_key) {
                            *domain_counts.entry(topology_value.clone()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }

    // Calculate skew if we place this pod on the candidate node
    let current_count = domain_counts
        .get(&node_topology_value)
        .copied()
        .unwrap_or(0);
    let new_count = current_count + 1;

    // Find min and max counts
    let min_count = domain_counts.values().min().copied().unwrap_or(0);
    let max_count = domain_counts.values().max().copied().unwrap_or(0);

    // Calculate skew after placing pod
    let skew = if new_count > min_count {
        new_count - min_count
    } else {
        max_count - min_count
    };

    // Check if skew exceeds max_skew
    if skew > constraint.max_skew {
        if constraint.when_unsatisfiable == "DoNotSchedule" {
            debug!(
                "Topology spread constraint violated: skew {} > max_skew {} for topology key {}",
                skew, constraint.max_skew, constraint.topology_key
            );
            return (false, 0);
        } else {
            // ScheduleAnyway - allow but penalize
            let penalty = (skew - constraint.max_skew) * 10; // Penalty proportional to skew violation
            return (true, penalty);
        }
    }

    // Check minDomains if specified
    if let Some(min_domains) = constraint.min_domains {
        let num_domains = domain_counts.len() as i32;
        if num_domains < min_domains {
            if constraint.when_unsatisfiable == "DoNotSchedule" {
                return (false, 0);
            } else {
                let penalty = (min_domains - num_domains) * 5;
                return (true, penalty);
            }
        }
    }

    // Constraint satisfied - add small penalty based on imbalance to prefer better spread
    let imbalance_penalty = ((new_count as f32 - min_count as f32) * 2.0) as i32;
    (true, imbalance_penalty.max(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpu_quantity() {
        assert_eq!(parse_resource_quantity("100m", "cpu"), 100);
        assert_eq!(parse_resource_quantity("1", "cpu"), 1000);
        assert_eq!(parse_resource_quantity("2", "cpu"), 2000);
        // Decimal CPU values (common in K8s)
        assert_eq!(parse_resource_quantity("0.5", "cpu"), 500);
        assert_eq!(parse_resource_quantity("0.8", "cpu"), 800);
        assert_eq!(parse_resource_quantity("1.5", "cpu"), 1500);
        assert_eq!(parse_resource_quantity("0.1", "cpu"), 100);
        assert_eq!(parse_resource_quantity("0.25", "cpu"), 250);
    }

    #[test]
    fn test_parse_memory_quantity() {
        assert_eq!(parse_resource_quantity("1Ki", "memory"), 1024);
        assert_eq!(parse_resource_quantity("1Mi", "memory"), 1024 * 1024);
        assert_eq!(parse_resource_quantity("1Gi", "memory"), 1024 * 1024 * 1024);
        assert_eq!(
            parse_resource_quantity("8Gi", "memory"),
            8 * 1024 * 1024 * 1024
        );
        // SI units
        assert_eq!(parse_resource_quantity("128M", "memory"), 128_000_000);
        assert_eq!(parse_resource_quantity("1G", "memory"), 1_000_000_000);
        // Plain bytes
        assert_eq!(parse_resource_quantity("128974848", "memory"), 128974848);
        // Scientific notation
        assert_eq!(parse_resource_quantity("129e6", "memory"), 129_000_000);
    }

    #[test]
    fn test_toleration_matches_taint() {
        let taint = Taint {
            key: "key1".to_string(),
            value: Some("value1".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        };

        let toleration = Toleration {
            key: Some("key1".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("value1".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        };

        assert!(toleration_matches_taint(&toleration, &taint));
    }

    use rusternetes_common::resources::NodeStatus;

    /// Helper: create a minimal container with resource requests
    fn make_container(cpu: &str, memory: &str) -> rusternetes_common::resources::Container {
        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), cpu.to_string());
        requests.insert("memory".to_string(), memory.to_string());
        rusternetes_common::resources::Container {
            name: "main".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: Some(rusternetes_common::types::ResourceRequirements {
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

    /// Helper: create a node with allocatable CPU and memory
    fn make_node(name: &str, cpu: &str, memory: &str) -> Node {
        let mut allocatable = HashMap::new();
        allocatable.insert("cpu".to_string(), cpu.to_string());
        allocatable.insert("memory".to_string(), memory.to_string());
        let mut node = Node::new(name);
        node.status = Some(NodeStatus {
            capacity: None,
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
        node
    }

    /// Helper: create a pod with given name, priority, and resource requests, scheduled on a node
    fn make_scheduled_pod(
        name: &str,
        priority: i32,
        cpu: &str,
        memory: &str,
        node_name: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container(cpu, memory)],
            priority: Some(priority),
            node_name: Some(node_name.to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Running),
            ..Default::default()
        });
        pod
    }

    /// Helper: create an unscheduled pod (the incoming pod wanting resources)
    fn make_incoming_pod(
        name: &str,
        priority: i32,
        cpu: &str,
        memory: &str,
        preemption_policy: Option<&str>,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container(cpu, memory)],
            priority: Some(priority),
            preemption_policy: preemption_policy.map(|s| s.to_string()),
            ..Default::default()
        };
        Pod::new(name, spec)
    }

    /// Extended-resource fit must honor canonical k8s quantity strings on the
    /// node's advertised capacity. The conformance `AddExtendedResource` helper
    /// writes `resource.MustParse("1000")`, which serializes as "1k"; a raw i64
    /// parse of "1k" yields 0 and wrongly rejects a pod that requests the
    /// resource (#542 — SchedulerPreemption PreemptionExecutionPath).
    #[test]
    fn test_extended_resource_fit_with_canonical_quantity() {
        let mut alloc = HashMap::new();
        alloc.insert("cpu".to_string(), "4".to_string());
        alloc.insert("memory".to_string(), "8Gi".to_string());
        alloc.insert("example.com/fakecpu".to_string(), "1k".to_string()); // == 1000
        let mut node = Node::new("node-2");
        node.status = Some(NodeStatus {
            capacity: None,
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

        let fakecpu_pod = |name: &str, req: &str| {
            let mut requests = HashMap::new();
            requests.insert("example.com/fakecpu".to_string(), req.to_string());
            let mut container = make_container("0", "0");
            container.resources = Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            });
            Pod::new(
                name,
                rusternetes_common::resources::PodSpec {
                    containers: vec![container],
                    ..Default::default()
                },
            )
        };

        // 200 of 1000 ("1k") MUST fit (score > 0).
        let pod = fakecpu_pod("rs-pod1", "200");
        assert!(
            calculate_resource_score_with_pods(&node, &pod, &[]) > 0,
            "pod requesting 200 of a 1k extended resource must fit"
        );

        // 2k (2000) of 1000 MUST NOT fit (score == 0).
        let big = fakecpu_pod("too-big", "2k");
        assert_eq!(
            calculate_resource_score_with_pods(&node, &big, &[]),
            0,
            "pod requesting 2k of a 1k extended resource must not fit"
        );
    }

    /// A node's freed space must be reserved for a higher-or-equal-priority pod
    /// already NOMINATED to it (by preemption), so a lower-priority candidate
    /// does not schedule into it and get re-preempted in a loop (#542).
    #[test]
    fn test_nominated_pod_reserves_extended_resource() {
        let mut alloc = HashMap::new();
        alloc.insert("cpu".to_string(), "4".to_string());
        alloc.insert("memory".to_string(), "8Gi".to_string());
        alloc.insert("example.com/fakecpu".to_string(), "1k".to_string()); // 1000
        let mut node = Node::new("node-2");
        node.status = Some(NodeStatus {
            capacity: None,
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

        let fakecpu_pod = |name: &str, req: &str, priority: i32| {
            let mut requests = HashMap::new();
            requests.insert("example.com/fakecpu".to_string(), req.to_string());
            let mut container = make_container("0", "0");
            container.resources = Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            });
            let mut pod = Pod::new(
                name,
                rusternetes_common::resources::PodSpec {
                    containers: vec![container],
                    priority: Some(priority),
                    ..Default::default()
                },
            );
            pod.metadata = pod.metadata.with_namespace("default");
            pod
        };

        // A high-priority preemptor (900) nominated to node-2 but not yet bound.
        let mut nominee = fakecpu_pod("preemptor", "900", 100);
        nominee.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Pending),
            nominated_node_name: Some("node-2".to_string()),
            ..Default::default()
        });

        // Low-priority candidate (priority 1) requesting 200.
        let candidate = fakecpu_pod("rs-pod1", "200", 1);

        // Without the nominee, 200 of 1000 fits.
        assert!(
            calculate_resource_score_with_pods(&node, &candidate, &[]) > 0,
            "200 of 1000 must fit when nothing is reserved"
        );

        // With the higher-priority nominee reserving 900, only 100 is free → 200 must NOT fit.
        assert_eq!(
            calculate_resource_score_with_pods(&node, &candidate, std::slice::from_ref(&nominee)),
            0,
            "candidate must not schedule into space reserved for a higher-priority nominee"
        );

        // The nominee itself is not reserved against itself — it still fits its nominated node.
        assert!(
            calculate_resource_score_with_pods(&node, &nominee, std::slice::from_ref(&nominee)) > 0,
            "the nominee must still fit its own nominated node"
        );
    }

    #[test]
    fn test_preemption_policy_never_should_not_preempt() {
        // Node with 2 CPUs
        let node = make_node("node-1", "2", "4Gi");

        // Existing low-priority pod using 1 CPU on node-1
        let existing = make_scheduled_pod("low-pri-pod", 100, "1", "1Gi", "node-1");

        // Incoming high-priority pod with preemptionPolicy=Never
        let incoming = make_incoming_pod("high-pri-pod", 1000, "2", "2Gi", Some("Never"));

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[existing]);

        assert!(
            !can_preempt,
            "Pod with preemptionPolicy=Never should not preempt"
        );
        assert!(
            pods_to_evict.is_empty(),
            "No pods should be evicted when preemptionPolicy=Never"
        );
    }

    #[test]
    fn test_preemption_blocked_for_system_critical_pod() {
        // Node with 2 CPUs
        let node = make_node("node-1", "2", "4Gi");

        // Existing system-critical pod (priority 2000000000) using 1 CPU
        let system_pod = make_scheduled_pod("system-critical", 2_000_000_000, "1", "1Gi", "node-1");

        // Incoming pod with priority 1000000000 — lower than the system-critical pod
        let incoming = make_incoming_pod("wants-resources", 1_000_000_000, "2", "2Gi", None);

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[system_pod]);

        // The incoming pod has lower priority than the system-critical pod,
        // so it should NOT be able to preempt it
        assert!(
            !can_preempt || pods_to_evict.is_empty(),
            "System-critical pod (priority >= 2000000000) should not be preempted by lower-priority pod"
        );
    }

    #[test]
    fn test_preemption_works_normally_for_lower_priority_pods() {
        // Sanity check: normal preemption still works
        let node = make_node("node-1", "2000m", "4Gi");

        // Existing low-priority pod using 1500m CPU
        let existing = make_scheduled_pod("low-pri", 100, "1500m", "1Gi", "node-1");

        // Incoming high-priority pod needs 1500m CPU (won't fit without eviction)
        let incoming = make_incoming_pod("high-pri", 1000, "1500m", "1Gi", None);

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[existing]);

        assert!(can_preempt, "Normal preemption should work");
        assert!(
            pods_to_evict.contains(&"low-pri".to_string()),
            "Low-priority pod should be evicted"
        );
    }

    // ---- HostPort conflict detection tests ----

    use rusternetes_common::resources::ContainerPort;

    /// Helper: create a container with a hostPort binding
    fn make_container_with_host_port(
        host_port: u16,
        protocol: &str,
        host_ip: &str,
    ) -> rusternetes_common::resources::Container {
        rusternetes_common::resources::Container {
            name: "main".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: Some(vec![ContainerPort {
                container_port: 80,
                name: None,
                protocol: protocol.to_string(),
                host_port: Some(host_port),
                host_ip: if host_ip.is_empty() {
                    None
                } else {
                    Some(host_ip.to_string())
                },
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

    /// Helper: create a pod with a hostPort, scheduled on a node
    fn make_host_port_pod(
        name: &str,
        host_port: u16,
        protocol: &str,
        host_ip: &str,
        node_name: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container_with_host_port(host_port, protocol, host_ip)],
            node_name: Some(node_name.to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Running),
            ..Default::default()
        });
        pod
    }

    /// Helper: create an unscheduled pod with a hostPort (incoming pod)
    fn make_incoming_host_port_pod(
        name: &str,
        host_port: u16,
        protocol: &str,
        host_ip: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container_with_host_port(host_port, protocol, host_ip)],
            ..Default::default()
        };
        Pod::new(name, spec)
    }

    #[test]
    fn test_host_port_no_conflict_when_no_host_ports() {
        let node = make_node("node-1", "2", "4Gi");
        let incoming = make_incoming_pod("pod-a", 0, "100m", "128Mi", None);
        assert!(
            check_host_port_conflicts(&node, &incoming, &[]),
            "Pod without hostPort should have no conflicts"
        );
    }

    #[test]
    fn test_host_port_conflict_same_port_same_protocol_same_ip() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort, same protocol, same (wildcard) hostIP should conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_port() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 9090, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Different hostPort should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_protocol() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "UDP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort but different protocol should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_host_ip() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "10.0.0.1", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.2");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort and protocol but different specific hostIPs should not conflict"
        );
    }

    #[test]
    fn test_host_port_conflict_wildcard_vs_specific_ip() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod binds to 0.0.0.0 (all interfaces)
        let existing = make_host_port_pod("existing", 8080, "TCP", "0.0.0.0", "node-1");
        // Incoming pod binds to a specific IP
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.1");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Wildcard hostIP (0.0.0.0) should conflict with any specific IP"
        );
    }

    #[test]
    fn test_host_port_conflict_empty_vs_specific_ip() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod with empty hostIP (means all interfaces)
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        // Incoming pod binds to a specific IP
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.1");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Empty hostIP (wildcard) should conflict with any specific IP"
        );
    }

    #[test]
    fn test_host_port_no_conflict_on_different_node() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod on node-2 (different node)
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-2");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Pods on different nodes should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_terminated_pod() {
        let node = make_node("node-1", "2", "4Gi");
        let mut existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        // Mark existing pod as Succeeded (terminated)
        existing.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Succeeded),
            ..Default::default()
        });
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Terminated pods should not cause conflicts"
        );
    }

    #[test]
    fn test_host_port_allows_same_port_different_ip_and_protocol() {
        // This is the exact scenario from the conformance test:
        // Two pods with same hostPort but different hostIP and protocol should coexist
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("pod-tcp", 8080, "TCP", "10.0.0.1", "node-1");
        let incoming = make_incoming_host_port_pod("pod-udp", 8080, "UDP", "10.0.0.2");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort with different hostIP AND different protocol should not conflict"
        );
    }

    #[test]
    fn test_host_ips_overlap() {
        // Wildcard cases
        assert!(host_ips_overlap("", "10.0.0.1"));
        assert!(host_ips_overlap("10.0.0.1", ""));
        assert!(host_ips_overlap("0.0.0.0", "10.0.0.1"));
        assert!(host_ips_overlap("10.0.0.1", "0.0.0.0"));
        assert!(host_ips_overlap("::", "10.0.0.1"));
        assert!(host_ips_overlap("", ""));
        assert!(host_ips_overlap("0.0.0.0", "0.0.0.0"));

        // Specific IPs
        assert!(host_ips_overlap("10.0.0.1", "10.0.0.1"));
        assert!(!host_ips_overlap("10.0.0.1", "10.0.0.2"));
        assert!(!host_ips_overlap("192.168.1.1", "10.0.0.1"));
    }
}
