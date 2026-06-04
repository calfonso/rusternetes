/// Pod admission controllers for ResourceQuota, LimitRange enforcement, and ServiceAccount injection
use rusternetes_common::{
    resources::{LimitRange, Pod, ResourceQuota, ServiceAccount},
    types::ResourceRequirements,
};
use rusternetes_storage::Storage;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Check if a pod is BestEffort QoS class.
/// A pod is BestEffort if NONE of its containers specify any resource requests or limits.
fn is_pod_best_effort(pod: &Pod) -> bool {
    let spec = match &pod.spec {
        Some(s) => s,
        None => return true,
    };
    for container in &spec.containers {
        if let Some(resources) = &container.resources {
            if let Some(requests) = &resources.requests {
                if !requests.is_empty() {
                    return false;
                }
            }
            if let Some(limits) = &resources.limits {
                if !limits.is_empty() {
                    return false;
                }
            }
        }
    }
    if let Some(init_containers) = &spec.init_containers {
        for container in init_containers {
            if let Some(resources) = &container.resources {
                if let Some(requests) = &resources.requests {
                    if !requests.is_empty() {
                        return false;
                    }
                }
                if let Some(limits) = &resources.limits {
                    if !limits.is_empty() {
                        return false;
                    }
                }
            }
        }
    }
    true
}

/// Check if a pod matches the scopes of a ResourceQuota.
/// All scopes must match (AND logic).
fn pod_matches_quota_scopes(pod: &Pod, quota: &ResourceQuota) -> bool {
    let is_terminating = pod.metadata.deletion_timestamp.is_some()
        || pod
            .spec
            .as_ref()
            .and_then(|s| s.active_deadline_seconds)
            .is_some();
    let is_best_effort = is_pod_best_effort(pod);

    // Check scopes list
    if let Some(scopes) = &quota.spec.scopes {
        for scope in scopes {
            match scope.as_str() {
                "Terminating" if !is_terminating => {
                    return false;
                }
                "NotTerminating" if is_terminating => {
                    return false;
                }
                "BestEffort" if !is_best_effort => {
                    return false;
                }
                "NotBestEffort" if is_best_effort => {
                    return false;
                }
                _ => {}
            }
        }
    }

    // Check scopeSelector if present
    if let Some(selector) = &quota.spec.scope_selector {
        for req in &selector.match_expressions {
            match req.scope_name.as_str() {
                "Terminating" => {
                    let matches = match req.operator.as_str() {
                        "Exists" => is_terminating,
                        "DoesNotExist" => !is_terminating,
                        _ => true,
                    };
                    if !matches {
                        return false;
                    }
                }
                "NotTerminating" => {
                    let matches = match req.operator.as_str() {
                        "Exists" => !is_terminating,
                        "DoesNotExist" => is_terminating,
                        _ => true,
                    };
                    if !matches {
                        return false;
                    }
                }
                "BestEffort" => {
                    let matches = match req.operator.as_str() {
                        "Exists" => is_best_effort,
                        "DoesNotExist" => !is_best_effort,
                        _ => true,
                    };
                    if !matches {
                        return false;
                    }
                }
                "NotBestEffort" => {
                    let matches = match req.operator.as_str() {
                        "Exists" => !is_best_effort,
                        "DoesNotExist" => is_best_effort,
                        _ => true,
                    };
                    if !matches {
                        return false;
                    }
                }
                "PriorityClass" => {
                    let pod_priority_class = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.priority_class_name.as_deref())
                        .unwrap_or("");
                    let matches = match req.operator.as_str() {
                        "In" => req
                            .values
                            .as_ref()
                            .is_some_and(|v| v.iter().any(|val| val == pod_priority_class)),
                        "NotIn" => req
                            .values
                            .as_ref()
                            .is_none_or(|v| !v.iter().any(|val| val == pod_priority_class)),
                        "Exists" => !pod_priority_class.is_empty(),
                        "DoesNotExist" => pod_priority_class.is_empty(),
                        _ => true,
                    };
                    if !matches {
                        return false;
                    }
                }
                _ => {}
            }
        }
    }

    true
}

/// Check if pod creation would exceed ResourceQuota limits.
///
/// Delegates to [`check_resource_quota_with_old`] with `old_pod = None`
/// so the new pod's full resource footprint counts against the quota.
pub async fn check_resource_quota<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    pod: &Pod,
) -> anyhow::Result<bool> {
    check_resource_quota_with_old(storage, namespace, pod, None).await
}

/// Check if a pod CREATE or UPDATE would exceed ResourceQuota limits.
///
/// `old_pod` is `Some` for UPDATE/PATCH and `None` for CREATE. When set,
/// the previous pod's resource contribution is subtracted from the live
/// namespace usage before the new pod's footprint is added — this
/// implements K8s delta-usage semantics so an UPDATE that does not raise
/// total usage past `.spec.hard` is admitted even if the live namespace
/// recount still sees the stale pod row.
///
/// K8s ref: staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/controller.go
/// (`charge` builds usage from `New` ‑ `Old`).
pub async fn check_resource_quota_with_old<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    pod: &Pod,
    old_pod: Option<&Pod>,
) -> anyhow::Result<bool> {
    let quota_prefix = format!("/registry/resourcequotas/{}/", namespace);
    let quotas: Vec<ResourceQuota> = storage.list(&quota_prefix).await?;

    if quotas.is_empty() {
        return Ok(true);
    }

    let pod_requests = calculate_pod_requests(pod);
    let pod_limits_cpu = calculate_pod_limits_cpu(pod);
    let pod_limits_memory = calculate_pod_limits_memory(pod);

    // For UPDATE: compute the old pod's contribution so we can subtract
    // it from the live namespace recount (which still includes the old
    // pod row in storage). On CREATE the deltas are all zero.
    let old_requests = old_pod.map(calculate_pod_requests).unwrap_or_default();
    let old_limits_cpu = old_pod.map(calculate_pod_limits_cpu).unwrap_or(0);
    let old_limits_memory = old_pod.map(calculate_pod_limits_memory).unwrap_or(0);
    // Whether the OLD pod was already counted by the namespace recount
    // (i.e. it still occupies a "pods" slot from the listing's POV).
    let old_pod_counted = old_pod
        .map(|p| {
            let phase = p.status.as_ref().and_then(|s| s.phase.as_ref());
            !matches!(
                phase,
                Some(rusternetes_common::types::Phase::Succeeded)
                    | Some(rusternetes_common::types::Phase::Failed)
            ) && p.metadata.deletion_timestamp.is_none()
        })
        .unwrap_or(false);

    for mut quota in quotas {
        if !pod_matches_quota_scopes(pod, &quota) {
            continue;
        }

        let hard = match &quota.spec.hard {
            Some(h) => h.clone(),
            None => continue,
        };

        // Always compute live usage from actual pods, not from quota status.used.
        // K8s quota evaluator recomputes usage to avoid stale data causing
        // false rejections (e.g., after pods are deleted, status.used is stale
        // until the quota controller reconciles).
        // K8s ref: staging/src/k8s.io/apiserver/pkg/quota/v1/generic/evaluator.go
        let current_usage = calculate_namespace_usage(storage, namespace).await?;

        let mut new_usage = current_usage.clone();
        let mut exceeded = Vec::new();

        // Check and increment pod count. On CREATE we add +1; on UPDATE
        // the slot is already counted, so the delta is 0 — an UPDATE can
        // never fail the pod-count check by itself.
        if let Some(pod_limit_str) = hard.get("pods") {
            let current_pods = current_usage
                .get("pods")
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            let baseline_pods = if old_pod_counted {
                (current_pods - 1).max(0)
            } else {
                current_pods
            };
            let limit = pod_limit_str.parse::<i64>().unwrap_or(i64::MAX);
            let projected_pods = baseline_pods + 1;
            // Only enforce the limit when this admission adds to the
            // count (CREATE). UPDATE keeps the slot count unchanged.
            if old_pod.is_none() && projected_pods > limit {
                exceeded.push(format!(
                    "pods, requested: 1, used: {}, limited: {}",
                    baseline_pods, limit
                ));
            } else {
                new_usage.insert("pods".to_string(), projected_pods.to_string());
            }
        }

        // Check requests.cpu (K8s aliases "cpu" → "requests.cpu").
        if let Some(cpu_limit_str) = hard.get("requests.cpu").or_else(|| hard.get("cpu")) {
            let current_cpu = current_usage
                .get("requests.cpu")
                .and_then(|s| parse_cpu_to_millicores(s).ok())
                .unwrap_or(0);
            let pod_cpu = pod_requests.get("cpu").copied().unwrap_or(0);
            let old_cpu = old_requests.get("cpu").copied().unwrap_or(0);
            let baseline_cpu = (current_cpu - old_cpu).max(0);
            let limit = parse_cpu_to_millicores(cpu_limit_str).unwrap_or(i64::MAX);
            if baseline_cpu + pod_cpu > limit {
                exceeded.push(format!(
                    "requests.cpu, requested: {}m, used: {}m, limited: {}m",
                    pod_cpu, baseline_cpu, limit
                ));
            } else {
                new_usage.insert(
                    "requests.cpu".to_string(),
                    format!("{}m", baseline_cpu + pod_cpu),
                );
            }
        }

        // Check requests.memory (K8s aliases "memory" → "requests.memory").
        if let Some(mem_limit_str) = hard.get("requests.memory").or_else(|| hard.get("memory")) {
            let current_mem = current_usage
                .get("requests.memory")
                .and_then(|s| parse_memory_to_bytes(s).ok())
                .unwrap_or(0);
            let pod_mem = pod_requests.get("memory").copied().unwrap_or(0);
            let old_mem = old_requests.get("memory").copied().unwrap_or(0);
            let baseline_mem = (current_mem - old_mem).max(0);
            let limit = parse_memory_to_bytes(mem_limit_str).unwrap_or(i64::MAX);
            if baseline_mem + pod_mem > limit {
                exceeded.push(format!(
                    "requests.memory, requested: {}, used: {}, limited: {}",
                    pod_mem, baseline_mem, limit
                ));
            } else {
                new_usage.insert(
                    "requests.memory".to_string(),
                    format!("{}", baseline_mem + pod_mem),
                );
            }
        }

        // Check limits.cpu.
        if let Some(cpu_limit_quota) = hard.get("limits.cpu") {
            let current_cpu = current_usage
                .get("limits.cpu")
                .and_then(|s| parse_cpu_to_millicores(s).ok())
                .unwrap_or(0);
            let baseline_cpu = (current_cpu - old_limits_cpu).max(0);
            let limit = parse_cpu_to_millicores(cpu_limit_quota).unwrap_or(i64::MAX);
            if baseline_cpu + pod_limits_cpu > limit {
                exceeded.push(format!(
                    "limits.cpu, requested: {}m, used: {}m, limited: {}m",
                    pod_limits_cpu, baseline_cpu, limit
                ));
            } else {
                new_usage.insert(
                    "limits.cpu".to_string(),
                    format!("{}m", baseline_cpu + pod_limits_cpu),
                );
            }
        }

        // Check limits.memory.
        if let Some(mem_limit_quota) = hard.get("limits.memory") {
            let current_mem = current_usage
                .get("limits.memory")
                .and_then(|s| parse_memory_to_bytes(s).ok())
                .unwrap_or(0);
            let baseline_mem = (current_mem - old_limits_memory).max(0);
            let limit = parse_memory_to_bytes(mem_limit_quota).unwrap_or(i64::MAX);
            if baseline_mem + pod_limits_memory > limit {
                exceeded.push(format!(
                    "limits.memory, requested: {}, used: {}, limited: {}",
                    pod_limits_memory, baseline_mem, limit
                ));
            } else {
                new_usage.insert(
                    "limits.memory".to_string(),
                    format!("{}", baseline_mem + pod_limits_memory),
                );
            }
        }

        // Check requests.ephemeral-storage.
        if let Some(es_limit_str) = hard.get("requests.ephemeral-storage") {
            let current_es = current_usage
                .get("requests.ephemeral-storage")
                .and_then(|s| parse_memory_to_bytes(s).ok())
                .unwrap_or(0);
            let pod_es = pod_requests.get("ephemeral-storage").copied().unwrap_or(0);
            let old_es = old_requests.get("ephemeral-storage").copied().unwrap_or(0);
            let baseline_es = (current_es - old_es).max(0);
            let limit = parse_memory_to_bytes(es_limit_str).unwrap_or(i64::MAX);
            if baseline_es + pod_es > limit {
                exceeded.push(format!(
                    "requests.ephemeral-storage, requested: {}, used: {}, limited: {}",
                    pod_es, baseline_es, limit
                ));
            } else {
                new_usage.insert(
                    "requests.ephemeral-storage".to_string(),
                    format!("{}", baseline_es + pod_es),
                );
            }
        }

        // Check extended resources (requests.example.com/foo, etc.) — K8s
        // treats any quota key starting with "requests." that is not
        // cpu/memory/ephemeral-storage as an extended resource.
        for (key, limit_str) in &hard {
            if key.starts_with("requests.")
                && !matches!(
                    key.as_str(),
                    "requests.cpu" | "requests.memory" | "requests.ephemeral-storage"
                )
            {
                let ext_name = &key["requests.".len()..];
                let limit: i64 = limit_str.parse().unwrap_or(i64::MAX);
                let current: i64 = current_usage
                    .get(key)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let pod_request = sum_extended_request(pod, ext_name);
                let old_request = old_pod
                    .map(|p| sum_extended_request(p, ext_name))
                    .unwrap_or(0);
                let baseline = (current - old_request).max(0);
                if pod_request > 0 && baseline + pod_request > limit {
                    exceeded.push(format!(
                        "{}, requested: {}, used: {}, limited: {}",
                        key, pod_request, baseline, limit
                    ));
                }
            }
        }

        if !exceeded.is_empty() {
            warn!(
                "Forbidden: exceeded quota: {}, {}",
                quota.metadata.name,
                exceeded.join(", ")
            );
            return Ok(false);
        }

        // Atomically update quota status.used via CAS.
        // K8s ref: controller.go:288 — UpdateQuotaStatus with resourceVersion.
        // On UPDATE we don't bump pod count (slot already taken); on CREATE we do.
        let quota_key = format!(
            "/registry/resourcequotas/{}/{}",
            namespace, quota.metadata.name
        );
        let status = quota.status.get_or_insert_with(|| {
            rusternetes_common::resources::ResourceQuotaStatus {
                hard: quota.spec.hard.clone(),
                used: None,
            }
        });
        status.used = Some(new_usage);

        if let Err(e) = storage.update(&quota_key, &quota).await {
            warn!(
                "Failed to atomically update quota usage for {}: {} — retrying with fresh data",
                quota.metadata.name, e
            );
            // CAS conflict: re-read quota and retry once
            if let Ok(fresh_quota) = storage.get::<ResourceQuota>(&quota_key).await {
                let mut retry_quota = fresh_quota;
                // Re-check with fresh data — simplified: just recount.
                let fresh_usage = calculate_namespace_usage(storage, namespace).await?;
                if let Some(pod_limit_str) =
                    retry_quota.spec.hard.as_ref().and_then(|h| h.get("pods"))
                {
                    let fresh_pods = fresh_usage
                        .get("pods")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    let limit = pod_limit_str.parse::<i64>().unwrap_or(i64::MAX);
                    // CREATE adds +1; UPDATE doesn't (slot already counted).
                    let projected = fresh_pods + if old_pod.is_none() { 1 } else { 0 };
                    if projected > limit {
                        return Ok(false);
                    }
                }
                let mut retry_usage = fresh_usage;
                if old_pod.is_none() {
                    let pods_count = retry_usage
                        .get("pods")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    retry_usage.insert("pods".to_string(), (pods_count + 1).to_string());
                }
                let status = retry_quota.status.get_or_insert_with(|| {
                    rusternetes_common::resources::ResourceQuotaStatus {
                        hard: retry_quota.spec.hard.clone(),
                        used: None,
                    }
                });
                status.used = Some(retry_usage);
                let _ = storage.update(&quota_key, &retry_quota).await;
            }
        }
    }

    Ok(true)
}

/// Sum the request value for a named extended resource across all
/// containers in `pod`. Returns 0 if the resource is unset or unparseable.
fn sum_extended_request(pod: &Pod, ext_name: &str) -> i64 {
    pod.spec
        .as_ref()
        .map(|s| {
            s.containers
                .iter()
                .filter_map(|c| {
                    c.resources
                        .as_ref()
                        .and_then(|r| r.requests.as_ref())
                        .and_then(|reqs| reqs.get(ext_name))
                        .and_then(|v| v.parse::<i64>().ok())
                })
                .sum::<i64>()
        })
        .unwrap_or(0)
}

/// Apply LimitRange defaults and validate constraints
#[allow(dead_code)]
pub async fn apply_limit_range<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    pod: &mut Pod,
) -> anyhow::Result<bool> {
    let limit_prefix = format!("/registry/limitranges/{}/", namespace);
    let limit_ranges: Vec<LimitRange> = storage.list(&limit_prefix).await?;
    apply_limit_range_with(pod, &limit_ranges)
}

/// Apply LimitRange defaults and validate constraints using pre-fetched LimitRanges.
/// Use this when the caller already has the LimitRange list to avoid a redundant storage read.
pub fn apply_limit_range_with(
    pod: &mut Pod,
    limit_ranges: &Vec<LimitRange>,
) -> anyhow::Result<bool> {
    if limit_ranges.is_empty() {
        // No limits to apply
        return Ok(true);
    }

    // Apply defaults and validate for each container
    if let Some(spec) = &mut pod.spec {
        for container in &mut spec.containers {
            for limit_range in limit_ranges {
                for limit_item in &limit_range.spec.limits {
                    // Only apply Container limits to containers
                    if limit_item.item_type == "Container" {
                        // Apply defaults if not specified
                        if container.resources.is_none() {
                            container.resources = Some(ResourceRequirements {
                                limits: None,
                                requests: None,
                                claims: None,
                            });
                        }

                        let resources = container.resources.as_mut().unwrap();

                        // Apply default limits
                        if let Some(default_limits) = &limit_item.default {
                            if let Some(limits) = resources.limits.as_mut() {
                                // Merge with existing limits
                                for (key, value) in default_limits {
                                    limits.entry(key.clone()).or_insert_with(|| value.clone());
                                }
                            } else {
                                resources.limits = Some(default_limits.clone());
                            }
                        }

                        // Apply defaultRequest for missing request resources.
                        // If defaultRequest is not defined, fall back to default (limits).
                        let effective_defaults = limit_item
                            .default_request
                            .as_ref()
                            .or(limit_item.default.as_ref());
                        if let Some(defaults) = effective_defaults {
                            let requests = resources.requests.get_or_insert_with(HashMap::new);
                            for (key, value) in defaults {
                                requests.entry(key.clone()).or_insert_with(|| value.clone());
                            }
                        }

                        // Validate min constraints
                        if let Some(min) = &limit_item.min {
                            if !validate_min_resources(resources, min, &container.name)? {
                                return Ok(false);
                            }
                        }

                        // Validate max constraints
                        if let Some(max) = &limit_item.max {
                            if !validate_max_resources(resources, max, &container.name)? {
                                return Ok(false);
                            }
                        }

                        // Validate max limit/request ratio
                        if let Some(ratio) = &limit_item.max_limit_request_ratio {
                            if !validate_ratio(resources, ratio, &container.name)? {
                                return Ok(false);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(true)
}

// Helper functions

async fn calculate_namespace_usage<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
) -> anyhow::Result<HashMap<String, String>> {
    let mut usage = HashMap::new();

    // Count ACTIVE pods (exclude terminal and terminating).
    // K8s only counts non-terminal pods against quota.
    // K8s ref: pkg/quota/v1/evaluator/core/pods.go — PodEvaluator
    let pod_prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&pod_prefix).await?;
    let active_pods: Vec<&Pod> = pods
        .iter()
        .filter(|p| {
            let phase = p.status.as_ref().and_then(|s| s.phase.as_ref());
            !matches!(
                phase,
                Some(rusternetes_common::types::Phase::Succeeded)
                    | Some(rusternetes_common::types::Phase::Failed)
            ) && p.metadata.deletion_timestamp.is_none()
        })
        .collect();
    usage.insert("pods".to_string(), active_pods.len().to_string());

    // Calculate CPU and memory requests from ACTIVE pods only
    let mut total_cpu_requests = 0i64;
    let mut total_memory_requests = 0i64;

    for pod in &active_pods {
        if let Some(spec) = &pod.spec {
            for container in &spec.containers {
                if let Some(resources) = &container.resources {
                    if let Some(requests) = &resources.requests {
                        if let Some(cpu) = requests.get("cpu") {
                            if let Ok(millis) = parse_cpu_to_millicores(cpu) {
                                total_cpu_requests += millis;
                            }
                        }
                        if let Some(memory) = requests.get("memory") {
                            if let Ok(bytes) = parse_memory_to_bytes(memory) {
                                total_memory_requests += bytes;
                            }
                        }
                    }
                }
            }
        }
    }

    if total_cpu_requests > 0 {
        usage.insert(
            "requests.cpu".to_string(),
            format!("{}m", total_cpu_requests),
        );
    }
    if total_memory_requests > 0 {
        usage.insert(
            "requests.memory".to_string(),
            bytes_to_memory_string(total_memory_requests),
        );
    }

    // Count extended resources (anything that's not cpu/memory/ephemeral-storage)
    let mut extended_totals: HashMap<String, i64> = HashMap::new();
    for pod in &active_pods {
        if let Some(spec) = &pod.spec {
            for container in &spec.containers {
                if let Some(resources) = &container.resources {
                    if let Some(requests) = &resources.requests {
                        for (key, val) in requests {
                            if key != "cpu" && key != "memory" && key != "ephemeral-storage" {
                                if let Ok(n) = val.parse::<i64>() {
                                    *extended_totals
                                        .entry(format!("requests.{}", key))
                                        .or_insert(0) += n;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    for (key, total) in extended_totals {
        usage.insert(key, total.to_string());
    }

    Ok(usage)
}

fn calculate_pod_requests(pod: &Pod) -> HashMap<String, i64> {
    let mut requests = HashMap::new();
    let mut total_cpu = 0i64;
    let mut total_memory = 0i64;

    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(resources) = &container.resources {
                if let Some(reqs) = &resources.requests {
                    if let Some(cpu) = reqs.get("cpu") {
                        if let Ok(millis) = parse_cpu_to_millicores(cpu) {
                            total_cpu += millis;
                        }
                    }
                    if let Some(memory) = reqs.get("memory") {
                        if let Ok(bytes) = parse_memory_to_bytes(memory) {
                            total_memory += bytes;
                        }
                    }
                }
            }
        }
    }

    if total_cpu > 0 {
        requests.insert("cpu".to_string(), total_cpu);
    }
    if total_memory > 0 {
        requests.insert("memory".to_string(), total_memory);
    }

    requests
}

fn calculate_pod_limits_cpu(pod: &Pod) -> i64 {
    let mut total = 0i64;
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(resources) = &container.resources {
                if let Some(limits) = &resources.limits {
                    if let Some(cpu) = limits.get("cpu") {
                        if let Ok(millis) = parse_cpu_to_millicores(cpu) {
                            total += millis;
                        }
                    }
                }
            }
        }
    }
    total
}

fn calculate_pod_limits_memory(pod: &Pod) -> i64 {
    let mut total = 0i64;
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(resources) = &container.resources {
                if let Some(limits) = &resources.limits {
                    if let Some(memory) = limits.get("memory") {
                        if let Ok(bytes) = parse_memory_to_bytes(memory) {
                            total += bytes;
                        }
                    }
                }
            }
        }
    }
    total
}

fn validate_min_resources(
    resources: &ResourceRequirements,
    min: &HashMap<String, String>,
    container_name: &str,
) -> anyhow::Result<bool> {
    // Check requests against min
    if let Some(requests) = &resources.requests {
        for (resource, min_value) in min {
            if let Some(request_value) = requests.get(resource) {
                let below = if resource == "cpu" {
                    parse_cpu_to_millicores(request_value)? < parse_cpu_to_millicores(min_value)?
                } else {
                    parse_memory_to_bytes(request_value)? < parse_memory_to_bytes(min_value)?
                };
                if below {
                    warn!(
                        "Container {} has {} request {} below minimum {}",
                        container_name, resource, request_value, min_value
                    );
                    return Ok(false);
                }
            }
        }
    }
    // Check limits against min — K8s enforces min on both
    if let Some(limits) = &resources.limits {
        for (resource, min_value) in min {
            if let Some(limit_value) = limits.get(resource) {
                let below = if resource == "cpu" {
                    parse_cpu_to_millicores(limit_value)? < parse_cpu_to_millicores(min_value)?
                } else {
                    parse_memory_to_bytes(limit_value)? < parse_memory_to_bytes(min_value)?
                };
                if below {
                    warn!(
                        "Container {} has {} limit {} below minimum {}",
                        container_name, resource, limit_value, min_value
                    );
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

fn validate_max_resources(
    resources: &ResourceRequirements,
    max: &HashMap<String, String>,
    container_name: &str,
) -> anyhow::Result<bool> {
    // Check limits against max
    if let Some(limits) = &resources.limits {
        for (resource, max_value) in max {
            if let Some(limit_value) = limits.get(resource) {
                let exceeds = compare_resource_values(resource, limit_value, max_value)?;
                if exceeds {
                    warn!(
                        "Container {} has {} limit {} exceeding maximum {}",
                        container_name, resource, limit_value, max_value
                    );
                    return Ok(false);
                }
            }
        }
    }
    // Check requests against max — K8s enforces max on both limits and requests
    if let Some(requests) = &resources.requests {
        for (resource, max_value) in max {
            if let Some(request_value) = requests.get(resource) {
                let exceeds = compare_resource_values(resource, request_value, max_value)?;
                if exceeds {
                    warn!(
                        "Container {} has {} request {} exceeding maximum {}",
                        container_name, resource, request_value, max_value
                    );
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

/// Compare a resource value against a limit, returns true if value > limit.
/// Handles cpu, memory, ephemeral-storage, and other resources.
fn compare_resource_values(resource: &str, value: &str, limit: &str) -> anyhow::Result<bool> {
    if resource == "cpu" {
        Ok(parse_cpu_to_millicores(value)? > parse_cpu_to_millicores(limit)?)
    } else {
        // memory, ephemeral-storage, and other byte-based resources
        Ok(parse_memory_to_bytes(value)? > parse_memory_to_bytes(limit)?)
    }
}

fn validate_ratio(
    resources: &ResourceRequirements,
    max_ratio: &HashMap<String, String>,
    container_name: &str,
) -> anyhow::Result<bool> {
    if let (Some(limits), Some(requests)) = (&resources.limits, &resources.requests) {
        for (resource, max_ratio_str) in max_ratio {
            if let (Some(limit_value), Some(request_value)) =
                (limits.get(resource), requests.get(resource))
            {
                let ratio_limit = max_ratio_str.parse::<f64>()?;

                if resource == "cpu" {
                    let limit = parse_cpu_to_millicores(limit_value)? as f64;
                    let request = parse_cpu_to_millicores(request_value)? as f64;
                    if request > 0.0 {
                        let actual_ratio = limit / request;
                        if actual_ratio > ratio_limit {
                            warn!(
                                "Container {} has CPU limit/request ratio {:.2} exceeding maximum {:.2}",
                                container_name, actual_ratio, ratio_limit
                            );
                            return Ok(false);
                        }
                    }
                } else if resource == "memory" {
                    let limit = parse_memory_to_bytes(limit_value)? as f64;
                    let request = parse_memory_to_bytes(request_value)? as f64;
                    if request > 0.0 {
                        let actual_ratio = limit / request;
                        if actual_ratio > ratio_limit {
                            warn!(
                                "Container {} has memory limit/request ratio {:.2} exceeding maximum {:.2}",
                                container_name, actual_ratio, ratio_limit
                            );
                            return Ok(false);
                        }
                    }
                }
            }
        }
    }

    Ok(true)
}

fn parse_cpu_to_millicores(cpu: &str) -> anyhow::Result<i64> {
    if cpu.ends_with('m') {
        let millis = cpu.trim_end_matches('m').parse::<i64>()?;
        Ok(millis)
    } else {
        let cores = cpu.parse::<f64>()?;
        Ok((cores * 1000.0) as i64)
    }
}

fn parse_memory_to_bytes(memory: &str) -> anyhow::Result<i64> {
    let memory = memory.trim();

    if memory.ends_with("Gi") {
        let value = memory.trim_end_matches("Gi").parse::<f64>()?;
        Ok((value * 1024.0 * 1024.0 * 1024.0) as i64)
    } else if memory.ends_with("Mi") {
        let value = memory.trim_end_matches("Mi").parse::<f64>()?;
        Ok((value * 1024.0 * 1024.0) as i64)
    } else if memory.ends_with("Ki") {
        let value = memory.trim_end_matches("Ki").parse::<f64>()?;
        Ok((value * 1024.0) as i64)
    } else if memory.ends_with("G") {
        let value = memory.trim_end_matches("G").parse::<f64>()?;
        Ok((value * 1000.0 * 1000.0 * 1000.0) as i64)
    } else if memory.ends_with("M") {
        let value = memory.trim_end_matches("M").parse::<f64>()?;
        Ok((value * 1000.0 * 1000.0) as i64)
    } else if memory.ends_with("K") {
        let value = memory.trim_end_matches("K").parse::<f64>()?;
        Ok((value * 1000.0) as i64)
    } else {
        Ok(memory.parse::<i64>()?)
    }
}

fn bytes_to_memory_string(bytes: i64) -> String {
    const GI: i64 = 1024 * 1024 * 1024;
    const MI: i64 = 1024 * 1024;
    const KI: i64 = 1024;

    if bytes >= GI && bytes % GI == 0 {
        format!("{}Gi", bytes / GI)
    } else if bytes >= MI && bytes % MI == 0 {
        format!("{}Mi", bytes / MI)
    } else if bytes >= KI && bytes % KI == 0 {
        format!("{}Ki", bytes / KI)
    } else {
        format!("{}", bytes)
    }
}

/// DefaultStorageClass admission controller - sets default storage class for PVCs
/// This is a built-in admission controller that:
/// 1. If a PVC doesn't specify storageClassName, sets it to the default StorageClass
/// 2. Finds the default StorageClass by checking for the annotation:
///    storageclass.kubernetes.io/is-default-class: "true"
pub async fn set_default_storage_class<S: Storage>(
    storage: &Arc<S>,
    pvc: &mut rusternetes_common::resources::PersistentVolumeClaim,
) -> anyhow::Result<()> {
    // Check if storageClassName is already set
    if pvc.spec.storage_class_name.is_some() {
        info!(
            "PVC {}/{} already has storageClassName set",
            pvc.metadata.namespace.as_deref().unwrap_or("default"),
            pvc.metadata.name
        );
        return Ok(());
    }

    // Find default storage class
    let sc_prefix = "/registry/storageclasses/";
    let storage_classes: Vec<rusternetes_common::resources::StorageClass> =
        storage.list(sc_prefix).await?;

    // Look for the default storage class (marked with annotation)
    for sc in storage_classes {
        if let Some(annotations) = &sc.metadata.annotations {
            if annotations.get("storageclass.kubernetes.io/is-default-class")
                == Some(&"true".to_string())
                || annotations.get("storageclass.beta.kubernetes.io/is-default-class")
                    == Some(&"true".to_string())
            {
                info!(
                    "Setting default storage class '{}' for PVC {}/{}",
                    sc.metadata.name,
                    pvc.metadata.namespace.as_deref().unwrap_or("default"),
                    pvc.metadata.name
                );
                pvc.spec.storage_class_name = Some(sc.metadata.name.clone());
                return Ok(());
            }
        }
    }

    info!(
        "No default storage class found for PVC {}/{}",
        pvc.metadata.namespace.as_deref().unwrap_or("default"),
        pvc.metadata.name
    );

    Ok(())
}

/// ServiceAccount admission controller - injects service account token volumes into pods
/// This is a built-in admission controller that:
/// 1. Sets serviceAccountName to "default" if not specified
/// 2. Injects a volume for the service account token secret
/// 3. Mounts the token at /var/run/secrets/kubernetes.io/serviceaccount/ in all containers
pub async fn inject_service_account_token<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    pod: &mut Pod,
) -> anyhow::Result<()> {
    let spec = match &mut pod.spec {
        Some(spec) => spec,
        None => return Ok(()), // No spec, nothing to inject
    };

    // Set service account name to "default" if not specified
    let sa_name = spec
        .service_account_name
        .clone()
        .unwrap_or_else(|| "default".to_string());

    if spec.service_account_name.is_none() {
        info!(
            "Setting default service account for pod {}/{}",
            namespace, pod.metadata.name
        );
        spec.service_account_name = Some(sa_name.clone());
    }

    // Look up the ServiceAccount to check its automount setting
    let sa_key = format!("/registry/serviceaccounts/{}/{}", namespace, sa_name);
    let sa_automount = match storage.get::<ServiceAccount>(&sa_key).await {
        Ok(sa) => sa.automount_service_account_token,
        Err(_) => {
            warn!(
                "Service account {}/{} does not exist, but proceeding with token injection",
                namespace, sa_name
            );
            None
        }
    };

    // Determine whether to mount the SA token.
    // Pod-level setting takes precedence over SA-level.
    let pod_automount = spec.automount_service_account_token;
    let should_mount = match pod_automount {
        Some(false) => false,                 // Pod explicitly disabled
        Some(true) => true,                   // Pod explicitly enabled
        None => sa_automount.unwrap_or(true), // Use SA setting, default true
    };

    if !should_mount {
        info!(
            "Skipping service account token injection for pod {}/{} - automountServiceAccountToken is false",
            namespace, pod.metadata.name
        );
        return Ok(());
    }

    // Inject the projected kube-api-access volume (token + ca.crt + namespace)
    // and mount it into every container. Shared with the controller-manager so
    // controller-created pods (ReplicaSet/StatefulSet/etc.) get the same volume
    // — they write pods straight to storage and bypass this HTTP admission path.
    rusternetes_common::serviceaccount::add_kube_api_access_volume(spec);

    info!(
        "Service account token injection complete for pod {}/{} using SA {}",
        namespace, pod.metadata.name, sa_name
    );

    Ok(())
}

/// Check if creating a resource would exceed ResourceQuota count limits.
/// Returns Ok(()) if allowed, Err with quota exceeded message if not.
pub async fn check_count_quota<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    resource_type: &str,
) -> Result<(), rusternetes_common::Error> {
    let quota_prefix = format!("/registry/resourcequotas/{}/", namespace);
    let quotas: Vec<ResourceQuota> = match storage.list(&quota_prefix).await {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, namespace, "failed to list resource quotas; skipping count quota check");
            return Ok(());
        }
    };

    for quota in &quotas {
        if let Some(hard) = &quota.spec.hard {
            // Check count/{resource_type} and {resource_type} limits
            let count_key = format!("count/{}", resource_type);
            for limit_key in [&count_key, &resource_type.to_string()] {
                if let Some(limit_str) = hard.get(limit_key.as_str()) {
                    let limit: i64 = limit_str.parse().unwrap_or(i64::MAX);
                    // Count current resources
                    let prefix = format!("/registry/{}/{}/", resource_type, namespace);
                    let current: Vec<serde_json::Value> = match storage.list(&prefix).await {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(error = %e, namespace, resource_type, "failed to list resources for quota check; treating as over-quota");
                            return Err(rusternetes_common::Error::Forbidden(format!(
                                "could not verify quota for {}: {}",
                                limit_key, e
                            )));
                        }
                    };
                    if current.len() as i64 >= limit {
                        return Err(rusternetes_common::Error::Forbidden(format!(
                            "exceeded quota: {}, requested: 1, used: {}, limited: {}",
                            limit_key,
                            current.len(),
                            limit_str
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

/// PodSecurityAdmission — stub for the Kubernetes Pod Security Admission
/// (PSA) plugin.
///
/// PSA replaced the now-removed PodSecurityPolicy (PSP) in v1.25. Each
/// namespace selects a Pod Security Standard via the
/// `pod-security.kubernetes.io/enforce` label (`privileged`, `baseline`, or
/// `restricted`) and the admission plugin rejects pods that violate the
/// standard.
///
/// This struct exists so the api-server can wire a single PSA admission
/// plugin into the pod create / update flow. The current `admit()`
/// implementation is intentionally an **allow-all** stub: a small surface
/// area we can grow into a full enforcer without touching every callsite.
///
/// Today, partial PSA enforcement (privileged, hostPID / hostNetwork /
/// hostIPC) lives inline in `handlers::pod::create_pod`. The longer-term
/// plan is to fold that logic — plus volume types, runAsUser, capabilities,
/// seccomp / AppArmor profiles, etc. — into [`PodSecurityAdmission::admit`].
///
/// Upstream references:
/// - <https://kubernetes.io/docs/concepts/security/pod-security-admission/>
/// - <https://kubernetes.io/docs/concepts/security/pod-security-standards/>
/// - `staging/src/k8s.io/pod-security-admission/admission/admission.go`
#[derive(Debug, Default, Clone, Copy)]
pub struct PodSecurityAdmission;

impl PodSecurityAdmission {
    /// Create a new PSA admission plugin instance.
    pub const fn new() -> Self {
        Self
    }

    /// Evaluate a pod against the namespace's enforced Pod Security
    /// Standard.
    ///
    /// Returns `Ok(())` to admit the pod, `Err(Forbidden)` to reject.
    ///
    /// Enforcement keys off the namespace's
    /// `pod-security.kubernetes.io/enforce` label. An absent label or
    /// `privileged` admits everything. `baseline` and `restricted` apply the
    /// baseline check set (no privileged containers, no host namespaces, no
    /// hostPath volumes); `restricted` additionally requires non-root
    /// execution and forbids privilege escalation.
    ///
    /// Upstream parity:
    /// `staging/src/k8s.io/pod-security-admission/policy/` (release-1.35).
    pub async fn admit<S: Storage>(
        &self,
        storage: &Arc<S>,
        namespace: &str,
        pod: &Pod,
    ) -> Result<(), rusternetes_common::Error> {
        let ns_key = rusternetes_storage::build_key("namespaces", None, namespace);
        let level = match storage
            .get::<rusternetes_common::resources::Namespace>(&ns_key)
            .await
        {
            Ok(ns) => ns
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("pod-security.kubernetes.io/enforce"))
                .cloned()
                .unwrap_or_else(|| "privileged".to_string()),
            // If the namespace can't be read, fall back to allow-all rather
            // than blocking pod creation on a storage hiccup.
            Err(_) => "privileged".to_string(),
        };

        let (baseline, restricted) = match level.as_str() {
            "restricted" => (true, true),
            "baseline" => (true, false),
            // "privileged" or any unknown level: admit everything.
            _ => (false, false),
        };

        if !baseline {
            return Ok(());
        }

        let Some(spec) = &pod.spec else {
            return Ok(());
        };
        let pod_name = &pod.metadata.name;

        // Iterator over every workload container (regular + init), so the
        // checks apply uniformly.
        let regular = spec.containers.iter();
        let init = spec.init_containers.iter().flatten();
        let all_security_contexts = regular
            .chain(init)
            .map(|c| (c.name.as_str(), c.security_context.as_ref()));

        // --- Baseline: privileged containers ---
        for (name, sc) in all_security_contexts.clone() {
            if let Some(sc) = sc {
                if sc.privileged == Some(true) {
                    return Err(rusternetes_common::Error::Forbidden(format!(
                        "pod {pod_name} violates PodSecurity \"{level}\": privileged \
                         (container \"{name}\" must not set securityContext.privileged=true)"
                    )));
                }
            }
        }

        // --- Baseline: host namespaces ---
        if spec.host_network == Some(true)
            || spec.host_pid == Some(true)
            || spec.host_ipc == Some(true)
        {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "pod {pod_name} violates PodSecurity \"{level}\": host namespaces \
                 (hostNetwork, hostPID, and hostIPC must be unset or false)"
            )));
        }

        // --- Baseline: hostPath volumes ---
        if let Some(volumes) = &spec.volumes {
            for v in volumes {
                if v.host_path.is_some() {
                    return Err(rusternetes_common::Error::Forbidden(format!(
                        "pod {pod_name} violates PodSecurity \"{level}\": hostPath volumes \
                         (volume \"{}\" uses a forbidden hostPath volume type)",
                        v.name
                    )));
                }
            }
        }

        if !restricted {
            return Ok(());
        }

        let pod_sc = spec.security_context.as_ref();
        let pod_run_as_non_root = pod_sc.and_then(|sc| sc.run_as_non_root);
        let pod_run_as_user = pod_sc.and_then(|sc| sc.run_as_user);

        // --- Restricted: runAsUser must not be 0 (root) ---
        if pod_run_as_user == Some(0) {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "pod {pod_name} violates PodSecurity \"{level}\": runAsUser=0 \
                 (pod must not set securityContext.runAsUser=0)"
            )));
        }
        for (name, sc) in all_security_contexts.clone() {
            if let Some(sc) = sc {
                if sc.run_as_user == Some(0) {
                    return Err(rusternetes_common::Error::Forbidden(format!(
                        "pod {pod_name} violates PodSecurity \"{level}\": runAsUser=0 \
                         (container \"{name}\" must not set securityContext.runAsUser=0)"
                    )));
                }
            }
        }

        // --- Restricted: runAsNonRoot must be true (silence is not consent) ---
        // Satisfied if the pod-level securityContext sets runAsNonRoot=true,
        // or every container sets it to true. A container with no explicit
        // value falls back to the pod-level value.
        if pod_run_as_non_root != Some(true) {
            for (name, sc) in all_security_contexts.clone() {
                let effective = sc.and_then(|sc| sc.run_as_non_root).or(pod_run_as_non_root);
                if effective != Some(true) {
                    return Err(rusternetes_common::Error::Forbidden(format!(
                        "pod {pod_name} violates PodSecurity \"{level}\": runAsNonRoot != true \
                         (pod or container \"{name}\" must set securityContext.runAsNonRoot=true)"
                    )));
                }
            }
        }

        // --- Restricted: allowPrivilegeEscalation must be false ---
        for (name, sc) in all_security_contexts {
            let allowed = sc.and_then(|sc| sc.allow_privilege_escalation);
            if allowed != Some(false) {
                return Err(rusternetes_common::Error::Forbidden(format!(
                    "pod {pod_name} violates PodSecurity \"{level}\": allowPrivilegeEscalation != false \
                     (container \"{name}\" must set securityContext.allowPrivilegeEscalation=false)"
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::ObjectMeta;

    fn make_pod(name: &str, cpu_request: Option<&str>, cpu_limit: Option<&str>) -> Pod {
        let mut resources = serde_json::Map::new();
        if let Some(cpu) = cpu_request {
            resources.insert("requests".to_string(), serde_json::json!({"cpu": cpu}));
        }
        if let Some(cpu) = cpu_limit {
            resources.insert("limits".to_string(), serde_json::json!({"cpu": cpu}));
        }
        let resources_json = if resources.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::Object(resources)
        };
        let pod_json = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name},
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "busybox",
                    "resources": resources_json
                }]
            }
        });
        serde_json::from_value(pod_json).unwrap()
    }

    #[test]
    fn test_is_pod_best_effort_no_resources() {
        let pod = make_pod("test", None, None);
        assert!(is_pod_best_effort(&pod));
    }

    #[test]
    fn test_is_pod_best_effort_with_requests() {
        let pod = make_pod("test", Some("100m"), None);
        assert!(!is_pod_best_effort(&pod));
    }

    #[test]
    fn test_is_pod_best_effort_with_limits() {
        let pod = make_pod("test", None, Some("200m"));
        assert!(!is_pod_best_effort(&pod));
    }

    #[test]
    fn test_pod_matches_quota_scopes_no_scopes() {
        let pod = make_pod("test", Some("100m"), None);
        let quota = ResourceQuota {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "v1".to_string(),
                kind: "ResourceQuota".to_string(),
            },
            metadata: ObjectMeta::new("quota"),
            spec: rusternetes_common::resources::ResourceQuotaSpec {
                hard: None,
                scopes: None,
                scope_selector: None,
            },
            status: None,
        };
        assert!(pod_matches_quota_scopes(&pod, &quota));
    }

    #[test]
    fn test_pod_matches_quota_scopes_best_effort_match() {
        let pod = make_pod("be", None, None);
        let quota = ResourceQuota {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "v1".to_string(),
                kind: "ResourceQuota".to_string(),
            },
            metadata: ObjectMeta::new("quota"),
            spec: rusternetes_common::resources::ResourceQuotaSpec {
                hard: None,
                scopes: Some(vec!["BestEffort".to_string()]),
                scope_selector: None,
            },
            status: None,
        };
        assert!(pod_matches_quota_scopes(&pod, &quota));
    }

    #[test]
    fn test_pod_matches_quota_scopes_best_effort_no_match() {
        let pod = make_pod("not-be", Some("100m"), None);
        let quota = ResourceQuota {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "v1".to_string(),
                kind: "ResourceQuota".to_string(),
            },
            metadata: ObjectMeta::new("quota"),
            spec: rusternetes_common::resources::ResourceQuotaSpec {
                hard: None,
                scopes: Some(vec!["BestEffort".to_string()]),
                scope_selector: None,
            },
            status: None,
        };
        assert!(!pod_matches_quota_scopes(&pod, &quota));
    }

    #[test]
    fn test_pod_matches_quota_scopes_not_terminating() {
        let pod = make_pod("test", Some("100m"), None);
        let quota = ResourceQuota {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "v1".to_string(),
                kind: "ResourceQuota".to_string(),
            },
            metadata: ObjectMeta::new("quota"),
            spec: rusternetes_common::resources::ResourceQuotaSpec {
                hard: None,
                scopes: Some(vec!["NotTerminating".to_string()]),
                scope_selector: None,
            },
            status: None,
        };
        // Pod without activeDeadlineSeconds is NotTerminating
        assert!(pod_matches_quota_scopes(&pod, &quota));
    }

    #[test]
    fn test_parse_cpu_to_millicores_various() {
        assert_eq!(parse_cpu_to_millicores("100m").unwrap(), 100);
        assert_eq!(parse_cpu_to_millicores("1").unwrap(), 1000);
        assert_eq!(parse_cpu_to_millicores("0.5").unwrap(), 500);
        assert_eq!(parse_cpu_to_millicores("250m").unwrap(), 250);
        assert_eq!(parse_cpu_to_millicores("2").unwrap(), 2000);
    }

    #[test]
    fn test_parse_memory_to_bytes_various() {
        assert_eq!(parse_memory_to_bytes("0").unwrap(), 0);
        assert_eq!(parse_memory_to_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_memory_to_bytes("1Ki").unwrap(), 1024);
        assert_eq!(parse_memory_to_bytes("1Mi").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_to_bytes("1Gi").unwrap(), 1024 * 1024 * 1024);
    }

    // ---- LimitRange admission tests ----

    fn make_limit_range(
        default_cpu: Option<&str>,
        default_request_cpu: Option<&str>,
        min_cpu: Option<&str>,
        max_cpu: Option<&str>,
    ) -> LimitRange {
        let mut default = HashMap::new();
        if let Some(v) = default_cpu {
            default.insert("cpu".to_string(), v.to_string());
        }
        let mut default_request = HashMap::new();
        if let Some(v) = default_request_cpu {
            default_request.insert("cpu".to_string(), v.to_string());
        }
        let mut min = HashMap::new();
        if let Some(v) = min_cpu {
            min.insert("cpu".to_string(), v.to_string());
        }
        let mut max = HashMap::new();
        if let Some(v) = max_cpu {
            max.insert("cpu".to_string(), v.to_string());
        }
        LimitRange {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "v1".to_string(),
                kind: "LimitRange".to_string(),
            },
            metadata: ObjectMeta::new("test-limit-range").with_namespace("default"),
            spec: rusternetes_common::resources::LimitRangeSpec {
                limits: vec![rusternetes_common::resources::LimitRangeItem {
                    item_type: "Container".to_string(),
                    default: if default.is_empty() {
                        None
                    } else {
                        Some(default)
                    },
                    default_request: if default_request.is_empty() {
                        None
                    } else {
                        Some(default_request)
                    },
                    min: if min.is_empty() { None } else { Some(min) },
                    max: if max.is_empty() { None } else { Some(max) },
                    max_limit_request_ratio: None,
                }],
            },
        }
    }

    #[tokio::test]
    async fn test_limit_range_applies_default_request_cpu() {
        // Conformance test scenario: LimitRange with default=500m, defaultRequest=300m
        // Pod with NO resources should get requests.cpu=300m, limits.cpu=500m
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("300m"), Some("100m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result, "LimitRange admission should pass");

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(
            requests.get("cpu").unwrap(),
            "300m",
            "requests.cpu should be 300m from defaultRequest, not from default limits"
        );
        assert_eq!(
            limits.get("cpu").unwrap(),
            "500m",
            "limits.cpu should be 500m from default"
        );
    }

    #[tokio::test]
    async fn test_limit_range_requests_fallback_to_limits_when_no_default_request() {
        // When defaultRequest is NOT set but default (limits) IS set,
        // requests should default to the limit value
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), None, None, None);
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(limits.get("cpu").unwrap(), "500m");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "500m",
            "requests.cpu should fall back to default limits (500m) when defaultRequest not set"
        );
    }

    #[tokio::test]
    async fn test_limit_range_does_not_override_explicit_requests() {
        // Container with explicit requests.cpu=200m should NOT be overridden by defaultRequest
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("300m"), Some("100m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", Some("200m"), None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "200m",
            "explicit requests.cpu=200m should not be overridden by defaultRequest=300m"
        );
    }

    #[tokio::test]
    async fn test_limit_range_limits_default_to_requests_when_unset() {
        // K8s rule: if limits are set (from LimitRange default) but container has no requests,
        // requests should default to the limits value
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        // Only default limits, no defaultRequest
        let lr = make_limit_range(Some("400m"), None, None, None);
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(limits.get("cpu").unwrap(), "400m");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "400m",
            "requests.cpu should default to limits.cpu when no defaultRequest"
        );
    }

    #[tokio::test]
    async fn test_limit_range_explicit_limits_override_default_request() {
        // Conformance scenario: pod has explicit limits.cpu=300m but no requests.cpu.
        // LimitRange has default=500m, defaultRequest=100m.
        // The pod has explicit limits.cpu=300m but no requests.cpu.
        // apply_limit_range only handles LimitRange defaults — the pod-level
        // limits→requests defaulting happens in the pod handler BEFORE this.
        // So apply_limit_range should set requests.cpu = 100m (from defaultRequest).
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("100m"), Some("50m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, Some("300m"));
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(
            limits.get("cpu").unwrap(),
            "300m",
            "explicit limits.cpu=300m should be preserved"
        );
        // Note: the pod handler does limits→requests defaulting BEFORE calling
        // apply_limit_range, so in production requests.cpu=300m. But this unit
        // test only calls apply_limit_range, which applies defaultRequest=100m.
        assert_eq!(
            requests.get("cpu").unwrap(),
            "100m",
            "apply_limit_range sets requests from defaultRequest (pod handler does limits->requests)"
        );
    }

    #[test]
    fn test_validate_max_rejects_over_limit_cpu() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "800m".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "800m CPU should exceed max of 500m");
    }

    #[test]
    fn test_validate_max_rejects_over_limit_memory() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("memory".to_string(), "1Gi".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("memory".to_string(), "500Mi".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "1Gi memory should exceed max of 500Mi");
    }

    #[test]
    fn test_validate_max_rejects_over_limit_ephemeral_storage() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("ephemeral-storage".to_string(), "2Gi".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("ephemeral-storage".to_string(), "1Gi".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "2Gi ephemeral-storage should exceed max of 1Gi");
    }

    #[test]
    fn test_validate_max_checks_requests_too() {
        let resources = ResourceRequirements {
            limits: None,
            requests: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "800m".to_string());
                m
            }),
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "800m CPU request should exceed max of 500m");
    }

    #[test]
    fn test_validate_max_allows_within_limit() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "400m".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(result, "400m CPU should be within max of 500m");
    }

    // ----- PodSecurityAdmission allow-case coverage -----
    //
    // The reject paths are pinned by the HTTP-level tests in
    // `tests/pod_security_admission_test.rs`. These unit tests cover the
    // admit (allow) paths the integration tests don't assert.

    async fn put_namespace<S: Storage>(storage: &Arc<S>, name: &str, enforce: Option<&str>) {
        let mut labels = std::collections::BTreeMap::new();
        if let Some(level) = enforce {
            labels.insert(
                "pod-security.kubernetes.io/enforce".to_string(),
                level.to_string(),
            );
        }
        let ns = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": name, "labels": labels },
        });
        let ns: rusternetes_common::resources::Namespace = serde_json::from_value(ns).unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, name);
        storage.create(&key, &ns).await.unwrap();
    }

    fn pod_from_spec(name: &str, spec: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": name },
            "spec": spec,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn psa_privileged_namespace_allows_privileged_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("privileged")).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "hostPID": true,
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": { "privileged": true },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit(&storage, "ns", &pod)
            .await
            .expect("privileged namespace must admit everything");
    }

    #[tokio::test]
    async fn psa_missing_enforce_label_allows() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", None).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": { "privileged": true },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit(&storage, "ns", &pod)
            .await
            .expect("absent enforce label must admit everything");
    }

    #[tokio::test]
    async fn psa_restricted_admits_compliant_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("restricted")).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "securityContext": { "runAsNonRoot": true },
                "volumes": [{ "name": "data", "emptyDir": {} }],
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 1000,
                        "allowPrivilegeEscalation": false,
                    },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit(&storage, "ns", &pod)
            .await
            .expect("compliant restricted pod must be admitted");
    }
}
