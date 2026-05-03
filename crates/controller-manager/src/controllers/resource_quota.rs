use anyhow::Result;
use rusternetes_common::resources::{Pod, ResourceQuota, ResourceQuotaStatus, Service};
use rusternetes_common::types::Phase;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

/// ResourceQuotaController tracks resource usage per namespace and enforces quota limits.
/// It:
/// 1. Watches ResourceQuotas across all namespaces
/// 2. Calculates current resource usage (pods, cpu, memory, etc.)
/// 3. Updates ResourceQuota status with used vs hard limits
pub struct ResourceQuotaController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> ResourceQuotaController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting ResourceQuota controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("resourcequotas", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    /// Main reconciliation loop - syncs all resource quotas
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("resourcequotas", Some(ns), name);
            match self.storage.get::<ResourceQuota>(&storage_key).await {
                Ok(resource) => match self.reconcile_quota(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<ResourceQuota>("/registry/resourcequotas/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("resourcequotas/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list resourcequotas for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting resource quota reconciliation");

        // List all resource quotas across all namespaces
        let quotas: Vec<ResourceQuota> = self
            .storage
            .list(&build_prefix("resourcequotas", None))
            .await?;

        for quota in quotas {
            if let Err(e) = self.reconcile_quota(&quota).await {
                error!(
                    "Failed to reconcile quota {}/{}: {}",
                    quota
                        .metadata
                        .namespace
                        .as_ref()
                        .unwrap_or(&"default".to_string()),
                    &quota.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    /// Reconcile a single resource quota
    async fn reconcile_quota(&self, quota: &ResourceQuota) -> Result<()> {
        let namespace = quota
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ResourceQuota has no namespace"))?;
        let quota_name = &quota.metadata.name;

        debug!("Reconciling quota {}/{}", namespace, quota_name);

        // Collect the set of resource names tracked by this quota
        let hard_keys: Vec<String> = quota
            .spec
            .hard
            .as_ref()
            .map(|h| h.keys().cloned().collect())
            .unwrap_or_default();

        // Calculate current resource usage in the namespace, respecting scopes
        let scopes = quota.spec.scopes.as_deref().unwrap_or(&[]);
        let scope_selector = quota.spec.scope_selector.as_ref();
        let mut used = self
            .calculate_usage(namespace, scopes, scope_selector, &hard_keys)
            .await?;

        // Ensure every key in hard also appears in used (default to "0")
        if let Some(hard) = &quota.spec.hard {
            for key in hard.keys() {
                used.entry(key.clone()).or_insert_with(|| "0".to_string());
            }
        }

        // Build the desired status
        let new_status = Some(ResourceQuotaStatus {
            hard: quota.spec.hard.clone(),
            used: Some(used),
        });

        // Only write if status actually changed to avoid unnecessary storage writes
        // that cause resourceVersion conflicts with concurrent test PATCH operations
        if quota.status != new_status {
            let key = build_key("resourcequotas", Some(namespace), quota_name);
            // Re-read for fresh resourceVersion to avoid CAS conflicts
            let mut updated_quota: ResourceQuota = match self.storage.get(&key).await {
                Ok(q) => q,
                Err(_) => quota.clone(),
            };
            // Check again after re-read (may have been updated concurrently)
            if updated_quota.status != new_status {
                updated_quota.status = new_status;
                self.storage.update(&key, &updated_quota).await?;
                debug!("Updated quota {}/{} status", namespace, quota_name);
            }
        }

        Ok(())
    }

    /// Check if a pod is BestEffort QoS class.
    /// A pod is BestEffort if NONE of its containers specify any resource requests or limits.
    fn is_pod_best_effort(pod: &Pod) -> bool {
        let spec = match &pod.spec {
            Some(s) => s,
            None => return true, // no spec = no resources = best effort
        };
        for container in &spec.containers {
            if let Some(resources) = &container.resources {
                // Check if there are actual non-empty requests or limits
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
        // Also check init containers
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

    /// Check if a pod matches the given scopes
    fn pod_matches_scopes(
        pod: &Pod,
        scopes: &[String],
        scope_selector: Option<&rusternetes_common::resources::ScopeSelector>,
    ) -> bool {
        let is_terminating = pod.metadata.deletion_timestamp.is_some()
            || pod
                .spec
                .as_ref()
                .and_then(|s| s.active_deadline_seconds)
                .is_some();
        let is_best_effort = Self::is_pod_best_effort(pod);

        // All scopes must match (AND logic)
        for scope in scopes {
            match scope.as_str() {
                "Terminating" => {
                    if !is_terminating {
                        return false;
                    }
                }
                "NotTerminating" => {
                    if is_terminating {
                        return false;
                    }
                }
                "BestEffort" => {
                    if !is_best_effort {
                        return false;
                    }
                }
                "NotBestEffort" => {
                    if is_best_effort {
                        return false;
                    }
                }
                _ => {}
            }
        }

        // Check scopeSelector if present (all match expressions must match, AND logic)
        if let Some(selector) = scope_selector {
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

    /// Calculate resource usage in a namespace, respecting quota scopes.
    /// Only counts resources that appear in `hard_keys` to avoid unnecessary work.
    async fn calculate_usage(
        &self,
        namespace: &str,
        scopes: &[String],
        scope_selector: Option<&rusternetes_common::resources::ScopeSelector>,
        hard_keys: &[String],
    ) -> Result<HashMap<String, String>> {
        let mut usage = HashMap::new();

        // Determine which resource categories we need
        let needs_pods = hard_keys.iter().any(|k| {
            k == "pods"
                || k.starts_with("requests.")
                || k.starts_with("limits.")
                || k == "cpu"
                || k == "memory"
                || k.starts_with("count/pods")
        });
        let needs_services = hard_keys.iter().any(|k| {
            k == "services"
                || k == "count/services"
                || k == "services.nodeports"
                || k == "services.loadbalancers"
        });
        let needs_configmaps = hard_keys
            .iter()
            .any(|k| k == "configmaps" || k == "count/configmaps");
        let needs_secrets = hard_keys
            .iter()
            .any(|k| k == "secrets" || k == "count/secrets");
        let needs_replicasets = hard_keys.iter().any(|k| {
            k == "count/replicasets" || k == "count/replicasets.apps" || k == "replicasets"
        });
        let needs_pvcs = hard_keys
            .iter()
            .any(|k| k == "persistentvolumeclaims" || k == "count/persistentvolumeclaims");
        let needs_rcs = hard_keys
            .iter()
            .any(|k| k == "replicationcontrollers" || k == "count/replicationcontrollers");
        let needs_rqs = hard_keys
            .iter()
            .any(|k| k == "resourcequotas" || k == "count/resourcequotas");

        // Count active pods (not Failed/Succeeded), filtered by scopes
        if needs_pods {
            let pod_prefix = format!("/registry/pods/{}/", namespace);
            let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;
            let pods: Vec<&Pod> = all_pods
                .iter()
                .filter(|p| {
                    let phase = p.status.as_ref().and_then(|s| s.phase.as_ref());
                    if matches!(phase, Some(Phase::Failed) | Some(Phase::Succeeded)) {
                        return false;
                    }
                    Self::pod_matches_scopes(p, scopes, scope_selector)
                })
                .collect();
            let pod_count = pods.len().to_string();
            usage.insert("pods".to_string(), pod_count.clone());
            usage.insert("count/pods".to_string(), pod_count);

            // Calculate CPU, memory, and ephemeral-storage requests/limits.
            // K8s tracks all three standard resource types in quota.
            // See: pkg/quota/v1/evaluator/core/pods.go — PodUsageFunc
            let mut total_cpu_requests = 0i64;
            let mut total_memory_requests = 0i64;
            let mut total_cpu_limits = 0i64;
            let mut total_memory_limits = 0i64;
            let mut total_ephemeral_storage_requests = 0i64;
            let mut total_ephemeral_storage_limits = 0i64;

            for pod in pods.iter() {
                if let Some(spec) = &pod.spec {
                    for container in &spec.containers {
                        if let Some(resources) = &container.resources {
                            if let Some(requests) = &resources.requests {
                                if let Some(cpu) = requests.get("cpu") {
                                    if let Ok(millis) = self.parse_cpu_to_millicores(cpu) {
                                        total_cpu_requests += millis;
                                    }
                                }
                                if let Some(memory) = requests.get("memory") {
                                    if let Ok(bytes) = self.parse_memory_to_bytes(memory) {
                                        total_memory_requests += bytes;
                                    }
                                }
                                if let Some(es) = requests.get("ephemeral-storage") {
                                    if let Ok(bytes) = self.parse_memory_to_bytes(es) {
                                        total_ephemeral_storage_requests += bytes;
                                    }
                                }
                            }
                            if let Some(limits) = &resources.limits {
                                if let Some(cpu) = limits.get("cpu") {
                                    if let Ok(millis) = self.parse_cpu_to_millicores(cpu) {
                                        total_cpu_limits += millis;
                                    }
                                }
                                if let Some(memory) = limits.get("memory") {
                                    if let Ok(bytes) = self.parse_memory_to_bytes(memory) {
                                        total_memory_limits += bytes;
                                    }
                                }
                                if let Some(es) = limits.get("ephemeral-storage") {
                                    if let Ok(bytes) = self.parse_memory_to_bytes(es) {
                                        total_ephemeral_storage_limits += bytes;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Track extended resource requests (non-cpu/memory).
            // K8s quota controller counts ALL resources specified in the quota spec.
            let mut extended_requests: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            for pod in pods.iter() {
                if let Some(spec) = &pod.spec {
                    for container in &spec.containers {
                        if let Some(resources) = &container.resources {
                            if let Some(requests) = &resources.requests {
                                for (res_name, qty) in requests {
                                    if res_name == "cpu" || res_name == "memory" {
                                        continue;
                                    }
                                    // Parse quantity (extended resources are integers)
                                    let val = qty.parse::<i64>().unwrap_or(0);
                                    *extended_requests.entry(res_name.clone()).or_insert(0) += val;
                                }
                            }
                        }
                    }
                }
            }
            for (res_name, total) in &extended_requests {
                usage.insert(format!("requests.{}", res_name), total.to_string());
            }

            // Always insert resource usage values (even when zero, K8s expects "0" in status.used)
            let cpu_req_str = format!("{}m", total_cpu_requests);
            usage.insert("requests.cpu".to_string(), cpu_req_str.clone());
            usage.insert("cpu".to_string(), cpu_req_str);

            let mem_req_str = self.bytes_to_memory_string(total_memory_requests);
            usage.insert("requests.memory".to_string(), mem_req_str.clone());
            usage.insert("memory".to_string(), mem_req_str);

            usage.insert("limits.cpu".to_string(), format!("{}m", total_cpu_limits));
            usage.insert(
                "limits.memory".to_string(),
                self.bytes_to_memory_string(total_memory_limits),
            );

            // Track ephemeral-storage — K8s tracks this alongside cpu/memory
            let es_req_str = self.bytes_to_memory_string(total_ephemeral_storage_requests);
            usage.insert("requests.ephemeral-storage".to_string(), es_req_str.clone());
            usage.insert("ephemeral-storage".to_string(), es_req_str);
            usage.insert(
                "limits.ephemeral-storage".to_string(),
                self.bytes_to_memory_string(total_ephemeral_storage_limits),
            );
        }

        // Count services and service subtypes
        if needs_services {
            let svc_prefix = format!("/registry/services/{}/", namespace);
            let services: Vec<Service> = self.storage.list(&svc_prefix).await.unwrap_or_default();
            usage.insert("count/services".to_string(), services.len().to_string());
            usage.insert("services".to_string(), services.len().to_string());

            // Count NodePort services (NodePort + LoadBalancer both use NodePorts)
            let nodeport_count = services
                .iter()
                .filter(|s| {
                    matches!(
                        s.spec.service_type,
                        Some(rusternetes_common::resources::ServiceType::NodePort)
                            | Some(rusternetes_common::resources::ServiceType::LoadBalancer)
                    )
                })
                .count();
            usage.insert("services.nodeports".to_string(), nodeport_count.to_string());

            // Count LoadBalancer services
            let lb_count = services
                .iter()
                .filter(|s| {
                    matches!(
                        s.spec.service_type,
                        Some(rusternetes_common::resources::ServiceType::LoadBalancer)
                    )
                })
                .count();
            usage.insert("services.loadbalancers".to_string(), lb_count.to_string());
        }

        if needs_configmaps {
            let count_prefix = format!("/registry/configmaps/{}/", namespace);
            let configmaps: Vec<serde_json::Value> =
                self.storage.list(&count_prefix).await.unwrap_or_default();
            usage.insert("count/configmaps".to_string(), configmaps.len().to_string());
            usage.insert("configmaps".to_string(), configmaps.len().to_string());
        }

        if needs_secrets {
            let secret_prefix = format!("/registry/secrets/{}/", namespace);
            let secrets: Vec<serde_json::Value> =
                self.storage.list(&secret_prefix).await.unwrap_or_default();
            usage.insert("count/secrets".to_string(), secrets.len().to_string());
            usage.insert("secrets".to_string(), secrets.len().to_string());
        }

        if needs_replicasets {
            let rs_prefix = format!("/registry/replicasets/{}/", namespace);
            let replicasets: Vec<serde_json::Value> =
                self.storage.list(&rs_prefix).await.unwrap_or_default();
            let rs_count = replicasets.len().to_string();
            usage.insert("count/replicasets".to_string(), rs_count.clone());
            usage.insert("count/replicasets.apps".to_string(), rs_count.clone());
            usage.insert("replicasets".to_string(), rs_count);
        }

        if needs_pvcs {
            let pvc_prefix = format!("/registry/persistentvolumeclaims/{}/", namespace);
            let pvcs: Vec<serde_json::Value> =
                self.storage.list(&pvc_prefix).await.unwrap_or_default();
            usage.insert("persistentvolumeclaims".to_string(), pvcs.len().to_string());
            usage.insert(
                "count/persistentvolumeclaims".to_string(),
                pvcs.len().to_string(),
            );
        }

        if needs_rcs {
            let rc_prefix = format!("/registry/replicationcontrollers/{}/", namespace);
            let rcs: Vec<serde_json::Value> =
                self.storage.list(&rc_prefix).await.unwrap_or_default();
            usage.insert("replicationcontrollers".to_string(), rcs.len().to_string());
            usage.insert(
                "count/replicationcontrollers".to_string(),
                rcs.len().to_string(),
            );
        }

        if needs_rqs {
            let rq_prefix = format!("/registry/resourcequotas/{}/", namespace);
            let rqs: Vec<serde_json::Value> =
                self.storage.list(&rq_prefix).await.unwrap_or_default();
            usage.insert("resourcequotas".to_string(), rqs.len().to_string());
            usage.insert("count/resourcequotas".to_string(), rqs.len().to_string());
        }

        Ok(usage)
    }

    /// Parse CPU string to millicores (e.g., "1" -> 1000, "500m" -> 500)
    fn parse_cpu_to_millicores(&self, cpu: &str) -> Result<i64> {
        if cpu.ends_with('m') {
            // Already in millicores
            let millis = cpu.trim_end_matches('m').parse::<i64>()?;
            Ok(millis)
        } else {
            // In cores, convert to millicores
            let cores = cpu.parse::<f64>()?;
            Ok((cores * 1000.0) as i64)
        }
    }

    /// Parse memory string to bytes (e.g., "1Gi" -> 1073741824, "512Mi" -> 536870912)
    fn parse_memory_to_bytes(&self, memory: &str) -> Result<i64> {
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
            // Assume bytes
            Ok(memory.parse::<i64>()?)
        }
    }

    /// Convert bytes to human-readable memory string
    fn bytes_to_memory_string(&self, bytes: i64) -> String {
        if bytes == 0 {
            return "0".to_string();
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        Container, PodSpec, ResourceQuotaSpec, ScopeSelector, ScopedResourceSelectorRequirement,
    };
    use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn test_parse_cpu_to_millicores() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage);

        assert_eq!(controller.parse_cpu_to_millicores("1").unwrap(), 1000);
        assert_eq!(controller.parse_cpu_to_millicores("500m").unwrap(), 500);
        assert_eq!(controller.parse_cpu_to_millicores("0.5").unwrap(), 500);
        assert_eq!(controller.parse_cpu_to_millicores("2").unwrap(), 2000);
    }

    #[test]
    fn test_parse_memory_to_bytes() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage);

        assert_eq!(controller.parse_memory_to_bytes("1Gi").unwrap(), 1073741824);
        assert_eq!(
            controller.parse_memory_to_bytes("512Mi").unwrap(),
            536870912
        );
        assert_eq!(controller.parse_memory_to_bytes("1024Ki").unwrap(), 1048576);
        assert_eq!(controller.parse_memory_to_bytes("1000").unwrap(), 1000);
    }

    #[test]
    fn test_bytes_to_memory_string() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage);

        assert_eq!(controller.bytes_to_memory_string(1073741824), "1Gi");
        assert_eq!(controller.bytes_to_memory_string(536870912), "512Mi");
        assert_eq!(controller.bytes_to_memory_string(1048576), "1Mi");
        assert_eq!(controller.bytes_to_memory_string(1024), "1Ki");
        assert_eq!(controller.bytes_to_memory_string(1000), "1000");
        assert_eq!(controller.bytes_to_memory_string(0), "0");
    }

    fn make_container(name: &str, resources: Option<ResourceRequirements>) -> Container {
        Container {
            name: name.to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources,
            volume_mounts: None,
            volume_devices: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            image_pull_policy: None,
            security_context: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            resize_policy: None,
            restart_policy: None,
        }
    }

    fn make_pod(name: &str, namespace: &str, resources: Option<ResourceRequirements>) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec: Some(PodSpec {
                containers: vec![make_container("test", resources)],
                ..Default::default()
            }),
            status: None,
        }
    }

    fn make_pod_with_deadline(name: &str, namespace: &str, active_deadline: Option<i64>) -> Pod {
        let mut pod = make_pod(name, namespace, None);
        if let Some(spec) = &mut pod.spec {
            spec.active_deadline_seconds = active_deadline;
        }
        pod
    }

    #[test]
    fn test_is_pod_best_effort() {
        // Pod with no resources is BestEffort
        let pod = make_pod("test", "default", None);
        assert!(ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));

        // Pod with empty resources is BestEffort
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: None,
                limits: None,
                claims: None,
            }),
        );
        assert!(ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));

        // Pod with empty maps is BestEffort
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(HashMap::new()),
                limits: Some(HashMap::new()),
                claims: None,
            }),
        );
        assert!(ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));

        // Pod with CPU request is NOT BestEffort
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        assert!(!ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));

        // Pod with only limits is NOT BestEffort
        let mut limits = HashMap::new();
        limits.insert("memory".to_string(), "128Mi".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: None,
                limits: Some(limits),
                claims: None,
            }),
        );
        assert!(!ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));
    }

    #[test]
    fn test_pod_matches_scopes_terminating() {
        // Pod with active_deadline_seconds is Terminating
        let pod = make_pod_with_deadline("test", "default", Some(30));
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["Terminating".to_string()],
                None
            )
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotTerminating".to_string()],
                None
            )
        );

        // Pod without active_deadline_seconds is NotTerminating
        let pod = make_pod_with_deadline("test", "default", None);
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["Terminating".to_string()],
                None
            )
        );
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotTerminating".to_string()],
                None
            )
        );
    }

    #[test]
    fn test_pod_matches_scopes_best_effort() {
        // Pod with no resources = BestEffort
        let pod = make_pod("test", "default", None);
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["BestEffort".to_string()],
                None
            )
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotBestEffort".to_string()],
                None
            )
        );

        // Pod with resources = NotBestEffort
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["BestEffort".to_string()],
                None
            )
        );
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotBestEffort".to_string()],
                None
            )
        );
    }

    #[test]
    fn test_pod_matches_scope_selector() {
        let pod = make_pod_with_deadline("test", "default", Some(30));
        let selector = ScopeSelector {
            match_expressions: vec![ScopedResourceSelectorRequirement {
                scope_name: "Terminating".to_string(),
                operator: "Exists".to_string(),
                values: None,
            }],
        };
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &[],
                Some(&selector)
            )
        );

        let selector_not = ScopeSelector {
            match_expressions: vec![ScopedResourceSelectorRequirement {
                scope_name: "Terminating".to_string(),
                operator: "DoesNotExist".to_string(),
                values: None,
            }],
        };
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &[],
                Some(&selector_not)
            )
        );
    }

    #[tokio::test]
    async fn test_calculate_usage_with_scopes() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        // Create a BestEffort pod (no resources)
        let pod1 = make_pod("be-pod", "test-ns", None);
        storage
            .create("/registry/pods/test-ns/be-pod", &pod1)
            .await
            .unwrap();

        // Create a NotBestEffort pod (has resources)
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod2 = make_pod(
            "nbe-pod",
            "test-ns",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        storage
            .create("/registry/pods/test-ns/nbe-pod", &pod2)
            .await
            .unwrap();

        // BestEffort scope should count only the BE pod
        let usage = controller
            .calculate_usage(
                "test-ns",
                &["BestEffort".to_string()],
                None,
                &["pods".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "1");

        // NotBestEffort scope should count only the NBE pod
        let usage = controller
            .calculate_usage(
                "test-ns",
                &["NotBestEffort".to_string()],
                None,
                &["pods".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "1");

        // No scope should count both
        let usage = controller
            .calculate_usage("test-ns", &[], None, &["pods".to_string()])
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "2");
    }

    #[tokio::test]
    async fn test_reconcile_quota_sets_status_used() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        // Create a quota
        let mut hard = HashMap::new();
        hard.insert("pods".to_string(), "10".to_string());
        hard.insert("requests.cpu".to_string(), "4".to_string());
        let quota = ResourceQuota {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-quota").with_namespace("test-ns"),
            spec: ResourceQuotaSpec {
                hard: Some(hard),
                scopes: None,
                scope_selector: None,
            },
            status: None,
        };
        storage
            .create("/registry/resourcequotas/test-ns/test-quota", &quota)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile_all().await.unwrap();

        // Check status was set
        let updated: ResourceQuota = storage
            .get("/registry/resourcequotas/test-ns/test-quota")
            .await
            .unwrap();
        let status = updated.status.unwrap();
        assert!(status.hard.is_some());
        assert!(status.used.is_some());
        let used = status.used.unwrap();
        assert_eq!(used.get("pods").unwrap(), "0");
        assert_eq!(used.get("requests.cpu").unwrap(), "0m");
    }
}
