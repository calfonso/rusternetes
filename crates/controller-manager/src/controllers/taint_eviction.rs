use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// TaintEvictionController watches for NoExecute taints on nodes and evicts
/// pods that don't tolerate them. This implements the node lifecycle controller's
/// taint-based eviction behavior.
pub struct TaintEvictionController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> TaintEvictionController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Watch-based run loop. Watches nodes as primary resource.
    /// Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("nodes", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish watch: {}, retrying", e);
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
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let name = key.strip_prefix("nodes/").unwrap_or(&key);
            let storage_key = build_key("nodes", None, name);
            match self.storage.get::<Node>(&storage_key).await {
                Ok(resource) => match self.reconcile_node(&resource).await {
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
        match self.storage.list::<Node>("/registry/nodes/").await {
            Ok(items) => {
                for item in &items {
                    let key = format!("nodes/{}", item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list nodes for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting taint eviction reconciliation");

        let nodes: Vec<Node> = self.storage.list("/registry/nodes/").await?;

        for node in &nodes {
            if let Err(e) = self.reconcile_node(node).await {
                warn!(
                    "Taint eviction error for node {}: {}",
                    node.metadata.name, e
                );
            }
        }

        Ok(())
    }

    async fn reconcile_node(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;

        // Get NoExecute taints on this node
        let no_execute_taints: Vec<&rusternetes_common::resources::Taint> = node
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map(|taints| taints.iter().filter(|t| t.effect == "NoExecute").collect())
            .unwrap_or_default();

        if no_execute_taints.is_empty() {
            return Ok(());
        }

        // Find all pods on this node
        let all_pods: Vec<Pod> = self
            .storage
            .list::<Pod>("/registry/pods/")
            .await
            .unwrap_or_default();

        let node_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|pod| pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(node_name))
            .collect();

        let now = Utc::now();

        for pod in node_pods {
            // Check each NoExecute taint against pod tolerations
            let mut should_evict = false;

            for taint in &no_execute_taints {
                match self.pod_toleration_for_taint(pod, taint) {
                    TolerationResult::NotTolerated => {
                        // Pod doesn't tolerate this taint — evict immediately
                        should_evict = true;
                        break;
                    }
                    TolerationResult::ToleratedForever => {
                        // Pod tolerates this taint indefinitely — skip
                    }
                    TolerationResult::ToleratedFor(seconds) => {
                        // Pod tolerates for a limited time — check if time expired
                        // Use taint's timeAdded or the pod's creation time as baseline
                        let taint_added = taint
                            .time_added
                            .or(pod.metadata.creation_timestamp)
                            .unwrap_or(now);
                        let elapsed = (now - taint_added).num_seconds();
                        if elapsed >= seconds {
                            should_evict = true;
                            break;
                        }
                    }
                }
            }

            if should_evict {
                let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
                let pod_name = &pod.metadata.name;

                // Skip already-deleting pods
                if pod.metadata.deletion_timestamp.is_some() {
                    continue;
                }

                // Skip system pods (kube-system namespace)
                if namespace == "kube-system" {
                    debug!("Skipping eviction of system pod {}/{}", namespace, pod_name);
                    continue;
                }

                info!(
                    "Evicting pod {}/{} from node {} due to NoExecute taint",
                    namespace, pod_name, node_name
                );

                // K8s taint eviction: add DisruptionTarget condition then set
                // deletionTimestamp (graceful delete). The kubelet handles shutdown.
                // Do NOT delete directly from storage — that bypasses graceful
                // shutdown and causes "pod not found" errors in tests.
                let key = build_key("pods", Some(namespace), pod_name);
                match self.storage.get::<Pod>(&key).await {
                    Ok(mut evict_pod) => {
                        // Add DisruptionTarget condition
                        let condition = rusternetes_common::resources::PodCondition {
                            condition_type: "DisruptionTarget".to_string(),
                            status: "True".to_string(),
                            reason: Some("DeletionByTaintManager".to_string()),
                            message: Some(
                                "Taint manager: deleting due to NoExecute taint".to_string(),
                            ),
                            last_transition_time: Some(Utc::now()),
                            observed_generation: None,
                        };
                        if let Some(ref mut status) = evict_pod.status {
                            let conditions = status.conditions.get_or_insert_with(Vec::new);
                            conditions.push(condition);
                        }
                        // Set deletionTimestamp for graceful delete
                        evict_pod.metadata.deletion_timestamp = Some(Utc::now());
                        let _ = self.storage.update(&key, &evict_pod).await;
                        info!(
                            "Evicted pod {}/{} from node {} (set deletionTimestamp)",
                            namespace, pod_name, node_name
                        );
                    }
                    Err(rusternetes_common::Error::NotFound(_)) => {
                        // Already deleted
                    }
                    Err(e) => {
                        warn!("Failed to evict pod {}/{}: {}", namespace, pod_name, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Check how a pod tolerates a specific taint
    fn pod_toleration_for_taint(
        &self,
        pod: &Pod,
        taint: &rusternetes_common::resources::Taint,
    ) -> TolerationResult {
        let tolerations = match pod.spec.as_ref().and_then(|s| s.tolerations.as_ref()) {
            Some(t) => t,
            None => return TolerationResult::NotTolerated,
        };

        for toleration in tolerations {
            // Check if this toleration matches the taint
            if !self.toleration_matches_taint(toleration, taint) {
                continue;
            }

            // Toleration matches — check if it has a time limit
            if let Some(seconds) = toleration.toleration_seconds {
                return TolerationResult::ToleratedFor(seconds);
            }

            return TolerationResult::ToleratedForever;
        }

        TolerationResult::NotTolerated
    }

    fn toleration_matches_taint(
        &self,
        toleration: &rusternetes_common::resources::Toleration,
        taint: &rusternetes_common::resources::Taint,
    ) -> bool {
        // Empty key with Exists operator matches all taints
        if toleration.key.as_ref().is_none_or(|k| k.is_empty())
            && toleration.operator.as_deref() == Some("Exists")
        {
            return true;
        }

        // Key must match
        let key_matches = toleration.key.as_ref() == Some(&taint.key);
        if !key_matches {
            return false;
        }

        // Effect must match (or be empty which matches all effects)
        let effect_matches = toleration
            .effect
            .as_ref()
            .is_none_or(|e| e.is_empty() || e == &taint.effect);
        if !effect_matches {
            return false;
        }

        // Operator check
        match toleration.operator.as_deref() {
            Some("Exists") => true,
            Some("Equal") | None => {
                let taint_value = taint.value.as_deref().unwrap_or("");
                let toleration_value = toleration.value.as_deref().unwrap_or("");
                taint_value == toleration_value
            }
            _ => false,
        }
    }
}

enum TolerationResult {
    /// Pod doesn't tolerate this taint
    NotTolerated,
    /// Pod tolerates this taint indefinitely
    ToleratedForever,
    /// Pod tolerates this taint for N seconds
    ToleratedFor(i64),
}
