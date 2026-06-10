use anyhow::Result;
use rusternetes_common::resources::volume::{
    NodeSelectorTerm, PersistentVolumeClaimPhase, PersistentVolumeClaimStatus,
    PersistentVolumePhase, VolumeNodeAffinity,
};
use rusternetes_common::resources::{
    Node, PersistentVolume, PersistentVolumeClaim, PersistentVolumeStatus,
};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

pub struct PVBinderController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> PVBinderController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting PV/PVC Binder Controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("persistentvolumeclaims", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    time::sleep(Duration::from_secs(5)).await;
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
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("persistentvolumeclaims", Some(ns), name);
            match self
                .storage
                .get::<PersistentVolumeClaim>(&storage_key)
                .await
            {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.bind_pvc(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<PersistentVolumeClaim>("/registry/persistentvolumeclaims/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("persistentvolumeclaims/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list persistentvolumeclaims for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        // Get all PVCs
        let pvcs: Vec<PersistentVolumeClaim> = self
            .storage
            .list("/registry/persistentvolumeclaims/")
            .await?;

        for mut pvc in pvcs {
            if let Err(e) = self.bind_pvc(&mut pvc).await {
                error!("Failed to bind PVC {}: {}", pvc.metadata.name, e);
            }
        }

        Ok(())
    }

    async fn bind_pvc(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let pvc_name = &pvc.metadata.name;
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");

        // Skip if already bound
        if pvc.spec.volume_name.is_some() {
            return Ok(());
        }

        let pvc_spec = &pvc.spec;

        debug!("Looking for PV to bind to PVC {}/{}", namespace, pvc_name);
        debug!(
            "PVC requirements: storage_class={:?}, capacity={:?}, access_modes={:?}",
            pvc_spec.storage_class_name,
            pvc_spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
            pvc_spec.access_modes
        );

        // Get all available PVs
        let pvs: Vec<PersistentVolume> = self.storage.list("/registry/persistentvolumes/").await?;

        debug!("Found {} PVs to check for binding", pvs.len());

        // Lazily-loaded node inventory, only fetched when a candidate PV
        // actually declares node affinity (upstream `volume_scheduling`).
        let mut nodes_cache: Option<Vec<Node>> = None;

        // Find a matching available PV
        for mut pv in pvs {
            debug!("Checking PV {} (storage_class={:?}, capacity={:?}, access_modes={:?}, claim_ref={:?})",
                pv.metadata.name,
                pv.spec.storage_class_name,
                pv.spec.capacity,
                pv.spec.access_modes,
                pv.spec.claim_ref.is_some());

            // Skip if PV is already bound
            if pv.spec.claim_ref.is_some() {
                continue;
            }

            // Check if PV matches PVC requirements
            let matches = self.pv_matches_pvc(&pv.spec, pvc_spec);
            debug!(
                "PV {} matches PVC requirements: {}",
                pv.metadata.name, matches
            );
            if !matches {
                continue;
            }

            // Honor the PV's required node affinity (upstream parity with
            // `pkg/controller/volume/scheduling`): a PV constrained to a node
            // topology must not bind unless at least one node in the cluster
            // satisfies that constraint. Otherwise the bound volume could never
            // be mounted by any pod.
            if let Some(node_affinity) = pv.spec.node_affinity.as_ref() {
                if node_affinity.required.is_some() {
                    let nodes = match nodes_cache.as_ref() {
                        Some(n) => n,
                        None => {
                            let listed: Vec<Node> = self.storage.list("/registry/nodes/").await?;
                            nodes_cache = Some(listed);
                            nodes_cache.as_ref().unwrap()
                        }
                    };
                    if !node_affinity_satisfied(node_affinity, nodes) {
                        debug!(
                            "Skipping PV {}: required nodeAffinity matches no node",
                            pv.metadata.name
                        );
                        continue;
                    }
                }
            }

            info!(
                "Binding PVC {}/{} to PV {}",
                namespace, pvc_name, pv.metadata.name
            );

            // Clone values we need before mutating pv
            let pv_access_modes = pv.spec.access_modes.clone();
            let pv_capacity = pv.spec.capacity.clone();
            let pv_name = pv.metadata.name.clone();

            // Bind PV to PVC
            pv.spec.claim_ref = Some(
                rusternetes_common::resources::service_account::ObjectReference {
                    kind: Some("PersistentVolumeClaim".to_string()),
                    namespace: Some(namespace.to_string()),
                    name: Some(pvc_name.to_string()),
                    uid: Some(pvc.metadata.uid.clone()),
                    api_version: Some("v1".to_string()),
                    resource_version: None,
                    field_path: None,
                },
            );

            // Update PV status to Bound
            pv.status = Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Bound,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            });

            let pv_key = build_key("persistentvolumes", None, &pv_name);
            self.storage.update(&pv_key, &pv).await?;

            // Bind PVC to PV
            pvc.spec.volume_name = Some(pv_name.clone());

            // Update PVC status to Bound
            pvc.status = Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Bound,
                access_modes: Some(pv_access_modes),
                capacity: Some(pv_capacity),
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            });

            let pvc_key = build_key("persistentvolumeclaims", Some(namespace), pvc_name);
            self.storage.update(&pvc_key, pvc).await?;

            info!(
                "Successfully bound PVC {}/{} to PV {}",
                namespace, pvc_name, pv.metadata.name
            );
            return Ok(());
        }

        debug!("No matching PV found for PVC {}/{}", namespace, pvc_name);
        Ok(())
    }

    /// Check if a PV matches the requirements of a PVC
    fn pv_matches_pvc(
        &self,
        pv_spec: &rusternetes_common::resources::PersistentVolumeSpec,
        pvc_spec: &rusternetes_common::resources::PersistentVolumeClaimSpec,
    ) -> bool {
        // Check storage class match
        if let (Some(pv_class), Some(pvc_class)) =
            (&pv_spec.storage_class_name, &pvc_spec.storage_class_name)
        {
            if pv_class != pvc_class {
                return false;
            }
        }

        // Check capacity
        if let (Some(pv_storage), Some(pvc_storage)) = (
            pv_spec.capacity.get("storage"),
            pvc_spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
        ) {
            // Simple string comparison - in real Kubernetes, this would parse quantities
            // For now, we'll just check if PV storage >= PVC storage
            if !self.storage_sufficient(pv_storage, pvc_storage) {
                return false;
            }
        }

        // Check access modes - PV must support all modes requested by PVC
        for pvc_mode in &pvc_spec.access_modes {
            if !pv_spec.access_modes.contains(pvc_mode) {
                return false;
            }
        }

        true
    }

    /// Check if PV storage is sufficient for PVC
    /// This is a simple string comparison for now
    fn storage_sufficient(&self, pv_storage: &str, pvc_storage: &str) -> bool {
        // Parse the numeric part and unit from storage strings like "10Gi", "5Gi"
        let parse_storage = |s: &str| -> Option<(f64, String)> {
            let numeric_end = s.chars().position(|c| !c.is_numeric() && c != '.')?;
            let (num_str, unit) = s.split_at(numeric_end);
            let num = num_str.parse::<f64>().ok()?;
            Some((num, unit.to_string()))
        };

        match (parse_storage(pv_storage), parse_storage(pvc_storage)) {
            (Some((pv_num, pv_unit)), Some((pvc_num, pvc_unit))) => {
                // Units must match
                if pv_unit != pvc_unit {
                    debug!(
                        "Storage units don't match: PV has {}, PVC needs {}",
                        pv_unit, pvc_unit
                    );
                    return false;
                }
                // PV must have at least as much storage as PVC
                let sufficient = pv_num >= pvc_num;
                debug!(
                    "Storage comparison: PV has {}{}, PVC needs {}{} -> sufficient: {}",
                    pv_num, pv_unit, pvc_num, pvc_unit, sufficient
                );
                sufficient
            }
            _ => {
                debug!(
                    "Failed to parse storage values: PV='{}', PVC='{}'",
                    pv_storage, pvc_storage
                );
                // Fall back to string comparison if parsing fails
                pv_storage >= pvc_storage
            }
        }
    }
}

/// True if at least one node satisfies the PV's required node affinity.
///
/// `required.nodeSelectorTerms` are ORed; within a term every match expression
/// and match field must hold (AND). Mirrors upstream
/// `pkg/apis/core/v1/helper.MatchNodeSelectorTerms` semantics for the operators
/// rusternetes models.
fn node_affinity_satisfied(node_affinity: &VolumeNodeAffinity, nodes: &[Node]) -> bool {
    let required = match &node_affinity.required {
        Some(r) => r,
        None => return true,
    };
    nodes.iter().any(|node| {
        required
            .node_selector_terms
            .iter()
            .any(|term| volume_term_matches(node, term))
    })
}

/// A single node-selector term matches when all of its match expressions (over
/// node labels) and match fields (over `metadata.name`) hold.
fn volume_term_matches(node: &Node, term: &NodeSelectorTerm) -> bool {
    let labels = node.metadata.labels.as_ref();
    if let Some(exprs) = term.match_expressions.as_ref() {
        for req in exprs {
            let value = labels.and_then(|l| l.get(&req.key)).map(|s| s.as_str());
            if !requirement_matches(value, &req.operator, req.values.as_deref()) {
                return false;
            }
        }
    }
    if let Some(fields) = term.match_fields.as_ref() {
        for req in fields {
            let value = match req.key.as_str() {
                "metadata.name" => Some(node.metadata.name.as_str()),
                _ => None,
            };
            if !requirement_matches(value, &req.operator, req.values.as_deref()) {
                return false;
            }
        }
    }
    true
}

/// Evaluate one selector requirement against a (possibly absent) node value.
fn requirement_matches(value: Option<&str>, operator: &str, values: Option<&[String]>) -> bool {
    let values = values.unwrap_or(&[]);
    match operator {
        "In" => value
            .map(|v| values.iter().any(|x| x == v))
            .unwrap_or(false),
        "NotIn" => value
            .map(|v| !values.iter().any(|x| x == v))
            .unwrap_or(true),
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "Gt" | "Lt" => {
            let (node_val, req_val) = match (value, values.first()) {
                (Some(v), Some(r)) => match (v.parse::<i64>(), r.parse::<i64>()) {
                    (Ok(a), Ok(b)) => (a, b),
                    _ => return false,
                },
                _ => return false,
            };
            if operator == "Gt" {
                node_val > req_val
            } else {
                node_val < req_val
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn test_storage_comparison() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PVBinderController::new(storage);

        assert!(controller.storage_sufficient("10Gi", "5Gi"));
        assert!(controller.storage_sufficient("10Gi", "10Gi"));
        assert!(!controller.storage_sufficient("5Gi", "10Gi"));
        assert!(controller.storage_sufficient("100Mi", "50Mi"));
        assert!(!controller.storage_sufficient("50Mi", "100Mi"));
    }
}
