use anyhow::Result;
use rusternetes_common::resources::volume::{
    NodeSelectorTerm, PersistentVolumeClaimPhase, PersistentVolumeClaimStatus,
    PersistentVolumePhase, PersistentVolumeReclaimPolicy, VolumeNodeAffinity,
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

        // Release pass: a Bound PV whose claim has been deleted must move on.
        if let Err(e) = self.release_dangling_pvs().await {
            error!("PV release pass failed: {}", e);
        }

        Ok(())
    }

    /// Reclaim pass mirroring the claim-not-found branch of upstream
    /// `pv_controller.syncVolume` + `reclaimVolume`
    /// (`pkg/controller/volume/persistentvolume/pv_controller.go`).
    ///
    /// A `Bound` PV whose `claimRef` names a PVC that no longer exists (or was
    /// recreated with a different UID) is transitioned to `Released`. Then the
    /// reclaim policy is applied: `Retain` (and `Recycle`, deprecated) leave the
    /// `Released` PV in place with its `claimRef` for manual recovery; `Delete`
    /// removes the PV object.
    async fn release_dangling_pvs(&self) -> Result<()> {
        let pvs: Vec<PersistentVolume> = self.storage.list("/registry/persistentvolumes/").await?;
        for mut pv in pvs {
            // Only act on currently-Bound volumes — never re-touch a PV that is
            // already Released/Failed/Available (upstream's same guard).
            if pv.status.as_ref().map(|s| &s.phase) != Some(&PersistentVolumePhase::Bound) {
                continue;
            }
            let Some(claim_ref) = pv.spec.claim_ref.clone() else {
                continue;
            };
            let (Some(ns), Some(name)) =
                (claim_ref.namespace.as_deref(), claim_ref.name.as_deref())
            else {
                continue;
            };

            // Is the bound claim still present with the same identity?
            let pvc_key = build_key("persistentvolumeclaims", Some(ns), name);
            let claim_present = match self.storage.get::<PersistentVolumeClaim>(&pvc_key).await {
                Ok(pvc) => match claim_ref.uid.as_deref() {
                    // A non-empty claimRef UID must match; a recreated PVC with a
                    // new UID means the originally-bound claim is gone.
                    Some(ref_uid) if !ref_uid.is_empty() => ref_uid == pvc.metadata.uid,
                    _ => true,
                },
                Err(_) => false,
            };
            if claim_present {
                continue;
            }

            pv.status = Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Released,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            });
            let pv_key = build_key("persistentvolumes", None, &pv.metadata.name);
            match pv.spec.persistent_volume_reclaim_policy {
                Some(PersistentVolumeReclaimPolicy::Delete) => {
                    self.storage.delete(&pv_key).await?;
                    info!(
                        "Reclaim(Delete): deleted released PV {} (claim {}/{} gone)",
                        pv.metadata.name, ns, name
                    );
                }
                _ => {
                    self.storage.update(&pv_key, &pv).await?;
                    info!(
                        "Released PV {} (claim {}/{} gone); reclaim policy retains it",
                        pv.metadata.name, ns, name
                    );
                }
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

        // Phase 1 (#1095): honor a pre-bound PV. A dynamically provisioned PV
        // carries a `claimRef` naming the exact PVC it was provisioned for, so
        // it must bind to THAT PVC and no other — otherwise two PVCs provisioned
        // at the same time cross-bind to each other's PV. A PV whose claimRef
        // points to this PVC was provisioned for us; one pointing elsewhere is
        // unavailable. The pre-bound PV already satisfies the request (it was
        // sized for it), so it wins over any unclaimed first-match candidate.
        if let Some(pv) = pvs.iter().find(|pv| {
            pv.spec
                .claim_ref
                .as_ref()
                .is_some_and(|cr| claim_ref_points_to(cr, namespace, pvc_name, &pvc.metadata.uid))
        }) {
            info!(
                "Completing pre-bound PVC {}/{} ↔ PV {} (claimRef)",
                namespace, pvc_name, pv.metadata.name
            );
            return self.complete_binding(pv.clone(), pvc).await;
        }

        // Phase 2: first-match among genuinely unclaimed PVs (static volumes /
        // legacy PVs with no claimRef).
        for pv in pvs {
            debug!("Checking PV {} (storage_class={:?}, capacity={:?}, access_modes={:?}, claim_ref={:?})",
                pv.metadata.name,
                pv.spec.storage_class_name,
                pv.spec.capacity,
                pv.spec.access_modes,
                pv.spec.claim_ref.is_some());

            // Skip if PV is already bound/pre-bound (a pre-bind to this PVC was
            // handled in phase 1; anything else belongs to another claim).
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
            return self.complete_binding(pv, pvc).await;
        }

        debug!("No matching PV found for PVC {}/{}", namespace, pvc_name);
        Ok(())
    }

    /// Complete a PVC↔PV binding: pin the PV's `claimRef` to this PVC, mark both
    /// Bound, and persist them. Shared by the pre-bound (claimRef) and the
    /// first-match (unclaimed) paths in [`Self::bind_pvc`].
    async fn complete_binding(
        &self,
        mut pv: PersistentVolume,
        pvc: &mut PersistentVolumeClaim,
    ) -> Result<()> {
        let namespace = pvc.metadata.namespace.clone().unwrap_or("default".into());
        let pvc_name = pvc.metadata.name.clone();

        let pv_access_modes = pv.spec.access_modes.clone();
        let pv_capacity = pv.spec.capacity.clone();
        let pv_name = pv.metadata.name.clone();

        // Pin the PV to this PVC (idempotent for an already pre-bound PV).
        pv.spec.claim_ref = Some(
            rusternetes_common::resources::service_account::ObjectReference {
                kind: Some("PersistentVolumeClaim".to_string()),
                namespace: Some(namespace.clone()),
                name: Some(pvc_name.clone()),
                uid: Some(pvc.metadata.uid.clone()),
                api_version: Some("v1".to_string()),
                resource_version: None,
                field_path: None,
            },
        );
        pv.status = Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Bound,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        });
        let pv_key = build_key("persistentvolumes", None, &pv_name);
        self.storage.update(&pv_key, &pv).await?;

        pvc.spec.volume_name = Some(pv_name.clone());
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
        let pvc_key = build_key("persistentvolumeclaims", Some(&namespace), &pvc_name);
        self.storage.update(&pvc_key, pvc).await?;

        info!(
            "Successfully bound PVC {}/{} to PV {}",
            namespace, pvc_name, pv_name
        );
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
/// True when a PV's `claimRef` names this exact PVC (namespace + name), with a
/// matching UID when the ref carries one. A pre-bound PV from the dynamic
/// provisioner sets all three; the UID guard prevents binding to a PV that was
/// pinned to an earlier, since-deleted PVC of the same name (#1095).
fn claim_ref_points_to(
    claim_ref: &rusternetes_common::resources::service_account::ObjectReference,
    namespace: &str,
    pvc_name: &str,
    pvc_uid: &str,
) -> bool {
    claim_ref.name.as_deref() == Some(pvc_name)
        && claim_ref.namespace.as_deref() == Some(namespace)
        && claim_ref.uid.as_ref().is_none_or(|u| u == pvc_uid)
}

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
    use rusternetes_common::resources::service_account::ObjectReference;
    use rusternetes_common::resources::volume::{
        PersistentVolumeAccessMode, PersistentVolumeClaimPhase, PersistentVolumeClaimStatus,
        ResourceRequirements,
    };
    use rusternetes_common::resources::{
        PersistentVolumeClaimSpec, PersistentVolumeSpec, PersistentVolumeStatus,
    };
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

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

    #[test]
    fn claim_ref_points_to_matches_name_namespace_uid() {
        let cr = ObjectReference {
            kind: Some("PersistentVolumeClaim".into()),
            namespace: Some("ns".into()),
            name: Some("pvc".into()),
            uid: Some("uid-1".into()),
            api_version: Some("v1".into()),
            resource_version: None,
            field_path: None,
        };
        assert!(claim_ref_points_to(&cr, "ns", "pvc", "uid-1"));
        assert!(!claim_ref_points_to(&cr, "ns", "other", "uid-1")); // wrong name
        assert!(!claim_ref_points_to(&cr, "other", "pvc", "uid-1")); // wrong ns
        assert!(!claim_ref_points_to(&cr, "ns", "pvc", "uid-2")); // stale uid
                                                                  // A pre-bind without a uid still counts.
        let cr_no_uid = ObjectReference {
            uid: None,
            ..cr.clone()
        };
        assert!(claim_ref_points_to(&cr_no_uid, "ns", "pvc", "uid-1"));
    }

    fn make_pvc(name: &str, uid: &str) -> PersistentVolumeClaim {
        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "1Gi".to_string());
        PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".into(),
                api_version: "v1".into(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some("sstest".into());
                m.uid = uid.into();
                m
            },
            spec: PersistentVolumeClaimSpec {
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                resources: ResourceRequirements {
                    limits: None,
                    requests: Some(requests),
                },
                volume_name: None,
                storage_class_name: Some("standard".into()),
                volume_mode: None,
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Pending,
                access_modes: None,
                capacity: None,
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            }),
        }
    }

    fn make_prebound_pv(pv_name: &str, pvc_name: &str, pvc_uid: &str) -> PersistentVolume {
        let mut capacity = HashMap::new();
        capacity.insert("storage".to_string(), "1Gi".to_string());
        PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".into(),
                api_version: "v1".into(),
            },
            metadata: ObjectMeta::new(pv_name),
            spec: PersistentVolumeSpec {
                capacity,
                host_path: None,
                nfs: None,
                iscsi: None,
                local: None,
                csi: None,
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                persistent_volume_reclaim_policy: None,
                storage_class_name: Some("standard".into()),
                mount_options: None,
                volume_mode: None,
                node_affinity: None,
                claim_ref: Some(ObjectReference {
                    kind: Some("PersistentVolumeClaim".into()),
                    namespace: Some("sstest".into()),
                    name: Some(pvc_name.into()),
                    uid: Some(pvc_uid.into()),
                    api_version: Some("v1".into()),
                    resource_version: None,
                    field_path: None,
                }),
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Available,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            }),
        }
    }

    /// #1095: two PVCs whose PVs were pre-bound (claimRef) by the dynamic
    /// provisioner must each bind to THEIR OWN PV, never cross. PVs are stored
    /// in the reverse of the bind order so a first-match binder would mis-pair.
    #[tokio::test]
    async fn prebound_pvs_do_not_cross_bind() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PVBinderController::new(storage.clone());

        let pv_a = make_prebound_pv("pvc-sstest-explicit-pvc", "explicit-pvc", "uid-a");
        let pv_b = make_prebound_pv("pvc-sstest-default-pvc", "default-pvc", "uid-b");
        // Insert b first, a second — list order must not decide the pairing.
        storage
            .create(
                &build_key("persistentvolumes", None, &pv_b.metadata.name),
                &pv_b,
            )
            .await
            .unwrap();
        storage
            .create(
                &build_key("persistentvolumes", None, &pv_a.metadata.name),
                &pv_a,
            )
            .await
            .unwrap();

        let mut explicit = make_pvc("explicit-pvc", "uid-a");
        let mut default = make_pvc("default-pvc", "uid-b");
        // PVCs exist in storage (created by the user) before binding; the binder
        // persists the bound result via update().
        for pvc in [&explicit, &default] {
            storage
                .create(
                    &build_key(
                        "persistentvolumeclaims",
                        pvc.metadata.namespace.as_deref(),
                        &pvc.metadata.name,
                    ),
                    pvc,
                )
                .await
                .unwrap();
        }

        controller.bind_pvc(&mut explicit).await.unwrap();
        assert_eq!(
            explicit.spec.volume_name.as_deref(),
            Some("pvc-sstest-explicit-pvc"),
            "explicit-pvc must bind to its own pre-bound PV"
        );

        controller.bind_pvc(&mut default).await.unwrap();
        assert_eq!(
            default.spec.volume_name.as_deref(),
            Some("pvc-sstest-default-pvc"),
            "default-pvc must bind to its own pre-bound PV, not explicit-pvc's"
        );

        // Each PV's claimRef ends up pinned to the matching PVC.
        let bound_a: PersistentVolume = storage
            .get(&build_key(
                "persistentvolumes",
                None,
                "pvc-sstest-explicit-pvc",
            ))
            .await
            .unwrap();
        assert_eq!(
            bound_a.spec.claim_ref.unwrap().name.as_deref(),
            Some("explicit-pvc")
        );
    }
}
