use anyhow::{Context, Result};
use rusternetes_common::resources::volume::{
    HostPathType, HostPathVolumeSource, PersistentVolumePhase, PersistentVolumeReclaimPolicy,
};
use rusternetes_common::resources::{
    PersistentVolume, PersistentVolumeClaim, PersistentVolumeStatus, StorageClass, VolumeSnapshot,
    VolumeSnapshotContent,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

pub struct DynamicProvisionerController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> DynamicProvisionerController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting Dynamic Provisioner Controller");

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
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
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
                Ok(pvc) => {
                    // Only process unbound PVCs with a storage class
                    if pvc.spec.volume_name.is_none() && pvc.spec.storage_class_name.is_some() {
                        match self.provision_volume(&pvc).await {
                            Ok(()) => queue.forget(&key).await,
                            Err(e) => {
                                error!("Failed to provision volume for {}: {}", key, e);
                                queue.requeue_rate_limited(key.clone()).await;
                            }
                        }
                    } else {
                        queue.forget(&key).await;
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

        for pvc in pvcs {
            // Only process unbound PVCs with a storage class
            if pvc.spec.volume_name.is_none() && pvc.spec.storage_class_name.is_some() {
                if let Err(e) = self.provision_volume(&pvc).await {
                    error!(
                        "Failed to provision volume for PVC {}/{}: {}",
                        pvc.metadata.namespace.as_deref().unwrap_or("default"),
                        pvc.metadata.name,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    async fn provision_volume(&self, pvc: &PersistentVolumeClaim) -> Result<()> {
        let pvc_name = &pvc.metadata.name;
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");

        let storage_class_name = pvc
            .spec
            .storage_class_name
            .as_ref()
            .context("PVC has no storage class name")?;

        debug!(
            "Attempting to dynamically provision volume for PVC {}/{} using StorageClass {}",
            namespace, pvc_name, storage_class_name
        );

        // Get the StorageClass
        let sc_key = build_key("storageclasses", None, storage_class_name);
        let storage_class: StorageClass = self
            .storage
            .get(&sc_key)
            .await
            .with_context(|| format!("StorageClass {} not found", storage_class_name))?;

        debug!(
            "Found StorageClass {} with provisioner {}",
            storage_class_name, storage_class.provisioner
        );

        // Check if provisioner is supported
        if !self.is_provisioner_supported(&storage_class.provisioner) {
            warn!(
                "Provisioner {} is not supported. Skipping PVC {}/{}",
                storage_class.provisioner, namespace, pvc_name
            );
            return Ok(());
        }

        // Check if a PV already exists for this PVC (in case we're retrying)
        let pv_name = format!("pvc-{}-{}", namespace, pvc_name);
        let pv_key = build_key("persistentvolumes", None, &pv_name);

        if let Ok(_existing_pv) = self.storage.get::<PersistentVolume>(&pv_key).await {
            debug!(
                "PV {} already exists for PVC {}/{}",
                pv_name, namespace, pvc_name
            );
            return Ok(());
        }

        // Create the PV (with snapshot restore if dataSource is specified)
        let pv = self
            .create_pv_for_pvc(&storage_class, pvc, &pv_name)
            .await?;

        // Store the PV
        self.storage
            .create(&pv_key, &pv)
            .await
            .with_context(|| format!("Failed to create PV {}", pv_name))?;

        info!(
            "Successfully provisioned PV {} for PVC {}/{}",
            pv_name, namespace, pvc_name
        );

        Ok(())
    }

    fn is_provisioner_supported(&self, provisioner: &str) -> bool {
        matches!(
            provisioner,
            "rusternetes.io/hostpath" | "kubernetes.io/hostpath" | "hostpath"
        )
    }

    async fn create_pv_for_pvc(
        &self,
        storage_class: &StorageClass,
        pvc: &PersistentVolumeClaim,
        pv_name: &str,
    ) -> Result<PersistentVolume> {
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");

        // Get requested storage capacity
        let requested_storage = pvc
            .spec
            .resources
            .requests
            .as_ref()
            .and_then(|r| r.get("storage"))
            .context("PVC has no storage request")?;

        let mut capacity = HashMap::new();
        capacity.insert("storage".to_string(), requested_storage.clone());

        // Determine the path for the volume
        let base_path = storage_class
            .parameters
            .as_ref()
            .and_then(|p| p.get("path"))
            .map(|s| s.as_str())
            .unwrap_or("/tmp/rusternetes/dynamic-pvs");

        let volume_path = format!("{}/{}", base_path, pv_name);

        // Check if this PVC is being restored from a snapshot
        let snapshot_source_path = if let Some(data_source) = &pvc.spec.data_source {
            self.handle_snapshot_restore(data_source, namespace, &volume_path)
                .await?
        } else {
            None
        };

        let message = if snapshot_source_path.is_some() {
            Some("Dynamically provisioned from snapshot".to_string())
        } else {
            Some("Dynamically provisioned".to_string())
        };

        info!(
            "Creating PV {} with path {} and capacity {}{}",
            pv_name,
            volume_path,
            requested_storage,
            if snapshot_source_path.is_some() {
                " (restored from snapshot)"
            } else {
                ""
            }
        );

        // Validate provisioner type
        if !matches!(
            storage_class.provisioner.as_str(),
            "rusternetes.io/hostpath" | "kubernetes.io/hostpath" | "hostpath"
        ) {
            return Err(anyhow::anyhow!(
                "Unsupported provisioner: {}",
                storage_class.provisioner
            ));
        }
        let host_path_source = Some(HostPathVolumeSource {
            path: volume_path,
            r#type: Some(HostPathType::DirectoryOrCreate),
        });

        // Determine reclaim policy (default to Delete for dynamically provisioned volumes)
        let reclaim_policy = storage_class
            .reclaim_policy
            .clone()
            .unwrap_or(PersistentVolumeReclaimPolicy::Delete);

        // Create labels to track the PVC this was created for
        let mut labels = HashMap::new();
        labels.insert("pvc-name".to_string(), pvc.metadata.name.clone());
        labels.insert("pvc-namespace".to_string(), namespace.to_string());
        labels.insert("provisioner".to_string(), storage_class.provisioner.clone());
        labels.insert(
            "storage-class".to_string(),
            storage_class.metadata.name.clone(),
        );

        let pv = PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut meta = ObjectMeta::new(pv_name);
                meta.uid = uuid::Uuid::new_v4().to_string();
                meta.resource_version = Some("1".to_string());
                meta.labels = Some(labels);
                meta.annotations = Some({
                    let mut annotations = HashMap::new();
                    annotations.insert(
                        "pv.kubernetes.io/provisioned-by".to_string(),
                        storage_class.provisioner.clone(),
                    );
                    annotations
                });
                meta
            },
            spec: rusternetes_common::resources::PersistentVolumeSpec {
                capacity,
                host_path: host_path_source,
                nfs: None,
                iscsi: None,
                local: None,
                csi: None,
                access_modes: pvc.spec.access_modes.clone(),
                persistent_volume_reclaim_policy: Some(reclaim_policy),
                storage_class_name: Some(storage_class.metadata.name.clone()),
                // Upstream copies the StorageClass mount options onto every
                // dynamically-provisioned PV (pv_controller.go:1677:
                // `MountOptions: storageClass.MountOptions`).
                mount_options: storage_class.mount_options.clone(),
                volume_mode: pvc.spec.volume_mode.clone(),
                node_affinity: None,
                // Pre-bind the PV to the exact PVC it was provisioned for
                // (#1095). Without this, the PV binder first-matches by
                // capacity/class and can pair two concurrently-provisioned PVCs
                // with each other's PV. The binder honors this claimRef and only
                // completes the binding for the named PVC.
                claim_ref: Some(
                    rusternetes_common::resources::service_account::ObjectReference {
                        kind: Some("PersistentVolumeClaim".to_string()),
                        namespace: Some(namespace.to_string()),
                        name: Some(pvc.metadata.name.clone()),
                        uid: Some(pvc.metadata.uid.clone()),
                        api_version: Some("v1".to_string()),
                        resource_version: None,
                        field_path: None,
                    },
                ),
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Available,
                message,
                reason: None,
                last_phase_transition_time: None,
            }),
        };

        Ok(pv)
    }

    /// Handle snapshot restore by validating the snapshot and returning the source path
    async fn handle_snapshot_restore(
        &self,
        data_source: &rusternetes_common::resources::volume::TypedLocalObjectReference,
        namespace: &str,
        target_path: &str,
    ) -> Result<Option<String>> {
        // Check if data source is a VolumeSnapshot
        if data_source.kind != "VolumeSnapshot" {
            warn!(
                "Unsupported dataSource kind: {}. Only VolumeSnapshot is supported for restore.",
                data_source.kind
            );
            return Ok(None);
        }

        let snapshot_name = &data_source.name;
        info!(
            "PVC is requesting restore from VolumeSnapshot {}/{}",
            namespace, snapshot_name
        );

        // Get the VolumeSnapshot
        let snapshot_key = build_key("volumesnapshots", Some(namespace), snapshot_name);
        let snapshot: VolumeSnapshot =
            self.storage.get(&snapshot_key).await.with_context(|| {
                format!("VolumeSnapshot {}/{} not found", namespace, snapshot_name)
            })?;

        // Ensure snapshot is ready to use
        let ready = snapshot
            .status
            .as_ref()
            .and_then(|s| s.ready_to_use)
            .unwrap_or(false);

        if !ready {
            return Err(anyhow::anyhow!(
                "VolumeSnapshot {}/{} is not ready to use",
                namespace,
                snapshot_name
            ));
        }

        // Get the bound VolumeSnapshotContent
        let content_name = snapshot
            .status
            .as_ref()
            .and_then(|s| s.bound_volume_snapshot_content_name.as_ref())
            .context("VolumeSnapshot has no bound VolumeSnapshotContent")?;

        let content_key = build_key("volumesnapshotcontents", None, content_name);
        let content: VolumeSnapshotContent = self
            .storage
            .get(&content_key)
            .await
            .with_context(|| format!("VolumeSnapshotContent {} not found", content_name))?;

        // Get the snapshot handle (this would be the path to the snapshot data)
        let snapshot_handle = content
            .status
            .as_ref()
            .and_then(|s| s.snapshot_handle.as_ref())
            .context("VolumeSnapshotContent has no snapshot handle")?;

        info!(
            "Restoring from snapshot {} (handle: {}) to {}",
            content_name, snapshot_handle, target_path
        );

        // In a real implementation, this would:
        // 1. Copy data from the snapshot location to the new volume location
        // 2. For hostpath volumes, this could be a directory copy
        // 3. For CSI volumes, this would invoke the CSI driver's CreateVolumeFromSnapshot

        // For now, we'll just log the operation and mark it as successful
        // The actual data copy would be handled by the CSI driver or volume plugin
        info!(
            "Snapshot restore simulated: {} -> {}. In production, this would copy snapshot data.",
            snapshot_handle, target_path
        );

        Ok(Some(snapshot_handle.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::volume::{
        PersistentVolumeAccessMode, PersistentVolumeClaimPhase, PersistentVolumeClaimStatus,
        PersistentVolumeMode, ResourceRequirements,
    };
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn test_is_provisioner_supported() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DynamicProvisionerController::new(storage);

        assert!(controller.is_provisioner_supported("rusternetes.io/hostpath"));
        assert!(controller.is_provisioner_supported("kubernetes.io/hostpath"));
        assert!(controller.is_provisioner_supported("hostpath"));
        assert!(!controller.is_provisioner_supported("kubernetes.io/aws-ebs"));
    }

    #[tokio::test]
    async fn test_create_pv_for_pvc() {
        // Use MemoryStorage for testing
        let storage = Arc::new(MemoryStorage::new());
        let controller = DynamicProvisionerController::new(storage);

        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "5Gi".to_string());

        let pvc = PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut meta = ObjectMeta::new("test-pvc");
                meta.namespace = Some("default".to_string());
                meta
            },
            spec: rusternetes_common::resources::PersistentVolumeClaimSpec {
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                resources: ResourceRequirements {
                    limits: None,
                    requests: Some(requests),
                },
                volume_name: None,
                storage_class_name: Some("fast".to_string()),
                volume_mode: Some(PersistentVolumeMode::Filesystem),
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
        };

        let storage_class = StorageClass {
            type_meta: TypeMeta {
                kind: "StorageClass".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new("fast"),
            provisioner: "rusternetes.io/hostpath".to_string(),
            parameters: None,
            reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
            volume_binding_mode: None,
            allowed_topologies: None,
            allow_volume_expansion: None,
            mount_options: None,
        };

        let pv = controller
            .create_pv_for_pvc(&storage_class, &pvc, "pvc-default-test-pvc")
            .await
            .unwrap();

        // Verify PV metadata
        assert_eq!(pv.metadata.name, "pvc-default-test-pvc");
        assert_eq!(pv.spec.storage_class_name, Some("fast".to_string()));
        assert_eq!(pv.spec.capacity.get("storage"), Some(&"5Gi".to_string()));
        assert_eq!(
            pv.spec.persistent_volume_reclaim_policy,
            Some(PersistentVolumeReclaimPolicy::Delete)
        );
        assert_eq!(
            pv.spec.access_modes,
            vec![PersistentVolumeAccessMode::ReadWriteOnce]
        );
        assert_eq!(
            pv.status.as_ref().unwrap().phase,
            PersistentVolumePhase::Available
        );

        // Verify hostpath volume source
        let hp = pv
            .spec
            .host_path
            .as_ref()
            .expect("Expected HostPath volume source");
        assert_eq!(hp.path, "/tmp/rusternetes/dynamic-pvs/pvc-default-test-pvc");
        assert_eq!(hp.r#type, Some(HostPathType::DirectoryOrCreate));

        // Verify labels
        let labels = pv.metadata.labels.as_ref().unwrap();
        assert_eq!(labels.get("pvc-name"), Some(&"test-pvc".to_string()));
        assert_eq!(labels.get("pvc-namespace"), Some(&"default".to_string()));
        assert_eq!(
            labels.get("provisioner"),
            Some(&"rusternetes.io/hostpath".to_string())
        );
        assert_eq!(labels.get("storage-class"), Some(&"fast".to_string()));

        // Verify annotations
        let annotations = pv.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            annotations.get("pv.kubernetes.io/provisioned-by"),
            Some(&"rusternetes.io/hostpath".to_string())
        );

        // #1095: the PV is pre-bound — claimRef names the exact PVC it was
        // provisioned for, so the binder can't cross-bind it to another PVC.
        let cr = pv.spec.claim_ref.as_ref().expect("claimRef must be set");
        assert_eq!(cr.kind.as_deref(), Some("PersistentVolumeClaim"));
        assert_eq!(cr.namespace.as_deref(), Some("default"));
        assert_eq!(cr.name.as_deref(), Some("test-pvc"));
        assert_eq!(cr.uid.as_ref(), Some(&pvc.metadata.uid));
    }
}
