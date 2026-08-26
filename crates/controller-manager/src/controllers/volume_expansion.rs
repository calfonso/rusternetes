use anyhow::{Context, Result};
use rusternetes_common::resources::volume::{
    PersistentVolumeClaimPhase, PersistentVolumeClaimResizeStatus,
};
use rusternetes_common::resources::{
    PersistentVolume, PersistentVolumeClaim, PersistentVolumeClaimStatus, StorageClass,
};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

pub struct VolumeExpansionController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> VolumeExpansionController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting Volume Expansion Controller");

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
                Ok(resource) => match self.reconcile_pvc(&resource).await {
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
            .list::<PersistentVolumeClaim>("/registry/persistentvolumeclaims/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("persistentvolumeclaims/{}/{}", ns, item.metadata.name)
                    };
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
            if let Err(e) = self.reconcile_pvc(&pvc).await {
                error!(
                    "Failed to reconcile PVC {}/{}: {}",
                    pvc.metadata.namespace.as_deref().unwrap_or("default"),
                    pvc.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    async fn reconcile_pvc(&self, pvc: &PersistentVolumeClaim) -> Result<()> {
        let pvc_name = &pvc.metadata.name;
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");

        // Only process bound PVCs
        if pvc.status.as_ref().map(|s| &s.phase) != Some(&PersistentVolumeClaimPhase::Bound) {
            return Ok(());
        }

        // Check if expansion is needed
        if !self.needs_expansion(pvc)? {
            return Ok(());
        }

        info!("PVC {}/{} needs expansion", namespace, pvc_name);

        // Get the storage class
        let storage_class_name = pvc
            .spec
            .storage_class_name
            .as_ref()
            .context("PVC has no storage class name")?;

        let sc_key = build_key("storageclasses", None, storage_class_name);
        let storage_class: StorageClass = self
            .storage
            .get(&sc_key)
            .await
            .with_context(|| format!("StorageClass {} not found", storage_class_name))?;

        // Check if volume expansion is allowed
        if !storage_class.allow_volume_expansion.unwrap_or(false) {
            warn!(
                "Volume expansion not allowed for StorageClass {}. PVC {}/{} cannot be expanded.",
                storage_class_name, namespace, pvc_name
            );
            return Ok(());
        }

        // Perform the expansion
        self.expand_volume(pvc, &storage_class).await?;

        Ok(())
    }

    /// Check if a PVC needs expansion
    fn needs_expansion(&self, pvc: &PersistentVolumeClaim) -> Result<bool> {
        let status = pvc.status.as_ref().context("PVC has no status")?;

        // Get requested storage from spec
        let requested_storage = pvc
            .spec
            .resources
            .requests
            .as_ref()
            .and_then(|r| r.get("storage"))
            .context("PVC has no storage request")?;

        // Get current capacity from status
        let current_capacity = status.capacity.as_ref().and_then(|c| c.get("storage"));

        match current_capacity {
            None => Ok(false), // No capacity yet, not ready for expansion
            Some(current) => {
                // Check if requested is greater than current
                Ok(self.storage_greater_than(requested_storage, current))
            }
        }
    }

    /// Expand a PVC to the requested size
    async fn expand_volume(
        &self,
        pvc: &PersistentVolumeClaim,
        storage_class: &StorageClass,
    ) -> Result<()> {
        let pvc_name = &pvc.metadata.name;
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");

        let requested_storage = pvc
            .spec
            .resources
            .requests
            .as_ref()
            .and_then(|r| r.get("storage"))
            .context("PVC has no storage request")?;

        info!(
            "Expanding PVC {}/{} to {}",
            namespace, pvc_name, requested_storage
        );

        // Get the bound PV
        let pv_name = pvc
            .spec
            .volume_name
            .as_ref()
            .context("PVC has no volume name")?;

        let pv_key = build_key("persistentvolumes", None, pv_name);
        let mut pv: PersistentVolume = self
            .storage
            .get(&pv_key)
            .await
            .with_context(|| format!("PV {} not found", pv_name))?;

        // Update PVC status to indicate resize is in progress
        let mut updated_pvc = pvc.clone();
        let mut status = updated_pvc
            .status
            .clone()
            .unwrap_or(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Bound,
                access_modes: None,
                capacity: None,
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            });

        // Set allocated resources to the new requested size
        let mut allocated = HashMap::new();
        allocated.insert("storage".to_string(), requested_storage.clone());
        status.allocated_resources = Some(allocated);
        status.resize_status = Some(PersistentVolumeClaimResizeStatus::ControllerResizeInProgress);

        updated_pvc.status = Some(status.clone());

        let pvc_key = build_key("persistentvolumeclaims", Some(namespace), pvc_name);
        self.storage.update(&pvc_key, &updated_pvc).await?;

        info!(
            "Updated PVC {}/{} status to ControllerResizeInProgress",
            namespace, pvc_name
        );

        // Perform the actual expansion on the PV
        // For hostpath volumes, this is immediate
        // For CSI volumes, this would call the CSI driver
        match self
            .resize_pv(&mut pv, requested_storage, storage_class)
            .await
        {
            Ok(_) => {
                info!(
                    "Successfully resized PV {} to {}",
                    pv_name, requested_storage
                );

                // Update PVC status to indicate resize is complete
                status.capacity = Some({
                    let mut capacity = HashMap::new();
                    capacity.insert("storage".to_string(), requested_storage.clone());
                    capacity
                });
                status.resize_status = None; // Clear resize status when complete
                updated_pvc.status = Some(status);

                self.storage.update(&pvc_key, &updated_pvc).await?;

                info!("Expansion completed for PVC {}/{}", namespace, pvc_name);
                Ok(())
            }
            Err(e) => {
                error!("Failed to resize PV {}: {}", pv_name, e);

                // Update PVC status to indicate resize failed
                status.resize_status =
                    Some(PersistentVolumeClaimResizeStatus::ControllerResizeFailed);
                updated_pvc.status = Some(status);

                self.storage.update(&pvc_key, &updated_pvc).await?;

                Err(e)
            }
        }
    }

    /// Resize the underlying PersistentVolume
    async fn resize_pv(
        &self,
        pv: &mut PersistentVolume,
        new_size: &str,
        _storage_class: &StorageClass,
    ) -> Result<()> {
        let pv_name = &pv.metadata.name;

        info!("Resizing PV {} to {}", pv_name, new_size);

        // Update PV capacity
        pv.spec
            .capacity
            .insert("storage".to_string(), new_size.to_string());

        // In a real implementation, this would:
        // 1. For CSI volumes: Call CSI ControllerExpandVolume
        // 2. For hostpath: Adjust quota or filesystem size
        // 3. For cloud volumes: Resize the underlying disk

        // For now, we'll just update the capacity in etcd
        let pv_key = build_key("persistentvolumes", None, pv_name);
        self.storage.update(&pv_key, pv).await?;

        info!("Updated PV {} capacity to {}", pv_name, new_size);

        Ok(())
    }

    /// Compare storage sizes and return true if first is greater than second
    fn storage_greater_than(&self, size1: &str, size2: &str) -> bool {
        // Parse the numeric part and unit from storage strings like "10Gi", "5Gi"
        let parse_storage = |s: &str| -> Option<(f64, String)> {
            let numeric_end = s.chars().position(|c| !c.is_numeric() && c != '.')?;
            let (num_str, unit) = s.split_at(numeric_end);
            let num = num_str.parse::<f64>().ok()?;
            Some((num, unit.to_string()))
        };

        match (parse_storage(size1), parse_storage(size2)) {
            (Some((num1, unit1)), Some((num2, unit2))) => {
                // Units must match for comparison
                if unit1 != unit2 {
                    warn!("Storage units don't match: {} vs {}", unit1, unit2);
                    return false;
                }
                num1 > num2
            }
            _ => {
                warn!(
                    "Failed to parse storage values: size1='{}', size2='{}'",
                    size1, size2
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn test_storage_greater_than() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = VolumeExpansionController::new(storage);

        assert!(controller.storage_greater_than("10Gi", "5Gi"));
        assert!(!controller.storage_greater_than("5Gi", "10Gi"));
        assert!(!controller.storage_greater_than("10Gi", "10Gi")); // Equal is not greater
        assert!(controller.storage_greater_than("100Mi", "50Mi"));
        assert!(!controller.storage_greater_than("50Mi", "100Mi"));
        assert!(controller.storage_greater_than("2000Gi", "1000Gi")); // 2000Gi > 1000Gi
    }

    #[test]
    fn test_needs_expansion_no_capacity() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = VolumeExpansionController::new(storage);

        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "10Gi".to_string());

        let pvc = PersistentVolumeClaim {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-pvc"),
            spec: rusternetes_common::resources::PersistentVolumeClaimSpec {
                access_modes: vec![],
                resources: rusternetes_common::resources::volume::ResourceRequirements {
                    requests: Some(requests),
                    limits: None,
                },
                volume_name: None,
                storage_class_name: Some("fast".to_string()),
                volume_mode: None,
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Bound,
                access_modes: None,
                capacity: None, // No capacity yet
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            }),
        };

        assert!(!controller.needs_expansion(&pvc).unwrap());
    }

    #[test]
    fn test_needs_expansion_requested_greater() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = VolumeExpansionController::new(storage);

        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "10Gi".to_string());

        let mut capacity = HashMap::new();
        capacity.insert("storage".to_string(), "5Gi".to_string());

        let pvc = PersistentVolumeClaim {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-pvc"),
            spec: rusternetes_common::resources::PersistentVolumeClaimSpec {
                access_modes: vec![],
                resources: rusternetes_common::resources::volume::ResourceRequirements {
                    requests: Some(requests),
                    limits: None,
                },
                volume_name: None,
                storage_class_name: Some("fast".to_string()),
                volume_mode: None,
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Bound,
                access_modes: None,
                capacity: Some(capacity),
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            }),
        };

        assert!(controller.needs_expansion(&pvc).unwrap());
    }

    #[test]
    fn test_needs_expansion_requested_equal() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = VolumeExpansionController::new(storage);

        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "10Gi".to_string());

        let mut capacity = HashMap::new();
        capacity.insert("storage".to_string(), "10Gi".to_string());

        let pvc = PersistentVolumeClaim {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-pvc"),
            spec: rusternetes_common::resources::PersistentVolumeClaimSpec {
                access_modes: vec![],
                resources: rusternetes_common::resources::volume::ResourceRequirements {
                    requests: Some(requests),
                    limits: None,
                },
                volume_name: None,
                storage_class_name: Some("fast".to_string()),
                volume_mode: None,
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Bound,
                access_modes: None,
                capacity: Some(capacity),
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            }),
        };

        assert!(!controller.needs_expansion(&pvc).unwrap());
    }
}
