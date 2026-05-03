use anyhow::Result;
use rusternetes_common::resources::{
    AllocationResult, DeviceAllocationResult, DeviceClass, DeviceRequestAllocationResult,
    ResourceClaim, ResourceClaimStatus, ResourceSlice,
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// ResourceClaimController manages the allocation of devices for ResourceClaims
///
/// This controller:
/// 1. Watches for unallocated ResourceClaims
/// 2. Finds suitable devices from ResourceSlices based on DeviceClass selectors
/// 3. Allocates devices and updates ResourceClaim status
pub struct ResourceClaimController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> ResourceClaimController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting ResourceClaim Controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("resourceclaims", None);
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
            let storage_key = build_key("resourceclaims", Some(ns), name);
            match self.storage.get::<ResourceClaim>(&storage_key).await {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.reconcile_claim(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
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
            .list::<ResourceClaim>("/registry/resourceclaims/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    if let Some(ref meta) = item.metadata {
                        let ns = meta.namespace.as_deref().unwrap_or("");
                        let name = meta.name.as_deref().unwrap_or("");
                        let key = format!("resourceclaims/{}/{}", ns, name);
                        queue.add(key).await;
                    }
                }
            }
            Err(e) => {
                error!("Failed to list resourceclaims for enqueue: {}", e);
            }
        }
    }

    pub async fn reconcile_all(&self) -> Result<()> {
        // Get all ResourceClaims across all namespaces
        let claims: Vec<ResourceClaim> = self.storage.list("/registry/resourceclaims/").await?;

        for mut claim in claims {
            if let Err(e) = self.reconcile_claim(&mut claim).await {
                error!(
                    "Failed to reconcile ResourceClaim {}/{}: {}",
                    claim
                        .metadata
                        .as_ref()
                        .and_then(|m| m.namespace.as_ref())
                        .unwrap_or(&"".to_string()),
                    claim
                        .metadata
                        .as_ref()
                        .and_then(|m| m.name.as_ref())
                        .unwrap_or(&"".to_string()),
                    e
                );
            }
        }

        Ok(())
    }

    async fn reconcile_claim(&self, claim: &mut ResourceClaim) -> Result<()> {
        let metadata = claim
            .metadata
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ResourceClaim missing metadata"))?;

        let name = metadata
            .name
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ResourceClaim missing name"))?;

        let namespace = metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ResourceClaim missing namespace"))?;

        // Skip if already allocated
        if claim
            .status
            .as_ref()
            .and_then(|s| s.allocation.as_ref())
            .is_some()
        {
            return Ok(());
        }

        debug!(
            "Allocating devices for ResourceClaim {}/{}",
            namespace, name
        );

        // Process device requests
        let mut allocation_results = Vec::new();

        for request in &claim.spec.devices.requests {
            debug!("Processing device request: {}", request.name);

            // Try to allocate from exact request
            if let Some(exact) = &request.exactly {
                let device_class_name = &exact.device_class_name;

                // Get DeviceClass
                let device_class = match self.get_device_class(device_class_name).await {
                    Ok(dc) => dc,
                    Err(e) => {
                        warn!("DeviceClass {} not found: {}", device_class_name, e);
                        continue;
                    }
                };

                debug!("Found DeviceClass: {}", device_class_name);

                // Find suitable devices from ResourceSlices
                let devices = self
                    .find_suitable_devices(&device_class, exact.count)
                    .await?;

                if devices.is_empty() {
                    warn!("No suitable devices found for request {}", request.name);
                    continue;
                }

                debug!(
                    "Found {} suitable device(s) for request {}",
                    devices.len(),
                    request.name
                );

                // Allocate the first suitable device(s)
                for device in devices {
                    allocation_results.push(DeviceRequestAllocationResult {
                        request: request.name.clone(),
                        driver: device.driver,
                        pool: device.pool,
                        device: device.device_name,
                    });
                }
            }
        }

        if allocation_results.is_empty() {
            warn!(
                "No devices could be allocated for ResourceClaim {}/{}",
                namespace, name
            );
            return Ok(());
        }

        // Update ResourceClaim status with allocation
        let status = ResourceClaimStatus {
            allocation: Some(AllocationResult {
                devices: DeviceAllocationResult {
                    results: allocation_results,
                    config: vec![],
                },
                node_selector: None,
            }),
            devices: vec![],
            reserved_for: vec![],
            deallocation_requested: None,
        };

        claim.status = Some(status);

        // Save updated ResourceClaim
        let key = build_key("resourceclaims", Some(namespace), name);
        self.storage.update(&key, claim).await?;

        info!(
            "Successfully allocated devices for ResourceClaim {}/{}",
            namespace, name
        );
        Ok(())
    }

    async fn get_device_class(&self, name: &str) -> Result<DeviceClass> {
        let key = build_key("deviceclasses", None, name);
        let device_class: DeviceClass = self.storage.get(&key).await?;
        Ok(device_class)
    }

    async fn find_suitable_devices(
        &self,
        device_class: &DeviceClass,
        count: Option<i64>,
    ) -> Result<Vec<AllocatedDevice>> {
        let mut suitable_devices = Vec::new();
        let required_count = count.unwrap_or(1) as usize;

        // Get all ResourceSlices
        let slices: Vec<ResourceSlice> = self.storage.list("/registry/resourceslices/").await?;

        debug!(
            "Checking {} ResourceSlice(s) for suitable devices",
            slices.len()
        );

        for slice in slices {
            let driver = slice.spec.driver.clone();
            let pool_name = slice.spec.pool.name.clone();

            // Check each device in the slice
            for device in &slice.spec.devices {
                // Simple device selection - in production, this would:
                // 1. Evaluate CEL expressions from DeviceClass selectors
                // 2. Check device attributes against selector requirements
                // 3. Verify device capacity and availability
                // 4. Check node affinity constraints

                // For now, we'll do basic matching
                let is_suitable = self.device_matches_class(&device.name, device_class);

                if is_suitable {
                    suitable_devices.push(AllocatedDevice {
                        driver: driver.clone(),
                        pool: pool_name.clone(),
                        device_name: device.name.clone(),
                    });

                    // Stop if we have enough devices
                    if suitable_devices.len() >= required_count {
                        return Ok(suitable_devices);
                    }
                }
            }
        }

        Ok(suitable_devices)
    }

    /// Basic device matching - checks if device name/attributes match class selectors
    /// In production, this would evaluate CEL expressions
    fn device_matches_class(&self, _device_name: &str, device_class: &DeviceClass) -> bool {
        // If no selectors specified, match all devices
        if device_class.spec.selectors.is_empty() {
            return true;
        }

        // For now, accept all devices if selectors exist
        // In production, this would evaluate CEL expressions like:
        // - device.driver == "nvidia.com/gpu"
        // - device.attributes["model"].string == "A100"
        // - device.capacity["nvidia.com/gpu"].value >= "1"

        // TODO: Implement CEL evaluation using cel-interpreter crate
        true
    }
}

#[derive(Debug, Clone)]
struct AllocatedDevice {
    driver: String,
    pool: String,
    device_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{DeviceClaim, DeviceClassSpec, ResourceClaimSpec};
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_device_matching_no_selectors() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceClaimController::new(storage);

        let device_class = DeviceClass {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "DeviceClass".to_string(),
            metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
                name: Some("test-class".to_string()),
                ..Default::default()
            }),
            spec: DeviceClassSpec {
                selectors: vec![],
                config: vec![],
                suitable_nodes: None,
            },
        };

        // Should match any device when no selectors
        assert!(controller.device_matches_class("gpu-0", &device_class));
        assert!(controller.device_matches_class("any-device", &device_class));
    }

    #[tokio::test]
    async fn test_reconcile_already_allocated() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceClaimController::new(storage.clone());

        let mut claim = ResourceClaim {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "ResourceClaim".to_string(),
            metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
                name: Some("test-claim".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            }),
            spec: ResourceClaimSpec {
                devices: DeviceClaim::default(),
            },
            status: Some(ResourceClaimStatus {
                allocation: Some(AllocationResult {
                    devices: DeviceAllocationResult {
                        results: vec![],
                        config: vec![],
                    },
                    node_selector: None,
                }),
                devices: vec![],
                reserved_for: vec![],
                deallocation_requested: None,
            }),
        };

        // Should skip already allocated claims
        let result = controller.reconcile_claim(&mut claim).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_find_suitable_devices_empty_slices() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceClaimController::new(storage);

        let device_class = DeviceClass {
            api_version: "resource.k8s.io/v1".to_string(),
            kind: "DeviceClass".to_string(),
            metadata: Some(rusternetes_common::resources::dra::ObjectMeta {
                name: Some("test-class".to_string()),
                ..Default::default()
            }),
            spec: DeviceClassSpec::default(),
        };

        let devices = controller
            .find_suitable_devices(&device_class, Some(1))
            .await
            .unwrap();
        assert_eq!(devices.len(), 0);
    }
}
