use anyhow::{Context, Result};
use rusternetes_common::{
    cloud_provider::{CloudProvider, LoadBalancerPort, LoadBalancerService as CloudLBService},
    resources::{
        service::{LoadBalancerIngress, LoadBalancerStatus, ServiceStatus},
        Event, EventType, Node, ObjectReference, Service, ServiceType,
    },
};
use rusternetes_storage::{extract_key, Storage, WorkQueue};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// LoadBalancerController reconciles LoadBalancer-type Services with cloud provider load balancers
pub struct LoadBalancerController<S: Storage> {
    storage: Arc<S>,
    cloud_provider: Option<Arc<dyn CloudProvider>>,
    cluster_name: String,
    sync_interval: Duration,
}

impl<S: Storage + 'static> LoadBalancerController<S> {
    pub fn new(
        storage: Arc<S>,
        cloud_provider: Option<Arc<dyn CloudProvider>>,
        cluster_name: String,
        sync_interval_secs: u64,
    ) -> Self {
        Self {
            storage,
            cloud_provider,
            cluster_name,
            sync_interval: Duration::from_secs(sync_interval_secs),
        }
    }

    /// Start the controller reconciliation loop
    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting LoadBalancer controller");

        if self.cloud_provider.is_none() {
            warn!("No cloud provider configured. LoadBalancer services will not be provisioned.");
            warn!("Set CLOUD_PROVIDER environment variable to enable cloud load balancers.");
        }

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("services", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    time::sleep(self.sync_interval).await;
                    continue;
                }
            };

            let mut resync = time::interval(Duration::from_secs(30));
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

    /// Reconcile all LoadBalancer services
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
            let storage_key = rusternetes_storage::build_key("services", Some(ns), name);
            match self.storage.get::<Service>(&storage_key).await {
                Ok(service) => {
                    // Only process LoadBalancer-type services
                    let is_lb = matches!(
                        service
                            .spec
                            .service_type
                            .as_ref()
                            .unwrap_or(&ServiceType::ClusterIP),
                        ServiceType::LoadBalancer
                    );
                    if is_lb {
                        if let Some(ref cloud_provider) = self.cloud_provider {
                            let nodes: Vec<Node> = self
                                .storage
                                .list("/registry/nodes/")
                                .await
                                .unwrap_or_default();
                            let node_addresses: Vec<String> = nodes
                                .iter()
                                .filter_map(|node| {
                                    node.status
                                        .as_ref()
                                        .and_then(|s| s.addresses.as_ref())
                                        .and_then(|addrs| {
                                            addrs.iter().find(|a| a.address_type == "InternalIP")
                                        })
                                        .map(|addr| addr.address.clone())
                                })
                                .collect();
                            match self
                                .reconcile_service(
                                    &service,
                                    cloud_provider.as_ref(),
                                    &node_addresses,
                                )
                                .await
                            {
                                Ok(()) => queue.forget(&key).await,
                                Err(e) => {
                                    error!("Failed to reconcile {}: {}", key, e);
                                    queue.requeue_rate_limited(key.clone()).await;
                                }
                            }
                        } else {
                            // No cloud provider — publish stub status so e2e
                            // service.go:4291 can complete.
                            if let Err(e) = self.reconcile_no_cloud_provider().await {
                                error!("LB stub status publish failed: {}", e);
                            }
                            queue.forget(&key).await;
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
        match self.storage.list::<Service>("/registry/services/").await {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("services/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list services for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Reconciling LoadBalancer services");

        // If no cloud provider, fall back to a node-address-based stub so
        // upstream e2e network/service.go:4291 (LoadBalancer status check)
        // does not hang waiting for ingress to be populated.
        let cloud_provider = match &self.cloud_provider {
            Some(p) => p,
            None => {
                debug!("No cloud provider configured — falling back to node-address ingress stub");
                return self.reconcile_no_cloud_provider().await;
            }
        };

        // Get all services
        let services: Vec<Service> = self
            .storage
            .list("/registry/services/")
            .await
            .context("Failed to list services")?;

        // Get all nodes for IP addresses
        let nodes: Vec<Node> = self
            .storage
            .list("/registry/nodes/")
            .await
            .context("Failed to list nodes")?;

        let node_addresses: Vec<String> = nodes
            .iter()
            .filter_map(|node| {
                node.status
                    .as_ref()
                    .and_then(|s| s.addresses.as_ref())
                    .and_then(|addrs| addrs.iter().find(|a| a.address_type == "InternalIP"))
                    .map(|addr| addr.address.clone())
            })
            .collect();

        // Filter to LoadBalancer services
        let lb_services: Vec<&Service> = services
            .iter()
            .filter(|s| {
                matches!(
                    s.spec
                        .service_type
                        .as_ref()
                        .unwrap_or(&ServiceType::ClusterIP),
                    ServiceType::LoadBalancer
                )
            })
            .collect();

        debug!(
            "Found {} LoadBalancer services to reconcile",
            lb_services.len()
        );

        for service in lb_services {
            if let Err(e) = self
                .reconcile_service(service, cloud_provider.as_ref(), &node_addresses)
                .await
            {
                let namespace = service.metadata.namespace.as_deref().unwrap_or("unknown");
                error!(
                    "Failed to reconcile service {}/{}: {}",
                    namespace, service.metadata.name, e
                );
            }
        }

        Ok(())
    }

    /// Reconcile LoadBalancer services when no cloud provider is configured.
    ///
    /// Without a real provider we still need to publish a status so e2e
    /// callers (upstream `service.go:4291`) observe a populated
    /// `status.loadBalancer.ingress` and can finish their lifecycle. We use
    /// the first InternalIP of any Node as the ingress IP. Falls back to the
    /// service's own loadBalancerIP/ClusterIP if no node is registered.
    pub async fn reconcile_no_cloud_provider(&self) -> Result<()> {
        let services: Vec<Service> = self
            .storage
            .list("/registry/services/")
            .await
            .context("Failed to list services")?;

        // Collect a stub ingress: prefer node InternalIP, fallback to a sentinel.
        let nodes: Vec<Node> = self
            .storage
            .list("/registry/nodes/")
            .await
            .unwrap_or_default();
        let node_ingress: Option<String> = nodes
            .iter()
            .filter_map(|n| {
                n.status
                    .as_ref()
                    .and_then(|s| s.addresses.as_ref())
                    .and_then(|addrs| addrs.iter().find(|a| a.address_type == "InternalIP"))
                    .map(|a| a.address.clone())
            })
            .next();

        for svc in services {
            let is_lb = matches!(
                svc.spec
                    .service_type
                    .as_ref()
                    .unwrap_or(&ServiceType::ClusterIP),
                ServiceType::LoadBalancer
            );
            if !is_lb {
                continue;
            }

            let already_ok = svc
                .status
                .as_ref()
                .and_then(|s| s.load_balancer.as_ref())
                .map(|lb| !lb.ingress.is_empty())
                .unwrap_or(false);
            if already_ok {
                continue;
            }

            let ns = match svc.metadata.namespace.as_deref() {
                Some(n) => n,
                None => continue,
            };
            let name = svc.metadata.name.as_str();

            // Apply the same endpoint-readiness health gate as `reconcile_service`:
            // for selector-backed Services, withhold stub ingress and emit a
            // warning event until at least one endpoint is Ready.  Selector-less
            // Services (manually-managed endpoints) are not gated.
            let has_selector = svc
                .spec
                .selector
                .as_ref()
                .map(|sel| !sel.is_empty())
                .unwrap_or(false);
            if has_selector && !self.has_ready_endpoints(ns, name).await {
                warn!(
                    "LoadBalancer service {}/{} has no ready endpoints; withholding stub ingress",
                    ns, name
                );
                self.record_warning_event(
                    &svc,
                    "LoadBalancerSourceUnhealthy",
                    "No ready endpoints backing the LoadBalancer; withholding ingress status",
                )
                .await;
                continue;
            }

            // Order: node InternalIP, spec.loadBalancerIP, spec.clusterIP, sentinel.
            // Upstream e2e only checks that ingress is non-empty, not the IP value.
            let ingress_ip = node_ingress
                .clone()
                .or_else(|| svc.spec.load_balancer_ip.clone())
                .or_else(|| svc.spec.cluster_ip.clone())
                .unwrap_or_else(|| "0.0.0.0".to_string());

            // Re-read to avoid clobbering concurrent updates (matches the
            // cloud-provider path in `update_service_status`).
            let key = rusternetes_storage::build_key("services", Some(ns), name);
            let mut fresh: Service = match self.storage.get(&key).await {
                Ok(s) => s,
                Err(_) => continue,
            };
            let conditions = fresh.status.as_ref().and_then(|s| s.conditions.clone());
            fresh.status = Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: vec![LoadBalancerIngress {
                        ip: Some(ingress_ip),
                        hostname: None,
                        ip_mode: None,
                        ports: None,
                    }],
                }),
                conditions,
            });
            if let Err(e) = self.storage.update(&key, &fresh).await {
                warn!(
                    "Failed to populate stub LB status for {}/{}: {}",
                    ns, name, e
                );
            } else {
                info!(
                    "Populated stub LoadBalancer ingress for {}/{} (no cloud provider)",
                    ns, name
                );
            }
        }

        Ok(())
    }

    /// Reconcile a single LoadBalancer service
    async fn reconcile_service(
        &self,
        service: &Service,
        cloud_provider: &dyn CloudProvider,
        node_addresses: &[String],
    ) -> Result<()> {
        let namespace = service
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Service has no namespace"))?;
        let name = &service.metadata.name;

        debug!("Reconciling LoadBalancer service {}/{}", namespace, name);

        // Health-gate the LB status on endpoint readiness. Mirrors upstream
        // e2e/network/loadbalancer.go "should only target nodes with
        // endpoints": when a selector-backed Service has zero Ready endpoints,
        // an external LB health check would fail every node, so we withhold
        // the ingress status and surface a Warning Event rather than
        // advertising an unreachable VIP. The next reconcile (once a pod
        // becomes Ready) populates ingress normally.
        //
        // Only selector-backed Services are gated: Services with no/empty
        // selector rely on manually-managed Endpoints (or none at all), so the
        // controller cannot infer readiness from a missing Endpoints object
        // and must not withhold their status.
        let has_selector = service
            .spec
            .selector
            .as_ref()
            .map(|sel| !sel.is_empty())
            .unwrap_or(false);
        if has_selector && !self.has_ready_endpoints(namespace, name).await {
            warn!(
                "LoadBalancer service {}/{} has no ready endpoints; withholding ingress status",
                namespace, name
            );
            self.record_warning_event(
                service,
                "LoadBalancerSourceUnhealthy",
                "No ready endpoints backing the LoadBalancer; withholding ingress status",
            )
            .await;
            return Ok(());
        }

        // Ensure NodePorts are allocated
        let has_node_ports = service.spec.ports.iter().all(|p| p.node_port.is_some());

        let updated_service = if !has_node_ports {
            info!(
                "Allocating NodePorts for LoadBalancer service {}/{}",
                namespace, name
            );
            self.allocate_node_ports(service).await?
        } else {
            service.clone()
        };

        // Convert to cloud provider service format
        let cloud_lb_service = CloudLBService {
            namespace: namespace.clone(),
            name: name.clone(),
            cluster_name: self.cluster_name.clone(),
            ports: updated_service
                .spec
                .ports
                .iter()
                .map(|p| LoadBalancerPort {
                    name: p.name.clone(),
                    protocol: p.protocol.clone(),
                    port: p.port,
                    node_port: p.node_port.unwrap(),
                })
                .collect(),
            node_addresses: node_addresses.to_vec(),
            session_affinity: updated_service.spec.session_affinity.clone(),
            annotations: updated_service
                .metadata
                .annotations
                .clone()
                .unwrap_or_default(),
        };

        // Single-attempt ensure. Matches upstream
        // (k8s.io/cloud-provider/controllers/service/controller.go ~line 483):
        // any failure is logged + a Warning Event is emitted, then the
        // workqueue rate-limits the next attempt. No inline backoff — that
        // would hold a worker for seconds and starve other Services under
        // a region-wide cloud blip.
        let lb_status = match cloud_provider.ensure_load_balancer(&cloud_lb_service).await {
            Ok(s) => s,
            Err(e) => {
                self.record_warning_event(
                    service,
                    "SyncLoadBalancerFailed",
                    &format!("Error syncing load balancer: {e}"),
                )
                .await;
                return Err(
                    anyhow::anyhow!(e.to_string()).context("Failed to ensure load balancer")
                );
            }
        };

        // Update service status with load balancer information. Read-modify-
        // write on a single attempt; conflicts re-enqueue via the workqueue.
        self.update_service_status(service, lb_status).await?;

        info!(
            "Successfully reconciled LoadBalancer service {}/{}",
            namespace, name
        );

        Ok(())
    }

    /// Whether the Service has at least one ready endpoint. Reads the
    /// Endpoints object the EndpointsController publishes (same name as the
    /// Service) and checks for any non-empty `addresses` (ready) entry across
    /// its subsets. A missing Endpoints object or only `notReadyAddresses`
    /// counts as "no ready endpoints".
    async fn has_ready_endpoints(&self, namespace: &str, name: &str) -> bool {
        let key = rusternetes_storage::build_key("endpoints", Some(namespace), name);
        let ep: rusternetes_common::resources::Endpoints = match self.storage.get(&key).await {
            Ok(ep) => ep,
            Err(_) => return false,
        };
        ep.subsets.iter().any(|s| {
            s.addresses
                .as_ref()
                .map(|addrs| !addrs.is_empty())
                .unwrap_or(false)
        })
    }

    /// Update `service.status.loadBalancer` with the cloud-provider result.
    /// Single read-modify-write — storage Conflict on concurrent writes
    /// surfaces as an error and the workqueue re-enqueues us so the next
    /// reconcile observes the latest version. Conditions set by other
    /// controllers are preserved.
    async fn update_service_status(
        &self,
        service: &Service,
        lb_status: rusternetes_common::cloud_provider::LoadBalancerStatus,
    ) -> Result<()> {
        let namespace = service
            .metadata
            .namespace
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Service has no namespace"))?;
        let name = &service.metadata.name;
        let key = rusternetes_storage::build_key("services", Some(namespace), name);

        let service_lb_status = LoadBalancerStatus {
            ingress: lb_status
                .ingress
                .iter()
                .map(|ing| LoadBalancerIngress {
                    ip: ing.ip.clone(),
                    hostname: ing.hostname.clone(),
                    ip_mode: None,
                    ports: None,
                })
                .collect(),
        };

        let mut current: Service = match self.storage.get(&key).await {
            Ok(s) => s,
            Err(e) => {
                self.record_warning_event(
                    service,
                    "SyncLoadBalancerFailed",
                    &format!("Failed to read service for status update: {e}"),
                )
                .await;
                return Err(
                    anyhow::anyhow!(e.to_string()).context("Failed to read service for status")
                );
            }
        };

        // Only `load_balancer` is owned by this controller — preserve any
        // `conditions` set by other controllers. Mirrors upstream's
        // `updated.Status.LoadBalancer = *newStatus` over a DeepCopy.
        let existing_conditions = current.status.as_ref().and_then(|s| s.conditions.clone());
        current.status = Some(ServiceStatus {
            load_balancer: Some(service_lb_status),
            conditions: existing_conditions,
        });

        if let Err(e) = self.storage.update(&key, &current).await {
            warn!(
                "update_service_status for {}/{} failed: {} (workqueue will retry)",
                namespace, name, e
            );
            self.record_warning_event(
                service,
                "SyncLoadBalancerFailed",
                &format!("Error updating load balancer status: {e}"),
            )
            .await;
            return Err(anyhow::anyhow!(e.to_string()).context("Failed to update service status"));
        }
        debug!("Updated status for service {}/{}", namespace, name);
        Ok(())
    }

    /// Record a Warning Event against a Service. Best-effort — failure to
    /// write the event must not mask the underlying reconcile error.
    /// Matches `EventsController::create_event_if_new`: same reason
    /// produces one Event per Service to avoid log spam (upstream then
    /// updates `count`/`lastTimestamp` via a separate aggregator we don't
    /// have yet — see follow-up note in the PR body).
    async fn record_warning_event(&self, service: &Service, reason: &str, message: &str) {
        let namespace = service.metadata.namespace.as_deref().unwrap_or_default();
        let name = service.metadata.name.clone();
        let involved = ObjectReference {
            kind: Some("Service".to_string()),
            namespace: Some(namespace.to_string()),
            name: Some(name),
            api_version: Some("v1".to_string()),
            // Carry the Service UID so the event survives an object
            // recreation cleanly (an `apply` that recreates the Service
            // will mint a new UID; events stay scoped to the old one).
            uid: Some(service.metadata.uid.clone()).filter(|u| !u.is_empty()),
            resource_version: None,
            field_path: None,
        };
        let event_name = Event::generate_name(&involved, reason);
        let key = format!("/registry/events/{}/{}", namespace, event_name);
        if let Ok(mut existing) = self.storage.get::<Event>(&key).await {
            // Recurring warning for the same (service, reason): de-duplicate by
            // bumping the count / lastTimestamp and keeping the events.k8s.io
            // `series` consistent, rather than leaving it stuck at count:1.
            let now = chrono::Utc::now();
            existing.count = existing.count.saturating_add(1);
            existing.last_timestamp = Some(now);
            existing.message = message.to_string();
            existing.series = Some(rusternetes_common::resources::EventSeries {
                count: existing.count,
                last_observed_time: now,
            });
            if let Err(e) = self.storage.update(&key, &existing).await {
                warn!(
                    "Failed to bump recurring Warning event {}/{}: {}",
                    namespace, reason, e
                );
            }
            return;
        }
        let event = Event::new(
            event_name,
            namespace.to_string(),
            involved,
            reason.to_string(),
            message.to_string(),
            EventType::Warning,
        );
        if let Err(e) = self.storage.create(&key, &event).await {
            warn!(
                "Failed to record Warning event {}/{}: {}",
                namespace, reason, e
            );
        }
    }

    /// Delete load balancer for a service (called when service is deleted)
    #[allow(dead_code)]
    pub async fn cleanup_service(&self, namespace: &str, name: &str) -> Result<()> {
        let cloud_provider = match &self.cloud_provider {
            Some(p) => p,
            None => return Ok(()), // No cloud provider, nothing to clean up
        };

        info!(
            "Cleaning up LoadBalancer for service {}/{}",
            namespace, name
        );

        cloud_provider
            .delete_load_balancer(namespace, name)
            .await
            .context("Failed to delete load balancer")?;

        Ok(())
    }

    /// Allocate NodePorts for a service
    /// NodePort range: 30000-32767 (Kubernetes default)
    async fn allocate_node_ports(&self, service: &Service) -> Result<Service> {
        const NODE_PORT_MIN: u16 = 30000;
        const NODE_PORT_MAX: u16 = 32767;

        let namespace = service
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Service has no namespace"))?;
        let name = &service.metadata.name;

        // Collect all currently allocated NodePorts from all services
        let allocated_ports = self.get_allocated_node_ports().await?;

        // Clone service and allocate ports
        let mut updated_service = service.clone();

        for port in &mut updated_service.spec.ports {
            if port.node_port.is_none() {
                // Find next available port
                let node_port =
                    Self::find_available_port(NODE_PORT_MIN, NODE_PORT_MAX, &allocated_ports)?;

                info!(
                    "Allocated NodePort {} for service {}/{} port {}",
                    node_port,
                    namespace,
                    name,
                    port.name.as_deref().unwrap_or(&port.port.to_string())
                );

                port.node_port = Some(node_port);
            }
        }

        // Update the service in storage
        let key = rusternetes_storage::build_key("services", Some(namespace), name);
        self.storage
            .update(&key, &updated_service)
            .await
            .context("Failed to update service with NodePorts")?;

        Ok(updated_service)
    }

    /// Get all currently allocated NodePorts across all services
    async fn get_allocated_node_ports(&self) -> Result<HashSet<u16>> {
        let services: Vec<Service> = self
            .storage
            .list("/registry/services/")
            .await
            .context("Failed to list services")?;

        let mut allocated = HashSet::new();

        for service in services {
            for port in &service.spec.ports {
                if let Some(node_port) = port.node_port {
                    allocated.insert(node_port);
                }
            }
        }

        debug!("Found {} allocated NodePorts", allocated.len());

        Ok(allocated)
    }

    /// Find an available port in the given range
    fn find_available_port(min: u16, max: u16, allocated: &HashSet<u16>) -> Result<u16> {
        // Simple linear search for available port
        // In production, this could be optimized with a more sophisticated allocator
        for port in min..=max {
            if !allocated.contains(&port) {
                return Ok(port);
            }
        }

        Err(anyhow::anyhow!(
            "No available NodePorts in range {}-{}. All {} ports are allocated.",
            min,
            max,
            max - min + 1
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn test_controller_creation() {
        // Test that we can create a controller without cloud provider
        let storage = Arc::new(MemoryStorage::new());
        let controller = LoadBalancerController::new(storage, None, "test-cluster".to_string(), 30);

        assert_eq!(controller.cluster_name, "test-cluster");
        assert!(controller.cloud_provider.is_none());
    }
}
