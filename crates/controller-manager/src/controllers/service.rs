/// Service Controller
///
/// Manages service lifecycle including:
/// - ClusterIP allocation from service CIDR pool
/// - NodePort allocation for NodePort and LoadBalancer services
/// - Service type transitions (ClusterIP <-> NodePort <-> LoadBalancer)
/// - Cleanup of allocated resources on service deletion
///
/// The controller works in conjunction with:
/// - EndpointsController: Manages endpoint discovery based on selectors
/// - LoadBalancerController: Provisions external load balancers
/// - EndpointSliceController: Maintains endpoint slices for scalability
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::Service;
use rusternetes_common::resources::ServiceType;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Default service CIDR for ClusterIP allocation
const DEFAULT_SERVICE_CIDR: &str = "10.96.0.0/12";
/// Default NodePort range (Kubernetes standard)
const NODE_PORT_MIN: u16 = 30000;
const NODE_PORT_MAX: u16 = 32767;
/// Reserved ClusterIP for kubernetes.default service
const KUBERNETES_SERVICE_IP: &str = "10.96.0.1";

/// ServiceController manages service IP and port allocation
pub struct ServiceController<S: Storage> {
    storage: Arc<S>,
    /// Tracks allocated ClusterIPs to avoid collisions
    allocated_ips: Arc<Mutex<HashSet<String>>>,
    /// Tracks allocated NodePorts to avoid collisions
    allocated_node_ports: Arc<Mutex<HashSet<u16>>>,
    /// Service CIDR for ClusterIP allocation
    service_cidr: String,
}

impl<S: Storage + 'static> ServiceController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            allocated_ips: Arc::new(Mutex::new(HashSet::new())),
            allocated_node_ports: Arc::new(Mutex::new(HashSet::new())),
            service_cidr: DEFAULT_SERVICE_CIDR.to_string(),
        }
    }

    /// Watch-based run loop. Initializes, then watches for service changes.
    /// Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        self.initialize().await?;

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("services", None);
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

    /// Initialize the controller by scanning existing services
    pub async fn initialize(&self) -> Result<()> {
        info!("Initializing Service Controller");

        // Scan all existing services to populate allocated IPs and ports
        let services: Vec<Service> = self.storage.list("/registry/services/").await?;

        let mut ips = self.allocated_ips.lock().await;
        let mut ports = self.allocated_node_ports.lock().await;

        for service in services {
            // Track ClusterIP allocations
            if let Some(cluster_ip) = &service.spec.cluster_ip {
                if cluster_ip != "None" {
                    ips.insert(cluster_ip.clone());
                }
            }

            // Track ClusterIPs list (for dual-stack)
            if let Some(cluster_ips) = &service.spec.cluster_ips {
                for ip in cluster_ips {
                    if ip != "None" {
                        ips.insert(ip.clone());
                    }
                }
            }

            // Track NodePort allocations
            for port in &service.spec.ports {
                if let Some(node_port) = port.node_port {
                    ports.insert(node_port);
                }
            }
        }

        info!(
            "Service Controller initialized: {} IPs allocated, {} NodePorts allocated",
            ips.len(),
            ports.len()
        );

        Ok(())
    }

    /// Main reconciliation loop - syncs all services
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
            let storage_key = build_key("services", Some(ns), name);
            match self.storage.get::<Service>(&storage_key).await {
                Ok(resource) => match self.reconcile_service(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
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
        debug!("Starting service reconciliation");

        let services: Vec<Service> = self.storage.list("/registry/services/").await?;

        for service in services {
            if let Err(e) = self.reconcile_service(&service).await {
                error!(
                    "Failed to reconcile service {}/{}: {}",
                    service
                        .metadata
                        .namespace
                        .as_ref()
                        .unwrap_or(&"default".to_string()),
                    &service.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    /// Reconcile a single service
    async fn reconcile_service(&self, service: &Service) -> Result<()> {
        let namespace = service
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Service has no namespace"))?;
        let service_name = &service.metadata.name;

        debug!("Reconciling service {}/{}", namespace, service_name);

        let service_type = service
            .spec
            .service_type
            .as_ref()
            .unwrap_or(&ServiceType::ClusterIP);

        let mut updated_service = service.clone();
        let mut needs_update = false;

        // Handle ClusterIP allocation
        if service_type != &ServiceType::ExternalName {
            if service.spec.cluster_ip.is_none()
                || service
                    .spec
                    .cluster_ip
                    .as_ref()
                    .map(|s| s.is_empty())
                    .unwrap_or(false)
            {
                // Allocate a new ClusterIP
                let cluster_ip = self.allocate_cluster_ip().await?;
                info!(
                    "Allocated ClusterIP {} for service {}/{}",
                    cluster_ip, namespace, service_name
                );
                updated_service.spec.cluster_ip = Some(cluster_ip.clone());
                updated_service.spec.cluster_ips = Some(vec![cluster_ip]);
                needs_update = true;
            } else if service.spec.cluster_ip.as_ref() == Some(&"None".to_string()) {
                // Headless service - don't allocate IP
                debug!(
                    "Service {}/{} is headless, skipping ClusterIP allocation",
                    namespace, service_name
                );
            }
        }

        // Handle NodePort allocation for NodePort and LoadBalancer services
        if matches!(
            service_type,
            ServiceType::NodePort | ServiceType::LoadBalancer
        ) {
            for (i, port) in updated_service.spec.ports.iter_mut().enumerate() {
                if port.node_port.is_none() {
                    // Allocate a new NodePort
                    let node_port = self.allocate_node_port().await?;
                    info!(
                        "Allocated NodePort {} for service {}/{} port {}",
                        node_port, namespace, service_name, i
                    );
                    port.node_port = Some(node_port);
                    needs_update = true;
                }
            }
        }

        // Handle service type downgrades (e.g., LoadBalancer -> ClusterIP)
        // If service was previously NodePort/LoadBalancer but now is ClusterIP, release NodePorts
        if service_type == &ServiceType::ClusterIP
            && service.spec.ports.iter().any(|p| p.node_port.is_some())
        {
            warn!(
                "Service {}/{} changed from NodePort/LoadBalancer to ClusterIP, releasing NodePorts",
                namespace, service_name
            );
            let mut ports_lock = self.allocated_node_ports.lock().await;
            for port in &mut updated_service.spec.ports {
                if let Some(node_port) = port.node_port.take() {
                    ports_lock.remove(&node_port);
                    info!(
                        "Released NodePort {} for service {}/{}",
                        node_port, namespace, service_name
                    );
                    needs_update = true;
                }
            }
        }

        // Update service if changes were made
        if needs_update {
            let service_key = build_key("services", Some(namespace), service_name);
            self.storage.update(&service_key, &updated_service).await?;
            info!(
                "Updated service {}/{} with allocated resources",
                namespace, service_name
            );
        }

        Ok(())
    }

    /// Allocate a ClusterIP from the service CIDR range
    async fn allocate_cluster_ip(&self) -> Result<String> {
        let mut ips = self.allocated_ips.lock().await;

        // Parse service CIDR
        let cidr_parts: Vec<&str> = self.service_cidr.split('/').collect();
        if cidr_parts.len() != 2 {
            return Err(anyhow::anyhow!(
                "Invalid service CIDR: {}",
                self.service_cidr
            ));
        }

        let base_ip: Ipv4Addr = cidr_parts[0]
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid service CIDR IP: {}", e))?;
        let prefix_len: u8 = cidr_parts[1]
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid service CIDR prefix length: {}", e))?;

        // Calculate the number of available IPs in the CIDR range
        let num_ips = 2u32.pow((32 - prefix_len) as u32);
        let base_ip_u32: u32 = base_ip.into();

        // Reserve the first IP (.0) and the last IP (.255 for /24 or equivalent)
        // Start from .1, but .1 is reserved for kubernetes.default service
        for offset in 2..num_ips {
            let candidate_ip = Ipv4Addr::from(base_ip_u32 + offset);
            let candidate_str = candidate_ip.to_string();

            if !ips.contains(&candidate_str) && candidate_str != KUBERNETES_SERVICE_IP {
                ips.insert(candidate_str.clone());
                return Ok(candidate_str);
            }
        }

        Err(anyhow::anyhow!(
            "No available ClusterIPs in service CIDR {}",
            self.service_cidr
        ))
    }

    /// Allocate a NodePort from the available range
    async fn allocate_node_port(&self) -> Result<u16> {
        let mut ports = self.allocated_node_ports.lock().await;

        for port in NODE_PORT_MIN..=NODE_PORT_MAX {
            if !ports.contains(&port) {
                ports.insert(port);
                return Ok(port);
            }
        }

        Err(anyhow::anyhow!(
            "No available NodePorts in range {}-{}",
            NODE_PORT_MIN,
            NODE_PORT_MAX
        ))
    }

    /// Handle service deletion - release allocated resources
    #[allow(dead_code)]
    pub async fn handle_service_deletion(&self, namespace: &str, service_name: &str) -> Result<()> {
        info!(
            "Handling deletion of service {}/{}",
            namespace, service_name
        );

        let service_key = build_key("services", Some(namespace), service_name);

        // Get the service to release its resources
        let service: Result<Service, _> = self.storage.get(&service_key).await;

        match service {
            Ok(service) => {
                // Release ClusterIP
                if let Some(cluster_ip) = &service.spec.cluster_ip {
                    if cluster_ip != "None" {
                        let mut ips = self.allocated_ips.lock().await;
                        ips.remove(cluster_ip);
                        info!(
                            "Released ClusterIP {} for service {}/{}",
                            cluster_ip, namespace, service_name
                        );
                    }
                }

                // Release ClusterIPs list
                if let Some(cluster_ips) = &service.spec.cluster_ips {
                    let mut ips = self.allocated_ips.lock().await;
                    for ip in cluster_ips {
                        if ip != "None" {
                            ips.remove(ip);
                        }
                    }
                }

                // Release NodePorts
                let mut ports = self.allocated_node_ports.lock().await;
                for port in &service.spec.ports {
                    if let Some(node_port) = port.node_port {
                        ports.remove(&node_port);
                        info!(
                            "Released NodePort {} for service {}/{}",
                            node_port, namespace, service_name
                        );
                    }
                }

                Ok(())
            }
            Err(_) => {
                // Service already deleted, nothing to clean up
                debug!("Service {}/{} already deleted", namespace, service_name);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{IntOrString, ServicePort, ServiceSpec};
    use rusternetes_common::types::ObjectMeta;

    #[tokio::test]
    async fn test_allocate_cluster_ip() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceController::new(storage);

        // Allocate first IP (should be 10.96.0.2, as .1 is reserved)
        let ip1 = controller.allocate_cluster_ip().await.unwrap();
        assert_eq!(ip1, "10.96.0.2");

        // Allocate second IP
        let ip2 = controller.allocate_cluster_ip().await.unwrap();
        assert_eq!(ip2, "10.96.0.3");

        // IPs should be different
        assert_ne!(ip1, ip2);
    }

    #[tokio::test]
    async fn test_allocate_node_port() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceController::new(storage);

        // Allocate first NodePort
        let port1 = controller.allocate_node_port().await.unwrap();
        assert!((NODE_PORT_MIN..=NODE_PORT_MAX).contains(&port1));

        // Allocate second NodePort
        let port2 = controller.allocate_node_port().await.unwrap();
        assert!((NODE_PORT_MIN..=NODE_PORT_MAX).contains(&port2));

        // Ports should be different
        assert_ne!(port1, port2);
    }

    #[tokio::test]
    async fn test_reconcile_clusterip_service() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceController::new(storage.clone());

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-service".to_string(),
                namespace: Some("default".to_string()),
                uid: uuid::Uuid::new_v4().to_string(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: None,
                annotations: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: ServiceSpec {
                selector: Some(std::collections::HashMap::new()),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(IntOrString::Int(8080)),
                    protocol: Some("TCP".to_string()),
                    node_port: None,
                    app_protocol: None,
                }],
                service_type: Some(ServiceType::ClusterIP),
                cluster_ip: None,
                external_ips: None,
                session_affinity: None,
                external_name: None,
                cluster_ips: None,
                ip_families: None,
                ip_family_policy: None,
                internal_traffic_policy: None,
                external_traffic_policy: None,
                health_check_node_port: None,
                load_balancer_class: None,
                load_balancer_ip: None,
                load_balancer_source_ranges: None,
                allocate_load_balancer_node_ports: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                traffic_distribution: None,
            },
            status: None,
        };

        // Create service in storage first (like API server would)
        let service_key = build_key("services", Some("default"), "test-service");
        storage.create(&service_key, &service).await.unwrap();

        // Reconcile should allocate ClusterIP
        controller.reconcile_service(&service).await.unwrap();

        // Verify ClusterIP was allocated
        let ips = controller.allocated_ips.lock().await;
        assert!(!ips.is_empty());

        // Verify service was updated in storage with ClusterIP
        let updated_service: Service = storage.get(&service_key).await.unwrap();
        assert!(updated_service.spec.cluster_ip.is_some());
    }

    #[tokio::test]
    async fn test_reconcile_nodeport_service() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceController::new(storage.clone());

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-nodeport-service".to_string(),
                namespace: Some("default".to_string()),
                uid: uuid::Uuid::new_v4().to_string(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: None,
                annotations: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: ServiceSpec {
                selector: Some(std::collections::HashMap::new()),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(IntOrString::Int(8080)),
                    protocol: Some("TCP".to_string()),
                    node_port: None, // Should be allocated
                    app_protocol: None,
                }],
                service_type: Some(ServiceType::NodePort),
                cluster_ip: None, // Should be allocated
                external_ips: None,
                session_affinity: None,
                external_name: None,
                cluster_ips: None,
                ip_families: None,
                ip_family_policy: None,
                internal_traffic_policy: None,
                external_traffic_policy: None,
                health_check_node_port: None,
                load_balancer_class: None,
                load_balancer_ip: None,
                load_balancer_source_ranges: None,
                allocate_load_balancer_node_ports: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                traffic_distribution: None,
            },
            status: None,
        };

        // Create service in storage first (like API server would)
        let service_key = build_key("services", Some("default"), "test-nodeport-service");
        storage.create(&service_key, &service).await.unwrap();

        // Reconcile should allocate both ClusterIP and NodePort
        controller.reconcile_service(&service).await.unwrap();

        // Verify ClusterIP was allocated
        let ips = controller.allocated_ips.lock().await;
        assert!(!ips.is_empty());

        // Verify NodePort was allocated
        let ports = controller.allocated_node_ports.lock().await;
        assert!(!ports.is_empty());

        // Verify service was updated in storage
        let updated_service: Service = storage.get(&service_key).await.unwrap();
        assert!(updated_service.spec.cluster_ip.is_some());
        assert!(updated_service.spec.ports[0].node_port.is_some());
    }
}
