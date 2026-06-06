use anyhow::Result;
use rusternetes_common::resources::{
    ingress::{HTTPIngressPath, IngressBackend, IngressRule, IngressSpec, IngressTLS},
    Ingress, IngressClass, Secret, Service,
};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// IngressController watches Ingress resources and manages ingress routing.
///
/// Note: In a production Kubernetes cluster, Ingress controllers are typically
/// implemented as external components (nginx-ingress, traefik, etc.) that:
/// 1. Watch Ingress resources
/// 2. Configure load balancers/reverse proxies
/// 3. Update Ingress status with load balancer information
///
/// This controller provides basic validation and status management for conformance.
/// Actual traffic routing would be handled by external ingress implementations.
pub struct IngressController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> IngressController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting Ingress controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("ingresses", None);
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

    /// Main reconciliation loop - processes all Ingress resources
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
            let storage_key = build_key("ingresses", Some(ns), name);
            match self.storage.get::<Ingress>(&storage_key).await {
                Ok(resource) => match self.reconcile_ingress(&resource).await {
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
        match self.storage.list::<Ingress>("/registry/ingresses/").await {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("ingresses/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list ingresses for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting Ingress reconciliation");

        // List all Ingress resources across all namespaces
        let ingresses: Vec<Ingress> = self.storage.list("/registry/ingresses/").await?;

        debug!("Found {} ingress resources to reconcile", ingresses.len());

        for ingress in ingresses {
            if let Err(e) = self.reconcile_ingress(&ingress).await {
                error!(
                    "Failed to reconcile ingress {}/{}: {}",
                    ingress
                        .metadata
                        .namespace
                        .as_ref()
                        .unwrap_or(&"default".to_string()),
                    &ingress.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    /// Reconcile a single Ingress resource
    async fn reconcile_ingress(&self, ingress: &Ingress) -> Result<()> {
        let namespace = ingress
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Ingress has no namespace"))?;
        let ingress_name = &ingress.metadata.name;

        debug!("Reconciling ingress {}/{}", namespace, ingress_name);

        // Validate the Ingress spec
        if let Some(spec) = &ingress.spec {
            if let Err(e) = self.validate_ingress_spec(spec, namespace).await {
                warn!(
                    "Ingress {}/{} validation failed: {}",
                    namespace, ingress_name, e
                );
                return Ok(());
            }
        }

        // Update Ingress status with load balancer information
        // In production, this would come from actual load balancer provisioning
        // For now, we simulate a reference implementation
        self.update_ingress_status(ingress, namespace).await?;

        debug!(
            "Ingress {}/{} reconciled (routing handled by external ingress controller)",
            namespace, ingress_name
        );

        Ok(())
    }

    /// Validate Ingress spec
    async fn validate_ingress_spec(&self, spec: &IngressSpec, namespace: &str) -> Result<()> {
        // The referenced IngressClass (cluster-scoped) must exist — upstream
        // rejects an Ingress naming an unknown class.
        if let Some(class_name) = &spec.ingress_class_name {
            let class_key = build_key("ingressclasses", None, class_name);
            match self.storage.get::<IngressClass>(&class_key).await {
                Ok(_) => {}
                Err(rusternetes_common::Error::NotFound(_)) => {
                    return Err(anyhow::anyhow!("IngressClass {} not found", class_name));
                }
                // A transport/decode error is NOT a missing class — surface it
                // accurately rather than mislabelling it "not found". The
                // reconcile retries next interval.
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to look up IngressClass {}: {}",
                        class_name,
                        e
                    ));
                }
            }
        }

        // Validate default backend if specified
        if let Some(backend) = &spec.default_backend {
            self.validate_backend(backend, namespace).await?;
        }

        // Validate TLS configurations
        if let Some(tls_configs) = &spec.tls {
            for tls in tls_configs {
                self.validate_tls_config(tls, namespace).await?;
            }
        }

        // Validate rules
        if let Some(rules) = &spec.rules {
            for rule in rules {
                self.validate_ingress_rule(rule, namespace).await?;
            }
        }

        Ok(())
    }

    /// Validate an ingress backend
    async fn validate_backend(&self, backend: &IngressBackend, namespace: &str) -> Result<()> {
        // Check service backend
        if let Some(service_backend) = &backend.service {
            let service_name = &service_backend.name;

            // Verify service exists
            let service_key = build_key("services", Some(namespace), service_name);
            match self.storage.get::<Service>(&service_key).await {
                Ok(_) => debug!("Backend service {} exists", service_name),
                Err(_) => {
                    warn!(
                        "Backend service {} not found in namespace {}",
                        service_name, namespace
                    );
                    // Don't fail validation - service might be created later
                }
            }

            // Validate port specification
            if let Some(port) = &service_backend.port {
                if port.name.is_none() && port.number.is_none() {
                    return Err(anyhow::anyhow!(
                        "Service backend port must specify either name or number"
                    ));
                }
            }
        }

        // Resource backends are also valid but not commonly used
        if backend.service.is_none() && backend.resource.is_none() {
            return Err(anyhow::anyhow!(
                "Ingress backend must specify either service or resource"
            ));
        }

        Ok(())
    }

    /// Validate TLS configuration — the referenced Secret must exist in the
    /// Ingress namespace, else status population is blocked.
    async fn validate_tls_config(&self, tls: &IngressTLS, namespace: &str) -> Result<()> {
        if let Some(secret_name) = &tls.secret_name {
            let secret_key = build_key("secrets", Some(namespace), secret_name);
            match self.storage.get::<Secret>(&secret_key).await {
                Ok(_) => {}
                Err(rusternetes_common::Error::NotFound(_)) => {
                    return Err(anyhow::anyhow!(
                        "TLS secret {} not found in namespace {}",
                        secret_name,
                        namespace
                    ));
                }
                // Transport/decode error — not a missing secret; report it
                // accurately. The reconcile retries next interval.
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to look up TLS secret {} in namespace {}: {}",
                        secret_name,
                        namespace,
                        e
                    ));
                }
            }
        }
        Ok(())
    }

    /// Validate ingress rule
    async fn validate_ingress_rule(&self, rule: &IngressRule, namespace: &str) -> Result<()> {
        // Validate HTTP rules
        if let Some(http) = &rule.http {
            for path in &http.paths {
                self.validate_http_path(path, namespace).await?;
            }
        }

        Ok(())
    }

    /// Validate HTTP path
    async fn validate_http_path(&self, path: &HTTPIngressPath, namespace: &str) -> Result<()> {
        // Validate path type
        match path.path_type.as_str() {
            "Exact" | "Prefix" | "ImplementationSpecific" => {}
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid path type '{}', must be Exact, Prefix, or ImplementationSpecific",
                    path.path_type
                ));
            }
        }

        // Validate backend
        self.validate_backend(&path.backend, namespace).await?;

        Ok(())
    }

    /// Update Ingress status with load balancer information
    ///
    /// In a full implementation, this would:
    /// 1. Allocate/provision a load balancer (via cloud provider or MetalLB)
    /// 2. Configure TLS certificates from secrets
    /// 3. Set up routing rules on the load balancer
    /// 4. Update status with the load balancer's external IP/hostname
    ///
    /// For this reference implementation, we:
    /// - Simulate load balancer IP allocation
    /// - Update Ingress status to indicate readiness
    /// - Delegate actual traffic routing to external ingress controllers
    async fn update_ingress_status(&self, ingress: &Ingress, namespace: &str) -> Result<()> {
        let ingress_name = &ingress.metadata.name;

        // Check if status already has load balancer info
        if let Some(ref status) = ingress.status {
            if let Some(ref lb_status) = status.load_balancer {
                if let Some(ref ingress_list) = lb_status.ingress {
                    if !ingress_list.is_empty() {
                        // Status already set, nothing to do
                        debug!(
                            "Ingress {}/{} already has load balancer status",
                            namespace, ingress_name
                        );
                        return Ok(());
                    }
                }
            }
        }

        // Simulate load balancer IP allocation
        // In production, this would:
        // - Query cloud provider API (AWS ELB, GCP LB, Azure LB)
        // - Or allocate from MetalLB IP pool
        // - Or use a static ingress gateway IP
        let lb_ip = self.allocate_load_balancer_ip(ingress, namespace).await?;

        // Build updated ingress with status
        let mut updated_ingress = ingress.clone();

        // Create load balancer ingress status
        let lb_ingress_status = vec![
            rusternetes_common::resources::ingress::IngressLoadBalancerIngress {
                ip: Some(lb_ip.clone()),
                hostname: None,
                ports: None,
            },
        ];

        // Update status
        updated_ingress.status = Some(rusternetes_common::resources::ingress::IngressStatus {
            load_balancer: Some(
                rusternetes_common::resources::ingress::IngressLoadBalancerStatus {
                    ingress: Some(lb_ingress_status),
                },
            ),
        });

        // Update in storage
        let ingress_key = build_key("ingresses", Some(namespace), ingress_name);
        self.storage.update(&ingress_key, &updated_ingress).await?;

        info!(
            "Updated Ingress {}/{} status with load balancer IP: {}",
            namespace, ingress_name, lb_ip
        );

        Ok(())
    }

    /// Allocate a load balancer IP for the ingress
    ///
    /// This is a reference implementation that simulates IP allocation.
    /// In production, this would:
    /// - For cloud providers: Create ELB/LB and get its public IP
    /// - For MetalLB: Allocate IP from configured pool
    /// - For bare metal: Use configured ingress gateway IP
    async fn allocate_load_balancer_ip(
        &self,
        ingress: &Ingress,
        namespace: &str,
    ) -> Result<String> {
        // Check if ingress has a specific IP requested via annotation
        if let Some(ref annotations) = ingress.metadata.annotations {
            if let Some(requested_ip) = annotations.get("ingress.rusternetes.io/load-balancer-ip") {
                info!(
                    "Using requested load balancer IP {} for Ingress {}/{}",
                    requested_ip, namespace, ingress.metadata.name
                );
                return Ok(requested_ip.clone());
            }
        }

        // In a real implementation, we would:
        // 1. Check IngressClass to determine which controller should handle this
        // 2. Query the appropriate load balancer provisioner
        // 3. Wait for IP allocation
        // 4. Return the allocated IP
        //
        // For this reference implementation, we'll use a simulated IP
        // based on the namespace and name for consistency
        let simulated_ip = self.generate_simulated_lb_ip(namespace, &ingress.metadata.name);

        info!(
            "Allocated simulated load balancer IP {} for Ingress {}/{}",
            simulated_ip, namespace, ingress.metadata.name
        );

        Ok(simulated_ip)
    }

    /// Generate a simulated load balancer IP
    ///
    /// Uses a deterministic approach based on namespace and name
    /// to ensure consistent IPs across reconciliation loops
    fn generate_simulated_lb_ip(&self, namespace: &str, name: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Create a deterministic hash from namespace and name
        let mut hasher = DefaultHasher::new();
        namespace.hash(&mut hasher);
        name.hash(&mut hasher);
        let hash = hasher.finish();

        // Generate IP in the 10.x.x.x range (private IP space used for simulation)
        // Real implementations would use actual public IPs from cloud providers
        let octet2 = ((hash >> 16) & 0xFF) as u8;
        let octet3 = ((hash >> 8) & 0xFF) as u8;
        let octet4 = (hash & 0xFF) as u8;

        format!("10.{}.{}.{}", octet2, octet3, octet4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::ingress::{
        HTTPIngressRuleValue, IngressServiceBackend, ServiceBackendPort,
    };
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_validate_path_type_valid() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/".to_string()),
            path_type: "Prefix".to_string(),
            backend,
        };

        assert!(controller
            .validate_http_path(&path, "default")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_path_type_invalid() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/".to_string()),
            path_type: "Invalid".to_string(),
            backend,
        };

        assert!(controller
            .validate_http_path(&path, "default")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_validate_backend_with_port_number() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        assert!(controller
            .validate_backend(&backend, "default")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_backend_with_port_name() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: Some("http".to_string()),
                    number: None,
                }),
            }),
            resource: None,
        };

        assert!(controller
            .validate_backend(&backend, "default")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_backend_missing_port_spec() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: None,
                }),
            }),
            resource: None,
        };

        assert!(controller
            .validate_backend(&backend, "default")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_validate_ingress_rule() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/api".to_string()),
            path_type: "Prefix".to_string(),
            backend,
        };

        let rule = IngressRule {
            host: Some("example.com".to_string()),
            http: Some(HTTPIngressRuleValue { paths: vec![path] }),
        };

        assert!(controller
            .validate_ingress_rule(&rule, "default")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_reconcile_valid_ingress() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage.clone());

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/".to_string()),
            path_type: "Prefix".to_string(),
            backend,
        };

        let rule = IngressRule {
            host: Some("example.com".to_string()),
            http: Some(HTTPIngressRuleValue { paths: vec![path] }),
        };

        let spec = IngressSpec {
            ingress_class_name: Some("nginx".to_string()),
            default_backend: None,
            tls: None,
            rules: Some(vec![rule]),
        };

        let ingress = Ingress {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Ingress".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata: {
                let mut meta = ObjectMeta::new("test-ingress").with_namespace("default");
                meta.uid = uuid::Uuid::new_v4().to_string();
                meta
            },
            spec: Some(spec),
            status: None,
        };

        // Create the IngressClass "nginx" so validation passes
        use rusternetes_common::resources::IngressClass;
        let class_key = build_key("ingressclasses", None, "nginx");
        storage
            .create(&class_key, &IngressClass::new("nginx"))
            .await
            .unwrap();

        // Create ingress in storage first (like API server would)
        let ingress_key = build_key("ingresses", Some("default"), "test-ingress");
        storage.create(&ingress_key, &ingress).await.unwrap();

        // Reconcile should validate and update status
        assert!(controller.reconcile_ingress(&ingress).await.is_ok());

        // Verify ingress was updated with load balancer status
        let updated_ingress: Ingress = storage.get(&ingress_key).await.unwrap();
        assert!(updated_ingress.status.is_some());
        let status = updated_ingress.status.unwrap();
        assert!(status.load_balancer.is_some());
    }
}
