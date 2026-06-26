use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::{
    EndpointAddress, EndpointPort, EndpointReference, EndpointSubset, Endpoints, Pod, Service,
};
use rusternetes_common::types::OwnerReference;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error};

/// Upstream `pkg/controller/endpoint/endpoints_controller.go::maxCapacity`.
/// Endpoints objects with more than this many addresses won't be routed
/// correctly by kube-proxy, so the controller truncates and emits the
/// `endpoints.kubernetes.io/over-capacity=truncated` annotation.
pub const MAX_ENDPOINTS_CAPACITY: usize = 1000;

/// Annotation key the controller sets on a truncated Endpoints object.
/// Upstream constant: `corev1.EndpointsOverCapacity` ("endpoints.kubernetes.io/over-capacity").
const ENDPOINTS_OVER_CAPACITY_ANNOTATION: &str = "endpoints.kubernetes.io/over-capacity";

/// Truncate `subsets` in place so the total address count (ready +
/// notReady) does not exceed `cap`. Removes addresses proportionally
/// across subsets, mirroring upstream `truncateEndpoints`: ready
/// addresses are dropped before notReady ones because routable endpoints
/// are more valuable than placeholders.
///
/// Returns `true` if any addresses were dropped, so the caller can set the
/// `endpoints.kubernetes.io/over-capacity=truncated` annotation.
fn truncate_endpoint_subsets(subsets: &mut [EndpointSubset], cap: usize) -> bool {
    let total: usize = subsets
        .iter()
        .map(|s| {
            s.addresses.as_ref().map(|a| a.len()).unwrap_or(0)
                + s.not_ready_addresses.as_ref().map(|a| a.len()).unwrap_or(0)
        })
        .sum();
    if total <= cap {
        return false;
    }

    let mut to_drop = total - cap;

    // Drop from `addresses` first (ready), proportionally across subsets.
    // For deterministic output and assertion stability we drop from the tail.
    for s in subsets.iter_mut() {
        if to_drop == 0 {
            break;
        }
        if let Some(addrs) = s.addresses.as_mut() {
            let drop_here = to_drop.min(addrs.len());
            addrs.truncate(addrs.len() - drop_here);
            to_drop -= drop_here;
        }
    }

    // Then notReady if still over cap.
    for s in subsets.iter_mut() {
        if to_drop == 0 {
            break;
        }
        if let Some(addrs) = s.not_ready_addresses.as_mut() {
            let drop_here = to_drop.min(addrs.len());
            addrs.truncate(addrs.len() - drop_here);
            to_drop -= drop_here;
        }
    }

    true
}

/// Split each subset into per-node subsets so no resulting subset contains
/// addresses from more than one node. Each output subset preserves the ports
/// of the input subset its addresses came from — when the named-targetPort
/// path produced multiple subsets with different resolved ports, the per-node
/// grouping must keep each (node, ports) combination distinct so pods on
/// different nodes are not stamped with another node's resolved port.
/// Addresses with no `node_name` are grouped under a single empty-node bucket.
/// Output is sorted by (node name, serialized ports) for deterministic,
/// order-independent results. Used to enforce `internalTrafficPolicy=Local`.
fn split_subsets_by_node(subsets: Vec<EndpointSubset>) -> Vec<EndpointSubset> {
    use std::collections::BTreeMap;

    // (node, serialized-ports) -> (ready, notReady, actual ports). The ports
    // are keyed alongside the node so distinct port tuples (from the
    // named-targetPort multi-subset path) yield distinct output subsets.
    #[allow(clippy::type_complexity)]
    let mut by_node: BTreeMap<
        (String, String),
        (
            Vec<EndpointAddress>,
            Vec<EndpointAddress>,
            Option<Vec<EndpointPort>>,
        ),
    > = BTreeMap::new();

    for subset in subsets {
        let ports = subset.ports.clone();
        let ports_key = serde_json::to_string(&ports).unwrap_or_default();
        for addr in subset.addresses.into_iter().flatten() {
            let node = addr.node_name.clone().unwrap_or_default();
            by_node
                .entry((node, ports_key.clone()))
                .or_insert_with(|| (Vec::new(), Vec::new(), ports.clone()))
                .0
                .push(addr);
        }
        for addr in subset.not_ready_addresses.into_iter().flatten() {
            let node = addr.node_name.clone().unwrap_or_default();
            by_node
                .entry((node, ports_key.clone()))
                .or_insert_with(|| (Vec::new(), Vec::new(), ports.clone()))
                .1
                .push(addr);
        }
    }

    by_node
        .into_values()
        .map(|(ready, not_ready, ports)| EndpointSubset {
            addresses: if ready.is_empty() { None } else { Some(ready) },
            not_ready_addresses: if not_ready.is_empty() {
                None
            } else {
                Some(not_ready)
            },
            ports,
        })
        .collect()
}

/// EndpointsController watches Services and Pods to automatically maintain Endpoints resources.
/// It creates/updates Endpoints based on:
/// 1. Service selector matching pod labels
/// 2. Pod readiness status
/// 3. Pod IP assignment
pub struct EndpointsController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> EndpointsController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Watch-based run loop. Watches services AND pods as primary resources.
    /// When a pod changes, we find services whose selector matches the pod
    /// and enqueue them for reconciliation.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let svc_prefix = build_prefix("services", None);
            let pod_prefix = build_prefix("pods", None);

            let svc_watch = match self.storage.watch(&svc_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish service watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            let pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish pod watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut svc_watch = svc_watch;
            let mut pod_watch = pod_watch;
            let mut resync = tokio::time::interval(std::time::Duration::from_secs(5));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = svc_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Service watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Service watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                self.enqueue_services_for_pod(&queue, &ev).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Pod watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Pod watch stream ended, reconnecting");
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

    /// When a pod changes, find services in the same namespace whose selector
    /// matches the pod and enqueue them for reconciliation.
    async fn enqueue_services_for_pod(
        &self,
        queue: &WorkQueue,
        event: &rusternetes_storage::WatchEvent,
    ) {
        let pod_key = extract_key(event);
        // Parse pod key: "pods/{namespace}/{name}"
        let parts: Vec<&str> = pod_key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };

        // Get the pod to check its labels
        let storage_key = format!("/registry/{}", pod_key);
        let pod: Option<Pod> = self.storage.get(&storage_key).await.ok();

        // List services in this namespace and find matches
        if let Ok(services) = self
            .storage
            .list::<Service>(&build_prefix("services", Some(ns)))
            .await
        {
            match pod {
                Some(ref pod) => {
                    for svc in &services {
                        if let Some(ref selector) = svc.spec.selector {
                            if Self::labels_match(selector, &pod.metadata.labels) {
                                queue
                                    .add(format!("services/{}/{}", ns, svc.metadata.name))
                                    .await;
                            }
                        }
                    }
                }
                None => {
                    // Pod was deleted -- enqueue all services in this namespace
                    // since we don't know which ones matched
                    for svc in &services {
                        queue
                            .add(format!("services/{}/{}", ns, svc.metadata.name))
                            .await;
                    }
                }
            }
        }
    }

    /// Check if all selector key-value pairs exist in the pod's labels.
    fn labels_match(
        selector: &HashMap<String, String>,
        labels: &Option<HashMap<String, String>>,
    ) -> bool {
        let labels = match labels {
            Some(l) => l,
            None => return selector.is_empty(),
        };
        selector.iter().all(|(k, v)| labels.get(k) == Some(v))
    }

    /// Main reconciliation loop - syncs all services with their endpoints
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
                Ok(service) => match self.reconcile_service(&service).await {
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
                tracing::error!("Failed to list services for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting endpoints reconciliation");

        // List all services across all namespaces
        let services: Vec<Service> = self.storage.list("/registry/services/").await?;

        // Snapshot surviving Service identities for orphan-Endpoints GC below.
        // Without this, deleted Services leave stale Endpoints behind and
        // upstream e2e network/service.go:3459 (DeleteService) times out.
        let surviving: std::collections::HashSet<(String, String)> = services
            .iter()
            .map(|s| {
                let ns = s
                    .metadata
                    .namespace
                    .clone()
                    .unwrap_or_else(|| "default".to_string());
                (ns, s.metadata.name.clone())
            })
            .collect();

        for service in services {
            if let Err(e) = self.reconcile_service(&service).await {
                error!(
                    "Failed to reconcile endpoints for service {}/{}: {}",
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

        // GC Endpoints whose owning Service no longer exists.
        let all_endpoints: Vec<Endpoints> = self
            .storage
            .list(&build_prefix("endpoints", None))
            .await
            .unwrap_or_default();
        for ep in &all_endpoints {
            let ns = ep.metadata.namespace.as_deref().unwrap_or("default");
            let name = ep.metadata.name.as_str();
            if surviving.contains(&(ns.to_string(), name.to_string())) {
                continue;
            }
            let key = build_key("endpoints", Some(ns), name);
            match self.storage.delete(&key).await {
                Ok(_) => debug!("Deleted orphan Endpoints {}/{}", ns, name),
                Err(e) => debug!("Failed to delete orphan Endpoints {}/{}: {}", ns, name, e),
            }
        }

        Ok(())
    }

    /// Reconcile endpoints for a single service
    async fn reconcile_service(&self, service: &Service) -> Result<()> {
        let namespace = service
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Service has no namespace"))?;
        let service_name = &service.metadata.name;

        debug!(
            "Reconciling endpoints for service {}/{}",
            namespace, service_name
        );

        // Skip ExternalName services. Upstream
        // `pkg/controller/endpoint/endpoints_controller.go` (syncService) returns
        // early for `ServiceTypeExternalName` — DNS resolution is the data path,
        // there is no Endpoints object to publish even when a selector is set.
        if matches!(
            service.spec.service_type,
            Some(rusternetes_common::resources::ServiceType::ExternalName)
        ) {
            debug!(
                "Service {}/{} is ExternalName, skipping endpoint creation",
                namespace, service_name
            );
            return Ok(());
        }

        // Skip services without selectors (headless services without selector)
        let selector = match &service.spec.selector {
            Some(s) if !s.is_empty() => s,
            _ => {
                debug!(
                    "Service {}/{} has no selector, skipping endpoint creation",
                    namespace, service_name
                );
                return Ok(());
            }
        };

        // Find all pods in the same namespace
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Filter pods that match the service selector
        let matching_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|pod| self.pod_matches_selector(pod, selector))
            .collect();

        debug!(
            "Found {} matching pods for service {}/{}",
            matching_pods.len(),
            namespace,
            service_name
        );

        // Build endpoint subsets from matching pods
        let publish_not_ready = service.spec.publish_not_ready_addresses.unwrap_or(false);
        let mut subsets =
            self.build_endpoint_subsets(&matching_pods, &service.spec.ports, publish_not_ready);

        // internalTrafficPolicy=Local: segregate addresses by node so a
        // node-local kube-proxy only consumes endpoints scheduled on the same
        // node. Mirrors upstream behavior where the proxy filters endpoints by
        // the local node's name (pkg/proxy topology / internalTrafficPolicy).
        // We surface that here as one EndpointSubset per node, ensuring no
        // subset mixes addresses from more than one node.
        if matches!(
            service.spec.internal_traffic_policy,
            Some(rusternetes_common::resources::ServiceInternalTrafficPolicy::Local)
        ) {
            subsets = split_subsets_by_node(subsets);
        }

        // Enforce the upstream 1000-address Endpoints capacity cap and emit
        // the `endpoints.kubernetes.io/over-capacity=truncated` annotation
        // when truncation actually fires. Mirrors
        // `pkg/controller/endpoint/endpoints_controller.go::truncateEndpoints`
        // (`maxCapacity = 1000`). Endpoints with > 1000 addresses won't
        // route correctly via kube-proxy, so this is a hard cap.
        let truncated = truncate_endpoint_subsets(&mut subsets, MAX_ENDPOINTS_CAPACITY);

        // Derive annotations: copy from the service, then set/clear the
        // over-capacity marker based on whether we truncated this reconcile.
        let mut annotations = service.metadata.annotations.clone().unwrap_or_default();
        if truncated {
            annotations.insert(
                ENDPOINTS_OVER_CAPACITY_ANNOTATION.to_string(),
                "truncated".to_string(),
            );
        } else {
            annotations.remove(ENDPOINTS_OVER_CAPACITY_ANNOTATION);
        }
        let annotations = if annotations.is_empty() {
            None
        } else {
            Some(annotations)
        };

        // Create or update endpoints
        let mut endpoints = Endpoints {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Endpoints".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: service_name.clone(),
                generate_name: None,
                generation: None,
                managed_fields: None,
                namespace: Some(namespace.clone()),
                uid: String::new(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: Some(vec![OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "Service".to_string(),
                    name: service_name.clone(),
                    uid: service.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: service.metadata.labels.clone(),
                annotations,
            },
            subsets,
        };

        let endpoints_key = build_key("endpoints", Some(namespace), service_name);
        // Check if existing endpoints match — skip write if nothing changed
        if let Ok(existing) = self.storage.get::<Endpoints>(&endpoints_key).await {
            if existing.subsets == endpoints.subsets {
                debug!(
                    "Endpoints for service {}/{} unchanged, skipping write",
                    namespace, service_name
                );
                return Ok(());
            }
            // Preserve resource version for update
            endpoints.metadata.resource_version = existing.metadata.resource_version;
        }

        // Try to update first, if it doesn't exist, create it
        match self.storage.update(&endpoints_key, &endpoints).await {
            Ok(_) => {}
            Err(rusternetes_common::Error::NotFound(_)) => {
                self.storage.create(&endpoints_key, &endpoints).await?;
            }
            Err(e) => return Err(e.into()),
        }

        debug!(
            "Updated endpoints for service {}/{} with {} subsets",
            namespace,
            service_name,
            endpoints.subsets.len()
        );

        Ok(())
    }

    /// Check if a pod matches the service selector
    fn pod_matches_selector(&self, pod: &Pod, selector: &HashMap<String, String>) -> bool {
        let pod_labels = match &pod.metadata.labels {
            Some(labels) => labels,
            None => return false,
        };

        // All selector labels must match pod labels
        selector
            .iter()
            .all(|(key, value)| pod_labels.get(key).map(|v| v == value).unwrap_or(false))
    }

    /// Build endpoint subsets from pods, separating ready and not-ready pods.
    /// K8s ref: pkg/controller/endpoint/endpoints_controller.go — syncService
    fn build_endpoint_subsets(
        &self,
        pods: &[&Pod],
        service_ports: &[rusternetes_common::resources::ServicePort],
        publish_not_ready: bool,
    ) -> Vec<EndpointSubset> {
        // Separate pods by readiness
        let mut ready_addresses = Vec::new();
        let mut not_ready_addresses = Vec::new();

        for pod in pods {
            // K8s ShouldPodBeInEndpoints() checks:
            // 1. Skip terminal pods (Succeeded/Failed)
            // 2. Skip pods without IPs
            // 3. Skip terminating pods (unless publishNotReadyAddresses)
            // See: staging/src/k8s.io/endpointslice/util/controller_utils.go
            let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
            if matches!(
                phase,
                Some(rusternetes_common::types::Phase::Succeeded)
                    | Some(rusternetes_common::types::Phase::Failed)
            ) {
                continue; // Terminal pods are never in endpoints
            }
            if !publish_not_ready && pod.metadata.deletion_timestamp.is_some() {
                continue; // Terminating pods excluded unless publishNotReady
            }

            // Skip pods without an IP address
            let pod_ip = match &pod.status {
                Some(status) => match &status.pod_ip {
                    Some(ip) if !ip.is_empty() => ip.clone(),
                    _ => {
                        debug!(
                            "Pod {}/{} has no IP, skipping",
                            pod.metadata
                                .namespace
                                .as_ref()
                                .unwrap_or(&"default".to_string()),
                            &pod.metadata.name
                        );
                        continue;
                    }
                },
                None => {
                    debug!(
                        "Pod {}/{} has no status, skipping",
                        pod.metadata
                            .namespace
                            .as_ref()
                            .unwrap_or(&"default".to_string()),
                        &pod.metadata.name
                    );
                    continue;
                }
            };

            let address = EndpointAddress {
                ip: pod_ip,
                hostname: pod.spec.as_ref().and_then(|s| {
                    // Only set hostname when subdomain is also set (K8s behavior)
                    if s.subdomain.is_some() {
                        s.hostname.clone().or(Some(pod.metadata.name.clone()))
                    } else {
                        None
                    }
                }),
                node_name: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
                target_ref: Some(EndpointReference {
                    kind: Some("Pod".to_string()),
                    namespace: pod.metadata.namespace.clone(),
                    name: Some(pod.metadata.name.clone()),
                    uid: Some(pod.metadata.uid.clone()),
                    ..Default::default()
                }),
            };

            // Check if pod is ready — if publishNotReadyAddresses, all go to ready
            if publish_not_ready || self.is_pod_ready(pod) {
                ready_addresses.push(address);
            } else {
                not_ready_addresses.push(address);
            }
        }

        // Check if any service port uses a named targetPort — if so, we need to
        // resolve per-pod and group by resolved port tuple to create separate subsets.
        let has_named_target_port = service_ports.iter().any(|sp| {
            matches!(&sp.target_port, Some(rusternetes_common::resources::IntOrString::String(s)) if s.parse::<u16>().is_err())
        });

        if has_named_target_port {
            // Group pods by their resolved port tuple, since different pods may have
            // different containerPort values for the same named port.
            let mut port_groups: std::collections::HashMap<
                Vec<u16>,
                (Vec<EndpointAddress>, Vec<EndpointAddress>),
            > = std::collections::HashMap::new();

            for pod in pods {
                let pod_ip = match &pod.status {
                    Some(status) => match &status.pod_ip {
                        Some(ip) if !ip.is_empty() => ip.clone(),
                        _ => continue,
                    },
                    None => continue,
                };

                // Resolve each service port for this pod
                let resolved_ports: Vec<u16> = service_ports
                    .iter()
                    .map(|sp| {
                        match &sp.target_port {
                            Some(rusternetes_common::resources::IntOrString::Int(p)) => *p as u16,
                            Some(rusternetes_common::resources::IntOrString::String(s)) => {
                                if let Ok(p) = s.parse::<u16>() {
                                    p
                                } else {
                                    // Look up named port in pod's containers
                                    Self::resolve_named_port(pod, s).unwrap_or(sp.port)
                                }
                            }
                            None => sp.port,
                        }
                    })
                    .collect();

                let address = EndpointAddress {
                    ip: pod_ip,
                    hostname: pod.spec.as_ref().and_then(|s| {
                        // Only set hostname when subdomain is also set (K8s behavior)
                        if s.subdomain.is_some() {
                            s.hostname.clone().or(Some(pod.metadata.name.clone()))
                        } else {
                            None
                        }
                    }),
                    node_name: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
                    target_ref: Some(EndpointReference {
                        kind: Some("Pod".to_string()),
                        namespace: pod.metadata.namespace.clone(),
                        name: Some(pod.metadata.name.clone()),
                        uid: Some(pod.metadata.uid.clone()),
                        ..Default::default()
                    }),
                };

                let entry = port_groups
                    .entry(resolved_ports)
                    .or_insert_with(|| (Vec::new(), Vec::new()));
                if self.is_pod_ready(pod) {
                    entry.0.push(address);
                } else {
                    entry.1.push(address);
                }
            }

            // Create one subset per unique port combination
            let mut subsets = Vec::new();
            for (resolved_ports, (ready, not_ready)) in port_groups {
                if ready.is_empty() && not_ready.is_empty() {
                    continue;
                }
                let endpoint_ports: Vec<EndpointPort> = service_ports
                    .iter()
                    .zip(resolved_ports.iter())
                    .map(|(sp, &port)| EndpointPort {
                        name: sp.name.clone(),
                        port,
                        protocol: sp.protocol.clone(),
                        app_protocol: None,
                    })
                    .collect();
                subsets.push(EndpointSubset {
                    addresses: if ready.is_empty() { None } else { Some(ready) },
                    not_ready_addresses: if not_ready.is_empty() {
                        None
                    } else {
                        Some(not_ready)
                    },
                    ports: if endpoint_ports.is_empty() {
                        None
                    } else {
                        Some(endpoint_ports)
                    },
                });
            }
            subsets
        } else {
            // No named ports — simple path: all pods share the same port tuple
            let endpoint_ports: Vec<EndpointPort> = service_ports
                .iter()
                .map(|sp| EndpointPort {
                    name: sp.name.clone(),
                    port: match &sp.target_port {
                        Some(rusternetes_common::resources::IntOrString::Int(p)) => *p as u16,
                        None => sp.port,
                        _ => sp.port,
                    },
                    protocol: sp.protocol.clone(),
                    app_protocol: None,
                })
                .collect();

            let mut subsets = Vec::new();
            if !ready_addresses.is_empty() || !not_ready_addresses.is_empty() {
                subsets.push(EndpointSubset {
                    addresses: if ready_addresses.is_empty() {
                        None
                    } else {
                        Some(ready_addresses)
                    },
                    not_ready_addresses: if not_ready_addresses.is_empty() {
                        None
                    } else {
                        Some(not_ready_addresses)
                    },
                    ports: if endpoint_ports.is_empty() {
                        None
                    } else {
                        Some(endpoint_ports)
                    },
                });
            }
            subsets
        }
    }

    /// Resolve a named port to its containerPort value by searching the pod's containers.
    fn resolve_named_port(pod: &Pod, port_name: &str) -> Option<u16> {
        pod.spec.as_ref()?.containers.iter().find_map(|c| {
            c.ports.as_ref()?.iter().find_map(|p| {
                if p.name.as_deref() == Some(port_name) {
                    Some(p.container_port)
                } else {
                    None
                }
            })
        })
    }

    /// Check if a pod is ready by examining its conditions
    fn is_pod_ready(&self, pod: &Pod) -> bool {
        if let Some(conditions) = pod.status.as_ref().and_then(|s| s.conditions.as_ref()) {
            conditions
                .iter()
                .any(|c| c.condition_type == "Ready" && c.status == "True")
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_pod_matches_selector() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController { storage };

        let mut pod_labels = HashMap::new();
        pod_labels.insert("app".to_string(), "nginx".to_string());
        pod_labels.insert("tier".to_string(), "frontend".to_string());

        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "test-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: String::new(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: Some(pod_labels),
                annotations: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                init_containers: None,
                containers: vec![],
                volumes: None,
                restart_policy: None,
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                ephemeral_containers: None,
                overhead: None,
                scheduler_name: None,
                topology_spread_constraints: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: None,
        };

        // Test exact match
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "nginx".to_string());
        assert!(controller.pod_matches_selector(&pod, &selector));

        // Test multiple labels
        selector.insert("tier".to_string(), "frontend".to_string());
        assert!(controller.pod_matches_selector(&pod, &selector));

        // Test mismatch
        selector.insert("app".to_string(), "apache".to_string());
        assert!(!controller.pod_matches_selector(&pod, &selector));

        // Test missing label
        selector.clear();
        selector.insert("nonexistent".to_string(), "value".to_string());
        assert!(!controller.pod_matches_selector(&pod, &selector));
    }

    #[tokio::test]
    async fn test_is_pod_ready() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController { storage };

        // Pod without status
        let pod_no_status = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "test-pod".to_string(),
                namespace: None,
                uid: String::new(),
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
            spec: Some(rusternetes_common::resources::PodSpec {
                init_containers: None,
                containers: vec![],
                volumes: None,
                restart_policy: None,
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                ephemeral_containers: None,
                overhead: None,
                scheduler_name: None,
                topology_spread_constraints: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: None,
        };
        assert!(!controller.is_pod_ready(&pod_no_status));

        // Pod in Pending phase
        let pod_pending = Pod {
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Pending),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
            ..pod_no_status.clone()
        };
        assert!(!controller.is_pod_ready(&pod_pending));

        // Pod with Ready condition = True
        let pod_ready = Pod {
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                container_statuses: Some(vec![rusternetes_common::resources::ContainerStatus {
                    name: "nginx".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: None,
                    last_state: None,
                    image: Some("nginx:latest".to_string()),
                    image_id: None,
                    container_id: Some("container-123".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
            ..pod_no_status.clone()
        };
        assert!(controller.is_pod_ready(&pod_ready));

        // Pod with Ready condition = False (not ready)
        let pod_not_ready = Pod {
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "False".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                container_statuses: Some(vec![rusternetes_common::resources::ContainerStatus {
                    name: "nginx".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: None,
                    last_state: None,
                    image: Some("nginx:latest".to_string()),
                    image_id: None,
                    container_id: Some("container-123".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
            ..pod_no_status
        };
        assert!(!controller.is_pod_ready(&pod_not_ready));
    }

    /// Test that reconcile_all skips writing endpoints when nothing has changed.
    /// This prevents unnecessary etcd writes that would cause log spam and I/O pressure.
    #[tokio::test]
    async fn test_endpoints_skip_write_when_unchanged() {
        use rusternetes_common::resources::{Service, ServicePort, ServiceSpec};

        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());

        // Create a service with a selector
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-svc")
                .with_namespace("default"),
            spec: ServiceSpec {
                selector: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(rusternetes_common::resources::IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: Some("10.96.0.100".to_string()),
                cluster_ips: None,
                service_type: Some(rusternetes_common::resources::ServiceType::ClusterIP),
                external_ips: None,
                session_affinity: None,
                load_balancer_ip: None,
                external_name: None,
                external_traffic_policy: None,
                internal_traffic_policy: None,
                ip_families: None,
                ip_family_policy: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                allocate_load_balancer_node_ports: None,
                load_balancer_class: None,
                load_balancer_source_ranges: None,
                health_check_node_port: None,
                traffic_distribution: None,
            },
            status: None,
        };
        let svc_key = "/registry/services/default/test-svc";
        storage.create(svc_key, &service).await.unwrap();

        // First reconcile — should create endpoints
        controller.reconcile_all().await.unwrap();

        // Get the endpoints and their resource version
        let ep_key = "/registry/endpoints/default/test-svc";
        let ep1: Endpoints = storage.get(ep_key).await.unwrap();
        let rv1 = ep1.metadata.resource_version.clone();

        // Second reconcile — nothing changed, should skip the write
        controller.reconcile_all().await.unwrap();

        let ep2: Endpoints = storage.get(ep_key).await.unwrap();
        let rv2 = ep2.metadata.resource_version.clone();

        // Resource version should be unchanged because no write occurred
        assert_eq!(
            rv1, rv2,
            "Endpoints resource version changed despite no data change — \
             this means the controller is doing unnecessary writes every reconcile loop"
        );

        // Verify the subsets are correct (empty since no pods match)
        assert!(ep2.subsets.is_empty());
    }

    /// Test that endpoints ARE updated when pod data actually changes.
    #[tokio::test]
    async fn test_endpoints_write_when_changed() {
        use rusternetes_common::resources::{PodStatus, Service, ServicePort, ServiceSpec};
        use rusternetes_common::types::Phase;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());

        // Create a service
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-svc")
                .with_namespace("default"),
            spec: ServiceSpec {
                selector: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(rusternetes_common::resources::IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: Some("10.96.0.100".to_string()),
                cluster_ips: None,
                service_type: Some(rusternetes_common::resources::ServiceType::ClusterIP),
                external_ips: None,
                session_affinity: None,
                load_balancer_ip: None,
                external_name: None,
                external_traffic_policy: None,
                internal_traffic_policy: None,
                ip_families: None,
                ip_family_policy: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                allocate_load_balancer_node_ports: None,
                load_balancer_class: None,
                load_balancer_source_ranges: None,
                health_check_node_port: None,
                traffic_distribution: None,
            },
            status: None,
        };
        let svc_key = "/registry/services/default/test-svc";
        storage.create(svc_key, &service).await.unwrap();

        // First reconcile — creates empty endpoints
        controller.reconcile_all().await.unwrap();
        let ep_key = "/registry/endpoints/default/test-svc";
        let ep1: Endpoints = storage.get(ep_key).await.unwrap();
        let _rv1 = ep1.metadata.resource_version.clone();
        assert!(ep1.subsets.is_empty());

        // Now add a matching pod with an IP and Running phase
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "web-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: "pod-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.244.0.5".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_probe_time: None,
                    last_transition_time: None,
                    reason: None,
                    message: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/web-pod", &pod)
            .await
            .unwrap();

        // Second reconcile — should detect the new pod and update endpoints
        controller.reconcile_all().await.unwrap();
        let ep2: Endpoints = storage.get(ep_key).await.unwrap();

        // Subsets should have changed from empty to containing the pod
        assert_ne!(
            ep1.subsets, ep2.subsets,
            "Endpoints subsets didn't change even though a new pod was added — \
             the skip-unchanged optimization is too aggressive"
        );

        // Verify the pod's IP is in the endpoints
        assert!(
            !ep2.subsets.is_empty(),
            "Subsets should not be empty after adding a matching pod"
        );
        let addresses = ep2.subsets[0].addresses.as_ref().unwrap();
        assert_eq!(addresses[0].ip, "10.244.0.5");
    }

    /// Test that EndpointAddress.hostname is populated from pod spec when subdomain is set.
    #[tokio::test]
    async fn test_endpoint_address_hostname_from_pod_spec() {
        use rusternetes_common::resources::{PodStatus, Service, ServicePort, ServiceSpec};
        use rusternetes_common::types::Phase;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());

        // Create a headless service (clusterIP: None) with selector
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("my-svc")
                .with_namespace("default"),
            spec: ServiceSpec {
                selector: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(rusternetes_common::resources::IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: Some("None".to_string()),
                cluster_ips: None,
                service_type: Some(rusternetes_common::resources::ServiceType::ClusterIP),
                external_ips: None,
                session_affinity: None,
                load_balancer_ip: None,
                external_name: None,
                external_traffic_policy: None,
                internal_traffic_policy: None,
                ip_families: None,
                ip_family_policy: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                allocate_load_balancer_node_ports: None,
                load_balancer_class: None,
                load_balancer_source_ranges: None,
                health_check_node_port: None,
                traffic_distribution: None,
            },
            status: None,
        };
        storage
            .create("/registry/services/default/my-svc", &service)
            .await
            .unwrap();

        // Create a pod with hostname and subdomain set
        let pod_with_hostname = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "web-0".to_string(),
                namespace: Some("default".to_string()),
                uid: "pod-uid-hostname".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![],
                hostname: Some("my-host".to_string()),
                subdomain: Some("my-svc".to_string()),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.244.0.10".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_probe_time: None,
                    last_transition_time: None,
                    reason: None,
                    message: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/web-0", &pod_with_hostname)
            .await
            .unwrap();

        // Create a pod WITHOUT subdomain — hostname should NOT be set in endpoint
        let pod_no_subdomain = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "web-1".to_string(),
                namespace: Some("default".to_string()),
                uid: "pod-uid-no-subdomain".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![],
                hostname: Some("my-other-host".to_string()),
                subdomain: None,
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.244.0.11".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_probe_time: None,
                    last_transition_time: None,
                    reason: None,
                    message: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/web-1", &pod_no_subdomain)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile_all().await.unwrap();

        // Get the endpoints
        let ep: Endpoints = storage
            .get("/registry/endpoints/default/my-svc")
            .await
            .unwrap();

        assert!(!ep.subsets.is_empty(), "Endpoints should have subsets");
        let addresses = ep.subsets[0].addresses.as_ref().unwrap();
        assert_eq!(addresses.len(), 2, "Should have 2 ready addresses");

        // Find the address for the pod with hostname+subdomain
        let addr_with_hostname = addresses
            .iter()
            .find(|a| a.ip == "10.244.0.10")
            .expect("Should find address for pod with hostname");
        assert_eq!(
            addr_with_hostname.hostname,
            Some("my-host".to_string()),
            "EndpointAddress should have hostname set when pod has hostname+subdomain"
        );

        // Find the address for the pod WITHOUT subdomain
        let addr_no_subdomain = addresses
            .iter()
            .find(|a| a.ip == "10.244.0.11")
            .expect("Should find address for pod without subdomain");
        assert_eq!(
            addr_no_subdomain.hostname, None,
            "EndpointAddress should NOT have hostname when pod has no subdomain"
        );
    }

    /// Bug regression: `split_subsets_by_node` (internalTrafficPolicy=Local)
    /// must preserve each input subset's ports per node. When the named-port
    /// path produces multiple subsets with DIFFERENT resolved container ports,
    /// pods on a given node must keep the port from the subset they came from —
    /// not the first subset's port applied to every node.
    #[test]
    fn test_split_subsets_by_node_preserves_per_subset_ports() {
        let addr = |ip: &str, node: &str| EndpointAddress {
            ip: ip.to_string(),
            hostname: None,
            node_name: Some(node.to_string()),
            target_ref: None,
        };
        let port = |p: u16| {
            Some(vec![EndpointPort {
                name: Some("http".to_string()),
                port: p,
                protocol: "TCP".to_string(),
                app_protocol: None,
            }])
        };

        // Subset A resolves the named port to 8080 and holds a pod on each node;
        // subset B resolves it to 9090 and holds a different pod on each node.
        let subsets = vec![
            EndpointSubset {
                addresses: Some(vec![addr("10.0.0.1", "node-1"), addr("10.0.0.2", "node-2")]),
                not_ready_addresses: None,
                ports: port(8080),
            },
            EndpointSubset {
                addresses: Some(vec![addr("10.0.1.1", "node-1"), addr("10.0.1.2", "node-2")]),
                not_ready_addresses: None,
                ports: port(9090),
            },
        ];

        let out = split_subsets_by_node(subsets);

        // No subset may mix nodes.
        for s in &out {
            let nodes: std::collections::HashSet<&str> = s
                .addresses
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter_map(|a| a.node_name.as_deref())
                .collect();
            assert!(nodes.len() <= 1, "subset mixes nodes: {s:?}");
        }

        // Every address must carry the port from its ORIGINAL subset, not a
        // foreign one. Build the expected ip->port mapping and assert it holds.
        let expected: std::collections::HashMap<&str, u16> = [
            ("10.0.0.1", 8080),
            ("10.0.0.2", 8080),
            ("10.0.1.1", 9090),
            ("10.0.1.2", 9090),
        ]
        .into_iter()
        .collect();

        for s in &out {
            let subset_port = s.ports.as_ref().and_then(|p| p.first()).map(|p| p.port);
            for a in s.addresses.as_deref().unwrap_or(&[]) {
                assert_eq!(
                    subset_port,
                    Some(expected[a.ip.as_str()]),
                    "address {} got wrong port; subset={:?}",
                    a.ip,
                    s,
                );
            }
        }
    }
}
