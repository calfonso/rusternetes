use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::endpointslice::{
    Endpoint, EndpointConditions, EndpointPort, EndpointReference,
};
use rusternetes_common::resources::{EndpointSlice, Endpoints, Pod, Service};
use rusternetes_common::types::Phase;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

/// EndpointSliceController builds EndpointSlices directly from Services and Pods,
/// following the same approach as the K8s endpointslice controller.
///
/// Key behavior (matching K8s):
/// - Iterates over Services with selectors
/// - Finds matching pods by label selector
/// - For each pod, computes which service ports the pod actually serves
///   using FindPort logic (matching containerPort to service targetPort)
/// - Groups pods by their port mapping
/// - Creates separate EndpointSlices for each port group
///
/// This ensures that pods only appear in EndpointSlices with the ports
/// they actually serve, fixing the conformance test failure where pods
/// were incorrectly associated with all service ports.
pub struct EndpointSliceController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> EndpointSliceController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Watch-based run loop. Watches services, pods, AND endpoints as primary resources.
    /// When a pod changes, we find services whose selector matches the pod
    /// and enqueue them for reconciliation.
    /// When an endpoint changes, we enqueue it for mirroring (services without selectors).
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();
        // Separate queue for endpoints mirroring
        let mirror_queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        let mirror_worker_queue = mirror_queue.clone();
        let mirror_worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            mirror_worker_self.mirror_worker(mirror_worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;
            self.enqueue_all_endpoints(&mirror_queue).await;

            let svc_prefix = build_prefix("services", None);
            let pod_prefix = build_prefix("pods", None);
            let ep_prefix = build_prefix("endpoints", None);

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
            let ep_watch = match self.storage.watch(&ep_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish endpoints watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut svc_watch = svc_watch;
            let mut pod_watch = pod_watch;
            let mut ep_watch = ep_watch;
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
                    event = ep_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                // Enqueue the endpoint for mirroring
                                let key = extract_key(&ev);
                                // Convert endpoints/ns/name to same format
                                mirror_queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Endpoints watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Endpoints watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                        self.enqueue_all_endpoints(&mirror_queue).await;
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
        selector: &std::collections::HashMap<String, String>,
        labels: &Option<std::collections::HashMap<String, String>>,
    ) -> bool {
        let labels = match labels {
            Some(l) => l,
            None => return selector.is_empty(),
        };
        selector.iter().all(|(k, v)| labels.get(k) == Some(v))
    }

    /// Main reconciliation loop — syncs EndpointSlices for all Services
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
                        tracing::error!("Failed to reconcile {}: {}", key, e);
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

    /// Worker for mirroring Endpoints to EndpointSlices.
    /// K8s has a separate endpointslice-mirroring-controller that watches Endpoints
    /// and creates EndpointSlices for services without selectors and standalone Endpoints.
    async fn mirror_worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            match self.mirror_endpoint(ns, name).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    tracing::error!("Failed to mirror endpoint {}/{}: {}", ns, name, e);
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Mirror a single Endpoints resource into an EndpointSlice.
    /// Only mirrors Endpoints for services without selectors or standalone Endpoints.
    /// K8s ref: pkg/controller/endpointslicemirroring/reconciler.go
    async fn mirror_endpoint(&self, ns: &str, name: &str) -> Result<()> {
        let ep_key = build_key("endpoints", Some(ns), name);

        // Check if the Endpoints resource still exists
        let ep: Endpoints = match self.storage.get(&ep_key).await {
            Ok(ep) => ep,
            Err(_) => {
                // Endpoints deleted — clean up mirrored EndpointSlices
                let es_prefix = build_prefix("endpointslices", Some(ns));
                let existing_slices: Vec<EndpointSlice> =
                    self.storage.list(&es_prefix).await.unwrap_or_default();
                for slice in &existing_slices {
                    let managed = slice
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                        .map(|m| m == "endpointslice-mirroring-controller.k8s.io")
                        .unwrap_or(false);
                    let owned = slice
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("kubernetes.io/service-name"))
                        .map(|n| n == name)
                        .unwrap_or(false);
                    if managed && owned {
                        let slice_key = build_key("endpointslices", Some(ns), &slice.metadata.name);
                        let _ = self.storage.delete(&slice_key).await;
                        debug!(
                            "Deleted mirrored EndpointSlice {}/{} (source Endpoints deleted)",
                            ns, slice.metadata.name
                        );
                    }
                }
                return Ok(());
            }
        };

        // Skip Endpoints that have the skip-mirror label
        // K8s ref: endpointslicemirroring/utils.go — shouldMirror()
        if let Some(ref labels) = ep.metadata.labels {
            if labels.get("endpointslice.kubernetes.io/skip-mirror") == Some(&"true".to_string()) {
                return Ok(());
            }
        }

        // Check if a Service with a SELECTOR exists for this name.
        // If so, the main endpointslice controller handles it, skip mirroring.
        let svc_key = build_key("services", Some(ns), name);
        if let Ok(svc) = self.storage.get::<Service>(&svc_key).await {
            let has_selector = svc
                .spec
                .selector
                .as_ref()
                .map(|sel| !sel.is_empty())
                .unwrap_or(false);
            if has_selector {
                return Ok(());
            }
        }

        // Mirror the Endpoints to EndpointSlice(s).
        // Use a unique slice name with "-mirror" suffix to avoid conflicts
        // with selector-based EndpointSlices that use the service name directly.
        let endpointslices = EndpointSlice::from_endpoints(&ep);

        // If Endpoints has empty subsets, still create at least one empty EndpointSlice
        // so the test can find it. K8s mirroring controller always creates at least
        // one EndpointSlice per mirrored Endpoints object.
        let slices_to_create = if endpointslices.is_empty() {
            let mut empty_slice = EndpointSlice::new(name, "IPv4");
            empty_slice.metadata.namespace = Some(ns.to_string());
            vec![empty_slice]
        } else {
            endpointslices
        };

        // Mirror under the bare endpoints name when free; otherwise suffix
        // with "-mirrored" to avoid colliding with a selector-based slice.
        let bare_owned_by_selector = self.slice_owned_by_selector_controller(ns, name).await;

        for (idx, mut slice) in slices_to_create.into_iter().enumerate() {
            let slice_name = Self::mirrored_slice_name(name, idx, bare_owned_by_selector);
            slice.metadata.name = slice_name.clone();
            slice.metadata.namespace = Some(ns.to_string());

            let labels = slice.metadata.labels.get_or_insert_with(Default::default);
            labels.insert("kubernetes.io/service-name".to_string(), name.to_string());
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-mirroring-controller.k8s.io".to_string(),
            );

            slice.metadata.owner_references =
                Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "Endpoints".to_string(),
                    name: name.to_string(),
                    uid: ep.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);

            let slice_key = build_key("endpointslices", Some(ns), &slice_name);
            match self.storage.get::<EndpointSlice>(&slice_key).await {
                Ok(existing) => {
                    if existing.endpoints == slice.endpoints && existing.ports == slice.ports {
                        continue;
                    }
                    slice.metadata.resource_version = existing.metadata.resource_version;
                    match self.storage.update(&slice_key, &slice).await {
                        Ok(_) => {
                            debug!("Updated mirrored EndpointSlice {}/{}", ns, slice_name);
                        }
                        Err(e) => {
                            debug!(
                                "Failed to update mirrored EndpointSlice {}/{}: {}",
                                ns, slice_name, e
                            );
                        }
                    }
                }
                Err(_) => {
                    // Slice doesn't exist — create it
                    slice.metadata.resource_version = None;
                    match self.storage.create(&slice_key, &slice).await {
                        Ok(_) => {
                            info!("Created mirrored EndpointSlice {}/{}", ns, slice_name);
                        }
                        Err(e) => {
                            debug!(
                                "Failed to create mirrored EndpointSlice {}/{}: {}",
                                ns, slice_name, e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
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

    /// Enqueue all Endpoints for mirroring
    async fn enqueue_all_endpoints(&self, queue: &WorkQueue) {
        match self.storage.list::<Endpoints>("/registry/endpoints/").await {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("endpoints/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                tracing::error!("Failed to list endpoints for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting endpointslice reconciliation");

        // List all services across all namespaces
        let services: Vec<Service> = self
            .storage
            .list(&build_prefix("services", None))
            .await
            .unwrap_or_default();

        // Track which service names we've seen for orphan cleanup
        let mut service_names: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        for service in &services {
            let ns = service.metadata.namespace.as_deref().unwrap_or("default");
            service_names.insert((ns.to_string(), service.metadata.name.clone()));

            if let Err(e) = self.reconcile_service(service).await {
                error!(
                    "Failed to reconcile endpointslices for service {}/{}: {}",
                    ns, service.metadata.name, e
                );
            }
        }

        // Mirror Endpoints that don't have a corresponding Service.
        // K8s has a separate EndpointSlice mirroring controller for this.
        // This handles manually-created Endpoints and headless services without selectors.
        let all_endpoints: Vec<Endpoints> = self
            .storage
            .list(&build_prefix("endpoints", None))
            .await
            .unwrap_or_default();

        for ep in &all_endpoints {
            let ns = ep.metadata.namespace.as_deref().unwrap_or("default");
            let ep_name = &ep.metadata.name;

            // Skip if a Service with a SELECTOR exists (handled by the main loop above).
            // Services WITHOUT selectors need manual endpoint management via mirroring.
            // K8s endpointslice-mirroring-controller mirrors Endpoints for:
            // - Services without selectors
            // - Standalone Endpoints without a matching Service
            let svc_has_selector = services.iter().any(|s| {
                s.metadata.namespace.as_deref().unwrap_or("default") == ns
                    && s.metadata.name == *ep_name
                    && s.spec
                        .selector
                        .as_ref()
                        .map(|sel| !sel.is_empty())
                        .unwrap_or(false)
            });
            if svc_has_selector {
                continue;
            }

            // Mirror this Endpoints object to an EndpointSlice
            let endpointslices = EndpointSlice::from_endpoints(ep);

            // If Endpoints has empty subsets, create at least one empty EndpointSlice
            let slices_to_create = if endpointslices.is_empty() {
                let mut empty_slice = EndpointSlice::new(ep_name, "IPv4");
                empty_slice.metadata.namespace = Some(ns.to_string());
                vec![empty_slice]
            } else {
                endpointslices
            };

            let bare_owned_by_selector = self.slice_owned_by_selector_controller(ns, ep_name).await;

            for (idx, mut slice) in slices_to_create.into_iter().enumerate() {
                let slice_name = Self::mirrored_slice_name(ep_name, idx, bare_owned_by_selector);
                slice.metadata.name = slice_name.clone();
                slice.metadata.namespace = Some(ns.to_string());

                let labels = slice.metadata.labels.get_or_insert_with(Default::default);
                labels.insert("kubernetes.io/service-name".to_string(), ep_name.clone());
                labels.insert(
                    "endpointslice.kubernetes.io/managed-by".to_string(),
                    "endpointslice-mirroring-controller.k8s.io".to_string(),
                );

                slice.metadata.owner_references =
                    Some(vec![rusternetes_common::types::OwnerReference {
                        api_version: "v1".to_string(),
                        kind: "Endpoints".to_string(),
                        name: ep_name.clone(),
                        uid: ep.metadata.uid.clone(),
                        controller: Some(true),
                        block_owner_deletion: Some(true),
                    }]);

                let slice_key = build_key("endpointslices", Some(ns), &slice_name);
                match self.storage.get::<EndpointSlice>(&slice_key).await {
                    Ok(existing) => {
                        if existing.endpoints == slice.endpoints && existing.ports == slice.ports {
                            continue;
                        }
                        slice.metadata.resource_version = existing.metadata.resource_version;
                        let _ = self.storage.update(&slice_key, &slice).await;
                    }
                    Err(_) => {
                        slice.metadata.resource_version = None;
                        let _ = self.storage.create(&slice_key, &slice).await;
                    }
                }
            }
        }

        // Clean up orphaned EndpointSlices (whose Service no longer exists)
        let all_slices: Vec<EndpointSlice> = self
            .storage
            .list(&build_prefix("endpointslices", None))
            .await
            .unwrap_or_default();

        for slice in all_slices {
            let ns = slice.metadata.namespace.as_deref().unwrap_or("default");
            let svc_name = slice
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|s| s.as_str());

            if let Some(svc_name) = svc_name {
                // Only delete slices managed by our controllers
                let managed_by = slice
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                    .map(|v| v.as_str());

                let should_delete = match managed_by {
                    Some("endpointslice-controller.k8s.io") => {
                        // Selector-based: delete if service no longer exists
                        !service_names.contains(&(ns.to_string(), svc_name.to_string()))
                    }
                    Some("endpointslice-mirroring-controller.k8s.io") => {
                        // Mirrored: delete if source Endpoints no longer exists
                        // K8s ref: pkg/controller/endpointslicemirroring/reconciler.go
                        let ep_key = build_key("endpoints", Some(ns), svc_name);
                        self.storage
                            .get::<serde_json::Value>(&ep_key)
                            .await
                            .is_err()
                    }
                    _ => false,
                };

                if should_delete {
                    let key = build_key("endpointslices", Some(ns), &slice.metadata.name);
                    debug!(
                        "Deleting orphaned EndpointSlice {}/{}",
                        ns, slice.metadata.name
                    );
                    let _ = self.storage.delete(&key).await;
                }
            }
        }

        Ok(())
    }

    /// Reconcile EndpointSlices for a single Service
    async fn reconcile_service(&self, service: &Service) -> Result<()> {
        let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
        let service_name = &service.metadata.name;

        // Skip ExternalName services (K8s: services with Type ExternalName receive no endpoints)
        if matches!(
            service.spec.service_type,
            Some(rusternetes_common::resources::ServiceType::ExternalName)
        ) {
            return Ok(());
        }

        // Services without selectors (nil selector) are skipped.
        // Services with EMPTY selectors (selector: {}) still get EndpointSlices
        // but with no endpoints. K8s endpointslice controller handles both cases.
        let selector = match &service.spec.selector {
            Some(s) => s,
            None => return Ok(()), // nil selector = skip (headless without selector)
        };

        // Log at INFO so we can confirm the controller reconciles after pod changes
        info!(
            "Reconciling endpointslices for service {}/{} (matching_pods will follow)",
            namespace, service_name
        );

        // Find all pods matching the service's label selector
        let all_pods: Vec<Pod> = self
            .storage
            .list(&build_prefix("pods", Some(namespace)))
            .await
            .unwrap_or_default();

        // K8s endpointslice controller keeps terminating pods in slices with
        // terminating=true (rather than dropping them). Dropping them on the first
        // reconcile breaks rolling updates and the "serve" conformance tests that
        // race-check endpoint serving as pods are recreated. Only pods that are
        // fully terminal (Succeeded/Failed) are excluded.
        let publish_not_ready = service.spec.publish_not_ready_addresses.unwrap_or(false);

        let matching_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|pod| {
                // Match label selector
                if let Some(pod_labels) = &pod.metadata.labels {
                    selector.iter().all(|(k, v)| pod_labels.get(k) == Some(v))
                } else {
                    false
                }
            })
            .filter(|pod| {
                // K8s ShouldPodBeInEndpointSlice() excludes only:
                // - Pods in terminal phase (Succeeded/Failed)
                // - Pods without an IP
                // Terminating pods (with deletionTimestamp) are KEPT and marked
                // terminating=true so kube-proxy can drain them gracefully.
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                !matches!(phase, Some(Phase::Succeeded) | Some(Phase::Failed))
            })
            .collect();

        info!(
            "EndpointSlice reconcile {}/{}: {} matching pods (from {} total in namespace)",
            namespace,
            service_name,
            matching_pods.len(),
            all_pods.len()
        );

        // Topology-aware routing. When a Service requests topology hints — via
        // `trafficDistribution: PreferClose` or the
        // `service.kubernetes.io/topology-mode: Auto` annotation — the
        // EndpointSlice controller stamps each endpoint with its node's zone
        // and a matching `hints.forZones` so kube-proxy can prefer same-zone
        // endpoints. Mirrors upstream pkg/controller/endpointslice/topologycache
        // (here simplified to a per-endpoint same-zone hint rather than the full
        // proportional-allocation heuristic).
        let topology_enabled = Self::topology_hints_requested(service);
        let node_zones: HashMap<String, String> = if topology_enabled {
            self.build_node_zone_map().await
        } else {
            HashMap::new()
        };

        // Group pods by their resolved port mapping.
        // Each group gets its own EndpointSlice with only the ports those pods serve.
        // This follows K8s's reconcileByAddressType / getEndpointPorts pattern.
        // Use (serialized ports, ports) as key since EndpointPort doesn't implement Hash.
        let mut port_groups: HashMap<String, (Vec<EndpointPort>, Vec<Endpoint>)> = HashMap::new();

        for pod in &matching_pods {
            let endpoint_ports = self.get_endpoint_ports(service, pod);
            if endpoint_ports.is_empty() {
                continue; // Pod doesn't serve any of the service's ports
            }

            let pod_ip = pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.as_ref())
                .filter(|ip| !ip.is_empty());

            let Some(ip) = pod_ip else {
                continue; // Pod has no IP
            };

            let pod_ready = pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conditions| {
                    conditions
                        .iter()
                        .any(|c| c.condition_type == "Ready" && c.status == "True")
                })
                .unwrap_or(false);

            let is_terminating = pod.metadata.deletion_timestamp.is_some();

            // K8s endpointslice condition semantics:
            // - ready: pod is Ready AND not terminating, OR publishNotReadyAddresses=true
            // - serving: pod is Ready (independent of terminating state)
            // - terminating: pod has deletionTimestamp
            // Ref: pkg/controller/endpointslice/utils.go — podEndpointConditions
            let ready = if publish_not_ready {
                true
            } else {
                pod_ready && !is_terminating
            };
            let serving = if publish_not_ready { true } else { pod_ready };

            let endpoint = Endpoint {
                addresses: vec![ip.clone()],
                conditions: Some(EndpointConditions {
                    ready: Some(ready),
                    serving: Some(serving),
                    terminating: Some(is_terminating),
                }),
                hostname: pod.spec.as_ref().and_then(|s| {
                    if s.subdomain.is_some() {
                        s.hostname.clone().or(Some(pod.metadata.name.clone()))
                    } else {
                        None
                    }
                }),
                target_ref: Some(EndpointReference {
                    kind: Some("Pod".to_string()),
                    namespace: pod.metadata.namespace.clone(),
                    name: Some(pod.metadata.name.clone()),
                    uid: Some(pod.metadata.uid.clone()),
                    resource_version: None,
                    field_path: None,
                    ..Default::default()
                }),
                node_name: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
                zone: None,
                hints: None,
                deprecated_topology: None,
            };

            // Stamp zone + same-zone hint when topology routing is enabled and
            // the backing node carries a topology.kubernetes.io/zone label.
            let mut endpoint = endpoint;
            if topology_enabled {
                if let Some(node_name) = endpoint.node_name.as_ref() {
                    if let Some(zone) = node_zones.get(node_name) {
                        endpoint.zone = Some(zone.clone());
                        endpoint.hints = Some(
                            rusternetes_common::resources::endpointslice::EndpointHints {
                                for_zones: Some(vec![
                                    rusternetes_common::resources::endpointslice::ForZone {
                                        name: zone.clone(),
                                    },
                                ]),
                                for_nodes: None,
                            },
                        );
                    }
                }
            }

            let port_key = serde_json::to_string(&endpoint_ports).unwrap_or_default();
            port_groups
                .entry(port_key)
                .or_insert_with(|| (endpoint_ports, Vec::new()))
                .1
                .push(endpoint);
        }

        // If no port groups were created but the service has ports,
        // create an empty slice with the service's ports
        if port_groups.is_empty() && !service.spec.ports.is_empty() {
            let ports: Vec<EndpointPort> = service
                .spec
                .ports
                .iter()
                .map(|sp| EndpointPort {
                    // K8s always sets port name, even if empty string.
                    // A nil name causes kubectl describe to crash (nil pointer deref).
                    name: Some(sp.name.clone().unwrap_or_default()),
                    port: Some(sp.port as i32),
                    protocol: sp.protocol.clone(),
                    app_protocol: sp.app_protocol.clone(),
                })
                .collect();
            let port_key = serde_json::to_string(&ports).unwrap_or_default();
            port_groups.insert(port_key, (ports, Vec::new()));
        }

        // Create/update EndpointSlices for each port group
        let port_group_count = port_groups.len();
        for (idx, (_key, (ports, endpoints))) in port_groups.into_iter().enumerate() {
            let slice_name = if idx == 0 {
                service_name.clone()
            } else {
                format!("{}-{}", service_name, idx)
            };

            let mut slice = EndpointSlice::new(&slice_name, "IPv4");
            slice.metadata.namespace = Some(namespace.to_string());
            slice.ports = ports;
            slice.endpoints = endpoints;

            // Set labels
            let labels = slice.metadata.labels.get_or_insert_with(Default::default);
            labels.insert(
                "kubernetes.io/service-name".to_string(),
                service_name.clone(),
            );
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-controller.k8s.io".to_string(),
            );

            // Set owner reference to the Service
            slice.metadata.owner_references =
                Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "Service".to_string(),
                    name: service_name.clone(),
                    uid: service.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);

            let slice_key = build_key("endpointslices", Some(namespace), &slice_name);

            // Check if existing slice matches — skip write if nothing changed
            match self.storage.get::<EndpointSlice>(&slice_key).await {
                Ok(existing) => {
                    if existing.endpoints == slice.endpoints && existing.ports == slice.ports {
                        continue;
                    }
                    slice.metadata.resource_version = existing.metadata.resource_version;
                    match self.storage.update(&slice_key, &slice).await {
                        Ok(_) => {
                            info!(
                                "Updated endpointslice {}/{} for service ({} endpoints)",
                                namespace,
                                slice_name,
                                slice.endpoints.len()
                            );
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(_) => {
                    slice.metadata.resource_version = None;
                    self.storage.create(&slice_key, &slice).await?;
                    info!(
                        "Created endpointslice {}/{} for service",
                        namespace, slice_name
                    );
                }
            }
        }

        // Clean up stale EndpointSlices that are no longer needed.
        // K8s reconciler deletes slices that are no longer in the desired set.
        // Without this, deleted pods leave behind stale EndpointSlices with
        // outdated endpoint entries (causes "extra port mappings" test failures).
        let es_prefix = build_prefix("endpointslices", Some(namespace));
        let existing_slices: Vec<EndpointSlice> =
            self.storage.list(&es_prefix).await.unwrap_or_default();
        for existing in &existing_slices {
            // Only manage slices owned by this service
            let owned = existing
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|n| n == service_name)
                .unwrap_or(false);
            let managed = existing
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                .map(|m| m == "endpointslice-controller.k8s.io")
                .unwrap_or(false);
            if !owned || !managed {
                continue;
            }
            // Check if this slice name was in our current set
            let slice_name = &existing.metadata.name;
            let in_current_set = (0..port_group_count).any(|idx| {
                let expected = if idx == 0 {
                    service_name.clone()
                } else {
                    format!("{}-{}", service_name, idx)
                };
                slice_name == &expected
            });
            if !in_current_set {
                let stale_key = build_key("endpointslices", Some(namespace), slice_name);
                if let Err(e) = self.storage.delete(&stale_key).await {
                    debug!(
                        "Failed to delete stale endpointslice {}/{}: {}",
                        namespace, slice_name, e
                    );
                } else {
                    info!(
                        "Deleted stale endpointslice {}/{} (no longer needed for service {})",
                        namespace, slice_name, service_name
                    );
                }
            }
        }

        Ok(())
    }

    /// Whether a Service requests topology-aware endpoint hints. Mirrors
    /// upstream which enables hints for `trafficDistribution: PreferClose` or
    /// the legacy `service.kubernetes.io/topology-mode: Auto` annotation.
    fn topology_hints_requested(service: &Service) -> bool {
        if service.spec.traffic_distribution.as_deref() == Some("PreferClose") {
            return true;
        }
        service
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("service.kubernetes.io/topology-mode"))
            .map(|mode| mode == "Auto")
            .unwrap_or(false)
    }

    /// Build a map of node name -> zone from the `topology.kubernetes.io/zone`
    /// label on each Node. Nodes without the label are omitted.
    async fn build_node_zone_map(&self) -> HashMap<String, String> {
        let nodes: Vec<rusternetes_common::resources::Node> = self
            .storage
            .list(&build_prefix("nodes", None))
            .await
            .unwrap_or_default();

        let mut map = HashMap::new();
        for node in nodes {
            if let Some(zone) = node
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("topology.kubernetes.io/zone"))
            {
                map.insert(node.metadata.name.clone(), zone.clone());
            }
        }
        map
    }

    /// Compute endpoint ports for a pod based on the service's port definitions.
    /// Follows K8s's getEndpointPorts() logic:
    /// - For each service port, try to find a matching containerPort on the pod
    /// - If the service uses a named targetPort, look up the container port by name
    /// - If the service uses a numeric targetPort, use it directly
    /// - If no match is found, skip that port (don't include it)
    fn get_endpoint_ports(&self, service: &Service, pod: &Pod) -> Vec<EndpointPort> {
        let mut endpoint_ports = Vec::new();

        for sp in &service.spec.ports {
            let port_num = match self.find_port(pod, sp) {
                Some(p) => p,
                None => continue, // Pod doesn't serve this port
            };

            endpoint_ports.push(EndpointPort {
                // K8s always sets port name, even if empty string
                name: Some(sp.name.clone().unwrap_or_default()),
                port: Some(port_num),
                protocol: sp.protocol.clone(),
                app_protocol: sp.app_protocol.clone(),
            });
        }

        endpoint_ports
    }

    /// Find the container port on a pod that corresponds to a service port.
    /// Implements K8s's FindPort logic:
    /// - If targetPort is a string (named port), search pod containers for a
    ///   containerPort with that name
    /// - If targetPort is a number, use it directly
    /// - If targetPort is not set, use the service port number
    fn find_port(
        &self,
        pod: &Pod,
        service_port: &rusternetes_common::resources::ServicePort,
    ) -> Option<i32> {
        match &service_port.target_port {
            Some(rusternetes_common::resources::IntOrString::String(name)) => {
                // Named port — search pod containers
                if let Ok(port_num) = name.parse::<i32>() {
                    // It's actually a numeric string
                    return Some(port_num);
                }
                // Look up named port in pod containers
                if let Some(spec) = &pod.spec {
                    for container in &spec.containers {
                        if let Some(ports) = &container.ports {
                            for cp in ports {
                                if cp.name.as_deref() == Some(name.as_str()) {
                                    return Some(cp.container_port as i32);
                                }
                            }
                        }
                    }
                }
                None // Pod doesn't have this named port
            }
            Some(rusternetes_common::resources::IntOrString::Int(port)) => Some(*port),
            None => Some(service_port.port as i32), // Default to service port
        }
    }

    /// Check whether the EndpointSlice at `<ns>/<name>` is owned by the
    /// selector-based controller. Used to avoid collisions between selector-
    /// and mirror-managed slices that would otherwise share a name.
    async fn slice_owned_by_selector_controller(&self, ns: &str, name: &str) -> bool {
        let key = build_key("endpointslices", Some(ns), name);
        match self.storage.get::<EndpointSlice>(&key).await {
            Ok(existing) => existing
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                .map(|m| m == "endpointslice-controller.k8s.io")
                .unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Resolve the slice name a mirroring controller should use for the
    /// `idx`-th mirror slice of an Endpoints resource named `name`. Falls back
    /// to a "-mirrored" suffix when the bare name is already owned by a
    /// selector-based slice.
    fn mirrored_slice_name(name: &str, idx: usize, bare_owned: bool) -> String {
        match (idx, bare_owned) {
            (0, false) => name.to_string(),
            (0, true) => format!("{}-mirrored", name),
            (i, true) => format!("{}-mirrored-{}", name, i),
            (i, false) => format!("{}-{}", name, i),
        }
    }

    /// Clean up orphaned EndpointSlices whose owning Service or Endpoints no
    /// longer exists. Used by external callers and tests; the regular
    /// reconcile loop also runs this cleanup.
    #[allow(dead_code)]
    pub async fn cleanup_orphans(&self) -> Result<()> {
        let all_slices: Vec<EndpointSlice> = self
            .storage
            .list(&build_prefix("endpointslices", None))
            .await
            .unwrap_or_default();

        for slice in all_slices {
            let ns = slice.metadata.namespace.as_deref().unwrap_or("default");
            let svc_name = slice
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .cloned();

            let Some(svc_name) = svc_name else { continue };

            let managed_by = slice
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                .cloned();

            let should_delete = match managed_by.as_deref() {
                Some("endpointslice-controller.k8s.io") => {
                    // Selector-based: delete if owning Service no longer exists
                    self.storage
                        .get::<Service>(&build_key("services", Some(ns), &svc_name))
                        .await
                        .is_err()
                }
                Some("endpointslice-mirroring-controller.k8s.io") => {
                    // Mirrored: delete if source Endpoints no longer exists
                    self.storage
                        .get::<serde_json::Value>(&build_key("endpoints", Some(ns), &svc_name))
                        .await
                        .is_err()
                }
                _ => false,
            };

            if should_delete {
                let key = build_key("endpointslices", Some(ns), &slice.metadata.name);
                let _ = self.storage.delete(&key).await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{ContainerPort, ServicePort, ServiceSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;

    #[tokio::test]
    async fn test_endpointslice_controller_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let _controller = EndpointSliceController::new(storage);
    }

    #[tokio::test]
    async fn test_pod_port_filtering_named_ports() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // Create a service with two named target ports
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![
                    ServicePort {
                        name: Some("portname1".to_string()),
                        port: 80,
                        target_port: Some(rusternetes_common::resources::IntOrString::String(
                            "svc1".to_string(),
                        )),
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    },
                    ServicePort {
                        name: Some("portname2".to_string()),
                        port: 81,
                        target_port: Some(rusternetes_common::resources::IntOrString::String(
                            "svc2".to_string(),
                        )),
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    },
                ],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };

        // Pod1 only has containerPort named "svc1" (serves portname1 only)
        let pod1 = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("pod1")
                .with_namespace("default")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c1".to_string(),
                    ports: Some(vec![ContainerPort {
                        container_port: 100,
                        name: Some("svc1".to_string()),
                        protocol: "TCP".to_string(),
                        host_port: None,
                        host_ip: None,
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };

        // Pod1 should only get portname1 (svc1→100), NOT portname2 (svc2 not found)
        let ports = controller.get_endpoint_ports(&service, &pod1);
        assert_eq!(ports.len(), 1, "Pod1 should only match portname1");
        assert_eq!(ports[0].name, Some("portname1".to_string()));
        assert_eq!(ports[0].port, Some(100));
    }

    #[tokio::test]
    async fn test_orphan_detection_skips_externally_managed_slices() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // Create an EndpointSlice NOT managed by the controller
        let mut external_slice = EndpointSlice::new("external-slice", "IPv4");
        external_slice.metadata.namespace = Some("default".to_string());
        let key = build_key("endpointslices", Some("default"), "external-slice");
        storage.create(&key, &external_slice).await.unwrap();

        // Create an EndpointSlice managed by the controller
        let mut managed_slice = EndpointSlice::new("managed-slice", "IPv4");
        managed_slice.metadata.namespace = Some("default".to_string());
        let mut labels = HashMap::new();
        labels.insert(
            "endpointslice.kubernetes.io/managed-by".to_string(),
            "endpointslice-controller.k8s.io".to_string(),
        );
        labels.insert(
            "kubernetes.io/service-name".to_string(),
            "nonexistent-service".to_string(),
        );
        managed_slice.metadata.labels = Some(labels);
        let key2 = build_key("endpointslices", Some("default"), "managed-slice");
        storage.create(&key2, &managed_slice).await.unwrap();

        controller.reconcile_all().await.unwrap();

        // External slice should survive
        assert!(
            storage.get::<EndpointSlice>(&key).await.is_ok(),
            "externally-managed EndpointSlice should NOT be deleted"
        );

        // Managed slice should be deleted (orphaned)
        assert!(
            storage.get::<EndpointSlice>(&key2).await.is_err(),
            "controller-managed orphan EndpointSlice should be deleted"
        );
    }

    /// Regression test: when a service is created before its pods, the
    /// EndpointSlice is initially empty. When pods become ready, the
    /// EndpointSlice must be UPDATED with the pod's IP and ready=true.
    /// Bug: the controller created empty slices but never updated them,
    /// causing kube-proxy to never generate DNAT rules for the service.
    #[tokio::test]
    async fn test_endpointslice_updates_when_pod_becomes_ready() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // 1. Create a service with one port (port 80, no targetPort)
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("endpoint-test2").with_namespace("test-ns"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        let svc_key = build_key("services", Some("test-ns"), "endpoint-test2");
        storage.create(&svc_key, &service).await.unwrap();

        // 2. Reconcile with no pods — should create empty EndpointSlice
        controller.reconcile_service(&service).await.unwrap();

        let es_key = build_key("endpointslices", Some("test-ns"), "endpoint-test2");
        let es = storage.get::<EndpointSlice>(&es_key).await.unwrap();
        assert!(
            es.endpoints.is_empty(),
            "EndpointSlice should be empty when no pods exist"
        );
        assert!(
            !es.ports.is_empty(),
            "EndpointSlice should have ports from the service"
        );

        // 3. Create a matching pod with IP and Ready condition
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("pod1")
                .with_namespace("test-ns")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "agnhost".to_string(),
                    ports: Some(vec![ContainerPort {
                        container_port: 80,
                        name: None,
                        protocol: "TCP".to_string(),
                        host_port: None,
                        host_ip: None,
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                pod_ip: Some("10.89.0.22".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        let pod_key = build_key("pods", Some("test-ns"), "pod1");
        storage.create(&pod_key, &pod).await.unwrap();

        // 4. Reconcile again — EndpointSlice MUST be updated with the pod
        controller.reconcile_service(&service).await.unwrap();

        let es_updated = storage.get::<EndpointSlice>(&es_key).await.unwrap();
        assert_eq!(
            es_updated.endpoints.len(),
            1,
            "EndpointSlice must have 1 endpoint after pod becomes ready"
        );
        assert_eq!(es_updated.endpoints[0].addresses, vec!["10.89.0.22"]);
        assert_eq!(
            es_updated.endpoints[0]
                .conditions
                .as_ref()
                .and_then(|c| c.ready),
            Some(true),
            "Endpoint must be marked ready"
        );
    }

    /// Terminating pods (with deletionTimestamp) must remain in EndpointSlices
    /// with terminating=true, ready=false. K8s kube-proxy uses these to drain
    /// connections during rolling updates. Dropping them entirely causes
    /// in-flight requests to fail mid-flight.
    #[tokio::test]
    async fn test_endpointslice_keeps_terminating_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(&build_key("services", Some("default"), "svc"), &service)
            .await
            .unwrap();

        // Pod with deletionTimestamp set (terminating) but still Ready
        let mut pod_meta = ObjectMeta::new("p1")
            .with_namespace("default")
            .with_labels(HashMap::from([("app".to_string(), "test".to_string())]));
        pod_meta.deletion_timestamp = Some(chrono::Utc::now());
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.1".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "p1"), &pod)
            .await
            .unwrap();

        controller.reconcile_service(&service).await.unwrap();

        let es: EndpointSlice = storage
            .get(&build_key("endpointslices", Some("default"), "svc"))
            .await
            .unwrap();

        assert_eq!(
            es.endpoints.len(),
            1,
            "terminating pod must still appear in the EndpointSlice"
        );
        let conds = es.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(conds.terminating, Some(true), "terminating must be true");
        assert_eq!(
            conds.ready,
            Some(false),
            "ready must be false for terminating pods (unless publishNotReadyAddresses)"
        );
        assert_eq!(
            conds.serving,
            Some(true),
            "serving must mirror Ready condition independent of terminating state"
        );
    }

    /// publishNotReadyAddresses=true forces ready=true and serving=true on every
    /// endpoint, regardless of pod readiness. This is required for headless
    /// services that fronts gossip-style protocols (e.g. peer-discovery) where
    /// clients need to see all pods, including those still starting up.
    #[tokio::test]
    async fn test_endpointslice_publish_not_ready_addresses() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                publish_not_ready_addresses: Some(true),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(&build_key("services", Some("default"), "svc"), &service)
            .await
            .unwrap();

        // A pod that is Running but NOT Ready
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("p1")
                .with_namespace("default")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.2".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "False".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "p1"), &pod)
            .await
            .unwrap();

        controller.reconcile_service(&service).await.unwrap();

        let es: EndpointSlice = storage
            .get(&build_key("endpointslices", Some("default"), "svc"))
            .await
            .unwrap();
        assert_eq!(es.endpoints.len(), 1);
        let conds = es.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(
            conds.ready,
            Some(true),
            "publishNotReadyAddresses=true must force ready=true even for unready pods"
        );
        assert_eq!(
            conds.serving,
            Some(true),
            "publishNotReadyAddresses=true must force serving=true"
        );
    }
}
