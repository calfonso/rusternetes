use anyhow::{Context, Result};
use rusternetes_common::resources::{EndpointSlice, Endpoints, Pod, Service, ServiceType};
use rusternetes_storage::Storage;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::iptables::IptablesManager;

/// KubeProxy manages service networking through iptables rules.
///
/// Generic over the storage seam so the SAME proxy runs against either a real
/// `StorageBackend` (all-in-one) or `ApiStorage` (in-cluster compose service,
/// reading Services/Endpoints/EndpointSlices from the api-server).
pub struct KubeProxy<S: Storage> {
    storage: Arc<S>,
    iptables: IptablesManager,
    /// Name of the node we're running on. Used to filter pods so we only
    /// install KUBE-HOSTPORTS DNAT rules for locally-scheduled pods.
    node_name: String,
    /// Hash of last synced state to avoid unnecessary flush+rebuild cycles.
    /// K8s uses iptables-restore for atomic updates; we approximate by skipping
    /// sync when state hasn't changed, eliminating the flush gap.
    last_sync_hash: u64,
}

impl<S: Storage> KubeProxy<S> {
    pub fn new(
        storage: Arc<S>,
        cluster_cidr: String,
        nodeport_range: String,
        node_name: String,
    ) -> Result<Self> {
        let iptables = IptablesManager::new(cluster_cidr, nodeport_range);
        iptables.initialize()?;

        Ok(Self {
            storage,
            iptables,
            node_name,
            last_sync_hash: 0,
        })
    }

    /// Sync all services and their endpoints
    pub async fn sync(&mut self) -> Result<()> {
        info!("Starting kube-proxy sync");

        // Get all services
        let services: Vec<Service> = self
            .storage
            .list("/registry/services/")
            .await
            .context("Failed to list services")?;

        // Get all endpoints (old-style)
        let all_endpoints: Vec<Endpoints> = self
            .storage
            .list("/registry/endpoints/")
            .await
            .context("Failed to list endpoints")?;

        // Build endpoints map for quick lookup
        let endpoints_map: HashMap<String, Endpoints> = all_endpoints
            .into_iter()
            .filter_map(|ep| {
                let namespace = ep.metadata.namespace.as_ref()?;
                let key = format!("{}/{}", namespace, &ep.metadata.name);
                Some((key, ep))
            })
            .collect();

        // Get all EndpointSlices (new API) as fallback
        let all_endpointslices: Vec<EndpointSlice> = self
            .storage
            .list("/registry/endpointslices/")
            .await
            .unwrap_or_default();

        // Get all pods so we can install KUBE-HOSTPORTS DNAT rules for pods
        // scheduled on this node. Filtering is done inside
        // `build_hostport_rules` based on `pod.spec.node_name`.
        let all_pods: Vec<Pod> = self
            .storage
            .list("/registry/pods/")
            .await
            .unwrap_or_default();

        // Build EndpointSlice map: key = "namespace/service-name" -> list of (ip, port_name, port_number)
        // This preserves port information from EndpointSlices so kube-proxy can route multi-port services
        let mut endpointslice_map: HashMap<String, Vec<(String, Option<String>, u16)>> =
            HashMap::new();
        for es in &all_endpointslices {
            let namespace = es.metadata.namespace.as_deref().unwrap_or("default");
            // EndpointSlice's label kubernetes.io/service-name tells us which service it belongs to
            let service_name = es
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .cloned()
                .unwrap_or_else(|| {
                    // Fall back: strip the random suffix from the EndpointSlice name
                    es.metadata
                        .name
                        .rsplit_once('-')
                        .map(|(prefix, _)| prefix.to_string())
                        .unwrap_or_else(|| es.metadata.name.clone())
                });
            let key = format!("{}/{}", namespace, service_name);

            // Collect ready endpoint addresses
            let mut ready_addrs: Vec<String> = Vec::new();
            for endpoint in &es.endpoints {
                if let Some(conditions) = &endpoint.conditions {
                    if conditions.ready == Some(false) {
                        continue; // Skip not-ready endpoints
                    }
                }
                for addr in &endpoint.addresses {
                    ready_addrs.push(addr.clone());
                }
            }

            // For each port in the EndpointSlice, record (ip, port_name, port_number) tuples
            if es.ports.is_empty() {
                // No ports defined - just record IPs with port 0 (will use service target_port)
                for addr in &ready_addrs {
                    endpointslice_map
                        .entry(key.clone())
                        .or_default()
                        .push((addr.clone(), None, 0));
                }
            } else {
                for es_port in &es.ports {
                    let port_num = es_port.port.unwrap_or(0) as u16;
                    let port_name = es_port.name.clone();
                    for addr in &ready_addrs {
                        endpointslice_map.entry(key.clone()).or_default().push((
                            addr.clone(),
                            port_name.clone(),
                            port_num,
                        ));
                    }
                }
            }
        }

        debug!(
            "Kube-proxy sync: {} services, {} endpoints, {} endpointslices",
            services.len(),
            endpoints_map.len(),
            all_endpointslices.len()
        );

        // Compute an ORDER-INDEPENDENT hash of the current state.
        // Services and endpoints come back from etcd in arbitrary order,
        // so we must use a commutative hash (XOR of per-item hashes) to
        // avoid false "changed" detections that cause unnecessary flush+rebuild.
        // K8s uses atomic iptables-restore; we skip no-op syncs instead.
        use std::hash::{Hash, Hasher};

        let mut current_hash: u64 = 0;
        current_hash ^= services.len() as u64;
        current_hash ^= (endpoints_map.len() as u64).wrapping_mul(31);
        current_hash ^= (all_endpointslices.len() as u64).wrapping_mul(37);
        // Hash hostPort-relevant pod state so KUBE-HOSTPORTS resyncs
        // when a local pod is added, gets an IP, or has its ports changed.
        for pod in &all_pods {
            let Some(spec) = pod.spec.as_ref() else {
                continue;
            };
            if spec.node_name.as_deref() != Some(&self.node_name) {
                continue;
            }
            let mut h = std::collections::hash_map::DefaultHasher::new();
            pod.metadata.uid.hash(&mut h);
            pod.status
                .as_ref()
                .and_then(|s| s.pod_ip.as_deref())
                .hash(&mut h);
            for c in &spec.containers {
                for p in c.ports.iter().flatten() {
                    p.host_port.hash(&mut h);
                    p.container_port.hash(&mut h);
                    p.protocol.hash(&mut h);
                    p.host_ip.hash(&mut h);
                }
            }
            current_hash ^= h.finish();
        }

        for svc in &services {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            svc.spec.cluster_ip.hash(&mut h);
            svc.spec.session_affinity.hash(&mut h);
            // Include session affinity timeout in hash so config changes trigger resync
            svc.spec
                .session_affinity_config
                .as_ref()
                .and_then(|c| c.client_ip.as_ref())
                .and_then(|c| c.timeout_seconds)
                .hash(&mut h);
            for port in &svc.spec.ports {
                port.port.hash(&mut h);
                port.name.hash(&mut h);
                port.protocol.hash(&mut h);
                port.node_port.hash(&mut h);
            }
            // XOR makes it order-independent
            current_hash ^= h.finish();
        }
        for (key, entries) in &endpointslice_map {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut h);
            entries.len().hash(&mut h);
            // XOR individual entry hashes for order-independence within entries
            let mut entries_hash: u64 = 0;
            for (ip, name, port) in entries {
                let mut eh = std::collections::hash_map::DefaultHasher::new();
                ip.hash(&mut eh);
                name.hash(&mut eh);
                port.hash(&mut eh);
                entries_hash ^= eh.finish();
            }
            entries_hash.hash(&mut h);
            current_hash ^= h.finish();
        }

        if current_hash == self.last_sync_hash {
            debug!("Kube-proxy: state unchanged, skipping sync");
            return Ok(());
        }
        self.last_sync_hash = current_hash;

        // K8s uses iptables-restore to atomically replace all NAT rules.
        // Individual iptables -F + -A creates a gap where no rules exist,
        // causing "connection refused" for any ClusterIP traffic during rebuild.
        // See: pkg/proxy/iptables/proxier.go:1495 — RestoreAll with NoFlushTables
        //
        // Build all rules in memory, then apply atomically.
        info!(
            "Kube-proxy sync: {} services, {} endpoint entries",
            services.len(),
            endpointslice_map.len()
        );
        let nat_rules = self
            .iptables
            .build_nat_rules(&services, &endpointslice_map, &all_pods, &self.node_name)
            .await;
        info!("Kube-proxy built {} bytes of NAT rules", nat_rules.len());

        // Apply atomically via iptables-restore --noflush
        if let Err(e) = self.iptables.apply_nat_rules_atomic(&nat_rules) {
            error!(
                "Failed atomic iptables-restore, falling back to flush+rebuild: {}",
                e
            );
            // Fallback: flush and rebuild (has gap but better than nothing)
            self.iptables.flush_rules()?;
            for service in &services {
                if let Err(e) = self
                    .sync_service(service, &endpoints_map, &endpointslice_map)
                    .await
                {
                    let namespace = service.metadata.namespace.as_deref().unwrap_or("unknown");
                    let name = &service.metadata.name;
                    error!("Failed to sync service {}/{}: {}", namespace, name, e);
                }
            }
        }

        debug!("Kube-proxy sync completed");
        Ok(())
    }

    /// Sync a single service
    #[allow(clippy::type_complexity)]
    async fn sync_service(
        &mut self,
        service: &Service,
        endpoints_map: &HashMap<String, Endpoints>,
        endpointslice_map: &HashMap<String, Vec<(String, Option<String>, u16)>>,
    ) -> Result<()> {
        let namespace = service
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Service has no namespace"))?;
        let name = &service.metadata.name;

        debug!("Syncing service {}/{}", namespace, name);

        // Get service type
        let service_type = service
            .spec
            .service_type
            .as_ref()
            .cloned()
            .unwrap_or(ServiceType::ClusterIP);

        // Skip ExternalName services (they don't need iptables rules)
        if service_type == ServiceType::ExternalName {
            debug!("Skipping ExternalName service {}/{}", namespace, name);
            return Ok(());
        }

        // Get ClusterIP (required for ClusterIP/NodePort/LoadBalancer services)
        let cluster_ip = service.spec.cluster_ip.as_ref();
        debug!(
            "Service {}/{} - clusterIP field value: {:?}",
            namespace, name, cluster_ip
        );
        debug!(
            "Service {}/{} - full spec: {:?}",
            namespace, name, service.spec
        );
        let has_valid_cluster_ip = cluster_ip
            .map(|ip| {
                !ip.is_empty() && ip.as_str() != "None" && ip.as_str() != "null" && ip.contains('.')
            })
            .unwrap_or(false);
        if !has_valid_cluster_ip {
            debug!(
                "Service {}/{} has no valid ClusterIP ({:?}), skipping iptables rules",
                namespace, name, cluster_ip
            );
            return Ok(());
        }

        // Get endpoints for this service (try old-style Endpoints first, then EndpointSlices)
        let endpoint_key = format!("{}/{}", namespace, name);
        let endpoints = endpoints_map.get(&endpoint_key);

        // Extract endpoint addresses from old-style Endpoints
        let endpoint_addresses = self.extract_endpoint_addresses(endpoints);

        // Get EndpointSlice data (with port info)
        let endpointslice_entries = endpointslice_map.get(&endpoint_key);

        let use_endpointslices = endpoint_addresses.is_empty() && endpointslice_entries.is_some();

        if endpoint_addresses.is_empty()
            && endpointslice_entries.map(|e| e.is_empty()).unwrap_or(true)
        {
            debug!(
                "Service {}/{} (ClusterIP={}) has no ready endpoints, rules will have 0 backends",
                namespace,
                name,
                cluster_ip.unwrap_or(&String::new())
            );
        }

        // Check session affinity
        let use_session_affinity = service.spec.session_affinity.as_deref() == Some("ClientIP");

        // Get session affinity timeout (default 10800 seconds = 3 hours, per K8s spec)
        let affinity_timeout = service
            .spec
            .session_affinity_config
            .as_ref()
            .and_then(|c| c.client_ip.as_ref())
            .and_then(|c| c.timeout_seconds)
            .unwrap_or(10800);

        // Process each service port
        for service_port in &service.spec.ports {
            let protocol = service_port.protocol.as_str();
            let target_port = match &service_port.target_port {
                Some(rusternetes_common::resources::IntOrString::Int(p)) => *p as u16,
                Some(rusternetes_common::resources::IntOrString::String(s)) => {
                    s.parse::<u16>().unwrap_or(service_port.port)
                }
                None => service_port.port,
            };

            // Build list of endpoints with the correct port
            let endpoints_with_port: Vec<(String, u16)> = if use_endpointslices {
                // Use EndpointSlice port information: match by port name or port number
                let entries = endpointslice_entries.unwrap();
                let svc_port_name = service_port.name.as_deref().unwrap_or("");
                entries
                    .iter()
                    .filter(|(_, es_port_name, es_port_num)| {
                        // Match EndpointSlice port to service port:
                        // 1. By port name if both have names
                        // 2. By port number if no name match
                        // 3. If EndpointSlice port is 0, it means no port info - use target_port
                        if *es_port_num == 0 {
                            return true; // No port info, include all
                        }
                        if let Some(ref es_name) = es_port_name {
                            if !es_name.is_empty() && !svc_port_name.is_empty() {
                                return es_name == svc_port_name;
                            }
                        }
                        // If no name matching possible, match by port number against target port
                        *es_port_num == target_port
                    })
                    .map(|(ip, _, es_port_num)| {
                        let port = if *es_port_num == 0 {
                            target_port
                        } else {
                            *es_port_num
                        };
                        (ip.clone(), port)
                    })
                    .collect()
            } else {
                // Use old-style Endpoints - just pair IPs with target_port
                endpoint_addresses
                    .iter()
                    .map(|ip| (ip.clone(), target_port))
                    .collect()
            };

            // Add ClusterIP rules (for ClusterIP, NodePort, and LoadBalancer)
            if let Some(cluster_ip_str) = cluster_ip {
                self.iptables.add_clusterip_rules(
                    cluster_ip_str,
                    service_port.port,
                    &endpoints_with_port,
                    protocol,
                    use_session_affinity,
                    affinity_timeout,
                )?;
            }

            // Add NodePort rules (for NodePort and LoadBalancer)
            if matches!(
                service_type,
                ServiceType::NodePort | ServiceType::LoadBalancer
            ) {
                if let Some(node_port) = service_port.node_port {
                    self.iptables.add_nodeport_rules(
                        node_port,
                        &endpoints_with_port,
                        protocol,
                        use_session_affinity,
                        affinity_timeout,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Extract ready endpoint IP addresses from Endpoints resource
    fn extract_endpoint_addresses(&self, endpoints: Option<&Endpoints>) -> Vec<String> {
        let endpoints = match endpoints {
            Some(ep) => ep,
            None => return vec![],
        };

        let mut addresses = Vec::new();

        for subset in &endpoints.subsets {
            if let Some(ready_addresses) = &subset.addresses {
                for addr in ready_addresses {
                    addresses.push(addr.ip.clone());
                }
            }
        }

        addresses
    }
}

impl<S: Storage> Drop for KubeProxy<S> {
    fn drop(&mut self) {
        info!("Shutting down kube-proxy");
    }
}

#[cfg(test)]
mod tests {
    use rusternetes_common::resources::endpointslice::EndpointPort as ESEndpointPort;
    use rusternetes_common::resources::{Endpoint, EndpointConditions, EndpointSlice};
    use rusternetes_common::types::ObjectMeta;
    use std::collections::HashMap;

    #[test]
    fn test_endpointslice_port_aware_map_building() {
        // Build an EndpointSlice with two ports and two ready endpoints.
        // Verify the map correctly produces (IP, port_name, port_number) tuples.
        let mut labels = HashMap::new();
        labels.insert(
            "kubernetes.io/service-name".to_string(),
            "my-svc".to_string(),
        );
        let es = EndpointSlice {
            metadata: ObjectMeta {
                name: "my-svc-abc12".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(labels),
                ..ObjectMeta::default()
            },
            address_type: "IPv4".to_string(),
            endpoints: vec![
                Endpoint {
                    addresses: vec!["10.0.0.1".to_string()],
                    conditions: Some(EndpointConditions {
                        ready: Some(true),
                        serving: None,
                        terminating: None,
                    }),
                    hostname: None,
                    target_ref: None,
                    node_name: None,
                    zone: None,
                    hints: None,
                    deprecated_topology: None,
                },
                Endpoint {
                    addresses: vec!["10.0.0.2".to_string()],
                    conditions: None, // no conditions = assumed ready
                    hostname: None,
                    target_ref: None,
                    node_name: None,
                    zone: None,
                    hints: None,
                    deprecated_topology: None,
                },
            ],
            ports: vec![
                ESEndpointPort {
                    name: Some("http".to_string()),
                    port: Some(8080),
                    protocol: "TCP".to_string(),
                    app_protocol: None,
                },
                ESEndpointPort {
                    name: Some("https".to_string()),
                    port: Some(8443),
                    protocol: "TCP".to_string(),
                    app_protocol: None,
                },
            ],
            ..EndpointSlice::new("my-svc-abc12", "IPv4")
        };

        // Replicate the map-building logic from sync()
        let all_endpointslices = vec![es];
        let mut endpointslice_map: HashMap<String, Vec<(String, Option<String>, u16)>> =
            HashMap::new();
        for es in &all_endpointslices {
            let namespace = es.metadata.namespace.as_deref().unwrap_or("default");
            let service_name = es
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .cloned()
                .unwrap_or_else(|| es.metadata.name.clone());
            let key = format!("{}/{}", namespace, service_name);

            let mut ready_addrs: Vec<String> = Vec::new();
            for endpoint in &es.endpoints {
                if let Some(conditions) = &endpoint.conditions {
                    if conditions.ready == Some(false) {
                        continue;
                    }
                }
                for addr in &endpoint.addresses {
                    ready_addrs.push(addr.clone());
                }
            }

            if es.ports.is_empty() {
                for addr in &ready_addrs {
                    endpointslice_map
                        .entry(key.clone())
                        .or_default()
                        .push((addr.clone(), None, 0));
                }
            } else {
                for es_port in &es.ports {
                    let port_num = es_port.port.unwrap_or(0) as u16;
                    let port_name = es_port.name.clone();
                    for addr in &ready_addrs {
                        endpointslice_map.entry(key.clone()).or_default().push((
                            addr.clone(),
                            port_name.clone(),
                            port_num,
                        ));
                    }
                }
            }
        }

        let entries = endpointslice_map.get("default/my-svc").unwrap();
        // 2 ports * 2 endpoints = 4 tuples
        assert_eq!(entries.len(), 4, "expected 4 (ip, port_name, port) tuples");

        // Verify http port entries
        let http_entries: Vec<_> = entries
            .iter()
            .filter(|(_, name, _)| name.as_deref() == Some("http"))
            .collect();
        assert_eq!(http_entries.len(), 2);
        assert!(http_entries.iter().all(|(_, _, port)| *port == 8080));

        // Verify https port entries
        let https_entries: Vec<_> = entries
            .iter()
            .filter(|(_, name, _)| name.as_deref() == Some("https"))
            .collect();
        assert_eq!(https_entries.len(), 2);
        assert!(https_entries.iter().all(|(_, _, port)| *port == 8443));

        // Verify both IPs are present
        let ips: Vec<&str> = entries.iter().map(|(ip, _, _)| ip.as_str()).collect();
        assert!(ips.contains(&"10.0.0.1"));
        assert!(ips.contains(&"10.0.0.2"));
    }
}
