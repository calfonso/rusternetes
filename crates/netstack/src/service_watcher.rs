//! Service-VIP watcher — keeps the netstack's TCP listener pools in
//! sync with the cluster's `Service` + `EndpointSlice` state.
//!
//! ### What it does
//!
//! Periodically (`sync_interval`):
//!
//! 1. List every `Service` from storage; filter to type=ClusterIP
//!    with a real `clusterIP` set (skip `None` / "None" headless,
//!    skip ExternalName).
//! 2. List every `EndpointSlice`; group by `kubernetes.io/service-name`
//!    label into a `(namespace, name) → Vec<EndpointSlice>` map.
//! 3. For each Service + TCP port, compute the (vip, backends) pair:
//!    - `vip = clusterIP:port`
//!    - `backends` = each ready endpoint address × the slice's
//!      port matching this Service's port name/number.
//! 4. Diff against the previous reconcile's snapshot:
//!    - **New VIP** → `Netstack::bind_tcp_service`
//!    - **Gone VIP** → `Netstack::unbind_tcp_service`
//!    - **Backends changed** → unbind then bind with the new list
//!      (`RoundRobinPicker` is immutable, so re-install)
//!
//! ### Scope
//!
//! - TCP only. UDP Services aren't handled — kube-dns is the
//!   exception, served by the standalone `rusternetes-dns` task
//!   not via this watcher.
//! - IPv4 only. The netstack itself is IPv4-only today; v6 Services
//!   are skipped silently.
//! - Periodic poll; no watch streams yet. Pattern matches the
//!   existing kube-proxy reconcile loop.
//!
//! ### What's deliberately NOT in this commit
//!
//! - Reading the `Service.spec.ports.targetPort` (numeric vs named).
//!   We use `port.port` everywhere; named-port resolution against
//!   the pod's containers is a follow-up.
//! - SessionAffinity. RoundRobin only; sticky-IP picker is a
//!   future variant.

use crate::manager::NetstackHandle;
use rusternetes_common::resources::{EndpointSlice, Service, ServiceType};
use rusternetes_storage::{Storage, StorageBackend};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

/// Default pool size per Service VIP. 16 lets us absorb a small
/// burst of concurrent connections (kubectl exec, port-forward,
/// health-check probes) without dropping SYNs. Bigger Services that
/// host many parallel clients should override via the config struct.
pub const DEFAULT_POOL_SIZE: usize = 16;

/// Internal snapshot of what we last installed in the netstack —
/// used to compute the per-cycle diff.
#[derive(Default, Debug, Clone)]
struct InstalledState {
    /// vip → backends, with backends sorted so set-equality is a
    /// straight Vec comparison.
    vips: HashMap<SocketAddr, Vec<SocketAddr>>,
}

/// Run the Service-VIP watcher loop. Exits cleanly when `cancel`
/// fires (typically tied to the netstack's shutdown).
///
/// Errors during a single reconcile are logged and swallowed —
/// transient storage failures must not crash the watcher. A
/// catastrophic storage failure that persists across cycles will
/// surface as "no Services bound" rather than an exit, which is
/// the right behavior for a sync-loop.
pub async fn run(
    storage: Arc<StorageBackend>,
    netstack: Arc<dyn NetstackHandle>,
    sync_interval: Duration,
    cancel: Arc<Notify>,
) {
    info!(?sync_interval, "service_watcher: starting reconcile loop");
    let mut state = InstalledState::default();
    let mut ticker = tokio::time::interval(sync_interval);
    // Skip the immediate first tick — `interval` fires right away
    // and we want the first reconcile after a small delay so the
    // storage backend has time to settle.
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = cancel.notified() => {
                info!("service_watcher: cancel notified, exiting");
                return;
            }
            _ = ticker.tick() => {}
        }

        if let Err(e) = reconcile_once(&storage, &*netstack, &mut state).await {
            warn!("service_watcher: reconcile failed (will retry): {e:#}");
        }
    }
}

/// One pass: list cluster state, compute desired VIPs, apply the
/// diff. Pure function over `(storage, netstack, prev_state)` —
/// `state` is mutated to the new snapshot on success.
async fn reconcile_once(
    storage: &StorageBackend,
    netstack: &dyn NetstackHandle,
    state: &mut InstalledState,
) -> anyhow::Result<()> {
    let services: Vec<Service> = storage.list("/registry/services/").await?;
    let slices: Vec<EndpointSlice> = storage
        .list("/registry/endpointslices/")
        .await
        .unwrap_or_default();

    let desired = compute_desired_vips(&services, &slices);

    // Diff:
    //   - prev ∩ desired with same backends → no-op
    //   - prev ∩ desired with different backends → unbind + bind
    //   - prev - desired → unbind
    //   - desired - prev → bind
    let prev_keys: HashSet<SocketAddr> = state.vips.keys().copied().collect();
    let new_keys: HashSet<SocketAddr> = desired.keys().copied().collect();

    // Gone VIPs.
    for vip in prev_keys.difference(&new_keys) {
        if netstack.unbind_tcp_service(*vip).await {
            info!(?vip, "service_watcher: unbound (Service deleted)");
        }
        state.vips.remove(vip);
    }

    // Changed-backends VIPs (unbind first so the rebind is clean).
    for vip in prev_keys.intersection(&new_keys) {
        let prev = state.vips.get(vip).cloned().unwrap_or_default();
        let new = desired.get(vip).cloned().unwrap_or_default();
        if prev != new {
            netstack.unbind_tcp_service(*vip).await;
            if let Err(e) = netstack
                .bind_tcp_service(*vip, new.clone(), DEFAULT_POOL_SIZE)
                .await
            {
                warn!(?vip, error = %e, "service_watcher: rebind failed");
                state.vips.remove(vip);
                continue;
            }
            info!(
                ?vip,
                backend_count = new.len(),
                "service_watcher: rebound (EndpointSlice change)"
            );
            state.vips.insert(*vip, new);
        }
    }

    // New VIPs.
    for vip in new_keys.difference(&prev_keys) {
        let backends = desired.get(vip).cloned().unwrap_or_default();
        if let Err(e) = netstack
            .bind_tcp_service(*vip, backends.clone(), DEFAULT_POOL_SIZE)
            .await
        {
            warn!(?vip, error = %e, "service_watcher: initial bind failed");
            continue;
        }
        info!(
            ?vip,
            backend_count = backends.len(),
            "service_watcher: bound (new Service)"
        );
        state.vips.insert(*vip, backends);
    }

    debug!(
        bound = state.vips.len(),
        services = services.len(),
        slices = slices.len(),
        "service_watcher: reconcile complete"
    );
    Ok(())
}

/// Pure computation: given current Services + EndpointSlices,
/// return the desired `vip → backends` map.
///
/// Backends are sorted so the watcher's diff against the previous
/// snapshot is comparing canonical forms (no spurious "changed"
/// signals from ordering differences in the watcher's input).
fn compute_desired_vips(
    services: &[Service],
    slices: &[EndpointSlice],
) -> HashMap<SocketAddr, Vec<SocketAddr>> {
    // Group EndpointSlices by (namespace, service_name) via the
    // standard `kubernetes.io/service-name` label.
    let mut slices_by_service: HashMap<(String, String), Vec<&EndpointSlice>> = HashMap::new();
    for slice in slices {
        let Some(ns) = slice.metadata.namespace.as_deref() else {
            continue;
        };
        let Some(svc_name) = slice
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
        else {
            continue;
        };
        slices_by_service
            .entry((ns.to_string(), svc_name.clone()))
            .or_default()
            .push(slice);
    }

    let mut desired: HashMap<SocketAddr, Vec<SocketAddr>> = HashMap::new();
    for svc in services {
        let Some(ns) = svc.metadata.namespace.as_deref() else {
            continue;
        };
        // Skip non-ClusterIP, headless (cluster_ip = "None"), or
        // ExternalName.
        let is_cluster_ip = matches!(svc.spec.service_type, Some(ServiceType::ClusterIP) | None);
        if !is_cluster_ip {
            continue;
        }
        let Some(cluster_ip) = &svc.spec.cluster_ip else {
            continue;
        };
        if cluster_ip == "None" || cluster_ip.is_empty() {
            continue;
        }
        let Ok(ip) = cluster_ip.parse::<IpAddr>() else {
            continue;
        };
        let IpAddr::V4(vip_ip) = ip else {
            continue; // IPv6 Services skipped (netstack is v4-only today)
        };

        let svc_slices = slices_by_service
            .get(&(ns.to_string(), svc.metadata.name.clone()))
            .cloned()
            .unwrap_or_default();

        for port in &svc.spec.ports {
            // TCP only (default to TCP when protocol unset, per K8s spec).
            let protocol = port.protocol.as_str();
            if !protocol.eq_ignore_ascii_case("TCP") {
                continue;
            }
            let vip = SocketAddr::new(IpAddr::V4(vip_ip), port.port);
            let backends = backends_for_port(&svc_slices, port.port, port.name.as_deref());
            desired.insert(vip, backends);
        }
    }
    desired
}

/// Collect the ready backend `SocketAddr`s for one (Service, port)
/// pair across all matching EndpointSlices.
fn backends_for_port(
    slices: &[&EndpointSlice],
    svc_port: u16,
    svc_port_name: Option<&str>,
) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    for slice in slices {
        // Pair the Service port with an EndpointPort. K8s matches
        // by *name* when the Service port has a name (the most
        // common case for multi-port Services). When no name is set
        // — single-port Services, or anonymous ports — we take the
        // sole EndpointPort: the endpoints-controller generates one
        // EndpointPort per Service.spec.ports entry, so a
        // single-port Service maps unambiguously to a single
        // EndpointPort.
        let target_port: Option<u16> = match svc_port_name {
            Some(name) => slice
                .ports
                .iter()
                .find(|p| p.name.as_deref() == Some(name))
                .and_then(|p| p.port)
                .and_then(|n| u16::try_from(n).ok()),
            None if slice.ports.len() == 1 => slice
                .ports
                .first()
                .and_then(|p| p.port)
                .and_then(|n| u16::try_from(n).ok()),
            None => {
                // Multi-port slice with no name to disambiguate.
                // We could fall back to matching Service port → its
                // EndpointPort by index, but that requires the slice
                // to be built in the same order. Skip rather than
                // mis-route; flag as a follow-up if it bites in
                // production.
                let _ = svc_port; // suppress unused warning
                continue;
            }
        };
        let Some(tp) = target_port else {
            continue;
        };
        for ep in &slice.endpoints {
            let ready = ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true);
            if !ready {
                continue;
            }
            for addr in &ep.addresses {
                if let Ok(IpAddr::V4(v4)) = addr.parse::<IpAddr>() {
                    out.push(SocketAddr::new(IpAddr::V4(v4), tp));
                }
            }
        }
    }
    // Canonical sort so the diff against the previous snapshot is
    // a straight Vec comparison.
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::endpointslice::EndpointPort as SliceEndpointPort;
    use rusternetes_common::resources::{Endpoint, EndpointConditions, ServiceSpec, ServiceType};
    use std::collections::HashMap as Map;

    fn svc(name: &str, ns: &str, cluster_ip: &str, port: u16, port_name: Option<&str>) -> Service {
        let mut s = Service::new(name, ServiceSpec::default());
        s.metadata.namespace = Some(ns.to_string());
        s.spec.cluster_ip = Some(cluster_ip.to_string());
        s.spec.service_type = Some(ServiceType::ClusterIP);
        s.spec.ports = vec![rusternetes_common::resources::ServicePort {
            name: port_name.map(String::from),
            port,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        }];
        s
    }

    fn slice_for(svc_name: &str, ns: &str, target_port: u16, addrs: &[&str]) -> EndpointSlice {
        let mut es = EndpointSlice::new(format!("{svc_name}-abc"), "IPv4");
        es.metadata.namespace = Some(ns.to_string());
        let mut labels = Map::new();
        labels.insert(
            "kubernetes.io/service-name".to_string(),
            svc_name.to_string(),
        );
        es.metadata.labels = Some(labels);
        es.endpoints = addrs
            .iter()
            .map(|a| Endpoint {
                addresses: vec![a.to_string()],
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    serving: Some(true),
                    terminating: Some(false),
                }),
                hostname: None,
                target_ref: None,
                node_name: None,
                zone: None,
                hints: None,
                deprecated_topology: None,
            })
            .collect();
        es.ports = vec![SliceEndpointPort {
            name: None,
            port: Some(target_port as i32),
            protocol: "TCP".to_string(),
            app_protocol: None,
        }];
        es
    }

    #[test]
    fn compute_desired_vips_emits_one_vip_per_tcp_port() {
        let services = vec![svc("kubernetes", "default", "10.96.0.1", 443, None)];
        let slices = vec![slice_for(
            "kubernetes",
            "default",
            6443,
            &["10.244.0.5", "10.244.0.6"],
        )];

        let desired = compute_desired_vips(&services, &slices);
        let vip: SocketAddr = "10.96.0.1:443".parse().unwrap();
        assert_eq!(desired.len(), 1);
        let backends = &desired[&vip];
        assert_eq!(
            backends,
            &vec![
                "10.244.0.5:6443".parse().unwrap(),
                "10.244.0.6:6443".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn compute_desired_vips_skips_headless_and_external_name() {
        let mut headless = svc("headless", "default", "None", 80, None);
        headless.spec.service_type = Some(ServiceType::ClusterIP);

        let mut external = Service::new("external", ServiceSpec::default());
        external.metadata.namespace = Some("default".to_string());
        external.spec.service_type = Some(ServiceType::ExternalName);
        external.spec.external_name = Some("foo.example.com".to_string());

        let normal = svc("normal", "default", "10.96.0.2", 80, None);

        let desired = compute_desired_vips(&[headless, external, normal], &[]);
        assert_eq!(desired.len(), 1);
        assert!(desired.contains_key(&"10.96.0.2:80".parse::<SocketAddr>().unwrap()));
    }

    #[test]
    fn compute_desired_vips_skips_udp_ports() {
        let mut services = vec![svc("dns", "kube-system", "10.96.0.10", 53, None)];
        services[0].spec.ports[0].protocol = "UDP".to_string();
        let desired = compute_desired_vips(&services, &[]);
        assert!(
            desired.is_empty(),
            "UDP services aren't bound as TCP listeners"
        );
    }

    #[test]
    fn compute_desired_vips_filters_unready_endpoints() {
        let services = vec![svc("api", "default", "10.96.0.3", 8080, None)];
        let mut slices = vec![slice_for(
            "api",
            "default",
            8080,
            &["10.244.0.10", "10.244.0.11"],
        )];
        // Mark the second endpoint not-ready.
        slices[0].endpoints[1].conditions.as_mut().unwrap().ready = Some(false);

        let desired = compute_desired_vips(&services, &slices);
        let backends = desired
            .get(&"10.96.0.3:8080".parse::<SocketAddr>().unwrap())
            .unwrap();
        assert_eq!(backends, &vec!["10.244.0.10:8080".parse().unwrap()]);
    }

    #[test]
    fn compute_desired_vips_dedupes_backends_across_slices() {
        // Two EndpointSlices that share an address (legal during
        // slice splits) shouldn't be picked twice.
        let services = vec![svc("api", "default", "10.96.0.3", 80, None)];
        let slices = vec![
            slice_for("api", "default", 80, &["10.244.0.5"]),
            slice_for("api", "default", 80, &["10.244.0.5", "10.244.0.6"]),
        ];
        let desired = compute_desired_vips(&services, &slices);
        let backends = desired
            .get(&"10.96.0.3:80".parse::<SocketAddr>().unwrap())
            .unwrap();
        assert_eq!(backends.len(), 2, "deduped, sorted");
        assert_eq!(backends[0], "10.244.0.5:80".parse::<SocketAddr>().unwrap());
        assert_eq!(backends[1], "10.244.0.6:80".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn compute_desired_vips_returns_empty_backends_when_no_endpoints_exist() {
        let services = vec![svc("orphan", "default", "10.96.0.99", 443, None)];
        let desired = compute_desired_vips(&services, &[]);
        let backends = &desired[&"10.96.0.99:443".parse::<SocketAddr>().unwrap()];
        assert!(
            backends.is_empty(),
            "Service with no EndpointSlice → empty backends; \
             RoundRobinPicker returns None and dispatcher drops SYNs"
        );
    }
}
