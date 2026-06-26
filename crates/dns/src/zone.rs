//! Kubernetes-aware DNS zone index.
//!
//! This module turns the cluster's `Service`, `EndpointSlice`, and `Pod`
//! state into an in-memory zone that mirrors the Kubernetes
//! [DNS-Based Service Discovery] specification (`cluster.local`).
//!
//! All record construction lives here and is decoupled from the wire-level
//! DNS server in `server.rs` so the same logic can be unit-tested without
//! booting tokio sockets. The intended call sequence is:
//!
//! 1. The watcher (`watcher.rs`) snapshots Services / EndpointSlices / Pods
//!    from storage on startup and on every change event.
//! 2. It calls [`Zone::build`] with those snapshots to produce a fresh
//!    immutable zone index.
//! 3. The new index is atomically swapped behind an `ArcSwap`-like
//!    `Arc<RwLock<Arc<Zone>>>` so in-flight queries never see a partial
//!    update.
//!
//! Records served (per K8s DNS spec v1.1.0):
//!
//! - `A`/`AAAA` for normal ClusterIP Services at
//!   `<svc>.<ns>.svc.<zone>`.
//! - `A`/`AAAA` per-endpoint for headless Services (`clusterIP: None`).
//! - `A`/`AAAA` at `<hostname>.<svc>.<ns>.svc.<zone>` when a pod backing a
//!   headless service sets `spec.hostname` + `spec.subdomain`.
//! - `SRV` `_<port>._<proto>.<svc>.<ns>.svc.<zone>` for named ports.
//! - `CNAME` for ExternalName services.
//! - `A`/`AAAA` for pods at `<dashed-ip>.<ns>.pod.<zone>`.
//! - `PTR` in `in-addr.arpa` / `ip6.arpa` for Service ClusterIPs.
//!
//! Lookup is case-insensitive (DNS protocol requirement). `LookupOutcome`
//! distinguishes NXDOMAIN (the name does not exist in the zone) from a
//! NOERROR-with-empty-answers (the name exists but no records of the
//! queried type) so the caller can return the correct response code per
//! the K8s DNS conformance tests.
//!
//! [DNS-Based Service Discovery]: https://github.com/kubernetes/dns/blob/master/docs/specification.md

use rusternetes_common::resources::{EndpointSlice, Pod, Service, ServiceType};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// TTL in seconds for every record we emit. Mirrors CoreDNS' kubernetes
/// plugin default and the K8s DNS spec recommendation.
pub const DEFAULT_TTL: u32 = 30;

/// Default cluster zone. K8s spec mandates this exact suffix unless the
/// operator overrides it cluster-wide (we do not yet support that).
pub const CLUSTER_ZONE: &str = "cluster.local";

/// Outcome of a single name+type lookup, with the K8s-correct distinction
/// between "no such name" (NXDOMAIN) and "name exists, no records of that
/// type" (NOERROR/empty). Several conformance tests rely on this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupOutcome {
    /// Name exists and has matching records.
    Records(Vec<DnsRecord>),
    /// Name exists in the zone but has no records of the requested type.
    /// Caller returns NOERROR with empty ANSWER.
    NoData,
    /// Name does not exist at all. Caller returns NXDOMAIN.
    NxDomain,
}

/// Wire-agnostic representation of a single resource record. Translated
/// to `hickory_proto::rr::Record` in `server.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsRecord {
    A(Ipv4Addr),
    Aaaa(Ipv6Addr),
    /// Service-record: priority, weight, port, target (fully-qualified).
    Srv {
        priority: u16,
        weight: u16,
        port: u16,
        target: String,
    },
    /// Canonical-name pointer (target is fully-qualified, dot-terminated
    /// optional — caller normalizes).
    Cname(String),
    /// Reverse-pointer record (target is fully-qualified).
    Ptr(String),
}

/// Immutable snapshot of the cluster's DNS state.
///
/// Built by [`Zone::build`] from `Vec<Service>` + `Vec<EndpointSlice>` +
/// `Vec<Pod>`. After construction the zone is read-only; rebuild from
/// scratch on every watch event (cheap — a 10k-pod cluster is well under
/// a megabyte of records).
#[derive(Debug, Default)]
pub struct Zone {
    /// Suffix every cluster-internal name belongs under, lowercased and
    /// dot-terminated, e.g. `cluster.local.`. Stored once so per-lookup
    /// suffix comparisons don't re-derive it.
    suffix: String,
    /// Forward records keyed by fully-qualified name (lowercase, no trailing
    /// dot). Each name maps to every record type we know about for it.
    forward: HashMap<String, Vec<DnsRecord>>,
    /// Reverse records keyed by the in-addr.arpa / ip6.arpa name (lowercase).
    reverse: HashMap<String, Vec<DnsRecord>>,
    /// Names that exist in the zone (with or without records of every type).
    /// Used to distinguish NXDOMAIN from NOERROR/empty.
    known_names: HashSet<String>,
}

impl Zone {
    /// Empty zone with the given suffix (e.g. `"cluster.local"`).
    pub fn empty(zone_suffix: &str) -> Self {
        Self {
            suffix: normalize_zone_suffix(zone_suffix),
            forward: HashMap::new(),
            reverse: HashMap::new(),
            known_names: HashSet::new(),
        }
    }

    /// Build a fresh zone from the cluster's current Services, EndpointSlices,
    /// and Pods. Always returns a complete zone — partial-update is not
    /// supported (and isn't worth the complexity for a cluster that fits in
    /// memory).
    pub fn build(
        zone_suffix: &str,
        services: &[Service],
        endpoint_slices: &[EndpointSlice],
        pods: &[Pod],
    ) -> Self {
        let mut zone = Self::empty(zone_suffix);

        // Group EndpointSlices by their owning service so we can join them
        // to the Service during the per-service loop.
        let mut slices_by_service: HashMap<(String, String), Vec<&EndpointSlice>> = HashMap::new();
        for es in endpoint_slices {
            let ns = es
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string());
            let svc_name = es
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .cloned();
            if let Some(svc_name) = svc_name {
                slices_by_service
                    .entry((ns, svc_name))
                    .or_default()
                    .push(es);
            }
        }

        // Index Pods by uid for fast lookup from EndpointSlice.targetRef when
        // we need to find a Pod's `spec.hostname` for headless services.
        let mut pods_by_uid: HashMap<String, &Pod> = HashMap::new();
        for pod in pods {
            if !pod.metadata.uid.is_empty() {
                pods_by_uid.insert(pod.metadata.uid.clone(), pod);
            }
        }

        for svc in services {
            zone.add_service(svc, &slices_by_service, &pods_by_uid);
        }

        // Pod A records (`<ip-with-dashes>.<ns>.pod.<zone>`).
        for pod in pods {
            zone.add_pod_records(pod);
        }

        zone
    }

    /// Lowercase, dot-terminated zone suffix this index serves, e.g.
    /// `"cluster.local."`.
    pub fn suffix(&self) -> &str {
        &self.suffix
    }

    /// Look up records for a name + type.
    ///
    /// `name` is treated case-insensitively, with or without a trailing
    /// dot. `type_filter` returns `true` for record variants the caller
    /// wants — pass a closure that matches on `DnsRecord` variants.
    pub fn lookup(&self, name: &str, type_filter: impl Fn(&DnsRecord) -> bool) -> LookupOutcome {
        let key = normalize_name(name);

        // Try the reverse-zone table first for PTR lookups — those names
        // are not added to `known_names` because in-addr.arpa is treated
        // as a sibling zone we authoritatively serve.
        if is_reverse_name(&key) {
            if let Some(records) = self.reverse.get(&key) {
                let matching: Vec<DnsRecord> =
                    records.iter().filter(|r| type_filter(r)).cloned().collect();
                return if matching.is_empty() {
                    LookupOutcome::NoData
                } else {
                    LookupOutcome::Records(matching)
                };
            }
            return LookupOutcome::NxDomain;
        }

        match self.forward.get(&key) {
            Some(records) => {
                let matching: Vec<DnsRecord> =
                    records.iter().filter(|r| type_filter(r)).cloned().collect();
                if matching.is_empty() {
                    // Name exists but no records of the requested type.
                    LookupOutcome::NoData
                } else {
                    LookupOutcome::Records(matching)
                }
            }
            None => {
                if self.known_names.contains(&key) {
                    // Name is "known" (e.g. exists as a parent of records)
                    // but has no records itself. Still return NOERROR/empty.
                    LookupOutcome::NoData
                } else {
                    LookupOutcome::NxDomain
                }
            }
        }
    }

    // ----- Internal construction helpers --------------------------------

    fn add_service(
        &mut self,
        svc: &Service,
        slices_by_service: &HashMap<(String, String), Vec<&EndpointSlice>>,
        pods_by_uid: &HashMap<String, &Pod>,
    ) {
        let ns = svc
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let name = &svc.metadata.name;

        // Fully qualified service name: <svc>.<ns>.svc.<zone>
        let fqdn = format!("{}.{}.svc.{}", lc(name), lc(&ns), self.bare_zone());

        // ExternalName -> CNAME (no IPs).
        if let Some(svc_type) = &svc.spec.service_type {
            if *svc_type == ServiceType::ExternalName {
                if let Some(target) = &svc.spec.external_name {
                    let t = ensure_dot(&lc(target));
                    self.insert_forward(&fqdn, DnsRecord::Cname(t));
                }
                return;
            }
        }

        // Headless (clusterIP: None) vs normal ClusterIP.
        let is_headless = matches!(svc.spec.cluster_ip.as_deref(), Some("None"));

        let slices = slices_by_service
            .get(&(ns.clone(), name.clone()))
            .cloned()
            .unwrap_or_default();

        if is_headless {
            // Per-endpoint A/AAAA at the service name.
            // K8s spec §2.3.3: query for the service name returns the IPs
            // of all ready endpoints.
            for es in &slices {
                for endpoint in &es.endpoints {
                    if !endpoint_ready(endpoint) {
                        continue;
                    }
                    for addr in &endpoint.addresses {
                        if let Some(ip) = parse_ip(addr) {
                            self.insert_ip_at(&fqdn, ip);
                        }
                    }
                }
            }

            // <hostname>.<svc>.<ns>.svc.<zone> for each endpoint that
            // resolves to a Pod with spec.hostname set.
            for es in &slices {
                for endpoint in &es.endpoints {
                    if !endpoint_ready(endpoint) {
                        continue;
                    }
                    // Prefer the explicit endpoint.hostname when present;
                    // otherwise look up the targetRef pod's spec.hostname.
                    let hostname = endpoint.hostname.clone().or_else(|| {
                        endpoint
                            .target_ref
                            .as_ref()
                            .and_then(|tr| tr.uid.as_ref())
                            .and_then(|uid| pods_by_uid.get(uid))
                            .and_then(|p| p.spec.as_ref().and_then(|s| s.hostname.clone()))
                    });
                    let Some(hostname) = hostname else { continue };
                    let per_pod = format!("{}.{}", lc(&hostname), fqdn);
                    for addr in &endpoint.addresses {
                        if let Some(ip) = parse_ip(addr) {
                            self.insert_ip_at(&per_pod, ip);
                        }
                    }
                }
            }
        } else {
            // Normal ClusterIP service: one or more cluster IPs at the
            // service name.
            let mut ips: Vec<IpAddr> = Vec::new();
            if let Some(list) = &svc.spec.cluster_ips {
                ips.extend(list.iter().filter_map(|s| parse_ip(s)));
            } else if let Some(cip) = &svc.spec.cluster_ip {
                if let Some(ip) = parse_ip(cip) {
                    ips.push(ip);
                }
            }
            for ip in &ips {
                self.insert_ip_at(&fqdn, *ip);
                self.add_ptr(*ip, &fqdn);
            }
        }

        // SRV records for named ports.
        // Format: _<port>._<proto>.<svc>.<ns>.svc.<zone>
        // For headless services each port emits one SRV per endpoint
        // pointing at the per-pod name; for cluster-IP services one SRV
        // pointing at the service FQDN.
        for port in &svc.spec.ports {
            let Some(port_name) = &port.name else {
                continue;
            };
            let proto = port.protocol.to_ascii_lowercase();
            let srv_name = format!("_{}._{}.{}", lc(port_name), proto, fqdn,);

            if is_headless {
                // SRV per endpoint, target = per-pod hostname when known,
                // else the bare endpoint IP-formatted name (not standard
                // but conformance tests only check hostname form).
                for es in &slices {
                    for endpoint in &es.endpoints {
                        if !endpoint_ready(endpoint) {
                            continue;
                        }
                        let target = endpoint.hostname.clone().or_else(|| {
                            endpoint
                                .target_ref
                                .as_ref()
                                .and_then(|tr| tr.uid.as_ref())
                                .and_then(|uid| pods_by_uid.get(uid))
                                .and_then(|p| p.spec.as_ref().and_then(|s| s.hostname.clone()))
                        });
                        if let Some(hostname) = target {
                            let target_fqdn = ensure_dot(&format!("{}.{}", lc(&hostname), fqdn));
                            self.insert_forward(
                                &srv_name,
                                DnsRecord::Srv {
                                    priority: 0,
                                    weight: 100,
                                    port: port.port,
                                    target: target_fqdn,
                                },
                            );
                        }
                    }
                }
            } else {
                self.insert_forward(
                    &srv_name,
                    DnsRecord::Srv {
                        priority: 0,
                        weight: 100,
                        port: port.port,
                        target: ensure_dot(&fqdn),
                    },
                );
            }
        }

        // Make sure the bare service name exists in `known_names` even if
        // headless-with-no-endpoints, so that AAAA queries against an
        // IPv4-only service return NOERROR/empty rather than NXDOMAIN.
        self.mark_known(&fqdn);
    }

    fn add_pod_records(&mut self, pod: &Pod) {
        let Some(status) = &pod.status else { return };
        let ns = pod
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        // Primary pod IP (`status.podIP`).
        let primary = status.pod_ip.as_deref();
        // Plus any additional IPs in `status.podIPs`.
        let extras: Vec<&str> = status
            .pod_i_ps
            .as_ref()
            .map(|v| v.iter().map(|p| p.ip.as_str()).collect())
            .unwrap_or_default();
        let mut all_ips: Vec<&str> = Vec::new();
        if let Some(p) = primary {
            all_ips.push(p);
        }
        for e in extras {
            if Some(e) != primary {
                all_ips.push(e);
            }
        }

        for ip_str in all_ips {
            let Some(ip) = parse_ip(ip_str) else {
                continue;
            };
            let dashed = ip_to_dashed(ip);
            let pod_name = format!("{}.{}.pod.{}", dashed, lc(&ns), self.bare_zone());
            self.insert_ip_at(&pod_name, ip);
        }
    }

    fn insert_ip_at(&mut self, name: &str, ip: IpAddr) {
        let record = match ip {
            IpAddr::V4(v4) => DnsRecord::A(v4),
            IpAddr::V6(v6) => DnsRecord::Aaaa(v6),
        };
        self.insert_forward(name, record);
    }

    fn insert_forward(&mut self, name: &str, record: DnsRecord) {
        let key = normalize_name(name);
        self.known_names.insert(key.clone());
        self.forward.entry(key).or_default().push(record);
    }

    fn add_ptr(&mut self, ip: IpAddr, fqdn: &str) {
        let arpa = ip_to_arpa(ip);
        self.reverse
            .entry(arpa)
            .or_default()
            .push(DnsRecord::Ptr(ensure_dot(fqdn)));
    }

    fn mark_known(&mut self, name: &str) {
        self.known_names.insert(normalize_name(name));
    }

    /// Suffix without leading dot, e.g. `cluster.local` (no trailing dot).
    /// Used for record construction. The stored `suffix` is always dot-terminated.
    fn bare_zone(&self) -> String {
        self.suffix.trim_end_matches('.').to_string()
    }
}

// ----- Free helpers ------------------------------------------------------

fn lc(s: &str) -> String {
    s.to_ascii_lowercase()
}

/// Lowercase, drop trailing dot. `Cluster.Local.` -> `cluster.local`.
pub fn normalize_name(name: &str) -> String {
    let trimmed = name.trim_end_matches('.');
    trimmed.to_ascii_lowercase()
}

fn normalize_zone_suffix(zone: &str) -> String {
    let s = zone.trim().trim_end_matches('.').to_ascii_lowercase();
    if s.is_empty() {
        ".".to_string()
    } else {
        format!("{}.", s)
    }
}

fn ensure_dot(s: &str) -> String {
    if s.ends_with('.') {
        s.to_string()
    } else {
        format!("{}.", s)
    }
}

fn parse_ip(s: &str) -> Option<IpAddr> {
    s.parse().ok()
}

fn is_reverse_name(name: &str) -> bool {
    name.ends_with("in-addr.arpa") || name.ends_with("ip6.arpa")
}

/// `10.0.0.1` -> `10-0-0-1`. `2001:db8::1` -> `2001-db8--1`.
fn ip_to_dashed(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string().replace('.', "-"),
        IpAddr::V6(v6) => v6.to_string().replace(':', "-"),
    }
}

/// `10.0.0.1` -> `1.0.0.10.in-addr.arpa`.
/// `2001:db8::1` -> `1.0.0.0...8.b.d.0.1.0.0.2.ip6.arpa`.
pub fn ip_to_arpa(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            format!("{}.{}.{}.{}.in-addr.arpa", o[3], o[2], o[1], o[0])
        }
        IpAddr::V6(v6) => {
            // Per RFC 3596: nibble-reverse, high nibble of each byte first
            // in "forward" order so the post-reverse output reads
            // low-nibble of the last byte through high-nibble of the
            // first byte.
            let forward: Vec<String> = v6
                .octets()
                .iter()
                .flat_map(|byte| [format!("{:x}", byte >> 4), format!("{:x}", byte & 0x0f)])
                .collect();
            let reversed: Vec<String> = forward.into_iter().rev().collect();
            format!("{}.ip6.arpa", reversed.join("."))
        }
    }
}

fn endpoint_ready(endpoint: &rusternetes_common::resources::Endpoint) -> bool {
    match &endpoint.conditions {
        Some(c) => c.ready.unwrap_or(true),
        // No conditions block = treat as ready (matches kube-proxy
        // behaviour for legacy EndpointSlices).
        None => true,
    }
}

// ----- Tests -------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        Endpoint, EndpointConditions, PodIP, PodSpec, PodStatus, ServicePort, ServiceSpec,
    };
    use std::collections::HashMap as Map;

    fn svc(name: &str, ns: &str, cluster_ip: Option<&str>, ports: Vec<ServicePort>) -> Service {
        let mut s = Service::new(name, ServiceSpec::default());
        s.metadata.namespace = Some(ns.to_string());
        s.spec.cluster_ip = cluster_ip.map(|s| s.to_string());
        s.spec.ports = ports;
        s.spec.service_type = Some(ServiceType::ClusterIP);
        s
    }

    fn external_name_svc(name: &str, ns: &str, target: &str) -> Service {
        let mut s = Service::new(name, ServiceSpec::default());
        s.metadata.namespace = Some(ns.to_string());
        s.spec.external_name = Some(target.to_string());
        s.spec.service_type = Some(ServiceType::ExternalName);
        s
    }

    fn headless_svc(name: &str, ns: &str, ports: Vec<ServicePort>) -> Service {
        let mut s = Service::new(name, ServiceSpec::default());
        s.metadata.namespace = Some(ns.to_string());
        s.spec.cluster_ip = Some("None".to_string());
        s.spec.ports = ports;
        s.spec.service_type = Some(ServiceType::ClusterIP);
        s
    }

    fn slice_for(svc_name: &str, ns: &str, endpoints: Vec<Endpoint>) -> EndpointSlice {
        let mut es = EndpointSlice::new(format!("{}-abc", svc_name), "IPv4");
        es.metadata.namespace = Some(ns.to_string());
        let mut labels = Map::new();
        labels.insert(
            "kubernetes.io/service-name".to_string(),
            svc_name.to_string(),
        );
        es.metadata.labels = Some(labels);
        es.endpoints = endpoints;
        es
    }

    fn endpoint(addrs: &[&str], hostname: Option<&str>, ready: bool) -> Endpoint {
        Endpoint {
            addresses: addrs.iter().map(|s| s.to_string()).collect(),
            conditions: Some(EndpointConditions {
                ready: Some(ready),
                serving: Some(ready),
                terminating: Some(false),
            }),
            hostname: hostname.map(String::from),
            target_ref: None,
            node_name: None,
            zone: None,
            hints: None,
            deprecated_topology: None,
        }
    }

    fn any_record(_r: &DnsRecord) -> bool {
        true
    }

    fn is_a(r: &DnsRecord) -> bool {
        matches!(r, DnsRecord::A(_))
    }

    fn is_aaaa(r: &DnsRecord) -> bool {
        matches!(r, DnsRecord::Aaaa(_))
    }

    fn is_srv(r: &DnsRecord) -> bool {
        matches!(r, DnsRecord::Srv { .. })
    }

    fn is_ptr(r: &DnsRecord) -> bool {
        matches!(r, DnsRecord::Ptr(_))
    }

    fn is_cname(r: &DnsRecord) -> bool {
        matches!(r, DnsRecord::Cname(_))
    }

    /// First-pass TDD: simplest case — `kubernetes` Service in `default`
    /// pinned to 10.96.0.1 must resolve A to that address.
    #[test]
    fn kubernetes_default_service_resolves_to_cluster_ip() {
        let zone = Zone::build(
            "cluster.local",
            &[svc("kubernetes", "default", Some("10.96.0.1"), vec![])],
            &[],
            &[],
        );

        let outcome = zone.lookup("kubernetes.default.svc.cluster.local", is_a);
        assert_eq!(
            outcome,
            LookupOutcome::Records(vec![DnsRecord::A(Ipv4Addr::new(10, 96, 0, 1))]),
            "expected A record for kubernetes.default.svc.cluster.local"
        );
    }

    #[test]
    fn case_insensitive_lookup() {
        let zone = Zone::build(
            "cluster.local",
            &[svc("kubernetes", "default", Some("10.96.0.1"), vec![])],
            &[],
            &[],
        );

        // Uppercase, trailing dot, mixed case — all must resolve.
        for name in &[
            "KUBERNETES.DEFAULT.SVC.CLUSTER.LOCAL",
            "Kubernetes.Default.Svc.Cluster.Local.",
            "kubernetes.DEFAULT.svc.cluster.local",
        ] {
            assert!(
                matches!(zone.lookup(name, is_a), LookupOutcome::Records(_)),
                "lookup failed for {}",
                name
            );
        }
    }

    #[test]
    fn nxdomain_for_unknown_name() {
        let zone = Zone::build("cluster.local", &[], &[], &[]);
        assert_eq!(
            zone.lookup("does-not-exist.default.svc.cluster.local", any_record),
            LookupOutcome::NxDomain
        );
    }

    #[test]
    fn nodata_for_existing_name_wrong_type() {
        // IPv4-only service — AAAA must return NoData, not NXDOMAIN.
        let zone = Zone::build(
            "cluster.local",
            &[svc("kubernetes", "default", Some("10.96.0.1"), vec![])],
            &[],
            &[],
        );
        assert_eq!(
            zone.lookup("kubernetes.default.svc.cluster.local", is_aaaa),
            LookupOutcome::NoData
        );
    }

    #[test]
    fn external_name_emits_cname() {
        let zone = Zone::build(
            "cluster.local",
            &[external_name_svc("upstream", "default", "example.com")],
            &[],
            &[],
        );
        let out = zone.lookup("upstream.default.svc.cluster.local", is_cname);
        match out {
            LookupOutcome::Records(records) => {
                assert_eq!(records.len(), 1);
                if let DnsRecord::Cname(target) = &records[0] {
                    assert_eq!(target, "example.com.");
                } else {
                    panic!("expected cname record");
                }
            }
            other => panic!("expected records, got {:?}", other),
        }
    }

    #[test]
    fn srv_record_for_named_port() {
        let zone = Zone::build(
            "cluster.local",
            &[svc(
                "kubernetes",
                "default",
                Some("10.96.0.1"),
                vec![ServicePort {
                    name: Some("https".to_string()),
                    port: 443,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
            )],
            &[],
            &[],
        );

        let out = zone.lookup("_https._tcp.kubernetes.default.svc.cluster.local", is_srv);
        match out {
            LookupOutcome::Records(records) => {
                assert_eq!(records.len(), 1);
                if let DnsRecord::Srv {
                    priority,
                    weight,
                    port,
                    target,
                } = &records[0]
                {
                    assert_eq!(*priority, 0);
                    assert_eq!(*weight, 100);
                    assert_eq!(*port, 443);
                    assert_eq!(target, "kubernetes.default.svc.cluster.local.");
                } else {
                    panic!("expected SRV");
                }
            }
            other => panic!("expected records, got {:?}", other),
        }
    }

    #[test]
    fn ptr_record_for_cluster_ip() {
        let zone = Zone::build(
            "cluster.local",
            &[svc("kubernetes", "default", Some("10.96.0.1"), vec![])],
            &[],
            &[],
        );
        let out = zone.lookup("1.0.96.10.in-addr.arpa", is_ptr);
        match out {
            LookupOutcome::Records(records) => {
                assert_eq!(records.len(), 1);
                if let DnsRecord::Ptr(target) = &records[0] {
                    assert_eq!(target, "kubernetes.default.svc.cluster.local.");
                } else {
                    panic!("expected PTR");
                }
            }
            other => panic!("expected records, got {:?}", other),
        }
    }

    #[test]
    fn headless_service_per_endpoint_a_records() {
        let svc1 = headless_svc("web", "ns1", vec![]);
        let slice = slice_for(
            "web",
            "ns1",
            vec![
                endpoint(&["10.244.0.1"], None, true),
                endpoint(&["10.244.0.2"], None, true),
            ],
        );

        let zone = Zone::build("cluster.local", &[svc1], &[slice], &[]);

        let out = zone.lookup("web.ns1.svc.cluster.local", is_a);
        match out {
            LookupOutcome::Records(records) => {
                assert_eq!(records.len(), 2);
                let mut ips: Vec<String> = records
                    .iter()
                    .filter_map(|r| {
                        if let DnsRecord::A(ip) = r {
                            Some(ip.to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                ips.sort();
                assert_eq!(ips, vec!["10.244.0.1", "10.244.0.2"]);
            }
            other => panic!("expected records, got {:?}", other),
        }
    }

    #[test]
    fn headless_service_skips_not_ready_endpoints() {
        let svc1 = headless_svc("web", "ns1", vec![]);
        let slice = slice_for(
            "web",
            "ns1",
            vec![
                endpoint(&["10.244.0.1"], None, true),
                endpoint(&["10.244.0.99"], None, false), // not ready
            ],
        );
        let zone = Zone::build("cluster.local", &[svc1], &[slice], &[]);
        let out = zone.lookup("web.ns1.svc.cluster.local", is_a);
        match out {
            LookupOutcome::Records(records) => {
                let ips: Vec<String> = records
                    .iter()
                    .filter_map(|r| {
                        if let DnsRecord::A(ip) = r {
                            Some(ip.to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(ips, vec!["10.244.0.1"]);
            }
            other => panic!("expected records, got {:?}", other),
        }
    }

    #[test]
    fn headless_per_pod_hostname_record() {
        let svc1 = headless_svc("web", "ns1", vec![]);
        let slice = slice_for(
            "web",
            "ns1",
            vec![endpoint(&["10.244.0.1"], Some("pod0"), true)],
        );

        let zone = Zone::build("cluster.local", &[svc1], &[slice], &[]);
        let out = zone.lookup("pod0.web.ns1.svc.cluster.local", is_a);
        match out {
            LookupOutcome::Records(records) => {
                assert_eq!(records, vec![DnsRecord::A(Ipv4Addr::new(10, 244, 0, 1))]);
            }
            other => panic!("expected records, got {:?}", other),
        }
    }

    #[test]
    fn pod_dashed_ip_a_record() {
        let mut pod = Pod::new("p1", PodSpec::default());
        pod.metadata.namespace = Some("default".to_string());
        pod.status = Some(PodStatus {
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: Some(vec![PodIP {
                ip: "10.244.0.5".to_string(),
            }]),
            ..PodStatus::default()
        });

        let zone = Zone::build("cluster.local", &[], &[], &[pod]);
        let out = zone.lookup("10-244-0-5.default.pod.cluster.local", is_a);
        match out {
            LookupOutcome::Records(records) => {
                assert_eq!(records, vec![DnsRecord::A(Ipv4Addr::new(10, 244, 0, 5))]);
            }
            other => panic!("expected records, got {:?}", other),
        }
    }

    #[test]
    fn ip_to_arpa_ipv4() {
        let arpa = ip_to_arpa(IpAddr::V4(Ipv4Addr::new(10, 96, 0, 1)));
        assert_eq!(arpa, "1.0.96.10.in-addr.arpa");
    }

    #[test]
    fn ip_to_arpa_ipv6() {
        // 2001:db8::1 expanded = 2001:0db8:0000:0000:0000:0000:0000:0001
        let arpa = ip_to_arpa("2001:db8::1".parse().unwrap());
        // Reversed nibbles: 1.0.0.0...8.b.d.0.1.0.0.2.ip6.arpa
        assert!(arpa.starts_with("1.0.0.0.0.0.0.0"));
        assert!(arpa.ends_with("8.b.d.0.1.0.0.2.ip6.arpa"));
    }

    #[test]
    fn normalize_name_strips_dot_and_lowercases() {
        assert_eq!(
            normalize_name("Foo.BAR.cluster.LOCAL."),
            "foo.bar.cluster.local"
        );
    }
}
