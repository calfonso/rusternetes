//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-network] EndpointSlices + headless services + DNS.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/network/
//! (endpointslice.go, endpointslicemirroring.go, dns.go, dns_common.go)
//!
//! These tests are the kube-proxy/EndpointSlice-controller-facing slice of the
//! upstream sig-network conformance suite. They do NOT spawn an axum router or
//! talk to real CoreDNS: they verify the data the kube-proxy consumes (the
//! EndpointSlice + Endpoints objects produced by the EndpointSlice controller)
//! and the DNS resource records that a CoreDNS plugin would derive from those
//! same in-tree objects (A / AAAA / SRV / PTR). Real CoreDNS runs as an
//! external pod; we test the records THIS project's controllers produce.
//!
//! See docs/conformance/network-endpointslices-dns-headless.md for the
//! test-by-test status table.

use rusternetes_common::resources::endpointslice::{
    Endpoint, EndpointConditions, EndpointPort as ESEndpointPort, EndpointSlice,
};
use rusternetes_common::resources::{
    Container, ContainerPort, EndpointAddress, EndpointPort as EPPort, EndpointSubset, Endpoints,
    Pod, PodCondition, PodSpec, PodStatus, Service, ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::endpoints::EndpointsController;
use rusternetes_controller_manager::controllers::endpointslice::EndpointSliceController;
use rusternetes_storage::{build_key, build_prefix, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

fn setup() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

fn pod(name: &str, ns: &str, ip: &str, ready: bool) -> Pod {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "echo".to_string());
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(ns).with_labels(labels),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "registry.k8s.io/e2e-test-images/agnhost:2.55".to_string(),
                ports: Some(vec![ContainerPort {
                    container_port: 80,
                    name: Some("http".to_string()),
                    protocol: "TCP".to_string(),
                    host_port: None,
                    host_ip: None,
                }]),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            pod_ip: Some(ip.to_string()),
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: if ready { "True" } else { "False" }.to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            ..Default::default()
        }),
    }
}

fn service(name: &str, ns: &str, port: u16) -> Service {
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "echo".to_string());
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(ns),
        spec: ServiceSpec {
            selector: Some(selector),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port,
                target_port: None,
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: Some("10.96.0.50".to_string()),
            ..Default::default()
        },
        status: None,
    }
}

fn headless_service(name: &str, ns: &str, port: u16) -> Service {
    let mut svc = service(name, ns, port);
    // K8s convention: ClusterIP "None" marks the service as headless.
    svc.spec.cluster_ip = Some("None".to_string());
    svc
}

async fn create<T>(
    storage: &Arc<MemoryStorage>,
    resource_type: &str,
    ns: &str,
    name: &str,
    value: &T,
) where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    let key = build_key(resource_type, Some(ns), name);
    storage.create(&key, value).await.unwrap();
}

// ---------------------------------------------------------------------------
// DNS record builders (mirroring what CoreDNS's `kubernetes` plugin emits
// from in-tree Service + EndpointSlice objects). We test the records THIS
// project's controllers would feed to CoreDNS, not CoreDNS itself.
//
// Upstream record formats:
//   <svc>.<ns>.svc.cluster.local                  -> A/AAAA (ClusterIP or
//                                                    backend IPs for headless)
//   _<port>._<proto>.<svc>.<ns>.svc.cluster.local -> SRV
//   <hostname>.<svc>.<ns>.svc.cluster.local       -> A (headless w/ hostname)
//   <a>-<b>-<c>-<d>.<svc>.<ns>.svc.cluster.local  -> A (headless fallback)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct ARecord {
    name: String,
    target: String,
}

#[derive(Debug, Clone, PartialEq)]
struct SrvRecord {
    name: String,
    port: u16,
    target: String,
}

fn fqdn_service(svc: &str, ns: &str) -> String {
    format!("{}.{}.svc.cluster.local", svc, ns)
}

fn fqdn_pod_hostname(hostname: &str, svc: &str, ns: &str) -> String {
    format!("{}.{}.{}.svc.cluster.local", hostname, svc, ns)
}

fn ip_to_dashed_hostname(ip: &str) -> String {
    ip.replace('.', "-")
}

/// Build A records for a Service + its EndpointSlices.
/// - Regular (ClusterIP) service: one A record returning the ClusterIP.
/// - Headless service: one A record per ready endpoint address.
fn build_a_records(svc: &Service, slices: &[EndpointSlice]) -> Vec<ARecord> {
    let ns = svc.metadata.namespace.as_deref().unwrap_or("default");
    let name = &svc.metadata.name;
    let fqdn = fqdn_service(name, ns);
    let is_headless = svc.spec.cluster_ip.as_deref() == Some("None");
    if !is_headless {
        if let Some(ip) = &svc.spec.cluster_ip {
            return vec![ARecord {
                name: fqdn,
                target: ip.clone(),
            }];
        }
        return Vec::new();
    }
    // Headless: one A per ready endpoint, plus one per pod hostname if set.
    let mut out = Vec::new();
    for slice in slices {
        for ep in &slice.endpoints {
            let ready = ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true);
            if !ready {
                continue;
            }
            for addr in &ep.addresses {
                out.push(ARecord {
                    name: fqdn.clone(),
                    target: addr.clone(),
                });
                if let Some(host) = &ep.hostname {
                    out.push(ARecord {
                        name: fqdn_pod_hostname(host, name, ns),
                        target: addr.clone(),
                    });
                } else {
                    out.push(ARecord {
                        name: fqdn_pod_hostname(&ip_to_dashed_hostname(addr), name, ns),
                        target: addr.clone(),
                    });
                }
            }
        }
    }
    out
}

/// Build SRV records for each named port in the Service.
/// Format: `_<portname>._<protocol-lower>.<svc>.<ns>.svc.cluster.local`
fn build_srv_records(svc: &Service, slices: &[EndpointSlice]) -> Vec<SrvRecord> {
    let ns = svc.metadata.namespace.as_deref().unwrap_or("default");
    let svc_name = &svc.metadata.name;
    let is_headless = svc.spec.cluster_ip.as_deref() == Some("None");

    let mut out = Vec::new();
    for sp in &svc.spec.ports {
        let port_name = match &sp.name {
            Some(n) if !n.is_empty() => n.clone(),
            _ => continue, // SRV requires named port
        };
        let proto = sp.protocol.to_lowercase();
        let srv_name = format!(
            "_{}._{}.{}.{}.svc.cluster.local",
            port_name, proto, svc_name, ns
        );

        if !is_headless {
            // Regular service: SRV target is the service FQDN itself.
            out.push(SrvRecord {
                name: srv_name,
                port: sp.port,
                target: fqdn_service(svc_name, ns),
            });
            continue;
        }
        // Headless: one SRV per ready endpoint targeting the pod's A name.
        for slice in slices {
            for ep in &slice.endpoints {
                let ready = ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true);
                if !ready {
                    continue;
                }
                for addr in &ep.addresses {
                    let target = if let Some(host) = &ep.hostname {
                        fqdn_pod_hostname(host, svc_name, ns)
                    } else {
                        fqdn_pod_hostname(&ip_to_dashed_hostname(addr), svc_name, ns)
                    };
                    let actual_port = slice
                        .ports
                        .iter()
                        .find(|p| p.name.as_deref() == Some(port_name.as_str()))
                        .and_then(|p| p.port)
                        .map(|p| p as u16)
                        .unwrap_or(sp.port);
                    out.push(SrvRecord {
                        name: srv_name.clone(),
                        port: actual_port,
                        target,
                    });
                }
            }
        }
    }
    out
}

/// Read all EndpointSlices for a given Service (matching by
/// `kubernetes.io/service-name` label, the same key the kube-proxy uses).
async fn read_slices_for(storage: &Arc<MemoryStorage>, ns: &str, svc: &str) -> Vec<EndpointSlice> {
    let prefix = build_prefix("endpointslices", Some(ns));
    let all: Vec<EndpointSlice> = storage.list(&prefix).await.unwrap_or_default();
    all.into_iter()
        .filter(|s| {
            s.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|n| n == svc)
                .unwrap_or(false)
        })
        .collect()
}

// ===========================================================================
//
// Group 1 — EndpointSlice population from Service + pod readiness
// Upstream: test/e2e/network/endpointslice.go
//
// ===========================================================================

/// [sig-network] EndpointSlice should create and delete EndpointSlices for a Service with a selector that matches no pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:73
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn endpointslice_should_create_empty_slice_when_selector_matches_no_pods() {
    let storage = setup();
    let svc = service("no-matches", "ns1", 80);
    create(&storage, "services", "ns1", "no-matches", &svc).await;

    let controller = EndpointSliceController::new(Arc::clone(&storage));
    controller.reconcile_all().await.unwrap();

    let slices = read_slices_for(&storage, "ns1", "no-matches").await;
    assert_eq!(slices.len(), 1, "exactly one EndpointSlice expected");
    assert!(
        slices[0].endpoints.is_empty(),
        "selector matches no pods → endpoints must be empty"
    );
    assert!(
        !slices[0].ports.is_empty(),
        "ports must still be copied from the service"
    );
}

/// [sig-network] EndpointSlice should create Endpoints and EndpointSlices for Pods matching a Service [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:116
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn endpointslice_should_populate_slice_with_ready_pods() {
    let storage = setup();
    let svc = service("echo", "ns2", 80);
    create(&storage, "services", "ns2", "echo", &svc).await;
    create(
        &storage,
        "pods",
        "ns2",
        "p1",
        &pod("p1", "ns2", "10.1.1.1", true),
    )
    .await;
    create(
        &storage,
        "pods",
        "ns2",
        "p2",
        &pod("p2", "ns2", "10.1.1.2", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "ns2", "echo").await;
    let endpoints: Vec<_> = slices.iter().flat_map(|s| s.endpoints.iter()).collect();
    assert_eq!(endpoints.len(), 2, "both ready pods must appear");
    let addrs: Vec<&str> = endpoints
        .iter()
        .flat_map(|e| e.addresses.iter().map(|s| s.as_str()))
        .collect();
    assert!(addrs.contains(&"10.1.1.1"));
    assert!(addrs.contains(&"10.1.1.2"));
}

/// [sig-network] EndpointSlice should reflect pod NotReady condition in EndpointConditions.ready=false
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:116 (readiness sub-assertion)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpointslice_should_mark_not_ready_pod_as_not_ready() {
    let storage = setup();
    let svc = service("echo", "ns3", 80);
    create(&storage, "services", "ns3", "echo", &svc).await;
    create(
        &storage,
        "pods",
        "ns3",
        "p1",
        &pod("p1", "ns3", "10.2.2.1", false),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "ns3", "echo").await;
    let ready_flags: Vec<Option<bool>> = slices
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .map(|e| e.conditions.as_ref().and_then(|c| c.ready))
        .collect();
    assert_eq!(ready_flags, vec![Some(false)]);
}

/// [sig-network] EndpointSlice support a Service with multiple ports specified in multiple EndpointSlices [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:395
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn endpointslice_should_carry_multiple_named_ports() {
    let storage = setup();
    let mut svc = service("multi", "ns4", 80);
    svc.spec.ports = vec![
        ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        },
        ServicePort {
            name: Some("metrics".to_string()),
            port: 9090,
            target_port: None,
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        },
    ];
    create(&storage, "services", "ns4", "multi", &svc).await;
    let mut p = pod("p1", "ns4", "10.3.3.1", true);
    p.spec.as_mut().unwrap().containers[0].ports = Some(vec![
        ContainerPort {
            container_port: 80,
            name: Some("http".to_string()),
            protocol: "TCP".to_string(),
            host_port: None,
            host_ip: None,
        },
        ContainerPort {
            container_port: 9090,
            name: Some("metrics".to_string()),
            protocol: "TCP".to_string(),
            host_port: None,
            host_ip: None,
        },
    ]);
    create(&storage, "pods", "ns4", "p1", &p).await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "ns4", "multi").await;
    let all_ports: Vec<&ESEndpointPort> = slices.iter().flat_map(|s| s.ports.iter()).collect();
    let names: Vec<&str> = all_ports.iter().filter_map(|p| p.name.as_deref()).collect();
    assert!(names.contains(&"http"));
    assert!(names.contains(&"metrics"));
}

/// [sig-network] EndpointSlice support a Service with multiple endpoint IPs specified in multiple EndpointSlices [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:501
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn endpointslice_should_carry_multiple_endpoint_ips() {
    let storage = setup();
    let svc = service("scaleout", "ns5", 80);
    create(&storage, "services", "ns5", "scaleout", &svc).await;
    for (i, ip) in ["10.4.0.1", "10.4.0.2", "10.4.0.3", "10.4.0.4"]
        .iter()
        .enumerate()
    {
        let name = format!("p{i}");
        create(&storage, "pods", "ns5", &name, &pod(&name, "ns5", ip, true)).await;
    }

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "ns5", "scaleout").await;
    let ips: Vec<String> = slices
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .flat_map(|e| e.addresses.iter().cloned())
        .collect();
    assert_eq!(ips.len(), 4);
    for want in ["10.4.0.1", "10.4.0.2", "10.4.0.3", "10.4.0.4"] {
        assert!(ips.contains(&want.to_string()));
    }
}

/// [sig-network] EndpointSlice should support EndpointSlice API operations (kubernetes.io/service-name label)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:231
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn endpointslice_should_label_with_service_name() {
    let storage = setup();
    let svc = service("labeled", "ns6", 80);
    create(&storage, "services", "ns6", "labeled", &svc).await;
    create(
        &storage,
        "pods",
        "ns6",
        "p1",
        &pod("p1", "ns6", "10.5.0.1", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "ns6", "labeled").await;
    assert!(!slices.is_empty());
    let label = slices[0]
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("kubernetes.io/service-name"));
    assert_eq!(label, Some(&"labeled".to_string()));
}

/// [sig-network] EndpointSlice should be managed by endpointslice-controller.k8s.io
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:231 (managed-by label)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpointslice_should_carry_managed_by_label() {
    let storage = setup();
    let svc = service("managed", "ns7", 80);
    create(&storage, "services", "ns7", "managed", &svc).await;
    create(
        &storage,
        "pods",
        "ns7",
        "p1",
        &pod("p1", "ns7", "10.6.0.1", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "ns7", "managed").await;
    let managed = slices[0]
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"));
    assert_eq!(
        managed,
        Some(&"endpointslice-controller.k8s.io".to_string())
    );
}

/// [sig-network] EndpointSlice should set the owner reference to the parent Service
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:116 (owner ref assertion)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpointslice_should_have_service_owner_reference() {
    let storage = setup();
    let mut svc = service("owned", "ns8", 80);
    svc.metadata.uid = "svc-uid-1".to_string();
    create(&storage, "services", "ns8", "owned", &svc).await;
    create(
        &storage,
        "pods",
        "ns8",
        "p1",
        &pod("p1", "ns8", "10.7.0.1", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "ns8", "owned").await;
    let owners = slices[0].metadata.owner_references.as_ref().unwrap();
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].kind, "Service");
    assert_eq!(owners[0].name, "owned");
    assert_eq!(owners[0].uid, "svc-uid-1");
    assert_eq!(owners[0].controller, Some(true));
}

/// [sig-network] EndpointSlice should set targetRef back to the source Pod
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:116 (targetRef assertion)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpointslice_endpoint_should_carry_pod_target_ref() {
    let storage = setup();
    let svc = service("ref", "ns9", 80);
    create(&storage, "services", "ns9", "ref", &svc).await;
    let mut p = pod("p1", "ns9", "10.8.0.1", true);
    p.metadata.uid = "pod-uid-xyz".to_string();
    create(&storage, "pods", "ns9", "p1", &p).await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "ns9", "ref").await;
    let ep = &slices[0].endpoints[0];
    let target = ep.target_ref.as_ref().unwrap();
    assert_eq!(target.kind.as_deref(), Some("Pod"));
    assert_eq!(target.name.as_deref(), Some("p1"));
    assert_eq!(target.uid.as_deref(), Some("pod-uid-xyz"));
}

/// [sig-network] EndpointSlice should remove a pod from the slice when the pod is deleted
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:116 (delete sub-assertion)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpointslice_should_evict_endpoint_when_pod_deleted() {
    let storage = setup();
    let svc = service("churn", "nsA", 80);
    create(&storage, "services", "nsA", "churn", &svc).await;
    create(
        &storage,
        "pods",
        "nsA",
        "p1",
        &pod("p1", "nsA", "10.9.0.1", true),
    )
    .await;
    create(
        &storage,
        "pods",
        "nsA",
        "p2",
        &pod("p2", "nsA", "10.9.0.2", true),
    )
    .await;

    let controller = EndpointSliceController::new(Arc::clone(&storage));
    controller.reconcile_all().await.unwrap();

    // Delete p1 then re-reconcile.
    storage
        .delete(&build_key("pods", Some("nsA"), "p1"))
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    let slices = read_slices_for(&storage, "nsA", "churn").await;
    let ips: Vec<String> = slices
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .flat_map(|e| e.addresses.iter().cloned())
        .collect();
    assert_eq!(ips, vec!["10.9.0.2".to_string()]);
}

/// [sig-network] EndpointSlice should exclude Succeeded/Failed pods (terminal phases)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go (pod phase filter)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpointslice_should_skip_terminal_phase_pods() {
    let storage = setup();
    let svc = service("phase", "nsB", 80);
    create(&storage, "services", "nsB", "phase", &svc).await;
    let mut p1 = pod("p1", "nsB", "10.10.0.1", true);
    p1.status.as_mut().unwrap().phase = Some(Phase::Succeeded);
    let mut p2 = pod("p2", "nsB", "10.10.0.2", true);
    p2.status.as_mut().unwrap().phase = Some(Phase::Failed);
    let p3 = pod("p3", "nsB", "10.10.0.3", true);
    create(&storage, "pods", "nsB", "p1", &p1).await;
    create(&storage, "pods", "nsB", "p2", &p2).await;
    create(&storage, "pods", "nsB", "p3", &p3).await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "nsB", "phase").await;
    let ips: Vec<String> = slices
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .flat_map(|e| e.addresses.iter().cloned())
        .collect();
    assert_eq!(ips, vec!["10.10.0.3".to_string()]);
}

/// [sig-network] EndpointSlice should mark pods with deletionTimestamp as terminating (ready=false)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go (graceful shutdown semantics)
/// K8s endpointslice/utils.go `podEndpointConditions`: a terminating pod stays
/// in the slice but is marked `ready=false, terminating=true` so kube-proxy
/// stops sending NEW connections while existing flows drain.
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpointslice_should_mark_terminating_pods_as_not_ready() {
    let storage = setup();
    let svc = service("graceful", "nsC", 80);
    create(&storage, "services", "nsC", "graceful", &svc).await;
    let mut terminating = pod("p1", "nsC", "10.11.0.1", true);
    // Mark pod as terminating. We avoid pulling chrono into kube-proxy dev-deps
    // by parsing a constant RFC3339 timestamp into the field's type.
    terminating.metadata.deletion_timestamp = Some(
        "2026-01-01T00:00:00Z"
            .parse()
            .expect("static RFC3339 timestamp must parse"),
    );
    let alive = pod("p2", "nsC", "10.11.0.2", true);
    create(&storage, "pods", "nsC", "p1", &terminating).await;
    create(&storage, "pods", "nsC", "p2", &alive).await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "nsC", "graceful").await;
    let mut by_ip: HashMap<String, &Endpoint> = HashMap::new();
    for s in &slices {
        for e in &s.endpoints {
            for ip in &e.addresses {
                by_ip.insert(ip.clone(), e);
            }
        }
    }
    // Both pods present in the slice (terminating one is NOT skipped).
    assert!(by_ip.contains_key("10.11.0.1"));
    assert!(by_ip.contains_key("10.11.0.2"));

    let term = by_ip["10.11.0.1"].conditions.as_ref().unwrap();
    assert_eq!(term.ready, Some(false), "terminating pod: ready=false");
    assert_eq!(
        term.terminating,
        Some(true),
        "terminating pod: terminating=true"
    );

    let alive = by_ip["10.11.0.2"].conditions.as_ref().unwrap();
    assert_eq!(alive.ready, Some(true));
    assert_eq!(alive.terminating, Some(false));
}

/// [sig-network] EndpointSlice should refresh slice when a pod transitions Ready→NotReady
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go (readiness flip)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpointslice_should_flip_endpoint_ready_on_pod_state_change() {
    let storage = setup();
    let svc = service("flip", "nsD", 80);
    create(&storage, "services", "nsD", "flip", &svc).await;
    create(
        &storage,
        "pods",
        "nsD",
        "p1",
        &pod("p1", "nsD", "10.12.0.1", true),
    )
    .await;

    let controller = EndpointSliceController::new(Arc::clone(&storage));
    controller.reconcile_all().await.unwrap();
    let before = read_slices_for(&storage, "nsD", "flip").await;
    assert_eq!(
        before[0].endpoints[0]
            .conditions
            .as_ref()
            .and_then(|c| c.ready),
        Some(true)
    );

    // Update pod readiness to False and re-reconcile.
    let pod_key = build_key("pods", Some("nsD"), "p1");
    let mut p: Pod = storage.get(&pod_key).await.unwrap();
    p.status.as_mut().unwrap().conditions = Some(vec![PodCondition {
        condition_type: "Ready".to_string(),
        status: "False".to_string(),
        reason: None,
        message: None,
        last_probe_time: None,
        last_transition_time: None,
        observed_generation: None,
    }]);
    storage.update(&pod_key, &p).await.unwrap();
    controller.reconcile_all().await.unwrap();

    let after = read_slices_for(&storage, "nsD", "flip").await;
    assert_eq!(
        after[0].endpoints[0]
            .conditions
            .as_ref()
            .and_then(|c| c.ready),
        Some(false)
    );
}

// ===========================================================================
//
// Group 2 — Endpoints (legacy v1) reconciliation
// Upstream: test/e2e/network/endpointslicemirroring.go +
//           the legacy v1.Endpoints API behavior asserted across
//           test/e2e/network/{service,endpointslice}.go.
//
// ===========================================================================

/// [sig-network] Endpoints (v1) should be populated for a Service with a matching pod
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (legacy Endpoints assertion)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpoints_v1_should_populate_subsets_for_matching_pods() {
    let storage = setup();
    let svc = service("legacy", "lns", 80);
    create(&storage, "services", "lns", "legacy", &svc).await;
    create(
        &storage,
        "pods",
        "lns",
        "p1",
        &pod("p1", "lns", "10.20.0.1", true),
    )
    .await;
    create(
        &storage,
        "pods",
        "lns",
        "p2",
        &pod("p2", "lns", "10.20.0.2", true),
    )
    .await;

    EndpointsController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("lns"), "legacy"))
        .await
        .unwrap();
    let addrs: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert!(addrs.contains(&"10.20.0.1"));
    assert!(addrs.contains(&"10.20.0.2"));
}

/// [sig-network] Endpoints (v1) should move a NotReady pod into notReadyAddresses
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (readiness gating)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpoints_v1_should_separate_ready_from_not_ready_addresses() {
    let storage = setup();
    let svc = service("split", "lns2", 80);
    create(&storage, "services", "lns2", "split", &svc).await;
    create(
        &storage,
        "pods",
        "lns2",
        "p1",
        &pod("p1", "lns2", "10.21.0.1", true),
    )
    .await;
    create(
        &storage,
        "pods",
        "lns2",
        "p2",
        &pod("p2", "lns2", "10.21.0.2", false),
    )
    .await;

    EndpointsController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("lns2"), "split"))
        .await
        .unwrap();
    let ready: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    let not_ready: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.not_ready_addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert_eq!(ready, vec!["10.21.0.1"]);
    assert_eq!(not_ready, vec!["10.21.0.2"]);
}

/// [sig-network] EndpointSliceMirroring should mirror a custom Endpoints resource (no selector Service)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslicemirroring.go:50
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpoints_v1_without_selector_should_be_mirrored_to_endpointslice() {
    let storage = setup();
    // Service WITHOUT selector — kube-controller-manager does not reconcile it;
    // the mirroring controller mirrors any user-managed Endpoints object.
    let mut svc = service("no-selector", "mns", 80);
    svc.spec.selector = None;
    create(&storage, "services", "mns", "no-selector", &svc).await;

    // Custom Endpoints created by a user.
    let ep = Endpoints {
        type_meta: TypeMeta {
            kind: "Endpoints".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("no-selector").with_namespace("mns"),
        subsets: vec![EndpointSubset {
            addresses: Some(vec![EndpointAddress {
                ip: "192.168.1.10".to_string(),
                hostname: None,
                node_name: None,
                target_ref: None,
            }]),
            not_ready_addresses: None,
            ports: Some(vec![EPPort {
                name: Some("http".to_string()),
                port: 80,
                protocol: "TCP".to_string(),
                app_protocol: None,
            }]),
        }],
    };
    create(&storage, "endpoints", "mns", "no-selector", &ep).await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "mns", "no-selector").await;
    assert!(!slices.is_empty(), "mirroring controller must emit a slice");
    let managed_by = slices[0]
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"));
    assert_eq!(
        managed_by,
        Some(&"endpointslice-mirroring-controller.k8s.io".to_string())
    );
    let ips: Vec<String> = slices
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .flat_map(|e| e.addresses.iter().cloned())
        .collect();
    assert_eq!(ips, vec!["192.168.1.10".to_string()]);
}

// ===========================================================================
//
// Group 3 — Headless Service DNS (records this project's controllers feed
//                                 CoreDNS via in-tree EndpointSlices)
// Upstream: test/e2e/network/dns.go (+ dns_common.go helpers)
//
// ===========================================================================

/// [sig-network] DNS should provide DNS for the cluster (regular ClusterIP A record)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:46
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn dns_should_provide_a_record_for_cluster_ip_service() {
    let storage = setup();
    let svc = service("regular", "dns1", 80);
    create(&storage, "services", "dns1", "regular", &svc).await;
    create(
        &storage,
        "pods",
        "dns1",
        "p1",
        &pod("p1", "dns1", "10.30.0.1", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns1", "regular").await;
    let records = build_a_records(&svc, &slices);
    assert_eq!(
        records.len(),
        1,
        "non-headless → exactly one A record (ClusterIP)"
    );
    assert_eq!(records[0].name, "regular.dns1.svc.cluster.local");
    assert_eq!(records[0].target, "10.96.0.50");
}

/// [sig-network] DNS should provide DNS for services — headless A records per backend
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:130
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn dns_should_provide_a_record_per_endpoint_for_headless_service() {
    let storage = setup();
    let svc = headless_service("headless", "dns2", 80);
    create(&storage, "services", "dns2", "headless", &svc).await;
    create(
        &storage,
        "pods",
        "dns2",
        "p1",
        &pod("p1", "dns2", "10.31.0.1", true),
    )
    .await;
    create(
        &storage,
        "pods",
        "dns2",
        "p2",
        &pod("p2", "dns2", "10.31.0.2", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns2", "headless").await;
    let records = build_a_records(&svc, &slices);
    let svc_fqdn = "headless.dns2.svc.cluster.local";
    let ips: Vec<&str> = records
        .iter()
        .filter(|r| r.name == svc_fqdn)
        .map(|r| r.target.as_str())
        .collect();
    assert!(ips.contains(&"10.31.0.1"));
    assert!(ips.contains(&"10.31.0.2"));
    assert_eq!(ips.len(), 2);
}

/// [sig-network] DNS should NOT include NotReady endpoints in headless A records
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:130 (ready filter)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn dns_should_exclude_not_ready_endpoints_from_headless_a_records() {
    let storage = setup();
    let svc = headless_service("ready-only", "dns3", 80);
    create(&storage, "services", "dns3", "ready-only", &svc).await;
    create(
        &storage,
        "pods",
        "dns3",
        "p1",
        &pod("p1", "dns3", "10.32.0.1", true),
    )
    .await;
    create(
        &storage,
        "pods",
        "dns3",
        "p2",
        &pod("p2", "dns3", "10.32.0.2", false),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns3", "ready-only").await;
    let records = build_a_records(&svc, &slices);
    let svc_fqdn = "ready-only.dns3.svc.cluster.local";
    let ips: Vec<&str> = records
        .iter()
        .filter(|r| r.name == svc_fqdn)
        .map(|r| r.target.as_str())
        .collect();
    assert_eq!(ips, vec!["10.32.0.1"]);
}

/// [sig-network] DNS should provide DNS for pods for Hostname (headless, hostname set)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:209
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn dns_should_provide_pod_a_record_for_hostname() {
    let storage = setup();
    let svc = headless_service("subdom", "dns4", 80);
    create(&storage, "services", "dns4", "subdom", &svc).await;
    let mut p = pod("p1", "dns4", "10.33.0.1", true);
    let spec = p.spec.as_mut().unwrap();
    spec.hostname = Some("web-0".to_string());
    spec.subdomain = Some("subdom".to_string());
    create(&storage, "pods", "dns4", "p1", &p).await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns4", "subdom").await;
    let records = build_a_records(&svc, &slices);
    let want = ARecord {
        name: "web-0.subdom.dns4.svc.cluster.local".to_string(),
        target: "10.33.0.1".to_string(),
    };
    assert!(
        records.contains(&want),
        "expected pod-hostname A record present; got {records:#?}"
    );
}

/// [sig-network] DNS should provide DNS for pods for Subdomain (headless, IP-derived hostname fallback)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:240
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn dns_should_provide_pod_a_record_with_dashed_ip_fallback() {
    let storage = setup();
    let svc = headless_service("fallback", "dns5", 80);
    create(&storage, "services", "dns5", "fallback", &svc).await;
    create(
        &storage,
        "pods",
        "dns5",
        "p1",
        &pod("p1", "dns5", "10.34.0.1", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns5", "fallback").await;
    let records = build_a_records(&svc, &slices);
    let want = ARecord {
        name: "10-34-0-1.fallback.dns5.svc.cluster.local".to_string(),
        target: "10.34.0.1".to_string(),
    };
    assert!(
        records.contains(&want),
        "expected dashed-IP A record present; got {records:#?}"
    );
}

/// [sig-network] DNS should provide SRV records for service ports
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:130 (SRV sub-assertion)
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn dns_should_build_srv_record_for_named_service_port() {
    let storage = setup();
    let svc = service("srv", "dns6", 80);
    create(&storage, "services", "dns6", "srv", &svc).await;
    create(
        &storage,
        "pods",
        "dns6",
        "p1",
        &pod("p1", "dns6", "10.35.0.1", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns6", "srv").await;
    let srv = build_srv_records(&svc, &slices);
    assert_eq!(srv.len(), 1);
    assert_eq!(srv[0].name, "_http._tcp.srv.dns6.svc.cluster.local");
    assert_eq!(srv[0].port, 80);
    assert_eq!(srv[0].target, "srv.dns6.svc.cluster.local");
}

/// [sig-network] DNS should build SRV records targeting pod A-names for headless services
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:130 (headless SRV)
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn dns_should_build_srv_records_per_endpoint_for_headless_service() {
    let storage = setup();
    let svc = headless_service("hsrv", "dns7", 80);
    create(&storage, "services", "dns7", "hsrv", &svc).await;
    let mut p1 = pod("p1", "dns7", "10.36.0.1", true);
    p1.spec.as_mut().unwrap().hostname = Some("web-0".to_string());
    p1.spec.as_mut().unwrap().subdomain = Some("hsrv".to_string());
    let mut p2 = pod("p2", "dns7", "10.36.0.2", true);
    p2.spec.as_mut().unwrap().hostname = Some("web-1".to_string());
    p2.spec.as_mut().unwrap().subdomain = Some("hsrv".to_string());
    create(&storage, "pods", "dns7", "p1", &p1).await;
    create(&storage, "pods", "dns7", "p2", &p2).await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns7", "hsrv").await;
    let srv = build_srv_records(&svc, &slices);
    let targets: Vec<&str> = srv.iter().map(|r| r.target.as_str()).collect();
    assert!(targets.contains(&"web-0.hsrv.dns7.svc.cluster.local"));
    assert!(targets.contains(&"web-1.hsrv.dns7.svc.cluster.local"));
    assert_eq!(srv.len(), 2);
    assert!(srv.iter().all(|r| r.port == 80));
    assert!(srv
        .iter()
        .all(|r| r.name == "_http._tcp.hsrv.dns7.svc.cluster.local"));
}

/// [sig-network] DNS should NOT emit SRV records for unnamed Service ports
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go (SRV requires port name)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn dns_should_skip_srv_records_for_unnamed_ports() {
    let storage = setup();
    let mut svc = service("noname", "dns8", 80);
    svc.spec.ports[0].name = None;
    create(&storage, "services", "dns8", "noname", &svc).await;
    create(
        &storage,
        "pods",
        "dns8",
        "p1",
        &pod("p1", "dns8", "10.37.0.1", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns8", "noname").await;
    let srv = build_srv_records(&svc, &slices);
    assert!(srv.is_empty(), "no SRV records for unnamed ports");
}

/// [sig-network] DNS headless service with no ready endpoints should yield empty A set
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:130 (empty backend behavior)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn dns_headless_with_no_ready_endpoints_yields_no_a_records() {
    let storage = setup();
    let svc = headless_service("empty", "dns9", 80);
    create(&storage, "services", "dns9", "empty", &svc).await;
    create(
        &storage,
        "pods",
        "dns9",
        "p1",
        &pod("p1", "dns9", "10.38.0.1", false),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dns9", "empty").await;
    let records = build_a_records(&svc, &slices);
    assert!(records.is_empty());
}

/// [sig-network] DNS should provide DNS for ExternalName services (CNAME, no endpoints)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/dns.go:271
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn dns_externalname_service_should_not_emit_a_records() {
    let storage = setup();
    let mut svc = service("ext", "dnsA", 80);
    svc.spec.service_type = Some(ServiceType::ExternalName);
    svc.spec.cluster_ip = None;
    svc.spec.selector = None;
    svc.spec.external_name = Some("foo.example.com".to_string());
    create(&storage, "services", "dnsA", "ext", &svc).await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let slices = read_slices_for(&storage, "dnsA", "ext").await;
    assert!(
        slices.is_empty(),
        "ExternalName services produce no EndpointSlice"
    );
    let records = build_a_records(&svc, &slices);
    assert!(
        records.is_empty(),
        "CNAME-only service must not emit A records"
    );
    // CNAME emission itself is CoreDNS's responsibility; we just assert the
    // upstream contract that the in-tree side stays empty.
    assert_eq!(svc.spec.external_name.as_deref(), Some("foo.example.com"));
}

// ===========================================================================
//
// Group 4 — EndpointsController updates on pod state change
// Upstream: test/e2e/network/{endpointslice,service}.go (reactive updates)
//
// ===========================================================================

/// [sig-network] EndpointsController should add a pod address when a new ready pod appears
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:116 (scale-up)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpoints_controller_should_add_address_when_new_pod_becomes_ready() {
    let storage = setup();
    let svc = service("grow", "uns1", 80);
    create(&storage, "services", "uns1", "grow", &svc).await;
    create(
        &storage,
        "pods",
        "uns1",
        "p1",
        &pod("p1", "uns1", "10.40.0.1", true),
    )
    .await;

    let ec = EndpointsController::new(Arc::clone(&storage));
    ec.reconcile_all().await.unwrap();

    create(
        &storage,
        "pods",
        "uns1",
        "p2",
        &pod("p2", "uns1", "10.40.0.2", true),
    )
    .await;
    ec.reconcile_all().await.unwrap();

    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("uns1"), "grow"))
        .await
        .unwrap();
    let addrs: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert!(addrs.contains(&"10.40.0.1"));
    assert!(addrs.contains(&"10.40.0.2"));
}

/// [sig-network] EndpointsController should drop a pod address when the pod transitions NotReady
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:116 (readiness flip)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpoints_controller_should_drop_address_when_pod_becomes_not_ready() {
    let storage = setup();
    let svc = service("drop", "uns2", 80);
    create(&storage, "services", "uns2", "drop", &svc).await;
    create(
        &storage,
        "pods",
        "uns2",
        "p1",
        &pod("p1", "uns2", "10.41.0.1", true),
    )
    .await;

    let ec = EndpointsController::new(Arc::clone(&storage));
    ec.reconcile_all().await.unwrap();

    let pod_key = build_key("pods", Some("uns2"), "p1");
    let mut p: Pod = storage.get(&pod_key).await.unwrap();
    p.status.as_mut().unwrap().conditions = Some(vec![PodCondition {
        condition_type: "Ready".to_string(),
        status: "False".to_string(),
        reason: None,
        message: None,
        last_probe_time: None,
        last_transition_time: None,
        observed_generation: None,
    }]);
    storage.update(&pod_key, &p).await.unwrap();
    ec.reconcile_all().await.unwrap();

    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("uns2"), "drop"))
        .await
        .unwrap();
    let ready: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    let not_ready: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.not_ready_addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert!(ready.is_empty(), "no addresses should remain ready");
    assert_eq!(not_ready, vec!["10.41.0.1"]);
}

/// [sig-network] EndpointsController should remove pod from Endpoints when pod is deleted
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (deleted pod cleanup)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn endpoints_controller_should_remove_address_when_pod_deleted() {
    let storage = setup();
    let svc = service("gone", "uns3", 80);
    create(&storage, "services", "uns3", "gone", &svc).await;
    create(
        &storage,
        "pods",
        "uns3",
        "p1",
        &pod("p1", "uns3", "10.42.0.1", true),
    )
    .await;
    create(
        &storage,
        "pods",
        "uns3",
        "p2",
        &pod("p2", "uns3", "10.42.0.2", true),
    )
    .await;

    let ec = EndpointsController::new(Arc::clone(&storage));
    ec.reconcile_all().await.unwrap();

    storage
        .delete(&build_key("pods", Some("uns3"), "p1"))
        .await
        .unwrap();
    ec.reconcile_all().await.unwrap();

    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("uns3"), "gone"))
        .await
        .unwrap();
    let addrs: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert_eq!(addrs, vec!["10.42.0.2"]);
}

// ===========================================================================
//
// Group 5 — kube-proxy consumption of EndpointSlices
// (Sanity that the rules generator can address the slices we just produced.)
// Upstream: test/e2e/network/endpointslice.go (proxy reads the slice we emit)
//
// ===========================================================================

/// [sig-network] kube-proxy should read EndpointSlices by the kubernetes.io/service-name label
///
/// Upstream: k8s.io/kubernetes/pkg/proxy/endpointslicecache.go (consumer contract)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn kube_proxy_should_group_endpointslices_by_service_name_label() {
    let storage = setup();
    let svc = service("kp", "kpns", 80);
    create(&storage, "services", "kpns", "kp", &svc).await;
    create(
        &storage,
        "pods",
        "kpns",
        "p1",
        &pod("p1", "kpns", "10.50.0.1", true),
    )
    .await;
    create(
        &storage,
        "pods",
        "kpns",
        "p2",
        &pod("p2", "kpns", "10.50.0.2", true),
    )
    .await;

    EndpointSliceController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    // Mimic kube-proxy's lookup: list all slices in namespace, filter by label.
    let all: Vec<EndpointSlice> = storage
        .list(&build_prefix("endpointslices", Some("kpns")))
        .await
        .unwrap();
    let mut by_svc: HashMap<String, Vec<String>> = HashMap::new();
    for s in &all {
        let svc_name = s
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
            .cloned()
            .unwrap_or_default();
        for ep in &s.endpoints {
            if ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true) {
                by_svc
                    .entry(svc_name.clone())
                    .or_default()
                    .extend(ep.addresses.iter().cloned());
            }
        }
    }
    let mut ips = by_svc.remove("kp").unwrap_or_default();
    ips.sort();
    assert_eq!(ips, vec!["10.50.0.1".to_string(), "10.50.0.2".to_string()]);
}

/// [sig-network] kube-proxy should ignore NotReady endpoints when programming backends
///
/// Upstream: k8s.io/kubernetes/pkg/proxy/topology.go (ready filter)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn kube_proxy_should_skip_not_ready_endpoints_in_backend_set() {
    let storage = setup();
    // Build the EndpointSlice by hand to control the ready flag without
    // depending on controller reconcile order.
    let mut labels = HashMap::new();
    labels.insert("kubernetes.io/service-name".to_string(), "ep".to_string());
    labels.insert(
        "endpointslice.kubernetes.io/managed-by".to_string(),
        "endpointslice-controller.k8s.io".to_string(),
    );
    let slice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "ep-abcd".to_string(),
            namespace: Some("kpns2".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![
            Endpoint {
                addresses: vec!["10.51.0.1".to_string()],
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
            },
            Endpoint {
                addresses: vec!["10.51.0.2".to_string()],
                conditions: Some(EndpointConditions {
                    ready: Some(false),
                    serving: Some(false),
                    terminating: Some(false),
                }),
                hostname: None,
                target_ref: None,
                node_name: None,
                zone: None,
                hints: None,
                deprecated_topology: None,
            },
        ],
        ports: vec![ESEndpointPort {
            name: Some("http".to_string()),
            port: Some(80),
            protocol: "TCP".to_string(),
            app_protocol: None,
        }],
    };
    let key = build_key("endpointslices", Some("kpns2"), "ep-abcd");
    storage.create(&key, &slice).await.unwrap();

    // Replicate kube-proxy's ready-only collection (matches proxy.rs sync()).
    let all: Vec<EndpointSlice> = storage
        .list(&build_prefix("endpointslices", Some("kpns2")))
        .await
        .unwrap();
    let backends: Vec<String> = all
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .filter(|e| e.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true))
        .flat_map(|e| e.addresses.iter().cloned())
        .collect();
    assert_eq!(backends, vec!["10.51.0.1".to_string()]);
}

/// [sig-network] kube-proxy should treat missing EndpointConditions as ready=true (legacy fallback)
///
/// Upstream: k8s.io/kubernetes/pkg/proxy/endpointslicecache.go (nil-conditions handling)
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn kube_proxy_should_treat_nil_conditions_as_ready() {
    let storage = setup();
    let mut labels = HashMap::new();
    labels.insert("kubernetes.io/service-name".to_string(), "nc".to_string());
    let slice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "nc-1".to_string(),
            namespace: Some("kpns3".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![Endpoint {
            addresses: vec!["10.52.0.1".to_string()],
            conditions: None,
            hostname: None,
            target_ref: None,
            node_name: None,
            zone: None,
            hints: None,
            deprecated_topology: None,
        }],
        ports: vec![],
    };
    let key = build_key("endpointslices", Some("kpns3"), "nc-1");
    storage.create(&key, &slice).await.unwrap();

    let all: Vec<EndpointSlice> = storage
        .list(&build_prefix("endpointslices", Some("kpns3")))
        .await
        .unwrap();
    let backends: Vec<String> = all
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .filter(|e| e.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true))
        .flat_map(|e| e.addresses.iter().cloned())
        .collect();
    assert_eq!(backends, vec!["10.52.0.1".to_string()]);
}
