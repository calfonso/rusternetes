//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-network] Services — controller-manager half.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/network/
//! (`service.go`, `endpointslicemirroring.go`)
//!
//! These tests drive the reconcile loops of:
//!   - `ServiceController`  — ClusterIP/NodePort/ExternalName type transitions
//!   - `EndpointsController` — basic endpoint lifecycle from pods
//!   - `EndpointSliceController` — EndpointSliceMirroring (custom Endpoints)
//!
//! against `Arc<MemoryStorage>`. No HTTP harness, no Docker, no etcd.
//!
//! Tests in the "failing" bucket that need a live network (connectivity probes)
//! are stubs marked `#[ignore = "GAP: needs live network"]`.

use rusternetes_common::resources::{
    Container, ContainerPort, EndpointAddress, EndpointPort as EPPort, EndpointSubset, Endpoints,
    IntOrString, Pod, PodCondition, PodSpec, PodStatus, Service, ServicePort, ServiceSpec,
    ServiceType,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::endpoints::EndpointsController;
use rusternetes_controller_manager::controllers::endpointslice::EndpointSliceController;
use rusternetes_controller_manager::controllers::service::ServiceController;
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn pod_labels() -> HashMap<String, String> {
    let mut l = HashMap::new();
    l.insert("app".to_string(), "echo".to_string());
    l
}

fn ready_pod(name: &str, ns: &str, ip: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name)
            .with_namespace(ns)
            .with_labels(pod_labels()),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "registry.k8s.io/e2e-test-images/agnhost:2.55".to_string(),
                ports: Some(vec![ContainerPort {
                    container_port: 8080,
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
                status: "True".to_string(),
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

fn clusterip_service(name: &str, ns: &str) -> Service {
    let mut sel = HashMap::new();
    sel.insert("app".to_string(), "echo".to_string());
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(ns),
        spec: ServiceSpec {
            selector: Some(sel),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: None,
            ..ServiceSpec::default()
        },
        status: None,
    }
}

fn externalname_service(name: &str, ns: &str, external_name: &str) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(ns),
        spec: ServiceSpec {
            selector: None,
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ExternalName),
            cluster_ip: None,
            external_name: Some(external_name.to_string()),
            ..ServiceSpec::default()
        },
        status: None,
    }
}

fn nodeport_service(name: &str, ns: &str) -> Service {
    let mut sel = HashMap::new();
    sel.insert("app".to_string(), "echo".to_string());
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(ns),
        spec: ServiceSpec {
            selector: Some(sel),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                protocol: "TCP".to_string(),
                node_port: None, // allocated by controller
                app_protocol: None,
            }],
            service_type: Some(ServiceType::NodePort),
            cluster_ip: None, // allocated by controller
            ..ServiceSpec::default()
        },
        status: None,
    }
}

async fn create_service(storage: &Arc<MemoryStorage>, svc: &Service) {
    let ns = svc.metadata.namespace.as_deref().unwrap_or("default");
    let key = build_key("services", Some(ns), &svc.metadata.name);
    storage.create(&key, svc).await.unwrap();
}

async fn create_pod(storage: &Arc<MemoryStorage>, pod: &Pod) {
    let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
    let key = build_key("pods", Some(ns), &pod.metadata.name);
    storage.create(&key, pod).await.unwrap();
}

async fn get_service(storage: &Arc<MemoryStorage>, ns: &str, name: &str) -> Service {
    let key = build_key("services", Some(ns), name);
    storage.get(&key).await.unwrap()
}

// ---------------------------------------------------------------------------
// Group 1 — Service type transitions
// Upstream: test/e2e/network/service.go
// ---------------------------------------------------------------------------

/// [sig-network] Services should be able to change the type from ExternalName
/// to NodePort [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1896
/// Sonobuoy (Round 160): FAIL — in failing.txt
///
/// Controller-manager half: when a Service's `.spec.type` is changed from
/// ExternalName to NodePort the ServiceController must allocate a ClusterIP
/// AND a NodePort on the next reconcile. ExternalName services never have
/// a ClusterIP so the allocation must happen from scratch.
#[tokio::test]
async fn services_change_type_externalname_to_nodeport() {
    let storage = setup();

    // Phase 1: create as ExternalName.
    let ext = externalname_service("type-change-ext-np", "ns-tc1", "example.com");
    create_service(&storage, &ext).await;

    let ctrl = ServiceController::new(Arc::clone(&storage));
    ctrl.initialize().await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    // ExternalName must not receive a ClusterIP.
    let after_ext = get_service(&storage, "ns-tc1", "type-change-ext-np").await;
    assert!(
        after_ext.spec.cluster_ip.is_none()
            || after_ext
                .spec
                .cluster_ip
                .as_deref()
                .map(|s| s.is_empty())
                .unwrap_or(false),
        "ExternalName must not have ClusterIP allocated: {:?}",
        after_ext.spec.cluster_ip
    );

    // Phase 2: change type to NodePort and re-reconcile.
    let mut np = after_ext.clone();
    np.spec.service_type = Some(ServiceType::NodePort);
    np.spec.external_name = None;
    np.spec.selector = Some({
        let mut s = HashMap::new();
        s.insert("app".to_string(), "echo".to_string());
        s
    });
    let key = build_key("services", Some("ns-tc1"), "type-change-ext-np");
    storage.update(&key, &np).await.unwrap();

    ctrl.reconcile_all().await.unwrap();

    let after_np = get_service(&storage, "ns-tc1", "type-change-ext-np").await;
    // After transitioning to NodePort the controller must allocate a ClusterIP.
    assert!(
        after_np.spec.cluster_ip.is_some()
            && !after_np.spec.cluster_ip.as_deref().unwrap_or("").is_empty(),
        "NodePort service must have a ClusterIP after type change: {:?}",
        after_np.spec.cluster_ip
    );
    // And at least one port must have a NodePort assigned.
    let has_nodeport = after_np.spec.ports.iter().any(|p| p.node_port.is_some());
    assert!(
        has_nodeport,
        "NodePort service must have node_port allocated after type change"
    );
}

/// [sig-network] Services should be able to change the type from ClusterIP to
/// ExternalName [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1895
/// Sonobuoy (Round 160): PASS (newly-passing.txt)
///
/// Transitioning from ClusterIP to ExternalName: the externalName field must
/// be set and the service must NOT be treated as NodePort/LB.
#[tokio::test]
async fn services_change_type_clusterip_to_externalname() {
    let storage = setup();

    // Phase 1: ClusterIP service.
    let svc = clusterip_service("type-change-cip-ext", "ns-tc2");
    create_service(&storage, &svc).await;

    let ctrl = ServiceController::new(Arc::clone(&storage));
    ctrl.initialize().await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let after_cip = get_service(&storage, "ns-tc2", "type-change-cip-ext").await;
    let allocated_ip = after_cip.spec.cluster_ip.clone().unwrap_or_default();
    assert!(!allocated_ip.is_empty(), "ClusterIP must be allocated");

    // Phase 2: update to ExternalName.
    let mut ext = after_cip.clone();
    ext.spec.service_type = Some(ServiceType::ExternalName);
    ext.spec.external_name = Some("example.com".to_string());
    ext.spec.selector = None;
    let key = build_key("services", Some("ns-tc2"), "type-change-cip-ext");
    storage.update(&key, &ext).await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let after_ext = get_service(&storage, "ns-tc2", "type-change-cip-ext").await;
    // ExternalName field must be present.
    assert_eq!(
        after_ext.spec.external_name.as_deref(),
        Some("example.com"),
        "externalName must be set"
    );
    // No NodePorts expected.
    let has_nodeport = after_ext.spec.ports.iter().any(|p| p.node_port.is_some());
    assert!(
        !has_nodeport,
        "ExternalName service must not have NodePorts"
    );
}

/// [sig-network] Services should be able to change the type from ExternalName
/// to ClusterIP [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1928
/// Sonobuoy (Round 160): PASS (newly-passing.txt)
///
/// When changing ExternalName to ClusterIP the controller must allocate a
/// ClusterIP on the next reconcile and must NOT allocate a NodePort.
#[tokio::test]
async fn services_change_type_externalname_to_clusterip() {
    let storage = setup();

    let ext = externalname_service("type-change-ext-cip", "ns-tc3", "example.com");
    create_service(&storage, &ext).await;

    let ctrl = ServiceController::new(Arc::clone(&storage));
    ctrl.initialize().await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    // Change to ClusterIP.
    let mut cip = ext.clone();
    cip.spec.service_type = Some(ServiceType::ClusterIP);
    cip.spec.external_name = None;
    cip.spec.selector = Some({
        let mut s = HashMap::new();
        s.insert("app".to_string(), "echo".to_string());
        s
    });
    let key = build_key("services", Some("ns-tc3"), "type-change-ext-cip");
    storage.update(&key, &cip).await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let after = get_service(&storage, "ns-tc3", "type-change-ext-cip").await;
    assert!(
        after.spec.cluster_ip.is_some()
            && !after.spec.cluster_ip.as_deref().unwrap_or("").is_empty(),
        "ClusterIP must be allocated after ExternalName to ClusterIP transition"
    );
    let has_nodeport = after.spec.ports.iter().any(|p| p.node_port.is_some());
    assert!(
        !has_nodeport,
        "ClusterIP service must not have NodePorts after transition"
    );
}

/// [sig-network] Services should be able to change the type from NodePort to
/// ExternalName [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1934
/// Sonobuoy (Round 160): PASS (newly-passing.txt)
///
/// When changing NodePort to ExternalName the ServiceController must release
/// the NodePort allocations (clear node_port from ports) and set externalName.
#[tokio::test]
async fn services_change_type_nodeport_to_externalname() {
    let storage = setup();

    let np = nodeport_service("type-change-np-ext", "ns-tc4");
    create_service(&storage, &np).await;

    let ctrl = ServiceController::new(Arc::clone(&storage));
    ctrl.initialize().await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let after_np = get_service(&storage, "ns-tc4", "type-change-np-ext").await;
    let has_nodeport = after_np.spec.ports.iter().any(|p| p.node_port.is_some());
    assert!(
        has_nodeport,
        "NodePort service must have node_port allocated initially"
    );

    // Change to ExternalName.
    let mut ext = after_np.clone();
    ext.spec.service_type = Some(ServiceType::ExternalName);
    ext.spec.external_name = Some("example.com".to_string());
    ext.spec.cluster_ip = None;
    ext.spec.selector = None;
    for p in ext.spec.ports.iter_mut() {
        p.node_port = None;
    }
    let key = build_key("services", Some("ns-tc4"), "type-change-np-ext");
    storage.update(&key, &ext).await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let after_ext = get_service(&storage, "ns-tc4", "type-change-np-ext").await;
    assert_eq!(
        after_ext.spec.external_name.as_deref(),
        Some("example.com"),
        "externalName must be set after NodePort to ExternalName"
    );
    // No NodePorts after downgrade.
    let still_has_np = after_ext.spec.ports.iter().any(|p| p.node_port.is_some());
    assert!(
        !still_has_np,
        "ExternalName service must not retain NodePorts after type change"
    );
}

// ---------------------------------------------------------------------------
// Group 2 — Service status lifecycle (controller-manager half)
// Upstream: test/e2e/network/service.go:3246
// ---------------------------------------------------------------------------

/// [sig-network] Services should complete a service status lifecycle
/// [Conformance] — controller-manager view
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:3246
/// Sonobuoy (Round 160): FAIL (failing.txt) — full lifecycle including LB
/// status population. The kube-proxy half of this test is in
/// `crates/kube-proxy/tests/conformance_network_services_proxy.rs`.
///
/// This fragment validates the ServiceController-level contract: a NodePort
/// Service that receives a ClusterIP and NodePort during reconcile
/// subsequently produces the correct `.spec` shape.
#[tokio::test]
async fn services_service_status_lifecycle_spec_shape() {
    let storage = setup();

    let np = nodeport_service("lifecycle-svc", "ns-sl1");
    create_service(&storage, &np).await;

    let ctrl = ServiceController::new(Arc::clone(&storage));
    ctrl.initialize().await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let svc = get_service(&storage, "ns-sl1", "lifecycle-svc").await;

    // ClusterIP must be allocated.
    let ip = svc.spec.cluster_ip.as_deref().unwrap_or("");
    assert!(!ip.is_empty(), "status lifecycle: ClusterIP must be set");
    assert!(
        ip.starts_with("10."),
        "status lifecycle: ClusterIP must be from 10.x.x.x range, got {}",
        ip
    );

    // NodePort must be in the valid range.
    let port = svc
        .spec
        .ports
        .first()
        .and_then(|p| p.node_port)
        .expect("status lifecycle: NodePort must be allocated");
    assert!(
        (30000..=32767).contains(&port),
        "status lifecycle: NodePort {} not in 30000-32767",
        port
    );
}

// ---------------------------------------------------------------------------
// Group 3 — Endpoints serve basic endpoint from pods
// Upstream: test/e2e/network/service.go
// ---------------------------------------------------------------------------

/// [sig-network] Services should serve a basic endpoint from pods [Conformance]
/// — controller-manager view
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1039
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// The EndpointsController must populate Endpoints.subsets with the ready
/// pod's address when the pod's labels match the Service's selector.
#[tokio::test]
async fn services_serve_basic_endpoint_from_pods() {
    let storage = setup();

    let svc = clusterip_service("basic-ep", "ns-ep1");
    create_service(&storage, &svc).await;
    create_pod(&storage, &ready_pod("p1", "ns-ep1", "10.60.0.1")).await;

    EndpointsController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("ns-ep1"), "basic-ep"))
        .await
        .expect("Endpoints object must be created");

    let addrs: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert!(
        addrs.contains(&"10.60.0.1"),
        "basic endpoint: pod IP must be in ready addresses: {:?}",
        addrs
    );
}

/// [sig-network] EndpointsController should create and delete Endpoints for a
/// Service with a selector that matches no pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (no-match selector)
/// Sonobuoy (Round 160): PASS (newly-passing.txt)
///
/// An Endpoints object must be created even when no pod matches, with empty
/// subsets (not missing entirely).
#[tokio::test]
async fn endpoints_controller_create_empty_endpoints_when_selector_matches_no_pods() {
    let storage = setup();

    let svc = clusterip_service("no-match-ep", "ns-ep2");
    create_service(&storage, &svc).await;
    // No pods with matching labels.

    EndpointsController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    // Endpoints must exist, even if empty.
    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("ns-ep2"), "no-match-ep"))
        .await
        .expect("Endpoints object must exist for selector-with-no-matches");

    let ready_count: usize = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .map(|a| a.len())
        .sum();
    assert_eq!(
        ready_count, 0,
        "no-match selector: ready address list must be empty"
    );
}

/// [sig-network] EndpointsController should create Endpoints for Pods
/// matching a Service [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (matching pods)
/// Sonobuoy (Round 160): PASS (newly-passing.txt)
///
/// Two ready pods that match the selector must both appear in Endpoints.
#[tokio::test]
async fn endpoints_controller_creates_endpoints_for_matching_pods() {
    let storage = setup();

    let svc = clusterip_service("match-ep", "ns-ep3");
    create_service(&storage, &svc).await;
    create_pod(&storage, &ready_pod("p1", "ns-ep3", "10.61.0.1")).await;
    create_pod(&storage, &ready_pod("p2", "ns-ep3", "10.61.0.2")).await;

    EndpointsController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("ns-ep3"), "match-ep"))
        .await
        .unwrap();
    let addrs: Vec<&str> = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert!(addrs.contains(&"10.61.0.1"), "pod1 missing: {:?}", addrs);
    assert!(addrs.contains(&"10.61.0.2"), "pod2 missing: {:?}", addrs);
}

// ---------------------------------------------------------------------------
// Group 4 — Endpoints lifecycle test
// Upstream: test/e2e/network/endpoints.go
// ---------------------------------------------------------------------------

/// [sig-network] Endpoints should test the lifecycle of an Endpoint [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpoints.go:40
/// Sonobuoy (Round 160): PASS (newly-passing.txt)
///
/// Create then verify then scale to zero then verify empty then scale back
/// then verify. This mirrors the upstream lifecycle: pod added, removed,
/// re-added.
#[tokio::test]
async fn endpoints_lifecycle_create_delete_recreate() {
    let storage = setup();
    let svc = clusterip_service("ep-lifecycle", "ns-lc1");
    create_service(&storage, &svc).await;
    create_pod(&storage, &ready_pod("p1", "ns-lc1", "10.62.0.1")).await;

    let ec = EndpointsController::new(Arc::clone(&storage));

    // Step 1: initial reconcile — endpoint must appear.
    ec.reconcile_all().await.unwrap();
    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("ns-lc1"), "ep-lifecycle"))
        .await
        .unwrap();
    let addrs: Vec<_> = ep
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert_eq!(addrs, vec!["10.62.0.1"], "step 1: pod must appear");

    // Step 2: delete pod — endpoint must become empty.
    storage
        .delete(&build_key("pods", Some("ns-lc1"), "p1"))
        .await
        .unwrap();
    ec.reconcile_all().await.unwrap();
    let ep2: Endpoints = storage
        .get(&build_key("endpoints", Some("ns-lc1"), "ep-lifecycle"))
        .await
        .unwrap();
    let ready2: usize = ep2
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .map(|a| a.len())
        .sum();
    assert_eq!(ready2, 0, "step 2: endpoint must be empty after pod delete");

    // Step 3: create a replacement pod — endpoint must reappear.
    create_pod(&storage, &ready_pod("p2", "ns-lc1", "10.62.0.2")).await;
    ec.reconcile_all().await.unwrap();
    let ep3: Endpoints = storage
        .get(&build_key("endpoints", Some("ns-lc1"), "ep-lifecycle"))
        .await
        .unwrap();
    let addrs3: Vec<_> = ep3
        .subsets
        .iter()
        .filter_map(|s| s.addresses.as_ref())
        .flat_map(|a| a.iter().map(|x| x.ip.as_str()))
        .collect();
    assert_eq!(addrs3, vec!["10.62.0.2"], "step 3: replacement pod IP");
}

// ---------------------------------------------------------------------------
// Group 5 — EndpointSliceMirroring (custom Endpoints through create/update/delete)
// Upstream: test/e2e/network/endpointslicemirroring.go
// ---------------------------------------------------------------------------

/// [sig-network] EndpointSliceMirroring should mirror a custom Endpoints
/// resource through create update and delete [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslicemirroring.go:50
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// Phase 1: user creates a custom Endpoints for a Service with no selector.
///   EndpointSliceController must mirror it with the managed-by mirroring
///   label.
/// Phase 2: user changes the Endpoints IP. Mirror must reflect the new IP on
///   next reconcile.
/// Phase 3: user deletes the Endpoints. The mirror EndpointSlice must vanish.
#[tokio::test]
async fn endpointslicemirroring_create_update_delete() {
    let storage = setup();

    // Service without a selector (user-managed Endpoints).
    let mut svc = clusterip_service("mirror-svc", "ns-mir1");
    svc.spec.selector = None;
    create_service(&storage, &svc).await;

    let ctrl = EndpointSliceController::new(Arc::clone(&storage));

    let make_ep = |ip: &str| -> Endpoints {
        Endpoints {
            type_meta: TypeMeta {
                kind: "Endpoints".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("mirror-svc").with_namespace("ns-mir1"),
            subsets: vec![EndpointSubset {
                addresses: Some(vec![EndpointAddress {
                    ip: ip.to_string(),
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
        }
    };

    let ep_key = build_key("endpoints", Some("ns-mir1"), "mirror-svc");

    // ---- Phase 1: create ----
    storage
        .create(&ep_key, &make_ep("192.168.10.1"))
        .await
        .unwrap();
    ctrl.reconcile_all().await.unwrap();

    let slices_prefix = build_prefix("endpointslices", Some("ns-mir1"));
    let slices: Vec<rusternetes_common::resources::EndpointSlice> =
        storage.list(&slices_prefix).await.unwrap();
    let mirror: Vec<_> = slices
        .iter()
        .filter(|s| {
            s.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|n| n == "mirror-svc")
                .unwrap_or(false)
        })
        .collect();
    assert!(!mirror.is_empty(), "phase 1: mirror slice must be created");
    let managed_by = mirror[0]
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"));
    assert_eq!(
        managed_by,
        Some(&"endpointslice-mirroring-controller.k8s.io".to_string()),
        "phase 1: mirror slice must carry managed-by mirroring label"
    );
    let ips: Vec<_> = mirror
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .flat_map(|e| e.addresses.iter().map(|a| a.as_str()))
        .collect();
    assert!(
        ips.contains(&"192.168.10.1"),
        "phase 1: original IP must appear in mirror: {:?}",
        ips
    );

    // ---- Phase 2: update (change the endpoint IP) ----
    storage
        .update(&ep_key, &make_ep("192.168.10.2"))
        .await
        .unwrap();
    ctrl.reconcile_all().await.unwrap();

    let slices2: Vec<rusternetes_common::resources::EndpointSlice> =
        storage.list(&slices_prefix).await.unwrap();
    let ips2: Vec<String> = slices2
        .iter()
        .filter(|s| {
            s.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|n| n == "mirror-svc")
                .unwrap_or(false)
        })
        .flat_map(|s| s.endpoints.iter())
        .flat_map(|e| e.addresses.iter().cloned())
        .collect();
    assert!(
        ips2.contains(&"192.168.10.2".to_string()),
        "phase 2: updated IP must appear in mirror: {:?}",
        ips2
    );
    assert!(
        !ips2.contains(&"192.168.10.1".to_string()),
        "phase 2: old IP must not appear in mirror after update: {:?}",
        ips2
    );

    // ---- Phase 3: delete ----
    storage.delete(&ep_key).await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let slices3: Vec<rusternetes_common::resources::EndpointSlice> =
        storage.list(&slices_prefix).await.unwrap();
    let remaining: Vec<_> = slices3
        .iter()
        .filter(|s| {
            s.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|n| n == "mirror-svc")
                .unwrap_or(false)
        })
        .collect();
    assert!(
        remaining.is_empty(),
        "phase 3: mirror slice must be deleted when Endpoints is deleted: {:?}",
        remaining
    );
}

// ---------------------------------------------------------------------------
// Group 6 — Services serve endpoints on same port different protocols
// Upstream: test/e2e/network/service.go
// ---------------------------------------------------------------------------

/// [sig-network] Services should serve endpoints on same port and different
/// protocols [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:2398
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// A Service with two ports of the same port number but different protocols
/// (TCP and UDP) must receive both ports in Endpoints. The controller must
/// NOT deduplicate them — they are distinct L4 service entries.
#[tokio::test]
async fn services_serve_endpoints_same_port_different_protocols() {
    let storage = setup();

    let mut sel = HashMap::new();
    sel.insert("app".to_string(), "echo".to_string());
    let svc = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("dual-proto").with_namespace("ns-dp1"),
        spec: ServiceSpec {
            selector: Some(sel),
            ports: vec![
                ServicePort {
                    name: Some("tcp-http".to_string()),
                    port: 80,
                    target_port: Some(IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                },
                ServicePort {
                    name: Some("udp-http".to_string()),
                    port: 80,
                    target_port: Some(IntOrString::Int(8080)),
                    protocol: "UDP".to_string(),
                    node_port: None,
                    app_protocol: None,
                },
            ],
            service_type: Some(ServiceType::ClusterIP),
            cluster_ip: None,
            ..ServiceSpec::default()
        },
        status: None,
    };
    create_service(&storage, &svc).await;
    create_pod(&storage, &ready_pod("p1", "ns-dp1", "10.63.0.1")).await;

    EndpointsController::new(Arc::clone(&storage))
        .reconcile_all()
        .await
        .unwrap();

    let ep: Endpoints = storage
        .get(&build_key("endpoints", Some("ns-dp1"), "dual-proto"))
        .await
        .expect("Endpoints must be created for dual-protocol service");

    let protocols: Vec<String> = ep
        .subsets
        .iter()
        .filter_map(|s| s.ports.as_ref())
        .flat_map(|ports| ports.iter().map(|p| p.protocol.clone()))
        .collect();
    assert!(
        protocols.iter().any(|p| p == "TCP"),
        "dual-protocol: TCP must be present in Endpoints ports: {:?}",
        protocols
    );
    assert!(
        protocols.iter().any(|p| p == "UDP"),
        "dual-protocol: UDP must be present in Endpoints ports: {:?}",
        protocols
    );
}

// ---------------------------------------------------------------------------
// Group 7 — Service find/list/delete operations
// Upstream: test/e2e/network/service.go
// ---------------------------------------------------------------------------

/// [sig-network] Services should find a service from listing all namespaces
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (list all ns)
/// Sonobuoy (Round 160): PASS (newly-passing.txt)
///
/// When services exist in two different namespaces, listing all services
/// (cross-namespace) must return both.
#[tokio::test]
async fn services_find_from_listing_all_namespaces() {
    let storage = setup();

    let svc_a = clusterip_service("svc-a", "ns-list-a");
    let svc_b = clusterip_service("svc-b", "ns-list-b");
    create_service(&storage, &svc_a).await;
    create_service(&storage, &svc_b).await;

    let all: Vec<Service> = storage.list("/registry/services/").await.unwrap();
    let names: Vec<&str> = all.iter().map(|s| s.metadata.name.as_str()).collect();
    assert!(names.contains(&"svc-a"), "svc-a missing from cross-ns list");
    assert!(names.contains(&"svc-b"), "svc-b missing from cross-ns list");
}

/// [sig-network] Services should delete a collection of services [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go (collection delete)
/// Sonobuoy (Round 160): PASS (newly-passing.txt)
///
/// Creating three services and deleting them individually must leave an
/// empty list. The storage layer must not retain stale entries.
#[tokio::test]
async fn services_delete_collection() {
    let storage = setup();

    for i in 1..=3u8 {
        let name = format!("del-svc-{}", i);
        let svc = clusterip_service(&name, "ns-del1");
        create_service(&storage, &svc).await;
    }

    let before: Vec<Service> = storage
        .list(&build_prefix("services", Some("ns-del1")))
        .await
        .unwrap();
    assert_eq!(before.len(), 3, "expected 3 services before delete");

    for i in 1..=3u8 {
        let name = format!("del-svc-{}", i);
        let key = build_key("services", Some("ns-del1"), &name);
        storage.delete(&key).await.unwrap();
    }

    let after: Vec<Service> = storage
        .list(&build_prefix("services", Some("ns-del1")))
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "all services must be deleted: {:?}",
        after
    );
}

// ---------------------------------------------------------------------------
// Group 8 — NodePort: create a functioning NodePort service [Conformance]
// Upstream: test/e2e/network/service.go
// ---------------------------------------------------------------------------

/// [sig-network] Services should be able to create a functioning NodePort
/// service [Conformance] — controller-manager spec
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1687
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// Controller-manager half: ServiceController must allocate a NodePort in
/// 30000-32767 and a ClusterIP. Connectivity via the NodePort is a live-
/// network concern and is stubbed below.
#[tokio::test]
async fn services_create_functioning_nodeport_service_spec() {
    let storage = setup();

    let np = nodeport_service("functioning-np", "ns-fnp1");
    create_service(&storage, &np).await;

    let ctrl = ServiceController::new(Arc::clone(&storage));
    ctrl.initialize().await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let svc = get_service(&storage, "ns-fnp1", "functioning-np").await;

    let ip = svc.spec.cluster_ip.as_deref().unwrap_or("");
    assert!(!ip.is_empty(), "NodePort service must receive a ClusterIP");

    let np_val = svc
        .spec
        .ports
        .first()
        .and_then(|p| p.node_port)
        .expect("NodePort must be allocated");
    assert!(
        (30000..=32767).contains(&np_val),
        "NodePort {} not in valid range 30000-32767",
        np_val
    );
}

/// [sig-network] Services should be able to create a functioning NodePort
/// service — live connectivity (STUB)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:1687
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// The upstream test dials nodeIP:nodePort and verifies an HTTP/echo response
/// from the backend pod. That requires a real network stack. The kube-proxy
/// half of the iptables emission is tested in
/// crates/kube-proxy/tests/conformance_network_services_proxy.rs.
#[tokio::test]
#[ignore = "GAP: needs live network — NodePort connectivity from nodeIP:nodePort requires iptables + pod netns"]
async fn services_create_functioning_nodeport_service_connectivity() {}

// ---------------------------------------------------------------------------
// Group 9 — Session affinity (stub for connectivity portions)
// Upstream: test/e2e/network/service.go
// ---------------------------------------------------------------------------

/// [sig-network] Services should have session affinity work for NodePort
/// service [LinuxOnly] [Conformance] — live connectivity (STUB)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:2265
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// The iptables-rule emission side is tested in
/// crates/kube-proxy/tests/conformance_network_services_proxy.rs
/// services_should_have_session_affinity_for_nodeport.
#[tokio::test]
#[ignore = "GAP: needs live network — session affinity verification requires iptables xt_recent + live TCP/UDP sockets"]
async fn services_session_affinity_nodeport_connectivity() {}

/// [sig-network] Services should be able to switch session affinity for
/// NodePort service [LinuxOnly] [Conformance] — live connectivity (STUB)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/service.go:2287
/// Sonobuoy (Round 160): FAIL (failing.txt)
#[tokio::test]
#[ignore = "GAP: needs live network — session affinity switch requires iptables xt_recent + live TCP/UDP sockets"]
async fn services_switch_session_affinity_nodeport_connectivity() {}

// ---------------------------------------------------------------------------
// Group 10 — Proxy (STUB — live HTTP)
// Upstream: test/e2e/network/proxy.go
// ---------------------------------------------------------------------------

/// [sig-network] Proxy version v1 A set of valid responses are returned for
/// both pod and service Proxy [Conformance] — live connectivity (STUB)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/proxy.go:432
/// Sonobuoy (Round 160): FAIL (failing.txt)
///
/// The iptables side is covered in
/// crates/kube-proxy/tests/conformance_network_services_proxy.rs
/// proxy_valid_responses_for_pod_and_service.
#[tokio::test]
#[ignore = "GAP: needs live network — /proxy subresource requires running api-server + live pod HTTP backend"]
async fn proxy_valid_responses_for_pod_and_service_connectivity() {}

/// [sig-network] Proxy version v1 should proxy through a service and a pod
/// [Conformance] — live connectivity (STUB)
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/proxy.go:137
/// Sonobuoy (Round 160): FAIL (failing.txt)
#[tokio::test]
#[ignore = "GAP: needs live network — /proxy subresource requires running api-server + live pod HTTP backend"]
async fn proxy_through_service_and_pod_connectivity() {}
