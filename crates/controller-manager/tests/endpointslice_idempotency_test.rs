//! Idempotency / hot-loop regression tests for the EndpointSlice controller.
//!
//! Background: a kubelet hot-loop on terminal Pod status was fixed in
//! f0eea82 ("fix(kubelet): gate terminal-pod status writes to break sync_pod
//! hot-loop"). These tests pin Unit 4 (terminating-pod retention +
//! publishNotReadyAddresses) on the EndpointSlice side so a similar hot-loop
//! cannot creep back in through that controller.
//!
//! Each test reconciles twice over a stable state and asserts that the second
//! reconcile is either a no-op (slice byte-equal between iterations) or that
//! endpoint ordering is deterministic. If a reconcile keeps rewriting the
//! same slice we would see resource-version churn → watch storm → hot-loop.

use rusternetes_common::resources::endpointslice::{
    Endpoint, EndpointConditions, EndpointPort, EndpointReference,
};
use rusternetes_common::resources::{
    Container, ContainerPort, EndpointSlice, IntOrString, Pod, PodCondition, PodSpec, PodStatus,
    Service, ServicePort, ServiceSpec,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::endpointslice::EndpointSliceController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn svc(name: &str, ns: &str, publish_not_ready: bool) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(ns),
        spec: ServiceSpec {
            ports: vec![ServicePort {
                name: None,
                port: 80,
                target_port: Some(IntOrString::Int(80)),
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            selector: Some(HashMap::from([("app".to_string(), "foo".to_string())])),
            publish_not_ready_addresses: if publish_not_ready { Some(true) } else { None },
            ..Default::default()
        },
        status: None,
    }
}

fn ready_pod(name: &str, ns: &str, pod_ip: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name)
            .with_namespace(ns)
            .with_labels(HashMap::from([("app".to_string(), "foo".to_string())])),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
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
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            pod_ip: Some(pod_ip.to_string()),
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

/// Reconcile twice over a Service + Ready Pod + already-existing EndpointSlice
/// representation; the second reconcile must be a strict no-op (byte-equal
/// JSON). A non-equal JSON would mean the controller rewrites a stable state,
/// generating a watch event each interval — the classic hot-loop signature.
#[tokio::test]
async fn test_endpointslice_reconcile_idempotent_on_stable_state() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(Arc::clone(&storage));

    let s = svc("foo", "default", false);
    let p = ready_pod("p1", "default", "10.0.0.1");

    storage
        .create(&build_key("services", Some("default"), "foo"), &s)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("default"), "p1"), &p)
        .await
        .unwrap();

    // First reconcile populates the slice.
    controller.reconcile_all().await.unwrap();
    let slice_key = build_key("endpointslices", Some("default"), "foo");
    let first: EndpointSlice = storage.get(&slice_key).await.unwrap();
    // Compare via serde_json::Value so HashMap iteration order in labels
    // doesn't produce false positives — only semantic content differences
    // count as hot-loop signatures.
    let first_v: serde_json::Value = serde_json::to_value(&first).unwrap();

    // Second reconcile over identical state must not rewrite the slice.
    controller.reconcile_all().await.unwrap();
    let second: EndpointSlice = storage.get(&slice_key).await.unwrap();
    let second_v: serde_json::Value = serde_json::to_value(&second).unwrap();

    assert_eq!(
        first_v, second_v,
        "EndpointSlice content must be semantically equal between two reconciles over a stable state; \
         differences here are the hot-loop signature."
    );
    // Sanity: the slice does contain the pod's endpoint.
    assert_eq!(first.endpoints.len(), 1);
    assert_eq!(first.endpoints[0].addresses, vec!["10.0.0.1".to_string()]);
}

/// Pods in terminal Succeeded phase must never appear in an EndpointSlice
/// (K8s ShouldPodBeInEndpointSlice excludes Succeeded/Failed). The Pod
/// resource itself must not be mutated by reconcile (controller is
/// read-only on Pods). Both invariants verified by byte-equal Pod JSON.
#[tokio::test]
async fn test_endpointslice_does_not_include_succeeded_pods() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(Arc::clone(&storage));

    let s = svc("foo", "default", false);
    let mut p = ready_pod("done", "default", "10.0.0.9");
    // Override phase to Succeeded — pod has finished its work.
    if let Some(status) = p.status.as_mut() {
        status.phase = Some(Phase::Succeeded);
    }

    storage
        .create(&build_key("services", Some("default"), "foo"), &s)
        .await
        .unwrap();
    let pod_key = build_key("pods", Some("default"), "done");
    storage.create(&pod_key, &p).await.unwrap();

    let pod_before: Pod = storage.get(&pod_key).await.unwrap();
    let pod_before_json = serde_json::to_string(&pod_before).unwrap();

    controller.reconcile_all().await.unwrap();

    let slice: EndpointSlice = storage
        .get(&build_key("endpointslices", Some("default"), "foo"))
        .await
        .unwrap();
    assert!(
        slice.endpoints.is_empty(),
        "Succeeded pods must not appear in EndpointSlice endpoints; got {:?}",
        slice.endpoints
    );

    let pod_after: Pod = storage.get(&pod_key).await.unwrap();
    let pod_after_json = serde_json::to_string(&pod_after).unwrap();
    assert_eq!(
        pod_before_json, pod_after_json,
        "EndpointSlice reconcile must not mutate Pod objects"
    );
}

/// Terminating pods (deletionTimestamp set) are RETAINED with
/// `terminating=true, ready=false`. The representation must also be stable:
/// reconciling twice over the same terminating state must not rewrite the
/// slice (else kube-proxy sees a perpetual stream of "endpoint changed"
/// events and rebuilds iptables in a tight loop).
#[tokio::test]
async fn test_endpointslice_terminating_pod_stable_representation() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(Arc::clone(&storage));

    let s = svc("foo", "default", false);
    let mut p = ready_pod("p1", "default", "10.0.0.1");
    p.metadata.deletion_timestamp = Some(chrono::Utc::now());

    storage
        .create(&build_key("services", Some("default"), "foo"), &s)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("default"), "p1"), &p)
        .await
        .unwrap();

    // First reconcile: should produce terminating=true, ready=false, serving=true.
    controller.reconcile_all().await.unwrap();
    let slice_key = build_key("endpointslices", Some("default"), "foo");
    let first: EndpointSlice = storage.get(&slice_key).await.unwrap();
    let first_rv = first.metadata.resource_version.clone();

    assert_eq!(
        first.endpoints.len(),
        1,
        "terminating pod must remain in EndpointSlice for graceful drain"
    );
    let conds = first.endpoints[0]
        .conditions
        .as_ref()
        .expect("endpoint must have conditions");
    assert_eq!(conds.terminating, Some(true), "terminating must be true");
    assert_eq!(
        conds.ready,
        Some(false),
        "ready must be false for terminating pods unless publishNotReadyAddresses"
    );
    assert_eq!(
        conds.serving,
        Some(true),
        "serving mirrors pod Ready independent of terminating state"
    );

    let first_v: serde_json::Value = serde_json::to_value(&first).unwrap();

    // Second reconcile: must NOT write — semantic JSON equality + resource_version unchanged.
    controller.reconcile_all().await.unwrap();
    let second: EndpointSlice = storage.get(&slice_key).await.unwrap();
    let second_v: serde_json::Value = serde_json::to_value(&second).unwrap();

    assert_eq!(
        first_v, second_v,
        "second reconcile over a stable terminating pod must produce semantically equal JSON"
    );
    assert_eq!(
        first_rv, second.metadata.resource_version,
        "resourceVersion must NOT advance on a no-op reconcile (would imply a write)"
    );
}

/// Smoking-gun guard: with multiple pods grouped into a single EndpointSlice,
/// the endpoint ordering must be deterministic across reconciles. The
/// controller groups pods via a HashMap keyed by port-mapping, so two
/// reconciles over identical pod data MUST yield identical endpoint order.
/// If the order flips, kube-proxy observes a write every reconcile interval.
#[tokio::test]
async fn test_endpoint_ordering_deterministic() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointSliceController::new(Arc::clone(&storage));

    let s = svc("foo", "default", false);
    storage
        .create(&build_key("services", Some("default"), "foo"), &s)
        .await
        .unwrap();

    // Three pods with distinct names + IPs, all matching the same service port.
    for (name, ip) in [
        ("pod-alpha", "10.0.0.1"),
        ("pod-bravo", "10.0.0.2"),
        ("pod-charlie", "10.0.0.3"),
    ] {
        let p = ready_pod(name, "default", ip);
        storage
            .create(&build_key("pods", Some("default"), name), &p)
            .await
            .unwrap();
    }

    let slice_key = build_key("endpointslices", Some("default"), "foo");

    // First reconcile — capture the resulting order.
    controller.reconcile_all().await.unwrap();
    let first: EndpointSlice = storage.get(&slice_key).await.unwrap();
    assert_eq!(
        first.endpoints.len(),
        3,
        "all three matching ready pods must appear in the slice"
    );
    let order_first: Vec<String> = first
        .endpoints
        .iter()
        .map(|e| {
            e.target_ref
                .as_ref()
                .and_then(|r| r.name.clone())
                .unwrap_or_default()
        })
        .collect();

    // Second reconcile — order must be byte-identical.
    controller.reconcile_all().await.unwrap();
    let second: EndpointSlice = storage.get(&slice_key).await.unwrap();
    let order_second: Vec<String> = second
        .endpoints
        .iter()
        .map(|e| {
            e.target_ref
                .as_ref()
                .and_then(|r| r.name.clone())
                .unwrap_or_default()
        })
        .collect();

    assert_eq!(
        order_first, order_second,
        "endpoint ordering must be deterministic across reconciles; \
         observed flip would force a slice rewrite every interval (hot-loop). \
         Fix: sort endpoints by target_ref.name before writing."
    );

    // Belt-and-braces: full slice JSON must also be byte-equal.
    let first_json = serde_json::to_string(&PortsAndEndpoints {
        ports: first.ports.clone(),
        endpoints: first.endpoints.clone(),
    })
    .unwrap();
    let second_json = serde_json::to_string(&PortsAndEndpoints {
        ports: second.ports.clone(),
        endpoints: second.endpoints.clone(),
    })
    .unwrap();
    assert_eq!(
        first_json, second_json,
        "slice endpoints+ports JSON must be byte-equal across reconciles"
    );
}

/// Minimal helper struct for byte-equal comparison of just the relevant
/// EndpointSlice fields (ports + endpoints), avoiding metadata that may
/// carry implementation-specific resourceVersion/UID noise.
#[derive(serde::Serialize)]
struct PortsAndEndpoints {
    ports: Vec<EndpointPort>,
    endpoints: Vec<Endpoint>,
}

// Sanity check that helper types are wired correctly (won't surface as a
// false-negative on a missing import if the test file otherwise compiles).
#[allow(dead_code)]
fn _imports_alive(_: EndpointConditions, _: EndpointReference) {}
