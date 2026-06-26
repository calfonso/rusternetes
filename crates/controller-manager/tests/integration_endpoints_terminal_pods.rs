//! Integration tests mirroring the upstream Kubernetes Endpoints controller
//! integration suite as RED-state TDD pins.
//!
//! Upstream source (k/k release-1.35):
//!   test/integration/endpoints/endpoints_test.go
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/endpoints/endpoints_test.go>
//!
//! Mirrored tests (keep upstream Go names):
//!   * TestEndpointUpdates
//!   * TestEndpointWithMultiplePodUpdates
//!   * TestExternalNameToClusterIPTransition
//!   * TestEndpointWithTerminatingPod
//!   * TestEndpointTruncate
//!
//! These tests use `Arc<MemoryStorage>` plus direct `EndpointsController::reconcile_all`
//! calls (the canonical controller-driver pattern used in
//! `crates/controller-manager/tests/service_lb_endpoints_test.rs`). They are
//! deliberately RED — i.e. they encode the upstream invariants but the
//! corresponding controller surface either does not yet exist (truncation
//! annotation, ExternalName→ClusterIP transition handling, multi-service
//! parallel update) or has known gaps. Each test pins the invariant so that
//! once the implementation lands, the test flips GREEN without further edits.
//!
//! The existing `crates/controller-manager/tests/endpoints_controller_test.rs`
//! already covers basic Endpoints creation/selector/port-mapping. To avoid
//! duplication this file focuses exclusively on the upstream scenarios above:
//! resource-version stability, multi-service fan-out, service-type transition,
//! terminating-pod exclusion, and the 1000-address truncation cap.
//!
//! Part of the /batch landing upstream integration-test mirrors as RED-state
//! TDD pins.

use chrono::Utc;
use rusternetes_common::resources::{
    Container, ContainerStatus, Endpoints, IntOrString, Pod, PodCondition, PodSpec, PodStatus,
    Service, ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::endpoints::EndpointsController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Fixture helpers — kept local to this file (no cross-file leakage).
// ---------------------------------------------------------------------------

fn make_service(name: &str, namespace: &str, selector: HashMap<String, String>) -> Service {
    Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.namespace = Some(namespace.to_string());
            m.uid = uuid::Uuid::new_v4().to_string();
            m
        },
        spec: ServiceSpec {
            selector: Some(selector),
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                port: 80,
                target_port: Some(IntOrString::Int(8080)),
                node_port: None,
                app_protocol: None,
            }],
            cluster_ip: Some("10.96.0.1".to_string()),
            service_type: Some(ServiceType::ClusterIP),
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
    }
}

fn make_pod(
    name: &str,
    namespace: &str,
    labels: HashMap<String, String>,
    pod_ip: Option<&str>,
    ready: bool,
) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.namespace = Some(namespace.to_string());
            m.labels = Some(labels);
            m.uid = uuid::Uuid::new_v4().to_string();
            m
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:latest".to_string(),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ports: Some(vec![]),
                env: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                resources: None,
                working_dir: None,
                command: None,
                args: None,
                restart_policy: None,
                resize_policy: None,
                security_context: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            init_containers: None,
            ephemeral_containers: None,
            volumes: None,
            restart_policy: Some("Always".to_string()),
            node_name: Some("node-1".to_string()),
            node_selector: None,
            service_account_name: None,
            service_account: None,
            automount_service_account_token: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority: None,
            priority_class_name: None,
            scheduler_name: None,
            overhead: None,
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
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("192.168.1.10".to_string()),
            host_i_ps: None,
            pod_ip: pod_ip.map(|s| s.to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: if ready { "True" } else { "False" }.to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            container_statuses: Some(vec![ContainerStatus {
                name: "nginx".to_string(),
                ready,
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
    }
}

fn count_ready_addresses(ep: &Endpoints) -> usize {
    ep.subsets
        .iter()
        .map(|s| s.addresses.as_ref().map(|a| a.len()).unwrap_or(0))
        .sum()
}

// ---------------------------------------------------------------------------
// Upstream test mirrors
// ---------------------------------------------------------------------------

/// Upstream: `TestEndpointUpdates`
///
/// Upstream creates a pod + service, observes the endpoints, then mutates the
/// service in a way that has no impact on endpoints (creates a *second*
/// service with no matching pods) and asserts that the original service's
/// Endpoints resourceVersion does NOT change — i.e. the controller must avoid
/// spurious writes when nothing endpoints-relevant changed.
#[tokio::test]
async fn test_endpoint_updates() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "foo".to_string());

    let svc = make_service("svc-1", "default", selector.clone());
    storage
        .create(&build_key("services", Some("default"), "svc-1"), &svc)
        .await
        .unwrap();

    let pod = make_pod(
        "pod-1",
        "default",
        selector.clone(),
        Some("10.244.0.1"),
        true,
    );
    storage
        .create(&build_key("pods", Some("default"), "pod-1"), &pod)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let ep_key = build_key("endpoints", Some("default"), "svc-1");
    let ep1: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints must exist after first reconcile");
    let rv1 = ep1.metadata.resource_version.clone();
    assert_eq!(
        count_ready_addresses(&ep1),
        1,
        "first reconcile must publish the one matching pod"
    );

    // Add a *second* service with a disjoint selector — must not change
    // Endpoints for svc-1.
    let mut other_selector = HashMap::new();
    other_selector.insert("app".to_string(), "bar".to_string());
    let svc2 = make_service("svc-2", "default", other_selector);
    storage
        .create(&build_key("services", Some("default"), "svc-2"), &svc2)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let ep2: Endpoints = storage.get(&ep_key).await.unwrap();
    let rv2 = ep2.metadata.resource_version.clone();
    assert_eq!(
        rv1, rv2,
        "Endpoints for svc-1 must NOT be rewritten when an unrelated service \
         is added (upstream TestEndpointUpdates pins controller idempotency)"
    );
}

/// Upstream: `TestEndpointWithMultiplePodUpdates`
///
/// Upstream creates ten Services selecting the same pod, then flips the pod's
/// Ready condition rapidly. The invariant is that after the dust settles, all
/// ten Services' Endpoints reflect the pod's *final* readiness state. This
/// pins the controller's fan-out: a single pod change must propagate to every
/// selecting service without missing any sync.
#[tokio::test]
async fn test_endpoint_with_multiple_pod_updates() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "fanout".to_string());

    // Ten services, all selecting the same pod
    for i in 0..10 {
        let svc = make_service(&format!("svc-fan-{i}"), "default", selector.clone());
        storage
            .create(
                &build_key("services", Some("default"), &format!("svc-fan-{i}")),
                &svc,
            )
            .await
            .unwrap();
    }

    let pod = make_pod(
        "fanout-pod",
        "default",
        selector.clone(),
        Some("10.244.7.7"),
        true,
    );
    let pod_key = build_key("pods", Some("default"), "fanout-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // Flip ready -> not ready -> ready. Final state: ready.
    let mut not_ready = pod.clone();
    if let Some(status) = not_ready.status.as_mut() {
        status.conditions = Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        }]);
    }
    storage.update(&pod_key, &not_ready).await.unwrap();
    controller.reconcile_all().await.unwrap();

    let again_ready = pod.clone();
    storage.update(&pod_key, &again_ready).await.unwrap();
    controller.reconcile_all().await.unwrap();

    // Every one of the ten services must end up with the pod in `addresses`
    // (ready set), not `notReadyAddresses`.
    for i in 0..10 {
        let ep: Endpoints = storage
            .get(&build_key(
                "endpoints",
                Some("default"),
                &format!("svc-fan-{i}"),
            ))
            .await
            .unwrap_or_else(|_| panic!("Endpoints must exist for svc-fan-{i}"));
        assert_eq!(
            count_ready_addresses(&ep),
            1,
            "svc-fan-{i}: pod final ready state must be reflected (upstream \
             TestEndpointWithMultiplePodUpdates pins fan-out completeness)"
        );
    }
}

/// Upstream: `TestExternalNameToClusterIPTransition`
///
/// Upstream creates a pod + ExternalName Service, asserts NO endpoints are
/// generated, then mutates the Service into a ClusterIP type with the same
/// selector and asserts Endpoints ARE created — and notably that the
/// `service.kubernetes.io/headless` label is NOT set (the controller must not
/// leak the headless marker onto a non-headless Endpoints object).
#[tokio::test]
async fn test_external_name_to_cluster_ip_transition() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "transition".to_string());

    // Step 1: ExternalName service — no endpoints expected.
    let mut svc = make_service("transition-svc", "default", selector.clone());
    svc.spec.service_type = Some(ServiceType::ExternalName);
    svc.spec.external_name = Some("example.com".to_string());
    // ExternalName services have no cluster_ip
    svc.spec.cluster_ip = None;
    let svc_key = build_key("services", Some("default"), "transition-svc");
    storage.create(&svc_key, &svc).await.unwrap();

    let pod = make_pod(
        "transition-pod",
        "default",
        selector.clone(),
        Some("10.244.8.8"),
        true,
    );
    storage
        .create(&build_key("pods", Some("default"), "transition-pod"), &pod)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let ep_key = build_key("endpoints", Some("default"), "transition-svc");
    let initial: Result<Endpoints, _> = storage.get(&ep_key).await;
    assert!(
        initial.is_err(),
        "ExternalName services must NOT have Endpoints (upstream \
         TestExternalNameToClusterIPTransition step 1)"
    );

    // Step 2: transition to ClusterIP.
    let mut svc2 = svc.clone();
    svc2.spec.service_type = Some(ServiceType::ClusterIP);
    svc2.spec.external_name = None;
    svc2.spec.cluster_ip = Some("10.96.99.99".to_string());
    storage.update(&svc_key, &svc2).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let after: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints MUST be created after ExternalName→ClusterIP transition");
    assert_eq!(
        count_ready_addresses(&after),
        1,
        "transitioned ClusterIP service must publish the matching pod"
    );

    // Upstream specifically asserts the headless label is NOT applied to a
    // non-headless service's Endpoints.
    let has_headless_label = after
        .metadata
        .labels
        .as_ref()
        .map(|l| l.contains_key("service.kubernetes.io/headless"))
        .unwrap_or(false);
    assert!(
        !has_headless_label,
        "Endpoints must NOT carry the service.kubernetes.io/headless label \
         on a non-headless ClusterIP service (upstream \
         TestExternalNameToClusterIPTransition step 2)"
    );
}

/// Upstream: `TestEndpointWithTerminatingPod`
///
/// Upstream creates a pod + service, sees the endpoint, then marks the pod as
/// terminating (sets `deletionTimestamp`) and asserts that the pod's address
/// is excluded from `addresses` (and not silently shuffled into
/// `notReadyAddresses` either, unless `publishNotReadyAddresses` is set —
/// which it is not here). Pins the upstream `ShouldPodBeInEndpoints` rule.
#[tokio::test]
async fn test_endpoint_with_terminating_pod() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "terminating".to_string());

    let svc = make_service("term-svc", "default", selector.clone());
    storage
        .create(&build_key("services", Some("default"), "term-svc"), &svc)
        .await
        .unwrap();

    let pod = make_pod(
        "term-pod",
        "default",
        selector.clone(),
        Some("10.244.9.9"),
        true,
    );
    let pod_key = build_key("pods", Some("default"), "term-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let ep_key = build_key("endpoints", Some("default"), "term-svc");
    let before: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints must exist with ready pod");
    assert_eq!(
        count_ready_addresses(&before),
        1,
        "ready pod must appear in addresses before deletion"
    );

    // Mark pod as terminating
    let mut terminating = pod.clone();
    terminating.metadata.deletion_timestamp = Some(Utc::now());
    storage.update(&pod_key, &terminating).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let after: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints must still exist after pod marked terminating");
    assert_eq!(
        count_ready_addresses(&after),
        0,
        "terminating pod (deletionTimestamp set) MUST be excluded from \
         Endpoints addresses (upstream TestEndpointWithTerminatingPod)"
    );
    // And must not have silently leaked into notReadyAddresses either.
    let not_ready: usize = after
        .subsets
        .iter()
        .map(|s| s.not_ready_addresses.as_ref().map(|a| a.len()).unwrap_or(0))
        .sum();
    assert_eq!(
        not_ready, 0,
        "terminating pod must NOT appear in notReadyAddresses when \
         publishNotReadyAddresses is unset"
    );
}

/// Upstream: `TestEndpointTruncate`
///
/// Upstream creates 1001 matching pods, triggering Endpoints truncation at
/// 1000 (the documented cap). The truncated Endpoints must:
///   * contain exactly 1000 addresses
///   * carry the `endpoints.kubernetes.io/over-capacity = "truncated"`
///     annotation
///
/// Then it deletes 501 pods, leaving 500 matching pods, and re-asserts:
///   * Endpoints shrink to 500 addresses
///   * the over-capacity annotation is REMOVED (no longer applicable)
#[tokio::test]
async fn test_endpoint_truncate() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = EndpointsController::new(storage.clone());

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "truncate".to_string());

    let svc = make_service("trunc-svc", "default", selector.clone());
    storage
        .create(&build_key("services", Some("default"), "trunc-svc"), &svc)
        .await
        .unwrap();

    // 1001 matching pods — one over the upstream cap.
    for i in 0..1001 {
        // Distribute across a /16 to avoid IP collisions, irrelevant to the
        // truncation logic but useful for readable assertions in future runs.
        let ip = format!("10.244.{}.{}", i / 256, i % 256);
        let pod = make_pod(
            &format!("trunc-pod-{i}"),
            "default",
            selector.clone(),
            Some(&ip),
            true,
        );
        storage
            .create(
                &build_key("pods", Some("default"), &format!("trunc-pod-{i}")),
                &pod,
            )
            .await
            .unwrap();
    }

    controller.reconcile_all().await.unwrap();

    let ep_key = build_key("endpoints", Some("default"), "trunc-svc");
    let truncated: Endpoints = storage
        .get(&ep_key)
        .await
        .expect("Endpoints must exist after reconcile with 1001 pods");

    assert_eq!(
        count_ready_addresses(&truncated),
        1000,
        "Endpoints must be truncated to the 1000-address cap (upstream \
         TestEndpointTruncate)"
    );
    let annotation = truncated
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("endpoints.kubernetes.io/over-capacity"));
    assert_eq!(
        annotation.map(|s| s.as_str()),
        Some("truncated"),
        "truncated Endpoints must carry \
         endpoints.kubernetes.io/over-capacity=truncated (upstream \
         TestEndpointTruncate annotation invariant)"
    );

    // Delete 501 pods so the remaining count drops to 500 — below the cap.
    for i in 0..501 {
        storage
            .delete(&build_key(
                "pods",
                Some("default"),
                &format!("trunc-pod-{i}"),
            ))
            .await
            .unwrap();
    }

    controller.reconcile_all().await.unwrap();

    let shrunk: Endpoints = storage.get(&ep_key).await.unwrap();
    assert_eq!(
        count_ready_addresses(&shrunk),
        500,
        "After deleting 501 of 1001 pods Endpoints must shrink to 500 \
         addresses"
    );
    let still_annotated = shrunk
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("endpoints.kubernetes.io/over-capacity"));
    assert!(
        still_annotated.is_none(),
        "endpoints.kubernetes.io/over-capacity annotation must be CLEARED \
         once capacity is no longer exceeded (upstream TestEndpointTruncate)"
    );
}
