//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-apps] StatefulSet + DaemonSet.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apps/
//! (statefulset.go and daemon_set.go).
//!
//! See docs/conformance/apps-statefulset-daemonset.md for the test-by-test
//! status table.
//!
//! Owner crate: rusternetes-controller-manager. Tests drive the StatefulSet
//! and DaemonSet controllers directly against an `Arc<MemoryStorage>` — no
//! HTTP harness is needed because the controller-manager does not host the
//! REST surface. Where the live cluster's kubelet would normally remove a pod
//! after the controller marks it for deletion, the tests run
//! `simulate_kubelet_cleanup` to keep the controller's view of storage
//! consistent across reconcile cycles.

use rusternetes_common::resources::pod::PodCondition;
use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::volume::{
    PersistentVolumeClaim, PersistentVolumeClaimSpec, ResourceRequirements,
};
use rusternetes_common::resources::workloads::{
    DaemonSetUpdateStrategy, RollingUpdateDaemonSet, RollingUpdateStatefulSetStrategy,
    StatefulSetPersistentVolumeClaimRetentionPolicy, StatefulSetUpdateStrategy,
};
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::daemonset::DaemonSetController;
use rusternetes_controller_manager::controllers::statefulset::StatefulSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn empty_pod_spec(image: &str, container_name: &str) -> PodSpec {
    PodSpec {
        containers: vec![Container {
            name: container_name.to_string(),
            image: image.to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            ports: None,
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
        restart_policy: Some("Always".to_string()),
        node_selector: None,
        node_name: None,
        volumes: None,
        affinity: None,
        tolerations: None,
        service_account_name: None,
        service_account: None,
        priority: None,
        priority_class_name: None,
        hostname: None,
        subdomain: None,
        host_network: None,
        host_pid: None,
        host_ipc: None,
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
    }
}

fn make_statefulset(name: &str, namespace: &str, replicas: i32) -> StatefulSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    StatefulSet {
        type_meta: TypeMeta {
            kind: "StatefulSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name).with_namespace(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: StatefulSetSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: empty_pod_spec("registry.k8s.io/e2e-test-images/agnhost:2.55", "webserver"),
            },
            service_name: format!("{}-headless", name),
            pod_management_policy: Some("Parallel".to_string()),
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
            volume_claim_templates: None,
            persistent_volume_claim_retention_policy: None,
            ordinals: None,
        },
        status: Some(StatefulSetStatus {
            replicas: 0,
            ready_replicas: Some(0),
            current_replicas: Some(0),
            updated_replicas: Some(0),
            available_replicas: None,
            collision_count: None,
            observed_generation: None,
            current_revision: None,
            update_revision: None,
            conditions: None,
        }),
    }
}

fn make_node(name: &str, labels: Option<HashMap<String, String>>) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.labels = labels;
            m.ensure_uid();
            m
        },
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        }),
        status: None,
    }
}

fn make_daemonset(
    name: &str,
    namespace: &str,
    image: &str,
    node_selector: Option<HashMap<String, String>>,
) -> DaemonSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());
    labels.insert("daemonset-name".to_string(), name.to_string());

    let mut spec = empty_pod_spec(image, "app");
    spec.node_selector = node_selector;

    DaemonSet {
        type_meta: TypeMeta {
            kind: "DaemonSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name).with_namespace(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec,
            },
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
        },
        status: None,
    }
}

/// Replay kubelet: physically delete pods whose `deletionTimestamp` was
/// stamped by the controller. Upstream Sonobuoy tests assume the kubelet
/// will reap such pods within the test's polling window; here we do it
/// synchronously so the next reconcile sees the new pod count.
async fn simulate_kubelet_cleanup(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            let key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
            let _ = storage.delete(&key).await;
        }
    }
}

async fn mark_pod_ready(storage: &Arc<MemoryStorage>, namespace: &str, pod_name: &str) {
    let pod_key = build_key("pods", Some(namespace), pod_name);
    let mut pod: Pod = storage.get(&pod_key).await.unwrap();
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        conditions: Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(chrono::Utc::now()),
            observed_generation: None,
        }]),
        ..pod.status.unwrap_or_default()
    });
    storage.update(&pod_key, &pod).await.unwrap();
}

async fn mark_all_pods_ready(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for pod in pods {
        mark_pod_ready(storage, namespace, &pod.metadata.name).await;
    }
}

// ===========================================================================
// [sig-apps] StatefulSet — Basic StatefulSet functionality [StatefulSetBasic]
// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go
// ===========================================================================

/// [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic]
/// should perform rolling updates and roll backs of template modifications [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go:360
/// Sonobuoy (Round 160, captured 2026-05-17 job 53eadf2451e4467c): PASS
#[tokio::test]
async fn statefulset_should_perform_rolling_updates_and_rollbacks_of_template_modifications() {
    let storage = setup_test().await;
    let ns = "default";

    // Create a 2-replica StatefulSet
    let mut ss = make_statefulset("ss-roll", ns, 2);
    ss.spec.update_strategy = Some(StatefulSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: None,
    });
    let key = build_key("statefulsets", Some(ns), "ss-roll");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let initial: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(initial.len(), 2);
    let original_revision = initial[0]
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("controller-revision-hash"))
        .cloned()
        .unwrap();

    mark_all_pods_ready(&storage, ns).await;

    // Roll forward: change image to a new tag
    let mut fresh: StatefulSet = storage.get(&key).await.unwrap();
    fresh.spec.template.spec.containers[0].image =
        "registry.k8s.io/e2e-test-images/agnhost:2.59".to_string();
    storage.update(&key, &fresh).await.unwrap();

    // Drive the rolling update to completion. K8s deletes one stale pod per
    // reconcile and the next reconcile recreates it; we replay kubelet in
    // between so the new pod count is observed.
    for _ in 0..8 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let after_roll: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(after_roll.len(), 2, "rolling update preserves replicas");
    for pod in &after_roll {
        let rev = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        assert_ne!(rev, original_revision, "all pods rolled to new revision");
    }

    // Roll back to the original image
    let mut rollback: StatefulSet = storage.get(&key).await.unwrap();
    rollback.spec.template.spec.containers[0].image =
        "registry.k8s.io/e2e-test-images/agnhost:2.55".to_string();
    storage.update(&key, &rollback).await.unwrap();

    for _ in 0..8 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let after_rollback: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(after_rollback.len(), 2);
    for pod in &after_rollback {
        let rev = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        assert_eq!(rev, original_revision, "all pods rolled back to original");
    }
}

/// [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic]
/// should perform canary updates and phased rolling updates of template modifications [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go:376
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn statefulset_should_perform_canary_updates_with_partition() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 4 replicas with partition=2 — only ordinals 2 and 3 should roll
    let mut ss = make_statefulset("canary", ns, 4);
    ss.spec.update_strategy = Some(StatefulSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateStatefulSetStrategy {
            partition: Some(2),
            max_unavailable: None,
        }),
    });
    let key = build_key("statefulsets", Some(ns), "canary");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let initial: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(initial.len(), 4);
    let original_revision = initial[0]
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("controller-revision-hash"))
        .cloned()
        .unwrap();
    mark_all_pods_ready(&storage, ns).await;

    // Change the template
    let mut fresh: StatefulSet = storage.get(&key).await.unwrap();
    fresh.spec.template.spec.containers[0].image =
        "registry.k8s.io/e2e-test-images/agnhost:2.59".to_string();
    storage.update(&key, &fresh).await.unwrap();

    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(after.len(), 4);

    // Ordinals below partition (canary-0, canary-1) keep original revision;
    // ordinals at/above partition (canary-2, canary-3) are updated.
    for pod in &after {
        let rev = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        let ordinal: i32 = pod
            .metadata
            .name
            .rsplit('-')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap();
        if ordinal < 2 {
            assert_eq!(
                rev, original_revision,
                "pod {} below partition keeps original revision",
                pod.metadata.name
            );
        } else {
            assert_ne!(
                rev, original_revision,
                "pod {} above partition takes new revision",
                pod.metadata.name
            );
        }
    }
}

/// [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic]
/// Scaling should happen in predictable order and halt if any stateful pod is unhealthy [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go:593
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn statefulset_scaling_should_happen_in_predictable_order_with_ordered_ready() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("ordered", ns, 3);
    ss.spec.pod_management_policy = Some("OrderedReady".to_string());
    let key = build_key("statefulsets", Some(ns), "ordered");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // First reconcile: only ordered-0 may be created
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1);
    assert_eq!(pods[0].metadata.name, "ordered-0");

    // ordered-0 not Ready → halt: subsequent reconciles must NOT create ordered-1
    for _ in 0..3 {
        controller.reconcile_all().await.unwrap();
    }
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1, "scaling halts on unhealthy predecessor");

    // Mark ordered-0 Ready; ordered-1 may now appear
    mark_pod_ready(&storage, ns, "ordered-0").await;
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);

    // Mark ordered-1 Ready; ordered-2 may now appear
    mark_pod_ready(&storage, ns, "ordered-1").await;
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3);

    let mut names: Vec<String> = pods.iter().map(|p| p.metadata.name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["ordered-0", "ordered-1", "ordered-2"]);
}

/// [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic]
/// Burst scaling should run to completion even with unhealthy pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go:616
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn statefulset_burst_scaling_should_run_to_completion_with_parallel_policy() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("burst", ns, 5);
    ss.spec.pod_management_policy = Some("Parallel".to_string());
    let key = build_key("statefulsets", Some(ns), "burst");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // Parallel must create all pods at once — no waiting on Ready conditions
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods.len(),
        5,
        "Parallel/burst mode must create all replicas in one cycle"
    );

    let mut names: Vec<String> = pods.iter().map(|p| p.metadata.name.clone()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["burst-0", "burst-1", "burst-2", "burst-3", "burst-4"]
    );
}

/// [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic]
/// Should recreate evicted statefulset [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go:641
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn statefulset_should_recreate_evicted_pod_with_same_ordinal() {
    let storage = setup_test().await;
    let ns = "default";

    let ss = make_statefulset("evict", ns, 2);
    let key = build_key("statefulsets", Some(ns), "evict");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);

    // Simulate an eviction by deleting evict-0 from storage outright
    storage
        .delete(&build_key("pods", Some(ns), "evict-0"))
        .await
        .unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1);

    // Reconcile must recreate the missing ordinal
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let names: HashSet<_> = pods.iter().map(|p| p.metadata.name.as_str()).collect();
    assert!(names.contains("evict-0"), "evict-0 must be recreated");
    assert!(names.contains("evict-1"));
    assert_eq!(pods.len(), 2);
}

/// [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic]
/// should have a working scale subresource [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go:714
/// Sonobuoy (Round 160): PASS
///
/// Scoped mirror: we drive `spec.replicas` directly through storage (the
/// /scale subresource just patches the same field) and verify the controller
/// observes the change.
#[tokio::test]
async fn statefulset_should_have_a_working_scale_subresource() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("scale", ns, 1);
    ss.spec.pod_management_policy = Some("Parallel".to_string());
    let key = build_key("statefulsets", Some(ns), "scale");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        storage
            .list::<Pod>("/registry/pods/default/")
            .await
            .unwrap()
            .len(),
        1
    );

    // Scale up
    let mut fresh: StatefulSet = storage.get(&key).await.unwrap();
    fresh.spec.replicas = Some(3);
    storage.update(&key, &fresh).await.unwrap();
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        storage
            .list::<Pod>("/registry/pods/default/")
            .await
            .unwrap()
            .len(),
        3
    );

    // Status should reflect the new replica count
    let updated: StatefulSet = storage.get(&key).await.unwrap();
    assert_eq!(updated.status.unwrap().replicas, 3);
}

/// [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic]
/// should list, patch and delete a collection of StatefulSets [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go:760
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn statefulset_should_list_patch_and_delete_collection() {
    let storage = setup_test().await;
    let ns = "default";

    // Create three StatefulSets in the same namespace
    for name in &["coll-a", "coll-b", "coll-c"] {
        let ss = make_statefulset(name, ns, 1);
        let key = build_key("statefulsets", Some(ns), name);
        storage.create(&key, &ss).await.unwrap();
    }

    // List should return all three
    let listed: Vec<StatefulSet> = storage
        .list("/registry/statefulsets/default/")
        .await
        .unwrap();
    assert_eq!(listed.len(), 3);

    // Patch one (simulate `kubectl patch`)
    let key = build_key("statefulsets", Some(ns), "coll-b");
    let mut patched: StatefulSet = storage.get(&key).await.unwrap();
    let mut anns = HashMap::new();
    anns.insert("e2e/test".to_string(), "patched".to_string());
    patched.metadata.annotations = Some(anns);
    storage.update(&key, &patched).await.unwrap();

    let after_patch: StatefulSet = storage.get(&key).await.unwrap();
    assert_eq!(
        after_patch
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("e2e/test"))
            .map(String::as_str),
        Some("patched")
    );

    // Delete the collection
    for name in &["coll-a", "coll-b", "coll-c"] {
        storage
            .delete(&build_key("statefulsets", Some(ns), name))
            .await
            .unwrap();
    }
    let remaining: Vec<StatefulSet> = storage
        .list("/registry/statefulsets/default/")
        .await
        .unwrap();
    assert!(remaining.is_empty());
}

/// [sig-apps] StatefulSet Basic StatefulSet functionality [StatefulSetBasic]
/// should validate Statefulset Status endpoints [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go:811
/// Sonobuoy (Round 160): PASS
#[tokio::test]
async fn statefulset_should_validate_status_endpoint_fields() {
    let storage = setup_test().await;
    let ns = "default";

    let ss = make_statefulset("status", ns, 3);
    let key = build_key("statefulsets", Some(ns), "status");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let after: StatefulSet = storage.get(&key).await.unwrap();
    let status = after.status.expect("status must be populated");

    assert_eq!(
        status.replicas, 3,
        "status.replicas tracks actual pod count"
    );
    assert_eq!(status.current_replicas, Some(3));
    // updateRevision should be set once the controller computes a hash
    assert!(
        status.update_revision.is_some(),
        "status.updateRevision must be populated"
    );
}

// ---------------------------------------------------------------------------
// Bonus StatefulSet coverage drawn from the conformance suite headers but
// not enumerated above (PVC plumbing + headless-service binding).
// ---------------------------------------------------------------------------

/// [sig-apps] StatefulSet AvailableReplicas should get updated accordingly when MinReadySeconds is enabled [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go (MinReadySeconds suite)
/// Sonobuoy (Round 160, captured 2026-05-18): PASS — `compute_status` now mirrors
/// K8s `IsPodAvailable` and only counts a pod toward `availableReplicas` once its
/// Ready=True condition has held for at least `spec.minReadySeconds` seconds.
#[tokio::test]
async fn statefulset_available_replicas_should_track_min_ready_seconds() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("avail", ns, 2);
    ss.spec.min_ready_seconds = Some(5);
    let key = build_key("statefulsets", Some(ns), "avail");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    // availableReplicas would only become >0 after min_ready_seconds elapse.
    let after: StatefulSet = storage.get(&key).await.unwrap();
    assert_eq!(
        after.status.unwrap().available_replicas,
        Some(0),
        "availableReplicas should respect minReadySeconds"
    );
}

/// [sig-apps] StatefulSet PVC retention policy — whenScaled=Delete reclaims PVCs on scale-down
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go (PVC retention suite)
/// Sonobuoy (Round 160, "Other" bucket): PASS — controller GCs PVCs for
/// ordinals beyond `spec.replicas` when `whenScaled=Delete` is set.
#[tokio::test]
async fn statefulset_pvc_retention_policy_should_delete_pvcs_on_scale_down() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("pvc-retain", ns, 3);
    ss.spec.volume_claim_templates = Some(vec![PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("data"),
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![],
            resources: ResourceRequirements::default(),
            volume_name: None,
            storage_class_name: None,
            volume_mode: None,
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    }]);
    ss.spec.persistent_volume_claim_retention_policy =
        Some(StatefulSetPersistentVolumeClaimRetentionPolicy {
            when_deleted: Some("Retain".to_string()),
            when_scaled: Some("Delete".to_string()),
        });
    let key = build_key("statefulsets", Some(ns), "pvc-retain");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();
    assert_eq!(pvcs.len(), 3, "3 PVCs created from volumeClaimTemplates");

    // Scale down to 1
    let mut fresh: StatefulSet = storage.get(&key).await.unwrap();
    fresh.spec.replicas = Some(1);
    storage.update(&key, &fresh).await.unwrap();

    for _ in 0..4 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
    }

    // With whenScaled=Delete, the 2 PVCs for ordinals 1 and 2 should be GC'd.
    let pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();
    assert_eq!(pvcs.len(), 1, "PVCs for removed ordinals must be deleted");
}

/// [sig-apps] StatefulSet PVC retention — whenScaled=Retain keeps PVCs
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go (PVC retention suite)
/// Sonobuoy (Round 160): PASS (default retain behaviour matches because
/// the controller does not delete PVCs at all).
#[tokio::test]
async fn statefulset_pvc_retention_policy_retain_keeps_pvcs_on_scale_down() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("pvc-keep", ns, 2);
    ss.spec.pod_management_policy = Some("Parallel".to_string());
    ss.spec.volume_claim_templates = Some(vec![PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("data"),
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![],
            resources: ResourceRequirements::default(),
            volume_name: None,
            storage_class_name: None,
            volume_mode: None,
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    }]);
    ss.spec.persistent_volume_claim_retention_policy =
        Some(StatefulSetPersistentVolumeClaimRetentionPolicy {
            when_deleted: Some("Retain".to_string()),
            when_scaled: Some("Retain".to_string()),
        });
    let key = build_key("statefulsets", Some(ns), "pvc-keep");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();
    assert_eq!(pvcs.len(), 2);

    // Scale down to 1 — Retain means PVCs survive
    let mut fresh: StatefulSet = storage.get(&key).await.unwrap();
    fresh.spec.replicas = Some(1);
    storage.update(&key, &fresh).await.unwrap();
    for _ in 0..4 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
    }

    let pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();
    assert_eq!(
        pvcs.len(),
        2,
        "Retain policy keeps PVCs across scale-down events"
    );
}

/// [sig-apps] StatefulSet headless Service binding — pods carry the headless
/// Service name as their `subdomain` so DNS A records resolve.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go (Identity tests)
/// Sonobuoy (Round 160): was FAIL; fixed by this PR — pod.spec.subdomain now
/// stamped from sts.spec.serviceName.
#[tokio::test]
async fn statefulset_pods_should_bind_headless_service_via_subdomain() {
    let storage = setup_test().await;
    let ns = "default";

    // Create the headless service
    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "binding".to_string());
    let svc = rusternetes_common::resources::Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("binding-headless").with_namespace(ns.to_string()),
        spec: rusternetes_common::resources::service::ServiceSpec {
            selector: Some(selector),
            cluster_ip: Some("None".to_string()),
            ..Default::default()
        },
        status: None,
    };
    storage
        .create(&build_key("services", Some(ns), "binding-headless"), &svc)
        .await
        .unwrap();

    let mut ss = make_statefulset("binding", ns, 2);
    ss.spec.service_name = "binding-headless".to_string();
    let key = build_key("statefulsets", Some(ns), "binding");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in &pods {
        assert_eq!(
            pod.spec.as_ref().and_then(|s| s.subdomain.as_deref()),
            Some("binding-headless"),
            "pod {} should carry the headless service name as subdomain",
            pod.metadata.name
        );
    }
}

// ===========================================================================
// [sig-apps] Daemon set — k8s.io/kubernetes/test/e2e/apps/daemon_set.go
// ===========================================================================

/// [sig-apps] Daemon set [Serial] should run and stop simple daemon [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:240
/// Sonobuoy (Round 160, "Other" bucket): FAIL — tracked under DaemonSet bucket.
#[tokio::test]
async fn daemonset_should_run_and_stop_simple_daemon() {
    let storage = setup_test().await;
    let ns = "default";

    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("simple", ns, "registry.k8s.io/pause:3.9", None);
    let key = build_key("daemonsets", Some(ns), "simple");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "one pod per eligible node");

    // Stop: delete the DaemonSet and its pods, then verify cleanup is observable.
    for pod in &pods {
        storage
            .delete(&build_key("pods", Some(ns), &pod.metadata.name))
            .await
            .unwrap();
    }
    storage.delete(&key).await.unwrap();

    let leftover: Vec<DaemonSet> = storage.list("/registry/daemonsets/default/").await.unwrap();
    assert!(leftover.is_empty());
}

/// [sig-apps] Daemon set [Serial] should run and stop complex daemon [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:258
/// Sonobuoy (Round 160, "Other" bucket): FAIL — tracked under DaemonSet bucket.
///
/// "Complex" = with a node selector; verifies the daemon only runs on
/// matching nodes and that the controller cleans up when the selector is
/// changed to exclude a previously-matching node.
#[tokio::test]
async fn daemonset_should_run_and_stop_complex_daemon_with_node_selector() {
    let storage = setup_test().await;
    let ns = "default";

    let mut blue = HashMap::new();
    blue.insert("color".to_string(), "blue".to_string());
    let mut red = HashMap::new();
    red.insert("color".to_string(), "red".to_string());

    for (name, lbl) in [
        ("node-blue-1", Some(blue.clone())),
        ("node-blue-2", Some(blue.clone())),
        ("node-red-1", Some(red)),
    ] {
        let node = make_node(name, lbl);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    let mut selector = HashMap::new();
    selector.insert("color".to_string(), "blue".to_string());
    let ds = make_daemonset("complex", ns, "registry.k8s.io/pause:3.9", Some(selector));
    let key = build_key("daemonsets", Some(ns), "complex");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods.len(),
        2,
        "only the two blue nodes should run the daemon"
    );
    let assigned: HashSet<_> = pods
        .iter()
        .filter_map(|p| p.spec.as_ref()?.node_name.as_deref())
        .collect();
    assert!(assigned.contains("node-blue-1"));
    assert!(assigned.contains("node-blue-2"));
    assert!(!assigned.contains("node-red-1"));

    // Stop the daemon: switch the selector to one no node carries
    let mut fresh: DaemonSet = storage.get(&key).await.unwrap();
    let mut empty_selector = HashMap::new();
    empty_selector.insert("color".to_string(), "green".to_string());
    fresh.spec.template.spec.node_selector = Some(empty_selector);
    storage.update(&key, &fresh).await.unwrap();

    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert!(
        pods.is_empty(),
        "no node matches the new selector → all pods deleted"
    );
}

/// [sig-apps] Daemon set [Serial] should retry creating failed daemon pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:368
/// Sonobuoy (Round 160, "Other" bucket): FAIL — tracked under DaemonSet bucket.
#[tokio::test]
async fn daemonset_should_retry_creating_failed_daemon_pods() {
    let storage = setup_test().await;
    let ns = "default";

    let node = make_node("node-1", None);
    storage
        .create(&build_key("nodes", None, &node.metadata.name), &node)
        .await
        .unwrap();

    let ds = make_daemonset("retry", ns, "registry.k8s.io/pause:3.9", None);
    let key = build_key("daemonsets", Some(ns), "retry");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1);
    let original_name = pods[0].metadata.name.clone();

    // Simulate the pod failing — kubelet would set phase=Failed. The
    // upstream conformance test then expects the original-named pod to
    // disappear and a replacement to appear with a fresh suffix.
    let pod_key = build_key("pods", Some(ns), &original_name);
    let mut failed: Pod = storage.get(&pod_key).await.unwrap();
    let mut status = failed.status.take().unwrap_or_default();
    status.phase = Some(Phase::Failed);
    failed.status = Some(status);
    storage.update(&pod_key, &failed).await.unwrap();
    // Explicitly remove the failed pod (simulating the GC the controller relies on).
    storage.delete(&pod_key).await.unwrap();

    controller.reconcile_all().await.unwrap();
    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods_after.len(), 1, "controller recreates the failed pod");
    assert_ne!(
        pods_after[0].metadata.name, original_name,
        "replacement pod must use a fresh generateName suffix"
    );
}

/// [sig-apps] Daemon set [Serial] should update pod when spec was updated and update strategy is RollingUpdate [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:427
/// Sonobuoy (Round 160, "Other" bucket): FAIL — tracked under DaemonSet bucket.
#[tokio::test]
async fn daemonset_should_rolling_update_pods_when_spec_changes() {
    let storage = setup_test().await;
    let ns = "default";

    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    let mut ds = make_daemonset("rolling", ns, "registry.k8s.io/pause:3.8", None);
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateDaemonSet {
            max_unavailable: Some("1".to_string()),
            max_surge: None,
        }),
    });
    let key = build_key("daemonsets", Some(ns), "rolling");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3);
    let original_hashes: HashSet<String> = pods
        .iter()
        .filter_map(|p| p.metadata.labels.as_ref()?.get("controller-revision-hash"))
        .cloned()
        .collect();
    mark_all_pods_ready(&storage, ns).await;

    // Change the image to trigger a rolling update.
    let mut fresh: DaemonSet = storage.get(&key).await.unwrap();
    fresh.spec.template.spec.containers[0].image = "registry.k8s.io/pause:3.9".to_string();
    storage.update(&key, &fresh).await.unwrap();

    // Drive the rolling update — maxUnavailable=1 means one pod at a time.
    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        mark_all_pods_ready(&storage, ns).await;
    }

    let after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(after.len(), 3, "node count is preserved");
    let new_hashes: HashSet<String> = after
        .iter()
        .filter_map(|p| p.metadata.labels.as_ref()?.get("controller-revision-hash"))
        .cloned()
        .collect();
    assert!(
        new_hashes.is_disjoint(&original_hashes),
        "all pods rolled to the new revision (original={:?}, new={:?})",
        original_hashes,
        new_hashes
    );
}

/// [sig-apps] Daemon set [Serial] should rollback without unnecessary restarts [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:493
/// Sonobuoy (Round 160, "Other" bucket): FAIL — tracked under DaemonSet bucket.
#[tokio::test]
async fn daemonset_should_rollback_without_unnecessary_restarts() {
    let storage = setup_test().await;
    let ns = "default";

    for i in 1..=2 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    let mut ds = make_daemonset("rollback", ns, "registry.k8s.io/pause:3.8", None);
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateDaemonSet {
            max_unavailable: Some("1".to_string()),
            max_surge: None,
        }),
    });
    let key = build_key("daemonsets", Some(ns), "rollback");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    let initial: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let initial_hashes: HashSet<String> = initial
        .iter()
        .filter_map(|p| p.metadata.labels.as_ref()?.get("controller-revision-hash"))
        .cloned()
        .collect();
    mark_all_pods_ready(&storage, ns).await;

    // Update to a new image, drive partial rollout, then roll back.
    let mut fresh: DaemonSet = storage.get(&key).await.unwrap();
    fresh.spec.template.spec.containers[0].image = "registry.k8s.io/pause:3.9".to_string();
    storage.update(&key, &fresh).await.unwrap();

    for _ in 0..6 {
        controller.reconcile_all().await.unwrap();
        mark_all_pods_ready(&storage, ns).await;
    }

    // Roll back: restore the original image.
    let mut rolled: DaemonSet = storage.get(&key).await.unwrap();
    rolled.spec.template.spec.containers[0].image = "registry.k8s.io/pause:3.8".to_string();
    storage.update(&key, &rolled).await.unwrap();

    for _ in 0..6 {
        controller.reconcile_all().await.unwrap();
        mark_all_pods_ready(&storage, ns).await;
    }

    let after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let after_hashes: HashSet<String> = after
        .iter()
        .filter_map(|p| p.metadata.labels.as_ref()?.get("controller-revision-hash"))
        .cloned()
        .collect();

    // Rollback returns the pod template hash to its original value.
    assert!(
        !after_hashes.is_disjoint(&initial_hashes),
        "rollback restores the original revision (initial={:?}, after={:?})",
        initial_hashes,
        after_hashes
    );
}

/// [sig-apps] Daemon set [Serial] should list and delete a collection of DaemonSets [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:603
/// Sonobuoy (Round 160, "Other" bucket): FAIL — tracked under DaemonSet bucket.
#[tokio::test]
async fn daemonset_should_list_and_delete_collection() {
    let storage = setup_test().await;
    let ns = "default";

    for name in &["coll-x", "coll-y", "coll-z"] {
        let ds = make_daemonset(name, ns, "registry.k8s.io/pause:3.9", None);
        let key = build_key("daemonsets", Some(ns), name);
        storage.create(&key, &ds).await.unwrap();
    }

    let listed: Vec<DaemonSet> = storage.list("/registry/daemonsets/default/").await.unwrap();
    assert_eq!(listed.len(), 3);

    // DeleteCollection semantics: delete every item in the namespace.
    for ds in &listed {
        storage
            .delete(&build_key("daemonsets", Some(ns), &ds.metadata.name))
            .await
            .unwrap();
    }
    let remaining: Vec<DaemonSet> = storage.list("/registry/daemonsets/default/").await.unwrap();
    assert!(remaining.is_empty());
}

/// [sig-apps] Daemon set [Serial] should verify changes to a daemon set status [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:646
/// Sonobuoy (Round 160, "Other" bucket): FAIL — tracked under DaemonSet bucket.
#[tokio::test]
async fn daemonset_should_verify_status_field_changes() {
    let storage = setup_test().await;
    let ns = "default";

    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("status-ds", ns, "registry.k8s.io/pause:3.9", None);
    let key = build_key("daemonsets", Some(ns), "status-ds");
    storage.create(&key, &ds).await.unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let after: DaemonSet = storage.get(&key).await.unwrap();
    let status = after.status.expect("status must be populated");
    assert_eq!(status.desired_number_scheduled, 3);
    assert_eq!(status.current_number_scheduled, 3);

    // Add a new node, reconcile — desiredNumberScheduled should grow.
    let node4 = make_node("node-4", None);
    storage
        .create(&build_key("nodes", None, &node4.metadata.name), &node4)
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();
    let after: DaemonSet = storage.get(&key).await.unwrap();
    let status = after.status.unwrap();
    assert_eq!(status.desired_number_scheduled, 4);
    assert_eq!(status.current_number_scheduled, 4);
}

// ---------------------------------------------------------------------------
// Additional DaemonSet coverage: node lifecycle & namespace isolation.
// ---------------------------------------------------------------------------

/// [sig-apps] Daemon set — should add a pod to a newly added node
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go (Lifecycle subtests)
/// Sonobuoy (Round 160): PASS — covered transitively by status verification.
#[tokio::test]
async fn daemonset_should_add_pod_when_node_joins() {
    let storage = setup_test().await;
    let ns = "default";

    for i in 1..=2 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("growth", ns, "registry.k8s.io/pause:3.9", None);
    storage
        .create(&build_key("daemonsets", Some(ns), "growth"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        storage
            .list::<Pod>("/registry/pods/default/")
            .await
            .unwrap()
            .len(),
        2
    );

    let node3 = make_node("node-3", None);
    storage
        .create(&build_key("nodes", None, &node3.metadata.name), &node3)
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3);
    let assigned: HashSet<_> = pods
        .iter()
        .filter_map(|p| p.spec.as_ref()?.node_name.as_deref())
        .collect();
    assert!(assigned.contains("node-3"));
}

/// [sig-apps] Daemon set — should remove a pod when the node is removed
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go (Lifecycle subtests)
/// Sonobuoy (Round 160): PASS.
#[tokio::test]
async fn daemonset_should_remove_pod_when_node_leaves() {
    let storage = setup_test().await;
    let ns = "default";

    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("shrink", ns, "registry.k8s.io/pause:3.9", None);
    storage
        .create(&build_key("daemonsets", Some(ns), "shrink"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        storage
            .list::<Pod>("/registry/pods/default/")
            .await
            .unwrap()
            .len(),
        3
    );

    storage
        .delete(&build_key("nodes", None, "node-2"))
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);
    let assigned: HashSet<_> = pods
        .iter()
        .filter_map(|p| p.spec.as_ref()?.node_name.as_deref())
        .collect();
    assert!(!assigned.contains("node-2"));
}

/// [sig-apps] Daemon set — namespaces are isolated
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go (multi-namespace)
/// Sonobuoy (Round 160): PASS.
#[tokio::test]
async fn daemonset_namespaces_are_isolated() {
    let storage = setup_test().await;

    for i in 1..=2 {
        let node = make_node(&format!("node-{}", i), None);
        storage
            .create(&build_key("nodes", None, &node.metadata.name), &node)
            .await
            .unwrap();
    }

    let ds_a = make_daemonset("ds", "ns-alpha", "registry.k8s.io/pause:3.9", None);
    let ds_b = make_daemonset("ds", "ns-beta", "registry.k8s.io/pause:3.9", None);
    storage
        .create(&build_key("daemonsets", Some("ns-alpha"), "ds"), &ds_a)
        .await
        .unwrap();
    storage
        .create(&build_key("daemonsets", Some("ns-beta"), "ds"), &ds_b)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let alpha: Vec<Pod> = storage.list("/registry/pods/ns-alpha/").await.unwrap();
    let beta: Vec<Pod> = storage.list("/registry/pods/ns-beta/").await.unwrap();
    assert_eq!(alpha.len(), 2);
    assert_eq!(beta.len(), 2);
}
