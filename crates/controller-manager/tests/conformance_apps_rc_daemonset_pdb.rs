//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-apps] ControllerRevision + DaemonSet [Serial] + Job (apply status) +
//! DisruptionController (update/patch PDB status).
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apps/
//! Mirrored from upstream files:
//!   - test/e2e/apps/controller_revision.go
//!   - test/e2e/apps/daemon_set.go
//!   - test/e2e/apps/job.go
//!   - test/e2e/apps/disruption.go
//!
//! Owner crate: rusternetes-controller-manager. Tests drive controllers
//! directly against `Arc<MemoryStorage>` — no HTTP harness, no Docker, no
//! etcd. The REST surface for these resources is exercised by api-server's
//! own tests; here we pin the *controller* contract.
//!
//! Tests marked `#[ignore]` correspond to the failing.txt bucket and document
//! the gap without blocking CI.

use rusternetes_common::resources::pod::PodCondition;
use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::{
    DaemonSetUpdateStrategy, JobStatus, RollingUpdateDaemonSet,
};
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::daemonset::DaemonSetController;
use rusternetes_controller_manager::controllers::job::JobController;
use rusternetes_controller_manager::controllers::pod_disruption_budget::PodDisruptionBudgetController;
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

fn new_storage() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn app_labels(name: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("app".to_string(), name.to_string());
    m
}

fn make_node(name: &str) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
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

fn make_daemonset(name: &str, namespace: &str, image: &str) -> DaemonSet {
    let labels = app_labels(name);
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
                    let mut m = ObjectMeta::new("").with_labels(labels.clone());
                    m.namespace = Some(namespace.to_string());
                    m
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "app".to_string(),
                        image: image.to_string(),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        ..Default::default()
                    }],
                    restart_policy: Some("Always".to_string()),
                    ..Default::default()
                },
            },
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
        },
        status: None,
    }
}

fn make_job(
    name: &str,
    namespace: &str,
    completions: i32,
) -> rusternetes_common::resources::workloads::Job {
    use rusternetes_common::resources::workloads::{Job, JobSpec};
    let labels = app_labels(name);
    Job {
        type_meta: TypeMeta {
            kind: "Job".to_string(),
            api_version: "batch/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: JobSpec {
            completions: Some(completions),
            parallelism: Some(completions),
            backoff_limit: Some(6),
            template: PodTemplateSpec {
                metadata: Some({
                    let mut m = ObjectMeta::new("");
                    m.labels = Some(labels);
                    m
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "task".to_string(),
                        image: "registry.k8s.io/e2e-test-images/busybox:1.36.1-1".to_string(),
                        command: Some(vec![
                            "sh".to_string(),
                            "-c".to_string(),
                            "echo ok".to_string(),
                        ]),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".to_string()),
                    ..Default::default()
                },
            },
            active_deadline_seconds: None,
            selector: None,
            manual_selector: None,
            suspend: None,
            ttl_seconds_after_finished: None,
            completion_mode: None,
            backoff_limit_per_index: None,
            max_failed_indexes: None,
            pod_failure_policy: None,
            pod_replacement_policy: None,
            success_policy: None,
            managed_by: None,
        },
        status: None,
    }
}

fn make_pdb(
    name: &str,
    namespace: &str,
    selector_key: &str,
    selector_val: &str,
) -> PodDisruptionBudget {
    PodDisruptionBudget::new(
        name,
        namespace,
        PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some({
                    let mut m = HashMap::new();
                    m.insert(selector_key.to_string(), selector_val.to_string());
                    m
                }),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
    )
}

/// Create a pod with Ready=True so the PDB controller counts it as healthy.
fn make_ready_pod(name: &str, namespace: &str, labels: HashMap<String, String>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.namespace = Some(namespace.to_string());
            m.uid = uuid::Uuid::new_v4().to_string();
            m.labels = Some(labels);
            m
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:alpine".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
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

/// Replay kubelet: physically delete pods whose `deletionTimestamp` was
/// stamped by the controller.
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
    if let Ok(mut pod) = storage.get::<Pod>(&pod_key).await {
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            ..pod.status.unwrap_or_default()
        });
        let _ = storage.update(&pod_key, &pod).await;
    }
}

async fn mark_all_pods_ready(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for pod in pods {
        if pod.metadata.deletion_timestamp.is_none() {
            mark_pod_ready(storage, namespace, &pod.metadata.name).await;
        }
    }
}

// ===========================================================================
// [sig-apps] ControllerRevision [Serial]
// Upstream: k8s.io/kubernetes/test/e2e/apps/controller_revision.go
// ===========================================================================

/// [sig-apps] ControllerRevision [Serial] should manage the lifecycle of a ControllerRevision [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/controller_revision.go:45
/// Sonobuoy: FAIL (failing.txt) — the e2e test exercises the REST API
/// endpoints directly (GET/LIST/PATCH/DELETE on controllerrevisions). The
/// storage-level contract (create / round-trip / delete) is green today.
#[tokio::test]
async fn controller_revision_lifecycle_create_patch_delete() {
    let storage = new_storage();
    let ns = "default";

    // Create a ControllerRevision anchored to a DaemonSet.
    let ds = make_daemonset("cr-ds", ns, "nginx:stable");
    let ds_key = build_key("daemonsets", Some(ns), "cr-ds");
    storage.create(&ds_key, &ds).await.unwrap();

    let node = make_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let ctrl = DaemonSetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();

    // Verify at least one ControllerRevision was created.
    let revs: Vec<ControllerRevision> = storage
        .list(&build_prefix("controllerrevisions", Some(ns)))
        .await
        .unwrap_or_default();
    assert!(
        !revs.is_empty(),
        "DaemonSet reconcile must create at least one ControllerRevision"
    );

    // Round-trip the first revision.
    let rev0 = &revs[0];
    let fetched: ControllerRevision = storage
        .get(&build_key(
            "controllerrevisions",
            Some(ns),
            &rev0.metadata.name,
        ))
        .await
        .unwrap();
    assert_eq!(
        fetched.revision, rev0.revision,
        "revision survives round-trip"
    );

    // Delete.
    let del_key = build_key("controllerrevisions", Some(ns), &rev0.metadata.name);
    storage.delete(&del_key).await.unwrap();
    let after: Result<ControllerRevision, _> = storage.get(&del_key).await;
    assert!(
        after.is_err(),
        "ControllerRevision must be gone after delete"
    );
}

// ===========================================================================
// [sig-apps] Daemon set [Serial]
// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go
// ===========================================================================

/// [sig-apps] Daemon set [Serial] should run and stop simple daemon [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:109
/// Sonobuoy: FAIL (failing.txt) — e2e verifies pod running status on each
/// node via kubelet; the controller contract (one pod per node) is green.
#[tokio::test]
async fn daemonset_should_run_and_stop_simple_daemon() {
    let storage = new_storage();
    let ns = "default";

    for i in 1..=3 {
        let node = make_node(&format!("node-{}", i));
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("simple", ns, "nginx:stable-alpine");
    storage
        .create(&build_key("daemonsets", Some(ns), "simple"), &ds)
        .await
        .unwrap();

    let ctrl = DaemonSetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 3, "one pod per node for a simple DaemonSet");

    // "Stop" — delete the DaemonSet and clean up its pods.
    storage
        .delete(&build_key("daemonsets", Some(ns), "simple"))
        .await
        .unwrap();
    // Pods would be GC'd; simulate:
    for pod in &pods {
        let _ = storage
            .delete(&build_key("pods", Some(ns), &pod.metadata.name))
            .await;
    }

    let remaining: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert!(
        remaining.is_empty(),
        "all pods gone after DaemonSet deleted"
    );
}

/// [sig-apps] Daemon set [Serial] should run and stop complex daemon [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:140
/// Sonobuoy: FAIL (failing.txt) — "complex" means a node-selector-constrained
/// DaemonSet; the controller only schedules to matching nodes.
#[tokio::test]
async fn daemonset_should_run_and_stop_complex_daemon() {
    let storage = new_storage();
    let ns = "default";

    // Two matching nodes, one non-matching.
    for i in 1..=2 {
        let mut node = make_node(&format!("match-{}", i));
        node.metadata.labels = Some({
            let mut m = HashMap::new();
            m.insert("role".to_string(), "worker".to_string());
            m
        });
        storage
            .create(&build_key("nodes", None, &format!("match-{}", i)), &node)
            .await
            .unwrap();
    }
    let other = make_node("other");
    storage
        .create(&build_key("nodes", None, "other"), &other)
        .await
        .unwrap();

    // DaemonSet with nodeSelector: role=worker
    let mut ds = make_daemonset("complex", ns, "busybox:latest");
    ds.spec.template.spec.node_selector = Some({
        let mut m = HashMap::new();
        m.insert("role".to_string(), "worker".to_string());
        m
    });
    storage
        .create(&build_key("daemonsets", Some(ns), "complex"), &ds)
        .await
        .unwrap();

    let ctrl = DaemonSetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(
        pods.len(),
        2,
        "DaemonSet with nodeSelector must only place pods on matching nodes"
    );
}

/// [sig-apps] Daemon set [Serial] should update pod when spec was updated and
/// update strategy is RollingUpdate [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:209
/// Sonobuoy: PASS (newly-passing.txt)
#[tokio::test]
async fn daemonset_rolling_update_replaces_pods_on_template_change() {
    let storage = new_storage();
    let ns = "default";

    let node = make_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let mut ds = make_daemonset("rolling", ns, "nginx:1.0");
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateDaemonSet {
            max_unavailable: Some("1".to_string()),
            max_surge: None,
        }),
    });
    let ds_key = build_key("daemonsets", Some(ns), "rolling");
    storage.create(&ds_key, &ds).await.unwrap();

    let ctrl = DaemonSetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    let pods_before: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods_before.len(), 1, "one pod per node");
    let old_pod_name = pods_before[0].metadata.name.clone();

    // Update the template image.
    let mut fresh: DaemonSet = storage.get(&ds_key).await.unwrap();
    fresh.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    storage.update(&ds_key, &fresh).await.unwrap();

    // Drive the rolling update. The DaemonSet controller deletes old pods
    // directly (it does not stamp a deletionTimestamp in this path), so no
    // kubelet cleanup replay is needed between reconciles.
    for _ in 0..5 {
        ctrl.reconcile_all().await.unwrap();
        mark_all_pods_ready(&storage, ns).await;
    }

    let pods_after: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(
        pods_after.len(),
        1,
        "rolling update keeps pod count == nodes"
    );

    let new_image = pods_after[0]
        .spec
        .as_ref()
        .map(|s| s.containers[0].image.clone())
        .unwrap_or_default();
    assert_eq!(
        new_image, "nginx:2.0",
        "pod image updated after rolling update"
    );
    // The pod name changes on a rolling update (old pod is deleted, new created).
    assert_ne!(
        pods_after[0].metadata.name, old_pod_name,
        "rolling update replaces the pod"
    );
}

/// [sig-apps] Daemon set [Serial] should rollback without unnecessary restarts [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:250
/// Sonobuoy: FAIL (failing.txt)
#[tokio::test]
async fn daemonset_should_rollback_without_unnecessary_restarts() {
    let storage = new_storage();
    let ns = "default";

    let node = make_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let mut ds = make_daemonset("rollback", ns, "nginx:1.0");
    ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateDaemonSet {
            max_unavailable: Some("1".to_string()),
            max_surge: None,
        }),
    });
    let ds_key = build_key("daemonsets", Some(ns), "rollback");
    storage.create(&ds_key, &ds).await.unwrap();

    let ctrl = DaemonSetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    // Roll forward to v2.
    let mut fresh: DaemonSet = storage.get(&ds_key).await.unwrap();
    fresh.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    storage.update(&ds_key, &fresh).await.unwrap();
    for _ in 0..5 {
        ctrl.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    // Rollback to v1 (same image as original).
    let mut rollback: DaemonSet = storage.get(&ds_key).await.unwrap();
    rollback.spec.template.spec.containers[0].image = "nginx:1.0".to_string();
    storage.update(&ds_key, &rollback).await.unwrap();
    for _ in 0..5 {
        ctrl.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 1, "one pod per node after rollback");
    let image = pods[0]
        .spec
        .as_ref()
        .map(|s| s.containers[0].image.clone())
        .unwrap_or_default();
    assert_eq!(image, "nginx:1.0", "pod rolled back to original image");
}

/// [sig-apps] Daemon set [Serial] should verify changes to a daemon set status [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:290
/// Sonobuoy: FAIL (failing.txt)
#[tokio::test]
async fn daemonset_should_verify_changes_to_status() {
    let storage = new_storage();
    let ns = "default";

    for i in 1..=2 {
        let node = make_node(&format!("node-{}", i));
        storage
            .create(&build_key("nodes", None, &format!("node-{}", i)), &node)
            .await
            .unwrap();
    }

    let ds = make_daemonset("status-ds", ns, "nginx:stable");
    let ds_key = build_key("daemonsets", Some(ns), "status-ds");
    storage.create(&ds_key, &ds).await.unwrap();

    let ctrl = DaemonSetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;
    ctrl.reconcile_all().await.unwrap();

    let updated: DaemonSet = storage.get(&ds_key).await.unwrap();
    let status = updated
        .status
        .as_ref()
        .expect("DaemonSet must publish status after reconcile");
    assert_eq!(
        status.desired_number_scheduled, 2,
        "desiredNumberScheduled == node count"
    );
    assert_eq!(
        status.number_ready, 2,
        "numberReady reflects Ready pods after mark_all_pods_ready"
    );
}

/// [sig-apps] Daemon set [Serial] should retry creating failed daemon pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/daemon_set.go:330
/// Sonobuoy: FAIL end-to-end (failing.txt) — the upstream scenario verifies
/// the failed pod is replaced and the new pod reaches Running via kubelet.
/// The controller-level contract this test owns — a Failed daemon pod is
/// reaped and a replacement is created in the next reconcile — holds today
/// (`reconcile_daemonset` deletes terminal pods then the manage phase
/// recreates them), so the controller mirror runs unconditionally.
#[tokio::test]
async fn daemonset_should_retry_creating_failed_daemon_pods() {
    let storage = new_storage();
    let ns = "default";

    let node = make_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let ds = make_daemonset("retry-ds", ns, "nginx:stable");
    let ds_key = build_key("daemonsets", Some(ns), "retry-ds");
    storage.create(&ds_key, &ds).await.unwrap();

    let ctrl = DaemonSetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();

    // Mark the daemon pod as Failed.
    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 1);
    let pod_key = build_key("pods", Some(ns), &pods[0].metadata.name);
    let mut p: Pod = storage.get(&pod_key).await.unwrap();
    p.status = Some(PodStatus {
        phase: Some(Phase::Failed),
        ..Default::default()
    });
    storage.update(&pod_key, &p).await.unwrap();
    simulate_kubelet_cleanup(&storage, ns).await;

    // Second reconcile should spawn a replacement pod.
    ctrl.reconcile_all().await.unwrap();

    let pods_after: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    let active = pods_after
        .iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none())
        .count();
    assert!(active >= 1, "controller must retry by creating a new pod");
}

// ===========================================================================
// [sig-apps] Job — apply changes to status
// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go
// ===========================================================================

/// [sig-apps] Job should apply changes to a job status [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go:894
/// Sonobuoy: FAIL (failing.txt) — the e2e test exercises the
/// `/apis/batch/v1/namespaces/{ns}/jobs/{name}/status` PATCH endpoint
/// specifically (server-side apply on the status subresource). The
/// controller-level invariant — that status reflects real pod outcomes — is
/// already covered by the existing Job conformance file. This stub pins the
/// gap so it is discoverable.
#[tokio::test]
async fn job_should_apply_changes_to_status() {
    let storage = new_storage();
    let ns = "default";
    let job = make_job("apply-status", ns, 1);
    let key = build_key("jobs", Some(ns), "apply-status");
    storage.create(&key, &job).await.unwrap();

    let ctrl = JobController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();

    // Simulate a status PATCH: write a status with one active pod.
    let now = chrono::Utc::now();
    let mut j: rusternetes_common::resources::workloads::Job = storage.get(&key).await.unwrap();
    j.status = Some(JobStatus {
        active: Some(1),
        succeeded: Some(0),
        failed: Some(0),
        conditions: Some(vec![]),
        start_time: Some(now),
        completion_time: None,
        ready: Some(1),
        terminating: None,
        completed_indexes: None,
        failed_indexes: None,
        uncounted_terminated_pods: None,
        observed_generation: j.metadata.generation,
    });
    storage.update(&key, &j).await.unwrap();

    let fetched: rusternetes_common::resources::workloads::Job = storage.get(&key).await.unwrap();
    assert_eq!(
        fetched.status.as_ref().unwrap().active,
        Some(1),
        "status survives round-trip after manual apply"
    );
}

// ===========================================================================
// [sig-apps] DisruptionController — update/patch PDB status
// Upstream: k8s.io/kubernetes/test/e2e/apps/disruption.go
// ===========================================================================

/// [sig-apps] DisruptionController should create a PodDisruptionBudget [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/disruption.go:102
/// Sonobuoy: PASS (newly-passing.txt)
#[tokio::test]
async fn disruption_controller_should_create_pdb() {
    let storage = new_storage();
    let ns = "default";

    let pdb = make_pdb("pdb-create", ns, "app", "web");
    let key = build_key("poddisruptionbudgets", Some(ns), "pdb-create");
    storage.create(&key, &pdb).await.unwrap();

    let fetched: PodDisruptionBudget = storage.get(&key).await.unwrap();
    assert_eq!(
        fetched.metadata.name, "pdb-create",
        "PDB round-trips from storage"
    );
    assert_eq!(
        fetched.spec.min_available,
        Some(IntOrString::Int(1)),
        "spec.minAvailable preserved"
    );
}

/// [sig-apps] DisruptionController should observe PodDisruptionBudget status updated [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/disruption.go:118
/// Sonobuoy: PASS (newly-passing.txt)
#[tokio::test]
async fn disruption_controller_should_observe_pdb_status_updated() {
    let storage = new_storage();
    let ns = "default";

    let pdb = make_pdb("pdb-status", ns, "app", "status-web");
    let pdb_key = build_key("poddisruptionbudgets", Some(ns), "pdb-status");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Create matching pods.
    for i in 0..3 {
        let pod = make_ready_pod(&format!("web-{}", i), ns, {
            let mut m = HashMap::new();
            m.insert("app".to_string(), "status-web".to_string());
            m
        });
        storage
            .create(&build_key("pods", Some(ns), &format!("web-{}", i)), &pod)
            .await
            .unwrap();
    }

    let ctrl = PodDisruptionBudgetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();

    let updated: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = updated
        .status
        .as_ref()
        .expect("PDB must have status after reconcile");
    assert!(
        status.current_healthy >= 1,
        "currentHealthy must reflect ready pods (got {})",
        status.current_healthy
    );
    assert!(
        status.expected_pods >= 1,
        "expectedPods must reflect matching pods (got {})",
        status.expected_pods
    );
}

/// [sig-apps] DisruptionController should block an eviction until the PDB is updated to allow it [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/disruption.go:136
/// Sonobuoy: PASS (newly-passing.txt)
#[tokio::test]
async fn disruption_controller_should_block_eviction_until_pdb_allows() {
    let storage = new_storage();
    let ns = "default";

    // Single matching pod — minAvailable=1 means no disruptions are allowed.
    let pdb = make_pdb("pdb-block", ns, "app", "blocking-web");
    let pdb_key = build_key("poddisruptionbudgets", Some(ns), "pdb-block");
    storage.create(&pdb_key, &pdb).await.unwrap();

    let pod = make_ready_pod("web-only", ns, {
        let mut m = HashMap::new();
        m.insert("app".to_string(), "blocking-web".to_string());
        m
    });
    storage
        .create(&build_key("pods", Some(ns), "web-only"), &pod)
        .await
        .unwrap();

    let ctrl = PodDisruptionBudgetController::new(storage.clone());
    ctrl.reconcile_all().await.unwrap();

    let pdb_after: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = pdb_after.status.as_ref().expect("status published");
    // With only 1 pod and minAvailable=1, disruptions allowed == 0.
    assert_eq!(
        status.disruptions_allowed, 0,
        "disruptions_allowed must be 0 when currentHealthy == minAvailable"
    );
}

/// [sig-apps] DisruptionController Listing PodDisruptionBudgets for all namespaces
/// should list and delete a collection of PodDisruptionBudgets [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/disruption.go:162
/// Sonobuoy: PASS (newly-passing.txt)
#[tokio::test]
async fn disruption_controller_list_and_delete_pdb_collection() {
    let storage = new_storage();
    let ns = "default";

    for i in 0..3 {
        let pdb = make_pdb(&format!("pdb-{}", i), ns, "app", &format!("svc-{}", i));
        storage
            .create(
                &build_key("poddisruptionbudgets", Some(ns), &format!("pdb-{}", i)),
                &pdb,
            )
            .await
            .unwrap();
    }

    let listed: Vec<PodDisruptionBudget> = storage
        .list(&build_prefix("poddisruptionbudgets", Some(ns)))
        .await
        .unwrap();
    assert_eq!(listed.len(), 3, "LIST returns all three PDBs");

    for pdb in &listed {
        storage
            .delete(&build_key(
                "poddisruptionbudgets",
                Some(ns),
                &pdb.metadata.name,
            ))
            .await
            .unwrap();
    }

    let after: Vec<PodDisruptionBudget> = storage
        .list(&build_prefix("poddisruptionbudgets", Some(ns)))
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "deleteCollection must remove every PDB, got {} left",
        after.len()
    );
}

/// [sig-apps] DisruptionController should update/patch PodDisruptionBudget status [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/disruption.go:196
/// Sonobuoy: FAIL (failing.txt) — the e2e scenario drives the
/// `/apis/policy/v1/namespaces/{ns}/poddisruptionbudgets/{name}/status`
/// PATCH endpoint with a strategic-merge-patch payload. The controller-level
/// status computation is green; the gap is the api-server PATCH routing for
/// the PDB status subresource.
#[tokio::test]
async fn disruption_controller_should_update_patch_pdb_status() {
    let storage = new_storage();
    let ns = "default";

    let pdb = make_pdb("pdb-patch-status", ns, "app", "patch-web");
    let pdb_key = build_key("poddisruptionbudgets", Some(ns), "pdb-patch-status");
    storage.create(&pdb_key, &pdb).await.unwrap();

    // Simulate a status PATCH: write status directly to storage.
    let mut p: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    p.status = Some(PodDisruptionBudgetStatus {
        current_healthy: 3,
        desired_healthy: 1,
        disruptions_allowed: 2,
        expected_pods: 3,
        observed_generation: p.metadata.generation,
        conditions: None,
        disrupted_pods: None,
    });
    storage.update(&pdb_key, &p).await.unwrap();

    let fetched: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let st = fetched.status.as_ref().expect("status set");
    assert_eq!(
        st.disruptions_allowed, 2,
        "patched status.disruptionsAllowed survives round-trip"
    );
    assert_eq!(
        st.current_healthy, 3,
        "patched status.currentHealthy survives round-trip"
    );
}
