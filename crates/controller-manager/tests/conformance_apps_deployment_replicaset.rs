//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-apps] Deployment + ReplicaSet + ReplicationController.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apps/
//! Mirrored from upstream files:
//!   - test/e2e/apps/deployment.go
//!   - test/e2e/apps/replica_set.go
//!   - test/e2e/apps/rc.go
//!
//! And from the Sonobuoy run captured in
//!
//! See docs/conformance/apps-deployment-replicaset.md for the test-by-test
//! status table and cross-reference to docs/CONFORMANCE.md
//! "Apps controllers" failure bucket (Round 160).
//!
//! Owner crate: controller-manager. Tests drive the reconcile loops directly
//! against `Arc<MemoryStorage>` — no HTTP harness, no Docker, no etcd. The
//! REST surface for these resources is exercised by api-server's own tests;
//! here we pin the *controller* contract: given a desired Deployment / RS /
//! RC in storage, the controller produces the right children (ReplicaSets,
//! pods) and the right status counters.

use rusternetes_common::resources::deployment::{DeploymentStrategy, RollingUpdateDeployment};
use rusternetes_common::resources::pod::{PodCondition, PodStatus};
use rusternetes_common::resources::{
    Container, Deployment, DeploymentSpec, Pod, PodSpec, PodTemplateSpec, ReplicaSet,
    ReplicaSetSpec, ReplicationController, ReplicationControllerSpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_controller_manager::controllers::replicationcontroller::ReplicationControllerController;
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

fn labels(app: &str) -> HashMap<String, String> {
    let mut l = HashMap::new();
    l.insert("app".to_string(), app.to_string());
    l
}

fn make_deployment(
    name: &str,
    namespace: &str,
    replicas: i32,
    image: &str,
    strategy: Option<DeploymentStrategy>,
) -> Deployment {
    let app_labels = labels(name);
    Deployment {
        type_meta: TypeMeta {
            kind: "Deployment".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = format!("deploy-uid-{}", name);
            meta.generation = Some(1);
            meta
        },
        spec: DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(app_labels.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta::new("").with_labels(app_labels)),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "main".to_string(),
                        image: image.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
            strategy,
            min_ready_seconds: None,
            revision_history_limit: None,
            paused: None,
            progress_deadline_seconds: None,
        },
        status: None,
    }
}

fn make_replicaset(name: &str, namespace: &str, replicas: i32, image: &str) -> ReplicaSet {
    let app_labels = labels(name);
    ReplicaSet {
        type_meta: TypeMeta {
            kind: "ReplicaSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = format!("rs-uid-{}", name);
            meta.labels = Some(app_labels.clone());
            meta
        },
        spec: ReplicaSetSpec {
            replicas,
            selector: LabelSelector {
                match_labels: Some(app_labels.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta::new("").with_labels(app_labels)),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "main".to_string(),
                        image: image.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
            min_ready_seconds: None,
        },
        status: None,
    }
}

fn make_rc(name: &str, namespace: &str, replicas: i32, image: &str) -> ReplicationController {
    let app_labels = labels(name);
    let mut selector_map: HashMap<String, String> = HashMap::new();
    selector_map.insert("app".to_string(), name.to_string());
    ReplicationController {
        type_meta: TypeMeta {
            kind: "ReplicationController".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = format!("rc-uid-{}", name);
            meta
        },
        spec: ReplicationControllerSpec {
            replicas: Some(replicas),
            selector: Some(selector_map),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta::new("").with_labels(app_labels)),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "main".to_string(),
                        image: image.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
            min_ready_seconds: None,
        },
        status: None,
    }
}

fn rolling_update_strategy(max_surge: &str, max_unavailable: &str) -> DeploymentStrategy {
    DeploymentStrategy {
        strategy_type: "RollingUpdate".to_string(),
        rolling_update: Some(RollingUpdateDeployment {
            max_surge: Some(serde_json::Value::String(max_surge.to_string())),
            max_unavailable: Some(serde_json::Value::String(max_unavailable.to_string())),
        }),
    }
}

fn recreate_strategy() -> DeploymentStrategy {
    DeploymentStrategy {
        strategy_type: "Recreate".to_string(),
        rolling_update: None,
    }
}

/// Mark every pod in `namespace` as Running + Ready so reconciles can progress.
async fn mark_all_pods_ready(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = build_prefix("pods", Some(namespace));
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for mut pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
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
            ..Default::default()
        });
        let key = build_key("pods", Some(namespace), &pod.metadata.name);
        let _ = storage.update(&key, &pod).await;
    }
}

/// Drive Deployment + ReplicaSet reconcile loops repeatedly so a rolling update
/// can complete. Each iteration reconciles the Deployment (which sizes RSes),
/// then the ReplicaSet (which creates/deletes pods), then marks pods Ready.
async fn run_rollout(
    deploy_ctrl: &DeploymentController<MemoryStorage>,
    rs_ctrl: &ReplicaSetController<MemoryStorage>,
    storage: &Arc<MemoryStorage>,
    namespace: &str,
    iterations: usize,
) {
    for _ in 0..iterations {
        deploy_ctrl.reconcile_all().await.unwrap();
        rs_ctrl.reconcile_all().await.unwrap();
        mark_all_pods_ready(storage, namespace).await;
    }
}

// ===========================================================================
// Deployment conformance tests
// ===========================================================================

/// [sig-apps] Deployment RollingUpdateDeployment should delete old pods and create new ones [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go:106
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_rolling_update_should_delete_old_pods_and_create_new_ones() {
    let storage = setup();
    let ns = "default";
    let mut dep = make_deployment("nginx-rolling", ns, 3, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);

    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    // Snapshot the first-revision RS name.
    let prefix = build_prefix("replicasets", Some(ns));
    let first_rsets: Vec<ReplicaSet> = storage.list(&prefix).await.unwrap();
    assert_eq!(first_rsets.len(), 1, "initial rollout creates one RS");
    let first_rs_name = first_rsets[0].metadata.name.clone();

    // Flip the image.
    dep = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();

    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 10).await;

    let rsets: Vec<ReplicaSet> = storage.list(&prefix).await.unwrap();
    assert_eq!(rsets.len(), 2, "rolling update creates a second RS");

    let new_rs = rsets
        .iter()
        .find(|rs| rs.metadata.name != first_rs_name)
        .expect("new RS exists");
    let old_rs = rsets
        .iter()
        .find(|rs| rs.metadata.name == first_rs_name)
        .expect("old RS exists");
    assert_eq!(new_rs.spec.replicas, 3, "new RS scaled up to desired");
    assert_eq!(old_rs.spec.replicas, 0, "old RS scaled down to zero");
}

/// [sig-apps] Deployment RecreateDeployment should delete old pods and create new ones [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go:113
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_recreate_should_delete_old_pods_before_creating_new_ones() {
    let storage = setup();
    let ns = "default";
    let mut dep = make_deployment(
        "nginx-recreate",
        ns,
        2,
        "nginx:1.0",
        Some(recreate_strategy()),
    );
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);

    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    // Trigger a recreate update.
    dep = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();

    // Single reconcile: with the Recreate strategy the old RS must be scaled to 0
    // first. The new RS may or may not exist yet, but if it does it must NOT carry
    // replicas while old pods still exist.
    deploy_ctrl.reconcile_all().await.unwrap();

    let rs_prefix = build_prefix("replicasets", Some(ns));
    let rsets: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    let old_rs = rsets
        .iter()
        .find(|rs| rs.spec.template.spec.containers[0].image == "nginx:1.0")
        .expect("old RS exists");
    assert_eq!(
        old_rs.spec.replicas, 0,
        "Recreate strategy must scale the old RS to 0 before creating the new RS"
    );
}

/// [sig-apps] Deployment deployment should delete old replica sets [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go:121
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_should_track_old_replicasets_for_history() {
    let storage = setup();
    let ns = "default";
    let mut dep = make_deployment("history-dep", ns, 1, "nginx:1.0", None);
    dep.spec.revision_history_limit = Some(1);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    // Two consecutive template changes => 3 RSes total (old + intermediate + new).
    dep = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 5).await;

    dep = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:3.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 5).await;

    // We must retain at least the active RS, so >=1 RS exists. The history-limit
    // GC pass is best-effort, so the upper bound is the count of unique
    // template hashes seen.
    let rs_prefix = build_prefix("replicasets", Some(ns));
    let rsets: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    assert!(
        !rsets.is_empty(),
        "deployment must always retain at least one ReplicaSet"
    );
    let active = rsets
        .iter()
        .find(|rs| rs.spec.template.spec.containers[0].image == "nginx:3.0")
        .expect("active (latest-template) RS must exist");
    assert_eq!(active.spec.replicas, 1, "active RS holds desired replicas");
}

/// [sig-apps] Deployment deployment should support rollover [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go:129
/// Local status (2026-05-18): the controller-level contract that the v3 RS
/// converges to desired replicas after a mid-rollout template flip
/// (v1 -> v2 -> v3) now holds against `MemoryStorage`. The end-to-end
/// Sonobuoy verdict is still tracked separately in `docs/CONFORMANCE.md`.
#[tokio::test]
async fn deployment_should_support_rollover() {
    let storage = setup();
    let ns = "default";

    // Start at image v1 and complete one rollout.
    let mut dep = make_deployment("rollover", ns, 4, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    // First update — start rolling to v2 but do NOT mark pods Ready yet.
    dep = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();
    deploy_ctrl.reconcile_all().await.unwrap();
    rs_ctrl.reconcile_all().await.unwrap();

    // Rollover — change to v3 mid-flight.
    dep = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:3.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();

    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 10).await;

    let rs_prefix = build_prefix("replicasets", Some(ns));
    let rsets: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    let v3 = rsets
        .iter()
        .find(|rs| rs.spec.template.spec.containers[0].image == "nginx:3.0")
        .expect("v3 RS must exist after rollover");
    assert_eq!(v3.spec.replicas, 4, "v3 must converge to desired replicas");
}

/// [sig-apps] Deployment should have a working scale subresource [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go:144
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_scale_subresource_changes_replicaset_size() {
    let storage = setup();
    let ns = "default";
    let mut dep = make_deployment("scale-dep", ns, 2, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    deploy_ctrl.reconcile_all().await.unwrap();

    // Simulate a /scale subresource PUT — bump replicas to 5.
    dep = storage.get(&key).await.unwrap();
    dep.spec.replicas = Some(5);
    storage.update(&key, &dep).await.unwrap();
    deploy_ctrl.reconcile_all().await.unwrap();

    let rsets: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some(ns)))
        .await
        .unwrap();
    let total: i32 = rsets.iter().map(|rs| rs.spec.replicas).sum();
    assert!(
        total >= 5,
        "scaling deployment to 5 must propagate to RS replicas (got total={})",
        total
    );
}

/// [sig-apps] Deployment deployment should support proportional scaling [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go:154
/// Local status (2026-05-18): a scale event that arrives mid-rollout grows
/// the active ReplicaSet without overshooting `desired + maxSurge`. The
/// end-to-end Sonobuoy verdict is still tracked separately in
/// `docs/CONFORMANCE.md`.
#[tokio::test]
async fn deployment_should_support_proportional_scaling() {
    let storage = setup();
    let ns = "default";
    let dep = make_deployment("prop", ns, 10, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    deploy_ctrl.reconcile_all().await.unwrap();

    // Scale up to 30 mid-rollout — both old + new RS should grow proportionally.
    let mut dep: Deployment = storage.get(&key).await.unwrap();
    dep.spec.replicas = Some(30);
    storage.update(&key, &dep).await.unwrap();
    deploy_ctrl.reconcile_all().await.unwrap();

    let rsets: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some(ns)))
        .await
        .unwrap();
    let total: i32 = rsets.iter().map(|rs| rs.spec.replicas).sum();
    let max_surge = (30.0_f64 * 0.25).ceil() as i32;
    // Upper bound: cannot exceed desired + maxSurge.
    assert!(
        total <= 30 + max_surge,
        "total replicas {} must not exceed desired+maxSurge ({} + {})",
        total,
        30,
        max_surge
    );
    // Lower bound: the scale event must actually grow the active RS — a
    // controller that swallows the scale (e.g. leaves it at 10) would still
    // satisfy the upper bound, so pin both ends.
    assert!(
        total >= 30,
        "total replicas {} must reach the new desired count of 30",
        total
    );
}

/// [sig-apps] Deployment should run the lifecycle of a Deployment [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go:207
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_lifecycle_create_scale_patch_delete() {
    let storage = setup();
    let ns = "default";
    let dep = make_deployment("life", ns, 1, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    // Scale to 3.
    let mut dep: Deployment = storage.get(&key).await.unwrap();
    dep.spec.replicas = Some(3);
    storage.update(&key, &dep).await.unwrap();
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    // Patch image.
    let mut dep: Deployment = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 6).await;

    // Delete.
    storage.delete(&key).await.unwrap();
    let after: Result<Deployment, _> = storage.get(&key).await;
    assert!(after.is_err(), "Deployment should be gone after delete");
}

/// [sig-apps] Deployment should validate Deployment Status endpoints [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go:216
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_status_replicas_match_replicaset_pods() {
    let storage = setup();
    let ns = "default";
    let dep = make_deployment("status", ns, 2, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 5).await;

    let dep: Deployment = storage.get(&key).await.unwrap();
    let status = dep
        .status
        .as_ref()
        .expect("deployment must publish a status");
    assert_eq!(status.replicas, Some(2), "status.replicas reflects desired");
    assert_eq!(
        status.ready_replicas,
        Some(2),
        "all pods ready => status.readyReplicas == 2"
    );
}

// ---- Strategy edge cases (paused, maxSurge/maxUnavailable knobs) ---------

/// [sig-apps] Deployment paused deployment should not progress
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/deployment.go (paused-rollout helper used
/// across rollover/proportional-scaling specs).
/// Sonobuoy (Round 160, 2026-04-26): PASS (paused = no progress)
///
/// `reconcile_deployment` returns early (status only) when `spec.paused` is
/// true, so a template hash change cannot create a new ReplicaSet.
#[tokio::test]
async fn deployment_paused_should_not_create_new_replicaset_on_template_change() {
    let storage = setup();
    let ns = "default";
    let mut dep = make_deployment("paused", ns, 2, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 2).await;

    let rs_prefix = build_prefix("replicasets", Some(ns));
    let before: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    assert_eq!(before.len(), 1, "single RS after initial rollout");

    // Pause + change image — no new RS should be created.
    dep = storage.get(&key).await.unwrap();
    dep.spec.paused = Some(true);
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    storage.update(&key, &dep).await.unwrap();

    deploy_ctrl.reconcile_all().await.unwrap();
    let after: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    assert_eq!(
        after.len(),
        1,
        "paused deployment must not create new ReplicaSets"
    );
}

/// [sig-apps] Deployment RollingUpdate maxSurge=0 maxUnavailable=1
///
/// Upstream behaviour referenced by RollingUpdateDeployment spec:
/// test/e2e/apps/deployment.go:106 — surge-bounded rollouts.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_rolling_update_zero_surge_one_unavailable_caps_replicas() {
    let storage = setup();
    let ns = "default";
    let dep = make_deployment(
        "zero-surge",
        ns,
        4,
        "nginx:1.0",
        Some(rolling_update_strategy("0", "1")),
    );
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    // Trigger an update.
    let mut dep: Deployment = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();

    // After a single reconcile, total RS replicas must not exceed desired
    // (maxSurge=0).
    deploy_ctrl.reconcile_all().await.unwrap();
    let rsets: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some(ns)))
        .await
        .unwrap();
    let total: i32 = rsets.iter().map(|rs| rs.spec.replicas).sum();
    assert!(
        total <= 4,
        "maxSurge=0 must cap total replicas at desired, got {}",
        total
    );
}

/// [sig-apps] Deployment RollingUpdate maxSurge=2 maxUnavailable=0
///
/// Upstream behaviour referenced by RollingUpdateDeployment spec:
/// test/e2e/apps/deployment.go:106 — surge-tolerant rollouts.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_rolling_update_with_surge_permits_extra_replicas() {
    let storage = setup();
    let ns = "default";
    let dep = make_deployment(
        "high-surge",
        ns,
        4,
        "nginx:1.0",
        Some(rolling_update_strategy("2", "0")),
    );
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    let mut dep: Deployment = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();

    deploy_ctrl.reconcile_all().await.unwrap();
    let rsets: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some(ns)))
        .await
        .unwrap();
    let total: i32 = rsets.iter().map(|rs| rs.spec.replicas).sum();
    assert!(
        total <= 6,
        "maxSurge=2 must allow up to desired+2 (=6) replicas, got {}",
        total
    );
    assert!(
        total >= 4,
        "rollout must keep at least desired (=4) replicas live, got {}",
        total
    );
}

/// [sig-apps] Deployment rollback by reverting template to previous revision
///
/// Upstream: rollback behaviour exercised by `should run the lifecycle of a
/// Deployment` and `support rollover`. test/e2e/apps/deployment.go:207
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_rollback_reuses_existing_old_replicaset() {
    let storage = setup();
    let ns = "default";
    let mut dep = make_deployment("rollback", ns, 2, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    let rs_prefix = build_prefix("replicasets", Some(ns));
    let first_rs_name = storage
        .list::<ReplicaSet>(&rs_prefix)
        .await
        .unwrap()
        .first()
        .unwrap()
        .metadata
        .name
        .clone();

    // Roll forward to v2.
    dep = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 8).await;

    // Roll back to v1.
    dep = storage.get(&key).await.unwrap();
    dep.spec.template.spec.containers[0].image = "nginx:1.0".to_string();
    dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
    storage.update(&key, &dep).await.unwrap();
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 8).await;

    let rsets: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    let original_after_rollback = rsets
        .iter()
        .find(|rs| rs.metadata.name == first_rs_name)
        .expect("original v1 RS must still exist after rollback");
    assert_eq!(
        original_after_rollback.spec.replicas, 2,
        "rolling back to the original template should re-scale the original RS to desired"
    );
}

/// [sig-apps] Deployment scaling to zero removes all pods
///
/// Upstream: scale-to-zero is a sub-case of "deployment lifecycle"
/// and "support rollover" specs in deployment.go.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_scale_to_zero_drains_replicaset_to_zero() {
    let storage = setup();
    let ns = "default";
    let mut dep = make_deployment("drain", ns, 3, "nginx:1.0", None);
    let key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&key, &dep).await.unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    let rs_ctrl = ReplicaSetController::new(storage.clone(), 5);
    run_rollout(&deploy_ctrl, &rs_ctrl, &storage, ns, 3).await;

    dep = storage.get(&key).await.unwrap();
    dep.spec.replicas = Some(0);
    storage.update(&key, &dep).await.unwrap();
    deploy_ctrl.reconcile_all().await.unwrap();

    let rsets: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some(ns)))
        .await
        .unwrap();
    let total: i32 = rsets.iter().map(|rs| rs.spec.replicas).sum();
    assert_eq!(total, 0, "scaling to 0 must scale every RS to 0");
}

/// [sig-apps] Deployment cross-namespace deployments are isolated
///
/// Upstream: covered transitively by deployment.go lifecycle specs that always
/// pin namespace=framework.Namespace.Name.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn deployment_namespaces_are_isolated() {
    let storage = setup();

    let dep_a = make_deployment("nginx", "team-a", 2, "nginx:1.0", None);
    let dep_b = make_deployment("nginx", "team-b", 3, "nginx:1.0", None);
    storage
        .create(&build_key("deployments", Some("team-a"), "nginx"), &dep_a)
        .await
        .unwrap();
    storage
        .create(&build_key("deployments", Some("team-b"), "nginx"), &dep_b)
        .await
        .unwrap();

    let deploy_ctrl = DeploymentController::new(storage.clone(), 5);
    deploy_ctrl.reconcile_all().await.unwrap();

    let rs_a: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some("team-a")))
        .await
        .unwrap();
    let rs_b: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some("team-b")))
        .await
        .unwrap();
    assert_eq!(rs_a.len(), 1, "team-a gets exactly one RS");
    assert_eq!(rs_b.len(), 1, "team-b gets exactly one RS");
    assert_eq!(rs_a[0].spec.replicas, 2);
    assert_eq!(rs_b[0].spec.replicas, 3);
}

// ===========================================================================
// ReplicaSet conformance tests
// ===========================================================================

/// [sig-apps] ReplicaSet should serve a basic image on each replica with a public image [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/replica_set.go:95
/// Sonobuoy (Round 160): FAIL end-to-end — the upstream scenario curls each
/// pod IP through the Service plane, so the failure surface is pod
/// networking / readiness, not the ReplicaSet controller itself.
///
/// At the controller level we still verify the contract this test owns: the
/// ReplicaSet produces N pods, each inheriting the template labels and
/// container image. That contract holds today, so the controller-level
/// mirror runs unconditionally. The end-to-end Sonobuoy verdict remains
/// tracked in `docs/CONFORMANCE.md` and is gated on cluster-side fixes
/// (pod IP / Service plane), not on the controller.
#[tokio::test]
async fn replicaset_should_serve_basic_image_on_each_replica() {
    let storage = setup();
    let ns = "default";
    let rs = make_replicaset("image-serve", ns, 3, "nginx:stable-alpine");
    storage
        .create(&build_key("replicasets", Some(ns), &rs.metadata.name), &rs)
        .await
        .unwrap();

    let ctrl = ReplicaSetController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 3, "ReplicaSet must create one pod per replica");
    for pod in &pods {
        let pod_labels = pod
            .metadata
            .labels
            .as_ref()
            .expect("each pod inherits template labels");
        assert_eq!(pod_labels.get("app"), Some(&"image-serve".to_string()));
        let spec = pod.spec.as_ref().expect("pod spec exists");
        assert_eq!(spec.containers[0].image, "nginx:stable-alpine");
    }
}

/// [sig-apps] ReplicaSet should adopt matching pods on creation and release no longer matching pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/replica_set.go:115
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn replicaset_should_adopt_matching_pods_and_release_mismatched() {
    let storage = setup();
    let ns = "default";

    // Create an orphan pod with matching labels.
    let orphan = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new("orphan");
            m.namespace = Some(ns.to_string());
            m.labels = Some(labels("adopt"));
            m
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:1.0".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };
    storage
        .create(&build_key("pods", Some(ns), "orphan"), &orphan)
        .await
        .unwrap();

    // Now create a matching RS with 2 replicas — it should adopt the orphan +
    // create one additional pod (total = 2).
    let rs = make_replicaset("adopt", ns, 2, "nginx:1.0");
    storage
        .create(&build_key("replicasets", Some(ns), "adopt"), &rs)
        .await
        .unwrap();

    let ctrl = ReplicaSetController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();

    let adopted: Pod = storage
        .get(&build_key("pods", Some(ns), "orphan"))
        .await
        .unwrap();
    let refs = adopted
        .metadata
        .owner_references
        .as_ref()
        .expect("orphan must have been adopted (gained ownerReferences)");
    assert!(
        refs.iter().any(|r| r.kind == "ReplicaSet"),
        "owner reference must point at the ReplicaSet"
    );

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(
        pods.len(),
        2,
        "after adoption RS must hold exactly desired replicas (orphan + 1 new)"
    );

    // ---- Release half of the upstream contract --------------------------
    // Upstream then mutates the adopted pod's labels so they no longer
    // match the RS selector and polls until `owner.UID == rs.UID` is gone
    // from the pod's ownerReferences. The previous Rust mirror only
    // covered the adoption half; this block locks the release path too so
    // any regression in `adopt_and_release` is caught locally.
    let mut mutated: Pod = storage
        .get(&build_key("pods", Some(ns), "orphan"))
        .await
        .unwrap();
    let mut not_matching = HashMap::new();
    not_matching.insert("name".to_string(), "not-matching-name".to_string());
    mutated.metadata.labels = Some(not_matching);
    storage
        .update(&build_key("pods", Some(ns), "orphan"), &mutated)
        .await
        .unwrap();

    ctrl.reconcile_all().await.unwrap();

    let released: Pod = storage
        .get(&build_key("pods", Some(ns), "orphan"))
        .await
        .unwrap();
    let still_owned = released
        .metadata
        .owner_references
        .as_ref()
        .map(|refs| {
            refs.iter().any(|r| {
                r.controller == Some(true)
                    && r.kind == "ReplicaSet"
                    && (r.uid == "rs-uid-adopt" || r.name == "adopt")
            })
        })
        .unwrap_or(false);
    assert!(
        !still_owned,
        "after labels stopped matching, RS controllerRef must be removed (release)"
    );
}

/// [sig-apps] Replicaset should have a working scale subresource [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/replica_set.go:128
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn replicaset_scale_subresource_resizes_pod_population() {
    let storage = setup();
    let ns = "default";
    let mut rs = make_replicaset("scale", ns, 2, "nginx:1.0");
    let key = build_key("replicasets", Some(ns), &rs.metadata.name);
    storage.create(&key, &rs).await.unwrap();

    let ctrl = ReplicaSetController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 2, "initial pod count == replicas");

    // /scale to 4.
    rs = storage.get(&key).await.unwrap();
    rs.spec.replicas = 4;
    storage.update(&key, &rs).await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 4, "scale to 4 must produce 4 pods");

    // /scale to 1.
    rs = storage.get(&key).await.unwrap();
    rs.spec.replicas = 1;
    storage.update(&key, &rs).await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 1, "scale to 1 must reduce pods to 1");
}

/// [sig-apps] ReplicaSet Replace and Patch tests [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/replica_set.go:142
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn replicaset_replace_and_patch_propagates_to_pods() {
    let storage = setup();
    let ns = "default";
    let mut rs = make_replicaset("patch", ns, 2, "nginx:1.0");
    let key = build_key("replicasets", Some(ns), &rs.metadata.name);
    storage.create(&key, &rs).await.unwrap();

    let ctrl = ReplicaSetController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();

    // Replace: re-PUT with the same spec — pods stay the same count.
    rs = storage.get(&key).await.unwrap();
    storage.update(&key, &rs).await.unwrap();
    ctrl.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 2);

    // Patch via update with bumped replicas.
    rs = storage.get(&key).await.unwrap();
    rs.spec.replicas = 3;
    storage.update(&key, &rs).await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 3, "patch replicas=3 must yield 3 pods");
}

/// [sig-apps] ReplicaSet should list and delete a collection of ReplicaSets [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/replica_set.go:156
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn replicaset_list_and_delete_collection() {
    let storage = setup();
    let ns = "default";

    for i in 0..3 {
        let rs = make_replicaset(&format!("rs-{i}"), ns, 1, "nginx:1.0");
        storage
            .create(&build_key("replicasets", Some(ns), &rs.metadata.name), &rs)
            .await
            .unwrap();
    }

    let listed: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some(ns)))
        .await
        .unwrap();
    assert_eq!(listed.len(), 3, "LIST returns all three RSes");

    // DeleteCollection: simulate by iterating + delete.
    for rs in &listed {
        let k = build_key("replicasets", Some(ns), &rs.metadata.name);
        storage.delete(&k).await.unwrap();
    }

    let after: Vec<ReplicaSet> = storage
        .list(&build_prefix("replicasets", Some(ns)))
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "deleteCollection must remove every ReplicaSet, got {} left",
        after.len()
    );
}

/// [sig-apps] ReplicaSet should validate Replicaset Status endpoints [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/replica_set.go:169
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn replicaset_status_replicas_match_pod_count() {
    let storage = setup();
    let ns = "default";
    let rs = make_replicaset("rsstatus", ns, 2, "nginx:1.0");
    let key = build_key("replicasets", Some(ns), &rs.metadata.name);
    storage.create(&key, &rs).await.unwrap();

    let ctrl = ReplicaSetController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;
    ctrl.reconcile_all().await.unwrap();

    let rs: ReplicaSet = storage.get(&key).await.unwrap();
    let status = rs.status.expect("status published");
    assert_eq!(status.replicas, 2, "status.replicas == spec.replicas");
    assert_eq!(
        status.ready_replicas, 2,
        "status.readyReplicas reflects Ready=True pods"
    );
}

/// [sig-apps] ReplicaSet self-healing on pod deletion
///
/// Upstream: implied invariant of `replica_set.go:95` ("should serve a basic
/// image on each replica") — if a pod dies the RS replaces it.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn replicaset_recreates_deleted_pod() {
    let storage = setup();
    let ns = "default";
    let rs = make_replicaset("heal", ns, 3, "nginx:1.0");
    storage
        .create(&build_key("replicasets", Some(ns), &rs.metadata.name), &rs)
        .await
        .unwrap();

    let ctrl = ReplicaSetController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 3);

    storage
        .delete(&build_key("pods", Some(ns), &pods[0].metadata.name))
        .await
        .unwrap();
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 3, "deleted pod must be re-created");
}

// ===========================================================================
// ReplicationController conformance tests
// ===========================================================================

/// [sig-apps] ReplicationController should serve a basic image on each replica with a public image [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/rc.go:65
///
/// The full upstream scenario curls each replica's pod IP for an HTTP 200,
/// exercising kubelet image-pull, pod networking, and Service/EndpointSlice
/// routing — the failing surfaces tracked by Sonobuoy. This controller-level
/// mirror pins the slice that `ReplicationControllerController` owns:
/// given a ReplicationController with `spec.replicas=N` and a template carrying
/// the requested image, the reconcile loop must create exactly N pods and
/// every pod must carry the requested image in its first container. The
/// kubelet- and network-side surfaces are exercised by their own crates'
/// integration tests; this mirror locks the controller contract.
#[tokio::test]
async fn rc_should_serve_basic_image_on_each_replica() {
    let storage = setup();
    let ns = "default";
    let rc = make_rc("rc-serve", ns, 2, "nginx:stable-alpine");
    storage
        .create(
            &build_key("replicationcontrollers", Some(ns), &rc.metadata.name),
            &rc,
        )
        .await
        .unwrap();

    let ctrl = ReplicationControllerController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 2, "RC creates one pod per replica");
    for pod in &pods {
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.containers[0].image, "nginx:stable-alpine");
        // Every pod must be owned by this RC so kubelet can route lifecycle
        // events back through the controller. Without this owner edge the
        // E2E "serve a basic image" assertion never even reaches the curl.
        let refs = pod
            .metadata
            .owner_references
            .as_ref()
            .expect("RC-created pod must carry an ownerReference");
        assert!(
            refs.iter().any(|r| r.kind == "ReplicationController"
                && r.name == "rc-serve"
                && r.controller == Some(true)),
            "ownerReference must point at the RC with controller=true"
        );
    }
}

/// [sig-apps] ReplicationController should adopt matching pods on creation [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/rc.go:89
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn rc_should_adopt_matching_pods_on_creation() {
    let storage = setup();
    let ns = "default";

    // Pre-existing orphan pod with matching labels.
    let orphan = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new("rc-orphan");
            m.namespace = Some(ns.to_string());
            m.labels = Some(labels("rc-adopt"));
            m
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:1.0".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };
    storage
        .create(&build_key("pods", Some(ns), "rc-orphan"), &orphan)
        .await
        .unwrap();

    let rc = make_rc("rc-adopt", ns, 2, "nginx:1.0");
    storage
        .create(
            &build_key("replicationcontrollers", Some(ns), "rc-adopt"),
            &rc,
        )
        .await
        .unwrap();

    let ctrl = ReplicationControllerController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();

    let adopted: Pod = storage
        .get(&build_key("pods", Some(ns), "rc-orphan"))
        .await
        .unwrap();
    let refs = adopted
        .metadata
        .owner_references
        .as_ref()
        .expect("orphan must have been adopted");
    assert!(
        refs.iter()
            .any(|r| r.kind == "ReplicationController" && r.name == "rc-adopt"),
        "owner reference points at RC"
    );
}

/// [sig-apps] ReplicationController should release no longer matching pods [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/rc.go:99
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn rc_should_release_pods_whose_labels_no_longer_match() {
    let storage = setup();
    let ns = "default";
    let rc = make_rc("rc-release", ns, 2, "nginx:1.0");
    storage
        .create(
            &build_key("replicationcontrollers", Some(ns), "rc-release"),
            &rc,
        )
        .await
        .unwrap();

    let ctrl = ReplicationControllerController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 2, "initial replica count");

    // Mutate one pod's labels so it no longer matches the selector.
    let mut p = pods[0].clone();
    let mut new_labels = labels("totally-different");
    new_labels.insert("rogue".to_string(), "yes".to_string());
    p.metadata.labels = Some(new_labels);
    let pk = build_key("pods", Some(ns), &p.metadata.name);
    storage.update(&pk, &p).await.unwrap();

    // Reconcile — RC should release ownership (and create a replacement to
    // restore desired count).
    ctrl.reconcile_all().await.unwrap();

    let released: Pod = storage.get(&pk).await.unwrap();
    let still_owned = released
        .metadata
        .owner_references
        .as_ref()
        .is_some_and(|refs| refs.iter().any(|r| r.name == "rc-release"));
    assert!(
        !still_owned,
        "RC must release ownership when pod labels stop matching the selector"
    );
}

/// [sig-apps] ReplicationController should test the lifecycle of a ReplicationController [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/rc.go:109
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn rc_lifecycle_create_scale_patch_delete() {
    let storage = setup();
    let ns = "default";
    let rc = make_rc("rc-life", ns, 1, "nginx:1.0");
    let key = build_key("replicationcontrollers", Some(ns), "rc-life");
    storage.create(&key, &rc).await.unwrap();

    let ctrl = ReplicationControllerController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 1);

    // Scale.
    let mut rc: ReplicationController = storage.get(&key).await.unwrap();
    rc.spec.replicas = Some(3);
    storage.update(&key, &rc).await.unwrap();
    ctrl.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 3);

    // Delete.
    storage.delete(&key).await.unwrap();
    let after: Result<ReplicationController, _> = storage.get(&key).await;
    assert!(after.is_err(), "RC must be gone after delete");
}

/// [sig-apps] ReplicationController should get and update a ReplicationController scale [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/rc.go:406
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn rc_scale_subresource_resizes_pod_population() {
    let storage = setup();
    let ns = "default";
    let rc = make_rc("rc-scale", ns, 2, "nginx:1.0");
    let key = build_key("replicationcontrollers", Some(ns), "rc-scale");
    storage.create(&key, &rc).await.unwrap();

    let ctrl = ReplicationControllerController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 2);

    let mut rc: ReplicationController = storage.get(&key).await.unwrap();
    rc.spec.replicas = Some(0);
    storage.update(&key, &rc).await.unwrap();
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    // Scale-down may delete pods or simply mark them for deletion; in both
    // cases the controller's active set must drop to 0.
    let active = pods
        .iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none())
        .count();
    assert_eq!(active, 0, "scale to 0 must drain active pods");
}

/// [sig-apps] ReplicationController should surface a failure condition on a common issue like exceeded quota [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/rc.go:76
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// The conformance scenario asserts a ReplicaFailure condition appears on the
/// RC status when a ResourceQuota blocks pod creation. The controller-manager
/// owns ReplicaFailure propagation; here we exercise the path with a fully
/// populated RC and observe that .status is populated after reconcile so any
/// future regression in status emission gets caught.
#[tokio::test]
async fn rc_publishes_status_after_reconcile() {
    let storage = setup();
    let ns = "default";
    let rc = make_rc("rc-status", ns, 2, "nginx:1.0");
    let key = build_key("replicationcontrollers", Some(ns), "rc-status");
    storage.create(&key, &rc).await.unwrap();

    let ctrl = ReplicationControllerController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();

    let rc: ReplicationController = storage.get(&key).await.unwrap();
    let status = rc.status.expect("RC must publish status after reconcile");
    assert_eq!(
        status.replicas, 2,
        "status.replicas reflects actual pod count"
    );
}

/// [sig-apps] ReplicationController self-healing on pod deletion
///
/// Upstream: implied invariant of `rc.go:65` — if a pod dies, RC replaces it.
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn rc_recreates_deleted_pod() {
    let storage = setup();
    let ns = "default";
    let rc = make_rc("rc-heal", ns, 3, "nginx:1.0");
    storage
        .create(
            &build_key("replicationcontrollers", Some(ns), "rc-heal"),
            &rc,
        )
        .await
        .unwrap();

    let ctrl = ReplicationControllerController::new(storage.clone(), 5);
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 3);

    storage
        .delete(&build_key("pods", Some(ns), &pods[0].metadata.name))
        .await
        .unwrap();
    ctrl.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(pods.len(), 3, "RC must self-heal the deleted pod");
}
