//! Upstream-mirror RED-state TDD pins for the Kubernetes v1.35 e2e suite at
//! `test/e2e/apps/deployment.go`.
//!
//! Upstream source (permalink, release-1.35):
//!   https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/apps/deployment.go
//!
//! These tests pin behaviours from the upstream e2e suite that the
//! `DeploymentController` does not yet implement end-to-end:
//!
//! * progressDeadlineSeconds rollout timeout
//! * minReadySeconds enforcement (delay between Ready and Available)
//! * multi-container image swap rollouts
//! * env-var change rollouts (template hash invalidation)
//! * resource-limit change rollouts
//! * PodDisruptionBudget interaction with rolling updates
//! * status.observedGeneration tracking (per-PATCH bump)
//! * Available / Progressing / ReplicaFailure condition lifecycle
//!
//! Per the project's TDD convention, every test asserts the upstream
//! contract verbatim. Tests that exercise behaviour the in-process
//! `DeploymentController` does not yet implement carry `#[ignore =
//! "RED-state: ..."]` — the `#[ignore]` is the failing-spec marker and
//! removing it is the unit of work for whoever lands the matching
//! behaviour.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::policy::IntOrString;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, ResourceRequirements, TypeMeta};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_controller_manager::controllers::pod_disruption_budget::PodDisruptionBudgetController;
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers — kept tiny and local so each test reads top-to-bottom.
// ---------------------------------------------------------------------------

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn labels_for(app: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("app".to_string(), app.to_string());
    m
}

/// Mark every non-terminating pod in `namespace` as Running + Ready=True.
async fn make_all_pods_ready(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
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

/// Build a deployment with a single-container template named `app`.
fn make_deployment(name: &str, namespace: &str, replicas: i32) -> Deployment {
    let labels = labels_for(name);
    Deployment {
        type_meta: TypeMeta {
            kind: "Deployment".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta.generation = Some(1);
            meta.creation_timestamp = Some(chrono::Utc::now());
            meta
        },
        spec: DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            min_ready_seconds: None,
            revision_history_limit: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "app".to_string(),
                        image: "nginx:1.25-alpine".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
            strategy: None,
            paused: None,
            progress_deadline_seconds: None,
        },
        status: Some(DeploymentStatus::default()),
    }
}

// ---------------------------------------------------------------------------
// Mirrored tests
// ---------------------------------------------------------------------------

/// Mirror of `[sig-apps] Deployment deployment should support proportional
/// scaling [Conformance]` — failure case: when the rollout cannot make
/// progress within `progressDeadlineSeconds`, the Deployment must emit a
/// `Progressing=False, reason=ProgressDeadlineExceeded` condition.
///
/// Upstream contract: `pkg/controller/deployment/progress.go` (`syncRolloutStatus`).
///
/// Setup: a deployment with `progress_deadline_seconds = 1` and
/// `creation_timestamp` 2s in the past. Pods are never marked Ready, so
/// `available < desired`. After one reconcile the deployment must carry the
/// `ProgressDeadlineExceeded` condition.
#[tokio::test]
async fn test_deployment_progress_deadline_seconds_exceeded() {
    let storage = setup_test().await;
    let controller = DeploymentController::new(storage.clone(), 10);

    let ns = "ns-progress-deadline";
    let mut deployment = make_deployment("stalled", ns, 3);
    deployment.spec.progress_deadline_seconds = Some(1);
    // Push creationTimestamp into the past so the controller observes the
    // deadline as exceeded on the very first reconcile.
    deployment.metadata.creation_timestamp =
        Some(chrono::Utc::now() - chrono::Duration::seconds(10));

    let key = build_key("deployments", Some(ns), "stalled");
    storage.create(&key, &deployment).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let updated: Deployment = storage.get(&key).await.unwrap();
    let conditions = updated
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("Deployment must publish conditions after reconcile");

    let progressing = conditions
        .iter()
        .find(|c| c.condition_type == "Progressing")
        .expect("Progressing condition must be present");

    assert_eq!(
        progressing.status, "False",
        "Progressing must be False once progressDeadlineSeconds is exceeded"
    );
    assert_eq!(
        progressing.reason.as_deref(),
        Some("ProgressDeadlineExceeded"),
        "reason must be ProgressDeadlineExceeded per upstream contract"
    );
}

/// Mirror of `[sig-apps] Deployment RollingUpdate deployment should delete
/// old pods and create new ones [Conformance]` — failure case: a pod that
/// is Ready but younger than `minReadySeconds` must NOT count as available.
///
/// Upstream: `pkg/controller/deployment/util.IsPodAvailable`.
///
/// Setup: deployment with `minReadySeconds = 3600`, 2 replicas, both pods
/// Ready=True with `creation_timestamp = now`. After reconcile,
/// `status.available_replicas` must be 0 — the readiness gate is not
/// satisfied yet.
#[tokio::test]
async fn test_deployment_min_ready_seconds_enforcement() {
    let storage = setup_test().await;
    let dep_controller = DeploymentController::new(storage.clone(), 10);
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);

    let ns = "ns-min-ready";
    let mut deployment = make_deployment("min-ready", ns, 2);
    deployment.spec.min_ready_seconds = Some(3600);
    let key = build_key("deployments", Some(ns), "min-ready");
    storage.create(&key, &deployment).await.unwrap();

    dep_controller.reconcile_all().await.unwrap();
    rs_controller.reconcile_all().await.unwrap();
    make_all_pods_ready(&storage, ns).await;
    dep_controller.reconcile_all().await.unwrap();

    let updated: Deployment = storage.get(&key).await.unwrap();
    let status = updated.status.expect("status must exist");
    assert_eq!(
        status.ready_replicas,
        Some(2),
        "Both pods are Ready=True so readyReplicas must be 2"
    );
    assert_eq!(
        status.available_replicas,
        Some(0),
        "availableReplicas must be 0 — pods are younger than minReadySeconds=3600"
    );
}

/// Mirror of `[sig-apps] Deployment deployment with multiple container images
/// should perform rolling update`.
///
/// Setup: a deployment with two containers (`app` and `sidecar`). Update the
/// sidecar image. The deployment must create a new ReplicaSet whose
/// template has the new sidecar image while preserving the unchanged
/// `app` image.
#[tokio::test]
async fn test_deployment_with_multiple_container_images() {
    let storage = setup_test().await;
    let controller = DeploymentController::new(storage.clone(), 10);
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);

    let ns = "ns-multi-container";
    let mut deployment = make_deployment("multi", ns, 2);
    deployment.spec.template.spec.containers.push(Container {
        name: "sidecar".to_string(),
        image: "busybox:1.36".to_string(),
        ..Default::default()
    });
    let key = build_key("deployments", Some(ns), "multi");
    storage.create(&key, &deployment).await.unwrap();

    controller.reconcile_all().await.unwrap();
    rs_controller.reconcile_all().await.unwrap();
    make_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    // Snapshot the initial RS.
    let initial: Vec<ReplicaSet> = storage
        .list(&format!("/registry/replicasets/{}/", ns))
        .await
        .unwrap();
    assert_eq!(initial.len(), 1, "Should start with exactly one ReplicaSet");
    let initial_rs_name = initial[0].metadata.name.clone();

    // Mutate the sidecar image only — app must stay on nginx:1.25-alpine.
    let mut updated = deployment.clone();
    updated.spec.template.spec.containers[1].image = "busybox:1.37".to_string();
    updated.metadata.generation = Some(2);
    storage.update(&key, &updated).await.unwrap();

    // Run the deployment + RS controllers until two RSes exist (rolling update).
    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        rs_controller.reconcile_all().await.unwrap();
        make_all_pods_ready(&storage, ns).await;
    }

    let rss: Vec<ReplicaSet> = storage
        .list(&format!("/registry/replicasets/{}/", ns))
        .await
        .unwrap();
    assert_eq!(
        rss.len(),
        2,
        "Rolling update must create a second ReplicaSet for the new sidecar image"
    );

    let new_rs = rss
        .iter()
        .find(|rs| rs.metadata.name != initial_rs_name)
        .expect("a new ReplicaSet must be created");
    // Two containers carried through, with the updated sidecar image.
    assert_eq!(
        new_rs.spec.template.spec.containers.len(),
        2,
        "new RS must keep both containers"
    );
    let new_sidecar = new_rs
        .spec
        .template
        .spec
        .containers
        .iter()
        .find(|c| c.name == "sidecar")
        .expect("sidecar must exist on the new RS");
    assert_eq!(
        new_sidecar.image, "busybox:1.37",
        "new RS sidecar image must match the updated spec"
    );
    let new_app = new_rs
        .spec
        .template
        .spec
        .containers
        .iter()
        .find(|c| c.name == "app")
        .expect("app container must exist on the new RS");
    assert_eq!(
        new_app.image, "nginx:1.25-alpine",
        "app image must be unchanged"
    );
}

/// Mirror of upstream `TestDeploymentEnvVarChangeTriggersRollout` semantics:
/// changing only an env var on a container is a template mutation and must
/// trigger a rollout (new pod-template-hash → new ReplicaSet).
///
/// Upstream: `pkg/controller/deployment/util/deployment_util.go`
/// (`GetPodTemplateSpecHash` hashes the full container spec including env).
#[tokio::test]
async fn test_deployment_environment_variable_updates() {
    let storage = setup_test().await;
    let controller = DeploymentController::new(storage.clone(), 10);
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);

    let ns = "ns-env-update";
    let mut deployment = make_deployment("env-roll", ns, 2);
    deployment.spec.template.spec.containers[0].env = Some(vec![EnvVar {
        name: "LOG_LEVEL".to_string(),
        value: Some("info".to_string()),
        value_from: None,
    }]);
    let key = build_key("deployments", Some(ns), "env-roll");
    storage.create(&key, &deployment).await.unwrap();

    controller.reconcile_all().await.unwrap();
    rs_controller.reconcile_all().await.unwrap();
    make_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    let initial: Vec<ReplicaSet> = storage
        .list(&format!("/registry/replicasets/{}/", ns))
        .await
        .unwrap();
    assert_eq!(initial.len(), 1, "one RS expected before env mutation");
    let initial_rs_name = initial[0].metadata.name.clone();

    // Mutate the env value only.
    let mut updated = deployment.clone();
    updated.spec.template.spec.containers[0].env = Some(vec![EnvVar {
        name: "LOG_LEVEL".to_string(),
        value: Some("debug".to_string()),
        value_from: None,
    }]);
    updated.metadata.generation = Some(2);
    storage.update(&key, &updated).await.unwrap();

    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        rs_controller.reconcile_all().await.unwrap();
        make_all_pods_ready(&storage, ns).await;
    }

    let rss: Vec<ReplicaSet> = storage
        .list(&format!("/registry/replicasets/{}/", ns))
        .await
        .unwrap();
    assert_eq!(
        rss.len(),
        2,
        "env-var mutation must trigger a new ReplicaSet (pod-template-hash change)"
    );
    let new_rs = rss
        .iter()
        .find(|rs| rs.metadata.name != initial_rs_name)
        .expect("a new RS must be created on env change");
    let env = new_rs.spec.template.spec.containers[0]
        .env
        .as_ref()
        .expect("new RS template must carry the updated env");
    assert_eq!(env[0].value.as_deref(), Some("debug"));
}

/// Mirror of upstream rolling-update on resource-limit change. Changing
/// `resources.limits` on a container is a template mutation: the
/// pod-template-hash must change and a new ReplicaSet must be created
/// carrying the new limits.
#[tokio::test]
async fn test_deployment_resource_limits_updates() {
    let storage = setup_test().await;
    let controller = DeploymentController::new(storage.clone(), 10);
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);

    let ns = "ns-resource-update";
    let mut deployment = make_deployment("res-roll", ns, 2);

    let mut limits = HashMap::new();
    limits.insert("cpu".to_string(), "100m".to_string());
    limits.insert("memory".to_string(), "128Mi".to_string());
    deployment.spec.template.spec.containers[0].resources = Some(ResourceRequirements {
        limits: Some(limits),
        requests: None,
        claims: None,
    });
    let key = build_key("deployments", Some(ns), "res-roll");
    storage.create(&key, &deployment).await.unwrap();

    controller.reconcile_all().await.unwrap();
    rs_controller.reconcile_all().await.unwrap();
    make_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    let initial: Vec<ReplicaSet> = storage
        .list(&format!("/registry/replicasets/{}/", ns))
        .await
        .unwrap();
    assert_eq!(initial.len(), 1, "one RS expected before limits change");
    let initial_rs_name = initial[0].metadata.name.clone();

    // Double the limits.
    let mut new_limits = HashMap::new();
    new_limits.insert("cpu".to_string(), "200m".to_string());
    new_limits.insert("memory".to_string(), "256Mi".to_string());
    let mut updated = deployment.clone();
    updated.spec.template.spec.containers[0].resources = Some(ResourceRequirements {
        limits: Some(new_limits),
        requests: None,
        claims: None,
    });
    updated.metadata.generation = Some(2);
    storage.update(&key, &updated).await.unwrap();

    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        rs_controller.reconcile_all().await.unwrap();
        make_all_pods_ready(&storage, ns).await;
    }

    let rss: Vec<ReplicaSet> = storage
        .list(&format!("/registry/replicasets/{}/", ns))
        .await
        .unwrap();
    assert_eq!(
        rss.len(),
        2,
        "resource-limits change must trigger a new ReplicaSet"
    );
    let new_rs = rss
        .iter()
        .find(|rs| rs.metadata.name != initial_rs_name)
        .expect("a new RS must be created on resources change");
    let resources = new_rs.spec.template.spec.containers[0]
        .resources
        .as_ref()
        .expect("new RS template must carry the updated resources");
    let limits = resources
        .limits
        .as_ref()
        .expect("limits must be present on the new RS template");
    assert_eq!(limits.get("cpu").map(String::as_str), Some("200m"));
    assert_eq!(limits.get("memory").map(String::as_str), Some("256Mi"));
}

/// Mirror of `[sig-apps] Deployment should be able to roll back a deployment`
/// PDB-aware variant — when a PDB protects the deployment's pods, the PDB
/// controller's `status.disruptions_allowed` must reflect the deployment's
/// availability budget so the rollout drainer can throttle.
///
/// Upstream: `pkg/controller/disruption/disruption.go` (`PDB.Status.DisruptionsAllowed`)
/// and the deployment controller's interaction with PDB-protected pods.
///
/// Setup: deployment with 4 ready replicas + a PDB selecting them with
/// `minAvailable = 3`. After PDB reconciliation, `disruptions_allowed`
/// must be exactly 1 (`current_healthy - desired_healthy`).
#[tokio::test]
async fn test_deployment_with_pod_disruption_budget() {
    let storage = setup_test().await;
    let dep_controller = DeploymentController::new(storage.clone(), 10);
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);
    let pdb_controller = PodDisruptionBudgetController::new(storage.clone());

    let ns = "ns-pdb";
    let deployment = make_deployment("pdb-app", ns, 4);
    let key = build_key("deployments", Some(ns), "pdb-app");
    storage.create(&key, &deployment).await.unwrap();

    // Stand up the deployment + 4 ready pods.
    dep_controller.reconcile_all().await.unwrap();
    rs_controller.reconcile_all().await.unwrap();
    make_all_pods_ready(&storage, ns).await;
    dep_controller.reconcile_all().await.unwrap();

    // Attach a PDB selecting the same pods with minAvailable=3.
    let pdb = policy::PodDisruptionBudget {
        type_meta: TypeMeta {
            kind: "PodDisruptionBudget".to_string(),
            api_version: "policy/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("pdb-app-pdb");
            meta.namespace = Some(ns.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: policy::PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(3)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(labels_for("pdb-app")),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
        status: None,
    };
    let pdb_key = build_key("poddisruptionbudgets", Some(ns), "pdb-app-pdb");
    storage.create(&pdb_key, &pdb).await.unwrap();

    pdb_controller.reconcile_all().await.unwrap();

    let stored: policy::PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
    let status = stored
        .status
        .expect("PDB controller must publish status after reconcile");
    assert_eq!(
        status.current_healthy, 4,
        "PDB must count 4 healthy pods matching the deployment selector"
    );
    assert_eq!(
        status.desired_healthy, 3,
        "desired_healthy must equal minAvailable=3"
    );
    assert_eq!(
        status.disruptions_allowed, 1,
        "disruptions_allowed must be current_healthy - desired_healthy = 1"
    );
}

/// Mirror of upstream `TestDeploymentObservedGeneration` contract: every
/// spec mutation increments `metadata.generation`, and after a reconcile
/// `status.observed_generation` must catch up to it.
///
/// Upstream: `pkg/controller/deployment/sync.go`
/// (`calculateStatus` writes `status.ObservedGeneration = deployment.Generation`).
#[tokio::test]
async fn test_deployment_observed_generation() {
    let storage = setup_test().await;
    let controller = DeploymentController::new(storage.clone(), 10);

    let ns = "ns-observed-gen";
    let mut deployment = make_deployment("obs-gen", ns, 2);
    deployment.metadata.generation = Some(1);
    let key = build_key("deployments", Some(ns), "obs-gen");
    storage.create(&key, &deployment).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let after_first: Deployment = storage.get(&key).await.unwrap();
    assert_eq!(
        after_first
            .status
            .as_ref()
            .and_then(|s| s.observed_generation),
        Some(1),
        "observed_generation must catch up to generation=1 after first reconcile"
    );

    // Bump generation as if a PATCH mutated the spec.
    let mut updated: Deployment = storage.get(&key).await.unwrap();
    updated.metadata.generation = Some(2);
    updated.spec.replicas = Some(3);
    storage.update(&key, &updated).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let after_second: Deployment = storage.get(&key).await.unwrap();
    assert_eq!(
        after_second
            .status
            .as_ref()
            .and_then(|s| s.observed_generation),
        Some(2),
        "observed_generation must track the latest generation after subsequent reconcile"
    );
}

/// Mirror of `[sig-apps] Deployment` condition-lifecycle expectations:
/// on a healthy steady-state deployment, both `Available=True` and
/// `Progressing=True` must be present, and `ReplicaFailure` must NOT be
/// present (the upstream controller only emits it when a pod creation
/// genuinely failed).
///
/// Upstream: `pkg/controller/deployment/sync.go`
/// (`calculateStatus` → `updateDeploymentCondition`).
#[tokio::test]
async fn test_deployment_conditions_lifecycle_healthy_steady_state() {
    let storage = setup_test().await;
    let dep_controller = DeploymentController::new(storage.clone(), 10);
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);

    let ns = "ns-conditions-healthy";
    let deployment = make_deployment("healthy", ns, 2);
    let key = build_key("deployments", Some(ns), "healthy");
    storage.create(&key, &deployment).await.unwrap();

    // Bring the deployment to steady state.
    dep_controller.reconcile_all().await.unwrap();
    rs_controller.reconcile_all().await.unwrap();
    make_all_pods_ready(&storage, ns).await;
    dep_controller.reconcile_all().await.unwrap();

    let updated: Deployment = storage.get(&key).await.unwrap();
    let conditions = updated
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("status.conditions must be populated");

    let available = conditions
        .iter()
        .find(|c| c.condition_type == "Available")
        .expect("Available condition required");
    assert_eq!(
        available.status, "True",
        "Available must be True when desired replicas are available"
    );

    let progressing = conditions
        .iter()
        .find(|c| c.condition_type == "Progressing")
        .expect("Progressing condition required");
    assert_eq!(
        progressing.status, "True",
        "Progressing must be True for a healthy deployment"
    );
    assert_eq!(
        progressing.reason.as_deref(),
        Some("NewReplicaSetAvailable"),
        "reason must be NewReplicaSetAvailable once rollout completes"
    );

    let has_replica_failure = conditions
        .iter()
        .any(|c| c.condition_type == "ReplicaFailure");
    assert!(
        !has_replica_failure,
        "ReplicaFailure must NOT be set for a healthy deployment"
    );
}

/// Mirror of upstream `[sig-apps] Deployment` condition-lifecycle (failure
/// path): if a deployment is unable to make any of its pods available, the
/// `Available=False` condition must include `reason=MinimumReplicasUnavailable`
/// and the `Progressing` condition must reflect that no progress has been
/// made (either `False, ProgressDeadlineExceeded` once the deadline lapses,
/// or `True, ReplicaSetUpdated` while still within the deadline).
///
/// Upstream: `pkg/controller/deployment/sync.go::calculateStatus`.
#[tokio::test]
async fn test_deployment_conditions_lifecycle_unavailable() {
    let storage = setup_test().await;
    let dep_controller = DeploymentController::new(storage.clone(), 10);
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);

    let ns = "ns-conditions-unavail";
    let mut deployment = make_deployment("unavail", ns, 3);
    // Long deadline so we are explicitly inside the progress window.
    deployment.spec.progress_deadline_seconds = Some(3600);
    let key = build_key("deployments", Some(ns), "unavail");
    storage.create(&key, &deployment).await.unwrap();

    dep_controller.reconcile_all().await.unwrap();
    rs_controller.reconcile_all().await.unwrap();
    // Deliberately do NOT mark pods Ready — simulating a failing rollout.
    dep_controller.reconcile_all().await.unwrap();

    let updated: Deployment = storage.get(&key).await.unwrap();
    let conditions = updated
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("conditions must be populated");

    let available = conditions
        .iter()
        .find(|c| c.condition_type == "Available")
        .expect("Available condition required");
    assert_eq!(
        available.status, "False",
        "Available must be False when no pods are ready"
    );
    assert_eq!(
        available.reason.as_deref(),
        Some("MinimumReplicasUnavailable"),
        "reason must be MinimumReplicasUnavailable"
    );

    let progressing = conditions
        .iter()
        .find(|c| c.condition_type == "Progressing")
        .expect("Progressing condition required");
    // Within the deadline the reason is ReplicaSetUpdated; once the
    // deadline lapses it flips to ProgressDeadlineExceeded.
    let reason = progressing.reason.as_deref().unwrap_or("");
    assert!(
        reason == "ReplicaSetUpdated"
            || reason == "NewReplicaSetCreated"
            || reason == "ProgressDeadlineExceeded",
        "Progressing.reason must reflect an in-flight rollout, got {:?}",
        reason
    );
}

/// Mirror of upstream `TestDeploymentReplicaFailureCondition`: when a pod
/// fails to be created (e.g. quota exhausted), the deployment must publish
/// a `ReplicaFailure=True` condition citing the underlying RS failure.
///
/// Upstream: `pkg/controller/deployment/sync.go::calculateStatus` —
/// `ReplicaFailure` is copied from `replicaSet.status.conditions`.
///
/// Setup: deployment + RS where the RS publishes a `ReplicaFailure=True`
/// condition. The deployment controller must surface the same condition.
#[tokio::test]
async fn test_deployment_replica_failure_condition_surfaces_from_rs() {
    let storage = setup_test().await;
    let dep_controller = DeploymentController::new(storage.clone(), 10);

    let ns = "ns-replica-failure";
    let deployment = make_deployment("rf", ns, 2);
    let key = build_key("deployments", Some(ns), "rf");
    storage.create(&key, &deployment).await.unwrap();

    // Let the deployment controller create the RS.
    dep_controller.reconcile_all().await.unwrap();
    let rss: Vec<ReplicaSet> = storage
        .list(&format!("/registry/replicasets/{}/", ns))
        .await
        .unwrap();
    assert_eq!(rss.len(), 1, "deployment must own one RS");
    let mut rs = rss.into_iter().next().unwrap();
    let rs_key = build_key("replicasets", Some(ns), &rs.metadata.name);

    // Inject a ReplicaFailure=True condition on the RS.
    rs.status = Some(workloads::ReplicaSetStatus {
        replicas: 0,
        ready_replicas: 0,
        available_replicas: 0,
        fully_labeled_replicas: Some(0),
        observed_generation: Some(1),
        conditions: Some(vec![workloads::ReplicaSetCondition {
            condition_type: "ReplicaFailure".to_string(),
            status: "True".to_string(),
            reason: Some("FailedCreate".to_string()),
            message: Some("pods quota exceeded".to_string()),
            last_transition_time: None,
        }]),
        terminating_replicas: None,
    });
    storage.update(&rs_key, &rs).await.unwrap();

    dep_controller.reconcile_all().await.unwrap();

    let updated: Deployment = storage.get(&key).await.unwrap();
    let conditions = updated
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("conditions required");

    let rf = conditions
        .iter()
        .find(|c| c.condition_type == "ReplicaFailure")
        .expect("ReplicaFailure must be surfaced on the deployment when its RS has one");
    assert_eq!(rf.status, "True");
    assert_eq!(rf.reason.as_deref(), Some("FailedCreate"));
}

/// Mirror of upstream `TestDeploymentScalingEvent`: ownerReferences on the
/// owned ReplicaSet must reference the Deployment with `controller=true`
/// and `blockOwnerDeletion=true`, so that garbage collection and foreground
/// propagation honour the parent-child link.
///
/// Upstream: `pkg/controller/deployment/sync.go::getNewReplicaSet`.
#[tokio::test]
async fn test_deployment_replicaset_owner_reference_block_owner_deletion() {
    let storage = setup_test().await;
    let controller = DeploymentController::new(storage.clone(), 10);

    let ns = "ns-owner-ref";
    let deployment = make_deployment("owner-check", ns, 1);
    let key = build_key("deployments", Some(ns), "owner-check");
    storage.create(&key, &deployment).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let rss: Vec<ReplicaSet> = storage
        .list(&format!("/registry/replicasets/{}/", ns))
        .await
        .unwrap();
    assert_eq!(rss.len(), 1, "deployment must create exactly one RS");
    let owners = rss[0]
        .metadata
        .owner_references
        .as_ref()
        .expect("owned RS must carry owner references");
    let parent = owners
        .iter()
        .find(|o| o.kind == "Deployment" && o.name == "owner-check")
        .expect("owner reference must point at the Deployment");
    assert_eq!(
        parent.controller,
        Some(true),
        "controller flag must be true"
    );
    assert_eq!(
        parent.block_owner_deletion,
        Some(true),
        "block_owner_deletion must be true so foreground propagation works"
    );
    assert_eq!(
        parent.uid, deployment.metadata.uid,
        "owner UID must match the parent deployment"
    );
}
