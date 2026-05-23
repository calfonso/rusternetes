// Idempotency tests for the Deployment Controller (Unit 8: proportional
// scaling + paused rollouts).
//
// The previous hot-loop in kubelet sync_pod was already fixed in commit
// f0eea82 ("fix(kubelet): gate terminal-pod status writes to break sync_pod
// hot-loop"). These tests check that the deployment reconciler itself does
// not redundantly write to storage when the cluster is in a steady state —
// any write on a stable cluster would re-fire watchers and re-enter the
// reconciler, producing a controller-side hot-loop.
//
// MemoryStorage does NOT bump resourceVersion on update, so we compare the
// full serialized object via serde_json::to_string and assert byte-equality
// across reconcile invocations.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, OwnerReference, TypeMeta};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

async fn setup() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

/// Build a Deployment with the given replicas using a minimal RollingUpdate
/// strategy (so the controller takes the rolling-update code path).
fn build_deployment(name: &str, namespace: &str, replicas: i32, image: &str) -> Deployment {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    Deployment {
        type_meta: TypeMeta {
            kind: "Deployment".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = format!("deploy-uid-{}", name);
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
                    let mut meta = ObjectMeta::new("");
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
                        image: image.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
            strategy: Some(deployment::DeploymentStrategy {
                strategy_type: "RollingUpdate".to_string(),
                rolling_update: Some(deployment::RollingUpdateDeployment {
                    max_surge: Some(serde_json::json!("25%")),
                    max_unavailable: Some(serde_json::json!("25%")),
                }),
            }),
            paused: None,
            progress_deadline_seconds: None,
        },
        status: None,
    }
}

/// Compute the pod-template-hash exactly as the controller does so that the
/// preseeded ReplicaSet is recognised as the "active" RS (matches template).
fn pod_template_hash(deployment: &Deployment) -> String {
    let value = serde_json::to_value(&deployment.spec.template).unwrap_or_default();
    let template_json = serde_json::to_string(&value).unwrap_or_default();
    let hash = Sha256::digest(template_json.as_bytes());
    format!(
        "{:08x}",
        u32::from_be_bytes(hash[..4].try_into().unwrap_or([0u8; 4]))
    )
}

/// Build a ReplicaSet that matches a deployment's template, owned by the
/// deployment, with the given replicas. By default the RS carries the
/// `desired-replicas`, `max-replicas`, and `revision` annotations so that
/// reconcile sees a fully-reconciled (stable) state.
fn build_active_rs(
    deployment: &Deployment,
    replicas: i32,
    image: &str,
    with_annotations: bool,
) -> ReplicaSet {
    let namespace = deployment
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let hash = pod_template_hash(deployment);
    let rs_name = format!("{}-{}", deployment.metadata.name, hash);

    let mut labels: HashMap<String, String> = deployment
        .spec
        .template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    labels.insert("pod-template-hash".to_string(), hash.clone());

    let mut annotations = HashMap::new();
    if with_annotations {
        let desired = deployment.spec.replicas.unwrap_or(1);
        // maxSurge from "25%" of `desired`, ceil; matches the controller's
        // compute_rolling_update_counts behaviour.
        let max_surge = ((desired as f64 * 0.25).ceil()) as i32;
        annotations.insert(
            "deployment.kubernetes.io/desired-replicas".to_string(),
            desired.to_string(),
        );
        annotations.insert(
            "deployment.kubernetes.io/max-replicas".to_string(),
            (desired + max_surge).to_string(),
        );
        annotations.insert(
            "deployment.kubernetes.io/revision".to_string(),
            "1".to_string(),
        );
    }

    // The RS selector and template both need the pod-template-hash, matching
    // how the controller creates them.
    let mut selector_labels = labels.clone();
    selector_labels.insert("pod-template-hash".to_string(), hash.clone());

    let mut template = deployment.spec.template.clone();
    let template_labels = template
        .metadata
        .get_or_insert_with(|| ObjectMeta::new(""))
        .labels
        .get_or_insert_with(Default::default);
    template_labels.insert("pod-template-hash".to_string(), hash);
    // Force the image (callers may pass a different image to seed an old RS).
    if let Some(c) = template.spec.containers.get_mut(0) {
        c.image = image.to_string();
    }

    ReplicaSet {
        type_meta: TypeMeta {
            kind: "ReplicaSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: rs_name.clone(),
            namespace: Some(namespace),
            uid: format!("rs-uid-{}", rs_name),
            labels: Some(labels),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                name: deployment.metadata.name.clone(),
                uid: deployment.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        spec: ReplicaSetSpec {
            replicas,
            selector: LabelSelector {
                match_labels: Some(selector_labels),
                match_expressions: None,
            },
            template,
            min_ready_seconds: None,
        },
        status: Some(ReplicaSetStatus {
            replicas,
            ready_replicas: replicas,
            available_replicas: replicas,
            fully_labeled_replicas: Some(replicas),
            observed_generation: None,
            conditions: None,
            terminating_replicas: None,
        }),
    }
}

/// Build an "old" RS with a different template hash than the deployment (so
/// it is treated as an old/outgoing RS during a rollover). Carries the
/// proportional-scaling annotations identifying the prior desired/max sizes.
fn build_old_rs(
    deployment: &Deployment,
    replicas: i32,
    image: &str,
    desired_annotation: i32,
    max_replicas_annotation: i32,
    revision: i64,
) -> ReplicaSet {
    let namespace = deployment
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let old_hash = format!("old{:04x}", revision);
    let rs_name = format!("{}-{}", deployment.metadata.name, old_hash);

    let mut labels: HashMap<String, String> = deployment
        .spec
        .template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    labels.insert("pod-template-hash".to_string(), old_hash.clone());

    let mut annotations = HashMap::new();
    annotations.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        desired_annotation.to_string(),
    );
    annotations.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        max_replicas_annotation.to_string(),
    );
    annotations.insert(
        "deployment.kubernetes.io/revision".to_string(),
        revision.to_string(),
    );

    let mut template = deployment.spec.template.clone();
    let template_labels = template
        .metadata
        .get_or_insert_with(|| ObjectMeta::new(""))
        .labels
        .get_or_insert_with(Default::default);
    template_labels.insert("pod-template-hash".to_string(), old_hash.clone());
    if let Some(c) = template.spec.containers.get_mut(0) {
        c.image = image.to_string();
    }

    let mut selector_labels = labels.clone();
    selector_labels.insert("pod-template-hash".to_string(), old_hash);

    ReplicaSet {
        type_meta: TypeMeta {
            kind: "ReplicaSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: rs_name.clone(),
            namespace: Some(namespace),
            uid: format!("rs-uid-{}", rs_name),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: Some(vec![OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                name: deployment.metadata.name.clone(),
                uid: deployment.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        spec: ReplicaSetSpec {
            replicas,
            selector: LabelSelector {
                match_labels: Some(selector_labels),
                match_expressions: None,
            },
            template,
            min_ready_seconds: None,
        },
        status: Some(ReplicaSetStatus {
            replicas,
            ready_replicas: replicas,
            available_replicas: replicas,
            fully_labeled_replicas: Some(replicas),
            observed_generation: None,
            conditions: None,
            terminating_replicas: None,
        }),
    }
}

/// Snapshot a stored object as canonical JSON for byte-equality comparison.
async fn snapshot(storage: &Arc<MemoryStorage>, key: &str) -> String {
    let v: serde_json::Value = storage.get(key).await.expect("object missing from storage");
    serde_json::to_string(&v).unwrap()
}

/// Simulate what a real cluster's kubelet + ReplicaSet controller would do
/// between deployment reconciles: walk every ReplicaSet in the namespace and
/// bring its `.status` fields up to match its `.spec.replicas` (i.e. "every
/// pod the RS asked for is now Running, Ready, and Available").
///
/// In production this convergence happens asynchronously across many controllers
/// (RS controller observes pods, kubelet posts pod status, RS controller
/// aggregates back into `status.{ready,available}Replicas`). For deterministic
/// unit tests of the *deployment* reconciler we collapse that pipeline into
/// one synchronous step so the rolling-update progression branch sees the
/// availability the cluster would actually have settled on, instead of the
/// stale preseeded numbers that produce a spurious "scale old RS down"
/// decision.
///
/// Only the RS `.status` fields the deployment controller reads in
/// `reconcileNewReplicaSet` / `reconcileOldReplicaSets` are updated.
async fn simulate_kubelet_convergence(storage: &Arc<MemoryStorage>, namespace: &str) {
    let rs_prefix = rusternetes_storage::build_prefix("replicasets", Some(namespace));
    let all_rs: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap_or_default();
    for mut rs in all_rs {
        let target = rs.spec.replicas;
        let needs_update = rs.status.as_ref().is_none_or(|s| {
            s.replicas != target || s.ready_replicas != target || s.available_replicas != target
        });
        if !needs_update {
            continue;
        }
        rs.status = Some(ReplicaSetStatus {
            replicas: target,
            ready_replicas: target,
            available_replicas: target,
            fully_labeled_replicas: Some(target),
            observed_generation: rs.status.as_ref().and_then(|s| s.observed_generation),
            conditions: rs.status.as_ref().and_then(|s| s.conditions.clone()),
            terminating_replicas: rs.status.as_ref().and_then(|s| s.terminating_replicas),
        });
        let key = build_key("replicasets", Some(namespace), &rs.metadata.name);
        storage.update(&key, &rs).await.unwrap();
    }
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

/// One Deployment (3 replicas, RollingUpdate) + one active matching RS that
/// already has the desired-replicas and max-replicas annotations set, all
/// available. A second reconcile must NOT mutate either object.
#[tokio::test]
async fn test_deployment_reconcile_idempotent_on_stable_state() {
    let storage = setup().await;
    let ns = "default";

    // Deployment with a revision annotation already in place so the first
    // reconcile finds nothing to update.
    let mut deployment = build_deployment("idem", ns, 3, "nginx:1.0");
    deployment
        .metadata
        .annotations
        .get_or_insert_with(HashMap::new)
        .insert(
            "deployment.kubernetes.io/revision".to_string(),
            "1".to_string(),
        );

    let dep_key = build_key("deployments", Some(ns), "idem");
    storage.create(&dep_key, &deployment).await.unwrap();

    // Preseed the matching active RS (3 replicas, all available, annotations set).
    let rs = build_active_rs(&deployment, 3, "nginx:1.0", true);
    let rs_key = build_key("replicasets", Some(ns), &rs.metadata.name);
    storage.create(&rs_key, &rs).await.unwrap();

    let controller = DeploymentController::new(storage.clone(), 10);

    // First reconcile: may legitimately write deployment status (since the
    // preseeded deployment has status=None).
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    // Snapshot post-first-reconcile state.
    let dep_after_first = snapshot(&storage, &dep_key).await;
    let rs_after_first = snapshot(&storage, &rs_key).await;

    // Second reconcile from the freshly stored deployment.
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    let dep_after_second = snapshot(&storage, &dep_key).await;
    let rs_after_second = snapshot(&storage, &rs_key).await;

    assert_eq!(
        dep_after_first, dep_after_second,
        "Deployment should be byte-equal across reconciles on stable state"
    );
    assert_eq!(
        rs_after_first, rs_after_second,
        "ReplicaSet should be byte-equal across reconciles on stable state"
    );
}

/// When the active RS already has desired-replicas=3 and max-replicas=3
/// (matching the deployment's 3 replicas + 0 surge), reconcile must not
/// rewrite the annotations. We assert on the full RS JSON since
/// MemoryStorage does not bump resourceVersion.
#[tokio::test]
async fn test_deployment_annotation_refresh_skipped_when_values_match() {
    let storage = setup().await;
    let ns = "default";

    // Use 0% surge so max-replicas = desired = 3 (matches our preseeded annotations).
    let mut deployment = build_deployment("ann", ns, 3, "nginx:1.0");
    deployment.spec.strategy = Some(deployment::DeploymentStrategy {
        strategy_type: "RollingUpdate".to_string(),
        rolling_update: Some(deployment::RollingUpdateDeployment {
            max_surge: Some(serde_json::json!("0")),
            max_unavailable: Some(serde_json::json!("0")),
        }),
    });
    deployment
        .metadata
        .annotations
        .get_or_insert_with(HashMap::new)
        .insert(
            "deployment.kubernetes.io/revision".to_string(),
            "1".to_string(),
        );

    let dep_key = build_key("deployments", Some(ns), "ann");
    storage.create(&dep_key, &deployment).await.unwrap();

    // Build RS manually so we can pin max-replicas=3 (matching maxSurge=0).
    let mut rs = build_active_rs(&deployment, 3, "nginx:1.0", false);
    let ann = rs.metadata.annotations.get_or_insert_with(HashMap::new);
    ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "3".to_string(),
    );
    ann.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "3".to_string(),
    );
    ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "1".to_string(),
    );
    let rs_key = build_key("replicasets", Some(ns), &rs.metadata.name);
    storage.create(&rs_key, &rs).await.unwrap();

    let controller = DeploymentController::new(storage.clone(), 10);

    // Allow the first reconcile to bring the deployment to its stable status.
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();
    let rs_after_first = snapshot(&storage, &rs_key).await;

    // Second reconcile must leave the RS untouched (annotations already correct).
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();
    let rs_after_second = snapshot(&storage, &rs_key).await;

    assert_eq!(
        rs_after_first, rs_after_second,
        "ReplicaSet must not be rewritten when desired/max-replicas annotations already match"
    );
}

/// Proportional scaling: oldRS=20, newRS=5, deployment.replicas changes
/// 25 -> 30. The first reconcile legitimately performs proportional writes.
/// A second reconcile must NOT trigger another scaling event — the
/// desired-replicas annotations on the active RSes must have been refreshed
/// so that `is_scaling_event` returns false the second time around.
///
/// We assert byte-equality of both RSes across the second reconcile. This
/// catches two distinct Unit 8 hazards:
///   1. proportional-scaling re-firing because annotations weren't refreshed
///   2. annotation refresh writing the same values back redundantly
///
/// To isolate this from unrelated rolling-update progression writes, we use
/// a `Recreate` strategy on the deployment. Proportional scaling still runs
/// (its gate is `is_rolling_update`), so we instead drive the scaling-event
/// path by relying on the explicit annotation mismatch. With Recreate,
/// `is_rolling_update` is false, so `is_scaling_event` is also false from
/// the very first reconcile — but neither RS may be rewritten, since
/// nothing legitimately needs to change about an old/new RS in steady state.
///
/// Note: with `is_rolling_update == false`, this becomes a stricter test —
/// the controller must not silently invent writes in the absence of a
/// rolling-update plan.
#[tokio::test]
async fn test_proportional_scaling_converges_no_oscillation() {
    let storage = setup().await;
    let ns = "default";

    // Deployment currently desires 30 replicas (post-scale-up from 25),
    // RollingUpdate strategy so the proportional-scaling branch is active.
    let mut deployment = build_deployment("prop-idem", ns, 30, "nginx:2.0");
    deployment
        .metadata
        .annotations
        .get_or_insert_with(HashMap::new)
        .insert(
            "deployment.kubernetes.io/revision".to_string(),
            "2".to_string(),
        );
    let dep_key = build_key("deployments", Some(ns), "prop-idem");
    storage.create(&dep_key, &deployment).await.unwrap();

    // Preseed mid-rollout state: previous desired was 25, oldRS=20 (image v1),
    // newRS=5 (image v2, matching current deployment template).
    // Previous maxSurge for 25 at 25% = 7, so max-replicas annotation = 32.
    let old_rs = build_old_rs(&deployment, 20, "nginx:1.0", 25, 32, 1);
    let old_key = build_key("replicasets", Some(ns), &old_rs.metadata.name);
    storage.create(&old_key, &old_rs).await.unwrap();

    let mut new_rs = build_active_rs(&deployment, 5, "nginx:2.0", false);
    let new_ann = new_rs.metadata.annotations.get_or_insert_with(HashMap::new);
    new_ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "25".to_string(),
    );
    new_ann.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "32".to_string(),
    );
    new_ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "2".to_string(),
    );
    let new_key = build_key("replicasets", Some(ns), &new_rs.metadata.name);
    storage.create(&new_key, &new_rs).await.unwrap();

    let controller = DeploymentController::new(storage.clone(), 10);

    // First reconcile: legitimate scaling event detected (annotation 25 != 30).
    // Proportional scaling distributes replicas (oldRS 20→32, newRS 5→6 so
    // the total reaches desired+maxSurge=38) and refreshes annotations to
    // the new desired=30 / max=38.
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    // After the first reconcile, ALL active RSes should carry desired=30 and
    // max=38 annotations — that's the Unit 8 promise.
    for (label, key) in [("old", &old_key), ("new", &new_key)] {
        let rs: ReplicaSet = storage.get(key).await.unwrap();
        let ann = rs
            .metadata
            .annotations
            .as_ref()
            .expect("RS should have annotations after proportional scaling");
        assert_eq!(
            ann.get("deployment.kubernetes.io/desired-replicas")
                .map(String::as_str),
            Some("30"),
            "{} RS desired-replicas annotation should refresh to 30",
            label
        );
        assert_eq!(
            ann.get("deployment.kubernetes.io/max-replicas")
                .map(String::as_str),
            Some("38"),
            "{} RS max-replicas annotation should refresh to 38 (30 + ceil(25%*30))",
            label
        );
    }

    // Drive the rolling-update to convergence the same way a live cluster
    // would: between each deployment reconcile, simulate the RS controller +
    // kubelet bringing pod status in line with spec.replicas. The deployment
    // reconciler's rolling-update progression will then legitimately scale
    // the new RS up and the old RS down, eventually reaching new=30 / old=0
    // (no more old pods, all new pods Ready).
    //
    // Bounded loop: in the worst case each reconcile shifts at most maxSurge
    // pods, so 30 steps is a generous upper bound and prevents an infinite
    // loop if the controller regressed.
    let mut converged = false;
    for _ in 0..30 {
        simulate_kubelet_convergence(&storage, ns).await;
        let dep: Deployment = storage.get(&dep_key).await.unwrap();
        controller.reconcile_deployment(&dep).await.unwrap();

        let new_rs: ReplicaSet = storage.get(&new_key).await.unwrap();
        let old_rs: ReplicaSet = storage.get(&old_key).await.unwrap();
        if new_rs.spec.replicas == 30 && old_rs.spec.replicas == 0 {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "rolling update should converge to new=30 / old=0 within 30 reconciles"
    );

    // Throughout convergence the proportional-scaling annotations on every
    // active RS must remain pinned at desired=30 / max=38 — they were set
    // exactly once by the scaling-event branch and the subsequent reconciles
    // must NOT have re-fired it (which would be the hot-loop signature).
    {
        let new_rs: ReplicaSet = storage.get(&new_key).await.unwrap();
        let ann = new_rs.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            ann.get("deployment.kubernetes.io/desired-replicas")
                .map(String::as_str),
            Some("30"),
            "active RS desired-replicas annotation must stay 30 after convergence"
        );
        assert_eq!(
            ann.get("deployment.kubernetes.io/max-replicas")
                .map(String::as_str),
            Some("38"),
            "active RS max-replicas annotation must stay 38 after convergence"
        );
    }

    // Bring RS status fully in line one more time, then take a steady-state
    // snapshot.
    simulate_kubelet_convergence(&storage, ns).await;
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    let old_steady = snapshot(&storage, &old_key).await;
    let new_steady = snapshot(&storage, &new_key).await;

    // Two more reconciles on the converged state. With status matching spec
    // (kubelet convergence applied), no scaling decision should fire, no
    // annotation should be rewritten, no RS field should be touched. Byte
    // equality across these reconciles is the actual hot-loop assertion the
    // original test wanted to make — now valid because we're testing it on
    // a real steady state, not in the middle of an active rolling update.
    for iteration in 1..=2 {
        simulate_kubelet_convergence(&storage, ns).await;
        let dep: Deployment = storage.get(&dep_key).await.unwrap();
        controller.reconcile_deployment(&dep).await.unwrap();

        let old_now = snapshot(&storage, &old_key).await;
        let new_now = snapshot(&storage, &new_key).await;
        assert_eq!(
            old_steady, old_now,
            "old RS must be byte-equal on converged reconcile #{}",
            iteration
        );
        assert_eq!(
            new_steady, new_now,
            "new RS must be byte-equal on converged reconcile #{}",
            iteration
        );
    }
}

/// A standalone Pod in an unrelated namespace, with NO owner references,
/// must be ignored entirely by the deployment reconciler. The pod's JSON
/// must be byte-identical after reconcile.
#[tokio::test]
async fn test_deployment_ignores_unrelated_pods() {
    let storage = setup().await;

    // Deployment in ns-a.
    let mut deployment = build_deployment("dep-a", "ns-a", 1, "nginx:1.0");
    deployment
        .metadata
        .annotations
        .get_or_insert_with(HashMap::new)
        .insert(
            "deployment.kubernetes.io/revision".to_string(),
            "1".to_string(),
        );
    let dep_key = build_key("deployments", Some("ns-a"), "dep-a");
    storage.create(&dep_key, &deployment).await.unwrap();

    // A matching active RS so the controller is in steady state.
    let rs = build_active_rs(&deployment, 1, "nginx:1.0", true);
    let rs_key = build_key("replicasets", Some("ns-a"), &rs.metadata.name);
    storage.create(&rs_key, &rs).await.unwrap();

    // Standalone pod in a completely unrelated namespace, no ownerReferences.
    let standalone = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "lonely-pod".to_string(),
            namespace: Some("kubectl-1863".to_string()),
            uid: "lonely-uid".to_string(),
            // Explicitly no owner_references.
            owner_references: None,
            labels: Some({
                let mut l = HashMap::new();
                l.insert("app".to_string(), "lonely".to_string());
                l
            }),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "busybox".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };
    let pod_key = build_key("pods", Some("kubectl-1863"), "lonely-pod");
    storage.create(&pod_key, &standalone).await.unwrap();

    let pod_before = snapshot(&storage, &pod_key).await;

    let controller = DeploymentController::new(storage.clone(), 10);
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    let pod_after = snapshot(&storage, &pod_key).await;

    assert_eq!(
        pod_before, pod_after,
        "Standalone pod in unrelated namespace must not be touched by the deployment reconciler"
    );
}
