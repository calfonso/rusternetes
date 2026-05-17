// Integration tests for Deployment proportional scaling rounding.
//
// Upstream conformance: test/e2e/apps/deployment.go — "deployment should
// support proportional scaling" (testProportionalScalingDeployment).
//
// K8s sorts ReplicaSets by spec.replicas DESC before distributing the
// `deploymentReplicasToAdd` delta. On ties the secondary key is creation
// timestamp: oldest-first when scaling down, newest-first when scaling up.
// If the controller iterates RSes in storage/HashMap order (non-deterministic)
// the proportional split rounds incorrectly and the smaller RS absorbs the
// whole delta — failing the conformance assertion.
//
// This test exercises a deterministic scale-DOWN scenario where the bug is
// observable: deployment at 10 replicas, mid-rollout (old RS=6, new RS=4),
// user scales the deployment to 5. The split must place 3 replicas on the
// old RS and 4 on the new RS — not the inverse.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, OwnerReference, TypeMeta};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn rolling_update_strategy(max_surge: i64, max_unavailable: i64) -> deployment::DeploymentStrategy {
    deployment::DeploymentStrategy {
        strategy_type: "RollingUpdate".to_string(),
        rolling_update: Some(deployment::RollingUpdateDeployment {
            max_surge: Some(serde_json::Value::from(max_surge)),
            max_unavailable: Some(serde_json::Value::from(max_unavailable)),
        }),
    }
}

fn create_deployment(
    name: &str,
    namespace: &str,
    replicas: i32,
    image: &str,
    strategy: deployment::DeploymentStrategy,
) -> Deployment {
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
            strategy: Some(strategy),
            paused: None,
            progress_deadline_seconds: None,
        },
        status: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn create_owned_rs(
    name: &str,
    namespace: &str,
    replicas: i32,
    deploy_name: &str,
    deploy_uid: &str,
    image: &str,
    annotations: HashMap<String, String>,
    labels: HashMap<String, String>,
    creation_offset_secs: i64,
) -> ReplicaSet {
    let mut rs_labels = labels.clone();
    if !rs_labels.contains_key("pod-template-hash") {
        rs_labels.insert(
            "pod-template-hash".to_string(),
            name.rsplit('-').next().unwrap_or("hash").to_string(),
        );
    }

    ReplicaSet {
        type_meta: TypeMeta {
            kind: "ReplicaSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: format!("rs-uid-{}", name),
            labels: Some(rs_labels.clone()),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                name: deploy_name.to_string(),
                uid: deploy_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            creation_timestamp: Some(
                chrono::Utc::now() + chrono::Duration::seconds(creation_offset_secs),
            ),
            ..Default::default()
        },
        spec: ReplicaSetSpec {
            replicas,
            selector: LabelSelector {
                match_labels: Some(rs_labels.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new("");
                    meta.labels = Some(rs_labels);
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

fn rs_annotations(desired: i32, max_replicas: i32, revision: i32) -> HashMap<String, String> {
    let mut a = HashMap::new();
    a.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        desired.to_string(),
    );
    a.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        max_replicas.to_string(),
    );
    a.insert(
        "deployment.kubernetes.io/revision".to_string(),
        revision.to_string(),
    );
    a
}

/// Scale-down proportional rounding.
///
/// Setup: Deployment at 10 replicas, RollingUpdate(maxSurge=2, maxUnavailable=2).
/// Mid-rollout state: oldRS=6 replicas, newRS=4 replicas. Both annotated with
/// desired=10, max-replicas=12 (the surge ceiling at the time the rollout
/// started).
///
/// User scales the Deployment from 10 down to 5.
///
/// K8s sync.go scale():
///   allowedSize = 5 + maxSurge(2) = 7
///   allRSsReplicas = 6 + 4 = 10
///   deploymentReplicasToAdd = 7 - 10 = -3   (scale-down direction)
///
/// Sort RSes BySizeOlder: larger first (oldRS, 6), then smaller (newRS, 4).
///
/// getReplicaSetFraction(rs):
///   newSize = rs.spec.replicas * allowedSize / annotatedReplicas
///   fraction = round(newSize) - rs.spec.replicas
///
/// oldRS: round(6 * 7 / 12) - 6 = round(3.5) - 6 = 4 - 6 = -2
///   allowed = -3 - 0 = -3
///   proportion = max(rsFraction=-2, allowed=-3) = -2  → new = 6 + (-2) = 4
///
/// newRS: round(4 * 7 / 12) - 4 = round(2.33) - 4 = 2 - 4 = -2
///   allowed = -3 - (-2) = -1
///   proportion = max(rsFraction=-2, allowed=-1) = -1  → new = 4 + (-1) = 3
///
/// Expected K8s split: oldRS=4, newRS=3, total=7=allowedSize.
///
/// Bug today: the controller iterates `owned_replicasets` in HashMap (storage)
/// order, not size-sorted. When the new RS is visited first, it absorbs the
/// full -2 fraction and the old RS only loses -1 — flipping the distribution
/// (oldRS=5, newRS=2). The total is still 7 but the larger/older RS keeps
/// "too many" replicas, which violates K8s' canonical rounding.
#[tokio::test]
async fn test_proportional_scale_down_rounds_larger_rs_first() {
    let storage = setup().await;
    let ns = "default";
    let deploy_name = "prop-round";
    let deploy_uid = "deploy-uid-prop-round";

    // Explicit RollingUpdate strategy: integer maxSurge=2, maxUnavailable=2.
    // Keeping it integer (not a percentage) makes the arithmetic exact and
    // independent of any rounding inside parse_int_or_percent.
    let strategy = rolling_update_strategy(2, 2);
    let deployment = create_deployment(deploy_name, ns, 10, "nginx:1.0", strategy);
    let dep_key = build_key("deployments", Some(ns), deploy_name);
    storage.create(&dep_key, &deployment).await.unwrap();

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), deploy_name.to_string());

    // Old RS — created earlier (creation_offset_secs = -10).
    // desired=10, max-replicas=12 (= 10 + maxSurge(2)).
    let old_rs = create_owned_rs(
        &format!("{}-old", deploy_name),
        ns,
        6,
        deploy_name,
        deploy_uid,
        "nginx:1.0",
        rs_annotations(10, 12, 1),
        labels.clone(),
        -10,
    );
    let old_key = build_key("replicasets", Some(ns), &format!("{}-old", deploy_name));
    storage.create(&old_key, &old_rs).await.unwrap();

    // New RS — created later (creation_offset_secs = 0).
    let new_rs = create_owned_rs(
        &format!("{}-new", deploy_name),
        ns,
        4,
        deploy_name,
        deploy_uid,
        "nginx:2.0",
        rs_annotations(10, 12, 2),
        labels.clone(),
        0,
    );
    let new_key = build_key("replicasets", Some(ns), &format!("{}-new", deploy_name));
    storage.create(&new_key, &new_rs).await.unwrap();

    // Scale the Deployment from 10 to 5.
    let mut dep: Deployment = storage.get(&dep_key).await.unwrap();
    dep.spec.replicas = Some(5);
    storage.update(&dep_key, &dep).await.unwrap();

    // Reconcile.
    let controller = DeploymentController::new(storage.clone(), 10);
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    let updated_old: ReplicaSet = storage.get(&old_key).await.unwrap();
    let updated_new: ReplicaSet = storage.get(&new_key).await.unwrap();

    let total = updated_old.spec.replicas + updated_new.spec.replicas;
    let allowed_size = 5 + 2;

    // Total must equal allowedSize (= desired + maxSurge).
    assert_eq!(
        total, allowed_size,
        "total replicas must equal allowedSize ({}); got old={} new={}",
        allowed_size, updated_old.spec.replicas, updated_new.spec.replicas
    );

    // Larger RS must absorb its proportional share FIRST.
    //   oldRS:  6 * 7/12 = 3.5 → round 4, fraction = -2
    //   newRS:  4 * 7/12 = 2.33 → round 2, fraction = -2 (capped by allowed = -1)
    // Expected: oldRS=4, newRS=3.
    assert_eq!(
        updated_old.spec.replicas, 4,
        "old RS should be scaled to 4 (its proportional share); got old={} new={}",
        updated_old.spec.replicas, updated_new.spec.replicas
    );
    assert_eq!(
        updated_new.spec.replicas, 3,
        "new RS should be scaled to 3 (allowed leftover); got old={} new={}",
        updated_old.spec.replicas, updated_new.spec.replicas
    );

    // Both RSes must keep more replicas than they would if all -3 fell on one
    // of them — i.e. neither RS scales to 0 and neither stays at its original
    // count. This guards against the "all to one RS" rounding bug.
    assert!(
        updated_old.spec.replicas < 6 && updated_old.spec.replicas > 0,
        "old RS must scale down but not vanish; got {}",
        updated_old.spec.replicas
    );
    assert!(
        updated_new.spec.replicas < 4 && updated_new.spec.replicas > 0,
        "new RS must scale down but not vanish; got {}",
        updated_new.spec.replicas
    );
}
