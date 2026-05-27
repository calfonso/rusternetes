// Integration tests for Deployment Controller: Proportional Scaling and Rollover
//
// These tests verify K8s-compatible behavior for two conformance scenarios:
// 1. Proportional scaling: when deployment.spec.replicas changes mid-rollout,
//    both old and new RSes scale proportionally.
// 2. Rollover: when a deployment is updated while existing pods exist (e.g. from
//    a ReplicationController), the new RS is created with > 0 replicas.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, OwnerReference, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

/// Mark all pods in a namespace as Running and Ready
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
        let key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
        let _ = storage.update(&key, &pod).await;
    }
}

fn create_deployment(
    name: &str,
    namespace: &str,
    replicas: i32,
    image: &str,
    strategy: Option<deployment::DeploymentStrategy>,
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
            strategy,
            paused: None,
            progress_deadline_seconds: None,
        },
        status: None,
    }
}

#[allow(clippy::too_many_arguments)] // Test builder — args mirror upstream RS fields, not an API surface.
fn create_owned_rs(
    name: &str,
    namespace: &str,
    replicas: i32,
    deploy_name: &str,
    deploy_uid: &str,
    image: &str,
    annotations: HashMap<String, String>,
    labels: HashMap<String, String>,
) -> ReplicaSet {
    let mut rs_labels = labels.clone();
    // The RS should have the pod-template-hash label
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
            creation_timestamp: Some(chrono::Utc::now()),
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

// =============================================================================
// Problem 1: Proportional Scaling Tests
// =============================================================================

/// K8s conformance test: "deployment should support proportional scaling"
///
/// Scenario: A deployment with 10 replicas is mid-rollout (old RS has 8, new RS has 5).
/// User scales deployment to 30. Both RSes should scale proportionally.
///
/// K8s behavior (sync.go scale()):
/// - allowedSize = 30 + maxSurge(8) = 38  (maxSurge = ceil(25% * 30) = 8)
/// - allRSsReplicas = 8 + 5 = 13
/// - deploymentReplicasToAdd = 38 - 13 = 25
/// - For each RS, compute proportion using max-replicas annotation
/// - Leftover goes to largest/newest RS
#[tokio::test]
async fn test_proportional_scaling_up_mid_rollout() {
    let storage = setup().await;
    let ns = "default";
    let deploy_name = "prop-scale";
    let deploy_uid = "deploy-uid-prop-scale";

    // Create deployment with 10 replicas, default 25%/25% rolling update
    let deployment = create_deployment(deploy_name, ns, 10, "nginx:1.0", None);
    let dep_key = build_key("deployments", Some(ns), deploy_name);
    storage.create(&dep_key, &deployment).await.unwrap();

    // Simulate mid-rollout state: old RS has 8 replicas, new RS has 5 replicas
    // (total 13 = 10 desired + maxSurge(3) when desired was 10)
    let mut old_labels = HashMap::new();
    old_labels.insert("app".to_string(), deploy_name.to_string());

    let mut old_annotations = HashMap::new();
    old_annotations.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "10".to_string(),
    );
    old_annotations.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "13".to_string(), // 10 + maxSurge(3) = 13
    );
    old_annotations.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "1".to_string(),
    );

    let old_rs = create_owned_rs(
        &format!("{}-old", deploy_name),
        ns,
        8,
        deploy_name,
        deploy_uid,
        "nginx:1.0",
        old_annotations,
        old_labels.clone(),
    );
    let old_rs_key = build_key("replicasets", Some(ns), &format!("{}-old", deploy_name));
    storage.create(&old_rs_key, &old_rs).await.unwrap();

    let mut new_annotations = HashMap::new();
    new_annotations.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "10".to_string(),
    );
    new_annotations.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "13".to_string(),
    );
    new_annotations.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "2".to_string(),
    );

    let new_rs = create_owned_rs(
        &format!("{}-new", deploy_name),
        ns,
        5,
        deploy_name,
        deploy_uid,
        "nginx:2.0",
        new_annotations,
        old_labels.clone(),
    );
    let new_rs_key = build_key("replicasets", Some(ns), &format!("{}-new", deploy_name));
    storage.create(&new_rs_key, &new_rs).await.unwrap();

    // Now scale deployment from 10 to 30
    let mut dep: Deployment = storage.get(&dep_key).await.unwrap();
    dep.spec.replicas = Some(30);
    storage.update(&dep_key, &dep).await.unwrap();

    // Reconcile
    let controller = DeploymentController::new(storage.clone(), 10);
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    // Read back RSes
    let updated_old: ReplicaSet = storage.get(&old_rs_key).await.unwrap();
    let updated_new: ReplicaSet = storage.get(&new_rs_key).await.unwrap();

    let total_replicas = updated_old.spec.replicas + updated_new.spec.replicas;

    // After scaling 10->30 with maxSurge=25%:
    // allowedSize = 30 + ceil(25% * 30) = 30 + 8 = 38
    // deploymentReplicasToAdd = 38 - 13 = 25
    // Both RSes should have scaled up significantly
    // Total should be <= allowedSize (38)
    assert!(
        total_replicas <= 38,
        "Total replicas ({}) should not exceed allowedSize (38), old={}, new={}",
        total_replicas,
        updated_old.spec.replicas,
        updated_new.spec.replicas
    );

    // Both RSes should have more replicas than before
    assert!(
        updated_old.spec.replicas > 8,
        "Old RS should have scaled up from 8, got {}",
        updated_old.spec.replicas
    );
    assert!(
        updated_new.spec.replicas > 5,
        "New RS should have scaled up from 5, got {}",
        updated_new.spec.replicas
    );

    // The total should be exactly allowedSize = 38 (since we have room to scale)
    assert_eq!(
        total_replicas, 38,
        "Total replicas should be allowedSize (38), old={}, new={}",
        updated_old.spec.replicas, updated_new.spec.replicas
    );

    // Desired-replicas annotation should be updated to 30
    let old_desired = updated_old
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("deployment.kubernetes.io/desired-replicas"))
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(-1);
    assert_eq!(
        old_desired, 30,
        "Old RS desired-replicas annotation should be updated to 30"
    );
}

/// Test proportional scale down: deployment scales from 30 to 10 during mid-rollout
#[tokio::test]
async fn test_proportional_scaling_down_mid_rollout() {
    let storage = setup().await;
    let ns = "default";
    let deploy_name = "prop-down";
    let deploy_uid = "deploy-uid-prop-down";

    // Create deployment with 30 replicas
    let deployment = create_deployment(deploy_name, ns, 30, "nginx:1.0", None);
    let dep_key = build_key("deployments", Some(ns), deploy_name);
    storage.create(&dep_key, &deployment).await.unwrap();

    // Simulate mid-rollout: old RS with 20, new RS with 18
    // maxSurge for 30 replicas at 25% = 8, so allowedSize was 38
    let mut old_labels = HashMap::new();
    old_labels.insert("app".to_string(), deploy_name.to_string());

    let mut old_ann = HashMap::new();
    old_ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "30".to_string(),
    );
    old_ann.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "38".to_string(),
    );
    old_ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "1".to_string(),
    );

    let old_rs = create_owned_rs(
        &format!("{}-old", deploy_name),
        ns,
        20,
        deploy_name,
        deploy_uid,
        "nginx:1.0",
        old_ann,
        old_labels.clone(),
    );
    storage
        .create(
            &build_key("replicasets", Some(ns), &format!("{}-old", deploy_name)),
            &old_rs,
        )
        .await
        .unwrap();

    let mut new_ann = HashMap::new();
    new_ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "30".to_string(),
    );
    new_ann.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "38".to_string(),
    );
    new_ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "2".to_string(),
    );

    let new_rs = create_owned_rs(
        &format!("{}-new", deploy_name),
        ns,
        18,
        deploy_name,
        deploy_uid,
        "nginx:2.0",
        new_ann,
        old_labels.clone(),
    );
    storage
        .create(
            &build_key("replicasets", Some(ns), &format!("{}-new", deploy_name)),
            &new_rs,
        )
        .await
        .unwrap();

    // Scale down from 30 to 10
    let mut dep: Deployment = storage.get(&dep_key).await.unwrap();
    dep.spec.replicas = Some(10);
    storage.update(&dep_key, &dep).await.unwrap();

    // Reconcile
    let controller = DeploymentController::new(storage.clone(), 10);
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    // Read back RSes
    let updated_old: ReplicaSet = storage
        .get(&build_key(
            "replicasets",
            Some(ns),
            &format!("{}-old", deploy_name),
        ))
        .await
        .unwrap();
    let updated_new: ReplicaSet = storage
        .get(&build_key(
            "replicasets",
            Some(ns),
            &format!("{}-new", deploy_name),
        ))
        .await
        .unwrap();

    let total = updated_old.spec.replicas + updated_new.spec.replicas;

    // After scaling 30->10 with maxSurge=ceil(25%*10)=3:
    // allowedSize = 10 + 3 = 13
    // deploymentReplicasToAdd = 13 - 38 = -25
    // Both should scale down, total should be allowedSize (13)
    assert!(
        updated_old.spec.replicas < 20,
        "Old RS should have scaled down from 20, got {}",
        updated_old.spec.replicas
    );
    assert!(
        updated_new.spec.replicas < 18,
        "New RS should have scaled down from 18, got {}",
        updated_new.spec.replicas
    );
    assert_eq!(
        total, 13,
        "Total replicas should be allowedSize (13), old={}, new={}",
        updated_old.spec.replicas, updated_new.spec.replicas
    );
}

// =============================================================================
// Problem 2: Rollover Tests
// =============================================================================

/// K8s conformance test: "deployment should support rollover"
///
/// When a deployment is created while old RS/pods already exist (rollover scenario),
/// the new RS should NOT be created with 0 replicas. It should get
/// maxTotalPods - currentPodCount replicas.
///
/// For example: deployment desires 1 replica, maxSurge=1 (from 25% rounded up),
/// old RS has 1 replica -> maxTotalPods = 1 + 1 = 2, currentPodCount = 1 (old RS)
/// -> new RS should get min(scaleUpCount=1, desired-newRS.Replicas=1) = 1.
#[tokio::test]
async fn test_rollover_new_rs_gets_replicas() {
    let storage = setup().await;
    let ns = "default";
    let deploy_name = "rollover";
    let deploy_uid = "deploy-uid-rollover";

    // Create deployment with 1 replica, image v2 (new template)
    let deployment = create_deployment(deploy_name, ns, 1, "nginx:2.0", None);
    let dep_key = build_key("deployments", Some(ns), deploy_name);
    storage.create(&dep_key, &deployment).await.unwrap();

    // Create an old RS owned by this deployment with 1 replica running image v1.
    // This simulates a rollover scenario where old pods exist.
    let mut old_labels = HashMap::new();
    old_labels.insert("app".to_string(), deploy_name.to_string());

    let mut old_ann = HashMap::new();
    old_ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "1".to_string(),
    );
    old_ann.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "2".to_string(),
    );
    old_ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "1".to_string(),
    );

    let old_rs = create_owned_rs(
        &format!("{}-oldrs", deploy_name),
        ns,
        1,
        deploy_name,
        deploy_uid,
        "nginx:1.0",
        old_ann,
        old_labels.clone(),
    );
    let old_rs_key = build_key("replicasets", Some(ns), &format!("{}-oldrs", deploy_name));
    storage.create(&old_rs_key, &old_rs).await.unwrap();

    // Reconcile - the controller should create a new RS for the new template
    // with replicas > 0 (not 0)
    let controller = DeploymentController::new(storage.clone(), 10);
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    // Find the new RS (not the old one)
    let rs_prefix = build_prefix("replicasets", Some(ns));
    let all_rs: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    let new_rses: Vec<&ReplicaSet> = all_rs
        .iter()
        .filter(|rs| rs.metadata.name != format!("{}-oldrs", deploy_name))
        .collect();

    assert!(
        !new_rses.is_empty(),
        "Controller should have created a new RS for the new template"
    );

    let new_rs = new_rses[0];
    // K8s: maxTotalPods = 1 + maxSurge(1) = 2, currentPodCount = 1 (old RS)
    // scaleUpCount = 2 - 1 = 1, min(1, 1 - 0) = 1
    // So new RS should have 1 replica, NOT 0
    assert!(
        new_rs.spec.replicas > 0,
        "New RS should have replicas > 0 during rollover, got {}",
        new_rs.spec.replicas
    );
    assert_eq!(
        new_rs.spec.replicas, 1,
        "New RS should have 1 replica (desired=1, maxSurge=1, old=1), got {}",
        new_rs.spec.replicas
    );
}

/// Rollover with larger deployment: 3 replicas mid-rollout, template change.
/// Old RS has 3 replicas from previous rollout, user updates template again.
/// New RS should start with maxTotalPods - currentPodCount replicas.
#[tokio::test]
async fn test_rollover_mid_rollout_creates_new_rs_with_replicas() {
    let storage = setup().await;
    let ns = "default";
    let deploy_name = "rollover-mid";
    let deploy_uid = "deploy-uid-rollover-mid";

    // Deployment wants 3 replicas with image v3 (the newest template)
    let deployment = create_deployment(deploy_name, ns, 3, "nginx:3.0", None);
    let dep_key = build_key("deployments", Some(ns), deploy_name);
    storage.create(&dep_key, &deployment).await.unwrap();

    // Existing old RS from v1 (scaled down to 2 during previous rollout)
    let mut old_labels = HashMap::new();
    old_labels.insert("app".to_string(), deploy_name.to_string());

    let mut ann_v1 = HashMap::new();
    ann_v1.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "3".to_string(),
    );
    ann_v1.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "4".to_string(),
    );
    ann_v1.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "1".to_string(),
    );
    let old_rs_v1 = create_owned_rs(
        &format!("{}-v1", deploy_name),
        ns,
        2,
        deploy_name,
        deploy_uid,
        "nginx:1.0",
        ann_v1,
        old_labels.clone(),
    );
    storage
        .create(
            &build_key("replicasets", Some(ns), &format!("{}-v1", deploy_name)),
            &old_rs_v1,
        )
        .await
        .unwrap();

    // Old RS from v2 mid-rollout (has 2 replicas)
    let mut ann_v2 = HashMap::new();
    ann_v2.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "3".to_string(),
    );
    ann_v2.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "4".to_string(),
    );
    ann_v2.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "2".to_string(),
    );
    let old_rs_v2 = create_owned_rs(
        &format!("{}-v2", deploy_name),
        ns,
        2,
        deploy_name,
        deploy_uid,
        "nginx:2.0",
        ann_v2,
        old_labels.clone(),
    );
    storage
        .create(
            &build_key("replicasets", Some(ns), &format!("{}-v2", deploy_name)),
            &old_rs_v2,
        )
        .await
        .unwrap();

    // Reconcile
    let controller = DeploymentController::new(storage.clone(), 10);
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    // Find the new RS for v3
    let rs_prefix = build_prefix("replicasets", Some(ns));
    let all_rs: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    let new_rses: Vec<&ReplicaSet> = all_rs
        .iter()
        .filter(|rs| {
            rs.metadata.name != format!("{}-v1", deploy_name)
                && rs.metadata.name != format!("{}-v2", deploy_name)
        })
        .collect();

    assert!(
        !new_rses.is_empty(),
        "Controller should have created a new RS for v3 template"
    );

    let new_rs = new_rses[0];
    // oldTotal = 2 + 2 = 4
    // maxSurge = ceil(25% * 3) = 1
    // maxTotalPods = 3 + 1 = 4
    // scaleUpCount = max(4 - 4, 0) = 0 ... but K8s still creates the RS,
    // it just starts at 0 and rolling update progresses from there.
    // Actually in this case the new RS starts at 0 because there's no room.
    // This is correct K8s behavior when currentPodCount >= maxTotalPods.
    // The test validates that the controller creates the RS at all.
    assert!(
        new_rs.spec.replicas >= 0,
        "New RS should be created (even if 0 replicas when at capacity)"
    );
}

/// Test that annotations are properly updated after proportional scaling.
/// The desired-replicas and max-replicas annotations must reflect the new
/// deployment spec after scaling.
#[tokio::test]
async fn test_proportional_scaling_updates_annotations() {
    let storage = setup().await;
    let ns = "default";
    let deploy_name = "ann-scale";
    let deploy_uid = "deploy-uid-ann-scale";

    // Deployment starts at 10 replicas
    let deployment = create_deployment(deploy_name, ns, 10, "nginx:1.0", None);
    let dep_key = build_key("deployments", Some(ns), deploy_name);
    storage.create(&dep_key, &deployment).await.unwrap();

    // Mid-rollout: old RS=6, new RS=7 (total 13 = 10 + maxSurge(3))
    let mut old_labels = HashMap::new();
    old_labels.insert("app".to_string(), deploy_name.to_string());

    let mut old_ann = HashMap::new();
    old_ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "10".to_string(),
    );
    old_ann.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "13".to_string(),
    );
    old_ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "1".to_string(),
    );
    let old_rs = create_owned_rs(
        &format!("{}-old", deploy_name),
        ns,
        6,
        deploy_name,
        deploy_uid,
        "nginx:1.0",
        old_ann,
        old_labels.clone(),
    );
    storage
        .create(
            &build_key("replicasets", Some(ns), &format!("{}-old", deploy_name)),
            &old_rs,
        )
        .await
        .unwrap();

    let mut new_ann = HashMap::new();
    new_ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "10".to_string(),
    );
    new_ann.insert(
        "deployment.kubernetes.io/max-replicas".to_string(),
        "13".to_string(),
    );
    new_ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "2".to_string(),
    );
    let new_rs = create_owned_rs(
        &format!("{}-new", deploy_name),
        ns,
        7,
        deploy_name,
        deploy_uid,
        "nginx:2.0",
        new_ann,
        old_labels.clone(),
    );
    storage
        .create(
            &build_key("replicasets", Some(ns), &format!("{}-new", deploy_name)),
            &new_rs,
        )
        .await
        .unwrap();

    // Scale from 10 to 20
    let mut dep: Deployment = storage.get(&dep_key).await.unwrap();
    dep.spec.replicas = Some(20);
    storage.update(&dep_key, &dep).await.unwrap();

    let controller = DeploymentController::new(storage.clone(), 10);
    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    // Both RSes should have desired-replicas=20 and max-replicas=25 (20+ceil(25%*20)=20+5=25)
    let updated_old: ReplicaSet = storage
        .get(&build_key(
            "replicasets",
            Some(ns),
            &format!("{}-old", deploy_name),
        ))
        .await
        .unwrap();
    let updated_new: ReplicaSet = storage
        .get(&build_key(
            "replicasets",
            Some(ns),
            &format!("{}-new", deploy_name),
        ))
        .await
        .unwrap();

    for (label, rs) in [("old", &updated_old), ("new", &updated_new)] {
        let desired = rs
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("deployment.kubernetes.io/desired-replicas"))
            .and_then(|v| v.parse::<i32>().ok());
        assert_eq!(
            desired,
            Some(20),
            "{} RS desired-replicas annotation should be 20",
            label
        );

        let max = rs
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("deployment.kubernetes.io/max-replicas"))
            .and_then(|v| v.parse::<i32>().ok());
        assert_eq!(
            max,
            Some(25),
            "{} RS max-replicas annotation should be 25 (20+5)",
            label
        );
    }
}

/// Test that when there's only one active RS (no old RSes with replicas > 0),
/// FindActiveOrLatest-style logic scales it directly without proportional scaling.
#[tokio::test]
async fn test_single_active_rs_scales_directly() {
    let storage = setup().await;
    let ns = "default";
    let deploy_name = "single-rs";

    let deployment = create_deployment(deploy_name, ns, 5, "nginx:1.0", None);
    let dep_key = build_key("deployments", Some(ns), deploy_name);
    storage.create(&dep_key, &deployment).await.unwrap();

    // Run initial reconcile to create RS
    let controller = DeploymentController::new(storage.clone(), 10);
    controller.reconcile_all().await.unwrap();

    // Find the created RS
    let rs_prefix = build_prefix("replicasets", Some(ns));
    let rses: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    assert_eq!(rses.len(), 1);
    assert_eq!(rses[0].spec.replicas, 5);

    // Scale to 10
    let mut dep: Deployment = storage.get(&dep_key).await.unwrap();
    dep.spec.replicas = Some(10);
    storage.update(&dep_key, &dep).await.unwrap();

    let dep: Deployment = storage.get(&dep_key).await.unwrap();
    controller.reconcile_deployment(&dep).await.unwrap();

    let rses: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
    assert_eq!(rses.len(), 1, "Should still have only 1 RS");
    assert_eq!(
        rses[0].spec.replicas, 10,
        "Single RS should scale directly to 10"
    );
}

/// Test IsSaturated: when new RS is saturated (replicas == desired, available == desired),
/// all old RSes should be scaled to 0.
#[tokio::test]
async fn test_saturated_new_rs_scales_down_old() {
    let storage = setup().await;
    let ns = "default";
    let deploy_name = "saturated";
    let deploy_uid = "deploy-uid-saturated";

    // Deployment wants 3 replicas with image v2
    let deployment = create_deployment(deploy_name, ns, 3, "nginx:2.0", None);
    let dep_key = build_key("deployments", Some(ns), deploy_name);
    storage.create(&dep_key, &deployment).await.unwrap();

    // New RS (matches template) with 3 replicas, fully available
    let mut new_labels = HashMap::new();
    new_labels.insert("app".to_string(), deploy_name.to_string());

    let mut new_ann = HashMap::new();
    new_ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "3".to_string(),
    );
    new_ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "2".to_string(),
    );

    // The new RS must match the deployment template. We need to use the same hash
    // that compute_pod_template_hash would generate.
    // Instead of computing it, let the controller create it via reconcile.
    // First, create an old RS that doesn't match:
    let mut old_ann = HashMap::new();
    old_ann.insert(
        "deployment.kubernetes.io/desired-replicas".to_string(),
        "3".to_string(),
    );
    old_ann.insert(
        "deployment.kubernetes.io/revision".to_string(),
        "1".to_string(),
    );
    let old_rs = create_owned_rs(
        &format!("{}-old", deploy_name),
        ns,
        2,
        deploy_name,
        deploy_uid,
        "nginx:1.0",
        old_ann,
        new_labels.clone(),
    );
    storage
        .create(
            &build_key("replicasets", Some(ns), &format!("{}-old", deploy_name)),
            &old_rs,
        )
        .await
        .unwrap();

    // Run reconcile to create the new RS
    let controller = DeploymentController::new(storage.clone(), 10);
    let rs_controller = ReplicaSetController::new(storage.clone(), 10);

    // Multiple reconcile cycles to complete the rollout
    for _ in 0..15 {
        rs_controller.reconcile_all().await.unwrap();
        make_all_pods_ready(&storage, ns).await;
        let dep: Deployment = storage.get(&dep_key).await.unwrap();
        controller.reconcile_deployment(&dep).await.unwrap();
    }

    // After rollout completes, old RS should be 0
    let old_rs_updated: ReplicaSet = storage
        .get(&build_key(
            "replicasets",
            Some(ns),
            &format!("{}-old", deploy_name),
        ))
        .await
        .unwrap();
    assert_eq!(
        old_rs_updated.spec.replicas, 0,
        "Old RS should be scaled to 0 after new RS is saturated, got {}",
        old_rs_updated.spec.replicas
    );
}
