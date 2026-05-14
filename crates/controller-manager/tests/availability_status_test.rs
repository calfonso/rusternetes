// Regression tests for availability-status fixes.
//
// d7162b4 — RS availableReplicas must not require phase==Running.
// 36ff92b — Deployment availableReplicas must count pods directly, not trust stale RS status.

use rusternetes_common::resources::pod::{PodCondition, PodStatus};
use rusternetes_common::resources::{
    Container, Deployment, DeploymentSpec, DeploymentStatus, Pod, PodSpec, PodTemplateSpec,
    ReplicaSet, ReplicaSetSpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, OwnerReference, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_labels(app: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("app".to_string(), app.to_string());
    m
}

fn make_container() -> Container {
    Container {
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
    }
}

fn make_pod_spec(_labels: HashMap<String, String>) -> PodSpec {
    PodSpec {
        containers: vec![make_container()],
        init_containers: None,
        ephemeral_containers: None,
        volumes: None,
        restart_policy: Some("Always".to_string()),
        node_name: None,
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
    }
}

fn make_replicaset(name: &str, namespace: &str, replicas: i32) -> ReplicaSet {
    let labels = make_labels(name);
    let uid = uuid::Uuid::new_v4().to_string();
    ReplicaSet {
        type_meta: TypeMeta {
            kind: "ReplicaSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uid;
            meta
        },
        spec: ReplicaSetSpec {
            replicas,
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            min_ready_seconds: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels.clone());
                    meta
                }),
                spec: make_pod_spec(labels),
            },
        },
        status: None,
    }
}

/// Build a pod that is owned by `rs`, matches `rs`'s selector, has `Ready=True`
/// condition, but has the given phase (default Pending — deliberately NOT Running).
fn make_ready_pod_with_phase(
    pod_name: &str,
    namespace: &str,
    rs: &ReplicaSet,
    phase: Phase,
) -> Pod {
    let labels = rs.spec.selector.match_labels.clone().unwrap_or_default();

    let owner_ref = OwnerReference::new(
        "apps/v1",
        "ReplicaSet",
        rs.metadata.name.clone(),
        rs.metadata.uid.clone(),
    )
    .with_controller(true);

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(pod_name);
            meta.namespace = Some(namespace.to_string());
            meta.labels = Some(labels);
            meta.owner_references = Some(vec![owner_ref]);
            meta
        },
        spec: Some(rs.spec.template.spec.clone()),
        status: Some(PodStatus {
            phase: Some(phase),
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            ..Default::default()
        }),
    }
}

// ---------------------------------------------------------------------------
// Test for d7162b4 — RS must count Ready pods as available even if phase != Running
// ---------------------------------------------------------------------------

/// Regression test for d7162b4.
///
/// Creates an RS owning 3 pods, all with `Ready=True` but `phase=Pending`
/// (simulating phase transitions). The fix removes the extra `phase==Running`
/// guard from `is_pod_available`, so all 3 pods must count as available.
/// Reverting the fix re-adds the guard and drops the count to 0.
#[tokio::test]
async fn test_rs_available_replicas_ready_non_running_phase() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ReplicaSetController::new(storage.clone(), 10);

    let ns = "default";
    let rs = make_replicaset("avail-test", ns, 3);
    let rs_key = build_key("replicasets", Some(ns), &rs.metadata.name);
    storage.create(&rs_key, &rs).await.unwrap();

    // Insert 3 pods: Ready=True but phase=Pending (NOT Running)
    for i in 0..3 {
        let pod_name = format!("avail-pod-{}", i);
        let pod = make_ready_pod_with_phase(&pod_name, ns, &rs, Phase::Pending);
        let pod_key = build_key("pods", Some(ns), &pod_name);
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile to compute status
    controller.reconcile_all().await.unwrap();

    let updated_rs: ReplicaSet = storage.get(&rs_key).await.unwrap();
    let status = updated_rs
        .status
        .expect("RS must have status after reconcile");

    println!(
        "d7162b4 regression: replicas={}, ready={}, available={}",
        status.replicas, status.ready_replicas, status.available_replicas
    );

    assert_eq!(status.replicas, 3, "RS must report 3 total replicas");
    assert_eq!(
        status.ready_replicas, 3,
        "RS must report 3 ready replicas (Ready=True)"
    );
    assert_eq!(
        status.available_replicas, 3,
        "RS must report 3 available replicas — Ready=True is sufficient, phase==Running must NOT be required (d7162b4)"
    );
}

// ---------------------------------------------------------------------------
// Test for 36ff92b — Deployment must count pods directly to avoid stale RS status
// ---------------------------------------------------------------------------

fn make_deployment(name: &str, namespace: &str, replicas: i32) -> Deployment {
    let labels = make_labels(name);
    let uid = uuid::Uuid::new_v4().to_string();
    Deployment {
        type_meta: TypeMeta {
            kind: "Deployment".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uid;
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
                    meta.labels = Some(labels.clone());
                    meta
                }),
                spec: make_pod_spec(labels),
            },
            strategy: None,
            paused: None,
            progress_deadline_seconds: None,
        },
        status: Some(DeploymentStatus {
            replicas: Some(0),
            ready_replicas: Some(0),
            available_replicas: Some(0),
            unavailable_replicas: Some(0),
            updated_replicas: Some(0),
            conditions: None,
            collision_count: None,
            observed_generation: None,
            terminating_replicas: None,
        }),
    }
}

/// Regression test for 36ff92b.
///
/// Setup:
///   - Deployment owns an RS.
///   - RS has `status.available_replicas = 0`  (stale / not yet updated).
///   - 3 pods owned by that RS are in storage with `Ready=True`.
///
/// After reconciling the Deployment, `Deployment.status.available_replicas`
/// must equal 3 (pod truth wins over stale RS status).
/// Reverting the fix makes the Deployment trust the stale RS field → reports 0.
#[tokio::test]
async fn test_deployment_available_replicas_ignores_stale_rs_status() {
    let storage = Arc::new(MemoryStorage::new());
    let dep_controller = DeploymentController::new(storage.clone(), 10);

    let ns = "default";

    // Create Deployment
    let dep = make_deployment("stale-test", ns, 3);
    let dep_key = build_key("deployments", Some(ns), &dep.metadata.name);
    storage.create(&dep_key, &dep).await.unwrap();

    // Let deployment controller create the RS
    dep_controller.reconcile_all().await.unwrap();

    // Fetch the RS that was created
    let rss: Vec<ReplicaSet> = storage
        .list("/registry/replicasets/default/")
        .await
        .unwrap();
    assert_eq!(rss.len(), 1, "Deployment must create exactly one RS");
    let mut rs = rss.into_iter().next().unwrap();
    let rs_key = build_key("replicasets", Some(ns), &rs.metadata.name);

    // Overwrite RS status to be deliberately stale: 0 available even though pods will be Ready
    use rusternetes_common::resources::workloads::ReplicaSetStatus;
    rs.status = Some(ReplicaSetStatus {
        replicas: 3,
        ready_replicas: 3,
        available_replicas: 0, // stale
        fully_labeled_replicas: Some(3),
        observed_generation: None,
        conditions: None,
        terminating_replicas: None,
    });
    storage.update(&rs_key, &rs).await.unwrap();

    // Insert 3 pods owned by the RS, all Ready=True + Running
    for i in 0..3 {
        let pod_name = format!("stale-pod-{}", i);
        let pod = make_ready_pod_with_phase(&pod_name, ns, &rs, Phase::Running);
        let pod_key = build_key("pods", Some(ns), &pod_name);
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Reconcile the Deployment — it must count pods directly, not trust RS.status
    dep_controller.reconcile_all().await.unwrap();

    let updated_dep: Deployment = storage.get(&dep_key).await.unwrap();
    let status = updated_dep
        .status
        .expect("Deployment must have status after reconcile");

    println!(
        "36ff92b regression: dep available_replicas={:?}",
        status.available_replicas
    );

    assert_eq!(
        status.available_replicas,
        Some(3),
        "Deployment must report 3 available replicas from direct pod count, not stale RS status=0 (36ff92b)"
    );
}
