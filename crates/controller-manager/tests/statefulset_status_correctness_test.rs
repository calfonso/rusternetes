// StatefulSet Status-Correctness Regression Tests
//
// Each test covers one historical bug fix and is designed to fail
// when that fix is reverted in the production code.

use rusternetes_common::resources::pod::PodCondition;
use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::statefulset::StatefulSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn setup() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
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
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
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
                spec: PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
                        image: "nginx:1.25-alpine".to_string(),
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
                },
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

/// Seed a pod directly into storage without going through the controller.
/// The pod has phase=Running and the ss_name label so the controller can find it.
async fn seed_pod(
    storage: &Arc<MemoryStorage>,
    ss_name: &str,
    ss_uid: &str,
    namespace: &str,
    ordinal: i32,
    phase: Phase,
    ready: bool,
    terminating: bool,
) {
    let pod_name = format!("{}-{}", ss_name, ordinal);
    let pod_key = build_key("pods", Some(namespace), &pod_name);

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), ss_name.to_string());
    labels.insert(
        "statefulset.kubernetes.io/pod-name".to_string(),
        pod_name.clone(),
    );

    let conditions = if ready {
        Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_transition_time: Some(chrono::Utc::now()),
            observed_generation: None,
        }])
    } else {
        None
    };

    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(&pod_name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta.labels = Some(labels);
            meta.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "StatefulSet".to_string(),
                name: ss_name.to_string(),
                uid: ss_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            if terminating {
                meta.deletion_timestamp = Some(chrono::Utc::now());
            }
            meta
        },
        spec: None,
        status: Some(PodStatus {
            phase: Some(phase),
            conditions,
            ..Default::default()
        }),
    };
    // Remove spec to keep it simple
    pod.spec = Some(PodSpec {
        containers: vec![Container {
            name: "nginx".to_string(),
            image: "nginx:1.25-alpine".to_string(),
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
    });

    storage.create(&pod_key, &pod).await.unwrap();
}

// ---------------------------------------------------------------------------
// Test c02bdc1 — status.availableReplicas was always None
//
// Bug: available_replicas: None
// Fix: available_replicas: Some(final_ready_pods)
//
// Scenario: 2 ready pods. availableReplicas must be Some(2), not None.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_c02bdc1_available_replicas_not_none() {
    let storage = setup().await;

    let ss = make_statefulset("svc", "default", 2);
    let ss_uid = ss.metadata.uid.clone();
    let key = build_key("statefulsets", Some("default"), "svc");
    storage.create(&key, &ss).await.unwrap();

    // Seed 2 running and ready pods
    for ordinal in 0..2i32 {
        seed_pod(
            &storage,
            "svc",
            &ss_uid,
            "default",
            ordinal,
            Phase::Running,
            true,
            false,
        )
        .await;
    }

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let updated_ss: StatefulSet = storage.get(&key).await.unwrap();
    let status = updated_ss.status.expect("status must be set");

    println!(
        "status.available_replicas = {:?}",
        status.available_replicas
    );

    // The fix: available_replicas must be Some(2).
    // Revert (`available_replicas: None`) causes this assertion to fail.
    assert_eq!(
        status.available_replicas,
        Some(2),
        "availableReplicas should be Some(2) for 2 ready pods, not None"
    );
}

// ---------------------------------------------------------------------------
// Test b1aefae — readyReplicas counts only pods with Ready=True condition
//
// Bug: counted pods as ready if phase==Running (no condition check)
// Fix: requires both phase==Running AND Ready condition == "True"
//
// Scenario: 3 pods are Running, but only 1 has Ready=True.
// status.ready_replicas must be Some(1), not Some(3).
//
// IMPORTANT: we do NOT use the mark_pod_ready helper from the existing
// test file — that sets BOTH phase AND condition.  Here we deliberately
// create the asymmetric state: Running phase but no Ready condition.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_b1aefae_ready_replicas_checks_condition_not_just_phase() {
    let storage = setup().await;

    let ss = make_statefulset("app", "default", 3);
    let ss_uid = ss.metadata.uid.clone();
    let key = build_key("statefulsets", Some("default"), "app");
    storage.create(&key, &ss).await.unwrap();

    // Pod 0: Running + Ready=True  → should count toward readyReplicas
    seed_pod(
        &storage,
        "app",
        &ss_uid,
        "default",
        0,
        Phase::Running,
        true,
        false,
    )
    .await;

    // Pod 1: Running but NO Ready condition  → should NOT count
    seed_pod(
        &storage,
        "app",
        &ss_uid,
        "default",
        1,
        Phase::Running,
        false,
        false,
    )
    .await;

    // Pod 2: Running but NO Ready condition  → should NOT count
    seed_pod(
        &storage,
        "app",
        &ss_uid,
        "default",
        2,
        Phase::Running,
        false,
        false,
    )
    .await;

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let updated_ss: StatefulSet = storage.get(&key).await.unwrap();
    let status = updated_ss.status.expect("status must be set");

    println!(
        "status.ready_replicas = {:?} (3 Running, 1 Ready=True)",
        status.ready_replicas
    );

    // The fix: only 1 pod has Ready=True, so ready_replicas == Some(1).
    // Revert (phase==Running check only) produces Some(3) — assertion fails.
    assert_eq!(
        status.ready_replicas,
        Some(1),
        "readyReplicas should be 1 (only pods with Ready=True condition), not 3 (all Running)"
    );
}

// ---------------------------------------------------------------------------
// Test b0a3215 — scale-down uses graceful termination (deletion_timestamp)
//
// Bug: storage.delete() called directly — pod vanishes immediately
// Fix: sets metadata.deletion_timestamp — pod stays in storage; kubelet handles cleanup
//
// Scenario: SS spec.replicas=3 → 2. After one reconcile the removed pod
// must still exist in storage AND have deletion_timestamp set.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_b0a3215_scale_down_sets_deletion_timestamp_not_hard_delete() {
    let storage = setup().await;

    // SS starts at 3 replicas; we lower it to 2
    let mut ss = make_statefulset("db", "default", 3);
    let ss_uid = ss.metadata.uid.clone();
    let key = build_key("statefulsets", Some("default"), "db");
    storage.create(&key, &ss).await.unwrap();

    // Seed 3 running + ready pods
    for ordinal in 0..3i32 {
        seed_pod(
            &storage,
            "db",
            &ss_uid,
            "default",
            ordinal,
            Phase::Running,
            true,
            false,
        )
        .await;
    }

    // Scale down spec to 2
    ss.spec.replicas = Some(2);
    storage.update(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // pod-2 (ordinal 2) is the highest condemned pod.
    let pod2_key = build_key("pods", Some("default"), "db-2");

    // The fix: pod must still be present in storage (graceful — not hard-deleted)
    let pod2: Pod = storage
        .get(&pod2_key)
        .await
        .expect("db-2 must still exist in storage (graceful termination, not hard-deleted)");

    println!(
        "db-2 deletion_timestamp = {:?}",
        pod2.metadata.deletion_timestamp
    );

    // AND the fix sets deletion_timestamp
    assert!(
        pod2.metadata.deletion_timestamp.is_some(),
        "db-2 must have deletion_timestamp set (graceful scale-down)"
    );
}

// ---------------------------------------------------------------------------
// Test 6573091 — scale-down processes one pod at a time (waits for graceful termination)
//
// Bug: did not check for already-terminating condemned pods; could skip past them
//      and mark additional pods for deletion in the same reconcile
// Fix: when the highest-ordinal condemned pod is already terminating, block —
//      do NOT mark the next pod until the current one is fully gone
//
// Scenario: SS with 4 pods → desired 1. pod-3 is already terminating.
// After ONE reconcile: pod-2 must NOT gain a deletion_timestamp (we are
// waiting for pod-3 to finish).  Revert removes the blocking guard, so
// pod-2 would get marked in the same reconcile.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_6573091_scale_down_waits_for_terminating_pod_before_next() {
    let storage = setup().await;

    // SS spec: 4 pods scaled to 1
    let mut ss = make_statefulset("cache", "default", 4);
    let ss_uid = ss.metadata.uid.clone();
    let key = build_key("statefulsets", Some("default"), "cache");
    storage.create(&key, &ss).await.unwrap();

    // Seed pods 0-3. pod-3 is already terminating (deletion_timestamp set).
    for ordinal in 0..4i32 {
        let terminating = ordinal == 3;
        seed_pod(
            &storage,
            "cache",
            &ss_uid,
            "default",
            ordinal,
            Phase::Running,
            true,
            terminating,
        )
        .await;
    }

    // Scale down to 1
    ss.spec.replicas = Some(1);
    storage.update(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // pod-3 was already terminating — the controller should have blocked there.
    // pod-2 and pod-1 must NOT gain a deletion_timestamp in this reconcile.
    let pod2_key = build_key("pods", Some("default"), "cache-2");
    let pod2: Pod = storage
        .get(&pod2_key)
        .await
        .expect("cache-2 must still exist");

    let pod1_key = build_key("pods", Some("default"), "cache-1");
    let pod1: Pod = storage
        .get(&pod1_key)
        .await
        .expect("cache-1 must still exist");

    println!(
        "cache-2 deletion_timestamp = {:?}",
        pod2.metadata.deletion_timestamp
    );
    println!(
        "cache-1 deletion_timestamp = {:?}",
        pod1.metadata.deletion_timestamp
    );

    // The fix: neither pod-2 nor pod-1 should be marked while pod-3 is terminating.
    // Revert removes the terminating-pod guard, allowing pod-2 to be marked
    // in this same reconcile. The assertion below fails on the reverted code.
    assert!(
        pod2.metadata.deletion_timestamp.is_none(),
        "cache-2 must NOT be marked for deletion while cache-3 is still terminating"
    );
    assert!(
        pod1.metadata.deletion_timestamp.is_none(),
        "cache-1 must NOT be marked for deletion while cache-3 is still terminating"
    );
}
