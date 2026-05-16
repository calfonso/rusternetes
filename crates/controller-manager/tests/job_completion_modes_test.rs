// Job Completion Modes Integration Tests
// Reproduces the bugs exposed by upstream e2e sites
// apps/job.go:439 (Indexed), :556 (successPolicy), :630 (backoffLimitPerIndex).
// Captured from a live sonobuoy run on rusternetes:
//   :439 — "expected completed indexes [0,1,2,3], but got [0,2]"
//          (status.succeeded reached completions due to duplicate pod creation
//           but completedIndexes only listed the unique succeeded indexes)
//   :556 — Pod for a non-matching index was still Running after successPolicy fired
//          (active != 0 / extra index succeeded), so status.succeeded > matched.
//   :630 — status.failed counted duplicate per-index failures, leaving 5 instead of 4.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::*;
use rusternetes_common::types::{ObjectMeta, OwnerReference, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::job::JobController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn base_pod_spec() -> PodSpec {
    PodSpec {
        containers: vec![Container {
            name: "task".to_string(),
            image: "busybox:latest".to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo Hello".to_string(),
            ]),
            ports: None,
            env: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            resources: None,
            working_dir: None,
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
        restart_policy: Some("Never".to_string()),
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
    }
}

fn make_job(name: &str, namespace: &str, completions: i32, parallelism: i32) -> Job {
    let mut labels = HashMap::new();
    labels.insert("job-name".to_string(), name.to_string());

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
            parallelism: Some(parallelism),
            backoff_limit: Some(6),
            active_deadline_seconds: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: base_pod_spec(),
            },
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

fn succeeded_status() -> PodStatus {
    PodStatus {
        phase: Some(Phase::Succeeded),
        ..Default::default()
    }
}

fn failed_status() -> PodStatus {
    PodStatus {
        phase: Some(Phase::Failed),
        ..Default::default()
    }
}

async fn set_pod_status(
    storage: &Arc<MemoryStorage>,
    namespace: &str,
    pod: &Pod,
    new_status: PodStatus,
) {
    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
    let mut p = pod.clone();
    p.status = Some(new_status);
    storage.update(&pod_key, &p).await.unwrap();
}

fn pod_index(pod: &Pod) -> Option<i32> {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
        .and_then(|v| v.parse::<i32>().ok())
}

/// Create a manual indexed pod owned by the Job at a specific completion index.
/// Used to simulate the race where the controller creates a duplicate pod for
/// an index that already succeeded — what triggered the e2e failure at job.go:439.
async fn create_duplicate_indexed_pod(
    storage: &Arc<MemoryStorage>,
    job: &Job,
    namespace: &str,
    index: i32,
    phase: Phase,
) {
    let pod_name = format!("{}-dup-{}", job.metadata.name, uuid::Uuid::new_v4());
    let mut labels = HashMap::new();
    labels.insert("job-name".to_string(), job.metadata.name.clone());
    labels.insert("controller-uid".to_string(), job.metadata.uid.clone());
    labels.insert(
        "batch.kubernetes.io/job-completion-index".to_string(),
        index.to_string(),
    );
    let mut annotations = HashMap::new();
    annotations.insert(
        "batch.kubernetes.io/job-completion-index".to_string(),
        index.to_string(),
    );
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: pod_name.clone(),
            generate_name: None,
            generation: None,
            managed_fields: None,
            namespace: Some(namespace.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            uid: uuid::Uuid::new_v4().to_string(),
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: Some(vec![OwnerReference {
                api_version: "batch/v1".to_string(),
                kind: "Job".to_string(),
                name: job.metadata.name.clone(),
                uid: job.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
        },
        spec: Some(base_pod_spec()),
        status: Some(PodStatus {
            phase: Some(phase),
            ..Default::default()
        }),
    };
    let key = build_key("pods", Some(namespace), &pod_name);
    storage.create(&key, &pod).await.unwrap();
}

/// Mirrors upstream apps/job.go:439 — "expected completed indexes [0,1,2,3],
/// but got [0,2]". The bug: status.succeeded was incremented per *pod* even
/// when the same index already had a Succeeded pod, so the job appeared
/// complete (status.succeeded == completions) while completedIndexes only
/// listed the unique indexes that actually finished.
#[tokio::test]
async fn test_indexed_job_status_succeeded_counts_unique_indexes() {
    let storage = setup_test().await;

    let mut job = make_job("idx-unique", "default", 4, 2);
    job.spec.completion_mode = Some("Indexed".to_string());
    let key = build_key("jobs", Some("default"), "idx-unique");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());

    // First reconcile creates pods for indexes 0 and 1 (parallelism=2).
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "expect parallelism pods");

    // Find pod for index 0 and mark it Succeeded.
    let pod0 = pods
        .iter()
        .find(|p| pod_index(p) == Some(0))
        .cloned()
        .expect("pod for index 0");
    set_pod_status(&storage, "default", &pod0, succeeded_status()).await;

    // Simulate the bug condition observed in e2e: a duplicate pod for index 0
    // (same index, separate pod) also reaches Succeeded. This can happen if the
    // controller created a second pod for an index after a transient view in
    // which the original was missing — exactly the e2e log race.
    let fresh_job: Job = storage.get(&key).await.unwrap();
    create_duplicate_indexed_pod(&storage, &fresh_job, "default", 0, Phase::Succeeded).await;

    // Reconcile — should NOT count the duplicate succeed as a separate completion.
    controller.reconcile_all().await.unwrap();

    let updated: Job = storage.get(&key).await.unwrap();
    let status = updated.status.expect("status set");
    let completed = status
        .completed_indexes
        .as_deref()
        .unwrap_or("")
        .to_string();
    assert_eq!(
        completed, "0",
        "completedIndexes must list only the unique succeeded index"
    );
    assert_eq!(
        status.succeeded,
        Some(1),
        "status.succeeded for an Indexed Job is the count of UNIQUE \
         succeeded indexes, not the raw succeeded pod count \
         (e2e job.go:439 regression)"
    );
    // Sanity: the job is not yet Complete (only 1/4 indexes done).
    let has_complete = status
        .conditions
        .as_ref()
        .map(|cs| {
            cs.iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True")
        })
        .unwrap_or(false);
    assert!(
        !has_complete,
        "job must not be Complete when only 1 of 4 indexes has succeeded"
    );
}

/// Mirrors upstream apps/job.go:556 — when successPolicy fires, the pods at
/// non-matching indexes that were still Running must not be allowed to bump
/// status.succeeded once the policy has been satisfied.
#[tokio::test]
async fn test_indexed_job_success_policy_caps_status_succeeded_at_policy_match() {
    let storage = setup_test().await;

    let mut job = make_job("idx-sp-cap", "default", 2, 2);
    job.spec.completion_mode = Some("Indexed".to_string());
    // Policy fires the moment index 0 succeeds, regardless of index 1.
    job.spec.success_policy = Some(serde_json::json!({
        "rules": [
            { "succeededIndexes": "0", "succeededCount": 1 }
        ]
    }));
    let key = build_key("jobs", Some("default"), "idx-sp-cap");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());

    // Reconcile creates pods for both indexes.
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);

    let pod0 = pods
        .iter()
        .find(|p| pod_index(p) == Some(0))
        .cloned()
        .expect("pod for index 0");
    set_pod_status(&storage, "default", &pod0, succeeded_status()).await;

    // First reconcile: policy fires on index 0. Other pod gets terminated.
    controller.reconcile_all().await.unwrap();

    let after_policy: Job = storage.get(&key).await.unwrap();
    let st1 = after_policy.status.clone().expect("status after policy");
    assert_eq!(
        st1.succeeded,
        Some(1),
        "right after successPolicy match, succeeded must be 1 (only index 0)"
    );
    assert_eq!(
        st1.active,
        Some(0),
        "active must be 0 — other pod is being terminated"
    );

    // Simulate what the live e2e observed: the other pod, which had not yet
    // been terminated by kubelet, still racing to Succeeded. The Job is already
    // Complete via the policy; succeeded MUST NOT increase past 1.
    let pod1 = pods
        .iter()
        .find(|p| pod_index(p) == Some(1))
        .cloned()
        .expect("pod for index 1");
    set_pod_status(&storage, "default", &pod1, succeeded_status()).await;

    controller.reconcile_all().await.unwrap();
    let final_job: Job = storage.get(&key).await.unwrap();
    let st2 = final_job.status.expect("final status");
    assert_eq!(
        st2.succeeded,
        Some(1),
        "after a late-arriving succeed for index 1, succeeded must stay at 1 \
         — the Job already completed via successPolicy and the index is not \
         in succeededIndexes (e2e job.go:556 regression)"
    );
    let completed = st2.completed_indexes.as_deref().unwrap_or("");
    assert_eq!(
        completed, "0",
        "completedIndexes must only list the policy-matching index"
    );
}

/// Mirrors upstream apps/job.go:630 — with backoffLimitPerIndex, status.failed
/// should not double-count duplicate Failed pods for the same already-succeeded
/// index. The bug: a stray Failed pod for an index that has already Succeeded
/// inflated status.failed by one.
#[tokio::test]
async fn test_indexed_job_backoff_limit_per_index_failed_count_excludes_resolved_indexes() {
    let storage = setup_test().await;

    let mut job = make_job("idx-blpi-count", "default", 2, 2);
    job.spec.completion_mode = Some("Indexed".to_string());
    job.spec.backoff_limit_per_index = Some(1);
    job.spec.backoff_limit = Some(100);
    let key = build_key("jobs", Some("default"), "idx-blpi-count");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);

    // Index 0: succeed cleanly on first try.
    let pod0 = pods
        .iter()
        .find(|p| pod_index(p) == Some(0))
        .cloned()
        .expect("pod for index 0");
    set_pod_status(&storage, "default", &pod0, succeeded_status()).await;

    // Index 1: fail once.
    let pod1 = pods
        .iter()
        .find(|p| pod_index(p) == Some(1))
        .cloned()
        .expect("pod for index 1");
    set_pod_status(&storage, "default", &pod1, failed_status()).await;

    controller.reconcile_all().await.unwrap();

    // Simulate the e2e race: a stale Failed pod for index 0 (which already
    // succeeded) appears in storage. K8s upstream excludes already-resolved
    // indexes from the failed count.
    let fresh_job: Job = storage.get(&key).await.unwrap();
    create_duplicate_indexed_pod(&storage, &fresh_job, "default", 0, Phase::Failed).await;

    controller.reconcile_all().await.unwrap();

    let updated: Job = storage.get(&key).await.unwrap();
    let status = updated.status.expect("status set");
    assert_eq!(
        status.failed,
        Some(1),
        "status.failed must only count failed pods whose index is not yet \
         resolved as succeeded (e2e job.go:630 regression); got {:?}",
        status.failed
    );
}
