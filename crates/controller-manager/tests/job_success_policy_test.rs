//! Regression tests for the successPolicy status race fix (7094958).
//!
//! The `already_complete` guard in the job reconcile loop decides whether to skip
//! the regular status-update block.  Before the fix, the guard only checked for
//! `Complete=True`.  A job that carried `SuccessCriteriaMet=True` (but not yet
//! `Complete=True`) would slip past the guard and have its status overwritten,
//! resetting `ready` back to a non-zero value.
//!
//! Test strategy: pre-seed the job with only `SuccessCriteriaMet=True` (simulating
//! the intermediate state).  Remove the succeeded pods so that the successPolicy
//! short-circuit branch does NOT fire (success_policy_met=false), forcing the code to
//! reach the `already_complete` guard.  Assert that the guard prevents the overwrite.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::*;
use rusternetes_common::types::{ObjectMeta, OwnerReference, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::job::JobController;
use rusternetes_storage::{memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_indexed_job(name: &str, namespace: &str, completions: i32, parallelism: i32) -> Job {
    Job {
        type_meta: TypeMeta {
            kind: "Job".to_string(),
            api_version: "batch/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: "job-uid-sp-test".to_string(),
            creation_timestamp: Some(chrono::Utc::now()),
            ..Default::default()
        },
        spec: JobSpec {
            template: PodTemplateSpec {
                metadata: None,
                spec: PodSpec {
                    containers: vec![Container {
                        name: "task".to_string(),
                        image: "busybox:latest".to_string(),
                        command: Some(vec![
                            "sh".to_string(),
                            "-c".to_string(),
                            "exit 0".to_string(),
                        ]),
                        args: None,
                        env: None,
                        ports: None,
                        volume_mounts: None,
                        resources: None,
                        liveness_probe: None,
                        readiness_probe: None,
                        startup_probe: None,
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        working_dir: None,
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
                    restart_policy: Some("Never".to_string()),
                    ..Default::default()
                },
            },
            completions: Some(completions),
            parallelism: Some(parallelism),
            backoff_limit: Some(6),
            active_deadline_seconds: None,
            selector: None,
            manual_selector: None,
            suspend: None,
            ttl_seconds_after_finished: None,
            completion_mode: Some("Indexed".to_string()),
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

fn make_indexed_pod(name: &str, namespace: &str, phase: Phase, job_name: &str, index: i32) -> Pod {
    let mut labels = HashMap::new();
    labels.insert("job-name".to_string(), job_name.to_string());
    labels.insert(
        "batch.kubernetes.io/job-completion-index".to_string(),
        index.to_string(),
    );

    let mut annotations = HashMap::new();
    annotations.insert(
        "batch.kubernetes.io/job-completion-index".to_string(),
        index.to_string(),
    );

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: format!("pod-uid-{}", name),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: Some(vec![OwnerReference {
                api_version: "batch/v1".to_string(),
                kind: "Job".to_string(),
                name: job_name.to_string(),
                uid: "job-uid-sp-test".to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            creation_timestamp: Some(chrono::Utc::now()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "task".to_string(),
                image: "busybox:latest".to_string(),
                command: Some(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "exit 0".to_string(),
                ]),
                args: None,
                env: None,
                ports: None,
                volume_mounts: None,
                resources: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                image_pull_policy: Some("IfNotPresent".to_string()),
                working_dir: None,
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
            restart_policy: Some("Never".to_string()),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(phase),
            ..Default::default()
        }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Core regression test for 7094958.
///
/// Scenario: a job's SuccessCriteriaMet condition is already stored, but Complete is not
/// (state that can persist after a partial write from a previous reconcile cycle).
/// The succeeded pods are then absent from storage so that the successPolicy
/// short-circuit does NOT fire.  The `already_complete` guard must recognise
/// SuccessCriteriaMet=True and prevent the status from being overwritten.
///
/// Without the fix the guard only checked `Complete`; `already_complete=false`
/// caused the status update block to run and reset `ready` to the pending pod count.
#[tokio::test]
async fn test_success_criteria_met_alone_prevents_status_overwrite() {
    let storage = Arc::new(MemoryStorage::new());

    // Job with successPolicy requiring index 0 to succeed
    let mut job = make_indexed_job("sp-guard-job", "default", 3, 3);
    job.spec.success_policy = Some(serde_json::json!({
        "rules": [{"succeededIndexes": "0"}]
    }));

    // Pre-seed status with ONLY SuccessCriteriaMet (no Complete).
    // This simulates the intermediate state that the race could produce.
    job.status = Some(JobStatus {
        active: Some(0),
        succeeded: Some(1),
        failed: Some(0),
        ready: Some(0),
        terminating: Some(0),
        conditions: Some(vec![JobCondition {
            condition_type: "SuccessCriteriaMet".to_string(),
            status: "True".to_string(),
            last_probe_time: Some(chrono::Utc::now()),
            last_transition_time: Some(chrono::Utc::now()),
            reason: Some("SuccessPolicy".to_string()),
            message: Some("Job met success policy criteria".to_string()),
        }]),
        start_time: Some(chrono::Utc::now()),
        completion_time: Some(chrono::Utc::now()),
        completed_indexes: Some("0".to_string()),
        failed_indexes: None,
        uncounted_terminated_pods: None,
        observed_generation: None,
    });

    let job_key = "/registry/jobs/default/sp-guard-job";
    storage.create(job_key, &job).await.unwrap();

    // Store Running pods (indexes 1 and 2) WITH Ready=True — the succeeded pod (index 0) is absent.
    // Ready=True means ready counter will be 2 if the status-update block runs.
    // This makes success_policy_met=false so the reconcile reaches the already_complete guard.
    for (name, idx) in [("sp-guard-pod-1", 1i32), ("sp-guard-pod-2", 2)] {
        let mut pod = make_indexed_pod(name, "default", Phase::Running, "sp-guard-job", idx);
        // Add Ready=True condition so ready counter > 0 if the bug fires
        if let Some(ref mut s) = pod.status {
            s.conditions = Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_transition_time: None,
                observed_generation: None,
                reason: None,
                message: None,
            }]);
        }
        storage
            .create(&format!("/registry/pods/default/{}", name), &pod)
            .await
            .unwrap();
    }

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let after: Job = storage.get(job_key).await.unwrap();
    let status = after.status.as_ref().expect("status must be present");

    // The SuccessCriteriaMet guard must have fired, preserving the completed status.
    // Without the fix, already_complete would be false and the block below would run:
    //   job.status = Some(JobStatus { active: Some(2), ready: Some(2), ... })
    // overwriting the completion.
    assert!(
        status
            .conditions
            .as_ref()
            .unwrap()
            .iter()
            .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True"),
        "SuccessCriteriaMet=True must be preserved by the already_complete guard"
    );
    assert_eq!(
        status.ready,
        Some(0),
        "ready must remain 0 — the already_complete guard must block the status overwrite"
    );
    assert!(
        status.completion_time.is_some(),
        "completion_time must be preserved"
    );
}

/// Verify that when BOTH Complete and SuccessCriteriaMet are present, the second
/// reconcile's early-exit (is_finished check) fires and preserves the status unchanged.
/// This is the normal happy-path after the successPolicy branch completes fully.
#[tokio::test]
async fn test_both_conditions_preserve_completed_status_across_reconciles() {
    let storage = Arc::new(MemoryStorage::new());

    // Indexed job with 5 completions; successPolicy requires indexes 0-1
    let mut job = make_indexed_job("sp-both-job", "default", 5, 5);
    job.spec.success_policy = Some(serde_json::json!({
        "rules": [{"succeededIndexes": "0-1"}]
    }));

    let job_key = "/registry/jobs/default/sp-both-job";
    storage.create(job_key, &job).await.unwrap();

    // Indexes 0 and 1 have succeeded; indexes 2-4 are still pending
    for (name, idx) in [("sp-both-pod-0", 0i32), ("sp-both-pod-1", 1)] {
        let pod = make_indexed_pod(name, "default", Phase::Succeeded, "sp-both-job", idx);
        storage
            .create(&format!("/registry/pods/default/{}", name), &pod)
            .await
            .unwrap();
    }
    for (name, idx) in [
        ("sp-both-pod-2", 2i32),
        ("sp-both-pod-3", 3),
        ("sp-both-pod-4", 4),
    ] {
        let pod = make_indexed_pod(name, "default", Phase::Pending, "sp-both-job", idx);
        storage
            .create(&format!("/registry/pods/default/{}", name), &pod)
            .await
            .unwrap();
    }

    let controller = JobController::new(storage.clone());

    // First reconcile — successPolicy is met, both conditions written
    controller.reconcile_all().await.unwrap();

    let after_first: Job = storage.get(job_key).await.unwrap();
    let status_first = after_first
        .status
        .as_ref()
        .expect("status must be set after first reconcile");
    let conds_first = status_first
        .conditions
        .as_ref()
        .expect("conditions must be set");

    assert!(
        conds_first
            .iter()
            .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True"),
        "SuccessCriteriaMet=True must be present after first reconcile"
    );
    assert!(
        conds_first
            .iter()
            .any(|c| c.condition_type == "Complete" && c.status == "True"),
        "Complete=True must be present after first reconcile"
    );
    assert_eq!(
        status_first.ready,
        Some(0),
        "ready must be 0 after successPolicy completion (first reconcile)"
    );

    // Second reconcile — the is_finished early-exit should fire (Complete=True)
    controller.reconcile_all().await.unwrap();

    let after_second: Job = storage.get(job_key).await.unwrap();
    let status_second = after_second
        .status
        .as_ref()
        .expect("status must still be set after second reconcile");
    let conds_second = status_second
        .conditions
        .as_ref()
        .expect("conditions must still be set");

    assert!(
        conds_second
            .iter()
            .any(|c| c.condition_type == "Complete" && c.status == "True"),
        "Complete=True must still be present after second reconcile"
    );
    assert!(
        conds_second
            .iter()
            .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True"),
        "SuccessCriteriaMet=True must still be present after second reconcile"
    );
    assert_eq!(
        status_second.ready,
        Some(0),
        "ready must remain 0 after second reconcile"
    );
}
