// Integration tests for TTL Controller
// Tests automatic cleanup of finished Jobs based on TTL

use chrono::{Duration, Utc};
use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::{
    Job, JobCondition, JobSpec, JobStatus, PodTemplateSpec,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::ttl_controller::TTLController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration as TokioDuration};

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn create_test_job(name: &str, namespace: &str, ttl_seconds: i32, finished: bool) -> Job {
    let mut annotations = HashMap::new();
    annotations.insert(
        "ttlSecondsAfterFinished".to_string(),
        ttl_seconds.to_string(),
    );

    let finish_time = if finished {
        Some(Utc::now() - Duration::seconds(120)) // Finished 2 minutes ago
    } else {
        None
    };

    Job {
        type_meta: TypeMeta {
            kind: "Job".to_string(),
            api_version: "batch/v1".to_string(),
        },
        metadata: ObjectMeta::new(name)
            .with_namespace(namespace)
            .with_annotations(annotations),
        spec: JobSpec {
            template: PodTemplateSpec {
                metadata: None,
                spec: PodSpec {
                    containers: vec![Container {
                        name: "test".to_string(),
                        image: "busybox".to_string(),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        command: Some(vec!["echo".to_string(), "hello".to_string()]),
                        args: None,
                        ports: None,
                        env: None,
                        volume_mounts: None,
                        liveness_probe: None,
                        readiness_probe: None,
                        startup_probe: None,
                        resources: None,
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
                },
            },
            completions: Some(1),
            parallelism: Some(1),
            backoff_limit: Some(3),
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
        status: if finished {
            Some(JobStatus {
                active: Some(0),
                succeeded: Some(1),
                failed: Some(0),
                conditions: Some(vec![JobCondition {
                    condition_type: "Complete".to_string(),
                    status: "True".to_string(),
                    last_probe_time: finish_time,
                    last_transition_time: finish_time,
                    reason: Some("JobComplete".to_string()),
                    message: Some("Job completed successfully".to_string()),
                }]),
                start_time: None,
                completion_time: None,
                ready: None,
                terminating: None,
                completed_indexes: None,
                failed_indexes: None,
                uncounted_terminated_pods: None,
                observed_generation: None,
            })
        } else {
            Some(JobStatus {
                active: Some(1),
                succeeded: Some(0),
                failed: Some(0),
                conditions: None,
                start_time: None,
                completion_time: None,
                ready: None,
                terminating: None,
                completed_indexes: None,
                failed_indexes: None,
                uncounted_terminated_pods: None,
                observed_generation: None,
            })
        },
    }
}

#[tokio::test]
async fn test_ttl_controller_cleans_expired_job() {
    let storage = setup_test().await;

    // Create a finished job with TTL of 60 seconds (finished 120 seconds ago)
    let job = create_test_job("expired-job", "default", 60, true);
    let job_key = build_key("jobs", Some("default"), "expired-job");
    storage.create(&job_key, &job).await.unwrap();

    // Verify job exists
    let stored_job: Job = storage.get(&job_key).await.unwrap();
    assert_eq!(stored_job.metadata.name, "expired-job");

    // Run TTL controller
    let controller = TTLController::new(storage.clone());
    controller.check_and_cleanup().await.unwrap();

    // Wait for cleanup
    sleep(TokioDuration::from_millis(500)).await;

    // Verify job is deleted
    let result = storage.get::<Job>(&job_key).await;
    assert!(result.is_err(), "Expired job should be deleted");
}

#[tokio::test]
async fn test_ttl_controller_keeps_recent_job() {
    let storage = setup_test().await;

    // Create a finished job with TTL of 3600 seconds (1 hour)
    let job = create_test_job("recent-job", "default", 3600, true);
    let job_key = build_key("jobs", Some("default"), "recent-job");
    storage.create(&job_key, &job).await.unwrap();

    // Run TTL controller
    let controller = TTLController::new(storage.clone());
    controller.check_and_cleanup().await.unwrap();

    // Wait for potential cleanup
    sleep(TokioDuration::from_millis(500)).await;

    // Verify job still exists
    let stored_job: Job = storage.get(&job_key).await.unwrap();
    assert_eq!(stored_job.metadata.name, "recent-job");
}

#[tokio::test]
async fn test_ttl_controller_ignores_running_jobs() {
    let storage = setup_test().await;

    // Create a running job (not finished)
    let job = create_test_job("running-job", "default", 60, false);
    let job_key = build_key("jobs", Some("default"), "running-job");
    storage.create(&job_key, &job).await.unwrap();

    // Run TTL controller
    let controller = TTLController::new(storage.clone());
    controller.check_and_cleanup().await.unwrap();

    // Wait
    sleep(TokioDuration::from_millis(500)).await;

    // Verify job still exists (not deleted because it's not finished)
    let stored_job: Job = storage.get(&job_key).await.unwrap();
    assert_eq!(stored_job.metadata.name, "running-job");
}

#[tokio::test]
async fn test_ttl_controller_deletes_job_pods() {
    let storage = setup_test().await;

    // Create a finished job
    let job = create_test_job("job-with-pods", "default", 60, true);
    let job_uid = job.metadata.uid.clone();
    let job_key = build_key("jobs", Some("default"), "job-with-pods");
    storage.create(&job_key, &job).await.unwrap();

    // Create pods owned by the job
    for i in 0..3 {
        let mut pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(format!("job-pod-{}", i)).with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test".to_string(),
                    image: "busybox".to_string(),
                    image_pull_policy: Some("IfNotPresent".to_string()),
                    command: None,
                    args: None,
                    ports: None,
                    env: None,
                    volume_mounts: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    resources: None,
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
            }),
            status: Some(PodStatus {
                phase: Some(rusternetes_common::types::Phase::Succeeded),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
            }),
        };

        // Add owner reference to the job
        pod.metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference::new(
            "batch/v1",
            "Job",
            "job-with-pods",
            &job_uid,
        )
        .with_controller(true)]);

        let pod_key = build_key("pods", Some("default"), &format!("job-pod-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Verify pods exist
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should have 3 pods initially");

    // Run TTL controller
    let controller = TTLController::new(storage.clone());
    controller.check_and_cleanup().await.unwrap();

    // Wait for cleanup
    sleep(TokioDuration::from_millis(500)).await;

    // Verify job is deleted
    let result = storage.get::<Job>(&job_key).await;
    assert!(result.is_err(), "Job should be deleted");

    // Verify pods are also deleted
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 0, "Job pods should be deleted");
}

#[tokio::test]
async fn test_ttl_controller_handles_failed_jobs() {
    let storage = setup_test().await;

    // Create a failed job
    let mut job = create_test_job("failed-job", "default", 60, false);

    let finish_time = Some(Utc::now() - Duration::seconds(120));
    job.status = Some(JobStatus {
        active: Some(0),
        succeeded: Some(0),
        failed: Some(1),
        conditions: Some(vec![JobCondition {
            condition_type: "Failed".to_string(),
            status: "True".to_string(),
            last_probe_time: finish_time,
            last_transition_time: finish_time,
            reason: Some("BackoffLimitExceeded".to_string()),
            message: Some("Job has reached the specified backoff limit".to_string()),
        }]),
        start_time: None,
        completion_time: None,
        ready: None,
        terminating: None,
        completed_indexes: None,
        failed_indexes: None,
        uncounted_terminated_pods: None,
        observed_generation: None,
    });

    let job_key = build_key("jobs", Some("default"), "failed-job");
    storage.create(&job_key, &job).await.unwrap();

    // Run TTL controller
    let controller = TTLController::new(storage.clone());
    controller.check_and_cleanup().await.unwrap();

    // Wait for cleanup
    sleep(TokioDuration::from_millis(500)).await;

    // Verify failed job is also deleted (TTL applies to both Complete and Failed)
    let result = storage.get::<Job>(&job_key).await;
    assert!(result.is_err(), "Failed job should be deleted after TTL");
}

#[tokio::test]
async fn test_ttl_controller_handles_multiple_jobs() {
    let storage = setup_test().await;

    // Create multiple jobs with different TTLs
    let jobs = vec![
        ("expired-1", 30, true), // Expired (finished 120s ago, TTL 30s)
        ("expired-2", 60, true), // Expired (finished 120s ago, TTL 60s)
        ("recent", 3600, true),  // Not expired (TTL 1 hour)
        ("running", 60, false),  // Running (not finished)
    ];

    for (name, ttl, finished) in &jobs {
        let job = create_test_job(name, "default", *ttl, *finished);
        let job_key = build_key("jobs", Some("default"), name);
        storage.create(&job_key, &job).await.unwrap();
    }

    // Verify all jobs exist
    let all_jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(all_jobs.len(), 4);

    // Run TTL controller
    let controller = TTLController::new(storage.clone());
    controller.check_and_cleanup().await.unwrap();

    // Wait for cleanup
    sleep(TokioDuration::from_millis(500)).await;

    // Verify expired jobs are deleted
    assert!(storage
        .get::<Job>(&build_key("jobs", Some("default"), "expired-1"))
        .await
        .is_err());
    assert!(storage
        .get::<Job>(&build_key("jobs", Some("default"), "expired-2"))
        .await
        .is_err());

    // Verify non-expired jobs still exist
    assert!(storage
        .get::<Job>(&build_key("jobs", Some("default"), "recent"))
        .await
        .is_ok());
    assert!(storage
        .get::<Job>(&build_key("jobs", Some("default"), "running"))
        .await
        .is_ok());
}

#[tokio::test]
async fn test_ttl_zero_immediate_cleanup() {
    let storage = setup_test().await;

    // Create a finished job with TTL of 0 (should delete immediately)
    let job = create_test_job("immediate-cleanup", "default", 0, true);
    let job_key = build_key("jobs", Some("default"), "immediate-cleanup");
    storage.create(&job_key, &job).await.unwrap();

    // Run TTL controller
    let controller = TTLController::new(storage.clone());
    controller.check_and_cleanup().await.unwrap();

    // Wait briefly
    sleep(TokioDuration::from_millis(500)).await;

    // Verify job is deleted immediately
    let result = storage.get::<Job>(&job_key).await;
    assert!(
        result.is_err(),
        "Job with TTL=0 should be deleted immediately"
    );
}

#[tokio::test]
async fn test_ttl_controller_get_ttl_from_annotations() {
    let storage = setup_test().await;

    let controller = TTLController::new(storage.clone());

    // Create job with TTL annotation
    let job = create_test_job("test-job", "default", 100, true);

    // Get TTL from job
    let ttl = controller.get_ttl_seconds_after_finished(&job);
    assert_eq!(ttl, Some(100));
}

#[tokio::test]
async fn test_ttl_controller_job_without_ttl() {
    let storage = setup_test().await;

    // Create a finished job without TTL annotation
    let mut job = create_test_job("no-ttl-job", "default", 60, true);
    job.metadata.annotations = None; // Remove annotations

    let job_key = build_key("jobs", Some("default"), "no-ttl-job");
    storage.create(&job_key, &job).await.unwrap();

    // Run TTL controller
    let controller = TTLController::new(storage.clone());
    controller.check_and_cleanup().await.unwrap();

    // Wait
    sleep(TokioDuration::from_millis(500)).await;

    // Verify job still exists (no TTL means it won't be cleaned up)
    let stored_job: Job = storage.get(&job_key).await.unwrap();
    assert_eq!(stored_job.metadata.name, "no-ttl-job");
}
