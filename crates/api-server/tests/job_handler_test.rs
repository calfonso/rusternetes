//! Integration tests for Job handler
//!
//! Tests all CRUD operations, edge cases, and error handling for jobs

use rusternetes_common::resources::pod::{Container, PodSpec};
use rusternetes_common::resources::{Job, JobSpec, JobStatus, PodTemplateSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test job
fn create_test_job(name: &str, namespace: &str) -> Job {
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
            meta.labels = Some(labels.clone());
            meta
        },
        spec: JobSpec {
            parallelism: Some(1),
            completions: Some(1),
            backoff_limit: Some(6),
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.namespace = Some(namespace.to_string());
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "job-container".to_string(),
                        image: "busybox:latest".to_string(),
                        command: Some(vec![
                            "sh".to_string(),
                            "-c".to_string(),
                            "echo hello".to_string(),
                        ]),
                        args: None,
                        env: None,
                        ports: None,
                        volume_mounts: None,
                        resources: None,
                        liveness_probe: None,
                        readiness_probe: None,
                        startup_probe: None,
                        image_pull_policy: None,
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
                    restart_policy: Some("OnFailure".to_string()),
                    service_account_name: None,
                    service_account: None,
                    automount_service_account_token: None,
                    node_selector: None,
                    node_name: None,
                    affinity: None,
                    tolerations: None,
                    host_network: None,
                    host_pid: None,
                    host_ipc: None,
                    hostname: None,
                    subdomain: None,
                    priority_class_name: None,
                    priority: None,
                    scheduler_name: None,
                    volumes: None,
                    overhead: None,
                    topology_spread_constraints: None,
                    resource_claims: None,
                    ephemeral_containers: None,
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
        status: Some(JobStatus {
            active: Some(0),
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
        }),
    }
}

#[tokio::test]
async fn test_job_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let job = create_test_job("test-job", "default");
    let key = build_key("jobs", Some("default"), "test-job");

    // Create
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(created.metadata.name, "test-job");
    assert_eq!(created.metadata.namespace, Some("default".to_string()));
    assert_eq!(created.spec.completions, Some(1));
    assert!(!created.metadata.uid.is_empty());

    // Get
    let retrieved: Job = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-job");
    assert_eq!(retrieved.spec.parallelism, Some(1));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut job = create_test_job("test-update-job", "default");
    let key = build_key("jobs", Some("default"), "test-update-job");

    // Create
    storage.create(&key, &job).await.unwrap();

    // Update parallelism
    job.spec.parallelism = Some(5);
    let updated: Job = storage.update(&key, &job).await.unwrap();
    assert_eq!(updated.spec.parallelism, Some(5));

    // Verify update
    let retrieved: Job = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.parallelism, Some(5));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let job = create_test_job("test-delete-job", "default");
    let key = build_key("jobs", Some("default"), "test-delete-job");

    // Create
    storage.create(&key, &job).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<Job>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_job_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple jobs
    let job1 = create_test_job("job-1", "default");
    let job2 = create_test_job("job-2", "default");
    let job3 = create_test_job("job-3", "default");

    let key1 = build_key("jobs", Some("default"), "job-1");
    let key2 = build_key("jobs", Some("default"), "job-2");
    let key3 = build_key("jobs", Some("default"), "job-3");

    storage.create(&key1, &job1).await.unwrap();
    storage.create(&key2, &job2).await.unwrap();
    storage.create(&key3, &job3).await.unwrap();

    // List
    let prefix = build_prefix("jobs", Some("default"));
    let jobs: Vec<Job> = storage.list(&prefix).await.unwrap();

    assert!(jobs.len() >= 3);
    let names: Vec<String> = jobs.iter().map(|j| j.metadata.name.clone()).collect();
    assert!(names.contains(&"job-1".to_string()));
    assert!(names.contains(&"job-2".to_string()));
    assert!(names.contains(&"job-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_job_list_across_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create jobs in different namespaces
    let job1 = create_test_job("job-ns1", "namespace-1");
    let job2 = create_test_job("job-ns2", "namespace-2");

    let key1 = build_key("jobs", Some("namespace-1"), "job-ns1");
    let key2 = build_key("jobs", Some("namespace-2"), "job-ns2");

    storage.create(&key1, &job1).await.unwrap();
    storage.create(&key2, &job2).await.unwrap();

    // List all (no namespace filter)
    let prefix = build_prefix("jobs", None);
    let jobs: Vec<Job> = storage.list(&prefix).await.unwrap();

    // Should find at least our 2 jobs
    assert!(jobs.len() >= 2);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_job_with_status() {
    let storage = Arc::new(MemoryStorage::new());

    let mut job = create_test_job("test-status", "default");
    job.status = Some(JobStatus {
        active: Some(2),
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
    });

    let key = build_key("jobs", Some("default"), "test-status");

    // Create with status
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(created.status.as_ref().unwrap().active, Some(2));
    assert_eq!(created.status.as_ref().unwrap().succeeded, Some(0));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_parallel_execution() {
    let storage = Arc::new(MemoryStorage::new());

    let mut job = create_test_job("test-parallel", "default");
    job.spec.parallelism = Some(10);
    job.spec.completions = Some(50);

    let key = build_key("jobs", Some("default"), "test-parallel");

    // Create parallel job
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(created.spec.parallelism, Some(10));
    assert_eq!(created.spec.completions, Some(50));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_with_backoff_limit() {
    let storage = Arc::new(MemoryStorage::new());

    let mut job = create_test_job("test-backoff", "default");
    job.spec.backoff_limit = Some(3);

    let key = build_key("jobs", Some("default"), "test-backoff");

    // Create with backoff limit
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(created.spec.backoff_limit, Some(3));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_with_active_deadline() {
    let storage = Arc::new(MemoryStorage::new());

    let mut job = create_test_job("test-deadline", "default");
    job.spec.active_deadline_seconds = Some(3600); // 1 hour

    let key = build_key("jobs", Some("default"), "test-deadline");

    // Create with active deadline
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(created.spec.active_deadline_seconds, Some(3600));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_basic_creation() {
    let storage = Arc::new(MemoryStorage::new());

    let job = create_test_job("test-basic", "default");
    let key = build_key("jobs", Some("default"), "test-basic");

    // Create job
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(created.spec.completions, Some(1));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let job = create_test_job("test-immutable", "default");
    let key = build_key("jobs", Some("default"), "test-immutable");

    // Create
    let created: Job = storage.create(&key, &job).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_job = created.clone();
    updated_job.spec.parallelism = Some(5);

    let updated: Job = storage.update(&key, &updated_job).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(updated.spec.parallelism, Some(5));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("batch".to_string(), "nightly".to_string());
    labels.insert("priority".to_string(), "high".to_string());

    let mut job = create_test_job("test-labels", "default");
    job.metadata.labels = Some(labels.clone());

    let key = build_key("jobs", Some("default"), "test-labels");

    // Create with labels
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(created_labels.get("batch"), Some(&"nightly".to_string()));
    assert_eq!(created_labels.get("priority"), Some(&"high".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("jobs", Some("default"), "nonexistent");
    let result = storage.get::<Job>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_job_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let job = create_test_job("nonexistent", "default");
    let key = build_key("jobs", Some("default"), "nonexistent");

    let result = storage.update(&key, &job).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_job_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut job = create_test_job("test-finalizers", "default");
    job.metadata.finalizers = Some(vec!["finalizer.test.io".to_string()]);

    let key = build_key("jobs", Some("default"), "test-finalizers");

    // Create with finalizer
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: Job = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Clean up - remove finalizer first
    job.metadata.finalizers = None;
    storage.update(&key, &job).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_restart_policy_on_failure() {
    let storage = Arc::new(MemoryStorage::new());

    let job = create_test_job("test-restart", "default");
    let key = build_key("jobs", Some("default"), "test-restart");

    // Create job with OnFailure restart policy
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(
        created.spec.template.spec.restart_policy,
        Some("OnFailure".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_single_completion() {
    let storage = Arc::new(MemoryStorage::new());

    let job = create_test_job("test-single", "default");
    let key = build_key("jobs", Some("default"), "test-single");

    // Create single completion job
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert_eq!(created.spec.completions, Some(1));
    assert_eq!(created.spec.parallelism, Some(1));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_job_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert("description".to_string(), "Nightly batch job".to_string());

    let mut job = create_test_job("test-annotations", "default");
    job.metadata.annotations = Some(annotations);

    let key = build_key("jobs", Some("default"), "test-annotations");

    // Create with annotations
    let created: Job = storage.create(&key, &job).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    assert_eq!(
        created.metadata.annotations.unwrap().get("description"),
        Some(&"Nightly batch job".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}
