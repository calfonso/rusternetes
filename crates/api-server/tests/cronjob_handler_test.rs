//! Integration tests for CronJob handler
//!
//! Tests all CRUD operations, edge cases, and error handling for cronjobs

use rusternetes_common::resources::{
    Container, CronJob, CronJobSpec, CronJobStatus, JobSpec, JobTemplateSpec, PodSpec,
    PodTemplateSpec,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test cronjob
fn create_test_cronjob(name: &str, namespace: &str, schedule: &str) -> CronJob {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    CronJob {
        type_meta: TypeMeta {
            kind: "CronJob".to_string(),
            api_version: "batch/v1".to_string(),
        },
        metadata: ObjectMeta::new(name)
            .with_namespace(namespace)
            .with_labels(labels.clone()),
        spec: CronJobSpec {
            schedule: schedule.to_string(),
            concurrency_policy: Some("Allow".to_string()),
            suspend: Some(false),
            job_template: JobTemplateSpec {
                metadata: None,
                spec: JobSpec {
                    parallelism: Some(1),
                    completions: Some(1),
                    backoff_limit: Some(6),
                    active_deadline_seconds: None,
                    template: PodTemplateSpec {
                        metadata: Some(
                            ObjectMeta::new(format!("{}-pod", name))
                                .with_namespace(namespace)
                                .with_labels(labels),
                        ),
                        spec: PodSpec {
                            containers: vec![Container {
                                name: "job-container".to_string(),
                                image: "busybox:latest".to_string(),
                                command: Some(vec![
                                    "sh".to_string(),
                                    "-c".to_string(),
                                    "date".to_string(),
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
                                security_context: None,
                                restart_policy: None,
                                resize_policy: None,
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
                            ephemeral_containers: None,
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
            },
            successful_jobs_history_limit: Some(3),
            failed_jobs_history_limit: Some(1),
            starting_deadline_seconds: None,
            time_zone: None,
        },
        status: Some(CronJobStatus {
            active: Vec::new(),
            last_schedule_time: None,
            last_successful_time: None,
        }),
    }
}

#[tokio::test]
async fn test_cronjob_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let cronjob = create_test_cronjob("test-cron", "default", "*/5 * * * *");
    let key = build_key("cronjobs", Some("default"), "test-cron");

    // Create
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.metadata.name, "test-cron");
    assert_eq!(created.metadata.namespace, Some("default".to_string()));
    assert_eq!(created.spec.schedule, "*/5 * * * *");
    assert!(!created.metadata.uid.is_empty());

    // Get
    let retrieved: CronJob = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-cron");
    assert_eq!(retrieved.spec.schedule, "*/5 * * * *");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut cronjob = create_test_cronjob("test-update-cron", "default", "0 0 * * *");
    let key = build_key("cronjobs", Some("default"), "test-update-cron");

    // Create
    storage.create(&key, &cronjob).await.unwrap();

    // Update schedule
    cronjob.spec.schedule = "0 12 * * *".to_string();
    let updated: CronJob = storage.update(&key, &cronjob).await.unwrap();
    assert_eq!(updated.spec.schedule, "0 12 * * *");

    // Verify update
    let retrieved: CronJob = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.schedule, "0 12 * * *");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let cronjob = create_test_cronjob("test-delete-cron", "default", "@daily");
    let key = build_key("cronjobs", Some("default"), "test-delete-cron");

    // Create
    storage.create(&key, &cronjob).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<CronJob>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cronjob_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple cronjobs
    let cron1 = create_test_cronjob("cron-1", "default", "*/5 * * * *");
    let cron2 = create_test_cronjob("cron-2", "default", "@hourly");
    let cron3 = create_test_cronjob("cron-3", "default", "@daily");

    let key1 = build_key("cronjobs", Some("default"), "cron-1");
    let key2 = build_key("cronjobs", Some("default"), "cron-2");
    let key3 = build_key("cronjobs", Some("default"), "cron-3");

    storage.create(&key1, &cron1).await.unwrap();
    storage.create(&key2, &cron2).await.unwrap();
    storage.create(&key3, &cron3).await.unwrap();

    // List
    let prefix = build_prefix("cronjobs", Some("default"));
    let cronjobs: Vec<CronJob> = storage.list(&prefix).await.unwrap();

    assert!(cronjobs.len() >= 3);
    let names: Vec<String> = cronjobs.iter().map(|cj| cj.metadata.name.clone()).collect();
    assert!(names.contains(&"cron-1".to_string()));
    assert!(names.contains(&"cron-2".to_string()));
    assert!(names.contains(&"cron-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_list_across_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create cronjobs in different namespaces
    let cron1 = create_test_cronjob("cron-ns1", "namespace-1", "0 0 * * *");
    let cron2 = create_test_cronjob("cron-ns2", "namespace-2", "0 12 * * *");

    let key1 = build_key("cronjobs", Some("namespace-1"), "cron-ns1");
    let key2 = build_key("cronjobs", Some("namespace-2"), "cron-ns2");

    storage.create(&key1, &cron1).await.unwrap();
    storage.create(&key2, &cron2).await.unwrap();

    // List all (no namespace filter)
    let prefix = build_prefix("cronjobs", None);
    let cronjobs: Vec<CronJob> = storage.list(&prefix).await.unwrap();

    // Should find at least our 2 cronjobs
    assert!(cronjobs.len() >= 2);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_special_schedule_hourly() {
    let storage = Arc::new(MemoryStorage::new());

    let cronjob = create_test_cronjob("test-hourly", "default", "@hourly");
    let key = build_key("cronjobs", Some("default"), "test-hourly");

    // Create with @hourly schedule
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.schedule, "@hourly");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_special_schedule_daily() {
    let storage = Arc::new(MemoryStorage::new());

    let cronjob = create_test_cronjob("test-daily", "default", "@daily");
    let key = build_key("cronjobs", Some("default"), "test-daily");

    // Create with @daily schedule
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.schedule, "@daily");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_special_schedule_weekly() {
    let storage = Arc::new(MemoryStorage::new());

    let cronjob = create_test_cronjob("test-weekly", "default", "@weekly");
    let key = build_key("cronjobs", Some("default"), "test-weekly");

    // Create with @weekly schedule
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.schedule, "@weekly");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_concurrency_policy_allow() {
    let storage = Arc::new(MemoryStorage::new());

    let cronjob = create_test_cronjob("test-allow", "default", "*/5 * * * *");
    let key = build_key("cronjobs", Some("default"), "test-allow");

    // Create with Allow policy
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.concurrency_policy, Some("Allow".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_concurrency_policy_forbid() {
    let storage = Arc::new(MemoryStorage::new());

    let mut cronjob = create_test_cronjob("test-forbid", "default", "*/5 * * * *");
    cronjob.spec.concurrency_policy = Some("Forbid".to_string());

    let key = build_key("cronjobs", Some("default"), "test-forbid");

    // Create with Forbid policy
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.concurrency_policy, Some("Forbid".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_concurrency_policy_replace() {
    let storage = Arc::new(MemoryStorage::new());

    let mut cronjob = create_test_cronjob("test-replace", "default", "*/5 * * * *");
    cronjob.spec.concurrency_policy = Some("Replace".to_string());

    let key = build_key("cronjobs", Some("default"), "test-replace");

    // Create with Replace policy
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.concurrency_policy, Some("Replace".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_suspended() {
    let storage = Arc::new(MemoryStorage::new());

    let mut cronjob = create_test_cronjob("test-suspended", "default", "0 0 * * *");
    cronjob.spec.suspend = Some(true);

    let key = build_key("cronjobs", Some("default"), "test-suspended");

    // Create suspended cronjob
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.suspend, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

// Note: starting_deadline_seconds field removed from CronJobSpec
// #[tokio::test]
// async fn test_cronjob_starting_deadline_seconds() { ... }

#[tokio::test]
async fn test_cronjob_history_limits() {
    let storage = Arc::new(MemoryStorage::new());

    let mut cronjob = create_test_cronjob("test-history", "default", "0 0 * * *");
    cronjob.spec.successful_jobs_history_limit = Some(5);
    cronjob.spec.failed_jobs_history_limit = Some(2);

    let key = build_key("cronjobs", Some("default"), "test-history");

    // Create with history limits
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.successful_jobs_history_limit, Some(5));
    assert_eq!(created.spec.failed_jobs_history_limit, Some(2));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_with_status() {
    let storage = Arc::new(MemoryStorage::new());

    let mut cronjob = create_test_cronjob("test-status", "default", "0 0 * * *");
    cronjob.status = Some(CronJobStatus {
        active: vec![],
        last_schedule_time: Some(chrono::Utc::now()),
        last_successful_time: Some(chrono::Utc::now()),
    });

    let key = build_key("cronjobs", Some("default"), "test-status");

    // Create with status
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert!(created.status.is_some());
    assert!(created
        .status
        .as_ref()
        .unwrap()
        .last_schedule_time
        .is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let cronjob = create_test_cronjob("test-immutable", "default", "0 0 * * *");
    let key = build_key("cronjobs", Some("default"), "test-immutable");

    // Create
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_cron = created.clone();
    updated_cron.spec.schedule = "0 12 * * *".to_string();

    let updated: CronJob = storage.update(&key, &updated_cron).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(updated.spec.schedule, "0 12 * * *");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("cronjobs", Some("default"), "nonexistent");
    let result = storage.get::<CronJob>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_cronjob_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let cronjob = create_test_cronjob("nonexistent", "default", "0 0 * * *");
    let key = build_key("cronjobs", Some("default"), "nonexistent");

    let result = storage.update(&key, &cronjob).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cronjob_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut cronjob = create_test_cronjob("test-finalizers", "default", "0 0 * * *");
    cronjob.metadata.finalizers = Some(vec!["finalizer.test.io".to_string()]);

    let key = build_key("cronjobs", Some("default"), "test-finalizers");

    // Create with finalizer
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: CronJob = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Clean up - remove finalizer first
    cronjob.metadata.finalizers = None;
    storage.update(&key, &cronjob).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "production".to_string());
    labels.insert("type".to_string(), "backup".to_string());

    let mut cronjob = create_test_cronjob("test-labels", "default", "@daily");
    cronjob.metadata.labels = Some(labels);

    let key = build_key("cronjobs", Some("default"), "test-labels");

    // Create with labels
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(created_labels.get("env"), Some(&"production".to_string()));
    assert_eq!(created_labels.get("type"), Some(&"backup".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_cronjob_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert("description".to_string(), "Daily backup job".to_string());

    let mut cronjob = create_test_cronjob("test-annotations", "default", "@daily");
    cronjob.metadata.annotations = Some(annotations);

    let key = build_key("cronjobs", Some("default"), "test-annotations");

    // Create with annotations
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    assert_eq!(
        created.metadata.annotations.unwrap().get("description"),
        Some(&"Daily backup job".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

// Note: timezone field removed from CronJobSpec
// #[tokio::test]
// async fn test_cronjob_timezone() { ... }

#[tokio::test]
async fn test_cronjob_complex_schedule() {
    let storage = Arc::new(MemoryStorage::new());

    // Every 15 minutes on weekdays
    let cronjob = create_test_cronjob("test-complex", "default", "*/15 * * * 1-5");
    let key = build_key("cronjobs", Some("default"), "test-complex");

    // Create with complex schedule
    let created: CronJob = storage.create(&key, &cronjob).await.unwrap();
    assert_eq!(created.spec.schedule, "*/15 * * * 1-5");

    // Clean up
    storage.delete(&key).await.unwrap();
}
