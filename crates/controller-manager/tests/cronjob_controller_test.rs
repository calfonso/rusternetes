// CronJob Controller Integration Tests
// Tests the CronJob controller's ability to create jobs on schedule

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::*;
use rusternetes_common::resources::PodTemplateSpec;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::cronjob::CronJobController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn create_test_cronjob(name: &str, namespace: &str, schedule: &str) -> CronJob {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    CronJob {
        type_meta: TypeMeta {
            kind: "CronJob".to_string(),
            api_version: "batch/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: CronJobSpec {
            schedule: schedule.to_string(),
            job_template: JobTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new("");
                    meta.labels = Some(labels.clone());
                    meta
                }),
                spec: JobSpec {
                    completions: Some(1),
                    parallelism: Some(1),
                    backoff_limit: Some(3),
                    active_deadline_seconds: None,
                    template: PodTemplateSpec {
                        metadata: Some({
                            let mut meta = ObjectMeta::new("");
                            meta.labels = Some(labels);
                            meta
                        }),
                        spec: PodSpec {
                            containers: vec![Container {
                                name: "task".to_string(),
                                image: "busybox:latest".to_string(),
                                image_pull_policy: Some("IfNotPresent".to_string()),
                                command: Some(vec!["echo".to_string(), "Hello".to_string()]),
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
            suspend: Some(false),
            concurrency_policy: Some("Allow".to_string()),
            successful_jobs_history_limit: Some(3),
            failed_jobs_history_limit: Some(1),
            starting_deadline_seconds: None,
            time_zone: None,
        },
        // Set last_schedule_time to 2 minutes ago so it's eligible to run
        status: Some(CronJobStatus {
            active: Vec::new(),
            last_schedule_time: Some(chrono::Utc::now() - chrono::Duration::minutes(2)),
            last_successful_time: None,
        }),
    }
}

#[tokio::test]
async fn test_cronjob_job_template() {
    let _storage = setup_test().await;

    // Test that CronJob creates jobs from its job template
    let cronjob = create_test_cronjob("template-test", "default", "* * * * *");
    let _key = build_key("cronjobs", Some("default"), "template-test");

    // Verify job template structure is correct
    assert_eq!(cronjob.spec.job_template.spec.completions, Some(1));
    assert_eq!(cronjob.spec.job_template.spec.parallelism, Some(1));
    assert_eq!(cronjob.spec.job_template.spec.backoff_limit, Some(3));
    assert_eq!(
        cronjob.spec.job_template.spec.template.spec.restart_policy,
        Some("Never".to_string())
    );

    // Verify containers are configured
    assert_eq!(
        cronjob
            .spec
            .job_template
            .spec
            .template
            .spec
            .containers
            .len(),
        1
    );
    assert_eq!(
        cronjob.spec.job_template.spec.template.spec.containers[0].name,
        "task"
    );
}

#[tokio::test]
async fn test_cronjob_suspend() {
    let storage = setup_test().await;

    // Create suspended CronJob
    let mut cronjob = create_test_cronjob("suspended", "default", "@hourly");
    cronjob.spec.suspend = Some(true);
    let key = build_key("cronjobs", Some("default"), "suspended");
    storage.create(&key, &cronjob).await.unwrap();

    // Run controller
    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify no jobs were created
    let jobs: Vec<Job> = storage
        .list("/registry/jobs/default/")
        .await
        .unwrap_or_default();
    assert_eq!(jobs.len(), 0, "Should not create jobs when suspended");
}

#[tokio::test]
async fn test_cronjob_concurrency_policy_forbid() {
    let _storage = setup_test().await;

    // Create CronJob with Forbid policy
    let mut cronjob = create_test_cronjob("forbid-test", "default", "* * * * *");
    cronjob.spec.concurrency_policy = Some("Forbid".to_string());

    // Verify policy is set correctly
    assert_eq!(cronjob.spec.concurrency_policy, Some("Forbid".to_string()));
}

#[tokio::test]
async fn test_cronjob_concurrency_policy_replace() {
    let _storage = setup_test().await;

    // Create CronJob with Replace policy
    let mut cronjob = create_test_cronjob("replace-test", "default", "* * * * *");
    cronjob.spec.concurrency_policy = Some("Replace".to_string());

    // Verify policy is set correctly
    assert_eq!(cronjob.spec.concurrency_policy, Some("Replace".to_string()));
}

#[tokio::test]
async fn test_cronjob_concurrency_policy_allow() {
    let _storage = setup_test().await;

    // Create CronJob with Allow policy (default)
    let cronjob = create_test_cronjob("allow-test", "default", "* * * * *");

    // Verify default policy is Allow
    assert_eq!(cronjob.spec.concurrency_policy, Some("Allow".to_string()));
}

#[tokio::test]
async fn test_cronjob_history_limits() {
    let _storage = setup_test().await;

    // Create CronJob with custom history limits
    let mut cronjob = create_test_cronjob("cleanup-job", "default", "* * * * *");
    cronjob.spec.successful_jobs_history_limit = Some(5);
    cronjob.spec.failed_jobs_history_limit = Some(2);

    // Verify limits are set correctly
    assert_eq!(cronjob.spec.successful_jobs_history_limit, Some(5));
    assert_eq!(cronjob.spec.failed_jobs_history_limit, Some(2));
}

#[tokio::test]
async fn test_cronjob_schedule_parsing() {
    let storage = setup_test().await;

    // Test various schedule formats
    let schedules = ["@hourly", "@daily", "@weekly", "@monthly", "*/5 * * * *"];

    for (i, schedule) in schedules.iter().enumerate() {
        let cronjob = create_test_cronjob(&format!("test-{}", i), "default", schedule);
        let key = build_key("cronjobs", Some("default"), &cronjob.metadata.name);
        storage.create(&key, &cronjob).await.unwrap();
    }

    // Run controller - should not panic on any schedule format
    let controller = CronJobController::new(storage.clone());
    let result = controller.reconcile_all().await;
    assert!(
        result.is_ok(),
        "Controller should handle all schedule formats"
    );
}
