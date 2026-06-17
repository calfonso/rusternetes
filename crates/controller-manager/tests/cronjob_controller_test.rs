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

// ---------------------------------------------------------------------------
// Helpers shared by the extended Phase 5.1 coverage below.
// ---------------------------------------------------------------------------

/// Rewind a CronJob's `last_schedule_time` two minutes into the past so that a
/// wildcard `* * * * *` schedule is eligible to fire on the next reconcile,
/// then sleep just over one second so the controller's timestamp-based Job
/// name resolves to a different value than any previously created Job.
/// Mirrors the helper in `conformance_apps_job_cronjob.rs`.
async fn rewind_and_step(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) {
    let key = build_key("cronjobs", Some(namespace), name);
    let mut cj: CronJob = storage.get(&key).await.unwrap();
    if let Some(ref mut st) = cj.status {
        st.last_schedule_time = Some(chrono::Utc::now() - chrono::Duration::minutes(2));
    }
    storage.update(&key, &cj).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
}

/// Stamp a Job with `Complete=True` so the CronJob controller no longer counts
/// it among the active set. Required to step Forbid/history-limit tests
/// without driving a full pod lifecycle.
async fn mark_job_complete(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) {
    let key = build_key("jobs", Some(namespace), name);
    let mut job: Job = storage.get(&key).await.unwrap();
    let now = chrono::Utc::now();
    job.status = Some(JobStatus {
        active: Some(0),
        succeeded: Some(1),
        failed: Some(0),
        conditions: Some(vec![JobCondition {
            condition_type: "Complete".to_string(),
            status: "True".to_string(),
            last_probe_time: Some(now),
            last_transition_time: Some(now),
            reason: Some("CompletionsReached".to_string()),
            message: Some("Job completed".to_string()),
        }]),
        start_time: Some(now - chrono::Duration::minutes(1)),
        completion_time: Some(now),
        ready: Some(0),
        terminating: None,
        completed_indexes: None,
        failed_indexes: None,
        uncounted_terminated_pods: None,
        observed_generation: job.metadata.generation,
    });
    storage.update(&key, &job).await.unwrap();
}

/// Stamp a Job with `Failed=True` so it counts against `failedJobsHistoryLimit`
/// during cleanup.
async fn mark_job_failed(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) {
    let key = build_key("jobs", Some(namespace), name);
    let mut job: Job = storage.get(&key).await.unwrap();
    let now = chrono::Utc::now();
    job.status = Some(JobStatus {
        active: Some(0),
        succeeded: Some(0),
        failed: Some(1),
        conditions: Some(vec![JobCondition {
            condition_type: "Failed".to_string(),
            status: "True".to_string(),
            last_probe_time: Some(now),
            last_transition_time: Some(now),
            reason: Some("BackoffLimitExceeded".to_string()),
            message: Some("Job failed".to_string()),
        }]),
        start_time: Some(now - chrono::Duration::minutes(1)),
        completion_time: Some(now),
        ready: Some(0),
        terminating: None,
        completed_indexes: None,
        failed_indexes: None,
        uncounted_terminated_pods: None,
        observed_generation: job.metadata.generation,
    });
    storage.update(&key, &job).await.unwrap();
}

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
                                ..Default::default()
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
                            ..Default::default()
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

// ===========================================================================
// Phase 5.1 — extended CronJob controller coverage
//
// Mirrors `kubernetes/test/e2e/apps/cronjob.go` for behaviour that the
// existing unit tests assert only at the field level (concurrency policy
// values, history limits) and adds coverage for spec.timeZone + DST handling
// that the upstream `gocron` controller supports.
// ===========================================================================

/// `spec.timeZone` (IANA tz) must round-trip through storage unchanged.
///
/// Upstream: k8s.io/api/batch/v1/types.go (CronJobSpec.TimeZone, batch/v1 GA in 1.25)
///
/// The controller must not strip or mutate the timezone hint when reconciling
/// a CronJob whose schedule otherwise fires immediately. This test is the
/// minimum baseline: the stored CronJob keeps its `time_zone` field across a
/// full reconcile pass.
#[tokio::test]
async fn test_cronjob_timezone_field_persisted_across_reconcile() {
    let storage = setup_test().await;
    let mut cj = create_test_cronjob("tz-persist", "default", "* * * * *");
    cj.spec.time_zone = Some("America/New_York".to_string());
    let key = build_key("cronjobs", Some("default"), "tz-persist");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let round_trip: CronJob = storage.get(&key).await.unwrap();
    assert_eq!(
        round_trip.spec.time_zone.as_deref(),
        Some("America/New_York"),
        "controller must not drop or rewrite spec.timeZone during reconcile"
    );
}

/// `spec.timeZone` must be honoured when evaluating the schedule.
///
/// Upstream: k8s.io/kubernetes/pkg/controller/cronjob/utils.go:nextScheduleTimeDuration
/// Sonobuoy: currently NOT covered — our `should_run_now` ignores `time_zone`
/// and always interprets the schedule in UTC.
///
/// A tz-tagged CronJob fires its catch-up Job through the controller. The
/// controller now parses `spec.timeZone` (see `should_run_now`); the
/// deterministic UTC-vs-zone discrimination is unit-tested in
/// `cronjob::tests::should_run_now_honours_time_zone` (this integration test
/// can't pin wall-clock `now`, so it asserts the end-to-end firing path).
#[tokio::test]
async fn test_cronjob_timezone_aware_schedule_fires() {
    let storage = setup_test().await;
    // 23:30 UTC + America/New_York (UTC-5/-4) shifts the next "0 0 * * *"
    // boundary differently than plain UTC. With a tz-aware controller the
    // schedule should still be evaluable; without it, the test simply asserts
    // we do not panic on a tz-tagged spec.
    let mut cj = create_test_cronjob("tz-fire", "default", "0 0 * * *");
    cj.spec.time_zone = Some("America/New_York".to_string());
    if let Some(ref mut st) = cj.status {
        st.last_schedule_time = Some(chrono::Utc::now() - chrono::Duration::hours(25));
    }
    let key = build_key("cronjobs", Some("default"), "tz-fire");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "tz-aware CronJob must fire its catch-up Job exactly once per missed window"
    );
}

/// `successfulJobsHistoryLimit` bounds Complete Jobs retained per CronJob.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:276
///
/// Drives the controller through three successful ticks with a limit of 1
/// and asserts that history cleanup prunes the surplus Complete Jobs. The
/// active (not-yet-completed) Job from the final tick is excluded from the
/// successful count.
#[tokio::test]
async fn test_cronjob_successful_jobs_history_limit_prunes() {
    let storage = setup_test().await;
    let mut cj = create_test_cronjob("succ-history", "default", "* * * * *");
    cj.spec.successful_jobs_history_limit = Some(1);
    let key = build_key("cronjobs", Some("default"), "succ-history");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());

    // Three ticks → complete every Job after each tick so the next tick is
    // not blocked by concurrency considerations.
    for tick in 0..3 {
        rewind_and_step(&storage, "default", "succ-history").await;
        controller.reconcile_all().await.unwrap();

        let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
        assert!(
            !jobs.is_empty(),
            "tick {} must have produced at least one Job",
            tick
        );
        for j in &jobs {
            let already_complete = j
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|cs| {
                    cs.iter()
                        .any(|c| c.condition_type == "Complete" && c.status == "True")
                })
                .unwrap_or(false);
            if !already_complete {
                mark_job_complete(&storage, "default", &j.metadata.name).await;
            }
        }
    }

    // Final tick triggers cleanup with the freshly-completed history.
    rewind_and_step(&storage, "default", "succ-history").await;
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    let successful = jobs
        .iter()
        .filter(|j| {
            j.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|cs| {
                    cs.iter()
                        .any(|c| c.condition_type == "Complete" && c.status == "True")
                })
                .unwrap_or(false)
        })
        .count();
    assert!(
        successful <= 1,
        "successfulJobsHistoryLimit=1 must keep at most one Complete Job; got {}",
        successful
    );
}

/// `failedJobsHistoryLimit` bounds Failed Jobs retained per CronJob.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:287
///
/// Mirrors the successful-history test but stamps each Job with `Failed=True`
/// instead of `Complete=True`.
#[tokio::test]
async fn test_cronjob_failed_jobs_history_limit_prunes() {
    let storage = setup_test().await;
    let mut cj = create_test_cronjob("fail-history", "default", "* * * * *");
    cj.spec.failed_jobs_history_limit = Some(1);
    let key = build_key("cronjobs", Some("default"), "fail-history");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());

    for tick in 0..3 {
        rewind_and_step(&storage, "default", "fail-history").await;
        controller.reconcile_all().await.unwrap();
        let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
        assert!(
            !jobs.is_empty(),
            "tick {} must have produced at least one Job",
            tick
        );
        for j in &jobs {
            let already_terminal = j
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|cs| {
                    cs.iter().any(|c| {
                        (c.condition_type == "Complete" || c.condition_type == "Failed")
                            && c.status == "True"
                    })
                })
                .unwrap_or(false);
            if !already_terminal {
                mark_job_failed(&storage, "default", &j.metadata.name).await;
            }
        }
    }

    rewind_and_step(&storage, "default", "fail-history").await;
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    let failed_count = jobs
        .iter()
        .filter(|j| {
            j.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|cs| {
                    cs.iter()
                        .any(|c| c.condition_type == "Failed" && c.status == "True")
                })
                .unwrap_or(false)
        })
        .count();
    assert!(
        failed_count <= 1,
        "failedJobsHistoryLimit=1 must keep at most one Failed Job; got {}",
        failed_count
    );
}

/// `concurrencyPolicy: Forbid` blocks new Jobs while one is still active.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:102
///
/// This is enforcement (not just field-value) coverage: drive a Forbid
/// CronJob through two ticks while the first Job is still active, and assert
/// the controller refuses to spawn a second Job.
#[tokio::test]
async fn test_cronjob_concurrency_policy_forbid_enforcement() {
    let storage = setup_test().await;
    let mut cj = create_test_cronjob("forbid-enforce", "default", "* * * * *");
    cj.spec.concurrency_policy = Some("Forbid".to_string());
    let key = build_key("cronjobs", Some("default"), "forbid-enforce");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let first: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(first.len(), 1, "first tick must spawn exactly one Job");
    let first_uid = first[0].metadata.uid.clone();

    // Second tick — Job from the first tick is still active (no status set).
    rewind_and_step(&storage, "default", "forbid-enforce").await;
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "Forbid must not spawn a second Job while one is active; got {}",
        jobs.len()
    );
    assert_eq!(
        jobs[0].metadata.uid, first_uid,
        "Forbid must preserve the active Job (no replace)"
    );
}

/// `concurrencyPolicy: Replace` deletes the active Job and spawns a fresh one.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:134
#[tokio::test]
async fn test_cronjob_concurrency_policy_replace_enforcement() {
    let storage = setup_test().await;
    let mut cj = create_test_cronjob("replace-enforce", "default", "* * * * *");
    cj.spec.concurrency_policy = Some("Replace".to_string());
    let key = build_key("cronjobs", Some("default"), "replace-enforce");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    let first: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(first.len(), 1, "first tick must spawn exactly one Job");
    let first_name = first[0].metadata.name.clone();

    rewind_and_step(&storage, "default", "replace-enforce").await;
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "Replace must leave exactly one Job (the new one); got {}",
        jobs.len()
    );
    assert_ne!(
        jobs[0].metadata.name, first_name,
        "Replace must delete the previous Job"
    );
}

/// `concurrencyPolicy: Allow` lets a tick fire even when prior Jobs are active.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:57
///
/// With Allow (the default) the controller must keep the previously-active
/// Job and additionally spawn a new one on the second tick. Existing
/// `test_cronjob_concurrency_policy_allow` only asserts the field default;
/// this test asserts the controller's *behaviour* under that policy.
#[tokio::test]
async fn test_cronjob_concurrency_policy_allow_enforcement() {
    let storage = setup_test().await;
    let cj = create_test_cronjob("allow-enforce", "default", "* * * * *");
    let key = build_key("cronjobs", Some("default"), "allow-enforce");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let first: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(first.len(), 1, "first tick must spawn exactly one Job");
    let first_name = first[0].metadata.name.clone();

    rewind_and_step(&storage, "default", "allow-enforce").await;
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert!(
        jobs.len() >= 2,
        "Allow must permit a second Job alongside the active one; got {}",
        jobs.len()
    );
    assert!(
        jobs.iter().any(|j| j.metadata.name == first_name),
        "Allow must preserve the originally-active Job"
    );
}

/// DST transition: the controller must keep firing through a daylight-saving
/// boundary in the configured tz.
///
/// Upstream: k8s.io/kubernetes/pkg/controller/cronjob/utils.go honours DST via
/// the Go time package when `spec.timeZone` is set.
///
/// `spec.timeZone` is now honoured (`should_run_now` evaluates the schedule in
/// the named zone via chrono-tz, which carries DST). This asserts the catch-up
/// path produces exactly one Job for a DST-crossing daily schedule.
#[tokio::test]
async fn test_cronjob_dst_transition_handling() {
    let storage = setup_test().await;
    // Daily-at-02:30 schedule in a tz that crosses DST. On the "spring
    // forward" day there is no 02:30 local; on "fall back" 02:30 occurs
    // twice. A DST-aware controller must still produce exactly one Job per
    // calendar day. We assert only the catch-up behaviour: with a stale
    // `last_schedule_time`, the controller spawns exactly one Job, never
    // duplicates from the DST repeat.
    let mut cj = create_test_cronjob("dst-job", "default", "30 2 * * *");
    cj.spec.time_zone = Some("America/New_York".to_string());
    if let Some(ref mut st) = cj.status {
        // Last fired well before the next DST boundary so the controller has
        // multiple "missed" candidate windows to choose from.
        st.last_schedule_time = Some(chrono::Utc::now() - chrono::Duration::days(2));
    }
    let key = build_key("cronjobs", Some("default"), "dst-job");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "DST transition must not cause duplicate Job spawns for a single day; got {}",
        jobs.len()
    );
}
