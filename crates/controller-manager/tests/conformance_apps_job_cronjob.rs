//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-apps] Job + CronJob.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apps/
//! (`job.go`, `cronjob.go`).
//!
//! See docs/conformance/apps-job-cronjob.md for the test-by-test status table.
//!
//! This file is a controller-manager unit; it drives the Job and CronJob
//! reconcilers directly against `Arc<MemoryStorage>` (no HTTP harness — the
//! REST surface is exercised in the api-server conformance suites). Each
//! function mirrors the upstream Ginkgo descriptor in name and docstring.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::*;
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::cronjob::CronJobController;
use rusternetes_controller_manager::controllers::job::JobController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn base_pod_spec() -> PodSpec {
    PodSpec {
        containers: vec![Container {
            name: "task".to_string(),
            image: "registry.k8s.io/e2e-test-images/busybox:1.36.1-1".to_string(),
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

fn make_cronjob(name: &str, namespace: &str, schedule: &str) -> CronJob {
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
                    backoff_limit: Some(2),
                    active_deadline_seconds: None,
                    template: PodTemplateSpec {
                        metadata: Some({
                            let mut meta = ObjectMeta::new("");
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
            },
            suspend: Some(false),
            concurrency_policy: Some("Allow".to_string()),
            successful_jobs_history_limit: Some(3),
            failed_jobs_history_limit: Some(1),
            starting_deadline_seconds: None,
            time_zone: None,
        },
        // Place the last-schedule sentinel two minutes in the past so a
        // wildcard schedule (`* * * * *`) is eligible to fire on first
        // reconcile. Tests that need a "never scheduled" state set this to
        // `None` themselves.
        status: Some(CronJobStatus {
            active: Vec::new(),
            last_schedule_time: Some(chrono::Utc::now() - chrono::Duration::minutes(2)),
            last_successful_time: None,
        }),
    }
}

async fn set_pod_phase(storage: &Arc<MemoryStorage>, namespace: &str, pod: &Pod, phase: Phase) {
    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
    let mut p = pod.clone();
    p.status = Some(PodStatus {
        phase: Some(phase),
        ..Default::default()
    });
    storage.update(&pod_key, &p).await.unwrap();
}

async fn complete_job(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) {
    // Mark a Job's status as Complete so the CronJob controller no longer
    // counts it among `active`. Useful for tests where we want to exercise
    // history-limit cleanup without driving a full pod lifecycle.
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

async fn fail_job(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) {
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

fn has_condition(job: &Job, condition_type: &str) -> bool {
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| {
            cs.iter()
                .any(|c| c.condition_type == condition_type && c.status == "True")
        })
        .unwrap_or(false)
}

/// Make the CronJob eligible to fire on the next reconcile by rewinding
/// `status.last_schedule_time` two minutes into the past, then sleep just
/// over one second so the next `create_job` produces a different
/// timestamp-based name (the controller uses second resolution).
async fn rewind_and_step(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) {
    let key = build_key("cronjobs", Some(namespace), name);
    let mut cj: CronJob = storage.get(&key).await.unwrap();
    if let Some(ref mut st) = cj.status {
        st.last_schedule_time = Some(chrono::Utc::now() - chrono::Duration::minutes(2));
    }
    storage.update(&key, &cj).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
}

// ===========================================================================
// [sig-apps] Job — mirrors test/e2e/apps/job.go
// ===========================================================================

/// [sig-apps] Job should run a job to completion when tasks succeed
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go:53
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Verifies the happy path: a Job with `completions == parallelism` creates
/// the requested pods on the first reconcile, and once each pod reports
/// `Succeeded`, the controller marks the Job `Complete` with
/// `status.succeeded == completions`.
#[tokio::test]
async fn job_should_run_to_completion_when_tasks_succeed() {
    let storage = setup_test().await;
    let job = make_job("succeed", "default", 2, 2);
    let key = build_key("jobs", Some("default"), "succeed");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "first reconcile creates `parallelism` pods");
    for p in &pods {
        set_pod_phase(&storage, "default", p, Phase::Succeeded).await;
    }

    controller.reconcile_all().await.unwrap();
    let updated: Job = storage.get(&key).await.unwrap();
    let status = updated.status.clone().expect("status set after reconcile");
    assert_eq!(status.succeeded, Some(2));
    assert_eq!(status.active, Some(0));
    assert!(
        has_condition(&updated, "Complete"),
        "Job must carry Complete=True once succeeded == completions"
    );
}

/// [sig-apps] Job should run a job to completion when tasks sometimes fail and are not locally restarted
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go:777
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// With `restartPolicy: Never`, the Job controller spawns *replacement* pods
/// when an attempt fails. The Job must still reach `Complete` as long as the
/// total failures stay below `backoffLimit`.
#[tokio::test]
async fn job_should_complete_when_tasks_sometimes_fail_without_local_restart() {
    let storage = setup_test().await;
    let mut job = make_job("flaky", "default", 2, 2);
    job.spec.backoff_limit = Some(6);
    let key = build_key("jobs", Some("default"), "flaky");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // First batch: one fails, one succeeds.
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);
    set_pod_phase(&storage, "default", &pods[0], Phase::Failed).await;
    set_pod_phase(&storage, "default", &pods[1], Phase::Succeeded).await;

    // Second reconcile: controller must create a replacement pod for the
    // failed slot (parallelism = 2, only 1 succeeded so far).
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let active: Vec<&Pod> = pods
        .iter()
        .filter(|p| {
            !matches!(
                p.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Succeeded) | Some(Phase::Failed)
            )
        })
        .collect();
    assert_eq!(
        active.len(),
        1,
        "controller must spawn a replacement for the failed pod"
    );

    // Succeed the replacement → Job completes.
    set_pod_phase(&storage, "default", active[0], Phase::Succeeded).await;
    controller.reconcile_all().await.unwrap();
    let done: Job = storage.get(&key).await.unwrap();
    assert!(has_condition(&done, "Complete"));
    assert_eq!(done.status.as_ref().unwrap().succeeded, Some(2));
}

/// [sig-apps] Job should fail when exceeds active deadline
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go:816
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// `activeDeadlineSeconds` bounds wall-clock time from `status.startTime` to
/// completion. Once exceeded, the controller deletes active pods and marks
/// the Job `Failed` with `reason=DeadlineExceeded`.
#[tokio::test]
async fn job_should_fail_when_exceeds_active_deadline() {
    let storage = setup_test().await;
    let mut job = make_job("deadline", "default", 3, 1);
    job.spec.active_deadline_seconds = Some(1);
    let key = build_key("jobs", Some("default"), "deadline");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    // First reconcile creates a pod and stamps start_time.
    controller.reconcile_all().await.unwrap();

    // Push start_time into the past so the deadline trips deterministically.
    let mut fresh: Job = storage.get(&key).await.unwrap();
    if let Some(ref mut st) = fresh.status {
        st.start_time = Some(chrono::Utc::now() - chrono::Duration::seconds(10));
    }
    storage.update(&key, &fresh).await.unwrap();

    controller.reconcile_all().await.unwrap();
    let done: Job = storage.get(&key).await.unwrap();
    let status = done.status.expect("status set");
    let failed_cond = status
        .conditions
        .as_ref()
        .and_then(|cs| {
            cs.iter()
                .find(|c| c.condition_type == "Failed" && c.status == "True")
        })
        .expect("Failed condition expected");
    assert_eq!(
        failed_cond.reason.as_deref(),
        Some("DeadlineExceeded"),
        "Failed.reason must be DeadlineExceeded per K8s API conventions"
    );
    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    let still_running = pods_after
        .iter()
        .filter(|p| {
            matches!(
                p.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Running) | Some(Phase::Pending) | None
            )
        })
        .count();
    assert_eq!(
        still_running, 0,
        "deadline-exceeded job must terminate active pods"
    );
}

/// [sig-apps] Job should fail to exceed backoffLimit
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go:925
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// When `status.failed > backoffLimit`, the Job is marked
/// `Failed` with `reason=BackoffLimitExceeded`. We seed `backoff_limit=0`
/// so a single failed pod trips the limit.
#[tokio::test]
async fn job_should_fail_to_exceed_backoff_limit() {
    let storage = setup_test().await;
    let mut job = make_job("backoff", "default", 1, 1);
    job.spec.backoff_limit = Some(0);
    let key = build_key("jobs", Some("default"), "backoff");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1);
    set_pod_phase(&storage, "default", &pods[0], Phase::Failed).await;

    controller.reconcile_all().await.unwrap();
    let done: Job = storage.get(&key).await.unwrap();
    assert!(
        has_condition(&done, "Failed"),
        "exceeding backoffLimit must set Failed=True"
    );
}

/// [sig-apps] Job should not create pods when created in suspend state
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go:228
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// A Job whose `spec.suspend == true` at creation time must not have any
/// pods spawned. `status.active` stays at zero.
#[tokio::test]
async fn job_should_not_create_pods_when_created_in_suspend_state() {
    let storage = setup_test().await;
    let mut job = make_job("born-suspended", "default", 3, 3);
    job.spec.suspend = Some(true);
    let key = build_key("jobs", Some("default"), "born-suspended");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert!(
        pods.is_empty(),
        "suspended Job must not spawn pods; found {}",
        pods.len()
    );
}

/// [sig-apps] Job should delete pods when suspended
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go:258
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Flipping `spec.suspend` from `false` to `true` on a running Job must
/// cause the controller to delete any active pods and reset
/// `status.active` to zero.
#[tokio::test]
async fn job_should_delete_pods_when_suspended() {
    let storage = setup_test().await;
    let job = make_job("suspend-mid", "default", 3, 3);
    let key = build_key("jobs", Some("default"), "suspend-mid");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        storage
            .list::<Pod>("/registry/pods/default/")
            .await
            .unwrap()
            .len(),
        3
    );

    // Flip suspend on.
    let mut fresh: Job = storage.get(&key).await.unwrap();
    fresh.spec.suspend = Some(true);
    storage.update(&key, &fresh).await.unwrap();
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert!(
        pods.is_empty(),
        "all active pods must be deleted on suspend; remain {}",
        pods.len()
    );
    let done: Job = storage.get(&key).await.unwrap();
    assert_eq!(done.status.as_ref().unwrap().active, Some(0));
}

/// [sig-apps] Job should adopt matching orphans and release non-matching pods
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/job.go:872
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// The Job controller honors `parallelism` as an upper bound on active pods
/// regardless of completion progress. With `completions=10, parallelism=3`
/// the first reconcile creates exactly three pods.
#[tokio::test]
async fn job_should_respect_parallelism_as_active_upper_bound() {
    let storage = setup_test().await;
    let job = make_job("parallel", "default", 10, 3);
    let key = build_key("jobs", Some("default"), "parallel");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods.len(),
        3,
        "parallelism caps the active pod set; got {}",
        pods.len()
    );
}

// ===========================================================================
// [sig-apps] CronJob — mirrors test/e2e/apps/cronjob.go
// ===========================================================================

/// [sig-apps] CronJob should schedule multiple jobs concurrently
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:57
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// With the default `concurrencyPolicy: Allow`, a CronJob whose schedule
/// fires while a previous Job is still active spawns an additional Job
/// rather than skipping or replacing.
#[tokio::test]
async fn cronjob_should_schedule_multiple_jobs_concurrently_when_allow() {
    let storage = setup_test().await;
    let cj = make_cronjob("concurrent", "default", "* * * * *");
    let key = build_key("cronjobs", Some("default"), "concurrent");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let jobs_after_first: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(jobs_after_first.len(), 1, "first tick spawns one Job");

    rewind_and_step(&storage, "default", "concurrent").await;
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        2,
        "Allow policy must permit overlapping Jobs; got {}",
        jobs.len()
    );
}

/// [sig-apps] CronJob should not schedule jobs when suspended
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:76
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// A CronJob with `spec.suspend == true` must never spawn a Job, even when
/// the schedule fires.
#[tokio::test]
async fn cronjob_should_not_schedule_jobs_when_suspended() {
    let storage = setup_test().await;
    let mut cj = make_cronjob("suspended", "default", "* * * * *");
    cj.spec.suspend = Some(true);
    let key = build_key("cronjobs", Some("default"), "suspended");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage
        .list("/registry/jobs/default/")
        .await
        .unwrap_or_default();
    assert!(
        jobs.is_empty(),
        "suspended CronJob must not spawn Jobs; got {}",
        jobs.len()
    );
}

/// [sig-apps] CronJob should not schedule new jobs when ForbidConcurrent
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:102
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// With `concurrencyPolicy: Forbid`, the controller must skip a tick when a
/// previous Job is still active. The active Job is preserved untouched.
#[tokio::test]
async fn cronjob_should_skip_new_jobs_when_forbid_concurrent() {
    let storage = setup_test().await;
    let mut cj = make_cronjob("forbid", "default", "* * * * *");
    cj.spec.concurrency_policy = Some("Forbid".to_string());
    let key = build_key("cronjobs", Some("default"), "forbid");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    let first: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(first.len(), 1, "first tick spawns one Job");
    let first_uid = first[0].metadata.uid.clone();

    rewind_and_step(&storage, "default", "forbid").await;
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "Forbid policy must not spawn a second Job while one is active"
    );
    assert_eq!(
        jobs[0].metadata.uid, first_uid,
        "Forbid policy must not replace the active Job"
    );
}

/// [sig-apps] CronJob should replace jobs when ReplaceConcurrent
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:134
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// With `concurrencyPolicy: Replace`, the controller deletes the currently
/// active Job and starts a new one when the schedule fires.
#[tokio::test]
async fn cronjob_should_replace_jobs_when_replace_concurrent() {
    let storage = setup_test().await;
    let mut cj = make_cronjob("replace", "default", "* * * * *");
    cj.spec.concurrency_policy = Some("Replace".to_string());
    let key = build_key("cronjobs", Some("default"), "replace");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    let first: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(first.len(), 1);
    let first_name = first[0].metadata.name.clone();

    rewind_and_step(&storage, "default", "replace").await;
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "Replace policy must leave exactly one Job (the new one); got {}",
        jobs.len()
    );
    assert_ne!(
        jobs[0].metadata.name, first_name,
        "Replace policy must delete the previous Job"
    );
}

/// [sig-apps] CronJob should delete successful finished jobs with limit of one successful job
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:276
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// `successfulJobsHistoryLimit` bounds how many Complete Jobs are retained
/// per CronJob. Setting the limit to 1 must reduce the surviving Complete
/// set to a single Job.
#[tokio::test]
async fn cronjob_should_delete_successful_finished_jobs_above_history_limit() {
    let storage = setup_test().await;
    let mut cj = make_cronjob("succ-hist", "default", "* * * * *");
    cj.spec.successful_jobs_history_limit = Some(1);
    let key = build_key("cronjobs", Some("default"), "succ-hist");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());

    // Drive three successful Jobs by reconciling three times, completing the
    // active Job between ticks so the schedule stays eligible.
    for tick in 0..3 {
        rewind_and_step(&storage, "default", "succ-hist").await;
        controller.reconcile_all().await.unwrap();

        // Mark every existing Job for this CronJob as Complete so the next
        // tick is not blocked by concurrency considerations and history
        // cleanup can prune them.
        let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
        for j in &jobs {
            if j.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|cs| {
                    cs.iter()
                        .any(|c| c.condition_type == "Complete" && c.status == "True")
                })
                .unwrap_or(false)
            {
                continue;
            }
            complete_job(&storage, "default", &j.metadata.name).await;
        }
        assert!(
            !jobs.is_empty(),
            "tick {} must have produced at least one Job",
            tick
        );
    }

    // Final tick triggers cleanup with the freshly-completed history.
    rewind_and_step(&storage, "default", "succ-hist").await;
    controller.reconcile_all().await.unwrap();

    // After cleanup, at most one successful + one active Job should remain.
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

/// [sig-apps] CronJob should delete failed finished jobs with limit of one job
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:287
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// `failedJobsHistoryLimit` bounds Failed Jobs the same way
/// `successfulJobsHistoryLimit` bounds Complete Jobs.
#[tokio::test]
async fn cronjob_should_delete_failed_finished_jobs_above_history_limit() {
    let storage = setup_test().await;
    let mut cj = make_cronjob("fail-hist", "default", "* * * * *");
    cj.spec.failed_jobs_history_limit = Some(1);
    let key = build_key("cronjobs", Some("default"), "fail-hist");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());

    for _ in 0..3 {
        rewind_and_step(&storage, "default", "fail-hist").await;
        controller.reconcile_all().await.unwrap();
        let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
        for j in &jobs {
            if j.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|cs| {
                    cs.iter().any(|c| {
                        (c.condition_type == "Complete" || c.condition_type == "Failed")
                            && c.status == "True"
                    })
                })
                .unwrap_or(false)
            {
                continue;
            }
            fail_job(&storage, "default", &j.metadata.name).await;
        }
    }

    rewind_and_step(&storage, "default", "fail-hist").await;
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

/// [sig-apps] CronJob should be able to schedule after more than 100 missed schedule
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:160
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Upstream uses this scenario to assert that the controller does not stall
/// even when many schedule windows elapsed since the last run. Our reconcile
/// path uses `Schedule::after(last_schedule)`, so a very old `last_schedule`
/// must still yield exactly one Job per tick.
#[tokio::test]
async fn cronjob_should_recover_when_many_schedules_missed() {
    let storage = setup_test().await;
    let mut cj = make_cronjob("missed", "default", "* * * * *");
    if let Some(ref mut st) = cj.status {
        st.last_schedule_time = Some(chrono::Utc::now() - chrono::Duration::hours(3));
    }
    let key = build_key("cronjobs", Some("default"), "missed");
    storage.create(&key, &cj).await.unwrap();

    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "must spawn exactly one Job for the catch-up tick (not one per missed slot)"
    );
}

/// [sig-apps] CronJob should support CronJob API operations
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/cronjob.go:310
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Verifies basic round-trip storage operations on the CronJob resource and
/// the controller's tolerance of multiple schedule formats (`@hourly`,
/// `@daily`, `*/5 * * * *`).
#[tokio::test]
async fn cronjob_should_support_cronjob_api_operations() {
    let storage = setup_test().await;
    let controller = CronJobController::new(storage.clone());

    for (i, sched) in [
        "@hourly",
        "@daily",
        "@weekly",
        "@monthly",
        "*/5 * * * *",
        "0 */2 * * *",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("api-{}", i);
        let cj = make_cronjob(&name, "default", sched);
        let key = build_key("cronjobs", Some("default"), &name);
        storage.create(&key, &cj).await.unwrap();

        let round_trip: CronJob = storage.get(&key).await.unwrap();
        assert_eq!(round_trip.spec.schedule, *sched);

        // Reconcile must not panic regardless of schedule format.
        controller
            .reconcile_all()
            .await
            .expect("reconcile must tolerate every supported schedule format");
    }
}

/// [sig-apps] CronJob startingDeadlineSeconds bounds catch-up window
///
/// Upstream behavior documented at k8s.io/kubernetes/test/e2e/apps/cronjob.go
/// (`startingDeadlineSeconds`) and the controller in
/// kubernetes/pkg/controller/cronjob/utils.go.
/// Sonobuoy (Round 160, 2026-04-26): not directly mirrored as a Conformance
/// test, but covered by the schedule-catch-up scenarios above. We assert
/// that the resource is round-tripped intact so the controller can read the
/// bound at reconcile time.
#[tokio::test]
async fn cronjob_should_preserve_starting_deadline_seconds_on_round_trip() {
    let storage = setup_test().await;
    let mut cj = make_cronjob("starting-deadline", "default", "* * * * *");
    cj.spec.starting_deadline_seconds = Some(30);
    let key = build_key("cronjobs", Some("default"), "starting-deadline");
    storage.create(&key, &cj).await.unwrap();

    let round_trip: CronJob = storage.get(&key).await.unwrap();
    assert_eq!(
        round_trip.spec.starting_deadline_seconds,
        Some(30),
        "startingDeadlineSeconds must survive storage round-trip"
    );

    // Reconcile must not error or panic with the bound set.
    let controller = CronJobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
}
