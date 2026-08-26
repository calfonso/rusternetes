// Job Controller Integration Tests
// Tests the Job controller's ability to manage batch workloads

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::*;
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::job::JobController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn create_test_job(name: &str, namespace: &str, completions: i32, parallelism: i32) -> Job {
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
            backoff_limit: Some(3),
            active_deadline_seconds: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
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
        status: None,
    }
}

#[tokio::test]
async fn test_job_creates_pods() {
    let storage = setup_test().await;

    // Create job with 3 completions, parallelism 2
    let job = create_test_job("task", "default", 3, 2);
    let key = build_key("jobs", Some("default"), "task");
    storage.create(&key, &job).await.unwrap();

    // Run controller
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify 2 pods created (parallelism limit)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods.len(),
        2,
        "Should create 2 pods initially (parallelism limit)"
    );

    // Verify pods have correct labels
    for pod in &pods {
        let labels = pod
            .metadata
            .labels
            .as_ref()
            .expect("Pod should have labels");
        assert_eq!(labels.get("job-name"), Some(&"task".to_string()));
    }

    // Verify restart policy is Never
    for pod in &pods {
        assert_eq!(
            pod.spec.as_ref().unwrap().restart_policy,
            Some("Never".to_string())
        );
    }
}

#[tokio::test]
async fn test_job_respects_parallelism() {
    let storage = setup_test().await;

    // Create job with 10 completions, parallelism 3
    let job = create_test_job("parallel", "default", 10, 3);
    let key = build_key("jobs", Some("default"), "parallel");
    storage.create(&key, &job).await.unwrap();

    // Run controller
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify exactly 3 pods created (parallelism limit)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should respect parallelism limit of 3");
}

#[tokio::test]
async fn test_job_completion_detection() {
    let storage = setup_test().await;

    // Create job with 2 completions
    let job = create_test_job("complete", "default", 2, 2);
    let key = build_key("jobs", Some("default"), "complete");
    storage.create(&key, &job).await.unwrap();

    // Run controller to create pods
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Mark both pods as succeeded
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in &pods {
        let mut updated_pod = pod.clone();
        updated_pod.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            message: None,
            reason: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            host_ip: None,
            host_i_ps: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
        });
        let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
        storage.update(&pod_key, &updated_pod).await.unwrap();
    }

    // Run controller again
    controller.reconcile_all().await.unwrap();

    // Verify job is marked as complete
    let job: Job = storage.get(&key).await.unwrap();
    assert!(job.status.is_some());
    let status = job.status.unwrap();
    assert_eq!(status.succeeded, Some(2));

    if let Some(conditions) = status.conditions {
        assert!(conditions
            .iter()
            .any(|c| c.condition_type == "Complete" && c.status == "True"));
    }
}

#[tokio::test]
async fn test_job_creates_more_pods_as_they_complete() {
    let storage = setup_test().await;

    // Create job with 5 completions, parallelism 2
    let job = create_test_job("sequential", "default", 5, 2);
    let key = build_key("jobs", Some("default"), "sequential");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());

    // First reconcile - creates 2 pods
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);

    // Mark one pod as succeeded
    let pod_to_complete = &pods[0];
    let mut updated_pod = pod_to_complete.clone();
    updated_pod.status = Some(PodStatus {
        phase: Some(Phase::Succeeded),
        message: None,
        reason: None,
        pod_ip: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        host_ip: None,
        host_i_ps: None,
        conditions: None,
        container_statuses: None,
        init_container_statuses: None,
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
    });
    let pod_key = build_key("pods", Some("default"), &pod_to_complete.metadata.name);
    storage.update(&pod_key, &updated_pod).await.unwrap();

    // Second reconcile - should create one more pod (1 succeeded, 1 active, need 2 active)
    controller.reconcile_all().await.unwrap();
    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods_after.len(),
        3,
        "Should create another pod to maintain parallelism"
    );
}

#[tokio::test]
async fn test_job_backoff_limit() {
    let storage = setup_test().await;

    // Create job with backoff limit of 2
    let mut job = create_test_job("failing", "default", 5, 2);
    job.spec.backoff_limit = Some(2);
    let key = build_key("jobs", Some("default"), "failing");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Mark 3 pods as failed (exceeds backoff limit of 2)
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in pods.iter().take(2) {
        let mut failed_pod = pod.clone();
        failed_pod.status = Some(PodStatus {
            phase: Some(Phase::Failed),
            message: None,
            reason: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            host_ip: None,
            host_i_ps: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
        });
        let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
        storage.update(&pod_key, &failed_pod).await.unwrap();
    }

    // Create one more failed pod manually to exceed limit
    let extra_pod_name = "failing-extra".to_string();
    let mut extra_pod = pods[0].clone();
    extra_pod.metadata.name = extra_pod_name.clone();
    extra_pod.metadata.uid = uuid::Uuid::new_v4().to_string();
    extra_pod.status = Some(PodStatus {
        phase: Some(Phase::Failed),
        message: None,
        reason: None,
        pod_ip: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        host_ip: None,
        host_i_ps: None,
        conditions: None,
        container_statuses: None,
        init_container_statuses: None,
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
    });
    let extra_key = build_key("pods", Some("default"), &extra_pod_name);
    storage.create(&extra_key, &extra_pod).await.unwrap();

    // Reconcile - should mark job as failed
    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&key).await.unwrap();
    let status = updated_job.status.unwrap();

    if let Some(conditions) = status.conditions {
        assert!(conditions
            .iter()
            .any(|c| c.condition_type == "Failed" && c.status == "True"));
    }
}

#[tokio::test]
async fn test_job_single_completion() {
    let storage = setup_test().await;

    // Create job with 1 completion (default behavior)
    let job = create_test_job("single", "default", 1, 1);
    let key = build_key("jobs", Some("default"), "single");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Should create exactly 1 pod
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1);

    // Mark pod as succeeded
    let mut pod = pods[0].clone();
    pod.status = Some(PodStatus {
        phase: Some(Phase::Succeeded),
        message: None,
        reason: None,
        pod_ip: None,
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        host_ip: None,
        host_i_ps: None,
        conditions: None,
        container_statuses: None,
        init_container_statuses: None,
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
    });
    let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
    storage.update(&pod_key, &pod).await.unwrap();

    // Reconcile - job should be complete
    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&key).await.unwrap();
    let status = updated_job.status.unwrap();
    assert_eq!(status.succeeded, Some(1));
    assert_eq!(status.active, Some(0));
}

#[tokio::test]
async fn test_job_updates_status() {
    let storage = setup_test().await;

    let job = create_test_job("status-test", "default", 3, 2);
    let key = build_key("jobs", Some("default"), "status-test");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Check status after first reconcile
    let updated_job: Job = storage.get(&key).await.unwrap();
    let status = updated_job.status.unwrap();
    assert_eq!(status.active, Some(2));
    assert_eq!(status.succeeded, Some(0));
    assert_eq!(status.failed, Some(0));
}

#[tokio::test]
async fn test_job_suspend_deletes_active_pods() {
    let storage = setup_test().await;

    // Create a job with 3 completions, parallelism 3
    let job = create_test_job("suspend-test", "default", 3, 3);
    let key = build_key("jobs", Some("default"), "suspend-test");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());

    // First reconcile — creates 3 pods
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should create 3 pods initially");

    // Suspend the job
    let mut fresh_job: Job = storage.get(&key).await.unwrap();
    fresh_job.spec.suspend = Some(true);
    storage.update(&key, &fresh_job).await.unwrap();

    // Reconcile — should delete all active pods
    controller.reconcile_all().await.unwrap();

    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods_after.len(), 0, "Suspended job should have no pods");

    let updated_job: Job = storage.get(&key).await.unwrap();
    let status = updated_job.status.unwrap();
    assert_eq!(
        status.active,
        Some(0),
        "Suspended job should have 0 active pods"
    );
}

#[tokio::test]
async fn test_job_active_deadline_seconds() {
    let storage = setup_test().await;

    // Create a job with activeDeadlineSeconds of 1
    let mut job = create_test_job("deadline-test", "default", 3, 1);
    job.spec.active_deadline_seconds = Some(1);
    let key = build_key("jobs", Some("default"), "deadline-test");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());

    // First reconcile — creates 1 pod and sets start_time
    controller.reconcile_all().await.unwrap();

    // Set start_time to the past so the deadline is exceeded
    let mut fresh_job: Job = storage.get(&key).await.unwrap();
    if let Some(ref mut status) = fresh_job.status {
        status.start_time = Some(chrono::Utc::now() - chrono::Duration::seconds(10));
    }
    storage.update(&key, &fresh_job).await.unwrap();

    // Reconcile again — should detect deadline exceeded
    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&key).await.unwrap();
    let status = updated_job.status.unwrap();

    // Should have Failed condition with DeadlineExceeded reason
    if let Some(conditions) = status.conditions {
        let failed = conditions
            .iter()
            .find(|c| c.condition_type == "Failed" && c.status == "True");
        assert!(failed.is_some(), "Should have Failed condition");
        assert_eq!(
            failed.unwrap().reason.as_deref(),
            Some("DeadlineExceeded"),
            "Failed reason should be DeadlineExceeded"
        );
    } else {
        panic!("Job should have conditions after deadline exceeded");
    }
}

#[tokio::test]
async fn test_job_does_not_recreate_pods_when_complete() {
    let storage = setup_test().await;

    // Create job with 1 completion
    let job = create_test_job("once", "default", 1, 1);
    let key = build_key("jobs", Some("default"), "once");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Mark pod as succeeded
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1);
    let mut pod = pods[0].clone();
    pod.status = Some(PodStatus {
        phase: Some(Phase::Succeeded),
        ..Default::default()
    });
    let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
    storage.update(&pod_key, &pod).await.unwrap();

    // Reconcile — should mark job as Complete
    controller.reconcile_all().await.unwrap();
    let completed_job: Job = storage.get(&key).await.unwrap();
    assert!(completed_job
        .status
        .as_ref()
        .unwrap()
        .conditions
        .as_ref()
        .unwrap()
        .iter()
        .any(|c| c.condition_type == "Complete" && c.status == "True"));

    // Reconcile again — should NOT create new pods or change status
    controller.reconcile_all().await.unwrap();
    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods_after.len(),
        1,
        "Should not create new pods after completion"
    );
}

#[tokio::test]
async fn test_job_unsuspend_resumes_pod_creation() {
    let storage = setup_test().await;

    // Create a suspended job
    let mut job = create_test_job("resume-test", "default", 2, 2);
    job.spec.suspend = Some(true);
    let key = build_key("jobs", Some("default"), "resume-test");
    storage.create(&key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());

    // Reconcile — should NOT create pods while suspended
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 0, "Suspended job should not create pods");

    // Unsuspend the job
    let mut fresh_job: Job = storage.get(&key).await.unwrap();
    fresh_job.spec.suspend = Some(false);
    storage.update(&key, &fresh_job).await.unwrap();

    // Reconcile — should now create pods
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2, "Unsuspended job should create pods");
}
