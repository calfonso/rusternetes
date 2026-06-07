//! Extended Job Controller Tests — Phase 1 Priority Coverage
//!
//! Source of truth: Kubernetes v1.35 test/e2e/apps/job.go and
//! test/e2e/framework/job/wait.go
//!
//! This file extends the base job_controller_test.rs and conformance_apps_job_cronjob.rs
//! with additional scenarios from the upstream Go implementation that are critical
//! for 100% conformance coverage.
//!
//! Coverage goals:
//! - Orphan adoption and release
//! - Indexed job completion modes
//! - Success policies for distributed training
//! - Backoff limit per index
//! - ManagedBy field coordination
//! - Pod failure policies
//! - Node affinity constraints
//! - TTL seconds after finished cleanup

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, OwnerReference, Phase, TypeMeta};
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
            ..Default::default()
        }],
        restart_policy: Some("Never".to_string()),
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
            backoff_limit: Some(3),
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: base_pod_spec(),
            },
            active_deadline_seconds: None,
            ttl_seconds_after_finished: None,
            suspend: None,
            completion_mode: None,
            backoff_limit_per_index: None,
            max_failed_indexes: None,
            success_policy: None,
            managed_by: None,
            pod_failure_policy: None,
            manual_selector: None,
            pod_replacement_policy: None,
            selector: None,
        },
        status: None,
    }
}

fn make_indexed_job(name: &str, namespace: &str, completions: i32, parallelism: i32) -> Job {
    let mut job = make_job(name, namespace, completions, parallelism);
    job.spec.completion_mode = Some("Indexed".to_string());
    job
}

fn make_job_with_managed_by(name: &str, namespace: &str, managed_by: &str) -> Job {
    let mut job = make_job(name, namespace, 1, 1);
    job.spec.managed_by = Some(managed_by.to_string());
    job
}

fn make_job_with_node_affinity(name: &str, namespace: &str) -> Job {
    let mut job = make_job(name, namespace, 1, 1);

    // Add node affinity to schedule only on nodes with label "workload-type=batch"
    let match_expressions = vec![NodeSelectorRequirement {
        key: "workload-type".to_string(),
        operator: "In".to_string(),
        values: Some(vec!["batch".to_string()]),
    }];

    let node_selector_term = NodeSelectorTerm {
        match_expressions: Some(match_expressions),
        match_fields: None,
    };

    let preferred_scheduling_term = PreferredSchedulingTerm {
        preference: node_selector_term.clone(),
        weight: 100,
    };

    job.spec.template.spec.affinity = Some(Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: vec![node_selector_term],
            }),
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                preferred_scheduling_term,
            ]),
        }),
        pod_affinity: None,
        pod_anti_affinity: None,
    });

    job
}

fn make_job_with_ttl(name: &str, namespace: &str, ttl_seconds: i32) -> Job {
    let mut job = make_job(name, namespace, 1, 1);
    job.spec.ttl_seconds_after_finished = Some(ttl_seconds);
    job
}

fn make_job_with_success_policy(name: &str, namespace: &str, policy_type: &str) -> Job {
    let mut job = make_job(name, namespace, 4, 2);

    // SuccessPolicy for distributed training - succeed when at least N indexes succeed
    if policy_type == "AtLeastOnce" {
        job.spec.success_policy = Some(SuccessPolicy {
            rules: vec![SuccessPolicyRule {
                succeeded_indexes: Some("0".to_string()),
                succeeded_count: None,
            }],
        });
    } else if policy_type == "AllIndexes" {
        job.spec.success_policy = Some(SuccessPolicy {
            rules: vec![SuccessPolicyRule {
                succeeded_indexes: Some("0-3".to_string()),
                succeeded_count: Some(4),
            }],
        });
    }

    job
}

fn make_job_with_pod_failure_policy(name: &str, namespace: &str, rule_type: &str) -> Job {
    let mut job = make_job(name, namespace, 4, 2);

    // PodFailurePolicy for handling specific failure scenarios
    if rule_type == "FailJob" {
        job.spec.pod_failure_policy = Some(PodFailurePolicy {
            rules: vec![PodFailurePolicyRule {
                action: "FailJob".to_string(),
                on_exit_codes: Some(PodFailurePolicyOnExitCodesRequirement {
                    container_name: Some("task".to_string()),
                    operator: "In".to_string(),
                    values: vec![1],
                }),
                on_pod_conditions: vec![],
            }],
        });
    } else if rule_type == "Ignore" {
        job.spec.pod_failure_policy = Some(PodFailurePolicy {
            rules: vec![PodFailurePolicyRule {
                action: "Ignore".to_string(),
                on_exit_codes: None,
                on_pod_conditions: vec![PodFailurePolicyOnPodConditionsPattern {
                    condition_type: "DisruptionTarget".to_string(),
                    status: Some("True".to_string()),
                }],
            }],
        });
    }

    job
}

// Helper to simulate kubelet cleanup (delete pods marked for deletion)
async fn simulate_kubelet_cleanup(storage: &Arc<MemoryStorage>, namespace: &str) {
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    for pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            let key = build_key("pods", Some(namespace), &pod.metadata.name);
            let _ = storage.delete(&key).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Test: Job should adopt matching orphans
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_should_adopt_matching_orphan_pods() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job
    let job = make_job("adoption-test", namespace, 2, 2);
    let job_key = build_key("jobs", Some(namespace), "adoption-test");
    storage.create(&job_key, &job).await.unwrap();

    // Create an orphan pod with matching labels (no owner reference)
    let orphan_pod_labels = job.spec.template.metadata.as_ref().unwrap().labels.clone();
    let orphan_pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("orphan-pod");
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta.labels = orphan_pod_labels.clone();
            meta
        },
        spec: Some(base_pod_spec()),
        status: None,
    };

    let orphan_pod_key = build_key("pods", Some(namespace), "orphan-pod");
    storage.create(&orphan_pod_key, &orphan_pod).await.unwrap();

    // Reconcile
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify the orphan pod now has owner reference to the Job
    let updated_pod: Pod = storage.get(&orphan_pod_key).await.unwrap();
    let owner_refs = updated_pod.metadata.owner_references.unwrap();
    assert_eq!(
        owner_refs.len(),
        1,
        "Orphan pod should have one owner reference"
    );
    assert_eq!(owner_refs[0].kind, "Job", "Owner should be a Job");
    assert_eq!(
        owner_refs[0].name, "adoption-test",
        "Owner should be our job"
    );
    assert!(
        owner_refs[0].controller.unwrap(),
        "Owner should be controller"
    );
}

// ---------------------------------------------------------------------------
// Test: Job should release non-matching pods
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_should_release_non_matching_pods() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job with specific selector
    let mut job = make_job("release-test", namespace, 2, 2);
    let mut selector_labels = HashMap::new();
    selector_labels.insert("job-name".to_string(), "release-test".to_string());
    selector_labels.insert("version".to_string(), "v1".to_string());
    job.spec.selector = Some(LabelSelector {
        match_labels: Some(selector_labels.clone()),
        match_expressions: None,
    });

    let job_key = build_key("jobs", Some(namespace), "release-test");
    storage.create(&job_key, &job).await.unwrap();

    // Create a pod that was owned by this job but now has mismatched labels
    let old_pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("old-version-pod");
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            // Old labels - missing "version" label
            let mut labels = HashMap::new();
            labels.insert("job-name".to_string(), "release-test".to_string());
            meta.labels = Some(labels);

            // Has owner reference to the job
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "batch/v1".to_string(),
                kind: "Job".to_string(),
                name: "release-test".to_string(),
                uid: job.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: None,
            }]);
            meta
        },
        spec: Some(base_pod_spec()),
        status: None,
    };

    let old_pod_key = build_key("pods", Some(namespace), "old-version-pod");
    storage.create(&old_pod_key, &old_pod).await.unwrap();

    // Reconcile
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // The non-matching pod should be marked for deletion
    let updated_pod: Pod = storage.get(&old_pod_key).await.unwrap();
    assert!(
        updated_pod.metadata.deletion_timestamp.is_some(),
        "Non-matching pod should be marked for deletion"
    );
}

// ---------------------------------------------------------------------------
// Test: Indexed Job completion tracking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn indexed_job_should_track_completion_per_index() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create an indexed Job with 4 completions
    let job = make_indexed_job("indexed-completion", namespace, 4, 2);
    let job_key = build_key("jobs", Some(namespace), "indexed-completion");
    storage.create(&job_key, &job).await.unwrap();

    // Reconcile to create initial pods
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Check that pods were created with completion index annotations
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    assert_eq!(pods.len(), 2, "Should create parallelism=2 pods");

    // Verify pods have batch.kubernetes.io/job-completion-index annotation
    for pod in &pods {
        let annotations = pod.metadata.annotations.as_ref().unwrap();
        assert!(
            annotations.contains_key("batch.kubernetes.io/job-completion-index"),
            "Pod should have completion index annotation"
        );
    }

    // Mark first two pods as succeeded (indexes 0 and 1)
    for (i, pod) in pods.iter().enumerate() {
        let mut updated_pod = pod.clone();
        updated_pod.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &updated_pod).await.unwrap();

        // Annotate with specific index
        let mut pod_with_index: Pod = storage.get(&pod_key).await.unwrap();
        if let Some(ref mut annotations) = pod_with_index.metadata.annotations {
            annotations.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                i.to_string(),
            );
        }
        storage.update(&pod_key, &pod_with_index).await.unwrap();
    }

    // Reconcile - should create pods for remaining indexes (2 and 3)
    controller.reconcile_all().await.unwrap();

    let all_pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    assert_eq!(all_pods.len(), 4, "Should have pods for all 4 indexes");
}

// ---------------------------------------------------------------------------
// Test: Job with successPolicy - AtLeastOnce
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_with_success_policy_at_least_once_should_succeed_early() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job with successPolicy requiring only index 0 to succeed
    let job = make_job_with_success_policy("success-policy-test", namespace, "AtLeastOnce");
    let job_key = build_key("jobs", Some(namespace), "success-policy-test");
    storage.create(&job_key, &job).await.unwrap();

    // Reconcile to create pods
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    // Find and mark the pod for index 0 as succeeded
    for pod in &pods {
        let annotations = pod.metadata.annotations.as_ref().unwrap();
        if let Some(index_str) = annotations.get("batch.kubernetes.io/job-completion-index") {
            if index_str == "0" {
                let mut success_pod = pod.clone();
                success_pod.status = Some(PodStatus {
                    phase: Some(Phase::Succeeded),
                    ..Default::default()
                });
                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                storage.update(&pod_key, &success_pod).await.unwrap();
                break;
            }
        }
    }

    // Reconcile - should mark job as complete even though other pods haven't succeeded
    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job.status.as_ref().unwrap();

    // Job should be complete because successPolicy is satisfied
    if let Some(conditions) = &status.conditions {
        let complete = conditions
            .iter()
            .find(|c| c.condition_type == "Complete" && c.status == "True");
        assert!(
            complete.is_some(),
            "Job should be Complete when successPolicy is satisfied"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: Job with backoffLimitPerIndex
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_with_backoff_limit_per_index_should_retry_individual_indexes() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create an indexed Job with backoffLimitPerIndex
    let mut job = make_indexed_job("backoff-per-index", namespace, 4, 2);
    job.spec.backoff_limit_per_index = Some(2);
    let job_key = build_key("jobs", Some(namespace), "backoff-per-index");
    storage.create(&job_key, &job).await.unwrap();

    // Reconcile to create pods
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    // Mark first pod (index 0) as failed twice
    for pod in &pods[..1] {
        for _attempt in 0..2 {
            let mut failed_pod = pod.clone();
            failed_pod.status = Some(PodStatus {
                phase: Some(Phase::Failed),
                ..Default::default()
            });
            let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
            storage.update(&pod_key, &failed_pod).await.unwrap();

            // Simulate kubelet cleanup
            simulate_kubelet_cleanup(&storage, namespace).await;

            // Reconcile to create replacement
            controller.reconcile_all().await.unwrap();
        }
    }

    // After backoffLimitPerIndex failures, the index should be marked as failed
    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job.status.as_ref().unwrap();

    // Check that failed indexes are tracked
    if let Some(failed_indexes) = &status.failed_indexes {
        assert!(
            failed_indexes.contains("0"),
            "Index 0 should be in failed indexes after exceeding backoff limit"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: Job with managedBy field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_with_managed_by_should_coordinate_with_external_controller() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job managed by an external controller
    let job = make_job_with_managed_by("managed-job", namespace, "example.com/external-controller");
    let job_key = build_key("jobs", Some(namespace), "managed-job");
    storage.create(&job_key, &job).await.unwrap();

    // Reconcile - the in-tree controller must respect managedBy: when an
    // external controller owns the Job, the in-tree controller takes no action
    // (no pods, no status mutation). K8s ref:
    // pkg/controller/job/job_controller.go syncJob early-returns on a managedBy
    // mismatch.
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify the managedBy field is preserved (the in-tree controller didn't
    // touch the spec).
    let updated_job: Job = storage.get(&job_key).await.unwrap();
    assert_eq!(
        updated_job.spec.managed_by,
        Some("example.com/external-controller".to_string()),
        "managedBy field should be preserved"
    );

    // No pods are created — the external controller owns the Job's lifecycle.
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert!(
        pods.is_empty(),
        "In-tree controller must not create pods when managedBy points at an external controller (got {} pods)",
        pods.len()
    );
}

// ---------------------------------------------------------------------------
// Test: Job with node affinity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_with_node_affinity_should_respect_scheduling_constraints() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job with node affinity
    let job = make_job_with_node_affinity("affinity-job", namespace);
    let job_key = build_key("jobs", Some(namespace), "affinity-job");
    storage.create(&job_key, &job).await.unwrap();

    // Reconcile to create pods
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    assert_eq!(pods.len(), 1, "Should create one pod");

    // Verify the pod inherited the node affinity from the job template
    let pod = &pods[0];
    assert!(
        pod.spec.as_ref().unwrap().affinity.is_some(),
        "Pod should have affinity settings"
    );
    assert!(
        pod.spec
            .as_ref()
            .unwrap()
            .affinity
            .as_ref()
            .unwrap()
            .node_affinity
            .is_some(),
        "Pod should have node affinity"
    );
}

// ---------------------------------------------------------------------------
// Test: Job with pod failure policy - FailJob
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_with_pod_failure_policy_fail_job_should_fail_on_exit_code() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job with pod failure policy
    let job = make_job_with_pod_failure_policy("fail-policy-job", namespace, "FailJob");
    let job_key = build_key("jobs", Some(namespace), "fail-policy-job");
    storage.create(&job_key, &job).await.unwrap();

    // Reconcile to create pods
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    // Mark pod as failed with exit code 1
    for pod in &pods {
        let mut failed_pod = pod.clone();
        failed_pod.status = Some(PodStatus {
            phase: Some(Phase::Failed),
            container_statuses: Some(vec![ContainerStatus {
                name: "task".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState::Terminated {
                    exit_code: 1,
                    signal: None,
                    reason: Some("Error".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
                last_state: None,
                image: None,
                image_id: None,
                container_id: None,
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            }]),
            ..Default::default()
        });
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &failed_pod).await.unwrap();
    }

    // Reconcile - should fail the entire job due to pod failure policy
    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job.status.as_ref().unwrap();

    // Job should be Failed due to the pod failure policy
    if let Some(conditions) = &status.conditions {
        let failed = conditions
            .iter()
            .find(|c| c.condition_type == "Failed" && c.status == "True");
        assert!(
            failed.is_some(),
            "Job should be Failed when pod failure policy triggers"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: Job with pod failure policy - Ignore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_with_pod_failure_policy_ignore_should_continue_on_disruption() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job with pod failure policy to ignore disruption
    let job = make_job_with_pod_failure_policy("ignore-policy-job", namespace, "Ignore");
    let job_key = build_key("jobs", Some(namespace), "ignore-policy-job");
    storage.create(&job_key, &job).await.unwrap();

    // Reconcile to create pods
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    // Mark pod as failed due to disruption (preemption, node drain, etc.)
    for pod in &pods {
        let mut disrupted_pod = pod.clone();
        disrupted_pod.status = Some(PodStatus {
            phase: Some(Phase::Failed),
            conditions: Some(vec![PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            ..Default::default()
        });
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &disrupted_pod).await.unwrap();
    }

    // Reconcile - should ignore the failure and create replacement
    controller.reconcile_all().await.unwrap();

    // Job should NOT be marked as Failed
    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job.status.as_ref().unwrap();

    if let Some(conditions) = &status.conditions {
        let failed = conditions
            .iter()
            .find(|c| c.condition_type == "Failed" && c.status == "True");
        assert!(
            failed.is_none(),
            "Job should NOT be Failed when pod failure policy ignores disruption"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: Job TTL seconds after finished
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_with_ttl_should_be_cleaned_up_after_completion() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job with TTL of 60 seconds after finished
    let job = make_job_with_ttl("ttl-job", namespace, 60);
    let job_key = build_key("jobs", Some(namespace), "ttl-job");
    storage.create(&job_key, &job).await.unwrap();

    // Reconcile to create pods
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    // Mark pod as succeeded
    for pod in &pods {
        let mut success_pod = pod.clone();
        success_pod.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &success_pod).await.unwrap();
    }

    // Reconcile to mark job as complete
    controller.reconcile_all().await.unwrap();

    // Set completion time to past TTL
    let mut completed_job: Job = storage.get(&job_key).await.unwrap();
    if let Some(ref mut status) = completed_job.status {
        use chrono::{Duration, Utc};
        status.completion_time = Some(Utc::now() - Duration::seconds(120)); // 2 minutes ago
    }
    storage.update(&job_key, &completed_job).await.unwrap();

    // Reconcile - should delete the job due to TTL expiration
    controller.reconcile_all().await.unwrap();

    // Job should be marked for deletion
    let maybe_job: Option<Job> = storage.get(&job_key).await.ok();
    if let Some(job_obj) = maybe_job {
        assert!(
            job_obj.metadata.deletion_timestamp.is_some(),
            "Job should be marked for deletion after TTL expires"
        );
    }
    // Note: In real implementation, the job would be deleted from storage
}

// ---------------------------------------------------------------------------
// Test: Job multiple completions with varying parallelism
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_should_respect_parallelism_across_multiple_reconciles() {
    let storage = setup_test().await;
    let namespace = "default";

    // Create a Job with 10 completions but parallelism of 3
    let job = make_job("parallelism-test", namespace, 10, 3);
    let job_key = build_key("jobs", Some(namespace), "parallelism-test");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());

    // First reconcile - should create 3 pods (parallelism limit)
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert_eq!(pods.len(), 3, "Should create exactly parallelism pods");

    // Mark all 3 pods as succeeded
    for pod in &pods {
        let mut success_pod = pod.clone();
        success_pod.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &success_pod).await.unwrap();
    }

    // Second reconcile - should create 3 more pods
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert_eq!(
        pods.len(),
        6,
        "Should create 3 more pods after first batch succeeds"
    );

    // Mark the freshly-created (not-yet-succeeded) pods as succeeded. Selecting
    // by state rather than slice index keeps this order-independent: list()
    // returns key-sorted (not creation-ordered) results, so a new pod may sort
    // among the already-succeeded ones.
    for pod in pods
        .iter()
        .filter(|p| p.status.as_ref().and_then(|s| s.phase.as_ref()) != Some(&Phase::Succeeded))
    {
        let mut success_pod = pod.clone();
        success_pod.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &success_pod).await.unwrap();
    }

    // Third reconcile - should create 3 more pods (total 9)
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert_eq!(pods.len(), 9, "Should create 3 more pods");

    // Mark the freshly-created (not-yet-succeeded) pods as succeeded
    // (state-based selection — order-independent, see note above).
    for pod in pods
        .iter()
        .filter(|p| p.status.as_ref().and_then(|s| s.phase.as_ref()) != Some(&Phase::Succeeded))
    {
        let mut success_pod = pod.clone();
        success_pod.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &success_pod).await.unwrap();
    }

    // Fourth reconcile - should create only 1 more pod (to reach 10 completions)
    controller.reconcile_all().await.unwrap();
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert_eq!(
        pods.len(),
        10,
        "Should create only 1 final pod to reach completions"
    );
}

// ---------------------------------------------------------------------------
// Extended scenarios — Phase 1.1 batch worker 1/27 extension
//
// The tests below extend the coverage above with additional scenarios borrowed
// from kubernetes/test/e2e/apps/job.go and the corresponding integration
// tests under test/integration/job. They target the same six feature
// categories (Indexed, SuccessPolicy, BackoffLimitPerIndex, ManagedBy,
// PodFailurePolicy, NodeAffinity) but exercise different rules / edge cases
// than the original eight, e.g. SuccessPolicy.succeededCount vs.
// succeededIndexes, PodFailurePolicy FailIndex / Count actions, the
// status.completedIndexes range encoding, and managedBy contract semantics.
// ---------------------------------------------------------------------------

/// Mark every pod owned by `job_name` as Succeeded and annotate it with the
/// supplied completion index. Mirrors what the kubelet + status manager do
/// once a container in an Indexed Job exits 0.
async fn mark_indexed_pods_succeeded(
    storage: &Arc<MemoryStorage>,
    namespace: &str,
    job_name: &str,
    indexes: &[i32],
) {
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();

    // Filter to pods owned by this Job, then assign one index per pod in order.
    let owned: Vec<Pod> = pods
        .into_iter()
        .filter(|p| {
            p.metadata
                .owner_references
                .as_ref()
                .is_some_and(|refs| refs.iter().any(|r| r.name == job_name && r.kind == "Job"))
        })
        .collect();

    for (pod, idx) in owned.iter().zip(indexes.iter()) {
        let mut updated = pod.clone();
        updated.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        // Overwrite (or insert) the completion-index annotation so the
        // controller treats this specific pod as completing that index.
        let mut annotations = updated.metadata.annotations.clone().unwrap_or_default();
        annotations.insert(
            "batch.kubernetes.io/job-completion-index".to_string(),
            idx.to_string(),
        );
        updated.metadata.annotations = Some(annotations);

        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &updated).await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Test: Indexed Job — status.completedIndexes is populated as a range string
// ---------------------------------------------------------------------------
//
// Upstream contract: pkg/controller/job/indexed_job_utils.go encodes the set
// of succeeded indexes as a comma-separated range string (e.g. "0-2,5").
// The status field is what kubectl and downstream tooling display.
#[tokio::test]
async fn indexed_job_should_populate_completed_indexes_range_in_status() {
    let storage = setup_test().await;
    let namespace = "default";

    let job = make_indexed_job("completed-indexes", namespace, 4, 4);
    let job_key = build_key("jobs", Some(namespace), "completed-indexes");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());

    // First reconcile: spawn the four pods.
    controller.reconcile_all().await.unwrap();

    // Mark indexes 0, 1, 2 succeeded — index 3 still active.
    mark_indexed_pods_succeeded(&storage, namespace, "completed-indexes", &[0, 1, 2]).await;

    // Second reconcile: controller should publish completed_indexes = "0-2".
    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job
        .status
        .as_ref()
        .expect("status should be set after reconcile");
    assert_eq!(
        status.completed_indexes.as_deref(),
        Some("0-2"),
        "completed_indexes should encode succeeded indexes as a range string"
    );
}

// ---------------------------------------------------------------------------
// Test: Job successPolicy — succeededCount rule
// ---------------------------------------------------------------------------
//
// Distinct from the existing `job_with_success_policy_at_least_once_should_*`
// test, which exercises the `succeededIndexes` form. Upstream supports a
// `succeededCount` rule that succeeds the Job as soon as N indexes have
// completed, regardless of which indexes.
#[tokio::test]
async fn job_with_success_policy_succeeded_count_should_complete_on_threshold() {
    let storage = setup_test().await;
    let namespace = "default";

    // SuccessPolicy: succeed when any 2 indexes complete (completions=4).
    let mut job = make_indexed_job("success-count", namespace, 4, 4);
    job.spec.success_policy = Some(SuccessPolicy {
        rules: vec![SuccessPolicyRule {
            succeeded_indexes: None,
            succeeded_count: Some(2),
        }],
    });
    let job_key = build_key("jobs", Some(namespace), "success-count");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Only two of the four pods succeed (indexes 0 and 2).
    mark_indexed_pods_succeeded(&storage, namespace, "success-count", &[0, 2]).await;

    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job
        .status
        .as_ref()
        .expect("status should be set after reconcile");
    let conditions = status
        .conditions
        .as_ref()
        .expect("conditions should be populated when successPolicy matches");
    let complete = conditions
        .iter()
        .find(|c| c.condition_type == "Complete" && c.status == "True");
    assert!(
        complete.is_some(),
        "Job should be Complete once succeededCount=2 rule is met (got: {conditions:?})"
    );
}

// ---------------------------------------------------------------------------
// Test: Job with podFailurePolicy FailIndex — only the matching index fails
// ---------------------------------------------------------------------------
//
// Mirrors test/e2e/apps/job.go "should mark indexes as failed when matches
// FailIndex action in pod failure policy".
#[tokio::test]
async fn job_with_pod_failure_policy_fail_index_should_mark_only_that_index_failed() {
    let storage = setup_test().await;
    let namespace = "default";

    let mut job = make_indexed_job("fail-index-policy", namespace, 3, 3);
    job.spec.backoff_limit_per_index = Some(0);
    job.spec.pod_failure_policy = Some(PodFailurePolicy {
        rules: vec![PodFailurePolicyRule {
            action: "FailIndex".to_string(),
            on_exit_codes: Some(PodFailurePolicyOnExitCodesRequirement {
                container_name: Some("task".to_string()),
                operator: "In".to_string(),
                values: vec![42],
            }),
            on_pod_conditions: vec![],
        }],
    });
    let job_key = build_key("jobs", Some(namespace), "fail-index-policy");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Find pods and fail exactly one of them with exit code 42, annotated as
    // index 1. The other two indexes (0, 2) remain Pending.
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    let owned: Vec<Pod> = pods
        .into_iter()
        .filter(|p| {
            p.metadata
                .owner_references
                .as_ref()
                .is_some_and(|refs| refs.iter().any(|r| r.kind == "Job"))
        })
        .collect();
    assert!(
        owned.len() >= 3,
        "Expected at least 3 owned pods for an Indexed job with parallelism=3, got {}",
        owned.len()
    );

    let mut failed_pod = owned[1].clone();
    failed_pod.status = Some(PodStatus {
        phase: Some(Phase::Failed),
        container_statuses: Some(vec![ContainerStatus {
            name: "task".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState::Terminated {
                exit_code: 42,
                signal: None,
                reason: Some("Error".to_string()),
                message: None,
                started_at: None,
                finished_at: None,
                container_id: None,
            }),
            last_state: None,
            image: None,
            image_id: None,
            container_id: None,
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]),
        ..Default::default()
    });
    let mut annotations = failed_pod.metadata.annotations.clone().unwrap_or_default();
    annotations.insert(
        "batch.kubernetes.io/job-completion-index".to_string(),
        "1".to_string(),
    );
    failed_pod.metadata.annotations = Some(annotations);
    let pod_key = build_key("pods", Some(namespace), &failed_pod.metadata.name);
    storage.update(&pod_key, &failed_pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job.status.as_ref().expect("status set");
    let failed_indexes = status
        .failed_indexes
        .as_deref()
        .expect("failed_indexes should be set when FailIndex action triggers");
    assert!(
        failed_indexes.contains('1'),
        "Index 1 should appear in failed_indexes (got: {failed_indexes:?})"
    );

    // Job MUST NOT be marked Failed — only the single index failed.
    if let Some(ref conditions) = status.conditions {
        let job_failed = conditions
            .iter()
            .find(|c| c.condition_type == "Failed" && c.status == "True");
        assert!(
            job_failed.is_none(),
            "Job should not be Failed overall when only one index hit a FailIndex rule",
        );
    }
}

// ---------------------------------------------------------------------------
// Test: Job with podFailurePolicy Count action — increments backoff counter
// ---------------------------------------------------------------------------
//
// The `Count` action is the default for podFailurePolicy — it makes the
// failure count toward `.spec.backoffLimit` like a normal failure. Setting
// backoffLimit=0 lets us verify the rule path: a single Count-matched
// failure should fail the whole job.
#[tokio::test]
async fn job_with_pod_failure_policy_count_should_count_toward_backoff_limit() {
    let storage = setup_test().await;
    let namespace = "default";

    let mut job = make_job("count-policy", namespace, 1, 1);
    job.spec.backoff_limit = Some(0);
    job.spec.pod_failure_policy = Some(PodFailurePolicy {
        rules: vec![PodFailurePolicyRule {
            action: "Count".to_string(),
            on_exit_codes: Some(PodFailurePolicyOnExitCodesRequirement {
                container_name: None,
                operator: "In".to_string(),
                values: vec![7],
            }),
            on_pod_conditions: vec![],
        }],
    });
    let job_key = build_key("jobs", Some(namespace), "count-policy");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Fail the single pod with exit code 7.
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert_eq!(pods.len(), 1, "Job should have created exactly one pod");

    let mut failed = pods[0].clone();
    failed.status = Some(PodStatus {
        phase: Some(Phase::Failed),
        container_statuses: Some(vec![ContainerStatus {
            name: "task".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState::Terminated {
                exit_code: 7,
                signal: None,
                reason: Some("Error".to_string()),
                message: None,
                started_at: None,
                finished_at: None,
                container_id: None,
            }),
            last_state: None,
            image: None,
            image_id: None,
            container_id: None,
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }]),
        ..Default::default()
    });
    let pod_key = build_key("pods", Some(namespace), &failed.metadata.name);
    storage.update(&pod_key, &failed).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // With backoffLimit=0 and one Count-matched failure, job should now be Failed.
    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job.status.as_ref().expect("status set");
    let conditions = status.conditions.as_ref().expect("conditions populated");
    let failed_cond = conditions
        .iter()
        .find(|c| c.condition_type == "Failed" && c.status == "True");
    assert!(
        failed_cond.is_some(),
        "Job should be Failed after backoffLimit (0) is exceeded by Count-matched failure"
    );
}

// ---------------------------------------------------------------------------
// Test: Job with maxFailedIndexes — terminates early when threshold exceeded
// ---------------------------------------------------------------------------
//
// maxFailedIndexes is the upper bound on how many indexes may fail before the
// entire indexed Job is failed. Pair with backoffLimitPerIndex so each
// failure is counted per-index. Threshold=1 + two failed indexes =>
// job Failed with reason "MaxFailedIndexesExceeded".
#[tokio::test]
async fn job_with_max_failed_indexes_should_fail_when_exceeded() {
    let storage = setup_test().await;
    let namespace = "default";

    let mut job = make_indexed_job("max-failed-idx", namespace, 4, 4);
    job.spec.backoff_limit_per_index = Some(0);
    job.spec.max_failed_indexes = Some(1);
    let job_key = build_key("jobs", Some(namespace), "max-failed-idx");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    let owned: Vec<Pod> = pods
        .into_iter()
        .filter(|p| {
            p.metadata
                .owner_references
                .as_ref()
                .is_some_and(|refs| refs.iter().any(|r| r.kind == "Job"))
        })
        .collect();
    assert!(
        owned.len() >= 2,
        "Need at least 2 owned pods to fail two distinct indexes, got {}",
        owned.len()
    );

    // Fail two distinct indexes (0 and 1) — that exceeds maxFailedIndexes=1.
    for (i, pod) in owned.iter().take(2).enumerate() {
        let mut failed = pod.clone();
        failed.status = Some(PodStatus {
            phase: Some(Phase::Failed),
            ..Default::default()
        });
        let mut annotations = failed.metadata.annotations.clone().unwrap_or_default();
        annotations.insert(
            "batch.kubernetes.io/job-completion-index".to_string(),
            i.to_string(),
        );
        failed.metadata.annotations = Some(annotations);
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        storage.update(&pod_key, &failed).await.unwrap();
    }

    controller.reconcile_all().await.unwrap();

    let updated_job: Job = storage.get(&job_key).await.unwrap();
    let status = updated_job.status.as_ref().expect("status set");
    let conditions = status.conditions.as_ref().expect("conditions populated");
    let job_failed = conditions
        .iter()
        .find(|c| c.condition_type == "Failed" && c.status == "True");
    assert!(
        job_failed.is_some(),
        "Job should be Failed once failed indexes (2) exceed maxFailedIndexes (1)"
    );
}

// ---------------------------------------------------------------------------
// Test: Job managedBy default value — reconciliation proceeds normally
// ---------------------------------------------------------------------------
//
// Upstream constant: batch/v1.JobControllerName == "kubernetes.io/job-controller".
// When managedBy is unset OR equal to that string, the in-tree Job
// controller is responsible. We verify that pods are created in this case.
#[tokio::test]
async fn job_with_managed_by_default_controller_should_reconcile_normally() {
    let storage = setup_test().await;
    let namespace = "default";

    let job =
        make_job_with_managed_by("managed-default", namespace, "kubernetes.io/job-controller");
    let job_key = build_key("jobs", Some(namespace), "managed-default");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // managedBy preserved
    let updated_job: Job = storage.get(&job_key).await.unwrap();
    assert_eq!(
        updated_job.spec.managed_by.as_deref(),
        Some("kubernetes.io/job-controller"),
        "managedBy default value should be preserved across reconcile"
    );

    // Pods MUST be created since the in-tree controller owns the Job.
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert!(
        !pods.is_empty(),
        "Default managedBy means the in-tree controller manages — pods must be created"
    );
}

// ---------------------------------------------------------------------------
// Test: Job managedBy external controller — in-tree controller skips work
// ---------------------------------------------------------------------------
//
// RED-state: the current implementation does NOT honour managedBy. When an
// external controller name is set (e.g. Kueue's "kueue.x-k8s.io/multikueue"),
// the in-tree Job controller MUST NOT create pods or mutate status. See
// pkg/controller/job/job_controller.go:syncJob — early-return on managedBy
// mismatch. Pin as ignored until we implement the same gate.
#[tokio::test]
async fn job_with_external_managed_by_should_skip_pod_creation() {
    let storage = setup_test().await;
    let namespace = "default";

    let job = make_job_with_managed_by("external-managed", namespace, "kueue.x-k8s.io/multikueue");
    let job_key = build_key("jobs", Some(namespace), "external-managed");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // External managedBy means an external controller owns the lifecycle —
    // the in-tree controller must not create pods.
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert!(
        pods.is_empty(),
        "In-tree Job controller must not create pods when managedBy points at an external controller (got {} pods)",
        pods.len()
    );
}

// ---------------------------------------------------------------------------
// Test: Required-only node affinity — inherited verbatim by the spawned pod
// ---------------------------------------------------------------------------
//
// Complements `job_with_node_affinity_should_respect_scheduling_constraints`
// which exercises required + preferred. Here we verify the
// required-only path: when only `requiredDuringSchedulingIgnoredDuringExecution`
// is set, the pod must inherit the exact same NodeSelectorTerms and the
// preferred slice on the pod must remain unset (or empty), so the
// scheduler sees the same constraint set.
#[tokio::test]
async fn job_with_required_only_node_affinity_should_inherit_constraints_verbatim() {
    let storage = setup_test().await;
    let namespace = "default";

    let mut job = make_job("required-affinity", namespace, 1, 1);
    let term = NodeSelectorTerm {
        match_expressions: Some(vec![NodeSelectorRequirement {
            key: "topology.kubernetes.io/zone".to_string(),
            operator: "In".to_string(),
            values: Some(vec!["us-east-1a".to_string()]),
        }]),
        match_fields: None,
    };
    job.spec.template.spec.affinity = Some(Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                node_selector_terms: vec![term.clone()],
            }),
            preferred_during_scheduling_ignored_during_execution: None,
        }),
        pod_affinity: None,
        pod_anti_affinity: None,
    });

    let job_key = build_key("jobs", Some(namespace), "required-affinity");
    storage.create(&job_key, &job).await.unwrap();

    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert_eq!(pods.len(), 1, "Expected exactly one pod for the Job");

    let pod_spec = pods[0].spec.as_ref().expect("pod spec set");
    let node_affinity = pod_spec
        .affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
        .expect("node_affinity must be inherited from the Job template");

    let required = node_affinity
        .required_during_scheduling_ignored_during_execution
        .as_ref()
        .expect("required node affinity must be present on the pod");
    assert_eq!(
        required.node_selector_terms.len(),
        1,
        "Pod should carry the single NodeSelectorTerm from the job template"
    );
    assert_eq!(
        required.node_selector_terms[0], term,
        "NodeSelectorTerm must be inherited verbatim — same key/operator/values"
    );

    // Preferred path was unset on the job — the pod must NOT invent one.
    let preferred = node_affinity
        .preferred_during_scheduling_ignored_during_execution
        .as_ref();
    assert!(
        preferred.is_none() || preferred.map(Vec::is_empty).unwrap_or(true),
        "Pod must not introduce a preferred affinity that wasn't in the job template"
    );
}
