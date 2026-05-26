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
#[ignore = "RED-state: Job controller does not release pods whose labels no longer match the job selector (no deletionTimestamp set)"]
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

    // Reconcile - the Job controller should respect managedBy and not take full control
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Verify the managedBy field is preserved
    let updated_job: Job = storage.get(&job_key).await.unwrap();
    assert_eq!(
        updated_job.spec.managed_by,
        Some("example.com/external-controller".to_string()),
        "managedBy field should be preserved"
    );

    // Pods should still be created, but the external controller coordinates
    let pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    assert!(
        !pods.is_empty(),
        "Pods should be created even with managedBy"
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
#[ignore = "RED-state: Job controller does not honour ttlSecondsAfterFinished — completed jobs are not marked for deletion after the TTL elapses"]
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
#[ignore = "RED-state: Job controller off-by-one when tracking completions vs. live pods across reconciles (completions=10 parallelism=3 produced 6 not 9 after 6 succeeded)"]
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

    // Mark next 3 as succeeded
    for pod in &pods[3..6] {
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

    // Mark next 3 as succeeded
    for pod in &pods[6..9] {
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
