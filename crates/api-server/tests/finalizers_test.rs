//! Additional edge case tests for finalizers handler

use chrono::Utc;
use rusternetes_api_server::handlers::finalizers::{handle_delete_with_finalizers, HasMetadata};
use rusternetes_common::resources::{Pod, PodSpec};

use rusternetes_storage::{memory::MemoryStorage, Storage};
use std::sync::Arc;

fn empty_pod_spec() -> PodSpec {
    PodSpec {
        containers: vec![],
        init_containers: None,
        ephemeral_containers: None,
        restart_policy: None,
        node_selector: None,
        service_account_name: None,
        service_account: None,
        node_name: None,
        host_network: None,
        host_pid: None,
        host_ipc: None,
        volumes: None,
        hostname: None,
        subdomain: None,
        affinity: None,
        scheduler_name: None,
        tolerations: None,
        priority_class_name: None,
        priority: None,
        overhead: None,
        topology_spread_constraints: None,
        automount_service_account_token: None,
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

#[tokio::test]
async fn test_delete_resource_with_empty_finalizers_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create pod with empty finalizers list
    let mut pod = Pod::new("test-pod-empty-finalizers", empty_pod_spec());
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.ensure_uid();
    pod.metadata.ensure_creation_timestamp();
    pod.metadata.finalizers = Some(Vec::new()); // Empty list

    let key = "test/pods/default/test-pod-empty-finalizers";
    storage.create(key, &pod).await.unwrap();

    // Delete should remove immediately (empty finalizers = no finalizers)
    let deleted = handle_delete_with_finalizers(&storage, key, &pod)
        .await
        .unwrap();

    assert!(
        !deleted,
        "Resource with empty finalizers should be deleted immediately"
    );

    // Verify it's gone
    let result = storage.get::<Pod>(key).await;
    assert!(result.is_err(), "Resource should be deleted from storage");
}

#[tokio::test]
async fn test_delete_resource_with_multiple_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    // Create pod with multiple finalizers
    let mut pod = Pod::new("test-pod-multiple-finalizers", empty_pod_spec());
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.ensure_uid();
    pod.metadata.ensure_creation_timestamp();
    pod.metadata.finalizers = Some(vec![
        "finalizer.one.io".to_string(),
        "finalizer.two.io".to_string(),
        "finalizer.three.io".to_string(),
    ]);

    let key = "test/pods/default/test-pod-multiple-finalizers";
    storage.create(key, &pod).await.unwrap();

    // First delete should mark for deletion
    let marked = handle_delete_with_finalizers(&storage, key, &pod)
        .await
        .unwrap();

    assert!(marked, "Resource should be marked for deletion");

    // Verify it still exists with deletionTimestamp
    let updated_pod: Pod = storage.get(key).await.unwrap();
    assert!(updated_pod.metadata.deletion_timestamp.is_some());
    assert_eq!(updated_pod.metadata.finalizers.as_ref().unwrap().len(), 3);

    // Clean up
    storage.delete(key).await.unwrap();
}

#[tokio::test]
async fn test_delete_already_marked_logs_correctly() {
    let storage = Arc::new(MemoryStorage::new());

    // Create pod with finalizer and deletion timestamp
    let mut pod = Pod::new("test-pod-already-marked", empty_pod_spec());
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.ensure_uid();
    pod.metadata.ensure_creation_timestamp();
    pod.metadata.finalizers = Some(vec!["test.finalizer.io".to_string()]);
    pod.metadata.deletion_timestamp = Some(Utc::now());

    let key = "test/pods/default/test-pod-already-marked";
    storage.create(key, &pod).await.unwrap();

    // Baseline is the stored value (serialized at second precision per k8s
    // metav1.Time), not the in-memory nanosecond value handed to create().
    let before: Pod = storage.get(key).await.unwrap();

    // Delete should return true but not modify resource
    let marked = handle_delete_with_finalizers(&storage, key, &pod)
        .await
        .unwrap();

    assert!(marked, "Already marked resource should return true");

    // Verify deletion timestamp didn't change
    let updated_pod: Pod = storage.get(key).await.unwrap();
    assert_eq!(
        updated_pod.metadata.deletion_timestamp, before.metadata.deletion_timestamp,
        "Deletion timestamp should not change"
    );

    // Clean up
    storage.delete(key).await.unwrap();
}

#[tokio::test]
async fn test_delete_finalizer_workflow_complete() {
    let storage = Arc::new(MemoryStorage::new());

    // Step 1: Create pod with finalizers
    let mut pod = Pod::new("test-pod-workflow", empty_pod_spec());
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.ensure_uid();
    pod.metadata.ensure_creation_timestamp();
    pod.metadata.finalizers = Some(vec![
        "finalizer.controller-a.io".to_string(),
        "finalizer.controller-b.io".to_string(),
    ]);

    let key = "test/pods/default/test-pod-workflow";
    storage.create(key, &pod).await.unwrap();

    // Step 2: First delete marks for deletion
    let marked = handle_delete_with_finalizers(&storage, key, &pod)
        .await
        .unwrap();
    assert!(marked);

    // Step 3: Controller A removes its finalizer
    let mut updated_pod: Pod = storage.get(key).await.unwrap();
    updated_pod.metadata.finalizers = Some(vec!["finalizer.controller-b.io".to_string()]);
    storage.update(key, &updated_pod).await.unwrap();

    // Step 4: Another delete still returns marked (still has finalizer)
    let marked = handle_delete_with_finalizers(&storage, key, &updated_pod)
        .await
        .unwrap();
    assert!(marked);

    // Step 5: Controller B removes its finalizer
    let mut updated_pod: Pod = storage.get(key).await.unwrap();
    updated_pod.metadata.finalizers = None;
    storage.update(key, &updated_pod).await.unwrap();

    // Step 6: Final delete removes the resource
    let deleted = handle_delete_with_finalizers(&storage, key, &updated_pod)
        .await
        .unwrap();
    assert!(!deleted, "Resource should be deleted");

    // Verify it's gone
    let result = storage.get::<Pod>(key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_has_metadata_trait_implementations() {
    // Test that HasMetadata trait is implemented for common types
    let pod = Pod::new("test-pod", empty_pod_spec());
    assert_eq!(pod.metadata().name, "test-pod");

    let mut pod_mut = pod.clone();
    pod_mut.metadata_mut().name = "modified-pod".to_string();
    assert_eq!(pod_mut.metadata().name, "modified-pod");

    // Test with other resource types
    use rusternetes_common::resources::{Deployment, DeploymentSpec, Namespace, PodTemplateSpec};
    use rusternetes_common::types::LabelSelector;
    use std::collections::HashMap;

    let deployment = Deployment::new(
        "test-deployment",
        DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: None,
                spec: empty_pod_spec(),
            },
            strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
            paused: None,
            progress_deadline_seconds: None,
        },
    );
    assert_eq!(deployment.metadata().name, "test-deployment");

    let namespace = Namespace::new("test-namespace");
    assert_eq!(namespace.metadata().name, "test-namespace");
}

#[tokio::test]
async fn test_delete_with_finalizer_race_condition() {
    let storage = Arc::new(MemoryStorage::new());

    // Create pod with finalizer
    let mut pod = Pod::new("test-pod-race", empty_pod_spec());
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.ensure_uid();
    pod.metadata.ensure_creation_timestamp();
    pod.metadata.finalizers = Some(vec!["test.finalizer.io".to_string()]);

    let key = "test/pods/default/test-pod-race";
    storage.create(key, &pod).await.unwrap();

    // Simulate race condition: multiple delete calls
    let result1 = handle_delete_with_finalizers(&storage, key, &pod).await;
    let updated_pod: Pod = storage.get(key).await.unwrap();
    let result2 = handle_delete_with_finalizers(&storage, key, &updated_pod).await;

    // Both should succeed
    assert!(result1.is_ok());
    assert!(result2.is_ok());

    // Both should return marked=true
    assert!(result1.unwrap());
    assert!(result2.unwrap());

    // Resource should still exist
    let final_pod: Pod = storage.get(key).await.unwrap();
    assert!(final_pod.metadata.deletion_timestamp.is_some());

    // Clean up
    storage.delete(key).await.unwrap();
}

#[tokio::test]
async fn test_delete_without_finalizers_multiple_times() {
    let storage = Arc::new(MemoryStorage::new());

    // Create pod without finalizers
    let mut pod = Pod::new("test-pod-no-finalizer", empty_pod_spec());
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.ensure_uid();
    pod.metadata.ensure_creation_timestamp();

    let key = "test/pods/default/test-pod-no-finalizer";
    storage.create(key, &pod).await.unwrap();

    // First delete should succeed and remove resource
    let deleted = handle_delete_with_finalizers(&storage, key, &pod)
        .await
        .unwrap();
    assert!(!deleted);

    // Resource should be gone
    let result = storage.get::<Pod>(key).await;
    assert!(result.is_err());

    // Second delete should fail (resource not found)
    let result2 = handle_delete_with_finalizers(&storage, key, &pod).await;
    assert!(result2.is_err());
}
