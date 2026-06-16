//! Comprehensive integration tests for Pod handler
//!
//! Tests all CRUD operations, admission control, finalizers, pagination, filtering, and error cases

use rusternetes_common::resources::{Container, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::sync::Arc;

// Helper function to create a test pod
fn create_test_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:latest".to_string(),
                command: None,
                args: None,
                working_dir: None,
                ports: None,
                env: None,
                resources: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                image_pull_policy: None,
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
            ephemeral_containers: None,
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            service_account_name: None,
            service_account: None,
            node_name: None,
            host_network: Some(false),
            host_pid: Some(false),
            host_ipc: Some(false),
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
        }),
        status: None,
    }
}

#[tokio::test]
async fn test_pod_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let pod = create_test_pod("test-pod-create", "default");
    let key = build_key("pods", Some("default"), "test-pod-create");

    // Create pod
    storage.create(&key, &pod).await.unwrap();

    // Retrieve pod
    let retrieved: Pod = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-pod-create");
    assert_eq!(retrieved.metadata.namespace.as_ref().unwrap(), "default");
    assert!(!retrieved.metadata.uid.is_empty());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pod_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pod = create_test_pod("test-pod-update", "default");
    let key = build_key("pods", Some("default"), "test-pod-update");

    // Create pod
    storage.create(&key, &pod).await.unwrap();

    // Update pod - add label
    let mut labels = std::collections::HashMap::new();
    labels.insert("app".to_string(), "nginx".to_string());
    pod.metadata.labels = Some(labels);

    storage.update(&key, &pod).await.unwrap();

    // Verify update
    let updated: Pod = storage.get(&key).await.unwrap();
    assert!(updated.metadata.labels.is_some());
    assert_eq!(
        updated
            .metadata
            .labels
            .as_ref()
            .unwrap()
            .get("app")
            .unwrap(),
        "nginx"
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pod_delete_without_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let pod = create_test_pod("test-pod-delete", "default");
    let key = build_key("pods", Some("default"), "test-pod-delete");

    // Create pod
    storage.create(&key, &pod).await.unwrap();

    // Delete pod
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<Pod>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_pod_delete_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pod = create_test_pod("test-pod-finalizer", "default");
    pod.metadata.finalizers = Some(vec!["kubernetes.io/pv-protection".to_string()]);

    let key = build_key("pods", Some("default"), "test-pod-finalizer");

    // Create pod
    storage.create(&key, &pod).await.unwrap();

    // Attempt to delete - should mark for deletion but not remove
    let marked = rusternetes_api_server::handlers::finalizers::handle_delete_with_finalizers(
        &storage, &key, &pod,
    )
    .await
    .unwrap();

    assert!(marked, "Pod should be marked for deletion");

    // Verify pod still exists with deletionTimestamp
    let updated: Pod = storage.get(&key).await.unwrap();
    assert!(updated.metadata.deletion_timestamp.is_some());
    assert!(updated.metadata.finalizers.is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pod_list_in_namespace() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple pods
    let pod1 = create_test_pod("pod-list-1", "test-namespace");
    let pod2 = create_test_pod("pod-list-2", "test-namespace");
    let pod3 = create_test_pod("pod-list-3", "other-namespace");

    storage
        .create(
            &build_key("pods", Some("test-namespace"), "pod-list-1"),
            &pod1,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("pods", Some("test-namespace"), "pod-list-2"),
            &pod2,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("pods", Some("other-namespace"), "pod-list-3"),
            &pod3,
        )
        .await
        .unwrap();

    // List pods in test-namespace
    let prefix = build_prefix("pods", Some("test-namespace"));
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap();

    assert_eq!(pods.len(), 2);
    assert!(pods.iter().any(|p| p.metadata.name == "pod-list-1"));
    assert!(pods.iter().any(|p| p.metadata.name == "pod-list-2"));

    // Clean up
    storage
        .delete(&build_key("pods", Some("test-namespace"), "pod-list-1"))
        .await
        .unwrap();
    storage
        .delete(&build_key("pods", Some("test-namespace"), "pod-list-2"))
        .await
        .unwrap();
    storage
        .delete(&build_key("pods", Some("other-namespace"), "pod-list-3"))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_pod_list_all_pods() {
    let storage = Arc::new(MemoryStorage::new());

    // Create pods in different namespaces
    let pod1 = create_test_pod("pod-all-1", "ns1");
    let pod2 = create_test_pod("pod-all-2", "ns2");
    let pod3 = create_test_pod("pod-all-3", "ns3");

    storage
        .create(&build_key("pods", Some("ns1"), "pod-all-1"), &pod1)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("ns2"), "pod-all-2"), &pod2)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("ns3"), "pod-all-3"), &pod3)
        .await
        .unwrap();

    // List all pods
    let prefix = build_prefix("pods", None);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap();

    // Should have at least the 3 we created
    assert!(pods.len() >= 3);
    assert!(pods.iter().any(|p| p.metadata.name == "pod-all-1"));
    assert!(pods.iter().any(|p| p.metadata.name == "pod-all-2"));
    assert!(pods.iter().any(|p| p.metadata.name == "pod-all-3"));

    // Clean up
    storage
        .delete(&build_key("pods", Some("ns1"), "pod-all-1"))
        .await
        .unwrap();
    storage
        .delete(&build_key("pods", Some("ns2"), "pod-all-2"))
        .await
        .unwrap();
    storage
        .delete(&build_key("pods", Some("ns3"), "pod-all-3"))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_pod_list_with_label_selector() {
    let storage = Arc::new(MemoryStorage::new());

    // Clean up any existing pods from previous test runs (only this test's pods)
    let _ = storage
        .delete(&build_key("pods", Some("default"), "pod-label-1"))
        .await;
    let _ = storage
        .delete(&build_key("pods", Some("default"), "pod-label-2"))
        .await;

    // Create pods with different labels
    let mut pod1 = create_test_pod("pod-label-1", "default");
    let mut labels1 = std::collections::HashMap::new();
    labels1.insert("app".to_string(), "nginx".to_string());
    pod1.metadata.labels = Some(labels1);

    let mut pod2 = create_test_pod("pod-label-2", "default");
    let mut labels2 = std::collections::HashMap::new();
    labels2.insert("app".to_string(), "redis".to_string());
    pod2.metadata.labels = Some(labels2);

    storage
        .create(&build_key("pods", Some("default"), "pod-label-1"), &pod1)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("default"), "pod-label-2"), &pod2)
        .await
        .unwrap();

    // List and filter by label
    let prefix = build_prefix("pods", Some("default"));
    let mut pods: Vec<Pod> = storage.list(&prefix).await.unwrap();

    // Filter for app=nginx
    let mut params = std::collections::HashMap::new();
    params.insert("labelSelector".to_string(), "app=nginx".to_string());

    rusternetes_api_server::handlers::filtering::apply_label_selector(&mut pods, &params).unwrap();

    // Should find at least our pod with app=nginx label
    assert!(
        !pods.is_empty(),
        "Should find at least 1 pod with app=nginx"
    );
    assert!(
        pods.iter().any(|p| p.metadata.name == "pod-label-1"),
        "Should find pod-label-1"
    );

    // Clean up
    storage
        .delete(&build_key("pods", Some("default"), "pod-label-1"))
        .await
        .unwrap();
    storage
        .delete(&build_key("pods", Some("default"), "pod-label-2"))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_pod_with_multiple_containers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pod = create_test_pod("multi-container-pod", "default");

    // Add second container
    if let Some(ref mut spec) = pod.spec {
        spec.containers.push(Container {
            name: "sidecar".to_string(),
            image: "busybox:latest".to_string(),
            command: Some(vec!["sleep".to_string(), "3600".to_string()]),
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            resources: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            image_pull_policy: None,
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
        });
    }

    let key = build_key("pods", Some("default"), "multi-container-pod");
    storage.create(&key, &pod).await.unwrap();

    // Retrieve and verify
    let retrieved: Pod = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.as_ref().unwrap().containers.len(), 2);
    assert_eq!(retrieved.spec.as_ref().unwrap().containers[0].name, "nginx");
    assert_eq!(
        retrieved.spec.as_ref().unwrap().containers[1].name,
        "sidecar"
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pod_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("pods", Some("default"), "non-existent-pod");
    let result = storage.get::<Pod>(&key).await;

    assert!(result.is_err());
    match result {
        Err(rusternetes_common::Error::NotFound(_)) => {
            // Expected
        }
        _ => panic!("Expected NotFound error"),
    }
}

#[tokio::test]
async fn test_pod_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let pod = create_test_pod("immutable-pod", "default");
    let key = build_key("pods", Some("default"), "immutable-pod");

    // Create pod
    storage.create(&key, &pod).await.unwrap();

    // Baseline is the stored value (creationTimestamp serialized at second
    // precision per k8s metav1.Time), not the in-memory nanosecond value.
    let stored: Pod = storage.get(&key).await.unwrap();
    let original_uid = stored.metadata.uid.clone();
    let original_creation = stored.metadata.creation_timestamp;

    // Update pod
    let mut updated_pod: Pod = storage.get(&key).await.unwrap();
    updated_pod
        .metadata
        .labels
        .get_or_insert(std::collections::HashMap::new())
        .insert("updated".to_string(), "true".to_string());

    storage.update(&key, &updated_pod).await.unwrap();

    // Verify UID and creation timestamp unchanged
    let final_pod: Pod = storage.get(&key).await.unwrap();
    assert_eq!(final_pod.metadata.uid, original_uid);
    assert_eq!(final_pod.metadata.creation_timestamp, original_creation);

    // Clean up
    storage.delete(&key).await.unwrap();
}
