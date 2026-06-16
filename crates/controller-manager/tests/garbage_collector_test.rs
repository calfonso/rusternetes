// Integration tests for Garbage Collector and Finalizers
// Tests cascade deletion, orphaning, and owner reference handling

use rusternetes_common::deletion::{process_deletion, DeleteOptions, DeletionResult};
use rusternetes_common::resources::pod::*;
use rusternetes_common::types::{ObjectMeta, OwnerReference, Phase, TypeMeta};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::Value;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn create_test_pod(name: &str, namespace: &str, owner_uid: Option<&str>) -> Pod {
    let mut metadata = ObjectMeta::new(name);
    metadata.namespace = Some(namespace.to_string());
    metadata.uid = uuid::Uuid::new_v4().to_string();

    if let Some(uid) = owner_uid {
        metadata.owner_references = Some(vec![OwnerReference::new(
            "apps/v1",
            "Deployment",
            "test-deployment",
            uid,
        )
        .with_controller(true)
        .with_block_owner_deletion(true)]);
    }

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata,
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:1.25-alpine".to_string(),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ports: Some(vec![]),
                env: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                resources: None,
                working_dir: None,
                command: None,
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
            restart_policy: Some("Always".to_string()),
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
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        }),
    }
}

#[tokio::test]
async fn test_finalizer_blocks_deletion() {
    let storage = setup_test().await;

    // Create a pod with a finalizer
    let mut pod = create_test_pod("test-pod", "default", None);
    pod.metadata.add_finalizer("test-finalizer".to_string());

    let pod_key = build_key("pods", Some("default"), "test-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    // Convert to Value for deletion processing
    let mut pod_value: Value = serde_json::to_value(&pod).unwrap();

    // Process deletion
    let options = DeleteOptions::default();
    let result = process_deletion(&mut pod_value, &options).unwrap();

    // Should be marked for deletion, not deleted
    assert_eq!(result, DeletionResult::MarkedForDeletion);

    // Verify deletion timestamp is set
    assert!(pod_value["metadata"]["deletionTimestamp"].is_string());

    // Verify finalizer is still present
    let finalizers = pod_value["metadata"]["finalizers"].as_array().unwrap();
    assert_eq!(finalizers.len(), 1);
}

#[tokio::test]
async fn test_finalizer_removal_allows_deletion() {
    let _storage = setup_test().await;

    // Create a pod with a finalizer
    let mut pod = create_test_pod("test-pod", "default", None);
    pod.metadata.add_finalizer("test-finalizer".to_string());

    // Convert to Value for deletion processing
    let mut pod_value: Value = serde_json::to_value(&pod).unwrap();

    // Process deletion (should mark for deletion)
    let options = DeleteOptions::default();
    let result = process_deletion(&mut pod_value, &options).unwrap();
    assert_eq!(result, DeletionResult::MarkedForDeletion);

    // Now remove the finalizer
    pod.metadata.remove_finalizer("test-finalizer");
    pod_value = serde_json::to_value(&pod).unwrap();

    // Set deletion timestamp (simulating marked for deletion)
    let deletion_ts = chrono::Utc::now();
    pod_value["metadata"]["deletionTimestamp"] = serde_json::to_value(deletion_ts).unwrap();

    // Process deletion again
    let result = process_deletion(&mut pod_value, &options).unwrap();

    // Should now be deleted
    assert_eq!(result, DeletionResult::Deleted);
}

#[tokio::test]
async fn test_owner_reference_tracking() {
    let storage = setup_test().await;

    let deployment_uid = uuid::Uuid::new_v4().to_string();

    // Create pods with owner references
    for i in 0..3 {
        let pod = create_test_pod(&format!("pod-{}", i), "default", Some(&deployment_uid));

        // Verify owner reference is set correctly
        assert!(pod.metadata.owner_references.is_some());
        let owner_refs = pod.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].uid, deployment_uid);
        assert_eq!(owner_refs[0].kind, "Deployment");
        assert_eq!(owner_refs[0].controller, Some(true));

        let pod_key = build_key("pods", Some("default"), &format!("pod-{}", i));
        storage.create(&pod_key, &pod).await.unwrap();
    }

    // Verify all pods were created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3);
}

#[tokio::test]
async fn test_multiple_owner_references() {
    let storage = setup_test().await;

    let deployment1_uid = uuid::Uuid::new_v4().to_string();
    let deployment2_uid = uuid::Uuid::new_v4().to_string();

    // Create a pod with multiple owner references
    let mut pod = create_test_pod("multi-owner-pod", "default", Some(&deployment1_uid));

    // Add second owner reference
    if let Some(ref mut owner_refs) = pod.metadata.owner_references {
        owner_refs.push(
            OwnerReference::new(
                "apps/v1",
                "Deployment",
                "test-deployment-2",
                &deployment2_uid,
            )
            .with_controller(false),
        );
    }

    let pod_key = build_key("pods", Some("default"), "multi-owner-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    // Verify pod has two owner references
    let stored_pod: Pod = storage.get(&pod_key).await.unwrap();
    assert_eq!(
        stored_pod.metadata.owner_references.as_ref().unwrap().len(),
        2
    );
}

#[tokio::test]
async fn test_block_owner_deletion() {
    let storage = setup_test().await;

    let deployment_uid = uuid::Uuid::new_v4().to_string();

    // Create a pod with blockOwnerDeletion=true
    let mut pod = create_test_pod("blocking-pod", "default", Some(&deployment_uid));
    if let Some(ref mut owner_refs) = pod.metadata.owner_references {
        owner_refs[0].block_owner_deletion = Some(true);
    }

    let pod_key = build_key("pods", Some("default"), "blocking-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    // Verify blockOwnerDeletion is set
    let stored_pod: Pod = storage.get(&pod_key).await.unwrap();
    assert_eq!(
        stored_pod.metadata.owner_references.as_ref().unwrap()[0].block_owner_deletion,
        Some(true)
    );
}

#[tokio::test]
async fn test_cascade_deletion_propagation_policies() {
    use rusternetes_common::types::DeletionPropagation;

    let _storage = setup_test().await;

    // Test Foreground deletion
    let pod = create_test_pod("test-pod-fg", "default", None);
    let mut pod_value: Value = serde_json::to_value(&pod).unwrap();

    let options = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Foreground),
        ..Default::default()
    };

    let result = process_deletion(&mut pod_value, &options).unwrap();
    assert_eq!(result, DeletionResult::MarkedForDeletion);

    // Should have foregroundDeletion finalizer
    let finalizers = pod_value["metadata"]["finalizers"].as_array().unwrap();
    assert!(finalizers.iter().any(|f| f == "foregroundDeletion"));

    // Test Orphan deletion
    let pod2 = create_test_pod("test-pod-orphan", "default", None);
    let mut pod2_value: Value = serde_json::to_value(&pod2).unwrap();

    let options2 = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Orphan),
        ..Default::default()
    };

    let result2 = process_deletion(&mut pod2_value, &options2).unwrap();
    assert_eq!(result2, DeletionResult::MarkedForDeletion);

    // Should have orphan finalizer
    let finalizers2 = pod2_value["metadata"]["finalizers"].as_array().unwrap();
    assert!(finalizers2.iter().any(|f| f == "orphan"));

    // Test Background deletion (default)
    let pod3 = create_test_pod("test-pod-bg", "default", None);
    let mut pod3_value: Value = serde_json::to_value(&pod3).unwrap();

    let options3 = DeleteOptions {
        propagation_policy: Some(DeletionPropagation::Background),
        ..Default::default()
    };

    let result3 = process_deletion(&mut pod3_value, &options3).unwrap();
    // Background deletion with no existing finalizers should delete immediately
    assert_eq!(result3, DeletionResult::Deleted);
}

#[tokio::test]
async fn test_precondition_checks() {
    let _storage = setup_test().await;

    let pod = create_test_pod("test-pod", "default", None);
    let pod_uid = pod.metadata.uid.clone();
    let mut pod_value: Value = serde_json::to_value(&pod).unwrap();

    // Test with wrong UID precondition
    let options = DeleteOptions {
        preconditions: Some(rusternetes_common::deletion::Preconditions {
            uid: Some("wrong-uid".to_string()),
            resource_version: None,
        }),
        ..Default::default()
    };

    let result = process_deletion(&mut pod_value, &options).unwrap();
    match result {
        DeletionResult::Deferred(msg) => {
            assert!(msg.contains("UID precondition failed"));
        }
        _ => panic!("Expected deferred deletion due to UID mismatch"),
    }

    // Test with correct UID precondition
    let options2 = DeleteOptions {
        preconditions: Some(rusternetes_common::deletion::Preconditions {
            uid: Some(pod_uid),
            resource_version: None,
        }),
        ..Default::default()
    };

    let result2 = process_deletion(&mut pod_value, &options2).unwrap();
    assert_eq!(result2, DeletionResult::Deleted);
}

#[tokio::test]
async fn test_metadata_finalizer_helpers() {
    let _storage = setup_test().await;

    let mut metadata = ObjectMeta::new("test");

    // Test add_finalizer
    metadata.add_finalizer("my-finalizer".to_string());
    assert!(metadata.has_finalizers());
    assert_eq!(metadata.finalizers.as_ref().unwrap().len(), 1);

    // Test idempotent add
    metadata.add_finalizer("my-finalizer".to_string());
    assert_eq!(metadata.finalizers.as_ref().unwrap().len(), 1);

    // Test remove_finalizer
    metadata.remove_finalizer("my-finalizer");
    assert!(!metadata.has_finalizers());
}

// Tests for actual GarbageCollector functionality
mod garbage_collector_integration {
    use super::*;
    use chrono::Utc;
    use rusternetes_controller_manager::controllers::garbage_collector::GarbageCollector;

    #[tokio::test]
    async fn test_gc_deletes_orphaned_pods() {
        let storage = setup_test().await;
        let gc = GarbageCollector::new(storage.clone());

        // Create pods with owner reference to non-existent parent
        let fake_owner_uid = uuid::Uuid::new_v4().to_string();

        for i in 0..3 {
            let pod = create_test_pod(
                &format!("orphan-pod-{}", i),
                "default",
                Some(&fake_owner_uid),
            );
            let pod_key = build_key("pods", Some("default"), &format!("orphan-pod-{}", i));
            storage.create(&pod_key, &pod).await.unwrap();
        }

        // Verify pods exist
        let pods_before: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods_before.len(), 3);

        // Run GC
        gc.scan_and_collect().await.unwrap();

        // Pods should be deleted (orphaned)
        let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods_after.len(), 0);
    }

    #[tokio::test]
    async fn test_gc_cascades_with_owner_references() {
        let storage = setup_test().await;
        let gc = GarbageCollector::new(storage.clone());

        // Create a parent pod
        let parent = create_test_pod("parent-pod", "default", None);
        let parent_uid = parent.metadata.uid.clone();
        let parent_key = build_key("pods", Some("default"), "parent-pod");
        storage.create(&parent_key, &parent).await.unwrap();

        // Create child pods with owner reference
        for i in 0..2 {
            let child = create_test_pod(&format!("child-pod-{}", i), "default", Some(&parent_uid));
            let child_key = build_key("pods", Some("default"), &format!("child-pod-{}", i));
            storage.create(&child_key, &child).await.unwrap();
        }

        // Verify all resources exist
        assert!(storage.get::<Pod>(&parent_key).await.is_ok());
        let children_before: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(children_before.len(), 3); // parent + 2 children

        // Delete the parent
        storage.delete(&parent_key).await.unwrap();

        // Run GC - child pods should become orphaned and deleted
        gc.scan_and_collect().await.unwrap();

        // Children should be deleted
        let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods_after.len(), 0);
    }

    #[tokio::test]
    async fn test_gc_orphan_propagation_removes_owner_references() {
        let storage = setup_test().await;
        let gc = GarbageCollector::new(storage.clone());

        // Create a "deployment" (represented as a pod for simplicity, since GC works on generic resources)
        let mut owner = create_test_pod("owner-deploy", "default", None);
        let owner_uid = owner.metadata.uid.clone();
        let owner_key = build_key("pods", Some("default"), "owner-deploy");

        // Mark owner for deletion with orphan finalizer (simulating Orphan propagation policy)
        owner.metadata.deletion_timestamp = Some(Utc::now());
        owner.metadata.add_finalizer("orphan".to_string());
        storage.create(&owner_key, &owner).await.unwrap();

        // Create child resources with owner references
        for i in 0..3 {
            let child = create_test_pod(&format!("child-rs-{}", i), "default", Some(&owner_uid));
            let child_key = build_key("pods", Some("default"), &format!("child-rs-{}", i));
            storage.create(&child_key, &child).await.unwrap();
        }

        // Verify children have owner references
        for i in 0..3 {
            let child_key = build_key("pods", Some("default"), &format!("child-rs-{}", i));
            let child: Pod = storage.get(&child_key).await.unwrap();
            assert!(
                child.metadata.owner_references.is_some(),
                "Child should have owner references before GC"
            );
            let refs = child.metadata.owner_references.as_ref().unwrap();
            assert_eq!(refs.len(), 1);
            assert_eq!(refs[0].uid, owner_uid);
        }

        // Run GC — should process orphan propagation
        gc.scan_and_collect().await.unwrap();

        // Children should still exist (not deleted)
        for i in 0..3 {
            let child_key = build_key("pods", Some("default"), &format!("child-rs-{}", i));
            let child: Pod = storage.get(&child_key).await.unwrap();

            // Owner references should be removed (orphaned)
            let has_owner_ref = child
                .metadata
                .owner_references
                .as_ref()
                .is_some_and(|refs| !refs.is_empty());
            assert!(
                !has_owner_ref,
                "Child {} should have ownerReferences removed after orphan propagation, but still has {:?}",
                i,
                child.metadata.owner_references
            );
        }

        // Owner should be deleted (orphan finalizer removed, then deleted)
        assert!(
            storage.get::<Pod>(&owner_key).await.is_err(),
            "Owner should be deleted after orphan finalizer is processed"
        );
    }

    #[tokio::test]
    async fn test_gc_respects_finalizers() {
        let storage = setup_test().await;
        let gc = GarbageCollector::new(storage.clone());

        // Create pod with finalizer
        let mut pod = create_test_pod("finalized-pod", "default", None);
        pod.metadata.deletion_timestamp = Some(Utc::now());
        pod.metadata.add_finalizer("test-finalizer".to_string());

        let pod_key = build_key("pods", Some("default"), "finalized-pod");
        storage.create(&pod_key, &pod).await.unwrap();

        // Run GC
        gc.scan_and_collect().await.unwrap();

        // Pod should NOT be deleted (has finalizer)
        assert!(storage.get::<Pod>(&pod_key).await.is_ok());

        // Remove finalizer
        let mut pod = storage.get::<Pod>(&pod_key).await.unwrap();
        pod.metadata.remove_finalizer("test-finalizer");
        storage.update(&pod_key, &pod).await.unwrap();

        // Run GC again
        gc.scan_and_collect().await.unwrap();

        // Now pod should be deleted
        assert!(storage.get::<Pod>(&pod_key).await.is_err());
    }
}
