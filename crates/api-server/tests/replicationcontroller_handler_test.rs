//! Integration tests for ReplicationController handler
//!
//! Tests all CRUD operations, edge cases, and error handling for replicationcontrollers
//! ReplicationController is a legacy resource superseded by ReplicaSet and Deployment,
//! but still supported for backwards compatibility.

use rusternetes_api_server::handlers::filtering;
use rusternetes_common::resources::{
    Container, PodSpec, PodTemplateSpec, ReplicationController, ReplicationControllerSpec,
    ReplicationControllerStatus,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test replicationcontroller
fn create_test_rc(name: &str, namespace: &str, replicas: i32) -> ReplicationController {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    ReplicationController {
        type_meta: TypeMeta {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: ReplicationControllerSpec {
            replicas: Some(replicas),
            selector: Some(labels.clone()),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    name: format!("{}-pod", name),
                    namespace: Some(namespace.to_string()),
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: PodSpec {
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
                        image_pull_policy: None,
                        liveness_probe: None,
                        readiness_probe: None,
                        startup_probe: None,
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
                    ..Default::default()
                },
            },
            min_ready_seconds: Some(0),
        },
        status: Some(ReplicationControllerStatus {
            replicas: 0,
            fully_labeled_replicas: Some(0),
            ready_replicas: Some(0),
            available_replicas: Some(0),
            observed_generation: Some(0),
            conditions: None,
        }),
    }
}

#[tokio::test]
async fn test_rc_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let rc = create_test_rc("test-rc", "default", 3);
    let key = build_key("replicationcontrollers", Some("default"), "test-rc");

    // Create
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    assert_eq!(created.metadata.name, "test-rc");
    assert_eq!(created.metadata.namespace, Some("default".to_string()));
    assert_eq!(created.spec.replicas, Some(3));
    assert!(!created.metadata.uid.is_empty());

    // Get
    let retrieved: ReplicationController = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-rc");
    assert_eq!(retrieved.spec.replicas, Some(3));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-update-rc", "default", 3);
    let key = build_key("replicationcontrollers", Some("default"), "test-update-rc");

    // Create
    storage.create(&key, &rc).await.unwrap();

    // Update replicas
    rc.spec.replicas = Some(5);
    let updated: ReplicationController = storage.update(&key, &rc).await.unwrap();
    assert_eq!(updated.spec.replicas, Some(5));

    // Verify update
    let retrieved: ReplicationController = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.replicas, Some(5));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let rc = create_test_rc("test-delete-rc", "default", 3);
    let key = build_key("replicationcontrollers", Some("default"), "test-delete-rc");

    // Create
    storage.create(&key, &rc).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<ReplicationController>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_rc_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple replicationcontrollers
    let rc1 = create_test_rc("rc-1", "default", 3);
    let rc2 = create_test_rc("rc-2", "default", 5);
    let rc3 = create_test_rc("rc-3", "default", 2);

    let key1 = build_key("replicationcontrollers", Some("default"), "rc-1");
    let key2 = build_key("replicationcontrollers", Some("default"), "rc-2");
    let key3 = build_key("replicationcontrollers", Some("default"), "rc-3");

    storage.create(&key1, &rc1).await.unwrap();
    storage.create(&key2, &rc2).await.unwrap();
    storage.create(&key3, &rc3).await.unwrap();

    // List
    let prefix = build_prefix("replicationcontrollers", Some("default"));
    let rcs: Vec<ReplicationController> = storage.list(&prefix).await.unwrap();

    assert!(rcs.len() >= 3);
    let names: Vec<String> = rcs.iter().map(|rc| rc.metadata.name.clone()).collect();
    assert!(names.contains(&"rc-1".to_string()));
    assert!(names.contains(&"rc-2".to_string()));
    assert!(names.contains(&"rc-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_rc_list_across_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create replicationcontrollers in different namespaces
    let rc1 = create_test_rc("rc-ns1", "namespace-1", 3);
    let rc2 = create_test_rc("rc-ns2", "namespace-2", 5);

    let key1 = build_key("replicationcontrollers", Some("namespace-1"), "rc-ns1");
    let key2 = build_key("replicationcontrollers", Some("namespace-2"), "rc-ns2");

    storage.create(&key1, &rc1).await.unwrap();
    storage.create(&key2, &rc2).await.unwrap();

    // List all (no namespace filter)
    let prefix = build_prefix("replicationcontrollers", None);
    let rcs: Vec<ReplicationController> = storage.list(&prefix).await.unwrap();

    // Should find at least our 2 replicationcontrollers
    assert!(rcs.len() >= 2);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_rc_with_status() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-status", "default", 3);
    rc.status = Some(ReplicationControllerStatus {
        replicas: 3,
        fully_labeled_replicas: Some(3),
        ready_replicas: Some(2),
        available_replicas: Some(2),
        observed_generation: Some(1),
        conditions: None,
    });

    let key = build_key("replicationcontrollers", Some("default"), "test-status");

    // Create with status
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    assert_eq!(created.status.as_ref().unwrap().replicas, 3);
    assert_eq!(created.status.as_ref().unwrap().ready_replicas, Some(2));
    assert_eq!(created.status.as_ref().unwrap().available_replicas, Some(2));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-finalizers", "default", 3);
    rc.metadata.finalizers = Some(vec!["finalizer.test.io".to_string()]);

    let key = build_key("replicationcontrollers", Some("default"), "test-finalizers");

    // Create with finalizer
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: ReplicationController = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Clean up - remove finalizer first
    rc.metadata.finalizers = None;
    storage.update(&key, &rc).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let rc = create_test_rc("test-immutable", "default", 3);
    let key = build_key("replicationcontrollers", Some("default"), "test-immutable");

    // Create
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_rc = created.clone();
    updated_rc.spec.replicas = Some(10);

    let updated: ReplicationController = storage.update(&key, &updated_rc).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(updated.spec.replicas, Some(10));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_label_selector() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "frontend".to_string());
    labels.insert("tier".to_string(), "web".to_string());

    let mut rc = create_test_rc("test-labels", "default", 3);
    rc.metadata.labels = Some(labels.clone());
    rc.spec.selector = Some(labels);

    let key = build_key("replicationcontrollers", Some("default"), "test-labels");

    // Create with labels
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(created_labels.get("app"), Some(&"frontend".to_string()));
    assert_eq!(created_labels.get("tier"), Some(&"web".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("replicationcontrollers", Some("default"), "nonexistent");
    let result = storage.get::<ReplicationController>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_rc_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let rc = create_test_rc("nonexistent", "default", 3);
    let key = build_key("replicationcontrollers", Some("default"), "nonexistent");

    let result = storage.update(&key, &rc).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_rc_min_ready_seconds() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-min-ready", "default", 3);
    rc.spec.min_ready_seconds = Some(30);

    let key = build_key("replicationcontrollers", Some("default"), "test-min-ready");

    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    assert_eq!(created.spec.min_ready_seconds, Some(30));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_zero_replicas() {
    let storage = Arc::new(MemoryStorage::new());

    let rc = create_test_rc("test-zero", "default", 0);
    let key = build_key("replicationcontrollers", Some("default"), "test-zero");

    // Create with zero replicas
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    assert_eq!(created.spec.replicas, Some(0));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_with_owner_reference() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-owner", "default", 3);
    rc.metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
        api_version: "v1".to_string(),
        kind: "Service".to_string(),
        name: "parent-service".to_string(),
        uid: "parent-uid-123".to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }]);

    let key = build_key("replicationcontrollers", Some("default"), "test-owner");

    // Create with owner reference
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    assert!(created.metadata.owner_references.is_some());
    let owner_refs = created.metadata.owner_references.unwrap();
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0].kind, "Service");
    assert_eq!(owner_refs[0].name, "parent-service");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_observed_generation() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-generation", "default", 3);
    rc.status = Some(ReplicationControllerStatus {
        replicas: 3,
        fully_labeled_replicas: Some(3),
        ready_replicas: Some(3),
        available_replicas: Some(3),
        observed_generation: Some(5),
        conditions: None,
    });

    let key = build_key("replicationcontrollers", Some("default"), "test-generation");

    // Create with observed generation
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    assert_eq!(
        created.status.as_ref().unwrap().observed_generation,
        Some(5)
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_default_replicas() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-default", "default", 1);
    // Set replicas to None to test default behavior
    rc.spec.replicas = None;

    let key = build_key("replicationcontrollers", Some("default"), "test-default");

    // Create with no explicit replicas
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    // ReplicationController should handle None replicas gracefully
    assert!(created.spec.replicas.is_none());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_template_change() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-template", "default", 3);
    let key = build_key("replicationcontrollers", Some("default"), "test-template");

    // Create original
    storage.create(&key, &rc).await.unwrap();

    // Change pod template image
    rc.spec.template.spec.containers[0].image = "nginx:1.19".to_string();

    // Update with new template
    let updated: ReplicationController = storage.update(&key, &rc).await.unwrap();
    assert_eq!(updated.spec.template.spec.containers[0].image, "nginx:1.19");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_selector_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels1 = HashMap::new();
    labels1.insert("app".to_string(), "original".to_string());

    let mut rc = create_test_rc("test-selector", "default", 3);
    rc.spec.selector = Some(labels1);

    let key = build_key("replicationcontrollers", Some("default"), "test-selector");

    // Create
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    let original_selector = created.spec.selector.clone();

    // Try to change selector
    let mut labels2 = HashMap::new();
    labels2.insert("app".to_string(), "modified".to_string());

    let mut updated_rc = created.clone();
    updated_rc.spec.selector = Some(labels2);

    // Update - selector change should be stored
    // (Note: In real Kubernetes, selector is immutable, but storage layer doesn't enforce this.
    // This is validated at the API handler level with admission webhooks)
    let updated: ReplicationController = storage.update(&key, &updated_rc).await.unwrap();

    // Storage allows the change (handler would reject it)
    assert_ne!(updated.spec.selector, original_selector);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_multiple_containers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut rc = create_test_rc("test-multi-container", "default", 2);

    // Add a sidecar container
    rc.spec.template.spec.containers.push(Container {
        name: "sidecar".to_string(),
        image: "busybox:latest".to_string(),
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "sleep 3600".to_string(),
        ]),
        args: None,
        working_dir: None,
        ports: None,
        env: None,
        resources: None,
        volume_mounts: None,
        image_pull_policy: None,
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
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

    let key = build_key(
        "replicationcontrollers",
        Some("default"),
        "test-multi-container",
    );

    // Create with multiple containers
    let created: ReplicationController = storage.create(&key, &rc).await.unwrap();
    let containers = &created.spec.template.spec.containers;
    assert_eq!(containers.len(), 2);
    assert_eq!(containers[0].name, "nginx");
    assert_eq!(containers[1].name, "sidecar");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rc_list_with_label_selector_filtering() {
    let storage = Arc::new(MemoryStorage::new());

    // Create two RCs with different labels
    let mut rc1 = create_test_rc("rc-frontend", "default", 3);
    let mut labels1 = HashMap::new();
    labels1.insert("app".to_string(), "frontend".to_string());
    labels1.insert("tier".to_string(), "web".to_string());
    rc1.metadata.labels = Some(labels1);

    let mut rc2 = create_test_rc("rc-backend", "default", 2);
    let mut labels2 = HashMap::new();
    labels2.insert("app".to_string(), "backend".to_string());
    labels2.insert("tier".to_string(), "api".to_string());
    rc2.metadata.labels = Some(labels2);

    let key1 = build_key("replicationcontrollers", Some("default"), "rc-frontend");
    let key2 = build_key("replicationcontrollers", Some("default"), "rc-backend");

    storage.create(&key1, &rc1).await.unwrap();
    storage.create(&key2, &rc2).await.unwrap();

    // List all and filter by labelSelector=app=frontend
    let prefix = build_prefix("replicationcontrollers", Some("default"));
    let mut rcs: Vec<ReplicationController> = storage.list(&prefix).await.unwrap();

    let mut params = HashMap::new();
    params.insert("labelSelector".to_string(), "app=frontend".to_string());
    filtering::apply_selectors(&mut rcs, &params).unwrap();

    // Should only contain the frontend RC
    assert_eq!(rcs.len(), 1, "Expected exactly 1 RC matching app=frontend");
    assert_eq!(rcs[0].metadata.name, "rc-frontend");

    // Now filter by app=backend
    let mut rcs2: Vec<ReplicationController> = storage.list(&prefix).await.unwrap();
    let mut params2 = HashMap::new();
    params2.insert("labelSelector".to_string(), "app=backend".to_string());
    filtering::apply_selectors(&mut rcs2, &params2).unwrap();

    assert_eq!(rcs2.len(), 1, "Expected exactly 1 RC matching app=backend");
    assert_eq!(rcs2[0].metadata.name, "rc-backend");

    // Filter by non-existent label should return empty
    let mut rcs3: Vec<ReplicationController> = storage.list(&prefix).await.unwrap();
    let mut params3 = HashMap::new();
    params3.insert("labelSelector".to_string(), "app=nonexistent".to_string());
    filtering::apply_selectors(&mut rcs3, &params3).unwrap();

    assert_eq!(rcs3.len(), 0, "Expected 0 RCs matching app=nonexistent");

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}
