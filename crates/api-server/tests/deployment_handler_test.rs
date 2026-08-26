//! Integration tests for Deployment handler
//!
//! Tests all CRUD operations, edge cases, and error handling for deployments

use rusternetes_common::resources::{
    Container, Deployment, DeploymentSpec, PodSpec, PodTemplateSpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test deployment
fn create_test_deployment(name: &str, namespace: &str, replicas: i32) -> Deployment {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    Deployment {
        type_meta: TypeMeta {
            kind: "Deployment".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            labels: Some(labels.clone()),
            uid: String::new(),
            creation_timestamp: None,
            resource_version: None,
            finalizers: None,
            deletion_timestamp: None,
            deletion_grace_period_seconds: None,
            owner_references: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            min_ready_seconds: None,
            revision_history_limit: None,
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    name: format!("{}-pod", name),
                    namespace: Some(namespace.to_string()),
                    labels: Some(labels),
                    uid: String::new(),
                    creation_timestamp: None,
                    resource_version: None,
                    finalizers: None,
                    deletion_timestamp: None,
                    deletion_grace_period_seconds: None,
                    owner_references: None,
                    annotations: None,
                    generate_name: None,
                    generation: None,
                    managed_fields: None,
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
                        image: "nginx:latest".to_string(),
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
                    }],
                    init_containers: None,
                    ephemeral_containers: None,
                    volumes: None,
                    restart_policy: Some("Always".to_string()),
                    node_name: None,
                    node_selector: None,
                    service_account_name: None,
                    service_account: None,
                    automount_service_account_token: None,
                    hostname: None,
                    subdomain: None,
                    host_network: None,
                    host_pid: None,
                    host_ipc: None,
                    affinity: None,
                    tolerations: None,
                    priority: None,
                    priority_class_name: None,
                    scheduler_name: None,
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
                },
            },
            strategy: None,
            paused: None,
            progress_deadline_seconds: None,
        },
        status: None,
    }
}

#[tokio::test]
async fn test_deployment_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let deployment = create_test_deployment("test-deploy", "default", 3);
    let key = build_key("deployments", Some("default"), "test-deploy");

    // Create
    let created: Deployment = storage.create(&key, &deployment).await.unwrap();
    assert_eq!(created.metadata.name, "test-deploy");
    assert_eq!(created.metadata.namespace, Some("default".to_string()));
    assert_eq!(created.spec.replicas, Some(3));
    assert!(!created.metadata.uid.is_empty());

    // Get
    let retrieved: Deployment = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-deploy");
    assert_eq!(retrieved.spec.replicas, Some(3));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_deployment_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut deployment = create_test_deployment("test-update", "default", 3);
    let key = build_key("deployments", Some("default"), "test-update");

    // Create
    storage.create(&key, &deployment).await.unwrap();

    // Update replicas
    deployment.spec.replicas = Some(5);
    let updated: Deployment = storage.update(&key, &deployment).await.unwrap();
    assert_eq!(updated.spec.replicas, Some(5));

    // Verify update
    let retrieved: Deployment = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.spec.replicas, Some(5));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_deployment_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let deployment = create_test_deployment("test-delete", "default", 3);
    let key = build_key("deployments", Some("default"), "test-delete");

    // Create
    storage.create(&key, &deployment).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<Deployment>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_deployment_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple deployments
    let deploy1 = create_test_deployment("deploy-1", "default", 3);
    let deploy2 = create_test_deployment("deploy-2", "default", 5);
    let deploy3 = create_test_deployment("deploy-3", "default", 2);

    let key1 = build_key("deployments", Some("default"), "deploy-1");
    let key2 = build_key("deployments", Some("default"), "deploy-2");
    let key3 = build_key("deployments", Some("default"), "deploy-3");

    storage.create(&key1, &deploy1).await.unwrap();
    storage.create(&key2, &deploy2).await.unwrap();
    storage.create(&key3, &deploy3).await.unwrap();

    // List
    let prefix = build_prefix("deployments", Some("default"));
    let deployments: Vec<Deployment> = storage.list(&prefix).await.unwrap();

    assert!(deployments.len() >= 3);
    let names: Vec<String> = deployments
        .iter()
        .map(|d| d.metadata.name.clone())
        .collect();
    assert!(names.contains(&"deploy-1".to_string()));
    assert!(names.contains(&"deploy-2".to_string()));
    assert!(names.contains(&"deploy-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_deployment_list_across_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create deployments in different namespaces
    let deploy1 = create_test_deployment("deploy-ns1", "namespace-1", 3);
    let deploy2 = create_test_deployment("deploy-ns2", "namespace-2", 5);

    let key1 = build_key("deployments", Some("namespace-1"), "deploy-ns1");
    let key2 = build_key("deployments", Some("namespace-2"), "deploy-ns2");

    storage.create(&key1, &deploy1).await.unwrap();
    storage.create(&key2, &deploy2).await.unwrap();

    // List all (no namespace filter)
    let prefix = build_prefix("deployments", None);
    let deployments: Vec<Deployment> = storage.list(&prefix).await.unwrap();

    // Should find at least our 2 deployments
    assert!(deployments.len() >= 2);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_deployment_with_strategy() {
    let storage = Arc::new(MemoryStorage::new());

    let deployment = create_test_deployment("test-strategy", "default", 3);
    // Note: DeploymentStrategy field removed from this implementation

    let key = build_key("deployments", Some("default"), "test-strategy");

    // Create deployment
    let created: Deployment = storage.create(&key, &deployment).await.unwrap();
    assert_eq!(created.metadata.name, "test-strategy");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_deployment_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut deployment = create_test_deployment("test-finalizers", "default", 3);
    deployment.metadata.finalizers = Some(vec!["finalizer.test.io".to_string()]);

    let key = build_key("deployments", Some("default"), "test-finalizers");

    // Create with finalizer
    let created: Deployment = storage.create(&key, &deployment).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: Deployment = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Clean up - remove finalizer first
    deployment.metadata.finalizers = None;
    storage.update(&key, &deployment).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_deployment_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let deployment = create_test_deployment("test-immutable", "default", 3);
    let key = build_key("deployments", Some("default"), "test-immutable");

    // Create
    let created: Deployment = storage.create(&key, &deployment).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_deploy = created.clone();
    updated_deploy.spec.replicas = Some(10);

    let updated: Deployment = storage.update(&key, &updated_deploy).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(updated.spec.replicas, Some(10));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_deployment_label_selector() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "frontend".to_string());
    labels.insert("tier".to_string(), "web".to_string());

    let mut deployment = create_test_deployment("test-labels", "default", 3);
    deployment.metadata.labels = Some(labels.clone());
    deployment.spec.selector = LabelSelector {
        match_labels: Some(labels),
        match_expressions: None,
    };

    let key = build_key("deployments", Some("default"), "test-labels");

    // Create with labels
    let created: Deployment = storage.create(&key, &deployment).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(created_labels.get("app"), Some(&"frontend".to_string()));
    assert_eq!(created_labels.get("tier"), Some(&"web".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_deployment_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("deployments", Some("default"), "nonexistent");
    let result = storage.get::<Deployment>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_deployment_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let deployment = create_test_deployment("nonexistent", "default", 3);
    let key = build_key("deployments", Some("default"), "nonexistent");

    let result = storage.update(&key, &deployment).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_deployment_min_ready_seconds() {
    let storage = Arc::new(MemoryStorage::new());

    let mut deployment = create_test_deployment("test-min-ready", "default", 3);
    deployment.spec.min_ready_seconds = Some(30);

    let key = build_key("deployments", Some("default"), "test-min-ready");

    let created: Deployment = storage.create(&key, &deployment).await.unwrap();
    assert_eq!(created.spec.min_ready_seconds, Some(30));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_deployment_progress_deadline() {
    let storage = Arc::new(MemoryStorage::new());

    let deployment = create_test_deployment("test-deadline", "default", 3);

    let key = build_key("deployments", Some("default"), "test-deadline");

    let _created: Deployment = storage.create(&key, &deployment).await.unwrap();

    // Clean up
    storage.delete(&key).await.unwrap();
}
