//! Integration tests for DaemonSet handler
//!
//! Tests all CRUD operations, edge cases, and error handling for daemonsets

use rusternetes_common::resources::pod::{Container, PodSpec};
use rusternetes_common::resources::workloads::{
    DaemonSet, DaemonSetSpec, DaemonSetStatus, DaemonSetUpdateStrategy,
};
use rusternetes_common::resources::PodTemplateSpec;
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test daemonset
fn create_test_daemonset(name: &str, namespace: &str) -> DaemonSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    DaemonSet {
        type_meta: TypeMeta {
            kind: "DaemonSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.labels = Some(labels.clone());
            meta
        },
        spec: DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.namespace = Some(namespace.to_string());
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "agent".to_string(),
                        image: "agent:latest".to_string(),
                        command: None,
                        args: None,
                        env: None,
                        ports: None,
                        volume_mounts: None,
                        resources: None,
                        liveness_probe: None,
                        readiness_probe: None,
                        startup_probe: None,
                        image_pull_policy: None,
                        working_dir: None,
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
                    ephemeral_containers: None,
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
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
        },
        status: None,
    }
}

#[tokio::test]
async fn test_daemonset_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let daemonset = create_test_daemonset("test-ds", "default");
    let key = build_key("daemonsets", Some("default"), "test-ds");

    // Create
    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    assert_eq!(created.metadata.name, "test-ds");
    assert_eq!(created.metadata.namespace, Some("default".to_string()));
    assert!(!created.metadata.uid.is_empty());

    // Get
    let retrieved: DaemonSet = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-ds");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut daemonset = create_test_daemonset("test-update-ds", "default");
    let key = build_key("daemonsets", Some("default"), "test-update-ds");

    // Create
    storage.create(&key, &daemonset).await.unwrap();

    // Update update strategy
    daemonset.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("OnDelete".to_string()),
        rolling_update: None,
    });
    let updated: DaemonSet = storage.update(&key, &daemonset).await.unwrap();
    assert_eq!(
        updated.spec.update_strategy.as_ref().unwrap().strategy_type,
        Some("OnDelete".to_string())
    );

    // Verify update
    let retrieved: DaemonSet = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved
            .spec
            .update_strategy
            .as_ref()
            .unwrap()
            .strategy_type,
        Some("OnDelete".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let daemonset = create_test_daemonset("test-delete-ds", "default");
    let key = build_key("daemonsets", Some("default"), "test-delete-ds");

    // Create
    storage.create(&key, &daemonset).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<DaemonSet>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_daemonset_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple daemonsets
    let ds1 = create_test_daemonset("ds-1", "default");
    let ds2 = create_test_daemonset("ds-2", "default");
    let ds3 = create_test_daemonset("ds-3", "default");

    let key1 = build_key("daemonsets", Some("default"), "ds-1");
    let key2 = build_key("daemonsets", Some("default"), "ds-2");
    let key3 = build_key("daemonsets", Some("default"), "ds-3");

    storage.create(&key1, &ds1).await.unwrap();
    storage.create(&key2, &ds2).await.unwrap();
    storage.create(&key3, &ds3).await.unwrap();

    // List
    let prefix = build_prefix("daemonsets", Some("default"));
    let daemonsets: Vec<DaemonSet> = storage.list(&prefix).await.unwrap();

    assert!(daemonsets.len() >= 3);
    let names: Vec<String> = daemonsets
        .iter()
        .map(|ds| ds.metadata.name.clone())
        .collect();
    assert!(names.contains(&"ds-1".to_string()));
    assert!(names.contains(&"ds-2".to_string()));
    assert!(names.contains(&"ds-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_list_across_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create daemonsets in different namespaces
    let ds1 = create_test_daemonset("ds-ns1", "namespace-1");
    let ds2 = create_test_daemonset("ds-ns2", "namespace-2");

    let key1 = build_key("daemonsets", Some("namespace-1"), "ds-ns1");
    let key2 = build_key("daemonsets", Some("namespace-2"), "ds-ns2");

    storage.create(&key1, &ds1).await.unwrap();
    storage.create(&key2, &ds2).await.unwrap();

    // List all (no namespace filter)
    let prefix = build_prefix("daemonsets", None);
    let daemonsets: Vec<DaemonSet> = storage.list(&prefix).await.unwrap();

    // Should find at least our 2 daemonsets
    assert!(daemonsets.len() >= 2);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_with_status() {
    let storage = Arc::new(MemoryStorage::new());

    let mut daemonset = create_test_daemonset("test-status", "default");
    daemonset.status = Some(DaemonSetStatus {
        current_number_scheduled: 3,
        number_misscheduled: 0,
        desired_number_scheduled: 3,
        number_ready: 2,
        number_available: None,
        number_unavailable: None,
        updated_number_scheduled: None,
        observed_generation: None,
        collision_count: None,
        conditions: None,
    });

    let key = build_key("daemonsets", Some("default"), "test-status");

    // Create with status
    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    assert_eq!(created.status.as_ref().unwrap().current_number_scheduled, 3);
    assert_eq!(created.status.as_ref().unwrap().number_ready, 2);
    assert_eq!(created.status.as_ref().unwrap().desired_number_scheduled, 3);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_rolling_update_strategy() {
    let storage = Arc::new(MemoryStorage::new());

    let mut daemonset = create_test_daemonset("test-rolling", "default");
    daemonset.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: None,
    });
    let key = build_key("daemonsets", Some("default"), "test-rolling");

    // Create with RollingUpdate strategy
    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    assert_eq!(
        created.spec.update_strategy.as_ref().unwrap().strategy_type,
        Some("RollingUpdate".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_on_delete_strategy() {
    let storage = Arc::new(MemoryStorage::new());

    let mut daemonset = create_test_daemonset("test-ondelete", "default");
    daemonset.spec.update_strategy = Some(DaemonSetUpdateStrategy {
        strategy_type: Some("OnDelete".to_string()),
        rolling_update: None,
    });

    let key = build_key("daemonsets", Some("default"), "test-ondelete");

    // Create with OnDelete strategy
    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    assert_eq!(
        created.spec.update_strategy.as_ref().unwrap().strategy_type,
        Some("OnDelete".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_with_node_selector() {
    let storage = Arc::new(MemoryStorage::new());

    let mut node_selector = HashMap::new();
    node_selector.insert("disk".to_string(), "ssd".to_string());

    let mut daemonset = create_test_daemonset("test-node-selector", "default");
    daemonset.spec.template.spec.node_selector = Some(node_selector.clone());

    let key = build_key("daemonsets", Some("default"), "test-node-selector");

    // Create with node selector
    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    let spec = &created.spec.template.spec;
    assert!(spec.node_selector.is_some());
    assert_eq!(
        spec.node_selector.as_ref().unwrap().get("disk"),
        Some(&"ssd".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let daemonset = create_test_daemonset("test-immutable", "default");
    let key = build_key("daemonsets", Some("default"), "test-immutable");

    // Create
    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let updated: DaemonSet = storage.update(&key, &created).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("daemonsets", Some("default"), "nonexistent");
    let result = storage.get::<DaemonSet>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_daemonset_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let daemonset = create_test_daemonset("nonexistent", "default");
    let key = build_key("daemonsets", Some("default"), "nonexistent");

    let result = storage.update(&key, &daemonset).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_daemonset_basic_spec() {
    let storage = Arc::new(MemoryStorage::new());

    let daemonset = create_test_daemonset("test-basic", "default");
    let key = build_key("daemonsets", Some("default"), "test-basic");

    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    assert_eq!(created.metadata.name, "test-basic");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_with_host_network() {
    let storage = Arc::new(MemoryStorage::new());

    let mut daemonset = create_test_daemonset("test-host-network", "default");
    daemonset.spec.template.spec.host_network = Some(true);

    let key = build_key("daemonsets", Some("default"), "test-host-network");

    // Create with host network
    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    assert_eq!(created.spec.template.spec.host_network, Some(true));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_daemonset_label_selector() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "monitoring".to_string());
    labels.insert("tier".to_string(), "infrastructure".to_string());

    let mut daemonset = create_test_daemonset("test-labels", "default");
    daemonset.metadata.labels = Some(labels.clone());
    daemonset.spec.selector = LabelSelector {
        match_labels: Some(labels),
        match_expressions: None,
    };

    let key = build_key("daemonsets", Some("default"), "test-labels");

    // Create with labels
    let created: DaemonSet = storage.create(&key, &daemonset).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(created_labels.get("app"), Some(&"monitoring".to_string()));
    assert_eq!(
        created_labels.get("tier"),
        Some(&"infrastructure".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}
