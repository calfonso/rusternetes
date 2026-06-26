//! Integration tests for Endpoints handler
//!
//! Tests all CRUD operations, edge cases, and error handling for Endpoints

use rusternetes_common::resources::{EndpointAddress, EndpointPort, EndpointSubset, Endpoints};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// Helper function to create test Endpoints
fn create_test_endpoints(name: &str, namespace: &str) -> Endpoints {
    Endpoints {
        type_meta: TypeMeta {
            kind: "Endpoints".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        subsets: vec![EndpointSubset {
            addresses: Some(vec![EndpointAddress {
                ip: "10.0.0.1".to_string(),
                hostname: Some("pod-1".to_string()),
                node_name: Some("node-1".to_string()),
                target_ref: None,
            }]),
            not_ready_addresses: None,
            ports: Some(vec![EndpointPort {
                name: Some("http".to_string()),
                port: 80,
                protocol: "TCP".to_string(),
                app_protocol: None,
            }]),
        }],
    }
}

#[tokio::test]
async fn test_endpoints_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-create";
    let endpoints = create_test_endpoints("test-endpoints", namespace);

    let key = build_key("endpoints", Some(namespace), "test-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    assert_eq!(created.metadata.name, "test-endpoints");
    assert_eq!(created.metadata.namespace, Some(namespace.to_string()));
    assert!(!created.metadata.uid.is_empty());
    assert!(created.metadata.creation_timestamp.is_some());

    // Get the endpoints
    let retrieved: Endpoints = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-endpoints");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_update() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-update";
    let mut endpoints = create_test_endpoints("test-endpoints", namespace);

    let key = build_key("endpoints", Some(namespace), "test-endpoints");
    storage.create(&key, &endpoints).await.unwrap();

    // Update endpoints with additional address
    if let Some(ref mut addresses) = endpoints.subsets[0].addresses {
        addresses.push(EndpointAddress {
            ip: "10.0.0.2".to_string(),
            hostname: Some("pod-2".to_string()),
            node_name: Some("node-2".to_string()),
            target_ref: None,
        });
    }

    let updated: Endpoints = storage.update(&key, &endpoints).await.unwrap();
    assert_eq!(updated.subsets[0].addresses.as_ref().unwrap().len(), 2);

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-delete";
    let endpoints = create_test_endpoints("test-endpoints", namespace);

    let key = build_key("endpoints", Some(namespace), "test-endpoints");
    storage.create(&key, &endpoints).await.unwrap();

    // Delete the endpoints
    storage.delete(&key).await.unwrap();

    // Verify it's deleted
    let result: rusternetes_common::Result<Endpoints> = storage.get(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_endpoints_list_in_namespace() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-list";

    // Create multiple endpoints
    for i in 1..=3 {
        let endpoints = create_test_endpoints(&format!("endpoints-{}", i), namespace);
        let key = build_key("endpoints", Some(namespace), &format!("endpoints-{}", i));
        storage.create(&key, &endpoints).await.unwrap();
    }

    // List endpoints in namespace
    let prefix = build_prefix("endpoints", Some(namespace));
    let endpoints: Vec<Endpoints> = storage.list(&prefix).await.unwrap();

    assert_eq!(endpoints.len(), 3);

    // Cleanup
    for i in 1..=3 {
        let key = build_key("endpoints", Some(namespace), &format!("endpoints-{}", i));
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_endpoints_namespace_isolation() {
    let storage = Arc::new(MemoryStorage::new());

    let ns1 = "test-endpoints-ns1";
    let ns2 = "test-endpoints-ns2";

    // Create endpoints in different namespaces
    let endpoints1 = create_test_endpoints("endpoints-1", ns1);
    let endpoints2 = create_test_endpoints("endpoints-2", ns2);

    let key1 = build_key("endpoints", Some(ns1), "endpoints-1");
    let key2 = build_key("endpoints", Some(ns2), "endpoints-2");

    storage.create(&key1, &endpoints1).await.unwrap();
    storage.create(&key2, &endpoints2).await.unwrap();

    // List endpoints in ns1 - should only see endpoints1
    let prefix1 = build_prefix("endpoints", Some(ns1));
    let endpoints1_list: Vec<Endpoints> = storage.list(&prefix1).await.unwrap();
    assert_eq!(endpoints1_list.len(), 1);
    assert_eq!(endpoints1_list[0].metadata.name, "endpoints-1");

    // List endpoints in ns2 - should only see endpoints2
    let prefix2 = build_prefix("endpoints", Some(ns2));
    let endpoints2_list: Vec<Endpoints> = storage.list(&prefix2).await.unwrap();
    assert_eq!(endpoints2_list.len(), 1);
    assert_eq!(endpoints2_list[0].metadata.name, "endpoints-2");

    // Cleanup
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_with_multiple_subsets() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-multi-subset";
    let endpoints = Endpoints {
        type_meta: TypeMeta {
            kind: "Endpoints".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "multi-subset-endpoints".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        subsets: vec![
            EndpointSubset {
                addresses: Some(vec![EndpointAddress {
                    ip: "10.0.0.1".to_string(),
                    hostname: Some("pod-1".to_string()),
                    node_name: Some("node-1".to_string()),
                    target_ref: None,
                }]),
                not_ready_addresses: None,
                ports: Some(vec![EndpointPort {
                    name: Some("http".to_string()),
                    port: 80,
                    protocol: "TCP".to_string(),
                    app_protocol: None,
                }]),
            },
            EndpointSubset {
                addresses: Some(vec![EndpointAddress {
                    ip: "10.0.0.2".to_string(),
                    hostname: Some("pod-2".to_string()),
                    node_name: Some("node-2".to_string()),
                    target_ref: None,
                }]),
                not_ready_addresses: None,
                ports: Some(vec![EndpointPort {
                    name: Some("https".to_string()),
                    port: 443,
                    protocol: "TCP".to_string(),
                    app_protocol: None,
                }]),
            },
        ],
    };

    let key = build_key("endpoints", Some(namespace), "multi-subset-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    assert_eq!(created.subsets.len(), 2);
    assert_eq!(
        created.subsets[0].ports.as_ref().unwrap()[0].name,
        Some("http".to_string())
    );
    assert_eq!(
        created.subsets[1].ports.as_ref().unwrap()[0].name,
        Some("https".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_with_not_ready_addresses() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-not-ready";
    let endpoints = Endpoints {
        type_meta: TypeMeta {
            kind: "Endpoints".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "not-ready-endpoints".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        subsets: vec![EndpointSubset {
            addresses: Some(vec![EndpointAddress {
                ip: "10.0.0.1".to_string(),
                hostname: Some("pod-1".to_string()),
                node_name: Some("node-1".to_string()),
                target_ref: None,
            }]),
            not_ready_addresses: Some(vec![EndpointAddress {
                ip: "10.0.0.2".to_string(),
                hostname: Some("pod-2".to_string()),
                node_name: Some("node-2".to_string()),
                target_ref: None,
            }]),
            ports: Some(vec![EndpointPort {
                name: Some("http".to_string()),
                port: 80,
                protocol: "TCP".to_string(),
                app_protocol: None,
            }]),
        }],
    };

    let key = build_key("endpoints", Some(namespace), "not-ready-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    assert!(created.subsets[0].addresses.is_some());
    assert!(created.subsets[0].not_ready_addresses.is_some());
    assert_eq!(
        created.subsets[0]
            .not_ready_addresses
            .as_ref()
            .unwrap()
            .len(),
        1
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_with_multiple_ports() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-multi-port";
    let endpoints = Endpoints {
        type_meta: TypeMeta {
            kind: "Endpoints".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "multi-port-endpoints".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        subsets: vec![EndpointSubset {
            addresses: Some(vec![EndpointAddress {
                ip: "10.0.0.1".to_string(),
                hostname: Some("pod-1".to_string()),
                node_name: Some("node-1".to_string()),
                target_ref: None,
            }]),
            not_ready_addresses: None,
            ports: Some(vec![
                EndpointPort {
                    name: Some("http".to_string()),
                    port: 80,
                    protocol: "TCP".to_string(),
                    app_protocol: Some("http".to_string()),
                },
                EndpointPort {
                    name: Some("https".to_string()),
                    port: 443,
                    protocol: "TCP".to_string(),
                    app_protocol: Some("https".to_string()),
                },
                EndpointPort {
                    name: Some("metrics".to_string()),
                    port: 9090,
                    protocol: "TCP".to_string(),
                    app_protocol: None,
                },
            ]),
        }],
    };

    let key = build_key("endpoints", Some(namespace), "multi-port-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    assert_eq!(created.subsets[0].ports.as_ref().unwrap().len(), 3);

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_empty_subsets() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-empty";
    let endpoints = Endpoints {
        type_meta: TypeMeta {
            kind: "Endpoints".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "empty-endpoints".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        subsets: vec![],
    };

    let key = build_key("endpoints", Some(namespace), "empty-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    assert!(created.subsets.is_empty());

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-labels";
    let mut endpoints = create_test_endpoints("labeled-endpoints", namespace);

    endpoints.metadata.labels = Some({
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());
        labels
    });

    let key = build_key("endpoints", Some(namespace), "labeled-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    assert!(created.metadata.labels.is_some());
    assert_eq!(
        created.metadata.labels.as_ref().unwrap().get("app"),
        Some(&"nginx".to_string())
    );
    assert_eq!(
        created.metadata.labels.as_ref().unwrap().get("tier"),
        Some(&"frontend".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-annotations";
    let mut endpoints = create_test_endpoints("annotated-endpoints", namespace);

    endpoints.metadata.annotations = Some({
        let mut annotations = HashMap::new();
        annotations.insert("description".to_string(), "Test endpoints".to_string());
        annotations.insert("managed-by".to_string(), "test-controller".to_string());
        annotations
    });

    let key = build_key("endpoints", Some(namespace), "annotated-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    assert!(created.metadata.annotations.is_some());
    assert_eq!(
        created
            .metadata
            .annotations
            .as_ref()
            .unwrap()
            .get("description"),
        Some(&"Test endpoints".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-finalizers";
    let mut endpoints = create_test_endpoints("finalizer-endpoints", namespace);

    endpoints.metadata.finalizers = Some(vec!["test.finalizer.io/cleanup".to_string()]);

    let key = build_key("endpoints", Some(namespace), "finalizer-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    assert!(created.metadata.finalizers.is_some());
    assert_eq!(
        created.metadata.finalizers.as_ref().unwrap()[0],
        "test.finalizer.io/cleanup"
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-immutability";
    let endpoints = create_test_endpoints("test-endpoints", namespace);

    let key = build_key("endpoints", Some(namespace), "test-endpoints");
    let created: Endpoints = storage.create(&key, &endpoints).await.unwrap();

    let original_uid = created.metadata.uid.clone();
    let original_creation_timestamp = created.metadata.creation_timestamp;

    // Update should preserve UID and creation timestamp
    let updated: Endpoints = storage.update(&key, &created).await.unwrap();

    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(
        updated.metadata.creation_timestamp,
        original_creation_timestamp
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpoints_list_all_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create endpoints in multiple namespaces
    for i in 1..=3 {
        let namespace = format!("ns-{}", i);
        let endpoints = create_test_endpoints(&format!("endpoints-{}", i), &namespace);
        let key = build_key("endpoints", Some(&namespace), &format!("endpoints-{}", i));
        storage.create(&key, &endpoints).await.unwrap();
    }

    // List all endpoints across all namespaces
    let prefix = build_prefix("endpoints", None);
    let all_endpoints: Vec<Endpoints> = storage.list(&prefix).await.unwrap();

    assert!(all_endpoints.len() >= 3);

    // Cleanup
    for i in 1..=3 {
        let namespace = format!("ns-{}", i);
        let key = build_key("endpoints", Some(&namespace), &format!("endpoints-{}", i));
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_endpoints_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpoints-not-found";
    let key = build_key("endpoints", Some(namespace), "nonexistent");

    let result: rusternetes_common::Result<Endpoints> = storage.get(&key).await;
    assert!(result.is_err());
}
