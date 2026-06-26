//! Integration tests for EndpointSlice handler
//!
//! Tests all CRUD operations, edge cases, and error handling for EndpointSlices

use rusternetes_common::resources::endpointslice::{Endpoint, EndpointPort, EndpointSlice};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// Helper function to create test EndpointSlice
fn create_test_endpointslice(name: &str, namespace: &str) -> EndpointSlice {
    EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![Endpoint {
            addresses: vec!["10.0.0.1".to_string()],
            conditions: None,
            hostname: Some("pod-1".to_string()),
            target_ref: None,
            node_name: Some("node-1".to_string()),
            zone: None,
            hints: None,
            deprecated_topology: None,
        }],
        ports: vec![EndpointPort {
            name: Some("http".to_string()),
            protocol: "TCP".to_string(),
            port: Some(80),
            app_protocol: None,
        }],
    }
}

#[tokio::test]
async fn test_endpointslice_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-create";
    let endpointslice = create_test_endpointslice("test-endpointslice", namespace);

    let key = build_key("endpointslices", Some(namespace), "test-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert_eq!(created.metadata.name, "test-endpointslice");
    assert_eq!(created.metadata.namespace, Some(namespace.to_string()));
    assert_eq!(created.address_type, "IPv4");
    assert!(!created.metadata.uid.is_empty());
    assert!(created.metadata.creation_timestamp.is_some());

    // Get the endpointslice
    let retrieved: EndpointSlice = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-endpointslice");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_update() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-update";
    let mut endpointslice = create_test_endpointslice("test-endpointslice", namespace);

    let key = build_key("endpointslices", Some(namespace), "test-endpointslice");
    storage.create(&key, &endpointslice).await.unwrap();

    // Update endpointslice with additional endpoint
    endpointslice.endpoints.push(Endpoint {
        addresses: vec!["10.0.0.2".to_string()],
        conditions: None,
        hostname: Some("pod-2".to_string()),
        target_ref: None,
        node_name: Some("node-2".to_string()),
        zone: None,
        hints: None,
        deprecated_topology: None,
    });

    let updated: EndpointSlice = storage.update(&key, &endpointslice).await.unwrap();
    assert_eq!(updated.endpoints.len(), 2);

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-delete";
    let endpointslice = create_test_endpointslice("test-endpointslice", namespace);

    let key = build_key("endpointslices", Some(namespace), "test-endpointslice");
    storage.create(&key, &endpointslice).await.unwrap();

    // Delete the endpointslice
    storage.delete(&key).await.unwrap();

    // Verify it's deleted
    let result: rusternetes_common::Result<EndpointSlice> = storage.get(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_endpointslice_list_in_namespace() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-list";

    // Create multiple endpointslices
    for i in 1..=3 {
        let endpointslice = create_test_endpointslice(&format!("endpointslice-{}", i), namespace);
        let key = build_key(
            "endpointslices",
            Some(namespace),
            &format!("endpointslice-{}", i),
        );
        storage.create(&key, &endpointslice).await.unwrap();
    }

    // List endpointslices in namespace
    let prefix = build_prefix("endpointslices", Some(namespace));
    let endpointslices: Vec<EndpointSlice> = storage.list(&prefix).await.unwrap();

    assert_eq!(endpointslices.len(), 3);

    // Cleanup
    for i in 1..=3 {
        let key = build_key(
            "endpointslices",
            Some(namespace),
            &format!("endpointslice-{}", i),
        );
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_endpointslice_namespace_isolation() {
    let storage = Arc::new(MemoryStorage::new());

    let ns1 = "test-endpointslice-ns1";
    let ns2 = "test-endpointslice-ns2";

    // Create endpointslices in different namespaces
    let endpointslice1 = create_test_endpointslice("endpointslice-1", ns1);
    let endpointslice2 = create_test_endpointslice("endpointslice-2", ns2);

    let key1 = build_key("endpointslices", Some(ns1), "endpointslice-1");
    let key2 = build_key("endpointslices", Some(ns2), "endpointslice-2");

    storage.create(&key1, &endpointslice1).await.unwrap();
    storage.create(&key2, &endpointslice2).await.unwrap();

    // List endpointslices in ns1 - should only see endpointslice1
    let prefix1 = build_prefix("endpointslices", Some(ns1));
    let endpointslices1: Vec<EndpointSlice> = storage.list(&prefix1).await.unwrap();
    assert_eq!(endpointslices1.len(), 1);
    assert_eq!(endpointslices1[0].metadata.name, "endpointslice-1");

    // List endpointslices in ns2 - should only see endpointslice2
    let prefix2 = build_prefix("endpointslices", Some(ns2));
    let endpointslices2: Vec<EndpointSlice> = storage.list(&prefix2).await.unwrap();
    assert_eq!(endpointslices2.len(), 1);
    assert_eq!(endpointslices2[0].metadata.name, "endpointslice-2");

    // Cleanup
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_with_ipv6_addresses() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-ipv6";
    let endpointslice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "ipv6-endpointslice".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        address_type: "IPv6".to_string(),
        endpoints: vec![Endpoint {
            addresses: vec!["2001:db8::1".to_string(), "2001:db8::2".to_string()],
            conditions: None,
            hostname: Some("pod-1".to_string()),
            target_ref: None,
            node_name: Some("node-1".to_string()),
            zone: None,
            hints: None,
            deprecated_topology: None,
        }],
        ports: vec![EndpointPort {
            name: Some("http".to_string()),
            protocol: "TCP".to_string(),
            port: Some(80),
            app_protocol: None,
        }],
    };

    let key = build_key("endpointslices", Some(namespace), "ipv6-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert_eq!(created.address_type, "IPv6");
    assert_eq!(created.endpoints[0].addresses.len(), 2);
    assert_eq!(created.endpoints[0].addresses[0], "2001:db8::1");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_with_multiple_ports() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-multi-port";
    let endpointslice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "multi-port-endpointslice".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![Endpoint {
            addresses: vec!["10.0.0.1".to_string()],
            conditions: None,
            hostname: Some("pod-1".to_string()),
            target_ref: None,
            node_name: Some("node-1".to_string()),
            zone: None,
            hints: None,
            deprecated_topology: None,
        }],
        ports: vec![
            EndpointPort {
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                port: Some(80),
                app_protocol: Some("http".to_string()),
            },
            EndpointPort {
                name: Some("https".to_string()),
                protocol: "TCP".to_string(),
                port: Some(443),
                app_protocol: Some("https".to_string()),
            },
            EndpointPort {
                name: Some("metrics".to_string()),
                protocol: "TCP".to_string(),
                port: Some(9090),
                app_protocol: None,
            },
        ],
    };

    let key = build_key(
        "endpointslices",
        Some(namespace),
        "multi-port-endpointslice",
    );
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert_eq!(created.ports.len(), 3);
    assert_eq!(created.ports[0].port, Some(80));
    assert_eq!(created.ports[1].port, Some(443));
    assert_eq!(created.ports[2].port, Some(9090));

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_with_endpoint_conditions() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-conditions";
    let endpointslice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "conditions-endpointslice".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![
            Endpoint {
                addresses: vec!["10.0.0.1".to_string()],
                conditions: Some(rusternetes_common::resources::EndpointConditions {
                    ready: Some(true),
                    serving: Some(true),
                    terminating: Some(false),
                }),
                hostname: Some("ready-pod".to_string()),
                target_ref: None,
                node_name: Some("node-1".to_string()),
                zone: None,
                hints: None,
                deprecated_topology: None,
            },
            Endpoint {
                addresses: vec!["10.0.0.2".to_string()],
                conditions: Some(rusternetes_common::resources::EndpointConditions {
                    ready: Some(false),
                    serving: Some(false),
                    terminating: Some(true),
                }),
                hostname: Some("terminating-pod".to_string()),
                target_ref: None,
                node_name: Some("node-2".to_string()),
                zone: None,
                hints: None,
                deprecated_topology: None,
            },
        ],
        ports: vec![EndpointPort {
            name: Some("http".to_string()),
            protocol: "TCP".to_string(),
            port: Some(80),
            app_protocol: None,
        }],
    };

    let key = build_key(
        "endpointslices",
        Some(namespace),
        "conditions-endpointslice",
    );
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert_eq!(created.endpoints.len(), 2);
    assert_eq!(
        created.endpoints[0].conditions.as_ref().unwrap().ready,
        Some(true)
    );
    assert_eq!(
        created.endpoints[1]
            .conditions
            .as_ref()
            .unwrap()
            .terminating,
        Some(true)
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_with_zone_hints() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-zones";
    let endpointslice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "zoned-endpointslice".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![
            Endpoint {
                addresses: vec!["10.0.0.1".to_string()],
                conditions: None,
                hostname: Some("pod-us-west".to_string()),
                target_ref: None,
                node_name: Some("node-1".to_string()),
                zone: Some("us-west-1a".to_string()),
                hints: None,
                deprecated_topology: None,
            },
            Endpoint {
                addresses: vec!["10.0.0.2".to_string()],
                conditions: None,
                hostname: Some("pod-us-east".to_string()),
                target_ref: None,
                node_name: Some("node-2".to_string()),
                zone: Some("us-east-1b".to_string()),
                hints: None,
                deprecated_topology: None,
            },
        ],
        ports: vec![EndpointPort {
            name: Some("http".to_string()),
            protocol: "TCP".to_string(),
            port: Some(80),
            app_protocol: None,
        }],
    };

    let key = build_key("endpointslices", Some(namespace), "zoned-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert_eq!(created.endpoints[0].zone, Some("us-west-1a".to_string()));
    assert_eq!(created.endpoints[1].zone, Some("us-east-1b".to_string()));

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_empty_endpoints() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-empty";
    let endpointslice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "empty-endpointslice".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![],
        ports: vec![EndpointPort {
            name: Some("http".to_string()),
            protocol: "TCP".to_string(),
            port: Some(80),
            app_protocol: None,
        }],
    };

    let key = build_key("endpointslices", Some(namespace), "empty-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert!(created.endpoints.is_empty());

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-labels";
    let mut endpointslice = create_test_endpointslice("labeled-endpointslice", namespace);

    endpointslice.metadata.labels = Some({
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());
        labels.insert(
            "kubernetes.io/service-name".to_string(),
            "nginx-service".to_string(),
        );
        labels
    });

    let key = build_key("endpointslices", Some(namespace), "labeled-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert!(created.metadata.labels.is_some());
    assert_eq!(
        created.metadata.labels.as_ref().unwrap().get("app"),
        Some(&"nginx".to_string())
    );
    assert_eq!(
        created
            .metadata
            .labels
            .as_ref()
            .unwrap()
            .get("kubernetes.io/service-name"),
        Some(&"nginx-service".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-annotations";
    let mut endpointslice = create_test_endpointslice("annotated-endpointslice", namespace);

    endpointslice.metadata.annotations = Some({
        let mut annotations = HashMap::new();
        annotations.insert("description".to_string(), "Test endpointslice".to_string());
        annotations.insert("managed-by".to_string(), "test-controller".to_string());
        annotations
    });

    let key = build_key("endpointslices", Some(namespace), "annotated-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert!(created.metadata.annotations.is_some());
    assert_eq!(
        created
            .metadata
            .annotations
            .as_ref()
            .unwrap()
            .get("description"),
        Some(&"Test endpointslice".to_string())
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-finalizers";
    let mut endpointslice = create_test_endpointslice("finalizer-endpointslice", namespace);

    endpointslice.metadata.finalizers = Some(vec!["test.finalizer.io/cleanup".to_string()]);

    let key = build_key("endpointslices", Some(namespace), "finalizer-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert!(created.metadata.finalizers.is_some());
    assert_eq!(
        created.metadata.finalizers.as_ref().unwrap()[0],
        "test.finalizer.io/cleanup"
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-immutability";
    let endpointslice = create_test_endpointslice("test-endpointslice", namespace);

    let key = build_key("endpointslices", Some(namespace), "test-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    let original_uid = created.metadata.uid.clone();
    let original_creation_timestamp = created.metadata.creation_timestamp;

    // Update should preserve UID and creation timestamp
    let updated: EndpointSlice = storage.update(&key, &created).await.unwrap();

    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(
        updated.metadata.creation_timestamp,
        original_creation_timestamp
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_list_all_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create endpointslices in multiple namespaces
    for i in 1..=3 {
        let namespace = format!("ns-{}", i);
        let endpointslice = create_test_endpointslice(&format!("endpointslice-{}", i), &namespace);
        let key = build_key(
            "endpointslices",
            Some(&namespace),
            &format!("endpointslice-{}", i),
        );
        storage.create(&key, &endpointslice).await.unwrap();
    }

    // List all endpointslices across all namespaces
    let prefix = build_prefix("endpointslices", None);
    let all_endpointslices: Vec<EndpointSlice> = storage.list(&prefix).await.unwrap();

    assert!(all_endpointslices.len() >= 3);

    // Cleanup
    for i in 1..=3 {
        let namespace = format!("ns-{}", i);
        let key = build_key(
            "endpointslices",
            Some(&namespace),
            &format!("endpointslice-{}", i),
        );
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_endpointslice_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-not-found";
    let key = build_key("endpointslices", Some(namespace), "nonexistent");

    let result: rusternetes_common::Result<EndpointSlice> = storage.get(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_endpointslice_with_fqdn_address_type() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-fqdn";
    let endpointslice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "fqdn-endpointslice".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        address_type: "FQDN".to_string(),
        endpoints: vec![Endpoint {
            addresses: vec!["example.com".to_string(), "api.example.com".to_string()],
            conditions: None,
            hostname: None,
            target_ref: None,
            node_name: None,
            zone: None,
            hints: None,
            deprecated_topology: None,
        }],
        ports: vec![EndpointPort {
            name: Some("https".to_string()),
            protocol: "TCP".to_string(),
            port: Some(443),
            app_protocol: Some("https".to_string()),
        }],
    };

    let key = build_key("endpointslices", Some(namespace), "fqdn-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert_eq!(created.address_type, "FQDN");
    assert_eq!(created.endpoints[0].addresses[0], "example.com");
    assert_eq!(created.endpoints[0].addresses[1], "api.example.com");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_endpointslice_with_udp_protocol() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-endpointslice-udp";
    let endpointslice = EndpointSlice {
        type_meta: TypeMeta {
            kind: "EndpointSlice".to_string(),
            api_version: "discovery.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "udp-endpointslice".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: vec![Endpoint {
            addresses: vec!["10.0.0.1".to_string()],
            conditions: None,
            hostname: Some("dns-pod".to_string()),
            target_ref: None,
            node_name: Some("node-1".to_string()),
            zone: None,
            hints: None,
            deprecated_topology: None,
        }],
        ports: vec![
            EndpointPort {
                name: Some("dns".to_string()),
                protocol: "UDP".to_string(),
                port: Some(53),
                app_protocol: None,
            },
            EndpointPort {
                name: Some("dns-tcp".to_string()),
                protocol: "TCP".to_string(),
                port: Some(53),
                app_protocol: None,
            },
        ],
    };

    let key = build_key("endpointslices", Some(namespace), "udp-endpointslice");
    let created: EndpointSlice = storage.create(&key, &endpointslice).await.unwrap();

    assert_eq!(created.ports[0].protocol, "UDP");
    assert_eq!(created.ports[1].protocol, "TCP");
    assert_eq!(created.ports[0].port, Some(53));

    // Cleanup
    storage.delete(&key).await.unwrap();
}
