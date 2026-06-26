//! Integration tests for NetworkPolicy handler
//!
//! Tests all CRUD operations, edge cases, and error handling for networkpolicies.
//! NetworkPolicy describes what network traffic is allowed for a set of Pods.

use rusternetes_common::resources::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicySpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test networkpolicy with basic pod selector
fn create_test_networkpolicy(name: &str, namespace: &str) -> NetworkPolicy {
    let mut match_labels = HashMap::new();
    match_labels.insert("app".to_string(), "backend".to_string());

    NetworkPolicy {
        type_meta: TypeMeta {
            api_version: "networking.k8s.io/v1".to_string(),
            kind: "NetworkPolicy".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: NetworkPolicySpec {
            pod_selector: LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            },
            ingress: None,
            egress: None,
            policy_types: Some(vec!["Ingress".to_string()]),
        },
    }
}

#[tokio::test]
async fn test_networkpolicy_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let np = create_test_networkpolicy("test-np", "default");
    let key = build_key("networkpolicies", Some("default"), "test-np");

    // Create
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert_eq!(created.metadata.name, "test-np");
    assert_eq!(created.metadata.namespace, Some("default".to_string()));
    assert!(!created.metadata.uid.is_empty());
    assert_eq!(created.spec.policy_types, Some(vec!["Ingress".to_string()]));

    // Get
    let retrieved: NetworkPolicy = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-np");
    assert_eq!(
        retrieved.spec.policy_types,
        Some(vec!["Ingress".to_string()])
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut np = create_test_networkpolicy("test-update-np", "default");
    let key = build_key("networkpolicies", Some("default"), "test-update-np");

    // Create
    storage.create(&key, &np).await.unwrap();

    // Update policy types to include Egress
    np.spec.policy_types = Some(vec!["Ingress".to_string(), "Egress".to_string()]);
    let updated: NetworkPolicy = storage.update(&key, &np).await.unwrap();
    assert_eq!(
        updated.spec.policy_types,
        Some(vec!["Ingress".to_string(), "Egress".to_string()])
    );

    // Verify update
    let retrieved: NetworkPolicy = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.spec.policy_types,
        Some(vec!["Ingress".to_string(), "Egress".to_string()])
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let np = create_test_networkpolicy("test-delete-np", "default");
    let key = build_key("networkpolicies", Some("default"), "test-delete-np");

    // Create
    storage.create(&key, &np).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<NetworkPolicy>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_networkpolicy_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple networkpolicies
    let np1 = create_test_networkpolicy("np-1", "default");
    let np2 = create_test_networkpolicy("np-2", "default");
    let np3 = create_test_networkpolicy("np-3", "default");

    let key1 = build_key("networkpolicies", Some("default"), "np-1");
    let key2 = build_key("networkpolicies", Some("default"), "np-2");
    let key3 = build_key("networkpolicies", Some("default"), "np-3");

    storage.create(&key1, &np1).await.unwrap();
    storage.create(&key2, &np2).await.unwrap();
    storage.create(&key3, &np3).await.unwrap();

    // List
    let prefix = build_prefix("networkpolicies", Some("default"));
    let nps: Vec<NetworkPolicy> = storage.list(&prefix).await.unwrap();

    assert!(nps.len() >= 3);
    let names: Vec<String> = nps.iter().map(|np| np.metadata.name.clone()).collect();
    assert!(names.contains(&"np-1".to_string()));
    assert!(names.contains(&"np-2".to_string()));
    assert!(names.contains(&"np-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_list_across_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create networkpolicies in different namespaces
    let np1 = create_test_networkpolicy("np-ns1", "namespace-1");
    let np2 = create_test_networkpolicy("np-ns2", "namespace-2");

    let key1 = build_key("networkpolicies", Some("namespace-1"), "np-ns1");
    let key2 = build_key("networkpolicies", Some("namespace-2"), "np-ns2");

    storage.create(&key1, &np1).await.unwrap();
    storage.create(&key2, &np2).await.unwrap();

    // List all (no namespace filter)
    let prefix = build_prefix("networkpolicies", None);
    let nps: Vec<NetworkPolicy> = storage.list(&prefix).await.unwrap();

    // Should find at least our 2 networkpolicies
    assert!(nps.len() >= 2);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_with_ingress_rules() {
    let storage = Arc::new(MemoryStorage::new());

    let mut np = create_test_networkpolicy("test-ingress", "default");

    // Add ingress rules
    np.spec.ingress = Some(vec![NetworkPolicyIngressRule {
        ports: Some(vec![NetworkPolicyPort {
            protocol: "TCP".to_string(),
            port: Some(json!(80)),
            end_port: None,
        }]),
        from: Some(vec![NetworkPolicyPeer {
            pod_selector: Some(LabelSelector {
                match_labels: {
                    let mut labels = HashMap::new();
                    labels.insert("role".to_string(), "frontend".to_string());
                    Some(labels)
                },
                match_expressions: None,
            }),
            namespace_selector: None,
            ip_block: None,
        }]),
    }]);

    let key = build_key("networkpolicies", Some("default"), "test-ingress");

    // Create with ingress rules
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert!(created.spec.ingress.is_some());
    let ingress_rules = created.spec.ingress.unwrap();
    assert_eq!(ingress_rules.len(), 1);
    assert!(ingress_rules[0].ports.is_some());
    assert!(ingress_rules[0].from.is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_with_egress_rules() {
    let storage = Arc::new(MemoryStorage::new());

    let mut np = create_test_networkpolicy("test-egress", "default");
    np.spec.policy_types = Some(vec!["Egress".to_string()]);

    // Add egress rules
    np.spec.egress = Some(vec![NetworkPolicyEgressRule {
        ports: Some(vec![NetworkPolicyPort {
            protocol: "TCP".to_string(),
            port: Some(json!(443)),
            end_port: None,
        }]),
        to: Some(vec![NetworkPolicyPeer {
            pod_selector: None,
            namespace_selector: None,
            ip_block: Some(IPBlock {
                cidr: "10.0.0.0/24".to_string(),
                except: Some(vec!["10.0.0.5/32".to_string()]),
            }),
        }]),
    }]);

    let key = build_key("networkpolicies", Some("default"), "test-egress");

    // Create with egress rules
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert!(created.spec.egress.is_some());
    let egress_rules = created.spec.egress.unwrap();
    assert_eq!(egress_rules.len(), 1);
    assert!(egress_rules[0].ports.is_some());
    assert!(egress_rules[0].to.is_some());

    // Verify IPBlock
    let peer = &egress_rules[0].to.as_ref().unwrap()[0];
    assert!(peer.ip_block.is_some());
    let ip_block = peer.ip_block.as_ref().unwrap();
    assert_eq!(ip_block.cidr, "10.0.0.0/24");
    assert_eq!(ip_block.except, Some(vec!["10.0.0.5/32".to_string()]));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut np = create_test_networkpolicy("test-finalizers", "default");
    np.metadata.finalizers = Some(vec!["finalizer.test.io".to_string()]);

    let key = build_key("networkpolicies", Some("default"), "test-finalizers");

    // Create with finalizer
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: NetworkPolicy = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Clean up - remove finalizer first
    np.metadata.finalizers = None;
    storage.update(&key, &np).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let np = create_test_networkpolicy("test-immutable", "default");
    let key = build_key("networkpolicies", Some("default"), "test-immutable");

    // Create
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_np = created.clone();
    updated_np.spec.policy_types = Some(vec!["Ingress".to_string(), "Egress".to_string()]);

    let updated: NetworkPolicy = storage.update(&key, &updated_np).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    assert_eq!(
        updated.spec.policy_types,
        Some(vec!["Ingress".to_string(), "Egress".to_string()])
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("networkpolicies", Some("default"), "nonexistent");
    let result = storage.get::<NetworkPolicy>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_networkpolicy_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let np = create_test_networkpolicy("nonexistent", "default");
    let key = build_key("networkpolicies", Some("default"), "nonexistent");

    let result = storage.update(&key, &np).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_networkpolicy_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("environment".to_string(), "production".to_string());
    labels.insert("team".to_string(), "security".to_string());

    let mut np = create_test_networkpolicy("test-labels", "default");
    np.metadata.labels = Some(labels.clone());

    let key = build_key("networkpolicies", Some("default"), "test-labels");

    // Create with labels
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(
        created_labels.get("environment"),
        Some(&"production".to_string())
    );
    assert_eq!(created_labels.get("team"), Some(&"security".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert("description".to_string(), "Backend isolation".to_string());
    annotations.insert("owner".to_string(), "security-team".to_string());

    let mut np = create_test_networkpolicy("test-annotations", "default");
    np.metadata.annotations = Some(annotations.clone());

    let key = build_key("networkpolicies", Some("default"), "test-annotations");

    // Create with annotations
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    let created_annotations = created.metadata.annotations.unwrap();
    assert_eq!(
        created_annotations.get("description"),
        Some(&"Backend isolation".to_string())
    );
    assert_eq!(
        created_annotations.get("owner"),
        Some(&"security-team".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_with_owner_reference() {
    let storage = Arc::new(MemoryStorage::new());

    let mut np = create_test_networkpolicy("test-owner", "default");
    np.metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
        api_version: "v1".to_string(),
        kind: "Namespace".to_string(),
        name: "default".to_string(),
        uid: "namespace-uid-123".to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }]);

    let key = build_key("networkpolicies", Some("default"), "test-owner");

    // Create with owner reference
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert!(created.metadata.owner_references.is_some());
    let owner_refs = created.metadata.owner_references.unwrap();
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0].kind, "Namespace");
    assert_eq!(owner_refs[0].name, "default");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_empty_pod_selector() {
    let storage = Arc::new(MemoryStorage::new());

    // Empty pod selector matches all pods in namespace
    let np = NetworkPolicy {
        type_meta: TypeMeta {
            api_version: "networking.k8s.io/v1".to_string(),
            kind: "NetworkPolicy".to_string(),
        },
        metadata: ObjectMeta::new("test-empty-selector").with_namespace("default"),
        spec: NetworkPolicySpec {
            pod_selector: LabelSelector {
                match_labels: None,
                match_expressions: None,
            },
            ingress: None,
            egress: None,
            policy_types: Some(vec!["Ingress".to_string()]),
        },
    };

    let key = build_key("networkpolicies", Some("default"), "test-empty-selector");

    // Create with empty selector
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert!(created.spec.pod_selector.match_labels.is_none());
    assert!(created.spec.pod_selector.match_expressions.is_none());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_port_range() {
    let storage = Arc::new(MemoryStorage::new());

    let mut np = create_test_networkpolicy("test-port-range", "default");

    // Add ingress rule with port range
    np.spec.ingress = Some(vec![NetworkPolicyIngressRule {
        ports: Some(vec![NetworkPolicyPort {
            protocol: "TCP".to_string(),
            port: Some(json!(8000)),
            end_port: Some(9000),
        }]),
        from: None,
    }]);

    let key = build_key("networkpolicies", Some("default"), "test-port-range");

    // Create with port range
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    let ingress_rules = created.spec.ingress.unwrap();
    let ports = ingress_rules[0].ports.as_ref().unwrap();
    assert_eq!(ports[0].port, Some(json!(8000)));
    assert_eq!(ports[0].end_port, Some(9000));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_namespace_selector() {
    let storage = Arc::new(MemoryStorage::new());

    let mut np = create_test_networkpolicy("test-ns-selector", "default");

    // Add ingress rule with namespace selector
    np.spec.ingress = Some(vec![NetworkPolicyIngressRule {
        ports: None,
        from: Some(vec![NetworkPolicyPeer {
            pod_selector: None,
            namespace_selector: Some(LabelSelector {
                match_labels: {
                    let mut labels = HashMap::new();
                    labels.insert("environment".to_string(), "production".to_string());
                    Some(labels)
                },
                match_expressions: None,
            }),
            ip_block: None,
        }]),
    }]);

    let key = build_key("networkpolicies", Some("default"), "test-ns-selector");

    // Create with namespace selector
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    let ingress_rules = created.spec.ingress.unwrap();
    let from = ingress_rules[0].from.as_ref().unwrap();
    assert!(from[0].namespace_selector.is_some());

    let ns_selector = from[0].namespace_selector.as_ref().unwrap();
    assert!(ns_selector.match_labels.is_some());
    let labels = ns_selector.match_labels.as_ref().unwrap();
    assert_eq!(labels.get("environment"), Some(&"production".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_multiple_policy_types() {
    let storage = Arc::new(MemoryStorage::new());

    let mut np = create_test_networkpolicy("test-both-types", "default");
    np.spec.policy_types = Some(vec!["Ingress".to_string(), "Egress".to_string()]);

    // Add both ingress and egress rules
    np.spec.ingress = Some(vec![NetworkPolicyIngressRule {
        ports: Some(vec![NetworkPolicyPort {
            protocol: "TCP".to_string(),
            port: Some(json!(80)),
            end_port: None,
        }]),
        from: None,
    }]);

    np.spec.egress = Some(vec![NetworkPolicyEgressRule {
        ports: Some(vec![NetworkPolicyPort {
            protocol: "TCP".to_string(),
            port: Some(json!(443)),
            end_port: None,
        }]),
        to: None,
    }]);

    let key = build_key("networkpolicies", Some("default"), "test-both-types");

    // Create with both policy types
    let created: NetworkPolicy = storage.create(&key, &np).await.unwrap();
    assert_eq!(
        created.spec.policy_types,
        Some(vec!["Ingress".to_string(), "Egress".to_string()])
    );
    assert!(created.spec.ingress.is_some());
    assert!(created.spec.egress.is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_networkpolicy_multiple_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    let np1 = create_test_networkpolicy("test-np", "namespace-1");
    let np2 = create_test_networkpolicy("test-np", "namespace-2");

    let key1 = build_key("networkpolicies", Some("namespace-1"), "test-np");
    let key2 = build_key("networkpolicies", Some("namespace-2"), "test-np");

    // Create same-named NetworkPolicy in different namespaces
    let created1: NetworkPolicy = storage.create(&key1, &np1).await.unwrap();
    let created2: NetworkPolicy = storage.create(&key2, &np2).await.unwrap();

    assert_eq!(created1.metadata.name, "test-np");
    assert_eq!(created1.metadata.namespace, Some("namespace-1".to_string()));
    assert_eq!(created2.metadata.name, "test-np");
    assert_eq!(created2.metadata.namespace, Some("namespace-2".to_string()));
    assert_ne!(created1.metadata.uid, created2.metadata.uid);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}
