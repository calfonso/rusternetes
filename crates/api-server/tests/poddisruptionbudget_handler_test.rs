//! Integration tests for PodDisruptionBudget handler
//!
//! Tests all CRUD operations, edge cases, and error handling for poddisruptionbudgets.
//! PodDisruptionBudget limits the number of pods that can be disrupted during voluntary disruptions.

use rusternetes_common::resources::{
    IntOrString, PodDisruptionBudget, PodDisruptionBudgetCondition, PodDisruptionBudgetSpec,
    PodDisruptionBudgetStatus,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a test poddisruptionbudget with minAvailable
fn create_test_pdb(name: &str, namespace: &str, min_available: i32) -> PodDisruptionBudget {
    let mut match_labels = HashMap::new();
    match_labels.insert("app".to_string(), "web".to_string());

    PodDisruptionBudget {
        type_meta: TypeMeta {
            api_version: "policy/v1".to_string(),
            kind: "PodDisruptionBudget".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
        status: None,
    }
}

#[tokio::test]
async fn test_pdb_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let pdb = create_test_pdb("test-pdb", "default", 2);
    let key = build_key("poddisruptionbudgets", Some("default"), "test-pdb");

    // Create
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert_eq!(created.metadata.name, "test-pdb");
    assert_eq!(created.metadata.namespace, Some("default".to_string()));
    assert!(!created.metadata.uid.is_empty());
    assert!(created.spec.min_available.is_some());

    // Verify min_available value
    match &created.spec.min_available {
        Some(IntOrString::Int(n)) => assert_eq!(*n, 2),
        _ => panic!("Expected IntOrString::Int"),
    }

    // Get
    let retrieved: PodDisruptionBudget = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-pdb");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pdb = create_test_pdb("test-update-pdb", "default", 2);
    let key = build_key("poddisruptionbudgets", Some("default"), "test-update-pdb");

    // Create
    storage.create(&key, &pdb).await.unwrap();

    // Update min_available to 3
    pdb.spec.min_available = Some(IntOrString::Int(3));
    let updated: PodDisruptionBudget = storage.update(&key, &pdb).await.unwrap();

    match &updated.spec.min_available {
        Some(IntOrString::Int(n)) => assert_eq!(*n, 3),
        _ => panic!("Expected IntOrString::Int"),
    }

    // Verify update
    let retrieved: PodDisruptionBudget = storage.get(&key).await.unwrap();
    match &retrieved.spec.min_available {
        Some(IntOrString::Int(n)) => assert_eq!(*n, 3),
        _ => panic!("Expected IntOrString::Int"),
    }

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let pdb = create_test_pdb("test-delete-pdb", "default", 1);
    let key = build_key("poddisruptionbudgets", Some("default"), "test-delete-pdb");

    // Create
    storage.create(&key, &pdb).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deletion
    let result = storage.get::<PodDisruptionBudget>(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_pdb_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple pdbs
    let pdb1 = create_test_pdb("pdb-1", "default", 1);
    let pdb2 = create_test_pdb("pdb-2", "default", 2);
    let pdb3 = create_test_pdb("pdb-3", "default", 3);

    let key1 = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
    let key2 = build_key("poddisruptionbudgets", Some("default"), "pdb-2");
    let key3 = build_key("poddisruptionbudgets", Some("default"), "pdb-3");

    storage.create(&key1, &pdb1).await.unwrap();
    storage.create(&key2, &pdb2).await.unwrap();
    storage.create(&key3, &pdb3).await.unwrap();

    // List
    let prefix = build_prefix("poddisruptionbudgets", Some("default"));
    let pdbs: Vec<PodDisruptionBudget> = storage.list(&prefix).await.unwrap();

    assert!(pdbs.len() >= 3);
    let names: Vec<String> = pdbs.iter().map(|p| p.metadata.name.clone()).collect();
    assert!(names.contains(&"pdb-1".to_string()));
    assert!(names.contains(&"pdb-2".to_string()));
    assert!(names.contains(&"pdb-3".to_string()));

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
    storage.delete(&key3).await.unwrap();
}

#[tokio::test]
async fn test_pdb_list_across_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    // Create pdbs in different namespaces
    let pdb1 = create_test_pdb("pdb-ns1", "namespace-1", 2);
    let pdb2 = create_test_pdb("pdb-ns2", "namespace-2", 3);

    let key1 = build_key("poddisruptionbudgets", Some("namespace-1"), "pdb-ns1");
    let key2 = build_key("poddisruptionbudgets", Some("namespace-2"), "pdb-ns2");

    storage.create(&key1, &pdb1).await.unwrap();
    storage.create(&key2, &pdb2).await.unwrap();

    // List all (no namespace filter)
    let prefix = build_prefix("poddisruptionbudgets", None);
    let pdbs: Vec<PodDisruptionBudget> = storage.list(&prefix).await.unwrap();

    // Should find at least our 2 pdbs
    assert!(pdbs.len() >= 2);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_pdb_with_max_unavailable() {
    let storage = Arc::new(MemoryStorage::new());

    let mut match_labels = HashMap::new();
    match_labels.insert("app".to_string(), "database".to_string());

    let pdb = PodDisruptionBudget {
        type_meta: TypeMeta {
            api_version: "policy/v1".to_string(),
            kind: "PodDisruptionBudget".to_string(),
        },
        metadata: ObjectMeta::new("test-max-unavailable").with_namespace("default"),
        spec: PodDisruptionBudgetSpec {
            min_available: None,
            max_unavailable: Some(IntOrString::Int(1)),
            selector: LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
        status: None,
    };

    let key = build_key(
        "poddisruptionbudgets",
        Some("default"),
        "test-max-unavailable",
    );

    // Create with max_unavailable
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert!(created.spec.min_available.is_none());
    assert!(created.spec.max_unavailable.is_some());

    match &created.spec.max_unavailable {
        Some(IntOrString::Int(n)) => assert_eq!(*n, 1),
        _ => panic!("Expected IntOrString::Int"),
    }

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_with_percentage() {
    let storage = Arc::new(MemoryStorage::new());

    let mut match_labels = HashMap::new();
    match_labels.insert("tier".to_string(), "frontend".to_string());

    let pdb = PodDisruptionBudget {
        type_meta: TypeMeta {
            api_version: "policy/v1".to_string(),
            kind: "PodDisruptionBudget".to_string(),
        },
        metadata: ObjectMeta::new("test-percentage").with_namespace("default"),
        spec: PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::String("80%".to_string())),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
        status: None,
    };

    let key = build_key("poddisruptionbudgets", Some("default"), "test-percentage");

    // Create with percentage
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    match &created.spec.min_available {
        Some(IntOrString::String(s)) => assert_eq!(s, "80%"),
        _ => panic!("Expected IntOrString::String"),
    }

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_with_status() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pdb = create_test_pdb("test-status", "default", 3);

    // Set status
    pdb.status = Some(PodDisruptionBudgetStatus {
        current_healthy: 5,
        desired_healthy: 3,
        disruptions_allowed: 2,
        expected_pods: 5,
        observed_generation: Some(1),
        conditions: None,
        disrupted_pods: None,
    });

    let key = build_key("poddisruptionbudgets", Some("default"), "test-status");

    // Create with status
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert!(created.status.is_some());
    let status = created.status.as_ref().unwrap();
    assert_eq!(status.current_healthy, 5);
    assert_eq!(status.desired_healthy, 3);
    assert_eq!(status.disruptions_allowed, 2);
    assert_eq!(status.expected_pods, 5);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_with_conditions() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pdb = create_test_pdb("test-conditions", "default", 2);

    // Set status with conditions
    pdb.status = Some(PodDisruptionBudgetStatus {
        current_healthy: 4,
        desired_healthy: 2,
        disruptions_allowed: 2,
        expected_pods: 4,
        observed_generation: Some(2),
        conditions: Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some("2026-03-15T10:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: Some("Sufficient pods available for disruption".to_string()),
            ..Default::default()
        }]),
        disrupted_pods: None,
    });

    let key = build_key("poddisruptionbudgets", Some("default"), "test-conditions");

    // Create with conditions
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert!(created.status.is_some());
    let status = created.status.as_ref().unwrap();
    assert!(status.conditions.is_some());
    let conditions = status.conditions.as_ref().unwrap();
    assert_eq!(conditions.len(), 1);
    assert_eq!(conditions[0].condition_type, "DisruptionAllowed");
    assert_eq!(conditions[0].status, "True");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pdb = create_test_pdb("test-finalizers", "default", 1);
    pdb.metadata.finalizers = Some(vec!["finalizer.test.io".to_string()]);

    let key = build_key("poddisruptionbudgets", Some("default"), "test-finalizers");

    // Create with finalizer
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert_eq!(
        created.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Verify finalizer is present
    let retrieved: PodDisruptionBudget = storage.get(&key).await.unwrap();
    assert_eq!(
        retrieved.metadata.finalizers,
        Some(vec!["finalizer.test.io".to_string()])
    );

    // Clean up - remove finalizer first
    pdb.metadata.finalizers = None;
    storage.update(&key, &pdb).await.unwrap();
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_metadata_immutability() {
    let storage = Arc::new(MemoryStorage::new());

    let pdb = create_test_pdb("test-immutable", "default", 2);
    let key = build_key("poddisruptionbudgets", Some("default"), "test-immutable");

    // Create
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    let original_uid = created.metadata.uid.clone();

    // Try to update - UID should remain unchanged
    let mut updated_pdb = created.clone();
    updated_pdb.spec.min_available = Some(IntOrString::Int(5));

    let updated: PodDisruptionBudget = storage.update(&key, &updated_pdb).await.unwrap();
    assert_eq!(updated.metadata.uid, original_uid);
    match &updated.spec.min_available {
        Some(IntOrString::Int(n)) => assert_eq!(*n, 5),
        _ => panic!("Expected IntOrString::Int"),
    }

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_get_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let key = build_key("poddisruptionbudgets", Some("default"), "nonexistent");
    let result = storage.get::<PodDisruptionBudget>(&key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_pdb_update_not_found() {
    let storage = Arc::new(MemoryStorage::new());

    let pdb = create_test_pdb("nonexistent", "default", 1);
    let key = build_key("poddisruptionbudgets", Some("default"), "nonexistent");

    let result = storage.update(&key, &pdb).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_pdb_with_labels() {
    let storage = Arc::new(MemoryStorage::new());

    let mut labels = HashMap::new();
    labels.insert("environment".to_string(), "production".to_string());
    labels.insert("team".to_string(), "platform".to_string());

    let mut pdb = create_test_pdb("test-labels", "default", 2);
    pdb.metadata.labels = Some(labels.clone());

    let key = build_key("poddisruptionbudgets", Some("default"), "test-labels");

    // Create with labels
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert!(created.metadata.labels.is_some());
    let created_labels = created.metadata.labels.unwrap();
    assert_eq!(
        created_labels.get("environment"),
        Some(&"production".to_string())
    );
    assert_eq!(created_labels.get("team"), Some(&"platform".to_string()));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_with_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let mut annotations = HashMap::new();
    annotations.insert("description".to_string(), "Web tier PDB".to_string());
    annotations.insert("owner".to_string(), "platform-team".to_string());

    let mut pdb = create_test_pdb("test-annotations", "default", 3);
    pdb.metadata.annotations = Some(annotations.clone());

    let key = build_key("poddisruptionbudgets", Some("default"), "test-annotations");

    // Create with annotations
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert!(created.metadata.annotations.is_some());
    let created_annotations = created.metadata.annotations.unwrap();
    assert_eq!(
        created_annotations.get("description"),
        Some(&"Web tier PDB".to_string())
    );
    assert_eq!(
        created_annotations.get("owner"),
        Some(&"platform-team".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_with_owner_reference() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pdb = create_test_pdb("test-owner", "default", 1);
    pdb.metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
        api_version: "apps/v1".to_string(),
        kind: "Deployment".to_string(),
        name: "web-deployment".to_string(),
        uid: "deployment-uid-123".to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }]);

    let key = build_key("poddisruptionbudgets", Some("default"), "test-owner");

    // Create with owner reference
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert!(created.metadata.owner_references.is_some());
    let owner_refs = created.metadata.owner_references.unwrap();
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0].kind, "Deployment");
    assert_eq!(owner_refs[0].name, "web-deployment");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_with_unhealthy_pod_eviction_policy() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pdb = create_test_pdb("test-eviction-policy", "default", 2);
    pdb.spec.unhealthy_pod_eviction_policy = Some("AlwaysAllow".to_string());

    let key = build_key(
        "poddisruptionbudgets",
        Some("default"),
        "test-eviction-policy",
    );

    // Create with eviction policy
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert_eq!(
        created.spec.unhealthy_pod_eviction_policy,
        Some("AlwaysAllow".to_string())
    );

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_empty_selector() {
    let storage = Arc::new(MemoryStorage::new());

    // Empty selector matches all pods in namespace
    let pdb = PodDisruptionBudget {
        type_meta: TypeMeta {
            api_version: "policy/v1".to_string(),
            kind: "PodDisruptionBudget".to_string(),
        },
        metadata: ObjectMeta::new("test-empty-selector").with_namespace("default"),
        spec: PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: None,
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
        status: None,
    };

    let key = build_key(
        "poddisruptionbudgets",
        Some("default"),
        "test-empty-selector",
    );

    // Create with empty selector
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    assert!(created.spec.selector.match_labels.is_none());
    assert!(created.spec.selector.match_expressions.is_none());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_observed_generation() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pdb = create_test_pdb("test-generation", "default", 2);
    pdb.status = Some(PodDisruptionBudgetStatus {
        current_healthy: 3,
        desired_healthy: 2,
        disruptions_allowed: 1,
        expected_pods: 3,
        observed_generation: Some(5),
        conditions: None,
        disrupted_pods: None,
    });

    let key = build_key("poddisruptionbudgets", Some("default"), "test-generation");

    // Create with observed generation
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    let status = created.status.as_ref().unwrap();
    assert_eq!(status.observed_generation, Some(5));

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_zero_disruptions_allowed() {
    let storage = Arc::new(MemoryStorage::new());

    let mut pdb = create_test_pdb("test-zero-disruptions", "default", 3);
    pdb.status = Some(PodDisruptionBudgetStatus {
        current_healthy: 3,
        desired_healthy: 3,
        disruptions_allowed: 0, // No disruptions allowed
        expected_pods: 3,
        observed_generation: Some(1),
        conditions: None,
        disrupted_pods: None,
    });

    let key = build_key(
        "poddisruptionbudgets",
        Some("default"),
        "test-zero-disruptions",
    );

    // Create with zero disruptions allowed
    let created: PodDisruptionBudget = storage.create(&key, &pdb).await.unwrap();
    let status = created.status.as_ref().unwrap();
    assert_eq!(status.disruptions_allowed, 0);
    assert_eq!(status.current_healthy, 3);
    assert_eq!(status.desired_healthy, 3);

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_pdb_multiple_namespaces() {
    let storage = Arc::new(MemoryStorage::new());

    let pdb1 = create_test_pdb("test-pdb", "namespace-1", 2);
    let pdb2 = create_test_pdb("test-pdb", "namespace-2", 3);

    let key1 = build_key("poddisruptionbudgets", Some("namespace-1"), "test-pdb");
    let key2 = build_key("poddisruptionbudgets", Some("namespace-2"), "test-pdb");

    // Create same-named PDB in different namespaces
    let created1: PodDisruptionBudget = storage.create(&key1, &pdb1).await.unwrap();
    let created2: PodDisruptionBudget = storage.create(&key2, &pdb2).await.unwrap();

    assert_eq!(created1.metadata.name, "test-pdb");
    assert_eq!(created1.metadata.namespace, Some("namespace-1".to_string()));
    assert_eq!(created2.metadata.name, "test-pdb");
    assert_eq!(created2.metadata.namespace, Some("namespace-2".to_string()));
    assert_ne!(created1.metadata.uid, created2.metadata.uid);

    // Clean up
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}
