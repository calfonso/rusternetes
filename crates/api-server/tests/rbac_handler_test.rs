//! Integration tests for RBAC (Role-Based Access Control) handlers
//!
//! Tests all CRUD operations for Roles, RoleBindings, ClusterRoles, and ClusterRoleBindings
//! This is security-critical functionality requiring comprehensive test coverage

use rusternetes_common::resources::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
};
use rusternetes_common::types::ObjectMeta;
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// Helper function to create test Role
fn create_test_role(name: &str, namespace: &str) -> Role {
    use rusternetes_common::types::TypeMeta;
    Role {
        type_meta: TypeMeta {
            kind: "Role".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            api_groups: Some(vec!["".to_string()]),
            resources: Some(vec!["pods".to_string()]),
            verbs: vec!["get".to_string(), "list".to_string()],
            resource_names: None,
            non_resource_urls: None,
        }],
    }
}

// Helper function to create test RoleBinding
fn create_test_rolebinding(name: &str, namespace: &str, role_name: &str) -> RoleBinding {
    use rusternetes_common::types::TypeMeta;
    RoleBinding {
        type_meta: TypeMeta {
            kind: "RoleBinding".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "Role".to_string(),
            name: role_name.to_string(),
        },
        subjects: vec![Subject {
            kind: "User".to_string(),
            name: "test-user".to_string(),
            api_group: Some("rbac.authorization.k8s.io".to_string()),
            namespace: None,
        }],
    }
}

// Helper function to create test ClusterRole
fn create_test_clusterrole(name: &str) -> ClusterRole {
    use rusternetes_common::types::TypeMeta;
    ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None,
            ..Default::default()
        },
        rules: vec![PolicyRule {
            api_groups: Some(vec!["*".to_string()]),
            resources: Some(vec!["*".to_string()]),
            verbs: vec!["*".to_string()],
            resource_names: None,
            non_resource_urls: None,
        }],
        aggregation_rule: None,
    }
}

// Helper function to create test ClusterRoleBinding
fn create_test_clusterrolebinding(name: &str, role_name: &str) -> ClusterRoleBinding {
    use rusternetes_common::types::TypeMeta;
    ClusterRoleBinding {
        type_meta: TypeMeta {
            kind: "ClusterRoleBinding".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None,
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "ClusterRole".to_string(),
            name: role_name.to_string(),
        },
        subjects: vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: "default".to_string(),
            api_group: Some("".to_string()),
            namespace: Some("default".to_string()),
        }],
    }
}

// ======== ROLE TESTS ========

#[tokio::test]
async fn test_role_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-role-create";
    let role = create_test_role("test-role", namespace);

    let key = build_key("roles", Some(namespace), "test-role");
    let created: Role = storage.create(&key, &role).await.unwrap();

    assert_eq!(created.metadata.name, "test-role");
    assert_eq!(created.rules.len(), 1);
    assert!(!created.metadata.uid.is_empty());
    assert!(created.metadata.creation_timestamp.is_some());

    // Get the role
    let retrieved: Role = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-role");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_role_update() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-role-update";
    let mut role = create_test_role("test-role", namespace);

    let key = build_key("roles", Some(namespace), "test-role");
    storage.create(&key, &role).await.unwrap();

    // Update the role with additional permissions
    role.rules.push(PolicyRule {
        api_groups: Some(vec!["".to_string()]),
        resources: Some(vec!["services".to_string()]),
        verbs: vec!["get".to_string()],
        resource_names: None,
        non_resource_urls: None,
    });

    let updated: Role = storage.update(&key, &role).await.unwrap();
    assert_eq!(updated.rules.len(), 2);

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_role_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-role-delete";
    let role = create_test_role("test-role", namespace);

    let key = build_key("roles", Some(namespace), "test-role");
    storage.create(&key, &role).await.unwrap();

    // Delete the role
    storage.delete(&key).await.unwrap();

    // Verify it's deleted
    let result: rusternetes_common::Result<Role> = storage.get(&key).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_role_list_in_namespace() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-role-list";

    // Create multiple roles
    for i in 1..=3 {
        let role = create_test_role(&format!("role-{}", i), namespace);
        let key = build_key("roles", Some(namespace), &format!("role-{}", i));
        storage.create(&key, &role).await.unwrap();
    }

    // List roles in namespace
    let prefix = build_prefix("roles", Some(namespace));
    let roles: Vec<Role> = storage.list(&prefix).await.unwrap();

    assert_eq!(roles.len(), 3);

    // Cleanup
    for i in 1..=3 {
        let key = build_key("roles", Some(namespace), &format!("role-{}", i));
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_role_namespace_isolation() {
    let storage = Arc::new(MemoryStorage::new());

    let ns1 = "test-role-ns1";
    let ns2 = "test-role-ns2";

    // Create roles in different namespaces
    let role1 = create_test_role("role-1", ns1);
    let role2 = create_test_role("role-2", ns2);

    let key1 = build_key("roles", Some(ns1), "role-1");
    let key2 = build_key("roles", Some(ns2), "role-2");

    storage.create(&key1, &role1).await.unwrap();
    storage.create(&key2, &role2).await.unwrap();

    // List roles in ns1 - should only see role1
    let prefix1 = build_prefix("roles", Some(ns1));
    let roles1: Vec<Role> = storage.list(&prefix1).await.unwrap();
    assert_eq!(roles1.len(), 1);
    assert_eq!(roles1[0].metadata.name, "role-1");

    // List roles in ns2 - should only see role2
    let prefix2 = build_prefix("roles", Some(ns2));
    let roles2: Vec<Role> = storage.list(&prefix2).await.unwrap();
    assert_eq!(roles2.len(), 1);
    assert_eq!(roles2[0].metadata.name, "role-2");

    // Cleanup
    storage.delete(&key1).await.unwrap();
    storage.delete(&key2).await.unwrap();
}

#[tokio::test]
async fn test_role_with_multiple_rules() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-role-multi-rules";
    use rusternetes_common::types::TypeMeta;
    let role = Role {
        type_meta: TypeMeta {
            kind: "Role".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "multi-rule-role".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        rules: vec![
            PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["pods".to_string()]),
                verbs: vec!["get".to_string(), "list".to_string(), "watch".to_string()],
                resource_names: None,
                non_resource_urls: None,
            },
            PolicyRule {
                api_groups: Some(vec!["apps".to_string()]),
                resources: Some(vec!["deployments".to_string()]),
                verbs: vec!["get".to_string(), "list".to_string()],
                resource_names: None,
                non_resource_urls: None,
            },
            PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["services".to_string()]),
                verbs: vec!["*".to_string()],
                resource_names: None,
                non_resource_urls: None,
            },
        ],
    };

    let key = build_key("roles", Some(namespace), "multi-rule-role");
    let created: Role = storage.create(&key, &role).await.unwrap();

    assert_eq!(created.rules.len(), 3);
    assert_eq!(created.rules[0].resources.as_ref().unwrap()[0], "pods");
    assert_eq!(
        created.rules[1].resources.as_ref().unwrap()[0],
        "deployments"
    );
    assert_eq!(created.rules[2].verbs[0], "*");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_role_with_resource_names() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-role-resource-names";
    use rusternetes_common::types::TypeMeta;
    let role = Role {
        type_meta: TypeMeta {
            kind: "Role".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "specific-resources".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            api_groups: Some(vec!["".to_string()]),
            resources: Some(vec!["configmaps".to_string()]),
            verbs: vec!["get".to_string()],
            resource_names: Some(vec!["my-config".to_string(), "another-config".to_string()]),
            non_resource_urls: None,
        }],
    };

    let key = build_key("roles", Some(namespace), "specific-resources");
    let created: Role = storage.create(&key, &role).await.unwrap();

    assert!(created.rules[0].resource_names.is_some());
    assert_eq!(created.rules[0].resource_names.as_ref().unwrap().len(), 2);

    // Cleanup
    storage.delete(&key).await.unwrap();
}

// ======== ROLEBINDING TESTS ========

#[tokio::test]
async fn test_rolebinding_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-rb-create";
    let rb = create_test_rolebinding("test-rb", namespace, "test-role");

    let key = build_key("rolebindings", Some(namespace), "test-rb");
    let created: RoleBinding = storage.create(&key, &rb).await.unwrap();

    assert_eq!(created.metadata.name, "test-rb");
    assert_eq!(created.role_ref.name, "test-role");
    assert!(!created.subjects.is_empty());

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rolebinding_with_multiple_subjects() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-rb-multi-subjects";
    use rusternetes_common::types::TypeMeta;
    let rb = RoleBinding {
        type_meta: TypeMeta {
            kind: "RoleBinding".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "multi-subject-rb".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "Role".to_string(),
            name: "test-role".to_string(),
        },
        subjects: vec![
            Subject {
                kind: "User".to_string(),
                name: "user1".to_string(),
                api_group: Some("rbac.authorization.k8s.io".to_string()),
                namespace: None,
            },
            Subject {
                kind: "ServiceAccount".to_string(),
                name: "sa1".to_string(),
                api_group: Some("".to_string()),
                namespace: Some(namespace.to_string()),
            },
            Subject {
                kind: "Group".to_string(),
                name: "group1".to_string(),
                api_group: Some("rbac.authorization.k8s.io".to_string()),
                namespace: None,
            },
        ],
    };

    let key = build_key("rolebindings", Some(namespace), "multi-subject-rb");
    let created: RoleBinding = storage.create(&key, &rb).await.unwrap();

    assert_eq!(created.subjects.len(), 3);
    assert_eq!(created.subjects[0].kind, "User");
    assert_eq!(created.subjects[1].kind, "ServiceAccount");
    assert_eq!(created.subjects[2].kind, "Group");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rolebinding_referencing_clusterrole() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-rb-clusterrole-ref";
    use rusternetes_common::types::TypeMeta;
    let rb = RoleBinding {
        type_meta: TypeMeta {
            kind: "RoleBinding".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "rb-with-clusterrole".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "ClusterRole".to_string(),
            name: "admin".to_string(),
        },
        subjects: vec![Subject {
            kind: "User".to_string(),
            name: "admin-user".to_string(),
            api_group: Some("rbac.authorization.k8s.io".to_string()),
            namespace: None,
        }],
    };

    let key = build_key("rolebindings", Some(namespace), "rb-with-clusterrole");
    let created: RoleBinding = storage.create(&key, &rb).await.unwrap();

    assert_eq!(created.role_ref.kind, "ClusterRole");
    assert_eq!(created.role_ref.name, "admin");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

// ======== CLUSTERROLE TESTS ========

#[tokio::test]
async fn test_clusterrole_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let cr = create_test_clusterrole("test-cluster-role");

    let key = build_key("clusterroles", None, "test-cluster-role");
    let created: ClusterRole = storage.create(&key, &cr).await.unwrap();

    assert_eq!(created.metadata.name, "test-cluster-role");
    assert!(created.metadata.namespace.is_none());
    assert_eq!(created.rules.len(), 1);

    // Get the clusterrole
    let retrieved: ClusterRole = storage.get(&key).await.unwrap();
    assert_eq!(retrieved.metadata.name, "test-cluster-role");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_clusterrole_list() {
    let storage = Arc::new(MemoryStorage::new());

    // Create multiple clusterroles
    for i in 1..=3 {
        let cr = create_test_clusterrole(&format!("cr-{}", i));
        let key = build_key("clusterroles", None, &format!("cr-{}", i));
        storage.create(&key, &cr).await.unwrap();
    }

    // List clusterroles
    let prefix = build_prefix("clusterroles", None);
    let crs: Vec<ClusterRole> = storage.list(&prefix).await.unwrap();

    assert!(crs.len() >= 3); // May have system clusterroles

    // Cleanup
    for i in 1..=3 {
        let key = build_key("clusterroles", None, &format!("cr-{}", i));
        storage.delete(&key).await.unwrap();
    }
}

#[tokio::test]
async fn test_clusterrole_with_non_resource_urls() {
    let storage = Arc::new(MemoryStorage::new());

    use rusternetes_common::types::TypeMeta;
    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "non-resource-urls".to_string(),
            namespace: None,
            ..Default::default()
        },
        rules: vec![PolicyRule {
            api_groups: None,
            resources: None,
            verbs: vec!["get".to_string()],
            resource_names: None,
            non_resource_urls: Some(vec!["/healthz".to_string(), "/metrics".to_string()]),
        }],
        aggregation_rule: None,
    };

    let key = build_key("clusterroles", None, "non-resource-urls");
    let created: ClusterRole = storage.create(&key, &cr).await.unwrap();

    assert!(created.rules[0].non_resource_urls.is_some());
    assert_eq!(
        created.rules[0].non_resource_urls.as_ref().unwrap().len(),
        2
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_clusterrole_delete() {
    let storage = Arc::new(MemoryStorage::new());

    let cr = create_test_clusterrole("test-delete-cr");
    let key = build_key("clusterroles", None, "test-delete-cr");

    storage.create(&key, &cr).await.unwrap();

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify deleted
    let result: rusternetes_common::Result<ClusterRole> = storage.get(&key).await;
    assert!(result.is_err());
}

// ======== CLUSTERROLEBINDING TESTS ========

#[tokio::test]
async fn test_clusterrolebinding_create_and_get() {
    let storage = Arc::new(MemoryStorage::new());

    let crb = create_test_clusterrolebinding("test-crb", "admin");

    let key = build_key("clusterrolebindings", None, "test-crb");
    let created: ClusterRoleBinding = storage.create(&key, &crb).await.unwrap();

    assert_eq!(created.metadata.name, "test-crb");
    assert_eq!(created.role_ref.name, "admin");
    assert!(created.metadata.namespace.is_none());

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_clusterrolebinding_with_multiple_subjects() {
    let storage = Arc::new(MemoryStorage::new());

    use rusternetes_common::types::TypeMeta;
    let crb = ClusterRoleBinding {
        type_meta: TypeMeta {
            kind: "ClusterRoleBinding".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "multi-subject-crb".to_string(),
            namespace: None,
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "ClusterRole".to_string(),
            name: "cluster-admin".to_string(),
        },
        subjects: vec![
            Subject {
                kind: "User".to_string(),
                name: "admin1".to_string(),
                api_group: Some("rbac.authorization.k8s.io".to_string()),
                namespace: None,
            },
            Subject {
                kind: "ServiceAccount".to_string(),
                name: "system-admin".to_string(),
                api_group: Some("".to_string()),
                namespace: Some("kube-system".to_string()),
            },
        ],
    };

    let key = build_key("clusterrolebindings", None, "multi-subject-crb");
    let created: ClusterRoleBinding = storage.create(&key, &crb).await.unwrap();

    assert_eq!(created.subjects.len(), 2);

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_clusterrolebinding_update() {
    let storage = Arc::new(MemoryStorage::new());

    let mut crb = create_test_clusterrolebinding("test-crb-update", "view");
    let key = build_key("clusterrolebindings", None, "test-crb-update");

    storage.create(&key, &crb).await.unwrap();

    // Update subjects
    crb.subjects = vec![Subject {
        kind: "User".to_string(),
        name: "new-user".to_string(),
        api_group: Some("rbac.authorization.k8s.io".to_string()),
        namespace: None,
    }];

    let updated: ClusterRoleBinding = storage.update(&key, &crb).await.unwrap();
    assert_eq!(updated.subjects[0].name, "new-user");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

// ======== EDGE CASES AND VALIDATION ========

#[tokio::test]
async fn test_role_with_empty_rules() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-empty-rules";
    use rusternetes_common::types::TypeMeta;
    let role = Role {
        type_meta: TypeMeta {
            kind: "Role".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "empty-rules".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        rules: vec![],
    };

    let key = build_key("roles", Some(namespace), "empty-rules");
    let created: Role = storage.create(&key, &role).await.unwrap();

    assert_eq!(created.rules.len(), 0);

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rolebinding_without_subjects() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-no-subjects";
    use rusternetes_common::types::TypeMeta;
    let rb = RoleBinding {
        type_meta: TypeMeta {
            kind: "RoleBinding".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "no-subjects-rb".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "Role".to_string(),
            name: "test-role".to_string(),
        },
        subjects: vec![],
    };

    let key = build_key("rolebindings", Some(namespace), "no-subjects-rb");
    let created: RoleBinding = storage.create(&key, &rb).await.unwrap();

    assert!(created.subjects.is_empty());

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_role_with_labels_and_annotations() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-role-metadata";
    let mut role = create_test_role("labeled-role", namespace);

    role.metadata.labels = Some({
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());
        labels.insert("environment".to_string(), "dev".to_string());
        labels
    });

    role.metadata.annotations = Some({
        let mut annotations = HashMap::new();
        annotations.insert("description".to_string(), "Test role".to_string());
        annotations
    });

    let key = build_key("roles", Some(namespace), "labeled-role");
    let created: Role = storage.create(&key, &role).await.unwrap();

    assert!(created.metadata.labels.is_some());
    assert_eq!(
        created.metadata.labels.as_ref().unwrap().get("app"),
        Some(&"test".to_string())
    );
    assert!(created.metadata.annotations.is_some());

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_clusterrole_with_wildcard_permissions() {
    let storage = Arc::new(MemoryStorage::new());

    use rusternetes_common::types::TypeMeta;
    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".to_string(),
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "wildcard-cr".to_string(),
            namespace: None,
            ..Default::default()
        },
        rules: vec![PolicyRule {
            api_groups: Some(vec!["*".to_string()]),
            resources: Some(vec!["*".to_string()]),
            verbs: vec!["*".to_string()],
            resource_names: None,
            non_resource_urls: None,
        }],
        aggregation_rule: None,
    };

    let key = build_key("clusterroles", None, "wildcard-cr");
    let created: ClusterRole = storage.create(&key, &cr).await.unwrap();

    assert_eq!(created.rules[0].api_groups.as_ref().unwrap()[0], "*");
    assert_eq!(created.rules[0].resources.as_ref().unwrap()[0], "*");
    assert_eq!(created.rules[0].verbs[0], "*");

    // Cleanup
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_rbac_resources_with_finalizers() {
    let storage = Arc::new(MemoryStorage::new());

    let namespace = "test-finalizers";
    let mut role = create_test_role("finalizer-role", namespace);

    role.metadata.finalizers = Some(vec!["test.finalizer.io/cleanup".to_string()]);

    let key = build_key("roles", Some(namespace), "finalizer-role");
    let created: Role = storage.create(&key, &role).await.unwrap();

    assert!(created.metadata.finalizers.is_some());
    assert_eq!(
        created.metadata.finalizers.as_ref().unwrap()[0],
        "test.finalizer.io/cleanup"
    );

    // Cleanup
    storage.delete(&key).await.unwrap();
}
