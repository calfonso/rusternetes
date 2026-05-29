//! Table-driven unit tests for `RBACAuthorizer` and `NodeAuthorizer`.
//!
//! Mirrors the structure and coverage intent of the upstream Go test files:
//!   - <https://github.com/kubernetes/kubernetes/blob/master/plugin/pkg/auth/authorizer/rbac/rbac_test.go>
//!   - <https://github.com/kubernetes/kubernetes/blob/master/plugin/pkg/auth/authorizer/node/node_authorizer_test.go>
//!
//! Each authorizer is tested through the public `Authorizer::authorize` API.
//! Subjects are exercised via Role/RoleBinding and ClusterRole/ClusterRoleBinding
//! objects seeded into a purpose-built `MockAuthzStorage` that holds RBAC objects
//! in memory, mirroring what `rusternetes_storage::MemoryStorage` would do in
//! integration tests.

use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{
    AlwaysAllowAuthorizer, AlwaysDenyAuthorizer, Authorizer, AuthzStorage, Decision,
    RBACAuthorizer, RequestAttributes,
};
use rusternetes_common::error::Result;
use rusternetes_common::resources::rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Minimal in-test AuthzStorage so we don't need rusternetes-storage as a dep
// ---------------------------------------------------------------------------

/// Simple in-memory storage for RBAC objects, keyed by the same path patterns
/// that `MemoryStorage`'s `AuthzStorage` impl uses:
///   `/registry/roles/<ns>/<name>`
///   `/registry/rolebindings/<ns>/<name>`
///   `/registry/clusterroles/<name>`
///   `/registry/clusterrolebindings/<name>`
#[derive(Default)]
struct MockAuthzStorage {
    /// Serialized JSON blobs keyed by storage path.
    store: HashMap<String, String>,
}

impl MockAuthzStorage {
    fn new() -> Self {
        Self::default()
    }

    fn insert_role(&mut self, role: &Role) {
        let ns = role
            .metadata
            .namespace
            .as_deref()
            .expect("Role must have a namespace");
        let key = format!("/registry/roles/{}/{}", ns, role.metadata.name);
        self.store.insert(key, serde_json::to_string(role).unwrap());
    }

    fn insert_role_binding(&mut self, rb: &RoleBinding) {
        let ns = rb
            .metadata
            .namespace
            .as_deref()
            .expect("RoleBinding must have a namespace");
        let key = format!("/registry/rolebindings/{}/{}", ns, rb.metadata.name);
        self.store.insert(key, serde_json::to_string(rb).unwrap());
    }

    fn insert_cluster_role(&mut self, cr: &ClusterRole) {
        let key = format!("/registry/clusterroles/{}", cr.metadata.name);
        self.store.insert(key, serde_json::to_string(cr).unwrap());
    }

    fn insert_cluster_role_binding(&mut self, crb: &ClusterRoleBinding) {
        let key = format!("/registry/clusterrolebindings/{}", crb.metadata.name);
        self.store.insert(key, serde_json::to_string(crb).unwrap());
    }
}

#[async_trait]
impl AuthzStorage for MockAuthzStorage {
    /// Retrieve a single object by resource-type-derived key.
    ///
    /// Mirrors the `type_name` dispatch in `memory.rs` / `etcd.rs`.
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let tn = std::any::type_name::<T>();
        let full_key = match namespace {
            Some(ns) => {
                if tn.contains("RoleBinding") && !tn.contains("Cluster") {
                    format!("/registry/rolebindings/{}/{}", ns, key)
                } else if tn.contains("Role") && !tn.contains("Cluster") {
                    format!("/registry/roles/{}/{}", ns, key)
                } else {
                    format!("/registry/unknown/{}/{}", ns, key)
                }
            }
            None => {
                if tn.contains("ClusterRoleBinding") {
                    format!("/registry/clusterrolebindings/{}", key)
                } else if tn.contains("ClusterRole") && !tn.contains("Binding") {
                    format!("/registry/clusterroles/{}", key)
                } else {
                    format!("/registry/unknown/{}", key)
                }
            }
        };

        let json = self.store.get(&full_key).ok_or_else(|| {
            rusternetes_common::error::Error::NotFound(format!("key not found: {}", full_key))
        })?;

        serde_json::from_str(json).map_err(rusternetes_common::error::Error::Serialization)
    }

    /// List all objects of a given type in a namespace (or cluster-scope).
    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: serde::Serialize + DeserializeOwned + Send + Sync,
    {
        let tn = std::any::type_name::<T>();
        let prefix = match namespace {
            Some(ns) => {
                if tn.contains("RoleBinding") && !tn.contains("Cluster") {
                    format!("/registry/rolebindings/{}/", ns)
                } else if tn.contains("Role") && !tn.contains("Cluster") {
                    format!("/registry/roles/{}/", ns)
                } else {
                    format!("/registry/unknown/{}/", ns)
                }
            }
            None => {
                if tn.contains("ClusterRoleBinding") {
                    "/registry/clusterrolebindings/".to_string()
                } else if tn.contains("ClusterRole") && !tn.contains("Binding") {
                    "/registry/clusterroles/".to_string()
                } else {
                    "/registry/unknown/".to_string()
                }
            }
        };

        let mut out = Vec::new();
        for (k, v) in &self.store {
            if k.starts_with(&prefix) {
                let item: T = serde_json::from_str(v)
                    .map_err(rusternetes_common::error::Error::Serialization)?;
                out.push(item);
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn user(username: &str) -> UserInfo {
    UserInfo {
        username: username.to_string(),
        uid: "uid-1".to_string(),
        groups: vec![],
        extra: HashMap::new(),
    }
}

fn user_with_groups(username: &str, groups: &[&str]) -> UserInfo {
    UserInfo {
        username: username.to_string(),
        uid: "uid-1".to_string(),
        groups: groups.iter().map(|s| s.to_string()).collect(),
        extra: HashMap::new(),
    }
}

fn service_account_user(namespace: &str, name: &str) -> UserInfo {
    user(&format!("system:serviceaccount:{}:{}", namespace, name))
}

fn node_user(node_name: &str) -> UserInfo {
    user(&format!("system:node:{}", node_name))
}

fn policy_rule(
    verbs: &[&str],
    api_groups: &[&str],
    resources: &[&str],
    resource_names: Option<&[&str]>,
) -> PolicyRule {
    PolicyRule {
        verbs: verbs.iter().map(|s| s.to_string()).collect(),
        api_groups: Some(api_groups.iter().map(|s| s.to_string()).collect()),
        resources: Some(resources.iter().map(|s| s.to_string()).collect()),
        resource_names: resource_names.map(|names| names.iter().map(|s| s.to_string()).collect()),
        non_resource_urls: None,
    }
}

fn non_resource_rule(verbs: &[&str], urls: &[&str]) -> PolicyRule {
    PolicyRule {
        verbs: verbs.iter().map(|s| s.to_string()).collect(),
        api_groups: None,
        resources: None,
        resource_names: None,
        non_resource_urls: Some(urls.iter().map(|s| s.to_string()).collect()),
    }
}

fn rbac_authorizer(store: MockAuthzStorage) -> RBACAuthorizer<MockAuthzStorage> {
    RBACAuthorizer::new(Arc::new(store))
}

// ---------------------------------------------------------------------------
// AlwaysAllow / AlwaysDeny sanity tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn always_allow_authorizer_allows_everything() {
    let az = AlwaysAllowAuthorizer;
    let attrs = RequestAttributes::new(user("bob"), "delete", "pods")
        .with_namespace("default")
        .with_api_group("");
    let decision = az.authorize(&attrs).await.unwrap();
    assert_eq!(decision, Decision::Allow);
}

#[tokio::test]
async fn always_deny_authorizer_denies_everything() {
    let az = AlwaysDenyAuthorizer;
    let attrs = RequestAttributes::new(user("bob"), "get", "pods")
        .with_namespace("default")
        .with_api_group("");
    let decision = az.authorize(&attrs).await.unwrap();
    assert!(matches!(decision, Decision::Deny(_)));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — system:admin bypass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_system_admin_always_allowed() {
    // system:admin bypasses RBAC entirely, no bindings needed.
    let store = MockAuthzStorage::new();
    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("system:admin"), "delete", "pods")
        .with_namespace("kube-system")
        .with_api_group("");

    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — User subject via Role + RoleBinding
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_user_allowed_by_role_binding() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("pod-reader", "default").with_rules(vec![policy_rule(
        &["get", "list", "watch"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("bob-pod-reader", "default")
        .with_subjects(vec![Subject::user("bob")])
        .with_role_ref(RoleRef::role("pod-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    // Allowed verbs
    for verb in ["get", "list", "watch"] {
        let attrs = RequestAttributes::new(user("bob"), verb, "pods")
            .with_namespace("default")
            .with_api_group("");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "expected Allow for verb={verb}"
        );
    }

    // Denied verb
    let attrs = RequestAttributes::new(user("bob"), "delete", "pods")
        .with_namespace("default")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_user_denied_in_different_namespace() {
    // RoleBinding is in "default"; request is for "other" namespace — must deny.
    let mut store = MockAuthzStorage::new();

    let role = Role::new("pod-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("bob-pod-reader", "default")
        .with_subjects(vec![Subject::user("bob")])
        .with_role_ref(RoleRef::role("pod-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("bob"), "get", "pods")
        .with_namespace("other")
        .with_api_group("");

    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_user_denied_wrong_resource() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("pod-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("bob-pod-reader", "default")
        .with_subjects(vec![Subject::user("bob")])
        .with_role_ref(RoleRef::role("pod-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("bob"), "get", "secrets")
        .with_namespace("default")
        .with_api_group("");

    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — wildcard verb / resource
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_wildcard_verb_allows_any_verb() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("all-pods", "default").with_rules(vec![policy_rule(
        &["*"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-all-pods", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("all-pods"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    for verb in [
        "get", "list", "watch", "create", "update", "patch", "delete",
    ] {
        let attrs = RequestAttributes::new(user("alice"), verb, "pods")
            .with_namespace("default")
            .with_api_group("");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "wildcard verb should allow {verb}"
        );
    }
}

#[tokio::test]
async fn rbac_wildcard_resource_allows_any_resource() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("all-resources", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["*"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-all", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("all-resources"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    for resource in ["pods", "services", "configmaps", "secrets", "deployments"] {
        let attrs = RequestAttributes::new(user("alice"), "get", resource)
            .with_namespace("default")
            .with_api_group("");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "wildcard resource should allow {resource}"
        );
    }
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — resourceNames restriction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_resource_name_restriction_allows_matching_name() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("named-secret", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["secrets"],
        Some(&["my-secret"]),
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-named-secret", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("named-secret"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    // Allowed — name matches
    let attrs = RequestAttributes::new(user("alice"), "get", "secrets")
        .with_namespace("default")
        .with_api_group("")
        .with_name("my-secret");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_resource_name_restriction_denies_other_name() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("named-secret", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["secrets"],
        Some(&["my-secret"]),
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-named-secret", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("named-secret"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    // Denied — name does not match
    let attrs = RequestAttributes::new(user("alice"), "get", "secrets")
        .with_namespace("default")
        .with_api_group("")
        .with_name("other-secret");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_resource_name_unrestricted_allows_any_name() {
    // Rule has no resourceNames → applies to all names.
    let mut store = MockAuthzStorage::new();

    let role = Role::new("any-secret", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["secrets"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-any-secret", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("any-secret"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    for name in ["secret-a", "secret-b", "some-other-secret"] {
        let attrs = RequestAttributes::new(user("alice"), "get", "secrets")
            .with_namespace("default")
            .with_api_group("")
            .with_name(name);
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "unrestricted rule should allow {name}"
        );
    }
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — ClusterRole + ClusterRoleBinding (cluster-scoped)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_cluster_role_binding_allows_cluster_wide() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("node-reader").with_rules(vec![policy_rule(
        &["get", "list"],
        &[""],
        &["nodes"],
        None,
    )]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-node-reader")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("node-reader"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // ClusterRoleBinding → no namespace restriction
    let attrs = RequestAttributes::new(user("alice"), "get", "nodes").with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);

    // Also allowed with a namespace set (binding is cluster-scoped)
    let attrs = RequestAttributes::new(user("alice"), "list", "nodes")
        .with_api_group("")
        .with_namespace("kube-system");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_role_binding_referencing_cluster_role() {
    // A RoleBinding can reference a ClusterRole, scoping it to the binding's namespace.
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("pod-reader").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_cluster_role(&cr);

    let rb = RoleBinding::new("alice-pod-reader", "staging")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("pod-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    // Allowed in the binding's namespace
    let attrs = RequestAttributes::new(user("alice"), "get", "pods")
        .with_namespace("staging")
        .with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);

    // Denied in a different namespace
    let attrs = RequestAttributes::new(user("alice"), "get", "pods")
        .with_namespace("production")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — Group subject matching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_group_subject_allows_group_member() {
    let mut store = MockAuthzStorage::new();

    let cr =
        ClusterRole::new("ops-role").with_rules(vec![policy_rule(&["*"], &["*"], &["*"], None)]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("ops-binding")
        .with_subjects(vec![Subject::group("ops-team")])
        .with_role_ref(RoleRef::cluster_role("ops-role"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // User is member of the group
    let attrs = RequestAttributes::new(
        user_with_groups("carol", &["ops-team", "dev-team"]),
        "delete",
        "pods",
    )
    .with_namespace("default")
    .with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_group_subject_denies_non_group_member() {
    let mut store = MockAuthzStorage::new();

    let cr =
        ClusterRole::new("ops-role").with_rules(vec![policy_rule(&["*"], &["*"], &["*"], None)]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("ops-binding")
        .with_subjects(vec![Subject::group("ops-team")])
        .with_role_ref(RoleRef::cluster_role("ops-role"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // User does NOT belong to ops-team
    let attrs = RequestAttributes::new(user_with_groups("dave", &["dev-team"]), "delete", "pods")
        .with_namespace("default")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — ServiceAccount subject matching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_service_account_subject_allowed() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("sa-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["configmaps"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("sa-binding", "default")
        .with_subjects(vec![Subject::service_account("my-sa", "default")])
        .with_role_ref(RoleRef::role("sa-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(
        service_account_user("default", "my-sa"),
        "get",
        "configmaps",
    )
    .with_namespace("default")
    .with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_service_account_subject_denied_wrong_namespace() {
    // SA in "other" namespace should not match a binding for SA in "default".
    let mut store = MockAuthzStorage::new();

    let role = Role::new("sa-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["configmaps"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("sa-binding", "default")
        .with_subjects(vec![Subject::service_account("my-sa", "default")])
        .with_role_ref(RoleRef::role("sa-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(service_account_user("other", "my-sa"), "get", "configmaps")
        .with_namespace("default")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_service_account_subject_denied_wrong_name() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("sa-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["configmaps"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("sa-binding", "default")
        .with_subjects(vec![Subject::service_account("my-sa", "default")])
        .with_role_ref(RoleRef::role("sa-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(
        service_account_user("default", "other-sa"),
        "get",
        "configmaps",
    )
    .with_namespace("default")
    .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — nonResourceURLs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_non_resource_url_allowed() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("metrics-reader")
        .with_rules(vec![non_resource_rule(&["get"], &["/metrics", "/healthz"])]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-metrics")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("metrics-reader"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/metrics");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);

    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/healthz");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_non_resource_url_denied_unlisted_path() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("metrics-reader")
        .with_rules(vec![non_resource_rule(&["get"], &["/metrics"])]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-metrics")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("metrics-reader"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/livez");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_non_resource_url_wildcard_allows_any_path() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("url-wildcard").with_rules(vec![non_resource_rule(&["*"], &["*"])]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-url-wildcard")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("url-wildcard"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    for path in ["/metrics", "/healthz", "/livez", "/readyz", "/version"] {
        let attrs = RequestAttributes::new_non_resource(user("alice"), "get", path);
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "wildcard URL should allow path {path}"
        );
    }
}

// Note: authz.rs uses `path.starts_with(url)` for non-resource URL matching,
// which means a rule for "/health" also permits "/healthz".  This test
// documents the current (permissive) behaviour so that future tightening
// shows up as a deliberate change rather than a surprise breakage.
#[tokio::test]
async fn rbac_non_resource_url_prefix_match_is_permissive() {
    let mut store = MockAuthzStorage::new();

    // Rule grants only "/health" — NOT "/healthz".
    let cr =
        ClusterRole::new("health-only").with_rules(vec![non_resource_rule(&["get"], &["/health"])]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-health-only")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("health-only"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // "/health" itself is allowed.
    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/health");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);

    // "/healthz" is currently also allowed because of the starts_with check.
    // This documents a known permissive deviation from strict K8s semantics.
    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/healthz");
    assert_eq!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Allow,
        "current impl allows /healthz when rule is /health (starts_with); \
         tighten authz.rs to require trailing '/*' for prefix semantics"
    );
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — API group matching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_api_group_required_allows_matching_group() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("deployment-writer").with_rules(vec![policy_rule(
        &["create", "update"],
        &["apps"],
        &["deployments"],
        None,
    )]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-deployments")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("deployment-writer"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("alice"), "create", "deployments")
        .with_api_group("apps")
        .with_namespace("default");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_api_group_required_denies_wrong_group() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("deployment-writer").with_rules(vec![policy_rule(
        &["create"],
        &["apps"],
        &["deployments"],
        None,
    )]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-deployments")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("deployment-writer"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // Wrong API group ("" instead of "apps")
    let attrs = RequestAttributes::new(user("alice"), "create", "deployments")
        .with_api_group("")
        .with_namespace("default");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_wildcard_api_group_allows_any_group() {
    let mut store = MockAuthzStorage::new();

    let cr =
        ClusterRole::new("any-group").with_rules(vec![policy_rule(&["get"], &["*"], &["*"], None)]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-any-group")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("any-group"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    for group in ["", "apps", "batch", "extensions", "storage.k8s.io"] {
        let attrs = RequestAttributes::new(user("alice"), "get", "anything")
            .with_api_group(group)
            .with_namespace("default");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "wildcard api group should allow group={group}"
        );
    }
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — no bindings → deny
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_no_bindings_denies_everything() {
    let store = MockAuthzStorage::new(); // empty
    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("nobody"), "get", "pods")
        .with_namespace("default")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// NodeAuthorizer — own node access
// ---------------------------------------------------------------------------

#[tokio::test]
async fn node_authorizer_allows_own_node_get() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs = RequestAttributes::new(node_user("worker-1"), "get", "nodes")
        .with_api_group("")
        .with_name("worker-1");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn node_authorizer_allows_reading_other_node() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    // Rusternetes NodeAuthorizer is more permissive than the upstream Go implementation:
    // `is_node_allowed_resource` lists `("", "nodes")` as an unconditionally readable
    // resource, so any node user can read other nodes.  The upstream Go NodeAuthorizer
    // uses a graph-based scheme and restricts each kubelet to its OWN node object —
    // this is a known Rusternetes deviation (tracked for future tightening).
    let attrs = RequestAttributes::new(node_user("worker-1"), "get", "nodes")
        .with_api_group("")
        .with_name("worker-2");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn node_authorizer_allows_own_node_create() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs = RequestAttributes::new(node_user("worker-2"), "create", "nodes")
        .with_api_group("")
        .with_name("worker-2");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn node_authorizer_allows_own_node_status_update() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let mut attrs = RequestAttributes::new(node_user("worker-1"), "update", "nodes")
        .with_api_group("")
        .with_name("worker-1")
        .with_subresource("status");
    attrs.namespace = None;
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

// Note: the status-update check in authz.rs does not filter by verb, so any verb
// (including "delete") is allowed when resource="nodes", subresource="status", and
// the name matches the node.  This test documents the current permissive behaviour.
#[tokio::test]
async fn node_authorizer_status_path_has_no_verb_restriction() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    // "delete nodes/status" is currently allowed — documents a known deviation from
    // upstream K8s which only allows update/patch on a node's own status subresource.
    let mut attrs = RequestAttributes::new(node_user("worker-1"), "delete", "nodes")
        .with_api_group("")
        .with_name("worker-1")
        .with_subresource("status");
    attrs.namespace = None;
    assert_eq!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Allow,
        "current impl allows delete nodes/status (no verb check in status path); \
         tighten authz.rs to restrict to update/patch only"
    );
}

#[tokio::test]
async fn node_authorizer_denies_non_node_user() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    // Plain user (not system:node:*) must be rejected.
    let attrs = RequestAttributes::new(user("alice"), "get", "nodes")
        .with_api_group("")
        .with_name("worker-1");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn node_authorizer_allows_node_to_read_services() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    for verb in ["get", "list", "watch"] {
        let attrs =
            RequestAttributes::new(node_user("worker-1"), verb, "services").with_api_group("");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "node should be able to {verb} services"
        );
    }
}

#[tokio::test]
async fn node_authorizer_allows_node_to_read_endpoints() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs =
        RequestAttributes::new(node_user("worker-1"), "list", "endpoints").with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn node_authorizer_allows_node_to_read_persistentvolumes() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs = RequestAttributes::new(node_user("worker-1"), "get", "persistentvolumes")
        .with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn node_authorizer_allows_node_to_read_pods_in_namespace() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    for verb in ["get", "list", "watch"] {
        let attrs = RequestAttributes::new(node_user("worker-1"), verb, "pods")
            .with_api_group("")
            .with_namespace("default");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "node should {verb} pods in a namespace"
        );
    }
}

#[tokio::test]
async fn node_authorizer_allows_node_to_read_secrets_in_namespace() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs = RequestAttributes::new(node_user("worker-1"), "get", "secrets")
        .with_api_group("")
        .with_namespace("default");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn node_authorizer_allows_node_to_read_configmaps_in_namespace() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs = RequestAttributes::new(node_user("worker-1"), "get", "configmaps")
        .with_api_group("")
        .with_namespace("default");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn node_authorizer_allows_node_to_create_events() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs =
        RequestAttributes::new(node_user("worker-1"), "create", "events").with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn node_authorizer_denies_node_from_deleting_pods() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs = RequestAttributes::new(node_user("worker-1"), "delete", "pods")
        .with_api_group("")
        .with_namespace("default");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn node_authorizer_allows_coordination_lease() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    for verb in ["get", "create", "update", "patch"] {
        let attrs = RequestAttributes::new(node_user("worker-1"), verb, "leases")
            .with_api_group("coordination.k8s.io")
            .with_namespace("kube-node-lease");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "node should be able to {verb} leases in coordination.k8s.io"
        );
    }
}

#[tokio::test]
async fn node_authorizer_allows_csinodes() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    // CSINode belongs to the storage.k8s.io API group in real Kubernetes.
    // authz.rs currently allows create/update/patch on "csinodes" without checking
    // the api_group, so we test both the real group and the permissive fallback.
    for api_group in ["storage.k8s.io", ""] {
        for verb in ["create", "update", "patch"] {
            let attrs = RequestAttributes::new(node_user("worker-1"), verb, "csinodes")
                .with_api_group(api_group)
                .with_name("worker-1");
            assert_eq!(
                az.authorize(&attrs).await.unwrap(),
                Decision::Allow,
                "node should be able to {verb} its own CSINode (api_group={api_group})"
            );
        }
    }
}

#[tokio::test]
async fn node_authorizer_denies_deleting_other_nodes_objects() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    let attrs = RequestAttributes::new(node_user("worker-1"), "delete", "nodes")
        .with_api_group("")
        .with_name("worker-2");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// NodeAuthorizer — system:nodes group is irrelevant (username is the signal)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn node_authorizer_group_membership_irrelevant_username_matters() {
    let az = rusternetes_common::authz::NodeAuthorizer;

    // User with system:nodes group but wrong username prefix → denied.
    let u = user_with_groups("not-a-node", &["system:nodes"]);
    let attrs = RequestAttributes::new(u, "get", "nodes")
        .with_api_group("")
        .with_name("worker-1");
    // The NodeAuthorizer only checks username prefix, not group membership.
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}
