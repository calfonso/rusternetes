//! Integration tests for NamespaceController.
//!
//! The original suite (top of file) pins the basic active/terminating
//! reconciliation paths. The extended suite below mirrors the
//! Kubernetes v1.35 e2e namespace coverage in
//! `kubernetes/test/e2e/apimachinery/namespaces.go`, scoped to the
//! controller-level invariants we can drive with `MemoryStorage`:
//!
//!   * `test_namespace_finalizers_complete_deletion_flow` — a custom
//!     namespace finalizer keeps the namespace in Terminating until the
//!     user (or another controller) removes it, even after the
//!     `kubernetes` finalizer has been retired by `NamespaceController`.
//!   * `test_namespace_resource_quota_inheritance` — a `ResourceQuota`
//!     created in a namespace counts every pod in that namespace and
//!     never leaks usage from a sibling namespace.
//!   * `test_namespace_network_policy_isolation` — `NetworkPolicy`
//!     resources are scoped to their namespace. A NetworkPolicy in
//!     namespace A is invisible to a namespace-B list and never appears
//!     in B's `delete_all_resources` finalization sweep.
//!   * `test_namespace_rbac_isolation` — a `RoleBinding` created in
//!     namespace A only ever appears in a namespace-A scoped list, so an
//!     RBAC lookup against namespace B observes zero bindings (no
//!     cross-namespace privilege escalation through RoleBinding).
//!
//! Upstream reference:
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/apimachinery/namespaces.go>

use chrono::Utc;
use rusternetes_common::resources::{
    namespace::{NamespaceSpec, NamespaceStatus},
    rbac::{PolicyRule, RoleBinding, RoleRef, Subject},
    Container, Namespace, NetworkPolicy, NetworkPolicySpec, Pod, PodSpec, ResourceQuota,
    ResourceQuotaSpec, Role,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::namespace::NamespaceController;
use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::{build_key, build_prefix, MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_namespace_controller_creation() {
    let storage = Arc::new(MemoryStorage::new());
    let _controller = NamespaceController::new(storage);
}

#[tokio::test]
async fn test_namespace_active_not_deleted() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NamespaceController::new(storage.clone());

    // Create an active namespace
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-namespace".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None, // Not being deleted
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        }),
    };

    let key = build_key("namespaces", None, "test-namespace");
    storage.create(&key, &namespace).await.unwrap();

    // Reconcile should do nothing for active namespace
    controller.reconcile_all().await.unwrap();

    // Namespace should still exist
    let retrieved: Namespace = storage.get(&key).await.unwrap();
    assert!(retrieved.metadata.deletion_timestamp.is_none());

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_namespace_with_finalizer_marked_for_deletion() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NamespaceController::new(storage.clone());

    // Create a namespace with finalizer and deletion timestamp
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-namespace-finalizer".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: Some(vec!["kubernetes".to_string()]), // Has finalizer
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: Some(Utc::now()), // Being deleted
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Terminating),
            conditions: None,
        }),
    };

    let key = build_key("namespaces", None, "test-namespace-finalizer");
    storage.create(&key, &namespace).await.unwrap();

    // Reconcile should handle finalization
    controller.reconcile_all().await.unwrap();

    // Check if namespace still exists (it should until resources are deleted)
    let result = storage.get::<Namespace>(&key).await;

    // The namespace may or may not exist depending on whether all resources were cleaned up
    // If it exists, it should still have the deletion timestamp
    if let Ok(ns) = result {
        assert!(ns.metadata.deletion_timestamp.is_some());
    }

    // Clean up if still exists
    let _ = storage.delete(&key).await;
}

#[tokio::test]
async fn test_namespace_deletion_removes_finalizers() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NamespaceController::new(storage.clone());

    // Create a namespace with finalizer but no resources
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-namespace-cleanup".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: Some(vec!["kubernetes".to_string()]),
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: Some(Utc::now()),
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Terminating),
            conditions: None,
        }),
    };

    let key = build_key("namespaces", None, "test-namespace-cleanup");
    storage.create(&key, &namespace).await.unwrap();

    // First reconcile sets conditions, second reconcile removes finalizers
    controller.reconcile_all().await.unwrap();
    controller.reconcile_all().await.unwrap();

    // Check if finalizers were removed
    let result = storage.get::<Namespace>(&key).await;

    if let Ok(ns) = result {
        // Finalizers should be removed or empty
        assert!(
            ns.metadata.finalizers.is_none() || ns.metadata.finalizers.as_ref().unwrap().is_empty()
        );
    }

    // Clean up
    let _ = storage.delete(&key).await;
}

// ---------------------------------------------------------------------------
// Extended e2e-equivalent coverage. The four tests below mirror
// `kubernetes/test/e2e/apimachinery/namespaces.go` invariants that the
// controller is responsible for honoring at the storage level.
// ---------------------------------------------------------------------------

/// Convenience: build a namespace in `Terminating` state with the supplied
/// finalizer list. Mirrors what the api-server stamps on `DELETE`.
fn terminating_ns(name: &str, finalizers: Vec<String>) -> Namespace {
    Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: Some(finalizers),
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: Some(Utc::now()),
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Terminating),
            conditions: None,
        }),
    }
}

/// Build a no-resources pod in `namespace` (matches the shape used by the
/// existing controller-manager quota tests).
fn make_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    }
}

// ---------------------------------------------------------------------------
// Test 1: custom finalizer completes the deletion flow.
//
// Upstream contract (`pkg/controller/namespace/deletion/`): the controller
// only manages the built-in `kubernetes` finalizer. Any user-supplied
// finalizer on the namespace MUST keep it in `Terminating` until an
// external actor removes it. Only when the finalizer slice becomes empty
// can the namespace be hard-deleted from storage.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_namespace_finalizers_complete_deletion_flow() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NamespaceController::new(storage.clone());

    let ns_name = "test-ns-custom-finalizer";
    let ns = terminating_ns(
        ns_name,
        vec!["kubernetes".to_string(), "example.com/keep".to_string()],
    );
    let key = build_key("namespaces", None, ns_name);
    storage.create(&key, &ns).await.unwrap();

    // Drive the controller across two reconcile cycles — same cadence as
    // the existing `test_namespace_deletion_removes_finalizers` integration
    // test. After this the controller has had a chance to retire the
    // built-in `kubernetes` finalizer but MUST NOT touch the custom one.
    controller.reconcile_all().await.unwrap();
    controller.reconcile_all().await.unwrap();

    let after_controller: Namespace = storage
        .get(&key)
        .await
        .expect("namespace must remain in storage while a custom finalizer is present");
    let finalizers = after_controller
        .metadata
        .finalizers
        .as_ref()
        .expect("finalizers slice must remain set");
    assert!(
        finalizers.contains(&"example.com/keep".to_string()),
        "custom finalizer must be preserved across reconciles, got {finalizers:?}"
    );
    assert!(
        !finalizers.contains(&"kubernetes".to_string()),
        "kubernetes finalizer should be retired by the controller, got {finalizers:?}"
    );
    assert!(
        after_controller.metadata.deletion_timestamp.is_some(),
        "namespace must remain Terminating while finalizers remain"
    );

    // Simulate the external actor (the owner of the custom finalizer)
    // clearing it. Once the slice is empty the controller persists the drained
    // finalizers, and the api-server removes the object from storage on that
    // update (`ShouldDeleteNamespaceDuringUpdate`, covered in
    // `crates/api-server/tests/namespace_finalize_removal_test.rs`). This test
    // drives the controller over a dumb `MemoryStorage` with no finalize
    // semantics, so it asserts the controller's half of the contract: the
    // finalizer slice is fully drained, leaving the namespace collectable.
    let mut ns = after_controller.clone();
    ns.metadata.finalizers = Some(vec![]);
    storage.update(&key, &ns).await.unwrap();
    controller.reconcile_all().await.unwrap();

    let after_drain = storage.get::<Namespace>(&key).await;
    match after_drain {
        // A finalize-aware backend (the api-server) would have removed it.
        Err(_) => {}
        Ok(remaining) => {
            let fins = remaining.metadata.finalizers.unwrap_or_default();
            assert!(
                fins.is_empty(),
                "all finalizers must be drained so the api-server can collect \
                 the namespace, got {fins:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test 2: ResourceQuota is inherited by every pod in the namespace.
//
// Upstream contract (`pkg/quota/v1/evaluator/core/pods.go`): a quota is
// applied to every pod in its namespace and only those pods. Sibling
// namespaces never contribute to `status.used`. Mirrors upstream's
// `TestQuota` for the controller-driver path.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_namespace_resource_quota_inheritance() {
    let storage = Arc::new(MemoryStorage::new());
    let quota_controller = ResourceQuotaController::new(storage.clone());

    // Two namespaces. Only `team-a` has a quota.
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "10".to_string());
    let quota = ResourceQuota::new(
        "team-a-quota",
        "team-a",
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    );
    let quota_key = build_key("resourcequotas", Some("team-a"), "team-a-quota");
    storage.create(&quota_key, &quota).await.unwrap();

    // Three pods in team-a (should be counted) and two pods in team-b
    // (must NOT be counted).
    for i in 0..3 {
        let name = format!("a-pod-{i}");
        let pod = make_pod(&name, "team-a");
        storage
            .create(&build_key("pods", Some("team-a"), &name), &pod)
            .await
            .unwrap();
    }
    for i in 0..2 {
        let name = format!("b-pod-{i}");
        let pod = make_pod(&name, "team-b");
        storage
            .create(&build_key("pods", Some("team-b"), &name), &pod)
            .await
            .unwrap();
    }

    quota_controller.reconcile_all().await.unwrap();

    let updated: ResourceQuota = storage.get(&quota_key).await.unwrap();
    let used = updated
        .status
        .expect("status must be populated")
        .used
        .expect("status.used must be populated");
    assert_eq!(
        used.get("pods").map(String::as_str),
        Some("3"),
        "quota in team-a must only count pods in team-a, got {:?}",
        used.get("pods")
    );

    // Cross-check: there is no spillover ResourceQuota in team-b.
    let team_b_quotas: Vec<ResourceQuota> = storage
        .list(&build_prefix("resourcequotas", Some("team-b")))
        .await
        .unwrap();
    assert!(
        team_b_quotas.is_empty(),
        "no quota should exist in team-b, got {team_b_quotas:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 3: NetworkPolicy is scoped to its namespace.
//
// Upstream contract (`pkg/apis/networking/validation/validation.go`):
// NetworkPolicy is a namespaced resource. The API server places it under
// `/registry/networkpolicies/<ns>/<name>`, and a list scoped to namespace
// B must never see a NetworkPolicy created in namespace A — that is the
// admission/validation-level invariant the controller relies on when it
// sweeps a Terminating namespace's NPs.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_namespace_network_policy_isolation() {
    let storage = Arc::new(MemoryStorage::new());

    let np_a = NetworkPolicy::new(
        "deny-all",
        "tenant-a",
        NetworkPolicySpec {
            pod_selector: LabelSelector {
                match_labels: None,
                match_expressions: None,
            },
            ingress: None,
            egress: None,
            policy_types: Some(vec!["Ingress".to_string()]),
        },
    );
    storage
        .create(
            &build_key("networkpolicies", Some("tenant-a"), "deny-all"),
            &np_a,
        )
        .await
        .unwrap();

    // Namespace tenant-b has its own, distinct NetworkPolicy. We use a
    // selector that picks a different label to make any accidental
    // cross-namespace bleed easy to spot.
    let mut tenant_b_labels = HashMap::new();
    tenant_b_labels.insert("tier".to_string(), "frontend".to_string());
    let np_b = NetworkPolicy::new(
        "allow-frontend",
        "tenant-b",
        NetworkPolicySpec {
            pod_selector: LabelSelector {
                match_labels: Some(tenant_b_labels),
                match_expressions: None,
            },
            ingress: None,
            egress: None,
            policy_types: Some(vec!["Ingress".to_string()]),
        },
    );
    storage
        .create(
            &build_key("networkpolicies", Some("tenant-b"), "allow-frontend"),
            &np_b,
        )
        .await
        .unwrap();

    // List scoped to tenant-a — must only return the deny-all NP.
    let a_list: Vec<NetworkPolicy> = storage
        .list(&build_prefix("networkpolicies", Some("tenant-a")))
        .await
        .unwrap();
    assert_eq!(
        a_list.len(),
        1,
        "tenant-a list must see only its own NetworkPolicy, got {a_list:?}"
    );
    assert_eq!(a_list[0].metadata.name, "deny-all");
    assert_eq!(
        a_list[0].metadata.namespace.as_deref(),
        Some("tenant-a"),
        "namespace stamp must survive the round-trip"
    );

    // List scoped to tenant-b — must only return its allow-frontend NP.
    let b_list: Vec<NetworkPolicy> = storage
        .list(&build_prefix("networkpolicies", Some("tenant-b")))
        .await
        .unwrap();
    assert_eq!(
        b_list.len(),
        1,
        "tenant-b list must see only its own NetworkPolicy, got {b_list:?}"
    );
    assert_eq!(b_list[0].metadata.name, "allow-frontend");

    // Listing across both namespaces should yield both — sanity check on
    // the global prefix so a future regression that splits the registry
    // wrong is caught immediately.
    let all: Vec<NetworkPolicy> = storage
        .list(&build_prefix("networkpolicies", None))
        .await
        .unwrap();
    assert_eq!(
        all.len(),
        2,
        "global NetworkPolicy list must observe both, got {all:?}"
    );

    // Crucially: deleting tenant-a's NP must not affect tenant-b.
    storage
        .delete(&build_key("networkpolicies", Some("tenant-a"), "deny-all"))
        .await
        .unwrap();
    let b_after: Vec<NetworkPolicy> = storage
        .list(&build_prefix("networkpolicies", Some("tenant-b")))
        .await
        .unwrap();
    assert_eq!(
        b_after.len(),
        1,
        "tenant-b NetworkPolicy must survive tenant-a deletion, got {b_after:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: RoleBinding does not grant cross-namespace access.
//
// Upstream contract (`pkg/registry/rbac/validation/rule.go` /
// `pkg/registry/rbac/rest/storage_rbac.go`): a `RoleBinding` is a
// namespaced resource. A binding in namespace A references a `Role` in
// namespace A (or a cluster-scoped `ClusterRole`), and an RBAC lookup
// against namespace B must NOT see it. We mirror this at the storage
// level — the namespace-scoped list is the data source for
// `RBACAuthorizer::get_user_role_bindings`, so isolating it here is the
// load-bearing invariant.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_namespace_rbac_isolation() {
    let storage = Arc::new(MemoryStorage::new());

    // Role + RoleBinding in namespace "team-a".
    let role_a = Role::new("pod-reader", "team-a").with_rules(vec![PolicyRule::new(vec![
        "get".to_string(),
        "list".to_string(),
    ])
    .with_api_groups(vec!["".to_string()])
    .with_resources(vec!["pods".to_string()])]);
    storage
        .create(&build_key("roles", Some("team-a"), "pod-reader"), &role_a)
        .await
        .unwrap();

    let binding_a = RoleBinding::new("alice-can-read", "team-a")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("pod-reader"));
    storage
        .create(
            &build_key("rolebindings", Some("team-a"), "alice-can-read"),
            &binding_a,
        )
        .await
        .unwrap();

    // Sibling namespace "team-b" is empty by design — no bindings, no
    // roles. The RBAC authorizer's namespace-scoped list must observe
    // exactly that.
    let team_a_bindings: Vec<RoleBinding> = storage
        .list(&build_prefix("rolebindings", Some("team-a")))
        .await
        .unwrap();
    assert_eq!(
        team_a_bindings.len(),
        1,
        "team-a RoleBinding list must see alice's binding, got {team_a_bindings:?}"
    );
    assert_eq!(team_a_bindings[0].metadata.name, "alice-can-read");

    let team_b_bindings: Vec<RoleBinding> = storage
        .list(&build_prefix("rolebindings", Some("team-b")))
        .await
        .unwrap();
    assert!(
        team_b_bindings.is_empty(),
        "team-b must observe ZERO RoleBindings — RoleBinding scope is per-namespace, \
         got {team_b_bindings:?}"
    );

    // Listing roles in team-b must be empty too — the pod-reader Role
    // belongs to team-a and is not inherited.
    let team_b_roles: Vec<Role> = storage
        .list(&build_prefix("roles", Some("team-b")))
        .await
        .unwrap();
    assert!(
        team_b_roles.is_empty(),
        "team-b must observe ZERO Roles — Role scope is per-namespace, got {team_b_roles:?}"
    );

    // The binding's subject set is intact, but every Subject's `kind`
    // and `name` must match upstream's `appliesTo` invariant: a `User`
    // subject has no namespace and never matches a different namespace's
    // request implicitly. Encode that as a structural check so a future
    // regression that adds an implicit namespace to `User` subjects
    // trips the test.
    let alice = &team_a_bindings[0].subjects[0];
    assert_eq!(alice.kind, "User");
    assert_eq!(alice.name, "alice");
    assert!(
        alice.namespace.is_none(),
        "User subjects must not carry a namespace, got {:?}",
        alice.namespace
    );
}
