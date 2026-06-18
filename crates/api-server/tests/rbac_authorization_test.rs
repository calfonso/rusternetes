//! Phase 6.1 — RBAC authorization coverage for the api-server crate.
//!
//! Upstream mirror: `kubernetes/test/e2e/auth/rbac.go` (release-1.35) and the
//! companion validation suite at `pkg/registry/rbac/validation/`. RBAC is
//! enforced inside the api-server authorizer pipeline, so these tests drive
//! the production routes (`/apis/rbac.authorization.k8s.io/v1/...`) and the
//! `SubjectAccessReview` endpoint to assert the authorizer decision surface.
//!
//! The harness wires `ApiServerState` with the real
//! `rusternetes_common::authz::RBACAuthorizer` backed by a fresh
//! `MemoryStorage`. `skip_auth = true` injects the calling identity
//! `admin`/`system:masters`; a pre-seeded `ClusterRoleBinding` named
//! `test-bootstrap-admin` grants that identity `cluster-admin`-equivalent
//! `*/*/*` rules so that the SAR endpoint's caller-side authorize check
//! succeeds. The *interesting* authorization decision is then the inner
//! `state.authorizer.authorize(&check_attrs)` call which evaluates the
//! `SubjectAccessReview.spec.user`'s rights — that is the surface every test
//! below exercises.
//!
//! ### RED-state pins
//!
//! - `clusterrole_aggregation_collects_rules_from_labelled_clusterroles` —
//!   upstream `pkg/registry/rbac/clusterrole/policybased` materialises the
//!   aggregated `rules` field by listing every `ClusterRole` whose labels
//!   match the parent's `aggregationRule.clusterRoleSelectors`. The
//!   rusternetes api-server currently stores the parent with empty `rules`
//!   and does not perform aggregation, so the assertion that an aggregated
//!   verb is granted is `#[ignore]`d.
//! - `rolebinding_create_blocked_when_caller_lacks_escalate` — upstream
//!   `pkg/registry/rbac/rest/rest.go` validates that the caller possesses
//!   every PolicyRule contained in the bound Role (or the `escalate` verb)
//!   before allowing a `RoleBinding` POST. The rusternetes handler currently
//!   has no escalation check, so the assertion that the POST returns 403 is
//!   `#[ignore]`d.

#![allow(clippy::too_many_lines)]

use rusternetes_common::{
    auth::UserInfo,
    authz::{Authorizer, Decision, RBACAuthorizer, RequestAttributes},
    resources::{ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject},
    types::{ObjectMeta, TypeMeta},
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP / authorizer harness
// ---------------------------------------------------------------------------

/// Build an `ApiServerState` wired with the real `RBACAuthorizer`, plus a
/// `StorageBackend::Memory` (returned alongside as the typed
/// `Arc<MemoryStorage>` for direct seeding). `skip_auth = true` injects the
/// `admin`/`system:masters` calling identity into every request; a
/// `ClusterRoleBinding` granting `system:masters` wildcard access is seeded
/// so that the SAR endpoint's caller-side `authorize(create
/// subjectaccessreviews)` does not 403 before the *interesting* decision
/// runs.
async fn spawn_state() -> (TestApiServer, Arc<MemoryStorage>, Arc<StorageBackend>) {
    let api = TestApiServer::builder()
        .rbac()
        .secret(b"rbac-authz-test-secret")
        .build();
    let mem = api.storage.clone();
    let backend = Arc::new(StorageBackend::Memory(mem.clone()));

    seed_bootstrap_cluster_admin(&mem).await;
    (api, mem, backend)
}

/// Seed a `ClusterRole` + `ClusterRoleBinding` pair granting the `system:masters`
/// group full wildcard access. Mirrors the upstream `cluster-admin` bootstrap
/// at `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go` (release-1.35,
/// `ClusterRoleBindings()` → `system:masters` → `cluster-admin`).
async fn seed_bootstrap_cluster_admin(mem: &Arc<MemoryStorage>) {
    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "test-bootstrap-cluster-admin".into(),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            verbs: vec!["*".into()],
            api_groups: Some(vec!["*".into()]),
            resources: Some(vec!["*".into()]),
            resource_names: None,
            non_resource_urls: None,
        }],
        aggregation_rule: None,
    };
    mem.create(
        &build_key("clusterroles", None, "test-bootstrap-cluster-admin"),
        &cr,
    )
    .await
    .unwrap();

    let crb = ClusterRoleBinding {
        type_meta: TypeMeta {
            kind: "ClusterRoleBinding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "test-bootstrap-admin".into(),
            ..Default::default()
        },
        subjects: vec![Subject {
            kind: "Group".into(),
            name: "system:masters".into(),
            api_group: Some("rbac.authorization.k8s.io".into()),
            namespace: None,
        }],
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: "test-bootstrap-cluster-admin".into(),
        },
    };
    mem.create(
        &build_key("clusterrolebindings", None, "test-bootstrap-admin"),
        &crb,
    )
    .await
    .unwrap();
}

/// Like [`spawn_state`], but the seeded bootstrap binding grants the calling
/// `admin`/`system:masters` identity only the rights needed to *manage* RBAC
/// objects and create `SubjectAccessReview`s — explicitly NOT a `*/*/*`
/// cluster-admin. This models upstream's privilege-escalation scenario where
/// the caller can POST a RoleBinding (passing the outer `create rolebindings`
/// authorization) yet does not already hold the rules contained in the bound
/// role and lacks the `escalate` verb. With a full cluster-admin bootstrap the
/// escalation superset check is trivially satisfied, so this scoped caller is
/// required to exercise the 403 path.
async fn spawn_state_rbac_admin_only() -> (TestApiServer, Arc<MemoryStorage>, Arc<StorageBackend>) {
    let api = TestApiServer::builder()
        .rbac()
        .secret(b"rbac-authz-test-secret")
        .build();
    let mem = api.storage.clone();
    let backend = Arc::new(StorageBackend::Memory(mem.clone()));

    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "test-bootstrap-rbac-admin".into(),
            ..Default::default()
        },
        rules: vec![
            // Manage RBAC objects (lets the caller POST the RoleBinding) — but
            // crucially withOUT the `escalate` verb, so binding a role whose
            // rules the caller lacks must be rejected.
            PolicyRule {
                verbs: vec![
                    "get".into(),
                    "list".into(),
                    "watch".into(),
                    "create".into(),
                    "update".into(),
                    "delete".into(),
                ],
                api_groups: Some(vec!["rbac.authorization.k8s.io".into()]),
                resources: Some(vec![
                    "roles".into(),
                    "rolebindings".into(),
                    "clusterroles".into(),
                    "clusterrolebindings".into(),
                ]),
                resource_names: None,
                non_resource_urls: None,
            },
            // Allow the SAR endpoint's caller-side authorize check.
            PolicyRule {
                verbs: vec!["create".into()],
                api_groups: Some(vec!["authorization.k8s.io".into()]),
                resources: Some(vec!["subjectaccessreviews".into()]),
                resource_names: None,
                non_resource_urls: None,
            },
        ],
        aggregation_rule: None,
    };
    mem.create(
        &build_key("clusterroles", None, "test-bootstrap-rbac-admin"),
        &cr,
    )
    .await
    .unwrap();

    let crb = ClusterRoleBinding {
        type_meta: TypeMeta {
            kind: "ClusterRoleBinding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "test-bootstrap-rbac-admin-binding".into(),
            ..Default::default()
        },
        subjects: vec![Subject {
            kind: "Group".into(),
            name: "system:masters".into(),
            api_group: Some("rbac.authorization.k8s.io".into()),
            namespace: None,
        }],
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: "test-bootstrap-rbac-admin".into(),
        },
    };
    mem.create(
        &build_key(
            "clusterrolebindings",
            None,
            "test-bootstrap-rbac-admin-binding",
        ),
        &crb,
    )
    .await
    .unwrap();

    (api, mem, backend)
}

async fn post_json(state: TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = state.post(uri, body).await;
    (status.as_u16(), value)
}

async fn put_json(state: TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = state.put(uri, body).await;
    (status.as_u16(), value)
}

/// Build a `SubjectAccessReview` JSON body for the given principal + verb +
/// `(group, resource, namespace)` triple. Used everywhere we want the api-server
/// to compute "is `user` allowed to `verb` `resource` in `namespace`" via the
/// real `RBACAuthorizer`.
fn sar_body(
    user: &str,
    groups: &[&str],
    verb: &str,
    api_group: &str,
    resource: &str,
    namespace: Option<&str>,
) -> Value {
    let mut resource_attrs = json!({
        "verb": verb,
        "group": api_group,
        "resource": resource,
        "version": "v1",
    });
    if let Some(ns) = namespace {
        resource_attrs["namespace"] = json!(ns);
    }
    json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "metadata": {},
        "spec": {
            "user": user,
            "groups": groups,
            "resourceAttributes": resource_attrs,
        }
    })
}

/// Drive a SAR through the api-server and return `(allowed, reason)`.
async fn ask_sar(
    state: TestApiServer,
    user: &str,
    groups: &[&str],
    verb: &str,
    api_group: &str,
    resource: &str,
    namespace: Option<&str>,
) -> (bool, String) {
    let body = sar_body(user, groups, verb, api_group, resource, namespace);
    let (status, resp) = post_json(
        state,
        "/apis/authorization.k8s.io/v1/subjectaccessreviews",
        &body,
    )
    .await;
    assert_eq!(status, 200, "SAR HTTP must succeed, got {status}: {resp}");
    let allowed = resp["status"]["allowed"].as_bool().unwrap_or(false);
    let reason = resp["status"]["reason"].as_str().unwrap_or("").to_string();
    (allowed, reason)
}

// ---------------------------------------------------------------------------
// Test 1 — ClusterRole aggregation
// Upstream: pkg/registry/rbac/clusterrole/policybased + test/e2e/auth/rbac.go
//           "should support ClusterRoleAggregation" Conformance descriptor.
// ---------------------------------------------------------------------------

/// A parent `ClusterRole` carrying an `aggregationRule.clusterRoleSelectors`
/// must materialise the union of every child `ClusterRole`'s rules whose
/// labels match the selector. Upstream stamps this server-side in the
/// `clusterrole/policybased` storage layer (release-1.35). Until rusternetes
/// performs aggregation, the parent stores empty `rules` and a SAR against the
/// aggregated verb falls through to deny.
#[tokio::test]
async fn clusterrole_aggregation_collects_rules_from_labelled_clusterroles() {
    let (state, _mem, _backend) = spawn_state().await;

    // Child ClusterRole with a recognisable label + a pod-reader rule.
    let child = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {
            "name": "aggregate-child-pod-reader",
            "labels": { "rbac.example.com/aggregate-to-view": "true" }
        },
        "rules": [{
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["get", "list", "watch"]
        }]
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterroles",
        &child,
    )
    .await;
    assert_eq!(status, 201, "create child ClusterRole");

    // Parent ClusterRole with only an aggregationRule (no rules).
    let parent = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "aggregate-parent-view"},
        "rules": [],
        "aggregationRule": {
            "clusterRoleSelectors": [{
                "matchLabels": { "rbac.example.com/aggregate-to-view": "true" }
            }]
        }
    });
    let (status, created) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterroles",
        &parent,
    )
    .await;
    assert_eq!(status, 201, "create parent ClusterRole: {created}");

    // Bind the parent to a user via ClusterRoleBinding.
    let crb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "viewer-binding"},
        "subjects": [{
            "kind": "User",
            "name": "viewer",
            "apiGroup": "rbac.authorization.k8s.io"
        }],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "aggregate-parent-view"
        }
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
        &crb,
    )
    .await;
    assert_eq!(status, 201, "create ClusterRoleBinding");

    // The aggregated rules MUST grant "viewer" `get pods` cluster-wide.
    let (allowed, reason) = ask_sar(
        state.clone(),
        "viewer",
        &[],
        "get",
        "",
        "pods",
        Some("default"),
    )
    .await;
    assert!(
        allowed,
        "aggregation must grant `get pods` once child labels match (reason: {reason})"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — Privilege-escalation prevention on RoleBinding create
// Upstream: pkg/registry/rbac/rest/rest.go ValidateBinding + test/e2e/auth/rbac.go
//           "should not allow RoleBinding creating without escalating".
// ---------------------------------------------------------------------------

/// Upstream rejects a RoleBinding POST when the caller does not already possess
/// every rule contained in the bound Role and does not hold the synthetic
/// `escalate` verb. The api-server should respond with `403 Forbidden`. Our
/// handler currently accepts the binding unconditionally, so this is RED-state.
#[tokio::test]
async fn rolebinding_create_blocked_when_caller_lacks_escalate() {
    let (state, _mem, _backend) = spawn_state_rbac_admin_only().await;
    let ns = "rbac-escalation";

    // A powerful Role: full secret access in the namespace.
    let role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": "secret-master"},
        "rules": [{
            "apiGroups": [""],
            "resources": ["secrets"],
            "verbs": ["get", "list", "create", "update", "delete"]
        }]
    });
    let (status, _) = post_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles"),
        &role,
    )
    .await;
    assert_eq!(status, 201, "create Role");

    // A low-privilege user (impersonated via the SAR endpoint will be
    // checked separately). Here we simulate the upstream check by attempting
    // to bind `secret-master` to `mallory` while the *real* caller does not
    // possess equivalent rules.
    //
    // The skip_auth admin identity bypasses our home-grown bootstrap-binding
    // grant; upstream's `policybased` storage layer would catch the missing
    // `escalate` verb here. Until we implement that, the POST currently
    // succeeds — which is exactly what this RED-state pin documents.
    //
    // For the pin to GREEN, the future implementation MUST: (a) detect that
    // the request user lacks any of the rules in `secret-master`, (b) check
    // for the `escalate` verb on `rolebindings`, and (c) return 403 when
    // neither holds.
    let rb_body = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "mallory-secret-binding"},
        "subjects": [{
            "kind": "User",
            "name": "mallory",
            "apiGroup": "rbac.authorization.k8s.io"
        }],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "secret-master"
        }
    });

    // Per upstream, when the caller lacks `escalate`, this POST is rejected
    // with a 403 + an error message naming the missing rule. We assert the
    // 403 here; the message contract is whatever upstream uses
    // ("user X cannot escalate to role Y").
    let (status, body) = post_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings"),
        &rb_body,
    )
    .await;
    assert_eq!(
        status, 403,
        "escalation MUST be blocked without the `escalate` verb, got {status}: {body}"
    );
}

/// Upstream's `rolebinding/policybased` storage runs the same
/// `ConfirmNoEscalation` check on UPDATE as on CREATE: the bound role's rules
/// may be granted to (possibly new) subjects, so the updater must already hold
/// those rules or the `escalate` verb. `roleRef` is immutable, so we seed an
/// existing binding and PUT it back unchanged (only the subject list grows);
/// without `escalate` the caller still lacks the bound `secret-master` rules,
/// so the update must 403.
#[tokio::test]
async fn rolebinding_update_blocked_when_caller_lacks_escalate() {
    let (state, mem, _backend) = spawn_state_rbac_admin_only().await;
    let ns = "rbac-escalation-update";

    // A powerful Role: full secret access in the namespace.
    let role = Role {
        type_meta: TypeMeta {
            kind: "Role".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "secret-master".into(),
            namespace: Some(ns.into()),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            verbs: vec!["get".into(), "list".into(), "create".into()],
            api_groups: Some(vec!["".into()]),
            resources: Some(vec!["secrets".into()]),
            resource_names: None,
            non_resource_urls: None,
        }],
    };
    mem.create(&build_key("roles", Some(ns), "secret-master"), &role)
        .await
        .unwrap();

    // Seed an existing RoleBinding directly, bypassing the create-time gate.
    let existing = RoleBinding {
        type_meta: TypeMeta {
            kind: "RoleBinding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "mallory-secret-binding".into(),
            namespace: Some(ns.into()),
            ..Default::default()
        },
        subjects: vec![Subject {
            kind: "User".into(),
            name: "mallory".into(),
            api_group: Some("rbac.authorization.k8s.io".into()),
            namespace: None,
        }],
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "Role".into(),
            name: "secret-master".into(),
        },
    };
    mem.create(
        &build_key("rolebindings", Some(ns), "mallory-secret-binding"),
        &existing,
    )
    .await
    .unwrap();

    // PUT the binding back with an additional subject (roleRef unchanged, so the
    // immutability check passes and the escalation gate is reached).
    let rb_body = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "mallory-secret-binding", "namespace": ns},
        "subjects": [
            {"kind": "User", "name": "mallory", "apiGroup": "rbac.authorization.k8s.io"},
            {"kind": "User", "name": "eve", "apiGroup": "rbac.authorization.k8s.io"}
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "secret-master"
        }
    });
    let (status, body) = put_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings/mallory-secret-binding"),
        &rb_body,
    )
    .await;
    assert_eq!(
        status, 403,
        "RoleBinding update escalation MUST be blocked without `escalate`, got {status}: {body}"
    );
}

/// `clusterrolebinding/policybased` runs `ConfirmNoEscalation` on CREATE at
/// cluster scope (empty binding namespace). Binding a `ClusterRole` whose rules
/// the caller lacks, without the `escalate` verb, must 403.
#[tokio::test]
async fn clusterrolebinding_create_blocked_when_caller_lacks_escalate() {
    let (state, mem, _backend) = spawn_state_rbac_admin_only().await;

    // A powerful ClusterRole: cluster-wide secret access.
    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "cluster-secret-master".into(),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            verbs: vec!["get".into(), "list".into(), "create".into()],
            api_groups: Some(vec!["".into()]),
            resources: Some(vec!["secrets".into()]),
            resource_names: None,
            non_resource_urls: None,
        }],
        aggregation_rule: None,
    };
    mem.create(
        &build_key("clusterroles", None, "cluster-secret-master"),
        &cr,
    )
    .await
    .unwrap();

    let crb_body = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "mallory-cluster-secret-binding"},
        "subjects": [
            {"kind": "User", "name": "mallory", "apiGroup": "rbac.authorization.k8s.io"}
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "cluster-secret-master"
        }
    });
    let (status, body) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
        &crb_body,
    )
    .await;
    assert_eq!(
        status, 403,
        "ClusterRoleBinding create escalation MUST be blocked without `escalate`, got {status}: {body}"
    );
}

/// `clusterrolebinding/policybased` runs the same escalation check on UPDATE.
/// Seed an existing binding and PUT it back (roleRef immutable) with a new
/// subject; without `escalate` and lacking the bound rules, the update 403s.
#[tokio::test]
async fn clusterrolebinding_update_blocked_when_caller_lacks_escalate() {
    let (state, mem, _backend) = spawn_state_rbac_admin_only().await;

    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "cluster-secret-master".into(),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            verbs: vec!["get".into(), "list".into(), "create".into()],
            api_groups: Some(vec!["".into()]),
            resources: Some(vec!["secrets".into()]),
            resource_names: None,
            non_resource_urls: None,
        }],
        aggregation_rule: None,
    };
    mem.create(
        &build_key("clusterroles", None, "cluster-secret-master"),
        &cr,
    )
    .await
    .unwrap();

    let existing = ClusterRoleBinding {
        type_meta: TypeMeta {
            kind: "ClusterRoleBinding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "mallory-cluster-secret-binding".into(),
            ..Default::default()
        },
        subjects: vec![Subject {
            kind: "User".into(),
            name: "mallory".into(),
            api_group: Some("rbac.authorization.k8s.io".into()),
            namespace: None,
        }],
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: "cluster-secret-master".into(),
        },
    };
    mem.create(
        &build_key(
            "clusterrolebindings",
            None,
            "mallory-cluster-secret-binding",
        ),
        &existing,
    )
    .await
    .unwrap();

    let crb_body = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "mallory-cluster-secret-binding"},
        "subjects": [
            {"kind": "User", "name": "mallory", "apiGroup": "rbac.authorization.k8s.io"},
            {"kind": "User", "name": "eve", "apiGroup": "rbac.authorization.k8s.io"}
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "cluster-secret-master"
        }
    });
    let (status, body) = put_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/mallory-cluster-secret-binding",
        &crb_body,
    )
    .await;
    assert_eq!(
        status, 403,
        "ClusterRoleBinding update escalation MUST be blocked without `escalate`, got {status}: {body}"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — Wildcard verb / resource expansion in PolicyRule
// Upstream: pkg/registry/rbac/validation/rule.go RuleAllows.
// ---------------------------------------------------------------------------

/// A `PolicyRule` whose `verbs`, `resources`, or `apiGroups` contains `"*"`
/// MUST match every concrete value of that axis. We assert all three axes via
/// SAR, sourcing the calling user from a ClusterRoleBinding that points at a
/// wildcard `ClusterRole`.
#[tokio::test]
async fn rbac_wildcard_in_verbs_resources_and_api_groups_grants_everything() {
    let (state, _mem, _backend) = spawn_state().await;

    let wild = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "wildcard-everything"},
        "rules": [{
            "apiGroups": ["*"],
            "resources": ["*"],
            "verbs": ["*"]
        }]
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterroles",
        &wild,
    )
    .await;
    assert_eq!(status, 201, "create wildcard ClusterRole");

    let crb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "wild-binding"},
        "subjects": [{
            "kind": "User",
            "name": "wildcat",
            "apiGroup": "rbac.authorization.k8s.io"
        }],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "wildcard-everything"
        }
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
        &crb,
    )
    .await;
    assert_eq!(status, 201, "create wildcard ClusterRoleBinding");

    // Verb wildcard: should match `delete pods`.
    let (allowed, reason) = ask_sar(
        state.clone(),
        "wildcat",
        &[],
        "delete",
        "",
        "pods",
        Some("default"),
    )
    .await;
    assert!(
        allowed,
        "verb=* must match `delete pods` (reason: {reason})"
    );

    // Resource wildcard: should match a non-core resource like `deployments`
    // in the `apps` group (apiGroups=* combined with resources=*).
    let (allowed, reason) = ask_sar(
        state.clone(),
        "wildcat",
        &[],
        "list",
        "apps",
        "deployments",
        Some("default"),
    )
    .await;
    assert!(
        allowed,
        "resource=* + apiGroups=* must match `list deployments.apps` (reason: {reason})"
    );

    // apiGroups wildcard: should match the `rbac.authorization.k8s.io` group.
    let (allowed, reason) = ask_sar(
        state.clone(),
        "wildcat",
        &[],
        "get",
        "rbac.authorization.k8s.io",
        "roles",
        Some("default"),
    )
    .await;
    assert!(
        allowed,
        "apiGroups=* must match the rbac.authorization.k8s.io group (reason: {reason})"
    );

    // Sanity counter-example: a user with no binding gets denied.
    let (allowed, reason) = ask_sar(
        state.clone(),
        "stranger",
        &[],
        "list",
        "",
        "pods",
        Some("default"),
    )
    .await;
    assert!(
        !allowed,
        "user without any binding must be denied (reason: {reason})"
    );
}

// ---------------------------------------------------------------------------
// Test 4 — Subject kinds (User / Group / ServiceAccount)
// Upstream: pkg/registry/rbac/validation/rule.go subjectsAppliesTo +
//           test/e2e/auth/rbac.go subject permutations.
// ---------------------------------------------------------------------------

/// `RoleBinding.subjects[].kind` must match exactly what upstream specifies:
/// * `User`           — `subject.name == user.username`
/// * `Group`          — `subject.name ∈ user.groups`
/// * `ServiceAccount` — `username == "system:serviceaccount:<ns>:<name>"`
#[tokio::test]
async fn rbac_subject_kinds_user_group_and_serviceaccount() {
    let (state, _mem, _backend) = spawn_state().await;
    let ns = "rbac-subjects";

    // Role granting `get configmaps`.
    let role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": "cm-reader"},
        "rules": [{
            "apiGroups": [""],
            "resources": ["configmaps"],
            "verbs": ["get"]
        }]
    });
    let (status, _) = post_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles"),
        &role,
    )
    .await;
    assert_eq!(status, 201, "create Role");

    // Bind the Role to three principals at once.
    let rb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "cm-reader-binding"},
        "subjects": [
            {"kind": "User",  "name": "alice", "apiGroup": "rbac.authorization.k8s.io"},
            {"kind": "Group", "name": "ops",   "apiGroup": "rbac.authorization.k8s.io"},
            {"kind": "ServiceAccount", "name": "bot", "namespace": ns}
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "cm-reader"
        }
    });
    let (status, _) = post_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings"),
        &rb,
    )
    .await;
    assert_eq!(status, 201, "create RoleBinding");

    // User principal — username exactly "alice".
    let (allowed, _) = ask_sar(
        state.clone(),
        "alice",
        &[],
        "get",
        "",
        "configmaps",
        Some(ns),
    )
    .await;
    assert!(allowed, "User subject must match by username");

    // Group principal — username irrelevant, group must contain "ops".
    let (allowed, _) = ask_sar(
        state.clone(),
        "any-user",
        &["ops"],
        "get",
        "",
        "configmaps",
        Some(ns),
    )
    .await;
    assert!(
        allowed,
        "Group subject must match when user is in the group"
    );

    // ServiceAccount principal — username must be
    // `system:serviceaccount:<ns>:<name>`, regardless of supplied groups.
    let sa_user = format!("system:serviceaccount:{ns}:bot");
    let (allowed, _) = ask_sar(
        state.clone(),
        &sa_user,
        &[],
        "get",
        "",
        "configmaps",
        Some(ns),
    )
    .await;
    assert!(
        allowed,
        "ServiceAccount subject must match by the system:serviceaccount:<ns>:<name> principal"
    );

    // Negative: a stranger principal (no matching subject) must be denied.
    let (allowed, reason) = ask_sar(
        state.clone(),
        "stranger",
        &["others"],
        "get",
        "",
        "configmaps",
        Some(ns),
    )
    .await;
    assert!(
        !allowed,
        "non-matching principal must be denied (reason: {reason})"
    );
}

// ---------------------------------------------------------------------------
// Test 5 — Namespace isolation: Role vs ClusterRole scoping
// Upstream: pkg/registry/rbac/validation/rule.go visitRulesFor.
// ---------------------------------------------------------------------------

/// A `Role` + `RoleBinding` in namespace `A` MUST NOT grant access in
/// namespace `B`. A `ClusterRoleBinding` referencing the same rules MUST grant
/// the equivalent access across every namespace.
#[tokio::test]
async fn rbac_namespace_isolation_role_vs_clusterrole() {
    let (state, _mem, _backend) = spawn_state().await;

    // Role + RoleBinding in namespace `team-a` granting `get pods`.
    let role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": "pods-get"},
        "rules": [{
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["get"]
        }]
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/namespaces/team-a/roles",
        &role,
    )
    .await;
    assert_eq!(status, 201, "create Role in team-a");

    let rb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "alice-pods-get"},
        "subjects": [{"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "pods-get"
        }
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/namespaces/team-a/rolebindings",
        &rb,
    )
    .await;
    assert_eq!(status, 201, "create RoleBinding in team-a");

    // alice CAN get pods in team-a.
    let (allowed, _) = ask_sar(
        state.clone(),
        "alice",
        &[],
        "get",
        "",
        "pods",
        Some("team-a"),
    )
    .await;
    assert!(
        allowed,
        "Role binding must grant access inside the bound namespace"
    );

    // alice CANNOT get pods in team-b — namespace-scoped roles do not leak.
    let (allowed, reason) = ask_sar(
        state.clone(),
        "alice",
        &[],
        "get",
        "",
        "pods",
        Some("team-b"),
    )
    .await;
    assert!(
        !allowed,
        "Role binding in team-a MUST NOT grant access in team-b (reason: {reason})"
    );

    // Now bind a ClusterRole granting the same access. The same user should
    // immediately become authorized cluster-wide.
    let cr = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "cluster-pods-get"},
        "rules": [{
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["get"]
        }]
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterroles",
        &cr,
    )
    .await;
    assert_eq!(status, 201, "create ClusterRole");

    let crb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "alice-cluster-pods-get"},
        "subjects": [{"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "cluster-pods-get"
        }
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
        &crb,
    )
    .await;
    assert_eq!(status, 201, "create ClusterRoleBinding");

    // Now alice CAN get pods in any namespace.
    let (allowed, _) = ask_sar(
        state.clone(),
        "alice",
        &[],
        "get",
        "",
        "pods",
        Some("team-b"),
    )
    .await;
    assert!(allowed, "ClusterRoleBinding must grant access cluster-wide");
    let (allowed, _) = ask_sar(
        state.clone(),
        "alice",
        &[],
        "get",
        "",
        "pods",
        Some("kube-system"),
    )
    .await;
    assert!(
        allowed,
        "ClusterRoleBinding must grant access in every namespace"
    );
}

// ---------------------------------------------------------------------------
// Test 6 — Non-resource URL rules (e.g. /healthz, /metrics)
// Upstream: pkg/registry/rbac/validation/rule.go RuleAllowsNonResourceURL.
// ---------------------------------------------------------------------------

/// `PolicyRule.nonResourceURLs` MUST be honoured by the authorizer for
/// requests flagged as non-resource (path-only). The check is verb-scoped:
/// the rule's `verbs` must contain the request verb (`get` for /healthz). We
/// drive this directly against `RBACAuthorizer::authorize` because the SAR
/// endpoint surfaces only resource attributes — upstream's
/// `SubjectAccessReview` has a separate `nonResourceAttributes` carrier which
/// the rusternetes handler already reads.
#[tokio::test]
async fn rbac_non_resource_url_rule_grants_path_get() {
    let (_state, mem, backend) = spawn_state().await;

    // Bind `system:authenticated` group to a ClusterRole that grants
    // `get /healthz`.
    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "healthz-reader".into(),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            verbs: vec!["get".into()],
            api_groups: None,
            resources: None,
            resource_names: None,
            non_resource_urls: Some(vec!["/healthz".into(), "/livez".into()]),
        }],
        aggregation_rule: None,
    };
    mem.create(&build_key("clusterroles", None, "healthz-reader"), &cr)
        .await
        .unwrap();
    let crb = ClusterRoleBinding {
        type_meta: TypeMeta {
            kind: "ClusterRoleBinding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "healthz-binding".into(),
            ..Default::default()
        },
        subjects: vec![Subject {
            kind: "Group".into(),
            name: "system:authenticated".into(),
            api_group: Some("rbac.authorization.k8s.io".into()),
            namespace: None,
        }],
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: "healthz-reader".into(),
        },
    };
    mem.create(
        &build_key("clusterrolebindings", None, "healthz-binding"),
        &crb,
    )
    .await
    .unwrap();

    // Drive the RBAC authorizer directly (the SAR endpoint accepts
    // nonResourceAttributes but the JSON shape differs; for unit-level RBAC
    // coverage we use the Authorizer trait that the api-server itself calls).
    let authorizer = RBACAuthorizer::new(backend.clone());
    let user = UserInfo {
        username: "anyone".to_string(),
        uid: "uid-1".to_string(),
        groups: vec!["system:authenticated".to_string()],
        extra: std::collections::HashMap::new(),
    };

    let attrs = RequestAttributes::new_non_resource(user.clone(), "get", "/healthz");
    let decision = authorizer.authorize(&attrs).await.unwrap();
    assert!(
        matches!(decision, Decision::Allow),
        "non-resource rule must grant `get /healthz`, got {decision:?}"
    );

    // The same user is denied on a path not in the rule's nonResourceURLs.
    let attrs = RequestAttributes::new_non_resource(user.clone(), "get", "/readyz");
    let decision = authorizer.authorize(&attrs).await.unwrap();
    assert!(
        matches!(decision, Decision::Deny(_)),
        "non-resource rule must NOT grant /readyz, got {decision:?}"
    );

    // And a user without `system:authenticated` is denied even on the
    // covered path.
    let stranger = UserInfo {
        username: "stranger".to_string(),
        uid: "uid-2".to_string(),
        groups: vec![],
        extra: std::collections::HashMap::new(),
    };
    let attrs = RequestAttributes::new_non_resource(stranger, "get", "/healthz");
    let decision = authorizer.authorize(&attrs).await.unwrap();
    assert!(
        matches!(decision, Decision::Deny(_)),
        "non-resource rule must require a matching subject, got {decision:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 7 — `resourceNames` restriction narrows a PolicyRule to specific names.
// Upstream: pkg/registry/rbac/validation/rule.go ResourceNameMatches.
// ---------------------------------------------------------------------------

/// `PolicyRule.resourceNames` MUST narrow the rule so that it ONLY grants
/// access to the listed names. Other names fall through to the next rule (or
/// deny).
#[tokio::test]
async fn rbac_resource_names_restrict_rule_to_named_objects() {
    let (state, _mem, _backend) = spawn_state().await;
    let ns = "rbac-resource-names";

    // Role that grants `get configmap/whitelisted-cm` only — no list, no other
    // names.
    let role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": "named-cm-reader"},
        "rules": [{
            "apiGroups": [""],
            "resources": ["configmaps"],
            "resourceNames": ["whitelisted-cm"],
            "verbs": ["get"]
        }]
    });
    let (status, _) = post_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles"),
        &role,
    )
    .await;
    assert_eq!(status, 201, "create Role");

    let rb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "alice-named-binding"},
        "subjects": [{"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "named-cm-reader"
        }
    });
    let (status, _) = post_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings"),
        &rb,
    )
    .await;
    assert_eq!(status, 201, "create RoleBinding");

    // Build a SAR with `name=whitelisted-cm` — allowed.
    let mut sar = sar_body("alice", &[], "get", "", "configmaps", Some(ns));
    sar["spec"]["resourceAttributes"]["name"] = json!("whitelisted-cm");
    let (status, resp) = post_json(
        state.clone(),
        "/apis/authorization.k8s.io/v1/subjectaccessreviews",
        &sar,
    )
    .await;
    assert_eq!(status, 200, "SAR HTTP must succeed: {resp}");
    assert_eq!(
        resp["status"]["allowed"], true,
        "named resource MUST be allowed: {resp}"
    );

    // Build a SAR with `name=other-cm` — denied.
    let mut sar = sar_body("alice", &[], "get", "", "configmaps", Some(ns));
    sar["spec"]["resourceAttributes"]["name"] = json!("other-cm");
    let (status, resp) = post_json(
        state.clone(),
        "/apis/authorization.k8s.io/v1/subjectaccessreviews",
        &sar,
    )
    .await;
    assert_eq!(status, 200, "SAR HTTP must succeed: {resp}");
    assert_eq!(
        resp["status"]["allowed"], false,
        "unnamed resource MUST be denied: {resp}"
    );
}

// ---------------------------------------------------------------------------
// Test 8 — Multiple bindings union — additive permissions
// Upstream: pkg/registry/rbac/validation/rule.go visitRulesFor iterates every
// binding and short-circuits on the first allow.
// ---------------------------------------------------------------------------

/// When a user is bound through multiple `(Cluster)RoleBinding`s, the
/// effective permissions are the *union*. We verify by:
///   1. Granting `get pods` via a namespace-scoped RoleBinding.
///   2. Granting `list services` via a ClusterRoleBinding to the same user.
///   3. Asserting both verbs/resources are now allowed.
#[tokio::test]
async fn rbac_multiple_bindings_union_permissions() {
    let (state, _mem, _backend) = spawn_state().await;
    let ns = "rbac-union";

    // Role + RoleBinding for `get pods`.
    let role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": "pods-get"},
        "rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}]
    });
    let (status, _) = post_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles"),
        &role,
    )
    .await;
    assert_eq!(status, 201, "create Role");

    let rb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "bob-pods-get"},
        "subjects": [{"kind": "User", "name": "bob", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "pods-get"
        }
    });
    let (status, _) = post_json(
        state.clone(),
        &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings"),
        &rb,
    )
    .await;
    assert_eq!(status, 201, "create RoleBinding");

    // ClusterRole + ClusterRoleBinding for `list services` cluster-wide.
    let cr = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "services-list"},
        "rules": [{"apiGroups": [""], "resources": ["services"], "verbs": ["list"]}]
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterroles",
        &cr,
    )
    .await;
    assert_eq!(status, 201, "create ClusterRole");

    let crb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "bob-services-list"},
        "subjects": [{"kind": "User", "name": "bob", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "services-list"
        }
    });
    let (status, _) = post_json(
        state.clone(),
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
        &crb,
    )
    .await;
    assert_eq!(status, 201, "create ClusterRoleBinding");

    // Union: bob can get pods (from RoleBinding) AND list services (from CRB).
    let (allowed, _) = ask_sar(state.clone(), "bob", &[], "get", "", "pods", Some(ns)).await;
    assert!(allowed, "RoleBinding rule must apply");

    let (allowed, _) = ask_sar(state.clone(), "bob", &[], "list", "", "services", Some(ns)).await;
    assert!(allowed, "ClusterRoleBinding rule must apply in same NS");

    // Cross-binding negative: bob CANNOT delete pods anywhere.
    let (allowed, reason) =
        ask_sar(state.clone(), "bob", &[], "delete", "", "pods", Some(ns)).await;
    assert!(
        !allowed,
        "union does NOT grant rules absent from every binding (reason: {reason})"
    );
}

// ---------------------------------------------------------------------------
// Helper compile-test: `RoleRef.kind` distinguishes Role vs ClusterRole.
// Documents the type-level contract upstream relies on at validation time.
// ---------------------------------------------------------------------------

#[test]
fn rbac_typed_roleref_distinguishes_role_and_clusterrole() {
    let to_role = RoleRef {
        api_group: "rbac.authorization.k8s.io".into(),
        kind: "Role".into(),
        name: "r".into(),
    };
    let to_cluster_role = RoleRef {
        api_group: "rbac.authorization.k8s.io".into(),
        kind: "ClusterRole".into(),
        name: "cr".into(),
    };
    assert_ne!(to_role.kind, to_cluster_role.kind);
    // Round-trip both through serde — the camelCase mapping is what the
    // wire-format clients rely on.
    let v = serde_json::to_value(&to_role).unwrap();
    assert_eq!(v["apiGroup"], "rbac.authorization.k8s.io");
    assert_eq!(v["kind"], "Role");

    // Subject's `apiGroup` is optional in JSON — round-trip preserves it.
    let s = Subject {
        kind: "ServiceAccount".into(),
        name: "bot".into(),
        api_group: Some("".into()),
        namespace: Some("ns".into()),
    };
    let v = serde_json::to_value(&s).unwrap();
    assert_eq!(v["kind"], "ServiceAccount");
    assert_eq!(v["namespace"], "ns");
    let back: Subject = serde_json::from_value(v).unwrap();
    assert_eq!(back, s);

    // A Role with an empty rule list should still round-trip.
    let role = Role {
        type_meta: TypeMeta {
            kind: "Role".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "empty".into(),
            namespace: Some("ns".into()),
            ..Default::default()
        },
        rules: vec![],
    };
    let back: Role = serde_json::from_value(serde_json::to_value(&role).unwrap()).unwrap();
    assert_eq!(back, role);

    // A RoleBinding with a User subject round-trips with the optional
    // apiGroup field intact.
    let rb = RoleBinding {
        type_meta: TypeMeta {
            kind: "RoleBinding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "rb".into(),
            namespace: Some("ns".into()),
            ..Default::default()
        },
        subjects: vec![Subject {
            kind: "User".into(),
            name: "alice".into(),
            api_group: Some("rbac.authorization.k8s.io".into()),
            namespace: None,
        }],
        role_ref: to_role,
    };
    let back: RoleBinding = serde_json::from_value(serde_json::to_value(&rb).unwrap()).unwrap();
    assert_eq!(back, rb);
}
