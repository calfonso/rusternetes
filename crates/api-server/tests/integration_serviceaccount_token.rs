//! Integration mirror of the Kubernetes v1.35 integration test suite for
//! ServiceAccount token autocreation, automount, and authentication.
//!
//! Upstream source (release-1.35):
//!   https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/serviceaccount/service_account_test.go
//!
//! This file mirrors three integration tests:
//!   - TestServiceAccountAutoCreate            (upstream lines 56–87)
//!   - TestServiceAccountTokenAutoMount        (upstream lines 89–120)
//!   - TestServiceAccountTokenAuthentication   (upstream lines 122–187)
//!
//! Scope and non-duplication
//! -------------------------
//! The adjacent file `conformance_auth_rbac_serviceaccount.rs` covers the
//! sig-auth ServiceAccount **lifecycle + TokenRequest** conformance surface
//! (PUT/PATCH, label-selector list, TokenRequest then TokenReview). This file
//! is exclusively the integration-test mirror for:
//!   * **token autocreation** for the `default` ServiceAccount on namespace
//!     create and recreate-on-delete,
//!   * **automount** of the projected service-account-token volume into pods
//!     that omit `spec.serviceAccountName`, and
//!   * **bearer-token authentication** at the HTTP layer (the upstream test
//!     exercises an `OAuthTokenAuthenticator` against the API server).
//!
//! Harness
//! -------
//! Two harnesses are used:
//!   * `spawn_state()` — `skip_auth = true` + `AlwaysAllowAuthorizer`, drives
//!     the namespace POST / pod POST paths only.
//!   * `spawn_authn_state()` — `skip_auth = false` + `RBACAuthorizer`,
//!     exercises the real bearer-token auth pipeline and per-SA RBAC.
//!
//! Test 1 drives the production
//! `rusternetes_controller_manager::controllers::ServiceAccountController`
//! reconcile loop as a tokio task. The controller's `reconcile_all()` walks
//! every namespace and re-creates the `default` SA if missing — that is the
//! upstream "watch + workqueue + requeue-not-retry" surface, condensed into a
//! periodic ticker so the integration test can assert the recreate.
//!
//! Part of the /batch landing upstream integration-test mirrors.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    authz::{AlwaysAllowAuthorizer, RBACAuthorizer},
    observability::MetricsRegistry,
    resources::{ClusterRole, Namespace, PolicyRule, RoleRef, ServiceAccount, Subject},
    types::{ObjectMeta, TypeMeta},
};
use rusternetes_controller_manager::controllers::serviceaccount::ServiceAccountController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// HTTP harness
// ---------------------------------------------------------------------------

/// Build a fully-wired `ApiServerState` backed by an in-memory storage with
/// `skip_auth = true` and `AlwaysAllow` authorizer. Used by tests 1 and 2.
fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(rusternetes_common::auth::TokenManager::new(
        b"integration-sa-token-secret",
    ));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true, // skip_auth
    ))
}

fn spawn_state() -> Arc<ApiServerState> {
    let mem = Arc::new(MemoryStorage::new());
    make_state(mem)
}

/// Build a wired `ApiServerState` with `skip_auth = false` and an RBAC
/// authorizer. Used by test 3 to exercise the real bearer-token pipeline.
fn spawn_authn_state() -> Arc<ApiServerState> {
    let mem = Arc::new(MemoryStorage::new());
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(rusternetes_common::auth::TokenManager::new(
        b"integration-sa-token-secret",
    ));
    let authorizer = Arc::new(RBACAuthorizer::new(backend.clone()));
    let metrics = Arc::new(MetricsRegistry::new());
    Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        false, // skip_auth — exercise real auth middleware
    ))
}

/// POST JSON, return `(status, body)`.
async fn post_json(router: axum::Router, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// GET JSON, return `(status, body)`.
async fn get_json(router: axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// DELETE, return `(status, body)`.
async fn delete(router: axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// POST JSON with a bearer token, return `(status, body)`.
async fn post_json_bearer(
    router: axum::Router,
    uri: &str,
    token: &str,
    body: &Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// GET with a bearer token, return `(status, body)`.
async fn get_json_bearer(router: axum::Router, uri: &str, token: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// Convenience: POST a Namespace by name.
async fn create_namespace(state: &Arc<ApiServerState>, name: &str) -> (StatusCode, Value) {
    let router = build_router(state.clone(), None);
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": name},
    });
    post_json(router, "/api/v1/namespaces", &body).await
}

// ---------------------------------------------------------------------------
// TestServiceAccountAutoCreate
// ---------------------------------------------------------------------------

/// Upstream: `TestServiceAccountAutoCreate` — release-1.35
/// `test/integration/serviceaccount/service_account_test.go:56-87`.
///
/// 1. Create a namespace.
/// 2. The `default` ServiceAccount is automatically created in it.
/// 3. Delete the `default` ServiceAccount.
/// 4. A new `default` ServiceAccount is automatically created with a
///    different UID. Upstream message:
///    "Expected different UID with recreated serviceaccount."
///
/// Step 2 is satisfied by the synchronous default-SA creation in
/// `crates/api-server/src/handlers/namespace.rs`. Step 4 is driven by the
/// production `ServiceAccountController::reconcile_all` loop (same code
/// path the controller-manager runs in production) spawned as a tokio task
/// inside the test. Mirrors upstream — controller does the work, test polls.
#[tokio::test]
async fn test_service_account_auto_create() {
    let state = spawn_state();

    // Drive the production SA controller as a background reconcile task.
    // We poll `reconcile_all()` rather than the watch-based `run()` because
    // MemoryStorage's watch stream isn't deterministic in this short-lived
    // harness — but `reconcile_all` is the same upstream code path the
    // controller's workqueue worker eventually executes per namespace.
    // Requeue-not-retry: any reconcile error is logged inside
    // `ensure_default_serviceaccount` and the next tick retries naturally,
    // mirroring upstream `workqueue.RateLimitingInterface` semantics.
    let storage = state.storage.clone();
    let controller = Arc::new(ServiceAccountController::new(storage));
    let controller_handle = controller.clone();
    let reconcile_task = tokio::spawn(async move {
        loop {
            let _ = controller_handle.reconcile_all().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });

    let ns = "test-service-account-creation";

    // (1) Create the namespace.
    let (status, body) = create_namespace(&state, ns).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "POST /api/v1/namespaces must return 201: {body}"
    );

    // (2) The `default` ServiceAccount must exist in the new namespace.
    let router = build_router(state.clone(), None);
    let (status, default_sa) = get_json(
        router,
        &format!("/api/v1/namespaces/{ns}/serviceaccounts/default"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "default ServiceAccount must be auto-created: {default_sa}"
    );
    assert_eq!(default_sa["metadata"]["name"], "default");
    assert_eq!(default_sa["metadata"]["namespace"], ns);
    let original_uid = default_sa["metadata"]["uid"]
        .as_str()
        .filter(|u| !u.is_empty())
        .expect("server-assigned UID must be present")
        .to_string();

    // (3) Delete the default ServiceAccount.
    let router = build_router(state.clone(), None);
    let (status, body) = delete(
        router,
        &format!("/api/v1/namespaces/{ns}/serviceaccounts/default"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "DELETE of default SA must return 200: {body}"
    );

    // (4) Poll until the controller recreates the default SA with a fresh
    // UID. Bound at ~2s to keep the test snappy; the controller ticks every
    // 50ms above so a healthy recreation lands within 2–3 ticks.
    let mut recreated_uid: Option<String> = None;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let router = build_router(state.clone(), None);
        let (status, recreated) = get_json(
            router,
            &format!("/api/v1/namespaces/{ns}/serviceaccounts/default"),
        )
        .await;
        if status == StatusCode::OK {
            let new_uid = recreated["metadata"]["uid"].as_str().unwrap_or("");
            if !new_uid.is_empty() && new_uid != original_uid {
                recreated_uid = Some(new_uid.to_string());
                break;
            }
        }
    }

    reconcile_task.abort();

    let new_uid = recreated_uid.expect("controller did not recreate default SA within 2s");
    assert_ne!(
        new_uid, original_uid,
        "Expected different UID with recreated serviceaccount"
    );
}

// ---------------------------------------------------------------------------
// TestServiceAccountTokenAutoMount
// ---------------------------------------------------------------------------

/// Upstream: `TestServiceAccountTokenAutoMount` — release-1.35
/// `test/integration/serviceaccount/service_account_test.go:89-120`.
#[tokio::test]
async fn test_service_account_token_auto_mount() {
    let state = spawn_state();
    let ns = "auto-mount-ns";

    // (1) Namespace.
    let (status, _) = create_namespace(&state, ns).await;
    assert_eq!(status, StatusCode::CREATED, "namespace create must succeed");

    // (2) Pod with no serviceAccountName. Upstream calls this "protopod".
    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "protopod"},
        "spec": {
            "containers": [{
                "name": "container",
                "image": "nginx:latest",
            }],
        },
    });
    let router = build_router(state.clone(), None);
    let (status, created) =
        post_json(router, &format!("/api/v1/namespaces/{ns}/pods"), &pod_body).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "pod create must return 201: {created}"
    );

    // (3) The created pod must default to the "default" ServiceAccount.
    assert_eq!(
        created["spec"]["serviceAccountName"], "default",
        "pod must default ServiceAccountName to \"default\": {created}"
    );

    // (4) The pod must have a projected volume carrying a
    //     ServiceAccountToken projection.
    let volumes = created["spec"]["volumes"].as_array().cloned();
    let volumes = volumes
        .expect("Expected projected volume for service account token inserted (no volumes at all)");
    let has_projected_sa_token = volumes.iter().any(|v| {
        let sources = v["projected"]["sources"].as_array();
        match sources {
            Some(srcs) => srcs.iter().any(|s| !s["serviceAccountToken"].is_null()),
            None => false,
        }
    });
    assert!(
        has_projected_sa_token,
        "Expected projected volume for service account token inserted, got volumes: {volumes:?}"
    );
}

// ---------------------------------------------------------------------------
// TestServiceAccountTokenAuthentication
// ---------------------------------------------------------------------------

/// Seed a ClusterRole + namespaced RoleBinding so the given SA gets the
/// listed verbs on pods in `namespace`. Mirrors the upstream "read-only
/// kubelet client" pattern used by the integration test, but consolidated
/// to ClusterRole + RoleBinding because rusternetes' RBAC checker walks
/// both surfaces.
async fn grant_sa_pods_verbs(
    state: &Arc<ApiServerState>,
    role_name: &str,
    namespace: &str,
    sa_name: &str,
    verbs: &[&str],
) {
    let cluster_role = ClusterRole {
        type_meta: TypeMeta {
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
            kind: "ClusterRole".to_string(),
        },
        metadata: ObjectMeta {
            name: role_name.to_string(),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            verbs: verbs.iter().map(|v| (*v).to_string()).collect(),
            api_groups: Some(vec!["".to_string()]),
            resources: Some(vec!["pods".to_string()]),
            resource_names: None,
            non_resource_urls: None,
        }],
        aggregation_rule: None,
    };
    let key = build_key("clusterroles", None::<&str>, role_name);
    state.storage.create(&key, &cluster_role).await.unwrap();

    let binding = rusternetes_common::resources::RoleBinding {
        type_meta: TypeMeta {
            api_version: "rbac.authorization.k8s.io/v1".to_string(),
            kind: "RoleBinding".to_string(),
        },
        metadata: ObjectMeta {
            name: format!("{role_name}-bind"),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        subjects: vec![Subject::service_account(sa_name, namespace)],
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "ClusterRole".to_string(),
            name: role_name.to_string(),
        },
    };
    let key = build_key(
        "rolebindings",
        Some(namespace),
        &format!("{role_name}-bind"),
    );
    state.storage.create(&key, &binding).await.unwrap();
}

/// Seed a Namespace directly through storage (used by tests that disable
/// `skip_auth` and have no privileged client to talk through the HTTP API).
async fn seed_namespace(state: &Arc<ApiServerState>, name: &str) {
    let ns = Namespace {
        type_meta: TypeMeta {
            api_version: "v1".to_string(),
            kind: "Namespace".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            uid: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        },
        spec: None,
        status: None,
    };
    let key = build_key("namespaces", None::<&str>, name);
    state.storage.create(&key, &ns).await.unwrap();
}

/// Seed a ServiceAccount directly through storage and return the assigned UID.
async fn seed_service_account(state: &Arc<ApiServerState>, namespace: &str, name: &str) -> String {
    let uid = uuid::Uuid::new_v4().to_string();
    let sa = ServiceAccount {
        type_meta: TypeMeta {
            api_version: "v1".to_string(),
            kind: "ServiceAccount".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uid.clone(),
            ..Default::default()
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: None,
    };
    let key = build_key("serviceaccounts", Some(namespace), name);
    state.storage.create(&key, &sa).await.unwrap();
    uid
}

/// Mint a bearer token for a ServiceAccount directly through the
/// `TokenManager`. Mirrors what the TokenRequest handler does, but
/// bypasses RBAC so the setup step itself doesn't require an
/// already-authorized identity. Claim shape matches upstream
/// `pkg/serviceaccount/claims.go`:
///   sub: "system:serviceaccount:<ns>:<name>"
///   iss: "https://kubernetes.default.svc.cluster.local"
///   aud: ["https://kubernetes.default.svc"]
///   kubernetes.io: {namespace, serviceaccount: {name, uid}}
fn mint_sa_token(state: &Arc<ApiServerState>, namespace: &str, sa_name: &str, uid: &str) -> String {
    let now = chrono::Utc::now();
    let claims = rusternetes_common::auth::ServiceAccountClaims {
        sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
        namespace: namespace.to_string(),
        uid: uid.to_string(),
        iat: now.timestamp(),
        exp: (now + chrono::Duration::hours(1)).timestamp(),
        iss: "https://kubernetes.default.svc.cluster.local".to_string(),
        aud: vec!["https://kubernetes.default.svc".to_string()],
        kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
            namespace: namespace.to_string(),
            svcacct: rusternetes_common::auth::KubeRef {
                name: sa_name.to_string(),
                uid: uid.to_string(),
            },
            pod: None,
            node: None,
        }),
        pod_name: None,
        pod_uid: None,
        node_name: None,
        node_uid: None,
    };
    state.token_manager.generate_token(claims).unwrap()
}

/// Upstream: `TestServiceAccountTokenAuthentication` — release-1.35
/// `test/integration/serviceaccount/service_account_test.go:122-187`.
///
/// 1. Create `auth-ns` and `other-ns`.
/// 2. Create SAs `ro` and `rw` in `auth-ns`.
/// 3. Mint a bearer token for each (upstream uses
///    `serviceaccount.JWTTokenAuthenticator`; we drive `TokenManager`
///    directly, same claim shape — see `mint_sa_token`).
/// 4. `ro` may list pods in `auth-ns` but not create them; `rw` may both.
/// 5. Cross-namespace access is denied.
/// 6. Deleting the `ro` SA invalidates the token (401 — upstream parity
///    with `pkg/serviceaccount/legacy.go` which re-checks SA Getter on
///    every authenticate call).
///
/// Setup uses direct storage writes (namespaces, SAs, RBAC bindings) so the
/// authn-enabled harness doesn't need a privileged kubeconfig — the upstream
/// integration test uses the kubeapiserver's internal admin loopback for
/// the same reason.
#[tokio::test]
async fn test_service_account_token_authentication() {
    let state = spawn_authn_state();
    let auth_ns = "auth-ns";
    let other_ns = "other-ns";

    // (1) Two namespaces (direct storage write — no privileged client).
    seed_namespace(&state, auth_ns).await;
    seed_namespace(&state, other_ns).await;

    // (2) Two ServiceAccounts in auth-ns.
    let ro_uid = seed_service_account(&state, auth_ns, "ro").await;
    let rw_uid = seed_service_account(&state, auth_ns, "rw").await;

    // (3) Mint a bearer token for each.
    let mut tokens: std::collections::HashMap<&str, String> = std::collections::HashMap::new();
    tokens.insert("ro", mint_sa_token(&state, auth_ns, "ro", &ro_uid));
    tokens.insert("rw", mint_sa_token(&state, auth_ns, "rw", &rw_uid));

    // Seed RBAC: ro can get/list/watch pods in auth-ns; rw can do everything.
    grant_sa_pods_verbs(
        &state,
        "pod-reader",
        auth_ns,
        "ro",
        &["get", "list", "watch"],
    )
    .await;
    grant_sa_pods_verbs(
        &state,
        "pod-writer",
        auth_ns,
        "rw",
        &["get", "list", "watch", "create", "update", "delete"],
    )
    .await;

    // (4) ro may list pods in auth-ns but not create them.
    let router = build_router(state.clone(), None);
    let (status, body) = get_json_bearer(
        router,
        &format!("/api/v1/namespaces/{auth_ns}/pods"),
        &tokens["ro"],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ro can list pods in own ns: {body}");

    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "ro-pod"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
    });
    let router = build_router(state.clone(), None);
    let (status, body) = post_json_bearer(
        router,
        &format!("/api/v1/namespaces/{auth_ns}/pods"),
        &tokens["ro"],
        &pod_body,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "ro must not be able to create pods: {body}"
    );

    // rw may create pods in auth-ns.
    let pod_body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "rw-pod"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
    });
    let router = build_router(state.clone(), None);
    let (status, body) = post_json_bearer(
        router,
        &format!("/api/v1/namespaces/{auth_ns}/pods"),
        &tokens["rw"],
        &pod_body,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "rw must be able to create pods: {body}"
    );

    // (5) Cross-namespace: rw may not list pods in other-ns.
    let router = build_router(state.clone(), None);
    let (status, body) = get_json_bearer(
        router,
        &format!("/api/v1/namespaces/{other_ns}/pods"),
        &tokens["rw"],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "rw must not be able to list pods cross-namespace: {body}"
    );

    // (6) Revoke ro's token by deleting the SA. The auth middleware
    //     re-checks SA existence on every authenticate (upstream parity with
    //     `pkg/serviceaccount/legacy.go`), so subsequent ro requests must 401.
    //     Direct storage delete because the authn-enabled harness has no
    //     privileged HTTP identity (mirrors upstream loopback admin).
    let ro_key = build_key("serviceaccounts", Some(auth_ns), "ro");
    state.storage.delete(&ro_key).await.unwrap();

    let router = build_router(state.clone(), None);
    let (status, body) = get_json_bearer(
        router,
        &format!("/api/v1/namespaces/{auth_ns}/pods"),
        &tokens["ro"],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "ro token must be invalidated after SA delete (unauthorized error): {body}"
    );
}

// Belt-and-braces: keep the helpers in the file even if a future refactor
// removes one of the bearer-token tests, so the test surface still compiles.
#[allow(dead_code)]
fn _bind_helpers(_a: &dyn Fn() -> StatusCode) {}
