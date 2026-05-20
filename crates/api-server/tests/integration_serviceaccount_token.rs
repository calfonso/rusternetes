//! RED-state TDD mirror of the Kubernetes v1.35 integration test suite for
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
//! Tests drive the production routes through an inline `spawn_state()`
//! helper that wires `rusternetes_api_server::router::build_router` over an
//! in-memory storage with `AlwaysAllowAuthorizer` and `skip_auth = true`
//! (same pattern as `conformance_apimachinery_admission_webhooks.rs` and the
//! sibling `conformance_auth_rbac_serviceaccount.rs`). Requests go through
//! tower's `oneshot` so no live socket is required.
//!
//! RED-state policy
//! ----------------
//! Tests that exercise surface that exists today (namespace POST creating the
//! default SA, pod admission injecting the `kube-api-access` projected
//! volume) are left ungated — they will pass and serve as regression pins.
//! Tests that depend on surface that is **not yet implemented in the
//! api-server alone** (the SA controller recreating a deleted `default` SA
//! without the controller-manager loop, and TokenRequest + bearer-token
//! authentication wired through the auth middleware) are `#[ignore]`d with a
//! comment naming the missing surface, per the batch RED-state template.
//!
//! Part of the /batch landing upstream integration-test mirrors as
//! RED-state TDD pins.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// HTTP harness
// ---------------------------------------------------------------------------

/// Build a fully-wired `ApiServerState` backed by an in-memory storage. The
/// authorizer is `AlwaysAllow` and `skip_auth = true` so the router uses
/// `skip_auth_middleware` and no token is required.
fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(TokenManager::new(b"integration-sa-token-secret"));
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

/// State factory used by every HTTP-driven test. The router is rebuilt per
/// request because `axum::Router::oneshot` consumes the router.
fn spawn_state() -> Arc<ApiServerState> {
    let mem = Arc::new(MemoryStorage::new());
    make_state(mem)
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

/// POST JSON with a bearer token, return `(status, body)`. Used by the
/// authentication test once `skip_auth` is disabled.
#[allow(dead_code)]
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
#[allow(dead_code)]
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
/// RED-state notes:
///   * Step 2 already works in the api-server: the `POST /api/v1/namespaces`
///     handler synchronously creates the `default` ServiceAccount before
///     returning (see `crates/api-server/src/handlers/namespace.rs`).
///   * Step 4 is the assertion that fails today. In production, recreation
///     is performed by the **ServiceAccount controller** loop in the
///     controller-manager
///     (`crates/controller-manager/src/controllers/serviceaccount.rs`), which
///     is **not** running inside this api-server-only test harness. The
///     test mirror therefore stays RED until the SA controller is wired
///     into the integration harness (or an api-server-side guard
///     synchronously recreates the default SA on demand).
#[tokio::test]
#[ignore = "RED: ServiceAccount controller (controller-manager) is not driven by this api-server-only harness; default SA is not recreated after delete"]
async fn test_service_account_auto_create() {
    let state = spawn_state();
    let ns = "test-service-account-creation";

    // (1) Create the namespace.
    let (status, body) = create_namespace(&state, ns).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "POST /api/v1/namespaces must return 201: {body}"
    );

    // (2) The `default` ServiceAccount must exist in the new namespace.
    //     Upstream asserts via Core().ServiceAccounts(ns).Get("default").
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

    // (4) **RED**: the SA controller must recreate the default SA with a
    //     different UID. This is the assertion that fails today and the
    //     reason this whole test is `#[ignore]`d.
    let router = build_router(state.clone(), None);
    let (status, recreated) = get_json(
        router,
        &format!("/api/v1/namespaces/{ns}/serviceaccounts/default"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "default SA must be auto-recreated after deletion: {recreated}"
    );
    let new_uid = recreated["metadata"]["uid"].as_str().unwrap_or("");
    assert!(!new_uid.is_empty(), "recreated SA must have a UID");
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
///
/// Steps:
///   1. Create a namespace.
///   2. POST a pod that omits `spec.serviceAccountName`.
///   3. The created pod must have `serviceAccountName == "default"`.
///   4. The created pod's `spec.volumes` must contain a **projected** volume
///      that holds a `serviceAccountToken` projection.
///
/// In the api-server this defaulting + injection is performed in
/// `crates/api-server/src/admission.rs::inject_service_account_token`, called
/// from the pod POST handler. The volume name today is `kube-api-access` and
/// the projection source array contains a `ServiceAccountToken` projection
/// (path `token`, expiration ~3607s).
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
    //     Upstream upstream: `pod.Spec.ServiceAccountName == DefaultServiceAccountName`.
    assert_eq!(
        created["spec"]["serviceAccountName"], "default",
        "pod must default ServiceAccountName to \"default\": {created}"
    );

    // (4) The pod must have a projected volume carrying a
    //     ServiceAccountToken projection. Upstream message:
    //     "Expected projected volume for service account token inserted".
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

/// Upstream: `TestServiceAccountTokenAuthentication` — release-1.35
/// `test/integration/serviceaccount/service_account_test.go:122-187`.
///
/// Steps:
///   1. Create two namespaces (`auth-ns`, `other-ns`).
///   2. Create a read-only SA (`ro`) and a read-write SA (`rw`) in `auth-ns`.
///   3. Mint a bearer token for each via the legacy Secret-with-annotation
///      flow or the modern TokenRequest flow.
///   4. Authorize via the custom authorizer: `ro` may list pods in `auth-ns`
///      but may not create them; `rw` may both.
///   5. Cross-namespace access is denied for both.
///   6. Deleting the `ro` SA's token invalidates the token: subsequent
///      requests must return 401 Unauthorized.
///
/// RED-state notes:
///   * This file's harness is `skip_auth = true` with `AlwaysAllowAuthorizer`,
///     so bearer-token validation is bypassed and the per-SA authorization
///     verdict is unobservable. The mirror is `#[ignore]`d until the auth
///     pipeline can be exercised end-to-end from an integration test:
///       - a router built with `skip_auth = false`,
///       - an authorizer that honours per-SA RBAC bindings,
///       - and a way to **invalidate** a token after deletion (today's
///         `TokenManager` issues stateless JWTs; deletion of the SA secret
///         does not currently revoke them).
///   * Compare with the GREEN-on-Sonobuoy
///     `service_account_token_request_then_token_review_authenticates` in
///     `conformance_auth_rbac_serviceaccount.rs`, which only checks the
///     TokenRequest → TokenReview round-trip and does not exercise per-SA
///     authorization or token revocation.
#[tokio::test]
#[ignore = "RED: requires skip_auth=false harness, per-SA RBAC enforcement, and token revocation on SA/secret delete; only stateless JWTs exist today"]
async fn test_service_account_token_authentication() {
    let state = spawn_state();
    let auth_ns = "auth-ns";
    let other_ns = "other-ns";

    // (1) Two namespaces.
    for ns in [auth_ns, other_ns] {
        let (status, body) = create_namespace(&state, ns).await;
        assert_eq!(status, StatusCode::CREATED, "ns {ns}: {body}");
    }

    // (2) Two ServiceAccounts in auth-ns.
    for name in ["ro", "rw"] {
        let sa_body = json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": name},
        });
        let router = build_router(state.clone(), None);
        let (status, body) = post_json(
            router,
            &format!("/api/v1/namespaces/{auth_ns}/serviceaccounts"),
            &sa_body,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "create sa {name}: {body}");
    }

    // (3) Mint a TokenRequest for each. Upstream does this via the legacy
    //     Secret with `kubernetes.io/service-account.name` annotation; the
    //     v1.35 modern equivalent is the TokenRequest subresource.
    let mut tokens: std::collections::HashMap<&str, String> = std::collections::HashMap::new();
    for name in ["ro", "rw"] {
        let req = json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "metadata": {},
            "spec": {"audiences": ["https://kubernetes.default.svc"]},
        });
        let router = build_router(state.clone(), None);
        let (status, body) = post_json(
            router,
            &format!("/api/v1/namespaces/{auth_ns}/serviceaccounts/{name}/token"),
            &req,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "TokenRequest for {name}: {body}");
        let token = body["status"]["token"]
            .as_str()
            .filter(|t| !t.is_empty())
            .expect("token must be present")
            .to_string();
        tokens.insert(name, token);
    }

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

    // (6) Revoke ro's token by deleting the SA. Upstream deletes the
    //     legacy Secret holding the token; with TokenRequest the equivalent
    //     is deleting the bound SA (or rotating the signing key). Either
    //     way, subsequent ro requests must return 401.
    let router = build_router(state.clone(), None);
    let (status, body) = delete(
        router,
        &format!("/api/v1/namespaces/{auth_ns}/serviceaccounts/ro"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delete ro SA: {body}");

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
