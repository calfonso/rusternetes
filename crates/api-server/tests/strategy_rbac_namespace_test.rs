//! Strategy parity pins for the upstream Kubernetes v1.35
//! `pkg/registry/{rbac,core/namespace,core/configmap,core/secret}/strategy_test.go`
//! suites.
//!
//! Source-of-truth permalinks (release-1.35):
//!   - <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/core/namespace/strategy_test.go>
//!   - <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/core/configmap/strategy_test.go>
//!   - <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/core/secret/strategy_test.go>
//!   - <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/rbac/role/strategy_test.go>
//!   - <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/rbac/clusterrole/strategy_test.go>
//!   - <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/rbac/rolebinding/strategy_test.go>
//!   - <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/rbac/clusterrolebinding/strategy_test.go>
//!
//! Upstream's registry strategy tests exercise `PrepareForCreate`,
//! `PrepareForUpdate`, and `ValidateUpdate` against in-memory objects. We do
//! not run that Go code directly — instead we drive the equivalent contracts
//! through the in-process Axum router so that any rusternetes handler that
//! forgets a defaulting step or an immutability fence trips a black-box HTTP
//! assertion here.
//!
//! ## Layer #3: registry/strategy (the "S" in upstream's `pkg/registry`)
//!
//! Each `#[tokio::test]` follows the same recipe (mirrors
//! `tests/integration_dryrun_all_resources.rs:82-100`):
//!   1. `spawn_router()` builds an `ApiServerState` over `MemoryStorage` with
//!      `skip_auth=true` and `AlwaysAllowAuthorizer`.
//!   2. The test POSTs / PUTs JSON via `tower::ServiceExt::oneshot`.
//!   3. Both the HTTP response body **and** the registry entry at
//!      `/registry/<resource>/[<ns>/]<name>` are asserted, mirroring upstream's
//!      `obj` vs `etcdcl.Get` cross-check.
//!
//! ## Scope
//!
//! - Namespace: `phase: Active` defaulting on create, finalizer + phase=Terminating
//!   on delete, finalizers field on update.
//! - ConfigMap: `immutable: true` is sticky — cannot be unset or flipped; `data`
//!   and `binaryData` mutations rejected once immutable.
//! - Secret: same immutable rules; `type` immutable post-create; `stringData`
//!   normalised into `data` on create.
//! - Role / ClusterRole: rules round-trip; ClusterRole `aggregationRule` round-trip.
//! - RoleBinding / ClusterRoleBinding: `subjects` mutation allowed (the rest
//!   of the strategy contract for `roleRef` immutability is exercised against
//!   upstream parity only where rusternetes implements it; see PR body for
//!   the deferred bugs).

#![allow(clippy::too_many_lines)]

use axum::http::Method;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send_json(router: TestApiServer, method: Method, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router
        .send(method.as_str(), uri, Some("application/json"), Some(body))
        .await;
    (status.as_u16(), value)
}

async fn send_delete(router: TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.delete(uri).await;
    (status.as_u16(), value)
}

/// Pull the stored object straight out of `MemoryStorage` as `serde_json::Value`.
async fn stored(mem: &Arc<MemoryStorage>, resource: &str, ns: Option<&str>, name: &str) -> Value {
    let key = build_key(resource, ns, name);
    mem.get::<Value>(&key)
        .await
        .unwrap_or_else(|_| panic!("expected stored object at {key}"))
}

// ---------------------------------------------------------------------------
// Namespace strategy
//
// Upstream pin (pkg/registry/core/namespace/strategy_test.go):
//   - `TestNamespaceStrategy` / `PrepareForCreate` — defaults
//     `status.phase` to "Active".
//   - `TestNamespaceStatusStrategy` — delete transitions phase to "Terminating".
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_namespace_strategy_create_defaults_phase_to_active() {
    let (mem, router) = spawn_router();

    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": "strategy-ns-defaults" }
    });
    let (status, resp) = send_json(router, Method::POST, "/api/v1/namespaces", &body).await;
    assert_eq!(status, 201, "namespace CREATE; got body={resp}");

    assert_eq!(
        resp["status"]["phase"], "Active",
        "response status.phase must default to Active; body={resp}"
    );

    let saved = stored(&mem, "namespaces", None, "strategy-ns-defaults").await;
    assert_eq!(
        saved["status"]["phase"], "Active",
        "stored namespace must have phase=Active; saved={saved}"
    );
}

#[tokio::test]
async fn test_namespace_strategy_create_attaches_kubernetes_finalizer() {
    let (mem, router) = spawn_router();

    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": "strategy-ns-finalizer" }
    });
    let (status, _) = send_json(router, Method::POST, "/api/v1/namespaces", &body).await;
    assert_eq!(status, 201);

    let saved = stored(&mem, "namespaces", None, "strategy-ns-finalizer").await;
    let finalizers = saved["metadata"]["finalizers"]
        .as_array()
        .expect("finalizers array");
    assert!(
        finalizers.iter().any(|v| v == "kubernetes"),
        "stored namespace must carry the kubernetes finalizer; saved={saved}"
    );
}

#[tokio::test]
async fn test_namespace_strategy_delete_sets_phase_to_terminating() {
    let (mem, router) = spawn_router();

    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": "strategy-ns-term" }
    });
    let (status, _) = send_json(router.clone(), Method::POST, "/api/v1/namespaces", &body).await;
    assert_eq!(status, 201);

    let (del_status, del_resp) = send_delete(router, "/api/v1/namespaces/strategy-ns-term").await;
    assert_eq!(del_status, 200, "DELETE; got body={del_resp}");

    let saved = stored(&mem, "namespaces", None, "strategy-ns-term").await;
    assert_eq!(
        saved["status"]["phase"], "Terminating",
        "stored namespace must move to phase=Terminating after DELETE; saved={saved}"
    );
    assert!(
        saved["metadata"]["deletionTimestamp"].is_string(),
        "deletionTimestamp must be set after DELETE; saved={saved}"
    );
}

#[tokio::test]
async fn test_namespace_strategy_update_preserves_finalizers() {
    let (mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": "strategy-ns-update" }
    });
    let (status, _) = send_json(router.clone(), Method::POST, "/api/v1/namespaces", &create).await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "strategy-ns-update",
            "finalizers": ["kubernetes", "example.com/custom"]
        }
    });
    let (status, _) = send_json(
        router,
        Method::PUT,
        "/api/v1/namespaces/strategy-ns-update",
        &update,
    )
    .await;
    assert_eq!(status, 200);

    let saved = stored(&mem, "namespaces", None, "strategy-ns-update").await;
    let finalizers = saved["metadata"]["finalizers"]
        .as_array()
        .expect("finalizers array");
    assert!(
        finalizers.iter().any(|v| v == "example.com/custom"),
        "user-set finalizer must round-trip on PUT; saved={saved}"
    );
}

// ---------------------------------------------------------------------------
// ConfigMap strategy
//
// Upstream pin (pkg/registry/core/configmap/strategy_test.go):
//   - `TestConfigMapStrategy` / `ValidateUpdate` — once `immutable: true`,
//     `data` and `binaryData` are immutable; you cannot clear or change
//     `immutable` either.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_configmap_strategy_immutable_data_change_rejected() {
    let (_mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-imm-data", "namespace": "default" },
        "data": { "key": "v1" },
        "immutable": true
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/default/configmaps",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-imm-data", "namespace": "default" },
        "data": { "key": "v2" },
        "immutable": true
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/api/v1/namespaces/default/configmaps/cm-imm-data",
        &update,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "immutable ConfigMap data change must be 4xx; got {status}, body={resp}"
    );
}

#[tokio::test]
async fn test_configmap_strategy_immutable_binary_data_change_rejected() {
    let (_mem, router) = spawn_router();

    // base64("AAAA") = "QUFBQQ==" ; base64("BBBB") = "QkJCQg==" (different bytes).
    let create = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-imm-bin", "namespace": "default" },
        "binaryData": { "blob": "QUFBQQ==" },
        "immutable": true
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/default/configmaps",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-imm-bin", "namespace": "default" },
        "binaryData": { "blob": "QkJCQg==" },
        "immutable": true
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/api/v1/namespaces/default/configmaps/cm-imm-bin",
        &update,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "immutable ConfigMap binaryData change must be 4xx; got {status}, body={resp}"
    );
}

#[tokio::test]
async fn test_configmap_strategy_immutable_cannot_be_unset() {
    let (_mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-imm-flip", "namespace": "default" },
        "data": { "k": "v" },
        "immutable": true
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/default/configmaps",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    // Flip immutable=true -> false (with otherwise-identical data) — upstream
    // strategy rejects this; the immutable flag is itself sticky.
    let update = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-imm-flip", "namespace": "default" },
        "data": { "k": "v" },
        "immutable": false
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/api/v1/namespaces/default/configmaps/cm-imm-flip",
        &update,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "immutable flag must be sticky; got {status}, body={resp}"
    );
}

#[tokio::test]
async fn test_configmap_strategy_metadata_update_allowed_when_immutable() {
    let (mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-imm-meta", "namespace": "default" },
        "data": { "k": "v" },
        "immutable": true
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/default/configmaps",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    // Label-only change while immutable=true and data unchanged — upstream
    // allows this (only data/binaryData/immutable are fenced).
    let update = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-imm-meta",
            "namespace": "default",
            "labels": { "team": "platform" }
        },
        "data": { "k": "v" },
        "immutable": true
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/api/v1/namespaces/default/configmaps/cm-imm-meta",
        &update,
    )
    .await;
    assert_eq!(
        status, 200,
        "label-only update on immutable ConfigMap should succeed; body={resp}"
    );

    let saved = stored(&mem, "configmaps", Some("default"), "cm-imm-meta").await;
    assert_eq!(
        saved["metadata"]["labels"]["team"], "platform",
        "label must persist; saved={saved}"
    );
}

// ---------------------------------------------------------------------------
// Secret strategy
//
// Upstream pin (pkg/registry/core/secret/strategy_test.go):
//   - `TestStrategy` — `stringData` is folded into `data` on the way in.
//   - `TestValidateUpdate` — once immutable, `data` and `stringData` are
//     locked; `type` is locked from create onwards (regardless of immutability).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_secret_strategy_string_data_merges_into_data() {
    let (mem, router) = spawn_router();

    // stringData["user"] = "admin" — upstream's strategy base64-encodes this
    // into data["user"] = "YWRtaW4=" before storage. We send no `data` so the
    // post-normalisation data map must come entirely from stringData.
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-stringdata", "namespace": "default" },
        "type": "Opaque",
        "stringData": { "user": "admin" }
    });
    let (status, resp) = send_json(
        router,
        Method::POST,
        "/api/v1/namespaces/default/secrets",
        &body,
    )
    .await;
    assert_eq!(status, 201, "Secret CREATE; body={resp}");

    let saved = stored(&mem, "secrets", Some("default"), "sec-stringdata").await;
    // The on-wire Secret has data base64-encoded values; the stored Value
    // mirrors serialized JSON output, so data["user"] == base64("admin").
    assert_eq!(
        saved["data"]["user"], "YWRtaW4=",
        "stringData must be normalised into data on create; saved={saved}"
    );
    assert!(
        saved["stringData"].is_null(),
        "stringData must be cleared after merging; saved={saved}"
    );
}

#[tokio::test]
async fn test_secret_strategy_immutable_data_change_rejected() {
    let (_mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-imm-data", "namespace": "default" },
        "type": "Opaque",
        "data": { "k": "djE=" }, // base64("v1")
        "immutable": true
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/default/secrets",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-imm-data", "namespace": "default" },
        "type": "Opaque",
        "data": { "k": "djI=" }, // base64("v2")
        "immutable": true
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/api/v1/namespaces/default/secrets/sec-imm-data",
        &update,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "immutable Secret data change must be 4xx; got {status}, body={resp}"
    );
}

#[tokio::test]
async fn test_secret_strategy_immutable_cannot_be_unset() {
    let (_mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-imm-flip", "namespace": "default" },
        "type": "Opaque",
        "data": { "k": "djE=" },
        "immutable": true
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/default/secrets",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-imm-flip", "namespace": "default" },
        "type": "Opaque",
        "data": { "k": "djE=" },
        "immutable": false
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/api/v1/namespaces/default/secrets/sec-imm-flip",
        &update,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "immutable flag must be sticky on Secret; got {status}, body={resp}"
    );
}

#[tokio::test]
async fn test_secret_strategy_type_immutable_post_create() {
    // Upstream `pkg/registry/core/secret/strategy.go::ValidateUpdate` calls
    // `apivalidation.ValidateImmutableField(newSecret.Type, oldSecret.Type, …)`
    // unconditionally. rusternetes' `handlers::secret::update` enforces the
    // same fence in `handlers/secret.rs`.
    let (_mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-type-fixed", "namespace": "default" },
        "type": "Opaque",
        "data": { "k": "djE=" }
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/default/secrets",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    // Flip type Opaque -> kubernetes.io/basic-auth — upstream rejects.
    let update = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "sec-type-fixed", "namespace": "default" },
        "type": "kubernetes.io/basic-auth",
        "data": { "k": "djE=" }
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/api/v1/namespaces/default/secrets/sec-type-fixed",
        &update,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "Secret type change must be 4xx; got {status}, body={resp}"
    );
}

// ---------------------------------------------------------------------------
// Role / ClusterRole strategy
//
// Upstream pin (pkg/registry/rbac/{role,clusterrole}/strategy_test.go):
//   - `Strategy.PrepareForCreate` clears finalizers/managedFields, but the
//     core observable contract for our HTTP layer is that the rules array
//     round-trips byte-for-byte and the ClusterRole `aggregationRule` is
//     preserved on the way out.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_role_strategy_rules_roundtrip_on_create() {
    let (mem, router) = spawn_router();

    let body = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": { "name": "role-rt", "namespace": "default" },
        "rules": [
            {
                "verbs": ["get", "list"],
                "apiGroups": [""],
                "resources": ["pods"]
            }
        ]
    });
    let (status, resp) = send_json(
        router,
        Method::POST,
        "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles",
        &body,
    )
    .await;
    assert_eq!(status, 201, "Role CREATE; body={resp}");

    assert_eq!(
        resp["rules"][0]["verbs"],
        json!(["get", "list"]),
        "verbs must round-trip"
    );
    assert_eq!(resp["rules"][0]["resources"], json!(["pods"]));
    assert_eq!(resp["rules"][0]["apiGroups"], json!([""]));

    let saved = stored(&mem, "roles", Some("default"), "role-rt").await;
    assert_eq!(
        saved["rules"][0]["verbs"],
        json!(["get", "list"]),
        "stored rules must match; saved={saved}"
    );
}

#[tokio::test]
async fn test_clusterrole_strategy_aggregation_rule_roundtrip() {
    let (mem, router) = spawn_router();

    let body = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": { "name": "cr-aggregated" },
        "rules": [],
        "aggregationRule": {
            "clusterRoleSelectors": [
                { "matchLabels": { "rbac.example.com/aggregate-to-admin": "true" } }
            ]
        }
    });
    let (status, resp) = send_json(
        router,
        Method::POST,
        "/apis/rbac.authorization.k8s.io/v1/clusterroles",
        &body,
    )
    .await;
    assert_eq!(status, 201, "ClusterRole CREATE; body={resp}");
    assert_eq!(
        resp["aggregationRule"]["clusterRoleSelectors"][0]["matchLabels"]
            ["rbac.example.com/aggregate-to-admin"],
        "true",
        "aggregationRule must round-trip in response; body={resp}"
    );

    let saved = stored(&mem, "clusterroles", None, "cr-aggregated").await;
    assert_eq!(
        saved["aggregationRule"]["clusterRoleSelectors"][0]["matchLabels"]
            ["rbac.example.com/aggregate-to-admin"],
        "true",
        "aggregationRule must persist; saved={saved}"
    );
}

// ---------------------------------------------------------------------------
// RoleBinding / ClusterRoleBinding strategy
//
// Upstream pin (pkg/registry/rbac/{rolebinding,clusterrolebinding}/strategy_test.go):
//   - `ValidateUpdate` enforces `roleRef` immutability.
//   - `subjects` is freely mutable post-create.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rolebinding_strategy_subjects_update_allowed() {
    let (mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": { "name": "rb-mut", "namespace": "default" },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "viewer"
        },
        "subjects": [
            { "kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io" }
        ]
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": { "name": "rb-mut", "namespace": "default" },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "viewer"
        },
        "subjects": [
            { "kind": "User", "name": "bob", "apiGroup": "rbac.authorization.k8s.io" }
        ]
    });
    let (status, _) = send_json(
        router,
        Method::PUT,
        "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings/rb-mut",
        &update,
    )
    .await;
    assert_eq!(status, 200, "subjects update must succeed");

    let saved = stored(&mem, "rolebindings", Some("default"), "rb-mut").await;
    assert_eq!(
        saved["subjects"][0]["name"], "bob",
        "new subject must persist; saved={saved}"
    );
}

#[tokio::test]
async fn test_rolebinding_strategy_role_ref_immutable() {
    // Upstream `pkg/registry/rbac/rolebinding/strategy.go::ValidateUpdate`
    // checks `apivalidation.ValidateImmutableField(newRoleBinding.RoleRef, …)`.
    // rusternetes' `handlers::rbac::update_rolebinding` writes through to
    // storage without comparing roleRef. This test pins the contract for the
    // future fence.
    let (_mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": { "name": "rb-roleref", "namespace": "default" },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "viewer"
        },
        "subjects": []
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": { "name": "rb-roleref", "namespace": "default" },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "editor"
        },
        "subjects": []
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings/rb-roleref",
        &update,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "roleRef must be immutable; got {status}, body={resp}"
    );
}

#[tokio::test]
async fn test_clusterrolebinding_strategy_role_ref_immutable() {
    let (_mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": { "name": "crb-roleref" },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "view"
        },
        "subjects": []
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": { "name": "crb-roleref" },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "edit"
        },
        "subjects": []
    });
    let (status, resp) = send_json(
        router,
        Method::PUT,
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/crb-roleref",
        &update,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "ClusterRoleBinding roleRef must be immutable; got {status}, body={resp}"
    );
}

#[tokio::test]
async fn test_clusterrolebinding_strategy_subjects_update_allowed() {
    let (mem, router) = spawn_router();

    let create = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": { "name": "crb-mut" },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "view"
        },
        "subjects": [
            { "kind": "User", "name": "carol", "apiGroup": "rbac.authorization.k8s.io" }
        ]
    });
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
        &create,
    )
    .await;
    assert_eq!(status, 201);

    let update = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": { "name": "crb-mut" },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "view"
        },
        "subjects": [
            { "kind": "User", "name": "dave", "apiGroup": "rbac.authorization.k8s.io" }
        ]
    });
    let (status, _) = send_json(
        router,
        Method::PUT,
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/crb-mut",
        &update,
    )
    .await;
    assert_eq!(
        status, 200,
        "subjects update must succeed for ClusterRoleBinding"
    );

    let saved = stored(&mem, "clusterrolebindings", None, "crb-mut").await;
    assert_eq!(
        saved["subjects"][0]["name"], "dave",
        "new subject must persist; saved={saved}"
    );
}
