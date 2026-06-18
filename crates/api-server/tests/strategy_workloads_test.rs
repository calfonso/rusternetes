//! Strategy parity tests for `apps/v1` workload resources
//! (Deployment, ReplicaSet, StatefulSet, DaemonSet) plus a minimal
//! ControllerRevision lifecycle roundtrip.
//!
//! Mirrors upstream Kubernetes v1.35 `pkg/registry/apps/{deployment,replicaset,
//! statefulset,daemonset}/strategy_test.go` behavioral assertions:
//!
//! * **Generation bump on spec change** — PUT with mutated spec increments
//!   `metadata.generation`; PUT with identical spec keeps it.
//!   (`Strategy.PrepareForUpdate` in upstream, our
//!   `handlers::lifecycle::maybe_increment_generation` here.)
//! * **Status update isolation** — PUT against the main resource path
//!   does NOT touch `status`; PUT `/status` does NOT touch `spec`.
//!   (Upstream `StatusStrategy.PrepareForUpdate` resets the orthogonal field.)
//! * **Selector immutability** — `spec.selector` is immutable post-create per
//!   upstream `ValidateXUpdate`. Enforced in our handler chain via
//!   `handlers::lifecycle::validate_selector_immutable`, which returns 422
//!   Invalid when `old.spec.selector != new.spec.selector`.
//! * **Replicas defaulting** — Deployment / StatefulSet default missing
//!   `spec.replicas` to 1 via `apply_*_defaults`. ReplicaSet defaults to 1
//!   via serde `default = "default_one_replica"`. DaemonSet has no
//!   `spec.replicas` field — assert it stays absent from the stored object.
//! * **ControllerRevision** — handler is exposed at
//!   `/apis/apps/v1/namespaces/:ns/controllerrevisions[/:name]`. A create →
//!   list → delete roundtrip covers the basic strategy contract since the
//!   resource is immutable apart from `metadata.labels`.
//!
//! Handler parity gaps fixed in `fix(api-server): apps/v1 strategy parity`:
//!
//!   * ReplicaSet / StatefulSet / DaemonSet handlers now call
//!     `set_initial_generation` on create and `maybe_increment_generation`
//!     on update, matching the Deployment handler.
//!   * The main-PUT path on every apps/v1 workload now copies the stored
//!     object's status onto the incoming body before persisting, mirroring
//!     upstream `Strategy.PrepareForUpdate`. Status mutates only via the
//!     `/status` subresource.
//!   * Selector immutability is enforced via
//!     `handlers::lifecycle::validate_selector_immutable`, called from each
//!     apps/v1 workload update handler.
//!
//! Convention: in-process Axum router wrapped around `MemoryStorage`; HTTP
//! verbs driven via `tower::ServiceExt::oneshot`; assertions check BOTH the
//! response body AND the stored object (read back through the same router).

use axum::http::Method;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "workload-strategy-ns";

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

async fn send_get(router: TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.get(uri).await;
    (status.as_u16(), value)
}

async fn send_delete(router: TestApiServer, uri: &str) -> u16 {
    let (status, _) = router.delete(uri).await;
    status.as_u16()
}

// ---------------------------------------------------------------------------
// Stub builders — match upstream `pkg/registry/apps/*/strategy_test.go`
// fixtures in spirit (matchLabels + matching template labels).
// ---------------------------------------------------------------------------

fn deployment_stub(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        }
    })
}

fn replicaset_stub(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        }
    })
}

fn statefulset_stub(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "serviceName": "svc",
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        }
    })
}

fn daemonset_stub(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        }
    })
}

// ---------------------------------------------------------------------------
// URI helpers
// ---------------------------------------------------------------------------

fn collection_uri(resource: &str) -> String {
    format!("/apis/apps/v1/namespaces/{}/{}", TEST_NS, resource)
}

fn item_uri(resource: &str, name: &str) -> String {
    format!("/apis/apps/v1/namespaces/{}/{}/{}", TEST_NS, resource, name)
}

fn status_uri(resource: &str, name: &str) -> String {
    format!(
        "/apis/apps/v1/namespaces/{}/{}/{}/status",
        TEST_NS, resource, name
    )
}

/// Pull `metadata.generation` from a body; `None` if absent.
fn generation_of(v: &Value) -> Option<i64> {
    v.get("metadata")?.get("generation")?.as_i64()
}

/// Helper: POST a stub, return (status_code, created_body).
async fn create_resource(router: TestApiServer, resource: &str, stub: Value) -> (u16, Value) {
    send_json(router, Method::POST, &collection_uri(resource), &stub).await
}

// ===========================================================================
// Deployment
// ===========================================================================

#[tokio::test]
async fn test_deployment_strategy_generation_bump_on_spec_change() {
    let (_mem, router) = spawn_router();
    let name = "deploy-gen-bump";

    let (status, created) =
        create_resource(router.clone(), "deployments", deployment_stub(name)).await;
    assert_eq!(status, 201, "create deployment: {}", created);
    assert_eq!(
        generation_of(&created),
        Some(1),
        "fresh deployment must have generation=1"
    );

    // PUT with a changed spec — replicas now 5 — generation should bump to 2.
    let mut mutated = created.clone();
    mutated["spec"]["replicas"] = json!(5);
    let (status, updated) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("deployments", name),
        &mutated,
    )
    .await;
    assert_eq!(status, 200, "update deployment: {}", updated);
    assert_eq!(
        generation_of(&updated),
        Some(2),
        "spec change must bump generation"
    );

    // PUT with identical spec — generation must stay at 2.
    let (status, idempotent) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("deployments", name),
        &updated,
    )
    .await;
    assert_eq!(status, 200, "no-op update: {}", idempotent);
    assert_eq!(
        generation_of(&idempotent),
        Some(2),
        "spec-equal update must NOT bump generation"
    );

    // Confirm storage agrees with the response body.
    let (gs, stored) = send_get(router, &item_uri("deployments", name)).await;
    assert_eq!(gs, 200);
    assert_eq!(generation_of(&stored), Some(2));
}

#[tokio::test]
async fn test_deployment_strategy_status_update_isolation() {
    let (_mem, router) = spawn_router();
    let name = "deploy-status-iso";

    let (status, created) =
        create_resource(router.clone(), "deployments", deployment_stub(name)).await;
    assert_eq!(status, 201, "create deployment: {}", created);

    // First, mutate spec via main PUT and assert status is NOT touched.
    let mut spec_only = created.clone();
    spec_only["spec"]["replicas"] = json!(7);
    spec_only["status"] = json!({"replicas": 999, "readyReplicas": 999}); // should be ignored
    let (st, after_main_put) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("deployments", name),
        &spec_only,
    )
    .await;
    assert_eq!(st, 200, "main PUT: {}", after_main_put);
    assert_eq!(after_main_put["spec"]["replicas"], json!(7));
    // Upstream `Strategy.PrepareForUpdate` wipes status from main PUT bodies;
    // accept either absent OR the pre-existing empty status, but NEVER the
    // 999 the client tried to inject.
    let post_main_ready = after_main_put
        .get("status")
        .and_then(|s| s.get("readyReplicas"))
        .and_then(|v| v.as_i64());
    assert_ne!(
        post_main_ready,
        Some(999),
        "status.readyReplicas leaked from main PUT body"
    );

    // Now PUT against /status with a fake-spec body. Spec must be preserved.
    let status_body = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"app": "TAMPERED"}},
            "template": {
                "metadata": {"labels": {"app": "TAMPERED"}},
                "spec": {"containers": [{"name": "tampered", "image": "evil"}]}
            },
            "replicas": 42
        },
        "status": {"replicas": 3, "readyReplicas": 3}
    });
    let (st, after_status_put) = send_json(
        router.clone(),
        Method::PUT,
        &status_uri("deployments", name),
        &status_body,
    )
    .await;
    assert_eq!(st, 200, "status PUT: {}", after_status_put);
    assert_eq!(
        after_status_put["spec"]["replicas"],
        json!(7),
        "status PUT must NOT touch spec.replicas"
    );
    assert_eq!(
        after_status_put["spec"]["selector"]["matchLabels"]["app"],
        json!(name),
        "status PUT must NOT touch spec.selector"
    );
    assert_eq!(
        after_status_put["status"]["readyReplicas"],
        json!(3),
        "status PUT must apply status fields"
    );

    // Storage round-trip confirms spec stayed intact.
    let (gs, stored) = send_get(router, &item_uri("deployments", name)).await;
    assert_eq!(gs, 200);
    assert_eq!(stored["spec"]["replicas"], json!(7));
    assert_eq!(
        stored["spec"]["selector"]["matchLabels"]["app"],
        json!(name)
    );
}

#[tokio::test]
async fn test_deployment_strategy_selector_immutability() {
    let (_mem, router) = spawn_router();
    let name = "deploy-sel-immut";
    let (_st, created) =
        create_resource(router.clone(), "deployments", deployment_stub(name)).await;

    let mut mutated = created.clone();
    mutated["spec"]["selector"]["matchLabels"]["app"] = json!("changed-selector");
    let (status, body) = send_json(
        router,
        Method::PUT,
        &item_uri("deployments", name),
        &mutated,
    )
    .await;
    assert_eq!(
        status, 422,
        "changing spec.selector must return 422 Invalid (got body: {})",
        body
    );
}

#[tokio::test]
async fn test_deployment_strategy_replicas_default_to_one() {
    let (_mem, router) = spawn_router();
    let name = "deploy-repl-default";

    let mut stub = deployment_stub(name);
    // Explicitly omit replicas (it is already Option::None in the stub).
    assert!(stub["spec"].get("replicas").is_none());
    stub["spec"].as_object_mut().unwrap().remove("replicas");
    let (status, created) = create_resource(router.clone(), "deployments", stub).await;
    assert_eq!(status, 201, "create deployment: {}", created);
    assert_eq!(
        created["spec"]["replicas"],
        json!(1),
        "missing replicas must default to 1"
    );

    let (_gs, stored) = send_get(router, &item_uri("deployments", name)).await;
    assert_eq!(stored["spec"]["replicas"], json!(1));
}

// ===========================================================================
// ReplicaSet
// ===========================================================================

#[tokio::test]
async fn test_replicaset_strategy_generation_bump_on_spec_change() {
    let (_mem, router) = spawn_router();
    let name = "rs-gen-bump";

    let (status, created) =
        create_resource(router.clone(), "replicasets", replicaset_stub(name)).await;
    assert_eq!(status, 201, "create rs: {}", created);
    assert_eq!(generation_of(&created), Some(1));

    let mut mutated = created.clone();
    mutated["spec"]["replicas"] = json!(4);
    let (st, updated) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("replicasets", name),
        &mutated,
    )
    .await;
    assert_eq!(st, 200, "update rs: {}", updated);
    assert_eq!(generation_of(&updated), Some(2));

    let (st, idempotent) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("replicasets", name),
        &updated,
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(
        generation_of(&idempotent),
        Some(2),
        "spec-equal update must NOT bump generation"
    );
}

#[tokio::test]
async fn test_replicaset_strategy_status_update_isolation() {
    let (_mem, router) = spawn_router();
    let name = "rs-status-iso";
    let (_st, created) =
        create_resource(router.clone(), "replicasets", replicaset_stub(name)).await;

    let mut spec_only = created.clone();
    spec_only["spec"]["replicas"] = json!(6);
    spec_only["status"] = json!({"replicas": 999, "readyReplicas": 999});
    let (_st, after_main) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("replicasets", name),
        &spec_only,
    )
    .await;
    assert_eq!(after_main["spec"]["replicas"], json!(6));
    let leaked = after_main
        .get("status")
        .and_then(|s| s.get("readyReplicas"))
        .and_then(|v| v.as_i64());
    assert_ne!(leaked, Some(999), "main PUT must not write status");

    let status_body = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "replicas": 99,
            "selector": {"matchLabels": {"app": "tampered"}},
            "template": {
                "metadata": {"labels": {"app": "tampered"}},
                "spec": {"containers": [{"name": "x", "image": "evil"}]}
            }
        },
        "status": {"replicas": 2, "readyReplicas": 2}
    });
    let (_st, after_status) = send_json(
        router.clone(),
        Method::PUT,
        &status_uri("replicasets", name),
        &status_body,
    )
    .await;
    assert_eq!(
        after_status["spec"]["replicas"],
        json!(6),
        "status PUT must preserve spec.replicas"
    );
    assert_eq!(
        after_status["spec"]["selector"]["matchLabels"]["app"],
        json!(name),
        "status PUT must preserve spec.selector"
    );
    assert_eq!(after_status["status"]["readyReplicas"], json!(2));
}

#[tokio::test]
async fn test_replicaset_strategy_selector_immutability() {
    let (_mem, router) = spawn_router();
    let name = "rs-sel-immut";
    let (_st, created) =
        create_resource(router.clone(), "replicasets", replicaset_stub(name)).await;

    let mut mutated = created.clone();
    mutated["spec"]["selector"]["matchLabels"]["app"] = json!("changed");
    let (status, body) = send_json(
        router,
        Method::PUT,
        &item_uri("replicasets", name),
        &mutated,
    )
    .await;
    assert_eq!(status, 422, "selector change must 422, got: {}", body);
}

#[tokio::test]
async fn test_replicaset_strategy_replicas_default_to_one() {
    let (_mem, router) = spawn_router();
    let name = "rs-repl-default";

    let mut stub = replicaset_stub(name);
    stub["spec"].as_object_mut().unwrap().remove("replicas");
    let (status, created) = create_resource(router.clone(), "replicasets", stub).await;
    assert_eq!(status, 201, "create rs: {}", created);
    assert_eq!(
        created["spec"]["replicas"],
        json!(1),
        "missing replicas must default to 1"
    );

    let (_gs, stored) = send_get(router, &item_uri("replicasets", name)).await;
    assert_eq!(stored["spec"]["replicas"], json!(1));
}

// ===========================================================================
// StatefulSet
// ===========================================================================

#[tokio::test]
async fn test_statefulset_strategy_generation_bump_on_spec_change() {
    let (_mem, router) = spawn_router();
    let name = "ss-gen-bump";

    let (status, created) =
        create_resource(router.clone(), "statefulsets", statefulset_stub(name)).await;
    assert_eq!(status, 201, "create ss: {}", created);
    assert_eq!(generation_of(&created), Some(1));

    let mut mutated = created.clone();
    mutated["spec"]["replicas"] = json!(3);
    let (st, updated) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("statefulsets", name),
        &mutated,
    )
    .await;
    assert_eq!(st, 200, "update ss: {}", updated);
    assert_eq!(generation_of(&updated), Some(2));

    let (st, idempotent) = send_json(
        router,
        Method::PUT,
        &item_uri("statefulsets", name),
        &updated,
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(generation_of(&idempotent), Some(2));
}

#[tokio::test]
async fn test_statefulset_strategy_status_update_isolation() {
    let (_mem, router) = spawn_router();
    let name = "ss-status-iso";
    let (_st, created) =
        create_resource(router.clone(), "statefulsets", statefulset_stub(name)).await;

    let mut spec_only = created.clone();
    spec_only["spec"]["replicas"] = json!(8);
    spec_only["status"] = json!({"replicas": 999, "readyReplicas": 999});
    let (_st, after_main) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("statefulsets", name),
        &spec_only,
    )
    .await;
    assert_eq!(after_main["spec"]["replicas"], json!(8));
    let leaked = after_main
        .get("status")
        .and_then(|s| s.get("readyReplicas"))
        .and_then(|v| v.as_i64());
    assert_ne!(leaked, Some(999));

    let status_body = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "replicas": 99,
            "serviceName": "tampered-svc",
            "selector": {"matchLabels": {"app": "tampered"}},
            "template": {
                "metadata": {"labels": {"app": "tampered"}},
                "spec": {"containers": [{"name": "x", "image": "evil"}]}
            }
        },
        "status": {"replicas": 4, "readyReplicas": 4}
    });
    let (_st, after_status) = send_json(
        router.clone(),
        Method::PUT,
        &status_uri("statefulsets", name),
        &status_body,
    )
    .await;
    assert_eq!(after_status["spec"]["replicas"], json!(8));
    assert_eq!(
        after_status["spec"]["selector"]["matchLabels"]["app"],
        json!(name)
    );
    assert_eq!(after_status["spec"]["serviceName"], json!("svc"));
    assert_eq!(after_status["status"]["readyReplicas"], json!(4));
}

#[tokio::test]
async fn test_statefulset_strategy_selector_immutability() {
    let (_mem, router) = spawn_router();
    let name = "ss-sel-immut";
    let (_st, created) =
        create_resource(router.clone(), "statefulsets", statefulset_stub(name)).await;

    let mut mutated = created.clone();
    mutated["spec"]["selector"]["matchLabels"]["app"] = json!("changed");
    let (status, body) = send_json(
        router,
        Method::PUT,
        &item_uri("statefulsets", name),
        &mutated,
    )
    .await;
    assert_eq!(status, 422, "selector change must 422, got: {}", body);
}

#[tokio::test]
async fn test_statefulset_strategy_replicas_default_to_one() {
    let (_mem, router) = spawn_router();
    let name = "ss-repl-default";

    let mut stub = statefulset_stub(name);
    stub["spec"].as_object_mut().unwrap().remove("replicas");
    let (status, created) = create_resource(router.clone(), "statefulsets", stub).await;
    assert_eq!(status, 201, "create ss: {}", created);
    assert_eq!(
        created["spec"]["replicas"],
        json!(1),
        "missing replicas must default to 1"
    );

    let (_gs, stored) = send_get(router, &item_uri("statefulsets", name)).await;
    assert_eq!(stored["spec"]["replicas"], json!(1));
}

// ===========================================================================
// DaemonSet
// ===========================================================================

#[tokio::test]
async fn test_daemonset_strategy_generation_bump_on_spec_change() {
    let (_mem, router) = spawn_router();
    let name = "ds-gen-bump";

    let (status, created) =
        create_resource(router.clone(), "daemonsets", daemonset_stub(name)).await;
    assert_eq!(status, 201, "create ds: {}", created);
    assert_eq!(generation_of(&created), Some(1));

    // Mutate the pod template image — that's a real spec change for DS.
    let mut mutated = created.clone();
    mutated["spec"]["template"]["spec"]["containers"][0]["image"] = json!("busybox:1.36");
    let (st, updated) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("daemonsets", name),
        &mutated,
    )
    .await;
    assert_eq!(st, 200, "update ds: {}", updated);
    assert_eq!(generation_of(&updated), Some(2));

    let (st, idempotent) =
        send_json(router, Method::PUT, &item_uri("daemonsets", name), &updated).await;
    assert_eq!(st, 200);
    assert_eq!(generation_of(&idempotent), Some(2));
}

#[tokio::test]
async fn test_daemonset_strategy_status_update_isolation() {
    let (_mem, router) = spawn_router();
    let name = "ds-status-iso";
    let (_st, created) = create_resource(router.clone(), "daemonsets", daemonset_stub(name)).await;

    let mut spec_only = created.clone();
    spec_only["spec"]["template"]["spec"]["containers"][0]["image"] = json!("busybox:1.36");
    spec_only["status"] = json!({"numberReady": 999});
    let (_st, after_main) = send_json(
        router.clone(),
        Method::PUT,
        &item_uri("daemonsets", name),
        &spec_only,
    )
    .await;
    assert_eq!(
        after_main["spec"]["template"]["spec"]["containers"][0]["image"],
        json!("busybox:1.36")
    );
    let leaked = after_main
        .get("status")
        .and_then(|s| s.get("numberReady"))
        .and_then(|v| v.as_i64());
    assert_ne!(leaked, Some(999), "main PUT must not write status");

    let status_body = json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "selector": {"matchLabels": {"app": "tampered"}},
            "template": {
                "metadata": {"labels": {"app": "tampered"}},
                "spec": {"containers": [{"name": "x", "image": "evil"}]}
            }
        },
        "status": {"numberReady": 5, "currentNumberScheduled": 5}
    });
    let (_st, after_status) = send_json(
        router.clone(),
        Method::PUT,
        &status_uri("daemonsets", name),
        &status_body,
    )
    .await;
    assert_eq!(
        after_status["spec"]["template"]["spec"]["containers"][0]["image"],
        json!("busybox:1.36"),
        "status PUT must NOT touch spec.template"
    );
    assert_eq!(
        after_status["spec"]["selector"]["matchLabels"]["app"],
        json!(name),
        "status PUT must NOT touch spec.selector"
    );
    assert_eq!(after_status["status"]["numberReady"], json!(5));
}

#[tokio::test]
async fn test_daemonset_strategy_selector_immutability() {
    let (_mem, router) = spawn_router();
    let name = "ds-sel-immut";
    let (_st, created) = create_resource(router.clone(), "daemonsets", daemonset_stub(name)).await;

    let mut mutated = created.clone();
    mutated["spec"]["selector"]["matchLabels"]["app"] = json!("changed");
    let (status, body) =
        send_json(router, Method::PUT, &item_uri("daemonsets", name), &mutated).await;
    assert_eq!(status, 422, "selector change must 422, got: {}", body);
}

#[tokio::test]
async fn test_daemonset_strategy_no_replicas_field() {
    let (_mem, router) = spawn_router();
    let name = "ds-no-replicas";

    let (status, created) =
        create_resource(router.clone(), "daemonsets", daemonset_stub(name)).await;
    assert_eq!(status, 201, "create ds: {}", created);

    // DaemonSet has no `spec.replicas` — defaulting must NOT introduce one.
    assert!(
        created["spec"].get("replicas").is_none() || created["spec"]["replicas"].is_null(),
        "DaemonSet must not have a replicas field after defaulting, got: {}",
        created["spec"]
    );

    let (_gs, stored) = send_get(router, &item_uri("daemonsets", name)).await;
    assert!(
        stored["spec"].get("replicas").is_none() || stored["spec"]["replicas"].is_null(),
        "stored DaemonSet must not have a replicas field, got: {}",
        stored["spec"]
    );
}

// ===========================================================================
// ControllerRevision — minimal create + list + delete roundtrip.
// ControllerRevision is immutable apart from metadata.labels, so the upstream
// `strategy_test.go` mainly checks that it survives create / list / delete.
// ===========================================================================

#[tokio::test]
async fn test_controllerrevision_strategy_create_list_delete_roundtrip() {
    let (_mem, router) = spawn_router();
    let name = "cr-rev-1";

    let body = json!({
        "apiVersion": "apps/v1",
        "kind": "ControllerRevision",
        "metadata": {"name": name, "namespace": TEST_NS},
        "revision": 1,
        "data": {"spec": {"replicas": 3}}
    });
    let (st, created) = send_json(
        router.clone(),
        Method::POST,
        &collection_uri("controllerrevisions"),
        &body,
    )
    .await;
    assert_eq!(st, 201, "create cr: {}", created);
    assert_eq!(created["revision"], json!(1));
    assert_eq!(created["metadata"]["name"], json!(name));

    // List should include the just-created revision.
    let (lst_status, list) = send_get(router.clone(), &collection_uri("controllerrevisions")).await;
    assert_eq!(lst_status, 200);
    let items = list["items"].as_array().expect("items array");
    assert!(
        items.iter().any(|it| it["metadata"]["name"] == json!(name)),
        "list must contain the created revision, got: {:?}",
        items
    );

    // Delete and confirm it's gone.
    let del_status = send_delete(router.clone(), &item_uri("controllerrevisions", name)).await;
    assert!(
        del_status == 200 || del_status == 202,
        "delete cr returned {}",
        del_status
    );

    let (gone_status, _) = send_get(router, &item_uri("controllerrevisions", name)).await;
    assert_eq!(
        gone_status, 404,
        "deleted controllerrevision must be 404 on subsequent GET"
    );
}
