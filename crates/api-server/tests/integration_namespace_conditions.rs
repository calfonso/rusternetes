//! Upstream-mirror RED-state TDD pins for the Kubernetes v1.35 integration
//! suite at `test/integration/namespace/ns_conditions_test.go`.
//!
//! Source of truth (permalink):
//! https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/namespace/ns_conditions_test.go
//!
//! This file mirrors every `func Test*` in the upstream file as a
//! `#[tokio::test]` of the same name. The transport layer is exercised via an
//! inline `spawn_router()` helper that calls
//! `rusternetes_api_server::router::build_router` against an
//! `ApiServerState` backed by an `Arc<MemoryStorage>` so each test owns its
//! own storage and is trivially parallel. Requests go through
//! `tower::ServiceExt::oneshot` to mimic upstream's `clientset` calls.
//!
//! Both upstream tests depend on a running `NamespaceController` to
//! finalize a namespace and stamp the deletion conditions. We mirror that by
//! manually invoking `NamespaceController::reconcile_all()` (the
//! work-queue-aware path) instead of spawning the long-lived `run()` loop.
//!
//! ### RED-state expectations
//!
//! Both tests are expected to FAIL at the time of authoring — this is a
//! deliberate TDD pin, not an oversight. The failures encode upstream
//! contracts that our implementation has not yet honoured:
//!
//! - `TestNamespaceCondition` exercises the full
//!   `Delete -> finalize -> conditions` cycle.  The condition messages our
//!   controller produces drift from upstream (e.g. `"may be waiting for
//!   finalization"` vs upstream `"may be waiting on finalization"`, and
//!   `"All content successfully removed"` vs upstream
//!   `"Some resources are remaining: deployments.apps has 1 resource
//!   instances"`). We also do not yet special-case `deployments.apps` /
//!   `custom.io/finalizer` in the condition body.
//! - `TestNamespaceLabels` exercises PR kubernetes/kubernetes#96968 — the
//!   namespace registry must auto-attach the
//!   `kubernetes.io/metadata.name=<name>` label on every create (including
//!   `generateName` flows). Our `handlers::namespace::create` does not yet
//!   set this label; it also does not resolve `generateName` into a `name`.
//!
//! These tests will go GREEN as those gaps close.

// Upstream Go test names are preserved verbatim (`TestNamespaceCondition`,
// `TestNamespaceLabels`) so the file mirrors the source 1:1 — `cargo test`
// search by upstream symbol works exactly as it does in `go test`.
#![allow(non_snake_case)]

use axum::http::{Method, StatusCode};
use rusternetes_common::resources::{Deployment, Pod};
use rusternetes_controller_manager::controllers::namespace::NamespaceController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`, preserving this
// file's `(router, mem) = spawn_router()` + `send(&router, Method::X, …)` call
// sites. `mem` is the backing `MemoryStorage` so tests can seed and inspect
// storage directly (mirroring upstream's `dynamicClient` escape hatch).
// ---------------------------------------------------------------------------

fn spawn_router() -> (TestApiServer, Arc<MemoryStorage>) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (api, mem)
}

/// Issue a single request and return `(status, parsed JSON body)`.
async fn send(
    router: &TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    router.send(method.as_str(), uri, content_type, body).await
}

// ---------------------------------------------------------------------------
// TestNamespaceCondition
// Upstream: ns_conditions_test.go:39 (release-1.35)
//
// Setup mirrors `namespaceLifecycleSetup(t)` + the inline body:
//  1. Create the namespace via REST.
//  2. Seed a pod and a deployment (the deployment carries
//     `custom.io/finalizer`) under that namespace.
//  3. DELETE the namespace.
//  4. Drive the NamespaceController reconciler.
//  5. Re-GET the namespace and assert five named conditions appear with the
//     exact messages upstream encodes.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn TestNamespaceCondition() {
    let (router, mem) = spawn_router();
    let ns_name = "test-namespace-conditions";

    // (1) Create the namespace via the REST surface, matching upstream's
    // `kubeClient.CoreV1().Namespaces().Create(...)`.
    let (status, _) = send(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": ns_name },
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "namespace create should return 201, got {status}"
    );

    // (2) Seed a pod and a deployment (with `custom.io/finalizer`) inside the
    // namespace via the dynamic-client equivalent — direct storage writes
    // through the same backend the router uses. Upstream's `etcd.GetEtcd
    // StorageDataForNamespace(nsName)` produces canonical stubs; we use the
    // minimum spec the controller's discovery path inspects (name + ns +
    // finalizers).
    let pod: Pod = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": ns_name,
        },
        "spec": {
            "containers": [{ "name": "c", "image": "registry.k8s.io/pause:3.10" }],
        },
    }))
    .expect("pod stub deserializes");
    mem.create::<Pod>(&build_key("pods", Some(ns_name), "test-pod"), &pod)
        .await
        .expect("seed pod");

    let deployment: Deployment = serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "test-deployment",
            "namespace": ns_name,
            "finalizers": ["custom.io/finalizer"],
        },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": "test" } },
            "template": {
                "metadata": { "labels": { "app": "test" } },
                "spec": {
                    "containers": [{ "name": "c", "image": "registry.k8s.io/pause:3.10" }],
                },
            },
        },
    }))
    .expect("deployment stub deserializes");
    mem.create::<Deployment>(
        &build_key("deployments", Some(ns_name), "test-deployment"),
        &deployment,
    )
    .await
    .expect("seed deployment with finalizer");

    // (3) DELETE the namespace via REST (sets deletionTimestamp + kicks the
    // controller path).
    let (status, _) = send(
        &router,
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns_name}"),
        None,
    )
    .await;
    assert!(
        status.is_success() || status == StatusCode::ACCEPTED,
        "namespace delete should accept the request, got {status}"
    );

    // (4) Drive the controller — upstream calls `go nsController.Run(ctx, 5)`
    // and polls. We run reconcile_all() twice: the first pass observes
    // finalizers and stamps the initial conditions; the second pass crosses
    // into phase-2 where the "remaining" conditions reach their final form.
    let controller = NamespaceController::new(mem.clone());
    controller.reconcile_all().await.expect("first reconcile");
    controller.reconcile_all().await.expect("second reconcile");

    // (5) Re-GET the namespace and assert exactly the five upstream-spec'd
    // conditions appear with the exact message strings. We accept either the
    // controller-stamped status or the storage-level object as the source of
    // truth — upstream's `kubeClient.CoreV1().Namespaces().Get(...)` goes via
    // REST, so we mirror that.
    let (status, body) = send(
        &router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns_name}"),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "namespace get should succeed: {body:?}"
    );

    let conditions = body
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();

    let want_pairs: &[(&str, &str)] = &[
        (
            "NamespaceDeletionGroupVersionParsingFailure",
            "All legacy kube types successfully parsed",
        ),
        (
            "NamespaceDeletionDiscoveryFailure",
            "All resources successfully discovered",
        ),
        (
            "NamespaceDeletionContentFailure",
            "All content successfully deleted, may be waiting on finalization",
        ),
        (
            "NamespaceContentRemaining",
            "Some resources are remaining: deployments.apps has 1 resource instances",
        ),
        (
            "NamespaceFinalizersRemaining",
            "Some content in the namespace has finalizers remaining: custom.io/finalizer in 1 resource instances",
        ),
    ];
    let mut found = 0;
    for (want_type, want_msg) in want_pairs {
        let hit = conditions.iter().any(|c| {
            c.get("type").and_then(|v| v.as_str()) == Some(*want_type)
                && c.get("message").and_then(|v| v.as_str()) == Some(*want_msg)
        });
        if hit {
            found += 1;
        }
    }
    assert_eq!(
        found, 5,
        "expected all 5 deletion conditions, got {found}; observed conditions = {conditions:#?}"
    );
}

// ---------------------------------------------------------------------------
// TestNamespaceLabels
// Upstream: ns_conditions_test.go:104 (release-1.35) — pins PR
// kubernetes/kubernetes#96968. Each created namespace must auto-attach
// `kubernetes.io/metadata.name=<name>`. The upstream test also exercises a
// list-then-check pass.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn TestNamespaceLabels() {
    let (router, _mem) = spawn_router();

    // Upstream uses `GenerateName: "test-namespace-labels-generated"`. The
    // server is expected to materialise a unique `metadata.name`, then
    // populate `metadata.labels["kubernetes.io/metadata.name"]` with it.
    let (status, body) = send(
        &router,
        Method::POST,
        "/api/v1/namespaces",
        Some(&json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "generateName": "test-namespace-labels-generated" },
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "namespace generate-name create should return 201, got {status}: {body:?}"
    );

    let name = body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .expect("server must materialise metadata.name from generateName")
        .to_string();
    assert!(
        !name.is_empty(),
        "materialised namespace name must be non-empty"
    );

    let label = body
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.get("kubernetes.io/metadata.name"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    assert_eq!(
        label.as_deref(),
        Some(name.as_str()),
        "expected metadata.labels[\"kubernetes.io/metadata.name\"] = {name:?}, got {label:?}"
    );

    // Upstream also lists every namespace and re-asserts the invariant. The
    // REST list endpoint returns `{items: [...]}`.
    let (list_status, list_body) = send(&router, Method::GET, "/api/v1/namespaces", None).await;
    assert_eq!(list_status, StatusCode::OK, "list status should be 200");
    let items = list_body
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        !items.is_empty(),
        "list namespaces must include at least the one we just created"
    );
    for ns in &items {
        let ns_name = ns
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let ns_label = ns
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.get("kubernetes.io/metadata.name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert_eq!(
            ns_name, ns_label,
            "every namespace must have kubernetes.io/metadata.name == name (got name={ns_name:?}, label={ns_label:?})"
        );
    }
}
