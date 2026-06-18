//! Regression pin for #1161: the api-server must actually REMOVE a namespace
//! from storage once its finalizers drain while it is Terminating.
//!
//! Live bug (2026-06-15): nothing in the api-server ever deleted a Namespace
//! object. `delete_ns` only set `Terminating` + `deletionTimestamp` (and
//! re-added the `kubernetes` finalizer), and `update` (which also serves the
//! `/finalize` subresource) was a plain upsert. So every namespace leaked
//! `Terminating` forever and the namespace controller spun re-deleting them on
//! false `Ok` (30k+ no-op delete loops observed). Unit tests passed only
//! because they drove `MemoryStorage` directly, bypassing the HTTP handlers.
//!
//! Upstream contract (`pkg/registry/core/namespace/storage/storage.go`
//! release-1.35): `ShouldDeleteNamespaceDuringUpdate` =
//! `len(ns.Spec.Finalizers) == 0 && genericregistry.ShouldDeleteDuringUpdate(...)`
//! — the object is removed on the finalize/update path once finalizers drain
//! and `DeletionTimestamp` is set. rusternetes keeps the namespace lifecycle
//! finalizer in `metadata.finalizers` (its convention), so this mirror checks
//! the same condition there.
//!
//! Harness mirrors `integration_configmap_lifecycle.rs`: an
//! `Arc<MemoryStorage>` wired through `build_router` and driven with tower
//! `oneshot`, reading storage state via the HTTP surface so the handler logic
//! (not just the backend) is exercised.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

// Harness: `TestApiServer` (rusternetes-test-support) — `build_router` on
// `MemoryStorage` with `--skip-auth`, driven via `tower::oneshot`.

/// DELETE marks the namespace Terminating and keeps it (the `kubernetes`
/// finalizer added on create blocks immediate removal) — the controller has
/// not finished cleanup yet.
#[tokio::test]
async fn delete_keeps_namespace_terminating_until_finalizers_drained() {
    let state = TestApiServer::new();

    let (sc, _) = state
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-keep"}}),
        )
        .await;
    assert_eq!(sc, StatusCode::CREATED, "namespace create should succeed");

    let (sc, _) = state.delete("/api/v1/namespaces/ns-keep").await;
    assert!(sc.is_success(), "delete should accept (202/200), got {sc}");

    // Still present, now Terminating with the kubernetes finalizer.
    let (sc, ns) = state.get("/api/v1/namespaces/ns-keep").await;
    assert_eq!(
        sc,
        StatusCode::OK,
        "namespace must persist while finalizing"
    );
    assert_eq!(
        ns.pointer("/status/phase").and_then(|p| p.as_str()),
        Some("Terminating")
    );
    assert!(
        ns.pointer("/metadata/deletionTimestamp")
            .and_then(|d| d.as_str())
            .is_some(),
        "deletionTimestamp must be set"
    );
    assert!(
        ns.pointer("/metadata/finalizers")
            .and_then(|f| f.as_array())
            .map(|f| f.iter().any(|x| x == "kubernetes"))
            .unwrap_or(false),
        "kubernetes finalizer must keep the namespace around"
    );
}

/// Once the controller finishes cleanup and finalizes the namespace with an
/// empty finalizer list, the api-server MUST remove it from storage. This is
/// the bug in #1161: previously the finalize/update was a no-op upsert and the
/// namespace leaked Terminating forever.
#[tokio::test]
async fn finalize_with_drained_finalizers_removes_namespace() {
    let state = TestApiServer::new();

    state
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-gc"}}),
        )
        .await;
    state.delete("/api/v1/namespaces/ns-gc").await;

    // Read the Terminating object as the controller would.
    let (_, mut ns) = state.get("/api/v1/namespaces/ns-gc").await;
    assert!(
        ns.pointer("/metadata/deletionTimestamp").is_some(),
        "precondition: namespace Terminating"
    );

    // Controller drains finalizers and PUTs the /finalize subresource.
    if let Some(meta) = ns.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert("finalizers".to_string(), json!([]));
    }
    let (sc, _) = state.put("/api/v1/namespaces/ns-gc/finalize", &ns).await;
    assert!(sc.is_success(), "finalize should succeed, got {sc}");

    // The namespace must now be GONE from storage.
    let (sc, _) = state.get("/api/v1/namespaces/ns-gc").await;
    assert_eq!(
        sc,
        StatusCode::NOT_FOUND,
        "namespace with drained finalizers + deletionTimestamp MUST be removed from storage (#1161)"
    );
}

/// A namespace finalized with a non-`kubernetes` custom finalizer still present
/// must NOT be removed — its external owner has not released it yet.
#[tokio::test]
async fn finalize_with_remaining_custom_finalizer_keeps_namespace() {
    let state = TestApiServer::new();

    state
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-custom"}}),
        )
        .await;
    state.delete("/api/v1/namespaces/ns-custom").await;

    let (_, mut ns) = state.get("/api/v1/namespaces/ns-custom").await;
    // Controller removed `kubernetes` but a custom finalizer remains.
    if let Some(meta) = ns.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert("finalizers".to_string(), json!(["example.com/keep"]));
    }
    let (sc, _) = state
        .put("/api/v1/namespaces/ns-custom/finalize", &ns)
        .await;
    assert!(sc.is_success());

    let (sc, _) = state.get("/api/v1/namespaces/ns-custom").await;
    assert_eq!(
        sc,
        StatusCode::OK,
        "namespace with a remaining custom finalizer must stay Terminating"
    );
}
