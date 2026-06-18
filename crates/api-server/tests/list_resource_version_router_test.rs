//! Router-level pin of the upstream contract: every `*List` collection
//! response MUST carry a valid, non-empty `metadata.resourceVersion`.
//!
//! client-go's `Reflector.ListAndWatch` (used by every informer, e.g. Lens)
//! performs LIST -> read `list.metadata.resourceVersion` -> WATCH from it. An
//! empty or "0" list RV makes the reflector unable to start a watch, so it
//! falls into a constant relist loop and the UI never updates live.
//!
//! This test lists several namespaced and cluster-scoped kinds (after creating
//! one object) and asserts the serialized `metadata.resourceVersion`:
//!   * is present,
//!   * matches `^[0-9]+$`,
//!   * is not `""` and not `"0"`.
//!
//! Harness mirrors `list_empty_items_router_test.rs`:
//!   * `Arc<MemoryStorage>` backend (its `current_revision()` is a unix
//!     timestamp, always > 0).
//!   * `AlwaysAllowAuthorizer` + `skip_auth=true`.
//!   * `tower::ServiceExt::oneshot` per request.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// Harness: `TestApiServer` (rusternetes-test-support) boots the real
// `build_router` on `MemoryStorage` (whose `current_revision()` is a unix
// timestamp, always > 0) with `--skip-auth`, driven via `tower::oneshot`.
// `send_raw` returns the raw bytes so the wire-level resourceVersion check
// below can inspect them.

/// Assert the list body carries a valid `metadata.resourceVersion`.
fn assert_valid_list_rv(label: &str, raw: &[u8], body: &Value) {
    let raw_str = std::str::from_utf8(raw).expect("response body is not UTF-8");
    let meta = body
        .get("metadata")
        .unwrap_or_else(|| panic!("{label} response missing `metadata`: {raw_str}"));
    let rv = meta.get("resourceVersion").unwrap_or_else(|| {
        panic!("{label} response missing `metadata.resourceVersion`: {raw_str}")
    });
    let rv_str = rv
        .as_str()
        .unwrap_or_else(|| panic!("{label} `metadata.resourceVersion` not a string: {raw_str}"));
    assert!(
        !rv_str.is_empty(),
        "{label} `metadata.resourceVersion` is empty (\"\"); informers cannot watch: {raw_str}",
    );
    assert_ne!(
        rv_str, "0",
        "{label} `metadata.resourceVersion` is \"0\"; client-go rejects initial RV 0: {raw_str}",
    );
    assert!(
        rv_str.bytes().all(|b| b.is_ascii_digit()),
        "{label} `metadata.resourceVersion` must match ^[0-9]+$, got {rv_str:?}: {raw_str}",
    );
    // Wire-level double-check: never `"resourceVersion":""`.
    assert!(
        !raw_str.contains("\"resourceVersion\":\"\""),
        "{label} response must NOT contain empty resourceVersion on the wire: {raw_str}",
    );
}

async fn create_namespace(state: &TestApiServer, ns: &str) {
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": ns },
    });
    let (status, _body) = state.post("/api/v1/namespaces", &ns_body).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create returned {status}",
    );
}

/// Namespaced configmaps: the verified-failing case. After creating one CM the
/// list RV must be a valid revision.
#[tokio::test]
async fn list_configmaps_has_valid_resource_version() {
    let state = TestApiServer::new();
    let ns = "rv-cm-ns";
    create_namespace(&state, ns).await;

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "cm-1", "namespace": ns },
        "data": { "k": "v" },
    });
    let (status, _b) = state
        .post(&format!("/api/v1/namespaces/{ns}/configmaps"), &cm)
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "configmap create returned {status}",
    );

    let uri = format!("/api/v1/namespaces/{ns}/configmaps");
    let (status, raw, body) = state.send_raw("GET", &uri, None, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} expected 200; got {status}"
    );
    assert_eq!(body["kind"], "ConfigMapList");
    assert_valid_list_rv("ConfigMapList", &raw, &body);
}

/// Namespaced pods.
#[tokio::test]
async fn list_pods_has_valid_resource_version() {
    let state = TestApiServer::new();
    let ns = "rv-pod-ns";
    create_namespace(&state, ns).await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "pod-1", "namespace": ns },
        "spec": { "containers": [{ "name": "c", "image": "nginx" }] },
    });
    let (status, _b) = state
        .post(&format!("/api/v1/namespaces/{ns}/pods"), &pod)
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "pod create returned {status}",
    );

    let uri = format!("/api/v1/namespaces/{ns}/pods");
    let (status, raw, body) = state.send_raw("GET", &uri, None, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} expected 200; got {status}"
    );
    assert_eq!(body["kind"], "PodList");
    assert_valid_list_rv("PodList", &raw, &body);
}

/// Cluster-scoped namespaces list.
#[tokio::test]
async fn list_namespaces_has_valid_resource_version() {
    let state = TestApiServer::new();
    create_namespace(&state, "rv-ns-a").await;

    let uri = "/api/v1/namespaces";
    let (status, raw, body) = state.send_raw("GET", uri, None, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} expected 200; got {status}"
    );
    assert_eq!(body["kind"], "NamespaceList");
    assert_valid_list_rv("NamespaceList", &raw, &body);
}

/// Empty namespaced list (no items) must still carry a valid RV — this is the
/// case that broke Lens (empty collection -> `resourceVersion: ""`).
#[tokio::test]
async fn list_empty_configmaps_has_valid_resource_version() {
    let state = TestApiServer::new();
    let ns = "rv-empty-cm-ns";
    create_namespace(&state, ns).await;

    let uri = format!("/api/v1/namespaces/{ns}/configmaps");
    let (status, raw, body) = state.send_raw("GET", &uri, None, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} expected 200; got {status}"
    );
    assert_eq!(body["kind"], "ConfigMapList");
    assert_valid_list_rv("ConfigMapList (empty)", &raw, &body);
}

/// Secrets list goes through the `List::new`-only path (no explicit
/// `current_revision()` stamp historically). The list RV must equal the store
/// revision, not the small max-item resourceVersion. With `MemoryStorage` the
/// store revision is a unix timestamp (> 1e9), whereas freshly created object
/// RVs are tiny — so a list RV below the threshold proves the handler ignored
/// `current_revision()`.
#[tokio::test]
async fn list_secrets_uses_store_revision_not_item_rv() {
    let state = TestApiServer::new();
    let ns = "rv-secret-ns";
    create_namespace(&state, ns).await;

    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "s-1", "namespace": ns },
        "stringData": { "k": "v" },
    });
    let (status, _b) = state
        .post(&format!("/api/v1/namespaces/{ns}/secrets"), &secret)
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "secret create returned {status}",
    );

    let uri = format!("/api/v1/namespaces/{ns}/secrets");
    let (status, raw, body) = state.send_raw("GET", &uri, None, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} expected 200; got {status}"
    );
    assert_eq!(body["kind"], "SecretList");
    assert_valid_list_rv("SecretList", &raw, &body);

    let rv: i64 = body["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        rv > 1_000_000_000,
        "SecretList resourceVersion={rv} looks like a max-item RV, not the store \
         revision from current_revision(); informers watch from the store revision: {}",
        std::str::from_utf8(&raw).unwrap(),
    );
}
