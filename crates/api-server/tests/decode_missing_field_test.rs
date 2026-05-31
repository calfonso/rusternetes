//! Regression tests for strict-decode wrongly rejecting valid resources with
//! "missing field" errors observed in the full conformance suite.
//!
//! Three independent conformance failures are reproduced here, each driving the
//! in-process Axum router backed by `MemoryStorage` with the exact JSON shape a
//! real Kubernetes client sends:
//!
//! 1. CustomResourceDefinition with `spec.names.plural` present still failed
//!    with `failed to decode CRD: missing field 'plural'`
//!    (sig-api-machinery CustomResourceDefinition / AggregatedDiscovery /
//!    FieldValidation).
//! 2. PersistentVolume with `spec.capacity` present still failed with
//!    `failed to decode: missing field 'capacity'` (sig-storage CSI
//!    Conformance).
//! 3. A near-empty Pod body decoded with `missing field 'metadata'`
//!    (sig-node PreStop).
//!
//! Harness mirrors `tests/decoder_strict_fields_test.rs`.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    Router,
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
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

fn spawn_router() -> Router {
    let mem = Arc::new(MemoryStorage::new());
    build_router(make_state(mem), None)
}

async fn send_json(router: Router, method: Method, uri: &str, body: &Value) -> (StatusCode, Value) {
    let bytes = serde_json::to_vec(body).unwrap();
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(bytes))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, v)
}

// ---------------------------------------------------------------------------
// 1. CRD with spec.names.plural present must not report "missing field plural"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_crd_with_plural_is_accepted() {
    let router = spawn_router();

    // Shape mirrors a real apiextensions.k8s.io/v1 CustomResourceDefinition as
    // emitted by the conformance suite: plural lives at spec.names.plural and
    // IS present here.
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "e2e-tests.example.com"},
        "spec": {
            "group": "example.com",
            "names": {
                "plural": "e2e-tests",
                "singular": "e2e-test",
                "kind": "E2ETest",
                "listKind": "E2ETestList"
            },
            "scope": "Namespaced",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "x-kubernetes-preserve-unknown-fields": true
                    }
                }
            }]
        }
    });

    let (status, body) = send_json(
        router,
        Method::POST,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;

    assert!(
        status.is_success(),
        "CRD with spec.names.plural should be accepted, got {}: {}",
        status,
        body
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "must not report a missing field for a valid CRD: {}",
        body
    );
}

// ---------------------------------------------------------------------------
// 2. PersistentVolume with spec.capacity present must not report
//    "missing field capacity"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pv_with_capacity_is_accepted() {
    let router = spawn_router();

    // Mirrors a sig-storage CSI conformance PV: capacity is a map under
    // spec.capacity and IS present.
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "csi-pv-conformance"},
        "spec": {
            "capacity": {"storage": "5Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "csi": {
                "driver": "csi-mock",
                "volumeHandle": "vol-handle-1"
            }
        }
    });

    let (status, body) = send_json(router, Method::POST, "/api/v1/persistentvolumes", &pv).await;

    assert!(
        status.is_success(),
        "PV with spec.capacity should be accepted, got {}: {}",
        status,
        body
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "must not report a missing field for a valid PV: {}",
        body
    );
}

/// Reproduces sig-storage PersistentVolumes CSI Conformance
/// "should apply changes to a pv/pvc status": the framework posts a PV whose
/// `spec.capacity` is absent on the wire (capacity is `+optional` /
/// `json:",omitempty"` in upstream `PersistentVolumeSpec`). Our struct wrongly
/// required it, so decode failed with `missing field 'capacity'` instead of
/// admitting the object and letting validation handle it.
#[tokio::test]
async fn test_pv_without_capacity_decodes() {
    let router = spawn_router();

    // No `capacity`, no `accessModes` — both optional upstream.
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "csi-pv-no-capacity"},
        "spec": {
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "csi-sc",
            "csi": {
                "driver": "csi-mock",
                "volumeHandle": "vol-handle-2"
            }
        }
    });

    let (status, body) = send_json(router, Method::POST, "/api/v1/persistentvolumes", &pv).await;

    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "PV decode must not surface a missing-field error for optional capacity/accessModes \
         (status {}): {}",
        status,
        body
    );
    assert!(
        status.is_success(),
        "PV without capacity should decode and be created, got {}: {}",
        status,
        body
    );
}

// ---------------------------------------------------------------------------
// 3. RoleBinding whose roleRef omits apiGroup must decode (Go-parity).
//    Reproduces the [sig-auth] webhook/SubjectReview BeforeEach failure:
//      POST rolebindings -> 422 "roleRef: missing field `apiGroup`"
//    Go's json.Unmarshal leaves a missing scalar at its zero value; our
//    required String errored. Decode must admit it and let validation enforce.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rolebinding_roleref_without_apigroup_decodes() {
    let router = spawn_router();

    let rb = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "webhook-rb", "namespace": "default"},
        // roleRef intentionally omits apiGroup (the wire shape that broke us).
        "roleRef": {"kind": "Role", "name": "webhook-role"},
        "subjects": [{"kind": "ServiceAccount", "name": "default", "namespace": "default"}]
    });

    let (status, body) = send_json(
        router,
        Method::POST,
        "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings",
        &rb,
    )
    .await;

    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "RoleBinding decode must not reject a roleRef missing apiGroup (status {}): {}",
        status,
        body
    );
    assert!(
        status.is_success(),
        "RoleBinding with roleRef.apiGroup omitted should be accepted, got {}: {}",
        status,
        body
    );
}

// ---------------------------------------------------------------------------
// 4. PreStop: a Pod body that omits a section must report the *right* field,
//    not a confusing "missing field 'metadata'".
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pod_missing_metadata_reports_clear_error() {
    let router = spawn_router();

    // A Pod with spec but no metadata. Upstream rejects this for the missing
    // name, but the decode error must not be a misleading low-level
    // "missing field 'metadata' at line 1 column N".
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {"containers": [{"name": "c1", "image": "busybox"}]}
    });

    let (status, body) = send_json(
        router,
        Method::POST,
        "/api/v1/namespaces/default/pods",
        &pod,
    )
    .await;

    // Either it succeeds (metadata defaulted) or fails with a meaningful
    // validation error — but never a raw serde "missing field 'metadata'".
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field 'metadata'") && !msg.contains("missing field `metadata`"),
        "Pod decode must not surface a raw missing-metadata serde error (status {}): {}",
        status,
        body
    );
}
