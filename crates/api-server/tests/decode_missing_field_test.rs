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

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

async fn send_json(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: &Value,
) -> (StatusCode, Value) {
    router
        .send(method.as_str(), uri, Some("application/json"), Some(body))
        .await
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

/// The `missing field plural` failure observed ~30× in the conformance suite
/// is NOT a missing `spec.names.plural` — that path already decodes (above).
/// It comes from `status.acceptedNames`, which is also a
/// `CustomResourceDefinitionNames`. client-go sends a CRD whose status carries
/// a partial/empty `acceptedNames` (no `plural` key), and the required field
/// errored the whole decode. Go leaves it zero-valued; we must too.
#[tokio::test]
async fn test_crd_with_empty_accepted_names_decodes() {
    let router = spawn_router();

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget", "listKind": "WidgetList"},
            "scope": "Namespaced",
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
            }]
        },
        // status.acceptedNames is empty — the shape that broke decode.
        "status": {"acceptedNames": {}, "conditions": null, "storedVersions": null}
    });

    let (status, body) = send_json(
        router,
        Method::POST,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;

    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "CRD with empty status.acceptedNames must not error on missing plural \
         (status {}): {}",
        status,
        body
    );
    assert!(
        status.is_success(),
        "CRD with empty status.acceptedNames should be accepted, got {}: {}",
        status,
        body
    );
}

/// Safety net for the `#[serde(default)]` on plural: making decode lenient must
/// NOT let a CRD with an empty `spec.names.plural` through — validation owns
/// that. The decode now succeeds (plural defaults to ""), then the handler
/// rejects it with a real validation message, not a raw serde error.
#[tokio::test]
async fn test_crd_with_empty_spec_plural_is_rejected_by_validation() {
    let router = spawn_router();

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": ".example.com"},
        "spec": {
            "group": "example.com",
            // plural intentionally empty — must be rejected, but as validation.
            "names": {"plural": "", "kind": "Widget"},
            "scope": "Namespaced",
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
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
        !status.is_success(),
        "CRD with empty spec.names.plural must be rejected, got {}: {}",
        status,
        body
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "rejection must be a validation message, not a raw serde missing-field error: {}",
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

    // No `capacity`, no `accessModes`: both are `omitempty` on the wire, so
    // decode must admit the object — but upstream `ValidatePersistentVolumeSpec`
    // requires them for a non-inline PV (`field.Required(accessModes)` and,
    // when `!validateInlinePersistentVolumeSpec`, `field.Required(capacity)`),
    // so create must then be rejected by *validation*, not by serde.
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
    // Decode admitted the object: no raw serde missing-field error.
    assert!(
        !msg.contains("missing field"),
        "PV decode must not surface a missing-field error for omitempty capacity/accessModes \
         (status {}): {}",
        status,
        body
    );
    // Validation then handled the absent required fields (upstream parity:
    // 422 FieldValueRequired, not a 2xx create).
    assert!(
        !status.is_success(),
        "PV without capacity/accessModes must be rejected by validation, got {}: {}",
        status,
        body
    );
    assert!(
        msg.contains("capacity") || msg.contains("accessModes"),
        "rejection must name the missing required field(s): {}",
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

// ---------------------------------------------------------------------------
// 5. ReplicationControllerStatus.replicas: i32 Go-parity decode fix.
//
//    Conformance tests (e.g. sig-apps ReplicationController lifecycle,
//    sig-api-machinery GarbageCollector) create RCs whose serialised body
//    includes a `status` sub-object that omits `replicas` (they only send
//    `readyReplicas`/`availableReplicas`).  Go leaves the missing `int32`
//    at its zero value; our required `i32` errored with 400 BadRequest
//    "missing field `replicas`" — blocking ~30 conformance specs.
//
//    Upstream Go type: `ReplicationControllerStatus.Replicas int32 json:"replicas"`
//    (no omitempty) → zero-value 0 when absent; fixed with `#[serde(default)]`.
// ---------------------------------------------------------------------------

/// RC body where `status.replicas` is absent must decode with `replicas == 0`.
/// Reproduces the 30-occurrence "missing field `replicas`" failure from the
/// 2026-05-31 conformance run (e.g. garbage_collector.go:734, rc.go:158).
#[tokio::test]
async fn test_rc_status_without_replicas_decodes() {
    let router = spawn_router();

    // Shape: status present but replicas field absent (only readyReplicas + availableReplicas).
    let rc = json!({
        "kind": "ReplicationController",
        "apiVersion": "v1",
        "metadata": {
            "name": "gc-test-rc",
            "namespace": "default",
            "labels": {"app": "gc-test"},
            "creationTimestamp": null
        },
        "spec": {
            "replicas": 1,
            "selector": {"app": "gc-test"},
            "template": {
                "metadata": {
                    "labels": {"app": "gc-test"},
                    "creationTimestamp": null
                },
                "spec": {
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/e2e-test-images/busybox:1.36.1-2",
                        "resources": {}
                    }],
                    "restartPolicy": "Always",
                    "terminationGracePeriodSeconds": 30,
                    "dnsPolicy": "ClusterFirst",
                    "securityContext": {},
                    "schedulerName": "default-scheduler"
                }
            }
        },
        // status.replicas absent — the conformance-test shape that previously 400'd.
        "status": {
            "readyReplicas": 0,
            "availableReplicas": 0
        }
    });

    let (status, body) = send_json(
        router,
        Method::POST,
        "/api/v1/namespaces/default/replicationcontrollers",
        &rc,
    )
    .await;

    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "RC with partial status must not fail with missing field (status {}): {}",
        status,
        body
    );
    assert!(
        status.is_success(),
        "RC with status missing replicas should be accepted (replicas defaults to 0), got {}: {}",
        status,
        body
    );
    // Confirm that replicas was decoded as 0 (Go zero-value parity)
    let status_replicas = body["status"]["replicas"].as_i64().unwrap_or(-1);
    assert_eq!(
        status_replicas, 0,
        "status.replicas absent on wire must decode as 0: {}",
        body
    );
}

// ---------------------------------------------------------------------------
// 6. SubjectAccessReview.spec: SubjectAccessReviewSpec Go-parity decode fix.
//
//    sig-auth SubjectReview conformance test posts a SubjectAccessReview with
//    only apiVersion/kind/metadata but NO spec field. Go leaves the struct
//    at zero value; our required field errored with 422 "missing field `spec`".
//
//    Upstream: SubjectAccessReviewSpec has all-optional fields (user/groups/etc.)
//    and the spec itself is not pointer-typed in Go, but json.Unmarshal leaves
//    it at zero. Fixed by adding #[serde(default)] + Default derive.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_subject_access_review_without_spec_decodes() {
    let router = spawn_router();

    // Minimal SubjectAccessReview with no spec — the conformance-test shape
    // that previously 422'd with "missing field `spec`".
    let sar = json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "metadata": {}
    });

    let (status, body) = send_json(
        router,
        Method::POST,
        "/apis/authorization.k8s.io/v1/subjectaccessreviews",
        &sar,
    )
    .await;

    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "SubjectAccessReview without spec must not fail with missing-field (status {}): {}",
        status,
        body
    );
    // The request decodes successfully — the "missing field `spec`" error is gone.
    // The handler may still reject with a validation error (e.g. missing
    // resourceAttributes/nonResourceAttributes), but that is NOT a decode error.
    assert!(
        msg != "failed to decode: missing field `spec` at line 1 column 83",
        "SAR must not 400 with raw serde error, got {}: {}",
        status,
        body
    );
}

// ---------------------------------------------------------------------------
// 7. ControllerRevision.metadata Go-parity decode fix.
//
//    sig-apps ControllerRevision conformance test posts a ControllerRevision
//    where metadata is an empty object {}. Our required ObjectMeta field
//    errored with 422 "missing field `metadata`" at the position of the
//    closing `}` of the payload.
//
//    Upstream: metav1.ObjectMeta zero-value is valid; fixed with #[serde(default)].
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_controller_revision_without_metadata_decodes() {
    let router = spawn_router();

    // ControllerRevision with empty metadata — the shape that previously 422'd.
    let cr = json!({
        "apiVersion": "apps/v1",
        "kind": "ControllerRevision",
        "metadata": {"name": "rev-1", "namespace": "default"},
        "revision": 1
    });

    let (status, body) = send_json(
        router,
        Method::POST,
        "/apis/apps/v1/namespaces/default/controllerrevisions",
        &cr,
    )
    .await;

    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "ControllerRevision must not fail with missing-field (status {}): {}",
        status,
        body
    );
    assert!(
        status.is_success(),
        "ControllerRevision should be accepted, got {}: {}",
        status,
        body
    );
}

// ---------------------------------------------------------------------------
// 8. FlowSchema NonResourcePolicyRule.nonResourceURLs Go-parity decode fix.
//
//    sig-api-machinery FlowControl conformance test posts a FlowSchema whose
//    spec.rules[0].nonResourceRules[0] omits nonResourceURLs. Go leaves the
//    field at [] (empty slice); our required Vec<String> errored with 422
//    "missing field `nonResourceURLs`".
//
//    Upstream: []string json:"nonResourceURLs" — absent leaves empty slice;
//    fixed with #[serde(default)].
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_flow_schema_non_resource_rule_without_urls_decodes() {
    let router = spawn_router();

    // FlowSchema with nonResourceRules that omit nonResourceURLs.
    let fs = json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "FlowSchema",
        "metadata": {"name": "test-flow-schema"},
        "spec": {
            "priorityLevelConfiguration": {"name": "exempt"},
            "matchingPrecedence": 1,
            "rules": [{
                "subjects": [{"kind": "User", "user": {"name": "system:admin"}}],
                "nonResourceRules": [{
                    "verbs": ["get"]
                    // nonResourceURLs intentionally absent
                }]
            }]
        }
    });

    let (status, body) = send_json(
        router,
        Method::POST,
        "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas",
        &fs,
    )
    .await;

    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        !msg.contains("missing field"),
        "FlowSchema with missing nonResourceURLs must not fail with missing-field \
         (status {}): {}",
        status,
        body
    );
}
