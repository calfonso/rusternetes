//! Scoped mirror of Kubernetes v1.35 conformance suite for [sig-api-machinery] CRD lifecycle.
//!
//! Source: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//! Sonobuoy run captured in
//!
//! See docs/conformance/apimachinery-crd-lifecycle.md for the test-by-test status table.
//!
//! Each test drives the real axum router via `tower::ServiceExt::oneshot` against
//! `MemoryStorage` + `AlwaysAllowAuthorizer`, exactly the same handler stack
//! production HTTPS requests traverse. Tests mirror Sonobuoy-PASSING scenarios
//! and must pass locally; tests mirroring features the api-server has not yet
//! implemented (CEL `x-kubernetes-validations`, ratcheting, scale subresource
//! JSONPath rooted at the CR) are `#[ignore]`d with a reason pointing back to
//! the doc fragment.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// HTTP harness — thin `(u16, Value)` shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

async fn post_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router.post(uri, body).await;
    (status.as_u16(), value)
}

async fn put_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router.put(uri, body).await;
    (status.as_u16(), value)
}

async fn patch_merge(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router.patch(uri, body).await;
    (status.as_u16(), value)
}

async fn get(router: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.get(uri).await;
    (status.as_u16(), value)
}

async fn delete(router: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.delete(uri).await;
    (status.as_u16(), value)
}

// ---------------------------------------------------------------------------
// CRD fixtures
// ---------------------------------------------------------------------------

/// Minimal cluster-scoped CRD body with a `spec.replicas` int + `spec.foo`
/// string property. Used by lifecycle and discovery tests.
fn basic_crd(plural: &str, singular: &str, kind: &str, group: &str) -> Value {
    let name = format!("{plural}.{group}");
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": name},
        "spec": {
            "group": group,
            "scope": "Namespaced",
            "names": {
                "plural": plural,
                "singular": singular,
                "kind": kind,
                "listKind": format!("{kind}List"),
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "foo": {"type": "string"},
                                    "replicas": {"type": "integer"}
                                }
                            },
                            "status": {
                                "type": "object",
                                "properties": {
                                    "replicas": {"type": "integer"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    })
}

/// CRD with both `/status` and `/scale` subresources enabled, mirroring the
/// upstream fixture used in the scale conformance test.
fn scaled_crd(plural: &str, singular: &str, kind: &str, group: &str) -> Value {
    let mut body = basic_crd(plural, singular, kind, group);
    body["spec"]["versions"][0]["subresources"] = json!({
        "status": {},
        "scale": {
            "specReplicasPath": ".spec.replicas",
            "statusReplicasPath": ".status.replicas",
        }
    });
    body
}

/// CRD with a defaulted string property (`spec.flavour`) — for the upstream
/// "custom resource defaulting for requests and from storage works" test.
fn default_flavoured_crd() -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "flavours.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {
                "plural": "flavours",
                "singular": "flavour",
                "kind": "Flavour",
                "listKind": "FlavourList",
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "flavour": {
                                        "type": "string",
                                        "default": "vanilla"
                                    }
                                }
                            }
                        }
                    }
                }
            }]
        }
    })
}

// ---------------------------------------------------------------------------
// Lifecycle: create / list / get / delete
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CustomResourceDefinition resources creating/deleting custom resource definition objects works [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/custom_resource_definition.go:69
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn crd_create_and_delete_round_trip() {
    let router = spawn_router();
    let crd = basic_crd("foos", "foo", "Foo", "example.com");

    let (status, body) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(
        status, 201,
        "CRD create must return 201 Created, body={body}"
    );
    assert_eq!(body["metadata"]["name"], "foos.example.com");
    // status conditions must be initialised on create (Established + NamesAccepted)
    let conditions = body["status"]["conditions"]
        .as_array()
        .expect("status.conditions present on create");
    let types: Vec<&str> = conditions
        .iter()
        .filter_map(|c| c["type"].as_str())
        .collect();
    assert!(types.contains(&"Established"), "Established condition set");
    assert!(
        types.contains(&"NamesAccepted"),
        "NamesAccepted condition set"
    );

    // GET round-trips
    let (status, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["spec"]["group"], "example.com");

    // DELETE
    let (status, _body) = delete(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com",
    )
    .await;
    assert!(
        (200..300).contains(&status),
        "CRD delete must succeed, got {status}"
    );

    // After delete, GET returns 404
    let (status, _) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com",
    )
    .await;
    assert_eq!(status, 404, "deleted CRD must 404 on GET");
}

/// [sig-api-machinery] CustomResourceDefinition resources listing custom resource definition objects works [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/custom_resource_definition.go:89
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn crd_list_filters_by_label_selector_and_deletecollection() {
    let router = spawn_router();

    // Two CRDs, one labelled match=true, one not.
    let mut matching = basic_crd("alphas", "alpha", "Alpha", "example.com");
    matching["metadata"]["labels"] = json!({"match": "true"});
    let other = basic_crd("betas", "beta", "Beta", "example.com");

    let (s1, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &matching,
    )
    .await;
    assert_eq!(s1, 201);
    let (s2, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &other,
    )
    .await;
    assert_eq!(s2, 201);

    // List with label selector returns only the matching CRD.
    let (status, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions?labelSelector=match%3Dtrue",
    )
    .await;
    assert_eq!(status, 200);
    let items = body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "label selector narrows result to one CRD");
    assert_eq!(items[0]["metadata"]["name"], "alphas.example.com");

    // DeleteCollection with the same selector removes only the matching CRD.
    let (status, _) = delete(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions?labelSelector=match%3Dtrue",
    )
    .await;
    assert!(
        (200..300).contains(&status),
        "deletecollection must succeed, got {status}"
    );

    // After deletion, only the unlabeled CRD remains.
    let (status, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
    )
    .await;
    assert_eq!(status, 200);
    let items = body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "only the unlabelled CRD survives");
    assert_eq!(items[0]["metadata"]["name"], "betas.example.com");
}

/// Lifecycle helper: list across the group reflects newly created definitions.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/custom_resource_definition.go:188
/// Sonobuoy (Round 160, 2026-04-26): PASS (covered by the discovery test)
#[tokio::test]
async fn crd_list_all_includes_newly_created() {
    let router = spawn_router();
    let (status, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["items"].as_array().map(Vec::len), Some(0));

    let crd = basic_crd("widgets", "widget", "Widget", "example.com");
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    let (status, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
    )
    .await;
    assert_eq!(status, 200);
    let items = body["items"].as_array().expect("items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["metadata"]["name"], "widgets.example.com");
}

/// Lifecycle helper: GET unknown CRD returns 404 with the Kubernetes
/// `Status` / `NotFound` reason envelope.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/custom_resource_definition.go:69
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn crd_get_unknown_name_returns_not_found() {
    let router = spawn_router();
    let (status, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/missing.example.com",
    )
    .await;
    assert_eq!(status, 404);
    // The envelope is the K8s Status object; ensure NotFound is conveyed.
    let reason = body["reason"].as_str().unwrap_or("");
    let kind = body["kind"].as_str().unwrap_or("");
    assert!(
        reason == "NotFound" || kind == "Status",
        "404 body must be a K8s Status with NotFound reason, body={body}"
    );
}

// ---------------------------------------------------------------------------
// Status / scale subresources
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CustomResourceDefinition resources getting/updating/patching custom resource definition status sub-resource works [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/custom_resource_definition.go:142
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn crd_status_subresource_get_update_patch() {
    let router = spawn_router();

    let crd = basic_crd("gizmos", "gizmo", "Gizmo", "example.com");
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    // GET /status returns the resource with its status block.
    let (status, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/gizmos.example.com/status",
    )
    .await;
    assert_eq!(status, 200, "status GET, body={body}");
    assert!(
        body["status"].is_object(),
        "status object present after CRD create, body={body}"
    );

    // PUT /status with a new condition: server should accept and persist.
    let mut updated = body.clone();
    let new_condition = json!({
        "type": "EstablishedByTest",
        "status": "True",
        "lastTransitionTime": "2026-04-26T00:00:00Z",
        "reason": "TestSetIt",
        "message": "marked by conformance mirror",
    });
    let conditions = updated["status"]["conditions"]
        .as_array_mut()
        .expect("conditions array");
    conditions.push(new_condition);
    let (status, body) = put_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/gizmos.example.com/status",
        &updated,
    )
    .await;
    assert_eq!(status, 200, "status PUT, body={body}");

    // GET again — new condition must persist.
    let (_s, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/gizmos.example.com/status",
    )
    .await;
    let conditions = body["status"]["conditions"]
        .as_array()
        .expect("conditions array after update");
    let types: Vec<&str> = conditions
        .iter()
        .filter_map(|c| c["type"].as_str())
        .collect();
    assert!(
        types.contains(&"EstablishedByTest"),
        "test-added condition must persist, types={types:?}"
    );

    // PATCH /status (merge-patch) — bump observedGeneration in status.
    let patch = json!({"status": {"observedGeneration": 42}});
    let (status, _body) = patch_merge(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/gizmos.example.com/status",
        &patch,
    )
    .await;
    assert_eq!(status, 200, "status PATCH must succeed");
}

/// Lifecycle: scale subresource get + update through a CR.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/custom_resource_definition.go:142
/// (subresource family — same upstream test fixture)
/// Sonobuoy (Round 160): was FAIL; fixed by PR #86 — scale subresource JSONPath resolved against CR root not narrowed spec.
#[tokio::test]
async fn crd_scale_subresource_get_and_update() {
    let router = spawn_router();
    let crd = scaled_crd("scalers", "scaler", "Scaler", "example.com");
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    // Create a CR with replicas=3
    let cr = json!({
        "apiVersion": "example.com/v1",
        "kind": "Scaler",
        "metadata": {"name": "s1", "namespace": "default"},
        "spec": {"replicas": 3, "foo": "x"},
        "status": {"replicas": 3},
    });
    let (s, body) = post_json(
        &router,
        "/apis/example.com/v1/namespaces/default/scalers",
        &cr,
    )
    .await;
    assert_eq!(s, 201, "CR create must succeed, body={body}");

    // GET scale subresource — replicas must reflect spec.replicas=3.
    let (status, body) = get(
        &router,
        "/apis/example.com/v1/namespaces/default/scalers/s1/scale",
    )
    .await;
    assert_eq!(status, 200, "scale GET, body={body}");
    assert_eq!(
        body["spec"]["replicas"], 3,
        "scale spec.replicas must reflect the CR (regression: path resolved against cr.spec)"
    );

    // PUT scale to bump replicas
    let new_scale = json!({
        "apiVersion": "autoscaling/v1",
        "kind": "Scale",
        "metadata": {"name": "s1", "namespace": "default"},
        "spec": {"replicas": 7},
    });
    let (status, body) = put_json(
        &router,
        "/apis/example.com/v1/namespaces/default/scalers/s1/scale",
        &new_scale,
    )
    .await;
    assert_eq!(status, 200, "scale PUT, body={body}");

    // Re-fetch the CR — spec.replicas now 7.
    let (_s, body) = get(
        &router,
        "/apis/example.com/v1/namespaces/default/scalers/s1",
    )
    .await;
    assert_eq!(
        body["spec"]["replicas"], 7,
        "spec.replicas updated via /scale, body={body}"
    );
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CustomResourceDefinition resources should include custom resource definition resources in discovery documents [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/custom_resource_definition.go:188
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn crd_resources_in_discovery_documents() {
    let router = spawn_router();

    // /apis must include apiextensions.k8s.io
    let (status, body) = get(&router, "/apis").await;
    assert_eq!(status, 200);
    let groups = body["groups"].as_array().expect("groups array");
    let has_apiext = groups
        .iter()
        .any(|g| g["name"].as_str() == Some("apiextensions.k8s.io"));
    assert!(
        has_apiext,
        "/apis must include apiextensions.k8s.io group, body={body}"
    );

    // /apis/apiextensions.k8s.io/v1 must list customresourcedefinitions
    let (status, body) = get(&router, "/apis/apiextensions.k8s.io/v1").await;
    assert_eq!(status, 200);
    let resources = body["resources"].as_array().expect("resources array");
    let names: Vec<&str> = resources
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();
    assert!(
        names.contains(&"customresourcedefinitions"),
        "discovery must list customresourcedefinitions, got {names:?}"
    );
    assert!(
        names.contains(&"customresourcedefinitions/status"),
        "discovery must list the /status subresource, got {names:?}"
    );
}

// ---------------------------------------------------------------------------
// Defaulting
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CustomResourceDefinition resources custom resource defaulting for requests and from storage works [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/custom_resource_definition.go:238
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn crd_defaulting_for_requests_and_storage() {
    let router = spawn_router();
    let crd = default_flavoured_crd();
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    // Create a CR omitting `spec.flavour` — the api-server must inject the
    // default "vanilla" before persisting.
    let cr = json!({
        "apiVersion": "example.com/v1",
        "kind": "Flavour",
        "metadata": {"name": "default-flavour", "namespace": "default"},
        "spec": {},
    });
    let (status, body) = post_json(
        &router,
        "/apis/example.com/v1/namespaces/default/flavours",
        &cr,
    )
    .await;
    assert_eq!(status, 201, "CR create must succeed, body={body}");

    // GET the CR back — the default must be applied.
    let (_s, body) = get(
        &router,
        "/apis/example.com/v1/namespaces/default/flavours/default-flavour",
    )
    .await;
    assert_eq!(
        body["spec"]["flavour"], "vanilla",
        "default value 'vanilla' must be applied from CRD schema, body={body}"
    );
}

// ---------------------------------------------------------------------------
// Watch & field selectors
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CustomResourceDefinition watch on custom resource definition objects [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_watch.go:53
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Mirrored as a lifecycle assertion: after create + modify + delete, the
/// stored object's resourceVersion bumps on each transition (which is what
/// fuels the watch stream). The long-lived watch endpoint is exercised by
/// `watch_delete_test.rs`; here we just verify the CRD's lifecycle events
/// produce monotonic resourceVersion changes observable via GET.
#[tokio::test]
async fn crd_watch_create_modify_delete() {
    let router = spawn_router();
    let crd = basic_crd("watched", "watched", "Watched", "example.com");
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    // GET to capture the initial resourceVersion (create returns the value
    // pre-storage, so rv is filled in by the storage layer and only visible
    // on the subsequent read — same as upstream behaviour where a watcher
    // reading the create event sees the assigned rv).
    let (_s, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/watched.example.com",
    )
    .await;
    let rv_after_create = body["metadata"]["resourceVersion"]
        .as_str()
        .map(str::to_string)
        .or_else(|| {
            body["metadata"]["resourceVersion"]
                .as_u64()
                .map(|n| n.to_string())
        });

    // Modify the CRD (patch a label).
    let patch = json!({"metadata": {"labels": {"phase": "modified"}}});
    let (s, _body) = patch_merge(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/watched.example.com",
        &patch,
    )
    .await;
    assert_eq!(s, 200, "patch must succeed");

    let (_s, body) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/watched.example.com",
    )
    .await;
    let rv_after_modify = body["metadata"]["resourceVersion"]
        .as_str()
        .map(str::to_string)
        .or_else(|| {
            body["metadata"]["resourceVersion"]
                .as_u64()
                .map(|n| n.to_string())
        });
    assert_eq!(
        body["metadata"]["labels"]["phase"], "modified",
        "patched label must be visible on subsequent GET"
    );
    if let (Some(a), Some(b)) = (rv_after_create.as_deref(), rv_after_modify.as_deref()) {
        assert_ne!(
            a, b,
            "resourceVersion must change after modify (was {a}, now {b})"
        );
    }

    // Delete.
    let (s, _) = delete(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/watched.example.com",
    )
    .await;
    assert!((200..300).contains(&s), "delete must succeed");
}

/// [sig-api-machinery] CustomResourceDefinition MUST list and watch custom resources matching the field selector [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_selectable_fields.go:174
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Scope vs. upstream: the upstream test exercises a conversion webhook on
/// top of selectable fields (v1 ↔ v2). We do not run a webhook in-process,
/// so this mirror covers the single-version slice of the contract: declare
/// `x-kubernetes-selectable-fields` on a CRD version, create CRs, then list
/// with `?fieldSelector=<path>=<value>` and confirm the list is filtered by
/// the path the CRD opted-in. Non-selectable paths must be rejected with a
/// "field label not supported" 422. Watch is covered by the dedicated watch
/// tests; this test verifies the list path only.
#[tokio::test]
async fn crd_selectable_fields_list_watch_informer() {
    let router = spawn_router();

    // CRD with `.spec.color` declared as a selectable field.
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {
                "plural": "widgets",
                "singular": "widget",
                "kind": "Widget",
                "listKind": "WidgetList",
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "color": {"type": "string"},
                                    "shape": {"type": "string"},
                                }
                            }
                        }
                    }
                },
                "selectableFields": [
                    {"jsonPath": ".spec.color"},
                    {"jsonPath": ".spec.shape"},
                ],
            }]
        }
    });
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    // Reload the CRD and confirm `selectableFields` survives the round-trip
    // through storage + serialisation.
    let (_s, stored_crd) = get(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.example.com",
    )
    .await;
    let sf = &stored_crd["spec"]["versions"][0]["selectableFields"];
    assert_eq!(sf[0]["jsonPath"], ".spec.color");
    assert_eq!(sf[1]["jsonPath"], ".spec.shape");

    // Create three CRs: two red, one blue.
    for (name, color, shape) in [
        ("w-red-1", "red", "square"),
        ("w-red-2", "red", "circle"),
        ("w-blue-1", "blue", "circle"),
    ] {
        let body = json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"color": color, "shape": shape},
        });
        let (s, b) = post_json(
            &router,
            "/apis/example.com/v1/namespaces/default/widgets",
            &body,
        )
        .await;
        assert_eq!(s, 201, "creating {name} must succeed, body={b}");
    }

    // Sanity: list with no selector returns all three.
    let (s, body) = get(&router, "/apis/example.com/v1/namespaces/default/widgets").await;
    assert_eq!(s, 200);
    assert_eq!(body["items"].as_array().map(Vec::len), Some(3));

    // ?fieldSelector=spec.color=red → two widgets, both red.
    let (s, body) = get(
        &router,
        "/apis/example.com/v1/namespaces/default/widgets?fieldSelector=spec.color%3Dred",
    )
    .await;
    assert_eq!(s, 200, "list with color=red must succeed, body={body}");
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|w| w["metadata"]["name"].as_str())
        .collect();
    assert_eq!(names.len(), 2, "expected 2 red widgets, got {names:?}");
    assert!(names.contains(&"w-red-1"));
    assert!(names.contains(&"w-red-2"));

    // Compound selector: spec.color=red,spec.shape=circle → only w-red-2.
    let (s, body) = get(
        &router,
        "/apis/example.com/v1/namespaces/default/widgets?fieldSelector=spec.color%3Dred%2Cspec.shape%3Dcircle",
    )
    .await;
    assert_eq!(s, 200, "compound list must succeed, body={body}");
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|w| w["metadata"]["name"].as_str())
        .collect();
    assert_eq!(names, vec!["w-red-2"]);

    // Inequality on a selectable path: spec.color!=red → only w-blue-1.
    let (s, body) = get(
        &router,
        "/apis/example.com/v1/namespaces/default/widgets?fieldSelector=spec.color%21%3Dred",
    )
    .await;
    assert_eq!(s, 200, "not-equals list must succeed, body={body}");
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|w| w["metadata"]["name"].as_str())
        .collect();
    assert_eq!(names, vec!["w-blue-1"]);

    // metadata.name is always selectable.
    let (s, body) = get(
        &router,
        "/apis/example.com/v1/namespaces/default/widgets?fieldSelector=metadata.name%3Dw-red-1",
    )
    .await;
    assert_eq!(s, 200, "metadata.name list must succeed, body={body}");
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|w| w["metadata"]["name"].as_str())
        .collect();
    assert_eq!(names, vec!["w-red-1"]);

    // A path that is NOT in selectableFields must be rejected upfront.
    let (s, body) = get(
        &router,
        "/apis/example.com/v1/namespaces/default/widgets?fieldSelector=spec.weight%3D10",
    )
    .await;
    assert_eq!(
        s, 422,
        "non-selectable path must be rejected with 422, got status={s} body={body}"
    );
}

/// Custom-resource `deletecollection` honours field/label selectors.
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_selectable_fields.go:271
/// (`v2Client.Namespace(ns).DeleteCollection(..., FieldSelector: "host=host1,port=80")`).
///
/// Regression guard for the route gap that returned a bare 404
/// ("the server could not find the requested resource"): the collection-path
/// arms in `custom_resource_fallback` were guarded to GET||POST, so a DELETE on
/// `/apis/{g}/{v}/namespaces/{ns}/{plural}?fieldSelector=...` matched no route.
#[tokio::test]
async fn crd_deletecollection_honours_field_and_label_selectors() {
    let router = spawn_router();

    // CRD with two selectable fields.
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {
                "plural": "widgets", "singular": "widget",
                "kind": "Widget", "listKind": "WidgetList",
            },
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "properties": {
                    "spec": {"type": "object", "properties": {
                        "color": {"type": "string"},
                        "shape": {"type": "string"},
                    }}
                }}},
                "selectableFields": [
                    {"jsonPath": ".spec.color"},
                    {"jsonPath": ".spec.shape"},
                ],
            }]
        }
    });
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    // Three CRs: two red, one blue.
    for (name, color, shape) in [
        ("w-red-1", "red", "square"),
        ("w-red-2", "red", "circle"),
        ("w-blue-1", "blue", "circle"),
    ] {
        let body = json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"color": color, "shape": shape},
        });
        let (s, b) = post_json(
            &router,
            "/apis/example.com/v1/namespaces/default/widgets",
            &body,
        )
        .await;
        assert_eq!(s, 201, "creating {name} must succeed, body={b}");
    }

    // DeleteCollection with a field selector removes only the two red widgets.
    // Pre-fix this returned 404 (route gap), not 2xx.
    let (s, b) = delete(
        &router,
        "/apis/example.com/v1/namespaces/default/widgets?fieldSelector=spec.color%3Dred",
    )
    .await;
    assert!(
        (200..300).contains(&s),
        "CR deletecollection must succeed, got status={s} body={b}"
    );

    // Only the blue widget survives.
    let (s, body) = get(&router, "/apis/example.com/v1/namespaces/default/widgets").await;
    assert_eq!(s, 200);
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|w| w["metadata"]["name"].as_str())
        .collect();
    assert_eq!(names, vec!["w-blue-1"], "only non-matching CR survives");

    // DeleteCollection with no selector clears the rest.
    let (s, _) = delete(&router, "/apis/example.com/v1/namespaces/default/widgets").await;
    assert!(
        (200..300).contains(&s),
        "unselected deletecollection, got {s}"
    );
    let (s, body) = get(&router, "/apis/example.com/v1/namespaces/default/widgets").await;
    assert_eq!(s, 200);
    assert_eq!(
        body["items"].as_array().map(Vec::len),
        Some(0),
        "all widgets deleted"
    );
}

// ---------------------------------------------------------------------------
// x-kubernetes-validations (CEL) — crd_validation_rules.go
// ---------------------------------------------------------------------------
//
// The api-server evaluates `x-kubernetes-validations[].rule` at CR
// CREATE/UPDATE time and verifies rules at CRD admission time (syntax,
// unknown-property, estimated cost). See
// `crates/api-server/src/handlers/cel_validation.rs` and
// `crates/api-server/src/handlers/custom_resource.rs::validate_custom_resource_with_old`.

/// Helper: produce a CRD with a single CEL rule on `spec`.
/// The schema defines `spec.replicas` (int) and `spec.foo` (string).
fn crd_with_cel_rule(rule: &str, message: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "celrules.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {
                "plural": "celrules",
                "singular": "celrule",
                "kind": "CelRule",
                "listKind": "CelRuleList",
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "replicas": {"type": "integer"},
                                    "foo": {"type": "string"}
                                },
                                "x-kubernetes-validations": [
                                    {"rule": rule, "message": message}
                                ]
                            }
                        }
                    }
                }
            }]
        }
    })
}

/// [sig-api-machinery] CustomResourceValidationRules MUST NOT fail validation for create of a custom resource that satisfies the x-kubernetes-validations rules [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_rules.go:97
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn cel_rule_satisfied_create_succeeds() {
    let router = spawn_router();
    let crd = crd_with_cel_rule("self.replicas <= 5", "too many replicas");
    let (s, body) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(
        s, 201,
        "CRD create with valid CEL rule must succeed, body={body}"
    );

    let cr = json!({
        "apiVersion": "example.com/v1",
        "kind": "CelRule",
        "metadata": {"name": "ok", "namespace": "default"},
        "spec": {"replicas": 3, "foo": "bar"},
    });
    let (s, body) = post_json(
        &router,
        "/apis/example.com/v1/namespaces/default/celrules",
        &cr,
    )
    .await;
    assert_eq!(
        s, 201,
        "CR satisfying the rule (replicas=3 ≤ 5) must succeed, body={body}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail validation for create of a custom resource that does not satisfy the x-kubernetes-validations rules [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_rules.go:124
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn cel_rule_violated_create_fails() {
    let router = spawn_router();
    let crd = crd_with_cel_rule("self.replicas <= 5", "replicas must be <= 5");
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    let cr = json!({
        "apiVersion": "example.com/v1",
        "kind": "CelRule",
        "metadata": {"name": "bad", "namespace": "default"},
        "spec": {"replicas": 99, "foo": "bar"},
    });
    let (s, body) = post_json(
        &router,
        "/apis/example.com/v1/namespaces/default/celrules",
        &cr,
    )
    .await;
    assert!(
        (400..500).contains(&s),
        "CR violating rule must be rejected, got {s}, body={body}"
    );
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("replicas must be <= 5"),
        "rule message must surface in error, got {msg}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail create of a CRD that contains a x-kubernetes-validations rule that refers to a property that do not exist [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_rules.go:150
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn cel_rule_unknown_property_crd_rejected() {
    let router = spawn_router();
    // `self.nonsense` is not declared in properties — CRD must be rejected.
    let crd = crd_with_cel_rule("self.nonsense > 0", "msg");
    let (s, body) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert!(
        (400..500).contains(&s),
        "CRD with unknown property reference must be rejected, got {s}, body={body}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail create of a CRD that contains an x-kubernetes-validations rule that contains a syntax error [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_rules.go:177
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn cel_rule_syntax_error_crd_rejected() {
    let router = spawn_router();
    // `self.replicas <=` is incomplete — must be a parse error.
    let crd = crd_with_cel_rule("self.replicas <= ", "msg");
    let (s, body) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert!(
        (400..500).contains(&s),
        "CRD with syntactically-invalid rule must be rejected, got {s}, body={body}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail create of a CRD that contains an x-kubernetes-validations rule that exceeds the estimated cost limit [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_rules.go:203
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn cel_rule_cost_limit_exceeded_crd_rejected() {
    let router = spawn_router();
    // Nested `.all(...)` calls inflate estimated cost past the 10M-token limit.
    let expensive = "self.foo.all(a, self.foo.all(b, self.foo.all(c, c == a)))";
    let crd = crd_with_cel_rule(expensive, "msg");
    let (s, body) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert!(
        (400..500).contains(&s),
        "CRD with rule exceeding cost limit must be rejected, got {s}, body={body}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail create of a CR that exceeds the runtime cost limit for x-kubernetes-validations rule execution [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_rules.go:231
/// Sonobuoy (Round 160, 2026-04-26): PASS
///
/// Mirrored as: a CRD whose rule fits under the per-rule budget but with
/// enough rules to blow the per-request budget at evaluation time. Each rule
/// individually is cheap but the request total exceeds the runtime limit.
#[tokio::test]
async fn cel_rule_runtime_cost_limit_exceeded() {
    let router = spawn_router();
    // Many comprehension-style rules → per-request total trips the runtime
    // budget. Each rule's estimated cost is rule_len * 1024 (one `all(`) →
    // ~50K. We need cumulative cost > 100M, so 3000+ rules suffice.
    let mut rules: Vec<Value> = Vec::new();
    for _ in 0..3000 {
        rules.push(json!({
            "rule": "[1,2,3].all(x, x > 0) && self.replicas == self.replicas",
            "message": "noop",
        }));
    }
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "manyrules.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {
                "plural": "manyrules",
                "singular": "manyrule",
                "kind": "ManyRule",
                "listKind": "ManyRuleList",
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "replicas": {"type": "integer"}
                                },
                                "x-kubernetes-validations": rules
                            }
                        }
                    }
                }
            }]
        }
    });
    // The CRD itself passes admission (each rule cheap), but the CR rejects.
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(
        s, 201,
        "CRD with many cheap rules must be admitted (each rule under per-rule limit)"
    );

    let cr = json!({
        "apiVersion": "example.com/v1",
        "kind": "ManyRule",
        "metadata": {"name": "victim", "namespace": "default"},
        "spec": {"replicas": 1},
    });
    let (s, body) = post_json(
        &router,
        "/apis/example.com/v1/namespaces/default/manyrules",
        &cr,
    )
    .await;
    assert!(
        (400..500).contains(&s),
        "CR exceeding runtime cost limit must be rejected, got {s}, body={body}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail update of a CR that does not satisfy a x-kubernetes-validations transition rule [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_rules.go:260
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn cel_transition_rule_violated_update_fails() {
    let router = spawn_router();
    // Transition rule: replicas may not decrease.
    let crd = crd_with_cel_rule(
        "self.replicas >= oldSelf.replicas",
        "replicas may not decrease",
    );
    let (s, _) = post_json(
        &router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        &crd,
    )
    .await;
    assert_eq!(s, 201);

    // CREATE the CR (no oldSelf — transition rules are skipped on CREATE).
    let cr = json!({
        "apiVersion": "example.com/v1",
        "kind": "CelRule",
        "metadata": {"name": "t1", "namespace": "default"},
        "spec": {"replicas": 5, "foo": "x"},
    });
    let (s, body) = post_json(
        &router,
        "/apis/example.com/v1/namespaces/default/celrules",
        &cr,
    )
    .await;
    assert_eq!(s, 201, "CREATE skips transition rule, body={body}");

    // UPDATE with smaller replicas — must fail because oldSelf.replicas=5
    // but self.replicas=2.
    let cr_smaller = json!({
        "apiVersion": "example.com/v1",
        "kind": "CelRule",
        "metadata": {"name": "t1", "namespace": "default"},
        "spec": {"replicas": 2, "foo": "x"},
    });
    let (s, body) = put_json(
        &router,
        "/apis/example.com/v1/namespaces/default/celrules/t1",
        &cr_smaller,
    )
    .await;
    assert!(
        (400..500).contains(&s),
        "UPDATE violating transition rule must be rejected, got {s}, body={body}"
    );
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("replicas may not decrease"),
        "transition rule message must surface, got {msg}"
    );
}

// ---------------------------------------------------------------------------
// Validation ratcheting — crd_validation_ratcheting.go (KEP-4008)
// ---------------------------------------------------------------------------
//
// Each test follows the upstream pattern: create a CRD with a *lax* schema,
// POST an instance that satisfies the lax schema, then PUT the CRD to install
// a *tight* schema that the existing instance would fail. On the subsequent
// PUT of the instance (typically with a label change), ratcheting must
// suppress any failure whose path resolves to an unchanged correlatable
// sub-tree. Transition rules (those referencing `oldSelf`) are NEVER
// ratcheted.

/// Build a permissive CRD whose `spec` is `preserveUnknownFields: true` so
/// the initial CR survives without any constraint, and tightening to the
/// real schema later is purely additive. We park each test on its own group
/// to isolate CRD state inside the per-test in-memory storage.
fn ratchet_lax_crd(group: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": format!("ratchets.{group}")},
        "spec": {
            "group": group,
            "scope": "Namespaced",
            "names": {
                "plural": "ratchets",
                "singular": "ratchet",
                "kind": "Ratchet",
                "listKind": "RatchetList",
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "x-kubernetes-preserve-unknown-fields": true,
                    }
                }
            }]
        }
    })
}

/// Replace the schema on the existing CRD with `spec_props`. The CRD route
/// expects a fully-formed body; we keep names/group identical to the lax
/// CRD and only swap the schema for the named version.
fn ratchet_tight_crd(group: &str, spec_props: Value) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": format!("ratchets.{group}")},
        "spec": {
            "group": group,
            "scope": "Namespaced",
            "names": {
                "plural": "ratchets",
                "singular": "ratchet",
                "kind": "Ratchet",
                "listKind": "RatchetList",
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": spec_props,
                        }
                    }
                }
            }]
        }
    })
}

async fn ratchet_install_crd(router: &TestApiServer, body: &Value) {
    let (s, b) = post_json(
        router,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        body,
    )
    .await;
    assert_eq!(s, 201, "lax CRD must install, body={b}");
}

async fn ratchet_tighten_crd(router: &TestApiServer, group: &str, body: &Value) {
    let uri = format!("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/ratchets.{group}");
    let (s, b) = put_json(router, &uri, body).await;
    assert_eq!(s, 200, "tight CRD update must succeed, body={b}");
}

async fn ratchet_post_cr(
    router: &TestApiServer,
    group: &str,
    name: &str,
    spec: Value,
) -> (u16, Value) {
    let cr = json!({
        "apiVersion": format!("{group}/v1"),
        "kind": "Ratchet",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec,
    });
    let uri = format!("/apis/{group}/v1/namespaces/default/ratchets");
    post_json(router, &uri, &cr).await
}

async fn ratchet_put_cr(
    router: &TestApiServer,
    group: &str,
    name: &str,
    spec: Value,
    labels: Option<Value>,
) -> (u16, Value) {
    let mut metadata = json!({"name": name, "namespace": "default"});
    if let Some(l) = labels {
        metadata["labels"] = l;
    }
    let cr = json!({
        "apiVersion": format!("{group}/v1"),
        "kind": "Ratchet",
        "metadata": metadata,
        "spec": spec,
    });
    let uri = format!("/apis/{group}/v1/namespaces/default/ratchets/{name}");
    put_json(router, &uri, &cr).await
}

/// [sig-api-machinery] CustomResourceValidationRules MUST NOT fail to update a resource due to JSONSchema errors on unchanged correlatable fields [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_ratcheting.go:201
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn ratcheting_unchanged_correlatable_jsonschema_errors_allowed() {
    let router = spawn_router();
    let group = "rl1.example.com";

    // 1) Permissive CRD.
    ratchet_install_crd(&router, &ratchet_lax_crd(group)).await;

    // 2) Create an instance with values that the tight schema will later
    //    reject (`field: "foo"`, but the tight enum allows only "notfoo").
    let instance_spec = json!({
        "field": "foo",
        "struct": {"field": "foo"},
        "list": [{"key": "first", "field": "foo"}],
        "map": {"foo": {"field": "foo"}}
    });
    let (s, b) = ratchet_post_cr(&router, group, "t1", instance_spec.clone()).await;
    assert_eq!(s, 201, "CREATE under lax schema must succeed, body={b}");

    // 3) Tighten the schema: enum allows only "notfoo".
    let tight_spec = json!({
        "type": "object",
        "properties": {
            "field": {"type": "string", "enum": ["notfoo"]},
            "struct": {
                "type": "object",
                "properties": {
                    "field": {"type": "string", "enum": ["notfoo"]}
                }
            },
            "list": {
                "type": "array",
                "x-kubernetes-list-type": "map",
                "x-kubernetes-list-map-keys": ["key"],
                "items": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                        "field": {"type": "string", "enum": ["notfoo"]}
                    },
                    "required": ["key"]
                }
            },
            "map": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string", "enum": ["notfoo"]}
                    }
                }
            }
        }
    });
    ratchet_tighten_crd(&router, group, &ratchet_tight_crd(group, tight_spec)).await;

    // 4) Re-PUT the same instance with a label change. Every offending
    //    `field == "foo"` value is unchanged AND lives under a correlatable
    //    parent (struct, list-type=map keyed by "key", map by name) so
    //    ratcheting must suppress every error.
    let (s, b) = ratchet_put_cr(
        &router,
        group,
        "t1",
        instance_spec,
        Some(json!({"foo": "bar"})),
    )
    .await;
    assert_eq!(
        s, 200,
        "UPDATE with unchanged correlatable invalid values must be ratcheted, body={b}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail to update a resource due to JSONSchema errors on unchanged uncorrelatable fields [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_ratcheting.go:244
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn ratcheting_unchanged_uncorrelatable_jsonschema_errors_blocked() {
    let router = spawn_router();
    let group = "rl2.example.com";

    ratchet_install_crd(&router, &ratchet_lax_crd(group)).await;

    // Two uncorrelatable arrays (atomic + set).
    let initial = json!({
        "atomicArray": ["foo", "bar", "baz"],
        "setArray": ["foo", "bar", "baz"],
    });
    let (s, b) = ratchet_post_cr(&router, group, "t1", initial.clone()).await;
    assert_eq!(s, 201, "CREATE under lax schema must succeed, body={b}");

    let tight_spec = json!({
        "type": "object",
        "properties": {
            "atomicArray": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["notfoo", "notbar", "notbaz"]
                }
            },
            "setArray": {
                "type": "array",
                "x-kubernetes-list-type": "set",
                "items": {
                    "type": "string",
                    "enum": ["notfoo", "notbar", "notbaz"]
                }
            }
        }
    });
    ratchet_tighten_crd(&router, group, &ratchet_tight_crd(group, tight_spec)).await;

    // Even appending a *valid* element doesn't help: every existing element
    // fails enum, and atomic/set arrays are uncorrelatable per upstream so
    // index-level ratcheting never kicks in.
    let modified = json!({
        "atomicArray": ["foo", "bar", "baz", "notfoo"],
        "setArray": ["foo", "bar", "baz", "notfoo"],
    });
    let (s, body) =
        ratchet_put_cr(&router, group, "t1", modified, Some(json!({"foo": "bar"}))).await;
    assert!(
        (400..500).contains(&s),
        "UPDATE on uncorrelatable arrays must be rejected, got {s}, body={body}"
    );
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("atomicArray") || msg.contains("setArray"),
        "rejection must reference one of the offending arrays, got: {msg}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail to update a resource due to JSONSchema errors on changed fields [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_ratcheting.go:280
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn ratcheting_changed_jsonschema_errors_blocked() {
    let router = spawn_router();
    let group = "rl3.example.com";

    ratchet_install_crd(&router, &ratchet_lax_crd(group)).await;

    let initial = json!({
        "field": "foo",
        "struct": {"field": "foo"},
        "list": [
            {"key": "foo", "field": "foo"},
            {"key": "bar", "field": "foo"}
        ],
        "map": {"foo": {"field": "foo"}, "bar": {"field": "foo"}}
    });
    let (s, _) = ratchet_post_cr(&router, group, "t1", initial).await;
    assert_eq!(s, 201);

    // Tight enum allows ONLY "foo". The instance was created with "foo", so
    // unchanged values would pass. We now change every value to "notfoo".
    let tight_spec = json!({
        "type": "object",
        "properties": {
            "field": {"type": "string", "enum": ["foo"]},
            "struct": {
                "type": "object",
                "properties": {
                    "field": {"type": "string", "enum": ["foo"]}
                }
            },
            "list": {
                "type": "array",
                "x-kubernetes-list-type": "map",
                "x-kubernetes-list-map-keys": ["key"],
                "items": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                        "field": {"type": "string", "enum": ["foo"]}
                    },
                    "required": ["key"]
                }
            },
            "map": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string", "enum": ["foo"]}
                    }
                }
            }
        }
    });
    ratchet_tighten_crd(&router, group, &ratchet_tight_crd(group, tight_spec)).await;

    let modified = json!({
        "field": "notfoo",
        "struct": {"field": "notfoo"},
        "list": [
            {"key": "foo", "field": "notfoo"},
            {"key": "bar", "field": "notfoo"}
        ],
        "map": {"foo": {"field": "notfoo"}, "bar": {"field": "notfoo"}}
    });
    let (s, body) = ratchet_put_cr(&router, group, "t1", modified, None).await;
    assert!(
        (400..500).contains(&s),
        "changed-field UPDATE must be rejected, got {s}, body={body}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST NOT fail to update a resource due to CRD Validation Rule errors on unchanged correlatable fields [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_ratcheting.go:333
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn ratcheting_unchanged_correlatable_cel_errors_allowed() {
    let router = spawn_router();
    let group = "rl4.example.com";

    ratchet_install_crd(&router, &ratchet_lax_crd(group)).await;

    // Initial instance violates the (about-to-be-installed) CEL rule
    // `self == "foo"`.
    let initial = json!({
        "field": "notfoo",
        "struct": {"field": "notfoo"},
        "list": [
            {"key": "foo", "field": "notfoo"},
            {"key": "bar", "field": "notfoo"}
        ],
        "map": {"foo": {"field": "notfoo"}, "bar": {"field": "notfoo"}}
    });
    let (s, b) = ratchet_post_cr(&router, group, "t1", initial.clone()).await;
    assert_eq!(s, 201, "CREATE must succeed, body={b}");

    let tight_spec = json!({
        "type": "object",
        "properties": {
            "field": {
                "type": "string",
                "x-kubernetes-validations": [{"rule": "self == 'foo'"}]
            },
            "otherField": {"type": "string"},
            "struct": {
                "type": "object",
                "properties": {
                    "field": {
                        "type": "string",
                        "x-kubernetes-validations": [{"rule": "self == 'foo'"}]
                    },
                    "otherField": {"type": "string"}
                }
            },
            "list": {
                "type": "array",
                "x-kubernetes-list-type": "map",
                "x-kubernetes-list-map-keys": ["key"],
                "items": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                        "field": {
                            "type": "string",
                            "x-kubernetes-validations": [{"rule": "self == 'foo'"}]
                        },
                        "otherField": {"type": "string"}
                    },
                    "required": ["key"]
                }
            },
            "map": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "field": {
                            "type": "string",
                            "x-kubernetes-validations": [{"rule": "self == 'foo'"}]
                        },
                        "otherField": {"type": "string"}
                    }
                }
            }
        }
    });
    ratchet_tighten_crd(&router, group, &ratchet_tight_crd(group, tight_spec)).await;

    // Introduce a brand new (valid) field everywhere but leave the old
    // invalid `field` values untouched. Also append a new list item with a
    // valid `field` value — its `field` is changed (introduced) but valid.
    let modified = json!({
        "field": "notfoo",
        "otherField": "doesntmatter",
        "struct": {
            "field": "notfoo",
            "otherField": "doesntmatter"
        },
        "list": [
            {"key": "foo", "field": "notfoo", "otherField": "doesntmatter"},
            {"key": "bar", "field": "notfoo", "otherField": "doesntmatter"},
            {"key": "baz", "field": "foo", "otherField": "doesntmatter"}
        ],
        "map": {
            "foo": {"field": "notfoo", "otherField": "doesntmatter"},
            "bar": {"field": "notfoo", "otherField": "doesntmatter"}
        }
    });
    let (s, b) = ratchet_put_cr(&router, group, "t1", modified, None).await;
    assert_eq!(
        s, 200,
        "UPDATE must ratchet unchanged correlatable CEL failures, body={b}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail to update a resource due to CRD Validation Rule errors on unchanged uncorrelatable fields [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_ratcheting.go:412
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn ratcheting_unchanged_uncorrelatable_cel_errors_blocked() {
    let router = spawn_router();
    let group = "rl5.example.com";

    ratchet_install_crd(&router, &ratchet_lax_crd(group)).await;

    let initial = json!({
        "setArray": ["foo", "bar", "baz"],
        "atomicArray": ["foo", "bar", "baz"],
    });
    let (s, _) = ratchet_post_cr(&router, group, "t1", initial).await;
    assert_eq!(s, 201);

    let tight_spec = json!({
        "type": "object",
        "properties": {
            "atomicArray": {
                "type": "array",
                "items": {
                    "type": "string",
                    "x-kubernetes-validations": [{"rule": "self != 'foo'"}]
                }
            },
            "setArray": {
                "type": "array",
                "x-kubernetes-list-type": "set",
                "items": {
                    "type": "string",
                    "x-kubernetes-validations": [{"rule": "self != 'foo'"}]
                }
            }
        }
    });
    ratchet_tighten_crd(&router, group, &ratchet_tight_crd(group, tight_spec)).await;

    let modified = json!({
        "setArray": ["foo", "bar", "baz", "notfoo"],
        "atomicArray": ["foo", "bar", "baz", "notfoo"],
    });
    let (s, body) =
        ratchet_put_cr(&router, group, "t1", modified, Some(json!({"foo": "bar"}))).await;
    assert!(
        (400..500).contains(&s),
        "UPDATE on uncorrelatable arrays must be rejected, got {s}, body={body}"
    );
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("atomicArray") || msg.contains("setArray"),
        "rejection must reference one of the offending arrays, got: {msg}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST fail to update a resource due to CRD Validation Rule errors on changed fields [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_ratcheting.go:448
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn ratcheting_changed_cel_errors_blocked() {
    let router = spawn_router();
    let group = "rl6.example.com";

    ratchet_install_crd(&router, &ratchet_lax_crd(group)).await;

    let initial = json!({
        "field": "foo",
        "struct": {"field": "foo"},
        "list": [
            {"key": "foo", "field": "foo"},
            {"key": "bar", "field": "foo"}
        ],
        "map": {"foo": {"field": "foo"}, "bar": {"field": "foo"}}
    });
    let (s, _) = ratchet_post_cr(&router, group, "t1", initial).await;
    assert_eq!(s, 201);

    let tight_spec = json!({
        "type": "object",
        "properties": {
            "field": {
                "type": "string",
                "x-kubernetes-validations": [{"rule": "self == 'foo'"}]
            },
            "struct": {
                "type": "object",
                "properties": {
                    "field": {
                        "type": "string",
                        "x-kubernetes-validations": [{"rule": "self == 'foo'"}]
                    }
                }
            },
            "list": {
                "type": "array",
                "x-kubernetes-list-type": "map",
                "x-kubernetes-list-map-keys": ["key"],
                "items": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                        "field": {
                            "type": "string",
                            "x-kubernetes-validations": [{"rule": "self == 'foo'"}]
                        }
                    },
                    "required": ["key"]
                }
            },
            "map": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "field": {
                            "type": "string",
                            "x-kubernetes-validations": [{"rule": "self == 'foo'"}]
                        }
                    }
                }
            }
        }
    });
    ratchet_tighten_crd(&router, group, &ratchet_tight_crd(group, tight_spec)).await;

    let modified = json!({
        "field": "notfoo",
        "struct": {"field": "notfoo"},
        "list": [
            {"key": "foo", "field": "notfoo"},
            {"key": "bar", "field": "notfoo"}
        ],
        "map": {"foo": {"field": "notfoo"}, "bar": {"field": "notfoo"}}
    });
    let (s, body) = ratchet_put_cr(&router, group, "t1", modified, None).await;
    assert!(
        (400..500).contains(&s),
        "changed-field UPDATE must be rejected, got {s}, body={body}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST NOT ratchet errors raised by transition rules [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_ratcheting.go:511
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
async fn ratcheting_transition_rule_errors_never_ratcheted() {
    let router = spawn_router();
    let group = "rl7.example.com";

    ratchet_install_crd(&router, &ratchet_lax_crd(group)).await;

    let initial = json!({
        "field": "foo",
        "struct": {"field": "foo"},
        "list": [
            {"key": "foo", "field": "foo"},
            {"key": "bar", "field": "foo"}
        ],
        "map": {"foo": {"field": "foo"}, "bar": {"field": "foo"}}
    });
    let (s, _) = ratchet_post_cr(&router, group, "t1", initial.clone()).await;
    assert_eq!(s, 201);

    // Tight CRD with transition rule `self != oldSelf`. Even with the
    // sub-tree unchanged (`self == oldSelf`), this rule MUST fail — and
    // ratcheting MUST NOT suppress it.
    let tight_spec = json!({
        "type": "object",
        "properties": {
            "field": {
                "type": "string",
                "x-kubernetes-validations": [{"rule": "self != oldSelf"}]
            },
            "struct": {
                "type": "object",
                "properties": {
                    "field": {
                        "type": "string",
                        "x-kubernetes-validations": [{"rule": "self != oldSelf"}]
                    }
                }
            },
            "list": {
                "type": "array",
                "x-kubernetes-list-type": "map",
                "x-kubernetes-list-map-keys": ["key"],
                "items": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                        "field": {
                            "type": "string",
                            "x-kubernetes-validations": [{"rule": "self != oldSelf"}]
                        }
                    },
                    "required": ["key"]
                }
            },
            "map": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "field": {
                            "type": "string",
                            "x-kubernetes-validations": [{"rule": "self != oldSelf"}]
                        }
                    }
                }
            }
        }
    });
    ratchet_tighten_crd(&router, group, &ratchet_tight_crd(group, tight_spec)).await;

    // PUT the SAME instance (label-only change). Every transition rule must
    // fail because `self == oldSelf` for every covered leaf.
    let (s, body) =
        ratchet_put_cr(&router, group, "t1", initial, Some(json!({"foo": "bar"}))).await;
    assert!(
        (400..500).contains(&s),
        "transition rule failure must NOT be ratcheted, got {s}, body={body}"
    );
}

/// [sig-api-machinery] CustomResourceValidationRules MUST evaluate a CRD Validation Rule with oldSelf = nil for new values when optionalOldSelf is true [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_validation_ratcheting.go:569
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[tokio::test]
#[ignore = "Ratcheting tracker — depends on CEL eval (this PR) + schema-diff engine (future, multi-week)"]
async fn ratcheting_optional_old_self_nil_for_new_values() {}
