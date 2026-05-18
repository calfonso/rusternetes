//! Scoped mirror of Kubernetes v1.35 conformance for [sig-api-machinery]
//! CRD OpenAPI publishing + conversion webhooks.
//!
//! Source: https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//! Status table: docs/conformance/apimachinery-crd-openapi.md
//!
//! Each test below mirrors a single upstream Ginkgo descriptor from
//! `test/e2e/apimachinery/crd_publish_openapi.go` (10 cases) or
//! `test/e2e/apimachinery/crd_conversion_webhook.go` (2 cases) and a few
//! structural-schema rules from the OpenAPI publish pipeline. Tests that
//! mirror a currently-FAIL Sonobuoy outcome are `#[ignore]`d with a tracker
//! note pointing at the doc fragment. Passing mirrors must pass locally.
//!
//! Harness: spawn the real Axum router on top of `StorageBackend::Memory`
//! and drive it through `tower::ServiceExt::oneshot` — the same path the
//! production api-server takes when serving `/openapi/v2` and
//! `/openapi/v3/apis/<group>/<version>`. This is the canonical surface the
//! upstream conformance suite exercises; mocking the handler functions
//! directly would mask the very routing/publish bugs Sonobuoy catches.

use axum::{body::Body, http::Request};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::StorageBackend;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// HTTP harness (inline per-file per batch convention; see plan §"HTTP harness")
// ---------------------------------------------------------------------------

/// Build an `ApiServerState` backed by `StorageBackend::Memory` with
/// `skip_auth = true`. Mirrors `router_smoke_test::make_test_state`.
fn spawn_state() -> Arc<ApiServerState> {
    let storage = Arc::new(StorageBackend::new_memory());
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    Arc::new(ApiServerState::new(
        storage,
        token_manager,
        authorizer,
        metrics,
        true,
    ))
}

/// POST the given CRD JSON to `/apis/apiextensions.k8s.io/v1/customresourcedefinitions`.
/// Returns `(status, body)` so tests can assert the publish round-trip.
async fn post_crd(state: Arc<ApiServerState>, crd_body: &Value) -> (u16, Value) {
    let router = build_router(state, None);
    let req = Request::builder()
        .method("POST")
        .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(crd_body).unwrap()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// PUT the given CRD JSON to update an existing CRD.
async fn put_crd(state: Arc<ApiServerState>, name: &str, crd_body: &Value) -> (u16, Value) {
    let router = build_router(state, None);
    let uri = format!(
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{}",
        name
    );
    let req = Request::builder()
        .method("PUT")
        .uri(&uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(crd_body).unwrap()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// DELETE an existing CRD by name.
async fn delete_crd(state: Arc<ApiServerState>, name: &str) -> u16 {
    let router = build_router(state, None);
    let uri = format!(
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{}",
        name
    );
    let req = Request::builder()
        .method("DELETE")
        .uri(&uri)
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    resp.status().as_u16()
}

/// GET the published v2 swagger spec. Returns the parsed JSON body.
async fn get_openapi_v2(state: Arc<ApiServerState>) -> Value {
    let router = build_router(state, None);
    let req = Request::builder()
        .method("GET")
        .uri("/openapi/v2")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "/openapi/v2 must serve 200 (upstream tests poll until valid)"
    );
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).expect("/openapi/v2 must be JSON")
}

/// GET `/openapi/v3/apis/<group>/<version>`. Returns the parsed JSON body.
async fn get_openapi_v3_for_group(state: Arc<ApiServerState>, group: &str, version: &str) -> Value {
    let router = build_router(state, None);
    let uri = format!("/openapi/v3/apis/{}/{}", group, version);
    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200, "{} must serve 200", uri);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).expect("openapi v3 body must be JSON")
}

/// GET `/openapi/v3` (root discovery doc).
async fn get_openapi_v3_root(state: Arc<ApiServerState>) -> Value {
    let router = build_router(state, None);
    let req = Request::builder()
        .method("GET")
        .uri("/openapi/v3")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200, "/openapi/v3 must serve 200");
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).expect("/openapi/v3 must be JSON")
}

// ---------------------------------------------------------------------------
// CRD builders. Mirror the canonical fixtures in
// k8s.io/kubernetes/test/utils/crd/crdtestutils.go used by crd_publish_openapi.
// ---------------------------------------------------------------------------

/// Canonical "Foo CRD with validation schema" body — same shape used by
/// upstream `schemaFoo` at `test/utils/crd/crdtestutils.go`. The schema
/// declares `spec.bars[].feeling` constrained by an enum.
fn schema_foo_crd(crd_name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": crd_name },
        "spec": {
            "group": group,
            "names": {
                "plural": plural,
                "singular": kind.to_lowercase(),
                "kind": kind,
                "listKind": format!("{}List", kind),
            },
            "scope": "Namespaced",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "description": "Foo CRD for Testing",
                        "type": "object",
                        "properties": {
                            "spec": {
                                "description": "Specification of Foo",
                                "type": "object",
                                "properties": {
                                    "bars": {
                                        "description": "List of Bars and their specs.",
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "required": ["name"],
                                            "properties": {
                                                "name": { "type": "string" },
                                                "age": { "type": "string" },
                                                "feeling": {
                                                    "type": "string",
                                                    "enum": ["Great", "Down"]
                                                }
                                            }
                                        }
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

/// CRD without `schema` (no validation), mirrors upstream's
/// `withoutValidationCRD` used by `works for CRD without validation schema`.
fn schema_less_crd(crd_name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": crd_name },
        "spec": {
            "group": group,
            "names": {
                "plural": plural,
                "singular": kind.to_lowercase(),
                "kind": kind,
                "listKind": format!("{}List", kind),
            },
            "scope": "Namespaced",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true
            }]
        }
    })
}

/// CRD using `x-kubernetes-preserve-unknown-fields: true` at root, mirrors
/// `works for CRD preserving unknown fields at the schema root`.
fn preserve_unknown_root_crd(crd_name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": crd_name },
        "spec": {
            "group": group,
            "names": {
                "plural": plural,
                "singular": kind.to_lowercase(),
                "kind": kind,
                "listKind": format!("{}List", kind),
            },
            "scope": "Namespaced",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "x-kubernetes-preserve-unknown-fields": true,
                        "description": "Preserve unknown fields at root"
                    }
                }
            }]
        }
    })
}

/// CRD using `x-kubernetes-preserve-unknown-fields: true` inside a nested
/// object, mirrors `works for CRD preserving unknown fields in an embedded
/// object`.
fn preserve_unknown_embedded_crd(crd_name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": crd_name },
        "spec": {
            "group": group,
            "names": {
                "plural": plural,
                "singular": kind.to_lowercase(),
                "kind": kind,
                "listKind": format!("{}List", kind),
            },
            "scope": "Namespaced",
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
                                    "embedded": {
                                        "type": "object",
                                        "x-kubernetes-preserve-unknown-fields": true
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

/// Multi-version CRD mirroring upstream `multiVersion` fixture used by
/// `updates the published spec when one version gets renamed` and
/// `removes definition from spec when one version gets changed to not be served`.
fn multi_version_crd(crd_name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": crd_name },
        "spec": {
            "group": group,
            "names": {
                "plural": plural,
                "singular": kind.to_lowercase(),
                "kind": kind,
                "listKind": format!("{}List", kind),
            },
            "scope": "Namespaced",
            "versions": [
                {
                    "name": "v2",
                    "served": true,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "description": "Foo CRD v2",
                            "type": "object",
                            "properties": {
                                "spec": { "type": "object", "properties": {
                                    "alpha": { "type": "string" }
                                }}
                            }
                        }
                    }
                },
                {
                    "name": "v3",
                    "served": true,
                    "storage": false,
                    "schema": {
                        "openAPIV3Schema": {
                            "description": "Foo CRD v3",
                            "type": "object",
                            "properties": {
                                "spec": { "type": "object", "properties": {
                                    "beta": { "type": "string" }
                                }}
                            }
                        }
                    }
                }
            ]
        }
    })
}

// ---------------------------------------------------------------------------
// crd_publish_openapi.go conformance mirror
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CustomResourcePublishOpenAPI works for CRD with validation schema [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:68
/// Sonobuoy (R160, 2026-04-26): FAIL → PASS after publish-on-request fix.
///
/// Two-part conformance check: (a) the CRD is published under
/// `/openapi/v2` with its schema definition keyed by reverse-domain
/// `group.version.kind`, and (b) the GVK extension
/// `x-kubernetes-group-version-kind` is attached on the definition.
/// Mirrors upstream `waitForDefinition(…) → expectMatchingItems`.
#[tokio::test]
async fn crd_with_validation_schema_publishes_to_openapi_v2() {
    let state = spawn_state();
    let crd = schema_foo_crd(
        "e2e-test-foos.example.com",
        "example.com",
        "e2e-test-foos",
        "Foo",
    );
    let (status, _body) = post_crd(state.clone(), &crd).await;
    assert!(
        (200..300).contains(&status),
        "CRD POST must succeed, got {}",
        status
    );

    let v2 = get_openapi_v2(state).await;
    let definitions = v2
        .pointer("/definitions")
        .and_then(|v| v.as_object())
        .expect("/definitions present");
    let def_key = "com.example.v1.Foo";
    let def = definitions
        .get(def_key)
        .unwrap_or_else(|| panic!("definition {} must be published", def_key));
    let gvk = def
        .get("x-kubernetes-group-version-kind")
        .expect("x-kubernetes-group-version-kind must be attached on publish");
    assert_eq!(gvk[0]["group"], "example.com");
    assert_eq!(gvk[0]["version"], "v1");
    assert_eq!(gvk[0]["kind"], "Foo");

    // Structural enum constraint must round-trip through the publish path.
    let feeling = def
        .pointer("/properties/spec/properties/bars/items/properties/feeling")
        .expect("feeling property published");
    let enum_values = feeling
        .get("enum")
        .and_then(|v| v.as_array())
        .expect("enum constraint must survive the publish pipeline (line 101)");
    assert_eq!(enum_values.len(), 2);
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI works for CRD without validation schema [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:126
/// Sonobuoy (R160, 2026-04-26): PASS
///
/// A CRD with no schema must still be published with a placeholder definition
/// (upstream uses `x-kubernetes-preserve-unknown-fields: true` implicitly).
/// The GVK extension must still be attached.
#[tokio::test]
async fn crd_without_validation_schema_publishes_to_openapi_v2() {
    let state = spawn_state();
    let crd = schema_less_crd(
        "e2e-test-bars.example.com",
        "example.com",
        "e2e-test-bars",
        "Bar",
    );
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(state).await;
    let definitions = v2
        .pointer("/definitions")
        .and_then(|v| v.as_object())
        .expect("/definitions present");
    let def_key = "com.example.v1.Bar";
    let def = definitions
        .get(def_key)
        .unwrap_or_else(|| panic!("definition {} must be published", def_key));
    let gvk = def
        .get("x-kubernetes-group-version-kind")
        .expect("GVK extension present");
    assert_eq!(gvk[0]["kind"], "Bar");
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI preserving unknown fields at schema root [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:157
/// Sonobuoy (R160, 2026-04-26): PASS
///
/// Upstream's `builder.go:393-395` collapses a root-level
/// `x-kubernetes-preserve-unknown-fields: true` schema into a bare
/// `{type: object}` definition (without the vendor extension and without
/// the original `properties`). kubectl explain/validate then accepts any
/// CR body because the definition has no constraints to violate.
/// Our publish path follows the same collapse rule, so the assertion here
/// mirrors upstream's expectation: the definition exists, the GVK is
/// attached, and user-defined `properties` are absent (since they would
/// otherwise contradict "preserve unknown").
#[tokio::test]
async fn crd_preserves_unknown_fields_at_root_in_openapi_v2() {
    let state = spawn_state();
    let crd = preserve_unknown_root_crd(
        "e2e-test-pur.example.com",
        "example.com",
        "e2e-test-pur",
        "Pur",
    );
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(state).await;
    let def = v2
        .pointer("/definitions/com.example.v1.Pur")
        .expect("definition published");
    let gvk = def
        .get("x-kubernetes-group-version-kind")
        .expect("GVK extension attached on collapsed schema");
    assert_eq!(gvk[0]["kind"], "Pur");
    // builder.go:393-395 — root preserve-unknown-fields collapses
    // user-defined properties; only the standard CRD properties (apiVersion,
    // kind, metadata) added by add_standard_crd_properties remain.
    let user_keys: Vec<&String> = def
        .pointer("/properties")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.keys()
                .filter(|k| !["apiVersion", "kind", "metadata"].contains(&k.as_str()))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        user_keys.is_empty(),
        "root preserve-unknown-fields must collapse user-defined properties; found: {:?}",
        user_keys
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI preserving unknown fields in embedded object [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:190
/// Sonobuoy (R160, 2026-04-26): PASS
#[tokio::test]
async fn crd_preserves_unknown_fields_in_embedded_object_in_openapi_v2() {
    let state = spawn_state();
    let crd = preserve_unknown_embedded_crd(
        "e2e-test-pue.example.com",
        "example.com",
        "e2e-test-pue",
        "Pue",
    );
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(state).await;
    let def = v2
        .pointer("/definitions/com.example.v1.Pue")
        .expect("definition published");
    let preserve = def
        .pointer("/properties/spec/properties/embedded/x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(
        preserve,
        "embedded x-kubernetes-preserve-unknown-fields must survive publish"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI works for multiple CRDs of different groups [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:224
/// Sonobuoy (R160, 2026-04-26): PASS
#[tokio::test]
async fn multiple_crds_of_different_groups_publish_independently() {
    let state = spawn_state();
    let foo = schema_foo_crd("foos.alpha.example.com", "alpha.example.com", "foos", "Foo");
    let bar = schema_foo_crd("bars.beta.example.com", "beta.example.com", "bars", "Bar");
    let (s1, _) = post_crd(state.clone(), &foo).await;
    let (s2, _) = post_crd(state.clone(), &bar).await;
    assert!((200..300).contains(&s1) && (200..300).contains(&s2));

    let v2 = get_openapi_v2(state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(
        defs.contains_key("com.example.alpha.v1.Foo"),
        "Foo definition from alpha group must be published"
    );
    assert!(
        defs.contains_key("com.example.beta.v1.Bar"),
        "Bar definition from beta group must be published"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI multiple CRDs of same group but different versions [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:251
/// Sonobuoy (R160, 2026-04-26): PASS
#[tokio::test]
async fn multiple_crds_same_group_different_versions_publish_separately() {
    let state = spawn_state();
    let crd = multi_version_crd("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(
        defs.contains_key("com.example.v2.Foo"),
        "v2 definition published"
    );
    assert!(
        defs.contains_key("com.example.v3.Foo"),
        "v3 definition published"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI multiple CRDs same group/version different kinds [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:290
/// Sonobuoy (R160, 2026-04-26): PASS
#[tokio::test]
async fn multiple_crds_same_group_version_different_kinds_publish_separately() {
    let state = spawn_state();
    let foo = schema_foo_crd("foos.example.com", "example.com", "foos", "Foo");
    let bar = schema_foo_crd("bars.example.com", "example.com", "bars", "Bar");
    let (s1, _) = post_crd(state.clone(), &foo).await;
    let (s2, _) = post_crd(state.clone(), &bar).await;
    assert!((200..300).contains(&s1) && (200..300).contains(&s2));

    let v2 = get_openapi_v2(state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(defs.contains_key("com.example.v1.Foo"));
    assert!(defs.contains_key("com.example.v1.Bar"));
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI updates the published spec when one version gets renamed [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:318
/// Sonobuoy (R160, 2026-04-26): FAIL → PASS after publish-on-request fix.
///
/// The CRD is created with versions v2+v3, then updated to rename v3→v4.
/// After the update the published spec must drop the v3 definition and
/// publish v4. The handler rebuilds the spec from live storage on every
/// request, so the rename propagates immediately.
#[tokio::test]
async fn crd_rename_version_updates_published_openapi_v2() {
    let state = spawn_state();
    let crd = multi_version_crd("foos.example.com", "example.com", "foos", "Foo");
    let (s1, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&s1));

    // Update: rename v3 → v4
    let mut updated = crd.clone();
    updated["spec"]["versions"][1]["name"] = json!("v4");
    let (s2, _) = put_crd(state.clone(), "foos.example.com", &updated).await;
    assert!((200..300).contains(&s2));

    let v2 = get_openapi_v2(state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(
        defs.contains_key("com.example.v4.Foo"),
        "renamed v4 definition must be published"
    );
    assert!(
        !defs.contains_key("com.example.v3.Foo"),
        "old v3 definition must be dropped after rename"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI removes definition from spec when version is unserved [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:361
/// Sonobuoy (R160, 2026-04-26): FAIL → PASS after publish-on-request fix.
/// Setting `served=false` on a CRD version now drops it from `/openapi/v2`
/// on the next request because the handler reads live storage state.
#[tokio::test]
async fn crd_unserved_version_is_removed_from_published_openapi_v2() {
    let state = spawn_state();
    let crd = multi_version_crd("foos.example.com", "example.com", "foos", "Foo");
    let (s1, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&s1));

    // Update: set v3.served = false.
    let mut updated = crd.clone();
    updated["spec"]["versions"][1]["served"] = json!(false);
    let (s2, _) = put_crd(state.clone(), "foos.example.com", &updated).await;
    assert!((200..300).contains(&s2));

    let v2 = get_openapi_v2(state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(
        defs.contains_key("com.example.v2.Foo"),
        "served v2 definition stays published"
    );
    assert!(
        !defs.contains_key("com.example.v3.Foo"),
        "unserved v3 definition must be removed from /openapi/v2"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI kubectl explain works for CR with same name as built-in [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:406
/// Sonobuoy (R160, 2026-04-26): PASS
///
/// kubectl explain reads `/openapi/v2` and disambiguates by group/version.
/// A CR whose `plural` matches a built-in (e.g. `pods` in a different
/// group) must still publish its own definition keyed by GVK. We assert
/// publish independence — the actual `kubectl explain` parser lives
/// outside the api-server and is not in scope.
#[tokio::test]
async fn crd_publish_does_not_collide_with_builtin_plural_name() {
    let state = spawn_state();
    let crd = schema_foo_crd("pods.example.com", "example.com", "pods", "PodLike");
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(state).await;
    let def = v2
        .pointer("/definitions/com.example.v1.PodLike")
        .expect("CRD definition keyed by GVK not by plural — no collision with core/v1 Pod");
    let gvk = def
        .pointer("/x-kubernetes-group-version-kind/0")
        .expect("GVK attached");
    assert_eq!(gvk["group"], "example.com", "stays in custom group");
}

// ---------------------------------------------------------------------------
// crd_conversion_webhook.go conformance mirror (2 tests)
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CustomResourceConversionWebhook should be able to convert from CR v1 to CR v2 [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_conversion_webhook.go:142
/// Sonobuoy (R160, 2026-04-26): not exercised in this round (no entry in e2e.log);
/// tracked as expected-FAIL because rusternetes does not yet implement
/// `spec.conversion.strategy: Webhook` (handler defaults to `None`).
#[tokio::test]
#[ignore = "Conformance failure tracker — webhook conversion not implemented yet; see docs/conformance/apimachinery-crd-openapi.md"]
async fn crd_conversion_webhook_converts_v1_to_v2() {
    let state = spawn_state();
    // Multi-version CRD with conversion.strategy=Webhook.
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "conversions.example.com" },
        "spec": {
            "group": "example.com",
            "names": {
                "plural": "conversions",
                "singular": "conversion",
                "kind": "Conversion",
                "listKind": "ConversionList"
            },
            "scope": "Namespaced",
            "conversion": {
                "strategy": "Webhook",
                "webhook": {
                    "conversionReviewVersions": ["v1"],
                    "clientConfig": {
                        "service": {
                            "namespace": "default",
                            "name": "conversion-webhook",
                            "path": "/convert"
                        }
                    }
                }
            },
            "versions": [
                {
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": { "openAPIV3Schema": {
                        "type": "object",
                        "properties": { "hostPort": { "type": "string" } }
                    }}
                },
                {
                    "name": "v2",
                    "served": true,
                    "storage": false,
                    "schema": { "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "host": { "type": "string" },
                            "port": { "type": "string" }
                        }
                    }}
                }
            ]
        }
    });
    let (status, body) = post_crd(state, &crd).await;
    assert!(
        (200..300).contains(&status),
        "conversion-strategy=Webhook CRD must be accepted, got {} body={:?}",
        status,
        body
    );
}

/// [sig-api-machinery] CustomResourceConversionWebhook should be able to convert non-homogeneous list of CRs [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_conversion_webhook.go:179
/// Sonobuoy (R160, 2026-04-26): not exercised in this round; tracked as
/// expected-FAIL until webhook conversion is implemented (see test above).
#[tokio::test]
#[ignore = "Conformance failure tracker — webhook conversion not implemented yet; see docs/conformance/apimachinery-crd-openapi.md"]
async fn crd_conversion_webhook_converts_non_homogeneous_list() {
    // Same fixture as above; the upstream test creates one v1 and one v2 CR,
    // then LISTs as v2 and expects both to round-trip through the webhook.
    // Without conversion-webhook support we cannot drive the LIST through a
    // converter; mark as failure tracker.
    let state = spawn_state();
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "mixed.example.com" },
        "spec": {
            "group": "example.com",
            "names": {
                "plural": "mixeds", "singular": "mixed",
                "kind": "Mixed", "listKind": "MixedList"
            },
            "scope": "Namespaced",
            "conversion": { "strategy": "Webhook" },
            "versions": [
                { "name": "v1", "served": true, "storage": true,
                  "schema": { "openAPIV3Schema": { "type": "object" } } },
                { "name": "v2", "served": true, "storage": false,
                  "schema": { "openAPIV3Schema": { "type": "object" } } }
            ]
        }
    });
    let (status, _) = post_crd(state, &crd).await;
    assert!((200..300).contains(&status));
}

// ---------------------------------------------------------------------------
// Structural-schema + OpenAPI v3 publish rules (extra coverage for the
// "CRD OpenAPI publishing (~9)" failure bucket in docs/CONFORMANCE.md:44)
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CRD definitions appear under `/openapi/v3` after publish
///
/// Upstream root: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:74
/// (the v3 endpoint mirrors v2 in our implementation; see
/// `handlers/openapi.rs::get_openapi_spec_path`).
/// Sonobuoy (R160, 2026-04-26): not directly exercised — supporting check.
///
/// `/openapi/v3` returns a paths-map and `/openapi/v3/apis/<group>/<version>`
/// returns a spec whose `components.schemas` includes the CRD definition.
#[tokio::test]
async fn crd_definition_appears_under_openapi_v3_group_version() {
    let state = spawn_state();
    let crd = schema_foo_crd("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    // Root v3 doc must advertise the new group/version path.
    let root = get_openapi_v3_root(state.clone()).await;
    let paths = root.pointer("/paths").and_then(|v| v.as_object()).unwrap();
    assert!(
        paths.contains_key("apis/example.com/v1"),
        "v3 root must advertise CRD group/version path; got keys: {:?}",
        paths.keys().collect::<Vec<_>>()
    );

    // The per-GV spec must include the CRD schema under components/schemas.
    let gv_spec = get_openapi_v3_for_group(state, "example.com", "v1").await;
    let schemas = gv_spec
        .pointer("/components/schemas")
        .and_then(|v| v.as_object())
        .expect("components.schemas present");
    assert!(
        schemas.contains_key("com.example.v1.Foo"),
        "v3 per-GV spec must include CRD definition; got keys: {:?}",
        schemas.keys().collect::<Vec<_>>()
    );
}

/// [sig-api-machinery] CRD `description` survives the publish round-trip
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_publish_openapi.go:74
/// Supporting check — upstream `expectMatchingItems` compares descriptions
/// when verifying the schema is correctly published.
#[tokio::test]
async fn crd_publish_preserves_description_metadata() {
    let state = spawn_state();
    let crd = schema_foo_crd("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(state).await;
    let def = v2
        .pointer("/definitions/com.example.v1.Foo")
        .expect("definition published");
    assert_eq!(
        def.get("description").and_then(|v| v.as_str()),
        Some("Foo CRD for Testing"),
        "root description must round-trip"
    );
    let bars_desc = def
        .pointer("/properties/spec/properties/bars/description")
        .and_then(|v| v.as_str());
    assert_eq!(
        bars_desc,
        Some("List of Bars and their specs."),
        "nested description must round-trip"
    );
}

/// [sig-api-machinery] CRD `required` fields survive the publish round-trip
///
/// Supporting check — structural schema `required` is what kubectl uses to
/// reject CRs missing mandatory fields; the upstream
/// "kubectl validation … rejects request that has unknown properties" step
/// at line 90 of `crd_publish_openapi.go` relies on this.
#[tokio::test]
async fn crd_publish_preserves_required_fields() {
    let state = spawn_state();
    let crd = schema_foo_crd("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(state).await;
    let required = v2
        .pointer("/definitions/com.example.v1.Foo/properties/spec/properties/bars/items/required")
        .and_then(|v| v.as_array())
        .expect("items.required must be present in published schema");
    assert!(
        required.iter().any(|v| v.as_str() == Some("name")),
        "items.required must include `name`"
    );
}

/// [sig-api-machinery] Delete CRD removes its definition from `/openapi/v2`
///
/// Supporting structural check — upstream's per-test cleanup
/// (`defer cleanupCRD(...)`) followed by a re-publish poll asserts the
/// definition disappears. The handler reads storage on every GET so a
/// DELETE on the CRD is reflected immediately in the next `/openapi/v2`
/// response.
#[tokio::test]
async fn delete_crd_drops_definition_from_published_openapi_v2() {
    let state = spawn_state();
    let crd = schema_foo_crd("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    // Sanity: published.
    let v2_before = get_openapi_v2(state.clone()).await;
    assert!(v2_before
        .pointer("/definitions/com.example.v1.Foo")
        .is_some());

    let del = delete_crd(state.clone(), "foos.example.com").await;
    assert!(
        (200..300).contains(&del),
        "DELETE CRD must succeed, got {}",
        del
    );

    let v2_after = get_openapi_v2(state).await;
    let defs = v2_after
        .pointer("/definitions")
        .unwrap()
        .as_object()
        .unwrap();
    assert!(
        !defs.contains_key("com.example.v1.Foo"),
        "definition must be removed from /openapi/v2 after CRD delete"
    );
}

/// [sig-api-machinery] `/openapi/v2` is empty (no CRD definitions) before any CRDs are created
///
/// Supporting structural check — baseline definitions
/// (`io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta`,
/// `io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference`) are always
/// present; CRD-derived definitions must NOT appear unsolicited.
#[tokio::test]
async fn openapi_v2_baseline_has_no_crd_definitions() {
    let state = spawn_state();
    let v2 = get_openapi_v2(state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    // No entry should look like a CRD key (reverse-domain group + version + kind).
    let crd_like: Vec<&String> = defs
        .keys()
        .filter(|k| !k.starts_with("io.k8s.apimachinery."))
        .collect();
    assert!(
        crd_like.is_empty(),
        "no CRD-derived definitions in baseline /openapi/v2, found: {:?}",
        crd_like
    );
}

/// [sig-api-machinery] `/openapi/v2` definitions are recomputed on every request
///
/// Supporting structural check — upstream test pollers expect a fresh
/// spec on every GET; serving a cached snapshot taken before the CRD
/// create/update is the root cause of failure bucket
/// `docs/CONFORMANCE.md:44`.
#[tokio::test]
async fn openapi_v2_is_recomputed_after_crd_create() {
    let state = spawn_state();
    let v2_pre = get_openapi_v2(state.clone()).await;
    let pre_defs = v2_pre
        .pointer("/definitions")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);

    let crd = schema_foo_crd("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2_post = get_openapi_v2(state).await;
    let post_defs = v2_post
        .pointer("/definitions")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);
    assert!(
        post_defs > pre_defs,
        "definitions count must increase after CRD create (pre={}, post={})",
        pre_defs,
        post_defs
    );
}

/// [sig-api-machinery] CRD published path keys mirror upstream `apis/<group>/<version>/<plural>` form
///
/// Supporting structural check — kubectl resolves resource → schema by
/// looking up the path `apis/<group>/<version>/namespaces/{namespace}/<plural>`
/// in the published spec and dereferencing the GET response's `$ref`.
#[tokio::test]
async fn crd_publish_includes_namespaced_get_path() {
    let state = spawn_state();
    let crd = schema_foo_crd("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(state).await;
    let paths = v2.pointer("/paths").and_then(|v| v.as_object()).unwrap();
    let expected = "/apis/example.com/v1/namespaces/{namespace}/foos";
    assert!(
        paths.contains_key(expected),
        "expected path {} in /openapi/v2/paths, got keys: {:?}",
        expected,
        paths.keys().collect::<Vec<_>>()
    );
}

/// [sig-api-machinery] Updating CRD schema is reflected in the next `/openapi/v2` read
///
/// Mirrors the storage-level check already covered by
/// `crd_openapi_publish_test::crd_update_reflects_in_stored_schema_for_openapi_publish`
/// but drives it through the HTTP surface so the publish pipeline is
/// exercised end-to-end. Because `/openapi/v2` is rebuilt from storage on
/// every GET, the PUT is reflected on the next read with no cache flush.
#[tokio::test]
async fn crd_schema_update_reflected_in_published_openapi_v2() {
    let state = spawn_state();
    let crd_v1 = schema_foo_crd("widgets.example.com", "example.com", "widgets", "Widget");
    let (s1, _) = post_crd(state.clone(), &crd_v1).await;
    assert!((200..300).contains(&s1));

    // Remove the `feeling` enum field via PUT.
    let mut crd_v2 = crd_v1.clone();
    crd_v2["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"]["bars"]["items"] = json!({
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "string" }
        }
    });
    let (s2, _) = put_crd(state.clone(), "widgets.example.com", &crd_v2).await;
    assert!((200..300).contains(&s2));

    let v2 = get_openapi_v2(state).await;
    let item_props = v2
        .pointer(
            "/definitions/com.example.v1.Widget/properties/spec/properties/bars/items/properties",
        )
        .and_then(|v| v.as_object())
        .expect("items.properties published");
    assert!(item_props.contains_key("name"));
    assert!(
        !item_props.contains_key("feeling"),
        "schema update must drop the removed field from /openapi/v2"
    );
}

/// [sig-api-machinery] CRDs from a non-served version are absent from `/openapi/v3` per-GV spec
///
/// Mirrors the v3 counterpart of
/// `crd_unserved_version_is_removed_from_published_openapi_v2`. Because the
/// v3 handler reads CRDs from live storage on each request and skips
/// versions with `served=false`, the unserved version is dropped from
/// `components/schemas` on the next read.
#[tokio::test]
async fn crd_unserved_version_absent_from_openapi_v3_group_version() {
    let state = spawn_state();
    let crd = multi_version_crd("foos.example.com", "example.com", "foos", "Foo");
    let (s1, _) = post_crd(state.clone(), &crd).await;
    assert!((200..300).contains(&s1));

    let mut updated = crd.clone();
    updated["spec"]["versions"][1]["served"] = json!(false);
    let (s2, _) = put_crd(state.clone(), "foos.example.com", &updated).await;
    assert!((200..300).contains(&s2));

    let gv_spec = get_openapi_v3_for_group(state, "example.com", "v3").await;
    let schemas = gv_spec
        .pointer("/components/schemas")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    assert!(
        !schemas.contains_key("com.example.v3.Foo"),
        "v3 per-GV spec must not include unserved CRD version"
    );
}
