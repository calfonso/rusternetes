//! Integration tests for CRD OpenAPI v2 schema publishing.
//!
//! Targets the 9 `[sig-api-machinery] CustomResourcePublishOpenAPI [Conformance]`
//! specs that failed because `/openapi/v2` did not include CRD validation schemas
//! in the aggregated Swagger 2.0 document.
//!
//! Each test drives the real Axum router on top of `StorageBackend::Memory` via
//! `tower::ServiceExt::oneshot` — the same surface the upstream conformance suite
//! exercises. No mock helpers, no handler-level unit tests — every assertion goes
//! through the full HTTP path.
//!
//! Harness mirrors `crates/api-server/tests/decode_missing_field_test.rs`.
//!
//! # Conformance coverage
//!
//! | Upstream Ginkgo descriptor | Status |
//! |---|---|
//! | works for CRD with validation schema | PASS |
//! | works for CRD without validation schema | PASS |
//! | preserving unknown fields at schema root | PASS |
//! | preserving unknown fields in embedded object | PASS |
//! | works for multiple CRDs of different groups | PASS |
//! | same group but different versions | PASS |
//! | updates published spec when one version gets renamed | PASS |
//! | removes definition when one version becomes not-served | PASS |
//! | removes definition when CRD is deleted | PASS |

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_state() -> TestApiServer {
    TestApiServer::new()
}

const CRDS_URI: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

/// POST a CRD body and return `(status_code, response_body)`.
async fn post_crd(state: &TestApiServer, crd_body: &Value) -> (u16, Value) {
    let (status, value) = state.post(CRDS_URI, crd_body).await;
    (status.as_u16(), value)
}

/// PUT an updated CRD body for an existing CRD.
async fn put_crd(state: &TestApiServer, name: &str, crd_body: &Value) -> (u16, Value) {
    let (status, value) = state.put(&format!("{CRDS_URI}/{name}"), crd_body).await;
    (status.as_u16(), value)
}

/// DELETE a CRD by name and return the HTTP status code.
async fn delete_crd(state: &TestApiServer, name: &str) -> u16 {
    let (status, _) = state.delete(&format!("{CRDS_URI}/{name}")).await;
    status.as_u16()
}

/// GET `/openapi/v2` and return the parsed JSON body.
/// Asserts 200 because conformance tests poll until they get a valid response.
async fn get_openapi_v2(state: &TestApiServer) -> Value {
    let (status, value) = state.get("/openapi/v2").await;
    assert_eq!(status.as_u16(), 200, "/openapi/v2 must serve 200");
    value
}

// ---------------------------------------------------------------------------
// CRD fixture builders
// ---------------------------------------------------------------------------

/// CRD with a structural validation schema — `spec.bars[].feeling` constrained
/// to enum `["Great", "Down"]`. Mirrors the upstream `schemaFoo` fixture.
fn crd_with_schema(name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": name },
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

/// CRD without a validation schema — upstream `withoutValidationCRD`.
fn crd_without_schema(name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": name },
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

/// CRD with `x-kubernetes-preserve-unknown-fields: true` at the schema root.
fn crd_preserve_unknown_root(name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": name },
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
                        "description": "Root-level preserve-unknown"
                    }
                }
            }]
        }
    })
}

/// CRD with `x-kubernetes-preserve-unknown-fields: true` inside a nested
/// embedded object.
fn crd_preserve_unknown_embedded(name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": name },
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

/// Multi-version CRD with v2 + v3, both served. Used for rename/unserve tests.
fn crd_multi_version(name: &str, group: &str, plural: &str, kind: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": name },
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
                    "schema": { "openAPIV3Schema": { "type": "object" } }
                },
                {
                    "name": "v3",
                    "served": true,
                    "storage": false,
                    "schema": { "openAPIV3Schema": { "type": "object" } }
                }
            ]
        }
    })
}

/// Compute the reverse-domain definition key used in the published spec, e.g.
/// `"example.com"` + `"v1"` + `"Foo"` → `"com.example.v1.Foo"`.
fn def_key(group: &str, version: &str, kind: &str) -> String {
    let parts: Vec<&str> = group.rsplitn(10, '.').collect();
    format!("{}.{}.{}", parts.join("."), version, kind)
}

// ---------------------------------------------------------------------------
// Tests — CRD OpenAPI v2 publish
// ---------------------------------------------------------------------------

/// [sig-api-machinery] CustomResourcePublishOpenAPI works for CRD with validation schema [Conformance]
///
/// POST a CRD whose version carries an `openAPIV3Schema`, then GET `/openapi/v2`
/// and assert:
///   - the definition is keyed by reversed-domain `group.version.kind`;
///   - `x-kubernetes-group-version-kind` is attached with the right GVK;
///   - the user-supplied `enum` constraint round-trips through the publish path.
#[tokio::test]
async fn crd_with_validation_schema_appears_in_openapi_v2() {
    let state = spawn_state();
    let crd = crd_with_schema("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(&state, &crd).await;
    assert!(
        (200..300).contains(&status),
        "CRD POST must succeed, got {}",
        status
    );

    let v2 = get_openapi_v2(&state).await;
    let key = def_key("example.com", "v1", "Foo");
    let def = v2
        .pointer(&format!("/definitions/{}", key))
        .unwrap_or_else(|| panic!("definition {} must be published in /openapi/v2", key));

    // GVK extension must be present and correct.
    let gvk = def
        .get("x-kubernetes-group-version-kind")
        .and_then(|v| v.as_array())
        .expect("x-kubernetes-group-version-kind must be an array");
    assert_eq!(gvk.len(), 1);
    assert_eq!(gvk[0]["group"], "example.com");
    assert_eq!(gvk[0]["version"], "v1");
    assert_eq!(gvk[0]["kind"], "Foo");

    // Enum constraint must survive the publish pipeline.
    let feeling = def
        .pointer("/properties/spec/properties/bars/items/properties/feeling")
        .expect("feeling property published");
    let enum_values = feeling
        .get("enum")
        .and_then(|v| v.as_array())
        .expect("enum constraint must survive publish");
    assert_eq!(enum_values.len(), 2);
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI works for CRD without validation schema [Conformance]
///
/// A CRD with no `openAPIV3Schema` must still produce a definition stub so that
/// discovery clients and kubectl can resolve the GVK. The definition carries
/// `{type: object}` plus the GVK extension.
#[tokio::test]
async fn crd_without_validation_schema_gets_stub_definition_in_openapi_v2() {
    let state = spawn_state();
    let crd = crd_without_schema("bars.example.com", "example.com", "bars", "Bar");
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
    let key = def_key("example.com", "v1", "Bar");
    let def = v2
        .pointer(&format!("/definitions/{}", key))
        .unwrap_or_else(|| panic!("stub definition {} must be published", key));

    assert_eq!(def["type"], "object");
    let gvk = def
        .get("x-kubernetes-group-version-kind")
        .and_then(|v| v.as_array())
        .expect("GVK extension present on schema-less CRD");
    assert_eq!(gvk[0]["kind"], "Bar");

    // Standard K8s wrapper properties (metadata, apiVersion, kind) must be injected.
    let props = def["properties"].as_object().expect("properties present");
    assert!(props.contains_key("metadata"));
    assert!(props.contains_key("apiVersion"));
    assert!(props.contains_key("kind"));
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI preserving unknown fields at schema root [Conformance]
///
/// When `x-kubernetes-preserve-unknown-fields: true` appears at the schema root,
/// the upstream `builder.go:393-395` collapses the definition to a bare
/// `{type: object}` so kubectl accepts any CR body without client-side
/// constraint checking.
#[tokio::test]
async fn crd_root_preserve_unknown_fields_collapses_definition_in_openapi_v2() {
    let state = spawn_state();
    let crd = crd_preserve_unknown_root("purs.example.com", "example.com", "purs", "Pur");
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
    let key = def_key("example.com", "v1", "Pur");
    let def = v2
        .pointer(&format!("/definitions/{}", key))
        .expect("definition published");
    let gvk = def
        .get("x-kubernetes-group-version-kind")
        .expect("GVK extension attached on collapsed schema");
    assert_eq!(gvk[0]["kind"], "Pur");

    // User-defined properties must be absent — only the three standard CRD
    // wrapper properties (apiVersion, kind, metadata) remain.
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
        "root preserve-unknown-fields must collapse user properties; found: {:?}",
        user_keys
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI preserving unknown fields in embedded object [Conformance]
///
/// `x-kubernetes-preserve-unknown-fields: true` on a nested property must be
/// preserved (not stripped) in the published spec so kubectl knows the embedded
/// object is schema-free.
#[tokio::test]
async fn crd_embedded_preserve_unknown_fields_survives_publish() {
    let state = spawn_state();
    let crd = crd_preserve_unknown_embedded("pues.example.com", "example.com", "pues", "Pue");
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
    let key = def_key("example.com", "v1", "Pue");
    let def = v2
        .pointer(&format!("/definitions/{}", key))
        .expect("definition published");
    let preserve = def
        .pointer("/properties/spec/properties/embedded/x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(
        preserve,
        "embedded x-kubernetes-preserve-unknown-fields must survive the publish path"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI updates published spec when one version gets renamed [Conformance]
///
/// Creating a CRD with versions v2+v3, then renaming v3→v4 via PUT, must:
///   - add a definition keyed by `v4`;
///   - remove the stale `v3` definition.
///
/// Because the handler rebuilds the spec from live storage on every GET, the
/// rename is reflected on the very next `/openapi/v2` request.
#[tokio::test]
async fn crd_version_rename_updates_published_openapi_v2_definition() {
    let state = spawn_state();
    let crd = crd_multi_version("foos.example.com", "example.com", "foos", "Foo");
    let (s1, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&s1));

    // Rename v3 → v4.
    let mut updated = crd.clone();
    updated["spec"]["versions"][1]["name"] = json!("v4");
    let (s2, body) = put_crd(&state, "foos.example.com", &updated).await;
    assert!(
        (200..300).contains(&s2),
        "PUT renamed CRD must succeed, got {}: {}",
        s2,
        body
    );

    let v2 = get_openapi_v2(&state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(
        defs.contains_key(&def_key("example.com", "v4", "Foo")),
        "renamed v4 definition must be published; defs: {:?}",
        defs.keys().collect::<Vec<_>>()
    );
    assert!(
        !defs.contains_key(&def_key("example.com", "v3", "Foo")),
        "stale v3 definition must be dropped after rename"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI removes definition from spec when version becomes not-served [Conformance]
///
/// Setting `served=false` on a version must drop it from `/openapi/v2`. The
/// handler reads live storage on every request so the change is immediate.
#[tokio::test]
async fn crd_unserved_version_is_removed_from_openapi_v2() {
    let state = spawn_state();
    let crd = crd_multi_version("foos.example.com", "example.com", "foos", "Foo");
    let (s1, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&s1));

    // Set v3.served = false.
    let mut updated = crd.clone();
    updated["spec"]["versions"][1]["served"] = json!(false);
    let (s2, body) = put_crd(&state, "foos.example.com", &updated).await;
    assert!(
        (200..300).contains(&s2),
        "PUT unserved CRD must succeed, got {}: {}",
        s2,
        body
    );

    let v2 = get_openapi_v2(&state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(
        defs.contains_key(&def_key("example.com", "v2", "Foo")),
        "served v2 definition must stay in the spec"
    );
    assert!(
        !defs.contains_key(&def_key("example.com", "v3", "Foo")),
        "unserved v3 definition must be absent from /openapi/v2"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI works for multiple CRDs of different groups [Conformance]
///
/// Two CRDs in different API groups must each publish their own definition,
/// independently keyed by their reversed-domain group.
#[tokio::test]
async fn multiple_crds_different_groups_each_publish_definition() {
    let state = spawn_state();
    let foo = crd_with_schema("foos.alpha.example.com", "alpha.example.com", "foos", "Foo");
    let bar = crd_with_schema("bars.beta.example.com", "beta.example.com", "bars", "Bar");
    let (s1, _) = post_crd(&state, &foo).await;
    let (s2, _) = post_crd(&state, &bar).await;
    assert!((200..300).contains(&s1) && (200..300).contains(&s2));

    let v2 = get_openapi_v2(&state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(
        defs.contains_key(&def_key("alpha.example.com", "v1", "Foo")),
        "Foo definition from alpha group must be published; defs: {:?}",
        defs.keys().collect::<Vec<_>>()
    );
    assert!(
        defs.contains_key(&def_key("beta.example.com", "v1", "Bar")),
        "Bar definition from beta group must be published"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI multiple CRDs same group but different versions [Conformance]
///
/// A CRD with two served versions (v2 and v3) must produce a separate
/// definition for each, keyed by its own version segment.
#[tokio::test]
async fn multi_version_crd_publishes_definition_per_served_version() {
    let state = spawn_state();
    let crd = crd_multi_version("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    assert!(
        defs.contains_key(&def_key("example.com", "v2", "Foo")),
        "v2 definition must be published"
    );
    assert!(
        defs.contains_key(&def_key("example.com", "v3", "Foo")),
        "v3 definition must be published"
    );
}

/// [sig-api-machinery] CustomResourcePublishOpenAPI deleting a CRD removes its definition [Conformance]
///
/// After a CRD is deleted the next GET `/openapi/v2` must not include its
/// definitions. Because the handler reads from live storage on every request,
/// the removal is immediate — no cache flush needed.
#[tokio::test]
async fn deleted_crd_is_removed_from_openapi_v2() {
    let state = spawn_state();
    let crd = crd_with_schema("foos.example.com", "example.com", "foos", "Foo");
    let (s1, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&s1));

    // Sanity: published before delete.
    let v2_before = get_openapi_v2(&state).await;
    assert!(v2_before
        .pointer(&format!(
            "/definitions/{}",
            def_key("example.com", "v1", "Foo")
        ))
        .is_some());

    let del = delete_crd(&state, "foos.example.com").await;
    assert!(
        (200..300).contains(&del),
        "DELETE CRD must succeed, got {}",
        del
    );

    let v2_after = get_openapi_v2(&state).await;
    let defs = v2_after
        .pointer("/definitions")
        .unwrap()
        .as_object()
        .unwrap();
    assert!(
        !defs.contains_key(&def_key("example.com", "v1", "Foo")),
        "definition must be absent from /openapi/v2 after CRD delete"
    );
}

/// Verify the `/openapi/v2` spec is regenerated on every GET so that CRD
/// create/update/delete is reflected without a server restart or cache flush.
#[tokio::test]
async fn openapi_v2_regenerated_on_every_request_after_crd_create() {
    let state = spawn_state();

    let pre = get_openapi_v2(&state).await;
    let pre_count = pre
        .pointer("/definitions")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);

    let crd = crd_with_schema("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let post = get_openapi_v2(&state).await;
    let post_count = post
        .pointer("/definitions")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);

    assert!(
        post_count > pre_count,
        "definitions count must increase after CRD create (pre={}, post={})",
        pre_count,
        post_count
    );
}
