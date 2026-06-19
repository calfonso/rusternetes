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

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS_URI: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_state() -> TestApiServer {
    TestApiServer::new()
}

/// POST the given CRD JSON. Returns `(status, body)`.
async fn post_crd(state: &TestApiServer, crd_body: &Value) -> (u16, Value) {
    let (status, value) = state.post(CRDS_URI, crd_body).await;
    (status.as_u16(), value)
}

/// PUT the given CRD JSON to update an existing CRD.
async fn put_crd(state: &TestApiServer, name: &str, crd_body: &Value) -> (u16, Value) {
    let (status, value) = state.put(&format!("{CRDS_URI}/{name}"), crd_body).await;
    (status.as_u16(), value)
}

/// DELETE an existing CRD by name.
async fn delete_crd(state: &TestApiServer, name: &str) -> u16 {
    state.delete(&format!("{CRDS_URI}/{name}")).await.0.as_u16()
}

/// GET the published v2 swagger spec. Returns the parsed JSON body.
async fn get_openapi_v2(state: &TestApiServer) -> Value {
    let (status, value) = state.get("/openapi/v2").await;
    assert_eq!(
        status.as_u16(),
        200,
        "/openapi/v2 must serve 200 (upstream tests poll until valid)"
    );
    value
}

/// GET `/openapi/v3/apis/<group>/<version>`. Returns the parsed JSON body.
async fn get_openapi_v3_for_group(state: &TestApiServer, group: &str, version: &str) -> Value {
    let uri = format!("/openapi/v3/apis/{}/{}", group, version);
    let (status, value) = state.get(&uri).await;
    assert_eq!(status.as_u16(), 200, "{} must serve 200", uri);
    value
}

/// GET `/openapi/v3` (root discovery doc).
async fn get_openapi_v3_root(state: &TestApiServer) -> Value {
    let (status, value) = state.get("/openapi/v3").await;
    assert_eq!(status.as_u16(), 200, "/openapi/v3 must serve 200");
    value
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
    let (status, _body) = post_crd(&state, &crd).await;
    assert!(
        (200..300).contains(&status),
        "CRD POST must succeed, got {}",
        status
    );

    let v2 = get_openapi_v2(&state).await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
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
    let (s1, _) = post_crd(&state, &foo).await;
    let (s2, _) = post_crd(&state, &bar).await;
    assert!((200..300).contains(&s1) && (200..300).contains(&s2));

    let v2 = get_openapi_v2(&state).await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
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
    let (s1, _) = post_crd(&state, &foo).await;
    let (s2, _) = post_crd(&state, &bar).await;
    assert!((200..300).contains(&s1) && (200..300).contains(&s2));

    let v2 = get_openapi_v2(&state).await;
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
    let (s1, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&s1));

    // Update: rename v3 → v4
    let mut updated = crd.clone();
    updated["spec"]["versions"][1]["name"] = json!("v4");
    let (s2, _) = put_crd(&state, "foos.example.com", &updated).await;
    assert!((200..300).contains(&s2));

    let v2 = get_openapi_v2(&state).await;
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
    let (s1, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&s1));

    // Update: set v3.served = false.
    let mut updated = crd.clone();
    updated["spec"]["versions"][1]["served"] = json!(false);
    let (s2, _) = put_crd(&state, "foos.example.com", &updated).await;
    assert!((200..300).contains(&s2));

    let v2 = get_openapi_v2(&state).await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
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
//
// Mock conversion webhook server below mirrors upstream's `crconverter`
// reference implementation in test/images/agnhost/crd-conversion-webhook/converter,
// scoped to the two transformations exercised by the two upstream Ginkgo
// descriptors:
//   v1 `hostPort: "host:port"`  <->  v2 `host: "host", port: "port"`.
// The server speaks the apiextensions.k8s.io/v1 ConversionReview protocol.
// ---------------------------------------------------------------------------

/// Spawn a tiny HTTP server on a random localhost port that performs the
/// hostPort <-> host/port conversion between the two CRD versions. Returns
/// the URL to install in `conversion.webhook.clientConfig.url`.
async fn spawn_mock_conversion_webhook() -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => continue,
            };
            tokio::spawn(async move {
                // Read the full request — header + body. We don't pipeline.
                let mut buf = Vec::with_capacity(8192);
                let mut tmp = [0u8; 4096];
                // Naive read loop with a short bound — webhook bodies are tiny.
                for _ in 0..16 {
                    let n = sock.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    // Stop once we've definitely seen the body — last `}` after the
                    // CRLF/CRLF terminator.
                    let s = String::from_utf8_lossy(&buf);
                    if let Some(idx) = s.find("\r\n\r\n") {
                        let body = &s[idx + 4..];
                        if !body.is_empty() && body.trim_end().ends_with('}') {
                            break;
                        }
                    }
                }
                let text = String::from_utf8_lossy(&buf).to_string();
                let body_start = text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(text.len());
                let body = &text[body_start..];
                let review: Value = serde_json::from_str(body).unwrap_or(Value::Null);
                let req = &review["request"];
                let desired = req["desiredAPIVersion"].as_str().unwrap_or("").to_string();
                let uid = req["uid"].as_str().unwrap_or("").to_string();
                let objects = req["objects"].as_array().cloned().unwrap_or_default();

                let mut converted: Vec<Value> = Vec::with_capacity(objects.len());
                for mut obj in objects {
                    let from_ver = obj["apiVersion"]
                        .as_str()
                        .unwrap_or("")
                        .rsplit('/')
                        .next()
                        .unwrap_or("")
                        .to_string();
                    let to_ver = desired.rsplit('/').next().unwrap_or("").to_string();
                    if from_ver == "v1" && to_ver == "v2" {
                        if let Some(hp) = obj.get("hostPort").and_then(|v| v.as_str()) {
                            let mut parts = hp.splitn(2, ':');
                            let host = parts.next().unwrap_or("").to_string();
                            let port = parts.next().unwrap_or("").to_string();
                            let map = obj.as_object_mut().unwrap();
                            map.remove("hostPort");
                            map.insert("host".into(), Value::String(host));
                            map.insert("port".into(), Value::String(port));
                        }
                    } else if from_ver == "v2" && to_ver == "v1" {
                        let host = obj
                            .get("host")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let port = obj
                            .get("port")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let map = obj.as_object_mut().unwrap();
                        map.remove("host");
                        map.remove("port");
                        map.insert(
                            "hostPort".into(),
                            Value::String(format!("{}:{}", host, port)),
                        );
                    }
                    obj["apiVersion"] = Value::String(desired.clone());
                    converted.push(obj);
                }

                let resp_body = json!({
                    "apiVersion": "apiextensions.k8s.io/v1",
                    "kind": "ConversionReview",
                    "response": {
                        "uid": uid,
                        "convertedObjects": converted,
                        "result": { "status": "Success" }
                    }
                });
                let resp_bytes = serde_json::to_vec(&resp_body).unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    resp_bytes.len()
                );
                let _ = sock.write_all(header.as_bytes()).await;
                let _ = sock.write_all(&resp_bytes).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    format!("http://{}/convert", addr)
}

/// Issue a request through the router built on `state` and return `(status, body)`.
async fn router_request(
    state: &TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    let (status, value) = state.send(method, uri, content_type, body).await;
    (status.as_u16(), value)
}

/// [sig-api-machinery] CustomResourceConversionWebhook should be able to convert from CR v1 to CR v2 [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_conversion_webhook.go:142
///
/// Drives the full webhook conversion path end-to-end through the Axum router:
/// register a multi-version CRD whose `spec.conversion.strategy=Webhook` points
/// at an in-process mock that performs the canonical hostPort -> host/port
/// transformation, create a CR at v1, GET it at v2, then assert the response
/// body has the v2 shape (separate `host` + `port`, no `hostPort`).
#[tokio::test]
async fn crd_conversion_webhook_converts_v1_to_v2() {
    let state = spawn_state();
    let webhook_url = spawn_mock_conversion_webhook().await;

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
                    "clientConfig": { "url": webhook_url }
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
    let (status, body) = post_crd(&state, &crd).await;
    assert!(
        (200..300).contains(&status),
        "conversion-strategy=Webhook CRD must be accepted, got {} body={:?}",
        status,
        body
    );

    // Create a v1 CR with `hostPort: localhost:8080`.
    let cr_v1 = json!({
        "apiVersion": "example.com/v1",
        "kind": "Conversion",
        "metadata": { "name": "sample" },
        "hostPort": "localhost:8080"
    });
    let (cs, cb) = router_request(
        &state,
        "POST",
        "/apis/example.com/v1/namespaces/default/conversions",
        Some(&cr_v1),
    )
    .await;
    assert!(
        (200..300).contains(&cs),
        "create v1 CR failed: {} {}",
        cs,
        cb
    );

    // GET the same CR at v2 — must round-trip through the webhook.
    let (gs, gb) = router_request(
        &state,
        "GET",
        "/apis/example.com/v2/namespaces/default/conversions/sample",
        None,
    )
    .await;
    assert!((200..300).contains(&gs), "GET v2 CR failed: {} {}", gs, gb);
    assert_eq!(
        gb["apiVersion"].as_str(),
        Some("example.com/v2"),
        "GET v2 must return apiVersion=example.com/v2; body={}",
        gb
    );
    assert_eq!(
        gb["host"].as_str(),
        Some("localhost"),
        "v2 body must have host=localhost (webhook split hostPort); body={}",
        gb
    );
    assert_eq!(
        gb["port"].as_str(),
        Some("8080"),
        "v2 body must have port=8080 (webhook split hostPort); body={}",
        gb
    );
    assert!(
        gb.get("hostPort").is_none(),
        "v2 body must not retain v1 hostPort; body={}",
        gb
    );
}

/// [sig-api-machinery] CustomResourceConversionWebhook should be able to convert non-homogeneous list of CRs [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/crd_conversion_webhook.go:179
///
/// Creates two CRs at different stored versions (v1 + v2), LISTs at v2, and
/// asserts the webhook converted the v1-stored item into the v2 shape while
/// leaving the already-v2 item alone. Drives `convert_custom_resources` on
/// the LIST path.
#[tokio::test]
async fn crd_conversion_webhook_converts_non_homogeneous_list() {
    let state = spawn_state();
    let webhook_url = spawn_mock_conversion_webhook().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "mixeds.example.com" },
        "spec": {
            "group": "example.com",
            "names": {
                "plural": "mixeds", "singular": "mixed",
                "kind": "Mixed", "listKind": "MixedList"
            },
            "scope": "Namespaced",
            "conversion": {
                "strategy": "Webhook",
                "webhook": {
                    "conversionReviewVersions": ["v1"],
                    "clientConfig": { "url": webhook_url }
                }
            },
            "versions": [
                { "name": "v1", "served": true, "storage": true,
                  "schema": { "openAPIV3Schema": {
                      "type": "object",
                      "properties": { "hostPort": { "type": "string" } }
                  }}},
                { "name": "v2", "served": true, "storage": false,
                  "schema": { "openAPIV3Schema": {
                      "type": "object",
                      "properties": {
                          "host": { "type": "string" },
                          "port": { "type": "string" }
                      }
                  }}}
            ]
        }
    });
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    // CR #1: created at v1 (stored as v1).
    let cr_v1 = json!({
        "apiVersion": "example.com/v1",
        "kind": "Mixed",
        "metadata": { "name": "from-v1" },
        "hostPort": "alpha:1000"
    });
    let (s1, _) = router_request(
        &state,
        "POST",
        "/apis/example.com/v1/namespaces/default/mixeds",
        Some(&cr_v1),
    )
    .await;
    assert!((200..300).contains(&s1));

    // CR #2: created at v2 (stored as v2 — upstream test analogue switches
    // the served route; our storage records whichever version was used to POST).
    let cr_v2 = json!({
        "apiVersion": "example.com/v2",
        "kind": "Mixed",
        "metadata": { "name": "from-v2" },
        "host": "beta",
        "port": "2000"
    });
    let (s2, _) = router_request(
        &state,
        "POST",
        "/apis/example.com/v2/namespaces/default/mixeds",
        Some(&cr_v2),
    )
    .await;
    assert!((200..300).contains(&s2));

    // LIST at v2 — webhook must convert the v1-stored item, the v2-stored
    // item must pass through untouched.
    let (ls, lb) = router_request(
        &state,
        "GET",
        "/apis/example.com/v2/namespaces/default/mixeds",
        None,
    )
    .await;
    assert!((200..300).contains(&ls), "LIST v2 failed: {} {}", ls, lb);
    let items = lb["items"].as_array().expect("items array");
    assert_eq!(
        items.len(),
        2,
        "expected 2 items, got {}: {}",
        items.len(),
        lb
    );
    for item in items {
        assert_eq!(
            item["apiVersion"].as_str(),
            Some("example.com/v2"),
            "every listed item must have apiVersion=v2; got: {}",
            item
        );
        assert!(
            item.get("hostPort").is_none(),
            "no v2 item may retain v1 hostPort; got: {}",
            item
        );
        let host = item["host"].as_str().unwrap_or("");
        let port = item["port"].as_str().unwrap_or("");
        match item["metadata"]["name"].as_str() {
            Some("from-v1") => {
                assert_eq!(host, "alpha");
                assert_eq!(port, "1000");
            }
            Some("from-v2") => {
                assert_eq!(host, "beta");
                assert_eq!(port, "2000");
            }
            other => panic!("unexpected item name {:?}: {}", other, item),
        }
    }
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    // Root v3 doc must advertise the new group/version path.
    let root = get_openapi_v3_root(&state).await;
    let paths = root.pointer("/paths").and_then(|v| v.as_object()).unwrap();
    assert!(
        paths.contains_key("apis/example.com/v1"),
        "v3 root must advertise CRD group/version path; got keys: {:?}",
        paths.keys().collect::<Vec<_>>()
    );

    // The per-GV spec must include the CRD schema under components/schemas.
    let gv_spec = get_openapi_v3_for_group(&state, "example.com", "v1").await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    // Sanity: published.
    let v2_before = get_openapi_v2(&state).await;
    assert!(v2_before
        .pointer("/definitions/com.example.v1.Foo")
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
        !defs.contains_key("com.example.v1.Foo"),
        "definition must be removed from /openapi/v2 after CRD delete"
    );
}

/// [sig-api-machinery] `/openapi/v2` is empty (no CRD definitions) before any CRDs are created
///
/// Supporting structural check — baseline definitions
/// (`io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta`,
/// `io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference`, and the
/// `io.k8s.api.<group>.<version>.<Kind>` built-in GVK stubs upstream
/// kube-apiserver publishes via `kube-openapi/pkg/builder`) are always
/// present; CRD-derived definitions must NOT appear unsolicited.
#[tokio::test]
async fn openapi_v2_baseline_has_no_crd_definitions() {
    let state = spawn_state();
    let v2 = get_openapi_v2(&state).await;
    let defs = v2.pointer("/definitions").unwrap().as_object().unwrap();
    // No entry should look like a CRD key (reverse-domain group + version + kind).
    // Both `io.k8s.apimachinery.*` (shared meta types) and `io.k8s.api.*`
    // (built-in GVKs like Pod/Deployment/Job) are baseline and not CRD-derived.
    let crd_like: Vec<&String> = defs
        .keys()
        .filter(|k| !k.starts_with("io.k8s.apimachinery.") && !k.starts_with("io.k8s.api."))
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
    let v2_pre = get_openapi_v2(&state).await;
    let pre_defs = v2_pre
        .pointer("/definitions")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);

    let crd = schema_foo_crd("foos.example.com", "example.com", "foos", "Foo");
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2_post = get_openapi_v2(&state).await;
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
    let (status, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&status));

    let v2 = get_openapi_v2(&state).await;
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
    let (s1, _) = post_crd(&state, &crd_v1).await;
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
    let (s2, _) = put_crd(&state, "widgets.example.com", &crd_v2).await;
    assert!((200..300).contains(&s2));

    let v2 = get_openapi_v2(&state).await;
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
    let (s1, _) = post_crd(&state, &crd).await;
    assert!((200..300).contains(&s1));

    let mut updated = crd.clone();
    updated["spec"]["versions"][1]["served"] = json!(false);
    let (s2, _) = put_crd(&state, "foos.example.com", &updated).await;
    assert!((200..300).contains(&s2));

    let gv_spec = get_openapi_v3_for_group(&state, "example.com", "v3").await;
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
