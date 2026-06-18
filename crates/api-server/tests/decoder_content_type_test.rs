//! Content-Type negotiation and patch-decoder dispatch tests.
//!
//! Mirrors upstream Kubernetes' decoder selection behavior, where the
//! `Content-Type` header on a write request determines which decode / patch
//! code path the apiserver follows. The relevant upstream entry points are:
//!
//! - `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go` —
//!   `PatchResource` switches on `Content-Type` to choose between
//!   `JSONPatchType`, `MergePatchType`, `StrategicMergePatchType`, and
//!   `ApplyPatchType` (`application/apply-patch+yaml`, SSA).
//! - `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go` /
//!   `update.go` — pick a decoder based on `Content-Type` (JSON, YAML,
//!   protobuf). Unsupported types return 415.
//!
//! In rusternetes the dispatch sites are:
//! - `crates/api-server/src/middleware.rs::normalize_content_type_middleware`
//!   — preserves the original Content-Type in `x-original-content-type` for
//!   the three patch types, and forwards `application/apply-patch+yaml`
//!   unchanged for SSA.
//! - `crates/api-server/src/patch.rs::PatchType::from_content_type` —
//!   maps the three patch MIME types onto `PatchType` variants.
//! - `crates/api-server/src/handlers/generic_patch.rs::patch_namespaced_resource`
//!   and `crates/api-server/src/handlers/pod.rs::patch` — branch on
//!   `apply-patch` to take the server-side apply path when
//!   `?fieldManager=` is present.
//!
//! Each test below sends a single HTTP request through the in-process Axum
//! router via `tower::ServiceExt::oneshot`, then either:
//!   - asserts the response status code (success vs. 415), or
//!   - inspects the resulting stored object to verify the correct decoder /
//!     patch implementation was selected (the three patch types produce
//!     observably different results for the same input, particularly for
//!     arrays-of-maps with a `name` merge key).

use axum::http::Method;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `send_bytes` lets
// us push an arbitrary raw body with an explicit (or absent) Content-Type so
// the request-decoding middleware is exercised verbatim. `mem` is the backing
// store for stored-object assertions.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "default";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// Send an arbitrary-body request with an explicit `Content-Type` header
/// (no normalization on the test side — the middleware is what we are
/// exercising).
async fn send_with_ct(
    router: TestApiServer,
    method: Method,
    uri: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (u16, Value) {
    let (status, _, value) = router
        .send_bytes(method.as_str(), uri, Some(content_type), Some(body))
        .await;
    (status.as_u16(), value)
}

/// Same as `send_with_ct` but no `Content-Type` header at all. Mirrors
/// upstream's "no Content-Type" decode test.
async fn send_without_ct(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: Vec<u8>,
) -> (u16, Value) {
    let (status, _, value) = router
        .send_bytes(method.as_str(), uri, None, Some(body))
        .await;
    (status.as_u16(), value)
}

/// Read the JSON stored at `key`. Panics if the key is absent.
async fn read_stored(mem: &Arc<MemoryStorage>, key: &str) -> Value {
    mem.get::<Value>(key)
        .await
        .unwrap_or_else(|e| panic!("expected key {} to exist: {:?}", key, e))
}

/// Look up the named container in `stored.spec.containers`. Panics if the
/// container is missing — used in negative-path tests to prove that a
/// rejected PATCH did not mutate `c1`'s image.
fn container_image(stored: &Value, name: &str) -> String {
    stored["spec"]["containers"]
        .as_array()
        .unwrap_or_else(|| panic!("spec.containers must be an array; got {}", stored))
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("container {} must exist in {}", name, stored))["image"]
        .as_str()
        .unwrap_or_else(|| panic!("container {}.image must be a string", name))
        .to_string()
}

/// Seed a Pod into memory storage and return its registry key. The seeded
/// shape carries TWO containers — `c1` and `c2` — so that strategic-merge
/// vs RFC 7396 merge-patch differences are observable: SMP merges by the
/// `name` field (preserving `c2`), while RFC 7396 replaces the whole array.
async fn seed_pod(mem: &Arc<MemoryStorage>, name: &str) -> String {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
            "labels": {"original": "true"}
        },
        "spec": {
            "containers": [
                {"name": "c1", "image": "busybox:1.0"},
                {"name": "c2", "image": "nginx:1.0"}
            ]
        }
    });
    let key = build_key("pods", Some(TEST_NS), name);
    mem.create(&key, &pod).await.expect("seed pod");
    key
}

/// Seed a Deployment carrying the same TWO containers (`c1`, `c2`) inside
/// `spec.template.spec.containers`. Deployments are NOT covered by
/// `ValidatePodUpdate`'s immutability fence, so they remain the right
/// vehicle for proving SMP-vs-RFC7396-vs-RFC6902 array semantics
/// observably diverge. (Pods reject container add/remove on update, so
/// the same patch bodies against a Pod would 422 before the decoder
/// difference becomes visible.)
async fn seed_deployment(mem: &Arc<MemoryStorage>, name: &str) -> String {
    let dep = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
            "labels": {"original": "true"}
        },
        "spec": {
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {
                    "containers": [
                        {"name": "c1", "image": "busybox:1.0"},
                        {"name": "c2", "image": "nginx:1.0"}
                    ]
                }
            }
        }
    });
    let key = build_key("deployments", Some(TEST_NS), name);
    mem.create(&key, &dep).await.expect("seed deployment");
    key
}

// ---------------------------------------------------------------------------
// POST /pods — request decoder dispatch
// ---------------------------------------------------------------------------

/// `application/json` on POST is the canonical create path. The body is a
/// valid Pod JSON document; the response must be a 2xx and the object must
/// land in storage.
#[tokio::test]
async fn test_content_type_application_json_create_succeeds() {
    let (mem, router) = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "json-pod", "namespace": TEST_NS},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });
    let (status, response_body) = send_with_ct(
        router,
        Method::POST,
        "/api/v1/namespaces/default/pods",
        "application/json",
        serde_json::to_vec(&body).unwrap(),
    )
    .await;

    assert!(
        (200..300).contains(&status),
        "application/json create should succeed; got {} body={}",
        status,
        response_body
    );

    let stored = read_stored(&mem, &build_key("pods", Some(TEST_NS), "json-pod")).await;
    assert_eq!(stored["metadata"]["name"], "json-pod");
}

/// `application/xml` on POST is unsupported. rusternetes' middleware
/// rewrites the Content-Type to `application/json` while leaving the body
/// untouched. With an XML body, the JSON decoder then fails. We assert
/// the observable contract: a 4xx status and no object persisted.
///
/// Upstream apiserver returns 415 Unsupported Media Type; rusternetes
/// returns 400 (InvalidResource) because the rewrite happens before the
/// handler can inspect the original Content-Type. The status-class check
/// pins the rejection regardless of which 4xx code is chosen.
#[tokio::test]
async fn test_content_type_xml_returns_4xx() {
    let (mem, router) = spawn_router();
    let xml_body = b"<Pod><name>xml-pod</name></Pod>".to_vec();
    let (status, response_body) = send_with_ct(
        router,
        Method::POST,
        "/api/v1/namespaces/default/pods",
        "application/xml",
        xml_body,
    )
    .await;

    assert!(
        (400..500).contains(&status),
        "application/xml POST should be rejected (4xx); got {} body={}",
        status,
        response_body
    );
    assert!(
        mem.get::<Value>(&build_key("pods", Some(TEST_NS), "xml-pod"))
            .await
            .is_err(),
        "XML body must not be persisted"
    );
}

/// `text/plain` on POST: same contract as XML — rejected, nothing stored.
#[tokio::test]
async fn test_content_type_text_plain_returns_4xx() {
    let (_mem, router) = spawn_router();
    let body = b"this is not a pod".to_vec();
    let (status, response_body) = send_with_ct(
        router,
        Method::POST,
        "/api/v1/namespaces/default/pods",
        "text/plain",
        body,
    )
    .await;

    assert!(
        (400..500).contains(&status),
        "text/plain POST should be rejected (4xx); got {} body={}",
        status,
        response_body
    );
}

/// `application/yaml` on POST. The current rusternetes middleware does
/// NOT translate YAML to JSON — it only rewrites the Content-Type header,
/// so a YAML body fails JSON decoding. The test accepts either outcome
/// (2xx + persisted, or 4xx + nothing stored) so it stays green if YAML
/// support lands later; the assertion that always holds is "decoder
/// dispatch is deterministic and consistent with what was persisted."
#[tokio::test]
async fn test_content_type_application_yaml_create() {
    let (mem, router) = spawn_router();
    let yaml_body = b"apiVersion: v1\nkind: Pod\nmetadata:\n  name: yaml-pod\n  namespace: default\nspec:\n  containers:\n    - name: c\n      image: busybox\n".to_vec();
    let (status, response_body) = send_with_ct(
        router,
        Method::POST,
        "/api/v1/namespaces/default/pods",
        "application/yaml",
        yaml_body,
    )
    .await;

    let stored = mem
        .get::<Value>(&build_key("pods", Some(TEST_NS), "yaml-pod"))
        .await;

    if (200..300).contains(&status) {
        let s = stored.expect("YAML accept path must persist the pod");
        assert_eq!(s["metadata"]["name"], "yaml-pod");
    } else {
        assert!(
            (400..500).contains(&status),
            "application/yaml POST should be either 2xx or 4xx; got {} body={}",
            status,
            response_body
        );
        assert!(
            stored.is_err(),
            "YAML reject path must NOT persist the pod; stored={:?}",
            stored
        );
    }
}

/// POST with no Content-Type header. The middleware treats this as
/// "not protobuf, not patch" and rewrites it to `application/json`. A
/// valid JSON body must therefore still succeed. This pins the actual
/// behavior — if a stricter middleware lands later that returns 415 on
/// missing Content-Type, flip the assertion.
#[tokio::test]
async fn test_content_type_missing_defaults_to_json() {
    let (mem, router) = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "no-ct-pod", "namespace": TEST_NS},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });
    let (status, response_body) = send_without_ct(
        router,
        Method::POST,
        "/api/v1/namespaces/default/pods",
        serde_json::to_vec(&body).unwrap(),
    )
    .await;

    assert!(
        (200..300).contains(&status),
        "POST with missing Content-Type and JSON body should succeed (middleware defaults to JSON); got {} body={}",
        status,
        response_body
    );
    let stored = read_stored(&mem, &build_key("pods", Some(TEST_NS), "no-ct-pod")).await;
    assert_eq!(stored["metadata"]["name"], "no-ct-pod");
}

// ---------------------------------------------------------------------------
// PATCH /pods/:name — patch-type dispatch
//
// All three patch types use the same JSON body shape here so that the only
// variable is the Content-Type. The body adds a new container `c3` and
// updates `c1`'s image, leaving `c2` untouched. The three patch decoders
// produce observably different results:
//
//   * strategic-merge-patch+json  → merges `containers` by `name`. Result
//                                    keeps `c1` (updated), `c2` (untouched),
//                                    `c3` (added) — 3 containers total.
//   * merge-patch+json (RFC 7396) → replaces the whole `containers` array.
//                                    Result has only the 2 containers from
//                                    the patch body (`c1` + `c3`).
//   * json-patch+json (RFC 6902)  → operates on the array via explicit
//                                    add/replace ops at specific indices.
// ---------------------------------------------------------------------------

/// `application/strategic-merge-patch+json` PATCH. Verifies SMP semantics:
/// arrays of maps with a `name` merge key get merged, not replaced.
#[tokio::test]
async fn test_content_type_strategic_merge_patch_merges_arrays_by_name() {
    let (mem, router) = spawn_router();
    seed_deployment(&mem, "smp-dep").await;

    // SMP body: same array shape as the original; merge happens by `name`.
    // Updates c1, adds c3, omits c2 (which should be preserved).
    let smp = json!({
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {"name": "c1", "image": "busybox:2.0"},
                        {"name": "c3", "image": "alpine:3.20"}
                    ]
                }
            }
        }
    });

    let (status, response_body) = send_with_ct(
        router,
        Method::PATCH,
        "/apis/apps/v1/namespaces/default/deployments/smp-dep",
        "application/strategic-merge-patch+json",
        serde_json::to_vec(&smp).unwrap(),
    )
    .await;

    assert!(
        (200..300).contains(&status),
        "strategic-merge-patch+json should succeed; got {} body={}",
        status,
        response_body
    );

    let stored = read_stored(&mem, &build_key("deployments", Some(TEST_NS), "smp-dep")).await;
    let containers = stored["spec"]["template"]["spec"]["containers"]
        .as_array()
        .expect("containers must be an array");
    assert_eq!(
        containers.len(),
        3,
        "SMP must merge containers by name (keeping c1, c2, c3); got {} containers: {:?}",
        containers.len(),
        containers
    );
    // c1 image updated.
    let c1 = containers
        .iter()
        .find(|c| c["name"] == "c1")
        .expect("c1 must remain");
    assert_eq!(c1["image"], "busybox:2.0", "c1 image should be patched");
    // c2 preserved.
    assert!(
        containers.iter().any(|c| c["name"] == "c2"),
        "SMP must preserve untouched container c2; containers={:?}",
        containers
    );
    // c3 added.
    assert!(
        containers.iter().any(|c| c["name"] == "c3"),
        "SMP must add new container c3; containers={:?}",
        containers
    );
}

/// `application/merge-patch+json` (RFC 7396). Replaces the whole array
/// (no merge-key semantics).
#[tokio::test]
async fn test_content_type_merge_patch_replaces_arrays() {
    let (mem, router) = spawn_router();
    seed_deployment(&mem, "mp-dep").await;

    let mp = json!({
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {"name": "c1", "image": "busybox:2.0"},
                        {"name": "c3", "image": "alpine:3.20"}
                    ]
                }
            }
        }
    });

    let (status, response_body) = send_with_ct(
        router,
        Method::PATCH,
        "/apis/apps/v1/namespaces/default/deployments/mp-dep",
        "application/merge-patch+json",
        serde_json::to_vec(&mp).unwrap(),
    )
    .await;

    assert!(
        (200..300).contains(&status),
        "merge-patch+json should succeed; got {} body={}",
        status,
        response_body
    );

    let stored = read_stored(&mem, &build_key("deployments", Some(TEST_NS), "mp-dep")).await;
    let containers = stored["spec"]["template"]["spec"]["containers"]
        .as_array()
        .expect("containers must be an array");
    assert_eq!(
        containers.len(),
        2,
        "RFC 7396 merge-patch must REPLACE the array (no merge-by-name); got {} containers: {:?}",
        containers.len(),
        containers
    );
    assert!(
        !containers.iter().any(|c| c["name"] == "c2"),
        "merge-patch must NOT preserve c2 (it was not in the patch body); containers={:?}",
        containers
    );
}

/// `application/json-patch+json` (RFC 6902). The body is an array of ops,
/// not an object; the JSON-patch decoder is the only one that accepts
/// this shape. We replace `c1`'s image and `add` a new container.
#[tokio::test]
async fn test_content_type_json_patch_applies_ops_array() {
    let (mem, router) = spawn_router();
    seed_deployment(&mem, "jp-dep").await;

    // RFC 6902 ops. `-` appends to an array.
    let jp = json!([
        {"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": "busybox:2.0"},
        {"op": "add", "path": "/spec/template/spec/containers/-", "value": {"name": "c3", "image": "alpine:3.20"}}
    ]);

    let (status, response_body) = send_with_ct(
        router,
        Method::PATCH,
        "/apis/apps/v1/namespaces/default/deployments/jp-dep",
        "application/json-patch+json",
        serde_json::to_vec(&jp).unwrap(),
    )
    .await;

    assert!(
        (200..300).contains(&status),
        "json-patch+json should succeed; got {} body={}",
        status,
        response_body
    );

    let stored = read_stored(&mem, &build_key("deployments", Some(TEST_NS), "jp-dep")).await;
    let containers = stored["spec"]["template"]["spec"]["containers"]
        .as_array()
        .expect("containers must be an array");
    assert_eq!(
        containers.len(),
        3,
        "json-patch add(-) must append; got {} containers: {:?}",
        containers.len(),
        containers
    );
    assert_eq!(
        containers[0]["image"], "busybox:2.0",
        "replace op must update containers[0].image"
    );
    assert_eq!(
        containers[1]["name"], "c2",
        "json-patch must preserve c2 (no op touched it)"
    );
    assert_eq!(
        containers[2]["name"], "c3",
        "json-patch add(-) must place new container at the end"
    );
}

/// `application/json-patch+json` body sent as a plain object (not an ops
/// array) is malformed for this decoder. The decoder must reject it
/// instead of silently falling back to merge-patch semantics. Pins that
/// the patch-type dispatcher actually parses the body shape the
/// Content-Type advertises.
#[tokio::test]
async fn test_content_type_json_patch_rejects_non_array_body() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "jp-bad-pod").await;

    // Object body — wrong shape for json-patch.
    let bad = json!({"spec": {"containers": []}});

    let (status, response_body) = send_with_ct(
        router,
        Method::PATCH,
        "/api/v1/namespaces/default/pods/jp-bad-pod",
        "application/json-patch+json",
        serde_json::to_vec(&bad).unwrap(),
    )
    .await;

    assert!(
        (400..500).contains(&status),
        "json-patch with object body must be rejected; got {} body={}",
        status,
        response_body
    );

    // Storage must be unchanged.
    let stored = read_stored(&mem, &build_key("pods", Some(TEST_NS), "jp-bad-pod")).await;
    let containers = stored["spec"]["containers"]
        .as_array()
        .expect("containers must be an array");
    assert_eq!(
        containers.len(),
        2,
        "rejected json-patch must not mutate storage"
    );
}

/// PATCH with an unsupported `application/xml` Content-Type must be
/// rejected by the patch-type dispatcher.
#[tokio::test]
async fn test_content_type_patch_unsupported_content_type_rejected() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "ct-bad-pod").await;

    let body = json!({"spec": {"containers": [{"name": "c1", "image": "busybox:2.0"}]}});
    let (status, response_body) = send_with_ct(
        router,
        Method::PATCH,
        "/api/v1/namespaces/default/pods/ct-bad-pod",
        "application/xml",
        serde_json::to_vec(&body).unwrap(),
    )
    .await;

    assert!(
        (400..500).contains(&status),
        "PATCH with unsupported Content-Type must be rejected; got {} body={}",
        status,
        response_body
    );

    // Storage must be unchanged.
    let stored = read_stored(&mem, &build_key("pods", Some(TEST_NS), "ct-bad-pod")).await;
    assert_eq!(
        container_image(&stored, "c1"),
        "busybox:1.0",
        "rejected PATCH must not mutate storage"
    );
}

// ---------------------------------------------------------------------------
// Server-side apply dispatch — `application/apply-patch+yaml` + ?fieldManager
//
// Upstream apiserver routes apply-patch+yaml to SSA only when
// `fieldManager` is present. Without `fieldManager`, the request is
// rejected. This pins both branches via the in-process router.
// ---------------------------------------------------------------------------

/// `application/apply-patch+yaml` with `?fieldManager=<name>` routes to the
/// server-side apply code path. The body here is the JSON encoding of an
/// apply document (apiserver accepts JSON-flavored YAML); rusternetes'
/// SSA implementation reads it as JSON.
///
/// Success criterion: the PATCH returns 2xx and a `managedFields` entry
/// for our field manager appears on the stored object — the unambiguous
/// signal that the SSA path was taken (the three regular patch types
/// never write `managedFields`).
#[tokio::test]
async fn test_content_type_apply_patch_yaml_routes_to_ssa() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "ssa-pod").await;

    // Apply document — full intent of this field manager. JSON encoding is
    // valid YAML, which is what SSA accepts on the wire.
    let apply_doc = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "ssa-pod", "namespace": TEST_NS},
        "spec": {
            "containers": [
                {"name": "c1", "image": "busybox:3.0"}
            ]
        }
    });

    let (status, response_body) = send_with_ct(
        router,
        Method::PATCH,
        "/api/v1/namespaces/default/pods/ssa-pod?fieldManager=test-mgr",
        "application/apply-patch+yaml",
        serde_json::to_vec(&apply_doc).unwrap(),
    )
    .await;

    assert!(
        (200..300).contains(&status),
        "apply-patch+yaml with fieldManager should succeed (SSA); got {} body={}",
        status,
        response_body
    );

    let stored = read_stored(&mem, &build_key("pods", Some(TEST_NS), "ssa-pod")).await;
    let mf = stored["metadata"]["managedFields"].as_array();
    assert!(
        mf.is_some() && !mf.unwrap().is_empty(),
        "SSA path must populate metadata.managedFields; got stored={}",
        stored
    );
    let mgr_seen = mf
        .unwrap()
        .iter()
        .any(|entry| entry["manager"] == "test-mgr");
    assert!(
        mgr_seen,
        "managedFields must include our manager 'test-mgr'; got {:?}",
        mf
    );
}

/// `application/apply-patch+yaml` WITHOUT `?fieldManager`. Upstream
/// requires fieldManager for apply requests. rusternetes' pod handler
/// falls through to the regular patch dispatcher, which then fails to
/// map `apply-patch+yaml` onto a `PatchType` and returns a 4xx.
#[tokio::test]
async fn test_content_type_apply_patch_yaml_without_field_manager_rejected() {
    let (mem, router) = spawn_router();
    seed_pod(&mem, "ssa-nofm-pod").await;

    let apply_doc = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "ssa-nofm-pod", "namespace": TEST_NS},
        "spec": {"containers": [{"name": "c1", "image": "busybox:3.0"}]}
    });

    let (status, response_body) = send_with_ct(
        router,
        Method::PATCH,
        "/api/v1/namespaces/default/pods/ssa-nofm-pod",
        "application/apply-patch+yaml",
        serde_json::to_vec(&apply_doc).unwrap(),
    )
    .await;

    assert!(
        (400..500).contains(&status),
        "apply-patch+yaml WITHOUT fieldManager must be rejected; got {} body={}",
        status,
        response_body
    );

    // Storage must be unchanged — c1 still on its original image.
    let stored = read_stored(&mem, &build_key("pods", Some(TEST_NS), "ssa-nofm-pod")).await;
    assert_eq!(
        container_image(&stored, "c1"),
        "busybox:1.0",
        "rejected apply-patch must not mutate storage"
    );
}
