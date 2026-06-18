//! Mirror of upstream Kubernetes v1.35 conformance test
//! `[sig-node] Pods Extended (pod generation) Pod Generation pod generation
//! should start at 1 and increment per update [MinimumKubeletVersion:1.34]
//! [Conformance]`.
//!
//! Source (release-1.35):
//!   https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/node/pods.go#L437
//!
//! Closes rusternetes#483.
//!
//! The conformance test drives a sequence of mutations against a single Pod
//! and asserts that `metadata.generation` is bumped for genuine spec changes
//! and *only* for genuine spec changes. Crucially, the very first sub-test is
//! the "empty update": GET the pod, re-marshal it through `corev1.Pod`,
//! PUT it back unchanged, and expect `generation` to stay at 1.
//!
//! That sub-test was failing on rusternetes because Go's typed marshaller
//! emits `"resources":{}` on every container even when no resources are
//! defined (Go's `omitempty` does not detect zero-valued struct values), and
//! our `ValidatePodUpdate` immutability fence compared the resulting JSON
//! with a strict, structural equality. The fix lives in
//! `crates/common/src/validation/pod.rs::strip_empty_objects` which mirrors
//! upstream's `apiequality.Semantic.DeepEqual` semantics by normalising
//! empty `{}` objects to absent on both sides of the comparison before the
//! diff.
//!
//! The harness reuses the in-process Axum router pattern from
//! `strategy_pod_test.rs` — duplicated inline per the per-file contract.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const TEST_NS: &str = "pods-gen";

// Harness: thin `(u16, Value)` shims over `TestApiServer` (rusternetes-test-support),
// preserving this file's `*_json(&router, …)` call sites.
async fn post_json(api: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = api.post(uri, body).await;
    (status.as_u16(), value)
}

async fn put_json(api: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = api.put(uri, body).await;
    (status.as_u16(), value)
}

async fn get_json(api: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = api.get(uri).await;
    (status.as_u16(), value)
}

async fn create_namespace(router: &TestApiServer) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": TEST_NS },
    });
    let (status, _) = post_json(router, "/api/v1/namespaces", &body).await;
    assert!(status == 201 || status == 200);
}

/// Mirror of upstream `e2epod.NewAgnhostPod` with an init container — the
/// exact shape the conformance test seeds.
fn agnhost_pod_with_init(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "initContainers": [{"name": "init-container", "image": "busybox:1.36"}],
            "containers": [{"name": "agnhost-container", "image": "nginx:1.27"}]
        }
    })
}

/// Strip the fields a Go client would not echo back on a PUT and produce a
/// body that mimics `corev1.Pod` re-marshalling: defaults `resources` to
/// `{}` on every container, which is exactly the failure shape this test
/// is pinning down.
fn add_go_defaults(mut pod: Value) -> Value {
    if let Some(spec) = pod.get_mut("spec").and_then(|s| s.as_object_mut()) {
        if let Some(cs) = spec.get_mut("containers").and_then(|c| c.as_array_mut()) {
            for c in cs.iter_mut() {
                c.as_object_mut()
                    .unwrap()
                    .entry("resources".to_string())
                    .or_insert(json!({}));
            }
        }
        if let Some(ics) = spec
            .get_mut("initContainers")
            .and_then(|c| c.as_array_mut())
        {
            for c in ics.iter_mut() {
                c.as_object_mut()
                    .unwrap()
                    .entry("resources".to_string())
                    .or_insert(json!({}));
            }
        }
    }
    pod
}

// ===========================================================================
// Section 1 — the empty-update generation contract.
// ===========================================================================

/// Mirror of upstream's "empty update" sub-test (test/e2e/node/pods.go:462).
///
/// GET the pod, re-emit the body unchanged (with Go's typical
/// `"resources":{}` defaults), PUT it back, and assert that
/// `metadata.generation` stays at 1.
///
/// Regression coverage for closes #483.
#[tokio::test]
async fn test_pod_generation_empty_update_does_not_bump() {
    let router = TestApiServer::new();
    create_namespace(&router).await;

    // Create.
    let body = agnhost_pod_with_init("gen-empty-update");
    let create_uri = format!("/api/v1/namespaces/{}/pods", TEST_NS);
    let (status, resp) = post_json(&router, &create_uri, &body).await;
    assert!(
        status == 201 || status == 200,
        "create failed: {} {}",
        status,
        resp
    );
    assert_eq!(
        resp["metadata"]["generation"].as_i64(),
        Some(1),
        "create must yield generation=1"
    );

    // GET → re-marshal (mimic Go) → PUT.
    let get_uri = format!("/api/v1/namespaces/{}/pods/gen-empty-update", TEST_NS);
    let (status, fetched) = get_json(&router, &get_uri).await;
    assert_eq!(status, 200);
    let put_body = add_go_defaults(fetched);
    let (status, updated) = put_json(&router, &get_uri, &put_body).await;
    assert!(
        status == 200 || status == 201,
        "empty update must succeed: status={} body={}",
        status,
        updated
    );
    assert_eq!(
        updated["metadata"]["generation"].as_i64(),
        Some(1),
        "empty update must NOT bump generation: {}",
        updated
    );
}

/// Mirror of the "updating container image to trigger generation bump"
/// sub-test (test/e2e/node/pods.go:489): swap `containers[0].image` and
/// expect `metadata.generation` to advance to 2.
#[tokio::test]
async fn test_pod_generation_image_swap_bumps_to_2() {
    let router = TestApiServer::new();
    create_namespace(&router).await;

    let body = agnhost_pod_with_init("gen-image-swap");
    let create_uri = format!("/api/v1/namespaces/{}/pods", TEST_NS);
    let (_, resp) = post_json(&router, &create_uri, &body).await;
    assert_eq!(resp["metadata"]["generation"].as_i64(), Some(1));

    let get_uri = format!("/api/v1/namespaces/{}/pods/gen-image-swap", TEST_NS);
    let (_, fetched) = get_json(&router, &get_uri).await;
    let mut put_body = add_go_defaults(fetched);
    put_body["spec"]["containers"][0]["image"] = json!("nginx:1.28");

    let (status, updated) = put_json(&router, &get_uri, &put_body).await;
    assert!(
        status == 200 || status == 201,
        "image swap must succeed: status={} body={}",
        status,
        updated
    );
    assert_eq!(
        updated["metadata"]["generation"].as_i64(),
        Some(2),
        "image swap must bump generation to 2: {}",
        updated
    );
}

/// Mirror of "updating ActiveDeadlineSeconds to trigger generation bump":
/// flipping `spec.activeDeadlineSeconds` bumps generation.
#[tokio::test]
async fn test_pod_generation_active_deadline_bumps() {
    let router = TestApiServer::new();
    create_namespace(&router).await;

    let body = agnhost_pod_with_init("gen-ads");
    let create_uri = format!("/api/v1/namespaces/{}/pods", TEST_NS);
    let (_, resp) = post_json(&router, &create_uri, &body).await;
    assert_eq!(resp["metadata"]["generation"].as_i64(), Some(1));

    let get_uri = format!("/api/v1/namespaces/{}/pods/gen-ads", TEST_NS);
    let (_, fetched) = get_json(&router, &get_uri).await;
    let mut put_body = add_go_defaults(fetched);
    put_body["spec"]["activeDeadlineSeconds"] = json!(5000);

    let (status, updated) = put_json(&router, &get_uri, &put_body).await;
    assert!(
        status == 200 || status == 201,
        "activeDeadlineSeconds set must succeed: status={} body={}",
        status,
        updated
    );
    assert_eq!(
        updated["metadata"]["generation"].as_i64(),
        Some(2),
        "activeDeadlineSeconds set must bump generation: {}",
        updated
    );
}

/// Mirror of "updates to pod metadata should not trigger generation bump":
/// flipping `metadata.annotations` leaves generation untouched.
#[tokio::test]
async fn test_pod_generation_metadata_update_does_not_bump() {
    let router = TestApiServer::new();
    create_namespace(&router).await;

    let body = agnhost_pod_with_init("gen-meta");
    let create_uri = format!("/api/v1/namespaces/{}/pods", TEST_NS);
    let (_, resp) = post_json(&router, &create_uri, &body).await;
    assert_eq!(resp["metadata"]["generation"].as_i64(), Some(1));

    let get_uri = format!("/api/v1/namespaces/{}/pods/gen-meta", TEST_NS);
    let (_, fetched) = get_json(&router, &get_uri).await;
    let mut put_body = add_go_defaults(fetched);
    put_body["metadata"]["annotations"] = json!({"key": "value"});

    let (status, updated) = put_json(&router, &get_uri, &put_body).await;
    assert!(
        status == 200 || status == 201,
        "annotation set must succeed: status={} body={}",
        status,
        updated
    );
    assert_eq!(
        updated["metadata"]["generation"].as_i64(),
        Some(1),
        "annotation set must NOT bump generation: {}",
        updated
    );
}

/// Mirror of "pod generation updated by client should be ignored": the
/// server's stored generation is authoritative; client-set values do not
/// stick. (Setting `metadata.generation` on an otherwise no-op update is
/// effectively a metadata-only update — the spec is unchanged, so the
/// server must keep generation at 1.)
#[tokio::test]
async fn test_pod_generation_client_set_value_is_ignored() {
    let router = TestApiServer::new();
    create_namespace(&router).await;

    let body = agnhost_pod_with_init("gen-clientset");
    let create_uri = format!("/api/v1/namespaces/{}/pods", TEST_NS);
    let (_, resp) = post_json(&router, &create_uri, &body).await;
    assert_eq!(resp["metadata"]["generation"].as_i64(), Some(1));

    let get_uri = format!("/api/v1/namespaces/{}/pods/gen-clientset", TEST_NS);
    let (_, fetched) = get_json(&router, &get_uri).await;
    let mut put_body = add_go_defaults(fetched);
    // Client tries to set generation to 100 — must be ignored.
    put_body["metadata"]["generation"] = json!(100);

    let (status, updated) = put_json(&router, &get_uri, &put_body).await;
    assert!(
        status == 200 || status == 201,
        "client-set generation must not error: status={} body={}",
        status,
        updated
    );
    assert_eq!(
        updated["metadata"]["generation"].as_i64(),
        Some(1),
        "client-set generation must be ignored: {}",
        updated
    );
}
