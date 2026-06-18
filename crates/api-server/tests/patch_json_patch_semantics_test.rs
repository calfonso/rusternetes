//! JSON Patch (RFC 6902) operational semantics tests.
//!
//! Sends real `PATCH /api/v1/.../pods/<name>` requests with
//! `Content-Type: application/json-patch+json` through the in-process Axum
//! router, then inspects the result (response status + the object stored in
//! the backing `MemoryStorage`) to verify the dispatch in
//! `crates/api-server/src/patch.rs::apply_json_patch` implements RFC 6902
//! correctly.
//!
//! Coverage per RFC 6902 operation:
//!
//! - `add`: adds new field; `add` to an existing key REPLACES (per RFC §4.1);
//!   `add` to a list at `-` appends, at numeric index inserts.
//! - `remove`: removes existing field/element; removing a non-existent path
//!   is an error (4xx, storage unchanged).
//! - `replace`: replaces an existing field; replacing a non-existent path is
//!   an error (4xx); the path MUST exist (§4.3).
//! - `move`: moves; the `from` path must exist; moving into a descendant of
//!   `from` is an error (§4.4).
//! - `copy`: copies; the `from` path must exist (§4.5).
//! - `test`: succeeds when value equals; fails the entire patch with 4xx
//!   when the value differs (§4.6).
//!
//! Plus:
//!
//! - Path escaping per RFC 6901: `~1` → `/`, `~0` → `~`.
//! - Syntax errors: leading `/` mandatory; `/-` is "end of array".
//! - Atomicity: if ANY op fails, the entire patch is rejected and storage
//!   is unchanged.
//!
//! Pattern follows `decoder_content_type_test.rs` — helpers inline, no
//! shared `tests/common` mod, all assertions ride on the public HTTP surface.

use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin wrappers over the shared `TestApiServer`. `mem` is the
// backing store so tests seed pods and assert stored bytes directly.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "default";
const JSON_PATCH_CT: &str = "application/json-patch+json";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// Seed a Pod into memory storage and return its registry key. Carries an
/// annotation map + two containers so a wide variety of paths are
/// addressable from JSON Patch ops.
async fn seed_pod(mem: &Arc<MemoryStorage>, name: &str) -> String {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": TEST_NS,
            "labels": {"app": "demo"},
            "annotations": {
                "existing": "before",
            }
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

/// Send a JSON-Patch PATCH request and return (status, response_body).
async fn apply_patch(router: TestApiServer, pod_name: &str, ops: &Value) -> (u16, Value) {
    let uri = format!("/api/v1/namespaces/{TEST_NS}/pods/{pod_name}");
    let (status, value) = router
        .send("PATCH", &uri, Some(JSON_PATCH_CT), Some(ops))
        .await;
    (status.as_u16(), value)
}

/// Read the JSON stored at `key`. Panics if the key is absent.
async fn read_stored(mem: &Arc<MemoryStorage>, key: &str) -> Value {
    mem.get::<Value>(key)
        .await
        .unwrap_or_else(|e| panic!("expected key {} to exist: {:?}", key, e))
}

// ---------------------------------------------------------------------------
// add
// ---------------------------------------------------------------------------

/// `add` to a new field creates it.
#[tokio::test]
async fn test_json_patch_add_new_field() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "add-new").await;

    let ops = json!([
        {"op": "add", "path": "/metadata/annotations/newkey", "value": "added"}
    ]);
    let (status, body) = apply_patch(router, "add-new", &ops).await;
    assert!(
        (200..300).contains(&status),
        "add new field should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["newkey"], "added",
        "annotation must be added; stored={}",
        stored
    );
    assert_eq!(
        stored["metadata"]["annotations"]["existing"], "before",
        "existing annotation must be preserved"
    );
}

/// `add` on an existing key REPLACES per RFC 6902 §4.1
/// ("If the target location specifies an object member that does exist,
/// that member's value is replaced.").
#[tokio::test]
async fn test_json_patch_add_existing_key_replaces() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "add-replace").await;

    let ops = json!([
        {"op": "add", "path": "/metadata/annotations/existing", "value": "after"}
    ]);
    let (status, body) = apply_patch(router, "add-replace", &ops).await;
    assert!(
        (200..300).contains(&status),
        "add over existing key should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["existing"], "after",
        "existing key must be replaced (RFC 6902 §4.1)"
    );
}

/// `add` to an array at `-` appends.
#[tokio::test]
async fn test_json_patch_add_array_dash_appends() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "add-append").await;

    let ops = json!([
        {"op": "add", "path": "/spec/containers/-",
         "value": {"name": "c3", "image": "alpine:3.20"}}
    ]);
    let (status, body) = apply_patch(router, "add-append", &ops).await;
    assert!(
        (200..300).contains(&status),
        "add with '-' should append; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    let containers = stored["spec"]["containers"]
        .as_array()
        .expect("containers array");
    assert_eq!(containers.len(), 3, "array must grow to 3");
    assert_eq!(
        containers[2]["name"], "c3",
        "new element must be appended at the end"
    );
}

/// `add` to an array at a numeric index inserts (shifts existing elements
/// rightward).
#[tokio::test]
async fn test_json_patch_add_array_index_inserts() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "add-insert").await;

    let ops = json!([
        {"op": "add", "path": "/spec/containers/0",
         "value": {"name": "c0", "image": "scratch:0"}}
    ]);
    let (status, body) = apply_patch(router, "add-insert", &ops).await;
    assert!(
        (200..300).contains(&status),
        "add at index should insert; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    let containers = stored["spec"]["containers"]
        .as_array()
        .expect("containers array");
    assert_eq!(containers.len(), 3, "array length grows by one");
    assert_eq!(
        containers[0]["name"], "c0",
        "inserted element must be at index 0"
    );
    assert_eq!(containers[1]["name"], "c1", "c1 must shift to index 1");
    assert_eq!(containers[2]["name"], "c2", "c2 must shift to index 2");
}

// ---------------------------------------------------------------------------
// remove
// ---------------------------------------------------------------------------

/// `remove` of an existing field deletes it.
#[tokio::test]
async fn test_json_patch_remove_existing_field() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "rm-existing").await;

    let ops = json!([
        {"op": "remove", "path": "/metadata/annotations/existing"}
    ]);
    let (status, body) = apply_patch(router, "rm-existing", &ops).await;
    assert!(
        (200..300).contains(&status),
        "remove existing field should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert!(
        stored["metadata"]["annotations"].get("existing").is_none(),
        "removed key must be absent; stored={}",
        stored
    );
}

/// `remove` of an existing array element shifts later elements leftward.
#[tokio::test]
async fn test_json_patch_remove_array_element() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "rm-array").await;

    let ops = json!([
        {"op": "remove", "path": "/spec/containers/0"}
    ]);
    let (status, body) = apply_patch(router, "rm-array", &ops).await;
    assert!(
        (200..300).contains(&status),
        "remove array element should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    let containers = stored["spec"]["containers"]
        .as_array()
        .expect("containers array");
    assert_eq!(containers.len(), 1, "one container removed");
    assert_eq!(containers[0]["name"], "c2", "c2 must shift down to index 0");
}

/// `remove` of a non-existent path must fail. RFC 6902 §4.2 says the
/// target location MUST exist. Upstream apiserver returns 422 Invalid;
/// we accept any 4xx and additionally pin that storage is unchanged
/// (atomicity).
#[tokio::test]
async fn test_json_patch_remove_nonexistent_path_fails() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "rm-missing").await;

    let ops = json!([
        {"op": "remove", "path": "/metadata/annotations/does-not-exist"}
    ]);
    let (status, body) = apply_patch(router, "rm-missing", &ops).await;
    assert!(
        (400..500).contains(&status),
        "remove of non-existent path must be a 4xx; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["existing"], "before",
        "storage must be unchanged on failed remove"
    );
}

// ---------------------------------------------------------------------------
// replace
// ---------------------------------------------------------------------------

/// `replace` on an existing field updates the value.
#[tokio::test]
async fn test_json_patch_replace_existing_field() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "replace-ok").await;

    let ops = json!([
        {"op": "replace", "path": "/metadata/annotations/existing", "value": "after"}
    ]);
    let (status, body) = apply_patch(router, "replace-ok", &ops).await;
    assert!(
        (200..300).contains(&status),
        "replace existing field should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(stored["metadata"]["annotations"]["existing"], "after");
}

/// `replace` on a non-existent path must fail. RFC 6902 §4.3 requires the
/// target to exist. This distinguishes `replace` from `add` (which can
/// create).
#[tokio::test]
async fn test_json_patch_replace_nonexistent_path_fails() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "replace-missing").await;

    let ops = json!([
        {"op": "replace", "path": "/metadata/annotations/no-such-key", "value": "x"}
    ]);
    let (status, body) = apply_patch(router, "replace-missing", &ops).await;
    assert!(
        (400..500).contains(&status),
        "replace on non-existent path must be a 4xx (RFC 6902 §4.3); got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert!(
        stored["metadata"]["annotations"]
            .get("no-such-key")
            .is_none(),
        "failed replace must not insert the key"
    );
}

// ---------------------------------------------------------------------------
// move
// ---------------------------------------------------------------------------

/// `move` relocates a value from `from` to `path`.
#[tokio::test]
async fn test_json_patch_move_existing_field() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "mv-ok").await;

    let ops = json!([
        {"op": "move",
         "from": "/metadata/annotations/existing",
         "path": "/metadata/annotations/relocated"}
    ]);
    let (status, body) = apply_patch(router, "mv-ok", &ops).await;
    assert!(
        (200..300).contains(&status),
        "move should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["relocated"], "before",
        "value must land at new path"
    );
    assert!(
        stored["metadata"]["annotations"].get("existing").is_none(),
        "value must be removed from old path"
    );
}

/// `move` whose `from` path does not exist must fail (RFC 6902 §4.4).
#[tokio::test]
async fn test_json_patch_move_nonexistent_from_fails() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "mv-missing").await;

    let ops = json!([
        {"op": "move",
         "from": "/metadata/annotations/no-such",
         "path": "/metadata/annotations/target"}
    ]);
    let (status, body) = apply_patch(router, "mv-missing", &ops).await;
    assert!(
        (400..500).contains(&status),
        "move with non-existent 'from' must be a 4xx; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert!(
        stored["metadata"]["annotations"].get("target").is_none(),
        "failed move must not create the target"
    );
}

/// `move` into a descendant of `from` is forbidden by RFC 6902 §4.4
/// ("The 'from' location MUST NOT be a proper prefix of the 'path'
/// location"). Concrete case: move /spec/containers into
/// /spec/containers/0/relocated. Pinned as `#[ignore]` because the
/// current implementation in `crates/api-server/src/patch.rs` does not
/// detect this case (it just performs remove + add, and the post-remove
/// add fails for a different reason).
#[tokio::test]
async fn test_json_patch_move_into_descendant_fails() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "mv-descendant").await;

    let ops = json!([
        {"op": "move",
         "from": "/spec/containers",
         "path": "/spec/containers/0/relocated"}
    ]);
    let (status, body) = apply_patch(router, "mv-descendant", &ops).await;
    assert!(
        (400..500).contains(&status),
        "move into descendant of from must be a 4xx (RFC 6902 §4.4); got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    let containers = stored["spec"]["containers"]
        .as_array()
        .expect("containers must still be an array after failed move");
    assert_eq!(containers.len(), 2, "containers must be unchanged");
}

// ---------------------------------------------------------------------------
// copy
// ---------------------------------------------------------------------------

/// `copy` duplicates a value, leaving the source intact.
#[tokio::test]
async fn test_json_patch_copy_existing_field() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "cp-ok").await;

    let ops = json!([
        {"op": "copy",
         "from": "/metadata/annotations/existing",
         "path": "/metadata/annotations/dup"}
    ]);
    let (status, body) = apply_patch(router, "cp-ok", &ops).await;
    assert!(
        (200..300).contains(&status),
        "copy should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["existing"], "before",
        "source must remain after copy"
    );
    assert_eq!(
        stored["metadata"]["annotations"]["dup"], "before",
        "value must be duplicated at target"
    );
}

/// `copy` whose `from` path does not exist must fail (RFC 6902 §4.5).
#[tokio::test]
async fn test_json_patch_copy_nonexistent_from_fails() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "cp-missing").await;

    let ops = json!([
        {"op": "copy",
         "from": "/metadata/annotations/no-such",
         "path": "/metadata/annotations/target"}
    ]);
    let (status, body) = apply_patch(router, "cp-missing", &ops).await;
    assert!(
        (400..500).contains(&status),
        "copy with non-existent 'from' must be a 4xx; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert!(
        stored["metadata"]["annotations"].get("target").is_none(),
        "failed copy must not create the target"
    );
}

// ---------------------------------------------------------------------------
// test
// ---------------------------------------------------------------------------

/// `test` succeeds silently when value at path equals provided value.
/// The patch as a whole returns 2xx; the stored object reflects any other
/// ops that ran. Here we pair `test` with a `replace` so we can confirm
/// "test passed → replace applied".
#[tokio::test]
async fn test_json_patch_test_equal_proceeds() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "test-eq").await;

    let ops = json!([
        {"op": "test",
         "path": "/metadata/annotations/existing",
         "value": "before"},
        {"op": "replace",
         "path": "/metadata/annotations/existing",
         "value": "after"}
    ]);
    let (status, body) = apply_patch(router, "test-eq", &ops).await;
    assert!(
        (200..300).contains(&status),
        "test op with matching value should let the patch proceed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["existing"], "after",
        "subsequent replace must apply when test passes"
    );
}

/// `test` mismatch fails the ENTIRE patch (atomicity). The replace must
/// NOT apply.
#[tokio::test]
async fn test_json_patch_test_unequal_fails_entire_patch() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "test-neq").await;

    let ops = json!([
        {"op": "test",
         "path": "/metadata/annotations/existing",
         "value": "wrong-expected-value"},
        {"op": "replace",
         "path": "/metadata/annotations/existing",
         "value": "after"}
    ]);
    let (status, body) = apply_patch(router, "test-neq", &ops).await;
    assert!(
        (400..500).contains(&status),
        "test op with mismatched value must fail the patch (4xx); got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["existing"], "before",
        "failed test must prevent the subsequent replace (atomicity)"
    );
}

// ---------------------------------------------------------------------------
// Path escaping (RFC 6901 §3 — `~0` → `~`, `~1` → `/`)
// ---------------------------------------------------------------------------

/// Annotation key containing `/` is addressed via `~1`. Common in
/// Kubernetes (e.g. `kubernetes.io/foo`-style annotations).
#[tokio::test]
async fn test_json_patch_path_escape_slash() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "esc-slash").await;

    // Add an annotation whose key is literally `foo/bar` via `~1` escape.
    let ops = json!([
        {"op": "add",
         "path": "/metadata/annotations/foo~1bar",
         "value": "escaped-slash"}
    ]);
    let (status, body) = apply_patch(router, "esc-slash", &ops).await;
    assert!(
        (200..300).contains(&status),
        "add with ~1-escaped path should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["foo/bar"], "escaped-slash",
        "~1 must decode to '/'; stored={}",
        stored
    );
}

/// Annotation key containing `~` is addressed via `~0`.
#[tokio::test]
async fn test_json_patch_path_escape_tilde() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "esc-tilde").await;

    // Add an annotation whose key is literally `foo~bar` via `~0`.
    let ops = json!([
        {"op": "add",
         "path": "/metadata/annotations/foo~0bar",
         "value": "escaped-tilde"}
    ]);
    let (status, body) = apply_patch(router, "esc-tilde", &ops).await;
    assert!(
        (200..300).contains(&status),
        "add with ~0-escaped path should succeed; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert_eq!(
        stored["metadata"]["annotations"]["foo~bar"], "escaped-tilde",
        "~0 must decode to '~'; stored={}",
        stored
    );
}

// ---------------------------------------------------------------------------
// Path syntax errors (RFC 6901 §3)
// ---------------------------------------------------------------------------

/// Path missing the mandatory leading slash must be rejected.
#[tokio::test]
async fn test_json_patch_path_missing_leading_slash_fails() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "path-no-slash").await;

    let ops = json!([
        {"op": "add", "path": "metadata/annotations/x", "value": "v"}
    ]);
    let (status, body) = apply_patch(router, "path-no-slash", &ops).await;
    assert!(
        (400..500).contains(&status),
        "path without leading '/' must be rejected; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert!(
        stored["metadata"]["annotations"].get("x").is_none(),
        "rejected patch must not mutate storage"
    );
}

/// `/-` references the (nonexistent) element just past the end of an
/// array, which is valid for `add` (append). Pinned here as an
/// orthogonal-shape check next to the missing-slash case.
#[tokio::test]
async fn test_json_patch_path_dash_means_end_of_array() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "path-dash").await;

    let ops = json!([
        {"op": "add", "path": "/spec/containers/-",
         "value": {"name": "tail", "image": "scratch"}}
    ]);
    let (status, body) = apply_patch(router, "path-dash", &ops).await;
    assert!(
        (200..300).contains(&status),
        "'-' must mean end-of-array for add; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    let containers = stored["spec"]["containers"]
        .as_array()
        .expect("containers array");
    assert_eq!(containers.len(), 3, "appended one container");
    assert_eq!(
        containers[2]["name"], "tail",
        "appended container at end of array"
    );
}

// ---------------------------------------------------------------------------
// Atomicity — RFC 6902 §5 ("Operations are applied sequentially in the
// order they appear in the array. ... If a normative requirement is
// violated by a JSON Patch document, ... the entire patch document
// SHALL be considered to be in error.")
// ---------------------------------------------------------------------------

/// If a later op fails, the whole patch is rejected. Earlier ops must
/// NOT be observable in storage.
#[tokio::test]
async fn test_json_patch_atomicity_failed_op_rolls_back() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "atomic").await;

    // First op succeeds (would add annotation `step1`); second op is a
    // `remove` on a non-existent path which RFC 6902 requires to fail.
    let ops = json!([
        {"op": "add",
         "path": "/metadata/annotations/step1",
         "value": "should-not-persist"},
        {"op": "remove",
         "path": "/metadata/annotations/does-not-exist"}
    ]);
    let (status, body) = apply_patch(router, "atomic", &ops).await;
    assert!(
        (400..500).contains(&status),
        "patch with a failing op must be a 4xx; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert!(
        stored["metadata"]["annotations"].get("step1").is_none(),
        "earlier successful op must not be observable when a later op fails (RFC 6902 §5); stored={}",
        stored
    );
    assert_eq!(
        stored["metadata"]["annotations"]["existing"], "before",
        "original annotation must still be present"
    );
    let containers = stored["spec"]["containers"]
        .as_array()
        .expect("containers array unchanged");
    assert_eq!(containers.len(), 2, "containers must be unchanged");
}

/// Atomicity via a failing `test` op in the middle: earlier `add` must
/// not stick, and later `replace` must not run.
#[tokio::test]
async fn test_json_patch_atomicity_test_failure_in_middle() {
    let (mem, router) = spawn_router();
    let key = seed_pod(&mem, "atomic-test").await;

    let ops = json!([
        {"op": "add",
         "path": "/metadata/annotations/added",
         "value": "first"},
        {"op": "test",
         "path": "/metadata/annotations/existing",
         "value": "WRONG"},
        {"op": "replace",
         "path": "/metadata/annotations/existing",
         "value": "third"}
    ]);
    let (status, body) = apply_patch(router, "atomic-test", &ops).await;
    assert!(
        (400..500).contains(&status),
        "patch with a failing 'test' op must be 4xx; got {} body={}",
        status,
        body
    );

    let stored = read_stored(&mem, &key).await;
    assert!(
        stored["metadata"]["annotations"].get("added").is_none(),
        "earlier add must not persist when later test fails"
    );
    assert_eq!(
        stored["metadata"]["annotations"]["existing"], "before",
        "later replace must not run; original value preserved"
    );
}
