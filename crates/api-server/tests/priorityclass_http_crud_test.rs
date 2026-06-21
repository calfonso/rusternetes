//! Router-driven HTTP CRUD coverage for the cluster-scoped PriorityClass API
//! (`scheduling.k8s.io/v1`). Exercises POST / GET / LIST / PUT / PATCH /
//! DELETE in-process against the real api-server routes (handlers in
//! `crates/api-server/src/handlers/priorityclass.rs`, routes in
//! `crates/api-server/src/router.rs`).
//!
//! This is the api-server home for the conformance case
//! "PriorityClass endpoints can be operated with different HTTP methods"
//! (upstream: k8s.io/kubernetes/test/e2e/scheduling/priorities.go). The
//! scheduler-crate stub `priority_class_endpoints_http_methods` that used to
//! `unimplemented!()` for lack of an HTTP harness was removed in favour of
//! this file.
//!
//! Harness mirrors `list_empty_items_router_test.rs`:
//!   * `Arc<MemoryStorage>` backend.
//!   * `AlwaysAllowAuthorizer` + `skip_auth=true` so no bearer token is needed.
//!   * `tower::ServiceExt::oneshot` per request.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PC_COLLECTION: &str = "/apis/scheduling.k8s.io/v1/priorityclasses";

// ---------------------------------------------------------------------------
// HTTP harness (inline, matches the in-process pattern used elsewhere).
// ---------------------------------------------------------------------------

// Harness: thin delegates over `TestApiServer` (rusternetes-test-support),
// preserving this file's `*_json(&state, …)` call sites. `patch_json` keeps its
// explicit content-type arg (the merge- vs strategic-merge cases below).
async fn post_json(state: &TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    state.post(uri, body).await
}

async fn get_json(state: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    state.get(uri).await
}

async fn put_json(state: &TestApiServer, uri: &str, body: &Value) -> (StatusCode, Value) {
    state.put(uri, body).await
}

async fn patch_json(
    state: &TestApiServer,
    uri: &str,
    content_type: &str,
    body: &Value,
) -> (StatusCode, Value) {
    state
        .send("PATCH", uri, Some(content_type), Some(body))
        .await
}

async fn delete_json(state: &TestApiServer, uri: &str) -> (StatusCode, Value) {
    state.delete(uri).await
}

fn sample_priority_class(name: &str, value: i64) -> Value {
    json!({
        "apiVersion": "scheduling.k8s.io/v1",
        "kind": "PriorityClass",
        "metadata": { "name": name },
        "value": value,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full happy-path CRUD lifecycle through every HTTP verb the routes expose:
/// POST (create) → GET → LIST → PUT (update) → PATCH → DELETE.
#[tokio::test]
async fn priority_class_full_http_crud_lifecycle() {
    let state = TestApiServer::new();
    let name = "high-priority";
    let item_uri = format!("{PC_COLLECTION}/{name}");

    // POST — create returns 201 Created with the persisted object.
    let (status, body) = post_json(&state, PC_COLLECTION, &sample_priority_class(name, 1000)).await;
    assert_eq!(status, StatusCode::CREATED, "POST create body: {body}");
    assert_eq!(body["kind"], "PriorityClass");
    assert_eq!(body["metadata"]["name"], name);
    assert_eq!(body["value"], 1000);
    assert!(
        body["metadata"]["uid"].as_str().is_some(),
        "create must stamp a uid: {body}"
    );

    // GET — single object returns 200 OK with the `value` field intact.
    let (status, body) = get_json(&state, &item_uri).await;
    assert_eq!(status, StatusCode::OK, "GET body: {body}");
    assert_eq!(body["kind"], "PriorityClass");
    assert_eq!(body["metadata"]["name"], name);
    assert_eq!(body["value"], 1000);

    // LIST — collection returns 200 OK, kind PriorityClassList, our item present.
    let (status, body) = get_json(&state, PC_COLLECTION).await;
    assert_eq!(status, StatusCode::OK, "LIST body: {body}");
    assert_eq!(body["kind"], "PriorityClassList");
    assert_eq!(body["apiVersion"], "scheduling.k8s.io/v1");
    let items = body["items"].as_array().expect("items must be an array");
    assert_eq!(items.len(), 1, "expected exactly one item: {body}");
    assert_eq!(items[0]["metadata"]["name"], name);
    assert_eq!(items[0]["value"], 1000);

    // PUT — update a MUTABLE field (description), keeping value unchanged.
    let mut updated = sample_priority_class(name, 1000);
    updated["description"] = json!("top tier");
    let (status, body) = put_json(&state, &item_uri, &updated).await;
    assert_eq!(status, StatusCode::OK, "PUT body: {body}");
    assert_eq!(body["value"], 1000);
    assert_eq!(body["description"], "top tier");

    // PATCH — merge-patch the description field (mutable).
    let (status, body) = patch_json(
        &state,
        &item_uri,
        "application/merge-patch+json",
        &json!({ "description": "patched tier" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "PATCH body: {body}");
    assert_eq!(body["description"], "patched tier");
    assert_eq!(body["value"], 1000, "patch must not touch immutable value");

    // DELETE — returns 200 OK with the deleted object.
    let (status, body) = delete_json(&state, &item_uri).await;
    assert_eq!(status, StatusCode::OK, "DELETE body: {body}");
    assert_eq!(body["metadata"]["name"], name);

    // GET after delete — gone.
    let (status, _body) = get_json(&state, &item_uri).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "GET after delete must 404");
}

/// LIST on an empty collection returns 200 with `kind: PriorityClassList` and
/// an empty `items` array (never `null`, never absent).
#[tokio::test]
async fn priority_class_list_empty_collection() {
    let state = TestApiServer::new();
    let (status, body) = get_json(&state, PC_COLLECTION).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"], "PriorityClassList");
    assert_eq!(body["apiVersion"], "scheduling.k8s.io/v1");
    let items = body["items"].as_array().expect("items must be an array");
    assert!(items.is_empty(), "empty collection must yield []: {body}");
}

/// The handler enforces `value` immutability on PUT: attempting to change the
/// integer value of an existing PriorityClass is rejected (HTTP 422
/// Unprocessable Entity, the api-server mapping for an invalid resource).
#[tokio::test]
async fn priority_class_value_is_immutable_on_put() {
    let state = TestApiServer::new();
    let name = "immutable-pc";
    let item_uri = format!("{PC_COLLECTION}/{name}");

    let (status, _body) = post_json(&state, PC_COLLECTION, &sample_priority_class(name, 500)).await;
    assert_eq!(status, StatusCode::CREATED);

    // Attempt to change value 500 -> 999 via PUT.
    let (status, body) = put_json(&state, &item_uri, &sample_priority_class(name, 999)).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "changing value must be rejected: {body}"
    );

    // Confirm the stored value is unchanged.
    let (status, body) = get_json(&state, &item_uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], 500, "value must remain immutable: {body}");
}

/// The handler also enforces `value` immutability on PATCH: a merge-patch that
/// changes the value is rejected, and the stored value is preserved.
#[tokio::test]
async fn priority_class_value_is_immutable_on_patch() {
    let state = TestApiServer::new();
    let name = "immutable-patch-pc";
    let item_uri = format!("{PC_COLLECTION}/{name}");

    let (status, _body) = post_json(&state, PC_COLLECTION, &sample_priority_class(name, 700)).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = patch_json(
        &state,
        &item_uri,
        "application/merge-patch+json",
        &json!({ "value": 12345 }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "patching value must be rejected: {body}"
    );

    let (status, body) = get_json(&state, &item_uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], 700, "value must remain immutable: {body}");
}

/// At most one PriorityClass may be `globalDefault: true` (upstream priority
/// admission plugin). Creating a second default is rejected with 403 Forbidden;
/// re-applying the same default class via PUT is allowed.
#[tokio::test]
async fn priority_class_global_default_uniqueness() {
    let state = TestApiServer::new();

    let mut first = sample_priority_class("default-a", 1000);
    first["globalDefault"] = json!(true);
    let (code, _) = post_json(&state, PC_COLLECTION, &first).await;
    assert_eq!(code, StatusCode::CREATED, "first default must be accepted");

    // A second globalDefault PriorityClass conflicts.
    let mut second = sample_priority_class("default-b", 2000);
    second["globalDefault"] = json!(true);
    let (code, _) = post_json(&state, PC_COLLECTION, &second).await;
    assert_eq!(
        code,
        StatusCode::FORBIDDEN,
        "second globalDefault PriorityClass must be rejected"
    );

    // A non-default PriorityClass is fine.
    let third = sample_priority_class("regular", 500);
    let (code, _) = post_json(&state, PC_COLLECTION, &third).await;
    assert_eq!(code, StatusCode::CREATED, "non-default must be accepted");

    // Re-applying the existing default via PUT is allowed (same name).
    let mut reapply = sample_priority_class("default-a", 1000);
    reapply["globalDefault"] = json!(true);
    let (code, _) = put_json(&state, &format!("{PC_COLLECTION}/default-a"), &reapply).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "re-marking the same class default must be allowed"
    );

    // Marking the regular class as default while default-a exists is rejected.
    let mut promote = sample_priority_class("regular", 500);
    promote["globalDefault"] = json!(true);
    let (code, _) = put_json(&state, &format!("{PC_COLLECTION}/regular"), &promote).await;
    assert_eq!(
        code,
        StatusCode::FORBIDDEN,
        "promoting a second class to default must be rejected"
    );
}
