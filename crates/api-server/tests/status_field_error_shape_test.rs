//! Upstream-parity pins for the `Status.details.causes[]` shape on 422 Invalid
//! responses.
//!
//! Borrowed from upstream `staging/src/k8s.io/apimachinery/pkg/api/errors/errors_test.go`
//! (release-1.35). Upstream's `NewInvalid` constructor on a `field.ErrorList`
//! produces a `Status` whose `details.causes[]` carries one entry per
//! `field.Error` in the list, and each entry's `reason` is the upstream-
//! taxonomy mapping of the underlying `ErrorType`:
//!
//! | upstream `field.ErrorType` | `StatusCause.Type` (= `causes[].reason`) |
//! | -------------------------- | ---------------------------------------- |
//! | Required                   | `FieldValueRequired`                     |
//! | Invalid / TypeInvalid      | `FieldValueInvalid`                      |
//! | Duplicate                  | `FieldValueDuplicate`                    |
//! | Forbidden                  | `FieldValueForbidden`                    |
//! | NotFound                   | `FieldValueNotFound`                     |
//! | NotSupported               | `FieldValueNotSupported`                 |
//! | TooLong                    | `FieldValueTooLong`                      |
//! | TooMany                    | `FieldValueTooMany`                      |
//! | Internal                   | `FieldValueInternal`                     |
//!
//! Upstream source (release-1.35):
//!   * `apimachinery/pkg/api/errors/errors.go` — `NewInvalid`
//!   * `apimachinery/pkg/util/validation/field/errors.go` — `ErrorType`,
//!     `Error.ErrorTypeMapping` (the cause-reason map above)
//!   * `apimachinery/pkg/api/errors/errors_test.go` — `TestNewInvalid` and
//!     friends, which pin the resulting `Status` byte-for-byte.
//!
//! ---------------------------------------------------------------------------
//! Current rusternetes behaviour (release `feat/pod-update-immutability-parity`)
//! ---------------------------------------------------------------------------
//!
//! `crates/common/src/error.rs::extract_resource_details_for_invalid` builds a
//! single hardcoded cause for every 422 Invalid response:
//!
//! ```ignore
//! Some(StatusDetails {
//!     name: None, group: None, kind: None, uid: None,
//!     causes: Some(vec![StatusCause {
//!         reason:  Some("FieldValueInvalid".to_string()),
//!         message: Some(msg.to_string()),
//!         field:   Some("metadata.name".to_string()),
//!     }]),
//!     retry_after_seconds: None,
//! })
//! ```
//!
//! Consequences vs upstream:
//!   * `causes[].reason` is always `"FieldValueInvalid"` regardless of the
//!     real field-error type (Required, Duplicate, Forbidden, NotSupported…).
//!   * `causes[].field` is always `"metadata.name"` — the real field path
//!     (e.g. `spec.containers`, `spec.containers[1].name`) is stuffed into
//!     `message` only.
//!   * `details.name` / `.group` / `.kind` are always `None` — upstream sets
//!     these from the offending object (e.g. `kind="Pod"`, `group=""`).
//!   * Multi-cause aggregation: handlers short-circuit on the first
//!     `field.Error`, so multiple violations in one request still produce a
//!     single-entry `causes[]`.
//!
//! Every granular per-reason test in this file is therefore RED today and is
//! marked `#[ignore]` so the file compiles and runs green; the inline reason
//! tells future readers what each one is pinning. A baseline test
//! `current_invalid_shape_is_single_hardcoded_cause` runs unconditionally and
//! documents (and locks down) the current — wrong — shape, so any drift in
//! the build_status_response code path is also caught here.
//!
//! TODO(rusternetes): once `extract_resource_details_for_invalid` is replaced
//! with a `field::ErrorList`-aware builder (i.e. handlers return the list
//! rather than a flat `String`, and the IntoResponse impl maps each entry to
//! its upstream `causes[].reason`), drop every `#[ignore]` below.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// HTTP harness — thin `(u16, Value)` shims over the shared `TestApiServer`. We
// drive the production axum router so the full handler / IntoResponse stack runs.
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

async fn patch_json(
    router: &TestApiServer,
    uri: &str,
    content_type: &str,
    body: &Value,
) -> (u16, Value) {
    let (status, value) = router
        .send("PATCH", uri, Some(content_type), Some(body))
        .await;
    (status.as_u16(), value)
}

async fn create_namespace(router: &TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    });
    let (status, body) = post_json(router, "/api/v1/namespaces", &body).await;
    assert!(
        status == 201 || status == 200,
        "namespace create must succeed: status={status} body={body}"
    );
}

// ---------------------------------------------------------------------------
// Status-shape assertion helpers
// ---------------------------------------------------------------------------

/// Assert the top-level Status envelope: `kind=Status`, `status=Failure`,
/// `reason=Invalid`, `code=422`. Same envelope upstream's `NewInvalid`
/// produces.
fn assert_invalid_envelope(body: &Value) {
    assert_eq!(
        body.get("kind").and_then(|v| v.as_str()),
        Some("Status"),
        "Status.kind: body={body}"
    );
    assert_eq!(
        body.get("apiVersion").and_then(|v| v.as_str()),
        Some("v1"),
        "Status.apiVersion: body={body}"
    );
    assert_eq!(
        body.get("status").and_then(|v| v.as_str()),
        Some("Failure"),
        "Status.status: body={body}"
    );
    assert_eq!(
        body.get("reason").and_then(|v| v.as_str()),
        Some("Invalid"),
        "Status.reason: body={body}"
    );
    assert_eq!(
        body.get("code").and_then(|v| v.as_u64()),
        Some(422),
        "Status.code: body={body}"
    );
}

/// Upstream parity: assert that `causes[]` contains at least one entry with
/// the given `reason` and `field`. Used by the (currently `#[ignore]`d)
/// per-reason tests.
fn assert_status_with_cause(body: &Value, reason: &str, field: &str) {
    assert_invalid_envelope(body);
    let causes = body
        .pointer("/details/causes")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("Status.details.causes is missing or not an array: body={body}"));
    let matched = causes.iter().any(|c| {
        c.get("reason").and_then(|v| v.as_str()) == Some(reason)
            && c.get("field").and_then(|v| v.as_str()) == Some(field)
    });
    assert!(
        matched,
        "expected a cause with reason={reason:?} field={field:?}, got causes={causes:#?}"
    );
}

// ---------------------------------------------------------------------------
// Pod fixture
// ---------------------------------------------------------------------------

fn pod_with_spec(name: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name },
        "spec": spec,
    })
}

fn good_container() -> Value {
    json!({ "name": "c0", "image": "nginx:1.27" })
}

// ===========================================================================
// Baseline (post-fix): emptying `spec.containers` produces the upstream-shaped
// single-cause `FieldValueRequired` / `spec.containers` response. Pre-fix this
// test pinned the *wrong* shape (`FieldValueInvalid` / `metadata.name`) so
// the granular per-reason tests below could stay `#[ignore]`d; once the
// upstream-parity `field::ErrorList`-aware builder shipped the assertions
// were inverted to lock down the new — correct — contract.
// ===========================================================================

/// Single-cause smoke test: empty `spec.containers` → 422 Invalid with a
/// single `Status.details.causes[]` entry, reason `FieldValueRequired`,
/// field `spec.containers`. Mirrors upstream `field.Required` →
/// `metav1.CauseTypeFieldValueRequired` mapping in
/// `apimachinery/pkg/api/errors/errors.go::NewInvalid`.
#[tokio::test]
async fn invalid_shape_is_single_required_cause_for_empty_containers() {
    let router = spawn_router();
    create_namespace(&router, "default").await;

    let pod = pod_with_spec("p0", json!({ "containers": [] }));
    let (status, body) = post_json(&router, "/api/v1/namespaces/default/pods", &pod).await;
    assert_eq!(status, 422, "expected 422 Invalid: body={body}");
    assert_invalid_envelope(&body);

    let causes = body
        .pointer("/details/causes")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("expected details.causes array: body={body}"));
    assert_eq!(
        causes.len(),
        1,
        "exactly one violation → one cause: body={body}"
    );
    let c0 = &causes[0];
    assert_eq!(
        c0.get("reason").and_then(|v| v.as_str()),
        Some("FieldValueRequired"),
        "empty containers → FieldValueRequired: cause={c0}"
    );
    assert_eq!(
        c0.get("field").and_then(|v| v.as_str()),
        Some("spec.containers"),
        "field path is the real upstream breadcrumb: cause={c0}"
    );
    let msg = c0.get("message").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        msg.contains("spec.containers"),
        "cause.message carries the upstream `<field>: <ErrorType>: <detail>` rendering: cause={c0}"
    );
}

// ===========================================================================
// Upstream parity pins — each maps to one `causes[].reason` variant. All are
// `#[ignore]` until `extract_resource_details_for_invalid` is replaced with a
// `field::ErrorList`-aware builder. The inline reason strings are stable so
// `git grep` can find them when the gap is closed.
// ===========================================================================

/// Upstream: empty `spec.containers` → `field.Required(spec.containers, "")`
/// → `causes[0].reason = "FieldValueRequired"`, `field = "spec.containers"`.
///
/// Rust handler at `crates/api-server/src/handlers/pod.rs:108-117` does
/// return the right *message* (`"spec.containers: Required value..."`) but
/// the IntoResponse builder collapses it to `FieldValueInvalid` /
/// `metadata.name`.
#[tokio::test]
async fn invalid_pod_empty_containers_emits_field_value_required() {
    let router = spawn_router();
    create_namespace(&router, "default").await;

    let pod = pod_with_spec("p-required", json!({ "containers": [] }));
    let (status, body) = post_json(&router, "/api/v1/namespaces/default/pods", &pod).await;
    assert_eq!(status, 422, "expected 422 Invalid: body={body}");
    assert_status_with_cause(&body, "FieldValueRequired", "spec.containers");
}

/// Upstream: a container name that violates DNS-1123-label format
/// (uppercase, `!`, etc.) → `field.Invalid(spec.containers[i].name, ...)` →
/// `causes[].reason = "FieldValueInvalid"`, `field =
/// "spec.containers[0].name"`.
///
/// Rust handler currently does no name-format validation at all (see
/// `crates/api-server/src/handlers/pod.rs:118-131` which only checks
/// emptiness), so this test is doubly blocked: validation is missing AND the
/// builder hardcodes the field path.
#[tokio::test]
async fn invalid_container_name_emits_field_value_invalid() {
    let router = spawn_router();
    create_namespace(&router, "default").await;

    let pod = pod_with_spec(
        "p-invalid",
        json!({
            "containers": [{ "name": "BadName!", "image": "nginx:1.27" }],
        }),
    );
    let (status, body) = post_json(&router, "/api/v1/namespaces/default/pods", &pod).await;
    assert_eq!(status, 422, "expected 422 Invalid: body={body}");
    assert_status_with_cause(&body, "FieldValueInvalid", "spec.containers[0].name");
}

/// Upstream: two containers with the same name →
/// `field.Duplicate(spec.containers[1].name, "dup")` → `causes[].reason =
/// "FieldValueDuplicate"`, `field = "spec.containers[1].name"`.
///
/// Rust handler emits the right *message* shape at
/// `crates/api-server/src/handlers/pod.rs:137-167` (`"spec.containers[1].name:
/// Duplicate value: \"...\""`) but the cause `reason` is again hardcoded to
/// `FieldValueInvalid`.
#[tokio::test]
async fn duplicate_container_names_emit_field_value_duplicate() {
    let router = spawn_router();
    create_namespace(&router, "default").await;

    let pod = pod_with_spec(
        "p-dup",
        json!({
            "containers": [
                { "name": "ctr-a", "image": "nginx:1.27" },
                { "name": "ctr-a", "image": "nginx:1.27" },
            ],
        }),
    );
    let (status, body) = post_json(&router, "/api/v1/namespaces/default/pods", &pod).await;
    assert_eq!(status, 422, "expected 422 Invalid: body={body}");
    assert_status_with_cause(&body, "FieldValueDuplicate", "spec.containers[1].name");
}

/// Upstream: changing `spec.nodeName` after it has been set →
/// `field.Forbidden(spec.nodeName, "field is immutable")` →
/// `causes[].reason = "FieldValueForbidden"`, `field = "spec.nodeName"`.
///
/// Rust handler at `crates/api-server/src/handlers/pod.rs:893-914` emits the
/// wrong upstream `ErrorType` too (uses `Invalid value: ...: field is
/// immutable` instead of `Forbidden: field is immutable`), so this pin is
/// blocked on both the validator and the builder.
#[tokio::test]
async fn pod_update_changing_node_name_emits_field_value_forbidden() {
    let router = spawn_router();
    create_namespace(&router, "default").await;

    // Create the pod with nodeName set so the post-set immutability fence
    // applies on the update. (Equivalent of binding the pod upstream.)
    let pod = pod_with_spec(
        "p-fbd",
        json!({
            "containers": [good_container()],
            "nodeName": "node-1",
        }),
    );
    let (status, body) = post_json(&router, "/api/v1/namespaces/default/pods", &pod).await;
    assert!(
        status == 201 || status == 200,
        "pod create must succeed: status={status} body={body}"
    );

    // Now attempt to change `spec.nodeName` via the main resource PUT.
    let updated = pod_with_spec(
        "p-fbd",
        json!({
            "containers": [good_container()],
            "nodeName": "node-2",
        }),
    );
    let (status, body) = put_json(&router, "/api/v1/namespaces/default/pods/p-fbd", &updated).await;
    assert_eq!(status, 422, "expected 422 Invalid: body={body}");
    assert_status_with_cause(&body, "FieldValueForbidden", "spec.nodeName");
}

/// Upstream: a JSON Patch `{"op":"remove","path":"/spec/doesNotExist"}`
/// against an existing object → the patch library raises a "path not found"
/// failure that the api-server reflects as `field.NotFound(path, "")` →
/// `causes[].reason = "FieldValueNotFound"`, `field = "spec.doesNotExist"`.
///
/// Rust handler at `crates/api-server/src/handlers/pod.rs:1450-1500` does
/// surface patch failures as 422 Invalid (via
/// `Error::InvalidResource(e.to_string())`), but the builder still emits a
/// hardcoded `FieldValueInvalid` / `metadata.name` cause.
#[tokio::test]
async fn json_patch_remove_missing_field_emits_field_value_not_found() {
    let router = spawn_router();
    create_namespace(&router, "default").await;

    let pod = pod_with_spec("p-nf", json!({ "containers": [good_container()] }));
    let (status, body) = post_json(&router, "/api/v1/namespaces/default/pods", &pod).await;
    assert!(
        status == 201 || status == 200,
        "pod create must succeed: status={status} body={body}"
    );

    // RFC 6902 JSON Patch with a `remove` op pointing at a field that
    // does not exist on the persisted object.
    let patch = json!([
        { "op": "remove", "path": "/spec/doesNotExist" },
    ]);
    let (status, body) = patch_json(
        &router,
        "/api/v1/namespaces/default/pods/p-nf",
        "application/json-patch+json",
        &patch,
    )
    .await;
    assert_eq!(status, 422, "expected 422 Invalid: body={body}");
    assert_status_with_cause(&body, "FieldValueNotFound", "spec.doesNotExist");
}

/// Upstream: `spec.restartPolicy = "InvalidPolicy"` →
/// `field.NotSupported(spec.restartPolicy, "InvalidPolicy",
/// ["Always","OnFailure","Never"])` → `causes[].reason =
/// "FieldValueNotSupported"`, `field = "spec.restartPolicy"`.
///
/// Rust handler currently does not enforce the restartPolicy enum at all —
/// the value passes through into storage. So this pin is blocked on both the
/// validator and the cause builder.
#[tokio::test]
async fn invalid_restart_policy_emits_field_value_not_supported() {
    let router = spawn_router();
    create_namespace(&router, "default").await;

    let pod = pod_with_spec(
        "p-ns",
        json!({
            "containers": [good_container()],
            "restartPolicy": "InvalidPolicy",
        }),
    );
    let (status, body) = post_json(&router, "/api/v1/namespaces/default/pods", &pod).await;
    assert_eq!(status, 422, "expected 422 Invalid: body={body}");
    assert_status_with_cause(&body, "FieldValueNotSupported", "spec.restartPolicy");
}

/// Upstream: a single request that trips multiple validators emits a
/// `Status` whose `details.causes[]` has one entry per violation, in field-
/// path order. `TestNewInvalidMulti` in `errors_test.go` pins this.
///
/// Here we send a pod with TWO violations:
///   * `spec.containers[0].name` is `""` → `FieldValueRequired`,
///   * `spec.restartPolicy` is `"InvalidPolicy"` → `FieldValueNotSupported`.
///
/// Upstream returns `causes.len() == 2`. Rust handlers short-circuit on the
/// first failure, so we only see one cause today.
#[tokio::test]
async fn multi_violation_request_emits_one_cause_per_violation() {
    let router = spawn_router();
    create_namespace(&router, "default").await;

    let pod = pod_with_spec(
        "p-multi",
        json!({
            "containers": [{ "name": "", "image": "nginx:1.27" }],
            "restartPolicy": "InvalidPolicy",
        }),
    );
    let (status, body) = post_json(&router, "/api/v1/namespaces/default/pods", &pod).await;
    assert_eq!(status, 422, "expected 422 Invalid: body={body}");
    assert_invalid_envelope(&body);

    let causes = body
        .pointer("/details/causes")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("expected details.causes array: body={body}"));
    assert!(
        causes.len() >= 2,
        "expected at least 2 causes (one per violation), got {} cause(s): {:#?}",
        causes.len(),
        causes
    );
    // And: the two distinct reasons must both appear.
    let mut saw_required = false;
    let mut saw_not_supported = false;
    for c in causes {
        match c.get("reason").and_then(|v| v.as_str()) {
            Some("FieldValueRequired") => saw_required = true,
            Some("FieldValueNotSupported") => saw_not_supported = true,
            _ => {}
        }
    }
    assert!(
        saw_required && saw_not_supported,
        "expected both FieldValueRequired and FieldValueNotSupported in causes={causes:#?}"
    );
}
