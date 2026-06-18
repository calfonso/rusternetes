//! Strict JSON decode parity tests vs upstream `UnmarshalStrict`.
//!
//! Sibling files already cover the basic mode matrix (Strict / Warn / Ignore)
//! and the client-go / empty-object regression cases:
//!
//! - `decoder_strict_fields_test.rs` — mode dispatch, basic unknown / duplicate
//!   field rejection.
//! - `decoder_malformed_json_test.rs` — syntactic decode failures.
//! - `strict_decoding_client_go_pod_test.rs` — `creationTimestamp: null` etc.
//! - `strict_decoding_empty_object_test.rs` — `{}` collapsing to `None`.
//!
//! This file fills the remaining `UnmarshalStrict` parity gaps from upstream
//! `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/json/json.go`
//! (`SerializerOptions{Strict: true}`):
//!
//! 1. **Duplicate-field rejection** with both occurrences named in the message
//!    — the canonical `{"name":"foo","name":"bar"}` case that upstream
//!    apimachinery emits as `duplicate field "name"`.
//! 2. **Strict vs non-strict unknown field** — same body, different outcome
//!    (400 under `Strict`, 201 under `Ignore`, silently dropped on read-back).
//! 3. **Case sensitivity** — JSON is case-sensitive; `APIVersion` (wrong case)
//!    must be flagged as an unknown field, `apiVersion` must round-trip.
//! 4. **Int-vs-float preservation** — integer wire values must NOT round-trip
//!    to floats (Go's `json.Number` / encoding/json preserves the textual form;
//!    `serde_json` with arbitrary_precision off does the same for i64).
//! 5. **`Quantity` polymorphic input** — upstream accepts both
//!    `"requests":{"cpu":"100m"}` AND `"requests":{"cpu":100}` (bare integer
//!    coerced to string via the IntOrString-like decoder).
//! 6. **Null vs missing on required fields** — `spec: null` and `spec` omitted
//!    must both be rejected once schema validation runs (PodSpec.containers is
//!    required). Documents the current shape: the malformed_spec_null sibling
//!    test pins the contract for `spec: null`; here we exercise the contrast
//!    with the omitted case.

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const TEST_NS: &str = "default";

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `send_raw` pushes
// an arbitrary byte body (duplicate keys / odd formatting that `to_vec` would
// normalize away) via `send_bytes`.
// ---------------------------------------------------------------------------

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

/// Send a raw JSON byte body so we can exercise duplicate keys / odd
/// formatting that would otherwise be normalized away by `serde_json::to_vec`.
async fn send_raw(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let (status, _, value) = router
        .send_bytes(method.as_str(), uri, Some("application/json"), Some(body))
        .await;
    (status, value)
}

async fn send_json(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: &Value,
) -> (StatusCode, Value) {
    let bytes = serde_json::to_vec(body).unwrap();
    send_raw(router, method, uri, bytes).await
}

fn pod_uri() -> String {
    format!("/api/v1/namespaces/{}/pods", TEST_NS)
}

fn service_uri() -> String {
    format!("/api/v1/namespaces/{}/services", TEST_NS)
}

// ---------------------------------------------------------------------------
// 1. Duplicate-field rejection — `{"name":"foo","name":"bar"}` case.
// ---------------------------------------------------------------------------

/// Upstream apimachinery emits `duplicate field "name"` when a JSON object has
/// the same key twice. Our differ collects dotted paths, so the duplicate
/// `metadata.name` shows up as `metadata.name`. The brief asks that "both keys
/// be mentioned" — for a duplicated key, "both keys" means the key name
/// appears in the error and the duplicate detection actually fires (not
/// silently last-wins like in non-strict mode).
#[tokio::test]
async fn test_strict_duplicate_name_field_both_mentioned() {
    let router = spawn_router();
    // Two copies of `metadata.name` — one "foo", one "bar". serde_json
    // last-wins gives us "bar"; strict mode must reject before that.
    let body = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "foo", "name": "bar", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    }"#;

    let uri = format!("{}?fieldValidation=Strict", pod_uri());
    let (status, resp) = send_raw(router, Method::POST, &uri, body.as_bytes().to_vec()).await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "strict duplicate field must be 400, got {} body={}",
        status,
        resp
    );
    assert_eq!(resp["kind"], "Status");
    assert_eq!(resp["reason"], "BadRequest");
    let message = resp["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("strict decoding error"),
        "message must wrap in 'strict decoding error: …', got: {}",
        message
    );
    assert!(
        message.contains("duplicate field"),
        "message must mention 'duplicate field', got: {}",
        message
    );
    // "both keys mentioned" — the offending key name appears in the message.
    assert!(
        message.contains("name"),
        "message must identify the duplicated 'name' field, got: {}",
        message
    );
}

/// Non-strict (Ignore) mode must NOT reject duplicate fields — serde_json's
/// last-wins behaviour silently picks one. Companion to the strict case so a
/// future refactor can't tighten Ignore by accident.
#[tokio::test]
async fn test_ignore_duplicate_name_field_silently_accepted() {
    let router = spawn_router();
    let body = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "foo", "name": "bar", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    }"#;

    let uri = format!("{}?fieldValidation=Ignore", pod_uri());
    let (status, resp) = send_raw(router, Method::POST, &uri, body.as_bytes().to_vec()).await;

    assert!(
        status.is_success(),
        "Ignore mode must accept duplicate keys (last-wins), got {} body={}",
        status,
        resp
    );
    // Last-wins: the stored name is "bar".
    assert_eq!(
        resp["metadata"]["name"], "bar",
        "duplicate key last-wins should pick 'bar' in Ignore mode, got body={}",
        resp
    );
}

// ---------------------------------------------------------------------------
// 2. Unknown-field rejection — strict vs non-strict pair.
// ---------------------------------------------------------------------------

/// The canonical `{"name":"foo","notARealField":1}` shape: strict → 400,
/// non-strict (Ignore) → accepted and the unknown field is silently dropped
/// from the response body.
#[tokio::test]
async fn test_strict_unknown_field_rejected() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "foo", "namespace": TEST_NS},
        "spec": {
            "containers": [{"name": "c", "image": "busybox"}],
            "notARealField": 1
        }
    });

    let uri = format!("{}?fieldValidation=Strict", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "strict unknown field must be 400, got {} body={}",
        status,
        resp
    );
    let message = resp["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("unknown field") && message.contains("notARealField"),
        "message must identify 'notARealField' as unknown: {}",
        message
    );
}

#[tokio::test]
async fn test_ignore_unknown_field_silently_dropped() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "foo-ignore", "namespace": TEST_NS},
        "spec": {
            "containers": [{"name": "c", "image": "busybox"}],
            "notARealField": 1
        }
    });

    let uri = format!("{}?fieldValidation=Ignore", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_success(),
        "Ignore mode must accept the request, got {} body={}",
        status,
        resp
    );
    // Unknown field must NOT round-trip — strongly typed Pod has no slot.
    assert!(
        resp["spec"].get("notARealField").is_none(),
        "unknown field must be silently dropped on read-back, got spec={}",
        resp["spec"]
    );
    // The known field round-trips fine.
    assert_eq!(resp["metadata"]["name"], "foo-ignore");
}

// ---------------------------------------------------------------------------
// 3. Case sensitivity — JSON keys are case-sensitive.
// ---------------------------------------------------------------------------

/// `APIVersion` (Go field-name casing) must be rejected as unknown under
/// strict mode — JSON is case-sensitive, and the canonical wire spelling is
/// `apiVersion` (per `#[serde(rename_all = "camelCase")]` on `TypeMeta`).
/// Upstream `encoding/json` is also case-sensitive (with one Go-specific
/// quirk: it does case-insensitive struct tag matching, which we explicitly do
/// NOT replicate — Rust's serde is strictly case-sensitive).
#[tokio::test]
async fn test_strict_wrong_case_apiversion_rejected() {
    let router = spawn_router();
    // `APIVersion` instead of `apiVersion` — the former is the Go field name,
    // the latter is the JSON wire spelling.
    let body = json!({
        "APIVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "wrong-case", "namespace": TEST_NS},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let uri = format!("{}?fieldValidation=Strict", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_client_error(),
        "wrong-case `APIVersion` must be rejected as unknown under strict, \
         got {} body={}",
        status,
        resp
    );
    let message = resp["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("APIVersion"),
        "message must identify the wrong-case `APIVersion` key, got: {}",
        message
    );
}

/// Companion accept-path: the correct lowercase-prefix spelling `apiVersion`
/// must round-trip cleanly. Pins the contract that case sensitivity only
/// rejects truly different spellings, not the canonical wire form.
#[tokio::test]
async fn test_strict_correct_case_apiversion_accepted() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "right-case", "namespace": TEST_NS},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let uri = format!("{}?fieldValidation=Strict", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_success(),
        "canonical `apiVersion` must be accepted under strict, got {} body={}",
        status,
        resp
    );
    assert_eq!(resp["apiVersion"], "v1");
    assert_eq!(resp["kind"], "Pod");
}

// ---------------------------------------------------------------------------
// 4. Int-vs-float preservation — integer port should NOT round-trip to float.
// ---------------------------------------------------------------------------

/// Upstream Go `encoding/json` preserves the textual form of numbers: an
/// integer literal `8080` on the wire stays an integer through the
/// marshal/unmarshal round-trip (it does NOT become `8080.0`). serde_json
/// behaves the same way when targets are `i32`/`i64`/`u16` (and `Number`
/// preserves originality for the loose `serde_json::Value` path too).
///
/// This guards against an accidental switch to `f64` typing for any wire-int
/// field, which would silently break clients that string-match the response.
#[tokio::test]
async fn test_strict_integer_port_preserved_through_round_trip() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "int-port", "namespace": TEST_NS},
        "spec": {
            "type": "ClusterIP",
            "ports": [{"port": 8080, "targetPort": 8080}],
            "selector": {"app": "x"}
        }
    });

    let uri = format!("{}?fieldValidation=Strict", service_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_success(),
        "valid Service must be accepted, got {} body={}",
        status,
        resp
    );

    // Re-encode the response body to bytes and assert the wire form has
    // `"port":8080` (not `"port":8080.0`).
    let reencoded = serde_json::to_string(&resp).unwrap();
    assert!(
        reencoded.contains("\"port\":8080"),
        "port must round-trip as the integer 8080, not a float: {}",
        reencoded
    );
    assert!(
        !reencoded.contains("\"port\":8080.0"),
        "port must NOT round-trip to a float; got: {}",
        reencoded
    );

    // Also assert at the `serde_json::Value` level: `port` must be an i64,
    // not an f64.
    let port = &resp["spec"]["ports"][0]["port"];
    assert!(
        port.is_i64() || port.is_u64(),
        "port must be an integer JSON value, got {:?}",
        port
    );
    assert_eq!(port.as_i64(), Some(8080));
}

// ---------------------------------------------------------------------------
// 5. Quantity — accepts both bare integer and string forms.
// ---------------------------------------------------------------------------

/// Upstream `resource.Quantity` deserialisation accepts BOTH wire forms:
/// `"100"` and `100`. Rusternetes mirrors this via `deserialize_quantity_map`
/// in `crates/common/src/types.rs` (visit_map handles both `Value::String`
/// and `Value::Number`).
#[tokio::test]
async fn test_strict_quantity_accepts_string_form() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "q-string", "namespace": TEST_NS},
        "spec": {
            "containers": [{
                "name": "c",
                "image": "busybox",
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits":   {"cpu": "200m", "memory": "256Mi"}
                }
            }]
        }
    });

    let uri = format!("{}?fieldValidation=Strict", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_success(),
        "string-form Quantity must be accepted under strict, got {} body={}",
        status,
        resp
    );
    assert_eq!(
        resp["spec"]["containers"][0]["resources"]["requests"]["cpu"],
        "100m"
    );
}

/// Bare-integer `Quantity` (`"cpu": 1`) must also be accepted — matches
/// upstream's polymorphic JSON-vs-IntOrString-flavour Quantity decoder. The
/// stored form is the string `"1"` (per `deserialize_quantity_map` which
/// `.to_string()`s `Value::Number`).
#[tokio::test]
async fn test_strict_quantity_accepts_bare_integer_form() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "q-int", "namespace": TEST_NS},
        "spec": {
            "containers": [{
                "name": "c",
                "image": "busybox",
                "resources": {
                    "requests": {"cpu": 1}
                }
            }]
        }
    });

    let uri = format!("{}?fieldValidation=Strict", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_success(),
        "bare-integer Quantity must be accepted (upstream parity), got {} body={}",
        status,
        resp
    );
    // The integer coerces to the string "1" on storage.
    let cpu = &resp["spec"]["containers"][0]["resources"]["requests"]["cpu"];
    assert_eq!(
        cpu.as_str(),
        Some("1"),
        "bare integer Quantity should coerce to canonical string form, got {:?}",
        cpu
    );
}

// ---------------------------------------------------------------------------
// 6. Null vs missing on a required field.
// ---------------------------------------------------------------------------

/// Pod create with `spec: null` — upstream rejects this (PodSpec.containers is
/// `Required value`). Rusternetes currently treats `spec: null` as `None`
/// (PodSpec is `Option<PodSpec>` on `Pod`), which is the same parity gap
/// pinned by `decoder_malformed_json_test::test_malformed_spec_null`. Re-pin
/// here as part of the parity-gap matrix.
#[tokio::test]
async fn test_strict_spec_null_rejected() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "null-spec", "namespace": TEST_NS},
        "spec": null
    });

    let uri = format!("{}?fieldValidation=Strict", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_client_error(),
        "spec: null must be rejected (required field), got {} body={}",
        status,
        resp
    );
}

/// Pod create with `spec` entirely omitted — same expected outcome as the
/// `spec: null` case. Tests the "missing required field" half of the null vs
/// missing parity matrix.
///
/// Upstream behaviour: the JSON decoder accepts the missing key (Go zero-value
/// PodSpec), but downstream `ValidatePodSpec` then surfaces
/// `spec.containers: Required value` → HTTP 422 Invalid. We pin the same
/// 4xx + Status-object contract.
#[tokio::test]
async fn test_strict_spec_omitted_rejected() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "omitted-spec", "namespace": TEST_NS}
        // no `spec` key at all
    });

    let uri = format!("{}?fieldValidation=Strict", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_client_error(),
        "omitted spec must be rejected (containers required), got {} body={}",
        status,
        resp
    );
    assert_eq!(resp["kind"], "Status");
    assert_eq!(resp["status"], "Failure");
}

/// Sibling Ignore-mode case: even under permissive field-validation, the
/// REQUIRED-field business validation still fires. Pins that strict-vs-Ignore
/// only differs on unknown fields — required-field violations are universal.
#[tokio::test]
async fn test_ignore_spec_null_still_rejected_by_validation() {
    let router = spawn_router();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "null-spec-ignore", "namespace": TEST_NS},
        "spec": null
    });

    let uri = format!("{}?fieldValidation=Ignore", pod_uri());
    let (status, resp) = send_json(router, Method::POST, &uri, &body).await;

    assert!(
        status.is_client_error(),
        "spec: null must still be rejected even under Ignore (validation \
         layer), got {} body={}",
        status,
        resp
    );
    assert_eq!(resp["kind"], "Status");
}
