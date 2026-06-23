//! Strategy parity for `batch/v1` Job and CronJob, mirroring upstream
//! `pkg/registry/batch/{job,cronjob}/strategy_test.go` semantics.
//!
//! Upstream's `strategy` layer is the registry-level hook the apiserver calls
//! before persisting a `batch/v1` object. It does:
//!   * `PrepareForCreate` — set per-resource defaults (Job: completions,
//!     parallelism, completionMode, backoffLimit, suspend; CronJob:
//!     concurrencyPolicy, suspend, successful/failedJobsHistoryLimit).
//!   * `PrepareForUpdate` — preserve fields that are immutable on update.
//!   * Selector auto-generation for Jobs when `spec.manualSelector` is unset/false.
//!   * Status subresource isolation: `/status` PUTs MUST NOT bleed into spec.
//!
//! Rusternetes wires the equivalent logic in
//! `crates/api-server/src/handlers/{job,cronjob}.rs` (`apply_*_defaults`,
//! selector auto-generation, dispatch to the shared `status` subresource
//! handler). This file exercises that wiring through the live Axum router,
//! exactly the way a kube-apiserver upstream client would.
//!
//! ## Test layer
//!
//! Layer 3 (`registry/strategy`) of the six-layer rusternetes parity plan:
//!   1. roundtrip serialization (covered elsewhere)
//!   2. (reserved)
//!   3. **registry/strategy** ← this file
//!   4. decoder tests
//!   5. OpenAPI/discovery schema tests
//!   6. integration tests
//!   7. cargo-driven e2e
//!
//! Pattern after `crates/api-server/tests/integration_dryrun_all_resources.rs`:
//! spawn the router with `MemoryStorage`, drive it via `tower::ServiceExt::oneshot`,
//! and assert both the HTTP response body AND the stored object.

use axum::http::Method;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`. `mem` is the
// backing store for stored-object assertions.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "batchstrategy";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send_json(router: TestApiServer, method: Method, uri: &str, body: &Value) -> (u16, Value) {
    let (status, value) = router
        .send(method.as_str(), uri, Some("application/json"), Some(body))
        .await;
    (status.as_u16(), value)
}

async fn read_stored(mem: &Arc<MemoryStorage>, resource: &str, name: &str) -> Value {
    let key = build_key(resource, Some(TEST_NS), name);
    mem.get::<Value>(&key)
        .await
        .unwrap_or_else(|e| panic!("missing stored object at {key}: {e}"))
}

// ---------------------------------------------------------------------------
// Stub builders — minimal valid `batch/v1` bodies for each scenario.
// ---------------------------------------------------------------------------

fn minimal_job_stub(name: &str) -> Value {
    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"image": "busybox", "name": "c"}],
                    "restartPolicy": "Never"
                }
            }
        }
    })
}

fn manual_selector_job_stub(name: &str) -> Value {
    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "manualSelector": true,
            "selector": {"matchLabels": {"user-supplied": "yes"}},
            "template": {
                "metadata": {"labels": {"user-supplied": "yes"}},
                "spec": {
                    "containers": [{"image": "busybox", "name": "c"}],
                    "restartPolicy": "Never"
                }
            }
        }
    })
}

fn minimal_cronjob_stub(name: &str) -> Value {
    json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": name, "namespace": TEST_NS},
        "spec": {
            "schedule": "*/5 * * * *",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"image": "busybox", "name": "c"}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Job — create defaulting
// ---------------------------------------------------------------------------

/// Upstream `TestJobStrategy` (PrepareForCreate path) asserts:
///   - completions defaults to 1
///   - parallelism defaults to 1
///   - completionMode defaults to "NonIndexed"
///   - backoffLimit defaults to 6
///   - suspend defaults to false
#[tokio::test]
async fn test_job_strategy_create_applies_defaults() {
    let (mem, router) = spawn_router();
    let body = minimal_job_stub("defaulting-job");

    let (status, resp) = send_json(
        router,
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs"),
        &body,
    )
    .await;
    assert_eq!(status, 201, "POST job: {resp}");

    for source in [&resp, &read_stored(&mem, "jobs", "defaulting-job").await] {
        let spec = source.get("spec").expect("spec");
        assert_eq!(
            spec.get("completions").and_then(Value::as_i64),
            Some(1),
            "completions should default to 1 (mirrors SetDefaults_Job)"
        );
        assert_eq!(
            spec.get("parallelism").and_then(Value::as_i64),
            Some(1),
            "parallelism should default to 1"
        );
        assert_eq!(
            spec.get("completionMode").and_then(Value::as_str),
            Some("NonIndexed"),
            "completionMode should default to NonIndexed"
        );
        assert_eq!(
            spec.get("backoffLimit").and_then(Value::as_i64),
            Some(6),
            "backoffLimit should default to 6"
        );
        assert_eq!(
            spec.get("suspend").and_then(Value::as_bool),
            Some(false),
            "suspend should default to false"
        );
    }
}

// ---------------------------------------------------------------------------
// Job — selector / template label auto-generation
// ---------------------------------------------------------------------------

/// Upstream `TestJobStrategy_PrepareForCreate` (with `manualSelector` unset)
/// auto-populates `spec.selector.matchLabels[batch.kubernetes.io/controller-uid]`
/// and adds BOTH the prefixed and legacy `controller-uid` / `job-name` labels to
/// the pod template (matching `generateSelector`).
#[tokio::test]
async fn test_job_strategy_create_auto_generates_selector() {
    let (mem, router) = spawn_router();
    let body = minimal_job_stub("auto-selector");

    let (status, resp) = send_json(
        router,
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs"),
        &body,
    )
    .await;
    assert_eq!(status, 201, "POST job: {resp}");

    let stored = read_stored(&mem, "jobs", "auto-selector").await;
    let uid = stored
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .expect("controller assigns uid")
        .to_string();
    assert!(!uid.is_empty(), "uid must be non-empty after create");

    // selector must be populated on the prefixed controller-uid (upstream
    // generateSelector selects on batch.kubernetes.io/controller-uid).
    let selector_uid = stored
        .pointer("/spec/selector/matchLabels/batch.kubernetes.io~1controller-uid")
        .and_then(Value::as_str)
        .expect("auto-generated selector should set batch.kubernetes.io/controller-uid");
    assert_eq!(
        selector_uid, uid,
        "selector controller-uid matches metadata.uid"
    );

    // template labels must include BOTH the prefixed and legacy
    // controller-uid / job-name labels.
    for key in ["controller-uid", "batch.kubernetes.io~1controller-uid"] {
        let tmpl_uid = stored
            .pointer(&format!("/spec/template/metadata/labels/{key}"))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("template labels must include {key}"));
        assert_eq!(tmpl_uid, uid, "{key} must equal uid");
    }
    for key in ["job-name", "batch.kubernetes.io~1job-name"] {
        let job_name_label = stored
            .pointer(&format!("/spec/template/metadata/labels/{key}"))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("template labels must include {key}"));
        assert_eq!(job_name_label, "auto-selector", "{key} must equal job name");
    }
}

/// Upstream: when `spec.manualSelector` is true, the strategy does NOT
/// overwrite the user-supplied selector or template labels.
#[tokio::test]
async fn test_job_strategy_create_manual_selector_preserved() {
    let (mem, router) = spawn_router();
    let body = manual_selector_job_stub("manual-selector");

    let (status, resp) = send_json(
        router,
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs"),
        &body,
    )
    .await;
    assert_eq!(status, 201, "POST job: {resp}");

    let stored = read_stored(&mem, "jobs", "manual-selector").await;
    let labels = stored
        .pointer("/spec/selector/matchLabels")
        .and_then(Value::as_object)
        .expect("selector matchLabels");
    assert_eq!(
        labels.get("user-supplied").and_then(Value::as_str),
        Some("yes"),
        "user selector preserved when manualSelector=true"
    );
    // controller-uid must NOT be injected over the manual selector
    assert!(
        !labels.contains_key("controller-uid"),
        "manualSelector=true must skip controller-uid auto-injection"
    );
}

// ---------------------------------------------------------------------------
// Job — update behaviour
// ---------------------------------------------------------------------------

/// Upstream `TestJobStrategy_PrepareForUpdate` keeps `spec.parallelism` mutable
/// (along with `spec.activeDeadlineSeconds`), which the rusternetes update
/// handler must accept. We assert the round-trip persists the new value.
#[tokio::test]
async fn test_job_strategy_update_allows_parallelism_change() {
    let (mem, router) = spawn_router();
    let create_body = minimal_job_stub("mutable-fields");

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs"),
        &create_body,
    )
    .await;
    assert_eq!(status, 201);

    // Pull stored object so we have its uid and full defaulted spec.
    let mut stored = read_stored(&mem, "jobs", "mutable-fields").await;
    assert_eq!(
        stored.pointer("/spec/parallelism").and_then(Value::as_i64),
        Some(1),
        "precondition: parallelism defaulted to 1 on create"
    );

    // Bump parallelism + add activeDeadlineSeconds — both upstream-mutable.
    if let Some(spec) = stored.get_mut("spec").and_then(Value::as_object_mut) {
        spec.insert("parallelism".to_string(), json!(7));
        spec.insert("activeDeadlineSeconds".to_string(), json!(600));
    }

    let (status, resp) = send_json(
        router,
        Method::PUT,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs/mutable-fields"),
        &stored,
    )
    .await;
    assert_eq!(status, 200, "PUT job (parallelism): {resp}");

    let after = read_stored(&mem, "jobs", "mutable-fields").await;
    assert_eq!(
        after.pointer("/spec/parallelism").and_then(Value::as_i64),
        Some(7),
        "parallelism should be mutable on update"
    );
    assert_eq!(
        after
            .pointer("/spec/activeDeadlineSeconds")
            .and_then(Value::as_i64),
        Some(600),
        "activeDeadlineSeconds should be mutable on update"
    );
}

/// Upstream: `spec.completions`, `spec.completionMode`, `spec.selector`, and
/// `spec.template` are immutable on update (with narrow exceptions like
/// adding tolerations under feature gates). Mutating any of them should be
/// rejected by the registry strategy.
///
/// Rusternetes does not yet implement `ValidateJobUpdate` immutability —
/// the update handler today re-applies defaults but does not diff against
/// the stored object. We pin the upstream semantics here as `#[ignore]`d
/// so the test becomes the regression gate once immutability lands.
#[tokio::test]
async fn test_job_strategy_update_rejects_immutable_field_change() {
    let (mem, router) = spawn_router();
    let create_body = minimal_job_stub("immutable-fields");

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs"),
        &create_body,
    )
    .await;
    assert_eq!(status, 201);

    let mut stored = read_stored(&mem, "jobs", "immutable-fields").await;
    // Attempt to flip completions 1 -> 2 — upstream rejects with Invalid.
    if let Some(spec) = stored.get_mut("spec").and_then(Value::as_object_mut) {
        spec.insert("completions".to_string(), json!(2));
    }

    let (status, resp) = send_json(
        router,
        Method::PUT,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs/immutable-fields"),
        &stored,
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "mutating spec.completions on update should be rejected (got {status}: {resp})"
    );

    // And the stored object must NOT pick up the mutation.
    let after = read_stored(&mem, "jobs", "immutable-fields").await;
    assert_eq!(
        after.pointer("/spec/completions").and_then(Value::as_i64),
        Some(1),
        "stored completions must remain at create-time value"
    );
}

// ---------------------------------------------------------------------------
// Job — status subresource isolation
// ---------------------------------------------------------------------------

/// Upstream `Job` registers a `/status` subresource whose strategy preserves
/// spec on writes. PUT to `/status` with a mutated spec must NOT touch spec.
#[tokio::test]
async fn test_job_strategy_status_subresource_preserves_spec() {
    let (mem, router) = spawn_router();
    let create_body = minimal_job_stub("status-isolation");

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs"),
        &create_body,
    )
    .await;
    assert_eq!(status, 201);

    let mut stored = read_stored(&mem, "jobs", "status-isolation").await;
    assert_eq!(
        stored.pointer("/spec/parallelism").and_then(Value::as_i64),
        Some(1),
        "precondition: parallelism defaulted to 1 on create"
    );

    // Forge a PUT body whose spec is mutated AND whose status is set.
    if let Some(spec) = stored.get_mut("spec").and_then(Value::as_object_mut) {
        spec.insert("parallelism".to_string(), json!(99));
    }
    if let Some(obj) = stored.as_object_mut() {
        obj.insert(
            "status".to_string(),
            json!({"active": 3, "succeeded": 0, "failed": 0}),
        );
    }

    let (status, resp) = send_json(
        router,
        Method::PUT,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/jobs/status-isolation/status"),
        &stored,
    )
    .await;
    assert_eq!(status, 200, "PUT job/status: {resp}");

    let after = read_stored(&mem, "jobs", "status-isolation").await;
    assert_eq!(
        after.pointer("/spec/parallelism").and_then(Value::as_i64),
        Some(1),
        "/status subresource must NOT alter spec"
    );
    assert_eq!(
        after.pointer("/status/active").and_then(Value::as_i64),
        Some(3),
        "/status subresource must persist status fields"
    );
}

// ---------------------------------------------------------------------------
// CronJob — create defaulting
// ---------------------------------------------------------------------------

/// Upstream `TestCronJobStrategy` (PrepareForCreate path) asserts:
///   - concurrencyPolicy defaults to "Allow"
///   - suspend defaults to false
///   - successfulJobsHistoryLimit defaults to 3
///   - failedJobsHistoryLimit defaults to 1
///   - startingDeadlineSeconds is NOT defaulted (must remain absent)
#[tokio::test]
async fn test_cronjob_strategy_create_applies_defaults() {
    let (mem, router) = spawn_router();
    let body = minimal_cronjob_stub("defaulting-cj");

    let (status, resp) = send_json(
        router,
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/cronjobs"),
        &body,
    )
    .await;
    assert_eq!(status, 201, "POST cronjob: {resp}");

    for source in [&resp, &read_stored(&mem, "cronjobs", "defaulting-cj").await] {
        let spec = source.get("spec").expect("spec");
        assert_eq!(
            spec.get("concurrencyPolicy").and_then(Value::as_str),
            Some("Allow"),
            "concurrencyPolicy should default to Allow"
        );
        assert_eq!(
            spec.get("suspend").and_then(Value::as_bool),
            Some(false),
            "suspend should default to false"
        );
        assert_eq!(
            spec.get("successfulJobsHistoryLimit")
                .and_then(Value::as_i64),
            Some(3),
            "successfulJobsHistoryLimit should default to 3"
        );
        assert_eq!(
            spec.get("failedJobsHistoryLimit").and_then(Value::as_i64),
            Some(1),
            "failedJobsHistoryLimit should default to 1"
        );
        assert!(
            spec.get("startingDeadlineSeconds").is_none(),
            "startingDeadlineSeconds must NOT be defaulted; remained absent in upstream"
        );
    }
}

// ---------------------------------------------------------------------------
// CronJob — update behaviour
// ---------------------------------------------------------------------------

/// Upstream `TestCronJobStrategy_PrepareForUpdate`: the schedule and the
/// jobTemplate may be updated freely. Both must round-trip via PUT.
#[tokio::test]
async fn test_cronjob_strategy_update_allows_schedule_and_job_template_change() {
    let (mem, router) = spawn_router();
    let create_body = minimal_cronjob_stub("mutable-cj");

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/cronjobs"),
        &create_body,
    )
    .await;
    assert_eq!(status, 201);

    let mut stored = read_stored(&mem, "cronjobs", "mutable-cj").await;

    // Mutate schedule and jobTemplate image — both upstream-mutable.
    if let Some(spec) = stored.get_mut("spec").and_then(Value::as_object_mut) {
        spec.insert("schedule".to_string(), json!("0 0 * * *"));
    }
    if let Some(container) = stored.pointer_mut("/spec/jobTemplate/spec/template/spec/containers/0")
    {
        if let Some(obj) = container.as_object_mut() {
            obj.insert("image".to_string(), json!("alpine:3.20"));
        }
    }

    let (status, resp) = send_json(
        router,
        Method::PUT,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/cronjobs/mutable-cj"),
        &stored,
    )
    .await;
    assert_eq!(status, 200, "PUT cronjob: {resp}");

    let after = read_stored(&mem, "cronjobs", "mutable-cj").await;
    assert_eq!(
        after.pointer("/spec/schedule").and_then(Value::as_str),
        Some("0 0 * * *"),
        "schedule must be mutable on update"
    );
    assert_eq!(
        after
            .pointer("/spec/jobTemplate/spec/template/spec/containers/0/image")
            .and_then(Value::as_str),
        Some("alpine:3.20"),
        "jobTemplate must be mutable on update"
    );
}

// ---------------------------------------------------------------------------
// CronJob — status subresource isolation
// ---------------------------------------------------------------------------

/// Upstream `CronJob` /status strategy preserves spec; mutations to spec via
/// `/status` PUT must be discarded while status fields are persisted.
#[tokio::test]
async fn test_cronjob_strategy_status_subresource_preserves_spec() {
    let (mem, router) = spawn_router();
    let create_body = minimal_cronjob_stub("cj-status");

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/cronjobs"),
        &create_body,
    )
    .await;
    assert_eq!(status, 201);

    let mut stored = read_stored(&mem, "cronjobs", "cj-status").await;
    assert_eq!(
        stored.pointer("/spec/schedule").and_then(Value::as_str),
        Some("*/5 * * * *"),
        "precondition: schedule set by create"
    );

    // Forge body with mutated spec.schedule + new status payload.
    if let Some(spec) = stored.get_mut("spec").and_then(Value::as_object_mut) {
        spec.insert("schedule".to_string(), json!("@hourly"));
    }
    if let Some(obj) = stored.as_object_mut() {
        obj.insert(
            "status".to_string(),
            json!({
                "active": [],
                "lastScheduleTime": "2026-01-01T00:00:00Z"
            }),
        );
    }

    let (status, resp) = send_json(
        router,
        Method::PUT,
        &format!("/apis/batch/v1/namespaces/{TEST_NS}/cronjobs/cj-status/status"),
        &stored,
    )
    .await;
    assert_eq!(status, 200, "PUT cronjob/status: {resp}");

    let after = read_stored(&mem, "cronjobs", "cj-status").await;
    assert_eq!(
        after.pointer("/spec/schedule").and_then(Value::as_str),
        Some("*/5 * * * *"),
        "/status subresource must NOT alter spec.schedule"
    );
    assert_eq!(
        after
            .pointer("/status/lastScheduleTime")
            .and_then(Value::as_str),
        Some("2026-01-01T00:00:00Z"),
        "/status subresource must persist status fields"
    );
}
