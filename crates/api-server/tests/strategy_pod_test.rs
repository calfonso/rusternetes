//! Mirror of upstream Kubernetes v1.35
//! `pkg/registry/core/pod/strategy_test.go` semantics, exercised through the
//! in-process Axum router + `MemoryStorage`.
//!
//! Source (release-1.35):
//!   https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/registry/core/pod/strategy_test.go
//!
//! Upstream `strategy_test.go` exercises the pod REST strategy directly: it
//! constructs a `podStrategy` (and `podStatusStrategy`, `podBindingStrategy`),
//! invokes `PrepareForCreate` / `PrepareForUpdate` / `Validate` /
//! `ValidateUpdate`, and asserts on both the in-memory pod object and the
//! returned validation errors. We don't expose the strategy struct directly —
//! the same contract lives behind the HTTP handlers — so this file drives the
//! exact same scenarios through the real axum router via
//! `tower::ServiceExt::oneshot` against an `Arc<MemoryStorage>` and reads the
//! stored object back via `MemoryStorage::get` to assert on it.
//!
//! Scenarios mirrored:
//!
//! Create-time defaulting (`PrepareForCreate` → `SetDefaults_PodSpec` /
//! `SetDefaults_Container` in pkg/apis/core/v1/defaults.go):
//!   * `test_pod_strategy_default_restart_policy`
//!   * `test_pod_strategy_default_dns_policy`
//!   * `test_pod_strategy_default_image_pull_policy_if_not_present`
//!   * `test_pod_strategy_default_image_pull_policy_always_on_latest`
//!   * `test_pod_strategy_default_image_pull_policy_always_on_untagged`
//!   * `test_pod_strategy_default_termination_message_path`
//!   * `test_pod_strategy_default_termination_grace_period_seconds`
//!   * `test_pod_strategy_create_rejects_duplicate_container_names`
//!     (mirrors `ValidatePodSpec` duplicate-name check)
//!
//! Update-time immutability (`PrepareForUpdate` + `ValidateUpdate` →
//! `ValidatePodUpdate` in pkg/apis/core/validation/validation.go):
//!   * `test_pod_strategy_update_containers_image_mutable` — only `image`
//!     may change on `spec.containers[*]` post-create.
//!   * `test_pod_strategy_update_containers_command_immutable` — any other
//!     container field (here: `command`) is rejected.
//!   * `test_pod_strategy_update_node_name_immutable_once_set` — once a Pod
//!     has been bound (`spec.nodeName` set), the PUT path may not change it.
//!   * `test_pod_strategy_update_active_deadline_seconds_can_only_be_reduced`
//!     — `activeDeadlineSeconds` may only be reduced, never extended.
//!
//! Status subresource:
//!   * `test_pod_strategy_status_subresource_does_not_mutate_spec` —
//!     `PUT /status` must keep `spec` byte-equal.
//!   * `test_pod_strategy_main_resource_does_not_mutate_status` — `PUT` on
//!     the main resource must keep `status` byte-equal. `#[ignore]`d while
//!     the main pod UPDATE handler still accepts client-supplied status.
//!
//! Binding subresource:
//!   * `test_pod_strategy_binding_sets_node_name` — `POST /binding` writes
//!     `spec.nodeName` on the stored pod.
//!   * `test_pod_strategy_binding_can_only_be_done_once` — a second binding
//!     against an already-bound pod must be rejected (upstream Conflict /
//!     Forbidden depending on path). Currently the rusternetes binding
//!     handler overwrites silently, so this test is `#[ignore]`d until
//!     upstream parity ships.
//!
//! Per the unit's contract this file re-implements `make_state()` and
//! `spawn_router()` inline — they intentionally duplicate the helpers in
//! `integration_dryrun_all_resources.rs` so that this file remains a
//! single-file mirror of one upstream test file.

use axum::http::Method;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin `(u16, Value)` shims over the shared `TestApiServer`.
// `mem` is the backing store for direct post-prepare object assertions.
// ---------------------------------------------------------------------------

const TEST_NS: &str = "podstrategy";

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

async fn send_with_ct(
    router: &TestApiServer,
    method: Method,
    uri: &str,
    content_type: &str,
    body: &Value,
) -> (u16, Value) {
    let (status, value) = router
        .send(method.as_str(), uri, Some(content_type), Some(body))
        .await;
    (status.as_u16(), value)
}

async fn post_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    send_with_ct(router, Method::POST, uri, "application/json", body).await
}

async fn put_json(router: &TestApiServer, uri: &str, body: &Value) -> (u16, Value) {
    send_with_ct(router, Method::PUT, uri, "application/json", body).await
}

async fn get_json(router: &TestApiServer, uri: &str) -> (u16, Value) {
    let (status, value) = router.get(uri).await;
    (status.as_u16(), value)
}

/// Read the raw JSON object that was actually persisted under
/// `/registry/pods/{ns}/{name}` to verify storage-side state (mirrors the
/// upstream test pattern of inspecting the strategy's post-prepare object
/// directly rather than the response body).
async fn stored_pod(mem: &Arc<MemoryStorage>, name: &str) -> Value {
    let key = build_key("pods", Some(TEST_NS), name);
    mem.get::<Value>(&key)
        .await
        .expect("pod missing from storage")
}

/// Minimal pod fixture — matches the upstream `validNewPod()` helper:
/// one container, no other fields. The fixture deliberately omits all
/// defaultable fields (restartPolicy, dnsPolicy, terminationMessagePath,
/// terminationGracePeriodSeconds, imagePullPolicy) so the create-time
/// defaulting assertions exercise the actual `SetDefaults_PodSpec` path.
fn pod_body(name: &str, image: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": TEST_NS },
        "spec": {
            "containers": [{ "name": "ctr-a", "image": image }],
        }
    })
}

async fn create_namespace(router: &TestApiServer) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": TEST_NS },
    });
    let (status, body) = post_json(router, "/api/v1/namespaces", &body).await;
    assert!(
        status == 201 || status == 200,
        "namespace create must succeed: status={} body={}",
        status,
        body
    );
}

/// Create a pod through the handler and return the parsed response body. The
/// stored object is implicitly available via `stored_pod()`.
async fn create_pod(router: &TestApiServer, name: &str, image: &str) -> Value {
    let body = pod_body(name, image);
    let uri = format!("/api/v1/namespaces/{}/pods", TEST_NS);
    let (status, resp) = post_json(router, &uri, &body).await;
    assert!(
        status == 201 || status == 200,
        "pod create [{}] must succeed: status={} body={}",
        name,
        status,
        resp
    );
    resp
}

// ===========================================================================
// Section 1 — create-time defaulting (mirrors `SetDefaults_PodSpec` /
// `SetDefaults_Container` invocations from `PrepareForCreate`).
// ===========================================================================

/// Mirror of upstream `TestPodStrategy_DefaultRestartPolicy`: a Pod created
/// without `spec.restartPolicy` must default to `Always`.
#[tokio::test]
async fn test_pod_strategy_default_restart_policy() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;
    let resp = create_pod(&router, "default-restartpolicy", "nginx:1.27").await;

    // Response body assertion.
    assert_eq!(
        resp["spec"]["restartPolicy"].as_str(),
        Some("Always"),
        "response restartPolicy must default to Always: {}",
        resp
    );
    // Storage assertion.
    let stored = stored_pod(&mem, "default-restartpolicy").await;
    assert_eq!(
        stored["spec"]["restartPolicy"].as_str(),
        Some("Always"),
        "stored restartPolicy must default to Always: {}",
        stored
    );
}

/// Mirror of upstream `TestPodStrategy_DefaultDNSPolicy`: a Pod created
/// without `spec.dnsPolicy` must default to `ClusterFirst`.
#[tokio::test]
async fn test_pod_strategy_default_dns_policy() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;
    let resp = create_pod(&router, "default-dnspolicy", "nginx:1.27").await;
    assert_eq!(
        resp["spec"]["dnsPolicy"].as_str(),
        Some("ClusterFirst"),
        "response dnsPolicy must default to ClusterFirst: {}",
        resp
    );
    let stored = stored_pod(&mem, "default-dnspolicy").await;
    assert_eq!(
        stored["spec"]["dnsPolicy"].as_str(),
        Some("ClusterFirst"),
        "stored dnsPolicy must default to ClusterFirst: {}",
        stored
    );
}

/// Mirror of upstream `TestPodStrategy_DefaultImagePullPolicy`
/// (non-`:latest`, non-empty tag → `IfNotPresent`).
#[tokio::test]
async fn test_pod_strategy_default_image_pull_policy_if_not_present() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;
    let resp = create_pod(&router, "ipp-ifnotpresent", "nginx:1.27").await;
    assert_eq!(
        resp["spec"]["containers"][0]["imagePullPolicy"].as_str(),
        Some("IfNotPresent"),
        "response imagePullPolicy for tagged image must default to IfNotPresent: {}",
        resp
    );
    let stored = stored_pod(&mem, "ipp-ifnotpresent").await;
    assert_eq!(
        stored["spec"]["containers"][0]["imagePullPolicy"].as_str(),
        Some("IfNotPresent"),
        "stored imagePullPolicy for tagged image must default to IfNotPresent: {}",
        stored
    );
}

/// Mirror of upstream `TestPodStrategy_DefaultImagePullPolicy` (`:latest`
/// tag → `Always`).
#[tokio::test]
async fn test_pod_strategy_default_image_pull_policy_always_on_latest() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;
    let resp = create_pod(&router, "ipp-latest", "nginx:latest").await;
    assert_eq!(
        resp["spec"]["containers"][0]["imagePullPolicy"].as_str(),
        Some("Always"),
        "response imagePullPolicy for :latest image must default to Always: {}",
        resp
    );
    let stored = stored_pod(&mem, "ipp-latest").await;
    assert_eq!(
        stored["spec"]["containers"][0]["imagePullPolicy"].as_str(),
        Some("Always"),
        "stored imagePullPolicy for :latest image must default to Always: {}",
        stored
    );
}

/// Mirror of upstream `TestPodStrategy_DefaultImagePullPolicy` (no tag at all
/// → treated as `:latest` → `Always`).
#[tokio::test]
async fn test_pod_strategy_default_image_pull_policy_always_on_untagged() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;
    let resp = create_pod(&router, "ipp-untagged", "nginx").await;
    assert_eq!(
        resp["spec"]["containers"][0]["imagePullPolicy"].as_str(),
        Some("Always"),
        "response imagePullPolicy for untagged image must default to Always: {}",
        resp
    );
    let stored = stored_pod(&mem, "ipp-untagged").await;
    assert_eq!(
        stored["spec"]["containers"][0]["imagePullPolicy"].as_str(),
        Some("Always"),
        "stored imagePullPolicy for untagged image must default to Always: {}",
        stored
    );
}

/// Mirror of upstream `TestPodStrategy_DefaultTerminationMessagePath`: each
/// container's `terminationMessagePath` defaults to `/dev/termination-log`.
#[tokio::test]
async fn test_pod_strategy_default_termination_message_path() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;
    let resp = create_pod(&router, "default-termpath", "nginx:1.27").await;
    assert_eq!(
        resp["spec"]["containers"][0]["terminationMessagePath"].as_str(),
        Some("/dev/termination-log"),
        "response terminationMessagePath must default to /dev/termination-log: {}",
        resp
    );
    let stored = stored_pod(&mem, "default-termpath").await;
    assert_eq!(
        stored["spec"]["containers"][0]["terminationMessagePath"].as_str(),
        Some("/dev/termination-log"),
        "stored terminationMessagePath must default to /dev/termination-log: {}",
        stored
    );
}

/// Mirror of upstream `TestPodStrategy_DefaultTerminationGracePeriodSeconds`:
/// defaults to `30` when unset.
#[tokio::test]
async fn test_pod_strategy_default_termination_grace_period_seconds() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;
    let resp = create_pod(&router, "default-tgp", "nginx:1.27").await;
    assert_eq!(
        resp["spec"]["terminationGracePeriodSeconds"].as_i64(),
        Some(30),
        "response terminationGracePeriodSeconds must default to 30: {}",
        resp
    );
    let stored = stored_pod(&mem, "default-tgp").await;
    assert_eq!(
        stored["spec"]["terminationGracePeriodSeconds"].as_i64(),
        Some(30),
        "stored terminationGracePeriodSeconds must default to 30: {}",
        stored
    );
}

/// Mirror of upstream `ValidatePodSpec` duplicate container-name rule
/// (exercised from `PrepareForCreate`'s `Validate` call). The K8s API server
/// rejects pods with two containers sharing the same name with
/// `spec.containers[1].name: Duplicate value: "ctr-a"`.
#[tokio::test]
async fn test_pod_strategy_create_rejects_duplicate_container_names() {
    let (_, router) = spawn_router();
    create_namespace(&router).await;
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "dup-container-names", "namespace": TEST_NS },
        "spec": {
            "containers": [
                { "name": "ctr-a", "image": "nginx:1.27" },
                { "name": "ctr-a", "image": "nginx:1.28" },
            ],
        }
    });
    let uri = format!("/api/v1/namespaces/{}/pods", TEST_NS);
    let (status, resp) = post_json(&router, &uri, &body).await;
    assert!(
        (400..500).contains(&status),
        "duplicate container names must be rejected (4xx), got status={} body={}",
        status,
        resp
    );
}

// ===========================================================================
// Section 2 — update-time immutability (mirrors `ValidatePodUpdate` from
// pkg/apis/core/validation/validation.go).
// ===========================================================================

/// Mirror of upstream `TestPodStrategy_ValidateUpdate_ImageMutable`:
/// `spec.containers[*].image` is the only mutable container field on UPDATE.
/// A bare image swap (`nginx:1.27` → `nginx:1.28`) must be accepted.
#[tokio::test]
async fn test_pod_strategy_update_containers_image_mutable() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;

    let _created = create_pod(&router, "image-mutable", "nginx:1.27").await;
    let mut updated = pod_body("image-mutable", "nginx:1.28");
    // Keep the rest of the spec byte-identical to what was stored.
    let stored = stored_pod(&mem, "image-mutable").await;
    if let Some(spec) = stored["spec"].as_object() {
        for (k, v) in spec {
            if k == "containers" {
                continue;
            }
            updated["spec"][k] = v.clone();
        }
        // Container body: copy non-image fields from the stored container
        // so this PUT only ever differs in `image`.
        if let Some(c0) = spec.get("containers").and_then(|c| c.get(0)) {
            for (k, v) in c0.as_object().unwrap() {
                if k == "image" {
                    continue;
                }
                updated["spec"]["containers"][0][k] = v.clone();
            }
        }
    }

    let uri = format!("/api/v1/namespaces/{}/pods/image-mutable", TEST_NS);
    let (status, resp) = put_json(&router, &uri, &updated).await;
    assert!(
        status == 200 || status == 201,
        "image swap must be accepted (only-mutable field), got status={} body={}",
        status,
        resp
    );
    assert_eq!(
        resp["spec"]["containers"][0]["image"].as_str(),
        Some("nginx:1.28"),
        "response must reflect the new image: {}",
        resp
    );
    let stored = stored_pod(&mem, "image-mutable").await;
    assert_eq!(
        stored["spec"]["containers"][0]["image"].as_str(),
        Some("nginx:1.28"),
        "stored pod must reflect the new image: {}",
        stored
    );
}

/// Mirror of upstream `TestPodStrategy_ValidateUpdate_ContainerCommandImmutable`:
/// changing any non-image container field — here, `command` — is rejected by
/// `ValidatePodUpdate`.
#[tokio::test]
async fn test_pod_strategy_update_containers_command_immutable() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;

    let _ = create_pod(&router, "command-immutable", "nginx:1.27").await;

    // Build an update that flips `command` on container 0 (everything else
    // copied from storage so this is the SOLE diff).
    let stored = stored_pod(&mem, "command-immutable").await;
    let mut updated = stored.clone();
    updated["spec"]["containers"][0]["command"] = json!(["sh", "-c", "echo hi"]);
    // Strip resourceVersion so the server-side check is skipped (mirrors
    // `framework.Update` in upstream tests which doesn't carry it).
    if let Some(meta) = updated["metadata"].as_object_mut() {
        meta.remove("resourceVersion");
    }

    let uri = format!("/api/v1/namespaces/{}/pods/command-immutable", TEST_NS);
    let (status, resp) = put_json(&router, &uri, &updated).await;
    assert!(
        (400..500).contains(&status),
        "changing container[0].command must be rejected (4xx), got status={} body={}",
        status,
        resp
    );
}

/// Mirror of upstream `TestPodStrategy_ValidateUpdate_NodeNameImmutable`:
/// once `spec.nodeName` is set (typically via Binding), the regular PUT path
/// may not change it.
#[tokio::test]
async fn test_pod_strategy_update_node_name_immutable_once_set() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;

    let _ = create_pod(&router, "nodename-immutable", "nginx:1.27").await;

    // Bind the pod through the Binding subresource — this is the canonical
    // way to set nodeName from None → "node-a".
    let bind_uri = format!(
        "/api/v1/namespaces/{}/pods/nodename-immutable/binding",
        TEST_NS
    );
    let binding = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": { "name": "nodename-immutable", "namespace": TEST_NS },
        "target": { "kind": "Node", "name": "node-a" },
    });
    let (status, body) = post_json(&router, &bind_uri, &binding).await;
    assert!(
        status == 201 || status == 200,
        "binding must succeed: status={} body={}",
        status,
        body
    );
    let stored_after_bind = stored_pod(&mem, "nodename-immutable").await;
    assert_eq!(
        stored_after_bind["spec"]["nodeName"].as_str(),
        Some("node-a"),
        "binding must persist nodeName=node-a: {}",
        stored_after_bind
    );

    // Now attempt to PUT the pod with a different nodeName.
    let mut updated = stored_after_bind.clone();
    updated["spec"]["nodeName"] = json!("node-b");
    if let Some(meta) = updated["metadata"].as_object_mut() {
        meta.remove("resourceVersion");
    }
    let uri = format!("/api/v1/namespaces/{}/pods/nodename-immutable", TEST_NS);
    let (status, resp) = put_json(&router, &uri, &updated).await;
    assert!(
        (400..500).contains(&status),
        "changing spec.nodeName via PUT must be rejected (4xx), got status={} body={}",
        status,
        resp
    );

    // Storage must still reflect the original node binding.
    let stored = stored_pod(&mem, "nodename-immutable").await;
    assert_eq!(
        stored["spec"]["nodeName"].as_str(),
        Some("node-a"),
        "rejected PUT must not mutate stored nodeName: {}",
        stored
    );
}

/// Mirror of upstream `TestPodStrategy_ValidateUpdate_ActiveDeadlineSeconds`:
/// `activeDeadlineSeconds` may only be reduced once set.
#[tokio::test]
async fn test_pod_strategy_update_active_deadline_seconds_can_only_be_reduced() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;

    // Create with activeDeadlineSeconds=60.
    let mut body = pod_body("ads-update", "nginx:1.27");
    body["spec"]["activeDeadlineSeconds"] = json!(60);
    let uri_create = format!("/api/v1/namespaces/{}/pods", TEST_NS);
    let (status, resp) = post_json(&router, &uri_create, &body).await;
    assert!(
        status == 201 || status == 200,
        "create with activeDeadlineSeconds=60 must succeed: status={} body={}",
        status,
        resp
    );

    // Reduce to 30 — allowed.
    let mut reduced = stored_pod(&mem, "ads-update").await;
    reduced["spec"]["activeDeadlineSeconds"] = json!(30);
    if let Some(meta) = reduced["metadata"].as_object_mut() {
        meta.remove("resourceVersion");
    }
    let uri = format!("/api/v1/namespaces/{}/pods/ads-update", TEST_NS);
    let (status, resp) = put_json(&router, &uri, &reduced).await;
    assert!(
        status == 200 || status == 201,
        "reducing activeDeadlineSeconds must be accepted: status={} body={}",
        status,
        resp
    );
    let stored = stored_pod(&mem, "ads-update").await;
    assert_eq!(
        stored["spec"]["activeDeadlineSeconds"].as_i64(),
        Some(30),
        "reduction must persist: {}",
        stored
    );

    // Increase to 90 — rejected.
    let mut extended = stored;
    extended["spec"]["activeDeadlineSeconds"] = json!(90);
    if let Some(meta) = extended["metadata"].as_object_mut() {
        meta.remove("resourceVersion");
    }
    let (status, resp) = put_json(&router, &uri, &extended).await;
    assert!(
        (400..500).contains(&status),
        "extending activeDeadlineSeconds must be rejected (4xx), got status={} body={}",
        status,
        resp
    );
    let stored_after = stored_pod(&mem, "ads-update").await;
    assert_eq!(
        stored_after["spec"]["activeDeadlineSeconds"].as_i64(),
        Some(30),
        "rejected extension must not mutate storage: {}",
        stored_after
    );
}

// ===========================================================================
// Section 3 — status subresource (mirrors `podStatusStrategy` —
// `PrepareForUpdate` / `ValidateUpdate` on `/status`).
// ===========================================================================

/// Mirror of upstream `TestPodStrategy_StatusStrategy_PrepareForUpdate`:
/// PUT on `/status` must keep `spec` byte-equal to the stored spec, even
/// when the client tries to mutate it.
#[tokio::test]
async fn test_pod_strategy_status_subresource_does_not_mutate_spec() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;

    let _ = create_pod(&router, "status-noop-spec", "nginx:1.27").await;
    let before = stored_pod(&mem, "status-noop-spec").await;
    let spec_before = before["spec"].clone();

    // Client sends a status update plus a (forbidden) spec mutation; the
    // server must ignore the spec mutation.
    let status_update = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "status-noop-spec", "namespace": TEST_NS },
        "spec": {
            // Hostile spec mutation that must be IGNORED on the /status path.
            "containers": [{ "name": "tampered", "image": "evil:0.0.1" }],
        },
        "status": {
            "phase": "Running",
            "message": "all good",
        },
    });
    let uri = format!(
        "/api/v1/namespaces/{}/pods/status-noop-spec/status",
        TEST_NS
    );
    let (status, resp) = put_json(&router, &uri, &status_update).await;
    assert!(
        status == 200 || status == 201,
        "status update must succeed: status={} body={}",
        status,
        resp
    );

    // Status must reflect the update; spec must be byte-equal to before.
    let after = stored_pod(&mem, "status-noop-spec").await;
    assert_eq!(
        after["spec"], spec_before,
        "spec must NOT mutate on /status PUT; before={} after={}",
        spec_before, after["spec"],
    );
    assert_eq!(
        after["status"]["phase"].as_str(),
        Some("Running"),
        "status must reflect the /status PUT: {}",
        after
    );
}

/// Mirror of upstream `TestPodStrategy_PrepareForUpdate_DropsStatus`:
/// PUT on the main resource must keep `status` byte-equal to the stored
/// status — the status subresource is the only path that may mutate it.
///
/// Upstream `podStrategy.PrepareForUpdate` resets
/// `newPod.Status = oldPod.Status` before persisting; rusternetes' main
/// pod update handler currently accepts whatever status the client sends.
#[tokio::test]
async fn test_pod_strategy_main_resource_does_not_mutate_status() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;

    let _ = create_pod(&router, "main-noop-status", "nginx:1.27").await;

    // Seed status via the /status path first.
    let status_seed = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "main-noop-status", "namespace": TEST_NS },
        "status": { "phase": "Pending", "message": "scheduled" },
    });
    let status_uri = format!(
        "/api/v1/namespaces/{}/pods/main-noop-status/status",
        TEST_NS
    );
    let (st, body) = put_json(&router, &status_uri, &status_seed).await;
    assert!(
        st == 200 || st == 201,
        "seed status must succeed: status={} body={}",
        st,
        body
    );
    let stored = stored_pod(&mem, "main-noop-status").await;
    let status_before = stored["status"].clone();
    assert_eq!(
        status_before["phase"].as_str(),
        Some("Pending"),
        "seeded status must persist: {}",
        stored
    );

    // Now PUT the main resource with a (forbidden) status mutation.
    let mut updated = stored.clone();
    updated["status"] = json!({ "phase": "Running", "message": "tampered" });
    if let Some(meta) = updated["metadata"].as_object_mut() {
        meta.remove("resourceVersion");
    }
    let uri = format!("/api/v1/namespaces/{}/pods/main-noop-status", TEST_NS);
    let (status, resp) = put_json(&router, &uri, &updated).await;
    assert!(
        status == 200 || status == 201,
        "main PUT must succeed: status={} body={}",
        status,
        resp
    );

    let after = stored_pod(&mem, "main-noop-status").await;
    assert_eq!(
        after["status"]["phase"].as_str(),
        Some("Pending"),
        "status.phase must NOT mutate on main PUT; before={} after={}",
        status_before,
        after["status"],
    );
    assert_eq!(
        after["status"]["message"].as_str(),
        Some("scheduled"),
        "status.message must NOT mutate on main PUT; before={} after={}",
        status_before,
        after["status"],
    );
}

// ===========================================================================
// Section 4 — binding subresource (mirrors `BindingREST.Create` from
// pkg/registry/core/pod/storage/storage.go).
// ===========================================================================

/// Mirror of upstream `TestPodStrategy_Binding_SetsNodeName`: `POST` to
/// `/pods/{name}/binding` must set `spec.nodeName` to the target node.
#[tokio::test]
async fn test_pod_strategy_binding_sets_node_name() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;

    let _ = create_pod(&router, "bind-once", "nginx:1.27").await;
    let before = stored_pod(&mem, "bind-once").await;
    assert!(
        before["spec"]["nodeName"].as_str().is_none()
            || before["spec"]["nodeName"].as_str() == Some(""),
        "before binding the pod must be unbound: {}",
        before
    );

    let binding = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": { "name": "bind-once", "namespace": TEST_NS },
        "target": { "kind": "Node", "name": "worker-1" },
    });
    let uri = format!("/api/v1/namespaces/{}/pods/bind-once/binding", TEST_NS);
    let (status, resp) = post_json(&router, &uri, &binding).await;
    assert!(
        status == 201 || status == 200,
        "binding must succeed: status={} body={}",
        status,
        resp
    );

    // Response body shape mirrors upstream `Binding` echo.
    assert_eq!(resp["kind"].as_str(), Some("Binding"));
    assert_eq!(resp["target"]["name"].as_str(), Some("worker-1"));

    // Storage assertion — the pod must now carry spec.nodeName.
    let after = stored_pod(&mem, "bind-once").await;
    assert_eq!(
        after["spec"]["nodeName"].as_str(),
        Some("worker-1"),
        "binding must persist spec.nodeName: {}",
        after
    );

    // GET must echo the bound nodeName too.
    let get_uri = format!("/api/v1/namespaces/{}/pods/bind-once", TEST_NS);
    let (_st, fetched) = get_json(&router, &get_uri).await;
    assert_eq!(
        fetched["spec"]["nodeName"].as_str(),
        Some("worker-1"),
        "GET must reflect the binding: {}",
        fetched
    );
}

/// Mirror of upstream `TestPodStrategy_Binding_CanOnlyBeDoneOnce`: a second
/// `POST /binding` against an already-bound pod must be rejected. The
/// current rusternetes handler overwrites silently, so this test is
/// `#[ignore]`d until binding-once parity ships.
#[tokio::test]
async fn test_pod_strategy_binding_can_only_be_done_once() {
    let (mem, router) = spawn_router();
    create_namespace(&router).await;

    let _ = create_pod(&router, "bind-twice", "nginx:1.27").await;

    let binding_a = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": { "name": "bind-twice", "namespace": TEST_NS },
        "target": { "kind": "Node", "name": "worker-1" },
    });
    let uri = format!("/api/v1/namespaces/{}/pods/bind-twice/binding", TEST_NS);
    let (status, resp) = post_json(&router, &uri, &binding_a).await;
    assert!(
        status == 201 || status == 200,
        "first binding must succeed: status={} body={}",
        status,
        resp
    );

    let binding_b = json!({
        "apiVersion": "v1",
        "kind": "Binding",
        "metadata": { "name": "bind-twice", "namespace": TEST_NS },
        "target": { "kind": "Node", "name": "worker-2" },
    });
    let (status, resp) = post_json(&router, &uri, &binding_b).await;
    assert!(
        (400..500).contains(&status),
        "second binding must be rejected (4xx), got status={} body={}",
        status,
        resp
    );

    // Storage must still reflect the first binding.
    let after = stored_pod(&mem, "bind-twice").await;
    assert_eq!(
        after["spec"]["nodeName"].as_str(),
        Some("worker-1"),
        "rejected second binding must not mutate spec.nodeName: {}",
        after
    );
}
