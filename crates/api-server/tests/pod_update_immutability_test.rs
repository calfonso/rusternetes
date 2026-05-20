//! Router-driven tests for the pod-update immutability fence.
//!
//! Exercises both `PUT /api/v1/namespaces/:ns/pods/:name` (update) and
//! `PUT .../pods/:name/ephemeralcontainers` (subresource) through the real
//! Axum router via `tower::ServiceExt::oneshot`, asserting that
//! `validate_pod_spec_update` accepts the K8s-allowed mutations and rejects
//! everything else. Mirrors upstream
//! `pkg/apis/core/validation/validation.go::ValidatePodUpdate` (release-1.35).

use axum::{body::Body, http::Request};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::resources::{
    Container, EphemeralContainer, Pod, PodSchedulingGate, PodSecurityContext, PodSpec, Toleration,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true, // skip_auth
    ))
}

fn make_container(name: &str, image: &str) -> Container {
    Container {
        name: name.to_string(),
        image: image.to_string(),
        ..Default::default()
    }
}

/// Baseline pod stored in MemoryStorage. Spec fields are intentionally
/// populated for every immutable lane the tests poke at: 1 container,
/// 1 toleration, 1 scheduling gate, TGPS=30, ADS=600, securityContext set.
fn baseline_pod() -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "p1".to_string(),
            namespace: Some("default".to_string()),
            uid: "uid-p1".to_string(),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![make_container("c1", "nginx:1.25")],
            tolerations: Some(vec![Toleration {
                key: Some("k1".to_string()),
                operator: Some("Exists".to_string()),
                value: None,
                effect: None,
                toleration_seconds: None,
            }]),
            scheduling_gates: Some(vec![PodSchedulingGate {
                name: "gate-1".to_string(),
            }]),
            termination_grace_period_seconds: Some(30),
            active_deadline_seconds: Some(600),
            restart_policy: Some("Always".to_string()),
            dns_policy: Some("ClusterFirst".to_string()),
            host_network: Some(false),
            service_account_name: Some("default".to_string()),
            security_context: Some(PodSecurityContext::default()),
            ..Default::default()
        }),
        status: None,
    }
}

async fn seed(state: &Arc<ApiServerState>, pod: &Pod) {
    let key = build_key(
        "pods",
        pod.metadata.namespace.as_deref(),
        &pod.metadata.name,
    );
    state.storage.create(&key, pod).await.expect("seed pod");
}

/// Send PUT for the regular update path. Returns (status, body json).
async fn put_pod(state: Arc<ApiServerState>, pod: &Pod) -> (u16, Value) {
    let router = build_router(state, None);
    let body = serde_json::to_vec(pod).unwrap();
    let req = Request::builder()
        .method("PUT")
        .uri(format!(
            "/api/v1/namespaces/{}/pods/{}",
            pod.metadata.namespace.as_deref().unwrap_or("default"),
            pod.metadata.name
        ))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// Send PUT for the /ephemeralcontainers subresource.
async fn put_ephemeralcontainers(state: Arc<ApiServerState>, pod: &Pod) -> (u16, Value) {
    let router = build_router(state, None);
    let body = serde_json::to_vec(pod).unwrap();
    let req = Request::builder()
        .method("PUT")
        .uri(format!(
            "/api/v1/namespaces/{}/pods/{}/ephemeralcontainers",
            pod.metadata.namespace.as_deref().unwrap_or("default"),
            pod.metadata.name
        ))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

fn assert_rejected(status: u16, body: &Value, needle: &str) {
    assert!(status >= 400, "expected 4xx, got {} body={}", status, body);
    let msg = body.get("message").and_then(|m| m.as_str()).unwrap_or("");
    assert!(
        msg.contains(needle),
        "expected error message to contain {:?}, got: {}",
        needle,
        msg
    );
}

// ---------------------------------------------------------------------------
// Allowed mutations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_update_image_change_accepted() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().containers[0].image = "nginx:1.26".to_string();
    let (status, body) = put_pod(state, &new_pod).await;
    assert_eq!(status, 200, "image change must be accepted; body={}", body);
}

#[tokio::test]
async fn test_update_ads_decrease_accepted() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().active_deadline_seconds = Some(300);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_eq!(status, 200, "ADS reduction must be accepted; body={}", body);
}

#[tokio::test]
async fn test_update_toleration_addition_accepted() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().tolerations = Some(vec![
        Toleration {
            key: Some("k1".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        },
        Toleration {
            key: Some("k2".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        },
    ]);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_eq!(
        status, 200,
        "toleration add must be accepted; body={}",
        body
    );
}

#[tokio::test]
async fn test_update_scheduling_gate_deletion_accepted() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().scheduling_gates = Some(vec![]);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_eq!(status, 200, "gate deletion must be accepted; body={}", body);
}

#[tokio::test]
async fn test_update_tgps_negative_to_one_accepted() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let mut pod = baseline_pod();
    pod.spec.as_mut().unwrap().termination_grace_period_seconds = Some(-5);
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod
        .spec
        .as_mut()
        .unwrap()
        .termination_grace_period_seconds = Some(1);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_eq!(
        status, 200,
        "TGPS negative->1 must be accepted; body={}",
        body
    );
}

#[tokio::test]
async fn test_ephemeral_container_add_via_subresource_accepted() {
    // Critical regression test for the EC munge-fix in validation::pod.
    // Without `is_ephemeral_subresource=true` resetting ephemeral_containers
    // in the munged copy, the fence would reject this legitimate addition.
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().ephemeral_containers = Some(vec![EphemeralContainer {
        name: "debug".to_string(),
        image: "busybox:1.36".to_string(),
        command: None,
        args: None,
        working_dir: None,
        env: None,
        volume_mounts: None,
        image_pull_policy: None,
        security_context: None,
        target_container_name: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        resize_policy: None,
        restart_policy: None,
        resources: None,
        termination_message_path: None,
        termination_message_policy: None,
    }]);
    let (status, body) = put_ephemeralcontainers(state, &new_pod).await;
    assert_eq!(
        status, 200,
        "EC add via subresource must be accepted (regression for munge-fix); body={}",
        body
    );
}

// ---------------------------------------------------------------------------
// Rejected mutations — precise pre-checks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_update_container_count_added_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod
        .spec
        .as_mut()
        .unwrap()
        .containers
        .push(make_container("c2", "nginx:1.25"));
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(status, &body, "may not add or remove containers");
}

#[tokio::test]
async fn test_update_container_count_removed_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let mut pod = baseline_pod();
    pod.spec
        .as_mut()
        .unwrap()
        .containers
        .push(make_container("c2", "nginx:1.25"));
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().containers.pop();
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(status, &body, "may not add or remove containers");
}

#[tokio::test]
async fn test_update_ads_increase_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().active_deadline_seconds = Some(1200);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(status, &body, "must be less than or equal to");
}

#[tokio::test]
async fn test_update_ads_positive_to_nil_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().active_deadline_seconds = None;
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(status, &body, "must not be removed");
}

#[tokio::test]
async fn test_update_toleration_removal_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().tolerations = Some(vec![]);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "existing tolerations may not be modified or removed",
    );
}

#[tokio::test]
async fn test_update_scheduling_gate_addition_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().scheduling_gates = Some(vec![
        PodSchedulingGate {
            name: "gate-1".to_string(),
        },
        PodSchedulingGate {
            name: "gate-new".to_string(),
        },
    ]);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(status, &body, "only deletion is allowed");
}

#[tokio::test]
async fn test_update_tgps_arbitrary_change_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod
        .spec
        .as_mut()
        .unwrap()
        .termination_grace_period_seconds = Some(60);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(status, &body, "field is immutable");
}

// ---------------------------------------------------------------------------
// Rejected mutations — broad munge fence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_update_node_name_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().node_name = Some("worker-1".to_string());
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "pod updates may not change fields other than",
    );
}

#[tokio::test]
async fn test_update_host_network_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().host_network = Some(true);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "pod updates may not change fields other than",
    );
}

#[tokio::test]
async fn test_update_dns_policy_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().dns_policy = Some("Default".to_string());
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "pod updates may not change fields other than",
    );
}

#[tokio::test]
async fn test_update_restart_policy_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().restart_policy = Some("Never".to_string());
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "pod updates may not change fields other than",
    );
}

#[tokio::test]
async fn test_update_service_account_name_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().service_account_name = Some("alt-sa".to_string());
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "pod updates may not change fields other than",
    );
}

#[tokio::test]
async fn test_update_security_context_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().security_context = Some(PodSecurityContext {
        run_as_user: Some(1234),
        ..Default::default()
    });
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "pod updates may not change fields other than",
    );
}

#[tokio::test]
async fn test_update_container_body_except_image_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().containers[0].command =
        Some(vec!["sleep".to_string(), "infinity".to_string()]);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "pod updates may not change fields other than",
    );
}

/// Direct mirror of the upstream `[Conformance]` step at
/// `test/e2e/apimachinery/resource_quota.go:544` —
/// `framework.ConformanceIt("should create a ResourceQuota and capture
/// the life of a pod.")` includes the assertion:
///
/// > "Ensuring a pod cannot update its resource requirements" — a pod
/// > cannot dynamically update its resource requirements.
///
/// Plain PUT mutation of `containers[*].resources` must be rejected by
/// the immutability fence. (Legitimate in-place resize goes via the
/// `/resize` subresource, covered by
/// `resource_quota_captures_full_pod_lifecycle` in
/// `conformance_apimachinery_namespaces_quota_limits.rs`.)
#[tokio::test]
async fn test_update_container_resources_via_plain_put_rejected() {
    use rusternetes_common::types::ResourceRequirements;
    use std::collections::HashMap;

    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let mut pod = baseline_pod();
    let mut req = HashMap::new();
    req.insert("cpu".to_string(), "100m".to_string());
    req.insert("memory".to_string(), "100Mi".to_string());
    pod.spec.as_mut().unwrap().containers[0].resources = Some(ResourceRequirements {
        requests: Some(req),
        limits: None,
        claims: None,
    });
    seed(&state, &pod).await;

    // Mutate the requests — bump CPU. Upstream rejects with the
    // "may not change fields other than ..." error from ValidatePodUpdate.
    let mut new_pod = pod.clone();
    let mut new_req = HashMap::new();
    new_req.insert("cpu".to_string(), "200m".to_string());
    new_req.insert("memory".to_string(), "100Mi".to_string());
    new_pod.spec.as_mut().unwrap().containers[0].resources = Some(ResourceRequirements {
        requests: Some(new_req),
        limits: None,
        claims: None,
    });
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "pod updates may not change fields other than",
    );
}

// ---------------------------------------------------------------------------
// Ephemeral containers — subresource semantics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ephemeral_container_remove_via_subresource_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let mut pod = baseline_pod();
    pod.spec.as_mut().unwrap().ephemeral_containers = Some(vec![EphemeralContainer {
        name: "debug".to_string(),
        image: "busybox:1.36".to_string(),
        command: None,
        args: None,
        working_dir: None,
        env: None,
        volume_mounts: None,
        image_pull_policy: None,
        security_context: None,
        target_container_name: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        resize_policy: None,
        restart_policy: None,
        resources: None,
        termination_message_path: None,
        termination_message_policy: None,
    }]);
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().ephemeral_containers = Some(vec![]);
    let (status, body) = put_ephemeralcontainers(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "existing ephemeral containers may not be removed",
    );
}

#[tokio::test]
async fn test_ephemeral_container_on_main_path_rejected() {
    let mem = Arc::new(MemoryStorage::new());
    let state = make_state(mem);
    let pod = baseline_pod();
    seed(&state, &pod).await;

    let mut new_pod = pod.clone();
    new_pod.spec.as_mut().unwrap().ephemeral_containers = Some(vec![EphemeralContainer {
        name: "debug".to_string(),
        image: "busybox:1.36".to_string(),
        command: None,
        args: None,
        working_dir: None,
        env: None,
        volume_mounts: None,
        image_pull_policy: None,
        security_context: None,
        target_container_name: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        resize_policy: None,
        restart_policy: None,
        resources: None,
        termination_message_path: None,
        termination_message_policy: None,
    }]);
    let (status, body) = put_pod(state, &new_pod).await;
    assert_rejected(
        status,
        &body,
        "may not be updated outside of the ephemeralcontainers subresource",
    );
}
