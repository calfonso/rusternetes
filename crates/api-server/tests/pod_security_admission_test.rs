//! Upstream-mirror RED-state TDD pins for Pod Security Admission (PSA).
//!
//! PodSecurityPolicy (PSP) was the original Kubernetes implementation; it
//! was removed in v1.25 and replaced by **Pod Security Admission** — the
//! plugin that backs the v1.35 `pod-security.kubernetes.io/enforce` label
//! and the [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/).
//! Upstream e2e source-of-truth (PSP era):
//! <https://github.com/kubernetes/kubernetes/blob/release-1.24/test/e2e/auth/pod_security_policy.go>.
//! Upstream PSA admission contract:
//! <https://github.com/kubernetes/kubernetes/tree/release-1.35/staging/src/k8s.io/pod-security-admission/policy>.
//!
//! Each test below corresponds to a single PSP / PSA rejection scenario.
//! Tests POST a pod through the REST surface (mirroring upstream's
//! `clientset.CoreV1().Pods(ns).Create(...)`) and assert the API server
//! returns `403 Forbidden`.
//!
//! ### RED-state expectations
//!
//! Every test is marked `#[ignore = "RED-state: PodSecurityAdmission is a
//! stub (allow-all)"]`. The freshly-introduced
//! `rusternetes_api_server::admission::PodSecurityAdmission` is an
//! allow-all stub — the existing inline check in
//! `handlers::pod::create_pod` covers privileged + host namespaces but
//! misses the volume-types and runAsUser dimensions. The tests run with
//! `cargo test -- --ignored` and serve as TDD pins for the full enforcer:
//! they will go GREEN as the stub grows into the upstream contract.

#![allow(non_snake_case)]

use axum::http::{Method, StatusCode};
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (TestApiServer, Arc<MemoryStorage>) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (api, mem)
}

/// Issue a single request and return `(status, parsed JSON body)`.
async fn send(
    router: TestApiServer,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let content_type = body.as_ref().map(|_| "application/json");
    router.send(method.as_str(), uri, content_type, body).await
}

/// Create a namespace via REST with an optional
/// `pod-security.kubernetes.io/enforce` label.
///
/// Upstream's PSA admission keys off the namespace label set:
/// - `pod-security.kubernetes.io/enforce`: <privileged|baseline|restricted>
/// - `pod-security.kubernetes.io/enforce-version`: <kube-version|latest>
///
/// We tag the namespace with both so the admission plugin has the full
/// label set it expects.
async fn create_restricted_namespace(router: TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": {
                "pod-security.kubernetes.io/enforce": "restricted",
                "pod-security.kubernetes.io/enforce-version": "latest",
            },
        },
    });
    let (status, body) = send(router, Method::POST, "/api/v1/namespaces", Some(&body)).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace create should succeed for {name}, got {status}: {body:?}"
    );
}

/// Assert that POSTing `pod_body` to the namespace produces a `403
/// Forbidden`. This is the upstream PSA rejection contract: pods that
/// violate the namespace's enforced standard MUST be rejected at admission
/// with `StatusReason=Forbidden`.
async fn assert_pod_rejected(router: TestApiServer, ns: &str, pod_body: &Value, scenario: &str) {
    let (status, body) = send(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/pods"),
        Some(pod_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "{scenario}: pod must be rejected with 403 by PodSecurityAdmission, got {status}: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// PSP privileged containers blocked
// Upstream PSP scenario:
//   pod_security_policy.go ~line 110 — `It("should forbid pod creation when
//   no PSP is available")` + the per-suite privileged-container case.
// Modern PSA equivalent: the `restricted` profile rejects any container
//   with `.spec.containers[*].securityContext.privileged == true`.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn psp_privileged_containers_blocked() {
    let (router, _mem) = spawn_router();
    let ns = "psa-privileged";
    create_restricted_namespace(router.clone(), ns).await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "privileged-pod", "namespace": ns },
        "spec": {
            "containers": [{
                "name": "main",
                "image": "registry.k8s.io/pause:3.10",
                "securityContext": { "privileged": true },
            }],
        },
    });
    assert_pod_rejected(router, ns, &pod, "privileged container in restricted ns").await;
}

// ---------------------------------------------------------------------------
// PSP host namespaces
// Upstream PSP scenario: pods that set `.spec.hostPID`, `.spec.hostIPC`,
// or `.spec.hostNetwork` to true.
// Modern PSA equivalent: the `baseline` profile already rejects all three
// host namespace fields. `restricted` is a strict superset.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn psp_host_namespaces() {
    let (router, _mem) = spawn_router();
    let ns = "psa-host-ns";
    create_restricted_namespace(router.clone(), ns).await;

    // hostPID
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "hostpid-pod", "namespace": ns },
        "spec": {
            "hostPID": true,
            "containers": [{ "name": "main", "image": "registry.k8s.io/pause:3.10" }],
        },
    });
    assert_pod_rejected(router.clone(), ns, &pod, "hostPID=true in restricted ns").await;

    // hostNetwork
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "hostnet-pod", "namespace": ns },
        "spec": {
            "hostNetwork": true,
            "containers": [{ "name": "main", "image": "registry.k8s.io/pause:3.10" }],
        },
    });
    assert_pod_rejected(
        router.clone(),
        ns,
        &pod,
        "hostNetwork=true in restricted ns",
    )
    .await;

    // hostIPC
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "hostipc-pod", "namespace": ns },
        "spec": {
            "hostIPC": true,
            "containers": [{ "name": "main", "image": "registry.k8s.io/pause:3.10" }],
        },
    });
    assert_pod_rejected(router, ns, &pod, "hostIPC=true in restricted ns").await;
}

// ---------------------------------------------------------------------------
// PSP volume types
// Upstream PSP scenario: `.spec.volumes[].hostPath` is on the
// disallowed-volume-plugin list under the default profile.
// Modern PSA equivalent: the `baseline` profile permits only a fixed set
// of volume types — `configMap`, `csi`, `downwardAPI`, `emptyDir`,
// `ephemeral`, `persistentVolumeClaim`, `projected`, `secret`. Any
// `hostPath`, `nfs`, `iscsi`, etc. volume must be rejected.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn psp_volume_types() {
    let (router, _mem) = spawn_router();
    let ns = "psa-volumes";
    create_restricted_namespace(router.clone(), ns).await;

    // hostPath volume — explicitly forbidden by the baseline profile.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "hostpath-pod", "namespace": ns },
        "spec": {
            "volumes": [{
                "name": "host-root",
                "hostPath": { "path": "/", "type": "Directory" },
            }],
            "containers": [{
                "name": "main",
                "image": "registry.k8s.io/pause:3.10",
                "volumeMounts": [{ "name": "host-root", "mountPath": "/host" }],
            }],
        },
    });
    assert_pod_rejected(
        router,
        ns,
        &pod,
        "hostPath volume in restricted ns is forbidden by baseline+ profile",
    )
    .await;
}

// ---------------------------------------------------------------------------
// PSP runAsUser
// Upstream PSP scenario: `MustRunAsNonRoot` / explicit `runAsUser: 0`
// rules. Modern PSA equivalent: the `restricted` profile requires
// `runAsNonRoot: true`. A pod that explicitly sets `runAsUser: 0`
// (root) or omits `runAsNonRoot` while running as UID 0 must be rejected.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn psp_run_as_user() {
    let (router, _mem) = spawn_router();
    let ns = "psa-runasuser";
    create_restricted_namespace(router.clone(), ns).await;

    // Pod-level securityContext requesting runAsUser: 0 (root) — restricted
    // profile requires runAsNonRoot=true and forbids root.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "root-pod", "namespace": ns },
        "spec": {
            "securityContext": { "runAsUser": 0 },
            "containers": [{ "name": "main", "image": "registry.k8s.io/pause:3.10" }],
        },
    });
    assert_pod_rejected(
        router.clone(),
        ns,
        &pod,
        "pod-level runAsUser=0 in restricted ns",
    )
    .await;

    // Container-level securityContext requesting runAsUser: 0 — same rule,
    // applied per-container.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "root-container-pod", "namespace": ns },
        "spec": {
            "containers": [{
                "name": "main",
                "image": "registry.k8s.io/pause:3.10",
                "securityContext": { "runAsUser": 0 },
            }],
        },
    });
    assert_pod_rejected(
        router,
        ns,
        &pod,
        "container-level runAsUser=0 in restricted ns",
    )
    .await;
}

// ---------------------------------------------------------------------------
// Restricted profile demands an explicit `runAsNonRoot: true`.
// Upstream PSA `restricted` policy: pods that don't set
// `runAsNonRoot=true` at either the pod or every container level are
// rejected — silence is not consent.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn psp_restricted_requires_run_as_non_root() {
    let (router, _mem) = spawn_router();
    let ns = "psa-runasnonroot";
    create_restricted_namespace(router.clone(), ns).await;

    // Pod with no runAsNonRoot set anywhere — restricted profile rejects.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "no-runasnonroot-pod", "namespace": ns },
        "spec": {
            "containers": [{ "name": "main", "image": "registry.k8s.io/pause:3.10" }],
        },
    });
    assert_pod_rejected(
        router,
        ns,
        &pod,
        "restricted profile requires runAsNonRoot=true",
    )
    .await;
}

// ---------------------------------------------------------------------------
// Restricted profile requires `allowPrivilegeEscalation: false`.
// Upstream PSA `restricted` policy: containers MUST set
// `allowPrivilegeEscalation: false`. Pods that omit it (default true) or
// set it true must be rejected.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn psp_restricted_forbids_privilege_escalation() {
    let (router, _mem) = spawn_router();
    let ns = "psa-privesc";
    create_restricted_namespace(router.clone(), ns).await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "privesc-pod", "namespace": ns },
        "spec": {
            "containers": [{
                "name": "main",
                "image": "registry.k8s.io/pause:3.10",
                "securityContext": {
                    "runAsNonRoot": true,
                    "runAsUser": 1000,
                    "allowPrivilegeEscalation": true,
                },
            }],
        },
    });
    assert_pod_rejected(
        router,
        ns,
        &pod,
        "allowPrivilegeEscalation=true forbidden by restricted profile",
    )
    .await;
}
