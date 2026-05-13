//! Idempotency regression tests for the kubelet status-publish hot-loop.
//!
//! Background: during a Kubernetes v1.35 conformance run a single pod
//! (`kubectl-1863/e2e-test-agnhost-pod`, Phase=Succeeded) was MODIFIED at
//! resourceVersion 897500+ continuously — a feedback loop. Root cause:
//! `kubelet::sync_pod` re-derived terminal-pod status on every reconcile
//! and wrote it back unconditionally, emitting a MODIFIED watch event each
//! cycle. Fix: gate each terminal-pod `storage.update` with `pod_status_equal`.
//!
//! These tests pin the equality predicate so a future regression that
//! breaks the gate would fail here.

use rusternetes_common::resources::{
    Container, ContainerState, ContainerStatus, Pod, PodSpec, PodStatus,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_kubelet::kubelet::pod_status_equal;

fn make_succeeded_pod() -> Pod {
    let init_status = ContainerStatus {
        name: "init-0".to_string(),
        ready: true,
        restart_count: 0,
        state: Some(ContainerState::Terminated {
            exit_code: 0,
            signal: None,
            reason: Some("Completed".to_string()),
            message: None,
            started_at: Some("2026-05-13T10:00:00Z".to_string()),
            finished_at: Some("2026-05-13T10:00:01Z".to_string()),
            container_id: Some("docker://init0".to_string()),
        }),
        last_state: None,
        image: Some("busybox:1.36".to_string()),
        image_id: Some("docker-pullable://sha256:initimage".to_string()),
        container_id: Some("docker://init0".to_string()),
        started: Some(true),
        allocated_resources: None,
        allocated_resources_status: None,
        resources: None,
        user: None,
        volume_mounts: None,
        stop_signal: None,
    };
    let app_status = ContainerStatus {
        name: "agnhost".to_string(),
        ready: false,
        restart_count: 0,
        state: Some(ContainerState::Terminated {
            exit_code: 0,
            signal: None,
            reason: Some("Completed".to_string()),
            message: None,
            started_at: Some("2026-05-13T10:00:02Z".to_string()),
            finished_at: Some("2026-05-13T10:00:05Z".to_string()),
            container_id: Some("docker://app0".to_string()),
        }),
        last_state: None,
        image: Some("registry.k8s.io/e2e-test-images/agnhost:2.40".to_string()),
        image_id: Some("docker-pullable://sha256:agnimage".to_string()),
        container_id: Some("docker://app0".to_string()),
        started: Some(true),
        allocated_resources: None,
        allocated_resources_status: None,
        resources: None,
        user: None,
        volume_mounts: None,
        stop_signal: None,
    };
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("e2e-test-agnhost-pod").with_namespace("kubectl-1863"),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "agnhost".to_string(),
                image: "registry.k8s.io/e2e-test-images/agnhost:2.40".to_string(),
                ..Default::default()
            }],
            init_containers: Some(vec![Container {
                name: "init-0".to_string(),
                image: "busybox:1.36".to_string(),
                ..Default::default()
            }]),
            restart_policy: Some("Never".to_string()),
            node_name: Some("node-1".to_string()),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Succeeded),
            message: Some("Pod completed successfully".to_string()),
            host_ip: Some("127.0.0.1".to_string()),
            pod_ip: Some("10.244.0.5".to_string()),
            init_container_statuses: Some(vec![init_status]),
            container_statuses: Some(vec![app_status]),
            ..Default::default()
        }),
    }
}

#[test]
fn pod_status_equal_identical_pods() {
    let a = make_succeeded_pod();
    let b = a.clone();
    assert!(
        pod_status_equal(&a, &b),
        "identical Succeeded pod clones must compare equal"
    );
}

#[test]
fn pod_status_equal_detects_phase_change() {
    let a = make_succeeded_pod();
    let mut b = a.clone();
    if let Some(ref mut st) = b.status {
        st.phase = Some(Phase::Failed);
        st.message = Some("Pod failed".to_string());
    }
    assert!(
        !pod_status_equal(&a, &b),
        "phase Succeeded -> Failed must be detected as a real change"
    );
}

#[test]
fn pod_status_equal_detects_container_status_change() {
    let a = make_succeeded_pod();
    let mut b = a.clone();
    if let Some(ref mut st) = b.status {
        if let Some(ref mut css) = st.container_statuses {
            if let Some(cs) = css.first_mut() {
                cs.restart_count += 1;
            }
        }
    }
    assert!(
        !pod_status_equal(&a, &b),
        "restart count bump must be detected as a change"
    );
}

#[test]
fn pod_status_equal_ignores_metadata_changes() {
    let a = make_succeeded_pod();
    let mut b = a.clone();
    b.metadata.resource_version = Some("999999".to_string());
    b.metadata
        .labels
        .get_or_insert_with(std::collections::HashMap::new)
        .insert("k8s-app".to_string(), "kube-dns".to_string());
    assert!(
        pod_status_equal(&a, &b),
        "metadata-only changes must NOT trigger a status republish"
    );
}

#[test]
fn pod_status_equal_handles_none_status() {
    let mut a = make_succeeded_pod();
    let mut b = a.clone();
    a.status = None;
    b.status = None;
    assert!(
        pod_status_equal(&a, &b),
        "two None statuses must compare equal"
    );
    b.status = Some(PodStatus::default());
    assert!(
        !pod_status_equal(&a, &b),
        "None vs Some(default) must be detected"
    );
}
