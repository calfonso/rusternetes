// Conformance: in-place pod / container resize resource shape.
//
// Upstream references (k8s v1.35):
//   - `test/e2e/common/node/pod_resize.go`
//   - `test/e2e/common/node/pod_level_resources_resize.go`
//   - `staging/src/k8s.io/api/core/v1/types.go::ContainerResizePolicy`
//     and `Pod.status.resize`
//
// Pins the wire shape of:
//   - `Container.resizePolicy[*].resourceName` / `restartPolicy`
//   - `Pod.status.resize` (string enum: `Proposed`, `InProgress`,
//     `Deferred`, `Infeasible`, or empty)
//
// kubelet's `reconcile_pod()` reads `status.resize` to drive the in-place
// resize state machine; api-server's `/resize` subresource handler writes
// it. A drift on either side breaks the resize feature.

use rusternetes_common::resources::pod::ContainerResizePolicy;
use rusternetes_common::resources::{Container, Pod, PodSpec, PodStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};

fn container_with_resize_policy(name: &str, rp: Vec<ContainerResizePolicy>) -> Container {
    Container {
        name: name.to_string(),
        image: "nginx".to_string(),
        resize_policy: if rp.is_empty() { None } else { Some(rp) },
        image_pull_policy: None,
        command: None,
        args: None,
        ports: None,
        env: None,
        env_from: None,
        volume_mounts: None,
        volume_devices: None,
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
        resources: None,
        working_dir: None,
        security_context: None,
        restart_policy: None,
        lifecycle: None,
        termination_message_path: None,
        termination_message_policy: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        ..Default::default()
    }
}

fn pod_with_resize_status(status: Option<&str>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("p").with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![],
            init_containers: None,
            ephemeral_containers: None,
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            node_name: None,
            volumes: None,
            affinity: None,
            tolerations: None,
            service_account_name: None,
            service_account: None,
            priority: None,
            priority_class_name: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: None,
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: status.map(str::to_string),
            resource_claim_statuses: None,
            observed_generation: None,
            conditions: None,
            ..Default::default()
        }),
    }
}

// ---------------------------------------------------------------------------
// container.resizePolicy[*]
// ---------------------------------------------------------------------------

#[test]
fn resize_policy_cpu_not_required_serializes_camel_case() {
    let c = container_with_resize_policy(
        "c",
        vec![ContainerResizePolicy {
            resource_name: "cpu".to_string(),
            restart_policy: "NotRequired".to_string(),
        }],
    );
    let v = serde_json::to_value(&c).unwrap();
    assert_eq!(v["resizePolicy"][0]["resourceName"], "cpu");
    assert_eq!(v["resizePolicy"][0]["restartPolicy"], "NotRequired");
}

// ---------------------------------------------------------------------------
// pod.status.resize — the four upstream string states + empty/omit
// ---------------------------------------------------------------------------

#[test]
fn pod_status_resize_accepts_all_upstream_states() {
    // The four states kubelet's reconcile_pod() drives the in-place resize
    // state machine through. Drift on any of these strings breaks every
    // upstream resize test at the status assertion.
    for state in ["Proposed", "InProgress", "Deferred", "Infeasible"] {
        let p = pod_with_resize_status(Some(state));
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["status"]["resize"], state);

        let decoded: Pod = serde_json::from_value(v).unwrap();
        assert_eq!(decoded.status.unwrap().resize.as_deref(), Some(state));
    }
}
