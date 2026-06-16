//! Conformance: pod-level resource accounting + QoS edge cases.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/pod_level_resources.go`
//!   - `pkg/apis/core/v1/helper/qos/qos.go` — QoSClass derivation.
//!
//! These tests pin QoS-class computation across **multi-container** pods,
//! partial-spec edge cases (limits missing CPU vs memory), and serde
//! round-tripping of the optional `spec.resources` (PodLevelResources)
//! field on `PodSpec`. The single-container Guaranteed/Burstable/BestEffort
//! paths are already covered in `eviction_test.rs`.
//!
//! All tests are pure (no storage, no runtime).

use rusternetes_common::resources::{Container, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
use rusternetes_kubelet::eviction::{get_qos_class, QoSClass};
use std::collections::HashMap;

fn rr(req: &[(&str, &str)], lim: &[(&str, &str)]) -> ResourceRequirements {
    let to_map = |kv: &[(&str, &str)]| {
        if kv.is_empty() {
            None
        } else {
            Some(
                kv.iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect::<HashMap<_, _>>(),
            )
        }
    };
    ResourceRequirements {
        requests: to_map(req),
        limits: to_map(lim),
        claims: None,
    }
}

fn container(name: &str, resources: Option<ResourceRequirements>) -> Container {
    Container {
        name: name.to_string(),
        image: "nginx:latest".to_string(),
        resources,
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
        working_dir: None,
        security_context: None,
        restart_policy: None,
        resize_policy: None,
        lifecycle: None,
        termination_message_path: None,
        termination_message_policy: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        ..Default::default()
    }
}

fn pod_with_containers(name: &str, containers: Vec<Container>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers,
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
        status: None,
    }
}

// ---------------------------------------------------------------------------
// Multi-container QoS — one weak container drags whole pod down
// ---------------------------------------------------------------------------

#[test]
fn multi_container_one_besteffort_drops_pod_to_burstable() {
    // Container A is Guaranteed (limits == requests for cpu+memory).
    // Container B has no resources at all.
    // Pod-level QoS must be Burstable per K8s `qos.go`:
    // any container with requests/limits but not full match → not Guaranteed.
    let g = container(
        "a",
        Some(rr(
            &[("cpu", "100m"), ("memory", "128Mi")],
            &[("cpu", "100m"), ("memory", "128Mi")],
        )),
    );
    let none = container("b", None);
    let pod = pod_with_containers("mixed", vec![g, none]);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

#[test]
fn multi_container_all_guaranteed_pod_is_guaranteed() {
    let g1 = container(
        "a",
        Some(rr(
            &[("cpu", "100m"), ("memory", "128Mi")],
            &[("cpu", "100m"), ("memory", "128Mi")],
        )),
    );
    let g2 = container(
        "b",
        Some(rr(
            &[("cpu", "200m"), ("memory", "256Mi")],
            &[("cpu", "200m"), ("memory", "256Mi")],
        )),
    );
    let pod = pod_with_containers("all-guaranteed", vec![g1, g2]);
    assert_eq!(get_qos_class(&pod), QoSClass::Guaranteed);
}

// ---------------------------------------------------------------------------
// Partial-limits edge cases
// ---------------------------------------------------------------------------

#[test]
fn limits_missing_memory_is_not_guaranteed() {
    // CPU-only limits, no memory limit → Burstable, not Guaranteed.
    let c = container("cpu-only", Some(rr(&[("cpu", "100m")], &[("cpu", "100m")])));
    let pod = pod_with_containers("p", vec![c]);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

#[test]
fn limits_missing_cpu_is_not_guaranteed() {
    let c = container(
        "mem-only",
        Some(rr(&[("memory", "128Mi")], &[("memory", "128Mi")])),
    );
    let pod = pod_with_containers("p", vec![c]);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

#[test]
fn requests_lower_than_limits_is_burstable() {
    let c = container(
        "burst",
        Some(rr(
            &[("cpu", "100m"), ("memory", "128Mi")],
            &[("cpu", "200m"), ("memory", "256Mi")],
        )),
    );
    let pod = pod_with_containers("p", vec![c]);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

#[test]
fn only_requests_set_is_burstable() {
    let c = container(
        "req-only",
        Some(rr(&[("cpu", "50m"), ("memory", "64Mi")], &[])),
    );
    let pod = pod_with_containers("p", vec![c]);
    assert_eq!(get_qos_class(&pod), QoSClass::Burstable);
}

#[test]
fn empty_pod_spec_is_besteffort() {
    let pod = pod_with_containers("empty", vec![]);
    assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
}

// ---------------------------------------------------------------------------
// PodSpec.resources (pod-level resources) — serde round-trip
// ---------------------------------------------------------------------------

#[test]
fn pod_level_resources_field_roundtrips_through_serde() {
    let mut pod = pod_with_containers("pod-level", vec![container("a", None)]);
    pod.spec.as_mut().unwrap().resources = Some(rr(
        &[("cpu", "500m"), ("memory", "512Mi")],
        &[("cpu", "1"), ("memory", "1Gi")],
    ));
    let json = serde_json::to_value(&pod).unwrap();
    let spec_resources = &json["spec"]["resources"];
    assert_eq!(spec_resources["requests"]["cpu"], "500m");
    assert_eq!(spec_resources["limits"]["memory"], "1Gi");

    let decoded: Pod = serde_json::from_value(json).unwrap();
    assert!(decoded.spec.as_ref().unwrap().resources.is_some());
}
