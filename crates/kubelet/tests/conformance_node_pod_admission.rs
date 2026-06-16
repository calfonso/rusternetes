//! Conformance: pod-admission-relevant resource shape.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/pod_admission.go` — e2e admission scenarios.
//!   - `staging/src/k8s.io/pod-security-admission/api` — restricted /
//!     baseline / privileged level field expectations.
//!
//! Rusternetes' Pod Security Admission is a stub (see
//! `crates/api-server/src/admission.rs::PodSecurityAdmission::admit` and
//! the RED-state suite at
//! `crates/api-server/tests/pod_security_admission_test.rs`). These tests
//! pin the **resource-side** of the admission contract: the camelCase
//! field shapes that the future admission implementation will read, and
//! the OS / nodeSelector field that today's kubelet uses to refuse pods
//! it cannot run. Behavioural admit/deny coverage stays in api-server.

use rusternetes_common::resources::pod::{PodOS, PodSecurityContext};
use rusternetes_common::resources::{Container, Pod, PodSpec, SecurityContext, Toleration};
use rusternetes_common::types::{ObjectMeta, TypeMeta};

fn base_container() -> Container {
    Container {
        name: "c".to_string(),
        image: "busybox".to_string(),
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

fn base_pod() -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("p").with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![base_container()],
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
// PSA `baseline` / `restricted` — host-namespace fields
// ---------------------------------------------------------------------------

#[test]
fn host_network_pid_ipc_serialize_in_camel_case() {
    let mut p = base_pod();
    let spec = p.spec.as_mut().unwrap();
    spec.host_network = Some(true);
    spec.host_pid = Some(true);
    spec.host_ipc = Some(true);
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(v["spec"]["hostNetwork"], true);
    assert_eq!(v["spec"]["hostPID"], true);
    assert_eq!(v["spec"]["hostIPC"], true);
}

// ---------------------------------------------------------------------------
// PSA `restricted` — runAsNonRoot + allowPrivilegeEscalation
// ---------------------------------------------------------------------------

#[test]
fn pod_security_context_run_as_non_root_round_trips() {
    let mut p = base_pod();
    p.spec.as_mut().unwrap().security_context = Some(PodSecurityContext {
        run_as_non_root: Some(true),
        ..Default::default()
    });
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(v["spec"]["securityContext"]["runAsNonRoot"], true);
}

#[test]
fn container_allow_privilege_escalation_round_trips() {
    let mut p = base_pod();
    p.spec.as_mut().unwrap().containers[0].security_context = Some(SecurityContext {
        privileged: None,
        run_as_user: None,
        run_as_group: None,
        run_as_non_root: None,
        read_only_root_filesystem: None,
        allow_privilege_escalation: Some(false),
        proc_mount: None,
        capabilities: None,
        seccomp_profile: None,
        se_linux_options: None,
        app_armor_profile: None,
        windows_options: None,
    });
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(
        v["spec"]["containers"][0]["securityContext"]["allowPrivilegeEscalation"],
        false
    );
}

// ---------------------------------------------------------------------------
// OS-based kubelet admission — kubelet refuses pods whose `spec.os.name`
// doesn't match the node's OS, and the field must round-trip cleanly.
// ---------------------------------------------------------------------------

#[test]
fn pod_os_field_round_trips() {
    let mut p = base_pod();
    p.spec.as_mut().unwrap().os = Some(PodOS {
        name: "linux".to_string(),
    });
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(v["spec"]["os"]["name"], "linux");

    let decoded: Pod = serde_json::from_value(v).unwrap();
    assert_eq!(decoded.spec.unwrap().os.unwrap().name.as_str(), "linux");
}

// ---------------------------------------------------------------------------
// Tolerations — `tolerationSeconds` is the not-ready / unreachable taint
// timeout the scheduler-admission and kubelet-admission paths both read
// ---------------------------------------------------------------------------

#[test]
fn tolerations_round_trip_with_camel_case_seconds() {
    let mut p = base_pod();
    p.spec.as_mut().unwrap().tolerations = Some(vec![Toleration {
        key: Some("node.kubernetes.io/not-ready".to_string()),
        operator: Some("Exists".to_string()),
        value: None,
        effect: Some("NoExecute".to_string()),
        toleration_seconds: Some(300),
    }]);
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(v["spec"]["tolerations"][0]["tolerationSeconds"], 300);
    assert_eq!(
        v["spec"]["tolerations"][0]["key"],
        "node.kubernetes.io/not-ready"
    );
}
