//! Conformance: privileged container `SecurityContext.privileged` field.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/privileged.go`
//!   - `pkg/apis/core/v1/types.go::SecurityContext.Privileged`
//!
//! Pins the wire shape and omission behaviour of the `privileged` flag —
//! kubelet's container-config builder reads `container.securityContext.
//! privileged` (bool) and forwards it to the runtime via the CRI
//! `LinuxContainerSecurityContext.Privileged` field. Mismatches here
//! cause silently-unprivileged containers, which is one of the highest
//! severity K8s API drifts (privilege escalation surface).
//!
//! Behavioural (does-it-actually-launch-privileged) coverage requires a
//! real runtime; that is exercised separately in node conformance.

use rusternetes_common::resources::{Container, SecurityContext};

fn sec_ctx(privileged: Option<bool>) -> SecurityContext {
    SecurityContext {
        privileged,
        run_as_user: None,
        run_as_group: None,
        run_as_non_root: None,
        read_only_root_filesystem: None,
        allow_privilege_escalation: None,
        proc_mount: None,
        capabilities: None,
        seccomp_profile: None,
        se_linux_options: None,
        app_armor_profile: None,
        windows_options: None,
    }
}

fn container_with_sec_ctx(name: &str, sc: Option<SecurityContext>) -> Container {
    Container {
        name: name.to_string(),
        image: "nginx:latest".to_string(),
        security_context: sc,
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

#[test]
fn privileged_true_serializes_as_explicit_true() {
    let c = container_with_sec_ctx("priv", Some(sec_ctx(Some(true))));
    let v = serde_json::to_value(&c).unwrap();
    assert_eq!(v["securityContext"]["privileged"], true);
}

#[test]
fn privileged_false_serializes_as_explicit_false() {
    // Explicit `privileged: false` must round-trip — clients use it to
    // pin "do not escalate" intent.
    let c = container_with_sec_ctx("explicit-noop", Some(sec_ctx(Some(false))));
    let v = serde_json::to_value(&c).unwrap();
    assert_eq!(v["securityContext"]["privileged"], false);
}
