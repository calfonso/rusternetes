// Conformance: SecurityContext field shape (non-`privileged` fields).
//
// Upstream references (k8s v1.35):
//   - `test/e2e/common/node/security_context.go`
//   - `pkg/apis/core/v1/types.go::SecurityContext` and `PodSecurityContext`
//
// Pins serde round-trip + camelCase wire format for the security context
// fields that kubelet forwards to the CRI runtime. The `privileged`
// surface is covered in `conformance_node_privileged.rs`.

use rusternetes_common::resources::pod::PodSecurityContext;
use rusternetes_common::resources::{Capabilities, Container, SeccompProfile, SecurityContext};

fn empty_sc() -> SecurityContext {
    SecurityContext {
        privileged: None,
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

fn container_with(sc: SecurityContext) -> Container {
    Container {
        name: "c".to_string(),
        image: "nginx".to_string(),
        security_context: Some(sc),
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
fn run_as_user_and_group_are_camel_case() {
    let mut sc = empty_sc();
    sc.run_as_user = Some(1000);
    sc.run_as_group = Some(2000);
    let v = serde_json::to_value(container_with(sc)).unwrap();
    assert_eq!(v["securityContext"]["runAsUser"], 1000);
    assert_eq!(v["securityContext"]["runAsGroup"], 2000);
}

#[test]
fn run_as_non_root_round_trips() {
    let mut sc = empty_sc();
    sc.run_as_non_root = Some(true);
    let v = serde_json::to_value(container_with(sc)).unwrap();
    assert_eq!(v["securityContext"]["runAsNonRoot"], true);

    let decoded: Container = serde_json::from_value(v).unwrap();
    assert_eq!(
        decoded.security_context.unwrap().run_as_non_root,
        Some(true)
    );
}

#[test]
fn allow_privilege_escalation_round_trips() {
    let mut sc = empty_sc();
    sc.allow_privilege_escalation = Some(false);
    let v = serde_json::to_value(container_with(sc)).unwrap();
    assert_eq!(v["securityContext"]["allowPrivilegeEscalation"], false);
}

#[test]
fn read_only_root_filesystem_round_trips() {
    let mut sc = empty_sc();
    sc.read_only_root_filesystem = Some(true);
    let v = serde_json::to_value(container_with(sc)).unwrap();
    assert_eq!(v["securityContext"]["readOnlyRootFilesystem"], true);
}

#[test]
fn capabilities_add_and_drop_round_trip() {
    let mut sc = empty_sc();
    sc.capabilities = Some(Capabilities {
        add: Some(vec!["NET_ADMIN".to_string(), "SYS_TIME".to_string()]),
        drop: Some(vec!["ALL".to_string()]),
    });
    let v = serde_json::to_value(container_with(sc.clone())).unwrap();
    let caps = &v["securityContext"]["capabilities"];
    assert_eq!(caps["add"][0], "NET_ADMIN");
    assert_eq!(caps["add"][1], "SYS_TIME");
    assert_eq!(caps["drop"][0], "ALL");

    let decoded: Container = serde_json::from_value(v).unwrap();
    let caps = decoded.security_context.unwrap().capabilities.unwrap();
    assert_eq!(caps.add.unwrap().len(), 2);
    assert_eq!(caps.drop.unwrap()[0], "ALL");
}

#[test]
fn seccomp_profile_runtime_default_round_trips() {
    let mut sc = empty_sc();
    sc.seccomp_profile = Some(SeccompProfile {
        r#type: "RuntimeDefault".to_string(),
        localhost_profile: None,
    });
    let v = serde_json::to_value(container_with(sc)).unwrap();
    assert_eq!(
        v["securityContext"]["seccompProfile"]["type"],
        "RuntimeDefault"
    );
    assert!(v["securityContext"]["seccompProfile"]
        .get("localhostProfile")
        .is_none());
}

#[test]
fn seccomp_profile_localhost_carries_path() {
    let mut sc = empty_sc();
    sc.seccomp_profile = Some(SeccompProfile {
        r#type: "Localhost".to_string(),
        localhost_profile: Some("profiles/audit.json".to_string()),
    });
    let v = serde_json::to_value(container_with(sc)).unwrap();
    assert_eq!(v["securityContext"]["seccompProfile"]["type"], "Localhost");
    assert_eq!(
        v["securityContext"]["seccompProfile"]["localhostProfile"],
        "profiles/audit.json"
    );
}

#[test]
fn pod_security_context_run_as_non_root_camel_case() {
    // PodSecurityContext is read by PSA `restricted` enforcement —
    // pin the camelCase wire format the admission webhook will see.
    let psc = PodSecurityContext {
        run_as_non_root: Some(true),
        ..Default::default()
    };
    let v = serde_json::to_value(&psc).unwrap();
    assert_eq!(v["runAsNonRoot"], true);
}
