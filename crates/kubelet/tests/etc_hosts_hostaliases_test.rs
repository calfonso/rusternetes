//! Integration tests for kubelet-managed /etc/hosts content.
//!
//! Mirrors the upstream e2e site `test/e2e/common/node/kubelet_etc_hosts.go:143`,
//! which verifies that the kubelet writes a managed /etc/hosts file with the
//! standard localhost entries and any `spec.hostAliases` lines, and that
//! `hostNetwork: true` pods do NOT get a managed file.

use rusternetes_common::resources::pod::HostAlias;
use rusternetes_common::resources::{Container, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kubelet::kubelet::build_managed_hosts_content;

fn make_container(name: &str) -> Container {
    Container {
        name: name.to_string(),
        image: "nginx:latest".to_string(),
        image_pull_policy: Some("IfNotPresent".to_string()),
        command: None,
        args: None,
        ports: None,
        env: None,
        volume_mounts: None,
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
        env_from: None,
        volume_devices: None,
        ..Default::default()
    }
}

fn make_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![make_container("app")],
            init_containers: None,
            ephemeral_containers: None,
            restart_policy: Some("Always".to_string()),
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority: None,
            priority_class_name: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
            resource_claims: None,
            volumes: None,
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

/// Conformance: kubelet-managed /etc/hosts MUST start with the well-known
/// header so consumers (and the upstream e2e probe) can distinguish a
/// managed file from a container-runtime-default file.
#[test]
fn managed_hosts_file_has_kubernetes_header() {
    let pod = make_pod("hostaliases-pod", "default");
    let content = build_managed_hosts_content(&pod, None, "cluster.local")
        .expect("non-hostNetwork pod must produce managed content");

    assert!(
        content.starts_with("# Kubernetes-managed hosts file."),
        "expected upstream-compatible header, got: {:?}",
        content.lines().next()
    );
}

/// Conformance: the file MUST contain the standard loopback / multicast
/// entries with the exact IPv6 addresses upstream kubelet emits
/// (see `pkg/kubelet/kubelet_pods.go::managedHostsFileContent`).
#[test]
fn managed_hosts_file_has_standard_localhost_entries() {
    let pod = make_pod("hostaliases-pod", "default");
    let content = build_managed_hosts_content(&pod, None, "cluster.local")
        .expect("non-hostNetwork pod must produce managed content");

    // IPv4 loopback
    assert!(
        content.contains("127.0.0.1\tlocalhost"),
        "missing 127.0.0.1 loopback line; got:\n{}",
        content
    );
    // IPv6 loopback
    assert!(
        content.contains("::1\tlocalhost ip6-localhost ip6-loopback"),
        "missing ::1 loopback line; got:\n{}",
        content
    );
    // The four IPv6 multicast/network entries MUST match upstream values
    // exactly (fe00::0 / ff00::0 / ff02::1 / ff02::2).
    for required in [
        "fe00::0\tip6-localnet",
        "ff00::0\tip6-mcastprefix",
        "ff02::1\tip6-allnodes",
        "ff02::2\tip6-allrouters",
    ] {
        assert!(
            content.contains(required),
            "missing upstream-correct line `{}`; got:\n{}",
            required,
            content
        );
    }
}

/// Conformance: every entry in `spec.hostAliases` must be appended verbatim,
/// one line per IP, hostnames joined by tabs.
#[test]
fn managed_hosts_file_appends_host_aliases() {
    let mut pod = make_pod("alias-pod", "default");
    pod.spec.as_mut().unwrap().host_aliases = Some(vec![
        HostAlias {
            ip: "123.45.67.89".to_string(),
            hostnames: Some(vec!["foo.example".to_string(), "bar.example".to_string()]),
        },
        HostAlias {
            ip: "10.20.30.40".to_string(),
            hostnames: Some(vec!["baz.example".to_string()]),
        },
    ]);

    let content = build_managed_hosts_content(&pod, Some("10.244.1.5"), "cluster.local")
        .expect("non-hostNetwork pod must produce managed content");

    assert!(
        content.contains("123.45.67.89\tfoo.example\tbar.example"),
        "missing first HostAlias line; got:\n{}",
        content
    );
    assert!(
        content.contains("10.20.30.40\tbaz.example"),
        "missing second HostAlias line; got:\n{}",
        content
    );
}

/// Conformance: HostAlias entries with no hostnames (or empty) must be
/// silently dropped — never written as an IP-only line.
#[test]
fn managed_hosts_file_skips_empty_host_aliases() {
    let mut pod = make_pod("empty-alias", "default");
    pod.spec.as_mut().unwrap().host_aliases = Some(vec![
        HostAlias {
            ip: "1.2.3.4".to_string(),
            hostnames: Some(vec![]),
        },
        HostAlias {
            ip: "5.6.7.8".to_string(),
            hostnames: None,
        },
    ]);

    let content = build_managed_hosts_content(&pod, None, "cluster.local")
        .expect("non-hostNetwork pod must produce managed content");

    assert!(
        !content.contains("1.2.3.4"),
        "empty-hostnames HostAlias must not appear; got:\n{}",
        content
    );
    assert!(
        !content.contains("5.6.7.8"),
        "no-hostnames HostAlias must not appear; got:\n{}",
        content
    );
}

/// Conformance: `hostNetwork: true` pods MUST NOT receive a kubelet-managed
/// /etc/hosts. The function returns `None` so the runtime knows to skip the
/// bind mount and exec-write entirely (the pod uses the host's /etc/hosts).
#[test]
fn host_network_pod_has_no_managed_hosts_file() {
    let mut pod = make_pod("hostnet-pod", "default");
    pod.spec.as_mut().unwrap().host_network = Some(true);
    pod.spec.as_mut().unwrap().host_aliases = Some(vec![HostAlias {
        ip: "1.2.3.4".to_string(),
        hostnames: Some(vec!["should.not.appear".to_string()]),
    }]);

    let result = build_managed_hosts_content(&pod, Some("10.244.1.5"), "cluster.local");
    assert!(
        result.is_none(),
        "hostNetwork=true pod must NOT get a managed hosts file, got: {:?}",
        result
    );
}

/// Conformance: the pod's own IP / hostname entry must appear when an IP is
/// known. With a subdomain set, the FQDN is included on the same line.
#[test]
fn managed_hosts_file_includes_pod_ip_and_fqdn() {
    let mut pod = make_pod("web-0", "default");
    {
        let spec = pod.spec.as_mut().unwrap();
        spec.hostname = Some("web-0".to_string());
        spec.subdomain = Some("nginx".to_string());
    }

    let content = build_managed_hosts_content(&pod, Some("10.244.1.5"), "cluster.local")
        .expect("non-hostNetwork pod must produce managed content");

    assert!(
        content.contains("10.244.1.5\tweb-0\tweb-0.nginx.default.svc.cluster.local"),
        "missing IP / hostname / FQDN line; got:\n{}",
        content
    );
}
