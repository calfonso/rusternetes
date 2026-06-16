//! Translate a rusternetes [`Pod`] into CRI v1 sandbox and container configs.
//!
//! These are pure functions — they take a `Pod` (and, for containers, a
//! resolved image ref and a volume-name → host-path map) and produce the
//! `runtime.v1` config messages the CRI runtime expects. Keeping them pure makes
//! the Pod→CRI mapping unit-testable without a running runtime, and isolates the
//! single place where rusternetes' resource model meets the CRI wire model.
//!
//! Scope: the fields needed to launch a pod — metadata, labels, namespaces,
//! command/args/env (literal values), ports, resources, mounts, and the linux
//! security context. Deferred (tracked separately): `valueFrom` env resolution
//! (secrets/configMaps), `envFrom`, probes (driven kubelet-side via ExecSync),
//! and windows configs.

use std::collections::HashMap;

use rusternetes_common::resources::pod::{Container, Pod};
use rusternetes_cri::v1;

/// Well-known CRI metadata label keys the runtime indexes sandboxes/containers
/// by. They mirror the keys the upstream kubelet sets so `crictl`/tools work.
mod labels {
    pub const POD_NAME: &str = "io.kubernetes.pod.name";
    pub const POD_NAMESPACE: &str = "io.kubernetes.pod.namespace";
    pub const POD_UID: &str = "io.kubernetes.pod.uid";
    pub const CONTAINER_NAME: &str = "io.kubernetes.container.name";
}

fn namespace(pod: &Pod) -> &str {
    pod.metadata.namespace.as_deref().unwrap_or("default")
}

/// True when the pod requested the host network namespace.
fn host_network(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.host_network)
        .unwrap_or(false)
}

/// Build the sandbox-level namespace options (host vs pod network/pid/ipc).
fn namespace_options(pod: &Pod) -> v1::NamespaceOption {
    let spec = pod.spec.as_ref();
    let host = |f: fn(&rusternetes_common::resources::pod::PodSpec) -> Option<bool>| {
        spec.and_then(f).unwrap_or(false)
    };
    let mode = |on: bool| {
        if on {
            v1::NamespaceMode::Node as i32
        } else {
            v1::NamespaceMode::Pod as i32
        }
    };
    v1::NamespaceOption {
        network: mode(host_network(pod)),
        pid: mode(host(|s| s.host_pid)),
        ipc: mode(host(|s| s.host_ipc)),
        ..Default::default()
    }
}

/// Labels CRI attaches so the sandbox/container can be looked up by pod.
fn pod_labels(pod: &Pod) -> HashMap<String, String> {
    let mut l = HashMap::new();
    l.insert(labels::POD_NAME.to_string(), pod.metadata.name.clone());
    l.insert(
        labels::POD_NAMESPACE.to_string(),
        namespace(pod).to_string(),
    );
    l.insert(labels::POD_UID.to_string(), pod.metadata.uid.clone());
    l
}

/// Aggregate every container port into CRI sandbox port mappings.
fn port_mappings(pod: &Pod) -> Vec<v1::PortMapping> {
    let Some(spec) = pod.spec.as_ref() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for c in &spec.containers {
        let Some(ports) = c.ports.as_ref() else {
            continue;
        };
        for p in ports {
            out.push(v1::PortMapping {
                protocol: protocol_to_cri(p.protocol.as_deref()),
                container_port: i32::from(p.container_port),
                host_port: p.host_port.map(i32::from).unwrap_or(0),
                host_ip: p.host_ip.clone().unwrap_or_default(),
            });
        }
    }
    out
}

fn protocol_to_cri(proto: Option<&str>) -> i32 {
    match proto.unwrap_or("TCP").to_ascii_uppercase().as_str() {
        "UDP" => v1::Protocol::Udp as i32,
        "SCTP" => v1::Protocol::Sctp as i32,
        _ => v1::Protocol::Tcp as i32,
    }
}

/// Translate the pod into a CRI [`PodSandboxConfig`](v1::PodSandboxConfig).
///
/// `log_directory` is the kubelet-owned dir the runtime writes container logs
/// under; it must exist before `RunPodSandbox`.
pub fn sandbox_config(pod: &Pod, log_directory: &str) -> v1::PodSandboxConfig {
    let hostname = pod
        .spec
        .as_ref()
        .and_then(|s| s.hostname.clone())
        .unwrap_or_else(|| pod.metadata.name.clone());

    v1::PodSandboxConfig {
        metadata: Some(v1::PodSandboxMetadata {
            name: pod.metadata.name.clone(),
            uid: pod.metadata.uid.clone(),
            namespace: namespace(pod).to_string(),
            attempt: 0,
        }),
        hostname,
        log_directory: log_directory.to_string(),
        labels: pod_labels(pod),
        port_mappings: port_mappings(pod),
        linux: Some(v1::LinuxPodSandboxConfig {
            security_context: Some(v1::LinuxSandboxSecurityContext {
                namespace_options: Some(namespace_options(pod)),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Translate literal env vars; `valueFrom` (secret/configMap/field refs) is
/// resolved kubelet-side before translation and is not handled here.
fn env_vars(container: &Container) -> Vec<v1::KeyValue> {
    let Some(env) = container.env.as_ref() else {
        return Vec::new();
    };
    env.iter()
        .filter_map(|e| {
            e.value.as_ref().map(|v| v1::KeyValue {
                key: e.name.clone(),
                value: v.clone(),
            })
        })
        .collect()
}

/// Translate volume mounts into CRI mounts using a resolved volume-name →
/// host-path map (volume provisioning is runtime-agnostic and done earlier).
/// Mounts whose volume is absent from the map are skipped.
fn mounts(container: &Container, host_paths: &HashMap<String, String>) -> Vec<v1::Mount> {
    let Some(vms) = container.volume_mounts.as_ref() else {
        return Vec::new();
    };
    vms.iter()
        .filter_map(|vm| {
            host_paths.get(&vm.name).map(|host| v1::Mount {
                container_path: vm.mount_path.clone(),
                host_path: host.clone(),
                readonly: vm.read_only.unwrap_or(false),
                ..Default::default()
            })
        })
        .collect()
}

/// Parse a Kubernetes CPU quantity into millicores (`"500m"` → 500, `"2"` →
/// 2000). Returns `None` for unparseable input.
fn parse_cpu_millicores(q: &str) -> Option<i64> {
    let q = q.trim();
    if let Some(m) = q.strip_suffix('m') {
        m.trim().parse::<i64>().ok()
    } else {
        q.parse::<f64>().ok().map(|c| (c * 1000.0).round() as i64)
    }
}

/// Parse a Kubernetes memory quantity into bytes (`"128Mi"`, `"1Gi"`, `"1000000"`).
fn parse_memory_bytes(q: &str) -> Option<i64> {
    let q = q.trim();
    let units: &[(&str, i64)] = &[
        ("Ki", 1 << 10),
        ("Mi", 1 << 20),
        ("Gi", 1 << 30),
        ("Ti", 1i64 << 40),
        ("k", 1_000),
        ("M", 1_000_000),
        ("G", 1_000_000_000),
        ("T", 1_000_000_000_000),
    ];
    for (suffix, mult) in units {
        if let Some(n) = q.strip_suffix(suffix) {
            return n
                .trim()
                .parse::<f64>()
                .ok()
                .map(|v| (v * *mult as f64) as i64);
        }
    }
    q.parse::<i64>().ok()
}

/// Build CRI linux resources from a container's limits/requests. CPU limit →
/// cfs quota (100ms period); CPU request → shares; memory limit → byte cap.
fn linux_resources(container: &Container) -> Option<v1::LinuxContainerResources> {
    let req = container.resources.as_ref()?;
    let mut r = v1::LinuxContainerResources::default();
    let mut any = false;

    if let Some(limits) = req.limits.as_ref() {
        if let Some(cpu) = limits.get("cpu").and_then(|q| parse_cpu_millicores(q)) {
            r.cpu_period = 100_000;
            r.cpu_quota = cpu * 100; // millicores * (period/1000)
            any = true;
        }
        if let Some(mem) = limits.get("memory").and_then(|q| parse_memory_bytes(q)) {
            r.memory_limit_in_bytes = mem;
            any = true;
        }
    }
    if let Some(requests) = req.requests.as_ref() {
        if let Some(cpu) = requests.get("cpu").and_then(|q| parse_cpu_millicores(q)) {
            r.cpu_shares = cpu * 1024 / 1000;
            any = true;
        }
    }
    any.then_some(r)
}

fn linux_security_context(container: &Container) -> Option<v1::LinuxContainerSecurityContext> {
    let sc = container.security_context.as_ref()?;
    Some(v1::LinuxContainerSecurityContext {
        privileged: sc.privileged.unwrap_or(false),
        run_as_user: sc.run_as_user.map(|v| v1::Int64Value { value: v }),
        run_as_group: sc.run_as_group.map(|v| v1::Int64Value { value: v }),
        readonly_rootfs: sc.read_only_root_filesystem.unwrap_or(false),
        ..Default::default()
    })
}

/// Translate a single container into a CRI [`ContainerConfig`](v1::ContainerConfig).
///
/// `image_ref` is the canonical reference returned by `PullImage`. `host_paths`
/// maps volume names to their resolved host paths for mount translation.
pub fn container_config(
    pod: &Pod,
    container: &Container,
    image_ref: &str,
    host_paths: &HashMap<String, String>,
) -> v1::ContainerConfig {
    let mut labels = pod_labels(pod);
    labels.insert(labels::CONTAINER_NAME.to_string(), container.name.clone());

    let linux = {
        let resources = linux_resources(container);
        let security_context = linux_security_context(container);
        if resources.is_some() || security_context.is_some() {
            Some(v1::LinuxContainerConfig {
                resources,
                security_context,
            })
        } else {
            None
        }
    };

    v1::ContainerConfig {
        metadata: Some(v1::ContainerMetadata {
            name: container.name.clone(),
            attempt: 0,
        }),
        image: Some(v1::ImageSpec {
            image: image_ref.to_string(),
            ..Default::default()
        }),
        command: container.command.clone().unwrap_or_default(),
        args: container.args.clone().unwrap_or_default(),
        working_dir: container.working_dir.clone().unwrap_or_default(),
        envs: env_vars(container),
        mounts: mounts(container, host_paths),
        labels,
        log_path: format!("{}.log", container.name),
        linux,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::pod::PodSpec;

    fn pod_with(spec: PodSpec) -> Pod {
        let mut pod = Pod::new("web", spec);
        pod.metadata.uid = "uid-123".to_string();
        pod.metadata.namespace = Some("prod".to_string());
        pod
    }

    #[test]
    fn cpu_parsing() {
        assert_eq!(parse_cpu_millicores("500m"), Some(500));
        assert_eq!(parse_cpu_millicores("2"), Some(2000));
        assert_eq!(parse_cpu_millicores("0.5"), Some(500));
        assert_eq!(parse_cpu_millicores("garbage"), None);
    }

    #[test]
    fn memory_parsing() {
        assert_eq!(parse_memory_bytes("128Mi"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("1000000"), Some(1_000_000));
        assert_eq!(parse_memory_bytes("1M"), Some(1_000_000));
    }

    #[test]
    fn sandbox_carries_metadata_and_labels() {
        let pod = pod_with(PodSpec {
            host_network: Some(true),
            ..Default::default()
        });
        let cfg = sandbox_config(&pod, "/var/log/pods/web");
        let meta = cfg.metadata.unwrap();
        assert_eq!(meta.name, "web");
        assert_eq!(meta.uid, "uid-123");
        assert_eq!(meta.namespace, "prod");
        assert_eq!(cfg.labels.get(labels::POD_UID).unwrap(), "uid-123");
        // host_network -> NODE network namespace
        let ns = cfg
            .linux
            .unwrap()
            .security_context
            .unwrap()
            .namespace_options
            .unwrap();
        assert_eq!(ns.network, v1::NamespaceMode::Node as i32);
    }

    #[test]
    fn container_translates_command_env_resources() {
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.command = Some(vec!["/bin/sh".to_string()]);
        c.args = Some(vec!["-c".to_string(), "sleep 1".to_string()]);
        c.env = Some(vec![rusternetes_common::resources::pod::EnvVar {
            name: "FOO".to_string(),
            value: Some("bar".to_string()),
            value_from: None,
        }]);
        c.resources = Some(rusternetes_common::types::ResourceRequirements {
            limits: Some(HashMap::from([
                ("cpu".to_string(), "500m".to_string()),
                ("memory".to_string(), "64Mi".to_string()),
            ])),
            requests: None,
            claims: None,
        });
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let cfg = container_config(
            &pod,
            &c,
            "docker.io/library/busybox@sha256:abc",
            &HashMap::new(),
        );
        assert_eq!(cfg.command, vec!["/bin/sh"]);
        assert_eq!(cfg.args, vec!["-c", "sleep 1"]);
        assert_eq!(cfg.envs[0].key, "FOO");
        assert_eq!(cfg.envs[0].value, "bar");
        let res = cfg.linux.unwrap().resources.unwrap();
        assert_eq!(res.cpu_quota, 50_000); // 500m -> quota 50000 @ 100ms period
        assert_eq!(res.memory_limit_in_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.labels.get(labels::CONTAINER_NAME).unwrap(), "app");
    }
}
