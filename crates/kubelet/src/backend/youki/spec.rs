//! OCI runtime spec (`config.json`) for a container.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::backend::{ContainerConfig, HostConfig, ImageConfig};

/// Docker's default capability set.
const DEFAULT_CAPS: &[&str] = &[
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_FSETID",
    "CAP_FOWNER",
    "CAP_MKNOD",
    "CAP_NET_RAW",
    "CAP_SETGID",
    "CAP_SETUID",
    "CAP_SETFCAP",
    "CAP_SETPCAP",
    "CAP_NET_BIND_SERVICE",
    "CAP_SYS_CHROOT",
    "CAP_KILL",
    "CAP_AUDIT_WRITE",
];

const ALL_CAPS: &[&str] = &[
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_DAC_READ_SEARCH",
    "CAP_FOWNER",
    "CAP_FSETID",
    "CAP_KILL",
    "CAP_SETGID",
    "CAP_SETUID",
    "CAP_SETPCAP",
    "CAP_LINUX_IMMUTABLE",
    "CAP_NET_BIND_SERVICE",
    "CAP_NET_BROADCAST",
    "CAP_NET_ADMIN",
    "CAP_NET_RAW",
    "CAP_IPC_LOCK",
    "CAP_IPC_OWNER",
    "CAP_SYS_MODULE",
    "CAP_SYS_RAWIO",
    "CAP_SYS_CHROOT",
    "CAP_SYS_PTRACE",
    "CAP_SYS_PACCT",
    "CAP_SYS_ADMIN",
    "CAP_SYS_BOOT",
    "CAP_SYS_NICE",
    "CAP_SYS_RESOURCE",
    "CAP_SYS_TIME",
    "CAP_SYS_TTY_CONFIG",
    "CAP_MKNOD",
    "CAP_LEASE",
    "CAP_AUDIT_WRITE",
    "CAP_AUDIT_CONTROL",
    "CAP_SETFCAP",
    "CAP_MAC_OVERRIDE",
    "CAP_MAC_ADMIN",
    "CAP_SYSLOG",
    "CAP_WAKE_ALARM",
    "CAP_BLOCK_SUSPEND",
    "CAP_AUDIT_READ",
    "CAP_PERFMON",
    "CAP_BPF",
    "CAP_CHECKPOINT_RESTORE",
];

const DEFAULT_PATH: &str = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// How a container gets one of its namespaces.
#[derive(Debug, Clone, PartialEq)]
pub enum Ns {
    /// A fresh namespace.
    New,
    /// Join the namespace at this path.
    Join(PathBuf),
    /// Stay in the host's namespace.
    Host,
}

/// Inputs that the backend resolves before building the spec.
#[derive(Debug, Clone)]
pub struct SpecInput<'a> {
    pub config: &'a ContainerConfig,
    pub image: &'a ImageConfig,
    pub rootfs: &'a Path,
    pub uid: u32,
    pub gid: u32,
    pub net: Ns,
    pub ipc: Ns,
    pub pid: Ns,
    pub uts: Ns,
    /// A generated resolv.conf to bind over `/etc/resolv.conf`.
    pub resolv_conf: Option<&'a Path>,
    /// cgroup v2 path relative to the cgroup root.
    pub cgroups_path: &'a str,
}

/// Docker's rules: an explicit entrypoint drops the image's cmd unless a
/// cmd is given too.
pub fn process_args(config: &ContainerConfig, image: &ImageConfig) -> Result<Vec<String>> {
    let entrypoint = config
        .entrypoint
        .clone()
        .or_else(|| image.entrypoint.clone())
        .unwrap_or_default();
    let cmd = match (&config.cmd, &config.entrypoint) {
        (Some(cmd), _) => cmd.clone(),
        (None, Some(_)) => Vec::new(),
        (None, None) => image.cmd.clone().unwrap_or_default(),
    };
    let args: Vec<String> = entrypoint.into_iter().chain(cmd).collect();
    if args.is_empty() {
        bail!("no command specified");
    }
    Ok(args)
}

/// Image env, overridden key by key by the container env; PATH and
/// HOSTNAME are filled in like Docker does.
pub fn process_env(config: &ContainerConfig, image: &ImageConfig, hostname: &str) -> Vec<String> {
    let mut order: Vec<String> = Vec::new();
    let mut values: HashMap<String, String> = HashMap::new();
    let all = image
        .env
        .iter()
        .flatten()
        .chain(config.env.iter().flatten());
    for kv in all {
        let key = kv.split('=').next().unwrap_or("").to_string();
        if !values.contains_key(&key) {
            order.push(key.clone());
        }
        values.insert(key, kv.clone());
    }
    let mut env: Vec<String> = order.iter().map(|k| values[k].clone()).collect();
    if !values.contains_key("PATH") {
        env.insert(0, DEFAULT_PATH.to_string());
    }
    if !values.contains_key("HOSTNAME") && !hostname.is_empty() {
        env.push(format!("HOSTNAME={}", hostname));
    }
    env
}

fn cap_name(c: &str) -> String {
    let c = c.to_uppercase();
    if c.starts_with("CAP_") {
        c
    } else {
        format!("CAP_{}", c)
    }
}

pub fn capabilities(hc: &HostConfig) -> Vec<String> {
    if hc.privileged == Some(true) {
        return ALL_CAPS.iter().map(|s| s.to_string()).collect();
    }
    let mut caps: Vec<String> = DEFAULT_CAPS.iter().map(|s| s.to_string()).collect();
    for c in hc.cap_drop.iter().flatten() {
        if c.eq_ignore_ascii_case("ALL") {
            caps.clear();
        } else {
            let c = cap_name(c);
            caps.retain(|x| *x != c);
        }
    }
    for c in hc.cap_add.iter().flatten() {
        if c.eq_ignore_ascii_case("ALL") {
            caps = ALL_CAPS.iter().map(|s| s.to_string()).collect();
        } else {
            let c = cap_name(c);
            if !caps.contains(&c) {
                caps.push(c);
            }
        }
    }
    caps
}

/// `"src:dst[:opts]"` to an OCI bind mount.
pub fn bind_mount(bind: &str) -> Result<Value> {
    let parts: Vec<&str> = bind.split(':').collect();
    let (src, dst, opts) = match parts.as_slice() {
        [s, d] => (*s, *d, ""),
        [s, d, o] => (*s, *d, *o),
        _ => bail!("invalid bind {:?}", bind),
    };
    let mut options = vec!["rbind".to_string(), "rprivate".to_string()];
    let ro = opts.split(',').any(|o| o == "ro");
    options.push(if ro { "ro" } else { "rw" }.to_string());
    Ok(json!({"destination": dst, "type": "bind", "source": src, "options": options}))
}

fn tmpfs_mount(dst: &str, opts: &str) -> Value {
    let mut options: Vec<String> = vec!["nosuid".into(), "nodev".into()];
    options.extend(
        opts.split(',')
            .filter(|o| !o.is_empty())
            .map(str::to_string),
    );
    json!({"destination": dst, "type": "tmpfs", "source": "tmpfs", "options": options})
}

fn namespace(kind: &str, ns: &Ns) -> Option<Value> {
    match ns {
        Ns::New => Some(json!({"type": kind})),
        Ns::Join(p) => Some(json!({"type": kind, "path": p})),
        Ns::Host => None,
    }
}

/// Build `config.json`.
pub fn build(input: &SpecInput) -> Result<Value> {
    let hc = input.config.host_config.clone().unwrap_or_default();
    if hc.mounts.as_ref().is_some_and(|m| !m.is_empty()) {
        bail!("unsupported on youki: named volume mounts");
    }
    if hc.port_bindings.as_ref().is_some_and(|p| {
        p.values()
            .any(|b| b.as_ref().is_some_and(|b| !b.is_empty()))
    }) {
        bail!("unsupported on youki: hostPort bindings (use a CNI portmap plugin)");
    }

    let hostname = input.config.hostname.clone().unwrap_or_default();
    let mut spec = serde_json::to_value(oci_spec::runtime::Spec::default())?;

    let args = process_args(input.config, input.image)?;
    let cwd = input
        .config
        .working_dir
        .clone()
        .filter(|w| !w.is_empty())
        .or_else(|| input.image.working_dir.clone())
        .unwrap_or_else(|| "/".to_string());
    let caps = capabilities(&hc);
    let no_new_privs = hc
        .security_opt
        .iter()
        .flatten()
        .any(|o| o.starts_with("no-new-privileges"));
    spec["process"] = json!({
        "terminal": false,
        "user": {"uid": input.uid, "gid": input.gid},
        "args": args,
        "env": process_env(input.config, input.image, &hostname),
        "cwd": cwd,
        "capabilities": {"bounding": caps, "effective": caps, "permitted": caps},
        "rlimits": [{"type": "RLIMIT_NOFILE", "hard": 1048576u64, "soft": 1048576u64}],
        "noNewPrivileges": no_new_privs,
    });
    spec["root"] = json!({"path": input.rootfs, "readonly": hc.readonly_rootfs.unwrap_or(false)});
    if input.uts == Ns::New && !hostname.is_empty() {
        spec["hostname"] = json!(hostname);
    } else {
        spec.as_object_mut().unwrap().remove("hostname");
    }

    let mut mounts = spec["mounts"].as_array().cloned().unwrap_or_default();
    let privileged = hc.privileged == Some(true);
    if privileged {
        for m in mounts.iter_mut() {
            if m["destination"] == "/sys" {
                m["options"] = json!(["nosuid", "noexec", "nodev", "rw"]);
            }
        }
    }
    let mut targets: Vec<String> = Vec::new();
    for b in hc.binds.iter().flatten() {
        let m = bind_mount(b)?;
        targets.push(m["destination"].as_str().unwrap_or("").to_string());
        mounts.push(m);
    }
    for (dst, opts) in hc.tmpfs.iter().flatten() {
        targets.push(dst.clone());
        mounts.push(tmpfs_mount(dst, opts));
    }
    if let Some(resolv) = input.resolv_conf {
        if !targets.iter().any(|t| t == "/etc/resolv.conf") {
            mounts.push(json!({
                "destination": "/etc/resolv.conf", "type": "bind", "source": resolv,
                "options": ["rbind", "rprivate", "ro"]
            }));
        }
    }
    spec["mounts"] = json!(mounts);

    let linux = &mut spec["linux"];
    linux["namespaces"] = json!([
        namespace("pid", &input.pid),
        namespace("network", &input.net),
        namespace("ipc", &input.ipc),
        namespace("uts", &input.uts),
        Some(json!({"type": "mount"})),
        Some(json!({"type": "cgroup"})),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>());
    linux["cgroupsPath"] = json!(input.cgroups_path);
    if privileged {
        linux["maskedPaths"] = json!([]);
        linux["readonlyPaths"] = json!([]);
    }
    if let Some(sysctls) = &hc.sysctls {
        linux["sysctl"] = json!(sysctls);
    }
    let mut resources = json!({"devices": []});
    if let Some(m) = hc.memory.filter(|m| *m > 0) {
        resources["memory"] = json!({"limit": m});
    }
    let mut cpu = serde_json::Map::new();
    if let Some(s) = hc.cpu_shares.filter(|s| *s > 0) {
        cpu.insert("shares".into(), json!(s));
    }
    if let Some(q) = hc.cpu_quota.filter(|q| *q > 0) {
        cpu.insert("quota".into(), json!(q));
    }
    if let Some(p) = hc.cpu_period.filter(|p| *p > 0) {
        cpu.insert("period".into(), json!(p));
    }
    if !cpu.is_empty() {
        resources["cpu"] = Value::Object(cpu);
    }
    linux["resources"] = resources;

    // Round-trip through the typed spec so libcontainer will accept it.
    let typed: oci_spec::runtime::Spec = serde_json::from_value(spec)?;
    Ok(serde_json::to_value(typed)?)
}

/// resolv.conf contents from the container's DNS settings, or `None` to
/// use the host's.
pub fn resolv_conf(hc: &HostConfig) -> Option<String> {
    let servers = hc.dns.as_ref().filter(|d| !d.is_empty())?;
    let mut out = String::new();
    for s in servers {
        out.push_str(&format!("nameserver {}\n", s));
    }
    if let Some(search) = hc.dns_search.as_ref().filter(|s| !s.is_empty()) {
        out.push_str(&format!("search {}\n", search.join(" ")));
    }
    if let Some(opts) = hc.dns_options.as_ref().filter(|o| !o.is_empty()) {
        out.push_str(&format!("options {}\n", opts.join(" ")));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Option<Vec<String>> {
        Some(v.iter().map(|x| x.to_string()).collect())
    }

    #[test]
    fn args_follow_docker_rules() {
        let image = ImageConfig {
            entrypoint: s(&["/ep"]),
            cmd: s(&["default"]),
            ..Default::default()
        };
        let mut c = ContainerConfig::default();
        assert_eq!(process_args(&c, &image).unwrap(), ["/ep", "default"]);
        c.cmd = s(&["x"]);
        assert_eq!(process_args(&c, &image).unwrap(), ["/ep", "x"]);
        c.cmd = None;
        c.entrypoint = s(&["/bin/sh", "-c", "true"]);
        assert_eq!(process_args(&c, &image).unwrap(), ["/bin/sh", "-c", "true"]);
        c.cmd = Some(vec![]);
        assert_eq!(process_args(&c, &image).unwrap(), ["/bin/sh", "-c", "true"]);
        assert!(process_args(&ContainerConfig::default(), &ImageConfig::default()).is_err());
    }

    #[test]
    fn env_overrides_image_and_adds_path_and_hostname() {
        let image = ImageConfig {
            env: s(&["A=1", "B=2"]),
            ..Default::default()
        };
        let c = ContainerConfig {
            env: s(&["B=3", "C=4"]),
            ..Default::default()
        };
        assert_eq!(
            process_env(&c, &image, "web"),
            [DEFAULT_PATH, "A=1", "B=3", "C=4", "HOSTNAME=web"]
        );
    }

    #[test]
    fn capabilities_add_drop_and_privileged() {
        let hc = HostConfig {
            cap_add: s(&["NET_ADMIN"]),
            cap_drop: s(&["chown"]),
            ..Default::default()
        };
        let caps = capabilities(&hc);
        assert!(caps.contains(&"CAP_NET_ADMIN".to_string()));
        assert!(!caps.contains(&"CAP_CHOWN".to_string()));
        let hc = HostConfig {
            cap_drop: s(&["ALL"]),
            cap_add: s(&["NET_BIND_SERVICE"]),
            ..Default::default()
        };
        assert_eq!(capabilities(&hc), ["CAP_NET_BIND_SERVICE"]);
        let hc = HostConfig {
            privileged: Some(true),
            ..Default::default()
        };
        assert_eq!(capabilities(&hc).len(), ALL_CAPS.len());
    }

    fn input<'a>(c: &'a ContainerConfig, i: &'a ImageConfig) -> SpecInput<'a> {
        SpecInput {
            config: c,
            image: i,
            rootfs: Path::new("/r/rootfs"),
            uid: 1000,
            gid: 1000,
            net: Ns::Join("/var/run/netns/cni-web".into()),
            ipc: Ns::New,
            pid: Ns::New,
            uts: Ns::New,
            resolv_conf: Some(Path::new("/r/resolv.conf")),
            cgroups_path: "/rusternetes/web_app",
        }
    }

    #[test]
    fn spec_has_namespaces_mounts_and_limits() {
        let c = ContainerConfig {
            cmd: s(&["sleep", "1"]),
            hostname: Some("web".into()),
            host_config: Some(HostConfig {
                binds: s(&["/host/data:/data:ro", "/host/hosts:/etc/hosts"]),
                tmpfs: Some([("/tmp".to_string(), "size=64m".to_string())].into()),
                memory: Some(64 << 20),
                cpu_quota: Some(50000),
                cpu_period: Some(100000),
                security_opt: s(&["no-new-privileges"]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let image = ImageConfig::default();
        let spec = build(&input(&c, &image)).unwrap();
        assert_eq!(spec["process"]["args"], json!(["sleep", "1"]));
        assert_eq!(spec["process"]["user"]["uid"], 1000);
        assert_eq!(spec["process"]["noNewPrivileges"], true);
        assert_eq!(spec["hostname"], "web");
        let ns = spec["linux"]["namespaces"].as_array().unwrap();
        assert!(ns
            .iter()
            .any(|n| n["type"] == "network" && n["path"] == "/var/run/netns/cni-web"));
        let mounts = spec["mounts"].as_array().unwrap();
        let data = mounts.iter().find(|m| m["destination"] == "/data").unwrap();
        assert!(data["options"].as_array().unwrap().contains(&json!("ro")));
        assert!(mounts
            .iter()
            .any(|m| m["destination"] == "/tmp" && m["type"] == "tmpfs"));
        assert!(mounts
            .iter()
            .any(|m| m["destination"] == "/etc/resolv.conf"));
        assert_eq!(spec["linux"]["resources"]["memory"]["limit"], 64 << 20);
        assert_eq!(spec["linux"]["resources"]["cpu"]["quota"], 50000);
        assert_eq!(spec["linux"]["cgroupsPath"], "/rusternetes/web_app");
    }

    #[test]
    fn joined_uts_drops_hostname_and_host_net_drops_namespace() {
        let c = ContainerConfig {
            cmd: s(&["true"]),
            hostname: Some("web".into()),
            ..Default::default()
        };
        let image = ImageConfig::default();
        let mut i = input(&c, &image);
        i.uts = Ns::Join("/proc/1/ns/uts".into());
        i.net = Ns::Host;
        let spec = build(&i).unwrap();
        assert!(spec.get("hostname").is_none() || spec["hostname"].is_null());
        let ns = spec["linux"]["namespaces"].as_array().unwrap();
        assert!(!ns.iter().any(|n| n["type"] == "network"));
    }

    #[test]
    fn host_ports_and_named_volumes_are_refused() {
        let image = ImageConfig::default();
        let c = ContainerConfig {
            cmd: s(&["true"]),
            host_config: Some(HostConfig {
                port_bindings: Some(
                    [(
                        "80/tcp".to_string(),
                        Some(vec![crate::backend::PortBinding {
                            host_ip: None,
                            host_port: Some("8080".into()),
                        }]),
                    )]
                    .into(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = build(&input(&c, &image)).unwrap_err().to_string();
        assert!(err.contains("unsupported on youki"), "{err}");
    }

    #[test]
    fn resolv_conf_from_dns_settings() {
        let hc = HostConfig {
            dns: s(&["10.96.0.10"]),
            dns_search: s(&["default.svc.cluster.local", "svc.cluster.local"]),
            dns_options: s(&["ndots:5"]),
            ..Default::default()
        };
        assert_eq!(
            resolv_conf(&hc).unwrap(),
            "nameserver 10.96.0.10\nsearch default.svc.cluster.local svc.cluster.local\noptions ndots:5\n"
        );
        assert!(resolv_conf(&HostConfig::default()).is_none());
    }
}
