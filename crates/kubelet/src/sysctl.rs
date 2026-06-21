//! Kubelet sysctl admission allowlist.
//!
//! Faithful port of upstream `pkg/kubelet/sysctl` (`allowlist.go`,
//! `safe_sysctls.go`) plus `staging/src/k8s.io/component-helpers/node/util/sysctl`
//! (`namespace.go`, `sysctl.go::NormalizeName`).
//!
//! A pod that declares `spec.securityContext.sysctls` is admitted only if every
//! sysctl is *safe* (namespaced in the pod/container **and** isolated — no
//! influence on other pods) or has been explicitly permitted by the operator via
//! `--allowed-unsafe-sysctls`. Otherwise the kubelet rejects the pod with reason
//! `SysctlForbidden` (upstream `ForbiddenReason`).
//!
//! Note (parity): being allowlisted is *necessary but not sufficient* — the
//! container runtime may still refuse to launch a pod whose sysctl the running
//! kernel does not expose. Upstream gates a handful of safe sysctls on a minimum
//! kernel version; we list them unconditionally (modern kernels expose them, and
//! the runtime is the backstop), which can only ever be more permissive on the
//! *safe* set, never on the unsafe one.

use std::collections::HashMap;

use rusternetes_common::resources::pod::Pod;

/// Reason set on a pod rejected by sysctl admission (upstream `ForbiddenReason`).
pub const FORBIDDEN_REASON: &str = "SysctlForbidden";

/// The kernel namespace a sysctl belongs to. A sysctl with an
/// [`Namespace::Unknown`] classification is not known to be per-pod namespaced
/// and can never be allowlisted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Namespace {
    /// IPC namespace (`man 7 ipc_namespaces`).
    Ipc,
    /// Network namespace (`man 7 network_namespaces`).
    Net,
    /// Not known to be namespaced.
    Unknown,
}

impl Namespace {
    /// String used in the host-namespace rejection message, matching upstream's
    /// `Namespace` values (`"IPC"` / `"Net"`).
    fn as_str(self) -> &'static str {
        match self {
            Namespace::Ipc => "IPC",
            Namespace::Net => "Net",
            Namespace::Unknown => "",
        }
    }
}

/// `nameToNamespace` from upstream `namespace.go` (exact matches).
fn name_to_namespace(name: &str) -> Option<Namespace> {
    Some(match name {
        // kernel semaphore parameters: SEMMSL, SEMMNS, SEMOPM, SEMMNI.
        "kernel.sem" => Namespace::Ipc,
        // kernel shared-memory limits: shmall, shmmax, shmmni, shm_rmid_forced
        // (plus the `kernel.shm` backward-compat key).
        "kernel.shmall"
        | "kernel.shmmax"
        | "kernel.shmmni"
        | "kernel.shm_rmid_forced"
        | "kernel.shm" => Namespace::Ipc,
        // kernel messages: msgmni, msgmax, msgmnb (plus `kernel.msg`).
        "kernel.msgmax" | "kernel.msgmnb" | "kernel.msgmni" | "kernel.msg" => Namespace::Ipc,
        _ => return None,
    })
}

/// `namespaceOf` from upstream `namespace.go`: exact match first, then the
/// `prefixToNamespace` map (`net` → Net, `fs.mqueue` → IPC), matched as
/// `prefix + "."`.
fn namespace_of(val: &str) -> Namespace {
    if let Some(ns) = name_to_namespace(val) {
        return ns;
    }
    if val.starts_with("net.") {
        return Namespace::Net;
    }
    if val.starts_with("fs.mqueue.") {
        return Namespace::Ipc;
    }
    Namespace::Unknown
}

/// Port of `NormalizeName` (`sysctl.go`): names may use `.` or `/` separators.
/// If the first separator is `.` (or there is none), the name is already in
/// canonical dot form. If the first separator is `/`, swap every `.`↔`/` so a
/// `net/ipv4/...` name maps onto its `net.ipv4...` canonical form.
fn normalize_name(val: &str) -> String {
    if val.is_empty() {
        return String::new();
    }
    match val.find(['.', '/']) {
        None => val.to_string(),
        Some(i) if val.as_bytes()[i] == b'.' => val.to_string(),
        Some(_) => val
            .chars()
            .map(|c| match c {
                '.' => '/',
                '/' => '.',
                other => other,
            })
            .collect(),
    }
}

/// Port of `GetNamespace`: normalize, split off the `*` suffix (if any) to get
/// the lookup key, classify the namespace. Returns `(namespace, key, prefixed)`.
fn get_namespace(sysctl: &str) -> (Namespace, String, bool) {
    let mut key = normalize_name(sysctl);
    let mut prefixed = false;
    if let Some(idx) = key.find('*') {
        key.truncate(idx);
        prefixed = true;
    }
    let ns = namespace_of(&key);
    (ns, key, prefixed)
}

/// Safe sysctls — namespaced *and* isolated. Mirrors upstream `safeSysctls`
/// (`safe_sysctls.go`). The trailing block is kernel-version-gated upstream;
/// see the module note on why we list it unconditionally.
const SAFE_SYSCTLS: &[&str] = &[
    "kernel.shm_rmid_forced",
    "net.ipv4.ip_local_port_range",
    "net.ipv4.tcp_syncookies",
    "net.ipv4.ping_group_range",
    "net.ipv4.ip_unprivileged_port_start",
    "net.ipv4.ip_local_reserved_ports",
    "net.ipv4.tcp_keepalive_time",
    "net.ipv4.tcp_fin_timeout",
    "net.ipv4.tcp_keepalive_intvl",
    "net.ipv4.tcp_keepalive_probes",
    "net.ipv4.tcp_rmem",
    "net.ipv4.tcp_wmem",
];

/// The sysctl admission allowlist (upstream `patternAllowlist`): the safe set
/// plus any operator-permitted unsafe sysctls/prefixes, classified by kernel
/// namespace so host-namespace pods can be forbidden the namespaced ones.
pub struct Allowlist {
    sysctls: HashMap<String, Namespace>,
    prefixes: HashMap<String, Namespace>,
}

impl Allowlist {
    /// Build from the static safe set plus `--allowed-unsafe-sysctls` patterns
    /// (exact names or `*`-suffixed prefixes). Upstream `NewAllowlist` fails
    /// kubelet startup on a custom pattern that is not known to be namespaced;
    /// we instead skip such an entry with a warning so a single malformed flag
    /// value cannot wedge the whole kubelet.
    pub fn new(allowed_unsafe: &[String]) -> Self {
        let mut w = Allowlist {
            sysctls: HashMap::new(),
            prefixes: HashMap::new(),
        };
        let patterns = SAFE_SYSCTLS
            .iter()
            .map(|s| (*s).to_string())
            .chain(allowed_unsafe.iter().cloned());
        for pattern in patterns {
            let (ns, key, prefixed) = get_namespace(&pattern);
            if ns == Namespace::Unknown {
                tracing::warn!(
                    "ignoring allowed-unsafe sysctl {pattern:?}: not known to be namespaced"
                );
                continue;
            }
            if prefixed {
                w.prefixes.insert(key, ns);
            } else {
                w.sysctls.insert(key, ns);
            }
        }
        w
    }

    /// Port of `validateSysctl`: a sysctl is allowed when it (or a registered
    /// prefix) is in the allowlist, unless the pod shares the relevant host
    /// namespace (IPC/Net), which forbids the corresponding sysctls.
    fn validate(&self, name: &str, host_net: bool, host_ipc: bool) -> Result<(), String> {
        let name = normalize_name(name);
        let ns_check = |ns: Namespace| -> Result<(), String> {
            if ns == Namespace::Ipc && host_ipc {
                return Err(format!(
                    "{name:?} not allowed with host {} enabled",
                    ns.as_str()
                ));
            }
            if ns == Namespace::Net && host_net {
                return Err(format!(
                    "{name:?} not allowed with host {} enabled",
                    ns.as_str()
                ));
            }
            Ok(())
        };
        if let Some(&ns) = self.sysctls.get(&name) {
            return ns_check(ns);
        }
        for (p, &ns) in &self.prefixes {
            if name.starts_with(p) {
                return ns_check(ns);
            }
        }
        Err(format!("{name:?} not allowlisted"))
    }

    /// Port of `Admit`: check every sysctl declared in the pod's security
    /// context. Returns `Err(message)` for the first forbidden sysctl (the
    /// caller stamps `phase=Failed` / `reason=SysctlForbidden` with this
    /// message), or `Ok(())` if the pod has no sysctls or all are allowed.
    pub fn admit(&self, pod: &Pod) -> Result<(), String> {
        let sysctls = match pod
            .spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.sysctls.as_ref())
        {
            Some(list) if !list.is_empty() => list,
            _ => return Ok(()),
        };
        let host_net = pod
            .spec
            .as_ref()
            .and_then(|s| s.host_network)
            .unwrap_or(false);
        let host_ipc = pod.spec.as_ref().and_then(|s| s.host_ipc).unwrap_or(false);
        for s in sysctls {
            self.validate(&s.name, host_net, host_ipc)
                .map_err(|e| format!("forbidden sysctl: {e}"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::pod::{PodSecurityContext, PodSpec, Sysctl};

    fn pod_with(sysctls: Vec<(&str, &str)>, host_net: bool, host_ipc: bool) -> Pod {
        let spec = PodSpec {
            host_network: Some(host_net),
            host_ipc: Some(host_ipc),
            security_context: Some(PodSecurityContext {
                sysctls: Some(
                    sysctls
                        .into_iter()
                        .map(|(n, v)| Sysctl {
                            name: n.to_string(),
                            value: v.to_string(),
                        })
                        .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        };
        Pod::new("p", spec)
    }

    #[test]
    fn no_sysctls_admitted() {
        assert!(Allowlist::new(&[])
            .admit(&Pod::new("p", PodSpec::default()))
            .is_ok());
    }

    #[test]
    fn safe_sysctls_admitted() {
        let w = Allowlist::new(&[]);
        let pod = pod_with(
            vec![
                ("kernel.shm_rmid_forced", "1"),
                ("net.ipv4.ip_local_port_range", "1024 65000"),
            ],
            false,
            false,
        );
        assert!(w.admit(&pod).is_ok());
    }

    #[test]
    fn slash_separated_safe_name_normalized_and_admitted() {
        let w = Allowlist::new(&[]);
        let pod = pod_with(vec![("kernel/shm_rmid_forced", "1")], false, false);
        assert!(w.admit(&pod).is_ok());
    }

    #[test]
    fn unsafe_sysctl_rejected_by_default() {
        let w = Allowlist::new(&[]);
        let pod = pod_with(vec![("kernel.msgmax", "8192")], false, false);
        let err = w.admit(&pod).unwrap_err();
        assert!(err.contains("not allowlisted"), "got: {err}");
        assert!(err.starts_with("forbidden sysctl:"), "got: {err}");
    }

    #[test]
    fn allowed_unsafe_exact_admitted() {
        let w = Allowlist::new(&["kernel.msgmax".to_string()]);
        let pod = pod_with(vec![("kernel.msgmax", "8192")], false, false);
        assert!(w.admit(&pod).is_ok());
    }

    #[test]
    fn allowed_unsafe_prefix_admitted() {
        let w = Allowlist::new(&["kernel.msg*".to_string()]);
        let pod = pod_with(vec![("kernel.msgmnb", "16384")], false, false);
        assert!(w.admit(&pod).is_ok());
    }

    #[test]
    fn net_sysctl_forbidden_with_host_network() {
        let w = Allowlist::new(&[]);
        let pod = pod_with(vec![("net.ipv4.tcp_syncookies", "1")], true, false);
        let err = w.admit(&pod).unwrap_err();
        assert!(
            err.contains("not allowed with host Net enabled"),
            "got: {err}"
        );
    }

    #[test]
    fn ipc_sysctl_forbidden_with_host_ipc() {
        let w = Allowlist::new(&["kernel.msgmax".to_string()]);
        let pod = pod_with(vec![("kernel.msgmax", "8192")], false, true);
        let err = w.admit(&pod).unwrap_err();
        assert!(
            err.contains("not allowed with host IPC enabled"),
            "got: {err}"
        );
    }

    #[test]
    fn unknown_namespace_unsafe_pattern_skipped() {
        // `vm.swappiness` is not a namespaced sysctl; the operator entry is
        // dropped, so a pod requesting it is still rejected.
        let w = Allowlist::new(&["vm.swappiness".to_string()]);
        let pod = pod_with(vec![("vm.swappiness", "1")], false, false);
        assert!(w.admit(&pod).is_err());
    }
}
