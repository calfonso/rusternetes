//! Translate a rusternetes [`Pod`] into CRI v1 sandbox and container configs.
//!
//! These are pure functions — they take a `Pod` (and, for containers, a
//! resolved image ref and a volume-name → host-path map) and produce the
//! `runtime.v1` config messages the CRI runtime expects. Keeping them pure makes
//! the Pod→CRI mapping unit-testable without a running runtime, and isolates the
//! single place where rusternetes' resource model meets the CRI wire model.
//!
//! Scope: the fields needed to launch a pod — metadata, labels, namespaces,
//! command/args/env, ports, resources, mounts, and the linux security context.
//! `valueFrom` env is resolved here from inputs the kubelet fetches first:
//! `fieldRef`/`resourceFieldRef` from the pod itself, `configMapKeyRef`/
//! `secretKeyRef` from caller-supplied ConfigMap/Secret maps (the runtime reads
//! these from storage before translation — see `container_config`). `envFrom`
//! bulk-imports a ConfigMap/Secret's keys (optionally prefixed). Deferred
//! (tracked separately): probes (driven kubelet-side via ExecSync) and windows
//! configs.

use std::collections::HashMap;

use rusternetes_common::resources::pod::{Container, Pod};
use rusternetes_common::resources::{ConfigMap, Secret, Service};
use rusternetes_cri::v1;

/// Well-known CRI metadata label keys the runtime indexes sandboxes/containers
/// by. They mirror the keys the upstream kubelet sets so `crictl`/tools work.
pub(crate) mod labels {
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
                protocol: protocol_to_cri(Some(p.protocol.as_str())),
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

/// Sysctls to apply to the pod sandbox, from `spec.securityContext.sysctls`.
///
/// CRI carries these as `LinuxPodSandboxConfig.sysctls` (a `name -> value`
/// map); the runtime applies them to the sandbox's network/IPC namespaces. We
/// pass through what the pod declares — admission (allowed/unsafe-sysctl
/// gating) is a separate concern upstream, not the translation layer's.
fn sysctls(pod: &Pod) -> HashMap<String, String> {
    pod.spec
        .as_ref()
        .and_then(|s| s.security_context.as_ref())
        .and_then(|sc| sc.sysctls.as_ref())
        .map(|list| {
            list.iter()
                .map(|s| (s.name.clone(), s.value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// De-duplicate a sequence of strings, preserving first-seen order
/// (upstream `omitDuplicates`).
fn dedup<I: IntoIterator<Item = String>>(iter: I) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    iter.into_iter()
        .filter(|x| seen.insert(x.clone()))
        .collect()
}

/// Merge resolv.conf options name-keyed (upstream `mergeDNSOptions`): an option
/// from the pod's `dnsConfig` overrides a base option of the same name (e.g.
/// `ndots`), otherwise it is appended. Each option renders as `name` or
/// `name:value`.
fn merge_dns_options(
    base: Vec<String>,
    extra: &[rusternetes_common::resources::pod::PodDNSConfigOption],
) -> Vec<String> {
    let opt_name = |s: &str| s.split(':').next().unwrap_or(s).to_string();
    let mut out = base;
    for o in extra {
        let rendered = match &o.value {
            Some(v) => format!("{}:{}", o.name, v),
            None => o.name.clone(),
        };
        match out.iter().position(|e| opt_name(e) == o.name) {
            Some(pos) => out[pos] = rendered,
            None => out.push(rendered),
        }
    }
    out
}

/// Build the CRI [`DnsConfig`](v1::DnsConfig) for a pod from its
/// `dnsPolicy`/`dnsConfig`, the cluster DNS server IPs, and the cluster domain.
/// Ports the upstream kubelet `dns.Configurer.GetPodDNS`
/// (`pkg/kubelet/network/dns/dns.go`).
///
/// Returns `None` for the `Default` policy — and for `ClusterFirst*` when no
/// cluster DNS is configured (upstream falls back to `Default` there). Leaving
/// `PodSandboxConfig.dns_config` unset makes the runtime copy the host's
/// `/etc/resolv.conf` into the sandbox, which is exactly "inherit node DNS".
/// (Merging an explicit `dnsConfig` onto the host base for `Default` would need
/// the kubelet to read the host resolv.conf; not done here.)
pub fn dns_config(
    pod: &Pod,
    cluster_dns: &[String],
    cluster_domain: &str,
) -> Option<v1::DnsConfig> {
    let policy = pod
        .spec
        .as_ref()
        .and_then(|s| s.dns_policy.as_deref())
        // The api-server defaults an unset dnsPolicy to ClusterFirst.
        .unwrap_or("ClusterFirst");

    let mut cfg = match policy {
        // DNSNone: empty base, populated solely from the pod's dnsConfig.
        "None" => v1::DnsConfig::default(),
        // ClusterFirst on a hostNetwork pod falls back to Default (host DNS),
        // matching upstream getPodDNSType (pkg/kubelet/network/dns/dns.go): only
        // ClusterFirstWithHostNet keeps clusterDNS when sharing the host netns.
        // Without this, hostNetwork control-plane static pods (scheduler /
        // controller-manager, dnsPolicy unset => ClusterFirst) get
        // `nameserver <clusterDNS>` and can't resolve the `api-server` Docker
        // alias before cluster DNS is up — their reflectors never sync and the
        // cluster never schedules anything.
        "ClusterFirst" if host_network(pod) => return None,
        "ClusterFirst" | "ClusterFirstWithHostNet" => {
            if cluster_dns.is_empty() {
                return None; // no ClusterDNS -> fall back to Default (host DNS)
            }
            let domain = if cluster_domain.is_empty() {
                "cluster.local"
            } else {
                cluster_domain
            };
            let ns = namespace(pod);
            v1::DnsConfig {
                servers: cluster_dns.to_vec(),
                searches: vec![
                    format!("{ns}.svc.{domain}"),
                    format!("svc.{domain}"),
                    domain.to_string(),
                ],
                options: vec!["ndots:5".to_string()],
            }
        }
        // "Default" and any unknown value: inherit the node's resolv.conf.
        _ => return None,
    };

    // appendDNSConfig: additively merge the pod's explicit dnsConfig.
    if let Some(dns) = pod.spec.as_ref().and_then(|s| s.dns_config.as_ref()) {
        if let Some(ns) = dns.nameservers.as_ref() {
            cfg.servers = dedup(cfg.servers.into_iter().chain(ns.iter().cloned()));
        }
        if let Some(s) = dns.searches.as_ref() {
            cfg.searches = dedup(cfg.searches.into_iter().chain(s.iter().cloned()));
        }
        if let Some(opts) = dns.options.as_ref() {
            cfg.options = merge_dns_options(cfg.options, opts);
        }
    }

    Some(cfg)
}

/// Whether any container in the pod requests `privileged` (upstream
/// `kubecontainer.HasPrivilegedContainer`). Covers regular + init containers.
fn pod_has_privileged_container(pod: &Pod) -> bool {
    let Some(spec) = pod.spec.as_ref() else {
        return false;
    };
    let any_priv = |cs: &[Container]| {
        cs.iter().any(|c| {
            c.security_context
                .as_ref()
                .and_then(|s| s.privileged)
                .unwrap_or(false)
        })
    };
    any_priv(&spec.containers)
        || spec
            .init_containers
            .as_deref()
            .map(any_priv)
            .unwrap_or(false)
}

/// Build the pod sandbox's [`LinuxSandboxSecurityContext`], porting upstream
/// `generatePodSandboxLinuxConfig` (`pkg/kubelet/kuberuntime/kuberuntime_sandbox.go`).
///
/// Beyond the namespace options we already set, this carries the pod-level
/// security context onto the sandbox: `privileged` (if any container is),
/// `runAsUser`/`runAsGroup`, `supplementalGroups` (`fsGroup` first, then the
/// pod's `supplementalGroups`) + its policy, and `seLinuxOptions`. The sandbox
/// seccomp is forced to `RuntimeDefault` so pods can still pick least-privileged
/// seccomp at the container level (upstream issue #84623).
fn sandbox_security_context(pod: &Pod) -> v1::LinuxSandboxSecurityContext {
    use v1::security_profile::ProfileType;
    let mut sc = v1::LinuxSandboxSecurityContext {
        namespace_options: Some(namespace_options(pod)),
        privileged: pod_has_privileged_container(pod),
        seccomp: Some(v1::SecurityProfile {
            profile_type: ProfileType::RuntimeDefault as i32,
            localhost_ref: String::new(),
        }),
        ..Default::default()
    };
    if let Some(psc) = pod.spec.as_ref().and_then(|s| s.security_context.as_ref()) {
        sc.run_as_user = psc.run_as_user.map(|v| v1::Int64Value { value: v });
        sc.run_as_group = psc.run_as_group.map(|v| v1::Int64Value { value: v });
        let mut groups: Vec<i64> = Vec::new();
        if let Some(fsg) = psc.fs_group {
            groups.push(fsg);
        }
        if let Some(sg) = psc.supplemental_groups.as_ref() {
            groups.extend(sg.iter().copied());
        }
        sc.supplemental_groups = groups;
        if let Some(policy) = psc.supplemental_groups_policy.as_deref() {
            sc.supplemental_groups_policy = match policy {
                "Strict" => v1::SupplementalGroupsPolicy::Strict as i32,
                // "Merge" (default) and any unknown value.
                _ => v1::SupplementalGroupsPolicy::Merge as i32,
            };
        }
        if let Some(opts) = psc.se_linux_options.as_ref() {
            sc.selinux_options = Some(v1::SeLinuxOption {
                user: opts.user.clone().unwrap_or_default(),
                role: opts.role.clone().unwrap_or_default(),
                r#type: opts.type_.clone().unwrap_or_default(),
                level: opts.level.clone().unwrap_or_default(),
            });
        }
    }
    sc
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
            security_context: Some(sandbox_security_context(pod)),
            sysctls: sysctls(pod),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Translate env vars, resolving each `valueFrom` source in declared order.
/// Literal values pass through; `fieldRef`/`resourceFieldRef` resolve from the
/// pod; `configMapKeyRef`/`secretKeyRef` resolve from `config_maps`/`secrets`
/// (fetched from storage by the caller). A ref whose source/key is absent
/// yields no var (matching the optional case); the caller is responsible for
/// pre-validating non-optional refs.
/// Insert or override `key` in an ordered env list (env vars are name-unique;
/// a later write replaces an earlier one in place, keeping its position).
fn upsert_env(out: &mut Vec<v1::KeyValue>, key: String, value: String) {
    match out.iter_mut().find(|kv| kv.key == key) {
        Some(kv) => kv.value = value,
        None => out.push(v1::KeyValue { key, value }),
    }
}

/// Expand `$(VAR)` references in `input` against `mapping`, a faithful port of
/// upstream `third_party/forked/golang/expansion.Expand` +
/// `MappingFuncFor`:
/// - `$(VAR)` → `mapping[VAR]`, or the verbatim `$(VAR)` if VAR is unknown;
/// - `$$` → a literal `$`;
/// - an incomplete `$(` and a `$` not starting an expression are literal.
///
/// The kubelet runs this over both env values (against the vars defined so far)
/// and container command/args (against the full container env) — see
/// `pkg/kubelet/kubelet_pods.go::makeEnvironmentVariables` and
/// `kuberuntime_container.go::expandContainerCommandAndArgs`.
pub(crate) fn expand(input: &str, mapping: &HashMap<String, String>) -> String {
    let b = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut cursor = 0;
    while cursor < b.len() {
        if b[cursor] == b'$' && cursor + 1 < b.len() {
            match b[cursor + 1] {
                b'$' => {
                    out.push('$');
                    cursor += 2;
                    continue;
                }
                b'(' => {
                    if let Some(rel) = b[cursor + 2..].iter().position(|&c| c == b')') {
                        let name = &input[cursor + 2..cursor + 2 + rel];
                        match mapping.get(name) {
                            Some(v) => out.push_str(v),
                            // Unknown var: leave the whole `$(name)` verbatim.
                            None => out.push_str(&input[cursor..cursor + 2 + rel + 1]),
                        }
                        cursor = cursor + 2 + rel + 1;
                        continue;
                    }
                    // Incomplete reference `$(` → literal.
                    out.push_str("$(");
                    cursor += 2;
                    continue;
                }
                _ => {
                    // `$` not starting an expression: emit `$` + the next char.
                    out.push('$');
                    let ch = input[cursor + 1..].chars().next().unwrap();
                    out.push(ch);
                    cursor += 1 + ch.len_utf8();
                    continue;
                }
            }
        }
        let ch = input[cursor..].chars().next().unwrap();
        out.push(ch);
        cursor += ch.len_utf8();
    }
    out
}

/// Expand `$(VAR)` in every element of a command/args vector against `env`.
fn expand_all(items: Vec<String>, env: &HashMap<String, String>) -> Vec<String> {
    items.iter().map(|s| expand(s, env)).collect()
}

fn env_vars(
    pod: &Pod,
    container: &Container,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
    node_allocatable: Option<&HashMap<String, String>>,
) -> Vec<v1::KeyValue> {
    let mut out: Vec<v1::KeyValue> = Vec::new();
    // Mirror of `out` for `$(VAR)` lookups: upstream expands each literal env
    // value against the vars defined so far (`makeEnvironmentVariables`).
    let mut seen: HashMap<String, String> = HashMap::new();

    // 1. envFrom: bulk-expand referenced ConfigMaps/Secrets (declared order),
    //    each key optionally prefixed. A later source overrides an earlier one.
    if let Some(sources) = container.env_from.as_ref() {
        for src in sources {
            let prefix = src.prefix.as_deref().unwrap_or("");
            if let Some(cmr) = src.config_map_ref.as_ref() {
                if let Some(data) = config_maps.get(&cmr.name).and_then(|cm| cm.data.as_ref()) {
                    for (k, v) in data {
                        let key = format!("{prefix}{k}");
                        seen.insert(key.clone(), v.clone());
                        upsert_env(&mut out, key, v.clone());
                    }
                }
            }
            if let Some(skr) = src.secret_ref.as_ref() {
                if let Some(data) = secrets.get(&skr.name).and_then(|s| s.data.as_ref()) {
                    for (k, v) in data {
                        // Secret data is decoded bytes; env values are the UTF-8
                        // interpretation (upstream casts []byte→string).
                        let key = format!("{prefix}{k}");
                        let val = String::from_utf8_lossy(v).into_owned();
                        seen.insert(key.clone(), val.clone());
                        upsert_env(&mut out, key, val);
                    }
                }
            }
        }
    }

    // 2. Individual env entries (declared order); each overrides any
    //    envFrom-supplied var of the same name.
    if let Some(env) = container.env.as_ref() {
        for e in env {
            let value = if let Some(v) = e.value.as_ref() {
                // Literal values get `$(VAR)` expanded against vars defined so
                // far (upstream `makeEnvironmentVariables`); `valueFrom` does not.
                Some(expand(v, &seen))
            } else if let Some(src) = e.value_from.as_ref() {
                if let Some(fr) = src.field_ref.as_ref() {
                    pod_field_value(pod, &fr.field_path)
                } else if let Some(rfr) = src.resource_field_ref.as_ref() {
                    container_resource_value(
                        container,
                        &rfr.resource,
                        rfr.divisor.as_deref(),
                        node_allocatable,
                    )
                } else if let Some(cmr) = src.config_map_key_ref.as_ref() {
                    config_maps
                        .get(&cmr.name)
                        .and_then(|cm| cm.data.as_ref())
                        .and_then(|d| d.get(&cmr.key).cloned())
                } else if let Some(skr) = src.secret_key_ref.as_ref() {
                    // Secret data is the already-decoded bytes; env values are
                    // the UTF-8 interpretation (upstream casts []byte→string).
                    secrets
                        .get(&skr.name)
                        .and_then(|s| s.data.as_ref())
                        .and_then(|d| d.get(&skr.key))
                        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(v) = value {
                seen.insert(e.name.clone(), v.clone());
                upsert_env(&mut out, e.name.clone(), v);
            }
        }
    }

    out
}

/// Resolve a downward-API pod field path (`fieldRef`) to a string value. Only
/// the fields known at container-create time are returned; `None` otherwise.
fn pod_field_value(pod: &Pod, field_path: &str) -> Option<String> {
    match field_path {
        "metadata.name" => Some(pod.metadata.name.clone()),
        "metadata.namespace" => Some(namespace(pod).to_string()),
        "metadata.uid" => Some(pod.metadata.uid.clone()),
        "spec.nodeName" => pod.spec.as_ref().and_then(|s| s.node_name.clone()),
        "spec.serviceAccountName" => pod
            .spec
            .as_ref()
            .and_then(|s| s.service_account_name.clone()),
        // podIP is the pod's own address; hostIP is the NODE's address. These
        // are distinct — upstream returns `podIP` vs `hostIPs[0]` respectively
        // (pkg/kubelet/kubelet_pods.go podFieldSelectorRuntimeValue). Returning
        // one for the other broke pods that read status.hostIP via fieldRef.
        "status.podIP" => pod.status.as_ref().and_then(|st| st.pod_ip.clone()),
        "status.hostIP" => pod.status.as_ref().and_then(|st| st.host_ip.clone()),
        // Dual-stack plural forms: comma-joined list (matches upstream).
        "status.podIPs" => pod.status.as_ref().and_then(|st| {
            st.pod_i_ps.as_ref().map(|ips| {
                ips.iter()
                    .map(|p| p.ip.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        }),
        "status.hostIPs" => pod.status.as_ref().and_then(|st| {
            st.host_i_ps.as_ref().map(|ips| {
                ips.iter()
                    .map(|h| h.ip.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
        }),
        "spec.restartPolicy" => pod.spec.as_ref().and_then(|s| s.restart_policy.clone()),
        "spec.schedulerName" => pod.spec.as_ref().and_then(|s| s.scheduler_name.clone()),
        "status.phase" => pod
            .status
            .as_ref()
            .and_then(|st| st.phase.as_ref())
            .map(phase_str),
        // Subscript forms `metadata.labels['key']` / `metadata.annotations['key']`.
        _ => field_path
            .strip_prefix("metadata.labels[")
            .map(|k| (pod.metadata.labels.as_ref(), k))
            .or_else(|| {
                field_path
                    .strip_prefix("metadata.annotations[")
                    .map(|k| (pod.metadata.annotations.as_ref(), k))
            })
            .and_then(|(map, rest)| {
                let key = rest.strip_suffix(']')?.trim_matches(['\'', '"']);
                map?.get(key).cloned()
            }),
    }
}

/// Render a [`Phase`] to its Kubernetes string form (`status.phase` fieldRef).
fn phase_str(phase: &rusternetes_common::types::Phase) -> String {
    use rusternetes_common::types::Phase;
    match phase {
        Phase::Pending => "Pending",
        Phase::Running => "Running",
        Phase::Succeeded => "Succeeded",
        Phase::Failed => "Failed",
        Phase::Unknown => "Unknown",
        Phase::Active => "Active",
        Phase::Terminating => "Terminating",
    }
    .to_string()
}

/// Resolve a `resourceFieldRef` (`limits.cpu` / `requests.memory`, etc.) to its
/// numeric value as a decimal string. `None` if the resource is not set.
fn container_resource_value(
    container: &Container,
    resource: &str,
    divisor: Option<&str>,
    node_allocatable: Option<&HashMap<String, String>>,
) -> Option<String> {
    let (kind, name) = resource.split_once('.')?;
    let req = container.resources.as_ref();
    let explicit = |which: &str| -> Option<String> {
        match which {
            "limits" => req.and_then(|r| r.limits.as_ref()),
            "requests" => req.and_then(|r| r.requests.as_ref()),
            _ => None,
        }
        .and_then(|m| m.get(name).cloned())
    };
    // Upstream MergeContainerResourceLimits: an unset cpu/memory/
    // ephemeral-storage/hugepages LIMIT defaults to the node's allocatable;
    // an unset REQUEST defaults to the (possibly defaulted) limit. Other
    // resources have no default — the var is omitted when absent.
    let defaultable =
        matches!(name, "cpu" | "memory" | "ephemeral-storage") || name.starts_with("hugepages-");
    let from_allocatable = || -> Option<String> {
        if defaultable {
            node_allocatable.and_then(|a| a.get(name).cloned())
        } else {
            None
        }
    };
    let raw = match kind {
        "limits" => explicit("limits").or_else(from_allocatable),
        "requests" => explicit("requests")
            .or_else(|| explicit("limits"))
            .or_else(from_allocatable),
        _ => None,
    }?;
    let raw = &raw;
    // `divisor` defaults to "1" (cores for cpu, bytes for memory) — matches
    // upstream `resourcehelper.ExtractContainerResourceValue`. cpu rounds UP
    // (ceil of milli/divisor-milli); byte quantities truncate (floor).
    let div = divisor.unwrap_or("1");
    match name {
        "cpu" => {
            let milli = parse_cpu_millicores(raw)?;
            let div_milli = parse_cpu_millicores(div).filter(|d| *d > 0)?;
            // Ceil division (toolchain predates stable `div_ceil`).
            Some(((milli + div_milli - 1) / div_milli).to_string())
        }
        // memory, ephemeral-storage and hugepages-<size> are all byte quantities
        // upstream normalizes to an integer count of `divisor` bytes (floor).
        "memory" | "ephemeral-storage" => {
            let bytes = parse_memory_bytes(raw)?;
            let div_bytes = parse_memory_bytes(div).filter(|d| *d > 0)?;
            Some((bytes / div_bytes).to_string())
        }
        n if n.starts_with("hugepages-") => {
            let bytes = parse_memory_bytes(raw)?;
            let div_bytes = parse_memory_bytes(div).filter(|d| *d > 0)?;
            Some((bytes / div_bytes).to_string())
        }
        _ => Some(raw.clone()),
    }
}

/// Verify every *non-optional* `configMapKeyRef`/`secretKeyRef` env var in the
/// container resolves against the pre-fetched `config_maps`/`secrets`. Returns
/// the upstream error wording (`makeEnvironmentVariables`) on the first
/// unresolved ref; the caller fails container creation, matching upstream's
/// `CreateContainerConfigError` rather than silently dropping the var. Optional
/// refs are skipped (and may be legitimately absent).
pub fn validate_env_key_refs(
    pod: &Pod,
    container: &Container,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
) -> Result<(), String> {
    let ns = namespace(pod);
    for e in container.env.iter().flatten() {
        let Some(src) = e.value_from.as_ref() else {
            continue;
        };
        if let Some(cmr) = src.config_map_key_ref.as_ref() {
            if cmr.optional.unwrap_or(false) {
                continue;
            }
            match config_maps.get(&cmr.name) {
                None => return Err(format!("couldn't get ConfigMap {ns}/{}", cmr.name)),
                Some(cm) => {
                    if !cm.data.as_ref().is_some_and(|d| d.contains_key(&cmr.key)) {
                        return Err(format!(
                            "couldn't find key {} in ConfigMap {ns}/{}",
                            cmr.key, cmr.name
                        ));
                    }
                }
            }
        }
        if let Some(skr) = src.secret_key_ref.as_ref() {
            if skr.optional.unwrap_or(false) {
                continue;
            }
            match secrets.get(&skr.name) {
                None => return Err(format!("couldn't get Secret {ns}/{}", skr.name)),
                Some(s) => {
                    if !s.data.as_ref().is_some_and(|d| d.contains_key(&skr.key)) {
                        return Err(format!(
                            "couldn't find key {} in Secret {ns}/{}",
                            skr.key, skr.name
                        ));
                    }
                }
            }
        }
    }

    // `envFrom`: a non-optional configMapRef/secretRef that didn't resolve fails
    // container creation too, mirroring upstream `makeEnvironmentVariables`
    // (a missing required bulk source is `CreateContainerConfigError`, not a
    // silent skip). Optional sources are tolerated.
    for ef in container.env_from.iter().flatten() {
        if let Some(cmr) = ef.config_map_ref.as_ref() {
            if !cmr.optional.unwrap_or(false) && !config_maps.contains_key(&cmr.name) {
                return Err(format!("couldn't get ConfigMap {ns}/{}", cmr.name));
            }
        }
        if let Some(skr) = ef.secret_ref.as_ref() {
            if !skr.optional.unwrap_or(false) && !secrets.contains_key(&skr.name) {
                return Err(format!("couldn't get Secret {ns}/{}", skr.name));
            }
        }
    }

    Ok(())
}

/// Translate volume mounts into CRI mounts using a resolved volume-name →
/// host-path map (volume provisioning is runtime-agnostic and done earlier).
/// Mounts whose volume is absent from the map are skipped.
/// Map `volumeMount.mountPropagation` to the CRI propagation mode, porting
/// upstream `translateMountPropagation` (`pkg/kubelet/kubelet_pods.go`): unset,
/// `None`, and any unknown value → `Private` (the default);
/// `HostToContainer`/`Bidirectional` map straight through.
fn translate_mount_propagation(mode: Option<&str>) -> i32 {
    use v1::MountPropagation;
    match mode {
        Some("HostToContainer") => MountPropagation::PropagationHostToContainer as i32,
        Some("Bidirectional") => MountPropagation::PropagationBidirectional as i32,
        _ => MountPropagation::PropagationPrivate as i32,
    }
}

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
                propagation: translate_mount_propagation(vm.mount_propagation.as_deref()),
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
// CFS scheduling constants (upstream `pkg/kubelet/cm/helpers_linux.go`).
const MIN_SHARES: i64 = 2;
const MAX_SHARES: i64 = 262_144;
const SHARES_PER_CPU: i64 = 1024;
const MILLI_CPU_TO_CPU: i64 = 1000;
const QUOTA_PERIOD: i64 = 100_000;
const MIN_QUOTA_PERIOD: i64 = 1000;

/// Port of upstream `cm.MilliCPUToShares`: convert milliCPU to CFS shares,
/// clamped to `[MinShares, MaxShares]`. 0 milliCPU → `MinShares` (the kernel
/// default for "unset").
fn milli_cpu_to_shares(milli_cpu: i64) -> i64 {
    if milli_cpu == 0 {
        return MIN_SHARES;
    }
    ((milli_cpu * SHARES_PER_CPU) / MILLI_CPU_TO_CPU).clamp(MIN_SHARES, MAX_SHARES)
}

/// Port of upstream `cm.MilliCPUToQuota`: convert a milliCPU limit to a CFS
/// quota over `period` microseconds, with a 1ms (`MinQuotaPeriod`) floor.
fn milli_cpu_to_quota(milli_cpu: i64, period: i64) -> i64 {
    if milli_cpu == 0 {
        return 0;
    }
    ((milli_cpu * period) / MILLI_CPU_TO_CPU).max(MIN_QUOTA_PERIOD)
}

/// Port of upstream `calculateLinuxResources`
/// (`pkg/kubelet/kuberuntime/kuberuntime_container_linux.go`).
///
/// `cpu_shares` is **always** set — from the CPU request, else the CPU limit
/// (the api-server defaults request→limit, but controller-created pods written
/// straight to storage may lack it), else `MinShares`. A CPU limit additionally
/// sets the CFS quota/period; a memory limit sets `memory_limit_in_bytes`.
fn linux_resources(container: &Container) -> Option<v1::LinuxContainerResources> {
    let req = container.resources.as_ref();
    let cpu_of = |which: fn(
        &rusternetes_common::types::ResourceRequirements,
    ) -> &Option<HashMap<String, String>>| {
        req.and_then(|r| which(r).as_ref())
            .and_then(|m| m.get("cpu"))
            .and_then(|q| parse_cpu_millicores(q))
    };
    let cpu_request = cpu_of(|r| &r.requests);
    let cpu_limit = cpu_of(|r| &r.limits);
    let mem_limit = req
        .and_then(|r| r.limits.as_ref())
        .and_then(|m| m.get("memory"))
        .and_then(|q| parse_memory_bytes(q));

    let mut r = v1::LinuxContainerResources {
        // request, else limit, else 0 → MinShares.
        cpu_shares: milli_cpu_to_shares(cpu_request.or(cpu_limit).unwrap_or(0)),
        ..Default::default()
    };
    if let Some(limit) = cpu_limit {
        r.cpu_period = QUOTA_PERIOD;
        r.cpu_quota = milli_cpu_to_quota(limit, QUOTA_PERIOD);
    }
    if let Some(mem) = mem_limit {
        r.memory_limit_in_bytes = mem;
    }
    Some(r)
}

/// Resolve the effective container seccomp profile to a CRI [`SecurityProfile`].
///
/// Ports upstream `getSeccompProfile`/`fieldSeccompProfile`
/// (`pkg/kubelet/kuberuntime/helpers.go`): the container's
/// `securityContext.seccompProfile` wins, falling back to the pod's. `None` when
/// neither is set (the runtime then applies its own default; upstream's
/// `fallbackToRuntimeDefault` is off by default, so we don't force RuntimeDefault).
///
/// Note: for `Localhost`, upstream sets `localhost_ref` to
/// `join(seccompProfileRoot, localhostProfile)`. We pass the `localhostProfile`
/// through as-is (the translate layer is path-pure and has no kubelet root);
/// RuntimeDefault/Unconfined — the common cases — need no path.
fn seccomp_security_profile(pod: &Pod, container: &Container) -> Option<v1::SecurityProfile> {
    use rusternetes_common::resources::pod::SeccompProfile;
    let scmp: &SeccompProfile = container
        .security_context
        .as_ref()
        .and_then(|c| c.seccomp_profile.as_ref())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|s| s.security_context.as_ref())
                .and_then(|p| p.seccomp_profile.as_ref())
        })?;
    use v1::security_profile::ProfileType;
    let profile = match scmp.r#type.as_str() {
        "RuntimeDefault" => v1::SecurityProfile {
            profile_type: ProfileType::RuntimeDefault as i32,
            localhost_ref: String::new(),
        },
        "Localhost" => v1::SecurityProfile {
            profile_type: ProfileType::Localhost as i32,
            localhost_ref: scmp.localhost_profile.clone().unwrap_or_default(),
        },
        // "Unconfined" and any unknown value → Unconfined (upstream default arm).
        _ => v1::SecurityProfile {
            profile_type: ProfileType::Unconfined as i32,
            localhost_ref: String::new(),
        },
    };
    Some(profile)
}

/// Resolve the effective AppArmor profile to a CRI [`SecurityProfile`].
///
/// Ports upstream `getAppArmorProfile`/`apparmor.GetProfile`
/// (`pkg/kubelet/kuberuntime/helpers.go`): container `appArmorProfile` wins over
/// the pod's; `RuntimeDefault`/`Unconfined`/`Localhost` map to the CRI profile
/// (Localhost carries `localhost_ref`). We set only the modern `apparmor`
/// `SecurityProfile`, not the deprecated `apparmor_profile` string (that exists
/// solely for runtimes older than the CRI-v1 containerd we target).
fn apparmor_security_profile(pod: &Pod, container: &Container) -> Option<v1::SecurityProfile> {
    use rusternetes_common::resources::pod::AppArmorProfile;
    let profile: &AppArmorProfile = container
        .security_context
        .as_ref()
        .and_then(|c| c.app_armor_profile.as_ref())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|s| s.security_context.as_ref())
                .and_then(|p| p.app_armor_profile.as_ref())
        })?;
    use v1::security_profile::ProfileType;
    let profile = match profile.type_.as_str() {
        "RuntimeDefault" => v1::SecurityProfile {
            profile_type: ProfileType::RuntimeDefault as i32,
            localhost_ref: String::new(),
        },
        "Localhost" => v1::SecurityProfile {
            profile_type: ProfileType::Localhost as i32,
            localhost_ref: profile.localhost_profile.clone().unwrap_or_default(),
        },
        // "Unconfined" and any unknown value → Unconfined.
        _ => v1::SecurityProfile {
            profile_type: ProfileType::Unconfined as i32,
            localhost_ref: String::new(),
        },
    };
    Some(profile)
}

/// Effective SELinux options for the container (container `seLinuxOptions` wins
/// over the pod's), mapped to the CRI [`SeLinuxOption`] (upstream
/// `convertToRuntimeSELinuxOption`).
fn selinux_options(pod: &Pod, container: &Container) -> Option<v1::SeLinuxOption> {
    let opts = container
        .security_context
        .as_ref()
        .and_then(|c| c.se_linux_options.as_ref())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|s| s.security_context.as_ref())
                .and_then(|p| p.se_linux_options.as_ref())
        })?;
    Some(v1::SeLinuxOption {
        user: opts.user.clone().unwrap_or_default(),
        role: opts.role.clone().unwrap_or_default(),
        r#type: opts.type_.clone().unwrap_or_default(),
        level: opts.level.clone().unwrap_or_default(),
    })
}

/// Upstream `securitycontext.defaultMaskedPaths` (pkg/securitycontext/util.go).
/// The per-CPU `/sys/devices/system/cpu/cpuN/thermal_throttle` paths upstream
/// appends after a host `os.Stat` are omitted here — that host-FS scan is
/// runtime-side work; this static set is the contract-significant base.
const DEFAULT_MASKED_PATHS: &[&str] = &[
    "/proc/asound",
    "/proc/acpi",
    "/proc/interrupts",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/timer_list",
    "/proc/timer_stats",
    "/proc/sched_debug",
    "/proc/scsi",
    "/sys/firmware",
    "/sys/devices/virtual/powercap",
];

/// Upstream `securitycontext.defaultReadonlyPaths`.
const DEFAULT_READONLY_PATHS: &[&str] = &[
    "/proc/bus",
    "/proc/fs",
    "/proc/irq",
    "/proc/sys",
    "/proc/sysrq-trigger",
];

/// Port of upstream `securitycontext.ConvertToRuntimeMaskedPaths`: the default
/// masked paths unless `procMount: Unmasked`, in which case nothing is masked.
///
/// The kubelet must send these explicitly: containerd's CRI resets masked paths
/// to empty and only re-applies what the security context carries, so an unset
/// field leaves `/proc` unmasked.
fn convert_masked_paths(proc_mount: Option<&str>) -> Vec<String> {
    if proc_mount == Some("Unmasked") {
        return Vec::new();
    }
    DEFAULT_MASKED_PATHS.iter().map(|s| s.to_string()).collect()
}

/// Port of upstream `securitycontext.ConvertToRuntimeReadonlyPaths`.
fn convert_readonly_paths(proc_mount: Option<&str>) -> Vec<String> {
    if proc_mount == Some("Unmasked") {
        return Vec::new();
    }
    DEFAULT_READONLY_PATHS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

fn linux_security_context(
    pod: &Pod,
    container: &Container,
) -> Option<v1::LinuxContainerSecurityContext> {
    // seccomp/apparmor/selinux may come from the pod even when the container has
    // no security context of its own.
    let seccomp = seccomp_security_profile(pod, container);
    let apparmor = apparmor_security_profile(pod, container);
    let selinux_options = selinux_options(pod, container);
    let sc = container.security_context.as_ref();
    // Always build a security context: the masked/readonly proc paths must be
    // sent for every container (upstream sets them unconditionally), so there is
    // no early-out.
    // Translate requested Linux capabilities. Names are passed through verbatim
    // (e.g. "NET_ADMIN"), matching upstream — the kubelet does not add the
    // "CAP_" prefix; the runtime (containerd) does when building the OCI spec.
    // Without this, NET_ADMIN/NET_RAW never reach the container and capability-
    // dependent workloads fail, e.g. flannel's vxlan link creation returns
    // netlink EPERM ("Operation not permitted") and never writes subnet.env.
    let capabilities = sc
        .and_then(|sc| sc.capabilities.as_ref())
        .map(|caps| v1::Capability {
            add_capabilities: caps.add.clone().unwrap_or_default(),
            drop_capabilities: caps.drop.clone().unwrap_or_default(),
            add_ambient_capabilities: Vec::new(),
        });
    Some(v1::LinuxContainerSecurityContext {
        privileged: sc.and_then(|s| s.privileged).unwrap_or(false),
        capabilities,
        run_as_user: sc
            .and_then(|s| s.run_as_user)
            .map(|v| v1::Int64Value { value: v }),
        run_as_group: sc
            .and_then(|s| s.run_as_group)
            .map(|v| v1::Int64Value { value: v }),
        readonly_rootfs: sc
            .and_then(|s| s.read_only_root_filesystem)
            .unwrap_or(false),
        // `allowPrivilegeEscalation: false` → `no_new_privs`, blocking setuid/
        // file-capability escalation in the container. Upstream
        // `securitycontext.AddNoNewPrivileges`: true *only* when the field is
        // explicitly false (unset/true → false). Without this a pod that set
        // `allowPrivilegeEscalation: false` could still escalate.
        no_new_privs: sc.and_then(|s| s.allow_privilege_escalation) == Some(false),
        seccomp,
        apparmor,
        selinux_options,
        // procMount → masked/readonly proc paths (upstream ConvertToRuntime*Paths).
        masked_paths: convert_masked_paths(sc.and_then(|s| s.proc_mount.as_deref())),
        readonly_paths: convert_readonly_paths(sc.and_then(|s| s.proc_mount.as_deref())),
        ..Default::default()
    })
}

/// Translate a single container into a CRI [`ContainerConfig`](v1::ContainerConfig).
///
/// `image_ref` is the canonical reference returned by `PullImage`. `host_paths`
/// maps volume names to their resolved host paths for mount translation.
/// `config_maps`/`secrets` are the ConfigMap/Secret objects (by name) the
/// kubelet pre-fetched from storage so `configMapKeyRef`/`secretKeyRef` env can
/// be resolved here without storage access.
pub fn container_config(
    pod: &Pod,
    container: &Container,
    image_ref: &str,
    host_paths: &HashMap<String, String>,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
) -> v1::ContainerConfig {
    container_config_with_allocatable(
        pod,
        container,
        image_ref,
        host_paths,
        config_maps,
        secrets,
        None,
    )
}

/// As [`container_config`], but `node_allocatable` lets unset cpu/memory/
/// ephemeral-storage resourceFieldRef LIMITS default to the node's allocatable
/// (upstream `MergeContainerResourceLimits`). The kubelet passes its node
/// allocatable here; the no-allocatable wrapper above keeps `None`.
#[allow(clippy::too_many_arguments)]
pub fn container_config_with_allocatable(
    pod: &Pod,
    container: &Container,
    image_ref: &str,
    host_paths: &HashMap<String, String>,
    config_maps: &HashMap<String, ConfigMap>,
    secrets: &HashMap<String, Secret>,
    node_allocatable: Option<&HashMap<String, String>>,
) -> v1::ContainerConfig {
    let mut labels = pod_labels(pod);
    labels.insert(labels::CONTAINER_NAME.to_string(), container.name.clone());

    let linux = {
        let resources = linux_resources(container);
        let security_context = linux_security_context(pod, container);
        if resources.is_some() || security_context.is_some() {
            Some(v1::LinuxContainerConfig {
                resources,
                security_context,
            })
        } else {
            None
        }
    };

    let envs = env_vars(pod, container, config_maps, secrets, node_allocatable);
    // command/args expand `$(VAR)` against the full container env (upstream
    // `expandContainerCommandAndArgs`).
    let env_map: HashMap<String, String> = envs
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();

    v1::ContainerConfig {
        metadata: Some(v1::ContainerMetadata {
            name: container.name.clone(),
            attempt: 0,
        }),
        image: Some(v1::ImageSpec {
            image: image_ref.to_string(),
            ..Default::default()
        }),
        command: expand_all(container.command.clone().unwrap_or_default(), &env_map),
        args: expand_all(container.args.clone().unwrap_or_default(), &env_map),
        working_dir: container.working_dir.clone().unwrap_or_default(),
        envs,
        mounts: mounts(container, host_paths),
        labels,
        log_path: format!("{}.log", container.name),
        linux,
        ..Default::default()
    }
}

/// Build the Docker-link-style service env a container sees, per upstream
/// kubelet `getServiceEnvVarMap` + `makeLinkVariables`. Each service with a
/// usable ClusterIP contributes vars keyed by the uppercased ('-'→'_') service
/// name: `{N}_SERVICE_HOST`, `{N}_SERVICE_PORT`, optional named
/// `{N}_SERVICE_PORT_{PORTNAME}`, and for each declared port the docker-link
/// quartet `{N}_PORT_{port}_{PROTO}[ |_PROTO|_PORT|_ADDR]` plus a single
/// `{N}_PORT` from the first port. Headless ("None") and IP-less services are
/// skipped. Services are processed in name order for deterministic output.
pub(crate) fn service_link_env_vars(services: &[Service]) -> Vec<(String, String)> {
    let mut sorted: Vec<&Service> = services.iter().collect();
    sorted.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    let mut out: Vec<(String, String)> = Vec::new();
    for svc in sorted {
        let Some(ip) = svc.spec.cluster_ip.as_deref() else {
            continue;
        };
        if ip.is_empty() || ip == "None" {
            continue;
        }
        let Some(first) = svc.spec.ports.first() else {
            continue;
        };
        let n = svc.metadata.name.to_uppercase().replace('-', "_");
        out.push((format!("{n}_SERVICE_HOST"), ip.to_string()));
        out.push((format!("{n}_SERVICE_PORT"), first.port.to_string()));
        for p in &svc.spec.ports {
            if let Some(name) = p.name.as_deref().filter(|s| !s.is_empty()) {
                let pn = name.to_uppercase().replace('-', "_");
                out.push((format!("{n}_SERVICE_PORT_{pn}"), p.port.to_string()));
            }
        }
        let proto0 = first.protocol.to_lowercase();
        out.push((
            format!("{n}_PORT"),
            format!("{proto0}://{ip}:{}", first.port),
        ));
        for p in &svc.spec.ports {
            let proto = p.protocol.to_lowercase();
            let prefix = format!("{n}_PORT_{}_{}", p.port, p.protocol.to_uppercase());
            out.push((prefix.clone(), format!("{proto}://{ip}:{}", p.port)));
            out.push((format!("{prefix}_PROTO"), proto.clone()));
            out.push((format!("{prefix}_PORT"), p.port.to_string()));
            out.push((format!("{prefix}_ADDR"), ip.to_string()));
        }
    }
    out
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

    fn svc(name: &str, cluster_ip: Option<&str>, port: u16, proto: &str) -> Service {
        use rusternetes_common::resources::service::{ServicePort, ServiceSpec};
        let mut s = Service {
            type_meta: Default::default(),
            metadata: Default::default(),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port,
                    target_port: None,
                    protocol: proto.to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: cluster_ip.map(|s| s.to_string()),
                ..Default::default()
            },
            status: None,
        };
        s.metadata.name = name.to_string();
        s
    }

    #[test]
    fn service_links_emit_docker_link_vars_and_skip_headless() {
        // Upstream getServiceEnvVarMap/makeLinkVariables: each service with a
        // real ClusterIP yields the docker-link env quartet, keyed by the
        // uppercased, '-'→'_' service name. Headless ("None") and IP-less
        // services contribute nothing.
        let services = vec![
            svc("redis-master", Some("10.0.0.11"), 6379, "TCP"),
            svc("headless", Some("None"), 80, "TCP"),
            svc("pending", None, 80, "TCP"),
        ];
        let env = service_link_env_vars(&services);
        let get = |k: &str| env.iter().find(|(kk, _)| kk == k).map(|(_, v)| v.as_str());
        assert_eq!(get("REDIS_MASTER_SERVICE_HOST"), Some("10.0.0.11"));
        assert_eq!(get("REDIS_MASTER_SERVICE_PORT"), Some("6379"));
        assert_eq!(get("REDIS_MASTER_PORT"), Some("tcp://10.0.0.11:6379"));
        assert_eq!(
            get("REDIS_MASTER_PORT_6379_TCP"),
            Some("tcp://10.0.0.11:6379")
        );
        assert_eq!(get("REDIS_MASTER_PORT_6379_TCP_ADDR"), Some("10.0.0.11"));
        assert_eq!(get("REDIS_MASTER_PORT_6379_TCP_PROTO"), Some("tcp"));
        assert_eq!(get("REDIS_MASTER_PORT_6379_TCP_PORT"), Some("6379"));
        assert!(
            env.iter()
                .all(|(k, _)| !k.starts_with("HEADLESS_") && !k.starts_with("PENDING_")),
            "headless / IP-less services must contribute no env"
        );
    }

    #[test]
    fn downward_api_pod_ip_and_host_ip_are_distinct() {
        use rusternetes_common::resources::pod::{HostIP, PodIP, PodStatus};
        let mut pod = pod_with(PodSpec {
            node_name: Some("node-1".to_string()),
            ..Default::default()
        });
        pod.status = Some(PodStatus {
            pod_ip: Some("10.244.0.7".to_string()),
            host_ip: Some("172.20.0.5".to_string()),
            pod_i_ps: Some(vec![PodIP {
                ip: "10.244.0.7".to_string(),
            }]),
            host_i_ps: Some(vec![HostIP {
                ip: "172.20.0.5".to_string(),
            }]),
            ..Default::default()
        });
        // podIP is the pod's address; hostIP is the node's — never conflated.
        assert_eq!(
            pod_field_value(&pod, "status.podIP").as_deref(),
            Some("10.244.0.7")
        );
        assert_eq!(
            pod_field_value(&pod, "status.hostIP").as_deref(),
            Some("172.20.0.5")
        );
        assert_eq!(
            pod_field_value(&pod, "status.podIPs").as_deref(),
            Some("10.244.0.7")
        );
        assert_eq!(
            pod_field_value(&pod, "status.hostIPs").as_deref(),
            Some("172.20.0.5")
        );
        assert_eq!(
            pod_field_value(&pod, "spec.nodeName").as_deref(),
            Some("node-1")
        );
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
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(cfg.command, vec!["/bin/sh"]);
        assert_eq!(cfg.args, vec!["-c", "sleep 1"]);
        assert_eq!(cfg.envs[0].key, "FOO");
        assert_eq!(cfg.envs[0].value, "bar");
        let res = cfg.linux.unwrap().resources.unwrap();
        assert_eq!(res.cpu_quota, 50_000); // 500m -> quota 50000 @ 100ms period
                                           // No CPU request, so shares fall back to the limit: 500m -> 512 shares.
        assert_eq!(res.cpu_shares, 512);
        assert_eq!(res.memory_limit_in_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.labels.get(labels::CONTAINER_NAME).unwrap(), "app");
    }

    #[test]
    fn linux_resources_shares_quota_and_floors() {
        use rusternetes_common::types::ResourceRequirements;
        let res = |limits: Option<Vec<(&str, &str)>>, requests: Option<Vec<(&str, &str)>>| {
            let mut c = Container {
                name: "app".to_string(),
                image: "busybox".to_string(),
                ..Default::default()
            };
            let map = |v: Vec<(&str, &str)>| {
                v.into_iter()
                    .map(|(k, val)| (k.to_string(), val.to_string()))
                    .collect::<HashMap<_, _>>()
            };
            c.resources = Some(ResourceRequirements {
                limits: limits.map(map),
                requests: requests.map(map),
                claims: None,
            });
            linux_resources(&c).unwrap()
        };

        // CPU request drives shares; limit drives quota/period.
        let r = res(Some(vec![("cpu", "1")]), Some(vec![("cpu", "250m")]));
        assert_eq!(r.cpu_shares, 256); // 250m
        assert_eq!(r.cpu_quota, 100_000); // 1 CPU @ 100ms
        assert_eq!(r.cpu_period, 100_000);

        // No resources at all → MinShares, no quota.
        let r = res(None, None);
        assert_eq!(r.cpu_shares, MIN_SHARES);
        assert_eq!(r.cpu_quota, 0);

        // Tiny CPU limit floors the quota at MinQuotaPeriod (1ms).
        let r = res(Some(vec![("cpu", "5m")]), None);
        assert_eq!(r.cpu_quota, MIN_QUOTA_PERIOD); // 5m -> 500, floored to 1000
        assert_eq!(r.cpu_shares, 5); // 5m -> 5 shares (already >= MinShares)

        // 1m floors shares at MinShares (1m -> 1 share -> 2).
        let r = res(Some(vec![("cpu", "1m")]), None);
        assert_eq!(r.cpu_shares, MIN_SHARES);
    }

    #[test]
    fn volume_mounts_resolve_from_host_paths() {
        use rusternetes_common::resources::pod::VolumeMount;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.volume_mounts = Some(vec![
            VolumeMount {
                name: "data".to_string(),
                mount_path: "/data".to_string(),
                read_only: Some(true),
                sub_path: None,
                sub_path_expr: None,
                mount_propagation: None,
                recursive_read_only: None,
            },
            VolumeMount {
                name: "missing".to_string(),
                mount_path: "/nope".to_string(),
                read_only: None,
                sub_path: None,
                sub_path_expr: None,
                mount_propagation: None,
                recursive_read_only: None,
            },
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let host_paths = HashMap::from([("data".to_string(), "/host/data".to_string())]);
        let cfg = container_config(
            &pod,
            &c,
            "img",
            &host_paths,
            &HashMap::new(),
            &HashMap::new(),
        );
        // Only the resolvable mount is emitted; the unmapped one is skipped.
        assert_eq!(cfg.mounts.len(), 1);
        assert_eq!(cfg.mounts[0].container_path, "/data");
        assert_eq!(cfg.mounts[0].host_path, "/host/data");
        assert!(cfg.mounts[0].readonly);
        // Unset propagation defaults to Private.
        assert_eq!(
            cfg.mounts[0].propagation,
            v1::MountPropagation::PropagationPrivate as i32
        );
    }

    #[test]
    fn mount_propagation_modes() {
        use rusternetes_common::resources::pod::VolumeMount;
        use v1::MountPropagation;
        let mk = |mode: Option<&str>| VolumeMount {
            name: "v".to_string(),
            mount_path: "/v".to_string(),
            read_only: None,
            sub_path: None,
            sub_path_expr: None,
            mount_propagation: mode.map(str::to_string),
            recursive_read_only: None,
        };
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        let host_paths = HashMap::from([("v".to_string(), "/host/v".to_string())]);
        let mut prop = |mode: Option<&str>| -> i32 {
            c.volume_mounts = Some(vec![mk(mode)]);
            mounts(&c, &host_paths)[0].propagation
        };
        assert_eq!(
            prop(Some("HostToContainer")),
            MountPropagation::PropagationHostToContainer as i32
        );
        assert_eq!(
            prop(Some("Bidirectional")),
            MountPropagation::PropagationBidirectional as i32
        );
        assert_eq!(
            prop(Some("None")),
            MountPropagation::PropagationPrivate as i32
        );
        assert_eq!(prop(None), MountPropagation::PropagationPrivate as i32);
    }

    #[test]
    fn ports_map_with_protocol() {
        use rusternetes_common::resources::pod::ContainerPort;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.ports = Some(vec![
            ContainerPort {
                container_port: 53,
                name: Some("dns".to_string()),
                protocol: "UDP".to_string(),
                host_port: Some(5353),
                host_ip: None,
            },
            ContainerPort {
                container_port: 80,
                name: None,
                protocol: "TCP".to_string(), // defaults to TCP
                host_port: None,
                host_ip: None,
            },
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c],
            ..Default::default()
        });
        let cfg = sandbox_config(&pod, "/log");
        assert_eq!(cfg.port_mappings.len(), 2);
        assert_eq!(cfg.port_mappings[0].protocol, v1::Protocol::Udp as i32);
        assert_eq!(cfg.port_mappings[0].container_port, 53);
        assert_eq!(cfg.port_mappings[0].host_port, 5353);
        assert_eq!(cfg.port_mappings[1].protocol, v1::Protocol::Tcp as i32);
    }

    #[test]
    fn host_pid_and_ipc_map_to_node_namespace() {
        let pod = pod_with(PodSpec {
            host_pid: Some(true),
            host_ipc: Some(true),
            host_network: Some(false),
            ..Default::default()
        });
        let ns = sandbox_config(&pod, "/log")
            .linux
            .unwrap()
            .security_context
            .unwrap()
            .namespace_options
            .unwrap();
        assert_eq!(ns.pid, v1::NamespaceMode::Node as i32);
        assert_eq!(ns.ipc, v1::NamespaceMode::Node as i32);
        assert_eq!(ns.network, v1::NamespaceMode::Pod as i32);
    }

    #[test]
    fn pod_sysctls_map_onto_sandbox_linux_config() {
        use rusternetes_common::resources::pod::{PodSecurityContext, Sysctl};
        let pod = pod_with(PodSpec {
            security_context: Some(PodSecurityContext {
                sysctls: Some(vec![
                    Sysctl {
                        name: "net.core.somaxconn".to_string(),
                        value: "1024".to_string(),
                    },
                    Sysctl {
                        name: "kernel.shm_rmid_forced".to_string(),
                        value: "1".to_string(),
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let sysctls = sandbox_config(&pod, "/log").linux.unwrap().sysctls;
        assert_eq!(sysctls.len(), 2);
        assert_eq!(
            sysctls.get("net.core.somaxconn").map(String::as_str),
            Some("1024")
        );
        assert_eq!(
            sysctls.get("kernel.shm_rmid_forced").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn no_pod_sysctls_yields_empty_sandbox_map() {
        let pod = pod_with(PodSpec::default());
        assert!(sandbox_config(&pod, "/log")
            .linux
            .unwrap()
            .sysctls
            .is_empty());
    }

    #[test]
    fn dns_cluster_first_is_default_with_cluster_searches_and_ndots() {
        // No dnsPolicy => ClusterFirst (api-server default).
        let pod = pod_with(PodSpec::default());
        let dns = dns_config(&pod, &["10.96.0.10".to_string()], "cluster.local")
            .expect("ClusterFirst yields a DnsConfig");
        assert_eq!(dns.servers, vec!["10.96.0.10".to_string()]);
        assert_eq!(
            dns.searches,
            vec![
                "prod.svc.cluster.local".to_string(), // pod_with sets namespace=prod
                "svc.cluster.local".to_string(),
                "cluster.local".to_string(),
            ]
        );
        assert_eq!(dns.options, vec!["ndots:5".to_string()]);
    }

    #[test]
    fn dns_default_policy_inherits_host() {
        let pod = pod_with(PodSpec {
            dns_policy: Some("Default".to_string()),
            ..Default::default()
        });
        // None => leave dns_config unset so the runtime copies host resolv.conf.
        assert!(dns_config(&pod, &["10.96.0.10".to_string()], "cluster.local").is_none());
    }

    #[test]
    fn dns_cluster_first_without_cluster_dns_falls_back_to_host() {
        let pod = pod_with(PodSpec::default());
        assert!(dns_config(&pod, &[], "cluster.local").is_none());
    }

    #[test]
    fn dns_cluster_first_on_host_network_falls_back_to_host() {
        // hostNetwork pod with default (ClusterFirst) dnsPolicy: upstream
        // getPodDNSType falls back to Default (host resolv.conf), so we must
        // NOT stamp clusterDNS — else the pod can't resolve the `api-server`
        // alias before cluster DNS is up. Regression test for the in-cluster
        // control-plane static pods (scheduler / controller-manager).
        let pod = pod_with(PodSpec {
            host_network: Some(true),
            ..Default::default()
        });
        assert!(dns_config(&pod, &["10.96.0.10".to_string()], "cluster.local").is_none());
    }

    #[test]
    fn dns_cluster_first_with_host_net_keeps_cluster_dns_on_host_network() {
        // ClusterFirstWithHostNet explicitly opts a hostNetwork pod back into
        // clusterDNS (upstream getPodDNSType returns podDNSCluster).
        let pod = pod_with(PodSpec {
            host_network: Some(true),
            dns_policy: Some("ClusterFirstWithHostNet".to_string()),
            ..Default::default()
        });
        let dns = dns_config(&pod, &["10.96.0.10".to_string()], "cluster.local")
            .expect("ClusterFirstWithHostNet yields a DnsConfig on hostNetwork");
        assert_eq!(dns.servers, vec!["10.96.0.10".to_string()]);
    }

    #[test]
    fn dns_none_uses_only_pod_dns_config() {
        use rusternetes_common::resources::pod::{PodDNSConfig, PodDNSConfigOption};
        let pod = pod_with(PodSpec {
            dns_policy: Some("None".to_string()),
            dns_config: Some(PodDNSConfig {
                nameservers: Some(vec!["1.2.3.4".to_string()]),
                searches: Some(vec!["custom.example".to_string()]),
                options: Some(vec![PodDNSConfigOption {
                    name: "ndots".to_string(),
                    value: Some("2".to_string()),
                }]),
            }),
            ..Default::default()
        });
        let dns = dns_config(&pod, &["10.96.0.10".to_string()], "cluster.local").unwrap();
        // No cluster defaults leak in for policy None.
        assert_eq!(dns.servers, vec!["1.2.3.4".to_string()]);
        assert_eq!(dns.searches, vec!["custom.example".to_string()]);
        assert_eq!(dns.options, vec!["ndots:2".to_string()]);
    }

    #[test]
    fn dns_cluster_first_merges_pod_dns_config_and_overrides_ndots() {
        use rusternetes_common::resources::pod::{PodDNSConfig, PodDNSConfigOption};
        let pod = pod_with(PodSpec {
            dns_config: Some(PodDNSConfig {
                nameservers: Some(vec!["1.1.1.1".to_string()]),
                searches: Some(vec!["extra.example".to_string()]),
                options: Some(vec![
                    PodDNSConfigOption {
                        name: "ndots".to_string(),
                        value: Some("3".to_string()),
                    },
                    PodDNSConfigOption {
                        name: "edns0".to_string(),
                        value: None,
                    },
                ]),
            }),
            ..Default::default()
        });
        let dns = dns_config(&pod, &["10.96.0.10".to_string()], "cluster.local").unwrap();
        // cluster server + appended pod nameserver.
        assert_eq!(
            dns.servers,
            vec!["10.96.0.10".to_string(), "1.1.1.1".to_string()]
        );
        // cluster searches + appended pod search.
        assert!(dns.searches.contains(&"prod.svc.cluster.local".to_string()));
        assert!(dns.searches.contains(&"extra.example".to_string()));
        // ndots overridden in place; edns0 appended (no duplicate ndots).
        assert_eq!(
            dns.options,
            vec!["ndots:3".to_string(), "edns0".to_string()]
        );
    }

    #[test]
    fn security_context_maps_privileged_and_user() {
        use rusternetes_common::resources::pod::SecurityContext;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.security_context = Some(SecurityContext {
            privileged: Some(true),
            run_as_user: Some(1000),
            run_as_group: Some(2000),
            run_as_non_root: None,
            read_only_root_filesystem: Some(true),
            allow_privilege_escalation: None,
            proc_mount: None,
            capabilities: None,
            seccomp_profile: None,
            se_linux_options: None,
            app_armor_profile: None,
            windows_options: None,
        });
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let sc = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .linux
        .unwrap()
        .security_context
        .unwrap();
        assert!(sc.privileged);
        assert!(sc.readonly_rootfs);
        assert_eq!(sc.run_as_user.unwrap().value, 1000);
        assert_eq!(sc.run_as_group.unwrap().value, 2000);
        // allowPrivilegeEscalation unset -> no_new_privs stays false.
        assert!(!sc.no_new_privs);
    }

    #[test]
    fn no_new_privs_only_when_allow_priv_esc_false() {
        use serde_json::json;
        let no_new_privs = |sec_ctx: serde_json::Value| -> bool {
            let c: Container = serde_json::from_value(json!({
                "name": "app",
                "image": "busybox",
                "securityContext": sec_ctx
            }))
            .unwrap();
            let pod = pod_with(PodSpec {
                containers: vec![c.clone()],
                ..Default::default()
            });
            container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .linux
            .unwrap()
            .security_context
            .unwrap()
            .no_new_privs
        };
        // Upstream AddNoNewPrivileges: true only when explicitly false.
        assert!(
            no_new_privs(json!({"allowPrivilegeEscalation": false})),
            "explicit false must set no_new_privs"
        );
        assert!(
            !no_new_privs(json!({"allowPrivilegeEscalation": true})),
            "explicit true must not set no_new_privs"
        );
        assert!(!no_new_privs(json!({})), "unset must not set no_new_privs");
    }

    #[test]
    fn seccomp_profile_container_pod_fallback_and_types() {
        use serde_json::json;
        use v1::security_profile::ProfileType;

        // Returns (profile_type, localhost_ref) of the container's seccomp, or
        // None if unset, from a pod spec JSON.
        let seccomp = |spec: serde_json::Value| -> Option<(i32, String)> {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default"},
                "spec": spec
            }))
            .unwrap();
            let c = pod.spec.as_ref().unwrap().containers[0].clone();
            container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .linux
            .and_then(|l| l.security_context)
            .and_then(|s| s.seccomp)
            .map(|sp| (sp.profile_type, sp.localhost_ref))
        };

        // Container RuntimeDefault.
        assert_eq!(
            seccomp(json!({"containers": [{
                "name": "app", "image": "busybox",
                "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}}
            }]})),
            Some((ProfileType::RuntimeDefault as i32, String::new()))
        );

        // Container Localhost carries the profile ref.
        assert_eq!(
            seccomp(json!({"containers": [{
                "name": "app", "image": "busybox",
                "securityContext": {"seccompProfile": {"type": "Localhost", "localhostProfile": "profiles/audit.json"}}
            }]})),
            Some((
                ProfileType::Localhost as i32,
                "profiles/audit.json".to_string()
            ))
        );

        // Pod-level profile applies when the container has none.
        assert_eq!(
            seccomp(json!({
                "securityContext": {"seccompProfile": {"type": "Unconfined"}},
                "containers": [{"name": "app", "image": "busybox"}]
            })),
            Some((ProfileType::Unconfined as i32, String::new()))
        );

        // Container profile overrides the pod's.
        assert_eq!(
            seccomp(json!({
                "securityContext": {"seccompProfile": {"type": "Unconfined"}},
                "containers": [{
                    "name": "app", "image": "busybox",
                    "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}}
                }]
            })),
            Some((ProfileType::RuntimeDefault as i32, String::new()))
        );

        // No seccomp anywhere → unset.
        assert_eq!(
            seccomp(json!({"containers": [{"name": "app", "image": "busybox"}]})),
            None
        );
    }

    #[test]
    fn sandbox_security_context_carries_pod_fields() {
        use serde_json::json;
        use v1::security_profile::ProfileType;
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {
                "securityContext": {
                    "runAsUser": 1000,
                    "runAsGroup": 3000,
                    "fsGroup": 2000,
                    "supplementalGroups": [5, 6],
                    "supplementalGroupsPolicy": "Strict",
                    "seLinuxOptions": {"level": "s0:c1,c2"}
                },
                "containers": [{
                    "name": "app", "image": "busybox",
                    "securityContext": {"privileged": true}
                }]
            }
        }))
        .unwrap();
        let sc = sandbox_config(&pod, "/log")
            .linux
            .unwrap()
            .security_context
            .unwrap();
        assert!(sc.privileged, "sandbox privileged when a container is");
        assert_eq!(sc.run_as_user.unwrap().value, 1000);
        assert_eq!(sc.run_as_group.unwrap().value, 3000);
        // fsGroup first, then supplementalGroups.
        assert_eq!(sc.supplemental_groups, vec![2000, 5, 6]);
        assert_eq!(
            sc.supplemental_groups_policy,
            v1::SupplementalGroupsPolicy::Strict as i32
        );
        assert_eq!(sc.selinux_options.unwrap().level, "s0:c1,c2");
        // Sandbox seccomp is always forced to RuntimeDefault (#84623).
        assert_eq!(
            sc.seccomp.unwrap().profile_type,
            ProfileType::RuntimeDefault as i32
        );
    }

    #[test]
    fn apparmor_profile_container_pod_fallback_and_types() {
        use serde_json::json;
        use v1::security_profile::ProfileType;
        let apparmor = |spec: serde_json::Value| -> Option<(i32, String)> {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default"}, "spec": spec
            }))
            .unwrap();
            let c = pod.spec.as_ref().unwrap().containers[0].clone();
            container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .linux
            .and_then(|l| l.security_context)
            .and_then(|s| s.apparmor)
            .map(|p| (p.profile_type, p.localhost_ref))
        };
        assert_eq!(
            apparmor(json!({"containers": [{
                "name": "app", "image": "busybox",
                "securityContext": {"appArmorProfile": {"type": "Localhost", "localhostProfile": "k8s-audit"}}
            }]})),
            Some((ProfileType::Localhost as i32, "k8s-audit".to_string()))
        );
        // Pod-level applies when the container has none.
        assert_eq!(
            apparmor(json!({
                "securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}},
                "containers": [{"name": "app", "image": "busybox"}]
            })),
            Some((ProfileType::RuntimeDefault as i32, String::new()))
        );
        assert_eq!(
            apparmor(json!({"containers": [{"name": "app", "image": "busybox"}]})),
            None
        );
    }

    #[test]
    fn selinux_options_mapped_with_pod_fallback() {
        use serde_json::json;
        let selinux = |spec: serde_json::Value| -> Option<(String, String, String, String)> {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default"}, "spec": spec
            }))
            .unwrap();
            let c = pod.spec.as_ref().unwrap().containers[0].clone();
            container_config(
                &pod,
                &c,
                "img",
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .linux
            .and_then(|l| l.security_context)
            .and_then(|s| s.selinux_options)
            .map(|o| (o.user, o.role, o.r#type, o.level))
        };
        // Container options win.
        assert_eq!(
            selinux(json!({"containers": [{
                "name": "app", "image": "busybox",
                "securityContext": {"seLinuxOptions": {"level": "s0:c1,c2", "type": "spc_t"}}
            }]})),
            Some((
                String::new(),
                String::new(),
                "spc_t".to_string(),
                "s0:c1,c2".to_string()
            ))
        );
        // Pod-level applies when the container has none.
        assert_eq!(
            selinux(json!({
                "securityContext": {"seLinuxOptions": {"user": "system_u"}},
                "containers": [{"name": "app", "image": "busybox"}]
            })),
            Some((
                "system_u".to_string(),
                String::new(),
                String::new(),
                String::new()
            ))
        );
        assert_eq!(
            selinux(json!({"containers": [{"name": "app", "image": "busybox"}]})),
            None
        );
    }

    fn env_from(
        name: &str,
        src: rusternetes_common::resources::pod::EnvVarSource,
    ) -> rusternetes_common::resources::pod::EnvVar {
        rusternetes_common::resources::pod::EnvVar {
            name: name.to_string(),
            value: None,
            value_from: Some(src),
        }
    }

    fn empty_source() -> rusternetes_common::resources::pod::EnvVarSource {
        rusternetes_common::resources::pod::EnvVarSource {
            config_map_key_ref: None,
            secret_key_ref: None,
            field_ref: None,
            resource_field_ref: None,
            file_key_ref: None,
        }
    }

    #[test]
    fn field_ref_resolves_labels_annotations_spec_and_phase() {
        use rusternetes_common::types::Phase;
        let mut pod = pod_with(PodSpec {
            restart_policy: Some("OnFailure".to_string()),
            scheduler_name: Some("custom-sched".to_string()),
            ..Default::default()
        });
        pod.metadata.labels = Some(HashMap::from([("app".to_string(), "web".to_string())]));
        pod.metadata.annotations = Some(HashMap::from([("team".to_string(), "infra".to_string())]));
        pod.status = Some(rusternetes_common::resources::pod::PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });

        assert_eq!(
            pod_field_value(&pod, "metadata.labels['app']").as_deref(),
            Some("web")
        );
        assert_eq!(
            pod_field_value(&pod, "metadata.annotations['team']").as_deref(),
            Some("infra")
        );
        assert_eq!(
            pod_field_value(&pod, "spec.restartPolicy").as_deref(),
            Some("OnFailure")
        );
        assert_eq!(
            pod_field_value(&pod, "spec.schedulerName").as_deref(),
            Some("custom-sched")
        );
        assert_eq!(
            pod_field_value(&pod, "status.phase").as_deref(),
            Some("Running")
        );
        // Missing subscript key resolves to None (var omitted).
        assert_eq!(pod_field_value(&pod, "metadata.labels['nope']"), None);
    }

    #[test]
    fn resource_field_ref_resolves_ephemeral_storage_and_hugepages() {
        use rusternetes_common::types::ResourceRequirements;
        let c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            resources: Some(ResourceRequirements {
                limits: Some(HashMap::from([
                    ("ephemeral-storage".to_string(), "1Gi".to_string()),
                    ("hugepages-2Mi".to_string(), "4Mi".to_string()),
                ])),
                requests: None,
                claims: None,
            }),
            ..Default::default()
        };
        // Both normalize to bytes, like memory (not raw passthrough).
        assert_eq!(
            container_resource_value(&c, "limits.ephemeral-storage", None, None).as_deref(),
            Some("1073741824")
        );
        assert_eq!(
            container_resource_value(&c, "limits.hugepages-2Mi", None, None).as_deref(),
            Some("4194304")
        );
    }

    #[test]
    fn resource_field_ref_defaults_unset_limits_to_node_allocatable() {
        use rusternetes_common::types::ResourceRequirements;
        // upstream MergeContainerResourceLimits: an unset cpu/memory LIMIT
        // defaults to the node's allocatable for resourceFieldRef extraction
        // (the "default limits.cpu/memory from node allocatable" NodeConformance
        // spec). cpu rounds up to whole cores; memory is bytes.
        let c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            resources: Some(ResourceRequirements {
                limits: None,
                requests: None,
                claims: None,
            }),
            ..Default::default()
        };
        let alloc = HashMap::from([
            ("cpu".to_string(), "4".to_string()),
            ("memory".to_string(), "8Gi".to_string()),
        ]);
        assert_eq!(
            container_resource_value(&c, "limits.cpu", None, Some(&alloc)).as_deref(),
            Some("4")
        );
        assert_eq!(
            container_resource_value(&c, "limits.memory", None, Some(&alloc)).as_deref(),
            Some("8589934592")
        );
        // No node allocatable available → var omitted, as before.
        assert_eq!(container_resource_value(&c, "limits.cpu", None, None), None);
    }

    #[test]
    fn config_map_and_secret_key_ref_env_resolved() {
        use rusternetes_common::resources::pod::{ConfigMapKeySelector, SecretKeySelector};
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env = Some(vec![
            env_from(
                "FROM_CM",
                rusternetes_common::resources::pod::EnvVarSource {
                    config_map_key_ref: Some(ConfigMapKeySelector {
                        name: "cfg".to_string(),
                        key: "color".to_string(),
                        optional: None,
                    }),
                    ..empty_source()
                },
            ),
            env_from(
                "FROM_SECRET",
                rusternetes_common::resources::pod::EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "creds".to_string(),
                        key: "password".to_string(),
                        optional: None,
                    }),
                    ..empty_source()
                },
            ),
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let mut cm = ConfigMap::new("cfg", "prod");
        cm.data = Some(HashMap::from([("color".to_string(), "blue".to_string())]));
        let mut secret = Secret::new("creds", "prod");
        secret.data = Some(HashMap::from([(
            "password".to_string(),
            b"s3cr3t".to_vec(),
        )]));
        let config_maps = HashMap::from([("cfg".to_string(), cm)]);
        let secrets = HashMap::from([("creds".to_string(), secret)]);

        let cfg = container_config(&pod, &c, "img", &HashMap::new(), &config_maps, &secrets);
        let get = |k: &str| {
            cfg.envs
                .iter()
                .find(|e| e.key == k)
                .map(|e| e.value.as_str())
        };
        assert_eq!(get("FROM_CM"), Some("blue"));
        assert_eq!(get("FROM_SECRET"), Some("s3cr3t"));
    }

    #[test]
    fn env_from_bulk_imports_configmap_and_secret_with_prefix() {
        use rusternetes_common::resources::pod::{
            ConfigMapEnvSource, EnvFromSource, SecretEnvSource,
        };
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env_from = Some(vec![
            EnvFromSource {
                prefix: None,
                config_map_ref: Some(ConfigMapEnvSource {
                    name: "cfg".to_string(),
                    optional: None,
                }),
                secret_ref: None,
            },
            EnvFromSource {
                prefix: Some("SEC_".to_string()),
                config_map_ref: None,
                secret_ref: Some(SecretEnvSource {
                    name: "creds".to_string(),
                    optional: None,
                }),
            },
        ]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let mut cm = ConfigMap::new("cfg", "prod");
        cm.data = Some(HashMap::from([
            ("COLOR".to_string(), "blue".to_string()),
            ("SIZE".to_string(), "large".to_string()),
        ]));
        let mut secret = Secret::new("creds", "prod");
        secret.data = Some(HashMap::from([("TOKEN".to_string(), b"abc".to_vec())]));
        let config_maps = HashMap::from([("cfg".to_string(), cm)]);
        let secrets = HashMap::from([("creds".to_string(), secret)]);

        let cfg = container_config(&pod, &c, "img", &HashMap::new(), &config_maps, &secrets);
        let get = |k: &str| {
            cfg.envs
                .iter()
                .find(|e| e.key == k)
                .map(|e| e.value.as_str())
        };
        assert_eq!(get("COLOR"), Some("blue"));
        assert_eq!(get("SIZE"), Some("large"));
        // The secretRef prefix is prepended to every imported key.
        assert_eq!(get("SEC_TOKEN"), Some("abc"));
    }

    #[test]
    fn explicit_env_overrides_env_from_without_duplicating() {
        use rusternetes_common::resources::pod::{ConfigMapEnvSource, EnvFromSource, EnvVar};
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env_from = Some(vec![EnvFromSource {
            prefix: None,
            config_map_ref: Some(ConfigMapEnvSource {
                name: "cfg".to_string(),
                optional: None,
            }),
            secret_ref: None,
        }]);
        c.env = Some(vec![EnvVar {
            name: "COLOR".to_string(),
            value: Some("red".to_string()),
            value_from: None,
        }]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });

        let mut cm = ConfigMap::new("cfg", "prod");
        cm.data = Some(HashMap::from([("COLOR".to_string(), "blue".to_string())]));
        let config_maps = HashMap::from([("cfg".to_string(), cm)]);

        let cfg = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &config_maps,
            &HashMap::new(),
        );
        let colors: Vec<&str> = cfg
            .envs
            .iter()
            .filter(|e| e.key == "COLOR")
            .map(|e| e.value.as_str())
            .collect();
        // Explicit env wins over envFrom, and there is exactly one COLOR entry.
        assert_eq!(colors, vec!["red"]);
    }

    #[test]
    fn missing_key_ref_omits_var() {
        use rusternetes_common::resources::pod::ConfigMapKeySelector;
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env = Some(vec![env_from(
            "MISSING",
            rusternetes_common::resources::pod::EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: "absent".to_string(),
                    key: "k".to_string(),
                    optional: Some(true),
                }),
                ..empty_source()
            },
        )]);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let cfg = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        assert!(cfg.envs.iter().all(|e| e.key != "MISSING"));
    }

    fn pod_with_env(env: Vec<rusternetes_common::resources::pod::EnvVar>) -> (Pod, Container) {
        let mut c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        c.env = Some(env);
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        (pod, c)
    }

    #[test]
    fn validate_rejects_nonoptional_missing_key() {
        use rusternetes_common::resources::pod::ConfigMapKeySelector;
        let (pod, c) = pod_with_env(vec![env_from(
            "NEED",
            rusternetes_common::resources::pod::EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: "cfg".to_string(),
                    key: "absent".to_string(),
                    optional: None,
                }),
                ..empty_source()
            },
        )]);
        // ConfigMap exists but lacks the key.
        let mut cm = ConfigMap::new("cfg", "prod");
        cm.data = Some(HashMap::from([("present".to_string(), "x".to_string())]));
        let config_maps = HashMap::from([("cfg".to_string(), cm)]);
        let err = validate_env_key_refs(&pod, &c, &config_maps, &HashMap::new()).unwrap_err();
        assert_eq!(err, "couldn't find key absent in ConfigMap prod/cfg");
    }

    #[test]
    fn validate_rejects_nonoptional_missing_object() {
        use rusternetes_common::resources::pod::SecretKeySelector;
        let (pod, c) = pod_with_env(vec![env_from(
            "NEED",
            rusternetes_common::resources::pod::EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: "creds".to_string(),
                    key: "password".to_string(),
                    optional: None,
                }),
                ..empty_source()
            },
        )]);
        let err = validate_env_key_refs(&pod, &c, &HashMap::new(), &HashMap::new()).unwrap_err();
        assert_eq!(err, "couldn't get Secret prod/creds");
    }

    #[test]
    fn validate_allows_optional_missing_and_resolved_refs() {
        use rusternetes_common::resources::pod::{ConfigMapKeySelector, SecretKeySelector};
        let (pod, c) = pod_with_env(vec![
            // optional + missing -> ok
            env_from(
                "OPT",
                rusternetes_common::resources::pod::EnvVarSource {
                    config_map_key_ref: Some(ConfigMapKeySelector {
                        name: "gone".to_string(),
                        key: "k".to_string(),
                        optional: Some(true),
                    }),
                    ..empty_source()
                },
            ),
            // non-optional + present -> ok
            env_from(
                "OK",
                rusternetes_common::resources::pod::EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "creds".to_string(),
                        key: "password".to_string(),
                        optional: None,
                    }),
                    ..empty_source()
                },
            ),
        ]);
        let mut s = Secret::new("creds", "prod");
        s.data = Some(HashMap::from([("password".to_string(), b"x".to_vec())]));
        let secrets = HashMap::from([("creds".to_string(), s)]);
        assert!(validate_env_key_refs(&pod, &c, &HashMap::new(), &secrets).is_ok());
    }

    fn container_with_env_from(
        env_from: Vec<rusternetes_common::resources::pod::EnvFromSource>,
    ) -> (Pod, Container) {
        let c = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            env_from: Some(env_from),
            ..Default::default()
        };
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        (pod, c)
    }

    #[test]
    fn validate_rejects_nonoptional_missing_envfrom_configmap() {
        use rusternetes_common::resources::pod::{ConfigMapEnvSource, EnvFromSource};
        let (pod, c) = container_with_env_from(vec![EnvFromSource {
            prefix: None,
            config_map_ref: Some(ConfigMapEnvSource {
                name: "bulk".to_string(),
                optional: None,
            }),
            secret_ref: None,
        }]);
        let err = validate_env_key_refs(&pod, &c, &HashMap::new(), &HashMap::new()).unwrap_err();
        assert_eq!(err, "couldn't get ConfigMap prod/bulk");
    }

    #[test]
    fn validate_rejects_nonoptional_missing_envfrom_secret() {
        use rusternetes_common::resources::pod::{EnvFromSource, SecretEnvSource};
        let (pod, c) = container_with_env_from(vec![EnvFromSource {
            prefix: None,
            config_map_ref: None,
            secret_ref: Some(SecretEnvSource {
                name: "bulk-creds".to_string(),
                optional: None,
            }),
        }]);
        let err = validate_env_key_refs(&pod, &c, &HashMap::new(), &HashMap::new()).unwrap_err();
        assert_eq!(err, "couldn't get Secret prod/bulk-creds");
    }

    #[test]
    fn validate_allows_optional_missing_and_present_envfrom() {
        use rusternetes_common::resources::pod::{
            ConfigMapEnvSource, EnvFromSource, SecretEnvSource,
        };
        let (pod, c) = container_with_env_from(vec![
            // optional + missing -> ok
            EnvFromSource {
                prefix: None,
                config_map_ref: Some(ConfigMapEnvSource {
                    name: "gone".to_string(),
                    optional: Some(true),
                }),
                secret_ref: None,
            },
            // non-optional + present -> ok
            EnvFromSource {
                prefix: None,
                config_map_ref: None,
                secret_ref: Some(SecretEnvSource {
                    name: "creds".to_string(),
                    optional: None,
                }),
            },
        ]);
        let secrets = HashMap::from([("creds".to_string(), Secret::new("creds", "prod"))]);
        assert!(validate_env_key_refs(&pod, &c, &HashMap::new(), &secrets).is_ok());
    }

    fn sec_ctx_for(sec_ctx: serde_json::Value) -> v1::LinuxContainerSecurityContext {
        let mut c: Container = serde_json::from_value(serde_json::json!({
            "name": "app",
            "image": "busybox",
        }))
        .unwrap();
        if !sec_ctx.is_null() {
            c.security_context = Some(serde_json::from_value(sec_ctx).unwrap());
        }
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .linux
        .unwrap()
        .security_context
        .unwrap()
    }

    #[test]
    fn default_procmount_sends_default_masked_and_readonly_paths() {
        // Unset procMount → upstream default masked/readonly paths.
        let sc = sec_ctx_for(serde_json::Value::Null);
        assert_eq!(sc.masked_paths, DEFAULT_MASKED_PATHS);
        assert_eq!(sc.readonly_paths, DEFAULT_READONLY_PATHS);
    }

    #[test]
    fn explicit_default_procmount_sends_defaults() {
        let sc = sec_ctx_for(serde_json::json!({"procMount": "Default"}));
        assert_eq!(sc.masked_paths, DEFAULT_MASKED_PATHS);
        assert_eq!(sc.readonly_paths, DEFAULT_READONLY_PATHS);
    }

    #[test]
    fn unmasked_procmount_sends_empty_paths() {
        // procMount: Unmasked → nothing masked/readonly (upstream returns []).
        let sc = sec_ctx_for(serde_json::json!({"procMount": "Unmasked"}));
        assert!(sc.masked_paths.is_empty());
        assert!(sc.readonly_paths.is_empty());
    }

    #[test]
    fn bare_container_still_gets_masked_paths() {
        // A container with no securityContext must still receive the default
        // masked paths — containerd resets them to empty otherwise, leaving
        // /proc exposed.
        let c: Container = serde_json::from_value(serde_json::json!({
            "name": "app",
            "image": "busybox",
        }))
        .unwrap();
        let pod = pod_with(PodSpec {
            containers: vec![c.clone()],
            ..Default::default()
        });
        let sc = container_config(
            &pod,
            &c,
            "img",
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .linux
        .unwrap()
        .security_context
        .unwrap();
        assert_eq!(sc.masked_paths, DEFAULT_MASKED_PATHS);
    }
}
