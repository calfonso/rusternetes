//! Pod validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go` (release-1.35).
//!
//! Two layers:
//! * **Create-side** ([`validate_pod_create`] / [`validate_pod_spec`]) — the
//!   per-field validators the upstream `ValidatePod` / `ValidatePodSpec`
//!   pipeline runs on a brand-new Pod. Ported from
//!   `pkg/apis/core/validation/validation.go::ValidatePod`,
//!   `ValidatePodSpec`, `validateContainers`, `validateInitContainers`,
//!   `validateContainerPorts`, `validateProbe`, `validateContainerResources`,
//!   `validateRestartPolicy`, `validateDNSPolicy`, `validateVolumes`,
//!   `validateTolerations`. Tests mirror
//!   `TestValidatePod`, `TestValidatePodSpec`, `TestValidateContainers`,
//!   `TestValidateInitContainers` from upstream
//!   `pkg/apis/core/validation/validation_test.go`.
//! * **Update-side** ([`validate_pod_spec_update`]) — composes the four
//!   upstream pre-checks (container count, tolerations additions-only,
//!   schedulingGates deletions-only, terminationGracePeriodSeconds
//!   immutability with the negative→1 relaxation), the gated-pod
//!   `nodeSelector` / `nodeAffinity` relaxation (KEP-3521), and a
//!   munge+DeepEqual fence that catches everything else.

use std::collections::{HashMap, HashSet};

use crate::resources::pod::{
    Container, EnvVar, Lifecycle, LifecycleHandler, NodeAffinity, NodeSelectorTerm, Pod,
    PodDNSConfig, PodSchedulingGate, PodSecurityContext, PodSpec, Probe, Toleration,
    TopologySpreadConstraint, Volume, VolumeMount,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_dns1123_subdomain_with_underscore,
    is_qualified_name, validate_label_name, validate_label_selector,
    LabelSelectorValidationOptions,
};
use once_cell::sync::Lazy;
use regex::Regex;

/// Upstream `envVarNameFmt` / `envVarNameRegexp`.
static ENV_VAR_NAME_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[-._a-zA-Z][-._a-zA-Z0-9]*$").expect("env var name regex"));
const ENV_VAR_NAME_ERR_MSG: &str = "a valid environment variable name must consist of alphabetic characters, digits, '_', '-', or '.', and must not start with a digit";

/// Upstream `configMapKeyFmt` / `configMapKeyRegexp`.
static CONFIG_MAP_KEY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[-._a-zA-Z0-9]+$").expect("config map key regex"));

// ---------------------------------------------------------------------------
// Create-side validation
// ---------------------------------------------------------------------------

/// Top-level create-side validator.
///
/// Mirrors upstream `ValidatePod`
/// (`pkg/apis/core/validation/validation.go`, release-1.35).
///
/// `allow_relaxed_dns_search` should be wired from the
/// `RelaxedDNSSearchValidation` feature gate.
pub fn validate_pod_create(pod: &Pod, allow_relaxed_dns_search: bool) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let spec_path = Path::new("spec");

    let Some(ref spec) = pod.spec else {
        errs.push(Error::required(
            &spec_path.child("containers"),
            "must have at least one container",
        ));
        return errs;
    };

    errs.extend(validate_pod_spec(
        spec,
        &spec_path,
        allow_relaxed_dns_search,
    ));
    errs
}

/// Mirrors upstream `ValidatePodSpec`
/// (`pkg/apis/core/validation/validation.go`, release-1.35).
///
/// This function is intentionally conservative — it only rejects what Go's
/// upstream `ValidatePodSpec` rejects. Extra fields (volumes, probes,
/// resources) are validated by their own sub-validators below.
pub fn validate_pod_spec(
    spec: &PodSpec,
    fld_path: &Path,
    allow_relaxed_dns_search: bool,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let containers_path = fld_path.child("containers");

    // Declared volume names, for the volumeMount existence check
    // (upstream `IsMatchedVolume`).
    let volume_names: HashSet<&str> = spec
        .volumes
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|v| v.name.as_str())
        .collect();

    // Containers must be non-empty.
    if spec.containers.is_empty() {
        errs.push(Error::required(
            &containers_path,
            "must have at least one container",
        ));
    }

    // Validate containers and collect names for uniqueness check.
    let mut all_names: HashSet<String> = HashSet::new();
    for (i, c) in spec.containers.iter().enumerate() {
        errs.extend(validate_container(
            c,
            false,
            &volume_names,
            &containers_path.index(i),
        ));
        if !c.name.is_empty() && !all_names.insert(c.name.clone()) {
            errs.push(Error::duplicate(
                &containers_path.index(i).child("name"),
                c.name.clone(),
            ));
        }
    }

    // Validate init containers.
    if let Some(ref inits) = spec.init_containers {
        let init_path = fld_path.child("initContainers");
        for (i, c) in inits.iter().enumerate() {
            errs.extend(validate_container(
                c,
                true,
                &volume_names,
                &init_path.index(i),
            ));
            if !c.name.is_empty() && !all_names.insert(c.name.clone()) {
                errs.push(Error::duplicate(
                    &init_path.index(i).child("name"),
                    c.name.clone(),
                ));
            }
        }
    }

    // Ephemeral containers are forbidden on create.
    // Upstream: `pkg/registry/core/pod/strategy.go PrepareForCreate`.
    if let Some(ref ecs) = spec.ephemeral_containers {
        if !ecs.is_empty() {
            errs.push(Error::forbidden(
                &fld_path.child("ephemeralContainers"),
                "cannot be set on create",
            ));
        }
        // Still check EC names for uniqueness even if forbidden.
        let ec_path = fld_path.child("ephemeralContainers");
        for (i, ec) in ecs.iter().enumerate() {
            if !ec.name.is_empty() && !all_names.insert(ec.name.clone()) {
                errs.push(Error::duplicate(
                    &ec_path.index(i).child("name"),
                    ec.name.clone(),
                ));
            }
        }
    }

    // hostNetwork: a container port's hostPort must match its containerPort.
    // Upstream `validatePodHostNetworkDeps`: for a Pod (ResourceIsPod), the
    // values must be equal — an unset hostPort (0) does not match a non-zero
    // containerPort. Only regular containers are checked, matching upstream.
    if spec.host_network.unwrap_or(false) {
        for (i, c) in spec.containers.iter().enumerate() {
            if let Some(ports) = c.ports.as_ref() {
                for (j, p) in ports.iter().enumerate() {
                    let hp = p.host_port.unwrap_or(0);
                    if hp != p.container_port {
                        errs.push(Error::invalid(
                            &containers_path
                                .index(i)
                                .child("ports")
                                .index(j)
                                .child("hostPort"),
                            hp as i64,
                            "must match `containerPort` when `hostNetwork` is true",
                        ));
                    }
                }
            }
        }
    }

    // restartPolicy enum.
    errs.extend(validate_restart_policy(
        spec.restart_policy.as_deref(),
        &fld_path.child("restartPolicy"),
    ));

    // terminationGracePeriodSeconds >= 0.
    // Upstream: `validateTerminationGracePeriod`
    if let Some(tgps) = spec.termination_grace_period_seconds {
        if tgps < 0 {
            errs.push(Error::invalid(
                &fld_path.child("terminationGracePeriodSeconds"),
                tgps,
                "must be greater than or equal to 0",
            ));
        }
    }

    // activeDeadlineSeconds > 0.
    // Upstream: `validateActiveDeadlineSeconds`
    if let Some(ads) = spec.active_deadline_seconds {
        if ads <= 0 {
            errs.push(Error::invalid(
                &fld_path.child("activeDeadlineSeconds"),
                ads,
                "must be greater than 0",
            ));
        }
    }

    // Volumes: unique names.
    if let Some(ref vols) = spec.volumes {
        errs.extend(validate_volumes(vols, &fld_path.child("volumes")));
    }

    // Tolerations.
    if let Some(ref tols) = spec.tolerations {
        errs.extend(validate_tolerations(tols, &fld_path.child("tolerations")));
    }

    // topologySpreadConstraints.
    if let Some(ref tscs) = spec.topology_spread_constraints {
        errs.extend(validate_topology_spread_constraints(
            tscs,
            &fld_path.child("topologySpreadConstraints"),
        ));
    }

    // dnsPolicy + dnsConfig consistency.
    errs.extend(validate_dns_policy(
        spec.dns_policy.as_deref(),
        &fld_path.child("dnsPolicy"),
    ));
    errs.extend(validate_pod_dns_config(
        spec.dns_config.as_ref(),
        spec.dns_policy.as_deref(),
        allow_relaxed_dns_search,
        &fld_path.child("dnsConfig"),
    ));

    // securityContext.sysctls name format / uniqueness.
    if let Some(ref sc) = spec.security_context {
        errs.extend(validate_sysctls(
            sc,
            &fld_path.child("securityContext").child("sysctls"),
        ));
    }

    errs
}

/// Upstream `SysctlMaxLength` (`pkg/apis/core/validation/validation.go`).
const SYSCTL_MAX_LENGTH: usize = 253;

/// Printed form of upstream `sysctlContainSlashRegexp`, used verbatim in the
/// error detail so log greps that key on the message stay valid.
const SYSCTL_REGEX_STR: &str =
    r"^([a-z0-9]([-_a-z0-9]*[a-z0-9])?[\./])*[a-z0-9]([-_a-z0-9]*[a-z0-9])?$";

/// A single dot/slash-separated segment of a sysctl name. Mirrors upstream
/// `SysctlSegmentFmt` = `[a-z0-9]([-_a-z0-9]*[a-z0-9])?`: lowercase-alnum at both
/// ends, with `-`/`_`/alnum allowed in between.
fn is_valid_sysctl_segment(seg: &str) -> bool {
    let b = seg.as_bytes();
    if b.is_empty() {
        return false;
    }
    let is_alnum = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    let is_inner = |c: u8| is_alnum(c) || c == b'-' || c == b'_';
    is_alnum(b[0]) && is_alnum(b[b.len() - 1]) && b.iter().all(|&c| is_inner(c))
}

/// Upstream `IsValidSysctlName`: at most [`SYSCTL_MAX_LENGTH`] chars and matching
/// `SysctlContainSlashFmt` — one or more [`is_valid_sysctl_segment`] segments
/// joined by `.` or `/` separators (no leading/trailing/empty segments).
pub fn is_valid_sysctl_name(name: &str) -> bool {
    if name.is_empty() || name.len() > SYSCTL_MAX_LENGTH {
        return false;
    }
    name.split(['.', '/']).all(is_valid_sysctl_segment)
}

/// Upstream `validateSysctls`: each `securityContext.sysctls[]` name must be
/// present, a valid sysctl name, and unique.
fn validate_sysctls(sc: &PodSecurityContext, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(sysctls) = sc.sysctls.as_ref() else {
        return errs;
    };
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, s) in sysctls.iter().enumerate() {
        let name_path = fld_path.index(i).child("name");
        if s.name.is_empty() {
            errs.push(Error::required(&name_path, ""));
        } else if !is_valid_sysctl_name(&s.name) {
            errs.push(Error::invalid(
                &name_path,
                s.name.clone(),
                format!(
                    "must have at most {SYSCTL_MAX_LENGTH} characters and match regex {SYSCTL_REGEX_STR}"
                ),
            ));
        } else if seen.contains(s.name.as_str()) {
            errs.push(Error::duplicate(&name_path, s.name.clone()));
        }
        seen.insert(s.name.as_str());
    }
    errs
}

/// Validates a single container (regular or init). When `is_init` is true,
/// init-container-specific restrictions are applied (no readinessProbe,
/// no lifecycle).
///
/// Mirrors upstream `validateContainer` / `validateInitContainer`
/// (`pkg/apis/core/validation/validation.go`, release-1.35).
fn validate_container(
    c: &Container,
    is_init: bool,
    volume_names: &HashSet<&str>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // name — DNS-1123 label.
    if c.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    } else {
        for msg in is_dns1123_label(&c.name) {
            errs.push(Error::invalid(&fld_path.child("name"), c.name.clone(), msg));
        }
    }

    // image — must be non-empty.
    if c.image.is_empty() {
        errs.push(Error::required(&fld_path.child("image"), ""));
    }

    // ports.
    if let Some(ref ports) = c.ports {
        errs.extend(validate_container_ports(ports, &fld_path.child("ports")));
    }

    // Restartable init containers (sidecars: restartPolicy=Always) may carry
    // livenessProbe/readinessProbe/startupProbe/lifecycle — they run for the
    // life of the pod like regular containers. Plain init containers may not.
    // Upstream: pkg/apis/core/validation/validation.go validateInitContainers
    // (forbidden only "for init containers without restartPolicy=Always").
    let is_restartable_init = is_init && c.restart_policy.as_deref() == Some("Always");

    // probes.
    if let Some(ref p) = c.liveness_probe {
        errs.extend(validate_probe(p, &fld_path.child("livenessProbe")));
    }
    if let Some(ref p) = c.readiness_probe {
        if is_init && !is_restartable_init {
            errs.push(Error::forbidden(
                &fld_path.child("readinessProbe"),
                "must not be set for init containers",
            ));
        } else {
            errs.extend(validate_probe(p, &fld_path.child("readinessProbe")));
        }
    }
    if let Some(ref p) = c.startup_probe {
        errs.extend(validate_probe(p, &fld_path.child("startupProbe")));
    }

    // lifecycle — only restartable init containers may have lifecycle hooks.
    if is_init && !is_restartable_init && c.lifecycle.is_some() {
        errs.push(Error::forbidden(
            &fld_path.child("lifecycle"),
            "must not be set for init containers",
        ));
    }
    if let Some(ref lc) = c.lifecycle {
        errs.extend(validate_lifecycle(lc, &fld_path.child("lifecycle")));
    }

    // resources.
    if let Some(ref res) = c.resources {
        errs.extend(validate_resource_requirements(
            res,
            &fld_path.child("resources"),
        ));
    }

    // volumeMounts.
    if let Some(ref mounts) = c.volume_mounts {
        errs.extend(validate_volume_mounts(
            mounts,
            volume_names,
            c,
            &fld_path.child("volumeMounts"),
        ));
    }

    // env.
    if let Some(ref env) = c.env {
        errs.extend(validate_env(env, &fld_path.child("env")));
    }

    // imagePullPolicy enum (upstream validatePullPolicy). Unset/empty is
    // defaulted at runtime, so only an explicit unsupported value is rejected.
    if let Some(policy) = c.image_pull_policy.as_deref().filter(|p| !p.is_empty()) {
        if !PULL_POLICIES.contains(&policy) {
            errs.push(Error::not_supported(
                &fld_path.child("imagePullPolicy"),
                policy.to_string(),
                PULL_POLICIES,
            ));
        }
    }

    // terminationMessagePolicy enum (upstream validateContainerCommon).
    if let Some(tmp) = c
        .termination_message_policy
        .as_deref()
        .filter(|p| !p.is_empty())
    {
        if !TERMINATION_MESSAGE_POLICIES.contains(&tmp) {
            errs.push(Error::not_supported(
                &fld_path.child("terminationMessagePolicy"),
                tmp.to_string(),
                TERMINATION_MESSAGE_POLICIES,
            ));
        }
    }

    errs
}

const PULL_POLICIES: &[&str] = &["Always", "Never", "IfNotPresent"];
const TERMINATION_MESSAGE_POLICIES: &[&str] = &["File", "FallbackToLogsOnError"];

/// Port of upstream `validateLocalDescendingPath`: a `subPath`/`subPathExpr`
/// must be relative and contain no `..` component.
fn validate_local_descending_path(target: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if target.starts_with('/') {
        errs.push(Error::invalid(
            fld_path,
            target.to_string(),
            "must be a relative path",
        ));
    }
    if target.split('/').any(|seg| seg == "..") {
        errs.push(Error::invalid(
            fld_path,
            target.to_string(),
            "must not contain '..'",
        ));
    }
    errs
}

/// Port of upstream `validation.IsEnvVarName`: the env-var name format plus the
/// `hasChDirPrefix` guard (must not be `.`/`..` or start with `..`).
fn is_env_var_name(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if !ENV_VAR_NAME_RE.is_match(value) {
        errs.push(format!(
            "{ENV_VAR_NAME_ERR_MSG} (regex used for validation is '[-._a-zA-Z][-._a-zA-Z0-9]*')"
        ));
    }
    match value {
        "." => errs.push("must not be '.'".to_string()),
        ".." => errs.push("must not be '..'".to_string()),
        v if v.starts_with("..") => errs.push("must not start with '..'".to_string()),
        _ => {}
    }
    errs
}

/// Port of upstream `validateLifecycle`: validates the `postStart`/`preStop`
/// handlers (each must specify exactly one handler type).
fn validate_lifecycle(lifecycle: &Lifecycle, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(ps) = &lifecycle.post_start {
        errs.extend(validate_lifecycle_handler(ps, &fld_path.child("postStart")));
    }
    if let Some(pre) = &lifecycle.pre_stop {
        errs.extend(validate_lifecycle_handler(pre, &fld_path.child("preStop")));
    }
    errs
}

/// Port of upstream `ValidateVolumeMounts`: each mount needs a name that matches
/// a declared volume, a unique non-empty `mountPath` that does not collide with
/// the container's `volumeDevices`, `subPath`/`subPathExpr` that are mutually
/// exclusive relative non-backstepping paths, a supported `mountPropagation`
/// (`Bidirectional` only on privileged containers), and a `recursiveReadOnly`
/// consistent with `readOnly`/`mountPropagation`.
fn validate_volume_mounts(
    mounts: &[VolumeMount],
    volume_names: &HashSet<&str>,
    container: &Container,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut mountpoints: HashSet<&str> = HashSet::new();

    // volumeDevices name/path sets for the overlap check (upstream
    // `mountNameAlreadyExists` / `mountPathAlreadyExists`).
    let (device_names, device_paths): (HashSet<&str>, HashSet<&str>) = container
        .volume_devices
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|d| (d.name.as_str(), d.device_path.as_str()))
        .unzip();

    for (i, mnt) in mounts.iter().enumerate() {
        let idx = fld_path.index(i);

        if mnt.name.is_empty() {
            errs.push(Error::required(&idx.child("name"), ""));
        } else if !volume_names.contains(mnt.name.as_str()) {
            errs.push(Error::not_found(&idx.child("name"), mnt.name.clone()));
        }

        if mnt.mount_path.is_empty() {
            errs.push(Error::required(&idx.child("mountPath"), ""));
        }
        if !mnt.mount_path.is_empty() && !mountpoints.insert(mnt.mount_path.as_str()) {
            errs.push(Error::invalid(
                &idx.child("mountPath"),
                mnt.mount_path.clone(),
                "must be unique",
            ));
        }

        // Overlap with volumeDevices.
        if device_names.contains(mnt.name.as_str()) {
            errs.push(Error::invalid(
                &idx.child("name"),
                mnt.name.clone(),
                "must not already exist in volumeDevices",
            ));
        }
        if device_paths.contains(mnt.mount_path.as_str()) {
            errs.push(Error::invalid(
                &idx.child("mountPath"),
                mnt.mount_path.clone(),
                "must not already exist as a path in volumeDevices",
            ));
        }

        if let Some(sub_path) = mnt.sub_path.as_deref().filter(|s| !s.is_empty()) {
            errs.extend(validate_local_descending_path(
                sub_path,
                &fld_path.child("subPath"),
            ));
        }

        if let Some(sub_path_expr) = mnt.sub_path_expr.as_deref().filter(|s| !s.is_empty()) {
            if mnt.sub_path.as_deref().is_some_and(|s| !s.is_empty()) {
                errs.push(Error::invalid(
                    &idx.child("subPathExpr"),
                    sub_path_expr.to_string(),
                    "subPathExpr and subPath are mutually exclusive",
                ));
            }
            errs.extend(validate_local_descending_path(
                sub_path_expr,
                &fld_path.child("subPathExpr"),
            ));
        }

        errs.extend(validate_mount_propagation(
            mnt.mount_propagation.as_deref(),
            container,
            &fld_path.child("mountPropagation"),
        ));
        errs.extend(validate_mount_recursive_read_only(
            mnt,
            &fld_path.child("recursiveReadOnly"),
        ));
    }
    errs
}

const MOUNT_PROPAGATION_MODES: &[&str] = &["Bidirectional", "HostToContainer", "None"];

/// Port of upstream `validateMountPropagation`: a supported enum value, and
/// `Bidirectional` only on privileged containers.
fn validate_mount_propagation(
    mode: Option<&str>,
    container: &Container,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(mode) = mode else {
        return errs;
    };
    if !MOUNT_PROPAGATION_MODES.contains(&mode) {
        errs.push(Error::not_supported(
            fld_path,
            mode.to_string(),
            MOUNT_PROPAGATION_MODES,
        ));
    }
    let privileged = container
        .security_context
        .as_ref()
        .and_then(|sc| sc.privileged)
        .unwrap_or(false);
    if mode == "Bidirectional" && !privileged {
        errs.push(Error::forbidden(
            fld_path,
            "Bidirectional mount propagation is available only to privileged containers",
        ));
    }
    errs
}

/// Port of upstream `validateMountRecursiveReadOnly`: `Enabled`/`IfPossible`
/// require `readOnly: true` and `mountPropagation` `None`/unset; an unrecognized
/// value is rejected.
fn validate_mount_recursive_read_only(mount: &VolumeMount, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(rro) = mount.recursive_read_only.as_deref() else {
        return errs;
    };
    match rro {
        "Disabled" => {}
        "Enabled" | "IfPossible" => {
            if !mount.read_only.unwrap_or(false) {
                errs.push(Error::forbidden(
                    fld_path,
                    "may only be specified when readOnly is true",
                ));
            }
            if mount
                .mount_propagation
                .as_deref()
                .is_some_and(|p| p != "None")
            {
                errs.push(Error::forbidden(
                    fld_path,
                    "may only be specified when mountPropagation is None or not specified",
                ));
            }
        }
        other => {
            errs.push(Error::not_supported(
                fld_path,
                other.to_string(),
                &["Disabled", "IfPossible", "Enabled"],
            ));
        }
    }
    errs
}

/// Port of upstream `validateEnv`: each env var needs a valid name, and its
/// `valueFrom` (when present) must reference exactly one source and not coexist
/// with a non-empty `value`.
fn validate_env(env: &[EnvVar], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (i, ev) in env.iter().enumerate() {
        let idx = fld_path.index(i);
        if ev.name.is_empty() {
            errs.push(Error::required(&idx.child("name"), ""));
        } else {
            for msg in is_env_var_name(&ev.name) {
                errs.push(Error::invalid(&idx.child("name"), ev.name.clone(), msg));
            }
        }
        errs.extend(validate_env_var_value_from(ev, &idx.child("valueFrom")));
    }
    errs
}

/// Port of upstream `validateEnvVarValueFrom`: exactly one source, mutually
/// exclusive with a non-empty `value`, and the structural requireds of the
/// chosen source.
fn validate_env_var_value_from(ev: &EnvVar, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(vf) = ev.value_from.as_ref() else {
        return errs;
    };

    let mut num_sources = 0;
    if let Some(field_ref) = &vf.field_ref {
        num_sources += 1;
        errs.extend(validate_object_field_selector(
            field_ref,
            &ENV_DOWNWARD_API_FIELD_PATHS,
            &fld_path.child("fieldRef"),
        ));
    }
    if let Some(rfr) = &vf.resource_field_ref {
        num_sources += 1;
        if rfr.resource.is_empty() {
            errs.push(Error::required(
                &fld_path.child("resourceFieldRef").child("resource"),
                "",
            ));
        }
        // divisor, when set, must be a valid quantity.
        if let Some(div) = rfr.divisor.as_deref().filter(|d| !d.is_empty()) {
            if crate::quantity::Quantity::parse(div).is_err() {
                errs.push(Error::invalid(
                    &fld_path.child("resourceFieldRef").child("divisor"),
                    div.to_string(),
                    "must be a valid resource quantity",
                ));
            }
        }
    }
    if let Some(cm) = &vf.config_map_key_ref {
        num_sources += 1;
        errs.extend(validate_config_or_secret_key_selector(
            &cm.name,
            &cm.key,
            &fld_path.child("configMapKeyRef"),
        ));
    }
    if let Some(sk) = &vf.secret_key_ref {
        num_sources += 1;
        errs.extend(validate_config_or_secret_key_selector(
            &sk.name,
            &sk.key,
            &fld_path.child("secretKeyRef"),
        ));
    }
    // fileKeyRef (alpha, EnvFiles) still counts as a source so a fileKeyRef-only
    // env var isn't wrongly flagged as specifying none.
    if vf.file_key_ref.is_some() {
        num_sources += 1;
    }

    if num_sources == 0 {
        errs.push(Error::invalid(
            fld_path,
            String::new(),
            "must specify one of: `fieldRef`, `resourceFieldRef`, `configMapKeyRef` or `secretKeyRef`",
        ));
    } else if ev.value.as_deref().is_some_and(|v| !v.is_empty()) {
        errs.push(Error::invalid(
            fld_path,
            String::new(),
            "may not be specified when `value` is not empty",
        ));
    } else if num_sources > 1 {
        errs.push(Error::invalid(
            fld_path,
            String::new(),
            "may not have more than one field specified at a time",
        ));
    }
    errs
}

/// Port of upstream `validateHandler` for lifecycle hooks: exactly one of
/// `exec`/`httpGet`/`tcpSocket`/`sleep`; specifying a second is forbidden, and
/// specifying none is required.
fn validate_lifecycle_handler(handler: &LifecycleHandler, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut num_handlers = 0;
    let mut check = |present: bool, child: &str| {
        if present {
            if num_handlers > 0 {
                errs.push(Error::forbidden(
                    &fld_path.child(child),
                    "may not specify more than 1 handler type",
                ));
            } else {
                num_handlers += 1;
            }
        }
    };
    check(handler.exec.is_some(), "exec");
    check(handler.http_get.is_some(), "httpGet");
    check(handler.tcp_socket.is_some(), "tcpSocket");
    check(handler.sleep.is_some(), "sleep");
    if num_handlers == 0 {
        errs.push(Error::required(fld_path, "must specify a handler type"));
    }
    errs
}

const SCHEDULE_ACTIONS: &[&str] = &["DoNotSchedule", "ScheduleAnyway"];
const NODE_INCLUSION_POLICIES: &[&str] = &["Honor", "Ignore"];

/// Port of upstream `validateTopologySpreadConstraints`: per constraint —
/// `maxSkew > 0`, `topologyKey` required, `whenUnsatisfiable` enum, no repeated
/// `{topologyKey, whenUnsatisfiable}` tuple, `minDomains > 0` only with
/// `DoNotSchedule`, `nodeAffinityPolicy`/`nodeTaintsPolicy` enums,
/// `matchLabelKeys` (valid label names, disjoint from the selector), and the
/// `labelSelector` itself.
fn validate_topology_spread_constraints(
    constraints: &[TopologySpreadConstraint],
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (i, c) in constraints.iter().enumerate() {
        let sub = fld_path.index(i);

        if c.max_skew <= 0 {
            errs.push(Error::invalid(
                &sub.child("maxSkew"),
                c.max_skew as i64,
                "must be greater than 0",
            ));
        }
        if c.topology_key.is_empty() {
            errs.push(Error::required(
                &sub.child("topologyKey"),
                "can not be empty",
            ));
        }
        if !SCHEDULE_ACTIONS.contains(&c.when_unsatisfiable.as_str()) {
            errs.push(Error::not_supported(
                &sub.child("whenUnsatisfiable"),
                c.when_unsatisfiable.clone(),
                SCHEDULE_ACTIONS,
            ));
        }
        // {topologyKey, whenUnsatisfiable} must not repeat across constraints.
        for other in &constraints[i + 1..] {
            if c.topology_key == other.topology_key
                && c.when_unsatisfiable == other.when_unsatisfiable
            {
                errs.push(Error::duplicate(
                    &sub.child("{topologyKey, whenUnsatisfiable}"),
                    format!("{{{}, {}}}", c.topology_key, c.when_unsatisfiable),
                ));
                break;
            }
        }
        // minDomains: positive, and only with DoNotSchedule.
        if let Some(md) = c.min_domains {
            let md_path = sub.child("minDomains");
            if md <= 0 {
                errs.push(Error::invalid(
                    &md_path,
                    md as i64,
                    "must be greater than 0",
                ));
            }
            if c.when_unsatisfiable != "DoNotSchedule" {
                errs.push(Error::invalid(
                    &md_path,
                    md as i64,
                    format!(
                        "can only use minDomains if whenUnsatisfiable=DoNotSchedule, not {}",
                        c.when_unsatisfiable
                    ),
                ));
            }
        }
        // nodeAffinityPolicy / nodeTaintsPolicy enums.
        for (policy, child) in [
            (&c.node_affinity_policy, "nodeAffinityPolicy"),
            (&c.node_taints_policy, "nodeTaintsPolicy"),
        ] {
            if let Some(p) = policy.as_deref() {
                if !NODE_INCLUSION_POLICIES.contains(&p) {
                    errs.push(Error::not_supported(
                        &sub.child(child),
                        p.to_string(),
                        NODE_INCLUSION_POLICIES,
                    ));
                }
            }
        }
        // matchLabelKeys: valid label names, disjoint from the selector keys.
        errs.extend(validate_match_label_keys(
            c.match_label_keys.as_deref(),
            c.label_selector.as_ref(),
            &sub.child("matchLabelKeys"),
        ));
        // labelSelector.
        if let Some(sel) = &c.label_selector {
            errs.extend(validate_label_selector(
                sel,
                LabelSelectorValidationOptions::default(),
                &sub.child("labelSelector"),
            ));
        }
    }
    errs
}

/// Upstream `validEnvDownwardAPIFieldPathExpressions`.
const ENV_DOWNWARD_API_FIELD_PATHS: [&str; 9] = [
    "metadata.name",
    "metadata.namespace",
    "metadata.uid",
    "spec.nodeName",
    "spec.serviceAccountName",
    "status.hostIP",
    "status.hostIPs",
    "status.podIP",
    "status.podIPs",
];

/// Upstream `fieldpath.SplitMaybeSubscriptedPath`: detects the `path['key']`
/// form, returning `(path, subscript)`.
fn split_maybe_subscripted_path(field_path: &str) -> Option<(&str, &str)> {
    let s = field_path.strip_suffix("']")?;
    let (prefix, subscript) = s.split_once("['")?;
    if prefix.is_empty() {
        return None;
    }
    Some((prefix, subscript))
}

/// Port of upstream `validateObjectFieldSelector` for env `fieldRef`:
/// `apiVersion` + `fieldPath` required, subscripted `metadata.annotations`/
/// `metadata.labels` keys validated as qualified names (others reject the
/// subscript), and non-subscripted paths checked against the allowlist.
fn validate_object_field_selector(
    fs: &crate::resources::pod::ObjectFieldSelector,
    allowed: &[&str],
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if fs.api_version.as_deref().unwrap_or("").is_empty() {
        errs.push(Error::required(&fld_path.child("apiVersion"), ""));
        return errs;
    }
    if fs.field_path.is_empty() {
        errs.push(Error::required(&fld_path.child("fieldPath"), ""));
        return errs;
    }

    if let Some((path, subscript)) = split_maybe_subscripted_path(&fs.field_path) {
        match path {
            "metadata.annotations" => {
                for msg in is_qualified_name(&subscript.to_lowercase()) {
                    errs.push(Error::invalid(fld_path, subscript.to_string(), msg));
                }
            }
            "metadata.labels" => {
                for msg in is_qualified_name(subscript) {
                    errs.push(Error::invalid(fld_path, subscript.to_string(), msg));
                }
            }
            other => {
                errs.push(Error::invalid(
                    fld_path,
                    other.to_string(),
                    "does not support subscript",
                ));
            }
        }
    } else if !allowed.contains(&fs.field_path.as_str()) {
        errs.push(Error::not_supported(
            &fld_path.child("fieldPath"),
            fs.field_path.clone(),
            allowed,
        ));
    }
    errs
}

/// Port of upstream `ValidateMatchLabelKeysInTopologySpread`.
fn validate_match_label_keys(
    match_label_keys: Option<&[String]>,
    label_selector: Option<&crate::types::LabelSelector>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(keys) = match_label_keys.filter(|k| !k.is_empty()) else {
        return errs;
    };

    let mut selector_keys: HashSet<&str> = HashSet::new();
    match label_selector {
        Some(sel) => {
            if let Some(ml) = &sel.match_labels {
                selector_keys.extend(ml.keys().map(|k| k.as_str()));
            }
            if let Some(me) = &sel.match_expressions {
                selector_keys.extend(me.iter().map(|e| e.key.as_str()));
            }
        }
        None => {
            errs.push(Error::forbidden(
                fld_path,
                "must not be specified when labelSelector is not set",
            ));
        }
    }

    for (i, key) in keys.iter().enumerate() {
        errs.extend(validate_label_name(key, &fld_path.index(i)));
        if selector_keys.contains(key.as_str()) {
            errs.push(Error::invalid(
                &fld_path.index(i),
                key.clone(),
                "exists in both matchLabelKeys and labelSelector",
            ));
        }
    }
    errs
}

/// Upstream `validation.IsConfigMapKey`: ≤253 chars, `[-._a-zA-Z0-9]+`, and not
/// `.`/`..`/`..`-prefixed.
fn is_config_map_key(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > 253 {
        errs.push("must be no more than 253 bytes".to_string());
    }
    if !CONFIG_MAP_KEY_RE.is_match(value) {
        errs.push("a valid config key must consist of alphanumeric characters, '-', '_' or '.' (e.g. 'key.name', or 'KEY_NAME', or 'key-name', regex used for validation is '[-._a-zA-Z0-9]+')".to_string());
    }
    match value {
        "." => errs.push("must not be '.'".to_string()),
        ".." => errs.push("must not be '..'".to_string()),
        v if v.starts_with("..") => errs.push("must not start with '..'".to_string()),
        _ => {}
    }
    errs
}

/// Shared port of upstream `validateConfigMapKeySelector` /
/// `validateSecretKeySelector`: `name` a DNS-1123 subdomain, `key` required and
/// a valid config-map key.
fn validate_config_or_secret_key_selector(name: &str, key: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    } else {
        for msg in is_dns1123_subdomain(name) {
            errs.push(Error::invalid(
                &fld_path.child("name"),
                name.to_string(),
                msg,
            ));
        }
    }
    if key.is_empty() {
        errs.push(Error::required(&fld_path.child("key"), ""));
    } else {
        for msg in is_config_map_key(key) {
            errs.push(Error::invalid(&fld_path.child("key"), key.to_string(), msg));
        }
    }
    errs
}

/// Validates `spec.volumes[*]` — volume names must be unique DNS-1123 labels.
///
/// Mirrors upstream `validateVolumes`
/// (`pkg/apis/core/validation/validation.go`, release-1.35).
fn validate_volumes(volumes: &[Volume], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, vol) in volumes.iter().enumerate() {
        let vpath = fld_path.index(i).child("name");
        if vol.name.is_empty() {
            errs.push(Error::required(&vpath, ""));
        } else {
            for msg in is_dns1123_label(&vol.name) {
                errs.push(Error::invalid(&vpath, vol.name.clone(), msg));
            }
            if !seen.insert(vol.name.as_str()) {
                errs.push(Error::duplicate(&vpath, vol.name.clone()));
            }
        }
    }
    errs
}

/// Validates container ports.
///
/// Rules (mirroring upstream `validateContainerPorts`,
/// `pkg/apis/core/validation/validation.go`, release-1.35):
/// - `containerPort` must be in [1, 65535].
/// - `hostPort` (when set) must be in [0, 65535].
/// - `protocol` (when set) must be TCP / UDP / SCTP.
/// - Port names (when set) must be a DNS-1123 label and unique per container.
fn validate_container_ports(
    ports: &[crate::resources::pod::ContainerPort],
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut seen_names: HashSet<&str> = HashSet::new();

    for (i, p) in ports.iter().enumerate() {
        let ppath = fld_path.index(i);

        // containerPort range [1, 65535].
        // Note: container_port is u16 in our struct, so 0 is the only
        // out-of-range case (u16 max is 65535).
        if p.container_port == 0 {
            errs.push(Error::invalid(
                &ppath.child("containerPort"),
                p.container_port as i64,
                "must be between 1 and 65535, inclusive",
            ));
        }

        // hostPort range [0, 65535] — u16, so always valid.

        // protocol enum.
        if let Some(ref proto) = p.protocol {
            if !matches!(proto.as_str(), "TCP" | "UDP" | "SCTP") {
                errs.push(Error::not_supported(
                    &ppath.child("protocol"),
                    proto.clone(),
                    &["TCP", "UDP", "SCTP"],
                ));
            }
        }

        // port name must be a DNS-1123 label and unique.
        if let Some(ref name) = p.name {
            if !name.is_empty() {
                for msg in is_dns1123_label(name) {
                    errs.push(Error::invalid(&ppath.child("name"), name.clone(), msg));
                }
                if !seen_names.insert(name.as_str()) {
                    errs.push(Error::duplicate(&ppath.child("name"), name.clone()));
                }
            }
        }
    }
    errs
}

/// Validates a Probe — exactly one handler must be set.
///
/// Mirrors upstream `validateProbe`
/// (`pkg/apis/core/validation/validation.go`, release-1.35).
fn validate_probe(probe: &Probe, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let num_handlers = [
        probe.http_get.is_some(),
        probe.exec.is_some(),
        probe.tcp_socket.is_some(),
        probe.grpc.is_some(),
    ]
    .iter()
    .filter(|&&b| b)
    .count();

    if num_handlers == 0 {
        errs.push(Error::required(fld_path, "must specify a handler type"));
    } else if num_handlers > 1 {
        errs.push(Error::invalid(
            fld_path,
            BadValue::Omit,
            "may not specify more than 1 handler type",
        ));
    }

    errs
}

// BadValue re-exported from field module for internal use in validate_probe.
use crate::validation::field::BadValue;

/// Validates a container's resource requirements: requests <= limits for each
/// named resource. Does NOT parse Quantity strings (deferred — no
/// `IsValidQuantity` port yet); only compares using the existing
/// `Quantity::parse` if both sides parse. If parsing fails the field is
/// still accepted (conservative).
///
/// Mirrors upstream `validateResourceRequirements` / `validateResourceList`
/// (`pkg/apis/core/validation/validation.go`, release-1.35).
fn validate_resource_requirements(
    res: &crate::types::ResourceRequirements,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // Validate that Quantity strings are parseable.
    if let Some(ref limits) = res.limits {
        for (name, qty_str) in limits {
            if crate::quantity::Quantity::parse(qty_str).is_err() {
                errs.push(Error::invalid(
                    &fld_path.child("limits").key(name),
                    qty_str.clone(),
                    "quantities must match the regular expression '[+-]?([0-9]+\\.?[0-9]*|\\.?[0-9]+)[mMkKgGtTpPeE]*'",
                ));
            }
        }
    }
    if let Some(ref requests) = res.requests {
        for (name, qty_str) in requests {
            if crate::quantity::Quantity::parse(qty_str).is_err() {
                errs.push(Error::invalid(
                    &fld_path.child("requests").key(name),
                    qty_str.clone(),
                    "quantities must match the regular expression '[+-]?([0-9]+\\.?[0-9]*|\\.?[0-9]+)[mMkKgGtTpPeE]*'",
                ));
            }
        }
    }

    // requests must not exceed limits for any resource.
    // Upstream: `pkg/apis/core/validation/validation.go validateResourceRequirements`
    if let (Some(ref limits), Some(ref requests)) = (&res.limits, &res.requests) {
        for (name, req_str) in requests {
            if let Some(lim_str) = limits.get(name) {
                let req_q = crate::quantity::Quantity::parse(req_str);
                let lim_q = crate::quantity::Quantity::parse(lim_str);
                if let (Ok(r), Ok(l)) = (req_q, lim_q) {
                    if r.cmp_value(&l) == std::cmp::Ordering::Greater {
                        errs.push(Error::invalid(
                            &fld_path.child("requests").key(name),
                            req_str.clone(),
                            format!("must be less than or equal to {name} limit",),
                        ));
                    }
                }
            }
        }
    }

    errs
}

/// Validates `restartPolicy` — must be one of Always / OnFailure / Never.
/// An absent (None) or empty policy is allowed (the defaulter backfills).
///
/// Mirrors upstream `validateRestartPolicy`
/// (`pkg/apis/core/validation/validation.go`, release-1.35).
fn validate_restart_policy(policy: Option<&str>, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(p) = policy else { return errs };
    if !p.is_empty() && !matches!(p, "Always" | "OnFailure" | "Never") {
        errs.push(Error::not_supported(
            fld_path,
            p.to_string(),
            &["Always", "OnFailure", "Never"],
        ));
    }
    errs
}

/// Validates `dnsPolicy` — must be one of the four known values (or absent).
///
/// Mirrors upstream `validateDNSPolicy`
/// (`pkg/apis/core/validation/validation.go`, release-1.35).
fn validate_dns_policy(policy: Option<&str>, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(p) = policy else { return errs };
    if !matches!(
        p,
        "ClusterFirst" | "ClusterFirstWithHostNet" | "Default" | "None"
    ) {
        errs.push(Error::not_supported(
            fld_path,
            p.to_string(),
            &["ClusterFirst", "ClusterFirstWithHostNet", "Default", "None"],
        ));
    }
    errs
}

/// Validates `spec.tolerations[*]`.
///
/// Rules (mirroring upstream `validateTolerations`,
/// `pkg/apis/core/validation/validation.go`, release-1.35):
/// - `operator` (when set) must be Equal or Exists.
/// - `effect` (when set) must be NoSchedule, PreferNoSchedule, or NoExecute.
/// - When `operator` is Exists and a `value` is set, that is invalid.
/// - When `effect` is NoExecute, `tolerationSeconds` may be set; for other
///   effects it must be absent.
pub fn validate_tolerations(tolerations: &[Toleration], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    for (i, tol) in tolerations.iter().enumerate() {
        let tpath = fld_path.index(i);

        // operator enum.
        if let Some(ref op) = tol.operator {
            match op.as_str() {
                "Equal" => {
                    // value may or may not be set — upstream allows any value.
                }
                "Exists" => {
                    // Exists operator must not have a value.
                    if tol.value.as_ref().is_some_and(|v| !v.is_empty()) {
                        errs.push(Error::invalid(
                            &tpath.child("operator"),
                            op.clone(),
                            "if the operator is 'Exists', the value should be empty",
                        ));
                    }
                }
                other => {
                    errs.push(Error::not_supported(
                        &tpath.child("operator"),
                        other.to_string(),
                        &["Equal", "Exists"],
                    ));
                }
            }
        }

        // effect enum.
        if let Some(ref effect) = tol.effect {
            match effect.as_str() {
                "NoSchedule" | "PreferNoSchedule" => {
                    // tolerationSeconds must be absent for these effects.
                    if let Some(secs) = tol.toleration_seconds {
                        errs.push(Error::invalid(
                            &tpath.child("tolerationSeconds"),
                            secs,
                            "effect must be 'NoExecute' when `tolerationSeconds` is set",
                        ));
                    }
                }
                "NoExecute" => {
                    // tolerationSeconds may be set — any value is valid.
                }
                other => {
                    errs.push(Error::not_supported(
                        &tpath.child("effect"),
                        other.to_string(),
                        &["NoSchedule", "PreferNoSchedule", "NoExecute"],
                    ));
                }
            }
        } else if let Some(secs) = tol.toleration_seconds {
            // No effect but tolerationSeconds set → only valid for NoExecute.
            errs.push(Error::invalid(
                &tpath.child("tolerationSeconds"),
                secs,
                "effect must be 'NoExecute' when `tolerationSeconds` is set",
            ));
        }
    }

    errs
}

// Upstream limits — `pkg/apis/core/validation/validation.go` const block at
// line ~4126 in release-1.35.
pub const MAX_DNS_NAMESERVERS: usize = 3;
pub const MAX_DNS_SEARCH_PATHS: usize = 32;
pub const MAX_DNS_SEARCH_LIST_CHARS: usize = 2048;

/// Mirrors upstream `validatePodDNSConfig`
/// (`pkg/apis/core/validation/validation.go:4156`, release-1.35).
///
/// `allow_relaxed_dns_search_validation` is wired from the
/// `RelaxedDNSSearchValidation` feature gate. When `true`:
/// * the lone `.` domain is accepted verbatim, and
/// * non-`.` entries are validated with the underscore-permissive subdomain
///   regex (`IsDNS1123SubdomainWithUnderScore`).
///
/// When `false`, every entry is trimmed of a trailing `.` (kept for
/// rooted-name compatibility) and then validated with the strict
/// `IsDNS1123Subdomain` regex. This is the pre-1.32 behaviour and is what
/// emulated-version test clusters still exercise.
///
/// The `dns_policy` argument lets us emit the
/// `must provide \`dnsConfig\` when \`dnsPolicy\` is None` parity error.
/// Callers that don't track policy (e.g. PodTemplateSpec validators) may
/// pass `None`.
pub fn validate_pod_dns_config(
    dns_config: Option<&PodDNSConfig>,
    dns_policy: Option<&str>,
    allow_relaxed_dns_search_validation: bool,
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // DNSNone path — must provide a dnsConfig with at least one nameserver.
    if matches!(dns_policy, Some("None")) {
        match dns_config {
            None => {
                errs.push(Error::required(
                    path,
                    "must provide `dnsConfig` when `dnsPolicy` is None",
                ));
                return errs;
            }
            Some(cfg) => {
                let empty: Vec<String> = Vec::new();
                let ns = cfg.nameservers.as_ref().unwrap_or(&empty);
                if ns.is_empty() {
                    errs.push(Error::required(
                        &path.child("nameservers"),
                        "must provide at least one DNS nameserver when `dnsPolicy` is None",
                    ));
                    return errs;
                }
            }
        }
    }

    let Some(cfg) = dns_config else {
        return errs;
    };

    let empty_ns: Vec<String> = Vec::new();
    let nameservers = cfg.nameservers.as_ref().unwrap_or(&empty_ns);
    if nameservers.len() > MAX_DNS_NAMESERVERS {
        errs.push(Error::invalid(
            &path.child("nameservers"),
            nameservers.as_slice(),
            format!("must not have more than {MAX_DNS_NAMESERVERS} nameservers"),
        ));
    }
    // NOTE: upstream additionally runs `IsValidIPForLegacyField` per
    // nameserver. That helper isn't ported to rusternetes yet — DNS IP
    // validation lands with the upstream "legacy IP" port (see
    // pkg/apis/core/validation/validation.go::IsValidIPForLegacyField). The
    // current handler accepts any string here, which preserves prior
    // behaviour and stays orthogonal to this change.

    let empty_searches: Vec<String> = Vec::new();
    let searches = cfg.searches.as_ref().unwrap_or(&empty_searches);
    if searches.len() > MAX_DNS_SEARCH_PATHS {
        errs.push(Error::invalid(
            &path.child("searches"),
            searches.as_slice(),
            format!("must not have more than {MAX_DNS_SEARCH_PATHS} search paths"),
        ));
    }
    // Upstream includes the space between search paths — `strings.Join(..., " ")`.
    let joined_len = if searches.is_empty() {
        0
    } else {
        searches.iter().map(|s| s.len()).sum::<usize>() + (searches.len() - 1)
    };
    if joined_len > MAX_DNS_SEARCH_LIST_CHARS {
        errs.push(Error::invalid(
            &path.child("searches"),
            searches.as_slice(),
            format!(
                "must not have more than {MAX_DNS_SEARCH_LIST_CHARS} characters (including spaces) in the search list"
            ),
        ));
    }

    for (i, search) in searches.iter().enumerate() {
        let search_path = path.child("searches").index(i);
        if allow_relaxed_dns_search_validation {
            // The lone `.` is the canonical "no search" entry and is
            // accepted verbatim under the relaxed gate.
            if search == "." {
                continue;
            }
            let trimmed = search.strip_suffix('.').unwrap_or(search);
            for msg in is_dns1123_subdomain_with_underscore(trimmed) {
                errs.push(Error::invalid(&search_path, search.clone(), msg));
            }
        } else {
            let trimmed = search.strip_suffix('.').unwrap_or(search);
            for msg in is_dns1123_subdomain(trimmed) {
                errs.push(Error::invalid(&search_path, search.clone(), msg));
            }
        }
    }

    if let Some(options) = &cfg.options {
        for (i, option) in options.iter().enumerate() {
            if option.name.is_empty() {
                errs.push(Error::required(
                    &path.child("options").index(i),
                    "must not be empty",
                ));
            }
        }
    }

    errs
}

/// Mirrors upstream `validateOnlyAddedTolerations` (validation.go:5630).
pub fn validate_only_added_tolerations(
    old: &[Toleration],
    new: &[Toleration],
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for ot in old {
        if !new.iter().any(|nt| nt == ot) {
            errs.push(Error::forbidden(
                path,
                "existing tolerations may not be modified or removed",
            ));
            return errs;
        }
    }
    errs
}

/// Mirrors upstream `validateOnlyDeletedSchedulingGates` (validation.go:5651).
pub fn validate_only_deleted_scheduling_gates(
    old: &[PodSchedulingGate],
    new: &[PodSchedulingGate],
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (idx, ng) in new.iter().enumerate() {
        if !old.iter().any(|og| og.name == ng.name) {
            errs.push(Error::forbidden(
                &path.index(idx),
                "only deletion is allowed, but found new scheduling gate",
            ));
        }
    }
    errs
}

/// Mirrors upstream's TerminationGracePeriodSeconds rule
/// (validation.go:5780-5783). Field is immutable, with one relaxation:
/// an old negative value may be replaced by `1` (kubelet legacy).
///
/// A `None` on the `new` side is treated as "unchanged" (partial-update
/// semantics — the client omitted the field, server backfills from old).
pub fn validate_termination_grace_period_immutable(
    old: Option<i64>,
    new: Option<i64>,
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if old == new {
        return errs;
    }
    // Partial update: client omitted the field → backfill from old.
    if new.is_none() {
        return errs;
    }
    // negative → 1 relaxation (matches upstream)
    if let (Some(o), Some(1)) = (old, new) {
        if o < 0 {
            return errs;
        }
    }
    errs.push(Error::invalid(
        path,
        format!("{:?}", new),
        "field is immutable",
    ));
    errs
}

/// Mirrors upstream `validateNodeSelectorMutation` (validation.go:9311-9322).
///
/// Additions to `spec.nodeSelector` are allowed for gated pods. Existing
/// keys may not be deleted or mutated.
pub fn validate_node_selector_mutation(
    path: &Path,
    new_selector: Option<&HashMap<String, String>>,
    old_selector: Option<&HashMap<String, String>>,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let empty = HashMap::new();
    let old = old_selector.unwrap_or(&empty);
    let new = new_selector.unwrap_or(&empty);
    for (k, v1) in old {
        match new.get(k) {
            Some(v2) if v2 == v1 => {}
            _ => {
                errs.push(Error::invalid(
                    path,
                    format!("{:?}", new),
                    "only additions to spec.nodeSelector are allowed (no mutations or deletions)",
                ));
                return errs;
            }
        }
    }
    errs
}

/// Mirrors upstream `validateNodeSelectorTermHasOnlyAdditions`
/// (validation.go:9354-9380). Returns `true` iff `new_term` is `old_term`
/// extended only with additional `matchExpressions` / `matchFields`
/// entries — no deletions or in-place mutations to existing entries.
pub fn validate_node_selector_term_has_only_additions(
    new_term: &NodeSelectorTerm,
    old_term: &NodeSelectorTerm,
) -> bool {
    let old_me = old_term.match_expressions.as_deref().unwrap_or(&[]);
    let old_mf = old_term.match_fields.as_deref().unwrap_or(&[]);
    let new_me = new_term.match_expressions.as_deref().unwrap_or(&[]);
    let new_mf = new_term.match_fields.as_deref().unwrap_or(&[]);

    // If old term was empty, the new term must also be empty (upstream
    // refuses to let an empty term gain any requirements at all, because
    // an empty term matches every node and adding requirements would
    // narrow the match — the gated-pod relaxation is additions only and
    // here "addition" means "more constraints on the same term").
    if old_me.is_empty() && old_mf.is_empty() && (!new_me.is_empty() || !new_mf.is_empty()) {
        return false;
    }

    // matchExpressions: additions only.
    if !old_me.is_empty() {
        if new_me.len() < old_me.len() {
            return false;
        }
        if new_me[..old_me.len()] != *old_me {
            return false;
        }
    }
    // matchFields: additions only.
    if !old_mf.is_empty() {
        if new_mf.len() < old_mf.len() {
            return false;
        }
        if new_mf[..old_mf.len()] != *old_mf {
            return false;
        }
    }
    true
}

/// Mirrors upstream `validateNodeAffinityMutation` (validation.go:9324-9352).
///
/// For gated pods, `spec.affinity.nodeAffinity` may be mutated under these
/// rules:
/// - If `oldNodeAffinity` is nil or has no
///   `requiredDuringSchedulingIgnoredDuringExecution`, anything may be set.
/// - If `requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms`
///   was non-empty in `old`, the new list must have the same length and
///   each term must be an "additions-only" extension of the old term
///   (see [`validate_node_selector_term_has_only_additions`]).
pub fn validate_node_affinity_mutation(
    node_affinity_path: &Path,
    new_node_affinity: Option<&NodeAffinity>,
    old_node_affinity: Option<&NodeAffinity>,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    // If old was nil or had no required-DSIDE, anything goes.
    let old_required = match old_node_affinity {
        Some(na) => na
            .required_during_scheduling_ignored_during_execution
            .as_ref(),
        None => None,
    };
    let Some(old_required) = old_required else {
        return errs;
    };

    let old_terms = &old_required.node_selector_terms;
    let new_terms: &[NodeSelectorTerm] = new_node_affinity
        .and_then(|na| {
            na.required_during_scheduling_ignored_during_execution
                .as_ref()
                .map(|ns| ns.node_selector_terms.as_slice())
        })
        .unwrap_or(&[]);

    let terms_path = node_affinity_path
        .child("requiredDuringSchedulingIgnoredDuringExecution")
        .child("nodeSelectorTerms");

    if !old_terms.is_empty() && old_terms.len() != new_terms.len() {
        errs.push(Error::invalid(
            &terms_path,
            format!("{:?}", new_terms),
            "no additions/deletions to non-empty NodeSelectorTerms list are allowed",
        ));
        return errs;
    }

    for (i, old_term) in old_terms.iter().enumerate() {
        let new_term = &new_terms[i];
        if !validate_node_selector_term_has_only_additions(new_term, old_term) {
            errs.push(Error::invalid(
                &terms_path.index(i),
                format!("{:?}", new_term),
                "only additions are allowed (no mutations or deletions)",
            ));
        }
    }
    errs
}

/// Top-level immutability fence. Composes the four pre-checks above plus a
/// munge+DeepEqual fence that catches any other forbidden field changes.
/// Mirrors `ValidatePodUpdate` (validation.go:5695-5838).
///
/// `is_ephemeral_subresource` controls whether the ephemeral-containers
/// slice is reset in the munged copy — set to `true` when invoked from the
/// `/ephemeralcontainers` subresource path. The dedicated EC add-only
/// check runs upstream of this fence; resetting the field here lets
/// legitimate subresource additions pass the DeepEqual.
pub fn validate_pod_spec_update(
    old: &PodSpec,
    new: &PodSpec,
    is_ephemeral_subresource: bool,
) -> Result<(), String> {
    let spec = Path::new("spec");

    // 1. Container count immutability.
    if old.containers.len() != new.containers.len() {
        return Err("pod updates may not add or remove containers".to_string());
    }

    // 2. Tolerations: additions only.
    let empty_tols: Vec<Toleration> = Vec::new();
    let old_tols = old.tolerations.as_ref().unwrap_or(&empty_tols);
    let new_tols = new.tolerations.as_ref().unwrap_or(&empty_tols);
    let errs = validate_only_added_tolerations(old_tols, new_tols, &spec.child("tolerations"));
    if let Some(e) = errs.first() {
        return Err(e.to_string());
    }

    // 3. SchedulingGates: deletions only.
    let empty_gates: Vec<PodSchedulingGate> = Vec::new();
    let old_gates = old.scheduling_gates.as_ref().unwrap_or(&empty_gates);
    let new_gates = new.scheduling_gates.as_ref().unwrap_or(&empty_gates);
    let errs = validate_only_deleted_scheduling_gates(
        old_gates,
        new_gates,
        &spec.child("schedulingGates"),
    );
    if let Some(e) = errs.first() {
        return Err(e.to_string());
    }

    // 4. TerminationGracePeriodSeconds: immutable except negative→1.
    let errs = validate_termination_grace_period_immutable(
        old.termination_grace_period_seconds,
        new.termination_grace_period_seconds,
        &spec.child("terminationGracePeriodSeconds"),
    );
    if let Some(e) = errs.first() {
        return Err(e.to_string());
    }

    // 5. Munge + DeepEqual fence. Reset every field K8s allows to mutate to
    //    the OLD value, then compare. Any remaining diff = forbidden change.
    let mut munged = new.clone();
    for (i, c) in munged.containers.iter_mut().enumerate() {
        c.image = old.containers[i].image.clone();
    }
    if let (Some(old_init), Some(new_init)) = (&old.init_containers, &mut munged.init_containers) {
        for (i, c) in new_init.iter_mut().enumerate() {
            if i < old_init.len() {
                c.image = old_init[i].image.clone();
            }
        }
    }
    munged.active_deadline_seconds = old.active_deadline_seconds;
    munged.termination_grace_period_seconds = old.termination_grace_period_seconds;
    munged.tolerations = old.tolerations.clone();
    munged.scheduling_gates = old.scheduling_gates.clone();
    if is_ephemeral_subresource {
        munged.ephemeral_containers = old.ephemeral_containers.clone();
    }

    // KEP-3521: gated-pod nodeSelector / nodeAffinity relaxation.
    // Mirrors upstream validation.go:5785-5828. A pod is "gated" iff the
    // OLD spec has at least one entry in spec.schedulingGates — this matches
    // upstream's `podIsGated := len(oldPod.Spec.SchedulingGates) > 0`.
    let pod_is_gated = old.scheduling_gates.as_ref().is_some_and(|g| !g.is_empty());
    if pod_is_gated {
        // Additions to spec.nodeSelector are allowed for gated pods.
        if munged.node_selector != old.node_selector {
            let errs = validate_node_selector_mutation(
                &spec.child("nodeSelector"),
                munged.node_selector.as_ref(),
                old.node_selector.as_ref(),
            );
            if let Some(e) = errs.first() {
                return Err(e.to_string());
            }
            munged.node_selector = old.node_selector.clone();
        }

        // Validate node-affinity mutations.
        let old_node_affinity = old.affinity.as_ref().and_then(|a| a.node_affinity.as_ref());
        let munged_node_affinity = munged
            .affinity
            .as_ref()
            .and_then(|a| a.node_affinity.as_ref());

        let na_equal = serde_json::to_value(old_node_affinity).ok()
            == serde_json::to_value(munged_node_affinity).ok();
        if !na_equal {
            let errs = validate_node_affinity_mutation(
                &spec.child("affinity").child("nodeAffinity"),
                munged_node_affinity,
                old_node_affinity,
            );
            if let Some(e) = errs.first() {
                return Err(e.to_string());
            }
            // Re-munge so the trailing DeepEqual fence ignores this
            // legitimate mutation. Mirrors upstream's four-way switch
            // (validation.go:5807-5821).
            let munged_has_affinity = munged.affinity.is_some();
            let old_pod_has_affinity = old.affinity.is_some();
            let munged_has_other_affinity = munged
                .affinity
                .as_ref()
                .is_some_and(|a| a.pod_affinity.is_some() || a.pod_anti_affinity.is_some());

            if !munged_has_affinity && old_node_affinity.is_none() {
                // already effectively nil, no change needed
            } else if !munged_has_affinity && old_node_affinity.is_some() {
                munged.affinity = Some(crate::resources::pod::Affinity {
                    node_affinity: old_node_affinity.cloned(),
                    pod_affinity: None,
                    pod_anti_affinity: None,
                });
            } else if munged_has_affinity && !old_pod_has_affinity && !munged_has_other_affinity {
                // The mutation introduced only a NodeAffinity, and old had
                // no Affinity at all — drop munged.affinity entirely so
                // the DeepEqual matches.
                munged.affinity = None;
            } else if let Some(a) = munged.affinity.as_mut() {
                a.node_affinity = old_node_affinity.cloned();
            }
        }
    }

    let mut munged_json = serde_json::to_value(&munged).unwrap_or_default();
    let mut old_json = serde_json::to_value(old).unwrap_or_default();
    // Backfill null/missing fields in `new` with the corresponding value
    // from `old` before DeepEqual. Implements partial-update semantics
    // matching K8s' defaulting + admission pipeline re-running on every
    // request (so a client may omit server-managed fields).
    fill_nulls_from(&mut munged_json, &old_json);
    // Normalize empty `{}` objects to absent on both sides. This mirrors
    // Go's `apiequality.Semantic.DeepEqual` behaviour, which treats a
    // zero-valued struct as equal to a nil pointer. Without this, a
    // round-trip through Go's typed `corev1.Pod` (which marshals
    // `Resources ResourceRequirements` with `omitempty` but still emits
    // `"resources":{}` because Go's `omitempty` does not detect empty
    // structs) would falsely trip the immutability fence even on an
    // empty client-side update.
    strip_empty_objects(&mut munged_json);
    strip_empty_objects(&mut old_json);
    if munged_json != old_json {
        return Err("pod updates may not change fields other than \
             `spec.containers[*].image`, `spec.initContainers[*].image`, \
             `spec.activeDeadlineSeconds`, `spec.terminationGracePeriodSeconds`, \
             `spec.tolerations` (additions only), `spec.schedulingGates` (deletions only)"
            .to_string());
    }

    Ok(())
}

/// Recursively remove empty `{}` objects from a JSON value tree. Mirrors
/// Go's `apiequality.Semantic.DeepEqual` treatment of zero-valued structs
/// as equal to `nil` pointers. Without this, an empty `"resources":{}`
/// emitted by a Go client (because `omitempty` on a struct value type
/// does not detect zero-valued structs) would falsely trip our diff
/// fence on an empty update.
///
/// Applies post-order so a key whose value becomes empty after recursing
/// is itself stripped (e.g. `{"a":{"b":{}}}` → `{}`).
///
/// Exported for shared use by other DeepEqual-style spec comparators
/// (notably `crates/api-server/src/handlers/lifecycle.rs::
/// maybe_increment_generation`).
pub fn strip_empty_objects(v: &mut serde_json::Value) {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                if let Some(child) = map.get_mut(&k) {
                    strip_empty_objects(child);
                    let drop = match child {
                        Value::Object(m) => m.is_empty(),
                        Value::Null => true,
                        _ => false,
                    };
                    if drop {
                        map.remove(&k);
                    }
                }
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                strip_empty_objects(child);
            }
        }
        _ => {}
    }
}

/// Recursively backfill `null`/missing keys in `dst` with the corresponding
/// value from `src`. Arrays are merged element-wise only when both sides
/// have equal length.
fn fill_nulls_from(dst: &mut serde_json::Value, src: &serde_json::Value) {
    use serde_json::Value;
    match (dst, src) {
        (Value::Object(dst_map), Value::Object(src_map)) => {
            for (k, src_v) in src_map {
                match dst_map.get_mut(k) {
                    None => {
                        dst_map.insert(k.clone(), src_v.clone());
                    }
                    Some(dst_v) if dst_v.is_null() => {
                        *dst_v = src_v.clone();
                    }
                    Some(dst_v) => fill_nulls_from(dst_v, src_v),
                }
            }
        }
        (Value::Array(dst_arr), Value::Array(src_arr)) if dst_arr.len() == src_arr.len() => {
            for (d, s) in dst_arr.iter_mut().zip(src_arr.iter()) {
                fill_nulls_from(d, s);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::pod::Sysctl;

    #[test]
    fn sysctl_name_validity_matches_upstream() {
        // Valid: dot- and slash-separated lowercase-alnum segments, inner -/_.
        assert!(is_valid_sysctl_name("kernel.shmmax"));
        assert!(is_valid_sysctl_name("kernel/shm_rmid_forced")); // slash separator
        assert!(is_valid_sysctl_name("safe-and-unsafe")); // single segment, inner '-'
        assert!(is_valid_sysctl_name("net.ipv4.tcp_keepalive_time"));

        // Invalid: trailing '-', empty segments, leading/trailing separators.
        assert!(!is_valid_sysctl_name("foo-"));
        assert!(!is_valid_sysctl_name("bar.."));
        assert!(!is_valid_sysctl_name(""));
        assert!(!is_valid_sysctl_name(".leading"));
        assert!(!is_valid_sysctl_name("trailing."));
        assert!(!is_valid_sysctl_name("Upper.Case"));
        assert!(!is_valid_sysctl_name(&"a".repeat(SYSCTL_MAX_LENGTH + 1)));
    }

    fn sc_with_sysctls(names: &[&str]) -> PodSecurityContext {
        PodSecurityContext {
            sysctls: Some(
                names
                    .iter()
                    .map(|n| Sysctl {
                        name: n.to_string(),
                        value: "1".to_string(),
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn validate_sysctls_rejects_invalid_names_only() {
        // Mirrors the [sig-node] "should reject invalid sysctls" conformance
        // case: foo- and bar.. are rejected; kernel.shmmax and safe-and-unsafe
        // are accepted.
        let sc = sc_with_sysctls(&["foo-", "kernel.shmmax", "safe-and-unsafe", "bar.."]);
        let errs = validate_sysctls(
            &sc,
            &Path::new("spec").child("securityContext").child("sysctls"),
        );
        let bad: Vec<&str> = errs
            .iter()
            .filter_map(|e| match &e.bad_value {
                crate::validation::field::BadValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        assert!(bad.contains(&"foo-"), "foo- must be rejected: {errs:?}");
        assert!(bad.contains(&"bar.."), "bar.. must be rejected: {errs:?}");
        assert!(!bad.contains(&"kernel.shmmax"));
        assert!(!bad.contains(&"safe-and-unsafe"));
        assert_eq!(errs.len(), 2, "exactly the two invalid names: {errs:?}");
    }

    #[test]
    fn validate_sysctls_flags_duplicates_and_empty() {
        let sc = sc_with_sysctls(&["kernel.shmmax", "kernel.shmmax", ""]);
        let errs = validate_sysctls(
            &sc,
            &Path::new("spec").child("securityContext").child("sysctls"),
        );
        assert_eq!(errs.len(), 2, "one duplicate + one required: {errs:?}");
    }

    fn t(key: &str) -> Toleration {
        Toleration {
            key: Some(key.to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        }
    }

    fn g(name: &str) -> PodSchedulingGate {
        PodSchedulingGate {
            name: name.to_string(),
        }
    }

    #[test]
    fn tolerations_additions_only_allows_add() {
        let p = Path::new("spec").child("tolerations");
        let old = vec![t("a")];
        let new = vec![t("a"), t("b")];
        assert!(validate_only_added_tolerations(&old, &new, &p).is_empty());
    }

    #[test]
    fn tolerations_additions_only_rejects_remove() {
        let p = Path::new("spec").child("tolerations");
        let old = vec![t("a"), t("b")];
        let new = vec![t("a")];
        let errs = validate_only_added_tolerations(&old, &new, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("existing tolerations may not be modified or removed"));
    }

    #[test]
    fn gates_deletions_only_allows_remove() {
        let p = Path::new("spec").child("schedulingGates");
        let old = vec![g("a"), g("b")];
        let new = vec![g("a")];
        assert!(validate_only_deleted_scheduling_gates(&old, &new, &p).is_empty());
    }

    #[test]
    fn gates_deletions_only_rejects_add() {
        let p = Path::new("spec").child("schedulingGates");
        let old = vec![g("a")];
        let new = vec![g("a"), g("b")];
        let errs = validate_only_deleted_scheduling_gates(&old, &new, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("only deletion is allowed"));
    }

    #[test]
    fn tgps_unchanged_is_allowed() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        assert!(validate_termination_grace_period_immutable(Some(30), Some(30), &p).is_empty());
        assert!(validate_termination_grace_period_immutable(None, None, &p).is_empty());
    }

    #[test]
    fn tgps_negative_to_one_is_allowed() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        assert!(validate_termination_grace_period_immutable(Some(-5), Some(1), &p).is_empty());
    }

    #[test]
    fn tgps_arbitrary_change_is_rejected() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        let errs = validate_termination_grace_period_immutable(Some(30), Some(60), &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("field is immutable"));
    }

    fn dns_cfg(searches: &[&str]) -> PodDNSConfig {
        PodDNSConfig {
            nameservers: None,
            searches: Some(searches.iter().map(|s| s.to_string()).collect()),
            options: None,
        }
    }

    #[test]
    fn dns_search_underscore_rejected_when_gate_off() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = dns_cfg(&["_sip._tcp.abc_d.example.com"]);
        let errs = validate_pod_dns_config(Some(&cfg), None, false, &p);
        assert!(
            !errs.is_empty(),
            "underscore search must be rejected with relaxed gate disabled"
        );
        assert!(errs[0].to_string().contains("spec.dnsConfig.searches[0]"));
    }

    #[test]
    fn dns_search_underscore_accepted_when_gate_on() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = dns_cfg(&["_sip._tcp.abc_d.example.com"]);
        let errs = validate_pod_dns_config(Some(&cfg), None, true, &p);
        assert!(
            errs.is_empty(),
            "underscore search must be accepted with relaxed gate enabled: {:?}",
            errs
        );
    }

    #[test]
    fn dns_search_lone_dot_rejected_when_gate_off() {
        let p = Path::new("spec").child("dnsConfig");
        // Strict mode trims trailing `.` and then runs IsDNS1123Subdomain on
        // the empty string, which the upstream regex rejects.
        let cfg = dns_cfg(&["."]);
        let errs = validate_pod_dns_config(Some(&cfg), None, false, &p);
        assert!(
            !errs.is_empty(),
            "lone-dot search must be rejected with relaxed gate disabled"
        );
    }

    #[test]
    fn dns_search_lone_dot_accepted_when_gate_on() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = dns_cfg(&["."]);
        let errs = validate_pod_dns_config(Some(&cfg), None, true, &p);
        assert!(
            errs.is_empty(),
            "lone-dot search must be accepted with relaxed gate enabled: {:?}",
            errs
        );
    }

    #[test]
    fn dns_search_plain_subdomain_accepted_both_modes() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = dns_cfg(&["example.com"]);
        assert!(validate_pod_dns_config(Some(&cfg), None, true, &p).is_empty());
        assert!(validate_pod_dns_config(Some(&cfg), None, false, &p).is_empty());
    }

    #[test]
    fn dns_policy_none_without_config_is_rejected() {
        let p = Path::new("spec").child("dnsConfig");
        let errs = validate_pod_dns_config(None, Some("None"), true, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("must provide `dnsConfig` when `dnsPolicy` is None"));
    }

    #[test]
    fn dns_policy_none_with_empty_nameservers_is_rejected() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = PodDNSConfig {
            nameservers: Some(vec![]),
            searches: None,
            options: None,
        };
        let errs = validate_pod_dns_config(Some(&cfg), Some("None"), true, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("must provide at least one DNS nameserver"));
    }

    #[test]
    fn dns_search_too_many_paths_rejected() {
        let p = Path::new("spec").child("dnsConfig");
        let too_many: Vec<&str> = std::iter::repeat_n("a.com", MAX_DNS_SEARCH_PATHS + 1).collect();
        let cfg = dns_cfg(&too_many);
        let errs = validate_pod_dns_config(Some(&cfg), None, true, &p);
        assert!(errs.iter().any(|e| e
            .to_string()
            .contains("must not have more than 32 search paths")));
    }

    #[test]
    fn dns_option_empty_name_rejected() {
        let p = Path::new("spec").child("dnsConfig");
        let cfg = PodDNSConfig {
            nameservers: None,
            searches: None,
            options: Some(vec![crate::resources::pod::PodDNSConfigOption {
                name: String::new(),
                value: None,
            }]),
        };
        let errs = validate_pod_dns_config(Some(&cfg), None, true, &p);
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("must not be empty")),
            "got: {:?}",
            errs
        );
    }

    #[test]
    fn tgps_positive_to_nil_treated_as_partial_update() {
        // Partial-update semantics: client omitted the field, so the server
        // backfills from old. The fence treats `None` on the new side as
        // "unchanged", matching K8s' defaulting + admission re-run on each
        // request.
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        let errs = validate_termination_grace_period_immutable(Some(30), None, &p);
        assert!(errs.is_empty(), "client-omitted TGPS must not be rejected");
    }

    // ----- gated-pod nodeSelector / nodeAffinity relaxation (KEP-3521) -----

    use crate::resources::pod::{
        NodeAffinity as NA, NodeSelector as NS, NodeSelectorRequirement as NSR,
        NodeSelectorTerm as NST,
    };

    fn req(key: &str, op: &str, vals: &[&str]) -> NSR {
        NSR {
            key: key.to_string(),
            operator: op.to_string(),
            values: if vals.is_empty() {
                None
            } else {
                Some(vals.iter().map(|s| s.to_string()).collect())
            },
        }
    }

    fn term(me: Vec<NSR>) -> NST {
        NST {
            match_expressions: if me.is_empty() { None } else { Some(me) },
            match_fields: None,
        }
    }

    fn na_required(terms: Vec<NST>) -> NA {
        NA {
            required_during_scheduling_ignored_during_execution: Some(NS {
                node_selector_terms: terms,
            }),
            preferred_during_scheduling_ignored_during_execution: None,
        }
    }

    #[test]
    fn node_selector_addition_is_allowed() {
        let p = Path::new("spec").child("nodeSelector");
        let mut old = HashMap::new();
        old.insert("foo".to_string(), "bar".to_string());
        let mut new = old.clone();
        new.insert("baz".to_string(), "qux".to_string());
        assert!(validate_node_selector_mutation(&p, Some(&new), Some(&old)).is_empty());
    }

    #[test]
    fn node_selector_addition_from_empty_is_allowed() {
        let p = Path::new("spec").child("nodeSelector");
        let mut new = HashMap::new();
        new.insert("foo".to_string(), "bar".to_string());
        assert!(validate_node_selector_mutation(&p, Some(&new), None).is_empty());
    }

    #[test]
    fn node_selector_deletion_is_rejected() {
        let p = Path::new("spec").child("nodeSelector");
        let mut old = HashMap::new();
        old.insert("foo".to_string(), "bar".to_string());
        old.insert("a".to_string(), "b".to_string());
        let mut new = HashMap::new();
        new.insert("foo".to_string(), "bar".to_string());
        let errs = validate_node_selector_mutation(&p, Some(&new), Some(&old));
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("only additions to spec.nodeSelector are allowed"));
    }

    #[test]
    fn node_selector_value_mutation_is_rejected() {
        let p = Path::new("spec").child("nodeSelector");
        let mut old = HashMap::new();
        old.insert("foo".to_string(), "bar".to_string());
        let mut new = HashMap::new();
        new.insert("foo".to_string(), "baz".to_string());
        let errs = validate_node_selector_mutation(&p, Some(&new), Some(&old));
        assert_eq!(errs.len(), 1);
    }

    #[test]
    fn node_affinity_nil_old_allows_anything() {
        let p = Path::new("spec").child("affinity").child("nodeAffinity");
        let new = na_required(vec![term(vec![req("a", "In", &["1"])])]);
        assert!(validate_node_affinity_mutation(&p, Some(&new), None).is_empty());
    }

    #[test]
    fn node_affinity_term_addition_inside_term_is_allowed() {
        // Same number of terms; existing matchExpressions preserved as prefix.
        let p = Path::new("spec").child("affinity").child("nodeAffinity");
        let old = na_required(vec![term(vec![req("a", "In", &["1"])])]);
        let new = na_required(vec![term(vec![
            req("a", "In", &["1"]),
            req("b", "In", &["2"]),
        ])]);
        assert!(validate_node_affinity_mutation(&p, Some(&new), Some(&old)).is_empty());
    }

    #[test]
    fn node_affinity_term_count_change_is_rejected() {
        let p = Path::new("spec").child("affinity").child("nodeAffinity");
        let old = na_required(vec![term(vec![req("a", "In", &["1"])])]);
        let new = na_required(vec![
            term(vec![req("a", "In", &["1"])]),
            term(vec![req("b", "In", &["2"])]),
        ]);
        let errs = validate_node_affinity_mutation(&p, Some(&new), Some(&old));
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("no additions/deletions to non-empty NodeSelectorTerms list"));
    }

    #[test]
    fn node_affinity_existing_expression_mutation_is_rejected() {
        let p = Path::new("spec").child("affinity").child("nodeAffinity");
        let old = na_required(vec![term(vec![req("a", "In", &["1"])])]);
        let new = na_required(vec![term(vec![req("a", "In", &["2"])])]);
        let errs = validate_node_affinity_mutation(&p, Some(&new), Some(&old));
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("only additions are allowed"));
    }

    fn mounts_from(json: serde_json::Value) -> Vec<VolumeMount> {
        serde_json::from_value(json).unwrap()
    }

    fn bare_container() -> Container {
        serde_json::from_value(serde_json::json!({"name": "c", "image": "busybox"})).unwrap()
    }

    fn container_from(json: serde_json::Value) -> Container {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn volume_mount_must_reference_declared_volume() {
        let vols: HashSet<&str> = ["data"].into_iter().collect();
        let mounts = mounts_from(serde_json::json!([
            {"name": "data", "mountPath": "/data"},
            {"name": "missing", "mountPath": "/other"}
        ]));
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].to_string().contains("missing"));
    }

    #[test]
    fn duplicate_mount_path_rejected() {
        let vols: HashSet<&str> = ["a", "b"].into_iter().collect();
        let mounts = mounts_from(serde_json::json!([
            {"name": "a", "mountPath": "/x"},
            {"name": "b", "mountPath": "/x"}
        ]));
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("must be unique")),
            "{errs:?}"
        );
    }

    #[test]
    fn empty_name_and_mount_path_required() {
        let vols: HashSet<&str> = HashSet::new();
        let mounts = mounts_from(serde_json::json!([{"name": "", "mountPath": ""}]));
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(errs.iter().any(|e| e.field.ends_with("name")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field.ends_with("mountPath")),
            "{errs:?}"
        );
    }

    #[test]
    fn subpath_must_be_relative_and_no_backsteps() {
        assert!(validate_local_descending_path("data/sub", &Path::new("subPath")).is_empty());
        let abs = validate_local_descending_path("/etc", &Path::new("subPath"));
        assert!(
            abs.iter().any(|e| e.to_string().contains("relative path")),
            "{abs:?}"
        );
        let back = validate_local_descending_path("../../etc", &Path::new("subPath"));
        assert!(
            back.iter()
                .any(|e| e.to_string().contains("must not contain '..'")),
            "{back:?}"
        );
    }

    #[test]
    fn subpath_and_subpathexpr_mutually_exclusive() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        let mounts = mounts_from(serde_json::json!([
            {"name": "v", "mountPath": "/m", "subPath": "a", "subPathExpr": "$(POD_NAME)"}
        ]));
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("mutually exclusive")),
            "{errs:?}"
        );
    }

    #[test]
    fn bidirectional_propagation_requires_privileged() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        let mounts = mounts_from(
            serde_json::json!([{"name": "v", "mountPath": "/m", "mountPropagation": "Bidirectional"}]),
        );
        // Non-privileged → forbidden.
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("privileged containers")),
            "{errs:?}"
        );
        // Privileged → allowed.
        let priv_c = container_from(
            serde_json::json!({"name": "c", "image": "busybox", "securityContext": {"privileged": true}}),
        );
        assert!(
            validate_volume_mounts(&mounts, &vols, &priv_c, &Path::new("volumeMounts")).is_empty()
        );
    }

    #[test]
    fn unsupported_mount_propagation_rejected() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        let mounts = mounts_from(
            serde_json::json!([{"name": "v", "mountPath": "/m", "mountPropagation": "Sideways"}]),
        );
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter().any(|e| e.field.ends_with("mountPropagation")),
            "{errs:?}"
        );
    }

    #[test]
    fn recursive_read_only_requires_read_only() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        // Enabled without readOnly → forbidden.
        let mounts = mounts_from(
            serde_json::json!([{"name": "v", "mountPath": "/m", "recursiveReadOnly": "Enabled"}]),
        );
        let errs = validate_volume_mounts(
            &mounts,
            &vols,
            &bare_container(),
            &Path::new("volumeMounts"),
        );
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("readOnly is true")),
            "{errs:?}"
        );
        // Enabled with readOnly → ok.
        let ok = mounts_from(
            serde_json::json!([{"name": "v", "mountPath": "/m", "readOnly": true, "recursiveReadOnly": "Enabled"}]),
        );
        assert!(
            validate_volume_mounts(&ok, &vols, &bare_container(), &Path::new("volumeMounts"))
                .is_empty()
        );
    }

    #[test]
    fn mount_overlapping_volume_device_rejected() {
        let vols: HashSet<&str> = ["v"].into_iter().collect();
        let c = container_from(serde_json::json!({
            "name": "c", "image": "busybox",
            "volumeDevices": [{"name": "v", "devicePath": "/dev/xvda"}]
        }));
        let mounts = mounts_from(serde_json::json!([{"name": "v", "mountPath": "/dev/xvda"}]));
        let errs = validate_volume_mounts(&mounts, &vols, &c, &Path::new("volumeMounts"));
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("already exist in volumeDevices")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("already exist as a path in volumeDevices")),
            "{errs:?}"
        );
    }

    #[test]
    fn env_name_must_be_valid() {
        let env: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "MY_ENV.name-1", "value": "ok"},
            {"name": "1bad", "value": "x"},
            {"name": "", "value": "y"}
        ]))
        .unwrap();
        let errs = validate_env(&env, &Path::new("env"));
        assert!(
            errs.iter().any(|e| e.field.ends_with("[1].name")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e.field.ends_with("[2].name")),
            "{errs:?}"
        );
        // index 0 is valid.
        assert!(!errs.iter().any(|e| e.field.contains("[0]")), "{errs:?}");
    }

    #[test]
    fn env_chdir_prefix_rejected() {
        assert!(is_env_var_name("..").iter().any(|m| m.contains("'..'")));
        assert!(is_env_var_name("..foo")
            .iter()
            .any(|m| m.contains("start with '..'")));
        assert!(is_env_var_name(".").iter().any(|m| m.contains("'.'")));
        assert!(is_env_var_name("GOOD.name-1").is_empty());
    }

    #[test]
    fn env_value_and_valuefrom_mutually_exclusive() {
        let env: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "A", "value": "v", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": "k"}}}
        ])).unwrap();
        let errs = validate_env(&env, &Path::new("env"));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not be specified when `value` is not empty")),
            "{errs:?}"
        );
    }

    #[test]
    fn env_valuefrom_requires_exactly_one_source() {
        // zero sources
        let none: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "A", "valueFrom": {}}
        ]))
        .unwrap();
        assert!(validate_env(&none, &Path::new("env"))
            .iter()
            .any(|e| e.to_string().contains("must specify one of")));
        // two sources
        let two: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "A", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": "k"}, "secretKeyRef": {"name": "s", "key": "k"}}}
        ])).unwrap();
        assert!(validate_env(&two, &Path::new("env"))
            .iter()
            .any(|e| e.to_string().contains("more than one field")));
    }

    #[test]
    fn env_configmap_keyref_requires_key() {
        let env: Vec<EnvVar> = serde_json::from_value(serde_json::json!([
            {"name": "A", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": ""}}}
        ]))
        .unwrap();
        let errs = validate_env(&env, &Path::new("env"));
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("configMapKeyRef.key")),
            "{errs:?}"
        );
    }

    fn cerrs(json: serde_json::Value) -> Vec<String> {
        let vols: HashSet<&str> = HashSet::new();
        validate_container(
            &container_from(json),
            false,
            &vols,
            &Path::new("spec").child("containers").index(0),
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    fn tsc_errs(json: serde_json::Value) -> Vec<String> {
        let tscs: Vec<TopologySpreadConstraint> = serde_json::from_value(json).unwrap();
        validate_topology_spread_constraints(
            &tscs,
            &Path::new("spec").child("topologySpreadConstraints"),
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    #[test]
    fn image_pull_policy_enum() {
        assert!(cerrs(
            serde_json::json!({"name": "c", "image": "i", "imagePullPolicy": "Sometimes"})
        )
        .iter()
        .any(|e| e.contains("imagePullPolicy")));
        // valid + unset both pass.
        assert!(!cerrs(
            serde_json::json!({"name": "c", "image": "i", "imagePullPolicy": "IfNotPresent"})
        )
        .iter()
        .any(|e| e.contains("imagePullPolicy")));
        assert!(!cerrs(serde_json::json!({"name": "c", "image": "i"}))
            .iter()
            .any(|e| e.contains("imagePullPolicy")));
    }

    #[test]
    fn termination_message_policy_enum() {
        assert!(cerrs(
            serde_json::json!({"name": "c", "image": "i", "terminationMessagePolicy": "Bogus"})
        )
        .iter()
        .any(|e| e.contains("terminationMessagePolicy")));
        assert!(!cerrs(serde_json::json!({"name": "c", "image": "i", "terminationMessagePolicy": "FallbackToLogsOnError"}))
            .iter().any(|e| e.contains("terminationMessagePolicy")));
    }

    #[test]
    fn lifecycle_handler_requires_exactly_one() {
        // empty handler -> required
        let none = cerrs(serde_json::json!({
            "name": "c", "image": "i", "lifecycle": {"preStop": {}}
        }));
        assert!(
            none.iter()
                .any(|e| e.contains("must specify a handler type")),
            "{none:?}"
        );
        // two handlers -> forbidden
        let two = cerrs(serde_json::json!({
            "name": "c", "image": "i",
            "lifecycle": {"postStart": {"exec": {"command": ["x"]}, "sleep": {"seconds": 1}}}
        }));
        assert!(
            two.iter()
                .any(|e| e.contains("may not specify more than 1 handler type")),
            "{two:?}"
        );
        // single handler -> ok
        let ok = cerrs(serde_json::json!({
            "name": "c", "image": "i", "lifecycle": {"preStop": {"exec": {"command": ["x"]}}}
        }));
        assert!(!ok.iter().any(|e| e.contains("handler type")), "{ok:?}");
    }

    #[test]
    fn topology_spread_valid_passes() {
        assert!(tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule",
             "labelSelector": {"matchLabels": {"app": "x"}}}
        ]))
        .is_empty());
    }

    #[test]
    fn topology_spread_field_checks() {
        let e = tsc_errs(serde_json::json!([
            {"maxSkew": 0, "topologyKey": "", "whenUnsatisfiable": "Maybe"}
        ]));
        assert!(e.iter().any(|m| m.contains("maxSkew")), "{e:?}");
        assert!(e.iter().any(|m| m.contains("topologyKey")), "{e:?}");
        assert!(e.iter().any(|m| m.contains("whenUnsatisfiable")), "{e:?}");
    }

    #[test]
    fn topology_spread_duplicate_tuple_rejected() {
        let e = tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule"},
            {"maxSkew": 2, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule"}
        ]));
        assert!(
            e.iter()
                .any(|m| m.contains("topologyKey, whenUnsatisfiable")),
            "{e:?}"
        );
    }

    #[test]
    fn topology_spread_min_domains_requires_donotschedule() {
        let e = tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "ScheduleAnyway", "minDomains": 2}
        ]));
        assert!(
            e.iter()
                .any(|m| m.contains("can only use minDomains if whenUnsatisfiable=DoNotSchedule")),
            "{e:?}"
        );
    }

    #[test]
    fn topology_spread_node_policy_and_match_label_keys() {
        let bad_policy = tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "z", "whenUnsatisfiable": "DoNotSchedule", "nodeAffinityPolicy": "Bogus"}
        ]));
        assert!(
            bad_policy.iter().any(|m| m.contains("nodeAffinityPolicy")),
            "{bad_policy:?}"
        );
        // matchLabelKeys overlapping the selector is rejected.
        let overlap = tsc_errs(serde_json::json!([
            {"maxSkew": 1, "topologyKey": "z", "whenUnsatisfiable": "DoNotSchedule",
             "labelSelector": {"matchLabels": {"app": "x"}}, "matchLabelKeys": ["app"]}
        ]));
        assert!(
            overlap
                .iter()
                .any(|m| m.contains("exists in both matchLabelKeys and labelSelector")),
            "{overlap:?}"
        );
    }

    fn env_errs(json: serde_json::Value) -> Vec<String> {
        let env: Vec<EnvVar> = serde_json::from_value(json).unwrap();
        validate_env(&env, &Path::new("env"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn fieldref_allowlist_and_subscripts() {
        // allowed path passes
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.name"}}}
        ])).is_empty());
        // disallowed path -> NotSupported
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "spec.containers"}}}
        ])).iter().any(|e| e.contains("fieldPath")));
        // annotations subscript with a valid key passes
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.annotations['my.key/x']"}}}
        ])).is_empty());
        // subscript on a non-subscriptable path -> rejected
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.name['x']"}}}
        ])).iter().any(|e| e.contains("does not support subscript")));
    }

    #[test]
    fn fieldref_requires_apiversion() {
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}
        ]))
        .iter()
        .any(|e| e.contains("apiVersion")));
    }

    #[test]
    fn configmap_keyref_name_and_key_format() {
        // invalid name (uppercase not a DNS subdomain) + invalid key char
        let e = env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"configMapKeyRef": {"name": "Bad_Name", "key": "good.key"}}}
        ]));
        assert!(e.iter().any(|m| m.contains("name")), "{e:?}");
        // valid passes
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": "good.key-1"}}}
        ]))
        .is_empty());
        // backstep key rejected
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"secretKeyRef": {"name": "s", "key": ".."}}}
        ]))
        .iter()
        .any(|m| m.contains("'..'")));
    }

    #[test]
    fn resourcefieldref_divisor_must_be_quantity() {
        assert!(env_errs(serde_json::json!([
            {"name": "A", "valueFrom": {"resourceFieldRef": {"resource": "limits.cpu", "divisor": "notaqty"}}}
        ])).iter().any(|m| m.contains("divisor")));
    }
}
