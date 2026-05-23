//! Pod validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go` (release-1.35).
//!
//! Two layers:
//! * **Create-side** ([`validate_pod_dns_config`]) — the per-field validators
//!   the upstream `ValidatePodSpec` pipeline runs on a brand-new Pod. The
//!   first such validator ported here is `validatePodDNSConfig`, gated by
//!   the `RelaxedDNSSearchValidation` feature flag.
//! * **Update-side** ([`validate_pod_spec_update`]) — composes the four
//!   upstream pre-checks (container count, tolerations additions-only,
//!   schedulingGates deletions-only, terminationGracePeriodSeconds
//!   immutability with the negative→1 relaxation) and a munge+DeepEqual
//!   fence that catches everything else.
//!
//! NOT covered (intentionally deferred):
//! - Gated-pod `nodeSelector` / `nodeAffinity` mutation rules
//!   (`validation.go:5786-5828`) — only relevant once rusternetes ships a
//!   real scheduling-gates feature. The broad fence below is strictly more
//!   conservative (no gated-pod mutations are permitted at all).
//! - ActiveDeadlineSeconds precise semantics — the api-server handler
//!   enforces these directly (see `crates/api-server/src/handlers/pod.rs`)
//!   because the error wording is checked by tests pinned at that layer.

use crate::resources::pod::{PodDNSConfig, PodSchedulingGate, PodSpec, Toleration};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_subdomain, is_dns1123_subdomain_with_underscore};

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
}
