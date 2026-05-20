//! Pod update validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePodUpdate` (release-1.35).
//!
//! Composes the four upstream pre-checks (container count, tolerations
//! additions-only, schedulingGates deletions-only, terminationGracePeriodSeconds
//! immutability with the negative→1 relaxation) and a munge+DeepEqual fence
//! that catches everything else.
//!
//! NOT covered (intentionally deferred):
//! - Gated-pod `nodeSelector` / `nodeAffinity` mutation rules
//!   (`validation.go:5786-5828`) — only relevant once rusternetes ships a
//!   real scheduling-gates feature. The broad fence below is strictly more
//!   conservative (no gated-pod mutations are permitted at all).
//! - ActiveDeadlineSeconds precise semantics — the api-server handler
//!   enforces these directly (see `crates/api-server/src/handlers/pod.rs`)
//!   because the error wording is checked by tests pinned at that layer.

use crate::resources::pod::{PodSchedulingGate, PodSpec, Toleration};
use crate::validation::field::{Error, ErrorList, Path};

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
    let old_json = serde_json::to_value(old).unwrap_or_default();
    // Backfill null/missing fields in `new` with the corresponding value
    // from `old` before DeepEqual. Implements partial-update semantics
    // matching K8s' defaulting + admission pipeline re-running on every
    // request (so a client may omit server-managed fields).
    fill_nulls_from(&mut munged_json, &old_json);
    if munged_json != old_json {
        return Err("pod updates may not change fields other than \
             `spec.containers[*].image`, `spec.initContainers[*].image`, \
             `spec.activeDeadlineSeconds`, `spec.terminationGracePeriodSeconds`, \
             `spec.tolerations` (additions only), `spec.schedulingGates` (deletions only)"
            .to_string());
    }

    Ok(())
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
