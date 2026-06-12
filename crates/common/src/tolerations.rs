//! Default-toleration admission helper shared by the api-server (HTTP pod-create
//! admission) and the controller-manager (controllers that create pods by
//! writing directly to storage, bypassing the HTTP admission chain).
//!
//! K8s ref: `plugin/pkg/admission/defaulttolerationseconds/admission.go`
//! (release-1.35). That plugin appends, to every pod that does not already
//! tolerate them, a `node.kubernetes.io/not-ready:NoExecute` toleration and a
//! `node.kubernetes.io/unreachable:NoExecute` toleration, each with
//! `tolerationSeconds: 300` (the `defaultNotReadyTolerationSeconds` /
//! `defaultUnreachableTolerationSeconds` defaults at admission.go:44-45).
//!
//! Without this, rusternetes pods carry NO toleration for the NotReady taint
//! the node controller adds on a NotReady node, so a single transient node
//! flap under CI load lets the kubelet's NoExecute sweep evict (and ultimately
//! delete) every pod on the node instantly — including Succeeded pods whose
//! status conformance tests still need to observe. See #442.

use crate::resources::{PodSpec, Toleration};

/// Taint key added to a node that is not ready (`v1.TaintNodeNotReady`).
pub const TAINT_NODE_NOT_READY: &str = "node.kubernetes.io/not-ready";
/// Taint key added to a node that is unreachable (`v1.TaintNodeUnreachable`).
pub const TAINT_NODE_UNREACHABLE: &str = "node.kubernetes.io/unreachable";
/// Default toleration grace period, in seconds, for both default tolerations.
pub const DEFAULT_TOLERATION_SECONDS: i64 = 300;

const EFFECT_NO_EXECUTE: &str = "NoExecute";
const OP_EXISTS: &str = "Exists";

/// Mirror upstream `Plugin.Admit`'s "already tolerates" test
/// (admission.go:144-153): a pod already tolerates `taint_key:NoExecute` if any
/// existing toleration has (key == taint_key OR an empty key) AND (effect
/// NoExecute OR an empty effect). The operator/value are intentionally NOT part
/// of this gate — matching upstream, which only inspects key+effect here.
fn already_tolerates(tolerations: &[Toleration], taint_key: &str) -> bool {
    tolerations.iter().any(|t| {
        let key_match = t
            .key
            .as_deref()
            .is_none_or(|k| k.is_empty() || k == taint_key);
        let effect_match = t
            .effect
            .as_deref()
            .is_none_or(|e| e.is_empty() || e == EFFECT_NO_EXECUTE);
        key_match && effect_match
    })
}

fn default_toleration(key: &str) -> Toleration {
    Toleration {
        key: Some(key.to_string()),
        operator: Some(OP_EXISTS.to_string()),
        value: None,
        effect: Some(EFFECT_NO_EXECUTE.to_string()),
        toleration_seconds: Some(DEFAULT_TOLERATION_SECONDS),
    }
}

/// Append the default NotReady / Unreachable NoExecute tolerations
/// (`tolerationSeconds: 300`) to a pod spec, exactly as upstream's
/// `DefaultTolerationSeconds` admission plugin does.
///
/// Idempotent: a toleration that already covers the relevant taint (per
/// [`already_tolerates`], the same key+effect gate upstream uses) is left
/// untouched and the default is not appended. Returns the number of tolerations
/// added (0, 1, or 2).
pub fn add_default_tolerations(spec: &mut PodSpec) -> usize {
    let existing = spec.tolerations.get_or_insert_with(Vec::new);
    let mut added = 0;

    if !already_tolerates(existing, TAINT_NODE_NOT_READY) {
        existing.push(default_toleration(TAINT_NODE_NOT_READY));
        added += 1;
    }
    if !already_tolerates(existing, TAINT_NODE_UNREACHABLE) {
        existing.push(default_toleration(TAINT_NODE_UNREACHABLE));
        added += 1;
    }
    added
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::Container;

    fn spec_with_container() -> PodSpec {
        PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "busybox".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn tol(
        key: Option<&str>,
        effect: Option<&str>,
        op: Option<&str>,
        secs: Option<i64>,
    ) -> Toleration {
        Toleration {
            key: key.map(str::to_string),
            operator: op.map(str::to_string),
            value: None,
            effect: effect.map(str::to_string),
            toleration_seconds: secs,
        }
    }

    #[test]
    fn appends_both_defaults_when_none_present() {
        let mut spec = spec_with_container();
        assert_eq!(add_default_tolerations(&mut spec), 2);
        let tols = spec.tolerations.as_ref().unwrap();
        assert_eq!(tols.len(), 2);
        let not_ready = tols
            .iter()
            .find(|t| t.key.as_deref() == Some(TAINT_NODE_NOT_READY))
            .unwrap();
        assert_eq!(not_ready.operator.as_deref(), Some("Exists"));
        assert_eq!(not_ready.effect.as_deref(), Some("NoExecute"));
        assert_eq!(not_ready.toleration_seconds, Some(300));
        assert!(tols
            .iter()
            .any(|t| t.key.as_deref() == Some(TAINT_NODE_UNREACHABLE)));
    }

    #[test]
    fn skips_default_when_pod_already_tolerates_not_ready() {
        let mut spec = spec_with_container();
        // Pod already tolerates not-ready:NoExecute with its own (forever) toleration.
        spec.tolerations = Some(vec![tol(
            Some(TAINT_NODE_NOT_READY),
            Some("NoExecute"),
            Some("Exists"),
            None,
        )]);
        // Only the unreachable default should be appended.
        assert_eq!(add_default_tolerations(&mut spec), 1);
        let tols = spec.tolerations.as_ref().unwrap();
        assert_eq!(tols.len(), 2);
        // The pre-existing not-ready toleration keeps its tolerationSeconds:None.
        let not_ready = tols
            .iter()
            .find(|t| t.key.as_deref() == Some(TAINT_NODE_NOT_READY))
            .unwrap();
        assert_eq!(
            not_ready.toleration_seconds, None,
            "existing toleration untouched"
        );
    }

    #[test]
    fn empty_key_exists_toleration_covers_both() {
        let mut spec = spec_with_container();
        // Empty-key NoExecute toleration matches all taint keys (upstream rule 3).
        spec.tolerations = Some(vec![tol(None, Some("NoExecute"), Some("Exists"), None)]);
        assert_eq!(add_default_tolerations(&mut spec), 0);
        assert_eq!(spec.tolerations.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn empty_effect_toleration_covers_matching_key() {
        let mut spec = spec_with_container();
        // Empty effect matches all effects → covers NoExecute for not-ready.
        spec.tolerations = Some(vec![tol(
            Some(TAINT_NODE_NOT_READY),
            None,
            Some("Exists"),
            None,
        )]);
        // unreachable default still appended.
        assert_eq!(add_default_tolerations(&mut spec), 1);
    }

    #[test]
    fn noschedule_toleration_does_not_cover_noexecute() {
        let mut spec = spec_with_container();
        // A NoSchedule toleration for not-ready does NOT cover the NoExecute taint.
        spec.tolerations = Some(vec![tol(
            Some(TAINT_NODE_NOT_READY),
            Some("NoSchedule"),
            Some("Exists"),
            None,
        )]);
        assert_eq!(add_default_tolerations(&mut spec), 2);
    }

    #[test]
    fn idempotent_second_call_adds_nothing() {
        let mut spec = spec_with_container();
        add_default_tolerations(&mut spec);
        assert_eq!(add_default_tolerations(&mut spec), 0);
        assert_eq!(spec.tolerations.as_ref().unwrap().len(), 2);
    }
}
