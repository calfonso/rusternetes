//! Namespace validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateNamespace` (release-1.35).
//!
//! Validates `spec.finalizers` (the legacy namespace finalizer list, distinct
//! from `metadata.finalizers`): each must be a qualified name, and an
//! unqualified (no `/`) name must be one of the standard finalizers. ObjectMeta
//! is validated separately (#1087 / #1277).

use crate::resources::Namespace;
use crate::types::Phase;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_qualified_name;

/// Standard finalizer names (`pkg/apis/core/helper.standardFinalizers`):
/// `kubernetes` + the metav1 orphan / foreground-deletion finalizers.
const STANDARD_FINALIZERS: [&str; 3] = ["kubernetes", "orphan", "foregroundDeletion"];

/// Validate a `Namespace` on create. Mirrors upstream `ValidateNamespace` minus
/// ObjectMeta — the `spec.finalizers` checks (`validateFinalizerName` +
/// `validateKubeFinalizerName`).
pub fn validate_namespace(ns: &Namespace) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let path = Path::new("spec").child("finalizers");

    let Some(spec) = &ns.spec else {
        return errs;
    };
    let Some(finalizers) = &spec.finalizers else {
        return errs;
    };
    for f in finalizers {
        for msg in is_qualified_name(f) {
            errs.push(Error::invalid(&path, f.clone(), msg));
        }
        if !f.contains('/') && !STANDARD_FINALIZERS.contains(&f.as_str()) {
            errs.push(Error::invalid(
                &path,
                f.clone(),
                "name is neither a standard finalizer name nor is it fully qualified",
            ));
        }
    }
    errs
}

/// Render a `Phase` as the bare wire string (e.g. `Active`, `Terminating`) for
/// inclusion in a validation error's bad-value, matching how upstream reports
/// the offending `status.Phase` value. `None` renders as the empty string,
/// mirroring Go's zero-valued `core.NamespacePhase`.
fn phase_str(phase: Option<&Phase>) -> String {
    match phase {
        Some(p) => serde_json::to_value(p)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default(),
        None => String::new(),
    }
}

/// Validate a `Namespace` status update. Port of upstream
/// `ValidateNamespaceStatusUpdate` (release-1.35,
/// `pkg/apis/core/validation/validation.go`, lines 8202-8215).
///
/// The phase must be consistent with the deletion timestamp:
///   - when `deletionTimestamp` is empty, the phase may only be `Active`;
///   - when `deletionTimestamp` is set, the phase may only be `Terminating`.
///
/// ObjectMeta-update validation (the upstream `ValidateObjectMetaUpdate` call)
/// is handled separately and is not duplicated here. `old` is accepted for
/// signature parity with upstream even though the phase rule only inspects
/// `new`.
pub fn validate_namespace_status_update(new: &Namespace, _old: &Namespace) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    // Upstream uses field.NewPath("status", "Phase") — note the capitalised
    // "Phase" segment; preserve it verbatim for error-wording parity.
    let path = Path::new("status").child("Phase");

    let phase = new.status.as_ref().and_then(|s| s.phase.as_ref());

    if new.metadata.deletion_timestamp.is_none() {
        if phase != Some(&Phase::Active) {
            errs.push(Error::invalid(
                &path,
                phase_str(phase),
                "may only be 'Active' if `deletionTimestamp` is empty",
            ));
        }
    } else if phase != Some(&Phase::Terminating) {
        errs.push(Error::invalid(
            &path,
            phase_str(phase),
            "may only be 'Terminating' if `deletionTimestamp` is not empty",
        ));
    }

    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::NamespaceStatus;
    use chrono::Utc;

    fn ns_with(phase: Option<Phase>, deleting: bool) -> Namespace {
        let mut ns = Namespace::new("test");
        ns.status = Some(NamespaceStatus {
            phase,
            conditions: None,
        });
        ns.metadata.deletion_timestamp = if deleting { Some(Utc::now()) } else { None };
        ns
    }

    #[test]
    fn active_with_no_deletion_timestamp_is_valid() {
        let ns = ns_with(Some(Phase::Active), false);
        assert!(validate_namespace_status_update(&ns, &ns).is_empty());
    }

    #[test]
    fn terminating_with_deletion_timestamp_is_valid() {
        let ns = ns_with(Some(Phase::Terminating), true);
        assert!(validate_namespace_status_update(&ns, &ns).is_empty());
    }

    #[test]
    fn non_active_without_deletion_timestamp_is_invalid() {
        let ns = ns_with(Some(Phase::Terminating), false);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "status.Phase");
        assert_eq!(
            errs[0].detail,
            "may only be 'Active' if `deletionTimestamp` is empty"
        );
    }

    #[test]
    fn missing_phase_without_deletion_timestamp_is_invalid() {
        let ns = ns_with(None, false);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(
            errs[0].detail,
            "may only be 'Active' if `deletionTimestamp` is empty"
        );
    }

    #[test]
    fn non_terminating_with_deletion_timestamp_is_invalid() {
        let ns = ns_with(Some(Phase::Active), true);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "status.Phase");
        assert_eq!(
            errs[0].detail,
            "may only be 'Terminating' if `deletionTimestamp` is not empty"
        );
    }

    #[test]
    fn missing_phase_with_deletion_timestamp_is_invalid() {
        let ns = ns_with(None, true);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(
            errs[0].detail,
            "may only be 'Terminating' if `deletionTimestamp` is not empty"
        );
    }
}
