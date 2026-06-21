//! Namespace validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateNamespace` (release-1.35).
//!
//! Validates `spec.finalizers` (the legacy namespace finalizer list, distinct
//! from `metadata.finalizers`): each must be a qualified name, and an
//! unqualified (no `/`) name must be one of the standard finalizers. ObjectMeta
//! is validated separately (#1087 / #1277).

use crate::resources::Namespace;
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
