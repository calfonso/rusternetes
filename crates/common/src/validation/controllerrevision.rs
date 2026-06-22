//! ControllerRevision validation — port of upstream Kubernetes
//! `pkg/apis/apps/validation/validation.go::ValidateControllerRevisionCreate`
//! (release-1.35).
//!
//! `data` is mandatory and must be a JSON object. ObjectMeta is validated
//! separately (#1087 / #1277).

use crate::resources::ControllerRevision;
use crate::validation::field::{Error, ErrorList, Path};

/// Validate a `ControllerRevision` on create. Mirrors the `data` check of
/// upstream `ValidateControllerRevisionCreate`.
pub fn validate_controller_revision(cr: &ControllerRevision) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let data_path = Path::new("data");
    match &cr.data {
        None | Some(serde_json::Value::Null) => {
            errs.push(Error::required(&data_path, "data is mandatory"));
        }
        Some(v) if !v.is_object() => {
            errs.push(Error::required(
                &data_path,
                "data must be a valid JSON object",
            ));
        }
        Some(_) => {}
    }
    errs
}

/// Validate a `ControllerRevision` on update. Mirrors upstream
/// `ValidateControllerRevisionUpdate`: re-run the create validation and enforce
/// that `data` is immutable (only `revision` and metadata may change).
pub fn validate_controller_revision_update(
    new: &ControllerRevision,
    old: &ControllerRevision,
) -> ErrorList {
    let mut errs = validate_controller_revision(new);
    if new.data != old.data {
        errs.push(Error::invalid(
            &Path::new("data"),
            "<data>".to_string(),
            "field is immutable",
        ));
    }
    errs
}

#[cfg(test)]
mod update_tests {
    use super::*;

    fn cr(rev: i64, data: serde_json::Value) -> ControllerRevision {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "rev1"},
            "revision": rev,
            "data": data,
        }))
        .unwrap()
    }

    #[test]
    fn revision_may_change_data_may_not() {
        let old = cr(1, serde_json::json!({"k": "v"}));
        // Only revision changes -> allowed.
        let bumped = cr(2, serde_json::json!({"k": "v"}));
        assert!(validate_controller_revision_update(&bumped, &old).is_empty());
        // data changes -> immutable error.
        let changed = cr(1, serde_json::json!({"k": "w"}));
        let errs = validate_controller_revision_update(&changed, &old);
        assert!(
            errs.iter()
                .any(|e| e.field == "data" && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }
}
