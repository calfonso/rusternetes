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
