//! DeviceClass (resource.k8s.io / DRA) validation — port of upstream
//! Kubernetes `pkg/apis/resource/validation/validation.go::ValidateDeviceClass`
//! (release-1.35).
//!
//! Scope: the CEL-free structural checks of `spec` — the `selectors` set
//! (≤32, each must carry a `cel` selector whose `expression` is non-empty and
//! ≤10Ki) and the `config` set (≤32). Compiling the CEL expressions against the
//! DRA CEL environment is tracked in #1442. ObjectMeta is validated separately.

use crate::resources::{DeviceClass, DeviceClassSpec};
use crate::validation::field::{Error, ErrorList, Path};

const DEVICE_SELECTORS_MAX_SIZE: usize = 32;
const DEVICE_CONFIG_MAX_SIZE: usize = 32;
const CEL_SELECTOR_EXPRESSION_MAX_LENGTH: usize = 10 * 1024;

/// Validate a `DeviceClass` on create. Mirrors the structural part of upstream
/// `ValidateDeviceClass` (minus CEL compilation — see #1442).
pub fn validate_device_class(class: &DeviceClass) -> ErrorList {
    validate_device_class_spec(&class.spec, &Path::new("spec"))
}

// Note: `ValidateDeviceClassUpdate` upstream simply re-runs `ValidateDeviceClass`
// (the spec is mutable, no immutable fields), so the update handler calls
// `validate_device_class` directly.

fn validate_device_class_spec(spec: &DeviceClassSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let selectors_path = fld_path.child("selectors");
    if spec.selectors.len() > DEVICE_SELECTORS_MAX_SIZE {
        errs.push(Error::too_many(&selectors_path, DEVICE_SELECTORS_MAX_SIZE));
    }
    for (i, selector) in spec.selectors.iter().enumerate() {
        let sp = selectors_path.index(i);
        match &selector.cel {
            None => errs.push(Error::required(&sp.child("cel"), "")),
            Some(cel) => {
                let expr_path = sp.child("cel").child("expression");
                if cel.expression.is_empty() {
                    errs.push(Error::required(&expr_path, ""));
                } else if cel.expression.len() > CEL_SELECTOR_EXPRESSION_MAX_LENGTH {
                    errs.push(Error::too_long(
                        &expr_path,
                        CEL_SELECTOR_EXPRESSION_MAX_LENGTH,
                    ));
                }
            }
        }
    }

    if spec.config.len() > DEVICE_CONFIG_MAX_SIZE {
        errs.push(Error::too_many(
            &fld_path.child("config"),
            DEVICE_CONFIG_MAX_SIZE,
        ));
    }

    errs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dc(spec: serde_json::Value) -> DeviceClass {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "class-1"},
            "spec": spec,
        }))
        .unwrap()
    }

    fn errs(spec: serde_json::Value) -> Vec<String> {
        validate_device_class(&dc(spec))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_selector_passes() {
        assert!(errs(serde_json::json!({
            "selectors": [{"cel": {"expression": "device.attributes['x'] == 1"}}]
        }))
        .is_empty());
        // empty spec also passes
        assert!(errs(serde_json::json!({})).is_empty());
    }

    #[test]
    fn selector_requires_cel() {
        let e = errs(serde_json::json!({"selectors": [{}]}));
        assert!(e.iter().any(|m| m.contains("cel")), "{e:?}");
    }

    #[test]
    fn empty_expression_rejected() {
        let e = errs(serde_json::json!({"selectors": [{"cel": {"expression": ""}}]}));
        assert!(e.iter().any(|m| m.contains("expression")), "{e:?}");
    }

    #[test]
    fn too_long_expression_rejected() {
        let big = "a".repeat(CEL_SELECTOR_EXPRESSION_MAX_LENGTH + 1);
        let e = errs(serde_json::json!({"selectors": [{"cel": {"expression": big}}]}));
        assert!(
            e.iter()
                .any(|m| m.contains("expression") && m.contains("bytes")),
            "{e:?}"
        );
    }

    #[test]
    fn too_many_selectors_rejected() {
        let sels: Vec<_> = (0..DEVICE_SELECTORS_MAX_SIZE + 1)
            .map(|_| serde_json::json!({"cel": {"expression": "true"}}))
            .collect();
        let e = errs(serde_json::json!({"selectors": sels}));
        assert!(
            e.iter()
                .any(|m| m.contains("selectors") && m.contains("at most")),
            "{e:?}"
        );
    }
}
