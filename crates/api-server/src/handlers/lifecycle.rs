//! Resource lifecycle helpers for generation tracking and optimistic concurrency control.
//!
//! These helpers implement three critical Kubernetes conformance behaviors:
//! 1. `metadata.generation` tracking: incremented when spec changes, not on status-only updates
//! 2. `metadata.resourceVersion` conflict detection: 409 Conflict on stale updates
//! 3. `spec.selector` immutability: 422 Invalid on selector changes for apps/v1 workloads

use rusternetes_common::types::{LabelSelector, ObjectMeta};

/// Set generation to 1 on newly created resources.
///
/// This should be called during resource creation, after all mutations
/// but before storing the resource.
pub fn set_initial_generation(metadata: &mut ObjectMeta) {
    // K8s always sets generation to 1 on creation, regardless of what the client sends
    metadata.generation = Some(1);
}

/// Increment generation if spec has changed (by comparing old vs new JSON, ignoring metadata and status).
///
/// This should be called during resource updates (PUT), but NOT during status-only updates.
///
/// The comparison is performed after normalising empty `{}` objects to
/// absent on both sides (via `validation::pod::strip_empty_objects`) so a
/// no-op Go round-trip — which always emits `"resources":{}` on every
/// container because Go's `omitempty` does not detect zero-valued struct
/// values — does NOT trip a false-positive generation bump. This mirrors
/// upstream's `apiequality.Semantic.DeepEqual` semantics used by the same
/// strategy hook in `pkg/registry/core/pod/strategy.go::PrepareForUpdate`.
pub fn maybe_increment_generation(
    old_json: &serde_json::Value,
    new_json: &serde_json::Value,
    metadata: &mut ObjectMeta,
) {
    let mut old_spec = old_json.clone();
    let mut new_spec = new_json.clone();
    if let Some(obj) = old_spec.as_object_mut() {
        obj.remove("metadata");
        obj.remove("status");
    }
    if let Some(obj) = new_spec.as_object_mut() {
        obj.remove("metadata");
        obj.remove("status");
    }
    rusternetes_common::validation::pod::strip_empty_objects(&mut old_spec);
    rusternetes_common::validation::pod::strip_empty_objects(&mut new_spec);
    if old_spec != new_spec {
        let current = metadata.generation.unwrap_or(0);
        metadata.generation = Some(current + 1);
    }
}

/// Validate that `spec.selector` has not changed between the stored object and the
/// incoming update. Mirrors upstream `ValidateDeploymentUpdate` /
/// `ValidateReplicaSetUpdate` / `ValidateStatefulSetUpdate` / `ValidateDaemonSetUpdate`,
/// each of which calls `ValidateImmutableField(new.Spec.Selector, old.Spec.Selector,
/// field.NewPath("spec").Child("selector"))`.
///
/// Returns `Err(InvalidResource)` (HTTP 422) when the selector differs.
pub fn validate_selector_immutable(
    old_selector: &LabelSelector,
    new_selector: &LabelSelector,
    kind: &str,
) -> rusternetes_common::Result<()> {
    if old_selector != new_selector {
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "{}.spec.selector: Invalid value: field is immutable",
            kind
        )));
    }
    Ok(())
}

/// Check resourceVersion for optimistic concurrency control.
///
/// Returns Err(Conflict) if the provided resourceVersion doesn't match the stored one.
/// If either side is None, the check is skipped (for backwards compatibility).
pub fn check_resource_version(
    stored_rv: Option<&str>,
    provided_rv: Option<&str>,
    resource_name: &str,
) -> rusternetes_common::Result<()> {
    match (stored_rv, provided_rv) {
        (Some(stored), Some(provided)) if stored != provided => {
            Err(rusternetes_common::Error::Conflict(format!(
                "the object has been modified; please apply your changes to the latest version of {} (stored resourceVersion: {}, provided: {})",
                resource_name, stored, provided
            )))
        }
        _ => Ok(()),
    }
}

/// Parse a `DeleteOptions` body and enforce `preconditions.resourceVersion` /
/// `preconditions.uid` against the stored object's metadata.
///
/// Upstream contract:
/// `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go::Delete`
/// invokes `preconditions.Check(qualifiedKind, obj)` before calling the storage
/// delete; a mismatch returns 409 Conflict with reason `Conflict`.
///
/// `body` is the raw request bytes. An empty body or a body that doesn't parse
/// as JSON is treated as no preconditions (Kubernetes is lenient here — clients
/// frequently send empty DELETE bodies). When a `preconditions` block is
/// present, each declared field MUST match the stored object.
pub fn check_delete_preconditions(
    body: &[u8],
    stored_meta: &ObjectMeta,
    resource_name: &str,
) -> rusternetes_common::Result<()> {
    if body.is_empty() {
        return Ok(());
    }
    let parsed: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let preconditions = match parsed.get("preconditions") {
        Some(p) if p.is_object() => p,
        _ => return Ok(()),
    };

    if let Some(expected_rv) = preconditions
        .get("resourceVersion")
        .and_then(|v| v.as_str())
    {
        let stored_rv = stored_meta.resource_version.as_deref().unwrap_or("");
        if stored_rv != expected_rv {
            return Err(rusternetes_common::Error::Conflict(format!(
                "Precondition failed: ResourceVersion in precondition: {}, ResourceVersion in object meta: {} on resource {}",
                expected_rv, stored_rv, resource_name
            )));
        }
    }

    if let Some(expected_uid) = preconditions.get("uid").and_then(|v| v.as_str()) {
        let stored_uid = stored_meta.uid.as_str();
        if stored_uid != expected_uid {
            return Err(rusternetes_common::Error::Conflict(format!(
                "Precondition failed: UID in precondition: {}, UID in object meta: {} on resource {}",
                expected_uid, stored_uid, resource_name
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_initial_generation() {
        let mut meta = ObjectMeta::default();
        assert!(meta.generation.is_none());
        set_initial_generation(&mut meta);
        assert_eq!(meta.generation, Some(1));
    }

    #[test]
    fn test_set_initial_generation_preserves_existing() {
        let mut meta = ObjectMeta {
            generation: Some(5),
            ..Default::default()
        };
        set_initial_generation(&mut meta);
        assert_eq!(meta.generation, Some(1)); // K8s always sets generation=1 on creation
    }

    #[test]
    fn test_maybe_increment_generation_spec_changed() {
        let old = serde_json::json!({
            "metadata": {"name": "test", "generation": 1},
            "spec": {"replicas": 1},
            "status": {"ready": true}
        });
        let new = serde_json::json!({
            "metadata": {"name": "test", "generation": 1},
            "spec": {"replicas": 3},
            "status": {"ready": true}
        });
        let mut meta = ObjectMeta {
            generation: Some(1),
            ..Default::default()
        };
        maybe_increment_generation(&old, &new, &mut meta);
        assert_eq!(meta.generation, Some(2));
    }

    #[test]
    fn test_maybe_increment_generation_no_spec_change() {
        let old = serde_json::json!({
            "metadata": {"name": "test", "generation": 1},
            "spec": {"replicas": 1},
            "status": {"ready": false}
        });
        let new = serde_json::json!({
            "metadata": {"name": "test-changed", "generation": 2},
            "spec": {"replicas": 1},
            "status": {"ready": true}
        });
        let mut meta = ObjectMeta {
            generation: Some(1),
            ..Default::default()
        };
        maybe_increment_generation(&old, &new, &mut meta);
        assert_eq!(meta.generation, Some(1));
    }

    #[test]
    fn test_check_resource_version_match() {
        let result = check_resource_version(Some("5"), Some("5"), "test-pod");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_resource_version_mismatch() {
        let result = check_resource_version(Some("5"), Some("3"), "test-pod");
        assert!(result.is_err());
    }

    #[test]
    fn test_check_resource_version_none_stored() {
        let result = check_resource_version(None, Some("3"), "test-pod");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_resource_version_none_provided() {
        let result = check_resource_version(Some("5"), None, "test-pod");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_delete_preconditions_empty_body_allows_delete() {
        let meta = ObjectMeta {
            resource_version: Some("9".to_string()),
            uid: "u-1".to_string(),
            ..Default::default()
        };
        assert!(check_delete_preconditions(b"", &meta, "test").is_ok());
    }

    #[test]
    fn test_check_delete_preconditions_invalid_json_is_lenient() {
        let meta = ObjectMeta {
            resource_version: Some("9".to_string()),
            ..Default::default()
        };
        assert!(check_delete_preconditions(b"not-json", &meta, "test").is_ok());
    }

    #[test]
    fn test_check_delete_preconditions_no_preconditions_field_allows_delete() {
        let meta = ObjectMeta {
            resource_version: Some("9".to_string()),
            ..Default::default()
        };
        let body = br#"{"kind":"DeleteOptions","apiVersion":"v1"}"#;
        assert!(check_delete_preconditions(body, &meta, "test").is_ok());
    }

    #[test]
    fn test_check_delete_preconditions_matching_rv_passes() {
        let meta = ObjectMeta {
            resource_version: Some("9".to_string()),
            ..Default::default()
        };
        let body = br#"{"preconditions":{"resourceVersion":"9"}}"#;
        assert!(check_delete_preconditions(body, &meta, "test").is_ok());
    }

    #[test]
    fn test_check_delete_preconditions_mismatched_rv_returns_conflict() {
        let meta = ObjectMeta {
            resource_version: Some("9".to_string()),
            ..Default::default()
        };
        let body = br#"{"preconditions":{"resourceVersion":"1"}}"#;
        let err = check_delete_preconditions(body, &meta, "test").unwrap_err();
        match err {
            rusternetes_common::Error::Conflict(msg) => {
                assert!(msg.contains("ResourceVersion"));
            }
            other => panic!("expected Conflict, got {:?}", other),
        }
    }

    #[test]
    fn test_check_delete_preconditions_mismatched_uid_returns_conflict() {
        let meta = ObjectMeta {
            resource_version: Some("9".to_string()),
            uid: "u-current".to_string(),
            ..Default::default()
        };
        let body = br#"{"preconditions":{"uid":"u-stale"}}"#;
        let err = check_delete_preconditions(body, &meta, "test").unwrap_err();
        assert!(matches!(err, rusternetes_common::Error::Conflict(_)));
    }

    #[test]
    fn test_validate_selector_immutable_unchanged() {
        let mut ml = std::collections::HashMap::new();
        ml.insert("app".to_string(), "foo".to_string());
        let a = LabelSelector {
            match_labels: Some(ml.clone()),
            match_expressions: None,
        };
        let b = LabelSelector {
            match_labels: Some(ml),
            match_expressions: None,
        };
        assert!(validate_selector_immutable(&a, &b, "Deployment").is_ok());
    }

    #[test]
    fn test_validate_selector_immutable_changed_is_invalid_resource() {
        let mut ml_a = std::collections::HashMap::new();
        ml_a.insert("app".to_string(), "foo".to_string());
        let mut ml_b = std::collections::HashMap::new();
        ml_b.insert("app".to_string(), "bar".to_string());
        let a = LabelSelector {
            match_labels: Some(ml_a),
            match_expressions: None,
        };
        let b = LabelSelector {
            match_labels: Some(ml_b),
            match_expressions: None,
        };
        let err =
            validate_selector_immutable(&a, &b, "Deployment").expect_err("must reject the change");
        assert!(
            matches!(err, rusternetes_common::Error::InvalidResource(_)),
            "expected Invalid (422), got: {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("Deployment.spec.selector"), "msg: {msg}");
        assert!(msg.contains("immutable"), "msg: {msg}");
    }
}
