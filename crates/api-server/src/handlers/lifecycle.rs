//! Resource lifecycle helpers for generation tracking and optimistic concurrency control.
//!
//! These helpers implement two critical Kubernetes conformance behaviors:
//! 1. `metadata.generation` tracking: incremented when spec changes, not on status-only updates
//! 2. `metadata.resourceVersion` conflict detection: 409 Conflict on stale updates

use rusternetes_common::types::ObjectMeta;

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
    if old_spec != new_spec {
        let current = metadata.generation.unwrap_or(0);
        metadata.generation = Some(current + 1);
    }
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
}
