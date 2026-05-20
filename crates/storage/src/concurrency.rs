/// Concurrency control module for optimistic locking with resourceVersion
use rusternetes_common::Error;

/// Extract resourceVersion from metadata.
///
/// Returns `None` when the field is absent *or* when it is an empty string.
/// Kubernetes treats `resourceVersion: ""` identically to an absent
/// resourceVersion (no optimistic-concurrency check). Callers that
/// previously received `Some("")` would have tried to parse it as an i64
/// and failed with "Invalid resourceVersion: ".
pub fn extract_resource_version(metadata: &serde_json::Value) -> Option<String> {
    metadata
        .get("resourceVersion")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Validate that the provided resourceVersion matches the expected version
pub fn validate_resource_version(
    expected: Option<&str>,
    actual: Option<&str>,
) -> Result<(), Error> {
    match (expected, actual) {
        (Some(expected_rv), Some(actual_rv)) => {
            if expected_rv != actual_rv {
                return Err(Error::Conflict(format!(
                    "resourceVersion mismatch: expected '{}', got '{}'",
                    expected_rv, actual_rv
                )));
            }
            Ok(())
        }
        (Some(expected_rv), None) => Err(Error::Conflict(format!(
            "resourceVersion mismatch: expected '{}', got none",
            expected_rv
        ))),
        _ => Ok(()), // If no expected version specified, allow update
    }
}

/// Convert etcd mod_revision to resourceVersion string
pub fn mod_revision_to_resource_version(mod_revision: i64) -> String {
    mod_revision.to_string()
}

/// Parse resourceVersion string to mod_revision
pub fn resource_version_to_mod_revision(resource_version: &str) -> Result<i64, Error> {
    resource_version.parse::<i64>().map_err(|_| {
        Error::InvalidResource(format!("Invalid resourceVersion: {}", resource_version))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_resource_version() {
        let metadata = json!({
            "name": "test",
            "resourceVersion": "12345"
        });

        assert_eq!(
            extract_resource_version(&metadata),
            Some("12345".to_string())
        );

        let no_rv = json!({"name": "test"});
        assert_eq!(extract_resource_version(&no_rv), None);

        // Empty string must be treated as absent (same as K8s semantics).
        let empty_rv = json!({"name": "test", "resourceVersion": ""});
        assert_eq!(extract_resource_version(&empty_rv), None);
    }

    #[test]
    fn test_validate_resource_version() {
        // Matching versions should succeed
        assert!(validate_resource_version(Some("100"), Some("100")).is_ok());

        // No expected version should succeed
        assert!(validate_resource_version(None, Some("100")).is_ok());

        // Mismatched versions should fail
        assert!(validate_resource_version(Some("100"), Some("200")).is_err());

        // Expected version but actual is missing should fail
        assert!(validate_resource_version(Some("100"), None).is_err());
    }

    #[test]
    fn test_mod_revision_conversion() {
        let rv = mod_revision_to_resource_version(12345);
        assert_eq!(rv, "12345");

        let mod_rev = resource_version_to_mod_revision("12345").unwrap();
        assert_eq!(mod_rev, 12345);

        assert!(resource_version_to_mod_revision("invalid").is_err());
    }
}
