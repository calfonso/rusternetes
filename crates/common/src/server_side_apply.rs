// Server-Side Apply implementation for Kubernetes API compatibility
//
// Tracks field ownership across multiple clients and provides conflict detection.
// Implements the managedFields metadata tracking for field managers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Managed fields entry tracking which manager owns which fields
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedFieldsEntry {
    /// Manager identifier (e.g., "kubectl", "controller-manager")
    pub manager: String,

    /// Operation type
    pub operation: Operation,

    /// API version when this field was last modified
    pub api_version: String,

    /// Timestamp of last modification
    #[serde(
        default,
        serialize_with = "crate::time::k8s_time::serialize",
        deserialize_with = "crate::time::k8s_time::deserialize"
    )]
    pub time: Option<DateTime<Utc>>,

    /// Fields owned by this manager (JSON representation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields_v1: Option<Value>,

    /// Subresource if applicable
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subresource: Option<String>,
}

/// Operation type for managed fields
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Operation {
    /// Apply operation (server-side apply)
    Apply,
    /// Update operation (PUT/PATCH)
    Update,
}

/// Server-side apply request parameters
#[derive(Debug, Clone)]
pub struct ApplyParams {
    /// Field manager name
    pub field_manager: String,

    /// Force apply (override conflicts)
    pub force: bool,
}

impl ApplyParams {
    /// Create new apply parameters
    pub fn new(field_manager: String) -> Self {
        Self {
            field_manager,
            force: false,
        }
    }

    /// Enable force mode
    pub fn with_force(mut self) -> Self {
        self.force = true;
        self
    }
}

/// Conflict information when field managers disagree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    /// Field path that has conflict
    pub field: String,

    /// Current manager of the field
    pub current_manager: String,

    /// Manager trying to apply
    pub applying_manager: String,
}

/// Server-side apply result
#[derive(Debug)]
pub enum ApplyResult {
    /// Successfully applied
    Success(Value),

    /// Conflicts detected (requires force=true)
    Conflicts(Vec<Conflict>),
}

/// Apply a resource using server-side apply semantics
pub fn server_side_apply(
    current: Option<&Value>,
    desired: &Value,
    params: &ApplyParams,
) -> Result<ApplyResult, ApplyError> {
    // If no current resource exists, create new
    let current = match current {
        Some(c) => c.clone(),
        None => {
            // New resource - add managed fields
            let mut result = desired.clone();
            add_managed_fields(&mut result, &params.field_manager, Operation::Apply)?;
            return Ok(ApplyResult::Success(result));
        }
    };

    // Extract current managed fields
    let current_managed_fields = extract_managed_fields(&current)?;

    // Compute which fields are being modified
    let modified_fields = compute_modified_fields(&current, desired)?;

    // Check for conflicts
    let conflicts = detect_conflicts(
        &current_managed_fields,
        &modified_fields,
        &params.field_manager,
    );

    if !conflicts.is_empty() && !params.force {
        return Ok(ApplyResult::Conflicts(conflicts));
    }

    // Merge the changes
    let mut result = current.clone();
    merge_fields(&mut result, desired, &modified_fields)?;

    // Update managed fields
    update_managed_fields(
        &mut result,
        &params.field_manager,
        Operation::Apply,
        &modified_fields,
    )?;

    Ok(ApplyResult::Success(result))
}

/// Add managed fields to a new resource
fn add_managed_fields(
    resource: &mut Value,
    manager: &str,
    operation: Operation,
) -> Result<(), ApplyError> {
    // Compute fields_v1 before borrowing metadata mutably
    let api_version = extract_api_version(resource).unwrap_or_else(|| "v1".to_string());
    let fields_v1 = compute_fields_v1(resource)?;

    let metadata = resource
        .get_mut("metadata")
        .and_then(|v| v.as_object_mut())
        .ok_or(ApplyError::InvalidResource("Missing metadata".to_string()))?;

    let managed_entry = ManagedFieldsEntry {
        manager: manager.to_string(),
        operation,
        api_version,
        time: Some(Utc::now()),
        fields_v1: Some(fields_v1),
        subresource: None,
    };

    metadata.insert(
        "managedFields".to_string(),
        serde_json::to_value(vec![managed_entry])?,
    );

    Ok(())
}

/// Extract current managed fields from a resource
fn extract_managed_fields(resource: &Value) -> Result<Vec<ManagedFieldsEntry>, ApplyError> {
    let metadata = resource
        .get("metadata")
        .and_then(|v| v.as_object())
        .ok_or(ApplyError::InvalidResource("Missing metadata".to_string()))?;

    if let Some(managed_fields) = metadata.get("managedFields") {
        Ok(serde_json::from_value(managed_fields.clone())?)
    } else {
        Ok(Vec::new())
    }
}

/// Compute which fields have been modified
fn compute_modified_fields(
    current: &Value,
    desired: &Value,
) -> Result<HashMap<String, Value>, ApplyError> {
    let mut modified = HashMap::new();

    if let (Some(current_obj), Some(desired_obj)) = (current.as_object(), desired.as_object()) {
        for (key, desired_value) in desired_obj {
            if let Some(current_value) = current_obj.get(key) {
                if current_value != desired_value {
                    modified.insert(key.clone(), desired_value.clone());
                }
            } else {
                // New field
                modified.insert(key.clone(), desired_value.clone());
            }
        }
    }

    Ok(modified)
}

/// Detect conflicts between field managers
fn detect_conflicts(
    managed_fields: &[ManagedFieldsEntry],
    modified_fields: &HashMap<String, Value>,
    applying_manager: &str,
) -> Vec<Conflict> {
    let mut conflicts = Vec::new();

    for field in modified_fields.keys() {
        // Skip metadata fields (always allowed to update)
        if field == "metadata" {
            continue;
        }

        // Check if another manager owns this field
        for entry in managed_fields {
            if entry.manager != applying_manager {
                if let Some(fields_v1) = &entry.fields_v1 {
                    if field_owned_by(field, fields_v1) {
                        conflicts.push(Conflict {
                            field: field.clone(),
                            current_manager: entry.manager.clone(),
                            applying_manager: applying_manager.to_string(),
                        });
                    }
                }
            }
        }
    }

    conflicts
}

/// Check if a field is owned by a manager based on fields_v1
fn field_owned_by(field: &str, fields_v1: &Value) -> bool {
    // Simplified check - in real implementation, this would traverse the field structure
    if let Some(obj) = fields_v1.as_object() {
        return obj.contains_key(field);
    }
    false
}

/// Merge modified fields into the result
fn merge_fields(
    target: &mut Value,
    source: &Value,
    modified_fields: &HashMap<String, Value>,
) -> Result<(), ApplyError> {
    if let Some(target_obj) = target.as_object_mut() {
        for (key, value) in modified_fields {
            if key == "metadata" {
                // Special handling for metadata - merge carefully
                merge_metadata(target_obj, source)?;
            } else {
                target_obj.insert(key.clone(), value.clone());
            }
        }
    }

    Ok(())
}

/// Merge metadata fields carefully (preserve system fields)
fn merge_metadata(
    target_obj: &mut serde_json::Map<String, Value>,
    source: &Value,
) -> Result<(), ApplyError> {
    if let Some(source_metadata) = source.get("metadata").and_then(|v| v.as_object()) {
        let target_metadata = target_obj
            .entry("metadata".to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()))
            .as_object_mut()
            .ok_or(ApplyError::InvalidResource("Invalid metadata".to_string()))?;

        // Merge user-controllable fields
        for (key, value) in source_metadata {
            match key.as_str() {
                // Preserve system-managed fields
                "uid"
                | "resourceVersion"
                | "generation"
                | "creationTimestamp"
                | "deletionTimestamp"
                | "deletionGracePeriodSeconds" => {
                    // Don't overwrite
                }
                "managedFields" => {
                    // Don't copy from source - we'll compute it
                }
                // User-controllable fields
                "labels" | "annotations" | "finalizers" | "ownerReferences" => {
                    target_metadata.insert(key.clone(), value.clone());
                }
                _ => {
                    // Other fields - merge
                    target_metadata.insert(key.clone(), value.clone());
                }
            }
        }
    }

    Ok(())
}

/// Update managed fields after apply
fn update_managed_fields(
    resource: &mut Value,
    manager: &str,
    operation: Operation,
    modified_fields: &HashMap<String, Value>,
) -> Result<(), ApplyError> {
    let mut managed_fields = extract_managed_fields(resource)?;

    // Find existing entry for this manager
    if let Some(entry) = managed_fields.iter_mut().find(|e| e.manager == manager) {
        // Update existing entry
        entry.operation = operation;
        entry.time = Some(Utc::now());
        entry.api_version = extract_api_version(resource).unwrap_or_else(|| "v1".to_string());
        entry.fields_v1 = Some(compute_fields_from_modified(modified_fields)?);
    } else {
        // Add new entry
        managed_fields.push(ManagedFieldsEntry {
            manager: manager.to_string(),
            operation,
            api_version: extract_api_version(resource).unwrap_or_else(|| "v1".to_string()),
            time: Some(Utc::now()),
            fields_v1: Some(compute_fields_from_modified(modified_fields)?),
            subresource: None,
        });
    }

    // Update resource
    let metadata = resource
        .get_mut("metadata")
        .and_then(|v| v.as_object_mut())
        .ok_or(ApplyError::InvalidResource("Missing metadata".to_string()))?;

    metadata.insert(
        "managedFields".to_string(),
        serde_json::to_value(managed_fields)?,
    );

    Ok(())
}

/// Extract API version from resource
fn extract_api_version(resource: &Value) -> Option<String> {
    resource
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Compute fields_v1 representation from the entire resource
fn compute_fields_v1(resource: &Value) -> Result<Value, ApplyError> {
    // Simplified - just track top-level fields
    // Real implementation would create a structured field set
    let mut fields = serde_json::Map::new();

    if let Some(obj) = resource.as_object() {
        for key in obj.keys() {
            if key != "metadata" {
                fields.insert(key.clone(), Value::Bool(true));
            }
        }
    }

    Ok(Value::Object(fields))
}

/// Compute fields_v1 from modified fields
fn compute_fields_from_modified(
    modified_fields: &HashMap<String, Value>,
) -> Result<Value, ApplyError> {
    let mut fields = serde_json::Map::new();

    for key in modified_fields.keys() {
        if key != "metadata" {
            fields.insert(key.clone(), Value::Bool(true));
        }
    }

    Ok(Value::Object(fields))
}

/// Errors that can occur during server-side apply
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("Invalid resource: {0}")]
    InvalidResource(String),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Conflicts detected (use force=true to override)")]
    ConflictsDetected,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_apply_new_resource() {
        let desired = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod"
            },
            "spec": {
                "containers": []
            }
        });

        let params = ApplyParams::new("kubectl".to_string());
        let result = server_side_apply(None, &desired, &params).unwrap();

        match result {
            ApplyResult::Success(resource) => {
                let managed_fields = extract_managed_fields(&resource).unwrap();
                assert_eq!(managed_fields.len(), 1);
                assert_eq!(managed_fields[0].manager, "kubectl");
                assert_eq!(managed_fields[0].operation, Operation::Apply);
            }
            ApplyResult::Conflicts(_) => panic!("Unexpected conflicts"),
        }
    }

    #[test]
    fn test_apply_update_same_manager() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "managedFields": [{
                    "manager": "kubectl",
                    "operation": "Apply",
                    "apiVersion": "v1",
                    "fieldsV1": {"spec": true}
                }]
            },
            "spec": {
                "containers": []
            }
        });

        let desired = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod"
            },
            "spec": {
                "replicas": 3
            }
        });

        let params = ApplyParams::new("kubectl".to_string());
        let result = server_side_apply(Some(&current), &desired, &params).unwrap();

        match result {
            ApplyResult::Success(_) => {}
            ApplyResult::Conflicts(_) => panic!("Unexpected conflicts for same manager"),
        }
    }

    #[test]
    fn test_detect_conflict_different_manager() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "managedFields": [{
                    "manager": "controller",
                    "operation": "Update",
                    "apiVersion": "v1",
                    "fieldsV1": {"spec": true}
                }]
            },
            "spec": {
                "replicas": 1
            }
        });

        let desired = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod"
            },
            "spec": {
                "replicas": 3
            }
        });

        let params = ApplyParams::new("kubectl".to_string());
        let result = server_side_apply(Some(&current), &desired, &params).unwrap();

        match result {
            ApplyResult::Conflicts(conflicts) => {
                assert!(!conflicts.is_empty());
                assert_eq!(conflicts[0].current_manager, "controller");
                assert_eq!(conflicts[0].applying_manager, "kubectl");
            }
            ApplyResult::Success(_) => panic!("Expected conflicts"),
        }
    }

    #[test]
    fn test_force_apply_overrides_conflict() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "managedFields": [{
                    "manager": "controller",
                    "operation": "Update",
                    "apiVersion": "v1",
                    "fieldsV1": {"spec": true}
                }]
            },
            "spec": {
                "replicas": 1
            }
        });

        let desired = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod"
            },
            "spec": {
                "replicas": 3
            }
        });

        let params = ApplyParams::new("kubectl".to_string()).with_force();
        let result = server_side_apply(Some(&current), &desired, &params).unwrap();

        match result {
            ApplyResult::Success(resource) => {
                assert_eq!(resource["spec"]["replicas"], 3);
            }
            ApplyResult::Conflicts(_) => panic!("Force should override conflicts"),
        }
    }

    #[test]
    fn test_metadata_merge_preserves_system_fields() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "uid": "abc-123",
                "resourceVersion": "100",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            }
        });

        let desired = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "labels": {
                    "app": "test"
                }
            }
        });

        let params = ApplyParams::new("kubectl".to_string());
        let result = server_side_apply(Some(&current), &desired, &params).unwrap();

        match result {
            ApplyResult::Success(resource) => {
                // System fields preserved
                assert_eq!(resource["metadata"]["uid"], "abc-123");
                assert_eq!(resource["metadata"]["resourceVersion"], "100");
                // User fields applied
                assert_eq!(resource["metadata"]["labels"]["app"], "test");
            }
            ApplyResult::Conflicts(_) => panic!("Unexpected conflicts"),
        }
    }
}
