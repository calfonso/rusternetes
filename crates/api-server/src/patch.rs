//! PATCH operations implementation for Kubernetes API compatibility
//!
//! Supports three patch types:
//! 1. Strategic Merge Patch (Kubernetes-specific)
//! 2. JSON Merge Patch (RFC 7386)
//! 3. JSON Patch (RFC 6902)

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Patch types supported by the API
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq)]
pub enum PatchType {
    /// Strategic merge patch (Kubernetes-specific merge semantics)
    StrategicMergePatch,
    /// JSON merge patch (RFC 7386)
    JsonMergePatch,
    /// JSON patch (RFC 6902) - array of operations
    JsonPatch,
}

impl PatchType {
    /// Parse patch type from Content-Type header
    pub fn from_content_type(content_type: &str) -> Result<Self, PatchError> {
        match content_type {
            "application/strategic-merge-patch+json" => Ok(PatchType::StrategicMergePatch),
            "application/merge-patch+json" => Ok(PatchType::JsonMergePatch),
            "application/json-patch+json" => Ok(PatchType::JsonPatch),
            _ => Err(PatchError::UnsupportedContentType(content_type.to_string())),
        }
    }

    /// Get Content-Type header value for this patch type
    pub fn content_type(&self) -> &'static str {
        match self {
            PatchType::StrategicMergePatch => "application/strategic-merge-patch+json",
            PatchType::JsonMergePatch => "application/merge-patch+json",
            PatchType::JsonPatch => "application/json-patch+json",
        }
    }
}

/// Errors that can occur during patch operations
#[derive(Debug, thiserror::Error)]
pub enum PatchError {
    #[error("Unsupported content type: {0}")]
    UnsupportedContentType(String),

    #[error("Invalid patch document: {0}")]
    InvalidPatch(String),

    #[error("Patch operation failed: {0}")]
    OperationFailed(String),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Resource version conflict")]
    ResourceVersionConflict,
}

/// JSON Patch operation (RFC 6902)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonPatchOperation {
    pub op: JsonPatchOp,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
}

/// JSON Patch operation types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum JsonPatchOp {
    Add,
    Remove,
    Replace,
    Move,
    Copy,
    Test,
}

/// Apply a patch to a resource
pub fn apply_patch(
    original: &Value,
    patch: &Value,
    patch_type: PatchType,
) -> Result<Value, PatchError> {
    match patch_type {
        PatchType::JsonMergePatch => apply_merge_patch(original, patch),
        PatchType::JsonPatch => apply_json_patch(original, patch),
        PatchType::StrategicMergePatch => apply_strategic_merge_patch(original, patch),
    }
}

/// Apply JSON Merge Patch (RFC 7386)
///
/// Rules:
/// - If patch is not an object, replace original with patch
/// - If patch is an object:
///   - For each key in patch:
///     - If value is null, delete key from original
///     - If value is an object and original[key] is an object, recursively merge
///     - Otherwise, replace original[key] with patch[key]
fn apply_merge_patch(original: &Value, patch: &Value) -> Result<Value, PatchError> {
    if !patch.is_object() {
        // Non-object patch replaces the entire value
        return Ok(patch.clone());
    }

    let mut result = if original.is_object() {
        original.clone()
    } else {
        json!({})
    };

    let result_obj = result
        .as_object_mut()
        .ok_or_else(|| PatchError::InvalidPatch("merge patch base is not an object".to_string()))?;
    let patch_obj = patch
        .as_object()
        .ok_or_else(|| PatchError::InvalidPatch("merge patch is not an object".to_string()))?;

    for (key, value) in patch_obj {
        if value.is_null() {
            // Null value deletes the key
            result_obj.remove(key);
        } else if value.is_object() && result_obj.get(key).is_some_and(|v| v.is_object()) {
            // Both are objects - recursively merge
            let merged = apply_merge_patch(&result_obj[key], value)?;
            result_obj.insert(key.clone(), merged);
        } else {
            // Replace or add the value
            result_obj.insert(key.clone(), value.clone());
        }
    }

    Ok(result)
}

/// Apply JSON Patch (RFC 6902)
fn apply_json_patch(original: &Value, patch: &Value) -> Result<Value, PatchError> {
    let operations: Vec<JsonPatchOperation> = serde_json::from_value(patch.clone())
        .map_err(|e| PatchError::InvalidPatch(e.to_string()))?;

    let mut current = original.clone();

    for op in operations {
        current = apply_json_patch_operation(&current, &op)?;
    }

    Ok(current)
}

/// Apply a single JSON Patch operation
fn apply_json_patch_operation(value: &Value, op: &JsonPatchOperation) -> Result<Value, PatchError> {
    let require_value = |what: &str| -> Result<&Value, PatchError> {
        op.value
            .as_ref()
            .ok_or_else(|| PatchError::InvalidPatch(format!("'value' required for {}", what)))
    };
    match op.op {
        JsonPatchOp::Add => add_operation(value, &op.path, require_value("add")?),
        JsonPatchOp::Remove => remove_operation(value, &op.path),
        JsonPatchOp::Replace => replace_operation(value, &op.path, require_value("replace")?),
        JsonPatchOp::Move => {
            let from = op
                .from
                .as_ref()
                .ok_or_else(|| PatchError::InvalidPatch("'from' required for move".to_string()))?;
            move_operation(value, from, &op.path)
        }
        JsonPatchOp::Copy => {
            let from = op
                .from
                .as_ref()
                .ok_or_else(|| PatchError::InvalidPatch("'from' required for copy".to_string()))?;
            copy_operation(value, from, &op.path)
        }
        JsonPatchOp::Test => {
            test_operation(value, &op.path, require_value("test")?)?;
            Ok(value.clone())
        }
    }
}

/// Add operation - adds a value at the specified path
fn add_operation(value: &Value, path: &str, new_value: &Value) -> Result<Value, PatchError> {
    let mut result = value.clone();
    let (parent_path, key) = split_path(path)?;

    if parent_path.is_empty() {
        // Adding to root
        return Ok(new_value.clone());
    }

    let parent = get_mut_value(&mut result, parent_path)?;

    if let Some(obj) = parent.as_object_mut() {
        obj.insert(key.to_string(), new_value.clone());
    } else if let Some(arr) = parent.as_array_mut() {
        let index = if key == "-" {
            arr.len()
        } else {
            key.parse::<usize>()
                .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", key)))?
        };
        arr.insert(index, new_value.clone());
    } else {
        return Err(PatchError::OperationFailed(format!(
            "Cannot add to non-object/array at path: {}",
            parent_path
        )));
    }

    Ok(result)
}

/// Remove operation - removes a value at the specified path
fn remove_operation(value: &Value, path: &str) -> Result<Value, PatchError> {
    let mut result = value.clone();
    let (parent_path, key) = split_path(path)?;

    let parent = get_mut_value(&mut result, parent_path)?;

    if let Some(obj) = parent.as_object_mut() {
        obj.remove(key);
    } else if let Some(arr) = parent.as_array_mut() {
        let index: usize = key
            .parse()
            .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", key)))?;
        if index >= arr.len() {
            return Err(PatchError::OperationFailed(format!(
                "Array index out of bounds: {}",
                index
            )));
        }
        arr.remove(index);
    } else {
        return Err(PatchError::OperationFailed(format!(
            "Cannot remove from non-object/array at path: {}",
            parent_path
        )));
    }

    Ok(result)
}

/// Replace operation - replaces a value at the specified path
fn replace_operation(value: &Value, path: &str, new_value: &Value) -> Result<Value, PatchError> {
    let mut result = value.clone();

    if path.is_empty() || path == "/" {
        return Ok(new_value.clone());
    }

    let (parent_path, key) = split_path(path)?;
    let parent = get_mut_value(&mut result, parent_path)?;

    if let Some(obj) = parent.as_object_mut() {
        obj.insert(key.to_string(), new_value.clone());
    } else if let Some(arr) = parent.as_array_mut() {
        let index: usize = key
            .parse()
            .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", key)))?;
        if index >= arr.len() {
            return Err(PatchError::OperationFailed(format!(
                "Array index out of bounds: {}",
                index
            )));
        }
        arr[index] = new_value.clone();
    }

    Ok(result)
}

/// Move operation - moves a value from one path to another
fn move_operation(value: &Value, from: &str, to: &str) -> Result<Value, PatchError> {
    // Get the value at 'from'
    let moved_value = get_value(value, from)?;
    // Remove from 'from' location
    let mut result = remove_operation(value, from)?;
    // Add to 'to' location
    result = add_operation(&result, to, &moved_value)?;
    Ok(result)
}

/// Copy operation - copies a value from one path to another
fn copy_operation(value: &Value, from: &str, to: &str) -> Result<Value, PatchError> {
    let copied_value = get_value(value, from)?;
    add_operation(value, to, &copied_value)
}

/// Test operation - tests that a value at the specified path equals the given value
fn test_operation(value: &Value, path: &str, test_value: &Value) -> Result<(), PatchError> {
    let current_value = get_value(value, path)?;
    if current_value != *test_value {
        return Err(PatchError::OperationFailed(format!(
            "Test failed at path: {}",
            path
        )));
    }
    Ok(())
}

/// Apply Strategic Merge Patch (Kubernetes-specific)
///
/// Implements strategic merge with directive markers:
/// - `$patch`: Specifies merge strategy ("replace", "merge", "delete")
/// - `$retainKeys`: List of keys to retain when using replace strategy
/// - `$deleteFromPrimitiveList`: Values to delete from primitive arrays
/// - Arrays with items that have a 'name' field are merged by name
/// - Other arrays replace the original (unless directives specify otherwise)
/// - Objects are recursively merged
/// - Null values delete keys
fn apply_strategic_merge_patch(original: &Value, patch: &Value) -> Result<Value, PatchError> {
    if !patch.is_object() {
        return Ok(patch.clone());
    }

    let mut result = if original.is_object() {
        original.clone()
    } else {
        json!({})
    };

    let result_obj = result.as_object_mut().ok_or_else(|| {
        PatchError::InvalidPatch("strategic merge base is not an object".to_string())
    })?;
    let patch_obj = patch.as_object().ok_or_else(|| {
        PatchError::InvalidPatch("strategic merge patch is not an object".to_string())
    })?;

    // Check for $patch directive
    let patch_strategy = patch_obj
        .get("$patch")
        .and_then(|v| v.as_str())
        .unwrap_or("merge");

    // Check for $retainKeys directive
    let retain_keys: Option<Vec<String>> = patch_obj
        .get("$retainKeys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        });

    match patch_strategy {
        "replace" => {
            // Replace strategy - replace entire object but retain specified keys
            if let Some(keys_to_retain) = retain_keys {
                let mut new_obj = serde_json::Map::new();
                // First, copy retained keys from original
                for key in &keys_to_retain {
                    if let Some(value) = result_obj.get(key) {
                        new_obj.insert(key.clone(), value.clone());
                    }
                }
                // Then apply patch values (excluding directives)
                for (key, value) in patch_obj {
                    if !key.starts_with('$') {
                        new_obj.insert(key.clone(), value.clone());
                    }
                }
                return Ok(Value::Object(new_obj));
            } else {
                // Full replacement (excluding directives)
                let mut new_obj = serde_json::Map::new();
                for (key, value) in patch_obj {
                    if !key.starts_with('$') {
                        new_obj.insert(key.clone(), value.clone());
                    }
                }
                return Ok(Value::Object(new_obj));
            }
        }
        "delete" => {
            // Delete the object entirely
            return Ok(Value::Null);
        }
        _ => {
            // Default merge strategy
            for (key, patch_value) in patch_obj {
                // Skip directive keys
                if key.starts_with('$') {
                    continue;
                }

                if patch_value.is_null() {
                    // Null deletes the key
                    result_obj.remove(key);
                } else if patch_value.is_array()
                    && result_obj.get(key).is_some_and(|v| v.is_array())
                {
                    // Check for $deleteFromPrimitiveList directive
                    let delete_list: Option<Vec<Value>> = if let Some(obj) = patch_value.as_array()
                    {
                        // Look for $deleteFromPrimitiveList in array elements
                        obj.iter().find_map(|item| {
                            item.as_object()
                                .and_then(|o| o.get("$deleteFromPrimitiveList"))
                                .and_then(|v| v.as_array())
                                .cloned()
                        })
                    } else {
                        None
                    };

                    if let Some(to_delete) = delete_list {
                        // Remove specified values from the original array
                        let mut original_array = result_obj[key]
                            .as_array()
                            .ok_or_else(|| {
                                PatchError::InvalidPatch(format!(
                                    "strategic merge: '{}' is not an array",
                                    key
                                ))
                            })?
                            .clone();
                        original_array.retain(|item| !to_delete.contains(item));
                        result_obj.insert(key.clone(), Value::Array(original_array));
                    } else {
                        // Strategic merge for arrays
                        let orig_arr = result_obj[key].as_array().ok_or_else(|| {
                            PatchError::InvalidPatch(format!(
                                "strategic merge: '{}' is not an array",
                                key
                            ))
                        })?;
                        let patch_arr = patch_value.as_array().ok_or_else(|| {
                            PatchError::InvalidPatch(format!(
                                "strategic merge: patch for '{}' is not an array",
                                key
                            ))
                        })?;
                        let merged_array = strategic_merge_arrays(orig_arr, patch_arr)?;
                        result_obj.insert(key.clone(), Value::Array(merged_array));
                    }
                } else if patch_value.is_object()
                    && result_obj.get(key).is_some_and(|v| v.is_object())
                {
                    // Recursively merge objects
                    let merged = apply_strategic_merge_patch(&result_obj[key], patch_value)?;
                    result_obj.insert(key.clone(), merged);
                } else {
                    // Replace value
                    result_obj.insert(key.clone(), patch_value.clone());
                }
            }
        }
    }

    Ok(result)
}

/// Strategic merge for arrays
///
/// If array items have a 'name' field, merge by name.
/// Otherwise, replace the array.
fn strategic_merge_arrays(original: &[Value], patch: &[Value]) -> Result<Vec<Value>, PatchError> {
    // Check if this is a named array (items have 'name' field)
    let is_named_array = patch
        .iter()
        .all(|v| v.as_object().is_some_and(|o| o.contains_key("name")));

    if is_named_array {
        // Merge by name
        let mut result: HashMap<String, Value> = HashMap::new();

        // Add all original items
        for item in original {
            if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                result.insert(name.to_string(), item.clone());
            }
        }

        // Merge/add patch items
        for item in patch {
            if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                if let Some(original_item) = result.get(name) {
                    // Merge with existing item
                    let merged = apply_strategic_merge_patch(original_item, item)?;
                    result.insert(name.to_string(), merged);
                } else {
                    // Add new item
                    result.insert(name.to_string(), item.clone());
                }
            }
        }

        // K8s SMP enforces order: patch items first (in patch order),
        // then server-only items (items in original but not in patch).
        // See: apimachinery/pkg/util/strategicpatch/patch.go normalizeElementOrder
        let patch_names: Vec<String> = patch
            .iter()
            .filter_map(|v| {
                v.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        let mut final_array = Vec::new();

        // First: items from the patch, in patch order
        for name in &patch_names {
            if let Some(item) = result.remove(name) {
                final_array.push(item);
            }
        }

        // Then: server-only items (in original order, not in patch)
        for item in original {
            if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                if let Some(item) = result.remove(name) {
                    final_array.push(item);
                }
            }
        }

        Ok(final_array)
    } else {
        // Not a named array - replace entirely
        Ok(patch.to_vec())
    }
}

/// Get a value at the specified JSON pointer path
fn get_value(value: &Value, path: &str) -> Result<Value, PatchError> {
    if path.is_empty() || path == "/" {
        return Ok(value.clone());
    }

    let parts = parse_path(path)?;
    let mut current = value;

    for part in parts {
        if let Some(obj) = current.as_object() {
            current = obj
                .get(&part)
                .ok_or_else(|| PatchError::OperationFailed(format!("Path not found: {}", path)))?;
        } else if let Some(arr) = current.as_array() {
            let index: usize = part
                .parse()
                .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", part)))?;
            current = arr.get(index).ok_or_else(|| {
                PatchError::OperationFailed(format!("Array index out of bounds: {}", index))
            })?;
        } else {
            return Err(PatchError::OperationFailed(format!(
                "Cannot traverse non-object/array at path: {}",
                path
            )));
        }
    }

    Ok(current.clone())
}

/// Get a mutable reference to a value at the specified path
fn get_mut_value<'a>(value: &'a mut Value, path: &str) -> Result<&'a mut Value, PatchError> {
    if path.is_empty() || path == "/" {
        return Ok(value);
    }

    let parts = parse_path(path)?;
    let mut current = value;

    for part in parts {
        if current.is_object() {
            current = current
                .as_object_mut()
                .unwrap()
                .entry(part.clone())
                .or_insert(json!({}));
        } else if current.is_array() {
            let index: usize = part
                .parse()
                .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", part)))?;
            current = current
                .as_array_mut()
                .unwrap()
                .get_mut(index)
                .ok_or_else(|| {
                    PatchError::OperationFailed(format!("Array index out of bounds: {}", index))
                })?;
        } else {
            return Err(PatchError::OperationFailed(format!(
                "Cannot traverse non-object/array in path: {}",
                path
            )));
        }
    }

    Ok(current)
}

/// Parse a JSON pointer path into parts
fn parse_path(path: &str) -> Result<Vec<String>, PatchError> {
    if !path.starts_with('/') {
        return Err(PatchError::InvalidPatch(format!(
            "Path must start with '/': {}",
            path
        )));
    }

    Ok(path[1..]
        .split('/')
        .map(|part| {
            // Unescape ~1 -> / and ~0 -> ~
            part.replace("~1", "/").replace("~0", "~")
        })
        .collect())
}

/// Split a path into parent path and final key
fn split_path(path: &str) -> Result<(&str, &str), PatchError> {
    if path.is_empty() || path == "/" {
        return Err(PatchError::InvalidPatch(
            "Cannot split root path".to_string(),
        ));
    }

    let last_slash = path
        .rfind('/')
        .ok_or_else(|| PatchError::InvalidPatch(format!("Path missing '/': {}", path)))?;
    let parent = if last_slash == 0 {
        "/"
    } else {
        &path[..last_slash]
    };
    let key = &path[last_slash + 1..];

    Ok((parent, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_merge_patch_simple() {
        let original = json!({
            "name": "test",
            "value": 1
        });

        let patch = json!({
            "value": 2,
            "new_field": "hello"
        });

        let result = apply_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["name"], "test");
        assert_eq!(result["value"], 2);
        assert_eq!(result["new_field"], "hello");
    }

    #[test]
    fn test_json_merge_patch_delete() {
        let original = json!({
            "name": "test",
            "value": 1
        });

        let patch = json!({
            "value": null
        });

        let result = apply_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["name"], "test");
        assert!(result.get("value").is_none());
    }

    #[test]
    fn test_json_patch_add() {
        let original = json!({
            "name": "test"
        });

        let patch = json!([
            {"op": "add", "path": "/value", "value": 42}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert_eq!(result["value"], 42);
    }

    #[test]
    fn test_json_patch_remove() {
        let original = json!({
            "name": "test",
            "value": 42
        });

        let patch = json!([
            {"op": "remove", "path": "/value"}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert!(result.get("value").is_none());
    }

    #[test]
    fn test_json_patch_replace() {
        let original = json!({
            "name": "test",
            "value": 42
        });

        let patch = json!([
            {"op": "replace", "path": "/value", "value": 100}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert_eq!(result["value"], 100);
    }

    #[test]
    fn test_strategic_merge_patch_simple() {
        let original = json!({
            "metadata": {
                "name": "test",
                "labels": {
                    "app": "nginx"
                }
            },
            "spec": {
                "replicas": 1
            }
        });

        let patch = json!({
            "metadata": {
                "labels": {
                    "version": "1.0"
                }
            },
            "spec": {
                "replicas": 3
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["metadata"]["name"], "test");
        assert_eq!(result["metadata"]["labels"]["app"], "nginx");
        assert_eq!(result["metadata"]["labels"]["version"], "1.0");
        assert_eq!(result["spec"]["replicas"], 3);
    }

    #[test]
    fn test_strategic_merge_arrays_by_name() {
        let original = json!([
            {"name": "container1", "image": "nginx:1.0"},
            {"name": "container2", "image": "redis:5"}
        ]);

        let patch = json!([
            {"name": "container1", "image": "nginx:1.1"},
            {"name": "container3", "image": "postgres:12"}
        ]);

        let result =
            strategic_merge_arrays(original.as_array().unwrap(), patch.as_array().unwrap())
                .unwrap();

        assert_eq!(result.len(), 3);
        // K8s SMP order: patch items first, then server-only items
        assert_eq!(result[0]["image"], "nginx:1.1"); // Patch item 1 (updated)
        assert_eq!(result[1]["name"], "container3"); // Patch item 2 (added)
        assert_eq!(result[2]["image"], "redis:5"); // Server-only (preserved)
    }

    #[test]
    fn test_strategic_merge_arrays_patch_items_first() {
        // Reproduces StatefulSet patch scenario: patch adds container with different name
        // K8s SMP puts patch items before server-only items
        let original = json!([
            {"name": "webserver", "image": "agnhost:2.55", "args": ["test-webserver"]}
        ]);
        let patch = json!([
            {"name": "test-ss", "image": "pause:3.10.1"}
        ]);

        let result =
            strategic_merge_arrays(original.as_array().unwrap(), patch.as_array().unwrap())
                .unwrap();

        assert_eq!(result.len(), 2);
        // Patch item comes first
        assert_eq!(result[0]["name"], "test-ss");
        assert_eq!(result[0]["image"], "pause:3.10.1");
        // Server-only item comes second
        assert_eq!(result[1]["name"], "webserver");
        assert_eq!(result[1]["image"], "agnhost:2.55");
    }

    #[test]
    fn test_patch_type_from_content_type() {
        assert_eq!(
            PatchType::from_content_type("application/strategic-merge-patch+json").unwrap(),
            PatchType::StrategicMergePatch
        );
        assert_eq!(
            PatchType::from_content_type("application/merge-patch+json").unwrap(),
            PatchType::JsonMergePatch
        );
        assert_eq!(
            PatchType::from_content_type("application/json-patch+json").unwrap(),
            PatchType::JsonPatch
        );
    }

    #[test]
    fn test_strategic_merge_patch_directive() {
        let original = json!({
            "metadata": {
                "name": "test",
                "labels": {
                    "app": "nginx",
                    "version": "1.0"
                }
            },
            "spec": {
                "replicas": 1
            }
        });

        let patch = json!({
            "metadata": {
                "labels": {
                    "$patch": "replace",
                    "app": "apache"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // With $patch: replace, the labels should be replaced entirely
        assert_eq!(result["metadata"]["labels"]["app"], "apache");
        assert!(result["metadata"]["labels"].get("version").is_none());
    }

    #[test]
    fn test_strategic_merge_patch_retain_keys() {
        let original = json!({
            "metadata": {
                "name": "test",
                "uid": "abc-123",
                "labels": {
                    "app": "nginx",
                    "version": "1.0"
                }
            }
        });

        let patch = json!({
            "metadata": {
                "$patch": "replace",
                "$retainKeys": ["name", "uid"],
                "labels": {
                    "app": "apache"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // Should retain name and uid, but replace labels
        assert_eq!(result["metadata"]["name"], "test");
        assert_eq!(result["metadata"]["uid"], "abc-123");
        assert_eq!(result["metadata"]["labels"]["app"], "apache");
    }

    #[test]
    fn test_strategic_merge_delete_from_primitive_list() {
        let original = json!({
            "spec": {
                "finalizers": ["kubernetes.io/pv-protection", "example.com/my-finalizer"]
            }
        });

        let patch = json!({
            "spec": {
                "finalizers": [
                    {"$deleteFromPrimitiveList": ["example.com/my-finalizer"]}
                ]
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        let finalizers = result["spec"]["finalizers"].as_array().unwrap();
        assert_eq!(finalizers.len(), 1);
        assert_eq!(finalizers[0], "kubernetes.io/pv-protection");
    }

    #[test]
    fn test_strategic_merge_delete_directive() {
        let original = json!({
            "metadata": {
                "name": "test",
                "annotations": {
                    "foo": "bar"
                }
            }
        });

        let patch = json!({
            "metadata": {
                "annotations": {
                    "$patch": "delete"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert!(result["metadata"]["annotations"].is_null());
    }
}

/// Recursively merge two JSON objects. For nested objects, entries from
/// `patch` are merged INTO `target` without replacing existing entries.
/// Null values in patch remove the key from target.
/// This is used for strategic merge patch on status subresources where
/// maps like capacity/allocatable must be merged, not replaced.
pub fn deep_merge_objects(target: &mut serde_json::Value, patch: &serde_json::Value) {
    if let (Some(target_obj), Some(patch_obj)) = (target.as_object_mut(), patch.as_object()) {
        for (k, v) in patch_obj {
            if v.is_null() {
                target_obj.remove(k);
            } else if v.is_object() {
                let existing = target_obj
                    .entry(k.clone())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                deep_merge_objects(existing, v);
            } else {
                target_obj.insert(k.clone(), v.clone());
            }
        }
    }
}

#[test]
fn test_deep_merge_preserves_existing_map_entries() {
    let mut target = serde_json::json!({
        "capacity": {
            "cpu": "4",
            "memory": "8Gi",
            "pods": "110"
        },
        "conditions": [{"type": "Ready"}]
    });
    let patch = serde_json::json!({
        "capacity": {
            "scheduling.k8s.io/foo": "5"
        }
    });
    deep_merge_objects(&mut target, &patch);

    let cap = target.get("capacity").unwrap().as_object().unwrap();
    assert_eq!(cap.get("cpu").unwrap(), "4", "existing cpu preserved");
    assert_eq!(
        cap.get("memory").unwrap(),
        "8Gi",
        "existing memory preserved"
    );
    assert_eq!(
        cap.get("scheduling.k8s.io/foo").unwrap(),
        "5",
        "new extended resource added"
    );
    assert!(target.get("conditions").is_some(), "conditions preserved");
}
