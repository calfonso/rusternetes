// Field Selector implementation for server-side filtering
//
// Enables filtering resources by field values like:
// - status.phase=Running
// - spec.nodeName=node-1
// - metadata.namespace=default

use serde_json::Value;
use std::fmt;

/// Field selector for filtering resources by field values
#[derive(Debug, Clone, PartialEq)]
pub struct FieldSelector {
    /// Individual field requirements
    requirements: Vec<FieldRequirement>,
}

impl FieldSelector {
    /// Parse a field selector string
    ///
    /// Format: field1=value1,field2!=value2
    /// Operators: =, ==, !=
    ///
    /// Examples:
    /// - "status.phase=Running"
    /// - "spec.nodeName=node-1,status.phase!=Failed"
    /// - "metadata.namespace=default"
    pub fn parse(selector: &str) -> Result<Self, FieldSelectorError> {
        if selector.is_empty() {
            return Ok(Self {
                requirements: Vec::new(),
            });
        }

        let mut requirements = Vec::new();

        // Split by comma
        for requirement_str in selector.split(',') {
            let requirement = FieldRequirement::parse(requirement_str.trim())?;
            requirements.push(requirement);
        }

        Ok(Self { requirements })
    }

    /// Check if a resource matches this field selector
    pub fn matches(&self, resource: &Value) -> bool {
        // Empty selector matches everything
        if self.requirements.is_empty() {
            return true;
        }

        // All requirements must match
        self.requirements.iter().all(|req| req.matches(resource))
    }

    /// Get the individual requirements
    pub fn requirements(&self) -> &[FieldRequirement] {
        &self.requirements
    }

    /// Check if this is an empty selector
    pub fn is_empty(&self) -> bool {
        self.requirements.is_empty()
    }
}

/// A single field requirement
#[derive(Debug, Clone, PartialEq)]
pub struct FieldRequirement {
    /// Field path (e.g., "status.phase", "spec.nodeName")
    field: String,
    /// Operator (=, ==, !=)
    operator: FieldOperator,
    /// Expected value
    value: String,
}

impl FieldRequirement {
    /// Parse a single field requirement
    fn parse(requirement: &str) -> Result<Self, FieldSelectorError> {
        // Try to find operator
        if let Some(pos) = requirement.find("!=") {
            let field = requirement[..pos].trim().to_string();
            let value = requirement[pos + 2..].trim().to_string();
            return Ok(Self {
                field,
                operator: FieldOperator::NotEquals,
                value,
            });
        }

        if let Some(pos) = requirement.find("==") {
            let field = requirement[..pos].trim().to_string();
            let value = requirement[pos + 2..].trim().to_string();
            return Ok(Self {
                field,
                operator: FieldOperator::Equals,
                value,
            });
        }

        if let Some(pos) = requirement.find('=') {
            let field = requirement[..pos].trim().to_string();
            let value = requirement[pos + 1..].trim().to_string();
            return Ok(Self {
                field,
                operator: FieldOperator::Equals,
                value,
            });
        }

        Err(FieldSelectorError::InvalidRequirement(
            requirement.to_string(),
        ))
    }

    /// Check if a resource matches this requirement
    fn matches(&self, resource: &Value) -> bool {
        let field_value = extract_field_value(resource, &self.field);

        match &self.operator {
            FieldOperator::Equals => {
                if let Some(ref fv) = field_value {
                    fv == &self.value
                } else {
                    // Field is missing/null. In Kubernetes, missing boolean fields
                    // default to false, missing string fields default to "".
                    self.value == "false" || self.value.is_empty()
                }
            }
            FieldOperator::NotEquals => {
                if let Some(ref fv) = field_value {
                    fv != &self.value
                } else {
                    // Missing field: treat as "false" or "" for comparison
                    self.value != "false" && !self.value.is_empty()
                }
            }
        }
    }

    /// Get the field path
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Get the operator
    pub fn operator(&self) -> FieldOperator {
        self.operator
    }

    /// Get the expected value
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Field selector operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldOperator {
    /// Equals (= or ==)
    Equals,
    /// Not equals (!=)
    NotEquals,
}

impl fmt::Display for FieldOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldOperator::Equals => write!(f, "="),
            FieldOperator::NotEquals => write!(f, "!="),
        }
    }
}

/// Extract a field value from a JSON object using a path
///
/// Supports nested fields like "status.phase" or "metadata.namespace"
fn extract_field_value(resource: &Value, field_path: &str) -> Option<String> {
    // K8s field selector aliases — some fields are shorthands for nested paths
    let resolved_path = match field_path {
        // Event source.component is queried as just "source"
        "source" => "source.component",
        // events.k8s.io/v1 `reportingController` is a top-level field (it
        // serializes under that exact name), so the selector key resolves to
        // itself via the default arm below.
        // Event type is at top level
        "type" => "type",
        // Event reason
        "reason" => "reason",
        // involvedObject fields stay as-is (already dotted paths)
        _ => field_path,
    };

    let parts: Vec<&str> = resolved_path.split('.').collect();
    let mut current = resource;

    for part in parts {
        current = current.get(part)?;
    }

    // Convert the final value to string
    match current {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => Some("null".to_string()),
        _ => None, // Objects and arrays don't convert to simple strings
    }
}

/// Errors that can occur during field selector operations
#[derive(Debug, thiserror::Error)]
pub enum FieldSelectorError {
    #[error("Invalid field selector requirement: {0}")]
    InvalidRequirement(String),

    #[error("Invalid operator in field selector: {0}")]
    InvalidOperator(String),

    #[error("Field not found: {0}")]
    FieldNotFound(String),
}

/// Common field selector patterns for Kubernetes resources
impl FieldSelector {
    /// Match pods by status phase
    pub fn pod_phase(phase: &str) -> Self {
        Self {
            requirements: vec![FieldRequirement {
                field: "status.phase".to_string(),
                operator: FieldOperator::Equals,
                value: phase.to_string(),
            }],
        }
    }

    /// Match pods by node name
    pub fn pod_node(node_name: &str) -> Self {
        Self {
            requirements: vec![FieldRequirement {
                field: "spec.nodeName".to_string(),
                operator: FieldOperator::Equals,
                value: node_name.to_string(),
            }],
        }
    }

    /// Match resources by namespace
    pub fn namespace(namespace: &str) -> Self {
        Self {
            requirements: vec![FieldRequirement {
                field: "metadata.namespace".to_string(),
                operator: FieldOperator::Equals,
                value: namespace.to_string(),
            }],
        }
    }

    /// Match resources by name
    pub fn name(name: &str) -> Self {
        Self {
            requirements: vec![FieldRequirement {
                field: "metadata.name".to_string(),
                operator: FieldOperator::Equals,
                value: name.to_string(),
            }],
        }
    }
}

impl fmt::Display for FieldSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .requirements
            .iter()
            .map(|req| format!("{}{}{}", req.field, req.operator, req.value))
            .collect();
        write!(f, "{}", parts.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_single_requirement() {
        let selector = FieldSelector::parse("status.phase=Running").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].field, "status.phase");
        assert_eq!(selector.requirements[0].operator, FieldOperator::Equals);
        assert_eq!(selector.requirements[0].value, "Running");
    }

    #[test]
    fn test_parse_multiple_requirements() {
        let selector = FieldSelector::parse("status.phase=Running,spec.nodeName=node-1").unwrap();
        assert_eq!(selector.requirements.len(), 2);
        assert_eq!(selector.requirements[0].field, "status.phase");
        assert_eq!(selector.requirements[1].field, "spec.nodeName");
    }

    #[test]
    fn test_parse_not_equals() {
        let selector = FieldSelector::parse("status.phase!=Failed").unwrap();
        assert_eq!(selector.requirements[0].operator, FieldOperator::NotEquals);
        assert_eq!(selector.requirements[0].value, "Failed");
    }

    #[test]
    fn test_parse_double_equals() {
        let selector = FieldSelector::parse("status.phase==Running").unwrap();
        assert_eq!(selector.requirements[0].operator, FieldOperator::Equals);
    }

    #[test]
    fn test_matches_simple() {
        let selector = FieldSelector::parse("status.phase=Running").unwrap();
        let resource = json!({
            "status": {
                "phase": "Running"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_matches_nested() {
        let selector = FieldSelector::parse("spec.nodeName=node-1").unwrap();
        let resource = json!({
            "spec": {
                "nodeName": "node-1"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_not_matches() {
        let selector = FieldSelector::parse("status.phase=Running").unwrap();
        let resource = json!({
            "status": {
                "phase": "Pending"
            }
        });

        assert!(!selector.matches(&resource));
    }

    #[test]
    fn test_not_equals_matches() {
        let selector = FieldSelector::parse("status.phase!=Failed").unwrap();
        let resource = json!({
            "status": {
                "phase": "Running"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_not_equals_not_matches() {
        let selector = FieldSelector::parse("status.phase!=Failed").unwrap();
        let resource = json!({
            "status": {
                "phase": "Failed"
            }
        });

        assert!(!selector.matches(&resource));
    }

    #[test]
    fn test_multiple_requirements_all_match() {
        let selector = FieldSelector::parse("status.phase=Running,spec.nodeName=node-1").unwrap();
        let resource = json!({
            "status": {
                "phase": "Running"
            },
            "spec": {
                "nodeName": "node-1"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_multiple_requirements_one_fails() {
        let selector = FieldSelector::parse("status.phase=Running,spec.nodeName=node-1").unwrap();
        let resource = json!({
            "status": {
                "phase": "Running"
            },
            "spec": {
                "nodeName": "node-2"
            }
        });

        assert!(!selector.matches(&resource));
    }

    #[test]
    fn test_field_not_found() {
        let selector = FieldSelector::parse("status.phase=Running").unwrap();
        let resource = json!({
            "metadata": {
                "name": "test"
            }
        });

        assert!(!selector.matches(&resource));
    }

    #[test]
    fn test_empty_selector_matches_all() {
        let selector = FieldSelector::parse("").unwrap();
        let resource = json!({"any": "value"});

        assert!(selector.matches(&resource));
        assert!(selector.is_empty());
    }

    #[test]
    fn test_helper_pod_phase() {
        let selector = FieldSelector::pod_phase("Running");
        let resource = json!({
            "status": {
                "phase": "Running"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_helper_namespace() {
        let selector = FieldSelector::namespace("default");
        let resource = json!({
            "metadata": {
                "namespace": "default"
            }
        });

        assert!(selector.matches(&resource));
    }

    #[test]
    fn test_display() {
        let selector = FieldSelector::parse("status.phase=Running,spec.nodeName!=node-1").unwrap();
        let displayed = format!("{}", selector);
        assert!(displayed.contains("status.phase=Running"));
        assert!(displayed.contains("spec.nodeName!=node-1"));
    }

    #[test]
    fn test_extract_string_field() {
        let resource = json!({
            "metadata": {
                "name": "test-pod"
            }
        });

        let value = extract_field_value(&resource, "metadata.name");
        assert_eq!(value, Some("test-pod".to_string()));
    }

    #[test]
    fn test_extract_number_field() {
        let resource = json!({
            "spec": {
                "replicas": 3
            }
        });

        let value = extract_field_value(&resource, "spec.replicas");
        assert_eq!(value, Some("3".to_string()));
    }

    #[test]
    fn test_extract_bool_field() {
        let resource = json!({
            "status": {
                "ready": true
            }
        });

        let value = extract_field_value(&resource, "status.ready");
        assert_eq!(value, Some("true".to_string()));
    }
}
