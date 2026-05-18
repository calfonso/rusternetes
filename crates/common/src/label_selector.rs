// Label Selector implementation for server-side filtering
//
// Enables filtering resources by labels using both equality-based and set-based selectors.
// Supports the full Kubernetes label selector syntax:
// - Equality-based: "app=nginx,tier=frontend" or "app==nginx,tier!=backend"
// - Set-based: "environment in (production, qa),tier notin (frontend, backend)"
// - Mixed: "app=nginx,environment in (production, qa),tier notin (backend)"

use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Label selector for filtering resources by labels
#[derive(Debug, Clone, PartialEq)]
pub struct LabelSelector {
    /// Individual selector requirements
    requirements: Vec<LabelRequirement>,
}

impl LabelSelector {
    /// Parse a label selector string
    ///
    /// Format supports both equality-based and set-based selectors:
    /// - Equality: key=value, key==value, key!=value
    /// - Set-based: key in (val1,val2), key notin (val1,val2), key, !key
    ///
    /// Examples:
    /// - "app=nginx"
    /// - "app=nginx,tier!=backend"
    /// - "environment in (production, qa)"
    /// - "tier notin (frontend,backend)"
    /// - "app=nginx,environment in (production, qa)"
    /// - "hasKey" (key exists)
    /// - "!hasKey" (key does not exist)
    pub fn parse(selector: &str) -> Result<Self, LabelSelectorError> {
        if selector.is_empty() {
            return Ok(Self {
                requirements: Vec::new(),
            });
        }

        let mut requirements = Vec::new();
        let mut current = selector;

        while !current.is_empty() {
            current = current.trim();

            // Try to parse set-based selector first (they contain parentheses)
            if let Some((req, remaining)) = Self::try_parse_set_based(current)? {
                requirements.push(req);
                current = remaining;
            } else if let Some((req, remaining)) = Self::try_parse_equality_based(current)? {
                requirements.push(req);
                current = remaining;
            } else if let Some((req, remaining)) = Self::try_parse_existence_based(current)? {
                requirements.push(req);
                current = remaining;
            } else {
                return Err(LabelSelectorError::InvalidSelector(current.to_string()));
            }

            // Skip comma separator
            current = current.trim_start_matches(',').trim();
        }

        Ok(Self { requirements })
    }

    /// Try to parse a set-based selector (in/notin)
    fn try_parse_set_based(
        input: &str,
    ) -> Result<Option<(LabelRequirement, &str)>, LabelSelectorError> {
        // Look for "notin" operator first (longer match)
        // Match with or without spaces: "notin" or " notin "
        if let Some(notin_pos) = input.find("notin") {
            // Ensure it's not part of another word
            let before_ok = notin_pos == 0 || input.as_bytes()[notin_pos - 1].is_ascii_whitespace();
            let after_ok = notin_pos + 6 >= input.len()
                || input.as_bytes()[notin_pos + 6].is_ascii_whitespace()
                || input.as_bytes()[notin_pos + 6] == b'(';

            if before_ok && after_ok {
                let key = input[..notin_pos].trim();
                let rest = input[notin_pos + 6..].trim();
                let (values, remaining) = Self::parse_value_set(rest)?;

                return Ok(Some((
                    LabelRequirement {
                        key: key.to_string(),
                        operator: LabelOperator::NotIn,
                        values: Some(values),
                    },
                    remaining,
                )));
            }
        }

        // Look for "in" operator
        // Match with or without spaces: "in" or " in "
        if let Some(in_pos) = input.find("in") {
            // Ensure it's not part of another word (like "notin")
            let before_ok = in_pos == 0 || input.as_bytes()[in_pos - 1].is_ascii_whitespace();
            let after_ok = in_pos + 2 >= input.len()
                || input.as_bytes()[in_pos + 2].is_ascii_whitespace()
                || input.as_bytes()[in_pos + 2] == b'(';
            // Make sure it's not "notin"
            let not_notin = in_pos < 3 || &input[in_pos - 3..in_pos] != "not";

            if before_ok && after_ok && not_notin {
                let key = input[..in_pos].trim();
                let rest = input[in_pos + 2..].trim();
                let (values, remaining) = Self::parse_value_set(rest)?;

                return Ok(Some((
                    LabelRequirement {
                        key: key.to_string(),
                        operator: LabelOperator::In,
                        values: Some(values),
                    },
                    remaining,
                )));
            }
        }

        Ok(None)
    }

    /// Parse a value set like "(value1, value2, value3)"
    fn parse_value_set(input: &str) -> Result<(Vec<String>, &str), LabelSelectorError> {
        let input = input.trim();

        if !input.starts_with('(') {
            return Err(LabelSelectorError::InvalidSelector(
                "Expected '(' for value set".to_string(),
            ));
        }

        let close_paren = input.find(')').ok_or_else(|| {
            LabelSelectorError::InvalidSelector("Missing closing ')' for value set".to_string())
        })?;

        let values_str = &input[1..close_paren];
        let values: Vec<String> = values_str
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect();

        if values.is_empty() {
            return Err(LabelSelectorError::InvalidSelector(
                "Value set cannot be empty".to_string(),
            ));
        }

        let remaining = &input[close_paren + 1..];
        Ok((values, remaining))
    }

    /// Try to parse an equality-based selector (=, ==, !=)
    fn try_parse_equality_based(
        input: &str,
    ) -> Result<Option<(LabelRequirement, &str)>, LabelSelectorError> {
        // Find the next comma or end of string
        let end_pos = input.find(',').unwrap_or(input.len());
        let selector_part = &input[..end_pos];
        let remaining = if end_pos < input.len() {
            &input[end_pos..]
        } else {
            ""
        };

        // Try != first (two characters)
        if let Some(pos) = selector_part.find("!=") {
            let key = selector_part[..pos].trim().to_string();
            let value = selector_part[pos + 2..].trim().to_string();

            if key.is_empty() || value.is_empty() {
                return Err(LabelSelectorError::InvalidSelector(
                    "Key and value cannot be empty".to_string(),
                ));
            }

            return Ok(Some((
                LabelRequirement {
                    key,
                    operator: LabelOperator::NotEquals,
                    values: Some(vec![value]),
                },
                remaining,
            )));
        }

        // Try == (two characters)
        if let Some(pos) = selector_part.find("==") {
            let key = selector_part[..pos].trim().to_string();
            let value = selector_part[pos + 2..].trim().to_string();

            if key.is_empty() || value.is_empty() {
                return Err(LabelSelectorError::InvalidSelector(
                    "Key and value cannot be empty".to_string(),
                ));
            }

            return Ok(Some((
                LabelRequirement {
                    key,
                    operator: LabelOperator::Equals,
                    values: Some(vec![value]),
                },
                remaining,
            )));
        }

        // Try = (single character)
        if let Some(pos) = selector_part.find('=') {
            // Make sure it's not part of != or ==
            if pos > 0 && selector_part.as_bytes()[pos - 1] == b'!' {
                return Ok(None);
            }
            if pos + 1 < selector_part.len() && selector_part.as_bytes()[pos + 1] == b'=' {
                return Ok(None);
            }

            let key = selector_part[..pos].trim().to_string();
            let value = selector_part[pos + 1..].trim().to_string();

            if key.is_empty() || value.is_empty() {
                return Err(LabelSelectorError::InvalidSelector(
                    "Key and value cannot be empty".to_string(),
                ));
            }

            return Ok(Some((
                LabelRequirement {
                    key,
                    operator: LabelOperator::Equals,
                    values: Some(vec![value]),
                },
                remaining,
            )));
        }

        Ok(None)
    }

    /// Try to parse an existence-based selector (key or !key)
    fn try_parse_existence_based(
        input: &str,
    ) -> Result<Option<(LabelRequirement, &str)>, LabelSelectorError> {
        // Find the next comma or end of string
        let end_pos = input.find(',').unwrap_or(input.len());
        let selector_part = &input[..end_pos].trim();
        let remaining = if end_pos < input.len() {
            &input[end_pos..]
        } else {
            ""
        };

        // Check if it contains operators (then it's not existence-based)
        if selector_part.contains('=')
            || selector_part.contains(" in ")
            || selector_part.contains(" notin ")
        {
            return Ok(None);
        }

        // Check for negation (!)
        if let Some(key) = selector_part.strip_prefix('!') {
            let key = key.trim();
            if key.is_empty() {
                return Err(LabelSelectorError::InvalidSelector(
                    "Key cannot be empty after '!'".to_string(),
                ));
            }

            return Ok(Some((
                LabelRequirement {
                    key: key.to_string(),
                    operator: LabelOperator::DoesNotExist,
                    values: None,
                },
                remaining,
            )));
        }

        // Plain key existence
        if !selector_part.is_empty() {
            return Ok(Some((
                LabelRequirement {
                    key: selector_part.to_string(),
                    operator: LabelOperator::Exists,
                    values: None,
                },
                remaining,
            )));
        }

        Ok(None)
    }

    /// Check if a resource matches this label selector
    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        // Empty selector matches everything
        if self.requirements.is_empty() {
            return true;
        }

        // All requirements must match
        self.requirements.iter().all(|req| req.matches(labels))
    }

    /// Check if a resource (as JSON) matches this label selector
    pub fn matches_resource(&self, resource: &Value) -> bool {
        // Extract labels from metadata.labels
        let labels = resource
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect::<HashMap<String, String>>()
            })
            .unwrap_or_default();

        self.matches(&labels)
    }

    /// Get the individual requirements
    pub fn requirements(&self) -> &[LabelRequirement] {
        &self.requirements
    }

    /// Check if this is an empty selector
    pub fn is_empty(&self) -> bool {
        self.requirements.is_empty()
    }
}

/// A single label selector requirement
#[derive(Debug, Clone, PartialEq)]
pub struct LabelRequirement {
    /// Label key
    key: String,
    /// Operator
    operator: LabelOperator,
    /// Values (for In/NotIn/Equals/NotEquals operators)
    values: Option<Vec<String>>,
}

impl LabelRequirement {
    /// Check if labels match this requirement
    fn matches(&self, labels: &HashMap<String, String>) -> bool {
        match self.operator {
            LabelOperator::Equals => {
                let expected = self.values.as_ref().and_then(|v| v.first());
                labels.get(&self.key).map(|s| s.as_str()) == expected.map(|s| s.as_str())
            }
            LabelOperator::NotEquals => {
                let expected = self.values.as_ref().and_then(|v| v.first());
                labels.get(&self.key).map(|s| s.as_str()) != expected.map(|s| s.as_str())
            }
            LabelOperator::In => {
                if let Some(value) = labels.get(&self.key) {
                    if let Some(values) = &self.values {
                        values.contains(value)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            LabelOperator::NotIn => {
                if let Some(value) = labels.get(&self.key) {
                    if let Some(values) = &self.values {
                        !values.contains(value)
                    } else {
                        true
                    }
                } else {
                    true
                }
            }
            LabelOperator::Exists => labels.contains_key(&self.key),
            LabelOperator::DoesNotExist => !labels.contains_key(&self.key),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the operator
    pub fn operator(&self) -> LabelOperator {
        self.operator
    }

    /// Get the values
    pub fn values(&self) -> Option<&[String]> {
        self.values.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        key: impl Into<String>,
        operator: LabelOperator,
        values: Option<Vec<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            operator,
            values,
        }
    }
}

/// Label selector operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelOperator {
    /// Equals (= or ==)
    Equals,
    /// Not equals (!=)
    NotEquals,
    /// In (key in (val1, val2))
    In,
    /// Not in (key notin (val1, val2))
    NotIn,
    /// Key exists
    Exists,
    /// Key does not exist
    DoesNotExist,
}

impl fmt::Display for LabelOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LabelOperator::Equals => write!(f, "="),
            LabelOperator::NotEquals => write!(f, "!="),
            LabelOperator::In => write!(f, "in"),
            LabelOperator::NotIn => write!(f, "notin"),
            LabelOperator::Exists => write!(f, "exists"),
            LabelOperator::DoesNotExist => write!(f, "!"),
        }
    }
}

/// Errors that can occur during label selector operations
#[derive(Debug, thiserror::Error)]
pub enum LabelSelectorError {
    #[error("Invalid label selector: {0}")]
    InvalidSelector(String),

    #[error("Invalid operator in label selector: {0}")]
    InvalidOperator(String),
}

impl fmt::Display for LabelSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .requirements
            .iter()
            .map(|req| {
                let first_value = || {
                    req.values
                        .as_ref()
                        .and_then(|v| v.first())
                        .map(String::as_str)
                        .unwrap_or("")
                };
                let joined_values = || {
                    req.values
                        .as_ref()
                        .map(|v| v.join(", "))
                        .unwrap_or_default()
                };
                match req.operator {
                    LabelOperator::Equals => format!("{}={}", req.key, first_value()),
                    LabelOperator::NotEquals => format!("{}!={}", req.key, first_value()),
                    LabelOperator::In => format!("{} in ({})", req.key, joined_values()),
                    LabelOperator::NotIn => format!("{} notin ({})", req.key, joined_values()),
                    LabelOperator::Exists => req.key.clone(),
                    LabelOperator::DoesNotExist => format!("!{}", req.key),
                }
            })
            .collect();
        write!(f, "{}", parts.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_display_does_not_panic_on_missing_values() {
        // A LabelRequirement built without values (e.g. via a code path that
        // bypasses the parser) used to panic in the Display impl. Now it
        // renders as an empty value string.
        for op in [
            LabelOperator::Equals,
            LabelOperator::NotEquals,
            LabelOperator::In,
            LabelOperator::NotIn,
        ] {
            let req = LabelRequirement::new_for_test("app", op, None);
            let sel = LabelSelector {
                requirements: vec![req],
            };
            let _ = format!("{}", sel); // must not panic
        }
    }

    #[test]
    fn test_parse_equality_simple() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "app");
        assert_eq!(selector.requirements[0].operator, LabelOperator::Equals);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["nginx".to_string()])
        );
    }

    #[test]
    fn test_parse_equality_double_equals() {
        let selector = LabelSelector::parse("app==nginx").unwrap();
        assert_eq!(selector.requirements[0].operator, LabelOperator::Equals);
    }

    #[test]
    fn test_parse_not_equals() {
        let selector = LabelSelector::parse("tier!=backend").unwrap();
        assert_eq!(selector.requirements[0].operator, LabelOperator::NotEquals);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["backend".to_string()])
        );
    }

    #[test]
    fn test_parse_multiple_equality() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        assert_eq!(selector.requirements.len(), 2);
        assert_eq!(selector.requirements[0].key, "app");
        assert_eq!(selector.requirements[1].key, "tier");
    }

    #[test]
    fn test_parse_in_operator() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "environment");
        assert_eq!(selector.requirements[0].operator, LabelOperator::In);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["production".to_string(), "qa".to_string()])
        );
    }

    #[test]
    fn test_parse_notin_operator() {
        let selector = LabelSelector::parse("tier notin (frontend,backend)").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].operator, LabelOperator::NotIn);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["frontend".to_string(), "backend".to_string()])
        );
    }

    #[test]
    fn test_parse_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "hasFeature");
        assert_eq!(selector.requirements[0].operator, LabelOperator::Exists);
        assert_eq!(selector.requirements[0].values, None);
    }

    #[test]
    fn test_parse_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "deprecated");
        assert_eq!(
            selector.requirements[0].operator,
            LabelOperator::DoesNotExist
        );
    }

    #[test]
    fn test_parse_mixed() {
        let selector =
            LabelSelector::parse("app=nginx,environment in (production, qa),tier!=backend")
                .unwrap();
        assert_eq!(selector.requirements.len(), 3);
        assert_eq!(selector.requirements[0].operator, LabelOperator::Equals);
        assert_eq!(selector.requirements[1].operator, LabelOperator::In);
        assert_eq!(selector.requirements[2].operator, LabelOperator::NotEquals);
    }

    #[test]
    fn test_matches_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "apache".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_not_equals() {
        let selector = LabelSelector::parse("tier!=backend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("tier".to_string(), "frontend".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("environment".to_string(), "production".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("environment".to_string(), "staging".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_notin() {
        let selector = LabelSelector::parse("tier notin (frontend, backend)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("tier".to_string(), "middleware".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        let mut labels = HashMap::new();
        labels.insert("hasFeature".to_string(), "true".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        let labels = HashMap::new();

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        let labels = HashMap::new();

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        let mut labels = HashMap::new();
        labels.insert("deprecated".to_string(), "true".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_multiple_all_match() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_multiple_one_fails() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "backend".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_resource() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let resource = json!({
            "metadata": {
                "labels": {
                    "app": "nginx",
                    "tier": "frontend"
                }
            }
        });

        assert!(selector.matches_resource(&resource));
    }

    #[test]
    fn test_empty_selector_matches_all() {
        let selector = LabelSelector::parse("").unwrap();
        let labels = HashMap::new();

        assert!(selector.matches(&labels));
        assert!(selector.is_empty());
    }

    #[test]
    fn test_display_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        assert_eq!(format!("{}", selector), "app=nginx");
    }

    #[test]
    fn test_display_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        assert_eq!(format!("{}", selector), "environment in (production, qa)");
    }

    #[test]
    fn test_display_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        assert_eq!(format!("{}", selector), "hasFeature");
    }

    #[test]
    fn test_display_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        assert_eq!(format!("{}", selector), "!deprecated");
    }
}
