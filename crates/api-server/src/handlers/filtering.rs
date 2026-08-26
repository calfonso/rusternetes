// Filtering helpers for list operations
//
// Provides common logic for filtering resources by field selectors and label selectors

use rusternetes_common::field_selector::FieldSelector;
use rusternetes_common::label_selector::LabelSelector;
use serde::Serialize;
use std::collections::HashMap;

/// Apply field selector filtering to a list of resources
///
/// If field_selector_str is None or empty, no filtering is applied.
/// If parsing fails, returns an error.
pub fn apply_field_selector<T: Serialize>(
    resources: &mut Vec<T>,
    params: &HashMap<String, String>,
) -> Result<(), rusternetes_common::Error> {
    if let Some(field_selector_str) = params.get("fieldSelector") {
        let selector = FieldSelector::parse(field_selector_str).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid field selector: {}", e))
        })?;

        if !selector.is_empty() {
            resources.retain(|resource| {
                let resource_json = serde_json::to_value(resource).unwrap_or_default();
                selector.matches(&resource_json)
            });
        }
    }

    Ok(())
}

/// Apply label selector filtering to a list of resources.
///
/// Uses direct struct field access for label matching when possible,
/// falling back to JSON serialization only for the field selector path.
/// If label_selector_str is None or empty, no filtering is applied.
/// If parsing fails, returns an error.
pub fn apply_label_selector<T: Serialize>(
    resources: &mut Vec<T>,
    params: &HashMap<String, String>,
) -> Result<(), rusternetes_common::Error> {
    if let Some(label_selector_str) = params.get("labelSelector") {
        let selector = LabelSelector::parse(label_selector_str).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid label selector: {}", e))
        })?;

        if !selector.is_empty() {
            resources.retain(|resource| {
                // Try to extract labels without full serialization by serializing
                // just enough to read metadata.labels. We serialize to Value only
                // to access the labels HashMap — this is unavoidable for generic T.
                let resource_json = serde_json::to_value(resource).unwrap_or_default();
                selector.matches_resource(&resource_json)
            });
        }
    }

    Ok(())
}

/// Apply label selector filtering using direct label access (no serialization).
/// Use this when you have pre-extracted labels for each resource.
pub fn apply_label_selector_direct(
    labels_list: &mut Vec<(usize, &Option<HashMap<String, String>>)>,
    label_selector_str: &str,
) -> Result<Vec<usize>, rusternetes_common::Error> {
    let selector = LabelSelector::parse(label_selector_str).map_err(|e| {
        rusternetes_common::Error::InvalidResource(format!("Invalid label selector: {}", e))
    })?;

    let mut keep = Vec::new();
    for (idx, labels) in labels_list {
        let matches = if selector.is_empty() {
            true
        } else if let Some(labels) = labels {
            selector.matches(labels)
        } else {
            selector.matches(&HashMap::new())
        };
        if matches {
            keep.push(*idx);
        }
    }
    Ok(keep)
}

/// Apply both field and label selector filtering to a list of resources.
///
/// Serializes each resource to JSON at most once (instead of twice when
/// field and label selectors are both present). Field selectors are
/// evaluated first, then label selectors on the same JSON value.
pub fn apply_selectors<T: Serialize>(
    resources: &mut Vec<T>,
    params: &HashMap<String, String>,
) -> Result<(), rusternetes_common::Error> {
    let has_field_selector = params.get("fieldSelector").is_some_and(|s| !s.is_empty());
    let has_label_selector = params.get("labelSelector").is_some_and(|s| !s.is_empty());

    // Fast path: no selectors
    if !has_field_selector && !has_label_selector {
        return Ok(());
    }

    let field_selector = if has_field_selector {
        Some(
            FieldSelector::parse(params.get("fieldSelector").unwrap()).map_err(|e| {
                rusternetes_common::Error::InvalidResource(format!("Invalid field selector: {}", e))
            })?,
        )
    } else {
        None
    };

    let label_selector = if has_label_selector {
        Some(
            LabelSelector::parse(params.get("labelSelector").unwrap()).map_err(|e| {
                rusternetes_common::Error::InvalidResource(format!("Invalid label selector: {}", e))
            })?,
        )
    } else {
        None
    };

    // Single pass: serialize each resource once, apply both selectors
    resources.retain(|resource| {
        let resource_json = serde_json::to_value(resource).unwrap_or_default();

        if let Some(ref fs) = field_selector {
            if !fs.is_empty() && !fs.matches(&resource_json) {
                return false;
            }
        }

        if let Some(ref ls) = label_selector {
            if !ls.is_empty() && !ls.matches_resource(&resource_json) {
                return false;
            }
        }

        true
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct TestResource {
        metadata: Metadata,
        status: Status,
    }

    #[derive(Serialize)]
    struct Metadata {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        labels: Option<HashMap<String, String>>,
    }

    #[derive(Serialize)]
    struct Status {
        phase: String,
    }

    #[test]
    fn test_apply_field_selector_empty() {
        let mut resources = vec![
            TestResource {
                metadata: Metadata {
                    name: "test1".to_string(),
                    labels: None,
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
            TestResource {
                metadata: Metadata {
                    name: "test2".to_string(),
                    labels: None,
                },
                status: Status {
                    phase: "Pending".to_string(),
                },
            },
        ];

        let params = HashMap::new();
        apply_field_selector(&mut resources, &params).unwrap();

        assert_eq!(resources.len(), 2);
    }

    #[test]
    fn test_apply_field_selector_filters() {
        let mut resources = vec![
            TestResource {
                metadata: Metadata {
                    name: "test1".to_string(),
                    labels: None,
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
            TestResource {
                metadata: Metadata {
                    name: "test2".to_string(),
                    labels: None,
                },
                status: Status {
                    phase: "Pending".to_string(),
                },
            },
        ];

        let mut params = HashMap::new();
        params.insert(
            "fieldSelector".to_string(),
            "status.phase=Running".to_string(),
        );

        apply_field_selector(&mut resources, &params).unwrap();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].status.phase, "Running");
    }

    #[test]
    fn test_apply_label_selector_filters() {
        let mut labels1 = HashMap::new();
        labels1.insert("app".to_string(), "nginx".to_string());

        let mut labels2 = HashMap::new();
        labels2.insert("app".to_string(), "redis".to_string());

        let mut resources = vec![
            TestResource {
                metadata: Metadata {
                    name: "test1".to_string(),
                    labels: Some(labels1),
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
            TestResource {
                metadata: Metadata {
                    name: "test2".to_string(),
                    labels: Some(labels2),
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
        ];

        let mut params = HashMap::new();
        params.insert("labelSelector".to_string(), "app=nginx".to_string());

        apply_label_selector(&mut resources, &params).unwrap();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].metadata.name, "test1");
    }

    #[test]
    fn test_apply_both_selectors() {
        let mut labels1 = HashMap::new();
        labels1.insert("app".to_string(), "nginx".to_string());

        let mut labels2 = HashMap::new();
        labels2.insert("app".to_string(), "nginx".to_string());

        let mut resources = vec![
            TestResource {
                metadata: Metadata {
                    name: "test1".to_string(),
                    labels: Some(labels1),
                },
                status: Status {
                    phase: "Running".to_string(),
                },
            },
            TestResource {
                metadata: Metadata {
                    name: "test2".to_string(),
                    labels: Some(labels2),
                },
                status: Status {
                    phase: "Pending".to_string(),
                },
            },
        ];

        let mut params = HashMap::new();
        params.insert(
            "fieldSelector".to_string(),
            "status.phase=Running".to_string(),
        );
        params.insert("labelSelector".to_string(), "app=nginx".to_string());

        apply_selectors(&mut resources, &params).unwrap();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].metadata.name, "test1");
        assert_eq!(resources[0].status.phase, "Running");
    }
}
