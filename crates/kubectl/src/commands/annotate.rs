use crate::client::ApiClient;
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Update annotations on a resource
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    annotations: &[String],
    _overwrite: bool,
) -> Result<()> {
    // Parse annotations from key=value or key- (removal) format
    let mut annotation_map = HashMap::new();
    for annotation in annotations {
        if let Some(key) = annotation.strip_suffix('-') {
            // Annotation removal - set to null
            annotation_map.insert(key.to_string(), Value::Null);
        } else if let Some((key, value)) = annotation.split_once('=') {
            annotation_map.insert(key.to_string(), Value::String(value.to_string()));
        } else {
            anyhow::bail!(
                "Invalid annotation format: {}. Expected key=value or key-",
                annotation
            );
        }
    }

    // Build API path based on resource type
    let (api_path, resource_name) = match resource_type {
        "pod" | "pods" => ("api/v1", "pods"),
        "service" | "services" | "svc" => ("api/v1", "services"),
        "deployment" | "deployments" | "deploy" => ("apis/apps/v1", "deployments"),
        "daemonset" | "daemonsets" | "ds" => ("apis/apps/v1", "daemonsets"),
        "statefulset" | "statefulsets" | "sts" => ("apis/apps/v1", "statefulsets"),
        "replicaset" | "replicasets" | "rs" => ("apis/apps/v1", "replicasets"),
        "configmap" | "configmaps" | "cm" => ("api/v1", "configmaps"),
        "secret" | "secrets" => ("api/v1", "secrets"),
        "node" | "nodes" => ("api/v1", "nodes"),
        _ => anyhow::bail!("Unsupported resource type: {}", resource_type),
    };

    let path = if resource_name == "nodes" {
        format!("/{}/{}/{}", api_path, resource_name, name)
    } else {
        format!(
            "/{}/namespaces/{}/{}/{}",
            api_path, namespace, resource_name, name
        )
    };

    let patch_body = json!({
        "metadata": {
            "annotations": annotation_map
        }
    });

    // Use merge patch for annotations
    let result: Value = client
        .patch(&path, &patch_body, "application/merge-patch+json")
        .await
        .context("Failed to update annotations")?;

    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use std::collections::HashMap;

    fn parse_annotations(annotations: &[String]) -> Result<HashMap<String, Value>, String> {
        let mut map = HashMap::new();
        for annotation in annotations {
            if let Some(key) = annotation.strip_suffix('-') {
                map.insert(key.to_string(), Value::Null);
            } else if let Some((key, value)) = annotation.split_once('=') {
                map.insert(key.to_string(), Value::String(value.to_string()));
            } else {
                return Err(format!("Invalid annotation format: {}", annotation));
            }
        }
        Ok(map)
    }

    #[test]
    fn test_annotation_patch_set() {
        let annotations = vec!["app.kubernetes.io/name=myapp".to_string()];
        let map = parse_annotations(&annotations).unwrap();

        let patch = json!({"metadata": {"annotations": map}});
        assert_eq!(
            patch["metadata"]["annotations"]["app.kubernetes.io/name"],
            "myapp"
        );
    }

    #[test]
    fn test_annotation_patch_remove() {
        let annotations = vec!["obsolete-key-".to_string()];
        let map = parse_annotations(&annotations).unwrap();

        let patch = json!({"metadata": {"annotations": map}});
        assert!(patch["metadata"]["annotations"]["obsolete-key"].is_null());
    }

    #[test]
    fn test_annotation_patch_mixed() {
        let annotations = vec!["new-key=new-value".to_string(), "old-key-".to_string()];
        let map = parse_annotations(&annotations).unwrap();

        assert_eq!(
            map.get("new-key").unwrap(),
            &Value::String("new-value".to_string())
        );
        assert_eq!(map.get("old-key").unwrap(), &Value::Null);
    }

    #[test]
    fn test_annotation_invalid_format() {
        let annotations = vec!["no-equals-no-dash".to_string()];
        let result = parse_annotations(&annotations);
        assert!(result.is_err());
    }

    #[test]
    fn test_annotation_api_path_construction() {
        // Test that paths are built correctly for different resource types
        let test_cases = vec![
            ("pod", "api/v1", "pods"),
            ("deployment", "apis/apps/v1", "deployments"),
            ("node", "api/v1", "nodes"),
            ("configmap", "api/v1", "configmaps"),
        ];

        for (resource_type, api_path, resource_name) in test_cases {
            let name = "my-resource";
            let namespace = "default";
            let path = if resource_name == "nodes" {
                format!("/{}/{}/{}", api_path, resource_name, name)
            } else {
                format!(
                    "/{}/namespaces/{}/{}/{}",
                    api_path, namespace, resource_name, name
                )
            };

            match resource_type {
                "node" => assert_eq!(path, "/api/v1/nodes/my-resource"),
                "pod" => assert_eq!(path, "/api/v1/namespaces/default/pods/my-resource"),
                "deployment" => assert_eq!(
                    path,
                    "/apis/apps/v1/namespaces/default/deployments/my-resource"
                ),
                "configmap" => {
                    assert_eq!(path, "/api/v1/namespaces/default/configmaps/my-resource")
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_annotation_with_url_value() {
        let annotations = vec!["link=https://example.com/path?q=1".to_string()];
        let map = parse_annotations(&annotations).unwrap();
        // split_once on '=' only splits at the first '='
        assert_eq!(
            map.get("link").unwrap(),
            &Value::String("https://example.com/path?q=1".to_string())
        );
    }

    #[test]
    fn test_annotation_empty_value() {
        let annotations = vec!["key=".to_string()];
        let map = parse_annotations(&annotations).unwrap();
        assert_eq!(map.get("key").unwrap(), &Value::String("".to_string()));
    }

    #[test]
    fn test_annotation_resource_type_mapping() {
        let cases = vec![
            ("svc", "api/v1", "services"),
            ("deploy", "apis/apps/v1", "deployments"),
            ("ds", "apis/apps/v1", "daemonsets"),
            ("sts", "apis/apps/v1", "statefulsets"),
            ("rs", "apis/apps/v1", "replicasets"),
            ("cm", "api/v1", "configmaps"),
            ("secrets", "api/v1", "secrets"),
        ];
        for (input, expected_api, expected_resource) in cases {
            let (api_path, resource_name) = match input {
                "pod" | "pods" => ("api/v1", "pods"),
                "service" | "services" | "svc" => ("api/v1", "services"),
                "deployment" | "deployments" | "deploy" => ("apis/apps/v1", "deployments"),
                "daemonset" | "daemonsets" | "ds" => ("apis/apps/v1", "daemonsets"),
                "statefulset" | "statefulsets" | "sts" => ("apis/apps/v1", "statefulsets"),
                "replicaset" | "replicasets" | "rs" => ("apis/apps/v1", "replicasets"),
                "configmap" | "configmaps" | "cm" => ("api/v1", "configmaps"),
                "secret" | "secrets" => ("api/v1", "secrets"),
                "node" | "nodes" => ("api/v1", "nodes"),
                _ => panic!("unexpected"),
            };
            assert_eq!(api_path, expected_api, "for input '{}'", input);
            assert_eq!(resource_name, expected_resource, "for input '{}'", input);
        }
    }

    #[test]
    fn test_annotation_unsupported_resource_type() {
        let resource_type = "horizontalpodautoscaler";
        let result = match resource_type {
            "pod" | "pods" => Ok(("api/v1", "pods")),
            "service" | "services" | "svc" => Ok(("api/v1", "services")),
            "deployment" | "deployments" | "deploy" => Ok(("apis/apps/v1", "deployments")),
            "daemonset" | "daemonsets" | "ds" => Ok(("apis/apps/v1", "daemonsets")),
            "statefulset" | "statefulsets" | "sts" => Ok(("apis/apps/v1", "statefulsets")),
            "replicaset" | "replicasets" | "rs" => Ok(("apis/apps/v1", "replicasets")),
            "configmap" | "configmaps" | "cm" => Ok(("api/v1", "configmaps")),
            "secret" | "secrets" => Ok(("api/v1", "secrets")),
            "node" | "nodes" => Ok(("api/v1", "nodes")),
            _ => Err(format!("Unsupported resource type: {}", resource_type)),
        };
        assert!(result.is_err());
    }
}
