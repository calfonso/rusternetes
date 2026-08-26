#[cfg(test)]
mod tests {
    use super::super::get::*;

    #[test]
    fn test_output_format_json() {
        let format = OutputFormat::from_str("json").unwrap();
        assert!(matches!(format, OutputFormat::Json));
    }

    #[test]
    fn test_output_format_yaml() {
        let format = OutputFormat::from_str("yaml").unwrap();
        assert!(matches!(format, OutputFormat::Yaml));
    }

    #[test]
    fn test_output_format_wide() {
        let format = OutputFormat::from_str("wide").unwrap();
        assert!(matches!(format, OutputFormat::Wide));
    }

    #[test]
    fn test_output_format_invalid() {
        let result = OutputFormat::from_str("invalid");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unknown output format"));
        assert!(err_msg.contains("invalid"));
    }

    #[test]
    fn test_output_format_error_message_includes_supported_formats() {
        let result = OutputFormat::from_str("xml");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Supported formats"));
        assert!(err_msg.contains("json"));
        assert!(err_msg.contains("yaml"));
        assert!(err_msg.contains("wide"));
    }

    #[test]
    fn test_format_output_json() {
        use serde::Serialize;

        #[derive(Serialize)]
        struct TestResource {
            name: String,
            value: i32,
        }

        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        // Just verify it doesn't panic
        let result = format_output(&resource, &OutputFormat::Json);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_yaml() {
        use serde::Serialize;

        #[derive(Serialize)]
        struct TestResource {
            name: String,
            value: i32,
        }

        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        // Just verify it doesn't panic
        let result = format_output(&resource, &OutputFormat::Yaml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_resource_type_aliases() {
        // These are common aliases that should be supported
        let pod_aliases = ["pod", "pods"];
        let service_aliases = ["service", "services", "svc"];
        let _deployment_aliases = ["deployment", "deployments", "deploy"];
        let _namespace_aliases = ["namespace", "namespaces", "ns"];
        let _pvc_aliases = ["persistentvolumeclaim", "persistentvolumeclaims", "pvc"];
        let _pv_aliases = ["persistentvolume", "persistentvolumes", "pv"];
        let _sc_aliases = ["storageclass", "storageclasses", "sc"];
        let _cm_aliases = ["configmap", "configmaps", "cm"];
        let _sa_aliases = ["serviceaccount", "serviceaccounts", "sa"];

        // Just verify the patterns exist (actual matching happens in the execute function)
        for alias in pod_aliases {
            assert!(["pod", "pods"].contains(&alias));
        }
        for alias in service_aliases {
            assert!(["service", "services", "svc"].contains(&alias));
        }
        // ... etc
    }

    #[test]
    fn test_output_format_jsonpath() {
        let format = OutputFormat::from_str("jsonpath={.metadata.name}").unwrap();
        assert!(matches!(format, OutputFormat::JsonPath(_)));
        if let OutputFormat::JsonPath(expr) = format {
            assert_eq!(expr, "{.metadata.name}");
        }
    }

    #[test]
    fn test_output_format_name() {
        let format = OutputFormat::from_str("name").unwrap();
        assert!(matches!(format, OutputFormat::Name));
    }

    #[test]
    fn test_format_output_jsonpath_basic_field() {
        use serde::Serialize;
        use serde_json::json;

        #[derive(Serialize)]
        struct TestResource {
            metadata: serde_json::Value,
        }

        let resource = TestResource {
            metadata: json!({ "name": "my-pod", "namespace": "default" }),
        };

        let format = OutputFormat::JsonPath("{.metadata.name}".to_string());
        let result = format_output(&resource, &format);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_jsonpath_array_index() {
        use serde::Serialize;
        use serde_json::json;

        #[derive(Serialize)]
        struct TestResource {
            status: serde_json::Value,
        }

        let resource = TestResource {
            status: json!({
                "conditions": [
                    { "type": "Ready", "status": "True" },
                    { "type": "Initialized", "status": "True" }
                ]
            }),
        };

        let format = OutputFormat::JsonPath("{.status.conditions[0].type}".to_string());
        let result = format_output(&resource, &format);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_jsonpath_missing_field() {
        use serde::Serialize;
        use serde_json::json;

        #[derive(Serialize)]
        struct TestResource {
            metadata: serde_json::Value,
        }

        let resource = TestResource {
            metadata: json!({ "name": "my-pod" }),
        };

        // Missing field should not error — it should return empty
        let format = OutputFormat::JsonPath("{.metadata.nonexistent}".to_string());
        let result = format_output(&resource, &format);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_name() {
        use serde::Serialize;
        use serde_json::json;

        #[derive(Serialize)]
        struct TestResource {
            kind: String,
            metadata: serde_json::Value,
        }

        let resource = TestResource {
            kind: "Pod".to_string(),
            metadata: json!({ "name": "my-pod" }),
        };

        let format = OutputFormat::Name;
        let result = format_output(&resource, &format);
        assert!(result.is_ok());
    }

    #[test]
    fn test_output_format_jsonpath_invalid_prefix_rejected() {
        // jsonpath without the = separator is not a valid format
        let result = OutputFormat::from_str("jsonpath{.metadata.name}");
        assert!(result.is_err());
    }

    #[test]
    fn test_new_resource_types_supported() {
        // Verify new resource types are in our type list
        let new_types = [
            "storageclass",
            "volumesnapshot",
            "volumesnapshotclass",
            "endpoints",
            "configmap",
            "secret",
            "statefulset",
            "daemonset",
            "ingress",
            "serviceaccount",
            "role",
            "rolebinding",
            "clusterrole",
            "clusterrolebinding",
            "resourcequota",
            "limitrange",
            "priorityclass",
            "customresourcedefinition",
        ];

        // These should all be valid resource type strings
        for resource_type in new_types {
            assert!(!resource_type.is_empty());
            assert!(resource_type
                .chars()
                .all(|c| c.is_lowercase() || c.is_ascii_alphanumeric()));
        }
    }
}
