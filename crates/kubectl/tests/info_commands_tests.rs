//! Tests for kubectl informational commands
//! Tests version, cluster-info, api-resources, api-versions, and explain commands

#[cfg(test)]
mod version_tests {
    #[test]
    fn test_version_string_formatting() {
        fn format_version(major: &str, minor: &str, git_version: &str) -> String {
            format!("v{}.{} ({})", major, minor, git_version)
        }

        assert_eq!(format_version("1", "35", "v1.35.0"), "v1.35 (v1.35.0)");

        assert_eq!(format_version("1", "34", "v1.34.2"), "v1.34 (v1.34.2)");
    }

    #[test]
    fn test_version_info_display() {
        fn format_client_version(version: &str, commit: &str, build_date: &str) -> Vec<String> {
            vec![
                format!("Client Version: {}", version),
                format!("Git Commit: {}", commit),
                format!("Build Date: {}", build_date),
            ]
        }

        let info = format_client_version("v1.35.0", "abc123", "2024-01-01");
        assert_eq!(info[0], "Client Version: v1.35.0");
        assert_eq!(info[1], "Git Commit: abc123");
        assert_eq!(info[2], "Build Date: 2024-01-01");
    }

    #[test]
    fn test_version_output_format() {
        fn get_output_format(output: Option<&str>) -> &str {
            output.unwrap_or("text")
        }

        assert_eq!(get_output_format(Some("json")), "json");
        assert_eq!(get_output_format(Some("yaml")), "yaml");
        assert_eq!(get_output_format(None), "text");
    }

    #[test]
    fn test_short_version_flag() {
        fn format_short_version(version: &str, short: bool) -> String {
            if short {
                version.to_string()
            } else {
                format!(
                    "Version: {}\nCommit: abc123\nBuild Date: 2024-01-01",
                    version
                )
            }
        }

        assert_eq!(format_short_version("v1.35.0", true), "v1.35.0");
        assert!(format_short_version("v1.35.0", false).contains("Commit"));
    }
}

#[cfg(test)]
mod cluster_info_tests {
    #[test]
    fn test_cluster_info_formatting() {
        fn format_cluster_info(name: &str, server: &str) -> String {
            format!("Cluster: {}\nServer: {}", name, server)
        }

        let info = format_cluster_info("rusternetes", "https://localhost:6443");
        assert!(info.contains("Cluster: rusternetes"));
        assert!(info.contains("Server: https://localhost:6443"));
    }

    #[test]
    fn test_component_status_formatting() {
        fn format_component_status(name: &str, healthy: bool) -> String {
            let status = if healthy { "Healthy" } else { "Unhealthy" };
            format!("{}: {}", name, status)
        }

        assert_eq!(format_component_status("etcd-0", true), "etcd-0: Healthy");
        assert_eq!(
            format_component_status("controller-manager", false),
            "controller-manager: Unhealthy"
        );
    }

    #[test]
    fn test_cluster_dump_flag() {
        fn should_dump_cluster_info(dump: bool) -> bool {
            dump
        }

        assert!(should_dump_cluster_info(true));
        assert!(!should_dump_cluster_info(false));
    }

    #[test]
    fn test_cluster_endpoint_parsing() {
        fn parse_server_url(url: &str) -> Result<(&str, u16), String> {
            if let Some(stripped) = url.strip_prefix("https://") {
                if let Some((host, port)) = stripped.split_once(':') {
                    let port_num = port.parse::<u16>().map_err(|_| "Invalid port")?;
                    Ok((host, port_num))
                } else {
                    Ok((stripped, 443)) // Default HTTPS port
                }
            } else if let Some(stripped) = url.strip_prefix("http://") {
                if let Some((host, port)) = stripped.split_once(':') {
                    let port_num = port.parse::<u16>().map_err(|_| "Invalid port")?;
                    Ok((host, port_num))
                } else {
                    Ok((stripped, 80)) // Default HTTP port
                }
            } else {
                Err("Invalid URL scheme".to_string())
            }
        }

        assert_eq!(
            parse_server_url("https://localhost:6443").unwrap(),
            ("localhost", 6443)
        );
        assert_eq!(
            parse_server_url("https://api.example.com").unwrap(),
            ("api.example.com", 443)
        );
        assert_eq!(
            parse_server_url("http://localhost:8080").unwrap(),
            ("localhost", 8080)
        );
        assert!(parse_server_url("ftp://invalid").is_err());
    }
}

#[cfg(test)]
mod api_resources_tests {
    #[test]
    fn test_api_resource_formatting() {
        fn format_api_resource(
            name: &str,
            shortnames: &[&str],
            api_version: &str,
            namespaced: bool,
            kind: &str,
        ) -> String {
            let short = if shortnames.is_empty() {
                String::new()
            } else {
                shortnames.join(",")
            };
            let scope = if namespaced { "Namespaced" } else { "Cluster" };

            format!(
                "{:<30} {:<15} {:<20} {} {}",
                name, short, api_version, scope, kind
            )
        }

        let resource = format_api_resource("pods", &["po"], "v1", true, "Pod");
        assert!(resource.contains("pods"));
        assert!(resource.contains("po"));
        assert!(resource.contains("v1"));
        assert!(resource.contains("Namespaced"));
        assert!(resource.contains("Pod"));
    }

    #[test]
    fn test_namespaced_flag() {
        fn filter_namespaced<'a>(
            resources: &'a [(&'a str, bool)],
            namespaced: Option<bool>,
        ) -> Vec<&'a str> {
            match namespaced {
                Some(true) => resources
                    .iter()
                    .filter(|(_, ns)| *ns)
                    .map(|(name, _)| *name)
                    .collect(),
                Some(false) => resources
                    .iter()
                    .filter(|(_, ns)| !*ns)
                    .map(|(name, _)| *name)
                    .collect(),
                None => resources.iter().map(|(name, _)| *name).collect(),
            }
        }

        let resources = vec![
            ("pods", true),
            ("nodes", false),
            ("services", true),
            ("namespaces", false),
        ];

        let namespaced = filter_namespaced(&resources, Some(true));
        assert_eq!(namespaced.len(), 2);
        assert!(namespaced.contains(&"pods"));

        let cluster_scoped = filter_namespaced(&resources, Some(false));
        assert_eq!(cluster_scoped.len(), 2);
        assert!(cluster_scoped.contains(&"nodes"));

        let all = filter_namespaced(&resources, None);
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn test_api_group_filtering() {
        fn filter_by_api_group<'a>(
            resources: &'a [(&str, &str)],
            api_group: Option<&str>,
        ) -> Vec<&'a str> {
            match api_group {
                Some(group) => resources
                    .iter()
                    .filter(|(_, g)| *g == group)
                    .map(|(name, _)| *name)
                    .collect(),
                None => resources.iter().map(|(name, _)| *name).collect(),
            }
        }

        let resources = vec![
            ("pods", "v1"),
            ("deployments", "apps/v1"),
            ("services", "v1"),
            ("daemonsets", "apps/v1"),
        ];

        let core_resources = filter_by_api_group(&resources, Some("v1"));
        assert_eq!(core_resources.len(), 2);

        let apps_resources = filter_by_api_group(&resources, Some("apps/v1"));
        assert_eq!(apps_resources.len(), 2);
    }

    #[test]
    fn test_verbs_formatting() {
        fn format_verbs(verbs: &[&str]) -> String {
            verbs.join(", ")
        }

        assert_eq!(
            format_verbs(&["get", "list", "watch", "create", "update", "patch", "delete"]),
            "get, list, watch, create, update, patch, delete"
        );

        assert_eq!(format_verbs(&["get", "list", "watch"]), "get, list, watch");
    }

    #[test]
    fn test_categories_formatting() {
        fn format_categories(categories: &[&str]) -> String {
            if categories.is_empty() {
                "<none>".to_string()
            } else {
                categories.join(",")
            }
        }

        assert_eq!(format_categories(&["all"]), "all");
        assert_eq!(format_categories(&[]), "<none>");
    }
}

#[cfg(test)]
mod api_versions_tests {
    #[test]
    fn test_api_version_listing() {
        fn format_api_versions(versions: &[&str]) -> Vec<String> {
            versions.iter().map(|v| v.to_string()).collect()
        }

        let versions = format_api_versions(&["v1", "apps/v1", "batch/v1", "networking.k8s.io/v1"]);
        assert_eq!(versions.len(), 4);
        assert!(versions.contains(&"v1".to_string()));
        assert!(versions.contains(&"apps/v1".to_string()));
    }

    #[test]
    fn test_preferred_version_selection() {
        fn get_preferred_version<'a>(versions: &'a [&'a str]) -> &'a str {
            // Prefer stable versions over beta/alpha
            for v in versions {
                if !v.contains("beta") && !v.contains("alpha") {
                    return v;
                }
            }
            // Prefer beta over alpha
            for v in versions {
                if v.contains("beta") {
                    return v;
                }
            }
            versions.first().unwrap_or(&"")
        }

        assert_eq!(get_preferred_version(&["v1beta1", "v1beta2", "v1"]), "v1");

        assert_eq!(get_preferred_version(&["v1alpha1", "v1beta1"]), "v1beta1");
    }

    #[test]
    fn test_group_version_parsing() {
        fn parse_group_version(api_version: &str) -> (&str, &str) {
            if let Some((group, version)) = api_version.split_once('/') {
                (group, version)
            } else {
                ("", api_version)
            }
        }

        assert_eq!(parse_group_version("apps/v1"), ("apps", "v1"));
        assert_eq!(parse_group_version("v1"), ("", "v1"));
        assert_eq!(
            parse_group_version("networking.k8s.io/v1"),
            ("networking.k8s.io", "v1")
        );
    }
}

#[cfg(test)]
mod explain_tests {
    #[test]
    fn test_resource_path_parsing() {
        fn parse_resource_path(path: &str) -> Vec<&str> {
            path.split('.').collect()
        }

        let parts = parse_resource_path("pod.spec.containers");
        assert_eq!(parts, vec!["pod", "spec", "containers"]);

        let parts = parse_resource_path("deployment.metadata.labels");
        assert_eq!(parts, vec!["deployment", "metadata", "labels"]);
    }

    #[test]
    fn test_field_description_formatting() {
        fn format_field_description(
            field: &str,
            field_type: &str,
            description: &str,
            required: bool,
        ) -> String {
            let req = if required { "(required)" } else { "(optional)" };
            format!("{} <{}> {}\n  {}", field, field_type, req, description)
        }

        let desc = format_field_description("replicas", "integer", "Number of desired pods", false);

        assert!(desc.contains("replicas"));
        assert!(desc.contains("integer"));
        assert!(desc.contains("optional"));
        assert!(desc.contains("Number of desired pods"));
    }

    #[test]
    fn test_recursive_flag() {
        fn should_show_recursive(recursive: bool) -> bool {
            recursive
        }

        assert!(should_show_recursive(true));
        assert!(!should_show_recursive(false));
    }

    #[test]
    fn test_api_version_for_explain() {
        fn format_explain_header(kind: &str, version: &str, group: &str) -> String {
            if group.is_empty() {
                format!("KIND:     {}\nVERSION:  {}", kind, version)
            } else {
                format!("KIND:     {}\nVERSION:  {}/{}", kind, group, version)
            }
        }

        let header = format_explain_header("Pod", "v1", "");
        assert!(header.contains("KIND:     Pod"));
        assert!(header.contains("VERSION:  v1"));

        let header = format_explain_header("Deployment", "v1", "apps");
        assert!(header.contains("VERSION:  apps/v1"));
    }

    #[test]
    fn test_field_type_formatting() {
        fn format_field_type(field_type: &str, array: bool) -> String {
            if array {
                format!("[]<{}>", field_type)
            } else {
                format!("<{}>", field_type)
            }
        }

        assert_eq!(format_field_type("string", false), "<string>");
        assert_eq!(format_field_type("Container", true), "[]<Container>");
    }
}

#[cfg(test)]
mod options_tests {
    #[test]
    fn test_global_options_display() {
        fn get_global_options() -> Vec<String> {
            vec![
                "--kubeconfig".to_string(),
                "--context".to_string(),
                "--namespace".to_string(),
                "--server".to_string(),
            ]
        }

        let options = get_global_options();
        assert!(options.contains(&"--kubeconfig".to_string()));
        assert!(options.contains(&"--context".to_string()));
    }

    #[test]
    fn test_help_text_formatting() {
        fn format_option_help(flag: &str, description: &str) -> String {
            format!("  {:<20} {}", flag, description)
        }

        let help = format_option_help("--namespace", "Specify namespace");
        assert!(help.contains("--namespace"));
        assert!(help.contains("Specify namespace"));
    }
}
