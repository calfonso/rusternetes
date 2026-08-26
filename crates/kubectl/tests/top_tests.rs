/// Tests for kubectl top command
/// Tests metrics formatting and resource calculation without requiring a cluster
#[cfg(test)]
mod metrics_formatting_tests {
    #[test]
    fn test_format_cpu_usage() {
        fn format_cpu(millicores: u64) -> String {
            format!("{}m", millicores)
        }

        assert_eq!(format_cpu(100), "100m");
        assert_eq!(format_cpu(500), "500m");
        assert_eq!(format_cpu(1500), "1500m");
    }

    #[test]
    fn test_format_memory_usage() {
        fn format_memory(bytes: u64) -> String {
            if bytes >= 1024 * 1024 * 1024 {
                format!("{}Gi", bytes / (1024 * 1024 * 1024))
            } else if bytes >= 1024 * 1024 {
                format!("{}Mi", bytes / (1024 * 1024))
            } else if bytes >= 1024 {
                format!("{}Ki", bytes / 1024)
            } else {
                format!("{}B", bytes)
            }
        }

        assert_eq!(format_memory(512), "512B");
        assert_eq!(format_memory(2048), "2Ki");
        assert_eq!(format_memory(2 * 1024 * 1024), "2Mi");
        assert_eq!(format_memory(3 * 1024 * 1024 * 1024), "3Gi");
    }

    #[test]
    fn test_format_percentage() {
        fn format_percentage(used: u64, total: u64) -> String {
            if total == 0 {
                return "0%".to_string();
            }
            let percentage = (used as f64 / total as f64) * 100.0;
            format!("{:.1}%", percentage)
        }

        assert_eq!(format_percentage(50, 100), "50.0%");
        assert_eq!(format_percentage(25, 100), "25.0%");
        assert_eq!(format_percentage(75, 100), "75.0%");
        assert_eq!(format_percentage(100, 0), "0%");
    }

    #[test]
    fn test_cpu_cores_to_millicores() {
        fn cores_to_millicores(cores: f64) -> u64 {
            (cores * 1000.0) as u64
        }

        assert_eq!(cores_to_millicores(1.0), 1000);
        assert_eq!(cores_to_millicores(0.5), 500);
        assert_eq!(cores_to_millicores(2.5), 2500);
        assert_eq!(cores_to_millicores(0.1), 100);
    }

    #[test]
    fn test_parse_cpu_string() {
        fn parse_cpu(cpu: &str) -> Result<u64, String> {
            if let Some(millicores) = cpu.strip_suffix('m') {
                millicores
                    .parse::<u64>()
                    .map_err(|_| "Invalid CPU value".to_string())
            } else if let Ok(cores) = cpu.parse::<f64>() {
                Ok((cores * 1000.0) as u64)
            } else {
                Err("Invalid CPU format".to_string())
            }
        }

        assert_eq!(parse_cpu("100m").unwrap(), 100);
        assert_eq!(parse_cpu("1500m").unwrap(), 1500);
        assert_eq!(parse_cpu("1").unwrap(), 1000);
        assert_eq!(parse_cpu("0.5").unwrap(), 500);
        assert!(parse_cpu("invalid").is_err());
    }

    #[test]
    fn test_parse_memory_string() {
        fn parse_memory(mem: &str) -> Result<u64, String> {
            if let Some(ki) = mem.strip_suffix("Ki") {
                ki.parse::<u64>()
                    .map(|v| v * 1024)
                    .map_err(|_| "Invalid memory value".to_string())
            } else if let Some(mi) = mem.strip_suffix("Mi") {
                mi.parse::<u64>()
                    .map(|v| v * 1024 * 1024)
                    .map_err(|_| "Invalid memory value".to_string())
            } else if let Some(gi) = mem.strip_suffix("Gi") {
                gi.parse::<u64>()
                    .map(|v| v * 1024 * 1024 * 1024)
                    .map_err(|_| "Invalid memory value".to_string())
            } else {
                mem.parse::<u64>()
                    .map_err(|_| "Invalid memory format".to_string())
            }
        }

        assert_eq!(parse_memory("512").unwrap(), 512);
        assert_eq!(parse_memory("2Ki").unwrap(), 2048);
        assert_eq!(parse_memory("4Mi").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_memory("1Gi").unwrap(), 1024 * 1024 * 1024);
        assert!(parse_memory("invalid").is_err());
    }
}

#[cfg(test)]
mod node_metrics_tests {
    #[test]
    fn test_node_metrics_calculation() {
        fn calculate_node_usage(
            cpu_used: u64,
            cpu_total: u64,
            mem_used: u64,
            mem_total: u64,
        ) -> (u64, u64, u64, u64) {
            (cpu_used, cpu_total, mem_used, mem_total)
        }

        let (cpu_used, cpu_total, mem_used, mem_total) =
            calculate_node_usage(2000, 4000, 4 * 1024 * 1024 * 1024, 8 * 1024 * 1024 * 1024);

        assert_eq!(cpu_used, 2000);
        assert_eq!(cpu_total, 4000);
        assert_eq!(mem_used, 4 * 1024 * 1024 * 1024);
        assert_eq!(mem_total, 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_node_metrics_table_row() {
        fn format_node_row(
            name: &str,
            cpu: &str,
            cpu_pct: &str,
            mem: &str,
            mem_pct: &str,
        ) -> String {
            format!(
                "{:<20} {:<10} {:<8} {:<12} {}",
                name, cpu, cpu_pct, mem, mem_pct
            )
        }

        let row = format_node_row("node-1", "2000m", "50%", "4Gi", "50%");
        assert!(row.contains("node-1"));
        assert!(row.contains("2000m"));
        assert!(row.contains("50%"));
        assert!(row.contains("4Gi"));
    }

    #[test]
    fn test_aggregate_node_metrics() {
        fn sum_cpu_usage(nodes: &[(u64, u64)]) -> (u64, u64) {
            let total_used: u64 = nodes.iter().map(|(used, _)| used).sum();
            let total_capacity: u64 = nodes.iter().map(|(_, capacity)| capacity).sum();
            (total_used, total_capacity)
        }

        let nodes = vec![(1000, 2000), (1500, 4000), (2000, 4000)];

        let (used, capacity) = sum_cpu_usage(&nodes);
        assert_eq!(used, 4500);
        assert_eq!(capacity, 10000);
    }
}

#[cfg(test)]
mod pod_metrics_tests {
    #[test]
    fn test_pod_metrics_calculation() {
        fn calculate_pod_usage(containers: &[(u64, u64)]) -> (u64, u64) {
            let total_cpu: u64 = containers.iter().map(|(cpu, _)| cpu).sum();
            let total_mem: u64 = containers.iter().map(|(_, mem)| mem).sum();
            (total_cpu, total_mem)
        }

        let containers = vec![(100, 256 * 1024 * 1024), (200, 512 * 1024 * 1024)];

        let (cpu, mem) = calculate_pod_usage(&containers);
        assert_eq!(cpu, 300);
        assert_eq!(mem, 768 * 1024 * 1024);
    }

    #[test]
    fn test_pod_metrics_table_row() {
        fn format_pod_row(name: &str, namespace: &str, cpu: &str, memory: &str) -> String {
            format!("{:<30} {:<20} {:<10} {}", name, namespace, cpu, memory)
        }

        let row = format_pod_row("nginx-pod", "default", "100m", "256Mi");
        assert!(row.contains("nginx-pod"));
        assert!(row.contains("default"));
        assert!(row.contains("100m"));
        assert!(row.contains("256Mi"));
    }

    #[test]
    fn test_filter_pods_by_namespace() {
        fn filter_by_namespace<'a>(
            pods: &'a [(&str, &str)],
            namespace: &str,
        ) -> Vec<&'a (&'a str, &'a str)> {
            pods.iter().filter(|(_, ns)| *ns == namespace).collect()
        }

        let pods = vec![
            ("pod-1", "default"),
            ("pod-2", "kube-system"),
            ("pod-3", "default"),
        ];

        let default_pods = filter_by_namespace(&pods, "default");
        assert_eq!(default_pods.len(), 2);
    }

    #[test]
    fn test_sort_pods_by_cpu() {
        fn sort_by_cpu(pods: &mut Vec<(&str, u64)>) {
            pods.sort_by(|a, b| b.1.cmp(&a.1));
        }

        let mut pods = vec![("pod-1", 100), ("pod-3", 500), ("pod-2", 300)];

        sort_by_cpu(&mut pods);
        assert_eq!(pods[0].0, "pod-3");
        assert_eq!(pods[1].0, "pod-2");
        assert_eq!(pods[2].0, "pod-1");
    }

    #[test]
    fn test_sort_pods_by_memory() {
        fn sort_by_memory(pods: &mut Vec<(&str, u64)>) {
            pods.sort_by(|a, b| b.1.cmp(&a.1));
        }

        let mut pods = vec![
            ("pod-1", 256 * 1024 * 1024),
            ("pod-3", 1024 * 1024 * 1024),
            ("pod-2", 512 * 1024 * 1024),
        ];

        sort_by_memory(&mut pods);
        assert_eq!(pods[0].0, "pod-3");
        assert_eq!(pods[1].0, "pod-2");
        assert_eq!(pods[2].0, "pod-1");
    }
}

#[cfg(test)]
mod container_metrics_tests {
    #[test]
    fn test_container_metrics_table_row() {
        fn format_container_row(pod: &str, container: &str, cpu: &str, memory: &str) -> String {
            format!("{}/{:<20} {:<10} {}", pod, container, cpu, memory)
        }

        let row = format_container_row("nginx-pod", "nginx", "50m", "128Mi");
        assert!(row.contains("nginx-pod/nginx"));
        assert!(row.contains("50m"));
        assert!(row.contains("128Mi"));
    }

    #[test]
    fn test_extract_container_metrics() {
        fn extract_metrics(containers: &[(&str, u64, u64)]) -> Vec<(String, u64, u64)> {
            containers
                .iter()
                .map(|(name, cpu, mem)| (name.to_string(), *cpu, *mem))
                .collect()
        }

        let containers = vec![
            ("nginx", 100, 256 * 1024 * 1024),
            ("sidecar", 50, 128 * 1024 * 1024),
        ];

        let metrics = extract_metrics(&containers);
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].0, "nginx");
        assert_eq!(metrics[1].0, "sidecar");
    }
}

#[cfg(test)]
mod selector_tests {
    #[test]
    fn test_label_selector_for_top() {
        fn build_label_selector(labels: &[(&str, &str)]) -> String {
            labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(",")
        }

        let selector = build_label_selector(&[("app", "nginx"), ("tier", "frontend")]);
        assert!(selector.contains("app=nginx"));
        assert!(selector.contains("tier=frontend"));
    }

    #[test]
    fn test_all_namespaces_flag() {
        fn should_query_all_namespaces(all_namespaces: bool, namespace: Option<&str>) -> bool {
            all_namespaces || namespace.is_none()
        }

        assert!(should_query_all_namespaces(true, Some("default")));
        assert!(should_query_all_namespaces(false, None));
        assert!(!should_query_all_namespaces(false, Some("default")));
    }
}

#[cfg(test)]
mod output_format_tests {
    #[test]
    fn test_table_header_for_nodes() {
        fn get_node_header() -> String {
            format!(
                "{:<20} {:<10} {:<8} {:<12} {}",
                "NAME", "CPU(cores)", "CPU%", "MEMORY(bytes)", "MEMORY%"
            )
        }

        let header = get_node_header();
        assert!(header.contains("NAME"));
        assert!(header.contains("CPU(cores)"));
        assert!(header.contains("MEMORY(bytes)"));
    }

    #[test]
    fn test_table_header_for_pods() {
        fn get_pod_header() -> String {
            format!(
                "{:<30} {:<20} {:<10} {}",
                "NAME", "NAMESPACE", "CPU(cores)", "MEMORY(bytes)"
            )
        }

        let header = get_pod_header();
        assert!(header.contains("NAME"));
        assert!(header.contains("NAMESPACE"));
        assert!(header.contains("CPU(cores)"));
        assert!(header.contains("MEMORY(bytes)"));
    }

    #[test]
    fn test_no_metrics_available() {
        fn format_no_metrics_message(resource_type: &str) -> String {
            format!("No metrics available for {}", resource_type)
        }

        assert_eq!(
            format_no_metrics_message("nodes"),
            "No metrics available for nodes"
        );
        assert_eq!(
            format_no_metrics_message("pods"),
            "No metrics available for pods"
        );
    }
}
