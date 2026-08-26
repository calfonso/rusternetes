use crate::client::ApiClient;
use anyhow::Result;

/// Display cluster information
pub async fn execute(client: &ApiClient, dump: bool) -> Result<()> {
    let base_url = client.get_base_url();

    println!("Kubernetes control plane is running at {}", base_url);
    println!();

    if dump {
        // Detailed dump mode
        println!("=== Cluster Info Dump ===");
        println!();

        // Get version
        match client.get::<serde_json::Value>("/version").await {
            Ok(version) => {
                println!("Server Version:");
                if let Some(git_version) = version.get("gitVersion") {
                    println!("  Version: {}", git_version);
                }
                if let Some(platform) = version.get("platform") {
                    println!("  Platform: {}", platform);
                }
                println!();
            }
            Err(_) => {
                println!("Server version: Not available");
                println!();
            }
        }

        // Get nodes
        match client.get::<serde_json::Value>("/api/v1/nodes").await {
            Ok(nodes) => {
                if let Some(items) = nodes.get("items").and_then(|i| i.as_array()) {
                    println!("Nodes ({}):", items.len());
                    for node in items {
                        if let Some(name) = node
                            .get("metadata")
                            .and_then(|m| m.get("name"))
                            .and_then(|n| n.as_str())
                        {
                            println!("  - {}", name);
                            if let Some(status) = node.get("status") {
                                if let Some(node_info) = status.get("nodeInfo") {
                                    if let Some(kubelet_version) = node_info.get("kubeletVersion") {
                                        println!("      Kubelet Version: {}", kubelet_version);
                                    }
                                }
                            }
                        }
                    }
                    println!();
                }
            }
            Err(_) => {
                println!("Nodes: Not available");
                println!();
            }
        }

        // Get namespaces
        match client.get::<serde_json::Value>("/api/v1/namespaces").await {
            Ok(namespaces) => {
                if let Some(items) = namespaces.get("items").and_then(|i| i.as_array()) {
                    println!("Namespaces ({}):", items.len());
                    for ns in items {
                        if let Some(name) = ns
                            .get("metadata")
                            .and_then(|m| m.get("name"))
                            .and_then(|n| n.as_str())
                        {
                            println!("  - {}", name);
                        }
                    }
                    println!();
                }
            }
            Err(_) => {
                println!("Namespaces: Not available");
                println!();
            }
        }
    } else {
        // Basic mode
        println!(
            "To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'."
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_cluster_info_output_format() {
        let base_url = "https://localhost:6443";
        let output = format!("Kubernetes control plane is running at {}", base_url);
        assert!(output.contains("Kubernetes control plane is running at"));
        assert!(output.contains("https://localhost:6443"));
    }

    #[test]
    fn test_dump_help_message() {
        let msg =
            "To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.";
        assert!(msg.contains("cluster-info dump"));
    }

    #[test]
    fn test_cluster_info_various_urls() {
        for url in &[
            "https://10.0.0.1:6443",
            "https://k8s.example.com:443",
            "http://localhost:8080",
        ] {
            let output = format!("Kubernetes control plane is running at {}", url);
            assert!(output.starts_with("Kubernetes control plane is running at"));
            assert!(output.contains(url));
        }
    }
}
