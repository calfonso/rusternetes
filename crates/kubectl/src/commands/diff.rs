use crate::client::{ApiClient, GetError};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::io::{self, Read};

/// Show diff between current and applied configuration
pub async fn execute(client: &ApiClient, file: &str, namespace: &str) -> Result<()> {
    let contents = if file == "-" {
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .context("Failed to read from stdin")?;
        buffer
    } else {
        fs::read_to_string(file).context("Failed to read file")?
    };

    // Support for multi-document YAML files
    for document in serde_yaml::Deserializer::from_str(&contents) {
        let value = serde_yaml::Value::deserialize(document)?;

        if value.is_null() {
            continue;
        }

        diff_resource(client, &value, namespace).await?;
    }

    Ok(())
}

async fn diff_resource(
    client: &ApiClient,
    value: &serde_yaml::Value,
    default_namespace: &str,
) -> Result<()> {
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field")?;

    let metadata = value.get("metadata").context("Missing 'metadata' field")?;
    let name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .context("Missing 'name' in metadata")?;

    let namespace = metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .unwrap_or(default_namespace);

    // Construct API path based on resource kind
    let api_path = get_resource_api_path(kind, namespace, name)?;

    // Get current resource
    let current_yaml = match client.get::<serde_json::Value>(&api_path).await {
        Ok(current) => {
            // Convert to YAML for diffing
            serde_yaml::to_string(&current)?
        }
        Err(GetError::NotFound) => {
            println!("--- /dev/null");
            println!("+++ {}/{} (create)", kind, name);
            let new_yaml = serde_yaml::to_string(value)?;
            for line in new_yaml.lines() {
                println!("+{}", line);
            }
            println!();
            return Ok(());
        }
        Err(GetError::Other(e)) => {
            return Err(e);
        }
    };

    // Prepare new resource YAML
    let new_yaml = serde_yaml::to_string(value)?;

    // Calculate and display diff
    if current_yaml.trim() == new_yaml.trim() {
        println!("No changes for {}/{}", kind, name);
        println!();
        return Ok(());
    }

    println!("--- {}/{} (current)", kind, name);
    println!("+++ {}/{} (new)", kind, name);

    // Simple line-by-line diff
    let current_lines: Vec<&str> = current_yaml.lines().collect();
    let new_lines: Vec<&str> = new_yaml.lines().collect();

    let max_len = current_lines.len().max(new_lines.len());
    for i in 0..max_len {
        let current_line = current_lines.get(i).copied();
        let new_line = new_lines.get(i).copied();

        match (current_line, new_line) {
            (Some(c), Some(n)) if c == n => println!(" {}", c),
            (Some(c), Some(n)) => {
                println!("-{}", c);
                println!("+{}", n);
            }
            (Some(c), None) => println!("-{}", c),
            (None, Some(n)) => println!("+{}", n),
            (None, None) => unreachable!(),
        }
    }
    println!();

    Ok(())
}

fn get_resource_api_path(kind: &str, namespace: &str, name: &str) -> Result<String> {
    Ok(match kind {
        "Pod" => format!("/api/v1/namespaces/{}/pods/{}", namespace, name),
        "Service" => format!("/api/v1/namespaces/{}/services/{}", namespace, name),
        "Deployment" => format!(
            "/apis/apps/v1/namespaces/{}/deployments/{}",
            namespace, name
        ),
        "StatefulSet" => format!(
            "/apis/apps/v1/namespaces/{}/statefulsets/{}",
            namespace, name
        ),
        "DaemonSet" => format!("/apis/apps/v1/namespaces/{}/daemonsets/{}", namespace, name),
        "ReplicaSet" => format!(
            "/apis/apps/v1/namespaces/{}/replicasets/{}",
            namespace, name
        ),
        "Job" => format!("/apis/batch/v1/namespaces/{}/jobs/{}", namespace, name),
        "CronJob" => format!("/apis/batch/v1/namespaces/{}/cronjobs/{}", namespace, name),
        "ConfigMap" => format!("/api/v1/namespaces/{}/configmaps/{}", namespace, name),
        "Secret" => format!("/api/v1/namespaces/{}/secrets/{}", namespace, name),
        "ServiceAccount" => format!("/api/v1/namespaces/{}/serviceaccounts/{}", namespace, name),
        "Ingress" => format!(
            "/apis/networking.k8s.io/v1/namespaces/{}/ingresses/{}",
            namespace, name
        ),
        "PersistentVolumeClaim" => format!(
            "/api/v1/namespaces/{}/persistentvolumeclaims/{}",
            namespace, name
        ),
        "PersistentVolume" => format!("/api/v1/persistentvolumes/{}", name),
        "StorageClass" => format!("/apis/storage.k8s.io/v1/storageclasses/{}", name),
        "Namespace" => format!("/api/v1/namespaces/{}", name),
        "Node" => format!("/api/v1/nodes/{}", name),
        "Role" => format!(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/roles/{}",
            namespace, name
        ),
        "RoleBinding" => format!(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/rolebindings/{}",
            namespace, name
        ),
        "ClusterRole" => format!("/apis/rbac.authorization.k8s.io/v1/clusterroles/{}", name),
        "ClusterRoleBinding" => format!(
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{}",
            name
        ),
        "ResourceQuota" => format!("/api/v1/namespaces/{}/resourcequotas/{}", namespace, name),
        "LimitRange" => format!("/api/v1/namespaces/{}/limitranges/{}", namespace, name),
        "PriorityClass" => format!("/apis/scheduling.k8s.io/v1/priorityclasses/{}", name),
        "CustomResourceDefinition" => format!(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{}",
            name
        ),
        _ => anyhow::bail!("Unsupported resource kind: {}", kind),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_resource_api_path_pod() {
        let path = get_resource_api_path("Pod", "default", "nginx").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");
    }

    #[test]
    fn test_get_resource_api_path_deployment() {
        let path = get_resource_api_path("Deployment", "prod", "web").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/prod/deployments/web");
    }

    #[test]
    fn test_get_resource_api_path_cluster_scoped_pv() {
        let path = get_resource_api_path("PersistentVolume", "ignored", "pv-1").unwrap();
        assert_eq!(path, "/api/v1/persistentvolumes/pv-1");
    }

    #[test]
    fn test_get_resource_api_path_crd() {
        let path = get_resource_api_path("CustomResourceDefinition", "ignored", "foos.example.com")
            .unwrap();
        assert_eq!(
            path,
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foos.example.com"
        );
    }

    #[test]
    fn test_get_resource_api_path_unsupported() {
        let result = get_resource_api_path("Unknown", "default", "x");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_resource_api_path_service() {
        let path = get_resource_api_path("Service", "prod", "my-svc").unwrap();
        assert_eq!(path, "/api/v1/namespaces/prod/services/my-svc");
    }

    #[test]
    fn test_get_resource_api_path_configmap() {
        let path = get_resource_api_path("ConfigMap", "default", "cfg").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/configmaps/cfg");
    }

    #[test]
    fn test_get_resource_api_path_namespace_cluster_scoped() {
        let path = get_resource_api_path("Namespace", "ignored", "kube-system").unwrap();
        assert_eq!(path, "/api/v1/namespaces/kube-system");
    }

    #[test]
    fn test_get_resource_api_path_statefulset() {
        let path = get_resource_api_path("StatefulSet", "staging", "mysql").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/staging/statefulsets/mysql");
    }

    #[test]
    fn test_get_resource_api_path_cluster_scoped_node() {
        let path = get_resource_api_path("Node", "ignored", "worker-1").unwrap();
        assert_eq!(path, "/api/v1/nodes/worker-1");
    }

    #[test]
    fn test_get_resource_api_path_rbac_resources() {
        let path = get_resource_api_path("ClusterRole", "ignored", "admin").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin"
        );

        let path = get_resource_api_path("ClusterRoleBinding", "ignored", "admin-binding").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/admin-binding"
        );

        let path = get_resource_api_path("Role", "default", "pod-reader").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/pod-reader"
        );
    }
}
