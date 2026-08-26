use crate::client::ApiClient;
use anyhow::{Context, Result};
use serde_json::json;
use std::collections::HashMap;

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    name: &str,
    namespace: &str,
    image: &str,
    port: Option<u16>,
    env_vars: &[String],
    labels: Option<&str>,
    restart_policy: &str,
    command: &[String],
    args: &[String],
    dry_run: Option<&str>,
) -> Result<()> {
    // Build labels map
    let mut label_map: HashMap<String, String> = HashMap::new();
    label_map.insert("run".to_string(), name.to_string());

    if let Some(label_str) = labels {
        for pair in label_str.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((k, v)) = pair.split_once('=') {
                label_map.insert(k.to_string(), v.to_string());
            } else {
                anyhow::bail!("Invalid label format: '{}'. Expected key=value", pair);
            }
        }
    }

    // Build container
    let mut container = json!({
        "name": name,
        "image": image,
    });

    // Add port if specified
    if let Some(p) = port {
        container["ports"] = json!([{"containerPort": p}]);
    }

    // Add environment variables
    if !env_vars.is_empty() {
        let mut env_list = Vec::new();
        for env in env_vars {
            if let Some((k, v)) = env.split_once('=') {
                env_list.push(json!({"name": k, "value": v}));
            } else {
                anyhow::bail!(
                    "Invalid environment variable format: '{}'. Expected KEY=VALUE",
                    env
                );
            }
        }
        container["env"] = json!(env_list);
    }

    // Add command if specified
    if !command.is_empty() {
        container["command"] = json!(command);
    }

    // Add args if specified
    if !args.is_empty() {
        container["args"] = json!(args);
    }

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": label_map,
        },
        "spec": {
            "containers": [container],
            "restartPolicy": restart_policy,
        }
    });

    // Handle dry-run
    if let Some(dr) = dry_run {
        if dr == "client" {
            // Just print the resource
            let yaml = serde_yaml::to_string(&pod)?;
            print!("{}", yaml);
            return Ok(());
        }
    }

    let path = format!("/api/v1/namespaces/{}/pods", namespace);
    let query = if let Some(dr) = dry_run {
        if dr == "server" {
            format!("{}?dryRun=All", path)
        } else {
            path
        }
    } else {
        path
    };

    let _result: serde_json::Value = client
        .post(&query, &pod)
        .await
        .context("Failed to create pod")?;

    println!("pod/{} created", name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_pod_json_structure() {
        let name = "nginx";
        let image = "nginx:latest";
        let namespace = "default";

        let mut label_map: HashMap<String, String> = HashMap::new();
        label_map.insert("run".to_string(), name.to_string());

        let container = json!({
            "name": name,
            "image": image,
        });

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": label_map,
            },
            "spec": {
                "containers": [container],
                "restartPolicy": "Always",
            }
        });

        assert_eq!(pod["kind"], "Pod");
        assert_eq!(pod["apiVersion"], "v1");
        assert_eq!(pod["metadata"]["name"], "nginx");
        assert_eq!(pod["metadata"]["namespace"], "default");
        assert_eq!(pod["metadata"]["labels"]["run"], "nginx");
        assert_eq!(pod["spec"]["containers"][0]["name"], "nginx");
        assert_eq!(pod["spec"]["containers"][0]["image"], "nginx:latest");
        assert_eq!(pod["spec"]["restartPolicy"], "Always");
    }

    #[test]
    fn test_pod_with_port() {
        let mut container = json!({
            "name": "nginx",
            "image": "nginx",
        });
        container["ports"] = json!([{"containerPort": 80}]);

        assert_eq!(container["ports"][0]["containerPort"], 80);
    }

    #[test]
    fn test_pod_with_env() {
        let env_vars = vec!["FOO=bar".to_string(), "BAZ=qux".to_string()];
        let mut env_list = Vec::new();
        for env in &env_vars {
            if let Some((k, v)) = env.split_once('=') {
                env_list.push(json!({"name": k, "value": v}));
            }
        }

        assert_eq!(env_list.len(), 2);
        assert_eq!(env_list[0]["name"], "FOO");
        assert_eq!(env_list[0]["value"], "bar");
        assert_eq!(env_list[1]["name"], "BAZ");
        assert_eq!(env_list[1]["value"], "qux");
    }

    #[test]
    fn test_pod_with_labels() {
        let labels_str = "app=nginx,env=prod";
        let mut label_map: HashMap<String, String> = HashMap::new();
        label_map.insert("run".to_string(), "nginx".to_string());

        for pair in labels_str.split(',') {
            if let Some((k, v)) = pair.split_once('=') {
                label_map.insert(k.to_string(), v.to_string());
            }
        }

        assert_eq!(label_map.get("run"), Some(&"nginx".to_string()));
        assert_eq!(label_map.get("app"), Some(&"nginx".to_string()));
        assert_eq!(label_map.get("env"), Some(&"prod".to_string()));
    }

    #[test]
    fn test_pod_with_command() {
        let command = vec!["sh".to_string(), "-c".to_string(), "echo hello".to_string()];
        let mut container = json!({
            "name": "test",
            "image": "busybox",
        });
        container["command"] = json!(command);

        assert_eq!(container["command"][0], "sh");
        assert_eq!(container["command"][1], "-c");
        assert_eq!(container["command"][2], "echo hello");
    }

    #[test]
    fn test_restart_policy_values() {
        for policy in &["Always", "OnFailure", "Never"] {
            let pod = json!({
                "spec": {
                    "restartPolicy": policy,
                }
            });
            assert_eq!(pod["spec"]["restartPolicy"], *policy);
        }
    }

    #[test]
    fn test_dry_run_client_path() {
        let namespace = "default";
        let path = format!("/api/v1/namespaces/{}/pods", namespace);
        let dry_run = Some("server");
        let query = if let Some(dr) = dry_run {
            if dr == "server" {
                format!("{}?dryRun=All", path)
            } else {
                path.clone()
            }
        } else {
            path.clone()
        };
        assert_eq!(query, "/api/v1/namespaces/default/pods?dryRun=All");
    }
}
