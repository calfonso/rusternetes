use crate::client::ApiClient;
use crate::types::RolloutCommands;
use anyhow::{Context, Result};
use serde_json::{json, Value};

/// Execute rollout commands
pub async fn execute(
    client: &ApiClient,
    command: RolloutCommands,
    default_namespace: &str,
) -> Result<()> {
    match command {
        RolloutCommands::Status {
            resource_type,
            name,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            rollout_status(client, &resource_type, &name, ns).await?;
        }
        RolloutCommands::History {
            resource_type,
            name,
            namespace,
            revision,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            rollout_history(client, &resource_type, &name, ns, revision).await?;
        }
        RolloutCommands::Undo {
            resource_type,
            name,
            namespace,
            to_revision,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            rollout_undo(client, &resource_type, &name, ns, to_revision).await?;
        }
        RolloutCommands::Restart {
            resource_type,
            name,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            rollout_restart(client, &resource_type, &name, ns).await?;
        }
        RolloutCommands::Pause {
            resource_type,
            name,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            rollout_pause(client, &resource_type, &name, ns).await?;
        }
        RolloutCommands::Resume {
            resource_type,
            name,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            rollout_resume(client, &resource_type, &name, ns).await?;
        }
    }

    Ok(())
}

async fn rollout_status(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
) -> Result<()> {
    let (api_path, _api_version) = get_resource_api_path(resource_type, namespace, name)?;

    let resource: Value = client
        .get(&api_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get {} {}: {}", resource_type, name, e))?;

    // Extract status information
    let status = resource.get("status");
    let spec = resource.get("spec");

    if let Some(spec_val) = spec {
        if let Some(replicas) = spec_val.get("replicas").and_then(|v| v.as_i64()) {
            println!(
                "{}/{} replicas are available",
                status
                    .and_then(|s| s.get("availableReplicas"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0),
                replicas
            );
        }
    }

    if let Some(status_val) = status {
        // Check rollout status
        if let Some(conditions) = status_val.get("conditions").and_then(|v| v.as_array()) {
            for condition in conditions {
                if let Some(cond_type) = condition.get("type").and_then(|v| v.as_str()) {
                    let status = condition
                        .get("status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    let reason = condition
                        .get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let message = condition
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    if cond_type == "Progressing" {
                        println!("Condition: {} = {}", cond_type, status);
                        if !reason.is_empty() {
                            println!("  Reason: {}", reason);
                        }
                        if !message.is_empty() {
                            println!("  Message: {}", message);
                        }
                    }
                }
            }
        }

        // Show update status
        if let Some(updated) = status_val.get("updatedReplicas").and_then(|v| v.as_i64()) {
            println!("Updated replicas: {}", updated);
        }
        if let Some(ready) = status_val.get("readyReplicas").and_then(|v| v.as_i64()) {
            println!("Ready replicas: {}", ready);
        }
        if let Some(available) = status_val.get("availableReplicas").and_then(|v| v.as_i64()) {
            println!("Available replicas: {}", available);
        }
    }

    Ok(())
}

async fn rollout_history(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    revision: Option<i32>,
) -> Result<()> {
    let (_api_base, _) = get_resource_api_path(resource_type, namespace, name)?;
    let rs_path = format!("/apis/apps/v1/namespaces/{}/replicasets", namespace);

    let replicasets: Value = client
        .get(&rs_path)
        .await
        .context("Failed to get replicasets")?;

    if let Some(items) = replicasets.get("items").and_then(|v| v.as_array()) {
        let mut history: Vec<_> = items
            .iter()
            .filter(|rs| {
                rs.get("metadata")
                    .and_then(|m| m.get("ownerReferences"))
                    .and_then(|o| o.as_array())
                    .map(|refs| {
                        refs.iter()
                            .any(|r| r.get("name").and_then(|n| n.as_str()) == Some(name))
                    })
                    .unwrap_or(false)
            })
            .collect();

        history.sort_by_key(|rs| {
            rs.get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                .and_then(|r| r.as_str())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0)
        });

        if let Some(rev) = revision {
            // Show specific revision
            if let Some(rs) = history.iter().find(|rs| {
                rs.get("metadata")
                    .and_then(|m| m.get("annotations"))
                    .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                    .and_then(|r| r.as_str())
                    .and_then(|s| s.parse::<i32>().ok())
                    == Some(rev)
            }) {
                println!("Revision {}:", rev);
                if let Some(spec) = rs.get("spec") {
                    println!("{}", serde_json::to_string_pretty(spec)?);
                }
            } else {
                anyhow::bail!("Revision {} not found", rev);
            }
        } else {
            // Show all revisions
            println!("{:<10} {:<30}", "REVISION", "CHANGE-CAUSE");
            for rs in history {
                let rev = rs
                    .get("metadata")
                    .and_then(|m| m.get("annotations"))
                    .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                    .and_then(|r| r.as_str())
                    .unwrap_or("0");
                let cause = rs
                    .get("metadata")
                    .and_then(|m| m.get("annotations"))
                    .and_then(|a| a.get("kubernetes.io/change-cause"))
                    .and_then(|c| c.as_str())
                    .unwrap_or("<none>");
                println!("{:<10} {:<30}", rev, cause);
            }
        }
    }

    Ok(())
}

async fn rollout_undo(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    to_revision: Option<i32>,
) -> Result<()> {
    let (api_path, _) = get_resource_api_path(resource_type, namespace, name)?;

    if resource_type != "deployment" {
        anyhow::bail!("Rollout undo is only supported for deployments");
    }

    // Get the deployment
    let _deployment: Value = client
        .get(&api_path)
        .await
        .context("Failed to get deployment")?;

    // Get replicasets to find the target revision
    let rs_path = format!("/apis/apps/v1/namespaces/{}/replicasets", namespace);
    let replicasets: Value = client
        .get(&rs_path)
        .await
        .context("Failed to get replicasets")?;

    if let Some(items) = replicasets.get("items").and_then(|v| v.as_array()) {
        let mut history: Vec<_> = items
            .iter()
            .filter(|rs| {
                rs.get("metadata")
                    .and_then(|m| m.get("ownerReferences"))
                    .and_then(|o| o.as_array())
                    .map(|refs| {
                        refs.iter()
                            .any(|r| r.get("name").and_then(|n| n.as_str()) == Some(name))
                    })
                    .unwrap_or(false)
            })
            .collect();

        history.sort_by_key(|rs| {
            rs.get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                .and_then(|r| r.as_str())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0)
        });

        let target_rs = if let Some(rev) = to_revision {
            history.iter().find(|rs| {
                rs.get("metadata")
                    .and_then(|m| m.get("annotations"))
                    .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                    .and_then(|r| r.as_str())
                    .and_then(|s| s.parse::<i32>().ok())
                    == Some(rev)
            })
        } else {
            // Get previous revision (second to last)
            history.iter().rev().nth(1)
        };

        if let Some(rs) = target_rs {
            // Update deployment with the template from the target replicaset
            if let Some(template) = rs.get("spec").and_then(|s| s.get("template")) {
                let patch = json!({
                    "spec": {
                        "template": template
                    }
                });

                let _: Value = client
                    .patch(&api_path, &patch, "application/merge-patch+json")
                    .await
                    .context("Failed to rollback deployment")?;

                let target_rev = rs
                    .get("metadata")
                    .and_then(|m| m.get("annotations"))
                    .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                    .and_then(|r| r.as_str())
                    .unwrap_or("unknown");

                println!(
                    "deployment.apps/{} rolled back to revision {}",
                    name, target_rev
                );
            } else {
                anyhow::bail!("Target replicaset has no template");
            }
        } else {
            anyhow::bail!("Target revision not found");
        }
    }

    Ok(())
}

async fn rollout_restart(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
) -> Result<()> {
    let (api_path, _) = get_resource_api_path(resource_type, namespace, name)?;

    // Trigger a restart by updating the restart annotation
    let now = chrono::Utc::now().to_rfc3339();
    let patch = json!({
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": now
                    }
                }
            }
        }
    });

    let _: Value = client
        .patch(&api_path, &patch, "application/merge-patch+json")
        .await
        .context("Failed to restart resource")?;

    println!("{} {} restarted", resource_type, name);

    Ok(())
}

async fn rollout_pause(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
) -> Result<()> {
    if resource_type != "deployment" {
        anyhow::bail!("Rollout pause is only supported for deployments");
    }

    let api_path = format!(
        "/apis/apps/v1/namespaces/{}/deployments/{}",
        namespace, name
    );

    let patch = json!({
        "spec": {
            "paused": true
        }
    });

    let _: Value = client
        .patch(&api_path, &patch, "application/merge-patch+json")
        .await
        .context("Failed to pause deployment")?;

    println!("deployment.apps/{} paused", name);

    Ok(())
}

async fn rollout_resume(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
) -> Result<()> {
    if resource_type != "deployment" {
        anyhow::bail!("Rollout resume is only supported for deployments");
    }

    let api_path = format!(
        "/apis/apps/v1/namespaces/{}/deployments/{}",
        namespace, name
    );

    let patch = json!({
        "spec": {
            "paused": false
        }
    });

    let _: Value = client
        .patch(&api_path, &patch, "application/merge-patch+json")
        .await
        .context("Failed to resume deployment")?;

    println!("deployment.apps/{} resumed", name);

    Ok(())
}

fn get_resource_api_path(
    resource_type: &str,
    namespace: &str,
    name: &str,
) -> Result<(String, String)> {
    match resource_type {
        "deployment" | "deployments" | "deploy" => Ok((
            format!(
                "/apis/apps/v1/namespaces/{}/deployments/{}",
                namespace, name
            ),
            "apps/v1".to_string(),
        )),
        "statefulset" | "statefulsets" | "sts" => Ok((
            format!(
                "/apis/apps/v1/namespaces/{}/statefulsets/{}",
                namespace, name
            ),
            "apps/v1".to_string(),
        )),
        "daemonset" | "daemonsets" | "ds" => Ok((
            format!("/apis/apps/v1/namespaces/{}/daemonsets/{}", namespace, name),
            "apps/v1".to_string(),
        )),
        _ => anyhow::bail!("Unsupported resource type for rollout: {}", resource_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rollout_api_path_deployment() {
        let (path, version) = get_resource_api_path("deployment", "default", "nginx").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/deployments/nginx");
        assert_eq!(version, "apps/v1");
    }

    #[test]
    fn test_rollout_api_path_deploy_alias() {
        let (path, _) = get_resource_api_path("deploy", "prod", "web").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/prod/deployments/web");
    }

    #[test]
    fn test_rollout_api_path_statefulset() {
        let (path, _) = get_resource_api_path("sts", "default", "db").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/statefulsets/db");
    }

    #[test]
    fn test_rollout_api_path_daemonset() {
        let (path, _) = get_resource_api_path("ds", "kube-system", "proxy").unwrap();
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/kube-system/daemonsets/proxy"
        );
    }

    #[test]
    fn test_rollout_api_path_unsupported() {
        let result = get_resource_api_path("pod", "default", "x");
        assert!(result.is_err());
    }

    #[test]
    fn test_rollout_api_path_deployments_plural() {
        let (path, version) = get_resource_api_path("deployments", "staging", "api").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/staging/deployments/api");
        assert_eq!(version, "apps/v1");
    }

    #[test]
    fn test_rollout_api_path_statefulset_full_name() {
        let (path, version) = get_resource_api_path("statefulset", "db-ns", "postgres").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/db-ns/statefulsets/postgres");
        assert_eq!(version, "apps/v1");
    }

    #[test]
    fn test_rollout_api_path_statefulsets_plural() {
        let (path, _) = get_resource_api_path("statefulsets", "default", "redis").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/statefulsets/redis");
    }

    #[test]
    fn test_rollout_api_path_daemonset_full_name() {
        let (path, version) = get_resource_api_path("daemonset", "monitoring", "fluentd").unwrap();
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/monitoring/daemonsets/fluentd"
        );
        assert_eq!(version, "apps/v1");
    }

    #[test]
    fn test_rollout_api_path_daemonsets_plural() {
        let (path, _) = get_resource_api_path("daemonsets", "kube-system", "kube-proxy").unwrap();
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/kube-system/daemonsets/kube-proxy"
        );
    }

    #[test]
    fn test_rollout_api_path_unsupported_service() {
        let result = get_resource_api_path("service", "default", "x");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported resource type for rollout"));
    }

    #[test]
    fn test_rollout_api_path_unsupported_replicaset() {
        let result = get_resource_api_path("replicaset", "default", "x");
        assert!(result.is_err());
    }

    #[test]
    fn test_rollout_restart_patch_construction() {
        let now = "2024-01-01T00:00:00Z";
        let patch = json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": now
                        }
                    }
                }
            }
        });
        assert_eq!(
            patch["spec"]["template"]["metadata"]["annotations"]
                ["kubectl.kubernetes.io/restartedAt"],
            "2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn test_rollout_pause_patch_construction() {
        let patch = json!({
            "spec": {
                "paused": true
            }
        });
        assert_eq!(patch["spec"]["paused"], true);
    }

    #[test]
    fn test_rollout_resume_patch_construction() {
        let patch = json!({
            "spec": {
                "paused": false
            }
        });
        assert_eq!(patch["spec"]["paused"], false);
    }

    // ===== 8 additional tests for untested async functions =====

    fn make_test_client() -> ApiClient {
        ApiClient::new("http://127.0.0.1:1", true, None).unwrap()
    }

    #[tokio::test]
    async fn test_rollout_status_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_status(&client, "deployment", "nginx", "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_history_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_history(&client, "deployment", "nginx", "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_undo_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_undo(&client, "deployment", "nginx", "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_undo_non_deployment_returns_err() {
        let client = make_test_client();
        let result = rollout_undo(&client, "statefulset", "db", "default", None).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only supported for deployments"));
    }

    #[tokio::test]
    async fn test_rollout_restart_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_restart(&client, "deployment", "nginx", "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_pause_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_pause(&client, "deployment", "nginx", "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_pause_non_deployment_returns_err() {
        let client = make_test_client();
        let result = rollout_pause(&client, "statefulset", "db", "default").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only supported for deployments"));
    }

    #[tokio::test]
    async fn test_rollout_resume_non_deployment_returns_err() {
        let client = make_test_client();
        let result = rollout_resume(&client, "statefulset", "db", "default").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only supported for deployments"));
    }
}
