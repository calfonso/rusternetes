use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// Cordon a node (mark as unschedulable).
pub async fn execute_cordon(client: &ApiClient, node_name: &str) -> Result<()> {
    set_unschedulable(client, node_name, true).await?;
    println!("node/{} cordoned", node_name);
    Ok(())
}

/// Uncordon a node (mark as schedulable).
pub async fn execute_uncordon(client: &ApiClient, node_name: &str) -> Result<()> {
    set_unschedulable(client, node_name, false).await?;
    println!("node/{} uncordoned", node_name);
    Ok(())
}

/// Set the unschedulable field on a node.
async fn set_unschedulable(client: &ApiClient, node_name: &str, unschedulable: bool) -> Result<()> {
    let path = format!("/api/v1/nodes/{}", node_name);

    let patch_body = json!({
        "spec": {
            "unschedulable": unschedulable,
        }
    });

    let _result: Value = client
        .patch(&path, &patch_body, "application/merge-patch+json")
        .await
        .context("Failed to update node")?;

    Ok(())
}

/// Drain a node by cordoning it and evicting all pods.
///
/// Steps:
/// 1. Cordon the node (set unschedulable=true)
/// 2. List all pods on the node
/// 3. Filter out mirror pods (owned by the Node) and DaemonSet pods
/// 4. Evict remaining pods using the eviction API
pub async fn execute_drain(
    client: &ApiClient,
    node_name: &str,
    force: bool,
    ignore_daemonsets: bool,
    delete_emptydir_data: bool,
    grace_period: Option<i64>,
    timeout: Option<u64>,
) -> Result<()> {
    // Step 1: Cordon the node
    set_unschedulable(client, node_name, true).await?;
    println!("node/{} cordoned", node_name);

    // Step 2: List pods on this node
    let pods_path = format!("/api/v1/pods?fieldSelector=spec.nodeName={}", node_name);
    let pods: Value = client
        .get(&pods_path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to list pods on node")?;

    let items = pods
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();

    if items.is_empty() {
        println!("node/{} drained", node_name);
        return Ok(());
    }

    // Step 3: Filter pods
    let mut pods_to_evict: Vec<(String, String)> = Vec::new(); // (namespace, name)
    let mut warnings: Vec<String> = Vec::new();

    for pod in &items {
        let metadata = pod.get("metadata").unwrap_or(&Value::Null);
        let pod_name = metadata.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let namespace = metadata
            .get("namespace")
            .and_then(|n| n.as_str())
            .unwrap_or("default");

        // Check if mirror pod (has mirror pod annotation)
        let is_mirror = metadata
            .get("annotations")
            .and_then(|a| a.get("kubernetes.io/config.mirror"))
            .is_some();

        if is_mirror {
            continue;
        }

        // Check if DaemonSet pod
        let is_daemonset = metadata
            .get("ownerReferences")
            .and_then(|refs| refs.as_array())
            .map(|refs| {
                refs.iter()
                    .any(|r| r.get("kind").and_then(|k| k.as_str()) == Some("DaemonSet"))
            })
            .unwrap_or(false);

        if is_daemonset {
            if ignore_daemonsets {
                continue;
            } else {
                warnings.push(format!(
                    "WARNING: ignoring DaemonSet-managed pod {}/{}; use --ignore-daemonsets to suppress",
                    namespace, pod_name
                ));
                if !force {
                    anyhow::bail!(
                        "Cannot drain node: DaemonSet-managed pods exist. Use --ignore-daemonsets"
                    );
                }
                continue;
            }
        }

        // Check for local storage (emptyDir)
        let has_emptydir = pod
            .get("spec")
            .and_then(|s| s.get("volumes"))
            .and_then(|v| v.as_array())
            .map(|vols| vols.iter().any(|v| v.get("emptyDir").is_some()))
            .unwrap_or(false);

        if has_emptydir && !delete_emptydir_data {
            if !force {
                anyhow::bail!(
                    "Cannot drain node: pod {}/{} uses emptyDir. Use --delete-emptydir-data",
                    namespace,
                    pod_name
                );
            }
            warnings.push(format!(
                "WARNING: deleting pod {}/{} with local storage",
                namespace, pod_name
            ));
        }

        // Check for pods not managed by a controller
        let has_controller = metadata
            .get("ownerReferences")
            .and_then(|refs| refs.as_array())
            .map(|refs| !refs.is_empty())
            .unwrap_or(false);

        if !has_controller && !force {
            anyhow::bail!(
                "Cannot drain node: pod {}/{} is not managed by a controller. Use --force",
                namespace,
                pod_name
            );
        }

        pods_to_evict.push((namespace.to_string(), pod_name.to_string()));
    }

    // Print warnings
    for w in &warnings {
        eprintln!("{}", w);
    }

    // Step 4: Evict pods
    println!("evicting pods from node/{}", node_name);

    for (namespace, pod_name) in &pods_to_evict {
        let eviction_path = format!(
            "/api/v1/namespaces/{}/pods/{}/eviction",
            namespace, pod_name
        );

        let mut eviction = json!({
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {
                "name": pod_name,
                "namespace": namespace,
            },
        });

        if let Some(gp) = grace_period {
            eviction["deleteOptions"] = json!({
                "gracePeriodSeconds": gp,
            });
        }

        match client.post::<Value, Value>(&eviction_path, &eviction).await {
            Ok(_) => {
                println!("  evicting pod {}/{}", namespace, pod_name);
            }
            Err(e) => {
                let err_msg = format!("{}", e);
                if err_msg.contains("429") || err_msg.contains("Too Many Requests") {
                    eprintln!(
                        "  WARNING: pod {}/{} eviction blocked by PodDisruptionBudget, retrying...",
                        namespace, pod_name
                    );
                    // Simple retry with sleep
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    client
                        .post::<Value, Value>(&eviction_path, &eviction)
                        .await
                        .context(format!("Failed to evict pod {}/{}", namespace, pod_name))?;
                } else {
                    return Err(e)
                        .context(format!("Failed to evict pod {}/{}", namespace, pod_name));
                }
            }
        }
    }

    // Wait for pods to be deleted if timeout specified
    if let Some(timeout_secs) = timeout {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

        for (namespace, pod_name) in &pods_to_evict {
            let pod_path = format!("/api/v1/namespaces/{}/pods/{}", namespace, pod_name);
            loop {
                if std::time::Instant::now() > deadline {
                    anyhow::bail!(
                        "Timeout waiting for pod {}/{} to be deleted",
                        namespace,
                        pod_name
                    );
                }
                match client.get::<Value>(&pod_path).await {
                    Err(rusternetes_client::http::GetError::NotFound) => break,
                    Err(rusternetes_client::http::GetError::Other(e)) => {
                        return Err(e).context("Error checking pod status");
                    }
                    Ok(_) => {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }

    println!("node/{} drained", node_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    #[test]
    fn test_filter_mirror_pods() {
        let pod = json!({
            "metadata": {
                "name": "kube-apiserver",
                "namespace": "kube-system",
                "annotations": {
                    "kubernetes.io/config.mirror": "abc123"
                }
            }
        });

        let is_mirror = pod
            .get("metadata")
            .and_then(|m| m.get("annotations"))
            .and_then(|a| a.get("kubernetes.io/config.mirror"))
            .is_some();

        assert!(is_mirror);
    }

    #[test]
    fn test_filter_daemonset_pods() {
        let pod = json!({
            "metadata": {
                "name": "kube-proxy-abc",
                "namespace": "kube-system",
                "ownerReferences": [{
                    "kind": "DaemonSet",
                    "name": "kube-proxy"
                }]
            }
        });

        let is_daemonset = pod
            .get("metadata")
            .and_then(|m| m.get("ownerReferences"))
            .and_then(|refs| refs.as_array())
            .map(|refs| {
                refs.iter()
                    .any(|r| r.get("kind").and_then(|k| k.as_str()) == Some("DaemonSet"))
            })
            .unwrap_or(false);

        assert!(is_daemonset);
    }

    #[test]
    fn test_detect_emptydir_volumes() {
        let pod = json!({
            "spec": {
                "volumes": [
                    {"name": "data", "emptyDir": {}},
                    {"name": "config", "configMap": {"name": "my-config"}},
                ]
            }
        });

        let has_emptydir = pod
            .get("spec")
            .and_then(|s| s.get("volumes"))
            .and_then(|v| v.as_array())
            .map(|vols| vols.iter().any(|v| v.get("emptyDir").is_some()))
            .unwrap_or(false);

        assert!(has_emptydir);
    }

    #[test]
    fn test_eviction_body_construction() {
        let namespace = "default";
        let pod_name = "nginx";
        let grace_period: Option<i64> = Some(30);

        let mut eviction = json!({
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {
                "name": pod_name,
                "namespace": namespace,
            },
        });

        if let Some(gp) = grace_period {
            eviction["deleteOptions"] = json!({
                "gracePeriodSeconds": gp,
            });
        }

        assert_eq!(eviction["kind"], "Eviction");
        assert_eq!(eviction["metadata"]["name"], "nginx");
        assert_eq!(eviction["deleteOptions"]["gracePeriodSeconds"], 30);
    }

    #[test]
    fn test_unmanaged_pod_detection() {
        let managed_pod = json!({
            "metadata": {
                "ownerReferences": [{"kind": "ReplicaSet", "name": "nginx-abc"}]
            }
        });
        let unmanaged_pod = json!({
            "metadata": {}
        });

        let has_controller = |pod: &Value| -> bool {
            pod.get("metadata")
                .and_then(|m| m.get("ownerReferences"))
                .and_then(|refs| refs.as_array())
                .map(|refs| !refs.is_empty())
                .unwrap_or(false)
        };

        assert!(has_controller(&managed_pod));
        assert!(!has_controller(&unmanaged_pod));
    }

    #[test]
    fn test_non_mirror_pod_detection() {
        let pod = json!({
            "metadata": {
                "name": "nginx",
                "namespace": "default",
                "annotations": {
                    "some-other-annotation": "value"
                }
            }
        });

        let is_mirror = pod
            .get("metadata")
            .and_then(|m| m.get("annotations"))
            .and_then(|a| a.get("kubernetes.io/config.mirror"))
            .is_some();

        assert!(!is_mirror);
    }

    #[test]
    fn test_non_daemonset_pod() {
        let pod = json!({
            "metadata": {
                "ownerReferences": [{"kind": "ReplicaSet", "name": "web-abc"}]
            }
        });

        let is_daemonset = pod
            .get("metadata")
            .and_then(|m| m.get("ownerReferences"))
            .and_then(|refs| refs.as_array())
            .map(|refs| {
                refs.iter()
                    .any(|r| r.get("kind").and_then(|k| k.as_str()) == Some("DaemonSet"))
            })
            .unwrap_or(false);

        assert!(!is_daemonset);
    }

    #[test]
    fn test_no_emptydir_volumes() {
        let pod = json!({
            "spec": {
                "volumes": [
                    {"name": "config", "configMap": {"name": "my-config"}}
                ]
            }
        });

        let has_emptydir = pod
            .get("spec")
            .and_then(|s| s.get("volumes"))
            .and_then(|v| v.as_array())
            .map(|vols| vols.iter().any(|v| v.get("emptyDir").is_some()))
            .unwrap_or(false);

        assert!(!has_emptydir);
    }

    #[test]
    fn test_eviction_body_without_grace_period() {
        let namespace = "default";
        let pod_name = "nginx";
        let grace_period: Option<i64> = None;

        let mut eviction = json!({
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {
                "name": pod_name,
                "namespace": namespace,
            },
        });

        if let Some(gp) = grace_period {
            eviction["deleteOptions"] = json!({
                "gracePeriodSeconds": gp,
            });
        }

        assert_eq!(eviction["kind"], "Eviction");
        assert!(eviction.get("deleteOptions").is_none());
    }

    #[test]
    fn test_cordon_patch_body_construction() {
        let unschedulable = true;
        let patch_body = json!({
            "spec": {
                "unschedulable": unschedulable,
            }
        });
        assert_eq!(patch_body["spec"]["unschedulable"], true);

        let patch_body = json!({
            "spec": {
                "unschedulable": false,
            }
        });
        assert_eq!(patch_body["spec"]["unschedulable"], false);
    }

    #[test]
    fn test_node_path_construction() {
        let node_name = "worker-1";
        let path = format!("/api/v1/nodes/{}", node_name);
        assert_eq!(path, "/api/v1/nodes/worker-1");
    }

    #[test]
    fn test_pods_field_selector_construction() {
        let node_name = "node-1";
        let pods_path = format!("/api/v1/pods?fieldSelector=spec.nodeName={}", node_name);
        assert_eq!(pods_path, "/api/v1/pods?fieldSelector=spec.nodeName=node-1");
    }

    #[test]
    fn test_eviction_path_construction() {
        let namespace = "kube-system";
        let pod_name = "coredns-abc123";
        let eviction_path = format!(
            "/api/v1/namespaces/{}/pods/{}/eviction",
            namespace, pod_name
        );
        assert_eq!(
            eviction_path,
            "/api/v1/namespaces/kube-system/pods/coredns-abc123/eviction"
        );
    }
}
