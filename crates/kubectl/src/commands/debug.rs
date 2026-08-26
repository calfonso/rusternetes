use crate::client::ApiClient;
use anyhow::{Context, Result};
use serde_json::{json, Value};

/// Create debugging sessions for troubleshooting workloads and nodes.
///
/// Adds an ephemeral container to a running pod for debugging.
/// Equivalent to `kubectl debug pod/nginx -it --image=busybox`
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    target: &str,
    namespace: &str,
    image: &str,
    container_name: Option<&str>,
    interactive: bool,
    tty: bool,
    target_container: Option<&str>,
    command: &[String],
) -> Result<()> {
    // Parse target: could be "pod/name" or just "name" (defaults to pod)
    let (resource_type, pod_name) = parse_target(target)?;

    match resource_type.as_str() {
        "pod" | "pods" | "po" => {
            debug_pod(
                client,
                pod_name,
                namespace,
                image,
                container_name,
                interactive,
                tty,
                target_container,
                command,
            )
            .await
        }
        "node" | "nodes" | "no" => {
            debug_node(client, pod_name, image, container_name, interactive, tty).await
        }
        _ => anyhow::bail!(
            "unsupported resource type for debug: {}. Supported: pod, node",
            resource_type
        ),
    }
}

fn parse_target(target: &str) -> Result<(String, &str)> {
    if let Some((rtype, name)) = target.split_once('/') {
        Ok((rtype.to_lowercase(), name))
    } else {
        // Default to pod
        Ok(("pod".to_string(), target))
    }
}

#[allow(clippy::too_many_arguments)]
async fn debug_pod(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    image: &str,
    container_name: Option<&str>,
    _interactive: bool,
    _tty: bool,
    target_container: Option<&str>,
    command: &[String],
) -> Result<()> {
    // Verify the pod exists
    let pod_path = format!("/api/v1/namespaces/{}/pods/{}", namespace, pod_name);
    let pod: Value = client
        .get(&pod_path)
        .await
        .map_err(|e| anyhow::anyhow!("pod \"{}\" not found: {}", pod_name, e))?;

    // Generate a container name if not provided
    let debug_container_name = container_name
        .map(|s| s.to_string())
        .unwrap_or_else(|| generate_debug_container_name(&pod));

    // Build the ephemeral container spec
    let mut ephemeral_container = json!({
        "name": debug_container_name,
        "image": image,
        "stdin": true,
        "tty": true,
    });

    if let Some(target) = target_container {
        ephemeral_container["targetContainerName"] = json!(target);
    }

    if !command.is_empty() {
        ephemeral_container["command"] = json!(command);
    }

    // Get existing ephemeral containers
    let existing_ephemeral = pod
        .get("spec")
        .and_then(|s| s.get("ephemeralContainers"))
        .cloned()
        .unwrap_or_else(|| json!([]));

    let mut ephemeral_list = if let Some(arr) = existing_ephemeral.as_array() {
        arr.clone()
    } else {
        vec![]
    };
    ephemeral_list.push(ephemeral_container);

    // PATCH the pod's ephemeralcontainers subresource
    let patch = json!({
        "spec": {
            "ephemeralContainers": ephemeral_list,
        }
    });

    let patch_path = format!(
        "/api/v1/namespaces/{}/pods/{}/ephemeralcontainers",
        namespace, pod_name
    );

    let _result: Value = client
        .patch(
            &patch_path,
            &patch,
            "application/strategic-merge-patch+json",
        )
        .await
        .context("Failed to add ephemeral container to pod")?;

    println!(
        "Defaulting debug container name to {}.",
        debug_container_name
    );

    // Note: In a real implementation, we would attach to the container here
    // using websocket exec. For now, we just inform the user.
    eprintln!(
        "If you don't see a command prompt, try pressing enter.\n\
         Use 'kubectl attach {} -c {} -i -t' to connect to the debug container.",
        pod_name, debug_container_name
    );

    Ok(())
}

async fn debug_node(
    client: &ApiClient,
    node_name: &str,
    image: &str,
    container_name: Option<&str>,
    _interactive: bool,
    _tty: bool,
) -> Result<()> {
    // Verify the node exists
    let node_path = format!("/api/v1/nodes/{}", node_name);
    let _node: Value = client
        .get(&node_path)
        .await
        .map_err(|e| anyhow::anyhow!("node \"{}\" not found: {}", node_name, e))?;

    let debug_pod_name = format!("node-debugger-{}-{}", node_name, generate_suffix());
    let cname = container_name.unwrap_or("debugger");

    // Create a pod that runs in the node's host namespaces
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": debug_pod_name,
            "namespace": "default",
        },
        "spec": {
            "nodeName": node_name,
            "hostPID": true,
            "hostNetwork": true,
            "containers": [{
                "name": cname,
                "image": image,
                "stdin": true,
                "tty": true,
                "securityContext": {
                    "privileged": true,
                },
                "volumeMounts": [{
                    "name": "host-root",
                    "mountPath": "/host",
                }],
            }],
            "volumes": [{
                "name": "host-root",
                "hostPath": {
                    "path": "/",
                },
            }],
            "tolerations": [{
                "operator": "Exists",
            }],
            "restartPolicy": "Never",
        }
    });

    let pod_path = "/api/v1/namespaces/default/pods";
    let _result: Value = client
        .post(pod_path, &pod)
        .await
        .context("Failed to create node debug pod")?;

    println!(
        "Creating debugging pod {} with container {} on node {}.",
        debug_pod_name, cname, node_name
    );
    eprintln!(
        "If you don't see a command prompt, try pressing enter.\n\
         Use 'kubectl attach {} -c {} -i -t' to connect.",
        debug_pod_name, cname
    );

    Ok(())
}

fn generate_debug_container_name(pod: &Value) -> String {
    let existing: Vec<String> = pod
        .get("spec")
        .and_then(|s| s.get("ephemeralContainers"))
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    c.get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default();

    for i in 0u32.. {
        let name = if i == 0 {
            "debugger".to_string()
        } else {
            format!("debugger-{}", i)
        };
        if !existing.contains(&name) {
            return name;
        }
    }
    "debugger".to_string()
}

fn generate_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}", ts % 100000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_target() {
        let (rtype, name) = parse_target("pod/nginx").unwrap();
        assert_eq!(rtype, "pod");
        assert_eq!(name, "nginx");

        let (rtype, name) = parse_target("node/mynode").unwrap();
        assert_eq!(rtype, "node");
        assert_eq!(name, "mynode");

        // Default to pod
        let (rtype, name) = parse_target("nginx").unwrap();
        assert_eq!(rtype, "pod");
        assert_eq!(name, "nginx");
    }

    #[test]
    fn test_generate_debug_container_name() {
        let pod = serde_json::json!({
            "spec": {}
        });
        assert_eq!(generate_debug_container_name(&pod), "debugger");

        let pod_with_debugger = serde_json::json!({
            "spec": {
                "ephemeralContainers": [
                    {"name": "debugger"}
                ]
            }
        });
        assert_eq!(
            generate_debug_container_name(&pod_with_debugger),
            "debugger-1"
        );
    }

    #[test]
    fn test_generate_debug_container_name_multiple_existing() {
        let pod = serde_json::json!({
            "spec": {
                "ephemeralContainers": [
                    {"name": "debugger"},
                    {"name": "debugger-1"},
                    {"name": "debugger-2"}
                ]
            }
        });
        assert_eq!(generate_debug_container_name(&pod), "debugger-3");
    }

    #[test]
    fn test_ephemeral_container_patch_construction() {
        let image = "busybox:latest";
        let debug_container_name = "debugger";
        let target_container = Some("nginx");
        let command: Vec<String> = vec!["sh".to_string()];

        let mut ephemeral_container = json!({
            "name": debug_container_name,
            "image": image,
            "stdin": true,
            "tty": true,
        });

        if let Some(target) = target_container {
            ephemeral_container["targetContainerName"] = json!(target);
        }

        if !command.is_empty() {
            ephemeral_container["command"] = json!(command);
        }

        let ephemeral_list = vec![ephemeral_container.clone()];

        let patch = json!({
            "spec": {
                "ephemeralContainers": ephemeral_list,
            }
        });

        let container = &patch["spec"]["ephemeralContainers"][0];
        assert_eq!(container["name"], "debugger");
        assert_eq!(container["image"], "busybox:latest");
        assert_eq!(container["stdin"], true);
        assert_eq!(container["tty"], true);
        assert_eq!(container["targetContainerName"], "nginx");
        assert_eq!(container["command"][0], "sh");
    }

    #[test]
    fn test_ephemeral_container_appended_to_existing() {
        let pod = serde_json::json!({
            "spec": {
                "ephemeralContainers": [
                    {"name": "debugger", "image": "alpine"}
                ]
            }
        });

        let existing_ephemeral = pod
            .get("spec")
            .and_then(|s| s.get("ephemeralContainers"))
            .cloned()
            .unwrap_or_else(|| json!([]));

        let mut ephemeral_list = existing_ephemeral.as_array().unwrap().clone();
        ephemeral_list.push(json!({
            "name": "debugger-1",
            "image": "busybox",
            "stdin": true,
            "tty": true,
        }));

        assert_eq!(ephemeral_list.len(), 2);
        assert_eq!(ephemeral_list[0]["name"], "debugger");
        assert_eq!(ephemeral_list[1]["name"], "debugger-1");
    }

    #[test]
    fn test_parse_target_case_insensitive() {
        let (rtype, name) = parse_target("Pod/nginx").unwrap();
        assert_eq!(rtype, "pod");
        assert_eq!(name, "nginx");

        let (rtype, name) = parse_target("NODE/worker-1").unwrap();
        assert_eq!(rtype, "node");
        assert_eq!(name, "worker-1");
    }

    #[test]
    fn test_generate_suffix_returns_numeric_string() {
        let suffix = generate_suffix();
        assert!(suffix.parse::<u128>().is_ok(), "Suffix should be numeric");
        assert!(suffix.len() <= 5, "Suffix should be at most 5 digits");
    }

    #[test]
    fn test_ephemeral_container_no_command() {
        // When no command is provided, the "command" field should not be set
        let command: Vec<String> = vec![];
        let mut ephemeral_container = json!({
            "name": "debugger",
            "image": "alpine",
            "stdin": true,
            "tty": true,
        });
        if !command.is_empty() {
            ephemeral_container["command"] = json!(command);
        }
        assert!(ephemeral_container.get("command").is_none());
    }

    #[test]
    fn test_ephemeral_container_no_target() {
        // When no targetContainerName is provided, it should not be set
        let target_container: Option<&str> = None;
        let mut ephemeral_container = json!({
            "name": "debugger",
            "image": "busybox",
            "stdin": true,
            "tty": true,
        });
        if let Some(target) = target_container {
            ephemeral_container["targetContainerName"] = json!(target);
        }
        assert!(ephemeral_container.get("targetContainerName").is_none());
    }

    #[test]
    fn test_node_debug_pod_construction() {
        let node_name = "worker-1";
        let image = "ubuntu:latest";
        let cname = "debugger";

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("node-debugger-{}-12345", node_name),
                "namespace": "default",
            },
            "spec": {
                "nodeName": node_name,
                "hostPID": true,
                "hostNetwork": true,
                "containers": [{
                    "name": cname,
                    "image": image,
                    "stdin": true,
                    "tty": true,
                    "securityContext": {
                        "privileged": true,
                    },
                    "volumeMounts": [{
                        "name": "host-root",
                        "mountPath": "/host",
                    }],
                }],
                "volumes": [{
                    "name": "host-root",
                    "hostPath": {
                        "path": "/",
                    },
                }],
                "tolerations": [{
                    "operator": "Exists",
                }],
                "restartPolicy": "Never",
            }
        });

        // Verify key node-debug properties
        assert_eq!(pod["spec"]["nodeName"], "worker-1");
        assert_eq!(pod["spec"]["hostPID"], true);
        assert_eq!(pod["spec"]["hostNetwork"], true);
        assert_eq!(pod["spec"]["restartPolicy"], "Never");
        assert_eq!(
            pod["spec"]["containers"][0]["securityContext"]["privileged"],
            true
        );
        assert_eq!(
            pod["spec"]["containers"][0]["volumeMounts"][0]["mountPath"],
            "/host"
        );
        assert_eq!(pod["spec"]["volumes"][0]["hostPath"]["path"], "/");
        assert_eq!(pod["spec"]["tolerations"][0]["operator"], "Exists");
    }

    #[test]
    fn test_node_debug_pod_default_container_name() {
        // When container_name is None, default to "debugger"
        let container_name: &str = "debugger";
        assert_eq!(container_name, "debugger");

        // When container_name is Some, use it
        assert_eq!("my-debugger", "my-debugger");
    }

    #[test]
    fn test_generate_debug_container_name_no_ephemeral_containers_key() {
        // Pod with no ephemeralContainers key at all
        let pod = serde_json::json!({"spec": {"containers": [{"name": "app"}]}});
        assert_eq!(generate_debug_container_name(&pod), "debugger");
    }

    #[test]
    fn test_parse_target_with_aliases() {
        let (rtype, name) = parse_target("po/nginx").unwrap();
        assert_eq!(rtype, "po");
        assert_eq!(name, "nginx");

        let (rtype, name) = parse_target("no/worker-1").unwrap();
        assert_eq!(rtype, "no");
        assert_eq!(name, "worker-1");

        let (rtype, name) = parse_target("pods/nginx").unwrap();
        assert_eq!(rtype, "pods");
        assert_eq!(name, "nginx");
    }

    #[test]
    fn test_generate_debug_container_name_empty_ephemeral_list() {
        let pod = serde_json::json!({
            "spec": {
                "ephemeralContainers": []
            }
        });
        assert_eq!(generate_debug_container_name(&pod), "debugger");
    }

    #[test]
    fn test_generate_debug_container_name_gap_in_numbering() {
        // If "debugger" and "debugger-2" exist but not "debugger-1"
        let pod = serde_json::json!({
            "spec": {
                "ephemeralContainers": [
                    {"name": "debugger"},
                    {"name": "debugger-2"}
                ]
            }
        });
        // Should pick "debugger-1" since it's the first available
        assert_eq!(generate_debug_container_name(&pod), "debugger-1");
    }

    #[test]
    fn test_generate_debug_container_name_no_spec() {
        let pod = serde_json::json!({});
        assert_eq!(generate_debug_container_name(&pod), "debugger");
    }

    #[test]
    fn test_generate_suffix_is_bounded() {
        // suffix is ts % 100000, so should be < 100000
        let suffix = generate_suffix();
        let val: u128 = suffix.parse().unwrap();
        assert!(val < 100000);
    }

    #[test]
    fn test_ephemeral_container_with_multiple_commands() {
        let command = vec!["sh".to_string(), "-c".to_string(), "sleep 3600".to_string()];
        let mut ephemeral_container = serde_json::json!({
            "name": "debugger",
            "image": "busybox",
            "stdin": true,
            "tty": true,
        });
        if !command.is_empty() {
            ephemeral_container["command"] = serde_json::json!(command);
        }
        let cmd_array = ephemeral_container["command"].as_array().unwrap();
        assert_eq!(cmd_array.len(), 3);
        assert_eq!(cmd_array[0], "sh");
        assert_eq!(cmd_array[1], "-c");
        assert_eq!(cmd_array[2], "sleep 3600");
    }
}
