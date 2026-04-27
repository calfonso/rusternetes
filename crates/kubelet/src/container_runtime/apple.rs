// Apple Container runtime implementation using CLI

use super::*;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::process::{Command, Stdio};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;

pub struct AppleContainerRuntime {
    container_bin: String,
}

impl AppleContainerRuntime {
    pub fn new() -> Result<Self, RuntimeError> {
        // Verify container command is available
        let output = Command::new("container")
            .arg("--version")
            .output()
            .map_err(|e| RuntimeError::Generic(format!("container command not found: {}", e)))?;

        if !output.status.success() {
            return Err(RuntimeError::Generic(
                "container command failed to execute".to_string(),
            ));
        }

        Ok(Self {
            container_bin: "container".to_string(),
        })
    }

    fn run_json_command(&self, args: &[&str]) -> Result<Value, RuntimeError> {
        let output = Command::new(&self.container_bin)
            .args(args)
            .output()
            .map_err(|e| RuntimeError::CommandFailed(format!("Failed to execute: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RuntimeError::CommandFailed(stderr.to_string()));
        }

        serde_json::from_slice(&output.stdout)
            .map_err(|e| RuntimeError::ParseError(format!("JSON parse error: {}", e)))
    }

    fn run_command(&self, args: &[&str]) -> Result<String, RuntimeError> {
        let output = Command::new(&self.container_bin)
            .args(args)
            .output()
            .map_err(|e| RuntimeError::CommandFailed(format!("Failed to execute: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RuntimeError::CommandFailed(stderr.to_string()));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    async fn run_command_async(&self, args: &[&str]) -> Result<String, RuntimeError> {
        let output = TokioCommand::new(&self.container_bin)
            .args(args)
            .output()
            .await
            .map_err(|e| RuntimeError::CommandFailed(format!("Failed to execute: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RuntimeError::CommandFailed(stderr.to_string()));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

#[async_trait]
impl ContainerRuntime for AppleContainerRuntime {
    async fn inspect_image(&self, image: &str) -> Result<Option<ImageInfo>, RuntimeError> {
        match self.run_json_command(&["image", "inspect", image]) {
            Ok(json) => {
                // Parse the JSON response to extract image info
                let id = json["Id"].as_str().unwrap_or("").to_string();
                let repo_tags = json["RepoTags"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                let size = json["Size"].as_u64().unwrap_or(0);

                let config = json["Config"].as_object().map(|c| ImageConfig {
                    entrypoint: c
                        .get("Entrypoint")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        }),
                    cmd: c.get("Cmd").and_then(|v| v.as_array()).map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                    env: c.get("Env").and_then(|v| v.as_array()).map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    }),
                    working_dir: c.get("WorkingDir").and_then(|v| v.as_str()).map(String::from),
                    user: c.get("User").and_then(|v| v.as_str()).map(String::from),
                });

                Ok(Some(ImageInfo {
                    id,
                    repo_tags,
                    size,
                    config,
                }))
            }
            Err(RuntimeError::CommandFailed(msg)) if msg.contains("not found") => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn pull_image(&self, image: &str) -> Result<(), RuntimeError> {
        self.run_command_async(&["image", "pull", image])
            .await
            .map(|_| ())
    }

    async fn create_container(
        &self,
        name: &str,
        config: ContainerConfig,
    ) -> Result<String, RuntimeError> {
        let mut args = vec!["container", "create", "--name", name];

        // Add hostname
        if let Some(ref hostname) = config.hostname {
            args.push("--hostname");
            args.push(hostname);
        }

        // Add environment variables
        let env_strings: Vec<String> = config
            .env
            .iter()
            .map(|e| format!("--env={}", e))
            .collect();
        let env_refs: Vec<&str> = env_strings.iter().map(|s| s.as_str()).collect();
        args.extend(env_refs);

        // Add working directory
        if let Some(ref wd) = config.working_dir {
            args.push("--workdir");
            args.push(wd);
        }

        // Add user
        if let Some(ref user) = config.user {
            args.push("--user");
            args.push(user);
        }

        // Add labels
        let label_strings: Vec<String> = config
            .labels
            .iter()
            .map(|(k, v)| format!("--label={}={}", k, v))
            .collect();
        let label_refs: Vec<&str> = label_strings.iter().map(|s| s.as_str()).collect();
        args.extend(label_refs);

        // Add volume binds
        let bind_strings: Vec<String> = config
            .host_config
            .binds
            .iter()
            .map(|b| format!("--volume={}", b))
            .collect();
        let bind_refs: Vec<&str> = bind_strings.iter().map(|s| s.as_str()).collect();
        args.extend(bind_refs);

        // Add network mode
        if let Some(ref net) = config.host_config.network_mode {
            args.push("--network");
            args.push(net);
        }

        // Add privileged
        if config.host_config.privileged {
            args.push("--privileged");
        }

        // Add capabilities
        let cap_add_strings: Vec<String> = config
            .host_config
            .cap_add
            .iter()
            .map(|c| format!("--cap-add={}", c))
            .collect();
        let cap_add_refs: Vec<&str> = cap_add_strings.iter().map(|s| s.as_str()).collect();
        args.extend(cap_add_refs);

        let cap_drop_strings: Vec<String> = config
            .host_config
            .cap_drop
            .iter()
            .map(|c| format!("--cap-drop={}", c))
            .collect();
        let cap_drop_refs: Vec<&str> = cap_drop_strings.iter().map(|s| s.as_str()).collect();
        args.extend(cap_drop_refs);

        // Add devices
        let device_strings: Vec<String> = config
            .host_config
            .devices
            .iter()
            .map(|d| {
                format!(
                    "--device={}:{}:{}",
                    d.path_on_host, d.path_in_container, d.cgroup_permissions
                )
            })
            .collect();
        let device_refs: Vec<&str> = device_strings.iter().map(|s| s.as_str()).collect();
        args.extend(device_refs);

        // Add DNS
        let dns_strings: Vec<String> = config
            .host_config
            .dns
            .iter()
            .map(|d| format!("--dns={}", d))
            .collect();
        let dns_refs: Vec<&str> = dns_strings.iter().map(|s| s.as_str()).collect();
        args.extend(dns_refs);

        // Add DNS search
        let dns_search_strings: Vec<String> = config
            .host_config
            .dns_search
            .iter()
            .map(|d| format!("--dns-search={}", d))
            .collect();
        let dns_search_refs: Vec<&str> = dns_search_strings.iter().map(|s| s.as_str()).collect();
        args.extend(dns_search_refs);

        // Add extra hosts
        let host_strings: Vec<String> = config
            .host_config
            .extra_hosts
            .iter()
            .map(|h| format!("--add-host={}", h))
            .collect();
        let host_refs: Vec<&str> = host_strings.iter().map(|s| s.as_str()).collect();
        args.extend(host_refs);

        // Add readonly rootfs
        if config.host_config.readonly_rootfs {
            args.push("--read-only");
        }

        // Add TTY
        if config.tty {
            args.push("--tty");
        }

        // Add stdin flags
        if config.attach_stdin {
            args.push("--interactive");
        }

        // Add image
        args.push(&config.image);

        // Add command and entrypoint
        let ep_str;
        if let Some(ref entrypoint) = config.entrypoint {
            ep_str = entrypoint.join(" ");
            args.push("--entrypoint");
            args.push(&ep_str);
        }

        if let Some(ref cmd) = config.cmd {
            args.extend(cmd.iter().map(|s| s.as_str()));
        }

        let output = self.run_command_async(&args).await?;
        // Container ID is in the output
        Ok(output.trim().to_string())
    }

    async fn start_container(&self, id: &str) -> Result<(), RuntimeError> {
        self.run_command_async(&["container", "start", id])
            .await
            .map(|_| ())
    }

    async fn stop_container(&self, id: &str, timeout: u64) -> Result<(), RuntimeError> {
        let timeout_str = timeout.to_string();
        self.run_command_async(&["container", "stop", "--time", &timeout_str, id])
            .await
            .map(|_| ())
    }

    async fn remove_container(&self, id: &str, force: bool) -> Result<(), RuntimeError> {
        let mut args = vec!["container", "rm"];
        if force {
            args.push("--force");
        }
        args.push(id);
        self.run_command_async(&args).await.map(|_| ())
    }

    async fn inspect_container(&self, name: &str) -> Result<Option<ContainerInfo>, RuntimeError> {
        match self.run_json_command(&["container", "inspect", name]) {
            Ok(json) => {
                let arr = json
                    .as_array()
                    .ok_or_else(|| RuntimeError::ParseError("Expected array".to_string()))?;
                let info = arr
                    .first()
                    .ok_or_else(|| RuntimeError::ParseError("Empty array".to_string()))?;

                let id = info["Id"].as_str().unwrap_or("").to_string();
                let name = info["Name"].as_str().unwrap_or("").to_string();
                let image = info["Image"].as_str().unwrap_or("").to_string();

                let state_obj = &info["State"];
                let state = ContainerState {
                    running: state_obj["Running"].as_bool().unwrap_or(false),
                    paused: state_obj["Paused"].as_bool().unwrap_or(false),
                    restarting: state_obj["Restarting"].as_bool().unwrap_or(false),
                    status: state_obj["Status"]
                        .as_str()
                        .unwrap_or("unknown")
                        .to_string(),
                    exit_code: state_obj["ExitCode"].as_i64().map(|c| c as i32),
                    started_at: state_obj["StartedAt"].as_str().map(String::from),
                    finished_at: state_obj["FinishedAt"].as_str().map(String::from),
                };

                let network_settings = info["NetworkSettings"].as_object().map(|ns| {
                    let ip_address = ns["IPAddress"].as_str().map(String::from);
                    let networks = ns["Networks"]
                        .as_object()
                        .map(|nets| {
                            nets.iter()
                                .map(|(k, v)| {
                                    (
                                        k.clone(),
                                        EndpointSettings {
                                            ip_address: v["IPAddress"].as_str().map(String::from),
                                            network_id: v["NetworkID"].as_str().map(String::from),
                                        },
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    NetworkSettings {
                        ip_address,
                        networks,
                    }
                });

                Ok(Some(ContainerInfo {
                    id,
                    name,
                    state,
                    image,
                    network_settings,
                    config: None,
                }))
            }
            Err(RuntimeError::CommandFailed(msg)) if msg.contains("not found") => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn list_containers(
        &self,
        filters: ContainerFilters,
    ) -> Result<Vec<ContainerSummary>, RuntimeError> {
        let mut args = vec!["container", "ls", "--format=json"];

        if filters.all {
            args.push("--all");
        }

        // Apple container might not support all filter types
        // We'll filter after retrieval if needed
        let output = self.run_command_async(&args).await?;

        let containers: Vec<Value> = output
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();

        let mut results = Vec::new();
        for container in containers {
            let id = container["ID"].as_str().unwrap_or("").to_string();
            let names_str = container["Names"].as_str().unwrap_or("");
            let names = vec![names_str.to_string()];
            let image = container["Image"].as_str().unwrap_or("").to_string();
            let state = container["State"].as_str().unwrap_or("").to_string();
            let status = container["Status"].as_str().unwrap_or("").to_string();

            // Labels might need separate inspection
            let labels = HashMap::new();

            // Apply filters
            if let Some(ref name_filters) = filters.name {
                if !name_filters.iter().any(|f| names_str.contains(f)) {
                    continue;
                }
            }

            if let Some(ref status_filters) = filters.status {
                if !status_filters.iter().any(|f| state.contains(f)) {
                    continue;
                }
            }

            results.push(ContainerSummary {
                id,
                names,
                image,
                state,
                status,
                labels,
            });
        }

        Ok(results)
    }

    async fn create_exec(
        &self,
        container: &str,
        config: ExecConfig,
    ) -> Result<String, RuntimeError> {
        // Apple container exec doesn't have separate create/start phases
        // Return a synthetic exec ID that encodes the command
        let exec_id = format!("{}:{}", container, config.cmd.join(" "));
        Ok(exec_id)
    }

    async fn start_exec(&self, exec_id: &str) -> Result<ExecOutput, RuntimeError> {
        // Parse the synthetic exec_id back to container and command
        let parts: Vec<&str> = exec_id.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(RuntimeError::Generic("Invalid exec ID".to_string()));
        }

        let container = parts[0];
        let cmd = parts[1];
        let cmd_parts: Vec<&str> = cmd.split_whitespace().collect();

        let mut args = vec!["container", "exec", container];
        args.extend(&cmd_parts);

        let output = TokioCommand::new(&self.container_bin)
            .args(&args)
            .output()
            .await
            .map_err(|e| RuntimeError::CommandFailed(format!("Failed to execute: {}", e)))?;

        // Combine stdout and stderr
        let mut result = output.stdout;
        result.extend_from_slice(&output.stderr);

        Ok(ExecOutput { output: result })
    }

    async fn inspect_exec(&self, exec_id: &str) -> Result<ExecInfo, RuntimeError> {
        // Since we execute immediately in start_exec, the exec is never running
        Ok(ExecInfo {
            id: exec_id.to_string(),
            running: false,
            exit_code: Some(0),
        })
    }

    async fn logs(
        &self,
        container: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> Result<LogStream, RuntimeError> {
        let mut args = vec!["container", "logs"];

        if follow {
            args.push("--follow");
        }

        let tail_str;
        if let Some(n) = tail {
            args.push("--tail");
            tail_str = n.to_string();
            args.push(&tail_str);
        }

        args.push(container);

        let mut child = TokioCommand::new(&self.container_bin)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| RuntimeError::CommandFailed(format!("Failed to spawn: {}", e)))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::Generic("No stdout".to_string()))?;

        let lines = BufReader::new(stdout).lines();

        use futures::stream::{unfold, StreamExt};
        let stream = unfold(lines, |mut lines| async move {
            match lines.next_line().await {
                Ok(Some(line)) => Some((
                    Ok(LogEntry {
                        stream: LogStreamType::Stdout,
                        data: line.into_bytes(),
                    }),
                    lines,
                )),
                Ok(None) => None,
                Err(e) => Some((Err(RuntimeError::Io(e)), lines)),
            }
        });

        Ok(Box::pin(stream))
    }

    async fn download_from_container(
        &self,
        container: &str,
        path: &str,
    ) -> Result<Vec<u8>, RuntimeError> {
        let container_path = format!("{}:{}", container, path);
        let args = vec!["container", "cp", &container_path, "-"];
        let output = TokioCommand::new(&self.container_bin)
            .args(&args)
            .output()
            .await
            .map_err(|e| RuntimeError::CommandFailed(format!("Failed to execute: {}", e)))?;

        if !output.status.success() {
            return Err(RuntimeError::CommandFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        Ok(output.stdout)
    }

    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>, RuntimeError> {
        let output = self
            .run_command_async(&["volume", "ls", "--format=json"])
            .await?;

        let volumes: Vec<Value> = output
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();

        Ok(volumes
            .into_iter()
            .map(|v| VolumeInfo {
                name: v["Name"].as_str().unwrap_or("").to_string(),
                driver: v["Driver"].as_str().unwrap_or("").to_string(),
                mountpoint: v["Mountpoint"].as_str().unwrap_or("").to_string(),
                labels: HashMap::new(), // May need separate inspection
            })
            .collect())
    }

    async fn remove_volume(&self, name: &str) -> Result<(), RuntimeError> {
        self.run_command_async(&["volume", "rm", name])
            .await
            .map(|_| ())
    }
}
