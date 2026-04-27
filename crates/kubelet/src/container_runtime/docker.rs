// Docker/Podman runtime implementation using bollard

use super::*;
use async_trait::async_trait;
use bollard::Docker;
use bollard::image::CreateImageOptions;
use bollard::container::{
    CreateContainerOptions, InspectContainerOptions, ListContainersOptions,
    RemoveContainerOptions, StopContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{ContainerInspectResponse, ContainerSummary as BollardSummary};
use futures::StreamExt;
use std::collections::HashMap;
use std::default::Default;

pub struct DockerRuntime {
    docker: Docker,
}

impl DockerRuntime {
    pub fn new() -> Result<Self, RuntimeError> {
        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| RuntimeError::BollardError(e.to_string()))?;
        Ok(Self { docker })
    }
}

#[async_trait]
impl ContainerRuntime for DockerRuntime {
    async fn inspect_image(&self, image: &str) -> Result<Option<ImageInfo>, RuntimeError> {
        match self.docker.inspect_image(image).await {
            Ok(info) => {
                let config = info.config.map(|c| ImageConfig {
                    entrypoint: c.entrypoint,
                    cmd: c.cmd,
                    env: c.env,
                    working_dir: c.working_dir,
                    user: c.user,
                });

                Ok(Some(ImageInfo {
                    id: info.id.unwrap_or_default(),
                    repo_tags: info.repo_tags.unwrap_or_default(),
                    size: info.size.unwrap_or(0) as u64,
                    config,
                }))
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(None),
            Err(e) => Err(RuntimeError::BollardError(e.to_string())),
        }
    }

    async fn pull_image(&self, image: &str) -> Result<(), RuntimeError> {
        let options = Some(CreateImageOptions {
            from_image: image,
            ..Default::default()
        });

        let mut stream = self.docker.create_image(options, None, None);
        while let Some(result) = stream.next().await {
            result.map_err(|e| RuntimeError::BollardError(e.to_string()))?;
        }

        Ok(())
    }

    async fn create_container(
        &self,
        name: &str,
        config: ContainerConfig,
    ) -> Result<String, RuntimeError> {
        let bollard_config = bollard::container::Config {
            image: Some(config.image),
            hostname: config.hostname,
            env: Some(config.env),
            cmd: config.cmd,
            entrypoint: config.entrypoint,
            working_dir: config.working_dir,
            user: config.user,
            labels: Some(config.labels),
            tty: Some(config.tty),
            attach_stdin: Some(config.attach_stdin),
            attach_stdout: Some(config.attach_stdout),
            attach_stderr: Some(config.attach_stderr),
            open_stdin: Some(config.open_stdin),
            stdin_once: Some(config.stdin_once),
            host_config: Some(bollard::models::HostConfig {
                binds: Some(config.host_config.binds),
                network_mode: config.host_config.network_mode,
                privileged: Some(config.host_config.privileged),
                cap_add: Some(config.host_config.cap_add),
                cap_drop: Some(config.host_config.cap_drop),
                devices: Some(
                    config
                        .host_config
                        .devices
                        .into_iter()
                        .map(|d| bollard::models::DeviceMapping {
                            path_on_host: Some(d.path_on_host),
                            path_in_container: Some(d.path_in_container),
                            cgroup_permissions: Some(d.cgroup_permissions),
                        })
                        .collect(),
                ),
                ulimits: Some(
                    config
                        .host_config
                        .ulimits
                        .into_iter()
                        .map(|u| bollard::models::ResourcesUlimits {
                            name: Some(u.name),
                            soft: Some(u.soft),
                            hard: Some(u.hard),
                        })
                        .collect(),
                ),
                dns: Some(config.host_config.dns),
                dns_search: Some(config.host_config.dns_search),
                extra_hosts: Some(config.host_config.extra_hosts),
                readonly_rootfs: Some(config.host_config.readonly_rootfs),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions { name, ..Default::default() };
        let response = self
            .docker
            .create_container(Some(options), bollard_config)
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))?;

        Ok(response.id)
    }

    async fn start_container(&self, id: &str) -> Result<(), RuntimeError> {
        self.docker
            .start_container(id, None::<bollard::container::StartContainerOptions<String>>)
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))
    }

    async fn stop_container(&self, id: &str, timeout: u64) -> Result<(), RuntimeError> {
        let options = StopContainerOptions { t: timeout as i64 };
        self.docker
            .stop_container(id, Some(options))
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))
    }

    async fn remove_container(&self, id: &str, force: bool) -> Result<(), RuntimeError> {
        let options = RemoveContainerOptions {
            force,
            ..Default::default()
        };
        self.docker
            .remove_container(id, Some(options))
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))
    }

    async fn inspect_container(&self, name: &str) -> Result<Option<ContainerInfo>, RuntimeError> {
        match self
            .docker
            .inspect_container(name, None::<InspectContainerOptions>)
            .await
        {
            Ok(info) => Ok(Some(convert_container_info(info))),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(None),
            Err(e) => Err(RuntimeError::BollardError(e.to_string())),
        }
    }

    async fn list_containers(
        &self,
        filters: ContainerFilters,
    ) -> Result<Vec<ContainerSummary>, RuntimeError> {
        let mut filter_map = HashMap::new();

        if let Some(names) = filters.name {
            filter_map.insert("name".to_string(), names);
        }
        if let Some(labels) = filters.label {
            filter_map.insert("label".to_string(), labels);
        }
        if let Some(status) = filters.status {
            filter_map.insert("status".to_string(), status);
        }

        let options = ListContainersOptions {
            all: filters.all,
            filters: filter_map,
            ..Default::default()
        };

        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))?;

        Ok(containers.into_iter().map(convert_container_summary).collect())
    }

    async fn create_exec(
        &self,
        container: &str,
        config: ExecConfig,
    ) -> Result<String, RuntimeError> {
        let options = CreateExecOptions {
            attach_stdout: Some(config.attach_stdout),
            attach_stderr: Some(config.attach_stderr),
            attach_stdin: Some(config.attach_stdin),
            tty: Some(config.tty),
            cmd: Some(config.cmd),
            env: Some(config.env),
            user: config.user,
            working_dir: config.working_dir,
            privileged: Some(config.privileged),
            ..Default::default()
        };

        let exec = self
            .docker
            .create_exec(container, options)
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))?;

        Ok(exec.id)
    }

    async fn start_exec(&self, exec_id: &str) -> Result<ExecOutput, RuntimeError> {
        match self.docker.start_exec(exec_id, None).await {
            Ok(StartExecResults::Attached { mut output, .. }) => {
                let mut result = Vec::new();
                while let Some(chunk) = output.next().await {
                    let chunk = chunk.map_err(|e| RuntimeError::BollardError(e.to_string()))?;
                    result.extend_from_slice(&chunk.into_bytes());
                }
                Ok(ExecOutput { output: result })
            }
            Ok(StartExecResults::Detached) => Ok(ExecOutput { output: Vec::new() }),
            Err(e) => Err(RuntimeError::BollardError(e.to_string())),
        }
    }

    async fn inspect_exec(&self, exec_id: &str) -> Result<ExecInfo, RuntimeError> {
        let info = self
            .docker
            .inspect_exec(exec_id)
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))?;

        Ok(ExecInfo {
            id: info.id.unwrap_or_default(),
            running: info.running.unwrap_or(false),
            exit_code: info.exit_code,
        })
    }

    async fn logs(
        &self,
        container: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> Result<LogStream, RuntimeError> {
        let options = bollard::container::LogsOptions {
            follow,
            stdout: true,
            stderr: true,
            tail: tail.map(|t| t.to_string()).unwrap_or_else(|| "all".to_string()),
            ..Default::default()
        };

        let stream = self.docker.logs(container, Some(options));

        let mapped_stream = stream.map(|result| {
            result
                .map(|output| {
                    let (stream, data) = match output {
                        bollard::container::LogOutput::StdOut { message } => {
                            (LogStreamType::Stdout, message.to_vec())
                        }
                        bollard::container::LogOutput::StdErr { message } => {
                            (LogStreamType::Stderr, message.to_vec())
                        }
                        _ => (LogStreamType::Stdout, Vec::new()),
                    };
                    LogEntry { stream, data }
                })
                .map_err(|e| RuntimeError::BollardError(e.to_string()))
        });

        Ok(Box::pin(mapped_stream))
    }

    async fn download_from_container(
        &self,
        container: &str,
        path: &str,
    ) -> Result<Vec<u8>, RuntimeError> {
        let mut stream = self
            .docker
            .download_from_container(container, Some(bollard::container::DownloadFromContainerOptions { path }));

        let mut result = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| RuntimeError::BollardError(e.to_string()))?;
            result.extend_from_slice(&chunk);
        }

        Ok(result)
    }

    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>, RuntimeError> {
        let response = self
            .docker
            .list_volumes::<String>(None)
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))?;

        Ok(response
            .volumes
            .unwrap_or_default()
            .into_iter()
            .map(|v| VolumeInfo {
                name: v.name,
                driver: v.driver,
                mountpoint: v.mountpoint,
                labels: v.labels,
            })
            .collect())
    }

    async fn remove_volume(&self, name: &str) -> Result<(), RuntimeError> {
        self.docker
            .remove_volume(name, None)
            .await
            .map_err(|e| RuntimeError::BollardError(e.to_string()))
    }
}

// Helper conversion functions

fn convert_container_info(info: ContainerInspectResponse) -> ContainerInfo {
    let state = info.state.map(|s| ContainerState {
        running: s.running.unwrap_or(false),
        paused: s.paused.unwrap_or(false),
        restarting: s.restarting.unwrap_or(false),
        status: s.status.map(|st| format!("{:?}", st)).unwrap_or_else(|| "unknown".to_string()),
        exit_code: s.exit_code.map(|c| c as i32),
        started_at: s.started_at,
        finished_at: s.finished_at,
    }).unwrap_or(ContainerState {
        running: false,
        paused: false,
        restarting: false,
        status: "unknown".to_string(),
        exit_code: None,
        started_at: None,
        finished_at: None,
    });

    let network_settings = info.network_settings.map(|ns| NetworkSettings {
        ip_address: ns.ip_address,
        networks: ns
            .networks
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    EndpointSettings {
                        ip_address: v.ip_address,
                        network_id: v.network_id,
                    },
                )
            })
            .collect(),
    });

    ContainerInfo {
        id: info.id.unwrap_or_default(),
        name: info.name.unwrap_or_default(),
        state,
        image: info.image.unwrap_or_default(),
        network_settings,
        config: None, // TODO: Convert if needed
    }
}

fn convert_container_summary(summary: BollardSummary) -> ContainerSummary {
    ContainerSummary {
        id: summary.id.unwrap_or_default(),
        names: summary.names.unwrap_or_default(),
        image: summary.image.unwrap_or_default(),
        state: summary.state.unwrap_or_default(),
        status: summary.status.unwrap_or_default(),
        labels: summary.labels.unwrap_or_default(),
    }
}
