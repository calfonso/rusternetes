//! Docker and Podman (Docker-compatible API) via bollard.

use anyhow::Result;
use async_trait::async_trait;
use bollard::container as bc;
use bollard::models as bm;
use bollard::Docker;
use futures::StreamExt;

use super::types::*;
use super::ContainerBackend;

/// [`ContainerBackend`] for a Docker or Podman daemon.
#[derive(Clone)]
pub struct DockerBackend {
    docker: Docker,
}

impl DockerBackend {
    /// Connect with bollard's local defaults (`DOCKER_HOST` or the local socket).
    pub fn connect_with_local_defaults() -> Result<Self> {
        Ok(Self {
            docker: Docker::connect_with_local_defaults()?,
        })
    }
}

#[async_trait]
impl ContainerBackend for DockerBackend {
    async fn version_identity(&self) -> Result<String> {
        let version = self.docker.version().await?;
        Ok(version
            .platform
            .as_ref()
            .map(|p| p.name.as_str())
            .into_iter()
            .chain(version.components.iter().flatten().map(|c| c.name.as_str()))
            .collect::<Vec<_>>()
            .join(" "))
    }

    async fn inspect_image(&self, image: &str) -> Result<ImageInspect> {
        let info = self.docker.inspect_image(image).await?;
        Ok(ImageInspect {
            id: info.id,
            config: info.config.map(image_config),
        })
    }

    fn pull_image<'a>(&'a self, image: &'a str) -> BackendStream<'a, PullProgress> {
        let options = bollard::image::CreateImageOptions {
            from_image: image,
            ..Default::default()
        };
        self.docker
            .create_image(Some(options), None, None)
            .map(|r| {
                r.map(|info| PullProgress {
                    status: info.status,
                    progress: info.progress,
                    error: info.error,
                })
                .map_err(Into::into)
            })
            .boxed()
    }

    async fn create_container(&self, name: &str, config: ContainerConfig) -> Result<String> {
        let options = bc::CreateContainerOptions {
            name: name.to_string(),
            ..Default::default()
        };
        let created = self
            .docker
            .create_container(Some(options), to_bollard_config(config))
            .await?;
        Ok(created.id)
    }

    async fn start_container(&self, name: &str) -> Result<()> {
        self.docker
            .start_container(name, None::<bc::StartContainerOptions<String>>)
            .await?;
        Ok(())
    }

    async fn stop_container(&self, name: &str, options: StopContainerOptions) -> Result<()> {
        self.docker
            .stop_container(name, Some(bc::StopContainerOptions { t: options.t }))
            .await?;
        Ok(())
    }

    async fn remove_container(&self, name: &str, options: RemoveContainerOptions) -> Result<()> {
        self.docker
            .remove_container(
                name,
                Some(bc::RemoveContainerOptions {
                    force: options.force,
                    ..Default::default()
                }),
            )
            .await?;
        Ok(())
    }

    async fn list_containers(
        &self,
        options: ListContainersOptions,
    ) -> Result<Vec<ContainerSummary>> {
        let options = bc::ListContainersOptions::<String> {
            all: options.all,
            filters: options.filters,
            ..Default::default()
        };
        let containers = self.docker.list_containers(Some(options)).await?;
        Ok(containers
            .into_iter()
            .map(|c| ContainerSummary {
                id: c.id,
                names: c.names,
                image: c.image,
                created: c.created,
                state: c.state,
                host_config: c.host_config.map(|hc| InspectHostConfig {
                    network_mode: hc.network_mode,
                }),
            })
            .collect())
    }

    async fn inspect_container(&self, name: &str) -> Result<ContainerInspect> {
        let i = self
            .docker
            .inspect_container(name, None::<bc::InspectContainerOptions>)
            .await?;
        Ok(ContainerInspect {
            id: i.id,
            name: i.name,
            image: i.image,
            restart_count: i.restart_count,
            state: i.state.map(|s| ContainerStateInfo {
                status: s.status.map(state_status),
                running: s.running,
                oom_killed: s.oom_killed,
                pid: s.pid,
                exit_code: s.exit_code,
                error: s.error,
                started_at: s.started_at,
                finished_at: s.finished_at,
            }),
            network_settings: i.network_settings.map(|ns| NetworkSettings {
                ip_address: ns.ip_address,
                networks: ns.networks.map(|nets| {
                    nets.into_iter()
                        .map(|(k, v)| {
                            (
                                k,
                                EndpointSettings {
                                    ip_address: v.ip_address,
                                },
                            )
                        })
                        .collect()
                }),
            }),
            host_config: i.host_config.map(|hc| InspectHostConfig {
                network_mode: hc.network_mode,
            }),
            config: i.config.map(image_config),
        })
    }

    async fn update_container(&self, name: &str, options: UpdateContainerOptions) -> Result<()> {
        let update = bc::UpdateContainerOptions::<String> {
            cpu_period: options.cpu_period,
            cpu_quota: options.cpu_quota,
            cpu_shares: options.cpu_shares.map(|s| s as isize),
            memory: options.memory,
            memory_swap: options.memory_swap,
            ..Default::default()
        };
        self.docker.update_container(name, update).await?;
        Ok(())
    }

    fn stats<'a>(
        &'a self,
        name: &'a str,
        options: StatsOptions,
    ) -> BackendStream<'a, ContainerStats> {
        let options = bc::StatsOptions {
            stream: options.stream,
            one_shot: options.one_shot,
        };
        self.docker
            .stats(name, Some(options))
            .map(|r| {
                r.map(|s| ContainerStats {
                    memory_usage: s.memory_stats.usage,
                    memory_cache: s.memory_stats.stats.as_ref().map(|st| match st {
                        bc::MemoryStatsStats::V1(v1) => v1.cache,
                        bc::MemoryStatsStats::V2(v2) => v2.inactive_file,
                    }),
                    cpu_total_usage: s.cpu_stats.cpu_usage.total_usage,
                    system_cpu_usage: s.cpu_stats.system_cpu_usage,
                    precpu_total_usage: s.precpu_stats.cpu_usage.total_usage,
                    precpu_system_cpu_usage: s.precpu_stats.system_cpu_usage,
                    online_cpus: s.cpu_stats.online_cpus,
                    blkio_service_bytes: s
                        .blkio_stats
                        .io_service_bytes_recursive
                        .map(|entries| entries.iter().map(|e| e.value).collect()),
                })
                .map_err(Into::into)
            })
            .boxed()
    }

    async fn create_exec(&self, name: &str, options: CreateExecOptions) -> Result<String> {
        let config = bollard::exec::CreateExecOptions {
            cmd: options.cmd,
            attach_stdout: options.attach_stdout,
            attach_stderr: options.attach_stderr,
            attach_stdin: options.attach_stdin,
            tty: options.tty,
            ..Default::default()
        };
        Ok(self.docker.create_exec(name, config).await?.id)
    }

    async fn start_exec(&self, exec_id: &str) -> Result<StartExecResults> {
        Ok(match self.docker.start_exec(exec_id, None).await? {
            bollard::exec::StartExecResults::Attached { output, .. } => {
                StartExecResults::Attached {
                    output: output
                        .map(|r| r.map(log_output).map_err(Into::into))
                        .boxed(),
                }
            }
            bollard::exec::StartExecResults::Detached => StartExecResults::Detached,
        })
    }

    async fn inspect_exec(&self, exec_id: &str) -> Result<ExecInspect> {
        let info = self.docker.inspect_exec(exec_id).await?;
        Ok(ExecInspect {
            running: info.running,
            exit_code: info.exit_code,
        })
    }

    fn logs<'a>(&'a self, name: &'a str, options: LogsOptions) -> BackendStream<'a, LogOutput> {
        let options = bc::LogsOptions::<String> {
            stdout: options.stdout,
            stderr: options.stderr,
            tail: options.tail,
            ..Default::default()
        };
        self.docker
            .logs(name, Some(options))
            .map(|r| r.map(log_output).map_err(Into::into))
            .boxed()
    }

    fn download_from_container<'a>(
        &'a self,
        name: &'a str,
        path: &'a str,
    ) -> BackendStream<'a, Vec<u8>> {
        self.docker
            .download_from_container(
                name,
                Some(bc::DownloadFromContainerOptions {
                    path: path.to_string(),
                }),
            )
            .map(|r| r.map(|b| b.to_vec()).map_err(Into::into))
            .boxed()
    }

    async fn list_volumes(&self) -> Result<Vec<String>> {
        let volumes = self.docker.list_volumes::<String>(None).await?;
        Ok(volumes
            .volumes
            .unwrap_or_default()
            .into_iter()
            .map(|v| v.name)
            .collect())
    }

    async fn remove_volume(&self, name: &str) -> Result<()> {
        self.docker.remove_volume(name, None).await?;
        Ok(())
    }
}

fn image_config(c: bm::ContainerConfig) -> ImageConfig {
    ImageConfig {
        entrypoint: c.entrypoint,
        cmd: c.cmd,
        env: c.env,
        user: c.user,
        working_dir: c.working_dir,
    }
}

fn state_status(s: bm::ContainerStateStatusEnum) -> ContainerStateStatus {
    use bm::ContainerStateStatusEnum as S;
    match s {
        S::EMPTY => ContainerStateStatus::Empty,
        S::CREATED => ContainerStateStatus::Created,
        S::RUNNING => ContainerStateStatus::Running,
        S::PAUSED => ContainerStateStatus::Paused,
        S::RESTARTING => ContainerStateStatus::Restarting,
        S::REMOVING => ContainerStateStatus::Removing,
        S::EXITED => ContainerStateStatus::Exited,
        S::DEAD => ContainerStateStatus::Dead,
    }
}

fn log_output(l: bc::LogOutput) -> LogOutput {
    match l {
        bc::LogOutput::StdErr { message } => LogOutput::StdErr {
            message: message.to_vec(),
        },
        bc::LogOutput::StdOut { message } => LogOutput::StdOut {
            message: message.to_vec(),
        },
        bc::LogOutput::StdIn { message } => LogOutput::StdIn {
            message: message.to_vec(),
        },
        bc::LogOutput::Console { message } => LogOutput::Console {
            message: message.to_vec(),
        },
    }
}

fn to_bollard_config(c: ContainerConfig) -> bc::Config<String> {
    bc::Config {
        image: c.image,
        entrypoint: c.entrypoint,
        cmd: c.cmd,
        env: c.env,
        working_dir: c.working_dir,
        user: c.user,
        hostname: c.hostname,
        exposed_ports: c.exposed_ports,
        host_config: c.host_config.map(|h| bm::HostConfig {
            binds: h.binds,
            mounts: h.mounts.map(|ms| {
                ms.into_iter()
                    .map(|m| bm::Mount {
                        target: m.target,
                        source: m.source,
                        read_only: m.read_only,
                        ..Default::default()
                    })
                    .collect()
            }),
            tmpfs: h.tmpfs,
            port_bindings: h.port_bindings.map(|pb| {
                pb.into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            v.map(|bs| {
                                bs.into_iter()
                                    .map(|b| bm::PortBinding {
                                        host_ip: b.host_ip,
                                        host_port: b.host_port,
                                    })
                                    .collect()
                            }),
                        )
                    })
                    .collect()
            }),
            dns: h.dns,
            dns_search: h.dns_search,
            dns_options: h.dns_options,
            network_mode: h.network_mode,
            ipc_mode: h.ipc_mode,
            pid_mode: h.pid_mode,
            uts_mode: h.uts_mode,
            sysctls: h.sysctls,
            memory: h.memory,
            cpu_period: h.cpu_period,
            cpu_quota: h.cpu_quota,
            cpu_shares: h.cpu_shares,
            readonly_rootfs: h.readonly_rootfs,
            security_opt: h.security_opt,
            cap_add: h.cap_add,
            cap_drop: h.cap_drop,
            privileged: h.privileged,
            ..Default::default()
        }),
        ..Default::default()
    }
}
