//! Daemonless [`ContainerBackend`] on youki's `libcontainer` (feature `youki`).
//!
//! Layout under the backend root (default `/var/lib/rusternetes/youki`):
//!
//! ```text
//! images/<config digest>/{rootfs, image.json}   flattened image layers
//! refs/<reference>.json                         reference -> image
//! containers/<name>/meta.json                   what Docker would remember
//! containers/<name>/{config.json, rootfs}       the OCI bundle; rootfs is an
//!                                               overlay on the image rootfs
//! containers/<name>/{upper, work}               overlay upper and work dirs
//! containers/<name>/{output.log, exit.json}     stdout+stderr, exit status
//! containers/<name>/exec/<id>.*                 exec args, output, exit
//! ```
//!
//! libcontainer state lives under `/run/rusternetes/youki`. Each container
//! and each exec runs under a `rusternetes-youki-shim` process (see
//! [`shim`]). Pods get their network from CNI: the kubelet creates the pod
//! network namespace and containers join it, so no pause container is used.

pub mod image;
pub mod shim;
pub mod spec;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use libcontainer::container::{Container, ContainerStatus};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;

use super::types::*;
use super::ContainerBackend;
use image::{ImageStore, StoredImage};
use shim::ExitRecord;
use spec::Ns;

/// cgroup v2 parent of all containers, relative to the cgroup root.
const CGROUP_PARENT: &str = "rusternetes";

/// Where the backend keeps its files and how it finds the shim.
#[derive(Debug, Clone)]
pub struct YoukiConfig {
    pub root: PathBuf,
    pub state_root: PathBuf,
    pub shim: PathBuf,
}

impl YoukiConfig {
    /// `KUBELET_YOUKI_ROOT`, `KUBELET_YOUKI_STATE` and `KUBELET_YOUKI_SHIM`,
    /// defaulting to `/var/lib/rusternetes/youki`, `/run/rusternetes/youki`
    /// and `rusternetes-youki-shim` next to the running binary.
    pub fn from_env() -> Self {
        let var = |k: &str| std::env::var_os(k).map(PathBuf::from);
        let shim = var("KUBELET_YOUKI_SHIM").unwrap_or_else(|| {
            std::env::current_exe()
                .ok()
                .map(|e| e.with_file_name("rusternetes-youki-shim"))
                .filter(|p| p.exists())
                .unwrap_or_else(|| PathBuf::from("rusternetes-youki-shim"))
        });
        Self {
            root: var("KUBELET_YOUKI_ROOT")
                .unwrap_or_else(|| PathBuf::from("/var/lib/rusternetes/youki")),
            state_root: var("KUBELET_YOUKI_STATE")
                .unwrap_or_else(|| PathBuf::from("/run/rusternetes/youki")),
            shim,
        }
    }
}

/// What the backend remembers about a container.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meta {
    id: String,
    name: String,
    image: String,
    image_id: String,
    /// Seconds since the epoch.
    created: i64,
    started_at: Option<String>,
    network_mode: Option<String>,
    config: StoredProcess,
    cgroups_path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoredProcess {
    entrypoint: Option<Vec<String>>,
    cmd: Option<Vec<String>>,
    env: Option<Vec<String>>,
    user: Option<String>,
    working_dir: Option<String>,
}

struct Inner {
    cfg: YoukiConfig,
    images: ImageStore,
    http: reqwest::Client,
}

/// [`ContainerBackend`] running containers with libcontainer, no daemon.
#[derive(Clone)]
pub struct YoukiBackend {
    inner: Arc<Inner>,
}

impl YoukiBackend {
    pub fn new(cfg: YoukiConfig) -> Result<Self> {
        for d in [
            cfg.root.join("images"),
            cfg.root.join("refs"),
            cfg.root.join("containers"),
            cfg.root.join("tmp"),
            cfg.state_root.clone(),
        ] {
            fs::create_dir_all(&d).with_context(|| format!("creating {}", d.display()))?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                images: ImageStore::new(cfg.root.clone()),
                cfg,
                http: reqwest::Client::builder()
                    .user_agent("rusternetes-kubelet")
                    .build()?,
            }),
        })
    }

    async fn blocking<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Inner) -> Result<T> + Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&inner)).await?
    }
}

fn not_found(name: &str) -> anyhow::Error {
    anyhow!("No such container: {}", name)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// Docker-style name filter: `^/prefix` or a plain substring of `/name`.
fn name_matches(name: &str, pattern: &str) -> bool {
    let full = format!("/{}", name);
    match pattern.strip_prefix('^') {
        Some(prefix) => full.starts_with(prefix),
        None => full.contains(pattern),
    }
}

/// Resolve `"uid"`, `"uid:gid"` or user/group names against the rootfs.
fn resolve_user(user: Option<&str>, rootfs: &Path) -> Result<(u32, u32)> {
    let Some(user) = user.filter(|u| !u.is_empty()) else {
        return Ok((0, 0));
    };
    let (u, g) = match user.split_once(':') {
        Some((u, g)) => (u, Some(g)),
        None => (user, None),
    };
    let lookup = |file: &str, key: &str| -> Option<(u32, u32)> {
        let text = fs::read_to_string(rootfs.join(file)).ok()?;
        text.lines().find_map(|l| {
            let f: Vec<&str> = l.split(':').collect();
            (f.first() == Some(&key)).then(|| {
                (
                    f.get(2).and_then(|x| x.parse().ok()).unwrap_or(0),
                    f.get(3).and_then(|x| x.parse().ok()).unwrap_or(0),
                )
            })
        })
    };
    let (uid, primary_gid) = match u.parse::<u32>() {
        Ok(uid) => (
            uid,
            fs::read_to_string(rootfs.join("etc/passwd"))
                .ok()
                .and_then(|t| {
                    t.lines().find_map(|l| {
                        let f: Vec<&str> = l.split(':').collect();
                        (f.get(2) == Some(&u)).then(|| f.get(3)?.parse().ok())?
                    })
                })
                .unwrap_or(0),
        ),
        Err(_) => lookup("etc/passwd", u).ok_or_else(|| anyhow!("unable to find user {}", u))?,
    };
    let gid = match g {
        None => primary_gid,
        Some(g) => match g.parse::<u32>() {
            Ok(gid) => gid,
            Err(_) => {
                lookup("etc/group", g)
                    .ok_or_else(|| anyhow!("unable to find group {}", g))?
                    .0
            }
        },
    };
    Ok((uid, gid))
}

impl Inner {
    fn dir(&self, name: &str) -> PathBuf {
        self.cfg.root.join("containers").join(name)
    }

    fn metas(&self) -> Vec<Meta> {
        let mut out: Vec<Meta> = fs::read_dir(self.cfg.root.join("containers"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| read_json(&e.path().join("meta.json")))
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Find a container by name, `/name` or ID.
    fn meta(&self, name_or_id: &str) -> Result<Meta> {
        let name = name_or_id.trim_start_matches('/');
        if let Some(m) = read_json(&self.dir(name).join("meta.json")) {
            return Ok(m);
        }
        self.metas()
            .into_iter()
            .find(|m| m.id == name || (name.len() >= 12 && m.id.starts_with(name)))
            .ok_or_else(|| not_found(name_or_id))
    }

    fn load(&self, name: &str) -> Option<Container> {
        let mut c = Container::load(self.cfg.state_root.join(name)).ok()?;
        let _ = c.refresh_status();
        Some(c)
    }

    fn state(&self, m: &Meta) -> ContainerStateInfo {
        let exit: Option<ExitRecord> = read_json(&self.dir(&m.name).join("exit.json"));
        let lc = self.load(&m.name);
        let (status, pid) = match (&exit, &lc) {
            (Some(_), _) => (ContainerStateStatus::Exited, None),
            (None, Some(c)) => {
                let s = match c.status() {
                    ContainerStatus::Creating | ContainerStatus::Created => {
                        ContainerStateStatus::Created
                    }
                    ContainerStatus::Running => ContainerStateStatus::Running,
                    ContainerStatus::Paused => ContainerStateStatus::Paused,
                    ContainerStatus::Stopped => ContainerStateStatus::Exited,
                };
                (s, c.pid().map(|p| p.as_raw() as i64))
            }
            (None, None) => (ContainerStateStatus::Dead, None),
        };
        let lost = exit.is_none() && status == ContainerStateStatus::Exited;
        ContainerStateInfo {
            status: Some(status),
            running: Some(status == ContainerStateStatus::Running),
            oom_killed: Some(false),
            pid,
            exit_code: exit.as_ref().map(|e| e.exit_code).or(lost.then_some(255)),
            error: exit
                .as_ref()
                .and_then(|e| e.error.clone())
                .or_else(|| lost.then(|| "exit status was not recorded".to_string())),
            started_at: m.started_at.clone(),
            finished_at: exit.map(|e| e.finished_at),
        }
    }

    fn state_word(status: Option<ContainerStateStatus>) -> &'static str {
        match status {
            Some(ContainerStateStatus::Created) => "created",
            Some(ContainerStateStatus::Running) => "running",
            Some(ContainerStateStatus::Paused) => "paused",
            Some(ContainerStateStatus::Dead) => "dead",
            _ => "exited",
        }
    }

    fn running_pid(&self, name: &str) -> Result<i64> {
        let m = self.meta(name)?;
        let st = self.state(&m);
        match (st.running, st.pid) {
            (Some(true), Some(pid)) => Ok(pid),
            _ => bail!("container {} is not running", name),
        }
    }

    /// `"container:<name>"` / `"ns:<path>"` / `"host"` / `"none"` to a namespace.
    fn namespace(&self, mode: Option<&str>, kind: &str, default: Ns) -> Result<Ns> {
        match mode.filter(|m| !m.is_empty()) {
            None | Some("shareable") | Some("private") => Ok(default),
            Some("host") => Ok(Ns::Host),
            Some("none") if kind == "net" => Ok(Ns::New),
            Some(m) if m.starts_with("container:") => {
                let pid = self.running_pid(&m["container:".len()..])?;
                Ok(Ns::Join(PathBuf::from(format!(
                    "/proc/{}/ns/{}",
                    pid, kind
                ))))
            }
            Some(m) if m.starts_with("ns:") => Ok(Ns::Join(PathBuf::from(&m[3..]))),
            Some(m) if kind == "net" => bail!(
                "unsupported on youki: network {:?} is a Docker network; \
                 the youki backend needs CNI for pod networking",
                m
            ),
            Some(m) => bail!("unsupported on youki: {} mode {:?}", kind, m),
        }
    }

    fn create(&self, name: &str, config: &ContainerConfig) -> Result<(Meta, PathBuf)> {
        let dir = self.dir(name);
        if let Ok(existing) = self.meta(name) {
            bail!(
                "Conflict. The container name \"/{}\" is already in use by container \"{}\"",
                name,
                existing.id
            );
        }
        let image_ref = config
            .image
            .clone()
            .ok_or_else(|| anyhow!("no image specified"))?;
        let StoredImage {
            id: image_id,
            config: image_config,
            ..
        } = self.images.get(&image_ref)?;
        let image_config: ImageConfig = image_config.into();
        let hc = config.host_config.clone().unwrap_or_default();

        // Resolve everything that can fail before touching the disk.
        let net = self.namespace(hc.network_mode.as_deref(), "net", Ns::New)?;
        let ipc = self.namespace(hc.ipc_mode.as_deref(), "ipc", Ns::New)?;
        let pid = self.namespace(hc.pid_mode.as_deref(), "pid", Ns::New)?;
        let uts = self.namespace(hc.uts_mode.as_deref(), "uts", Ns::New)?;

        fs::create_dir_all(&dir)?;
        let result = (|| -> Result<Meta> {
            let rootfs = dir.join("rootfs");
            for d in ["rootfs", "upper", "work", "exec"] {
                fs::create_dir_all(dir.join(d))?;
            }
            let opts = format!(
                "lowerdir={},upperdir={},workdir={}",
                self.images.rootfs(&image_id).display(),
                dir.join("upper").display(),
                dir.join("work").display()
            );
            nix::mount::mount(
                Some("overlay"),
                &rootfs,
                Some("overlay"),
                nix::mount::MsFlags::empty(),
                Some(opts.as_str()),
            )
            .context("mounting overlay rootfs")?;

            let user = config.user.as_deref().or(image_config.user.as_deref());
            let (uid, gid) = resolve_user(user, &rootfs)?;
            let cwd = config
                .working_dir
                .clone()
                .filter(|w| !w.is_empty())
                .or_else(|| image_config.working_dir.clone())
                .unwrap_or_else(|| "/".into());
            fs::create_dir_all(rootfs.join(cwd.trim_start_matches('/')))?;

            let resolv = dir.join("resolv.conf");
            match spec::resolv_conf(&hc) {
                Some(text) => fs::write(&resolv, text)?,
                None => {
                    fs::copy("/etc/resolv.conf", &resolv).unwrap_or_default();
                }
            }
            let cgroups_path = format!("/{}/{}", CGROUP_PARENT, name);
            let spec = spec::build(&spec::SpecInput {
                config,
                image: &image_config,
                rootfs: &rootfs,
                uid,
                gid,
                net,
                ipc,
                pid,
                uts,
                resolv_conf: resolv.exists().then_some(resolv.as_path()),
                cgroups_path: &cgroups_path,
            })?;
            fs::write(dir.join("config.json"), serde_json::to_vec_pretty(&spec)?)?;

            let id = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            let meta = Meta {
                id,
                name: name.to_string(),
                image: image_ref.clone(),
                image_id: image_id.clone(),
                created: chrono::Utc::now().timestamp(),
                started_at: None,
                network_mode: hc.network_mode.clone(),
                config: StoredProcess {
                    entrypoint: config
                        .entrypoint
                        .clone()
                        .or(image_config.entrypoint.clone()),
                    cmd: config.cmd.clone().or(image_config.cmd.clone()),
                    env: Some(spec::process_env(
                        config,
                        &image_config,
                        config.hostname.as_deref().unwrap_or(""),
                    )),
                    user: user.map(str::to_string),
                    working_dir: Some(cwd),
                },
                cgroups_path,
            };
            image::write_json(&dir.join("meta.json"), &meta)?;
            Ok(meta)
        })();
        match result {
            Ok(meta) => Ok((meta, dir)),
            Err(e) => {
                self.cleanup_dir(&dir);
                Err(e)
            }
        }
    }

    fn cleanup_dir(&self, dir: &Path) {
        let _ = nix::mount::umount2(&dir.join("rootfs"), nix::mount::MntFlags::MNT_DETACH);
        let _ = fs::remove_dir_all(dir);
    }

    fn signal(&self, name: &str, sig: nix::sys::signal::Signal, all: bool) -> Result<()> {
        let mut c = self.load(name).ok_or_else(|| not_found(name))?;
        if c.can_kill() {
            c.kill(sig, all)?;
        }
        Ok(())
    }

    fn is_running(&self, m: &Meta) -> bool {
        self.state(m).running == Some(true)
    }

    fn remove(&self, name: &str, force: bool) -> Result<()> {
        let m = self.meta(name)?;
        if self.is_running(&m) {
            if !force {
                bail!(
                    "Conflict. You cannot remove a running container {}. Stop the container before attempting removal or force remove",
                    m.id
                );
            }
            let _ = self.signal(&m.name, nix::sys::signal::Signal::SIGKILL, true);
            for _ in 0..100 {
                if !self.is_running(&m) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        if let Some(mut c) = self.load(&m.name) {
            c.delete(true)?;
        }
        self.cleanup_dir(&self.dir(&m.name));
        Ok(())
    }

    fn inspect(&self, name: &str) -> Result<ContainerInspect> {
        let m = self.meta(name)?;
        let state = self.state(&m);
        let ip = state
            .pid
            .filter(|_| state.running == Some(true))
            .and_then(|pid| netns_ipv4(&format!("/proc/{}/ns/net", pid)));
        Ok(ContainerInspect {
            id: Some(m.id.clone()),
            name: Some(format!("/{}", m.name)),
            image: Some(m.image_id.clone()),
            restart_count: Some(0),
            state: Some(state),
            network_settings: Some(NetworkSettings {
                ip_address: ip,
                networks: None,
            }),
            host_config: Some(InspectHostConfig {
                network_mode: m.network_mode.clone(),
            }),
            config: Some(ImageConfig {
                entrypoint: m.config.entrypoint.clone(),
                cmd: m.config.cmd.clone(),
                env: m.config.env.clone(),
                user: m.config.user.clone(),
                working_dir: m.config.working_dir.clone(),
            }),
        })
    }

    fn cgroup_dir(&self, name: &str) -> Result<PathBuf> {
        let m = self.meta(name)?;
        Ok(Path::new("/sys/fs/cgroup").join(m.cgroups_path.trim_start_matches('/')))
    }
}

/// First non-loopback IPv4 address inside a network namespace.
///
/// `setns` only affects the calling thread, so a throwaway thread enters the
/// namespace and reads the addresses.
fn netns_ipv4(path: &str) -> Option<String> {
    let path = path.to_string();
    std::thread::spawn(move || -> Option<String> {
        let f = fs::File::open(&path).ok()?;
        nix::sched::setns(f, nix::sched::CloneFlags::CLONE_NEWNET).ok()?;
        nix::ifaddrs::getifaddrs().ok()?.find_map(|ifa| {
            let addr = ifa.address?;
            let v4 = addr.as_sockaddr_in()?;
            let ip = v4.ip();
            (!ip.is_loopback()).then(|| ip.to_string())
        })
    })
    .join()
    .ok()
    .flatten()
}

/// Last `tail` lines of `text` (`"all"` or unparsable: everything).
fn tail_lines(text: &str, tail: &str) -> String {
    match tail.parse::<usize>() {
        Ok(n) => {
            let lines: Vec<&str> = text.split_inclusive('\n').collect();
            lines[lines.len().saturating_sub(n)..].concat()
        }
        Err(_) => text.to_string(),
    }
}

fn read_u64(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn read_stat_key(path: &Path, key: &str) -> Option<u64> {
    fs::read_to_string(path).ok()?.lines().find_map(|l| {
        let (k, v) = l.split_once(' ')?;
        (k == key).then(|| v.trim().parse().ok())?
    })
}

/// Total host CPU time in nanoseconds, from `/proc/stat`.
fn system_cpu_ns() -> Option<u64> {
    let text = fs::read_to_string("/proc/stat").ok()?;
    let line = text.lines().next()?;
    let ticks: u64 = line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse::<u64>().ok())
        .sum();
    // USER_HZ is 100 on Linux.
    Some(ticks * 10_000_000)
}

fn sample_stats(cgroup: &Path) -> ContainerStats {
    let io = fs::read_to_string(cgroup.join("io.stat")).ok().map(|t| {
        t.lines()
            .map(|l| {
                l.split_whitespace()
                    .filter_map(|kv| kv.split_once('='))
                    .filter(|(k, _)| *k == "rbytes" || *k == "wbytes")
                    .filter_map(|(_, v)| v.parse::<u64>().ok())
                    .sum()
            })
            .collect()
    });
    ContainerStats {
        memory_usage: read_u64(&cgroup.join("memory.current")),
        memory_cache: read_stat_key(&cgroup.join("memory.stat"), "inactive_file"),
        cpu_total_usage: read_stat_key(&cgroup.join("cpu.stat"), "usage_usec").unwrap_or(0) * 1000,
        system_cpu_usage: system_cpu_ns(),
        precpu_total_usage: 0,
        precpu_system_cpu_usage: None,
        online_cpus: std::thread::available_parallelism()
            .ok()
            .map(|n| n.get() as u64),
        blkio_service_bytes: io,
    }
}

/// runc's cgroup v1 shares to v2 weight conversion.
fn shares_to_weight(shares: i64) -> u64 {
    let shares = shares.clamp(2, 262144) as u64;
    1 + ((shares - 2) * 9999) / 262142
}

#[async_trait]
impl ContainerBackend for YoukiBackend {
    fn joins_kubelet_netns(&self) -> bool {
        true
    }

    async fn version_identity(&self) -> Result<String> {
        Ok("youki libcontainer".to_string())
    }

    async fn inspect_image(&self, image: &str) -> Result<ImageInspect> {
        let image = image.to_string();
        self.blocking(move |i| {
            let s = i.images.get(&image)?;
            Ok(ImageInspect {
                id: Some(s.id),
                config: Some(s.config.into()),
            })
        })
        .await
    }

    fn pull_image<'a>(&'a self, image: &'a str) -> BackendStream<'a, PullProgress> {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        let inner = self.inner.clone();
        let image = image.to_string();
        let pull = async move {
            let progress_tx = tx.clone();
            let mut progress = move |status: String| {
                let _ = progress_tx.unbounded_send(Ok(PullProgress {
                    status: Some(status),
                    ..Default::default()
                }));
            };
            if let Err(e) = inner.images.pull(&inner.http, &image, &mut progress).await {
                let _ = tx.unbounded_send(Ok(PullProgress {
                    error: Some(format!("{:#}", e)),
                    ..Default::default()
                }));
            }
        };
        // Drive the pull while the caller reads progress.
        let pull = futures::stream::once(pull).filter_map(|_| async { None });
        futures::stream::select(rx, pull).boxed()
    }

    async fn create_container(&self, name: &str, config: ContainerConfig) -> Result<String> {
        let name_owned = name.to_string();
        let (meta, dir) = self
            .blocking(move |i| i.create(&name_owned, &config))
            .await?;
        let cfg = &self.inner.cfg;
        let shim_log = fs::File::create(dir.join("shim.log"))?;
        let spawned = tokio::process::Command::new(&cfg.shim)
            .arg("create")
            .arg(&cfg.state_root)
            .arg(name)
            .arg(&dir)
            .arg(dir.join("output.log"))
            .arg(dir.join("exit.json"))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(shim_log)
            .spawn();
        let mut child = match spawned {
            Ok(child) => child,
            Err(e) => {
                let inner = self.inner.clone();
                tokio::task::spawn_blocking(move || inner.cleanup_dir(&dir)).await?;
                return Err(e).with_context(|| format!("starting {}", cfg.shim.display()));
            }
        };
        let mut line = String::new();
        if let Some(out) = child.stdout.take() {
            let _ = tokio::io::BufReader::new(out).read_line(&mut line).await;
        }
        if !line.starts_with("ok ") {
            let _ = child.wait().await;
            let log = fs::read_to_string(dir.join("shim.log")).unwrap_or_default();
            let n = name.to_string();
            let _ = self.blocking(move |i| i.remove(&n, true)).await;
            bail!(
                "youki create {} failed: {} {}",
                name,
                line.trim().trim_start_matches("err "),
                log.trim()
            );
        }
        // The shim stays alive as the container's parent; reap it when it exits.
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
        Ok(meta.id)
    }

    async fn start_container(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        self.blocking(move |i| {
            let m = i.meta(&name)?;
            let mut c = i.load(&m.name).ok_or_else(|| not_found(&name))?;
            c.start()?;
            let mut m = m;
            m.started_at = Some(now_rfc3339());
            image::write_json(&i.dir(&m.name).join("meta.json"), &m)
        })
        .await
    }

    async fn stop_container(&self, name: &str, options: StopContainerOptions) -> Result<()> {
        let name = name.to_string();
        self.blocking(move |i| {
            let m = i.meta(&name)?;
            if !i.is_running(&m) {
                return Ok(());
            }
            i.signal(&m.name, nix::sys::signal::Signal::SIGTERM, false)?;
            let deadline = std::time::Instant::now() + Duration::from_secs(options.t.max(0) as u64);
            while i.is_running(&m) && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(100));
            }
            if i.is_running(&m) {
                i.signal(&m.name, nix::sys::signal::Signal::SIGKILL, true)?;
                for _ in 0..100 {
                    if !i.is_running(&m) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
            Ok(())
        })
        .await
    }

    async fn remove_container(&self, name: &str, options: RemoveContainerOptions) -> Result<()> {
        let name = name.to_string();
        self.blocking(move |i| i.remove(&name, options.force)).await
    }

    async fn list_containers(
        &self,
        options: ListContainersOptions,
    ) -> Result<Vec<ContainerSummary>> {
        self.blocking(move |i| {
            let names = options.filters.get("name");
            let statuses = options.filters.get("status");
            Ok(i.metas()
                .into_iter()
                .filter(|m| names.is_none_or(|ps| ps.iter().any(|p| name_matches(&m.name, p))))
                .filter_map(|m| {
                    let word = Inner::state_word(i.state(&m).status);
                    if !options.all && word != "running" {
                        return None;
                    }
                    if statuses.is_some_and(|ss| !ss.iter().any(|s| s == word)) {
                        return None;
                    }
                    Some(ContainerSummary {
                        id: Some(m.id.clone()),
                        names: Some(vec![format!("/{}", m.name)]),
                        image: Some(m.image.clone()),
                        created: Some(m.created),
                        state: Some(word.to_string()),
                        host_config: Some(InspectHostConfig {
                            network_mode: m.network_mode.clone(),
                        }),
                    })
                })
                .collect())
        })
        .await
    }

    async fn inspect_container(&self, name: &str) -> Result<ContainerInspect> {
        let name = name.to_string();
        self.blocking(move |i| i.inspect(&name)).await
    }

    async fn update_container(&self, name: &str, options: UpdateContainerOptions) -> Result<()> {
        let name = name.to_string();
        self.blocking(move |i| {
            let dir = i.cgroup_dir(&name)?;
            if options.cpu_quota.is_some() || options.cpu_period.is_some() {
                let quota = options
                    .cpu_quota
                    .filter(|q| *q > 0)
                    .map(|q| q.to_string())
                    .unwrap_or_else(|| "max".to_string());
                let period = options.cpu_period.filter(|p| *p > 0).unwrap_or(100_000);
                fs::write(dir.join("cpu.max"), format!("{} {}", quota, period))?;
            }
            if let Some(shares) = options.cpu_shares.filter(|s| *s > 0) {
                fs::write(dir.join("cpu.weight"), shares_to_weight(shares).to_string())?;
            }
            if let Some(mem) = options.memory.filter(|m| *m > 0) {
                fs::write(dir.join("memory.max"), mem.to_string())?;
                if let Some(swap) = options.memory_swap.filter(|s| *s >= mem) {
                    let _ = fs::write(dir.join("memory.swap.max"), (swap - mem).to_string());
                }
            }
            Ok(())
        })
        .await
    }

    fn stats<'a>(
        &'a self,
        name: &'a str,
        options: StatsOptions,
    ) -> BackendStream<'a, ContainerStats> {
        let dir = self.inner.cgroup_dir(name);
        let one = !options.stream || options.one_shot;
        futures::stream::unfold(
            (dir, None::<ContainerStats>, false),
            move |(dir, prev, done)| async move {
                if done {
                    return None;
                }
                let dir = match dir {
                    Ok(d) => d,
                    Err(e) => return Some((Err(e), (Err(anyhow!("stopped")), None, true))),
                };
                if prev.is_some() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                let mut s = sample_stats(&dir);
                if let Some(p) = &prev {
                    s.precpu_total_usage = p.cpu_total_usage;
                    s.precpu_system_cpu_usage = p.system_cpu_usage;
                }
                Some((Ok(s.clone()), (Ok(dir), Some(s), one)))
            },
        )
        .boxed()
    }

    async fn create_exec(&self, name: &str, options: CreateExecOptions) -> Result<String> {
        let name = name.to_string();
        self.blocking(move |i| {
            let m = i.meta(&name)?;
            if !i.is_running(&m) {
                bail!("container {} is not running", m.id);
            }
            if options.tty == Some(true) || options.attach_stdin == Some(true) {
                tracing::debug!("youki exec: tty and stdin are not attached");
            }
            let exec_id = uuid::Uuid::new_v4().simple().to_string();
            let args = shim::ExecArgs {
                cmd: options.cmd.unwrap_or_default(),
            };
            image::write_json(
                &i.dir(&m.name)
                    .join("exec")
                    .join(format!("{}.json", exec_id)),
                &args,
            )?;
            Ok(format!("{}.{}", m.name, exec_id))
        })
        .await
    }

    async fn start_exec(&self, exec_id: &str) -> Result<StartExecResults> {
        let (name, id) = exec_id
            .rsplit_once('.')
            .ok_or_else(|| anyhow!("No such exec instance: {}", exec_id))?;
        let base = self.inner.dir(name).join("exec").join(id);
        let file = |ext: &str| base.with_extension(ext);
        if !file("json").exists() {
            bail!("No such exec instance: {}", exec_id);
        }
        let cfg = &self.inner.cfg;
        let mut child = tokio::process::Command::new(&cfg.shim)
            .arg("exec")
            .arg(&cfg.state_root)
            .arg(name)
            .arg(file("json"))
            .arg(file("out"))
            .arg(file("err"))
            .arg(file("exit"))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .with_context(|| format!("starting {}", cfg.shim.display()))?;
        let (out, err) = (file("out"), file("err"));
        let output = futures::stream::once(async move {
            let _ = child.wait().await;
            let mut chunks = Vec::new();
            if let Ok(m) = tokio::fs::read(&out).await {
                if !m.is_empty() {
                    chunks.push(Ok(LogOutput::StdOut { message: m }));
                }
            }
            if let Ok(m) = tokio::fs::read(&err).await {
                if !m.is_empty() {
                    chunks.push(Ok(LogOutput::StdErr { message: m }));
                }
            }
            futures::stream::iter(chunks)
        })
        .flatten()
        .boxed();
        Ok(StartExecResults::Attached { output })
    }

    async fn inspect_exec(&self, exec_id: &str) -> Result<ExecInspect> {
        let (name, id) = exec_id
            .rsplit_once('.')
            .ok_or_else(|| anyhow!("No such exec instance: {}", exec_id))?;
        let base = self.inner.dir(name).join("exec").join(id);
        if !base.with_extension("json").exists() {
            bail!("No such exec instance: {}", exec_id);
        }
        let exit: Option<ExitRecord> = read_json(&base.with_extension("exit"));
        Ok(ExecInspect {
            running: Some(exit.is_none()),
            exit_code: exit.map(|e| e.exit_code),
        })
    }

    fn logs<'a>(&'a self, name: &'a str, options: LogsOptions) -> BackendStream<'a, LogOutput> {
        let inner = self.inner.clone();
        let name = name.to_string();
        futures::stream::once(async move {
            let m = inner.meta(&name)?;
            if !options.stdout && !options.stderr {
                return Ok(None);
            }
            let text = tokio::fs::read(inner.dir(&m.name).join("output.log"))
                .await
                .unwrap_or_default();
            let text = tail_lines(&String::from_utf8_lossy(&text), &options.tail);
            Ok((!text.is_empty()).then(|| LogOutput::StdOut {
                message: text.into_bytes(),
            }))
        })
        .filter_map(|r: Result<Option<LogOutput>>| async move { r.transpose() })
        .boxed()
    }

    fn download_from_container<'a>(
        &'a self,
        name: &'a str,
        path: &'a str,
    ) -> BackendStream<'a, Vec<u8>> {
        let inner = self.inner.clone();
        let (name, path) = (name.to_string(), path.to_string());
        futures::stream::once(async move {
            tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                let m = inner.meta(&name)?;
                let rootfs = inner.dir(&m.name).join("rootfs");
                let target = rootfs.join(path.trim_start_matches('/'));
                let real = fs::canonicalize(&target).with_context(|| {
                    format!("Could not find the file {} in container {}", path, name)
                })?;
                if !real.starts_with(fs::canonicalize(&rootfs)?) {
                    bail!("path {} escapes the container rootfs", path);
                }
                let file_name = Path::new(&path)
                    .file_name()
                    .map(|n| n.to_os_string())
                    .unwrap_or_else(|| "root".into());
                let mut b = tar::Builder::new(Vec::new());
                if real.is_dir() {
                    b.append_dir_all(&file_name, &real)?;
                } else {
                    b.append_path_with_name(&real, &file_name)?;
                }
                Ok(b.into_inner()?)
            })
            .await?
        })
        .boxed()
    }

    async fn list_volumes(&self) -> Result<Vec<String>> {
        // There are no named volumes; pod volumes are host directories.
        Ok(Vec::new())
    }

    async fn remove_volume(&self, name: &str) -> Result<()> {
        bail!("unsupported on youki: named volumes ({})", name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_resolution_uses_rootfs_passwd() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("etc")).unwrap();
        fs::write(
            root.path().join("etc/passwd"),
            "root:x:0:0::/root:/bin/sh\nnginx:x:101:102::/:/sbin/nologin\n",
        )
        .unwrap();
        fs::write(root.path().join("etc/group"), "root:x:0:\nweb:x:500:\n").unwrap();
        let r = |u: Option<&str>| resolve_user(u, root.path());
        assert_eq!(r(None).unwrap(), (0, 0));
        assert_eq!(r(Some("1000")).unwrap(), (1000, 0));
        assert_eq!(r(Some("101")).unwrap(), (101, 102));
        assert_eq!(r(Some("nginx")).unwrap(), (101, 102));
        assert_eq!(r(Some("1000:2000")).unwrap(), (1000, 2000));
        assert_eq!(r(Some("nginx:web")).unwrap(), (101, 500));
        assert!(r(Some("nobody")).is_err());
    }

    #[test]
    fn tails_lines() {
        assert_eq!(tail_lines("a\nb\nc\n", "2"), "b\nc\n");
        assert_eq!(tail_lines("a\nb", "all"), "a\nb");
        assert_eq!(tail_lines("a\n", "10"), "a\n");
    }

    #[test]
    fn name_filter_matches_docker_usage() {
        assert!(name_matches("web_app", "^/web"));
        assert!(name_matches("web_app", "web_"));
        assert!(!name_matches("myweb_app", "^/web"));
    }

    #[test]
    fn shares_convert_like_runc() {
        assert_eq!(shares_to_weight(2), 1);
        assert_eq!(shares_to_weight(1024), 39);
        assert_eq!(shares_to_weight(262144), 10000);
    }
}
