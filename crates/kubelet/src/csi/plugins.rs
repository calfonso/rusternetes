//! Plugin registration: the kubelet side of `pluginregistration.Registration`.
//!
//! A plugin (in practice the `node-driver-registrar` sidecar) places a socket
//! in the registry directory. The watcher calls `GetInfo` on it, checks that
//! it is a CSI plugin with a v1 API, asks the driver's own endpoint for
//! `NodeGetInfo` / `NodeGetCapabilities`, records the driver, updates the
//! node's `CSINode`, and reports the outcome with `NotifyRegistrationStatus`.
//! A socket that goes away deregisters its driver.

use super::client::connect_unix;
use super::proto::csi::v1 as csipb;
use super::proto::pluginregistration as regpb;
use anyhow::{anyhow, bail, Context, Result};
use rusternetes_common::resources::csi::{
    CSINode, CSINodeDriver, CSINodeSpec, VolumeNodeResources,
};
use rusternetes_common::resources::Node;
use rusternetes_common::types::{ObjectMeta, OwnerReference, TypeMeta};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

/// Plugin type string for CSI drivers in `PluginInfo.type`.
pub const CSI_PLUGIN_TYPE: &str = "CSIPlugin";

const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(15);

/// A CSI driver registered on this node.
#[derive(Debug)]
pub struct CsiDriver {
    pub name: String,
    pub endpoint: PathBuf,
    pub node_id: String,
    pub max_volumes_per_node: i64,
    pub accessible_topology: HashMap<String, String>,
    /// The driver has the `STAGE_UNSTAGE_VOLUME` node capability.
    pub stage_unstage: bool,
    channel: Channel,
}

impl CsiDriver {
    pub fn node_client(&self) -> csipb::node_client::NodeClient<Channel> {
        csipb::node_client::NodeClient::new(self.channel.clone())
    }
}

/// Drivers registered on this node, by name.
#[derive(Debug, Default)]
pub struct CsiDriverRegistry {
    drivers: RwLock<HashMap<String, Arc<CsiDriver>>>,
}

impl CsiDriverRegistry {
    pub fn get(&self, name: &str) -> Option<Arc<CsiDriver>> {
        self.drivers.read().unwrap().get(name).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.drivers.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    fn insert(&self, driver: CsiDriver) {
        self.drivers
            .write()
            .unwrap()
            .insert(driver.name.clone(), Arc::new(driver));
    }

    fn remove(&self, name: &str) {
        self.drivers.write().unwrap().remove(name);
    }
}

/// What the watcher knows about one socket: its inode (a recreated socket
/// is registered again) and the driver it registered, if any.
struct Known {
    ino: u64,
    driver: Option<String>,
}

/// Scans the plugin registry directory and keeps the registry in sync.
pub struct PluginWatcher {
    dir: PathBuf,
    registry: Arc<CsiDriverRegistry>,
    csinode: Option<(Arc<StorageBackend>, String)>,
    known: HashMap<PathBuf, Known>,
}

impl PluginWatcher {
    pub fn new(dir: PathBuf, registry: Arc<CsiDriverRegistry>) -> Self {
        Self {
            dir,
            registry,
            csinode: None,
            known: HashMap::new(),
        }
    }

    /// Also keep the `CSINode` object of `node_name` up to date.
    pub fn with_csinode(mut self, storage: Arc<StorageBackend>, node_name: String) -> Self {
        self.csinode = Some((storage, node_name));
        self
    }

    /// Scan forever. Upstream uses fsnotify; a short poll is enough here and
    /// also picks up sockets created before the kubelet started.
    pub async fn run(mut self, interval: Duration) {
        if let Err(e) = std::fs::create_dir_all(&self.dir) {
            warn!(
                "CSI plugin registration disabled: cannot create {}: {}",
                self.dir.display(),
                e
            );
            return;
        }
        info!("Watching {} for CSI plugins", self.dir.display());
        loop {
            self.scan().await;
            tokio::time::sleep(interval).await;
        }
    }

    /// One pass: register new or recreated sockets, deregister removed ones.
    pub async fn scan(&mut self) {
        let mut present = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                // Upstream ignores dot-files.
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue;
                }
                if let Ok(meta) = entry.metadata() {
                    use std::os::unix::fs::{FileTypeExt, MetadataExt};
                    if meta.file_type().is_socket() {
                        present.insert(entry.path(), meta.ino());
                    }
                }
            }
        }

        let gone: Vec<PathBuf> = self
            .known
            .iter()
            .filter(|(path, known)| present.get(*path) != Some(&known.ino))
            .map(|(path, _)| path.clone())
            .collect();
        for path in gone {
            if let Some(Known {
                driver: Some(name), ..
            }) = self.known.remove(&path)
            {
                self.deregister(&name).await;
            }
        }

        for (path, ino) in present {
            if self.known.contains_key(&path) {
                continue;
            }
            let driver = match self.register(&path).await {
                Ok(name) => Some(name),
                Err(e) => {
                    warn!("CSI plugin at {} not registered: {:#}", path.display(), e);
                    None
                }
            };
            self.known.insert(path, Known { ino, driver });
        }
    }

    async fn register(&self, socket: &Path) -> Result<String> {
        let channel = connect_unix(socket).await?;
        let mut reg = regpb::registration_client::RegistrationClient::new(channel);
        let info = tokio::time::timeout(REGISTRATION_TIMEOUT, reg.get_info(regpb::InfoRequest {}))
            .await
            .context("GetInfo timed out")?
            .context("GetInfo")?
            .into_inner();

        let result = self.validate_and_add(socket, &info).await;
        let status = regpb::RegistrationStatus {
            plugin_registered: result.is_ok(),
            error: result
                .as_ref()
                .err()
                .map(|e| format!("{:#}", e))
                .unwrap_or_default(),
        };
        if let Err(e) =
            tokio::time::timeout(REGISTRATION_TIMEOUT, reg.notify_registration_status(status))
                .await
                .map_err(|_| anyhow!("timed out"))
                .and_then(|r| r.map_err(anyhow::Error::from))
        {
            warn!("NotifyRegistrationStatus to {} failed: {:#}", info.name, e);
        }
        result.map(|_| info.name)
    }

    async fn validate_and_add(&self, socket: &Path, info: &regpb::PluginInfo) -> Result<()> {
        if info.r#type != CSI_PLUGIN_TYPE {
            bail!("unsupported plugin type {:?}", info.r#type);
        }
        if info.name.is_empty() {
            bail!("plugin reported an empty name");
        }
        if !info
            .supported_versions
            .iter()
            .any(|v| v.trim_start_matches('v').starts_with("1."))
        {
            bail!(
                "no supported CSI version in {:?} (need 1.x)",
                info.supported_versions
            );
        }
        let endpoint = if info.endpoint.is_empty() {
            socket.to_path_buf()
        } else {
            PathBuf::from(&info.endpoint)
        };

        let channel = connect_unix(&endpoint).await?;
        let mut node = csipb::node_client::NodeClient::new(channel.clone());
        let node_info = tokio::time::timeout(
            REGISTRATION_TIMEOUT,
            node.node_get_info(csipb::NodeGetInfoRequest {}),
        )
        .await
        .context("NodeGetInfo timed out")?
        .context("NodeGetInfo")?
        .into_inner();
        if node_info.node_id.is_empty() {
            bail!("NodeGetInfo returned an empty node_id");
        }
        let caps = tokio::time::timeout(
            REGISTRATION_TIMEOUT,
            node.node_get_capabilities(csipb::NodeGetCapabilitiesRequest {}),
        )
        .await
        .context("NodeGetCapabilities timed out")?
        .context("NodeGetCapabilities")?
        .into_inner();
        let stage_unstage = caps.capabilities.iter().any(|c| {
            matches!(
                &c.r#type,
                Some(csipb::node_service_capability::Type::Rpc(rpc))
                    if rpc.r#type
                        == csipb::node_service_capability::rpc::Type::StageUnstageVolume as i32
            )
        });

        let driver = CsiDriver {
            name: info.name.clone(),
            endpoint,
            node_id: node_info.node_id,
            max_volumes_per_node: node_info.max_volumes_per_node,
            accessible_topology: node_info
                .accessible_topology
                .map(|t| t.segments)
                .unwrap_or_default(),
            stage_unstage,
            channel,
        };
        if let Some((storage, node_name)) = &self.csinode {
            set_csinode_driver(storage, node_name, &driver)
                .await
                .context("updating CSINode")?;
        }
        info!(
            "Registered CSI driver {} (node ID {}, endpoint {}, stage/unstage: {})",
            driver.name,
            driver.node_id,
            driver.endpoint.display(),
            driver.stage_unstage
        );
        self.registry.insert(driver);
        Ok(())
    }

    async fn deregister(&self, name: &str) {
        info!("CSI driver {} went away, deregistering", name);
        self.registry.remove(name);
        if let Some((storage, node_name)) = &self.csinode {
            if let Err(e) = set_csinode_driver_named(storage, node_name, name, None).await {
                warn!(
                    "Removing {} from CSINode {} failed: {:#}",
                    name, node_name, e
                );
            }
        }
    }
}

async fn set_csinode_driver(
    storage: &StorageBackend,
    node_name: &str,
    d: &CsiDriver,
) -> Result<()> {
    let entry = {
        let mut keys: Vec<String> = d.accessible_topology.keys().cloned().collect();
        keys.sort();
        CSINodeDriver {
            name: d.name.clone(),
            node_id: d.node_id.clone(),
            topology_keys: (!keys.is_empty()).then_some(keys),
            allocatable: (d.max_volumes_per_node > 0).then(|| VolumeNodeResources {
                count: Some(d.max_volumes_per_node.min(i32::MAX as i64) as i32),
            }),
        }
    };
    set_csinode_driver_named(storage, node_name, &d.name, Some(entry)).await
}

/// Add/replace (`Some`) or remove (`None`) the driver entry `name` in the
/// node's `CSINode`, creating the object when needed.
async fn set_csinode_driver_named(
    storage: &StorageBackend,
    node_name: &str,
    name: &str,
    entry: Option<CSINodeDriver>,
) -> Result<()> {
    let key = build_key("csinodes", None, node_name);
    let mut last_err = None;
    for _ in 0..3 {
        let existing = storage.get::<CSINode>(&key).await.ok();
        let creating = existing.is_none();
        let mut csinode = match existing {
            Some(c) => c,
            None if entry.is_none() => return Ok(()),
            None => new_csinode(storage, node_name).await,
        };
        csinode.spec.drivers.retain(|d| d.name != name);
        if let Some(entry) = &entry {
            csinode.spec.drivers.push(entry.clone());
        }
        let res = if creating {
            storage.create(&key, &csinode).await.map(|_| ())
        } else {
            storage.update(&key, &csinode).await.map(|_| ())
        };
        match res {
            Ok(()) => return Ok(()),
            Err(e) => {
                debug!("CSINode {} write failed, retrying: {}", node_name, e);
                last_err = Some(e);
            }
        }
    }
    Err(anyhow!(
        "{}",
        last_err.map(|e| e.to_string()).unwrap_or_default()
    ))
}

async fn new_csinode(storage: &StorageBackend, node_name: &str) -> CSINode {
    let mut metadata = ObjectMeta::new(node_name);
    // Upstream owns the CSINode by its Node, so it goes away with the node.
    if let Ok(node) = storage
        .get::<Node>(&build_key("nodes", None, node_name))
        .await
    {
        metadata.owner_references = Some(vec![OwnerReference::new(
            "v1",
            "Node",
            node_name,
            node.metadata.uid.clone(),
        )]);
    }
    CSINode {
        type_meta: TypeMeta {
            kind: "CSINode".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata,
        spec: CSINodeSpec {
            drivers: Vec::new(),
        },
    }
}
