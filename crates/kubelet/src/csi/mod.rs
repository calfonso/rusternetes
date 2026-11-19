//! Container Storage Interface (CSI) support for the kubelet, node side.
//!
//! - [`plugins`] watches `<root>/plugins_registry` for plugin sockets and
//!   speaks the kubelet plugin registration API (`GetInfo` /
//!   `NotifyRegistrationStatus`), so the standard `node-driver-registrar`
//!   sidecar works unchanged. Registered drivers are kept in a
//!   [`CsiDriverRegistry`] and written to this node's `CSINode` object.
//! - [`CsiManager`] runs the node RPCs for a volume: `NodeStageVolume` into the
//!   global staging path (when the driver has `STAGE_UNSTAGE_VOLUME`), then
//!   `NodePublishVolume` into the pod volume directory, and the reverse on
//!   teardown.
//!
//! Paths follow the upstream kubelet layout under the kubelet root directory
//! (`KUBELET_ROOT_DIR`, default `/var/lib/kubelet`):
//! `plugins/kubernetes.io/csi/<driver>/<sha256(volumeHandle)>/globalmount` for
//! staging. The pod target is `<volume dir>/mount` (or `<volume dir>/block`
//! for raw block), with `<volume dir>/vol_data.json` recording what was
//! published so teardown works without the pod spec and across restarts.
//!
//! Attach/detach (`ControllerPublishVolume`) is not done here: if a
//! `VolumeAttachment` for the PV and node is attached, its metadata is passed
//! on as the publish context.

pub mod client;
#[cfg(test)]
pub(crate) mod fake;
pub mod plugins;
#[cfg(test)]
mod tests;

pub use plugins::{CsiDriver, CsiDriverRegistry, PluginWatcher};

use anyhow::{anyhow, bail, Context, Result};
use proto::csi::v1 as csipb;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Generated gRPC code for the vendored protos.
pub mod proto {
    pub mod csi {
        #[allow(clippy::all, rustdoc::all)]
        pub mod v1 {
            tonic::include_proto!("csi.v1");
        }
    }
    #[allow(clippy::all, rustdoc::all)]
    pub mod pluginregistration {
        tonic::include_proto!("pluginregistration");
    }
}

/// Time limit for one stage/publish/unpublish/unstage call (upstream: 2 min).
const CSI_OP_TIMEOUT: Duration = Duration::from_secs(120);

/// Name of the per-volume state file, as in the upstream kubelet.
pub const VOL_DATA_FILE: &str = "vol_data.json";

/// Volume context key the upstream kubelet sets for pod info and ephemeral volumes.
pub const CTX_EPHEMERAL: &str = "csi.storage.k8s.io/ephemeral";

/// How the workload accesses the volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessType {
    Mount {
        fs_type: String,
        mount_flags: Vec<String>,
    },
    Block,
}

/// Everything needed to make one CSI volume available to one pod.
#[derive(Debug, Clone)]
pub struct MountRequest {
    pub driver: String,
    pub volume_handle: String,
    /// The pod's directory for this volume, `<volumes>/<pod>/<volume>`.
    pub volume_dir: PathBuf,
    pub read_only: bool,
    pub access_type: AccessType,
    pub access_mode: csipb::volume_capability::access_mode::Mode,
    pub volume_context: HashMap<String, String>,
    pub publish_context: HashMap<String, String>,
    pub stage_secrets: HashMap<String, String>,
    pub publish_secrets: HashMap<String, String>,
    /// Inline ephemeral volumes are never staged.
    pub ephemeral: bool,
}

/// What was published for one pod volume (`vol_data.json`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VolData {
    pub driver_name: String,
    pub volume_handle: String,
    pub target_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging_path: Option<String>,
    #[serde(default)]
    pub block: bool,
}

/// Runs CSI node operations against registered drivers.
pub struct CsiManager {
    root_dir: PathBuf,
    registry: Arc<CsiDriverRegistry>,
}

impl CsiManager {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            registry: Arc::new(CsiDriverRegistry::default()),
        }
    }

    /// Root from `KUBELET_ROOT_DIR`, else `/var/lib/kubelet` (the path
    /// `node-driver-registrar` expects by default).
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("KUBELET_ROOT_DIR").unwrap_or_else(|_| "/var/lib/kubelet".to_string()),
        )
    }

    pub fn registry(&self) -> &Arc<CsiDriverRegistry> {
        &self.registry
    }

    /// Directory the plugin watcher scans for registration sockets.
    pub fn plugins_registry_dir(&self) -> PathBuf {
        self.root_dir.join("plugins_registry")
    }

    /// Global staging path for a volume, shared by all pods on the node.
    pub fn staging_path(&self, driver: &str, volume_handle: &str) -> PathBuf {
        self.root_dir
            .join("plugins/kubernetes.io/csi")
            .join(driver)
            .join(sha256_hex(volume_handle))
            .join("globalmount")
    }

    /// A [`PluginWatcher`] for this manager's registry directory.
    pub fn plugin_watcher(&self) -> PluginWatcher {
        PluginWatcher::new(self.plugins_registry_dir(), self.registry.clone())
    }

    /// Stage (if the driver supports it) and publish a volume; returns the
    /// path to bind into the containers. Safe to call again for the same
    /// volume: every step is idempotent, as the CSI spec requires.
    pub async fn mount_volume(&self, req: &MountRequest) -> Result<PathBuf> {
        let driver = self
            .registry
            .get(&req.driver)
            .ok_or_else(|| anyhow!("CSI driver {} is not registered on this node", req.driver))?;
        let block = req.access_type == AccessType::Block;
        let target = req.volume_dir.join(if block { "block" } else { "mount" });
        let staging = (driver.stage_unstage && !req.ephemeral)
            .then(|| self.staging_path(&req.driver, &req.volume_handle));

        // Record the volume before touching the driver, so a crash or a failed
        // publish still leaves enough behind for teardown.
        std::fs::create_dir_all(&req.volume_dir)
            .with_context(|| format!("creating {}", req.volume_dir.display()))?;
        write_vol_data(
            &req.volume_dir,
            &VolData {
                driver_name: req.driver.clone(),
                volume_handle: req.volume_handle.clone(),
                target_path: target.to_string_lossy().into_owned(),
                staging_path: staging.as_ref().map(|p| p.to_string_lossy().into_owned()),
                block,
            },
        )?;

        let capability = volume_capability(&req.access_type, req.access_mode);
        let mut node = driver.node_client();

        if let Some(staging) = &staging {
            std::fs::create_dir_all(staging)
                .with_context(|| format!("creating staging path {}", staging.display()))?;
            let request = csipb::NodeStageVolumeRequest {
                volume_id: req.volume_handle.clone(),
                publish_context: req.publish_context.clone(),
                staging_target_path: staging.to_string_lossy().into_owned(),
                volume_capability: Some(capability.clone()),
                secrets: req.stage_secrets.clone(),
                volume_context: req.volume_context.clone(),
            };
            call(
                &req.driver,
                "NodeStageVolume",
                node.node_stage_volume(request),
            )
            .await?;
            debug!(
                "CSI {} staged {} at {}",
                req.driver,
                req.volume_handle,
                staging.display()
            );
        }

        let request = csipb::NodePublishVolumeRequest {
            volume_id: req.volume_handle.clone(),
            publish_context: req.publish_context.clone(),
            staging_target_path: staging
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            target_path: target.to_string_lossy().into_owned(),
            volume_capability: Some(capability),
            readonly: req.read_only,
            secrets: req.publish_secrets.clone(),
            volume_context: req.volume_context.clone(),
        };
        call(
            &req.driver,
            "NodePublishVolume",
            node.node_publish_volume(request),
        )
        .await?;
        info!(
            "CSI {} published {} at {}",
            req.driver,
            req.volume_handle,
            target.display()
        );
        Ok(target)
    }

    /// Unpublish the volume recorded in `volume_dir`, and unstage it when no
    /// other pod volume on the node still uses the staging path. A directory
    /// without `vol_data.json` is not a CSI volume and is left alone.
    pub async fn unmount_volume(&self, volume_dir: &Path) -> Result<()> {
        let Some(data) = read_vol_data(volume_dir)? else {
            return Ok(());
        };
        let driver = self.registry.get(&data.driver_name).ok_or_else(|| {
            anyhow!(
                "CSI driver {} is not registered; cannot unpublish {}",
                data.driver_name,
                data.volume_handle
            )
        })?;
        let mut node = driver.node_client();
        let request = csipb::NodeUnpublishVolumeRequest {
            volume_id: data.volume_handle.clone(),
            target_path: data.target_path.clone(),
        };
        call(
            &data.driver_name,
            "NodeUnpublishVolume",
            node.node_unpublish_volume(request),
        )
        .await?;
        info!(
            "CSI {} unpublished {} from {}",
            data.driver_name, data.volume_handle, data.target_path
        );
        let _ = std::fs::remove_file(volume_dir.join(VOL_DATA_FILE));

        if let Some(staging) = &data.staging_path {
            let volumes_base = volume_dir.parent().and_then(Path::parent);
            let still_used = volumes_base
                .map(|base| staging_in_use(base, staging))
                .unwrap_or(false);
            if still_used {
                debug!("CSI staging path {} still in use", staging);
            } else {
                let request = csipb::NodeUnstageVolumeRequest {
                    volume_id: data.volume_handle.clone(),
                    staging_target_path: staging.clone(),
                };
                call(
                    &data.driver_name,
                    "NodeUnstageVolume",
                    node.node_unstage_volume(request),
                )
                .await?;
                info!(
                    "CSI {} unstaged {} from {}",
                    data.driver_name, data.volume_handle, staging
                );
                // Only empty directories: never recurse into a mount.
                let _ = std::fs::remove_dir(staging);
                if let Some(parent) = Path::new(staging).parent() {
                    let _ = std::fs::remove_dir(parent);
                }
            }
        }
        Ok(())
    }

    /// Tear down every CSI volume under a pod's volume directory. Returns an
    /// error if any of them could not be unpublished; the caller must then
    /// keep the directory, since it may still hold a live mount.
    pub async fn unmount_pod_volumes(&self, pod_dir: &Path) -> Result<()> {
        let Ok(entries) = std::fs::read_dir(pod_dir) else {
            return Ok(());
        };
        let mut failed = Vec::new();
        for entry in entries.flatten() {
            let dir = entry.path();
            if !dir.join(VOL_DATA_FILE).exists() {
                continue;
            }
            if let Err(e) = self.unmount_volume(&dir).await {
                warn!("CSI teardown of {} failed: {:#}", dir.display(), e);
                failed.push(dir.display().to_string());
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            bail!("CSI volumes not torn down: {}", failed.join(", "))
        }
    }
}

/// The volume handle the upstream kubelet uses for an inline ephemeral volume.
pub fn ephemeral_volume_handle(pod_uid: &str, volume_name: &str) -> String {
    format!("csi-{}", sha256_hex(&format!("{}{}", pod_uid, volume_name)))
}

fn sha256_hex(s: &str) -> String {
    Sha256::digest(s.as_bytes())
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

fn volume_capability(
    access_type: &AccessType,
    mode: csipb::volume_capability::access_mode::Mode,
) -> csipb::VolumeCapability {
    use csipb::volume_capability as vc;
    let access_type = match access_type {
        AccessType::Mount {
            fs_type,
            mount_flags,
        } => vc::AccessType::Mount(vc::MountVolume {
            fs_type: fs_type.clone(),
            mount_flags: mount_flags.clone(),
            volume_mount_group: String::new(),
        }),
        AccessType::Block => vc::AccessType::Block(vc::BlockVolume {}),
    };
    csipb::VolumeCapability {
        access_mode: Some(vc::AccessMode { mode: mode as i32 }),
        access_type: Some(access_type),
    }
}

async fn call<T>(
    driver: &str,
    op: &str,
    fut: impl std::future::Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>,
) -> Result<T> {
    match tokio::time::timeout(CSI_OP_TIMEOUT, fut).await {
        Err(_) => bail!("CSI {} {} timed out", driver, op),
        Ok(Err(status)) => bail!(
            "CSI {} {} failed: {:?}: {}",
            driver,
            op,
            status.code(),
            status.message()
        ),
        Ok(Ok(resp)) => Ok(resp.into_inner()),
    }
}

fn write_vol_data(volume_dir: &Path, data: &VolData) -> Result<()> {
    let path = volume_dir.join(VOL_DATA_FILE);
    let tmp = volume_dir.join(format!(".{}.tmp", VOL_DATA_FILE));
    std::fs::write(&tmp, serde_json::to_vec(data)?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("writing {}", path.display()))
}

/// Reads `vol_data.json` from a pod volume directory, if there is one.
pub fn read_vol_data(volume_dir: &Path) -> Result<Option<VolData>> {
    let path = volume_dir.join(VOL_DATA_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing {}", path.display()))?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Whether any pod volume under `volumes_base` (`<base>/<pod>/<volume>`)
/// still records `staging`.
fn staging_in_use(volumes_base: &Path, staging: &str) -> bool {
    let Ok(pods) = std::fs::read_dir(volumes_base) else {
        return false;
    };
    pods.flatten()
        .filter_map(|pod| std::fs::read_dir(pod.path()).ok())
        .flat_map(|vols| vols.flatten())
        .filter_map(|vol| read_vol_data(&vol.path()).ok().flatten())
        .any(|d| d.staging_path.as_deref() == Some(staging))
}

#[cfg(test)]
mod path_tests {
    use super::*;

    #[test]
    fn staging_path_matches_upstream_layout() {
        let m = CsiManager::new("/var/lib/kubelet");
        let p = m.staging_path("hostpath.csi.k8s.io", "vol-1");
        assert_eq!(
            p,
            PathBuf::from(format!(
                "/var/lib/kubelet/plugins/kubernetes.io/csi/hostpath.csi.k8s.io/{}/globalmount",
                sha256_hex("vol-1")
            ))
        );
        assert_eq!(sha256_hex("").len(), 64);
    }

    #[test]
    fn ephemeral_handle_is_stable() {
        let a = ephemeral_volume_handle("uid-1", "data");
        assert_eq!(a, ephemeral_volume_handle("uid-1", "data"));
        assert_ne!(a, ephemeral_volume_handle("uid-2", "data"));
        assert!(a.starts_with("csi-"));
    }

    #[test]
    fn staging_in_use_scans_other_pods() {
        let base = tempfile::tempdir().unwrap();
        let vol = base.path().join("pod-a/data");
        std::fs::create_dir_all(&vol).unwrap();
        write_vol_data(
            &vol,
            &VolData {
                driver_name: "d".into(),
                volume_handle: "h".into(),
                target_path: "/t".into(),
                staging_path: Some("/s".into()),
                block: false,
            },
        )
        .unwrap();
        assert!(staging_in_use(base.path(), "/s"));
        assert!(!staging_in_use(base.path(), "/other"));
    }
}
