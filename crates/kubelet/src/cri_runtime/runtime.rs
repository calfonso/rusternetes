//! CRI-backed container runtime — the lifecycle type that replaces the bollard
//! [`crate::runtime::ContainerRuntime`].
//!
//! It is stateless about pod→id mappings: like the upstream kubelet, it
//! discovers sandboxes and containers by querying the runtime with the
//! `io.kubernetes.*` labels [`translate`] stamps on them, so it reconciles
//! correctly across restarts without an in-memory index.
//!
//! This is incremental — the methods here (`start_pod`, `is_pod_running`,
//! `list_running_pods`, `stop_and_remove_pod`) cover pod bring-up, liveness and
//! teardown. The remaining `ContainerRuntime` surface (full status mapping,
//! probes, gc, metrics) lands on top of this. Tracked in the migration issue.

use std::path::Path;

use rusternetes_common::resources::pod::Pod;
use rusternetes_cri::{v1, CriClient, CriError};

use super::translate;

/// Errors from the CRI-backed runtime.
#[derive(Debug, thiserror::Error)]
pub enum CriRuntimeError {
    #[error(transparent)]
    Cri(#[from] CriError),

    #[error("preparing pod log directory {dir}: {source}")]
    LogDir {
        dir: String,
        #[source]
        source: std::io::Error,
    },
}

type Result<T> = std::result::Result<T, CriRuntimeError>;

/// Drives a CRI v1 runtime (containerd → Youki) for the kubelet.
#[derive(Clone)]
pub struct CriContainerRuntime {
    cri: CriClient,
    /// Runtime class passed to `RunPodSandbox` (e.g. `youki`); empty = default.
    runtime_handler: String,
    /// Root under which per-pod log directories are created.
    log_root: String,
}

impl CriContainerRuntime {
    /// Connect to the CRI runtime at `socket` and use `runtime_handler` for new
    /// sandboxes. `log_root` is the base dir for per-pod container logs.
    pub async fn connect(
        socket: impl AsRef<Path>,
        runtime_handler: impl Into<String>,
        log_root: impl Into<String>,
    ) -> Result<Self> {
        let cri = CriClient::connect(socket).await?;
        Ok(Self {
            cri,
            runtime_handler: runtime_handler.into(),
            log_root: log_root.into(),
        })
    }

    /// Per-pod log directory: `<log_root>/<namespace>_<name>_<uid>`.
    fn log_dir_for(&self, pod: &Pod) -> String {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        format!(
            "{}/{}_{}_{}",
            self.log_root, ns, pod.metadata.name, pod.metadata.uid
        )
    }

    /// Bring a pod up: pull each container image, run the sandbox, then create
    /// and start every container from its translated config.
    ///
    /// Init containers, probes and volume provisioning are handled by the
    /// kubelet around this call; here we run `pod.spec.containers` in order.
    pub async fn start_pod(&self, pod: &Pod) -> Result<()> {
        let Some(spec) = pod.spec.as_ref() else {
            return Ok(());
        };

        let log_dir = self.log_dir_for(pod);
        std::fs::create_dir_all(&log_dir).map_err(|source| CriRuntimeError::LogDir {
            dir: log_dir.clone(),
            source,
        })?;

        let sandbox_cfg = translate::sandbox_config(pod, &log_dir);
        let handler = self.runtime_handler.clone();

        let mut cri = self.cri.clone();
        let sandbox_id = cri.run_pod_sandbox(sandbox_cfg.clone(), &handler).await?;

        for container in &spec.containers {
            // Pull on behalf of this sandbox so registry config resolves.
            let image_ref = cri
                .pull_image(&container.image, Some(&handler), Some(sandbox_cfg.clone()))
                .await?;
            let cfg = translate::container_config(
                pod,
                container,
                &image_ref,
                &std::collections::HashMap::new(),
            );
            let container_id = cri
                .create_container(&sandbox_id, cfg, sandbox_cfg.clone())
                .await?;
            cri.start_container(&container_id).await?;
        }
        Ok(())
    }

    /// Find the sandbox id for a pod by its name label, if one exists.
    pub async fn sandbox_id_for(&self, pod_name: &str) -> Result<Option<String>> {
        let filter = v1::PodSandboxFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_NAME.to_string(),
                pod_name.to_string(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let sandboxes = cri.list_pod_sandbox(Some(filter)).await?;
        Ok(sandboxes.into_iter().next().map(|s| s.id))
    }

    /// True when at least one of the pod's containers is in the RUNNING state.
    pub async fn is_pod_running(&self, pod: &Pod) -> Result<bool> {
        let filter = v1::ContainerFilter {
            label_selector: std::collections::HashMap::from([(
                translate::labels::POD_UID.to_string(),
                pod.metadata.uid.clone(),
            )]),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let containers = cri.list_containers(Some(filter)).await?;
        let running = v1::ContainerState::ContainerRunning as i32;
        Ok(containers.iter().any(|c| c.state == running))
    }

    /// Names of all pods with a READY sandbox on this runtime.
    pub async fn list_running_pods(&self) -> Result<Vec<String>> {
        let filter = v1::PodSandboxFilter {
            state: Some(v1::PodSandboxStateValue {
                state: v1::PodSandboxState::SandboxReady as i32,
            }),
            ..Default::default()
        };
        let mut cri = self.cri.clone();
        let sandboxes = cri.list_pod_sandbox(Some(filter)).await?;
        Ok(sandboxes
            .into_iter()
            .filter_map(|s| s.metadata.map(|m| m.name))
            .collect())
    }

    /// Stop and remove a pod's sandbox; removing the sandbox tears down its
    /// containers. No-op if the pod has no sandbox.
    pub async fn stop_and_remove_pod(&self, pod_name: &str) -> Result<()> {
        let Some(sandbox_id) = self.sandbox_id_for(pod_name).await? else {
            return Ok(());
        };
        let mut cri = self.cri.clone();
        cri.stop_pod_sandbox(&sandbox_id).await?;
        cri.remove_pod_sandbox(&sandbox_id).await?;
        Ok(())
    }
}
