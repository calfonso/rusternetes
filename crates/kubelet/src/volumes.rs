//! Runtime-agnostic volume provisioning.
//!
//! [`VolumeManager`] owns the logic that materialises pod volumes on the host
//! filesystem (configMap / secret / projected / downwardAPI / emptyDir /
//! hostPath) plus the periodic resync/refresh of mutable sources. It is
//! deliberately free of any container-runtime coupling so that the CRI runtime
//! ([`crate::cri_runtime::CriContainerRuntime`]) can reuse it; behavior is
//! identical to when this code lived inline in `runtime.rs`.

use anyhow::{Context, Result};
use rusternetes_common::resources::{
    ConfigMap, PersistentVolume, PersistentVolumeClaim, Pod, Secret,
};
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

// Free helpers that stay in `runtime.rs` (they are not runtime-specific but are
// shared with non-volume code paths there). Imported so the moved bodies keep
// calling them by their bare names, verbatim.
use crate::runtime::{
    check_host_path_type, mount_tmpfs_for_emptydir, parse_cpu_quantity, parse_memory_quantity,
    parse_quantity_bytes, pod_dir_key, setup_emptydir_dir, HostPathCheck,
};

/// Provisions and maintains pod volumes on the host filesystem, independent of
/// any container runtime. See the module docs.
#[derive(Clone)]
pub struct VolumeManager {
    pub volumes_base_path: String,
    pub storage: Option<Arc<rusternetes_storage::StorageBackend>>,
    pub token_manager: rusternetes_common::auth::TokenManager,
}

impl VolumeManager {
    /// Construct a `VolumeManager` from the same three values the bollard
    /// `ContainerRuntime` already carries.
    pub fn new(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
    ) -> Self {
        Self {
            volumes_base_path,
            storage,
            token_manager,
        }
    }

    /// Remove the runtime-agnostic per-pod volume directory tree. The bollard
    /// runtime's `cleanup_pod_volumes` calls this after its own
    /// runtime-specific teardown (hostport rules, live-incarnation check,
    /// Docker named-volume removal). Kept verbatim from the original
    /// `cleanup_pod_volumes` body.
    pub fn remove_pod_volume_dir(&self, pod_name: &str) {
        let volume_dir = format!("{}/{}", self.volumes_base_path, pod_name);
        if std::path::Path::new(&volume_dir).exists() {
            if let Err(e) = std::fs::remove_dir_all(&volume_dir) {
                warn!("Failed to remove volume directory {}: {}", volume_dir, e);
            } else {
                info!("Cleaned up volumes for pod {}", pod_name);
            }
        }
    }

    /// Create volumes for a pod and return volume bindings for containers
    pub async fn create_pod_volumes(&self, pod: &Pod) -> Result<HashMap<String, String>> {
        let mut volume_paths = HashMap::new();

        if let Some(volumes) = &pod.spec.as_ref().unwrap().volumes {
            for volume in volumes {
                let volume_path = self.create_volume(pod, volume).await?;
                volume_paths.insert(volume.name.clone(), volume_path);
            }
        }

        // Apply fsGroup: change group ownership on all volume files.
        // Real Kubernetes behavior: fsGroup changes the group owner of volume files
        // to the specified GID, and sets group permission bits to match the owner bits
        // (i.e., if owner has read, group gets read; if owner has write, group gets write).
        // This preserves the defaultMode permissions — a file with mode 0440 stays 0440,
        // not 0460 (which would happen with unconditional g+rwX).
        #[cfg(unix)]
        if let Some(fs_group) = pod
            .spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.fs_group)
        {
            use std::os::unix::fs::PermissionsExt;
            for path in volume_paths.values() {
                // Recursively chown to fsGroup
                let _ = std::process::Command::new("chown")
                    .args(["-R", &format!(":{}", fs_group), path])
                    .output();

                // Set group bits to mirror owner bits on each file/directory.
                // This matches real K8s behavior: if owner=r--, group becomes r--
                // (not r+w which chmod g+rwX would do).
                fn apply_fsgroup_permissions(dir: &std::path::Path) {
                    use std::os::unix::fs::PermissionsExt;
                    if let Ok(entries) = std::fs::read_dir(dir) {
                        for entry in entries.flatten() {
                            let fpath = entry.path();
                            if let Ok(meta) = std::fs::metadata(&fpath) {
                                let mode = meta.permissions().mode();
                                // Copy owner bits (bits 8-6) to group bits (bits 5-3)
                                let owner_bits = (mode >> 6) & 0o7;
                                let new_mode = (mode & !0o070) | (owner_bits << 3);
                                if new_mode != mode {
                                    let _ = std::fs::set_permissions(
                                        &fpath,
                                        std::fs::Permissions::from_mode(new_mode),
                                    );
                                }
                                if meta.is_dir() {
                                    apply_fsgroup_permissions(&fpath);
                                }
                            }
                        }
                    }
                }
                apply_fsgroup_permissions(std::path::Path::new(path));

                // Set setgid bit on the directory itself so new files inherit group
                if let Ok(meta) = std::fs::metadata(path) {
                    let mode = meta.permissions().mode();
                    let owner_bits = (mode >> 6) & 0o7;
                    let new_mode = (mode & !0o070) | (owner_bits << 3) | 0o2000;
                    let _ =
                        std::fs::set_permissions(path, std::fs::Permissions::from_mode(new_mode));
                }
            }
            info!(
                "Applied fsGroup {} to {} volumes",
                fs_group,
                volume_paths.len()
            );
        }

        Ok(volume_paths)
    }

    /// Resync projected/secret/configmap volumes for a running pod.
    /// Re-reads source data from storage and updates volume files if changed.
    pub async fn resync_volumes<S: rusternetes_storage::Storage>(
        &self,
        pod: &Pod,
        storage: &S,
    ) -> Result<()> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        if let Some(volumes) = &pod.spec.as_ref().unwrap().volumes {
            for volume in volumes {
                // Resync secret volumes
                if let Some(secret_source) = &volume.secret {
                    let secret_name = match &secret_source.secret_name {
                        Some(n) => n,
                        None => continue,
                    };
                    let key =
                        rusternetes_storage::build_key("secrets", Some(namespace), secret_name);
                    let volume_dir =
                        format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
                    if let Ok(secret) = storage
                        .get::<rusternetes_common::resources::Secret>(&key)
                        .await
                    {
                        let mut expected_files: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        if let Some(data) = &secret.data {
                            if let Some(ref items) = secret_source.items {
                                // Only mount the specified keys at their mapped paths
                                for item in items {
                                    if let Some(v) = data.get(&item.key) {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        expected_files.insert(item.path.clone());
                                        if let Ok(existing) = std::fs::read(&file_path) {
                                            if existing == *v {
                                                continue;
                                            }
                                        }
                                        if let Some(parent) =
                                            std::path::Path::new(&file_path).parent()
                                        {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(&file_path, v);
                                    }
                                }
                            } else {
                                // Mount all keys
                                for (k, v) in data {
                                    let file_path = format!("{}/{}", volume_dir, k);
                                    expected_files.insert(k.clone());
                                    // Only write if content changed
                                    if let Ok(existing) = std::fs::read(&file_path) {
                                        if existing == *v {
                                            continue;
                                        }
                                    }
                                    let _ = std::fs::write(&file_path, v);
                                }
                            }
                        }
                        // Remove files that are no longer expected
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                if let Some(name) = entry.file_name().to_str() {
                                    if !expected_files.contains(name) {
                                        let _ = std::fs::remove_file(entry.path());
                                    }
                                }
                            }
                        }
                    } else if secret_source.optional != Some(true) {
                        // Secret was deleted entirely — remove all files if not optional
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                let _ = std::fs::remove_file(entry.path());
                            }
                        }
                    }
                }
                // Resync configmap volumes
                if let Some(cm_source) = &volume.config_map {
                    if let Some(cm_name) = &cm_source.name {
                        let key =
                            rusternetes_storage::build_key("configmaps", Some(namespace), cm_name);
                        if let Ok(cm) = storage
                            .get::<rusternetes_common::resources::ConfigMap>(&key)
                            .await
                        {
                            let volume_dir =
                                format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
                            if let Some(ref items) = cm_source.items {
                                // Only mount the specified keys at their mapped paths
                                for item in items {
                                    if let Some(value) =
                                        cm.data.as_ref().and_then(|d| d.get(&item.key))
                                    {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                            if existing == *value {
                                                continue;
                                            }
                                        }
                                        if let Some(parent) =
                                            std::path::Path::new(&file_path).parent()
                                        {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(&file_path, value);
                                    } else if let Some(value) =
                                        cm.binary_data.as_ref().and_then(|d| d.get(&item.key))
                                    {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        if let Ok(existing) = std::fs::read(&file_path) {
                                            if existing == *value {
                                                continue;
                                            }
                                        }
                                        if let Some(parent) =
                                            std::path::Path::new(&file_path).parent()
                                        {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(&file_path, value);
                                    }
                                }
                            } else {
                                // Mount all keys from data
                                if let Some(data) = &cm.data {
                                    for (k, v) in data {
                                        let file_path = format!("{}/{}", volume_dir, k);
                                        if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                            if existing == *v {
                                                continue;
                                            }
                                        }
                                        let _ = std::fs::write(&file_path, v);
                                    }
                                }
                                // Mount all keys from binaryData
                                if let Some(binary_data) = &cm.binary_data {
                                    for (k, v) in binary_data {
                                        let file_path = format!("{}/{}", volume_dir, k);
                                        if let Ok(existing) = std::fs::read(&file_path) {
                                            if existing == *v {
                                                continue;
                                            }
                                        }
                                        let _ = std::fs::write(&file_path, v);
                                    }
                                }
                            }
                        }
                    }
                }
                // Resync projected volumes (may contain configmap/secret projections)
                if let Some(projected) = &volume.projected {
                    if let Some(sources) = &projected.sources {
                        let volume_dir =
                            format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
                        // Track expected files so we can delete stale ones
                        let mut expected_files: std::collections::HashSet<String> =
                            std::collections::HashSet::new();

                        // Per-file permissions must survive a resync rewrite, matching
                        // the initial-mount path and upstream's atomic writer (which
                        // re-applies each file's mode on every update). A plain
                        // `fs::write` of a *new* key (added after pod start) would
                        // otherwise leave it at the umask default instead of the
                        // item's `mode` / the projection `defaultMode` (#1050).
                        let proj_default_mode = projected.default_mode.unwrap_or(0o644);
                        let apply_mode = |path: &str, mode: i32| {
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                let _ = std::fs::set_permissions(
                                    path,
                                    std::fs::Permissions::from_mode(mode as u32),
                                );
                            }
                            #[cfg(not(unix))]
                            {
                                let _ = (path, mode);
                            }
                        };

                        for source in sources {
                            if let Some(cm_proj) = &source.config_map {
                                if let Some(cm_name) = &cm_proj.name {
                                    let key = rusternetes_storage::build_key(
                                        "configmaps",
                                        Some(namespace),
                                        cm_name,
                                    );
                                    if let Ok(cm) = storage
                                        .get::<rusternetes_common::resources::ConfigMap>(&key)
                                        .await
                                    {
                                        if let Some(items) = &cm_proj.items {
                                            // Selective projection — only mount specified keys
                                            for item in items {
                                                let file_path =
                                                    format!("{}/{}", volume_dir, item.path);
                                                expected_files.insert(file_path.clone());
                                                if let Some(value) =
                                                    cm.data.as_ref().and_then(|d| d.get(&item.key))
                                                {
                                                    if let Ok(existing) =
                                                        std::fs::read_to_string(&file_path)
                                                    {
                                                        if existing == *value {
                                                            continue;
                                                        }
                                                    }
                                                    if let Some(parent) =
                                                        std::path::Path::new(&file_path).parent()
                                                    {
                                                        let _ = std::fs::create_dir_all(parent);
                                                    }
                                                    if std::fs::write(&file_path, value).is_ok() {
                                                        apply_mode(
                                                            &file_path,
                                                            item.mode.unwrap_or(proj_default_mode),
                                                        );
                                                    }
                                                }
                                            }
                                        } else if let Some(data) = &cm.data {
                                            // Mount all keys
                                            for (k, v) in data {
                                                let file_path = format!("{}/{}", volume_dir, k);
                                                expected_files.insert(file_path.clone());
                                                if let Ok(existing) =
                                                    std::fs::read_to_string(&file_path)
                                                {
                                                    if existing == *v {
                                                        continue;
                                                    }
                                                }
                                                if std::fs::write(&file_path, v).is_ok() {
                                                    apply_mode(&file_path, proj_default_mode);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some(sec_proj) = &source.secret {
                                if let Some(sec_name) = &sec_proj.name {
                                    let key = rusternetes_storage::build_key(
                                        "secrets",
                                        Some(namespace),
                                        sec_name,
                                    );
                                    if let Ok(secret) = storage
                                        .get::<rusternetes_common::resources::Secret>(&key)
                                        .await
                                    {
                                        if let Some(data) = &secret.data {
                                            if let Some(items) = &sec_proj.items {
                                                for item in items {
                                                    let file_path =
                                                        format!("{}/{}", volume_dir, item.path);
                                                    expected_files.insert(file_path.clone());
                                                    if let Some(v) = data.get(&item.key) {
                                                        if let Ok(existing) =
                                                            std::fs::read(&file_path)
                                                        {
                                                            if existing == *v {
                                                                apply_mode(
                                                                    &file_path,
                                                                    item.mode.unwrap_or(
                                                                        proj_default_mode,
                                                                    ),
                                                                );
                                                                continue;
                                                            }
                                                        }
                                                        if let Some(parent) =
                                                            std::path::Path::new(&file_path)
                                                                .parent()
                                                        {
                                                            let _ = std::fs::create_dir_all(parent);
                                                        }
                                                        if std::fs::write(&file_path, v).is_ok() {
                                                            apply_mode(
                                                                &file_path,
                                                                item.mode
                                                                    .unwrap_or(proj_default_mode),
                                                            );
                                                        }
                                                    }
                                                }
                                            } else {
                                                for (k, v) in data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    expected_files.insert(file_path.clone());
                                                    if let Ok(existing) = std::fs::read(&file_path)
                                                    {
                                                        if existing == *v {
                                                            apply_mode(
                                                                &file_path,
                                                                proj_default_mode,
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                    if std::fs::write(&file_path, v).is_ok() {
                                                        apply_mode(&file_path, proj_default_mode);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // ServiceAccountToken projection resync — preserve the token file
                            if let Some(sa_token) = &source.service_account_token {
                                let file_path = format!("{}/{}", volume_dir, sa_token.path);
                                expected_files.insert(file_path);
                            }
                            // DownwardAPI projection resync
                            if let Some(downward_api) = &source.downward_api {
                                if let Some(items) = &downward_api.items {
                                    for item in items {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        expected_files.insert(file_path.clone());
                                        let value = if let Some(ref field_ref) = item.field_ref {
                                            self.get_pod_field_value(pod, &field_ref.field_path)
                                                .unwrap_or_default()
                                        } else if let Some(ref resource_ref) =
                                            item.resource_field_ref
                                        {
                                            self.get_container_resource_value(pod, resource_ref)
                                                .unwrap_or_default()
                                        } else {
                                            String::new()
                                        };
                                        if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                            if existing == value {
                                                continue;
                                            }
                                        }
                                        let _ = std::fs::write(&file_path, &value);
                                    }
                                }
                            }
                        }

                        // Delete stale files that are no longer in any projection source
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_file() {
                                    let path_str = path.to_string_lossy().to_string();
                                    if !expected_files.contains(&path_str) {
                                        let _ = std::fs::remove_file(&path);
                                    }
                                }
                            }
                        }
                    }
                }
                // Resync standalone downwardAPI volumes
                if let Some(downward_api) = &volume.downward_api {
                    if let Some(items) = &downward_api.items {
                        let volume_dir =
                            format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
                        for item in items {
                            let file_path = format!("{}/{}", volume_dir, item.path);
                            let value = if let Some(ref field_ref) = item.field_ref {
                                self.get_pod_field_value(pod, &field_ref.field_path)
                                    .unwrap_or_default()
                            } else if let Some(ref resource_ref) = item.resource_field_ref {
                                self.get_container_resource_value(pod, resource_ref)
                                    .unwrap_or_default()
                            } else {
                                String::new()
                            };
                            if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                if existing == value {
                                    continue;
                                }
                            }
                            let _ = std::fs::write(&file_path, &value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Create a single volume and return its host path
    pub(crate) async fn create_volume(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> Result<String> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // EmptyDir: create a directory on the shared volumes path.
        // K8s ref: pkg/volume/emptydir/empty_dir.go — setupDir() sets mode 0777.
        // Note: host bind mounts through virtiofs (Podman Machine / Docker Desktop)
        // may not enforce chmod correctly. The emptyDir 0777/0666 permission tests
        // are pre-existing failures on macOS VM-based runtimes. On Linux (where
        // conformance actually runs), bind mounts preserve mode bits, so setup_emptydir_dir
        // ensures the directory exists with mode 0o777 and idempotently re-chmods even
        // when the directory pre-exists from a prior run.
        if volume.empty_dir.is_some() {
            // Key the on-disk path on pod UID, not name, to mirror upstream
            // pkg/kubelet/kubelet_getters.go::getPodVolumeDir +
            // pkg/volume/emptydir/empty_dir.go::getPath. A recreated pod gets a
            // new UID, so the new emptyDir is guaranteed fresh — kubelet never
            // reads the previous pod's files.
            let pod_key = pod_dir_key(pod);
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_key, volume.name);
            setup_emptydir_dir(&volume_dir).context("Failed to create emptyDir volume")?;

            // Memory-medium emptyDir is a tmpfs. Mount it on the host volume dir
            // (propagated to the host daemon via the kubelet's rshared bind) so
            // it persists across container restarts AND reports fs_type=tmpfs.
            // K8s ref: pkg/volume/emptydir/empty_dir.go.
            let is_memory =
                volume.empty_dir.as_ref().and_then(|e| e.medium.as_deref()) == Some("Memory");
            if is_memory {
                let size_bytes = volume
                    .empty_dir
                    .as_ref()
                    .and_then(|e| e.size_limit.as_deref())
                    .and_then(parse_quantity_bytes);
                mount_tmpfs_for_emptydir(&volume_dir, size_bytes);
            }
            info!("Created emptyDir volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // HostPath: use the specified host path. The `type` field is validated
        // (and "OrCreate" variants are materialised) via `check_host_path_type`,
        // mirroring upstream `pkg/volume/host_path/host_path.go::checkType` —
        // see also tests/conformance_storage_emptydir_hostpath.rs.
        if let Some(host_path) = &volume.host_path {
            // Expand environment variables in the path
            let path = crate::runtime::expand_env_vars(&host_path.path);
            match check_host_path_type(&path, host_path.type_.as_deref()) {
                HostPathCheck::Ok => {}
                HostPathCheck::Missing => {
                    return Err(anyhow::anyhow!(
                        "hostPath {} does not exist (type={:?})",
                        path,
                        host_path.type_
                    ));
                }
                HostPathCheck::WrongKind => {
                    return Err(anyhow::anyhow!(
                        "hostPath {} exists but does not match type={:?}",
                        path,
                        host_path.type_
                    ));
                }
                HostPathCheck::UnsupportedType => {
                    return Err(anyhow::anyhow!(
                        "hostPath {} declared unknown type {:?}",
                        path,
                        host_path.type_
                    ));
                }
            }
            info!("Using hostPath volume {} at {}", volume.name, path);
            return Ok(path);
        }

        // ConfigMap: mount configmap data as files
        if let Some(configmap_source) = &volume.config_map {
            let storage = self
                .storage
                .as_ref()
                .context("Storage not available for ConfigMap volumes")?;

            let configmap_name = configmap_source
                .name
                .as_ref()
                .context("ConfigMap volume must specify name")?;

            let is_optional = configmap_source.optional.unwrap_or(false);

            let key = build_key("configmaps", Some(namespace), configmap_name);
            let configmap_result: Result<ConfigMap, _> = storage.get(&key).await;

            // Create volume directory
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create ConfigMap volume directory")?;

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let cm_default_mode = configmap_source.default_mode.unwrap_or(0o644);

            // Compute final directory permissions (will be applied after files are written)
            #[cfg(unix)]
            let cm_dir_mode = cm_default_mode as u32 | 0o111;

            match configmap_result {
                Ok(configmap) => {
                    // Helper closure to write a file and set permissions
                    let write_cm_file =
                        |file_path: &str, content: &[u8], mode: i32| -> Result<()> {
                            if let Some(parent) = std::path::Path::new(file_path).parent() {
                                std::fs::create_dir_all(parent)?;
                            }
                            std::fs::write(file_path, content)?;
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                std::fs::set_permissions(
                                    file_path,
                                    std::fs::Permissions::from_mode(mode as u32),
                                )?;
                            }
                            Ok(())
                        };

                    // Check if specific items are requested
                    if let Some(ref items) = configmap_source.items {
                        // Only mount the specified keys (look in both data and binaryData)
                        for item in items {
                            let mode = item.mode.unwrap_or(cm_default_mode);
                            let file_path = format!("{}/{}", volume_dir, item.path);

                            // Try data first, then binary_data
                            if let Some(value) =
                                configmap.data.as_ref().and_then(|d| d.get(&item.key))
                            {
                                write_cm_file(&file_path, value.as_bytes(), mode).with_context(
                                    || {
                                        format!(
                                            "Failed to write ConfigMap key {} to file",
                                            item.key
                                        )
                                    },
                                )?;
                                info!("Wrote ConfigMap key {} to {}", item.key, file_path);
                            } else if let Some(value) = configmap
                                .binary_data
                                .as_ref()
                                .and_then(|d| d.get(&item.key))
                            {
                                write_cm_file(&file_path, value, mode).with_context(|| {
                                    format!(
                                        "Failed to write ConfigMap binaryData key {} to file",
                                        item.key
                                    )
                                })?;
                                info!(
                                    "Wrote ConfigMap binaryData key {} to {}",
                                    item.key, file_path
                                );
                            } else if !is_optional {
                                warn!("ConfigMap {} missing key {}", configmap_name, item.key);
                            }
                        }
                    } else {
                        // Mount all keys from data
                        if let Some(data) = &configmap.data {
                            for (key, value) in data {
                                let file_path = format!("{}/{}", volume_dir, key);
                                write_cm_file(&file_path, value.as_bytes(), cm_default_mode)
                                    .with_context(|| {
                                        format!("Failed to write ConfigMap key {} to file", key)
                                    })?;
                                info!("Wrote ConfigMap key {} to {}", key, file_path);
                            }
                        }
                        // Mount all keys from binaryData
                        if let Some(binary_data) = &configmap.binary_data {
                            for (key, value) in binary_data {
                                let file_path = format!("{}/{}", volume_dir, key);
                                write_cm_file(&file_path, value, cm_default_mode).with_context(
                                    || {
                                        format!(
                                            "Failed to write ConfigMap binaryData key {} to file",
                                            key
                                        )
                                    },
                                )?;
                                info!("Wrote ConfigMap binaryData key {} to {}", key, file_path);
                            }
                        }
                    }
                }
                Err(e) => {
                    if is_optional {
                        info!(
                            "Optional ConfigMap {} not found in namespace {}, creating empty volume",
                            configmap_name, namespace
                        );
                    } else {
                        // Required ConfigMap not found — abort pod start so kubelet
                        // retries on next reconciliation (when the ConfigMap exists).
                        return Err(anyhow::anyhow!(
                            "ConfigMap {} not found in namespace {}: {}",
                            configmap_name,
                            namespace,
                            e
                        ));
                    }
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(cm_dir_mode),
                )?;
            }

            info!("Created ConfigMap volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // Secret: mount secret data as files
        if let Some(secret_source) = &volume.secret {
            let storage = self
                .storage
                .as_ref()
                .context("Storage not available for Secret volumes")?;

            let secret_name = secret_source
                .secret_name
                .as_ref()
                .context("Secret volume must specify secret_name")?;

            let is_optional = secret_source.optional.unwrap_or(false);

            // For SA token volumes, generate a bound token with pod reference
            // instead of using the static token from the Secret.
            let is_sa_token_volume =
                volume.name.contains("kube-api-access") || secret_name.ends_with("-token");
            let bound_token: Option<String> = if is_sa_token_volume {
                let sa_name = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.service_account_name.as_deref())
                    .unwrap_or("default");
                let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);
                let sa_uid = storage
                    .get::<serde_json::Value>(&sa_key)
                    .await
                    .ok()
                    .and_then(|v| {
                        v.pointer("/metadata/uid")
                            .and_then(|u| u.as_str())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_default();
                let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
                let node_uid = if let Some(ref nn) = node_name {
                    let node_key = build_key("nodes", None::<&str>, nn);
                    storage
                        .get::<serde_json::Value>(&node_key)
                        .await
                        .ok()
                        .and_then(|v| {
                            v.pointer("/metadata/uid")
                                .and_then(|u| u.as_str())
                                .map(|s| s.to_string())
                        })
                } else {
                    None
                };
                let now = chrono::Utc::now();
                let claims = rusternetes_common::auth::ServiceAccountClaims {
                    sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
                    namespace: namespace.to_string(),
                    uid: sa_uid.clone(),
                    iat: now.timestamp(),
                    exp: (now + chrono::Duration::hours(1)).timestamp(),
                    iss: "https://kubernetes.default.svc.cluster.local".to_string(),
                    aud: vec!["rusternetes".to_string()],
                    kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                        namespace: namespace.to_string(),
                        svcacct: rusternetes_common::auth::KubeRef {
                            name: sa_name.to_string(),
                            uid: sa_uid,
                        },
                        pod: Some(rusternetes_common::auth::KubeRef {
                            name: pod_name.to_string(),
                            uid: pod.metadata.uid.clone(),
                        }),
                        node: node_name
                            .as_ref()
                            .map(|nn| rusternetes_common::auth::KubeRef {
                                name: nn.clone(),
                                uid: node_uid.clone().unwrap_or_default(),
                            }),
                    }),
                    pod_name: Some(pod_name.to_string()),
                    pod_uid: Some(pod.metadata.uid.clone()),
                    node_name,
                    node_uid,
                };
                self.token_manager.generate_token(claims).ok()
            } else {
                None
            };

            let key = build_key("secrets", Some(namespace), secret_name);
            let secret_result: Result<Secret, _> = storage.get(&key).await;

            // Create volume directory
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create Secret volume directory")?;

            // Compute final directory permissions (will be applied after files are written)
            #[cfg(unix)]
            let secret_dir_mode = secret_source.default_mode.unwrap_or(0o644) as u32 | 0o111;

            let secret = match secret_result {
                Ok(s) => Some(s),
                Err(e) => {
                    if is_optional {
                        info!(
                            "Optional Secret {} not found in namespace {}, creating empty volume",
                            secret_name, namespace
                        );
                        None
                    } else {
                        // Required secret not found — return error so the kubelet
                        // retries on next sync. K8s leaves the pod in Pending with
                        // ContainerCreating and retries until the secret exists.
                        // K8s ref: pkg/kubelet/kubelet_pods.go — makeVolumes
                        return Err(anyhow::anyhow!(
                            "Secret {} not found in namespace {}: {}",
                            secret_name,
                            namespace,
                            e
                        ));
                    }
                }
            };

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let secret_default_mode = secret_source.default_mode.unwrap_or(0o644);

            // Write secret data as files
            if let Some(data) = secret.as_ref().and_then(|s| s.data.as_ref()) {
                if let Some(ref items) = secret_source.items {
                    // Only mount the specified keys
                    for item in items {
                        if let Some(value) = data.get(&item.key) {
                            let file_path = format!("{}/{}", volume_dir, item.path);
                            // Create parent directories if needed
                            if let Some(parent) = std::path::Path::new(&file_path).parent() {
                                std::fs::create_dir_all(parent).with_context(|| {
                                    format!(
                                        "Failed to create directory for Secret item {}",
                                        item.path
                                    )
                                })?;
                            }
                            // For SA token volumes, substitute the bound token
                            let write_value: &[u8] = if item.key == "token" {
                                if let Some(ref bt) = bound_token {
                                    bt.as_bytes()
                                } else {
                                    value
                                }
                            } else {
                                value
                            };
                            std::fs::write(&file_path, write_value).with_context(|| {
                                format!("Failed to write Secret key {} to file", item.key)
                            })?;
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                let mode = item.mode.unwrap_or(secret_default_mode) as u32;
                                std::fs::set_permissions(
                                    &file_path,
                                    std::fs::Permissions::from_mode(mode),
                                )?;
                            }
                            if bound_token.is_some() && item.key == "token" {
                                info!("Wrote bound SA token for pod {} to {}", pod_name, file_path);
                            } else {
                                info!("Wrote Secret key {} to {}", item.key, file_path);
                            }
                        }
                    }
                } else {
                    // Mount all keys
                    for (key, value) in data {
                        let file_path = format!("{}/{}", volume_dir, key);
                        // For SA token volumes, substitute the bound token
                        let write_value: &[u8] = if key == "token" {
                            if let Some(ref bt) = bound_token {
                                bt.as_bytes()
                            } else {
                                value.as_slice()
                            }
                        } else {
                            value.as_slice()
                        };
                        std::fs::write(&file_path, write_value).with_context(|| {
                            format!("Failed to write Secret key {} to file", key)
                        })?;
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            std::fs::set_permissions(
                                &file_path,
                                std::fs::Permissions::from_mode(secret_default_mode as u32),
                            )?;
                        }
                        info!("Wrote Secret key {} to {}", key, file_path);
                    }
                }
            }

            // Special handling for service account token secrets - add ca.crt
            // Service account secrets are identified by having a "token" key or by name pattern
            let is_service_account_secret = secret
                .as_ref()
                .and_then(|s| s.data.as_ref())
                .map(|data| data.contains_key("token"))
                .unwrap_or(false)
                || secret_name.ends_with("-token");

            if is_service_account_secret {
                // Check if ca.crt already exists in the secret data
                let has_ca_cert = secret
                    .as_ref()
                    .and_then(|s| s.data.as_ref())
                    .map(|data| data.contains_key("ca.crt"))
                    .unwrap_or(false);

                if !has_ca_cert {
                    // Inject ca.crt from the cluster CA certificate
                    // Try multiple locations: environment variable, volumes/_certs, then fallback to .rusternetes/certs
                    let ca_cert_source = std::env::var("CA_CERT_PATH").unwrap_or_else(|_| {
                        // First try volumes/_certs (accessible from kubelet container)
                        let volumes_cert_path = format!("{}/_certs/ca.crt", self.volumes_base_path);
                        if std::path::Path::new(&volumes_cert_path).exists() {
                            volumes_cert_path
                        } else {
                            // Fallback to .rusternetes/certs (for host-based kubelet)
                            format!(
                                "{}/.rusternetes/certs/ca.crt",
                                std::env::var("HOME").unwrap_or_else(|_| "/root".to_string())
                            )
                        }
                    });

                    let ca_path = format!("{}/ca.crt", volume_dir);
                    if let Ok(ca_content) = std::fs::read(&ca_cert_source) {
                        std::fs::write(&ca_path, ca_content)
                            .context("Failed to write CA certificate")?;
                        info!("Injected CA certificate into service account secret volume at {} (from {})", ca_path, ca_cert_source);
                    } else {
                        warn!("CA certificate not found at {}, pods may not be able to verify API server", ca_cert_source);
                    }
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values (e.g., 0o400) don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(secret_dir_mode),
                )?;
            }

            info!("Created Secret volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // PersistentVolumeClaim: find bound PV and use its path
        if let Some(pvc_source) = &volume.persistent_volume_claim {
            let storage = self
                .storage
                .as_ref()
                .context("Storage not available for PersistentVolumeClaim volumes")?;

            let pvc_key = build_key(
                "persistentvolumeclaims",
                Some(namespace),
                &pvc_source.claim_name,
            );
            let pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.with_context(|| {
                format!(
                    "PersistentVolumeClaim {} not found in namespace {}",
                    pvc_source.claim_name, namespace
                )
            })?;

            // Get the bound PV name
            let pv_name = pvc
                .spec
                .volume_name
                .as_ref()
                .context("PersistentVolumeClaim is not bound to a volume")?;

            // Get the PV
            let pv_key = build_key("persistentvolumes", None, pv_name);
            let pv: PersistentVolume = storage
                .get(&pv_key)
                .await
                .with_context(|| format!("PersistentVolume {} not found", pv_name))?;

            // Get the host path from the PV
            let path = if let Some(hp) = &pv.spec.host_path {
                hp.path.clone()
            } else {
                return Err(anyhow::anyhow!(
                    "PersistentVolume does not have a hostPath volume source"
                ));
            };
            info!(
                "Using PersistentVolumeClaim volume {} backed by PV {} at {}",
                volume.name, pv_name, path
            );
            return Ok(path);
        }

        // DownwardAPI: expose pod/container metadata as files
        if let Some(downward_api) = &volume.downward_api {
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create DownwardAPI volume directory")?;

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let da_default_mode = downward_api.default_mode.unwrap_or(0o644);

            // Compute final directory permissions (applied after files are written)
            #[cfg(unix)]
            let da_dir_mode = da_default_mode as u32 | 0o111;

            if let Some(items) = &downward_api.items {
                for item in items {
                    let file_path = format!("{}/{}", volume_dir, item.path);

                    // Create parent directories if needed
                    if let Some(parent) = std::path::Path::new(&file_path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }

                    // Get the value from field_ref or resource_field_ref
                    let value = if let Some(field_ref) = &item.field_ref {
                        self.get_pod_field_value(pod, &field_ref.field_path)?
                    } else if let Some(resource_ref) = &item.resource_field_ref {
                        self.get_container_resource_value(pod, resource_ref)?
                    } else {
                        return Err(anyhow::anyhow!(
                            "DownwardAPI item must have either fieldRef or resourceFieldRef"
                        ));
                    };

                    std::fs::write(&file_path, value).with_context(|| {
                        format!("Failed to write DownwardAPI file {}", file_path)
                    })?;

                    // Set file permissions: per-item mode overrides defaultMode
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let mode = item.mode.unwrap_or(da_default_mode) as u32;
                        std::fs::set_permissions(
                            &file_path,
                            std::fs::Permissions::from_mode(mode),
                        )?;
                    }

                    info!(
                        "Wrote DownwardAPI file {} with value from {}",
                        file_path, item.path
                    );
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(da_dir_mode),
                )?;
            }

            info!(
                "Created DownwardAPI volume {} at {}",
                volume.name, volume_dir
            );
            return Ok(volume_dir);
        }

        // CSI: ephemeral inline volume (handled by external CSI driver)
        if let Some(_csi) = &volume.csi {
            // CSI ephemeral inline volumes are managed by the CSI driver via the kubelet CSI plugin
            // For conformance, we create a placeholder directory and rely on the CSI driver to populate it
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create CSI volume directory")?;

            info!(
                "Created CSI ephemeral volume {} at {} (managed by CSI driver)",
                volume.name, volume_dir
            );
            return Ok(volume_dir);
        }

        // Ephemeral: generic ephemeral volume with PVC template
        if let Some(ephemeral) = &volume.ephemeral {
            if let Some(pvc_template) = &ephemeral.volume_claim_template {
                let storage = self
                    .storage
                    .as_ref()
                    .context("Storage not available for ephemeral volumes")?;

                // Create a PVC name based on pod name and volume name
                let pvc_name = format!("{}-{}", pod_name, volume.name);

                // Check if PVC already exists
                let pvc_key = build_key("persistentvolumeclaims", Some(namespace), &pvc_name);
                let pvc_exists = storage.get::<PersistentVolumeClaim>(&pvc_key).await.is_ok();

                if !pvc_exists {
                    // Create the PVC from the template
                    let mut pvc = PersistentVolumeClaim {
                        type_meta: rusternetes_common::types::TypeMeta {
                            kind: "PersistentVolumeClaim".to_string(),
                            api_version: "v1".to_string(),
                        },
                        metadata: rusternetes_common::types::ObjectMeta::new(&pvc_name)
                            .with_namespace(namespace),
                        spec: pvc_template.spec.clone(),
                        status: None,
                    };

                    // Copy labels and annotations from template if provided
                    if let Some(template_meta) = &pvc_template.metadata {
                        if let Some(labels) = &template_meta.labels {
                            pvc.metadata.labels = Some(labels.clone());
                        }
                        if let Some(annotations) = &template_meta.annotations {
                            pvc.metadata.annotations = Some(annotations.clone());
                        }
                    }

                    // Store the PVC
                    storage
                        .create(&pvc_key, &pvc)
                        .await
                        .context("Failed to create ephemeral PVC")?;

                    info!(
                        "Created ephemeral PVC {} for volume {}",
                        pvc_name, volume.name
                    );

                    // Wait for PVC to be bound (simplified - in production would poll/watch)
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }

                // Now use the PVC like a regular PersistentVolumeClaim
                let pvc: PersistentVolumeClaim = storage
                    .get(&pvc_key)
                    .await
                    .with_context(|| format!("Ephemeral PVC {} not found", pvc_name))?;

                if let Some(pv_name) = &pvc.spec.volume_name {
                    let pv_key = build_key("persistentvolumes", None, pv_name);
                    let pv: PersistentVolume = storage.get(&pv_key).await.with_context(|| {
                        format!(
                            "PersistentVolume {} not found for ephemeral volume",
                            pv_name
                        )
                    })?;

                    let path = if let Some(hp) = &pv.spec.host_path {
                        hp.path.clone()
                    } else {
                        return Err(anyhow::anyhow!(
                            "PersistentVolume does not have a hostPath volume source"
                        ));
                    };

                    info!(
                        "Using ephemeral volume {} backed by PVC {} and PV {} at {}",
                        volume.name, pvc_name, pv_name, path
                    );
                    return Ok(path);
                } else {
                    return Err(anyhow::anyhow!(
                        "Ephemeral PVC {} is not bound yet",
                        pvc_name
                    ));
                }
            }
        }

        // Projected: combine multiple volume sources (configMap, secret, downwardAPI, serviceAccountToken) into one directory
        if let Some(projected) = &volume.projected {
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create projected volume directory")?;

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let proj_default_mode = projected.default_mode.unwrap_or(0o644);

            // Compute final directory permissions (will be applied after files are written)
            #[cfg(unix)]
            let proj_dir_mode = proj_default_mode as u32 | 0o111;

            if let Some(sources) = &projected.sources {
                let storage = self.storage.as_ref();

                for source in sources {
                    // ConfigMap projection
                    if let Some(cm_proj) = &source.config_map {
                        if let Some(cm_name) = &cm_proj.name {
                            let key = build_key("configmaps", Some(namespace), cm_name);
                            if let Some(storage) = storage {
                                match storage.get::<ConfigMap>(&key).await {
                                    Ok(cm) => {
                                        // Helper to write a projected file with permissions
                                        let write_proj_file =
                                            |path: &str, content: &[u8], mode: i32| -> Result<()> {
                                                if let Some(parent) =
                                                    std::path::Path::new(path).parent()
                                                {
                                                    std::fs::create_dir_all(parent)?;
                                                }
                                                std::fs::write(path, content)?;
                                                #[cfg(unix)]
                                                {
                                                    use std::os::unix::fs::PermissionsExt;
                                                    std::fs::set_permissions(
                                                        path,
                                                        std::fs::Permissions::from_mode(
                                                            mode as u32,
                                                        ),
                                                    )?;
                                                }
                                                Ok(())
                                            };

                                        if let Some(items) = &cm_proj.items {
                                            for item in items {
                                                let mode = item.mode.unwrap_or(proj_default_mode);
                                                let file_path =
                                                    format!("{}/{}", volume_dir, item.path);
                                                // Try data first, then binaryData
                                                if let Some(value) =
                                                    cm.data.as_ref().and_then(|d| d.get(&item.key))
                                                {
                                                    write_proj_file(
                                                        &file_path,
                                                        value.as_bytes(),
                                                        mode,
                                                    )?;
                                                } else if let Some(value) = cm
                                                    .binary_data
                                                    .as_ref()
                                                    .and_then(|d| d.get(&item.key))
                                                {
                                                    write_proj_file(&file_path, value, mode)?;
                                                }
                                            }
                                        } else {
                                            // Mount all keys from data
                                            if let Some(data) = &cm.data {
                                                for (k, v) in data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    write_proj_file(
                                                        &file_path,
                                                        v.as_bytes(),
                                                        proj_default_mode,
                                                    )?;
                                                }
                                            }
                                            // Mount all keys from binaryData
                                            if let Some(binary_data) = &cm.binary_data {
                                                for (k, v) in binary_data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    write_proj_file(
                                                        &file_path,
                                                        v,
                                                        proj_default_mode,
                                                    )?;
                                                }
                                            }
                                        }
                                    }
                                    Err(_) if cm_proj.optional.unwrap_or(false) => {
                                        // Optional configmap not found, skip
                                    }
                                    Err(e) => {
                                        warn!("Failed to get ConfigMap {} for projected volume: {}. Skipping.", cm_name, e);
                                    }
                                }
                            }
                        }
                    }

                    // Secret projection
                    if let Some(secret_proj) = &source.secret {
                        if let Some(secret_name) = &secret_proj.name {
                            let key = build_key("secrets", Some(namespace), secret_name);
                            if let Some(storage) = storage {
                                match storage.get::<Secret>(&key).await {
                                    Ok(secret) => {
                                        if let Some(data) = &secret.data {
                                            if let Some(items) = &secret_proj.items {
                                                for item in items {
                                                    if let Some(value) = data.get(&item.key) {
                                                        let file_path =
                                                            format!("{}/{}", volume_dir, item.path);
                                                        if let Some(parent) =
                                                            std::path::Path::new(&file_path)
                                                                .parent()
                                                        {
                                                            std::fs::create_dir_all(parent)?;
                                                        }
                                                        std::fs::write(&file_path, value)?;
                                                        #[cfg(unix)]
                                                        {
                                                            use std::os::unix::fs::PermissionsExt;
                                                            let mode = item
                                                                .mode
                                                                .unwrap_or(proj_default_mode)
                                                                as u32;
                                                            std::fs::set_permissions(
                                                                &file_path,
                                                                std::fs::Permissions::from_mode(
                                                                    mode,
                                                                ),
                                                            )?;
                                                        }
                                                    }
                                                }
                                            } else {
                                                for (k, v) in data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    std::fs::write(&file_path, v)?;
                                                    #[cfg(unix)]
                                                    {
                                                        use std::os::unix::fs::PermissionsExt;
                                                        std::fs::set_permissions(
                                                            &file_path,
                                                            std::fs::Permissions::from_mode(
                                                                proj_default_mode as u32,
                                                            ),
                                                        )?;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(_) if secret_proj.optional.unwrap_or(false) => {
                                        // Optional secret not found, skip
                                    }
                                    Err(e) => {
                                        warn!("Failed to get Secret {} for projected volume: {}. Skipping.", secret_name, e);
                                    }
                                }
                            }
                        }
                    }

                    // DownwardAPI projection
                    if let Some(downward_api) = &source.downward_api {
                        if let Some(items) = &downward_api.items {
                            for item in items {
                                let file_path = format!("{}/{}", volume_dir, item.path);
                                if let Some(parent) = std::path::Path::new(&file_path).parent() {
                                    std::fs::create_dir_all(parent)?;
                                }
                                let value = if let Some(ref field_ref) = item.field_ref {
                                    self.get_pod_field_value(pod, &field_ref.field_path)
                                        .unwrap_or_default()
                                } else if let Some(ref resource_ref) = item.resource_field_ref {
                                    self.get_container_resource_value(pod, resource_ref)
                                        .unwrap_or_default()
                                } else {
                                    String::new()
                                };
                                std::fs::write(&file_path, &value)?;
                                #[cfg(unix)]
                                {
                                    use std::os::unix::fs::PermissionsExt;
                                    let mode = item.mode.unwrap_or(proj_default_mode) as u32;
                                    std::fs::set_permissions(
                                        &file_path,
                                        std::fs::Permissions::from_mode(mode),
                                    )?;
                                }
                            }
                        }
                    }

                    // ServiceAccountToken projection
                    if let Some(sa_token) = &source.service_account_token {
                        let token_path = format!("{}/{}", volume_dir, sa_token.path);
                        if let Some(parent) = std::path::Path::new(&token_path).parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        // Generate a real JWT token bound to this pod
                        let sa_name = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.service_account_name.as_deref())
                            .unwrap_or("default");
                        let sa_uid = if let Some(storage) = storage {
                            let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);
                            match storage
                                .get::<rusternetes_common::resources::ServiceAccount>(&sa_key)
                                .await
                            {
                                Ok(sa) => sa.metadata.uid.clone(),
                                Err(_) => String::new(),
                            }
                        } else {
                            String::new()
                        };
                        let expiration_seconds = sa_token.expiration_seconds.unwrap_or(3600);
                        let now = chrono::Utc::now();
                        let exp = now.timestamp() + expiration_seconds;
                        let mut audiences = vec!["rusternetes".to_string()];
                        if let Some(ref aud) = sa_token.audience {
                            audiences = vec![aud.clone()];
                        }
                        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
                        let node_uid = if let (Some(ref nn), Some(st)) = (&node_name, storage) {
                            let node_key = build_key("nodes", None::<&str>, nn);
                            st.get::<serde_json::Value>(&node_key)
                                .await
                                .ok()
                                .and_then(|v| {
                                    v.pointer("/metadata/uid")
                                        .and_then(|u| u.as_str())
                                        .map(|s| s.to_string())
                                })
                        } else {
                            None
                        };
                        let claims = rusternetes_common::auth::ServiceAccountClaims {
                            sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
                            namespace: namespace.to_string(),
                            uid: sa_uid.clone(),
                            iat: now.timestamp(),
                            exp,
                            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
                            aud: audiences,
                            kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                                namespace: namespace.to_string(),
                                svcacct: rusternetes_common::auth::KubeRef {
                                    name: sa_name.to_string(),
                                    uid: sa_uid,
                                },
                                pod: Some(rusternetes_common::auth::KubeRef {
                                    name: pod_name.clone(),
                                    uid: pod.metadata.uid.clone(),
                                }),
                                node: node_name.as_ref().map(|nn| {
                                    rusternetes_common::auth::KubeRef {
                                        name: nn.clone(),
                                        uid: node_uid.clone().unwrap_or_default(),
                                    }
                                }),
                            }),
                            pod_name: Some(pod_name.clone()),
                            pod_uid: Some(pod.metadata.uid.clone()),
                            node_name,
                            node_uid,
                        };
                        let token = match self.token_manager.generate_token(claims) {
                            Ok(t) => t,
                            Err(e) => {
                                warn!(
                                    "Failed to generate SA token for pod {}: {}, using placeholder",
                                    pod_name, e
                                );
                                "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.placeholder".to_string()
                            }
                        };
                        std::fs::write(&token_path, &token)?;
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            std::fs::set_permissions(
                                &token_path,
                                std::fs::Permissions::from_mode(proj_default_mode as u32),
                            )?;
                        }
                    }
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(proj_dir_mode),
                )?;
            }

            info!("Created projected volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // Fallback: create an empty directory for unrecognized volume types
        // (e.g. nfs, iscsi, image, or any future types)
        // This prevents pod startup failures for volumes we don't natively handle.
        warn!(
            "Unknown volume type for volume {}, creating empty directory as fallback (volume debug: downward_api={}, empty_dir={}, host_path={}, config_map={}, secret={}, projected={}, pvc={}, csi={}, ephemeral={})",
            volume.name,
            volume.downward_api.is_some(),
            volume.empty_dir.is_some(),
            volume.host_path.is_some(),
            volume.config_map.is_some(),
            volume.secret.is_some(),
            volume.projected.is_some(),
            volume.persistent_volume_claim.is_some(),
            volume.csi.is_some(),
            volume.ephemeral.is_some(),
        );
        let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);
        std::fs::create_dir_all(&volume_dir)
            .context("Failed to create fallback volume directory")?;
        Ok(volume_dir)
    }

    /// Refresh Secret and ConfigMap volumes for a running pod.
    /// Re-reads the data from storage and overwrites files on disk.
    pub async fn refresh_volumes(&self, pod: &Pod) -> Result<()> {
        let storage = match &self.storage {
            Some(s) => s,
            None => return Ok(()),
        };
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let spec = match &pod.spec {
            Some(s) => s,
            None => return Ok(()),
        };
        let volumes = match &spec.volumes {
            Some(v) => v,
            None => return Ok(()),
        };

        for volume in volumes {
            let volume_dir = format!("{}/{}/{}", self.volumes_base_path, pod_name, volume.name);

            // Refresh Secret volumes
            if let Some(secret_source) = &volume.secret {
                // Create volume dir if it doesn't exist (optional Secret that was created later)
                let _ = std::fs::create_dir_all(&volume_dir);
                let secret_name = match &secret_source.secret_name {
                    Some(n) => n,
                    None => continue,
                };
                let secret_key =
                    rusternetes_storage::build_key("secrets", Some(namespace), secret_name);
                match storage
                    .get::<rusternetes_common::resources::Secret>(&secret_key)
                    .await
                {
                    Ok(secret) => {
                        if let Some(data) = &secret.data {
                            let items = secret_source.items.as_ref();
                            if let Some(items) = items {
                                for item in items {
                                    if let Some(value) = data.get(&item.key) {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        let _ = std::fs::write(&file_path, value);
                                    }
                                }
                            } else {
                                // Write all current keys
                                for (key, value) in data {
                                    let file_path = format!("{}/{}", volume_dir, key);
                                    let _ = std::fs::write(&file_path, value);
                                }
                                // Delete files for keys that no longer exist
                                if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                    for entry in entries.flatten() {
                                        if let Some(fname) = entry.file_name().to_str() {
                                            if !data.contains_key(fname)
                                                && fname != "..data"
                                                && fname != "ca.crt"
                                            {
                                                let _ = std::fs::remove_file(entry.path());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Secret was deleted — if optional, remove all volume files
                        let is_optional = secret_source.optional.unwrap_or(false);
                        if is_optional {
                            if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                for entry in entries.flatten() {
                                    let _ = std::fs::remove_file(entry.path());
                                }
                            }
                        }
                    }
                }
            }

            // Refresh ConfigMap volumes
            if let Some(cm_source) = &volume.config_map {
                let _ = std::fs::create_dir_all(&volume_dir);
                let cm_name = match &cm_source.name {
                    Some(n) => n,
                    None => continue,
                };
                let cm_key = rusternetes_storage::build_key("configmaps", Some(namespace), cm_name);
                match storage
                    .get::<rusternetes_common::resources::ConfigMap>(&cm_key)
                    .await
                {
                    Ok(cm) => {
                        if let Some(data) = &cm.data {
                            let items = cm_source.items.as_ref();
                            if let Some(items) = items {
                                for item in items {
                                    if let Some(value) = data.get(&item.key) {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        let _ = std::fs::write(&file_path, value);
                                    }
                                }
                            } else {
                                for (key, value) in data {
                                    let file_path = format!("{}/{}", volume_dir, key);
                                    let _ = std::fs::write(&file_path, value);
                                }
                                // Delete files for keys removed from ConfigMap
                                if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                    for entry in entries.flatten() {
                                        if let Some(fname) = entry.file_name().to_str() {
                                            if !data.contains_key(fname) && fname != "..data" {
                                                let _ = std::fs::remove_file(entry.path());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // ConfigMap deleted — clean up files if optional
                        let is_optional = cm_source.optional.unwrap_or(false);
                        if is_optional {
                            if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                for entry in entries.flatten() {
                                    let _ = std::fs::remove_file(entry.path());
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get a pod field value for DownwardAPI
    pub(crate) fn get_pod_field_value(&self, pod: &Pod, field_path: &str) -> Result<String> {
        let value = match field_path {
            "metadata.name" => pod.metadata.name.clone(),
            "metadata.namespace" => pod
                .metadata
                .namespace
                .clone()
                .unwrap_or("default".to_string()),
            "metadata.uid" => pod.metadata.uid.clone(),
            "spec.nodeName" => pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone())
                .unwrap_or("".to_string()),
            "spec.serviceAccountName" => pod
                .spec
                .as_ref()
                .and_then(|s| s.service_account_name.clone())
                .unwrap_or("default".to_string()),
            "status.podIP" => pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.clone())
                .unwrap_or("".to_string()),
            "status.hostIP" => pod
                .status
                .as_ref()
                .and_then(|s| s.host_ip.clone())
                .unwrap_or_else(|| "127.0.0.1".to_string()),
            // All labels formatted as key="value"\n (with trailing newline, matching K8s)
            "metadata.labels" => pod
                .metadata
                .labels
                .as_ref()
                .map(|labels| {
                    let mut pairs: Vec<_> = labels.iter().collect();
                    pairs.sort_by_key(|(k, _)| (*k).clone());
                    let mut result = pairs
                        .iter()
                        .map(|(k, v)| format!("{}=\"{}\"", k, v))
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !result.is_empty() {
                        result.push('\n');
                    }
                    result
                })
                .unwrap_or_default(),
            // All annotations formatted as key="value"\n (with trailing newline, matching K8s)
            "metadata.annotations" => pod
                .metadata
                .annotations
                .as_ref()
                .map(|anns| {
                    let mut pairs: Vec<_> = anns.iter().collect();
                    pairs.sort_by_key(|(k, _)| (*k).clone());
                    let mut result = pairs
                        .iter()
                        .map(|(k, v)| format!("{}=\"{}\"", k, v))
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !result.is_empty() {
                        result.push('\n');
                    }
                    result
                })
                .unwrap_or_default(),
            _ => {
                // Support metadata.labels['key'] and metadata.annotations['key']
                if field_path.starts_with("metadata.labels['") && field_path.ends_with("']") {
                    let key = &field_path[17..field_path.len() - 2];
                    pod.metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get(key))
                        .cloned()
                        .unwrap_or("".to_string())
                } else if field_path.starts_with("metadata.annotations['")
                    && field_path.ends_with("']")
                {
                    let key = &field_path[22..field_path.len() - 2];
                    pod.metadata
                        .annotations
                        .as_ref()
                        .and_then(|annotations| annotations.get(key))
                        .cloned()
                        .unwrap_or("".to_string())
                } else {
                    return Err(anyhow::anyhow!("Unsupported field path: {}", field_path));
                }
            }
        };
        Ok(value)
    }

    /// Get a container resource value for DownwardAPI
    ///
    /// Returns the resource value formatted according to the divisor.
    /// For memory: returns bytes (or bytes/divisor) as a string.
    /// For CPU: returns millicores (or cores with divisor "1") as a string.
    /// When divisor is "0" or absent, default units are used (bytes for memory, whole-number
    /// representation for CPU).
    pub(crate) fn get_container_resource_value(
        &self,
        pod: &Pod,
        resource_ref: &rusternetes_common::resources::ResourceFieldSelector,
    ) -> Result<String> {
        let spec = pod.spec.as_ref().context("Pod has no spec")?;

        // Find the container — if containerName is not set, default to the first container
        let container = if let Some(ref container_name) = resource_ref.container_name {
            spec.containers
                .iter()
                .find(|c| &c.name == container_name)
                .with_context(|| format!("Container {} not found", container_name))?
        } else {
            spec.containers.first().context("Pod has no containers")?
        };

        let is_cpu =
            resource_ref.resource.contains("cpu") || resource_ref.resource.contains("hugepages");
        let is_memory = resource_ref.resource.contains("memory")
            || resource_ref.resource.contains("ephemeral-storage");

        let raw_value = match resource_ref.resource.as_str() {
            "limits.cpu" => container
                .resources
                .as_ref()
                .and_then(|r| r.limits.as_ref())
                .and_then(|l| l.get("cpu"))
                .cloned(),
            "limits.memory" => container
                .resources
                .as_ref()
                .and_then(|r| r.limits.as_ref())
                .and_then(|l| l.get("memory"))
                .cloned(),
            "limits.ephemeral-storage" => container
                .resources
                .as_ref()
                .and_then(|r| r.limits.as_ref())
                .and_then(|l| l.get("ephemeral-storage"))
                .cloned(),
            "requests.cpu" => container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .and_then(|l| l.get("cpu"))
                .cloned(),
            "requests.memory" => container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .and_then(|l| l.get("memory"))
                .cloned(),
            "requests.ephemeral-storage" => container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref())
                .and_then(|l| l.get("ephemeral-storage"))
                .cloned(),
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported resource field: {}",
                    resource_ref.resource
                ))
            }
        };

        // Parse the divisor (default: "1" meaning base units — bytes for memory, cores for cpu)
        let divisor_str = resource_ref.divisor.as_deref().unwrap_or("0");
        // A divisor of "0" means use default units (same as "1")

        if is_cpu {
            // Convert CPU value to millicores, then apply divisor
            // When no limit is set, use node capacity (default 4 cores = 4000m)
            let millicores = raw_value.as_deref().map(parse_cpu_quantity).unwrap_or(4000); // 4 cores default
            let divisor_millicores = if divisor_str == "0" || divisor_str == "1" {
                // Default divisor "1" means return in cores (1 core = 1000 millicores)
                1000
            } else {
                parse_cpu_quantity(divisor_str).max(1)
            };
            // Kubernetes uses ceiling division for resource quantities
            let result = (millicores + divisor_millicores - 1) / divisor_millicores;
            Ok(result.to_string())
        } else if is_memory {
            // Convert memory value to bytes, then apply divisor
            // When no limit is set, use node allocatable memory (default 8Gi)
            let bytes = raw_value
                .as_deref()
                .map(parse_memory_quantity)
                .unwrap_or(8 * 1024 * 1024 * 1024); // 8Gi default
            let divisor_bytes = if divisor_str == "0" || divisor_str == "1" {
                1 // return bytes
            } else {
                parse_memory_quantity(divisor_str).max(1)
            };
            // Kubernetes uses ceiling division for resource quantities
            let result = (bytes + divisor_bytes - 1) / divisor_bytes;
            Ok(result.to_string())
        } else {
            // Unknown resource type, return raw value
            Ok(raw_value.unwrap_or_else(|| "0".to_string()))
        }
    }
}

#[cfg(all(test, unix))]
mod projected_mode_tests {
    use super::*;
    use rusternetes_storage::{build_key, Storage, StorageBackend};
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    /// A projected configMap item with an explicit `mode` must have that mode
    /// applied to the file the resync path (re)writes — matching the
    /// initial-mount path and upstream's atomic writer. Regression guard for the
    /// resync gap where new keys landed at the umask default (#1050).
    #[tokio::test]
    async fn resync_projected_configmap_applies_item_mode() {
        let tmp = std::env::temp_dir().join(format!("rn-projmode-{}-{}", std::process::id(), "cm"));
        let _ = std::fs::remove_dir_all(&tmp);

        let storage = Arc::new(StorageBackend::new_memory());
        let cm: rusternetes_common::resources::ConfigMap = serde_json::from_value(json!({
            "metadata": {"name": "cfg", "namespace": "default"},
            "data": {"app.conf": "hello"}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm,
        )
        .await
        .unwrap();

        // defaultMode 0644 (420), item mode 0400 (256).
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": [], "volumes": [{
                "name": "proj",
                "projected": {
                    "defaultMode": 420,
                    "sources": [{"configMap": {
                        "name": "cfg",
                        "items": [{"key": "app.conf", "path": "app.conf", "mode": 256}]
                    }}]
                }
            }]}
        }))
        .unwrap();

        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();

        let file = tmp.join("p").join("proj").join("app.conf");
        let perms = std::fs::metadata(&file)
            .expect("projected file must exist after resync")
            .permissions();
        assert_eq!(
            perms.mode() & 0o777,
            0o400,
            "projected configMap item mode must be applied on resync"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// Projected secret resync must honor `items` mappings. If it writes every
    /// secret key at the volume root, stale cleanup can delete the mapped file
    /// that the container is waiting for.
    #[tokio::test]
    async fn resync_projected_secret_honors_items_mapping() {
        let tmp =
            std::env::temp_dir().join(format!("rn-projmode-{}-{}", std::process::id(), "secret"));
        let _ = std::fs::remove_dir_all(&tmp);

        let storage = Arc::new(StorageBackend::new_memory());
        let secret: rusternetes_common::resources::Secret = serde_json::from_value(json!({
            "metadata": {"name": "sec", "namespace": "default"},
            "data": {"data-1": "dmFsdWUtMQ=="}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": [], "volumes": [{
                "name": "proj",
                "projected": {
                    "defaultMode": 420,
                    "sources": [{"secret": {
                        "name": "sec",
                        "items": [{"key": "data-1", "path": "new-path-data-1", "mode": 256}]
                    }}]
                }
            }]}
        }))
        .unwrap();

        let volume_dir = tmp.join("p").join("proj");
        std::fs::create_dir_all(&volume_dir).unwrap();
        std::fs::write(volume_dir.join("data-1"), b"stale-root").unwrap();

        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();

        let mapped = volume_dir.join("new-path-data-1");
        assert_eq!(std::fs::read(&mapped).unwrap(), b"value-1");
        assert!(
            !volume_dir.join("data-1").exists(),
            "unmapped root secret key must be removed as stale"
        );
        let perms = std::fs::metadata(&mapped).unwrap().permissions();
        assert_eq!(
            perms.mode() & 0o777,
            0o400,
            "projected secret item mode must be applied on resync"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
