//! Static pod support: pods defined by local manifest files, run by the
//! kubelet and projected into storage as read-only "mirror pods".
//!
//! Upstream: pkg/kubelet/config/file.go (file source),
//! pkg/kubelet/pod/mirror_client.go (mirror lifecycle),
//! pkg/kubelet/types/pod_update.go (config annotations).

use anyhow::{bail, Context, Result};
use rusternetes_common::resources::Pod;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use tracing::warn;

pub const CONFIG_SOURCE_ANNOTATION: &str = "kubernetes.io/config.source";
pub const CONFIG_MIRROR_ANNOTATION: &str = "kubernetes.io/config.mirror";
pub const CONFIG_HASH_ANNOTATION: &str = "kubernetes.io/config.hash";

/// Parse one manifest file (YAML or JSON) into a Pod. Rejects non-Pod kinds
/// and pods without a name.
pub fn parse_manifest(bytes: &[u8], file_name: &str) -> Result<Pod> {
    let pod: Pod = if file_name.ends_with(".json") {
        serde_json::from_slice(bytes).with_context(|| format!("parsing {file_name} as JSON"))?
    } else {
        serde_yaml::from_slice(bytes).with_context(|| format!("parsing {file_name} as YAML"))?
    };
    if pod.type_meta.kind != "Pod" {
        bail!("{file_name}: kind must be Pod");
    }
    if pod.metadata.name.is_empty() {
        bail!("{file_name}: metadata.name is required");
    }
    Ok(pod)
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Hash the pod's identity + spec (not status, not the hash annotations
/// themselves). Stable across kubelet restarts so mirrors don't churn.
pub fn pod_config_hash(pod: &Pod) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pod.metadata.namespace.as_deref().unwrap_or("default"));
    hasher.update(b"/");
    hasher.update(&pod.metadata.name);
    hasher.update(serde_json::to_vec(&pod.spec).unwrap_or_default());
    hex_encode(&hasher.finalize()[..16])
}

/// Apply static-pod defaults, mirroring upstream applyDefaults
/// (pkg/kubelet/config/common.go): name suffix "-<node>", default namespace,
/// pin spec.nodeName, stamp config.source/config.hash, deterministic UID.
pub fn normalize_static_pod(mut pod: Pod, node_name: &str) -> Result<Pod> {
    pod.metadata.name = format!("{}-{}", pod.metadata.name, node_name);
    if pod.metadata.namespace.as_deref().unwrap_or("").is_empty() {
        pod.metadata.namespace = Some("default".to_string());
    }
    let spec = pod
        .spec
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("{}: spec is required", pod.metadata.name))?;
    spec.node_name = Some(node_name.to_string());

    let hash = pod_config_hash(&pod);
    // UID derived from the hash: stable while the manifest is unchanged, so
    // container labels and status writers agree across kubelet restarts.
    let h = Sha256::digest(format!("static:{node_name}:{hash}").as_bytes());
    pod.metadata.uid = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8], h[9], h[10], h[11], h[12], h[13], h[14], h[15]
    );

    let ann = pod.metadata.annotations.get_or_insert_with(HashMap::new);
    ann.insert(CONFIG_SOURCE_ANNOTATION.to_string(), "file".to_string());
    ann.insert(CONFIG_HASH_ANNOTATION.to_string(), hash);
    Ok(pod)
}

/// The API-side projection of a static pod.
pub fn make_mirror_pod(static_pod: &Pod) -> Pod {
    let mut mirror = static_pod.clone();
    let hash = mirror
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(CONFIG_HASH_ANNOTATION))
        .cloned()
        .unwrap_or_default();
    mirror
        .metadata
        .annotations
        .get_or_insert_with(HashMap::new)
        .insert(CONFIG_MIRROR_ANNOTATION.to_string(), hash);
    mirror
}

pub fn is_mirror_pod(pod: &Pod) -> bool {
    pod.metadata
        .annotations
        .as_ref()
        .map(|a| a.contains_key(CONFIG_MIRROR_ANNOTATION))
        .unwrap_or(false)
}

/// Read every *.yaml/*.yml/*.json in `dir`, parse + normalize. Invalid files
/// are skipped with a warning (upstream file source behavior). Deterministic
/// name-sorted order.
pub fn load_static_pods(dir: &Path, node_name: &str) -> Vec<Pod> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(
                "static pods: cannot read manifest dir {}: {}",
                dir.display(),
                e
            );
            return Vec::new();
        }
    };
    let mut pods = Vec::new();
    let mut paths: Vec<_> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !matches!(ext, "yaml" | "yml" | "json") {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_string();
        let result = std::fs::read(&path)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| parse_manifest(&bytes, &file_name))
            .and_then(|pod| normalize_static_pod(pod, node_name));
        match result {
            Ok(pod) => pods.push(pod),
            Err(e) => warn!("static pods: skipping {}: {:#}", file_name, e),
        }
    }
    pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    pods
}

use rusternetes_storage::{build_key, build_prefix, Storage};

/// Project `desired` static pods into storage as mirror pods and delete
/// stale mirrors for this node. Never touches non-mirror pods.
/// Upstream: pkg/kubelet/pod/mirror_client.go CreateMirrorPod /
/// DeleteMirrorPod (hash-compare via the config.mirror annotation).
pub async fn reconcile_mirror_pods<S: Storage>(
    storage: &S,
    node_name: &str,
    desired: &[Pod],
) -> Result<()> {
    for pod in desired {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key("pods", Some(ns), &pod.metadata.name);
        let want_hash = pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(CONFIG_HASH_ANNOTATION))
            .cloned()
            .unwrap_or_default();
        match storage.get::<Pod>(&key).await {
            Ok(existing) => {
                let have = existing
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(CONFIG_MIRROR_ANNOTATION))
                    .cloned()
                    .unwrap_or_default();
                if have != want_hash {
                    // manifest changed: recreate the mirror (upstream behavior)
                    let _ = storage.delete(&key).await;
                    storage.create(&key, &make_mirror_pod(pod)).await?;
                }
            }
            Err(_) => {
                storage.create(&key, &make_mirror_pod(pod)).await?;
            }
        }
    }

    // Delete stale mirrors: mirror-annotated pods on this node whose name is
    // no longer in the desired set.
    let desired_names: std::collections::HashSet<&str> =
        desired.iter().map(|p| p.metadata.name.as_str()).collect();
    let all: Vec<Pod> = storage.list(&build_prefix("pods", None)).await?;
    for pod in all {
        let on_node = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .map(|n| n == node_name)
            .unwrap_or(false);
        if on_node && is_mirror_pod(&pod) && !desired_names.contains(pod.metadata.name.as_str()) {
            let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
            let _ = storage
                .delete(&build_key("pods", Some(ns), &pod.metadata.name))
                .await;
        }
    }
    Ok(())
}

/// Merge storage-sourced pods with file-sourced static pods for one node.
/// Mirror copies of static pods are dropped in favor of the file version
/// (the file source is authoritative; upstream never runs mirror pods).
pub fn merge_node_pods(storage_pods: Vec<Pod>, static_pods: Vec<Pod>, node_name: &str) -> Vec<Pod> {
    let static_names: std::collections::HashSet<String> = static_pods
        .iter()
        .map(|p| p.metadata.name.clone())
        .collect();
    let mut merged: Vec<Pod> = storage_pods
        .into_iter()
        .filter(|p| {
            p.spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                .map(|n| n == node_name)
                .unwrap_or(false)
        })
        .filter(|p| !(is_mirror_pod(p) || static_names.contains(&p.metadata.name)))
        .collect();
    merged.extend(static_pods);
    merged
}
